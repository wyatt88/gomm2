// Package mirror implements the core replication engine for gomm2.
package mirror

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/gomm2/gomm2/internal/config"
	"github.com/gomm2/gomm2/internal/filter"
	"github.com/gomm2/gomm2/internal/kafka"
	"github.com/gomm2/gomm2/internal/metrics"
	"github.com/gomm2/gomm2/internal/offset"
	"github.com/gomm2/gomm2/internal/policy"
	"github.com/gomm2/gomm2/pkg/types"
)

// Source replicates records from a source cluster to a target cluster.
type Source struct {
	cfg         config.ReplicationConfig
	srcCfg      config.ClusterConfig
	tgtCfg      config.ClusterConfig
	topicFilter *filter.TopicFilter
	policy      policy.ReplicationPolicy
	metrics     *metrics.Metrics
	syncStore   *offset.SyncStore
	syncWriter  *offset.SyncWriter
	dlq         *DLQ
	logger      *slog.Logger

	consumer *kgo.Client
	producer *kgo.Client

	mu               sync.Mutex
	running          bool
	cancel           context.CancelFunc
	wg               sync.WaitGroup
	subscribedTopics map[string]struct{}
	inflight         sync.WaitGroup
}

// NewSource creates a new Source replicator.
func NewSource(
	cfg config.ReplicationConfig,
	srcCfg, tgtCfg config.ClusterConfig,
	m *metrics.Metrics,
	syncStore *offset.SyncStore,
	logger *slog.Logger,
) (*Source, error) {
	topicFilter, err := filter.NewTopicFilter(cfg.TopicFilter.Whitelist, cfg.TopicFilter.Blacklist)
	if err != nil {
		return nil, fmt.Errorf("create topic filter: %w", err)
	}
	pol, err := policy.NewPolicy(cfg.ReplicationPolicy, cfg.Separator)
	if err != nil {
		return nil, fmt.Errorf("create replication policy: %w", err)
	}
	return &Source{
		cfg:              cfg,
		srcCfg:           srcCfg,
		tgtCfg:           tgtCfg,
		topicFilter:      topicFilter,
		policy:           pol,
		metrics:          m,
		syncStore:        syncStore,
		logger:           logger.With("component", "source", "flow", cfg.Source+"->"+cfg.Target),
		subscribedTopics: make(map[string]struct{}),
	}, nil
}

// SetSyncWriter sets the offset sync writer for persisting syncs to Kafka.
func (s *Source) SetSyncWriter(w *offset.SyncWriter) {
	s.syncWriter = w
}

// Start begins replication.
func (s *Source) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return fmt.Errorf("source already running")
	}

	ctx, cancel := context.WithCancel(ctx)
	s.cancel = cancel

	// === SOURCE CONSUMER — optimized for max single-partition throughput ===
	srcOpts, err := kafka.BuildClientOpts(s.srcCfg)
	if err != nil {
		cancel()
		return fmt.Errorf("build source client opts: %w", err)
	}
	srcOpts = append(srcOpts,
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),

		// Fetch tuning: maximize bytes per round-trip
		// BDP = bandwidth × RTT. For 100MB/s link with 40ms RTT = 4MB.
		// We want each fetch to return as much data as possible to amortize RTT.
		kgo.FetchMaxBytes(100*1024*1024),           // 100MB overall max per fetch
		kgo.FetchMaxPartitionBytes(10*1024*1024),    // 10MB per partition — THE key knob for single-partition
		kgo.FetchMinBytes(512*1024),                 // 512KB min before broker responds (reduces empty fetches)
		kgo.FetchMaxWait(500*time.Millisecond),      // max wait 500ms if min bytes not met

		kgo.ConsumerGroup("gomm2-source-"+s.cfg.Source+"-"+s.cfg.Target),
		kgo.DisableAutoCommit(),
	)
	if s.cfg.ReadCommitted {
		srcOpts = append(srcOpts, kgo.FetchIsolationLevel(kgo.ReadCommitted()))
	}

	consumer, err := kgo.NewClient(srcOpts...)
	if err != nil {
		cancel()
		return fmt.Errorf("create source consumer: %w", err)
	}
	s.consumer = consumer

	// === TARGET PRODUCER — optimized for cross-region writes ===
	tgtOpts, err := kafka.BuildClientOpts(s.tgtCfg)
	if err != nil {
		consumer.Close()
		cancel()
		return fmt.Errorf("build target client opts: %w", err)
	}
	tgtOpts = append(tgtOpts,
		// Large batches: fill up to 1MB before sending → fewer requests over the wire
		kgo.ProducerBatchMaxBytes(int32(s.cfg.ProducerBatchSize)),
		kgo.ProducerLinger(time.Duration(s.cfg.ProducerLingerMs)*time.Millisecond),

		// Manual partitioning: preserve source partition assignment
		kgo.RecordPartitioner(kgo.ManualPartitioner()),

		// All-ISR acks for durability
		kgo.RequiredAcks(kgo.AllISRAcks()),

		kgo.ProducerBatchCompression(compressionCodec(s.cfg.Compression)),

		// Idempotent writes enabled (default) for ordering guarantees.
		// Idempotent mode supports high inflight without risking reordering.

		// High inflight: multiple produce batches can be in-flight simultaneously
		// This is THE most important producer knob for cross-region latency.
		// With 40ms RTT and 20 inflight: 20 × 1MB / 40ms = 500 MB/s theoretical max
		kgo.MaxProduceRequestsInflightPerBroker(20),

		// Buffer up to 256MB in-flight records (default is 100MB)
		kgo.MaxBufferedRecords(100000),
	)

	producer, err := kgo.NewClient(tgtOpts...)
	if err != nil {
		consumer.Close()
		cancel()
		return fmt.Errorf("create target producer: %w", err)
	}
	s.producer = producer

	s.running = true
	s.wg.Add(1)
	go s.replicationLoop(ctx)

	s.logger.Info("source replication started")
	return nil
}

// UpdateTopics dynamically adds source topics to the consumer subscription.
func (s *Source) UpdateTopics(topics []string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.consumer == nil {
		return
	}

	var newTopics []string
	for _, t := range topics {
		if _, ok := s.subscribedTopics[t]; !ok {
			s.subscribedTopics[t] = struct{}{}
			newTopics = append(newTopics, t)
		}
	}
	if len(newTopics) > 0 {
		s.consumer.AddConsumeTopics(newTopics...)
		s.logger.Info("subscribed to new source topics",
			"new_topics", newTopics,
			"total_subscribed", len(s.subscribedTopics))
	}
}

// Stop gracefully shuts down the source replicator.
func (s *Source) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running {
		return
	}
	s.logger.Info("stopping source replication")
	s.cancel()
	s.wg.Wait()

	// Drain inflight produces then commit
	s.logger.Info("draining inflight produces")
	s.inflight.Wait()

	if s.consumer != nil {
		s.logger.Info("committing final offsets before shutdown")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := s.consumer.CommitUncommittedOffsets(ctx); err != nil {
			s.logger.Error("failed to commit final offsets", "err", err)
		} else {
			s.logger.Info("final offsets committed")
		}
		cancel()
		s.consumer.Close()
	}
	if s.producer != nil {
		s.producer.Close()
	}
	s.running = false
	s.logger.Info("source replication stopped")
}

// replicationLoop: fully async pipeline with batched metrics.
//
// Design: Consumer polls → records fire-and-forget into producer → producer batches
// and sends with up to 20 inflight requests per broker. We never block between
// poll and produce. We only drain (inflight.Wait) at commit boundaries.
//
// Metrics are batched: we accumulate counters per topic:partition in local maps
// and flush to Prometheus periodically, avoiding WithLabelValues() per record.
func (s *Source) replicationLoop(ctx context.Context) {
	defer s.wg.Done()

	var (
		recordsSinceCommit int64
		lastCommitTime     = time.Now()
		commitInterval     = s.cfg.OffsetCommitInterval.Duration
		commitBatch        = s.cfg.OffsetCommitBatch
		syncWriteInterval  = int64(2000)
		recordsSinceSync   atomic.Int64

		// Batched metrics: accumulate locally, flush periodically
		localRecCounts  = make(map[string]float64) // key: topic:partition
		localByteCounts = make(map[string]float64)
		lastMetricFlush = time.Now()
		metricFlushInterval = 5 * time.Second
	)

	srcLabel := s.cfg.Source
	tgtLabel := s.cfg.Target

	// Final metrics flush on exit — ensure no counters are lost
	defer func() {
		for key, count := range localRecCounts {
			var topic, part string
			for i := len(key) - 1; i >= 0; i-- {
				if key[i] == ':' {
					topic = key[:i]
					part = key[i+1:]
					break
				}
			}
			s.metrics.RecordsReplicated.WithLabelValues(srcLabel, tgtLabel, topic, part).Add(count)
			s.metrics.BytesReplicated.WithLabelValues(srcLabel, tgtLabel, topic, part).Add(localByteCounts[key])
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		fetches := s.consumer.PollFetches(ctx)

		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				s.logger.Error("consume error", "topic", e.Topic, "partition", e.Partition, "err", e.Err)
				s.metrics.ConsumeErrors.WithLabelValues(s.cfg.Source, s.cfg.Target).Inc()
			}
		}

		records := fetches.Records()
		if len(records) == 0 {
			continue
		}

		batchLen := len(records)

		// === ASYNC PRODUCE — no per-batch wait ===
		for _, record := range records {
			targetTopic := s.policy.FormatRemoteTopic(s.cfg.Source, record.Topic)
			targetRecord := &kgo.Record{
				Topic:     targetTopic,
				Partition: record.Partition,
				Key:       record.Key,
				Value:     record.Value,
				Headers:   record.Headers,
				Timestamp: record.Timestamp,
			}

			srcTP := types.TopicPartition{Topic: record.Topic, Partition: record.Partition}
			partStr := strconv.FormatInt(int64(record.Partition), 10)

			// Accumulate metrics locally (fast path, no Prometheus overhead)
			metricsKey := targetTopic + ":" + partStr
			localRecCounts[metricsKey]++
			localByteCounts[metricsKey] += float64(len(record.Value))

			s.inflight.Add(1)
			srcOffset := record.Offset
			s.producer.Produce(ctx, targetRecord, func(r *kgo.Record, err error) {
				defer s.inflight.Done()
				if err != nil {
					s.logger.Error("produce error", "topic", targetTopic, "err", err)
					s.metrics.ProduceErrors.WithLabelValues(srcLabel, tgtLabel, targetTopic).Inc()
					return
				}

				if config.BoolDefault(s.cfg.EmitOffsetSyncs, true) && s.syncStore != nil {
					osync := types.OffsetSync{
						TopicPartition:   srcTP,
						UpstreamOffset:   srcOffset,
						DownstreamOffset: r.Offset,
					}
					s.syncStore.HandleSync(osync)

					if s.syncWriter != nil {
						if recordsSinceSync.Add(1)%syncWriteInterval == 0 {
							s.inflight.Add(1)
							go func(o types.OffsetSync) {
								defer s.inflight.Done()
								writeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
								defer cancel()
								if wErr := s.syncWriter.Write(writeCtx, o); wErr != nil {
									s.logger.Warn("failed to persist offset sync", "err", wErr)
								}
							}(osync)
						}
					}
				}
			})
		}

		// === FLUSH METRICS PERIODICALLY ===
		if time.Since(lastMetricFlush) >= metricFlushInterval {
			for key, count := range localRecCounts {
				// Parse topic:partition from key
				var topic, part string
				for i := len(key) - 1; i >= 0; i-- {
					if key[i] == ':' {
						topic = key[:i]
						part = key[i+1:]
						break
					}
				}
				s.metrics.RecordsReplicated.WithLabelValues(srcLabel, tgtLabel, topic, part).Add(count)
				s.metrics.BytesReplicated.WithLabelValues(srcLabel, tgtLabel, topic, part).Add(localByteCounts[key])
				delete(localRecCounts, key)
				delete(localByteCounts, key)
			}
			s.metrics.PollBatchSize.WithLabelValues(srcLabel, tgtLabel).Observe(float64(batchLen))
			lastMetricFlush = time.Now()
		}

		// === PERIODIC OFFSET COMMIT ===
		recordsSinceCommit += int64(batchLen)
		if recordsSinceCommit >= int64(commitBatch) || time.Since(lastCommitTime) >= commitInterval {
			// Drain all in-flight produces before committing offsets
			s.inflight.Wait()

			if err := s.consumer.CommitUncommittedOffsets(ctx); err != nil {
				s.logger.Error("offset commit failed", "err", err)
				s.metrics.OffsetCommitErrors.WithLabelValues(s.cfg.Source, s.cfg.Target).Inc()
			} else {
				s.logger.Debug("offsets committed", "records_since_last", recordsSinceCommit)
				s.metrics.OffsetCommits.WithLabelValues(s.cfg.Source, s.cfg.Target).Inc()
				recordsSinceCommit = 0
				lastCommitTime = time.Now()
			}
		}
	}
}

func compressionCodec(name string) kgo.CompressionCodec {
	switch name {
	case "gzip":
		return kgo.GzipCompression()
	case "snappy":
		return kgo.SnappyCompression()
	case "lz4":
		return kgo.Lz4Compression()
	case "zstd":
		return kgo.ZstdCompression()
	default:
		return kgo.NoCompression()
	}
}
