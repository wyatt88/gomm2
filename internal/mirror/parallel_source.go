package mirror

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/gomm2/gomm2/internal/config"
	"github.com/gomm2/gomm2/internal/kafka"
	"github.com/gomm2/gomm2/internal/metrics"
	"github.com/gomm2/gomm2/internal/policy"
)

// ParallelSource runs N independent consumers on non-overlapping offset ranges
// of the same partition to multiply cross-region fetch throughput.
//
// Architecture (v2 — ring-buffer ordered pipeline):
//
//	Per partition:
//	  N fetcher goroutines → OrderedRingBuffer → 1 drainer goroutine → shared producer
//
//	Fetchers: each has its own franz-go client (= independent TCP connection),
//	assigned a non-overlapping [fromOffset, toOffset) range. They Put() records
//	into the ring buffer indexed by source offset.
//
//	Drainer: a single goroutine per partition calls DrainBatch() which yields
//	records in strict source-offset order. It then Produce()s them to the shared
//	producer with ManualPartitioner, preserving within-partition ordering.
//
//	Shared Producer: one kgo.Client for all partitions/workers. Maximizes
//	batching and amortises connection overhead to the target cluster.
//
// Memory budget:
//
//	Workers × FetchMaxPartitionBytes ≈ 48 × 5MB = 240MB fetch buffers
//	Ring buffers: 8 partitions × 16384 slots × pointer = negligible
//	Producer buffers: ~256MB max
//	Total: ~500MB — well within 8GB limit
type ParallelSource struct {
	cfg    config.ReplicationConfig
	srcCfg config.ClusterConfig
	tgtCfg config.ClusterConfig
	policy policy.ReplicationPolicy
	m      *metrics.Metrics
	logger *slog.Logger
	workers            int
	fetchMaxPartBytes  int // per-worker FetchMaxPartitionBytes
	ringBufferCapacity int // ring buffer slots per partition

	totalBytes   atomic.Int64
	totalRecords atomic.Int64

	// [FIX-FORGE Bug3] DLQ and retry support for parallel catch-up produce failures
	dlq            *DLQ
	retryCfg       RetryConfig
	circuitBreaker *CircuitBreaker
}

func NewParallelSource(
	cfg config.ReplicationConfig,
	srcCfg, tgtCfg config.ClusterConfig,
	m *metrics.Metrics,
	workers int,
	logger *slog.Logger,
) (*ParallelSource, error) {
	pol, err := policy.NewPolicy(cfg.ReplicationPolicy, cfg.Separator)
	if err != nil {
		return nil, fmt.Errorf("create replication policy: %w", err)
	}
	if workers < 2 {
		workers = 2
	}

	fetchMaxPartBytes := cfg.FetchMaxPartitionBytes
	if fetchMaxPartBytes <= 0 {
		fetchMaxPartBytes = 5 * 1024 * 1024 // 5MB default
	}
	ringCap := cfg.RingBufferCapacity
	if ringCap <= 0 {
		ringCap = 16384
	}

	// [FIX-FORGE Bug3] Initialize DLQ if enabled
	var dlq *DLQ
	if cfg.DLQEnabled {
		dlq, err = NewDLQ(cfg, tgtCfg, m, logger)
		if err != nil {
			logger.Warn("failed to create DLQ for parallel source, continuing without DLQ", "err", err)
		}
	}

	// [FIX-FORGE Bug3] Build retry config from replication config
	retryCfg := RetryConfig{
		MaxRetries:  cfg.MaxRetries,
		BackoffBase: cfg.RetryBackoffBase.Duration,
		BackoffMax:  cfg.RetryBackoffMax.Duration,
	}
	if retryCfg.MaxRetries == 0 {
		retryCfg = DefaultRetryConfig()
	}

	return &ParallelSource{
		cfg:                cfg,
		srcCfg:             srcCfg,
		tgtCfg:             tgtCfg,
		policy:             pol,
		m:                  m,
		logger:             logger.With("component", "parallel_source", "flow", cfg.Source+"->"+cfg.Target),
		workers:            workers,
		fetchMaxPartBytes:  fetchMaxPartBytes,
		ringBufferCapacity: ringCap,
		dlq:                dlq,
		retryCfg:           retryCfg,
		circuitBreaker:     NewCircuitBreaker(DefaultCircuitBreakerConfig()),
	}, nil
}

// CatchUpMultiPartition runs parallel catch-up across ALL partitions simultaneously.
// Each partition gets `workers` parallel fetchers piped through an OrderedRingBuffer
// into a single drainer that produces in strict offset order.
func (ps *ParallelSource) CatchUpMultiPartition(ctx context.Context, partitionLags map[int32][2]int64, topic string) error {
	if len(partitionLags) == 0 {
		return nil
	}

	// Build ONE shared producer for all workers — maximizes batching
	tgtOpts, err := kafka.BuildClientOpts(ps.tgtCfg)
	if err != nil {
		return fmt.Errorf("build target opts: %w", err)
	}
	tgtOpts = append(tgtOpts,
		kgo.ProducerBatchMaxBytes(1048576),       // 1MB batches
		kgo.ProducerLinger(5*time.Millisecond),    // 5ms linger — target is same-VPC
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.Lz4Compression()),
		kgo.MaxProduceRequestsInflightPerBroker(20),
		kgo.MaxBufferedRecords(500000),            // 500K buffered records
	)
	producer, err := kgo.NewClient(tgtOpts...)
	if err != nil {
		return fmt.Errorf("create shared producer: %w", err)
	}
	defer producer.Close()

	startTime := time.Now()
	ps.totalBytes.Store(0)
	ps.totalRecords.Store(0)

	totalLag := int64(0)
	for _, lag := range partitionLags {
		totalLag += lag[1] - lag[0]
	}

	totalWorkers := len(partitionLags) * ps.workers
	ps.logger.Info("starting multi-partition parallel catch-up (v2 ring-buffer pipeline)",
		"topic", topic,
		"partitions", len(partitionLags),
		"workers_per_partition", ps.workers,
		"total_workers", totalWorkers,
		"total_lag", totalLag,
		"gomaxprocs", runtime.GOMAXPROCS(0),
	)

	// Launch all partitions concurrently
	var wg sync.WaitGroup
	for part, offsets := range partitionLags {
		startOff, endOff := offsets[0], offsets[1]
		wg.Add(1)
		go func(p int32, from, to int64) {
			defer wg.Done()
			ps.catchUpPartitionPipelined(ctx, producer, topic, p, from, to)
		}(part, startOff, endOff)
	}
	wg.Wait()

	elapsed := time.Since(startTime)
	tb := ps.totalBytes.Load()
	tr := ps.totalRecords.Load()
	mbs := float64(tb) / elapsed.Seconds() / 1024 / 1024

	ps.logger.Info("multi-partition catch-up complete",
		"topic", topic,
		"elapsed", elapsed.Round(time.Millisecond),
		"total_bytes_gb", fmt.Sprintf("%.2f", float64(tb)/1024/1024/1024),
		"total_records", tr,
		"throughput_mbs", fmt.Sprintf("%.1f", mbs),
	)
	return nil
}

// catchUpPartitionPipelined uses the OrderedRingBuffer to decouple parallel
// fetchers from in-order production. This is the key architectural change:
//
//   N fetchers → RingBuffer(capacity) → 1 drainer → shared producer
//
// The ring buffer provides backpressure: if fetchers get too far ahead of the
// drainer, they block on Put() until slots are freed. This bounds memory to
// capacity × avg_record_size.
func (ps *ParallelSource) catchUpPartitionPipelined(ctx context.Context, producer *kgo.Client, topic string, partition int32, startOffset, endOffset int64) {
	totalRange := endOffset - startOffset
	if totalRange <= 0 {
		return
	}

	// Determine effective worker count for this partition
	effectiveWorkers := ps.workers
	segmentSize := totalRange / int64(effectiveWorkers)
	// Don't bother with multiple workers if the range is tiny
	if segmentSize < 1000 {
		effectiveWorkers = 1
		segmentSize = totalRange
	}

	ps.logger.Info("partition catch-up starting (pipelined)",
		"topic", topic, "partition", partition,
		"start", startOffset, "end", endOffset, "range", totalRange,
		"workers", effectiveWorkers,
	)

	// Ring buffer capacity: enough to absorb fetch bursts without blocking fetchers.
	// With 5MB FetchMaxPartitionBytes and ~40KB records, one fetch ≈ 128 records.
	// With N workers, worst case burst ≈ N × 128 = 768 records.
	// Use 16384 for headroom — at ~40KB each this is ~640MB if all slots full,
	// but typically only a fraction is filled due to drain rate.
	// Actually the buffer holds pointers, not data. The data lives in fetch buffers
	// until the producer callback fires, at which point franz-go releases them.
	ringCap := ps.ringBufferCapacity
	if totalRange < int64(ringCap) {
		ringCap = int(totalRange) + 1
	}
	rb := NewOrderedRingBuffer(ringCap, startOffset)

	targetTopic := ps.policy.FormatRemoteTopic(ps.cfg.Source, topic)

	// [FIX-FORGE Bug1] Track fetcher errors so we can close the ring buffer on failure.
	// fetcherFailed is set to true by any fetcher that exits with a fatal error.
	// When set, the ring buffer is closed with error to unblock the drainer.
	var fetcherFailed atomic.Bool

	// Start the drainer goroutine — reads from ring buffer in offset order,
	// produces to target maintaining strict ordering.
	var drainerWg sync.WaitGroup
	drainerWg.Add(1)
	go func() {
		defer drainerWg.Done()
		// [FIX-FORGE Bug2+Bug3] Pass source topic for DLQ record tracing
		ps.drainPartition(ctx, rb, producer, targetTopic, topic, partition, startOffset, endOffset)
	}()

	// Launch fetcher workers with non-overlapping offset ranges
	var fetcherWg sync.WaitGroup
	for i := 0; i < effectiveWorkers; i++ {
		segStart := startOffset + int64(i)*segmentSize
		segEnd := segStart + segmentSize
		if i == effectiveWorkers-1 {
			segEnd = endOffset // last worker takes the remainder
		}
		if segStart >= endOffset {
			break
		}
		fetcherWg.Add(1)
		go func(id int, from, to int64) {
			defer fetcherWg.Done()
			// [FIX-FORGE Bug1] fetchWorker now returns error; on failure we close
			// the ring buffer to unblock the drainer that's waiting on DrainBatch().
			if err := ps.fetchWorker(ctx, rb, id, topic, partition, from, to); err != nil {
				ps.logger.Error("fetcher failed fatally, closing ring buffer to unblock drainer",
					"fetcher", id, "partition", partition, "err", err)
				fetcherFailed.Store(true)
				rb.CloseWithError()
			}
		}(i, segStart, segEnd)
	}

	// Wait for all fetchers to finish, then close the ring buffer
	fetcherWg.Wait()
	if fetcherFailed.Load() {
		// [FIX-FORGE Bug1] Ring buffer already closed by failed fetcher;
		// ensure it's closed (idempotent) so drainer can finish.
		rb.CloseWithError()
		ps.logger.Error("partition catch-up had fetcher failures",
			"topic", topic, "partition", partition)
	} else {
		rb.Close()
	}

	// Wait for drainer to finish flushing
	drainerWg.Wait()
}

// fetchWorker consumes records from a source partition in the range [fromOffset, toOffset)
// and inserts them into the ring buffer. Each worker has its own franz-go client
// (= its own TCP connection) for maximum parallelism across the cross-region link.
// [FIX-FORGE Bug1] fetchWorker now returns an error so the caller can detect
// fatal failures and close the ring buffer to unblock the drainer goroutine.
// Previously, a silent return on error (e.g., BuildClientOpts or NewClient failure)
// left the drainer permanently blocked on DrainBatch() waiting for offsets that
// would never arrive — this caused the P5 hang observed in the 300GB test.
func (ps *ParallelSource) fetchWorker(ctx context.Context, rb *OrderedRingBuffer, id int, topic string, partition int32, fromOffset, toOffset int64) error {
	logger := ps.logger.With("fetcher", id, "partition", partition, "from", fromOffset, "to", toOffset)

	srcOpts, err := kafka.BuildClientOpts(ps.srcCfg)
	if err != nil {
		logger.Error("build source opts failed", "err", err)
		return fmt.Errorf("build source opts: %w", err)
	}

	// Tuning per-worker fetch parameters:
	// - FetchMaxPartitionBytes: configurable (default 5MB) to control memory.
	// - FetchMaxBytes: 2× FetchMaxPartitionBytes as overall cap per fetch.
	// - FetchMinBytes: 256KB reduces empty fetches without adding latency.
	// - FetchMaxWait: 200ms — don't wait too long if data is sparse.
	fetchMaxPart := int32(ps.fetchMaxPartBytes)
	srcOpts = append(srcOpts,
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			topic: {partition: kgo.NewOffset().At(fromOffset)},
		}),
		kgo.FetchMaxBytes(2*fetchMaxPart),                 // 2× partition max as overall cap
		kgo.FetchMaxPartitionBytes(fetchMaxPart),           // configurable per partition
		kgo.FetchMinBytes(256*1024),                        // 256KB min
		kgo.FetchMaxWait(200*time.Millisecond),
	)

	consumer, err := kgo.NewClient(srcOpts...)
	if err != nil {
		logger.Error("create consumer failed", "err", err)
		return fmt.Errorf("create consumer: %w", err)
	}
	defer consumer.Close()

	// [FIX-FORGE Bug1] Track consecutive fetch errors to detect sustained failures.
	// If we hit too many in a row, return an error so the ring buffer gets closed
	// and the drainer doesn't hang forever waiting for records.
	consecutiveFetchErrors := 0
	const maxConsecutiveFetchErrors = 10

	for {
		if ctx.Err() != nil {
			return nil // context cancelled is not a fatal error
		}

		fetches := consumer.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				logger.Error("fetch error", "err", e.Err)
			}
			consecutiveFetchErrors++
			// [FIX-FORGE Bug1] Too many consecutive fetch errors → fatal failure.
			if consecutiveFetchErrors >= maxConsecutiveFetchErrors {
				return fmt.Errorf("fetcher %d: %d consecutive fetch errors, last: %v",
					id, consecutiveFetchErrors, errs[len(errs)-1].Err)
			}
		}

		records := fetches.Records()
		if len(records) == 0 {
			continue
		}

		// Reset consecutive errors on successful fetch with records
		consecutiveFetchErrors = 0

		for _, record := range records {
			if record.Offset >= toOffset {
				return nil // reached our segment boundary
			}
			// Put blocks if the ring buffer is full (backpressure from drainer)
			if !rb.Put(ctx, record.Offset, record) {
				return nil // buffer closed or context cancelled
			}
		}

		// Check if last record reached segment boundary
		if records[len(records)-1].Offset >= toOffset-1 {
			return nil
		}
	}
}

// drainPartition reads records from the ring buffer in strict offset order
// and produces them to the target. This goroutine is the serialization point
// that guarantees within-partition ordering.
//
// [FIX-FORGE Bug2] Produce errors now tracked via produceErrors counter.
// If any produce fails, the drainer logs and sends to DLQ (Bug3).
// [FIX-FORGE Bug3] Integrated retry with exponential backoff and DLQ fallback.
func (ps *ParallelSource) drainPartition(ctx context.Context, rb *OrderedRingBuffer, producer *kgo.Client, targetTopic string, sourceTopic string, partition int32, startOffset, endOffset int64) {
	var (
		inflight       sync.WaitGroup
		produceErrors  atomic.Int64 // [FIX-FORGE Bug2] Track produce errors
		periodBytes    int64        // bytes in current reporting period
		periodRecs     int64        // records in current reporting period
		totalBytes     int64        // total bytes for this partition
		totalRecs      int64        // total records for this partition
		lastLogTime    = time.Now()
		partStr        = fmt.Sprintf("%d", partition)
	)

	// Pre-cache metrics label values to avoid repeated allocations in hot path
	srcLabel := ps.cfg.Source
	tgtLabel := ps.cfg.Target

	for {
		if ctx.Err() != nil {
			break
		}

		batch, ok := rb.DrainBatch(ctx, 512) // drain up to 512 records at a time
		if !ok || len(batch) == 0 {
			break // buffer closed and drained
		}

		// Produce the entire batch in offset order
		for _, record := range batch {
			targetRecord := &kgo.Record{
				Topic:     targetTopic,
				Partition: partition,
				Key:       record.Key,
				Value:     record.Value,
				Headers:   record.Headers,
				Timestamp: record.Timestamp,
			}

			recBytes := int64(len(record.Value) + len(record.Key))
			periodBytes += recBytes
			periodRecs++
			totalBytes += recBytes
			totalRecs++

			// [FIX-FORGE Bug2+Bug3] Capture record metadata for retry/DLQ on failure
			srcOffset := record.Offset
			srcKey := record.Key
			srcValue := record.Value
			srcHeaders := record.Headers

			inflight.Add(1)
			producer.Produce(ctx, targetRecord, func(_ *kgo.Record, err error) {
				defer inflight.Done()
				if err != nil {
					ps.logger.Error("produce error in drain", "partition", partition, "offset", srcOffset, "err", err)
					ps.m.ProduceErrors.WithLabelValues(srcLabel, tgtLabel, targetTopic).Inc()

					// [FIX-FORGE Bug3] Retry the failed produce with exponential backoff
					retryErr := Retry(ctx, ps.retryCfg, func(retryCtx context.Context) error {
						ps.m.RetryAttempts.WithLabelValues(srcLabel, tgtLabel, "produce").Inc()
						retryRecord := &kgo.Record{
							Topic:     targetTopic,
							Partition: partition,
							Key:       srcKey,
							Value:     srcValue,
							Headers:   srcHeaders,
						}
						var produceErr error
						var retryWg sync.WaitGroup
						retryWg.Add(1)
						producer.Produce(retryCtx, retryRecord, func(_ *kgo.Record, e error) {
							defer retryWg.Done()
							produceErr = e
						})
						retryWg.Wait()
						return produceErr
					})

					if retryErr != nil {
						// [FIX-FORGE Bug2] Track the error
						produceErrors.Add(1)
						ps.m.RetryExhausted.WithLabelValues(srcLabel, tgtLabel, "produce").Inc()

						// [FIX-FORGE Bug3] Send to DLQ if available
						if ps.dlq != nil {
							ps.dlq.Send(ctx, sourceTopic, partition, srcOffset, srcKey, srcValue, srcHeaders, retryErr)
						}
						ps.logger.Error("produce failed after all retries, sent to DLQ",
							"partition", partition, "offset", srcOffset, "err", retryErr)
					}
				}
			})
		}

		// Periodic progress logging — batch-update Prometheus counters
		if time.Since(lastLogTime) > 10*time.Second {
			elapsed := time.Since(lastLogTime).Seconds()
			mbs := float64(periodBytes) / elapsed / 1024 / 1024
			ps.logger.Info("partition drain progress",
				"partition", partition,
				"period_records", periodRecs,
				"period_mb", periodBytes/1024/1024,
				"total_records", totalRecs,
				"throughput_mbs", fmt.Sprintf("%.1f", mbs),
				"ring_drain_offset", rb.DrainOffset(),
				"target_end", endOffset,
				"produce_errors", produceErrors.Load(),
			)
			// Batch-update Prometheus counters periodically instead of per-record
			ps.m.RecordsReplicated.WithLabelValues(srcLabel, tgtLabel, targetTopic, partStr).Add(float64(periodRecs))
			ps.m.BytesReplicated.WithLabelValues(srcLabel, tgtLabel, targetTopic, partStr).Add(float64(periodBytes))
			periodBytes = 0
			periodRecs = 0
			lastLogTime = time.Now()
		}
	}

	// Wait for all inflight produces to complete
	inflight.Wait()

	// Final metrics flush for remaining period
	if periodRecs > 0 {
		ps.m.RecordsReplicated.WithLabelValues(srcLabel, tgtLabel, targetTopic, partStr).Add(float64(periodRecs))
		ps.m.BytesReplicated.WithLabelValues(srcLabel, tgtLabel, targetTopic, partStr).Add(float64(periodBytes))
	}

	ps.totalBytes.Add(totalBytes)
	ps.totalRecords.Add(totalRecs)

	errCount := produceErrors.Load()
	ps.logger.Info("partition drain completed",
		"partition", partition,
		"total_records", totalRecs,
		"total_bytes_mb", totalBytes/1024/1024,
		"produce_errors", errCount,
	)
	if errCount > 0 {
		ps.logger.Warn("partition drain had produce errors — some records may have been sent to DLQ",
			"partition", partition, "error_count", errCount)
	}
}

// CatchUp is the legacy single-partition catch-up (kept for compatibility).
func (ps *ParallelSource) CatchUp(ctx context.Context, topic string, partition int32, startOffset, endOffset int64) error {
	return ps.CatchUpMultiPartition(ctx, map[int32][2]int64{
		partition: {startOffset, endOffset},
	}, topic)
}

// GetPartitionLag returns committed offset and HWM for a topic:partition.
//
// Deprecated: Use GetAllPartitionLags for batch queries that reuse a single admin client.
func GetPartitionLag(ctx context.Context, srcCfg config.ClusterConfig, group, topic string, partition int32) (int64, int64, error) {
	opts, err := kafka.BuildClientOpts(srcCfg)
	if err != nil {
		return 0, 0, err
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return 0, 0, err
	}
	defer client.Close()

	adm := kadm.NewClient(client)

	offsets, err := adm.FetchOffsets(ctx, group)
	if err != nil {
		return 0, 0, fmt.Errorf("fetch offsets: %w", err)
	}

	var committedOffset int64
	offsets.Each(func(o kadm.OffsetResponse) {
		if o.Topic == topic && o.Partition == partition {
			committedOffset = o.At
		}
	})

	endOffsets, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		return 0, 0, fmt.Errorf("list end offsets: %w", err)
	}

	var hwm int64
	endOffsets.Each(func(o kadm.ListedOffset) {
		if o.Topic == topic && o.Partition == partition {
			hwm = o.Offset
		}
	})

	return committedOffset, hwm, nil
}

// GetAllPartitionLags returns lag for all partitions of given topics using a single admin client.
func GetAllPartitionLags(ctx context.Context, srcCfg config.ClusterConfig, group string, topics []string) (map[string]map[int32][2]int64, error) {
	opts, err := kafka.BuildClientOpts(srcCfg)
	if err != nil {
		return nil, err
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	defer client.Close()
	adm := kadm.NewClient(client)

	offsets, err := adm.FetchOffsets(ctx, group)
	if err != nil {
		return nil, fmt.Errorf("fetch offsets: %w", err)
	}

	endOffsets, err := adm.ListEndOffsets(ctx, topics...)
	if err != nil {
		return nil, fmt.Errorf("list end offsets: %w", err)
	}

	result := make(map[string]map[int32][2]int64)
	endOffsets.Each(func(o kadm.ListedOffset) {
		hwm := o.Offset
		var committed int64
		offsets.Each(func(co kadm.OffsetResponse) {
			if co.Topic == o.Topic && co.Partition == o.Partition {
				committed = co.At
			}
		})
		if _, ok := result[o.Topic]; !ok {
			result[o.Topic] = make(map[int32][2]int64)
		}
		result[o.Topic][o.Partition] = [2]int64{committed, hwm}
	})
	return result, nil
}
