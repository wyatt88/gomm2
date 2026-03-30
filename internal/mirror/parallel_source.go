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
	workers int

	totalBytes   atomic.Int64
	totalRecords atomic.Int64
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
	return &ParallelSource{
		cfg:     cfg,
		srcCfg:  srcCfg,
		tgtCfg:  tgtCfg,
		policy:  pol,
		m:       m,
		logger:  logger.With("component", "parallel_source", "flow", cfg.Source+"->"+cfg.Target),
		workers: workers,
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
	ringCap := 16384
	if totalRange < int64(ringCap) {
		ringCap = int(totalRange) + 1
	}
	rb := NewOrderedRingBuffer(ringCap, startOffset)

	targetTopic := ps.policy.FormatRemoteTopic(ps.cfg.Source, topic)

	// Start the drainer goroutine — reads from ring buffer in offset order,
	// produces to target maintaining strict ordering.
	var drainerWg sync.WaitGroup
	drainerWg.Add(1)
	go func() {
		defer drainerWg.Done()
		ps.drainPartition(ctx, rb, producer, targetTopic, partition, startOffset, endOffset)
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
			ps.fetchWorker(ctx, rb, id, topic, partition, from, to)
		}(i, segStart, segEnd)
	}

	// Wait for all fetchers to finish, then close the ring buffer
	fetcherWg.Wait()
	rb.Close()

	// Wait for drainer to finish flushing
	drainerWg.Wait()
}

// fetchWorker consumes records from a source partition in the range [fromOffset, toOffset)
// and inserts them into the ring buffer. Each worker has its own franz-go client
// (= its own TCP connection) for maximum parallelism across the cross-region link.
func (ps *ParallelSource) fetchWorker(ctx context.Context, rb *OrderedRingBuffer, id int, topic string, partition int32, fromOffset, toOffset int64) {
	logger := ps.logger.With("fetcher", id, "partition", partition, "from", fromOffset, "to", toOffset)

	srcOpts, err := kafka.BuildClientOpts(ps.srcCfg)
	if err != nil {
		logger.Error("build source opts failed", "err", err)
		return
	}

	// Tuning per-worker fetch parameters:
	// - FetchMaxPartitionBytes: 5MB keeps memory bounded. At 30ms RTT this gives
	//   ~166 MB/s theoretical per connection. With 6 workers per partition × 8
	//   partitions = 48 connections, theoretical aggregate = 48 × 166 = ~8 GB/s
	//   (far more than broker or network limit).
	// - FetchMinBytes: 256KB reduces empty fetches without adding latency.
	// - FetchMaxWait: 200ms — don't wait too long if data is sparse.
	srcOpts = append(srcOpts,
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			topic: {partition: kgo.NewOffset().At(fromOffset)},
		}),
		kgo.FetchMaxBytes(10*1024*1024),           // 10MB overall per fetch
		kgo.FetchMaxPartitionBytes(5*1024*1024),   // 5MB per partition — reduced for memory
		kgo.FetchMinBytes(256*1024),                // 256KB min
		kgo.FetchMaxWait(200*time.Millisecond),
	)

	consumer, err := kgo.NewClient(srcOpts...)
	if err != nil {
		logger.Error("create consumer failed", "err", err)
		return
	}
	defer consumer.Close()

	for {
		if ctx.Err() != nil {
			break
		}

		fetches := consumer.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				logger.Error("fetch error", "err", e.Err)
			}
		}

		records := fetches.Records()
		if len(records) == 0 {
			continue
		}

		for _, record := range records {
			if record.Offset >= toOffset {
				return // reached our segment boundary
			}
			// Put blocks if the ring buffer is full (backpressure from drainer)
			if !rb.Put(ctx, record.Offset, record) {
				return // buffer closed or context cancelled
			}
		}

		// Check if last record reached segment boundary
		if records[len(records)-1].Offset >= toOffset-1 {
			return
		}
	}
}

// drainPartition reads records from the ring buffer in strict offset order
// and produces them to the target. This goroutine is the serialization point
// that guarantees within-partition ordering.
func (ps *ParallelSource) drainPartition(ctx context.Context, rb *OrderedRingBuffer, producer *kgo.Client, targetTopic string, partition int32, startOffset, endOffset int64) {
	var (
		inflight       sync.WaitGroup
		periodBytes    int64 // bytes in current reporting period
		periodRecs     int64 // records in current reporting period
		totalBytes     int64 // total bytes for this partition
		totalRecs      int64 // total records for this partition
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

			inflight.Add(1)
			producer.Produce(ctx, targetRecord, func(_ *kgo.Record, err error) {
				defer inflight.Done()
				if err != nil {
					ps.logger.Error("produce error in drain", "partition", partition, "err", err)
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
	ps.logger.Info("partition drain completed",
		"partition", partition,
		"total_records", totalRecs,
		"total_bytes_mb", totalBytes/1024/1024,
	)
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
