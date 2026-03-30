# gomm2 Performance Optimization Notes (v2)

## Architecture: Ring-Buffer Pipelined Parallel Catch-Up

### Previous Architecture (v1)
```
Per partition:
  N workers → each independently: fetch → produce → wait
  No ordering guarantee between workers
  Each worker has its own producer OR shared producer with fire-and-forget
```

**Problems:**
- No within-partition ordering guarantee (workers could produce out of order)
- Per-record Prometheus `WithLabelValues()` calls in hot path (~500ns each × 2M records = 1s overhead)
- 128 workers OOM'd due to each franz-go client allocating 20-50MB fetch buffers
- 32 workers achieved ~280 MB/s network / ~200 MB/s app-layer

### New Architecture (v2)
```
Per partition:
  N fetcher goroutines → OrderedRingBuffer(16384 slots) → 1 drainer goroutine → shared producer

Across all partitions:
  (8 partitions × 6 fetchers) = 48 TCP connections to source brokers
  1 shared producer to target cluster (same VPC, <1ms RTT)
```

**Key design decisions:**

1. **OrderedRingBuffer per partition**: Decouples parallel fetching from in-order production.
   Each fetcher `Put()`s records into the ring buffer indexed by source offset. The single
   drainer goroutine calls `DrainBatch()` which yields records in strict source-offset order.
   This guarantees within-partition ordering with zero coordination between fetchers.

2. **Shared producer**: One `kgo.Client` for all workers across all partitions. With `<1ms` RTT
   to target (same VPC), producer is never the bottleneck. Shared client maximizes batching
   (1MB batches, 5ms linger) and connection reuse.

3. **6 workers per partition**: Each worker has its own TCP connection to source. With 8 partitions
   across 3 brokers, this gives 48 total connections, ~16 per broker. Each connection can sustain
   ~10-15 MB/s after BBR cwnd ramp-up. Theoretical: 48 × 12 MB/s = 576 MB/s (matching broker limit).

4. **5MB FetchMaxPartitionBytes per worker**: Reduced from 10MB to control memory.
   48 workers × 5MB = 240MB fetch buffer budget. With ring buffer pointers (not copies)
   and producer buffers (~256MB max), total memory stays under 600MB.

5. **Batched Prometheus metrics**: Instead of per-record `WithLabelValues()` calls, we accumulate
   counters locally and flush every 10 seconds. Eliminates ~1μs per record of mutex+map overhead.

### Data Flow

```
Source Kafka (SIN, 30ms RTT)
    ↓
[Fetcher-0] offset [0, 250K)    ─┐
[Fetcher-1] offset [250K, 500K) ─┤
[Fetcher-2] offset [500K, 750K) ─┼→ OrderedRingBuffer(16384) → [Drainer] → Shared Producer
[Fetcher-3] offset [750K, 1M)   ─┤                                              ↓
[Fetcher-4] offset [1M, 1.25M)  ─┤                                    Target Kafka (BKK, <1ms)
[Fetcher-5] offset [1.25M, 1.5M)─┘
    (× 8 partitions)
```

## Theoretical Throughput Calculation

### Source-side (fetch from SIN)

```
Per TCP connection:
  BDP = bandwidth × RTT
  With BBR, steady-state cwnd ≈ BDP / MSS
  
  After ramp-up (~10s with BBR):
    cwnd ≈ 1500 (BBR will probe up to this)
    MSS = 8448 (jumbo frames MTU 9001)
    Throughput = cwnd × MSS / RTT = 1500 × 8448 / 0.030 = 422 MB/s per connection (theoretical)
    
  Realistic per-connection (Kafka fetch overhead, protocol framing):
    ~10-20 MB/s per connection after ramp-up

Per broker:
  kafka.m5.large max throughput: ~188 MB/s
  Connections per broker: ~16 (48 total / 3 brokers)
  16 × 15 MB/s = 240 MB/s per broker (exceeds broker limit)
  
Total source:
  3 brokers × 188 MB/s = 564 MB/s theoretical max
  With overhead: ~400-500 MB/s realistic
```

### Target-side (produce to BKK)

```
Same VPC, <1ms RTT
1 shared producer, 20 inflight requests per broker, 1MB batches
Throughput = 20 × 1MB / 0.001s = 20 GB/s per broker (never the bottleneck)
```

### Network

```
EC2 m7i.2xlarge: 12.5 Gbps baseline = ~1.5 GB/s
Source fetch: ~500 MB/s
Target produce: ~500 MB/s
Total: ~1 GB/s network (within 12.5 Gbps)
```

### Bottleneck Analysis

```
Source brokers: 564 MB/s (3 × 188 MB/s)  ← LIMITING FACTOR
EC2 network:   1500 MB/s (12.5 Gbps)
Target brokers: unlimited (same VPC)
CPU: ~20% at 500 MB/s (mostly in compression)
Memory: ~600MB (well under 8GB budget)
```

**Expected throughput: 400-564 MB/s** (2-3× improvement over v1's 200 MB/s)

The hard ceiling is the source MSK broker throughput (kafka.m5.large × 3).
To exceed 564 MB/s, you'd need larger source brokers (kafka.m5.xlarge = 375 MB/s × 3 = 1.1 GB/s).

## Memory Budget

### Original Estimate (v2, workers_per_partition=6)

| Component | Per-unit | Count | Total |
|-----------|----------|-------|-------|
| Fetcher franz-go clients | ~10MB (5MB fetch + overhead) | 48 | ~480MB |
| Ring buffers (pointers only) | ~128KB | 8 | ~1MB |
| Shared producer buffers | ~256MB | 1 | ~256MB |
| Go runtime + other | ~200MB | 1 | ~200MB |
| **Total (estimated)** | | | **~937MB** |

### ⚠️ Actual Measurement (300GB / 80M records test, 2026-03-30)

| Phase | RSS | Notes |
|-------|-----|-------|
| Catch-up start (+15s) | 6.4GB | 48 workers spinning up |
| Catch-up peak (+18min) | **6.9GB** | All 48 workers + producer active |
| 5/8 partitions done (+42min) | 6.2GB | Drained partitions releasing buffers |
| 7/8 partitions done (+52min) | 5.0GB | Near completion |
| Tail (+54min) | 4.9GB | Only 1 partition still running |

**Estimate was off by 7.4×.** Root cause analysis:

| Component | Estimated | Actual (inferred) | Why |
|-----------|-----------|-------------------|-----|
| franz-go fetch buffers per worker | ~10MB | ~50-100MB | franz-go allocates internal response buffers, decompression buffers, and record slices beyond FetchMaxPartitionBytes |
| 48 workers × actual buffer | 480MB | **2.4-4.8GB** | This is the dominant memory consumer |
| Ring buffer record pointers | ~1MB | ~488MB | Slots hold `*kgo.Record` but the pointed-to data resides in fetch buffers; overlap with above |
| MaxBufferedRecords=500K | (not counted) | up to **1.87GB** | 500K × 3.75KB per record if target has backpressure |
| Go runtime (GC metadata, goroutine stacks) | ~200MB | ~300-500MB | 48+ goroutines, large heap, GOMAXPROCS=2 limited GC parallelism |
| **Total** | **937MB** | **6.9GB** | |

### Revised Memory Model (v2.1)

The key insight: **each franz-go consumer allocates significantly more than just `FetchMaxPartitionBytes`**.
Internal buffers include: fetch response buffer, decompression buffer, record slice header pool,
and topic/partition metadata. Empirically, each consumer uses ~100-140MB at peak.

**Formula:**

```
Memory ≈ (workers_per_partition × partitions × franz_go_per_consumer)
        + producer_buffers
        + go_runtime_overhead

Where:
  franz_go_per_consumer ≈ 100-140MB (empirical, depends on record size and fetch rate)
  producer_buffers ≈ 256-512MB (MaxBufferedRecords × avg_record_size)
  go_runtime_overhead ≈ 300-500MB (GC metadata, stacks, internal structures)
```

### Recommended Configurations

| Scenario | workers_per_partition | Partitions | Total Workers | Est. Memory | K8s Limit |
|----------|----------------------|------------|---------------|-------------|-----------|
| **Small** (≤50GB, ≤4 partitions) | 2 | 4 | 8 | ~2GB | 4Gi |
| **Medium** (50-300GB, 8 partitions) | 4 | 8 | 32 | ~4-5GB | 8Gi |
| **Large** (>300GB, 16+ partitions) | 3 | 16 | 48 | ~6-7GB | 10Gi |
| **XL** (>1TB, 32+ partitions) | 2 | 32 | 64 | ~8-10GB | 16Gi |

> **Rule of thumb:** `workers_per_partition × partitions × 140MB + 1GB overhead`.
> Set K8s memory limit to 1.2× this value. Set GOMEMLIMIT to 75% of K8s limit.

### New Default: workers_per_partition=4 (was 6)

Reducing from 6 to 4 workers per partition:
- **Memory savings:** 32 workers instead of 48 → ~2GB less fetch buffer memory
- **Throughput impact:** Minimal. With 4 workers × 8 partitions = 32 connections,
  still ~10-11 per broker. At 15 MB/s per connection = ~480 MB/s aggregate,
  still saturates kafka.m5.large × 3 (564 MB/s limit).
- **Configurable:** `workers_per_partition` is now in YAML config, not hardcoded.

## Data Integrity Guarantees

### 1. Within-Partition Ordering ✅
The `OrderedRingBuffer` is the key mechanism. Fetchers write records at their source offset
position. The drainer reads records sequentially starting from `startOffset`, advancing by 1
each time. Records can only be drained in strict offset order. If a fetcher is slow and leaves
a gap, the drainer blocks until that offset is filled. The drainer is the ONLY goroutine that
produces to the target for that partition, so records arrive in order.

### 2. Completeness ✅
Each fetcher is assigned a non-overlapping offset range `[from, to)`. The union of all fetcher
ranges exactly covers `[startOffset, endOffset)`. The ring buffer blocks the drainer if any
offset is missing (it won't advance past a gap). A fetcher only terminates after it has Put()
all records up to its segment boundary.

### 3. At-Least-Once Semantics ✅
The producer uses `AllISRAcks()` and does not use idempotent write (so duplicates are possible
on retries). On crash, the catch-up will restart from the last committed consumer group offset,
re-replicating any records that were produced but not committed. This guarantees at-least-once.

### 4. Offset Commit Safety ✅
During parallel catch-up, consumer group offsets are NOT committed (there is no consumer group
in catch-up mode — it uses direct partition assignment). After catch-up completes, the normal
source starts with the consumer group, which will pick up from the last committed offset.
The engine's Phase 2→Phase 3 transition ensures catch-up completes before normal source starts.

### 5. Offset Sync Mapping ✅
Offset syncs are NOT emitted during catch-up (the parallel source doesn't track them). The
normal source emits offset syncs during steady-state replication. For catch-up data, the
checkpoint emitter will eventually produce checkpoints once the sync store has enough data points.

## Configuration Recommendations

### For `perf-test-8p` (8 partitions, ~3.75KB avg records, 3 brokers)

```yaml
# In config.yaml under the replication entry:
workers_per_partition: 4       # 4 × 8 = 32 total connections (was 6/48)
fetch_max_partition_bytes: 5242880  # 5MB per worker fetch
ring_buffer_capacity: 16384    # slots per partition ring buffer
```

### Environment variables (set in K8s deployment or runtime)

```bash
GOMAXPROCS=4       # match CPU limit, enables GC parallelism
GOMEMLIMIT=6GiB    # 75% of 8Gi K8s limit, prevents OOM while allowing Go GC to work efficiently
```

### Tuning Knobs (now configurable via YAML)

| Parameter | Default | Range | Rationale |
|-----------|---------|-------|-----------|
| `workers_per_partition` | 4 | 2-8 | More workers = higher throughput but more memory. ~140MB per worker. |
| `fetch_max_partition_bytes` | 5MB | 1-10MB | Larger = fewer fetches but more memory per worker |
| `ring_buffer_capacity` | 16384 | 4096-65536 | Larger = more burst absorption, more memory |
| `ProducerBatchMaxBytes` | 1MB | — | Standard Kafka batch size (hardcoded) |
| `ProducerLinger` | 5ms | — | Low linger since target is same-VPC (hardcoded) |
| `MaxBufferedRecords` | 500K | — | Large buffer for producer backpressure (hardcoded) |

### For Larger Deployments

If source brokers are upgraded (e.g., kafka.m5.xlarge):
- Increase `workersPerPartition` to 8-10
- Increase `FetchMaxPartitionBytes` to 10MB
- Monitor memory usage with `go tool pprof`

If EC2 instance is upgraded (e.g., m7i.4xlarge with 25 Gbps):
- The network is already not the bottleneck
- Focus on source broker throughput

### OS-Level Tuning (already applied)

```bash
# TCP BBR congestion control
sysctl net.core.default_qdisc=fq
sysctl net.ipv4.tcp_congestion_control=bbr

# Large receive buffers for cross-region
sysctl net.core.rmem_max=16777216
sysctl net.ipv4.tcp_rmem="4096 131072 16777216"

# Jumbo frames (MTU 9001, MSS 8448)
# Configured at EC2 instance level
```

## Changes Summary

### `internal/mirror/parallel_source.go` (MAJOR REWRITE)
- **Ring-buffer pipeline**: Fetchers → OrderedRingBuffer → Drainer → Producer
- **Strict ordering guarantee** via single-drainer-per-partition design
- **Memory-efficient**: 5MB FetchMaxPartitionBytes per worker (was 10MB)
- **Batched metrics**: Prometheus counters flushed every 10s (was per-record)
- **Configurable ring buffer capacity**: 16384 slots default

### `internal/mirror/ring_buffer.go` (BUG FIX)
- Fixed bogus CAS in `Put()` (was `CompareAndSwap(empty, empty)` — no-op)
- Fast path: if slot is empty, write and signal without taking the lock

### `internal/mirror/source.go` (OPTIMIZATION)
- **Batched Prometheus metrics**: Local counters flushed every 5s instead of per-record
- **Pre-cached label values**: `srcLabel`/`tgtLabel` computed once, not per iteration
- Removed per-record `PollBatchSize` observation (moved to periodic flush)

### `internal/mirror/engine.go` (TUNING)
- Increased `workersPerPartition` from 4 to 6 (48 total connections)
- Added detailed comments on worker allocation math

### `internal/offset/sync_store_test.go` & `translation_test.go` (TEST FIX)
- Renamed `sync()` helper to `makeSync()` to avoid shadowing `sync` package import
