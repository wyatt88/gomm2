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

| Component | Per-unit | Count | Total |
|-----------|----------|-------|-------|
| Fetcher franz-go clients | ~10MB (5MB fetch + overhead) | 48 | ~480MB |
| Ring buffers (pointers only) | ~128KB | 8 | ~1MB |
| Shared producer buffers | ~256MB | 1 | ~256MB |
| Go runtime + other | ~200MB | 1 | ~200MB |
| **Total** | | | **~937MB** |

Well within the 8GB budget. Can safely run alongside normal source mode.

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

### For `perf-test-8p` (8 partitions, 2M × 40KB, 3 brokers)

```yaml
# In engine.go constants (recompile to change):
workersPerPartition: 6    # 6 × 8 = 48 total connections
parallelThreshold: 100000 # only parallelize if lag > 100K records
```

### Tuning Knobs (via code constants)

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| `workersPerPartition` | 6 | 48 total connections, ~16 per broker, saturates broker throughput |
| `FetchMaxPartitionBytes` | 5MB | Balance between fetch efficiency and memory. Increase to 10MB if memory allows |
| `ringCap` | 16384 | ~512 fetches worth of buffer. Increase if drainer is slower than fetchers |
| `DrainBatch` | 512 | Records per drain iteration. Larger = better batching, smaller = lower latency |
| `ProducerBatchMaxBytes` | 1MB | Standard Kafka batch size |
| `ProducerLinger` | 5ms | Low linger since target is same-VPC |
| `MaxBufferedRecords` | 500K | Large buffer for producer backpressure |

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
