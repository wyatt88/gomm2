# DESIGN.md — gomm2 Technical Design Document

## Overview

gomm2 is a Golang rewrite of Apache Kafka MirrorMaker 2 (MM2), the official cross-cluster replication tool introduced in Kafka 2.4 (KIP-382). This document explains the design decisions, architecture differences, and trade-offs compared to the Java original.

## Java MM2 Architecture

Java MM2 is built on the **Kafka Connect** framework:

```
MirrorMaker (main)
  └── MirrorHerder (per source→target flow)
       └── Connect Worker
            ├── MirrorSourceConnector → MirrorSourceTask(s)
            ├── MirrorCheckpointConnector → MirrorCheckpointTask(s)
            └── MirrorHeartbeatConnector → MirrorHeartbeatTask
```

**Strengths:** Distributed, fault-tolerant, leverages Connect ecosystem.
**Weaknesses:** Heavy (~1-4GB RAM), slow startup (30-60s), complex deployment (requires Connect cluster), JVM GC pauses.

## gomm2 Architecture

gomm2 replaces the entire Kafka Connect layer with a single Go binary:

```
Engine (main orchestrator)
  ├── Source (per flow)     — goroutine-based consumer → producer pipeline
  ├── Heartbeat (per flow)  — periodic heartbeat emitter
  ├── Checkpoint (per flow) — periodic offset translation emitter
  └── SyncStore (per flow)  — in-memory offset sync store
```

### Why Go?

1. **No JVM overhead** — Go compiles to a static binary. No class loading, no JIT warmup, no GC pauses measured in tens of milliseconds.

2. **Goroutines vs Threads** — Java MM2 uses thread pools via Kafka Connect's worker model. Go's goroutines are ~2KB stack (vs ~1MB for Java threads), enabling thousands of concurrent partition handlers without OS thread exhaustion.

3. **Channels as pipelines** — Go channels provide natural backpressure between consumer and producer stages, replacing Java's complex queue + semaphore patterns (see `MirrorSourceTask.consumerAccess`).

4. **Fast startup** — Go binary starts in <1s. No Kafka Connect framework initialization, no Herder election, no config store bootstrap.

5. **Minimal memory** — 50-200MB typical vs 1-4GB for JVM. Critical for edge deployments and cost-conscious environments.

### Why franz-go?

| | confluent-kafka-go | franz-go | segmentio/kafka-go |
|---|---|---|---|
| **Implementation** | CGO wrapper (librdkafka) | Pure Go | Pure Go |
| **CGO required** | Yes | No | No |
| **Transactions** | Yes | Yes | No |
| **Protocol coverage** | Complete | Complete | Partial |
| **Performance** | Excellent (C) | Excellent (Go) | Good |
| **Cross-compilation** | Hard (CGO) | Easy | Easy |

franz-go was chosen because:
- **Pure Go** — no CGO means easy cross-compilation, Alpine containers, reproducible builds
- **Feature-complete** — transactions, exactly-once, admin APIs, all Kafka protocol versions
- **Performance** — benchmarks show throughput comparable to librdkafka
- **Active maintenance** — regularly updated, tracks latest Kafka protocol versions

## Component Design

### Source Replicator

**Java approach:** `MirrorSourceConnector` discovers topics and partitions, then divides them among `MirrorSourceTask` instances via round-robin. Each task runs in a Connect worker thread, calling `poll()` to consume and returning `SourceRecord` objects for the framework to produce.

**Go approach:** A single `Source` struct manages the full pipeline:
1. Creates a franz-go consumer with direct partition assignment
2. Polls records in a tight loop (one goroutine)
3. For each record, creates a target record with remapped topic name
4. Produces asynchronously with callbacks for offset sync tracking
5. Uses `kgo.ManualPartitioner` to preserve source partition assignment

**Key difference:** No Connect framework overhead. The consumer→producer pipeline is a direct goroutine loop with async produce. Java MM2's poll/commit cycle goes through multiple abstraction layers (SourceTask → WorkerSourceTask → Producer).

### Offset Sync Store

The offset sync store is the most algorithmically complex component. It maintains a mapping between upstream (source) and downstream (target) offsets for each topic-partition.

**Java implementation** (`OffsetSyncStore.java`, 340 lines):
- Uses a fixed-size array of 64 `OffsetSync` entries per partition (one per bit of `long`)
- Maintains exponentially-spaced syncs with invariants:
  - A: `syncs[0]` is always the latest
  - B: `syncs[i].upstream <= syncs[j].upstream + 2^j - 2^i` (for i < j)
  - C: `syncs[i].upstream >= syncs[j].upstream + 2^(i-2)` (for i < j)
  - D: `syncs[63]` is the earliest usable sync
- Uses `ConcurrentHashMap` with immutable array snapshots for thread safety

**Go implementation** (`sync_store.go`):
- Same 64-entry array, same invariants
- Uses `sync.RWMutex` for read-heavy workload (translations are reads)
- Value-type arrays (`[64]OffsetSync`) — copied on write, no allocation on read
- Simpler update logic leveraging Go's value semantics

**Translation algorithm** (identical in both):
1. Linear search from `syncs[0]` (latest) to `syncs[63]` (earliest)
2. Find the first sync where `sync.upstream <= targetOffset`
3. Return `sync.downstream + (1 if consumer is ahead, else 0)`

This provides O(log N) accuracy with O(1) lookup — the exponential spacing ensures syncs cover the full offset range with 64 points.

### Heartbeat Emitter

**Java:** `MirrorHeartbeatConnector` creates a single `MirrorHeartbeatTask` that emits a `SourceRecord` with serialized `Heartbeat` (source cluster, target cluster, timestamp).

**Go:** A goroutine with a `time.Ticker` produces heartbeat records directly via franz-go. Serialization uses the same wire format (length-prefixed strings + int64 timestamp) for compatibility with Java MM2 consumers.

### Checkpoint Emitter

**Java:** `MirrorCheckpointConnector` discovers consumer groups, assigns them to tasks. Each `MirrorCheckpointTask` periodically fetches group offsets, translates via `OffsetSyncStore`, and emits `Checkpoint` records.

**Go:** Single goroutine per flow. Uses `kadm.Client` (franz-go admin) to list groups and fetch offsets. Translates via the shared `SyncStore` and produces checkpoint records. No need for Connect's task distribution — Go handles the concurrency natively.

## Configuration

Java MM2 uses `.properties` files with a flat key structure and per-cluster prefixing:
```properties
clusters = primary, backup
primary.bootstrap.servers = vip1:9092
primary->backup.enabled = true
primary->backup.topics = .*
```

gomm2 uses structured YAML:
```yaml
clusters:
  primary:
    bootstrap_servers: ["vip1:9092"]
replications:
  - source: primary
    target: backup
    enabled: true
    topic_filter:
      whitelist: [".*"]
```

**Rationale:** YAML is more readable, supports complex nesting, and is the de-facto standard for Go/K8s ecosystem tooling.

## Topic Naming & Replication Policy

Both implementations support the same policies:

1. **DefaultPolicy** — prepends source cluster alias: `us-west.orders`
2. **IdentityPolicy** — passes topic name unchanged (risk of cycles)

Wire formats for internal topics (`heartbeats`, `checkpoints`, `mm2-offset-syncs`) are binary-compatible with Java MM2 for interoperability.

## Performance Comparison (Expected)

| Metric | Java MM2 | gomm2 (expected) |
|--------|----------|-------------------|
| Memory (100 topics) | 1-2 GB | 100-200 MB |
| Startup time | 30-60s | <1s |
| P99 latency | 10-50ms (+GC pauses) | 5-20ms |
| Max throughput/instance | ~500K msg/s | ~1M msg/s |
| Max partitions/instance | ~1,000 | ~10,000+ |
| Binary size | N/A (JVM) | ~15 MB |
| Container image | ~500 MB | ~20 MB |

*(Benchmarks pending — these are estimates based on franz-go benchmarks and similar Go↔Java Kafka tool comparisons)*

## Migration Guide (Java MM2 → gomm2)

### 1. Configuration Translation

Map your `mm2.properties` to `config.yaml`:

| Java MM2 Property | gomm2 YAML Path |
|---|---|
| `clusters = A, B` | `clusters: {A: ..., B: ...}` |
| `A.bootstrap.servers` | `clusters.A.bootstrap_servers` |
| `A->B.enabled` | `replications[i].enabled` |
| `A->B.topics` | `replications[i].topic_filter.whitelist` |
| `replication.factor` | `replications[i].replication_factor` |
| `refresh.topics.interval.seconds` | `replications[i].refresh_topics_interval` |
| `emit.heartbeats.enabled` | `replications[i].emit_heartbeats` |
| `emit.checkpoints.enabled` | `replications[i].emit_checkpoints` |

### 2. Internal Topic Compatibility

gomm2 uses the same internal topic names and wire formats:
- `heartbeats` — heartbeat records
- `<source>.checkpoints.internal` — checkpoint records
- `mm2-offset-syncs.<cluster>.internal` — offset syncs

You can run gomm2 alongside Java MM2 during migration.

### 3. Consumer Group Offset

gomm2 uses its own consumer group (`gomm2-source-<source>-<target>`). When migrating:
1. Stop Java MM2
2. Note the last committed offsets
3. Start gomm2 — it will begin from the earliest uncommitted offset
4. Monitor via `/metrics` endpoint

### 4. Monitoring

Replace Connect JMX metrics with Prometheus metrics at `/metrics`. Key mappings:

| Java MM2 JMX | gomm2 Prometheus |
|---|---|
| `kafka.connect.mirror:type=MirrorSourceConnector,*` | `gomm2_records_replicated_total` |
| `kafka.connect.mirror:type=MirrorSourceConnector,*` bytes | `gomm2_bytes_replicated_total` |
| `kafka.connect.mirror:type=MirrorCheckpointConnector,*` | `gomm2_checkpoints_emitted_total` |

## Future Work

1. **Exactly-once semantics** — Transactional produce with franz-go transaction support
2. **ACL replication** — Sync topic ACLs between clusters
3. **Topic config sync** — Incremental config changes
4. **Multi-instance coordination** — Distributed partition assignment (etcd/K8s lease)
5. **Admin CLI** — `gomm2 status` showing per-partition lag via admin API
6. **Web UI** — Optional dashboard for monitoring replication flows
