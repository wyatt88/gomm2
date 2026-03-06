# gomm2 — Kafka MirrorMaker 2, Rewritten in Go

[![Go](https://img.shields.io/badge/go-1.22+-00ADD8)](https://go.dev)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)

**gomm2** is a high-performance, pure-Go rewrite of Apache Kafka's MirrorMaker 2 (MM2). It replicates data, consumer group offsets, and heartbeats between Kafka clusters — without the JVM.

## Why?

| | Java MM2 | gomm2 |
|---|---|---|
| **Runtime** | JVM + Kafka Connect | Single static binary |
| **Memory** | 1-4 GB heap typical | 50-200 MB |
| **Startup** | 30-60s (framework init) | <1s |
| **Dependencies** | Kafka Connect, ZooKeeper-compatible | None (pure Go) |
| **Concurrency** | Thread pools | Goroutines + channels |
| **Config** | Java properties | YAML |
| **Deployment** | Kafka Connect cluster | Single binary / K8s pod |

## Architecture

```
┌─────────────────────────────────────────────────┐
│                    gomm2 Engine                  │
│                                                  │
│  ┌──────────┐  ┌───────────┐  ┌──────────────┐ │
│  │  Source   │  │ Heartbeat │  │  Checkpoint  │ │
│  │Replicator│  │  Emitter  │  │   Emitter    │ │
│  └────┬─────┘  └─────┬─────┘  └──────┬───────┘ │
│       │               │               │         │
│  ┌────┴───────────────┴───────────────┴───┐     │
│  │          Offset Sync Store             │     │
│  │   (exponentially-spaced sync array)    │     │
│  └────────────────────────────────────────┘     │
│                                                  │
│  ┌──────────┐  ┌──────────┐  ┌──────────────┐  │
│  │ /metrics │  │ /healthz │  │   /readyz    │  │
│  └──────────┘  └──────────┘  └──────────────┘  │
└─────────────────────────────────────────────────┘
        │                              │
        ▼                              ▼
  ┌──────────┐                  ┌──────────┐
  │  Source   │   franz-go      │  Target  │
  │  Kafka    │◄───────────────►│  Kafka   │
  │  Cluster  │                 │  Cluster │
  └──────────┘                  └──────────┘
```

## Core Components

### Source Replicator
Consumes from source, produces to target. Partition-aware, preserves message ordering. Supports at-least-once delivery with idempotent producer.

### Heartbeat Emitter
Periodic heartbeats to `heartbeats` topic on target. Monitors replication liveness and latency.

### Checkpoint Emitter
Translates consumer group offsets from source to target using the offset sync store. Enables seamless failover.

### Offset Sync Store
In-memory store with 64 exponentially-spaced sync points per partition (matching Java MM2's algorithm). Provides accurate offset translation with O(1) lookup.

## Quick Start

```bash
# Build
make build

# Validate config
./bin/gomm2 validate --config configs/minimal.yaml

# Run
./bin/gomm2 --config configs/minimal.yaml
```

## Docker

```bash
# Build image
make docker

# Run
docker run -v ./config.yaml:/etc/gomm2/config.yaml gomm2:dev
```

## Kubernetes

```bash
kubectl apply -f deploy/kubernetes/configmap.yaml
kubectl apply -f deploy/kubernetes/deployment.yaml
```

## Configuration

See [configs/example.yaml](configs/example.yaml) for full configuration reference.

Minimal config:
```yaml
clusters:
  source:
    bootstrap_servers: ["source:9092"]
  target:
    bootstrap_servers: ["target:9092"]
replications:
  - source: source
    target: target
    enabled: true
metrics:
  enabled: true
  address: ":9090"
```

## Monitoring

Prometheus metrics at `/metrics`:

| Metric | Type | Description |
|--------|------|-------------|
| `gomm2_records_replicated_total` | Counter | Records replicated per topic/partition |
| `gomm2_bytes_replicated_total` | Counter | Bytes replicated |
| `gomm2_replication_lag` | Gauge | Per-partition replication lag |
| `gomm2_record_age_seconds` | Histogram | Age of replicated records |
| `gomm2_heartbeat_latency_seconds` | Histogram | Heartbeat round-trip latency |
| `gomm2_checkpoints_emitted_total` | Counter | Checkpoints emitted |
| `gomm2_produce_errors_total` | Counter | Produce errors |

Health checks:
- `/healthz` — Liveness (always 200 when running)
- `/readyz` — Readiness (200 when all sync stores initialized)

## Performance

Compared to Java MM2:

- **~10x less memory** — no JVM overhead, no Kafka Connect framework
- **~50x faster startup** — single binary, no class loading
- **Lower tail latency** — Go GC is sub-millisecond; JVM GC can pause 50-200ms
- **goroutine-per-partition** — 10,000+ partitions with minimal overhead
- **Zero-copy forwarding** — records forwarded as byte slices, no serde overhead

## Kafka Client

Built on [franz-go](https://github.com/twmb/franz-go) — a pure Go Kafka client with:
- No CGO / librdkafka dependency
- Complete Kafka protocol support
- Comparable throughput to librdkafka
- Built-in connection pooling and batching

## License

Apache License 2.0 — same as Apache Kafka.
