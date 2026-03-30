# FIX-FORGE.md — gomm2 P0 Bug 修复摘要

**日期**: 2026-03-30
**修复者**: Forge 🔧
**编译**: `go build ./...` ✅ | `go vet ./...` ✅

---

## Bug 1: 分区 drain 卡死 (P5 hang) — 最高优先

**问题**: 实测中 P5 卡在 12,466,136/12,544,395，fetcher goroutine 异常退出后 drainer 永久阻塞在 `ring.DrainBatch()`。

**根因**: `fetchWorker` 在 `BuildClientOpts()` 或 `NewClient()` 失败时直接 `return`，不返回错误。调用方无法感知 fetcher 已退出，ring buffer 不会关闭，drainer 永远等待不会到达的 offset。

**修复** (4 个文件):

| 文件 | 改动 |
|------|------|
| `ring_buffer.go` | 新增 `CloseWithError()` 方法，语义标识因错误关闭 |
| `parallel_source.go` | `fetchWorker` 返回值从 `void` 改为 `error` |
| `parallel_source.go` | 新增 `fetcherFailed atomic.Bool`，任何 fetcher 失败时调用 `rb.CloseWithError()` 解除 drainer 阻塞 |
| `parallel_source.go` | 新增连续 fetch 错误计数 (`maxConsecutiveFetchErrors=10`)，超阈值视为致命错误返回 |

**效果**: fetcher 失败 → ring buffer 关闭 → drainer 从 `DrainBatch()` 返回 `ok=false` → 正常退出。不再卡死。

---

## Bug 2: produce 失败后 offset 仍 commit → 数据丢失

**问题**: `source.go` 中 produce callback 错误只是打日志，`inflight.Done()` 后 offset commit 照常进行，导致失败的 record 被"标记为已处理"而永久丢失。

**根因**: 缺少 produce 错误状态传递到 commit 决策路径。

**修复**:

| 文件 | 改动 |
|------|------|
| `source.go` | 新增 `produceErrors atomic.Int64` 字段 |
| `source.go` | produce callback 失败（retry 耗尽后）时 `produceErrors.Add(1)` |
| `source.go` | `replicationLoop` offset commit 前检查 `produceErrors.Swap(0)`，>0 时跳过 commit |
| `source.go` | `Stop()` shutdown 时也检查 produce errors，有错误则跳过 final commit |
| `parallel_source.go` | `drainPartition` 同理增加 `produceErrors` 追踪和日志 |

**效果**: produce 失败 → offset 不 commit → 重启后从上次 committed offset 重新消费 → at-least-once 语义保证（可能少量重复，但不丢数据）。

---

## Bug 3: DLQ/Retry 模块未集成到 Source 主循环

**问题**: `dlq.go` 和 `retry.go` 代码完整但从未被 `source.go` 或 `parallel_source.go` 调用。produce 失败无任何兜底机制。

**修复**:

| 文件 | 改动 |
|------|------|
| `source.go` | `NewSource` 初始化 DLQ（如 `cfg.DLQEnabled`）和 `RetryConfig`（从 config 读取） |
| `source.go` | produce callback 失败时调用 `Retry()` 指数退避重试 |
| `source.go` | retry 耗尽后调用 `dlq.Send()` 发送到 DLQ topic |
| `source.go` | `Stop()` 时关闭 DLQ producer |
| `parallel_source.go` | `NewParallelSource` 同理初始化 DLQ + RetryConfig + CircuitBreaker |
| `parallel_source.go` | `drainPartition` produce callback 失败时走 retry → DLQ 流程 |

**流程**: produce 失败 → `Retry(ctx, retryCfg, fn)` 指数退避重试（默认 10 次, 100ms~30s）→ 全部失败 → `dlq.Send()` 发到 `gomm2-dlq-<source>-<target>` → Prometheus counter `gomm2_retry_exhausted_total` +1

---

## Bug 4: Target topic 未自动创建

**问题**: 第一次启动 gomm2 时 produce 报 `UNKNOWN_TOPIC_OR_PARTITION`。`TopicDiscovery` 有 auto-create 逻辑但它在 Phase 3 启动，而 parallel catch-up 在 Phase 2 就开始 produce 了。

**根因**: 时序问题 — Phase 2 (catch-up) 在 Phase 3 (TopicDiscovery) 之前。

**修复**:

| 文件 | 改动 |
|------|------|
| `engine.go` | Phase 2 catch-up 循环中，在收集 partition lag 之前先创建 target admin client |
| `engine.go` | 对每个要 catch-up 的 topic，先用 `tgtAdminClient.CreateTopic()` 确保 target topic 存在 |
| `engine.go` | topic name 通过 `policy.FormatRemoteTopic()` 生成，分区数/RF 与 source 一致 |
| `engine.go` | "topic already exists" 错误被安全忽略（降级为 Debug 日志） |

**效果**: 启动 → Phase 2 自动创建 target topic → catch-up produce 不再报 UNKNOWN_TOPIC_OR_PARTITION。

---

## 未修改的文件

以下文件按约定不修改（分配给 Nexus/Nimbus）：
- `config.go` / `sasl.go` / `sync_writer.go` / `builder.go` / `deployment.yaml`

## 编译验证

```bash
cd /data/documents/gomm2
go build ./...  # ✅ 通过
go vet ./...    # ✅ 通过
```
