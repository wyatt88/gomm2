# FIX-NIMBUS.md — 内存 & K8s 部署修复摘要

**修复人**: Nimbus ☁️
**日期**: 2026-03-30
**问题**: 内存 6.9GB 远超预估 937MB；K8s deployment 1Gi limit 必然 OOMKill

---

## 修复清单

### 1. workers_per_partition 可配置化（P0 内存）

**文件**: `internal/config/config.go`
- 新增 `WorkersPerPartition` (默认 4, 原硬编码 6)
- 新增 `FetchMaxPartitionBytes` (默认 5MB, 可调)
- 新增 `RingBufferCapacity` (默认 16384)

**文件**: `internal/mirror/engine.go`
- `workersPerPartition` 从 `const 6` 改为从 `r.WorkersPerPartition` 读取

**文件**: `internal/mirror/parallel_source.go`
- 新增 `fetchMaxPartBytes` / `ringBufferCapacity` 字段
- `fetchWorker` 中 `FetchMaxPartitionBytes` 和 `FetchMaxBytes` 使用配置值
- `catchUpPartitionPipelined` 中 ring buffer capacity 使用配置值

**效果**: 默认 4 workers × 8 partitions = 32 workers（原 48），预估内存降至 ~4-5GB。
用户可通过 YAML 配置调节，无需重新编译。

### 2. K8s Deployment 资源调整（P0 OOMKill）

**文件**: `deploy/kubernetes/deployment.yaml`

| 资源 | 旧值 | 新值 | 理由 |
|------|------|------|------|
| requests.cpu | 500m | 2 | catch-up 阶段 32+ goroutines |
| requests.memory | 256Mi | 4Gi | 稳态基线 |
| limits.cpu | 2 | 4 | GC 并行度 |
| limits.memory | 1Gi | **8Gi** | 实测峰值 6.9GB (6 workers); 4 workers 预估 ~5GB + headroom |

新增环境变量:
- `GOMAXPROCS=4` — 匹配 CPU limit，GC 不再受限于 2 核
- `GOMEMLIMIT=6GiB` — K8s limit 75%，Go GC soft target

新增:
- `topologySpreadConstraints` — pod 分散到不同节点
- `podAntiAffinity` — 避免与其他 gomm2 实例同节点
- `livenessProbe.failureThreshold: 5` — catch-up 阶段容忍更多超时

### 3. GOMAXPROCS / GOMEMLIMIT 运行时配置

**文件**: `cmd/gomm2/main.go`
- 新增 `configureRuntime()` 在 `run()` 最早执行
- 读取 `GOMAXPROCS` 环境变量设置处理器数（默认 = NumCPU）
- 读取 `GOMEMLIMIT` 环境变量设置 Go 软内存限制（支持 GiB/MiB 后缀）
- 启动时打印 `GOMAXPROCS` 和 `GOMEMLIMIT` 到 stderr

### 4. PERF-NOTES.md 更新

- 新增 **Actual Measurement** 部分，对比预估 vs 实测
- 根因分析：franz-go 每 consumer 实际占用 100-140MB（不是预估的 10MB）
- 新增 **Revised Memory Model** 公式
- 新增 **Recommended Configurations** 表（Small/Medium/Large/XL 场景）
- 更新 **Configuration Recommendations** — 参数现在是 YAML 可配置而非硬编码

---

## 编译验证

```bash
cd /data/documents/gomm2 && go build ./...
# ✅ 通过 (2026-03-30)
```

## 配置示例 (YAML)

```yaml
replications:
  - source: sin
    target: bkk
    enabled: true
    workers_per_partition: 4       # 默认 4, 高内存实例可设 6
    fetch_max_partition_bytes: 5242880  # 5MB
    ring_buffer_capacity: 16384
```

## 未修改文件

按任务分工，以下文件**未动**：
- `source.go` / `engine.go`（drain/produce 逻辑）→ Forge
- `dlq.go` / `retry.go` → Forge
- `sync_writer.go` → Nexus
- `sasl.go` → Nexus
- `ring_buffer.go` — 结构未变，只是 parallel_source 读取新的 capacity 配置

> 注: `sasl.go` 有预存编译错误 (IAM Authenticate 签名不匹配 franz-go v1.18.1)，属 Nexus 修复范围。
> 当前 `go build ./...` 能通过，但如果 sasl.go 的 IAM 代码路径被触发会 panic。
