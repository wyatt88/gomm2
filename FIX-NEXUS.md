# FIX-NEXUS.md — Kafka 协议兼容性修复摘要

**修复者**: Nexus 🔗 (Kafka 协议与集成专家)
**日期**: 2026-03-30
**编译状态**: ✅ `go build ./...` 通过

---

## Bug 1 (P0): SyncWriter 绑定 Bug — 多 flow 只有最后一个 persist

**文件**: `internal/mirror/engine.go` — `bootstrapOffsetSyncs()`

**根因**: `bootstrapOffsetSyncs` 遍历 replications 时，用 `e.sources[len(e.sources)-1]` 给 source 绑定 SyncWriter。这意味着不管循环到第几个 flow，writer 总是绑到最后一个 source 上。第 1 个、第 2 个 flow 的 source.syncWriter 永远是 nil → offset sync 永远不会 persist 到 Kafka。

**修复**: 引入 `sourceIdx` 计数器，与 `NewEngine()` 中 sources 的 append 顺序一一对应。每个 flow 的 SyncWriter 正确绑定到 `e.sources[sourceIdx]`。

**影响**: 多 flow 场景（如 source→target + target→source 双向复制，或多个独立复制 flow）现在每个 flow 都能正确 persist offset syncs。单 flow 场景行为不变。

---

## Bug 2 (P0): IAM Auth 不支持 credential chain — 只有静态 AK/SK

**文件**: `internal/config/sasl.go` — `BuildSASLOpt()`

**根因**: `AWS_MSK_IAM` 分支只接受 `username`/`password` 字段作为 Access Key / Secret Key。在 EKS (IRSA)、EC2 (instance profile)、ECS (task role) 等场景下，credentials 由平台注入，不应硬编码 AK/SK。

**修复**:
1. **新增 AWS SDK Go v2 依赖**: `aws-sdk-go-v2`, `aws-sdk-go-v2/config`
2. **双路径逻辑**:
   - `username` 非空 → 走原有静态 credentials 路径（向后兼容）
   - `username` 为空 → 调用 `awsconfig.LoadDefaultConfig()` 使用 AWS 默认 credential chain
3. **临时凭证自动刷新**: 如果 credentials 可过期（IRSA/instance profile），创建 `refreshableIAM` 机制，每次 SASL 认证时重新 Retrieve credentials，确保 token 不过期

**使用方式**:
```yaml
# 静态 AK/SK（旧方式，向后兼容）
sasl:
  mechanism: AWS_MSK_IAM
  username: AKIAXXXXXXXX
  password: xxxxxxxxxxxx

# 默认 credential chain（新方式，推荐）
sasl:
  mechanism: AWS_MSK_IAM
  # 不填 username/password，自动使用 IAM role / IRSA / instance profile
```

---

## Bug 3 (P1): offset.lag.max 硬编码 2000 — Java MM2 默认 100

**文件**:
- `internal/config/config.go` — 新增 `OffsetSyncInterval` 字段
- `internal/mirror/source.go` — `replicationLoop()` 中使用配置值

**根因**: `source.go` 第 251 行 `syncWriteInterval = int64(2000)` 硬编码，意味着每 2000 条 record 才 persist 一次 offset sync。Java MirrorMaker 2 的 `offset.lag.max` 默认值是 100。gomm2 的精度比 Java MM2 差 20 倍，导致 consumer group offset 翻译在 failover 时可能回退最多 2000 条 record。

**修复**:
1. `ReplicationConfig` 新增 `OffsetSyncInterval int` 字段 (`yaml:"offset_sync_interval"`)
2. `setDefaults()` 中默认值设为 100（匹配 Java MM2）
3. `replicationLoop()` 中 `syncWriteInterval` 从 `s.cfg.OffsetSyncInterval` 读取

**使用方式**:
```yaml
replications:
  - source: us-east-1
    target: eu-west-1
    offset_sync_interval: 100  # 默认值，每 100 条 persist 一次 offset sync
    # offset_sync_interval: 50  # 更高精度，适合金融场景
```

---

## 依赖变更

| 新增依赖 | 版本 | 用途 |
|----------|------|------|
| `github.com/aws/aws-sdk-go-v2` | v1.41.5 | AWS core SDK |
| `github.com/aws/aws-sdk-go-v2/config` | v1.32.13 | Default credential chain |
| `github.com/aws/aws-sdk-go-v2/credentials` | v1.19.13 | Credential types |
| `github.com/aws/aws-sdk-go-v2/feature/ec2/imds` | v1.18.21 | EC2 instance metadata |
| `github.com/aws/aws-sdk-go-v2/service/sts` | v1.41.10 | AssumeRole/IRSA |
| `github.com/aws/aws-sdk-go-v2/service/sso` | v1.30.14 | SSO credentials |
| `github.com/aws/smithy-go` | v1.24.2 | AWS SDK core |

---

## 修改文件清单

| 文件 | Bug # | 变更类型 |
|------|-------|---------|
| `internal/mirror/engine.go` | Bug 1 | 修复 SyncWriter 绑定逻辑 |
| `internal/config/sasl.go` | Bug 2 | 重写，新增 credential chain + 自动刷新 |
| `internal/config/config.go` | Bug 3 | 新增 OffsetSyncInterval 字段 + 默认值 |
| `internal/mirror/source.go` | Bug 3 | 使用配置值替换硬编码 2000 |
| `go.mod` / `go.sum` | Bug 2 | 新增 AWS SDK Go v2 依赖 |
