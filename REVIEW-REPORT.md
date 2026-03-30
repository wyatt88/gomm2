# gomm2 迁移能力综合验证报告

**日期**: 2026-03-30
**审查团队**: Forge 🔧 / Nexus 🔗 / Pulse 📊 / Nimbus ☁️ / Flux 🔍
**总指挥**: Ark 🚀
**目标数据量**: 300GB / 8000 万条

---

## 一、综合评分

| 维度 | 审查人 | 评级 | 分数 |
|------|--------|------|------|
| 代码正确性 | Forge 🔧 | ❌ 有 Bug | 4/10 |
| Kafka 协议兼容性 | Nexus 🔗 | ⚠️ 部分达标 | 7/10 |
| 运维就绪性 | Pulse 📊 | ⚠️ 有风险 | 5/10 |
| 基础设施与性能 | Nimbus ☁️ | ⚠️ 有风险 | 6/10 |
| 迁移风险 | Flux 🔍 | ⚠️ 需评估 | 5.5/10 |
| **综合** | | **⚠️ 未达生产就绪** | **5.5/10** |

---

## 二、必须修复的 P0 问题（6 个）

### Bug 类

| # | 问题 | 发现者 | 影响 |
|---|------|--------|------|
| 1 | **produce 失败 → offset 仍 commit → 数据丢失** | Forge | 违反 at-least-once 语义，数据永远丢失 |
| 2 | **DLQ/Retry 模块未集成到 Source 主循环** | Forge | 写了没用，produce 失败无兜底 |
| 3 | **SyncWriter 绑定 Bug** — 多 flow 只有最后一个 persist | Nexus + Forge | 多 flow 时前面的 offset sync 全丢 |
| 4 | **IAM Auth 不支持 credential chain** — 只有静态 AK/SK | Nexus | MSK IAM + EC2/EKS 部署不可用 |

### 配置类

| # | 问题 | 发现者 | 影响 |
|---|------|--------|------|
| 5 | **K8s memory limit 1Gi** — 实际需要 2-4Gi | Pulse + Nimbus | 几乎必然 OOMKill |
| 6 | **offset.lag.max 硬编码 2000** — Java MM2 默认 100 | Nexus | Offset 翻译精度差 20 倍 |

---

## 三、300GB / 8000 万条数据量评估

### 数据特征推算

| 参数 | 值 |
|------|------|
| 总数据量 | 300 GB |
| 总记录数 | 80,000,000 |
| 平均记录大小 | ~3.75 KB/条 |
| 假设 8 分区 | 37.5 GB/分区 ≈ 1000 万条/分区 |

### Parallel Catch-Up 预估

| 场景 | 吞吐 | 耗时 | 说明 |
|------|------|------|------|
| **乐观** (broker 无瓶颈) | 400 MB/s | ~12.5 分钟 | PERF-NOTES 理论上限 |
| **现实** (EBS baseline) | 200-300 MB/s | 17-25 分钟 | kafka.t3.small 测试环境 |
| **保守** (网络/GC 叠加) | 100-150 MB/s | 33-50 分钟 | 跨 Region 真实场景 |

### ⚠️ 300GB 场景特有风险

| 风险 | 说明 |
|------|------|
| **Catch-up 崩溃 = 全量重来** | 无进度持久化，8000 万条全部重新 fetch+produce |
| **Ring buffer 背压** | 3.75KB × 16384 slots = 61MB/partition，8 partition = 488MB — 叠加 fetch buffer 总内存可能到 1.5-2GB |
| **Go GC 风暴** | 8000 万条 record 对象经过 fetch→ring→drain→produce→callback 链，heap 压力大 |
| **MaxBufferedRecords=500K 爆炸** | 如果目标 MSK 短暂不可用：500K × 3.75KB = **1.87GB** producer buffer |
| **Topic catch-up 串行** | 如果多 topic，按顺序执行，不能并行 |

---

## 四、测试环境进度

| 资源 | 状态 | 说明 |
|------|------|------|
| CloudFormation Stack | 🔄 CREATE_IN_PROGRESS | `gomm2-test-env` |
| Source MSK (gomm2-test-source) | 🔄 创建中 | kafka.t3.small × 3, Kafka 3.6.0 |
| Target MSK (gomm2-test-target) | 🔄 创建中 | kafka.t3.small × 3, Kafka 3.6.0 |
| Runner EC2 (m7i.large) | ⏳ 等 MSK | AL2023 + Go 1.22 + Kafka CLI |
| VPC / SG / IAM | ✅ 完成 | poc-vpc, 安全组已配置 |

> MSK 通常需要 15-25 分钟创建。创建完成后需要：
> 1. 创建测试 topic (8 分区)
> 2. 灌入 300GB / 8000 万条测试数据
> 3. 编译部署 gomm2
> 4. 运行迁移测试

---

## 五、迁移测试计划

### Phase 1: 环境准备 (MSK 就绪后)

```bash
# 1. 获取 bootstrap servers
# 2. 创建测试 topic
kafka-topics.sh --create --topic perf-test-8p \
  --partitions 8 --replication-factor 2 \
  --config retention.bytes=350000000000

# 3. 灌入 300GB 测试数据 (kafka-producer-perf-test)
kafka-producer-perf-test.sh \
  --topic perf-test-8p \
  --num-records 80000000 \
  --record-size 3750 \
  --throughput -1 \
  --producer-props bootstrap.servers=<source>
```

### Phase 2: gomm2 编译部署

```bash
# 编译
cd /data/documents/gomm2 && make build

# 配置
cat > /opt/gomm2/config.yaml << EOF
clusters:
  source:
    bootstrap_servers: [<source-brokers>]
  target:
    bootstrap_servers: [<target-brokers>]
replications:
  - source: source
    target: target
    enabled: true
    topic_filter:
      whitelist: ["perf-test-8p"]
metrics:
  enabled: true
  address: ":9090"
EOF

# 运行
./bin/gomm2 --config /opt/gomm2/config.yaml
```

### Phase 3: 验证项

| 测试项 | 验证方法 | 通过标准 |
|--------|---------|---------|
| **数据完整性** | 对比 source/target HWM | 目标 HWM ≥ 源 HWM |
| **记录数一致** | kafka-consumer-groups describe | 无 lag |
| **吞吐量** | Prometheus gomm2_bytes_replicated_total | > 100 MB/s |
| **内存峰值** | `go tool pprof` / RSS 监控 | < 2GB |
| **分区有序性** | 自定义 consumer 校验 offset 单调性 | 每分区 offset 严格递增 |
| **Crash 恢复** | kill -9 后重启，检查重复/丢失 | 无丢失（可接受少量重复）|
| **Graceful shutdown** | SIGTERM，检查最终 offset commit | 所有 inflight drain 完成 |

---

## 六、建议

### 短期 (测试环境验证)
1. **先在测试环境跑通** — 不修 bug，观察 300GB 场景的实际表现
2. **重点监控**: 内存、GC pause、produce error count、end-to-end latency
3. **Crash 测试**: 在 catch-up 中途 kill，观察恢复行为

### 中期 (修复后再考虑生产)
1. 修复 6 个 P0 问题
2. 补充 Ring buffer 并发测试 + 集成测试
3. K8s 资源配置调整 (memory 4Gi, cpu 4-8)
4. 添加 GOMEMLIMIT + catch-up 进度持久化

### 长期
1. 多实例协调 (K8s lease/etcd)
2. EOS (事务性 producer)
3. Admin CLI + Web UI
