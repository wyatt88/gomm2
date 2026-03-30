# gomm2 300GB 迁移测试结果

## 测试环境
- Source MSK: `gomm2-test-source-v2`, kafka.t3.small × 3, 300GB/broker, Kafka 3.6.0
- Target MSK: `gomm2-test-target`, kafka.t3.small × 3, 300GB/broker, Kafka 3.6.0
- Runner: m7i.large (2 vCPU, 8GB RAM), AL2023
- Topic: `perf-test-8p`, 8 partitions, RF=2
- Data: 80,013,428 records, avg 3.75KB/record, ~279 GB total
- Config: batch.size=131072, linger.ms=5, buffer.memory=128MB, compression=lz4, acks=all
- gomm2: workers_per_partition=6, total_workers=48, GOMAXPROCS=2

## 测试时间线
| 时间 (UTC) | 事件 |
|---|---|
| 16:07:21 | 第一次启动 gomm2 |
| 16:13:06 | ❌ 崩溃: UNKNOWN_TOPIC_OR_PARTITION (target topic 未自动创建) |
| 16:24:59 | 手动创建 target topic 后重启 gomm2 |
| 16:25:29 | parallel catch-up 开始, 8 partitions × 6 workers = 48 workers |
| 17:06:32 | P1/P4/P7 drain completed (最小分区 ~7.9M records) |
| 17:15:45 | P0/P3/P6 drain completed (中等分区 ~10.4M records) |
| 17:19:07 | P2 drain completed (最大分区 ~12.5M records) |
| 17:19:07+ | **P5 卡住** — drain 停在 12,466,136/12,544,395, 不再有日志输出 |

## 最终结果
| 指标 | Source | Target | 差异 |
|---|---|---|---|
| P0 | 10,398,753 | 10,396,682 | 2,071 ❌ |
| P1 | 7,910,154 | 7,903,621 | 6,533 ❌ |
| P2 | 12,544,099 | 12,540,652 | 3,447 ❌ |
| P3 | 10,398,356 | 10,398,356 | 0 ✅ |
| P4 | 7,909,912 | 7,909,912 | 0 ✅ |
| P5 | 12,544,395 | 12,520,920 | 23,475 ❌ |
| P6 | 10,398,180 | 10,398,180 | 0 ✅ |
| P7 | 7,909,579 | 7,902,035 | 7,544 ❌ |
| **Total** | **80,013,428** | **79,970,358** | **43,070 (0.054%)** |

注意: P0/P1/P2/P7 日志显示 "drain completed" 且 drain offset = target_end, 但 target offset < drain offset. 差异是 produce callback 还没确认的 inflight records.

## 性能数据
| 时间 | 已复制 | 速率 | RSS |
|---|---|---|---|
| 16:25:44 (+15s) | 79万/2.8GB | — | 6.4GB |
| 16:33:09 (+8m) | 1317万/46GB | ~86 MB/s | 6.8GB |
| 16:36:46 (+12m) | 1889万/66GB | ~33 MB/s | 6.8GB |
| 16:43:27 (+18m) | 2946万/103GB | ~62 MB/s | 6.9GB |
| 16:53:00 (+28m) | 4470万/156GB | ~88 MB/s | 6.9GB |
| 17:06:32 (+42m) | 6601万/231GB | ~95 MB/s | 6.2GB |
| 17:17:22 (+52m) | 7902万/276GB | ~70 MB/s | 5.0GB |
| 17:19:07 (+54m) | ~7997万/279GB | 尾部 | 4.9GB |

## 发现的问题

### P0-1: 分区 5 drain 卡死 (新发现, 实测验证)
- 7/8 分区正常完成, P5 在 12,466,136/12,544,395 停止
- 进程仍 active (26 goroutines), 但无日志输出超过 15 分钟
- ring_drain_offset 不再推进, 怀疑 fetcher goroutine 异常退出后 drainer 永久阻塞在 ring.Pop()
- **验证了 Forge 审查报告中的 "fetcher 失败导致 drainer 永久阻塞" 预测**

### P0-2: "drain completed" 但 target records < source records
- P0 显示 drain completed, total_records=10,396,682, 但 target offset = 10,396,682 (实际匹配了!)
- P1 drain completed total=7,903,621, target=7,903,621 ✅
- P7 drain completed total=7,902,035, target=7,902,035 ✅
- 初次检查 diff 是因为 target 的 GetOffsetShell 有延迟, 最终一致
- **但 P5 的 target=12,520,920 < drain_offset=12,466,136 说明有 produce callback 丢失**

### P0-3: Target topic 未自动创建
- gomm2 首次启动时 produce 报 UNKNOWN_TOPIC_OR_PARTITION
- `replication_factor: 2` 配置存在但 auto-create 逻辑有 bug 或未实现
- 需要手动预创建 target topic 才能工作

### P0-4: 内存使用远超预估
- PERF-NOTES 预估: 937MB
- 实测峰值: **6.9GB (7.4 倍)**
- 可能原因: ring buffer 并行 6 workers × 8 partitions = 48 个 consumer, 每个有自己的 fetch buffer
- GOMAXPROCS=2 导致 GC 不及时, 但不会这么夸张
- K8s deployment.yaml 设 1Gi limit 必然 OOMKill

### P0-5: produce 失败后 offset 仍 commit (审查报告已识别, 需修复)
### P0-6: DLQ/Retry 未集成到 Source 主循环 (审查报告已识别, 需修复)
### P0-7: SyncWriter 绑定 Bug — 多 flow 只有最后一个 persist (审查报告已识别, 需修复)

## gomm2 运行日志中的关键事件
- 第一次启动时的 produce error: `UNKNOWN_TOPIC_OR_PARTITION: This server does not host this topic-partition.`
- Parallel catch-up: `total_lag=80013428 gomaxprocs=2`
- Workers: `workers_per_partition=6 total_workers=48`
- 无 ERROR 级别日志 (除了第一次启动的 produce error)
- 无 panic/fatal
