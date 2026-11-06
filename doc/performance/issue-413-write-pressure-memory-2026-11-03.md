# Issue #413：受压写的按需页与批量发布实验（2026-11-03）

分支：`fix/issue-413-write-pressure-memory`，从 `f570671` 开始。按需页提交 `f88701e`，批量发布提交 `708331d`。[Draft PR #414](https://github.com/brewfs/brewfs/pull/414)。所有测量脚本和原始产物保留在 `/mnt/brewfs-data/home/hxy/issue413/`。

4 KiB 独立写原先为每个 slice 保留 64 KiB 数据页和约 40 KiB 页索引。这个候选改为稀疏索引和按需页，并把同一 chunk 中已完成上传的连续 slice 前缀原子发布，减少重复的元数据扫描与提交。

**状态：未达到，保持 draft，不合并。** 组合候选已降低内存和延迟，并改善包含 close 的吞吐；但包含 close 的排空仍需 22.106 s，mixed 诊断出现读 EIO，cold-read 重挂载重复报 workspace busy，完整 broad 未通过。两种崩溃后的全量内容校验通过。Refs #413。

## 归因

沿用 #410 的 heaptrack 归因，不重新猜测准入瓶颈。先在干净 `f5706712302435206269a253bf861a1a648761d1` 上复现，再改代码。本次基线脏页 1,074,003,968 B、16,388 live slices、RSS 1415.434 MiB、clat p99 1132.462 ms；没有把历史 #408 的 RSS 1516 MiB 替代为本次测量值。

按需页降低了单 slice 保留成本，但让发布积压从 16,388 增到 42,586 个 slice，close 尾部增加到 848 秒。这个负面结果决定了第二步必须改善发布吞吐，不能仅接受 fio 活跃带宽的提升。

## 改动

- `CacheSlice` 用 `BTreeMap` 按需建立索引；数据缓冲从 4 KiB 起按几何倍数增长。保留 64 KiB 逻辑页和现有 chunk、压缩、对象布局；未分配尾部按零读取和上传，按实际分配字节记账及释放。
- VFS 只认领同一 chunk 中连续、完整上传、epoch 有效、尚无提交者且不涉及共享对象保留的前缀，最多 64 个 slice。遇到任何边界即停止，维持覆盖顺序。
- workspace 在原有 mutation gate 下用一个原子 mutation 发布该前缀。传输错误可能意味着整批已经提交，因此重试相同的完整有序批次；重试预算、错误上报、孤儿记录、读缓存失效先于 `Committed` 的规则保留。其他元数据后端继续单条提交。
- 复用现有组提交与 #412 的按 chunk compaction 预算实现；未改变 `upload_before_commit`，未提前释放未提交数据。

## 前后数字

同一 compose 协议：fio 3.39、4 jobs × iodepth 32 × 4 KiB randwrite、512 MiB/job、direct=1、libaio、60 s、固定 seed 391；workspace-v1 + Redis + RustFS/S3、无压缩、读/SSD 缓存 0、写预算 512 MiB、总预算 2 GiB、FUSE workers 16、max_background 512、prefetch 关闭、coalesce 65536 B / 0 ms。后端镜像固定 digest，每轮保存源码/二进制哈希、有效 argv 和原始采样。

| 指标 | f570671 基线 | 仅按需页（不予接受） | 按需页 + 批量发布 | 门槛 |
|---|---:|---:|---:|---:|
| fio error / close EIO | 0 / 0 | 0 / 0 | 0 / 0 | 0 |
| 脏页峰值 MiB | 1024.250 | 166.836 | 110.086 | ≤256 |
| live slices 峰值 | 16388 | 42586 | 28108 | 如实报告 |
| RSS 峰值 MiB | 1415.434 | 368.535 | 304.551 | ≤700 |
| write clat p50 ms | 212.861 | 164.626 | 135.266 | 如实报告 |
| write clat p99 ms | 1132.462 | 371.196 | 278.921 | ≤858 |
| 活跃带宽 MiB/s | 1.562330 | 2.892701 | 3.523839 | ≥2.0 |
| fio 返回后的严格排空 s | 1.771086 | 1.767294 | 1.682883 | 单列 runner 口径 |
| close/tool 尾部 s | 157.973733 | 848.491540 | 20.259038 | 不得转移成本 |
| 活跃写入结束到严格排空 s | 159.928554 | 850.430648 | **22.105756** | **≤5，未达到** |
| effective wall 带宽 MiB/s | 0.430098 | 0.191169 | 2.634483 | 如实报告 |
| effective active + drain MiB/s | 1.517543 | 2.809992 | 3.427718 | 如实报告 |
| effective tool + drain MiB/s | 0.426275 | 0.190762 | 2.575237 | 如实报告 |

现有 runner 的严格排空从 fio 返回后开始，因此不能用约 1.7 s 掩盖先前的 close 等待。表中同时给出从活跃写入结束到严格零债务的完整尾部。本 PR 暂按这个包含 close 的口径判定 5 s。仅按需页的活跃带宽提高 85%，但包含 close 的有效带宽下降约 56%，不能独立接受。组合候选把 close 从 158 s 降到 20.259 s，包含 close/排空的有效吞吐提高到 2.575 MiB/s；发布积压仍未消除，不能说已经达到 5 s。

受压写入期间，29.92 / 50.56 / 60.51 s 的脏页采样为 63.375 / 97.293 / 110.086 MiB。发布吞吐已经改善，但仍落后于前台写入速度。

对象放大基本没有改善：基线 / 仅按需页 / 组合候选分别为 261390 / 261313 / 261205 PUT/GiB，平均 PUT 为 4112 / 4113 / 4115 B，partial-tail 均为 100%。Redis GET/write 为 386.75 / 737.27 / **15.83**，批量发布显著降低元数据访问开销。比较使用仓库 `tools/perf/compare_artifacts.py`，另保存纳秒边界计算的精确 close/drain 时间。

## 本地验证与未达到项

- 按需页的两项测试先在旧实现上真实断言失败，再完成实现；新增五项测试覆盖小分配、增长/覆盖、空洞零尾部、释放、截断及冻结数据。
- 组合候选新增批次上限/认领边界测试，以及实际 KV 的 64 条重叠 slice 原子提交、GC ownership 和非法成员不得部分发布测试。
- 组合候选全部本地门禁通过：workspace 为 1332 个库测试 + 2 个二进制测试；完整 overlay 为 2763 passed / 0 failed / 477 ignored。fmt/diff、workspace check/build、默认及 overlay all-target clippy `-D warnings`、Tokio/io-uring 特性检查和全部 shell/report 检查均通过。rfuse3 不属于 workspace，其条件检查不适用。
- 按需页轮次的完整 overlay 为 2758 passed / 1 failed / 477 ignored。未修改的 `writable_acl_independent_sqlite_instances_keep_mode_acl_during_atime_writes` 在 `observer.open` 返回 EBUSY，单独重跑也失败；任务开始前的 #408 日志已有同一失败。组合候选的完整运行中该测试通过。保留早先失败的全部记录，未修改断言、重试预算或测试过滤；其间歇失败原因尚未证明由本改动解决。
- 未使用 GitHub Actions 作为验证依据。
- fsync+close 和 close-only 两种模式分别经过 60 s 的 4 jobs × depth 32 × 4 KiB direct/libaio 写入，然后 kill -9、新缓存重启。两轮各自校验完整 2 GiB、524,288 个 4 KiB 块，逐块独立 SHA-256 零不一致；close-only 的四个写入进程均未调用 fsync。两轮写入量分别为 56,608 / 56,256 次 I/O。
- fsync+close 尾部仍为 25.69–25.84 s，close-only 为 21.42–21.51 s，独立确认发布积压尚未消除。
- 两轮附加的重挂载后 workspace inspect 诊断均失败：`generic metadata scan exceeds bounded budget`，随后 JSON 解析失败。inspect 在读取整个 layer delta 时触发预算限制，尚未执行 lease 查询。实际挂载与上述全量读回成功；诊断错误单独保留，没有把 cell 的 exit 0 宣称为全部检查通过，也没有修改扫描预算来取绿。

## 未验证项

原始 direct guard **未通过**：基线 seqwrite/randwrite 通过，但 randrw 四个 job 均出现读 EIO（fio exit 4 / error 5）；候选 seqwrite/randwrite 通过，但预填充后的 cold-read 重挂载报 `workspace is busy`，未得到候选 randrw 数据。不能用这一对失败产物证明 mixed 无回归。

| 已完成的 direct 场景指标 | f570671 | 708331d | 变化 |
|---|---:|---:|---:|
| seqwrite active MiB/s | 216.128 | 216.496 | +0.2% |
| seqwrite clat p99 ms | 108.528 | 108.528 | 无改善 |
| seqwrite effective wall MiB/s | 180.125 | 173.240 | **-3.8%** |
| seqwrite effective active+drain MiB/s | 205.838 | 196.819 | **-4.4%** |
| randwrite active MiB/s | 3.236 | 4.706 | +45.4% |
| randwrite clat p99 ms | 325.059 | 235.930 | -27.4% |
| randwrite effective wall MiB/s | 0.617 | 3.253 | +427.4% |
| randwrite effective active+drain MiB/s | 2.942 | 4.483 | +52.4% |

上述为失败 guard 中已完成的场景，未作为整组验收通过，也未更新 README 的接受结果表。独立 fresh-workspace mixed 诊断在基线与候选上都出现读 EIO，fio 分别 exit 4 / 3；候选不能通过 mixed 正确性门禁。启用 debug 的完整顺序诊断中，基线通过，候选再次因 `workspace is busy` 无法进入 randrw。原始失败及诊断失败均保留，读 EIO 的具体原因尚未定位；诊断吞吐不作为验收数据。

辅助进度日志一度因打印时间覆盖退出码而误报诊断通过，已按原始 `.status`、容器 `exit.json` 和 fio 结果交叉核对并纠正。底层退出码和原始数据没有改写；汇总为 `diagnostic-status-correction.json`。60 s 压力结果、CI 和 SHA 校验不依赖这条辅助日志。

元数据补测实际使用镜像中的 **Python fallback**，没有原生 xfstests metaperf/dirstress/dirperf。两端均零操作错误，但单次样本出现如下下降，不能宣称元数据无回归，也不能代替原生工具准入：

| Python fallback，ops/s | f570671 | 708331d | 变化 |
|---|---:|---:|---:|
| metaperf create | 81.431 | 75.498 | -7.3% |
| metaperf open | 319.770 | 285.605 | -10.7% |
| metaperf stat | 2736.689 | 6388.577 | +133.4% |
| metaperf readdir | 120.428 | 119.323 | -0.9% |
| metaperf rename | 412.012 | 394.590 | -4.2% |
| dirstress | 459.290 | 434.261 | -5.4% |
| dirperf create | 77.201 | 74.843 | -3.1% |
| dirperf stat | 14222.967 | 10659.534 | -25.1% |
| dirperf readdir | 44.913 | 49.761 | +10.8% |

buffered broad 全量和原生元数据压力测试未完成；原始 direct guard 失败后自动 broad 队列停止。metadata fallback 下降是否稳定尚未复测确认。传输层丢回复后的整批重试尚未另行做故障注入验证。未验证不算通过，没有合并 SHA。

自审检查了零尾部、实际分配计账、认领/epoch/共享对象边界、整批重试顺序和读缓存失效顺序。当前卡点是发布速度仍不足以消除积压，以及 mixed/重挂载门禁失败；下一轮需要定位这些错误并拆分 mutation gate 等待与提交服务耗时，不能仅扩大预算或放宽门槛。

本地脚本及完整原始证据位于 `/mnt/brewfs-data/home/hxy/issue413/`，脚本没有提交到 git。基线 `baseline60/`；按需页 `memory-v1-pressure/`；组合候选 `batch-v2/` 和 `batch-v2-pressure/`。候选二进制 SHA-256：`5d7a25617f9702dda766ba53d313c826eba5682c682160bd12199f05c18df53f`，源码对应 `708331d`；映射见 `batch-v2/committed-source.json`。`batch-v1/` 保留新测试误用旧枚举名称的编译失败，修正为实际 `Readonly` 状态后从头运行完整门禁。

第一小时复现期限未达到：先等待已有编译结束，再进行干净基线的 release 构建；产品代码在基线完成后才修改。所有长编译串行放在 tmux，未加 timeout 包装，未修改 #402/#406 的分支。
