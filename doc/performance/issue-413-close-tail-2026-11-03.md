# #413 close 尾部跟进（2026-11-03）

继续 [draft PR #414](https://github.com/brewfs/brewfs/pull/414)，进入本轮时为
`8973f92`。原有按需页和 64 条原子发布的交付见
[前一轮报告](issue-413-write-pressure-memory-2026-11-03.md)。本轮从
11:58 UTC 开始，首版改动及测量窗口截至 13:58 UTC，总窗口截至 15:58 UTC。
原始证据在 `/mnt/brewfs-data/home/hxy/issue413/close-tail-followup/`。

压力参数为 fio 3.39、4 jobs × depth 32 × 4 KiB randwrite、512 MiB/job、
direct=1、libaio、60 秒、seed 391；workspace-v1 + Redis + RustFS/S3，
无压缩，关闭读缓存及预取，写内存 512 MiB、总预算 2 GiB，coalesce
65536 B / 0 ms，FUSE workers/background 16/512。镜像 digest 和有效 argv
保留在每轮产物中。完整尾部沿用现有框架口径：
`strict_drained_timestamp - tool_start_timestamp - fio_active_runtime`；
close/tool 差额包含工具开销，不能只看 fio 返回后的排空时长。

**A 未达到 ≤5 s：两项性能实验均已定向撤回，PR #414 保持 draft。**
512 条前缀测得 5.678368 s；增加同文件等待后认领测得 12.670227 s。
现有按需页和 64 条发布实现保留。最终代码只增加 10 个 debug 阶段观测点，
已提交为 `3a70bd9`；所需本地门禁均获得通过结果，overlay 首轮失败和完整
重跑结果详见下文。B 已证实为既有缺陷并记录在 #415。

## 先测后改：尾部已有满批提交

原 `batch-v2-pressure` 的 66.733–78.613 秒区间没有新增 S3 PUT；live slices
从 25,038 降到 2,126，完成的批量提交增加 358 次，即每次恰好回收 64 条。
旧 FUSE trace 中 FLUSH 为 19,596–19,620 ms，RELEASE 为 0.322–4.254 ms。
`commit_chunk` 在 close/flush 期间本来就反复尝试原子前缀，不能把这段长尾
归因于“close 全走单条提交”。

新增 debug span 的诊断二进制于 12:38:49 UTC 构建完成。没有并发编译的
60 秒受压诊断中，活跃结束后的 539 次发布服务有 508 次包含 64 条写入。
以下为裁剪到活跃结束至 fio 返回窗口的累计耗时，仅统计发布调用的子阶段：

| 阶段 | 累计 ms | 说明 |
|---|---:|---|
| 发布服务整体 | 25,523.8 | mutation gate 内的串行服务 |
| 读取 mutation version | 659.5 | 每次重新建立版本条件 |
| inode prefetch | 854.3 | 每次构造 mutation 前读取 |
| extent compaction | 16,665.1 | 包含下面两行 |
| 其中：历史 extent 扫描 | 15,135.3 | 重复扫描同一 chunk 历史 |
| 其中：保留被覆盖 slice 的 ownership | 2.2 | 本轮几乎没有覆盖 |
| packet preparation | 3,409.6 | 包括 generation / deletion fence |
| 最终 CAS | 1,917.0 | 不是占比最大的阶段 |

其他时间用于事务读、lease 检查及构造。由父子 span 差值估算，尾部调用平均
排队 1,693.8 ms，服务平均 47.5 ms；并发等待不能相加为总尾时，父阶段与子阶段也
不能重复相加。诊断完整尾部 27.814 s 受 debug 日志影响，不作为验收数字。
诊断二进制的 inode/chunk tracing 字段为空，仅阶段嵌套、批次数和耗时有效。

原候选冻结二进制随后在同环境关闭 debug 复测（`control-pressure`），完整尾部
为 **21.859529 s**，与旧结果 22.105756 s 接近；脏页 112.805 MiB、RSS
309.816 MiB、write clat p99 258.998 ms、活跃带宽 3.626553 MiB/s、含工具与
严格排空的有效带宽 2.658271 MiB/s，无 fio/close EIO。两轮均复用原 compose
框架、参数和固定后端镜像，编译与测量串行运行。

对照轮的采样进一步显示：活跃结束后 5.929 秒仍有 115 次 PUT 未完成，
7.015 秒首次观察到最终 PUT 计数和零远端上传在途量，此时仍有 25,858 个
slice、101.23 MiB 脏数据、32 个元数据调用在途。20.193 秒首次采样到全部
债务为零，21.860 秒完成严格稳定排空。这些是采样观察边界；上传与发布
重叠，不能把两个区间当作独立串行服务时间相加。

## 首版实验：512 条已上传前缀

假设：扩大同一 chunk 的连续已上传前缀，可以摊薄相同历史的重复扫描。
VFS 认领上限与 workspace 原子前缀上限从 64 改为 512，独立排队的单条
请求仍以 64 条组提交。#412 的逐 chunk 扫描预算、读取预算、入站行数上限
与 packet 上限均保留；上传边界、epoch、共享对象保留、覆盖顺序、整批重试
和读缓存失效后才能标记 Committed 的规则沿用原实现。

新增测试构造 513 个 slice，在第 401 个放置未完成上传的边界：旧实现实测
失败，`left: 64 / right: 400`（`large-prefix-red`，12:47:36 UTC）。实现后
该测试通过，同时检查后端只允许 96 条时遵守其上限。实际 KV 测试扩展为
512 条重叠 slice 的单次原子发布、最终覆盖结果和全部 GC ownership；非法
成员移到整个大前缀末尾，检查不能部分发布。

首版于 12:59:02 UTC 通过 workspace 硬门禁：1333 个库测试、2 个二进制
测试，零失败。之后才开始 release 构建和压力验证，源码差异冻结于
`prefix512/validation/tested.patch`。其后完整尾部门槛失败，未运行完整 CI
和恢复验收；详见下文验证范围。

首版在 13:37 UTC 完成测量，满足两小时内给出改动及测量的时间要求，
但**不满足完整尾部 ≤5 秒**，因此没有提交或作为验收结果接受：

| 指标 | 同环境原 64 条对照 | 512 条首版 |
|---|---:|---:|
| 活跃结束至严格排空 s | 21.859529 | **5.678368** |
| close/tool 尾部 s | 19.998317 | 3.817135 |
| fio 返回后严格排空 s | 1.695183 | 1.714940 |
| 脏页峰值 MiB | 112.805 | 71.047 |
| RSS 峰值 MiB | 309.816 | 244.754 |
| write clat p99 ms | 258.998 | 304.087 |
| 活跃带宽 MiB/s | 3.626553 | 3.558351 |
| 包含工具和严格排空的有效带宽 MiB/s | 2.658271 | 3.250720 |

`prefix512-pressure` 无 fio/close EIO，内存采样覆盖完整，全部最终债务为零。
完整尾部还包括 fio 返回至 drain 启动的 146.294 ms 间隔。
活跃结束后 1.830 秒首次观察到上传已完成，仍有 5,630 个 live slices；
3.989 秒首次采样到零债务。严格稳定等待仍完整计算，没有缩短采样或排空条件。

同一二进制的 `prefix512-diagnostic` 进一步显示：全部 614 次发布中最大批次
仅 311 条，没有任何 512 条满批；尾部 90 次发布平均 205.7 条。故没有继续
扩大上限。debug 诊断中的完整尾部为 8.756 s，其中发布服务 6,621.0 ms；
历史扫描 3,724.8 ms（包含于 compaction 4,083.7 ms）、packet preparation
1,310.1 ms、CAS 623.9 ms、mutation version 127.0 ms、inode prefetch
110.3 ms。并发排队平均 3,782.7 ms，不能与服务时间直接相加。

## 第二版假设：等待后再认领已上传前缀

现有 VFS 在等待全局元数据 gate 之前就固定了待提交 slice 列表；同文件的
多个 chunk 排队期间新完成的上传只能进入下一轮。第二版在同文件的原子
前缀路径中增加提交轮次锁，在轮到该 chunk 后重新读取最新已上传前缀。
上传不依赖这个锁；上限仍为 512，未改变 #412 的扫描预算或整批重试规则。
不具备原子批量能力的后端及 commit-before-upload 路径沿用现有行为。

workspace 发布原本就在全局 mutation gate 下串行执行。例如第三条上传在
排队期间完成，原顺序提交 `[1, 2]` 后还需下一轮提交 `[3]`；新顺序在轮到
该 chunk 后认领 `[1, 2, 3]`，预期减少重复扫描。实际性能仍以对照测量为准。

确定性测试在 13:45:47 UTC 于提前认领版本真实失败：等待 gate 时已有 slice
被标记 `meta_write_started`。实现将认领移到等待之后，并增加已提交队首的
快速退出，避免逐个回收已提交 slice 时重复排队。测试同时验证等待期间完成的
第三个上传进入同一前缀，以及已提交队首不会被忙碌的 gate 阻塞。

第二版于 13:56:41 UTC 通过 workspace 硬门禁：1,334 个库测试及 2 个
二进制测试，零失败；随后才进入 release 构建。源码冻结于
`lateclaim512/validation/tested.patch`，所有长编译与性能测量串行执行。

第二版完整尾部为 **12.670227 s**，不予接受。以下为无 debug 跟踪的压力
结果；每个候选仅完成一轮，未进入通过后的重复及恢复验收队列：

| 指标 | 同环境 64 条对照 | 512 条（未接受） | 512 条 + 等待后认领（未接受） |
|---|---:|---:|---:|
| 活跃结束至严格排空 s | 21.859529 | 5.678368 | 12.670227 |
| close/tool 尾部 s | 19.998317 | 3.817135 | 10.765778 |
| fio 返回后严格排空 s | 1.695183 | 1.714940 | 1.739419 |
| fio 返回至 drain 启动 ms | 166.029 | 146.294 | 165.031 |
| 脏页峰值 MiB | 112.805 | 71.047 | 78.008 |
| RSS 高水位 MiB | 309.816 | 244.754 | 335.621 |
| write clat p99 ms | 258.998 | 304.087 | 235.930 |
| 活跃带宽 MiB/s | 3.626553 | 3.558351 | 4.072548 |
| 含工具和严格排空的有效带宽 MiB/s | 2.658271 | 3.250720 | 3.362871 |
| fio / close EIO | 0 / 0 | 0 / 0 | 0 / 0 |

第二版的其他压力门槛通过，最终债务均为零，但活跃吞吐改善不能替代完整尾部
门槛。活跃结束后 97 ms 的采样仍有 19,909 个 slice、78.01 MiB 脏数据，
最终 62,312 次 PUT 中仅完成 53,748 次，即仍差 8,564 次。活跃结束后
8.790 秒的采样仍差 435 次 PUT；9.935 秒首次观察到最终 PUT 计数和零
上传在途量，仍有 1,948 个 slice。10.963 秒首次采样到零债务。
上传和发布相互重叠，不能把这些采样区间当成可相加的串行阶段。

对象放大也未消除：对照 / 首版 / 第二版分别为 261,227 / 261,190 /
260,959 PUT/GiB，平均 PUT 4,114 / 4,115 / 4,119 B。partial-tail 次数
分别为 55,520 / 54,460 / 62,312，与各轮 PUT 总数相同。更高的活跃吞吐
仍产生大量小对象上传，不能仅从 metadata 在途调用减少推断 close 会变短。

同一冻结二进制的 `lateclaim512-diagnostic` 给出以下发布阶段耗时；两列都
裁剪到活跃结束至 fio 返回，并只统计发布服务的子调用：

| 阶段 | 512 条诊断 ms | 等待后认领诊断 ms |
|---|---:|---:|
| 发布服务整体 | 6,621.0 | 8,337.1 |
| mutation version | 127.0 | 136.0 |
| inode prefetch | 110.3 | 155.3 |
| compaction，含扫描/ownership | 4,083.7 | 4,929.8 |
| 其中：历史扫描 | 3,724.8 | 4,536.9 |
| 其中：ownership | 0.5 | 0.5 |
| packet preparation | 1,310.1 | 1,937.6 |
| 最终 CAS | 623.9 | 719.4 |

第二版诊断完整尾部 10.705536 s，仅用于归因。尾部 84 次发布平均 228.8 条，
仅一批达到 512；后端 gate 前的估算平均排队从 3,782.7 ms 降至 360.6 ms，
但新增的文件内等待不在该数值内，不能把它解释成总等待消失。两组诊断均未
证明继续增大批次能解决问题。剩余主债仍包含重复历史扫描和尾部未完成上传。

下一步应同时测量 slice ID 分配等待、上传限流等待、实际 PUT 和发布服务，
再决定是否减少重复扫描或调整 ID 批量预留。代码中 slice ID 每 64 个预留
一次，且在发出上传前分配；本轮没有直接计时证明该分配器是上传尾部的原因，
因此没有据此扩大预留块或提交第三个推测性改动。

## 最终保留内容与验证范围

14:40 UTC 用定向补丁撤回 512 上限、文件内等待、辅助认领函数及对应实验测试。
VFS 和 workspace 的上限恢复为 64；#412 的预算从未修改。两个候选的源码
补丁、二进制、构建清单、TDD 失败/成功日志和原始测量全部保留。

最终只保留 mutation version、inode prefetch、批次发布、hot mutation、
历史扫描/compaction、ownership、packet preparation 和 CAS 的 debug spans。
它们用于重现上述分段诊断，不作为吞吐或 close 性能改善。最终 CI 从
14:40:37 UTC 开始，首轮流程于 15:32:43 UTC 结束，产物为
`observability-final/validation/`。workspace 硬门禁通过：1,332 个库测试及
2 个二进制测试，零失败，264 忽略。fmt、diff、四项脚本语法检查、三项报告
测试、workspace check/build、Tokio/io-uring feature check、workspace clippy
及普通/overlay all-targets clippy `-D warnings` 均通过。rfuse3 不是 workspace
member，条件式 feature 检查不适用。

首轮完整 overlay 为 **2,762 通过、1 失败、477 忽略**：未修改的
`writable_acl_independent_sqlite_instances_keep_mode_acl_during_atime_writes`
在 `src/workspace_overlay/meta_layer/tests.rs:1477` 的 `observer.open` 返回
`EBUSY=16`。旧 `memory-v1/validation/overlay-full.log` 已有相同断言和错误，
该测试文件自 `f570671` 起未变；这不证明本轮改动修复了间歇失败或未改变其频率。
保留首轮 `overlay-full.status=101` 和 `ci.status=1`，未覆盖失败日志。

15:32:45–15:38:02 UTC 在完全相同的源码和参数下重跑**完整** overlay，
得到 **2,763 通过、0 失败、477 忽略**，该 ACL/atime 测试也通过。没有过滤
测试、修改断言或调整重试预算；首次及重跑的源码 diff 均与冻结快照逐字节相等。
重跑保存为 `overlay-full-repeat2.*`，`gate-resolution.json` 记录每项门禁的
通过结果及首轮失败。因此只接受观测代码，未接受任何新性能优化。

两项候选均因第一轮压力门槛失败而停止后续重复、SHA 恢复、完整 CI、mixed
及原生元数据验收。上一轮 #413 的两项 2 GiB 恢复结果不能作为这两项拒绝
候选的恢复证明。原生 metaperf/dirstress/dirperf 已从固定 LFS 哈希准备好，
但本轮队列未执行；没有将 Python fallback 或未执行的检查计为通过。
README 接受结果表保持原值。

紧凑证据见 [close-tail-summary.json](evidence/issue-413-close-tail-20261103/close-tail-summary.json)，
包含各轮源码/二进制摘要、精确门槛、采样边界、阶段耗时、TDD 和最终 CI 日志哈希。
原始构建补丁及完整日志在上述本地产物目录中。使用新产物名、在没有编译时可重放：

```bash
bash /mnt/brewfs-data/home/hxy/issue413/pressure.sh \
  close-tail-followup/replay-prefix512 \
  /mnt/brewfs-data/home/hxy/issue413/close-tail-followup/prefix512/candidate/brewfs
python3 /mnt/brewfs-data/home/hxy/issue413/close-tail-followup/analyze-acceptance.py \
  /mnt/brewfs-data/home/hxy/issue413/close-tail-followup/replay-prefix512
```

第二项实验使用 `lateclaim512/candidate/brewfs`。这些冻结二进制是已拒绝的实验
版本；当前源码已撤回其行为改动。阶段诊断可重放为：

```bash
python3 /mnt/brewfs-data/home/hxy/issue413/close-tail-followup/run-diagnostic.py \
  close-tail-followup/replay-prefix512-diagnostic pressure \
  --binary /mnt/brewfs-data/home/hxy/issue413/close-tail-followup/prefix512/candidate/brewfs \
  --volume workspace-v1 --backend redis --data-bind --coalesce 65536 \
  --direct 1 --runtime 60 --trace
python3 /mnt/brewfs-data/home/hxy/issue413/harness/analyze-pressure.py \
  /mnt/brewfs-data/home/hxy/issue413/close-tail-followup/replay-prefix512-diagnostic
python3 /mnt/brewfs-data/home/hxy/issue413/close-tail-followup/analyze-spans.py \
  /mnt/brewfs-data/home/hxy/issue413/close-tail-followup/replay-prefix512-diagnostic
```

诊断标记应保留，不把 debug 数据提升为验收数据。

## B：mixed EIO 已定位为既有缺陷

完整结论及证据见 [#415 诊断报告](issue-415-mixed-read-eio-2026-11-03.md)。
干净 `f570671` 连续三轮捕获 Busy→EIO 转换链，其中一轮的四个源头探针
均确认 layer generation 连续三次失配 → `WorkspaceError::Busy`；随后为
`EBUSY=16` → anyhow 包装 → FUSE 兜底 `EIO=5`。原候选也有同样的错误转换链。
已创建 [#415](https://github.com/brewfs/brewfs/issues/415)，
本 PR 不修该既有读路径缺陷。这不能替代 mixed 吞吐与延迟的性能门禁。
