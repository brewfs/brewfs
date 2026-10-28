# PR #375 本机 Redis 配对测量

状态：**未达判据 + 卡在基线 release 编译；正式改前/改后各 0 次，PR 保持 draft。** 已读取
`/home/hxy/ISSUE.md` 与 `/home/hxy/HANDOFF.md`。按用户最新要求，验收目标为
4 KiB 随机读吞吐 ≥134.8 MiB/s 且 p99 ≤80 ms。原 ISSUE 的 100k inode stat
判据属于另一负载，不能从随机读推导其结论。

硬截止为 2026-10-28 19:53:42 +0800；19:46:43 已按预留的构建上限终止基线
Cargo/rustc，停止尝试，不重试。限时期间未运行 Rust 测试或额外 release 构建。
Redis 已完成真实挂载探测；基线仅生成了库文件，尚无命令行可执行文件。
本轮所有路径均在 `/home/hxy`，未寻找已删除的实验机。

## 改前

版本 `975deacc13131335b60a4c72de4ed781bbfe7154`，源码 `/home/hxy/brewfs-base`，
target `/home/hxy/target-opt-base`。接管的唯一一次构建为
`cargo build --release -j 1 --features workspace-overlay --bin brewfs`。
主库已完成，卡在 `src/main.rs` 的 O3 编译。19:44:42 的现场记录：Cargo 已运行
2:07:37，主程序 rustc 已运行 34:23、CPU 100%、线程 `opt cgu.11`，
`target-opt-base/release/brewfs` 不存在。19:46:43 到预留上限仍未完成而终止。
正式重复数为 0；历史
66.93 / 67.43 / 68.06 MiB/s 不充当本机新基线。

## 改后

版本 `514a902a4416896fda575d4fd66c11f1637e0341`，分支 `perf/overlay-cont`。
基线阻塞后没有启动候选 release 构建或重建语料，正式重复数为 0。
本轮只更正实验 metadata backend，不改性能实现或 ACL TTL。

| 版本 | 有效次数 / 要求 | 指标 | min | median | max |
| --- | ---: | --- | --- | --- | --- |
| 改前 | 0 / 3 | 吞吐 MiB/s | 未测 | 未测 | 未测 |
| 改前 | 0 / 3 | clat p99 ms | 未测 | 未测 | 未测 |
| 改后 | 0 / 3 | 吞吐 MiB/s | 未测 | 未测 | 未测 |
| 改后 | 0 / 3 | clat p99 ms | 未测 | 未测 | 未测 |

total latency p99 同样未测。**差距：欠 3 次改前和 3 次改后有效采样，无法计算
距 134.8 MiB/s、80 ms 的数值差或性能倍率。** 缺少的是限时内构建完成的两个
准确版本可执行文件及其 fio 产物；Redis 和 FUSE 能力探测已成功。

## 已有五个分段计时

已定位到本地留存的历史报告 `/home/hxy/opt-reports/372/REPORT.md:47`。
以下是 `profile-overlay-completion` 的已有实测，**不是本轮 Redis 改后结果**。
历史负载为 io_uring、4 jobs、fio 3.39，当前实验为 libaio、fio 3.36；不混算倍率。

| 已有计时 | 数值 | 对应含义 |
| --- | ---: | --- |
| fio clat p99 | 68.682 ms | 原值 68.681728 ms |
| 后续提交读的内核服务累计时间均值 | 42.258 ms | 原值 42.258084 ms；解释完成后回收等待的 98.6524% |
| VFS 子采样平均耗时 | 1.217 ms | 3,613 个样本，原值 1.217260 ms |
| VFS 内 Redis | 740.680 µs | VFS 平均耗时的 60.8481% |
| VFS 内数据路径 | 432.844 µs | VFS 平均耗时的 35.5589% |

历史边界上 p99 的单个关联样本为 70.535289 ms，其中服务 2.844564 ms、完成后
回收等待 67.690725 ms。它与 fio p99 边界不同；不能将独立分位数相减，也不能把
全部尾延迟归给 Redis。原因判断：历史证据支持重复 Redis 工作经后续提交累积进
fio clat，因此整 chunk extent 计划复用是待验证假设；当前版本能消除多少尚未测。

## 已准备的正式参数与实际尝试

正式脚本已准备，但没有进入正式语料创建/采样阶段。init-volume 与 mount
均配置为 Redis `redis://127.0.0.1:6379/0`，独立 namespace
`pr375-local-redis-20261028`。每轮只恢复本 namespace 的相同 DUMP 快照并校验，
不 FLUSHDB。workspace-v1 + local-fs，同一真实 1 GiB 文件；fio 3.36，4 KiB
randread、direct=1、libaio、4 jobs、iodepth=32、20 秒、time_based、group_reporting、
randrepeat=1、randseed=375、readonly、allow_file_create=0。按最新限时要求先完成
B1/B2/B3，再进行唯一一次候选构建并测 A1/A2/A3，避免编译与测量争抢资源。

每轮新挂载、新缓存目录、sync 后 drop_caches=3。FUSE workers=16、
max_background=512、keep_cache=false、BREWFS_FUSE_READ_DIRECT_IO=1；chunk=64 MiB、
block=4 MiB、RAM read cache=8 GiB、SSD read cache=0、compression=none；关闭普通及
range 后台预取，read-plan cache weight=65536。两版本均 defaults + workspace-overlay、
release O3、debug=0、strip=symbols、incremental=0、`-j 1`。吞吐取 fio
`read.bw_bytes / 1048576`，p99 取 clat 99th percentile，另保留 total latency p99。

SQLite 能力探测成功但不计为正式测量。18:58:18–18:58:21，Redis PING 返回 PONG；
Redis workspace 初始化、FUSE 真挂载、8 MiB 直接写入、4 KiB 随机直接读取、卸载及
daemon 退出均成功。25 个探测 namespace 键已验证快照恢复前后 SHA-256 一致。
普通用户写根目录的 EACCES 已保留，后续固定用 sudo fio。

收到限时要求之前，fmt、七项 shell 检查、workspace lib/bins 已通过：1275 库测试 +2 binary
测试，0 失败，原有 262 ignored 未增加。overlay 测试在会话切换时中断，保留日志；
按最新要求不再重跑全量 overlay 或 clippy，未完成项标「未测」。available <4096 MiB 时停止 rustc；
继承构建的独立监控在 18:52:57–18:58:57、19:13:10–19:16:42 有驱动切换
造成的采样空档，未补造记录。恢复监控后每 30 秒采样；记录的 available
没有低于 4096 MiB。停止后确认 Cargo/rustc 为 0、无残留 FUSE 挂载。

证据目录：`/home/hxy/overlay-pr375-local-20261028/`。

- [实际尝试记录](/home/hxy/overlay-pr375-local-20261028/ATTEMPTS.md)
- [Redis 初始化/挂载/读写/卸载记录](/home/hxy/overlay-pr375-local-20261028/redis-probe/probe-commands.json)
- [构建参数与源码出处](/home/hxy/overlay-pr375-local-20261028/baseline-inherited-provenance.json)
- [构建现场与完整 rustc 命令](/home/hxy/overlay-pr375-local-20261028/build-before-cutoff.json)
- [超时停止结果](/home/hxy/overlay-pr375-local-20261028/deadline-outcome.json)
- [退出后状态](/home/hxy/overlay-pr375-local-20261028/final-local-state.json)

未测项：本轮随机读六次对照、当前候选分段归因、相关子集测试及未完成的
clippy。风险是用历史 io_uring 计时替代当前 libaio 结果，或把一次能力探测
当作性能验收。历史计时只作定位参考，README 性能表未更新，PR 保持 draft。
