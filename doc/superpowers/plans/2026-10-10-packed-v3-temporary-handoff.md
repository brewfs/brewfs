# BrewFS packed-v3 临时交接文档

更新时间：2026-10-10
工作区：`/home/hxy/brewfs`
本地分支：`codex/packed-metadata-aliyun-20260930`
PR：[brewfs/brewfs#160](https://github.com/brewfs/brewfs/pull/160)

## 1. 交接范围和三项创新

本项目当前只接受 packed-v3/005。旧 v2、旧 wire、flat、CR 兼容不是本轮验收目标；代码中仍存在的旧 004 helper 不构成 v3 证据，也不应继续扩展兼容协议。

三个创新的完整范围是：

1. readonly packed metadata：Redis/TiKV 管理 workspace/catalog/manifest/binding，RustFS/S3 承载 packed 对象，真实 FUSE 负责读取和错误/卸载语义。
2. dynamic immutable data placement：同一 immutable placement 由 native/packed 共享计划和 executor；支持 static/dynamic frame、inline-off、metadata/data codec 和带训练 provenance 的 p90 policy。
3. overlay workspace：S0→fork A/B→mutation→durable drain→seal/repack→原子发布→remount/next-fork→GC，并由 operator 管理 capability、binding、lease、finalizer 和恢复。

这三项必须共同满足系统出口 S 后，才能进入主实验 X。局部代码测试、native-only 结果和文档不能把 S/X 标成完成。

## 2. 当前提交与本轮修改

本轮主要实现基线是提交 706b6c5（feat(packed-v3): close generation and runner audit gaps）；随后补充了临时交接文档、稳定 Rust API 修复和 CI 资源约束；本轮最终代码提交为 e194cd3。推送后应以 git log 和 PR 页面记录实际的本地/远端 head SHA。

本轮实现内容：

- G11：wire-005 `FetchedSources` 绑定 manifest 派生的 `ReadGeneration`；generation 不匹配在填充 buffer 前返回 typed `ReadViewChanged`/`StaleView`；readonly adapter 保留 retryable downcast，observer 分类为 `FailureClass::Generation`。
- G15：p90 policy 认证 training trace digest；sample count、range、rank、histogram 累加均有边界和 checked arithmetic；runner 将冻结 policy 传给 v3 fixture，并写入 manifest/profile provenance。
- G16：local runner 记录 toolchain、seed、layout controls 和 durable `packed-v3-resource-journal-v1`；Compose、mount、worker、temporary work 的 ownership/cleanup 有 terminal event，成功仍有资源存活时 fail closed，失败也保留 cleanup decision。
- CI 工具链：把 stable 已标记 deprecated 的 `Atomic::fetch_update` 改为等价的 `try_update`，避免后续 `clippy -D warnings` 将既有 warning 当成失败；这是兼容当前稳定 Rust 的小修复，不改变业务语义。

## 3. G01–G17 逐项状态

下表是交接基线。`窄契约` 表示代码和局部证据已具备，仍不能替代外部生命周期出口；`开放` 表示不应合并为“已完成”。

| ID | 当前状态/已有证据 | 仍需完成的出口 |
| --- | --- | --- |
| G01 | **窄项已签收**：005 writer/decoder 的 kind/mode/rdev/identity 与旧 payload 拒绝已覆盖。 | 继续只验收 v3；完整当前 payload/gate 仍按 CI 运行。 |
| G02 | **rooted/frozen source 窄项已签收**：root-FD/no-follow、Btrfs readonly lease、source guard 和 46 gate/8 FUSE/3 Btrfs 证据。 | frozen-view 协议、完整 workspace publication/recovery、single-file 弱一致性边界。 |
| G03 | **source sparse/blocks/external 窄项已签收**：PM09/EX09/PS09/LE09/LD05、有界 chunks、hardlink 复用、raw/zstd 525-entry FUSE。 | >64MiB dense/all-hole、300 段 sparse、跨 chunk/page/EOF/holes、blocks/attrs/EROFS 与 workspace release/GC 的新证据。 |
| G04 | **readonly ACL/typed errno 窄项已签收**：raw names、special、hardlink、xattr、readonly ACL。 | 可写 set/remove/chmod/create 继承、后端独立实例并发/事务故障、完整 POSIX。 |
| G05 | **rank/select、SQL seek、有界 36k inventory 窄项已签收**：raw/zstd 导入、分页 cookie、restart、active fd、20 秒卸载曾通过；失败工作证据保留。 | typed GET 图、physical eviction/refetch、较大规模/hot protection、RSS 和 teardown 根因。 |
| G06 | **HTTP status-drop 子契约已签收**：backend/validated/logical/http-attempt/semantic terminal 账本守恒，非 2xx body 丢弃按 HttpStatus 失败记账。 | startup/type/retry、raw/decoded/union、probe/manifest/IP06/inline/FD/CA/payload 的完整请求图和计数。 |
| G07 | **V3 admission shutdown 窄项已签收**：Stored/Raw 阻塞 admission close 唤醒、无 partial charge。 | read/queue/pin/stored/raw/decoder/output 全生命周期预算，cancel/failure/slow consumer/eviction/shutdown、真实 FUSE/RSS。 |
| G08 | **same-snapshot unified executor 窄项已签收**：native/packed 共享 UnifiedReadPlan/Fetcher，计划/字节回读和 adapter read 局部通过。 | 四格、真实 Redis/TiKV/RustFS/S3/FUSE、同 immutable placement 的完整实验路径。 |
| G09 | **005 singleflight/profile/cancel/join 窄项已签收**：22 passed，覆盖 bounded flight、同帧单 GET、profile 和 shutdown/join。 | 生产 FUSE 跨请求 trace、stats、eviction/refetch、cold/warm、取消/卸载和性能接受。 |
| G10 | **PWB3 binding/open/attach 窄项已实现**：独立 manifest、本地 fixture/KV 初次 install、authority、missing binding reject、Absent/Hole fallback。SQLite 只作为局部回归 fixture，不是后端验收目标。 | 全 namespace/hot/cold/full/partial 的真实 mounted A/B；fork→mutation→seal→remount；Redis/TiKV 组合验收。 |
| G11 | **generation fence 本轮完成窄项**：005 FetchedSources 和旧 helper 均 typed fence；readonly retryable 证据已通过。 | 并发真实 FUSE/native/packed 生命周期的有限整体 retry、全输出丢弃、无混合 generation bytes。 |
| G12 | **same-head publication 窄项已实现**：KV timed CAS、epoch/sequence/allocator、producer/readback/manifest-last 子项；本地 transaction 仅是回归 fixture，Redis/TiKV 是目标后端。 | IP06/FD/cold/container/external 依赖闭合、durable candidate journal/staging roots、effective-view/head rotation、crash/reopen/幂等 recovery。 |
| G13 | **native PWB root/ABA/grace 窄项已实现**：root generation、Deleting 扫描前复检、层集合代数和回归。 | durable reader pins、history retirement、workspace/lease/journal roots、005 object graph mark/sweep、orphan 回收、无误删。 |
| G14 | **capability/status/CRD 窄项已实现**：v3 capability/binding conditions、缺失能力不推导 Ready、CRD panic 修复。 | API-server/CEL、Redis/TiKV Kubernetes E2E、conditions/finalizer/lease、real packed binding 和 operator recovery/GC。 |
| G15 | **本轮控制窄项完成**：static/dynamic/inline-off/raw/zstd/p90 policy 和 provenance 已接 fixture/manifest。 | offline builder 完整 distribution、同内容/group/container 规则、实际输出统计和 paired experiment。 |
| G16 | **local runner 窄项完成**：toolchain/trace/layout/resource journal/cleanup proof。 | release build、cloud dispatch、三端同 trace、10k 真实 FUSE、artifact retention、未测字段 null 和 paired acceptance。 |
| G17 | **全开放**：已有历史 1M/TTL 记录不能替代当前 005 paired acceptance。 | 完整 AGENTS gate、消融、生命周期、接受/拒绝记录、源码/vendor 可复现、最终交付。 |

## 4. Redis/TiKV/RustFS 当前实情

最新 Redis/TiKV metadata 证据不是“从未验证”：stores05 为 328 passed/0 failed/101 ignored，SDK03 为 122 passed/0 failed/1 ignored。真实 diagnostic04 的 46 项全部执行，但只有 38 passed、8 failed；失败阶段为 fork 0/2、initial composer 1/1、headless 0/2、native resume 11/1、specialized auth 0/2。对象 runtime/admin 凭据分离已接入，但尚未完成编译和真实权限验证。生产 CLI6、operator/中央 leader GC、完整同迭代门禁仍开放。

RustFS/S3 有窄的真实证据：12 objects、55,228 bytes、48 HTTP attempts/24 ranges、HEAD 零 payload、账本与库存一致，missing cold/GC suffix 被拒绝，容器和进程清理通过。parent context/namespace/codec/external digest、journal/atomic rotation、pins 和 packed GC 仍开放；这项证据不能升级成完整 RustFS/S3 生命周期完成。

## 5. 本地验证和 CI 边界

已完成的局部和提交门禁验证：

- python3 -m unittest discover -s tools/perf -p test_packed_*.py：30 passed。
- cargo fmt --all --check、git diff --check、shell/Python syntax：通过。
- CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo check --workspace：通过。
- CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo build --workspace：通过。
- CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo test --workspace --lib --bins：1177 passed、0 failed、225 ignored；bin tests 通过。
- CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo clippy --workspace：通过；只有既有 vendor dead-code/future-incompat 警告。
- cargo check -p brewfs --no-default-features --features fuse-tokio-runtime：通过。
- cargo check -p brewfs --no-default-features --features fuse-io-uring-runtime：通过。
- generation fence 定向 Rust test：1 passed；asyncfuse tokio runtime：47 passed。
- compose 脚本静态 gate（perf report、JuiceFS matrix/report）：通过。

完整 all-features 测试、operator/CRD diff 和真实 Redis/TiKV/RustFS/S3/FUSE lifecycle 仍由外部 CI/环境负责；focused tests 不能替代这些出口。

PR #160 最新 run 38061849001 已完成：Rust all-features job 以 exit 143 结束，Redis/TiKV overlay jobs 分别在 control smoke 阶段被 cancelled；日志明确为 The runner has received a shutdown signal/Terminated，没有测试断言、编译错误、OOM 或磁盘不足证据。stress-ng、pjdfstest、Results web 已通过。该 run 的失败应记录为 GitHub runner/service 基础设施中止，不归因于本轮代码回归；最终提交后仍需重跑 CI，若出现真实编译或断言失败再修复。


## 6. S/X 实验进入条件和下一步

系统 S 必须先证明：一致 S0→fork A/B→A mutation→durable drain→seal/repack→S1 原子发布→remount/next-fork→GC；源、权限、数据、namespace、恢复、资源均正确。实验 X 之后才能冻结：native/packed 同 placement/executor、static/dynamic/inline-off/p90 控制、release/provenance、固定请求 trace、Redis/TiKV/RustFS/S3 端到端、active/drain/unmount/GC 分段计时、paired 对照和接受/拒绝规则。

下一位 agent 的顺序：

1. 将本轮最终提交推送到 PR #160，触发并记录新的 CI run；纯 runner shutdown/exit 143 作为基础设施故障保留证据。
2. 在 Redis、TiKV、RustFS/S3 三条 campaign 入口执行小规模 control smoke，再扩展真实 FUSE、operator 和 paired experiment。
3. 只有外部证据齐全才更新 G08/G10/G11/G12/G13/G14/G17 的验收状态；不要用文档或合成数值关闭开放项。
4. 用户已授权合并；PR 检查结论和合并 commit 需要追加到本文件或 PR 记录。

关键文件：

- `src/workspace_overlay/packed_v3/remote.rs`
- `src/workspace_overlay/packed_v3/wire.rs`
- `src/workspace_overlay/packed_v3/wire005.rs`
- `src/workspace_overlay/packed_v3/wire005/manifest.rs`
- `src/workspace_overlay/packed_v3/readonly.rs`
- `tools/perf/run_packed_local.sh`
- `tools/perf/packed_run_manifest.py`
- `tools/perf/packed_resource_journal.py`
- `doc/superpowers/plans/2026-10-03-packed-v3-spec-gap-audit.md`
- `doc/superpowers/plans/2026-10-04-brewfs-system-readiness.md`
- `doc/superpowers/plans/2026-10-10-packed-v3-temporary-handoff.md`
