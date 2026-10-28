# Issue #371：native lower 4 KiB 随机写延迟

关联 issue：https://github.com/brewfs/brewfs/issues/371
工作目录：`/work/brewfs`；分支：`perf/issue-371-native-write-latency`。
本轮从 2026-10-28 11:36 UTC 开始，截止 12:16 UTC（北京时间 20:16）。
本分支的 draft PR 已创建：https://github.com/brewfs/brewfs/pull/376 。

**本轮收口：唯一一次构建因时间预算主动停止，direct=1 三次均未测；没有可报告的本轮性能数字。**

## 范围与口径

只处理 4 KiB 随机写定位及可取得的测量；重复打开未测。
原 issue 的对照为：

| 原 issue 配置 | 吞吐 MiB/s | IOPS | p99 ms |
|---|---:|---:|---:|
| overlay + native lower | 3.7 | 955 | 4280 |
| overlay + packed lower | 49.6 | 12689 | 101 |

4,280 ms 在 `/work/ISSUE.md` 中是 **p99**；本轮不将其改称 p95。
原环境为四配置并行、4×250 GiB ESSD PL1、EC 2+2、Redis，
每格三次取中位数、drop_caches + 重挂载。完整 fio 命令及文件来源未给出。

本机是 8 vCPU、32 GiB RAM、单 ext4 数据盘，Redis DB 13、本地目录对象后端；
没有复现原 ESSD/EC/S3 拓扑。运行时为 `workspace-v1`，比较 native lower 和
packed-v3 lower。两种 lower 均只有根目录；4 个 64 MiB 工作文件在 upper
创建、预填充、fsync，卸载后 drop_caches 再重挂载。因此测的是 upper 文件随机
覆盖写，不是 lower 既有文件首次 copy-up。本机数字只能作诊断及原 issue 的参考对照。

`workspace-native-v2` 是另一条路径。启动时已有的 `src/cli.rs` row-cache 改动
属于该路径，不是本 issue 的 native-lower 修复；其他已有 Rust 改动及
未接线的 `src/vfs/write_timing.rs` 保留在本机，不作为本 PR 的修复提交。
本轮未重新引入已撤回的 IdBlock 候选。

## 本机基线

### 本轮：唯一构建因时限停止，direct=1 未测

只执行一次：

```bash
CARGO_TARGET_DIR=/work/target CARGO_INCREMENTAL=0 CARGO_PROFILE_RELEASE_DEBUG=0 \
  cargo build --release --features workspace-overlay --bin brewfs -j 1
```

构建于 11:37:03 UTC 开始，12:08:29 UTC 因本轮 40 分钟预算主动停止，
耗时 **1886.514 秒（31 分 26 秒）**。Cargo 退出码为 -15（SIGTERM）；
这是主动停止，不是已确认的 Rust 编译错误。未进行第二次构建，未启动 fio。
记录：`.issue371/round-20261028-1136/build.json`、`build.log`、
`build-memory.log`、`source.patch`、`status-before.txt`。
源 HEAD 为 `975deacc13131335b60a4c72de4ed781bbfe7154`，构建包含已有工作树改动；
不能将此构建声称为原始 main 或已验证的优化版本。

停止前没有 Rust 编译错误，无法提供不存在的“编译错误前 20 行”。以下为构建日志末 20 行原文；
完整日志保留在本机，PR 中附 `current-build-last20.txt`、最终构建 JSON、停止原因及来源哈希。

```text
   |
31 | pub struct Participant {
   |            ^^^^^^^^^^^

warning: struct `Participant` is never constructed
  --> vendor/tikv-client/src/generated/tsopb.rs:63:12
   |
63 | pub struct Participant {
   |            ^^^^^^^^^^^

warning: associated function `new` is never used
  --> vendor/tikv-client/src/region_cache.rs:64:12
   |
63 | impl<Client> RegionCache<Client> {
   | -------------------------------- associated function in this implementation
64 |     pub fn new(inner_client: Arc<Client>) -> RegionCache<Client> {
   |            ^^^

warning: `tikv-client` (lib) generated 26 warnings
   Compiling brewfs v0.1.2 (/work/brewfs)
```

以下是**未执行**的计划命令，不表示已测量三次：

```bash
python3 -u tools/perf/issue371_local.py \
  --binary /work/target/release/brewfs \
  --fixture .issue371/base-bin/packed_v3_snapshot_fixture \
  --output .issue371/round-20261028-1136/native-direct \
  --arms native --direct 1 --repeats 3 --runtime 20 --size-mib 64 --jobs 4
```

fio 参数为 `randwrite / bs=4k / direct=1 / libaio / numjobs=4 / iodepth=32 / runtime=20`。
FUSE 的 `BREWFS_FUSE_DIRECT_IO=0` 与 fio 的 O_DIRECT 分别记录，不混同二者。
计划每轮保留 fio JSON、命令、Redis commandstats、写回债务、fsync 和卸载结果。
计划取 fio `write.lat_ns.percentile["99.000000"]` 作为总 I/O 延迟 p99，
先求每轮 p99，再报告三个 p99 的 min/median/max；本轮没有产生这些样本。

| 本轮 native direct=1 | 吞吐 MiB/s min / median / max | p99 ms min / median / max | 有效次数 |
|---|---|---|---|
| 当前工作树 | 未测 / 未测 / 未测 | 未测 / 未测 / 未测 | 0/3（构建未完成，实际执行 0 次） |

与原 issue 的 3.7 MiB/s / p99 4280 ms 对照：本轮吞吐及 p99 均未测，差距与收益均不能计算。
未达到 issue 的吞吐至少翻倍、p99 至少减半的验收要求；不据旧的无效样本声称达到目标。

### 此前 buffered 诊断：六个样本全部无效

此前可追溯 release 二进制 SHA256：
`b706d6ac81ebba4c2824d8d977995a3541d60f6ee542e26dc4cb982b56f964b2`。
其命令与源快照在 `.issue371/base-bin/provenance.json`；
测量配置在 `.issue371/before-v1/profile.json`，其中 **direct=0**，
不能与本轮 direct=1 作为配对前后收益。

native 三轮 fio error=0，但全部后续 fsync EIO；packed 三轮卸载退出码 1，
后两轮 fio error=5。全部记录包含 `Failed to get slice id: ... EBUSY`。
以下仅列原始诊断数，不能作为有效性能基线或验收收益：

| lower | BW MiB/s min / median / max | p99 ms min / median / max | 有效次数 |
|---|---|---|---|
| native | 2.415728 / 2.758891 / 2.772152 | 826.277888 / 1283.457024 / 1719.664640 | 0/3 |
| packed | 1.588708 / 1.716587 / 1.769946 | 1803.550720 / 2499.805184 / 2533.359616 | 0/3 |

数据见 `.issue371/before-v1/raw-invalid-summary.json` 及各轮原始记录。
native r1 的 fio wall 为 159.14 秒，后续 fsync 约 21.98 秒；
只看 active BW 会掩盖失败的排空，不能据此宣布优化有效。

## 分段定位

### 已实测：轻负载 pwrite 包含时间

此前使用现有 span-close 日志，每种 lower 各 2048 次同步 4 KiB pwrite；
未接入遗留 `write_timing.rs`。各段是**包含时间**，
占比分母为平均用户 pwrite，不能相加，也不能代替重负载 fio 的完整分解。

| 段 | native 平均 µs / pwrite 占比 | packed 平均 µs / pwrite 占比 |
|---|---|---|
| 用户 pwrite | 601.118 / 100% | 661.825 / 100% |
| FUSE handler | 96.856 / 16.11% | 116.281 / 17.57% |
| VFS write | 93.502 / 15.55% | 111.331 / 16.82% |
| FileWriter write_at | 79.067 / 13.15% | 82.894 / 12.52% |
| chunk/slice write_at | 73.173 / 12.17% | 76.412 / 11.55% |
| 内存 try_write | 15.006 / 2.50% | 15.384 / 2.32% |
| stat_ino_checked | 7.183 / 1.19% | 8.185 / 1.24% |

native 后续四次 fsync 合计约 6.716 秒，packed 约 4.582 秒，八次均 EIO。
来源：`.issue371/breakdown-before/span-summary.json` 及各轮 pwrite/trace/stats。
用户到 FUSE 差额包含 Python、内核及派发；未继续拆分。
whiteout、lower 查询、generation 校验未单独计时，不能推为零。

### 已观察及日志推断：allocator CAS 反复失败

此前重负载 native r1 的 fio 平均 slat 为 5653.576 µs（3.12%）、
clat 为 175388.534 µs（96.88%）。重新核验该轮 redis-before/after：
全实例 EVALSHA 从 167664 增至 7070691，增量 **6,903,027**，覆盖 fio 和排空区间。
这修正了此前约 491 万的中间采样描述；全实例计数包含多类脚本，不能全部算作 allocator 重试。

native r3 的 240 行 MONITOR 短采样包含 20 次 allocator CAS，
使用相同旧值，脚本执行到 GET 后没有 SET。按 `stores/redis.rs` 中 CAS Lua
的检查顺序，推断这 20 次是比较失败；MONITOR 本身不记录脚本返回值或逐写耗时。
来源：`.issue371/before-v1/r3-native/redis-monitor-sample.txt` 和
`redis-monitor-summary.json`。这是本机异常运行的证据，不是原 issue 的实测归因。

### 代码推断：并发补充 ID 块与 native 元数据提交的 CAS 重试

- `vfs/io/writer.rs::spawn_upload_task` 在分发上传块前调用
  `meta.next_id(SLICE_ID_KEY)`，经 `WorkspaceMetaLayer::next_id`
  到 `KvWorkspaceStore::allocate_id("slice")`。ID 块有余量时走本地 `take()`；
  不是每个 4 KiB 用户写都必然单独发一次远端 CAS。
- `IdBlock` 只有保护本地范围的同步 mutex；空块时不串行化跨 await 的远端补充。
  多个调用者可同时各自预留 64 个 ID，`refill()` 只保留首次仍为空时的范围，
  其余已预留范围被丢弃；refill 与再次 take 之间也可被其他调用者耗尽。
  `allocate_id_block_named` 最多进行 `CAS_MAX_RETRIES=256` 次读/CAS，
  失败后 `yield_now()`，耗尽返回 Busy，可沿上传失败进入 fsync EIO/写回背压。
- 普通 native 的 `commit_with_written` 走 `apply_versioned_mutation`，
  进入 `hot_mutation_for_version`。它每次读取 workspace/head/lease（及 expected base），
  重新检查权限版本和 lease，准备提交并做 CAS；冲突可在 256 次上限内重试。
  packed 走 `apply_packed_versioned_mutation`，store 有本地
  `packed_mutation_gate`。MetaLayer 本身也有 `mutation_gate` 和分组提交，
  因而不能仅凭 packed 的额外 gate 断言 native 提交没有串行化。
  这是两条提交路径的代码差异，尚无本轮分段计时或竞争率证明。

已有内存后端确定性单元测试曾暂停 allocator 读，让 64 个调用者到达第一次挂起点；
它不使用 Redis，也不是 fio 重放。
旧实现实测失败：并发 allocator 读 64 次，durable next-ID 为 4097，
预期为 65；这证明了本地重复预留，不证明修复后 fio 会变快。
来源：`.issue371/id-block-before-test.log`，0 passed / 1 failed。
此前尝试的每块 async refill gate 候选及测试已撤回并归档为
`.issue371/id-block-candidate-stopped.patch`、
`.issue371/id-block-concurrency-tests-stopped.rs`；
after-test 编译曾被中断，没有通过结果，本轮不重新启用候选。

因此当前假设是“写回 ID 分配/元数据提交存在 CAS 放大和排队”，
仍不能把全部 fio clat、whiteout 成本或原 native/packed 的 13 倍差距归因于此。

## 改动与门禁

本 PR 提交可复跑的本机诊断 runner、报告及必要的小型证据；
未提交或接受生产路径优化。runner 新增 `--arms native`，以便本轮只跑指定三次。
Python 语法编译、`--help`、`cargo fmt --all --check`、`git diff --check` 已通过。

PR 内的 `doc/superpowers/plans/issue371-evidence-20261028/` 保存关键历史摘要、
MONITOR 原始短采样、失败测试摘录，以及本轮构建状态、停止原因、编译器版本、
构建时工作树补丁和来源 SHA256；完整原始大日志仍保留在本机 `.issue371/`。

本轮构建仅一次，`-j 1`，目标目录 `/work/target`，每 30 秒记录 `free -m`。
按本轮明确限制，不跑全量 cargo test。若执行 Rust 测试，仅允许
`cargo test -p brewfs --lib workspace_overlay::`（需启用 workspace-overlay 功能）。
本轮尚未执行该子集；此前失败回归是历史证据，不能算作当前门禁通过。

没有接受性能收益或更新 README 性能比较表。完整 CI、clippy、
compose randrw/metadata/drain 门禁尚未完成，因此此 PR 保持 draft，
不能作为 issue 已修复的声明。

## 未验证项

- 原 ESSD/EC/S3 拓扑下的配对三次测量与原生/packed 差距。
- 当前工作树的有效 direct=1 三次结果、完整重负载分段和逐次 CAS 等待时间。
- lower 既有文件首次覆盖写、whiteout/lower/generation 的独立开销。
- packed 重复打开、混合读写、元数据压力、非 root 权限和写回排空回归。
- IdBlock 候选的通过测试、取消/故障语义、实际性能及完整 CI。

本轮不放松 ACL TTL、whiteout、隔离、权限、open-unlink 或 lease 语义。
