# Overlay continuation log

## Iteration 0 — design and acceptance gate (2026-10-28)

### Design

The target is the overlay read path, with the first hypothesis aimed at the
metadata work done for every 4 KiB request.  The current native resolver keys a
read plan by the exact byte range.  Random 4 KiB reads therefore almost never
reuse a plan, even when they address the same inode and chunk.  The first
architecture change is a bounded, immutable extent snapshot cache keyed by
`(workspace, head epoch, inode, chunk, inode data version)`.  A snapshot covers
one complete chunk; each request is clipped in memory.  A successful bounded
backend query populates it, and inode/view invalidation removes it.  If a
backend does not implement the bounded query capability, the old exact-range
path remains the compatibility path and no unbounded snapshot is retained.

The existing per-key upper-row cache change remains in place.  It reduces
unrelated refill loss, but it does not weaken the cancellation, uncertain
commit, lease, or ACL fences.

### Bottleneck evidence

The supplied C3 measurement is 67.4 MiB/s for 4 KiB random reads with a 160 ms
p99.  Code inspection identifies three repeated costs in the native path:

* At base `975deac`, `src/workspace_overlay/meta_layer/mod.rs:4124,4133`
  keyed and looked up the resolver cache by exact `range_start/range_end`.
* `src/workspace_overlay/stores/kv_store/extent_reads.rs:42` scanned both
  layer prefixes and read extent generations around every query.
* `src/workspace_overlay/stores/database.rs:2679` issued a range query for
  every request as well.

This is a code-based hypothesis, not a measured attribution of the 160 ms
tail. If the reported 8–12% handler share represents non-overlapping wall
time, eliminating it entirely gives only 1.09–1.14× by Amdahl's law. Queueing
or serialization outside that interval must improve to reach 2×. The new
snapshot cache does not yet expose hit/miss or fill-wait counters; existing
row-cache counters do not measure it. A matched run needs FUSE request
arrival/dispatch/reply timestamps, resolver/fill timing, and scheduler or
syscall evidence before attributing a change to the snapshot.

### Performance判据

Use the same host and workload for at least three runs and report
min/median/max.  Acceptance requires both 4 KiB random-read throughput of at
least 134.8 MiB/s and p99 of at most 80 ms, or (as the alternate gate) 100k-file
`stat` wall time of at most 600 µs.  A result that moves work into close,
flush, or drain is rejected.  The supplied C3/C4 numbers are the baseline;
they are not re-run. Only candidate measurements on the original host with
the original workload qualify; any different-host diagnostic is labelled as
such and cannot populate the README comparison table.

### Generation answer

**结论 B：部分必要。** 必须保留“旧读取不能在失效后回填”的栅栏，
但每次精确写入都让整个缓存换代并非必要。精确 key 版本是替代方案；
只做 `invalidate(key)`、没有回填票据校验，会留下 clear/read/insert 竞态。
generation 本身既不证明 lease 有效，也不能证明后端提交成功。

以下为 `0f2d665` 的完整生产行缓存 bump 调用图（测试调用不计；
未注明目录的 `.rs` 文件均位于 `src/workspace_overlay/meta_layer/`）。
全局实际增量在 `row_cache.rs:235`；逐 key 实际增量在 `row_cache.rs:245–246`。

| 调用点 | 触发条件 | 省略 bump 的具体后果 / 是否需要全局 bump |
|---|---|---|
| `mod.rs:380,383` ← `kernel_cache.rs:80` | 重新验证后的 head/base `LayerRecord` 与先前记录不同，包括同 epoch 的写入 | clear 之前启动的读可能在 clear 之后填入旧 inode/权限/路径。需要全局栅栏，除非有完整变更集合可做精确失效。 |
| `mod.rs:380,383` ← `mod.rs:767,772` | 替换 `ViewContext` 前、后各一次 | 旧 workspace/view 的读取可以跨 await 回填到新视图；只做前一次也不能隔离替换期间启动的读取。两侧均需栅栏或等价的不可变 view 身份校验。 |
| `mod.rs:395` ← `packed_permissions.rs:263,271` | 提交前、后；`RowsWritten::of` 能列全 inode/extent/xattr 影响的 inode+permission、dentry、xattr keys | 旧行可能被并发 refill 重新放回。逐 key 栅栏必要，全局 bump 不必要；不会把无关 inode 的 refill 丢掉。 |
| `mod.rs:396` ← 同上 | mutation 含 ACL，`row_keys=None`，无法证明精确影响集 | 缓存 permission 可能继续授予旧权限；保守全局栅栏必要，不能凭 TTL 替代。 |
| `mod.rs:403` ← 同上 | create 或文件 namespace mutation 提供完整受影响 inode 集；每个 inode 的 Paths/Aliases | rename/unlink 后的旧路径或别名可能重新进入缓存。精确 key 栅栏必要；覆盖目标 inode 也必须在集合内。 |
| `mod.rs:414` ← 同上 | directory mutation、删除或其他无法列全后代路径的 namespace mutation | 子孙路径仍可能指向旧目录树，whiteout/覆盖后的旧名称复活。需要全局 namespace 栅栏。 |
| `mod.rs:440,443` | 本地 lease 证明 `authoritative()==false`，upper/namespace 各清一次 | 过期期间启动的读可能跨续租回填。这里不能把 false 都当永久状态：`LeaseValidity::extend` 可延长过期 deadline；`revoke` 才永久失效。需栅栏，或经完整验证后换不可复用的 lease epoch。 |
| `row_cache.rs:231` ← `:148` (`CacheCommitGuard::drop`) | commit/replacement 被取消，或结果不是确认成功/明确 Busy 无写拒绝 | **必须永久 `disabled=true`**，否则后端在取消后完成写入，缓存又重新声称有效。disabled 之后的数值 bump 对授权已冗余（get/insert 均绕过），本轮保守保留；不能把 disabled 改成“清一次再恢复”。 |

`packed_permissions.rs:263,271` 包围所有三类提交入口：`:220–221`
普通 mutation、`:231–232` create、`:243–245` namespace mutation。
`RowsWritten::of/of_create` 在 `mod.rs:206–272` 定义精确影响集。
明确 Busy 的无写拒绝仍做保守行失效，但不使 namespace cookie 换代；
这是避免错误重用与避免无谓目录重试两个独立问题。

另外几种 generation 不可混为行缓存计数：

- `namespace_epoch.rs:40`，从 `:84,86` 的 guard drop 调用：成功的
  dentry 提交或 view 替换使旧 readdir/path snapshot 无效；不确定/取消
  同时永久设置 `uncertain`。明确无写 Busy 不 bump。取消后的数字变化
  不能替代 `uncertain`。
- `stores/kv_store/native_slice_deletion.rs:255` 更新持久化 extent
  generation；compaction 可以改写旧 sequence 并删除后面的 mask，
  没有前后代次检查的分页读会拼出从未存在过的 extent 历史。本轮回归
  测试专门证明它会把 hole 遮住的数据重新读出。
- `src/vfs/io/reader.rs:1278` 的 slice generation 与
  `src/vfs/inode.rs:176` 的 data epoch 是数据读取/写回栅栏，不能用行
  cache key 版本取代。后者生产调用在 `src/vfs/fs/mod.rs:4214,4451,4822,4880,7052`
  及 `src/vfs/io/writer.rs:6381`；这些调用未修改。

本轮审计追加的全局 bump 位于 `row_cache.rs:244`，在逐 key 表清理之后。
原 bump 仍在 `:235`，逐 key 增量现位于 `:253–254`。原因是读取可能在
第一次全局 bump 后捕获 `(新 global, key=0)`，并发写把 key 推进到 1，
随后 clear 又把它变回 0；没有第二道全局栅栏，旧票据会错误恢复有效。
这不是每次精确写都全局 bump；只有全量清理才需要封住这个重置窗口。

替代方案的风险：精确集合漏 key 会造成 stale read；全量清理逐 key
版本表必须避免 ABA 和并发 refill；当前 DashMap 会随不同写入 key 增长，
直到全量失效才清理，不能把它称为有界缓存。这是继承的 WIP 风险，
不是已获性能验收的方案。

红线逐项保持：`src/fuse/mod.rs:3885` 的 `permission_cache_ttl()` 对可写
POSIX ACL 返回 `Duration::ZERO`；`row_cache.rs:229–231` 的永久 disabled
及 `:189,194,208,213` 的读取/回填检查不删除。全局/逐 key bump 都不能
绕过这些权限与取消栅栏。

### Packed-v3 object-side answer

The code establishes the layout but the current workspace has no mountable C4
volume or retained object-server/ESSD read log. Paths in this section are
relative to `src/workspace_overlay/packed_v3/`. Ordinary content objects use
`{prefix}/objects/{kind as u8}/{sha256}` (`wire005/producer.rs:450-458`);
authenticated index pages use `{prefix}/indexes/{kind as u8}/{sha256}`
(`wire005/index_builder.rs:153-159`).  PM11 stores seven independent roots:
Groups, Inodes, Containers, Frames, ColdAttributes, ReverseNames, and
LargePlacements.  Group metadata and frame payloads are in GroupContainer
objects; external placement uses an inode+big-endian file-offset selector key
(`wire005/placement.rs:135-137`) and LargeData objects.

Object kind numbers are `Manifest=0`, `GroupContainer=1`, `ColdAttributes=2`,
`FrameDirectory=3`, `LargeData=4`, then indexes `Group=5`, `Inode=6`,
`Reverse=7`, `Container=8`, `Frame=9`, `Cold=10`, `Large=11`, `SourceStats=12`
(`wire005.rs:92`). An extent is a logical range/placement, **not one object**.
GroupContainer holds group metadata and compressed frames; larger external
data uses frame placements in LD05 chunks and an authenticated FD05 directory.
Inline files must be below 256 KiB and fit the group budget (`meta.rs:30`).
LD05 code bounds are raw chunk ≤16 MiB, body ≤24 MiB, ≤1024 frames, individual
raw frame ≤8 MiB (`wire005/large_chunk.rs:10,55–65`). These are format bounds,
not observed extent sizes or 4 KiB read amplification.

| Required measurement | Result |
|---|---|
| One cold/warm `stat`: GET count and returned/device bytes | **未测** |
| One cold/warm 4 KiB `read`: extent length, GET count, range bytes and device bytes | **未测** |
| Packed object count and total stored bytes for the same 100k files / 8 GiB corpus | **未测**; cannot compare against C3's 373,040 Redis keys / 192 MB |

The retained `/home/hxy/exp-results-redis/c4/REPORT.md` records three mount
failures **before FUSE attach**: `packed workspace binding requires complete
catalog permission/ID/lifecycle capabilities`. The fixture there has only
**1 file, 96,435,616 logical/source bytes and 23 extents**; those are neither
stored-object totals nor an equivalent 100k-file corpus. Prerequisites are a
catalog with complete packed capabilities (currently Redis/TiKV), a valid
retained manifest/fixture, its object endpoint, and server request logs or
object-server block-device counters. Do not silently change C4 from SQLx to
another catalog and label it the original C4 configuration.

Measurement must keep mount-time manifest/index reads separate, retain all
GET/range records for each isolated operation, distinguish cold and warm
caches, and count unique object keys plus actual response bytes. Device
`read_bytes` includes cache/EC effects and must name the observed process or
device. Original C3 `measure_w1.sh` sampled fio descendants' `/proc/*/io`,
**not the object server**; its roughly 51 MB cannot establish object-side
read amplification. No object count or byte total is inferred from it.

### Risk and rollback

The snapshot is bounded by the existing `MAX_UPPER_EXTENT_ROWS` backend limit
and a weighted Moka capacity.  A chunk with too many rows, an unsupported
bounded backend, or any cache miss follows the existing exact-range resolver.
The change can be rolled back by removing the snapshot lookup/fill while
leaving the cache invalidation tests and fences intact.

## Architecture iteration 1 — extent snapshot cache

Status: unaccepted performance candidate; correctness review and final local
gates passed. The existing WIP commit is `0f2d665`; it is not a claim of a measured
optimization win. Only the candidate can be measured next; supplied baseline
artifacts are preserved.

### 设计

从按精确 byte range 缓存，改为按 inode data version 缓存完整 chunk 的
extent 计划，在内存中裁剪每次请求。逐 key 行缓存是继承的前一方向；
本轮不把它再当作主要性能假设。完整设计、代次审计和 packed 前提见上文。

### 判据

原 C3 同机、相同 workload，至少三次，报告 min/median/max：randread
≥134.8 MiB/s 且 p99 ≤80 ms，或 100k-stat wall ≤600 µs；完整正确性
门禁和混合 I/O、metadata、drain/teardown 回归检查都必须满足。

### 基线（仅重新汇总已有产物，没有重测）

| C3 W1 randread，3 次 | min | median | max |
|---|---:|---:|---:|
| MiB/s | 66.934348 | 67.432175 | 68.055190 |
| IOPS | 17,135.193240 | 17,262.636868 | 17,422.128894 |
| p99 µs | 160,432.128 | 160,432.128 | 162,529.280 |

Source: `w1.csv` and `raw/w1-randread-rep{1,2,3}.json`; fio 3.36,
`libaio`, `bs=4k`, `direct=1`, `numjobs=8`, `iodepth=32`, `size=1G`,
`time_based`, `runtime=20`, one **shared** filename. Each repetition remounts,
drops caches, and sets `fuse_keep_cache=false`. Mount arguments come from
`raw/measure_w1.sh` and `raw/mount-w1-w1-randread-1.log`, not the archive's
directory name. This SQLx/local-fs result does not establish EC/S3 performance.

### 改动与正确性复现

The WIP was preserved in commit `0f2d665` during validation. Review found that
its KV snapshot fill used `get_extent_deltas_bounded`, which bounds row count
but uses `scan_prefix_bounded` without the ordinary read's before/after extent
generation fence. Redis's implementation separates ZRANGEBYLEX from MGET and
can page across a compaction. A mixed scan can revive bytes hidden by a hole;
caching that scan is unacceptable. The fix will reuse the existing paged,
byte-bounded, generation-checked reader with a caller-supplied row limit.
The 1024-row snapshot limit and the ordinary fallback limit stay distinct.
The correctness criterion is the existing real-compaction interleaving oracle
passing for both the ordinary query and the bounded snapshot query, without
removing any assertion. Sustained churn must still terminate with Busy.

The reproduction ran **1 test, 0 passed / 1 failed**, exit 101, before the
reader fix. The oracle expected byte 1 to be `Hole`, while the bounded API
returned `Data { slice_id: 777, slice_offset: 7 }`. It replays an actual
atomic compaction mutation between scan pages; the original ordinary-reader
assertions are unchanged. Artifacts:

- `/home/hxy/overlay-refactor-evidence-20261028/extent-race-before.log`
- `extent-race-test.patch` in the same directory, applied to `0f2d665`, SHA256
  `62934ff20fb0b446227a27edab2324fd6619a8481d5c65bfd45d32071a585aae`.

The implementation now routes both ordinary and bounded KV queries through
`read_extent_deltas_consistent(request, max_rows)`: generation reads surround
every attempt, including a failed/over-budget scan; at most three attempts
are made. Pages retain byte limits and ask for at most the remaining row
allowance plus one overflow sentinel. Raw rows from both layers count before
range filtering, so a narrow range cannot conceal an over-budget history.

A second review finding is that the WIP gives each of its two Moka caches
the full configured weight, doubling the aggregate resolver budget. Before
measurement, exact-range plans and full-chunk snapshots will use one weighted
cache with a tagged key. Both remain scoped to workspace/epoch/inode/version;
inode and workspace invalidation must remove both kinds. The existing segment
weight formula stays unchanged. A mixed-entry retention test must show that
the sum stays within one configured budget. This avoids attributing an extra
cache allowance to the architecture change.

Both entries now use one `Cache<ResolverCacheKey, Arc<ResolvedReadPlan>>`.
The key's Range/Chunk tag preserves the two lookup shapes. One-segment plans
still cost two weight units, with a single default total of 65,536. Existing
invalidation assertions remain; a mixed-entry test checks aggregate retained
weight. A nested permission-cache insertion was also flattened to a let-chain
to satisfy overlay-enabled `clippy -D warnings`, without changing behavior.

A third review finding was an ABA race in the inherited per-key generations:
a full invalidation first advances the global epoch; a read then takes a
zero key-version ticket; a concurrent local write advances that key; clearing
the version map resets it to zero. The old ticket can match again. The new
`clearing_key_generations_cannot_revive_a_racing_refill_ticket` test pauses at
that exact boundary. It failed before the fix and passed after a second
global epoch advance following the map clear. Fresh tickets still work, and
precise invalidations still retain unrelated tickets.

The fast before/after proof uses the real `row_cache.rs`, Moka and DashMap
with inert row payload types in a standalone test harness. It ran 1 test
with exit 101 before, 1 test with exit 0 after; **this is supplementary,
not a replacement for the full workspace gate**. The actual test is also
included in the repository's overlay suite. Evidence is in
`row-cache-race-{before,after}.log`, `row-cache-race-test.patch`, and
`row-cache-race-{harness.rs,check.py}` under the same evidence directory.
The first final-clippy attempt was deliberately interrupted to apply this
fix; its log is preserved as `clippy-overlay-interrupted-for-race-fix.log`.

The original C3 evidence has now been located under
`/home/hxy/exp-results-redis/c3/`. Its W1 fio JSON and mount logs establish
`libaio`, direct=1, jobs=8, depth=32, a **shared 1 GiB file**, 20 seconds,
SQLx metadata, and local-fs data. The archive directory name does not turn
these W1 results into Redis/S3 results. The supplied Redis key-count measurement
is kept separate. The current build host is `brewfs-singapore-v2`, with no
`/work/d1`–`/work/d4`; the original host identity is being sought. A default
compose invocation was stopped during compilation before any fio run because
it would have built flat-v1 and used the wrong workload. It yields no perf data.

### 改后测量

本轮没有启动改后 fio：当前构建主机 `brewfs-singapore-v2` 没有原 C3
的 `/work/d1`–`/work/d4`、catalog 或 data。对已知 us-east-1 C3 实例和
磁盘 ID 的只读云查询均返回空；`/home/hxy/target-opt/release/brewfs`
也不存在（只有 release 中间产物）。因此没有把其他主机、旧候选或不同
工作负载当作改后结果。

| 指标 | 改前 C3 min | 改前 C3 median | 改前 C3 max | 改后候选 min | 改后候选 median | 改后候选 max | 倍率/判定 |
|---|---:|---:|---:|---:|---:|---:|---|
| 4 KiB randread throughput (MiB/s) | 66.934348 | 67.432175 | 68.055190 | 未测 | 未测 | 未测 | 不可计算 |
| p99 (ms) | 160.432128 | 160.432128 | 162.529280 | 未测 | 未测 | 未测 | 不可计算 |
| 100k-file stat wall (µs) | 未测 | 未测 | 未测 | 未测 | 未测 | 未测 | 不可计算 |

存档基线的真实参数是 fio 3.36、`libaio`、`direct=1`、`numjobs=8`、
`iodepth=32`、共享 1 GiB 文件、20 秒；本轮用户指定候选为 4 jobs，
所以即便候选可运行，也不能把它与 67.432175 MiB/s 宣称为同参数倍率。
本轮缺少原 C3 主机（或等价的同机 catalog/data 与挂载权限）和可运行的
候选 binary，改后 throughput、p99、100k-stat wall 及倍率均保持未测。

Final local gates, run after the correctness fixes and with the source frozen:

| Gate | Result |
|---|---|
| `cargo test -p brewfs --lib -j 4` | **1275 passed, 0 failed**, 262 existing ignored |
| `cargo test -p brewfs --lib --features workspace-overlay` | **2568 passed, 0 failed**, 451 existing ignored; both race regressions and shared cache budget test passed |
| `CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo test --workspace --lib --bins` | **1275 library + 2 binary tests passed, 0 failed**; 262 existing library ignores |
| `cargo clippy --all-targets -- -D warnings` | exit 0 |
| `cargo clippy --workspace --all-targets --features workspace-overlay -- -D warnings` | exit 0 |
| `cargo clippy --workspace` | exit 0 |
| `cargo check --workspace`, `cargo build --workspace` | both exit 0 |
| `cargo check -p brewfs --no-default-features --features fuse-tokio-runtime` and `fuse-io-uring-runtime` | both exit 0 |
| `cargo fmt --all --check`, `git diff --check` | exit 0 |
| Four runner `bash -n` checks and the three required perf report/matrix scripts | all exit 0 |
| Conditional rfuse3 runtime checks | not applicable: `cargo metadata` confirms rfuse3 is not a workspace member |

No new `#[ignore]` or deleted assertion expressions. The existing ignores
remain visible in the totals; live mounted POSIX/performance suites were not
substituted with unit-test numbers. All ten final Rust commands and their exit
codes are recorded in `final-gates.json`; each has a corresponding
`*-final.log` under `/home/hxy/overlay-refactor-evidence-20261028/`. Shell and
format checks are in `shell-format-final.log` and `format-after-race-fix.log`.
The tested source delta against `0f2d665` is retained as `candidate-code.patch`,
SHA256 `984db67e30ffff5eda86d868c2304520dbfc1c5cafe021cb901d7f4dce37c129`.

### 结果

**未达判据（缺少同机测量条件，不能判定性能增益）**。当前主机是
`brewfs-singapore-v2`；只读查询所用云 profile 的 us-east-1 实例列表为空，
本机没有原 `/work/d1`–`/work/d4` 和 C3 卷。需要原 C3 SSH 别名或实例标识
及保留的 catalog/data。未把其他新加坡实例当作同机，未重新测 supplied
baseline，未更新 README 接受表。

后续候选的三次跑法必须保持上述条件，并附混合读写、metadata、drain
和 teardown 回归记录。若原机器已释放，不能声称满足本轮“同机”判据；
应由用户确定新的比较约束。直到恢复测量条件，不继续叠加第二个
未经归因的性能方案。

### 这条路径的上限与继续条件

精确-range cache 的重复 miss 确实存在，但“每次 metadata work 变少”
尚不能推出 fio wall 翻倍。若主要时间是 FUSE 请求排队、调度、驱动批量
分派或 inode 锁竞争，必须证明 metadata 减负能缩短这些等待；若是对象
读取，则 extent cache 不改变对象字节数。100k-stat 工作集也不直接使用
extent 快照，因此本方案不能承诺解决该替代判据。需要原机的 off-CPU/
syscall/FUSE 时间分解来选下一轮架构方向。两轮无可测收益时停止的规则
仍有效；本轮没有有效候选跑分，不能冒充成功或失败测量来凑轮次。

Builds now run through `/tmp/brewfs-overlay-build-guard.py`, with persistent
30-second `free -g` and available-byte/disk-byte samples under
`/home/hxy/overlay-refactor-evidence-20261028/`. The guard enforces the requested
2 GiB available / 6 GiB free floor and `pkill -x rustc` on a breach. Prior
manual resource samples were less frequent than 30 seconds; no resource breach
was observed, but those samples do not satisfy the requested sampling interval.

The final guarded gates recorded minimum available memory **4.426 GiB** and
minimum free disk **22.464 GiB**, above the 2/6 GiB stops. Every build inherited
`CARGO_PROFILE_RELEASE_DEBUG=0`, `CARGO_PROFILE_RELEASE_STRIP=symbols`,
`CARGO_TARGET_DIR=/home/hxy/target-opt`, and `CARGO_BUILD_JOBS=2`; the requested
library-test command explicitly uses `-j 4`. The exact guard and serial gate
runner are retained as `build-guard.py` and `final-gates.py` in the evidence
directory. Compose cleanup (`down -v --remove-orphans`) completed; accepted
and supplied artifacts were preserved.

### PR

Stage 1 is the inherited WIP commit `0f2d665`, containing per-key row versions
and chunk extent plans. Stage 2 is `fa56fa5` (`fix(overlay): fence extent
snapshots and cache version resets`): the two demonstrated race fixes and
one shared resolver budget, after the final local gates above.

[Draft PR #375](https://github.com/brewfs/brewfs/pull/375),
`perf(overlay): cache native extent plans by chunk`, targets `main` from
`perf/overlay-cont`. This is **not an accepted optimization**. Its body
records the architecture, baseline min/median/max, missing candidate numbers,
correctness evidence, and remaining risks. The exact description is also
retained at `/home/hxy/overlay-refactor-evidence-20261028/PR-DRAFT.md`.
README performance tables remain unchanged; matched performance acceptance
and a mountable packed fixture are still outstanding.
