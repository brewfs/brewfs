# BrewFS Metadata Maintenance And Performance Stability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Keep metadata GC/compaction assistance correct while preventing recent maintenance changes from causing measurable read/write performance regressions.

**Architecture:** Treat metadata maintenance, read retry recovery, local block-cache persistence, and perf tooling as one guarded change set. Metadata stores must reject stale compaction replacements, the VFS read path must refresh stale slice metadata only on transient failures, and disk-cache population must remain best-effort without making foreground or mixed fio workloads pay avoidable local I/O cost.

**Tech Stack:** Rust, Tokio, SeaORM/SQLite, Redis, etcd, `bytes::Bytes`, moka cache, FUSE/VFS tests, Docker compose xfstests perf scripts, fio, perf/inferno flamegraph tooling.

---

## Current Baseline

| Item | Status | Evidence |
| --- | --- | --- |
| Baseline commit before this perf iteration | Done | `137a96797 Improve metadata maintenance and cache perf` |
| Baseline full unit/integration suite | Passing | `cargo test -p brewfs --lib --bins --tests` |
| Script syntax checks | Passing | `bash -n tools/perf/run_perf.sh docker/compose-xfstests/run_perf_in_container.sh docker/compose-xfstests/run_juicefs_perf_in_container.sh` |
| Whitespace check | Passing | `git diff --check` and `git diff --cached --check` |
| Focused perf check | Improved from low run | `docker/compose-xfstests/artifacts/perf-run-1780147102-22090` |

Perf comparison from the focused rerun:

| Workload | Low run `1780127871-11806` | Focused rerun `1780147102-22090` | Direction |
| --- | ---: | ---: | --- |
| `fio-randrw` read BW | 80.30 MiB/s | 114.68 MiB/s | Better |
| `fio-randrw` write BW | 36.67 MiB/s | 52.89 MiB/s | Better |
| `dirperf` wall time | 59s | 28s | Better |

The focused rerun is not a replacement for a full perf baseline because it only ran `fio-randrw dirperf`. Use it as a regression smoke result, not as the final performance certificate.

## 2026-05-30 Focused Performance Iteration

This iteration targets the recent metadata/cache hot paths that showed up in Docker compose perf runs, while keeping the previous GC/compaction safety work intact.

Implemented changes:

| Area | Change | Intent |
| --- | --- | --- |
| Disk cache | Opportunistic disk persistence now inserts hot cache first and skips background disk writes when write permits are saturated | Prevent mixed fio/write workloads from queueing avoidable local disk I/O |
| Disk cache | Disk-store paths accept `bytes::Bytes` | Avoid unnecessary `Vec` copies after writes and compression |
| Chunk store | Fresh write cache population keeps block data as `Bytes` | Preserve write visibility with fewer allocations |
| VFS create/mkdir | `create_file_at()` and `mkdir_at()` try the metadata mutation first, then fall back only for semantic errors | Remove redundant pre-stat/lookup on the successful create path |
| FUSE create/mkdir/mknod | Strict create-new semantics use `mkdir_at_new()` and `create_new=true`; `create` honors `O_EXCL` | Keep FUSE errors correct without extra parent/child probes |
| FUSE create/open | New entries use cached attrs and skip immediate close-to-open stat refresh | Avoid a redundant metadata stat right after create |
| FUSE setattr | `apply_new_entry_attrs()` skips no-op `set_attr` when uid/gid/mode already match | Avoid needless metadata writes on create-heavy workloads |
| Redis metadata | `create_entry()` updates the local node cache for parent and new inode after successful Lua create | Avoid immediate follow-up Redis reads from the same client |
| Redis metadata | `unlink()` now performs dentry lookup, node lookup, type check, nlink update, deleted marker insertion, hardlink parent restoration, and parent timestamp bump inside one Lua operation | Remove duplicate Redis round trips on successful unlink while preserving directory rejection and hardlink semantics |
| Redis metadata | `rename()` lets Lua perform source/destination dentry lookup and return invalidation inodes; overwritten file targets are tombstoned and queued for cleanup | Remove two Rust-side Redis dentry lookups on successful rename while preserving POSIX overwrite semantics and GC visibility |
| FUSE/VFS open | `open_fresh_ino()` performs the required fresh stat once and FUSE `open()` no longer does an extra cached stat before opening | Preserve close-to-open freshness while removing a duplicate metadata lookup on normal open |
| Redis metadata | `create_entry()` lets Lua do the parent directory lookup and setgid inheritance check; Rust updates only local node cache from the Lua result | Remove a duplicated Redis parent `GET` on cold create paths |
| Meta client open | `stat_fresh()` refreshes cached file metadata in place while still dropping stale slice/parent/children state | Avoid reallocating the inode cache entry on every close-to-open refresh |
| VFS open/read | File handles create `FileReader` lazily on first committed read instead of during open | Remove reader allocation and reader registry work from open-only workloads |
| Meta client create/mkdir | Successful create/mkdir no longer force-loads the parent inode into the client cache after Lua already validated it | Remove an extra parent stat/Redis `GET` on cold successful create paths |
| Perf tooling | Redis perf runner can preserve per-run FUSE op logs with `PERF_FUSE_OPS_LOG=1` | Make Task 7 operation-count traces reproducible and artifact-local |
| VFS unlink/setattr | Recently unlinked inode attrs are kept briefly so post-unlink timestamp-only `setattr` stays local | Avoid Redis `GET`/`SET` traffic generated by kernel timestamp updates after create/unlink-heavy dirperf loops |
| VFS flush/close | File handles now track whether they actually wrote data, and no-write `flush()`/`close()` skips timestamp metadata updates | Avoid Redis `SET` traffic from empty create/open/flush/release cycles while preserving timestamp updates for dirty handles and pending writeback |
| FUSE lock cleanup | `flush()`/`release()` now skips POSIX owner unlock metadata work unless this process actually observed a FUSE `setlk` for that inode/owner | Avoid redundant Redis Lua lock-cleanup scripts on ordinary close-heavy workloads while preserving cleanup for known lock owners, including owner `0` |
| FUSE dispatch | Default `fuse_workers` is now `1`, which keeps the low-overhead `asyncfuse` session dispatch path unless the operator explicitly requests a worker pool | Avoid worker-pool scheduling overhead on metadata-heavy workloads while preserving `--fuse-workers > 1` for high-concurrency override |
| Perf tooling | Redis perf runner can inject `BREWFS_FUSE_WORKERS` and `BREWFS_FUSE_MAX_BACKGROUND` into generated backend config | Make FUSE dispatch and background-queue experiments reproducible from artifact-local perf runs |
| Redis metadata | `stat_fs()` now batches node payload loads with one Redis `MGET` after the node-key scan | Remove per-inode Redis `GET` round trips from FUSE `statfs` without changing accounting semantics |
| Perf tooling | Redis perf runner can pass `BREWFS_S3_PART_SIZE` and `BREWFS_S3_MAX_CONCURRENCY` to the perf container | Make S3/RustFS small-write concurrency sweeps reproducible without changing the default benchmark config |
| VFS writeback | Best-effort SSD dirty-slice persistence now runs concurrently with the object upload for each upload batch | Remove local fsync/writeback-cache latency from the foreground flush critical path without weakening upload-before-commit visibility |
| VFS unlink/setattr | Recently-unlinked inode attr cleanup is throttled after the threshold and no-handle timestamp-only `setattr` removes the short-lived attr in one map operation | Avoid repeated full-map cleanup and one extra hot-path map operation in create/unlink-heavy `dirperf` loops |
| VFS local bookkeeping | Removed the unused `ModifiedTracker` and its create/unlink/rename/write hot-path updates | Drop a DashMap write path that had no production readers and only added local bookkeeping cost |

Environment note:

| Item | Evidence | Decision |
| --- | --- | --- |
| Stale local BrewFS perf mount was consuming memory before the latest focused reruns | Process `456320`, mount `/tmp/brewfs-perf-455770/mnt` | Cleaned with lazy unmount and process kill before accepting new perf numbers |

Rejected experiment:

| Area | Artifact | Result | Decision |
| --- | --- | --- | --- |
| FUSE unlink/rmdir and VFS rmdir precheck removal | `docker/compose-xfstests/artifacts/perf-run-1780157265-27175` | `dirperf` was 25s, worse than the prior focused 24s result | Reverted; do not reintroduce without new evidence |
| Redis `lookup_with_attr` helper for open/create follow-up attrs | `docker/compose-xfstests/artifacts/perf-run-1780158557-8433` | `dirperf` was 25s, worse than the prior focused result; `metaperf` improved only slightly to 210s | Reverted; avoid broad metadata API expansion until operation traces prove the exact call pattern |
| Redis `rmdir()` Rust-side prelookup removal | `docker/compose-xfstests/artifacts/perf-run-1780163094-25634` | `dirperf` stayed 21s and `metaperf` regressed to 236s | Reverted; the safe rmdir Lua operation remains, but the prelookup removal did not earn its keep |
| VFS read-only open attr cache | `docker/compose-xfstests/artifacts/perf-run-1780171930-10210` | `dirperf` stayed 20s and `open` regressed to 2077.3 ops/s from 2103.9 ops/s, although `metaperf` wall time was 212s | Reverted; do not cache around close-to-open freshness until focused traces prove it pays for a real workload |
| MetaClient complete-directory negative lookup fast path | `docker/compose-xfstests/artifacts/perf-run-1780177846-14181` | `dirperf` stayed 16s and `metaperf` regressed slightly to 206s, despite the commandstats test removing Redis `HGET` for complete-cache misses | Reverted; remaining gap is not explained by complete-directory negative lookup misses |
| VFS lazy write-handle `FileWriter` allocation | `docker/compose-xfstests/artifacts/perf-run-1780179246-24852` | `dirperf` stayed 16s, but `metaperf` regressed to 209s; `open` improved to 2922.3 ops/s while `create`, `readdir`, and `rename` regressed | Reverted; avoiding writer allocation on empty write handles did not improve the accepted wall-time baseline |
| FUSE unlink known-child helper | `docker/compose-xfstests/artifacts/perf-run-1780182725-32445` | `dirperf` stayed 15s and `metaperf` regressed to 204s; `rename` dropped to 922.0 ops/s from 949.6 ops/s | Reverted; removing local duplicate VFS lookup/stat in FUSE unlink did not improve the accepted baseline |
| Redis create/unlink Lua array reply parser | `docker/compose-xfstests/artifacts/perf-run-1780183967-4848` | `dirperf` stayed 15s and `metaperf` regressed to 202s; `rename` dropped to 904.5 ops/s from 949.6 ops/s | Reverted; avoiding Lua `cjson.encode` plus Rust JSON parse for tiny create/unlink responses did not improve the accepted baseline |
| FUSE `max_background=64` as default candidate | `docker/compose-xfstests/artifacts/perf-run-1780192554-24836` | `dirperf` stayed 14s and `metaperf` was 204s; fio read/write stayed healthy at 119.91/55.74 MiB/s but tail latency worsened versus default | Rejected as a default change; queue depth alone does not explain mixed-run variance |
| FUSE `workers=2` as default candidate | `docker/compose-xfstests/artifacts/perf-run-1780193173-15361` | Isolated `dirperf` matched 13s in `perf-run-1780193143-32485`, but mixed `dirperf` regressed to 15s despite `metaperf` improving to 198s | Rejected as a default change; keep worker pool as explicit override for metaperf-oriented experiments |
| S3 SDK single-`Bytes` `ByteStream` fast path | `docker/compose-xfstests/artifacts/perf-run-1780196012-19201` | Single-op create dropped to 198.7 ops/s from the prior 213.7 ops/s diagnostic | Reverted; the existing stream construction is not the create bottleneck |
| Immediate `CommitBeforeUpload` for explicit `commit_first` mode | `docker/compose-xfstests/artifacts/perf-run-1780197692-30157` | After wiring the config so the mode actually applied, single-op create reached only 197.5 ops/s | Reverted; early metadata visibility is unsafe and did not improve the S3/RustFS create gap |
| S3 `max_concurrency=32` as perf-runner default candidate | `docker/compose-xfstests/artifacts/perf-run-1780197799-26251` | Single-op create dropped to 178.3 ops/s even though the generated backend config showed `max_concurrency: 32` | Reverted; RustFS small PUTs regress under this concurrency in the current workload |
| Redis/VFS `unlink_with_attr` returning Lua attr | `docker/compose-xfstests/artifacts/perf-run-1780204449-28172` | Mixed `dirperf` stayed 14s and Redis output bytes rose to 2.44MB from the prior 0.73MB-class run because every unlink returned attr JSON | Reverted; it did not reduce command counts and increased Redis response traffic |

Focused comparison:

| Workload | BrewFS artifact | BrewFS result | JuiceFS artifact | JuiceFS result | Status |
| --- | --- | ---: | --- | ---: | --- |
| `fio-randrw` read BW | `perf-run-1780191648-27152` | 120.86 MiB/s | `juicefs-perf-run-1780153510-28102` | 66.00 MiB/s | BrewFS faster |
| `fio-randrw` write BW | `perf-run-1780191648-27152` | 56.01 MiB/s | `juicefs-perf-run-1780153510-28102` | 29.29 MiB/s | BrewFS faster |
| `dirperf` wall time | `perf-run-1780191973-11822` | 13s | `juicefs-perf-run-1780190527-11724` | 13s | Matches in isolated run |
| `metaperf` wall time | `perf-run-1780191648-27152` | 203s | `juicefs-perf-run-1780153510-28102` | 222s | Better on wall time |

Important caveat: isolated `dirperf` now matches JuiceFS at 13s after switching the default to low-overhead FUSE dispatch, but the mixed `fio-randrw dirperf metaperf` verification still reports `dirperf` at 14s and `metaperf` at 203s. Individual metadata ops still lag JuiceFS on create/readdir/rename. Redis diagnostics from `perf-run-1780191973-11822` show Redis Lua server CPU is about 1.38s of a 13s wall-clock run, and the PID-attached perf profile points at FUSE/kernel scheduling and wakeup cost, so further changes should target scheduling variability or operation shape before adding more Redis Lua micro-optimizations.

Latest focused metadata detail:

| Operation | BrewFS `perf-run-1780191648-27152` | JuiceFS `juicefs-perf-run-1780153510-28102` | Gap |
| --- | ---: | ---: | --- |
| create | 178.0 ops/s | 285.1 ops/s | BrewFS slower, still variable |
| open | 5794.6 ops/s | 6049.8 ops/s | Close to JuiceFS |
| stat | 1,100,979.1 ops/s | 1,101,680 ops/s | Similar |
| readdir | 31,160.5 ops/s | 37,075.7 ops/s | BrewFS slower |
| rename | 948.3 ops/s | 1438.3 ops/s | BrewFS slower, repeat-variable |

Latest focused `dirperf` shape:

```text
docker/compose-xfstests/artifacts/perf-run-1780191973-11822
100 1.148
200 1.171
300 1.229
400 1.158
500 1.151
600 1.160
700 1.153
800 1.213
900 1.263
1000 1.269
```

Latest pre-commit verification:

| Command | Result |
| --- | --- |
| `cargo test -p brewfs meta::stores::redis::tests::test_unlink_last_reference_updates_parent_and_deleted_child_atomically -- --ignored --nocapture` | Passed for lib and bin test targets |
| `cargo test -p brewfs meta::stores::redis::tests::test_unlink_directory_rejected_fallback -- --ignored --nocapture` | Passed for lib and bin test targets |
| `cargo test -p brewfs meta::stores::redis::tests::test_hardlink_state_machine_full_transition -- --ignored --nocapture` | Passed for lib and bin test targets |
| `cargo test -p brewfs meta::stores::redis::tests::test_fuse_flush_ -- --ignored --nocapture` | Passed: no-lock flush skips redundant unlock metadata and known POSIX lock owner `0` is released |
| `cargo test -p brewfs vfs::fs::tests::basic_tests -- --nocapture` | 9 passed for lib target and 9 passed for bin test target |
| `bash docker/compose-xfstests/run_redis_perf.sh --tools "dirperf metaperf"` | Passed: `perf-run-1780180872-21146`, `dirperf` 15s, `metaperf` 201s |
| `cargo test -p brewfs --lib --bins --tests -- --format terse` | Passed: lib target 290 passed/145 ignored, bin test target 282 passed/145 ignored, integration tests passed with expected ignored external-service tests |
| `git diff --check` | Passed |

Latest post-commit metadata continuation:

| Change | Evidence | Decision |
| --- | --- | --- |
| Redis rename Lua-side dentry lookup | New ignored commandstats test failed before the patch with 5 Redis `HGET` calls, then passed after the patch with only Lua-side dentry lookups plus the test verification lookup | Keep |
| Redis rename overwrite tombstone and same-inode hardlink no-op | `test_rename_lua_existing_file_target_is_replaced`, `test_rename_lua_overwrite_file`, and `test_rename_lua_hardlink_same_inode_target_is_noop` pass | Keep |
| Redis rmdir Lua-side dentry lookup | Commandstats test passed after patch, but focused Docker perf did not improve `dirperf` and worsened `metaperf` | Reverted |
| FUSE/VFS open duplicate stat removal | `test_open_fresh_by_ino_checks_current_attr_once` passes and focused Docker perf improved `metaperf` to 207s without changing `dirperf` | Keep |
| Redis create Lua-side parent lookup | New ignored commandstats test failed before the patch with 2 Redis `GET` calls, then passed after moving parent lookup/setgid inheritance into Lua | Keep |
| Meta client/VFS open local bookkeeping | New tests failed before the patch because fresh stat reallocated the inode cache entry and open eagerly created `FileReader`; focused Docker perf improved open throughput but not wall time | Keep cautiously; useful for open gap but insufficient |
| Meta client create/mkdir parent stat removal | New Redis commandstats tests failed before the patch with 2 Redis `GET` calls, then passed with at most 1 `GET` after skipping the client-side parent stat | Keep; `dirperf` matched the best 20s run |
| VFS no-write flush/close timestamp skip | New Redis commandstats tests failed before the patch with 1 Redis `SET` on no-write flush and close, then passed with zero Redis `SET` calls | Keep; `dirperf` improved to 16s and `metaperf` to 205s |
| FUSE POSIX no-lock cleanup skip | New FUSE flush tests cover both no-lock skip and known lock owner release, including owner `0`; Docker perf improved to `dirperf` 15s and `metaperf` 201s | Keep |
| Redis same-dir rename parent update trim | New ignored commandstats test failed before the patch with 3 Redis `GET` calls, then passed after skipping the redundant parent `GET`/`SET` when old and new parent are identical; `test_rename_lua` still passed | Keep; Docker perf repeated `dirperf` 14s and kept `metaperf` at 196-199s |
| FUSE low-overhead default dispatch | Config default test failed before the patch with host-derived worker count, then passed after setting default `fuse_workers=1`; no-env `dirperf` reproduced 13s in `perf-run-1780191973-11822` | Keep cautiously; it matches JuiceFS isolated `dirperf` and did not hurt fio, while mixed metadata results remain variable |

Focused continuation artifacts:

| Artifact | Code state | `dirperf` | `metaperf` | Metadata note |
| --- | --- | ---: | ---: | --- |
| `perf-run-1780162288-9988` | Redis rename optimization only | 21s | 213s | `rename` improved to 935.6 ops/s from 840.5 ops/s |
| `perf-run-1780163094-25634` | Redis rename plus rejected rmdir prelookup removal | 21s | 236s | Rejected because wall time regressed |
| `perf-run-1780164106-6479` | Redis rename plus FUSE/VFS open fresh-stat de-duplication | 21s | 207s | `open` improved to 1984.6 ops/s; current best metadata wall time, still far from JuiceFS `dirperf` |
| `perf-run-1780165688-18917` | Redis create parent prelookup removal before response-size cleanup | 20s | 209s | `create` improved to 185.6 ops/s; first `dirperf` result at 20s |
| `perf-run-1780166421-11764` | Redis create parent prelookup removal with compact Lua response | 20s | 216s | `dirperf` repeated at 20s; `metaperf` outlier was worse |
| `perf-run-1780166694-29412` | Same code, metaperf-only rerun | n/a | 210s | `create` improved to 188.3 ops/s; used as latest metadata operation detail |
| `perf-run-1780168064-1374` | MetaClient `stat_fresh()` in-place cache refresh only | 21s | 211s | `open` improved to 2062.6 ops/s, but `dirperf` regressed from best |
| `perf-run-1780168941-8386` | In-place fresh stat plus lazy `FileReader` allocation | 21s | 214s | `open` improved to 2083.6 ops/s, but wall time did not improve |
| `perf-run-1780169879-22264` | Add MetaClient create/mkdir parent-stat removal | 20s | 213s | `open` improved to 2103.9 ops/s and `dirperf` matched best; still far from JuiceFS |
| `perf-run-1780170763-16114` | Open-only diagnostic with Redis commandstats kept for inspection | n/a | n/a | Open-only `metaperf` reported 2132.4 ops/s; Redis counters included setup/background traffic, so use as diagnostic only |
| `perf-run-1780171930-10210` | Rejected VFS read-only open attr cache experiment | 20s | 212s | `open` regressed to 2077.3 ops/s and `dirperf` did not improve; reverted instead of keeping speculative local caching |
| `perf-run-1780173116-3563` | Small `dirperf` with `PERF_FUSE_OPS_LOG=1` after fixing trace artifact handling | 3s | n/a | FUSE requests showed 1201 create/unlink pairs and 1202 post-unlink timestamp `setattr` requests in the reduced loop |
| `perf-run-1780173959-16014` | Post-unlink timestamp-only `setattr` stays local | 18s | 208s | First Docker proof that the setattr short-circuit improves `dirperf` from 20s to 18s |
| `perf-run-1780174664-25456` | Same code after adding TTL cleanup to the recently-unlinked attr cache | 18s | 208s | Accepted result before no-write flush/close tracking; `dirperf` repeated at 18s |
| `perf-run-1780176122-15261` | No-write flush/close skips timestamp metadata `SET` | 16s | 205s | Previous accepted result; `dirperf` moved closer to JuiceFS 13s and `metaperf` improved from 208s |
| `perf-run-1780180872-21146` | FUSE no-lock POSIX owner cleanup suppression | 15s | 201s | Current accepted result; `open` rose to 5664.9 ops/s and wall time improved without hurting create/stat/readdir |
| `perf-run-1780190903-31660` | `BREWFS_FUSE_WORKERS=1 BREWFS_FUSE_MAX_BACKGROUND=64` dirperf-only experiment | 13s | n/a | First proof that low-overhead FUSE dispatch can match JuiceFS isolated `dirperf` |
| `perf-run-1780190943-25001` | Same FUSE override with `dirperf metaperf` | 14s | 198s | Metadata wall time stayed better than JuiceFS, but `dirperf` remained run-to-run variable |
| `perf-run-1780191179-17401` | Same FUSE override with `fio-randrw dirperf` | 14s | n/a | fio read/write stayed healthy at 119.47/55.41 MiB/s |
| `perf-run-1780191648-27152` | Code default `fuse_workers=1`, no env override, mixed `fio-randrw dirperf metaperf` | 14s | 203s | fio improved to 120.86/56.01 MiB/s; backend config contained no explicit `fuse:` section |
| `perf-run-1780191973-11822` | Code default `fuse_workers=1`, no env override, dirperf-only repeat | 13s | n/a | Isolated `dirperf` matches JuiceFS `juicefs-perf-run-1780190527-11724` |
| `perf-run-1780192554-24836` | Code default workers with `BREWFS_FUSE_MAX_BACKGROUND=64`, mixed `fio-randrw dirperf metaperf` | 14s | 204s | Negative result; smaller background queue did not improve mixed metadata and worsened fio tail latency |
| `perf-run-1780192933-16633` | Current-code reduced `dirperf` with `PERF_FUSE_OPS_LOG=1` on local-fs | 1s | n/a | FUSE trace showed create/unlink remain dominant but close to prior accepted latency: create avg 255.4 us, unlink avg 248.4 us |
| `perf-run-1780193143-32485` | `BREWFS_FUSE_WORKERS=2` dirperf-only experiment | 13s | n/a | Isolated `dirperf` still matches JuiceFS, so two workers are not immediately disqualified |
| `perf-run-1780193173-15361` | `BREWFS_FUSE_WORKERS=2`, mixed `fio-randrw dirperf metaperf` | 15s | 198s | Metaperf improves, but mixed dirperf regresses; reject as default |
| `perf-run-1780193771-12678` | S3/RustFS single-op `metaperf create` diagnostic | n/a | n/a | Create was 213.7 ops/s over 15s; Redis `evalsha` CPU was only 537ms, so create latency is not Redis-server CPU dominated |
| `juicefs-perf-run-1780193824-1655` | Matching JuiceFS single-op `metaperf create` diagnostic | n/a | n/a | JuiceFS create was 305.8 ops/s with the same 15s parameters |
| `perf-run-1780193978-14212` | BrewFS local-fs single-op `metaperf create` diagnostic | n/a | n/a | Local-fs create reached 381.6 ops/s, showing the S3/RustFS small-write path contributes heavily to the create gap |
| `perf-run-1780194080-13474` | BrewFS local-fs `fio-randrw dirperf` diagnostic | 14s | n/a | `dirperf` still reported 14s after fio, so mixed metadata variance is not only object-store specific |
| `perf-run-1780194725-4446` | Reduced local-fs `dirperf` with Redis `stat_fs()` MGET batching and `PERF_FUSE_OPS_LOG=1` | 1s | n/a | FUSE `statfs` fell from 129.8ms total in `perf-run-1780192933-16633` to 6.7ms total |
| `perf-run-1780194768-12125` | Redis `stat_fs()` MGET batching, mixed `fio-randrw dirperf metaperf` | 14s | 204s | fio stayed healthy at 117.37/54.70 MiB/s, create/open/rename improved slightly versus `perf-run-1780191648-27152`, but mixed `dirperf` did not reach 13s |
| `perf-run-1780195456-1924` | `BREWFS_COMPRESSION=off` single-op S3/RustFS `metaperf create` diagnostic | n/a | n/a | Create was 208.4 ops/s, worse than the prior 213.7 ops/s default diagnostic; compression is not the create gap |
| `perf-run-1780195524-27745` | Reduced S3/RustFS create with `PERF_FUSE_OPS_LOG=1` | n/a | n/a | FUSE `flush` dominated latency: avg 2944.9us, p95 4756.9us; `create` itself averaged 293.4us |
| `perf-run-1780195546-21381` | Matching local-fs create with `PERF_FUSE_OPS_LOG=1` | n/a | n/a | FUSE `flush` was much lower at avg 1630.2us while `create` stayed similar at 283.9us; object writeback is the S3-specific part of the gap |
| `perf-run-1780196408-25723` | `BREWFS_WRITEBACK_MODE=commit_first` before config plumbing | n/a | n/a | Create was 203.8 ops/s, but generated config did not contain writeback settings; this proved the env was not reaching `CacheConfig` |
| `perf-run-1780197692-30157` | True `cache.writeback_mode: commit_first` after temporary config plumbing and immediate pre-upload commit | n/a | n/a | Create was 197.5 ops/s; rejected and reverted because it was slower and weakened visibility semantics |
| `perf-run-1780197799-26251` | S3 `max_concurrency=32` single-op create diagnostic | n/a | n/a | Create was 178.3 ops/s; rejected as a perf-runner default |
| `perf-run-1780197857-13682` | S3 `max_concurrency=4` single-op create diagnostic | n/a | n/a | Create was 211.1 ops/s, close to but not better than the old 8-concurrency diagnostic |
| `perf-run-1780197948-14241` | S3 `max_concurrency=1` single-op create diagnostic | n/a | n/a | Create was 205.0 ops/s; lower concurrency does not close the JuiceFS gap |
| `perf-run-1780199010-26913` | Best-effort SSD persist overlapped with S3 upload, single-op S3/RustFS `metaperf create` | n/a | n/a | Create improved to 279.8 ops/s from the prior 213.7 ops/s S3 diagnostic, closing most of the JuiceFS 305.8 ops/s gap |
| `perf-run-1780199065-12909` | Same writeback overlap change, mixed `fio-randrw dirperf metaperf` | 15s | 199s | fio stayed healthy at 118.48/54.72 MiB/s and create improved to 231.0 ops/s, but mixed `dirperf` regressed to 15s |
| `perf-run-1780199386-12772` | Same writeback overlap change, dirperf-only repeat | 13s | n/a | Isolated `dirperf` still matches JuiceFS; the remaining 15s mixed result is still fio-aftereffect variance rather than this writeback change breaking standalone metadata |
| `perf-run-1780200122-4750` | Full `dirperf` with FUSE op trace enabled | 16s | n/a | Trace overhead inflated wall time, but showed create/unlink dominate service time: create 3.109s total, unlink 3.072s total |
| `perf-run-1780200151-7896` | `fio-randrw dirperf` with FUSE op trace enabled | 17s | n/a | Trace-only comparison showed post-fio create/unlink service time rises by about 0.65s and Redis CPU rises only about 0.2s, pointing back to VFS/FUSE hot-path overhead rather than Redis command count |
| `perf-run-1780201045-24597` | Recently-unlinked cleanup throttling, dirperf-only repeat | 14s | n/a | Isolated run was 14s, within the current 13-14s variance band and not accepted as proof of isolated improvement |
| `perf-run-1780201077-17050` | Recently-unlinked cleanup throttling, mixed `fio-randrw dirperf metaperf` before remove-first setattr cleanup | 14s | 201s | Mixed `dirperf` improved from the prior 15s to 14s; fio stayed healthy at 117.81/55.02 MiB/s and create improved to 271.5 ops/s |
| `perf-run-1780201496-2914` | Same cleanup throttling, `fio-randrw dirperf` repeat | 14s | n/a | Repeated 14s mixed `dirperf`, suggesting the improvement from 15s to 14s is stable |
| `perf-run-1780202056-13269` | Cleanup throttling plus no-handle timestamp-only setattr remove-first fast path, `fio-randrw dirperf` | 14s | n/a | Remove-first fast path did not move wall time beyond 14s but avoids an extra map operation in the hot post-unlink setattr path |
| `perf-run-1780202194-31491` | Cleanup throttling plus remove-first fast path, full mixed `fio-randrw dirperf metaperf` | 14s | 201s | Current-code mixed validation: fio 124.04/57.28 MiB/s, create 258.3 ops/s, open 5933.1 ops/s, rename 961.9 ops/s; Step 14 remains open because `dirperf` is still one second above JuiceFS |
| `perf-run-1780203555-860` | Removed unused `ModifiedTracker` hot-path writes, `fio-randrw dirperf` | 14s | n/a | Wall time did not move, but the change removes dead local DashMap writes from create/unlink/rename/write paths |
| `perf-run-1780204449-28172` | Temporary Redis/VFS `unlink_with_attr` attr-return experiment, `fio-randrw dirperf` | 14s | n/a | Rejected and reverted: wall time and Redis command counts stayed flat while Redis output bytes increased materially |
| `perf-run-1780205229-27708` | Final current code after reverting attr-return experiment, `fio-randrw dirperf` | 14s | n/a | Confirms retained code still has the 14s mixed result; Redis output bytes returned to the 0.73MB-class baseline |

Latest continuation verification:

| Command | Result |
| --- | --- |
| `cargo test -p brewfs meta::stores::redis::tests::test_rename_uses_lua_dentry_lookup_without_rust_prelookups -- --ignored --nocapture` | Passed for lib and bin test targets |
| `cargo test -p brewfs meta::stores::redis::tests::test_rename_lua -- --ignored --nocapture` | Passed: 13 rename Lua cases for lib and bin test targets |
| `cargo test -p brewfs meta::stores::redis::tests::test_rmdir_lua -- --ignored --nocapture` | Passed: 4 rmdir Lua cases for lib and bin test targets |
| `cargo test -p brewfs meta::client::tests::test_rename_operations -- --nocapture` | Passed for lib and bin test targets |
| `cargo test -p brewfs --test rename_integration_test -- --nocapture` | Passed: 6 integration tests |
| `cargo test -p brewfs vfs::fs::tests::basic_tests -- --nocapture` | Passed: 8 basic VFS tests for lib and bin test targets |
| `cargo test -p brewfs meta::stores::redis::tests::test_create_entry_uses_lua_parent_lookup_without_rust_prelookup -- --ignored --nocapture` | Passed for lib and bin test targets after failing before the patch with 2 Redis `GET` calls |
| `cargo test -p brewfs meta::stores::redis::tests::test_create_entry_updates_parent_node_cache -- --ignored --nocapture` | Passed for lib and bin test targets |
| `cargo test -p brewfs meta::stores::redis::tests::test_create_entry_lua -- --ignored --nocapture` | Passed: 4 create Lua cases for lib and bin test targets |
| `cargo test -p brewfs meta::client::tests::test_stat_fresh_refreshes_cached_file_entry_in_place -- --nocapture` | Red/green verified; passed for lib and bin test targets |
| `cargo test -p brewfs vfs::fs::tests::basic_tests::test_open_defers_reader_until_first_read -- --nocapture` | Red/green verified; passed for lib and bin test targets |
| `cargo test -p brewfs test_meta_client_ -- --ignored --nocapture` | Red/green verified: create_file and mkdir avoid the extra parent Redis `GET`; passed for lib and bin test targets |
| `cargo test -p brewfs meta::stores::redis::tests::test_meta_client_stat_fresh_uses_warm_store_node_cache -- --ignored --nocapture` | Added diagnostic coverage showing hot `stat_fresh()` reuses the Redis store node cache instead of issuing Redis `GET` calls |
| `cargo test -p brewfs meta::stores::redis::tests::test_vfs_deleted_inode_timestamp_setattr_stays_local -- --ignored --nocapture` | Red/green verified: failed before the VFS fast path with 1 Redis `GET`, then passed with zero Redis `GET`/`SET` calls |
| `cargo test -p brewfs meta::stores::redis::tests::test_vfs_close_without_write_skips_timestamp_metadata_update -- --ignored --nocapture` | Red/green verified: failed before the handle dirty tracking patch with 1 Redis `SET`, then passed with zero Redis `SET` calls |
| `cargo test -p brewfs meta::stores::redis::tests::test_vfs_flush_without_write_skips_timestamp_metadata_update -- --ignored --nocapture` | Red/green verified: failed before the handle dirty tracking patch with 1 Redis `SET`, then passed with zero Redis `SET` calls |
| `cargo test -p brewfs meta::stores::redis::tests::test_vfs_ -- --ignored --nocapture` | Passed: deleted-inode timestamp setattr plus no-write flush/close tests for lib and bin test targets |
| `cargo test -p brewfs meta::stores::redis::tests::test_fuse_flush_ -- --ignored --nocapture` | Passed: no-lock flush plus known owner cleanup tests for lib and bin test targets |
| `cargo test -p brewfs vfs::fs::tests::basic_tests -- --nocapture` | Passed: 9 basic VFS tests for lib and bin test targets |
| `bash docker/compose-xfstests/run_redis_perf.sh --tools "dirperf metaperf"` | Passed: `perf-run-1780176122-15261`, `dirperf` 16s, `metaperf` 205s |
| `bash docker/compose-xfstests/run_redis_perf.sh --tools "dirperf metaperf"` | Passed: `perf-run-1780180872-21146`, `dirperf` 15s, `metaperf` 201s |
| `cargo test -p brewfs --test gc_test -- --nocapture` | Passed after an earlier full-suite-only `test_gc_respects_min_age` flake |
| `cargo test -p brewfs test_stat_fs_batches_node_fetches_with_mget -- --ignored --nocapture` | Red/green verified: failed before the patch with 6 Redis `GET` calls, then passed for lib and bin test targets with 1 Redis `MGET` |
| `PERF_FUSE_OPS_LOG=1 PERF_DIRPERF_ARGS='-d /mnt/brewfs/.perf-dirperf -a 100 -f 100 -l 300 -c 16 -n 2 -s 1' ./docker/compose-xfstests/run_redis_perf.sh --local-fs --tools "dirperf"` | Passed: `perf-run-1780194725-4446`, reduced `dirperf` 1s, FUSE `statfs` total latency reduced to 6.7ms |
| `./docker/compose-xfstests/run_redis_perf.sh --tools "fio-randrw dirperf metaperf"` | Passed: `perf-run-1780194768-12125`, fio 117.37/54.70 MiB/s, `dirperf` 14s, `metaperf` 204s |
| `cargo test -p brewfs --lib --bins --tests -- --format terse` | Passed: lib target 291 passed/147 ignored, bin target 283 passed/147 ignored, integration tests passed with expected ignored external-service tests |
| `git diff --check` | Passed |
| `bash -n docker/compose-xfstests/run_perf_in_container.sh docker/compose-xfstests/run_redis_perf.sh` | Passed after adding perf-runner FUSE override plumbing |
| `cargo test -p brewfs config::tests::mount_config_defaults_use_low_overhead_fuse_dispatch -- --nocapture` | Red/green verified: failed before the default change with host-derived workers, then passed with `fuse_workers=1` |
| `BREWFS_FUSE_WORKERS=1 BREWFS_FUSE_MAX_BACKGROUND=64 ./docker/compose-xfstests/run_redis_perf.sh --tools "dirperf"` | Passed: `perf-run-1780190903-31660`, `dirperf` 13s |
| `./docker/compose-xfstests/run_redis_perf.sh --tools "fio-randrw dirperf metaperf"` | Passed with no FUSE env override: `perf-run-1780191648-27152`, fio 120.86/56.01 MiB/s, `dirperf` 14s, `metaperf` 203s |
| `./docker/compose-xfstests/run_redis_perf.sh --tools "dirperf"` | Passed with no FUSE env override: `perf-run-1780191973-11822`, `dirperf` 13s |
| `BREWFS_COMPRESSION=off PERF_METAPERF_ARGS='-d /mnt/brewfs/.perf-metaperf -t 15 -s 4096 -l 16 -L 16 -n 200 -N 2000 create' ./docker/compose-xfstests/run_redis_perf.sh --tools "metaperf"` | Passed: `perf-run-1780195456-1924`, create 208.4 ops/s; rejected as an optimization |
| `PERF_FUSE_OPS_LOG=1 PERF_METAPERF_ARGS='-d /mnt/brewfs/.perf-metaperf -t 5 -s 4096 -l 16 -L 16 -n 100 -N 500 create' ./docker/compose-xfstests/run_redis_perf.sh --tools "metaperf"` | Passed: `perf-run-1780195524-27745`; FUSE `flush` avg 2944.9us dominated S3 create |
| `PERF_FUSE_OPS_LOG=1 PERF_METAPERF_ARGS='-d /mnt/brewfs/.perf-metaperf -t 5 -s 4096 -l 16 -L 16 -n 100 -N 500 create' ./docker/compose-xfstests/run_redis_perf.sh --local-fs --tools "metaperf"` | Passed: `perf-run-1780195546-21381`; local-fs `flush` avg 1630.2us |
| `BREWFS_S3_MAX_CONCURRENCY=4 PERF_METAPERF_ARGS='-d /mnt/brewfs/.perf-metaperf -t 15 -s 4096 -l 16 -L 16 -n 200 -N 2000 create' ./docker/compose-xfstests/run_redis_perf.sh --tools "metaperf"` | Passed: `perf-run-1780197857-13682`, generated config showed `max_concurrency: 4`, create 211.1 ops/s |
| `BREWFS_S3_MAX_CONCURRENCY=1 PERF_METAPERF_ARGS='-d /mnt/brewfs/.perf-metaperf -t 15 -s 4096 -l 16 -L 16 -n 200 -N 2000 create' ./docker/compose-xfstests/run_redis_perf.sh --tools "metaperf"` | Passed: `perf-run-1780197948-14241`, generated config showed `max_concurrency: 1`, create 205.0 ops/s |
| `bash -n docker/compose-xfstests/run_redis_perf.sh` | Passed after adding S3 perf env pass-through |
| `git diff --check` | Passed after updating this plan and perf runner pass-through |
| `cargo test -p brewfs vfs::io::writer::tests::test_best_effort_persist_runs_concurrently_with_upload --lib -- --nocapture` | Red/green verified: failed before the helper existed, then passed after running best-effort persist concurrently with upload |
| `cargo test -p brewfs vfs::io::writer::tests::test_flush_blocks_write_until_upload_done --lib -- --nocapture` | Passed after the writeback overlap change |
| `cargo test -p brewfs vfs::io::writer::tests::test_flush_reports_upload_failure --lib -- --nocapture` | Passed after the writeback overlap change |
| `cargo test -p brewfs vfs::io::writer::tests::test_file_writer_flush_commits_and_reads --lib -- --nocapture` | Passed after the writeback overlap change |
| `cargo test -p brewfs vfs::io::writer::tests --lib -- --nocapture` | Passed: 17 writer tests |
| `PERF_METAPERF_ARGS='-d /mnt/brewfs/.perf-metaperf -t 15 -s 4096 -l 16 -L 16 -n 200 -N 2000 create' ./docker/compose-xfstests/run_redis_perf.sh --tools "metaperf"` | Passed: `perf-run-1780199010-26913`, default S3/RustFS create improved to 279.8 ops/s |
| `./docker/compose-xfstests/run_redis_perf.sh --tools "fio-randrw dirperf metaperf"` | Passed: `perf-run-1780199065-12909`, fio 118.48/54.72 MiB/s, `dirperf` 15s, `metaperf` 199s |
| `cargo test -p brewfs vfs::fs::tests::basic_tests -- --nocapture` | Passed after removing unused `ModifiedTracker`: 9 VFS basic tests for lib and bin targets |
| `cargo test -p brewfs meta::stores::redis::tests::test_meta_client_unlink_with_attr_avoids_prelookup_stat -- --ignored --nocapture` | Red/green diagnostic for the rejected attr-return experiment: failed before the API existed, then passed after implementation; experiment was reverted because Docker perf did not improve |
| `cargo test -p brewfs meta::stores::redis::tests::test_unlink_last_reference_updates_parent_and_deleted_child_atomically -- --ignored --nocapture` | Passed for lib and bin test targets during the rejected attr-return experiment validation |
| `cargo test -p brewfs meta::stores::redis::tests::test_unlink_directory_rejected_fallback -- --ignored --nocapture` | Passed for lib and bin test targets during the rejected attr-return experiment validation |
| `cargo test -p brewfs meta::stores::redis::tests::test_hardlink_state_machine_full_transition -- --ignored --nocapture` | Passed for lib and bin test targets during the rejected attr-return experiment validation |
| `./docker/compose-xfstests/run_redis_perf.sh --tools "fio-randrw dirperf"` | Passed: `perf-run-1780203555-860`, fio 116.17/53.43 MiB/s, `dirperf` 14s |
| `./docker/compose-xfstests/run_redis_perf.sh --tools "fio-randrw dirperf"` | Passed for rejected attr-return experiment: `perf-run-1780204449-28172`, fio 124.29/57.39 MiB/s, `dirperf` 14s |
| `./docker/compose-xfstests/run_redis_perf.sh --tools "fio-randrw dirperf"` | Passed after reverting attr-return experiment: `perf-run-1780205229-27708`, fio 116.34/53.85 MiB/s, `dirperf` 14s |
| `./docker/compose-xfstests/run_redis_perf.sh --tools "dirperf"` | Passed: `perf-run-1780199386-12772`, isolated `dirperf` 13s |
| `cargo test -p brewfs --lib --bins --tests -- --format terse` | Passed on final retained code: lib 292 passed/148 ignored, bin 284 passed/148 ignored, compaction/GC/rename/native tests passed with expected ignored external-service tests |
| `PERF_FUSE_OPS_LOG=1 ./docker/compose-xfstests/run_redis_perf.sh --tools "dirperf"` | Passed: `perf-run-1780200122-4750`; trace overhead made `dirperf` 16s but isolated FUSE service was dominated by create/unlink |
| `PERF_FUSE_OPS_LOG=1 ./docker/compose-xfstests/run_redis_perf.sh --tools "fio-randrw dirperf"` | Passed: `perf-run-1780200151-7896`; post-fio trace made `dirperf` 17s and showed create/unlink service time rises more than Redis command counts explain |
| `cargo test -p brewfs vfs::fs::tests::test_recently_unlinked_cleanup_is_not_run_on_every_threshold_insert --lib -- --nocapture` | Red/green verified: failed before cleanup throttling, then passed after throttling recently-unlinked attr cleanup |
| `cargo test -p brewfs vfs::fs::tests::basic_tests -- --nocapture` | Passed: 9 VFS basic tests for lib and bin targets after the cleanup throttling change |
| `cargo test -p brewfs meta::stores::redis::tests::test_vfs_deleted_inode_timestamp_setattr_stays_local -- --ignored --nocapture` | Passed for lib and bin targets after the no-handle remove-first timestamp setattr fast path |
| `./docker/compose-xfstests/run_redis_perf.sh --tools "dirperf"` | Passed: `perf-run-1780201045-24597`, isolated `dirperf` 14s |
| `./docker/compose-xfstests/run_redis_perf.sh --tools "fio-randrw dirperf metaperf"` | Passed: `perf-run-1780201077-17050`, fio 117.81/55.02 MiB/s, `dirperf` 14s, `metaperf` 201s |
| `./docker/compose-xfstests/run_redis_perf.sh --tools "fio-randrw dirperf"` | Passed: `perf-run-1780201496-2914`, repeated mixed `dirperf` 14s |
| `./docker/compose-xfstests/run_redis_perf.sh --tools "fio-randrw dirperf"` | Passed: `perf-run-1780202056-13269`, current remove-first fast path repeated mixed `dirperf` 14s |
| `./docker/compose-xfstests/run_redis_perf.sh --tools "fio-randrw dirperf metaperf"` | Passed: `perf-run-1780202194-31491`, fio 124.04/57.28 MiB/s, `dirperf` 14s, `metaperf` 201s |
| `cargo test -p brewfs --lib --bins --tests -- --format terse` | Passed fresh before commit: lib 293 passed / 0 failed, bin 285 passed / 0 failed, compaction/GC/rename/native tests passed |

## Files And Responsibilities

| File | Responsibility |
| --- | --- |
| `src/meta/stores/redis/mod.rs` | Redis compaction replacement conflict detection and delayed-slice metadata updates |
| `src/meta/stores/database/mod.rs` | SQLite/database rename overwrite semantics and delayed/uncommitted GC selection |
| `src/meta/stores/etcd/mod.rs` | etcd delayed-slice cutoff selection parity |
| `src/meta/client/mod.rs` | Meta-layer rename error normalization |
| `src/meta/layer.rs` | Trait default rename error normalization |
| `src/config.rs` | FUSE worker and max-background defaults |
| `src/vfs/io/reader.rs` | Transient read retry and stale slice-cache invalidation |
| `src/vfs/fs/mod.rs` | VFS rename error mapping and read stats reporting |
| `src/chunk/cache.rs` | Disk cache atomic temp-file publish and low-copy cache persistence |
| `src/chunk/store.rs` | Hot-cache write visibility and cache test compatibility |
| `tests/redis_compact_conflict_test.rs` | Redis versioned compaction conflict regression test |
| `tests/compaction_worker_test.rs` | Compaction lock release flake guard |
| `tests/rename_integration_test.rs` | Serial rename integration tests to avoid SQLite deadlock noise |
| `tools/perf/run_perf.sh` | libc symbol/debuginfo perf reporting |
| `docker/compose-xfstests/run_perf_in_container.sh` | BrewFS perf summary output |
| `docker/compose-xfstests/run_juicefs_perf_in_container.sh` | JuiceFS comparison perf runner parity |

## Task 1: Maintain The Verified Baseline

**Files:**
- Modify: `doc/superpowers/plans/2026-06-17-metadata-maintenance-performance-stability.md`
- Read: `git status --short`
- Read: `git show --stat --oneline HEAD`

- [x] **Step 1: Record the current commit**

Run:

```bash
git show --stat --oneline --summary HEAD
```

Expected: output starts with:

```text
137a96797 Improve metadata maintenance and cache perf
```

- [x] **Step 2: Record files that are intentionally outside this perf iteration**

Run:

```bash
git status --short
```

Current known unrelated local files. Do not stage these with the perf/cache metadata patch unless the user explicitly asks:

```text
 M benches/brewfs_bench.rs
?? .VSCodeCounter/
?? doc/report.md
?? doc/superpowers/plans/2026-05-25-brewfs-io-hotpath-performance-plan.md
?? doc/superpowers/plans/2026-05-26-read-pipeline-refactor-plan.md
?? examples/sdk_fio_bench.rs
```

This plan belongs to the working iteration and should be staged together with
the related perf/cache metadata patch if the iteration is committed.

## Task 2: Verify Metadata Compaction Conflict Safety

**Files:**
- Read: `src/meta/stores/redis/mod.rs`
- Read: `tests/redis_compact_conflict_test.rs`

- [x] **Step 1: Ensure Redis compaction checks the expected slice set**

Confirm `replace_slices_for_compact_with_version()` deserializes the current Redis slice list and compares it with `expected_slices` by count, `slice_id`, `offset`, and `length`.

Run:

```bash
rg -n "expected_slices|CompactConflict|current_slices" src/meta/stores/redis/mod.rs tests/redis_compact_conflict_test.rs
```

Expected: both implementation and regression test contain those terms.

- [ ] **Step 2: Run the ignored live Redis conflict test when Redis is available**

Run:

```bash
cargo test -p brewfs --test redis_compact_conflict_test -- --ignored --nocapture
```

Expected with Redis on `127.0.0.1:6379`: `redis_versioned_compaction_rejects_changed_slice_set ... ok`.

If Redis is not available, keep the test ignored and rely on the normal full suite plus Docker perf runs.

## Task 3: Guard Disk Cache Against Perf Regression

**Files:**
- Read: `src/chunk/cache.rs`
- Read: `src/chunk/store.rs`

- [x] **Step 1: Preserve atomic disk-cache publication**

Confirm `DiskStorage::store_with_permit()` writes to a private temp path and publishes with `std::fs::rename`.

Run:

```bash
rg -n "DISK_CACHE_TMP_COUNTER|spawn_blocking|std::fs::rename|store_with_permit" src/chunk/cache.rs
```

Expected: output includes the temp counter, blocking write, and atomic rename path.

- [x] **Step 2: Verify cache and block-store tests**

Run:

```bash
cargo test -p brewfs chunk::cache::tests
cargo test -p brewfs chunk::store::tests
```

Expected: both commands pass.

- [x] **Step 3: Re-run focused mixed workload after future cache changes**

Run:

```bash
bash docker/compose-xfstests/run_redis_perf.sh --tools "fio-randrw dirperf"
```

Expected minimum smoke bar:

```text
fio-randrw read BW >= 100 MiB/s
fio-randrw write BW >= 45 MiB/s
dirperf wall time <= 35s
```

If this fails, inspect `src/chunk/cache.rs` and `src/chunk/store.rs` before touching metadata code.

Verified evidence:

```text
docker/compose-xfstests/artifacts/perf-run-1780152993-885
fio-randrw read BW 115.41 MiB/s
fio-randrw write BW 53.15 MiB/s
dirperf 28s

docker/compose-xfstests/artifacts/perf-run-1780156496-30677
dirperf 24s
metaperf 212s

docker/compose-xfstests/artifacts/perf-run-1780160758-17955
dirperf 21s
metaperf 212s
```

## Task 4: Stabilize Rename And Metadata Tests

**Files:**
- Read: `tests/rename_integration_test.rs`
- Read: `src/meta/stores/database/mod.rs`
- Read: `src/vfs/fs/mod.rs`

- [x] **Step 1: Serialize rename integration tests**

Confirm every `#[tokio::test]` in `tests/rename_integration_test.rs` also has:

```rust
#[serial(rename_integration)]
```

Run:

```bash
rg -n "#\\[tokio::test\\]|#\\[serial\\(rename_integration\\)\\]" tests/rename_integration_test.rs
```

Expected: the counts match.

- [x] **Step 2: Re-run the rename integration file**

Run:

```bash
cargo test -p brewfs --test rename_integration_test -- --nocapture
```

Expected: `6 passed; 0 failed`.

## Task 5: Keep Perf Tooling Actionable

**Files:**
- Read: `tools/perf/run_perf.sh`
- Read: `docker/compose-xfstests/run_perf_in_container.sh`
- Read: `docker/compose-xfstests/run_juicefs_perf_in_container.sh`

- [x] **Step 1: Verify shell syntax**

Run:

```bash
bash -n tools/perf/run_perf.sh docker/compose-xfstests/run_perf_in_container.sh docker/compose-xfstests/run_juicefs_perf_in_container.sh
```

Expected: no output and exit code `0`.

- [ ] **Step 2: Install libc debuginfo before deep flamegraph analysis**

On Debian/Ubuntu perf hosts, run:

```bash
apt-get update
apt-get install -y libc6-dbg
```

Then run:

```bash
tools/perf/run_perf.sh
```

Expected: `tools/perf/results/<timestamp>/flame/libc-report.txt` contains resolved libc symbols instead of only `[libc.so.6]` or raw addresses.

## Task 6: Full Perf Baseline Gate Before The Next Merge

**Files:**
- Read: `docker/compose-xfstests/run_redis_perf.sh`
- Read: `docker/compose-xfstests/artifacts/*/report.md`

- [ ] **Step 1: Run the full Redis/RustFS perf suite**

Run:

```bash
bash docker/compose-xfstests/run_redis_perf.sh
```

Expected: a new artifact directory under:

```text
docker/compose-xfstests/artifacts/perf-run-*
```

- [ ] **Step 2: Compare against the last relevant artifacts**

Run:

```bash
for d in \
  docker/compose-xfstests/artifacts/perf-run-1780126255-7366 \
  docker/compose-xfstests/artifacts/perf-run-1780127871-11806 \
  docker/compose-xfstests/artifacts/perf-run-1780147102-22090 \
  docker/compose-xfstests/artifacts/perf-run-*; do
  test -f "$d/results/fio-randrw.json" || continue
  echo "== $d"
  jq -r '.jobs[0] |
    "randrw read=\(.read.bw/1024)MiB/s write=\(.write.bw/1024)MiB/s read_p99=\(.read.clat_ns.percentile."99.000000"/1000000)ms write_p99=\(.write.clat_ns.percentile."99.000000"/1000000)ms"' \
    "$d/results/fio-randrw.json"
  test -f "$d/perf-summary.tsv" && awk '$1=="dirperf"{print "dirperf seconds="$3}' "$d/perf-summary.tsv"
done
```

Expected: the new run should not repeat the low-run shape unless there is a clear environmental explanation:

```text
fio-randrw read around or above 100 MiB/s
fio-randrw write around or above 45 MiB/s
dirperf around or below 35s
```

## Task 7: Close The Remaining Metadata Hot-Path Gap

**Files:**
- Read: `src/fuse/mod.rs`
- Read: `src/vfs/fs/mod.rs`
- Read: `src/meta/stores/redis/mod.rs`
- Read: `docker/compose-xfstests/artifacts/perf-run-1780156496-30677/report.md`
- Read: `docker/compose-xfstests/artifacts/juicefs-perf-run-1780153510-28102/report.md`

- [x] **Step 1: Quantify operation counts before the next metadata patch**

Run a focused FUSE operation trace or perf profile around `dirperf` and `metaperf`.

Expected: identify whether create/open/rename are still paying extra lookup/stat/setattr Redis round trips compared with JuiceFS.

Latest diagnostic notes:

```text
PERF_FUSE_OPS_LOG=1 PERF_DIRPERF_ARGS='-d /mnt/brewfs/.perf-dirperf -a 100 -f 100 -l 300 -c 16 -n 2 -s 1' \
  bash docker/compose-xfstests/run_redis_perf.sh --tools dirperf
docker/compose-xfstests/artifacts/perf-run-1780173116-3563
FUSE request counts in reduced dirperf:
  create 1201
  unlink 1201
  setattr 1202
  getattr 3606
  lookup 1214 (1209 ENOENT)
The high-frequency extra metadata candidate was post-unlink timestamp-only setattr on nlink=0 inodes.

docker/compose-xfstests/artifacts/perf-run-1780170763-16114
open-only metaperf: 2132.4 ops/s
Redis commandstats were not clean enough to isolate the timed phase because setup/background work was included.

cargo test -p brewfs meta::stores::redis::tests::test_meta_client_stat_fresh_uses_warm_store_node_cache -- --ignored --nocapture
hot stat_fresh() diagnostic: Redis GET calls stay at 0 after cache warmup.

docker/compose-xfstests/artifacts/perf-run-1780171930-10210
VFS read-only open attr cache: dirperf 20s, metaperf 212s, open 2077.3 ops/s.
Rejected because the target open operation regressed and dirperf did not move closer to JuiceFS 13s.

docker/compose-xfstests/artifacts/perf-run-1780177060-9477
Reduced dirperf FUSE latency split after the no-write flush/close patch:
  unlink 1201 calls, avg 275.0 us, total 330.3 ms
  create 1201 calls, avg 246.7 us, total 296.3 ms
  release 1202 calls, avg 165.6 us, total 199.0 ms
  flush 1203 calls, avg 156.7 us, total 188.5 ms
  lookup 1214 calls, avg 109.8 us, total 133.2 ms
  getattr 3606 calls, avg 12.5 us, total 45.2 ms
This shows repeated external parent getattr is not the primary remaining wall-time sink.

docker/compose-xfstests/artifacts/perf-run-1780179246-24852
VFS lazy write-handle FileWriter allocation: dirperf 16s, metaperf 209s, open 2922.3 ops/s.
Rejected because wall time regressed from the accepted 205s metadata baseline and create/readdir/rename also moved backward.

docker/compose-xfstests/artifacts/perf-run-1780180872-21146
FUSE no-lock POSIX owner cleanup suppression: dirperf 15s, metaperf 201s.
Accepted because it improved the accepted metadata baseline from dirperf 16s/metaperf 205s, with open rising to 5664.9 ops/s and rename rising to 949.6 ops/s while create/stat/readdir stayed in the same range.

docker/compose-xfstests/artifacts/perf-run-1780181989-11184
Current-code reduced dirperf FUSE latency split after no-lock cleanup:
  unlink 1201 calls, avg 243.9 us, total 293.0 ms
  create 1201 calls, avg 238.9 us, total 286.9 ms
  lookup 1212 calls, avg 111.6 us, total 135.3 ms
  flush 1203 calls, avg 17.1 us, total 20.6 ms
  release 1159 calls, avg 16.1 us, total 18.7 ms
This leaves create/unlink/lookup as the only visible high-frequency dirperf costs.

docker/compose-xfstests/artifacts/perf-run-1780182725-32445
FUSE unlink known-child helper: dirperf 15s, metaperf 204s.
Rejected because it did not move dirperf closer to JuiceFS and regressed wall time from the accepted 201s metadata baseline.

docker/compose-xfstests/artifacts/perf-run-1780183967-4848
Redis create/unlink Lua array reply parser: dirperf 15s, metaperf 202s.
Rejected because it did not move dirperf closer to JuiceFS and regressed wall time from the accepted 201s metadata baseline; rename also dropped from 949.6 ops/s to 904.5 ops/s.

docker/compose-xfstests/artifacts/perf-run-1780185311-21708
Complete-empty newly-created directory cache, reduced dirperf FUSE latency split:
  lookup 1212 calls, avg 14.3 us, total 17.3 ms
  create 1201 calls, avg 257.8 us, total 309.6 ms
  unlink 1201 calls, avg 247.6 us, total 297.4 ms
Accepted as a targeted lookup-path improvement because the pre-change reduced trace had lookup at 111.6 us avg and 135.3 ms total.

docker/compose-xfstests/artifacts/perf-run-1780185361-22827
Complete-empty newly-created directory cache: dirperf 14s, metaperf 202s.

docker/compose-xfstests/artifacts/perf-run-1780185731-28879
Repeat full run for the complete-empty directory cache: dirperf 15s, metaperf 198s.
Kept because the reduced trace proves the intended ENOENT lookup cost was removed, one full run moved dirperf closer to JuiceFS, and the repeat improved metaperf wall time versus the accepted 201s baseline. Treat the full dirperf result as variable until a third independent run confirms 14s.

docker/compose-xfstests/artifacts/perf-run-1780187143-19761
DashMap ModifiedTracker plus batched VFS touch, reduced dirperf FUSE latency split:
  create 1201 calls, avg 247.4 us, total 297.1 ms
  unlink 1201 calls, avg 238.6 us, total 286.5 ms
  lookup 1212 calls, avg 14.0 us, total 17.0 ms
Accepted as a local create/unlink-path improvement because the prior reduced trace had create at 257.8 us and unlink at 247.6 us.

docker/compose-xfstests/artifacts/perf-run-1780187198-23645
DashMap ModifiedTracker plus batched VFS touch: dirperf 14s, metaperf 199s.
Metadata operations: create 205.9 ops/s, open 5743.6 ops/s, stat 1120831.2 ops/s, readdir 33374.8 ops/s, rename 927.6 ops/s.

docker/compose-xfstests/artifacts/perf-run-1780187438-20171
Repeat full run for DashMap ModifiedTracker plus batched VFS touch: dirperf 14s, metaperf 201s.
Metadata operations: create 185.6 ops/s, open 5524.5 ops/s, stat 1117480.9 ops/s, readdir 32657.3 ops/s, rename 919.2 ops/s.
Kept because two full runs repeated the 14s dirperf result, the reduced trace lowered create/unlink latency, and metaperf stayed within the previous accepted 198-202s range.

docker/compose-xfstests/artifacts/perf-run-1780189185-18181
Perf runner Redis diagnostics plus same-dir rename parent update trim: dirperf 14s, metaperf 196s.
Metadata operations: create 202.6 ops/s, open 5602.8 ops/s, stat 1114674.4 ops/s, readdir 32368.0 ops/s, rename 952.4 ops/s.
Redis dirperf diagnostics: evalsha 22,006 calls, 1.42s total server CPU, worst command latency spike 2ms.

docker/compose-xfstests/artifacts/perf-run-1780190018-24278
Repeat after final verification: dirperf 14s, metaperf 199s.
Metadata operations: create 197.7 ops/s, open 5670.8 ops/s, stat 1116279.7 ops/s, readdir 32110.8 ops/s, rename 926.8 ops/s.
Redis dirperf diagnostics: evalsha 22,006 calls, 1.41s total server CPU, worst Redis latency spike 1ms.
Kept because commandstats prove the same-dir rename micro-optimization removes redundant Redis parent GET/SET, the repeat preserves the 14s dirperf result, and the new diagnostics show the remaining dirperf gap is not dominated by Redis Lua CPU.
```

- [x] **Step 2: Collapse Redis unlink into one atomic operation**

Run:

```bash
cargo test -p brewfs meta::stores::redis::tests::test_unlink_last_reference_updates_parent_and_deleted_child_atomically -- --ignored --nocapture
cargo test -p brewfs meta::stores::redis::tests::test_unlink_directory_rejected_fallback -- --ignored --nocapture
cargo test -p brewfs meta::stores::redis::tests::test_hardlink_state_machine_full_transition -- --ignored --nocapture
bash docker/compose-xfstests/run_redis_perf.sh --tools "dirperf metaperf"
```

Verified evidence:

```text
docker/compose-xfstests/artifacts/perf-run-1780160758-17955
dirperf pass 21s
metaperf pass 212s
```

- [x] **Step 3: Collapse Redis rename source/destination dentry lookup into one Lua operation**

Run:

```bash
cargo test -p brewfs meta::stores::redis::tests::test_rename_uses_lua_dentry_lookup_without_rust_prelookups -- --ignored --nocapture
cargo test -p brewfs meta::stores::redis::tests::test_rename_lua -- --ignored --nocapture
cargo test -p brewfs meta::client::tests::test_rename_operations -- --nocapture
cargo test -p brewfs --test rename_integration_test -- --nocapture
bash docker/compose-xfstests/run_redis_perf.sh --tools "dirperf metaperf"
```

Verified evidence:

```text
docker/compose-xfstests/artifacts/perf-run-1780162288-9988
dirperf pass 21s
metaperf pass 213s
rename 935.6 ops/s
```

The optimization is kept because it reduced store-side Redis `HGET` calls and improved isolated rename throughput. It does not close the overall `dirperf` gap by itself.

- [x] **Step 4: Collapse Redis create parent lookup into the Lua operation**

`create_entry()` previously loaded the parent node in Rust to check directory type and setgid inheritance, then the Lua script loaded the same parent node again for the atomic create. The Lua script now owns that parent lookup and returns the new inode plus final gid, letting Rust update only its local node cache.

Run:

```bash
cargo test -p brewfs meta::stores::redis::tests::test_create_entry_uses_lua_parent_lookup_without_rust_prelookup -- --ignored --nocapture
cargo test -p brewfs meta::stores::redis::tests::test_create_entry_updates_parent_node_cache -- --ignored --nocapture
cargo test -p brewfs meta::stores::redis::tests::test_create_entry_lua -- --ignored --nocapture
bash docker/compose-xfstests/run_redis_perf.sh --tools "dirperf metaperf"
bash docker/compose-xfstests/run_redis_perf.sh --tools "metaperf"
```

Verified evidence:

```text
docker/compose-xfstests/artifacts/perf-run-1780166421-11764
dirperf pass 20s
metaperf pass 216s

docker/compose-xfstests/artifacts/perf-run-1780166694-29412
metaperf pass 210s
create 188.3 ops/s
```

The optimization is kept because it moves `dirperf` from 21s to 20s and improves the create operation on rerun. It does not close the `dirperf` gap to JuiceFS.

- [x] **Step 5: Remove duplicate FUSE open stat without relaxing close-to-open refresh**

FUSE `open()` previously did a cached `stat_ino()` and then called `Vfs::open()`, which performed `meta_stat_fresh()` again for close-to-open semantics. It now calls `open_fresh_ino()`, which performs the fresh stat once, rejects directories and missing inodes, and then opens with the already-fresh attr.

Run:

```bash
cargo test -p brewfs vfs::fs::tests::basic_tests::test_open_fresh_by_ino_checks_current_attr_once -- --nocapture
bash docker/compose-xfstests/run_redis_perf.sh --tools "dirperf metaperf"
```

Verified evidence:

```text
docker/compose-xfstests/artifacts/perf-run-1780164106-6479
dirperf pass 21s
metaperf pass 207s
open 1984.6 ops/s
```

The optimization is kept because it preserves the fresh stat and improves the focused metadata wall time. It does not close the `dirperf` gap to JuiceFS.

- [x] **Step 6: Trim local open bookkeeping and client-side create parent stats**

`open_fresh_ino()` still needs a fresh metadata check, but `MetaClient::stat_fresh()` no longer deletes and reallocates the whole inode cache entry when it can refresh the existing one in place. VFS file handles also defer `FileReader` allocation until the first committed read, so open-only workloads do not pay reader setup. On create/mkdir, `MetaClient` no longer force-loads the parent inode after Redis Lua has already validated it; cached parents still get incremental `add_child()` updates, while cold parents avoid the extra stat.

Run:

```bash
cargo test -p brewfs meta::client::tests::test_stat_fresh_refreshes_cached_file_entry_in_place -- --nocapture
cargo test -p brewfs vfs::fs::tests::basic_tests::test_open_defers_reader_until_first_read -- --nocapture
cargo test -p brewfs test_meta_client_ -- --ignored --nocapture
cargo test -p brewfs vfs::fs::tests::basic_tests -- --nocapture
bash docker/compose-xfstests/run_redis_perf.sh --tools "dirperf metaperf"
```

Verified evidence:

```text
docker/compose-xfstests/artifacts/perf-run-1780169879-22264
dirperf pass 20s
metaperf pass 213s
open 2103.9 ops/s
```

The optimization is kept because it improves the open operation and removes proven extra Redis `GET` calls on cold create/mkdir, while matching the best 20s `dirperf` run. It still does not close the `dirperf` gap to JuiceFS.

- [x] **Step 7: Keep post-unlink timestamp setattr local**

The FUSE trace showed `dirperf` issues a timestamp-only `setattr` immediately after unlinking created files. Redis keeps deleted nodes for GC visibility, so the old path still performed a metadata `GET` plus save for nlink=0 inodes that no path can observe. VFS now keeps a short-lived recently-unlinked attr entry, applies atime/mtime/ctime-only updates locally, updates any still-open handle attrs, and drops the entry on `forget` or after an opportunistic TTL cleanup.

Run:

```bash
cargo test -p brewfs meta::stores::redis::tests::test_vfs_deleted_inode_timestamp_setattr_stays_local -- --ignored --nocapture
cargo test -p brewfs vfs::fs::tests::basic_tests -- --nocapture
bash docker/compose-xfstests/run_redis_perf.sh --tools "dirperf metaperf"
```

Verified evidence:

```text
docker/compose-xfstests/artifacts/perf-run-1780173959-16014
dirperf pass 18s
metaperf pass 208s

docker/compose-xfstests/artifacts/perf-run-1780174664-25456
dirperf pass 18s
metaperf pass 208s
```

The optimization is kept because two Docker runs improved `dirperf` from the prior accepted 20s to 18s while keeping `metaperf` at 208s, better than the prior 213s. It still does not close the `dirperf` gap to JuiceFS 13s.

- [x] **Step 8: Skip no-write flush/close timestamp metadata updates**

`dirperf` still includes many empty create/open/flush/release cycles. VFS `flush()` and `close()` previously updated mtime/ctime for any write-opened handle, even when the handle never wrote data and the shared writer had nothing pending. File handles now keep a write-dirty bit, normal writes and FUSE writeback-cache writes mark the handle dirty, and flush/close only update timestamps when the handle wrote data or the shared writer actually flushed pending data.

Run:

```bash
cargo test -p brewfs meta::stores::redis::tests::test_vfs_close_without_write_skips_timestamp_metadata_update -- --ignored --nocapture
cargo test -p brewfs meta::stores::redis::tests::test_vfs_flush_without_write_skips_timestamp_metadata_update -- --ignored --nocapture
cargo test -p brewfs meta::stores::redis::tests::test_vfs_ -- --ignored --nocapture
cargo test -p brewfs vfs::fs::tests::basic_tests -- --nocapture
bash docker/compose-xfstests/run_redis_perf.sh --tools "dirperf metaperf"
```

Verified evidence:

```text
docker/compose-xfstests/artifacts/perf-run-1780176122-15261
dirperf pass 16s
metaperf pass 205s
create 197.1 ops/s
open 2814.3 ops/s
```

The optimization is kept because RED/GREEN Redis commandstats proved it removes one Redis `SET` from no-write `flush()` and no-write `close()`, and Docker perf improved both `dirperf` (18s to 16s) and `metaperf` (208s to 205s). It still does not fully close the `dirperf` gap to JuiceFS 13s.

- [ ] **Step 9: Consider remaining Redis single-EVAL equivalents**

Apply the `unlink()` lesson narrowly: move only duplicated store-side round trips into existing atomic Redis scripts where the script already has the data, then verify with ignored Redis tests and focused `dirperf metaperf`.

Rejected subtest: changing only `CREATE_ENTRY_LUA` and `UNLINK_LUA` to return Redis array replies instead of JSON strings passed targeted parser/Lua/VFS tests, but `perf-run-1780183967-4848` regressed `metaperf` and rename while leaving `dirperf` unchanged. Do not retry response-format-only rewrites without evidence that JSON parse/encode dominates CPU.

Expected: improve create/open/rename or directory cleanup paths without reintroducing the reverted FUSE/VFS precheck-removal regression.

- [x] **Step 10: Cache known-empty newly-created directories for negative lookup**

The reduced FUSE trace showed 1209/1212 lookups were ENOENT, mostly under newly-created empty directories. MetaClient now marks freshly-created directories as having a complete empty child map, and `cached_lookup()` can return a cached negative result from a complete map instead of falling through to Redis `HGET`.

Run:

```bash
cargo test -p brewfs meta::stores::redis::tests::test_meta_client_new_directory_negative_lookup_stays_local -- --ignored --nocapture
cargo test -p brewfs meta::stores::redis::tests::test_meta_client_mkdir_avoids_parent_stat_after_lua_create -- --ignored --nocapture
cargo test -p brewfs meta::stores::redis::tests::test_create_entry_updates_parent_node_cache -- --ignored --nocapture
cargo test -p brewfs vfs::fs::tests::basic_tests -- --nocapture
PERF_FUSE_OPS_LOG=1 PERF_DIRPERF_ARGS='-d /mnt/brewfs/.perf-dirperf -a 100 -f 100 -l 300 -c 16 -n 2 -s 1' bash docker/compose-xfstests/run_redis_perf.sh --tools dirperf
bash docker/compose-xfstests/run_redis_perf.sh --tools "dirperf metaperf"
```

Verified evidence:

```text
RED before implementation:
test_meta_client_new_directory_negative_lookup_stays_local observed 2 Redis HGET calls.

GREEN after implementation:
test_meta_client_new_directory_negative_lookup_stays_local passed with 0 Redis HGET calls.

docker/compose-xfstests/artifacts/perf-run-1780185311-21708
lookup avg 14.3 us, total 17.3 ms, down from 111.6 us / 135.3 ms in perf-run-1780181989-11184.

docker/compose-xfstests/artifacts/perf-run-1780185361-22827
dirperf pass 14s
metaperf pass 202s

docker/compose-xfstests/artifacts/perf-run-1780185731-28879
dirperf pass 15s
metaperf pass 198s
```

The optimization is kept because it removes a proven Redis `HGET` from hot negative lookups under newly-created empty directories, improves the reduced FUSE lookup latency sharply, and does not show a stable metaperf wall-time regression. It still does not consistently reach JuiceFS `dirperf` 13s, and rename ops/sec remains below the previous 949.6 ops/s high-water mark.

- [x] **Step 11: Shard local modification tracking and batch touch updates**

The reduced trace still left create/unlink around 250 us each while lookup was no longer the dominant high-frequency cost. VFS metadata mutations also updated `ModifiedTracker` through a single global `tokio::Mutex<HashMap>` and often performed two lock acquisitions per create/unlink/rename. `ModifiedTracker` now uses `DashMap`, and create/unlink/mkdir/rmdir/link/rename hot paths update related inodes with one `touch_many()` call.

Run:

```bash
cargo test -p brewfs vfs::fs::tests::test_modified_tracker_touch_many_marks_all_inodes -- --nocapture
cargo test -p brewfs vfs::fs::tests::basic_tests -- --nocapture
cargo test -p brewfs meta::client::tests::test_rename_operations -- --nocapture
cargo test -p brewfs --test rename_integration_test -- --nocapture
PERF_FUSE_OPS_LOG=1 PERF_DIRPERF_ARGS='-d /mnt/brewfs/.perf-dirperf -a 100 -f 100 -l 300 -c 16 -n 2 -s 1' bash docker/compose-xfstests/run_redis_perf.sh --tools dirperf
bash docker/compose-xfstests/run_redis_perf.sh --tools "dirperf metaperf"
```

Verified evidence:

```text
RED before implementation:
test_modified_tracker_touch_many_marks_all_inodes failed to compile because touch_many did not exist.

GREEN after implementation:
test_modified_tracker_touch_many_marks_all_inodes passed for lib and bin targets.
VFS basic tests, MetaClient rename tests, and rename integration tests passed.

docker/compose-xfstests/artifacts/perf-run-1780187143-19761
create avg 247.4 us, unlink avg 238.6 us, down from 257.8 us / 247.6 us in perf-run-1780185311-21708.

docker/compose-xfstests/artifacts/perf-run-1780187198-23645
dirperf pass 14s
metaperf pass 199s

docker/compose-xfstests/artifacts/perf-run-1780187438-20171
dirperf pass 14s
metaperf pass 201s
```

The optimization is kept because it removes local mutex serialization from a hot VFS bookkeeping path, repeats the 14s `dirperf` result twice, and does not show a stable `metaperf` regression. It still does not close the final 1s `dirperf` gap to JuiceFS.

- [x] **Step 12: Investigate remaining 1s dirperf gap and trim same-dir rename parent updates**

The latest FUSE trace shows create/unlink still dominate high-frequency operation latency even after local bookkeeping cleanup. Perf runner diagnostics were extended so redis-backed runs can capture per-tool Redis `INFO commandstats`, `SLOWLOG`, `LATENCY`, and working FUSE op traces under the current artifact directory.

Run:

```bash
bash -n docker/compose-xfstests/run_perf_in_container.sh
PERF_FUSE_OPS_LOG=1 PERF_DIRPERF_ARGS='-d /mnt/brewfs/.perf-dirperf -a 100 -f 100 -l 200 -c 16 -n 2 -s 1' ./docker/compose-xfstests/run_redis_perf.sh --local-fs --tools "dirperf"
./docker/compose-xfstests/run_redis_perf.sh --tools "dirperf metaperf"
cargo test -p brewfs test_rename_same_dir_skips_redundant_parent_get_set -- --ignored --nocapture
cargo test -p brewfs test_rename_lua -- --ignored --nocapture
./docker/compose-xfstests/run_redis_perf.sh --tools "dirperf metaperf"
git diff --check
cargo test -p brewfs
```

Verified evidence:

```text
docker/compose-xfstests/artifacts/perf-run-1780188294-9506
Small local-fs diagnostic run: Redis diagnostics generated and brewfs_fuse_ops.log contained 12,176 lines.

docker/compose-xfstests/artifacts/perf-run-1780188323-25552
Before same-dir rename parent update trim:
dirperf pass 14s
metaperf pass 231s
Redis dirperf evalsha: 22,006 calls, 1.36s total server CPU, worst Redis command latency spike 3ms.

RED before implementation:
test_rename_same_dir_skips_redundant_parent_get_set failed with 3 Redis GET calls.

GREEN after implementation:
test_rename_same_dir_skips_redundant_parent_get_set passed.
test_rename_lua passed 13 ignored Redis tests for both lib and bin test targets.

docker/compose-xfstests/artifacts/perf-run-1780189185-18181
dirperf pass 14s
metaperf pass 196s
Metadata operations: create 202.6 ops/s, open 5602.8 ops/s, stat 1114674.4 ops/s, readdir 32368.0 ops/s, rename 952.4 ops/s.
Redis dirperf evalsha: 22,006 calls, 1.42s total server CPU, worst Redis command latency spike 2ms.

docker/compose-xfstests/artifacts/perf-run-1780190018-24278
Final repeat:
dirperf pass 14s
metaperf pass 199s
Metadata operations: create 197.7 ops/s, open 5670.8 ops/s, stat 1116279.7 ops/s, readdir 32110.8 ops/s, rename 926.8 ops/s.
Redis dirperf evalsha: 22,006 calls, 1.41s total server CPU, worst Redis latency spike 1ms.

Final checks:
git diff --check passed.
cargo test -p brewfs passed: lib 291 passed/147 ignored, bin 283 passed/147 ignored, integration tests passed, doctests 9 ignored.
```

Decision: keep the perf-runner diagnostic support and the same-directory rename Lua trim. The test proves one redundant parent `GET`/`SET` is removed from same-dir rename, and Docker perf did not regress. However, `dirperf` stayed at 14s while Redis Lua CPU was only about 1.4s of the 14s wall time, so the remaining 1s gap is not primarily Lua execution cost.

- [x] **Step 13: Profile the remaining `dirperf` wall-clock outside Redis Lua CPU**

Expected: identify whether the last 14s vs 13s gap is caused by Redis command round trips/command count, FUSE scheduling, kernel request pattern, or userspace `dirperf` serialization. Prefer CPU/off-CPU perf, FUSE trace summaries, or comparable JuiceFS Redis/FUSE evidence before adding another metadata cache or Lua micro-optimization.

Run:

```bash
./docker/compose-xfstests/run_juicefs_perf.sh --tools "dirperf" --keep
perf record -F 99 -e cpu-clock --call-graph fp -a -o /tmp/brewfs-step13-perf/brewfs-dirperf.data -- ./docker/compose-xfstests/run_redis_perf.sh --tools "dirperf"
BREWFS_FUSE_WORKERS=1 BREWFS_FUSE_MAX_BACKGROUND=64 ./docker/compose-xfstests/run_redis_perf.sh --tools "dirperf"
BREWFS_FUSE_WORKERS=1 BREWFS_FUSE_MAX_BACKGROUND=64 ./docker/compose-xfstests/run_redis_perf.sh --tools "dirperf metaperf"
BREWFS_FUSE_WORKERS=1 BREWFS_FUSE_MAX_BACKGROUND=64 ./docker/compose-xfstests/run_redis_perf.sh --tools "fio-randrw dirperf"
cargo test -p brewfs config::tests::mount_config_defaults_use_low_overhead_fuse_dispatch -- --nocapture
./docker/compose-xfstests/run_redis_perf.sh --tools "fio-randrw dirperf metaperf"
./docker/compose-xfstests/run_redis_perf.sh --tools "dirperf"
```

Verified evidence:

```text
docker/compose-xfstests/artifacts/juicefs-perf-run-1780190527-11724
JuiceFS isolated dirperf: 13s.

docker/compose-xfstests/artifacts/perf-run-1780190602-1966
System-wide perf around BrewFS isolated dirperf: dirperf 14s.
The report was diluted by idle/system-wide samples, but confirmed the run was not Redis-only CPU bound.

docker/compose-xfstests/artifacts/perf-run-1780190745-31502
PID-attached perf around the BrewFS mount process: dirperf 15s.
The release binary was stripped, so Rust userspace frames were unresolved; visible samples were dominated by kernel/FUSE scheduling and wakeup paths such as `_raw_spin_unlock_irqrestore`, `finish_task_switch`, `eventfd_write`, and `fuse_request_end`.

docker/compose-xfstests/artifacts/perf-run-1780190903-31660
FUSE override experiment: `BREWFS_FUSE_WORKERS=1 BREWFS_FUSE_MAX_BACKGROUND=64`.
dirperf pass 13s; Redis command count remained 176,066, so the improvement was not from fewer Redis operations.

docker/compose-xfstests/artifacts/perf-run-1780190943-25001
Same FUSE override: dirperf pass 14s, metaperf pass 198s.

docker/compose-xfstests/artifacts/perf-run-1780191179-17401
Same FUSE override: fio-randrw pass 72s with read/write 119.47/55.41 MiB/s, dirperf pass 14s.

RED before implementation:
config::tests::mount_config_defaults_use_low_overhead_fuse_dispatch failed with `left: 8, right: 1`.

GREEN after implementation:
config::tests::mount_config_defaults_use_low_overhead_fuse_dispatch passed.

docker/compose-xfstests/artifacts/perf-run-1780191648-27152
No FUSE env override after the config default change.
fio-randrw pass 72s with read/write 120.86/56.01 MiB/s, dirperf pass 14s, metaperf pass 203s.
The generated backend config had no explicit `fuse:` section, proving the run used the code default.

docker/compose-xfstests/artifacts/perf-run-1780191973-11822
No FUSE env override, dirperf-only repeat after the config default change.
dirperf pass 13s.
Redis dirperf diagnostics: evalsha 22,006 calls, 1.38s total server CPU, total Redis commands 176,067.
```

Decision: keep the low-overhead default FUSE dispatch and the perf-runner override knobs. This matches JuiceFS on isolated `dirperf`, keeps fio read/write above the previous accepted BrewFS baseline, and preserves the operator escape hatch for worker-pool concurrency. The remaining issue is not proven to be Redis Lua CPU; it is mixed-run variability plus slower create/readdir/rename operation rates.

- [ ] **Step 14: Stabilize mixed metadata variability without sacrificing fio**

Expected: improve the mixed `fio-randrw dirperf metaperf` run so `dirperf` consistently lands at or below 13s and `metaperf` stays below the JuiceFS 222s comparison, while keeping `fio-randrw` read/write at or above 100/45 MiB/s. Start from FUSE/kernel scheduling evidence or request-shape evidence; do not add speculative Redis caching unless a trace shows the exact extra operation.

Current diagnostic evidence:

```text
docker/compose-xfstests/artifacts/perf-run-1780192554-24836
Experiment: BREWFS_FUSE_MAX_BACKGROUND=64 with code default workers.
fio-randrw read/write: 119.91/55.74 MiB/s.
dirperf: 14s.
metaperf: 204s.
Decision: reject as a default change; the smaller queue did not improve mixed metadata and worsened fio tail latency versus perf-run-1780191648-27152.

docker/compose-xfstests/artifacts/perf-run-1780192933-16633
Reduced local-fs dirperf FUSE trace after the default workers=1 change:
  create 1201 calls, avg 255.4 us, total 306.7 ms
  unlink 1201 calls, avg 248.4 us, total 298.3 ms
  lookup 1212 calls, avg 15.9 us, total 19.2 ms
  statfs 3 calls, avg 43.3 ms, total 129.8 ms
Compared with perf-run-1780187143-19761, create/unlink are only slightly slower and lookup remains fixed, so there is no clear new extra FUSE request to remove.

docker/compose-xfstests/artifacts/perf-run-1780193143-32485
Experiment: BREWFS_FUSE_WORKERS=2, dirperf-only.
dirperf: 13s.

docker/compose-xfstests/artifacts/perf-run-1780193173-15361
Experiment: BREWFS_FUSE_WORKERS=2, mixed fio-randrw dirperf metaperf.
fio-randrw read/write: 115.48/53.05 MiB/s.
dirperf: 15s.
metaperf: 198s.
Decision: reject as a default change. Two workers may be useful for metaperf-oriented diagnostics, but it directly violates the mixed dirperf target.

docker/compose-xfstests/artifacts/perf-run-1780193771-12678
Experiment: single-op S3/RustFS metaperf create, 15s.
create: 213.7 ops/s.
Redis commandstats: evalsha 8606 calls, 537ms total server CPU.

docker/compose-xfstests/artifacts/juicefs-perf-run-1780193824-1655
Matching JuiceFS single-op metaperf create, 15s.
create: 305.8 ops/s.

docker/compose-xfstests/artifacts/perf-run-1780193978-14212
Matching BrewFS local-fs single-op metaperf create, 15s.
create: 381.6 ops/s.
Decision: the S3/RustFS small-write path is a major part of the create gap, while Redis server CPU is not.

docker/compose-xfstests/artifacts/perf-run-1780194080-13474
Experiment: local-fs fio-randrw followed by dirperf.
fio-randrw read/write: 119.44/55.60 MiB/s.
dirperf: 14s.
Decision: mixed dirperf variance is not purely S3/RustFS-specific; fio/cache/system load also perturbs the following metadata phase.

docker/compose-xfstests/artifacts/perf-run-1780194725-4446
Reduced local-fs dirperf FUSE trace after Redis stat_fs MGET batching:
  statfs 3 calls, avg 2.2 ms, total 6.7 ms
  create 1201 calls, avg 245.0 us, total 294.3 ms
  unlink 1201 calls, avg 242.9 us, total 291.7 ms
Compared with perf-run-1780192933-16633, statfs dropped by about 123ms total without increasing create/unlink latency.

docker/compose-xfstests/artifacts/perf-run-1780194768-12125
Mixed verification after Redis stat_fs MGET batching:
fio-randrw read/write: 117.37/54.70 MiB/s.
dirperf: 14s.
metaperf: 204s.
Decision: keep the statfs batching as a narrow local hotspot fix, but Step 14 remains open because mixed dirperf is still above the 13s target.

docker/compose-xfstests/artifacts/perf-run-1780195456-1924
Experiment: BREWFS_COMPRESSION=off, single-op S3/RustFS metaperf create.
create: 208.4 ops/s.
Decision: reject compression as the primary create-gap explanation; disabling it was slightly worse than the prior 213.7 ops/s default diagnostic.

docker/compose-xfstests/artifacts/perf-run-1780195524-27745
Experiment: reduced S3/RustFS metaperf create with PERF_FUSE_OPS_LOG=1.
FUSE flush: 1803 calls, avg 2944.9 us, p95 4756.9 us, p99 7171.9 us.
FUSE create: 701 calls, avg 293.4 us.

docker/compose-xfstests/artifacts/perf-run-1780195546-21381
Experiment: matching reduced local-fs metaperf create with PERF_FUSE_OPS_LOG=1.
FUSE flush: 2603 calls, avg 1630.2 us, p95 2392.1 us, p99 4073.1 us.
FUSE create: 701 calls, avg 283.9 us.
Decision: the S3 create gap is mostly the flush/object-writeback phase, not the metadata create operation.

docker/compose-xfstests/artifacts/perf-run-1780196012-19201
Experiment: temporary S3 SDK single-Bytes ByteStream fast path.
create: 198.7 ops/s.
Decision: reverted; the SDK stream wrapper was not the small-write bottleneck.

docker/compose-xfstests/artifacts/perf-run-1780197692-30157
Experiment: temporary true cache.writeback_mode=commit_first with immediate metadata commit before upload.
create: 197.5 ops/s.
Decision: reverted; the unsafe early-visibility mode did not improve create and should not be kept as a performance fix.

docker/compose-xfstests/artifacts/perf-run-1780197799-26251
Experiment: temporary S3 max_concurrency=32 perf-runner default candidate.
create: 178.3 ops/s.

docker/compose-xfstests/artifacts/perf-run-1780197857-13682
Experiment: explicit BREWFS_S3_MAX_CONCURRENCY=4 diagnostic.
create: 211.1 ops/s.

docker/compose-xfstests/artifacts/perf-run-1780197948-14241
Experiment: explicit BREWFS_S3_MAX_CONCURRENCY=1 diagnostic.
create: 205.0 ops/s.
Decision: keep only the env pass-through for reproducibility; do not change the default 8-concurrency perf-runner config because none of 1/4/32 beats the prior 8-concurrency diagnostic.

docker/compose-xfstests/artifacts/perf-run-1780199010-26913
Experiment: run best-effort SSD dirty-slice persistence concurrently with the object upload, single-op S3/RustFS metaperf create.
create: 279.8 ops/s.
Decision: keep. This removes sequential local fsync/writeback-cache cost from the foreground upload path while still waiting for both best-effort persist and object upload before marking the upload batch complete. The result closes most of the gap to JuiceFS create at 305.8 ops/s.

docker/compose-xfstests/artifacts/perf-run-1780199065-12909
Mixed verification after the writeback overlap change:
fio-randrw read/write: 118.48/54.72 MiB/s.
dirperf: 15s.
metaperf: 199s.
metadata detail: create 231.0 ops/s, open 6012.9 ops/s, stat 1,104,350.4 ops/s, readdir 33,007.1 ops/s, rename 972.7 ops/s.
Decision: keep the writeback overlap because it improves create and metaperf without hurting fio, but Step 14 remains open because mixed dirperf is still above the 13s target.

docker/compose-xfstests/artifacts/perf-run-1780199386-12772
Dirperf-only repeat after the writeback overlap change:
dirperf: 13s.
Decision: isolated dirperf still matches JuiceFS; the remaining mixed 15s result is fio-aftereffect variance, not a standalone metadata regression from the writeback overlap change.

docker/compose-xfstests/artifacts/perf-run-1780200122-4750
Experiment: full isolated dirperf with PERF_FUSE_OPS_LOG=1.
dirperf: 16s with trace overhead.
FUSE service summary: create 11001 calls / 3.109s total / 282.6us avg; unlink 11001 calls / 3.072s total / 279.3us avg.
Decision: trace overhead invalidates the wall time as a benchmark, but create/unlink are still the hot service-time operations.

docker/compose-xfstests/artifacts/perf-run-1780200151-7896
Experiment: full fio-randrw plus dirperf with PERF_FUSE_OPS_LOG=1.
dirperf: 17s with trace overhead.
FUSE service summary during dirperf: create 11000 calls / 3.269s total / 297.2us avg; unlink 10883 completed calls before the timestamp cutoff / 3.426s total / 314.8us avg.
Decision: post-fio Redis command counts were unchanged and Redis CPU increase was too small to explain the wall-time gap; keep optimizing VFS/FUSE unlink/create local overhead.

docker/compose-xfstests/artifacts/perf-run-1780201077-17050
Experiment: throttle recently-unlinked attr cleanup after the threshold.
fio-randrw read/write: 117.81/55.02 MiB/s.
dirperf: 14s.
metaperf: 201s.
metadata detail: create 271.5 ops/s, open 5814.0 ops/s, stat 1,111,528.9 ops/s, readdir 32,632.6 ops/s, rename 964.6 ops/s.
Decision: keep. Mixed dirperf improves from 15s to 14s and create remains close to the S3 single-op diagnostic, though isolated dirperf did not prove a standalone win.

docker/compose-xfstests/artifacts/perf-run-1780202194-31491
Experiment: current code after adding the no-handle timestamp-only setattr remove-first fast path.
fio-randrw read/write: 124.04/57.28 MiB/s.
dirperf: 14s.
metaperf: 201s.
metadata detail: create 258.3 ops/s, open 5933.1 ops/s, stat 1,105,574.9 ops/s, readdir 32,333.6 ops/s, rename 961.9 ops/s.
Decision: keep. The remove-first fast path did not push wall time below 14s, but it reduces one map operation on the hot post-unlink timestamp setattr path and preserved the improved mixed result.

docker/compose-xfstests/artifacts/perf-run-1780203555-860
Experiment: remove unused `ModifiedTracker` hot-path writes.
fio-randrw read/write: 116.17/53.43 MiB/s.
dirperf: 14s.
Decision: keep cautiously. It did not move wall time, but the tracker had no production readers and wrote to a DashMap in create/unlink/rename/write paths.

docker/compose-xfstests/artifacts/perf-run-1780204449-28172
Experiment: temporary Redis/VFS `unlink_with_attr` path returning deleted attr from Lua.
fio-randrw read/write: 124.29/57.39 MiB/s.
dirperf: 14s.
Redis note: command counts stayed unchanged and `total_net_output_bytes` rose to 2.44MB.
Decision: reverted. Returning attr JSON from every unlink increased response traffic without improving mixed dirperf.

docker/compose-xfstests/artifacts/perf-run-1780205229-27708
Experiment: final current code after reverting the attr-return experiment.
fio-randrw read/write: 116.34/53.85 MiB/s.
dirperf: 14s.
Redis note: command counts match the pre-experiment shape and `total_net_output_bytes` is back to 731197 bytes.
Decision: current retained code is performance-neutral on wall time; Step 14 remains open for timing/perf-symbol investigation.

docker/compose-xfstests/artifacts/perf-run-1780206512-22044
Command: `BREWFS_VFS_TIMING=1 ./docker/compose-xfstests/run_redis_perf.sh --tools "dirperf"`.
Experiment: gated VFS timing counters plus automatic `.stats` artifact snapshots.
dirperf: 13s.
VFS timing:
  create total 11001 ops / 2.692628s total; create metadata 2.689575s.
  unlink total 11001 ops / 2.621313s total; lookup 8.187ms, stat 6.796ms, metadata unlink 2.577286s, recently-unlinked map update 5.800ms.
  deleted-inode timestamp setattr remove-first map path 11001 ops / 0.157ms total.
Decision: keep the gated timing/artifact path. The recently-unlinked map is no longer the bottleneck; create/unlink await time is overwhelmingly metadata-store/Lua-side, so further map cleanup is unlikely to recover the remaining second.

docker/compose-xfstests/artifacts/perf-run-1780206984-7508
Command: `./docker/compose-xfstests/run_redis_perf.sh --tools "fio-randrw dirperf"`.
Experiment: temporary FUSE `unlink` precheck removal, leaving VFS `unlink_at` as the single validation point.
fio-randrw read/write: 117.80/54.85 MiB/s.
dirperf: 14s.
Decision: reverted. The cleanup is plausible but did not move mixed dirperf below 14s, so it is not enough evidence for a performance patch.

docker/compose-xfstests/artifacts/perf-run-1780207569-21270
Command: `./docker/compose-xfstests/run_redis_perf.sh --tools "fio-randrw dirperf"`.
Experiment: final retained code after reverting the FUSE unlink precheck experiment, with gated VFS timing disabled by default and `.stats` snapshots copied into tool diagnostics.
fio-randrw read/write: 122.79/56.97 MiB/s.
dirperf: 14s.
Stats artifact note: `diagnostics/stats-dirperf-after.txt` is NUL-stripped and shows VFS timing counters at zero when `BREWFS_VFS_TIMING` is not enabled.
Decision: keep the diagnostic infrastructure, not as a direct performance win. The mixed dirperf target remains open.
```

Next hypothesis: mixed `dirperf` remains at a stable 14s, while JuiceFS/isolated target remains 13s. Low-overhead VFS timing ruled out recently-unlinked map work and showed create/unlink are dominated by metadata create/unlink await time. Next, either inspect Redis create/unlink Lua command shape for a real command-count/round-trip reduction or use an unstripped PID-attached perf profile to split the remaining cost between Redis client await/scheduler overhead and server-side Lua execution. Avoid further local map cleanup or FUSE precheck cleanup unless a profile shows it contributes measurable mixed wall time.

## Maintenance Rules

- Update this file whenever a metadata maintenance, compaction, GC, read-retry, or perf-runner change lands.
- Keep finished work marked with `[x]` and leave future verification with `[ ]`.
- Add artifact IDs and exact commands, not prose-only claims.
- Do not add `.VSCodeCounter/` or transient report files to commits unless the user explicitly asks.
