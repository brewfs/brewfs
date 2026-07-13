# BrewFS Filesystem Test Matrix

Last audited: 2026-07-12.

This matrix keeps the filesystem correctness suites in one place. The default
CI path should stay fast enough for pull requests, while manual and local runs
cover the heavier open-source filesystem suites.

## Required PR Coverage

| Suite | Scope | Runner | Notes |
| --- | --- | --- | --- |
| Rust tests | Unit, integration, feature checks | `cargo test --workspace --lib --bins -- --test-threads=1` | Runs in CI on every PR. |
| pjdfstest supported set | POSIX path, chmod/chown/link/open/rename/unlink/utimensat for supported inode kinds | `bash docker/compose-xfstests/run_redis_pjdfstest.sh` | Uses `pjdfstest_skip_tests.txt` plus BrewFS-specific regular-file tests. |
| stress-ng smoke | Short metadata and small-write stress | `bash docker/compose-xfstests/run_redis_stress_ng.sh --profile smoke` | Runs in CI on every PR. |

## Manual Extended Coverage

| Suite | Scope | Runner | When to run |
| --- | --- | --- | --- |
| xfstests smoke | A small FUSE sanity slice | `bash docker/compose-xfstests/run_redis_xfstests.sh --cases "generic/001 generic/002 generic/100"` | Manual workflow or before risky VFS/FUSE changes. |
| xfstests full Redis+RustFS | Main correctness gate for Redis metadata plus RustFS object storage | `bash docker/compose-xfstests/run_redis_xfstests.sh` | Before merge when touching writeback, mmap, FUSE, metadata, locking, or object-store commit paths. |
| xfstests targeted excluded case | Re-check one excluded case without the default exclude file | `bash docker/compose-xfstests/run_redis_xfstests.sh --check-args "-fuse generic/091"` | Use before removing a case from `xfstests_slayer.exclude`. |
| LTP fs | Linux Test Project filesystem scenario | `bash docker/compose-xfstests/run_redis_ltp.sh` | Manual workflow or local extended run after metadata/cache changes. `iogen01` remains a default skip. POSIX record-lock growfiles variants are enabled. |
| LTP iogen01 diagnostic | Lock-heavy buffered+sync doio verifier | `BREWFS_FUSE_OP_LOG=1 bash docker/compose-xfstests/run_redis_ltp.sh --no-default-skip --extra-args "-s iogen01"` | Buffered profile currently expected to fail; direct-I/O profile passes after the dirty-overlay fix. Use it to debug FUSE writeback page-cache coherency before removing the default skip. |
| xfstests smoke TiKV+RustFS | TiKV metadata plus RustFS object-store sanity slice | `bash docker/compose-xfstests/run_tikv_xfstests.sh --cases "generic/001 generic/002 generic/100"` | Manual workflow before TiKV metadata-store changes. Uses a per-run RustFS data directory so interrupted runs are easy to clean. |
| pjdfstest TiKV+RustFS | POSIX path behavior against TiKV metadata | `bash docker/compose-xfstests/run_tikv_pjdfstest.sh` | Manual workflow after MetaStore API or TiKV transaction changes; mirrors the Redis pjdfstest supported set. |
| LTP TiKV+RustFS | Linux Test Project filesystem scenario against TiKV metadata | `bash docker/compose-xfstests/run_tikv_ltp.sh` | Manual workflow for TiKV backend regression hunting. Uses the same BrewFS LTP skip file as Redis, including buffered `iogen01`. |
| stress-ng profiles | Longer metadata/link/symlink/small-write stress | `bash docker/compose-xfstests/run_redis_stress_ng.sh --profile metadata-heavy` | Local regression hunting and release validation. |
| fio/perf | Performance guard | `bash docker/compose-xfstests/run_redis_perf.sh --s3` | Required after changes in read/write hot paths. Keep regression below 5%. |

Latest Redis+RustFS validation:

| Suite | Artifact | Result |
| --- | --- | --- |
| xfstests full | `run-1783835489-21324` | Passed all 708 configured tests. |
| LTP default | `run-1783849729-17223` | Passed with `iogen01` skipped; `failures_count: 0`. |
| pjdfstest supported set | `pjdfstest-run-1783853192-21814` | PASS; 176 selected files, 0 failed, 70 skipped. |
| stress-ng smoke | `perf-run-1783535542-30210` | Passed. |
| fio-randrw guard | `perf-run-1783547278-18906` | Read `386.36 MiB/s`, write `178.79 MiB/s`; `-0.73%` read and `-0.68%` write versus previous focused baseline, no >5% regression. |

## Exclude Review Policy

Do not remove an exclude only because a single local run passes once. For each
candidate, run it with `--check-args`, inspect the artifact, then repeat or run
the full suite before moving it into the required set.

Recently restored:

| Case | Evidence |
| --- | --- |
| `generic/736` | Targeted passes in `run-1783532285-12981` and `run-1783532356-26779`; latest full Redis+RustFS xfstests pass with 708 tests in `run-1783545550-19958`. |

Keep these xfstests excluded unless the underlying feature is intentionally
implemented:

| Cases | Reason |
| --- | --- |
| `generic/426`, `generic/467`, `generic/477` | `open_by_handle_at` file handles are not implemented. |
| `generic/632`, `generic/633` | Shared mount/device-file behavior is outside the current object-store filesystem model. |
| `generic/735` | `FALLOC_FL_INSERT_RANGE` is not supported. |
| `generic/095`, `generic/066` | FUSE subtype/remount infrastructure mismatch. |
| `generic/504`, `generic/478` | Lock visibility/refcount semantics need deeper async-fuse support. |
| `generic/647`, `generic/729` | O_DIRECT sparse-hole behavior through FUSE. |

Prioritize these for periodic targeted re-checks:

| Cases | Why |
| --- | --- |
| `generic/075`, `generic/091`, `generic/112`, `generic/127`, `generic/263`, `generic/438` | mmap/O_DIRECT/page-cache coherency may improve as writeback invalidation changes. `generic/075` diagnostics are summarized in the xfstests fix plan. |
| `generic/074` | Tiny-overwrite soak is expensive, but useful as a release soak when disk budget permits. |
| `generic/476`, `generic/521`, `generic/522`, `generic/650` | Long fsstress soaks should stay manual, but can expose compaction and dirty-cache regressions. |
| LTP `inode02`, `writetest`, `fs_di`, `rwtest03`, `rwtest04`, `rwtest05` | These are closer to BrewFS behavior than pure container/kernel noise. Re-test one at a time. |
| pjdfstest mixed special-node files | Revisit only after BrewFS persists FIFO/socket/device inode kinds or supports sticky/setuid/setgid semantics. |

Latest resolved regression decision:

| Case | Evidence and required validation |
| --- | --- |
| `generic/075` | Excluded after repeated buffered stale-data failures. Direct I/O cannot mmap, writeback-cache still fails, and post-reply invalidation deadlocks in `request_wait_answer`; see `run-1783853390-17236` through `run-1783857084-26829`. |

## Artifact Conventions

All compose suites write under `docker/compose-xfstests/artifacts/` except the
legacy `docker/compose-pjdfstest` runner. Prefer the `compose-xfstests` runners
for new work because they provide skip files, reports, and consistent RustFS
object-store setup.

## TiKV + RustFS Coverage

TiKV suites are manual `workflow_dispatch` jobs because they start PD, TiKV,
RustFS, and a privileged FUSE runner. Use them when touching
`src/meta/stores/tikv`, store-level transactions, FUSE writeback behavior, or
shared object-store commit paths.

| Suite | Runner |
| --- | --- |
| xfstests smoke | `bash docker/compose-xfstests/run_tikv_xfstests.sh --cases "generic/001 generic/002 generic/100"` |
| pjdfstest supported set | `bash docker/compose-xfstests/run_tikv_pjdfstest.sh` |
| LTP fs | `bash docker/compose-xfstests/run_tikv_ltp.sh` |
| perf | `bash docker/compose-xfstests/run_tikv_perf.sh --s3 --tools "fio-randrw"` |

Latest TiKV+RustFS evidence:

| Suite | Artifact | Result |
| --- | --- | --- |
| LTP fs | `run-1783852001-9407` | PASS; `failures_count: 0`. |
| xfstests full | `run-1783857700-31592`, `run-1783864499-12546` | The run enumerated all 707 configured cases and reported no failure except a missing xfstests `/tmp/28.out` for `generic/002`; an isolated rerun passed in 0s. This is a harness temporary-file race, not a hard-link failure. |
| pjdfstest supported set | `pjdfstest-run-1783853289-6218` | PASS; 176 selected files, 0 failed, 70 skipped. |
| LTP POSIX record-lock growfiles | `run-1783582046-16962`, `run-1783582552-4223`, `run-1783582599-29527`, `run-1783582749-12088`, `run-1783583452-6929` | PASS; `gf01`, `gf14`, `gf16`, `gf17`, and `gf18` each have `failures_count: 0`. |

TiKV now implements POSIX byte-range locks (`fcntl` / `F_SETLK` /
`F_SETLKW` / `F_GETLK`) through the BrewFS `plock` MetaStore API. BSD
`flock(2)` is separate: if future xfstests require `/proc/locks` visibility
for BSD locks, revisit asyncfuse `FUSE_FLOCK_LOCKS` negotiation and expose
`FUSE_LK_FLOCK` to BrewFS.

Latest cross-backend Compose evidence:

| Backend | xfstests | LTP |
| --- | --- | --- |
| SQLite + RustFS | `run-1783831206-2178`: all 708 configured tests passed | `run-1783848194-27300`: `failures_count: 0` |
| Redis + RustFS | `run-1783835489-21324`: all 708 configured tests passed | `run-1783849729-17223`: `failures_count: 0` |
| etcd + RustFS | `run-1783837045-32187`: all 708 configured tests passed | `run-1783850809-11579`: `failures_count: 0` |
| TiKV + RustFS | `run-1783857700-31592`: only transient `generic/002`; isolated pass in `run-1783864499-12546` | `run-1783852001-9407`: `failures_count: 0` |

## LTP Cache Profiles

The full LTP runner uses BrewFS' normal kernel page-cache behavior and skips
`iogen01` by default. Keep it skipped until the buffered writeback-cache
failure is fixed:

- Direct-I/O tiny-overlap shape is fixed in
  `docker/compose-xfstests/artifacts/run-1783536533-27423`.
- Buffered split-write/page-cache race remains in
  `docker/compose-xfstests/artifacts/run-1783543020-21424`. One application
  write at `314609+65912` was split into FUSE writes at `314609+783` and
  `315392+65129`; a cross-lock read between those writes can cache stale bytes
  for the writer's range.

Read-direct, write-direct, and full direct-I/O profiles are diagnostic only
right now. Full direct I/O makes mmap-based cases such as `rwtest01` and
`rwtest02` return `ENODEV`, while write-only direct I/O stalled `iogen01` in
`docker/compose-xfstests/artifacts/run-1783543528-5145`.
