# BrewFS Filesystem Test Matrix

Last audited: 2026-07-09.

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
| LTP fs | Linux Test Project filesystem scenario | `bash docker/compose-xfstests/run_redis_ltp.sh` | Manual workflow or local extended run after metadata/cache changes. `iogen01` remains a known failing diagnostic and is skipped by default. |
| LTP iogen01 diagnostic | Lock-heavy buffered+sync doio verifier | `BREWFS_FUSE_OP_LOG=1 bash docker/compose-xfstests/run_redis_ltp.sh --no-default-skip --extra-args "-s iogen01"` | Buffered profile currently expected to fail; direct-I/O profile passes after the dirty-overlay fix. Use it to debug FUSE writeback page-cache coherency before removing the default skip. |
| stress-ng profiles | Longer metadata/link/symlink/small-write stress | `bash docker/compose-xfstests/run_redis_stress_ng.sh --profile metadata-heavy` | Local regression hunting and release validation. |
| fio/perf | Performance guard | `bash docker/compose-xfstests/run_redis_perf.sh --s3` | Required after changes in read/write hot paths. Keep regression below 5%. |

Latest Redis+RustFS validation:

| Suite | Artifact | Result |
| --- | --- | --- |
| xfstests full | `run-1783545550-19958` | Passed all 708 configured tests. |
| LTP default | `run-1783543877-9990` | Passed with `iogen01` skipped; `failures_count: 0`. |
| pjdfstest supported set | `pjdfstest-run-1783535421-5656` | Passed 176 files / 1389 tests. |
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
| `generic/091`, `generic/112`, `generic/127`, `generic/263`, `generic/438` | mmap/O_DIRECT/page-cache coherency may improve as writeback invalidation changes. |
| `generic/074` | Tiny-overwrite soak is expensive, but useful as a release soak when disk budget permits. |
| `generic/476`, `generic/521`, `generic/522`, `generic/650` | Long fsstress soaks should stay manual, but can expose compaction and dirty-cache regressions. |
| LTP `inode02`, `writetest`, `fs_di`, `rwtest03`, `rwtest04`, `rwtest05` | These are closer to BrewFS behavior than pure container/kernel noise. Re-test one at a time. |
| pjdfstest mixed special-node files | Revisit only after BrewFS persists FIFO/socket/device inode kinds or supports sticky/setuid/setgid semantics. |

## Artifact Conventions

All compose suites write under `docker/compose-xfstests/artifacts/` except the
legacy `docker/compose-pjdfstest` runner. Prefer the `compose-xfstests` runners
for new work because they provide skip files, reports, and consistent RustFS
object-store setup.

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
