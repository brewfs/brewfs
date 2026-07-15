# BrewFS Agent Guide

This repository is a Rust/FUSE distributed filesystem. Treat correctness,
POSIX behavior, and repeatable performance evidence as first-class
requirements. Do not accept a performance change because a single focused
benchmark improves.

## Repository Map

- `src/vfs/`: FUSE-facing VFS, read/write path, writeback cache, stats, and
  memory budgeting.
- `src/chunk/`: chunk layout, object reads/writes, cache, compression, and
  compaction helpers.
- `src/meta/`: metadata client and Redis/SQLx/etcd/TiKV stores.
- `src/cadapter/`: object backend adapters, including S3-compatible storage.
- `docker/compose-xfstests/`: BrewFS and JuiceFS perf runners.
- `docker/compose-pjdfstest/`: POSIX smoke tests.
- `tools/perf/`: profiling/flamegraph helpers. Use these for diagnosis, not as
  the sole acceptance baseline.
- `doc/performance/`: current performance analysis, JuiceFS comparison, and
  accepted/rejected tuning notes.
- `doc/performance/performance-agent-guide.md`: extended guidance for
  performance-focused agent work and current optimization gaps.
- `doc/superpowers/plans/`: detailed implementation and experiment logs.

## Development Discipline

- Read the relevant module and nearby tests before editing.
- Keep changes small and tied to one hypothesis.
- Use `rg`/`rg --files` for search.
- Use `apply_patch` for manual edits.
- Do not revert user changes or unrelated worktree changes.
- Do not leave failed performance code in the tree. Revert rejected candidates
  with a targeted patch, then record the artifact and reason.
- For Rust behavior changes, prefer TDD: write or identify a failing test,
  prove it fails, then make the smallest implementation change.

## Local CI Gate For Accepted Code

Every accepted code change, including performance work, must pass the relevant
local CI gate before commit. At minimum, run the Rust job commands from
`.github/workflows/ci.yml`:

The active performance goal treats the workflow's `Test workspace` step as a
hard validity gate for every accepted optimization attempt. Run
`CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo test --workspace --lib --bins`
locally in the same iteration before accepting perf numbers, updating the
README comparison table, or committing code. Focused unit tests are useful
during development, but they do not replace this CI test gate.

```bash
cargo fmt --all --check

bash -n docker/compose-xfstests/run_perf_in_container.sh
bash -n docker/compose-xfstests/run_redis_perf.sh
bash -n docker/compose-xfstests/run_juicefs_perf_in_container.sh
bash -n docker/compose-xfstests/run_juicefs_perf.sh
bash docker/compose-xfstests/test_perf_report_delta.sh
bash docker/compose-xfstests/test_juicefs_direct_matrix.sh
bash docker/compose-xfstests/test_juicefs_perf_report.sh

CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo check --workspace
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo build --workspace

metadata="$(cargo metadata --no-deps --format-version 1)"
if python3 -c 'import json, sys; data = json.loads(sys.stdin.read()); members = set(data["workspace_members"]); sys.exit(not any(pkg["name"] == "rfuse3" and pkg["id"] in members for pkg in data["packages"]))' <<< "$metadata"; then
  cargo check -p rfuse3 --no-default-features --features tokio-runtime
  cargo check -p rfuse3 --no-default-features --features io-uring-runtime
  cargo check -p rfuse3 --no-default-features --features async-io-runtime
fi

CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo check -p brewfs --no-default-features --features fuse-tokio-runtime
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo check -p brewfs --no-default-features --features fuse-io-uring-runtime
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo test --workspace --lib --bins
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo clippy --workspace
git diff --check
```

If disk space is tight, remove rebuildable Rust target directories before perf
runs. Do not delete accepted perf artifacts unless explicitly asked.

## Performance Acceptance Rules

Use compose runners as the acceptance baseline. Use `tools/perf/` only after a
compose gap identifies where profiling is needed.

Focused writeback comparison command:

```bash
PERF_LOG_TO_CONSOLE=false PERF_FIO_SIZE=512m PERF_FIO_RUNTIME=20 \
  bash docker/compose-xfstests/run_redis_perf.sh --s3 \
  --writeback-throughput-profile \
  --tools "fio-seqwrite fio-randwrite fio-randrw"
```

JuiceFS focused comparison command:

```bash
PERF_LOG_TO_CONSOLE=false PERF_FIO_SIZE=512m PERF_FIO_RUNTIME=20 \
  bash docker/compose-xfstests/run_juicefs_perf.sh \
  --writeback-throughput-profile \
  --tools "fio-seqwrite fio-randwrite fio-randrw"
```

Direct-IO guard for writeback-sensitive changes:

```bash
PERF_LOG_TO_CONSOLE=false PERF_FIO_DIRECT=1 PERF_FIO_SIZE=512m PERF_FIO_RUNTIME=20 \
  bash docker/compose-xfstests/run_redis_perf.sh --s3 \
  --writeback-throughput-profile \
  --tools "fio-seqwrite fio-randwrite fio-randrw"
```

For a broad acceptance pass, include all core fio scenes and metadata stress:

```bash
PERF_LOG_TO_CONSOLE=false PERF_FIO_SIZE=512m PERF_FIO_RUNTIME=20 \
  bash docker/compose-xfstests/run_redis_perf.sh --s3 \
  --writeback-throughput-profile \
  --tools "fio-seqread fio-seqwrite fio-randread fio-randwrite fio-randrw fio-bigread fio-bigwrite metaperf dirstress dirperf"
```

Compare artifacts with:

```bash
python3 tools/perf/compare_artifacts.py <baseline-artifact> <candidate-artifact>
```

Use the generated `*_effective_wall_bw_mib_s` and
`*_effective_active_plus_drain_bw_mib_s` rows alongside fio active bandwidth;
several rejected writeback candidates improved runtime BW while moving cost
into close/flush/drain.

Acceptance requires:

- No correctness test regression.
- No material `fio-randrw` throughput or latency regression.
- No unexplained post-write drain timeout, dirty-byte tail, or FUSE teardown
  hang.
- No hidden win that only shifts cost from fio runtime into close/flush/drain.
- README performance tables updated only for accepted measurements.
- Rejected experiments recorded in the relevant plan or performance document.

## JuiceFS Comparison Notes

BrewFS is not a JuiceFS fork, but JuiceFS is the production reference for cache,
metadata, writeback, and object-store behavior. Before changing architecture,
check:

- `doc/juicefs/README.md`
- `doc/performance/brewfs-vs-juicefs-analysis.md`
- `doc/performance/review-writeback-writer.md`
- `doc/performance/review-read-cache.md`
- `doc/performance/review-metadata-cache.md`
- `doc/performance/review-perf-harness-config.md`

When comparing to JuiceFS, keep compression, cache budgets, fio direct mode,
runtime, working set, upload/download concurrency, and drain semantics explicit.
Do not compare a BrewFS strict-drain artifact against a JuiceFS artifact whose
writeback state was only loosely drained unless the report calls that out.

## Current Performance Guardrails

- Treat `fio-randrw` as a first-class gate. Multiple rejected candidates
  improved one write metric while severely regressing mixed read/write.
- Watch object amplification: PUT/GiB, average PUT object size, partial-tail
  ratios, upload batch shape, and slice count.
- Watch writeback debt: `brewfs_writeback_dirty_bytes`,
  `brewfs_writeback_live_dirty_bytes`,
  `brewfs_writeback_recent_pending_upload_bytes`, and
  `brewfs_writeback_remote_upload_inflight_bytes`.
- Watch metadata cache and Redis commandstats during metaperf. A faster data
  path that regresses open/stat/readdir/rename is not acceptable.
- Prefer observability additions when the bottleneck source is ambiguous.
  Behavior changes need a single clear hypothesis and a matched perf gate.

## Artifact Hygiene

- Preserve accepted baseline artifacts referenced from README or performance
  docs.
- After failed or long-running perf tests, stop compose services and remove
  volumes:

```bash
docker compose -f docker/compose-xfstests/docker-compose.redis-perf.yml down -v --remove-orphans
```

- Check disk before long perf or release builds:

```bash
df -h /mnt/slayerfs /mnt/slayerfs/brewfs
du -sh target docker/compose-xfstests/artifacts 2>/dev/null || true
docker system df
```

## Known POSIX And FUSE Limitations

- Keep `generic/075` excluded from default xfstests. Buffered FUSE mmap after
  truncate/extend can expose stale pre-truncate page-cache data. Direct I/O
  cannot run the mmap shape (`ENODEV`), writeback cache still reproduces it,
  and ordered post-reply `FUSE_NOTIFY_INVAL_INODE` experiments can leave fsx
  permanently blocked in `request_wait_answer`. Re-enable only after repeated
  `generic/075 generic/014` passes without a D-state task or >5% performance
  regression. See `doc/testing/xfstests-redis-rustfs-fix-plan.md`.
- Keep LTP `iogen01` in the default skip list. Its tiny-overlap direct-I/O
  diagnostic passes, but the normal buffered FUSE profile still has a
  split-write/page-cache coherency race. Full direct I/O is not a substitute:
  mmap workloads return `ENODEV`.
- FIFO, socket, character-device, and block-device inode kinds and `rdev` are
  persisted by every metadata backend. Keep the full pjdfstest corpus enabled;
  Redis and TiKV each pass all 246 files and 9,134 assertions in
  `pjdfstest-run-1783976619-22517` and `pjdfstest-run-1783976847-8934`.
- If xfstests `generic/002` reports a missing driver file such as
  `/tmp/<check-pid>.out` without an `.out.bad`, treat it as a harness race only
  when the missing-output cases exactly equal the reported failure set. The
  compose runner retries only that strict shape; all other failures stay fatal.
- A FUSE test process stuck in `request_wait_answer` cannot be killed or its
  container removed until the kernel request returns; a host reboot can be
  required. Preserve diagnostics first, and never add post-reply invalidation
  ordering without checking teardown and D-state tasks.
