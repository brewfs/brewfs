# BrewFS Agent Guide

This file is the working guide for agents tuning BrewFS. Keep changes small,
measured, reviewed, and easy to roll back.

## Mission

Bring BrewFS performance close to JuiceFS while preserving filesystem
correctness. Treat JuiceFS as the production reference for cache behavior,
writeback semantics, metadata hot paths, object-store IO shape, and benchmark
discipline. BrewFS is not a JuiceFS fork; copy ideas only after understanding
the local architecture.

Current primary gaps from the latest Redis plus S3/RustFS comparison:

- Cold `bigread` and random reads still trail JuiceFS significantly; sequential
  read is closer but not at parity.
- Sequential write and random write active bandwidth still trail JuiceFS,
  although random-write wall time can beat the noisy local JuiceFS run when
  JuiceFS is slowed by disk-cache timeout and direct-upload fallback.
- Mixed `randrw` active IO bandwidth can exceed the local JuiceFS run, but
  BrewFS still has a much longer wall-time close/flush tail.
- Metadata `stat` is at parity; `create`, `open`, `readdir`, and `rename` still
  need focused work, and total `metaperf` wall time remains high when fixed-time
  subtests create more small-file writeback cleanup.

## Reference Code

- BrewFS code lives in this repository.
- JuiceFS reference code should live outside the BrewFS worktree, for example
  `/mnt/slayerfs/juicefs`. If a user explicitly provides `brewfs/juicefs/`, it
  may be read as a reference, but do not commit a JuiceFS source checkout into
  this repository.
- When comparing behavior, read JuiceFS code first and map it to BrewFS
  boundaries instead of transplanting abstractions mechanically.

Useful JuiceFS areas:

- `pkg/vfs/`: high-level read/write/open/flush behavior.
- `pkg/chunk/`: chunk cache, upload/download scheduling, writeback, compaction.
- `pkg/object/`: object-store request shape and buffering.
- `pkg/meta/`: metadata transaction and hot-path operation shape.
- `pkg/cache/` and disk-cache related files: cache admission, eviction, and
  background IO policy.

Useful BrewFS areas:

- `src/vfs/`: POSIX-facing inode, file handle, read/write, flush, and setattr
  behavior.
- `src/chunk/`: chunk manager, block store, cache, writeback, compaction, and
  delayed deletion.
- `src/meta/`: metadata client and backend-specific optimizations.
- `src/cadapter/`: object-store backend, especially S3 request construction.
- `docker/compose-xfstests/`: correctness and perf harnesses.
- `tools/perf/`: profiler and flamegraph tooling.

## Optimization Loop

Every performance attempt must follow this loop:

1. State the hypothesis and expected metric movement.
2. Read the latest README rejected-tuning notes and performance docs, then
   explicitly rule out repeated attempts unless a new constraint or metric makes
   the retry meaningful.
3. Inspect the BrewFS hot path and the matching JuiceFS code path.
4. Add or adjust focused tests before risky behavior changes.
5. Make the smallest code change that can prove or disprove the hypothesis.
6. Run the local CI gate from `.github/workflows/ci.yml` before perf. At
   minimum this means the Rust job commands listed in the Required Local CI
   Gate section below; for FUSE, Docker, or POSIX behavior changes, also run
   the matching Docker smoke job locally.
7. Run BrewFS perf and JuiceFS perf with matching parameters.
8. Compare fio JSON, metadata logs, total script wall time, and internal BrewFS
   counters such as slice creation, partial-tail uploads, cache hit ratio, and
   metadata cache hits.
9. Update `README.md` with the new performance comparison table.
10. Review the diff for correctness, concurrency, cache consistency, and
    cleanup.
11. Commit only changes backed by CI, perf, or correctness evidence.
12. Revert experiments that do not improve the target metric or that regress
    important secondary scenarios.

Do not keep speculative changes because they look theoretically better. Perf
evidence decides.

## Required Local CI Gate

Before running expensive perf comparisons or committing code, reproduce the
Rust job from `.github/workflows/ci.yml` locally:

```bash
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 \
  cargo fmt --all --check
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 \
  cargo check --workspace
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 \
  cargo build --workspace
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 \
  cargo check -p brewfs --no-default-features --features fuse-tokio-runtime
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 \
  cargo check -p brewfs --no-default-features --features fuse-io-uring-runtime
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 \
  cargo test --workspace --lib --bins
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 \
  cargo clippy --workspace
```

If `rfuse3` is a workspace member in the checkout, also run the three rfuse3
runtime feature checks from the workflow. This repository layout currently
skips those checks when `rfuse3` is not a workspace member, matching CI.

For FUSE or POSIX-facing changes, run the Docker smoke suite before claiming the
change is merge-ready:

```bash
bash docker/compose-pjdfstest/run_redis_pjdfstest.sh
bash docker/compose-xfstests/run_redis_stress_ng.sh --profile smoke
```

## Required Perf Coverage

Use the default throughput profile for the final local comparison, because it
matches the README tables and the checked-in compose defaults:

```bash
PERF_LOG_TO_CONSOLE=false \
  bash docker/compose-xfstests/run_redis_perf.sh --s3 --writeback-throughput-profile

PERF_LOG_TO_CONSOLE=false \
  bash docker/compose-xfstests/run_juicefs_perf.sh --writeback-throughput-profile
```

For read-cache, writeback, or page-cache-sensitive changes, also run a
`PERF_FIO_DIRECT=1` guard on the touched fio workloads so Linux page cache and
FUSE writeback-cache effects do not hide regressions:

```bash
PERF_FIO_DIRECT=1 PERF_FIO_RUNTIME=30 PERF_LOG_TO_CONSOLE=false \
  bash docker/compose-xfstests/run_redis_perf.sh --s3 \
  --writeback-throughput-profile \
  --tools "fio-seqwrite fio-randwrite fio-randrw"
```

The default perf run must include these scenarios unless the user explicitly
narrows the scope:

- `fio-bigwrite`
- `fio-bigread`
- `fio-seqread`
- `fio-seqwrite`
- `fio-randread`
- `fio-randwrite`
- `fio-randrw`
- `dirstress`
- `dirperf`
- `metaperf`
- `looptest`

For fast diagnostics, a focused run is allowed, but it cannot replace the final
full comparison:

```bash
bash docker/compose-xfstests/run_redis_perf.sh --s3 --tools "fio-randrw dirperf metaperf"
```

Rejected ideas already recorded in `README.md` are part of the test history.
Before coding around writeback slice coalescing, auto-flush timing, staging IO,
upload concurrency, range prefetch, notify polling, or compression, check the
corresponding rejected section first. A repeated experiment needs a narrower
guard, a different workload target, or a clear reason the prior negative result
no longer applies.

## Performance Reporting

For fio scenarios, report both:

- fio runtime bandwidth from `results/*.json`
- effective throughput from total IO divided by `perf-summary.tsv` wall time

Writeback can make fio foreground latency look good while background drain still
dominates wall time. Prefer effective throughput when judging user-visible write
performance.

The README comparison table must include:

- command profile and date
- artifact paths for BrewFS and JuiceFS
- status for all default tools
- fio read/write bandwidth
- wall time, fio bandwidth ratio, and any large close/flush tail
- metadata operation throughput from `metaperf`
- notable warnings, such as slow object PUTs, disk-cache timeouts, or long drain
  phases

## Review Checklist

Before committing a performance change, review these points:

- Correctness: read-after-write, truncate, rename, unlink, hardlink, symlink,
  sparse IO, and fsync/flush semantics still hold.
- Cache coherence: metadata, memory cache, disk cache, and dirty overlays agree
  after write, flush, remount, and deletion.
- Backpressure: background upload/download/cache work cannot grow unbounded or
  starve foreground operations.
- Cancellation: aborted reads/writes do not leave permanent pending state.
- Object-store shape: avoid extra small PUTs, duplicate GETs, unnecessary
  copies, and foreground waits for best-effort cache writes.
- Metadata shape: avoid extra Redis round trips, large JSON replies in hot paths,
  and broad invalidations when a precise one is available.
- Regression risk: secondary workloads such as `randrw`, `dirperf`, and
  `metaperf` must not pay a large cost for a narrow improvement.
- Prior art: if the diff resembles a previously rejected README experiment,
  prove why it is not the same change before running the expensive perf loop.

## Flamegraph Guidance

Use flamegraphs when perf data points to CPU, scheduling, lock contention, or
unclear hot-path cost. Prefer focused workloads to reduce noise:

```bash
bash tools/perf/run_perf.sh
```

Capture the artifact path in the plan or README note. Summarize only actionable
findings: hot functions, lock contention, excessive copies, scheduler wakeups,
or object/meta calls that explain the measured gap.

## Storage Cleanup

Perf runs can consume object data, cache directories, Docker volumes, and mount
state. After each round:

- Unmount stale BrewFS/JuiceFS mount points.
- Stop compose services that are no longer needed.
- Remove transient cache and object-store data only when it is not part of the
  artifact being reported.
- Keep the latest accepted BrewFS and JuiceFS artifact directories.
- Check disk usage before long perf runs:

```bash
df -h /mnt/slayerfs .
du -sh docker/compose-xfstests/artifacts 2>/dev/null || true
```

## Commit Policy

Use small commits with messages that name the validated effect:

- `perf: reduce writeback foreground flush latency`
- `perf: avoid duplicate redis lookup on create`
- `docs: refresh brewfs juicefs perf comparison`

Do not commit:

- untracked JuiceFS source under `brewfs/juicefs/`
- transient Docker artifacts
- flamegraph outputs unless the user asks to preserve them
- failed experiments

When an experiment fails, revert it and record the negative result in the active
plan or performance notes so the same path is not retried blindly.
