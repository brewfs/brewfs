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
- JuiceFS reference code is expected under `brewfs/juicefs/`.
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
2. Inspect the BrewFS hot path and the matching JuiceFS code path.
3. Add or adjust focused tests before risky behavior changes.
4. Make the smallest code change that can prove or disprove the hypothesis.
5. Run local compile/test checks relevant to the touched code.
6. Run BrewFS perf and JuiceFS perf with matching parameters.
7. Compare fio JSON, metadata logs, and total script wall time.
8. Update `README.md` with the new performance comparison table.
9. Review the diff for correctness, concurrency, cache consistency, and cleanup.
10. Commit only changes backed by perf or correctness evidence.
11. Revert experiments that do not improve the target metric or that regress
    important secondary scenarios.

Do not keep speculative changes because they look theoretically better. Perf
evidence decides.

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
