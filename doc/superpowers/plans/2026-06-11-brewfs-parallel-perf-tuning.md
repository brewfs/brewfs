# BrewFS Parallel Performance Tuning Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Use isolated parallel agents to produce small BrewFS performance candidates, then merge only changes that improve the same perf matrix without hiding cost in buffered writes, close, or post-write drain.

**Architecture:** Each agent works in a dedicated git worktree and owns a non-overlapping subsystem. The coordinator branch cherry-picks only reviewed commits, runs the same baseline/candidate matrix, and reverts any candidate that improves one metric while regressing `fio-randrw` direct mode, write tail, metadata tests, or post-write drain.

**Tech Stack:** Rust BrewFS VFS/cache/meta code, Docker compose Redis/RustFS perf runner, fio direct matrix, `post-write-drain.tsv`, BrewFS `.stats`, and optional `tools/perf` artifact analyzers.

---

## Base And Coordination State

- Base branch: `codex/writeback-backpressure-drain`
- Base commit: `a28239a perf: expose writeback drain backpressure metrics`
- Coordinator worktree: `/mnt/slayerfs/brewfs/.worktrees/perf-tune-integration`
- Coordinator branch: `codex/perf-tune-integration`

The base contains:

- `.stats` counters for writeback backpressure soft sleeps and hard waits.
- `PERF_FIO_POST_WRITE_DRAIN` support in the compose perf runner.
- A rejected hysteresis experiment result: buffered `fio-randrw` improved, but direct `fio-randrw` regressed and post-write drain was mixed. Do not reintroduce hysteresis as a default.

## Agent Roster

| Agent | ID | Branch | Worktree | Ownership |
| --- | --- | --- | --- | --- |
| writer admission | `019eb62c-61fa-7923-8014-c8609ccfa533` | `codex/perf-tune-writer` | `/mnt/slayerfs/brewfs/.worktrees/perf-tune-writer` | `src/vfs/io/writer.rs` admission/backpressure only |
| upload pipeline | `019eb62c-8df1-7bd1-8027-76473251f75a` | `codex/perf-tune-upload` | `/mnt/slayerfs/brewfs/.worktrees/perf-tune-upload` | upload dispatch/drain path, avoid admission policy |
| read/cache | `019eb62c-bb43-7722-bde3-c9250cf58dbc` | `codex/perf-tune-read` | `/mnt/slayerfs/brewfs/.worktrees/perf-tune-read` | reader/cache/prefetch path only |
| metadata cache | `019eb62c-e5ca-7443-a36e-0da60abc62aa` | `codex/perf-tune-meta` | `/mnt/slayerfs/brewfs/.worktrees/perf-tune-meta` | metadata cache analysis and plan |
| perf harness | `019eb62d-0f70-7f41-8d1b-dc0254098303` | `codex/perf-tune-harness` | `/mnt/slayerfs/brewfs/.worktrees/perf-tune-harness` | scripts/tools for A/B report only |

## Merge Rules

- Do not merge directly from an agent branch.
- Read the agent's final report and inspect its diff first:

```bash
git -C /mnt/slayerfs/brewfs/.worktrees/perf-tune-integration diff --stat a28239a..<agent-branch>
git -C /mnt/slayerfs/brewfs/.worktrees/perf-tune-integration log --oneline a28239a..<agent-branch>
```

- Reject candidates that:
  - edit outside their ownership scope without a clear reason,
  - introduce broad config/default changes without an A/B gate,
  - do not include targeted tests for code changes,
  - improve buffered `direct=0` while regressing `direct=1` by more than 5% throughput or 25% p99.9 latency,
  - increase post-write drain for write workloads by more than 10% unless the workload's fio runtime improves enough to reduce total wall time.
- Cherry-pick one candidate at a time onto `codex/perf-tune-integration`.
- After each cherry-pick, run targeted tests before trying the next candidate.

## Baseline Matrix

Run the baseline from `a28239a` and record artifact IDs before evaluating candidates:

```bash
cd /mnt/slayerfs/brewfs/.worktrees/perf-tune-integration
PERF_FIO_DIRECT_MATRIX="0 1" \
PERF_FIO_RANDRW_RUNTIME=20 \
PERF_FIO_RANDWRITE_RUNTIME=20 \
PERF_FIO_SEQWRITE_RUNTIME=20 \
PERF_FIO_POST_WRITE_DRAIN=true \
PERF_FIO_PREFILL_DRAIN_TIMEOUT_SECS=600 \
BREWFS_COMPRESSION=none \
BREWFS_PREFETCH_ENABLED=true \
BREWFS_UPLOAD_CONCURRENCY=32 \
bash docker/compose-xfstests/run_redis_perf.sh \
  --writeback-throughput-profile \
  --tools "fio-seqwrite fio-randwrite fio-randrw"
```

Required baseline artifacts:

- `perf-summary.tsv`
- `post-write-drain.tsv`
- `results/fio-*.json`
- `diagnostics/stats-*-after.txt`
- `report.md`

## Candidate Matrix

Run the same command on every candidate after targeted tests pass. For read/cache candidates, add read-heavy tools:

```bash
PERF_FIO_DIRECT_MATRIX="0 1" \
PERF_FIO_RANDREAD_RUNTIME=20 \
PERF_FIO_SEQREAD_RUNTIME=20 \
PERF_FIO_POST_WRITE_DRAIN=true \
PERF_FIO_PREFILL_DRAIN_TIMEOUT_SECS=600 \
BREWFS_COMPRESSION=none \
BREWFS_PREFETCH_ENABLED=true \
BREWFS_UPLOAD_CONCURRENCY=32 \
bash docker/compose-xfstests/run_redis_perf.sh \
  --writeback-throughput-profile \
  --tools "fio-seqread fio-randread fio-randrw"
```

For final acceptance, run the full set:

```bash
PERF_FIO_DIRECT_MATRIX="0 1" \
PERF_FIO_POST_WRITE_DRAIN=true \
PERF_FIO_PREFILL_DRAIN_TIMEOUT_SECS=600 \
BREWFS_COMPRESSION=none \
BREWFS_PREFETCH_ENABLED=true \
BREWFS_UPLOAD_CONCURRENCY=32 \
bash docker/compose-xfstests/run_redis_perf.sh \
  --writeback-throughput-profile \
  --tools "fio-bigwrite fio-bigread fio-seqread fio-seqwrite fio-randread fio-randwrite fio-randrw metaperf dirstress dirperf"
```

## Task 1: Collect Agent Results

- [ ] Wait for all five agents to reach `DONE`, `DONE_WITH_CONCERNS`, or `BLOCKED`.
- [ ] Record each final report in this plan or in a sibling review document.
- [ ] For every code-changing branch, capture:

```bash
git -C /mnt/slayerfs/brewfs/.worktrees/perf-tune-integration log --oneline a28239a..<agent-branch>
git -C /mnt/slayerfs/brewfs/.worktrees/perf-tune-integration diff --stat a28239a..<agent-branch>
```

## Task 2: Review Candidate Diffs

- [ ] Check whether the changed files match the agent ownership table.
- [ ] Check whether tests exist for changed Rust behavior.
- [ ] Check whether scripts fail clearly when artifacts are missing.
- [ ] Reject or send back candidates with unbounded background work, hidden async tasks, or default config changes without perf evidence.

## Task 3: Integrate One Candidate At A Time

- [ ] Cherry-pick the smallest accepted candidate.
- [ ] Run its targeted tests.
- [ ] Run the baseline/candidate perf matrix against the current integration branch.
- [ ] If metrics fail acceptance, revert the cherry-pick immediately:

```bash
git revert <candidate-sha>
```

- [ ] If metrics pass, keep the commit and move to the next candidate.

## Task 4: Final Verification

- [ ] Run Rust targeted tests for touched modules.
- [ ] Run `cargo fmt --all --check`.
- [ ] Run `bash -n` on touched shell scripts.
- [ ] Run the final full perf set.
- [ ] Commit/push only accepted changes plus this management plan.

## Reporting Template

For every attempted candidate, record:

```text
Candidate:
Branch:
Commit:
Touched files:
Targeted tests:
Perf artifact baseline:
Perf artifact candidate:
fio-randrw direct0 delta:
fio-randrw direct1 delta:
post-write drain delta:
Decision: keep / revert
Reason:
```

## Attempt Log

### Attempt 1: Writer Soft-Sleep Recheck

Candidate: recheck pending bytes after every soft backpressure sleep instead of admitting immediately.
Branch: `codex/perf-tune-writer`
Commit: `b27460084d555a3a6376af93eb388fecda60d56d`
Integration commit: `c453ce7`
Revert commit: `32b7923`
Touched files: `src/vfs/io/writer.rs`
Targeted tests: `CARGO_TARGET_DIR=/mnt/slayerfs/brewfs/target cargo test -p brewfs vfs::io::writer --lib` passed, 25 tests.
Perf artifact baseline: `docker/compose-xfstests/artifacts/perf-run-1781173824-24909`
Perf artifact candidate: `docker/compose-xfstests/artifacts/perf-run-1781174643-16097`

Result:

| Tool | Metric | Baseline | Candidate | Delta |
| --- | --- | ---: | ---: | ---: |
| `fio-randrw-direct0` | total seconds | 61 | 40 | -34.4% |
| `fio-randrw-direct0` | post-write drain seconds | 26 | 2 | -92.3% |
| `fio-randrw-direct0` | read BW MiB/s | 307.72 | 253.32 | -17.7% |
| `fio-randrw-direct0` | write BW MiB/s | 139.95 | 116.04 | -17.1% |
| `fio-randrw-direct0` | write p99 ms | 11.47 | 3036.68 | +26377.7% |
| `fio-randrw-direct0` | write p99.9 ms | 40.11 | 8791.26 | +21819.0% |
| `fio-randrw-direct1` | total seconds | 70 | 44 | -37.1% |
| `fio-randrw-direct1` | post-write drain seconds | 40 | 20 | -50.0% |
| `fio-randrw-direct1` | read BW MiB/s | 235.44 | 213.63 | -9.3% |
| `fio-randrw-direct1` | write BW MiB/s | 108.60 | 98.48 | -9.3% |
| `fio-randrw-direct1` | write p99 ms | 193.99 | 242.22 | +24.9% |
| `fio-randrw-direct1` | write p99.9 ms | 16844.33 | 15770.58 | -6.4% |
| `fio-randwrite-direct0` | total seconds | 71 | 42 | -40.8% |
| `fio-randwrite-direct0` | write BW MiB/s | 77.89 | 88.10 | +13.1% |
| `fio-randwrite-direct0` | write p99 ms | 50.07 | 11609.83 | +23087.4% |
| `fio-randwrite-direct1` | total seconds | 83 | 57 | -31.3% |
| `fio-randwrite-direct1` | write BW MiB/s | 66.42 | 55.84 | -15.9% |
| `fio-seqwrite-direct0` | total seconds | 69 | 47 | -31.9% |
| `fio-seqwrite-direct1` | total seconds | 41 | 43 | +4.9% |

Decision: reverted.

Reason: the candidate correctly reduced pending-upload overshoot and hard waits, but did so by turning soft backpressure into millions of foreground sleeps. That improved post-write drain and total wall time for several write workloads, but violated the acceptance gate by regressing `fio-randrw` active read/write throughput by more than 5% and causing severe `direct=0` write tail regressions. A follow-up candidate must cap or budget soft rechecks instead of looping until pending bytes drain.

### Attempt 2: Writer Single Soft Recheck

Candidate: cap the soft backpressure recheck loop to one sleep/recheck before allowing soft-band writes.
Branch: `codex/perf-tune-writer`
Commits: `b27460084d555a3a6376af93eb388fecda60d56d`, `5d60341`
Integration commits: `d6b4596`, `9deb4d4`
Revert commits: `ddd0806`, `1eac5b6`
Touched files: `src/vfs/io/writer.rs`
Targeted tests: `CARGO_TARGET_DIR=/mnt/slayerfs/brewfs/target cargo test -p brewfs vfs::io::writer --lib` passed, 25 tests.
Perf artifact baseline: `docker/compose-xfstests/artifacts/perf-run-1781173824-24909`
Perf artifact candidate: `docker/compose-xfstests/artifacts/perf-run-1781176203-28231`

Result:

| Tool | Metric | Baseline | Candidate | Delta |
| --- | --- | ---: | ---: | ---: |
| `fio-randrw-direct0` | total seconds | 61 | 58 | -4.9% |
| `fio-randrw-direct0` | read BW MiB/s | 307.72 | 305.09 | -0.9% |
| `fio-randrw-direct0` | write BW MiB/s | 139.95 | 139.49 | -0.3% |
| `fio-randrw-direct0` | write p99 ms | 11.47 | 383.78 | +3246.3% |
| `fio-randrw-direct0` | write p99.9 ms | 40.11 | 583.01 | +1353.6% |
| `fio-randrw-direct1` | total seconds | 70 | 64 | -8.6% |
| `fio-randrw-direct1` | read BW MiB/s | 235.44 | 270.24 | +14.8% |
| `fio-randrw-direct1` | write BW MiB/s | 108.60 | 127.12 | +17.1% |
| `fio-randrw-direct1` | write p99 ms | 193.99 | 28.18 | -85.5% |
| `fio-randwrite-direct0` | total seconds | 71 | 73 | +2.8% |
| `fio-randwrite-direct0` | write p99 ms | 50.07 | 110.63 | +120.9% |
| `fio-randwrite-direct1` | total seconds | 83 | 79 | -4.8% |
| `fio-seqwrite-direct0` | total seconds | 69 | 66 | -4.3% |
| `fio-seqwrite-direct0` | write BW MiB/s | 164.67 | 151.41 | -8.1% |
| `fio-seqwrite-direct1` | total seconds | 41 | 42 | +2.4% |

Decision: reverted.

Reason: the cap avoids the Attempt 1 soft-sleep explosion and improves `fio-randrw-direct1`, but the change is still not safe as a default. `fio-randrw-direct0` write p99/p99.9 regressed far beyond the 25% tail gate, `fio-randwrite-direct0` p99 doubled, and `fio-seqwrite-direct0` throughput regressed by 8.1%. This suggests admission-only tweaks are trading where latency appears instead of removing the underlying upload/drain bottleneck. Next write-path work should target upload queueing, object count, or slice aggregation rather than more soft admission tuning.

### Attempt 3: Writeback Upload Concurrency 6

Candidate: run the same writeback throughput matrix with `BREWFS_WRITEBACK_UPLOAD_CONCURRENCY=6` instead of the profile default `4`.
Branch: `codex/perf-tune-integration`
Commit: none; configuration-only experiment.
Touched files: none.
Perf artifact baseline: `docker/compose-xfstests/artifacts/perf-run-1781173824-24909`
Perf artifact candidate: `docker/compose-xfstests/artifacts/perf-run-1781177224-18180`

Partial result before aborting the rejected run:

| Tool | Metric | Baseline | Candidate | Delta |
| --- | --- | ---: | ---: | ---: |
| `fio-seqwrite-direct0` | fio seconds | 56 | 52 | -7.1% |
| `fio-seqwrite-direct0` | post-write drain seconds | 13 | 17 | +30.8% |
| `fio-seqwrite-direct1` | fio seconds | 33 | 31 | -6.1% |
| `fio-seqwrite-direct1` | post-write drain seconds | 8 | 15 | +87.5% |

Decision: rejected; no code or default config change.

Reason: raising global writeback upload concurrency from 4 to 6 made active fio time slightly shorter but moved more cost into post-write drain. Both seqwrite direct modes exceeded the 10% drain regression gate before the run reached `fio-randrw`, so the run was stopped early. This suggests simply widening the global writeback PUT pool increases burstiness rather than improving end-to-end writeback completion. The next candidate should reduce object/slice amplification or improve drain scheduling fairness, not only raise concurrency.

### Attempt 4: Delay Writable Slice Dispatch

Candidate: keep full blocks in a still-writable slice from being background-dispatched until flush/freeze, gated by `BREWFS_DELAY_WRITABLE_SLICE_DISPATCH=1`.
Branch: `codex/perf-tune-dispatch-delay`
Commits tested: `53ce2bc perf: gate writable slice dispatch delay`, plus test-only env propagation and the current integration fixes.
Perf artifact baseline: `docker/compose-xfstests/artifacts/perf-run-1781179262-25151`
Perf artifact candidate: `/mnt/slayerfs/brewfs/.worktrees/perf-tune-dispatch-delay/docker/compose-xfstests/artifacts/perf-run-1781180416-71`

Smoke command:

```bash
PERF_FIO_DIRECT_MATRIX="0 1" \
PERF_FIO_SEQWRITE_RUNTIME=15 \
PERF_FIO_RANDRW_RUNTIME=15 \
PERF_FIO_POST_WRITE_DRAIN=true \
PERF_FIO_POST_WRITE_DRAIN_TIMEOUT_SECS=600 \
PERF_FIO_PREFILL_DRAIN_TIMEOUT_SECS=600 \
BREWFS_COMPRESSION=none \
BREWFS_PREFETCH_ENABLED=true \
BREWFS_UPLOAD_CONCURRENCY=32 \
BREWFS_DELAY_WRITABLE_SLICE_DISPATCH=1 \
CARGO_PROFILE_RELEASE_DEBUG=0 \
bash docker/compose-xfstests/run_redis_perf.sh \
  --writeback-throughput-profile \
  --tools "fio-seqwrite fio-randrw"
```

Key result:

| Tool | Metric | Baseline | Candidate | Delta |
| --- | --- | ---: | ---: | ---: |
| `fio-randrw-direct0` | tool wall seconds | 71 | 100 | +40.8% |
| `fio-randrw-direct0` | read BW MiB/s | 743.14 | 846.92 | +14.0% |
| `fio-randrw-direct0` | write BW MiB/s | 336.90 | 385.73 | +14.5% |
| `fio-randrw-direct0` | PUT ops/GiB written | 2266.85 | 3479.27 | +53.5% |
| `fio-randrw-direct0` | S3 PUT avg object MiB | 0.466 | 0.301 | -35.4% |
| `fio-randrw-direct0` | soft backpressure sleep ms | 162140 | 247406 | +52.6% |
| `fio-randrw-direct1` | tool wall seconds | 44 | 33 | -25.0% |
| `fio-randrw-direct1` | post-write drain seconds | 44 | 51 | +15.9% |
| `fio-seqwrite-direct0` | tool wall seconds | 50 | 39 | -22.0% |
| `fio-seqwrite-direct0` | post-write drain seconds | 2 | 6 | +200.0% |
| `fio-seqwrite-direct1` | write BW MiB/s | 145.26 | 137.06 | -5.6% |
| `fio-seqwrite-direct1` | write p99.9 ms | 7683.97 | 15770.58 | +105.2% |

Decision: rejected; do not merge as a performance change.

Reason: the candidate shifts cost out of some foreground paths but increases buffered `randrw` wall time, object count, and soft backpressure. It also exceeds the drain regression gate for `seqwrite-direct0` and `randrw-direct1`, and regresses `seqwrite-direct1` throughput and p99.9. This confirms that delaying dispatch of still-writable full blocks is not the right default direction. The next candidate should target JuiceFS-style staged upload queueing or object-count reduction, not later dispatch of already full blocks.

### Attempt 5: FileReader Direct Output Buffer

Candidate: change `FileReader::read_at` to fill the final output buffer directly through `DataFetcher::read_at_into`, avoiding per-span `Bytes` allocation and final concatenation.
Branch: `codex/perf-tune-integration`
Commit: none; reverted after smoke.
Perf artifact baseline: `docker/compose-xfstests/artifacts/perf-run-1781179262-25151`
Perf artifact candidate: `docker/compose-xfstests/artifacts/perf-run-1781181822-17005`

Validation before perf:

```bash
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/mnt/slayerfs/brewfs/target cargo test -p brewfs vfs::io::reader --lib
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/mnt/slayerfs/brewfs/target cargo test -p brewfs chunk::reader --lib
cargo fmt -p brewfs --check
git diff --check
```

Perf smoke command:

```bash
PERF_FIO_DIRECT_MATRIX="0 1" \
PERF_FIO_SEQREAD_RUNTIME=15 \
PERF_FIO_RANDRW_RUNTIME=15 \
PERF_FIO_POST_WRITE_DRAIN=true \
PERF_FIO_POST_WRITE_DRAIN_TIMEOUT_SECS=600 \
PERF_FIO_PREFILL_DRAIN_TIMEOUT_SECS=600 \
BREWFS_COMPRESSION=none \
BREWFS_PREFETCH_ENABLED=true \
BREWFS_UPLOAD_CONCURRENCY=32 \
CARGO_PROFILE_RELEASE_DEBUG=0 \
bash docker/compose-xfstests/run_redis_perf.sh \
  --writeback-throughput-profile \
  --tools "fio-seqread fio-randrw"
```

Key result:

| Tool | Metric | Baseline | Candidate | Delta |
| --- | --- | ---: | ---: | ---: |
| `fio-seqread-direct0` | read BW MiB/s | 1506.57 | 1749.75 | +16.1% |
| `fio-seqread-direct1` | read BW MiB/s | 3125.39 | 3887.47 | +24.4% |
| `fio-randrw-direct0` | tool wall seconds | 71 | 49 | -31.0% |
| `fio-randrw-direct0` | read BW MiB/s | 743.14 | 631.57 | -15.0% |
| `fio-randrw-direct0` | write BW MiB/s | 336.90 | 290.06 | -13.9% |
| `fio-randrw-direct0` | write p99 ms | 6.32 | 96.99 | +1433.7% |
| `fio-randrw-direct0` | write p99.9 ms | 11.86 | 1770.00 | +14821.5% |
| `fio-randrw-direct0` | GET ops/GiB read | 307.90 | 153.77 | -50.1% |
| `fio-randrw-direct1` | read/write BW MiB/s | 156.17 / 72.02 | 173.96 / 80.68 | +11.4% / +12.0% |
| `fio-randrw-direct1` | post-write drain seconds | 44 | 48 | +9.1% |

Decision: rejected; code reverted.

Reason: the refactor substantially improves sequential read and reduces GET amplification, but it violates the mixed workload hard gate: `fio-randrw-direct0` active read/write throughput regressed by more than 5%, and write p99/p99.9 regressed catastrophically. The lower wall time and lower GET count are not enough to accept a change that makes buffered mixed writes unpredictable. A future read-path optimization needs repeated samples and a design that does not worsen write tail under FUSE writeback-cache.

### Attempt 6: Max Unflushed Slices 16

Candidate: expose `MAX_UNFLUSHED_SLICES` as `BREWFS_MAX_UNFLUSHED_SLICES`, keep the default at `3`, and run the writeback smoke with `BREWFS_MAX_UNFLUSHED_SLICES=16`.
Branch: `codex/perf-tune-integration`
Commit: none; code reverted after smoke.
Perf artifact baseline: `docker/compose-xfstests/artifacts/perf-run-1781179262-25151`
Perf artifact candidate: `docker/compose-xfstests/artifacts/perf-run-1781182550-30708`

Validation before perf:

```bash
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/mnt/slayerfs/brewfs/target cargo test -p brewfs write_config_defaults_and_sets_max_unflushed_slices --lib
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/mnt/slayerfs/brewfs/target cargo test -p brewfs vfs::io::writer --lib
bash -n docker/compose-xfstests/run_redis_perf.sh
bash -n docker/compose-xfstests/run_perf_in_container.sh
cargo fmt -p brewfs --check
git diff --check
```

Perf smoke command:

```bash
PERF_FIO_DIRECT_MATRIX="0 1" \
PERF_FIO_SEQWRITE_RUNTIME=15 \
PERF_FIO_RANDRW_RUNTIME=15 \
PERF_FIO_POST_WRITE_DRAIN=true \
PERF_FIO_POST_WRITE_DRAIN_TIMEOUT_SECS=600 \
PERF_FIO_PREFILL_DRAIN_TIMEOUT_SECS=600 \
BREWFS_COMPRESSION=none \
BREWFS_PREFETCH_ENABLED=true \
BREWFS_UPLOAD_CONCURRENCY=32 \
BREWFS_MAX_UNFLUSHED_SLICES=16 \
CARGO_PROFILE_RELEASE_DEBUG=0 \
bash docker/compose-xfstests/run_redis_perf.sh \
  --writeback-throughput-profile \
  --tools "fio-seqwrite fio-randrw"
```

Key result:

| Tool | Metric | Baseline | Candidate | Delta |
| --- | --- | ---: | ---: | ---: |
| `fio-randrw-direct0` | tool wall seconds | 71 | 99 | +39.4% |
| `fio-randrw-direct0` | write p99 ms | 6.32 | 10.42 | +64.8% |
| `fio-randrw-direct0` | write p99.9 ms | 11.86 | 1019.22 | +8492.3% |
| `fio-randrw-direct0` | PUT ops/GiB written | 2266.85 | 3200.40 | +41.2% |
| `fio-randrw-direct0` | GET ops/GiB read | 307.90 | 441.57 | +43.4% |
| `fio-randrw-direct0` | S3 PUT avg object MiB | 0.466 | 0.339 | -27.4% |
| `fio-randrw-direct1` | read/write BW MiB/s | 156.17 / 72.02 | 175.20 / 81.07 | +12.2% / +12.6% |
| `fio-randrw-direct1` | post-write drain seconds | 44 | 43 | -2.3% |
| `fio-seqwrite-direct0` | write BW MiB/s | 186.37 | 171.93 | -7.8% |
| `fio-seqwrite-direct0` | write p99.9 ms | 13.17 | 233.83 | +1675.1% |
| `fio-seqwrite-direct1` | post-write drain seconds | 40 | 46 | +15.0% |

Decision: rejected; code reverted.

Reason: allowing many more unflushed writable slices worsened the exact object-amplification symptom it was meant to address. It improved `randrw-direct1` foreground throughput, but caused buffered `randrw-direct0` wall time, PUT/GET ops per GiB, and write tail to regress hard, and it also regressed `seqwrite-direct0` throughput and `seqwrite-direct1` drain. This falsifies the simple "raise max unflushed slices" theory. The next write-path attempt should instrument freeze reasons and live slice counts first, or move to a bounded stage/upload queue with clear recovery semantics.

### Attempt 7: Stage-Before-Commit Barrier

Candidate: require local writeback `persist_slice` to complete before `CommitBeforeUpload` publishes metadata for a still-uploading slice. The prototype added a per-slice `writeback_persisted` flag, woke `commit_chunk` when staging finished, and reported persist failure through the existing writeback error path.
Branch: `codex/perf-tune-integration`
Commit: none; code reverted after perf.
Touched files: `src/vfs/io/writer.rs`
Perf artifact baseline: `docker/compose-xfstests/artifacts/perf-run-1781179262-25151`
Perf artifact candidate: `docker/compose-xfstests/artifacts/perf-run-1781188559-17056`

Validation before perf:

```bash
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/mnt/slayerfs/brewfs/target cargo test -p brewfs test_commit_before_upload_requires_persist_before_metadata_commit --lib
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/mnt/slayerfs/brewfs/target cargo test -p brewfs vfs::io::writer --lib
cargo fmt --check
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/mnt/slayerfs/brewfs/target cargo clippy -p brewfs --lib -- -D warnings
```

Perf command:

```bash
PERF_FIO_DIRECT_MATRIX="0 1" \
PERF_FIO_RANDRW_RUNTIME=20 \
PERF_FIO_RANDWRITE_RUNTIME=20 \
PERF_FIO_RANDREAD_RUNTIME=20 \
PERF_FIO_SEQWRITE_RUNTIME=20 \
PERF_FIO_SEQREAD_RUNTIME=20 \
PERF_FIO_POST_WRITE_DRAIN=true \
PERF_FIO_PREFILL_DRAIN_TIMEOUT_SECS=600 \
BREWFS_COMPRESSION=none \
BREWFS_PREFETCH_ENABLED=true \
BREWFS_UPLOAD_CONCURRENCY=32 \
bash docker/compose-xfstests/run_redis_perf.sh \
  --writeback-throughput-profile \
  --tools "fio-seqread fio-seqwrite fio-randread fio-randwrite fio-randrw"
```

Key result:

| Tool | Metric | Baseline | Candidate | Delta |
| --- | --- | ---: | ---: | ---: |
| `fio-randrw-direct0` | tool wall seconds | 71 | 50 | -29.6% |
| `fio-randrw-direct0` | read/write BW MiB/s | 743.14 / 336.90 | 810.56 / 365.69 | +9.1% / +8.5% |
| `fio-randrw-direct0` | write p99 ms | 6.32 | 9.11 | +44.0% |
| `fio-randrw-direct0` | write p99.9 ms | 11.86 | 123.21 | +938.7% |
| `fio-randrw-direct0` | post-write drain seconds | 10 | 20 | +100.0% |
| `fio-randrw-direct0` | PUT ops/GiB written | 2266.85 | 389.44 | -82.8% |
| `fio-randrw-direct0` | S3 PUT avg object MiB | 0.466 | 2.266 | +386.0% |
| `fio-randrw-direct1` | tool wall seconds | 44 | 128 | +190.9% |
| `fio-randrw-direct1` | read/write BW MiB/s | 156.17 / 72.02 | 206.65 / 92.40 | +32.3% / +28.3% |
| `fio-randrw-direct1` | write p99 ms | 27.39 | 88.61 | +223.4% |
| `fio-randrw-direct1` | post-write drain seconds | 44 | 53 | +20.5% |
| `fio-seqwrite-direct0` | write BW MiB/s | 186.37 | 1884.71 | +911.3% |
| `fio-seqwrite-direct1` | write BW MiB/s | 145.26 | 225.05 | +54.9% |
| `fio-seqread-direct0` | read BW MiB/s | 1506.57 | 1185.54 | -21.3% |
| `fio-seqread-direct1` | read BW MiB/s | 3125.39 | 3475.83 | +11.2% |

Decision: rejected; code reverted. The perf script produced the artifact and all benchmark rows passed, but the wrapper exited with `container=1, bench=0` during container teardown.

Reason: the barrier improved object size and reduced `direct0` PUT amplification, which confirms that staging/aggregation is the right area. However, the implementation is not acceptable: it doubled `randrw-direct0` drain, regressed `randrw-direct0` write p99/p99.9 beyond the tail gate, and caused `randrw-direct1` wall time, write p99, and drain to regress far beyond the direct-mode gate. A safe design cannot make foreground direct writes wait on local staging or cause the upload task to complete only after both staging and remote upload. The next attempt should decouple the three phases explicitly: seal/stage quickly, commit metadata only after stage, and let a bounded uploader drain staged records without delaying direct foreground progress.

### Attempt 8: Writeback Phase Accounting

Candidate: add diagnostics for the staged-upload bottleneck without changing writeback behavior. The patch tracks local staging in-flight bytes, remote upload in-flight bytes, stage ops/bytes/latency/failures, and metadata commits that happen before local staging completes.
Branch: `codex/perf-tune-integration`
Touched files: `src/vfs/io/writer.rs`, `src/vfs/fs/mod.rs`, `src/vfs/stats.rs`, `tools/perf/compare_artifacts.py`, `docker/compose-xfstests/run_perf_in_container.sh`
Perf artifact candidate: `docker/compose-xfstests/artifacts/perf-run-1781191702-22056`

Validation:

```bash
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/mnt/slayerfs/brewfs/target cargo test -p brewfs test_writeback_phase_metrics_track_stage_and_remote_upload --lib
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/mnt/slayerfs/brewfs/target cargo test -p brewfs render_contains_all_metrics --lib
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/mnt/slayerfs/brewfs/target cargo test -p brewfs snapshot_exposes_derived_values_without_divide_by_zero --lib
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/mnt/slayerfs/brewfs/target cargo test -p brewfs vfs::io::writer --lib
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/mnt/slayerfs/brewfs/target cargo test -p brewfs vfs::stats --lib
bash -n docker/compose-xfstests/run_perf_in_container.sh
bash tools/perf/test_compare_artifacts.sh
cargo fmt --check
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/mnt/slayerfs/brewfs/target cargo clippy -p brewfs --lib -- -D warnings
```

Perf smoke command:

```bash
PERF_FIO_DIRECT_MATRIX="0 1" \
PERF_FIO_RANDRW_RUNTIME=10 \
PERF_FIO_POST_WRITE_DRAIN=true \
PERF_FIO_PREFILL_DRAIN_TIMEOUT_SECS=600 \
BREWFS_COMPRESSION=none \
BREWFS_PREFETCH_ENABLED=true \
BREWFS_UPLOAD_CONCURRENCY=32 \
CARGO_PROFILE_RELEASE_DEBUG=0 \
bash docker/compose-xfstests/run_redis_perf.sh \
  --writeback-throughput-profile \
  --tools "fio-randrw"
```

Smoke result:

| Tool | fio seconds | post-write drain seconds | Stage ops | Stage bytes | Stage latency | Stage failures | Commit before stage | Remote upload in-flight at tool end |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `fio-randrw-direct0` | 18 | 16 | 1649 | 2631.2 MiB | 2620.3s | 1 | 443 | 1267.9 MiB |
| `fio-randrw-direct0-post-write-drained` | n/a | n/a | 1685 | 2764.0 MiB | 2622.7s | 1 | 443 | 0.0 MiB |
| `fio-randrw-direct1` | 31 | 53 | 695 | 2811.0 MiB | 5395.2s | 0 | 457 | 1467.0 MiB |
| `fio-randrw-direct1-post-write-drained` | n/a | n/a | 776 | 3212.0 MiB | 5399.4s | 0 | 457 | 0.0 MiB |

Decision: keep as diagnostics if code review stays clean.

Reason: this confirms the first-stage barrier cannot safely sit on the current per-batch `persist_slice` model. The current pipeline stages each upload batch using the same `DirtySliceKey { ino, chunk_id, local_seq: slice_id }`, so multiple batches for one slice share the same local `.slice`, `.tmp`, and `.meta` paths. The nonzero stage failure count is therefore a real reliability signal, not just perf noise. It also shows hundreds of metadata commits before local staging completes, which explains why a naive stage-before-commit barrier badly regressed `direct=1`: it adds a new foreground dependency to a path that currently publishes metadata before staging catches up.

### Attempt 9: Unique Staged Batch Keys

Candidate: add a `stage_seq` dimension to `DirtySliceKey`, write each pipelined staged batch to a distinct `.slice/.meta/.tmp` path, track all staged keys in `SliceState`, and clean those keys after metadata commit plus upload drain. The candidate also corrected the dirty record offset from slice-relative to chunk-relative for recovery/overlay semantics.
Branch: `codex/perf-tune-integration`
Touched files: `src/vfs/cache/keys.rs`, `src/vfs/cache/write_back.rs`, `src/vfs/io/writer.rs`
Perf artifact candidate: `docker/compose-xfstests/artifacts/perf-run-1781193104-15857`

Validation:

```bash
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/mnt/slayerfs/brewfs/target cargo test -p brewfs persist_slice_keeps_staged_batches_for_one_remote_slice_distinct --lib
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/mnt/slayerfs/brewfs/target cargo test -p brewfs test_slice_tracks_multiple_writeback_stage_keys_for_cleanup --lib
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/mnt/slayerfs/brewfs/target cargo test -p brewfs vfs::cache::write_back --lib
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/mnt/slayerfs/brewfs/target cargo test -p brewfs vfs::io::writer --lib
cargo fmt
git diff --check
cargo fmt --check
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/mnt/slayerfs/brewfs/target cargo clippy -p brewfs --lib -- -D warnings
```

Perf smoke command:

```bash
PERF_FIO_DIRECT_MATRIX="0 1" \
PERF_FIO_RANDRW_RUNTIME=10 \
PERF_FIO_POST_WRITE_DRAIN=true \
PERF_FIO_PREFILL_DRAIN_TIMEOUT_SECS=600 \
BREWFS_COMPRESSION=none \
BREWFS_PREFETCH_ENABLED=true \
BREWFS_UPLOAD_CONCURRENCY=32 \
CARGO_PROFILE_RELEASE_DEBUG=0 \
bash docker/compose-xfstests/run_redis_perf.sh \
  --writeback-throughput-profile \
  --tools "fio-randrw"
```

Smoke comparison against Attempt 8:

| Tool | Metric | Attempt 8 | Attempt 9 | Delta |
| --- | --- | ---: | ---: | ---: |
| `fio-randrw-direct0` | active wall | 18s | 11s | -38.9% |
| `fio-randrw-direct0` | post-write drain | 16s | 14s | -12.5% |
| `fio-randrw-direct0` | write BW | 276.2 MiB/s | 248.9 MiB/s | -9.9% |
| `fio-randrw-direct0` | stage failures | 1 | 14 | +1300.0% |
| `fio-randrw-direct0-post-write-drained` | PUT ops | 1693 | 1266 | -25.2% |
| `fio-randrw-direct1` | active wall | 31s | 39s | +25.8% |
| `fio-randrw-direct1` | post-write drain | 53s | 46s | -13.2% |
| `fio-randrw-direct1` | write BW | 110.8 MiB/s | 86.3 MiB/s | -22.1% |
| `fio-randrw-direct1` | hard wait ops | 357 | 577 | +61.6% |

Decision: reject and revert the code candidate.

Reason: unique staged batch paths improved some drain/object-count symptoms, but it made the reliability metric worse (`stage_failures_total` rose from 1 to 14 during the direct0 phase) and regressed the `direct=1` active path by more than the acceptable gate. The new failures appear during the buffered direct0 phase and then carry forward into cumulative stats; this points at a remaining staging/cleanup race rather than a safe foundation for a commit-before-stage barrier. The next candidate should not expand per-batch local records in the foreground pipeline. Prefer either a single durable slice-level stage record written once after sealing, or a feature-gated staged uploader queue where stage ownership and cleanup are centralized outside the upload subtask.

### Attempt 10: Full-Slice Stage Record

Candidate: disable per-batch SSD persist for `CommitBeforeUpload` and stage a frozen slice once as a full local dirty record. The first version started full-slice staging before dispatching remaining upload batches; the second version dispatched upload batches first and then started the full-slice stage task. Both versions kept upload-before-commit behavior unchanged and guarded dirty-record cleanup until local staging had completed.
Branch: `codex/perf-tune-integration`
Touched files: `src/vfs/io/writer.rs`
Perf artifacts: `docker/compose-xfstests/artifacts/perf-run-1781194276-26856` (stage before dispatch), `docker/compose-xfstests/artifacts/perf-run-1781194717-1519` (dispatch before stage)

Validation:

```bash
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/mnt/slayerfs/brewfs/target cargo test -p brewfs test_frozen_slice_prepares_one_full_writeback_stage_plan --lib
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/mnt/slayerfs/brewfs/target cargo test -p brewfs test_commit_before_upload_removes_writeback_record_after_upload --lib
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/mnt/slayerfs/brewfs/target cargo test -p brewfs vfs::io::writer --lib
cargo fmt
git diff --check
cargo fmt --check
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/mnt/slayerfs/brewfs/target cargo clippy -p brewfs --lib -- -D warnings
```

Perf smoke command:

```bash
PERF_FIO_DIRECT_MATRIX="0 1" \
PERF_FIO_RANDRW_RUNTIME=10 \
PERF_FIO_POST_WRITE_DRAIN=true \
PERF_FIO_PREFILL_DRAIN_TIMEOUT_SECS=600 \
BREWFS_COMPRESSION=none \
BREWFS_PREFETCH_ENABLED=true \
BREWFS_UPLOAD_CONCURRENCY=32 \
CARGO_PROFILE_RELEASE_DEBUG=0 \
bash docker/compose-xfstests/run_redis_perf.sh \
  --writeback-throughput-profile \
  --tools "fio-randrw"
```

Final dispatch-before-stage comparison against Attempt 8:

| Tool | Metric | Attempt 8 | Attempt 10 | Delta |
| --- | --- | ---: | ---: | ---: |
| `fio-randrw-direct0` | active wall | 18s | 13s | -27.8% |
| `fio-randrw-direct0` | post-write drain | 16s | 2s | -87.5% |
| `fio-randrw-direct0` | read BW | 598.8 MiB/s | 389.9 MiB/s | -34.9% |
| `fio-randrw-direct0` | write BW | 276.2 MiB/s | 172.8 MiB/s | -37.4% |
| `fio-randrw-direct0-post-write-drained` | PUT ops | 1693 | 4160 | +145.7% |
| `fio-randrw-direct0` | stage failures | 1 | 0 | -100.0% |
| `fio-randrw-direct0` | commit before stage | 443 | 64 | -85.6% |
| `fio-randrw-direct1` | active wall | 31s | 19s | -38.7% |
| `fio-randrw-direct1` | post-write drain | 53s | 52s | -1.9% |
| `fio-randrw-direct1` | write BW | 110.8 MiB/s | 197.4 MiB/s | +78.2% |

Decision: reject and revert the code candidate.

Reason: full-slice staging fixes the local-stage reliability symptom and substantially improves `direct=1`, but it badly regresses buffered `randrw` throughput and explodes S3 PUT object count. This means the remaining dominant bottleneck is not just local staging durability; it is slice/object fragmentation under buffered mixed workloads. The next candidate should target object aggregation or freeze/upload dispatch policy directly, with an acceptance gate that preserves `fio-randrw-direct0` throughput and PUT ops while keeping the `direct=1` improvement.

## Next Target: Staged Upload And Object Count

- Treat `fio-randrw-direct0` object amplification as the primary write-path bottleneck: baseline already shows thousands of PUT ops/GiB written and sub-1MiB average PUT object size.
- Keep commit-before-upload semantics, but separate foreground commit progress from S3 PUT completion through a bounded staged uploader design, similar to JuiceFS `stage -> metadata commit -> delayed upload`.
- Do not accept a plain "stage-before-commit barrier" inside the current upload task. Attempt 7 showed that this can improve object aggregation while badly regressing `direct=1` wall time and write tails.
- Add explicit phase accounting before the next code candidate: time spent staging, time waiting for stage before metadata commit, staged bytes queued for upload, uploader active bytes, and object upload completion lag.
- Before enforcing any commit-before-stage barrier, fix the staging model so one local durable record corresponds to one logical slice, or give each staged batch a unique durable key and recovery semantics. The current per-batch persist-to-same-key pattern is not a safe barrier foundation.
- Preserve the current safe path as the default; any staged uploader behavior must be feature-gated and must pass recovery, remount, and post-write-drain checks before becoming part of the throughput profile.
- Use `compare_artifacts.py` amplification metrics as the acceptance gate. A candidate must reduce PUT ops or tail/backpressure without regressing `direct=1` throughput, p99.9, or post-write drain beyond the existing gates.
