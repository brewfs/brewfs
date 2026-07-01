# BrewFS Writeback Backpressure And Drain Observability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the next BrewFS performance attempt target the observed writer tail bottleneck without hiding cost in kernel page cache, close, or post-fio background drain.

**Architecture:** First add report-only observability for writeback backpressure waits and post-write drain time, then run a feature-flagged hysteresis admission experiment for `CommitBeforeUpload`. Keep the first implementation reversible and do not start small-write coalescing until the new metrics prove that PUT/object count, not admission waiting, is the dominant tail driver.

**Tech Stack:** Rust BrewFS writer and stats modules, Tokio, Redis metadata backend, RustFS/S3 object backend, Docker compose perf runner, fio direct matrix, existing `.stats` Prometheus text output.

---

## Review Inputs

The plan is based on the current review set under `/mnt/slayerfs/doc/`:

- `/mnt/slayerfs/doc/performance/review-writeback-writer.md`: `CommitBeforeUpload` creates metadata-visible, not-yet-uploaded slices; current soft/hard backpressure has no wait counters and can trade close tail for write p99.
- `/mnt/slayerfs/doc/performance/review-perf-harness-config.md`: `direct=0` mixes kernel page cache and FUSE writeback-cache; write workloads need post-write drain accounting; all accepted optimizations must include `randrw`.
- `/mnt/slayerfs/doc/performance/review-object-store-cache.md`: random write/mixed workloads are dominated by small object PUT amplification; concurrency tuning without object-count/drain metrics is easy to misread.
- `/mnt/slayerfs/doc/performance/review-metadata-cache.md`: metadata batching is not the next default attempt because the earlier batch direction improved a narrow profile but failed compose `randrw`.
- `/mnt/slayerfs/doc/performance/review-parallel-agents-summary.md`: implement writer pending/staging/watchdog metrics first, then improve the read/object and metadata layers.
- `/mnt/slayerfs/doc/performance/perf-optimization-roadmap.md`: range-prefetch pending gate and LZ4 raw-fallback zero-copy were tested and rejected; avoid repeating them.

## Decision

Next attempt:

1. Add `.stats` counters for writeback backpressure soft sleep and hard wait time.
2. Add perf-runner post-write drain measurement for `fio-seqwrite`, `fio-randwrite`, `fio-randrw`, and `fio-bigwrite`.
3. Implement an opt-in hysteresis mode for `CommitBeforeUpload` backpressure.
4. Accept the code only if full fio coverage, including `randrw`, shows a real improvement without tail regression.

Deferred:

- Small-write coalescing: likely high-impact, but too risky before we can attribute current tail to PUT object count versus admission wait.
- Metadata `write_batch`: explicitly deferred because prior compose `randrw` behavior was not stable.
- LZ4 read-copy micro-optimizations: recent A/B regressed write tail and should not be retried this cycle.

## Files

- Modify: `/mnt/slayerfs/src/vfs/io/writer.rs`
  - Add backpressure wait metrics around `wait_for_writeback_backpressure`.
  - Add pure decision tests for hysteresis.
  - Keep the new hysteresis behavior behind an environment/config flag.
- Modify: `/mnt/slayerfs/src/vfs/stats.rs`
  - Add counters to `FsStats`, `FsStatsSnapshot`, and Prometheus text output.
- Modify: `/mnt/slayerfs/src/vfs/config.rs`
  - Add an opt-in backpressure policy mode, parsed from `BREWFS_WRITEBACK_BACKPRESSURE_POLICY`.
- Modify: `/mnt/slayerfs/docker/compose-xfstests/run_perf_in_container.sh`
  - Add report-only `PERF_FIO_POST_WRITE_DRAIN` support and artifact fields.
- Modify if needed: `/mnt/slayerfs/doc/performance/perf-optimization-roadmap.md`
  - Record accepted and rejected A/B results.

## Acceptance Gates

Run every candidate with direct matrix enabled:

```bash
cd /mnt/slayerfs/brewfs
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

Required before any performance commit:

- All tools exit `0`.
- `fio-randrw` read BW and write BW do not regress by more than 5% versus the same-commit baseline.
- `fio-randrw` write p99 and p99.9 do not regress by more than 25%.
- `fio-randwrite` write p99 and p99.9 do not regress by more than 25%.
- `post_fio_drain_s` decreases or stays flat for the improved workload.
- New backpressure metrics explain the result: soft sleep/hard wait time must move in the expected direction.
- If `direct=0` improves but `direct=1` regresses, reject the change unless the plan explicitly targets page-cache-visible product behavior and records the regression.

Full acceptance before push:

```bash
cd /mnt/slayerfs/brewfs
PERF_FIO_DIRECT_MATRIX="0 1" \
PERF_FIO_POST_WRITE_DRAIN=true \
PERF_FIO_PREFILL_DRAIN_TIMEOUT_SECS=600 \
BREWFS_COMPRESSION=none \
bash docker/compose-xfstests/run_redis_perf.sh \
  --writeback-throughput-profile \
  --tools "fio-bigwrite fio-bigread fio-seqread fio-seqwrite fio-randread fio-randwrite fio-randrw metaperf dirstress dirperf"
```

## Task 0: Establish Same-Commit Baseline

**Files:**
- No source edits.

- [ ] **Step 1: Capture current branch and dirty state**

Run:

```bash
cd /mnt/slayerfs/brewfs
git rev-parse HEAD
git status --short --branch
```

Expected:
- Branch and commit are recorded in the implementation notes.
- Existing unrelated untracked files remain unstaged.

- [ ] **Step 2: Run the baseline direct matrix**

Run:

```bash
cd /mnt/slayerfs/brewfs
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

Expected:
- Exit code `0`.
- Artifact contains direct `0` and direct `1` rows for each fio profile.
- If this fails before source edits, stop and fix the runner/environment before implementing writer changes.

## Task 1: Add Writeback Backpressure Metrics

**Files:**
- Modify: `/mnt/slayerfs/src/vfs/stats.rs`
- Modify: `/mnt/slayerfs/src/vfs/io/writer.rs`

- [ ] **Step 1: Write the failing stats rendering test**

Add assertions to the existing stats render test in `/mnt/slayerfs/src/vfs/stats.rs`:

```rust
stats.add_writeback_backpressure_soft_sleep(Duration::from_micros(12));
stats.add_writeback_backpressure_hard_wait(Duration::from_micros(34));
let output = stats.render_prometheus();
assert!(output.contains("brewfs_writeback_backpressure_soft_sleep_ops 1"));
assert!(output.contains("brewfs_writeback_backpressure_soft_sleep_us 12"));
assert!(output.contains("brewfs_writeback_backpressure_hard_wait_ops 1"));
assert!(output.contains("brewfs_writeback_backpressure_hard_wait_us 34"));
```

Run:

```bash
cd /mnt/slayerfs/brewfs
cargo test -p brewfs stats --lib
```

Expected:
- The test fails because the counters and methods do not exist.

- [ ] **Step 2: Add stats fields and render output**

Add these fields to `FsStatsSnapshot` and corresponding `AtomicU64` fields to `FsStats`:

```rust
pub writeback_backpressure_soft_sleep_ops: u64,
pub writeback_backpressure_soft_sleep_us: u64,
pub writeback_backpressure_hard_wait_ops: u64,
pub writeback_backpressure_hard_wait_us: u64,
```

Add methods:

```rust
pub fn add_writeback_backpressure_soft_sleep(&self, duration: Duration) {
    self.writeback_backpressure_soft_sleep_ops.fetch_add(1, ORD);
    self.writeback_backpressure_soft_sleep_us
        .fetch_add(duration.as_micros().min(u128::from(u64::MAX)) as u64, ORD);
}

pub fn add_writeback_backpressure_hard_wait(&self, duration: Duration) {
    self.writeback_backpressure_hard_wait_ops.fetch_add(1, ORD);
    self.writeback_backpressure_hard_wait_us
        .fetch_add(duration.as_micros().min(u128::from(u64::MAX)) as u64, ORD);
}
```

Render the metrics as:

```rust
out.push_str(&format!(
    "brewfs_writeback_backpressure_soft_sleep_ops {}\n",
    snapshot.writeback_backpressure_soft_sleep_ops
));
out.push_str(&format!(
    "brewfs_writeback_backpressure_soft_sleep_us {}\n",
    snapshot.writeback_backpressure_soft_sleep_us
));
out.push_str(&format!(
    "brewfs_writeback_backpressure_hard_wait_ops {}\n",
    snapshot.writeback_backpressure_hard_wait_ops
));
out.push_str(&format!(
    "brewfs_writeback_backpressure_hard_wait_us {}\n",
    snapshot.writeback_backpressure_hard_wait_us
));
```

Run:

```bash
cd /mnt/slayerfs/brewfs
cargo test -p brewfs stats --lib
```

Expected:
- The stats tests pass.

- [ ] **Step 3: Record waits in the writer**

In `/mnt/slayerfs/src/vfs/io/writer.rs`, wrap the existing waits in `wait_for_writeback_backpressure`:

```rust
WritebackBackpressureDecision::SoftSleep(duration) => {
    let started = Instant::now();
    tokio::time::sleep(duration).await;
    self.shared
        .stats
        .add_writeback_backpressure_soft_sleep(started.elapsed());
    return Ok(());
}
WritebackBackpressureDecision::Wait => {
    let started = Instant::now();
    self.shared.recent_pending_upload.notify.notified().await;
    self.shared
        .stats
        .add_writeback_backpressure_hard_wait(started.elapsed());
}
```

If `Shared` does not already carry stats, add the smallest local plumbing from the owning VFS stats handle rather than introducing a global singleton.

Run:

```bash
cd /mnt/slayerfs/brewfs
cargo test -p brewfs vfs::io::writer --lib
cargo test -p brewfs stats --lib
cargo fmt --all --check
```

Expected:
- Writer and stats tests pass.
- No formatting diff.

## Task 2: Add Post-Write Drain Accounting To Perf Runner

**Files:**
- Modify: `/mnt/slayerfs/docker/compose-xfstests/run_perf_in_container.sh`

- [ ] **Step 1: Add a report-only drain helper**

Add a helper near `wait_for_fio_prefill_drain()`:

```bash
wait_for_fio_post_write_drain() {
    local tool="$1"
    local timeout="${PERF_FIO_POST_WRITE_DRAIN_TIMEOUT_SECS:-600}"
    local interval="${PERF_FIO_POST_WRITE_DRAIN_INTERVAL_SECS:-2}"
    local threshold="${PERF_FIO_POST_WRITE_DRAIN_PENDING_BYTES:-0}"
    local start now elapsed pending dirty buffer_dirty drain_bytes

    truthy_env "${PERF_FIO_POST_WRITE_DRAIN:-false}" || return 0
    case "$tool" in
        fio-seqwrite*|fio-randwrite*|fio-randrw*|fio-bigwrite*) ;;
        *) return 0 ;;
    esac

    start="$(date +%s)"
    while true; do
        pending="$(numeric_stat_or_zero brewfs_writeback_recent_pending_upload_bytes)"
        dirty="$(numeric_stat_or_zero brewfs_writeback_dirty_bytes)"
        buffer_dirty="$(numeric_stat_or_zero brewfs_buffer_dirty_bytes)"
        drain_bytes="$(max_u64 "$pending" "$dirty")"
        drain_bytes="$(max_u64 "$drain_bytes" "$buffer_dirty")"
        now="$(date +%s)"
        elapsed="$((now - start))"

        if (( drain_bytes <= threshold )); then
            printf '%s\t%s\t%s\t%s\t%s\n' "$tool" "$elapsed" "$pending" "$dirty" "$buffer_dirty" \
                >>"$artifact_dir/post-write-drain.tsv"
            stats_snapshot_after_tool "${tool}-post-write-drained"
            return 0
        fi
        if (( elapsed >= timeout )); then
            printf '%s\ttimeout:%s\t%s\t%s\t%s\n' "$tool" "$elapsed" "$pending" "$dirty" "$buffer_dirty" \
                >>"$artifact_dir/post-write-drain.tsv"
            stats_snapshot_after_tool "${tool}-post-write-drain-timeout"
            return 1
        fi
        sleep "$interval"
    done
}
```

Initialize the TSV in `prepare_artifacts()`:

```bash
printf 'tool\tpost_fio_drain_s\tpending_bytes\tdirty_bytes\tbuffer_dirty_bytes\n' \
    >"$artifact_dir/post-write-drain.tsv"
```

- [ ] **Step 2: Call the helper after fio write profiles**

After `append_fio_log_summary` in `run_fio_profile()`, call:

```bash
wait_for_fio_post_write_drain "$tool"
```

Expected behavior:
- The helper is a no-op unless `PERF_FIO_POST_WRITE_DRAIN=true`.
- The helper runs for write and mixed fio profiles only.

- [ ] **Step 3: Validate shell syntax**

Run:

```bash
cd /mnt/slayerfs/brewfs
bash -n docker/compose-xfstests/run_perf_in_container.sh
```

Expected:
- Exit code `0`.

## Task 3: Add Opt-In Hysteresis Backpressure

**Files:**
- Modify: `/mnt/slayerfs/src/vfs/config.rs`
- Modify: `/mnt/slayerfs/src/vfs/io/writer.rs`

- [ ] **Step 1: Write pure decision tests**

Add tests in `/mnt/slayerfs/src/vfs/io/writer.rs`:

```rust
#[test]
fn test_hysteresis_allows_below_low_watermark() {
    assert!(matches!(
        decide_writeback_backpressure_hysteresis(900, 50, 1024, 2048),
        WritebackBackpressureDecision::Allow
    ));
}

#[test]
fn test_hysteresis_waits_at_or_above_high_watermark() {
    assert!(matches!(
        decide_writeback_backpressure_hysteresis(1900, 200, 1024, 2048),
        WritebackBackpressureDecision::Wait
    ));
}

#[test]
fn test_hysteresis_soft_sleeps_between_low_and_high() {
    assert!(matches!(
        decide_writeback_backpressure_hysteresis(1200, 200, 1024, 2048),
        WritebackBackpressureDecision::SoftSleep(_)
    ));
}
```

Run:

```bash
cd /mnt/slayerfs/brewfs
cargo test -p brewfs test_hysteresis_ --lib
```

Expected:
- The tests fail because the function does not exist.

- [ ] **Step 2: Add config for policy selection**

In `/mnt/slayerfs/src/vfs/config.rs`, add:

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WritebackBackpressurePolicy {
    Current,
    Hysteresis,
}
```

Add a field to `WriteConfig`:

```rust
pub writeback_backpressure_policy: WritebackBackpressurePolicy,
```

Parse:

```rust
let writeback_backpressure_policy =
    match std::env::var("BREWFS_WRITEBACK_BACKPRESSURE_POLICY")
        .ok()
        .as_deref()
        .map(str::trim)
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("hysteresis") => WritebackBackpressurePolicy::Hysteresis,
        _ => WritebackBackpressurePolicy::Current,
    };
```

Expected:
- Default behavior remains current behavior.
- `BREWFS_WRITEBACK_BACKPRESSURE_POLICY=hysteresis` enables the experimental path.

- [ ] **Step 3: Implement the pure hysteresis decision**

In `/mnt/slayerfs/src/vfs/io/writer.rs`, add:

```rust
fn decide_writeback_backpressure_hysteresis(
    pending: u64,
    incoming: u64,
    low_limit: u64,
    high_limit: u64,
) -> WritebackBackpressureDecision {
    if low_limit == 0 {
        return WritebackBackpressureDecision::Allow;
    }
    let high = high_limit.max(low_limit);
    let projected = pending.saturating_add(incoming);
    if projected < low_limit {
        return WritebackBackpressureDecision::Allow;
    }
    if projected >= high {
        return WritebackBackpressureDecision::Wait;
    }
    decide_writeback_backpressure(pending, incoming, low_limit, high)
}
```

Select it inside `wait_for_writeback_backpressure()` only when the policy is `Hysteresis`.

Run:

```bash
cd /mnt/slayerfs/brewfs
cargo test -p brewfs test_hysteresis_ --lib
cargo test -p brewfs test_writeback_backpressure_decision --lib
cargo fmt --all --check
```

Expected:
- Current policy tests remain unchanged.
- Hysteresis tests pass.

## Task 4: A/B Validate The Candidate

**Files:**
- Modify: `/mnt/slayerfs/doc/performance/perf-optimization-roadmap.md`

- [ ] **Step 1: Run baseline with new metrics but current policy**

Run:

```bash
cd /mnt/slayerfs/brewfs
PERF_FIO_DIRECT_MATRIX="0 1" \
PERF_FIO_POST_WRITE_DRAIN=true \
PERF_FIO_PREFILL_DRAIN_TIMEOUT_SECS=600 \
BREWFS_WRITEBACK_BACKPRESSURE_POLICY=current \
BREWFS_COMPRESSION=none \
bash docker/compose-xfstests/run_redis_perf.sh \
  --writeback-throughput-profile \
  --tools "fio-seqwrite fio-randwrite fio-randrw"
```

Expected:
- Record artifact name.
- Metrics include `brewfs_writeback_backpressure_*`.
- `post-write-drain.tsv` exists.

- [ ] **Step 2: Run hysteresis policy**

Run:

```bash
cd /mnt/slayerfs/brewfs
PERF_FIO_DIRECT_MATRIX="0 1" \
PERF_FIO_POST_WRITE_DRAIN=true \
PERF_FIO_PREFILL_DRAIN_TIMEOUT_SECS=600 \
BREWFS_WRITEBACK_BACKPRESSURE_POLICY=hysteresis \
BREWFS_COMPRESSION=none \
bash docker/compose-xfstests/run_redis_perf.sh \
  --writeback-throughput-profile \
  --tools "fio-seqwrite fio-randwrite fio-randrw"
```

Expected:
- Record artifact name.
- Compare read/write BW, p99, p99.9, wall seconds, post-write drain seconds, pending bytes, soft sleep us, and hard wait us.

- [ ] **Step 3: Accept, tune, or revert**

Accept only if:
- `fio-randrw` and `fio-randwrite` meet the acceptance gates.
- `post_fio_drain_s` improves or stays flat.
- Backpressure hard wait time does not explain a new write p99 spike.

Reject and revert the hysteresis code if:
- Throughput is flat but tail latency regresses.
- `direct=0` improves while `direct=1` regresses materially.
- The metrics cannot explain the observed delta.

- [ ] **Step 4: Record the result**

Append a result section to `/mnt/slayerfs/doc/performance/perf-optimization-roadmap.md` with:

```markdown
## Result: Writeback Backpressure Hysteresis Experiment

Date: 2026-06-11

| Variant | Artifact | Direct | Workload | Read BW | Write BW | Read P99 | Write P99 | Write P99.9 | Post Drain | Decision |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| current | `perf-run-1781112437-22526` | 0 | randrw | 127.11 MiB/s | 57.88 MiB/s | 58.982 ms | 22.413 ms | not recorded | not recorded | historical reference only |
| current | `tools/perf/results/20260611-052102` | 0 | randrw | 759.0 MiB/s | 343.6 MiB/s | 37.49 ms | 111.67 ms | 2.84 s | not recorded | same-code tools/perf reference only |

Add one row per new artifact immediately below the two reference rows. The conclusion must state either `keep` or `revert`, name the accepted artifact, and cite the metric that drove the decision.
```

## Task 5: Commit Discipline

**Files:**
- No source edits.

- [ ] **Step 1: Keep effective commits small**

If Task 1 and Task 2 pass tests and shell syntax, commit them separately from the hysteresis behavior:

```bash
cd /mnt/slayerfs/brewfs
git add src/vfs/stats.rs src/vfs/io/writer.rs docker/compose-xfstests/run_perf_in_container.sh
git commit -m "perf: expose writeback backpressure drain metrics"
```

- [ ] **Step 2: Commit hysteresis only after A/B validation**

If Task 4 accepts the hysteresis behavior:

```bash
cd /mnt/slayerfs/brewfs
git add src/vfs/config.rs src/vfs/io/writer.rs doc/performance/perf-optimization-roadmap.md
git commit -m "perf: add opt-in writeback backpressure hysteresis"
git push
```

If Task 4 rejects the behavior:

```bash
cd /mnt/slayerfs/brewfs
git diff -- src/vfs/config.rs src/vfs/io/writer.rs
```

Use a targeted reverse patch to remove the rejected hysteresis code, keep the useful metrics, and record the negative result in `/mnt/slayerfs/doc/performance/perf-optimization-roadmap.md`.

## Self-Review Checklist

- [ ] The plan covers writer, stats, perf runner, and documentation.
- [ ] The plan includes `randrw`, `randwrite`, and direct `0/1`, not only big sequential profiles.
- [ ] The plan avoids known failed directions: range-prefetch pending gate, LZ4 raw-fallback zero-copy, and unsafe metadata batch default.
- [ ] The plan has explicit revert criteria.
- [ ] The plan records results in the roadmap whether the experiment succeeds or fails.
