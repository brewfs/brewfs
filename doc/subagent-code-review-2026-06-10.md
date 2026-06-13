# Subagent Code Review - 2026-06-10

## Scope

Reviewed the remaining code left in the earlier subagent worktrees:

- `/mnt/slayerfs/worktrees/brewfs/perf-backpressure`
- `/mnt/slayerfs/worktrees/brewfs/perf-harness`
- `/mnt/slayerfs/worktrees/brewfs/perf-read-cache`

The harness and read-cache worktree changes were already represented by the
mainline commits:

- `87a3fc6 docs: add brewfs perf review plans`
- `9b7c19a perf: align harness and gate range prefetch`

The only unmerged candidate with distinct code behavior was the
`perf-backpressure` change in `src/vfs/io/writer.rs`.

## Candidate Reviewed

The candidate changed `recent_pending_upload` accounting from a per-slice
boolean to exact pending bytes:

- Current mainline behavior: once a committed slice is tracked as pending
  upload, the whole slice allocation contributes to writeback backpressure
  until the slice is fully uploaded.
- Candidate behavior: only the unuploaded gap contributes to pending-upload
  backpressure, and upload progress reconciles the accounted byte count.

The idea is technically sound for reducing over-throttling when a committed
slice is partially uploaded.

## Correctness Check

A red test was added temporarily on current `main`:

```text
cargo test -p brewfs test_recent_pending_accounting_uses_unuploaded_gap --lib
```

Before the candidate implementation, it failed as expected:

```text
left: 8192
right: 4096
```

After applying the candidate implementation, the focused regression tests
passed:

```text
cargo test -p brewfs test_recent_pending_accounting_uses_unuploaded_gap --lib
cargo test -p brewfs test_recent_pending_upload_accounting_tracks_commit_and_upload_completion --lib
cargo test -p brewfs test_writeback_backpressure_waits_for_pending_upload_to_drain --lib
cargo fmt --all --check
```

## Performance Check

The candidate did not show stable performance improvement.

Baseline from the previous mainline run:

```text
compose perf-run-1781112437-22526
randrw direct=0 bs=4m jobs=4 runtime=20s
read 127.11 MiB/s, write 57.88 MiB/s
read P99 58.982 ms, write P99 22.413 ms, write P99.9 17112.760 ms
script wall 125 s
```

Candidate compose samples:

```text
perf-run-1781113694-3442
read 123.98 MiB/s, write 56.70 MiB/s
read P99 56.361 ms, write P99 191.889 ms, write P99.9 17112.760 ms
script wall 128 s

perf-run-1781113906-7662
read 132.23 MiB/s, write 59.76 MiB/s
read P99 55.312 ms, write P99 29.491 ms, write P99.9 240.124 ms
script wall 122 s
```

Candidate `tools/perf` quick samples:

```text
20260610-175501
read 723.2 MiB/s, write 329.1 MiB/s
read P99 28.44 ms, write P99 24.51 ms

20260610-175739
read 391.6 MiB/s, write 179.3 MiB/s
read P99 32.64 ms, write P99 212.86 ms, write P99.9 7.21 s
```

The compose result was mixed: one sample regressed and one sample improved.
The `tools/perf` quick path was not supportive of a stable throughput win.

## Decision

Do not merge the candidate code in its current form.

The accounting change is directionally reasonable, but the current performance
evidence is too noisy and includes a severe `tools/perf` regression sample.
The code was rolled back from the working tree instead of being committed.

## Follow-up

If this idea is revisited, it should be paired with a targeted benchmark that
creates partially uploaded committed slices under sustained write pressure.
The follow-up should test both:

- exact pending-byte accounting
- retuned `writeback_recent_pending_*` limits, since exact accounting makes the
  existing byte limits more permissive

Only merge after both compose randrw and tools/perf randrw show no material
regression across repeated samples.
