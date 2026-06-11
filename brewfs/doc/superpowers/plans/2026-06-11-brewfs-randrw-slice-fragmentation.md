# BrewFS Randrw Slice Fragmentation Follow-up Plan

## Goal

Close the BrewFS `fio-randrw` gap by reducing small-slice and small-object amplification without hiding cost in page cache or post-fio drain.

## Evidence

Diagnostic run:

```bash
cd /mnt/slayerfs/brewfs/.worktrees/perf-tune-integration
PERF_TOOLS="fio-randrw" \
PERF_FIO_DIRECT_MATRIX="0 1" \
PERF_FIO_POST_WRITE_DRAIN=true \
PERF_FIO_POST_WRITE_DRAIN_TIMEOUT_SECS=900 \
bash brewfs/docker/compose-xfstests/run_redis_perf.sh \
  --s3 \
  --writeback-throughput-profile \
  --tools "fio-randrw"
```

Artifact:

```text
brewfs/docker/compose-xfstests/artifacts/perf-run-1781197298-12549
```

Summary:

```text
fio-randrw-direct0: pass, wall=88s, post-write-drain=2s
fio-randrw-direct1: pass, wall=71s, post-write-drain=32s
```

Post-drained writer diagnostics:

```text
direct0:
  s3_put_ops=12970, s3_put_mib=5659.0, fuse_write_mib=5996.0, byte_amp=0.944
  upload_batch_ops=12966, avg_upload_batch_mib=0.462, partial_tail_ratio=0.955
  slice_create_ops=12876, slice_reuse_ops=44375
  freeze_size/max_unflushed/explicit_flush/auto=51/11128/130/1567

direct1:
  s3_put_ops=1235, s3_put_mib=4602.3, fuse_write_mib=4156.0, byte_amp=1.107
  upload_batch_ops=1023, avg_upload_batch_mib=4.047, partial_tail_ratio=0.032
  slice_create_ops=872, slice_reuse_ops=3284
  freeze_size/max_unflushed/explicit_flush/auto=7/76/95/694
```

Interpretation:

- `direct0` is dominated by sub-block upload batches, not post-write drain.
- `max_unflushed` freezes are the primary source of direct0 fragmentation: 11,128 freezes versus 51 size freezes.
- 95.5% of direct0 upload batches include a frozen partial tail, so most PUTs are small object fragments.
- `direct1` already has much larger batches and low partial-tail ratio, so any fix must protect direct1 write p99 and drain time.

## Hypothesis

`ChunkHandle::find_slice_or_create` freezes older writable slices as soon as they are more than `MAX_UNFLUSHED_SLICES` away from the newest slice. Under buffered random writeback (`direct=0`), this freezes many sub-block slices before they have a chance to absorb overwrites. Delaying that specific `max_unflushed` freeze for sub-block slices should reduce partial-tail batch count and PUT count.

## Candidate A

Change only the `max_unflushed` freeze path:

- Do not freeze a writable slice for `max_unflushed` unless its logical length is at least one block.
- Leave explicit flush, size/chunk-end freeze, auto-flush, pressure flush, and commit-age safety unchanged.
- Keep the change local and reversible; if memory or direct1 tail regresses, reject it.

Expected movement:

- `fio-randrw-direct0` `writeback_freeze_max_unflushed_ops` decreases.
- `fio-randrw-direct0` `writeback_upload_partial_tail_ops / writeback_upload_batch_ops` decreases.
- `fio-randrw-direct0` `s3_put_ops_per_gib_written` decreases.
- `fio-randrw-direct1` read/write BW does not regress by more than 5%, and write p99/p99.9 does not regress by more than 25%.

## Verification

Targeted code gates:

```bash
CARGO_TARGET_DIR=/mnt/slayerfs/brewfs/target CARGO_INCREMENTAL=0 \
  cargo test -p brewfs --lib 'vfs::io::writer::tests::test_idx_need_upload'

CARGO_TARGET_DIR=/mnt/slayerfs/brewfs/target CARGO_INCREMENTAL=0 \
  cargo test -p brewfs --lib vfs::io::writer::tests::test_dirty_breakdown_reports_slice_lifecycle_metrics

CARGO_TARGET_DIR=/mnt/slayerfs/brewfs/target CARGO_INCREMENTAL=0 \
  cargo test -p brewfs --lib 'vfs::stats::tests::'

CARGO_TARGET_DIR=/mnt/slayerfs/brewfs/target CARGO_INCREMENTAL=0 \
  cargo clippy -p brewfs --lib -- -D warnings

bash brewfs/tools/perf/test_compare_artifacts.sh
bash -n brewfs/docker/compose-xfstests/run_perf_in_container.sh brewfs/docker/compose-xfstests/run_redis_perf.sh
```

Perf gate:

```bash
PERF_TOOLS="fio-randrw" \
PERF_FIO_DIRECT_MATRIX="0 1" \
PERF_FIO_POST_WRITE_DRAIN=true \
PERF_FIO_POST_WRITE_DRAIN_TIMEOUT_SECS=900 \
bash brewfs/docker/compose-xfstests/run_redis_perf.sh \
  --s3 \
  --writeback-throughput-profile \
  --tools "fio-randrw"
```

Compare against `perf-run-1781197298-12549` with:

```bash
python3 brewfs/tools/perf/compare_artifacts.py \
  brewfs/docker/compose-xfstests/artifacts/perf-run-1781197298-12549 \
  <candidate-artifact> \
  --format markdown
```

## Candidate A Result

Artifact:

```text
brewfs/docker/compose-xfstests/artifacts/perf-run-1781198447-23355
```

Post-drained diagnostics:

```text
direct0:
  s3_put_ops=11180, s3_put_mib=6600.2, fuse_write_mib=7000.0, byte_amp=0.943
  upload_batch_ops=11174, avg_upload_batch_mib=0.626, partial_tail_ratio=0.932
  slice_create_ops=11061, slice_reuse_ops=71217
  freeze_size/max_unflushed/explicit_flush/auto=59/120/378/10504

direct1:
  s3_put_ops=1135, s3_put_mib=4239.4, fuse_write_mib=3772.0, byte_amp=1.124
  upload_batch_ops=916, avg_upload_batch_mib=4.092, partial_tail_ratio=0.028
  slice_create_ops=768, slice_reuse_ops=3004
  freeze_size/max_unflushed/explicit_flush/auto=8/81/86/593
```

Movement versus baseline:

- `direct0` max-unflushed freezes dropped from 11,128 to 120 (-98.9%).
- `direct0` S3 PUT count dropped from 12,970 to 11,180 while fio wrote 16.7% more data.
- `direct0` S3 PUTs per GiB written improved from about 2,215 to 1,636 (-26.1%).
- `direct0` average upload batch size improved from 0.462 MiB to 0.626 MiB (+35.5%).
- `direct0` write bandwidth improved from 84.5 MiB/s to 115.2 MiB/s (+36.2%).
- `direct1` write bandwidth moved from 59.5 MiB/s to 56.7 MiB/s (-4.7%), drain improved from 32s to 27s, and write p99 improved from 10.4s to 7.9s.
- `direct1` read bandwidth was -5.6%, just outside the planned 5% guard band, so run a direct1-only guard before accepting the code.

Interpretation:

- Candidate A is directionally effective for buffered randrw: it converts premature `max_unflushed` partial-tail freezes into slice reuse and larger uploads.
- Remaining `direct0` partial-tail ratio is still high at 93.2%, so the next bottleneck is auto/explicit flush of sub-block tails rather than `max_unflushed`.
- Direct I/O must remain protected in follow-up work; the current result is close enough to the guard band to require a direct1 repeat before commit.

Direct1 guard artifact:

```text
brewfs/docker/compose-xfstests/artifacts/perf-run-1781199012-6131
```

Guard result versus baseline:

```text
direct1:
  read_bw_mib_s=190.4 vs 129.4 (+47.1%)
  write_bw_mib_s=85.9 vs 59.5 (+44.5%)
  post_write_drain_s=18 vs 32 (-43.8%)
  s3_put_ops_per_gib_written=258.6 vs 304.3 (-15.0%)
  avg_upload_batch_mib=4.091 vs 4.047 (+1.1%)
  partial_tail_ratio=0.032 vs 0.032
```

Decision:

- Accept Candidate A.
- It improves buffered random write fragmentation and does not show a stable direct I/O regression.
- Next optimization target: reduce `direct0` auto/explicit flush partial-tail uploads while preserving direct1 batch shape and drain behavior.

## Candidate B Result

Hypothesis:

- Make background `flush_once()` non-strict so it does not seal sub-block tails.
- Delay cached-writeback sub-block `auto_flush` until the 5s safety age or memory pressure.
- Keep strict `flush()/fsync()/truncate()/close` semantics unchanged.

Artifact:

```text
brewfs/docker/compose-xfstests/artifacts/perf-run-1781200659-21772
```

Post-drained diagnostics versus Candidate A:

```text
direct0:
  s3_put_ops=8473 vs 11180
  fuse_write_mib=6484.0 vs 7000.0
  s3_put_ops_per_gib_written=1338.1 vs 1635.5
  avg_upload_batch_mib=0.711 vs 0.626
  partial_tail_ratio=0.903 vs 0.932
  freeze_size/max_unflushed/explicit_flush/auto=58/136/372/7630
  write_bw_mib_s=89.8 vs 115.2
  post_write_drain_s=4 vs 5

direct1:
  s3_put_ops_per_gib_written=308.0 vs 308.1
  avg_upload_batch_mib=4.151 vs 4.092
  partial_tail_ratio=0.015 vs 0.028
  write_bw_mib_s=56.0 vs 56.7
  post_write_drain_s=46 vs 27
```

Decision:

- Reject Candidate B.
- It reduced PUT/object amplification, but direct0 write bandwidth regressed by about 22% and direct1 post-write drain regressed by about 70%.
- The rollback keeps Candidate A as the current accepted code.

Interpretation:

- Delaying cached sub-block sealing too broadly shifts cost into dirty/backpressure queues: `writeback_hard_wait_ms` grew from about 9.3M to 28.4M in direct0.
- The next attempt should avoid holding dirty data longer globally. Prefer either per-reason metrics first, or a narrower change that reduces object count without increasing recent-pending hard waits.

## Post-Candidate B Attribution

Candidate C first step is observability, not behavior change.

Artifact after adding partial-tail attribution by top-level freeze reason:

```text
brewfs/docker/compose-xfstests/artifacts/perf-run-1781201913-31389
```

Direct0-only result:

```text
direct0:
  tool_wall_s=81, write_bw_mib_s=88.3, post_write_drain_s=16
  s3_put_ops=12440, fuse_write_mib=6032.0
  put_ops_per_gib_written=2111.8
  avg_upload_batch_mib=0.483
  partial_tail_ratio=0.957
  partial_tail_total=11896
  partial_tail_auto=10988
  partial_tail_explicit_flush=815
  partial_tail_max_unflushed=29
  partial_tail_size=64
  partial_tail_commit_age=0
```

Interpretation:

- Remaining direct0 partial-tail uploads are overwhelmingly from `Auto`: about 92.4% of all partial-tail batches in this run.
- Candidate A fixed premature `MaxUnflushed` sealing, but did not answer which auto trigger still seals the tail: max-age, idle, memory pressure, too-many-slices, or buffer-high.
- Next instrumentation should split `Auto` partial-tail uploads by trigger before another behavior change.

## Rejected Config: Pending Backpressure 2G/4G

Hypothesis:

- Raise `BREWFS_WRITEBACK_RECENT_PENDING_SOFT_BYTES` to 2GiB and hard limit to 4GiB.
- If hard waits are dominating, the higher threshold should reduce backpressure without code changes.

Command:

```bash
BREWFS_WRITEBACK_RECENT_PENDING_SOFT_BYTES=2147483648 \
BREWFS_WRITEBACK_RECENT_PENDING_HARD_BYTES=4294967296 \
PERF_TOOLS="fio-randrw" \
PERF_FIO_DIRECT_MATRIX="0" \
PERF_FIO_POST_WRITE_DRAIN=true \
PERF_FIO_POST_WRITE_DRAIN_TIMEOUT_SECS=900 \
bash brewfs/docker/compose-xfstests/run_redis_perf.sh \
  --s3 \
  --writeback-throughput-profile \
  --tools "fio-randrw"
```

Artifact:

```text
brewfs/docker/compose-xfstests/artifacts/perf-run-1781202337-31807
```

Result versus the direct0 attribution run:

```text
direct0:
  tool_wall_s=143 vs 81
  write_bw_mib_s=69.2 vs 88.3
  put_ops_per_gib_written=2550.5 vs 2111.8
  avg_upload_batch_mib=0.392 vs 0.483
  partial_tail_ratio=0.965 vs 0.957
  partial_tail_auto=21058 vs 10988
  post_write_drain_s=0 vs 16
```

Decision:

- Reject this config for randrw.
- It hides drain and hard-wait symptoms by allowing a larger backlog, but the main fio runtime, write bandwidth, object count, and auto partial-tail count all worsen.

## Candidate C

Goal:

- Split `Auto` partial-tail attribution by trigger:
  `age`, `idle`, `pressure`, `too_many`, `buffer_high`, `flush_duration`, and `unknown`.
- Keep behavior unchanged in this step.
- Use direct0 perf to decide the next behavior candidate:
  if `age` dominates, test a narrow cached sub-block age deferral;
  if `pressure` dominates, test full-block-first pressure flushing;
  if `too_many` dominates, test slice-count policy changes;
  if `buffer_high` dominates, tune buffer-driven flushing.

Verification:

```bash
CARGO_TARGET_DIR=/mnt/slayerfs/brewfs/target CARGO_INCREMENTAL=0 \
  cargo test -p brewfs --lib 'vfs::io::writer::tests::'

CARGO_TARGET_DIR=/mnt/slayerfs/brewfs/target CARGO_INCREMENTAL=0 \
  cargo test -p brewfs --lib 'vfs::stats::tests::'

bash brewfs/tools/perf/test_compare_artifacts.sh
python3 -m py_compile brewfs/tools/perf/compare_artifacts.py
bash -n brewfs/docker/compose-xfstests/run_perf_in_container.sh

CARGO_TARGET_DIR=/mnt/slayerfs/brewfs/target CARGO_INCREMENTAL=0 \
  cargo clippy -p brewfs --lib -- -D warnings
```

Perf gate:

```bash
PERF_TOOLS="fio-randrw" \
PERF_FIO_DIRECT_MATRIX="0" \
PERF_FIO_POST_WRITE_DRAIN=true \
PERF_FIO_POST_WRITE_DRAIN_TIMEOUT_SECS=900 \
bash brewfs/docker/compose-xfstests/run_redis_perf.sh \
  --s3 \
  --writeback-throughput-profile \
  --tools "fio-randrw"
```

## Candidate C Result

Implementation:

- Added auto partial-tail trigger attribution:
  `age`, `idle`, `pressure`, `too_many`, `buffer_high`, `flush_duration`, and `unknown`.
- Kept flush/freeze behavior unchanged.
- Extended `.stats`, perf report, and `compare_artifacts.py` mapping.

Verification completed:

```text
cargo test -p brewfs --lib 'vfs::io::writer::tests::'  # 30 passed
cargo test -p brewfs --lib 'vfs::stats::tests::'        # 3 passed
bash brewfs/tools/perf/test_compare_artifacts.sh        # passed
python3 -m py_compile brewfs/tools/perf/compare_artifacts.py
bash -n brewfs/docker/compose-xfstests/run_perf_in_container.sh
cargo clippy -p brewfs --lib -- -D warnings
```

Direct0 artifact:

```text
brewfs/docker/compose-xfstests/artifacts/perf-run-1781203850-6552
```

Direct0 result versus `perf-run-1781201913-31389`:

```text
tool_wall_s=78 vs 81
write_bw_mib_s=91.4 vs 88.3
post_write_drain_s=2 vs 16
post-drained put_ops_per_gib_written=2021.3 vs 2111.8
avg_upload_batch_mib=0.505 vs 0.483
partial_tail_ratio=0.956 vs 0.957
partial_tail_total=11577
partial_tail_auto=10833
  auto_age=10640
  auto_too_many=193
  auto_idle=0
  auto_pressure=0
  auto_buffer_high=0
  auto_flush_duration=0
  auto_unknown=0
```

Direct1 guard artifact:

```text
brewfs/docker/compose-xfstests/artifacts/perf-run-1781204206-4747
```

Direct1 guard versus `perf-run-1781199012-6131`:

```text
write_bw_mib_s=110.5 vs 85.9
read_bw_mib_s=247.2 vs 190.4
post_write_drain_s=30 vs 18
post-drained put_ops_per_gib_written=259.9 vs 258.6
avg_upload_batch_mib=4.024 vs 4.091
partial_tail_ratio=0.032 vs 0.032
partial_tail_auto=48
  auto_age=48
```

Decision:

- Accept Candidate C as useful observability.
- It does not attempt to improve behavior, but it identifies the next high-confidence bottleneck: `auto_age`.
- Direct0 did not regress in the measured run, and direct1 object amplification stayed effectively flat.
- Direct1 drain was longer than the best guard run, but the run also wrote 27% more data and had substantially higher read/write throughput; treat this as a guard to watch on behavior changes, not a reason to reject instrumentation.

## Candidate D

Goal:

- Reduce direct0 partial-tail object amplification by narrowing the `auto_age` trigger for cached-writeback sub-block slices.
- Preserve strict `flush()/fsync()/close()/truncate()` semantics.
- Preserve direct1 object shape and post-write drain.

Hypothesis:

- For direct0/kernel writeback cache, many tiny slices are being sealed solely because they are older than `auto_flush_max_age` (500ms in the perf profile).
- If a cached-writeback slice is still smaller than one block, `auto_age` should not immediately seal it. It should stay writable until it reaches a full block, hits explicit flush, hits memory pressure, or becomes constrained by too-many-slices cleanup.

Guard rails:

- Do not change explicit flush, fsync, close, truncate, or commit-age safety.
- Do not disable memory-pressure flush.
- Do not defer direct1/non-cached sub-block auto-age slices until a test proves it is safe.
- Reject if direct0 write bandwidth falls by more than 5%, hard-wait time grows materially, direct1 post-drain worsens materially, or direct1 PUT/GiB worsens by more than 5%.

Candidate D test target:

```text
test_auto_flush_defers_cached_sub_block_age_freeze_until_explicit_flush
```

Expected movement:

- direct0 `writeback_upload_partial_tail_auto_age_ops` decreases.
- direct0 `put_ops_per_gib_written` decreases.
- direct0 `writeback_avg_upload_batch_mib` increases.
- direct1 `writeback_partial_tail_ratio` remains about 0.03 and `avg_upload_batch_mib` remains about 4MiB.

## Candidate D Result

Implementation:

- Added `test_auto_flush_defers_cached_sub_block_age_freeze_until_explicit_flush`.
- Changed only the periodic auto-flush age path:
  if a writable slice was created by cached writeback (`creation_unique != 0`) and is still smaller than one block, `auto_age` does not freeze it.
- Kept explicit flush, fsync, close, truncate, pressure, too-many-slices, buffer-high, and non-cached direct write behavior unchanged.

Verification:

```text
cargo test -p brewfs --lib \
  vfs::io::writer::tests::test_auto_flush_defers_cached_sub_block_age_freeze_until_explicit_flush
  # red before implementation, green after implementation

cargo test -p brewfs --lib 'vfs::io::writer::tests::'  # 31 passed
cargo clippy -p brewfs --lib -- -D warnings
```

Direct0 artifact:

```text
brewfs/docker/compose-xfstests/artifacts/perf-run-1781205242-516
```

Direct0 result versus Candidate C (`perf-run-1781203850-6552`):

```text
write_bw_mib_s=99.3 vs 91.4 (+8.7%)
tool_wall_s=83 vs 78 (+6.4%)
post_write_drain_s=6 vs 2
post-drained put_ops_per_gib_written=1928.8 vs 2021.3 (-4.6%)
s3_put_ops=11226 vs 12120 (-7.4%)
avg_upload_batch_mib=0.525 vs 0.505 (+3.9%)
writeback_hard_wait_ms=16.2M vs 24.7M (-34.4%)
partial_tail_total=10712 vs 11577 (-7.5%)
partial_tail_auto_age=46 vs 10640 (-99.6%)
partial_tail_auto_idle=8313 vs 0
partial_tail_auto_too_many=553 vs 193
partial_tail_explicit_flush=1718 vs 669
```

Direct1 guard artifact:

```text
brewfs/docker/compose-xfstests/artifacts/perf-run-1781205600-16115
```

Direct1 guard versus Candidate C (`perf-run-1781204206-4747`):

```text
read_bw_mib_s=232.7 vs 247.2 (-5.9%)
write_bw_mib_s=106.2 vs 110.5 (-3.9%)
post_write_drain_s=22 vs 30
post-drained put_ops_per_gib_written=259.0 vs 259.9 (-0.3%)
avg_upload_batch_mib=4.046 vs 4.024 (+0.5%)
partial_tail_ratio=0.030 vs 0.032
partial_tail_total=47 vs 53
```

Decision:

- Accept Candidate D.
- It reduces direct0 object amplification and hard-wait pressure without a direct1 object-shape regression.
- It is not sufficient as the final optimization because most remaining direct0 auto tails moved from `age` to `idle`. The next candidate should decide whether to defer cached sub-block `idle` too, or use a full-block-first/pressure-aware scan to avoid accumulating too many pending tiny slices.

Next bottleneck:

```text
direct0 remaining partial-tail attribution:
  auto_idle=8313
  explicit_flush=1718
  auto_too_many=553
```

## Rejected Candidate E: Defer Cached Sub-Block Idle

Hypothesis:

- Since Candidate D moved most direct0 auto tails from `age` to `idle`, also deferring cached sub-block `idle` could reduce direct0 object amplification without affecting explicit flush or pressure paths.

Implementation tested:

- Added a unit test proving cached sub-block slices are not auto-idle frozen before explicit flush.
- Changed the periodic auto-flush idle trigger from `idle_time > idle && age > idle` to also require `!cached_sub_block`.

Verification before perf:

```text
cargo test -p brewfs --lib \
  vfs::io::writer::tests::test_auto_flush_defers_cached_sub_block_idle_freeze_until_explicit_flush
  # red before implementation, green after implementation

cargo test -p brewfs --lib 'vfs::io::writer::tests::'  # 32 passed
cargo clippy -p brewfs --lib -- -D warnings
```

Direct0 artifact:

```text
brewfs/docker/compose-xfstests/artifacts/perf-run-1781206434-27374
```

Direct0 result versus Candidate D (`perf-run-1781205242-516`):

```text
tool_wall_s=68 vs 83 (-18.1%)
write_bw_mib_s=94.8 vs 99.3 (-4.6%)
active_plus_drain_s=65.0 vs 66.0 (-1.6%)
post_write_drain_s=2 vs 6
post-drained put_ops_per_gib_written=1077.5 vs 1928.8 (-44.1%)
s3_put_ops=6280 vs 11226 (-44.1%)
avg_upload_batch_mib=0.904 vs 0.525 (+72.2%)
partial_tail_total=5510 vs 10712 (-48.6%)
partial_tail_auto_idle=0 vs 8313 (-100%)
partial_tail_auto_too_many=1695 vs 553
partial_tail_auto_flush_duration=2006 vs 0
```

Direct1 guard artifact:

```text
brewfs/docker/compose-xfstests/artifacts/perf-run-1781206873-2278
```

Direct1 guard versus Candidate D (`perf-run-1781205600-16115`):

```text
read_bw_mib_s=208.2 vs 232.7 (-10.5%)
write_bw_mib_s=95.2 vs 106.2 (-10.4%)
active_plus_drain_s=87.4 vs 82.1 (+6.4%)
post_write_drain_s=24 vs 22
post-drained put_ops_per_gib_written=260.4 vs 259.0 (+0.5%)
avg_upload_batch_mib=4.016 vs 4.046 (-0.7%)
partial_tail_ratio=0.038 vs 0.030
```

Decision:

- Reject and roll back Candidate E.
- The direct0 object-amplification improvement is real, but the direct1 guard regresses read/write throughput by about 10%.
- The next attempt must reduce direct0 `auto_idle` object amplification without globally disabling idle freezes for cached sub-block slices.

Next target:

```text
Prefer a narrower direct0-only batching strategy:
  preserve idle freezing for direct1-like latency-sensitive traffic;
  batch only when the writer has clear evidence of kernel writeback/cache coalescing opportunity;
  keep direct1 throughput regression under 5%;
  keep direct0 post-drained PUT/GiB below Candidate D.
```
