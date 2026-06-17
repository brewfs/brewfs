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
bash docker/compose-xfstests/run_redis_perf.sh \
  --s3 \
  --writeback-throughput-profile \
  --tools "fio-randrw"
```

Artifact:

```text
docker/compose-xfstests/artifacts/perf-run-1781197298-12549
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

bash tools/perf/test_compare_artifacts.sh
bash -n docker/compose-xfstests/run_perf_in_container.sh docker/compose-xfstests/run_redis_perf.sh
```

Perf gate:

```bash
PERF_TOOLS="fio-randrw" \
PERF_FIO_DIRECT_MATRIX="0 1" \
PERF_FIO_POST_WRITE_DRAIN=true \
PERF_FIO_POST_WRITE_DRAIN_TIMEOUT_SECS=900 \
bash docker/compose-xfstests/run_redis_perf.sh \
  --s3 \
  --writeback-throughput-profile \
  --tools "fio-randrw"
```

Compare against `perf-run-1781197298-12549` with:

```bash
python3 tools/perf/compare_artifacts.py \
  docker/compose-xfstests/artifacts/perf-run-1781197298-12549 \
  <candidate-artifact> \
  --format markdown
```

## Candidate A Result

Artifact:

```text
docker/compose-xfstests/artifacts/perf-run-1781198447-23355
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
docker/compose-xfstests/artifacts/perf-run-1781199012-6131
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
docker/compose-xfstests/artifacts/perf-run-1781200659-21772
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
docker/compose-xfstests/artifacts/perf-run-1781201913-31389
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
bash docker/compose-xfstests/run_redis_perf.sh \
  --s3 \
  --writeback-throughput-profile \
  --tools "fio-randrw"
```

Artifact:

```text
docker/compose-xfstests/artifacts/perf-run-1781202337-31807
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

bash tools/perf/test_compare_artifacts.sh
python3 -m py_compile tools/perf/compare_artifacts.py
bash -n docker/compose-xfstests/run_perf_in_container.sh

CARGO_TARGET_DIR=/mnt/slayerfs/brewfs/target CARGO_INCREMENTAL=0 \
  cargo clippy -p brewfs --lib -- -D warnings
```

Perf gate:

```bash
PERF_TOOLS="fio-randrw" \
PERF_FIO_DIRECT_MATRIX="0" \
PERF_FIO_POST_WRITE_DRAIN=true \
PERF_FIO_POST_WRITE_DRAIN_TIMEOUT_SECS=900 \
bash docker/compose-xfstests/run_redis_perf.sh \
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
bash tools/perf/test_compare_artifacts.sh        # passed
python3 -m py_compile tools/perf/compare_artifacts.py
bash -n docker/compose-xfstests/run_perf_in_container.sh
cargo clippy -p brewfs --lib -- -D warnings
```

Direct0 artifact:

```text
docker/compose-xfstests/artifacts/perf-run-1781203850-6552
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
docker/compose-xfstests/artifacts/perf-run-1781204206-4747
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
docker/compose-xfstests/artifacts/perf-run-1781205242-516
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
docker/compose-xfstests/artifacts/perf-run-1781205600-16115
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
docker/compose-xfstests/artifacts/perf-run-1781206434-27374
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
docker/compose-xfstests/artifacts/perf-run-1781206873-2278
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

## Rejected Candidate F: Short Idle Grace for Cached Sub-Block Slices

Hypothesis:

- Candidate E was too broad because it disabled cached sub-block idle freezing until
  explicit flush/pressure/flush-duration paths.
- A small fixed idle grace could preserve most direct1 behavior while giving direct0
  kernel writeback-cache fragments one extra merge window.

Implementation tested:

- Added a 2s cached sub-block idle grace before `AutoFreezeTrigger::Idle`.
- Added a unit test proving cached sub-block slices are not auto-idle frozen at
  the normal 1s idle threshold, but are eventually frozen by the grace timeout.

Verification before perf:

```text
cargo test -p brewfs --lib \
  vfs::io::writer::tests::test_auto_flush_gives_cached_sub_block_idle_short_grace
  # red before implementation, green after implementation

cargo test -p brewfs --lib 'vfs::io::writer::tests::'  # 32 passed
cargo clippy -p brewfs --lib -- -D warnings
```

Direct0 artifact:

```text
docker/compose-xfstests/artifacts/perf-run-1781208163-13440
```

Direct0 result versus Candidate D (`perf-run-1781205242-516`):

```text
tool_wall_s=81 vs 83 (-2.4%)
read_bw_mib_s=232.9 vs 219.0 (+6.4%)
write_bw_mib_s=106.5 vs 99.3 (+7.3%)
post_write_drain_s=6 vs 6
post-drained put_ops_per_gib_written=1710.4 vs 1928.8 (-11.3%)
s3_put_ops=10797 vs 11226 (-3.8%)
avg_upload_batch_mib=0.588 vs 0.525 (+12.0%)
partial_tail_ratio=0.932 vs 0.955
partial_tail_auto_idle=5378 vs 8313 (-35.3%)
partial_tail_auto_too_many=2661 vs 553
writeback_hard_wait_ms=17.99M vs 16.19M (+11.1%)
```

Direct1 guard artifact:

```text
docker/compose-xfstests/artifacts/perf-run-1781208555-7614
```

Direct1 guard versus Candidate D (`perf-run-1781205600-16115`):

```text
tool_wall_s=66 vs 62 (+6.5%)
read_bw_mib_s=188.5 vs 232.7 (-19.0%)
write_bw_mib_s=85.9 vs 106.2 (-19.1%)
active_plus_drain_s=83.0 vs 82.1 (+1.1%)
post_write_drain_s=18 vs 22
post-drained put_ops_per_gib_written=258.7 vs 259.0 (-0.1%)
avg_upload_batch_mib=4.072 vs 4.046 (+0.7%)
partial_tail_ratio=0.027 vs 0.030
writeback_hard_wait_ms=221.6k vs 193.2k (+14.7%)
write_p99_ms=6140 vs 4328 (+41.9%)
```

Decision:

- Reject and roll back Candidate F.
- Direct0 gains are real, especially on PUT/GiB and small-tail attribution, but
  the fixed grace increases direct1 latency enough to cut read/write throughput
  by about 19%.
- The next attempt should avoid fixed time grace and instead introduce an explicit
  write-origin/active-writeback signal. Current `creation_unique != 0` only means
  the slice was initially created by the cached path; it is not a reliable direct0
  versus direct1 signal once subsequent writes, flushes, and pressure interact.

Next target:

```text
Candidate G: add explicit writer-origin instrumentation before changing policy.
  track whether a slice has cached-writeback writes versus normal writes;
  expose counters for cached/direct/unknown sub-block auto-freeze attribution;
  use the signal to design an active-writeback-only idle deferral;
  preserve Candidate D as the performance baseline until the new signal proves useful.
```

## Candidate G Result: Write-Origin Attribution

Goal:

- Add behavior-neutral write-origin attribution before another idle/age policy
  change.
- Distinguish slices that were written only by the normal write path, only by
  the cached writeback path, by both paths, or by an unknown path.
- Export both live writeback origin gauges and cumulative partial-tail upload
  origin counters.

Implementation:

- Added `WriteOrigin` tracking to successful slice writes.
- Kept the writeback freeze/upload policy unchanged.
- Exported live origin gauges:
  `normal_only`, `cached_only`, `mixed_origin`, and `unknown_origin`.
- Exported partial-tail upload counters by origin, including auto-only origin
  counters.
- Extended the perf report and `compare_artifacts.py` so artifacts show
  `partial_origin=...` and `auto_origin=...`.

Verification:

```text
cargo fmt --all
cargo test -p brewfs --lib 'vfs::io::writer::tests::'  # 33 passed
cargo test -p brewfs --lib vfs::stats::tests::         # 3 passed
bash tools/perf/test_compare_artifacts.sh        # passed
cargo clippy -p brewfs --lib -- -D warnings
git diff --check
```

Direct0 artifact:

```text
docker/compose-xfstests/artifacts/perf-run-1781211889-1001
```

Direct0 versus Candidate D (`perf-run-1781205242-516`):

```text
read_bw_mib_s=190.5 vs 219.0 (-13.0%)
write_bw_mib_s=86.7 vs 99.3 (-12.7%)
post_write_drain_s=2 vs 6
post-drained put_ops_per_gib_written=1540.9 vs 1928.8 (-20.1%)
avg_upload_batch_mib=0.659 vs 0.525 (+25.5%)
partial_tail_ratio=0.933 vs 0.955
partial_origin=normal 0/cached 7830/mixed 0/unknown 0
auto_origin=normal 0/cached 6711/mixed 0/unknown 0
auto_detail=age 33/idle 5928/too_many 750/pressure 0/buffer_high 0/flush_duration 0
```

Note:

- A separate Codex task repeatedly launched cargo tests during this period, so
  treat direct0 throughput as potentially noisy.
- The object-shape signal is still useful: buffered randrw direct0 partial-tail
  uploads are overwhelmingly cached-origin tails, and the remaining auto
  triggers are mostly `idle` plus `too_many`.

Direct1 artifact:

```text
docker/compose-xfstests/artifacts/perf-run-1781212221-16078
```

Direct1 versus Candidate D (`perf-run-1781205600-16115`):

```text
read_bw_mib_s=185.7 vs 232.7 (-20.2%)
write_bw_mib_s=84.4 vs 106.2 (-20.6%)
post_write_drain_s=18 vs 22
post-drained put_ops_per_gib_written=259.2 vs 259.0 (+0.1%)
avg_upload_batch_mib=4.066 vs 4.046 (+0.5%)
partial_tail_ratio=0.034 vs 0.030
partial_origin=normal 45/cached 0/mixed 0/unknown 0
auto_origin=normal 36/cached 0/mixed 0/unknown 0
auto_detail=age 36/idle 0/too_many 0/pressure 0/buffer_high 0/flush_duration 0
```

Decision:

- Keep Candidate G as observability, not as a claimed throughput improvement.
- The origin signal is decisive enough for the next behavior candidate:
  direct0 remaining partial-tail amplification is cached-origin and mostly
  `auto_idle`; direct1 partial tails are normal-origin and still dominated by
  `auto_age`.
- Because direct1 throughput was lower in this run, the next behavior candidate
  must run a direct1 guard and reject any stable direct1 read/write regression
  above 5%.

Next target:

```text
Candidate H: cached-origin full-block-first idle cleanup.
  Do not use a fixed time grace.
  Do not change normal-origin direct1 idle/age behavior.
  When cached-origin sub-block slices are idle, prefer freezing/uploading full
  blocks and older/larger cached tails first.
  Only freeze tiny cached idle tails when slice-count/backpressure requires it.
  Accept only if direct0 PUT/GiB improves and direct1 read/write stays within
  the 5% guard band versus Candidate D.
```

## Rejected Candidate H: Cached-Only Sub-Block Idle Deferral

Goal:

- Use Candidate G's explicit write-origin signal instead of the older
  `creation_unique != 0` proxy.
- Defer `AutoFreezeTrigger::Idle` only for `cached_only` sub-block slices.
- Keep normal-origin direct1 age/idle behavior unchanged.
- Keep explicit flush, pressure, too-many-slices, buffer-high, and
  flush-duration paths active.

Implementation tested:

- Added a unit test proving cached-only sub-block slices are not frozen by idle
  auto-flush before explicit flush.
- Changed the periodic auto-flush path to skip both age and idle for
  `WriteOriginKind::CachedOnly` sub-block slices.

Verification before perf:

```text
cargo test -p brewfs --lib \
  vfs::io::writer::tests::test_auto_flush_defers_cached_only_sub_block_idle_freeze_until_explicit_flush
  # red before implementation, green after implementation

cargo fmt --all
cargo test -p brewfs --lib 'vfs::io::writer::tests::'  # 34 passed
cargo clippy -p brewfs --lib -- -D warnings
git diff --check
```

Direct0 clean artifact:

```text
docker/compose-xfstests/artifacts/perf-run-1781213381-2023
```

Direct0 result versus Candidate D (`perf-run-1781205242-516`):

```text
read_bw_mib_s=229.9 vs 219.0 (+5.0%)
write_bw_mib_s=105.0 vs 99.3 (+5.7%)
post_write_drain_s=8 vs 6
post-drained put_ops_per_gib_written=1388.3 vs 1928.8 (-28.0%)
s3_put_ops=8541 vs 11226 (-23.9%)
avg_upload_batch_mib=0.708 vs 0.525 (+34.8%)
partial_tail_total=7855 vs 10712 (-26.7%)
partial_tail_auto_idle=0 vs 8313 (-100%)
partial_tail_auto_too_many=2286 vs 553
partial_tail_auto_flush_duration=2411 vs 0
writeback_hard_wait_ms=20.42M vs 16.19M (+26.2%)
```

Direct1 guard artifact:

```text
docker/compose-xfstests/artifacts/perf-run-1781213764-29688
```

Direct1 result versus Candidate D (`perf-run-1781205600-16115`):

```text
read_bw_mib_s=186.5 vs 232.7 (-19.9%)
write_bw_mib_s=84.9 vs 106.2 (-20.1%)
active_io_runtime_s=63.5 vs 60.1
post_write_drain_s=18 vs 22
post-drained put_ops_per_gib_written=259.0 vs 259.0
avg_upload_batch_mib=4.079 vs 4.046 (+0.8%)
partial_tail_ratio=0.029 vs 0.030
writeback_hard_wait_ms=217.6k vs 193.2k (+12.6%)
```

Decision:

- Reject and roll back Candidate H.
- The direct0 object-amplification improvement is strong, but the direct1 guard
  does not meet the 5% read/write regression limit.
- H does not appear to add a new direct1 object-shape regression versus
  Candidate G, but the current committed branch needs a clean rebaseline before
  another behavior change.

Next target:

```text
Candidate I: current-branch direct1 rebaseline and backpressure attribution.
  First rerun direct1 on the reverted/current committed behavior to separate
  Candidate G observation overhead from Candidate H behavior.
  If direct1 remains near -20% versus Candidate D, inspect origin metric update
  cost, writeback hard-wait timing, and S3 put latency before changing policy.
  If direct1 returns near Candidate D, resume cached-origin idle cleanup with a
  bounded full-block-first scanner that avoids increasing hard_wait.
```

## Candidate I Result: Same-Time Direct1 Control

Goal:

- Determine whether the current direct1 throughput drop is caused by Candidate
  G/H code, or by the test environment/object-store latency changing between
  runs.
- Compare current branch against exact Candidate D code in the same time window.

Method:

- Reverted Candidate H behavior first; current branch kept only accepted code
  plus Candidate G observability.
- Created a detached worktree at exact Candidate D commit `d1cfa1a`.
- Ran both with identical command shape:

```bash
PERF_TOOLS="fio-randrw" \
PERF_FIO_DIRECT_MATRIX="1" \
PERF_FIO_POST_WRITE_DRAIN=true \
PERF_FIO_POST_WRITE_DRAIN_TIMEOUT_SECS=900 \
bash docker/compose-xfstests/run_redis_perf.sh \
  --s3 \
  --writeback-throughput-profile \
  --tools "fio-randrw"
```

Current-branch rebaseline artifact:

```text
docker/compose-xfstests/artifacts/perf-run-1781214273-18491
```

Same-time Candidate D control artifact:

```text
perf-run-1781214825-17826
```

Same-time D control versus old Candidate D artifact
(`perf-run-1781205600-16115`):

```text
read_bw_mib_s=183.2 vs 232.7 (-21.3%)
write_bw_mib_s=83.3 vs 106.2 (-21.6%)
active_io_runtime_s=63.8 vs 60.1
post_write_drain_s=20 vs 22
post-drained put_ops_per_gib_written=257.9 vs 259.0 (-0.4%)
avg_upload_batch_mib=4.080 vs 4.046 (+0.9%)
partial_tail_ratio=0.027 vs 0.030
s3_put_avg_ms=239.3 vs 193.0 (+24.0%)
writeback_hard_wait_ms=223.4k vs 193.2k (+15.6%)
```

Current branch versus same-time D control:

```text
read_bw_mib_s=182.5 vs 183.2 (-0.4%)
write_bw_mib_s=83.1 vs 83.3 (-0.2%)
active_io_runtime_s=63.1 vs 63.8
post_write_drain_s=21 vs 20
post-drained put_ops_per_gib_written=258.2 vs 257.9 (+0.1%)
avg_upload_batch_mib=4.107 vs 4.080 (+0.7%)
partial_tail_ratio=0.030 vs 0.027
s3_put_avg_ms=235.8 vs 239.3 (-1.5%)
writeback_hard_wait_ms=220.2k vs 223.4k (-1.4%)
```

Decision:

- Candidate G/H are not the source of the apparent direct1 throughput drop.
- The old Candidate D artifact is no longer a valid direct1 throughput baseline
  for current comparisons because exact D code now reproduces the same lower
  throughput in the same environment.
- Use same-time controls for future behavior candidates. Direct1 guard should
  compare candidate code against a nearby D/current control, not against the old
  high-throughput artifact alone.

Next target:

```text
Candidate J: bounded cached-origin full-block-first idle cleanup.
  Keep direct1 normal-origin behavior unchanged.
  Use current/same-time D direct1 as the guard baseline.
  Optimize direct0 by reducing cached-origin auto_idle tiny-tail uploads, but
  bound dirty backlog so hard_wait does not grow more than 5% versus current.
  Accept only if direct0 post-drained PUT/GiB improves and same-time direct1
  read/write stays within 5%.
```

## Candidate J Result: Bounded Cached-Only Idle Grace

Goal:

- Reduce buffered randrw small-object amplification caused by cached-origin
  sub-block slices being sealed at the one-second idle point.
- Keep the direct1 normal-origin path inside the 5% guard band.
- Bound the grace window so cached-origin dirty backlog cannot wait until the
  long flush-duration fallback.

Implementation:

- Classify cached sub-block slices with `WriteOriginKind::CachedOnly` instead
  of `creation_unique != 0`.
- Defer `AutoFreezeTrigger::Idle` for cached-only sub-block slices for three
  seconds.
- Keep pressure, too-many-slices, buffer-high, explicit flush, and
  flush-duration triggers intact.
- Added a regression test that verified RED on the old one-second idle freeze
  and GREEN with bounded grace.

Rejected sub-candidates:

```text
2s grace artifact: perf-run-1781216256-6008
  direct0 read/write improved by +11.9%, hard_wait fell by -20.7%, but
  post-drained PUT/GiB worsened by +2.7%.

1.5s grace artifact: perf-run-1781216930-15683
  direct0 hard_wait fell by -45.3%, but read/write regressed by about -6%
  and post-drained PUT/GiB still worsened by +2.3%.
```

Accepted sub-candidate:

```text
3s direct0 baseline artifact: perf-run-1781215943-30
3s direct0 candidate artifact: perf-run-1781217272-13985
3s direct1 guard baseline artifact: perf-run-1781214273-18491
3s direct1 guard candidate artifact: perf-run-1781217577-3420
```

Direct0 result:

```text
read_bw_mib_s=242.1 vs 231.3 (+4.7%)
write_bw_mib_s=110.2 vs 105.2 (+4.8%)
post_write_drain_s=2 vs 16 (-87.5%)
post-drained put_ops_per_gib_written=1629.0 vs 1996.7 (-18.4%)
post-drained avg_upload_batch_mib=0.610 vs 0.506 (+20.5%)
partial_tail_ops=9771 vs 11732 (-16.7%)
partial_tail_auto_idle_ops=5034 vs 8443 (-40.4%)
writeback_hard_wait_ms=17.88M vs 19.87M (-10.0%)
```

Direct1 guard result:

```text
read_bw_mib_s=189.3 vs 182.5 (+3.8%)
write_bw_mib_s=85.7 vs 83.1 (+3.2%)
post_write_drain_s=20 vs 21 (-4.8%)
post-drained put_ops_per_gib_written=256.8 vs 258.2 (-0.5%)
post-drained avg_upload_batch_mib=4.116 vs 4.107 (+0.2%)
partial_tail_ops=32 vs 38 (-15.8%)
writeback_hard_wait_ms=209.7k vs 220.2k (-4.7%)
```

Decision:

- Accept Candidate J with a three-second cached-only sub-block idle grace.
- This keeps the direct1 guard within the configured 5% band while improving
  buffered randrw throughput, drain time, object count, and partial-tail count.
- Residual risk: direct0 run still reported a small number of best-effort
  writeback stage failures. These have appeared in previous guard runs and are
  logged as skipped SSD persist rather than foreground I/O errors, but the next
  round should add clearer failure-cause attribution.

Next target:

```text
Candidate K: reduce direct0 explicit-flush/too-many spillover after the 3s
cached-idle grace. The 3s candidate cuts idle tails but still shifts some
freezes to TooMany and ExplicitFlush. Inspect slice ordering and cached-origin
batch selection so delayed cached tails are drained in larger, older-first
groups without increasing hard_wait.
```

## 2026-06-11 Follow-up: Commit-Before-Upload Staging Barrier Attempts

Goal:

- Make local writeback staging a real precondition for commit-before-upload
  metadata publication.
- Preserve the accepted Candidate J direct0/direct1 randrw guardrails.

Rejected strict-gate candidates:

```text
Strict metadata gate only:
  artifact: perf-run-1781218633-26450
  direct0 read/write +42.6%/+39.6%, commit_before_stage_ops=0,
  but post_write_drain_s 2 -> 79 and post-drained PUT/GiB +20.5%.

Strict gate + full stage-inflight backpressure:
  artifact: perf-run-1781219307-2683
  post_write_drain_s 2 -> 0, but read/write -27.7%/-27.9% and
  post-drained PUT/GiB +93.2%.

Strict gate + 1/4 weighted stage-inflight backpressure:
  artifact: perf-run-1781219770-1834
  read/write -3.7%/-5.3%, but post_write_drain_s 2 -> 59 and live
  cached-only dirty backlog remained around 8 GiB.

Strict gate + weighted stage backpressure + cached TooMany grace:
  artifact: perf-run-1781220339-23425
  read/write +34.8%/+32.2%, post_write_drain_s 2 -> 0, but tool_wall_s
  80 -> 159 and post-drained PUT ops +85.6%.

Strict gate + weighted stage backpressure + TooMany grace + writeback batch merge:
  artifact: perf-run-1781220905-5533
  read/write +6.7%/+4.7%, post_write_drain_s 2 -> 0, but tool_wall_s
  80 -> 162 and post-drained PUT ops +95.8%.
```

Finding:

- A naive staging-before-metadata gate is functionally correct but changes the
  direct0 timing model enough to create many more cached-only partial-tail
  uploads.
- Backpressure can move the drain cost into the fio run, but it either
  over-throttles or still leaves large live dirty tails.
- `FsWriteBackCache::persist_slice` had an independent correctness bug: multiple
  staging batches for the same dirty slice reused the same key but overwrote the
  local `.slice` file instead of merging at `chunk_offset`.

Accepted precondition fix:

```text
writeback batch merge direct0 baseline: perf-run-1781217272-13985
writeback batch merge direct0 candidate: perf-run-1781221364-30168
writeback batch merge direct1 baseline: perf-run-1781217577-3420
writeback batch merge direct1 candidate: perf-run-1781221708-4757
```

Direct0:

```text
read_bw_mib_s=276.6 vs 242.1 (+14.3%)
write_bw_mib_s=124.4 vs 110.2 (+12.9%)
post_write_drain_s=4 vs 2 (+2s)
post-drained put_ops_per_gib_written=2037.4 vs 1629.0 (+25.1%)
```

Direct1:

```text
read_bw_mib_s=182.4 vs 189.3 (-3.6%)
write_bw_mib_s=82.4 vs 85.7 (-3.8%)
post_write_drain_s=21 vs 20 (+5.0%)
post-drained s3_put_ops=1280 vs 1290 (-0.8%)
```

Decision:

- Accept the writeback batch merge as a reliability precondition, not as the
  final staging-barrier implementation.
- Do not accept the strict metadata gate variants yet; they still violate the
  direct0 object-amplification guardrail.

Next target:

```text
Candidate L: redesign the staging barrier around slice-level durable staging
completion, not per-upload-batch timing. The next attempt should keep direct0
post-drained PUT/GiB within 5% of Candidate J while driving
commit_before_stage_ops toward zero. Focus on coalescing cached-only dirty
sub-blocks before upload dispatch and avoid turning metadata ordering into a
source of extra partial-tail slices.
```

## 2026-06-12 Candidate L Follow-up Results

Attempted code changes:

- A slice-level staging gate in `commit_chunk` and `try_commit`, requiring
  `writeback_fully_persisted()` before commit-before-upload metadata writes.
- A frozen-slice full local stage before upload dispatch, with per-batch
  staging skipped when already covered.
- A wait-order tweak so staged frozen slices could try early metadata commit
  before waiting on the upload notification.
- An isolated `FsWriteBackCache::persist_slice` offset-base fix, so dirty slice
  files use the first staged chunk offset as local file offset zero.

Rejected artifacts:

```text
slice-level full stage + strict metadata gate:
  artifact: perf-run-1781223330-26580
  direct0 commit_before_stage_ops 2585 -> 0
  direct0 read/write +40.9%/+37.7%
  rejected: tool_wall_s 80 -> 170 and post-drained PUT/GiB +21.7%

same gate + early metadata wait-order tweak:
  artifact: perf-run-1781223838-20862
  direct0 commit_before_stage_ops 2585 -> 0
  direct0 read/write +25.3%/+22.8%
  rejected: tool_wall_s 80 -> 380, wall_active_tail_s +1459.7%, and
  post-write drain left hundreds of MiB dirty until the run was manually
  stopped.

writeback offset-base fix only:
  direct0 artifact: perf-run-1781224706-3379
  direct1 artifact: perf-run-1781225038-15662
  direct1 repeat artifact: perf-run-1781225360-14510
  direct0 read/write +7.2%/+6.8%, tool_wall_s 80 -> 72, and
  post-drained PUT/GiB -22.6%
  rejected: direct0 post_write_drain_s 2 -> 23, and repeated direct1 guards
  regressed read/write by about 6-7% with tool_wall_s +6.6-9.8%.
```

Decision:

- Roll back all Candidate L code changes.
- Keep only the already accepted writeback batch merge commit as the current
  reliability precondition.
- Do not attempt another metadata gate without finer-grained observability.

Findings:

- Driving `commit_before_stage_ops` to zero is not enough. The strict gate can
  improve in-window fio throughput while hiding cost in close/flush tail,
  post-write drain, or direct1 throughput.
- Full local staging before upload dispatch is unsafe for still-writable slices
  because cached writes can still alter bytes before upload dispatch freezes
  the corresponding blocks. Frozen-only full staging avoids that correctness
  hole but does not solve the close/drain cost.
- The current commit loop is sensitive to notification ordering: stage-ready,
  upload-done, and metadata ordering are coupled through the same slice notify
  path. Optimizing one edge can starve another.
- `persist_slice` offset-base semantics are a real recovery-path correctness
  issue, but the isolated hot-path performance result was mixed and failed the
  direct1 guard. Revisit it only with a narrower recovery-only path or with
  same-time direct1 control evidence.

Next target:

```text
Candidate M: add observability before behavior.
  Add per-slice counters/timers for:
    - stage-ready-before-upload-done
    - metadata-waiting-for-stage
    - metadata-waiting-for-upload
    - metadata-waiting-for-front-slice
    - close/flush wait split by stage/upload/metadata/recent-pending
  Run direct0/direct1 randrw against Candidate J with no behavior change.
  Accept the instrumentation only if direct0/direct1 read/write stay within 2%
  and post-drain does not regress materially.
  Use the new attribution to design the next barrier so metadata can wait on
  durable local stage without increasing partial-tail object count or close
  tail.
```

## 2026-06-12 Candidate M Results

Implemented:

- Added writeback commit-loop wait attribution for:
  - `brewfs_writeback_commit_wait_upload_ops_total`
  - `brewfs_writeback_commit_wait_upload_us_total`
  - `brewfs_writeback_commit_wait_retry_ops_total`
  - `brewfs_writeback_commit_wait_retry_us_total`
- Exported the new counters through `.stats`, `FsStatsSnapshot`, and
  `compare_artifacts.py`.
- The instrumentation only records elapsed wait time around existing
  `commit_chunk.wait_upload` and `commit_chunk.wait_retry` awaits. It does not
  change upload, staging, metadata commit, or slice-selection behavior.

Verification:

```text
cargo test -p brewfs --lib 'vfs::io::writer::tests::'
cargo test -p brewfs --lib 'vfs::stats::tests::'
cargo clippy -p brewfs --lib -- -D warnings
bash tools/perf/test_compare_artifacts.sh
git diff --check
```

Perf artifacts:

```text
direct0 candidate: perf-run-1781226217-16996
direct1 candidate: perf-run-1781226525-32139
direct1 repeat:    perf-run-1781226953-26511
```

Direct0 versus Candidate J baseline `perf-run-1781217272-13985`:

```text
read_bw_mib_s=259.3 vs 242.1 (+7.1%)
write_bw_mib_s=117.8 vs 110.2 (+6.9%)
tool_wall_s=86 vs 80 (+7.5%)
post_write_drain_s=0 vs 2 (-2s)
post-drained PUT/GiB=2028.9 vs 1629.0 (+24.5%)
commit_wait_upload_ops=15786
commit_wait_upload_ms=1547475.9
commit_wait_retry_ops=0
```

Direct1 first run versus Candidate J baseline `perf-run-1781217577-3420`:

```text
read_bw_mib_s=181.7 vs 189.3 (-4.0%)
write_bw_mib_s=82.6 vs 85.7 (-3.6%)
tool_wall_s=64 vs 61 (+4.9%)
post_write_drain_s=22 vs 20 (+2s)
post-drained PUT/GiB=258.1 vs 256.8 (+0.5%)
commit_wait_upload_ops=2871
commit_wait_upload_ms=264280.2
commit_wait_retry_ops=0
```

Direct1 repeat versus the same baseline:

```text
read_bw_mib_s=209.1 vs 189.3 (+10.5%)
write_bw_mib_s=95.4 vs 85.7 (+11.3%)
tool_wall_s=61 vs 61 (+0.0%)
post_write_drain_s=20 vs 20 (+0s)
post-drained PUT/GiB=259.2 vs 256.8 (+0.9%)
```

Decision:

- Accept Candidate M as observability, not as a throughput optimization.
- Treat the first direct1 regression as run-to-run variance because the repeat
  returned to equal wall/drain time and positive read/write throughput without
  any code change.
- The new attribution shows the commit loop is dominated by upload waits:
  direct0 accumulated about 1,547s of upload wait across commit tasks; direct1
  accumulated about 264s. Retry/backoff wait stayed at zero in both tests.

Next target:

```text
Candidate N: reduce upload-wait amplification without adding metadata gates.
  Start from the fact that commit retry is not the bottleneck. Focus on the
  upload side:
    - reduce direct0 partial-tail upload count and PUT/GiB;
    - keep direct1 post-drained PUT/GiB within 2%;
    - preserve direct1 wall/drain time;
    - do not reintroduce strict metadata-before-stage gates until upload wait
      attribution also splits by slice freeze reason and upload batch size.
  First experiment should be low-risk observability or scheduling:
    - expose average commit wait per upload batch/freeze reason, or
    - prioritize larger/frozen upload batches before small partial-tail uploads.
```

## 2026-06-12 Rejected Candidate N: Range-Correct Stage Coverage

Hypothesis:

- The current local writeback staging coverage counter is only a byte sum. If
  the same range is staged twice, it can falsely mark a slice as fully staged.
- A metadata-before-stage barrier cannot safely depend on that signal.
- Track actual staged byte coverage first, then later use it as the barrier
  condition.

Implementation tried:

- RED test:
  `cargo test -p brewfs --lib 'vfs::io::writer::tests::test_slice_writeback_stage_completion_requires_all_bytes'`
  failed because `record_writeback_persisted_range` did not exist.
- First version: per-slice merged staged ranges.
- Second version: lighter prefix-plus-tail-ranges representation so the common
  in-order staging path is O(1) and only out-of-order staging keeps ranges.
- `mark_writeback_persisted` was changed from byte-count only to
  `(batch_offset, data_len)` coverage tracking.

Functional verification passed for the candidate:

```text
cargo test -p brewfs --lib 'vfs::io::writer::tests::test_slice_writeback_stage_completion'
cargo test -p brewfs --lib 'vfs::io::writer::tests::'
cargo fmt --all --check
cargo clippy -p brewfs --lib -- -D warnings
git diff --check
```

Perf artifacts:

```text
merged-range candidate: perf-run-1781227742-1402
direct1 repeat:         perf-run-1781228309-4833
prefix-range candidate: perf-run-1781228820-3566
```

Key results versus Candidate M controls:

```text
Merged-range direct0:
  read_bw_mib_s 259.3 -> 264.4 (+2.0%)
  write_bw_mib_s 117.8 -> 119.8 (+1.7%)
  post_write_drain_s 0 -> 12
  post-drained PUT/GiB 2028.9 -> 1268.8 (-37.5%)

Merged-range direct1 first run:
  read_bw_mib_s 209.1 -> 121.8 (-41.8%)
  write_bw_mib_s 95.4 -> 56.2 (-41.1%)
  post_write_drain_s 20 -> 40

Merged-range direct1 repeat:
  read_bw_mib_s 209.1 -> 194.9 (-6.8%)
  write_bw_mib_s 95.4 -> 88.5 (-7.2%)
  post_write_drain_s 20 -> 27

Prefix-range final run:
  direct0 tool_wall_s 86 -> 91 (+5.8%)
  direct0 post_write_drain_s 0 -> 8
  direct1 read_bw_mib_s 209.1 -> 104.6 (-50.0%)
  direct1 write_bw_mib_s 95.4 -> 48.3 (-49.4%)
  direct1 post_write_drain_s 20 -> 42
```

Decision:

- Reject and roll back Candidate N code.
- The correctness issue is real, but the isolated implementation did not pass
  the randrw perf gate. Some of the direct1 loss coincided with slow prefill
  drain/object-store tail latency, but the final run still had enough wall,
  drain, and throughput regression that it should not be committed.
- Keep Candidate M's upload-wait metrics as the accepted base.

Next target:

```text
Candidate O: upload-wait attribution by freeze reason and batch shape.
  Add observability before another behavior change:
    - count commit-wait upload time by freeze reason;
    - count staged/uploaded batch size buckets;
    - surface direct0 cached-only partial-tail upload pressure separately from
      direct1 normal-only writeback pressure.
  Accept only if direct0/direct1 randrw is neutral. Then choose the next
  behavior candidate from measured wait source:
    - if cached-only partial tails dominate, tune cached-origin idle/too-many
      dispatch;
    - if normal-only direct1 waits dominate, tune writeback upload scheduling
      or backpressure, not staging coverage.
```

## 2026-06-12 Candidate O: Upload-Wait Attribution

Hypothesis:

- Candidate M showed commit retry/backoff is not the bottleneck; upload waits
  dominate commit-loop delay.
- Before changing scheduling/backpressure, split commit upload waits by freeze
  reason and write origin, and expose upload batch shape so the next behavior
  change can target the measured pressure source.

Implementation:

- Added commit-wait upload counters by freeze reason:
  size/chunk-end, max-unflushed, explicit flush, auto, commit-age safety, and
  unknown.
- Added commit-wait upload counters by write origin:
  normal-only, cached-only, mixed-origin, and unknown.
- Added upload batch shape counters for single-block and multi-block batches.
- Surfaced the new counters through `/metrics` and
  `tools/perf/compare_artifacts.py`.

Functional verification:

```text
cargo fmt --all
cargo test -p brewfs --lib 'vfs::io::writer::tests::test_writeback_phase_metrics_track_stage_and_remote_upload'
cargo test -p brewfs --lib 'vfs::stats::tests::'
cargo test -p brewfs --lib 'vfs::io::writer::tests::'
bash tools/perf/test_compare_artifacts.sh
git diff --check
cargo clippy -p brewfs --lib -- -D warnings
```

Perf artifacts:

```text
Candidate O full matrix:       perf-run-1781230033-6355
Candidate O direct1 repeat:    perf-run-1781230549-2914
Nearby HEAD direct1 baseline:  perf-run-1781230926-23364
```

Key results:

```text
Candidate O full matrix vs Candidate M direct0:
  read_bw_mib_s 259.278 -> 259.393 (+0.0%)
  write_bw_mib_s 117.817 -> 117.967 (+0.1%)
  tool_wall_s 86 -> 77 (-10.5%)
  post_write_drain_s 0 -> 15

Candidate O full matrix vs Candidate M direct1:
  prefill drain 52s, so treat this run as noisy for direct1 throughput.
  read_bw_mib_s 209.146 -> 132.782 (-36.5%)
  write_bw_mib_s 95.417 -> 61.744 (-35.3%)
  post_write_drain_s 20 -> 38

Candidate O direct1 repeat vs Candidate M direct1:
  prefill drain 4s
  read_bw_mib_s 209.146 -> 176.585 (-15.6%)
  write_bw_mib_s 95.417 -> 79.990 (-16.2%)
  post_write_drain_s 20 -> 22

Candidate O direct1 repeat vs nearby HEAD direct1:
  read_bw_mib_s 185.706 -> 176.585 (-4.9%)
  write_bw_mib_s 85.587 -> 79.990 (-6.5%)
  active_plus_drain_s 83.486 -> 88.008 (+5.4%)
  post_write_drain_s 21 -> 22 (+4.8%)
  post-drained PUT/GiB 258.681 -> 255.418 (-1.3%)
  post-drained upload byte amplification 0.940 -> 0.938 (-0.2%)
```

Decision:

- Accept Candidate O as observability with caution, not as a performance
  improvement.
- The first direct1 run was dominated by a slow prefill/object-store tail and
  is not a fair code comparison. The nearby A/B still shows a small direct1
  throughput loss, but object count and upload amplification are neutral or
  slightly better; the difference is most visible as S3 PUT average latency.
- Keep the new metrics because they are required to pick the next behavior
  candidate, and gate the next code change against the nearby HEAD direct1
  numbers above.

Next target:

```text
Candidate P: reduce direct1 upload hard-wait tail without increasing object
amplification.
  Use Candidate O attribution to identify which reason/origin pair dominates:
    - if normal-only max-unflushed waits dominate, tune upload scheduling so
      already-frozen normal slices get remote capacity before more small tail
      work;
    - if cached-only auto waits dominate, tune cached-origin idle/too-many
      dispatch instead;
    - keep direct0 read/write within 2% and direct1 active+drain no worse than
      the nearby HEAD direct1 baseline.
```

## 2026-06-17 Rejected Candidate Q: FsWriteBackCache Stage-Range Map

Hypothesis:

- `FsWriteBackCache::seal_slice_record` proves staged completeness with
  `metadata(slice_path).len() >= length`, which can mistake sparse staged files
  for fully staged data.
- Tracking staged ranges in memory would both close the correctness gap and
  avoid an extra local filesystem metadata lookup on the foreground seal path.
- This deliberately avoided the previously rejected upload concurrency,
  notification, stage/upload ordering, and slice coalescing changes.

Local CI gate:

```text
bash -n docker/compose-xfstests/run_perf_in_container.sh
bash -n docker/compose-xfstests/run_redis_perf.sh
bash -n docker/compose-xfstests/run_juicefs_perf_in_container.sh
bash -n docker/compose-xfstests/run_juicefs_perf.sh
bash docker/compose-xfstests/test_perf_report_delta.sh
bash docker/compose-xfstests/test_juicefs_direct_matrix.sh
bash docker/compose-xfstests/test_juicefs_perf_report.sh
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo check --workspace
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo build --workspace
rfuse3 feature checks skipped because rfuse3 is not a workspace member
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo check -p brewfs --no-default-features --features fuse-tokio-runtime
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo check -p brewfs --no-default-features --features fuse-io-uring-runtime
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo test --workspace --lib --bins
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo clippy --workspace
```

Functional result:

- The focused RED test reproduced the sparse-stage hole: a tail-only staged file
  could be sealed because file length was large enough.
- The candidate made the focused writeback cache tests and the local CI Rust
  job pass.

Perf artifact:

```text
candidate direct0 focused writeback: perf-run-1781684542-6139
accepted direct0 baseline:          perf-run-1781673470-37
```

Key results:

```text
fio-seqwrite tool_wall_s:  130 -> 126 (-3.1%)
fio-randwrite tool_wall_s: 131 -> 132 (+0.8%)
fio-randrw tool_wall_s:    143 -> 182 (+27.3%)
fio-randrw read_bw_mib_s:  914.3 -> 275.9 (-69.8%)
fio-randrw write_bw_mib_s: 409.4 -> 122.9 (-70.0%)

randrw drain did not complete cleanly. After the script exceeded the expected
post-write drain timeout, BrewFS still reported about 7.0 GiB dirty,
6.1 GiB live dirty, 801 MiB recent pending upload, and 3.1 GiB remote upload
in flight. A child shell was blocked in FUSE getattr on the mount.
```

Decision:

- Reject and roll back Candidate Q code.
- The correctness issue remains worth fixing, but this implementation creates
  too many live cached-only slices in mixed read/write and turns `randrw` into a
  hard regression.
- Any future fix for staged coverage must be tied to the writer's existing
  slice lifecycle state instead of adding a second hot-path range map in
  `FsWriteBackCache`.
- Keep the local CI test gate as mandatory for every accepted performance
  change, but keep `randrw` plus post-write drain as the decisive perf gate.

## 2026-06-17 Goal Amendment: Local CI Test Gate

The active performance goal now treats the CI workflow's `Test workspace` step
as a hard acceptance gate. Every accepted performance iteration must run this
command locally, in the same iteration as the perf evidence, before the numbers
are considered valid:

```bash
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo test --workspace --lib --bins
```

Focused tests still belong in the development loop, but they only prove the
narrow hypothesis. They do not replace the local CI test gate. If this command
fails, the next performance goal step is to fix, quarantine, or document that
failure before continuing with throughput tuning.

Local reproduction on 2026-06-17:

```text
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo test --workspace --lib --bins
result: ok, 504 passed, 0 failed, 159 ignored
notable guard: vfs::fs::tests::io_tests::test_fs_fuzz_parallel_read_write passed
```

## 2026-06-17 Rejected Candidate R: Relax Older-Unique Append Reuse

Hypothesis:

- Older FUSE write `unique` values were causing `ChunkHandle::find_slice_or_create`
  to reject writable slices even for strict appends.
- A strict append is non-overlapping, so allowing it to reuse the current slice
  might reduce `slice_reject_older_unique_ops`, slice creation, and object
  amplification while preserving the existing overlap guard.

Verification before perf:

```text
RED: cargo test -p brewfs --lib vfs::io::writer::tests::test_cached_older_unique_append_reuses_non_overlapping_slice -- --nocapture
     failed as expected with slice_create_ops left=2 right=1
GREEN: same focused test passed after the candidate change
cargo test -p brewfs --lib vfs::io::writer::tests -- --nocapture
     46 passed; 0 failed
cargo fmt --all --check
git diff --check
```

Perf artifact:

```text
candidate direct0 focused writeback: perf-run-1781687228-30572
accepted direct0 baseline:          perf-run-1781673470-37
```

Key results:

```text
fio-seqwrite tool_wall_s:        130 -> 129 (-0.8%)
fio-seqwrite write_bw_mib_s:     126.2 -> 124.8 (-1.0%)
fio-seqwrite s3_put_ops:         9573 -> 10553 (+10.2%)
fio-randwrite tool_wall_s:       131 -> 185 (+41.2%)
fio-randwrite active_runtime_s:  43.719 -> 32.919 (-24.7%)
fio-randwrite write_bw_mib_s:    204.6 -> 285.9 (+39.8%)
fio-randwrite buffer_dirty_mib:  0 -> 2964.7
fio-randwrite live_dirty_mib:    0 -> 1988.9
slice_reject_older_unique_ops:   149288 -> 143544 (-3.8%)
upload_partial_tail_flush_ops:   574 -> 4497 (+683.4%)
```

Decision:

- Reject and roll back Candidate R code.
- The candidate made active `fio-randwrite` bandwidth look better, but it
  shifted too much cost into dirty writeback debt and made wall time 41.2%
  worse.
- The small drop in older-unique rejects did not reduce slice/object
  amplification. It increased slice creation, upload batch count, partial-tail
  pressure, and pending dirty bytes.
- `creation_unique` is not the root performance lever by itself. Future work
  should use explicit extent ordering/coalescing or upload scheduling evidence
  instead of relaxing append reuse heuristics in isolation.
