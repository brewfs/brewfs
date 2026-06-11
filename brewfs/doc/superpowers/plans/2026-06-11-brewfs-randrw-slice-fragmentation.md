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
