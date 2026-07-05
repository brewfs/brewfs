# xfstests Redis + RustFS Fix Map

Short handoff for the Redis metadata + RustFS object-store correctness run.

## Current Status

- Latest full Redis+RustFS xfstests artifact:
  `docker/compose-xfstests/artifacts/run-1783213072-12594`
- Result: `Passed all 707 tests` for the configured test set, with the
  maintained exclude file in `tests/scripts/xfstests_slayer.exclude`.
- Latest focused perf artifact:
  `docker/compose-xfstests/artifacts/perf-run-1783214950-22792`
  - `fio-randrw`: read `383.29 MiB/s`, write `176.86 MiB/s`
  - Focused baseline `perf-run-1783206722-27507`: read `280.01 MiB/s`,
    write `131.11 MiB/s`
  - Delta: read `+36.89%`, write `+34.90%`
- LFS pointer check after test/perf runners:
  `tests/scripts/xfstests-prebuilt/xfstests-prebuilt.tar.gz` is the expected
  Git LFS pointer for
  `sha256:b6395f6c1d1058c774317aeaa93eaf78a9d0eb1985c164caae53a060f2650488`.

## Fixed Cases

- `generic/075`
  - Symptoms: fsx readback mismatch after the probe enabled copy-range;
    previous run copied zeros/stale data through the kernel fallback path.
  - Fix: enable BrewFS FUSE `copy_file_range` by default, keep
    `BREWFS_FUSE_COPY_FILE_RANGE=0` as an opt-out, and keep the earlier dirty
    handle/fallocate ordering fixes.
  - Touch points: `src/fuse/mod.rs`, `src/vfs/fs/mod.rs`,
    `src/vfs/io/writer.rs`, `docker/compose-xfstests/*`.
  - Focused pass: `run-1783208627-2221`.
  - Full pass: `run-1783213072-12594`.
- `generic/564`
  - Symptoms: very large destination copy returned `EIO`; expected
    `EFBIG`/`File too large`.
  - Fix: reject `off + len >= i64::MAX` in both internal
    `copy_file_range` and the write path used by kernel fallback copies.
  - Touch points: `src/vfs/fs/mod.rs`, `src/vfs/fs/tests.rs`.
  - Focused pass: `run-1783212557-22962`.
  - Full pass: `run-1783213072-12594`.
- `generic/568` and `generic/694`
  - Fix: persist Redis `st_blocks` and update fallocate/write block
    accounting without slowing the common overwrite path.
  - Touch points: `src/meta/stores/redis/mod.rs`, `src/meta/client/mod.rs`,
    `src/meta/layer.rs`, `src/meta/store.rs`, `src/vfs/fs/mod.rs`,
    `src/vfs/meta_ops.rs`.
  - Focused pass: `run-1783200166-11522`.
  - Full pass: `run-1783213072-12594`.
- `generic/391`
  - Fix: cache FUSE `statfs` briefly to avoid Redis pressure during repeated
    small fallocates.
  - Touch point: `src/vfs/fs/mod.rs`.
  - Focused pass: `run-1783191384-13539`.
- `generic/438`
  - Hang path was reduced by bounded dirty snapshot flushes and FUSE request
    ordering before flush/fsync/fallocate.
  - Keep excluded: the remaining failure is mmap + byte-at-a-time fallocate
    stale/zeroing behavior through Linux FUSE page cache invalidation.
  - Touch points: `src/fuse/mod.rs`, `src/vfs/io/writer.rs`,
    `src/vfs/fs/mod.rs`, `tests/scripts/xfstests_slayer.exclude`.

## Known Excludes

Keep these excluded unless the underlying feature or FUSE limitation is
intentionally changed:

- `generic/074`: impractical tiny-object soak for Redis+RustFS.
- `generic/112`, `generic/127`, `generic/263`, `generic/438`: mmap/FUSE page
  cache coherence limitations.
- `generic/478`: OFD lock close/refcount semantics are not fully modeled
  through async-fuse callbacks, though the earlier hang is gone.
- Other entries in `tests/scripts/xfstests_slayer.exclude` are feature gaps,
  infrastructure limits, or soak tests unsuitable for this FUSE setup.

## Validation

```bash
cargo fmt --check
cargo test -p brewfs fuse_copy_file_range_defaults_on_and_can_opt_out --lib
cargo test -p brewfs test_fs_copy_file_range_reports_efbig_only_after_source_data_exists --lib
cargo test -p brewfs fallocate --lib

RUN_TAG=full-after-boundary-1783213071-29601 \
  bash /tmp/brewfs_xfstests_case_runner.sh

PERF_FIO_SIZE=512m PERF_FIO_RUNTIME=20 PERF_FIO_COLD_READ=true \
  PERF_FIO_DROP_CACHES=false \
  bash docker/compose-xfstests/run_redis_perf.sh --s3 --tools "fio-randrw"
```

## If New Failures Appear

Update this file with only:

- failing case name
- artifact path
- first useful `.out.bad` or log excerpt
- likely code area to inspect
