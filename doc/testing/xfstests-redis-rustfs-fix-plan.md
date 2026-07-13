# xfstests Redis + RustFS Fix Map

Short handoff for the Redis metadata + RustFS object-store correctness run.

## Current Status

- Full xfstests passed on SQLite, Redis, and etcd with 708 configured cases:
  `run-1783831206-2178`, `run-1783835489-21324`, and
  `run-1783837045-32187`.
- TiKV enumerated all 707 configured cases in `run-1783857700-31592`. The only
  reported failure, `generic/002`, lost the xfstests driver's `/tmp/28.out`;
  isolated rerun `run-1783864499-12546` passed in 0s. Treat this as a harness temporary-file
  race, not a BrewFS hard-link defect.
- Full LTP passed with `failures_count: 0` on SQLite, Redis, etcd, and TiKV:
  `run-1783848194-27300`, `run-1783849729-17223`,
  `run-1783850809-11579`, and `run-1783852001-9407`.
  Environment-only `TCONF` entries remain for unavailable container kernel
  modules and image tools.
- LTP `iogen01` remains known failing in the normal buffered FUSE profile and
  must stay in `docker/compose-xfstests/ltp_skip_tests.txt`.
- pjdfstest supported sets passed on Redis and TiKV:
  `pjdfstest-run-1783853192-21814` and
  `pjdfstest-run-1783853289-6218`; each selected 176 files with 0 failures and
  70 explicit skips.
- stress-ng smoke passed:
  `docker/compose-xfstests/artifacts/perf-run-1783535542-30210`
  - Result: `stress-ng` completed successfully.
- Focused `fio-randrw` performance guard passed:
  `docker/compose-xfstests/artifacts/perf-run-1783547278-18906`
  - Read: `386.36 MiB/s`, write: `178.79 MiB/s`
  - Compared with previous focused baseline `389.21 MiB/s` read and
    `180.02 MiB/s` write: read `-0.73%`, write `-0.68%`.

## Fixed This Round

- Fixed `--no-default-skip` in
  `docker/compose-xfstests/run_ltp_in_container.sh`.
  - Prior bug: using `/dev/null` as the skip file made `awk` consume the LTP
    command file as the first file and emit an empty run list, so `pan`
    failed with `Must supply a file collection or a command`.
- Tightened dirty overlay slice reuse in `src/vfs/io/writer.rs`.
  - Older dirty slices are no longer reused for newer overlapping writes.
  - Added focused dirty-overlay regression coverage.
- Fixed the direct-I/O `iogen01` tiny-overlap shape:
  `docker/compose-xfstests/artifacts/run-1783536533-27423`
  - Result: `LTP PASS` with `BREWFS_FUSE_DIRECT_IO=1`.
- Restored xfstests `generic/736` to the default Redis+RustFS run by removing
  it from `tests/scripts/xfstests_slayer.exclude`.
  - First pass in a targeted batch:
    `docker/compose-xfstests/artifacts/run-1783532285-12981`
  - Repeat focused pass:
    `docker/compose-xfstests/artifacts/run-1783532356-26779`
  - Full-suite pass:
    `docker/compose-xfstests/artifacts/run-1783545550-19958`

## Remaining Excluded Cases

### xfstests `generic/075`

TiKV+RustFS full run `run-1783841429-4658` and focused runs
`run-1783853390-17236`, `run-1783854319-13752`, and
`run-1783856733-20048` expose pre-truncate bytes after shrink followed by mmap,
copy, or fallocate extension. VFS-only shrink/rewrite and shrink/fallocate
regressions pass, isolating the remaining issue to buffered FUSE mmap cache
coherence.

- Direct I/O run `run-1783856978-31310` cannot execute MAPWRITE (`ENODEV`).
- Writeback-cache run `run-1783857084-26829` still returns stale bytes.
- Post-reply inode invalidation in `run-1783855018-22012` deadlocked fsx in
  `request_wait_answer`; the test container cannot be reaped without reboot.
- Default suites exclude `generic/075` next to `generic/112`, `generic/127`,
  `generic/263`, and `generic/438`. Re-enable it only with a kernel/async-fuse
  mmap invalidation strategy that passes `generic/075 generic/014` without a
  D-state task or a material throughput regression.

### LTP `iogen01`

Keep this excluded from the default LTP suite until the buffered FUSE
writeback-cache shape is fixed.

- Direct-I/O tiny-overlap shape is fixed:
  `docker/compose-xfstests/artifacts/run-1783536533-27423`
  - Result: `LTP PASS` with `BREWFS_FUSE_DIRECT_IO=1`.
  - Regression coverage:
    `test_dirty_overlay_prefers_latest_tiny_tail_overlap` and
    `test_fs_read_after_tiny_overlapping_writes_uses_latest_dirty_bytes`.
- Buffered split-write/page-cache race remains:
  `docker/compose-xfstests/artifacts/run-1783543020-21424`
  - Application write: `314609+65912`.
  - FUSE split: `314609+783` then `315392+65129`.
  - Interleaving: another process read `274432+53248` and `376832+28672`
    between the split writes, which can leave stale bytes in the kernel page
    cache for the writer's locked range.
  - Failed experiments: extending the split-write barrier to 1s only delayed
    the second split write until after the read returned; `notify.store` was
    not a sufficient ordering barrier; write-only direct I/O stalled doio.
- Likely area: FUSE buffered page-cache invalidation or a targeted
    direct-I/O policy in `src/vfs/fs/mod.rs` and `src/fuse/mod.rs`.

Useful diagnostic command:

```bash
BREWFS_FUSE_OP_LOG=1 \
  bash docker/compose-xfstests/run_redis_ltp.sh --no-default-skip --extra-args "-s iogen01"
```

Avoid relying on full direct-I/O LTP as a correctness signal: mmap-based cases
such as `rwtest01` and `rwtest02` can return `ENODEV` under FUSE `direct_io`.

## Validation From This Round

```bash
bash -n docker/compose-xfstests/run_ltp_in_container.sh
bash -n docker/compose-xfstests/run_redis_ltp.sh
cargo fmt --all
cargo test -p brewfs dirty_overlay --lib
bash docker/compose-xfstests/run_redis_xfstests.sh --check-args "-fuse generic/736"
bash docker/compose-xfstests/run_redis_xfstests.sh
bash docker/compose-xfstests/run_redis_ltp.sh
bash docker/compose-xfstests/run_redis_pjdfstest.sh
bash docker/compose-xfstests/run_redis_stress_ng.sh --profile smoke

PERF_FIO_SIZE=512m PERF_FIO_RUNTIME=20 PERF_FIO_COLD_READ=true \
  PERF_FIO_DROP_CACHES=false \
  bash docker/compose-xfstests/run_redis_perf.sh --s3 --tools "fio-randrw"

BREWFS_FUSE_DIRECT_IO=1 BREWFS_FUSE_OP_LOG=1 \
  bash docker/compose-xfstests/run_redis_ltp.sh --no-default-skip --extra-args "-s iogen01"
BREWFS_FUSE_OP_LOG=1 \
  bash docker/compose-xfstests/run_redis_ltp.sh --no-default-skip --extra-args "-s iogen01"
```

The direct-I/O diagnostic now passes. The normal buffered diagnostic still
fails, most recently at `docker/compose-xfstests/artifacts/run-1783543020-21424`.
The latest focused `fio-randrw` guard passed at
`docker/compose-xfstests/artifacts/perf-run-1783547278-18906` with no
regression over the 5% threshold.

## If New Failures Appear

Update this file with only:

- failing case name
- artifact path
- first useful `.out.bad` or log excerpt
- likely code area to inspect
