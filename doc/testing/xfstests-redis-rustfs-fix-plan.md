# xfstests Redis + RustFS Fix Map

Short handoff for the Redis metadata + RustFS object-store correctness run.

## Current Status

- Full xfstests passed all 708 configured cases on SQLite, Redis, etcd, and
  TiKV: `run-1783958148-29779`, `run-1783956574-9468`,
  `run-1783965696-23764`, and `run-1783958147-10169`.
- The runner now retries only failures where the reported failure set exactly
  matches missing xfstests `/tmp/<pid>.out` files and no `.out.bad` exists.
  Real filesystem failures remain fatal.
- Full LTP passed with `failures_count: 0` on SQLite, Redis, etcd, and TiKV:
  `run-1783973125-31675`, `run-1783972047-5725`,
  `run-1783974271-14804`, and `run-1783975421-24234`.
  Environment-only `TCONF` entries remain for unavailable container kernel
  modules and image tools.
- Redis+RustFS LTP also passed after the runner began applying bounded cache
  defaults: `run-1784362121-30367`, `failures_count: 0`. The preceding
  `run-1784128661-5712` failure was one FUSE daemon disconnect during `gf17`;
  all 23 later failures were consequential `ENOTCONN` results, not independent
  filesystem failures.
- LTP `iogen01` remains known failing in the normal buffered FUSE profile and
  must stay in `docker/compose-xfstests/ltp_skip_tests.txt`.
- Full pjdfstest passed without default exclusions on Redis and TiKV:
  `pjdfstest-run-1783976619-22517` and
  `pjdfstest-run-1783976847-8934`; each selected all 246 files and passed
  9,134 assertions with 0 failures.
- stress-ng smoke passed:
  `docker/compose-xfstests/artifacts/perf-run-1783981107-15941`
  - Result: `stress-ng` completed successfully.
- Focused `fio-randrw` performance guard passed on the final release binary:
  `docker/compose-xfstests/artifacts/perf-run-1783980882-18667`
  - Read: `377.13 MiB/s`, write: `174.18 MiB/s`.
  - Compared with the accepted `386.36/178.79 MiB/s` baseline: read `-2.39%`,
    write `-2.58%`, both below the 5% limit.

## Fixed This Round

- Bound the LTP runner's default cache profile to 1 GiB read memory, 256 MiB
  write memory, and a 1 GiB VFS budget. The values remain overridable through
  `BREWFS_READ_MEMORY_BYTES`, `BREWFS_WRITE_MEMORY_BYTES`, and
  `BREWFS_MEMORY_BUDGET_BYTES`. This keeps the full correctness suite from
  relying on idle host RAM; `run-1784362121-30367` passed at an observed
  BrewFS RSS peak of about 4.76 GiB, with no new exclusions.
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
- Added FIFO, socket, character-device, and block-device kind plus `rdev`
  persistence to TiKV and etcd. Redis and SQLite already supported these node
  types. The full pjdfstest corpus now runs without default exclusions.
- Restored xfstests `generic/633`. The shared FUSE create paths now inherit the
  parent GID under a setgid directory and propagate setgid to new child
  directories without an extra parent-attribute lookup. Final-release targeted
  passes cover SQLite `run-1783980166-29700`, Redis `run-1783980181-16080`,
  etcd `run-1783980192-26585`, and TiKV `run-1783980222-31648`.

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
`docker/compose-xfstests/artifacts/perf-run-1783980882-18667` with no
regression over the 5% threshold.

## If New Failures Appear

Update this file with only:

- failing case name
- artifact path
- first useful `.out.bad` or log excerpt
- likely code area to inspect
