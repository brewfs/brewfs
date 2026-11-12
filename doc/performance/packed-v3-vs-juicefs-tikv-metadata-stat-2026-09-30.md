# Packed v3 vs JuiceFS+TiKV metadata-only stat plan (2026-09-30)

## Status

The matched workload and runners are implemented and locally smoke-tested. A
full JuiceFS+TiKV result is **not yet available** because this host could not
pull `pingcap/pd:v8.5.0`, `pingcap/tikv:v8.5.0`, or the readiness image through
either configured Docker mirror; both attempts failed with TLS handshake
timeouts before any service started. Cleanup left no benchmark container or
volume running.

No performance-win claim is made from this document until the TiKV side runs.

## Workload

The intended comparison isolates metadata rather than payload reads:

- 10,000 regular files, 10 directories × 1,000 files;
- each file is 100 KiB and has deterministic independent fixture identity;
- 16 scanner workers;
- walk the exact directory shape and call `stat` for every file;
- verify regular-file mode and exact size;
- never open/read file payload (`payload_bytes=0`);
- payload memory/SSD caches are zero and the host page cache is dropped for the
  formal runner;
- packed uses `BREWFS_PACKED_METADATA_PREFETCH=eager` and a 256 MiB metadata
  budget; JuiceFS disables local data cache/prefetch and uses TiKV metadata.

The tools are `packed-stat`, `juicefs-stat` in the native runners, and
`smallfiles-stat` in the compose JuiceFS runner. The latter was smoke-tested on
a 10-file Redis fixture and reported `bytes=0`, then Compose removed its
containers and volumes.

## Packed local integration baseline

A real BrewFS FUSE mount over `LocalFsBackend` was built from
`packed_v3_snapshot_fixture` using the matched 10 × 1,000 shape. This is a local
integration baseline, not a TiKV comparison and not an OSS result:

```text
mount_ready_ms=106.638
metadata_warmup_ms=77
scan_seconds=0.357571
mount_plus_scan_seconds=0.555152
files_per_sec=27966.48
files=10000
errors=0
data_range_gets=0
frames_decoded=0
frame_directory_remote_gets=0
```

The eager metadata profile matters: unlike adaptive `auto`, it did not warm any
frame directory for the metadata-only workload. The request graph was one group
index object, three inode index objects, and 60 coalesced GroupMeta ranges. The
run removed its temporary fixture, cache, mount, and process on exit.

## Packed RustFS/HTTP integration baseline

The same 10 × 1,000 fixture was then published through the SDK to local RustFS
and mounted through BrewFS' S3-compatible HTTP backend. This keeps the scan
metadata-only while exercising real HTTP Range GETs during mount warm-up:

```text
mount_ready_ms=323.348
metadata_warmup_ms=206
scan_seconds=0.377112
mount_plus_scan_seconds=0.768167
files_per_sec=26517.33
stat_latency_p50_ms=0.439063
stat_latency_p95_ms=1.148058
scanner_peak_rss_kib=38428
files=10000
errors=0
data_range_gets=0
frames_decoded=0
frame_directory_remote_gets=0
```

The host page cache was dropped successfully with non-interactive sudo immediately
before the fresh mount. The run removed its temporary fixture, mount, cache,
RustFS container and Compose volumes on exit.


## Matched Redis diagnostic (not the requested TiKV result)

While the TiKV images were unavailable, the same shared scanner was run against
JuiceFS 1.3.1 with loopback/container-network Redis, which is a more favorable
metadata backend for JuiceFS than remote TiKV. Both sides used the same 10 ×
1,000 directory shape, 100 KiB file size, 16 workers and stat-only semantics:

| mounted-FUSE scan | seconds | files/s | payload bytes |
| --- | ---: | ---: | ---: |
| packed v3, RustFS HTTP metadata, eager warm | 0.377112 | 26,517.33 | 0 |
| JuiceFS + Redis, attr/entry/dir/open caches disabled | 0.651548 | 15,348.05 | 0 |

The packed HTTP scanner phase is `1.73x` the Redis strict scan rate. This is useful
diagnostic evidence that metadata-only immutable scans are the right candidate
scenario, but it is not the requested JuiceFS+TiKV result. The two filesystems
use different metadata backends/layouts by design. Packed mount readiness was
323.348 ms; mount plus scan was 0.768167 s. The JuiceFS artifact is:

```text
docker/compose-xfstests/artifacts/juicefs-perf-run-1790783857-8985/
```

Its JSON records p50/p95 stat latency of 0.875/1.205 ms, zero payload bytes and
38,508 KiB scanner peak RSS. Compose cleanup left no Redis/RustFS/perf container
or benchmark volume running.


## TiKV execution command

Once the standard images are available locally, run the compose baseline:

```bash
JUICEFS_META_BACKEND=tikv \
PERF_LOG_TO_CONSOLE=false \
PERF_SMALLFILE_DIRS=10 \
PERF_SMALLFILE_FILES_PER_DIR=1000 \
PERF_SMALLFILE_SIZE=102400 \
PERF_SMALLFILE_COLD_READ=true \
JFS_CACHE_SIZE_MIB=0 \
JFS_BUFFER_SIZE_MIB=0 \
JFS_PREFETCH=0 \
JFS_OPEN_CACHE=0s \
JFS_OPEN_CACHE_LIMIT=0 \
JFS_ATTR_CACHE=0s \
JFS_ENTRY_CACHE=0s \
JFS_DIR_ENTRY_CACHE=0s \
JFS_BACKUP_META=0 \
bash docker/compose-xfstests/run_juicefs_perf.sh --tools 'smallfiles-stat'
```

The wrapper always runs `docker compose ... down -v --remove-orphans` unless
`--keep` is explicitly passed. Do not use `--keep` for acceptance runs.

## Acceptance boundary

Only compare the packed and TiKV numbers after both use the same directory
shape, file count/size, worker count, cache policy and page-cache state. Report
both post-mount scan time and mount/setup-plus-scan time. A packed win after
excluding its metadata warm-up is only a metadata-warm steady-state result; a
mount-inclusive win may be called end-to-end for this immutable metadata scan.

This scenario does not replace the existing full-payload comparison, where
packed v3 remains slower than JuiceFS. It tests the narrower design claim that
an authenticated immutable metadata image can outperform a transactional TiKV
metadata service for repeated, read-only namespace scans.
