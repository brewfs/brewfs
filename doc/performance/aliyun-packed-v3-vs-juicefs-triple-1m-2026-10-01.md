# Aliyun packed-v3 vs JuiceFS Redis/TiKV million-file validation (2026-10-01)

## Status

The matched 10k smoke passed for all three filesystems/backends, the
lexicographic 1M baseline completed for all three rows, and the GPU-like
shuffled 1M campaign completed for all four packed/Redis/TiKV rows with full
byte validation and zero errors.

**Conclusion: packed v3 does not win the shuffled GPU-shaped 1M profile.** It
shows a narrower ordered stat advantage over TiKV, not a general ordered/tree
or full-read win. See the measurement caveat below.

## Measurement caveat added on 2026-10-02

The packed runner exported `BREWFS_METADATA_CACHE_TTL_MS`, whereas the FUSE
adapter reads `BREWFS_CACHE_TTL_MS`. Before the forwarding correction, setting
the former did not prove the actual packed kernel TTL. The numeric rows below
are preserved as historical observations, but statements that both sides used
the same metadata TTL/cache semantics are **not verified**. Re-run matched
profiles with the effective variable and FUSE operation counters before making
new fairness or general superiority claims. Likewise, `data_range_gets=0` does
not count inline file bytes fetched through GroupMeta.

The archived strict r2 epoch-2 log contains errors and remains invalid; it must
not be silently substituted for the later successful row summarized here. Raw
artifacts are local generated evidence, not part of the Git commit. The completed
valid warm/Redis/TiKV rows are under
`docker/compose-xfstests/artifacts/aliyun-gpu-1m-r3-20261001/`.

## Workload contract

- one disposable `ecs.u1-c1m4.2xlarge` (32 GiB, 100 GiB ESSD);
- one OSS bucket/internal endpoint, unique root prefix;
- 1,000,000 independent 4 KiB files;
- 3 directory levels, fanout 10, 1,000 files per leaf;
- shared `tools/perf/smallfiles_scan.py`, 16 workers;
- tools run in order: tree, stat, full-read;
- a fresh mount and successful `drop_caches=3` before every tool;
- persistent payload cache, window cache and ordinary data prefetch disabled;
- JuiceFS attr/entry/dir-entry/open/data cache disabled;
- fixture/import, mount/warm-up, scanner active and drain durations reported
  separately;
- full reads validate every byte and require the same file count, byte count and
  checksum on all three sides.

The comparison rows are:

1. packed-metadata-v3, metadata prefetch `auto`, decoded-frame cache 0
   (`strict-cold`);
2. JuiceFS 1.3.1 + Redis strict caches;
3. JuiceFS 1.3.1 + TiKV v6.5.3 strict caches.

Any explicit packed decoded-frame cache result is reported separately as
`warm-frame-cache`; it cannot replace row 1.

## 10k smoke result

The smoke used the same layout at 10,000 files (`10×10×100`) and 4 KiB/file.
All rows passed full payload validation with checksum `1273096`, 40,960,000
payload bytes and zero errors. Cache directories were zero bytes/files before
and after every JuiceFS tool.

| tool | packed v3 files/s | JuiceFS+Redis files/s | JuiceFS+TiKV files/s |
| --- | ---: | ---: | ---: |
| tree | 56,568.50 | 47,553.74 | 15,730.31 |
| stat | 15,153.93 | 7,331.85 | 2,736.77 |
| full | 1,290.64 | 1,122.20 | 769.82 |

Latency and payload request evidence:

- packed stat p50/p95: 0.682/1.497 ms; data range GETs: 0;
- Redis stat p50/p95: 1.639/2.060 ms;
- TiKV stat p50/p95: 4.560/6.391 ms;
- packed full p50/p95: 8.747/33.357 ms, 723 physical data ranges,
  130,301,952 fetched bytes for 40,960,000 logical bytes;
- Redis full p50/p95: 13.781/21.356 ms;
- TiKV full p50/p95: 19.148/28.103 ms.

The smoke therefore confirms runner correctness and establishes that the
metadata-only packed scenario is favorable, but it is not the requested 1M
result.

## Diagnostics captured

- scanner JSON/log: files, directories, stat calls, bytes, checksum, errors,
  files/s, MiB/s, p50/p95 and scanner RSS;
- active, unmount drain and active+drain seconds;
- packed metadata warm-up, metadata/data GET and bytes, overscan, frame,
  coalescing, singleflight, pipeline and cache metrics;
- JuiceFS metrics before/after each tool;
- Redis command calls, memory and key count before/after;
- PD cluster status, store count and TiKV storage command totals before/after;
- cache and page-cache proof plus ECS memory/disk proof.

## Resource isolation and cleanup

JuiceFS data uses an explicit per-run OSS prefix instead of bucket-root
`chunks/`. Redis and TiKV use different volume/data/work identities. TiKV
cleanup waits for processes, force-kills leftovers and removes its TiUP home.
Every run is protected by ECS auto-release and outer `finally` cleanup.

The 10k smoke ECS was stopped/deleted after completion. An independent
`DescribeInstances` query returned zero instances for its id, and an OSS JSON
listing returned zero objects under the smoke root prefix.

## Completed lexicographic 1M baseline

The first 1M campaign completed all three rows with the lexicographic scanner.
Every full row validated 1,000,000 files, 4,096,000,000 payload bytes, checksum
`127493920` and zero errors. Local data caches remained disabled/empty.

| tool | packed v3 | JuiceFS+Redis | JuiceFS+TiKV |
| --- | ---: | ---: | ---: |
| tree files/s | 10,203.53 | 79,147.18 | 46,359.94 |
| stat files/s | 4,595.13 | 6,089.22 | 2,186.64 |
| full files/s | 681.04 | 1,144.90 | 674.13 |
| full MiB/s | 2.66 | 4.47 | 2.63 |
| full p50/p95 ms | 19.54 / 48.44 | 12.43 / 23.79 | 21.64 / 35.63 |

Active+drain seconds were:

- packed tree/stat/full: 99 / 219 / 1,471 seconds (approximately, from mount
  uptime and scanner/runtime evidence; concise runner rows recorded drain
  separately after the smoke fix);
- Redis: 14.69 / 168.00 / 877.91 seconds;
- TiKV: 23.72 / 460.85 / 1,488.53 seconds.

Packed metadata behavior changed with scale. A 512 MiB metadata budget warmed
only 31/245 inode-index pages and 913/4,111 GroupMeta pages. The full phase
recorded 911 inode-index remote GETs and 8,260 GroupMeta GETs in the archived
full-phase log (666/8,262 belong to the stat phase). Packed full read issued
136,883 data ranges and fetched 32.65 GB (30.41 GiB) for 4.096 GB logical bytes;
this random 4 KiB layout therefore has substantial frame overscan. Redis kept
about 2,002,230 keys and used about 422 MiB.

Interpretation:

- packed beats TiKV stat by about 2.10x and is approximately tied on full read
  (1.01x), but loses tree;
- packed loses Redis at 1M even though it led the matched 10k stat smoke;
- this access order is lexicographic and benefits the physical packed order, so
  it is not a GPU DataLoader claim.

The campaign root prefix and ECS were independently verified empty/deleted after
completion.

## GPU-like 10k smoke

The shuffled scanner and runner wiring passed a matched 10k smoke with two
epochs, 16 workers, batch size 256, deterministic seed `20261001`, bounded
in-flight batches, full payload validation and zero errors.

| profile | epoch 1 files/s | epoch 2 files/s | epoch 1 p50/p95 ms | epoch 2 p50/p95 ms |
| --- | ---: | ---: | ---: | ---: |
| packed buffered, decoded cache 0 | 559.47 | 11,906.76 | 21.15 / 62.45 | 1.26 / 1.70 |
| packed decoded cache 64 MiB | 5,113.42 | 9,647.82 | 1.71 / 3.00 | 1.60 / 1.83 |
| JuiceFS+Redis, local data cache 0 | 942.41 | 3,754.76 | 13.58 / 20.50 | 3.92 / 4.62 |
| JuiceFS+TiKV, local data cache 0 | 666.50 | 1,300.45 | 18.30 / 26.10 | 10.92 / 13.92 |

Packed buffered mode uses `direct_io=0` and `keep_cache=1`, matching the
reference filesystems' kernel page-cache behavior. Epoch 1 is still cold at
mount start; epoch 2 explicitly measures the repeated training epoch and is not
a cold result. The decoded-cache row is a separate 64 MiB application-cache
profile. It fetched only 101 exact packed frames across both 10k epochs, with
8,696 decoded-frame hits and no eviction.

The GPU smoke ECS and all GPU smoke OSS prefixes were independently verified
deleted before the 1M GPU campaign.

## Completed GPU-like shuffled validation

A follow-up scanner profile uses deterministic per-epoch shuffle, batch size
256, 16 worker batches, bounded in-flight batches and two epochs. It still
performs stat/open/read-to-EOF and validates every payload byte. Epoch 1 is the
cold/shuffled result; epoch 2 measures repeated-epoch behavior. Packed strict
and explicit warm-frame-cache profiles remain separate. The lexicographic table
above remains a namespace/sequential baseline only.

### Packed 1M shuffled two-epoch result

Measured on the same 1M x 4 KiB fixture, buffered FUSE, decoded-frame cache 0,
metadata budget 512 MiB, prefetch `auto`, 16 workers, batch 256, deterministic
shuffle seed `20261001`:

| epoch | files/s | MiB/s | p50 ms | p95 ms | errors |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 (cold, shuffled) | 186.54 | 0.729 | 80.79 | 153.28 | 0 |
| 2 (repeated epoch) | 411.35 | 1.607 | 38.98 | 75.10 | 0 |

Both epochs validated 1,000,000 files, 4,096,000,000 payload bytes and checksum
`127493920`; the runner reported `pass` with 7,795.58 s active and 5.09 s drain,
and scanner peak RSS stayed at 690 MiB.

Random 4 KiB shuffled access is close to the worst case for this packed layout.
The cold epoch issued 775,628 physical data ranges and fetched 189.96 GB for
4.096 GB of logical payload, and the metadata budget covered only a prefix of the
1M-inode snapshot, so inode-index and GroupMeta pages were re-read repeatedly.
The repeated epoch improves about 2.2x because payload bytes come from the kernel
page cache, but it still returns to userspace for every lookup.

An earlier attempt at this row is invalid and is not used for any number above:
it hit the two-hour tool limit during epoch 2, and the runner then unmounted the
filesystem while the scanner process was still running, so the surviving epoch-2
entries reported `ENOENT`. That was a harness artifact, not a metadata loss. The
runner now stops the scanner's own process tree before unmounting and accepts a
four-hour tool timeout, so the rerun keeps a real epoch-2 measurement.

### Packed warm-frame-cache row

The same fixture, shuffle seed, batch and worker settings, with an explicit 4 GiB
exact decoded-frame cache and zero persistent data cache:

| epoch | files/s | MiB/s | p50 ms | p95 ms | errors |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 (cold, shuffled) | 369.05 | 1.442 | 41.06 | 83.28 | 0 |
| 2 (repeated epoch) | 411.12 | 1.606 | 38.94 | 75.41 | 0 |

Both epochs passed with 5,145.98 s active and 5.39 s drain. The exact decoded
cache lifts the cold shuffled epoch about 1.98x over the strict row, because a
4 KiB random workload revisits shared frames immediately. It does not change
epoch 2: both rows are then dominated by the kernel page cache, so the two rows
converge at about 411 files/s.

This is the important distinction for the GPU-shaped workload: packed v3's
advantage is in the first, cold pass over an immutable snapshot, not in steady
repeated epochs once the kernel already serves the payload.

### JuiceFS + Redis row

Same fixture, shuffle seed, batch and worker settings, `cache-size=0`,
`prefetch=0`, and metadata caches disabled on both sides for a fair strict row:

| epoch | files/s | MiB/s | p50 ms | p95 ms | errors |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 (cold, shuffled) | 826.84 | 3.230 | 16.29 | 37.70 | 0 |
| 2 (repeated epoch) | 814.01 | 3.180 | 18.15 | 38.39 | 0 |

Both epochs passed with 2,441.75 s active and 2.71 s drain.

### JuiceFS + TiKV row

Same fixture and settings, with the TiKV v6.5.3 playground started by the
runner and metadata caches disabled:

| epoch | files/s | MiB/s | p50 ms | p95 ms | errors |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 (cold, shuffled) | 590.73 | 2.308 | 24.46 | 43.80 | 0 |
| 2 (repeated epoch) | 600.66 | 2.346 | 24.38 | 43.81 | 0 |

Both epochs passed with 3,361.20 s active and 2.74 s drain. Importing 1M objects
into TiKV took about 42 minutes versus about 18 minutes for Redis.

### Complete shuffled 1M comparison

| profile | epoch 1 files/s | epoch 2 files/s | epoch 1 MiB/s | epoch 1 p50/p95 ms | errors |
| --- | ---: | ---: | ---: | ---: | ---: |
| packed strict (decoded cache 0) | 186.54 | 411.35 | 0.729 | 80.79 / 153.28 | 0 |
| packed warm (decoded cache 4 GiB) | 369.05 | 411.12 | 1.442 | 41.06 / 83.28 | 0 |
| JuiceFS + Redis strict | 826.84 | 814.01 | 3.230 | 16.29 / 37.70 | 0 |
| JuiceFS + TiKV strict | 590.73 | 600.66 | 2.308 | 24.46 / 43.80 | 0 |

Every row validated 1,000,000 files, 4,096,000,000 payload bytes and checksum
`127493920` on both epochs with zero errors, so the comparison is like-for-like.

**Result: packed v3 does not win the shuffled GPU-shaped 1M profile.** Redis is
2.24x ahead of the best packed row, and TiKV is 1.60x ahead.

Random 4 KiB shuffled access is a hostile case for packed v3. Redis answers a
shuffled stat/open on a warm keyspace in tens of microseconds, while packed must
resolve the inode through an authenticated page and then fetch a physical frame;
with 4 KiB files that is roughly one frame per file, and the cold shuffled epoch
fell to about 0.73 MiB/s with 775,628 physical ranges and 189.96 GB fetched for
4.096 GB of payload.

The decoded-frame cache recovers about half the gap but does not close it. Its
remaining cost is metadata, not payload: a 512 MiB budget cannot hold the inode
index and GroupMeta of a 1M-inode snapshot, so repeated lookups re-read index
pages from OSS. The valid archived warm row records 1,284,176 inode-index GETs
and 1,236,238 GroupMeta GETs across both epochs (about 933.3 GiB metadata-range
bytes); these are millions of requests, not tens of thousands.

The packed rows still pay off where they did in the lexicographic campaign and in
the 10k metadata-only smoke: ordered access and metadata-dominant scans. The
honest summary across both 1M profiles is that ordered stat is faster than TiKV,
ordered tree is slower, full read is roughly tied with TiKV and slower than
Redis, and packed loses on a tiny random-read workload dominated by
per-file payload and metadata misses. Making packed competitive in the shuffled
profile needs a smaller hot working set per inode (or a metadata layout that
avoids a remote page per random lookup), not more payload caching.

## 1M acceptance

The 1M result is valid only if all three tools pass for all three rows, every
full-read checksum/byte count matches, all drop-cache/cache proofs are present,
and post-run ECS/prefix verification is zero. A TiUP/download/mount timeout or
cleanup failure invalidates the run and stops the campaign; Redis numbers must
not substitute for a missing TiKV result.
