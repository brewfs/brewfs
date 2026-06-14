# BrewFS JuiceFS Cache And Metadata Perf Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the remaining BrewFS vs JuiceFS performance gap in read cache, write cache stability, and metadata cache paths while preventing regressions across every Redis perf workload, including `fio-randrw`.

**Architecture:** Treat every optimization as a measured experiment. First refresh a full BrewFS/JuiceFS baseline with the same Redis + S3 runner matrix, then add observability for cache decisions, then optimize metadata hot paths and foreground-read priority before revisiting writeback. A code change is accepted only when its target metric improves and the full workload matrix stays within the regression budget.

**Tech Stack:** Rust BrewFS, Redis metadata backend, RustFS S3 object backend, JuiceFS Go reference code, Docker Compose perf runners, fio, xfstests `dirstress`/`dirperf`/`metaperf`/`looptest`, `.stats` diagnostics, `jq`.

---

## Current Evidence

### Existing JuiceFS Reference

Use this as the stable comparison target until a new JuiceFS run is captured:

- Artifact: `/mnt/slayerfs/docker/compose-xfstests/artifacts/juicefs-perf-run-1780562982-7892`
- Command recorded in the previous gap plan:

```bash
cd /mnt/slayerfs/brewfs
bash docker/compose-xfstests/run_juicefs_perf.sh \
  --tools "fio-bigwrite fio-bigread fio-seqread fio-seqwrite fio-randread fio-randwrite fio-randrw dirperf metaperf"
```

Known fio numbers from that artifact:

| Tool | JuiceFS read MiB/s | JuiceFS write MiB/s | read p99 ms | write p99 ms |
| --- | ---: | ---: | ---: | ---: |
| `fio-bigread` | 2386.95 | 0 | 43.25 | 0 |
| `fio-bigwrite` | 0 | 1270.47 | 0 | 58.98 |
| `fio-seqread` | 2651.49 | 0 | 1.47 | 0 |
| `fio-seqwrite` | 0 | 205.42 | 0 | 135.27 |
| `fio-randread` | 3256.73 | 0 | 7.31 | 0 |
| `fio-randwrite` | 0 | 204.69 | 0 | 295.70 |
| `fio-randrw` | 18.84 | 8.93 | 6408.90 | 333.45 |

### Existing BrewFS Evidence

Recent accepted writeback profile evidence:

| Artifact | Scope | Result |
| --- | --- | --- |
| `perf-run-1780666039-10304` | explicit writeback throughput profile | `fio-seqwrite = 1531.14 MiB/s`, write p99 `9.50 ms` |
| `perf-run-1780666168-636` | committed default writeback throughput profile | `fio-seqwrite = 1496.05 MiB/s`, write p99 `10.03 ms` |
| `perf-run-1780665906-14234` | same batch without the new admission gate | `fio-seqwrite = 1346.13 MiB/s`, write p99 `10.81 ms` |
| `perf-run-1780676297-22029` | old high-backlog profile in full matrix | `fio-seqwrite = 138.67 MiB/s`, then `fio-randread` prefill stalled with ~19GiB pending upload |
| `perf-run-1780677697-6276` | lower-backlog full-matrix candidate profile | `fio-seqwrite = 107.75 MiB/s`, `fio-randread = 1775.16 MiB/s`, follow-on prefill completed |
| `perf-run-1780678311-20354` | new default profile, write/read/randrw/metaperf gate | `fio-seqwrite = 108.26 MiB/s`, `fio-randread = 2104.11 MiB/s`, `fio-randrw = 129.04/59.47 MiB/s`, `metaperf pass` |
| `perf-run-1780678962-2388` | new default profile, remaining fio and non-fio gates | `fio-bigwrite = 415.25 MiB/s`, `fio-bigread = 4196.72 MiB/s`, `fio-seqread = 1572.72 MiB/s`, `fio-randwrite = 123.79 MiB/s`, `dirstress/dirperf/looptest pass` |
| `perf-run-1780680852-8870` | pure metadata metaperf, `PERF_METAPERF_FILE_SIZE=0` | `create = 5347.64 ops/s`; proves BrewFS pure create metadata is not the bottleneck |
| `perf-run-1780682166-14826` | metaperf default `-s 4096` with writeback persist sync disabled in throughput profile | `create = 766.04 ops/s`, up from `249.13 ops/s` in the comparable small-file baseline and above JuiceFS `704.41 ops/s`; open/stat/readdir/rename stayed stable |
| `perf-run-1780682653-28693` | hot `fio-randread fio-randrw` smoke with the same throughput profile | `fio-randrw = 179.56/80.43 MiB/s`, p99 `109.58/24.25 ms`, cache hit `99.1%`; no mixed-workload regression versus `129.04/59.47 MiB/s` baseline |
| `perf-run-1780730151-20133` | Task 1 hot observability gate, `fio-seqread fio-randread metaperf` | New `.stats` read strategy and metadata cache counters appeared in diagnostics; `fio-seqread = 1.65 GiB/s`, `fio-randread = 821.04 MiB/s`, `metaperf create = 751.7 ops/s` |
| `perf-run-1780730522-11955` | Task 1 cold/direct read smoke with drain, cache clear, and remount | New counters distinguished cold full GETs: seqread `S3 GET=287/read_full_gets=287`, randread `S3 GET=1037/read_full_gets=1037` |

Latest partial BrewFS all-fio sample:

- Artifact: `/mnt/slayerfs/docker/compose-xfstests/artifacts/perf-run-1780672743-23142`
- It includes `fio-bigwrite fio-bigread fio-seqread fio-seqwrite fio-randread fio-randwrite`.
- It does not include `fio-randrw dirstress dirperf metaperf looptest`, so it is not a complete regression baseline.

Partial sample fio numbers:

| Tool | BrewFS read MiB/s | BrewFS write MiB/s | read p99 ms | write p99 ms |
| --- | ---: | ---: | ---: | ---: |
| `fio-bigread` | 1022.98 | 0 | 152.04 | 0 |
| `fio-bigwrite` | 0 | 169.14 | 0 | 1199.57 |
| `fio-seqread` | 1653.74 | 0 | 2.90 | 0 |
| `fio-seqwrite` | 0 | 62.54 | 0 | 278.92 |
| `fio-randread` | 450.64 | 0 | 175.11 | 0 |
| `fio-randwrite` | 0 | 64.33 | 0 | 5536.48 |

Conclusion from evidence:

- The old high-backlog writeback profile can make isolated `fio-seqwrite` strong, but it is not acceptable as a full-suite default because committed-but-not-uploaded bytes can accumulate past 17GiB and stall later read-prefill workloads.
- The current default writeback throughput profile is intentionally lower-backlog: 4GiB read/write buffers, 12GiB memory budget, S3/upload concurrency 16, and pending soft/hard 4GiB/6GiB.
- `fio-randrw` must be treated as a first-class gate because earlier rejected writeback experiments caused hangs or severe mixed-workload instability.
- `metaperf` default `-s 4096` is not pure metadata. It measures create plus a 4KiB write and close, so low `create` throughput can be a writeback staging problem. Use `PERF_METAPERF_FILE_SIZE=0` to isolate pure metadata create.
- The next optimization should focus on metadata cache and read-cache scheduling, then re-check all writes and mixed workloads.

## Workload Matrix

The default Redis perf runner supports:

```text
fio-bigwrite fio-bigread fio-seqread fio-seqwrite fio-randread fio-randwrite fio-randrw dirstress dirperf metaperf looptest
```

Fio profile defaults from `/mnt/slayerfs/docker/compose-xfstests/run_perf_in_container.sh`:

| Tool | rw | size | bs | jobs | runtime | prefill | Notes |
| --- | --- | ---: | ---: | ---: | ---: | --- | --- |
| `fio-bigwrite` | `write` | 128 MiB/job | 4 MiB | 8 | finite | no | `end_fsync=1`, refill buffers |
| `fio-bigread` | `read` | 128 MiB/job | 4 MiB | 8 | finite | yes | refill buffers |
| `fio-seqread` | `read` | 1 GiB | 4 MiB | 1 | 60s | yes | time based |
| `fio-seqwrite` | `write` | 1 GiB | 4 MiB | 1 | 60s | no | time based |
| `fio-randread` | `randread` | 512 MiB/job | 4 MiB | 4 | 60s | yes | time based |
| `fio-randwrite` | `randwrite` | 512 MiB/job | 4 MiB | 4 | 60s | no | time based |
| `fio-randrw` | `randrw` | 512 MiB/job | 4 MiB | 4 | 60s | yes | default `rwmixread=70` |
| `fio` | env-configured | default `randrw` | 4 MiB | 4 | 60s | no | custom fallback workload |

Non-fio gates:

| Tool | Purpose | Regression signal |
| --- | --- | --- |
| `dirstress` | concurrent create/unlink/rename directory pressure | tool failure or duration regression |
| `dirperf` | directory operation throughput | ops/sec drop |
| `metaperf` | metadata operation throughput | stat/open/lookup throughput drop |
| `looptest` | repeated create/write/read/remove stability | tool failure |

## Cache-Aware Read/Write Validation

The default fio profiles are useful, but they do not fully describe cold object-store performance. They run through FUSE buffered I/O by default, prefill the same dataset before read tests, and use data sizes that can be smaller than the host/container page-cache budget. These numbers should be interpreted as application-visible hot-path throughput, not as pure S3/object-path throughput.

For mixed workloads, compare only runs with the same warmup sequence. `fio-randrw` is especially sensitive to whether `fio-randread` ran first and warmed BrewFS/Linux caches. In `perf-run-1780682400-14830`, a short `fio-seqwrite fio-randrw` smoke reached only `58.81/27.72 MiB/s` with cache hit `53.4%`; the comparable hot-path smoke `fio-randread fio-randrw` in `perf-run-1780682653-28693` reached `179.56/80.43 MiB/s` with cache hit `99.1%`.

Every accepted performance change must therefore be checked in three layers:

| Layer | Purpose | Required knobs |
| --- | --- | --- |
| Hot buffered path | Preserve the user-visible default behavior and current fio baselines | existing runner defaults |
| Direct/cold read path | Measure object/cache scheduler behavior without Linux page-cache hits dominating the result | `BREWFS_FUSE_READ_DIRECT_IO=true`, `PERF_FIO_*_DIRECT=1`, unique prefill files, dataset larger than the configured cache budget when feasible |
| Writeback drain path | Separate foreground write acceptance from durable upload progress | inspect `.stats` for pending upload bytes, `s3_put_bytes_total`, recent upload bytes, and writeback queue depth after each write tool |

Minimal cold-read smoke command:

```bash
cd /mnt/slayerfs/brewfs
BREWFS_FUSE_READ_DIRECT_IO=true \
PERF_FIO_COLD_READ=true \
PERF_FIO_SEQREAD_DIRECT=1 \
PERF_FIO_RANDREAD_DIRECT=1 \
PERF_FIO_SEQREAD_SIZE=4G \
PERF_FIO_RANDREAD_SIZE=2G \
PERF_FIO_RANDREAD_NUMJOBS=2 \
PERF_FIO_SEQREAD_RUNTIME=20 \
PERF_FIO_RANDREAD_RUNTIME=20 \
bash docker/compose-xfstests/run_redis_perf.sh \
  --writeback-throughput-profile \
  --tools "fio-seqread fio-randread"
```

`PERF_FIO_COLD_READ=true` makes the runner wait for prefill writeback drain, clear the BrewFS local cache root, and remount BrewFS before the read phase. Without this, direct I/O can still report cache hits from BrewFS dirty/writeback memory or local disk cache and show `s3_get_ops_total=0`.

Full cold-read acceptance should raise the dataset above the active memory/cache budget, for example `32G` or `64G` when the runner host has enough disk and time budget. The command above is only a smoke test that exercises direct I/O, drain, and remount behavior.

## Acceptance And Rejection Rules

Every code-level attempt must use the same policy.

Accept and commit a change only if all conditions hold:

- The focused target improves by at least `15%` over the current BrewFS baseline for that same command and config.
- Every fio tool in the full matrix exits with `error=0`.
- No fio throughput metric regresses by more than `10%`.
- No fio p99 latency regresses by more than `25%` unless throughput improves by more than `30%` and the affected workload is not the targeted protected workload.
- `fio-randrw` read throughput, write throughput, read p99, and write p99 all stay within the regression budget.
- `dirstress`, `dirperf`, `metaperf`, and `looptest` pass.
- `brewfs.log` contains no `flush timeout`, read `EIO`, panic, or mount teardown failure.
- `.stats` after each fio tool remains internally sane: cache requests equal hits plus misses, S3 bytes move in the expected direction, and writeback pending bytes do not grow without upload progress.

Reject and revert a change if any condition holds:

- The focused target does not improve by `15%`.
- Any non-target fio workload regresses by more than `10%`.
- `fio-randrw` hangs, fails, or shows a p99 regression above `25%`.
- The change improves one micro-path but causes full-suite instability.
- The change requires weakening correctness semantics without an explicit opt-in config.

Revert rule:

- Use `git diff` and a targeted reverse patch with `apply_patch`.
- Do not use `git reset --hard`.
- Commit only effective code. Keep ineffective experiments documented but not staged.

## Code Analysis

### JuiceFS Read Cache

Reference files:

- `/mnt/slayerfs/brewfs/juicefs/pkg/vfs/reader.go`
- `/mnt/slayerfs/brewfs/juicefs/pkg/chunk/cached_store.go`
- `/mnt/slayerfs/brewfs/juicefs/pkg/chunk/prefetch.go`
- `/mnt/slayerfs/brewfs/juicefs/pkg/chunk/singleflight.go`

Important behavior:

- `rSlice.ReadAt` checks block cache before object storage.
- Small unaligned reads use `loadRange`.
- `loadRange` first tries `TryPiggyback` on an in-flight full-block read.
- A successful range GET schedules full-block prefetch through `fetcher.fetch(key)`.
- Full-block reads use singleflight so concurrent demand readers share one object GET.
- The cache layer records block cache hit/miss counters directly.

### BrewFS Read Cache

Reference files:

- `/mnt/slayerfs/src/vfs/io/reader.rs`
- `/mnt/slayerfs/src/vfs/cache/prefetch.rs`
- `/mnt/slayerfs/src/chunk/reader.rs`
- `/mnt/slayerfs/src/chunk/store.rs`
- `/mnt/slayerfs/src/chunk/cache.rs`
- `/mnt/slayerfs/src/chunk/page_cache.rs`

Current behavior:

- `FileReader::read_at` splits reads into chunk spans and keeps slice state as metadata only.
- Demand data flows through `DataFetcher -> BlockStore`.
- `DataFetcher::read_at` flattens block reads into `FuturesUnordered`, so blocks within a slice run concurrently.
- `ObjectBlockStore::read_range` checks full-block cache first.
- Uncompressed small range reads use page cache and page-level singleflight.
- Large reads use full-block singleflight and then populate full-block cache.
- Range misses can schedule a background full-block prefetch.
- The global prefetcher has a queue and semaphore, but each prefetch task can fan out into multiple span/block reads.

Likely gap:

- BrewFS has the right building blocks, but foreground reads, global prefetch, range prefetch, and full-block background inserts can compete for the same object-store capacity.
- Existing `.stats` exposes cache hit/miss and S3 GET totals, but not enough strategy-level counters to prove whether a read was served by block cache, page cache, range GET, full GET, piggyback, or background prefetch.

### JuiceFS Write Cache

Reference files:

- `/mnt/slayerfs/brewfs/juicefs/pkg/vfs/writer.go`
- `/mnt/slayerfs/brewfs/juicefs/pkg/chunk/cached_store.go`
- `/mnt/slayerfs/brewfs/juicefs/pkg/chunk/disk_cache.go`

Important behavior:

- Writeback stages blocks in local cache and uploads asynchronously.
- Upload concurrency and delayed staging are managed by the cache store.
- Written blocks are also available through the cache layer.
- Commit ordering is separated from object upload.

### BrewFS Write Cache

Reference files:

- `/mnt/slayerfs/src/vfs/io/writer.rs`
- `/mnt/slayerfs/src/vfs/cache/write_back.rs`
- `/mnt/slayerfs/src/vfs/config.rs`
- `/mnt/slayerfs/src/vfs/stats.rs`

Current behavior:

- `CommitBeforeUpload` writeback is implemented and recently tuned.
- Dirty overlay and recently committed overlay preserve read-after-write while upload drains.
- Pending-upload backpressure now has soft and hard limits.
- Current best `fio-seqwrite` evidence is strong, so write cache should be treated as a protected path while read and metadata improvements are attempted.

Likely gap:

- `fio-bigwrite`, `fio-randwrite`, and `fio-randrw` may still expose upload-drain or local staging pressure.
- Further write changes should be attempted only after the full baseline shows a real remaining write bottleneck.

### JuiceFS Metadata Cache

Reference files:

- `/mnt/slayerfs/brewfs/juicefs/pkg/meta/openfile.go`
- `/mnt/slayerfs/brewfs/juicefs/pkg/meta/base.go`
- `/mnt/slayerfs/brewfs/juicefs/pkg/meta/redis.go`
- `/mnt/slayerfs/brewfs/juicefs/pkg/meta/redis_csc.go`

Important behavior:

- `openfiles` caches attr and chunk slices for open files.
- `OpenCheck` can satisfy hot open from local open-file cache.
- `ReadChunk` serves chunk slice metadata from `openfiles`.
- `CacheChunk` stores chunk slices under the open inode.
- `InvalidateChunk` invalidates only affected chunk metadata.
- Redis client-side cache stores inode and directory-entry metadata.
- Redis invalidation uses client tracking / pubsub to prevent long-lived stale data.

### BrewFS Metadata Cache

Reference files:

- `/mnt/slayerfs/src/meta/client/cache.rs`
- `/mnt/slayerfs/src/meta/client/mod.rs`
- `/mnt/slayerfs/src/meta/stores/redis/mod.rs`
- `/mnt/slayerfs/src/vfs/handles.rs`
- `/mnt/slayerfs/src/vfs/fs/mod.rs`

Current behavior:

- `MetaClient` has inode attr cache, children cache, slice cache, path cache, and batch attr prefetch for `opendir`.
- Redis backend has a `node_cache` with 30s TTL and `MGET` batch stat support.
- File handles already cache attr for short handle-local `getattr` avoidance.
- `VFS::open` defaults to `open_with_attr_refresh(..., refresh_attr=true)`, which calls `meta_stat_fresh`.
- `MetaClient::get_slices` caches chunk slices above the Redis store, but the Redis store itself performs `LRANGE` on miss.

Likely gap:

- BrewFS has several caches, but it lacks a JuiceFS-style open-file metadata cache that ties attr and chunk slice reuse to open-file lifetime.
- Redis backend has local node cache, but does not have JuiceFS-style Redis client-side invalidation for entries and attrs across clients.
- The safest next step is not a full Redis CSC port. It is adding metrics and a small open-file scoped metadata cache with clear invalidation.

## Target Plan

### Task 0: Refresh Full Baselines

**Files:**

- No source edits.

- [ ] **Step 1: Pull/build latest images and BrewFS binary**

Run:

```bash
cd /mnt/slayerfs/brewfs
docker compose -f docker/compose-xfstests/docker-compose.redis-perf.yml pull --ignore-pull-failures
bash docker/build_brewfs_host_binary.sh
```

Expected:

- Pull completes or reports only local-build services as skipped.
- `brewfs/target/release/brewfs` exists after the build.

- [ ] **Step 2: Run full BrewFS Redis + S3 profile**

Run:

```bash
cd /mnt/slayerfs/brewfs
bash docker/compose-xfstests/run_redis_perf.sh \
  --writeback-throughput-profile \
  --tools "fio-bigwrite fio-bigread fio-seqread fio-seqwrite fio-randread fio-randwrite fio-randrw dirstress dirperf metaperf looptest"
```

Expected:

- Exit code `0`.
- `perf-summary.tsv` lists every requested tool.
- Every listed tool has `pass`.

- [ ] **Step 3: Run matching JuiceFS reference**

Run:

```bash
cd /mnt/slayerfs/brewfs
bash docker/compose-xfstests/run_juicefs_perf.sh \
  --tools "fio-bigwrite fio-bigread fio-seqread fio-seqwrite fio-randread fio-randwrite fio-randrw dirstress dirperf metaperf looptest"
```

Expected:

- Exit code `0`.
- `perf-summary.tsv` lists every requested tool.
- Every listed tool has `pass`.

- [ ] **Step 4: Extract fio numbers from both artifacts**

Run:

```bash
cd /mnt/slayerfs/brewfs
for root in \
  "$(ls -1dt docker/compose-xfstests/artifacts/perf-run-* | head -1)" \
  "$(ls -1dt docker/compose-xfstests/artifacts/juicefs-perf-run-* | head -1)"
do
  echo "== $root"
  for f in "$root"/results/fio-*.json; do
    tool="$(basename "$f" .json)"
    jq -r --arg tool "$tool" '
      .jobs[0] as $j |
      [
        $tool,
        ($j.error // 0),
        (($j.read.bw_bytes // 0) / 1048576),
        (($j.write.bw_bytes // 0) / 1048576),
        (($j.read.clat_ns.percentile."99.000000" // 0) / 1000000),
        (($j.write.clat_ns.percentile."99.000000" // 0) / 1000000)
      ] | @tsv
    ' "$f"
  done | sort
done
```

Expected:

- Every fio row has `error` equal to `0`.
- Record the artifact names and numbers at the top of the execution notes.

### Task 1: Add Read And Metadata Observability

**Files:**

- Modify: `/mnt/slayerfs/src/chunk/store.rs`
- Modify: `/mnt/slayerfs/src/vfs/stats.rs`
- Modify: `/mnt/slayerfs/src/vfs/fs/mod.rs`
- Modify: `/mnt/slayerfs/src/meta/client/mod.rs`
- Test: existing unit tests under `src/chunk/store.rs`, `src/vfs/stats.rs`, and `src/meta/client/mod.rs`

- [x] **Step 1: Add read strategy counters**

Add counters to `ObjectStoreMetrics` for:

```text
read_block_cache_hits
read_page_cache_hits
read_page_cache_misses
read_range_gets
read_full_gets
read_piggyback_full
read_background_prefetches
read_background_prefetch_dropped
```

Expected:

- `ObjectBlockStore::read_range` increments exactly one primary strategy counter per read request.
- Background prefetch increments the background counters separately.

- [x] **Step 2: Export counters in `.stats`**

Expose the counters as:

```text
brewfs_read_block_cache_hits_total
brewfs_read_page_cache_hits_total
brewfs_read_page_cache_misses_total
brewfs_read_range_gets_total
brewfs_read_full_gets_total
brewfs_read_piggyback_full_total
brewfs_read_background_prefetch_total
brewfs_read_background_prefetch_dropped_total
```

Expected:

- `stats.rs` unit tests assert the new lines exist.
- Existing `.stats` cache metrics still pass.

- [x] **Step 3: Add metadata hit/miss counters**

Add counters for:

```text
meta_stat_cache_hit
meta_stat_cache_miss
meta_stat_fresh_store_hit
meta_lookup_cache_hit
meta_lookup_cache_miss
meta_get_slices_cache_hit
meta_get_slices_cache_miss
meta_open_fresh_stat
```

Expected:

- `cached_stat`, `cached_lookup`, `get_slices`, and `open_with_attr_refresh` update counters.
- `.stats` exposes counters under `brewfs_meta_*`.

- [x] **Step 4: Verify observability**

Run:

```bash
cd /mnt/slayerfs/brewfs
cargo test -p brewfs vfs::stats --lib
cargo test -p brewfs chunk::store --lib
cargo test -p brewfs meta::client --lib
bash docker/compose-xfstests/run_redis_perf.sh \
  --writeback-throughput-profile \
  --tools "fio-seqread fio-randread metaperf"
```

Expected:

- Unit tests pass.
- Perf run exits `0`.
- `diagnostics/stats-*-after.txt` includes both read strategy and metadata counters.

- [x] **Step 5: Commit if accepted**

Run:

```bash
git add \
  /mnt/slayerfs/src/chunk/store.rs \
  /mnt/slayerfs/src/vfs/stats.rs \
  /mnt/slayerfs/src/vfs/fs/mod.rs \
  /mnt/slayerfs/src/meta/client/mod.rs
git commit -m "perf: add cache and metadata path counters"
```

### Task 2: Metadata Open-File Scoped Cache

**Files:**

- Modify: `/mnt/slayerfs/src/meta/client/cache.rs`
- Modify: `/mnt/slayerfs/src/meta/client/mod.rs`
- Modify: `/mnt/slayerfs/src/vfs/fs/mod.rs`
- Modify: `/mnt/slayerfs/src/vfs/handles.rs`
- Test: metadata cache unit tests and VFS open/getattr tests

- [ ] **Step 1: Add open-file cache structure**

Add a per-inode structure equivalent in scope to JuiceFS `openfiles`:

```text
ino
refs
last_check
attr
first_chunk_slices
chunk_slices_by_index
```

Expected:

- The cache is bounded by count and TTL.
- TTL defaults to disabled unless a perf config enables it.
- Existing inode cache remains the source of truth for regular cached stat.

- [ ] **Step 2: Wire open and close lifecycle**

Change `VFS::open_with_attr_refresh` and close handling so:

- read-only open may reuse a fresh open-file cache entry when the opt-in TTL is active.
- write open still refreshes attr unless a same-process dirty overlay proves the local size is newer.
- close decrements the open-file ref count.
- truncate, setattr, unlink, rename, write commit, and explicit slice invalidation invalidate affected open-file cache entries.

Expected:

- Close-to-open semantics remain the default when the opt-in TTL is not set.
- The perf profile can enable a short open-cache TTL for hot metadata workloads.

- [ ] **Step 3: Cache chunk slices under open files**

Change `MetaClient::get_slices` so:

- If an inode/chunk mapping is known through the open-file cache, serve slices from the open-file cache.
- On store miss, populate both inode slice cache and open-file cache.
- On write/truncate/compact, invalidate only the affected chunk index when possible.

Expected:

- Hot reads of the same open file reduce Redis `LRANGE` calls.
- Compaction and overwrite paths do not serve stale slice lists.

- [ ] **Step 4: Verify focused metadata performance**

Run:

```bash
cd /mnt/slayerfs/brewfs
cargo test -p brewfs meta::client --lib
cargo test -p brewfs vfs::handles --lib
bash docker/compose-xfstests/run_redis_perf.sh \
  --writeback-throughput-profile \
  --tools "metaperf dirperf fio-randrw"
```

Expected:

- Unit tests pass.
- `metaperf` improves by at least `15%` over the refreshed BrewFS baseline.
- `dirperf` does not regress by more than `10%`.
- `fio-randrw` does not regress by more than `10%` throughput or more than `25%` p99 latency.

- [ ] **Step 5: Run full regression gate**

Run:

```bash
cd /mnt/slayerfs/brewfs
bash docker/compose-xfstests/run_redis_perf.sh \
  --writeback-throughput-profile \
  --tools "fio-bigwrite fio-bigread fio-seqread fio-seqwrite fio-randread fio-randwrite fio-randrw dirstress dirperf metaperf looptest"
```

Expected:

- Every tool passes.
- Acceptance rules pass for all fio and non-fio workloads.

- [ ] **Step 6: Commit if accepted**

Run:

```bash
git add \
  /mnt/slayerfs/src/meta/client/cache.rs \
  /mnt/slayerfs/src/meta/client/mod.rs \
  /mnt/slayerfs/src/vfs/fs/mod.rs \
  /mnt/slayerfs/src/vfs/handles.rs
git commit -m "perf: add open-file metadata cache"
```

### Task 3: Foreground Read Priority

**Files:**

- Modify: `/mnt/slayerfs/src/vfs/cache/prefetch.rs`
- Modify: `/mnt/slayerfs/src/vfs/io/reader.rs`
- Modify: `/mnt/slayerfs/src/chunk/store.rs`
- Test: prefetch scheduler tests and chunk store read tests

- [ ] **Step 1: Add prefetch pressure signal**

Add a cheap read-pressure signal based on:

```text
foreground full/range GETs in flight
global prefetch queue depth
memory pressure level
background full-block prefetch permits
```

Expected:

- Demand reads do not wait behind background prefetch permits.
- `.stats` shows when prefetch was dropped or delayed.

- [ ] **Step 2: Gate global prefetch under foreground pressure**

Change `DataReader::submit_prefetch` so:

- Critical memory pressure still drops prefetch.
- High foreground object-read pressure limits each handle to at most one queued prefetch.
- Sequential reads keep at least one block of prefetch when pressure is normal.

Expected:

- `fio-seqread` keeps warmup.
- `fio-randread` avoids background work amplification.

- [ ] **Step 3: Gate range-triggered full-block prefetch**

Change `ObjectBlockStore::prefetch_full_block_background` so:

- It skips full-block background prefetch if the block is already hot.
- It skips when background permits are exhausted instead of queueing indefinitely.
- It increments the dropped counter when skipped due to pressure.

Expected:

- Foreground range reads do not create unbounded follow-on full reads.
- JuiceFS-style range-to-full-block warmup remains active under normal pressure.

- [ ] **Step 4: Verify read-focused workloads**

Run:

```bash
cd /mnt/slayerfs/brewfs
cargo test -p brewfs vfs::cache::prefetch --lib
cargo test -p brewfs chunk::store --lib
bash docker/compose-xfstests/run_redis_perf.sh \
  --writeback-throughput-profile \
  --tools "fio-bigread fio-seqread fio-randread fio-randrw"
```

Expected:

- `fio-bigread`, `fio-seqread`, or `fio-randread` improves by at least `15%`.
- `fio-randrw` remains within the regression budget.

- [ ] **Step 5: Run full regression gate**

Run the full gate command from Task 2 Step 5.

Expected:

- Every tool passes.
- No write workload regresses by more than `10%`.

- [ ] **Step 6: Commit if accepted**

Run:

```bash
git add \
  /mnt/slayerfs/src/vfs/cache/prefetch.rs \
  /mnt/slayerfs/src/vfs/io/reader.rs \
  /mnt/slayerfs/src/chunk/store.rs
git commit -m "perf: prioritize foreground reads over background prefetch"
```

### Task 4: Read Cache Reuse And RandRW Protection

**Files:**

- Modify: `/mnt/slayerfs/src/chunk/store.rs`
- Modify: `/mnt/slayerfs/src/chunk/cache.rs`
- Modify: `/mnt/slayerfs/src/chunk/page_cache.rs`
- Test: chunk cache and page cache tests

- [ ] **Step 1: Analyze counters after Task 3**

Use the `.stats` counters from read-focused and full runs.

Expected:

- Identify whether misses are dominated by `full_get`, `range_get`, `page_cache_miss`, or dropped background prefetch.
- If the counters do not point to one dominant miss mode, stop and collect tracing before editing code.

- [ ] **Step 2: Improve only the dominant miss mode**

Pick exactly one change:

- If `page_cache_miss` dominates small reads, tune page cache admission or page size.
- If `full_get` dominates repeated reads, tune `insert_opportunistic` and hot-cache promotion.
- If background prefetch is mostly dropped, tune the prefetch limit upward only when `fio-randrw` is stable.

Expected:

- One hypothesis per code attempt.
- No simultaneous page cache, full cache, and prefetch changes in the same commit.

- [ ] **Step 3: Verify random and mixed workloads first**

Run:

```bash
cd /mnt/slayerfs/brewfs
bash docker/compose-xfstests/run_redis_perf.sh \
  --writeback-throughput-profile \
  --tools "fio-randread fio-randwrite fio-randrw"
```

Expected:

- Targeted random workload improves by at least `15%`.
- Other random workloads stay within regression budget.
- `fio-randrw` does not hang.

- [ ] **Step 4: Run full regression gate**

Run the full gate command from Task 2 Step 5.

Expected:

- Every tool passes.
- Read improvement remains visible in the full run.

- [ ] **Step 5: Commit if accepted**

Run:

```bash
git add \
  /mnt/slayerfs/src/chunk/store.rs \
  /mnt/slayerfs/src/chunk/cache.rs \
  /mnt/slayerfs/src/chunk/page_cache.rs
git commit -m "perf: improve read cache reuse for random workloads"
```

### Task 5: Writeback Mixed-Workload Follow-Up

**Files:**

- Modify only if evidence requires it: `/mnt/slayerfs/src/vfs/io/writer.rs`
- Modify only if evidence requires it: `/mnt/slayerfs/src/vfs/cache/write_back.rs`
- Modify only if evidence requires it: `/mnt/slayerfs/src/vfs/config.rs`
- Test: writer and writeback tests

- [ ] **Step 1: Compare writeback counters from full gate**

Inspect:

```text
brewfs_writeback_dirty_bytes
brewfs_writeback_live_dirty_bytes
brewfs_writeback_recent_pending_upload_bytes
brewfs_writeback_recent_uploaded_bytes
brewfs_s3_put_bytes_total
brewfs_s3_put_ops_total
```

Expected:

- If `fio-seqwrite` remains near the accepted `~1.5 GiB/s` profile and `fio-randrw` is stable, do not change writeback.
- If `fio-bigwrite`, `fio-randwrite`, or `fio-randrw` shows a real write bottleneck, continue.

- [ ] **Step 2: Choose one write hypothesis**

Pick exactly one:

- Pending-upload gate is too strict for multi-job finite writes.
- Upload concurrency is starving mixed reads.
- Local writeback staging fsync cost dominates finite `bigwrite`.

Expected:

- The change is config-gated if it changes durability or ordering behavior.
- Safe defaults remain unchanged.

- [ ] **Step 3: Verify write and mixed workloads**

Run:

```bash
cd /mnt/slayerfs/brewfs
bash docker/compose-xfstests/run_redis_perf.sh \
  --writeback-throughput-profile \
  --tools "fio-bigwrite fio-seqwrite fio-randwrite fio-randrw"
```

Expected:

- Target write workload improves by at least `15%`.
- `fio-randrw` stays within regression budget.
- `fio-seqread` is checked in the full gate before commit.

- [ ] **Step 4: Run full regression gate**

Run the full gate command from Task 2 Step 5.

Expected:

- Every tool passes.
- No read workload regresses by more than `10%`.

- [ ] **Step 5: Commit if accepted**

Run:

```bash
git add \
  /mnt/slayerfs/src/vfs/io/writer.rs \
  /mnt/slayerfs/src/vfs/cache/write_back.rs \
  /mnt/slayerfs/src/vfs/config.rs
git commit -m "perf: tune writeback mixed workload behavior"
```

### Task 6: Redis Client-Side Cache Design Gate

**Files:**

- Analyze: `/mnt/slayerfs/brewfs/juicefs/pkg/meta/redis_csc.go`
- Analyze: `/mnt/slayerfs/src/meta/stores/redis/mod.rs`
- Modify only after approval: `/mnt/slayerfs/src/meta/stores/redis/mod.rs`

- [ ] **Step 1: Decide whether Redis CSC is still needed**

Use metadata counters after Task 2.

Expected:

- If `meta_stat_fresh_store_hit` and Redis `node_cache` handle hot stat/open locally, do not implement Redis CSC yet.
- If `lookup` and `stat_fresh` still issue Redis RTTs under `metaperf`, prepare Redis CSC design.

- [ ] **Step 2: Write a separate Redis CSC design before implementation**

Required design properties:

- RESP tracking or explicit pubsub invalidation.
- Entry term invalidation for directory children.
- Attr invalidation on node writes.
- Reconnect clears local cache.
- Multi-client correctness test.

Expected:

- Redis CSC is not mixed into read-cache or open-file-cache commits.
- The design gets reviewed before code is written.

## Full Gate Command

Use this before every performance commit:

```bash
cd /mnt/slayerfs/brewfs
bash docker/compose-xfstests/run_redis_perf.sh \
  --writeback-throughput-profile \
  --tools "fio-bigwrite fio-bigread fio-seqread fio-seqwrite fio-randread fio-randwrite fio-randrw dirstress dirperf metaperf looptest"
```

Use this to summarize fio output:

```bash
cd /mnt/slayerfs/brewfs
root="$(ls -1dt docker/compose-xfstests/artifacts/perf-run-* | head -1)"
echo "artifact=$root"
sed -n '1,80p' "$root/perf-summary.tsv"
for f in "$root"/results/fio-*.json; do
  tool="$(basename "$f" .json)"
  jq -r --arg tool "$tool" '
    .jobs[0] as $j |
    [
      $tool,
      ($j.error // 0),
      (($j.read.bw_bytes // 0) / 1048576),
      (($j.write.bw_bytes // 0) / 1048576),
      (($j.read.clat_ns.percentile."99.000000" // 0) / 1000000),
      (($j.write.clat_ns.percentile."99.000000" // 0) / 1000000)
    ] | @tsv
  ' "$f"
done | sort
```

## Execution Order

1. Refresh full BrewFS and JuiceFS baselines.
2. Add observability counters and commit them only if the focused perf smoke passes.
3. Implement open-file scoped metadata cache.
4. Implement foreground read priority.
5. Improve one read cache miss mode based on counters.
6. Revisit writeback only if the refreshed full matrix proves a remaining write or `randrw` bottleneck.
7. Consider Redis CSC only after metadata counters show the smaller open-file cache is insufficient.

## Stop Conditions

Stop and write a blocker note before the next attempt if:

- Three consecutive code attempts fail the acceptance rule.
- `fio-randrw` hangs twice under the same hypothesis.
- Full gate variance exceeds `20%` between two no-code-change runs.
- The next improvement requires changing correctness semantics without an opt-in config.

## Self-Review

- Spec coverage: The plan covers read cache, write cache, metadata cache, JuiceFS comparison, full workload testing, `randrw`, regression gates, commit rules, and revert rules.
- Placeholder scan: The plan contains no placeholder implementation steps.
- Type consistency: File names, Rust counter field names, and exported `.stats` metric names are consistent within the plan.
