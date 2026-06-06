# BrewFS Redis metadata next performance plan

Date: 2026-06-06

## Current baseline

Latest effective change: `5b9afc4 perf: add opt-in open file metadata cache`.

Final implementation result on 2026-06-06:

- Baseline artifact: `brewfs/docker/compose-xfstests/artifacts/perf-run-1780752406-2679`
- Candidate artifact: `brewfs/docker/compose-xfstests/artifacts/perf-run-1780754910-22164`
- Candidate command: `bash brewfs/docker/compose-xfstests/run_redis_perf.sh`
- Candidate script exit: 0
- `metaperf create`: 236.905765 -> 241.019743 ops/s, +1.74%
- `metaperf open`: 10044.462945 -> 10175.593116 ops/s, +1.31%
- `metaperf stat`: 1023299.984465 -> 1015498.657925 ops/s, -0.76%
- `metaperf readdir`: 66337.993490 -> 67664.706298 ops/s, +2.00%
- `metaperf rename`: 1984.088049 -> 2123.408809 ops/s, +7.02%
- `fio-randrw` read: 76.250323 -> 96.833272 MiB/s
- `fio-randrw` write: 34.724021 -> 44.553961 MiB/s
- `fio-randrw` read p99: 3271.557120 -> 1820.327936 ms
- `fio-randrw` write p99: 1920.991232 -> 254.803968 ms
- `brewfs_meta_stat_cache_miss_total`: 6062 -> 117
- `brewfs_meta_lookup_attr_fused_miss_total`: 0 -> 85083
- `brewfs_meta_lookup_attr_fused_hit_total`: 0 -> 45888
- `brewfs_meta_lookup_attr_fused_error_total`: 0 -> 0

The first candidate attempted to route lookup-only callers through the fused Lua path too, which raised Redis `evalsha` calls to 306440 during metaperf. The final implementation narrows the optimization to attr-needed callers and keeps lookup-only callers on the lighter inode-only path.

Full perf run with open-file metadata cache enabled:

- Artifact: `brewfs/docker/compose-xfstests/artifacts/perf-run-1780738990-10798`
- `metaperf create`: 767.1 ops/s
- `metaperf open`: 10386.6 ops/s
- `metaperf stat`: 1025706.8 ops/s
- `metaperf readdir`: 66580.8 ops/s
- `metaperf rename`: 2157.6 ops/s
- `fio-randrw` read: 206.27 MiB/s
- `fio-randrw` write: 92.59 MiB/s
- `fio-randrw` read p99: 42.205 ms
- `fio-randrw` write p99: 86.508 ms

Paired TTL=0 control:

- Artifact: `brewfs/docker/compose-xfstests/artifacts/perf-run-1780739386-14395`
- `metaperf create`: 767.6 ops/s
- `metaperf open`: 10169.3 ops/s
- `metaperf stat`: 1024816.6 ops/s
- `metaperf readdir`: 66813.5 ops/s
- `metaperf rename`: 2164.6 ops/s

Open-file cache effect:

- `brewfs_meta_open_fresh_stat_total`: 363201 -> 31401
- Fresh open stat load reduction: about 91.4%
- `metaperf open`: 10169.3 -> 10386.6 ops/s, about +2.1%

Conclusion: open-file metadata cache removed most repeated fresh `stat` traffic from open, but it is not the main remaining throughput limiter.

## Remaining bottlenecks

### 1. Lookup returns inode only in BrewFS

BrewFS Redis lookup currently maps to `directory_child(parent, name)` and returns only the child inode. When the caller needs attributes, the upper layer must call `cached_stat(ino)` afterwards.

Relevant BrewFS paths:

- `brewfs/src/meta/stores/redis/mod.rs`: `lookup -> directory_child`
- `brewfs/src/meta/client/mod.rs`: `lookup_path_with_attr -> resolve_path -> cached_stat`
- `brewfs/src/meta/client/mod.rs`: FUSE-facing lookup paths commonly perform child lookup plus attr fetch

JuiceFS does this differently. Its Redis `scriptLookup` performs `HGET` on the directory entry and `GET` on the inode attribute inside one Redis script, returning both inode and attr to `baseMeta.Lookup`.

Relevant JuiceFS paths:

- `brewfs/juicefs/pkg/meta/lua_scripts.go`: `scriptLookup`
- `brewfs/juicefs/pkg/meta/redis.go`: `doLookup`
- `brewfs/juicefs/pkg/meta/base.go`: `Lookup`

Expected BrewFS issue: cache misses in `lookup` paths still pay extra async steps and often extra Redis network round trips before attr is available.

### 2. Rename pre-validation duplicates store work

BrewFS `MetaClient::rename` currently does:

- `cached_lookup_required(old_parent, old_name)`
- `cached_stat(src_ino)`
- `ensure_directory_exists(new_parent)`
- `cached_lookup(new_parent, new_name)`
- `store.rename(...)`

Redis `store.rename` then runs `RENAME_LUA`, which already checks source, destination, parent directory state, and directory emptiness. This gives correctness, but on hot rename microbenchmarks it likely duplicates metadata reads before the atomic Lua path.

Expected BrewFS issue: `metaperf rename` is still low and likely pays several cache/raw-store operations before the actual Redis transaction.

### 3. Create benchmark mixes metadata and small object PUTs

`metaperf create` writes small files, so the create result is not pure metadata. In the latest run, Redis commandstats and S3 PUT counters both move heavily. This means create optimization must be judged with both metadata-only and data-write scenarios:

- metadata-only create/open/stat/rename/readdir
- `dirperf`
- `fio-randrw`
- S3 PUT/GET/HEAD/delete counters

## Next implementation target

Primary target: add a fused lookup-with-attr path, modeled after JuiceFS `scriptLookup`.

The goal is to make the common lookup path return `(ino, FileAttr)` without forcing the caller through separate `lookup` then `stat` stages when the attr is needed immediately.

Proposed shape:

1. Add `MetaStore::lookup_with_attr(parent, name) -> Result<Option<(i64, FileAttr)>, MetaError>`.
2. Provide a default implementation for non-Redis stores using existing `lookup + stat`.
3. Add a Redis override using a small Lua script:
   - `HGET dir:<parent> name`
   - if missing: return nil/ENOENT-compatible response
   - `GET node:<ino>`
   - parse `StoredNode`
   - populate the Redis raw node cache
4. Add `MetaLayer::lookup_with_attr` or a narrower MetaClient helper.
5. Wire FUSE/VFS lookup callers that immediately need attrs to use the fused path.
6. Update MetaClient inode/dir caches from the returned attr so later `stat` stays hot.
7. Add counters:
   - `brewfs_meta_lookup_attr_fused_hit_total`
   - `brewfs_meta_lookup_attr_fused_miss_total`
   - `brewfs_meta_lookup_attr_fused_error_total`
8. Keep existing `lookup` behavior for callers that only need inode.

## Test plan

Unit tests:

- Redis store test: lookup-with-attr returns inode and attr for existing child.
- Redis store test: missing child returns `Ok(None)`.
- Redis store test: stale directory entry with missing node returns a clear `MetaError::NotFound`.
- MetaClient test: fused lookup populates inode stat cache.
- Cache invalidation test: rename/unlink/setattr/truncate invalidates fused attr state correctly.

Static checks:

- `cargo fmt`
- `cargo clippy -p brewfs --all-targets -- -D warnings`
- `cargo test -p brewfs meta::client --lib`
- `cargo test -p brewfs meta::stores::redis --lib`
- `cargo test -p brewfs vfs --lib` or narrower VFS lookup/open tests if full VFS is too slow
- `git diff --check`

Perf validation:

Run paired perf tests with the same profile and environment:

- Baseline before code: `bash brewfs/docker/compose-xfstests/run_redis_perf.sh`
- Candidate after code: `bash brewfs/docker/compose-xfstests/run_redis_perf.sh`
- Include all current scenarios: `metaperf`, `dirperf`, `fio-randrw`
- Capture `.stats` and Redis commandstats for every run

Success threshold:

- `metaperf open`: at least +3% versus paired baseline, or clear Redis round-trip reduction with neutral throughput
- `metaperf stat`: no regression greater than 3%
- `metaperf create`: no regression greater than 3%
- `metaperf rename`: no regression greater than 3%
- `metaperf readdir`: no regression greater than 3%
- `dirperf`: no regression greater than 5%
- `fio-randrw` read/write throughput: no regression greater than 10%
- `fio-randrw` p99 latency: no regression greater than 25% unless repeated baseline variance shows the same behavior

Rollback rule:

- If fused lookup produces no measurable improvement and increases any major scenario beyond the regression threshold, revert the code change.
- If fused lookup improves lookup/open but hurts randrw p99, keep the implementation behind an opt-in config and do not enable it in the default perf profile until a second run confirms stability.

## Follow-up target if lookup fusion is insufficient

Secondary target: Redis rename checked path.

Plan:

1. Add a store-level operation that returns rename side effects from Redis Lua:
   - source inode
   - overwritten destination inode, if any
   - source/destination parent changed timestamps
2. Let `MetaClient::rename` avoid redundant `cached_stat(src_ino)` and destination lookup when the store can return this information atomically.
3. Preserve generic-store behavior with the existing conservative pre-validation path.
4. Validate specifically against `metaperf rename`, directory overwrite tests, hardlink rename tests, and cross-directory rename tests.

Success threshold:

- `metaperf rename`: at least +5%
- No correctness regression in Redis rename tests
- No regression greater than 3% in create/open/stat/readdir
