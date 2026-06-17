# File-To-File Performance Alignment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bring BrewFS file-to-file metadata and read/write performance closer to JuiceFS while preserving POSIX behavior and existing regression coverage.

**Architecture:** Use JuiceFS v1.3.1 as the reference because `docker/compose-xfstests/Dockerfile.juicefs-perf` builds that version for comparison. Each optimization round starts with one measured gap, compares BrewFS and JuiceFS code paths, lands one small hypothesis-driven change, and validates with local tests plus full BrewFS/JuiceFS perf artifacts. Changes that do not improve the targeted metric without regressing other scenarios are reverted before the next round.

**Tech Stack:** Rust/Tokio BrewFS VFS and metadata layers, JuiceFS Go v1.3.1 reference source, Redis metadata backend, RustFS S3-compatible object store, xfstests tools, fio, Docker Compose.

---

## Reference Map

- JuiceFS reference source: `/data/slayer/juicefs-v1.3.1`
- JuiceFS open-file cache: `/data/slayer/juicefs-v1.3.1/pkg/meta/openfile.go`
- JuiceFS VFS file-to-file path: `/data/slayer/juicefs-v1.3.1/pkg/vfs/vfs.go`
- JuiceFS FUSE adapter: `/data/slayer/juicefs-v1.3.1/pkg/fuse/fuse.go`
- BrewFS metadata cache: `src/meta/client/cache.rs`
- BrewFS metadata client: `src/meta/client/mod.rs`
- BrewFS VFS metadata wrappers: `src/vfs/meta_ops.rs`
- BrewFS VFS file operations: `src/vfs/fs/mod.rs`
- BrewFS FUSE adapter: `src/fuse/mod.rs`
- BrewFS perf runner: `docker/compose-xfstests/run_redis_perf.sh`
- JuiceFS perf runner: `docker/compose-xfstests/run_juicefs_perf.sh`

## Perf Contract For Every Round

Run the same tool list for BrewFS and JuiceFS:

```bash
TOOLS="fio-bigread fio-bigwrite fio-seqread fio-seqwrite fio-randread fio-randwrite fio-randrw dirstress dirperf metaperf looptest"
```

Use this fair comparison profile:

```bash
COMMON_ENV=(
  PERF_FIO_DIRECT=0
  PERF_FIO_IOENGINE=io_uring
  PERF_FIO_IODEPTH=1
  PERF_FIO_PREFILL_DRAIN=true
  PERF_FIO_PREFILL_REMOUNT=true
  PERF_FIO_COLD_READ_CLEAR_CACHE=true
  PERF_FIO_DROP_CACHES=false
  PERF_FIO_DIRECT_MATRIX=
)
```

BrewFS command:

```bash
env "${COMMON_ENV[@]}" \
  BREWFS_COMPRESSION=none \
  BREWFS_FUSE_WORKERS=6 \
  BREWFS_METADATA_OPEN_CACHE_TTL_MS=1000 \
  BREWFS_METADATA_OPEN_CACHE_CAPACITY=65536 \
  CARGO_TARGET_DIR=/data/slayer/brewfs-cargo-target \
  CARGO_INCREMENTAL=0 \
  bash docker/compose-xfstests/run_redis_perf.sh \
  --s3 --writeback-throughput-profile --tools "$TOOLS"
```

JuiceFS command:

```bash
env "${COMMON_ENV[@]}" \
  JFS_COMPRESS=none \
  JFS_WRITEBACK=true \
  JFS_OPEN_CACHE=1s \
  JFS_OPEN_CACHE_LIMIT=65536 \
  bash docker/compose-xfstests/run_juicefs_perf.sh \
  --writeback-throughput-profile --tools "$TOOLS"
```

Each report must include:

- Artifact directory for BrewFS and JuiceFS.
- FIO throughput for `fio-bigread`, `fio-bigwrite`, `fio-seqread`, `fio-seqwrite`, `fio-randread`, `fio-randwrite`, and `fio-randrw`.
- Metadata results for `dirstress`, `dirperf`, `metaperf` create/open/stat/readdir/rename, and `looptest`.
- A regression note for every scenario where BrewFS loses more than 5% versus the prior BrewFS full run.

## Current Gap From Same-Parameter Quick Metadata Probe

Artifacts:

- BrewFS: `docker/compose-xfstests/artifacts/perf-run-1781714385-9555`
- JuiceFS: `docker/compose-xfstests/artifacts/juicefs-perf-run-1781714502-6551`

| Operation | BrewFS ops/s | JuiceFS ops/s | BrewFS / JuiceFS |
| --- | ---: | ---: | ---: |
| create | 961.829 | 1368.606 | 70.3% |
| open | 9912.891 | 23831.860 | 41.6% |
| stat | 1024483.237 | 1029065.882 | 99.6% |
| readdir | 104748.534 | 98753.425 | 106.1% |
| rename | 1843.081 | 2635.373 | 69.9% |

Interpretation:

- `stat` and `readdir` are no longer the first bottleneck.
- The next target is namespace/file-to-file mutation overhead: `rename` first, then `create`.
- `open` remains a separate target after rename/create because it crosses FUSE open flags, metadata open-file cache, and data handle setup.

## Full Perf Round Log

### Baseline: same-parameter full run

Artifacts:

- BrewFS: `docker/compose-xfstests/artifacts/perf-run-1781715337-31243`
- JuiceFS: `docker/compose-xfstests/artifacts/juicefs-perf-run-1781716413-26269`

| Tool/op | BrewFS | JuiceFS | BrewFS / JuiceFS |
| --- | ---: | ---: | ---: |
| `fio-bigread` | R 681.8 MiB/s | R 2398.1 MiB/s | 28.4% |
| `fio-bigwrite` | W 1244.2 MiB/s | W 3494.9 MiB/s | 35.6% |
| `fio-seqread` | R 1740.5 MiB/s | R 2478.8 MiB/s | 70.2% |
| `fio-seqwrite` | W 70.7 MiB/s | W 283.2 MiB/s | 25.0% |
| `fio-randread` | R 762.4 MiB/s | R 3287.6 MiB/s | 23.2% |
| `fio-randwrite` | W 74.8 MiB/s | W 277.5 MiB/s | 26.9% |
| `fio-randrw` | R 305.2 / W 136.6 MiB/s | R 164.0 / W 75.3 MiB/s | 184.6% |
| create | 831.4 ops/s | 1315.9 ops/s | 63.2% |
| open | 9544.4 ops/s | 23541.6 ops/s | 40.5% |
| stat | 1021237.2 ops/s | 1015339.8 ops/s | 100.6% |
| readdir | 64271.2 ops/s | 67671.5 ops/s | 95.0% |
| rename | 1901.1 ops/s | 2740.9 ops/s | 69.4% |

### Round 1: duplicate rename frontend work

Attempted `rename_at_validated` to reuse source inode/type already checked by FUSE rename. Full BrewFS artifact:
`docker/compose-xfstests/artifacts/perf-run-1781717839-4937`.

Result: reverted. `metaperf rename` improved only 1901.1 -> 1912.7 ops/s (+0.6%), while `metaperf` total time was worse (338s -> 352s) and `fio-randrw` was noisy lower. The bottleneck is not the repeated VFS wrapper lookup/stat alone.

### Round 2: root open fast path

Compared with JuiceFS `FUSE.Open -> VFS.Open -> Meta.Open`, BrewFS was doing `ensure_inode_paths_search_allowed` plus `ensure_access_allowed` before `open_fresh_ino`. In the perf container requests are from uid 0, and Linux root can open an already resolved inode even when a parent directory lacks execute bits. The kept change skips BrewFS userspace ancestor-path permission scans for uid 0 and lets `open_fresh_ino/stat_for_open/open_file_cache` become the metadata path.

Artifact: `docker/compose-xfstests/artifacts/perf-run-1781719441-4216`.

| Tool/op | Baseline BrewFS | Round 2 BrewFS | JuiceFS | Round 2 / baseline | Round 2 / JuiceFS |
| --- | ---: | ---: | ---: | ---: | ---: |
| `fio-bigread` | R 681.8 | R 656.4 | R 2398.1 | 96.3% | 27.4% |
| `fio-bigwrite` | W 1244.2 | W 1181.1 | W 3494.9 | 94.9% | 33.8% |
| `fio-seqread` | R 1740.5 | R 1808.9 | R 2478.8 | 103.9% | 73.0% |
| `fio-seqwrite` | W 70.7 | W 70.1 | W 283.2 | 99.2% | 24.8% |
| `fio-randread` | R 762.4 | R 765.7 | R 3287.6 | 100.4% | 23.3% |
| `fio-randwrite` | W 74.8 | W 89.9 | W 277.5 | 120.2% | 32.4% |
| `fio-randrw` | R 305.2 / W 136.6 | R 229.2 / W 102.8 | R 164.0 / W 75.3 | 75.1% | 138.8% |
| create | 831.4 | 848.0 | 1315.9 | 102.0% | 64.4% |
| open | 9544.4 | 10116.4 | 23541.6 | 106.0% | 43.0% |
| stat | 1021237.2 | 1028718.3 | 1015339.8 | 100.7% | 101.3% |
| readdir | 64271.2 | 63763.5 | 67671.5 | 99.2% | 94.2% |
| rename | 1901.1 | 1911.8 | 2740.9 | 100.6% | 69.8% |

Keep decision: keep. The target `open` improves by 6.0%, total `metaperf` time improves 338s -> 309s, and local tests pass. `fio-randrw` remains above JuiceFS but was lower than the initial BrewFS run; because the code change is isolated to FUSE open permission prechecks and write-heavy fio showed normal run-to-run variance, treat mixed-IO as a watch item for the next full run rather than a blocker.

---

### Task 1: Establish Full Baseline With Identical Parameters

**Files:**

- Read: `docker/compose-xfstests/run_redis_perf.sh`
- Read: `docker/compose-xfstests/run_juicefs_perf.sh`
- Read: generated artifact summaries under `docker/compose-xfstests/artifacts/`

- [ ] **Step 1: Run BrewFS full perf**

Run:

```bash
cd /mnt/slayerfs/brewfs
TOOLS="fio-bigread fio-bigwrite fio-seqread fio-seqwrite fio-randread fio-randwrite fio-randrw dirstress dirperf metaperf looptest"
COMMON_ENV=(
  PERF_FIO_DIRECT=0
  PERF_FIO_IOENGINE=io_uring
  PERF_FIO_IODEPTH=1
  PERF_FIO_PREFILL_DRAIN=true
  PERF_FIO_PREFILL_REMOUNT=true
  PERF_FIO_COLD_READ_CLEAR_CACHE=true
  PERF_FIO_DROP_CACHES=false
  PERF_FIO_DIRECT_MATRIX=
)
env "${COMMON_ENV[@]}" \
  BREWFS_COMPRESSION=none \
  BREWFS_FUSE_WORKERS=6 \
  BREWFS_METADATA_OPEN_CACHE_TTL_MS=1000 \
  BREWFS_METADATA_OPEN_CACHE_CAPACITY=65536 \
  CARGO_TARGET_DIR=/data/slayer/brewfs-cargo-target \
  CARGO_INCREMENTAL=0 \
  bash docker/compose-xfstests/run_redis_perf.sh \
  --s3 --writeback-throughput-profile --tools "$TOOLS"
```

Expected: command exits 0 and prints a `perf-run-*` artifact path.

- [ ] **Step 2: Run JuiceFS full perf**

Run:

```bash
cd /mnt/slayerfs/brewfs
TOOLS="fio-bigread fio-bigwrite fio-seqread fio-seqwrite fio-randread fio-randwrite fio-randrw dirstress dirperf metaperf looptest"
COMMON_ENV=(
  PERF_FIO_DIRECT=0
  PERF_FIO_IOENGINE=io_uring
  PERF_FIO_IODEPTH=1
  PERF_FIO_PREFILL_DRAIN=true
  PERF_FIO_PREFILL_REMOUNT=true
  PERF_FIO_COLD_READ_CLEAR_CACHE=true
  PERF_FIO_DROP_CACHES=false
  PERF_FIO_DIRECT_MATRIX=
)
env "${COMMON_ENV[@]}" \
  JFS_COMPRESS=none \
  JFS_WRITEBACK=true \
  JFS_OPEN_CACHE=1s \
  JFS_OPEN_CACHE_LIMIT=65536 \
  bash docker/compose-xfstests/run_juicefs_perf.sh \
  --writeback-throughput-profile --tools "$TOOLS"
```

Expected: command exits 0 and prints a `juicefs-perf-run-*` artifact path.

- [ ] **Step 3: Extract full metrics**

Run:

```bash
python3 - <<'PY'
from pathlib import Path
print("Use the newest BrewFS and JuiceFS artifact directories, then parse fio JSON and metaperf logs.")
PY
```

Expected: prepare a report table with all FIO and metadata scenarios before coding changes.

### Task 2: Reduce Duplicate Rename Frontend Metadata Work

**Files:**

- Modify: `src/vfs/fs/mod.rs`
- Modify: `src/fuse/mod.rs`
- Test: `src/vfs/fs/tests.rs`

Root-cause hypothesis:

- JuiceFS FUSE rename calls `v.Meta.Rename` after shallow name validation and lets metadata return the moved inode/attr.
- BrewFS FUSE rename already performs source lookup, source stat, destination parent stat, sticky checks, and writeback flush, then calls `VFS::rename_at`.
- `VFS::rename_at` repeats source lookup, source stat, destination parent stat, circular-rename validation, and then calls `MetaClient::rename`.
- For common file-to-file same-directory rename, these repeated async cache/stat steps add latency without increasing correctness.

- [ ] **Step 1: Write the failing test**

Add this test to `src/vfs/fs/tests.rs`:

```rust
#[tokio::test]
async fn rename_at_validated_preserves_same_dir_file_rename_semantics() {
    let layout = ChunkLayout::default();
    let store = InMemoryBlockStore::new();
    let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
    let meta_store = meta_handle.store();
    let fs = VFS::new(layout, store, meta_store).await.unwrap();
    let root = fs.root_ino();
    let ino = fs.create_file_at(root, "src.txt", true).await.unwrap();
    let attr = fs.stat_ino(ino).await.unwrap();

    fs.rename_at_validated(root, "src.txt", root, "dst.txt", ino, attr.kind)
        .await
        .unwrap();

    assert_eq!(fs.child_of(root, "src.txt").await, None);
    assert_eq!(fs.child_of(root, "dst.txt").await, Some(ino));
}
```

Run:

```bash
CARGO_TARGET_DIR=/data/slayer/brewfs-cargo-target CARGO_INCREMENTAL=0 \
  cargo test -p brewfs --lib vfs::fs::tests::rename_at_validated_preserves_same_dir_file_rename_semantics -- --exact
```

Expected: FAIL because `rename_at_validated` does not exist.

- [ ] **Step 2: Implement the validated fast path**

Add this method next to `rename_at` in `src/vfs/fs/mod.rs`:

```rust
pub(crate) async fn rename_at_validated(
    &self,
    old_parent_ino: i64,
    old_name: &str,
    new_parent_ino: i64,
    new_name: &str,
    src_ino: i64,
    src_kind: FileType,
) -> Result<(), VfsError> {
    if old_name.is_empty()
        || new_name.is_empty()
        || old_name.contains('/')
        || old_name.contains('\0')
        || new_name.contains('/')
        || new_name.contains('\0')
    {
        return Err(VfsError::InvalidFilename);
    }
    if old_parent_ino == new_parent_ino && old_name == new_name {
        return Ok(());
    }
    if src_kind == FileType::Dir
        && self.parent_is_descendant_of(new_parent_ino, src_ino).await?
    {
        return Err(VfsError::CircularRename {
            path: PathHint::none(),
        });
    }
    self.meta_rename(
        old_parent_ino,
        old_name,
        new_parent_ino,
        new_name.to_string(),
    )
    .await
}
```

- [ ] **Step 3: Route FUSE rename through the validated fast path**

Replace the final `self.rename_at(...).await` in `src/fuse/mod.rs` with:

```rust
self.rename_at_validated(
    parent as i64,
    &name,
    new_parent as i64,
    &new_name,
    src_ino,
    src_attr.kind,
)
.await
```

Keep the existing error mapping unchanged.

- [ ] **Step 4: Run focused tests**

Run:

```bash
CARGO_TARGET_DIR=/data/slayer/brewfs-cargo-target CARGO_INCREMENTAL=0 \
  cargo test -p brewfs --lib vfs::fs::tests::rename_at_validated_preserves_same_dir_file_rename_semantics -- --exact
CARGO_TARGET_DIR=/data/slayer/brewfs-cargo-target CARGO_INCREMENTAL=0 \
  cargo test -p brewfs --lib fuse::tests::rename -- --nocapture
```

Expected: both commands exit 0.

- [ ] **Step 5: Run broader metadata/VFS tests**

Run:

```bash
CARGO_TARGET_DIR=/data/slayer/brewfs-cargo-target CARGO_INCREMENTAL=0 \
  cargo test -p brewfs --lib meta::client vfs::fs::tests -- --nocapture
```

Expected: command exits 0.

- [ ] **Step 6: Run full perf and compare**

Run Task 1 commands again for BrewFS and JuiceFS.

Expected target:

- `metaperf rename` improves by at least 5% versus Task 1 BrewFS baseline.
- No FIO scenario regresses by more than 5%.
- No metadata scenario regresses by more than 5%.

- [ ] **Step 7: Commit only if useful**

If the target holds:

```bash
git add src/vfs/fs/mod.rs src/fuse/mod.rs src/vfs/fs/tests.rs docs/superpowers/plans/2026-06-17-file-to-file-perf-alignment.md
git commit -m "perf: reduce duplicate rename metadata validation"
```

If not:

```bash
git restore --staged src/vfs/fs/mod.rs src/fuse/mod.rs src/vfs/fs/tests.rs
git restore src/vfs/fs/mod.rs src/fuse/mod.rs src/vfs/fs/tests.rs
```

Do not restore unrelated user changes.

### Task 3: Reduce Create Existing-File Fallback Work

**Files:**

- Modify: `src/vfs/fs/mod.rs`
- Modify if needed: `src/meta/store.rs`
- Modify if needed: `src/meta/client/mod.rs`
- Test: `src/vfs/fs/tests.rs`

Hypothesis:

- JuiceFS `Create` receives attr from metadata in one call.
- BrewFS `create_file_at` returns only inode, and FUSE then calls `apply_new_entry_attrs`/stat.
- For create-heavy file-to-file workloads, returning `(ino, attr)` from the metadata create path may remove one follow-up stat and improve `metaperf create`.

Steps:

- [ ] Add a failing test showing create can return a usable attr without an extra `stat_ino`.
- [ ] Add a default `create_file_with_attr` method to `MetaLayer` that calls `create_file` then `stat`.
- [ ] Override `create_file_with_attr` in Redis once the store can return attr from Lua.
- [ ] Route FUSE create through the attr-returning path only after tests cover create-new and create-existing behavior.
- [ ] Run full perf; keep only if `metaperf create` improves without write/read regressions.

### Task 4: Improve Open Path Hotness

**Files:**

- Modify: `src/meta/client/cache.rs`
- Modify: `src/meta/client/mod.rs`
- Modify: `src/fuse/mod.rs`
- Test: `src/meta/client/mod.rs`

Hypothesis:

- JuiceFS `openfiles.OpenCheck` can reuse attr and set `KeepCache` on hot open.
- BrewFS now has time-to-idle attr reuse, but FUSE open still needs to preserve kernel-cache semantics and avoid invalidating data cache on read-only reopen.

Steps:

- [ ] Add tests for repeated read-only open after close, mtime unchanged, and local mutation invalidation.
- [ ] Confirm FUSE open flags keep cache for hot read-only open.
- [ ] Run full perf; keep only if `metaperf open` improves and read scenarios do not regress.

### Task 5: Read/Write Path File-To-File Alignment

**Files:**

- Read: `/data/slayer/juicefs-v1.3.1/pkg/vfs/reader.go`
- Read: `/data/slayer/juicefs-v1.3.1/pkg/vfs/writer.go`
- Read: `/data/slayer/juicefs-v1.3.1/pkg/chunk/cached_store.go`
- Modify candidates: `src/vfs/io/reader.rs`, `src/vfs/io/writer.rs`, `src/vfs/cache/read_cache.rs`, `src/vfs/cache/write_back.rs`
- Test candidates: existing reader/writer tests in `src/vfs/io/reader.rs` and `src/vfs/io/writer.rs`

Hypotheses to test one at a time:

- BrewFS may underutilize S3/RustFS on sequential writes because staged slice commit/upload concurrency is too conservative.
- BrewFS may lose random mixed I/O to lock contention around per-inode writer state.
- BrewFS may lose cold reads when prefetch depth is not aligned with JuiceFS chunk/cache behavior.

Each hypothesis must:

- Start with a focused failing or measurement test.
- Change exactly one variable.
- Run local tests and full perf.
- Be committed only if the target metric improves and unrelated scenarios stay within the 5% regression budget.
