# BrewFS Cached Block Assembler Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce BrewFS explicit-flush partial-tail amplification by adding a feature-gated cached-write block assembler inspired by JuiceFS, then accept it only if local CI and full BrewFS/JuiceFS perf show a real write-path gain without read, randrw, metadata, or POSIX regressions.

**Goal Addendum:** Every future performance iteration must reproduce the local CI `Test workspace` gate before perf evidence is accepted. A change that has not passed `cargo fmt --all --check` and `cargo test --workspace --lib --bins` in this worktree is not eligible for README benchmark updates, commits as a performance improvement, or comparison claims against JuiceFS.

**Goal Addendum Verification:** After reverting the rejected writer wiring, the current worktree passed the local CI test gate with `CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo test --workspace --lib --bins`: `510 passed; 0 failed; 159 ignored`.

**Perf Tooling Addendum:** `tools/perf/compare_artifacts.py` now reports `read_effective_wall_bw_mib_s`, `write_effective_wall_bw_mib_s`, and `*_effective_active_plus_drain_bw_mib_s` from fio IO bytes divided by script wall time and active-plus-drain time. Use these rows when accepting or rejecting writeback candidates; previous rejected attempts repeatedly improved fio active bandwidth while losing or barely moving the end-to-end user-visible throughput.

**Architecture:** JuiceFS keeps page writes in a `wSlice`, uploads full blocks with `FlushTo`, and only finalizes the partial tail on `Flush`/`Close`; BrewFS currently turns many FUSE writeback-cache page writes into independent `SliceState`s, so explicit flush often stages and commits thousands of small cached-only tails. This plan adds a separate cached-write assembler that buffers page-sized cached writes per inode/chunk/block, emits full-block slices as soon as they are complete, and drains remaining partial runs only at explicit flush/truncate/close. The existing `SliceState` path remains the default until perf and correctness gates prove the assembler is safe.

**Tech Stack:** Rust, Tokio, BrewFS `src/vfs/io/writer.rs`, `src/vfs/fs/mod.rs`, `src/vfs/config.rs`, existing Redis/S3 Docker perf runners, local GitHub Actions `Test workspace` reproduction.

---

## Evidence And Current Root Cause

Latest accepted BrewFS artifact:

- BrewFS: `docker/compose-xfstests/artifacts/perf-run-1781673470-37`
- JuiceFS: `docker/compose-xfstests/artifacts/juicefs-perf-run-1781673944-2163`

The current gap is not primarily remote upload wait. Under `CommitBeforeUpload`, BrewFS can commit metadata after local writeback staging and before remote upload completes. The dominant remaining write-path issue is object and metadata shape:

- `fio-seqwrite`: `8733` upload batches, `0.96` partial-tail ratio, `14599` flush-wait slices.
- `fio-randwrite`: `14826` upload batches, `0.94` partial-tail ratio, `18234` flush-wait slices.
- `fio-randrw`: `13228` upload batches, `0.94` partial-tail ratio, `11831` explicit-flush partial tails, `9250` flush-wait slices.

Rejected ideas already rule out repeating the small fixes:

- Do not raise writeback upload workers as the primary fix; concurrency `6 -> 8` regressed active throughput and tail latency.
- Do not repeat stage/upload overlap; it moved cost and regressed randwrite/randrw guards.
- Do not repeat simple cached adjacent slice merge; focused tests passed, but perf rejected it.
- Do not repeat simple older-unique reuse; it lowered older-unique rejects slightly but worsened randwrite and dirty tail.
- Do not rely on timing-only grace. Earlier too-many/idle timing shifts raised partial-tail counts and regressed metaperf.

The next attempt must change the representation of cached page writes before they become many small `SliceState`s.

## Acceptance Gate

Every accepted code change in this plan must pass this local CI gate before perf numbers count. This is now part of the active optimization goal, not an optional clean-up step:

```bash
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo fmt --all --check
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo test --workspace --lib --bins
```

For the final accepted candidate, also run the matching workflow build/check steps when disk space allows:

```bash
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo check --workspace
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo build --workspace
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo clippy --workspace
```

Perf acceptance requires same-parameter BrewFS and JuiceFS runs:

```bash
PERF_LOG_TO_CONSOLE=false PERF_FIO_SIZE=512m PERF_FIO_RUNTIME=20 \
  bash docker/compose-xfstests/run_redis_perf.sh --s3 \
  --writeback-throughput-profile \
  --tools "fio-seqwrite fio-randwrite fio-randrw"

PERF_LOG_TO_CONSOLE=false PERF_FIO_SIZE=512m PERF_FIO_RUNTIME=20 \
  bash docker/compose-xfstests/run_juicefs_perf.sh \
  --writeback-throughput-profile \
  --tools "fio-seqwrite fio-randwrite fio-randrw"
```

Before making the assembler default, run the full matrix:

```bash
PERF_LOG_TO_CONSOLE=false \
  bash docker/compose-xfstests/run_redis_perf.sh --s3 --writeback-throughput-profile

PERF_LOG_TO_CONSOLE=false \
  bash docker/compose-xfstests/run_juicefs_perf.sh --writeback-throughput-profile
```

The accepted BrewFS result must satisfy all of these checks:

- `fio-seqwrite` active write bandwidth improves by at least 15% versus `perf-run-1781673470-37`, and tool+drain does not regress.
- `fio-randwrite` active write bandwidth improves by at least 15%, with write p99 not worse than 25%.
- `fio-randrw` tool+drain improves or remains within 5%, and read/write active bandwidth remains within 10%.
- `upload_partial_tail_explicit_flush_ops` falls materially for `fio-randrw`.
- `upload_batch` count and average batch size move in the expected direction.
- `metaperf`, `dirperf`, and read workloads do not show a large regression in the full matrix.
- README gets an updated performance table with the new artifact directories.

## Task 1: Add Explicit-Flush Fragmentation Metrics

**Files:**

- Modify: `src/vfs/io/writer.rs`
- Modify: `src/vfs/stats.rs`
- Test: `src/vfs/io/writer.rs`
- Docs: `README.md` after perf

- [x] **Step 1: Write the failing test**

Add a unit test near the existing writer metrics tests:

```rust
#[test]
fn test_flush_fragmentation_metrics_track_cached_sub_block_slices() {
    let state = RecentPendingUploadState::new();

    state.record_flush_fragmentation(
        3,
        10 * 1024,
        2,
        2 * 1024,
        1,
        8 * 1024,
    );

    let snapshot = state.snapshot();
    assert_eq!(snapshot.flush_fragmentation_ops, 1);
    assert_eq!(snapshot.flush_fragmentation_slices, 3);
    assert_eq!(snapshot.flush_fragmentation_bytes, 10 * 1024);
    assert_eq!(snapshot.flush_fragmentation_cached_sub_block_slices, 2);
    assert_eq!(snapshot.flush_fragmentation_cached_sub_block_bytes, 2 * 1024);
    assert_eq!(snapshot.flush_fragmentation_full_block_slices, 1);
    assert_eq!(snapshot.flush_fragmentation_full_block_bytes, 8 * 1024);
}
```

- [x] **Step 2: Verify RED**

Run:

```bash
cargo test -p brewfs vfs::io::writer::tests::test_flush_fragmentation_metrics_track_cached_sub_block_slices
```

Expected: fails because `record_flush_fragmentation` and snapshot fields do not exist.

- [x] **Step 3: Implement minimal metrics**

Add counters to `RecentPendingUploadState`:

```rust
flush_fragmentation_ops: AtomicU64,
flush_fragmentation_slices: AtomicU64,
flush_fragmentation_bytes: AtomicU64,
flush_fragmentation_cached_sub_block_slices: AtomicU64,
flush_fragmentation_cached_sub_block_bytes: AtomicU64,
flush_fragmentation_full_block_slices: AtomicU64,
flush_fragmentation_full_block_bytes: AtomicU64,
```

Add:

```rust
fn record_flush_fragmentation(
    &self,
    slices: u64,
    bytes: u64,
    cached_sub_block_slices: u64,
    cached_sub_block_bytes: u64,
    full_block_slices: u64,
    full_block_bytes: u64,
) {
    self.flush_fragmentation_ops.fetch_add(1, Ordering::Relaxed);
    self.flush_fragmentation_slices.fetch_add(slices, Ordering::Relaxed);
    self.flush_fragmentation_bytes.fetch_add(bytes, Ordering::Relaxed);
    self.flush_fragmentation_cached_sub_block_slices
        .fetch_add(cached_sub_block_slices, Ordering::Relaxed);
    self.flush_fragmentation_cached_sub_block_bytes
        .fetch_add(cached_sub_block_bytes, Ordering::Relaxed);
    self.flush_fragmentation_full_block_slices
        .fetch_add(full_block_slices, Ordering::Relaxed);
    self.flush_fragmentation_full_block_bytes
        .fetch_add(full_block_bytes, Ordering::Relaxed);
}
```

Wire the fields through the writer stats snapshot and `src/vfs/stats.rs` rendering.

- [x] **Step 4: Record metrics during explicit flush snapshot**

In `FileWriter::flush_with_deadline`, after collecting the `slices` vector and before freezing it, compute:

```rust
let mut total_bytes = 0u64;
let mut cached_sub_block_slices = 0u64;
let mut cached_sub_block_bytes = 0u64;
let mut full_block_slices = 0u64;
let mut full_block_bytes = 0u64;
for slice in &slices {
    let s = slice.lock();
    let len = s.data.len();
    total_bytes = total_bytes.saturating_add(len);
    let block = s.data.block_size() as u64;
    if matches!(s.write_origin_kind(), WriteOriginKind::CachedOnly) && len < block {
        cached_sub_block_slices += 1;
        cached_sub_block_bytes = cached_sub_block_bytes.saturating_add(len);
    }
    if len >= block && len % block == 0 {
        full_block_slices += 1;
        full_block_bytes = full_block_bytes.saturating_add(len);
    }
}
self.shared.recent_pending_upload.record_flush_fragmentation(
    slices.len() as u64,
    total_bytes,
    cached_sub_block_slices,
    cached_sub_block_bytes,
    full_block_slices,
    full_block_bytes,
);
```

- [x] **Step 5: Verify GREEN**

Run:

```bash
cargo test -p brewfs vfs::io::writer::tests::test_flush_fragmentation_metrics_track_cached_sub_block_slices
```

Expected: pass.

Task 1 verification on 2026-06-17:

- `CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo fmt --all --check` passed.
- `git diff --check` passed.
- `CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo test -p brewfs vfs::io::writer::tests::test_flush_fragmentation_metrics_track_cached_sub_block_slices` passed.
- `CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo test -p brewfs vfs::stats::tests` passed.
- Local CI `Test workspace` equivalent passed: `CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo test --workspace --lib --bins` finished with `505 passed; 0 failed; 159 ignored`.

## Task 2: Add A Pure Cached Block Assembler Model

**Files:**

- Create: `src/vfs/io/cached_block_assembler.rs`
- Modify: `src/vfs/io/mod.rs`
- Test: `src/vfs/io/cached_block_assembler.rs`

- [x] **Step 1: Write failing tests**

Create tests for the pure model before wiring it into `FileWriter`:

```rust
#[test]
fn assembler_emits_full_block_after_page_writes() {
    let mut a = CachedBlockAssembler::new(4096, 1024);
    for i in 0..4 {
        a.write(i * 1024, vec![i as u8; 1024], i as u64 + 1);
    }
    let ready = a.drain_ready_full_blocks();
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].offset, 0);
    assert_eq!(ready[0].data.len(), 4096);
}

#[test]
fn assembler_keeps_last_writer_for_overlap() {
    let mut a = CachedBlockAssembler::new(4096, 1024);
    a.write(0, vec![1; 1024], 10);
    a.write(0, vec![2; 1024], 20);
    let pending = a.drain_all();
    assert_eq!(&pending[0].data[..1024], &[2; 1024]);
}

#[test]
fn assembler_rejects_older_overlap_after_newer_unique() {
    let mut a = CachedBlockAssembler::new(4096, 1024);
    a.write(0, vec![2; 1024], 20);
    assert_eq!(
        a.try_write(0, vec![1; 1024], 10),
        Err(AssemblerWriteError::OlderOverlap)
    );
}

#[test]
fn assembler_truncate_drops_tail_pages() {
    let mut a = CachedBlockAssembler::new(4096, 1024);
    a.write(0, vec![1; 1024], 1);
    a.write(4096, vec![2; 1024], 2);
    a.truncate(1024);
    let pending = a.drain_all();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].offset, 0);
    assert_eq!(pending[0].data.len(), 1024);
}
```

- [x] **Step 2: Verify RED**

Run:

```bash
cargo test -p brewfs vfs::io::cached_block_assembler
```

Expected: fails because the module and types do not exist.

- [x] **Step 3: Implement the pure model**

Implement `CachedBlockAssembler` as an in-memory per-chunk page map:

```rust
pub(crate) struct CachedBlockAssembler {
    block_size: u64,
    page_size: u64,
    pages: BTreeMap<u64, CachedPage>,
}

struct CachedPage {
    unique: u64,
    data: Bytes,
}

pub(crate) struct AssembledExtent {
    pub(crate) offset: u64,
    pub(crate) data: Bytes,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum AssemblerWriteError {
    OlderOverlap,
}
```

The first implementation should support only page-aligned cached writes whose length is at most one page. Return `OlderOverlap` if an incoming unique is lower than an existing overlapping page unique. This intentionally preserves the correctness boundary that made the simple older-unique reuse candidate unsafe.

- [x] **Step 4: Verify GREEN**

Run:

```bash
cargo test -p brewfs vfs::io::cached_block_assembler
```

Expected: pass.

Task 2 verification on 2026-06-17:

- RED: `CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo test -p brewfs vfs::io::cached_block_assembler` failed because `CachedBlockAssembler` and `AssemblerWriteError` were missing.
- GREEN: the same focused command passed with `4 passed; 0 failed` in both `src/lib.rs` and `src/main.rs` unit test targets.
- `CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo fmt --all --check` passed.
- `git diff --check` passed.
- Local CI `Test workspace` equivalent passed: `CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo test --workspace --lib --bins` finished with `509 passed; 0 failed; 159 ignored`.
- Perf was not run for Task 2 because this is an unconnected pure model and does not change runtime write-path behavior.

## Task 3: Feature-Gate The Assembler Configuration

**Files:**

- Modify: `src/vfs/config.rs`
- Modify: `src/vfs/io/writer.rs`
- Test: `src/vfs/config.rs`

- [x] **Step 1: Write failing config test**

Add:

```rust
#[test]
fn write_config_parses_cached_block_assembler_env() {
    temp_env::with_var("BREWFS_CACHED_BLOCK_ASSEMBLER", Some("1"), || {
        let config = WriteConfig::from_env(ChunkLayout::default());
        assert!(config.cached_block_assembler);
    });
}
```

- [x] **Step 2: Verify RED**

Run:

```bash
cargo test -p brewfs vfs::config::tests::write_config_parses_cached_block_assembler_env
```

Expected: fails because the config flag does not exist.

- [x] **Step 3: Add the disabled-by-default flag**

Add `cached_block_assembler: bool` to `WriteConfig`, parse `BREWFS_CACHED_BLOCK_ASSEMBLER=1|true|yes`, and keep the default `false`.

- [x] **Step 4: Verify GREEN**

Run:

```bash
cargo test -p brewfs vfs::config::tests::write_config_parses_cached_block_assembler_env
```

Expected: pass.

Task 3 verification on 2026-06-17:

- RED: `CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo test -p brewfs vfs::config::tests::write_config_parses_cached_block_assembler_env` failed because `cached_block_assembler` and its builder method were missing.
- GREEN: the same focused command passed with `1 passed; 0 failed` in both `src/lib.rs` and `src/main.rs` unit test targets.
- `CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo test -p brewfs vfs::config::tests` passed with `4 passed; 0 failed` in both unit test targets after serializing the process-wide env var in tests.
- `CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo fmt --all --check` passed.
- `git diff --check` passed.
- Local CI `Test workspace` equivalent passed: `CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo test --workspace --lib --bins` finished with `510 passed; 0 failed; 159 ignored`.
- Perf was not run for Task 3 because the new flag defaults off and the assembler is still not wired into runtime write-path behavior.

## Task 4: Wire Full-Block Assembly Into Cached Writes

**Files:**

- Modify: `src/vfs/io/writer.rs`
- Modify: `src/vfs/io/cached_block_assembler.rs`
- Test: `src/vfs/io/writer.rs`

- [ ] **Step 1: Write failing writer test**

Add a test proving page writes become one full-block slice when the feature flag is enabled:

```rust
#[tokio::test]
async fn test_cached_block_assembler_emits_one_full_block_slice() {
    let layout = ChunkLayout {
        chunk_size: 16 * 1024,
        block_size: 4 * 1024,
    };
    let config = Arc::new(
        WriteConfig::new(layout)
            .page_size(1024)
            .freeze_min_bytes(4096)
            .cached_block_assembler(true)
            .writeback_mode(WriteBackMode::CommitBeforeUpload),
    );
    let store = InMemoryBlockStore::new();
    let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
    let backend = Backend::new(layout, store.clone(), meta_handle.store());
    let inode = Arc::new(Inode::new(1, 0));
    let reader = Arc::new(DataFetcher::new(backend.clone(), inode.clone(), ReadConfig::default()));
    let writer = FileWriter::new_with_config(backend, inode, reader, config).unwrap();

    for i in 0..4 {
        writer
            .write_at_cached(i * 1024, &[i as u8; 1024], i + 1)
            .await
            .unwrap();
    }

    writer.flush().await.unwrap();
    let breakdown = writer.dirty_breakdown().await;
    assert!(breakdown.upload_batch_ops <= 1);
    assert_eq!(breakdown.upload_partial_tail_ops, 0);
}
```

- [ ] **Step 2: Verify RED**

Run:

```bash
cargo test -p brewfs vfs::io::writer::tests::test_cached_block_assembler_emits_one_full_block_slice
```

Expected: fails because cached writes still go directly to `SliceState`.

- [ ] **Step 3: Implement minimal wiring**

Add an optional assembler map to each writer/chunk for cached writes only. If the flag is enabled and the write is page-aligned, page-sized, non-sparse, and within one chunk, insert it into the assembler. When a block becomes complete, emit a single normal `SliceState` covering the full block through the existing `ChunkHandle::write_at` path with `WriteOrigin::Cached`. If the assembler rejects an older overlap, fall back to the existing `SliceState` path.

- [ ] **Step 4: Verify GREEN**

Run:

```bash
cargo test -p brewfs vfs::io::writer::tests::test_cached_block_assembler_emits_one_full_block_slice
```

Expected: pass.

**2026-06-17 rejected attempt:** A minimal writer wiring attempt passed the focused regression test, cached assembler unit tests, config tests, `cargo fmt --all --check`, `git diff --check`, and local CI equivalent:

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo test --workspace --lib --bins
```

It finished with `511 passed; 0 failed; 159 ignored`, then failed the perf acceptance gate and was reverted. Focused BrewFS artifact: `docker/compose-xfstests/artifacts/perf-run-1781692642-30426`.

| Tool | Baseline artifact | Rejected attempt | Decision signal |
| --- | ---: | ---: | --- |
| `fio-seqwrite` | `126.15 MiB/s`, `130s` | `122.69 MiB/s`, `137s` | Worse active bandwidth and wall time |
| `fio-randwrite` | `204.58 MiB/s`, `131s` | `171.05 MiB/s`, `131s` | Clear active bandwidth regression |
| `fio-randrw` | read `914.30 MiB/s`, write `409.37 MiB/s`, `143s` | read `985.20 MiB/s`, write `441.76 MiB/s`, `142s` | Mixed gain, not enough to offset pure-write regression |

The simple per-block assembler only helped a narrow out-of-order page model. Real fio still emitted very high cached sub-block and partial-tail counts, including `flush_fragmentation_slices_total 16646` for `fio-seqwrite` and partial-tail ratios near `0.95` to `0.97` for write-heavy tools. Do not repeat this exact writer-level assembly approach; the next attempt must assemble or defer cached partials at a wider flush-aware extent boundary before many `SliceState`s exist.

## Task 5: Drain Assembler On Flush, Truncate, And Cleanup

**Files:**

- Modify: `src/vfs/io/writer.rs`
- Modify: `src/vfs/fs/mod.rs`
- Test: `src/vfs/io/writer.rs`
- Test: `src/vfs/fs/tests.rs`

- [ ] **Step 1: Write failing flush test**

Add:

```rust
#[tokio::test]
async fn test_cached_block_assembler_flush_drains_partial_tail() {
    let writer = new_cached_block_assembler_test_writer().await;
    writer.write_at_cached(0, &[7u8; 1024], 10).await.unwrap();
    writer.flush().await.unwrap();

    let mut out = vec![0u8; 1024];
    assert!(writer.overlay_dirty(0, &mut out).await.unwrap() || out == vec![7u8; 1024]);
    let breakdown = writer.dirty_breakdown().await;
    assert_eq!(breakdown.upload_partial_tail_explicit_flush_ops, 1);
}
```

- [ ] **Step 2: Write failing truncate test**

Add a VFS-level test that writes cached pages through `write_cached_ino`, truncates the inode, and reads back only the retained prefix. This protects truncate epoch invalidation.

- [ ] **Step 3: Implement drain hooks**

Before `flush_with_deadline` snapshots slices, drain each chunk assembler into `SliceState`s, then use the existing freeze/commit path. On truncate or writer clear, drop assembler pages beyond the new length before draining or clearing existing slices.

- [ ] **Step 4: Verify GREEN**

Run:

```bash
cargo test -p brewfs \
  vfs::io::writer::tests::test_cached_block_assembler_flush_drains_partial_tail \
  vfs::fs::tests::io_tests::test_cached_block_assembler_truncate_preserves_prefix
```

Expected: pass.

## Task 6: Local CI, Focused Perf, And Decision

**Files:**

- Modify: `README.md`
- Modify: `doc/superpowers/plans/2026-06-17-brewfs-cached-block-assembler.md`

- [ ] **Step 1: Run local CI gate**

```bash
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo fmt --all --check
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo test --workspace --lib --bins
```

Expected: exit 0. Record pass count in README and this plan.

- [ ] **Step 2: Run focused BrewFS perf with assembler enabled**

```bash
BREWFS_CACHED_BLOCK_ASSEMBLER=1 \
PERF_LOG_TO_CONSOLE=false PERF_FIO_SIZE=512m PERF_FIO_RUNTIME=20 \
  bash docker/compose-xfstests/run_redis_perf.sh --s3 \
  --writeback-throughput-profile \
  --tools "fio-seqwrite fio-randwrite fio-randrw"
```

Expected: all three tools pass.

- [ ] **Step 3: Run matching JuiceFS perf**

```bash
PERF_LOG_TO_CONSOLE=false PERF_FIO_SIZE=512m PERF_FIO_RUNTIME=20 \
  bash docker/compose-xfstests/run_juicefs_perf.sh \
  --writeback-throughput-profile \
  --tools "fio-seqwrite fio-randwrite fio-randrw"
```

Expected: all selected tools pass or any JuiceFS warnings are recorded.

- [ ] **Step 4: Decide**

Accept only if the acceptance gate passes. If the assembler improves one pure write workload but regresses `fio-randrw`, write p99, or metadata tests beyond the guard, revert the code and keep only the metrics/plan notes.

- [ ] **Step 5: Run full matrix before defaulting**

Run the full BrewFS/JuiceFS profile without narrowing `--tools`. If the feature remains opt-in, record it as an experimental profile. If making it default, update README's latest snapshot and commit the default change only after the full matrix passes.

## Self-Review

- Spec coverage: This plan covers the explicit flush partial-tail bottleneck, JuiceFS comparison, FUSE unique ordering, overlapping writes, truncate behavior, mmap/writeback cached writes through `write_cached_ino`, local CI, full perf, README updates, and rollback criteria.
- Placeholder scan: The plan has no placeholder markers.
- Scope check: The assembler is feature-gated and isolated from normal writes, so each task can be reverted independently.
- Ambiguity check: The acceptance thresholds use the current accepted artifact `perf-run-1781673470-37` as the BrewFS baseline and require a same-parameter JuiceFS comparison before README performance claims.
