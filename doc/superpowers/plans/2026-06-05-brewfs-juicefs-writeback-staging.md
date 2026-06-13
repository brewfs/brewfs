# BrewFS Writeback Backpressure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add safe writeback backpressure for `CommitBeforeUpload` so BrewFS bounds committed-but-not-uploaded dirty memory without introducing read errors, flush hangs, or seqwrite regressions.

**Architecture:** Do not release in-memory pages first. The rejected experiments showed that page release needs a deeper read/commit state-machine redesign. Start with a non-invasive backlog accounting layer, then add an opt-in pending-upload admission gate that delays new foreground writes only when committed dirty bytes exceed a configurable soft limit. Every accepted step must pass perf validation through `docker/compose-xfstests/run_redis_perf.sh`.

**Tech Stack:** Rust BrewFS, Tokio, Redis metadata backend, RustFS S3 backend, Docker Compose perf runner, fio, existing BrewFS `.stats` diagnostics.

---

## Evidence From Rejected Experiments

The following attempts were tested and rolled back. Do not reintroduce these changes without a new design review.

| Attempt | Artifact | Result | Decision |
| --- | --- | --- | --- |
| Release staged pages and read them back from SSD overlay | `perf-run-1780652805-12274` | `fio-seqread` failed with EIO at offset `45576192`; writer logged `flush timeout` with 43 `Readonly` slices; `fio-seqwrite` fell to `90.62 MiB/s` | Rejected and reverted |
| Track per-block staged state without release | `perf-run-1780654865-13977` | `fio-seqread` recovered to `1249.23 MiB/s`, but `fio-seqwrite` was only `122.79 MiB/s`; `fio-randread` hung for 18 minutes | Rejected and reverted |
| Stats-only staged fields | `perf-run-1780657238-10758` with tools `fio-seqread fio-seqwrite` | Exit code `0`, but no useful staged signal: `staged=0`; `recent_pending_upload_bytes=18813419520`; `fio-seqwrite=275.13 MiB/s` | Rejected and reverted |

Stable historical writeback evidence:

| Artifact | Config | seqwrite | write p99 | S3 PUT bytes | dirty bytes | dirty breakdown |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| `perf-run-1780640373-26967` | baseline | 1336.53 MiB/s | 12.52 ms | 1649259937 | 13779337216 | no breakdown metric yet |
| `perf-run-1780647039-23577` | writeback concurrency 4 | 1419.72 MiB/s | 9.77 ms | 1592075619 | 14719975424 | live=0, pending=14716698624, uploaded=3276800 |
| `perf-run-1780647150-9098` | writeback concurrency 5 | 1562.51 MiB/s | 9.24 ms | 1776227568 | 16250896384 | live=0, pending=16250896384, uploaded=0 |
| `perf-run-1780647242-6471` | writeback concurrency 6 | 1475.70 MiB/s | 10.16 ms | 1872988781 | 15279456256 | live=0, pending=15235219456, uploaded=44236800 |

Conclusion:
- The bottleneck remains `recently_committed_pending_upload_bytes`, not live writable slices.
- Backpressure is needed, but it must gate admission based on pending-upload backlog.
- Page release is out of scope for this plan because the first implementation caused read errors and flush state-machine stalls.

## New Targets

Primary comparable-performance target for this plan:
- Comparable command passes:

```bash
cd /mnt/slayerfs/brewfs
PERF_FIO_SEQREAD_RUNTIME=10 \
PERF_FIO_SEQREAD_SIZE=8G \
PERF_FIO_SEQWRITE_RUNTIME=10 \
PERF_FIO_SEQWRITE_SIZE=8G \
bash docker/compose-xfstests/run_redis_perf.sh \
  --writeback-throughput-profile \
  --tools "fio-seqread fio-seqwrite"
```

Performance gates:
- `fio-seqwrite >= 1336 MiB/s`.
- `fio-seqread` has `error=0`.
- No `flush timeout` appears in `brewfs.log`.
- `brewfs_writeback_recent_pending_upload_bytes <= 11459592192` after `fio-seqwrite` in the full profile. This is 25% lower than `15279456256`.
- S3 PUT bytes after `fio-seqwrite >= 1592075619`.

Stability soak gate:
- After comparable-performance gates pass, run the script default 60s profile at least for `fio-seqread fio-seqwrite`.
- The 60s profile must exit `0`, have no read EIO, and have no `flush timeout`.

Commit rule:
- Commit and push only after all gates pass.
- Revert rejected code with targeted patches; do not use `git reset --hard`.
- If three code-level attempts fail, stop and write a blocker note before trying a fourth.

## Files

- Modify: `/mnt/slayerfs/src/vfs/config.rs`
  - Add opt-in pending-upload backpressure config.
  - Parse env vars for soft/hard limits.
- Modify: `/mnt/slayerfs/src/vfs/io/writer.rs`
  - Add precise recently-committed pending byte accounting.
  - Add wait/notify gate before accepting new writes in `CommitBeforeUpload`.
  - Add tests for admission delay and wakeup.
- Modify: `/mnt/slayerfs/src/vfs/stats.rs`
  - Add backpressure metrics after the behavior is implemented.
- Modify: `/mnt/slayerfs/src/vfs/fs/mod.rs`
  - Export backpressure metrics into `.stats`.
- Modify only if needed: `/mnt/slayerfs/docker/compose-xfstests/run_perf_in_container.sh`
  - Include new `.stats` metrics in reports if not already captured.
- Reference only: `/mnt/slayerfs/brewfs/juicefs/pkg/chunk/cached_store.go`
- Reference only: `/mnt/slayerfs/brewfs/juicefs/pkg/vfs/writer.go`
- Reference only: `/mnt/slayerfs/brewfs/juicefs/pkg/chunk/disk_cache.go`

## Task 0: Reproduce A Clean Baseline Before Code Changes

**Files:**
- No source edits.

- [ ] **Step 1: Run key writeback tools**

Run:

```bash
cd /mnt/slayerfs/brewfs
bash docker/compose-xfstests/run_redis_perf.sh \
  --writeback-throughput-profile \
  --tools "fio-seqread fio-seqwrite"
```

Expected:
- Exit code `0`.
- `fio-seqread` has `error=0`.
- `fio-seqwrite` result exists in `results/fio-seqwrite.json`.
- This run is a 60s stability baseline, not the comparable throughput baseline.

- [ ] **Step 2: Extract baseline numbers**

Run:

```bash
cd /mnt/slayerfs/brewfs
python3 - <<'PY'
import json, pathlib
root = max(pathlib.Path("docker/compose-xfstests/artifacts").glob("perf-run-*"), key=lambda p: p.stat().st_mtime)
for name in ["fio-seqread", "fio-seqwrite"]:
    job = json.loads((root / "results" / f"{name}.json").read_text())["jobs"][0]
    print(root.name, name, "error", job.get("error"), "elapsed", job.get("elapsed"))
    for op in ["read", "write"]:
        data = job.get(op, {})
        if data.get("bw_bytes", 0):
            print(op, f"{data['bw_bytes'] / 1048576:.2f} MiB/s")
for stats in sorted((root / "diagnostics").glob("stats-fio-seqwrite-after.txt")):
    for line in stats.read_text().splitlines():
        if line.startswith(("brewfs_writeback_", "brewfs_s3_put_bytes_total")):
            print(line)
PY
```

Expected:
- Record artifact name and metrics in the implementation notes.
- If this 60s run fails or logs EIO/flush timeout, stop and diagnose environment variance before implementing backpressure.

- [ ] **Step 3: Run comparable 10s baseline**

Run:

```bash
cd /mnt/slayerfs/brewfs
PERF_FIO_SEQREAD_RUNTIME=10 \
PERF_FIO_SEQREAD_SIZE=8G \
PERF_FIO_SEQWRITE_RUNTIME=10 \
PERF_FIO_SEQWRITE_SIZE=8G \
bash docker/compose-xfstests/run_redis_perf.sh \
  --writeback-throughput-profile \
  --tools "fio-seqread fio-seqwrite"
```

Expected:
- Exit code `0`.
- `fio-seqwrite >= 1000 MiB/s` before code changes.
- If `fio-seqwrite < 1000 MiB/s`, stop and diagnose environment variance before implementing backpressure.

## Task 1: Add Pending-Upload Backlog Accounting

**Files:**
- Modify: `/mnt/slayerfs/src/vfs/io/writer.rs`
- Test: `/mnt/slayerfs/src/vfs/io/writer.rs`

- [ ] **Step 1: Write the failing unit test**

Add this test in `mod tests` in `/mnt/slayerfs/src/vfs/io/writer.rs`:

```rust
#[tokio::test]
async fn test_recent_pending_accounting_tracks_move_and_upload_completion() {
    let layout = ChunkLayout {
        chunk_size: 8 * 1024,
        block_size: 4 * 1024,
    };
    let store = Arc::new(BlockingStore::new(true));
    let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
    let meta = meta_handle.layer();
    let backend = Arc::new(Backend::new(store.clone(), meta.clone()));
    let ino = meta
        .create_file(1, "pending_accounting.txt".to_string())
        .await
        .unwrap();
    let inode = Inode::new(ino, 0);
    let reader = Arc::new(DataReader::new(
        Arc::new(ReadConfig::new(layout)),
        backend.clone(),
    ));
    let data_writer = Arc::new(DataWriter::new(
        test_config_with_writeback(layout, WriteBackMode::CommitBeforeUpload),
        backend,
        reader,
        None,
    ));
    let file_writer = data_writer.ensure_file(inode);

    file_writer
        .write_at(0, &vec![5u8; layout.block_size as usize])
        .await
        .unwrap();
    timeout(Duration::from_secs(2), file_writer.flush())
        .await
        .expect("commit-before-upload flush should return while upload is blocked")
        .unwrap();

    assert_eq!(
        data_writer.recent_pending_upload_bytes(),
        layout.block_size as u64
    );

    store.unblock();
    timeout(Duration::from_secs(2), async {
        loop {
            if data_writer.recent_pending_upload_bytes() == 0 {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("pending bytes should drop after upload completes");
}
```

- [ ] **Step 2: Verify the test fails**

Run:

```bash
cd /mnt/slayerfs/brewfs
cargo test -p brewfs vfs::io::writer::tests::test_recent_pending_accounting_tracks_move_and_upload_completion --lib
```

Expected:
- FAIL because `DataWriter::recent_pending_upload_bytes()` does not exist.

- [ ] **Step 3: Add atomic accounting fields**

In `/mnt/slayerfs/src/vfs/io/writer.rs`, add to `DataWriter`:

```rust
recent_pending_upload_bytes: Arc<AtomicU64>,
recent_pending_notify: Arc<Notify>,
```

Initialize in `DataWriter::new()`:

```rust
recent_pending_upload_bytes: Arc::new(AtomicU64::new(0)),
recent_pending_notify: Arc::new(Notify::new()),
```

Pass both into `FileWriter::new_with_memory_budget()` and store them in `Shared`.

- [ ] **Step 4: Add the accessor**

Add to `impl DataWriter<B, M>`:

```rust
pub(crate) fn recent_pending_upload_bytes(&self) -> u64 {
    self.recent_pending_upload_bytes
        .load(std::sync::atomic::Ordering::Acquire)
}
```

- [ ] **Step 5: Update counters at lifecycle boundaries**

In `move_front_slice_to_recently_committed()`, after pushing the slice into `recently_committed`, add:

```rust
let bytes = expected.lock().data.alloc_bytes();
shared
    .recent_pending_upload_bytes
    .fetch_add(bytes, Ordering::AcqRel);
```

In `advance_upload_range()`, when a slice transitions to upload complete, subtract once. Add a `recent_pending_accounted: bool` field to `SliceState`, initialized `false`, and set it when adding. When upload completes:

```rust
if s.recent_pending_accounted && s.upload_complete() {
    let bytes = s.data.alloc_bytes();
    s.recent_pending_accounted = false;
    self.shared
        .recent_pending_upload_bytes
        .fetch_sub(bytes, Ordering::AcqRel);
    self.shared.recent_pending_notify.notify_waiters();
}
```

Expected:
- The unit test passes.
- Existing dirty breakdown still works.

- [ ] **Step 6: Verify**

Run:

```bash
cd /mnt/slayerfs/brewfs
cargo test -p brewfs vfs::io::writer::tests::test_recent_pending_accounting_tracks_move_and_upload_completion --lib
cargo clippy --workspace --all-targets -- -D warnings
```

Expected:
- Both commands exit `0`.

## Task 2: Add Opt-In Backpressure Config

**Files:**
- Modify: `/mnt/slayerfs/src/vfs/config.rs`
- Modify: `/mnt/slayerfs/src/vfs/io/writer.rs`

- [ ] **Step 1: Add config fields**

Add to `WriteConfig`:

```rust
pub writeback_recent_pending_soft_limit: u64,
pub writeback_recent_pending_hard_limit: u64,
```

Default both to `0`, meaning disabled.

- [ ] **Step 2: Parse env vars**

In `impl Default for WriteConfig`, before constructing `Self`, add:

```rust
let writeback_recent_pending_soft_limit = std::env::var("BREWFS_WRITEBACK_RECENT_PENDING_SOFT_BYTES")
    .ok()
    .and_then(|value| value.parse::<u64>().ok())
    .unwrap_or(0);
let writeback_recent_pending_hard_limit = std::env::var("BREWFS_WRITEBACK_RECENT_PENDING_HARD_BYTES")
    .ok()
    .and_then(|value| value.parse::<u64>().ok())
    .unwrap_or(0);
```

Then include both fields in `Self`.

Expected:
- Existing configs keep behavior unchanged.

- [ ] **Step 3: Add builder helpers**

Add to `impl WriteConfig`:

```rust
pub fn writeback_recent_pending_soft_limit(self, limit: u64) -> Self {
    Self {
        writeback_recent_pending_soft_limit: limit,
        ..self
    }
}

pub fn writeback_recent_pending_hard_limit(self, limit: u64) -> Self {
    Self {
        writeback_recent_pending_hard_limit: limit,
        ..self
    }
}
```

Expected:
- Tests can configure a small soft limit without env mutation.

- [ ] **Step 4: Verify config default**

Run:

```bash
cd /mnt/slayerfs/brewfs
cargo test -p brewfs vfs::config --lib
```

Expected:
- Existing config tests pass. If no focused config test exists, run `cargo test -p brewfs --lib vfs::config`.

## Task 3: Gate Foreground Writes On Pending-Upload Backlog

**Files:**
- Modify: `/mnt/slayerfs/src/vfs/io/writer.rs`
- Test: `/mnt/slayerfs/src/vfs/io/writer.rs`

- [ ] **Step 1: Write the failing backpressure test**

Add:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_writeback_backpressure_waits_when_recent_pending_exceeds_soft_limit() {
    let layout = ChunkLayout {
        chunk_size: 8 * 1024,
        block_size: 4 * 1024,
    };
    let store = Arc::new(BlockingStore::new(true));
    let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
    let meta = meta_handle.layer();
    let backend = Arc::new(Backend::new(store.clone(), meta.clone()));
    let ino = meta
        .create_file(1, "backpressure_waits.txt".to_string())
        .await
        .unwrap();
    let inode = Inode::new(ino, 0);
    let reader = Arc::new(DataReader::new(
        Arc::new(ReadConfig::new(layout)),
        backend.clone(),
    ));
    let config = Arc::new(
        WriteConfig::new(layout)
            .freeze_min_bytes(4096)
            .writeback_mode(WriteBackMode::CommitBeforeUpload)
            .writeback_recent_pending_soft_limit(layout.block_size as u64),
    );
    let data_writer = Arc::new(DataWriter::new(config, backend, reader, None));
    let file_writer = data_writer.ensure_file(inode);

    file_writer
        .write_at(0, &vec![1u8; layout.block_size as usize])
        .await
        .unwrap();
    file_writer.flush().await.unwrap();

    let blocked = {
        let writer = file_writer.clone();
        tokio::spawn(async move {
            writer
                .write_at(layout.block_size as u64, &vec![2u8; 512])
                .await
        })
    };

    sleep(Duration::from_millis(50)).await;
    assert!(!blocked.is_finished(), "write should wait while pending backlog is at the soft limit");

    store.unblock();
    timeout(Duration::from_secs(2), blocked)
        .await
        .expect("write should wake after pending upload drains")
        .unwrap()
        .unwrap();
}
```

- [ ] **Step 2: Verify the test fails**

Run:

```bash
cd /mnt/slayerfs/brewfs
cargo test -p brewfs vfs::io::writer::tests::test_writeback_backpressure_waits_when_recent_pending_exceeds_soft_limit --lib
```

Expected:
- FAIL because no backpressure wait exists.

- [ ] **Step 3: Add admission wait**

Add this method to `FileWriter`:

```rust
async fn wait_for_writeback_backpressure(&self, incoming_len: usize) {
    if !matches!(self.shared.config.writeback_mode, WriteBackMode::CommitBeforeUpload) {
        return;
    }
    let soft = self.shared.config.writeback_recent_pending_soft_limit;
    if soft == 0 {
        return;
    }
    let incoming = incoming_len as u64;
    loop {
        let pending = self
            .shared
            .recent_pending_upload_bytes
            .load(Ordering::Acquire);
        if pending.saturating_add(incoming) <= soft {
            return;
        }
        self.shared.recent_pending_notify.notified().await;
    }
}
```

At the start of `FileWriter::write_at()`, before entering the write lock, call:

```rust
self.wait_for_writeback_backpressure(buf.len()).await;
```

Expected:
- The unit test passes.
- Default behavior remains unchanged because the soft limit defaults to `0`.

- [ ] **Step 4: Verify**

Run:

```bash
cd /mnt/slayerfs/brewfs
cargo test -p brewfs vfs::io::writer::tests::test_writeback_backpressure_waits_when_recent_pending_exceeds_soft_limit --lib
cargo clippy --workspace --all-targets -- -D warnings
```

Expected:
- Both commands exit `0`.

## Task 4: Perf Search For Backpressure Limits

**Files:**
- No source edits unless a perf runner env propagation bug is found.

- [ ] **Step 1: Run conservative limit**

Run:

```bash
cd /mnt/slayerfs/brewfs
BREWFS_WRITEBACK_RECENT_PENDING_SOFT_BYTES=8589934592 \
BREWFS_WRITEBACK_RECENT_PENDING_HARD_BYTES=12884901888 \
PERF_FIO_SEQREAD_RUNTIME=10 \
PERF_FIO_SEQREAD_SIZE=8G \
PERF_FIO_SEQWRITE_RUNTIME=10 \
PERF_FIO_SEQWRITE_SIZE=8G \
bash docker/compose-xfstests/run_redis_perf.sh \
  --writeback-throughput-profile \
  --tools "fio-seqread fio-seqwrite"
```

Expected:
- Exit code `0`.
- `fio-seqwrite >= 1336 MiB/s`.
- `brewfs_writeback_recent_pending_upload_bytes <= 11459592192`.

- [ ] **Step 2: Run aggressive limit only if conservative passes**

Run:

```bash
cd /mnt/slayerfs/brewfs
BREWFS_WRITEBACK_RECENT_PENDING_SOFT_BYTES=6442450944 \
BREWFS_WRITEBACK_RECENT_PENDING_HARD_BYTES=9663676416 \
PERF_FIO_SEQREAD_RUNTIME=10 \
PERF_FIO_SEQREAD_SIZE=8G \
PERF_FIO_SEQWRITE_RUNTIME=10 \
PERF_FIO_SEQWRITE_SIZE=8G \
bash docker/compose-xfstests/run_redis_perf.sh \
  --writeback-throughput-profile \
  --tools "fio-seqread fio-seqwrite"
```

Expected:
- Accept this limit only if it keeps `fio-seqwrite >= 1336 MiB/s`.

- [ ] **Step 3: Extract results**

Run:

```bash
cd /mnt/slayerfs/brewfs
python3 - <<'PY'
import json, pathlib
root = max(pathlib.Path("docker/compose-xfstests/artifacts").glob("perf-run-*"), key=lambda p: p.stat().st_mtime)
print("artifact", root)
for name in ["fio-seqread", "fio-seqwrite", "fio-randread", "fio-randwrite", "fio-randrw"]:
    p = root / "results" / f"{name}.json"
    if not p.exists():
        continue
    job = json.loads(p.read_text())["jobs"][0]
    print(name, "error", job.get("error"), "elapsed", job.get("elapsed"))
    for op in ["read", "write"]:
        data = job.get(op, {})
        if data.get("bw_bytes", 0):
            print(op, f"{data['bw_bytes'] / 1048576:.2f} MiB/s")
for stats in sorted((root / "diagnostics").glob("stats-fio-seqwrite-after.txt")):
    print(stats)
    for line in stats.read_text().splitlines():
        if line.startswith(("brewfs_writeback_", "brewfs_s3_put_bytes_total")):
            print(line)
PY
```

Expected:
- Record artifact, throughput, dirty bytes, and accept/reject decision.

## Task 5: Commit Accepted Backpressure

**Files:**
- Modify only files from accepted tasks.

- [ ] **Step 1: Verify final commands**

Run:

```bash
cd /mnt/slayerfs/brewfs
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
bash docker/compose-xfstests/run_redis_perf.sh --writeback-throughput-profile
```

Expected:
- All commands exit `0`.
- Perf gates in "New Targets" pass.

- [ ] **Step 2: Commit and push**

Run:

```bash
cd /mnt/slayerfs/brewfs
git add src/vfs/config.rs src/vfs/io/writer.rs src/vfs/stats.rs src/vfs/fs/mod.rs
git commit -m "perf: add writeback pending-upload backpressure"
git push origin main
```

Expected:
- Commit is pushed only if the gates pass.

## Stop Conditions

Stop and report instead of coding more when any condition occurs:
- Baseline reproduction before code changes is unstable or below `1000 MiB/s` seqwrite.
- Backpressure causes any read EIO.
- Backpressure causes a `flush timeout`.
- Three code-level attempts fail acceptance.
- Full perf does not exit cleanly.

If stopped, write the next architectural blocker note into this plan under "Execution Notes".

## Execution Notes

- 2026-06-05: Direct staged-page release was rejected. It caused `fio-seqread` EIO and writer flush timeout.
- 2026-06-05: Per-block staged marking without release was rejected. It did not pass full perf and caused `fio-randread` to hang.
- 2026-06-05: Stats-only staged fields were rejected. They produced no staged signal and no performance improvement.
- Next execution must begin with Task 0 baseline reproduction. If Task 0 is slow or unstable, diagnose environment/config before editing code.
