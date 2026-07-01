# BrewFS Writeback Foreground Gap Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce the BrewFS write-path gap against JuiceFS without weakening existing writeback correctness or breaking the current unit, pjdfstest, xfstests, and perf gates.

**Architecture:** This plan intentionally starts with low-risk correctness and observability work before changing write admission policy. First make the writeback throughput profile explicit in config/artifacts, then fix recent-pending accounting so backpressure decisions use stable bytes, then add metrics that explain foreground `commit_wait_upload` time. Only after those pass do we attempt an opt-in low-risk admission tweak; small-write coalescing remains a follow-up plan because it changes slice ordering behavior.

**Tech Stack:** Rust 2024, Tokio, FUSE, BrewFS VFS writer, Redis/S3 compose perf runners, fio, Git LFS artifacts.

---

## Files

- Modify: `src/config.rs`
  - Add optional YAML fields for writeback pending soft/hard limits so perf artifacts show the real profile instead of relying on invisible environment variables.
- Modify: `src/vfs/cache/config.rs`
  - Store writeback pending soft/hard limits in `CacheConfig`.
- Modify: `src/vfs/config.rs`
  - Thread the new `CacheConfig` fields into `WriteConfig`.
- Modify: `src/vfs/io/writer.rs`
  - Add stable `recent_pending_accounted_bytes`.
  - Clear recent-pending accounting on upload completion, failure, and explicit clear/drop paths.
  - Add focused tests around failed upload accounting and partial slice accounting.
- Modify: `docker/compose-xfstests/run_perf_in_container.sh`
  - Write pending soft/hard settings into generated `backend.yml`.
  - Stop advertising unused `BREWFS_WRITEBACK_UPLOAD_CONCURRENCY` as if it controlled writer upload permits.
- Modify: `docker/compose-xfstests/run_redis_perf.sh`
  - Align help text with the actual effective knobs.
- Modify: `doc/operations/configuration.md`
  - Document the explicit writeback pending fields and warn that `commit_before_upload` is weak close-to-open unless strict drain is enabled later.

## Guardrails

- Do not change `WriteBackMode::UploadBeforeCommit` behavior.
- Do not make `CommitBeforeUpload` wait for S3 upload completion in `flush()` or `close()` in this first pass; existing tests assert fast flush while object upload is blocked.
- Do not implement small-write coalescing in this plan. It has higher risk around overlap ordering, truncate epochs, mmap writeback, and FUSE unique ordering.
- Each task must run its narrow unit test before the next task. Run the full focused writer/config tests before any commit.

### Task 1: Make Writeback Pending Limits Explicit

**Files:**
- Modify: `src/vfs/cache/config.rs`
- Modify: `src/config.rs`
- Modify: `src/vfs/config.rs`
- Modify: `docker/compose-xfstests/run_perf_in_container.sh`
- Modify: `docker/compose-xfstests/run_redis_perf.sh`
- Test: `src/config.rs`
- Test: `src/vfs/config.rs`

- [ ] **Step 1: Add failing config parse expectations**

In `src/config.rs`, extend `mount_config_parses_cache_section` by adding these YAML fields under `cache:`:

```yaml
  writeback_recent_pending_soft_bytes: 1073741824
  writeback_recent_pending_hard_bytes: 2147483648
```

Then add assertions after `assert!(!config.cache.writeback_persist_sync);`:

```rust
        assert_eq!(config.cache.writeback_recent_pending_soft_bytes, 1073741824);
        assert_eq!(config.cache.writeback_recent_pending_hard_bytes, 2147483648);
```

In `src/vfs/config.rs`, extend `vfs_config_applies_cache_budget_knobs` by setting:

```rust
            writeback_recent_pending_soft_bytes: 123,
            writeback_recent_pending_hard_bytes: 456,
```

Then add assertions after `assert_eq!(config.write.upload_concurrency, cache.upload_concurrency);`:

```rust
        assert_eq!(config.write.writeback_recent_pending_soft_limit, 123);
        assert_eq!(config.write.writeback_recent_pending_hard_limit, 456);
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```bash
cargo test -p brewfs mount_config_parses_cache_section vfs_config_applies_cache_budget_knobs -- --nocapture
```

Expected: fail because `CacheConfig` does not yet have `writeback_recent_pending_soft_bytes` / `writeback_recent_pending_hard_bytes`.

- [ ] **Step 3: Add config fields and parsing**

In `src/vfs/cache/config.rs`, add fields after `writeback_mode`:

```rust
    pub writeback_recent_pending_soft_bytes: u64,
    pub writeback_recent_pending_hard_bytes: u64,
```

Set defaults after `writeback_mode: WriteBackMode::UploadBeforeCommit,`:

```rust
            writeback_recent_pending_soft_bytes: 0,
            writeback_recent_pending_hard_bytes: 0,
```

In `src/config.rs`, add fields to `CacheFileConfig` after `writeback_persist_sync`:

```rust
    pub writeback_recent_pending_soft_bytes: Option<u64>,
    pub writeback_recent_pending_hard_bytes: Option<u64>,
```

Apply them in `CacheFileConfig::apply_to` after `writeback_persist_sync`:

```rust
        if let Some(writeback_recent_pending_soft_bytes) =
            self.writeback_recent_pending_soft_bytes
        {
            cache.writeback_recent_pending_soft_bytes = writeback_recent_pending_soft_bytes;
        }
        if let Some(writeback_recent_pending_hard_bytes) =
            self.writeback_recent_pending_hard_bytes
        {
            cache.writeback_recent_pending_hard_bytes = writeback_recent_pending_hard_bytes;
        }
```

In `src/vfs/config.rs`, extend the `WriteConfig` builder chain in `VFSConfig::new_with_cache_config`:

```rust
                .writeback_mode(cache.writeback_mode)
                .writeback_recent_pending_soft_limit(
                    cache.writeback_recent_pending_soft_bytes,
                )
                .writeback_recent_pending_hard_limit(
                    cache.writeback_recent_pending_hard_bytes,
                ),
```

- [ ] **Step 4: Write generated YAML fields**

In `docker/compose-xfstests/run_perf_in_container.sh`, add both values to the generated `cache:` section after `writeback_persist_sync`:

```bash
            [[ -n "${BREWFS_WRITEBACK_RECENT_PENDING_SOFT_BYTES:-}" ]] && echo "  writeback_recent_pending_soft_bytes: ${BREWFS_WRITEBACK_RECENT_PENDING_SOFT_BYTES}"
            [[ -n "${BREWFS_WRITEBACK_RECENT_PENDING_HARD_BYTES:-}" ]] && echo "  writeback_recent_pending_hard_bytes: ${BREWFS_WRITEBACK_RECENT_PENDING_HARD_BYTES}"
```

Update the `cache` section predicate so these env vars cause the section to be emitted:

```bash
            || -n "${BREWFS_WRITEBACK_RECENT_PENDING_SOFT_BYTES:-}" \
            || -n "${BREWFS_WRITEBACK_RECENT_PENDING_HARD_BYTES:-}" \
```

In `docker/compose-xfstests/run_redis_perf.sh`, change the `--writeback-throughput-profile` help line to avoid implying the unused `BREWFS_WRITEBACK_UPLOAD_CONCURRENCY` controls writer upload permits:

```text
                             启用 S3 writeback 全场景吞吐 profile（4GiB read/write buffer, 12GiB memory budget, S3 max concurrency=16, writer upload_concurrency=32, pending soft/hard=1GiB/2GiB, writeback persist fsync=false, compression=lz4, fuse workers=6, fio prefill drain+remount）
```

Keep exporting `BREWFS_WRITEBACK_UPLOAD_CONCURRENCY` for compatibility, but do not rely on it for pass/fail.

- [ ] **Step 5: Run focused config tests**

Run:

```bash
cargo test -p brewfs mount_config_parses_cache_section vfs_config_applies_cache_budget_knobs -- --nocapture
```

Expected: both tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/config.rs src/vfs/cache/config.rs src/vfs/config.rs docker/compose-xfstests/run_perf_in_container.sh docker/compose-xfstests/run_redis_perf.sh
git commit -m "config: expose writeback pending limits"
```

### Task 2: Make Recent Pending Accounting Stable

**Files:**
- Modify: `src/vfs/io/writer.rs`
- Test: `src/vfs/io/writer.rs`

- [ ] **Step 1: Add failing partial accounting test**

Add this test near `test_recent_pending_upload_accounting_tracks_commit_and_upload_completion`:

```rust
    #[tokio::test]
    async fn test_recent_pending_upload_accounting_uses_stable_logical_bytes() {
        let layout = ChunkLayout {
            chunk_size: 8 * 1024,
            block_size: 4 * 1024,
        };
        let store = Arc::new(BlockingStore::new(true));
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta = meta_handle.layer();
        let backend = Arc::new(Backend::new(store.clone(), meta.clone()));
        let ino = meta
            .create_file(1, "pending_upload_partial_accounting.txt".to_string())
            .await
            .unwrap();
        let inode = Inode::new(ino, 0);
        let reader = Arc::new(DataReader::new(
            Arc::new(ReadConfig::new(layout)),
            backend.clone(),
        ));
        let writer = Arc::new(DataWriter::new(
            test_config_with_writeback(layout, WriteBackMode::CommitBeforeUpload),
            backend,
            reader,
            None,
        ));
        let file_writer = writer.ensure_file(inode);

        file_writer.write_at(0, &vec![3u8; 1536]).await.unwrap();
        timeout(Duration::from_secs(2), file_writer.flush())
            .await
            .expect("commit-before-upload flush should return while object upload is blocked")
            .unwrap();

        assert_eq!(
            writer.recent_pending_upload_bytes(),
            1536,
            "pending upload accounting should use the committed logical slice length"
        );

        store.unblock();
        timeout(Duration::from_secs(2), async {
            loop {
                if writer.recent_pending_upload_bytes() == 0 {
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("pending-upload bytes should clear after upload completes");
    }
```

- [ ] **Step 2: Run test and verify it fails**

Run:

```bash
cargo test -p brewfs test_recent_pending_upload_accounting_uses_stable_logical_bytes -- --nocapture
```

Expected: fail because current accounting uses `state.data.alloc_bytes()` rather than logical length.

- [ ] **Step 3: Add stable accounted bytes to `SliceState`**

In `SliceState`, add after `recent_pending_accounted: bool`:

```rust
    recent_pending_accounted_bytes: u64,
```

Initialize it in `SliceState::new`:

```rust
            recent_pending_accounted_bytes: 0,
```

- [ ] **Step 4: Replace add/subtract logic**

Replace `clear_recent_pending_if_complete` with:

```rust
    fn clear_recent_pending_accounting(&self, s: &mut SliceState) {
        if !s.recent_pending_accounted {
            return;
        }
        let bytes = s.recent_pending_accounted_bytes;
        s.recent_pending_accounted = false;
        s.recent_pending_accounted_bytes = 0;
        if bytes > 0 {
            self.shared
                .recent_pending_upload
                .bytes
                .fetch_sub(bytes, Ordering::AcqRel);
        }
        self.shared.recent_pending_upload.notify.notify_waiters();
    }

    fn clear_recent_pending_if_complete(&self, s: &mut SliceState) {
        if s.upload_complete() {
            self.clear_recent_pending_accounting(s);
        }
    }
```

Update `account_recent_pending_if_needed`:

```rust
        let bytes = {
            let mut state = slice.lock();
            if state.recent_pending_accounted || state.upload_complete() {
                0
            } else {
                let bytes = state.data.len();
                state.recent_pending_accounted = true;
                state.recent_pending_accounted_bytes = bytes;
                bytes
            }
        };
```

- [ ] **Step 5: Clear accounting on failure**

Update `mark_failed` so the failure path cannot leave pending bytes stuck:

```rust
        self.with_mut(|s| {
            s.state = SliceStatus::Failed;
            s.in_flight = 0;
            s.upload_task_active = false;
            s.err = Some(message.clone());
            self.clear_recent_pending_accounting(s);
            s.notify.notify_waiters();
        });
```

- [ ] **Step 6: Run focused accounting tests**

Run:

```bash
cargo test -p brewfs test_recent_pending_upload_accounting -- --nocapture
```

Expected: all matching pending-accounting tests pass.

- [ ] **Step 7: Commit**

```bash
git add src/vfs/io/writer.rs
git commit -m "fix: stabilize writeback pending accounting"
```

### Task 3: Ensure Failed Uploads Clear Pending Without Weakening Error Visibility

**Files:**
- Modify: `src/vfs/io/writer.rs`
- Test: `src/vfs/io/writer.rs`

- [ ] **Step 1: Add failing failed-upload pending test**

Add this test near `test_flush_reports_upload_failure`:

```rust
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_commit_before_upload_failed_upload_clears_recent_pending() {
        let layout = ChunkLayout {
            chunk_size: 8 * 1024,
            block_size: 4 * 1024,
        };
        let store = Arc::new(FailingStore);
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta = meta_handle.layer();
        let backend = Arc::new(Backend::new(store, meta.clone()));
        let ino = meta
            .create_file(1, "failed_upload_clears_pending.txt".to_string())
            .await
            .unwrap();
        let inode = Inode::new(ino, 0);
        let reader = Arc::new(DataReader::new(
            Arc::new(ReadConfig::new(layout)),
            backend.clone(),
        ));
        let writer = Arc::new(DataWriter::new(
            test_config_with_writeback(layout, WriteBackMode::CommitBeforeUpload),
            backend,
            reader,
            None,
        ));
        let file_writer = writer.ensure_file(inode);

        file_writer
            .write_at(0, &vec![1u8; layout.block_size as usize])
            .await
            .unwrap();

        let err = timeout(Duration::from_secs(2), file_writer.flush())
            .await
            .expect("flush should not hang after injected upload failure")
            .expect_err("upload failure should still be visible to flush");
        assert!(
            err.to_string().contains("writeback failed"),
            "unexpected flush error: {err:?}"
        );
        assert_eq!(
            writer.recent_pending_upload_bytes(),
            0,
            "failed upload must not leave recent pending bytes that throttle later writes forever"
        );
        assert!(
            file_writer.has_pending().await,
            "writeback error should remain observable by later flush/fsync/close calls"
        );
    }
```

- [ ] **Step 2: Run test and verify behavior**

Run:

```bash
cargo test -p brewfs test_commit_before_upload_failed_upload_clears_recent_pending -- --nocapture
```

Expected before Task 2 implementation: fail or expose pending accounting not being cleared. Expected after Task 2 implementation: pass while preserving `has_pending()`.

- [ ] **Step 3: Run existing failure tests**

Run:

```bash
cargo test -p brewfs test_flush_reports_upload_failure test_commit_before_upload_requires_writeback_stage_success -- --nocapture
```

Expected: both pass. These protect against accidentally hiding writeback errors.

- [ ] **Step 4: Commit**

```bash
git add src/vfs/io/writer.rs
git commit -m "test: cover writeback failed-upload accounting"
```

### Task 4: Add Report-Only Foreground Wait Ratios

**Files:**
- Modify: `docker/compose-xfstests/run_perf_in_container.sh`
- Test: manual report generation on existing artifact or short perf run

- [ ] **Step 1: Extend report table locally**

In the report generator section of `docker/compose-xfstests/run_perf_in_container.sh`, add a derived `commit_wait_upload_s` and `stage_s` column to the BrewFS stats output. Use existing metrics:

```python
stage_s = metrics.get("brewfs_writeback_stage_lat_us_total", 0.0) / 1_000_000.0
commit_wait_s = metrics.get("brewfs_writeback_commit_wait_upload_us_total", 0.0) / 1_000_000.0
```

Add them to the table as:

```python
f"stage={stage_s:.2f}s, commit_wait={commit_wait_s:.2f}s"
```

Do not change runner pass/fail behavior in this task.

- [ ] **Step 2: Verify script syntax**

Run:

```bash
bash -n docker/compose-xfstests/run_perf_in_container.sh
```

Expected: exit 0.

- [ ] **Step 3: Run a short write-only smoke perf**

Run:

```bash
PERF_FIO_DIRECT=1 PERF_FIO_RUNTIME=5 PERF_FIO_SIZE=128m PERF_LOG_TO_CONSOLE=false \
  bash docker/compose-xfstests/run_redis_perf.sh --s3 --writeback-throughput-profile \
  --tools "fio-seqwrite fio-randwrite"
```

Expected:
- Both tools pass.
- `report.md` includes stage and commit-wait values.
- No new pending/writeback errors appear in `brewfs.log`.

- [ ] **Step 4: Commit**

```bash
git add docker/compose-xfstests/run_perf_in_container.sh
git commit -m "perf: report writeback foreground wait time"
```

### Task 5: Opt-In Hysteresis Admission Experiment

**Files:**
- Modify: `src/vfs/io/writer.rs`
- Test: `src/vfs/io/writer.rs`

- [ ] **Step 1: Add pure decision tests**

Add tests near `test_writeback_backpressure_decision_uses_soft_sleep_before_hard_wait`:

```rust
    #[test]
    fn test_writeback_backpressure_hysteresis_blocks_until_low_watermark() {
        assert!(decide_writeback_backpressure_hysteresis(
            2048,
            512,
            1024,
            2048,
        ));
        assert!(!decide_writeback_backpressure_hysteresis(
            900,
            512,
            1024,
            2048,
        ));
    }
```

- [ ] **Step 2: Run test and verify it fails**

Run:

```bash
cargo test -p brewfs test_writeback_backpressure_hysteresis_blocks_until_low_watermark -- --nocapture
```

Expected: fail because `decide_writeback_backpressure_hysteresis` does not exist.

- [ ] **Step 3: Add opt-in policy enum**

Add near `WritebackBackpressureDecision`:

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WritebackBackpressurePolicy {
    Current,
    Hysteresis,
}

fn writeback_backpressure_policy() -> WritebackBackpressurePolicy {
    match std::env::var("BREWFS_WRITEBACK_BACKPRESSURE_POLICY")
        .ok()
        .as_deref()
    {
        Some("hysteresis") => WritebackBackpressurePolicy::Hysteresis,
        _ => WritebackBackpressurePolicy::Current,
    }
}

fn decide_writeback_backpressure_hysteresis(
    pending: u64,
    incoming: u64,
    low_limit: u64,
    high_limit: u64,
) -> bool {
    high_limit > low_limit && pending.saturating_add(incoming) >= high_limit && pending > low_limit
}
```

- [ ] **Step 4: Use policy in wait path without changing default behavior**

At the start of `wait_for_writeback_backpressure`, after reading `soft` and `hard`, add:

```rust
        if matches!(
            writeback_backpressure_policy(),
            WritebackBackpressurePolicy::Hysteresis
        ) && hard > soft
        {
            loop {
                self.shared.writeback_result()?;
                let pending = self
                    .shared
                    .recent_pending_upload
                    .bytes
                    .load(Ordering::Acquire);
                if !decide_writeback_backpressure_hysteresis(pending, incoming_len as u64, soft, hard)
                {
                    return Ok(());
                }
                let start = Instant::now();
                self.shared.recent_pending_upload.notify.notified().await;
                self.shared
                    .recent_pending_upload
                    .record_hard_wait(start.elapsed());
            }
        }
```

Default remains the current soft sleep / hard wait logic.

- [ ] **Step 5: Run focused tests**

Run:

```bash
cargo test -p brewfs test_writeback_backpressure_decision test_writeback_backpressure_hysteresis_blocks_until_low_watermark test_writeback_backpressure_waits_for_pending_upload_to_drain -- --nocapture
```

Expected: pass.

- [ ] **Step 6: Run short A/B perf**

Baseline:

```bash
PERF_FIO_DIRECT=1 PERF_FIO_RUNTIME=10 PERF_FIO_SIZE=256m PERF_LOG_TO_CONSOLE=false \
  bash docker/compose-xfstests/run_redis_perf.sh --s3 --writeback-throughput-profile \
  --tools "fio-seqwrite fio-randwrite fio-randrw"
```

Experiment:

```bash
BREWFS_WRITEBACK_BACKPRESSURE_POLICY=hysteresis \
PERF_FIO_DIRECT=1 PERF_FIO_RUNTIME=10 PERF_FIO_SIZE=256m PERF_LOG_TO_CONSOLE=false \
  bash docker/compose-xfstests/run_redis_perf.sh --s3 --writeback-throughput-profile \
  --tools "fio-seqwrite fio-randwrite fio-randrw"
```

Accept only if:
- `fio-randrw` still passes.
- `fio-randwrite` write p99 does not regress by more than 25%.
- Script wall time for `fio-randwrite` or `fio-randrw` improves or commit-wait/stage metrics clearly improve.

- [ ] **Step 7: Commit only if accepted**

```bash
git add src/vfs/io/writer.rs
git commit -m "perf: add opt-in writeback hysteresis policy"
```

If perf does not improve, revert the Task 5 diff only:

```bash
git restore --source=HEAD -- src/vfs/io/writer.rs
```

## Required Verification Before Merging

Run focused tests:

```bash
cargo test -p brewfs mount_config_parses_cache_section vfs_config_applies_cache_budget_knobs -- --nocapture
cargo test -p brewfs test_recent_pending_upload_accounting -- --nocapture
cargo test -p brewfs test_commit_before_upload_failed_upload_clears_recent_pending test_flush_reports_upload_failure test_commit_before_upload_requires_writeback_stage_success -- --nocapture
cargo test -p brewfs test_writeback_backpressure_decision test_writeback_backpressure_waits_for_pending_upload_to_drain -- --nocapture
```

Run broader checks:

```bash
cargo fmt --check
cargo clippy -p brewfs --all-targets --all-features -- -D warnings
cargo test -p brewfs --bin brewfs
```

Run perf gate:

```bash
PERF_FIO_DIRECT=1 PERF_FIO_RUNTIME=20 PERF_FIO_SIZE=512m PERF_LOG_TO_CONSOLE=false \
  bash docker/compose-xfstests/run_redis_perf.sh --s3 --writeback-throughput-profile \
  --tools "fio-bigwrite fio-bigread fio-seqread fio-seqwrite fio-randread fio-randwrite fio-randrw"
```

Compare against the current README snapshot:

- `fio-seqread` should remain near parity with the previous BrewFS result.
- `fio-randread` must still pass.
- `fio-randwrite` must not regress wall time or write p99 by more than 25%.
- `fio-randrw` must pass and not show a new long hang or write p99 spike.
- Any accepted performance claim must cite the new artifact directory and exact env.

## Follow-Up Plan Boundary

Do not implement per-chunk small-write coalescing in this plan. If Tasks 1-5 do not materially reduce the write-path gap, create a separate plan for a feature-flagged coalescer that covers:

- FUSE unique ordering.
- overlapping writes and last-writer-wins.
- truncate/setattr epoch invalidation.
- mmap/writeback cached writes.
- `pjdfstest` and xfstests regression coverage.

## Self-Review

- Spec coverage: The plan addresses the suspected writeback misconfiguration, current foreground wait root cause, and performance validation without changing default writeback semantics in one large step.
- Placeholder scan: No task uses TBD/TODO/fill-in-later language.
- Type consistency: Field names are consistently `writeback_recent_pending_soft_bytes` / `writeback_recent_pending_hard_bytes` in `CacheConfig` and map to `WriteConfig` soft/hard limits.
