// Write pipeline (high-level):
// - FileWriter::write_at splits a file write into chunk spans and appends data into SliceState
//   (Writeable). Slices are append-only and live inside each ChunkState.
// - When a slice is frozen (Readonly), it becomes eligible for upload. auto_flush and explicit
//   flush() can freeze slices. spawn_flush_slice performs the upload:
//     Readonly -> Uploading -> Uploaded/Failed
// - commit_chunk runs per-chunk and waits for Uploaded slices. It appends metadata (SliceDesc)
//   to the metadata layer and marks them Committed. Only Committed slices are visible to readers.
// - FileWriter::flush() freezes all slices and waits until commit threads drain the chunks.
//   While flushing, new writes are blocked via flush_waiting/write_waiting gates.

use super::reader::DataReader;
use crate::chunk::writer::{DataUploader, UploadPriority};
use crate::chunk::{BlockStore, SliceDesc};
use crate::meta::backoff::backoff;
use crate::meta::store::{MetaError, RetryReason};
use crate::meta::{MetaLayer, SLICE_ID_KEY};
use crate::utils::{NumCastExt, UsageGuard};
use crate::vfs::Inode;
use crate::vfs::backend::Backend;
use crate::vfs::cache::config::WriteBackMode;
use crate::vfs::cache::page::CacheSlice;
use crate::vfs::cache::page::WriteAction as PageWriteAction;
use crate::vfs::cache::write_back::WriteBackCache;
use crate::vfs::chunk_id_for;
use crate::vfs::config::WriteConfig;
use crate::vfs::extract_ino_and_chunk_index;
use crate::vfs::io::split_chunk_spans;
use crate::vfs::memory::{MemoryBudget, MemoryConsumer, MemoryUsageGuard, PressureLevel};
use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::Mutex as ParkingMutex;
use rand::RngCore;
use std::collections::{BTreeMap, VecDeque};
use std::fmt::Display;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Weak};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Notify, Semaphore};
use tokio::task::JoinSet;
use tokio::time::{interval, timeout};
use tracing::{Instrument, warn};

const FLUSH_DURATION: Duration = Duration::from_secs(5);
const COMMIT_WAIT_SLICE: Duration = Duration::from_millis(100);
const FLUSH_WAIT: Duration = Duration::from_secs(3);
const FLUSH_DEADLINE: Duration = Duration::from_secs(300);
const TRUNCATE_FLUSH_DEADLINE: Duration = Duration::from_secs(10);
/// Shorter deadline for close-triggered flushes.  FUSE already calls flush()
/// before close(), so close() only needs to drain residual in-flight work.
const CLOSE_FLUSH_DEADLINE: Duration = Duration::from_secs(5);
/// Maximum time commit_chunk will wait for a single slice's upload before
/// marking it failed.  Prevents indefinite hangs on stalled S3 connections.
const COMMIT_UPLOAD_MAX_WAIT: Duration = Duration::from_secs(180);
const UPLOAD_MAX_RETRIES: u64 = 5;
const COMMIT_RETRY_BASE_MS: u64 = 20;
const COMMIT_RETRY_MAX_MS: u64 = 2000;
const COMMIT_META_MAX_RETRIES: u32 = 15;
// High-throughput foreground writes can race with background freeze/flush for
// several scheduler turns; keep this below "spin forever", but high enough not
// to surface an internal slice handoff as EIO.
const WRITE_SLICE_MAX_RETRIES: u32 = 1024;
/// Maximum age of a Writable slice before auto_flush freezes it and starts
/// background upload, regardless of idle time.  For S3 backends, a longer
/// threshold aggregates more data per slice, reducing small-object PUT
/// amplification.  fsync/close still force-seal immediately.
/// NOTE: This is the fallback; prefer config.auto_flush_max_age when available.
const AUTO_FLUSH_MAX_AGE: Duration = Duration::from_millis(2000);

const MAX_UNFLUSHED_SLICES: usize = 3;
const MAX_SLICES_THRESHOLD: usize = 800;
const WRITE_MAX_WAIT: Duration = Duration::from_secs(30);
const WRITEBACK_WRITE_MAX_WAIT: Duration = Duration::from_secs(300);
const CACHED_SUB_BLOCK_IDLE_GRACE: Duration = Duration::from_secs(3);
const CACHED_SUB_BLOCK_TOO_MANY_MIN_AGE: Duration = Duration::from_secs(1);
const CACHED_SUB_BLOCK_AUTO_FREEZE_MIN_AGE: Duration = Duration::from_secs(10);
const WRITEBACK_SOFT_BACKPRESSURE_MIN_SLEEP: Duration = Duration::from_millis(1);
const WRITEBACK_SOFT_BACKPRESSURE_MAX_SLEEP: Duration = Duration::from_millis(6);
const WRITEBACK_HARD_BACKPRESSURE_MAX_SLEEP: Duration = Duration::from_millis(6);
/// Minimum number of bytes a Writable slice must hold before `should_freeze`
/// returns true on a size basis.  32 MiB gives 8 blocks per upload batch,
/// maximizing pipeline parallelism while keeping flush latency reasonable.
/// fsync/close bypass this threshold and force-seal regardless of size.
/// NOTE: This is the fallback; prefer config.freeze_min_bytes when available.
const SHOULD_FREEZE_MIN_BYTES: u64 = 8 * 1024 * 1024;

static CACHED_SUB_BLOCK_IDLE_GRACE_CONFIG: LazyLock<Duration> = LazyLock::new(|| {
    env_duration_ms(
        "BREWFS_CACHED_SUB_BLOCK_IDLE_GRACE_MS",
        CACHED_SUB_BLOCK_IDLE_GRACE,
    )
});

static CACHED_SUB_BLOCK_TOO_MANY_MIN_AGE_CONFIG: LazyLock<Duration> = LazyLock::new(|| {
    env_duration_ms(
        "BREWFS_CACHED_SUB_BLOCK_TOO_MANY_MIN_AGE_MS",
        CACHED_SUB_BLOCK_TOO_MANY_MIN_AGE,
    )
});

fn env_duration_ms(name: &str, default: Duration) -> Duration {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(default)
}

fn cached_sub_block_idle_grace() -> Duration {
    *CACHED_SUB_BLOCK_IDLE_GRACE_CONFIG
}

fn cached_sub_block_too_many_min_age() -> Duration {
    *CACHED_SUB_BLOCK_TOO_MANY_MIN_AGE_CONFIG
}

enum WritebackBackpressureDecision {
    Allow,
    SoftSleep(Duration),
    Wait,
}

fn decide_writeback_backpressure(
    pending: u64,
    incoming: u64,
    soft_limit: u64,
    hard_limit: u64,
) -> WritebackBackpressureDecision {
    if soft_limit == 0 {
        return WritebackBackpressureDecision::Allow;
    }

    let projected = pending.saturating_add(incoming);
    if projected <= soft_limit {
        return WritebackBackpressureDecision::Allow;
    }

    if hard_limit > soft_limit && projected <= hard_limit {
        let over_soft = projected.saturating_sub(soft_limit);
        let soft_range = hard_limit - soft_limit;
        let sleep_span_ms = (WRITEBACK_SOFT_BACKPRESSURE_MAX_SLEEP
            - WRITEBACK_SOFT_BACKPRESSURE_MIN_SLEEP)
            .as_millis() as u64;
        let extra_ms =
            ((over_soft as u128) * (sleep_span_ms as u128) / (soft_range as u128)) as u64;
        return WritebackBackpressureDecision::SoftSleep(
            WRITEBACK_SOFT_BACKPRESSURE_MIN_SLEEP + Duration::from_millis(extra_ms),
        );
    }

    WritebackBackpressureDecision::Wait
}

fn write_buffer_max_wait(mode: WriteBackMode) -> Duration {
    match mode {
        WriteBackMode::CommitBeforeUpload => WRITEBACK_WRITE_MAX_WAIT,
        WriteBackMode::UploadBeforeCommit => WRITE_MAX_WAIT,
    }
}

fn truncate_flush_deadline() -> Duration {
    std::env::var("BREWFS_TRUNCATE_FLUSH_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|seconds| *seconds > 0)
        .map(Duration::from_secs)
        .unwrap_or(TRUNCATE_FLUSH_DEADLINE)
}

fn commit_retry_backoff(failures: u32) -> Duration {
    let exp = failures.saturating_sub(1).min(16);
    let step = COMMIT_RETRY_BASE_MS.checked_shl(exp).unwrap_or(u64::MAX);
    let base = step.min(COMMIT_RETRY_MAX_MS);
    // Scale jitter with base delay to spread retry bursts at higher backoff levels.
    let jitter_span = (base / 10).max(20);
    let jitter = rand::rng().next_u64() % (jitter_span.saturating_add(1));
    Duration::from_millis(base.saturating_add(jitter))
}

fn looks_retryable_backend_error(err: &impl Display) -> bool {
    let message = err.to_string().to_ascii_lowercase();
    [
        "deadlock",
        "database is locked",
        "database is busy",
        "serialization",
        "retry",
        "timeout",
        "timed out",
    ]
    .iter()
    .any(|needle| message.contains(needle))
}

fn should_retry_meta_write(err: &MetaError) -> bool {
    match err {
        MetaError::ContinueRetry(_) => true,
        MetaError::Database(err) => looks_retryable_backend_error(err),
        MetaError::Io(err) => matches!(
            err.kind(),
            std::io::ErrorKind::Interrupted
                | std::io::ErrorKind::TimedOut
                | std::io::ErrorKind::WouldBlock
                | std::io::ErrorKind::ConnectionAborted
                | std::io::ErrorKind::ConnectionReset
        ),
        _ => false,
    }
}

struct UploadPlan {
    chunk_id: u64,
    data: Vec<(usize, Vec<Bytes>)>,
    slice_id: Option<u64>,
    uploaded: u64,
    write_origin: WriteOriginKind,
}

async fn join_best_effort_persist<P, U, T>(
    persist: Option<P>,
    upload: U,
) -> (Option<anyhow::Result<()>>, T)
where
    P: Future<Output = anyhow::Result<()>>,
    U: Future<Output = T>,
{
    match persist {
        Some(persist) => {
            let (persist_result, upload_result) = tokio::join!(persist, upload);
            (Some(persist_result), upload_result)
        }
        None => (None, upload.await),
    }
}

async fn join_writeback_stage_then_upload<P, U, T>(persist: P, upload: U) -> (anyhow::Result<()>, T)
where
    P: Future<Output = anyhow::Result<()>>,
    U: Future<Output = T>,
{
    let persist_result = persist.await;
    let upload_result = upload.await;
    (persist_result, upload_result)
}

fn should_stage_first_writeback_upload(
    priority: UploadPriority,
    has_persist: bool,
    write_origin: WriteOriginKind,
) -> bool {
    has_persist
        && matches!(priority, UploadPriority::Writeback)
        && matches!(write_origin, WriteOriginKind::CachedOnly)
}

#[derive(Default, Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum SliceStatus {
    /// Writable: slice is writable and there may be uploaded blocks.
    #[default]
    Writable,
    /// Readonly: frozen, no more writes allowed.
    Readonly,
    /// Uploaded: data uploaded successfully.
    Uploaded,
    /// Failed: upload or metadata commit exhausted its retry budget.
    Failed,
    /// Committed: metadata committed.
    Committed,
}

#[derive(Debug, Clone, Copy)]
enum SliceFreezeReason {
    SizeOrChunkEnd,
    MaxUnflushed,
    ExplicitFlush,
    Auto,
    CommitAgeSafety,
}

#[derive(Debug, Clone, Copy)]
enum AutoFreezeTrigger {
    Age,
    Idle,
    Pressure,
    TooMany,
    BufferHigh,
    FlushDuration,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum WriteOrigin {
    Normal,
    Cached,
}

impl WriteOrigin {
    fn mask(self) -> u8 {
        match self {
            Self::Normal => 0b01,
            Self::Cached => 0b10,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WriteOriginKind {
    Unknown,
    NormalOnly,
    CachedOnly,
    Mixed,
}

pub(crate) struct SliceState {
    state: SliceStatus,
    /// ID of the chunk it belongs to.
    chunk_id: u64,
    /// ID of this slice (assigned on flush).
    slice_id: Option<u64>,
    /// Offset relative to the chunk start.
    offset: u64,
    /// Contiguous byte boundary of confirmed uploads (all blocks below this
    /// offset have completed their S3 PUT).
    uploaded: u64,
    /// Highest block index that has been dispatched for upload.  Blocks in
    /// `[uploaded/block_size .. dispatched_end)` are in-flight.
    dispatched_end: usize,
    /// Bitmask of completed block indices.  Bit N is set when block N's upload
    /// has been confirmed.  Max 64 blocks per slice (256MB/4MB = 64).
    block_done: u64,
    /// Number of upload batches currently in-flight.
    in_flight: u32,
    /// Set to `true` when a pipeline upload task has been spawned for this
    /// slice to prevent duplicate top-level upload tasks.
    upload_task_active: bool,
    /// True while this slice's bytes are included in pending-upload accounting.
    recent_pending_accounted: bool,
    /// Stable logical byte count added to pending-upload accounting.
    recent_pending_accounted_bytes: u64,
    /// Bytes successfully persisted to the local writeback stage for this
    /// slice.
    writeback_persisted_bytes: u64,
    /// True while a task is writing the recoverable local dirty record.
    writeback_record_sealing: bool,
    /// Commit-before-upload may publish metadata only after staged data covers
    /// the whole sealed slice and this recoverable dirty record is sealed.
    writeback_record_sealed: bool,
    data: CacheSlice,
    usage: UsageGuard,
    memory_usage: Option<MemoryUsageGuard>,
    /// Error occurred at background thread.
    err: Option<String>,
    notify: Arc<Notify>,
    started: Instant,
    last_mod: Instant,
    /// Inode data_epoch captured when this slice is frozen.
    /// If the inode epoch advances (truncate/setattr), stale commits are skipped.
    frozen_epoch: u64,
    /// Set to `true` when a meta.write() has been initiated (or completed)
    /// to prevent both try_commit and commit_chunk from writing the same slice.
    meta_write_started: bool,
    /// Reason this slice was sealed. Used to attribute partial-tail uploads.
    freeze_reason: Option<SliceFreezeReason>,
    /// More precise trigger for auto freezes. Used only when `freeze_reason` is `Auto`.
    auto_freeze_trigger: Option<AutoFreezeTrigger>,
    /// FUSE request unique id that created this slice, used to order overlapping
    /// slices for correct commit sequencing (lower unique = older data = commit first).
    creation_unique: u64,
    /// Highest FUSE unique that has written to this slice.  A write with
    /// unique < max_write_unique is rejected (must go to its own slice) to
    /// prevent an older concurrent write from overwriting newer data.
    max_write_unique: u64,
    /// Bitmask of write paths that have successfully appended to this slice.
    write_origin_mask: u8,
}

impl SliceState {
    pub(crate) fn new(
        chunk_id: u64,
        offset: u64,
        config: Arc<WriteConfig>,
        usage: Arc<AtomicU64>,
        memory_budget: Option<MemoryBudget>,
        creation_unique: u64,
    ) -> Self {
        let now = Instant::now();
        Self {
            state: SliceStatus::Writable,
            slice_id: None,
            chunk_id,
            offset,
            uploaded: 0,
            dispatched_end: 0,
            block_done: 0,
            in_flight: 0,
            upload_task_active: false,
            recent_pending_accounted: false,
            recent_pending_accounted_bytes: 0,
            writeback_persisted_bytes: 0,
            writeback_record_sealing: false,
            writeback_record_sealed: false,
            data: CacheSlice::new(config),
            usage: UsageGuard::new(usage),
            memory_usage: memory_budget
                .map(|budget| MemoryUsageGuard::new(budget, MemoryConsumer::Writer)),
            err: None,
            notify: Arc::new(Notify::new()),
            started: now,
            last_mod: now,
            frozen_epoch: 0,
            meta_write_started: false,
            freeze_reason: None,
            auto_freeze_trigger: None,
            creation_unique,
            max_write_unique: creation_unique,
            write_origin_mask: 0,
        }
    }

    fn update_usage(&mut self, bytes: u64) {
        self.usage.update_bytes(bytes);
        if let Some(memory_usage) = &mut self.memory_usage {
            memory_usage.update_bytes(bytes);
        }
    }

    fn record_writeback_persisted_bytes(&mut self, bytes: u64) {
        self.writeback_persisted_bytes = self.writeback_persisted_bytes.saturating_add(bytes);
    }

    fn writeback_data_fully_persisted(&self) -> bool {
        self.writeback_persisted_bytes >= self.data.len()
    }

    fn writeback_fully_persisted(&self) -> bool {
        self.writeback_data_fully_persisted() && self.writeback_record_sealed
    }

    pub(crate) fn can_write(
        &self,
        offset: u64,
        len: usize,
        allow_gap_from: Option<u64>,
    ) -> Option<PageWriteAction> {
        if !matches!(self.state, SliceStatus::Writable) || offset < self.offset {
            return None;
        }

        let size = self.data.block_size();
        let pending_start = self.dispatched_end as u64 * size as u64;

        let off_to_slice = offset - self.offset;

        // Uploaded/dispatched blocks cannot be overlapped.
        if off_to_slice < pending_start.max(self.uploaded) {
            return None;
        }

        let allow_gap = allow_gap_from
            .is_some_and(|safe_from| self.offset.saturating_add(self.data.len()) >= safe_from);

        // For this function, the `offset` is relative to the chunk start,
        // whereas in `CacheSlice.append`, it is relative to the slice start.
        self.data
            .can_write_with_gap(off_to_slice, len as u64, allow_gap)
    }

    #[tracing::instrument(level = "trace", skip(self, buf), fields(len = buf.len()))]
    pub(crate) fn write(
        &mut self,
        offset: u64,
        buf: &[u8],
        action: PageWriteAction,
        origin: WriteOrigin,
    ) -> anyhow::Result<()> {
        self.data.write(offset - self.offset, buf, action)?;
        self.write_origin_mask |= origin.mask();
        self.last_mod = Instant::now();
        Ok(())
    }

    fn write_origin_kind(&self) -> WriteOriginKind {
        match (
            self.write_origin_mask & WriteOrigin::Normal.mask() != 0,
            self.write_origin_mask & WriteOrigin::Cached.mask() != 0,
        ) {
            (false, false) => WriteOriginKind::Unknown,
            (true, false) => WriteOriginKind::NormalOnly,
            (false, true) => WriteOriginKind::CachedOnly,
            (true, true) => WriteOriginKind::Mixed,
        }
    }

    fn data_range(&self) -> Option<(u64, u64)> {
        let len = self.data.len();
        if len == 0 {
            return None;
        }

        Some((self.offset, self.offset + len))
    }

    fn can_coalesce_cached_writable(&self) -> bool {
        matches!(self.state, SliceStatus::Writable)
            && self.slice_id.is_none()
            && self.uploaded == 0
            && self.dispatched_end == 0
            && self.block_done == 0
            && self.in_flight == 0
            && !self.upload_task_active
            && !self.recent_pending_accounted
            && self.writeback_persisted_bytes == 0
            && !self.writeback_record_sealing
            && !self.writeback_record_sealed
            && self.err.is_none()
            && self.frozen_epoch == 0
            && !self.meta_write_started
            && self.freeze_reason.is_none()
            && self.auto_freeze_trigger.is_none()
            && self.data.len() > 0
            && self.data.contiguous_written_len() == self.data.len()
            && matches!(self.write_origin_kind(), WriteOriginKind::CachedOnly)
    }

    fn write_range_has_written_overlap(&self, offset: u64, len: usize) -> bool {
        if offset < self.offset {
            return false;
        }
        self.data
            .has_written_overlap(offset - self.offset, len as u64)
    }

    fn can_overlay_read(&self) -> bool {
        match self.state {
            SliceStatus::Writable
            | SliceStatus::Readonly
            | SliceStatus::Uploaded
            | SliceStatus::Failed => true,
            // Commit-before-upload exposes metadata before object upload
            // completion; overlay only needs to cover that upload gap.
            SliceStatus::Committed => !self.upload_complete(),
        }
    }

    pub fn has_idle_block(&self) -> bool {
        let size = self.data.block_size();
        // Use dispatched_end as the frontier — blocks below this are either
        // uploaded or in-flight.
        let pending_end = (self.dispatched_end as u64 * size as u64).max(self.uploaded);

        let ready_len = self.upload_ready_len();
        let remaining = ready_len.saturating_sub(pending_end);

        if matches!(
            self.state,
            SliceStatus::Readonly | SliceStatus::Failed | SliceStatus::Committed
        ) {
            remaining > 0
        } else {
            remaining >= size as u64
        }
    }

    pub fn idx_need_upload(&self) -> (usize, usize) {
        let size = self.data.block_size() as u64;
        // Start from dispatched_end (not uploaded) — pipeline allows dispatching
        // new blocks while earlier ones are still in-flight.
        let start = self.dispatched_end;
        let ready_len = self.upload_ready_len();
        let end = if matches!(
            self.state,
            SliceStatus::Readonly | SliceStatus::Failed | SliceStatus::Committed
        ) {
            if ready_len == 0 {
                0
            } else {
                ready_len.div_ceil(size) as usize
            }
        } else {
            (ready_len / size) as usize
        };

        (start, end)
    }

    fn upload_ready_len(&self) -> u64 {
        if matches!(self.state, SliceStatus::Writable) {
            self.data.contiguous_written_len()
        } else {
            self.data.len()
        }
    }

    fn upload_complete(&self) -> bool {
        self.in_flight == 0 && !self.has_idle_block()
    }
}

pub(crate) struct ChunkState {
    /// ID of the chunk.
    chunk_id: u64,
    slices: VecDeque<Arc<ParkingMutex<SliceState>>>,
    /// Committed slices kept for a grace period so that overlay_dirty can
    /// still serve their data after commit_chunk marks them Committed.
    recently_committed: VecDeque<Arc<ParkingMutex<SliceState>>>,
    commit_started: bool,
}

#[derive(Clone, Copy)]
struct CachedCoalesceCandidate {
    index: usize,
    start: u64,
    end: u64,
}

impl ChunkState {
    pub(crate) fn new(id: u64) -> Self {
        Self {
            chunk_id: id,
            slices: VecDeque::new(),
            recently_committed: VecDeque::new(),
            commit_started: false,
        }
    }
}

struct SliceHandle<'a, B, M>
where
    B: BlockStore,
    M: MetaLayer,
{
    slice: &'a Arc<ParkingMutex<SliceState>>,
    shared: &'a Shared<B, M>,
}

impl<'a, B, M> SliceHandle<'a, B, M>
where
    B: BlockStore,
    M: MetaLayer,
{
    fn with_mut<T>(&self, f: impl FnOnce(&mut SliceState) -> T) -> T {
        let mut guard = self.slice.lock();
        f(&mut guard)
    }

    fn with_ref<T>(&self, f: impl FnOnce(&SliceState) -> T) -> T {
        let guard = self.slice.lock();
        f(&guard)
    }

    fn can_write(
        &self,
        offset: u64,
        len: usize,
        allow_gap_from: Option<u64>,
    ) -> Option<PageWriteAction> {
        self.with_ref(|s| s.can_write(offset, len, allow_gap_from))
    }

    fn has_written_overlap(&self, offset: u64, len: usize) -> bool {
        self.with_ref(|s| s.write_range_has_written_overlap(offset, len))
    }

    fn rejects_dispatched_prefix(&self, offset: u64, len: usize) -> bool {
        self.with_ref(|s| {
            if !matches!(s.state, SliceStatus::Writable) || offset < s.offset {
                return false;
            }
            let write_end = offset.saturating_add(len as u64);
            let slice_end = s.offset.saturating_add(s.data.len());
            if offset >= slice_end || s.offset >= write_end {
                return false;
            }

            let block_size = s.data.block_size();
            let pending_start = s.dispatched_end as u64 * block_size as u64;
            let prefix_end = pending_start.max(s.uploaded);
            offset - s.offset < prefix_end
        })
    }

    fn can_freeze_for_max_unflushed(&self) -> bool {
        self.with_ref(|s| matches!(s.state, SliceStatus::Writable) && s.has_idle_block())
    }

    fn try_write(
        &self,
        offset: u64,
        buf: &[u8],
        origin: WriteOrigin,
        allow_gap_from: Option<u64>,
    ) -> anyhow::Result<bool> {
        let wrote = self.with_mut(|s| match s.can_write(offset, buf.len(), allow_gap_from) {
            Some(action) => {
                s.write(offset, buf, action, origin)?;
                s.update_usage(s.data.alloc_bytes());
                Ok::<bool, anyhow::Error>(true)
            }
            None => Ok::<bool, anyhow::Error>(false),
        })?;

        Ok(wrote)
    }

    fn freeze_with_reason(&self, reason: SliceFreezeReason) -> bool {
        self.freeze_with_reason_and_auto_trigger(reason, None)
    }

    fn freeze_auto_with_trigger(&self, trigger: AutoFreezeTrigger) -> bool {
        self.freeze_with_reason_and_auto_trigger(SliceFreezeReason::Auto, Some(trigger))
    }

    fn freeze_with_reason_and_auto_trigger(
        &self,
        reason: SliceFreezeReason,
        auto_trigger: Option<AutoFreezeTrigger>,
    ) -> bool {
        let mut empty_committed = false;
        let mut frozen_bytes = 0u64;
        let froze = self.with_mut(|s| {
            if !matches!(s.state, SliceStatus::Writable) {
                return false;
            }

            if s.data.len() == 0 {
                s.state = SliceStatus::Committed;
                s.err = None;
                s.notify.notify_waiters();
                empty_committed = true;
                return false;
            }

            frozen_bytes = s.data.len();
            s.state = SliceStatus::Readonly;
            s.frozen_epoch = self.shared.inode.data_epoch();
            s.freeze_reason = Some(reason);
            s.auto_freeze_trigger = if matches!(reason, SliceFreezeReason::Auto) {
                auto_trigger
            } else {
                None
            };
            s.data.freeze();

            if s.in_flight == 0 && !s.has_idle_block() {
                s.state = SliceStatus::Uploaded;
                s.err = None;
                s.notify.notify_waiters();
            }
            true
        });

        if empty_committed {
            self.shared.flush_notify.notify_waiters();
        }
        if froze {
            self.shared
                .recent_pending_upload
                .record_freeze(reason, frozen_bytes);
        }

        froze
    }

    /// Called when a block range `[start_idx, end_idx)` finishes uploading.
    /// Marks the blocks done in the bitmask and advances `uploaded` through
    /// the highest contiguous completed boundary.
    fn advance_upload_range(&self, start_idx: usize, end_idx: usize, _len: u64) {
        let made_progress = self.with_mut(|s| {
            let previous_uploaded = s.uploaded;
            let previous_state = s.state;

            // Mark completed blocks in bitmask (guard against overflow).
            for idx in start_idx..end_idx {
                if idx < 64 {
                    s.block_done |= 1u64 << idx;
                }
            }
            s.in_flight = s.in_flight.saturating_sub(1);

            // Advance `uploaded` through contiguous completed blocks.
            let block_size = s.data.block_size() as u64;
            let mut current_block = (s.uploaded / block_size) as usize;
            while current_block < 64 && (s.block_done >> current_block) & 1 == 1 {
                current_block += 1;
            }
            let new_uploaded = current_block as u64 * block_size;
            if new_uploaded > s.uploaded {
                s.uploaded = new_uploaded;
            }

            // Keep uploaded pages resident until metadata commit removes the slice.
            s.update_usage(s.data.alloc_bytes());

            if matches!(s.state, SliceStatus::Readonly | SliceStatus::Failed)
                && s.in_flight == 0
                && !s.has_idle_block()
            {
                s.state = SliceStatus::Uploaded;
                s.err = None;
            }
            self.clear_recent_pending_if_complete(s);
            s.notify.notify_waiters();
            s.uploaded != previous_uploaded || s.state != previous_state
        });
        if made_progress {
            self.shared.flush_notify.notify_waiters();
        }
    }

    /// Legacy advance_upload for backward compatibility with single-batch callers.
    fn advance_upload(&self, len: u64, _uploaded_blocks: Vec<usize>) {
        let made_progress = self.with_mut(|s| {
            let previous_uploaded = s.uploaded;
            let previous_state = s.state;

            s.in_flight = s.in_flight.saturating_sub(1);
            s.uploaded += len;

            // Mark all blocks up to uploaded as done.
            let block_size = s.data.block_size() as u64;
            let done_end = (s.uploaded / block_size) as usize;
            for idx in 0..done_end.min(64) {
                s.block_done |= 1u64 << idx;
            }

            s.update_usage(s.data.alloc_bytes());

            if matches!(s.state, SliceStatus::Readonly | SliceStatus::Failed)
                && s.in_flight == 0
                && !s.has_idle_block()
            {
                s.state = SliceStatus::Uploaded;
                s.err = None;
            }
            self.clear_recent_pending_if_complete(s);
            s.notify.notify_waiters();
            s.uploaded != previous_uploaded || s.state != previous_state
        });
        if made_progress {
            self.shared.flush_notify.notify_waiters();
        }
    }

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

    fn should_freeze(&self) -> bool {
        self.with_ref(|s| {
            let ready_len = s.upload_ready_len();
            if ready_len < s.data.len() {
                return false;
            }
            let end = s.offset + ready_len;
            let freeze_min = self.shared.config.freeze_min_bytes;
            end >= self.shared.config.layout.chunk_size || ready_len >= freeze_min
        })
    }

    fn runtime_snapshot(&self) -> SliceRuntime {
        self.with_ref(|s| SliceRuntime {
            status: s.state,
            err: s.err.clone(),
            frozen: !matches!(s.state, SliceStatus::Writable),
            freeze_reason: s.freeze_reason,
            write_origin: s.write_origin_kind(),
            started: s.started,
            notify: s.notify.clone(),
        })
    }

    fn can_continue_upload(&self) -> bool {
        self.with_ref(|s| s.has_idle_block() && !s.upload_task_active)
    }

    // Mark data upload failure and wake commit waiters.
    fn mark_failed(&self, err: anyhow::Error) {
        let message = err.to_string();
        self.with_mut(|s| {
            s.state = SliceStatus::Failed;
            s.in_flight = 0;
            s.upload_task_active = false;
            s.err = Some(message.clone());
            self.clear_recent_pending_accounting(s);

            s.notify.notify_waiters();
        });
        self.shared.record_writeback_error(message);
        self.shared.flush_notify.notify_waiters();
    }

    fn mark_writeback_persisted(&self, bytes: u64) {
        self.with_mut(|s| {
            s.record_writeback_persisted_bytes(bytes);
            s.notify.notify_waiters();
        });
        self.shared.flush_notify.notify_waiters();
    }

    fn claim_writeback_record_seal(
        &self,
    ) -> Option<(crate::vfs::cache::keys::DirtySliceKey, u64, u64)> {
        let ino = self.shared.inode.ino();
        self.with_mut(|s| {
            if matches!(s.state, SliceStatus::Writable)
                || !s.writeback_data_fully_persisted()
                || s.writeback_record_sealed
                || s.writeback_record_sealing
            {
                return None;
            }

            let slice_id = s.slice_id?;
            s.writeback_record_sealing = true;
            Some((
                crate::vfs::cache::keys::DirtySliceKey {
                    ino,
                    chunk_id: s.chunk_id,
                    local_seq: slice_id,
                    epoch: 0,
                },
                s.offset,
                s.data.len(),
            ))
        })
    }

    fn mark_writeback_record_sealed(&self) {
        self.with_mut(|s| {
            s.writeback_record_sealing = false;
            s.writeback_record_sealed = true;
            s.notify.notify_waiters();
        });
        self.shared.flush_notify.notify_waiters();
    }

    fn prepare_upload(&self) -> anyhow::Result<Option<UploadPlan>> {
        self.with_mut(|s| {
            if matches!(s.state, SliceStatus::Failed) {
                return Ok(None);
            }
            if !s.has_idle_block() {
                return Ok(None);
            }

            let (start, end) = s.idx_need_upload();

            if end <= start {
                return Ok(None);
            }

            s.data.freeze_blocks(start, end);

            let data = s.data.collect_pages(start, end)?;
            let data_len = data
                .iter()
                .flat_map(|(_, chunks)| chunks.iter())
                .map(|chunk| chunk.len() as u64)
                .sum();
            // Pipeline: track dispatched frontier and in-flight count instead
            // of a single exclusive `uploading` range.
            s.dispatched_end = end;
            s.in_flight += 1;

            // Compute the byte offset for this batch based on block indices.
            let block_size = s.data.block_size() as u64;
            let batch_offset = start as u64 * block_size;
            let partial_tail = matches!(
                s.state,
                SliceStatus::Readonly | SliceStatus::Failed | SliceStatus::Committed
            ) && s.data.len() % block_size != 0
                && end as u64 * block_size >= s.data.len();
            let partial_tail_reason = s.freeze_reason;
            let partial_tail_auto_trigger = s.auto_freeze_trigger;
            let write_origin = s.write_origin_kind();
            self.shared.recent_pending_upload.record_upload_batch(
                data_len,
                (end - start) as u64,
                partial_tail,
                partial_tail_reason,
                partial_tail_auto_trigger,
                write_origin,
            );

            Ok(Some(UploadPlan {
                chunk_id: s.chunk_id,
                data,
                slice_id: s.slice_id,
                uploaded: batch_offset,
                write_origin,
            }))
        })
    }

    fn set_slice_id(&self, id: u64) {
        self.with_mut(|s| {
            if s.slice_id.is_none() {
                s.slice_id = Some(id);
            }
        })
    }

    fn desc_for_commit(&self) -> Option<SliceDesc> {
        self.with_ref(|s| {
            let length = s.data.len();
            let slice_id = match s.slice_id {
                Some(id) => id,
                None => return None,
            };
            if length == 0 {
                return None;
            }
            Some(SliceDesc {
                slice_id,
                chunk_id: s.chunk_id,
                offset: s.offset,
                length,
            })
        })
    }

    fn mark_committed(&self) {
        self.with_mut(|s| {
            s.state = SliceStatus::Committed;
            s.notify.notify_waiters();
        });
        self.shared.flush_notify.notify_waiters();
    }
}

impl<'a, B, M> SliceHandle<'a, B, M>
where
    B: BlockStore + Send + Sync + 'static,
    M: MetaLayer + Send + Sync + 'static,
{
    /// Attempt to commit a fully-uploaded slice immediately.
    /// Called from the upload task when all blocks have been transferred,
    /// so that flush() callers do not wait on the commit_chunk poll loop.
    ///
    /// To preserve metadata ordering (later slices must appear after earlier
    /// ones), we only commit if this slice is at the front of the chunk's
    /// deque — i.e. all preceding slices have already been popped.
    async fn try_commit(&self) {
        if !self.runtime_snapshot().can_commit() {
            return;
        }

        let slice_epoch = self.slice.lock().frozen_epoch;
        if slice_epoch != 0 && self.shared.inode.data_epoch() != slice_epoch {
            tracing::warn!(
                ino = self.shared.inode.ino(),
                slice_epoch,
                current_epoch = self.shared.inode.data_epoch(),
                "skipping stale try_commit after inode epoch change"
            );
            self.mark_committed();
            return;
        }

        // Claim the right to write metadata for this slice.  Both try_commit
        // (from upload task) and commit_chunk (from commit loop) race here;
        // the first to set `meta_write_started` wins and the other skips.
        let chunk_id = {
            let mut s = self.slice.lock();
            if s.meta_write_started {
                return;
            }
            s.meta_write_started = true;
            s.chunk_id
        };

        // Only commit if we are the front slice.  Out-of-order metadata
        // appends would let an older slice win over a newer one in the
        // "last writer wins" resolution used by readers.
        {
            let guard = self.shared.inner.lock().await;
            let is_front = guard
                .chunks
                .get(&chunk_id)
                .and_then(|c| c.slices.front())
                .is_some_and(|front| Arc::ptr_eq(front, self.slice));
            if !is_front {
                // Revert the flag so commit_chunk can handle it when it
                // becomes the front slice.
                self.slice.lock().meta_write_started = false;
                return;
            }
        }

        let desc = match self.desc_for_commit() {
            Some(d) => d,
            None => return,
        };

        let (ino, chunk_index) = crate::vfs::extract_ino_and_chunk_index(desc.chunk_id);
        let new_size =
            chunk_index * self.shared.config.layout.chunk_size + desc.offset + desc.length;

        let mut attempts = 0u32;
        loop {
            match self
                .shared
                .backend
                .meta()
                .write(ino, desc.chunk_id, desc, new_size)
                .await
            {
                Ok(()) => {
                    self.shared.inode.set_committed_size(new_size);
                    self.shared
                        .inode
                        .add_estimated_allocated_bytes(desc.length.as_usize() as u64);

                    // Invalidate reader cache BEFORE marking committed so that
                    // when the flush loop sees the Committed state, the reader
                    // already has fresh data.  Otherwise flush can return while
                    // the reader still serves stale cached pages.
                    let file_offset =
                        chunk_index * self.shared.config.layout.chunk_size + desc.offset;
                    let _ = self
                        .shared
                        .reader
                        .invalidate(ino as u64, file_offset, desc.length.as_usize())
                        .await;

                    self.mark_committed();

                    if let Some(wb) = &self.shared.write_back {
                        let key = crate::vfs::cache::keys::DirtySliceKey {
                            ino,
                            chunk_id: desc.chunk_id,
                            local_seq: desc.slice_id,
                            epoch: 0,
                        };
                        let _ = wb.remove(&key).await;
                    }
                    return;
                }
                Err(err) => {
                    let retryable = should_retry_meta_write(&err);
                    attempts = attempts.saturating_add(1);
                    if retryable && attempts < COMMIT_META_MAX_RETRIES {
                        tokio::time::sleep(commit_retry_backoff(attempts)).await;
                        continue;
                    }
                    if retryable {
                        tracing::debug!(
                            ino,
                            chunk_id = desc.chunk_id,
                            slice_id = desc.slice_id,
                            attempts,
                            error = ?err,
                            "try_commit exhausted retries, deferring to commit_chunk"
                        );
                        // Reset so commit_chunk can pick this up.
                        self.slice.lock().meta_write_started = false;
                    } else {
                        self.mark_failed(anyhow::anyhow!(
                            "try_commit failed for ino {ino}, chunk {}, slice {}: {err}",
                            desc.chunk_id,
                            desc.slice_id
                        ));
                        if let Some(wb) = &self.shared.write_back {
                            let key = crate::vfs::cache::keys::DirtySliceKey {
                                ino,
                                chunk_id: desc.chunk_id,
                                local_seq: desc.slice_id,
                                epoch: 0,
                            };
                            let _ = wb.remove(&key).await;
                        }
                    }
                    return;
                }
            }
        }
    }
}

/// A snapshot of a slice, allowing us to check slice status without lock.
struct SliceRuntime {
    status: SliceStatus,
    err: Option<String>,
    frozen: bool,
    freeze_reason: Option<SliceFreezeReason>,
    write_origin: WriteOriginKind,
    started: Instant,
    notify: Arc<Notify>,
}

impl SliceRuntime {
    fn upload_done(&self) -> bool {
        matches!(self.status, SliceStatus::Uploaded | SliceStatus::Committed)
    }

    fn can_commit(&self) -> bool {
        matches!(self.status, SliceStatus::Uploaded)
    }
}

struct WriteAction {
    start_commit: bool,
    flush: Vec<Arc<ParkingMutex<SliceState>>>,
}

struct ChunkHandle<'a, B, M>
where
    B: BlockStore,
    M: MetaLayer,
{
    chunk_id: u64,
    inner: &'a mut Inner,
    shared: &'a Shared<B, M>,
}

impl<'a, B, M> ChunkHandle<'a, B, M>
where
    B: BlockStore,
    M: MetaLayer,
{
    /// Find or create the next slice which can be written.
    /// A slice is append-only.
    fn find_slice_or_create(
        &mut self,
        offset: u64,
        len: usize,
        creation_unique: u64,
        allow_gap_from: Option<u64>,
    ) -> anyhow::Result<(Arc<ParkingMutex<SliceState>>, WriteAction)> {
        let (chunk_id, mut slices) = {
            let chunk = self
                .inner
                .chunks
                .get_mut(&self.chunk_id)
                .ok_or_else(|| anyhow::anyhow!("invalid chunk id"))?;
            let slices = std::mem::take(&mut chunk.slices);
            (chunk.chunk_id, slices)
        };

        anyhow::ensure!(
            offset + len as u64 <= self.shared.config.layout.chunk_size,
            "A write operation cannot exceed the chunk size"
        );

        let mut found: Option<Arc<ParkingMutex<SliceState>>> = None;
        let mut flush = Vec::new();
        let mut rejected_dispatched_prefix = false;
        for (idx, slice) in slices.iter().rev().enumerate() {
            let handle = SliceHandle {
                slice,
                shared: self.shared,
            };

            if handle.can_write(offset, len, allow_gap_from).is_some() {
                // Reject reuse if this write is older than the newest write
                // already in the slice.  Without this check, an older concurrent
                // FUSE write (lower unique) processed after a newer one could
                // overwrite the newer data in the overlapping region.  Sparse
                // gap append tracks actual user-written ranges, so an older
                // request may still fill a zero gap left by a newer tail write.
                if creation_unique != 0 {
                    let max_u = slice.lock().max_write_unique;
                    if max_u != 0
                        && creation_unique < max_u
                        && handle.has_written_overlap(offset, len)
                    {
                        self.shared
                            .recent_pending_upload
                            .record_slice_reject_older_unique();
                        continue;
                    }
                }
                found = Some(slice.clone());
                break;
            } else if handle.rejects_dispatched_prefix(offset, len) {
                rejected_dispatched_prefix = true;
            }

            // Prevent slices from remaining unflushed for too long.
            if idx > MAX_UNFLUSHED_SLICES
                && handle.can_freeze_for_max_unflushed()
                && handle.freeze_with_reason(SliceFreezeReason::MaxUnflushed)
            {
                flush.push(slice.clone());
            }
        }

        let slice = match found {
            Some(slice) => {
                self.shared.recent_pending_upload.record_slice_reuse();
                // Update max_write_unique so future older writes won't reuse this slice.
                if creation_unique != 0 {
                    let mut s = slice.lock();
                    if creation_unique > s.max_write_unique {
                        s.max_write_unique = creation_unique;
                    }
                }
                slice
            }
            None => {
                let slice = Arc::new(ParkingMutex::new(SliceState::new(
                    chunk_id,
                    offset,
                    self.shared.config.clone(),
                    self.shared.buffer_usage.clone(),
                    self.shared.memory_budget.clone(),
                    creation_unique,
                )));
                self.shared.recent_pending_upload.record_slice_create();
                if rejected_dispatched_prefix {
                    self.shared
                        .recent_pending_upload
                        .record_slice_reject_dispatched_prefix();
                }
                // Insert in sorted position by creation_unique so that slices
                // committed in FIFO (front-first) order reflect the kernel's
                // temporal write ordering. This prevents a race where concurrent
                // FUSE request processing reorders overlapping writes.
                // creation_unique=0 means the ordering is unknown (non-cached
                // write path); append to back to preserve original FIFO behavior.
                let insert_pos = if creation_unique == 0 {
                    slices.len()
                } else {
                    slices
                        .iter()
                        .position(|s| s.lock().creation_unique > creation_unique)
                        .unwrap_or(slices.len())
                };
                slices.insert(insert_pos, slice.clone());
                slice
            }
        };

        let chunk = self
            .inner
            .chunks
            .get_mut(&self.chunk_id)
            .ok_or_else(|| anyhow::anyhow!("invalid chunk id"))?;

        // This `slices` includes the newly created slice.
        chunk.slices = slices;
        let mut start_commit = false;

        // Enable the background commit thread if there is already a slice.
        if !chunk.commit_started && !chunk.slices.is_empty() {
            chunk.commit_started = true;
            start_commit = true;
        }

        Ok((
            slice,
            WriteAction {
                start_commit,
                flush,
            },
        ))
    }

    /// Append data to a writable slice. If the slice reaches chunk end, freeze + flush it.
    #[tracing::instrument(level = "trace", skip(self, buf), fields(len = buf.len()))]
    fn write_at(
        &mut self,
        offset: u64,
        buf: &[u8],
        creation_unique: u64,
        origin: WriteOrigin,
        allow_gap_from: Option<u64>,
    ) -> anyhow::Result<WriteAction> {
        let mut start_commit = false;
        let mut flush = Vec::new();

        // There is a potential race condition in the time window between `find_slice_or_create` and `try_append`.
        // `find_slice_or_create` checks and returns a slice that can be appended, but after it selects the slice,
        // it releases the lock. `auto_flush` and `commit_chunk` can freeze a slice without holding the lock,
        // so when handle trying appending buf, the slice may have become readonly. This is highly unlikely to happen,
        // therefore, it is ok to retry briefly, but not forever.
        let mut failed_cnt = 0;

        loop {
            let (slice, action) =
                self.find_slice_or_create(offset, buf.len(), creation_unique, allow_gap_from)?;
            start_commit |= action.start_commit;
            flush.extend(action.flush);

            let handle = SliceHandle {
                slice: &slice,
                shared: self.shared,
            };

            if handle.try_write(offset, buf, origin, allow_gap_from)? {
                let should_flush = if matches!(
                    self.shared.config.writeback_mode,
                    WriteBackMode::CommitBeforeUpload
                ) || matches!(origin, WriteOrigin::Normal)
                {
                    handle.should_freeze()
                        && handle.freeze_with_reason(SliceFreezeReason::SizeOrChunkEnd)
                } else {
                    handle.can_continue_upload()
                        || handle.should_freeze()
                            && handle.freeze_with_reason(SliceFreezeReason::SizeOrChunkEnd)
                };
                if should_flush {
                    flush.push(slice);
                }

                return Ok(WriteAction {
                    start_commit,
                    flush,
                });
            }

            failed_cnt += 1;
            if failed_cnt == 10 {
                warn!(
                    chunk_id = self.chunk_id,
                    offset,
                    len = buf.len(),
                    "write_at retried {failed_cnt} times due to concurrent slice freezing"
                );
            }
            if failed_cnt >= WRITE_SLICE_MAX_RETRIES {
                return Err(anyhow::anyhow!(
                    "write_at failed to append after {failed_cnt} retries due to concurrent slice freezing"
                ));
            }
            std::thread::yield_now();
        }
    }
}

struct Shared<B, M> {
    inode: Arc<Inode>,
    config: Arc<WriteConfig>,
    buffer_usage: Arc<AtomicU64>,
    inner: Mutex<Inner>,
    /// Notify signal to wait write.
    write_notify: Notify,
    /// Notify signal to wait flush.
    flush_notify: Notify,
    backend: Arc<Backend<B, M>>,
    reader: Arc<DataReader<B, M>>,
    /// Local SSD write-back cache for persisting frozen slices before upload.
    write_back: Option<Arc<crate::vfs::cache::write_back::FsWriteBackCache>>,
    memory_budget: Option<MemoryBudget>,
    /// Monotonically incremented on each write.  Used together with
    /// `last_flushed_gen` to let `has_pending()` avoid a lock acquisition
    /// when no new data has arrived since the last successful flush.
    write_gen: AtomicU64,
    /// Snapshot of `write_gen` taken after a flush completes successfully.
    last_flushed_gen: AtomicU64,
    /// Bytes in recently committed slices whose object upload is not complete.
    recent_pending_upload: Arc<RecentPendingUploadState>,
    /// First durable writeback error observed by background upload/commit.
    writeback_error: ParkingMutex<Option<String>>,
    /// Per-writer limit for concurrently dispatched block uploads.
    upload_limit: Arc<Semaphore>,
    /// The last user handle was released, but writeback overlay may still be
    /// needed until committed slices finish uploading and age out.
    released: AtomicBool,
}

struct RecentPendingUploadState {
    bytes: AtomicU64,
    soft_sleep_ops: AtomicU64,
    soft_sleep_us: AtomicU64,
    hard_wait_ops: AtomicU64,
    hard_wait_us: AtomicU64,
    buffer_soft_sleep_ops: AtomicU64,
    buffer_soft_sleep_us: AtomicU64,
    buffer_moderate_sleep_ops: AtomicU64,
    buffer_moderate_sleep_us: AtomicU64,
    buffer_hard_sleep_ops: AtomicU64,
    buffer_hard_sleep_us: AtomicU64,
    stage_inflight_bytes: Arc<AtomicU64>,
    remote_upload_inflight_bytes: Arc<AtomicU64>,
    stage_ops: AtomicU64,
    stage_bytes: AtomicU64,
    stage_us: AtomicU64,
    stage_failures: AtomicU64,
    commit_before_stage_ops: AtomicU64,
    commit_wait_upload_ops: AtomicU64,
    commit_wait_upload_us: AtomicU64,
    commit_wait_upload_size_ops: AtomicU64,
    commit_wait_upload_size_us: AtomicU64,
    commit_wait_upload_max_unflushed_ops: AtomicU64,
    commit_wait_upload_max_unflushed_us: AtomicU64,
    commit_wait_upload_explicit_flush_ops: AtomicU64,
    commit_wait_upload_explicit_flush_us: AtomicU64,
    commit_wait_upload_auto_ops: AtomicU64,
    commit_wait_upload_auto_us: AtomicU64,
    commit_wait_upload_commit_age_ops: AtomicU64,
    commit_wait_upload_commit_age_us: AtomicU64,
    commit_wait_upload_unknown_reason_ops: AtomicU64,
    commit_wait_upload_unknown_reason_us: AtomicU64,
    commit_wait_upload_normal_only_ops: AtomicU64,
    commit_wait_upload_normal_only_us: AtomicU64,
    commit_wait_upload_cached_only_ops: AtomicU64,
    commit_wait_upload_cached_only_us: AtomicU64,
    commit_wait_upload_mixed_origin_ops: AtomicU64,
    commit_wait_upload_mixed_origin_us: AtomicU64,
    commit_wait_upload_unknown_origin_ops: AtomicU64,
    commit_wait_upload_unknown_origin_us: AtomicU64,
    commit_wait_retry_ops: AtomicU64,
    commit_wait_retry_us: AtomicU64,
    flush_wait_ops: AtomicU64,
    flush_wait_us: AtomicU64,
    flush_wait_slices: AtomicU64,
    flush_fragmentation_ops: AtomicU64,
    flush_fragmentation_slices: AtomicU64,
    flush_fragmentation_bytes: AtomicU64,
    flush_fragmentation_cached_sub_block_slices: AtomicU64,
    flush_fragmentation_cached_sub_block_bytes: AtomicU64,
    flush_fragmentation_full_block_slices: AtomicU64,
    flush_fragmentation_full_block_bytes: AtomicU64,
    slice_create_ops: AtomicU64,
    slice_reuse_ops: AtomicU64,
    slice_reject_older_unique_ops: AtomicU64,
    slice_reject_dispatched_prefix_ops: AtomicU64,
    freeze_size_ops: AtomicU64,
    freeze_size_bytes: AtomicU64,
    freeze_max_unflushed_ops: AtomicU64,
    freeze_max_unflushed_bytes: AtomicU64,
    freeze_explicit_flush_ops: AtomicU64,
    freeze_explicit_flush_bytes: AtomicU64,
    freeze_auto_ops: AtomicU64,
    freeze_auto_bytes: AtomicU64,
    freeze_commit_age_ops: AtomicU64,
    freeze_commit_age_bytes: AtomicU64,
    upload_batch_ops: AtomicU64,
    upload_batch_bytes: AtomicU64,
    upload_batch_blocks: AtomicU64,
    upload_batch_single_block_ops: AtomicU64,
    upload_batch_multi_block_ops: AtomicU64,
    upload_partial_tail_ops: AtomicU64,
    upload_partial_tail_size_ops: AtomicU64,
    upload_partial_tail_max_unflushed_ops: AtomicU64,
    upload_partial_tail_explicit_flush_ops: AtomicU64,
    upload_partial_tail_auto_ops: AtomicU64,
    upload_partial_tail_normal_only_ops: AtomicU64,
    upload_partial_tail_cached_only_ops: AtomicU64,
    upload_partial_tail_mixed_origin_ops: AtomicU64,
    upload_partial_tail_unknown_origin_ops: AtomicU64,
    upload_partial_tail_auto_age_ops: AtomicU64,
    upload_partial_tail_auto_idle_ops: AtomicU64,
    upload_partial_tail_auto_pressure_ops: AtomicU64,
    upload_partial_tail_auto_too_many_ops: AtomicU64,
    upload_partial_tail_auto_buffer_high_ops: AtomicU64,
    upload_partial_tail_auto_flush_duration_ops: AtomicU64,
    upload_partial_tail_auto_unknown_ops: AtomicU64,
    upload_partial_tail_auto_normal_only_ops: AtomicU64,
    upload_partial_tail_auto_cached_only_ops: AtomicU64,
    upload_partial_tail_auto_mixed_origin_ops: AtomicU64,
    upload_partial_tail_auto_unknown_origin_ops: AtomicU64,
    upload_partial_tail_commit_age_ops: AtomicU64,
    notify: Notify,
}

struct InflightBytesGuard {
    counter: Arc<AtomicU64>,
    bytes: u64,
}

impl Drop for InflightBytesGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

impl RecentPendingUploadState {
    fn new() -> Self {
        Self {
            bytes: AtomicU64::new(0),
            soft_sleep_ops: AtomicU64::new(0),
            soft_sleep_us: AtomicU64::new(0),
            hard_wait_ops: AtomicU64::new(0),
            hard_wait_us: AtomicU64::new(0),
            buffer_soft_sleep_ops: AtomicU64::new(0),
            buffer_soft_sleep_us: AtomicU64::new(0),
            buffer_moderate_sleep_ops: AtomicU64::new(0),
            buffer_moderate_sleep_us: AtomicU64::new(0),
            buffer_hard_sleep_ops: AtomicU64::new(0),
            buffer_hard_sleep_us: AtomicU64::new(0),
            stage_inflight_bytes: Arc::new(AtomicU64::new(0)),
            remote_upload_inflight_bytes: Arc::new(AtomicU64::new(0)),
            stage_ops: AtomicU64::new(0),
            stage_bytes: AtomicU64::new(0),
            stage_us: AtomicU64::new(0),
            stage_failures: AtomicU64::new(0),
            commit_before_stage_ops: AtomicU64::new(0),
            commit_wait_upload_ops: AtomicU64::new(0),
            commit_wait_upload_us: AtomicU64::new(0),
            commit_wait_upload_size_ops: AtomicU64::new(0),
            commit_wait_upload_size_us: AtomicU64::new(0),
            commit_wait_upload_max_unflushed_ops: AtomicU64::new(0),
            commit_wait_upload_max_unflushed_us: AtomicU64::new(0),
            commit_wait_upload_explicit_flush_ops: AtomicU64::new(0),
            commit_wait_upload_explicit_flush_us: AtomicU64::new(0),
            commit_wait_upload_auto_ops: AtomicU64::new(0),
            commit_wait_upload_auto_us: AtomicU64::new(0),
            commit_wait_upload_commit_age_ops: AtomicU64::new(0),
            commit_wait_upload_commit_age_us: AtomicU64::new(0),
            commit_wait_upload_unknown_reason_ops: AtomicU64::new(0),
            commit_wait_upload_unknown_reason_us: AtomicU64::new(0),
            commit_wait_upload_normal_only_ops: AtomicU64::new(0),
            commit_wait_upload_normal_only_us: AtomicU64::new(0),
            commit_wait_upload_cached_only_ops: AtomicU64::new(0),
            commit_wait_upload_cached_only_us: AtomicU64::new(0),
            commit_wait_upload_mixed_origin_ops: AtomicU64::new(0),
            commit_wait_upload_mixed_origin_us: AtomicU64::new(0),
            commit_wait_upload_unknown_origin_ops: AtomicU64::new(0),
            commit_wait_upload_unknown_origin_us: AtomicU64::new(0),
            commit_wait_retry_ops: AtomicU64::new(0),
            commit_wait_retry_us: AtomicU64::new(0),
            flush_wait_ops: AtomicU64::new(0),
            flush_wait_us: AtomicU64::new(0),
            flush_wait_slices: AtomicU64::new(0),
            flush_fragmentation_ops: AtomicU64::new(0),
            flush_fragmentation_slices: AtomicU64::new(0),
            flush_fragmentation_bytes: AtomicU64::new(0),
            flush_fragmentation_cached_sub_block_slices: AtomicU64::new(0),
            flush_fragmentation_cached_sub_block_bytes: AtomicU64::new(0),
            flush_fragmentation_full_block_slices: AtomicU64::new(0),
            flush_fragmentation_full_block_bytes: AtomicU64::new(0),
            slice_create_ops: AtomicU64::new(0),
            slice_reuse_ops: AtomicU64::new(0),
            slice_reject_older_unique_ops: AtomicU64::new(0),
            slice_reject_dispatched_prefix_ops: AtomicU64::new(0),
            freeze_size_ops: AtomicU64::new(0),
            freeze_size_bytes: AtomicU64::new(0),
            freeze_max_unflushed_ops: AtomicU64::new(0),
            freeze_max_unflushed_bytes: AtomicU64::new(0),
            freeze_explicit_flush_ops: AtomicU64::new(0),
            freeze_explicit_flush_bytes: AtomicU64::new(0),
            freeze_auto_ops: AtomicU64::new(0),
            freeze_auto_bytes: AtomicU64::new(0),
            freeze_commit_age_ops: AtomicU64::new(0),
            freeze_commit_age_bytes: AtomicU64::new(0),
            upload_batch_ops: AtomicU64::new(0),
            upload_batch_bytes: AtomicU64::new(0),
            upload_batch_blocks: AtomicU64::new(0),
            upload_batch_single_block_ops: AtomicU64::new(0),
            upload_batch_multi_block_ops: AtomicU64::new(0),
            upload_partial_tail_ops: AtomicU64::new(0),
            upload_partial_tail_size_ops: AtomicU64::new(0),
            upload_partial_tail_max_unflushed_ops: AtomicU64::new(0),
            upload_partial_tail_explicit_flush_ops: AtomicU64::new(0),
            upload_partial_tail_auto_ops: AtomicU64::new(0),
            upload_partial_tail_normal_only_ops: AtomicU64::new(0),
            upload_partial_tail_cached_only_ops: AtomicU64::new(0),
            upload_partial_tail_mixed_origin_ops: AtomicU64::new(0),
            upload_partial_tail_unknown_origin_ops: AtomicU64::new(0),
            upload_partial_tail_auto_age_ops: AtomicU64::new(0),
            upload_partial_tail_auto_idle_ops: AtomicU64::new(0),
            upload_partial_tail_auto_pressure_ops: AtomicU64::new(0),
            upload_partial_tail_auto_too_many_ops: AtomicU64::new(0),
            upload_partial_tail_auto_buffer_high_ops: AtomicU64::new(0),
            upload_partial_tail_auto_flush_duration_ops: AtomicU64::new(0),
            upload_partial_tail_auto_unknown_ops: AtomicU64::new(0),
            upload_partial_tail_auto_normal_only_ops: AtomicU64::new(0),
            upload_partial_tail_auto_cached_only_ops: AtomicU64::new(0),
            upload_partial_tail_auto_mixed_origin_ops: AtomicU64::new(0),
            upload_partial_tail_auto_unknown_origin_ops: AtomicU64::new(0),
            upload_partial_tail_commit_age_ops: AtomicU64::new(0),
            notify: Notify::new(),
        }
    }

    fn record_soft_sleep(&self, duration: Duration) {
        self.soft_sleep_ops.fetch_add(1, Ordering::Relaxed);
        self.soft_sleep_us.fetch_add(
            duration.as_micros().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
    }

    fn record_hard_wait(&self, duration: Duration) {
        self.hard_wait_ops.fetch_add(1, Ordering::Relaxed);
        self.hard_wait_us.fetch_add(
            duration.as_micros().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
    }

    fn record_buffer_soft_sleep(&self, duration: Duration) {
        self.buffer_soft_sleep_ops.fetch_add(1, Ordering::Relaxed);
        self.buffer_soft_sleep_us.fetch_add(
            duration.as_micros().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
    }

    fn record_buffer_moderate_sleep(&self, duration: Duration) {
        self.buffer_moderate_sleep_ops
            .fetch_add(1, Ordering::Relaxed);
        self.buffer_moderate_sleep_us.fetch_add(
            duration.as_micros().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
    }

    fn record_buffer_hard_sleep(&self, duration: Duration) {
        self.buffer_hard_sleep_ops.fetch_add(1, Ordering::Relaxed);
        self.buffer_hard_sleep_us.fetch_add(
            duration.as_micros().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
    }

    fn record_stage_start(&self, bytes: u64) -> Instant {
        self.stage_inflight_bytes.fetch_add(bytes, Ordering::AcqRel);
        Instant::now()
    }

    fn record_stage_finish(&self, start: Instant, bytes: u64, success: bool) {
        self.stage_inflight_bytes.fetch_sub(bytes, Ordering::AcqRel);
        self.stage_ops.fetch_add(1, Ordering::Relaxed);
        self.stage_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.stage_us.fetch_add(
            start.elapsed().as_micros().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
        if !success {
            self.stage_failures.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn track_remote_upload_inflight(&self, bytes: u64) -> InflightBytesGuard {
        self.remote_upload_inflight_bytes
            .fetch_add(bytes, Ordering::AcqRel);
        InflightBytesGuard {
            counter: self.remote_upload_inflight_bytes.clone(),
            bytes,
        }
    }

    fn record_commit_before_stage(&self) {
        self.commit_before_stage_ops.fetch_add(1, Ordering::Relaxed);
    }

    fn record_commit_wait_upload(
        &self,
        duration: Duration,
        reason: Option<SliceFreezeReason>,
        origin: WriteOriginKind,
    ) {
        let elapsed_us = duration.as_micros().min(u128::from(u64::MAX)) as u64;
        self.commit_wait_upload_ops.fetch_add(1, Ordering::Relaxed);
        self.commit_wait_upload_us
            .fetch_add(elapsed_us, Ordering::Relaxed);

        let (reason_ops, reason_us) = match reason {
            Some(SliceFreezeReason::SizeOrChunkEnd) => (
                &self.commit_wait_upload_size_ops,
                &self.commit_wait_upload_size_us,
            ),
            Some(SliceFreezeReason::MaxUnflushed) => (
                &self.commit_wait_upload_max_unflushed_ops,
                &self.commit_wait_upload_max_unflushed_us,
            ),
            Some(SliceFreezeReason::ExplicitFlush) => (
                &self.commit_wait_upload_explicit_flush_ops,
                &self.commit_wait_upload_explicit_flush_us,
            ),
            Some(SliceFreezeReason::Auto) => (
                &self.commit_wait_upload_auto_ops,
                &self.commit_wait_upload_auto_us,
            ),
            Some(SliceFreezeReason::CommitAgeSafety) => (
                &self.commit_wait_upload_commit_age_ops,
                &self.commit_wait_upload_commit_age_us,
            ),
            None => (
                &self.commit_wait_upload_unknown_reason_ops,
                &self.commit_wait_upload_unknown_reason_us,
            ),
        };
        reason_ops.fetch_add(1, Ordering::Relaxed);
        reason_us.fetch_add(elapsed_us, Ordering::Relaxed);

        let (origin_ops, origin_us) = match origin {
            WriteOriginKind::NormalOnly => (
                &self.commit_wait_upload_normal_only_ops,
                &self.commit_wait_upload_normal_only_us,
            ),
            WriteOriginKind::CachedOnly => (
                &self.commit_wait_upload_cached_only_ops,
                &self.commit_wait_upload_cached_only_us,
            ),
            WriteOriginKind::Mixed => (
                &self.commit_wait_upload_mixed_origin_ops,
                &self.commit_wait_upload_mixed_origin_us,
            ),
            WriteOriginKind::Unknown => (
                &self.commit_wait_upload_unknown_origin_ops,
                &self.commit_wait_upload_unknown_origin_us,
            ),
        };
        origin_ops.fetch_add(1, Ordering::Relaxed);
        origin_us.fetch_add(elapsed_us, Ordering::Relaxed);
    }

    fn record_commit_wait_retry(&self, duration: Duration) {
        self.commit_wait_retry_ops.fetch_add(1, Ordering::Relaxed);
        self.commit_wait_retry_us.fetch_add(
            duration.as_micros().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
    }

    fn record_flush_wait(&self, duration: Duration, slices: u64) {
        self.flush_wait_ops.fetch_add(1, Ordering::Relaxed);
        self.flush_wait_us.fetch_add(
            duration.as_micros().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
        self.flush_wait_slices.fetch_add(slices, Ordering::Relaxed);
    }

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
        self.flush_fragmentation_slices
            .fetch_add(slices, Ordering::Relaxed);
        self.flush_fragmentation_bytes
            .fetch_add(bytes, Ordering::Relaxed);
        self.flush_fragmentation_cached_sub_block_slices
            .fetch_add(cached_sub_block_slices, Ordering::Relaxed);
        self.flush_fragmentation_cached_sub_block_bytes
            .fetch_add(cached_sub_block_bytes, Ordering::Relaxed);
        self.flush_fragmentation_full_block_slices
            .fetch_add(full_block_slices, Ordering::Relaxed);
        self.flush_fragmentation_full_block_bytes
            .fetch_add(full_block_bytes, Ordering::Relaxed);
    }

    fn record_slice_create(&self) {
        self.slice_create_ops.fetch_add(1, Ordering::Relaxed);
    }

    fn record_slice_reuse(&self) {
        self.slice_reuse_ops.fetch_add(1, Ordering::Relaxed);
    }

    fn record_slice_reject_older_unique(&self) {
        self.slice_reject_older_unique_ops
            .fetch_add(1, Ordering::Relaxed);
    }

    fn record_slice_reject_dispatched_prefix(&self) {
        self.slice_reject_dispatched_prefix_ops
            .fetch_add(1, Ordering::Relaxed);
    }

    fn record_freeze(&self, reason: SliceFreezeReason, bytes: u64) {
        let (ops, total_bytes) = match reason {
            SliceFreezeReason::SizeOrChunkEnd => (&self.freeze_size_ops, &self.freeze_size_bytes),
            SliceFreezeReason::MaxUnflushed => (
                &self.freeze_max_unflushed_ops,
                &self.freeze_max_unflushed_bytes,
            ),
            SliceFreezeReason::ExplicitFlush => (
                &self.freeze_explicit_flush_ops,
                &self.freeze_explicit_flush_bytes,
            ),
            SliceFreezeReason::Auto => (&self.freeze_auto_ops, &self.freeze_auto_bytes),
            SliceFreezeReason::CommitAgeSafety => {
                (&self.freeze_commit_age_ops, &self.freeze_commit_age_bytes)
            }
        };
        ops.fetch_add(1, Ordering::Relaxed);
        total_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    fn record_upload_batch(
        &self,
        bytes: u64,
        blocks: u64,
        partial_tail: bool,
        partial_tail_reason: Option<SliceFreezeReason>,
        partial_tail_auto_trigger: Option<AutoFreezeTrigger>,
        partial_tail_origin: WriteOriginKind,
    ) {
        self.upload_batch_ops.fetch_add(1, Ordering::Relaxed);
        self.upload_batch_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.upload_batch_blocks
            .fetch_add(blocks, Ordering::Relaxed);
        if blocks <= 1 {
            self.upload_batch_single_block_ops
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.upload_batch_multi_block_ops
                .fetch_add(1, Ordering::Relaxed);
        }
        if partial_tail {
            self.upload_partial_tail_ops.fetch_add(1, Ordering::Relaxed);
            let origin_counter = match partial_tail_origin {
                WriteOriginKind::NormalOnly => &self.upload_partial_tail_normal_only_ops,
                WriteOriginKind::CachedOnly => &self.upload_partial_tail_cached_only_ops,
                WriteOriginKind::Mixed => &self.upload_partial_tail_mixed_origin_ops,
                WriteOriginKind::Unknown => &self.upload_partial_tail_unknown_origin_ops,
            };
            origin_counter.fetch_add(1, Ordering::Relaxed);
            if let Some(reason) = partial_tail_reason {
                let counter = match reason {
                    SliceFreezeReason::SizeOrChunkEnd => &self.upload_partial_tail_size_ops,
                    SliceFreezeReason::MaxUnflushed => &self.upload_partial_tail_max_unflushed_ops,
                    SliceFreezeReason::ExplicitFlush => {
                        &self.upload_partial_tail_explicit_flush_ops
                    }
                    SliceFreezeReason::Auto => &self.upload_partial_tail_auto_ops,
                    SliceFreezeReason::CommitAgeSafety => &self.upload_partial_tail_commit_age_ops,
                };
                counter.fetch_add(1, Ordering::Relaxed);
                if matches!(reason, SliceFreezeReason::Auto) {
                    let auto_counter = match partial_tail_auto_trigger {
                        Some(AutoFreezeTrigger::Age) => &self.upload_partial_tail_auto_age_ops,
                        Some(AutoFreezeTrigger::Idle) => &self.upload_partial_tail_auto_idle_ops,
                        Some(AutoFreezeTrigger::Pressure) => {
                            &self.upload_partial_tail_auto_pressure_ops
                        }
                        Some(AutoFreezeTrigger::TooMany) => {
                            &self.upload_partial_tail_auto_too_many_ops
                        }
                        Some(AutoFreezeTrigger::BufferHigh) => {
                            &self.upload_partial_tail_auto_buffer_high_ops
                        }
                        Some(AutoFreezeTrigger::FlushDuration) => {
                            &self.upload_partial_tail_auto_flush_duration_ops
                        }
                        None => &self.upload_partial_tail_auto_unknown_ops,
                    };
                    auto_counter.fetch_add(1, Ordering::Relaxed);
                    let auto_origin_counter = match partial_tail_origin {
                        WriteOriginKind::NormalOnly => {
                            &self.upload_partial_tail_auto_normal_only_ops
                        }
                        WriteOriginKind::CachedOnly => {
                            &self.upload_partial_tail_auto_cached_only_ops
                        }
                        WriteOriginKind::Mixed => &self.upload_partial_tail_auto_mixed_origin_ops,
                        WriteOriginKind::Unknown => {
                            &self.upload_partial_tail_auto_unknown_origin_ops
                        }
                    };
                    auto_origin_counter.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }
}

impl<B, M> Shared<B, M>
where
    B: BlockStore,
    M: MetaLayer,
{
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        inode: Arc<Inode>,
        config: Arc<WriteConfig>,
        backend: Arc<Backend<B, M>>,
        reader: Arc<DataReader<B, M>>,
        buffer_usage: Arc<AtomicU64>,
        write_back: Option<Arc<crate::vfs::cache::write_back::FsWriteBackCache>>,
        memory_budget: Option<MemoryBudget>,
        recent_pending_upload: Arc<RecentPendingUploadState>,
    ) -> Self {
        let upload_concurrency = config.upload_concurrency.max(1);
        Self {
            inode,
            config,
            buffer_usage,
            inner: Mutex::new(Inner {
                flush_waiting: 0,
                write_waiting: 0,
                chunks: BTreeMap::default(),
            }),
            write_notify: Notify::new(),
            flush_notify: Notify::new(),
            backend,
            reader,
            write_back,
            memory_budget,
            write_gen: AtomicU64::new(0),
            last_flushed_gen: AtomicU64::new(0),
            recent_pending_upload,
            writeback_error: ParkingMutex::new(None),
            upload_limit: Arc::new(Semaphore::new(upload_concurrency)),
            released: AtomicBool::new(false),
        }
    }

    fn record_writeback_error(&self, err: String) {
        let mut guard = self.writeback_error.lock();
        if guard.is_none() {
            *guard = Some(err);
        }
        self.flush_notify.notify_waiters();
        self.recent_pending_upload.notify.notify_waiters();
    }

    fn writeback_error(&self) -> Option<String> {
        self.writeback_error.lock().clone()
    }

    fn writeback_result(&self) -> anyhow::Result<()> {
        match self.writeback_error() {
            Some(err) => Err(anyhow::anyhow!("writeback failed: {err}")),
            None => Ok(()),
        }
    }
}

struct Inner {
    flush_waiting: u16,
    write_waiting: u16,
    chunks: BTreeMap<u64, ChunkState>,
}

impl Inner {
    fn chunk_handle<'a, B, M>(
        &'a mut self,
        shared: &'a Shared<B, M>,
        chunk_id: u64,
    ) -> ChunkHandle<'a, B, M>
    where
        B: BlockStore,
        M: MetaLayer,
    {
        ChunkHandle {
            chunk_id,
            inner: self,
            shared,
        }
    }

    fn get_or_create_chunk(&mut self, cid: u64) -> u64 {
        if self.chunks.contains_key(&cid) {
            return cid;
        }
        self.chunks.insert(cid, ChunkState::new(cid));
        cid
    }

    fn chunk_ids(&self) -> Vec<u64> {
        self.chunks.keys().copied().collect()
    }

    fn has_chunks(&self) -> bool {
        !self.chunks.is_empty()
    }
}

pub(crate) struct FileWriter<B, M> {
    shared: Arc<Shared<B, M>>,
}

impl<B, M> FileWriter<B, M>
where
    B: BlockStore + Send + Sync + 'static,
    M: MetaLayer + Send + Sync + 'static,
{
    async fn wait_for_buffer_progress(&self, max_sleep: Duration) -> Duration {
        let start = Instant::now();
        let _ = timeout(max_sleep, self.shared.flush_notify.notified()).await;
        start.elapsed()
    }

    async fn back_pressure(&self) -> anyhow::Result<()> {
        if let Some(budget) = &self.shared.memory_budget {
            let level = budget.pressure_level();
            if level >= PressureLevel::High {
                budget.log_state();
            }
            if level >= PressureLevel::Critical {
                self.force_flush_for_pressure().await;
                tokio::task::yield_now().await;
            }
        }

        let soft_limit = self.shared.config.buffer_size;
        if soft_limit == 0 {
            return Ok(());
        }

        let usage = self.shared.buffer_usage.load(Ordering::Relaxed);
        if usage <= soft_limit {
            return Ok(());
        }

        // Graduated backpressure: sleep proportionally to buffer fullness.
        // Keep sleeps short because FUSE writes arrive on the foreground path;
        // long fixed sleeps dominate p50 latency once usage hovers near the limit.
        let hard_limit = soft_limit.saturating_mul(2);
        let mid_limit = soft_limit + soft_limit / 2; // 1.5x soft

        if usage <= mid_limit {
            // Between soft and 1.5x: light sleep to let uploads drain
            let elapsed = self
                .wait_for_buffer_progress(WRITEBACK_SOFT_BACKPRESSURE_MIN_SLEEP)
                .await;
            self.shared
                .recent_pending_upload
                .record_buffer_soft_sleep(elapsed);
            return Ok(());
        }

        if usage <= hard_limit {
            // Between 1.5x and 2x: moderate sleep
            let elapsed = self
                .wait_for_buffer_progress(WRITEBACK_SOFT_BACKPRESSURE_MAX_SLEEP)
                .await;
            self.shared
                .recent_pending_upload
                .record_buffer_moderate_sleep(elapsed);
            return Ok(());
        }

        // Above hard limit: wait for actual flush progress, but keep the wait
        // quantum small. A fixed 20ms sleep here turns brief S3 completions
        // into foreground write tail latency.
        let mut total_wait = Duration::ZERO;
        let max_wait = write_buffer_max_wait(self.shared.config.writeback_mode);
        while self.shared.buffer_usage.load(Ordering::Relaxed) > hard_limit {
            if total_wait >= max_wait {
                return Err(anyhow::anyhow!(
                    "Timeout waiting for write buffer after {:?}. Current usage: {} bytes, limit: {} bytes",
                    total_wait,
                    self.shared.buffer_usage.load(Ordering::Relaxed),
                    hard_limit,
                ));
            }

            let elapsed = self
                .wait_for_buffer_progress(WRITEBACK_HARD_BACKPRESSURE_MAX_SLEEP)
                .await;
            self.shared
                .recent_pending_upload
                .record_buffer_hard_sleep(elapsed);
            total_wait += elapsed;
        }
        Ok(())
    }

    async fn force_flush_for_pressure(&self) {
        const PRESSURE_FLUSH_LIMIT: usize = 16;

        let slices: Vec<Arc<ParkingMutex<SliceState>>> = {
            let guard = self.shared.inner.lock().await;
            guard
                .chunks
                .values()
                .flat_map(|chunk| chunk.slices.iter().cloned())
                .collect()
        };

        let mut flushed = 0usize;
        for slice in slices {
            let handle = SliceHandle {
                slice: &slice,
                shared: &self.shared,
            };

            let should_try = handle.with_ref(|s| {
                matches!(s.state, SliceStatus::Writable)
                    && s.data.len() > 0
                    && s.last_mod.elapsed() >= Duration::from_millis(10)
            });
            if should_try && handle.freeze_auto_with_trigger(AutoFreezeTrigger::Pressure) {
                Self::spawn_flush_slice(self.shared.clone(), slice);
                flushed += 1;
                if flushed >= PRESSURE_FLUSH_LIMIT {
                    break;
                }
            }
        }
    }

    async fn wait_for_writeback_backpressure(&self, incoming_len: usize) -> anyhow::Result<()> {
        if !matches!(
            self.shared.config.writeback_mode,
            WriteBackMode::CommitBeforeUpload
        ) {
            return Ok(());
        }

        let soft = self.shared.config.writeback_recent_pending_soft_limit;
        let hard = self.shared.config.writeback_recent_pending_hard_limit;
        if soft == 0 {
            return Ok(());
        }

        let incoming = incoming_len as u64;
        loop {
            self.shared.writeback_result()?;
            let pending = self
                .shared
                .recent_pending_upload
                .bytes
                .load(Ordering::Acquire);
            let decision = decide_writeback_backpressure(pending, incoming, soft, hard);
            match decision {
                WritebackBackpressureDecision::Allow => return Ok(()),
                WritebackBackpressureDecision::SoftSleep(duration) => {
                    let start = Instant::now();
                    tokio::time::sleep(duration).await;
                    self.shared
                        .recent_pending_upload
                        .record_soft_sleep(start.elapsed());
                    return Ok(());
                }
                WritebackBackpressureDecision::Wait => {
                    let start = Instant::now();
                    self.shared.recent_pending_upload.notify.notified().await;
                    self.shared
                        .recent_pending_upload
                        .record_hard_wait(start.elapsed());
                }
            }
        }
    }

    pub(crate) fn new(
        inode: Arc<Inode>,
        config: Arc<WriteConfig>,
        backend: Arc<Backend<B, M>>,
        reader: Arc<DataReader<B, M>>,
        buffer_usage: Arc<AtomicU64>,
        write_back: Option<Arc<crate::vfs::cache::write_back::FsWriteBackCache>>,
    ) -> Self {
        Self::new_with_memory_budget(
            inode,
            config,
            backend,
            reader,
            buffer_usage,
            write_back,
            None,
            Arc::new(RecentPendingUploadState::new()),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn new_with_memory_budget(
        inode: Arc<Inode>,
        config: Arc<WriteConfig>,
        backend: Arc<Backend<B, M>>,
        reader: Arc<DataReader<B, M>>,
        buffer_usage: Arc<AtomicU64>,
        write_back: Option<Arc<crate::vfs::cache::write_back::FsWriteBackCache>>,
        memory_budget: Option<MemoryBudget>,
        recent_pending_upload: Arc<RecentPendingUploadState>,
    ) -> Self {
        let shared = Arc::new(Shared::new(
            inode,
            config,
            backend,
            reader,
            buffer_usage,
            write_back,
            memory_budget,
            recent_pending_upload,
        ));
        let flush_shared = Arc::downgrade(&shared);
        tokio::spawn(async move { Self::auto_flush(flush_shared).await });
        Self { shared }
    }

    // Write path: split into chunk spans, append to per-chunk slices, and possibly
    // trigger background flush/commit. Updates in-memory inode size at the end.
    #[tracing::instrument(
        level = "trace",
        skip(self, buf),
        fields(offset, len = buf.len(), bypass_flush_gate = false)
    )]
    pub(crate) async fn write_at(&self, offset: u64, buf: &[u8]) -> anyhow::Result<usize> {
        self.write_at_inner(offset, buf, false, 0, WriteOrigin::Normal)
            .await
    }

    #[tracing::instrument(
        level = "trace",
        skip(self, buf),
        fields(offset, len = buf.len(), bypass_flush_gate = true)
    )]
    pub(crate) async fn write_at_cached(
        &self,
        offset: u64,
        buf: &[u8],
        creation_unique: u64,
    ) -> anyhow::Result<usize> {
        self.write_at_inner(offset, buf, true, creation_unique, WriteOrigin::Cached)
            .await
    }

    async fn write_at_inner(
        &self,
        offset: u64,
        buf: &[u8],
        bypass_flush_gate: bool,
        creation_unique: u64,
        origin: WriteOrigin,
    ) -> anyhow::Result<usize> {
        self.shared.writeback_result()?;
        self.back_pressure().await?;
        self.wait_for_writeback_backpressure(buf.len()).await?;
        let mut guard = self.shared.inner.lock().await;

        if !bypass_flush_gate {
            // Wait for any ongoing flush to finish. This serializes ordinary
            // user writes with flush(). Kernel writeback-cache traffic uses the
            // bypass path so fsync/sync can continue draining dirty pages while
            // flush waits for the resulting slices to commit.
            guard.write_waiting += 1;
            while guard.flush_waiting > 0 {
                drop(guard);
                self.shared.write_notify.notified().await;
                guard = self.shared.inner.lock().await;
            }
            guard.write_waiting -= 1;
        }

        let layout = self.shared.config.layout;
        let chunk_index = layout.chunk_index_of(offset);
        let within_offset = layout.within_chunk_offset(offset);

        // Fast path: write fits entirely within a single chunk (99%+ of cached
        // 4KB page writes).  Avoids split_chunk_spans Vec allocation and loop.
        if within_offset + buf.len() as u64 <= layout.chunk_size {
            let cid = chunk_id_for(self.shared.inode.ino(), chunk_index)?;
            let ckey = guard.get_or_create_chunk(cid);
            let mut handle = guard.chunk_handle(&self.shared, ckey);
            let action = handle.write_at(within_offset, buf, creation_unique, origin, None)?;
            drop(guard);

            for slice in action.flush {
                Self::spawn_flush_slice(self.shared.clone(), slice);
            }
            if action.start_commit {
                let shared = self.shared.clone();
                tokio::spawn(async move { Self::commit_chunk(shared, ckey).await });
            }
        } else {
            // Slow path: write crosses chunk boundary.
            let mut position = 0;
            let spans = split_chunk_spans(layout, offset, buf.len());
            for span in spans {
                let cid = chunk_id_for(self.shared.inode.ino(), span.index)?;
                let ckey = guard.get_or_create_chunk(cid);
                let mut handle = guard.chunk_handle(&self.shared, ckey);
                let span_len = span.len.as_usize();
                let action = handle.write_at(
                    span.offset,
                    &buf[position..position + span_len],
                    creation_unique,
                    origin,
                    None,
                )?;
                drop(guard);

                for slice in action.flush {
                    Self::spawn_flush_slice(self.shared.clone(), slice);
                }
                if action.start_commit {
                    let shared = self.shared.clone();
                    tokio::spawn(async move { Self::commit_chunk(shared, ckey).await });
                }

                position += span_len;
                if position >= buf.len() {
                    break;
                }
                guard = self.shared.inner.lock().await;
            }
        }

        let new_len = offset + buf.len() as u64;
        if new_len > self.shared.inode.file_size() {
            self.shared.inode.extend_size(new_len);
        }
        self.shared.write_gen.fetch_add(1, Ordering::Release);
        Ok(buf.len())
    }

    async fn overlay_dirty_impl(&self, offset: u64, buf: &mut [u8]) -> anyhow::Result<bool> {
        if buf.is_empty() {
            return Ok(true);
        }

        let layout = self.shared.config.layout;
        let spans = split_chunk_spans(layout, offset, buf.len());

        // Pre-compute chunk IDs (propagate errors immediately).
        let span_cids: Vec<_> = spans
            .iter()
            .map(|s| chunk_id_for(self.shared.inode.ino(), s.index))
            .collect::<std::io::Result<Vec<_>>>()?;

        // Snapshot relevant slice Arcs under the inner lock, then release
        // immediately so that writes are not blocked during the data copy.
        let slice_refs: Vec<Option<Vec<Arc<ParkingMutex<SliceState>>>>> = {
            let guard = self.shared.inner.lock().await;
            span_cids
                .iter()
                .map(|cid| {
                    guard.chunks.get(cid).map(|chunk| {
                        chunk
                            .recently_committed
                            .iter()
                            .chain(chunk.slices.iter())
                            .cloned()
                            .collect()
                    })
                })
                .collect()
        };

        let mut has_overlap = false;
        for (span, slices_opt) in spans.iter().zip(slice_refs.iter()) {
            let Some(slices) = slices_opt else {
                continue;
            };
            let span_start = span.offset;
            let span_end = span.offset + span.len;
            for slice in slices {
                let state = slice.lock();
                if !state.can_overlay_read() {
                    continue;
                }
                let slice_start = state.offset;
                let slice_end = state.offset + state.data.len();
                let read_start = span_start.max(slice_start);
                let read_end = span_end.min(slice_end);
                if read_start < read_end
                    && state.write_range_has_written_overlap(
                        read_start,
                        (read_end - read_start).as_usize(),
                    )
                {
                    has_overlap = true;
                    break;
                }
            }
            if has_overlap {
                break;
            }
        }
        if !has_overlap {
            return Ok(false);
        }

        let mut missing = crate::utils::Intervals::new(offset, offset + buf.len() as u64);

        for (span, slices_opt) in spans.iter().zip(slice_refs.iter()) {
            let Some(slices) = slices_opt else {
                continue;
            };

            let chunk_start = span.index * layout.chunk_size;
            let span_start = span.offset;
            let span_end = span.offset + span.len;

            // Slices are append-only in creation order; later slices must win
            // over earlier dirty data for overlapping rewrites.  A slice in
            // `recently_committed` is always older than any live slice still
            // queued in `chunk.slices`, so apply committed grace-period data
            // first and let live dirty slices overwrite it when ranges overlap.
            for slice in slices {
                let state = slice.lock();
                if !state.can_overlay_read() {
                    continue;
                }

                let slice_start = state.offset;
                let slice_end = state.offset + state.data.len();
                let read_start = span_start.max(slice_start);
                let read_end = span_end.min(slice_end);
                if read_start >= read_end {
                    continue;
                }

                let dst_start = (chunk_start + read_start - offset).as_usize();
                let dst_end = (chunk_start + read_end - offset).as_usize();
                let copied = state
                    .data
                    .copy_written_into(read_start - slice_start, &mut buf[dst_start..dst_end])?;
                for (copied_start, copied_end) in copied {
                    missing.cut(
                        chunk_start + slice_start + copied_start,
                        chunk_start + slice_start + copied_end,
                    );
                }
            }
        }

        Ok(missing.collect().is_empty())
    }

    pub(crate) async fn overlay_dirty(&self, offset: u64, buf: &mut [u8]) -> anyhow::Result<()> {
        self.overlay_dirty_impl(offset, buf).await?;
        Ok(())
    }

    pub(crate) async fn read_dirty_if_fully_covered(
        &self,
        offset: u64,
        len: usize,
    ) -> anyhow::Result<Option<Vec<u8>>> {
        let mut buf = vec![0u8; len];
        if self.overlay_dirty_impl(offset, &mut buf).await? {
            Ok(Some(buf))
        } else {
            Ok(None)
        }
    }

    async fn coalesce_cached_slices_before_explicit_flush(&self) -> anyhow::Result<()> {
        let mut guard = self.shared.inner.lock().await;
        for chunk in guard.chunks.values_mut() {
            while Self::coalesce_one_cached_slice_run(&self.shared, chunk)? {}
        }
        Ok(())
    }

    fn coalesce_one_cached_slice_run(
        shared: &Arc<Shared<B, M>>,
        chunk: &mut ChunkState,
    ) -> anyhow::Result<bool> {
        let Some(indices) =
            Self::find_cached_coalesce_run(&chunk.slices, shared.config.cached_block_assembler)
        else {
            return Ok(false);
        };
        let run_slices = indices
            .iter()
            .filter_map(|idx| chunk.slices.get(*idx).cloned())
            .collect::<Vec<_>>();
        if run_slices.len() < 2 {
            return Ok(false);
        }

        let merged = Self::build_coalesced_cached_slice(shared, chunk.chunk_id, &run_slices)?;
        chunk
            .slices
            .retain(|slice| !run_slices.iter().any(|old| Arc::ptr_eq(slice, old)));

        let creation_unique = merged.lock().creation_unique;
        let insert_pos = if creation_unique == 0 {
            chunk.slices.len()
        } else {
            chunk
                .slices
                .iter()
                .position(|slice| slice.lock().creation_unique > creation_unique)
                .unwrap_or(chunk.slices.len())
        };
        chunk.slices.insert(insert_pos, merged);
        Ok(true)
    }

    fn find_cached_coalesce_run(
        slices: &VecDeque<Arc<ParkingMutex<SliceState>>>,
        allow_overlap: bool,
    ) -> Option<Vec<usize>> {
        if slices.len() < 2 {
            return None;
        }

        let mut candidates = Vec::new();
        for (index, slice) in slices.iter().enumerate() {
            let state = slice.lock();
            if !state.can_coalesce_cached_writable() {
                continue;
            }
            let (start, end) = state.data_range()?;
            candidates.push(CachedCoalesceCandidate { index, start, end });
        }
        candidates.sort_by_key(|candidate| (candidate.start, candidate.end, candidate.index));

        let mut run: Vec<CachedCoalesceCandidate> = Vec::new();
        let mut run_end = 0;
        for candidate in candidates {
            if !run.is_empty() {
                let extends_current_run = if allow_overlap {
                    candidate.start <= run_end
                } else {
                    candidate.start == run_end
                };
                if extends_current_run {
                    run_end = run_end.max(candidate.end);
                    run.push(candidate);
                    continue;
                }

                if let Some(indices) = Self::valid_cached_coalesce_indices(slices, &run) {
                    return Some(indices);
                }
                run.clear();
            }
            run_end = candidate.end;
            run.push(candidate);
        }

        Self::valid_cached_coalesce_indices(slices, &run)
    }

    fn valid_cached_coalesce_indices(
        slices: &VecDeque<Arc<ParkingMutex<SliceState>>>,
        run: &[CachedCoalesceCandidate],
    ) -> Option<Vec<usize>> {
        if run.len() < 2 {
            return None;
        }

        let run_start = run.first()?.start;
        let run_end = run.iter().map(|candidate| candidate.end).max()?;
        if Self::cached_coalesce_run_has_external_overlap(slices, run, run_start, run_end) {
            return None;
        }

        Some(run.iter().map(|candidate| candidate.index).collect())
    }

    fn cached_coalesce_run_has_external_overlap(
        slices: &VecDeque<Arc<ParkingMutex<SliceState>>>,
        run: &[CachedCoalesceCandidate],
        run_start: u64,
        run_end: u64,
    ) -> bool {
        for (index, slice) in slices.iter().enumerate() {
            if run.iter().any(|candidate| candidate.index == index) {
                continue;
            }

            let state = slice.lock();
            let Some((start, end)) = state.data_range() else {
                continue;
            };
            if start < run_end && run_start < end {
                return true;
            }
        }

        false
    }

    fn build_coalesced_cached_slice(
        shared: &Arc<Shared<B, M>>,
        chunk_id: u64,
        run_slices: &[Arc<ParkingMutex<SliceState>>],
    ) -> anyhow::Result<Arc<ParkingMutex<SliceState>>> {
        struct CachedPiece {
            offset: u64,
            creation_unique: u64,
            max_write_unique: u64,
            write_origin_mask: u8,
            data: Vec<u8>,
            order: usize,
        }

        let mut pieces = Vec::with_capacity(run_slices.len());
        for (order, slice) in run_slices.iter().enumerate() {
            let state = slice.lock();
            anyhow::ensure!(
                state.can_coalesce_cached_writable(),
                "cached slice changed while preparing coalesce"
            );
            let len = state.data.len().as_usize();
            let mut data = vec![0; len];
            let copied = state.data.copy_into(0, &mut data)?;
            anyhow::ensure!(
                copied == len,
                "cached slice copy length mismatch while preparing coalesce"
            );
            pieces.push(CachedPiece {
                offset: state.offset,
                creation_unique: state.creation_unique,
                max_write_unique: state.max_write_unique,
                write_origin_mask: state.write_origin_mask,
                data,
                order,
            });
        }

        let Some(run_start) = pieces.iter().map(|piece| piece.offset).min() else {
            return Err(anyhow::anyhow!("cannot coalesce empty cached slice run"));
        };
        let run_end = pieces
            .iter()
            .map(|piece| piece.offset.saturating_add(piece.data.len() as u64))
            .max()
            .unwrap_or(run_start);

        let merged_len = (run_end - run_start).as_usize();
        let mut merged_data = vec![0; merged_len];
        let mut creation_unique = u64::MAX;
        let mut max_write_unique = 0;
        let mut write_origin_mask = 0u8;
        pieces.sort_by_key(|piece| (piece.creation_unique, piece.order));
        for piece in &pieces {
            let start = (piece.offset - run_start).as_usize();
            let end = start + piece.data.len();
            merged_data[start..end].copy_from_slice(&piece.data);
            if piece.creation_unique != 0 && piece.creation_unique < creation_unique {
                creation_unique = piece.creation_unique;
            }
            max_write_unique = max_write_unique.max(piece.max_write_unique);
            write_origin_mask |= piece.write_origin_mask;
        }
        if creation_unique == u64::MAX {
            creation_unique = 0;
        }

        let mut merged = SliceState::new(
            chunk_id,
            run_start,
            shared.config.clone(),
            shared.buffer_usage.clone(),
            shared.memory_budget.clone(),
            creation_unique,
        );
        merged.data.append(&merged_data)?;
        merged.max_write_unique = max_write_unique;
        merged.write_origin_mask = write_origin_mask;
        merged.update_usage(merged.data.alloc_bytes());

        Ok(Arc::new(ParkingMutex::new(merged)))
    }

    // Flush: freeze all slices, upload them, and wait for those slices to commit.
    // This blocks new writes until flushing completes (flush_waiting gate).
    //
    // We track *specific slices* rather than waiting for chunks to drain.
    // Waiting for chunks is incorrect under continuous writes: for small files
    // where all writes target the same chunk, new slices keep arriving in that
    // chunk, so it never becomes empty and flush never returns.  By freezing
    // every slice present at the start and then waiting only for those slices
    // to reach Committed, we guarantee forward progress regardless of
    // concurrent write traffic. Failed writeback is returned to the caller
    // instead of being treated as a successful flush.
    #[tracing::instrument(level = "trace", skip(self))]
    pub(crate) async fn flush(&self) -> anyhow::Result<()> {
        self.flush_with_deadline(FLUSH_DEADLINE).await
    }

    async fn pending_committed_uploads_for_range(
        &self,
        offset: u64,
        len: usize,
    ) -> anyhow::Result<Vec<Arc<ParkingMutex<SliceState>>>> {
        if len == 0 {
            return Ok(Vec::new());
        }

        let layout = self.shared.config.layout;
        let spans = split_chunk_spans(layout, offset, len);
        let span_cids: Vec<_> = spans
            .iter()
            .map(|span| chunk_id_for(self.shared.inode.ino(), span.index))
            .collect::<std::io::Result<Vec<_>>>()?;

        let mut pending = Vec::new();
        let guard = self.shared.inner.lock().await;
        for (span, cid) in spans.iter().zip(span_cids.iter()) {
            let Some(chunk) = guard.chunks.get(cid) else {
                continue;
            };
            let chunk_start = span.index * layout.chunk_size;
            let span_start = span.offset;
            let span_end = span.offset + span.len;

            for slice in chunk.recently_committed.iter().chain(chunk.slices.iter()) {
                let state = slice.lock();
                if !matches!(state.state, SliceStatus::Committed) || state.upload_complete() {
                    continue;
                }

                let slice_start = state.offset;
                let slice_end = state.offset + state.data.len();
                if span_start < slice_end && slice_start < span_end {
                    tracing::trace!(
                        ino = self.shared.inode.ino(),
                        chunk_id = *cid,
                        offset = chunk_start + slice_start,
                        len = state.data.len(),
                        "waiting for committed writeback upload before source read"
                    );
                    pending.push(slice.clone());
                }
            }
        }

        Ok(pending)
    }

    async fn wait_committed_uploads_for_range(
        &self,
        offset: u64,
        len: usize,
        deadline: Duration,
    ) -> anyhow::Result<()> {
        if len == 0 {
            return Ok(());
        }

        let start = Instant::now();
        loop {
            self.shared.writeback_result()?;
            let pending = self
                .pending_committed_uploads_for_range(offset, len)
                .await?;
            if pending.is_empty() {
                return Ok(());
            }

            for slice in &pending {
                let handle = SliceHandle {
                    slice,
                    shared: &self.shared,
                };
                if handle.can_continue_upload() {
                    Self::spawn_flush_slice(self.shared.clone(), slice.clone());
                }
            }

            if start.elapsed() > deadline {
                let states: Vec<_> = pending
                    .iter()
                    .map(|slice| {
                        let state = slice.lock();
                        format!(
                            "{:?}@{}+{} uploaded={} in_flight={}",
                            state.state,
                            state.offset,
                            state.data.len(),
                            state.uploaded,
                            state.in_flight
                        )
                    })
                    .collect();
                let ino = self.shared.inode.ino();
                return Err(anyhow::anyhow!(
                    "upload wait timeout after {:?} for ino {ino}, offset {offset}, len {len}, pending: {:?}",
                    deadline,
                    states
                ));
            }

            let _ = timeout(
                Duration::from_millis(20),
                self.shared.flush_notify.notified(),
            )
            .await;
        }
    }

    pub(crate) async fn flush_with_deadline(&self, deadline: Duration) -> anyhow::Result<()> {
        self.shared.writeback_result()?;
        {
            let mut guard = self.shared.inner.lock().await;
            if !guard.has_chunks() {
                return self.shared.writeback_result();
            }

            guard.flush_waiting += 1;
        }

        let start = Instant::now();
        let mut captured_slices = 0u64;
        let result = {
            let mut flushed_gen = self.shared.write_gen.load(Ordering::Acquire);
            loop {
                self.coalesce_cached_slices_before_explicit_flush().await?;

                // Snapshot every slice that exists right now.
                let slices: Vec<Arc<ParkingMutex<SliceState>>> = {
                    let guard = self.shared.inner.lock().await;
                    guard
                        .chunks
                        .values()
                        .flat_map(|chunk| chunk.slices.iter().cloned())
                        .collect()
                };
                let mut snapshot_bytes = 0u64;
                let mut cached_sub_block_slices = 0u64;
                let mut cached_sub_block_bytes = 0u64;
                let mut full_block_slices = 0u64;
                let mut full_block_bytes = 0u64;
                for slice in &slices {
                    let state = slice.lock();
                    let len = state.data.len();
                    snapshot_bytes = snapshot_bytes.saturating_add(len);
                    let block_size = state.data.block_size() as u64;
                    if matches!(state.write_origin_kind(), WriteOriginKind::CachedOnly)
                        && len < block_size
                    {
                        cached_sub_block_slices += 1;
                        cached_sub_block_bytes = cached_sub_block_bytes.saturating_add(len);
                    }
                    if len >= block_size && len % block_size == 0 {
                        full_block_slices += 1;
                        full_block_bytes = full_block_bytes.saturating_add(len);
                    }
                }
                if !slices.is_empty() {
                    self.shared
                        .recent_pending_upload
                        .record_flush_fragmentation(
                            slices.len() as u64,
                            snapshot_bytes,
                            cached_sub_block_slices,
                            cached_sub_block_bytes,
                            full_block_slices,
                            full_block_bytes,
                        );
                }
                captured_slices = captured_slices.saturating_add(slices.len() as u64);

                // Freeze any that are still writable and kick off their uploads.
                for slice in &slices {
                    let handle = SliceHandle {
                        slice,
                        shared: &self.shared,
                    };
                    if handle.freeze_with_reason(SliceFreezeReason::ExplicitFlush) {
                        Self::spawn_flush_slice(self.shared.clone(), slice.clone());
                    }
                }

                // Wait for every slice we captured to be committed.  Cached
                // writeback may append more data while we are flushing, so once
                // this batch drains we re-check write_gen and repeat until no
                // new writes arrived during the wait.
                let batch_result: anyhow::Result<()> = loop {
                    if let Some(err) = self.shared.writeback_error() {
                        break Err(anyhow::anyhow!("writeback failed: {err}"));
                    }

                    let all_done = slices
                        .iter()
                        .all(|s| matches!(s.lock().state, SliceStatus::Committed));

                    if all_done {
                        break Ok(());
                    }

                    if timeout(FLUSH_WAIT, self.shared.flush_notify.notified())
                        .await
                        .is_err()
                    {
                        if start.elapsed() > deadline {
                            let pending: Vec<_> = slices
                                .iter()
                                .filter(|s| !matches!(s.lock().state, SliceStatus::Committed))
                                .map(|s| {
                                    let g = s.lock();
                                    format!("{:?}@{}", g.state, g.offset)
                                })
                                .collect();
                            let ino = self.shared.inode.ino();
                            tracing::error!(
                                ino,
                                elapsed_ms = start.elapsed().as_millis() as u64,
                                pending_slices = pending.len(),
                                pending_states = ?pending,
                                "flush timeout"
                            );
                            break Err(anyhow::anyhow!(
                                "flush timeout after {:?} for ino {ino}, {}/{} slices still pending: {:?}",
                                deadline,
                                ino,
                                pending.len(),
                                pending
                            ));
                        }

                        // Safety net: FLUSH_WAIT elapsed without progress.  If
                        // any slice is Uploaded but commit_chunk is not running,
                        // re-spawn commit_chunk so flush does not wait forever.
                        let mut guard = self.shared.inner.lock().await;
                        for (cid, chunk) in guard.chunks.iter_mut() {
                            if !chunk.commit_started
                                && chunk
                                    .slices
                                    .iter()
                                    .any(|s| matches!(s.lock().state, SliceStatus::Uploaded))
                            {
                                chunk.commit_started = true;
                                let shared = self.shared.clone();
                                let cid = *cid;
                                tokio::spawn(async move { Self::commit_chunk(shared, cid).await });
                            }
                        }
                    }
                };

                batch_result?;

                let current_gen = self.shared.write_gen.load(Ordering::Acquire);
                if current_gen == flushed_gen {
                    break Ok(());
                }
                flushed_gen = current_gen;
            }
        };
        self.shared
            .recent_pending_upload
            .record_flush_wait(start.elapsed(), captured_slices);

        // Notify all write events.
        let mut guard = self.shared.inner.lock().await;
        if guard.flush_waiting > 0 {
            guard.flush_waiting -= 1;
        }
        if guard.flush_waiting == 0 && guard.write_waiting > 0 {
            self.shared.write_notify.notify_waiters();
        }

        // Let has_pending() short-circuit when no new writes arrived since we finished.
        if result.is_ok() {
            self.shared.last_flushed_gen.store(
                self.shared.write_gen.load(Ordering::Acquire),
                Ordering::Release,
            );
        }

        result
    }

    pub(crate) async fn clear(&self) {
        let slices: Vec<Arc<ParkingMutex<SliceState>>> = {
            let guard = self.shared.inner.lock().await;
            guard
                .chunks
                .values()
                .flat_map(|chunk| chunk.slices.iter().cloned())
                .collect()
        };

        for slice in slices {
            let mut guard = slice.lock();
            guard.data.release_all();
            guard.update_usage(0);
        }

        let mut guard = self.shared.inner.lock().await;
        guard.chunks.clear();

        if guard.flush_waiting > 0 {
            self.shared.flush_notify.notify_waiters();
        }
    }

    pub(crate) async fn has_pending(&self) -> bool {
        if self.shared.writeback_error().is_some() {
            return true;
        }
        // Fast path: if no writes arrived since the last successful flush,
        // there cannot be any unflushed chunks.
        let gen_val = self.shared.write_gen.load(Ordering::Acquire);
        let flushed = self.shared.last_flushed_gen.load(Ordering::Acquire);
        if gen_val == flushed {
            return false;
        }
        let guard = self.shared.inner.lock().await;
        guard.has_chunks()
    }

    pub(crate) async fn has_overlay_state(&self) -> bool {
        let guard = self.shared.inner.lock().await;
        guard.chunks.values().any(|chunk| {
            chunk
                .recently_committed
                .iter()
                .chain(chunk.slices.iter())
                .any(|slice| slice.lock().can_overlay_read())
        })
    }

    fn mark_active(&self) {
        self.shared.released.store(false, Ordering::Release);
    }

    fn mark_released(&self) {
        self.shared.released.store(true, Ordering::Release);
    }

    async fn released_cleanup_ready(&self) -> bool {
        self.shared.released.load(Ordering::Acquire)
            && !self.has_pending().await
            && !self.has_overlay_state().await
    }

    /// Spawn a background task to upload a frozen slice's data.
    /// Metadata commit is handled separately by commit_chunk.
    fn spawn_flush_slice(shared: Arc<Shared<B, M>>, slice: Arc<ParkingMutex<SliceState>>) {
        // Guard against spawning duplicate upload tasks for the same slice.
        {
            let mut s = slice.lock();
            if s.upload_task_active {
                return;
            }
            s.upload_task_active = true;
        }
        Self::spawn_upload_task(shared, slice);
    }

    async fn seal_writeback_record_if_ready(
        shared: &Arc<Shared<B, M>>,
        slice: &Arc<ParkingMutex<SliceState>>,
    ) -> anyhow::Result<bool> {
        let Some(wb) = &shared.write_back else {
            return Ok(false);
        };

        let handle = SliceHandle { slice, shared };
        let Some((key, chunk_offset, length)) = handle.claim_writeback_record_seal() else {
            return Ok(false);
        };

        match wb.seal_slice_record(key, chunk_offset, length).await {
            Ok(()) => {
                handle.mark_writeback_record_sealed();
                Ok(true)
            }
            Err(err) => {
                let message = format!("writeback record seal failed: {err}");
                handle.mark_failed(anyhow::anyhow!(message));
                Err(err)
            }
        }
    }

    /// Pipeline upload task: dispatches multiple block batches concurrently
    /// using a JoinSet.  As each block range completes, `uploaded` advances
    /// through contiguous confirmed blocks.  New blocks that become ready
    /// (from ongoing writes) are dispatched immediately without waiting for
    /// previous uploads to finish.
    fn spawn_upload_task(shared: Arc<Shared<B, M>>, slice: Arc<ParkingMutex<SliceState>>) {
        tokio::spawn(async move {
            // Allocate slice_id once, up front, before dispatching any blocks.
            let slice_id = {
                let handle = SliceHandle {
                    slice: &slice,
                    shared: &shared,
                };
                let existing = handle.with_ref(|s| s.slice_id);
                match existing {
                    Some(id) => id,
                    None => match shared.backend.meta().next_id(SLICE_ID_KEY).await {
                        Ok(id) => {
                            let id = id as u64;
                            handle.set_slice_id(id);
                            id
                        }
                        Err(e) => {
                            handle.mark_failed(anyhow::anyhow!("Failed to get slice id: {e}"));
                            return;
                        }
                    },
                }
            };

            // Result type for each upload sub-task: (start_idx, end_idx, bytes_len)
            type UploadResult = Result<(usize, usize, u64), anyhow::Error>;
            let mut join_set: JoinSet<UploadResult> = JoinSet::new();

            loop {
                // Dispatch all currently-available blocks.
                loop {
                    let handle = SliceHandle {
                        slice: &slice,
                        shared: &shared,
                    };

                    let plan = match handle.prepare_upload() {
                        Ok(Some(plan)) => plan,
                        Ok(None) => break,
                        Err(err) => {
                            warn!(error = ?err, "prepare_upload failed");
                            handle.mark_failed(err);
                            join_set.abort_all();
                            return;
                        }
                    };

                    let UploadPlan {
                        chunk_id,
                        data,
                        slice_id: _,
                        uploaded: batch_offset,
                        write_origin,
                    } = plan;

                    let mut all_chunks = Vec::new();
                    let mut data_len = 0u64;
                    let mut start_idx = usize::MAX;
                    let mut end_idx = 0usize;

                    for (index, chunks) in data {
                        if index < start_idx {
                            start_idx = index;
                        }
                        if index + 1 > end_idx {
                            end_idx = index + 1;
                        }
                        for chunk in chunks {
                            data_len += chunk.len() as u64;
                            all_chunks.push(chunk);
                        }
                    }

                    // Spawn a sub-task for this batch of blocks.
                    let shared2 = shared.clone();
                    let wb_ref = if matches!(
                        shared.config.writeback_mode,
                        WriteBackMode::CommitBeforeUpload
                    ) {
                        shared.write_back.clone()
                    } else {
                        None
                    };
                    let slice_for_persist = slice.clone();
                    let ino = shared.inode.ino();
                    let layout = shared.config.layout;
                    let upload_priority = if matches!(
                        shared.config.writeback_mode,
                        WriteBackMode::CommitBeforeUpload
                    ) {
                        UploadPriority::Writeback
                    } else {
                        UploadPriority::Foreground
                    };
                    join_set.spawn(async move {
                        // Persist locally only for commit-before-upload, where
                        // metadata may become visible before remote upload lands.
                        let persist = wb_ref.as_ref().map(|wb| {
                            let wb = wb.clone();
                            let chunks = all_chunks.clone();
                            let shared_for_persist = shared2.clone();
                            let slice_for_persist = slice_for_persist.clone();
                            let key = crate::vfs::cache::keys::DirtySliceKey {
                                ino,
                                chunk_id,
                                local_seq: slice_id,
                                epoch: 0,
                            };
                            async move {
                                let stage_start = shared_for_persist
                                    .recent_pending_upload
                                    .record_stage_start(data_len);
                                let result = async {
                                    wb.persist_slice_data(key, chunks, batch_offset).await?;
                                    SliceHandle {
                                        slice: &slice_for_persist,
                                        shared: &shared_for_persist,
                                    }
                                    .mark_writeback_persisted(data_len);
                                    Self::seal_writeback_record_if_ready(
                                        &shared_for_persist,
                                        &slice_for_persist,
                                    )
                                    .await?;
                                    Self::try_commit_before_upload_if_front(
                                        &shared_for_persist,
                                        &slice_for_persist,
                                    )
                                    .await;
                                    Ok::<(), anyhow::Error>(())
                                }
                                .await;
                                shared_for_persist
                                    .recent_pending_upload
                                    .record_stage_finish(stage_start, data_len, result.is_ok());
                                match result {
                                    Ok(()) => Ok(()),
                                    Err(err) => {
                                        let message =
                                            format!("writeback stage persist failed: {err}");
                                        SliceHandle {
                                            slice: &slice_for_persist,
                                            shared: &shared_for_persist,
                                        }
                                        .mark_failed(anyhow::anyhow!(message));
                                        Err(err)
                                    }
                                }
                            }
                        });

                        let uploader = DataUploader::new(layout, &shared2.backend);
                        let upload = async {
                            let _remote_upload = shared2
                                .recent_pending_upload
                                .track_remote_upload_inflight(data_len);
                            backoff(UPLOAD_MAX_RETRIES, || async {
                                match uploader
                                    .write_at_vectored_with_priority_and_limit(
                                        slice_id,
                                        batch_offset.into(),
                                        &all_chunks,
                                        upload_priority,
                                        Some(shared2.upload_limit.clone()),
                                    )
                                    .await
                                {
                                    Ok(_) => Ok(()),
                                    Err(err) => {
                                        warn!(
                                            chunk_id,
                                            slice_id,
                                            offset = batch_offset,
                                            len = data_len,
                                            error = ?err,
                                            "pipeline upload failed, retrying"
                                        );
                                        Err(MetaError::ContinueRetry(RetryReason::VersionConflict))
                                    }
                                }
                            })
                            .await
                        };

                        let stage_first_upload = should_stage_first_writeback_upload(
                            upload_priority,
                            persist.is_some(),
                            write_origin,
                        );
                        let (persist_result, result) = if stage_first_upload {
                            let persist = persist.expect("stage-first upload requires persist");
                            let (persist_result, result) =
                                join_writeback_stage_then_upload(persist, upload).await;
                            (Some(persist_result), result)
                        } else {
                            join_best_effort_persist(persist, upload).await
                        };
                        if let Some(Err(e)) = persist_result {
                            warn!(
                                ino, chunk_id, slice_id, error = ?e,
                                "writeback stage persist failed"
                            );
                            return Err(anyhow::anyhow!("writeback stage persist failed: {e}"));
                        }

                        match result {
                            Ok(()) => Ok((start_idx, end_idx, data_len)),
                            Err(err) => Err(anyhow::anyhow!(err)),
                        }
                    });
                }

                // Check if we're done (no in-flight, no more blocks).
                let is_done = {
                    let s = slice.lock();
                    s.upload_complete()
                        && matches!(
                            s.state,
                            SliceStatus::Uploaded | SliceStatus::Committed | SliceStatus::Failed
                        )
                };
                if is_done && join_set.is_empty() {
                    slice.lock().upload_task_active = false;
                    shared.flush_notify.notify_waiters();
                    return;
                }

                if join_set.is_empty() {
                    // No in-flight uploads and no blocks to dispatch, but slice
                    // isn't done yet (still Writable/Readonly with no idle blocks).
                    // This means writes haven't filled the next block yet.
                    // Wait for notification that new data arrived or slice was frozen.
                    let handle = SliceHandle {
                        slice: &slice,
                        shared: &shared,
                    };
                    // Check one more time after waking.
                    let has_work = handle.with_ref(|s| s.has_idle_block());
                    if !has_work {
                        // If the slice is frozen with nothing in flight, we're done.
                        let done_check = handle.with_ref(|s| {
                            matches!(
                                s.state,
                                SliceStatus::Uploaded
                                    | SliceStatus::Committed
                                    | SliceStatus::Failed
                            )
                        });
                        if done_check {
                            slice.lock().upload_task_active = false;
                            shared.flush_notify.notify_waiters();
                            return;
                        }
                        // Wait for slice notification (new data or state change).
                        let notify = slice.lock().notify.clone();
                        let _ = tokio::time::timeout(Duration::from_millis(50), notify.notified())
                            .await;
                    }
                    continue;
                }

                // Wait for at least one upload to complete.
                match join_set.join_next().await {
                    Some(Ok(Ok((start_idx, end_idx, data_len)))) => {
                        let handle = SliceHandle {
                            slice: &slice,
                            shared: &shared,
                        };
                        handle.advance_upload_range(start_idx, end_idx, data_len);
                        Self::split_uploaded_prefix_tail(
                            &shared,
                            handle.with_ref(|s| s.chunk_id),
                            &slice,
                        )
                        .await;
                        Self::remove_writeback_record_if_uploaded_committed(&shared, &slice).await;
                        handle.try_commit().await;
                    }
                    Some(Ok(Err(err))) => {
                        let handle = SliceHandle {
                            slice: &slice,
                            shared: &shared,
                        };
                        warn!(error = ?err, "pipeline upload batch failed after retries");
                        handle.mark_failed(err);
                        join_set.abort_all();
                        return;
                    }
                    Some(Err(join_err)) => {
                        let handle = SliceHandle {
                            slice: &slice,
                            shared: &shared,
                        };
                        handle.mark_failed(anyhow::anyhow!("upload task panicked: {}", join_err));
                        join_set.abort_all();
                        return;
                    }
                    None => unreachable!("join_set is not empty"),
                }
            }
        });
    }

    async fn pop_front_slice(shared: &Arc<Shared<B, M>>, chunk_id: u64) -> bool {
        let mut guard = shared
            .inner
            .lock()
            .instrument(tracing::trace_span!("commit_chunk.pop_lock"))
            .await;
        if let Some(chunk) = guard.chunks.get_mut(&chunk_id) {
            let _ = chunk.slices.pop_front();
        }
        if guard.flush_waiting > 0 {
            shared.flush_notify.notify_waiters();
        }

        let empty = guard
            .chunks
            .get(&chunk_id)
            .map(|c| c.slices.is_empty() && c.recently_committed.is_empty())
            .unwrap_or(true);
        if empty {
            guard.chunks.remove(&chunk_id);
            if !guard.has_chunks() && guard.flush_waiting > 0 {
                shared.flush_notify.notify_waiters();
            }
        }
        empty
    }

    fn account_recent_pending_if_needed(
        shared: &Arc<Shared<B, M>>,
        slice: &Arc<ParkingMutex<SliceState>>,
    ) {
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
        if bytes > 0 {
            shared
                .recent_pending_upload
                .bytes
                .fetch_add(bytes, Ordering::AcqRel);
        }
    }

    async fn remove_writeback_record_if_uploaded_committed(
        shared: &Arc<Shared<B, M>>,
        slice: &Arc<ParkingMutex<SliceState>>,
    ) {
        let key = {
            let state = slice.lock();
            if !matches!(state.state, SliceStatus::Committed) || !state.upload_complete() {
                return;
            }
            let Some(slice_id) = state.slice_id else {
                return;
            };
            crate::vfs::cache::keys::DirtySliceKey {
                ino: shared.inode.ino(),
                chunk_id: state.chunk_id,
                local_seq: slice_id,
                epoch: 0,
            }
        };

        if let Some(wb) = &shared.write_back {
            let _ = wb.remove(&key).await;
        }
    }

    fn keep_recently_committed_if_overlay_needed(
        slice: Arc<ParkingMutex<SliceState>>,
        recently_committed: &mut VecDeque<Arc<ParkingMutex<SliceState>>>,
    ) {
        let needs_overlay = {
            let mut state = slice.lock();
            if state.can_overlay_read() {
                true
            } else {
                state.data.release_all();
                state.update_usage(0);
                false
            }
        };
        if needs_overlay {
            recently_committed.push_back(slice);
        }
    }

    async fn split_uploaded_prefix_tail(
        shared: &Arc<Shared<B, M>>,
        chunk_id: u64,
        expected: &Arc<ParkingMutex<SliceState>>,
    ) -> bool {
        if !matches!(
            shared.config.writeback_mode,
            WriteBackMode::UploadBeforeCommit
        ) {
            return false;
        }
        if !shared.config.upload_before_commit_prefix_split {
            return false;
        }

        let mut guard = shared.inner.lock().await;
        let Some(chunk) = guard.chunks.get_mut(&chunk_id) else {
            return false;
        };
        let is_front = chunk
            .slices
            .front()
            .is_some_and(|front| Arc::ptr_eq(front, expected));
        if !is_front {
            return false;
        }

        let mut state = expected.lock();
        if !matches!(state.state, SliceStatus::Writable)
            || state.in_flight != 0
            || state.meta_write_started
            || state.slice_id.is_none()
        {
            return false;
        }

        let block_size = state.data.block_size() as u64;
        let uploaded = state.uploaded;
        let data_len = state.data.len();
        let dispatched_end = state.dispatched_end as u64 * block_size;
        if uploaded == 0
            || uploaded >= data_len
            || uploaded != dispatched_end
            || !uploaded.is_multiple_of(block_size)
        {
            return false;
        }

        let tail_len = data_len - uploaded;
        if tail_len >= block_size {
            return false;
        }

        let mut tail_buf = vec![0_u8; tail_len.as_usize()];
        match state.data.copy_into(uploaded, &mut tail_buf) {
            Ok(read) if read == tail_buf.len() => {}
            Ok(read) => {
                warn!(
                    chunk_id,
                    uploaded, tail_len, read, "prefix split could not copy the full tail"
                );
                return false;
            }
            Err(err) => {
                warn!(
                    chunk_id,
                    uploaded,
                    tail_len,
                    error = ?err,
                    "prefix split tail copy failed"
                );
                return false;
            }
        }

        let tail_offset = state.offset + uploaded;
        let mut tail = SliceState::new(
            chunk_id,
            tail_offset,
            shared.config.clone(),
            shared.buffer_usage.clone(),
            shared.memory_budget.clone(),
            state.creation_unique,
        );
        tail.max_write_unique = state.max_write_unique;
        tail.write_origin_mask = state.write_origin_mask;
        if let Err(err) = tail.data.append(&tail_buf) {
            warn!(
                chunk_id,
                tail_offset,
                tail_len,
                error = ?err,
                "prefix split tail append failed"
            );
            return false;
        }
        let tail_alloc = tail.data.alloc_bytes();
        tail.update_usage(tail_alloc);

        if let Err(err) = state.data.truncate(uploaded) {
            warn!(
                chunk_id,
                uploaded,
                error = ?err,
                "prefix split truncate failed"
            );
            return false;
        }
        let prefix_alloc = state.data.alloc_bytes();
        state.update_usage(prefix_alloc);
        state.state = SliceStatus::Uploaded;
        state.notify.notify_waiters();
        drop(state);

        chunk.slices.insert(1, Arc::new(ParkingMutex::new(tail)));
        shared.flush_notify.notify_waiters();
        true
    }

    async fn move_front_slice_to_recently_committed(
        shared: &Arc<Shared<B, M>>,
        chunk_id: u64,
        expected: &Arc<ParkingMutex<SliceState>>,
    ) -> bool {
        let mut guard = shared
            .inner
            .lock()
            .instrument(tracing::trace_span!(
                "commit_chunk.move_front_to_recently_committed"
            ))
            .await;

        if let Some(chunk) = guard.chunks.get_mut(&chunk_id) {
            let is_front = chunk
                .slices
                .front()
                .is_some_and(|front| Arc::ptr_eq(front, expected));
            if is_front && let Some(slice) = chunk.slices.pop_front() {
                Self::account_recent_pending_if_needed(shared, &slice);
                Self::keep_recently_committed_if_overlay_needed(
                    slice,
                    &mut chunk.recently_committed,
                );
            }
        }

        if guard.flush_waiting > 0 {
            shared.flush_notify.notify_waiters();
        }

        let empty = guard
            .chunks
            .get(&chunk_id)
            .map(|c| c.slices.is_empty() && c.recently_committed.is_empty())
            .unwrap_or(true);
        if empty {
            guard.chunks.remove(&chunk_id);
            if !guard.has_chunks() && guard.flush_waiting > 0 {
                shared.flush_notify.notify_waiters();
            }
        }
        empty
    }

    async fn try_commit_before_upload_front(
        shared: &Arc<Shared<B, M>>,
        slice: &Arc<ParkingMutex<SliceState>>,
    ) -> bool {
        if !matches!(
            shared.config.writeback_mode,
            WriteBackMode::CommitBeforeUpload
        ) {
            return false;
        }

        let handle = SliceHandle { slice, shared };
        let runtime = handle.runtime_snapshot();
        if !runtime.frozen || runtime.upload_done() {
            return false;
        }

        let Some(desc) = handle.desc_for_commit() else {
            return false;
        };

        let slice_epoch = slice.lock().frozen_epoch;
        if slice_epoch != 0 && shared.inode.data_epoch() != slice_epoch {
            tracing::warn!(
                ino = shared.inode.ino(),
                slice_epoch,
                current_epoch = shared.inode.data_epoch(),
                "skipping stale commit-before-upload after inode epoch change"
            );
            handle.mark_committed();
            return true;
        }

        if let Err(err) = Self::seal_writeback_record_if_ready(shared, slice).await {
            warn!(
                chunk_id = desc.chunk_id,
                slice_id = desc.slice_id,
                error = ?err,
                "writeback record seal failed before commit-before-upload"
            );
            return false;
        }

        let stage_ready = {
            let s = slice.lock();
            shared.write_back.is_none()
                || !shared.config.writeback_require_stage_before_commit
                || s.writeback_fully_persisted()
        };
        if !stage_ready {
            return false;
        }

        let (claimed, commit_before_stage) = {
            let mut s = slice.lock();
            if s.meta_write_started {
                (false, false)
            } else {
                s.meta_write_started = true;
                (
                    true,
                    shared.write_back.is_some() && !s.writeback_fully_persisted(),
                )
            }
        };

        if !claimed {
            return false;
        }
        if commit_before_stage {
            shared.recent_pending_upload.record_commit_before_stage();
        }

        let (ino, chunk_index) = extract_ino_and_chunk_index(desc.chunk_id);
        let file_offset = chunk_index * shared.config.layout.chunk_size + desc.offset;
        let new_size = file_offset + desc.length;

        let result = shared
            .backend
            .meta()
            .write(ino, desc.chunk_id, desc, new_size)
            .await;

        match result {
            Ok(()) => {
                shared.inode.set_committed_size(new_size);
                shared
                    .inode
                    .add_estimated_allocated_bytes(desc.length.as_usize() as u64);
                let _ = shared
                    .reader
                    .invalidate(ino as u64, file_offset, desc.length.as_usize())
                    .await;
                handle.mark_committed();
                true
            }
            Err(err) => {
                slice.lock().meta_write_started = false;
                warn!(
                    ino,
                    chunk_id = desc.chunk_id,
                    slice_id = desc.slice_id,
                    offset = desc.offset,
                    len = desc.length,
                    new_size,
                    error = ?err,
                    "commit-before-upload metadata write failed; falling back to upload-before-commit wait"
                );
                false
            }
        }
    }

    async fn try_commit_before_upload_if_front(
        shared: &Arc<Shared<B, M>>,
        slice: &Arc<ParkingMutex<SliceState>>,
    ) -> bool {
        let chunk_id = slice.lock().chunk_id;
        let is_front = {
            let guard = shared.inner.lock().await;
            guard
                .chunks
                .get(&chunk_id)
                .and_then(|chunk| chunk.slices.front())
                .is_some_and(|front| Arc::ptr_eq(front, slice))
        };

        if !is_front {
            return false;
        }

        Self::try_commit_before_upload_front(shared, slice).await
    }

    /// The background thread for committing a chunk.
    /// It waits for Uploaded slices, appends metadata, and marks them Committed.
    /// Each chunk will have a unique committing thread.
    #[tracing::instrument(
        name = "FileWriter.commit_chunk",
        level = "trace",
        skip(shared),
        fields(chunk_id)
    )]
    async fn commit_chunk(shared: Arc<Shared<B, M>>, chunk_id: u64) {
        let mut commit_failures = 0u32;
        loop {
            let slice = {
                let guard = shared.inner.lock().await;
                let Some(chunk) = guard.chunks.get(&chunk_id) else {
                    return;
                };

                // Just flush one slice in each check.
                chunk.slices.front().cloned()
            };

            let Some(slice) = slice else {
                let mut guard = shared.inner.lock().await;
                // Only remove the chunk if it has no recently_committed slices
                // that overlay_dirty still needs to see.
                let keep = guard
                    .chunks
                    .get(&chunk_id)
                    .map(|c| !c.recently_committed.is_empty())
                    .unwrap_or(false);
                if !keep {
                    guard.chunks.remove(&chunk_id);
                } else if let Some(chunk) = guard.chunks.get_mut(&chunk_id) {
                    // recently_committed keeps the chunk alive but slices is
                    // empty.  A new cached write can race in and add a slice
                    // while commit_chunk is about to return.  Re-check under
                    // the lock: if new slices appeared, keep processing them.
                    if !chunk.slices.is_empty() {
                        drop(guard);
                        continue;
                    }
                    // No new slices yet — reset commit_started so the next
                    // write will spawn a fresh commit_chunk task.
                    chunk.commit_started = false;
                }

                if !guard.has_chunks() && guard.flush_waiting > 0 {
                    shared.flush_notify.notify_waiters();
                }

                return;
            };

            Self::split_uploaded_prefix_tail(&shared, chunk_id, &slice).await;

            // Get a snapshot of the current slice to check its status.
            // The `notification` in `Notify` is not queued (but the waiters are), this may result in a lost wake-up.
            // So it is needed to wait for an extra cycle to check timeout (COMMIT_WAIT_SLICE).
            let runtime = SliceHandle {
                slice: &slice,
                shared: &shared,
            }
            .runtime_snapshot();

            if matches!(runtime.status, SliceStatus::Failed) {
                warn!(
                    chunk_id,
                    error = ?runtime.err,
                    "commit_chunk dropping failed slice after retry budget exhausted"
                );
                if Self::pop_front_slice(&shared, chunk_id).await {
                    return;
                }
                continue;
            }

            if !runtime.upload_done() {
                let handle = SliceHandle {
                    slice: &slice,
                    shared: &shared,
                };
                if runtime.frozen && Self::try_commit_before_upload_front(&shared, &slice).await {
                    commit_failures = 0;
                    if handle.can_continue_upload() {
                        Self::spawn_flush_slice(shared.clone(), slice.clone());
                    }
                    Self::move_front_slice_to_recently_committed(&shared, chunk_id, &slice).await;
                    continue;
                }

                let wait_start = Instant::now();
                let wait_result = timeout(COMMIT_WAIT_SLICE, runtime.notify.notified())
                    .instrument(tracing::trace_span!("commit_chunk.wait_upload"))
                    .await;
                if runtime.frozen {
                    shared.recent_pending_upload.record_commit_wait_upload(
                        wait_start.elapsed(),
                        runtime.freeze_reason,
                        runtime.write_origin,
                    );
                }
                if wait_result.is_ok() {
                    if Self::try_commit_before_upload_front(&shared, &slice).await {
                        commit_failures = 0;
                        if handle.can_continue_upload() {
                            Self::spawn_flush_slice(shared.clone(), slice.clone());
                        }
                        Self::move_front_slice_to_recently_committed(&shared, chunk_id, &slice)
                            .await;
                    }
                    continue;
                }

                // A frozen slice must eventually have an upload task.  Flush and
                // auto-flush normally spawn it, but a race can leave commit
                // waiting on a Readonly slice with no uploader.  Re-kick it here
                // so FUSE flush/truncate cannot wait forever on commit progress.
                if runtime.frozen {
                    if Self::try_commit_before_upload_front(&shared, &slice).await {
                        commit_failures = 0;
                        if handle.can_continue_upload() {
                            Self::spawn_flush_slice(shared.clone(), slice.clone());
                        }
                        Self::move_front_slice_to_recently_committed(&shared, chunk_id, &slice)
                            .await;
                        continue;
                    }

                    if handle.can_continue_upload() {
                        Self::spawn_flush_slice(shared.clone(), slice.clone());
                    }

                    // If the slice has been waiting too long for upload, give up
                    // to prevent indefinite hangs from stalled S3 connections.
                    if runtime.started.elapsed() > COMMIT_UPLOAD_MAX_WAIT {
                        warn!(
                            chunk_id,
                            age_secs = runtime.started.elapsed().as_secs(),
                            "commit_chunk: upload stalled too long, marking slice failed"
                        );
                        handle.mark_failed(anyhow::anyhow!(
                            "upload stalled for {:?}, giving up",
                            runtime.started.elapsed()
                        ));
                        if Self::pop_front_slice(&shared, chunk_id).await {
                            return;
                        }
                    }
                    continue;
                }

                // If the slice is too old, it will be frozen and flushed.
                if runtime.started.elapsed() > FLUSH_DURATION * 2 {
                    let _span = tracing::trace_span!("commit_chunk.freeze").entered();
                    let froze = handle.freeze_with_reason(SliceFreezeReason::CommitAgeSafety);

                    if froze {
                        let _spawn_span =
                            tracing::trace_span!("commit_chunk.spawn_flush").entered();
                        Self::spawn_flush_slice(shared.clone(), slice.clone());
                    }
                }
                continue;
            }

            let mut should_pop = false;

            if runtime.can_commit() && runtime.err.is_none() {
                // Epoch check: if a truncate/setattr happened after this slice
                // was frozen, the commit is stale and must be skipped.
                let slice_epoch = slice.lock().frozen_epoch;
                if slice_epoch != 0 && shared.inode.data_epoch() != slice_epoch {
                    tracing::warn!(
                        ino = shared.inode.ino(),
                        slice_epoch,
                        current_epoch = shared.inode.data_epoch(),
                        "skipping stale commit after truncate"
                    );
                    SliceHandle {
                        slice: &slice,
                        shared: &shared,
                    }
                    .mark_committed();
                    should_pop = true;
                } else {
                    // Claim the right to write metadata.  try_commit (from
                    // the upload task) may have already claimed it.
                    let claimed = {
                        let mut s = slice.lock();
                        if s.meta_write_started {
                            false
                        } else {
                            s.meta_write_started = true;
                            true
                        }
                    };

                    if !claimed {
                        // try_commit is handling this slice; wait for it to
                        // mark Committed so we can pop it on the next pass.
                        let notify = slice.lock().notify.clone();
                        let _ = timeout(COMMIT_WAIT_SLICE, notify.notified()).await;
                        continue;
                    }

                    let desc = SliceHandle {
                        slice: &slice,
                        shared: &shared,
                    }
                    .desc_for_commit();

                    if let Some(desc) = desc {
                        let (ino, chunk_index) = extract_ino_and_chunk_index(desc.chunk_id);
                        let file_offset =
                            chunk_index * shared.config.layout.chunk_size + desc.offset;
                        let new_size = file_offset + desc.length;

                        let result = shared
                            .backend
                            .meta()
                            .write(ino, desc.chunk_id, desc, new_size)
                            .instrument(tracing::trace_span!(
                                "commit_chunk.meta_write",
                                ino,
                                chunk_id = desc.chunk_id,
                                slice_id = desc.slice_id,
                                offset = desc.offset,
                                len = desc.length,
                                new_size
                            ))
                            .await;

                        if let Err(err) = result {
                            let retryable = should_retry_meta_write(&err);
                            if retryable {
                                commit_failures = commit_failures.saturating_add(1);
                            }

                            if !retryable || commit_failures >= COMMIT_META_MAX_RETRIES {
                                let message = if retryable {
                                    format!(
                                        "metadata commit failed after {commit_failures} attempts for ino {ino}, chunk {}, slice {}: {err}",
                                        desc.chunk_id, desc.slice_id
                                    )
                                } else {
                                    format!(
                                        "metadata commit failed with non-retryable error for ino {ino}, chunk {}, slice {}: {err}",
                                        desc.chunk_id, desc.slice_id
                                    )
                                };
                                warn!(
                                    ino,
                                    chunk_id = desc.chunk_id,
                                    slice_id = desc.slice_id,
                                    offset = desc.offset,
                                    len = desc.length,
                                    new_size,
                                    retry_failures = commit_failures,
                                    retryable,
                                    error = ?err,
                                    "commit_chunk meta write failed, giving up"
                                );
                                SliceHandle {
                                    slice: &slice,
                                    shared: &shared,
                                }
                                .mark_failed(anyhow::anyhow!(message));

                                // Clean up local SSD dirty record so a future
                                // recovery scan does not re-upload a slice that
                                // will fail the same metadata commit again.
                                if let Some(wb) = &shared.write_back {
                                    let key = crate::vfs::cache::keys::DirtySliceKey {
                                        ino,
                                        chunk_id: desc.chunk_id,
                                        local_seq: desc.slice_id,
                                        epoch: 0,
                                    };
                                    let _ = wb.remove(&key).await;
                                }

                                should_pop = true;
                            } else {
                                let backoff = commit_retry_backoff(commit_failures);
                                let reason = match &err {
                                    MetaError::ContinueRetry(r) => r.to_string(),
                                    _ => "backend_error".to_string(),
                                };
                                warn!(
                                    ino,
                                    chunk_id = desc.chunk_id,
                                    slice_id = desc.slice_id,
                                    offset = desc.offset,
                                    len = desc.length,
                                    new_size,
                                    retry_failures = commit_failures,
                                    retry_backoff_ms = backoff.as_millis() as u64,
                                    %reason,
                                    error = ?err,
                                    "commit_chunk meta write failed, retrying"
                                );
                            }
                        } else {
                            commit_failures = 0;
                            shared.inode.set_committed_size(new_size);

                            // Track committed bytes on the inode for accurate st_blocks.
                            shared
                                .inode
                                .add_estimated_allocated_bytes(desc.length.as_usize() as u64);

                            // Clean up local SSD dirty copy now that data is committed.
                            if let Some(wb) = &shared.write_back {
                                let key = crate::vfs::cache::keys::DirtySliceKey {
                                    ino,
                                    chunk_id: desc.chunk_id,
                                    local_seq: desc.slice_id,
                                    epoch: 0,
                                };
                                let _ = wb.remove(&key).await;
                            }

                            // Invalidate reader cache BEFORE marking committed.
                            // This ensures that when the flush loop observes
                            // Committed, the reader already serves fresh data.
                            let _ = shared
                                .reader
                                .invalidate(ino as u64, file_offset, desc.length.as_usize())
                                .instrument(tracing::trace_span!(
                                    "commit_chunk.invalidate",
                                    ino,
                                    offset = file_offset,
                                    len = desc.length
                                ))
                                .await;

                            SliceHandle {
                                slice: &slice,
                                shared: &shared,
                            }
                            .mark_committed();

                            should_pop = true;
                        }
                    } else {
                        should_pop = true;
                    }
                } // end epoch-ok else block
            } else if matches!(runtime.status, SliceStatus::Committed) {
                // Slice was already committed by try_commit in the upload task.
                // We must still invalidate the reader cache for this range so
                // that reads after flush (where overlay_dirty is skipped because
                // has_pending() returns false) fetch fresh data from S3.
                let desc = SliceHandle {
                    slice: &slice,
                    shared: &shared,
                }
                .desc_for_commit();
                if let Some(desc) = desc {
                    let (ino_val, chunk_index) = extract_ino_and_chunk_index(desc.chunk_id);
                    let file_offset = chunk_index * shared.config.layout.chunk_size + desc.offset;
                    let _ = shared
                        .reader
                        .invalidate(ino_val as u64, file_offset, desc.length.as_usize())
                        .await;
                }
                should_pop = true;
            }

            if !should_pop {
                let backoff = commit_retry_backoff(commit_failures.max(1));
                tracing::trace!(
                    status = ?runtime.status,
                    err = ?runtime.err,
                    backoff_ms = backoff.as_millis() as u64,
                    "commit_chunk retrying"
                );
                let wait_start = Instant::now();
                tokio::time::sleep(backoff)
                    .instrument(tracing::trace_span!("commit_chunk.wait_retry"))
                    .await;
                shared
                    .recent_pending_upload
                    .record_commit_wait_retry(wait_start.elapsed());
                continue;
            }

            // A completed front slice must not leak retry history to the next one.
            commit_failures = 0;

            // Move committed slices to recently_committed so overlay_dirty can
            // still serve their data during the grace period.  Failed or empty
            // slices are discarded immediately.
            let committed = matches!(runtime.status, SliceStatus::Committed);
            if committed {
                // Move from slices to recently_committed.
                let mut guard = shared
                    .inner
                    .lock()
                    .instrument(tracing::trace_span!(
                        "commit_chunk.move_to_recently_committed"
                    ))
                    .await;
                if let Some(chunk) = guard.chunks.get_mut(&chunk_id)
                    && let Some(s) = chunk.slices.pop_front()
                {
                    Self::account_recent_pending_if_needed(&shared, &s);
                    Self::keep_recently_committed_if_overlay_needed(
                        s,
                        &mut chunk.recently_committed,
                    );
                }
                if guard.flush_waiting > 0 {
                    shared.flush_notify.notify_waiters();
                }
            } else if Self::pop_front_slice(&shared, chunk_id).await {
                return;
            }
        }
    }

    /// The automatic flush loop: periodically freezes older/idle slices to reduce memory
    /// usage and ensure progress. It does not commit metadata directly.
    /// Use `Weak` to stop it when the `FileWriter` was dropped.
    async fn auto_flush(shared: Weak<Shared<B, M>>) {
        let idle = Duration::from_secs(1);
        let mut tick: u64 = 0;
        let cached_sub_block_idle_grace = cached_sub_block_idle_grace();
        let cached_sub_block_too_many_min_age = cached_sub_block_too_many_min_age();

        loop {
            let Some(shared) = shared.upgrade() else {
                return;
            };

            // Fast path: skip lock acquisition when no unflushed data exists.
            let gen_val = shared.write_gen.load(Ordering::Acquire);
            let flushed = shared.last_flushed_gen.load(Ordering::Acquire);
            if gen_val == flushed && !tick.is_multiple_of(100) {
                tick += 1;
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }

            let mut to_flush = Vec::new();
            {
                let guard = shared.inner.lock().await;
                let now = Instant::now();
                let mut total_slices = 0usize;
                let mut chunk_slices = Vec::new();

                for chunk in guard.chunks.values() {
                    total_slices += chunk.slices.len();
                    chunk_slices.push(chunk.slices.iter().cloned().collect::<Vec<_>>());
                }
                drop(guard);

                // if there are too many slices, it should flush "a few more" to reduce memory usage.
                let too_many = total_slices > MAX_SLICES_THRESHOLD;
                let too_many_excess = total_slices.saturating_sub(MAX_SLICES_THRESHOLD);
                let block_sized_backlog = if too_many {
                    chunk_slices
                        .iter()
                        .flat_map(|slices| slices.iter())
                        .filter(|slice| {
                            let guard = slice.lock();
                            guard.data.len() >= guard.data.block_size() as u64
                        })
                        .count()
                } else {
                    0
                };
                let too_many_has_block_sized_relief = block_sized_backlog >= too_many_excess.max(1);
                let force_pressure_flush = shared
                    .memory_budget
                    .as_ref()
                    .is_some_and(|budget| budget.should_force_flush());

                // Size-based proactive flush: when buffer usage exceeds 70% of
                // soft_limit, freeze oldest writable slices to start draining
                // before backpressure kicks in.  This keeps the buffer from
                // spiking past the soft limit during sustained sequential writes.
                let soft_limit = shared.config.buffer_size;
                let buffer_high = soft_limit > 0 && {
                    let usage = shared.buffer_usage.load(Ordering::Relaxed);
                    usage > soft_limit * 7 / 10
                };

                // Randomly select a half of chunk to do extra flush to avoid jitter.
                let pick_bit = (rand::rng().next_u64() & 1) as usize;

                for (chunk_idx, slices) in chunk_slices.iter().enumerate() {
                    let half = slices.len() / 2;

                    for (idx, slice) in slices.iter().enumerate() {
                        let handle = SliceHandle {
                            slice,
                            shared: &shared,
                        };

                        let (age, idle_time, data_len, writeable, cached_small_tail) = handle
                            .with_ref(|s| {
                                let data_len = s.data.len();
                                let block_size = s.data.block_size() as u64;
                                let pending_end =
                                    (s.dispatched_end as u64 * block_size).max(s.uploaded);
                                let unuploaded_tail = data_len.saturating_sub(pending_end);
                                let cached_small_tail =
                                    matches!(s.write_origin_kind(), WriteOriginKind::CachedOnly)
                                        && unuploaded_tail > 0
                                        && unuploaded_tail < block_size;
                                (
                                    now.duration_since(s.started),
                                    now.duration_since(s.last_mod),
                                    data_len,
                                    matches!(s.state, SliceStatus::Writable),
                                    cached_small_tail,
                                )
                            });

                        if !writeable || data_len == 0 {
                            continue;
                        }

                        // Freeze Writable slices that have existed long enough
                        // for a background upload cycle to be worthwhile.
                        // The short AUTO_FLUSH_MAX_AGE threshold lets the
                        // background path pre-empt foreground fsync: by the
                        // time fsync calls flush(), auto_flush has usually
                        // already frozen the slice and kicked off the upload,
                        // so flush only waits for in-flight work to land.
                        let auto_flush_max = shared.config.auto_flush_max_age;
                        let cached_tail_in_grace =
                            cached_small_tail && age <= cached_sub_block_idle_grace;
                        let mut trigger = if age > auto_flush_max && !cached_small_tail {
                            Some(AutoFreezeTrigger::Age)
                        } else if idle_time > idle && age > idle && !cached_tail_in_grace {
                            Some(AutoFreezeTrigger::Idle)
                        } else if age > FLUSH_DURATION && !cached_tail_in_grace {
                            Some(AutoFreezeTrigger::FlushDuration)
                        } else {
                            None
                        };
                        if trigger.is_none()
                            && force_pressure_flush
                            && idle_time >= Duration::from_millis(10)
                        {
                            trigger = Some(AutoFreezeTrigger::Pressure);
                        }
                        let cached_too_many_too_young =
                            cached_small_tail && age <= cached_sub_block_too_many_min_age;
                        let cached_too_many_has_block_relief =
                            cached_small_tail && too_many_has_block_sized_relief;
                        if trigger.is_none()
                            && too_many
                            && !cached_too_many_too_young
                            && !cached_too_many_has_block_relief
                        {
                            // idx <= half represents older slices.
                            if chunk_idx % 2 == pick_bit && idx <= half {
                                trigger = Some(AutoFreezeTrigger::TooMany);
                            }
                        }
                        // Proactive size-based flush: freeze slices with enough
                        // data when the buffer is filling up, even if they haven't
                        // reached auto_flush_max_age.
                        if trigger.is_none()
                            && buffer_high
                            && data_len >= shared.config.freeze_min_bytes
                            && idle_time >= Duration::from_millis(5)
                        {
                            trigger = Some(AutoFreezeTrigger::BufferHigh);
                        }

                        if let Some(trigger) = trigger {
                            if !handle.freeze_auto_with_trigger(trigger) {
                                continue;
                            }
                            tracing::debug!(
                                age_ms = age.as_millis(),
                                idle_ms = idle_time.as_millis(),
                                "auto_flush: freezing slice"
                            );
                            to_flush.push(slice.clone());
                        }
                    }
                }
            }

            for slice in to_flush {
                Self::spawn_flush_slice(shared.clone(), slice);
            }

            // Periodically drain recently_committed slices that have been kept
            // long enough for overlay_dirty to consume them.
            if tick.is_multiple_of(100) {
                let mut guard = shared.inner.lock().await;
                let mut emptied = Vec::new();
                let mut stale_uploads = Vec::new();
                let keep_writeback_overlay = matches!(
                    shared.config.writeback_mode,
                    WriteBackMode::CommitBeforeUpload
                );
                for (cid, chunk) in guard.chunks.iter_mut() {
                    // Keep recently-committed slices for ~2 s.
                    chunk.recently_committed.retain(|s| {
                        let state = s.lock();
                        let pending_writeback_upload = keep_writeback_overlay
                            && matches!(state.state, SliceStatus::Committed | SliceStatus::Failed)
                            && !state.upload_complete();
                        let age = state.started.elapsed();
                        if pending_writeback_upload
                            && matches!(state.state, SliceStatus::Committed)
                            && age > COMMIT_UPLOAD_MAX_WAIT
                        {
                            stale_uploads.push((*cid, s.clone(), age));
                        }

                        age < Duration::from_secs(2) || pending_writeback_upload
                    });
                    if chunk.slices.is_empty() && chunk.recently_committed.is_empty() {
                        emptied.push(*cid);
                    }
                }
                for cid in emptied {
                    guard.chunks.remove(&cid);
                }
                if !guard.has_chunks() && guard.flush_waiting > 0 {
                    shared.flush_notify.notify_waiters();
                }
                drop(guard);

                for (chunk_id, slice, age) in stale_uploads {
                    warn!(
                        chunk_id,
                        age_secs = age.as_secs(),
                        "auto_flush: background writeback upload stalled too long, marking slice failed"
                    );
                    SliceHandle {
                        slice: &slice,
                        shared: &shared,
                    }
                    .mark_failed(anyhow::anyhow!("upload stalled for {age:?}, giving up"));
                }
            }

            // Heartbeat every ~30s so we can see auto_flush is alive.
            // The tick counter is only for diagnostics and wraps harmlessly.
            tick += 1;
            if tick.is_multiple_of(3000) {
                tracing::info!(iteration = tick, "auto_flush: alive");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

pub(crate) struct DataWriter<B, M> {
    config: Arc<WriteConfig>,
    backend: Arc<Backend<B, M>>,
    reader: Arc<DataReader<B, M>>,
    files: DashMap<u64, Arc<FileWriter<B, M>>>,
    buffer_usage: Arc<AtomicU64>,
    write_back: Option<Arc<crate::vfs::cache::write_back::FsWriteBackCache>>,
    memory_budget: Option<MemoryBudget>,
    recent_pending_upload: Arc<RecentPendingUploadState>,
}

#[derive(Default, Debug, Clone, Copy)]
pub(crate) struct WritebackDirtyBreakdown {
    pub live_bytes: u64,
    pub live_slices: u64,
    pub live_normal_only_bytes: u64,
    pub live_normal_only_slices: u64,
    pub live_cached_only_bytes: u64,
    pub live_cached_only_slices: u64,
    pub live_mixed_origin_bytes: u64,
    pub live_mixed_origin_slices: u64,
    pub live_unknown_origin_bytes: u64,
    pub live_unknown_origin_slices: u64,
    pub recently_committed_pending_upload_bytes: u64,
    pub recently_committed_pending_upload_slices: u64,
    pub recently_committed_uploaded_bytes: u64,
    pub recently_committed_uploaded_slices: u64,
    pub backpressure_soft_sleep_ops: u64,
    pub backpressure_soft_sleep_us: u64,
    pub backpressure_hard_wait_ops: u64,
    pub backpressure_hard_wait_us: u64,
    pub buffer_soft_sleep_ops: u64,
    pub buffer_soft_sleep_us: u64,
    pub buffer_moderate_sleep_ops: u64,
    pub buffer_moderate_sleep_us: u64,
    pub buffer_hard_sleep_ops: u64,
    pub buffer_hard_sleep_us: u64,
    pub stage_inflight_bytes: u64,
    pub remote_upload_inflight_bytes: u64,
    pub stage_ops: u64,
    pub stage_bytes: u64,
    pub stage_us: u64,
    pub stage_failures: u64,
    pub commit_before_stage_ops: u64,
    pub commit_wait_upload_ops: u64,
    pub commit_wait_upload_us: u64,
    pub commit_wait_upload_size_ops: u64,
    pub commit_wait_upload_size_us: u64,
    pub commit_wait_upload_max_unflushed_ops: u64,
    pub commit_wait_upload_max_unflushed_us: u64,
    pub commit_wait_upload_explicit_flush_ops: u64,
    pub commit_wait_upload_explicit_flush_us: u64,
    pub commit_wait_upload_auto_ops: u64,
    pub commit_wait_upload_auto_us: u64,
    pub commit_wait_upload_commit_age_ops: u64,
    pub commit_wait_upload_commit_age_us: u64,
    pub commit_wait_upload_unknown_reason_ops: u64,
    pub commit_wait_upload_unknown_reason_us: u64,
    pub commit_wait_upload_normal_only_ops: u64,
    pub commit_wait_upload_normal_only_us: u64,
    pub commit_wait_upload_cached_only_ops: u64,
    pub commit_wait_upload_cached_only_us: u64,
    pub commit_wait_upload_mixed_origin_ops: u64,
    pub commit_wait_upload_mixed_origin_us: u64,
    pub commit_wait_upload_unknown_origin_ops: u64,
    pub commit_wait_upload_unknown_origin_us: u64,
    pub commit_wait_retry_ops: u64,
    pub commit_wait_retry_us: u64,
    pub flush_wait_ops: u64,
    pub flush_wait_us: u64,
    pub flush_wait_slices: u64,
    pub flush_fragmentation_ops: u64,
    pub flush_fragmentation_slices: u64,
    pub flush_fragmentation_bytes: u64,
    pub flush_fragmentation_cached_sub_block_slices: u64,
    pub flush_fragmentation_cached_sub_block_bytes: u64,
    pub flush_fragmentation_full_block_slices: u64,
    pub flush_fragmentation_full_block_bytes: u64,
    pub slice_create_ops: u64,
    pub slice_reuse_ops: u64,
    pub slice_reject_older_unique_ops: u64,
    pub slice_reject_dispatched_prefix_ops: u64,
    pub freeze_size_ops: u64,
    pub freeze_size_bytes: u64,
    pub freeze_max_unflushed_ops: u64,
    pub freeze_max_unflushed_bytes: u64,
    pub freeze_explicit_flush_ops: u64,
    pub freeze_explicit_flush_bytes: u64,
    pub freeze_auto_ops: u64,
    pub freeze_auto_bytes: u64,
    pub freeze_commit_age_ops: u64,
    pub freeze_commit_age_bytes: u64,
    pub upload_batch_ops: u64,
    pub upload_batch_bytes: u64,
    pub upload_batch_blocks: u64,
    pub upload_batch_single_block_ops: u64,
    pub upload_batch_multi_block_ops: u64,
    pub upload_partial_tail_ops: u64,
    pub upload_partial_tail_size_ops: u64,
    pub upload_partial_tail_max_unflushed_ops: u64,
    pub upload_partial_tail_explicit_flush_ops: u64,
    pub upload_partial_tail_auto_ops: u64,
    pub upload_partial_tail_normal_only_ops: u64,
    pub upload_partial_tail_cached_only_ops: u64,
    pub upload_partial_tail_mixed_origin_ops: u64,
    pub upload_partial_tail_unknown_origin_ops: u64,
    pub upload_partial_tail_auto_age_ops: u64,
    pub upload_partial_tail_auto_idle_ops: u64,
    pub upload_partial_tail_auto_pressure_ops: u64,
    pub upload_partial_tail_auto_too_many_ops: u64,
    pub upload_partial_tail_auto_buffer_high_ops: u64,
    pub upload_partial_tail_auto_flush_duration_ops: u64,
    pub upload_partial_tail_auto_unknown_ops: u64,
    pub upload_partial_tail_auto_normal_only_ops: u64,
    pub upload_partial_tail_auto_cached_only_ops: u64,
    pub upload_partial_tail_auto_mixed_origin_ops: u64,
    pub upload_partial_tail_auto_unknown_origin_ops: u64,
    pub upload_partial_tail_commit_age_ops: u64,
}

impl<B, M> DataWriter<B, M>
where
    B: BlockStore + Send + Sync + 'static,
    M: MetaLayer + Send + Sync + 'static,
{
    pub(crate) fn new(
        config: Arc<WriteConfig>,
        backend: Arc<Backend<B, M>>,
        reader: Arc<DataReader<B, M>>,
        write_back: Option<Arc<crate::vfs::cache::write_back::FsWriteBackCache>>,
    ) -> Self {
        Self {
            config,
            backend,
            reader,
            files: DashMap::new(),
            buffer_usage: Arc::new(AtomicU64::new(0)),
            write_back,
            memory_budget: None,
            recent_pending_upload: Arc::new(RecentPendingUploadState::new()),
        }
    }

    pub(crate) fn with_memory_budget(mut self, memory_budget: MemoryBudget) -> Self {
        self.memory_budget = Some(memory_budget);
        self
    }

    pub(crate) fn ensure_file(&self, inode: Arc<Inode>) -> Arc<FileWriter<B, M>> {
        let writer = self
            .files
            .entry(inode.ino() as u64)
            .or_insert_with(|| {
                Arc::new(FileWriter::new_with_memory_budget(
                    inode.clone(),
                    self.config.clone(),
                    self.backend.clone(),
                    self.reader.clone(),
                    self.buffer_usage.clone(),
                    self.write_back.clone(),
                    self.memory_budget.clone(),
                    self.recent_pending_upload.clone(),
                ))
            })
            .clone();
        writer.mark_active();
        writer
    }

    pub(crate) fn recent_pending_upload_bytes(&self) -> u64 {
        self.recent_pending_upload.bytes.load(Ordering::Acquire)
    }

    pub(crate) async fn dirty_breakdown(&self) -> WritebackDirtyBreakdown {
        let writers: Vec<Arc<FileWriter<B, M>>> = self
            .files
            .iter()
            .map(|entry| entry.value().clone())
            .collect();
        let mut breakdown = WritebackDirtyBreakdown {
            backpressure_soft_sleep_ops: self
                .recent_pending_upload
                .soft_sleep_ops
                .load(Ordering::Relaxed),
            backpressure_soft_sleep_us: self
                .recent_pending_upload
                .soft_sleep_us
                .load(Ordering::Relaxed),
            backpressure_hard_wait_ops: self
                .recent_pending_upload
                .hard_wait_ops
                .load(Ordering::Relaxed),
            backpressure_hard_wait_us: self
                .recent_pending_upload
                .hard_wait_us
                .load(Ordering::Relaxed),
            buffer_soft_sleep_ops: self
                .recent_pending_upload
                .buffer_soft_sleep_ops
                .load(Ordering::Relaxed),
            buffer_soft_sleep_us: self
                .recent_pending_upload
                .buffer_soft_sleep_us
                .load(Ordering::Relaxed),
            buffer_moderate_sleep_ops: self
                .recent_pending_upload
                .buffer_moderate_sleep_ops
                .load(Ordering::Relaxed),
            buffer_moderate_sleep_us: self
                .recent_pending_upload
                .buffer_moderate_sleep_us
                .load(Ordering::Relaxed),
            buffer_hard_sleep_ops: self
                .recent_pending_upload
                .buffer_hard_sleep_ops
                .load(Ordering::Relaxed),
            buffer_hard_sleep_us: self
                .recent_pending_upload
                .buffer_hard_sleep_us
                .load(Ordering::Relaxed),
            stage_inflight_bytes: self
                .recent_pending_upload
                .stage_inflight_bytes
                .load(Ordering::Acquire),
            remote_upload_inflight_bytes: self
                .recent_pending_upload
                .remote_upload_inflight_bytes
                .load(Ordering::Acquire),
            stage_ops: self.recent_pending_upload.stage_ops.load(Ordering::Relaxed),
            stage_bytes: self
                .recent_pending_upload
                .stage_bytes
                .load(Ordering::Relaxed),
            stage_us: self.recent_pending_upload.stage_us.load(Ordering::Relaxed),
            stage_failures: self
                .recent_pending_upload
                .stage_failures
                .load(Ordering::Relaxed),
            commit_before_stage_ops: self
                .recent_pending_upload
                .commit_before_stage_ops
                .load(Ordering::Relaxed),
            commit_wait_upload_ops: self
                .recent_pending_upload
                .commit_wait_upload_ops
                .load(Ordering::Relaxed),
            commit_wait_upload_us: self
                .recent_pending_upload
                .commit_wait_upload_us
                .load(Ordering::Relaxed),
            commit_wait_upload_size_ops: self
                .recent_pending_upload
                .commit_wait_upload_size_ops
                .load(Ordering::Relaxed),
            commit_wait_upload_size_us: self
                .recent_pending_upload
                .commit_wait_upload_size_us
                .load(Ordering::Relaxed),
            commit_wait_upload_max_unflushed_ops: self
                .recent_pending_upload
                .commit_wait_upload_max_unflushed_ops
                .load(Ordering::Relaxed),
            commit_wait_upload_max_unflushed_us: self
                .recent_pending_upload
                .commit_wait_upload_max_unflushed_us
                .load(Ordering::Relaxed),
            commit_wait_upload_explicit_flush_ops: self
                .recent_pending_upload
                .commit_wait_upload_explicit_flush_ops
                .load(Ordering::Relaxed),
            commit_wait_upload_explicit_flush_us: self
                .recent_pending_upload
                .commit_wait_upload_explicit_flush_us
                .load(Ordering::Relaxed),
            commit_wait_upload_auto_ops: self
                .recent_pending_upload
                .commit_wait_upload_auto_ops
                .load(Ordering::Relaxed),
            commit_wait_upload_auto_us: self
                .recent_pending_upload
                .commit_wait_upload_auto_us
                .load(Ordering::Relaxed),
            commit_wait_upload_commit_age_ops: self
                .recent_pending_upload
                .commit_wait_upload_commit_age_ops
                .load(Ordering::Relaxed),
            commit_wait_upload_commit_age_us: self
                .recent_pending_upload
                .commit_wait_upload_commit_age_us
                .load(Ordering::Relaxed),
            commit_wait_upload_unknown_reason_ops: self
                .recent_pending_upload
                .commit_wait_upload_unknown_reason_ops
                .load(Ordering::Relaxed),
            commit_wait_upload_unknown_reason_us: self
                .recent_pending_upload
                .commit_wait_upload_unknown_reason_us
                .load(Ordering::Relaxed),
            commit_wait_upload_normal_only_ops: self
                .recent_pending_upload
                .commit_wait_upload_normal_only_ops
                .load(Ordering::Relaxed),
            commit_wait_upload_normal_only_us: self
                .recent_pending_upload
                .commit_wait_upload_normal_only_us
                .load(Ordering::Relaxed),
            commit_wait_upload_cached_only_ops: self
                .recent_pending_upload
                .commit_wait_upload_cached_only_ops
                .load(Ordering::Relaxed),
            commit_wait_upload_cached_only_us: self
                .recent_pending_upload
                .commit_wait_upload_cached_only_us
                .load(Ordering::Relaxed),
            commit_wait_upload_mixed_origin_ops: self
                .recent_pending_upload
                .commit_wait_upload_mixed_origin_ops
                .load(Ordering::Relaxed),
            commit_wait_upload_mixed_origin_us: self
                .recent_pending_upload
                .commit_wait_upload_mixed_origin_us
                .load(Ordering::Relaxed),
            commit_wait_upload_unknown_origin_ops: self
                .recent_pending_upload
                .commit_wait_upload_unknown_origin_ops
                .load(Ordering::Relaxed),
            commit_wait_upload_unknown_origin_us: self
                .recent_pending_upload
                .commit_wait_upload_unknown_origin_us
                .load(Ordering::Relaxed),
            commit_wait_retry_ops: self
                .recent_pending_upload
                .commit_wait_retry_ops
                .load(Ordering::Relaxed),
            commit_wait_retry_us: self
                .recent_pending_upload
                .commit_wait_retry_us
                .load(Ordering::Relaxed),
            flush_wait_ops: self
                .recent_pending_upload
                .flush_wait_ops
                .load(Ordering::Relaxed),
            flush_wait_us: self
                .recent_pending_upload
                .flush_wait_us
                .load(Ordering::Relaxed),
            flush_wait_slices: self
                .recent_pending_upload
                .flush_wait_slices
                .load(Ordering::Relaxed),
            flush_fragmentation_ops: self
                .recent_pending_upload
                .flush_fragmentation_ops
                .load(Ordering::Relaxed),
            flush_fragmentation_slices: self
                .recent_pending_upload
                .flush_fragmentation_slices
                .load(Ordering::Relaxed),
            flush_fragmentation_bytes: self
                .recent_pending_upload
                .flush_fragmentation_bytes
                .load(Ordering::Relaxed),
            flush_fragmentation_cached_sub_block_slices: self
                .recent_pending_upload
                .flush_fragmentation_cached_sub_block_slices
                .load(Ordering::Relaxed),
            flush_fragmentation_cached_sub_block_bytes: self
                .recent_pending_upload
                .flush_fragmentation_cached_sub_block_bytes
                .load(Ordering::Relaxed),
            flush_fragmentation_full_block_slices: self
                .recent_pending_upload
                .flush_fragmentation_full_block_slices
                .load(Ordering::Relaxed),
            flush_fragmentation_full_block_bytes: self
                .recent_pending_upload
                .flush_fragmentation_full_block_bytes
                .load(Ordering::Relaxed),
            slice_create_ops: self
                .recent_pending_upload
                .slice_create_ops
                .load(Ordering::Relaxed),
            slice_reuse_ops: self
                .recent_pending_upload
                .slice_reuse_ops
                .load(Ordering::Relaxed),
            slice_reject_older_unique_ops: self
                .recent_pending_upload
                .slice_reject_older_unique_ops
                .load(Ordering::Relaxed),
            slice_reject_dispatched_prefix_ops: self
                .recent_pending_upload
                .slice_reject_dispatched_prefix_ops
                .load(Ordering::Relaxed),
            freeze_size_ops: self
                .recent_pending_upload
                .freeze_size_ops
                .load(Ordering::Relaxed),
            freeze_size_bytes: self
                .recent_pending_upload
                .freeze_size_bytes
                .load(Ordering::Relaxed),
            freeze_max_unflushed_ops: self
                .recent_pending_upload
                .freeze_max_unflushed_ops
                .load(Ordering::Relaxed),
            freeze_max_unflushed_bytes: self
                .recent_pending_upload
                .freeze_max_unflushed_bytes
                .load(Ordering::Relaxed),
            freeze_explicit_flush_ops: self
                .recent_pending_upload
                .freeze_explicit_flush_ops
                .load(Ordering::Relaxed),
            freeze_explicit_flush_bytes: self
                .recent_pending_upload
                .freeze_explicit_flush_bytes
                .load(Ordering::Relaxed),
            freeze_auto_ops: self
                .recent_pending_upload
                .freeze_auto_ops
                .load(Ordering::Relaxed),
            freeze_auto_bytes: self
                .recent_pending_upload
                .freeze_auto_bytes
                .load(Ordering::Relaxed),
            freeze_commit_age_ops: self
                .recent_pending_upload
                .freeze_commit_age_ops
                .load(Ordering::Relaxed),
            freeze_commit_age_bytes: self
                .recent_pending_upload
                .freeze_commit_age_bytes
                .load(Ordering::Relaxed),
            upload_batch_ops: self
                .recent_pending_upload
                .upload_batch_ops
                .load(Ordering::Relaxed),
            upload_batch_bytes: self
                .recent_pending_upload
                .upload_batch_bytes
                .load(Ordering::Relaxed),
            upload_batch_blocks: self
                .recent_pending_upload
                .upload_batch_blocks
                .load(Ordering::Relaxed),
            upload_batch_single_block_ops: self
                .recent_pending_upload
                .upload_batch_single_block_ops
                .load(Ordering::Relaxed),
            upload_batch_multi_block_ops: self
                .recent_pending_upload
                .upload_batch_multi_block_ops
                .load(Ordering::Relaxed),
            upload_partial_tail_ops: self
                .recent_pending_upload
                .upload_partial_tail_ops
                .load(Ordering::Relaxed),
            upload_partial_tail_size_ops: self
                .recent_pending_upload
                .upload_partial_tail_size_ops
                .load(Ordering::Relaxed),
            upload_partial_tail_max_unflushed_ops: self
                .recent_pending_upload
                .upload_partial_tail_max_unflushed_ops
                .load(Ordering::Relaxed),
            upload_partial_tail_explicit_flush_ops: self
                .recent_pending_upload
                .upload_partial_tail_explicit_flush_ops
                .load(Ordering::Relaxed),
            upload_partial_tail_auto_ops: self
                .recent_pending_upload
                .upload_partial_tail_auto_ops
                .load(Ordering::Relaxed),
            upload_partial_tail_normal_only_ops: self
                .recent_pending_upload
                .upload_partial_tail_normal_only_ops
                .load(Ordering::Relaxed),
            upload_partial_tail_cached_only_ops: self
                .recent_pending_upload
                .upload_partial_tail_cached_only_ops
                .load(Ordering::Relaxed),
            upload_partial_tail_mixed_origin_ops: self
                .recent_pending_upload
                .upload_partial_tail_mixed_origin_ops
                .load(Ordering::Relaxed),
            upload_partial_tail_unknown_origin_ops: self
                .recent_pending_upload
                .upload_partial_tail_unknown_origin_ops
                .load(Ordering::Relaxed),
            upload_partial_tail_auto_age_ops: self
                .recent_pending_upload
                .upload_partial_tail_auto_age_ops
                .load(Ordering::Relaxed),
            upload_partial_tail_auto_idle_ops: self
                .recent_pending_upload
                .upload_partial_tail_auto_idle_ops
                .load(Ordering::Relaxed),
            upload_partial_tail_auto_pressure_ops: self
                .recent_pending_upload
                .upload_partial_tail_auto_pressure_ops
                .load(Ordering::Relaxed),
            upload_partial_tail_auto_too_many_ops: self
                .recent_pending_upload
                .upload_partial_tail_auto_too_many_ops
                .load(Ordering::Relaxed),
            upload_partial_tail_auto_buffer_high_ops: self
                .recent_pending_upload
                .upload_partial_tail_auto_buffer_high_ops
                .load(Ordering::Relaxed),
            upload_partial_tail_auto_flush_duration_ops: self
                .recent_pending_upload
                .upload_partial_tail_auto_flush_duration_ops
                .load(Ordering::Relaxed),
            upload_partial_tail_auto_unknown_ops: self
                .recent_pending_upload
                .upload_partial_tail_auto_unknown_ops
                .load(Ordering::Relaxed),
            upload_partial_tail_auto_normal_only_ops: self
                .recent_pending_upload
                .upload_partial_tail_auto_normal_only_ops
                .load(Ordering::Relaxed),
            upload_partial_tail_auto_cached_only_ops: self
                .recent_pending_upload
                .upload_partial_tail_auto_cached_only_ops
                .load(Ordering::Relaxed),
            upload_partial_tail_auto_mixed_origin_ops: self
                .recent_pending_upload
                .upload_partial_tail_auto_mixed_origin_ops
                .load(Ordering::Relaxed),
            upload_partial_tail_auto_unknown_origin_ops: self
                .recent_pending_upload
                .upload_partial_tail_auto_unknown_origin_ops
                .load(Ordering::Relaxed),
            upload_partial_tail_commit_age_ops: self
                .recent_pending_upload
                .upload_partial_tail_commit_age_ops
                .load(Ordering::Relaxed),
            ..WritebackDirtyBreakdown::default()
        };

        for writer in writers {
            let guard = writer.shared.inner.lock().await;
            for chunk in guard.chunks.values() {
                for slice in &chunk.slices {
                    let state = slice.lock();
                    let bytes = state.data.alloc_bytes();
                    breakdown.live_slices = breakdown.live_slices.saturating_add(1);
                    breakdown.live_bytes = breakdown.live_bytes.saturating_add(bytes);
                    match state.write_origin_kind() {
                        WriteOriginKind::NormalOnly => {
                            breakdown.live_normal_only_slices =
                                breakdown.live_normal_only_slices.saturating_add(1);
                            breakdown.live_normal_only_bytes =
                                breakdown.live_normal_only_bytes.saturating_add(bytes);
                        }
                        WriteOriginKind::CachedOnly => {
                            breakdown.live_cached_only_slices =
                                breakdown.live_cached_only_slices.saturating_add(1);
                            breakdown.live_cached_only_bytes =
                                breakdown.live_cached_only_bytes.saturating_add(bytes);
                        }
                        WriteOriginKind::Mixed => {
                            breakdown.live_mixed_origin_slices =
                                breakdown.live_mixed_origin_slices.saturating_add(1);
                            breakdown.live_mixed_origin_bytes =
                                breakdown.live_mixed_origin_bytes.saturating_add(bytes);
                        }
                        WriteOriginKind::Unknown => {
                            breakdown.live_unknown_origin_slices =
                                breakdown.live_unknown_origin_slices.saturating_add(1);
                            breakdown.live_unknown_origin_bytes =
                                breakdown.live_unknown_origin_bytes.saturating_add(bytes);
                        }
                    }
                }
                for slice in &chunk.recently_committed {
                    let state = slice.lock();
                    let bytes = state.data.alloc_bytes();
                    if state.upload_complete() {
                        breakdown.recently_committed_uploaded_slices = breakdown
                            .recently_committed_uploaded_slices
                            .saturating_add(1);
                        breakdown.recently_committed_uploaded_bytes = breakdown
                            .recently_committed_uploaded_bytes
                            .saturating_add(bytes);
                    } else {
                        breakdown.recently_committed_pending_upload_slices = breakdown
                            .recently_committed_pending_upload_slices
                            .saturating_add(1);
                        breakdown.recently_committed_pending_upload_bytes = breakdown
                            .recently_committed_pending_upload_bytes
                            .saturating_add(bytes);
                    }
                }
            }
        }

        breakdown
    }

    pub(crate) fn start_flush_background(self: &Arc<Self>) {
        let flush_interval = self.config.flush_all_interval;
        let weak = Arc::downgrade(self);

        tokio::spawn(async move {
            let mut ticker = interval(flush_interval);
            loop {
                ticker.tick().await;
                let Some(writer) = weak.upgrade() else {
                    return;
                };
                writer.flush_once().await;
            }
        });
    }

    pub(crate) async fn flush_if_exists(&self, ino: u64) {
        let writer = self.files.get(&ino).map(|entry| entry.value().clone());
        if let Some(writer) = writer
            && writer.has_pending().await
        {
            let _ = writer.flush().await;
        }
    }

    pub(crate) async fn has_dirty_state(&self, ino: u64) -> bool {
        let writer = self.files.get(&ino).map(|entry| entry.value().clone());
        if let Some(writer) = writer {
            writer.has_pending().await || writer.has_overlay_state().await
        } else {
            false
        }
    }

    pub(crate) async fn overlay_dirty_if_exists(
        &self,
        ino: u64,
        offset: u64,
        buf: &mut [u8],
    ) -> anyhow::Result<()> {
        let writer = self.files.get(&ino).map(|entry| entry.value().clone());
        match writer {
            Some(ref writer) if writer.has_overlay_state().await => {
                writer.overlay_dirty(offset, buf).await?;
            }
            #[cfg(not(test))]
            None => {
                // SSD fallback: no in-memory writer exists for this inode.
                // Covers the crash recovery window where dirty data is on
                // SSD but hasn't been re-uploaded yet.
                if let Some(wb) = &self.write_back {
                    let layout = self.config.layout;
                    let spans = split_chunk_spans(layout, offset, buf.len());
                    for span in spans {
                        let cid = chunk_id_for(ino as i64, span.index)?;
                        let chunk_start = span.index * layout.chunk_size;
                        let dst_start = (chunk_start + span.offset - offset) as usize;
                        let dst_end = dst_start + span.len.as_usize();
                        let _ = wb
                            .overlay_dirty_range(
                                ino as i64,
                                cid,
                                span.offset,
                                &mut buf[dst_start..dst_end],
                            )
                            .await;
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }

    pub(crate) async fn read_dirty_if_fully_covered(
        &self,
        ino: u64,
        offset: u64,
        len: usize,
    ) -> anyhow::Result<Option<Vec<u8>>> {
        let writer = self.files.get(&ino).map(|entry| entry.value().clone());
        match writer {
            Some(ref writer) if writer.has_overlay_state().await => {
                writer.read_dirty_if_fully_covered(offset, len).await
            }
            _ => Ok(None),
        }
    }

    /// Like `flush_if_exists` but propagates errors.  Used in truncate paths
    /// where a failed flush means data would be silently lost.
    pub(crate) async fn flush_required(&self, ino: u64) -> anyhow::Result<bool> {
        let writer = self.files.get(&ino).map(|entry| entry.value().clone());
        if let Some(writer) = writer
            && writer.has_pending().await
        {
            let start = std::time::Instant::now();
            writer.flush().await?;
            let ms = start.elapsed().as_millis();
            if ms > 100 {
                tracing::info!(ino, elapsed_ms = ms, "flush_required: slow flush");
            }
            return Ok(true);
        }
        Ok(false)
    }

    pub(crate) async fn wait_committed_uploads_for_range(
        &self,
        ino: u64,
        offset: u64,
        len: usize,
    ) -> anyhow::Result<()> {
        let writer = self.files.get(&ino).map(|entry| entry.value().clone());
        if let Some(writer) = writer {
            writer
                .wait_committed_uploads_for_range(offset, len, FLUSH_DEADLINE)
                .await?;
        }
        Ok(())
    }

    /// Truncate/ftruncate runs on the kernel SETATTR path.  A 300s writeback
    /// wait looks like a stuck FUSE request, so use a short, explicit deadline
    /// and log the operation boundary for xfstests-style debugging.
    pub(crate) async fn flush_required_for_truncate(&self, ino: u64) -> anyhow::Result<()> {
        let writer = self.files.get(&ino).map(|entry| entry.value().clone());
        if let Some(writer) = writer
            && writer.has_pending().await
        {
            let deadline = truncate_flush_deadline();
            let start = Instant::now();
            tracing::debug!(
                ino,
                timeout_ms = deadline.as_millis() as u64,
                "truncate flush_required: start"
            );
            writer.flush_with_deadline(deadline).await.map_err(|err| {
                anyhow::anyhow!(
                    "truncate flush failed after {:?} for ino {ino}: {err}",
                    deadline
                )
            })?;
            tracing::debug!(
                ino,
                elapsed_ms = start.elapsed().as_millis() as u64,
                "truncate flush_required: completed"
            );
        }
        Ok(())
    }

    /// Flush for close: uses a shorter deadline because FUSE already called
    /// flush() before close() for write handles.  This only drains residual
    /// in-flight work that was already kicked off by the preceding flush.
    pub(crate) async fn flush_for_close(&self, ino: u64) -> anyhow::Result<bool> {
        let writer = self.files.get(&ino).map(|entry| entry.value().clone());
        if let Some(writer) = writer
            && writer.has_pending().await
        {
            writer.flush_with_deadline(CLOSE_FLUSH_DEADLINE).await?;
            return Ok(true);
        }
        Ok(false)
    }

    pub(crate) async fn clear(&self, ino: u64) {
        let writer = self.files.get(&ino).map(|entry| entry.value().clone());
        if let Some(writer) = writer {
            writer.clear().await;
        }
    }

    pub(crate) async fn discard(&self, ino: u64) {
        if let Some((_, removed)) = self.files.remove(&ino) {
            removed.shared.inode.bump_data_epoch();
            removed.clear().await;
        }
    }

    pub(crate) async fn release(&self, ino: u64) {
        let writer = self.files.get(&ino).map(|entry| entry.value().clone());
        if let Some(writer) = writer
            && (writer.has_pending().await || writer.has_overlay_state().await)
        {
            writer.mark_released();
            return;
        }

        if let Some((_, removed)) = self.files.remove(&ino) {
            removed.clear().await;
        }
    }

    #[cfg(test)]
    pub(crate) fn has_file(&self, ino: u64) -> bool {
        self.files.contains_key(&ino)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn flush_once(&self) {
        let writers: Vec<(u64, Arc<FileWriter<B, M>>)> = self
            .files
            .iter()
            .map(|entry| (*entry.key(), entry.value().clone()))
            .collect();

        for (ino, writer) in writers {
            let released = writer.shared.released.load(Ordering::Acquire);
            if released && writer.has_pending().await {
                let _ = writer.flush().await;
            }
            if writer.released_cleanup_ready().await
                && let Some((_, removed)) = self.files.remove(&ino)
            {
                removed.clear().await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunk::ChunkLayout;
    use crate::chunk::reader::DataFetcher;
    use crate::chunk::store::{BlockKey, BlockStore, InMemoryBlockStore};
    use crate::meta::MetaLayer;
    use crate::meta::client::{MetaClient, MetaClientOptions};
    use crate::meta::config::{CacheCapacity, CacheTtl};
    use crate::meta::factory::create_meta_store_from_url;
    use crate::meta::store::MetaStore;
    use crate::vfs::Inode;
    use crate::vfs::config::ReadConfig;
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use tokio::time::{sleep, timeout};

    fn test_config_with_writeback_and_auto_flush(
        layout: ChunkLayout,
        writeback_mode: WriteBackMode,
        auto_flush_max_age: Duration,
    ) -> Arc<WriteConfig> {
        Arc::new(
            WriteConfig::new(layout)
                .page_size(4 * 1024)
                .freeze_min_bytes(4096)
                .auto_flush_max_age(auto_flush_max_age)
                .writeback_mode(writeback_mode),
        )
    }

    fn test_config_with_writeback(
        layout: ChunkLayout,
        writeback_mode: WriteBackMode,
    ) -> Arc<WriteConfig> {
        test_config_with_writeback_and_auto_flush(layout, writeback_mode, Duration::from_millis(5))
    }

    fn test_config(layout: ChunkLayout) -> Arc<WriteConfig> {
        test_config_with_writeback(layout, WriteBackMode::UploadBeforeCommit)
    }

    fn test_config_with_prefix_split(layout: ChunkLayout) -> Arc<WriteConfig> {
        Arc::new(
            WriteConfig::new(layout)
                .page_size(4 * 1024)
                .freeze_min_bytes(4096)
                .auto_flush_max_age(Duration::from_millis(5))
                .writeback_mode(WriteBackMode::UploadBeforeCommit)
                .upload_before_commit_prefix_split(true),
        )
    }

    fn test_config_without_auto_flush(layout: ChunkLayout) -> Arc<WriteConfig> {
        // Keep live-slice assertions deterministic instead of racing auto_flush.
        test_config_with_writeback_and_auto_flush(
            layout,
            WriteBackMode::UploadBeforeCommit,
            Duration::from_secs(60),
        )
    }

    #[test]
    fn test_writeback_backpressure_decision_uses_soft_sleep_before_hard_wait() {
        let soft = 12 * 1024;
        let hard = 16 * 1024;

        assert!(matches!(
            decide_writeback_backpressure(soft - 512, 256, soft, hard),
            WritebackBackpressureDecision::Allow
        ));
        match decide_writeback_backpressure(soft, 512, soft, hard) {
            WritebackBackpressureDecision::SoftSleep(duration) => {
                assert!(duration >= WRITEBACK_SOFT_BACKPRESSURE_MIN_SLEEP);
                assert!(duration <= WRITEBACK_SOFT_BACKPRESSURE_MAX_SLEEP);
            }
            _ => panic!("expected soft sleep before hard wait"),
        }
        assert!(matches!(
            decide_writeback_backpressure(hard, 512, soft, hard),
            WritebackBackpressureDecision::Wait
        ));
    }

    #[test]
    fn test_writeback_backpressure_soft_sleep_reaches_max_for_large_ranges() {
        let soft = 1;
        let hard = u64::MAX;

        match decide_writeback_backpressure(hard - 1, 1, soft, hard) {
            WritebackBackpressureDecision::SoftSleep(duration) => {
                assert_eq!(duration, WRITEBACK_SOFT_BACKPRESSURE_MAX_SLEEP);
            }
            _ => panic!("expected max soft sleep at the hard boundary"),
        }
    }

    #[test]
    fn test_commit_before_upload_extends_write_buffer_wait_budget() {
        assert_eq!(
            write_buffer_max_wait(WriteBackMode::UploadBeforeCommit),
            WRITE_MAX_WAIT
        );
        assert!(
            write_buffer_max_wait(WriteBackMode::CommitBeforeUpload) > WRITE_MAX_WAIT,
            "commit-before-upload can legitimately wait on local stage plus remote upload"
        );
    }

    #[test]
    fn test_writeback_phase_metrics_track_stage_and_remote_upload() {
        let state = Arc::new(RecentPendingUploadState::new());

        let stage_start = state.record_stage_start(4096);
        std::thread::sleep(Duration::from_micros(10));
        state.record_stage_finish(stage_start, 4096, true);

        {
            let _remote_upload = state.track_remote_upload_inflight(8192);
            assert_eq!(
                state.remote_upload_inflight_bytes.load(Ordering::Acquire),
                8192
            );
        }

        state.record_commit_before_stage();
        state.record_commit_wait_upload(
            Duration::from_micros(21),
            Some(SliceFreezeReason::Auto),
            WriteOriginKind::CachedOnly,
        );
        state.record_commit_wait_retry(Duration::from_micros(34));
        state.record_flush_wait(Duration::from_micros(55), 3);
        state.record_upload_batch(4096, 1, false, None, None, WriteOriginKind::NormalOnly);
        state.record_upload_batch(8192, 2, false, None, None, WriteOriginKind::NormalOnly);

        assert_eq!(state.stage_inflight_bytes.load(Ordering::Acquire), 0);
        assert_eq!(
            state.remote_upload_inflight_bytes.load(Ordering::Acquire),
            0
        );
        assert_eq!(state.stage_ops.load(Ordering::Relaxed), 1);
        assert_eq!(state.stage_bytes.load(Ordering::Relaxed), 4096);
        assert!(state.stage_us.load(Ordering::Relaxed) > 0);
        assert_eq!(state.stage_failures.load(Ordering::Relaxed), 0);
        assert_eq!(state.commit_before_stage_ops.load(Ordering::Relaxed), 1);
        assert_eq!(state.commit_wait_upload_ops.load(Ordering::Relaxed), 1);
        assert_eq!(state.commit_wait_upload_us.load(Ordering::Relaxed), 21);
        assert_eq!(state.commit_wait_upload_auto_ops.load(Ordering::Relaxed), 1);
        assert_eq!(state.commit_wait_upload_auto_us.load(Ordering::Relaxed), 21);
        assert_eq!(
            state
                .commit_wait_upload_cached_only_ops
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            state
                .commit_wait_upload_cached_only_us
                .load(Ordering::Relaxed),
            21
        );
        assert_eq!(
            state.upload_batch_single_block_ops.load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            state.upload_batch_multi_block_ops.load(Ordering::Relaxed),
            1
        );
        assert_eq!(state.commit_wait_retry_ops.load(Ordering::Relaxed), 1);
        assert_eq!(state.commit_wait_retry_us.load(Ordering::Relaxed), 34);
        assert_eq!(state.flush_wait_ops.load(Ordering::Relaxed), 1);
        assert_eq!(state.flush_wait_us.load(Ordering::Relaxed), 55);
        assert_eq!(state.flush_wait_slices.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn test_flush_fragmentation_metrics_track_cached_sub_block_slices() {
        let state = RecentPendingUploadState::new();

        state.record_flush_fragmentation(3, 10 * 1024, 2, 2 * 1024, 1, 8 * 1024);

        assert_eq!(state.flush_fragmentation_ops.load(Ordering::Relaxed), 1);
        assert_eq!(state.flush_fragmentation_slices.load(Ordering::Relaxed), 3);
        assert_eq!(
            state.flush_fragmentation_bytes.load(Ordering::Relaxed),
            10 * 1024
        );
        assert_eq!(
            state
                .flush_fragmentation_cached_sub_block_slices
                .load(Ordering::Relaxed),
            2
        );
        assert_eq!(
            state
                .flush_fragmentation_cached_sub_block_bytes
                .load(Ordering::Relaxed),
            2 * 1024
        );
        assert_eq!(
            state
                .flush_fragmentation_full_block_slices
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            state
                .flush_fragmentation_full_block_bytes
                .load(Ordering::Relaxed),
            8 * 1024
        );
    }

    #[test]
    fn test_slice_writeback_stage_completion_requires_all_bytes() {
        let layout = ChunkLayout {
            chunk_size: 16 * 1024,
            block_size: 4 * 1024,
        };
        let mut slice = SliceState::new(
            1,
            0,
            test_config_with_writeback(layout, WriteBackMode::CommitBeforeUpload),
            Arc::new(AtomicU64::new(0)),
            None,
            0,
        );
        slice.data.append(&vec![1u8; 8192]).unwrap();

        slice.record_writeback_persisted_bytes(4096);
        assert!(
            !slice.writeback_fully_persisted(),
            "a single staged batch must not make the whole slice look durable"
        );

        slice.record_writeback_persisted_bytes(4096);
        assert!(slice.writeback_data_fully_persisted());
        assert!(
            !slice.writeback_fully_persisted(),
            "staged data is not durable enough for commit-before-upload until the record is sealed"
        );

        slice.writeback_record_sealed = true;
        assert!(slice.writeback_fully_persisted());
    }

    #[tokio::test]
    async fn test_upload_before_commit_skips_local_writeback_stage() {
        let layout = ChunkLayout {
            chunk_size: 8 * 1024,
            block_size: 4 * 1024,
        };
        let store = Arc::new(InMemoryBlockStore::new());
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta = meta_handle.layer();
        let backend = Arc::new(Backend::new(store, meta.clone()));
        let ino = meta
            .create_file(1, "upload_before_commit_no_stage.txt".to_string())
            .await
            .unwrap();
        let inode = Inode::new(ino, 0);
        let reader = Arc::new(DataReader::new(
            Arc::new(ReadConfig::new(layout)),
            backend.clone(),
        ));
        let temp = tempfile::tempdir().unwrap();
        let write_back = Arc::new(
            crate::vfs::cache::write_back::FsWriteBackCache::new_with_sync(
                temp.path().to_path_buf(),
                false,
            ),
        );
        let writer = Arc::new(DataWriter::new(
            test_config_with_writeback(layout, WriteBackMode::UploadBeforeCommit),
            backend,
            reader,
            Some(write_back.clone()),
        ));
        let file_writer = writer.ensure_file(inode);

        file_writer
            .write_at(0, &vec![5u8; layout.block_size as usize])
            .await
            .unwrap();
        timeout(Duration::from_secs(2), file_writer.flush())
            .await
            .expect("upload-before-commit flush should finish")
            .unwrap();

        let breakdown = writer.dirty_breakdown().await;
        assert_eq!(breakdown.stage_ops, 0);
        assert!(
            write_back.recover().await.unwrap().is_empty(),
            "upload-before-commit must not create recoverable local dirty records"
        );
    }

    fn blocks_len(data: &[(usize, Vec<Bytes>)]) -> usize {
        data.iter()
            .map(|(_, pages)| pages.iter().map(|b| b.len()).sum::<usize>())
            .sum()
    }

    struct BlockingStore {
        inner: InMemoryBlockStore,
        blocked: AtomicBool,
        notify: Notify,
    }

    impl BlockingStore {
        fn new(blocked: bool) -> Self {
            Self {
                inner: InMemoryBlockStore::new(),
                blocked: AtomicBool::new(blocked),
                notify: Notify::new(),
            }
        }

        fn unblock(&self) {
            self.blocked.store(false, Ordering::Release);
            self.notify.notify_waiters();
        }
    }

    async fn wait_for_recent_pending_upload_bytes<B, M>(
        writer: &Arc<DataWriter<B, M>>,
        expected: u64,
    ) where
        B: BlockStore + Send + Sync + 'static,
        M: MetaLayer + Send + Sync + 'static,
    {
        timeout(Duration::from_secs(2), async {
            loop {
                if writer.recent_pending_upload_bytes() == expected {
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "recent pending upload bytes did not reach {expected}; got {}",
                writer.recent_pending_upload_bytes()
            )
        });
    }

    #[async_trait]
    impl BlockStore for BlockingStore {
        async fn write_fresh_range(
            &self,
            key: BlockKey,
            offset: u64,
            data: &[u8],
        ) -> anyhow::Result<u64> {
            while self.blocked.load(Ordering::Acquire) {
                self.notify.notified().await;
            }
            self.inner.write_fresh_range(key, offset, data).await
        }

        async fn read_range(
            &self,
            key: BlockKey,
            offset: u64,
            buf: &mut [u8],
        ) -> anyhow::Result<()> {
            self.inner.read_range(key, offset, buf).await
        }

        async fn delete_range(&self, key: BlockKey, block_count: u64) -> anyhow::Result<()> {
            self.inner.delete_range(key, block_count).await
        }
    }

    struct FailingStore;

    #[async_trait]
    impl BlockStore for FailingStore {
        async fn write_fresh_range(
            &self,
            _key: BlockKey,
            _offset: u64,
            _data: &[u8],
        ) -> anyhow::Result<u64> {
            anyhow::bail!("injected write failure")
        }

        async fn read_range(
            &self,
            _key: BlockKey,
            _offset: u64,
            _buf: &mut [u8],
        ) -> anyhow::Result<()> {
            anyhow::bail!("injected read failure")
        }

        async fn delete_range(&self, _key: BlockKey, _block_count: u64) -> anyhow::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn test_idx_need_upload_writable_only_full_blocks() {
        let layout = ChunkLayout {
            chunk_size: 16 * 1024,
            block_size: 4 * 1024,
        };
        let mut slice = SliceState::new(
            1,
            0,
            test_config(layout),
            Arc::new(AtomicU64::new(0)),
            None,
            0,
        );
        let len = layout.block_size as usize + (layout.block_size as usize / 2);
        slice.data.append(&vec![1u8; len]).unwrap();

        let (start, end) = slice.idx_need_upload();
        assert_eq!((start, end), (0, 1));
    }

    #[test]
    fn test_idx_need_upload_writable_stops_before_sparse_gap() {
        let layout = ChunkLayout {
            chunk_size: 16 * 1024,
            block_size: 4 * 1024,
        };
        let mut slice = SliceState::new(
            1,
            0,
            test_config(layout),
            Arc::new(AtomicU64::new(0)),
            None,
            0,
        );
        let block = layout.block_size as usize;
        slice.data.append(&vec![1u8; block]).unwrap();
        slice
            .data
            .write(
                (block * 2) as u64,
                &vec![3u8; block],
                PageWriteAction::GapAppend,
            )
            .unwrap();

        assert_eq!(slice.data.len(), (block * 3) as u64);
        let (start, end) = slice.idx_need_upload();
        assert_eq!((start, end), (0, 1));

        slice
            .data
            .write_at(block as u64, &vec![2u8; block])
            .unwrap();
        let (start, end) = slice.idx_need_upload();
        assert_eq!((start, end), (0, 3));
    }

    #[test]
    fn test_idx_need_upload_readonly_includes_partial_block() {
        let layout = ChunkLayout {
            chunk_size: 16 * 1024,
            block_size: 4 * 1024,
        };
        let mut slice = SliceState::new(
            1,
            0,
            test_config(layout),
            Arc::new(AtomicU64::new(0)),
            None,
            0,
        );
        let len = layout.block_size as usize + (layout.block_size as usize / 2);
        let data = vec![2u8; len];
        slice.data.append(&data).unwrap();
        slice.data.freeze();
        slice.state = SliceStatus::Readonly;

        let (start, end) = slice.idx_need_upload();
        assert_eq!((start, end), (0, 2));

        let blocks = slice.data.collect_pages(start, end).unwrap();
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks_len(&blocks), data.len());
    }

    #[test]
    fn test_idx_need_upload_committed_writeback_includes_partial_block() {
        let layout = ChunkLayout {
            chunk_size: 16 * 1024,
            block_size: 4 * 1024,
        };
        let mut slice = SliceState::new(
            1,
            0,
            test_config_with_writeback(layout, WriteBackMode::CommitBeforeUpload),
            Arc::new(AtomicU64::new(0)),
            None,
            0,
        );
        let len = layout.block_size as usize + (layout.block_size as usize / 2);
        let data = vec![3u8; len];
        slice.data.append(&data).unwrap();
        slice.data.freeze();
        slice.state = SliceStatus::Committed;

        let (start, end) = slice.idx_need_upload();
        assert_eq!((start, end), (0, 2));

        let blocks = slice.data.collect_pages(start, end).unwrap();
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks_len(&blocks), data.len());
    }

    #[test]
    fn test_should_retry_meta_write_classifies_errors_correctly() {
        use crate::meta::store::RetryReason;

        // All ContinueRetry variants are retryable regardless of reason.
        assert!(should_retry_meta_write(&MetaError::ContinueRetry(
            RetryReason::VersionConflict
        )));
        assert!(should_retry_meta_write(&MetaError::ContinueRetry(
            RetryReason::CompactConflict
        )));
        assert!(should_retry_meta_write(&MetaError::ContinueRetry(
            RetryReason::TransactionConflict
        )));
        assert!(should_retry_meta_write(&MetaError::ContinueRetry(
            RetryReason::LockContention
        )));

        // Non-retryable errors.
        assert!(!should_retry_meta_write(&MetaError::NotFound(1)));
        assert!(!should_retry_meta_write(&MetaError::Internal(
            "fatal".into()
        )));
    }

    #[test]
    fn test_uploaded_blocks_reject_overwrite() {
        let layout = ChunkLayout {
            chunk_size: 16 * 1024,
            block_size: 4 * 1024,
        };
        let mut slice = SliceState::new(
            1,
            0,
            test_config(layout),
            Arc::new(AtomicU64::new(0)),
            None,
            0,
        );
        slice
            .data
            .append(&vec![0u8; layout.block_size as usize * 2])
            .unwrap();
        slice.uploaded = layout.block_size as u64;

        assert!(slice.can_write(0, 16, None).is_none());
        assert!(
            slice
                .can_write(layout.block_size as u64, 16, None)
                .is_some()
        );
    }

    #[test]
    fn test_gap_append_requires_uncommitted_tail_range() {
        let layout = ChunkLayout {
            chunk_size: 16 * 1024,
            block_size: 4 * 1024,
        };
        let mut slice = SliceState::new(
            1,
            0,
            test_config(layout),
            Arc::new(AtomicU64::new(0)),
            None,
            1,
        );
        slice
            .data
            .append(&vec![1u8; layout.block_size as usize])
            .unwrap();

        assert_eq!(
            slice.can_write(
                layout.block_size as u64 * 2,
                layout.block_size as usize,
                Some(layout.block_size as u64)
            ),
            Some(PageWriteAction::GapAppend)
        );
        assert!(
            slice
                .can_write(
                    layout.block_size as u64 * 2,
                    layout.block_size as usize,
                    Some(layout.block_size as u64 * 2)
                )
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_file_writer_flush_commits_and_reads() {
        let layout = ChunkLayout::default();
        let store = Arc::new(InMemoryBlockStore::new());
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta_store = meta_handle.store();
        let meta = meta_handle.layer();
        let backend = Arc::new(Backend::new(store.clone(), meta.clone()));
        let ino = meta
            .create_file(1, "flush_reads.txt".to_string())
            .await
            .unwrap();
        let inode = Inode::new(ino, 0);
        let reader = Arc::new(DataReader::new(
            Arc::new(ReadConfig::new(layout)),
            backend.clone(),
        ));
        let writer = FileWriter::new(
            inode.clone(),
            test_config(layout),
            backend.clone(),
            reader,
            Arc::new(AtomicU64::new(0)),
            None,
        );

        let len = (layout.block_size / 2) as usize;
        let mut data = vec![0u8; len];
        for (i, b) in data.iter_mut().enumerate() {
            *b = (i % 251) as u8;
        }

        writer.write_at(0, &data).await.unwrap();
        writer.flush().await.unwrap();

        assert!(inode.file_size() >= len as u64);

        let cid = chunk_id_for(inode.ino(), 0).unwrap();
        let slices = meta_store.get_slices(cid).await.unwrap();
        assert_eq!(slices.len(), 1);

        let mut reader = DataFetcher::new(layout, cid, backend.as_ref());
        reader.prepare_slices().await.unwrap();
        let out = reader.read_at(0u64.into(), len).await.unwrap();
        assert_eq!(out, data);
    }

    #[tokio::test]
    async fn test_file_writer_appends_slices_for_overwrite() {
        let layout = ChunkLayout::default();
        let store = Arc::new(InMemoryBlockStore::new());
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta_store = meta_handle.store();
        let meta = meta_handle.layer();
        let backend = Arc::new(Backend::new(store.clone(), meta.clone()));
        let ino = meta
            .create_file(1, "overwrite.txt".to_string())
            .await
            .unwrap();
        let inode = Inode::new(ino, 0);
        let reader = Arc::new(DataReader::new(
            Arc::new(ReadConfig::new(layout)),
            backend.clone(),
        ));
        let writer = FileWriter::new(
            inode.clone(),
            test_config(layout),
            backend.clone(),
            reader,
            Arc::new(AtomicU64::new(0)),
            None,
        );

        let len = (layout.block_size / 4) as usize;
        let first = vec![1u8; len];
        writer.write_at(0, &first).await.unwrap();

        // Flush to freeze the first slice so the overwrite creates a new one.
        writer.flush().await.unwrap();

        let second = vec![2u8; len];
        writer.write_at(0, &second).await.unwrap();

        writer.flush().await.unwrap();

        let cid = chunk_id_for(inode.ino(), 0).unwrap();
        let slices = meta_store.get_slices(cid).await.unwrap();
        // The first flush commits slice 1, then the overwrite at offset 0
        // cannot append to a Committed slice, so it creates a fresh slice.
        assert_eq!(slices.len(), 2);

        let mut reader = DataFetcher::new(layout, cid, backend.as_ref());
        reader.prepare_slices().await.unwrap();
        let out = reader.read_at(0u64.into(), len).await.unwrap();
        assert_eq!(out, second);
    }

    #[tokio::test]
    async fn test_cached_out_of_order_gap_backfill_commits_dense_slices() {
        let layout = ChunkLayout {
            chunk_size: 16 * 1024,
            block_size: 4 * 1024,
        };
        let store = Arc::new(InMemoryBlockStore::new());
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta_store = meta_handle.store();
        let meta = meta_handle.layer();
        let backend = Arc::new(Backend::new(store.clone(), meta.clone()));
        let ino = meta
            .create_file(1, "cached_gap_backfill.txt".to_string())
            .await
            .unwrap();
        let inode = Inode::new(ino, 0);
        let reader = Arc::new(DataReader::new(
            Arc::new(ReadConfig::new(layout)),
            backend.clone(),
        ));
        let writer = FileWriter::new(
            inode.clone(),
            test_config(layout),
            backend.clone(),
            reader,
            Arc::new(AtomicU64::new(0)),
            None,
        );

        let block = layout.block_size as usize;
        let first = vec![1u8; block];
        let middle = vec![2u8; block];
        let tail = vec![3u8; block];

        writer.write_at_cached(0, &first, 1).await.unwrap();
        writer
            .write_at_cached((block * 2) as u64, &tail, 3)
            .await
            .unwrap();
        writer
            .write_at_cached(block as u64, &middle, 2)
            .await
            .unwrap();
        writer.flush().await.unwrap();

        let cid = chunk_id_for(inode.ino(), 0).unwrap();
        let slices = meta_store.get_slices(cid).await.unwrap();
        assert_eq!(slices.len(), 2);
        assert!(
            slices
                .iter()
                .any(|s| s.offset == 0 && s.length == (block * 2) as u64)
        );
        assert!(
            slices
                .iter()
                .any(|s| s.offset == (block * 2) as u64 && s.length == block as u64)
        );
        assert!(
            !slices
                .iter()
                .any(|s| s.offset == 0 && s.length >= (block * 3) as u64),
            "out-of-order cached writes must not commit a sparse range as dense metadata"
        );

        let mut expected = first;
        expected.extend_from_slice(&middle);
        expected.extend_from_slice(&tail);

        let mut reader = DataFetcher::new(layout, cid, backend.as_ref());
        reader.prepare_slices().await.unwrap();
        let out = reader.read_at(0u64.into(), expected.len()).await.unwrap();
        assert_eq!(out, expected);
    }

    #[tokio::test]
    async fn test_overlay_dirty_prefers_live_slice_over_recently_committed() {
        let layout = ChunkLayout::default();
        let store = Arc::new(InMemoryBlockStore::new());
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta = meta_handle.layer();
        let backend = Arc::new(Backend::new(store, meta.clone()));
        let ino = meta
            .create_file(1, "overlay_live_wins.txt".to_string())
            .await
            .unwrap();
        let inode = Inode::new(ino, 0);
        let reader = Arc::new(DataReader::new(
            Arc::new(ReadConfig::new(layout)),
            backend.clone(),
        ));
        let writer = FileWriter::new(
            inode.clone(),
            test_config(layout),
            backend,
            reader.clone(),
            Arc::new(AtomicU64::new(0)),
            None,
        );

        let len = (layout.block_size / 4) as usize;
        let first = vec![7u8; len];
        writer.write_at(0, &first).await.unwrap();
        writer.flush().await.unwrap();

        let file_reader = reader.open_for_handle(inode.clone(), 11);
        let cached = file_reader.read(0, len).await.unwrap();
        assert_eq!(cached, first);

        let second = vec![8u8; len];
        writer.write_at(0, &second).await.unwrap();

        let mut combined = file_reader.read(0, len).await.unwrap();
        writer.overlay_dirty(0, &mut combined).await.unwrap();
        assert_eq!(combined, second);
    }

    #[tokio::test]
    async fn test_reader_cache_sees_overwrite_after_flush() {
        let layout = ChunkLayout::default();
        let store = Arc::new(InMemoryBlockStore::new());
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta = meta_handle.layer();
        let backend = Arc::new(Backend::new(store, meta.clone()));
        let ino = meta
            .create_file(1, "flush_invalidate_overwrite.txt".to_string())
            .await
            .unwrap();
        let inode = Inode::new(ino, 0);
        let reader = Arc::new(DataReader::new(
            Arc::new(ReadConfig::new(layout)),
            backend.clone(),
        ));
        let writer = FileWriter::new(
            inode.clone(),
            test_config(layout),
            backend,
            reader.clone(),
            Arc::new(AtomicU64::new(0)),
            None,
        );

        let len = (layout.block_size / 4) as usize;
        let first = vec![7u8; len];
        writer.write_at(0, &first).await.unwrap();
        writer.flush().await.unwrap();

        let file_reader = reader.open_for_handle(inode, 12);
        let cached = file_reader.read(0, len).await.unwrap();
        assert_eq!(cached, first);

        let second = vec![8u8; len];
        writer.write_at(0, &second).await.unwrap();
        writer.flush().await.unwrap();

        let out = file_reader.read(0, len).await.unwrap();
        assert_eq!(out, second);
    }

    #[tokio::test]
    async fn test_upload_before_commit_uploaded_slices_do_not_keep_overlay_state_after_flush() {
        let layout = ChunkLayout::default();
        let store = Arc::new(InMemoryBlockStore::new());
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta = meta_handle.layer();
        let backend = Arc::new(Backend::new(store, meta.clone()));
        let ino = meta
            .create_file(1, "recently_committed_overlay.txt".to_string())
            .await
            .unwrap();
        let inode = Inode::new(ino, 0);
        let reader = Arc::new(DataReader::new(
            Arc::new(ReadConfig::new(layout)),
            backend.clone(),
        ));
        let writer = FileWriter::new(
            inode,
            test_config(layout),
            backend,
            reader,
            Arc::new(AtomicU64::new(0)),
            None,
        );

        writer.write_at(0, &[1u8; 4096]).await.unwrap();
        writer.flush().await.unwrap();

        assert!(
            !writer.has_pending().await,
            "flush should drain live pending writes"
        );
        assert!(
            !writer.has_overlay_state().await,
            "upload-before-commit slices are already durable before metadata commit and must not keep the read path in overlay mode"
        );
    }

    #[tokio::test]
    async fn test_upload_before_commit_discards_uploaded_recently_committed_slices() {
        let layout = ChunkLayout {
            chunk_size: 8 * 1024,
            block_size: 4 * 1024,
        };
        let store = Arc::new(InMemoryBlockStore::new());
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta = meta_handle.layer();
        let backend = Arc::new(Backend::new(store, meta.clone()));
        let ino = meta
            .create_file(1, "recently_committed_discard.txt".to_string())
            .await
            .unwrap();
        let inode = Inode::new(ino, 0);
        let reader = Arc::new(DataReader::new(
            Arc::new(ReadConfig::new(layout)),
            backend.clone(),
        ));
        let writer = Arc::new(DataWriter::new(test_config(layout), backend, reader, None));
        let file_writer = writer.ensure_file(inode);

        file_writer
            .write_at(0, &vec![1u8; layout.block_size as usize])
            .await
            .unwrap();
        file_writer.flush().await.unwrap();

        let breakdown = writer.dirty_breakdown().await;
        assert_eq!(
            breakdown.recently_committed_uploaded_slices, 0,
            "uploaded committed slices no longer need overlay data in upload-before-commit mode"
        );
        assert_eq!(breakdown.recently_committed_pending_upload_slices, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_upload_before_commit_does_not_publish_uploaded_prefix_by_default() {
        let layout = ChunkLayout {
            chunk_size: 16 * 1024,
            block_size: 4 * 1024,
        };
        let store = Arc::new(InMemoryBlockStore::new());
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta = meta_handle.layer();
        let backend = Arc::new(Backend::new(store, meta.clone()));
        let ino = meta
            .create_file(1, "uploaded_prefix_no_split.txt".to_string())
            .await
            .unwrap();
        let inode = Inode::new(ino, 0);
        let reader = Arc::new(DataReader::new(
            Arc::new(ReadConfig::new(layout)),
            backend.clone(),
        ));
        let writer = Arc::new(DataWriter::new(test_config(layout), backend, reader, None));
        let file_writer = writer.ensure_file(inode.clone());
        let block = layout.block_size as usize;

        file_writer
            .write_at_cached(0, &vec![1u8; block + 1024], 10)
            .await
            .unwrap();

        sleep(Duration::from_millis(200)).await;
        let cid = chunk_id_for(inode.ino(), 0).unwrap();
        assert!(
            meta.get_slices(cid).await.unwrap().is_empty(),
            "uploaded full-block prefixes should not become visible before close/flush by default"
        );

        file_writer.flush().await.unwrap();
        let slices = meta.get_slices(cid).await.unwrap();
        assert!(
            slices
                .iter()
                .any(|s| s.offset == 0 && s.length == (block + 1024) as u64),
            "explicit flush should publish the complete slice"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_upload_before_commit_splits_uploaded_prefix_and_keeps_tail_writable() {
        let layout = ChunkLayout {
            chunk_size: 16 * 1024,
            block_size: 4 * 1024,
        };
        let store = Arc::new(InMemoryBlockStore::new());
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta = meta_handle.layer();
        let backend = Arc::new(Backend::new(store, meta.clone()));
        let ino = meta
            .create_file(1, "uploaded_prefix_split.txt".to_string())
            .await
            .unwrap();
        let inode = Inode::new(ino, 0);
        let reader = Arc::new(DataReader::new(
            Arc::new(ReadConfig::new(layout)),
            backend.clone(),
        ));
        let writer = Arc::new(DataWriter::new(
            test_config_with_prefix_split(layout),
            backend,
            reader.clone(),
            None,
        ));
        let file_writer = writer.ensure_file(inode.clone());
        let block = layout.block_size as usize;

        file_writer
            .write_at_cached(0, &vec![1u8; block + 1024], 10)
            .await
            .unwrap();

        let cid = chunk_id_for(inode.ino(), 0).unwrap();
        timeout(Duration::from_secs(2), async {
            loop {
                let slices = meta.get_slices(cid).await.unwrap();
                if slices
                    .iter()
                    .any(|s| s.offset == 0 && s.length == block as u64)
                {
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("uploaded full-block prefix should commit before the partial tail freezes");

        assert!(
            file_writer.has_pending().await,
            "partial tail should remain writable after prefix commit"
        );

        file_writer
            .write_at_cached((block + 1024) as u64, &vec![2u8; block - 1024], 11)
            .await
            .unwrap();
        file_writer.flush().await.unwrap();

        let file_reader = reader.open_for_handle(inode, 31);
        let out = file_reader.read(0, block * 2).await.unwrap();
        let mut expected = vec![1u8; block + 1024];
        expected.extend_from_slice(&vec![2u8; block - 1024]);
        assert_eq!(out, expected);
    }

    #[tokio::test]
    async fn test_commit_before_upload_keeps_overlay_until_upload_finishes() {
        let layout = ChunkLayout {
            chunk_size: 8 * 1024,
            block_size: 4 * 1024,
        };
        let store = Arc::new(BlockingStore::new(true));
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta = meta_handle.layer();
        let backend = Arc::new(Backend::new(store.clone(), meta.clone()));
        let ino = meta
            .create_file(1, "writeback_overlay.txt".to_string())
            .await
            .unwrap();
        let inode = Inode::new(ino, 0);
        let reader = Arc::new(DataReader::new(
            Arc::new(ReadConfig::new(layout)),
            backend.clone(),
        ));
        let writer = FileWriter::new(
            inode.clone(),
            test_config_with_writeback(layout, WriteBackMode::CommitBeforeUpload),
            backend,
            reader.clone(),
            Arc::new(AtomicU64::new(0)),
            None,
        );

        let len = (layout.block_size / 2) as usize;
        let data = vec![9u8; len];
        writer.write_at(0, &data).await.unwrap();

        timeout(Duration::from_secs(2), writer.flush())
            .await
            .expect("commit-before-upload flush should not wait for blocked object upload")
            .unwrap();

        assert!(
            !writer.has_pending().await,
            "flush should treat metadata-committed slices as drained"
        );
        assert!(
            writer.has_overlay_state().await,
            "overlay must remain while the object upload is still blocked"
        );
        assert_eq!(
            writer
                .read_dirty_if_fully_covered(0, len)
                .await
                .unwrap()
                .unwrap(),
            data
        );

        store.unblock();
        let file_reader = reader.open_for_handle(inode, 21);
        let uploaded = timeout(Duration::from_secs(2), async {
            loop {
                if let Ok(out) = file_reader.read(0, len).await
                    && out == data
                {
                    break out;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("blocked object upload should eventually finish");

        assert_eq!(uploaded, data);
        timeout(Duration::from_secs(2), async {
            loop {
                if writer
                    .read_dirty_if_fully_covered(0, len)
                    .await
                    .unwrap()
                    .is_none()
                {
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("uploaded committed data should stop participating in overlay");
    }

    #[tokio::test]
    async fn test_commit_before_upload_waits_source_range_until_upload_finishes() {
        let layout = ChunkLayout {
            chunk_size: 8 * 1024,
            block_size: 4 * 1024,
        };
        let store = Arc::new(BlockingStore::new(true));
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta = meta_handle.layer();
        let backend = Arc::new(Backend::new(store.clone(), meta.clone()));
        let ino = meta
            .create_file(1, "writeback_copy_source.txt".to_string())
            .await
            .unwrap();
        let inode = Inode::new(ino, 0);
        let reader = Arc::new(DataReader::new(
            Arc::new(ReadConfig::new(layout)),
            backend.clone(),
        ));
        let writer = FileWriter::new(
            inode,
            test_config_with_writeback(layout, WriteBackMode::CommitBeforeUpload),
            backend,
            reader,
            Arc::new(AtomicU64::new(0)),
            None,
        );

        let data = vec![0x5au8; layout.block_size as usize / 2];
        writer.write_at(0, &data).await.unwrap();
        writer.flush().await.unwrap();

        assert!(
            timeout(
                Duration::from_millis(50),
                writer.wait_committed_uploads_for_range(0, data.len(), Duration::from_secs(2))
            )
            .await
            .is_err(),
            "source-range wait should not finish while the remote upload is blocked"
        );

        store.unblock();
        timeout(
            Duration::from_secs(2),
            writer.wait_committed_uploads_for_range(0, data.len(), Duration::from_secs(2)),
        )
        .await
        .expect("upload wait should finish after unblocking the store")
        .unwrap();
    }

    #[tokio::test]
    async fn test_cached_gap_writes_commit_as_separate_dense_slices() {
        let layout = ChunkLayout {
            chunk_size: 16 * 1024,
            block_size: 4 * 1024,
        };
        let store = Arc::new(InMemoryBlockStore::new());
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta = meta_handle.layer();
        let backend = Arc::new(Backend::new(store.clone(), meta.clone()));
        let ino = meta
            .create_file(1, "cached_gap_dense_slices.txt".to_string())
            .await
            .unwrap();
        let inode = Inode::new(ino, 0);
        let reader = Arc::new(DataReader::new(
            Arc::new(ReadConfig::new(layout)),
            backend.clone(),
        ));
        let writer = FileWriter::new(
            inode.clone(),
            test_config(layout),
            backend,
            reader.clone(),
            Arc::new(AtomicU64::new(0)),
            None,
        );

        let block = layout.block_size as usize;
        writer
            .write_at_cached(0, &vec![0x11; block], 10)
            .await
            .unwrap();
        writer
            .write_at_cached((block * 2) as u64, &vec![0x22; block], 11)
            .await
            .unwrap();
        writer.flush().await.unwrap();

        let chunk_id = chunk_id_for(ino, 0).unwrap();
        let slices = meta.get_slices(chunk_id).await.unwrap();
        assert!(
            slices
                .iter()
                .any(|s| s.offset == 0 && s.length == block as u64)
        );
        assert!(
            slices
                .iter()
                .any(|s| s.offset == (block * 2) as u64 && s.length == block as u64)
        );
        assert!(
            !slices
                .iter()
                .any(|s| s.offset == 0 && s.length >= (block * 3) as u64),
            "a cached sparse gap must not be committed as one dense slice"
        );

        let file_reader = reader.open_for_handle(inode, 41);
        let out = file_reader.read(0, block * 3).await.unwrap();
        assert_eq!(&out[..block], vec![0x11; block].as_slice());
        assert_eq!(&out[block..block * 2], vec![0; block].as_slice());
        assert_eq!(&out[block * 2..], vec![0x22; block].as_slice());
    }

    #[tokio::test]
    async fn test_commit_before_upload_commits_on_stage_notify_without_wait_timeout() {
        let layout = ChunkLayout {
            chunk_size: 8 * 1024,
            block_size: 4 * 1024,
        };
        let store = Arc::new(BlockingStore::new(true));
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta = meta_handle.layer();
        let backend = Arc::new(Backend::new(store.clone(), meta.clone()));
        let ino = meta
            .create_file(1, "stage_notify_early_commit.txt".to_string())
            .await
            .unwrap();
        let inode = Inode::new(ino, 0);
        let reader = Arc::new(DataReader::new(
            Arc::new(ReadConfig::new(layout)),
            backend.clone(),
        ));
        let temp = tempfile::tempdir().unwrap();
        let write_back = Arc::new(
            crate::vfs::cache::write_back::FsWriteBackCache::new_with_sync(
                temp.path().to_path_buf(),
                false,
            ),
        );
        let writer = Arc::new(DataWriter::new(
            test_config_with_writeback(layout, WriteBackMode::CommitBeforeUpload),
            backend,
            reader,
            Some(write_back),
        ));
        let file_writer = writer.ensure_file(inode);

        let data = vec![9u8; layout.block_size as usize];
        file_writer.write_at(0, &data).await.unwrap();

        let started = Instant::now();
        timeout(Duration::from_secs(2), file_writer.flush())
            .await
            .expect("commit-before-upload flush should not wait for blocked object upload")
            .unwrap();
        let elapsed = started.elapsed();

        let breakdown = writer.dirty_breakdown().await;
        assert!(
            breakdown.commit_wait_upload_us < COMMIT_WAIT_SLICE.as_micros() as u64,
            "stage-ready commit should not wait for the {:?} upload poll timeout; waited {}us, flush elapsed {:?}",
            COMMIT_WAIT_SLICE,
            breakdown.commit_wait_upload_us,
            elapsed
        );
        assert_eq!(
            breakdown.commit_before_stage_ops, 0,
            "metadata must not commit before the writeback stage record is sealed"
        );
        wait_for_recent_pending_upload_bytes(&writer, layout.block_size as u64).await;

        store.unblock();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_stage_ready_commit_before_upload_helper_requires_front_slice() {
        let layout = ChunkLayout {
            chunk_size: 8 * 1024,
            block_size: 4 * 1024,
        };
        let store = Arc::new(BlockingStore::new(true));
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta = meta_handle.layer();
        let backend = Arc::new(Backend::new(store, meta.clone()));
        let ino = meta
            .create_file(1, "stage_done_front_order.txt".to_string())
            .await
            .unwrap();
        let inode = Inode::new(ino, 0);
        let reader = Arc::new(DataReader::new(
            Arc::new(ReadConfig::new(layout)),
            backend.clone(),
        ));
        let temp = tempfile::tempdir().unwrap();
        let write_back = Arc::new(
            crate::vfs::cache::write_back::FsWriteBackCache::new_with_sync(
                temp.path().to_path_buf(),
                false,
            ),
        );
        let writer = FileWriter::new(
            inode.clone(),
            test_config_with_writeback(layout, WriteBackMode::CommitBeforeUpload),
            backend,
            reader,
            Arc::new(AtomicU64::new(0)),
            Some(write_back),
        );

        let cid = chunk_id_for(inode.ino(), 0).unwrap();
        let first = Arc::new(ParkingMutex::new(SliceState::new(
            cid,
            0,
            test_config_with_writeback(layout, WriteBackMode::CommitBeforeUpload),
            Arc::new(AtomicU64::new(0)),
            None,
            1,
        )));
        let second = Arc::new(ParkingMutex::new(SliceState::new(
            cid,
            layout.block_size.into(),
            test_config_with_writeback(layout, WriteBackMode::CommitBeforeUpload),
            Arc::new(AtomicU64::new(0)),
            None,
            2,
        )));

        for (slice, id) in [(&first, 11), (&second, 12)] {
            let mut state = slice.lock();
            state.data.append(&vec![id as u8; 1024]).unwrap();
            state.state = SliceStatus::Readonly;
            state.slice_id = Some(id);
            state.frozen_epoch = inode.data_epoch();
            state.freeze_reason = Some(SliceFreezeReason::ExplicitFlush);
            state.writeback_persisted_bytes = state.data.len();
            state.writeback_record_sealed = true;
        }

        {
            let mut guard = writer.shared.inner.lock().await;
            let mut chunk = ChunkState::new(cid);
            chunk.slices.push_back(first.clone());
            chunk.slices.push_back(second.clone());
            guard.chunks.insert(cid, chunk);
        }

        assert!(
            !FileWriter::try_commit_before_upload_if_front(&writer.shared, &second).await,
            "stage-done helper must not commit a later slice before earlier slices"
        );
        assert!(
            !matches!(second.lock().state, SliceStatus::Committed),
            "non-front slice must remain uncommitted"
        );
        assert!(
            meta.get_slices(cid).await.unwrap().is_empty(),
            "non-front slice must not be written to metadata"
        );

        assert!(
            FileWriter::try_commit_before_upload_if_front(&writer.shared, &first).await,
            "front stage-ready slice should commit without waiting for object upload"
        );
        assert!(matches!(first.lock().state, SliceStatus::Committed));
        let slices = meta.get_slices(cid).await.unwrap();
        assert_eq!(slices.len(), 1);
        assert_eq!(slices[0].slice_id, 11);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_commit_before_upload_can_skip_stage_when_configured() {
        let layout = ChunkLayout {
            chunk_size: 8 * 1024,
            block_size: 4 * 1024,
        };
        let store = Arc::new(BlockingStore::new(true));
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta = meta_handle.layer();
        let backend = Arc::new(Backend::new(store, meta.clone()));
        let ino = meta
            .create_file(1, "stage_skip_commit.txt".to_string())
            .await
            .unwrap();
        let inode = Inode::new(ino, 0);
        let reader = Arc::new(DataReader::new(
            Arc::new(ReadConfig::new(layout)),
            backend.clone(),
        ));
        let temp = tempfile::tempdir().unwrap();
        let write_back = Arc::new(
            crate::vfs::cache::write_back::FsWriteBackCache::new_with_sync(
                temp.path().to_path_buf(),
                false,
            ),
        );
        let config = Arc::new(
            WriteConfig::new(layout)
                .page_size(4 * 1024)
                .freeze_min_bytes(4096)
                .auto_flush_max_age(Duration::from_millis(5))
                .writeback_mode(WriteBackMode::CommitBeforeUpload)
                .writeback_require_stage_before_commit(false),
        );
        let writer = FileWriter::new(
            inode.clone(),
            config.clone(),
            backend,
            reader,
            Arc::new(AtomicU64::new(0)),
            Some(write_back),
        );

        let cid = chunk_id_for(inode.ino(), 0).unwrap();
        let slice = Arc::new(ParkingMutex::new(SliceState::new(
            cid,
            0,
            config,
            Arc::new(AtomicU64::new(0)),
            None,
            1,
        )));
        {
            let mut state = slice.lock();
            state.data.append(&vec![31u8; 1024]).unwrap();
            state.state = SliceStatus::Readonly;
            state.slice_id = Some(31);
            state.frozen_epoch = inode.data_epoch();
            state.freeze_reason = Some(SliceFreezeReason::ExplicitFlush);
        }

        {
            let mut guard = writer.shared.inner.lock().await;
            let mut chunk = ChunkState::new(cid);
            chunk.slices.push_back(slice.clone());
            guard.chunks.insert(cid, chunk);
        }

        assert!(
            FileWriter::try_commit_before_upload_if_front(&writer.shared, &slice).await,
            "front slice should commit without waiting for local stage when configured"
        );
        assert!(matches!(slice.lock().state, SliceStatus::Committed));
        assert_eq!(
            writer
                .shared
                .recent_pending_upload
                .commit_before_stage_ops
                .load(Ordering::Relaxed),
            1
        );
        let slices = meta.get_slices(cid).await.unwrap();
        assert_eq!(slices.len(), 1);
        assert_eq!(slices[0].slice_id, 31);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_commit_before_upload_watchdog_fails_stale_background_upload() {
        let layout = ChunkLayout {
            chunk_size: 8 * 1024,
            block_size: 4 * 1024,
        };
        let store = Arc::new(BlockingStore::new(true));
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta = meta_handle.layer();
        let backend = Arc::new(Backend::new(store.clone(), meta.clone()));
        let ino = meta
            .create_file(1, "stale_background_upload.txt".to_string())
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
            .write_at(0, &vec![8u8; layout.block_size as usize])
            .await
            .unwrap();
        timeout(Duration::from_secs(2), file_writer.flush())
            .await
            .expect("commit-before-upload flush should return while object upload is blocked")
            .unwrap();

        wait_for_recent_pending_upload_bytes(&writer, layout.block_size as u64).await;

        let stale_slice = {
            let guard = file_writer.shared.inner.lock().await;
            guard
                .chunks
                .values()
                .find_map(|chunk| chunk.recently_committed.front().cloned())
                .expect("flush should leave a recently committed slice pending upload")
        };
        stale_slice.lock().started =
            Instant::now() - COMMIT_UPLOAD_MAX_WAIT - Duration::from_secs(1);

        timeout(Duration::from_secs(2), async {
            loop {
                if writer.recent_pending_upload_bytes() == 0
                    && file_writer.shared.writeback_error().is_some()
                {
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("stale background upload should be failed by watchdog");

        let err = file_writer.shared.writeback_error().unwrap();
        assert!(
            err.contains("upload stalled"),
            "unexpected writeback error: {err}"
        );
        store.unblock();
    }

    #[tokio::test]
    async fn test_data_writer_release_keeps_writeback_overlay_until_upload_finishes() {
        let layout = ChunkLayout {
            chunk_size: 8 * 1024,
            block_size: 4 * 1024,
        };
        let store = Arc::new(BlockingStore::new(true));
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta = meta_handle.layer();
        let backend = Arc::new(Backend::new(store.clone(), meta.clone()));
        let ino = meta
            .create_file(1, "release_writeback_overlay.txt".to_string())
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

        let len = (layout.block_size / 2) as usize;
        let data = vec![7u8; len];
        let file_writer = writer.ensure_file(inode);
        file_writer.write_at(0, &data).await.unwrap();
        timeout(Duration::from_secs(2), file_writer.flush())
            .await
            .expect("commit-before-upload flush should not wait for blocked object upload")
            .unwrap();

        assert!(file_writer.has_overlay_state().await);
        writer.release(ino as u64).await;

        assert!(
            writer.has_file(ino as u64),
            "released writeback writer should stay indexed while overlay is needed"
        );
        let mut out = vec![0u8; len];
        writer
            .overlay_dirty_if_exists(ino as u64, 0, &mut out)
            .await
            .unwrap();
        assert_eq!(out, data);

        store.unblock();
    }

    #[tokio::test]
    async fn test_recent_pending_upload_accounting_tracks_commit_and_upload_completion() {
        let layout = ChunkLayout {
            chunk_size: 8 * 1024,
            block_size: 4 * 1024,
        };
        let store = Arc::new(BlockingStore::new(true));
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta = meta_handle.layer();
        let backend = Arc::new(Backend::new(store.clone(), meta.clone()));
        let ino = meta
            .create_file(1, "pending_upload_accounting.txt".to_string())
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
            .write_at(0, &vec![3u8; layout.block_size as usize])
            .await
            .unwrap();
        timeout(Duration::from_secs(2), file_writer.flush())
            .await
            .expect("commit-before-upload flush should return while object upload is blocked")
            .unwrap();

        wait_for_recent_pending_upload_bytes(&writer, layout.block_size as u64).await;

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
        .expect("pending-upload bytes should drop after object upload completes");
    }

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

        wait_for_recent_pending_upload_bytes(&writer, 1536).await;

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

    #[tokio::test]
    async fn test_commit_before_upload_removes_writeback_record_after_upload() {
        let layout = ChunkLayout {
            chunk_size: 8 * 1024,
            block_size: 4 * 1024,
        };
        let store = Arc::new(BlockingStore::new(true));
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta = meta_handle.layer();
        let backend = Arc::new(Backend::new(store.clone(), meta.clone()));
        let ino = meta
            .create_file(1, "pending_upload_record_cleanup.txt".to_string())
            .await
            .unwrap();
        let inode = Inode::new(ino, 0);
        let reader = Arc::new(DataReader::new(
            Arc::new(ReadConfig::new(layout)),
            backend.clone(),
        ));
        let temp = tempfile::tempdir().unwrap();
        let write_back = Arc::new(
            crate::vfs::cache::write_back::FsWriteBackCache::new_with_sync(
                temp.path().to_path_buf(),
                false,
            ),
        );
        let writer = Arc::new(DataWriter::new(
            test_config_with_writeback(layout, WriteBackMode::CommitBeforeUpload),
            backend,
            reader,
            Some(write_back.clone()),
        ));
        let file_writer = writer.ensure_file(inode);

        file_writer
            .write_at(0, &vec![5u8; layout.block_size as usize])
            .await
            .unwrap();
        timeout(Duration::from_secs(2), file_writer.flush())
            .await
            .expect("commit-before-upload flush should return while object upload is blocked")
            .unwrap();

        timeout(Duration::from_secs(2), async {
            loop {
                if write_back.recover().await.unwrap().len() == 1 {
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("blocked upload should leave one recoverable dirty record");

        store.unblock();
        timeout(Duration::from_secs(2), async {
            loop {
                if writer.recent_pending_upload_bytes() == 0
                    && write_back.recover().await.unwrap().is_empty()
                {
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("dirty record should be removed after object upload completes");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_commit_before_upload_requires_writeback_stage_success() {
        let layout = ChunkLayout {
            chunk_size: 8 * 1024,
            block_size: 4 * 1024,
        };
        let store = Arc::new(BlockingStore::new(true));
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta = meta_handle.layer();
        let backend = Arc::new(Backend::new(store.clone(), meta.clone()));
        let ino = meta
            .create_file(1, "stage_failure_blocks_commit.txt".to_string())
            .await
            .unwrap();
        let inode = Inode::new(ino, 0);
        let reader = Arc::new(DataReader::new(
            Arc::new(ReadConfig::new(layout)),
            backend.clone(),
        ));
        let temp = tempfile::tempdir().unwrap();
        let invalid_root = temp.path().join("not-a-directory");
        std::fs::write(&invalid_root, b"not a directory").unwrap();
        let write_back = Arc::new(
            crate::vfs::cache::write_back::FsWriteBackCache::new_with_sync(invalid_root, false),
        );
        let writer = Arc::new(DataWriter::new(
            test_config_with_writeback(layout, WriteBackMode::CommitBeforeUpload),
            backend,
            reader,
            Some(write_back),
        ));
        let file_writer = writer.ensure_file(inode);

        file_writer
            .write_at(0, &vec![6u8; layout.block_size as usize])
            .await
            .unwrap();

        let flush_result = timeout(Duration::from_secs(2), file_writer.flush())
            .await
            .expect("flush should not hang when local staging fails");
        store.unblock();
        let err = flush_result.expect_err("metadata must not commit after staging failure");
        assert!(
            err.to_string().contains("writeback failed"),
            "unexpected flush error: {err:?}"
        );

        let breakdown = writer.dirty_breakdown().await;
        assert_eq!(breakdown.stage_failures, 1);
        assert_eq!(
            breakdown.commit_before_stage_ops, 0,
            "staging failures must block metadata commit-before-upload"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_writeback_backpressure_waits_for_pending_upload_to_drain() {
        let layout = ChunkLayout {
            chunk_size: 8 * 1024,
            block_size: 4 * 1024,
        };
        let store = Arc::new(BlockingStore::new(true));
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta = meta_handle.layer();
        let backend = Arc::new(Backend::new(store.clone(), meta.clone()));
        let ino = meta
            .create_file(1, "pending_upload_backpressure.txt".to_string())
            .await
            .unwrap();
        let inode = Inode::new(ino, 0);
        let reader = Arc::new(DataReader::new(
            Arc::new(ReadConfig::new(layout)),
            backend.clone(),
        ));
        let config = Arc::new(
            WriteConfig::new(layout)
                .page_size(4 * 1024)
                .freeze_min_bytes(4096)
                .writeback_mode(WriteBackMode::CommitBeforeUpload)
                .writeback_recent_pending_soft_limit(layout.block_size as u64),
        );
        let writer = Arc::new(DataWriter::new(config, backend, reader, None));
        let file_writer = writer.ensure_file(inode);

        file_writer
            .write_at(0, &vec![1u8; layout.block_size as usize])
            .await
            .unwrap();
        timeout(Duration::from_secs(2), file_writer.flush())
            .await
            .expect("commit-before-upload flush should return while object upload is blocked")
            .unwrap();

        timeout(Duration::from_secs(2), async {
            loop {
                if writer.recent_pending_upload_bytes() >= layout.block_size as u64 {
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("blocked upload should be counted before testing backpressure");

        let unblocked = Arc::new(AtomicBool::new(false));
        let unblock_task = {
            let store = store.clone();
            let unblocked = unblocked.clone();
            tokio::spawn(async move {
                sleep(Duration::from_millis(50)).await;
                unblocked.store(true, Ordering::Release);
                store.unblock();
            })
        };
        let second_write = vec![2u8; 512];
        timeout(
            Duration::from_secs(2),
            file_writer.write_at(layout.block_size as u64, &second_write),
        )
        .await
        .expect("write should wake after pending-upload backlog drains")
        .unwrap();
        assert!(
            unblocked.load(Ordering::Acquire),
            "write completed before pending-upload backlog was drained"
        );
        timeout(Duration::from_secs(2), unblock_task)
            .await
            .expect("unblock task should finish")
            .unwrap();

        let breakdown = writer.dirty_breakdown().await;
        assert_eq!(breakdown.backpressure_hard_wait_ops, 1);
        assert!(
            breakdown.backpressure_hard_wait_us > 0,
            "hard wait duration should be recorded after blocked write wakes"
        );
    }

    #[tokio::test]
    async fn test_dirty_breakdown_reports_slice_lifecycle_metrics() {
        let layout = ChunkLayout {
            chunk_size: 16 * 1024,
            block_size: 4 * 1024,
        };
        let store = Arc::new(InMemoryBlockStore::new());
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta = meta_handle.layer();
        let backend = Arc::new(Backend::new(store, meta.clone()));
        let ino = meta
            .create_file(1, "slice_lifecycle_metrics.txt".to_string())
            .await
            .unwrap();
        let inode = Inode::new(ino, 0);
        let reader = Arc::new(DataReader::new(
            Arc::new(ReadConfig::new(layout)),
            backend.clone(),
        ));
        let writer = Arc::new(DataWriter::new(
            test_config_without_auto_flush(layout),
            backend,
            reader,
            None,
        ));
        let file_writer = writer.ensure_file(inode);

        file_writer.write_at(0, &[1u8; 1024]).await.unwrap();
        file_writer.write_at(1024, &[2u8; 1024]).await.unwrap();
        let before_flush = writer.dirty_breakdown().await;
        assert_eq!(before_flush.live_slices, 1);
        assert_eq!(before_flush.slice_create_ops, 1);
        assert_eq!(before_flush.slice_reuse_ops, 1);

        file_writer.flush().await.unwrap();
        let after_flush = writer.dirty_breakdown().await;
        assert_eq!(after_flush.freeze_explicit_flush_ops, 1);
        assert_eq!(after_flush.upload_batch_ops, 1);
        assert_eq!(after_flush.upload_partial_tail_ops, 1);
        assert_eq!(after_flush.upload_partial_tail_explicit_flush_ops, 1);
        assert_eq!(after_flush.upload_partial_tail_auto_ops, 0);
        assert_eq!(after_flush.upload_partial_tail_max_unflushed_ops, 0);
    }

    #[tokio::test]
    async fn test_explicit_flush_coalesces_adjacent_cached_slices() {
        let layout = ChunkLayout {
            chunk_size: 16 * 1024,
            block_size: 4 * 1024,
        };
        let store = Arc::new(InMemoryBlockStore::new());
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta = meta_handle.layer();
        let backend = Arc::new(Backend::new(store, meta.clone()));
        let ino = meta
            .create_file(1, "cached_flush_coalesce.txt".to_string())
            .await
            .unwrap();
        let inode = Inode::new(ino, 0);
        let reader = Arc::new(DataReader::new(
            Arc::new(ReadConfig::new(layout)),
            backend.clone(),
        ));
        let writer = Arc::new(DataWriter::new(
            test_config_without_auto_flush(layout),
            backend,
            reader.clone(),
            None,
        ));
        let file_writer = writer.ensure_file(inode.clone());

        file_writer
            .write_at_cached(1024, &[2u8; 1024], 20)
            .await
            .unwrap();
        file_writer
            .write_at_cached(0, &[1u8; 1024], 10)
            .await
            .unwrap();

        let before_flush = writer.dirty_breakdown().await;
        assert_eq!(before_flush.live_slices, 2);
        assert_eq!(before_flush.slice_create_ops, 2);
        assert_eq!(before_flush.slice_reuse_ops, 0);

        file_writer.flush().await.unwrap();

        let after_flush = writer.dirty_breakdown().await;
        assert_eq!(after_flush.flush_fragmentation_slices, 1);
        assert_eq!(after_flush.flush_fragmentation_cached_sub_block_slices, 1);
        assert_eq!(after_flush.freeze_explicit_flush_ops, 1);
        assert_eq!(after_flush.upload_batch_ops, 1);
        assert_eq!(after_flush.upload_partial_tail_explicit_flush_ops, 1);

        let cid = chunk_id_for(inode.ino(), 0).unwrap();
        let slices = meta.get_slices(cid).await.unwrap();
        assert_eq!(slices.len(), 1);
        assert_eq!(slices[0].offset, 0);
        assert_eq!(slices[0].length, 2048);

        let file_reader = reader.open_for_handle(inode, 1);
        let out = file_reader.read(0, 2048).await.unwrap();
        assert_eq!(&out[..1024], &[1u8; 1024]);
        assert_eq!(&out[1024..], &[2u8; 1024]);
    }

    #[tokio::test]
    async fn test_cached_block_assembler_coalesces_overlapping_cached_slices() {
        let layout = ChunkLayout {
            chunk_size: 16 * 1024,
            block_size: 4 * 1024,
        };
        let store = Arc::new(InMemoryBlockStore::new());
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta = meta_handle.layer();
        let backend = Arc::new(Backend::new(store, meta.clone()));
        let ino = meta
            .create_file(1, "cached_overlap_coalesce.txt".to_string())
            .await
            .unwrap();
        let inode = Inode::new(ino, 0);
        let reader = Arc::new(DataReader::new(
            Arc::new(ReadConfig::new(layout)),
            backend.clone(),
        ));
        let writer = Arc::new(DataWriter::new(
            Arc::new(
                WriteConfig::new(layout)
                    .page_size(4 * 1024)
                    .freeze_min_bytes(4096)
                    .auto_flush_max_age(Duration::from_secs(60))
                    .cached_block_assembler(true),
            ),
            backend,
            reader.clone(),
            None,
        ));
        let file_writer = writer.ensure_file(inode.clone());

        file_writer
            .write_at_cached(0, &[2u8; 2048], 20)
            .await
            .unwrap();
        file_writer
            .write_at_cached(0, &[1u8; 1024], 10)
            .await
            .unwrap();

        let before_flush = writer.dirty_breakdown().await;
        assert_eq!(before_flush.live_slices, 2);
        assert_eq!(before_flush.slice_create_ops, 2);
        assert_eq!(before_flush.slice_reject_older_unique_ops, 1);

        file_writer.flush().await.unwrap();

        let after_flush = writer.dirty_breakdown().await;
        assert_eq!(after_flush.flush_fragmentation_slices, 1);
        assert_eq!(after_flush.freeze_explicit_flush_ops, 1);
        assert_eq!(after_flush.upload_batch_ops, 1);

        let cid = chunk_id_for(inode.ino(), 0).unwrap();
        let slices = meta.get_slices(cid).await.unwrap();
        assert_eq!(slices.len(), 1);
        assert_eq!(slices[0].offset, 0);
        assert_eq!(slices[0].length, 2048);

        let file_reader = reader.open_for_handle(inode, 1);
        let out = file_reader.read(0, 2048).await.unwrap();
        assert_eq!(out, vec![2u8; 2048]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_normal_write_waits_for_freeze_min_before_pipeline_upload() {
        let layout = ChunkLayout {
            chunk_size: 16 * 1024,
            block_size: 4 * 1024,
        };
        let store = Arc::new(InMemoryBlockStore::new());
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta = meta_handle.layer();
        let backend = Arc::new(Backend::new(store, meta.clone()));
        let ino = meta
            .create_file(1, "normal_freeze_min_pipeline.txt".to_string())
            .await
            .unwrap();
        let inode = Inode::new(ino, 0);
        let reader = Arc::new(DataReader::new(
            Arc::new(ReadConfig::new(layout)),
            backend.clone(),
        ));
        let config = Arc::new(
            WriteConfig::new(layout)
                .page_size(4 * 1024)
                .freeze_min_bytes(layout.block_size as u64 * 2)
                .auto_flush_max_age(Duration::from_secs(60)),
        );
        let writer = Arc::new(DataWriter::new(config, backend, reader, None));
        let file_writer = writer.ensure_file(inode);

        file_writer
            .write_at(0, &vec![1u8; layout.block_size as usize])
            .await
            .unwrap();
        sleep(Duration::from_millis(50)).await;
        let before_min = writer.dirty_breakdown().await;
        assert_eq!(
            before_min.upload_batch_ops, 0,
            "normal writes should aggregate until freeze_min before pipeline upload"
        );

        file_writer
            .write_at(
                layout.block_size as u64,
                &vec![2u8; layout.block_size as usize],
            )
            .await
            .unwrap();

        timeout(Duration::from_secs(2), async {
            loop {
                let breakdown = writer.dirty_breakdown().await;
                if breakdown.upload_batch_ops > 0 {
                    assert_eq!(breakdown.upload_batch_ops, 1);
                    assert_eq!(breakdown.upload_batch_blocks, 2);
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("normal write should upload once freeze_min is reached");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_writeback_commit_wait_upload_ignores_unflushable_slice() {
        let layout = ChunkLayout {
            chunk_size: 16 * 1024,
            block_size: 4 * 1024,
        };
        let store = Arc::new(InMemoryBlockStore::new());
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta = meta_handle.layer();
        let backend = Arc::new(Backend::new(store, meta.clone()));
        let ino = meta
            .create_file(1, "writeback_deferred_commit.txt".to_string())
            .await
            .unwrap();
        let inode = Inode::new(ino, 0);
        let reader = Arc::new(DataReader::new(
            Arc::new(ReadConfig::new(layout)),
            backend.clone(),
        ));
        let config = Arc::new(
            WriteConfig::new(layout)
                .page_size(4 * 1024)
                .freeze_min_bytes(layout.block_size as u64 * 2)
                .auto_flush_max_age(Duration::from_secs(60))
                .writeback_mode(WriteBackMode::CommitBeforeUpload),
        );
        let writer = Arc::new(DataWriter::new(config, backend, reader, None));
        let file_writer = writer.ensure_file(inode);

        file_writer
            .write_at(0, &vec![1u8; layout.block_size as usize])
            .await
            .unwrap();
        sleep(COMMIT_WAIT_SLICE * 2 + Duration::from_millis(20)).await;

        let before_flushable = writer.dirty_breakdown().await;
        assert_eq!(before_flushable.live_slices, 1);
        assert_eq!(before_flushable.upload_batch_ops, 0);
        assert_eq!(
            before_flushable.commit_wait_upload_ops, 0,
            "writable slices should not be counted as upload wait before they can be flushed"
        );

        file_writer
            .write_at(
                layout.block_size as u64,
                &vec![2u8; layout.block_size as usize],
            )
            .await
            .unwrap();

        timeout(Duration::from_secs(2), async {
            loop {
                let breakdown = writer.dirty_breakdown().await;
                if breakdown.upload_batch_ops > 0 {
                    assert_eq!(breakdown.upload_batch_blocks, 2);
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("writeback upload should start once freeze_min is reached");
    }

    #[tokio::test]
    async fn test_dirty_breakdown_reports_live_write_origin_mix() {
        let layout = ChunkLayout {
            chunk_size: 16 * 1024,
            block_size: 4 * 1024,
        };
        let store = Arc::new(InMemoryBlockStore::new());
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta = meta_handle.layer();
        let backend = Arc::new(Backend::new(store, meta.clone()));
        let reader = Arc::new(DataReader::new(
            Arc::new(ReadConfig::new(layout)),
            backend.clone(),
        ));
        let writer = Arc::new(DataWriter::new(
            test_config_without_auto_flush(layout),
            backend,
            reader,
            None,
        ));

        let normal_ino = meta
            .create_file(1, "origin_normal.txt".to_string())
            .await
            .unwrap();
        writer
            .ensure_file(Inode::new(normal_ino, 0))
            .write_at(0, &[1u8; 1024])
            .await
            .unwrap();

        let cached_ino = meta
            .create_file(1, "origin_cached.txt".to_string())
            .await
            .unwrap();
        writer
            .ensure_file(Inode::new(cached_ino, 0))
            .write_at_cached(0, &[2u8; 1024], 10)
            .await
            .unwrap();

        let mixed_ino = meta
            .create_file(1, "origin_mixed.txt".to_string())
            .await
            .unwrap();
        let mixed_writer = writer.ensure_file(Inode::new(mixed_ino, 0));
        mixed_writer
            .write_at_cached(0, &[3u8; 1024], 20)
            .await
            .unwrap();
        mixed_writer.write_at(1024, &[4u8; 1024]).await.unwrap();

        let breakdown = writer.dirty_breakdown().await;
        assert_eq!(breakdown.live_slices, 3);
        assert_eq!(breakdown.live_normal_only_slices, 1);
        assert_eq!(breakdown.live_cached_only_slices, 1);
        assert_eq!(breakdown.live_mixed_origin_slices, 1);
        assert_eq!(breakdown.live_unknown_origin_slices, 0);
    }

    #[tokio::test]
    async fn test_upload_partial_tail_metrics_are_attributed_by_write_origin() {
        let layout = ChunkLayout {
            chunk_size: 16 * 1024,
            block_size: 4 * 1024,
        };
        let store = Arc::new(InMemoryBlockStore::new());
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta = meta_handle.layer();
        let backend = Arc::new(Backend::new(store, meta.clone()));
        let reader = Arc::new(DataReader::new(
            Arc::new(ReadConfig::new(layout)),
            backend.clone(),
        ));
        let writer = Arc::new(DataWriter::new(test_config(layout), backend, reader, None));

        let normal_ino = meta
            .create_file(1, "tail_origin_normal.txt".to_string())
            .await
            .unwrap();
        let normal_writer = writer.ensure_file(Inode::new(normal_ino, 0));
        normal_writer.write_at(0, &[1u8; 1024]).await.unwrap();
        normal_writer.flush().await.unwrap();

        let cached_ino = meta
            .create_file(1, "tail_origin_cached.txt".to_string())
            .await
            .unwrap();
        let cached_writer = writer.ensure_file(Inode::new(cached_ino, 0));
        cached_writer
            .write_at_cached(0, &[2u8; 1024], 10)
            .await
            .unwrap();
        cached_writer.flush().await.unwrap();

        let mixed_ino = meta
            .create_file(1, "tail_origin_mixed.txt".to_string())
            .await
            .unwrap();
        let mixed_writer = writer.ensure_file(Inode::new(mixed_ino, 0));
        mixed_writer
            .write_at_cached(0, &[3u8; 1024], 20)
            .await
            .unwrap();
        mixed_writer.write_at(1024, &[4u8; 1024]).await.unwrap();
        mixed_writer.flush().await.unwrap();

        let breakdown = writer.dirty_breakdown().await;
        assert_eq!(breakdown.upload_partial_tail_ops, 3);
        assert_eq!(breakdown.upload_partial_tail_normal_only_ops, 1);
        assert_eq!(breakdown.upload_partial_tail_cached_only_ops, 1);
        assert_eq!(breakdown.upload_partial_tail_mixed_origin_ops, 1);
        assert_eq!(breakdown.upload_partial_tail_unknown_origin_ops, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_auto_flush_age_partial_tail_is_attributed() {
        let layout = ChunkLayout {
            chunk_size: 16 * 1024,
            block_size: 4 * 1024,
        };
        let store = Arc::new(InMemoryBlockStore::new());
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta = meta_handle.layer();
        let backend = Arc::new(Backend::new(store, meta.clone()));
        let ino = meta
            .create_file(1, "auto_age_partial_tail_metrics.txt".to_string())
            .await
            .unwrap();
        let inode = Inode::new(ino, 0);
        let reader = Arc::new(DataReader::new(
            Arc::new(ReadConfig::new(layout)),
            backend.clone(),
        ));
        let writer = Arc::new(DataWriter::new(test_config(layout), backend, reader, None));
        let file_writer = writer.ensure_file(inode);

        file_writer.write_at(0, &[9u8; 1024]).await.unwrap();

        timeout(Duration::from_secs(2), async {
            loop {
                let breakdown = writer.dirty_breakdown().await;
                if breakdown.upload_partial_tail_auto_age_ops == 1 {
                    assert_eq!(breakdown.upload_partial_tail_ops, 1);
                    assert_eq!(breakdown.upload_partial_tail_auto_ops, 1);
                    assert_eq!(breakdown.upload_partial_tail_auto_idle_ops, 0);
                    assert_eq!(breakdown.upload_partial_tail_auto_pressure_ops, 0);
                    assert_eq!(breakdown.upload_partial_tail_auto_too_many_ops, 0);
                    assert_eq!(breakdown.upload_partial_tail_auto_buffer_high_ops, 0);
                    assert_eq!(breakdown.upload_partial_tail_auto_flush_duration_ops, 0);
                    assert_eq!(breakdown.upload_partial_tail_auto_unknown_ops, 0);
                    assert_eq!(breakdown.upload_partial_tail_normal_only_ops, 1);
                    assert_eq!(breakdown.upload_partial_tail_cached_only_ops, 0);
                    assert_eq!(breakdown.upload_partial_tail_mixed_origin_ops, 0);
                    assert_eq!(breakdown.upload_partial_tail_unknown_origin_ops, 0);
                    assert_eq!(breakdown.upload_partial_tail_auto_normal_only_ops, 1);
                    assert_eq!(breakdown.upload_partial_tail_auto_cached_only_ops, 0);
                    assert_eq!(breakdown.upload_partial_tail_auto_mixed_origin_ops, 0);
                    assert_eq!(breakdown.upload_partial_tail_auto_unknown_origin_ops, 0);
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("auto age partial-tail upload should be attributed");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_auto_flush_defers_cached_sub_block_age_freeze_until_explicit_flush() {
        let layout = ChunkLayout {
            chunk_size: 16 * 1024,
            block_size: 4 * 1024,
        };
        let store = Arc::new(InMemoryBlockStore::new());
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta = meta_handle.layer();
        let backend = Arc::new(Backend::new(store, meta.clone()));
        let ino = meta
            .create_file(1, "cached_auto_age_deferral.txt".to_string())
            .await
            .unwrap();
        let inode = Inode::new(ino, 0);
        let reader = Arc::new(DataReader::new(
            Arc::new(ReadConfig::new(layout)),
            backend.clone(),
        ));
        let writer = Arc::new(DataWriter::new(test_config(layout), backend, reader, None));
        let file_writer = writer.ensure_file(inode);

        file_writer
            .write_at_cached(0, &[4u8; 1024], 10)
            .await
            .unwrap();

        sleep(Duration::from_millis(1200)).await;

        let before_flush = writer.dirty_breakdown().await;
        assert_eq!(
            before_flush.freeze_auto_ops, 0,
            "cached sub-block slices should not be auto-age frozen before explicit flush"
        );
        assert_eq!(before_flush.upload_partial_tail_auto_age_ops, 0);
        assert_eq!(before_flush.upload_partial_tail_ops, 0);

        file_writer.flush().await.unwrap();

        let after_flush = writer.dirty_breakdown().await;
        assert_eq!(after_flush.freeze_explicit_flush_ops, 1);
        assert_eq!(after_flush.upload_partial_tail_ops, 1);
        assert_eq!(after_flush.upload_partial_tail_explicit_flush_ops, 1);
        assert_eq!(after_flush.upload_partial_tail_auto_age_ops, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_auto_flush_defers_cached_partial_tail_after_full_block_dispatch() {
        let layout = ChunkLayout {
            chunk_size: 16 * 1024,
            block_size: 4 * 1024,
        };
        let store = Arc::new(InMemoryBlockStore::new());
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta = meta_handle.layer();
        let backend = Arc::new(Backend::new(store, meta.clone()));
        let ino = meta
            .create_file(1, "cached_tail_auto_age_deferral.txt".to_string())
            .await
            .unwrap();
        let inode = Inode::new(ino, 0);
        let reader = Arc::new(DataReader::new(
            Arc::new(ReadConfig::new(layout)),
            backend.clone(),
        ));
        let writer = Arc::new(DataWriter::new(test_config(layout), backend, reader, None));
        let file_writer = writer.ensure_file(inode);
        let len = layout.block_size as usize + 1024;

        file_writer
            .write_at_cached(0, &vec![4u8; len], 10)
            .await
            .unwrap();

        sleep(Duration::from_millis(50)).await;

        let before_flush = writer.dirty_breakdown().await;
        assert_eq!(
            before_flush.freeze_auto_ops, 0,
            "cached partial tails should get the same coalescing grace after full blocks are dispatched"
        );
        assert_eq!(before_flush.upload_partial_tail_auto_age_ops, 0);
        assert_eq!(before_flush.upload_partial_tail_ops, 0);

        file_writer.flush().await.unwrap();

        let after_flush = writer.dirty_breakdown().await;
        assert_eq!(after_flush.freeze_explicit_flush_ops, 1);
        assert_eq!(after_flush.upload_partial_tail_explicit_flush_ops, 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_auto_flush_defers_cached_sub_block_idle_freeze_with_bounded_grace() {
        let layout = ChunkLayout {
            chunk_size: 16 * 1024,
            block_size: 4 * 1024,
        };
        let store = Arc::new(InMemoryBlockStore::new());
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta = meta_handle.layer();
        let backend = Arc::new(Backend::new(store, meta.clone()));
        let ino = meta
            .create_file(1, "cached_idle_bounded_deferral.txt".to_string())
            .await
            .unwrap();
        let inode = Inode::new(ino, 0);
        let reader = Arc::new(DataReader::new(
            Arc::new(ReadConfig::new(layout)),
            backend.clone(),
        ));
        let writer = Arc::new(DataWriter::new(test_config(layout), backend, reader, None));
        let file_writer = writer.ensure_file(inode);

        file_writer
            .write_at_cached(0, &[4u8; 1024], 10)
            .await
            .unwrap();

        sleep(Duration::from_millis(50)).await;

        let before_flush = writer.dirty_breakdown().await;
        assert_eq!(
            before_flush.freeze_auto_ops, 0,
            "cached sub-block idle slices should keep coalescing instead of creating partial-tail uploads"
        );
        assert_eq!(before_flush.upload_partial_tail_auto_idle_ops, 0);

        file_writer.flush().await.unwrap();

        let after_flush = writer.dirty_breakdown().await;
        assert_eq!(after_flush.freeze_explicit_flush_ops, 1);
        assert_eq!(after_flush.upload_partial_tail_explicit_flush_ops, 1);
        assert_eq!(after_flush.upload_partial_tail_auto_idle_ops, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_auto_flush_too_many_defers_cached_sub_block_slices_during_grace() {
        let layout = ChunkLayout {
            chunk_size: 16 * 1024,
            block_size: 4 * 1024,
        };
        let store = Arc::new(BlockingStore::new(true));
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta = meta_handle.layer();
        let backend = Arc::new(Backend::new(store.clone(), meta.clone()));
        let ino = meta
            .create_file(1, "cached_too_many_deferral.txt".to_string())
            .await
            .unwrap();
        let inode = Inode::new(ino, 0);
        let reader = Arc::new(DataReader::new(
            Arc::new(ReadConfig::new(layout)),
            backend.clone(),
        ));
        let writer = Arc::new(DataWriter::new(test_config(layout), backend, reader, None));
        let file_writer = writer.ensure_file(inode);

        for idx in 0..(MAX_SLICES_THRESHOLD + 16) as u64 {
            file_writer
                .write_at_cached(idx * layout.chunk_size, &[idx as u8; 1024], idx + 1)
                .await
                .unwrap();
        }

        sleep(Duration::from_millis(500)).await;

        let breakdown = writer.dirty_breakdown().await;
        store.unblock();
        assert!(
            breakdown.live_slices > MAX_SLICES_THRESHOLD as u64,
            "test setup should keep enough live slices to trigger tooMany"
        );
        assert_eq!(
            breakdown.upload_partial_tail_auto_too_many_ops, 0,
            "tooMany should not force cached sub-block tails while they can still coalesce"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_auto_flush_too_many_prefers_block_sized_backlog_over_cached_sub_block_tails() {
        let layout = ChunkLayout {
            chunk_size: 16 * 1024,
            block_size: 4 * 1024,
        };
        let store = Arc::new(BlockingStore::new(true));
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta = meta_handle.layer();
        let backend = Arc::new(Backend::new(store.clone(), meta.clone()));
        let ino = meta
            .create_file(1, "cached_too_many_block_relief.txt".to_string())
            .await
            .unwrap();
        let inode = Inode::new(ino, 0);
        let reader = Arc::new(DataReader::new(
            Arc::new(ReadConfig::new(layout)),
            backend.clone(),
        ));
        let writer = Arc::new(DataWriter::new(test_config(layout), backend, reader, None));
        let file_writer = writer.ensure_file(inode);

        let block_sized_slices = MAX_SLICES_THRESHOLD + 32;
        for idx in 0..block_sized_slices as u64 {
            file_writer
                .write_at_cached(
                    idx * layout.block_size as u64 * 2,
                    &[idx as u8; 4096],
                    idx + 1,
                )
                .await
                .unwrap();
        }

        let cached_tail_base = block_sized_slices as u64 * layout.block_size as u64 * 2;
        for idx in 0..32u64 {
            file_writer
                .write_at_cached(
                    cached_tail_base + idx * layout.block_size as u64 * 2,
                    &[idx as u8; 1024],
                    idx + 10_000,
                )
                .await
                .unwrap();
        }

        sleep(Duration::from_millis(1500)).await;

        let breakdown = writer.dirty_breakdown().await;
        store.unblock();
        assert!(
            breakdown.live_slices > MAX_SLICES_THRESHOLD as u64,
            "test setup should keep slice pressure above the tooMany threshold"
        );
        assert_eq!(
            breakdown.upload_partial_tail_auto_too_many_ops, 0,
            "tooMany should drain block-sized backlog before freezing cached sub-block tails"
        );
    }

    #[tokio::test]
    async fn test_max_unflushed_keeps_sub_block_slices_writable() {
        let layout = ChunkLayout {
            chunk_size: 64 * 1024,
            block_size: 4 * 1024,
        };
        let store = Arc::new(InMemoryBlockStore::new());
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta = meta_handle.layer();
        let backend = Arc::new(Backend::new(store, meta.clone()));
        let ino = meta
            .create_file(1, "max_unflushed_sub_block.txt".to_string())
            .await
            .unwrap();
        let inode = Inode::new(ino, 0);
        let reader = Arc::new(DataReader::new(
            Arc::new(ReadConfig::new(layout)),
            backend.clone(),
        ));
        let writer = Arc::new(DataWriter::new(test_config(layout), backend, reader, None));
        let file_writer = writer.ensure_file(inode);

        for idx in 0..6u64 {
            file_writer
                .write_at(idx * 8 * 1024, &[idx as u8; 1024])
                .await
                .unwrap();
        }

        let breakdown = writer.dirty_breakdown().await;
        assert_eq!(breakdown.slice_create_ops, 6);
        assert_eq!(
            breakdown.freeze_max_unflushed_ops, 0,
            "max_unflushed should not force sub-block slices into partial-tail uploads"
        );
    }

    #[tokio::test]
    async fn test_file_writer_cross_chunks() {
        let layout = ChunkLayout {
            chunk_size: 8 * 1024,
            block_size: 4 * 1024,
        };
        let store = Arc::new(InMemoryBlockStore::new());
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let _meta_store = meta_handle.store();
        let meta = meta_handle.layer();
        let backend = Arc::new(Backend::new(store.clone(), meta.clone()));
        let ino = meta
            .create_file(1, "cross_chunks.txt".to_string())
            .await
            .unwrap();
        let inode = Inode::new(ino, 0);

        let reader_cfg = Arc::new(ReadConfig::new(layout));
        let reader = Arc::new(DataReader::new(reader_cfg, backend.clone()));
        let writer = FileWriter::new(
            inode.clone(),
            test_config(layout),
            backend.clone(),
            reader.clone(),
            Arc::new(AtomicU64::new(0)),
            None,
        );

        let len = layout.chunk_size as usize + 1024;
        let mut data = vec![0u8; len];
        for (i, b) in data.iter_mut().enumerate() {
            *b = (i % 251) as u8;
        }

        writer.write_at(0, &data).await.unwrap();
        writer.flush().await.unwrap();

        let file_reader = reader.open_for_handle(inode, 1);
        let out = file_reader.read(0, len).await.unwrap();
        assert_eq!(out, data);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_auto_flush_does_not_freeze_empty_slice() {
        let layout = ChunkLayout {
            chunk_size: 8 * 1024,
            block_size: 4 * 1024,
        };
        let store = Arc::new(InMemoryBlockStore::new());
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta = meta_handle.layer();
        let backend = Arc::new(Backend::new(store.clone(), meta.clone()));
        let ino = meta
            .create_file(1, "empty_auto_flush.txt".to_string())
            .await
            .unwrap();
        let inode = Inode::new(ino, 0);
        let reader = Arc::new(DataReader::new(
            Arc::new(ReadConfig::new(layout)),
            backend.clone(),
        ));
        let writer = FileWriter::new(
            inode.clone(),
            test_config(layout),
            backend.clone(),
            reader,
            Arc::new(AtomicU64::new(0)),
            None,
        );

        let cid = chunk_id_for(inode.ino(), 0).unwrap();
        let slice = Arc::new(ParkingMutex::new(SliceState::new(
            cid,
            0,
            test_config(layout),
            Arc::new(AtomicU64::new(0)),
            None,
            0,
        )));
        {
            let mut guard = writer.shared.inner.lock().await;
            let mut chunk = ChunkState::new(cid);
            chunk.slices.push_back(slice.clone());
            guard.chunks.insert(cid, chunk);
        }

        sleep(AUTO_FLUSH_MAX_AGE + Duration::from_millis(30)).await;

        assert!(
            matches!(slice.lock().state, SliceStatus::Writable),
            "auto_flush must not freeze an empty slice before write_at appends data"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_flush_commits_empty_slice_without_waiting() {
        let layout = ChunkLayout {
            chunk_size: 8 * 1024,
            block_size: 4 * 1024,
        };
        let store = Arc::new(InMemoryBlockStore::new());
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta = meta_handle.layer();
        let backend = Arc::new(Backend::new(store.clone(), meta.clone()));
        let ino = meta
            .create_file(1, "empty_flush.txt".to_string())
            .await
            .unwrap();
        let inode = Inode::new(ino, 0);
        let reader = Arc::new(DataReader::new(
            Arc::new(ReadConfig::new(layout)),
            backend.clone(),
        ));
        let writer = FileWriter::new(
            inode.clone(),
            test_config(layout),
            backend,
            reader,
            Arc::new(AtomicU64::new(0)),
            None,
        );

        let cid = chunk_id_for(inode.ino(), 0).unwrap();
        let slice = Arc::new(ParkingMutex::new(SliceState::new(
            cid,
            0,
            test_config(layout),
            Arc::new(AtomicU64::new(0)),
            None,
            0,
        )));
        {
            let mut guard = writer.shared.inner.lock().await;
            let mut chunk = ChunkState::new(cid);
            chunk.slices.push_back(slice.clone());
            chunk.commit_started = true;
            guard.chunks.insert(cid, chunk);
        }

        timeout(Duration::from_millis(200), writer.flush())
            .await
            .expect("flush should not block on empty slices")
            .unwrap();

        assert!(
            matches!(slice.lock().state, SliceStatus::Committed),
            "flush should mark an empty slice committed instead of waiting forever"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_flush_blocks_write_until_upload_done() {
        let layout = ChunkLayout {
            chunk_size: 8 * 1024,
            block_size: 4 * 1024,
        };
        let store = Arc::new(BlockingStore::new(true));
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let _meta_store = meta_handle.store();
        let meta = meta_handle.layer();
        let backend = Arc::new(Backend::new(store.clone(), meta.clone()));
        let ino = meta
            .create_file(1, "flush_blocking.txt".to_string())
            .await
            .unwrap();
        let inode = Inode::new(ino, 0);

        let reader = Arc::new(DataReader::new(
            Arc::new(ReadConfig::new(layout)),
            backend.clone(),
        ));
        let writer = Arc::new(FileWriter::new(
            inode,
            test_config(layout),
            backend.clone(),
            reader,
            Arc::new(AtomicU64::new(0)),
            None,
        ));

        let data = vec![3u8; 2048];
        writer.write_at(0, &data).await.unwrap();

        let flush_task = {
            let w = writer.clone();
            tokio::spawn(async move { w.flush().await })
        };
        sleep(Duration::from_millis(20)).await;
        assert!(!flush_task.is_finished());

        let write_task = {
            let w = writer.clone();
            let buf = vec![4u8; 512];
            tokio::spawn(async move { w.write_at(0, &buf).await })
        };
        sleep(Duration::from_millis(20)).await;
        assert!(!write_task.is_finished());

        store.unblock();

        timeout(Duration::from_secs(1), flush_task)
            .await
            .expect("flush should finish")
            .unwrap()
            .unwrap();
        timeout(Duration::from_secs(1), write_task)
            .await
            .expect("write should finish")
            .unwrap()
            .unwrap();
    }

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

        let first_flush = timeout(Duration::from_secs(2), file_writer.flush())
            .await
            .expect("initial flush should not hang after injected upload failure");
        let err = match first_flush {
            Ok(()) => timeout(Duration::from_secs(2), async {
                loop {
                    match file_writer.flush().await {
                        Ok(()) => sleep(Duration::from_millis(10)).await,
                        Err(err) => break err,
                    }
                }
            })
            .await
            .expect("upload failure should become visible to a later flush"),
            Err(err) => err,
        };
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_flush_reports_upload_failure() {
        let layout = ChunkLayout {
            chunk_size: 8 * 1024,
            block_size: 4 * 1024,
        };
        let store = Arc::new(FailingStore);
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta = meta_handle.layer();
        let backend = Arc::new(Backend::new(store, meta.clone()));
        let ino = meta
            .create_file(1, "flush_upload_failure.txt".to_string())
            .await
            .unwrap();
        let inode = Inode::new(ino, 0);
        let reader = Arc::new(DataReader::new(
            Arc::new(ReadConfig::new(layout)),
            backend.clone(),
        ));
        let writer = FileWriter::new(
            inode,
            test_config(layout),
            backend.clone(),
            reader,
            Arc::new(AtomicU64::new(0)),
            None,
        );

        writer.write_at(0, &[9u8; 2048]).await.unwrap();

        let err = timeout(Duration::from_secs(2), writer.flush())
            .await
            .expect("flush should return the upload error promptly")
            .expect_err("upload failure must not be reported as a successful flush");

        assert!(
            err.to_string().contains("writeback failed"),
            "unexpected flush error: {err:?}"
        );
        assert!(
            writer.has_pending().await,
            "writeback error should remain observable by later flush/fsync/close calls"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_best_effort_persist_runs_concurrently_with_upload() {
        let start = Instant::now();
        let (persist_result, upload_result) = join_best_effort_persist(
            Some(async {
                sleep(Duration::from_millis(120)).await;
                anyhow::Ok(())
            }),
            async {
                sleep(Duration::from_millis(40)).await;
                Ok::<usize, anyhow::Error>(7usize)
            },
        )
        .await;

        assert!(
            start.elapsed() < Duration::from_millis(150),
            "persist and upload should overlap instead of running sequentially"
        );
        assert!(persist_result.unwrap().is_ok());
        assert_eq!(upload_result.unwrap(), 7);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_writeback_stage_completes_before_upload_starts() {
        let stage_done = Arc::new(AtomicBool::new(false));
        let upload_saw_stage_done = Arc::new(AtomicBool::new(false));

        let stage_done_for_persist = stage_done.clone();
        let stage_done_for_upload = stage_done.clone();
        let upload_saw_stage_done_for_upload = upload_saw_stage_done.clone();

        let start = Instant::now();
        let (persist_result, upload_result) = join_writeback_stage_then_upload(
            async move {
                sleep(Duration::from_millis(120)).await;
                stage_done_for_persist.store(true, Ordering::SeqCst);
                anyhow::Ok(())
            },
            async move {
                upload_saw_stage_done_for_upload.store(
                    stage_done_for_upload.load(Ordering::SeqCst),
                    Ordering::SeqCst,
                );
                Ok::<usize, anyhow::Error>(7usize)
            },
        )
        .await;

        assert!(
            start.elapsed() >= Duration::from_millis(120),
            "writeback stage-first helper should wait for staging before upload"
        );
        assert!(persist_result.is_ok());
        assert_eq!(upload_result.unwrap(), 7);
        assert!(
            upload_saw_stage_done.load(Ordering::SeqCst),
            "upload must not start before writeback stage is complete"
        );
    }

    #[test]
    fn test_stage_first_upload_is_limited_to_cached_writeback() {
        assert!(should_stage_first_writeback_upload(
            UploadPriority::Writeback,
            true,
            WriteOriginKind::CachedOnly
        ));
        assert!(!should_stage_first_writeback_upload(
            UploadPriority::Writeback,
            true,
            WriteOriginKind::NormalOnly
        ));
        assert!(!should_stage_first_writeback_upload(
            UploadPriority::Writeback,
            true,
            WriteOriginKind::Mixed
        ));
        assert!(!should_stage_first_writeback_upload(
            UploadPriority::Foreground,
            true,
            WriteOriginKind::CachedOnly
        ));
        assert!(!should_stage_first_writeback_upload(
            UploadPriority::Writeback,
            false,
            WriteOriginKind::CachedOnly
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_flush_reports_non_retryable_meta_failure() {
        let layout = ChunkLayout {
            chunk_size: 8 * 1024,
            block_size: 4 * 1024,
        };
        let store = Arc::new(InMemoryBlockStore::new());
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let writable_meta = meta_handle.layer();
        let read_only_meta = MetaClient::with_options(
            meta_handle.store(),
            CacheCapacity::default(),
            CacheTtl::for_sqlite(),
            MetaClientOptions {
                read_only: true,
                no_background_jobs: true,
                ..Default::default()
            },
        );
        let backend = Arc::new(Backend::new(store, read_only_meta));
        let ino = writable_meta
            .create_file(1, "flush_meta_failure.txt".to_string())
            .await
            .unwrap();
        let inode = Inode::new(ino, 0);
        let reader = Arc::new(DataReader::new(
            Arc::new(ReadConfig::new(layout)),
            backend.clone(),
        ));
        let writer = FileWriter::new(
            inode,
            test_config(layout),
            backend.clone(),
            reader,
            Arc::new(AtomicU64::new(0)),
            None,
        );

        let cid = chunk_id_for(ino, 0).unwrap();
        let slice = Arc::new(ParkingMutex::new(SliceState::new(
            cid,
            0,
            test_config(layout),
            Arc::new(AtomicU64::new(0)),
            None,
            0,
        )));
        {
            let mut state = slice.lock();
            state.data.append(&[5u8; 2048]).unwrap();
            state.data.freeze();
            state.slice_id = Some(7);
            state.state = SliceStatus::Uploaded;
        }
        {
            let mut guard = writer.shared.inner.lock().await;
            let mut chunk = ChunkState::new(cid);
            chunk.commit_started = true;
            chunk.slices.push_back(slice);
            guard.chunks.insert(cid, chunk);
        }

        let shared = writer.shared.clone();
        tokio::spawn(async move { FileWriter::commit_chunk(shared, cid).await });

        let err = timeout(Duration::from_secs(2), writer.flush())
            .await
            .expect("flush should return the metadata error promptly")
            .expect_err("metadata commit failure must not be reported as a successful flush");

        assert!(
            err.to_string()
                .contains("metadata commit failed with non-retryable error"),
            "unexpected flush error: {err:?}"
        );
    }

    #[tokio::test]
    async fn test_background_flush_all_commits() {
        let layout = ChunkLayout {
            chunk_size: 8 * 1024,
            block_size: 4 * 1024,
        };
        let block_store = Arc::new(InMemoryBlockStore::new());
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta_store = meta_handle.store();
        let meta = meta_handle.layer();
        let backend = Arc::new(Backend::new(block_store.clone(), meta.clone()));

        let reader = Arc::new(DataReader::new(
            Arc::new(ReadConfig::new(layout)),
            backend.clone(),
        ));
        let write_cfg = Arc::new(
            WriteConfig::new(layout)
                .page_size(4 * 1024)
                .flush_all_interval(Duration::from_millis(50)),
        );
        let writer_pool = Arc::new(DataWriter::new(write_cfg, backend.clone(), reader, None));
        writer_pool.start_flush_background();

        let ino = meta
            .create_file(1, "background_flush.txt".to_string())
            .await
            .unwrap();
        let inode = Inode::new(ino, 0);
        let writer = writer_pool.ensure_file(inode.clone());
        let data = vec![7u8; 1024];
        writer.write_at(0, &data).await.unwrap();

        let cid = chunk_id_for(inode.ino(), 0).unwrap();
        timeout(Duration::from_secs(1), async {
            loop {
                if !meta_store.get_slices(cid).await.unwrap().is_empty() {
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("flush-all should commit");
    }

    #[tokio::test]
    async fn test_background_flush_does_not_force_active_cached_sub_block() {
        let layout = ChunkLayout {
            chunk_size: 8 * 1024,
            block_size: 4 * 1024,
        };
        let block_store = Arc::new(InMemoryBlockStore::new());
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta = meta_handle.layer();
        let backend = Arc::new(Backend::new(block_store, meta.clone()));
        let reader = Arc::new(DataReader::new(
            Arc::new(ReadConfig::new(layout)),
            backend.clone(),
        ));
        let write_cfg = Arc::new(
            WriteConfig::new(layout)
                .page_size(4 * 1024)
                .flush_all_interval(Duration::from_millis(20)),
        );
        let writer_pool = Arc::new(DataWriter::new(write_cfg, backend, reader, None));
        writer_pool.start_flush_background();

        let ino = meta
            .create_file(1, "background_no_active_flush.txt".to_string())
            .await
            .unwrap();
        let writer = writer_pool.ensure_file(Inode::new(ino, 0));
        writer.write_at_cached(0, &[9u8; 1024], 1).await.unwrap();

        sleep(Duration::from_millis(80)).await;

        let breakdown = writer_pool.dirty_breakdown().await;
        assert_eq!(
            breakdown.freeze_explicit_flush_ops, 0,
            "background maintenance must not force active cached writers through explicit flush"
        );
        assert!(
            writer.has_pending().await,
            "active cached sub-block data should wait for explicit flush/close"
        );
    }
}
