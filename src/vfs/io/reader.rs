// Read pipeline (high-level):
// - FileReader::read_at splits a file read into chunk spans.
// - read_chunk_span reads through BlockStore/DataFetcher so all data is served by
//   the unified cache layer; committed demand reads do not create FileReader
//   SliceState reservations.
// - Writer commit calls DataReader::invalidate(...) to mark slice metadata stale.

use crate::chunk::reader::DataFetcher;
use crate::chunk::{BlockStore, ChunkLayout};
use crate::meta::MetaLayer;
use crate::utils::NumCastExt;
use crate::vfs::Inode;
use crate::vfs::backend::Backend;
use crate::vfs::chunk_id_for;
use crate::vfs::config::ReadConfig;
use crate::vfs::io::split_chunk_spans;
use crate::vfs::memory::{MemoryBudget, PressureLevel};
use dashmap::{DashMap, Entry};
use futures_util::stream::{FuturesUnordered, StreamExt};
use parking_lot::Mutex as ParkingMutex;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::{Mutex, Notify};
use tokio::time::Instant;
use tracing::Instrument;

const DEFAULT_TOTAL_AHEAD_LIMIT: u64 = 256 * 1024 * 1024;
const READ_SESSIONS: usize = 2;
const MAX_SLICE_READ_RETRIES: u32 = 5;

fn is_transient_read_error(e: &anyhow::Error) -> bool {
    let msg = format!("{e:?}").to_lowercase();
    msg.contains("timeout")
        || msg.contains("connection reset")
        || msg.contains("connection refused")
        || msg.contains("temporary failure")
        || msg.contains("eagain")
        || msg.contains("broken pipe")
        || msg.contains("request canceled")
}

fn retry_delay(attempt: u32) -> Duration {
    let attempt = attempt.saturating_add(1);
    Duration::from_millis(u64::from((attempt * attempt * 10).min(1000)))
}

fn align_up_to(value: u64, align: u64) -> u64 {
    if align == 0 {
        return value;
    }
    value.div_ceil(align).saturating_mul(align)
}

#[allow(clippy::type_complexity)]
pub(crate) struct DataReader<B, M> {
    config: Arc<ReadConfig>,
    /// Per-handle readers, grouped by inode
    files: DashMap<u64, Vec<(u64, Arc<FileReader<B, M>>)>>, // ino -> (fh, reader)
    backend: Arc<Backend<B, M>>,
    prefetcher: Option<Arc<dyn crate::vfs::cache::prefetch::Prefetcher>>,
    memory_budget: Option<MemoryBudget>,
}

impl<B, M> DataReader<B, M>
where
    B: BlockStore + Send + Sync + 'static,
    M: MetaLayer + Send + Sync + 'static,
{
    pub(crate) fn new(config: Arc<ReadConfig>, backend: Arc<Backend<B, M>>) -> Self {
        Self {
            config,
            files: DashMap::new(),
            backend,
            prefetcher: None,
            memory_budget: None,
        }
    }

    pub(crate) fn with_prefetcher(
        mut self,
        prefetcher: Arc<dyn crate::vfs::cache::prefetch::Prefetcher>,
    ) -> Self {
        self.prefetcher = Some(prefetcher);
        self
    }

    pub(crate) fn with_memory_budget(mut self, memory_budget: MemoryBudget) -> Self {
        self.memory_budget = Some(memory_budget);
        self
    }

    pub(crate) fn open_for_handle(&self, ino: Arc<Inode>, fh: u64) -> Arc<FileReader<B, M>> {
        let ino_number = ino.ino();
        let reader = Arc::new(FileReader::new(
            self.config.clone(),
            ino,
            self.backend.clone(),
            self.memory_budget.clone(),
        ));

        self.files
            .entry(ino_number as u64)
            .or_default()
            .push((fh, reader.clone()));
        reader
    }

    pub(crate) async fn close_for_handle(&self, ino: u64, fh: u64) {
        if let Some(prefetcher) = &self.prefetcher {
            prefetcher.cancel_for_handle(ino as i64, fh).await;
        }

        let removed = if let Entry::Occupied(mut entry) = self.files.entry(ino) {
            let mut removed = Vec::new();
            let list = entry.get_mut();

            list.retain(|(id, reader)| {
                if *id == fh {
                    removed.push(reader.clone());
                    false
                } else {
                    true
                }
            });

            if list.is_empty() {
                entry.remove();
            }

            removed
        } else {
            Vec::new()
        };

        for reader in removed {
            reader.invalidate_all().await;
        }
    }

    /// Submit a prefetch task for the range following a completed read.
    /// Called by the VFS after each successful read to warm the cache.
    pub(crate) fn submit_prefetch(&self, ino: i64, fh: u64, offset: u64, read_len: u64) {
        if let Some(prefetcher) = &self.prefetcher {
            if self
                .memory_budget
                .as_ref()
                .is_some_and(|budget| budget.pressure_level() >= PressureLevel::Critical)
            {
                return;
            }

            use crate::vfs::cache::prefetch::{PrefetchPriority, PrefetchTask};
            let Some(reader) = self.reader_for_handle(ino as u64, fh) else {
                return;
            };
            let Some((ahead_start, ahead_len)) = reader.prefetch_range_after_read(offset, read_len)
            else {
                return;
            };
            let p = prefetcher.clone();
            let task = PrefetchTask {
                ino,
                start: ahead_start,
                len: ahead_len,
                priority: PrefetchPriority::Sequential,
                owner_fh: fh,
            };
            tokio::spawn(async move { p.submit(task).await });
        }
    }

    #[allow(dead_code)]
    pub(crate) fn reader_for_handle(&self, ino: u64, fh: u64) -> Option<Arc<FileReader<B, M>>> {
        self.files.get(&ino).and_then(|entry| {
            entry
                .iter()
                .find(|(id, _)| *id == fh)
                .map(|(_, reader)| reader.clone())
        })
    }

    fn collect_readers(&self, ino: u64) -> Vec<Arc<FileReader<B, M>>> {
        match self.files.get(&ino) {
            Some(entry) => entry.iter().map(|(_, reader)| reader.clone()).collect(),
            None => vec![],
        }
    }

    pub(crate) async fn invalidate(&self, ino: u64, offset: u64, len: usize) -> anyhow::Result<()> {
        for reader in self.collect_readers(ino) {
            reader.invalidate(offset, len).await;
        }
        Ok(())
    }

    pub(crate) async fn invalidate_all(&self, ino: u64) {
        for reader in self.collect_readers(ino) {
            reader.invalidate_all().await;
        }
    }
}

/// A Session tracks the read pattern of a specific handle to guide slice eviction.
///
/// There are 4 fields:
/// 1. `ahead`: possible readahead length.
/// 2. `last_off`: the offset of the last read operation.
/// 3. `total`: total read length of the session.
/// 4. `atime`: the last time.
///
/// According to the Principle of Locality, when a range is read, its adjacent ranges are
/// likely to be read soon. The session records the last read offset and predicts a readahead range.
/// It uses this pattern to evaluate slice utility:
/// slices outside the predicted range are treated as "useless" and will be cleaned to satisfy the buffer size limit.
///
/// A slice `[start, end]` is considered "useful" if it falls within the window:
/// `[last_off - backward_tolerance, last_off + forward_prediction]`
/// where:
/// - `backward_tolerance = max(ahead / 8, block_size)`
/// - `forward_prediction = 2 * ahead + 2 * block_size`
///
/// This windows reflects an aggressive forward readahead strategy while remaining tolerant of small backward seeks.
///
/// To adapt to larger sequential reads, the `ahead` length is doubled whenever the total read length reaches the current
/// `ahead` threshold, effectively expanding the predictive window. In contract, it reduces by half to adapt smaller reads.
///
/// A handle generally maintains two independent Sessions to support concurrent read patterns.
/// This is particularly beneficial for interleaved `pread` operations, as it allows the system to track
/// two separate read streams simultaneously without their predictive windows interfering with each other.
///
/// If these two sessions are both available, it selects the oldest (atime).
#[derive(Clone, Copy)]
struct Session {
    ahead: u64,
    last_off: u64,
    total: u64,
    atime: Instant,
}

impl Default for Session {
    fn default() -> Self {
        Self {
            ahead: 0,
            last_off: 0,
            total: 0,
            atime: Instant::now(),
        }
    }
}

impl Session {
    fn reset(&mut self, off: u64, _len: u64) {
        self.last_off = off;
        self.total = 0;
        self.ahead = 0;
        self.atime = Instant::now();
    }

    fn update(&mut self, off: u64, len: u64) {
        let end = off + len;
        if end > self.last_off {
            self.total += end - self.last_off;
            self.last_off = end;
        }
        self.atime = Instant::now();
    }

    fn window(&self, block_size: u64) -> (u64, u64) {
        let back = (self.ahead / 8).max(block_size);

        let win_start = self.last_off.saturating_sub(back);
        let win_end = self
            .last_off
            .saturating_add(self.ahead.saturating_mul(2))
            .saturating_add(block_size.saturating_mul(2));
        (win_start, win_end)
    }

    fn update_ahead(
        &mut self,
        block_size: u64,
        max_ahead: u64,
        total_ahead_limit: u64,
        usage: u64,
        offset: u64,
        len: u64,
    ) {
        let mut ahead = self.ahead;

        if ahead == 0 && block_size <= max_ahead && (offset == 0 || self.total > len) {
            // Start with 2 blocks to immediately fill the pipeline.
            ahead = block_size.saturating_mul(2).min(max_ahead);
        } else if ahead < max_ahead
            && self.total >= ahead
            && total_ahead_limit > usage.saturating_add(ahead.saturating_mul(4))
        {
            ahead = ahead.saturating_mul(2).min(max_ahead);
        } else if ahead >= block_size
            && (total_ahead_limit < usage.saturating_add(ahead / 2) || self.total < ahead / 4)
        {
            ahead /= 2;
        }

        self.ahead = ahead;
    }
}

#[derive(Copy, Clone)]
enum SliceStatus {
    /// Created and fetching has not yet begun.
    New = 0,
    /// Fetching data
    Busy,
    /// Data is ready
    Ready,
    /// Data is stale and may be recycled
    Invalid,
    /// Refreshing data
    Refresh,
}

struct SliceState {
    /// Chunk index it belongs to
    index: u64,
    /// Range it contains
    range: (u64, u64),
    state: SliceStatus,
    err: Option<String>,
    notify: Arc<Notify>,
    /// Generation count
    generation: u64,
    /// Reference count
    refs: u16,
    /// Queue delay (milliseconds) before the fetch task actually started.
    queue_delay_ms: Option<u64>,
    /// Fetch duration (milliseconds) for the last successful/failed attempt.
    fetch_ms: Option<u64>,
    /// Last access time for eviction decisions.
    last_access: Instant,
}

impl SliceState {
    fn in_flight(&self) -> bool {
        matches!(
            self.state,
            SliceStatus::Refresh | SliceStatus::New | SliceStatus::Busy
        )
    }

    fn range_to_file(&self, chunk_size: u64) -> (u64, u64) {
        let base = self.index * chunk_size;
        (base + self.range.0, base + self.range.1)
    }

    fn overlaps(&self, offset: u64, len: u64) -> bool {
        let end = offset.saturating_add(len);
        self.range.0 < end && offset < self.range.1
    }

    fn background_fetch<B, M>(
        this: Arc<ParkingMutex<SliceState>>,
        ino: u64,
        layout: ChunkLayout,
        backend: Arc<Backend<B, M>>,
    ) where
        B: BlockStore + Send + Sync + 'static,
        M: MetaLayer + Send + Sync + 'static,
    {
        let queued_at = Instant::now();

        tokio::spawn(async move {
            let start_at = Instant::now();
            let queue_delay_ms = start_at.duration_since(queued_at).as_millis() as u64;
            let (index, (start, end), generation) = {
                let mut guard = this.lock();
                match guard.state {
                    SliceStatus::Busy | SliceStatus::Invalid => {
                        return;
                    }
                    _ => {
                        guard.state = SliceStatus::Busy;
                    }
                }
                guard.queue_delay_ms = Some(queue_delay_ms);
                guard.fetch_ms = None;
                (guard.index, guard.range, guard.generation)
            };

            let chunk_id = match chunk_id_for(ino as i64, index) {
                Ok(id) => id,
                Err(err) => {
                    let mut guard = this.lock();
                    guard.state = SliceStatus::Invalid;
                    guard.err = Some(err.to_string());
                    guard.notify.notify_waiters();
                    return;
                }
            };
            let f = || async {
                let mut fetcher = DataFetcher::new(layout, chunk_id, &backend);
                fetcher.prepare_slices().await?;

                let out = fetcher
                    .read_at(start.into(), (end - start).as_usize())
                    .await?;
                Ok::<_, anyhow::Error>(out)
            };

            let mut result = f().await;
            for attempt in 0..MAX_SLICE_READ_RETRIES.saturating_sub(1) {
                let should_retry = match &result {
                    Ok(_) => false,
                    Err(err) => is_transient_read_error(err),
                };
                if !should_retry {
                    break;
                }

                let _ = backend
                    .meta()
                    .invalidate_chunk_slices(ino as i64, index)
                    .await;
                tokio::time::sleep(retry_delay(attempt)).await;
                result = f().await;
            }
            let fetch_ms = start_at.elapsed().as_millis() as u64;
            let mut guard = this.lock();

            // Stale fetch and needs to drop.
            if guard.generation != generation {
                return;
            }

            guard.fetch_ms = Some(fetch_ms);
            match result {
                Ok(_) => {
                    guard.state = SliceStatus::Ready;
                    guard.err = None;
                }
                Err(e) => {
                    guard.state = SliceStatus::Invalid;
                    guard.err = Some(e.to_string());
                }
            }
            guard.notify.notify_waiters();
        });
    }
}

pub(crate) struct FileReader<B, M> {
    config: Arc<ReadConfig>,
    inode: Arc<Inode>,
    slices: Mutex<VecDeque<Arc<ParkingMutex<SliceState>>>>,
    sessions: ParkingMutex<[Session; READ_SESSIONS]>,
    backend: Arc<Backend<B, M>>,
    memory_budget: Option<MemoryBudget>,
    /// Per-chunk slice metadata cache — avoids repeated meta.get_slices()
    /// (Redis / InodeCache) queries for sequential reads within the same
    /// 64 MiB chunk.  Invalidated when the writer commits new slices.
    chunk_slices: DashMap<u64, Arc<Vec<crate::chunk::SliceDesc>>>,
    /// Reads-since-last-cleanup counter.  clean_evictable_slices scans the
    /// entire slice list (O(n)) so we amortize it over many reads.
    read_count: AtomicU64,
}

impl<B, M> FileReader<B, M>
where
    B: BlockStore + Send + Sync + 'static,
    M: MetaLayer + Send + Sync + 'static,
{
    pub(crate) fn new(
        config: Arc<ReadConfig>,
        inode: Arc<Inode>,
        backend: Arc<Backend<B, M>>,
        memory_budget: Option<MemoryBudget>,
    ) -> Self {
        Self {
            config,
            inode,
            slices: Mutex::new(VecDeque::new()),
            sessions: ParkingMutex::new([Session::default(); READ_SESSIONS]),
            backend,
            memory_budget,
            chunk_slices: DashMap::new(),
            read_count: AtomicU64::new(0),
        }
    }

    pub(crate) async fn read(&self, offset: u64, len: usize) -> anyhow::Result<Vec<u8>> {
        if len == 0 {
            return Ok(Vec::new());
        }

        self.read_at(offset, len).await
    }

    fn select_forward_session_match(
        &self,
        sessions: &[Session; READ_SESSIONS],
        offset: u64,
    ) -> Option<usize> {
        let sat = |s: &Session, offset: u64| {
            s.last_off <= offset
                && offset <= s.last_off + s.ahead + self.config.layout.block_size as u64
        };

        let max_off = if sessions[0].last_off > sessions[1].last_off {
            0
        } else {
            1
        };

        if sat(&sessions[max_off], offset) {
            return Some(max_off);
        }
        if sat(&sessions[1 - max_off], offset) {
            return Some(1 - max_off);
        }
        None
    }

    fn select_back_session_match(
        &self,
        sessions: &[Session; READ_SESSIONS],
        offset: u64,
    ) -> Option<usize> {
        let sat = |s: &Session, offset: u64| {
            let back = (s.ahead / 8).max(self.config.layout.block_size as u64);
            offset < s.last_off && offset >= s.last_off.saturating_sub(back)
        };

        let min_off = if sessions[0].last_off < sessions[1].last_off {
            0
        } else {
            1
        };

        if sat(&sessions[min_off], offset) {
            return Some(min_off);
        }
        if sat(&sessions[1 - min_off], offset) {
            return Some(1 - min_off);
        }
        None
    }

    fn select_session_fallback(
        &self,
        sessions: &mut [Session; READ_SESSIONS],
        offset: u64,
        len: usize,
    ) -> usize {
        if sessions[0].total == 0 {
            sessions[0].reset(offset, len as u64);
            return 0;
        }
        if sessions[1].total == 0 {
            sessions[1].reset(offset, len as u64);
            return 1;
        }

        let oldest_atime = if sessions[0].atime < sessions[1].atime {
            0
        } else {
            1
        };
        sessions[oldest_atime].reset(offset, len as u64);
        oldest_atime
    }

    fn check_session(&self, offset: u64, len: usize) -> u64 {
        let mut session = self.sessions.lock();

        let selected = self
            .select_forward_session_match(&session, offset)
            .or_else(|| self.select_back_session_match(&session, offset))
            .unwrap_or(self.select_session_fallback(&mut session, offset, len));

        session[selected].update(offset, len as u64);
        session[selected].update_ahead(
            self.config.layout.block_size as u64,
            self.max_ahead(),
            self.total_ahead_limit(),
            0,
            offset,
            len as u64,
        );
        session[selected].ahead
    }

    fn total_ahead_limit(&self) -> u64 {
        let limit = if self.config.buffer_size > 0 {
            self.config.buffer_size * 8 / 10
        } else {
            DEFAULT_TOTAL_AHEAD_LIMIT
        };
        self.apply_readahead_factor(limit)
    }

    fn apply_readahead_factor(&self, value: u64) -> u64 {
        let Some(budget) = &self.memory_budget else {
            return value;
        };
        if value == 0 {
            return 0;
        }
        let factor = budget.readahead_factor();
        if factor >= 1.0 {
            return value;
        }
        let block_size = self.config.layout.block_size as u64;
        ((value as f64 * factor).ceil() as u64)
            .max(block_size.min(value))
            .min(value)
    }

    fn max_ahead(&self) -> u64 {
        self.apply_readahead_factor(self.config.max_ahead)
            .min(self.total_ahead_limit())
    }

    fn max_slice_amount(&self) -> usize {
        // Allow each session to keep approximately `max_ahead / block_size` slices.
        self.max_ahead()
            .saturating_div(self.config.layout.block_size as u64)
            .saturating_mul(READ_SESSIONS as u64)
            .saturating_add(1) as usize
    }

    fn prefetch_range_after_read(&self, offset: u64, read_len: u64) -> Option<(u64, u64)> {
        if read_len == 0 {
            return None;
        }

        let block_size = self.config.layout.block_size as u64;
        let read_end = offset.checked_add(read_len)?;
        let file_size = self.inode.file_size();
        if read_end >= file_size {
            return None;
        }

        let ahead = {
            let sessions = self.sessions.lock();
            sessions
                .iter()
                .filter(|session| {
                    session.total > 0 && session.last_off == read_end && session.ahead >= block_size
                })
                .map(|session| session.ahead)
                .max()
        }?;

        let ahead_start = align_up_to(read_end, block_size);
        if ahead_start >= file_size {
            return None;
        }

        let requested = read_len.max(block_size).min(ahead);
        let remaining = file_size - ahead_start;
        let ahead_len = requested.min(remaining);
        let ahead_end = align_up_to(ahead_start.saturating_add(ahead_len), block_size);
        let ahead_len = ahead_end.min(file_size).saturating_sub(ahead_start);
        (ahead_len > 0).then_some((ahead_start, ahead_len))
    }

    async fn clean_evictable_slices(&self, offset: u64, len: usize) {
        let sessions = *self.sessions.lock();
        let windows = sessions
            .iter()
            .filter(|s| s.total > 0)
            .map(|s| s.window(self.config.layout.block_size as u64))
            .collect::<Vec<_>>();

        let slice_limit = self.max_slice_amount();

        let cur_start = offset;
        let cur_end = offset + len as u64;
        let now = Instant::now();

        let mut guard = self.slices.lock().await;
        let mut cnt = 0_usize;

        guard.retain(|s| {
            let state = s.lock();

            let (slice_start, slice_end) = state.range_to_file(self.config.layout.chunk_size);

            let overlaps_current = slice_start < cur_end && cur_start < slice_end;
            let needed_by_session = windows
                .iter()
                .any(|(win_start, win_end)| slice_start < *win_end && *win_start < slice_end);
            let expired = now.duration_since(state.last_access) > Duration::from_secs(30);

            let mut keep = true;
            if (matches!(state.state, SliceStatus::Invalid) && state.refs == 0)
                || (!overlaps_current
                    && (expired || !needed_by_session)
                    && state.refs == 0
                    && !state.in_flight())
            {
                keep = false;
            }

            if keep && !overlaps_current {
                cnt = cnt.saturating_add(1);
            }

            keep
        });

        if cnt > slice_limit {
            guard.retain(|s| {
                let state = s.lock();

                let (slice_start, slice_end) = state.range_to_file(self.config.layout.chunk_size);
                let overlaps_current = slice_start < cur_end && cur_start < slice_end;

                if !overlaps_current && cnt > slice_limit && state.refs == 0 && !state.in_flight() {
                    cnt = cnt.saturating_sub(1);
                    return false;
                }
                true
            })
        }
    }

    async fn back_pressure(&self) -> anyhow::Result<()> {
        if let Some(budget) = &self.memory_budget {
            let level = budget.pressure_level();
            if level >= PressureLevel::High {
                budget.log_state();
                tokio::task::yield_now().await;
            }
        }
        Ok(())
    }

    pub(crate) async fn read_at(&self, offset: u64, len: usize) -> anyhow::Result<Vec<u8>> {
        if len == 0 {
            return Ok(Vec::new());
        }

        let file_size = self.inode.file_size();
        if file_size <= offset {
            return Ok(Vec::new());
        }

        let actual_len = std::cmp::min(len, file_size as usize - offset as usize);
        if actual_len == 0 {
            return Ok(Vec::new());
        }

        // Evict stale slices every N reads.  Both cleanup paths scan the full
        // slice list, so keep them out of the per-read hot path.
        // 4 MiB read adds ~10-50 µs of overhead that adds up at 46 reads/sec.
        let should_clean = self
            .read_count
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1)
            .is_multiple_of(64);
        if should_clean {
            self.clean_evictable_slices(offset, actual_len)
                .instrument(tracing::trace_span!(
                    "read_at.clean_evictable_slices",
                    offset,
                    len = actual_len
                ))
                .await;
        }
        self.back_pressure()
            .instrument(tracing::trace_span!("read_at.back_pressure"))
            .await?;

        let spans = tracing::trace_span!("read_at.split_spans", offset, len = actual_len)
            .in_scope(|| split_chunk_spans(self.config.layout, offset, actual_len));

        // Read demand data first — do not synchronously submit readahead
        // before the foreground read.  The GlobalPrefetcher (VFS layer)
        // handles asynchronous readahead after each successful read.
        let _ahead = self.check_session(offset, actual_len);

        if spans.len() == 1 {
            let span = spans[0];
            let mut data = vec![0; actual_len];
            let result = self
                .read_chunk_span_into(span.index, span.offset, &mut data)
                .instrument(tracing::trace_span!(
                    "read_at.read_single_span_into",
                    index = span.index,
                    offset = span.offset,
                    len = actual_len
                ))
                .await;

            if should_clean {
                self.cleanup_invalid()
                    .instrument(tracing::trace_span!("read_at.cleanup_invalid"))
                    .await;
            }

            return result.map(|_| data);
        }

        let mut chunks: Vec<Option<bytes::Bytes>> = vec![None; spans.len()];
        let result = async {
            let mut reads = FuturesUnordered::new();
            for (idx, span) in spans.into_iter().enumerate() {
                let span_len = span.len.as_usize();
                reads.push(async move {
                    let data = self
                        .read_chunk_span(span.index, span.offset, span_len)
                        .await?;
                    Ok::<_, anyhow::Error>((idx, data))
                });
            }

            while let Some(res) = reads.next().await {
                let (idx, data) = res?;
                chunks[idx] = Some(data);
            }

            Ok::<_, anyhow::Error>(actual_len)
        }
        .instrument(tracing::trace_span!("read_at.read_spans"))
        .await;

        // Assemble Bytes chunks into output
        let data = if result.is_ok() {
            let total: usize = chunks
                .iter()
                .filter_map(|chunk| chunk.as_ref())
                .map(|chunk| chunk.len())
                .sum();
            let mut out = Vec::with_capacity(total);
            for chunk in chunks.iter().filter_map(|chunk| chunk.as_ref()) {
                out.extend_from_slice(chunk);
            }
            out
        } else {
            Vec::new()
        };

        if should_clean {
            self.cleanup_invalid()
                .instrument(tracing::trace_span!("read_at.cleanup_invalid"))
                .await;
        }
        result.map(|_| data)
    }

    // Read one chunk span through DataFetcher using the per-handle chunk→slice
    // metadata cache to skip repeated meta queries within the same chunk.
    async fn read_chunk_span(
        &self,
        index: u64,
        offset: u64,
        len: usize,
    ) -> anyhow::Result<bytes::Bytes> {
        let mut data = vec![0; len];
        self.read_chunk_span_into(index, offset, &mut data).await?;
        Ok(bytes::Bytes::from(data))
    }

    async fn read_chunk_span_into(
        &self,
        index: u64,
        offset: u64,
        buf: &mut [u8],
    ) -> anyhow::Result<()> {
        let chunk_id = chunk_id_for(self.inode.ino(), index)?;

        for attempt in 0..MAX_SLICE_READ_RETRIES {
            let result = async {
                let slices_arc = match self.chunk_slices.get(&chunk_id) {
                    Some(cached) => cached.clone(),
                    None => {
                        let mut fetcher =
                            DataFetcher::new(self.config.layout, chunk_id, &self.backend);
                        fetcher.prepare_slices().await?;
                        let slices = fetcher.into_slices();
                        let arc = Arc::new(slices);
                        self.chunk_slices.insert(chunk_id, arc.clone());
                        arc
                    }
                };

                let mut fetcher = DataFetcher::with_slices(
                    self.config.layout,
                    chunk_id,
                    &self.backend,
                    (*slices_arc).clone(),
                );
                fetcher.read_at_into(offset.into(), buf).await
            }
            .await;

            match result {
                Ok(()) => return Ok(()),
                Err(err)
                    if attempt + 1 < MAX_SLICE_READ_RETRIES && is_transient_read_error(&err) =>
                {
                    self.chunk_slices.remove(&chunk_id);
                    let _ = self
                        .backend
                        .meta()
                        .invalidate_chunk_slices(self.inode.ino(), index)
                        .await;
                    tokio::time::sleep(retry_delay(attempt)).await;
                }
                Err(err) => return Err(err),
            }
        }

        unreachable!("read_chunk_span retry loop should return before exhausting attempts")
    }

    async fn invalidate(&self, offset: u64, len: usize) {
        if len == 0 {
            return;
        }

        let spans = split_chunk_spans(self.config.layout, offset, len);

        // Invalidate per-handle chunk→slice metadata cache for affected chunks
        // so subsequent reads re-fetch the updated slice list from meta.
        for span in &spans {
            if let Ok(chunk_id) = chunk_id_for(self.inode.ino(), span.index) {
                self.chunk_slices.remove(&chunk_id);
            }
        }

        let mut span_map = HashMap::new();
        for span in spans {
            span_map.insert(span.index, (span.offset, span.len));
        }

        let mut to_fetch = Vec::new();
        let mut new_slices = VecDeque::new();

        {
            let mut guard = self.slices.lock().await;
            for slice in guard.drain(..) {
                let mut state = slice.lock();
                let Some((span_offset, span_len)) = span_map.get(&state.index) else {
                    new_slices.push_back(slice.clone());
                    continue;
                };
                if !state.overlaps(*span_offset, *span_len) {
                    new_slices.push_back(slice.clone());
                    continue;
                }

                state.generation += 1;

                match state.state {
                    SliceStatus::Ready => {
                        if state.refs > 0 {
                            state.state = SliceStatus::Refresh;
                            to_fetch.push(slice.clone());
                        } else {
                            state.state = SliceStatus::Invalid;
                        }
                    }
                    SliceStatus::Busy | SliceStatus::New | SliceStatus::Refresh => {
                        state.state = SliceStatus::Refresh;
                        to_fetch.push(slice.clone());
                    }
                    SliceStatus::Invalid => {}
                }
                state.notify.notify_waiters();

                if !matches!(state.state, SliceStatus::Invalid) || state.refs > 0 {
                    new_slices.push_back(slice.clone());
                }
            }
            *guard = new_slices;
        }

        // Invalidated slices must be re-fetched.
        for slice in to_fetch {
            SliceState::background_fetch(
                slice,
                self.inode.ino() as u64,
                self.config.layout,
                self.backend.clone(),
            );
        }
    }

    async fn invalidate_all(&self) {
        self.chunk_slices.clear();
        let mut guard = self.slices.lock().await;
        for slice in guard.drain(..) {
            let mut state = slice.lock();
            state.generation = state.generation.saturating_add(1);
            state.state = SliceStatus::Invalid;
            state.notify.notify_waiters();
        }
    }

    /// Clean all invalid and unused slices.
    async fn cleanup_invalid(&self) {
        let mut guard = self.slices.lock().await;
        guard.retain(|slice| {
            let state = slice.lock();
            !(matches!(state.state, SliceStatus::Invalid) && state.refs == 0)
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunk::store::{BlockKey, BlockStore, InMemoryBlockStore};
    use crate::chunk::writer::DataUploader;
    use crate::chunk::{ChunkLayout, SliceDesc};
    use crate::meta::MetaLayer;
    use crate::meta::SLICE_ID_KEY;
    use crate::meta::factory::create_meta_store_from_url;
    use crate::meta::store::MetaStore;
    use crate::vfs::Inode;
    use crate::vfs::cache::prefetch::{PrefetchTask, Prefetcher};
    use crate::vfs::config::{ReadConfig, WriteConfig};
    use crate::vfs::io::writer::FileWriter;
    use bytes::Bytes;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::time::{sleep, timeout};

    fn small_layout() -> ChunkLayout {
        ChunkLayout {
            chunk_size: 8 * 1024,
            block_size: 4 * 1024,
        }
    }

    #[derive(Default)]
    struct CapturePrefetcher {
        tasks: StdMutex<Vec<PrefetchTask>>,
    }

    impl CapturePrefetcher {
        fn tasks(&self) -> Vec<PrefetchTask> {
            self.tasks.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl Prefetcher for CapturePrefetcher {
        async fn submit(&self, task: PrefetchTask) {
            self.tasks.lock().unwrap().push(task);
        }

        async fn cancel_for_handle(&self, _ino: i64, _fh: u64) {}
    }

    #[tokio::test]
    async fn test_file_reader_cross_chunks() {
        let layout = small_layout();
        let block_store = Arc::new(InMemoryBlockStore::new());
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta_store = meta_handle.store();
        let meta = meta_handle.layer();
        let backend = Arc::new(Backend::new(block_store.clone(), meta.clone()));

        let ino: i64 = 11;
        let offset = layout.chunk_size - 512;
        let data = vec![9u8; 2048];
        let head = &data[..512];
        let tail = &data[512..];

        let slice_id1 = meta_store.next_id(SLICE_ID_KEY).await.unwrap();
        let uploader = DataUploader::new(layout, backend.as_ref());
        uploader
            .write_at_vectored(
                slice_id1 as u64,
                0u64.into(),
                &[bytes::Bytes::copy_from_slice(head)],
            )
            .await
            .unwrap();
        meta_store
            .append_slice(
                chunk_id_for(ino, 0).unwrap(),
                SliceDesc {
                    slice_id: slice_id1 as u64,
                    chunk_id: chunk_id_for(ino, 0).unwrap(),
                    offset,
                    length: head.len() as u64,
                },
            )
            .await
            .unwrap();

        let slice_id2 = meta_store.next_id(SLICE_ID_KEY).await.unwrap();
        let uploader = DataUploader::new(layout, backend.as_ref());
        uploader
            .write_at_vectored(
                slice_id2 as u64,
                0u64.into(),
                &[Bytes::copy_from_slice(tail)],
            )
            .await
            .unwrap();
        meta_store
            .append_slice(
                chunk_id_for(ino, 1).unwrap(),
                SliceDesc {
                    slice_id: slice_id2 as u64,
                    chunk_id: chunk_id_for(ino, 1).unwrap(),
                    offset: 0,
                    length: tail.len() as u64,
                },
            )
            .await
            .unwrap();

        let inode = Inode::new(ino, offset + data.len() as u64);
        let reader = DataReader::new(Arc::new(ReadConfig::new(layout)), backend.clone());
        let file_reader = reader.open_for_handle(inode, 1);
        let out = file_reader.read(offset, data.len()).await.unwrap();
        assert_eq!(out, data);
    }

    #[tokio::test]
    async fn test_reader_invalidate_refresh() {
        let layout = ChunkLayout {
            chunk_size: 16 * 1024,
            block_size: 4 * 1024,
        };
        let block_store = Arc::new(InMemoryBlockStore::new());
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta_store = meta_handle.store();
        let meta = meta_handle.layer();
        let backend = Arc::new(Backend::new(block_store.clone(), meta.clone()));

        let ino: i64 = 22;
        let data1 = vec![1u8; 2048];
        let data2 = vec![2u8; 2048];

        let slice_id1 = meta_store.next_id(SLICE_ID_KEY).await.unwrap();
        let uploader = DataUploader::new(layout, backend.as_ref());
        uploader
            .write_at_vectored(
                slice_id1 as u64,
                0u64.into(),
                &[Bytes::copy_from_slice(&data1)],
            )
            .await
            .unwrap();
        meta_store
            .append_slice(
                chunk_id_for(ino, 0).unwrap(),
                SliceDesc {
                    slice_id: slice_id1 as u64,
                    chunk_id: chunk_id_for(ino, 0).unwrap(),
                    offset: 0,
                    length: data1.len() as u64,
                },
            )
            .await
            .unwrap();

        let inode = Inode::new(ino, data1.len() as u64);
        let reader = DataReader::new(Arc::new(ReadConfig::new(layout)), backend.clone());
        let file_reader = reader.open_for_handle(inode, 1);
        let out1 = file_reader.read(0, data1.len()).await.unwrap();
        assert_eq!(out1, data1);

        let slice_id2 = meta_store.next_id(SLICE_ID_KEY).await.unwrap();
        uploader
            .write_at_vectored(
                slice_id2 as u64,
                0u64.into(),
                &[Bytes::copy_from_slice(&data2)],
            )
            .await
            .unwrap();
        meta_store
            .append_slice(
                chunk_id_for(ino, 0).unwrap(),
                SliceDesc {
                    slice_id: slice_id2 as u64,
                    chunk_id: chunk_id_for(ino, 0).unwrap(),
                    offset: 0,
                    length: data2.len() as u64,
                },
            )
            .await
            .unwrap();

        reader.invalidate(ino as u64, 0, data2.len()).await.unwrap();
        let out2 = file_reader.read(0, data2.len()).await.unwrap();
        assert_eq!(out2, data2);
    }

    #[tokio::test]
    async fn test_readahead_starts_after_current_read() {
        let layout = ChunkLayout {
            chunk_size: 16 * 1024,
            block_size: 4 * 1024,
        };
        let block_store = Arc::new(InMemoryBlockStore::new());
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta_store = meta_handle.store();
        let meta = meta_handle.layer();
        let backend = Arc::new(Backend::new(block_store.clone(), meta.clone()));

        let ino: i64 = 33;
        let data = vec![7u8; (layout.block_size * 3) as usize];
        let slice_id = meta_store.next_id(SLICE_ID_KEY).await.unwrap();
        let uploader = DataUploader::new(layout, backend.as_ref());
        uploader
            .write_at_vectored(
                slice_id as u64,
                0u64.into(),
                &[Bytes::copy_from_slice(&data)],
            )
            .await
            .unwrap();
        meta_store
            .append_slice(
                chunk_id_for(ino, 0).unwrap(),
                SliceDesc {
                    slice_id: slice_id as u64,
                    chunk_id: chunk_id_for(ino, 0).unwrap(),
                    offset: 0,
                    length: data.len() as u64,
                },
            )
            .await
            .unwrap();

        let inode = Inode::new(ino, data.len() as u64);
        let config = Arc::new(
            ReadConfig::new(layout)
                .buffer_size(64 * 1024)
                .max_ahead(layout.block_size as u64 * 2),
        );
        let reader = DataReader::new(config, backend.clone());
        let file_reader = reader.open_for_handle(inode, 1);

        let out = file_reader
            .read(0, layout.block_size as usize)
            .await
            .unwrap();
        assert_eq!(out, data[..layout.block_size as usize]);

        tokio::time::sleep(Duration::from_millis(20)).await;

        let ranges = {
            let guard = file_reader.slices.lock().await;
            guard
                .iter()
                .map(|slice| slice.lock().range)
                .collect::<Vec<_>>()
        };

        assert!(
            ranges.is_empty(),
            "demand reads should not retain FileReader slice state; ranges={ranges:?}"
        );
    }

    #[tokio::test]
    async fn test_first_nonzero_read_does_not_start_readahead() {
        let layout = small_layout();
        let block_store = Arc::new(InMemoryBlockStore::new());
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta = meta_handle.layer();
        let backend = Arc::new(Backend::new(block_store, meta));
        let inode = Inode::new(77, layout.chunk_size as u64 * 8);
        let reader = DataReader::new(
            Arc::new(ReadConfig::new(layout).max_ahead(layout.block_size as u64 * 4)),
            backend,
        );
        let file_reader = reader.open_for_handle(inode, 1);

        let first_offset = layout.block_size as u64 * 3;
        let first_ahead = file_reader.check_session(first_offset, 512);
        assert_eq!(
            first_ahead, 0,
            "a first read at a non-zero offset is random until a contiguous follow-up read confirms a stream"
        );

        let second_ahead = file_reader.check_session(first_offset + 512, 512);
        assert!(
            second_ahead >= layout.block_size as u64,
            "the next contiguous read should enable readahead for the detected stream"
        );
    }

    #[tokio::test]
    async fn test_submit_prefetch_requires_confirmed_stream_and_aligns_to_next_block() {
        let layout = small_layout();
        let block_store = Arc::new(InMemoryBlockStore::new());
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta = meta_handle.layer();
        let backend = Arc::new(Backend::new(block_store, meta));
        let capture = Arc::new(CapturePrefetcher::default());
        let ino = 78;
        let fh = 2;
        let inode = Inode::new(ino, layout.chunk_size as u64 * 8);
        let reader = DataReader::new(
            Arc::new(ReadConfig::new(layout).max_ahead(layout.block_size as u64 * 4)),
            backend,
        )
        .with_prefetcher(capture.clone());
        let file_reader = reader.open_for_handle(inode, fh);

        let first_offset = layout.block_size as u64 * 3;
        let read_len = 512;
        assert_eq!(file_reader.check_session(first_offset, read_len), 0);
        reader.submit_prefetch(ino, fh, first_offset, read_len as u64);
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            capture.tasks().is_empty(),
            "a first non-zero offset read should not schedule speculative readahead"
        );

        let second_offset = first_offset + read_len as u64;
        let ahead = file_reader.check_session(second_offset, read_len);
        assert!(ahead >= layout.block_size as u64);
        reader.submit_prefetch(ino, fh, second_offset, read_len as u64);
        tokio::time::sleep(Duration::from_millis(20)).await;

        let tasks = capture.tasks();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].start, layout.block_size as u64 * 4);
        assert_eq!(tasks[0].len, layout.block_size as u64);
    }

    #[derive(Default)]
    struct FlakyBlockStore {
        data: StdMutex<HashMap<BlockKey, Vec<u8>>>,
        read_attempts: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl BlockStore for FlakyBlockStore {
        async fn write_fresh_range(
            &self,
            key: BlockKey,
            offset: u64,
            data: &[u8],
        ) -> anyhow::Result<u64> {
            let mut guard = self.data.lock().unwrap();
            let entry = guard.entry(key).or_default();
            let start = offset as usize;
            let end = start + data.len();
            if entry.len() < end {
                entry.resize(end, 0);
            }
            entry[start..end].copy_from_slice(data);
            Ok(data.len() as u64)
        }

        async fn read_range(
            &self,
            key: BlockKey,
            offset: u64,
            buf: &mut [u8],
        ) -> anyhow::Result<()> {
            let attempt = self.read_attempts.fetch_add(1, Ordering::SeqCst) + 1;
            if attempt <= 2 {
                anyhow::bail!("timeout reading test block");
            }

            let guard = self.data.lock().unwrap();
            if let Some(src) = guard.get(&key) {
                let start = offset as usize;
                let end = (start + buf.len()).min(src.len());
                if start < end {
                    buf[..end - start].copy_from_slice(&src[start..end]);
                }
            }
            Ok(())
        }

        async fn delete_range(&self, key: BlockKey, block_count: u64) -> anyhow::Result<()> {
            let mut guard = self.data.lock().unwrap();
            for block in key.1..key.1 + block_count as u32 {
                guard.remove(&(key.0, block));
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_slice_read_retries_transient_failures() {
        let layout = ChunkLayout {
            chunk_size: 8 * 1024,
            block_size: 4 * 1024,
        };
        let block_store = Arc::new(FlakyBlockStore::default());
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta_store = meta_handle.store();
        let meta = meta_handle.layer();
        let backend = Arc::new(Backend::new(block_store.clone(), meta.clone()));

        let ino: i64 = 44;
        let data = vec![9u8; 2048];
        let slice_id = meta_store.next_id(SLICE_ID_KEY).await.unwrap();
        block_store
            .write_fresh_range((slice_id as u64, 0), 0, &data)
            .await
            .unwrap();
        meta_store
            .append_slice(
                chunk_id_for(ino, 0).unwrap(),
                SliceDesc {
                    slice_id: slice_id as u64,
                    chunk_id: chunk_id_for(ino, 0).unwrap(),
                    offset: 0,
                    length: data.len() as u64,
                },
            )
            .await
            .unwrap();

        let inode = Inode::new(ino, data.len() as u64);
        let reader = DataReader::new(Arc::new(ReadConfig::new(layout)), backend.clone());
        let file_reader = reader.open_for_handle(inode, 1);

        let out = file_reader.read(0, data.len()).await.unwrap();
        assert_eq!(out, data);
        assert!(
            block_store.read_attempts.load(Ordering::SeqCst) >= 3,
            "transient failures should be retried before the read succeeds"
        );
    }

    #[derive(Default)]
    struct CountingBlockStore {
        data: StdMutex<HashMap<BlockKey, Vec<u8>>>,
        read_attempts: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl BlockStore for CountingBlockStore {
        async fn write_fresh_range(
            &self,
            key: BlockKey,
            offset: u64,
            data: &[u8],
        ) -> anyhow::Result<u64> {
            let mut guard = self.data.lock().unwrap();
            let entry = guard.entry(key).or_default();
            let start = offset as usize;
            let end = start + data.len();
            if entry.len() < end {
                entry.resize(end, 0);
            }
            entry[start..end].copy_from_slice(data);
            Ok(data.len() as u64)
        }

        async fn read_range(
            &self,
            key: BlockKey,
            offset: u64,
            buf: &mut [u8],
        ) -> anyhow::Result<()> {
            self.read_attempts.fetch_add(1, Ordering::SeqCst);
            let guard = self.data.lock().unwrap();
            if let Some(src) = guard.get(&key) {
                let start = offset as usize;
                let end = (start + buf.len()).min(src.len());
                if start < end {
                    buf[..end - start].copy_from_slice(&src[start..end]);
                }
            }
            Ok(())
        }

        async fn delete_range(&self, key: BlockKey, block_count: u64) -> anyhow::Result<()> {
            let mut guard = self.data.lock().unwrap();
            for block in key.1..key.1 + block_count as u32 {
                guard.remove(&(key.0, block));
            }
            Ok(())
        }
    }

    struct DelayedBlockStore {
        data: StdMutex<HashMap<BlockKey, Vec<u8>>>,
        read_delay: Duration,
        active_reads: AtomicUsize,
        max_active_reads: AtomicUsize,
    }

    impl DelayedBlockStore {
        fn new(read_delay: Duration) -> Self {
            Self {
                data: StdMutex::new(HashMap::new()),
                read_delay,
                active_reads: AtomicUsize::new(0),
                max_active_reads: AtomicUsize::new(0),
            }
        }

        fn max_active_reads(&self) -> usize {
            self.max_active_reads.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl BlockStore for DelayedBlockStore {
        async fn write_fresh_range(
            &self,
            key: BlockKey,
            offset: u64,
            data: &[u8],
        ) -> anyhow::Result<u64> {
            let mut guard = self.data.lock().unwrap();
            let entry = guard.entry(key).or_default();
            let start = offset as usize;
            let end = start + data.len();
            if entry.len() < end {
                entry.resize(end, 0);
            }
            entry[start..end].copy_from_slice(data);
            Ok(data.len() as u64)
        }

        async fn read_range(
            &self,
            key: BlockKey,
            offset: u64,
            buf: &mut [u8],
        ) -> anyhow::Result<()> {
            let active = self.active_reads.fetch_add(1, Ordering::SeqCst) + 1;
            let _ =
                self.max_active_reads
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                        Some(current.max(active))
                    });
            sleep(self.read_delay).await;
            self.active_reads.fetch_sub(1, Ordering::SeqCst);

            let guard = self.data.lock().unwrap();
            if let Some(src) = guard.get(&key) {
                let start = offset as usize;
                let end = (start + buf.len()).min(src.len());
                if start < end {
                    buf[..end - start].copy_from_slice(&src[start..end]);
                }
            }
            Ok(())
        }

        async fn delete_range(&self, key: BlockKey, block_count: u64) -> anyhow::Result<()> {
            let mut guard = self.data.lock().unwrap();
            for block in key.1..key.1 + block_count as u32 {
                guard.remove(&(key.0, block));
            }
            Ok(())
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_cross_chunk_read_fetches_chunks_concurrently() {
        let layout = ChunkLayout {
            chunk_size: 4 * 1024,
            block_size: 4 * 1024,
        };
        let block_store = Arc::new(DelayedBlockStore::new(Duration::from_millis(50)));
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta_store = meta_handle.store();
        let meta = meta_handle.layer();
        let backend = Arc::new(Backend::new(block_store.clone(), meta.clone()));

        let ino: i64 = 67;
        let mut expected = Vec::new();
        for chunk_index in 0..4 {
            let data = vec![chunk_index as u8 + 1; layout.chunk_size as usize];
            expected.extend_from_slice(&data);

            let slice_id = meta_store.next_id(SLICE_ID_KEY).await.unwrap();
            block_store
                .write_fresh_range((slice_id as u64, 0), 0, &data)
                .await
                .unwrap();
            meta_store
                .append_slice(
                    chunk_id_for(ino, chunk_index).unwrap(),
                    SliceDesc {
                        slice_id: slice_id as u64,
                        chunk_id: chunk_id_for(ino, chunk_index).unwrap(),
                        offset: 0,
                        length: data.len() as u64,
                    },
                )
                .await
                .unwrap();
        }

        let inode = Inode::new(ino, expected.len() as u64);
        let reader = DataReader::new(Arc::new(ReadConfig::new(layout)), backend.clone());
        let file_reader = reader.open_for_handle(inode, 1);

        let out = file_reader.read(0, expected.len()).await.unwrap();

        assert_eq!(out, expected);
        assert!(
            block_store.max_active_reads() > 1,
            "cross-chunk reads should overlap block fetches"
        );
    }

    #[tokio::test]
    async fn test_demand_read_does_not_double_fetch_current_slice() {
        let layout = ChunkLayout {
            chunk_size: 8 * 1024,
            block_size: 4 * 1024,
        };
        let block_store = Arc::new(CountingBlockStore::default());
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta_store = meta_handle.store();
        let meta = meta_handle.layer();
        let backend = Arc::new(Backend::new(block_store.clone(), meta.clone()));

        let ino: i64 = 65;
        let data = vec![5u8; 2048];
        let slice_id = meta_store.next_id(SLICE_ID_KEY).await.unwrap();
        block_store
            .write_fresh_range((slice_id as u64, 0), 0, &data)
            .await
            .unwrap();
        meta_store
            .append_slice(
                chunk_id_for(ino, 0).unwrap(),
                SliceDesc {
                    slice_id: slice_id as u64,
                    chunk_id: chunk_id_for(ino, 0).unwrap(),
                    offset: 0,
                    length: data.len() as u64,
                },
            )
            .await
            .unwrap();

        let inode = Inode::new(ino, data.len() as u64);
        let reader = DataReader::new(Arc::new(ReadConfig::new(layout)), backend.clone());
        let file_reader = reader.open_for_handle(inode, 1);

        assert_eq!(file_reader.read(0, data.len()).await.unwrap(), data);
        assert_eq!(
            block_store.read_attempts.load(Ordering::SeqCst),
            1,
            "current demand reads should not background-fetch then foreground-read the same slice"
        );
    }

    #[tokio::test]
    async fn test_repeated_slice_read_goes_through_block_store() {
        let layout = ChunkLayout {
            chunk_size: 8 * 1024,
            block_size: 4 * 1024,
        };
        let block_store = Arc::new(CountingBlockStore::default());
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta_store = meta_handle.store();
        let meta = meta_handle.layer();
        let backend = Arc::new(Backend::new(block_store.clone(), meta.clone()));

        let ino: i64 = 66;
        let data = vec![6u8; 2048];
        let slice_id = meta_store.next_id(SLICE_ID_KEY).await.unwrap();
        block_store
            .write_fresh_range((slice_id as u64, 0), 0, &data)
            .await
            .unwrap();
        meta_store
            .append_slice(
                chunk_id_for(ino, 0).unwrap(),
                SliceDesc {
                    slice_id: slice_id as u64,
                    chunk_id: chunk_id_for(ino, 0).unwrap(),
                    offset: 0,
                    length: data.len() as u64,
                },
            )
            .await
            .unwrap();

        let inode = Inode::new(ino, data.len() as u64);
        let reader = DataReader::new(Arc::new(ReadConfig::new(layout)), backend.clone());
        let file_reader = reader.open_for_handle(inode, 1);

        assert_eq!(file_reader.read(0, data.len()).await.unwrap(), data);
        let after_first = block_store.read_attempts.load(Ordering::SeqCst);

        assert_eq!(file_reader.read(0, data.len()).await.unwrap(), data);
        let after_second = block_store.read_attempts.load(Ordering::SeqCst);

        assert!(
            after_second > after_first,
            "repeated reads must route through BlockStore/ChunksCache instead of copying SliceState.page"
        );
    }

    fn ranges_cover(ranges: &[(u64, u64)], start: u64, end: u64) -> bool {
        let mut ranges = ranges.to_vec();
        ranges.sort_by_key(|range| range.0);
        let mut cursor = start;
        for (left, right) in ranges {
            if right <= cursor {
                continue;
            }
            if left > cursor {
                return false;
            }
            cursor = cursor.max(right);
            if cursor >= end {
                return true;
            }
        }
        false
    }

    // Tail prefetch is now handled asynchronously by the GlobalPrefetcher at the
    // VFS layer (fs/mod.rs), not by FileReader::read_at.

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_read_while_write_eventually_sees_data() {
        let layout = ChunkLayout {
            chunk_size: 8 * 1024,
            block_size: 4 * 1024,
        };
        let block_store = Arc::new(InMemoryBlockStore::new());
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta = meta_handle.layer();
        let backend = Arc::new(Backend::new(block_store.clone(), meta.clone()));

        let ino = meta
            .create_file(1, "reader_write_eventual.txt".to_string())
            .await
            .unwrap();
        let data = vec![5u8; 2048];
        let inode = Inode::new(ino, 0);

        let reader = Arc::new(DataReader::new(
            Arc::new(ReadConfig::new(layout)),
            backend.clone(),
        ));
        let file_reader = reader.open_for_handle(inode.clone(), 1);

        let writer = Arc::new(FileWriter::new(
            inode,
            Arc::new(WriteConfig::new(layout).page_size(4 * 1024)),
            backend.clone(),
            reader,
            Arc::new(AtomicU64::new(0)),
            None,
        ));

        let write_task = {
            let w = writer.clone();
            let payload = data.clone();
            tokio::spawn(async move {
                w.write_at(0, &payload).await.unwrap();
                w.flush().await.unwrap();
            })
        };

        timeout(Duration::from_secs(1), async {
            loop {
                let out = file_reader.read(0, data.len()).await.unwrap();
                if out == data {
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("reader should eventually see flushed data");

        write_task.await.unwrap();
    }
}
