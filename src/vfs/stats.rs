//! Real-time performance statistics collector for BrewFS.
//!
//! Provides atomic counters for FUSE operations, metadata ops, S3 object
//! traffic, and buffer usage. Metrics are exposed via a `.stats` virtual
//! file at the mount root (similar to JuiceFS) in a Prometheus-compatible
//! text format.
//!
//! The `stats` CLI tool reads this file periodically to display real-time
//! throughput and latency in the terminal.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Relaxed ordering is sufficient for stats counters — we only need eventual
/// visibility, not happens-before relationships.
const ORD: Ordering = Ordering::Relaxed;

thread_local! {
    // Valid only during Future::poll; never held across an await. Include the
    // observer address so independent mounts cannot borrow each other's IDs.
    static REQUEST_POLL_CONTEXT: std::cell::Cell<Option<(usize, u64)>> = const { std::cell::Cell::new(None) };
}

struct RequestPollContextGuard(Option<(usize, u64)>);
impl Drop for RequestPollContextGuard {
    fn drop(&mut self) {
        REQUEST_POLL_CONTEXT.set(self.0);
    }
}

/// Request-level timing segments.  The values are deliberately kept as a
/// closed enum so that the detailed observer cannot grow an unbounded label
/// set on a hot FUSE path.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(usize)]
pub enum RequestSegment {
    FuseQueueWait = 0,
    FileLockWait = 1,
    ChunkLockWait = 2,
    DirtyMerge = 3,
    FlushWait = 4,
    MetadataRpcWait = 5,
    MetadataCommit = 6,
    ObjectPutQueueWait = 7,
    ObjectPut = 8,
    Reply = 9,
    Handler = 10,
    /// Wall time inside Future::poll, not a claim of on-CPU time.
    HandlerPoll = 11,
}

/// Small fixed set of operation classes for the request-level latency
/// summaries.  Keeping this separate from the FUSE opcode avoids exposing a
/// high-cardinality label on the hot path while still giving close/fsync their
/// own latency distributions.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum RequestOperation {
    Read,
    Write,
    Close,
    Fsync,
    Metadata,
}

impl RequestOperation {
    pub const ALL: [Self; 5] = [
        Self::Read,
        Self::Write,
        Self::Close,
        Self::Fsync,
        Self::Metadata,
    ];

    pub const fn name(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
            Self::Close => "close",
            Self::Fsync => "fsync",
            Self::Metadata => "metadata",
        }
    }

    fn from_opcode(opcode: u32) -> Self {
        // Linux FUSE opcode values are stable across the supported ABI.  Keep
        // this mapping local to the observer so the vendor crate does not
        // need to depend on BrewFS's statistics module.
        match opcode {
            15 => Self::Read,            // FUSE_READ
            16 => Self::Write,           // FUSE_WRITE
            18 | 25 | 29 => Self::Close, // FUSE_RELEASE, FUSE_FLUSH, FUSE_RELEASEDIR
            20 | 30 => Self::Fsync,      // FUSE_FSYNC, FUSE_FSYNCDIR
            _ => Self::Metadata,
        }
    }
}

impl RequestSegment {
    pub const ALL: [Self; 12] = [
        Self::FuseQueueWait,
        Self::FileLockWait,
        Self::ChunkLockWait,
        Self::DirtyMerge,
        Self::FlushWait,
        Self::MetadataRpcWait,
        Self::MetadataCommit,
        Self::ObjectPutQueueWait,
        Self::ObjectPut,
        Self::Reply,
        Self::Handler,
        Self::HandlerPoll,
    ];

    pub const fn name(self) -> &'static str {
        match self {
            Self::FuseQueueWait => "fuse_queue_wait",
            Self::FileLockWait => "file_lock_wait",
            Self::ChunkLockWait => "chunk_lock_wait",
            Self::DirtyMerge => "dirty_merge",
            Self::FlushWait => "flush_wait",
            Self::MetadataRpcWait => "metadata_rpc_wait",
            Self::MetadataCommit => "metadata_commit",
            Self::ObjectPutQueueWait => "object_put_queue_wait",
            Self::ObjectPut => "object_put",
            Self::Reply => "reply",
            Self::Handler => "handler",
            Self::HandlerPoll => "handler_poll",
        }
    }
}

const REQUEST_SEGMENT_COUNT: usize = RequestSegment::ALL.len();
const REQUEST_OPERATION_COUNT: usize = RequestOperation::ALL.len();
const REQUEST_HISTOGRAM_BUCKETS: usize = 32;
const REQUEST_TRACE_RECORD_LIMIT: usize = 256;
const REQUEST_INTERVAL_LIMIT: usize = 128;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RequestSegmentSnapshot {
    pub count: u64,
    pub total_ns: u64,
    pub buckets: [u64; REQUEST_HISTOGRAM_BUCKETS],
}

/// Point-in-time state for a wait domain.  The queue state is kept separate
/// from the latency histogram because a request can leave a queue before its
/// segment timer is rendered in a stats snapshot.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RequestQueueSnapshot {
    pub depth: u64,
    pub oldest_age_ns: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RequestObservationSnapshot {
    pub instance_id: String,
    pub enabled: bool,
    pub sample_rate: u64,
    pub received: u64,
    pub sampled: u64,
    pub closed: u64,
    pub cancelled: u64,
    pub errors: u64,
    pub dropped: u64,
    pub active: u64,
    pub closure_gap_ns_total: u64,
    pub closure_gap_ns_max: u64,
    pub fuse_queue_depth: u64,
    pub fuse_queue_oldest_age_ns: u64,
    pub segments: [RequestSegmentSnapshot; REQUEST_SEGMENT_COUNT],
    pub queues: [RequestQueueSnapshot; REQUEST_SEGMENT_COUNT],
    pub operations: [RequestSegmentSnapshot; REQUEST_OPERATION_COUNT],
    pub trace_records: Vec<RequestTraceRecord>,
    pub trace_records_dropped: u64,
}

/// A bounded, point-in-time request association record.  Records are emitted
/// only when detailed observation is enabled and are intentionally kept out of
/// metric labels.  They make it possible to attribute a slow sample to the
/// file/chunk/slice/lease context without retaining an unbounded event log.
#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize)]
pub struct RequestTraceRecord {
    pub unique: u64,
    pub opcode: u32,
    pub terminal: &'static str,
    pub total_ns: u64,
    /// Wall-clock anchor plus monotonic elapsed time, for phase selection.
    pub received_at_ns: u64,
    /// Inclusive foreground durations retained for compatibility with the
    /// aggregate histograms. Only `exclusive_ns` is additive within a request.
    pub segments: [u64; REQUEST_SEGMENT_COUNT],
    /// Disjoint wall time. Handler is the residual, not an explained wait.
    pub exclusive_ns: [u64; REQUEST_SEGMENT_COUNT],
    /// Independent causal work, excluded even when it overlaps the handler.
    pub background_ns: [u64; REQUEST_SEGMENT_COUNT],
    /// [start_ns, end_ns, segment_index], relative to request receipt.
    pub intervals: Vec<[u64; 3]>,
    pub intervals_truncated: bool,
    pub ambiguous_ns: u64,
    pub file_id: Option<i64>,
    pub chunk_id: Option<u64>,
    pub slice_id: Option<u64>,
    pub lease_id: Option<u64>,
}

/// Named aliases of the inclusive foreground durations, created only while
/// exporting a snapshot. Keep background work in `background_ns` and use
/// `exclusive_ns` when adding durations within a request. This view adds no
/// storage to the bounded request ring or work to the request hot path.
#[derive(serde::Serialize)]
struct RequestTraceExport<'a> {
    #[serde(flatten)]
    record: &'a RequestTraceRecord,
    queue_wait_ns: u64,
    file_lock_wait_ns: u64,
    chunk_lock_wait_ns: u64,
    dirty_merge_ns: u64,
    flush_wait_ns: u64,
    metadata_rpc_wait_ns: u64,
    metadata_commit_ns: u64,
    object_put_queue_wait_ns: u64,
    object_put_ns: u64,
    reply_ns: u64,
}

impl RequestTraceRecord {
    fn export(&self) -> RequestTraceExport<'_> {
        RequestTraceExport {
            record: self,
            queue_wait_ns: self.segments[RequestSegment::FuseQueueWait as usize],
            file_lock_wait_ns: self.segments[RequestSegment::FileLockWait as usize],
            chunk_lock_wait_ns: self.segments[RequestSegment::ChunkLockWait as usize],
            dirty_merge_ns: self.segments[RequestSegment::DirtyMerge as usize],
            flush_wait_ns: self.segments[RequestSegment::FlushWait as usize],
            metadata_rpc_wait_ns: self.segments[RequestSegment::MetadataRpcWait as usize],
            metadata_commit_ns: self.segments[RequestSegment::MetadataCommit as usize],
            object_put_queue_wait_ns: self.segments[RequestSegment::ObjectPutQueueWait as usize],
            object_put_ns: self.segments[RequestSegment::ObjectPut as usize],
            reply_ns: self.segments[RequestSegment::Reply as usize],
        }
    }
}

#[derive(Debug)]
struct AtomicRequestSegment {
    count: AtomicU64,
    total_ns: AtomicU64,
    buckets: [AtomicU64; REQUEST_HISTOGRAM_BUCKETS],
}

impl AtomicRequestSegment {
    fn new() -> Self {
        Self {
            count: AtomicU64::new(0),
            total_ns: AtomicU64::new(0),
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }

    fn observe(&self, duration: Duration) {
        let nanos = duration.as_nanos().min(u128::from(u64::MAX)) as u64;
        let bucket = if nanos == 0 {
            0
        } else {
            (64 - (nanos - 1).leading_zeros() as usize).min(REQUEST_HISTOGRAM_BUCKETS - 1)
        };
        self.count.fetch_add(1, ORD);
        self.total_ns.fetch_add(nanos, ORD);
        self.buckets[bucket].fetch_add(1, ORD);
    }

    fn snapshot(&self) -> RequestSegmentSnapshot {
        RequestSegmentSnapshot {
            count: self.count.load(ORD),
            total_ns: self.total_ns.load(ORD),
            buckets: std::array::from_fn(|index| self.buckets[index].load(ORD)),
        }
    }
}

#[derive(Debug)]
struct ActiveRequest {
    opcode: u32,
    received_at: Instant,
    worker_at: Option<Instant>,
    handler_at: Option<Instant>,
    handler_done_at: Option<Instant>,
    segments: [u64; REQUEST_SEGMENT_COUNT],
    background_ns: [u64; REQUEST_SEGMENT_COUNT],
    intervals: Vec<[u64; 3]>,
    intervals_truncated: bool,
    file_id: Option<i64>,
    chunk_id: Option<u64>,
    slice_id: Option<u64>,
    lease_id: Option<u64>,
}

impl ActiveRequest {
    fn new(opcode: u32) -> Self {
        Self {
            opcode,
            received_at: Instant::now(),
            worker_at: None,
            handler_at: None,
            handler_done_at: None,
            segments: [0; REQUEST_SEGMENT_COUNT],
            background_ns: [0; REQUEST_SEGMENT_COUNT],
            intervals: Vec::new(),
            intervals_truncated: false,
            file_id: None,
            chunk_id: None,
            slice_id: None,
            lease_id: None,
        }
    }
}

/// Partition a single request's wall clock; never sum inclusive spans. Nested
/// intervals select the inner operation. Crossing intervals from different
/// operations are explicitly ambiguous and cannot pass the attribution gate.
fn partition_request(total: u64, intervals: &[[u64; 3]]) -> ([u64; REQUEST_SEGMENT_COUNT], u64) {
    let mut boundaries = vec![0, total];
    for &[start, end, _] in intervals {
        boundaries.extend([start.min(total), end.min(total)]);
    }
    boundaries.sort_unstable();
    boundaries.dedup();
    let mut exclusive = [0; REQUEST_SEGMENT_COUNT];
    let mut ambiguous = 0;
    for pair in boundaries.windows(2) {
        let (start, end) = (pair[0], pair[1]);
        let covering: Vec<_> = intervals
            .iter()
            .filter(|interval| interval[0] <= start && interval[1] >= end)
            .collect();
        let selected = covering.iter().min_by_key(|interval| {
            (
                interval[2] == RequestSegment::HandlerPoll as u64,
                interval[1] - interval[0],
            )
        });
        let segment = selected.map_or(RequestSegment::Handler as usize, |interval| {
            interval[2] as usize
        });
        exclusive[segment] += end - start;
        if covering.iter().enumerate().any(|(index, a)| {
            covering[index + 1..].iter().any(|b| {
                a[2] != b[2]
                    && a[2] != RequestSegment::HandlerPoll as u64
                    && b[2] != RequestSegment::HandlerPoll as u64
                    && !((a[0] <= b[0] && a[1] >= b[1]) || (b[0] <= a[0] && b[1] >= a[1]))
            })
        }) {
            ambiguous += end - start;
        }
    }
    (exclusive, ambiguous)
}

/// Optional request-level observer.  It is cheap when disabled: the session
/// stores no request state and all callbacks return immediately.
#[derive(Debug)]
pub struct RequestObservation {
    instance_id: String,
    clock_anchor: (Instant, u64),
    enabled: AtomicBool,
    sample_rate: AtomicU64,
    sample_counter: AtomicU64,
    received: AtomicU64,
    sampled: AtomicU64,
    closed: AtomicU64,
    cancelled: AtomicU64,
    errors: AtomicU64,
    dropped: AtomicU64,
    queue_token: AtomicU64,
    queue_waiters: Mutex<HashMap<u64, (RequestSegment, Instant)>>,
    closure_gap_ns_total: AtomicU64,
    closure_gap_ns_max: AtomicU64,
    active: Mutex<HashMap<u64, ActiveRequest>>,
    trace_records: Mutex<VecDeque<RequestTraceRecord>>,
    trace_records_dropped: AtomicU64,
    segments: [AtomicRequestSegment; REQUEST_SEGMENT_COUNT],
    operations: [AtomicRequestSegment; REQUEST_OPERATION_COUNT],
}

impl RequestObservation {
    fn with_request_context(self: &Arc<Self>, unique: u64, poll: &mut dyn FnMut()) {
        let _poll_timer = self.segment_timer(unique, RequestSegment::HandlerPoll);
        let _restore = RequestPollContextGuard(
            REQUEST_POLL_CONTEXT.replace(Some((Arc::as_ptr(self) as usize, unique))),
        );
        poll();
    }

    pub fn contextual_segment_timer(
        self: &Arc<Self>,
        segment: RequestSegment,
    ) -> Option<RequestSegmentTimer> {
        if !self.enabled() {
            return None;
        }
        match REQUEST_POLL_CONTEXT.get() {
            Some((owner, unique)) if owner == Arc::as_ptr(self) as usize => {
                self.segment_timer(unique, segment)
            }
            _ => self.unscoped_segment_timer(segment),
        }
    }

    /// Start a sampled request explicitly for a caller that does not use the
    /// asyncfuse session callback (for example a client-side metadata probe).
    pub fn begin(&self, unique: u64, opcode: u32) {
        self.request_received(unique, opcode);
    }

    /// Construct an observer with an explicit configuration.  Mounts use
    /// `from_environment`, while tests and small diagnostic probes can avoid
    /// process-global environment state.
    pub fn new(enabled: bool, sample_rate: u64) -> Self {
        Self::with_config(enabled, sample_rate)
    }

    fn with_config(enabled: bool, sample_rate: u64) -> Self {
        Self {
            instance_id: uuid::Uuid::new_v4().to_string(),
            clock_anchor: (
                Instant::now(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos()
                    .min(u128::from(u64::MAX)) as u64,
            ),
            enabled: AtomicBool::new(enabled),
            sample_rate: AtomicU64::new(sample_rate.max(1)),
            sample_counter: AtomicU64::new(0),
            received: AtomicU64::new(0),
            sampled: AtomicU64::new(0),
            closed: AtomicU64::new(0),
            cancelled: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            queue_token: AtomicU64::new(0),
            queue_waiters: Mutex::new(HashMap::new()),
            closure_gap_ns_total: AtomicU64::new(0),
            closure_gap_ns_max: AtomicU64::new(0),
            active: Mutex::new(HashMap::new()),
            trace_records: Mutex::new(VecDeque::with_capacity(REQUEST_TRACE_RECORD_LIMIT)),
            trace_records_dropped: AtomicU64::new(0),
            segments: std::array::from_fn(|_| AtomicRequestSegment::new()),
            operations: std::array::from_fn(|_| AtomicRequestSegment::new()),
        }
    }

    fn from_environment() -> Self {
        let enabled = std::env::var("BREWFS_REQUEST_TRACE")
            .or_else(|_| std::env::var("BREWFS_FUSE_REQUEST_TRACE"))
            .map(|value| {
                matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            .unwrap_or(false);
        let sample_rate = std::env::var("BREWFS_REQUEST_TRACE_SAMPLE_RATE")
            .ok()
            .and_then(|value| value.trim().parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(1);
        Self::with_config(enabled, sample_rate)
    }

    pub fn enabled(&self) -> bool {
        self.enabled.load(ORD)
    }

    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, ORD);
        if !enabled {
            let mut active = self
                .active
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let dropped = active.len() as u64;
            active.clear();
            self.dropped.fetch_add(dropped, ORD);
            self.queue_waiters
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .clear();
        }
    }

    pub fn set_sample_rate(&self, sample_rate: u64) {
        self.sample_rate.store(sample_rate.max(1), ORD);
    }

    fn request_received(&self, unique: u64, opcode: u32) {
        if !self.enabled() {
            return;
        }
        self.received.fetch_add(1, ORD);
        let sequence = self.sample_counter.fetch_add(1, ORD);
        if !sequence.is_multiple_of(self.sample_rate.load(ORD).max(1)) {
            // Unsampled requests intentionally have no lifecycle state. They
            // are outside the sampled closure denominator, and their later
            // worker/reply callbacks must therefore be no-ops rather than
            // fabricated dropped samples.
            return;
        }
        let mut active = self
            .active
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if active.contains_key(&unique) {
            self.errors.fetch_add(1, ORD);
            self.dropped.fetch_add(1, ORD);
            return;
        }
        active.insert(unique, ActiveRequest::new(opcode));
        self.sampled.fetch_add(1, ORD);
    }

    fn record_at(&self, timer: &RequestSegmentTimer, finished: Instant) {
        if !self.enabled() {
            return;
        }
        let unique = timer.unique.expect("scoped timer");
        let segment = timer.segment;
        let duration = finished.saturating_duration_since(timer.started);
        let nanos = duration.as_nanos().min(u128::from(u64::MAX)) as u64;
        let mut active = self
            .active
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(request) = active.get_mut(&unique) {
            if timer.background {
                request.background_ns[segment as usize] =
                    request.background_ns[segment as usize].saturating_add(nanos);
            } else {
                request.segments[segment as usize] =
                    request.segments[segment as usize].saturating_add(nanos);
                if let Some(index) = timer.interval_index {
                    request.intervals[index][1] = finished
                        .saturating_duration_since(request.received_at)
                        .as_nanos()
                        .min(u128::from(u64::MAX))
                        as u64;
                }
            }
            self.segments[segment as usize].observe(duration);
            return;
        }
        drop(active);

        // Foreground intervals are clipped at the terminal event. Only
        // explicitly causal background work may update a closed record.
        if !timer.background {
            return;
        }
        let mut records = self
            .trace_records
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(record) = records
            .iter_mut()
            .rev()
            .find(|record| record.unique == unique)
        {
            record.background_ns[segment as usize] =
                record.background_ns[segment as usize].saturating_add(nanos);
            self.segments[segment as usize].observe(duration);
        }
    }

    const fn tracks_queue(segment: RequestSegment) -> bool {
        matches!(
            segment,
            RequestSegment::FileLockWait
                | RequestSegment::ChunkLockWait
                | RequestSegment::FlushWait
                | RequestSegment::MetadataRpcWait
                | RequestSegment::ObjectPutQueueWait
        )
    }

    fn begin_queue_wait(&self, segment: RequestSegment) -> u64 {
        let token = self.queue_token.fetch_add(1, ORD).wrapping_add(1);
        self.queue_waiters
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(token, (segment, Instant::now()));
        token
    }

    fn end_queue_wait(&self, token: u64) {
        self.queue_waiters
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(&token);
    }

    fn worker_started(&self, unique: u64) {
        let now = Instant::now();
        let mut active = self
            .active
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(request) = active.get_mut(&unique) {
            request.worker_at = Some(now);
            let duration = now.saturating_duration_since(request.received_at);
            request.segments[RequestSegment::FuseQueueWait as usize] =
                duration.as_nanos().min(u128::from(u64::MAX)) as u64;
            self.segments[RequestSegment::FuseQueueWait as usize].observe(duration);
        }
    }

    fn handler_started(&self, unique: u64) {
        if let Some(request) = self
            .active
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get_mut(&unique)
        {
            request.handler_at = Some(Instant::now());
        }
    }

    fn handler_completed(&self, unique: u64) {
        let now = Instant::now();
        let mut active = self
            .active
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(request) = active.get_mut(&unique) {
            if request.handler_done_at.is_some() {
                return;
            }
            request.handler_done_at = Some(now);
            if let Some(start) = request.handler_at {
                let duration = now.saturating_duration_since(start);
                request.segments[RequestSegment::Handler as usize] =
                    duration.as_nanos().min(u128::from(u64::MAX)) as u64;
                self.segments[RequestSegment::Handler as usize].observe(duration);
            }
        }
    }

    fn completed_without_reply(&self, unique: u64) {
        let _ = self.finish(unique, "no_reply", false, false);
    }

    /// Attach bounded causal identifiers to a sampled request. They are kept
    /// in the in-memory lifecycle record and are never rendered as labels, so
    /// enabling the observer cannot create an unbounded metric cardinality.
    pub fn set_context(
        &self,
        unique: u64,
        file_id: Option<i64>,
        chunk_id: Option<u64>,
        slice_id: Option<u64>,
        lease_id: Option<u64>,
    ) -> bool {
        if !self.enabled() {
            return false;
        }
        let mut active = self
            .active
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(request) = active.get_mut(&unique) {
            request.file_id = file_id;
            request.chunk_id = chunk_id;
            request.slice_id = slice_id;
            request.lease_id = lease_id;
            return true;
        }
        drop(active);
        let mut records = self
            .trace_records
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let Some(record) = records
            .iter_mut()
            .rev()
            .find(|record| record.unique == unique)
        else {
            return false;
        };
        record.file_id = file_id;
        record.chunk_id = chunk_id;
        record.slice_id = slice_id;
        record.lease_id = lease_id;
        true
    }

    /// Add the identifiers that become known after a request has entered a
    /// lower layer. `None` leaves an existing identifier intact, which lets a
    /// writer attach a slice id without erasing the file/chunk context set by
    /// the FUSE handler. No identifier is synthesized when a layer cannot
    /// provide one.
    pub fn update_context(
        &self,
        unique: u64,
        file_id: Option<i64>,
        chunk_id: Option<u64>,
        slice_id: Option<u64>,
        lease_id: Option<u64>,
    ) -> bool {
        if !self.enabled() {
            return false;
        }
        let mut active = self
            .active
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(request) = active.get_mut(&unique) {
            if file_id.is_some() {
                request.file_id = file_id;
            }
            if chunk_id.is_some() {
                request.chunk_id = chunk_id;
            }
            if slice_id.is_some() {
                request.slice_id = slice_id;
            }
            if lease_id.is_some() {
                request.lease_id = lease_id;
            }
            return true;
        }
        drop(active);
        let mut records = self
            .trace_records
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let Some(record) = records
            .iter_mut()
            .rev()
            .find(|record| record.unique == unique)
        else {
            return false;
        };
        if file_id.is_some() {
            record.file_id = file_id;
        }
        if chunk_id.is_some() {
            record.chunk_id = chunk_id;
        }
        if slice_id.is_some() {
            record.slice_id = slice_id;
        }
        if lease_id.is_some() {
            record.lease_id = lease_id;
        }
        true
    }

    fn reply_submitted(&self, unique: u64) {
        let _ = self.finish(unique, "reply", false, false);
    }

    fn terminal(&self, unique: u64, cancelled: bool, error: bool) {
        let terminal = if cancelled { "cancelled" } else { "error" };
        let _ = self.finish(unique, terminal, cancelled, error);
    }

    fn finish(&self, unique: u64, terminal: &'static str, cancelled: bool, error: bool) -> bool {
        let now = Instant::now();
        let mut active = self
            .active
            .lock()
            .unwrap_or_else(|value| value.into_inner());
        let Some(mut request) = active.remove(&unique) else {
            return false;
        };
        // Release diagnostic state before partitioning; never hold an
        // application lock or extend handler/reply lifetimes for observation.
        drop(active);
        let total = now
            .saturating_duration_since(request.received_at)
            .as_nanos()
            .min(u128::from(u64::MAX)) as u64;
        let offset = |instant: Instant| {
            instant
                .saturating_duration_since(request.received_at)
                .as_nanos()
                .min(u128::from(u64::MAX)) as u64
        };
        for interval in &mut request.intervals {
            if interval[1] == u64::MAX {
                interval[1] = total;
            }
            // The poll surrounds dispatch and enqueue callbacks. The queue
            // and reply own those boundaries, rather than this residual span.
            if interval[2] == RequestSegment::HandlerPoll as u64 {
                interval[0] = interval[0].max(request.worker_at.map_or(0, offset));
                interval[1] = interval[1].min(request.handler_done_at.map_or(total, offset));
                interval[0] = interval[0].min(interval[1]);
            }
        }
        request.intervals.push([
            0,
            request.worker_at.map_or(total, offset),
            RequestSegment::FuseQueueWait as u64,
        ]);
        if let Some(start) = request.handler_done_at {
            let duration = now.saturating_duration_since(start);
            request.segments[RequestSegment::Reply as usize] =
                duration.as_nanos().min(u128::from(u64::MAX)) as u64;
            self.segments[RequestSegment::Reply as usize].observe(duration);
            request
                .intervals
                .push([offset(start), total, RequestSegment::Reply as u64]);
        }
        let (exclusive_ns, ambiguous_ns) = partition_request(total, &request.intervals);
        let accounted = [
            RequestSegment::FuseQueueWait,
            RequestSegment::Handler,
            RequestSegment::Reply,
        ]
        .into_iter()
        .map(|segment| request.segments[segment as usize])
        .sum::<u64>();
        let gap = total.saturating_sub(accounted);
        self.closure_gap_ns_total.fetch_add(gap, ORD);
        self.closure_gap_ns_max.fetch_max(gap, ORD);
        self.operations[RequestOperation::from_opcode(request.opcode) as usize]
            .observe(Duration::from_nanos(total));
        self.push_trace(RequestTraceRecord {
            unique,
            opcode: request.opcode,
            terminal,
            total_ns: total,
            received_at_ns: self.clock_anchor.1.saturating_add(
                request
                    .received_at
                    .saturating_duration_since(self.clock_anchor.0)
                    .as_nanos()
                    .min(u128::from(u64::MAX)) as u64,
            ),
            segments: request.segments,
            exclusive_ns,
            background_ns: request.background_ns,
            intervals: request.intervals,
            intervals_truncated: request.intervals_truncated,
            ambiguous_ns,
            file_id: request.file_id,
            chunk_id: request.chunk_id,
            slice_id: request.slice_id,
            lease_id: request.lease_id,
        });
        if cancelled {
            self.cancelled.fetch_add(1, ORD);
        }
        if error {
            self.errors.fetch_add(1, ORD);
        }
        self.closed.fetch_add(1, ORD);
        true
    }

    fn push_trace(&self, record: RequestTraceRecord) {
        let mut records = self
            .trace_records
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if records.len() == REQUEST_TRACE_RECORD_LIMIT {
            records.pop_front();
            self.trace_records_dropped.fetch_add(1, ORD);
        }
        records.push_back(record);
    }

    pub fn snapshot(&self) -> RequestObservationSnapshot {
        let now = Instant::now();
        let active_guard = self
            .active
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let active = active_guard.len() as u64;
        let mut fuse_queue_depth = 0;
        let mut fuse_queue_oldest_age_ns = 0;
        for request in active_guard
            .values()
            .filter(|request| request.worker_at.is_none())
        {
            fuse_queue_depth += 1;
            fuse_queue_oldest_age_ns = fuse_queue_oldest_age_ns.max(
                now.saturating_duration_since(request.received_at)
                    .as_nanos()
                    .min(u128::from(u64::MAX)) as u64,
            );
        }
        drop(active_guard);
        let trace_records = self
            .trace_records
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .iter()
            .cloned()
            .collect();
        RequestObservationSnapshot {
            instance_id: self.instance_id.clone(),
            enabled: self.enabled(),
            sample_rate: self.sample_rate.load(ORD).max(1),
            received: self.received.load(ORD),
            sampled: self.sampled.load(ORD),
            closed: self.closed.load(ORD),
            cancelled: self.cancelled.load(ORD),
            errors: self.errors.load(ORD),
            dropped: self.dropped.load(ORD),
            active,
            closure_gap_ns_total: self.closure_gap_ns_total.load(ORD),
            closure_gap_ns_max: self.closure_gap_ns_max.load(ORD),
            fuse_queue_depth,
            fuse_queue_oldest_age_ns,
            segments: std::array::from_fn(|index| self.segments[index].snapshot()),
            queues: {
                let queue_waiters = self
                    .queue_waiters
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                let mut queues = [RequestQueueSnapshot::default(); REQUEST_SEGMENT_COUNT];
                for (segment, started) in queue_waiters.values() {
                    let index = *segment as usize;
                    queues[index].depth = queues[index].depth.saturating_add(1);
                    queues[index].oldest_age_ns = queues[index].oldest_age_ns.max(
                        now.saturating_duration_since(*started)
                            .as_nanos()
                            .min(u128::from(u64::MAX)) as u64,
                    );
                }
                queues
            },
            operations: std::array::from_fn(|index| self.operations[index].snapshot()),
            trace_records,
            trace_records_dropped: self.trace_records_dropped.load(ORD),
        }
    }
}

/// A manually timed request segment.  It does not hold any application lock
/// across an await; dropping it only publishes an atomic histogram sample.
pub struct RequestSegmentTimer {
    observation: Arc<RequestObservation>,
    unique: Option<u64>,
    segment: RequestSegment,
    started: Instant,
    queue_token: Option<u64>,
    interval_index: Option<usize>,
    background: bool,
}

impl Drop for RequestSegmentTimer {
    fn drop(&mut self) {
        let finished = Instant::now();
        if self.unique.is_some() {
            self.observation.record_at(self, finished);
        } else {
            self.observation.record_unscoped(
                self.segment,
                finished.saturating_duration_since(self.started),
            );
        }
        if let Some(token) = self.queue_token {
            self.observation.end_queue_wait(token);
        }
    }
}

impl RequestObservation {
    pub fn segment_timer(
        self: &Arc<Self>,
        unique: u64,
        segment: RequestSegment,
    ) -> Option<RequestSegmentTimer> {
        if !self.enabled() {
            return None;
        }
        let started = Instant::now();
        let mut active = self
            .active
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let request = active.get_mut(&unique)?;
        let interval_index = if request.intervals.len() < REQUEST_INTERVAL_LIMIT {
            let index = request.intervals.len();
            let start = started
                .saturating_duration_since(request.received_at)
                .as_nanos()
                .min(u128::from(u64::MAX)) as u64;
            request.intervals.push([start, u64::MAX, segment as u64]);
            Some(index)
        } else {
            request.intervals_truncated = true;
            None
        };
        drop(active);
        Some(RequestSegmentTimer {
            observation: Arc::clone(self),
            unique: Some(unique),
            segment,
            started,
            queue_token: Self::tracks_queue(segment).then(|| self.begin_queue_wait(segment)),
            interval_index,
            background: false,
        })
    }

    /// The ID is a cause, not the owner of this asynchronous wall time.
    pub fn background_segment_timer(
        self: &Arc<Self>,
        unique: u64,
        segment: RequestSegment,
    ) -> Option<RequestSegmentTimer> {
        self.enabled().then(|| RequestSegmentTimer {
            observation: Arc::clone(self),
            unique: Some(unique),
            segment,
            started: Instant::now(),
            interval_index: None,
            background: true,
            queue_token: Self::tracks_queue(segment).then(|| self.begin_queue_wait(segment)),
        })
    }

    /// Record a segment belonging to a background task that has no live FUSE
    /// request owner. This updates the aggregate histogram without affecting
    /// sampled request closure accounting.
    pub fn unscoped_segment_timer(
        self: &Arc<Self>,
        segment: RequestSegment,
    ) -> Option<RequestSegmentTimer> {
        self.enabled().then(|| RequestSegmentTimer {
            observation: Arc::clone(self),
            unique: None,
            segment,
            started: Instant::now(),
            queue_token: Self::tracks_queue(segment).then(|| self.begin_queue_wait(segment)),
            interval_index: None,
            background: true,
        })
    }

    fn record_unscoped(&self, segment: RequestSegment, duration: Duration) {
        if self.enabled() {
            self.segments[segment as usize].observe(duration);
        }
    }
}

/// Optional mount-scoped backend statistics. Rendering must be synchronous and
/// must not issue backend requests or retain an additional cache of data.
pub trait FsStatsExtension: std::fmt::Debug + Send + Sync {
    fn begin_stats_observation(&self) -> Option<crate::cadapter::read_observer::TerminalGuard> {
        None
    }
    /// A conservative maximum for one render at the current finite state.
    fn render_max_bytes(&self) -> usize {
        64 * 1024
    }
    fn render_into(&self, output: &mut dyn std::fmt::Write);
}

#[derive(Debug, thiserror::Error)]
#[error("statistics snapshot exceeds its admitted allocation or allocation failed")]
pub struct StatsRenderLimit;

/// The owner is shared by the handle and every zero-copy FUSE reply slice.
pub(crate) struct VirtualStatsSnapshot {
    pub(crate) text: String,
    pub(crate) _output: Option<crate::meta::layer::MetadataMemoryGuard>,
    pub(crate) _control: Option<crate::meta::layer::MetadataMemoryGuard>,
}
impl AsRef<[u8]> for VirtualStatsSnapshot {
    fn as_ref(&self) -> &[u8] {
        self.text.as_bytes()
    }
}

struct StatsText {
    text: String,
    limit: Option<usize>,
    failed: bool,
}
impl StatsText {
    fn new(limit: Option<usize>) -> Self {
        Self {
            text: String::new(),
            limit,
            failed: false,
        }
    }
    fn push_str(&mut self, text: &str) {
        let _ = std::fmt::Write::write_str(self, text);
    }
    fn finish(self) -> Result<String, StatsRenderLimit> {
        if self.failed {
            Err(StatsRenderLimit)
        } else {
            Ok(self.text)
        }
    }
}
impl std::fmt::Write for StatsText {
    fn write_str(&mut self, text: &str) -> std::fmt::Result {
        let Some(next) = self.text.len().checked_add(text.len()) else {
            self.failed = true;
            return Err(std::fmt::Error);
        };
        if self.failed || self.limit.is_some_and(|limit| next > limit) {
            self.failed = true;
            return Err(std::fmt::Error);
        }
        if next > self.text.capacity()
            && self.text.try_reserve_exact(next - self.text.len()).is_err()
        {
            self.failed = true;
            return Err(std::fmt::Error);
        }
        self.text.push_str(text);
        Ok(())
    }
}

/// Point-in-time copy of the counters exposed through `.stats`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FsStatsSnapshot {
    pub uptime_seconds: u64,
    pub fuse_read_ops: u64,
    pub fuse_read_bytes: u64,
    pub fuse_read_lat_us: u64,
    pub fuse_write_ops: u64,
    pub fuse_write_bytes: u64,
    pub fuse_write_lat_us: u64,
    pub fuse_lookup_ops: u64,
    pub fuse_lookup_lat_us: u64,
    pub fuse_getattr_ops: u64,
    pub fuse_getattr_lat_us: u64,
    /// Most recently observed FUSE attribute reply validity in nanoseconds.
    pub fuse_attr_valid_ns: u64,
    /// Most recently observed FUSE entry reply validity in nanoseconds.
    pub fuse_entry_valid_ns: u64,
    pub fuse_open_ops: u64,
    pub fuse_create_ops: u64,
    pub fuse_unlink_ops: u64,
    pub fuse_readdir_ops: u64,
    pub fuse_flush_ops: u64,
    pub fuse_flush_lat_us: u64,
    pub meta_ops: u64,
    pub meta_lat_us: u64,
    pub meta_txn_ops: u64,
    pub meta_txn_lat_us: u64,
    pub vfs_create_total_ops: u64,
    pub vfs_create_total_lat_us: u64,
    pub vfs_create_meta_ops: u64,
    pub vfs_create_meta_lat_us: u64,
    pub vfs_unlink_total_ops: u64,
    pub vfs_unlink_total_lat_us: u64,
    pub vfs_unlink_lookup_ops: u64,
    pub vfs_unlink_lookup_lat_us: u64,
    pub vfs_unlink_stat_ops: u64,
    pub vfs_unlink_stat_lat_us: u64,
    pub vfs_unlink_meta_ops: u64,
    pub vfs_unlink_meta_lat_us: u64,
    pub vfs_unlink_recent_ops: u64,
    pub vfs_unlink_recent_lat_us: u64,
    pub vfs_setattr_recent_remove_ops: u64,
    pub vfs_setattr_recent_remove_lat_us: u64,
    pub vfs_setattr_recent_get_mut_ops: u64,
    pub vfs_setattr_recent_get_mut_lat_us: u64,
    pub vfs_read_dirty_probe_ops: u64,
    pub vfs_read_dirty_probe_lat_us: u64,
    pub vfs_read_handle_ops: u64,
    pub vfs_read_handle_lat_us: u64,
    pub vfs_read_overlay_ops: u64,
    pub vfs_read_overlay_lat_us: u64,
    pub s3_get_ops: u64,
    pub s3_get_bytes: u64,
    pub s3_get_lat_us: u64,
    pub s3_put_ops: u64,
    pub s3_put_bytes: u64,
    pub s3_put_lat_us: u64,
    pub s3_put_prepare_lat_us: u64,
    pub s3_put_cache_lat_us: u64,
    pub s3_del_ops: u64,
    pub buf_dirty_bytes: u64,
    pub buf_read_bytes: u64,
    pub memory_cas_retries: u64,
    pub memory_allocation_rejections: u64,
    pub writeback_live_dirty_bytes: u64,
    pub writeback_live_slices: u64,
    pub writeback_live_normal_only_bytes: u64,
    pub writeback_live_normal_only_slices: u64,
    pub writeback_live_cached_only_bytes: u64,
    pub writeback_live_cached_only_slices: u64,
    pub writeback_live_mixed_origin_bytes: u64,
    pub writeback_live_mixed_origin_slices: u64,
    pub writeback_live_unknown_origin_bytes: u64,
    pub writeback_live_unknown_origin_slices: u64,
    pub writeback_recent_pending_upload_bytes: u64,
    pub writeback_recent_pending_upload_slices: u64,
    pub writeback_recent_uploaded_bytes: u64,
    pub writeback_recent_uploaded_slices: u64,
    pub writeback_backpressure_soft_sleep_ops: u64,
    pub writeback_backpressure_soft_sleep_us: u64,
    pub writeback_backpressure_hard_wait_ops: u64,
    pub writeback_backpressure_hard_wait_us: u64,
    pub writeback_buffer_soft_sleep_ops: u64,
    pub writeback_buffer_soft_sleep_us: u64,
    pub writeback_buffer_moderate_sleep_ops: u64,
    pub writeback_buffer_moderate_sleep_us: u64,
    pub writeback_buffer_hard_sleep_ops: u64,
    pub writeback_buffer_hard_sleep_us: u64,
    pub writeback_stage_inflight_bytes: u64,
    pub writeback_remote_upload_inflight_bytes: u64,
    pub writeback_metadata_inflight: u64,
    pub writeback_stage_ops: u64,
    pub writeback_stage_bytes: u64,
    pub writeback_stage_lat_us: u64,
    pub writeback_stage_failures: u64,
    pub writeback_commit_before_stage_ops: u64,
    pub writeback_commit_wait_upload_ops: u64,
    pub writeback_commit_wait_upload_us: u64,
    pub writeback_commit_wait_upload_size_ops: u64,
    pub writeback_commit_wait_upload_size_us: u64,
    pub writeback_commit_wait_upload_max_unflushed_ops: u64,
    pub writeback_commit_wait_upload_max_unflushed_us: u64,
    pub writeback_commit_wait_upload_explicit_flush_ops: u64,
    pub writeback_commit_wait_upload_explicit_flush_us: u64,
    pub writeback_commit_wait_upload_auto_ops: u64,
    pub writeback_commit_wait_upload_auto_us: u64,
    pub writeback_commit_wait_upload_commit_age_ops: u64,
    pub writeback_commit_wait_upload_commit_age_us: u64,
    pub writeback_commit_wait_upload_unknown_reason_ops: u64,
    pub writeback_commit_wait_upload_unknown_reason_us: u64,
    pub writeback_commit_wait_upload_normal_only_ops: u64,
    pub writeback_commit_wait_upload_normal_only_us: u64,
    pub writeback_commit_wait_upload_cached_only_ops: u64,
    pub writeback_commit_wait_upload_cached_only_us: u64,
    pub writeback_commit_wait_upload_mixed_origin_ops: u64,
    pub writeback_commit_wait_upload_mixed_origin_us: u64,
    pub writeback_commit_wait_upload_unknown_origin_ops: u64,
    pub writeback_commit_wait_upload_unknown_origin_us: u64,
    pub writeback_commit_wait_retry_ops: u64,
    pub writeback_commit_wait_retry_us: u64,
    pub writeback_flush_wait_ops: u64,
    pub writeback_flush_wait_us: u64,
    pub writeback_flush_wait_slices: u64,
    pub writeback_flush_fragmentation_ops: u64,
    pub writeback_flush_fragmentation_slices: u64,
    pub writeback_flush_fragmentation_bytes: u64,
    pub writeback_flush_fragmentation_cached_sub_block_slices: u64,
    pub writeback_flush_fragmentation_cached_sub_block_bytes: u64,
    pub writeback_flush_fragmentation_full_block_slices: u64,
    pub writeback_flush_fragmentation_full_block_bytes: u64,
    pub writeback_slice_create_ops: u64,
    pub writeback_slice_reuse_ops: u64,
    pub writeback_slice_reject_older_unique_ops: u64,
    pub writeback_slice_reject_dispatched_prefix_ops: u64,
    pub writeback_freeze_size_ops: u64,
    pub writeback_freeze_size_bytes: u64,
    pub writeback_freeze_max_unflushed_ops: u64,
    pub writeback_freeze_max_unflushed_bytes: u64,
    pub writeback_freeze_explicit_flush_ops: u64,
    pub writeback_freeze_explicit_flush_bytes: u64,
    pub writeback_freeze_auto_ops: u64,
    pub writeback_freeze_auto_bytes: u64,
    pub writeback_freeze_commit_age_ops: u64,
    pub writeback_freeze_commit_age_bytes: u64,
    pub writeback_upload_batch_ops: u64,
    pub writeback_upload_batch_bytes: u64,
    pub writeback_upload_batch_blocks: u64,
    pub writeback_upload_batch_single_block_ops: u64,
    pub writeback_upload_batch_multi_block_ops: u64,
    pub writeback_upload_partial_tail_ops: u64,
    pub writeback_upload_partial_tail_size_ops: u64,
    pub writeback_upload_partial_tail_max_unflushed_ops: u64,
    pub writeback_upload_partial_tail_explicit_flush_ops: u64,
    pub writeback_upload_partial_tail_auto_ops: u64,
    pub writeback_upload_partial_tail_normal_only_ops: u64,
    pub writeback_upload_partial_tail_cached_only_ops: u64,
    pub writeback_upload_partial_tail_mixed_origin_ops: u64,
    pub writeback_upload_partial_tail_unknown_origin_ops: u64,
    pub writeback_upload_partial_tail_auto_age_ops: u64,
    pub writeback_upload_partial_tail_auto_idle_ops: u64,
    pub writeback_upload_partial_tail_auto_pressure_ops: u64,
    pub writeback_upload_partial_tail_auto_too_many_ops: u64,
    pub writeback_upload_partial_tail_auto_buffer_high_ops: u64,
    pub writeback_upload_partial_tail_auto_flush_duration_ops: u64,
    pub writeback_upload_partial_tail_auto_unknown_ops: u64,
    pub writeback_upload_partial_tail_auto_normal_only_ops: u64,
    pub writeback_upload_partial_tail_auto_cached_only_ops: u64,
    pub writeback_upload_partial_tail_auto_mixed_origin_ops: u64,
    pub writeback_upload_partial_tail_auto_unknown_origin_ops: u64,
    pub writeback_upload_partial_tail_commit_age_ops: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub read_block_cache_hits: u64,
    pub read_page_cache_hits: u64,
    pub read_page_cache_misses: u64,
    pub read_range_gets: u64,
    pub read_full_gets: u64,
    pub read_piggyback_full: u64,
    pub read_background_prefetches: u64,
    pub read_background_prefetch_dropped: u64,
    pub meta_stat_cache_hit: u64,
    pub meta_stat_cache_miss: u64,
    pub meta_stat_fresh_store_hit: u64,
    pub meta_lookup_cache_hit: u64,
    pub meta_lookup_cache_miss: u64,
    pub meta_get_slices_cache_hit: u64,
    pub meta_get_slices_cache_miss: u64,
    pub meta_open_fresh_stat: u64,
    pub meta_open_file_cache_hit: u64,
    pub meta_open_file_cache_miss: u64,
    pub meta_lookup_attr_fused_hit: u64,
    pub meta_lookup_attr_fused_miss: u64,
    pub meta_lookup_attr_fused_error: u64,
}

impl FsStatsSnapshot {
    pub fn cache_requests(&self) -> u64 {
        self.cache_hits + self.cache_misses
    }

    pub fn cache_hit_ratio(&self) -> f64 {
        ratio(self.cache_hits, self.cache_requests())
    }

    pub fn avg_fuse_read_lat_us(&self) -> f64 {
        ratio(self.fuse_read_lat_us, self.fuse_read_ops)
    }

    pub fn avg_fuse_write_lat_us(&self) -> f64 {
        ratio(self.fuse_write_lat_us, self.fuse_write_ops)
    }

    pub fn avg_fuse_flush_lat_us(&self) -> f64 {
        ratio(self.fuse_flush_lat_us, self.fuse_flush_ops)
    }

    pub fn avg_s3_get_lat_us(&self) -> f64 {
        ratio(self.s3_get_lat_us, self.s3_get_ops)
    }

    pub fn avg_s3_put_lat_us(&self) -> f64 {
        ratio(self.s3_put_lat_us, self.s3_put_ops)
    }

    pub fn avg_s3_put_prepare_lat_us(&self) -> f64 {
        ratio(self.s3_put_prepare_lat_us, self.s3_put_ops)
    }

    pub fn avg_s3_put_cache_lat_us(&self) -> f64 {
        ratio(self.s3_put_cache_lat_us, self.s3_put_ops)
    }
}

fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

/// Global filesystem statistics, designed for lock-free concurrent updates.
#[derive(Debug)]
pub struct FsStats {
    pub start_time: Instant,
    extension: std::sync::OnceLock<std::sync::Arc<dyn FsStatsExtension>>,
    request_observation: Arc<RequestObservation>,

    // ─── FUSE layer ───────────────────────────────────────────────
    /// Total FUSE read operations
    pub fuse_read_ops: AtomicU64,
    /// Total bytes read via FUSE
    pub fuse_read_bytes: AtomicU64,
    /// Total FUSE read latency in microseconds
    pub fuse_read_lat_us: AtomicU64,
    /// Total FUSE write operations
    pub fuse_write_ops: AtomicU64,
    /// Total bytes written via FUSE
    pub fuse_write_bytes: AtomicU64,
    /// Total FUSE write latency in microseconds
    pub fuse_write_lat_us: AtomicU64,
    /// Total FUSE lookup operations
    pub fuse_lookup_ops: AtomicU64,
    /// Total FUSE lookup latency in microseconds
    pub fuse_lookup_lat_us: AtomicU64,
    /// Total FUSE getattr operations
    pub fuse_getattr_ops: AtomicU64,
    /// Total FUSE getattr latency in microseconds
    pub fuse_getattr_lat_us: AtomicU64,
    /// Most recently observed FUSE attribute reply validity in nanoseconds.
    pub fuse_attr_valid_ns: AtomicU64,
    /// Most recently observed FUSE entry reply validity in nanoseconds.
    pub fuse_entry_valid_ns: AtomicU64,
    /// Total FUSE open operations
    pub fuse_open_ops: AtomicU64,
    /// Total FUSE create operations
    pub fuse_create_ops: AtomicU64,
    /// Total FUSE unlink/rmdir operations
    pub fuse_unlink_ops: AtomicU64,
    /// Total FUSE readdir operations
    pub fuse_readdir_ops: AtomicU64,
    /// Total FUSE flush/fsync operations
    pub fuse_flush_ops: AtomicU64,
    /// Total FUSE flush/fsync latency in microseconds
    pub fuse_flush_lat_us: AtomicU64,

    // ─── Meta layer ──────────────────────────────────────────────
    /// Total metadata operations (get_node, lookup, etc.)
    pub meta_ops: AtomicU64,
    /// Total metadata operation latency in microseconds
    pub meta_lat_us: AtomicU64,
    /// Total metadata transaction (write/commit) operations
    pub meta_txn_ops: AtomicU64,
    /// Total metadata transaction latency in microseconds
    pub meta_txn_lat_us: AtomicU64,

    // ─── VFS diagnostic timing ───────────────────────────────────
    /// Total VFS create_file_at operations timed by the optional diagnostic path
    pub vfs_create_total_ops: AtomicU64,
    /// Total VFS create_file_at latency in microseconds
    pub vfs_create_total_lat_us: AtomicU64,
    /// Total metadata create calls inside create_file_at
    pub vfs_create_meta_ops: AtomicU64,
    /// Total metadata create latency inside create_file_at in microseconds
    pub vfs_create_meta_lat_us: AtomicU64,
    /// Total VFS unlink_at operations timed by the optional diagnostic path
    pub vfs_unlink_total_ops: AtomicU64,
    /// Total VFS unlink_at latency in microseconds
    pub vfs_unlink_total_lat_us: AtomicU64,
    /// Total lookup calls inside unlink_at
    pub vfs_unlink_lookup_ops: AtomicU64,
    /// Total lookup latency inside unlink_at in microseconds
    pub vfs_unlink_lookup_lat_us: AtomicU64,
    /// Total stat calls inside unlink_at
    pub vfs_unlink_stat_ops: AtomicU64,
    /// Total stat latency inside unlink_at in microseconds
    pub vfs_unlink_stat_lat_us: AtomicU64,
    /// Total metadata unlink calls inside unlink_at
    pub vfs_unlink_meta_ops: AtomicU64,
    /// Total metadata unlink latency inside unlink_at in microseconds
    pub vfs_unlink_meta_lat_us: AtomicU64,
    /// Total recently-unlinked map updates inside unlink_at
    pub vfs_unlink_recent_ops: AtomicU64,
    /// Total recently-unlinked map update latency inside unlink_at in microseconds
    pub vfs_unlink_recent_lat_us: AtomicU64,
    /// Total remove-first deleted-inode setattr map probes
    pub vfs_setattr_recent_remove_ops: AtomicU64,
    /// Total remove-first deleted-inode setattr map latency in microseconds
    pub vfs_setattr_recent_remove_lat_us: AtomicU64,
    /// Total get_mut deleted-inode setattr map probes
    pub vfs_setattr_recent_get_mut_ops: AtomicU64,
    /// Total get_mut deleted-inode setattr map latency in microseconds
    pub vfs_setattr_recent_get_mut_lat_us: AtomicU64,
    /// Total dirty-overlay probes before VFS reads committed data
    pub vfs_read_dirty_probe_ops: AtomicU64,
    /// Total dirty-overlay probe latency before VFS reads committed data
    pub vfs_read_dirty_probe_lat_us: AtomicU64,
    /// Total handle.read calls inside VFS read
    pub vfs_read_handle_ops: AtomicU64,
    /// Total handle.read latency inside VFS read
    pub vfs_read_handle_lat_us: AtomicU64,
    /// Total post-read dirty overlay calls inside VFS read
    pub vfs_read_overlay_ops: AtomicU64,
    /// Total post-read dirty overlay latency inside VFS read
    pub vfs_read_overlay_lat_us: AtomicU64,

    // ─── Object storage (S3) layer ───────────────────────────────
    /// Total S3 GET requests
    pub s3_get_ops: AtomicU64,
    /// Total bytes fetched from S3
    pub s3_get_bytes: AtomicU64,
    /// Total S3 GET latency in microseconds
    pub s3_get_lat_us: AtomicU64,
    /// Total S3 PUT requests
    pub s3_put_ops: AtomicU64,
    /// Total bytes uploaded to S3
    pub s3_put_bytes: AtomicU64,
    /// Total S3 PUT latency in microseconds
    pub s3_put_lat_us: AtomicU64,
    /// Total block preparation latency before S3 PUT in microseconds
    pub s3_put_prepare_lat_us: AtomicU64,
    /// Total write-cache population latency after S3 PUT in microseconds
    pub s3_put_cache_lat_us: AtomicU64,
    /// Total S3 DELETE requests
    pub s3_del_ops: AtomicU64,

    // ─── Buffer/cache usage ──────────────────────────────────────
    /// Current dirty write buffer bytes
    pub buf_dirty_bytes: AtomicU64,
    /// Current reader cache bytes
    pub buf_read_bytes: AtomicU64,
    /// Failed reader-budget reservation CAS attempts.
    pub memory_cas_retries: AtomicU64,
    /// Reader-budget reservations rejected at the critical watermark.
    pub memory_allocation_rejections: AtomicU64,
    /// Dirty bytes in active writeback slices.
    pub writeback_live_dirty_bytes: AtomicU64,
    /// Active writeback slices currently visible to overlay/commit.
    pub writeback_live_slices: AtomicU64,
    /// Active writeback bytes in slices written only through the ordinary write path.
    pub writeback_live_normal_only_bytes: AtomicU64,
    /// Active writeback slices written only through the ordinary write path.
    pub writeback_live_normal_only_slices: AtomicU64,
    /// Active writeback bytes in slices written only through the cached writeback path.
    pub writeback_live_cached_only_bytes: AtomicU64,
    /// Active writeback slices written only through the cached writeback path.
    pub writeback_live_cached_only_slices: AtomicU64,
    /// Active writeback bytes in slices that received both ordinary and cached writes.
    pub writeback_live_mixed_origin_bytes: AtomicU64,
    /// Active writeback slices that received both ordinary and cached writes.
    pub writeback_live_mixed_origin_slices: AtomicU64,
    /// Active writeback bytes in slices whose write origin is unknown.
    pub writeback_live_unknown_origin_bytes: AtomicU64,
    /// Active writeback slices whose write origin is unknown.
    pub writeback_live_unknown_origin_slices: AtomicU64,
    /// Recently committed bytes still waiting for S3 upload completion.
    pub writeback_recent_pending_upload_bytes: AtomicU64,
    /// Recently committed slices still waiting for S3 upload completion.
    pub writeback_recent_pending_upload_slices: AtomicU64,
    /// Recently committed bytes already uploaded to S3 but kept for overlay grace.
    pub writeback_recent_uploaded_bytes: AtomicU64,
    /// Recently committed slices already uploaded to S3 but kept for overlay grace.
    pub writeback_recent_uploaded_slices: AtomicU64,
    /// Soft backpressure sleeps on committed-but-not-uploaded writeback bytes.
    pub writeback_backpressure_soft_sleep_ops: AtomicU64,
    /// Total soft backpressure sleep duration in microseconds.
    pub writeback_backpressure_soft_sleep_us: AtomicU64,
    /// Hard backpressure waits on committed-but-not-uploaded writeback bytes.
    pub writeback_backpressure_hard_wait_ops: AtomicU64,
    /// Total hard backpressure wait duration in microseconds.
    pub writeback_backpressure_hard_wait_us: AtomicU64,
    /// Light sleeps caused by the writer buffer exceeding its soft limit.
    pub writeback_buffer_soft_sleep_ops: AtomicU64,
    /// Total light writer-buffer sleep duration in microseconds.
    pub writeback_buffer_soft_sleep_us: AtomicU64,
    /// Moderate sleeps caused by the writer buffer approaching its hard limit.
    pub writeback_buffer_moderate_sleep_ops: AtomicU64,
    /// Total moderate writer-buffer sleep duration in microseconds.
    pub writeback_buffer_moderate_sleep_us: AtomicU64,
    /// Hard-limit writer-buffer sleep loops.
    pub writeback_buffer_hard_sleep_ops: AtomicU64,
    /// Total hard-limit writer-buffer sleep duration in microseconds.
    pub writeback_buffer_hard_sleep_us: AtomicU64,
    /// Bytes currently being persisted to the local writeback stage.
    pub writeback_stage_inflight_bytes: AtomicU64,
    /// Bytes currently being uploaded from the writer pipeline to object storage.
    pub writeback_remote_upload_inflight_bytes: AtomicU64,
    /// Metadata publication RPCs currently in flight.
    pub writeback_metadata_inflight: AtomicU64,
    /// Total local writeback stage operations.
    pub writeback_stage_ops: AtomicU64,
    /// Total bytes persisted to the local writeback stage.
    pub writeback_stage_bytes: AtomicU64,
    /// Total local writeback stage latency in microseconds.
    pub writeback_stage_lat_us: AtomicU64,
    /// Total local writeback stage failures.
    pub writeback_stage_failures: AtomicU64,
    /// Metadata commits that happened before local staging finished.
    pub writeback_commit_before_stage_ops: AtomicU64,
    /// Commit-loop waits for a slice upload notification or timeout.
    pub writeback_commit_wait_upload_ops: AtomicU64,
    /// Total commit-loop upload wait duration in microseconds.
    pub writeback_commit_wait_upload_us: AtomicU64,
    /// Commit-loop upload waits for size/chunk-end frozen slices.
    pub writeback_commit_wait_upload_size_ops: AtomicU64,
    pub writeback_commit_wait_upload_size_us: AtomicU64,
    /// Commit-loop upload waits for max-unflushed frozen slices.
    pub writeback_commit_wait_upload_max_unflushed_ops: AtomicU64,
    pub writeback_commit_wait_upload_max_unflushed_us: AtomicU64,
    /// Commit-loop upload waits for explicit flush frozen slices.
    pub writeback_commit_wait_upload_explicit_flush_ops: AtomicU64,
    pub writeback_commit_wait_upload_explicit_flush_us: AtomicU64,
    /// Commit-loop upload waits for auto-frozen slices.
    pub writeback_commit_wait_upload_auto_ops: AtomicU64,
    pub writeback_commit_wait_upload_auto_us: AtomicU64,
    /// Commit-loop upload waits for commit-age safety frozen slices.
    pub writeback_commit_wait_upload_commit_age_ops: AtomicU64,
    pub writeback_commit_wait_upload_commit_age_us: AtomicU64,
    /// Commit-loop upload waits without freeze reason attribution.
    pub writeback_commit_wait_upload_unknown_reason_ops: AtomicU64,
    pub writeback_commit_wait_upload_unknown_reason_us: AtomicU64,
    /// Commit-loop upload waits for ordinary-write-only slices.
    pub writeback_commit_wait_upload_normal_only_ops: AtomicU64,
    pub writeback_commit_wait_upload_normal_only_us: AtomicU64,
    /// Commit-loop upload waits for cached-writeback-only slices.
    pub writeback_commit_wait_upload_cached_only_ops: AtomicU64,
    pub writeback_commit_wait_upload_cached_only_us: AtomicU64,
    /// Commit-loop upload waits for mixed-origin slices.
    pub writeback_commit_wait_upload_mixed_origin_ops: AtomicU64,
    pub writeback_commit_wait_upload_mixed_origin_us: AtomicU64,
    /// Commit-loop upload waits for slices without origin attribution.
    pub writeback_commit_wait_upload_unknown_origin_ops: AtomicU64,
    pub writeback_commit_wait_upload_unknown_origin_us: AtomicU64,
    /// Commit-loop retry backoff sleeps after commit conditions are not met.
    pub writeback_commit_wait_retry_ops: AtomicU64,
    /// Total commit-loop retry backoff duration in microseconds.
    pub writeback_commit_wait_retry_us: AtomicU64,
    /// Foreground flush/close waits observed by the writer.
    pub writeback_flush_wait_ops: AtomicU64,
    /// Total foreground flush/close wait duration in microseconds.
    pub writeback_flush_wait_us: AtomicU64,
    /// Total slices captured by foreground flush/close waits.
    pub writeback_flush_wait_slices: AtomicU64,
    /// Explicit flush snapshot observations.
    pub writeback_flush_fragmentation_ops: AtomicU64,
    /// Total slices seen in explicit flush snapshots.
    pub writeback_flush_fragmentation_slices: AtomicU64,
    /// Total bytes seen in explicit flush snapshots.
    pub writeback_flush_fragmentation_bytes: AtomicU64,
    /// Cached-writeback sub-block slices seen in explicit flush snapshots.
    pub writeback_flush_fragmentation_cached_sub_block_slices: AtomicU64,
    /// Cached-writeback sub-block bytes seen in explicit flush snapshots.
    pub writeback_flush_fragmentation_cached_sub_block_bytes: AtomicU64,
    /// Full-block-aligned slices seen in explicit flush snapshots.
    pub writeback_flush_fragmentation_full_block_slices: AtomicU64,
    /// Full-block-aligned bytes seen in explicit flush snapshots.
    pub writeback_flush_fragmentation_full_block_bytes: AtomicU64,
    /// Newly-created writer slices.
    pub writeback_slice_create_ops: AtomicU64,
    /// Writes that reused an existing writer slice.
    pub writeback_slice_reuse_ops: AtomicU64,
    /// Slice reuse rejects caused by older FUSE unique ordering.
    pub writeback_slice_reject_older_unique_ops: AtomicU64,
    /// Slice reuse rejects caused by already-dispatched/uploaded block prefixes.
    pub writeback_slice_reject_dispatched_prefix_ops: AtomicU64,
    /// Slices frozen because they reached target size or chunk end.
    pub writeback_freeze_size_ops: AtomicU64,
    /// Bytes in slices frozen because they reached target size or chunk end.
    pub writeback_freeze_size_bytes: AtomicU64,
    /// Slices frozen to cap per-chunk unflushed slice count.
    pub writeback_freeze_max_unflushed_ops: AtomicU64,
    /// Bytes in slices frozen to cap per-chunk unflushed slice count.
    pub writeback_freeze_max_unflushed_bytes: AtomicU64,
    /// Slices frozen by explicit flush/fsync/close.
    pub writeback_freeze_explicit_flush_ops: AtomicU64,
    /// Bytes in slices frozen by explicit flush/fsync/close.
    pub writeback_freeze_explicit_flush_bytes: AtomicU64,
    /// Slices frozen by auto-flush, memory pressure, or background pressure flush.
    pub writeback_freeze_auto_ops: AtomicU64,
    /// Bytes in slices frozen by auto-flush, memory pressure, or background pressure flush.
    pub writeback_freeze_auto_bytes: AtomicU64,
    /// Slices frozen by commit-loop age safety.
    pub writeback_freeze_commit_age_ops: AtomicU64,
    /// Bytes in slices frozen by commit-loop age safety.
    pub writeback_freeze_commit_age_bytes: AtomicU64,
    /// Upload batches dispatched from writer slices.
    pub writeback_upload_batch_ops: AtomicU64,
    /// Bytes included in writer upload batches.
    pub writeback_upload_batch_bytes: AtomicU64,
    /// Blocks included in writer upload batches.
    pub writeback_upload_batch_blocks: AtomicU64,
    /// Upload batches with zero or one block.
    pub writeback_upload_batch_single_block_ops: AtomicU64,
    /// Upload batches with more than one block.
    pub writeback_upload_batch_multi_block_ops: AtomicU64,
    /// Upload batches that included a frozen partial tail block.
    pub writeback_upload_partial_tail_ops: AtomicU64,
    /// Partial-tail upload batches from size/chunk-end freezes.
    pub writeback_upload_partial_tail_size_ops: AtomicU64,
    /// Partial-tail upload batches from max-unflushed freezes.
    pub writeback_upload_partial_tail_max_unflushed_ops: AtomicU64,
    /// Partial-tail upload batches from explicit flush/fsync/close freezes.
    pub writeback_upload_partial_tail_explicit_flush_ops: AtomicU64,
    /// Partial-tail upload batches from auto flush freezes.
    pub writeback_upload_partial_tail_auto_ops: AtomicU64,
    /// Partial-tail upload batches from ordinary-write-only slices.
    pub writeback_upload_partial_tail_normal_only_ops: AtomicU64,
    /// Partial-tail upload batches from cached-writeback-only slices.
    pub writeback_upload_partial_tail_cached_only_ops: AtomicU64,
    /// Partial-tail upload batches from slices that mixed ordinary and cached writes.
    pub writeback_upload_partial_tail_mixed_origin_ops: AtomicU64,
    /// Partial-tail upload batches from slices whose write origin is unknown.
    pub writeback_upload_partial_tail_unknown_origin_ops: AtomicU64,
    /// Partial-tail auto upload batches caused by max-age auto flush.
    pub writeback_upload_partial_tail_auto_age_ops: AtomicU64,
    /// Partial-tail auto upload batches caused by idle auto flush.
    pub writeback_upload_partial_tail_auto_idle_ops: AtomicU64,
    /// Partial-tail auto upload batches caused by memory pressure.
    pub writeback_upload_partial_tail_auto_pressure_ops: AtomicU64,
    /// Partial-tail auto upload batches caused by too many live slices.
    pub writeback_upload_partial_tail_auto_too_many_ops: AtomicU64,
    /// Partial-tail auto upload batches caused by high buffer usage.
    pub writeback_upload_partial_tail_auto_buffer_high_ops: AtomicU64,
    /// Partial-tail auto upload batches caused by flush-duration safety.
    pub writeback_upload_partial_tail_auto_flush_duration_ops: AtomicU64,
    /// Partial-tail auto upload batches without trigger attribution.
    pub writeback_upload_partial_tail_auto_unknown_ops: AtomicU64,
    /// Auto partial-tail upload batches from ordinary-write-only slices.
    pub writeback_upload_partial_tail_auto_normal_only_ops: AtomicU64,
    /// Auto partial-tail upload batches from cached-writeback-only slices.
    pub writeback_upload_partial_tail_auto_cached_only_ops: AtomicU64,
    /// Auto partial-tail upload batches from slices that mixed ordinary and cached writes.
    pub writeback_upload_partial_tail_auto_mixed_origin_ops: AtomicU64,
    /// Auto partial-tail upload batches from slices whose write origin is unknown.
    pub writeback_upload_partial_tail_auto_unknown_origin_ops: AtomicU64,
    /// Partial-tail upload batches from commit-age safety freezes.
    pub writeback_upload_partial_tail_commit_age_ops: AtomicU64,
    /// Block cache hit count
    pub cache_hits: AtomicU64,
    /// Block cache miss count
    pub cache_misses: AtomicU64,
    /// Full-block cache read hits
    pub read_block_cache_hits: AtomicU64,
    /// Page-cache read hits
    pub read_page_cache_hits: AtomicU64,
    /// Page-cache read misses
    pub read_page_cache_misses: AtomicU64,
    /// Foreground range GET requests
    pub read_range_gets: AtomicU64,
    /// Foreground full-block GET requests
    pub read_full_gets: AtomicU64,
    /// Reads piggybacked onto an in-flight full-block GET
    pub read_piggyback_full: AtomicU64,
    /// Background full-block prefetches scheduled by range reads
    pub read_background_prefetches: AtomicU64,
    /// Background full-block prefetches dropped before execution
    pub read_background_prefetch_dropped: AtomicU64,
    /// Metadata stat cache hits
    pub meta_stat_cache_hit: AtomicU64,
    /// Metadata stat cache misses
    pub meta_stat_cache_miss: AtomicU64,
    /// Fresh stat store hits
    pub meta_stat_fresh_store_hit: AtomicU64,
    /// Metadata lookup cache hits, including cached negative answers
    pub meta_lookup_cache_hit: AtomicU64,
    /// Metadata lookup cache misses
    pub meta_lookup_cache_miss: AtomicU64,
    /// Metadata slice cache hits
    pub meta_get_slices_cache_hit: AtomicU64,
    /// Metadata slice cache misses
    pub meta_get_slices_cache_miss: AtomicU64,
    /// Fresh stat calls issued for close-to-open/open refresh
    pub meta_open_fresh_stat: AtomicU64,
    /// Open-file scoped metadata cache hits
    pub meta_open_file_cache_hit: AtomicU64,
    /// Open-file scoped metadata cache misses
    pub meta_open_file_cache_miss: AtomicU64,
    /// Fused lookup-with-attr cache hits
    pub meta_lookup_attr_fused_hit: AtomicU64,
    /// Fused lookup-with-attr backend misses
    pub meta_lookup_attr_fused_miss: AtomicU64,
    /// Fused lookup-with-attr backend errors
    pub meta_lookup_attr_fused_error: AtomicU64,
}

impl FsStats {
    pub(crate) const BASE_RENDER_MAX_BYTES: usize = 64 * 1024;
    pub fn new() -> Self {
        Self {
            start_time: Instant::now(),
            extension: std::sync::OnceLock::new(),
            request_observation: Arc::new(RequestObservation::from_environment()),
            fuse_read_ops: AtomicU64::new(0),
            fuse_read_bytes: AtomicU64::new(0),
            fuse_read_lat_us: AtomicU64::new(0),
            fuse_write_ops: AtomicU64::new(0),
            fuse_write_bytes: AtomicU64::new(0),
            fuse_write_lat_us: AtomicU64::new(0),
            fuse_lookup_ops: AtomicU64::new(0),
            fuse_lookup_lat_us: AtomicU64::new(0),
            fuse_getattr_ops: AtomicU64::new(0),
            fuse_getattr_lat_us: AtomicU64::new(0),
            fuse_attr_valid_ns: AtomicU64::new(0),
            fuse_entry_valid_ns: AtomicU64::new(0),
            fuse_open_ops: AtomicU64::new(0),
            fuse_create_ops: AtomicU64::new(0),
            fuse_unlink_ops: AtomicU64::new(0),
            fuse_readdir_ops: AtomicU64::new(0),
            fuse_flush_ops: AtomicU64::new(0),
            fuse_flush_lat_us: AtomicU64::new(0),
            meta_ops: AtomicU64::new(0),
            meta_lat_us: AtomicU64::new(0),
            meta_txn_ops: AtomicU64::new(0),
            meta_txn_lat_us: AtomicU64::new(0),
            vfs_create_total_ops: AtomicU64::new(0),
            vfs_create_total_lat_us: AtomicU64::new(0),
            vfs_create_meta_ops: AtomicU64::new(0),
            vfs_create_meta_lat_us: AtomicU64::new(0),
            vfs_unlink_total_ops: AtomicU64::new(0),
            vfs_unlink_total_lat_us: AtomicU64::new(0),
            vfs_unlink_lookup_ops: AtomicU64::new(0),
            vfs_unlink_lookup_lat_us: AtomicU64::new(0),
            vfs_unlink_stat_ops: AtomicU64::new(0),
            vfs_unlink_stat_lat_us: AtomicU64::new(0),
            vfs_unlink_meta_ops: AtomicU64::new(0),
            vfs_unlink_meta_lat_us: AtomicU64::new(0),
            vfs_unlink_recent_ops: AtomicU64::new(0),
            vfs_unlink_recent_lat_us: AtomicU64::new(0),
            vfs_setattr_recent_remove_ops: AtomicU64::new(0),
            vfs_setattr_recent_remove_lat_us: AtomicU64::new(0),
            vfs_setattr_recent_get_mut_ops: AtomicU64::new(0),
            vfs_setattr_recent_get_mut_lat_us: AtomicU64::new(0),
            vfs_read_dirty_probe_ops: AtomicU64::new(0),
            vfs_read_dirty_probe_lat_us: AtomicU64::new(0),
            vfs_read_handle_ops: AtomicU64::new(0),
            vfs_read_handle_lat_us: AtomicU64::new(0),
            vfs_read_overlay_ops: AtomicU64::new(0),
            vfs_read_overlay_lat_us: AtomicU64::new(0),
            s3_get_ops: AtomicU64::new(0),
            s3_get_bytes: AtomicU64::new(0),
            s3_get_lat_us: AtomicU64::new(0),
            s3_put_ops: AtomicU64::new(0),
            s3_put_bytes: AtomicU64::new(0),
            s3_put_lat_us: AtomicU64::new(0),
            s3_put_prepare_lat_us: AtomicU64::new(0),
            s3_put_cache_lat_us: AtomicU64::new(0),
            s3_del_ops: AtomicU64::new(0),
            buf_dirty_bytes: AtomicU64::new(0),
            buf_read_bytes: AtomicU64::new(0),
            memory_cas_retries: AtomicU64::new(0),
            memory_allocation_rejections: AtomicU64::new(0),
            writeback_live_dirty_bytes: AtomicU64::new(0),
            writeback_live_slices: AtomicU64::new(0),
            writeback_live_normal_only_bytes: AtomicU64::new(0),
            writeback_live_normal_only_slices: AtomicU64::new(0),
            writeback_live_cached_only_bytes: AtomicU64::new(0),
            writeback_live_cached_only_slices: AtomicU64::new(0),
            writeback_live_mixed_origin_bytes: AtomicU64::new(0),
            writeback_live_mixed_origin_slices: AtomicU64::new(0),
            writeback_live_unknown_origin_bytes: AtomicU64::new(0),
            writeback_live_unknown_origin_slices: AtomicU64::new(0),
            writeback_recent_pending_upload_bytes: AtomicU64::new(0),
            writeback_recent_pending_upload_slices: AtomicU64::new(0),
            writeback_recent_uploaded_bytes: AtomicU64::new(0),
            writeback_recent_uploaded_slices: AtomicU64::new(0),
            writeback_backpressure_soft_sleep_ops: AtomicU64::new(0),
            writeback_backpressure_soft_sleep_us: AtomicU64::new(0),
            writeback_backpressure_hard_wait_ops: AtomicU64::new(0),
            writeback_backpressure_hard_wait_us: AtomicU64::new(0),
            writeback_buffer_soft_sleep_ops: AtomicU64::new(0),
            writeback_buffer_soft_sleep_us: AtomicU64::new(0),
            writeback_buffer_moderate_sleep_ops: AtomicU64::new(0),
            writeback_buffer_moderate_sleep_us: AtomicU64::new(0),
            writeback_buffer_hard_sleep_ops: AtomicU64::new(0),
            writeback_buffer_hard_sleep_us: AtomicU64::new(0),
            writeback_stage_inflight_bytes: AtomicU64::new(0),
            writeback_remote_upload_inflight_bytes: AtomicU64::new(0),
            writeback_metadata_inflight: AtomicU64::new(0),
            writeback_stage_ops: AtomicU64::new(0),
            writeback_stage_bytes: AtomicU64::new(0),
            writeback_stage_lat_us: AtomicU64::new(0),
            writeback_stage_failures: AtomicU64::new(0),
            writeback_commit_before_stage_ops: AtomicU64::new(0),
            writeback_commit_wait_upload_ops: AtomicU64::new(0),
            writeback_commit_wait_upload_us: AtomicU64::new(0),
            writeback_commit_wait_upload_size_ops: AtomicU64::new(0),
            writeback_commit_wait_upload_size_us: AtomicU64::new(0),
            writeback_commit_wait_upload_max_unflushed_ops: AtomicU64::new(0),
            writeback_commit_wait_upload_max_unflushed_us: AtomicU64::new(0),
            writeback_commit_wait_upload_explicit_flush_ops: AtomicU64::new(0),
            writeback_commit_wait_upload_explicit_flush_us: AtomicU64::new(0),
            writeback_commit_wait_upload_auto_ops: AtomicU64::new(0),
            writeback_commit_wait_upload_auto_us: AtomicU64::new(0),
            writeback_commit_wait_upload_commit_age_ops: AtomicU64::new(0),
            writeback_commit_wait_upload_commit_age_us: AtomicU64::new(0),
            writeback_commit_wait_upload_unknown_reason_ops: AtomicU64::new(0),
            writeback_commit_wait_upload_unknown_reason_us: AtomicU64::new(0),
            writeback_commit_wait_upload_normal_only_ops: AtomicU64::new(0),
            writeback_commit_wait_upload_normal_only_us: AtomicU64::new(0),
            writeback_commit_wait_upload_cached_only_ops: AtomicU64::new(0),
            writeback_commit_wait_upload_cached_only_us: AtomicU64::new(0),
            writeback_commit_wait_upload_mixed_origin_ops: AtomicU64::new(0),
            writeback_commit_wait_upload_mixed_origin_us: AtomicU64::new(0),
            writeback_commit_wait_upload_unknown_origin_ops: AtomicU64::new(0),
            writeback_commit_wait_upload_unknown_origin_us: AtomicU64::new(0),
            writeback_commit_wait_retry_ops: AtomicU64::new(0),
            writeback_commit_wait_retry_us: AtomicU64::new(0),
            writeback_flush_wait_ops: AtomicU64::new(0),
            writeback_flush_wait_us: AtomicU64::new(0),
            writeback_flush_wait_slices: AtomicU64::new(0),
            writeback_flush_fragmentation_ops: AtomicU64::new(0),
            writeback_flush_fragmentation_slices: AtomicU64::new(0),
            writeback_flush_fragmentation_bytes: AtomicU64::new(0),
            writeback_flush_fragmentation_cached_sub_block_slices: AtomicU64::new(0),
            writeback_flush_fragmentation_cached_sub_block_bytes: AtomicU64::new(0),
            writeback_flush_fragmentation_full_block_slices: AtomicU64::new(0),
            writeback_flush_fragmentation_full_block_bytes: AtomicU64::new(0),
            writeback_slice_create_ops: AtomicU64::new(0),
            writeback_slice_reuse_ops: AtomicU64::new(0),
            writeback_slice_reject_older_unique_ops: AtomicU64::new(0),
            writeback_slice_reject_dispatched_prefix_ops: AtomicU64::new(0),
            writeback_freeze_size_ops: AtomicU64::new(0),
            writeback_freeze_size_bytes: AtomicU64::new(0),
            writeback_freeze_max_unflushed_ops: AtomicU64::new(0),
            writeback_freeze_max_unflushed_bytes: AtomicU64::new(0),
            writeback_freeze_explicit_flush_ops: AtomicU64::new(0),
            writeback_freeze_explicit_flush_bytes: AtomicU64::new(0),
            writeback_freeze_auto_ops: AtomicU64::new(0),
            writeback_freeze_auto_bytes: AtomicU64::new(0),
            writeback_freeze_commit_age_ops: AtomicU64::new(0),
            writeback_freeze_commit_age_bytes: AtomicU64::new(0),
            writeback_upload_batch_ops: AtomicU64::new(0),
            writeback_upload_batch_bytes: AtomicU64::new(0),
            writeback_upload_batch_blocks: AtomicU64::new(0),
            writeback_upload_batch_single_block_ops: AtomicU64::new(0),
            writeback_upload_batch_multi_block_ops: AtomicU64::new(0),
            writeback_upload_partial_tail_ops: AtomicU64::new(0),
            writeback_upload_partial_tail_size_ops: AtomicU64::new(0),
            writeback_upload_partial_tail_max_unflushed_ops: AtomicU64::new(0),
            writeback_upload_partial_tail_explicit_flush_ops: AtomicU64::new(0),
            writeback_upload_partial_tail_auto_ops: AtomicU64::new(0),
            writeback_upload_partial_tail_normal_only_ops: AtomicU64::new(0),
            writeback_upload_partial_tail_cached_only_ops: AtomicU64::new(0),
            writeback_upload_partial_tail_mixed_origin_ops: AtomicU64::new(0),
            writeback_upload_partial_tail_unknown_origin_ops: AtomicU64::new(0),
            writeback_upload_partial_tail_auto_age_ops: AtomicU64::new(0),
            writeback_upload_partial_tail_auto_idle_ops: AtomicU64::new(0),
            writeback_upload_partial_tail_auto_pressure_ops: AtomicU64::new(0),
            writeback_upload_partial_tail_auto_too_many_ops: AtomicU64::new(0),
            writeback_upload_partial_tail_auto_buffer_high_ops: AtomicU64::new(0),
            writeback_upload_partial_tail_auto_flush_duration_ops: AtomicU64::new(0),
            writeback_upload_partial_tail_auto_unknown_ops: AtomicU64::new(0),
            writeback_upload_partial_tail_auto_normal_only_ops: AtomicU64::new(0),
            writeback_upload_partial_tail_auto_cached_only_ops: AtomicU64::new(0),
            writeback_upload_partial_tail_auto_mixed_origin_ops: AtomicU64::new(0),
            writeback_upload_partial_tail_auto_unknown_origin_ops: AtomicU64::new(0),
            writeback_upload_partial_tail_commit_age_ops: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            read_block_cache_hits: AtomicU64::new(0),
            read_page_cache_hits: AtomicU64::new(0),
            read_page_cache_misses: AtomicU64::new(0),
            read_range_gets: AtomicU64::new(0),
            read_full_gets: AtomicU64::new(0),
            read_piggyback_full: AtomicU64::new(0),
            read_background_prefetches: AtomicU64::new(0),
            read_background_prefetch_dropped: AtomicU64::new(0),
            meta_stat_cache_hit: AtomicU64::new(0),
            meta_stat_cache_miss: AtomicU64::new(0),
            meta_stat_fresh_store_hit: AtomicU64::new(0),
            meta_lookup_cache_hit: AtomicU64::new(0),
            meta_lookup_cache_miss: AtomicU64::new(0),
            meta_get_slices_cache_hit: AtomicU64::new(0),
            meta_get_slices_cache_miss: AtomicU64::new(0),
            meta_open_fresh_stat: AtomicU64::new(0),
            meta_open_file_cache_hit: AtomicU64::new(0),
            meta_open_file_cache_miss: AtomicU64::new(0),
            meta_lookup_attr_fused_hit: AtomicU64::new(0),
            meta_lookup_attr_fused_miss: AtomicU64::new(0),
            meta_lookup_attr_fused_error: AtomicU64::new(0),
        }
    }

    pub fn snapshot(&self) -> FsStatsSnapshot {
        FsStatsSnapshot {
            uptime_seconds: self.start_time.elapsed().as_secs(),
            fuse_read_ops: self.fuse_read_ops.load(ORD),
            fuse_read_bytes: self.fuse_read_bytes.load(ORD),
            fuse_read_lat_us: self.fuse_read_lat_us.load(ORD),
            fuse_write_ops: self.fuse_write_ops.load(ORD),
            fuse_write_bytes: self.fuse_write_bytes.load(ORD),
            fuse_write_lat_us: self.fuse_write_lat_us.load(ORD),
            fuse_lookup_ops: self.fuse_lookup_ops.load(ORD),
            fuse_lookup_lat_us: self.fuse_lookup_lat_us.load(ORD),
            fuse_getattr_ops: self.fuse_getattr_ops.load(ORD),
            fuse_getattr_lat_us: self.fuse_getattr_lat_us.load(ORD),
            fuse_attr_valid_ns: self.fuse_attr_valid_ns.load(ORD),
            fuse_entry_valid_ns: self.fuse_entry_valid_ns.load(ORD),
            fuse_open_ops: self.fuse_open_ops.load(ORD),
            fuse_create_ops: self.fuse_create_ops.load(ORD),
            fuse_unlink_ops: self.fuse_unlink_ops.load(ORD),
            fuse_readdir_ops: self.fuse_readdir_ops.load(ORD),
            fuse_flush_ops: self.fuse_flush_ops.load(ORD),
            fuse_flush_lat_us: self.fuse_flush_lat_us.load(ORD),
            meta_ops: self.meta_ops.load(ORD),
            meta_lat_us: self.meta_lat_us.load(ORD),
            meta_txn_ops: self.meta_txn_ops.load(ORD),
            meta_txn_lat_us: self.meta_txn_lat_us.load(ORD),
            vfs_create_total_ops: self.vfs_create_total_ops.load(ORD),
            vfs_create_total_lat_us: self.vfs_create_total_lat_us.load(ORD),
            vfs_create_meta_ops: self.vfs_create_meta_ops.load(ORD),
            vfs_create_meta_lat_us: self.vfs_create_meta_lat_us.load(ORD),
            vfs_unlink_total_ops: self.vfs_unlink_total_ops.load(ORD),
            vfs_unlink_total_lat_us: self.vfs_unlink_total_lat_us.load(ORD),
            vfs_unlink_lookup_ops: self.vfs_unlink_lookup_ops.load(ORD),
            vfs_unlink_lookup_lat_us: self.vfs_unlink_lookup_lat_us.load(ORD),
            vfs_unlink_stat_ops: self.vfs_unlink_stat_ops.load(ORD),
            vfs_unlink_stat_lat_us: self.vfs_unlink_stat_lat_us.load(ORD),
            vfs_unlink_meta_ops: self.vfs_unlink_meta_ops.load(ORD),
            vfs_unlink_meta_lat_us: self.vfs_unlink_meta_lat_us.load(ORD),
            vfs_unlink_recent_ops: self.vfs_unlink_recent_ops.load(ORD),
            vfs_unlink_recent_lat_us: self.vfs_unlink_recent_lat_us.load(ORD),
            vfs_setattr_recent_remove_ops: self.vfs_setattr_recent_remove_ops.load(ORD),
            vfs_setattr_recent_remove_lat_us: self.vfs_setattr_recent_remove_lat_us.load(ORD),
            vfs_setattr_recent_get_mut_ops: self.vfs_setattr_recent_get_mut_ops.load(ORD),
            vfs_setattr_recent_get_mut_lat_us: self.vfs_setattr_recent_get_mut_lat_us.load(ORD),
            vfs_read_dirty_probe_ops: self.vfs_read_dirty_probe_ops.load(ORD),
            vfs_read_dirty_probe_lat_us: self.vfs_read_dirty_probe_lat_us.load(ORD),
            vfs_read_handle_ops: self.vfs_read_handle_ops.load(ORD),
            vfs_read_handle_lat_us: self.vfs_read_handle_lat_us.load(ORD),
            vfs_read_overlay_ops: self.vfs_read_overlay_ops.load(ORD),
            vfs_read_overlay_lat_us: self.vfs_read_overlay_lat_us.load(ORD),
            s3_get_ops: self.s3_get_ops.load(ORD),
            s3_get_bytes: self.s3_get_bytes.load(ORD),
            s3_get_lat_us: self.s3_get_lat_us.load(ORD),
            s3_put_ops: self.s3_put_ops.load(ORD),
            s3_put_bytes: self.s3_put_bytes.load(ORD),
            s3_put_lat_us: self.s3_put_lat_us.load(ORD),
            s3_put_prepare_lat_us: self.s3_put_prepare_lat_us.load(ORD),
            s3_put_cache_lat_us: self.s3_put_cache_lat_us.load(ORD),
            s3_del_ops: self.s3_del_ops.load(ORD),
            buf_dirty_bytes: self.buf_dirty_bytes.load(ORD),
            buf_read_bytes: self.buf_read_bytes.load(ORD),
            memory_cas_retries: self.memory_cas_retries.load(ORD),
            memory_allocation_rejections: self.memory_allocation_rejections.load(ORD),
            writeback_live_dirty_bytes: self.writeback_live_dirty_bytes.load(ORD),
            writeback_live_slices: self.writeback_live_slices.load(ORD),
            writeback_live_normal_only_bytes: self.writeback_live_normal_only_bytes.load(ORD),
            writeback_live_normal_only_slices: self.writeback_live_normal_only_slices.load(ORD),
            writeback_live_cached_only_bytes: self.writeback_live_cached_only_bytes.load(ORD),
            writeback_live_cached_only_slices: self.writeback_live_cached_only_slices.load(ORD),
            writeback_live_mixed_origin_bytes: self.writeback_live_mixed_origin_bytes.load(ORD),
            writeback_live_mixed_origin_slices: self.writeback_live_mixed_origin_slices.load(ORD),
            writeback_live_unknown_origin_bytes: self.writeback_live_unknown_origin_bytes.load(ORD),
            writeback_live_unknown_origin_slices: self
                .writeback_live_unknown_origin_slices
                .load(ORD),
            writeback_recent_pending_upload_bytes: self
                .writeback_recent_pending_upload_bytes
                .load(ORD),
            writeback_recent_pending_upload_slices: self
                .writeback_recent_pending_upload_slices
                .load(ORD),
            writeback_recent_uploaded_bytes: self.writeback_recent_uploaded_bytes.load(ORD),
            writeback_recent_uploaded_slices: self.writeback_recent_uploaded_slices.load(ORD),
            writeback_backpressure_soft_sleep_ops: self
                .writeback_backpressure_soft_sleep_ops
                .load(ORD),
            writeback_backpressure_soft_sleep_us: self
                .writeback_backpressure_soft_sleep_us
                .load(ORD),
            writeback_backpressure_hard_wait_ops: self
                .writeback_backpressure_hard_wait_ops
                .load(ORD),
            writeback_backpressure_hard_wait_us: self.writeback_backpressure_hard_wait_us.load(ORD),
            writeback_buffer_soft_sleep_ops: self.writeback_buffer_soft_sleep_ops.load(ORD),
            writeback_buffer_soft_sleep_us: self.writeback_buffer_soft_sleep_us.load(ORD),
            writeback_buffer_moderate_sleep_ops: self.writeback_buffer_moderate_sleep_ops.load(ORD),
            writeback_buffer_moderate_sleep_us: self.writeback_buffer_moderate_sleep_us.load(ORD),
            writeback_buffer_hard_sleep_ops: self.writeback_buffer_hard_sleep_ops.load(ORD),
            writeback_buffer_hard_sleep_us: self.writeback_buffer_hard_sleep_us.load(ORD),
            writeback_stage_inflight_bytes: self.writeback_stage_inflight_bytes.load(ORD),
            writeback_remote_upload_inflight_bytes: self
                .writeback_remote_upload_inflight_bytes
                .load(ORD),
            writeback_metadata_inflight: self.writeback_metadata_inflight.load(ORD),
            writeback_stage_ops: self.writeback_stage_ops.load(ORD),
            writeback_stage_bytes: self.writeback_stage_bytes.load(ORD),
            writeback_stage_lat_us: self.writeback_stage_lat_us.load(ORD),
            writeback_stage_failures: self.writeback_stage_failures.load(ORD),
            writeback_commit_before_stage_ops: self.writeback_commit_before_stage_ops.load(ORD),
            writeback_commit_wait_upload_ops: self.writeback_commit_wait_upload_ops.load(ORD),
            writeback_commit_wait_upload_us: self.writeback_commit_wait_upload_us.load(ORD),
            writeback_commit_wait_upload_size_ops: self
                .writeback_commit_wait_upload_size_ops
                .load(ORD),
            writeback_commit_wait_upload_size_us: self
                .writeback_commit_wait_upload_size_us
                .load(ORD),
            writeback_commit_wait_upload_max_unflushed_ops: self
                .writeback_commit_wait_upload_max_unflushed_ops
                .load(ORD),
            writeback_commit_wait_upload_max_unflushed_us: self
                .writeback_commit_wait_upload_max_unflushed_us
                .load(ORD),
            writeback_commit_wait_upload_explicit_flush_ops: self
                .writeback_commit_wait_upload_explicit_flush_ops
                .load(ORD),
            writeback_commit_wait_upload_explicit_flush_us: self
                .writeback_commit_wait_upload_explicit_flush_us
                .load(ORD),
            writeback_commit_wait_upload_auto_ops: self
                .writeback_commit_wait_upload_auto_ops
                .load(ORD),
            writeback_commit_wait_upload_auto_us: self
                .writeback_commit_wait_upload_auto_us
                .load(ORD),
            writeback_commit_wait_upload_commit_age_ops: self
                .writeback_commit_wait_upload_commit_age_ops
                .load(ORD),
            writeback_commit_wait_upload_commit_age_us: self
                .writeback_commit_wait_upload_commit_age_us
                .load(ORD),
            writeback_commit_wait_upload_unknown_reason_ops: self
                .writeback_commit_wait_upload_unknown_reason_ops
                .load(ORD),
            writeback_commit_wait_upload_unknown_reason_us: self
                .writeback_commit_wait_upload_unknown_reason_us
                .load(ORD),
            writeback_commit_wait_upload_normal_only_ops: self
                .writeback_commit_wait_upload_normal_only_ops
                .load(ORD),
            writeback_commit_wait_upload_normal_only_us: self
                .writeback_commit_wait_upload_normal_only_us
                .load(ORD),
            writeback_commit_wait_upload_cached_only_ops: self
                .writeback_commit_wait_upload_cached_only_ops
                .load(ORD),
            writeback_commit_wait_upload_cached_only_us: self
                .writeback_commit_wait_upload_cached_only_us
                .load(ORD),
            writeback_commit_wait_upload_mixed_origin_ops: self
                .writeback_commit_wait_upload_mixed_origin_ops
                .load(ORD),
            writeback_commit_wait_upload_mixed_origin_us: self
                .writeback_commit_wait_upload_mixed_origin_us
                .load(ORD),
            writeback_commit_wait_upload_unknown_origin_ops: self
                .writeback_commit_wait_upload_unknown_origin_ops
                .load(ORD),
            writeback_commit_wait_upload_unknown_origin_us: self
                .writeback_commit_wait_upload_unknown_origin_us
                .load(ORD),
            writeback_commit_wait_retry_ops: self.writeback_commit_wait_retry_ops.load(ORD),
            writeback_commit_wait_retry_us: self.writeback_commit_wait_retry_us.load(ORD),
            writeback_flush_wait_ops: self.writeback_flush_wait_ops.load(ORD),
            writeback_flush_wait_us: self.writeback_flush_wait_us.load(ORD),
            writeback_flush_wait_slices: self.writeback_flush_wait_slices.load(ORD),
            writeback_flush_fragmentation_ops: self.writeback_flush_fragmentation_ops.load(ORD),
            writeback_flush_fragmentation_slices: self
                .writeback_flush_fragmentation_slices
                .load(ORD),
            writeback_flush_fragmentation_bytes: self.writeback_flush_fragmentation_bytes.load(ORD),
            writeback_flush_fragmentation_cached_sub_block_slices: self
                .writeback_flush_fragmentation_cached_sub_block_slices
                .load(ORD),
            writeback_flush_fragmentation_cached_sub_block_bytes: self
                .writeback_flush_fragmentation_cached_sub_block_bytes
                .load(ORD),
            writeback_flush_fragmentation_full_block_slices: self
                .writeback_flush_fragmentation_full_block_slices
                .load(ORD),
            writeback_flush_fragmentation_full_block_bytes: self
                .writeback_flush_fragmentation_full_block_bytes
                .load(ORD),
            writeback_slice_create_ops: self.writeback_slice_create_ops.load(ORD),
            writeback_slice_reuse_ops: self.writeback_slice_reuse_ops.load(ORD),
            writeback_slice_reject_older_unique_ops: self
                .writeback_slice_reject_older_unique_ops
                .load(ORD),
            writeback_slice_reject_dispatched_prefix_ops: self
                .writeback_slice_reject_dispatched_prefix_ops
                .load(ORD),
            writeback_freeze_size_ops: self.writeback_freeze_size_ops.load(ORD),
            writeback_freeze_size_bytes: self.writeback_freeze_size_bytes.load(ORD),
            writeback_freeze_max_unflushed_ops: self.writeback_freeze_max_unflushed_ops.load(ORD),
            writeback_freeze_max_unflushed_bytes: self
                .writeback_freeze_max_unflushed_bytes
                .load(ORD),
            writeback_freeze_explicit_flush_ops: self.writeback_freeze_explicit_flush_ops.load(ORD),
            writeback_freeze_explicit_flush_bytes: self
                .writeback_freeze_explicit_flush_bytes
                .load(ORD),
            writeback_freeze_auto_ops: self.writeback_freeze_auto_ops.load(ORD),
            writeback_freeze_auto_bytes: self.writeback_freeze_auto_bytes.load(ORD),
            writeback_freeze_commit_age_ops: self.writeback_freeze_commit_age_ops.load(ORD),
            writeback_freeze_commit_age_bytes: self.writeback_freeze_commit_age_bytes.load(ORD),
            writeback_upload_batch_ops: self.writeback_upload_batch_ops.load(ORD),
            writeback_upload_batch_bytes: self.writeback_upload_batch_bytes.load(ORD),
            writeback_upload_batch_blocks: self.writeback_upload_batch_blocks.load(ORD),
            writeback_upload_batch_single_block_ops: self
                .writeback_upload_batch_single_block_ops
                .load(ORD),
            writeback_upload_batch_multi_block_ops: self
                .writeback_upload_batch_multi_block_ops
                .load(ORD),
            writeback_upload_partial_tail_ops: self.writeback_upload_partial_tail_ops.load(ORD),
            writeback_upload_partial_tail_size_ops: self
                .writeback_upload_partial_tail_size_ops
                .load(ORD),
            writeback_upload_partial_tail_max_unflushed_ops: self
                .writeback_upload_partial_tail_max_unflushed_ops
                .load(ORD),
            writeback_upload_partial_tail_explicit_flush_ops: self
                .writeback_upload_partial_tail_explicit_flush_ops
                .load(ORD),
            writeback_upload_partial_tail_auto_ops: self
                .writeback_upload_partial_tail_auto_ops
                .load(ORD),
            writeback_upload_partial_tail_normal_only_ops: self
                .writeback_upload_partial_tail_normal_only_ops
                .load(ORD),
            writeback_upload_partial_tail_cached_only_ops: self
                .writeback_upload_partial_tail_cached_only_ops
                .load(ORD),
            writeback_upload_partial_tail_mixed_origin_ops: self
                .writeback_upload_partial_tail_mixed_origin_ops
                .load(ORD),
            writeback_upload_partial_tail_unknown_origin_ops: self
                .writeback_upload_partial_tail_unknown_origin_ops
                .load(ORD),
            writeback_upload_partial_tail_auto_age_ops: self
                .writeback_upload_partial_tail_auto_age_ops
                .load(ORD),
            writeback_upload_partial_tail_auto_idle_ops: self
                .writeback_upload_partial_tail_auto_idle_ops
                .load(ORD),
            writeback_upload_partial_tail_auto_pressure_ops: self
                .writeback_upload_partial_tail_auto_pressure_ops
                .load(ORD),
            writeback_upload_partial_tail_auto_too_many_ops: self
                .writeback_upload_partial_tail_auto_too_many_ops
                .load(ORD),
            writeback_upload_partial_tail_auto_buffer_high_ops: self
                .writeback_upload_partial_tail_auto_buffer_high_ops
                .load(ORD),
            writeback_upload_partial_tail_auto_flush_duration_ops: self
                .writeback_upload_partial_tail_auto_flush_duration_ops
                .load(ORD),
            writeback_upload_partial_tail_auto_unknown_ops: self
                .writeback_upload_partial_tail_auto_unknown_ops
                .load(ORD),
            writeback_upload_partial_tail_auto_normal_only_ops: self
                .writeback_upload_partial_tail_auto_normal_only_ops
                .load(ORD),
            writeback_upload_partial_tail_auto_cached_only_ops: self
                .writeback_upload_partial_tail_auto_cached_only_ops
                .load(ORD),
            writeback_upload_partial_tail_auto_mixed_origin_ops: self
                .writeback_upload_partial_tail_auto_mixed_origin_ops
                .load(ORD),
            writeback_upload_partial_tail_auto_unknown_origin_ops: self
                .writeback_upload_partial_tail_auto_unknown_origin_ops
                .load(ORD),
            writeback_upload_partial_tail_commit_age_ops: self
                .writeback_upload_partial_tail_commit_age_ops
                .load(ORD),
            cache_hits: self.cache_hits.load(ORD),
            cache_misses: self.cache_misses.load(ORD),
            read_block_cache_hits: self.read_block_cache_hits.load(ORD),
            read_page_cache_hits: self.read_page_cache_hits.load(ORD),
            read_page_cache_misses: self.read_page_cache_misses.load(ORD),
            read_range_gets: self.read_range_gets.load(ORD),
            read_full_gets: self.read_full_gets.load(ORD),
            read_piggyback_full: self.read_piggyback_full.load(ORD),
            read_background_prefetches: self.read_background_prefetches.load(ORD),
            read_background_prefetch_dropped: self.read_background_prefetch_dropped.load(ORD),
            meta_stat_cache_hit: self.meta_stat_cache_hit.load(ORD),
            meta_stat_cache_miss: self.meta_stat_cache_miss.load(ORD),
            meta_stat_fresh_store_hit: self.meta_stat_fresh_store_hit.load(ORD),
            meta_lookup_cache_hit: self.meta_lookup_cache_hit.load(ORD),
            meta_lookup_cache_miss: self.meta_lookup_cache_miss.load(ORD),
            meta_get_slices_cache_hit: self.meta_get_slices_cache_hit.load(ORD),
            meta_get_slices_cache_miss: self.meta_get_slices_cache_miss.load(ORD),
            meta_open_fresh_stat: self.meta_open_fresh_stat.load(ORD),
            meta_open_file_cache_hit: self.meta_open_file_cache_hit.load(ORD),
            meta_open_file_cache_miss: self.meta_open_file_cache_miss.load(ORD),
            meta_lookup_attr_fused_hit: self.meta_lookup_attr_fused_hit.load(ORD),
            meta_lookup_attr_fused_miss: self.meta_lookup_attr_fused_miss.load(ORD),
            meta_lookup_attr_fused_error: self.meta_lookup_attr_fused_error.load(ORD),
        }
    }

    pub fn sync_cache_counters(&self, hits: u64, misses: u64) {
        self.cache_hits.store(hits, ORD);
        self.cache_misses.store(misses, ORD);
    }

    pub fn sync_buffer_bytes(&self, dirty_bytes: u64, read_bytes: u64) {
        self.buf_dirty_bytes.store(dirty_bytes, ORD);
        self.buf_read_bytes.store(read_bytes, ORD);
    }

    pub fn sync_memory_allocator_metrics(&self, cas_retries: u64, allocation_rejections: u64) {
        self.memory_cas_retries.store(cas_retries, ORD);
        self.memory_allocation_rejections
            .store(allocation_rejections, ORD);
    }

    pub fn sync_writeback_dirty_breakdown(
        &self,
        live_bytes: u64,
        live_slices: u64,
        recent_pending_upload_bytes: u64,
        recent_pending_upload_slices: u64,
        recent_uploaded_bytes: u64,
        recent_uploaded_slices: u64,
    ) {
        self.writeback_live_dirty_bytes.store(live_bytes, ORD);
        self.writeback_live_slices.store(live_slices, ORD);
        self.writeback_recent_pending_upload_bytes
            .store(recent_pending_upload_bytes, ORD);
        self.writeback_recent_pending_upload_slices
            .store(recent_pending_upload_slices, ORD);
        self.writeback_recent_uploaded_bytes
            .store(recent_uploaded_bytes, ORD);
        self.writeback_recent_uploaded_slices
            .store(recent_uploaded_slices, ORD);
    }

    #[allow(clippy::too_many_arguments)]
    pub fn sync_writeback_live_origin_metrics(
        &self,
        normal_only_bytes: u64,
        normal_only_slices: u64,
        cached_only_bytes: u64,
        cached_only_slices: u64,
        mixed_origin_bytes: u64,
        mixed_origin_slices: u64,
        unknown_origin_bytes: u64,
        unknown_origin_slices: u64,
    ) {
        self.writeback_live_normal_only_bytes
            .store(normal_only_bytes, ORD);
        self.writeback_live_normal_only_slices
            .store(normal_only_slices, ORD);
        self.writeback_live_cached_only_bytes
            .store(cached_only_bytes, ORD);
        self.writeback_live_cached_only_slices
            .store(cached_only_slices, ORD);
        self.writeback_live_mixed_origin_bytes
            .store(mixed_origin_bytes, ORD);
        self.writeback_live_mixed_origin_slices
            .store(mixed_origin_slices, ORD);
        self.writeback_live_unknown_origin_bytes
            .store(unknown_origin_bytes, ORD);
        self.writeback_live_unknown_origin_slices
            .store(unknown_origin_slices, ORD);
    }

    pub fn add_writeback_backpressure_soft_sleep(&self, duration: Duration) {
        self.writeback_backpressure_soft_sleep_ops.fetch_add(1, ORD);
        self.writeback_backpressure_soft_sleep_us
            .fetch_add(duration.as_micros().min(u128::from(u64::MAX)) as u64, ORD);
    }

    pub fn add_writeback_backpressure_hard_wait(&self, duration: Duration) {
        self.writeback_backpressure_hard_wait_ops.fetch_add(1, ORD);
        self.writeback_backpressure_hard_wait_us
            .fetch_add(duration.as_micros().min(u128::from(u64::MAX)) as u64, ORD);
    }

    pub fn sync_writeback_backpressure_metrics(
        &self,
        soft_sleep_ops: u64,
        soft_sleep_us: u64,
        hard_wait_ops: u64,
        hard_wait_us: u64,
    ) {
        self.writeback_backpressure_soft_sleep_ops
            .store(soft_sleep_ops, ORD);
        self.writeback_backpressure_soft_sleep_us
            .store(soft_sleep_us, ORD);
        self.writeback_backpressure_hard_wait_ops
            .store(hard_wait_ops, ORD);
        self.writeback_backpressure_hard_wait_us
            .store(hard_wait_us, ORD);
    }

    pub fn sync_writeback_buffer_backpressure_metrics(
        &self,
        soft_sleep_ops: u64,
        soft_sleep_us: u64,
        moderate_sleep_ops: u64,
        moderate_sleep_us: u64,
        hard_sleep_ops: u64,
        hard_sleep_us: u64,
    ) {
        self.writeback_buffer_soft_sleep_ops
            .store(soft_sleep_ops, ORD);
        self.writeback_buffer_soft_sleep_us
            .store(soft_sleep_us, ORD);
        self.writeback_buffer_moderate_sleep_ops
            .store(moderate_sleep_ops, ORD);
        self.writeback_buffer_moderate_sleep_us
            .store(moderate_sleep_us, ORD);
        self.writeback_buffer_hard_sleep_ops
            .store(hard_sleep_ops, ORD);
        self.writeback_buffer_hard_sleep_us
            .store(hard_sleep_us, ORD);
    }

    #[allow(clippy::too_many_arguments)]
    pub fn sync_writeback_phase_metrics(
        &self,
        stage_inflight_bytes: u64,
        remote_upload_inflight_bytes: u64,
        stage_ops: u64,
        stage_bytes: u64,
        stage_lat_us: u64,
        stage_failures: u64,
        commit_before_stage_ops: u64,
    ) {
        self.writeback_stage_inflight_bytes
            .store(stage_inflight_bytes, ORD);
        self.writeback_remote_upload_inflight_bytes
            .store(remote_upload_inflight_bytes, ORD);
        self.writeback_stage_ops.store(stage_ops, ORD);
        self.writeback_stage_bytes.store(stage_bytes, ORD);
        self.writeback_stage_lat_us.store(stage_lat_us, ORD);
        self.writeback_stage_failures.store(stage_failures, ORD);
        self.writeback_commit_before_stage_ops
            .store(commit_before_stage_ops, ORD);
    }

    /// Synchronize the number of metadata publication calls currently in
    /// flight. Kept separate from the long-standing phase method so adding
    /// this diagnostic gauge does not change its public argument order.
    pub fn sync_writeback_metadata_inflight(&self, metadata_inflight: u64) {
        self.writeback_metadata_inflight
            .store(metadata_inflight, ORD);
    }

    pub fn sync_writeback_commit_wait_metrics(
        &self,
        upload_ops: u64,
        upload_us: u64,
        retry_ops: u64,
        retry_us: u64,
    ) {
        self.writeback_commit_wait_upload_ops.store(upload_ops, ORD);
        self.writeback_commit_wait_upload_us.store(upload_us, ORD);
        self.writeback_commit_wait_retry_ops.store(retry_ops, ORD);
        self.writeback_commit_wait_retry_us.store(retry_us, ORD);
    }

    pub fn sync_writeback_flush_wait_metrics(&self, ops: u64, us: u64, slices: u64) {
        self.writeback_flush_wait_ops.store(ops, ORD);
        self.writeback_flush_wait_us.store(us, ORD);
        self.writeback_flush_wait_slices.store(slices, ORD);
    }

    #[allow(clippy::too_many_arguments)]
    pub fn sync_writeback_flush_fragmentation_metrics(
        &self,
        ops: u64,
        slices: u64,
        bytes: u64,
        cached_sub_block_slices: u64,
        cached_sub_block_bytes: u64,
        full_block_slices: u64,
        full_block_bytes: u64,
    ) {
        self.writeback_flush_fragmentation_ops.store(ops, ORD);
        self.writeback_flush_fragmentation_slices.store(slices, ORD);
        self.writeback_flush_fragmentation_bytes.store(bytes, ORD);
        self.writeback_flush_fragmentation_cached_sub_block_slices
            .store(cached_sub_block_slices, ORD);
        self.writeback_flush_fragmentation_cached_sub_block_bytes
            .store(cached_sub_block_bytes, ORD);
        self.writeback_flush_fragmentation_full_block_slices
            .store(full_block_slices, ORD);
        self.writeback_flush_fragmentation_full_block_bytes
            .store(full_block_bytes, ORD);
    }

    #[allow(clippy::too_many_arguments)]
    pub fn sync_writeback_commit_wait_breakdown_metrics(
        &self,
        size_ops: u64,
        size_us: u64,
        max_unflushed_ops: u64,
        max_unflushed_us: u64,
        explicit_flush_ops: u64,
        explicit_flush_us: u64,
        auto_ops: u64,
        auto_us: u64,
        commit_age_ops: u64,
        commit_age_us: u64,
        unknown_reason_ops: u64,
        unknown_reason_us: u64,
        normal_only_ops: u64,
        normal_only_us: u64,
        cached_only_ops: u64,
        cached_only_us: u64,
        mixed_origin_ops: u64,
        mixed_origin_us: u64,
        unknown_origin_ops: u64,
        unknown_origin_us: u64,
    ) {
        self.writeback_commit_wait_upload_size_ops
            .store(size_ops, ORD);
        self.writeback_commit_wait_upload_size_us
            .store(size_us, ORD);
        self.writeback_commit_wait_upload_max_unflushed_ops
            .store(max_unflushed_ops, ORD);
        self.writeback_commit_wait_upload_max_unflushed_us
            .store(max_unflushed_us, ORD);
        self.writeback_commit_wait_upload_explicit_flush_ops
            .store(explicit_flush_ops, ORD);
        self.writeback_commit_wait_upload_explicit_flush_us
            .store(explicit_flush_us, ORD);
        self.writeback_commit_wait_upload_auto_ops
            .store(auto_ops, ORD);
        self.writeback_commit_wait_upload_auto_us
            .store(auto_us, ORD);
        self.writeback_commit_wait_upload_commit_age_ops
            .store(commit_age_ops, ORD);
        self.writeback_commit_wait_upload_commit_age_us
            .store(commit_age_us, ORD);
        self.writeback_commit_wait_upload_unknown_reason_ops
            .store(unknown_reason_ops, ORD);
        self.writeback_commit_wait_upload_unknown_reason_us
            .store(unknown_reason_us, ORD);
        self.writeback_commit_wait_upload_normal_only_ops
            .store(normal_only_ops, ORD);
        self.writeback_commit_wait_upload_normal_only_us
            .store(normal_only_us, ORD);
        self.writeback_commit_wait_upload_cached_only_ops
            .store(cached_only_ops, ORD);
        self.writeback_commit_wait_upload_cached_only_us
            .store(cached_only_us, ORD);
        self.writeback_commit_wait_upload_mixed_origin_ops
            .store(mixed_origin_ops, ORD);
        self.writeback_commit_wait_upload_mixed_origin_us
            .store(mixed_origin_us, ORD);
        self.writeback_commit_wait_upload_unknown_origin_ops
            .store(unknown_origin_ops, ORD);
        self.writeback_commit_wait_upload_unknown_origin_us
            .store(unknown_origin_us, ORD);
    }

    pub fn sync_writeback_slice_selection_metrics(
        &self,
        create_ops: u64,
        reuse_ops: u64,
        reject_older_unique_ops: u64,
        reject_dispatched_prefix_ops: u64,
    ) {
        self.writeback_slice_create_ops.store(create_ops, ORD);
        self.writeback_slice_reuse_ops.store(reuse_ops, ORD);
        self.writeback_slice_reject_older_unique_ops
            .store(reject_older_unique_ops, ORD);
        self.writeback_slice_reject_dispatched_prefix_ops
            .store(reject_dispatched_prefix_ops, ORD);
    }

    #[allow(clippy::too_many_arguments)]
    pub fn sync_writeback_freeze_metrics(
        &self,
        size_ops: u64,
        size_bytes: u64,
        max_unflushed_ops: u64,
        max_unflushed_bytes: u64,
        explicit_flush_ops: u64,
        explicit_flush_bytes: u64,
        auto_ops: u64,
        auto_bytes: u64,
        commit_age_ops: u64,
        commit_age_bytes: u64,
    ) {
        self.writeback_freeze_size_ops.store(size_ops, ORD);
        self.writeback_freeze_size_bytes.store(size_bytes, ORD);
        self.writeback_freeze_max_unflushed_ops
            .store(max_unflushed_ops, ORD);
        self.writeback_freeze_max_unflushed_bytes
            .store(max_unflushed_bytes, ORD);
        self.writeback_freeze_explicit_flush_ops
            .store(explicit_flush_ops, ORD);
        self.writeback_freeze_explicit_flush_bytes
            .store(explicit_flush_bytes, ORD);
        self.writeback_freeze_auto_ops.store(auto_ops, ORD);
        self.writeback_freeze_auto_bytes.store(auto_bytes, ORD);
        self.writeback_freeze_commit_age_ops
            .store(commit_age_ops, ORD);
        self.writeback_freeze_commit_age_bytes
            .store(commit_age_bytes, ORD);
    }

    #[allow(clippy::too_many_arguments)]
    pub fn sync_writeback_upload_batch_metrics(
        &self,
        ops: u64,
        bytes: u64,
        blocks: u64,
        partial_tail_ops: u64,
        partial_tail_size_ops: u64,
        partial_tail_max_unflushed_ops: u64,
        partial_tail_explicit_flush_ops: u64,
        partial_tail_auto_ops: u64,
        partial_tail_auto_age_ops: u64,
        partial_tail_auto_idle_ops: u64,
        partial_tail_auto_pressure_ops: u64,
        partial_tail_auto_too_many_ops: u64,
        partial_tail_auto_buffer_high_ops: u64,
        partial_tail_auto_flush_duration_ops: u64,
        partial_tail_auto_unknown_ops: u64,
        partial_tail_commit_age_ops: u64,
    ) {
        self.writeback_upload_batch_ops.store(ops, ORD);
        self.writeback_upload_batch_bytes.store(bytes, ORD);
        self.writeback_upload_batch_blocks.store(blocks, ORD);
        self.writeback_upload_partial_tail_ops
            .store(partial_tail_ops, ORD);
        self.writeback_upload_partial_tail_size_ops
            .store(partial_tail_size_ops, ORD);
        self.writeback_upload_partial_tail_max_unflushed_ops
            .store(partial_tail_max_unflushed_ops, ORD);
        self.writeback_upload_partial_tail_explicit_flush_ops
            .store(partial_tail_explicit_flush_ops, ORD);
        self.writeback_upload_partial_tail_auto_ops
            .store(partial_tail_auto_ops, ORD);
        self.writeback_upload_partial_tail_auto_age_ops
            .store(partial_tail_auto_age_ops, ORD);
        self.writeback_upload_partial_tail_auto_idle_ops
            .store(partial_tail_auto_idle_ops, ORD);
        self.writeback_upload_partial_tail_auto_pressure_ops
            .store(partial_tail_auto_pressure_ops, ORD);
        self.writeback_upload_partial_tail_auto_too_many_ops
            .store(partial_tail_auto_too_many_ops, ORD);
        self.writeback_upload_partial_tail_auto_buffer_high_ops
            .store(partial_tail_auto_buffer_high_ops, ORD);
        self.writeback_upload_partial_tail_auto_flush_duration_ops
            .store(partial_tail_auto_flush_duration_ops, ORD);
        self.writeback_upload_partial_tail_auto_unknown_ops
            .store(partial_tail_auto_unknown_ops, ORD);
        self.writeback_upload_partial_tail_commit_age_ops
            .store(partial_tail_commit_age_ops, ORD);
    }

    pub fn sync_writeback_upload_batch_shape_metrics(
        &self,
        single_block_ops: u64,
        multi_block_ops: u64,
    ) {
        self.writeback_upload_batch_single_block_ops
            .store(single_block_ops, ORD);
        self.writeback_upload_batch_multi_block_ops
            .store(multi_block_ops, ORD);
    }

    #[allow(clippy::too_many_arguments)]
    pub fn sync_writeback_upload_origin_metrics(
        &self,
        partial_tail_normal_only_ops: u64,
        partial_tail_cached_only_ops: u64,
        partial_tail_mixed_origin_ops: u64,
        partial_tail_unknown_origin_ops: u64,
        partial_tail_auto_normal_only_ops: u64,
        partial_tail_auto_cached_only_ops: u64,
        partial_tail_auto_mixed_origin_ops: u64,
        partial_tail_auto_unknown_origin_ops: u64,
    ) {
        self.writeback_upload_partial_tail_normal_only_ops
            .store(partial_tail_normal_only_ops, ORD);
        self.writeback_upload_partial_tail_cached_only_ops
            .store(partial_tail_cached_only_ops, ORD);
        self.writeback_upload_partial_tail_mixed_origin_ops
            .store(partial_tail_mixed_origin_ops, ORD);
        self.writeback_upload_partial_tail_unknown_origin_ops
            .store(partial_tail_unknown_origin_ops, ORD);
        self.writeback_upload_partial_tail_auto_normal_only_ops
            .store(partial_tail_auto_normal_only_ops, ORD);
        self.writeback_upload_partial_tail_auto_cached_only_ops
            .store(partial_tail_auto_cached_only_ops, ORD);
        self.writeback_upload_partial_tail_auto_mixed_origin_ops
            .store(partial_tail_auto_mixed_origin_ops, ORD);
        self.writeback_upload_partial_tail_auto_unknown_origin_ops
            .store(partial_tail_auto_unknown_origin_ops, ORD);
    }

    #[allow(clippy::too_many_arguments)]
    pub fn sync_object_store_metrics(
        &self,
        get_ops: u64,
        get_bytes: u64,
        get_lat_us: u64,
        put_ops: u64,
        put_bytes: u64,
        put_lat_us: u64,
        put_prepare_lat_us: u64,
        put_cache_lat_us: u64,
        del_ops: u64,
    ) {
        self.s3_get_ops.store(get_ops, ORD);
        self.s3_get_bytes.store(get_bytes, ORD);
        self.s3_get_lat_us.store(get_lat_us, ORD);
        self.s3_put_ops.store(put_ops, ORD);
        self.s3_put_bytes.store(put_bytes, ORD);
        self.s3_put_lat_us.store(put_lat_us, ORD);
        self.s3_put_prepare_lat_us.store(put_prepare_lat_us, ORD);
        self.s3_put_cache_lat_us.store(put_cache_lat_us, ORD);
        self.s3_del_ops.store(del_ops, ORD);
    }

    #[allow(clippy::too_many_arguments)]
    pub fn sync_read_strategy_metrics(
        &self,
        block_cache_hits: u64,
        page_cache_hits: u64,
        page_cache_misses: u64,
        range_gets: u64,
        full_gets: u64,
        piggyback_full: u64,
        background_prefetches: u64,
        background_prefetch_dropped: u64,
    ) {
        self.read_block_cache_hits.store(block_cache_hits, ORD);
        self.read_page_cache_hits.store(page_cache_hits, ORD);
        self.read_page_cache_misses.store(page_cache_misses, ORD);
        self.read_range_gets.store(range_gets, ORD);
        self.read_full_gets.store(full_gets, ORD);
        self.read_piggyback_full.store(piggyback_full, ORD);
        self.read_background_prefetches
            .store(background_prefetches, ORD);
        self.read_background_prefetch_dropped
            .store(background_prefetch_dropped, ORD);
    }

    #[allow(clippy::too_many_arguments)]
    pub fn sync_meta_client_metrics(
        &self,
        stat_cache_hit: u64,
        stat_cache_miss: u64,
        stat_fresh_store_hit: u64,
        lookup_cache_hit: u64,
        lookup_cache_miss: u64,
        get_slices_cache_hit: u64,
        get_slices_cache_miss: u64,
        open_fresh_stat: u64,
        open_file_cache_hit: u64,
        open_file_cache_miss: u64,
        lookup_attr_fused_hit: u64,
        lookup_attr_fused_miss: u64,
        lookup_attr_fused_error: u64,
    ) {
        self.meta_stat_cache_hit.store(stat_cache_hit, ORD);
        self.meta_stat_cache_miss.store(stat_cache_miss, ORD);
        self.meta_stat_fresh_store_hit
            .store(stat_fresh_store_hit, ORD);
        self.meta_lookup_cache_hit.store(lookup_cache_hit, ORD);
        self.meta_lookup_cache_miss.store(lookup_cache_miss, ORD);
        self.meta_get_slices_cache_hit
            .store(get_slices_cache_hit, ORD);
        self.meta_get_slices_cache_miss
            .store(get_slices_cache_miss, ORD);
        self.meta_open_fresh_stat.store(open_fresh_stat, ORD);
        self.meta_open_file_cache_hit
            .store(open_file_cache_hit, ORD);
        self.meta_open_file_cache_miss
            .store(open_file_cache_miss, ORD);
        self.meta_lookup_attr_fused_hit
            .store(lookup_attr_fused_hit, ORD);
        self.meta_lookup_attr_fused_miss
            .store(lookup_attr_fused_miss, ORD);
        self.meta_lookup_attr_fused_error
            .store(lookup_attr_fused_error, ORD);
    }

    /// Record the validity supplied with the most recent FUSE attribute reply.
    pub fn record_fuse_attr_valid(&self, valid: Duration) {
        self.fuse_attr_valid_ns
            .store(valid.as_nanos().min(u128::from(u64::MAX)) as u64, ORD);
    }

    /// Record the validity supplied with the most recent FUSE entry reply.
    pub fn record_fuse_entry_valid(&self, valid: Duration) {
        self.fuse_entry_valid_ns
            .store(valid.as_nanos().min(u128::from(u64::MAX)) as u64, ORD);
    }

    pub fn set_extension(&self, extension: std::sync::Arc<dyn FsStatsExtension>) -> bool {
        self.extension.set(extension).is_ok()
    }

    /// Shared request observer used by the FUSE session and VFS call sites.
    /// The returned handle is stable for the lifetime of the mount.
    pub fn request_observation(&self) -> Arc<RequestObservation> {
        Arc::clone(&self.request_observation)
    }

    /// Attach request context without cloning the observer handle on the hot
    /// FUSE path. Context is ignored while detailed observation is disabled.
    pub fn set_request_context(
        &self,
        unique: u64,
        file_id: Option<i64>,
        chunk_id: Option<u64>,
        slice_id: Option<u64>,
        lease_id: Option<u64>,
    ) -> bool {
        self.request_observation
            .set_context(unique, file_id, chunk_id, slice_id, lease_id)
    }

    /// Start a request segment timer without cloning an `Arc` when detailed
    /// observation is disabled.
    pub fn request_segment_timer(
        &self,
        unique: u64,
        segment: RequestSegment,
    ) -> Option<RequestSegmentTimer> {
        self.request_observation.segment_timer(unique, segment)
    }

    pub fn request_observation_snapshot(&self) -> RequestObservationSnapshot {
        self.request_observation.snapshot()
    }

    /// Render all counters in Prometheus text format (one metric per line).
    /// Format: `metric_name value\n`
    pub fn render_allocation_limit(&self) -> usize {
        Self::BASE_RENDER_MAX_BYTES
            .saturating_add(if self.request_observation.enabled() {
                REQUEST_TRACE_RECORD_LIMIT * (4096 + REQUEST_INTERVAL_LIMIT * 64)
            } else {
                0
            })
            .saturating_add(self.extension.get().map_or(0, |e| e.render_max_bytes()))
    }

    pub fn render(&self) -> String {
        self.render_with_limit(None)
            .expect("unbounded statistics rendering")
    }

    pub fn render_bounded(&self, limit: usize) -> Result<String, StatsRenderLimit> {
        self.render_with_limit(Some(limit))
    }

    fn render_with_limit(&self, limit: Option<usize>) -> Result<String, StatsRenderLimit> {
        let observation = self
            .extension
            .get()
            .and_then(|extension| extension.begin_stats_observation());
        let snapshot = self.snapshot();
        let mut out = StatsText::new(limit);

        // System
        out.push_str(&format!(
            "brewfs_uptime_seconds {}\n",
            snapshot.uptime_seconds
        ));

        // FUSE
        out.push_str(&format!(
            "brewfs_fuse_read_ops_total {}\n",
            self.fuse_read_ops.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_fuse_read_bytes_total {}\n",
            self.fuse_read_bytes.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_fuse_read_lat_us_total {}\n",
            self.fuse_read_lat_us.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_fuse_write_ops_total {}\n",
            self.fuse_write_ops.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_fuse_write_bytes_total {}\n",
            self.fuse_write_bytes.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_fuse_write_lat_us_total {}\n",
            self.fuse_write_lat_us.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_fuse_lookup_ops_total {}\n",
            self.fuse_lookup_ops.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_fuse_lookup_lat_us_total {}\n",
            self.fuse_lookup_lat_us.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_fuse_getattr_ops_total {}\n",
            self.fuse_getattr_ops.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_fuse_getattr_lat_us_total {}\n",
            self.fuse_getattr_lat_us.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_fuse_attr_valid_ns {}\n",
            self.fuse_attr_valid_ns.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_fuse_entry_valid_ns {}\n",
            self.fuse_entry_valid_ns.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_fuse_open_ops_total {}\n",
            self.fuse_open_ops.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_fuse_create_ops_total {}\n",
            self.fuse_create_ops.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_fuse_unlink_ops_total {}\n",
            self.fuse_unlink_ops.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_fuse_readdir_ops_total {}\n",
            self.fuse_readdir_ops.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_fuse_flush_ops_total {}\n",
            self.fuse_flush_ops.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_fuse_flush_lat_us_total {}\n",
            self.fuse_flush_lat_us.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_fuse_read_avg_lat_us {:.6}\n",
            snapshot.avg_fuse_read_lat_us()
        ));
        out.push_str(&format!(
            "brewfs_fuse_write_avg_lat_us {:.6}\n",
            snapshot.avg_fuse_write_lat_us()
        ));
        out.push_str(&format!(
            "brewfs_fuse_flush_avg_lat_us {:.6}\n",
            snapshot.avg_fuse_flush_lat_us()
        ));

        // Meta
        out.push_str(&format!(
            "brewfs_meta_ops_total {}\n",
            self.meta_ops.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_meta_lat_us_total {}\n",
            self.meta_lat_us.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_meta_txn_ops_total {}\n",
            self.meta_txn_ops.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_meta_txn_lat_us_total {}\n",
            self.meta_txn_lat_us.load(ORD)
        ));

        // VFS diagnostic timing
        out.push_str(&format!(
            "brewfs_vfs_create_total_ops_total {}\n",
            self.vfs_create_total_ops.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_vfs_create_total_lat_us_total {}\n",
            self.vfs_create_total_lat_us.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_vfs_create_meta_ops_total {}\n",
            self.vfs_create_meta_ops.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_vfs_create_meta_lat_us_total {}\n",
            self.vfs_create_meta_lat_us.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_vfs_unlink_total_ops_total {}\n",
            self.vfs_unlink_total_ops.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_vfs_unlink_total_lat_us_total {}\n",
            self.vfs_unlink_total_lat_us.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_vfs_unlink_lookup_ops_total {}\n",
            self.vfs_unlink_lookup_ops.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_vfs_unlink_lookup_lat_us_total {}\n",
            self.vfs_unlink_lookup_lat_us.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_vfs_unlink_stat_ops_total {}\n",
            self.vfs_unlink_stat_ops.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_vfs_unlink_stat_lat_us_total {}\n",
            self.vfs_unlink_stat_lat_us.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_vfs_unlink_meta_ops_total {}\n",
            self.vfs_unlink_meta_ops.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_vfs_unlink_meta_lat_us_total {}\n",
            self.vfs_unlink_meta_lat_us.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_vfs_unlink_recent_ops_total {}\n",
            self.vfs_unlink_recent_ops.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_vfs_unlink_recent_lat_us_total {}\n",
            self.vfs_unlink_recent_lat_us.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_vfs_setattr_recent_remove_ops_total {}\n",
            self.vfs_setattr_recent_remove_ops.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_vfs_setattr_recent_remove_lat_us_total {}\n",
            self.vfs_setattr_recent_remove_lat_us.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_vfs_setattr_recent_get_mut_ops_total {}\n",
            self.vfs_setattr_recent_get_mut_ops.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_vfs_setattr_recent_get_mut_lat_us_total {}\n",
            self.vfs_setattr_recent_get_mut_lat_us.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_vfs_read_dirty_probe_ops_total {}\n",
            self.vfs_read_dirty_probe_ops.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_vfs_read_dirty_probe_lat_us_total {}\n",
            self.vfs_read_dirty_probe_lat_us.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_vfs_read_handle_ops_total {}\n",
            self.vfs_read_handle_ops.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_vfs_read_handle_lat_us_total {}\n",
            self.vfs_read_handle_lat_us.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_vfs_read_overlay_ops_total {}\n",
            self.vfs_read_overlay_ops.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_vfs_read_overlay_lat_us_total {}\n",
            self.vfs_read_overlay_lat_us.load(ORD)
        ));

        // Object storage
        out.push_str(&format!(
            "brewfs_s3_get_ops_total {}\n",
            self.s3_get_ops.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_s3_get_bytes_total {}\n",
            self.s3_get_bytes.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_s3_get_lat_us_total {}\n",
            self.s3_get_lat_us.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_s3_put_ops_total {}\n",
            self.s3_put_ops.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_s3_put_bytes_total {}\n",
            self.s3_put_bytes.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_s3_put_lat_us_total {}\n",
            self.s3_put_lat_us.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_s3_put_prepare_lat_us_total {}\n",
            self.s3_put_prepare_lat_us.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_s3_put_cache_lat_us_total {}\n",
            self.s3_put_cache_lat_us.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_s3_del_ops_total {}\n",
            self.s3_del_ops.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_s3_get_avg_lat_us {:.6}\n",
            snapshot.avg_s3_get_lat_us()
        ));
        out.push_str(&format!(
            "brewfs_s3_put_avg_lat_us {:.6}\n",
            snapshot.avg_s3_put_lat_us()
        ));
        out.push_str(&format!(
            "brewfs_s3_put_prepare_avg_lat_us {:.6}\n",
            snapshot.avg_s3_put_prepare_lat_us()
        ));
        out.push_str(&format!(
            "brewfs_s3_put_cache_avg_lat_us {:.6}\n",
            snapshot.avg_s3_put_cache_lat_us()
        ));

        // Buffer/cache
        out.push_str(&format!(
            "brewfs_buffer_dirty_bytes {}\n",
            self.buf_dirty_bytes.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_buffer_read_bytes {}\n",
            self.buf_read_bytes.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_memory_cas_retries_total {}\n",
            snapshot.memory_cas_retries
        ));
        out.push_str(&format!(
            "brewfs_memory_allocation_rejections_total {}\n",
            snapshot.memory_allocation_rejections
        ));
        out.push_str(&format!(
            "brewfs_writeback_dirty_bytes {}\n",
            snapshot.buf_dirty_bytes
        ));
        out.push_str(&format!(
            "brewfs_writeback_live_dirty_bytes {}\n",
            snapshot.writeback_live_dirty_bytes
        ));
        out.push_str(&format!(
            "brewfs_writeback_live_slices {}\n",
            snapshot.writeback_live_slices
        ));
        out.push_str(&format!(
            "brewfs_writeback_live_normal_only_bytes {}\n",
            snapshot.writeback_live_normal_only_bytes
        ));
        out.push_str(&format!(
            "brewfs_writeback_live_normal_only_slices {}\n",
            snapshot.writeback_live_normal_only_slices
        ));
        out.push_str(&format!(
            "brewfs_writeback_live_cached_only_bytes {}\n",
            snapshot.writeback_live_cached_only_bytes
        ));
        out.push_str(&format!(
            "brewfs_writeback_live_cached_only_slices {}\n",
            snapshot.writeback_live_cached_only_slices
        ));
        out.push_str(&format!(
            "brewfs_writeback_live_mixed_origin_bytes {}\n",
            snapshot.writeback_live_mixed_origin_bytes
        ));
        out.push_str(&format!(
            "brewfs_writeback_live_mixed_origin_slices {}\n",
            snapshot.writeback_live_mixed_origin_slices
        ));
        out.push_str(&format!(
            "brewfs_writeback_live_unknown_origin_bytes {}\n",
            snapshot.writeback_live_unknown_origin_bytes
        ));
        out.push_str(&format!(
            "brewfs_writeback_live_unknown_origin_slices {}\n",
            snapshot.writeback_live_unknown_origin_slices
        ));
        out.push_str(&format!(
            "brewfs_writeback_recent_pending_upload_bytes {}\n",
            snapshot.writeback_recent_pending_upload_bytes
        ));
        out.push_str(&format!(
            "brewfs_writeback_recent_pending_upload_slices {}\n",
            snapshot.writeback_recent_pending_upload_slices
        ));
        out.push_str(&format!(
            "brewfs_writeback_recent_uploaded_bytes {}\n",
            snapshot.writeback_recent_uploaded_bytes
        ));
        out.push_str(&format!(
            "brewfs_writeback_recent_uploaded_slices {}\n",
            snapshot.writeback_recent_uploaded_slices
        ));
        out.push_str(&format!(
            "brewfs_writeback_backpressure_soft_sleep_ops {}\n",
            snapshot.writeback_backpressure_soft_sleep_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_backpressure_soft_sleep_us {}\n",
            snapshot.writeback_backpressure_soft_sleep_us
        ));
        out.push_str(&format!(
            "brewfs_writeback_backpressure_hard_wait_ops {}\n",
            snapshot.writeback_backpressure_hard_wait_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_backpressure_hard_wait_us {}\n",
            snapshot.writeback_backpressure_hard_wait_us
        ));
        out.push_str(&format!(
            "brewfs_writeback_buffer_soft_sleep_ops {}\n",
            snapshot.writeback_buffer_soft_sleep_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_buffer_soft_sleep_us {}\n",
            snapshot.writeback_buffer_soft_sleep_us
        ));
        out.push_str(&format!(
            "brewfs_writeback_buffer_moderate_sleep_ops {}\n",
            snapshot.writeback_buffer_moderate_sleep_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_buffer_moderate_sleep_us {}\n",
            snapshot.writeback_buffer_moderate_sleep_us
        ));
        out.push_str(&format!(
            "brewfs_writeback_buffer_hard_sleep_ops {}\n",
            snapshot.writeback_buffer_hard_sleep_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_buffer_hard_sleep_us {}\n",
            snapshot.writeback_buffer_hard_sleep_us
        ));
        out.push_str(&format!(
            "brewfs_writeback_stage_inflight_bytes {}\n",
            snapshot.writeback_stage_inflight_bytes
        ));
        out.push_str(&format!(
            "brewfs_writeback_remote_upload_inflight_bytes {}\n",
            snapshot.writeback_remote_upload_inflight_bytes
        ));
        out.push_str(&format!(
            "brewfs_writeback_metadata_inflight {}\n",
            snapshot.writeback_metadata_inflight
        ));
        out.push_str(&format!(
            "brewfs_writeback_stage_ops_total {}\n",
            snapshot.writeback_stage_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_stage_bytes_total {}\n",
            snapshot.writeback_stage_bytes
        ));
        out.push_str(&format!(
            "brewfs_writeback_stage_lat_us_total {}\n",
            snapshot.writeback_stage_lat_us
        ));
        out.push_str(&format!(
            "brewfs_writeback_stage_failures_total {}\n",
            snapshot.writeback_stage_failures
        ));
        out.push_str(&format!(
            "brewfs_writeback_commit_before_stage_ops_total {}\n",
            snapshot.writeback_commit_before_stage_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_commit_wait_upload_ops_total {}\n",
            snapshot.writeback_commit_wait_upload_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_commit_wait_upload_us_total {}\n",
            snapshot.writeback_commit_wait_upload_us
        ));
        out.push_str(&format!(
            "brewfs_writeback_commit_wait_upload_size_ops_total {}\n",
            snapshot.writeback_commit_wait_upload_size_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_commit_wait_upload_size_us_total {}\n",
            snapshot.writeback_commit_wait_upload_size_us
        ));
        out.push_str(&format!(
            "brewfs_writeback_commit_wait_upload_max_unflushed_ops_total {}\n",
            snapshot.writeback_commit_wait_upload_max_unflushed_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_commit_wait_upload_max_unflushed_us_total {}\n",
            snapshot.writeback_commit_wait_upload_max_unflushed_us
        ));
        out.push_str(&format!(
            "brewfs_writeback_commit_wait_upload_explicit_flush_ops_total {}\n",
            snapshot.writeback_commit_wait_upload_explicit_flush_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_commit_wait_upload_explicit_flush_us_total {}\n",
            snapshot.writeback_commit_wait_upload_explicit_flush_us
        ));
        out.push_str(&format!(
            "brewfs_writeback_commit_wait_upload_auto_ops_total {}\n",
            snapshot.writeback_commit_wait_upload_auto_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_commit_wait_upload_auto_us_total {}\n",
            snapshot.writeback_commit_wait_upload_auto_us
        ));
        out.push_str(&format!(
            "brewfs_writeback_commit_wait_upload_commit_age_ops_total {}\n",
            snapshot.writeback_commit_wait_upload_commit_age_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_commit_wait_upload_commit_age_us_total {}\n",
            snapshot.writeback_commit_wait_upload_commit_age_us
        ));
        out.push_str(&format!(
            "brewfs_writeback_commit_wait_upload_unknown_reason_ops_total {}\n",
            snapshot.writeback_commit_wait_upload_unknown_reason_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_commit_wait_upload_unknown_reason_us_total {}\n",
            snapshot.writeback_commit_wait_upload_unknown_reason_us
        ));
        out.push_str(&format!(
            "brewfs_writeback_commit_wait_upload_normal_only_ops_total {}\n",
            snapshot.writeback_commit_wait_upload_normal_only_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_commit_wait_upload_normal_only_us_total {}\n",
            snapshot.writeback_commit_wait_upload_normal_only_us
        ));
        out.push_str(&format!(
            "brewfs_writeback_commit_wait_upload_cached_only_ops_total {}\n",
            snapshot.writeback_commit_wait_upload_cached_only_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_commit_wait_upload_cached_only_us_total {}\n",
            snapshot.writeback_commit_wait_upload_cached_only_us
        ));
        out.push_str(&format!(
            "brewfs_writeback_commit_wait_upload_mixed_origin_ops_total {}\n",
            snapshot.writeback_commit_wait_upload_mixed_origin_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_commit_wait_upload_mixed_origin_us_total {}\n",
            snapshot.writeback_commit_wait_upload_mixed_origin_us
        ));
        out.push_str(&format!(
            "brewfs_writeback_commit_wait_upload_unknown_origin_ops_total {}\n",
            snapshot.writeback_commit_wait_upload_unknown_origin_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_commit_wait_upload_unknown_origin_us_total {}\n",
            snapshot.writeback_commit_wait_upload_unknown_origin_us
        ));
        out.push_str(&format!(
            "brewfs_writeback_commit_wait_retry_ops_total {}\n",
            snapshot.writeback_commit_wait_retry_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_commit_wait_retry_us_total {}\n",
            snapshot.writeback_commit_wait_retry_us
        ));
        out.push_str(&format!(
            "brewfs_writeback_flush_wait_ops_total {}\n",
            snapshot.writeback_flush_wait_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_flush_wait_us_total {}\n",
            snapshot.writeback_flush_wait_us
        ));
        out.push_str(&format!(
            "brewfs_writeback_flush_wait_slices_total {}\n",
            snapshot.writeback_flush_wait_slices
        ));
        out.push_str(&format!(
            "brewfs_writeback_flush_fragmentation_ops_total {}\n",
            snapshot.writeback_flush_fragmentation_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_flush_fragmentation_slices_total {}\n",
            snapshot.writeback_flush_fragmentation_slices
        ));
        out.push_str(&format!(
            "brewfs_writeback_flush_fragmentation_bytes_total {}\n",
            snapshot.writeback_flush_fragmentation_bytes
        ));
        out.push_str(&format!(
            "brewfs_writeback_flush_fragmentation_cached_sub_block_slices_total {}\n",
            snapshot.writeback_flush_fragmentation_cached_sub_block_slices
        ));
        out.push_str(&format!(
            "brewfs_writeback_flush_fragmentation_cached_sub_block_bytes_total {}\n",
            snapshot.writeback_flush_fragmentation_cached_sub_block_bytes
        ));
        out.push_str(&format!(
            "brewfs_writeback_flush_fragmentation_full_block_slices_total {}\n",
            snapshot.writeback_flush_fragmentation_full_block_slices
        ));
        out.push_str(&format!(
            "brewfs_writeback_flush_fragmentation_full_block_bytes_total {}\n",
            snapshot.writeback_flush_fragmentation_full_block_bytes
        ));
        out.push_str(&format!(
            "brewfs_writeback_slice_create_ops_total {}\n",
            snapshot.writeback_slice_create_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_slice_reuse_ops_total {}\n",
            snapshot.writeback_slice_reuse_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_slice_reject_older_unique_ops_total {}\n",
            snapshot.writeback_slice_reject_older_unique_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_slice_reject_dispatched_prefix_ops_total {}\n",
            snapshot.writeback_slice_reject_dispatched_prefix_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_freeze_size_ops_total {}\n",
            snapshot.writeback_freeze_size_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_freeze_size_bytes_total {}\n",
            snapshot.writeback_freeze_size_bytes
        ));
        out.push_str(&format!(
            "brewfs_writeback_freeze_max_unflushed_ops_total {}\n",
            snapshot.writeback_freeze_max_unflushed_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_freeze_max_unflushed_bytes_total {}\n",
            snapshot.writeback_freeze_max_unflushed_bytes
        ));
        out.push_str(&format!(
            "brewfs_writeback_freeze_explicit_flush_ops_total {}\n",
            snapshot.writeback_freeze_explicit_flush_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_freeze_explicit_flush_bytes_total {}\n",
            snapshot.writeback_freeze_explicit_flush_bytes
        ));
        out.push_str(&format!(
            "brewfs_writeback_freeze_auto_ops_total {}\n",
            snapshot.writeback_freeze_auto_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_freeze_auto_bytes_total {}\n",
            snapshot.writeback_freeze_auto_bytes
        ));
        out.push_str(&format!(
            "brewfs_writeback_freeze_commit_age_ops_total {}\n",
            snapshot.writeback_freeze_commit_age_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_freeze_commit_age_bytes_total {}\n",
            snapshot.writeback_freeze_commit_age_bytes
        ));
        out.push_str(&format!(
            "brewfs_writeback_upload_batch_ops_total {}\n",
            snapshot.writeback_upload_batch_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_upload_batch_bytes_total {}\n",
            snapshot.writeback_upload_batch_bytes
        ));
        out.push_str(&format!(
            "brewfs_writeback_upload_batch_blocks_total {}\n",
            snapshot.writeback_upload_batch_blocks
        ));
        out.push_str(&format!(
            "brewfs_writeback_upload_batch_single_block_ops_total {}\n",
            snapshot.writeback_upload_batch_single_block_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_upload_batch_multi_block_ops_total {}\n",
            snapshot.writeback_upload_batch_multi_block_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_upload_partial_tail_ops_total {}\n",
            snapshot.writeback_upload_partial_tail_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_upload_partial_tail_size_ops_total {}\n",
            snapshot.writeback_upload_partial_tail_size_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_upload_partial_tail_max_unflushed_ops_total {}\n",
            snapshot.writeback_upload_partial_tail_max_unflushed_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_upload_partial_tail_explicit_flush_ops_total {}\n",
            snapshot.writeback_upload_partial_tail_explicit_flush_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_upload_partial_tail_auto_ops_total {}\n",
            snapshot.writeback_upload_partial_tail_auto_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_upload_partial_tail_normal_only_ops_total {}\n",
            snapshot.writeback_upload_partial_tail_normal_only_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_upload_partial_tail_cached_only_ops_total {}\n",
            snapshot.writeback_upload_partial_tail_cached_only_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_upload_partial_tail_mixed_origin_ops_total {}\n",
            snapshot.writeback_upload_partial_tail_mixed_origin_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_upload_partial_tail_unknown_origin_ops_total {}\n",
            snapshot.writeback_upload_partial_tail_unknown_origin_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_upload_partial_tail_auto_age_ops_total {}\n",
            snapshot.writeback_upload_partial_tail_auto_age_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_upload_partial_tail_auto_idle_ops_total {}\n",
            snapshot.writeback_upload_partial_tail_auto_idle_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_upload_partial_tail_auto_pressure_ops_total {}\n",
            snapshot.writeback_upload_partial_tail_auto_pressure_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_upload_partial_tail_auto_too_many_ops_total {}\n",
            snapshot.writeback_upload_partial_tail_auto_too_many_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_upload_partial_tail_auto_buffer_high_ops_total {}\n",
            snapshot.writeback_upload_partial_tail_auto_buffer_high_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_upload_partial_tail_auto_flush_duration_ops_total {}\n",
            snapshot.writeback_upload_partial_tail_auto_flush_duration_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_upload_partial_tail_auto_unknown_ops_total {}\n",
            snapshot.writeback_upload_partial_tail_auto_unknown_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_upload_partial_tail_auto_normal_only_ops_total {}\n",
            snapshot.writeback_upload_partial_tail_auto_normal_only_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_upload_partial_tail_auto_cached_only_ops_total {}\n",
            snapshot.writeback_upload_partial_tail_auto_cached_only_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_upload_partial_tail_auto_mixed_origin_ops_total {}\n",
            snapshot.writeback_upload_partial_tail_auto_mixed_origin_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_upload_partial_tail_auto_unknown_origin_ops_total {}\n",
            snapshot.writeback_upload_partial_tail_auto_unknown_origin_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_upload_partial_tail_commit_age_ops_total {}\n",
            snapshot.writeback_upload_partial_tail_commit_age_ops
        ));
        out.push_str(&format!(
            "brewfs_reader_buffer_bytes {}\n",
            snapshot.buf_read_bytes
        ));
        out.push_str(&format!(
            "brewfs_cache_hits_total {}\n",
            self.cache_hits.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_cache_misses_total {}\n",
            self.cache_misses.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_cache_requests_total {}\n",
            snapshot.cache_requests()
        ));
        out.push_str(&format!(
            "brewfs_cache_hit_ratio {:.6}\n",
            snapshot.cache_hit_ratio()
        ));
        out.push_str(&format!(
            "brewfs_read_block_cache_hits_total {}\n",
            snapshot.read_block_cache_hits
        ));
        out.push_str(&format!(
            "brewfs_read_page_cache_hits_total {}\n",
            snapshot.read_page_cache_hits
        ));
        out.push_str(&format!(
            "brewfs_read_page_cache_misses_total {}\n",
            snapshot.read_page_cache_misses
        ));
        out.push_str(&format!(
            "brewfs_read_range_gets_total {}\n",
            snapshot.read_range_gets
        ));
        out.push_str(&format!(
            "brewfs_read_full_gets_total {}\n",
            snapshot.read_full_gets
        ));
        out.push_str(&format!(
            "brewfs_read_piggyback_full_total {}\n",
            snapshot.read_piggyback_full
        ));
        out.push_str(&format!(
            "brewfs_read_background_prefetch_total {}\n",
            snapshot.read_background_prefetches
        ));
        out.push_str(&format!(
            "brewfs_read_background_prefetch_dropped_total {}\n",
            snapshot.read_background_prefetch_dropped
        ));
        out.push_str(&format!(
            "brewfs_meta_stat_cache_hit_total {}\n",
            snapshot.meta_stat_cache_hit
        ));
        out.push_str(&format!(
            "brewfs_meta_stat_cache_miss_total {}\n",
            snapshot.meta_stat_cache_miss
        ));
        out.push_str(&format!(
            "brewfs_meta_stat_fresh_store_hit_total {}\n",
            snapshot.meta_stat_fresh_store_hit
        ));
        out.push_str(&format!(
            "brewfs_meta_lookup_cache_hit_total {}\n",
            snapshot.meta_lookup_cache_hit
        ));
        out.push_str(&format!(
            "brewfs_meta_lookup_cache_miss_total {}\n",
            snapshot.meta_lookup_cache_miss
        ));
        out.push_str(&format!(
            "brewfs_meta_get_slices_cache_hit_total {}\n",
            snapshot.meta_get_slices_cache_hit
        ));
        out.push_str(&format!(
            "brewfs_meta_get_slices_cache_miss_total {}\n",
            snapshot.meta_get_slices_cache_miss
        ));
        out.push_str(&format!(
            "brewfs_meta_open_fresh_stat_total {}\n",
            snapshot.meta_open_fresh_stat
        ));
        out.push_str(&format!(
            "brewfs_meta_open_file_cache_hit_total {}\n",
            snapshot.meta_open_file_cache_hit
        ));
        out.push_str(&format!(
            "brewfs_meta_open_file_cache_miss_total {}\n",
            snapshot.meta_open_file_cache_miss
        ));
        out.push_str(&format!(
            "brewfs_meta_lookup_attr_fused_hit_total {}\n",
            snapshot.meta_lookup_attr_fused_hit
        ));
        out.push_str(&format!(
            "brewfs_meta_lookup_attr_fused_miss_total {}\n",
            snapshot.meta_lookup_attr_fused_miss
        ));
        out.push_str(&format!(
            "brewfs_meta_lookup_attr_fused_error_total {}\n",
            snapshot.meta_lookup_attr_fused_error
        ));

        let request_observation = self.request_observation.snapshot();
        out.push_str(&format!(
            "# brewfs_request_trace_instance {}\n",
            request_observation.instance_id
        ));
        out.push_str(&format!(
            "brewfs_request_observation_enabled {}\n",
            u8::from(request_observation.enabled)
        ));
        out.push_str(&format!(
            "brewfs_request_observation_sample_rate {}\n",
            request_observation.sample_rate
        ));
        out.push_str(&format!(
            "brewfs_request_observation_received_total {}\n",
            request_observation.received
        ));
        out.push_str(&format!(
            "brewfs_request_observation_sampled_total {}\n",
            request_observation.sampled
        ));
        out.push_str(&format!(
            "brewfs_request_observation_closed_total {}\n",
            request_observation.closed
        ));
        out.push_str(&format!(
            "brewfs_request_observation_cancelled_total {}\n",
            request_observation.cancelled
        ));
        out.push_str(&format!(
            "brewfs_request_observation_errors_total {}\n",
            request_observation.errors
        ));
        out.push_str(&format!(
            "brewfs_request_observation_dropped_total {}\n",
            request_observation.dropped
        ));
        out.push_str(&format!(
            "brewfs_request_observation_active {}\n",
            request_observation.active
        ));
        out.push_str(&format!(
            "brewfs_request_observation_closure_gap_ns_total {}\n",
            request_observation.closure_gap_ns_total
        ));
        out.push_str(&format!(
            "brewfs_request_observation_closure_gap_ns_max {}\n",
            request_observation.closure_gap_ns_max
        ));
        out.push_str(&format!(
            "brewfs_request_fuse_queue_depth {}\n",
            request_observation.fuse_queue_depth
        ));
        out.push_str(&format!(
            "brewfs_request_fuse_queue_oldest_age_ns {}\n",
            request_observation.fuse_queue_oldest_age_ns
        ));
        for (index, segment) in RequestSegment::ALL.into_iter().enumerate() {
            let values = request_observation.segments[index];
            let metric = format!("brewfs_request_{}_ns", segment.name());
            out.push_str(&format!("{metric}_count {}\n", values.count));
            out.push_str(&format!("{metric}_sum {}\n", values.total_ns));
            let mut cumulative = 0;
            for (bucket, count) in values.buckets.into_iter().enumerate() {
                cumulative += count;
                let upper = if bucket == REQUEST_HISTOGRAM_BUCKETS - 1 {
                    "+Inf".to_owned()
                } else {
                    (1u64 << bucket).to_string()
                };
                out.push_str(&format!("{metric}_bucket{{le=\"{upper}\"}} {cumulative}\n"));
            }
            let queue = request_observation.queues[index];
            out.push_str(&format!(
                "brewfs_request_{}_queue_depth {}\n",
                segment.name(),
                queue.depth
            ));
            out.push_str(&format!(
                "brewfs_request_{}_queue_oldest_age_ns {}\n",
                segment.name(),
                queue.oldest_age_ns
            ));
        }
        for (index, operation) in RequestOperation::ALL.into_iter().enumerate() {
            let values = request_observation.operations[index];
            let metric = format!("brewfs_request_operation_{}_ns", operation.name());
            out.push_str(&format!("{metric}_count {}\n", values.count));
            out.push_str(&format!("{metric}_sum {}\n", values.total_ns));
            let mut cumulative = 0;
            for (bucket, count) in values.buckets.into_iter().enumerate() {
                cumulative += count;
                let upper = if bucket == REQUEST_HISTOGRAM_BUCKETS - 1 {
                    "+Inf".to_owned()
                } else {
                    (1u64 << bucket).to_string()
                };
                out.push_str(&format!("{metric}_bucket{{le=\"{upper}\"}} {cumulative}\n"));
            }
        }
        for record in request_observation.trace_records.iter() {
            // Trace records are a bounded diagnostic stream, not Prometheus
            // label dimensions. Include exact intervals for offline validation.
            out.push_str(&format!(
                "# brewfs_request_trace_v2 {}\n",
                serde_json::to_string(&record.export()).expect("serializable request record")
            ));
        }
        out.push_str(&format!(
            "brewfs_request_trace_records_dropped_total {}\n",
            request_observation.trace_records_dropped
        ));

        if let Some(extension) = self.extension.get() {
            extension.render_into(&mut out);
        }
        if let Some(observation) = observation {
            observation.deliver(0);
        }
        out.finish()
    }

    pub fn record_duration(ops_counter: &AtomicU64, lat_counter: &AtomicU64, duration: Duration) {
        let elapsed_us = duration.as_micros() as u64;
        ops_counter.fetch_add(1, ORD);
        lat_counter.fetch_add(elapsed_us, ORD);
    }
}

impl asyncfuse::raw::RequestObserver for FsStats {
    fn with_request_context(&self, unique: u64, poll: &mut dyn FnMut()) {
        self.request_observation.with_request_context(unique, poll);
    }
    fn request_received(&self, unique: u64, opcode: u32) {
        self.request_observation.request_received(unique, opcode);
    }

    fn request_received_on_inode(&self, unique: u64, opcode: u32, inode: u64) {
        // An OPEN of .stats renders its snapshot before its own reply. Do not
        // turn every collector read into an apparently unclosed workload
        // sample. LOOKUP on the parent remains a real metadata request.
        if inode != crate::fuse::STATS_INODE {
            self.request_observation.request_received(unique, opcode);
            if let Ok(inode) = i64::try_from(inode) {
                self.request_observation
                    .set_context(unique, Some(inode), None, None, None);
            }
        }
    }

    fn request_worker_started(&self, unique: u64) {
        self.request_observation.worker_started(unique);
    }

    fn request_handler_started(&self, unique: u64) {
        self.request_observation.handler_started(unique);
    }

    fn request_handler_completed(&self, unique: u64) {
        self.request_observation.handler_completed(unique);
    }

    fn request_reply_submitted(&self, unique: u64) {
        self.request_observation.reply_submitted(unique);
    }

    fn request_reply_submitted_result(&self, unique: u64, error: i32) {
        self.request_observation.finish(
            unique,
            if error == 0 { "reply" } else { "reply_error" },
            false,
            error != 0,
        );
    }

    fn request_completed_without_reply(&self, unique: u64) {
        self.request_observation.completed_without_reply(unique);
    }

    fn request_cancelled(&self, unique: u64) {
        self.request_observation.terminal(unique, true, false);
    }

    fn request_failed(&self, unique: u64) {
        self.request_observation.terminal(unique, false, true);
    }
}

impl Default for FsStats {
    fn default() -> Self {
        Self::new()
    }
}

/// RAII guard for timing an operation and recording latency + count.
/// Records stats on drop, so it can be used as `let _timer = OpTimer::new(...)`.
pub struct OpTimer<'a> {
    start: Instant,
    ops_counter: &'a AtomicU64,
    lat_counter: &'a AtomicU64,
}

impl<'a> OpTimer<'a> {
    pub fn new(ops_counter: &'a AtomicU64, lat_counter: &'a AtomicU64) -> Self {
        Self {
            start: Instant::now(),
            ops_counter,
            lat_counter,
        }
    }

    /// Finish timing and record the operation (consumes self).
    pub fn finish(self) {
        // Drop impl handles the actual recording.
    }
}

impl<'a> Drop for OpTimer<'a> {
    fn drop(&mut self) {
        FsStats::record_duration(self.ops_counter, self.lat_counter, self.start.elapsed());
    }
}

/// Optional timer for diagnostic hot-path stats. Disabled timers avoid
/// `Instant::now()` so production hot paths only pay a cheap branch.
pub struct MaybeOpTimer<'a> {
    start: Option<Instant>,
    ops_counter: &'a AtomicU64,
    lat_counter: &'a AtomicU64,
}

impl<'a> MaybeOpTimer<'a> {
    pub fn new(enabled: bool, ops_counter: &'a AtomicU64, lat_counter: &'a AtomicU64) -> Self {
        Self {
            start: enabled.then(Instant::now),
            ops_counter,
            lat_counter,
        }
    }
}

impl<'a> Drop for MaybeOpTimer<'a> {
    fn drop(&mut self) {
        if let Some(start) = self.start {
            FsStats::record_duration(self.ops_counter, self.lat_counter, start.elapsed());
        }
    }
}

/// Convenience macro for timing an async operation and recording stats.
///
/// Usage:
/// ```ignore
/// let result = timed_op!(stats.fuse_read_ops, stats.fuse_read_lat_us, {
///     handle.read(offset, len).await
/// });
/// ```
#[macro_export]
macro_rules! timed_op {
    ($ops:expr, $lat:expr, $body:expr) => {{
        let __start = std::time::Instant::now();
        let __result = $body;
        let __elapsed_us = __start.elapsed().as_micros() as u64;
        $ops.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        $lat.fetch_add(__elapsed_us, std::sync::atomic::Ordering::Relaxed);
        __result
    }};
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_trace_exports_ten_named_nanosecond_fields() {
        let stats = FsStats::new();
        stats.request_observation.set_enabled(true);
        let segments = std::array::from_fn(|index| (index as u64 + 1) * 123);
        stats.request_observation.push_trace(RequestTraceRecord {
            unique: 42,
            segments,
            background_ns: [999; REQUEST_SEGMENT_COUNT],
            ..Default::default()
        });
        let output = stats.render();
        let json = output
            .lines()
            .find_map(|line| line.strip_prefix("# brewfs_request_trace_v2 "))
            .expect("request trace must be exported");
        let record: serde_json::Value = serde_json::from_str(json).unwrap();
        for (index, name) in [
            "queue_wait_ns",
            "file_lock_wait_ns",
            "chunk_lock_wait_ns",
            "dirty_merge_ns",
            "flush_wait_ns",
            "metadata_rpc_wait_ns",
            "metadata_commit_ns",
            "object_put_queue_wait_ns",
            "object_put_ns",
            "reply_ns",
        ]
        .into_iter()
        .enumerate()
        {
            assert_eq!(record[name].as_u64(), Some(segments[index]), "{name}");
        }
        assert_eq!(record["segments"], serde_json::json!(segments));
        assert_eq!(record["background_ns"][8], 999);
    }

    #[test]
    fn request_observation_is_disabled_by_default_configuration() {
        let observation = RequestObservation::new(false, 1);
        observation.request_received(1, 15);
        observation.worker_started(1);
        observation.reply_submitted(1);
        let snapshot = observation.snapshot();
        assert!(!snapshot.enabled);
        assert_eq!(snapshot.received, 0);
        assert_eq!(snapshot.active, 0);
    }

    #[test]
    fn unsampled_requests_do_not_count_as_dropped() {
        let observation = RequestObservation::new(true, 2);
        observation.request_received(1, 15); // sampled
        observation.request_received(2, 15); // deliberately unsampled
        observation.worker_started(1);
        observation.reply_submitted(1);
        observation.worker_started(2);
        observation.reply_submitted(2);
        let snapshot = observation.snapshot();
        assert_eq!(snapshot.received, 2);
        assert_eq!(snapshot.sampled, 1);
        assert_eq!(snapshot.closed, 1);
        assert_eq!(snapshot.dropped, 0);
        assert_eq!(snapshot.active, 0);
    }

    #[test]
    fn terminal_callbacks_close_sampled_request_once() {
        let observation = RequestObservation::new(true, 1);
        observation.request_received(7, 16);
        observation.worker_started(7);
        observation.handler_started(7);
        observation.handler_completed(7);
        observation.terminal(7, true, false);
        observation.terminal(7, false, true);
        observation.reply_submitted(7);
        let snapshot = observation.snapshot();
        assert_eq!(snapshot.closed, 1);
        assert_eq!(snapshot.cancelled, 1);
        assert_eq!(snapshot.errors, 0);
        assert_eq!(snapshot.active, 0);
    }

    #[test]
    fn handler_completion_is_idempotent_before_reply() {
        let observation = RequestObservation::new(true, 1);
        observation.request_received(19, 16);
        observation.worker_started(19);
        observation.handler_started(19);
        observation.handler_completed(19);
        let completed = observation.active.lock().unwrap()[&19].handler_done_at;
        observation.handler_completed(19);
        assert_eq!(
            observation.active.lock().unwrap()[&19].handler_done_at,
            completed
        );
        assert_eq!(
            observation.snapshot().segments[RequestSegment::Handler as usize].count,
            1
        );
    }

    #[test]
    fn late_background_segment_updates_closed_trace_without_reopening_request() {
        let observation = Arc::new(RequestObservation::new(true, 1));
        observation.request_received(9, 16);
        observation.worker_started(9);
        observation.handler_started(9);
        observation.handler_completed(9);
        observation.reply_submitted(9);
        assert!(observation.update_context(9, Some(12), Some(34), Some(56), Some(78)));
        let timer = observation
            .background_segment_timer(9, RequestSegment::ObjectPut)
            .expect("enabled observer should create timer");
        drop(timer);

        let snapshot = observation.snapshot();
        assert_eq!(snapshot.active, 0);
        assert_eq!(snapshot.trace_records.len(), 1);
        let trace = &snapshot.trace_records[0];
        assert_eq!(trace.file_id, Some(12));
        assert_eq!(trace.chunk_id, Some(34));
        assert_eq!(trace.slice_id, Some(56));
        assert_eq!(trace.lease_id, Some(78));
        assert!(trace.background_ns[RequestSegment::ObjectPut as usize] > 0);
        assert_eq!(trace.segments[RequestSegment::ObjectPut as usize], 0);
        assert_eq!(trace.exclusive_ns[RequestSegment::ObjectPut as usize], 0);
        assert_eq!(
            snapshot.segments[RequestSegment::ObjectPut as usize].count,
            1
        );
    }

    #[test]
    fn unscoped_segment_updates_aggregate_without_active_request() {
        let observation = Arc::new(RequestObservation::new(true, 1));
        let timer = observation
            .unscoped_segment_timer(RequestSegment::ObjectPut)
            .expect("enabled observer should create timer");
        drop(timer);
        let snapshot = observation.snapshot();
        assert_eq!(
            snapshot.segments[RequestSegment::ObjectPut as usize].count,
            1
        );
    }

    #[test]
    fn queue_wait_snapshot_is_bounded_and_retires_on_timer_drop() {
        let observation = Arc::new(RequestObservation::new(true, 1));
        observation.request_received(11, 16);
        let timer = observation
            .segment_timer(11, RequestSegment::ObjectPutQueueWait)
            .expect("enabled observer should create timer");
        let waiting = observation.snapshot();
        let queue = waiting.queues[RequestSegment::ObjectPutQueueWait as usize];
        assert_eq!(queue.depth, 1);
        assert!(queue.oldest_age_ns < 1_000_000_000);
        drop(timer);
        let drained = observation.snapshot();
        assert_eq!(
            drained.queues[RequestSegment::ObjectPutQueueWait as usize].depth,
            0
        );
    }

    #[test]
    fn partition_nested_waits_preserves_wall_time_and_residual() {
        let intervals = [
            [0, 10, RequestSegment::FuseQueueWait as u64],
            [20, 80, RequestSegment::FlushWait as u64],
            [30, 60, RequestSegment::MetadataRpcWait as u64],
            [90, 100, RequestSegment::Reply as u64],
        ];
        let (parts, ambiguous) = partition_request(100, &intervals);
        assert_eq!(parts.iter().sum::<u64>(), 100);
        assert_eq!(parts[RequestSegment::FlushWait as usize], 30);
        assert_eq!(parts[RequestSegment::MetadataRpcWait as usize], 30);
        assert_eq!(parts[RequestSegment::Handler as usize], 20);
        assert_eq!(ambiguous, 0);
    }

    #[test]
    fn crossing_concurrent_spans_are_ambiguous_not_added() {
        let intervals = [
            [10, 70, RequestSegment::MetadataRpcWait as u64],
            [40, 90, RequestSegment::ObjectPut as u64],
        ];
        let (parts, ambiguous) = partition_request(100, &intervals);
        assert_eq!(parts.iter().sum::<u64>(), 100);
        assert_eq!(ambiguous, 30);
    }

    #[test]
    fn poll_wall_time_fills_residual_without_stealing_a_wait() {
        let intervals = [
            [0, 20, RequestSegment::HandlerPoll as u64],
            [10, 70, RequestSegment::MetadataRpcWait as u64],
            [60, 80, RequestSegment::HandlerPoll as u64],
        ];
        let (parts, ambiguous) = partition_request(100, &intervals);
        assert_eq!(parts[RequestSegment::MetadataRpcWait as usize], 60);
        assert_eq!(parts[RequestSegment::HandlerPoll as usize], 20);
        assert_eq!(parts[RequestSegment::Handler as usize], 20);
        assert_eq!(parts.iter().sum::<u64>(), 100);
        assert_eq!(ambiguous, 0);
    }

    #[test]
    fn cancellation_clips_open_timer_and_late_drop_cannot_change_wall_time() {
        let observation = Arc::new(RequestObservation::new(true, 1));
        observation.request_received(42, 16);
        observation.worker_started(42);
        observation.handler_started(42);
        let timer = observation
            .segment_timer(42, RequestSegment::FileLockWait)
            .unwrap();
        observation.terminal(42, true, false);
        let before = observation.snapshot().trace_records.remove(0);
        drop(timer);
        let after = observation.snapshot().trace_records.remove(0);
        assert_eq!(before, after);
        assert_eq!(after.exclusive_ns.iter().sum::<u64>(), after.total_ns);
        assert!(
            after
                .intervals
                .iter()
                .all(|interval| interval[1] <= after.total_ns)
        );
    }

    #[test]
    fn unsampled_timer_has_no_state_and_interval_overflow_is_explicit() {
        let observation = Arc::new(RequestObservation::new(true, 1));
        assert!(
            observation
                .segment_timer(1, RequestSegment::FileLockWait)
                .is_none()
        );
        observation.request_received(2, 16);
        for _ in 0..=REQUEST_INTERVAL_LIMIT {
            drop(observation.segment_timer(2, RequestSegment::FileLockWait));
        }
        observation.reply_submitted(2);
        let trace = observation.snapshot().trace_records.remove(0);
        assert!(trace.intervals_truncated);
        assert!(trace.intervals.len() <= REQUEST_INTERVAL_LIMIT + 2);
    }

    #[test]
    fn poll_context_is_restored_on_nested_return_and_unwind() {
        let observation = Arc::new(RequestObservation::new(true, 1));
        let other = Arc::new(RequestObservation::new(true, 1));
        observation.request_received(1, 16);
        observation.request_received(2, 16);
        observation.with_request_context(1, &mut || {
            let timer = observation
                .contextual_segment_timer(RequestSegment::MetadataRpcWait)
                .unwrap();
            assert_eq!(timer.unique, Some(1));
            let unrelated = other
                .contextual_segment_timer(RequestSegment::MetadataRpcWait)
                .unwrap();
            assert_eq!(unrelated.unique, None);
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                observation.with_request_context(2, &mut || panic!("controlled poll unwind"));
            }));
            assert_eq!(REQUEST_POLL_CONTEXT.get().unwrap().1, 1);
        });
        assert_eq!(REQUEST_POLL_CONTEXT.get(), None);
    }

    #[derive(Debug)]
    struct TestStatsExtension;

    impl FsStatsExtension for TestStatsExtension {
        fn render_into(&self, output: &mut dyn std::fmt::Write) {
            let _ = output.write_str("brewfs_test_backend_gets_total 7\n");
        }
    }

    #[test]
    fn mount_extension_is_rendered_once_without_replacing_common_stats() {
        let stats = FsStats::new();
        assert!(stats.set_extension(std::sync::Arc::new(TestStatsExtension)));
        assert!(!stats.set_extension(std::sync::Arc::new(TestStatsExtension)));
        let rendered = stats.render();
        assert_eq!(
            rendered.matches("brewfs_test_backend_gets_total 7").count(),
            1
        );
        assert!(rendered.contains("brewfs_fuse_read_ops_total 0"));
        assert!(
            !FsStats::new()
                .render()
                .contains("brewfs_test_backend_gets_total")
        );
    }

    #[test]
    fn render_contains_all_metrics() {
        let stats = FsStats::new();
        stats.fuse_read_ops.store(42, ORD);
        stats.fuse_read_bytes.store(1024 * 1024, ORD);
        stats.fuse_read_lat_us.store(840, ORD);
        stats.record_fuse_attr_valid(Duration::from_secs(1));
        stats.record_fuse_entry_valid(Duration::from_millis(250));
        stats.s3_put_ops.store(10, ORD);
        stats.s3_put_lat_us.store(250, ORD);
        stats.s3_put_prepare_lat_us.store(500, ORD);
        stats.s3_put_cache_lat_us.store(100, ORD);
        stats.sync_cache_counters(8, 2);
        stats.sync_buffer_bytes(4096, 8192);
        stats.sync_writeback_dirty_breakdown(1024, 2, 2048, 3, 512, 4);
        stats.sync_writeback_live_origin_metrics(128, 1, 256, 2, 512, 3, 1024, 4);
        stats.add_writeback_backpressure_soft_sleep(Duration::from_micros(12));
        stats.add_writeback_backpressure_hard_wait(Duration::from_micros(34));
        stats.sync_writeback_buffer_backpressure_metrics(70, 700, 80, 800, 90, 900);
        stats.sync_writeback_phase_metrics(1, 2, 3, 4, 5, 6, 7);
        stats.sync_writeback_metadata_inflight(8);
        stats.sync_writeback_commit_wait_metrics(40, 41, 42, 43);
        stats.sync_writeback_flush_wait_metrics(64, 65, 66);
        stats.sync_writeback_flush_fragmentation_metrics(67, 68, 69, 70, 71, 72, 73);
        stats.sync_writeback_commit_wait_breakdown_metrics(
            44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63,
        );
        stats.sync_writeback_slice_selection_metrics(8, 9, 10, 11);
        stats.sync_writeback_freeze_metrics(12, 1024, 13, 2048, 14, 4096, 15, 8192, 16, 16384);
        stats.sync_writeback_upload_batch_metrics(
            17, 32768, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
        );
        stats.sync_writeback_upload_batch_shape_metrics(64, 65);
        stats.sync_writeback_upload_origin_metrics(32, 33, 34, 35, 36, 37, 38, 39);

        let output = stats.render();
        assert!(output.contains("brewfs_fuse_read_ops_total 42"));
        assert!(output.contains("brewfs_fuse_read_bytes_total 1048576"));
        assert!(output.contains("brewfs_fuse_read_avg_lat_us 20.000000"));
        assert!(output.contains("brewfs_s3_put_ops_total 10"));
        assert!(output.contains("brewfs_s3_put_avg_lat_us 25.000000"));
        assert!(output.contains("brewfs_s3_put_prepare_avg_lat_us 50.000000"));
        assert!(output.contains("brewfs_s3_put_cache_avg_lat_us 10.000000"));
        assert!(output.contains("brewfs_uptime_seconds"));
        assert!(output.contains("brewfs_cache_hits_total 8"));
        assert!(output.contains("brewfs_cache_misses_total 2"));
        assert!(output.contains("brewfs_cache_requests_total 10"));
        assert!(output.contains("brewfs_cache_hit_ratio 0.800000"));
        assert!(output.contains("brewfs_read_block_cache_hits_total 0"));
        assert!(output.contains("brewfs_read_page_cache_hits_total 0"));
        assert!(output.contains("brewfs_read_page_cache_misses_total 0"));
        assert!(output.contains("brewfs_read_range_gets_total 0"));
        assert!(output.contains("brewfs_read_full_gets_total 0"));
        assert!(output.contains("brewfs_read_piggyback_full_total 0"));
        assert!(output.contains("brewfs_read_background_prefetch_total 0"));
        assert!(output.contains("brewfs_read_background_prefetch_dropped_total 0"));
        assert!(output.contains("brewfs_meta_stat_cache_hit_total 0"));
        assert!(output.contains("brewfs_meta_stat_cache_miss_total 0"));
        assert!(output.contains("brewfs_meta_stat_fresh_store_hit_total 0"));
        assert!(output.contains("brewfs_meta_lookup_cache_hit_total 0"));
        assert!(output.contains("brewfs_meta_lookup_cache_miss_total 0"));
        assert!(output.contains("brewfs_meta_get_slices_cache_hit_total 0"));
        assert!(output.contains("brewfs_meta_get_slices_cache_miss_total 0"));
        assert!(output.contains("brewfs_meta_open_fresh_stat_total 0"));
        assert!(output.contains("brewfs_meta_open_file_cache_hit_total 0"));
        assert!(output.contains("brewfs_meta_open_file_cache_miss_total 0"));
        assert!(output.contains("brewfs_meta_lookup_attr_fused_hit_total 0"));
        assert!(output.contains("brewfs_meta_lookup_attr_fused_miss_total 0"));
        assert!(output.contains("brewfs_meta_lookup_attr_fused_error_total 0"));
        assert!(output.contains("brewfs_writeback_dirty_bytes 4096"));
        assert!(output.contains("brewfs_writeback_live_dirty_bytes 1024"));
        assert!(output.contains("brewfs_writeback_live_slices 2"));
        assert!(output.contains("brewfs_writeback_live_normal_only_bytes 128"));
        assert!(output.contains("brewfs_writeback_live_normal_only_slices 1"));
        assert!(output.contains("brewfs_writeback_live_cached_only_bytes 256"));
        assert!(output.contains("brewfs_writeback_live_cached_only_slices 2"));
        assert!(output.contains("brewfs_writeback_live_mixed_origin_bytes 512"));
        assert!(output.contains("brewfs_writeback_live_mixed_origin_slices 3"));
        assert!(output.contains("brewfs_writeback_live_unknown_origin_bytes 1024"));
        assert!(output.contains("brewfs_writeback_live_unknown_origin_slices 4"));
        assert!(output.contains("brewfs_writeback_recent_pending_upload_bytes 2048"));
        assert!(output.contains("brewfs_writeback_recent_pending_upload_slices 3"));
        assert!(output.contains("brewfs_writeback_recent_uploaded_bytes 512"));
        assert!(output.contains("brewfs_writeback_recent_uploaded_slices 4"));
        assert!(output.contains("brewfs_writeback_backpressure_soft_sleep_ops 1"));
        assert!(output.contains("brewfs_writeback_backpressure_soft_sleep_us 12"));
        assert!(output.contains("brewfs_writeback_backpressure_hard_wait_ops 1"));
        assert!(output.contains("brewfs_writeback_backpressure_hard_wait_us 34"));
        assert!(output.contains("brewfs_writeback_buffer_soft_sleep_ops 70"));
        assert!(output.contains("brewfs_writeback_buffer_soft_sleep_us 700"));
        assert!(output.contains("brewfs_writeback_buffer_moderate_sleep_ops 80"));
        assert!(output.contains("brewfs_writeback_buffer_moderate_sleep_us 800"));
        assert!(output.contains("brewfs_writeback_buffer_hard_sleep_ops 90"));
        assert!(output.contains("brewfs_writeback_buffer_hard_sleep_us 900"));
        assert!(output.contains("brewfs_writeback_stage_inflight_bytes 1"));
        assert!(output.contains("brewfs_writeback_remote_upload_inflight_bytes 2"));
        assert!(output.contains("brewfs_writeback_metadata_inflight 8"));
        assert!(output.contains("brewfs_writeback_stage_ops_total 3"));
        assert!(output.contains("brewfs_writeback_stage_bytes_total 4"));
        assert!(output.contains("brewfs_writeback_stage_lat_us_total 5"));
        assert!(output.contains("brewfs_writeback_stage_failures_total 6"));
        assert!(output.contains("brewfs_writeback_commit_before_stage_ops_total 7"));
        assert!(output.contains("brewfs_writeback_commit_wait_upload_ops_total 40"));
        assert!(output.contains("brewfs_writeback_commit_wait_upload_us_total 41"));
        assert!(output.contains("brewfs_writeback_commit_wait_upload_size_ops_total 44"));
        assert!(output.contains("brewfs_writeback_commit_wait_upload_size_us_total 45"));
        assert!(output.contains("brewfs_writeback_commit_wait_upload_max_unflushed_ops_total 46"));
        assert!(output.contains("brewfs_writeback_commit_wait_upload_max_unflushed_us_total 47"));
        assert!(output.contains("brewfs_writeback_commit_wait_upload_explicit_flush_ops_total 48"));
        assert!(output.contains("brewfs_writeback_commit_wait_upload_explicit_flush_us_total 49"));
        assert!(output.contains("brewfs_writeback_commit_wait_upload_auto_ops_total 50"));
        assert!(output.contains("brewfs_writeback_commit_wait_upload_auto_us_total 51"));
        assert!(output.contains("brewfs_writeback_commit_wait_upload_commit_age_ops_total 52"));
        assert!(output.contains("brewfs_writeback_commit_wait_upload_commit_age_us_total 53"));
        assert!(output.contains("brewfs_writeback_commit_wait_upload_unknown_reason_ops_total 54"));
        assert!(output.contains("brewfs_writeback_commit_wait_upload_unknown_reason_us_total 55"));
        assert!(output.contains("brewfs_writeback_commit_wait_upload_normal_only_ops_total 56"));
        assert!(output.contains("brewfs_writeback_commit_wait_upload_normal_only_us_total 57"));
        assert!(output.contains("brewfs_writeback_commit_wait_upload_cached_only_ops_total 58"));
        assert!(output.contains("brewfs_writeback_commit_wait_upload_cached_only_us_total 59"));
        assert!(output.contains("brewfs_writeback_commit_wait_upload_mixed_origin_ops_total 60"));
        assert!(output.contains("brewfs_writeback_commit_wait_upload_mixed_origin_us_total 61"));
        assert!(output.contains("brewfs_writeback_commit_wait_upload_unknown_origin_ops_total 62"));
        assert!(output.contains("brewfs_writeback_commit_wait_upload_unknown_origin_us_total 63"));
        assert!(output.contains("brewfs_writeback_commit_wait_retry_ops_total 42"));
        assert!(output.contains("brewfs_writeback_commit_wait_retry_us_total 43"));
        assert!(output.contains("brewfs_writeback_flush_wait_ops_total 64"));
        assert!(output.contains("brewfs_writeback_flush_wait_us_total 65"));
        assert!(output.contains("brewfs_writeback_flush_wait_slices_total 66"));
        assert!(output.contains("brewfs_writeback_flush_fragmentation_ops_total 67"));
        assert!(output.contains("brewfs_writeback_flush_fragmentation_slices_total 68"));
        assert!(output.contains("brewfs_writeback_flush_fragmentation_bytes_total 69"));
        assert!(
            output
                .contains("brewfs_writeback_flush_fragmentation_cached_sub_block_slices_total 70")
        );
        assert!(
            output.contains("brewfs_writeback_flush_fragmentation_cached_sub_block_bytes_total 71")
        );
        assert!(output.contains("brewfs_writeback_flush_fragmentation_full_block_slices_total 72"));
        assert!(output.contains("brewfs_writeback_flush_fragmentation_full_block_bytes_total 73"));
        assert!(output.contains("brewfs_writeback_slice_create_ops_total 8"));
        assert!(output.contains("brewfs_writeback_slice_reuse_ops_total 9"));
        assert!(output.contains("brewfs_writeback_slice_reject_older_unique_ops_total 10"));
        assert!(output.contains("brewfs_writeback_slice_reject_dispatched_prefix_ops_total 11"));
        assert!(output.contains("brewfs_writeback_freeze_size_ops_total 12"));
        assert!(output.contains("brewfs_writeback_freeze_size_bytes_total 1024"));
        assert!(output.contains("brewfs_writeback_freeze_max_unflushed_ops_total 13"));
        assert!(output.contains("brewfs_writeback_freeze_max_unflushed_bytes_total 2048"));
        assert!(output.contains("brewfs_writeback_freeze_explicit_flush_ops_total 14"));
        assert!(output.contains("brewfs_writeback_freeze_explicit_flush_bytes_total 4096"));
        assert!(output.contains("brewfs_writeback_freeze_auto_ops_total 15"));
        assert!(output.contains("brewfs_writeback_freeze_auto_bytes_total 8192"));
        assert!(output.contains("brewfs_writeback_freeze_commit_age_ops_total 16"));
        assert!(output.contains("brewfs_writeback_freeze_commit_age_bytes_total 16384"));
        assert!(output.contains("brewfs_writeback_upload_batch_ops_total 17"));
        assert!(output.contains("brewfs_writeback_upload_batch_bytes_total 32768"));
        assert!(output.contains("brewfs_writeback_upload_batch_blocks_total 18"));
        assert!(output.contains("brewfs_writeback_upload_batch_single_block_ops_total 64"));
        assert!(output.contains("brewfs_writeback_upload_batch_multi_block_ops_total 65"));
        assert!(output.contains("brewfs_writeback_upload_partial_tail_ops_total 19"));
        assert!(output.contains("brewfs_writeback_upload_partial_tail_size_ops_total 20"));
        assert!(output.contains("brewfs_writeback_upload_partial_tail_max_unflushed_ops_total 21"));
        assert!(
            output.contains("brewfs_writeback_upload_partial_tail_explicit_flush_ops_total 22")
        );
        assert!(output.contains("brewfs_writeback_upload_partial_tail_auto_ops_total 23"));
        assert!(output.contains("brewfs_writeback_upload_partial_tail_normal_only_ops_total 32"));
        assert!(output.contains("brewfs_writeback_upload_partial_tail_cached_only_ops_total 33"));
        assert!(output.contains("brewfs_writeback_upload_partial_tail_mixed_origin_ops_total 34"));
        assert!(
            output.contains("brewfs_writeback_upload_partial_tail_unknown_origin_ops_total 35")
        );
        assert!(output.contains("brewfs_writeback_upload_partial_tail_auto_age_ops_total 24"));
        assert!(output.contains("brewfs_writeback_upload_partial_tail_auto_idle_ops_total 25"));
        assert!(output.contains("brewfs_writeback_upload_partial_tail_auto_pressure_ops_total 26"));
        assert!(output.contains("brewfs_writeback_upload_partial_tail_auto_too_many_ops_total 27"));
        assert!(
            output.contains("brewfs_writeback_upload_partial_tail_auto_buffer_high_ops_total 28")
        );
        assert!(
            output
                .contains("brewfs_writeback_upload_partial_tail_auto_flush_duration_ops_total 29")
        );
        assert!(output.contains("brewfs_writeback_upload_partial_tail_auto_unknown_ops_total 30"));
        assert!(
            output.contains("brewfs_writeback_upload_partial_tail_auto_normal_only_ops_total 36")
        );
        assert!(
            output.contains("brewfs_writeback_upload_partial_tail_auto_cached_only_ops_total 37")
        );
        assert!(
            output.contains("brewfs_writeback_upload_partial_tail_auto_mixed_origin_ops_total 38")
        );
        assert!(
            output
                .contains("brewfs_writeback_upload_partial_tail_auto_unknown_origin_ops_total 39")
        );
        assert!(output.contains("brewfs_writeback_upload_partial_tail_commit_age_ops_total 31"));
        assert!(output.contains("brewfs_reader_buffer_bytes 8192"));
        assert!(output.contains("brewfs_vfs_create_total_ops_total 0"));
        assert!(output.contains("brewfs_vfs_unlink_lookup_lat_us_total 0"));
        assert!(output.contains("brewfs_vfs_unlink_recent_ops_total 0"));
        assert!(output.contains("brewfs_vfs_setattr_recent_remove_lat_us_total 0"));
        assert!(output.contains("brewfs_vfs_read_dirty_probe_ops_total 0"));
        assert!(output.contains("brewfs_vfs_read_handle_ops_total 0"));
        assert!(output.contains("brewfs_vfs_read_overlay_ops_total 0"));
        assert!(output.contains("brewfs_fuse_attr_valid_ns 1000000000"));
        assert!(output.contains("brewfs_fuse_entry_valid_ns 250000000"));
    }

    #[test]
    fn reply_validity_observation_starts_at_zero_and_keeps_latest_value() {
        let stats = FsStats::new();
        let initial = stats.snapshot();
        assert_eq!(initial.fuse_attr_valid_ns, 0);
        assert_eq!(initial.fuse_entry_valid_ns, 0);

        stats.record_fuse_attr_valid(Duration::from_nanos(17));
        stats.record_fuse_entry_valid(Duration::from_nanos(23));
        let observed = stats.snapshot();
        assert_eq!(observed.fuse_attr_valid_ns, 17);
        assert_eq!(observed.fuse_entry_valid_ns, 23);
    }

    #[test]
    fn snapshot_exposes_derived_values_without_divide_by_zero() {
        let stats = FsStats::new();
        let empty = stats.snapshot();
        assert_eq!(empty.cache_requests(), 0);
        assert_eq!(empty.cache_hit_ratio(), 0.0);
        assert_eq!(empty.avg_fuse_read_lat_us(), 0.0);

        stats.fuse_write_ops.store(4, ORD);
        stats.fuse_write_lat_us.store(1000, ORD);
        stats.sync_cache_counters(3, 1);
        stats.sync_writeback_dirty_breakdown(11, 2, 22, 3, 33, 4);
        stats.sync_writeback_live_origin_metrics(55, 6, 66, 7, 77, 8, 88, 9);
        stats.add_writeback_backpressure_soft_sleep(Duration::from_micros(44));
        stats.add_writeback_backpressure_hard_wait(Duration::from_micros(55));
        stats.sync_writeback_backpressure_metrics(66, 77, 88, 99);
        stats.sync_writeback_buffer_backpressure_metrics(166, 177, 188, 199, 200, 201);
        stats.sync_writeback_phase_metrics(111, 222, 444, 555, 666, 777, 888);
        stats.sync_writeback_metadata_inflight(333);
        stats.sync_writeback_commit_wait_metrics(778, 779, 780, 781);
        stats.sync_writeback_flush_wait_metrics(804, 805, 806);
        stats.sync_writeback_flush_fragmentation_metrics(807, 808, 809, 810, 811, 812, 813);
        stats.sync_writeback_commit_wait_breakdown_metrics(
            782, 783, 784, 785, 786, 787, 788, 789, 790, 791, 792, 793, 794, 795, 796, 797, 798,
            799, 800, 801,
        );
        stats.sync_writeback_slice_selection_metrics(888, 999, 1000, 1001);
        stats.sync_writeback_freeze_metrics(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        stats.sync_writeback_upload_batch_metrics(
            11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26,
        );
        stats.sync_writeback_upload_batch_shape_metrics(802, 803);
        stats.sync_writeback_upload_origin_metrics(27, 28, 29, 30, 31, 32, 33, 34);
        stats.sync_object_store_metrics(2, 8192, 50, 1, 4096, 25, 75, 125, 3);

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.cache_requests(), 4);
        assert_eq!(snapshot.cache_hit_ratio(), 0.75);
        assert_eq!(snapshot.avg_fuse_write_lat_us(), 250.0);
        assert_eq!(snapshot.s3_get_ops, 2);
        assert_eq!(snapshot.s3_get_bytes, 8192);
        assert_eq!(snapshot.avg_s3_get_lat_us(), 25.0);
        assert_eq!(snapshot.s3_put_ops, 1);
        assert_eq!(snapshot.s3_put_bytes, 4096);
        assert_eq!(snapshot.avg_s3_put_lat_us(), 25.0);
        assert_eq!(snapshot.avg_s3_put_prepare_lat_us(), 75.0);
        assert_eq!(snapshot.avg_s3_put_cache_lat_us(), 125.0);
        assert_eq!(snapshot.s3_del_ops, 3);
        assert_eq!(snapshot.writeback_live_dirty_bytes, 11);
        assert_eq!(snapshot.writeback_live_slices, 2);
        assert_eq!(snapshot.writeback_live_normal_only_bytes, 55);
        assert_eq!(snapshot.writeback_live_normal_only_slices, 6);
        assert_eq!(snapshot.writeback_live_cached_only_bytes, 66);
        assert_eq!(snapshot.writeback_live_cached_only_slices, 7);
        assert_eq!(snapshot.writeback_live_mixed_origin_bytes, 77);
        assert_eq!(snapshot.writeback_live_mixed_origin_slices, 8);
        assert_eq!(snapshot.writeback_live_unknown_origin_bytes, 88);
        assert_eq!(snapshot.writeback_live_unknown_origin_slices, 9);
        assert_eq!(snapshot.writeback_recent_pending_upload_bytes, 22);
        assert_eq!(snapshot.writeback_recent_pending_upload_slices, 3);
        assert_eq!(snapshot.writeback_recent_uploaded_bytes, 33);
        assert_eq!(snapshot.writeback_recent_uploaded_slices, 4);
        assert_eq!(snapshot.writeback_backpressure_soft_sleep_ops, 66);
        assert_eq!(snapshot.writeback_backpressure_soft_sleep_us, 77);
        assert_eq!(snapshot.writeback_backpressure_hard_wait_ops, 88);
        assert_eq!(snapshot.writeback_backpressure_hard_wait_us, 99);
        assert_eq!(snapshot.writeback_buffer_soft_sleep_ops, 166);
        assert_eq!(snapshot.writeback_buffer_soft_sleep_us, 177);
        assert_eq!(snapshot.writeback_buffer_moderate_sleep_ops, 188);
        assert_eq!(snapshot.writeback_buffer_moderate_sleep_us, 199);
        assert_eq!(snapshot.writeback_buffer_hard_sleep_ops, 200);
        assert_eq!(snapshot.writeback_buffer_hard_sleep_us, 201);
        assert_eq!(snapshot.writeback_stage_inflight_bytes, 111);
        assert_eq!(snapshot.writeback_remote_upload_inflight_bytes, 222);
        assert_eq!(snapshot.writeback_metadata_inflight, 333);
        assert_eq!(snapshot.writeback_stage_ops, 444);
        assert_eq!(snapshot.writeback_stage_bytes, 555);
        assert_eq!(snapshot.writeback_stage_lat_us, 666);
        assert_eq!(snapshot.writeback_stage_failures, 777);
        assert_eq!(snapshot.writeback_commit_before_stage_ops, 888);
        assert_eq!(snapshot.writeback_commit_wait_upload_ops, 778);
        assert_eq!(snapshot.writeback_commit_wait_upload_us, 779);
        assert_eq!(snapshot.writeback_commit_wait_upload_size_ops, 782);
        assert_eq!(snapshot.writeback_commit_wait_upload_size_us, 783);
        assert_eq!(snapshot.writeback_commit_wait_upload_max_unflushed_ops, 784);
        assert_eq!(snapshot.writeback_commit_wait_upload_max_unflushed_us, 785);
        assert_eq!(
            snapshot.writeback_commit_wait_upload_explicit_flush_ops,
            786
        );
        assert_eq!(snapshot.writeback_commit_wait_upload_explicit_flush_us, 787);
        assert_eq!(snapshot.writeback_commit_wait_upload_auto_ops, 788);
        assert_eq!(snapshot.writeback_commit_wait_upload_auto_us, 789);
        assert_eq!(snapshot.writeback_commit_wait_upload_commit_age_ops, 790);
        assert_eq!(snapshot.writeback_commit_wait_upload_commit_age_us, 791);
        assert_eq!(
            snapshot.writeback_commit_wait_upload_unknown_reason_ops,
            792
        );
        assert_eq!(snapshot.writeback_commit_wait_upload_unknown_reason_us, 793);
        assert_eq!(snapshot.writeback_commit_wait_upload_normal_only_ops, 794);
        assert_eq!(snapshot.writeback_commit_wait_upload_normal_only_us, 795);
        assert_eq!(snapshot.writeback_commit_wait_upload_cached_only_ops, 796);
        assert_eq!(snapshot.writeback_commit_wait_upload_cached_only_us, 797);
        assert_eq!(snapshot.writeback_commit_wait_upload_mixed_origin_ops, 798);
        assert_eq!(snapshot.writeback_commit_wait_upload_mixed_origin_us, 799);
        assert_eq!(
            snapshot.writeback_commit_wait_upload_unknown_origin_ops,
            800
        );
        assert_eq!(snapshot.writeback_commit_wait_upload_unknown_origin_us, 801);
        assert_eq!(snapshot.writeback_commit_wait_retry_ops, 780);
        assert_eq!(snapshot.writeback_commit_wait_retry_us, 781);
        assert_eq!(snapshot.writeback_flush_wait_ops, 804);
        assert_eq!(snapshot.writeback_flush_wait_us, 805);
        assert_eq!(snapshot.writeback_flush_wait_slices, 806);
        assert_eq!(snapshot.writeback_flush_fragmentation_ops, 807);
        assert_eq!(snapshot.writeback_flush_fragmentation_slices, 808);
        assert_eq!(snapshot.writeback_flush_fragmentation_bytes, 809);
        assert_eq!(
            snapshot.writeback_flush_fragmentation_cached_sub_block_slices,
            810
        );
        assert_eq!(
            snapshot.writeback_flush_fragmentation_cached_sub_block_bytes,
            811
        );
        assert_eq!(
            snapshot.writeback_flush_fragmentation_full_block_slices,
            812
        );
        assert_eq!(snapshot.writeback_flush_fragmentation_full_block_bytes, 813);
        assert_eq!(snapshot.writeback_slice_create_ops, 888);
        assert_eq!(snapshot.writeback_slice_reuse_ops, 999);
        assert_eq!(snapshot.writeback_slice_reject_older_unique_ops, 1000);
        assert_eq!(snapshot.writeback_slice_reject_dispatched_prefix_ops, 1001);
        assert_eq!(snapshot.writeback_freeze_size_ops, 1);
        assert_eq!(snapshot.writeback_freeze_size_bytes, 2);
        assert_eq!(snapshot.writeback_freeze_max_unflushed_ops, 3);
        assert_eq!(snapshot.writeback_freeze_max_unflushed_bytes, 4);
        assert_eq!(snapshot.writeback_freeze_explicit_flush_ops, 5);
        assert_eq!(snapshot.writeback_freeze_explicit_flush_bytes, 6);
        assert_eq!(snapshot.writeback_freeze_auto_ops, 7);
        assert_eq!(snapshot.writeback_freeze_auto_bytes, 8);
        assert_eq!(snapshot.writeback_freeze_commit_age_ops, 9);
        assert_eq!(snapshot.writeback_freeze_commit_age_bytes, 10);
        assert_eq!(snapshot.writeback_upload_batch_ops, 11);
        assert_eq!(snapshot.writeback_upload_batch_bytes, 12);
        assert_eq!(snapshot.writeback_upload_batch_blocks, 13);
        assert_eq!(snapshot.writeback_upload_batch_single_block_ops, 802);
        assert_eq!(snapshot.writeback_upload_batch_multi_block_ops, 803);
        assert_eq!(snapshot.writeback_upload_partial_tail_ops, 14);
        assert_eq!(snapshot.writeback_upload_partial_tail_size_ops, 15);
        assert_eq!(snapshot.writeback_upload_partial_tail_max_unflushed_ops, 16);
        assert_eq!(
            snapshot.writeback_upload_partial_tail_explicit_flush_ops,
            17
        );
        assert_eq!(snapshot.writeback_upload_partial_tail_auto_ops, 18);
        assert_eq!(snapshot.writeback_upload_partial_tail_normal_only_ops, 27);
        assert_eq!(snapshot.writeback_upload_partial_tail_cached_only_ops, 28);
        assert_eq!(snapshot.writeback_upload_partial_tail_mixed_origin_ops, 29);
        assert_eq!(
            snapshot.writeback_upload_partial_tail_unknown_origin_ops,
            30
        );
        assert_eq!(snapshot.writeback_upload_partial_tail_auto_age_ops, 19);
        assert_eq!(snapshot.writeback_upload_partial_tail_auto_idle_ops, 20);
        assert_eq!(snapshot.writeback_upload_partial_tail_auto_pressure_ops, 21);
        assert_eq!(snapshot.writeback_upload_partial_tail_auto_too_many_ops, 22);
        assert_eq!(
            snapshot.writeback_upload_partial_tail_auto_buffer_high_ops,
            23
        );
        assert_eq!(
            snapshot.writeback_upload_partial_tail_auto_flush_duration_ops,
            24
        );
        assert_eq!(snapshot.writeback_upload_partial_tail_auto_unknown_ops, 25);
        assert_eq!(
            snapshot.writeback_upload_partial_tail_auto_normal_only_ops,
            31
        );
        assert_eq!(
            snapshot.writeback_upload_partial_tail_auto_cached_only_ops,
            32
        );
        assert_eq!(
            snapshot.writeback_upload_partial_tail_auto_mixed_origin_ops,
            33
        );
        assert_eq!(
            snapshot.writeback_upload_partial_tail_auto_unknown_origin_ops,
            34
        );
        assert_eq!(snapshot.writeback_upload_partial_tail_commit_age_ops, 26);
    }

    #[test]
    fn op_timer_records_latency() {
        let ops = AtomicU64::new(0);
        let lat = AtomicU64::new(0);

        let timer = OpTimer::new(&ops, &lat);
        std::thread::sleep(std::time::Duration::from_millis(1));
        timer.finish();

        assert_eq!(ops.load(ORD), 1);
        assert!(lat.load(ORD) >= 1000); // at least 1ms = 1000us
    }
}

#[cfg(test)]
mod bounded_snapshot_tests {
    use super::*;
    use std::sync::Arc;
    #[derive(Debug)]
    struct Guard(Arc<AtomicU64>);
    impl Drop for Guard {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    #[test]
    fn bounded_stats_does_not_deliver_truncation_and_reply_clones_keep_guards() {
        let stats = FsStats::new();
        assert!(stats.render_bounded(64).is_err());
        assert_eq!(
            stats
                .render_bounded(stats.render_allocation_limit())
                .unwrap(),
            stats.render()
        );
        let drops = Arc::new(AtomicU64::new(0));
        let owner = VirtualStatsSnapshot {
            text: "long stable snapshot".into(),
            _output: Some(Arc::new(Guard(drops.clone()))),
            _control: Some(Arc::new(Guard(drops.clone()))),
        };
        let handle = bytes::Bytes::from_owner(owner);
        let reply = handle.slice(0..4);
        let clone = reply.clone();
        drop(handle);
        drop(reply);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert_eq!(clone.as_ref(), b"long");
        drop(clone);
        assert_eq!(drops.load(Ordering::SeqCst), 2);
    }
}
