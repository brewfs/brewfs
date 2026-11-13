//! Typed, bounded read ledgers with independent body, validation, HTTP
//! attempt and caller-visible delivery lifetimes.

use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use anyhow::Result;
use bytes::Bytes;
use futures_util::{Stream, StreamExt};

use super::client::{ObjectBackend, ObjectByteStream, ObjectClient};
pub mod http;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum Engine {
    Packed,
    Native,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum Phase {
    Startup,
    Runtime,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ReadClass {
    ManifestProbe,
    Manifest,
    GroupIndex,
    InodeIndex,
    ContainerIndex,
    FrameIndex,
    ReverseIndex,
    ColdIndex,
    LargeIndex,
    SourceStatsIndex,
    GroupMetadata,
    FrameDirectory,
    ColdAttributes,
    PackedPayload,
    ExternalPayload,
    NativeIndex,
    NativeAttributes,
    NativePayload,
    LogicalRead,
    InlinePayload,
    StatsSnapshot,
    /// Whole immutable object readback during publication or upload recovery.
    /// A dedicated class records the mixed metadata and payload bytes.
    PublicationVerification,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum Origin {
    Demand,
    Prefetch,
    Warmup,
    StatsObserver,
}

/// These events have distinct meanings and cannot be summed into file/GET counts.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ReadEvent {
    CacheHit,
    CacheLookupMiss,
    CacheDisabled,
    FetchLeader,
    SharedResultAfterMiss,
}
impl ReadEvent {
    fn label(self) -> &'static str {
        match self {
            Self::CacheHit => "cache_hit",
            Self::CacheLookupMiss => "cache_lookup_miss",
            Self::CacheDisabled => "cache_disabled",
            Self::FetchLeader => "fetch_leader",
            Self::SharedResultAfterMiss => "shared_result_after_miss",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ReadWork {
    Authentication,
    Decode,
}
impl ReadWork {
    fn label(self) -> &'static str {
        match self {
            Self::Authentication => "authentication_wall",
            Self::Decode => "decode_wall",
        }
    }
}
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Latency {
    pub count: u64,
    pub sum_nanoseconds: u64,
    pub buckets: [u64; 32],
}
impl Latency {
    fn observe(&mut self, nanos: u64) -> bool {
        let bucket = if nanos == 0 {
            0
        } else {
            (64 - (nanos - 1).leading_zeros() as usize).min(31)
        };
        add(&mut self.count, 1)
            | add(&mut self.sum_nanoseconds, nanos)
            | add(&mut self.buckets[bucket], 1)
    }
    fn render(&self, output: &mut dyn std::fmt::Write, metric: &str, labels: &str) {
        let _ = writeln!(output, "{metric}_count{{{labels}}} {}", self.count);
        let _ = writeln!(output, "{metric}_sum{{{labels}}} {}", self.sum_nanoseconds);
        let mut cumulative = 0u64;
        for (index, value) in self.buckets.iter().enumerate() {
            cumulative = cumulative.saturating_add(*value);
            let upper = if index == 31 {
                "+Inf".into()
            } else {
                (1u64 << index).to_string()
            };
            let _ = writeln!(
                output,
                "{metric}_bucket{{{labels},le=\"{upper}\"}} {cumulative}"
            );
        }
    }
}
#[derive(Debug)]
pub struct WorkTimer {
    observer: Arc<ReadObserver>,
    context: ReadContext,
    work: ReadWork,
    start: std::time::Instant,
}
impl Drop for WorkTimer {
    fn drop(&mut self) {
        let nanos = u64::try_from(self.start.elapsed().as_nanos()).unwrap_or(u64::MAX);
        let mut state = self
            .observer
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let overflow = state
            .work
            .entry((self.context, self.work))
            .or_default()
            .observe(nanos);
        state.overflowed |= overflow;
    }
}

/// Construct at a typed reference/descriptor call site, never from a key.
/// Common transport code cannot authenticate a format-specific reference.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ReadContext {
    pub engine: Engine,
    pub phase: Phase,
    pub class: ReadClass,
    pub origin: Origin,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum Ledger {
    BackendBody,
    ValidatedFetch,
    LogicalOperation,
    /// Start at connector future first poll, including SDK retries.
    HttpAttempt,
    SemanticValidation,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Outcome {
    Success,
    Failed,
    Cancelled,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
#[repr(usize)]
pub enum FailureClass {
    Backend,
    ShortBody,
    ExcessBody,
    Authentication,
    Decode,
    Schema,
    Generation,
    Admission,
    HttpStatus,
}

impl FailureClass {
    const ALL: [Self; 9] = [
        Self::Backend,
        Self::ShortBody,
        Self::ExcessBody,
        Self::Authentication,
        Self::Decode,
        Self::Schema,
        Self::Generation,
        Self::Admission,
        Self::HttpStatus,
    ];
}

/// Fixed storage avoids one heap tree for each terminal row's reasons.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FailureReasons([u64; 9]);
impl FailureReasons {
    fn values(&self) -> impl Iterator<Item = &u64> {
        self.0.iter()
    }
    fn increment(&mut self, reason: FailureClass) -> bool {
        add(&mut self.0[reason as usize], 1)
    }
    fn iter(&self) -> impl Iterator<Item = (FailureClass, u64)> + '_ {
        FailureClass::ALL
            .into_iter()
            .zip(self.0)
            .filter(|(_, count)| *count != 0)
    }
}
impl std::ops::Index<&FailureClass> for FailureReasons {
    type Output = u64;
    fn index(&self, reason: &FailureClass) -> &u64 {
        &self.0[*reason as usize]
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ReadBoundaryError {
    #[error("typed range exceeds its declared allocation/end bound")]
    Admission,
    #[error("typed range body is short: expected {expected}, received {received}")]
    Short { expected: u64, received: u64 },
    #[error("typed range body contains more bytes than requested")]
    Excess,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Counters {
    pub started: u64,
    pub success: u64,
    pub failed: u64,
    pub cancelled: u64,
    pub inflight: u64,
    pub requested: u64,
    pub requested_unknown: u64,
    pub received: u64,
    pub received_success: u64,
    pub received_failed: u64,
    pub received_cancelled: u64,
    pub received_inflight: u64,
    pub logical_delivered: u64,
    pub failure_reasons: FailureReasons,
    pub terminal_latency: Latency,
}

impl Counters {
    pub fn conserved(&self) -> bool {
        let outcomes = self
            .success
            .checked_add(self.failed)
            .and_then(|value| value.checked_add(self.cancelled))
            .and_then(|value| value.checked_add(self.inflight));
        let bytes = self
            .received_success
            .checked_add(self.received_failed)
            .and_then(|value| value.checked_add(self.received_cancelled))
            .and_then(|value| value.checked_add(self.received_inflight));
        let failure_reasons = self
            .failure_reasons
            .values()
            .try_fold(0u64, |sum, value| sum.checked_add(*value));
        self.requested_unknown <= self.started
            && outcomes == Some(self.started)
            && bytes == Some(self.received)
            && failure_reasons == Some(self.failed)
            && self
                .terminal_latency
                .buckets
                .iter()
                .try_fold(0u64, |sum, value| sum.checked_add(*value))
                == Some(self.terminal_latency.count)
            && self
                .success
                .checked_add(self.failed)
                .and_then(|value| value.checked_add(self.cancelled))
                == Some(self.terminal_latency.count)
    }
}

#[derive(Clone, Debug, Default)]
pub struct Snapshot {
    pub rows: BTreeMap<(Ledger, ReadContext), Counters>,
    pub overflowed: bool,
    pub http_observed: bool,
    pub raw_observed: bool,
    pub raw: BTreeMap<ReadContext, RawSummary>,
    pub events: BTreeMap<(ReadContext, ReadEvent), u64>,
    pub work: BTreeMap<(ReadContext, ReadWork), Latency>,
}

#[derive(Debug, Default)]
pub struct ReadObserver {
    state: Mutex<Snapshot>,
    // Declaration order drops all statistic allocations before their owner.
    _memory_owner: Option<Box<dyn std::fmt::Debug + Send + Sync>>,
    owner_budget_identity: Option<usize>,
}

fn add(value: &mut u64, delta: u64) -> bool {
    match value.checked_add(delta) {
        Some(next) => {
            *value = next;
            false
        }
        None => {
            *value = u64::MAX;
            true
        }
    }
}

impl ReadObserver {
    /// Conservative owned bound for the complete finite enum product,
    /// including terminal, work, event and raw rows for every context.
    /// Includes BTree internal nodes, fixed reason arrays and render labels.
    /// Rendering holds the state lock and does not clone these trees.
    pub const MEMORY_BOUND_BYTES: u64 = 8 << 20;

    /// Complete finite enum product, including maximum-width u64 counters.
    /// Mount admission must allow this state before any runtime rows exist.
    pub const MAX_RENDER_BYTES: usize = Self::render_bound(
        Ledger::ALL.len() * ReadContext::MAX_CONTEXTS,
        ReadWork::ALL.len() * ReadContext::MAX_CONTEXTS,
        ReadEvent::ALL.len() * ReadContext::MAX_CONTEXTS,
        ReadContext::MAX_CONTEXTS,
    );

    const fn render_bound(rows: usize, work: usize, events: usize, raw: usize) -> usize {
        4096 + rows * (57 * 208) + work * (34 * 208) + events * 208 + raw * (7 * 208)
    }

    pub fn with_memory_owner(owner: impl std::fmt::Debug + Send + Sync + 'static) -> Self {
        Self {
            state: Mutex::new(Snapshot::default()),
            _memory_owner: Some(Box::new(owner)),
            owner_budget_identity: None,
        }
    }

    #[cfg(feature = "workspace")]
    pub(crate) fn with_mount_budget(
        budget: &Arc<crate::workspace::packed::snapshot::SnapshotMountBudget>,
    ) -> crate::workspace::packed::PackedResult<Self> {
        let owner = budget.admit(&[(
            crate::workspace::packed::snapshot::SnapshotBudgetPool::Roots,
            Self::MEMORY_BOUND_BYTES,
        )])?;
        Ok(Self {
            state: Mutex::new(Snapshot::default()),
            _memory_owner: Some(Box::new(owner)),
            owner_budget_identity: Some(Arc::as_ptr(budget) as usize),
        })
    }

    pub fn owned_by_budget(&self, budget_identity: usize) -> bool {
        self.owner_budget_identity == Some(budget_identity)
    }

    fn update(
        &self,
        ledger: Ledger,
        context: ReadContext,
        change: impl FnOnce(&mut Counters) -> bool,
    ) {
        // Recover diagnostic access after a poison; mutations below do not run
        // user callbacks. The validity flag prevents silently claiming exactness.
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poison) => {
                let mut state = poison.into_inner();
                state.overflowed = true;
                state
            }
        };
        let overflowed = change(state.rows.entry((ledger, context)).or_default());
        state.overflowed |= overflowed;
    }

    pub fn start(
        self: &Arc<Self>,
        ledger: Ledger,
        context: ReadContext,
        requested: u64,
    ) -> TerminalGuard {
        self.update(ledger, context, |row| {
            add(&mut row.started, 1)
                | add(&mut row.inflight, 1)
                | add(&mut row.requested, requested)
        });
        TerminalGuard {
            observer: Arc::clone(self),
            ledger,
            context,
            received: 0,
            terminal: false,
            start: std::time::Instant::now(),
            failure: None,
            delivery: (ledger == Ledger::LogicalOperation)
                .then(|| OperationDelivery::new(Arc::clone(self))),
        }
    }

    pub fn start_request(
        self: &Arc<Self>,
        ledger: Ledger,
        context: ReadContext,
        requested: Option<u64>,
    ) -> TerminalGuard {
        let guard = self.start(ledger, context, requested.unwrap_or(0));
        if requested.is_none() {
            self.update(ledger, context, |row| add(&mut row.requested_unknown, 1));
        }
        guard
    }

    /// Test-only copies are intentionally outside mount allocation accounting.
    #[cfg(test)]
    pub fn snapshot(&self) -> Snapshot {
        match self.state.lock() {
            Ok(state) => state.clone(),
            Err(poison) => {
                let mut snapshot = poison.into_inner().clone();
                snapshot.overflowed = true;
                snapshot
            }
        }
    }

    pub fn work(self: &Arc<Self>, context: ReadContext, work: ReadWork) -> WorkTimer {
        WorkTimer {
            observer: Arc::clone(self),
            context,
            work,
            start: std::time::Instant::now(),
        }
    }

    pub fn event(&self, context: ReadContext, event: ReadEvent) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let overflow = add(state.events.entry((context, event)).or_default(), 1);
        state.overflowed |= overflow;
    }

    pub fn enable_http(&self) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.http_observed = true;
    }

    pub fn enable_raw(&self) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.raw_observed = true;
    }

    fn record_raw(&self, context: ReadContext, raw: RawSummary) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.raw_observed = true;
        let overflow = state.raw.entry(context).or_default().accumulate(raw);
        state.overflowed |= overflow;
    }

    pub fn render_max_bytes(&self) -> usize {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        // All labels and metric names are fixed enums. Each terminal/work
        // histogram has <=34 rows. The longest fixed enum label/metric
        // line, with a 20-digit u64 and bucket label, is at most 208 bytes.
        // Terminal rows allow 57 lines including native unknown-length totals.
        Self::render_bound(
            state.rows.len(),
            state.work.len(),
            state.events.len(),
            state.raw.len(),
        )
    }

    pub fn render_into(&self, output: &mut dyn std::fmt::Write) {
        let snapshot = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let valid = !snapshot.overflowed
            && snapshot.rows.values().all(Counters::conserved)
            && snapshot.raw.values().all(RawSummary::conserved);
        let _ = writeln!(
            output,
            "brewfs_object_read_counters_valid {}",
            u8::from(valid)
        );
        // Unknown/unwired measurements are capabilities, not invented zeros.
        let _ = writeln!(
            output,
            "brewfs_object_read_http_attempts_observed {}",
            u8::from(snapshot.http_observed)
        );
        let _ = writeln!(
            output,
            "brewfs_object_read_http_body_bytes_observed {}",
            u8::from(snapshot.http_observed)
        );
        let _ = writeln!(
            output,
            "brewfs_object_read_raw_union_observed {}",
            u8::from(snapshot.raw_observed)
        );
        for ((context, work), latency) in &snapshot.work {
            let labels = format!(
                "engine=\"{}\",phase=\"{}\",kind=\"{}\",origin=\"{}\",work=\"{}\"",
                context.engine.label(),
                context.phase.label(),
                context.class.label(),
                context.origin.label(),
                work.label()
            );
            latency.render(output, "brewfs_object_read_work_nanoseconds", &labels);
        }
        for ((context, event), value) in &snapshot.events {
            let _ = writeln!(
                output,
                "brewfs_object_read_events_total{{engine=\"{}\",phase=\"{}\",kind=\"{}\",origin=\"{}\",event=\"{}\"}} {value}",
                context.engine.label(),
                context.phase.label(),
                context.class.label(),
                context.origin.label(),
                event.label()
            );
        }
        for (context, raw) in &snapshot.raw {
            let labels = format!(
                "engine=\"{}\",phase=\"{}\",kind=\"{}\",origin=\"{}\"",
                context.engine.label(),
                context.phase.label(),
                context.class.label(),
                context.origin.label()
            );
            for (name, value) in [
                ("decoded_raw_bytes_total", raw.decoded_raw),
                ("requested_raw_union_bytes_total", raw.requested_union),
                ("copied_raw_union_bytes_total", raw.copied_union),
                ("delivered_raw_union_bytes_total", raw.delivered_union),
                (
                    "requested_overfetch_raw_bytes_total",
                    raw.requested_overfetch,
                ),
                ("copied_overfetch_raw_bytes_total", raw.copied_overfetch),
                (
                    "undelivered_decoded_raw_bytes_total",
                    raw.undelivered_decoded_raw,
                ),
            ] {
                let _ = writeln!(output, "brewfs_object_read_{name}{{{labels}}} {value}");
            }
        }
        for ((ledger, context), row) in &snapshot.rows {
            let labels = format!(
                "layer=\"{}\",engine=\"{}\",phase=\"{}\",kind=\"{}\",origin=\"{}\"",
                ledger.label(),
                context.engine.label(),
                context.phase.label(),
                context.class.label(),
                context.origin.label()
            );
            row.terminal_latency.render(
                output,
                "brewfs_object_read_terminal_latency_nanoseconds",
                &labels,
            );
            for (name, value) in [
                ("started_total", row.started),
                ("successful_total", row.success),
                ("failed_total", row.failed),
                ("cancelled_total", row.cancelled),
                ("inflight", row.inflight),
            ] {
                let _ = writeln!(output, "brewfs_object_read_{name}{{{labels}}} {value}");
            }
            if *ledger == Ledger::LogicalOperation {
                let _ = writeln!(
                    output,
                    "brewfs_object_read_requested_logical_bytes_total{{{labels}}} {}",
                    row.requested
                );
                let _ = writeln!(
                    output,
                    "brewfs_object_read_logical_delivered_bytes_total{{{labels}}} {}",
                    row.logical_delivered
                );
            } else {
                for (name, value) in [
                    (
                        "requested_body_known_operations_total",
                        row.started - row.requested_unknown,
                    ),
                    (
                        "requested_body_unknown_operations_total",
                        row.requested_unknown,
                    ),
                    ("requested_body_known_bytes_total", row.requested),
                    ("requested_body_bytes_total", row.requested),
                    ("received_body_bytes_total", row.received),
                    ("received_successful_body_bytes_total", row.received_success),
                    ("received_failed_body_bytes_total", row.received_failed),
                    (
                        "received_cancelled_body_bytes_total",
                        row.received_cancelled,
                    ),
                    ("received_inflight_body_bytes", row.received_inflight),
                ] {
                    let _ = writeln!(output, "brewfs_object_read_{name}{{{labels}}} {value}");
                }
            }
            for (reason, value) in row.failure_reasons.iter() {
                let _ = writeln!(
                    output,
                    "brewfs_object_read_failure_reasons_total{{{labels},reason=\"{}\"}} {value}",
                    reason.label()
                );
            }
        }
    }
}

/// Own this from first future poll through the last body consumer. Dropping a
/// future before headers or dropping its returned body both terminate once.
#[derive(Debug)]
pub struct TerminalGuard {
    observer: Arc<ReadObserver>,
    ledger: Ledger,
    context: ReadContext,
    received: u64,
    terminal: bool,
    start: std::time::Instant,
    failure: Option<FailureClass>,
    delivery: Option<Arc<OperationDelivery>>,
}

impl TerminalGuard {
    pub fn delivery_token(&self) -> Option<Arc<OperationDelivery>> {
        self.delivery.clone()
    }

    /// Discard one prepared view without starting a second logical operation.
    /// Late owners of the old token still record their bytes as undelivered.
    pub(crate) fn restart_delivery_attempt(&mut self) {
        debug_assert!(!self.terminal);
        if let Some(delivery) = self.delivery.take() {
            delivery.finish(false);
            self.delivery = Some(OperationDelivery::new(Arc::clone(&self.observer)));
        }
    }

    pub fn receive(&mut self, bytes: u64) {
        debug_assert!(!self.terminal);
        let own_overflow = add(&mut self.received, bytes);
        self.observer.update(self.ledger, self.context, |row| {
            own_overflow | add(&mut row.received, bytes) | add(&mut row.received_inflight, bytes)
        });
    }

    fn finish(&mut self, outcome: Outcome, delivered: u64) {
        if self.terminal {
            return;
        }
        self.terminal = true;
        if let Some(delivery) = &self.delivery {
            delivery.finish(outcome == Outcome::Success);
        }
        let received = self.received;
        let nanos = u64::try_from(self.start.elapsed().as_nanos()).unwrap_or(u64::MAX);
        let failure = self.failure;
        self.observer.update(self.ledger, self.context, |row| {
            let invalid_inflight = row.inflight == 0 || row.received_inflight < received;
            row.inflight = row.inflight.saturating_sub(1);
            row.received_inflight = row.received_inflight.saturating_sub(received);
            let (count, bytes) = match outcome {
                Outcome::Success => (&mut row.success, &mut row.received_success),
                Outcome::Failed => (&mut row.failed, &mut row.received_failed),
                Outcome::Cancelled => (&mut row.cancelled, &mut row.received_cancelled),
            };
            let terminal_overflow =
                add(count, 1) | add(bytes, received) | add(&mut row.logical_delivered, delivered);
            let reason_overflow = match failure {
                Some(reason) if outcome == Outcome::Failed => row.failure_reasons.increment(reason),
                _ => false,
            };
            invalid_inflight
                | terminal_overflow
                | reason_overflow
                | row.terminal_latency.observe(nanos)
        });
    }

    pub fn succeed(mut self) {
        self.finish(Outcome::Success, 0);
    }

    pub fn fail(mut self, class: FailureClass) {
        self.failure = Some(class);
        self.finish(Outcome::Failed, 0);
    }

    /// Only the complete caller-visible operation may commit logical bytes.
    pub fn deliver(mut self, bytes: u64) {
        assert_eq!(self.ledger, Ledger::LogicalOperation);
        self.finish(Outcome::Success, bytes);
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        self.finish(Outcome::Cancelled, 0);
    }
}

pub struct ObservedBody {
    stream: Option<ObjectByteStream>,
    guard: Option<TerminalGuard>,
}

impl Stream for ObservedBody {
    type Item = Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.guard.is_none() {
            return Poll::Ready(None);
        }
        match self.stream.as_mut().unwrap().as_mut().poll_next(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(Ok(bytes))) => {
                self.guard.as_mut().unwrap().receive(bytes.len() as u64);
                Poll::Ready(Some(Ok(bytes)))
            }
            Poll::Ready(Some(Err(error))) => {
                self.guard.take().unwrap().fail(FailureClass::Backend);
                self.stream.take();
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(None) => {
                self.guard.take().unwrap().succeed();
                self.stream.take();
                Poll::Ready(None)
            }
        }
    }
}

/// This is an application-visible backend-body ledger. It does not claim to
/// count bytes consumed/discarded inside an SDK retry or physical network bytes.
pub async fn range_stream<B: ObjectBackend>(
    client: &ObjectClient<B>,
    observer: &Arc<ReadObserver>,
    context: ReadContext,
    key: &str,
    offset: u64,
    length: u64,
) -> Result<ObservedBody> {
    let guard = observer.start(Ledger::BackendBody, context, length);
    match client
        .backend_range_stream_observed(key, offset, length, context, Arc::clone(observer))
        .await
    {
        Ok(stream) => Ok(ObservedBody {
            stream: Some(stream),
            guard: Some(guard),
        }),
        Err(error) => {
            guard.fail(FailureClass::Backend);
            Err(error)
        }
    }
}

/// Exact-body and format authentication form a separate terminal ledger.
/// The decoder supplies a typed failure class; a backend EOF can succeed
/// while this ledger fails for truncation or a bad digest/schema.
pub struct VerifiedReadRequest<'a> {
    pub key: &'a str,
    pub offset: u64,
    pub length: u64,
    pub allocation_limit: u64,
}

pub async fn exact_verified<B, T, Verify>(
    client: &ObjectClient<B>,
    observer: &Arc<ReadObserver>,
    context: ReadContext,
    request: VerifiedReadRequest<'_>,
    verify: Verify,
) -> Result<T>
where
    B: ObjectBackend,
    Verify: FnOnce(Vec<u8>) -> std::result::Result<T, (FailureClass, anyhow::Error)>,
{
    let VerifiedReadRequest {
        key,
        offset,
        length,
        allocation_limit,
    } = request;
    let mut fetch = observer.start(Ledger::ValidatedFetch, context, length);
    if length > allocation_limit || offset.checked_add(length).is_none() {
        fetch.fail(FailureClass::Admission);
        return Err(ReadBoundaryError::Admission.into());
    }
    let expected = match usize::try_from(length) {
        Ok(value) => value,
        Err(_) => {
            fetch.fail(FailureClass::Admission);
            return Err(ReadBoundaryError::Admission.into());
        }
    };
    let mut stream = match range_stream(client, observer, context, key, offset, length).await {
        Ok(value) => value,
        Err(error) => {
            fetch.fail(FailureClass::Backend);
            return Err(error);
        }
    };
    let mut bytes = Vec::with_capacity(expected);
    while let Some(chunk) = stream.next().await {
        let chunk = match chunk {
            Ok(value) => value,
            Err(error) => {
                fetch.fail(FailureClass::Backend);
                return Err(error);
            }
        };
        fetch.receive(chunk.len() as u64);
        if chunk.len() > expected - bytes.len() {
            fetch.fail(FailureClass::ExcessBody);
            return Err(ReadBoundaryError::Excess.into());
        }
        bytes.extend_from_slice(&chunk);
    }
    if bytes.len() != expected {
        fetch.fail(FailureClass::ShortBody);
        return Err(ReadBoundaryError::Short {
            expected: length,
            received: bytes.len() as u64,
        }
        .into());
    }
    match verify(bytes) {
        Ok(value) => {
            fetch.succeed();
            Ok(value)
        }
        Err((class, error)) => {
            fetch.fail(class);
            Err(error)
        }
    }
}

/// Whole-body stream ledger, including unknown native-block request lengths.
pub async fn whole_verified<B, T, Verify>(
    client: &ObjectClient<B>,
    scope: Option<(Arc<ReadObserver>, ReadContext)>,
    key: &str,
    expected: Option<u64>,
    allocation_limit: u64,
    verify: Verify,
) -> Result<Option<T>>
where
    B: ObjectBackend,
    Verify: FnOnce(Vec<u8>) -> std::result::Result<T, (FailureClass, anyhow::Error)>,
{
    let fetch = scope.as_ref().map(|(observer, context)| {
        observer.start_request(Ledger::ValidatedFetch, *context, expected)
    });
    if expected.is_some_and(|n| n > allocation_limit) || usize::try_from(allocation_limit).is_err()
    {
        if let Some(fetch) = fetch {
            fetch.fail(FailureClass::Admission);
        }
        return Err(ReadBoundaryError::Admission.into());
    }
    let mut body_guard = scope
        .as_ref()
        .map(|(observer, context)| observer.start_request(Ledger::BackendBody, *context, expected));
    let body = client
        .backend_object_stream(key, expected, scope.as_ref().map(|(_, c)| *c))
        .await;
    let body = match body {
        Ok(Some(body)) => body,
        Ok(None) => {
            if let Some(body) = body_guard.take() {
                body.succeed();
            }
            if let Some(fetch) = fetch {
                fetch.fail(FailureClass::Schema);
            }
            return Ok(None);
        }
        Err(error) => {
            if let Some(body) = body_guard.take() {
                body.fail(FailureClass::Backend);
            }
            if let Some(fetch) = fetch {
                fetch.fail(FailureClass::Backend);
            }
            return Err(error);
        }
    };
    let body: ObjectByteStream = match body_guard {
        Some(guard) => Box::pin(ObservedBody {
            stream: Some(body),
            guard: Some(guard),
        }),
        None => body,
    };
    collect_verified(body, fetch, expected, allocation_limit, verify)
        .await
        .map(Some)
}

async fn collect_verified<T, Verify>(
    mut body: ObjectByteStream,
    mut fetch: Option<TerminalGuard>,
    expected: Option<u64>,
    limit: u64,
    verify: Verify,
) -> Result<T>
where
    Verify: FnOnce(Vec<u8>) -> std::result::Result<T, (FailureClass, anyhow::Error)>,
{
    let mut bytes = Vec::new();
    while let Some(chunk) = body.next().await {
        let chunk = match chunk {
            Ok(chunk) => chunk,
            Err(error) => {
                if let Some(fetch) = fetch.take() {
                    fetch.fail(FailureClass::Backend);
                }
                return Err(error);
            }
        };
        if let Some(fetch) = &mut fetch {
            fetch.receive(chunk.len() as u64);
        }
        let next = bytes.len().checked_add(chunk.len());
        if next.is_none_or(|n| n as u64 > expected.unwrap_or(limit) || n as u64 > limit) {
            if let Some(fetch) = fetch.take() {
                fetch.fail(FailureClass::ExcessBody);
            }
            return Err(ReadBoundaryError::Excess.into());
        }
        if bytes.try_reserve_exact(chunk.len()).is_err() {
            if let Some(fetch) = fetch.take() {
                fetch.fail(FailureClass::Admission);
            }
            return Err(ReadBoundaryError::Admission.into());
        }
        bytes.extend_from_slice(&chunk);
    }
    if let Some(expected) = expected.filter(|n| *n != bytes.len() as u64) {
        if let Some(fetch) = fetch.take() {
            fetch.fail(FailureClass::ShortBody);
        }
        return Err(ReadBoundaryError::Short {
            expected,
            received: bytes.len() as u64,
        }
        .into());
    }
    match verify(bytes) {
        Ok(value) => {
            if let Some(fetch) = fetch {
                fetch.succeed();
            }
            Ok(value)
        }
        Err((reason, error)) => {
            if let Some(fetch) = fetch {
                fetch.fail(reason);
            }
            Err(error)
        }
    }
}

pub async fn bounded_range<B: ObjectBackend>(
    client: &ObjectClient<B>,
    scope: Option<(Arc<ReadObserver>, ReadContext)>,
    key: &str,
    offset: u64,
    length: u64,
) -> Result<Vec<u8>> {
    let fetch = scope
        .as_ref()
        .map(|(observer, context)| observer.start(Ledger::ValidatedFetch, *context, length));
    if usize::try_from(length).is_err() || offset.checked_add(length).is_none() {
        if let Some(fetch) = fetch {
            fetch.fail(FailureClass::Admission);
        }
        return Err(ReadBoundaryError::Admission.into());
    }
    let body = match &scope {
        Some((observer, context)) => range_stream(client, observer, *context, key, offset, length)
            .await
            .map(|body| Box::pin(body) as ObjectByteStream),
        None => client.backend_range_stream(key, offset, length).await,
    };
    match body {
        Ok(body) => collect_verified(body, fetch, None, length, Ok).await,
        Err(error) => {
            if let Some(fetch) = fetch {
                fetch.fail(FailureClass::Backend);
            }
            Err(error)
        }
    }
}

/// Install once into FsStats after initialization, retaining the observer
/// allocated before startup probe and manifest authentication.
#[derive(Debug)]
pub struct ReadStatsExtension(pub Arc<ReadObserver>);

#[derive(Debug)]
pub struct ObserverSession(pub Arc<ReadObserver>);

impl Drop for ObserverSession {
    fn drop(&mut self) {
        // Snapshot contains only bounded enum labels and numeric counters.
        // It intentionally contains no request key, URL or SDK source chain.
        tracing::info!(snapshot = ?self.0.state.lock().unwrap_or_else(|e| e.into_inner()), "object read observer session closed");
    }
}

impl crate::vfs::stats::FsStatsExtension for ReadStatsExtension {
    fn render_max_bytes(&self) -> usize {
        self.0.render_max_bytes()
    }
    fn begin_stats_observation(&self) -> Option<TerminalGuard> {
        Some(self.0.start(
            Ledger::LogicalOperation,
            ReadContext {
                engine: Engine::Packed,
                phase: Phase::Runtime,
                class: ReadClass::StatsSnapshot,
                origin: Origin::StatsObserver,
            },
            0,
        ))
    }
    fn render_into(&self, output: &mut dyn std::fmt::Write) {
        self.0.render_into(output);
    }
}

#[derive(Debug)]
pub struct NativeReadStatsExtension(pub Arc<ReadObserver>);
impl crate::vfs::stats::FsStatsExtension for NativeReadStatsExtension {
    fn render_max_bytes(&self) -> usize {
        self.0.render_max_bytes()
    }
    fn begin_stats_observation(&self) -> Option<TerminalGuard> {
        Some(self.0.start(
            Ledger::LogicalOperation,
            ReadContext {
                engine: Engine::Native,
                phase: Phase::Runtime,
                class: ReadClass::StatsSnapshot,
                origin: Origin::StatsObserver,
            },
            0,
        ))
    }
    fn render_into(&self, output: &mut dyn std::fmt::Write) {
        self.0.render_into(output);
    }
}

/// One authenticated decode in one prepared operation. Not a global
/// frame/cookie map. Its storage must be admitted as part of the G07 permit.
/// A future shared cache needs a last-consumer lifetime and independent
/// operation delivery leases; summary(success) is only valid for this scope.
#[derive(Debug)]
pub struct RawCoverage {
    raw_bytes: u64,
    requested: Vec<u64>,
    copied: Vec<u64>,
    requested_union: u64,
    copied_union: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RawSummary {
    pub decoded_raw: u64,
    pub requested_union: u64,
    pub copied_union: u64,
    pub delivered_union: u64,
    pub requested_overfetch: u64,
    pub copied_overfetch: u64,
    pub undelivered_decoded_raw: u64,
}

impl RawCoverage {
    pub fn new(decoded_raw: u64, tracking_budget: u64) -> Result<Self> {
        if decoded_raw > 8 * 1024 * 1024 {
            anyhow::bail!("raw observation unit exceeds the authenticated decode limit");
        }
        let bytes = Self::required_tracking_bytes(decoded_raw)?;
        let words = decoded_raw.div_ceil(64);
        if bytes > tracking_budget {
            anyhow::bail!("raw tracking requires an admitted byte budget");
        }
        let words = usize::try_from(words)?;
        Ok(Self {
            raw_bytes: decoded_raw,
            requested: vec![0; words],
            copied: vec![0; words],
            requested_union: 0,
            copied_union: 0,
        })
    }

    pub fn required_tracking_bytes(decoded_raw: u64) -> Result<u64> {
        if decoded_raw > 8 * 1024 * 1024 {
            anyhow::bail!("raw observation unit exceeds authenticated decode limit");
        }
        decoded_raw
            .div_ceil(64)
            .checked_mul(16)
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Self>() as u64))
            .ok_or_else(|| anyhow::anyhow!("raw tracking allocation overflows"))
    }

    pub fn tracking_bytes(&self) -> u64 {
        ((self.requested.capacity() + self.copied.capacity()) * std::mem::size_of::<u64>()
            + std::mem::size_of::<Self>()) as u64
    }

    fn mark(
        raw_bytes: u64,
        bits: &mut [u64],
        required: Option<&[u64]>,
        start: u64,
        length: u64,
    ) -> Result<u64> {
        let end = start
            .checked_add(length)
            .ok_or_else(|| anyhow::anyhow!("raw union range overflows"))?;
        if end > raw_bytes {
            anyhow::bail!("raw union range exceeds authenticated decode");
        }
        if length == 0 {
            return Ok(0);
        }
        let first = (start / 64) as usize;
        let last = ((end - 1) / 64) as usize;
        let mask_for = |word: usize| {
            let lower = if word == first {
                (start % 64) as u32
            } else {
                0
            };
            let upper = if word == last {
                ((end - 1) % 64 + 1) as u32
            } else {
                64
            };
            (u64::MAX << lower)
                & if upper == 64 {
                    u64::MAX
                } else {
                    (1u64 << upper) - 1
                }
        };
        // Check the entire interval before mutating, so an invalid copied
        // extent cannot leave partial accounting behind.
        if let Some(required) = required {
            for (offset, required_word) in required[first..=last].iter().enumerate() {
                let word = first + offset;
                let mask = mask_for(word);
                if *required_word & mask != mask {
                    anyhow::bail!("raw copied interval was not requested by this operation");
                }
            }
        }
        let mut added = 0;
        for (offset, bits_word) in bits[first..=last].iter_mut().enumerate() {
            let word = first + offset;
            let mask = mask_for(word);
            added += u64::from((mask & !*bits_word).count_ones());
            *bits_word |= mask;
        }
        Ok(added)
    }

    pub fn request(&mut self, start: u64, length: u64) -> Result<()> {
        self.requested_union +=
            Self::mark(self.raw_bytes, &mut self.requested, None, start, length)?;
        Ok(())
    }

    pub fn copied(&mut self, start: u64, length: u64) -> Result<()> {
        self.copied_union += Self::mark(
            self.raw_bytes,
            &mut self.copied,
            Some(&self.requested),
            start,
            length,
        )?;
        Ok(())
    }

    pub fn summary(&self, operation_succeeded: bool) -> RawSummary {
        let delivered = if operation_succeeded {
            self.copied_union
        } else {
            0
        };
        RawSummary {
            decoded_raw: self.raw_bytes,
            requested_union: self.requested_union,
            copied_union: self.copied_union,
            delivered_union: delivered,
            requested_overfetch: self.raw_bytes - self.requested_union,
            copied_overfetch: self.raw_bytes - self.copied_union,
            undelivered_decoded_raw: self.raw_bytes - delivered,
        }
    }
}

impl Engine {
    const ALL: [Self; 2] = [Self::Packed, Self::Native];
    fn label(self) -> &'static str {
        match self {
            Self::Packed => "packed",
            Self::Native => "native",
        }
    }
}
impl Phase {
    const ALL: [Self; 2] = [Self::Startup, Self::Runtime];
    fn label(self) -> &'static str {
        match self {
            Self::Startup => "startup",
            Self::Runtime => "runtime",
        }
    }
}
impl Origin {
    const ALL: [Self; 4] = [
        Self::Demand,
        Self::Prefetch,
        Self::Warmup,
        Self::StatsObserver,
    ];
    fn label(self) -> &'static str {
        match self {
            Self::Demand => "demand",
            Self::Prefetch => "prefetch",
            Self::Warmup => "warmup",
            Self::StatsObserver => "stats_observer",
        }
    }
}
impl Ledger {
    const ALL: [Self; 5] = [
        Self::BackendBody,
        Self::ValidatedFetch,
        Self::LogicalOperation,
        Self::HttpAttempt,
        Self::SemanticValidation,
    ];
    fn label(self) -> &'static str {
        match self {
            Self::BackendBody => "backend_body",
            Self::ValidatedFetch => "validated_fetch",
            Self::LogicalOperation => "logical_operation",
            Self::HttpAttempt => "http_attempt",
            Self::SemanticValidation => "semantic_validation",
        }
    }
}
impl ReadClass {
    const ALL: [Self; 22] = [
        Self::ManifestProbe,
        Self::Manifest,
        Self::GroupIndex,
        Self::InodeIndex,
        Self::ContainerIndex,
        Self::FrameIndex,
        Self::ReverseIndex,
        Self::ColdIndex,
        Self::LargeIndex,
        Self::SourceStatsIndex,
        Self::GroupMetadata,
        Self::FrameDirectory,
        Self::ColdAttributes,
        Self::PackedPayload,
        Self::ExternalPayload,
        Self::NativeIndex,
        Self::NativeAttributes,
        Self::NativePayload,
        Self::LogicalRead,
        Self::InlinePayload,
        Self::StatsSnapshot,
        Self::PublicationVerification,
    ];
    fn label(self) -> &'static str {
        match self {
            Self::ManifestProbe => "manifest_probe",
            Self::Manifest => "manifest",
            Self::GroupIndex => "group_index",
            Self::InodeIndex => "inode_index",
            Self::ContainerIndex => "container_index",
            Self::FrameIndex => "frame_index",
            Self::ReverseIndex => "reverse_index",
            Self::ColdIndex => "cold_index",
            Self::LargeIndex => "large_index",
            Self::SourceStatsIndex => "source_stats_index",
            Self::GroupMetadata => "group_metadata",
            Self::FrameDirectory => "frame_directory",
            Self::ColdAttributes => "cold_attributes",
            Self::PackedPayload => "packed_payload",
            Self::ExternalPayload => "external_payload",
            Self::NativeIndex => "native_index",
            Self::NativeAttributes => "native_attributes",
            Self::NativePayload => "native_payload",
            Self::LogicalRead => "logical_read",
            Self::InlinePayload => "inline_payload",
            Self::StatsSnapshot => "stats_snapshot",
            Self::PublicationVerification => "publication_verify",
        }
    }
}

impl FailureClass {
    fn label(self) -> &'static str {
        match self {
            Self::Backend => "backend",
            Self::ShortBody => "short_body",
            Self::ExcessBody => "excess_body",
            Self::Authentication => "authentication",
            Self::Decode => "decode",
            Self::Schema => "schema",
            Self::Generation => "generation",
            Self::Admission => "admission",
            Self::HttpStatus => "http_status",
        }
    }
}

impl ReadWork {
    const ALL: [Self; 2] = [Self::Authentication, Self::Decode];
}

impl ReadEvent {
    const ALL: [Self; 5] = [
        Self::CacheHit,
        Self::CacheLookupMiss,
        Self::CacheDisabled,
        Self::FetchLeader,
        Self::SharedResultAfterMiss,
    ];
}

impl ReadContext {
    const MAX_CONTEXTS: usize =
        Engine::ALL.len() * Phase::ALL.len() * ReadClass::ALL.len() * Origin::ALL.len();
}

mod shared_raw;
#[cfg(test)]
mod tests;
pub(crate) use shared_raw::SharedRawReceipt;
#[cfg(feature = "workspace")]
pub(crate) use shared_raw::{SharedRawCoverage, SharedRawLimit};

impl RawSummary {
    fn conserved(&self) -> bool {
        self.requested_union.checked_add(self.requested_overfetch) == Some(self.decoded_raw)
            && self.copied_union.checked_add(self.copied_overfetch) == Some(self.decoded_raw)
            && self
                .delivered_union
                .checked_add(self.undelivered_decoded_raw)
                == Some(self.decoded_raw)
            && self.delivered_union <= self.copied_union
            && self.copied_union <= self.requested_union
    }
    fn accumulate(&mut self, other: Self) -> bool {
        add(&mut self.decoded_raw, other.decoded_raw)
            | add(&mut self.requested_union, other.requested_union)
            | add(&mut self.copied_union, other.copied_union)
            | add(&mut self.delivered_union, other.delivered_union)
            | add(&mut self.requested_overfetch, other.requested_overfetch)
            | add(&mut self.copied_overfetch, other.copied_overfetch)
            | add(
                &mut self.undelivered_decoded_raw,
                other.undelivered_decoded_raw,
            )
    }
    fn complete(mut self, delivered: bool) -> Self {
        self.delivered_union = if delivered { self.copied_union } else { 0 };
        self.undelivered_decoded_raw = self.decoded_raw - self.delivered_union;
        self
    }
}

#[derive(Debug, Default)]
struct DeliveryState {
    terminal: Option<bool>,
    raw: BTreeMap<ReadContext, RawSummary>,
    overflowed: bool,
    shared: Vec<Arc<SharedRawReceipt>>,
}

/// Finite enum buckets per complete read, shared by all of its chunk plans.
/// A frame may be dropped before the operation finishes; this token commits
/// delivery only once the complete caller-visible read succeeds.
#[derive(Debug)]
pub struct OperationDelivery {
    observer: Arc<ReadObserver>,
    state: Mutex<DeliveryState>,
}
impl OperationDelivery {
    fn new(observer: Arc<ReadObserver>) -> Arc<Self> {
        Arc::new(Self {
            observer,
            state: Mutex::new(DeliveryState::default()),
        })
    }
    fn record(&self, context: ReadContext, summary: RawSummary) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(delivered) = state.terminal {
            self.observer
                .record_raw(context, summary.complete(delivered));
        } else {
            let overflow = state.raw.entry(context).or_default().accumulate(summary);
            state.overflowed |= overflow;
        }
    }
    fn finish(&self, delivered: bool) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.terminal.is_some() {
            return;
        }
        state.terminal = Some(delivered);
        for (context, summary) in std::mem::take(&mut state.raw) {
            self.observer
                .record_raw(context, summary.complete(delivered));
        }
        if state.overflowed {
            self.observer
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .overflowed = true;
        }
        let shared = std::mem::take(&mut state.shared);
        drop(state);
        for receipt in shared {
            receipt.finish(delivered);
        }
    }
}
impl Drop for OperationDelivery {
    fn drop(&mut self) {
        self.finish(false);
    }
}

/// Holds bitmap union storage for one authenticated frame/inline decode.
/// Its G07 permit must reserve RawCoverage::required_tracking_bytes(raw).
#[derive(Debug)]
pub struct RawLease {
    coverage: RawCoverage,
    delivery: Arc<OperationDelivery>,
    context: ReadContext,
}
impl RawLease {
    pub fn new(
        raw: u64,
        tracking_budget: u64,
        delivery: Arc<OperationDelivery>,
        context: ReadContext,
    ) -> Result<Self> {
        delivery.observer.enable_raw();
        Ok(Self {
            coverage: RawCoverage::new(raw, tracking_budget)?,
            delivery,
            context,
        })
    }
    pub fn request(&mut self, offset: u64, length: u64) -> Result<()> {
        self.coverage.request(offset, length)
    }
    pub fn copied(&mut self, offset: u64, length: u64) -> Result<()> {
        self.coverage.copied(offset, length)
    }
}
impl Drop for RawLease {
    fn drop(&mut self) {
        self.delivery
            .record(self.context, self.coverage.summary(false));
    }
}
