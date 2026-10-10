//! Mount-scoped wire-005 demand planning and shared-body cancellation contract.
//! Results are in-flight owners, never a retained payload cache.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use tokio::sync::Notify;
mod coordinator;
pub(super) use coordinator::{V3DemandCoordinator, V3SharedFrame, queue_roots_bytes};

use super::budget::V3Owned;
use super::{V3BudgetPool, V3MountBudget, V3ObjectRef, V3OwnedPermit};
use crate::cadapter::read_observer::ReadContext;
use crate::chunk::read_plan::ReadGeneration;
use crate::workspace_overlay::packed_v3::wire::{PackedResult, PackedWireError};
use crate::workspace_overlay::packed_v3::{
    AccessProfile, PackedFrameDescriptor, SizeClass, SizeClassTable,
};

fn invalid(reason: &str) -> PackedWireError {
    PackedWireError::Invalid(format!("wire005 pipeline: {reason}"))
}
fn cancelled() -> Arc<PackedWireError> {
    Arc::new(PackedWireError::Backend(
        "wire005 shared body cancelled".into(),
    ))
}
fn limit(reason: &str) -> PackedWireError {
    PackedWireError::LimitExceeded(format!("wire005 pipeline: {reason}"))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct V3PipelineLimits {
    pub collection_delay: Duration,
    pub max_merge_gap: u64,
    pub max_range_bytes: u64,
    pub max_batch_frames: usize,
    pub max_pending_frames: usize,
    pub max_inflight_collections: usize,
}
impl Default for V3PipelineLimits {
    fn default() -> Self {
        Self {
            collection_delay: Duration::from_micros(250),
            max_merge_gap: 64 << 10,
            max_range_bytes: 8 << 20,
            max_batch_frames: 64,
            max_pending_frames: 1024,
            max_inflight_collections: 8,
        }
    }
}
impl V3PipelineLimits {
    pub fn validate(self) -> PackedResult<Self> {
        if self.collection_delay > Duration::from_millis(10)
            || self.max_merge_gap > 64 << 10
            || self.max_range_bytes == 0
            || self.max_range_bytes > 8 << 20
            || self.max_batch_frames == 0
            || self.max_batch_frames > 64
            || self.max_pending_frames == 0
            || self.max_pending_frames > 1024
            || self.max_inflight_collections == 0
            || self.max_inflight_collections > 8
        {
            return Err(invalid("unbounded collection/range/queue limits"));
        }
        Ok(self)
    }
}

/// Construct only after the manifest/FD/container route has been authenticated.
/// The complete route and attribution are part of the flight identity.
#[derive(Clone, Debug)]
pub struct V3FrameDemand {
    pub generation: ReadGeneration,
    pub context: ReadContext,
    pub container: V3ObjectRef,
    pub profile: AccessProfile,
    pub size_classes: SizeClassTable,
    pub frame_policy: super::V3FramePolicy,
    pub descriptor: PackedFrameDescriptor,
    pub raw_offset: u64,
    pub logical_length: u64,
}
impl V3FrameDemand {
    pub fn validate(&self) -> PackedResult<()> {
        let frame = &self.descriptor;
        super::validate_key(&self.container.key)?;
        self.size_classes
            .validate()
            .map_err(|_| invalid("size table"))?;
        // A static frame target is independent of the file's size-class label.
        // Use the same authenticated policy/profile checks as an FD06 page.
        super::V3BuildPolicy {
            frames: self.frame_policy,
            inline_data: false,
            p90: None,
        }
        .select(1, self.profile, self.size_classes)?;
        let maximum = self
            .frame_policy
            .target()
            .unwrap_or(match frame.size_class {
                SizeClass::Tiny => 256 << 10,
                SizeClass::Small => 1 << 20,
                SizeClass::Medium | SizeClass::Large => match self.profile {
                    AccessProfile::RandomSmallFile | AccessProfile::Mixed => {
                        self.size_classes.max_random_frame_raw_bytes
                    }
                    AccessProfile::SequentialSmallFile => {
                        self.size_classes.max_sequential_frame_raw_bytes
                    }
                },
            });
        let codec = crate::workspace_overlay::packed_v3::PackedCodec::from_u8(frame.codec)?;
        if self.generation.lower_snapshot == [0; 32]
            || self.container.digest == [0; 32]
            || !matches!(
                self.container.kind,
                super::V3ObjectKind::GroupContainer | super::V3ObjectKind::LargeData
            )
            || self.context.class
                != if self.container.kind == super::V3ObjectKind::LargeData {
                    crate::cadapter::read_observer::ReadClass::ExternalPayload
                } else {
                    crate::cadapter::read_observer::ReadClass::PackedPayload
                }
            || self.container.object_len < (super::V3_HEADER_LEN + super::V3_FOOTER_LEN) as u64
            || self.container.object_len
                > (super::V3_HEADER_LEN + super::V3_MAX_BODY_BYTES + super::V3_FOOTER_LEN) as u64
            || self.size_classes.min_frame_raw_bytes > 256 << 10
            || self.size_classes.max_random_frame_raw_bytes > 4 << 20
            || self.size_classes.max_sequential_frame_raw_bytes > 8 << 20
            || self.logical_length == 0
            || self
                .raw_offset
                .checked_add(self.logical_length)
                .is_none_or(|n| n > u64::from(frame.raw_len))
            || frame.object_offset < super::V3_HEADER_LEN as u64
            || frame
                .object_offset
                .checked_add(u64::from(frame.stored_len))
                .is_none_or(|n| {
                    n > self
                        .container
                        .object_len
                        .saturating_sub(super::V3_FOOTER_LEN as u64)
                })
            || frame.raw_len == 0
            || u64::from(frame.raw_len) > maximum
            || frame.stored_len == 0
            || frame.stored_len > 8 << 20
            || frame.first_file_slot > frame.last_file_slot
            || (codec == crate::workspace_overlay::packed_v3::PackedCodec::Raw
                && frame.stored_len != frame.raw_len)
        {
            return Err(invalid("demand bounds/immutable identity"));
        }
        Ok(())
    }
    fn key(&self) -> V3FlightKey {
        let f = &self.descriptor;
        V3FlightKey {
            snapshot: self.generation.lower_snapshot,
            epoch: self.generation.workspace_head_epoch,
            mutation_sequence: self.generation.workspace_mutation_sequence,
            context: self.context,
            container_key: self.container.key.clone(),
            container_kind: self.container.kind as u8,
            container_digest: self.container.digest,
            container_length: self.container.object_len,
            profile: self.profile as u8,
            frame_policy: self.frame_policy as u8,
            ordinal: f.frame_ordinal,
            offset: f.object_offset,
            stored: f.stored_len,
            raw: f.raw_len,
            class: f.size_class as u8,
            codec: f.codec,
            digest: f.frame_digest,
            first_slot: f.first_file_slot,
            last_slot: f.last_file_slot,
            min_raw: self.size_classes.min_frame_raw_bytes,
            random_raw: self.size_classes.max_random_frame_raw_bytes,
            sequential_raw: self.size_classes.max_sequential_frame_raw_bytes,
        }
    }
}
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct V3FlightKey {
    snapshot: [u8; 32],
    epoch: u64,
    /// Same-head mutations advance this fence without changing the head
    /// epoch. It must be part of the flight identity so a request submitted
    /// after such a mutation cannot join a body captured by the prior view.
    mutation_sequence: u64,
    context: ReadContext,
    container_key: String,
    container_kind: u8,
    container_digest: [u8; 32],
    container_length: u64,
    profile: u8,
    frame_policy: u8,
    ordinal: u32,
    offset: u64,
    stored: u32,
    raw: u32,
    class: u8,
    codec: u8,
    digest: [u8; 16],
    first_slot: u32,
    last_slot: u32,
    min_raw: u64,
    random_raw: u64,
    sequential_raw: u64,
}

/// One immutable, admitted key is shared by the map, waiter and leader. The
/// String and its permit survive the originating plan until the last user.
#[derive(Clone, Debug)]
struct RegistryKey(Arc<V3Owned<V3FlightKey>>);
impl PartialEq for RegistryKey {
    fn eq(&self, other: &Self) -> bool {
        **self.0 == **other.0
    }
}
impl Eq for RegistryKey {}
impl PartialOrd for RegistryKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for RegistryKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (**self.0).cmp(&**other.0)
    }
}

#[derive(Debug)]
pub struct V3FrameBatch {
    pub offset: u64,
    pub length: u64,
    pub frames: Vec<V3FrameDemand>,
    /// Same-frame repeated logical contributions are counted independently.
    pub logical_contribution_bytes: u64,
    /// Submitted distinct frame bytes; does not include gaps in this GET.
    pub stored_frame_bytes: u64,
}
impl V3FrameBatch {
    pub fn gap_bytes(&self) -> u64 {
        self.length.saturating_sub(self.stored_frame_bytes)
    }
    /// Admit before opening any body. Decode is sequential within a batch.
    /// Returned raw/tracking ownership must be transferred to final Arc owners.
    pub fn admit(&self, budget: &Arc<V3MountBudget>) -> PackedResult<V3OwnedPermit> {
        budget.admit(&self.admission_charges()?)
    }
    fn fits_capacity(&self, budget: &Arc<V3MountBudget>) -> PackedResult<bool> {
        Ok(self
            .admission_charges()?
            .iter()
            .all(|(pool, bytes)| *bytes <= budget.capacity(*pool)))
    }
    fn admission_charges(&self) -> PackedResult<[(V3BudgetPool, u64); 4]> {
        let raw = self.frames.iter().try_fold(0u64, |sum, d| {
            sum.checked_add(u64::from(d.descriptor.raw_len))
                .ok_or_else(|| invalid("raw sum overflows"))
        })?;
        let workspace = self
            .frames
            .iter()
            .map(|d| {
                crate::workspace_overlay::packed_v3::codec::decode_workspace_bytes(
                    crate::workspace_overlay::packed_v3::PackedCodec::from_u8(d.descriptor.codec)?,
                )
            })
            .collect::<PackedResult<Vec<_>>>()?
            .into_iter()
            .max()
            .unwrap_or(0) as u64;
        let tracking = self.frames.iter().try_fold(0u64, |sum, d| {
            let bytes = crate::cadapter::read_observer::SharedRawCoverage::required_tracking_bytes(
                u64::from(d.descriptor.raw_len),
            )
            .map_err(|e| limit(&e.to_string()))?;
            sum.checked_add(bytes)
                .ok_or_else(|| invalid("shared union tracking overflows"))
        })?;
        Ok([
            (
                V3BudgetPool::Stored,
                self.length
                    .checked_mul(2)
                    .ok_or_else(|| invalid("stored ownership overflows"))?,
            ),
            (V3BudgetPool::Raw, raw),
            (
                V3BudgetPool::Workspace,
                workspace
                    .checked_add(tracking)
                    .ok_or_else(|| invalid("workspace ownership overflows"))?,
            ),
            (V3BudgetPool::Control, 4096 + self.frames.len() as u64 * 512),
        ])
    }
    /// The mount worker pre-admits and retains its fixed execution/body state
    /// in Roots. Only final per-frame controls need new admission here. The
    /// standalone public batch path retains its independent 4096-byte owner.
    fn worker_admission_charges(&self) -> PackedResult<[(V3BudgetPool, u64); 4]> {
        let mut charges = self.admission_charges()?;
        charges[3].1 -= 4096;
        Ok(charges)
    }
}

/// Only submitted demands enter the planner. No adjacent frame is invented.
/// Equal snapshot/container/profile/class/codec/context is required for merging.
pub fn plan_v3_demand_batches(
    demands: &[V3FrameDemand],
    limits: V3PipelineLimits,
    budget: &Arc<V3MountBudget>,
) -> PackedResult<V3Owned<Vec<V3FrameBatch>>> {
    plan_v3_weighted_batches(demands, limits, budget, |d| d.logical_length)
}
fn plan_v3_weighted_batches(
    demands: &[V3FrameDemand],
    limits: V3PipelineLimits,
    budget: &Arc<V3MountBudget>,
    contribution: impl Fn(&V3FrameDemand) -> u64,
) -> PackedResult<V3Owned<Vec<V3FrameBatch>>> {
    let limits = limits.validate()?;
    if demands.len() > limits.max_pending_frames {
        return Err(limit("pending frame count exceeds limit"));
    }
    let bytes = demands.iter().try_fold(4096u64, |sum, d| {
        (d.container.key.len() as u64)
            .checked_mul(4)
            .and_then(|n| n.checked_add((std::mem::size_of::<V3FrameDemand>() as u64) * 8 + 512))
            .and_then(|n| sum.checked_add(n))
            .ok_or_else(|| limit("plan ownership overflows"))
    })?;
    let permit = budget.admit(&[(V3BudgetPool::Plans, bytes)])?;
    let mut unique = BTreeMap::<V3FlightKey, (V3FrameDemand, u64)>::new();
    for d in demands {
        d.validate()?;
        let row = unique.entry(d.key()).or_insert_with(|| (d.clone(), 0));
        row.1 = row
            .1
            .checked_add(contribution(d))
            .ok_or_else(|| invalid("logical contribution overflows"))?;
    }
    let mut rows = unique.into_values().collect::<Vec<_>>();
    rows.sort_by(|(a, _), (b, _)| {
        let mut ak = a.key();
        let mut bk = b.key();
        // Ordinal need not determine physical order. Offsets do.
        ak.ordinal = 0;
        bk.ordinal = 0;
        ak.offset = 0;
        bk.offset = 0;
        ak.stored = 0;
        bk.stored = 0;
        ak.raw = 0;
        bk.raw = 0;
        ak.digest = [0; 16];
        bk.digest = [0; 16];
        ak.first_slot = 0;
        bk.first_slot = 0;
        ak.last_slot = 0;
        bk.last_slot = 0;
        ak.cmp(&bk)
            .then(a.descriptor.object_offset.cmp(&b.descriptor.object_offset))
    });
    let mut batches = Vec::<V3FrameBatch>::new();
    for (d, logical) in rows {
        let start = d.descriptor.object_offset;
        let end = start + u64::from(d.descriptor.stored_len);
        let eligible = batches
            .last()
            .and_then(|b| b.frames.first().map(|first| (b, first)))
            .is_some_and(|(b, first)| {
                let a = first.key();
                let c = d.key();
                let same = a.snapshot == c.snapshot
                    && a.epoch == c.epoch
                    && a.mutation_sequence == c.mutation_sequence
                    && a.context == c.context
                    && a.container_key == c.container_key
                    && a.container_kind == c.container_kind
                    && a.container_digest == c.container_digest
                    && a.container_length == c.container_length
                    && a.profile == c.profile
                    && a.frame_policy == c.frame_policy
                    && a.class == c.class
                    && a.codec == c.codec
                    && a.min_raw == c.min_raw
                    && a.random_raw == c.random_raw
                    && a.sequential_raw == c.sequential_raw;
                if !same {
                    return false;
                }
                let Some(prior_end) = b.offset.checked_add(b.length) else {
                    return false;
                };
                if start < prior_end {
                    return false;
                }
                let Some(merged) = end.checked_sub(b.offset) else {
                    return false;
                };
                let logical = b.logical_contribution_bytes.saturating_add(logical);
                let multiplier = if d.profile == AccessProfile::SequentialSmallFile {
                    16
                } else {
                    4
                };
                start - prior_end <= limits.max_merge_gap
                    && merged <= limits.max_range_bytes
                    && merged <= logical.saturating_mul(multiplier)
                    && b.frames.len() < limits.max_batch_frames
            });
        if eligible {
            let b = batches.last_mut().unwrap();
            b.length = end - b.offset;
            b.logical_contribution_bytes = b
                .logical_contribution_bytes
                .checked_add(logical)
                .ok_or_else(|| invalid("batch contribution overflows"))?;
            b.stored_frame_bytes += u64::from(d.descriptor.stored_len);
            b.frames.push(d);
        } else {
            if u64::from(d.descriptor.stored_len) > limits.max_range_bytes {
                return Err(limit("one frame exceeds configured range cap"));
            }
            batches.push(V3FrameBatch {
                offset: start,
                length: u64::from(d.descriptor.stored_len),
                stored_frame_bytes: u64::from(d.descriptor.stored_len),
                logical_contribution_bytes: logical,
                frames: vec![d],
            });
        }
    }
    Ok(V3Owned::new(batches, permit))
}

#[derive(Debug, Default)]
struct BodyState {
    sealed: bool,
    cancelled: bool,
    live_flights: usize,
}
/// One physical GET may serve several flights. Cancel only after the final
/// flight loses its final logical waiter, or mount shutdown forces cancellation.
#[derive(Debug, Default)]
pub struct V3BodyCancellation {
    state: Mutex<BodyState>,
    changed: Notify,
}
impl V3BodyCancellation {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
    pub fn seal(&self) {
        let mut s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        s.sealed = true;
        if s.live_flights == 0 {
            s.cancelled = true;
            self.changed.notify_waiters();
        }
    }
    pub fn cancel(&self) {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .cancelled = true;
        self.changed.notify_waiters();
    }
    pub async fn cancelled(&self) {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .cancelled
            {
                return;
            }
            notified.await;
        }
    }
    fn attach(self: &Arc<Self>) -> PackedResult<BodyFlight> {
        let mut s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if s.sealed || s.cancelled {
            return Err(invalid("body no longer accepts new frames"));
        }
        s.live_flights = s
            .live_flights
            .checked_add(1)
            .ok_or_else(|| invalid("body flight count overflows"))?;
        Ok(BodyFlight(self.clone()))
    }
}
#[derive(Debug)]
struct BodyFlight(Arc<V3BodyCancellation>);
impl Drop for BodyFlight {
    fn drop(&mut self) {
        let mut s = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
        s.live_flights -= 1;
        if s.sealed && s.live_flights == 0 {
            s.cancelled = true;
            self.0.changed.notify_waiters();
        }
    }
}

pub(crate) type SharedFrameResult<T> = Result<Arc<T>, Arc<PackedWireError>>;
struct FlightState<T> {
    waiters: usize,
    result: Option<SharedFrameResult<T>>,
    body: Option<BodyFlight>,
    logical_contribution: u64,
    requests: Vec<(u64, u64, V3OwnedPermit)>,
    coverage: Option<Weak<crate::cadapter::read_observer::SharedRawCoverage>>,
}
struct Flight<T> {
    state: Mutex<FlightState<T>>,
    changed: Notify,
    _roots: Arc<V3OwnedPermit>,
    // Retire the slot after the state/requests and shared fixed owner are
    // destroyed, so a detached ticket cannot admit a replacement early.
    _slot: FlightSlot,
}
struct FlightSlot(Arc<AtomicUsize>);
impl Drop for FlightSlot {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}
pub(super) fn registry_roots_bytes<T>(limits: V3PipelineLimits) -> u64 {
    // At most max_pending_frames genuinely live Arc<Flight>, including
    // cancelled tickets no longer in the map. Four entry footprints plus 256
    // bytes bound BTreeMap node slack and allocator bookkeeping per slot.
    let slot = std::mem::size_of::<Flight<T>>()
        + 2 * std::mem::size_of::<usize>()
        + 4 * std::mem::size_of::<(RegistryKey, Weak<Flight<T>>)>()
        + 256;
    32768 + limits.max_pending_frames as u64 * slot as u64
}
struct RegistryState<T> {
    closed: bool,
    flights: BTreeMap<RegistryKey, Weak<Flight<T>>>,
}
struct RegistryInner<T> {
    state: Mutex<RegistryState<T>>,
    budget: Arc<V3MountBudget>,
    limits: V3PipelineLimits,
    live_slots: Arc<AtomicUsize>,
    roots: Arc<V3OwnedPermit>,
}
pub struct V3FlightRegistry<T> {
    inner: Arc<RegistryInner<T>>,
}
pub struct V3FrameWaiter<T> {
    flight: Arc<Flight<T>>,
    registry: Weak<RegistryInner<T>>,
    key: RegistryKey,
    _control: V3OwnedPermit,
}
/// Move this work ticket into a worker independent of the first request future.
/// Dropping the first waiter must not drop this ticket while followers exist.
pub struct V3FrameLeader<T> {
    flight: Arc<Flight<T>>,
    registry: Weak<RegistryInner<T>>,
    key: RegistryKey,
    finished: bool,
}

fn remove<T>(registry: &Weak<RegistryInner<T>>, key: &RegistryKey, flight: &Arc<Flight<T>>) {
    if let Some(registry) = registry.upgrade() {
        let mut state = registry.state.lock().unwrap_or_else(|e| e.into_inner());
        if state
            .flights
            .get(key)
            .and_then(Weak::upgrade)
            .is_some_and(|found| Arc::ptr_eq(&found, flight))
        {
            state.flights.remove(key);
        }
    }
}
fn finish<T>(flight: &Arc<Flight<T>>, result: SharedFrameResult<T>) {
    let mut state = flight.state.lock().unwrap_or_else(|e| e.into_inner());
    if state.result.is_none() {
        state.result = Some(result);
        state.body = None;
    }
    drop(std::mem::take(&mut state.requests));
    drop(state);
    flight.changed.notify_waiters();
}
impl<T> V3FlightRegistry<T> {
    pub fn new(budget: Arc<V3MountBudget>, limits: V3PipelineLimits) -> PackedResult<Self> {
        let limits = limits.validate()?;
        let roots =
            Arc::new(budget.admit(&[(V3BudgetPool::Roots, registry_roots_bytes::<T>(limits))])?);
        Ok(Self {
            inner: Arc::new(RegistryInner {
                state: Mutex::new(RegistryState {
                    closed: false,
                    flights: BTreeMap::new(),
                }),
                budget,
                limits,
                live_slots: Arc::new(AtomicUsize::new(0)),
                roots,
            }),
        })
    }
    pub fn begin(
        &self,
        demand: &V3FrameDemand,
    ) -> PackedResult<(V3FrameWaiter<T>, Option<V3FrameLeader<T>>)> {
        demand.validate()?;
        // The fixed waiter/future bound stays in Control. The old two String
        // copies have been replaced by one independent, shared recipe owner.
        let control = self.inner.budget.admit(&[(V3BudgetPool::Control, 2048)])?;
        // Logical demand knowledge survives a cancelled waiter until its
        // physical decode completes. It therefore has independent ownership.
        let request_owner = self.inner.budget.admit(&[(V3BudgetPool::Control, 256)])?;
        let key_bytes = std::mem::size_of::<V3Owned<V3FlightKey>>() as u64
            + 2 * std::mem::size_of::<usize>() as u64
            + demand.container.key.len() as u64
            + 128;
        let key_owner = self
            .inner
            .budget
            .admit(&[(V3BudgetPool::Plans, key_bytes)])?;
        let key = RegistryKey(Arc::new(V3Owned::new(demand.key(), key_owner)));
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.closed {
            return Err(invalid("mount pipeline is shut down"));
        }
        if let Some((existing_key, flight)) = state
            .flights
            .get_key_value(&key)
            .and_then(|(key, flight)| flight.upgrade().map(|flight| (key.clone(), flight)))
        {
            let mut status = flight.state.lock().unwrap_or_else(|e| e.into_inner());
            if status.result.is_some() || status.waiters == 0 {
                drop(status);
                state.flights.remove(&key);
            } else {
                if status.requests.len() >= 4096 {
                    return Err(limit("same-frame contribution limit"));
                }
                let logical = status
                    .logical_contribution
                    .checked_add(demand.logical_length)
                    .ok_or_else(|| limit("logical contribution overflows"))?;
                let waiters = status
                    .waiters
                    .checked_add(1)
                    .ok_or_else(|| invalid("waiter count overflows"))?;
                if let Some(coverage) = status.coverage.as_ref().and_then(Weak::upgrade) {
                    coverage
                        .request(demand.raw_offset, demand.logical_length)
                        .map_err(|e| invalid(&e.to_string()))?;
                }
                status.waiters = waiters;
                status.logical_contribution = logical;
                status
                    .requests
                    .push((demand.raw_offset, demand.logical_length, request_owner));
                drop(status);
                return Ok((
                    V3FrameWaiter {
                        flight,
                        registry: Arc::downgrade(&self.inner),
                        key: existing_key,
                        _control: control,
                    },
                    None,
                ));
            }
        }
        // Map removal alone cannot release a slot: a cancelled worker ticket
        // may still own the flight. Acquire before the Arc allocation and let
        // its last owner retire the actual slot and shared Roots together.
        self.inner
            .live_slots
            .try_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                (count < self.inner.limits.max_pending_frames).then(|| count + 1)
            })
            .map_err(|_| limit("mount pending queue is full"))?;
        let slot = FlightSlot(self.inner.live_slots.clone());
        let flight = Arc::new(Flight {
            state: Mutex::new(FlightState {
                waiters: 1,
                result: None,
                body: None,
                logical_contribution: demand.logical_length,
                requests: vec![(demand.raw_offset, demand.logical_length, request_owner)],
                coverage: None,
            }),
            changed: Notify::new(),
            _slot: slot,
            _roots: self.inner.roots.clone(),
        });
        state.flights.insert(key.clone(), Arc::downgrade(&flight));
        Ok((
            V3FrameWaiter {
                flight: flight.clone(),
                registry: Arc::downgrade(&self.inner),
                key: key.clone(),
                _control: control,
            },
            Some(V3FrameLeader {
                flight,
                registry: Arc::downgrade(&self.inner),
                key,
                finished: false,
            }),
        ))
    }
    pub fn shutdown(&self) {
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        state.closed = true;
        let flights = std::mem::take(&mut state.flights).into_values();
        drop(state);
        for flight in flights {
            if let Some(flight) = flight.upgrade() {
                let mut status = flight.state.lock().unwrap_or_else(|e| e.into_inner());
                if let Some(body) = &status.body {
                    body.0.cancel();
                }
                if status.result.is_none() {
                    status.result = Some(Err(cancelled()));
                    status.body = None;
                }
                drop(status);
                flight.changed.notify_waiters();
            }
        }
    }
}
impl<T> Drop for V3FlightRegistry<T> {
    fn drop(&mut self) {
        self.shutdown();
    }
}
impl<T> V3FrameWaiter<T> {
    pub async fn wait(&self) -> SharedFrameResult<T> {
        loop {
            let notified = self.flight.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(result) = &self
                .flight
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .result
            {
                return result.clone();
            }
            notified.await;
        }
    }
}
impl<T> Drop for V3FrameWaiter<T> {
    fn drop(&mut self) {
        let mut status = self.flight.state.lock().unwrap_or_else(|e| e.into_inner());
        status.waiters -= 1;
        let last = status.waiters == 0 && status.result.is_none();
        if last {
            status.result = Some(Err(cancelled()));
            status.body = None;
        }
        drop(status);
        if last {
            remove(&self.registry, &self.key, &self.flight);
            self.flight.changed.notify_waiters();
        }
    }
}
impl<T> V3FrameLeader<T> {
    fn record_requests(
        &self,
        coverage: &Arc<crate::cadapter::read_observer::SharedRawCoverage>,
    ) -> PackedResult<()> {
        let mut status = self.flight.state.lock().unwrap_or_else(|e| e.into_inner());
        status.coverage = Some(Arc::downgrade(coverage));
        for (start, length, _) in &status.requests {
            coverage
                .request(*start, *length)
                .map_err(|e| invalid(&e.to_string()))?;
        }
        Ok(())
    }
    fn contribution(&self) -> u64 {
        self.flight
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .logical_contribution
    }
    fn live(&self) -> bool {
        let status = self.flight.state.lock().unwrap_or_else(|e| e.into_inner());
        status.waiters != 0 && status.result.is_none()
    }
    pub fn attach_body(&self, body: &Arc<V3BodyCancellation>) -> PackedResult<bool> {
        let mut status = self.flight.state.lock().unwrap_or_else(|e| e.into_inner());
        if status.waiters == 0 || status.result.is_some() {
            return Ok(false);
        }
        if status.body.is_some() {
            return Err(invalid("frame was attached to two physical bodies"));
        }
        status.body = Some(body.attach()?);
        Ok(true)
    }
    /// Call only after actual EOF and every requested frame's authentication.
    pub fn complete(mut self, result: Result<T, PackedWireError>) {
        remove(&self.registry, &self.key, &self.flight);
        finish(&self.flight, result.map(Arc::new).map_err(Arc::new));
        self.finished = true;
    }
}
impl<T> Drop for V3FrameLeader<T> {
    fn drop(&mut self) {
        if !self.finished {
            remove(&self.registry, &self.key, &self.flight);
            finish(&self.flight, Err(cancelled()));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cadapter::read_observer::{Engine, Origin, Phase, ReadClass};
    use crate::workspace_overlay::packed_v3::wire005::V3ObjectKind;
    fn demand(ordinal: u32, offset: u64) -> V3FrameDemand {
        V3FrameDemand {
            generation: ReadGeneration {
                workspace_head_epoch: 0,
                workspace_mutation_sequence: 0,
                lower_snapshot: [7; 32],
            },
            context: ReadContext {
                engine: Engine::PackedV3,
                phase: Phase::Runtime,
                origin: Origin::Demand,
                class: ReadClass::PackedPayload,
            },
            container: V3ObjectRef {
                key: "container".into(),
                kind: V3ObjectKind::GroupContainer,
                object_len: 8192,
                digest: [8; 32],
            },
            profile: AccessProfile::RandomSmallFile,
            size_classes: SizeClassTable::default(),
            frame_policy: Default::default(),
            descriptor: PackedFrameDescriptor {
                frame_ordinal: ordinal,
                object_offset: 4096 + offset,
                stored_len: 64,
                raw_len: 64,
                first_file_slot: 0,
                last_file_slot: 0,
                size_class: SizeClass::Tiny,
                codec: 0,
                frame_digest: [9; 16],
            },
            raw_offset: 0,
            logical_length: 64,
        }
    }
    #[test]
    fn cancelled_ticket_retains_the_bounded_slot_until_its_real_owner_drops() {
        let budget = V3MountBudget::defaults();
        let limits = V3PipelineLimits {
            max_pending_frames: 1,
            ..Default::default()
        };
        let registry = V3FlightRegistry::<Vec<u8>>::new(budget.clone(), limits).unwrap();
        let (waiter, ticket) = registry.begin(&demand(0, 64)).unwrap();
        drop(waiter);
        // The map has detached this cancelled flight, but its worker ticket
        // still owns the fixed flight allocation and physical contribution.
        assert!(registry.begin(&demand(1, 160)).is_err());
        assert_eq!(budget.state().used[V3BudgetPool::Control as usize], 256);
        drop(ticket);
        let (fresh, ticket) = registry.begin(&demand(1, 160)).unwrap();
        drop(fresh);
        drop(registry);
        assert!(budget.state().used[V3BudgetPool::Roots as usize] > 0);
        drop(ticket);
        assert_eq!(budget.state().used, [0; 8]);
    }
    #[test]
    fn maximum_key_has_one_shared_recipe_owner_across_all_registry_users() {
        let budget = V3MountBudget::defaults();
        let registry =
            V3FlightRegistry::<Vec<u8>>::new(budget.clone(), V3PipelineLimits::default()).unwrap();
        let mut demand = demand(0, 64);
        demand.container.key = "a".repeat(4096);
        let (first, ticket) = registry.begin(&demand).unwrap();
        let ticket = ticket.unwrap();
        let (second, follower) = registry.begin(&demand).unwrap();
        assert!(follower.is_none());
        assert!(Arc::ptr_eq(&first.key.0, &second.key.0));
        assert!(Arc::ptr_eq(&first.key.0, &ticket.key.0));
        let state = registry.inner.state.lock().unwrap();
        let map_key = state.flights.first_key_value().unwrap().0;
        assert!(Arc::ptr_eq(&first.key.0, &map_key.0));
        drop(state);
        let recipe_bytes = budget.state().used[V3BudgetPool::Plans as usize];
        assert!(recipe_bytes >= 4096);
        assert_eq!(
            budget.state().used[V3BudgetPool::Control as usize],
            2 * (2048 + 256)
        );
        drop(first);
        assert_eq!(
            budget.state().used[V3BudgetPool::Plans as usize],
            recipe_bytes
        );
        ticket.complete(Ok(vec![7]));
        drop(second);
        drop(registry);
        assert_eq!(budget.state().used, [0; 8]);
    }
    #[tokio::test]
    async fn new_submission_does_not_join_a_terminal_flight_awaiting_registry_removal() {
        let budget = V3MountBudget::defaults();
        let registry =
            V3FlightRegistry::<Vec<u8>>::new(budget.clone(), V3PipelineLimits::default()).unwrap();
        let (old, ticket) = registry.begin(&demand(0, 64)).unwrap();
        // Pause the old flight in the terminal-before-map-removal state.
        old.flight.state.lock().unwrap().result = Some(Err(cancelled()));
        let (fresh, new_ticket) = registry.begin(&demand(0, 64)).unwrap();
        let new_ticket = new_ticket.expect("terminal entries must elect fresh work");
        drop(old);
        drop(ticket);
        new_ticket.complete(Ok(vec![7]));
        assert_eq!(*fresh.wait().await.unwrap(), vec![7]);
        drop(fresh);
        drop(registry);
        assert_eq!(budget.state().used, [0; 8]);
    }
    #[test]
    fn demand_only_batches_deduplicate_and_keep_class_codec_snapshot_origin_separate() {
        let budget = V3MountBudget::defaults();
        let a = demand(0, 64);
        let b = demand(1, 160);
        let batches = plan_v3_demand_batches(
            &[a.clone(), a.clone(), b.clone()],
            V3PipelineLimits::default(),
            &budget,
        )
        .unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].frames.len(), 2);
        assert_eq!(batches[0].gap_bytes(), 32);
        assert_eq!(batches[0].logical_contribution_bytes, 192);
        for variant in 0..5 {
            let mut b = b.clone();
            match variant {
                0 => b.context.engine = Engine::Native,
                1 => b.context.origin = Origin::Prefetch,
                2 => b.generation.lower_snapshot = [1; 32],
                3 => b.descriptor.size_class = SizeClass::Small,
                _ => b.frame_policy = super::super::V3FramePolicy::Static1Mib,
            };
            assert_eq!(
                plan_v3_demand_batches(&[a.clone(), b], V3PipelineLimits::default(), &budget)
                    .unwrap()
                    .len(),
                2
            );
        }
        drop(batches);
        assert_eq!(budget.state().used[V3BudgetPool::Plans as usize], 0);
    }
    #[test]
    fn same_head_mutation_sequence_never_shares_a_flight() {
        let budget = V3MountBudget::defaults();
        let registry =
            V3FlightRegistry::<Vec<u8>>::new(budget.clone(), V3PipelineLimits::default()).unwrap();
        let first = demand(0, 0);
        let mut after_same_epoch_mutation = first.clone();
        after_same_epoch_mutation
            .generation
            .workspace_mutation_sequence = 1;

        let (old_waiter, old_leader) = registry.begin(&first).unwrap();
        let (new_waiter, new_leader) = registry.begin(&after_same_epoch_mutation).unwrap();
        assert!(old_leader.is_some(), "the initial view must elect a leader");
        assert!(
            new_leader.is_some(),
            "a same-head mutation must force a new physical flight"
        );

        drop(old_waiter);
        drop(new_waiter);
        drop(old_leader);
        drop(new_leader);
        drop(registry);
        assert_eq!(budget.state().used, [0; 8]);
    }

    #[test]
    fn demand_batches_keep_same_epoch_mutation_sequences_separate() {
        let budget = V3MountBudget::defaults();
        let first = demand(0, 0);
        let mut after_same_epoch_mutation = first.clone();
        after_same_epoch_mutation
            .generation
            .workspace_mutation_sequence = 1;
        let batches = plan_v3_demand_batches(
            &[first, after_same_epoch_mutation],
            V3PipelineLimits::default(),
            &budget,
        )
        .unwrap();
        assert_eq!(
            batches.len(),
            2,
            "a plan must never coalesce frames from different mutation fences"
        );
        drop(batches);
        assert_eq!(budget.state().used[V3BudgetPool::Plans as usize], 0);
    }

    #[test]
    fn static_policy_keeps_file_class_and_separates_identical_frame_flights() {
        use super::super::V3FramePolicy;
        for (policy, raw) in [
            (V3FramePolicy::Static1Mib, 1 << 20),
            (V3FramePolicy::Static4Mib, 4 << 20),
        ] {
            let mut large_tiny = demand(0, 0);
            large_tiny.container.object_len = 16 << 20;
            large_tiny.frame_policy = policy;
            large_tiny.descriptor.stored_len = raw;
            large_tiny.descriptor.raw_len = raw;
            assert_eq!(large_tiny.descriptor.size_class, SizeClass::Tiny);
            large_tiny.validate().unwrap();
            large_tiny.frame_policy = V3FramePolicy::SizeOnly;
            assert!(large_tiny.validate().is_err());
        }
        let budget = V3MountBudget::defaults();
        let registry =
            V3FlightRegistry::<Vec<u8>>::new(budget.clone(), V3PipelineLimits::default()).unwrap();
        let first = demand(0, 0);
        let mut different_policy = first.clone();
        different_policy.frame_policy = V3FramePolicy::Static1Mib;
        let (a, first_work) = registry.begin(&first).unwrap();
        let (b, second_work) = registry.begin(&different_policy).unwrap();
        assert!(first_work.is_some());
        assert!(
            second_work.is_some(),
            "policy is part of immutable flight identity"
        );
        drop(a);
        drop(b);
        drop(first_work);
        drop(second_work);
        drop(registry);
        assert_eq!(budget.state().used, [0; 8]);
    }

    #[tokio::test]
    async fn first_waiter_cancellation_does_not_cancel_followers_and_late_call_is_new_work() {
        let budget = V3MountBudget::defaults();
        let registry =
            V3FlightRegistry::<V3Owned<Vec<u8>>>::new(budget.clone(), V3PipelineLimits::default())
                .unwrap();
        let (first, work) = registry.begin(&demand(0, 64)).unwrap();
        let work = work.unwrap();
        let (second, follower) = registry.begin(&demand(0, 64)).unwrap();
        assert!(follower.is_none());
        let body = V3BodyCancellation::new();
        assert!(work.attach_body(&body).unwrap());
        body.seal();
        drop(first);
        assert!(
            tokio::time::timeout(Duration::from_millis(1), body.cancelled())
                .await
                .is_err()
        );
        let raw = budget.admit(&[(V3BudgetPool::Raw, 4)]).unwrap();
        work.complete(Ok(V3Owned::new(vec![1, 2, 3, 4], raw)));
        let frame = second.wait().await.unwrap();
        drop(second);
        assert_eq!(budget.state().used[V3BudgetPool::Raw as usize], 4);
        let (late, new_work) = registry.begin(&demand(0, 64)).unwrap();
        assert!(new_work.is_some());
        drop(late);
        drop(new_work);
        drop(frame);
        assert_eq!(budget.state().used[V3BudgetPool::Raw as usize], 0);
        drop(registry);
        assert_eq!(budget.state().used, [0; 8]);
    }
    #[tokio::test]
    async fn coalesced_body_cancels_after_last_waiter_of_last_frame_and_shutdown_wakes_all() {
        let budget = V3MountBudget::defaults();
        let registry =
            V3FlightRegistry::<Vec<u8>>::new(budget.clone(), V3PipelineLimits::default()).unwrap();
        let (a, wa) = registry.begin(&demand(0, 64)).unwrap();
        let (b, wb) = registry.begin(&demand(1, 160)).unwrap();
        let wa = wa.unwrap();
        let wb = wb.unwrap();
        let body = V3BodyCancellation::new();
        assert!(wa.attach_body(&body).unwrap());
        assert!(wb.attach_body(&body).unwrap());
        body.seal();
        drop(a);
        assert!(
            tokio::time::timeout(Duration::from_millis(1), body.cancelled())
                .await
                .is_err()
        );
        drop(b);
        tokio::time::timeout(Duration::from_secs(1), body.cancelled())
            .await
            .unwrap();
        drop(wa);
        drop(wb);
        let (w, leader) = registry.begin(&demand(0, 64)).unwrap();
        registry.shutdown();
        assert!(w.wait().await.is_err());
        assert!(registry.begin(&demand(0, 64)).is_err());
        drop(w);
        drop(leader);
        drop(registry);
        assert_eq!(budget.state().used, [0; 8]);
    }
}
