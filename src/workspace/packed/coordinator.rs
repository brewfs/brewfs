//! Bounded frame range planning and cold-read coordination.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::Bytes;
use futures_util::StreamExt;
#[cfg(test)]
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex, Notify, Semaphore, oneshot};

use super::layout::AccessProfile;
use super::metrics::PackedRuntimeMetrics;
use super::remote::RemotePackedObject;
use super::{PackedFrameDescriptor, PackedResult, PackedWireError, SizeClass};

const COALESCE_DELAY: Duration = Duration::from_micros(250);

/// Limits applied before any remote range request is started.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CoordinatorLimits {
    pub max_merge_gap: u64,
    pub max_coalesced_range: u64,
    pub pipeline_bytes_budget: u64,
    pub max_inflight_ranges: usize,
}

impl Default for CoordinatorLimits {
    fn default() -> Self {
        Self {
            max_merge_gap: 64 * 1024,
            max_coalesced_range: 8 * 1024 * 1024,
            pipeline_bytes_budget: 32 * 1024 * 1024,
            max_inflight_ranges: 16,
        }
    }
}

impl CoordinatorLimits {
    fn validate(self) -> PackedResult<Self> {
        if self.max_coalesced_range == 0
            || self.pipeline_bytes_budget == 0
            || self.max_inflight_ranges == 0
            || self.max_coalesced_range > super::remote::MAX_PACKED_STREAM_RANGE_BYTES
        {
            return Err(PackedWireError::LimitExceeded(
                "invalid packed group coordinator limits".into(),
            ));
        }
        Ok(self)
    }
}

/// A requested frame and its logical contribution to the caller's read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FrameReadRequest {
    pub descriptor: PackedFrameDescriptor,
    pub logical_len: u64,
}

/// One physical range request and the frames contained in it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoalescedRange {
    pub offset: u64,
    pub length: u64,
    pub size_class: SizeClass,
    pub frames: Vec<FrameReadRequest>,
    pub logical_bytes: u64,
}

/// Per-mount coordinator configuration. It is intentionally stateless between
/// calls: decoded frames are returned to the caller and are not retained as a
/// hidden data cache.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GroupReadCoordinator {
    profile: AccessProfile,
    limits: CoordinatorLimits,
}

/// Mount-scoped demand coordinator.  FUSE normally delivers adjacent file
/// reads as separate tasks, so a stateless per-call planner cannot amortize
/// the object-store RTT.  This coordinator collects requests for one tick,
/// groups them by immutable container and access profile, and then reuses the
/// same bounded range planner.  It retains no frame bytes after replies are
/// delivered; the only state between calls is the short pending queue.
#[derive(Clone)]
pub(crate) struct SharedGroupReadCoordinator<B: crate::cadapter::client::ObjectBackend + Clone> {
    state: Arc<SharedCoordinatorState<B>>,
    limits: CoordinatorLimits,
}

struct SharedCoordinatorState<B: crate::cadapter::client::ObjectBackend + Clone> {
    pending: Mutex<Vec<PendingRead<B>>>,
    notify: Notify,
    worker_started: AtomicBool,
    runtime_metrics: Arc<PackedRuntimeMetrics>,
}

struct PendingRead<B: crate::cadapter::client::ObjectBackend + Clone> {
    object: Arc<RemotePackedObject<B>>,
    profile: AccessProfile,
    requests: Vec<FrameReadRequest>,
    reply: oneshot::Sender<PackedResult<BTreeMap<u32, Bytes>>>,
}

struct PendingBatchGroup<B: crate::cadapter::client::ObjectBackend + Clone> {
    object: Arc<RemotePackedObject<B>>,
    profile: AccessProfile,
    requests: Vec<FrameReadRequest>,
    waiters: Vec<PendingRead<B>>,
}

/// Batch-scoped permits shared by every container group dispatched in one
/// coordinator tick.  Keeping the byte and range budgets here prevents group
/// parallelism from multiplying the configured mount-level limits.
struct SharedReadBudget {
    bytes: Arc<Semaphore>,
    ranges: Arc<Semaphore>,
}

impl SharedReadBudget {
    fn new(limits: CoordinatorLimits) -> PackedResult<Arc<Self>> {
        let bytes = usize::try_from(limits.pipeline_bytes_budget).map_err(|_| {
            PackedWireError::LimitExceeded("pipeline byte budget exceeds usize".into())
        })?;
        Ok(Arc::new(Self {
            bytes: Arc::new(Semaphore::new(bytes)),
            ranges: Arc::new(Semaphore::new(limits.max_inflight_ranges)),
        }))
    }
}

impl<B> SharedGroupReadCoordinator<B>
where
    B: crate::cadapter::client::ObjectBackend + Clone + 'static,
{
    pub(crate) fn new(limits: CoordinatorLimits) -> PackedResult<Self> {
        Self::new_with_metrics(limits, Arc::new(PackedRuntimeMetrics::default()))
    }

    pub(crate) fn new_with_metrics(
        limits: CoordinatorLimits,
        runtime_metrics: Arc<PackedRuntimeMetrics>,
    ) -> PackedResult<Self> {
        Ok(Self {
            state: Arc::new(SharedCoordinatorState {
                pending: Mutex::new(Vec::new()),
                notify: Notify::new(),
                worker_started: AtomicBool::new(false),
                runtime_metrics,
            }),
            limits: limits.validate()?,
        })
    }

    pub(crate) async fn submit(
        &self,
        object: Arc<RemotePackedObject<B>>,
        profile: AccessProfile,
        requests: Vec<FrameReadRequest>,
    ) -> PackedResult<BTreeMap<u32, Bytes>> {
        if requests.is_empty() {
            return Ok(BTreeMap::new());
        }
        let (reply, result) = oneshot::channel();
        {
            let mut pending = self.state.pending.lock().await;
            pending.push(PendingRead {
                object,
                profile,
                requests,
                reply,
            });
        }
        self.start_worker();
        self.state.notify.notify_one();
        result.await.map_err(|_| {
            PackedWireError::Backend("packed read coordinator worker stopped".into())
        })?
    }

    fn start_worker(&self) {
        if self
            .state
            .worker_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            let state = Arc::clone(&self.state);
            let limits = self.limits;
            tokio::spawn(async move { shared_coordinator_worker(state, limits).await });
        }
    }
}

async fn shared_coordinator_worker<B>(
    state: Arc<SharedCoordinatorState<B>>,
    limits: CoordinatorLimits,
) where
    B: crate::cadapter::client::ObjectBackend + Clone + 'static,
{
    loop {
        // Construct the notification future before checking the queue so a
        // producer racing with the check cannot lose its wake-up.
        let notified = state.notify.notified();
        if state.pending.lock().await.is_empty() {
            notified.await;
            continue;
        }
        drop(notified);
        tokio::time::sleep(COALESCE_DELAY).await;
        let batch = {
            let mut pending = state.pending.lock().await;
            std::mem::take(&mut *pending)
        };
        if batch.is_empty() {
            continue;
        }
        dispatch_shared_batch(batch, limits, Arc::clone(&state.runtime_metrics)).await;
    }
}

async fn dispatch_shared_batch<B>(
    batch: Vec<PendingRead<B>>,
    limits: CoordinatorLimits,
    runtime_metrics: Arc<PackedRuntimeMetrics>,
) where
    B: crate::cadapter::client::ObjectBackend + Clone + 'static,
{
    let mut groups = Vec::<PendingBatchGroup<B>>::new();
    let mut indexes = HashMap::<(String, AccessProfile), usize>::new();
    for waiter in batch {
        let key = (waiter.object.object_key().to_owned(), waiter.profile);
        let group_index = if let Some(index) = indexes.get(&key).copied() {
            index
        } else {
            let index = groups.len();
            indexes.insert(key, index);
            groups.push(PendingBatchGroup {
                object: Arc::clone(&waiter.object),
                profile: waiter.profile,
                requests: Vec::new(),
                waiters: Vec::new(),
            });
            index
        };
        groups[group_index]
            .requests
            .extend(waiter.requests.iter().cloned());
        groups[group_index].waiters.push(waiter);
    }

    let budget = match SharedReadBudget::new(limits) {
        Ok(budget) => budget,
        Err(error) => {
            for group in groups {
                for waiter in group.waiters {
                    let _ = waiter.reply.send(Err(error.clone()));
                }
            }
            return;
        }
    };
    // Groups can now overlap their object-store RTT, while the shared range
    // and byte permits below keep the total in-flight work within the mount
    // budget.  Use the range limit as the group fan-out cap so a large batch
    // cannot create an unbounded number of active group futures.
    let group_concurrency = limits.max_inflight_ranges.max(1);
    let mut groups = futures_util::stream::iter(groups.into_iter().map(|group| {
        let budget = Arc::clone(&budget);
        let runtime_metrics = Arc::clone(&runtime_metrics);
        async move {
            let PendingBatchGroup {
                object,
                profile,
                requests,
                waiters,
            } = group;
            let result = read_coalesced_frames_with_budget(
                &object,
                profile,
                requests,
                limits,
                budget,
                Some(runtime_metrics),
            )
            .await;
            (waiters, result)
        }
    }))
    .buffer_unordered(group_concurrency);

    while let Some((waiters, result)) = groups.next().await {
        match result {
            Ok(frames) => {
                for waiter in waiters {
                    let mut output = BTreeMap::new();
                    let mut error = None;
                    for request in waiter.requests {
                        let ordinal = request.descriptor.frame_ordinal;
                        let Some(payload) = frames.get(&ordinal) else {
                            error = Some(PackedWireError::Invalid(
                                "coordinator response is missing a requested frame".into(),
                            ));
                            break;
                        };
                        output.insert(ordinal, payload.clone());
                    }
                    let _ = waiter.reply.send(match error {
                        Some(error) => Err(error),
                        None => Ok(output),
                    });
                }
            }
            Err(error) => {
                for waiter in waiters {
                    let _ = waiter.reply.send(Err(error.clone()));
                }
            }
        }
    }
}

impl GroupReadCoordinator {
    pub fn new(profile: AccessProfile, limits: CoordinatorLimits) -> PackedResult<Self> {
        Ok(Self {
            profile,
            limits: limits.validate()?,
        })
    }

    pub fn profile(&self) -> AccessProfile {
        self.profile
    }

    pub fn limits(&self) -> CoordinatorLimits {
        self.limits
    }

    pub fn plan(
        &self,
        requests: impl IntoIterator<Item = FrameReadRequest>,
    ) -> PackedResult<Vec<CoalescedRange>> {
        coalesce_frame_ranges(self.profile, requests, self.limits)
    }

    pub async fn read_frames<B: crate::cadapter::client::ObjectBackend + Clone>(
        &self,
        object: &RemotePackedObject<B>,
        requests: impl IntoIterator<Item = FrameReadRequest>,
    ) -> PackedResult<std::collections::BTreeMap<u32, Bytes>> {
        read_coalesced_frames(object, self.profile, requests, self.limits).await
    }
}

impl CoalescedRange {
    pub fn overscan_bytes(&self) -> u64 {
        self.length.saturating_sub(self.logical_bytes)
    }
}

/// Build deterministic, class-aware physical ranges. Duplicate frame
/// requests are collapsed here, which is the synchronous singleflight layer
/// used by strict-cold reads.
pub fn coalesce_frame_ranges(
    profile: AccessProfile,
    requests: impl IntoIterator<Item = FrameReadRequest>,
    limits: CoordinatorLimits,
) -> PackedResult<Vec<CoalescedRange>> {
    let limits = limits.validate()?;
    // A range can never consume more than the bytes available to the
    // pipeline.  Keep this bound in the merge decision itself so a valid
    // configuration with `pipeline_bytes_budget < max_coalesced_range`
    // produces several bounded requests instead of building an oversized
    // range and failing only after planning.
    let max_range = limits.max_coalesced_range.min(limits.pipeline_bytes_budget);
    let mut unique = HashMap::<u32, FrameReadRequest>::new();
    for request in requests {
        let descriptor = &request.descriptor;
        if descriptor.stored_len == 0
            || descriptor.raw_len == 0
            || descriptor.stored_len != descriptor.raw_len
            || request.logical_len == 0
            || request.logical_len > u64::from(descriptor.raw_len)
        {
            return Err(PackedWireError::Invalid(
                "frame range request has invalid descriptor or logical lengths".into(),
            ));
        }
        let end = descriptor
            .object_offset
            .checked_add(u64::from(descriptor.stored_len))
            .ok_or_else(|| PackedWireError::LimitExceeded("frame range overflows".into()))?;
        if end < descriptor.object_offset {
            return Err(PackedWireError::Invalid("frame range is inverted".into()));
        }
        match unique.entry(descriptor.frame_ordinal) {
            std::collections::hash_map::Entry::Occupied(mut existing) => {
                if existing.get().descriptor != request.descriptor {
                    return Err(PackedWireError::Invalid(
                        "same frame ordinal has conflicting descriptors".into(),
                    ));
                }
                let logical_len = existing.get().logical_len.max(request.logical_len);
                existing.get_mut().logical_len = logical_len;
            }
            std::collections::hash_map::Entry::Vacant(slot) => {
                slot.insert(request);
            }
        }
    }

    let mut by_class = BTreeMap::<u8, Vec<FrameReadRequest>>::new();
    for request in unique.into_values() {
        by_class
            .entry(request.descriptor.size_class as u8)
            .or_default()
            .push(request);
    }
    let mut result = Vec::new();
    for (_, mut requests) in by_class {
        requests.sort_by_key(|request| request.descriptor.object_offset);
        let mut current: Option<CoalescedRange> = None;
        for request in requests {
            let descriptor = &request.descriptor;
            let start = descriptor.object_offset;
            let end = start + u64::from(descriptor.stored_len);
            let should_merge = current.as_ref().is_some_and(|range| {
                let current_end = range.offset + range.length;
                let gap = start.saturating_sub(current_end);
                let merged_len = end.saturating_sub(range.offset);
                let logical = range.logical_bytes.saturating_add(request.logical_len);
                let multiplier = match profile {
                    AccessProfile::SequentialSmallFile => 16,
                    AccessProfile::RandomSmallFile | AccessProfile::Mixed => 4,
                };
                gap <= limits.max_merge_gap
                    && merged_len <= max_range
                    && merged_len <= logical.saturating_mul(multiplier).max(1)
            });
            if should_merge {
                let range = current.as_mut().expect("checked above");
                let range_end = end.max(range.offset + range.length);
                range.length = range_end - range.offset;
                range.logical_bytes = range.logical_bytes.saturating_add(request.logical_len);
                range.frames.push(request);
            } else {
                if let Some(range) = current.take() {
                    result.push(range);
                }
                current = Some(CoalescedRange {
                    offset: start,
                    length: u64::from(descriptor.stored_len),
                    size_class: descriptor.size_class,
                    logical_bytes: request.logical_len,
                    frames: vec![request],
                });
            }
        }
        if let Some(range) = current {
            result.push(range);
        }
    }
    result.sort_by_key(|range| range.offset);
    // A single frame larger than the pipeline budget cannot be split by this
    // planner.  Keep the explicit error for that case while merged ranges
    // are already bounded by `max_range` above.
    if result
        .iter()
        .any(|range| range.length > limits.pipeline_bytes_budget)
    {
        return Err(PackedWireError::LimitExceeded(
            "coalesced range exceeds pipeline byte budget".into(),
        ));
    }
    Ok(result)
}

/// Fetch the planned ranges with bounded in-flight bytes. Returned frame
/// payloads are detached from the range buffer, so callers can release the
/// coalesced response immediately after delivery.
pub async fn read_coalesced_frames<B: crate::cadapter::client::ObjectBackend + Clone>(
    object: &RemotePackedObject<B>,
    profile: AccessProfile,
    requests: impl IntoIterator<Item = FrameReadRequest>,
    limits: CoordinatorLimits,
) -> PackedResult<BTreeMap<u32, Bytes>> {
    let limits = limits.validate()?;
    let budget = SharedReadBudget::new(limits)?;
    read_coalesced_frames_with_budget(object, profile, requests, limits, budget, None).await
}

async fn read_coalesced_frames_with_budget<B: crate::cadapter::client::ObjectBackend + Clone>(
    object: &RemotePackedObject<B>,
    profile: AccessProfile,
    requests: impl IntoIterator<Item = FrameReadRequest>,
    limits: CoordinatorLimits,
    budget: Arc<SharedReadBudget>,
    runtime_metrics: Option<Arc<PackedRuntimeMetrics>>,
) -> PackedResult<BTreeMap<u32, Bytes>> {
    let requests = requests.into_iter().collect::<Vec<_>>();
    let ranges = coalesce_frame_ranges(profile, requests.clone(), limits)?;
    if let Some(metrics) = &runtime_metrics {
        metrics.record_coalesced_ranges(ranges.len() as u64);
        let unique = requests
            .iter()
            .map(|request| request.descriptor.frame_ordinal)
            .collect::<std::collections::HashSet<_>>();
        metrics.record_singleflight(requests.len().saturating_sub(unique.len()) as u64);
        for range in &ranges {
            let class = range.size_class as usize;
            metrics.record_overscan(range.overscan_bytes(), class);
        }
    }
    let mut stream = futures_util::stream::iter(ranges.into_iter().map(|range| {
        let budget = Arc::clone(&budget);
        let runtime_metrics = runtime_metrics.clone();
        async move {
            let permits = u32::try_from(range.length).map_err(|_| {
                PackedWireError::LimitExceeded("coalesced range exceeds semaphore permits".into())
            })?;
            let range_permit = budget
                .ranges
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| PackedWireError::Backend("range semaphore closed".into()))?;
            let byte_permit = budget
                .bytes
                .clone()
                .acquire_many_owned(permits)
                .await
                .map_err(|_| PackedWireError::Backend("pipeline semaphore closed".into()))?;
            let frames = range
                .frames
                .iter()
                .map(|request| request.descriptor.clone())
                .collect::<Vec<_>>();
            if let Some(metrics) = &runtime_metrics {
                metrics.pipeline_acquire(range.length);
            }
            let payloads = object
                .read_frames_in_range(range.offset, range.length, &frames)
                .await;
            if let Some(metrics) = &runtime_metrics {
                metrics.pipeline_release(range.length);
            }
            let payloads = payloads?;
            if let Some(metrics) = &runtime_metrics {
                for frame in &frames {
                    metrics.record_frame(frame.size_class as usize, u64::from(frame.raw_len));
                }
            }
            Ok::<_, PackedWireError>((range, payloads, range_permit, byte_permit))
        }
    }))
    // All range futures are cheap descriptors waiting on the shared permits;
    // the permits, rather than this buffer size, define actual concurrency.
    .buffer_unordered(limits.max_inflight_ranges.max(1));

    let mut output = BTreeMap::new();
    while let Some(result) = stream.next().await {
        let (range, payloads, range_permit, byte_permit) = result?;
        for request in range.frames {
            let payload = payloads
                .get(&request.descriptor.frame_ordinal)
                .ok_or_else(|| {
                    PackedWireError::Invalid("streamed range is missing a requested frame".into())
                })?;
            output.insert(request.descriptor.frame_ordinal, payload.clone());
        }
        drop(byte_permit);
        drop(range_permit);
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coalescing_delay_stays_below_one_millisecond() {
        assert!(COALESCE_DELAY < Duration::from_millis(1));
    }

    fn request(ordinal: u32, offset: u64, size_class: SizeClass) -> FrameReadRequest {
        let raw = [ordinal as u8; 1024];
        let digest: [u8; 16] = Sha256::digest(raw)[..16].try_into().unwrap();
        FrameReadRequest {
            descriptor: PackedFrameDescriptor {
                frame_ordinal: ordinal,
                object_offset: offset,
                stored_len: raw.len() as u32,
                raw_len: raw.len() as u32,
                first_file_slot: ordinal,
                last_file_slot: ordinal,
                size_class,
                codec: 0,
                frame_digest: digest,
            },
            logical_len: 1024,
        }
    }

    #[test]
    fn planner_deduplicates_and_merges_same_class() {
        let ranges = coalesce_frame_ranges(
            AccessProfile::RandomSmallFile,
            vec![
                request(1, 1000, SizeClass::Tiny),
                request(0, 0, SizeClass::Tiny),
                request(1, 1000, SizeClass::Tiny),
            ],
            CoordinatorLimits {
                max_merge_gap: 0,
                max_coalesced_range: 4096,
                pipeline_bytes_budget: 4096,
                max_inflight_ranges: 1,
            },
        )
        .unwrap();
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].frames.len(), 2);
        assert_eq!(ranges[0].length, 2024);
    }

    #[test]
    fn planner_keeps_size_classes_separate() {
        let ranges = coalesce_frame_ranges(
            AccessProfile::SequentialSmallFile,
            vec![
                request(0, 0, SizeClass::Tiny),
                request(1, 1024, SizeClass::Small),
            ],
            CoordinatorLimits::default(),
        )
        .unwrap();
        assert_eq!(ranges.len(), 2);
    }

    #[test]
    fn planner_enforces_random_overscan() {
        let error = coalesce_frame_ranges(
            AccessProfile::RandomSmallFile,
            vec![
                request(0, 0, SizeClass::Tiny),
                request(1, 8000, SizeClass::Tiny),
            ],
            CoordinatorLimits {
                max_merge_gap: 8192,
                max_coalesced_range: 16 * 1024,
                pipeline_bytes_budget: 16 * 1024,
                max_inflight_ranges: 1,
            },
        )
        .unwrap();
        assert_eq!(error.len(), 2);
    }

    #[test]
    fn planner_splits_when_pipeline_budget_is_below_merge_limit() {
        let ranges = coalesce_frame_ranges(
            AccessProfile::RandomSmallFile,
            vec![
                request(0, 0, SizeClass::Tiny),
                request(1, 1024, SizeClass::Tiny),
                request(2, 2048, SizeClass::Tiny),
            ],
            CoordinatorLimits {
                max_merge_gap: 0,
                max_coalesced_range: 4096,
                pipeline_bytes_budget: 2048,
                max_inflight_ranges: 1,
            },
        )
        .unwrap();

        assert_eq!(ranges.len(), 2);
        assert_eq!(ranges[0].length, 2048);
        assert_eq!(ranges[1].length, 1024);
        assert!(ranges.iter().all(|range| range.length <= 2048));
    }

    #[test]
    fn planner_rejects_a_single_frame_over_pipeline_budget() {
        let error = coalesce_frame_ranges(
            AccessProfile::RandomSmallFile,
            [request(0, 0, SizeClass::Tiny)],
            CoordinatorLimits {
                max_merge_gap: 0,
                max_coalesced_range: 4096,
                pipeline_bytes_budget: 512,
                max_inflight_ranges: 1,
            },
        )
        .unwrap_err();

        assert!(matches!(error, PackedWireError::LimitExceeded(_)));
    }

    #[tokio::test]
    async fn shared_read_budget_caps_bytes_across_groups() {
        let limits = CoordinatorLimits {
            pipeline_bytes_budget: 2 * 1024,
            max_inflight_ranges: 4,
            ..CoordinatorLimits::default()
        };
        let budget = SharedReadBudget::new(limits).unwrap();
        let active_bytes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let peak_bytes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let tasks = (0..4).map(|_| {
            let budget = Arc::clone(&budget);
            let active_bytes = Arc::clone(&active_bytes);
            let peak_bytes = Arc::clone(&peak_bytes);
            tokio::spawn(async move {
                let range_permit = budget.ranges.clone().acquire_owned().await.unwrap();
                let byte_permit = budget.bytes.clone().acquire_many_owned(1024).await.unwrap();
                let active = active_bytes.fetch_add(1024, Ordering::AcqRel) + 1024;
                peak_bytes.fetch_max(active, Ordering::AcqRel);
                tokio::time::sleep(Duration::from_millis(5)).await;
                active_bytes.fetch_sub(1024, Ordering::AcqRel);
                drop(byte_permit);
                drop(range_permit);
            })
        });
        for task in tasks {
            task.await.unwrap();
        }
        assert!(peak_bytes.load(Ordering::Acquire) <= limits.pipeline_bytes_budget as usize);
        assert_eq!(active_bytes.load(Ordering::Acquire), 0);
    }
}
