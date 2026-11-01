//! A single mount-owned worker collects only submitted demands and consumes
//! each actual response EOF before authenticating/publishing any shared frame.

use super::verify_memo::{FrameIdentity, VerifyMemo};
use super::*;
use crate::cadapter::client::{ObjectBackend, ObjectClient};
use crate::cadapter::read_observer::{ReadEvent, ReadWork, SharedRawCoverage};
use futures_util::{StreamExt, stream::FuturesUnordered};
#[cfg(test)]
mod tests;

#[derive(Debug)]
pub(crate) struct V3SharedFrame {
    pub raw: Vec<u8>,
    pub coverage: Option<Arc<SharedRawCoverage>>,
    _control: V3OwnedPermit,
}
type Frame = V3Owned<V3SharedFrame>;
struct Job {
    demand: V3FrameDemand,
    ticket: V3FrameLeader<Frame>,
    _recipe_owner: V3OwnedPermit,
}
pub(crate) fn queue_roots_bytes(limits: V3PipelineLimits) -> u64 {
    65536
        + limits.max_pending_frames as u64 * (std::mem::size_of::<Job>() as u64 * 4 + 256)
        + limits.max_inflight_collections as u64 * 8192
}
#[derive(Default)]
struct WorkerCompletion {
    finished: std::sync::atomic::AtomicBool,
    changed: Notify,
    join_error: Mutex<Option<String>>,
}
impl WorkerCompletion {
    fn record_join_error(&self, error: tokio::task::JoinError) {
        let mut slot = self.join_error.lock().unwrap_or_else(|e| e.into_inner());
        *slot = Some(format!("provider collector join: {error}"));
    }
}
// The original actor join waker belongs to the persistent completion owner,
// never to a cancellable shutdown caller. Every registered waiter is notified.
impl futures_util::task::ArcWake for WorkerCompletion {
    fn wake_by_ref(owner: &Arc<Self>) {
        owner.changed.notify_waiters();
    }
}
struct CompletionGuard(Arc<WorkerCompletion>);
impl Drop for CompletionGuard {
    fn drop(&mut self) {
        self.0
            .finished
            .store(true, std::sync::atomic::Ordering::Release);
        self.0.changed.notify_waiters();
    }
}
pub(crate) struct V3DemandCoordinator<B: ObjectBackend + Clone> {
    client: ObjectClient<B>,
    budget: Arc<V3MountBudget>,
    registry: V3FlightRegistry<Frame>,
    sender: tokio::sync::mpsc::Sender<Job>,
    worker: Mutex<Option<tokio::task::JoinHandle<()>>>,
    worker_abort: tokio::task::AbortHandle,
    completion: Arc<WorkerCompletion>,
    _queue_roots: Arc<V3OwnedPermit>,
}
impl<B: ObjectBackend + Clone + 'static> V3DemandCoordinator<B> {
    pub(crate) fn new(
        client: ObjectClient<B>,
        budget: Arc<V3MountBudget>,
        limits: V3PipelineLimits,
    ) -> PackedResult<Arc<Self>> {
        Self::new_configured(client, budget, limits, VerifyMemo::enabled_from_env()?)
    }

    fn new_configured(
        client: ObjectClient<B>,
        budget: Arc<V3MountBudget>,
        limits: V3PipelineLimits,
        verify_memo_enabled: bool,
    ) -> PackedResult<Arc<Self>> {
        let limits = limits.validate()?;
        // Identity/timestamp metadata is paid from Metadata, never a payload
        // pool. Roots must retain its headroom for runtime index structures.
        // The worker keeps this reservation until its memo is dropped.
        let memo_bytes = if verify_memo_enabled {
            super::verify_memo::RESERVATION_BYTES
        } else {
            0
        };
        let roots = budget.admit(&[
            (V3BudgetPool::Roots, queue_roots_bytes(limits)),
            (V3BudgetPool::Metadata, memo_bytes),
        ])?;
        let roots = Arc::new(roots);
        let memo = VerifyMemo::new(verify_memo_enabled);
        // Prepay every concurrently owned collection future, its linked node
        // and fixed body state. Dynamic recipes remain independently owned.
        let state_bytes = std::mem::size_of_val(&process_jobs::<B, true>(
            &client,
            &budget,
            &memo,
            limits,
            Vec::new(),
        ))
        .max(std::mem::size_of_val(&process_jobs::<B, false>(
            &client,
            &budget,
            &memo,
            limits,
            Vec::new(),
        ))) as u64;
        if state_bytes + 256 + 4096 > 8192 {
            return Err(limit("collection state exceeds its mount reservation"));
        }
        // mpsc slots, the collection Vec/BTreeMap and demand/key clones are
        // bounded and allocated only after this mount lifetime reservation.
        let registry = V3FlightRegistry::new(budget.clone(), limits)?;
        let (sender, receiver) = tokio::sync::mpsc::channel(limits.max_pending_frames);
        let worker_client = client.clone();
        let worker_budget = budget.clone();
        let worker_roots = roots.clone();
        let completion = Arc::new(WorkerCompletion::default());
        let completion_guard = CompletionGuard(completion.clone());
        let handle = tokio::spawn(async move {
            let _completion = completion_guard;
            let _queue_roots = worker_roots;
            // Select once per worker. The disabled specialization eliminates
            // identity construction, LRU access and memo branches per frame.
            if verify_memo_enabled {
                collect::<B, true>(worker_client, worker_budget, memo, limits, receiver).await;
            } else {
                collect::<B, false>(worker_client, worker_budget, memo, limits, receiver).await;
            }
        });
        Ok(Arc::new(Self {
            client,
            budget,
            registry,
            sender,
            worker_abort: handle.abort_handle(),
            worker: Mutex::new(Some(handle)),
            completion,
            _queue_roots: roots,
        }))
    }
    pub(crate) fn submit(&self, demand: &V3FrameDemand) -> PackedResult<V3FrameWaiter<Frame>> {
        self.validate_context(demand)?;
        let (waiter, leader) = self.registry.begin(demand)?;
        if let Some(ticket) = leader {
            let owner = self.reserve_job_recipe(demand.container.key.len())?;
            self.enqueue(demand.clone(), ticket, owner)?;
        } else {
            self.record_follower(demand);
        }
        Ok(waiter)
    }

    /// Reserve before constructing a demand, then move its container key into
    /// the job. The caller does not create a second, separately owned clone.
    pub(crate) fn submit_with(
        &self,
        key_len: usize,
        make_demand: impl FnOnce() -> V3FrameDemand,
    ) -> PackedResult<V3FrameWaiter<Frame>> {
        if key_len == 0 || key_len > 4096 {
            return Err(invalid("submitted container key length is invalid"));
        }
        let owner = self.reserve_job_recipe(key_len)?;
        let demand = make_demand();
        if demand.container.key.len() != key_len {
            return Err(invalid("submitted key length differs from reserved demand"));
        }
        self.validate_context(&demand)?;
        let (waiter, leader) = self.registry.begin(&demand)?;
        if let Some(ticket) = leader {
            self.enqueue(demand, ticket, owner)?;
        } else {
            self.record_follower(&demand);
        }
        Ok(waiter)
    }

    fn validate_context(&self, demand: &V3FrameDemand) -> PackedResult<()> {
        if self
            .client
            .read_context(demand.context.class)
            .is_some_and(|context| {
                context.engine != demand.context.engine || context.origin != demand.context.origin
            })
        {
            return Err(invalid(
                "submitted demand attribution differs from mount client",
            ));
        }
        Ok(())
    }

    fn reserve_job_recipe(&self, key_len: usize) -> PackedResult<V3OwnedPermit> {
        let bytes = (key_len as u64)
            .checked_mul(3)
            .and_then(|bytes| bytes.checked_add(2048))
            .ok_or_else(|| limit("demand recipe ownership overflows"))?;
        // Fixed Job slots already belong to queue Roots. This independent
        // immutable recipe owns its String and execution clones until the Job
        // retires, even if the first request/plan has already been cancelled.
        self.budget.admit(&[(V3BudgetPool::Plans, bytes)])
    }

    fn enqueue(
        &self,
        demand: V3FrameDemand,
        ticket: V3FrameLeader<Frame>,
        owner: V3OwnedPermit,
    ) -> PackedResult<()> {
        // The phase was captured with the immutable demand. The mount may
        // transition before submit or during collection.
        if let Some(observer) = self.client.read_observer() {
            observer.event(demand.context, ReadEvent::FetchLeader);
        }
        self.sender
            .try_send(Job {
                demand,
                ticket,
                _recipe_owner: owner,
            })
            .map_err(|_| limit("mount demand collection queue is unavailable"))
    }

    fn record_follower(&self, demand: &V3FrameDemand) {
        if let Some(observer) = self.client.read_observer() {
            observer.event(demand.context, ReadEvent::SharedResultAfterMiss);
        }
    }
    pub(crate) async fn shutdown(&self) {
        self.registry.shutdown();
        self.worker_abort.abort();
        loop {
            // Register every join waiter before polling the one original slot.
            // Its mutex is held only for a single poll, never across an await.
            let changed = self.completion.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let joined = futures_util::future::poll_fn(|cx| {
                let join_waker = futures_util::task::waker_ref(&self.completion);
                let mut join_cx = std::task::Context::from_waker(&join_waker);
                let mut slot = self.worker.lock().unwrap_or_else(|e| e.into_inner());
                let ready = match slot.as_mut() {
                    None => true,
                    Some(handle) => {
                        match std::future::Future::poll(std::pin::Pin::new(handle), &mut join_cx) {
                            std::task::Poll::Pending => false,
                            std::task::Poll::Ready(result) => {
                                if let Err(error) = result
                                    && !error.is_cancelled()
                                {
                                    self.completion.record_join_error(error);
                                }
                                drop(slot.take());
                                true
                            }
                        }
                    }
                };
                drop(slot);
                if ready {
                    // Wake every concurrent caller when the one original
                    // slot is consumed. The actor's Ready wake uses the same
                    // persistent broadcaster even if its last caller cancels.
                    self.completion.changed.notify_waiters();
                    return std::task::Poll::Ready(true);
                }
                match std::future::Future::poll(changed.as_mut(), cx) {
                    std::task::Poll::Ready(()) => std::task::Poll::Ready(false),
                    std::task::Poll::Pending => std::task::Poll::Pending,
                }
            })
            .await;
            if joined {
                break;
            }
        }
        loop {
            let notified = self.completion.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self
                .completion
                .finished
                .load(std::sync::atomic::Ordering::Acquire)
            {
                break;
            }
            notified.await;
        }
    }
}
impl<B: ObjectBackend + Clone> Drop for V3DemandCoordinator<B> {
    fn drop(&mut self) {
        self.registry.shutdown();
        self.worker_abort.abort();
    }
}

async fn collect<B: ObjectBackend + Clone + 'static, const MEMO: bool>(
    client: ObjectClient<B>,
    budget: Arc<V3MountBudget>,
    memo: VerifyMemo,
    limits: V3PipelineLimits,
    mut receiver: tokio::sync::mpsc::Receiver<Job>,
) {
    // These futures are owned and polled by this worker. Aborting and joining
    // it drops every pending userspace body before completion is announced.
    let mut active = FuturesUnordered::new();
    loop {
        let first = tokio::select! {
            biased;
            Some(()) = active.next(), if !active.is_empty() => continue,
            job = receiver.recv(), if active.len() < limits.max_inflight_collections => {
                match job {
                    Some(job) => job,
                    None => break,
                }
            }
        };
        // Grow with actual submissions. Eight sparse collections must not
        // each retain a max_pending_frames-sized buffer outside Roots.
        let mut jobs = Vec::new();
        jobs.push(first);
        tokio::time::sleep(limits.collection_delay).await;
        while jobs.len() < limits.max_pending_frames {
            match receiver.try_recv() {
                Ok(job) => jobs.push(job),
                Err(_) => break,
            }
        }
        active.push(process_jobs::<B, MEMO>(
            &client, &budget, &memo, limits, jobs,
        ));
    }
    while active.next().await.is_some() {}
}

async fn process_jobs<B: ObjectBackend + Clone + 'static, const MEMO: bool>(
    client: &ObjectClient<B>,
    budget: &Arc<V3MountBudget>,
    memo: &VerifyMemo,
    limits: V3PipelineLimits,
    mut jobs: Vec<Job>,
) {
    jobs.retain(|job| job.ticket.live());
    if jobs.is_empty() {
        return;
    }
    let mut jobs = jobs
        .into_iter()
        .map(|job| (job.demand.key(), job))
        .collect::<BTreeMap<_, _>>();
    let demands = jobs
        .values()
        .map(|job| job.demand.clone())
        .collect::<Vec<_>>();
    let batches = plan_v3_weighted_batches(&demands, limits, budget, |d| {
        jobs.get(&d.key())
            .map(|j| j.ticket.contribution())
            .unwrap_or(0)
    });
    let batches = match batches {
        Ok(batches) => batches,
        Err(error) => {
            for (_, job) in jobs {
                job.ticket.complete(Err(error.clone()));
            }
            return;
        }
    };
    for batch in batches.iter() {
        // A valid coalesced range may still exceed aggregate raw/workspace
        // capacity. Reduce to individual frames instead of retaining a
        // partially admitted bundle while waiting for another resource.
        match batch.fits_capacity(budget) {
            Ok(true) => run_batch::<B, MEMO>(client, budget, memo, batch, &mut jobs).await,
            Ok(false) if batch.frames.len() > 1 => {
                for demand in &batch.frames {
                    let one = V3FrameBatch {
                        offset: demand.descriptor.object_offset,
                        length: u64::from(demand.descriptor.stored_len),
                        stored_frame_bytes: u64::from(demand.descriptor.stored_len),
                        logical_contribution_bytes: demand.logical_length,
                        frames: vec![demand.clone()],
                    };
                    match one.fits_capacity(budget) {
                        Ok(true) => {
                            run_batch::<B, MEMO>(client, budget, memo, &one, &mut jobs).await
                        }
                        Ok(false) => {
                            if let Some(job) = jobs.remove(&demand.key()) {
                                job.ticket
                                    .complete(Err(limit("one frame exceeds mount capacity")));
                            }
                        }
                        Err(error) => {
                            if let Some(job) = jobs.remove(&demand.key()) {
                                job.ticket.complete(Err(error));
                            }
                        }
                    }
                }
            }
            Ok(false) => {
                for d in &batch.frames {
                    if let Some(job) = jobs.remove(&d.key()) {
                        job.ticket
                            .complete(Err(limit("one frame exceeds mount capacity")));
                    }
                }
            }
            Err(error) => {
                for d in &batch.frames {
                    if let Some(job) = jobs.remove(&d.key()) {
                        job.ticket.complete(Err(error.clone()));
                    }
                }
            }
        }
    }
}

async fn run_batch<B: ObjectBackend + Clone + 'static, const MEMO: bool>(
    client: &ObjectClient<B>,
    budget: &Arc<V3MountBudget>,
    memo: &VerifyMemo,
    batch: &V3FrameBatch,
    jobs: &mut BTreeMap<V3FlightKey, Job>,
) {
    let body = V3BodyCancellation::new();
    let mut any = false;
    for demand in &batch.frames {
        if let Some(job) = jobs.get(&demand.key()) {
            match job.ticket.attach_body(&body) {
                Ok(attached) => any |= attached,
                Err(error) => {
                    body.cancel();
                    for d in &batch.frames {
                        if let Some(job) = jobs.remove(&d.key()) {
                            job.ticket.complete(Err(error.clone()));
                        }
                    }
                    return;
                }
            }
        }
    }
    body.seal();
    if !any {
        for demand in &batch.frames {
            if let Some(job) = jobs.remove(&demand.key()) {
                job.ticket.complete(Err((*cancelled()).clone()));
            }
        }
        return;
    }
    let client = client
        .clone()
        .with_fixed_read_context(batch.frames[0].context);
    let fetched = match batch.worker_admission_charges() {
        Err(error) => Err(error),
        Ok(charges) => {
            let admitted = tokio::select! {
                biased;
                _=body.cancelled()=>Err((*cancelled()).clone()),
                result=budget.admit_when_available(&charges)=>result,
            };
            match admitted {
                Err(error) => Err(error),
                Ok(permit) => tokio::select! {
                    biased;
                    _=body.cancelled()=>Err((*cancelled()).clone()),
                    result=fetch_batch::<B, MEMO>(&client,budget,memo,batch,permit,jobs)=>result,
                },
            }
        }
    };
    match fetched {
        Ok(frames) => {
            for (key, frame) in frames {
                if let Some(job) = jobs.remove(&key) {
                    job.ticket.complete(Ok(frame));
                }
            }
        }
        Err(error) => {
            for d in &batch.frames {
                if let Some(job) = jobs.remove(&d.key()) {
                    job.ticket.complete(Err(error.clone()));
                }
            }
        }
    }
}

async fn fetch_batch<B: ObjectBackend + Clone, const MEMO: bool>(
    client: &ObjectClient<B>,
    _budget: &Arc<V3MountBudget>,
    memo: &VerifyMemo,
    batch: &V3FrameBatch,
    mut permit: V3OwnedPermit,
    jobs: &BTreeMap<V3FlightKey, Job>,
) -> PackedResult<Vec<(V3FlightKey, Frame)>> {
    let first = &batch.frames[0];
    client
        .typed_exact(
            first.context.class,
            &first.container.key,
            batch.offset,
            batch.length,
            batch.length,
            |stored| {
                let mut frames = Vec::with_capacity(batch.frames.len());
                for demand in &batch.frames {
                    let f = &demand.descriptor;
                    let start = (f.object_offset - batch.offset) as usize;
                    let stop = start + f.stored_len as usize;
                    let bytes = stored.get(start..stop).ok_or_else(|| {
                        super::super::observer_validation_error(invalid(
                            "coalesced frame range exceeds body",
                        ))
                    })?;
                    let identity = MEMO.then(|| FrameIdentity::from_demand(demand));
                    let needs_verification = identity
                        .as_ref()
                        .is_none_or(|identity| !memo.contains_verified(identity));
                    if needs_verification {
                        let hash_timer =
                            client.measure_read_work(first.context.class, ReadWork::Authentication);
                        let digest = super::super::frame_directory::frame_digest(bytes);
                        drop(hash_timer);
                        if digest != f.frame_digest {
                            return Err(super::super::observer_validation_error(
                                PackedWireError::HashMismatch {
                                    what: "wire005 coalesced frame",
                                    expected: hex::encode(f.frame_digest),
                                    computed: hex::encode(digest),
                                },
                            ));
                        }
                    }
                    let decode_timer =
                        client.measure_read_work(first.context.class, ReadWork::Decode);
                    let raw = crate::workspace_overlay::packed_v3::codec::decode_block(
                        crate::workspace_overlay::packed_v3::PackedCodec::from_u8(f.codec)
                            .map_err(super::super::observer_validation_error)?,
                        bytes,
                        f.raw_len as usize,
                        8 << 20,
                        f.raw_len as usize,
                    )
                    .map_err(|error| {
                        (
                            crate::cadapter::read_observer::FailureClass::Decode,
                            error.into(),
                        )
                    })?;
                    drop(decode_timer);
                    if let Some(identity) = identity.filter(|_| needs_verification) {
                        memo.remember_verified(identity);
                    }
                    let raw_permit = permit
                        .split(V3BudgetPool::Raw, raw.capacity() as u64)
                        .map_err(super::super::observer_validation_error)?;
                    let coverage = if let Some(observer) = client.read_observer() {
                        let tracking =
                            SharedRawCoverage::required_tracking_bytes(u64::from(f.raw_len))
                                .map_err(|e| {
                                    super::super::observer_validation_error(limit(&e.to_string()))
                                })?;
                        let owner = permit
                            .split(V3BudgetPool::Workspace, tracking)
                            .map_err(super::super::observer_validation_error)?;
                        Some(
                            SharedRawCoverage::new(
                                observer,
                                demand.context,
                                u64::from(f.raw_len),
                                tracking,
                                Box::new(owner),
                            )
                            .map_err(|e| {
                                super::super::observer_validation_error(limit(&e.to_string()))
                            })?,
                        )
                    } else {
                        None
                    };
                    if let (Some(coverage), Some(job)) = (&coverage, jobs.get(&demand.key())) {
                        job.ticket
                            .record_requests(coverage)
                            .map_err(super::super::observer_validation_error)?;
                    }
                    let control = permit
                        .split(V3BudgetPool::Control, 512)
                        .map_err(super::super::observer_validation_error)?;
                    frames.push((
                        demand.key(),
                        V3Owned::new(
                            V3SharedFrame {
                                raw,
                                coverage,
                                _control: control,
                            },
                            raw_permit,
                        ),
                    ));
                }
                Ok(frames)
            },
        )
        .await
        .map_err(super::super::observer_backend_error)
}
