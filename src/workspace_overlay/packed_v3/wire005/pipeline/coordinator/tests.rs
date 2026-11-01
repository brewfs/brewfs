use super::*;
use crate::cadapter::client::ObjectByteStream;
use crate::cadapter::read_observer::{Engine, Origin, Phase, ReadClass, ReadObserver};
use crate::workspace_overlay::packed_v3::wire005::{V3ObjectKind, encode_v3_object};
use bytes::Bytes;
use futures_util::{Stream, StreamExt, stream};
use sha2::{Digest, Sha256};
use std::pin::Pin;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::task::{Context, Poll};

#[derive(Default)]
struct Probe {
    mode: AtomicU8,
    calls: Mutex<Vec<(String, u64, u64)>>,
    initial_chunk: Notify,
    release: Notify,
    cancelled_bodies: AtomicUsize,
}
struct TrackedBody {
    inner: ObjectByteStream,
    probe: Arc<Probe>,
    eof: bool,
}
impl Stream for TrackedBody {
    type Item = anyhow::Result<Bytes>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let next = self.inner.as_mut().poll_next(cx);
        if matches!(next, Poll::Ready(None)) {
            self.eof = true;
        }
        next
    }
}
impl Drop for TrackedBody {
    fn drop(&mut self) {
        if !self.eof {
            self.probe.cancelled_bodies.fetch_add(1, Ordering::SeqCst);
        }
    }
}
#[derive(Clone)]
struct Backend {
    data: Bytes,
    probe: Arc<Probe>,
}
#[async_trait::async_trait]
impl ObjectBackend for Backend {
    async fn put_object(&self, _: &str, _: &[u8]) -> anyhow::Result<()> {
        unreachable!()
    }
    async fn get_object(&self, _: &str) -> anyhow::Result<Option<Vec<u8>>> {
        panic!("full GET is forbidden")
    }
    async fn get_object_range(&self, _: &str, _: u64, _: &mut [u8]) -> anyhow::Result<usize> {
        panic!("actual body stream required")
    }
    async fn get_object_range_stream(
        &self,
        key: &str,
        offset: u64,
        length: u64,
    ) -> anyhow::Result<ObjectByteStream> {
        self.probe
            .calls
            .lock()
            .unwrap()
            .push((key.into(), offset, length));
        let mode = self.probe.mode.load(Ordering::SeqCst);
        if mode == 6 {
            panic!("controlled provider collector panic");
        }
        let mut bytes = self.data.slice(offset as usize..(offset + length) as usize);
        if mode == 4 {
            bytes = bytes.slice(..bytes.len() - 1);
        }
        if mode == 5 {
            let mut changed = bytes.to_vec();
            changed[0] ^= 1;
            bytes = Bytes::from(changed);
        }
        let probe = self.probe.clone();
        let head = bytes.slice(..3);
        let rest = bytes.slice(3..);
        let first_probe = probe.clone();
        let first = stream::once(async move {
            first_probe.initial_chunk.notify_one();
            Ok(head)
        });
        let last = stream::once(async move {
            if mode == 1 {
                probe.release.notified().await;
            }
            Ok(rest)
        });
        let tail = stream::iter(match mode {
            2 => vec![Err(anyhow::anyhow!(
                "terminal body error after exact length"
            ))],
            3 => vec![Ok(Bytes::from_static(b"x"))],
            _ => vec![],
        });
        Ok(Box::pin(TrackedBody {
            inner: Box::pin(first.chain(last).chain(tail)),
            probe: self.probe.clone(),
            eof: false,
        }))
    }
    async fn get_etag(&self, _: &str) -> anyhow::Result<String> {
        unreachable!()
    }
    async fn delete_object(&self, _: &str) -> anyhow::Result<()> {
        unreachable!()
    }
}
fn fixture() -> (Backend, V3FrameDemand, V3FrameDemand) {
    let first = [1u8; 64];
    let second = [2u8; 64];
    let mut body = first.to_vec();
    body.extend([0u8; 32]);
    body.extend(second);
    let data = encode_v3_object(V3ObjectKind::GroupContainer, &body, 1024).unwrap();
    let container = V3ObjectRef::from_bytes(
        "first-container".into(),
        V3ObjectKind::GroupContainer,
        &data,
    )
    .unwrap();
    let demand = |ordinal: u32, offset: u64, raw: &[u8]| V3FrameDemand {
        generation: ReadGeneration::readonly([7; 32]),
        context: ReadContext {
            engine: Engine::Native,
            phase: Phase::Runtime,
            origin: Origin::Demand,
            class: ReadClass::PackedPayload,
        },
        container: container.clone(),
        profile: AccessProfile::RandomSmallFile,
        size_classes: SizeClassTable::default(),
        frame_policy: Default::default(),
        descriptor: PackedFrameDescriptor {
            frame_ordinal: ordinal,
            object_offset: offset,
            stored_len: 64,
            raw_len: 64,
            first_file_slot: ordinal,
            last_file_slot: ordinal,
            size_class: SizeClass::Tiny,
            codec: 0,
            frame_digest: Sha256::digest(raw)[..16].try_into().unwrap(),
        },
        raw_offset: 0,
        logical_length: 64,
    };
    let a = demand(0, 4096, &first);
    let b = demand(1, 4192, &second);
    (
        Backend {
            data: Bytes::from(data),
            probe: Arc::new(Probe::default()),
        },
        a,
        b,
    )
}
fn coordinator(
    backend: &Backend,
    budget: Arc<V3MountBudget>,
    observer: Arc<ReadObserver>,
) -> Arc<V3DemandCoordinator<Backend>> {
    coordinator_with_memo(backend, budget, observer, true)
}
fn coordinator_with_memo(
    backend: &Backend,
    budget: Arc<V3MountBudget>,
    observer: Arc<ReadObserver>,
    enabled: bool,
) -> Arc<V3DemandCoordinator<Backend>> {
    V3DemandCoordinator::new_configured(
        ObjectClient::new(backend.clone()).with_read_observer(
            observer,
            Engine::Native,
            Phase::Runtime,
            Origin::Demand,
        ),
        budget,
        V3PipelineLimits::default(),
        enabled,
    )
    .unwrap()
}
async fn wait_for_cleanup(budget: &Arc<V3MountBudget>) {
    tokio::time::timeout(Duration::from_secs(1), async {
        while budget.state().used != [0; 8] {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn verified_frame_memo_uses_metadata_budget_without_consuming_root_headroom() {
    for enabled in [false, true] {
        let (backend, demand, _) = fixture();
        let budget = V3MountBudget::defaults();
        let limits = V3PipelineLimits::default();
        let coordinator_roots = queue_roots_bytes(limits) + registry_roots_bytes::<Frame>(limits);
        // Runtime index structures can fill Roots after mount startup. A memo
        // must not take their remaining space or raise the configured budget.
        let held = budget
            .admit(&[(
                V3BudgetPool::Roots,
                budget.capacity(V3BudgetPool::Roots) - coordinator_roots,
            )])
            .unwrap();
        let pipeline = coordinator_with_memo(
            &backend,
            budget.clone(),
            Arc::new(ReadObserver::default()),
            enabled,
        );
        assert_eq!(
            budget.state().used[V3BudgetPool::Roots as usize],
            budget.capacity(V3BudgetPool::Roots)
        );
        assert_eq!(
            budget.state().used[V3BudgetPool::Metadata as usize],
            if enabled {
                super::super::verify_memo::RESERVATION_BYTES
            } else {
                0
            }
        );
        let waiter = pipeline.submit(&demand).unwrap();
        assert_eq!(waiter.wait().await.unwrap().raw, [1; 64]);
        drop(waiter);
        pipeline.shutdown().await;
        drop(pipeline);
        drop(held);
        wait_for_cleanup(&budget).await;
    }
}

#[tokio::test]
async fn enabled_verify_memo_retains_zero_payload_bytes_after_consumers_drop() {
    let (backend, first, second) = fixture();
    let budget = V3MountBudget::defaults();
    let observer = Arc::new(ReadObserver::default());
    let pipeline = coordinator_with_memo(&backend, budget.clone(), observer.clone(), true);
    let idle = budget.state().used;
    assert_eq!(
        idle[V3BudgetPool::Metadata as usize],
        super::super::verify_memo::RESERVATION_BYTES
    );
    assert!(idle[V3BudgetPool::Metadata as usize] > 0);
    assert!(idle[V3BudgetPool::Metadata as usize] <= 2 << 20);
    for (index, demand) in [&first, &second, &first, &second].into_iter().enumerate() {
        let waiter = pipeline.submit(demand).unwrap();
        let frame = waiter.wait().await.unwrap();
        assert_eq!(frame.raw, [1 + demand.descriptor.frame_ordinal as u8; 64]);
        assert_eq!(budget.state().used[V3BudgetPool::Raw as usize], 64);
        let released_frame = Arc::downgrade(&frame);
        drop(frame);
        drop(waiter);
        tokio::time::timeout(Duration::from_secs(1), async {
            while budget.state().used != idle {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("completed read retained payload or unaccounted metadata");
        assert!(released_frame.upgrade().is_none(), "memo retained a frame");
        for pool in [
            V3BudgetPool::Stored,
            V3BudgetPool::Raw,
            V3BudgetPool::Workspace,
            V3BudgetPool::Output,
        ] {
            assert_eq!(budget.state().used[pool as usize], 0, "{pool:?}");
        }
        assert_eq!(backend.probe.calls.lock().unwrap().len(), index + 1);
    }
    let snapshot = observer.snapshot();
    assert_eq!(snapshot.work[&(first.context, ReadWork::Decode)].count, 4);
    assert_eq!(
        snapshot.work[&(first.context, ReadWork::Authentication)].count,
        2,
        "the repeated reads must actually hit the enabled memo"
    );
    pipeline.shutdown().await;
    drop(pipeline);
    wait_for_cleanup(&budget).await;
}

#[tokio::test]
async fn verified_frame_memo_repeats_get_and_decode_but_not_authentication() {
    let (backend, demand, _) = fixture();
    let budget = V3MountBudget::defaults();
    let observer = Arc::new(ReadObserver::default());
    // Also serves as the negative control when this one test is explicitly
    // invoked with BREWFS_PACKED_VERIFY_MEMO=0: two hashes must then be seen.
    let pipeline = coordinator_with_memo(
        &backend,
        budget.clone(),
        observer.clone(),
        VerifyMemo::enabled_from_env().unwrap(),
    );
    for _ in 0..2 {
        let waiter = pipeline.submit(&demand).unwrap();
        let frame = waiter.wait().await.unwrap();
        assert_eq!(frame.raw, [1; 64]);
        drop(frame);
        drop(waiter);
    }
    assert_eq!(backend.probe.calls.lock().unwrap().len(), 2);
    let snapshot = observer.snapshot();
    assert_eq!(snapshot.work[&(demand.context, ReadWork::Decode)].count, 2);
    assert_eq!(
        snapshot.work[&(demand.context, ReadWork::Authentication)].count,
        1
    );
    assert_eq!(budget.state().used[V3BudgetPool::Raw as usize], 0);
    pipeline.shutdown().await;
    drop(pipeline);
    wait_for_cleanup(&budget).await;
}

#[tokio::test]
async fn verified_frame_memo_never_remembers_corrupt_first_responses() {
    for enabled in [false, true] {
        let (backend, demand, _) = fixture();
        let budget = V3MountBudget::defaults();
        let observer = Arc::new(ReadObserver::default());
        let pipeline = coordinator_with_memo(&backend, budget.clone(), observer.clone(), enabled);
        backend.probe.mode.store(5, Ordering::SeqCst);
        for _ in 0..2 {
            let waiter = pipeline.submit(&demand).unwrap();
            assert!(matches!(
                waiter.wait().await.unwrap_err().as_ref(),
                PackedWireError::HashMismatch { .. }
            ));
        }
        backend.probe.mode.store(0, Ordering::SeqCst);
        for _ in 0..2 {
            let waiter = pipeline.submit(&demand).unwrap();
            assert_eq!(waiter.wait().await.unwrap().raw, [1; 64]);
        }
        assert_eq!(backend.probe.calls.lock().unwrap().len(), 4);
        assert_eq!(
            observer.snapshot().work[&(demand.context, ReadWork::Authentication)].count,
            if enabled { 3 } else { 4 }
        );
        pipeline.shutdown().await;
        drop(pipeline);
        wait_for_cleanup(&budget).await;
    }
}

#[tokio::test]
async fn verified_frame_memo_hits_still_reject_terminal_short_and_excess_bodies() {
    let (backend, demand, _) = fixture();
    let budget = V3MountBudget::defaults();
    let observer = Arc::new(ReadObserver::default());
    let pipeline = coordinator(&backend, budget.clone(), observer.clone());
    {
        let waiter = pipeline.submit(&demand).unwrap();
        assert_eq!(waiter.wait().await.unwrap().raw, [1; 64]);
    }
    for mode in [2, 3, 4] {
        backend.probe.mode.store(mode, Ordering::SeqCst);
        let waiter = pipeline.submit(&demand).unwrap();
        assert!(waiter.wait().await.is_err(), "body mode {mode}");
    }
    let mut invalid = demand.clone();
    invalid.logical_length = 65;
    assert!(pipeline.submit(&invalid).is_err());
    backend.probe.mode.store(0, Ordering::SeqCst);
    let mut invalid_codec_body = demand.clone();
    invalid_codec_body.descriptor.codec = 1; // zstd: these raw bytes are not a valid frame.
    {
        let waiter = pipeline.submit(&invalid_codec_body).unwrap();
        assert!(waiter.wait().await.is_err());
    }
    assert_eq!(backend.probe.calls.lock().unwrap().len(), 5);
    assert_eq!(
        observer.snapshot().work[&(demand.context, ReadWork::Authentication)].count,
        1
    );
    pipeline.shutdown().await;
    drop(pipeline);
    wait_for_cleanup(&budget).await;
}

#[tokio::test]
async fn verified_frame_memo_does_not_transfer_evidence_to_changed_identities() {
    let (backend, demand, _) = fixture();
    let budget = V3MountBudget::defaults();
    let observer = Arc::new(ReadObserver::default());
    let pipeline = coordinator(&backend, budget.clone(), observer.clone());
    {
        let waiter = pipeline.submit(&demand).unwrap();
        assert_eq!(waiter.wait().await.unwrap().raw, [1; 64]);
    }
    // Same corrupt response, but each authenticated identity component must
    // cause a miss even though the original frame was successfully verified.
    backend.probe.mode.store(5, Ordering::SeqCst);
    for variant in 0..6 {
        let mut changed = demand.clone();
        match variant {
            0 => changed.container.digest[0] ^= 1,
            1 => changed.descriptor.frame_ordinal += 1,
            2 => changed.descriptor.frame_digest[0] ^= 1,
            3 => changed.descriptor.object_offset += 1,
            5 => changed.container.key = "another-physical-object".into(),
            _ => {
                changed.descriptor.stored_len -= 1;
                changed.descriptor.raw_len -= 1;
                changed.logical_length -= 1;
            }
        }
        let waiter = pipeline.submit(&changed).unwrap();
        assert!(
            matches!(
                waiter.wait().await.unwrap_err().as_ref(),
                PackedWireError::HashMismatch { .. }
            ),
            "identity variant {variant}"
        );
    }
    assert_eq!(
        observer.snapshot().work[&(demand.context, ReadWork::Authentication)].count,
        7
    );
    pipeline.shutdown().await;
    drop(pipeline);
    wait_for_cleanup(&budget).await;
}

#[tokio::test]
async fn verified_frame_memo_requires_immutable_responses_and_off_rechecks_each_response() {
    for enabled in [false, true] {
        let (backend, demand, _) = fixture();
        let budget = V3MountBudget::defaults();
        let pipeline = coordinator_with_memo(
            &backend,
            budget.clone(),
            Arc::new(ReadObserver::default()),
            enabled,
        );
        {
            let waiter = pipeline.submit(&demand).unwrap();
            assert_eq!(waiter.wait().await.unwrap().raw, [1; 64]);
        }
        // Explicitly document the trust boundary: a name/digest does not stop
        // a backend from violating the immutable-object contract after a hit.
        backend.probe.mode.store(5, Ordering::SeqCst);
        let waiter = pipeline.submit(&demand).unwrap();
        if enabled {
            let mut changed = [1; 64];
            changed[0] ^= 1;
            assert_eq!(waiter.wait().await.unwrap().raw, changed);
        } else {
            assert!(matches!(
                waiter.wait().await.unwrap_err().as_ref(),
                PackedWireError::HashMismatch { .. }
            ));
        }
        drop(waiter);
        pipeline.shutdown().await;
        drop(pipeline);
        wait_for_cleanup(&budget).await;
    }
}

#[tokio::test]
async fn captured_demand_phase_survives_mount_transition_for_body_raw_and_events() {
    let (backend, mut demand, _) = fixture();
    let budget = V3MountBudget::defaults();
    let observer = Arc::new(ReadObserver::default());
    let phase = Arc::new(AtomicU8::new(0));
    let client = ObjectClient::new(backend.clone())
        .with_read_observer(
            observer.clone(),
            Engine::Native,
            Phase::Startup,
            Origin::Demand,
        )
        .with_phase_control(phase.clone());
    demand.context = client.read_context(ReadClass::PackedPayload).unwrap();
    let pipeline =
        V3DemandCoordinator::new(client, budget.clone(), V3PipelineLimits::default()).unwrap();
    // This deterministic ordering exercises a transition between immutable
    // context capture and submission, without relying on a scheduler race.
    phase.store(1, Ordering::Release);
    let mut wrong = demand.clone();
    wrong.context.engine = Engine::PackedV3;
    assert!(pipeline.submit(&wrong).is_err());
    wrong = demand.clone();
    wrong.context.origin = Origin::Prefetch;
    assert!(pipeline.submit(&wrong).is_err());
    let first = pipeline.submit(&demand).unwrap();
    let second = pipeline.submit(&demand).unwrap();
    let first_frame = tokio::time::timeout(Duration::from_secs(1), first.wait())
        .await
        .unwrap()
        .unwrap();
    let second_frame = tokio::time::timeout(Duration::from_secs(1), second.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(Arc::ptr_eq(&first_frame, &second_frame));
    assert_eq!(first_frame.raw, [1; 64]);
    drop(first_frame);
    drop(second_frame);
    drop(first);
    drop(second);
    pipeline.shutdown().await;
    drop(pipeline);
    wait_for_cleanup(&budget).await;
    let snapshot = observer.snapshot();
    assert_eq!(
        snapshot.rows[&(
            crate::cadapter::read_observer::Ledger::BackendBody,
            demand.context,
        )]
            .success,
        1
    );
    assert_eq!(snapshot.raw[&demand.context].decoded_raw, 64);
    assert_eq!(
        snapshot.events[&(demand.context, ReadEvent::FetchLeader)],
        1
    );
    assert_eq!(
        snapshot.events[&(demand.context, ReadEvent::SharedResultAfterMiss)],
        1
    );
    assert!(
        snapshot
            .rows
            .keys()
            .all(|(_, context)| context.phase == Phase::Startup)
    );
    assert!(
        snapshot
            .raw
            .keys()
            .all(|context| context.phase == Phase::Startup)
    );
    assert!(
        snapshot
            .events
            .keys()
            .all(|(context, _)| context.phase == Phase::Startup)
    );
    assert_eq!(backend.probe.calls.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn actual_same_frame_fetch_survives_first_waiter_cancel_and_raw_permit_final_owner() {
    let (backend, mut a, _) = fixture();
    backend.probe.mode.store(1, Ordering::SeqCst);
    let budget = V3MountBudget::defaults();
    let observer = Arc::new(ReadObserver::default());
    let pipeline = coordinator(&backend, budget.clone(), observer.clone());
    a.logical_length = 32;
    let first = pipeline.submit(&a).unwrap();
    a.raw_offset = 16;
    a.logical_length = 48;
    let second = pipeline.submit(&a).unwrap();
    backend.probe.initial_chunk.notified().await;
    assert_eq!(backend.probe.calls.lock().unwrap().len(), 1);
    drop(first);
    assert_eq!(backend.probe.cancelled_bodies.load(Ordering::SeqCst), 0);
    backend.probe.release.notify_waiters();
    let frame = tokio::time::timeout(Duration::from_secs(1), second.wait())
        .await
        .unwrap()
        .unwrap();
    drop(second);
    assert_eq!(frame.raw, [1; 64]);
    assert_eq!(budget.state().used[V3BudgetPool::Raw as usize], 64);
    let guard = observer.start(
        crate::cadapter::read_observer::Ledger::LogicalOperation,
        a.context,
        48,
    );
    let tracking = frame.coverage.as_ref().unwrap();
    let bytes = SharedRawCoverage::required_receipt_bytes(1).unwrap();
    let receipt = tracking
        .attach(
            &guard.delivery_token().unwrap(),
            &[(16, 48)],
            bytes,
            Box::new(budget.admit(&[(V3BudgetPool::Control, bytes)]).unwrap()),
        )
        .unwrap();
    receipt.copied(16, 48).unwrap();
    guard.deliver(48);
    pipeline.shutdown().await;
    drop(pipeline);
    assert_eq!(budget.state().used[V3BudgetPool::Raw as usize], 64);
    drop(frame);
    drop(receipt);
    wait_for_cleanup(&budget).await;
    let raw = observer.snapshot().raw[&a.context];
    assert_eq!(
        (
            raw.decoded_raw,
            raw.requested_union,
            raw.copied_union,
            raw.delivered_union
        ),
        (64, 64, 48, 48)
    );
}

#[tokio::test]
async fn same_frame_singleflight_is_unique_for_every_supported_profile() {
    for profile in [
        AccessProfile::RandomSmallFile,
        AccessProfile::SequentialSmallFile,
        AccessProfile::Mixed,
    ] {
        let (backend, mut demand, _) = fixture();
        demand.profile = profile;
        let budget = V3MountBudget::defaults();
        let pipeline = coordinator(&backend, budget.clone(), Arc::new(ReadObserver::default()));

        let first = pipeline.submit(&demand).unwrap();
        let second = pipeline.submit(&demand).unwrap();
        let first_frame = tokio::time::timeout(Duration::from_secs(1), first.wait())
            .await
            .unwrap()
            .unwrap();
        let second_frame = tokio::time::timeout(Duration::from_secs(1), second.wait())
            .await
            .unwrap()
            .unwrap();

        assert!(Arc::ptr_eq(&first_frame, &second_frame));
        assert_eq!(first_frame.raw, [1; 64]);
        assert_eq!(backend.probe.calls.lock().unwrap().len(), 1);

        drop(first_frame);
        drop(second_frame);
        drop(first);
        drop(second);
        pipeline.shutdown().await;
        drop(pipeline);
        wait_for_cleanup(&budget).await;
    }
}

#[tokio::test]
async fn actual_demand_coalescing_uses_one_range_and_last_flight_cancels_its_body() {
    let (backend, a, b) = fixture();
    backend.probe.mode.store(1, Ordering::SeqCst);
    let budget = V3MountBudget::defaults();
    let observer = Arc::new(ReadObserver::default());
    let pipeline = coordinator(&backend, budget.clone(), observer.clone());
    let first = pipeline.submit(&a).unwrap();
    let second = pipeline.submit(&b).unwrap();
    backend.probe.initial_chunk.notified().await;
    assert_eq!(
        *backend.probe.calls.lock().unwrap(),
        vec![(a.container.key.clone(), 4096, 160)]
    );
    drop(first);
    tokio::task::yield_now().await;
    assert_eq!(backend.probe.cancelled_bodies.load(Ordering::SeqCst), 0);
    drop(second);
    tokio::time::timeout(Duration::from_secs(1), async {
        while backend.probe.cancelled_bodies.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(budget.state().used[V3BudgetPool::Stored as usize], 0);
    let snapshot = observer.snapshot();
    for ledger in [
        crate::cadapter::read_observer::Ledger::BackendBody,
        crate::cadapter::read_observer::Ledger::ValidatedFetch,
    ] {
        let row = &snapshot.rows[&(ledger, a.context)];
        assert_eq!((row.cancelled, row.received_cancelled), (1, 3));
    }
    pipeline.shutdown().await;
    drop(pipeline);
    wait_for_cleanup(&budget).await;
}

#[tokio::test]
async fn actual_eof_tail_short_corruption_fail_all_followers_and_next_call_fetches_again() {
    for mode in [2, 3, 4, 5] {
        let (backend, a, b) = fixture();
        backend.probe.mode.store(mode, Ordering::SeqCst);
        let budget = V3MountBudget::defaults();
        let pipeline = coordinator(&backend, budget.clone(), Arc::new(ReadObserver::default()));
        let first = pipeline.submit(&a).unwrap();
        let follower = pipeline.submit(&a).unwrap();
        let second = pipeline.submit(&b).unwrap();
        assert!(first.wait().await.is_err());
        assert!(follower.wait().await.is_err());
        assert!(second.wait().await.is_err());
        assert_eq!(backend.probe.calls.lock().unwrap().len(), 1);
        assert_eq!(budget.state().used[V3BudgetPool::Raw as usize], 0);
        drop(first);
        drop(follower);
        drop(second);
        backend.probe.mode.store(0, Ordering::SeqCst);
        let next = pipeline.submit(&a).unwrap();
        let frame = next.wait().await.unwrap();
        assert_eq!(frame.raw, [1; 64]);
        assert_eq!(backend.probe.calls.lock().unwrap().len(), 2);
        drop(frame);
        drop(next);
        pipeline.shutdown().await;
        drop(pipeline);
        wait_for_cleanup(&budget).await;
    }
}

#[tokio::test]
async fn multiple_container_bodies_are_separate_and_shutdown_waits_for_worker_release() {
    let (backend, a, mut b) = fixture();
    b.container.key = "second-container".into();
    let budget = V3MountBudget::defaults();
    let pipeline = coordinator(&backend, budget.clone(), Arc::new(ReadObserver::default()));
    let first = pipeline.submit(&a).unwrap();
    let second = pipeline.submit(&b).unwrap();
    let a_frame = first.wait().await.unwrap();
    let b_frame = second.wait().await.unwrap();
    assert_eq!(backend.probe.calls.lock().unwrap().len(), 2);
    drop(first);
    drop(second);
    drop(a_frame);
    drop(b_frame);
    backend.probe.mode.store(1, Ordering::SeqCst);
    let _ = futures_util::FutureExt::now_or_never(backend.probe.initial_chunk.notified());
    let pending = pipeline.submit(&a).unwrap();
    backend.probe.initial_chunk.notified().await;
    pipeline.shutdown().await;
    assert!(pending.wait().await.is_err());
    assert!(pipeline.submit(&a).is_err());
    drop(pending);
    drop(pipeline);
    wait_for_cleanup(&budget).await;
}

#[tokio::test]
async fn pending_body_does_not_block_a_later_independent_collection() {
    let (backend, a, mut b) = fixture();
    b.container.key = "second-container".into();
    backend.probe.mode.store(1, Ordering::SeqCst);
    let budget = V3MountBudget::defaults();
    let pipeline = coordinator(&backend, budget.clone(), Arc::new(ReadObserver::default()));
    let first = pipeline.submit(&a).unwrap();
    tokio::time::timeout(
        Duration::from_secs(1),
        backend.probe.initial_chunk.notified(),
    )
    .await
    .expect("first actual body did not become pending");
    let second = pipeline.submit(&b).unwrap();
    tokio::time::timeout(
        Duration::from_secs(1),
        backend.probe.initial_chunk.notified(),
    )
    .await
    .expect("pending body blocked a later independent collection");
    assert_eq!(backend.probe.calls.lock().unwrap().len(), 2);
    assert_eq!(backend.probe.cancelled_bodies.load(Ordering::SeqCst), 0);
    pipeline.shutdown().await;
    assert_eq!(backend.probe.cancelled_bodies.load(Ordering::SeqCst), 2);
    assert!(first.wait().await.is_err());
    assert!(second.wait().await.is_err());
    drop(first);
    drop(second);
    drop(pipeline);
    assert_eq!(budget.state().used, [0; 8]);
}

#[tokio::test]
async fn raw_capacity_reduces_batch_and_slow_consumers_backpressure_the_next_body() {
    let (backend, a, b) = fixture();
    let mut limits = super::super::super::V3BudgetLimits::default();
    limits.bytes[V3BudgetPool::Raw as usize] = 64;
    let budget = V3MountBudget::new(limits).unwrap();
    let pipeline = coordinator(&backend, budget.clone(), Arc::new(ReadObserver::default()));
    let first = pipeline.submit(&a).unwrap();
    let second = pipeline.submit(&b).unwrap();
    let frame = first.wait().await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(2), second.wait())
            .await
            .is_err()
    );
    assert_eq!(
        *backend.probe.calls.lock().unwrap(),
        vec![(a.container.key.clone(), 4096, 64)]
    );
    drop(first);
    drop(frame);
    let frame = tokio::time::timeout(Duration::from_secs(1), second.wait())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(frame.raw, [2; 64]);
    drop(second);
    drop(frame);
    pipeline.shutdown().await;
    drop(pipeline);
    wait_for_cleanup(&budget).await;
}

#[tokio::test]
async fn cancellation_while_waiting_for_raw_releases_job_without_opening_a_body() {
    let (backend, a, _) = fixture();
    let budget = V3MountBudget::defaults();
    let held = budget
        .admit(&[(V3BudgetPool::Raw, budget.capacity(V3BudgetPool::Raw))])
        .unwrap();
    let pipeline = coordinator(&backend, budget.clone(), Arc::new(ReadObserver::default()));
    let waiter = pipeline.submit(&a).unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while budget.state().rejections == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    drop(waiter);
    tokio::time::timeout(Duration::from_secs(1), async {
        while budget.state().used[V3BudgetPool::Control as usize] != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(backend.probe.calls.lock().unwrap().is_empty());
    drop(held);
    pipeline.shutdown().await;
    drop(pipeline);
    wait_for_cleanup(&budget).await;
}

#[tokio::test]
async fn workspace_dependency_failure_is_typed_and_never_waits_for_the_current_read() {
    let (backend, a, _) = fixture();
    let budget = V3MountBudget::defaults();
    let held = budget
        .admit(&[(
            V3BudgetPool::Workspace,
            budget.capacity(V3BudgetPool::Workspace),
        )])
        .unwrap();
    let pipeline = coordinator(&backend, budget.clone(), Arc::new(ReadObserver::default()));
    let waiter = pipeline.submit(&a).unwrap();
    let error = tokio::time::timeout(Duration::from_secs(1), waiter.wait())
        .await
        .unwrap()
        .unwrap_err();
    assert!(matches!(*error, PackedWireError::LimitExceeded(_)));
    assert!(backend.probe.calls.lock().unwrap().is_empty());
    drop(waiter);
    drop(held);
    pipeline.shutdown().await;
    drop(pipeline);
    wait_for_cleanup(&budget).await;
}

#[tokio::test]
async fn owned_demand_admission_refuses_before_running_the_allocation_factory() {
    let (backend, demand, _) = fixture();
    let budget = V3MountBudget::defaults();
    let held = budget
        .admit(&[(V3BudgetPool::Plans, budget.capacity(V3BudgetPool::Plans))])
        .unwrap();
    let pipeline = coordinator(&backend, budget.clone(), Arc::new(ReadObserver::default()));
    let made = AtomicUsize::new(0);
    let result = pipeline.submit_with(demand.container.key.len(), || {
        made.fetch_add(1, Ordering::SeqCst);
        demand.clone()
    });
    assert!(matches!(result, Err(PackedWireError::LimitExceeded(_))));
    assert_eq!(made.load(Ordering::SeqCst), 0);
    assert!(backend.probe.calls.lock().unwrap().is_empty());
    assert_eq!(
        budget.state().used[V3BudgetPool::Plans as usize],
        budget.capacity(V3BudgetPool::Plans)
    );
    drop(held);
    pipeline.shutdown().await;
    drop(pipeline);
    wait_for_cleanup(&budget).await;
}

#[tokio::test]
async fn owned_demand_job_releases_its_owner_only_after_pending_body_cancellation() {
    let (backend, demand, _) = fixture();
    backend.probe.mode.store(1, Ordering::SeqCst);
    let budget = V3MountBudget::defaults();
    let pipeline = coordinator(&backend, budget.clone(), Arc::new(ReadObserver::default()));
    let made = AtomicUsize::new(0);
    let waiter = pipeline
        .submit_with(demand.container.key.len(), || {
            made.fetch_add(1, Ordering::SeqCst);
            demand.clone()
        })
        .unwrap();
    tokio::time::timeout(
        Duration::from_secs(1),
        backend.probe.initial_chunk.notified(),
    )
    .await
    .unwrap();
    assert_eq!(made.load(Ordering::SeqCst), 1);
    assert!(budget.state().used[V3BudgetPool::Plans as usize] > 0);
    drop(waiter);
    tokio::time::timeout(Duration::from_secs(1), async {
        while budget.state().used[V3BudgetPool::Control as usize] != 0
            || budget.state().used[V3BudgetPool::Plans as usize] != 0
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("moved recipe or waiter retained its owner after body cancellation");
    assert_eq!(backend.probe.cancelled_bodies.load(Ordering::SeqCst), 1);
    pipeline.shutdown().await;
    drop(pipeline);
    wait_for_cleanup(&budget).await;
}

#[tokio::test]
async fn minimum_control_serves_shared_maximum_key_after_the_first_consumer_cancels() {
    let (backend, mut demand, _) = fixture();
    demand.container.key = "a".repeat(4096);
    backend.probe.mode.store(1, Ordering::SeqCst);
    let mut limits = super::super::super::V3BudgetLimits::default();
    limits.bytes[V3BudgetPool::Control as usize] = 32 << 10;
    let budget = V3MountBudget::new(limits).unwrap();
    budget.validate_frame_capability(8 << 20).unwrap();
    // Supplemental coordinator isolation: the unchanged adapter/raw Session
    // fixtures must separately acquire these owners through their real APIs.
    let request_owners = budget.admit(&[(V3BudgetPool::Control, 26664)]).unwrap();
    let pipeline = coordinator(&backend, budget.clone(), Arc::new(ReadObserver::default()));
    let first = pipeline
        .submit_with(demand.container.key.len(), || demand.clone())
        .unwrap();
    tokio::time::timeout(
        Duration::from_secs(1),
        backend.probe.initial_chunk.notified(),
    )
    .await
    .unwrap();
    let follower = pipeline
        .submit_with(demand.container.key.len(), || demand.clone())
        .unwrap();
    drop(first);
    assert!(budget.state().used[V3BudgetPool::Plans as usize] >= 4096);
    assert_eq!(backend.probe.cancelled_bodies.load(Ordering::SeqCst), 0);
    backend.probe.release.notify_one();
    let frame = tokio::time::timeout(Duration::from_secs(1), follower.wait())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(frame.raw, [1; 64]);
    assert_eq!(
        *backend.probe.calls.lock().unwrap(),
        vec![(demand.container.key.clone(), 4096, 64)]
    );
    drop(follower);
    pipeline.shutdown().await;
    drop(pipeline);
    assert_eq!(budget.state().used[V3BudgetPool::Plans as usize], 0);
    assert_eq!(
        budget.state().used[V3BudgetPool::Control as usize],
        26664 + 512
    );
    assert!(budget.state().peak[V3BudgetPool::Control as usize] <= 32 << 10);
    drop(frame);
    drop(request_owners);
    wait_for_cleanup(&budget).await;
}

// Existing API: real coordinator worker and a real pending response Body.
// Cancellation loses only this waiter, never the original actor handle.
#[tokio::test(flavor = "current_thread")]
async fn current_shutdown_waiter_cancel_keeps_actual_provider_handle_until_resumed_join() {
    use std::future::Future;

    let (backend, demand, _) = fixture();
    backend.probe.mode.store(1, Ordering::SeqCst);
    let budget = V3MountBudget::defaults();
    let pipeline = coordinator(&backend, budget.clone(), Arc::new(ReadObserver::default()));
    let pending = pipeline.submit(&demand).unwrap();
    tokio::time::timeout(
        Duration::from_secs(1),
        backend.probe.initial_chunk.notified(),
    )
    .await
    .expect("real provider response body was not polled");
    let original_id = pipeline.worker.lock().unwrap().as_ref().unwrap().id();
    let waker = futures_util::task::noop_waker();
    let mut cx = Context::from_waker(&waker);

    // No await/yield between stop and the captured ownership facts. On the
    // single executor thread abort cannot run the actor's Drop here.
    let mut first = Box::pin(pipeline.shutdown());
    let first_pending = matches!(first.as_mut().poll(&mut cx), Poll::Pending);
    drop(first);
    let retained_id = pipeline
        .worker
        .lock()
        .unwrap()
        .as_ref()
        .map(|handle| handle.id());
    let body_still_owned = backend.probe.cancelled_bodies.load(Ordering::SeqCst) == 0;
    let completion_not_published = !pipeline.completion.finished.load(Ordering::Acquire);

    let mut resumed = Box::pin(pipeline.shutdown());
    let resumed_pending = matches!(resumed.as_mut().poll(&mut cx), Poll::Pending);
    drop(resumed);

    // Real cleanup runs before any RED assertion. Completion flags, byte zero
    // and this later cleanup never overwrite the earlier captured handle loss.
    tokio::time::timeout(Duration::from_secs(1), pipeline.shutdown())
        .await
        .expect("fixture provider cleanup stalled");
    let handle_consumed_after_resume = pipeline.worker.lock().unwrap().is_none();
    let actual_body_dropped = backend.probe.cancelled_bodies.load(Ordering::SeqCst) == 1;
    assert!(pending.wait().await.is_err());
    drop(pending);
    drop(pipeline);
    wait_for_cleanup(&budget).await;

    assert!(
        first_pending,
        "fixture must reach an actual Pending provider join"
    );
    assert!(
        body_still_owned && completion_not_published,
        "fixture must retain the actual pending body"
    );
    assert_eq!(
        retained_id,
        Some(original_id),
        "cancelled shutdown waiter must retain the same actual provider JoinHandle"
    );
    assert!(
        resumed_pending,
        "resumed shutdown must wait for the original provider actor"
    );
    assert!(
        handle_consumed_after_resume && actual_body_dropped,
        "actual resumed join and body Drop must precede handle retirement"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn current_shutdown_records_provider_collector_join_error_before_handle_retirement() {
    let (backend, demand, _) = fixture();
    backend.probe.mode.store(6, Ordering::SeqCst);
    let budget = V3MountBudget::defaults();
    let pipeline = coordinator(&backend, budget.clone(), Arc::new(ReadObserver::default()));
    let pending = pipeline.submit(&demand).unwrap();

    tokio::time::timeout(Duration::from_secs(1), async {
        while backend.probe.calls.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("provider collector did not enter the panic fixture");
    tokio::task::yield_now().await;

    pipeline.shutdown().await;
    let join_error = pipeline
        .completion
        .join_error
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone();
    assert!(
        join_error
            .as_deref()
            .is_some_and(|error| error.contains("provider collector join")),
        "collector JoinError was not retained: {join_error:?}"
    );
    assert!(pending.wait().await.is_err());
    drop(pending);
    drop(pipeline);
    wait_for_cleanup(&budget).await;
}
