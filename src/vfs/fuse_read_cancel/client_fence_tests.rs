//! Real registry transitions and requested-layout Roots owners. This module
//! does not simulate an adapter close or claim kernel/allocator acceptance.
use super::*;
use crate::workspace::packed::snapshot::{
    SnapshotBudgetPool, SnapshotMountBudget, SnapshotOwnedPermit,
};
use futures_util::FutureExt;
use std::sync::Barrier;

fn admit_owner(budget: &Arc<SnapshotMountBudget>, requested: u64) -> MetadataMemoryGuard {
    // Admit the permit's own Arc requested layout before allocating that Arc.
    let bytes = requested
        + (2 * std::mem::size_of::<usize>() + std::mem::size_of::<SnapshotOwnedPermit>()) as u64;
    let permit = budget.admit(&[(SnapshotBudgetPool::Roots, bytes)]).unwrap();
    Arc::new(permit)
}

fn fixture() -> (Arc<SnapshotMountBudget>, ReadRegistry) {
    let budget = SnapshotMountBudget::defaults();
    let owner = admit_owner(&budget, ReadRegistry::roots_requested_layout_bytes());
    let registry = ReadRegistry::new_owned(Some(owner));
    (budget, registry)
}

fn client_owner(budget: &Arc<SnapshotMountBudget>) -> MetadataMemoryGuard {
    admit_owner(budget, ReadRegistry::client_requested_layout_bytes())
}

fn assert_client(registry: &ReadRegistry, count: usize, phase: ClientPhase) {
    let state = registry.requests.lock().unwrap();
    assert_eq!(state.live_clients, count);
    assert!(
        state
            .clients
            .as_ref()
            .is_some_and(|lease| lease.phase == phase)
    );
}

#[test]
fn g07_registry_same_lock_open_race_is_rejected_or_retained_until_rollback() {
    let (budget, registry) = fixture();
    let idle = budget.state().used;
    {
        let owner = client_owner(&budget);
        let charged = budget.state().used;
        let barrier = Barrier::new(2);
        let registry_ref = &registry;
        let barrier_ref = &barrier;
        let (opened, closing) = std::thread::scope(|scope| {
            let open = scope.spawn(move || {
                barrier_ref.wait();
                registry_ref.begin_client_open(91, ClientKind::File, 901, Some(owner))
            });
            let close = scope.spawn(move || {
                barrier_ref.wait();
                registry_ref.prepare_unmount().now_or_never()
            });
            let closing = close.join().unwrap();
            (open.join().unwrap(), closing)
        });
        assert!(registry.requests.lock().unwrap().closed);
        match opened {
            Ok(mut pending) => {
                assert_eq!(closing, None);
                assert_client(&registry, 1, ClientPhase::Opening);
                assert_eq!(budget.state().used, charged);
                assert_eq!(pending.commit(71), Ok(false));
                assert_client(&registry, 1, ClientPhase::Rollback);
                assert_eq!(budget.state().used, charged);
                pending.finish_rollback().unwrap();
            }
            Err(error) => {
                assert_eq!(error, Errno::from(libc::ENODEV));
                assert_eq!(closing, Some(Ok(())));
                let state = registry.requests.lock().unwrap();
                assert_eq!(state.live_clients, 0);
                assert!(state.clients.is_none());
            }
        }
    }
    assert_eq!(registry.prepare_unmount().now_or_never(), Some(Ok(())));
    assert_eq!(budget.state().used, idle);
    drop(registry);
    assert_eq!(budget.state().used, [0; 8]);
}

#[tokio::test]
async fn g07_registry_closing_commit_retains_original_prepare_and_rollback_owner() {
    let (budget, registry) = fixture();
    let idle = budget.state().used;
    let mut pending = registry
        .begin_client_open(92, ClientKind::Directory, 902, Some(client_owner(&budget)))
        .unwrap();
    let held = budget.state().used;
    let prepare = registry.prepare_unmount();
    tokio::pin!(prepare);
    assert_eq!(prepare.as_mut().now_or_never(), None);
    assert_eq!(pending.commit(72), Ok(false));
    assert_client(&registry, 1, ClientPhase::Rollback);
    assert_eq!(budget.state().used, held);
    assert_eq!(prepare.as_mut().now_or_never(), None);
    pending.finish_rollback().unwrap();
    assert_eq!(budget.state().used, idle);
    prepare.await.unwrap();
}

#[test]
fn g07_registry_release_checks_inode_kind_and_duplicate_without_retiring_owner() {
    let (budget, registry) = fixture();
    let idle = budget.state().used;
    let mut pending = registry
        .begin_client_open(93, ClientKind::Stats, 903, Some(client_owner(&budget)))
        .unwrap();
    assert_eq!(pending.commit(73), Ok(true));
    drop(pending);
    let held = budget.state().used;
    for (ino, fh, kind) in [
        (94, 73, ClientKind::Stats),
        (93, 73, ClientKind::File),
        (93, 73, ClientKind::Directory),
        (93, 0, ClientKind::Stats),
        (93, 74, ClientKind::Stats),
    ] {
        assert_eq!(
            registry.begin_client_release(ino, fh, kind).err(),
            Some(libc::EBADF.into())
        );
        assert_client(&registry, 1, ClientPhase::Live);
        assert_eq!(budget.state().used, held);
    }
    let release = registry
        .begin_client_release(93, 73, ClientKind::Stats)
        .unwrap();
    assert_client(&registry, 1, ClientPhase::Releasing);
    assert_eq!(
        registry
            .begin_client_release(93, 73, ClientKind::Stats)
            .err(),
        Some(libc::EBADF.into())
    );
    assert_eq!(budget.state().used, held);
    assert_eq!(registry.prepare_unmount().now_or_never(), None);
    release.finish().unwrap();
    assert_eq!(budget.state().used, idle);
    assert_eq!(
        registry
            .begin_client_release(93, 73, ClientKind::Stats)
            .err(),
        Some(libc::EBADF.into())
    );
    assert_eq!(registry.prepare_unmount().now_or_never(), Some(Ok(())));
}

#[test]
fn g07_registry_dropped_open_sticks_first_eio_and_preserves_all_unresolved_roots() {
    let (budget, registry) = fixture();
    let first = registry
        .begin_client_open(94, ClientKind::File, 904, Some(client_owner(&budget)))
        .unwrap();
    let later = registry
        .begin_client_open(95, ClientKind::Directory, 905, Some(client_owner(&budget)))
        .unwrap();
    let held = budget.state().used;
    drop(first);
    later.fail(libc::EPIPE.into());
    assert_client(&registry, 2, ClientPhase::Opening);
    assert_eq!(
        registry.requests.lock().unwrap().client_failure,
        Some(libc::EIO.into())
    );
    assert_eq!(budget.state().used, held);
    assert_eq!(
        registry.prepare_unmount().now_or_never(),
        Some(Err(libc::EIO.into()))
    );
    assert_eq!(budget.state().used, held);
    drop(registry);
    assert_eq!(budget.state().used, [0; 8]);
}

#[test]
fn g07_registry_actual_release_failure_retains_first_errno_node_and_roots() {
    let (budget, registry) = fixture();
    let mut pending = registry
        .begin_client_open(96, ClientKind::File, 906, Some(client_owner(&budget)))
        .unwrap();
    assert_eq!(pending.commit(76), Ok(true));
    drop(pending);
    let later = registry
        .begin_client_open(97, ClientKind::Stats, 907, Some(client_owner(&budget)))
        .unwrap();
    let held = budget.state().used;
    let release = registry
        .begin_client_release(96, 76, ClientKind::File)
        .unwrap();
    release.fail(libc::EPIPE.into());
    drop(later); // fallback EIO must not erase the actual close failure.
    assert_eq!(registry.requests.lock().unwrap().live_clients, 2);
    assert_eq!(
        registry.requests.lock().unwrap().client_failure,
        Some(libc::EPIPE.into())
    );
    assert_eq!(budget.state().used, held);
    assert_eq!(
        registry.prepare_unmount().now_or_never(),
        Some(Err(libc::EPIPE.into()))
    );
    assert_eq!(budget.state().used, held);
    drop(registry);
    assert_eq!(budget.state().used, [0; 8]);
}

#[test]
fn g07_registry_zero_or_duplicate_commit_sticks_eoverflow_before_drop_fallback() {
    for collide in [false, true] {
        let (budget, registry) = fixture();
        if collide {
            let mut live = registry
                .begin_client_open(98, ClientKind::File, 908, Some(client_owner(&budget)))
                .unwrap();
            assert_eq!(live.commit(78), Ok(true));
        }
        let mut pending = registry
            .begin_client_open(99, ClientKind::Directory, 909, Some(client_owner(&budget)))
            .unwrap();
        let held = budget.state().used;
        assert_eq!(
            pending.commit(if collide { 78 } else { 0 }),
            Err(libc::EOVERFLOW.into())
        );
        drop(pending);
        assert_eq!(
            registry.requests.lock().unwrap().client_failure,
            Some(libc::EOVERFLOW.into())
        );
        assert_eq!(budget.state().used, held);
        assert_eq!(
            registry.prepare_unmount().now_or_never(),
            Some(Err(libc::EOVERFLOW.into()))
        );
        assert_eq!(budget.state().used, held);
        drop(registry);
        assert_eq!(budget.state().used, [0; 8]);
    }
}

#[test]
fn g07_registry_failed_retirement_records_ebadf_before_drop_fallback() {
    let (budget, registry) = fixture();
    let mut pending = registry
        .begin_client_open(100, ClientKind::File, 910, Some(client_owner(&budget)))
        .unwrap();
    let held = budget.state().used;
    // Deliberate private-state fault, exercising the real retirement error
    // path without adding a production hook or claiming an adapter failure.
    pending.generation += 1;
    assert_eq!(pending.finish_without_handle(), Err(libc::EBADF.into()));
    assert_eq!(registry.requests.lock().unwrap().live_clients, 1);
    assert_eq!(budget.state().used, held);
    assert_eq!(
        registry.prepare_unmount().now_or_never(),
        Some(Err(libc::EBADF.into()))
    );
}

#[test]
fn g07_registry_commit_failure_wakes_already_pending_prepare_before_rollback() {
    use futures_util::task::{ArcWake, waker};
    use std::future::Future;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll};

    struct WakeCount(AtomicUsize);
    impl ArcWake for WakeCount {
        fn wake_by_ref(owner: &Arc<Self>) {
            owner.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    let (budget, registry) = fixture();
    let mut pending = registry
        .begin_client_open(101, ClientKind::File, 911, Some(client_owner(&budget)))
        .unwrap();
    let held = budget.state().used;
    let wakes = Arc::new(WakeCount(AtomicUsize::new(0)));
    let task_waker = waker(wakes.clone());
    let mut context = Context::from_waker(&task_waker);
    let mut prepare = std::pin::pin!(registry.prepare_unmount());
    assert_eq!(prepare.as_mut().poll(&mut context), Poll::Pending);
    assert_eq!(wakes.0.load(Ordering::SeqCst), 0);
    assert_eq!(pending.commit(0), Err(libc::EOVERFLOW.into()));
    assert!(
        wakes.0.load(Ordering::SeqCst) > 0,
        "commit error did not wake typed preparation"
    );
    assert_eq!(budget.state().used, held);
    assert_eq!(registry.requests.lock().unwrap().live_clients, 1);
    assert_eq!(
        prepare.as_mut().poll(&mut context),
        Poll::Ready(Err(libc::EOVERFLOW.into()))
    );
    drop(pending); // EIO fallback still cannot overwrite the actual first error.
    assert_eq!(
        registry.requests.lock().unwrap().client_failure,
        Some(libc::EOVERFLOW.into())
    );
    assert_eq!(budget.state().used, held);
}
