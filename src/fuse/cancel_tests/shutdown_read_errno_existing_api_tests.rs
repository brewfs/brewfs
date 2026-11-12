//! Existing v3 adapters: terminal mount shutdown versus per-request interrupt.
//! LocalFS fixture; no Python retry, kernel reply, or live FUSE proof.
use super::*;

fn shutdown_errno_limits() -> SnapshotBudgetLimits {
    let mut limits = SnapshotBudgetLimits::default();
    limits.bytes[SnapshotBudgetPool::Control as usize] = 32 << 10;
    limits.bytes[SnapshotBudgetPool::Metadata as usize] = 64 << 20;
    limits
}

#[tokio::test]
async fn g07_existing_api_global_prepare_returns_terminal_eio_for_pending_read() {
    let budget = SnapshotMountBudget::new(shutdown_errno_limits()).unwrap();
    budget.validate_frame_capability(8 << 20).unwrap();
    let f = fixture_with_budget(budget).await;
    f.gate.mode.store(1, Ordering::SeqCst);
    let owner = Filesystem::reserve_request_memory(&f.fs, 40)
        .unwrap()
        .unwrap();
    let fs = f.fs.clone();
    let ino = f.ino;
    let reader = tokio::spawn(async move {
        let result = Filesystem::read(&fs, request(1901), ino, 0, 0, 8192).await;
        drop(owner);
        result
    });
    tokio::time::timeout(Duration::from_secs(2), f.gate.entered.notified())
        .await
        .expect("real payload body did not become pending");
    assert_eq!(f.fs.open_file_handle_count(), 1);
    assert_eq!(f.gate.payload_ranges.lock().unwrap().len(), 1);
    assert_eq!(f.gate.dropped.load(Ordering::SeqCst), 0);
    tokio::time::timeout(Duration::from_secs(2), Filesystem::prepare_unmount(&f.fs))
        .await
        .expect("global preparation did not retire the actual read owners")
        .expect("global preparation returned failure");
    let cancelled = tokio::time::timeout(Duration::from_secs(2), reader)
        .await
        .expect("globally cancelled read did not return")
        .unwrap();
    assert_eq!(f.gate.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(f.fs.open_file_handle_count(), 0);
    assert_cancelled_read(&f.observer);
    assert_eq!(
        f.budget.state().used[SnapshotBudgetPool::Control as usize],
        0
    );
    assert_eq!(
        f.budget.state().used[SnapshotBudgetPool::Output as usize],
        0
    );
    assert!(f.budget.state().peak[SnapshotBudgetPool::Control as usize] <= 32768);
    // Shutdown is terminal. EINTR invites Python's installed pread retry loop
    // to issue another read while the mount is already closing or unmounted.
    assert_eq!(cancelled.err(), Some(Errno::from(libc::EIO)));
}

#[tokio::test]
async fn g07_existing_api_closed_read_admission_returns_terminal_eio() {
    let budget = SnapshotMountBudget::new(shutdown_errno_limits()).unwrap();
    budget.validate_frame_capability(8 << 20).unwrap();
    let f = fixture_with_budget(budget).await;
    tokio::time::timeout(Duration::from_secs(2), Filesystem::prepare_unmount(&f.fs))
        .await
        .expect("idle preparation did not finish")
        .expect("global preparation returned failure");
    let owner = Filesystem::reserve_request_memory(&f.fs, 40)
        .unwrap()
        .unwrap();
    let rejected = Filesystem::read(&f.fs, request(1911), f.ino, 0, 0, 8192).await;
    drop(owner);
    assert_eq!(f.fs.open_file_handle_count(), 0);
    assert_eq!(f.gate.dropped.load(Ordering::SeqCst), 0);
    assert!(f.gate.payload_ranges.lock().unwrap().is_empty());
    assert_eq!(
        f.budget.state().used[SnapshotBudgetPool::Control as usize],
        0
    );
    assert!(f.budget.state().peak[SnapshotBudgetPool::Control as usize] <= 32768);
    // This is the READ admission policy. Closing OPEN may still use ENODEV;
    // do not add ENODEV to the real cancelled-reader helper's accepted errno.
    assert_eq!(rejected.err(), Some(Errno::from(libc::EIO)));
}

#[tokio::test]
async fn g07_existing_api_explicit_interrupt_keeps_eintr_and_next_read_recovers() {
    let budget = SnapshotMountBudget::new(shutdown_errno_limits()).unwrap();
    budget.validate_frame_capability(8 << 20).unwrap();
    let f = fixture_with_budget(budget).await;
    f.gate.mode.store(1, Ordering::SeqCst);
    let owner = Filesystem::reserve_request_memory(&f.fs, 40)
        .unwrap()
        .unwrap();
    let fs = f.fs.clone();
    let ino = f.ino;
    let reader = tokio::spawn(async move {
        let result = Filesystem::read(&fs, request(1921), ino, 0, 0, 8192).await;
        drop(owner);
        result
    });
    tokio::time::timeout(Duration::from_secs(2), f.gate.entered.notified())
        .await
        .expect("real payload body did not become pending");
    assert_eq!(f.fs.open_file_handle_count(), 1);
    let interrupt_owner = Filesystem::reserve_control_memory(&f.fs, 1024)
        .unwrap()
        .unwrap();
    Filesystem::interrupt(&f.fs, request(1922), 1921)
        .await
        .unwrap();
    drop(interrupt_owner);
    let cancelled = tokio::time::timeout(Duration::from_secs(2), reader)
        .await
        .expect("explicit interrupt did not cancel the original unique")
        .unwrap();
    assert_eq!(cancelled.err(), Some(Errno::from(libc::EINTR)));
    assert_eq!(f.gate.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(f.fs.open_file_handle_count(), 0);
    assert_cancelled_read(&f.observer);
    f.gate.mode.store(0, Ordering::SeqCst);
    let owner = Filesystem::reserve_request_memory(&f.fs, 40)
        .unwrap()
        .unwrap();
    let reply = Filesystem::read(&f.fs, request(1923), f.ino, 0, 0, 8192)
        .await
        .expect("explicit interrupt incorrectly closed mount-wide read admission");
    assert_eq!(reply.data.as_ref(), &[0x75; 8192]);
    drop(reply);
    drop(owner);
    assert_eq!(f.fs.open_file_handle_count(), 0);
    assert_eq!(
        f.budget.state().used[SnapshotBudgetPool::Control as usize],
        0
    );
    assert_eq!(
        f.budget.state().used[SnapshotBudgetPool::Output as usize],
        0
    );
    assert!(f.budget.state().peak[SnapshotBudgetPool::Control as usize] <= 32768);
    assert!(
        f.observer
            .snapshot()
            .rows
            .values()
            .all(|row| row.conserved())
    );
}
