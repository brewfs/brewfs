//! Existing v3 production adapter APIs; LocalFS fixture, not a kernel/FUSE proof.
use super::*;
use std::future::Future;
use std::pin::Pin;
use std::task::Poll;

fn close_fence_limits() -> SnapshotBudgetLimits {
    let mut limits = SnapshotBudgetLimits::default();
    limits.bytes[SnapshotBudgetPool::Control as usize] = 32 << 10;
    limits.bytes[SnapshotBudgetPool::Metadata as usize] = 64 << 20;
    limits
}

// Poll the same preparation future. A negative check never uses a sleep or
// treats a timeout as evidence of a client close. Do not repoll after Ready.
async fn poll_once<F: Future + ?Sized>(mut future: Pin<&mut F>) -> Poll<F::Output> {
    std::future::poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx))).await
}

#[tokio::test]
async fn g07_existing_api_prepare_waits_for_file_dir_and_stats_release() {
    // Isolate each kind: a still-open stats client must not conceal a missing
    // file/directory lease. Each case keeps its own original prepare future.
    for kind in ["file", "dir", "stats"] {
        let budget = SnapshotMountBudget::new(close_fence_limits()).unwrap();
        budget.validate_frame_capability(8 << 20).unwrap();
        let f = fixture_with_budget(budget).await;
        assert!(f.fs.meta_layer().install_snapshot_stats(f.fs.stats()));
        let owner = Filesystem::reserve_request_memory(&f.fs, 8)
            .unwrap()
            .unwrap();
        let (ino, opened) = match kind {
            "file" => (
                f.ino,
                Filesystem::open(&f.fs, request(1801), f.ino, libc::O_RDONLY as u32)
                    .await
                    .unwrap(),
            ),
            "dir" => (
                1,
                Filesystem::opendir(&f.fs, request(1801), 1, libc::O_RDONLY as u32)
                    .await
                    .unwrap(),
            ),
            "stats" => (
                STATS_INODE,
                Filesystem::open(&f.fs, request(1801), STATS_INODE, libc::O_RDONLY as u32)
                    .await
                    .unwrap(),
            ),
            _ => unreachable!(),
        };
        drop(owner);
        assert_ne!(opened.fh, 0);
        assert_eq!(
            f.fs.open_file_handle_count(),
            if kind == "file" { 1 } else { 0 }
        );
        if kind == "stats" {
            assert!(f.fs.virtual_stats_snapshot(opened.fh).is_some());
        }
        let prepare = Filesystem::prepare_unmount(&f.fs);
        tokio::pin!(prepare);
        assert!(
            poll_once(prepare.as_mut()).await.is_pending(),
            "preparation completed before real {kind} client RELEASE"
        );
        // Call the real close adapter. Session close queue/reply accounting is
        // a separate gate; an ordinary request owner cannot replace that lane.
        if kind == "dir" {
            Filesystem::releasedir(&f.fs, request(1802), ino, opened.fh, 0)
                .await
                .unwrap();
        } else {
            Filesystem::release(&f.fs, request(1802), ino, opened.fh, 0, 0, false)
                .await
                .unwrap();
        }
        assert_eq!(f.fs.open_file_handle_count(), 0);
        assert!(f.fs.virtual_stats_snapshot(opened.fh).is_none());
        tokio::time::timeout(Duration::from_secs(2), prepare.as_mut())
            .await
            .expect("preparation did not finish after the matching real RELEASE completed")
            .expect("client preparation returned failure");
        assert_eq!(
            f.budget.state().used[SnapshotBudgetPool::Control as usize],
            0
        );
        assert!(f.budget.state().peak[SnapshotBudgetPool::Control as usize] <= 32768);
        assert_eq!(f.budget.capacity(SnapshotBudgetPool::Metadata), 64 << 20);
    }
}

#[tokio::test]
async fn g07_existing_api_closing_rejects_new_file_dir_and_stats_open() {
    let budget = SnapshotMountBudget::new(close_fence_limits()).unwrap();
    budget.validate_frame_capability(8 << 20).unwrap();
    let f = fixture_with_budget(budget).await;
    assert!(f.fs.meta_layer().install_snapshot_stats(f.fs.stats()));
    let owner = Filesystem::reserve_request_memory(&f.fs, 8)
        .unwrap()
        .unwrap();
    let held = Filesystem::open(&f.fs, request(1811), f.ino, libc::O_RDONLY as u32)
        .await
        .unwrap();
    drop(owner);
    assert_ne!(held.fh, 0);
    let prepare = Filesystem::prepare_unmount(&f.fs);
    tokio::pin!(prepare);
    // Drive the existing API into its closing path even on the current source,
    // which completes too early. This test must fail on OPEN behavior itself.
    let already_ready = match poll_once(prepare.as_mut()).await {
        std::task::Poll::Ready(result) => {
            result.expect("preparation returned failure");
            true
        }
        std::task::Poll::Pending => false,
    };
    let owner = Filesystem::reserve_request_memory(&f.fs, 8)
        .unwrap()
        .unwrap();
    let new_file = Filesystem::open(&f.fs, request(1812), f.ino, libc::O_RDONLY as u32).await;
    drop(owner);
    let owner = Filesystem::reserve_request_memory(&f.fs, 8)
        .unwrap()
        .unwrap();
    let new_dir = Filesystem::opendir(&f.fs, request(1813), 1, libc::O_RDONLY as u32).await;
    drop(owner);
    let owner = Filesystem::reserve_request_memory(&f.fs, 8)
        .unwrap()
        .unwrap();
    let new_stats =
        Filesystem::open(&f.fs, request(1814), STATS_INODE, libc::O_RDONLY as u32).await;
    drop(owner);

    // Clean up every unexpected success using its corresponding real RELEASE,
    // before reporting the behavioral failure; do not repoll a completed future.
    if let Ok(opened) = &new_file {
        Filesystem::release(&f.fs, request(1815), f.ino, opened.fh, 0, 0, false)
            .await
            .unwrap();
    }
    if let Ok(opened) = &new_dir {
        Filesystem::releasedir(&f.fs, request(1816), 1, opened.fh, 0)
            .await
            .unwrap();
    }
    if let Ok(opened) = &new_stats {
        Filesystem::release(&f.fs, request(1817), STATS_INODE, opened.fh, 0, 0, false)
            .await
            .unwrap();
        assert!(f.fs.virtual_stats_snapshot(opened.fh).is_none());
    }
    Filesystem::release(&f.fs, request(1818), f.ino, held.fh, 0, 0, false)
        .await
        .unwrap();
    if !already_ready {
        tokio::time::timeout(Duration::from_secs(2), prepare.as_mut())
            .await
            .expect("closing did not finish after the held client RELEASE")
            .expect("client preparation returned failure");
    }
    assert_eq!(f.fs.open_file_handle_count(), 0);
    assert_eq!(
        f.budget.state().used[SnapshotBudgetPool::Control as usize],
        0
    );
    assert!(f.budget.state().peak[SnapshotBudgetPool::Control as usize] <= 32768);
    // ENODEV refuses a new OPEN. The terminal READ errno is independently EIO;
    // this does not broaden the cancelled-reader gate's allowed errno set.
    assert_eq!(new_file.err(), Some(Errno::from(libc::ENODEV)));
    assert_eq!(new_dir.err(), Some(Errno::from(libc::ENODEV)));
    assert_eq!(new_stats.err(), Some(Errno::from(libc::ENODEV)));
}

#[tokio::test]
async fn g07_existing_api_flush_and_temporary_close_do_not_replace_client_release() {
    let budget = SnapshotMountBudget::new(close_fence_limits()).unwrap();
    budget.validate_frame_capability(8 << 20).unwrap();
    let f = fixture_with_budget(budget).await;
    let owner = Filesystem::reserve_request_memory(&f.fs, 8)
        .unwrap()
        .unwrap();
    let client = Filesystem::open(&f.fs, request(1821), f.ino, libc::O_RDONLY as u32)
        .await
        .unwrap();
    drop(owner);
    assert_ne!(client.fh, 0);
    f.gate.mode.store(1, Ordering::SeqCst);
    let read_owner = Filesystem::reserve_request_memory(&f.fs, 40)
        .unwrap()
        .unwrap();
    let fs = f.fs.clone();
    let ino = f.ino;
    let reader = tokio::spawn(async move {
        // fh=0 takes the real adapter's internal FileGuard path. It is not a
        // second FUSE client OPEN and cannot retire the actual client above.
        let result = Filesystem::read(&fs, request(1822), ino, 0, 0, 8192).await;
        drop(read_owner);
        result
    });
    tokio::time::timeout(Duration::from_secs(2), f.gate.entered.notified())
        .await
        .expect("real LocalFS payload body did not become pending");
    assert_eq!(f.gate.payload_ranges.lock().unwrap().len(), 1);
    assert_eq!(f.gate.dropped.load(Ordering::SeqCst), 0);
    assert_eq!(f.fs.open_file_handle_count(), 2);
    let prepare = Filesystem::prepare_unmount(&f.fs);
    tokio::pin!(prepare);
    assert!(poll_once(prepare.as_mut()).await.is_pending());
    let cancelled = tokio::time::timeout(Duration::from_secs(2), reader)
        .await
        .expect("preparation did not cancel the original read unique")
        .unwrap();
    assert_eq!(cancelled.err(), Some(Errno::from(libc::EIO)));
    assert_eq!(f.gate.dropped.load(Ordering::SeqCst), 1);
    assert_cancelled_read(&f.observer);
    assert_eq!(f.fs.open_file_handle_count(), 1);
    let completed_after_temporary_close = poll_once(prepare.as_mut()).await.is_ready();

    Filesystem::flush(&f.fs, request(1823), f.ino, client.fh, 0)
        .await
        .unwrap();
    assert_eq!(f.fs.open_file_handle_count(), 1);
    let completed_after_flush =
        completed_after_temporary_close || poll_once(prepare.as_mut()).await.is_ready();
    Filesystem::release(&f.fs, request(1824), f.ino, client.fh, 0, 0, false)
        .await
        .unwrap();
    if !completed_after_flush {
        tokio::time::timeout(Duration::from_secs(2), prepare.as_mut())
            .await
            .expect("preparation did not finish after the actual client RELEASE")
            .expect("client preparation returned failure");
    }
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
        !completed_after_temporary_close,
        "internal FileGuard close retired a real client"
    );
    assert!(
        !completed_after_flush,
        "FLUSH substituted for the actual client RELEASE"
    );
}
