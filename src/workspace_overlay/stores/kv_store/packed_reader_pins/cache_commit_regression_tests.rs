//! Exercise cached authority through the real metadata commit path.
use super::*;

#[tokio::test]
async fn canceled_attribute_commit_cannot_leave_old_rows_after_the_owned_cas_finishes() {
    use crate::workspace_overlay::lifecycle::LeaseValidity;
    let validity = Arc::new(LeaseValidity::new(
        std::time::Instant::now(),
        std::time::Duration::from_secs(60),
    ));
    let f = fixture_with_lease(false, packed().await, Some(validity.clone())).await;
    let ino = f.meta.lookup(1, "nonzero").await.unwrap().unwrap();
    let original = f.meta.stat(ino).await.unwrap().unwrap().mode & 0o777;
    let changed = original ^ 0o111;
    let entered = Arc::new(tokio::sync::Notify::new());
    let dispatch = Arc::new(tokio::sync::Notify::new());
    let committed = Arc::new(tokio::sync::Notify::new());
    let reply = Arc::new(tokio::sync::Notify::new());
    *f.backend.pre_cas_barrier.lock().await = Some((entered.clone(), dispatch.clone()));
    *f.backend.reply_barrier.lock().await = Some((committed.clone(), reply.clone()));
    let meta = f.meta.clone();
    let caller = tokio::spawn(async move { meta.chmod(ino, changed).await });
    tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
        .await
        .expect("attribute commit reached the dispatched CAS");
    // The common commit path has already invalidated its keys. This read can
    // repopulate them from old server state while the owned driver is paused.
    assert_eq!(
        f.meta.stat(ino).await.unwrap().unwrap().mode & 0o777,
        original
    );
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    assert!(validity.authoritative());
    // Clearing once on cancellation is insufficient: a late server commit
    // can follow another successful read of the still-old server rows.
    assert_eq!(
        f.meta.stat(ino).await.unwrap().unwrap().mode & 0o777,
        original
    );
    dispatch.notify_one();
    tokio::time::timeout(std::time::Duration::from_secs(5), committed.notified())
        .await
        .expect("owned CAS still commits after its caller was canceled");
    let visible = f.meta.stat(ino).await.unwrap().unwrap().mode & 0o777;
    reply.notify_one();
    assert_eq!(
        visible, changed,
        "the late commit must not be hidden by a refilled old cache row"
    );
    f.reader.shutdown().await.unwrap();
}

#[tokio::test]
async fn native_overlay_stat_and_lookup_cache_avoid_points_only_while_lease_is_proven() {
    let (meta, backend, validity) = super::performance_tests::native_cached_meta(true).await;
    let root = meta.root_ino();
    let ino = meta.create_file(root, "hot".into()).await.unwrap();
    assert_eq!(meta.lookup(root, "hot").await.unwrap(), Some(ino));
    let expected = meta.stat(ino).await.unwrap().unwrap();
    backend.point_reads.store(0, Ordering::SeqCst);
    for _ in 0..16 {
        assert_eq!(meta.lookup(root, "hot").await.unwrap(), Some(ino));
        assert_eq!(meta.stat(ino).await.unwrap().unwrap().mode, expected.mode);
    }
    assert_eq!(
        backend.point_reads.load(Ordering::SeqCst),
        0,
        "the getattr/lookup hot path must avoid ordinary get_many point reads"
    );
    meta.chmod(ino, 0o600).await.unwrap();
    assert_eq!(meta.stat(ino).await.unwrap().unwrap().mode & 0o777, 0o600);
    validity.revoke();
    backend.point_reads.store(0, Ordering::SeqCst);
    assert_eq!(meta.lookup(root, "hot").await.unwrap(), Some(ino));
    assert_eq!(meta.stat(ino).await.unwrap().unwrap().mode & 0o777, 0o600);
    assert!(
        backend.point_reads.load(Ordering::SeqCst) > 0,
        "lost lease authority must bypass warmed ordinary metadata rows"
    );
}
