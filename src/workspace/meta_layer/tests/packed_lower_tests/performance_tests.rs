use super::*;

#[tokio::test]
async fn packed_hot_inode_memo_coalesces_concurrent_misses() {
    let f = fixture().await;
    let lower = f.meta.packed_lower().unwrap();
    f.gets.store(0, Ordering::SeqCst);
    let results = futures::future::join_all((0..16).map(|_| lower.inode_hot(400))).await;
    let values: Vec<_> = results
        .into_iter()
        .map(|value| value.unwrap().unwrap())
        .collect();
    let distinct = values
        .iter()
        .filter(|value| !Arc::ptr_eq(value, &values[0]))
        .count()
        + 1;
    eprintln!(
        "hot inode: {distinct} separately loaded values, {} object reads",
        f.gets.load(Ordering::SeqCst)
    );
    assert_eq!(
        distinct, 1,
        "one authenticated inode load for a concurrent cold burst"
    );
}

#[tokio::test]
async fn packed_cold_inode_memo_coalesces_concurrent_misses() {
    let f = fixture().await;
    let lower = f.meta.packed_lower().unwrap();
    f.gets.store(0, Ordering::SeqCst);
    let results = futures::future::join_all((0..16).map(|_| lower.inode_cold(400))).await;
    let values: Vec<_> = results
        .into_iter()
        .map(|value| value.unwrap().unwrap())
        .collect();
    let distinct = values
        .iter()
        .filter(|value| !Arc::ptr_eq(value, &values[0]))
        .count()
        + 1;
    eprintln!(
        "cold inode: {distinct} separately loaded values, {} object reads",
        f.gets.load(Ordering::SeqCst)
    );
    assert_eq!(
        distinct, 1,
        "one authenticated cold-attribute load per burst"
    );
}

#[tokio::test]
async fn packed_reverse_alias_memo_coalesces_concurrent_page_walks() {
    let f = fixture().await;
    let lower = f.meta.packed_lower().unwrap();
    let pages = AtomicU64::new(0);
    let results = futures::future::join_all((0..16).map(|_| {
        lower.reverse_aliases(400, || {
            pages.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    }))
    .await;
    for result in results {
        assert_eq!(*result.unwrap(), vec![(1, b"file".to_vec())]);
    }
    assert_eq!(
        pages.load(Ordering::SeqCst),
        2,
        "one complete two-page reverse walk per burst"
    );
}

#[tokio::test]
async fn packed_inode_memo_keeps_absence_cached_and_errors_retryable() {
    let f = fixture().await;
    let lower = f.meta.packed_lower().unwrap();
    f.fail_reads.store(true, Ordering::SeqCst);
    assert!(lower.inode_hot(400).await.is_err());
    f.fail_reads.store(false, Ordering::SeqCst);
    assert!(lower.inode_hot(400).await.unwrap().is_some());
    assert!(lower.inode_hot(999_999).await.unwrap().is_none());
    let before = f.gets.load(Ordering::SeqCst);
    assert!(lower.inode_hot(999_999).await.unwrap().is_none());
    assert_eq!(f.gets.load(Ordering::SeqCst), before);
}

#[tokio::test]
async fn packed_inode_memo_cancellation_does_not_block_other_keys_or_retry() {
    use futures::FutureExt;
    let f = fixture().await;
    let lower = f.meta.packed_lower().unwrap();
    let mut first = Box::pin(lower.inode_hot(400));
    assert!(first.as_mut().now_or_never().is_none());
    // Leave the first caller unpolled: another key must not wait on its fill.
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(5), lower.inode_hot(999_999))
            .await
            .unwrap()
            .unwrap()
            .is_none()
    );
    drop(first);
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(5), lower.inode_hot(400))
            .await
            .unwrap()
            .unwrap()
            .is_some()
    );
}
