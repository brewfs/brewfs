use super::*;

fn warm(cache: &UpperRowCache, ino: i64) {
    let key = RowKey::Inode(ino);
    cache.insert(
        cache.ticket(&key),
        key.clone(),
        RowValue::Inode(Arc::new(Vec::new())),
    );
    assert!(cache.get(&key).is_some());
}

#[test]
fn unresolved_commit_disables_both_caches_and_later_refills() {
    let upper = UpperRowCache::new(Duration::from_secs(60));
    let namespace = UpperRowCache::new(Duration::from_secs(60));
    warm(&upper, 1);
    warm(&namespace, 1);
    let key = RowKey::Inode(1);
    let in_flight = upper.ticket(&key);
    drop(CacheCommitGuard::new(Some(&upper), Some(&namespace)));
    for cache in [&upper, &namespace] {
        assert!(cache.get(&RowKey::Inode(1)).is_none());
        // A later backend read can observe the old server state while an
        // uncertain request is still executing. It must never become cached.
        for ticket in [in_flight, cache.ticket(&key)] {
            cache.insert(ticket, key.clone(), RowValue::Inode(Arc::new(Vec::new())));
            assert!(cache.get(&RowKey::Inode(1)).is_none());
        }
        cache.invalidate_all();
        assert!(cache.get(&RowKey::Inode(1)).is_none());
    }
}

#[test]
fn confirmed_commit_does_not_disable_unrelated_cached_rows() {
    let cache = UpperRowCache::new(Duration::from_secs(60));
    warm(&cache, 1);
    warm(&cache, 2);
    let commit = CacheCommitGuard::new(Some(&cache), None);
    cache.invalidate_keys(&[RowKey::Inode(1)]);
    cache.invalidate_keys(&[RowKey::Inode(1)]);
    commit.resolved();
    assert!(cache.get(&RowKey::Inode(2)).is_some());
    warm(&cache, 1);
}

#[tokio::test]
async fn canceling_the_await_disables_authority_even_with_a_live_lease() {
    use futures::FutureExt;
    let cache = Arc::new(UpperRowCache::new(Duration::from_secs(60)));
    let validity = Arc::new(crate::workspace_overlay::lifecycle::LeaseValidity::new(
        std::time::Instant::now(),
        Duration::from_secs(60),
    ));
    let fence = LeaseLocalFence {
        cache: cache.clone(),
        validity: validity.clone(),
    };
    warm(&cache, 1);
    assert!(fence.unchanged(1, &[]));
    let mut pending = Box::pin(async {
        let _guard = CacheCommitGuard::new(Some(&cache), None);
        std::future::pending::<()>().await;
    });
    assert!(pending.as_mut().now_or_never().is_none());
    drop(pending);
    assert!(
        validity.authoritative(),
        "cache cancellation must not revoke the actual lease"
    );
    assert!(!fence.unchanged(1, &[]));
    cache.insert(
        cache.ticket(&RowKey::Inode(1)),
        RowKey::Inode(1),
        RowValue::Inode(Arc::new(Vec::new())),
    );
    assert!(
        !fence.unchanged(1, &[]),
        "refill cannot revive local read authority"
    );
}
