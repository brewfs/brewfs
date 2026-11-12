use super::*;

fn warm(cache: &UpperRowCache, ino: i64) {
    cache.insert(
        cache.ticket(),
        RowKey::Inode(ino),
        RowValue::Inode(Arc::new(Vec::new())),
    );
    assert!(cache.get(&RowKey::Inode(ino)).is_some());
}

#[test]
fn unresolved_commit_disables_both_caches_and_later_refills() {
    let upper = UpperRowCache::new(Duration::from_secs(60));
    let namespace = UpperRowCache::new(Duration::from_secs(60));
    warm(&upper, 1);
    warm(&namespace, 1);
    let in_flight = upper.ticket();
    drop(CacheCommitGuard::new(Some(&upper), Some(&namespace)));
    for cache in [&upper, &namespace] {
        assert!(cache.get(&RowKey::Inode(1)).is_none());
        // A later backend read can observe the old server state while an
        // uncertain request is still executing. It must never become cached.
        for ticket in [in_flight, cache.ticket()] {
            cache.insert(
                ticket,
                RowKey::Inode(1),
                RowValue::Inode(Arc::new(Vec::new())),
            );
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

#[test]
fn negative_rows_cannot_survive_unrelated_mutation_or_cancelled_commit() {
    let cache = UpperRowCache::new(Duration::from_secs(60));
    // The value's generation is checked even when ordinary invalidation does
    // not name its key; the same path protects both empty query variants.
    let key = RowKey::Inode(99);
    let old = cache.ticket();
    cache.insert(old, key.clone(), RowValue::EmptyUpper(old));
    assert!(cache.get(&key).is_some());
    cache.invalidate_keys(&[RowKey::Inode(1)]);
    assert!(cache.get(&key).is_none());
    cache.insert(old, key.clone(), RowValue::EmptyUpper(old));
    assert!(cache.get(&key).is_none());
    let fresh = cache.ticket();
    cache.insert(fresh, key.clone(), RowValue::EmptyUpper(fresh));
    drop(CacheCommitGuard::new(Some(&cache), None));
    assert!(cache.get(&key).is_none());
    let after = cache.ticket();
    cache.insert(after, key.clone(), RowValue::EmptyUpper(after));
    assert!(cache.get(&key).is_none());
}

#[test]
fn negative_rows_share_the_existing_capacity_and_expire() {
    let cache = UpperRowCache::new(Duration::from_secs(60));
    let ticket = cache.ticket();
    for ino in 0..(super::CAPACITY + 1000) {
        cache.insert(
            ticket,
            RowKey::Inode(ino as i64),
            RowValue::EmptyUpper(ticket),
        );
    }
    cache.rows.run_pending_tasks();
    assert!(cache.rows.entry_count() <= super::CAPACITY);
    // The ticket is not a license to retain a negative result forever.
    let cache = UpperRowCache::new(Duration::from_millis(25));
    let ticket = cache.ticket();
    cache.insert(ticket, RowKey::Inode(9), RowValue::EmptyUpper(ticket));
    assert!(cache.get(&RowKey::Inode(9)).is_some());
    std::thread::sleep(Duration::from_millis(30));
    assert!(cache.get(&RowKey::Inode(9)).is_none());
}

#[test]
fn negative_keys_separate_workspaces_leases_layer_versions_and_ranges() {
    let view = ReadViewKey {
        workspace: WorkspaceId::new(),
        epoch: 1,
        lease: LeaseId::new(),
        holder: 1,
        layers: [(LayerId::new(), 2, None), (LayerId::new(), 1, Some(1))],
    };
    let cache = UpperRowCache::new(Duration::from_secs(60));
    let key = RowKey::EmptyExtent(view.clone(), 400, 0, 0, 64);
    let ticket = cache.ticket();
    cache.insert(ticket, key.clone(), RowValue::EmptyUpper(ticket));
    assert!(cache.get(&key).is_some());
    let mut alternatives = Vec::new();
    let mut changed = view.clone();
    changed.workspace = WorkspaceId::new();
    alternatives.push(changed);
    let mut changed = view.clone();
    changed.epoch += 1;
    alternatives.push(changed);
    let mut changed = view.clone();
    changed.lease = LeaseId::new();
    alternatives.push(changed);
    let mut changed = view.clone();
    changed.holder += 1;
    alternatives.push(changed);
    let mut changed = view.clone();
    changed.layers[0].0 = LayerId::new();
    alternatives.push(changed);
    let mut changed = view.clone();
    changed.layers[0].1 += 1;
    alternatives.push(changed);
    let mut changed = view.clone();
    changed.layers[1].2 = Some(2);
    alternatives.push(changed);
    for changed in alternatives {
        assert!(
            cache
                .get(&RowKey::EmptyExtent(changed, 400, 0, 0, 64))
                .is_none()
        );
    }
    for (ino, chunk, start, end) in [
        (401, 0, 0, 64),
        (400, 1, 0, 64),
        (400, 0, 1, 64),
        (400, 0, 0, 65),
    ] {
        assert!(
            cache
                .get(&RowKey::EmptyExtent(view.clone(), ino, chunk, start, end))
                .is_none()
        );
    }
    assert!(cache.get(&RowKey::EmptyDirectory(view, 400)).is_none());
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
        cache.ticket(),
        RowKey::Inode(1),
        RowValue::Inode(Arc::new(Vec::new())),
    );
    assert!(
        !fence.unchanged(1, &[]),
        "refill cannot revive local read authority"
    );
}
