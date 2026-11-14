use super::*;

fn leased_cache() -> (
    UpperRowCache,
    Arc<crate::workspace::lifecycle::LeaseValidity>,
) {
    let lease = Arc::new(crate::workspace::lifecycle::LeaseValidity::new(
        Instant::now(),
        Duration::from_secs(600),
    ));
    let counters = Arc::new(RowCacheCounters::default());
    let budget = RowBudget::new(8192, &counters);
    (
        UpperRowCache::with_lease_budget(
            Duration::from_secs(60),
            counters,
            budget,
            Some(lease.clone()),
        ),
        lease,
    )
}

#[test]
fn retired_cache_fences_old_readers_without_erasing_commit_uncertainty() {
    let (cache, lease) = leased_cache();
    let cache = Arc::new(cache);
    warm(&cache, 1);
    let old_reader = LeaseLocalFence {
        cache: cache.clone(),
        validity: lease,
    };
    assert!(old_reader.unchanged(1, &[]));
    assert!(
        !cache.retire(),
        "a resolved cache may be replaced by a fresh one"
    );
    assert!(!old_reader.unchanged(1, &[]));
    let (uncertain, _) = leased_cache();
    drop(CacheCommitGuard::new(Some(&uncertain), None));
    let disabled = uncertain.retire();
    assert!(disabled);
    let (replacement, _) = leased_cache();
    replacement.inherit_disabled(disabled);
    replacement.insert(
        replacement.ticket(),
        RowKey::Inode(1),
        RowValue::Inode(Retained::new(Vec::new())),
    );
    assert!(replacement.get(&RowKey::Inode(1)).is_none());
}

#[test]
fn lease_retention_survives_ttl_but_not_key_version_or_lease_invalidation() {
    let (cache, lease) = leased_cache();
    warm(&cache, 1);
    warm(&cache, 2);
    let ticket = cache.ticket();
    cache.insert(ticket, RowKey::Inode(3), RowValue::EmptyUpper(ticket));
    for (_, entry) in cache.rows.write().entries.iter_mut() {
        entry.inserted = Instant::now() - Duration::from_secs(61);
    }
    assert!(cache.get(&RowKey::Inode(1)).is_some());
    assert!(cache.get(&RowKey::Inode(3)).is_some());
    cache.invalidate_keys(&[RowKey::Inode(1)]);
    assert!(cache.get(&RowKey::Inode(1)).is_none());
    assert!(cache.get(&RowKey::Inode(2)).is_some());
    assert!(cache.get(&RowKey::Inode(3)).is_none());
    cache.insert(
        ticket,
        RowKey::Inode(1),
        RowValue::Inode(Retained::new(Vec::new())),
    );
    assert!(
        cache.get(&RowKey::Inode(1)).is_none(),
        "old fill cannot restore a row"
    );
    lease.revoke();
    assert!(cache.get(&RowKey::Inode(2)).is_none());
    lease.extend(Instant::now(), Duration::from_secs(600));
    assert!(cache.get(&RowKey::Inode(2)).is_none());
    assert_eq!(cache.budget.used.load(Ordering::Acquire), 0);
}

#[test]
fn lease_retention_bounds_memory_and_disables_after_uncertain_commit() {
    let (cache, _) = leased_cache();
    for ino in 0..1000 {
        cache.insert(
            cache.ticket(),
            RowKey::Inode(ino),
            RowValue::Inode(Retained::new(Vec::new())),
        );
        assert!(cache.budget.used.load(Ordering::Acquire) <= 8192);
    }
    assert!(cache.get(&RowKey::Inode(999)).is_some());
    drop(CacheCommitGuard::new(Some(&cache), None));
    assert!(cache.get(&RowKey::Inode(999)).is_none());
    cache.insert(
        cache.ticket(),
        RowKey::Inode(999),
        RowValue::Inode(Retained::new(Vec::new())),
    );
    assert!(cache.get(&RowKey::Inode(999)).is_none());
}

#[test]
fn observed_lease_lapse_cannot_resurrect_retained_rows() {
    let (mut cache, _) = leased_cache();
    warm(&cache, 1);
    let lease = Arc::new(crate::workspace::lifecycle::LeaseValidity::new(
        Instant::now(),
        Duration::ZERO,
    ));
    cache.lease = Some(lease.clone());
    assert!(cache.get(&RowKey::Inode(1)).is_none());
    lease.extend(Instant::now(), Duration::from_secs(600));
    cache.insert(
        cache.ticket(),
        RowKey::Inode(1),
        RowValue::Inode(Retained::new(Vec::new())),
    );
    assert!(cache.get(&RowKey::Inode(1)).is_none());
}

fn warm(cache: &UpperRowCache, ino: i64) {
    cache.insert(
        cache.ticket(),
        RowKey::Inode(ino),
        RowValue::Inode(Retained::new(Vec::new())),
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
                RowValue::Inode(Retained::new(Vec::new())),
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
    assert!(cache.rows.read().len() as u64 <= super::CAPACITY);
    assert!(cache.budget.used.load(Ordering::Acquire) <= cache.budget.limit);
    // The ticket is not a license to retain a negative result forever.
    let cache = UpperRowCache::new(Duration::from_millis(25));
    let ticket = cache.ticket();
    cache.insert(ticket, RowKey::Inode(9), RowValue::EmptyUpper(ticket));
    assert!(cache.get(&RowKey::Inode(9)).is_some());
    std::thread::sleep(Duration::from_millis(30));
    assert!(cache.get(&RowKey::Inode(9)).is_none());
}

#[test]
fn negative_rows_obey_small_byte_budgets_and_release_charges() {
    let counters = Arc::new(RowCacheCounters::default());
    let budget = RowBudget::new(4096, &counters);
    let cache = UpperRowCache::with_budget(Duration::from_secs(60), counters, budget.clone());
    let ticket = cache.ticket();
    for ino in 0..100 {
        cache.insert(ticket, RowKey::Inode(ino), RowValue::EmptyUpper(ticket));
        assert!(budget.used.load(Ordering::Acquire) <= 4096);
    }
    assert!(cache.get(&RowKey::Inode(99)).is_some());
    assert!(cache.rows.read().len() < 100);
    cache.invalidate_all();
    assert_eq!(budget.used.load(Ordering::Acquire), 0);
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
    let validity = Arc::new(crate::workspace::lifecycle::LeaseValidity::new(
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
        RowValue::Inode(Retained::new(Vec::new())),
    );
    assert!(
        !fence.unchanged(1, &[]),
        "refill cannot revive local read authority"
    );
}
