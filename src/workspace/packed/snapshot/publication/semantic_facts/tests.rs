use super::*;
use crate::workspace::packed::snapshot::SnapshotBudgetPool;

fn limits() -> SemanticFactLimits {
    SemanticFactLimits {
        inventory: InventoryLimits {
            max_objects: 64,
            max_declared_bytes: 16 << 20,
            max_disk_bytes: 2 << 20,
            sqlite_cache_bytes: 64 << 10,
        },
        max_contexts: 32,
        max_visits: 128,
        max_leaves: 512,
        max_page_records: 512,
        max_sql_operations: 10000,
        max_sql_vm_steps: 1000000,
    }
}
fn leaf(kind: SnapshotObjectKind, key: u64) -> SnapshotIndexPage {
    SnapshotIndexPage {
        kind,
        height: 0,
        records: vec![SnapshotIndexRecord {
            first_key: key.to_be_bytes().to_vec(),
            last_key: key.to_be_bytes().to_vec(),
            value: SnapshotIndexValue::Leaf(vec![1, 2, 3]),
        }],
    }
}
fn reference(key: &str, page: &SnapshotIndexPage) -> SnapshotObjectRef {
    let bytes = page.encode().unwrap();
    SnapshotObjectRef::from_bytes(key.into(), page.kind, &bytes).unwrap()
}
async fn save(facts: &mut SemanticFacts, reference: &SnapshotObjectRef, page: &SnapshotIndexPage) {
    facts.register_object(reference).await.unwrap();
    facts.mark_authenticated(reference).await.unwrap();
    facts
        .store_authenticated_page(reference, page)
        .await
        .unwrap();
}
async fn consume_leaf(facts: &mut SemanticFacts, visit: &PageVisit) {
    let record = facts
        .next_page_record(&visit.reference, None)
        .await
        .unwrap()
        .unwrap();
    facts
        .insert_leaf(visit, record.slot, &record.record)
        .await
        .unwrap();
    drop(record);
    facts.finish_visit(visit.id).await.unwrap();
}

#[tokio::test]
async fn complete_leaf_context_and_indexed_lookup_are_owned() {
    let parent = tempfile::tempdir().unwrap();
    let budget = SnapshotMountBudget::defaults();
    let mut facts = SemanticFacts::create(
        parent.path(),
        budget.clone(),
        limits(),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    let page = leaf(SnapshotObjectKind::InodeIndex, 3);
    let object = reference("facts/inodes", &page);
    let context = facts
        .register_context(SemanticRole::Inodes, &object, Some(1))
        .await
        .unwrap();
    save(&mut facts, &object, &page).await;
    let visit = facts.next_visit().await.unwrap().unwrap();
    consume_leaf(&mut facts, &visit).await;
    drop(visit);
    facts.finish_context(context).await.unwrap();
    let lookup = facts
        .context_leaf(context, &3u64.to_be_bytes())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(lookup.first_key, 3u64.to_be_bytes());
    assert!(
        facts
            .next_context_leaf(context, Some(&lookup.first_key))
            .await
            .unwrap()
            .is_none()
    );
    let summary = facts.finish_all().await.unwrap();
    assert_eq!(
        (summary.contexts, summary.visits, summary.leaves),
        (1, 1, 1)
    );
    assert_eq!(summary.inventory.authenticated_objects, 1);
    assert!(summary.sql_vm_steps > 0);
    facts.close().await.unwrap();
    assert_eq!(
        budget.state().used[SnapshotBudgetPool::Metadata as usize],
        32 << 10
    );
    drop(lookup);
    assert!(budget.state().used.iter().all(|bytes| *bytes == 0));
    assert_eq!(std::fs::read_dir(parent.path()).unwrap().count(), 0);
}

#[tokio::test]
async fn same_physical_page_has_independent_root_occurrences() {
    let parent = tempfile::tempdir().unwrap();
    let mut facts = SemanticFacts::create(
        parent.path(),
        SnapshotMountBudget::defaults(),
        limits(),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    let page = leaf(SnapshotObjectKind::LargeIndex, 3);
    let object = reference("facts/shared", &page);
    let first = facts
        .register_context(
            SemanticRole::ExternalExtents { inode: 3, eof: 100 },
            &object,
            Some(1),
        )
        .await
        .unwrap();
    let second = facts
        .register_context(
            SemanticRole::ExternalExtents { inode: 4, eof: 200 },
            &object,
            Some(1),
        )
        .await
        .unwrap();
    save(&mut facts, &object, &page).await;
    for context in [first, second] {
        let visit = facts.next_visit().await.unwrap().unwrap();
        assert_eq!(visit.context, context);
        consume_leaf(&mut facts, &visit).await;
        drop(visit);
        facts.finish_context(context).await.unwrap();
    }
    let summary = facts.finish_all().await.unwrap();
    assert_eq!(
        (
            summary.visits,
            summary.leaves,
            summary.inventory.registered_objects
        ),
        (2, 2, 1)
    );
    facts.close().await.unwrap();
}

#[tokio::test]
async fn authenticated_child_must_match_each_parent_weight() {
    let parent = tempfile::tempdir().unwrap();
    let mut facts = SemanticFacts::create(
        parent.path(),
        SnapshotMountBudget::defaults(),
        limits(),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    let child_page = leaf(SnapshotObjectKind::InodeIndex, 3);
    let child = reference("facts/child", &child_page);
    let branch = SnapshotIndexPage {
        kind: SnapshotObjectKind::InodeIndex,
        height: 1,
        records: vec![SnapshotIndexRecord {
            first_key: 3u64.to_be_bytes().to_vec(),
            last_key: 3u64.to_be_bytes().to_vec(),
            value: SnapshotIndexValue::Child {
                reference: child.clone(),
                subtree_weight: 2,
            },
        }],
    };
    let root = reference("facts/root", &branch);
    facts
        .register_context(SemanticRole::Inodes, &root, Some(2))
        .await
        .unwrap();
    save(&mut facts, &root, &branch).await;
    save(&mut facts, &child, &child_page).await;
    let parent_visit = facts.next_visit().await.unwrap().unwrap();
    facts
        .enqueue_child(&parent_visit, 0, &branch.records[0])
        .await
        .unwrap();
    facts.finish_visit(parent_visit.id).await.unwrap();
    drop(parent_visit);
    let child_visit = facts.next_visit().await.unwrap().unwrap();
    facts
        .insert_leaf(&child_visit, 0, &child_page.records[0])
        .await
        .unwrap();
    let error = facts.finish_visit(child_visit.id).await.unwrap_err();
    assert!(
        matches!(error, PackedWireError::Invalid(message) if message.contains("fences/weight"))
    );
    assert!(facts.finish_all().await.is_err());
    drop(child_visit);
    facts.close().await.unwrap();
}

#[tokio::test]
async fn missing_leaf_cannot_complete_a_visit_or_context() {
    let parent = tempfile::tempdir().unwrap();
    let mut facts = SemanticFacts::create(
        parent.path(),
        SnapshotMountBudget::defaults(),
        limits(),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    let page = leaf(SnapshotObjectKind::InodeIndex, 3);
    let object = reference("facts/missing", &page);
    let context = facts
        .register_context(SemanticRole::Inodes, &object, Some(1))
        .await
        .unwrap();
    save(&mut facts, &object, &page).await;
    let visit = facts.next_visit().await.unwrap().unwrap();
    assert!(matches!(
        facts.finish_visit(visit.id).await,
        Err(PackedWireError::Invalid(_))
    ));
    assert!(facts.finish_context(context).await.is_err());
    drop(visit);
    facts.close().await.unwrap();
}

#[tokio::test]
async fn identity_conflict_and_visit_quota_reject_before_new_occurrence() {
    for conflict in [true, false] {
        let parent = tempfile::tempdir().unwrap();
        let mut policy = limits();
        policy.max_visits = 1;
        let mut facts = SemanticFacts::create(
            parent.path(),
            SnapshotMountBudget::defaults(),
            policy,
            CancellationToken::new(),
        )
        .await
        .unwrap();
        let page = leaf(SnapshotObjectKind::LargeIndex, 3);
        let object = reference("facts/quota", &page);
        facts
            .register_context(
                SemanticRole::ExternalExtents { inode: 3, eof: 100 },
                &object,
                None,
            )
            .await
            .unwrap();
        if conflict {
            let mut changed = object.clone();
            changed.digest[0] ^= 1;
            assert!(matches!(
                facts.register_object(&changed).await,
                Err(PackedWireError::Invalid(_))
            ));
        } else {
            assert!(matches!(
                facts
                    .register_context(
                        SemanticRole::ExternalExtents { inode: 4, eof: 200 },
                        &object,
                        None
                    )
                    .await,
                Err(PackedWireError::LimitExceeded(_))
            ));
        }
        assert_eq!(facts.summary.visits, 1);
        facts.close().await.unwrap();
    }
}

#[tokio::test]
async fn semantic_vm_quota_interrupts_real_sql_and_still_closes_storage() {
    let parent = tempfile::tempdir().unwrap();
    let budget = SnapshotMountBudget::defaults();
    let mut policy = limits();
    policy.max_sql_vm_steps = 1;
    assert!(
        matches!(SemanticFacts::create(parent.path(), budget.clone(), policy, CancellationToken::new()).await, Err(PackedWireError::LimitExceeded(message)) if message.contains("VM-work"))
    );
    // Failed creation uses the same detached close worker. Explicitly allow
    // that acknowledgment to finish; no Tokio-only cleanup is assumed.
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while budget.state().used.iter().any(|bytes| *bytes != 0) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(std::fs::read_dir(parent.path()).unwrap().count(), 0);
}

#[tokio::test]
async fn dropped_semantic_query_retains_admission_until_real_close() {
    use std::time::Duration;

    let parent = tempfile::tempdir().unwrap();
    let budget = SnapshotMountBudget::defaults();
    let mut facts = SemanticFacts::create(
        parent.path(),
        budget.clone(),
        limits(),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    let barrier = facts
        .inventory
        .install_query_owner_test_barrier()
        .await
        .unwrap();
    let metadata_before = budget.state().used[SnapshotBudgetPool::Metadata as usize];
    let reached_worker_barrier = {
        let operation = facts.execute(|| sea_orm::sqlx::query("SELECT 1"));
        tokio::pin!(operation);
        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::select! {
                _ = barrier.entered() => true,
                _ = &mut operation => false,
            }
        })
        .await
        .is_ok_and(|entered| entered)
        // Drop the pinned future itself at the scope boundary before checking
        // its reservation; dropping only Pin<&mut _> would retain the future.
    };
    let worker_still_blocked = barrier.is_blocked();
    let held_metadata = budget.state().used[SnapshotBudgetPool::Metadata as usize];
    let reuse_rejected = tokio::time::timeout(Duration::from_millis(200), facts.next_visit())
        .await
        .is_ok_and(|result| result.is_err());
    let reuse_preserved_metadata =
        budget.state().used[SnapshotBudgetPool::Metadata as usize] == held_metadata;

    // Release and acknowledge the real close before asserting observations,
    // so any RED result still leaves no intentionally blocked worker behind.
    barrier.release();
    let close_result = facts.close().await;
    let after_close = budget.state();
    assert!(close_result.is_ok(), "{close_result:?}");
    assert!(after_close.used.iter().all(|bytes| *bytes == 0));
    assert_eq!(std::fs::read_dir(parent.path()).unwrap().count(), 0);
    assert!(
        reached_worker_barrier,
        "query did not reach sqlite3_step barrier"
    );
    assert!(
        worker_still_blocked,
        "worker completed before caller future drop"
    );
    assert_eq!(held_metadata, metadata_before + (32 << 10));
    assert!(reuse_rejected, "dropped query allowed a new SQL operation");
    assert!(
        reuse_preserved_metadata,
        "reuse replaced the live worker owner"
    );
}

#[tokio::test]
async fn leaf_quota_rejects_before_context_counter_or_second_leaf_changes() {
    let parent = tempfile::tempdir().unwrap();
    let mut policy = limits();
    policy.max_leaves = 1;
    let mut facts = SemanticFacts::create(
        parent.path(),
        SnapshotMountBudget::defaults(),
        policy,
        CancellationToken::new(),
    )
    .await
    .unwrap();
    let mut page = leaf(SnapshotObjectKind::InodeIndex, 3);
    page.records
        .push(leaf(SnapshotObjectKind::InodeIndex, 4).records.remove(0));
    let object = reference("facts/leaf-cap", &page);
    facts
        .register_context(SemanticRole::Inodes, &object, Some(2))
        .await
        .unwrap();
    save(&mut facts, &object, &page).await;
    let visit = facts.next_visit().await.unwrap().unwrap();
    facts
        .insert_leaf(&visit, 0, &page.records[0])
        .await
        .unwrap();
    assert!(matches!(
        facts.insert_leaf(&visit, 1, &page.records[1]).await,
        Err(PackedWireError::LimitExceeded(_))
    ));
    assert_eq!(facts.summary.leaves, 1);
    assert!(facts.finish_all().await.is_err());
    drop(visit);
    facts.close().await.unwrap();
}

#[tokio::test]
async fn semantic_schema_cannot_exceed_physical_disk_owner_quota() {
    let parent = tempfile::tempdir().unwrap();
    let budget = SnapshotMountBudget::defaults();
    let mut policy = limits();
    policy.inventory.max_disk_bytes = 4 * 4096;
    assert!(
        matches!(SemanticFacts::create(parent.path(), budget.clone(), policy, CancellationToken::new()).await, Err(PackedWireError::LimitExceeded(message)) if message.contains("disk quota"))
    );
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while budget.state().used.iter().any(|bytes| *bytes != 0) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(std::fs::read_dir(parent.path()).unwrap().count(), 0);
}
