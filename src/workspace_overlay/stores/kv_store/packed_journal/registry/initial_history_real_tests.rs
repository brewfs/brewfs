//! Actual authenticated initial packed-v3 graph adoption and final deletion.
//! Redis/TiKV own UUID namespaces; all catalog facts come from production APIs.

use super::*;
use crate::chunk::ChunkLayout;
use crate::chunk::store::InMemoryBlockStore;
use crate::workspace_overlay::catalog::{CreateSnapshot, MarkDeleting, RenewLease, WorkspaceStore};
use crate::workspace_overlay::gc::WorkspaceGc;
use crate::workspace_overlay::model::{LeaseState, WorkspaceState};
use crate::workspace_overlay::packed_reader_lifecycle::PackedReaderLeaseOptions;
use crate::workspace_overlay::stores::binding_tests::{packed, request};
use crate::workspace_overlay::stores::redis::RedisWorkspaceBackend;
use crate::workspace_overlay::stores::tikv::TiKvWorkspaceBackend;
use std::path::Path;
use std::time::Duration;

const INITIAL_GRACE_NS: u64 = 10_000_000_000;

fn retirement_options(incarnation: Uuid) -> PackedHistoryRetirementOptions {
    PackedHistoryRetirementOptions {
        incarnation,
        grace_ns: INITIAL_GRACE_NS,
        max_native_holds: 1000,
        max_current_bindings: 1000,
        max_work: u64::MAX,
        cancel: CancellationToken::new(),
    }
}

async fn assert_initial_retained<B: WorkspaceKvBackend>(
    backend: &B,
    binding: &PackedLowerBindingRecord,
    root: &RootRow,
    original_root: &[u8],
    objects: &Path,
    references: &[V3ObjectRef],
) {
    for key in [
        registry_root_key(root.incarnation),
        registry_history_root_key(binding),
    ] {
        assert_eq!(
            backend.get(&key).await.unwrap().as_deref(),
            Some(original_root)
        );
    }
    let encoded = binding.encode().unwrap();
    for key in [
        packed_current_key(binding.workspace_id),
        packed_history_key(binding.workspace_id, 1),
    ] {
        assert_eq!(backend.get(&key).await.unwrap(), Some(encoded.clone()));
    }
    assert_eq!(
        backend
            .get(&packed_claim_key(binding.workspace_id))
            .await
            .unwrap()
            .as_deref(),
        Some(PACKED_CLAIM)
    );
    for reference in references {
        assert!(
            objects.join("objects").join(&reference.key).is_file(),
            "actual adopted object disappeared before final retirement: {}",
            reference.key
        );
        let member = MemberRow::decode(
            &backend
                .get(&registry_member_key(reference, root.incarnation))
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert!(member.retained && member.adopted);
        assert!(!member.pending_put && !member.dispatched);
        let object = ObjectRow::decode(
            &backend
                .get(&registry_object_key(reference))
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(object.state, ObjectState::Live);
        assert_eq!(object.memberships, 1);
    }
}

async fn contract<B: WorkspaceKvBackend>(backend: Arc<B>) {
    // This is the same actual LocalFS producer + authenticated lower fixture
    // used by native-publication history tests. It is installed as history 1;
    // the registry migration authenticates every reachable object itself.
    let (objects, client, snapshot, lower, _payload) = packed().await;
    let scratch = tempfile::tempdir().unwrap();
    let budget = V3MountBudget::defaults();
    let store = Arc::new(
        KvWorkspaceStore::from_arc(backend.clone()).with_packed_reader_pin_budget(budget.clone()),
    );
    let install = request(store.as_ref(), lower).await;
    let binding = store
        .install_packed_lower_binding(install.clone())
        .await
        .unwrap();
    assert_eq!(binding.binding.binding_version, 1);
    assert_eq!(binding.binding.manifest, *snapshot.manifest_reference());
    let guard = HeadGuard {
        expected_head_epoch: binding.head_epoch,
        ..install.guard
    };
    store
        .renew_lease(RenewLease {
            lease_id: guard.lease_id,
            holder_generation: guard.holder_generation,
            ttl_ns: 300_000_000_000,
        })
        .await
        .unwrap();
    let report = store
        .migrate_packed_object_registry(
            &client,
            &budget,
            collector::PackedRegistryMigrationOptions {
                scratch: scratch.path(),
                graph_limits: V3IndexAuditLimits::default(),
                max_catalog_rows: 100,
                cancel: CancellationToken::new(),
            },
        )
        .await
        .unwrap();
    assert!(report.catalog_rows >= 2);
    assert!(report.audited_objects > 0);
    let original_root = backend
        .get(&registry_history_root_key(&binding))
        .await
        .unwrap()
        .unwrap();
    let root = RootRow::decode(&original_root).unwrap();
    assert_eq!(root.state, RootState::BindingHistory);
    assert_eq!(root.binding.as_ref(), Some(&binding));
    assert!(root.members > 0);
    assert_eq!(root.pending_puts, 0);
    assert!(
        backend
            .get(&journal_key(root.journal_id))
            .await
            .unwrap()
            .is_none(),
        "an adopted initial root must not be backed by fabricated PPJ rows"
    );
    let mut references = Vec::new();
    for ordinal in 0..root.members {
        let raw = backend
            .get(&registry_reverse_key(root.incarnation, ordinal))
            .await
            .unwrap()
            .unwrap();
        let reference = V3ObjectRef::decode_value(&raw).unwrap();
        let member = MemberRow::decode(
            &backend
                .get(&registry_member_key(&reference, root.incarnation))
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(member.reference, reference);
        assert_eq!(member.ordinal, ordinal);
        assert_eq!(member.journal_id, root.journal_id);
        assert!(member.put_id.is_nil());
        references.push(reference);
    }
    assert!(references.contains(&binding.binding.manifest));
    assert_initial_retained(
        backend.as_ref(),
        &binding,
        &root,
        &original_root,
        objects.path(),
        &references,
    )
    .await;
    store
        .migrate_native_packed_holds(&budget, 1000, CancellationToken::new())
        .await
        .unwrap();
    let native_snapshot = store
        .create_snapshot(CreateSnapshot {
            snapshot_id: SnapshotId::new(),
            name: Some(format!("initial-history-native-hold-{}", Uuid::new_v4())),
            revision: binding.base_revision.clone(),
            owner_id: None,
        })
        .await
        .unwrap();
    let snapshot_hold = format!(
        "packed/v3/native-hold/snapshot/{}",
        native_snapshot.snapshot_id
    )
    .into_bytes();
    assert!(backend.get(&snapshot_hold).await.unwrap().is_some());
    // Live history 1 remains the durable lineage sentinel, even after every
    // object has been authenticated and both retirement gates are active.
    assert!(matches!(
        store
            .retire_packed_binding_history(
                client.clone(),
                budget.clone(),
                retirement_options(root.incarnation)
            )
            .await,
        Err(WorkspaceError::Fenced)
    ));
    assert_initial_retained(
        backend.as_ref(),
        &binding,
        &root,
        &original_root,
        objects.path(),
        &references,
    )
    .await;
    let reader = store
        .clone()
        .open_packed_reader_session(
            guard.clone(),
            budget.clone(),
            PackedReaderLeaseOptions::default(),
        )
        .await
        .unwrap();
    let request_owner = reader.retain_request().unwrap();
    store
        .mark_workspace_deleting(MarkDeleting {
            workspace_id: binding.workspace_id,
            force_fence_lease: true,
        })
        .await
        .unwrap();
    assert_eq!(
        store
            .load_workspace(binding.workspace_id)
            .await
            .unwrap()
            .state,
        WorkspaceState::Deleting
    );
    assert_eq!(
        store.load_layer(binding.head_layer_id).await.unwrap().state,
        LayerState::Deleting
    );
    let leases = store.list_leases(binding.workspace_id).await.unwrap();
    assert_eq!(leases.len(), 1);
    assert_eq!(leases[0].state, LeaseState::Released);
    for key in [
        format!("packed/v3/native-hold/workspace/{}", binding.workspace_id).into_bytes(),
        format!("packed/v3/native-hold/lease/{}", guard.lease_id).into_bytes(),
    ] {
        assert!(
            backend.get(&key).await.unwrap().is_none(),
            "Deleting must release native workspace/lease sidecars in its actual CAS"
        );
    }
    // The independently created native snapshot is still a real hold. The
    // driver checks this census before it reaches the packed reader pins.
    let census = store
        .native_packed_hold_census(
            &binding.base_revision,
            1000,
            &budget,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(census.matched(), 1);
    drop(census);
    assert!(matches!(
        store
            .retire_packed_binding_history(
                client.clone(),
                budget.clone(),
                retirement_options(root.incarnation)
            )
            .await,
        Err(WorkspaceError::Busy | WorkspaceError::Fenced)
    ));
    assert_initial_retained(
        backend.as_ref(),
        &binding,
        &root,
        &original_root,
        objects.path(),
        &references,
    )
    .await;
    store
        .delete_snapshot(native_snapshot.snapshot_id)
        .await
        .unwrap();
    assert!(backend.get(&snapshot_hold).await.unwrap().is_none());
    let census = store
        .native_packed_hold_census(
            &binding.base_revision,
            1000,
            &budget,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(census.matched(), 0);
    drop(census);
    // Native holds now equal zero. Only the real old reader's retained request
    // prevents initial history retirement; cancelling its waiter is insufficient.
    let mut shutdown = Box::pin(reader.shutdown());
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut shutdown)
            .await
            .is_err()
    );
    assert!(reader.retain_request().is_err());
    let pins = store.packed_reader_pin_roots().await.unwrap();
    assert!(pins.bindings.contains(&binding));
    drop(pins);
    assert!(matches!(
        store
            .retire_packed_binding_history(
                client.clone(),
                budget.clone(),
                retirement_options(root.incarnation)
            )
            .await,
        Err(WorkspaceError::Busy)
    ));
    assert_initial_retained(
        backend.as_ref(),
        &binding,
        &root,
        &original_root,
        objects.path(),
        &references,
    )
    .await;
    drop(request_owner);
    tokio::time::timeout(Duration::from_secs(10), &mut shutdown)
        .await
        .unwrap()
        .unwrap();
    drop(shutdown);
    drop(reader);
    assert!(
        !store
            .packed_reader_pin_roots()
            .await
            .unwrap()
            .bindings
            .contains(&binding)
    );
    // Reopen the catalog owner to exclude an in-process cache as evidence.
    let fresh = Arc::new(
        KvWorkspaceStore::from_arc(backend.clone()).with_packed_reader_pin_budget(budget.clone()),
    );
    let observation = fresh
        .retire_packed_binding_history(
            client.clone(),
            budget.clone(),
            retirement_options(root.incarnation),
        )
        .await
        .unwrap();
    assert!(observation.observing && !observation.retired);
    assert_eq!(observation.released_members, 0);
    assert_eq!(observation.deleted_objects, 0);
    assert_eq!(observation.quarantined_objects, 0);
    assert_initial_retained(
        backend.as_ref(),
        &binding,
        &root,
        &original_root,
        objects.path(),
        &references,
    )
    .await;
    let before_grace = fresh
        .retire_packed_binding_history(
            client.clone(),
            budget.clone(),
            retirement_options(root.incarnation),
        )
        .await
        .unwrap();
    assert!(before_grace.observing && !before_grace.retired);
    assert_eq!(before_grace.not_before_ns, observation.not_before_ns);
    assert_eq!(before_grace.released_members, 0);
    assert_eq!(before_grace.deleted_objects, 0);
    // Even a zero-grace native collector must retain the actual head/base
    // until the initial PWB3 history itself has retired.
    let blocks = Arc::new(InMemoryBlockStore::new());
    let gc = WorkspaceGc::new(
        fresh.clone(),
        blocks,
        ChunkLayout {
            chunk_size: 4096,
            block_size: 4096,
        },
        Duration::ZERO,
        Duration::ZERO,
    );
    let gc_before = gc
        .run_at(backend.server_time_ns().await.unwrap())
        .await
        .unwrap();
    assert!(gc_before.deleted_layers.is_empty());
    assert_initial_retained(
        backend.as_ref(),
        &binding,
        &root,
        &original_root,
        objects.path(),
        &references,
    )
    .await;
    tokio::time::timeout(Duration::from_secs(45), async {
        while backend.server_time_ns().await.unwrap() < observation.not_before_ns {
            assert_initial_retained(
                backend.as_ref(),
                &binding,
                &root,
                &original_root,
                objects.path(),
                &references,
            )
            .await;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
    let retired = fresh
        .retire_packed_binding_history(
            client.clone(),
            budget.clone(),
            retirement_options(root.incarnation),
        )
        .await
        .unwrap();
    assert!(!retired.observing && retired.retired);
    assert_eq!(retired.not_before_ns, observation.not_before_ns);
    assert_eq!(retired.released_members, root.members);
    assert_eq!(retired.deleted_objects, root.members);
    assert_eq!(retired.quarantined_objects, 0);
    for reference in &references {
        assert!(!objects.path().join("objects").join(&reference.key).exists());
        let object = ObjectRow::decode(
            &backend
                .get(&registry_object_key(reference))
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(object.state, ObjectState::Deleted);
        assert_eq!(object.memberships, 0);
        let member = MemberRow::decode(
            &backend
                .get(&registry_member_key(reference, root.incarnation))
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert!(!member.retained && member.adopted);
    }
    for key in [
        packed_current_key(binding.workspace_id),
        packed_claim_key(binding.workspace_id),
        packed_history_key(binding.workspace_id, 1),
    ] {
        assert!(backend.get(&key).await.unwrap().is_none());
    }
    let terminal_raw = backend
        .get(&registry_root_key(root.incarnation))
        .await
        .unwrap()
        .unwrap();
    let terminal = RootRow::decode(&terminal_raw).unwrap();
    assert_eq!(terminal.state, RootState::Retired);
    assert_eq!(terminal.members, 0);
    assert_eq!(terminal.binding.as_ref(), Some(&binding));
    assert_eq!(
        backend
            .get(&registry_history_root_key(&binding))
            .await
            .unwrap(),
        Some(terminal_raw.clone())
    );
    let queue = format!(
        "packed/v3/registry/history-delete-queue/{}/",
        root.incarnation.simple()
    );
    assert!(
        backend
            .scan_prefix(queue.as_bytes())
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        fresh
            .load_workspace(binding.workspace_id)
            .await
            .unwrap()
            .state,
        WorkspaceState::Deleting,
        "deleting authority remains until all queued DELETEs have completed"
    );
    let repeat = fresh
        .retire_packed_binding_history(client, budget.clone(), retirement_options(root.incarnation))
        .await
        .unwrap();
    assert!(repeat.retired && !repeat.observing);
    assert_eq!(repeat.not_before_ns, retired.not_before_ns);
    assert_eq!(repeat.released_members, 0);
    assert_eq!(repeat.deleted_objects, 0);
    assert_eq!(repeat.quarantined_objects, 0);
    assert_eq!(
        backend
            .get(&registry_root_key(root.incarnation))
            .await
            .unwrap(),
        Some(terminal_raw)
    );
    let roots = fresh
        .gc_snapshot(backend.server_time_ns().await.unwrap(), 0)
        .await
        .unwrap();
    assert!(!roots.root_layers.contains(&binding.base_revision.layer_id));
    assert!(!roots.root_layers.contains(&binding.head_layer_id));
    let gc_after = gc
        .run_at(backend.server_time_ns().await.unwrap())
        .await
        .unwrap();
    assert_eq!(gc_after.deleted_layers.len(), 2);
    for layer in [binding.base_revision.layer_id, binding.head_layer_id] {
        assert!(gc_after.deleted_layers.contains(&layer));
        assert!(
            matches!(fresh.load_layer(layer).await, Err(WorkspaceError::LayerNotFound(found)) if found == layer)
        );
        assert!(backend.get(&hot_layer_key(layer)).await.unwrap().is_none());
    }
    assert!(gc_after.deleted_slices.is_empty());
    let gc_repeat = gc
        .run_at(backend.server_time_ns().await.unwrap())
        .await
        .unwrap();
    assert!(gc_repeat.deleted_layers.is_empty());
    assert!(gc_repeat.deleted_slices.is_empty());
    eprintln!(
        "packed-v3 initial history actual adoption + reader drain + native snapshot hold + backend grace + remote DELETE + native GC passed; backend={}",
        backend.name()
    );
}

async fn isolated<B: WorkspaceKvBackend>(backend: B) {
    let backend = Arc::new(backend);
    let worker = backend.clone();
    let result = tokio::spawn(async move { contract(worker).await }).await;
    // This backend's namespace was generated below. Exact checked deletion
    // only removes this test's own rows, including after an assertion panic.
    let entries = backend.scan_prefix(b"").await.unwrap();
    for batch in entries.chunks(32) {
        let checks = batch
            .iter()
            .map(|entry| KvCheck {
                key: entry.key.clone(),
                expected: Some(entry.value.clone()),
            })
            .collect::<Vec<_>>();
        let writes = batch
            .iter()
            .map(|entry| KvWrite::Delete {
                key: entry.key.clone(),
            })
            .collect::<Vec<_>>();
        assert!(backend.compare_and_swap(&checks, &writes).await.unwrap());
    }
    assert!(backend.scan_prefix(b"").await.unwrap().is_empty());
    backend.shutdown_metadata_backend().await.unwrap();
    match result {
        Ok(()) => {}
        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        Err(error) => panic!("initial history actual contract cancelled: {error}"),
    }
}

#[tokio::test]
#[ignore = "requires BREWFS_TEST_REDIS_URL; owns UUID namespace and actual LocalFS graph"]
async fn real_redis_authenticated_initial_history_final_delete_and_native_gc() {
    let url = std::env::var("BREWFS_TEST_REDIS_URL").expect("BREWFS_TEST_REDIS_URL");
    let namespace = format!("g12-initial-history-{}", Uuid::new_v4());
    isolated(
        RedisWorkspaceBackend::connect(&url, &namespace)
            .await
            .unwrap(),
    )
    .await;
}

#[tokio::test]
#[ignore = "requires BREWFS_TEST_TIKV_PD_ENDPOINTS; owns UUID namespace and actual LocalFS graph"]
async fn real_tikv_authenticated_initial_history_final_delete_and_native_gc() {
    let Some(endpoints) = crate::test_support::tikv_endpoints() else {
        eprintln!("skip: BREWFS_TEST_TIKV_PD_ENDPOINTS is unset or empty");
        return;
    };
    let namespace = format!("g12-initial-history-{}", Uuid::new_v4());
    isolated(
        TiKvWorkspaceBackend::connect(endpoints, &namespace)
            .await
            .unwrap(),
    )
    .await;
}
