//! Deleting is an extra condition; actual grace/owners/pins/DELETE proofs remain.
//! These driver fixtures do not certify a published object graph or real backend.
use super::*;
use crate::workspace_overlay::stores::kv_store::packed_reader_pins::AcquirePackedReaderPin;

fn workspace(binding: &PackedLowerBindingRecord, state: WorkspaceState) -> WorkspaceRecord {
    WorkspaceRecord {
        upper_bucket: None,
        workspace_id: binding.workspace_id,
        head_layer_id: binding.head_layer_id,
        head_epoch: binding.head_epoch,
        fork_base: Some(binding.base_revision.clone()),
        active_lease: None,
        owner_id: Some("initial-history-test".into()),
        state,
        created_at_ns: 100,
        updated_at_ns: 1000,
    }
}

async fn initial_fixture(state: WorkspaceState, current: bool) -> Fixture {
    let mut fixture = Fixture::new().await;
    let old = fixture.root.binding.clone().unwrap();
    fixture
        .root
        .binding
        .as_mut()
        .unwrap()
        .binding
        .binding_version = 1;
    let binding = fixture.root.binding.as_ref().unwrap();
    let base = LayerRecord {
        layer_id: binding.base_revision.layer_id,
        parent_layer_id: None,
        state: LayerState::Sealed,
        schema_version: WORKSPACE_SCHEMA_VERSION,
        sealed_version: Some(binding.base_revision.sealed_version),
        delta_digest: Some([22; 32]),
        root_hash: Some(binding.base_revision.root_hash),
        depth: 1,
        owner_workspace_id: None,
        next_sequence: 1,
        owned_slice_count: 0,
        owned_bytes: 0,
        created_at_ns: 100,
        sealed_at_ns: Some(200),
    };
    let mut head = writable_layer(
        binding.head_layer_id,
        binding.base_revision.layer_id,
        2,
        binding.workspace_id,
        100,
    );
    if state == WorkspaceState::Deleting {
        head.state = LayerState::Deleting;
        head.owner_workspace_id = None;
    }
    let marker = workspace(binding, state);
    let mut rows = fixture.backend.memory.rows.lock().await;
    rows.remove(&packed_history_key(old.workspace_id, 2));
    rows.remove(&registry_history_root_key(&old));
    rows.extend([
        (
            registry_root_key(fixture.root.incarnation),
            fixture.root.encode().unwrap(),
        ),
        (
            registry_history_root_key(binding),
            fixture.root.encode().unwrap(),
        ),
        (
            packed_history_key(binding.workspace_id, 1),
            binding.encode().unwrap(),
        ),
        (
            hot_workspace_key(binding.workspace_id),
            encode(&marker).unwrap(),
        ),
        (hot_layer_key(base.layer_id), encode(&base).unwrap()),
        (hot_layer_key(head.layer_id), encode(&head).unwrap()),
    ]);
    if state != WorkspaceState::Deleting {
        // This setup added a live entity after Fixture::new's empty census.
        // Rebuild its certificate through the real migration below.
        rows.remove(HOLD_FEATURE);
        rows.remove(TEST_NATIVE_JOURNAL_HEADS_FEATURE);
    }
    if current {
        rows.extend([
            (
                packed_current_key(binding.workspace_id),
                binding.encode().unwrap(),
            ),
            (
                packed_claim_key(binding.workspace_id),
                PACKED_CLAIM.to_vec(),
            ),
        ]);
    }
    drop(rows);
    if state != WorkspaceState::Deleting {
        fixture
            .store
            .migrate_native_packed_holds(&fixture.budget, 32, CancellationToken::new())
            .await
            .unwrap();
    }
    fixture
}

async fn set_state(fixture: &Fixture, state: WorkspaceState) {
    let binding = fixture.root.binding.as_ref().unwrap();
    fixture.backend.memory.rows.lock().await.insert(
        hot_workspace_key(binding.workspace_id),
        encode(&workspace(binding, state)).unwrap(),
    );
}

#[tokio::test]
async fn packed_history_retirement_initial_deleted_workspace_grace_and_tombstone() {
    let fixture = initial_fixture(WorkspaceState::Deleting, false).await;
    fixture.backend.lose_reply.store(true, Ordering::SeqCst);
    let observing = fixture.run().await.unwrap();
    assert!(observing.observing && !observing.retired);
    assert_eq!(observing.not_before_ns, 1100);
    fixture.backend.now.store(1099, Ordering::SeqCst);
    assert_eq!(fixture.run().await.unwrap().not_before_ns, 1100);
    assert_eq!(fixture.root().await.members, 1);
    fixture.backend.now.store(1100, Ordering::SeqCst);
    let retired = fixture.run().await.unwrap();
    assert!(retired.retired);
    assert_eq!((retired.released_members, retired.deleted_objects), (1, 1));
    let binding = fixture.root.binding.as_ref().unwrap();
    assert!(
        fixture
            .backend
            .get(&packed_history_key(binding.workspace_id, 1))
            .await
            .unwrap()
            .is_none()
    );
    assert!(!fixture.objects.path().join(&fixture.reference.key).exists());
    assert_eq!(fixture.root().await.state, RootState::Retired);
    assert!(fixture.run().await.unwrap().retired);
}

#[tokio::test]
async fn packed_history_retirement_initial_exact_current_and_claim_drop_atomically() {
    let fixture = initial_fixture(WorkspaceState::Deleting, true).await;
    let binding = fixture.root.binding.as_ref().unwrap();
    let before = fixture.run().await.unwrap();
    assert!(before.observing);
    for key in [
        packed_history_key(binding.workspace_id, 1),
        packed_current_key(binding.workspace_id),
        packed_claim_key(binding.workspace_id),
    ] {
        assert!(fixture.backend.get(&key).await.unwrap().is_some());
    }
    fixture
        .backend
        .now
        .store(before.not_before_ns, Ordering::SeqCst);
    assert!(fixture.run().await.unwrap().retired);
    for key in [
        packed_history_key(binding.workspace_id, 1),
        packed_current_key(binding.workspace_id),
        packed_claim_key(binding.workspace_id),
    ] {
        assert!(fixture.backend.get(&key).await.unwrap().is_none());
    }
}

#[tokio::test]
async fn packed_history_retirement_initial_requires_exact_durable_deleting_marker() {
    for state in [
        WorkspaceState::Active,
        WorkspaceState::Quiescing,
        WorkspaceState::Sealing,
        WorkspaceState::Error,
    ] {
        let fixture = initial_fixture(state, false).await;
        let before = fixture.backend.memory.rows.lock().await.clone();
        assert!(matches!(fixture.run().await, Err(WorkspaceError::Fenced)));
        assert_eq!(*fixture.backend.memory.rows.lock().await, before);
    }
    for wrong_identity in [false, true] {
        let fixture = initial_fixture(WorkspaceState::Deleting, false).await;
        let binding = fixture.root.binding.as_ref().unwrap();
        let key = hot_workspace_key(binding.workspace_id);
        let mut rows = fixture.backend.memory.rows.lock().await;
        if wrong_identity {
            let mut marker = workspace(binding, WorkspaceState::Deleting);
            marker.workspace_id = WorkspaceId::new();
            rows.insert(key, encode(&marker).unwrap());
        } else {
            rows.remove(&key);
        }
        drop(rows);
        let before = fixture.backend.memory.rows.lock().await.clone();
        assert!(matches!(fixture.run().await, Err(WorkspaceError::Fenced)));
        assert_eq!(*fixture.backend.memory.rows.lock().await, before);
    }
}

#[tokio::test]
async fn packed_history_retirement_initial_newer_current_and_orphan_claim_are_fenced() {
    for newer_current in [false, true] {
        let fixture = initial_fixture(WorkspaceState::Deleting, false).await;
        let binding = fixture.root.binding.as_ref().unwrap();
        let mut rows = fixture.backend.memory.rows.lock().await;
        rows.insert(
            packed_claim_key(binding.workspace_id),
            PACKED_CLAIM.to_vec(),
        );
        if newer_current {
            let mut newer = binding.clone();
            newer.binding.binding_version = 2;
            let mut base: LayerRecord = decode(
                rows.get(&hot_layer_key(binding.base_revision.layer_id))
                    .unwrap(),
            )
            .unwrap();
            base.layer_id = LayerId::new();
            base.root_hash = Some([31; 32]);
            newer.base_revision = revision_from_layer(&base).unwrap();
            newer.binding.base_layer_id = base.layer_id;
            newer.head_layer_id = LayerId::new();
            newer.head_epoch += 1;
            let head = writable_layer(
                newer.head_layer_id,
                base.layer_id,
                2,
                newer.workspace_id,
                100,
            );
            rows.insert(hot_layer_key(base.layer_id), encode(&base).unwrap());
            rows.insert(hot_layer_key(head.layer_id), encode(&head).unwrap());
            rows.insert(
                hot_workspace_key(newer.workspace_id),
                encode(&workspace(&newer, WorkspaceState::Deleting)).unwrap(),
            );
            rows.insert(
                packed_current_key(binding.workspace_id),
                newer.encode().unwrap(),
            );
            rows.insert(
                packed_history_key(binding.workspace_id, 2),
                newer.encode().unwrap(),
            );
        }
        drop(rows);
        let before = fixture.backend.memory.rows.lock().await.clone();
        assert!(matches!(fixture.run().await, Err(WorkspaceError::Fenced)));
        assert_eq!(*fixture.backend.memory.rows.lock().await, before);
        assert!(fixture.objects.path().join(&fixture.reference.key).exists());
    }
}

#[tokio::test]
async fn packed_history_retirement_initial_marker_change_before_final_cas_is_busy() {
    let fixture = initial_fixture(WorkspaceState::Deleting, false).await;
    fixture.run().await.unwrap();
    fixture.backend.now.store(1100, Ordering::SeqCst);
    let entered = Arc::new(tokio::sync::Notify::new());
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    *fixture.backend.pause_next_write.lock().await = Some((entered.clone(), gate.clone()));
    let store = fixture.store.clone();
    let client = fixture.client.clone();
    let budget = fixture.budget.clone();
    let incarnation = fixture.root.incarnation;
    let waiter = tokio::spawn(async move {
        store
            .retire_packed_binding_history(client, budget, options(incarnation))
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(2), entered.notified())
        .await
        .unwrap();
    set_state(&fixture, WorkspaceState::Active).await;
    gate.add_permits(1);
    assert!(matches!(waiter.await.unwrap(), Err(WorkspaceError::Busy)));
    assert_eq!(fixture.root().await.state, RootState::BindingHistory);
    assert!(fixture.objects.path().join(&fixture.reference.key).exists());
}

#[tokio::test]
async fn packed_history_retirement_initial_cancelled_delete_pickup_rechecks_marker() {
    let fixture = initial_fixture(WorkspaceState::Deleting, false).await;
    fixture.run().await.unwrap();
    fixture.backend.now.store(1100, Ordering::SeqCst);
    let operation = options(fixture.root.incarnation);
    *fixture.backend.cancel_after_queue.lock().await = Some(operation.cancel.clone());
    assert!(matches!(
        fixture
            .store
            .retire_packed_binding_history(
                fixture.client.clone(),
                fixture.budget.clone(),
                operation
            )
            .await,
        Err(WorkspaceError::Busy)
    ));
    assert_eq!(
        (fixture.root().await.state, fixture.root().await.members),
        (RootState::Retiring, 0)
    );
    assert!(
        fixture
            .backend
            .get(&delete_queue_key(fixture.root.incarnation, 0))
            .await
            .unwrap()
            .is_some()
    );
    set_state(&fixture, WorkspaceState::Active).await;
    assert!(matches!(fixture.run().await, Err(WorkspaceError::Fenced)));
    assert!(fixture.objects.path().join(&fixture.reference.key).exists());
    set_state(&fixture, WorkspaceState::Deleting).await;
    assert!(fixture.run().await.unwrap().retired);
    assert!(!fixture.objects.path().join(&fixture.reference.key).exists());
}

#[tokio::test]
async fn packed_history_retirement_initial_snapshot_lease_and_sibling_holds_still_retain() {
    for owner_kind in 0..3 {
        let fixture = initial_fixture(WorkspaceState::Deleting, false).await;
        fixture
            .store
            .configure_packed_reader_pin_budget(fixture.budget.clone())
            .unwrap();
        let binding = fixture.root.binding.as_ref().unwrap();
        let (key, value) = match owner_kind {
            0 => {
                let snapshot = SnapshotRecord {
                    snapshot_id: SnapshotId::new(),
                    name: Some("initial-retained".into()),
                    revision: binding.base_revision.clone(),
                    owner_id: None,
                    created_at_ns: 1000,
                };
                (
                    hot_snapshot_key(snapshot.snapshot_id),
                    encode(&snapshot).unwrap(),
                )
            }
            1 => {
                let lease = SnapshotLease {
                    lease_id: LeaseId::new(),
                    workspace_id: binding.workspace_id,
                    base_revision: binding.base_revision.clone(),
                    holder_generation: 1,
                    writable: false,
                    state: LeaseState::Expired,
                    expires_at_ns: 999,
                    created_at_ns: 100,
                    updated_at_ns: 1000,
                };
                (
                    hot_lease_key(lease.workspace_id, lease.lease_id),
                    encode(&lease).unwrap(),
                )
            }
            _ => {
                let mut sibling = workspace(binding, WorkspaceState::Active);
                sibling.workspace_id = WorkspaceId::new();
                sibling.head_layer_id = LayerId::new();
                let head = writable_layer(
                    sibling.head_layer_id,
                    binding.base_revision.layer_id,
                    2,
                    sibling.workspace_id,
                    100,
                );
                fixture
                    .backend
                    .memory
                    .rows
                    .lock()
                    .await
                    .insert(hot_layer_key(head.layer_id), encode(&head).unwrap());
                (
                    hot_workspace_key(sibling.workspace_id),
                    encode(&sibling).unwrap(),
                )
            }
        };
        let mut checks = vec![KvCheck {
            key: key.clone(),
            expected: None,
        }];
        let mut writes = vec![KvWrite::Put { key, value }];
        let owner = fixture
            .store
            .prepare_native_owner_cas(&mut checks, &mut writes)
            .await
            .unwrap();
        assert!(
            fixture
                .backend
                .compare_and_swap(&checks, &writes)
                .await
                .unwrap()
        );
        drop(owner);
        let census = fixture
            .store
            .native_packed_hold_census(
                &binding.base_revision,
                options(fixture.root.incarnation).max_native_holds,
                &fixture.budget,
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(
            census.matched(),
            1,
            "the canonical native owner must retain"
        );
        drop(census);
        let before = fixture.backend.memory.rows.lock().await.clone();
        assert!(matches!(fixture.run().await, Err(WorkspaceError::Fenced)));
        assert_eq!(*fixture.backend.memory.rows.lock().await, before);
        assert!(fixture.objects.path().join(&fixture.reference.key).exists());
    }
}

#[tokio::test]
async fn packed_history_retirement_initial_persistent_reader_pin_still_retains() {
    let fixture = initial_fixture(WorkspaceState::Active, true).await;
    fixture
        .store
        .configure_packed_reader_pin_budget(fixture.budget.clone())
        .unwrap();
    let binding = fixture.root.binding.as_ref().unwrap();
    let lease = SnapshotLease {
        lease_id: LeaseId::new(),
        workspace_id: binding.workspace_id,
        base_revision: binding.base_revision.clone(),
        holder_generation: 1,
        writable: true,
        state: LeaseState::Active,
        expires_at_ns: 10000,
        created_at_ns: 100,
        updated_at_ns: 1000,
    };
    let lease_key = hot_lease_key(lease.workspace_id, lease.lease_id);
    let lease_bytes = encode(&lease).unwrap();
    let workspace_key = hot_workspace_key(binding.workspace_id);
    let workspace_before = fixture.backend.get(&workspace_key).await.unwrap().unwrap();
    let mut leased_workspace: WorkspaceRecord = decode(&workspace_before).unwrap();
    leased_workspace.active_lease = Some(lease.lease_id);
    let workspace_bytes = encode(&leased_workspace).unwrap();
    let lease_index = hot_lease_index_key(lease.lease_id);
    let mut checks = vec![
        KvCheck {
            key: lease_key.clone(),
            expected: None,
        },
        KvCheck {
            key: workspace_key.clone(),
            expected: Some(workspace_before),
        },
        KvCheck {
            key: lease_index.clone(),
            expected: None,
        },
    ];
    let mut writes = vec![
        KvWrite::Put {
            key: lease_key.clone(),
            value: lease_bytes.clone(),
        },
        KvWrite::Put {
            key: workspace_key.clone(),
            value: workspace_bytes.clone(),
        },
        KvWrite::Put {
            key: lease_index,
            value: encode(&lease.workspace_id).unwrap(),
        },
    ];
    let lease_owner = fixture
        .store
        .prepare_native_owner_cas(&mut checks, &mut writes)
        .await
        .unwrap();
    assert!(
        fixture
            .backend
            .compare_and_swap(&checks, &writes)
            .await
            .unwrap()
    );
    drop(lease_owner);
    let head: LayerRecord = decode(
        &fixture
            .backend
            .get(&hot_layer_key(binding.head_layer_id))
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    let base: LayerRecord = decode(
        &fixture
            .backend
            .get(&hot_layer_key(binding.base_revision.layer_id))
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    let request = AcquirePackedReaderPin {
        slot: 0,
        expected_slot_generation: 0,
        pin_id: Uuid::new_v4(),
        owner_id: "initial-pin-test".into(),
        holder_generation: 1,
        ttl_ns: 1000,
        gc_grace_ns: 100,
        guard: HeadGuard {
            workspace_id: binding.workspace_id,
            expected_head_layer_id: binding.head_layer_id,
            expected_head_epoch: binding.head_epoch,
            lease_id: lease.lease_id,
            holder_generation: 1,
        },
        expected_layers: [head.clone(), base],
        expected_binding: binding.clone(),
    };
    let pin = fixture
        .store
        .acquire_packed_reader_pin(&request)
        .await
        .unwrap();
    let mut released_lease = lease.clone();
    released_lease.state = LeaseState::Released;
    let mut deleting_workspace = leased_workspace;
    deleting_workspace.state = WorkspaceState::Deleting;
    deleting_workspace.active_lease = None;
    let mut checks = vec![
        KvCheck {
            key: lease_key.clone(),
            expected: Some(lease_bytes),
        },
        KvCheck {
            key: workspace_key.clone(),
            expected: Some(workspace_bytes),
        },
    ];
    let mut writes = vec![
        KvWrite::Put {
            key: lease_key,
            value: encode(&released_lease).unwrap(),
        },
        KvWrite::Put {
            key: workspace_key,
            value: encode(&deleting_workspace).unwrap(),
        },
    ];
    let lease_owner = fixture
        .store
        .prepare_native_owner_cas(&mut checks, &mut writes)
        .await
        .unwrap();
    assert!(
        fixture
            .backend
            .compare_and_swap(&checks, &writes)
            .await
            .unwrap()
    );
    drop(lease_owner);
    // The durable pin was acquired by the real pin CAS; the admin route cannot erase it.
    assert!(matches!(
        fixture.run_routed().await,
        Err(WorkspaceError::Busy)
    ));
    assert!(fixture.objects.path().join(&fixture.reference.key).exists());
    let released = fixture
        .store
        .release_packed_reader_pin(&pin, Uuid::new_v4())
        .await
        .unwrap();
    drop(released);
    assert!(fixture.run_routed().await.unwrap().observing);
}
