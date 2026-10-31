//! Native ownership projections share the actual catalog CAS and root epoch.
//! Collection never materializes a whole native namespace to infer ownership.

use super::*;
#[path = "native_holds_clean_rebuild.rs"]
mod clean_rebuild;
pub(super) mod lease_reaper;

enum NativeOwnerPreparation {
    Ready(Option<V3OwnedPermit>),
    EpochOverlap,
    BirthCompareFalse,
}

#[cfg(test)]
#[path = "native_holds_active_gate_tests.rs"]
mod active_gate_tests;

#[cfg(test)]
#[path = "native_holds_journal_heads_tests.rs"]
mod journal_heads_tests;

const HOLD_PREFIX: &[u8] = b"packed/v3/native-hold/";
pub(super) const HOLD_FEATURE: &[u8] = b"packed/v3/native-hold-feature";
const HOLD_JOURNAL_HEADS_FEATURE: &[u8] = b"packed/v3/native-hold-journal-heads";
const HOLD_LIMIT: usize = 512;
const HOLD_OPERATION_BYTES: u64 = 16 << 20;
const MAX_HOLD_CHANGES: usize = 128;
const MAX_HOLD_BIRTH_ROOT_VISITS: u64 = 8192;
const CURRENT_PREFIX: &[u8] = b"packed/v3/current/";
const CURRENT_RECORD_LIMIT: usize = 8192;

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
enum NativeHold {
    Workspace {
        id: WorkspaceId,
        head: LayerId,
        epoch: u64,
        fork: Option<BaseRevision>,
    },
    Snapshot {
        id: SnapshotId,
        revision: BaseRevision,
    },
    Lease {
        id: LeaseId,
        workspace: WorkspaceId,
        revision: BaseRevision,
        generation: u64,
        expires: i64,
    },
    Journal {
        id: JournalId,
        workspace: WorkspaceId,
        head: LayerId,
        epoch: u64,
    },
    JournalNewHead {
        id: JournalId,
        workspace: WorkspaceId,
        head: LayerId,
        epoch: u64,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workspace_overlay::stores::kv_store::packed_journal::tests::JournalMemoryBackend;

    fn sealed(id: LayerId, parent: Option<LayerId>, version: u64, hash: u8) -> LayerRecord {
        LayerRecord {
            layer_id: id,
            parent_layer_id: parent,
            state: LayerState::Sealed,
            schema_version: WORKSPACE_SCHEMA_VERSION,
            sealed_version: Some(version),
            delta_digest: Some([hash; 32]),
            root_hash: Some([hash; 32]),
            depth: if parent.is_some() { 2 } else { 1 },
            owner_workspace_id: None,
            next_sequence: 1,
            owned_slice_count: 0,
            owned_bytes: 0,
            created_at_ns: 1,
            sealed_at_ns: Some(1),
        }
    }

    fn snapshot(revision: BaseRevision) -> SnapshotRecord {
        SnapshotRecord {
            snapshot_id: SnapshotId::new(),
            name: None,
            revision,
            owner_id: None,
            created_at_ns: 1,
        }
    }

    fn lease(workspace: WorkspaceId, revision: BaseRevision) -> SnapshotLease {
        SnapshotLease {
            lease_id: LeaseId::new(),
            workspace_id: workspace,
            base_revision: revision,
            holder_generation: 1,
            writable: false,
            state: LeaseState::Expired,
            expires_at_ns: 2,
            created_at_ns: 1,
            updated_at_ns: 2,
        }
    }

    fn workspace(revision: BaseRevision) -> WorkspaceRecord {
        WorkspaceRecord {
            upper_bucket: None,
            workspace_id: WorkspaceId::new(),
            head_layer_id: LayerId::new(),
            head_epoch: 1,
            fork_base: Some(revision),
            owner_id: None,
            state: WorkspaceState::Active,
            active_lease: None,
            created_at_ns: 1,
            updated_at_ns: 1,
        }
    }

    #[tokio::test]
    async fn native_holds_snapshot_expired_lease_and_detached_fork_retain_ancestor() {
        let backend = Arc::new(JournalMemoryBackend::default());
        let store = KvWorkspaceStore::from_arc(backend.clone());
        let base = sealed(LayerId::new(), None, 1, 11);
        let child = sealed(LayerId::new(), Some(base.layer_id), 2, 22);
        let detached = sealed(LayerId::new(), None, 3, 33);
        let target = revision_from_layer(&base).unwrap();
        let source = revision_from_layer(&child).unwrap();
        {
            let mut rows = backend.rows.lock().await;
            for row in [&base, &child, &detached] {
                rows.insert(hot_layer_key(row.layer_id), encode(row).unwrap());
            }
        }
        let snapshot = NativeHold::snapshot(&snapshot(source.clone()));
        let expired = NativeHold::lease(&lease(WorkspaceId::new(), source.clone())).unwrap();
        let fork = NativeHold::Workspace {
            id: WorkspaceId::new(),
            head: detached.layer_id,
            epoch: 1,
            fork: Some(source),
        };
        for hold in [&snapshot, &expired, &fork] {
            assert!(store.hold_matches(hold, &target, &[]).await.unwrap());
        }
        // A detached current chain alone would not preserve this old fork.
        assert!(
            !store
                .hold_chain_contains(detached.layer_id, &target, &[])
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn native_holds_detached_fork_is_native_gc_root_until_workspace_deleting() {
        let backend = Arc::new(JournalMemoryBackend::default());
        let store = KvWorkspaceStore::from_arc(backend.clone());
        store
            .configure_packed_reader_pin_budget(V3MountBudget::defaults())
            .unwrap();
        let base = sealed(LayerId::new(), None, 1, 11);
        let fork = sealed(LayerId::new(), Some(base.layer_id), 2, 22);
        let detached = sealed(LayerId::new(), None, 3, 33);
        let mut workspace = workspace(revision_from_layer(&fork).unwrap());
        let head = writable_layer(
            workspace.head_layer_id,
            detached.layer_id,
            2,
            workspace.workspace_id,
            1,
        );
        let mut control = ControlState::default();
        for row in [&base, &fork, &detached, &head] {
            control.layers.insert(row.layer_id, row.clone());
        }
        control
            .workspaces
            .insert(workspace.workspace_id, workspace.clone());
        let reachable = reachable_layers(&control, 0);
        assert!(reachable.contains(&fork.layer_id));
        assert!(reachable.contains(&base.layer_id));
        assert!(reachable.contains(&head.layer_id));
        test_write_topology_rows(&mut *backend.rows.lock().await, &control);
        let snapshot = store.gc_snapshot(10, 0).await.unwrap();
        assert!(snapshot.root_layers.contains(&fork.layer_id));
        assert!(snapshot.root_layers.contains(&head.layer_id));

        workspace.state = WorkspaceState::Deleting;
        control.workspaces.insert(workspace.workspace_id, workspace);
        assert!(reachable_layers(&control, 0).is_empty());
        test_write_topology_rows(&mut *backend.rows.lock().await, &control);
        assert!(
            store
                .gc_snapshot(10, 0)
                .await
                .unwrap()
                .root_layers
                .is_empty()
        );
    }

    #[tokio::test]
    async fn native_holds_require_exact_source_revision_and_observe_overlay_deletion() {
        let backend = Arc::new(JournalMemoryBackend::default());
        let store = KvWorkspaceStore::from_arc(backend.clone());
        let base = sealed(LayerId::new(), None, 1, 11);
        let child = sealed(LayerId::new(), Some(base.layer_id), 2, 22);
        let target = revision_from_layer(&base).unwrap();
        let source = revision_from_layer(&child).unwrap();
        {
            let mut rows = backend.rows.lock().await;
            for row in [&base, &child] {
                rows.insert(hot_layer_key(row.layer_id), encode(row).unwrap());
            }
        }
        let mut stale = source.clone();
        stale.root_hash[0] ^= 1;
        assert!(matches!(
            store.hold_revision_contains(&stale, &target, &[]).await,
            Err(WorkspaceError::Fenced)
        ));
        assert!(matches!(
            store
                .hold_revision_contains(
                    &source,
                    &target,
                    &[KvWrite::Delete {
                        key: hot_layer_key(child.layer_id),
                    }]
                )
                .await,
            Err(WorkspaceError::Fenced)
        ));
    }

    #[tokio::test]
    async fn native_holds_cycle_does_not_become_a_zero_owner_answer() {
        let backend = Arc::new(JournalMemoryBackend::default());
        let store = KvWorkspaceStore::from_arc(backend.clone());
        let first_id = LayerId::new();
        let second_id = LayerId::new();
        let first = sealed(first_id, Some(second_id), 1, 11);
        let second = sealed(second_id, Some(first_id), 2, 22);
        let absent = BaseRevision {
            layer_id: LayerId::new(),
            sealed_version: 3,
            root_hash: [33; 32],
        };
        {
            let mut rows = backend.rows.lock().await;
            for row in [&first, &second] {
                rows.insert(hot_layer_key(row.layer_id), encode(row).unwrap());
            }
        }
        assert!(matches!(
            store
                .hold_revision_contains(&revision_from_layer(&first).unwrap(), &absent, &[])
                .await,
            Err(WorkspaceError::CorruptMetadata(_))
        ));
    }

    #[tokio::test]
    async fn native_holds_admit_before_decoding_entity_changes() {
        let backend = Arc::new(JournalMemoryBackend::default());
        let store = KvWorkspaceStore::from_arc(backend.clone());
        let budget = V3MountBudget::defaults();
        store
            .configure_packed_reader_pin_budget(budget.clone())
            .unwrap();
        let _occupied = budget
            .admit(&[(
                V3BudgetPool::Metadata,
                budget.capacity(V3BudgetPool::Metadata),
            )])
            .unwrap();
        let key = hot_workspace_key(WorkspaceId::new());
        let mut checks = vec![KvCheck {
            key: key.clone(),
            expected: None,
        }];
        let mut writes = vec![KvWrite::Put {
            key,
            value: vec![0; 32],
        }];
        // Without pre-admission, the malformed entity produces a decode
        // error; exhaustion must reject before that materialization begins.
        assert!(matches!(
            store
                .prepare_native_owner_cas(&mut checks, &mut writes)
                .await,
            Err(WorkspaceError::InvalidReadPlan(_))
        ));
        assert_eq!(budget.state().rejections, 1);
        assert!(backend.rows.lock().await.is_empty());
    }

    #[tokio::test]
    async fn native_holds_census_refuses_mismatched_entity_owner_identities() {
        let revision = BaseRevision {
            layer_id: LayerId::new(),
            sealed_version: 1,
            root_hash: [11; 32],
        };
        for kind in 0..3 {
            let backend = Arc::new(JournalMemoryBackend::default());
            let store = KvWorkspaceStore::from_arc(backend.clone());
            let budget = V3MountBudget::defaults();
            let mut control = ControlState::default();
            match kind {
                0 => {
                    let row = workspace(revision.clone());
                    control.workspaces.insert(WorkspaceId::new(), row);
                }
                1 => {
                    let row = snapshot(revision.clone());
                    control.snapshots.insert(SnapshotId::new(), row);
                }
                _ => {
                    let row = lease(WorkspaceId::new(), revision.clone());
                    control.leases.insert(LeaseId::new(), row);
                }
            }
            test_write_topology_rows(&mut *backend.rows.lock().await, &control);
            assert!(matches!(
                store
                    .migrate_native_packed_holds(&budget, 16, CancellationToken::new())
                    .await,
                Err(WorkspaceError::Fenced)
            ));
            let rows = backend.rows.lock().await;
            let gate = NativeHoldGate::decode(rows.get(HOLD_FEATURE).unwrap()).unwrap();
            assert!(!gate.active);
            assert!(!rows.keys().any(|key| key.starts_with(HOLD_PREFIX)));
        }
    }

    #[tokio::test]
    async fn native_holds_census_uses_current_entity_owner_values() {
        let backend = Arc::new(JournalMemoryBackend::default());
        let store = KvWorkspaceStore::from_arc(backend.clone());
        let budget = V3MountBudget::defaults();
        let old = BaseRevision {
            layer_id: LayerId::new(),
            sealed_version: 1,
            root_hash: [11; 32],
        };
        let mut control = ControlState::default();
        let old_snapshot = snapshot(old.clone());
        let mut hot_snapshot = old_snapshot.clone();
        hot_snapshot.revision = BaseRevision {
            layer_id: LayerId::new(),
            sealed_version: 2,
            root_hash: [22; 32],
        };
        let old_workspace = workspace(old.clone());
        let mut hot_workspace = old_workspace.clone();
        hot_workspace.state = WorkspaceState::Deleting;
        let old_lease = lease(old_workspace.workspace_id, old);
        let mut hot_lease = old_lease.clone();
        hot_lease.state = LeaseState::Released;
        control
            .snapshots
            .insert(old_snapshot.snapshot_id, old_snapshot);
        control
            .workspaces
            .insert(old_workspace.workspace_id, old_workspace);
        control.leases.insert(old_lease.lease_id, old_lease);
        {
            let mut rows = backend.rows.lock().await;
            test_write_topology_rows(&mut rows, &control);
            rows.insert(
                hot_snapshot_key(hot_snapshot.snapshot_id),
                encode(&hot_snapshot).unwrap(),
            );
            rows.insert(
                hot_workspace_key(hot_workspace.workspace_id),
                encode(&hot_workspace).unwrap(),
            );
            rows.insert(
                hot_lease_key(hot_lease.workspace_id, hot_lease.lease_id),
                encode(&hot_lease).unwrap(),
            );
        }
        assert_eq!(
            store
                .migrate_native_packed_holds(&budget, 16, CancellationToken::new())
                .await
                .unwrap(),
            3
        );
        let rows = backend.rows.lock().await;
        assert!(
            NativeHoldGate::decode(rows.get(HOLD_FEATURE).unwrap())
                .unwrap()
                .active
        );
        let expected = NativeHold::snapshot(&hot_snapshot);
        assert_eq!(
            NativeHold::decode(rows.get(&expected.key()).unwrap()).unwrap(),
            expected
        );
        assert_eq!(
            rows.keys()
                .filter(|key| key.starts_with(HOLD_PREFIX))
                .count(),
            1
        );
    }
    #[tokio::test]
    async fn native_holds_census_counts_ancestry_and_refuses_quota_as_zero() {
        let backend = Arc::new(JournalMemoryBackend::default());
        let store = Arc::new(KvWorkspaceStore::from_arc(backend.clone()));
        let budget = V3MountBudget::defaults();
        let base = sealed(LayerId::new(), None, 1, 11);
        let child = sealed(LayerId::new(), Some(base.layer_id), 2, 22);
        let target = revision_from_layer(&base).unwrap();
        let source = revision_from_layer(&child).unwrap();
        {
            let mut rows = backend.rows.lock().await;
            rows.insert(
                HOLD_FEATURE.to_vec(),
                encode(&NativeHoldGate {
                    run: Uuid::new_v4(),
                    active: true,
                })
                .unwrap(),
            );
            let gate = rows.get(HOLD_FEATURE).unwrap().clone();
            rows.insert(HOLD_JOURNAL_HEADS_FEATURE.to_vec(), gate);
            for row in [&base, &child] {
                rows.insert(hot_layer_key(row.layer_id), encode(row).unwrap());
            }
            for _ in 0..2 {
                let hold = NativeHold::snapshot(&snapshot(source.clone()));
                rows.insert(hold.key(), hold.encode().unwrap());
            }
        }
        let census = store
            .native_packed_hold_census(&target, 8, &budget, CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(census.matched(), 2);
        assert!(matches!(
            census.zero_owner_checks(&store, &target, &budget),
            Err(WorkspaceError::Fenced)
        ));
        assert!(matches!(
            store
                .native_packed_hold_census(&target, 1, &budget, CancellationToken::new())
                .await,
            Err(WorkspaceError::Busy)
        ));
    }

    #[tokio::test]
    async fn native_holds_census_zero_requires_active_census_and_same_context() {
        let backend = Arc::new(JournalMemoryBackend::default());
        let store = Arc::new(KvWorkspaceStore::from_arc(backend.clone()));
        let budget = V3MountBudget::defaults();
        let target = revision_from_layer(&sealed(LayerId::new(), None, 1, 11)).unwrap();
        assert!(matches!(
            store
                .native_packed_hold_census(&target, 8, &budget, CancellationToken::new())
                .await,
            Err(WorkspaceError::Fenced)
        ));
        backend.rows.lock().await.insert(
            HOLD_FEATURE.to_vec(),
            encode(&NativeHoldGate {
                run: Uuid::new_v4(),
                active: true,
            })
            .unwrap(),
        );
        {
            let mut rows = backend.rows.lock().await;
            let gate = rows.get(HOLD_FEATURE).unwrap().clone();
            rows.insert(HOLD_JOURNAL_HEADS_FEATURE.to_vec(), gate);
        }
        let census = store
            .native_packed_hold_census(&target, 8, &budget, CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(census.matched(), 0);
        assert_eq!(
            census
                .zero_owner_checks(&store, &target, &budget)
                .unwrap()
                .len(),
            4
        );
        let other_store = Arc::new(KvWorkspaceStore::from_arc(backend));
        assert!(matches!(
            census.zero_owner_checks(&other_store, &target, &budget),
            Err(WorkspaceError::Fenced)
        ));
        assert!(matches!(
            census.zero_owner_checks(&store, &target, &V3MountBudget::defaults()),
            Err(WorkspaceError::Fenced)
        ));
        let mut other_target = target;
        other_target.root_hash[0] ^= 1;
        assert!(matches!(
            census.zero_owner_checks(&store, &other_target, &budget),
            Err(WorkspaceError::Fenced)
        ));
    }

    fn current_binding(
        workspace_id: WorkspaceId,
        head_layer_id: LayerId,
        base_revision: BaseRevision,
    ) -> PackedLowerBindingRecord {
        PackedLowerBindingRecord {
            workspace_id,
            head_layer_id,
            head_epoch: 1,
            highest_inode: 400,
            binding: PackedLowerBinding {
                binding_version: 2,
                base_layer_id: base_revision.layer_id,
                manifest: V3ObjectRef {
                    key: "packed-v3/tests/current-manifest".into(),
                    kind: crate::workspace_overlay::packed_v3::wire005::V3ObjectKind::Manifest,
                    object_len: (crate::workspace_overlay::packed_v3::wire005::V3_HEADER_LEN
                        + crate::workspace_overlay::packed_v3::wire005::V3_FOOTER_LEN)
                        as u64,
                    digest: [12; 32],
                },
            },
            base_revision,
        }
    }

    async fn current_epochs(backend: &JournalMemoryBackend) -> Vec<KvCheck> {
        let values = [
            (
                HOLD_FEATURE.to_vec(),
                encode(&NativeHoldGate {
                    run: Uuid::new_v4(),
                    active: true,
                })
                .unwrap(),
            ),
            (PACKED_ROOT_GENERATION_KEY.to_vec(), encode(&1u64).unwrap()),
            (
                LAYER_INVENTORY_GENERATION_KEY.to_vec(),
                encode(&1u64).unwrap(),
            ),
        ];
        let mut rows = backend.rows.lock().await;
        rows.extend(values.iter().cloned());
        rows.insert(HOLD_JOURNAL_HEADS_FEATURE.to_vec(), values[0].1.clone());
        drop(rows);
        values
            .into_iter()
            .map(|(key, value)| KvCheck {
                key,
                expected: Some(value),
            })
            .collect()
    }

    async fn install_current(
        backend: &JournalMemoryBackend,
        binding: &PackedLowerBindingRecord,
        state: WorkspaceState,
    ) {
        let mut workspace = workspace(binding.base_revision.clone());
        workspace.workspace_id = binding.workspace_id;
        workspace.head_layer_id = binding.head_layer_id;
        workspace.state = state;
        let bytes = binding.encode().unwrap();
        backend.rows.lock().await.extend([
            (packed_current_key(binding.workspace_id), bytes.clone()),
            (
                packed_claim_key(binding.workspace_id),
                PACKED_CLAIM.to_vec(),
            ),
            (
                packed_history_key(binding.workspace_id, binding.binding.binding_version),
                bytes,
            ),
            (
                hot_workspace_key(binding.workspace_id),
                encode(&workspace).unwrap(),
            ),
        ]);
    }

    #[tokio::test]
    async fn native_current_history_census_retains_other_deleting_ancestor_and_exact_drop_checks() {
        let backend = Arc::new(JournalMemoryBackend::default());
        backend
            .page_size
            .store(1, std::sync::atomic::Ordering::SeqCst);
        let store = KvWorkspaceStore::from_arc(backend.clone());
        let budget = V3MountBudget::defaults();
        let root = sealed(LayerId::new(), None, 1, 11);
        let child = sealed(LayerId::new(), Some(root.layer_id), 2, 22);
        let target_head = sealed(LayerId::new(), Some(root.layer_id), 3, 33);
        let other_head = sealed(LayerId::new(), Some(child.layer_id), 4, 44);
        let target = current_binding(
            WorkspaceId::new(),
            target_head.layer_id,
            revision_from_layer(&root).unwrap(),
        );
        let other = current_binding(
            WorkspaceId::new(),
            other_head.layer_id,
            revision_from_layer(&child).unwrap(),
        );
        for layer in [&root, &child, &target_head, &other_head] {
            backend
                .rows
                .lock()
                .await
                .insert(hot_layer_key(layer.layer_id), encode(layer).unwrap());
        }
        install_current(&backend, &target, WorkspaceState::Deleting).await;
        install_current(&backend, &other, WorkspaceState::Deleting).await;
        let epochs = current_epochs(&backend).await;
        let census = store
            .current_packed_history_census(&target, &epochs, 8, &budget, &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(census.matched, 1);
        assert!(census.drop_target_current);
        assert_eq!(census.checks.len(), 4);
        assert!(
            store
                .backend
                .compare_and_swap(&census.checks, &[])
                .await
                .unwrap()
        );
        assert_eq!(
            backend.page_calls.load(std::sync::atomic::Ordering::SeqCst),
            3
        );
        let mut workspace: WorkspaceRecord = decode(
            backend
                .rows
                .lock()
                .await
                .get(&hot_workspace_key(target.workspace_id))
                .unwrap(),
        )
        .unwrap();
        workspace.state = WorkspaceState::Active;
        backend.rows.lock().await.insert(
            hot_workspace_key(target.workspace_id),
            encode(&workspace).unwrap(),
        );
        assert!(
            !store
                .backend
                .compare_and_swap(&census.checks, &[])
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn native_current_history_census_different_version_is_a_current_owner() {
        let backend = Arc::new(JournalMemoryBackend::default());
        let store = KvWorkspaceStore::from_arc(backend.clone());
        let budget = V3MountBudget::defaults();
        let root = sealed(LayerId::new(), None, 1, 11);
        let head = sealed(LayerId::new(), Some(root.layer_id), 2, 22);
        let target = current_binding(
            WorkspaceId::new(),
            head.layer_id,
            revision_from_layer(&root).unwrap(),
        );
        for layer in [&root, &head] {
            backend
                .rows
                .lock()
                .await
                .insert(hot_layer_key(layer.layer_id), encode(layer).unwrap());
        }
        let mut current = target.clone();
        current.binding.binding_version = 3;
        install_current(&backend, &current, WorkspaceState::Deleting).await;
        let epochs = current_epochs(&backend).await;
        let census = store
            .current_packed_history_census(&target, &epochs, 8, &budget, &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(census.matched, 1);
        assert!(!census.drop_target_current);
        assert!(census.checks.is_empty());
    }

    #[tokio::test]
    async fn native_current_history_census_other_actual_deleting_head_preserves_retention() {
        use crate::workspace_overlay::catalog::{MarkDeleting, WorkspaceStore};

        for retains_target in [false, true] {
            let backend = Arc::new(JournalMemoryBackend::default());
            backend
                .page_size
                .store(1, std::sync::atomic::Ordering::SeqCst);
            let store = KvWorkspaceStore::from_arc(backend.clone());
            let budget = V3MountBudget::defaults();
            let base = sealed(LayerId::new(), None, 1, 11);
            let other_base = sealed(
                LayerId::new(),
                retains_target.then_some(base.layer_id),
                2,
                22,
            );
            let target_owner = workspace(revision_from_layer(&base).unwrap());
            let other_owner = workspace(revision_from_layer(&other_base).unwrap());
            let target_head = writable_layer(
                target_owner.head_layer_id,
                base.layer_id,
                base.depth + 1,
                target_owner.workspace_id,
                1,
            );
            let other_head = writable_layer(
                other_owner.head_layer_id,
                other_base.layer_id,
                other_base.depth + 1,
                other_owner.workspace_id,
                1,
            );
            let target = current_binding(
                target_owner.workspace_id,
                target_head.layer_id,
                revision_from_layer(&base).unwrap(),
            );
            let other = current_binding(
                other_owner.workspace_id,
                other_head.layer_id,
                revision_from_layer(&other_base).unwrap(),
            );
            let mut control = ControlState::default();
            for layer in [&base, &other_base, &target_head, &other_head] {
                control.layers.insert(layer.layer_id, layer.clone());
                backend
                    .rows
                    .lock()
                    .await
                    .insert(hot_layer_key(layer.layer_id), encode(layer).unwrap());
            }
            for owner in [&target_owner, &other_owner] {
                control.workspaces.insert(owner.workspace_id, owner.clone());
            }
            test_write_topology_rows(&mut *backend.rows.lock().await, &control);
            install_current(&backend, &target, WorkspaceState::Active).await;
            install_current(&backend, &other, WorkspaceState::Active).await;
            store
                .migrate_native_packed_holds(&budget, 16, CancellationToken::new())
                .await
                .unwrap();
            for owner in [&target_owner, &other_owner] {
                store
                    .mark_workspace_deleting(MarkDeleting {
                        workspace_id: owner.workspace_id,
                        force_fence_lease: false,
                    })
                    .await
                    .unwrap();
                let marked = store.load_layer(owner.head_layer_id).await.unwrap();
                assert_eq!(marked.state, LayerState::Deleting);
                assert_eq!(marked.owner_workspace_id, None);
            }
            let keys = [
                HOLD_FEATURE.to_vec(),
                PACKED_ROOT_GENERATION_KEY.to_vec(),
                LAYER_INVENTORY_GENERATION_KEY.to_vec(),
            ];
            let epochs = keys
                .iter()
                .cloned()
                .zip(backend.get_many_consistent(&keys).await.unwrap())
                .map(|(key, expected)| KvCheck { key, expected })
                .collect::<Vec<_>>();
            let census = store
                .current_packed_history_census(
                    &target,
                    &epochs,
                    8,
                    &budget,
                    &CancellationToken::new(),
                )
                .await
                .unwrap();
            assert_eq!(census.matched, u64::from(retains_target));
            assert!(census.drop_target_current);
            assert_eq!(census.checks.len(), 4);
            let target_keys = [
                packed_current_key(target.workspace_id),
                packed_claim_key(target.workspace_id),
                packed_history_key(target.workspace_id, target.binding.binding_version),
                hot_workspace_key(target.workspace_id),
            ];
            assert!(
                census
                    .checks
                    .iter()
                    .all(|check| target_keys.contains(&check.key))
            );
            assert!(backend.compare_and_swap(&census.checks, &[]).await.unwrap());
            drop(census);

            // Only the exact Deleting workspace's current head gets the
            // tombstone exception. A stale head/epoch, active workspace or
            // deleted ancestor must never turn retention into a zero proof.
            let original = backend.rows.lock().await.clone();
            for corruption in 0..4 {
                *backend.rows.lock().await = original.clone();
                let mut invalid = store.load_workspace(other.workspace_id).await.unwrap();
                match corruption {
                    0 => invalid.head_layer_id = LayerId::new(),
                    1 => invalid.head_epoch += 1,
                    2 => invalid.state = WorkspaceState::Active,
                    3 => {
                        let mut ancestor = other_base.clone();
                        ancestor.state = LayerState::Deleting;
                        backend
                            .rows
                            .lock()
                            .await
                            .insert(hot_layer_key(ancestor.layer_id), encode(&ancestor).unwrap());
                    }
                    _ => unreachable!(),
                }
                backend.rows.lock().await.insert(
                    hot_workspace_key(other.workspace_id),
                    encode(&invalid).unwrap(),
                );
                assert!(matches!(
                    store
                        .current_packed_history_census(
                            &target,
                            &epochs,
                            8,
                            &budget,
                            &CancellationToken::new(),
                        )
                        .await,
                    Err(WorkspaceError::Fenced)
                ));
            }
        }
    }

    #[tokio::test]
    async fn native_current_history_census_real_mark_deleting_head_keeps_exact_drop_authority() {
        use crate::workspace_overlay::catalog::{MarkDeleting, WorkspaceStore};
        let backend = Arc::new(JournalMemoryBackend::default());
        let store = KvWorkspaceStore::from_arc(backend.clone());
        let budget = V3MountBudget::defaults();
        let base = sealed(LayerId::new(), None, 1, 11);
        let mut owner = workspace(revision_from_layer(&base).unwrap());
        let head = writable_layer(owner.head_layer_id, base.layer_id, 2, owner.workspace_id, 1);
        let target = current_binding(
            owner.workspace_id,
            head.layer_id,
            revision_from_layer(&base).unwrap(),
        );
        let mut control = ControlState::default();
        control
            .layers
            .extend([(base.layer_id, base.clone()), (head.layer_id, head.clone())]);
        control.workspaces.insert(owner.workspace_id, owner.clone());
        test_write_topology_rows(&mut *backend.rows.lock().await, &control);
        install_current(&backend, &target, WorkspaceState::Active).await;
        store
            .migrate_native_packed_holds(&budget, 16, CancellationToken::new())
            .await
            .unwrap();
        store
            .mark_workspace_deleting(MarkDeleting {
                workspace_id: owner.workspace_id,
                force_fence_lease: false,
            })
            .await
            .unwrap();
        owner = store.load_workspace(owner.workspace_id).await.unwrap();
        assert_eq!(owner.state, WorkspaceState::Deleting);
        let marked = store.load_layer(head.layer_id).await.unwrap();
        assert_eq!(marked.state, LayerState::Deleting);
        assert_eq!(marked.owner_workspace_id, None);
        let keys = [
            HOLD_FEATURE.to_vec(),
            PACKED_ROOT_GENERATION_KEY.to_vec(),
            LAYER_INVENTORY_GENERATION_KEY.to_vec(),
            HOLD_JOURNAL_HEADS_FEATURE.to_vec(),
        ];
        let epochs = keys
            .iter()
            .zip(backend.get_many_consistent(&keys).await.unwrap())
            .map(|(key, expected)| KvCheck {
                key: key.clone(),
                expected,
            })
            .collect::<Vec<_>>();
        let census = store
            .current_packed_history_census(&target, &epochs, 8, &budget, &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(census.matched, 0);
        assert!(census.drop_target_current);
        assert_eq!(census.checks.len(), 4);
        // The two physical authorities remain exact after the tombstone state
        // exception; changing the workspace/head cannot produce another zero.
        let original = backend.rows.lock().await.clone();
        for corruption in 0..4 {
            *backend.rows.lock().await = original.clone();
            let mut invalid_owner = owner.clone();
            let mut invalid_head = marked.clone();
            match corruption {
                0 => invalid_owner.head_epoch += 1,
                1 => invalid_owner.head_layer_id = LayerId::new(),
                2 => invalid_head.owner_workspace_id = Some(owner.workspace_id),
                _ => invalid_head.parent_layer_id = None,
            }
            backend.rows.lock().await.extend([
                (
                    hot_workspace_key(owner.workspace_id),
                    encode(&invalid_owner).unwrap(),
                ),
                (hot_layer_key(head.layer_id), encode(&invalid_head).unwrap()),
            ]);
            assert!(matches!(
                store
                    .current_packed_history_census(
                        &target,
                        &epochs,
                        8,
                        &budget,
                        &CancellationToken::new()
                    )
                    .await,
                Err(WorkspaceError::Fenced)
            ));
        }
        *backend.rows.lock().await = original;
        let mut other_version = target.clone();
        other_version.binding.binding_version += 1;
        install_current(&backend, &other_version, WorkspaceState::Deleting).await;
        let retained = store
            .current_packed_history_census(&target, &epochs, 8, &budget, &CancellationToken::new())
            .await
            .unwrap();
        // The later current still retains the base. It supplies no deletion
        // checks, so the retirement caller cannot consume the old target.
        assert_eq!(retained.matched, 1);
        assert!(!retained.drop_target_current);
        assert!(retained.checks.is_empty());
    }

    #[tokio::test]
    async fn native_current_history_census_cancel_quota_and_stale_epochs_never_zero() {
        let backend = Arc::new(JournalMemoryBackend::default());
        backend
            .page_size
            .store(1, std::sync::atomic::Ordering::SeqCst);
        let store = KvWorkspaceStore::from_arc(backend.clone());
        let budget = V3MountBudget::defaults();
        let root = sealed(LayerId::new(), None, 1, 11);
        let head = sealed(LayerId::new(), Some(root.layer_id), 2, 22);
        let target = current_binding(
            WorkspaceId::new(),
            head.layer_id,
            revision_from_layer(&root).unwrap(),
        );
        for layer in [&root, &head] {
            backend
                .rows
                .lock()
                .await
                .insert(hot_layer_key(layer.layer_id), encode(layer).unwrap());
        }
        for _ in 0..2 {
            let mut current = target.clone();
            current.workspace_id = WorkspaceId::new();
            install_current(&backend, &current, WorkspaceState::Deleting).await;
        }
        let epochs = current_epochs(&backend).await;
        assert!(matches!(
            store
                .current_packed_history_census(
                    &target,
                    &epochs,
                    1,
                    &budget,
                    &CancellationToken::new()
                )
                .await,
            Err(WorkspaceError::Busy)
        ));
        let cancel = CancellationToken::new();
        cancel.cancel();
        assert!(matches!(
            store
                .current_packed_history_census(&target, &epochs, 8, &budget, &cancel)
                .await,
            Err(WorkspaceError::Busy)
        ));
        assert!(matches!(
            store
                .current_packed_history_census(
                    &target,
                    &epochs[..2],
                    8,
                    &budget,
                    &CancellationToken::new()
                )
                .await,
            Err(WorkspaceError::Fenced)
        ));
        backend
            .rows
            .lock()
            .await
            .insert(PACKED_ROOT_GENERATION_KEY.to_vec(), encode(&2u64).unwrap());
        assert!(matches!(
            store
                .current_packed_history_census(
                    &target,
                    &epochs,
                    8,
                    &budget,
                    &CancellationToken::new()
                )
                .await,
            Err(WorkspaceError::Busy)
        ));
    }

    #[tokio::test]
    async fn native_current_history_census_refuses_claim_source_and_head_corruption() {
        for case in 0..3 {
            let backend = Arc::new(JournalMemoryBackend::default());
            let store = KvWorkspaceStore::from_arc(backend.clone());
            let budget = V3MountBudget::defaults();
            let root = sealed(LayerId::new(), None, 1, 11);
            let mut head = sealed(LayerId::new(), Some(root.layer_id), 2, 22);
            let target = current_binding(
                WorkspaceId::new(),
                head.layer_id,
                revision_from_layer(&root).unwrap(),
            );
            let mut current = target.clone();
            if case == 1 {
                current.base_revision.sealed_version += 1;
            }
            if case == 2 {
                head.parent_layer_id = Some(head.layer_id);
            }
            for layer in [&root, &head] {
                backend
                    .rows
                    .lock()
                    .await
                    .insert(hot_layer_key(layer.layer_id), encode(layer).unwrap());
            }
            install_current(&backend, &current, WorkspaceState::Deleting).await;
            if case == 0 {
                backend
                    .rows
                    .lock()
                    .await
                    .remove(&packed_claim_key(current.workspace_id));
            }
            let epochs = current_epochs(&backend).await;
            assert!(
                store
                    .current_packed_history_census(
                        &target,
                        &epochs,
                        8,
                        &budget,
                        &CancellationToken::new()
                    )
                    .await
                    .is_err()
            );
        }
    }

    #[tokio::test]
    async fn native_current_history_census_refuses_disconnected_head_and_base() {
        let backend = Arc::new(JournalMemoryBackend::default());
        let store = KvWorkspaceStore::from_arc(backend.clone());
        let budget = V3MountBudget::defaults();
        let target_root = sealed(LayerId::new(), None, 1, 11);
        let declared_base = sealed(LayerId::new(), None, 2, 22);
        let actual_base = sealed(LayerId::new(), None, 3, 33);
        let disconnected_head = sealed(LayerId::new(), Some(actual_base.layer_id), 4, 44);
        let target = current_binding(
            WorkspaceId::new(),
            LayerId::new(),
            revision_from_layer(&target_root).unwrap(),
        );
        let current = current_binding(
            WorkspaceId::new(),
            disconnected_head.layer_id,
            revision_from_layer(&declared_base).unwrap(),
        );
        for layer in [
            &target_root,
            &declared_base,
            &actual_base,
            &disconnected_head,
        ] {
            backend
                .rows
                .lock()
                .await
                .insert(hot_layer_key(layer.layer_id), encode(layer).unwrap());
        }
        install_current(&backend, &current, WorkspaceState::Deleting).await;
        let epochs = current_epochs(&backend).await;
        assert!(matches!(
            store
                .current_packed_history_census(
                    &target,
                    &epochs,
                    8,
                    &budget,
                    &CancellationToken::new()
                )
                .await,
            Err(WorkspaceError::Fenced)
        ));
    }

    #[tokio::test]
    async fn native_holds_births_share_one_total_root_visit_limit() {
        let backend = Arc::new(JournalMemoryBackend::default());
        let store = KvWorkspaceStore::from_arc(backend.clone());
        let budget = V3MountBudget::defaults();
        store.configure_packed_reader_pin_budget(budget).unwrap();
        current_epochs(&backend).await;
        {
            let mut rows = backend.rows.lock().await;
            for _ in 0..MAX_HOLD_BIRTH_ROOT_VISITS / 2 + 1 {
                let root = RootRow {
                    journal_id: JournalId::new(),
                    incarnation: Uuid::new_v4(),
                    revision: 1,
                    state: RootState::Retired,
                    members: 0,
                    pending_puts: 0,
                    binding: None,
                };
                rows.insert(registry_root_key(root.incarnation), root.encode().unwrap());
            }
        }
        let layer = sealed(LayerId::new(), None, 1, 11);
        backend
            .rows
            .lock()
            .await
            .insert(hot_layer_key(layer.layer_id), encode(&layer).unwrap());
        let source = revision_from_layer(&layer).unwrap();
        let snapshots = [snapshot(source.clone()), snapshot(source)];
        let mut checks = snapshots
            .iter()
            .map(|row| KvCheck {
                key: hot_snapshot_key(row.snapshot_id),
                expected: None,
            })
            .collect::<Vec<_>>();
        let mut writes = snapshots
            .iter()
            .map(|row| put(hot_snapshot_key(row.snapshot_id), row).unwrap())
            .collect::<Vec<_>>();
        assert!(matches!(
            store
                .prepare_native_owner_cas(&mut checks, &mut writes)
                .await,
            Err(WorkspaceError::Busy)
        ));
        let rows = backend.rows.lock().await;
        for row in &snapshots {
            assert!(!rows.contains_key(&hot_snapshot_key(row.snapshot_id)));
            assert!(!rows.contains_key(&NativeHold::snapshot(row).key()));
        }
        assert_eq!(
            decode::<u64>(rows.get(PACKED_ROOT_GENERATION_KEY).unwrap()).unwrap(),
            1
        );
    }
}

impl NativeHold {
    fn workspace(row: &WorkspaceRecord) -> Option<Self> {
        (row.state != WorkspaceState::Deleting).then(|| Self::Workspace {
            id: row.workspace_id,
            head: row.head_layer_id,
            epoch: row.head_epoch,
            fork: row.fork_base.clone(),
        })
    }
    fn snapshot(row: &SnapshotRecord) -> Self {
        Self::Snapshot {
            id: row.snapshot_id,
            revision: row.revision.clone(),
        }
    }
    fn lease(row: &SnapshotLease) -> Option<Self> {
        // Expired remains a hold until the separate backend-clock grace reaper.
        (row.state != LeaseState::Released).then(|| Self::Lease {
            id: row.lease_id,
            workspace: row.workspace_id,
            revision: row.base_revision.clone(),
            generation: row.holder_generation,
            expires: row.expires_at_ns,
        })
    }
    fn journal(row: &SealJournal) -> Option<Self> {
        (!matches!(row.phase, SealPhase::Completed | SealPhase::Aborted)).then_some(Self::Journal {
            id: row.journal_id,
            workspace: row.workspace_id,
            head: row.old_head_layer_id,
            epoch: row.expected_head_epoch,
        })
    }
    fn journal_new_head(row: &SealJournal) -> Option<Self> {
        (!matches!(row.phase, SealPhase::Completed | SealPhase::Aborted))
            .then_some(row.new_head_layer_id)
            .flatten()
            .map(|head| Self::JournalNewHead {
                id: row.journal_id,
                workspace: row.workspace_id,
                head,
                epoch: row.expected_head_epoch,
            })
    }
    fn key(&self) -> Vec<u8> {
        let suffix = match self {
            Self::Workspace { id, .. } => format!("workspace/{id}"),
            Self::Snapshot { id, .. } => format!("snapshot/{id}"),
            Self::Lease { id, .. } => format!("lease/{id}"),
            Self::Journal { id, .. } => format!("journal/{id}"),
            Self::JournalNewHead { id, .. } => format!("journal-new-head/{id}"),
        };
        [HOLD_PREFIX, suffix.as_bytes()].concat()
    }
    fn encode(&self) -> Result<Vec<u8>, WorkspaceError> {
        let revision_valid = |revision: &BaseRevision| {
            !revision.layer_id.as_uuid().is_nil() && revision.sealed_version > 0
        };
        let valid = match self {
            Self::Workspace { id, head, fork, .. } => {
                !id.as_uuid().is_nil()
                    && !head.as_uuid().is_nil()
                    && fork.as_ref().is_none_or(revision_valid)
            }
            Self::Snapshot { id, revision } => !id.as_uuid().is_nil() && revision_valid(revision),
            Self::Lease {
                id,
                workspace,
                revision,
                generation,
                expires,
            } => {
                !id.as_uuid().is_nil()
                    && !workspace.as_uuid().is_nil()
                    && revision_valid(revision)
                    && *generation > 0
                    && *expires > 0
            }
            Self::Journal {
                id,
                workspace,
                head,
                ..
            }
            | Self::JournalNewHead {
                id,
                workspace,
                head,
                ..
            } => {
                !id.as_uuid().is_nil() && !workspace.as_uuid().is_nil() && !head.as_uuid().is_nil()
            }
        };
        if !valid {
            return Err(journal_error("invalid native hold identity"));
        }
        let raw = encode(self)?;
        if raw.len() > HOLD_LIMIT {
            return Err(journal_error("native hold exceeds schema"));
        }
        Ok(raw)
    }
    fn decode(raw: &[u8]) -> Result<Self, WorkspaceError> {
        let result: Self = decode_open_value(raw, HOLD_LIMIT)?;
        result.encode()?;
        Ok(result)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
struct NativeHoldGate {
    run: Uuid,
    active: bool,
}
impl NativeHoldGate {
    fn decode(raw: &[u8]) -> Result<Self, WorkspaceError> {
        let gate: Self = decode_open_value(raw, 128)?;
        if gate.run.is_nil() {
            return Err(journal_error("nil native hold census"));
        }
        Ok(gate)
    }
}

pub(super) fn require_active_native_hold_retirement_gate(
    raw: &Option<Vec<u8>>,
) -> Result<(), WorkspaceError> {
    let gate = NativeHoldGate::decode(raw.as_deref().ok_or(WorkspaceError::Fenced)?)?;
    if !gate.active {
        return Err(WorkspaceError::Fenced);
    }
    Ok(())
}

struct HoldChange {
    source_key: Vec<u8>,
    key: Vec<u8>,
    old: Option<NativeHold>,
    new: Option<NativeHold>,
}

fn append_hold_change(
    changes: &mut Vec<HoldChange>,
    change: HoldChange,
) -> Result<(), WorkspaceError> {
    if changes.len() >= MAX_HOLD_CHANGES {
        return Err(WorkspaceError::Busy);
    }
    changes.push(change);
    Ok(())
}

/// A complete native-sidecar traversal under one actual ownership/layer epoch.
/// This certifies native ownership only; packed pins/PPJ/current are separate.
pub(super) struct NativeHoldCensus<B> {
    store: Arc<KvWorkspaceStore<B>>,
    target: BaseRevision,
    matched: u64,
    checks: Vec<KvCheck>,
    budget: Arc<V3MountBudget>,
    _permit: V3OwnedPermit,
}

impl<B: WorkspaceKvBackend> NativeHoldCensus<B> {
    pub(super) fn matched(&self) -> u64 {
        self.matched
    }

    pub(super) fn zero_owner_checks(
        &self,
        store: &Arc<KvWorkspaceStore<B>>,
        target: &BaseRevision,
        budget: &Arc<V3MountBudget>,
    ) -> Result<&[KvCheck], WorkspaceError> {
        if self.matched != 0
            || &self.target != target
            || !Arc::ptr_eq(store, &self.store)
            || !Arc::ptr_eq(budget, &self.budget)
            || budget.state().closed
        {
            return Err(WorkspaceError::Fenced);
        }
        Ok(&self.checks)
    }
}

/// Complete current-binding traversal under the caller's retained owner epoch.
/// Only an exact target in its Deleting workspace may be removed by final CAS.
pub(super) struct PackedCurrentHistoryCensus {
    pub(super) checks: Vec<KvCheck>,
    pub(super) matched: u64,
    pub(super) drop_target_current: bool,
    pub(super) _permit: V3OwnedPermit,
}

fn hold_limits(records: usize) -> KvReadLimits {
    KvReadLimits {
        max_records: records,
        max_key_bytes: 1024,
        max_value_bytes: OPEN_RECORD_MAX_BYTES,
        max_total_bytes: 512 << 10,
        max_response_bytes: 16 << 10,
        max_data_requests: 1024,
    }
}
fn hold_pages() -> KvReadLimits {
    KvReadLimits {
        max_records: 32,
        ..hold_limits(32)
    }
}
fn root_pages() -> KvReadLimits {
    KvReadLimits {
        max_records: 32,
        max_key_bytes: 1024,
        max_value_bytes: REGISTRY_RECORD_LIMIT,
        max_total_bytes: 512 << 10,
        max_response_bytes: 512 << 10,
        max_data_requests: 1024,
    }
}
fn merge(
    checks: &mut Vec<KvCheck>,
    added: impl IntoIterator<Item = KvCheck>,
) -> Result<(), WorkspaceError> {
    for next in added {
        if let Some(old) = checks.iter().find(|old| old.key == next.key) {
            if old.expected != next.expected {
                #[cfg(test)]
                eprintln!(
                    "[packed-v3-native-begin-diag] stage=native-owner-overlap key={}",
                    String::from_utf8_lossy(&next.key)
                );
                return Err(WorkspaceError::Busy);
            }
        } else {
            checks.push(next);
        }
    }
    Ok(())
}
fn hot_projection(key: &[u8], raw: Option<&[u8]>) -> Result<Option<NativeHold>, WorkspaceError> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    let result = if key.starts_with(HOT_WORKSPACE_PREFIX) {
        let row: WorkspaceRecord = decode_open_value(raw, OPEN_RECORD_MAX_BYTES)?;
        if key != hot_workspace_key(row.workspace_id) {
            return Err(WorkspaceError::Fenced);
        }
        NativeHold::workspace(&row)
    } else if key.starts_with(HOT_SNAPSHOT_PREFIX) {
        let row: SnapshotRecord = decode_open_value(raw, OPEN_RECORD_MAX_BYTES)?;
        if key != hot_snapshot_key(row.snapshot_id) {
            return Err(WorkspaceError::Fenced);
        }
        Some(NativeHold::snapshot(&row))
    } else if key.starts_with(HOT_LEASE_PREFIX) {
        let row: SnapshotLease = decode_open_value(raw, OPEN_RECORD_MAX_BYTES)?;
        if key != hot_lease_key(row.workspace_id, row.lease_id) {
            return Err(WorkspaceError::Fenced);
        }
        NativeHold::lease(&row)
    } else {
        return Err(journal_error("invalid native owner projection prefix"));
    };
    Ok(result)
}

fn owner_changes(
    checks: &[KvCheck],
    writes: &[KvWrite],
) -> Result<Vec<HoldChange>, WorkspaceError> {
    let mut changes = Vec::new();
    for write in writes {
        let (key, new_raw) = match write {
            KvWrite::Put { key, value } => (key, Some(value.as_slice())),
            KvWrite::Delete { key } => (key, None),
        };
        if key.starts_with(HOT_WORKSPACE_PREFIX)
            || key.starts_with(HOT_SNAPSHOT_PREFIX)
            || key.starts_with(HOT_LEASE_PREFIX)
        {
            let old = checks
                .iter()
                .find(|check| check.key == *key)
                .ok_or(WorkspaceError::Fenced)?;
            let old = hot_projection(key, old.expected.as_deref())?;
            let new = hot_projection(key, new_raw)?;
            if old != new {
                let identity = new
                    .as_ref()
                    .or(old.as_ref())
                    .ok_or(WorkspaceError::Fenced)?;
                append_hold_change(
                    &mut changes,
                    HoldChange {
                        source_key: key.clone(),
                        key: identity.key(),
                        old,
                        new,
                    },
                )?;
            }
        } else if key.starts_with(HOT_JOURNAL_PREFIX) {
            let old = checks
                .iter()
                .find(|check| check.key == *key)
                .ok_or(WorkspaceError::Fenced)?;
            let read_journal = |raw: Option<&[u8]>| -> Result<Option<SealJournal>, WorkspaceError> {
                let Some(raw) = raw else {
                    return Ok(None);
                };
                let row: SealJournal = decode_open_value(raw, OPEN_RECORD_MAX_BYTES)?;
                if key != &hot_journal_key(row.workspace_id, row.journal_id) {
                    return Err(WorkspaceError::Fenced);
                }
                Ok(Some(row))
            };
            let old = read_journal(old.expected.as_deref())?;
            let new = read_journal(new_raw)?;
            for projection in [NativeHold::journal, NativeHold::journal_new_head] {
                let previous = old.as_ref().and_then(projection);
                let next = new.as_ref().and_then(projection);
                if previous != next
                    && let Some(identity) = next.as_ref().or(previous.as_ref())
                {
                    append_hold_change(
                        &mut changes,
                        HoldChange {
                            source_key: key.clone(),
                            key: identity.key(),
                            old: previous,
                            new: next,
                        },
                    )?;
                }
            }
        }
        if changes.len() > MAX_HOLD_CHANGES {
            return Err(WorkspaceError::Busy);
        }
    }
    Ok(changes)
}

impl<B: WorkspaceKvBackend> KvWorkspaceStore<B> {
    /// Empty pages, quota exhaustion and malformed metadata are distinct.
    /// A proof is issued only after the actual empty terminal page and epoch CAS.
    pub(super) async fn native_packed_hold_census(
        self: &Arc<Self>,
        target: &BaseRevision,
        max_rows: u64,
        budget: &Arc<V3MountBudget>,
        cancel: CancellationToken,
    ) -> Result<NativeHoldCensus<B>, WorkspaceError> {
        self.configure_packed_reader_pin_budget(budget.clone())?;
        let permit = budget
            .admit(&[(V3BudgetPool::Metadata, 2 << 20)])
            .map_err(journal_budget_error)?;
        if max_rows == 0
            || max_rows > MAX_OBJECTS
            || target.layer_id.as_uuid().is_nil()
            || target.sealed_version == 0
            || budget.state().closed
            || cancel.is_cancelled()
        {
            return Err(WorkspaceError::Fenced);
        }
        let keys = [
            HOLD_FEATURE.to_vec(),
            PACKED_ROOT_GENERATION_KEY.to_vec(),
            LAYER_INVENTORY_GENERATION_KEY.to_vec(),
            HOLD_JOURNAL_HEADS_FEATURE.to_vec(),
        ];
        let (values, _) = self
            .backend
            .get_many_consistent_with_time_bounded(&keys, hold_limits(keys.len()))
            .await?;
        if values.len() != keys.len() {
            return Err(journal_error("short native ownership census basis"));
        }
        let gate = NativeHoldGate::decode(values[0].as_deref().ok_or(WorkspaceError::Fenced)?)?;
        if !gate.active || values[3] != values[0] {
            return Err(WorkspaceError::Fenced);
        }
        next_packed_root_generation(&values[1])?;
        layer_inventory_generation(&values[2])?;
        let checks = keys
            .into_iter()
            .zip(values)
            .map(|(key, expected)| KvCheck { key, expected })
            .collect::<Vec<_>>();
        let mut after = None;
        let mut visited = 0;
        let mut matched = 0;
        loop {
            if cancel.is_cancelled() || budget.state().closed {
                return Err(WorkspaceError::Busy);
            }
            if !self.backend.compare_and_swap(&checks, &[]).await? {
                return Err(WorkspaceError::Busy);
            }
            let page = self
                .backend
                .scan_prefix_page_with_byte_limits(HOLD_PREFIX, after.as_deref(), hold_pages())
                .await?;
            if !self.backend.compare_and_swap(&checks, &[]).await? {
                return Err(WorkspaceError::Busy);
            }
            if page.is_empty() {
                return Ok(NativeHoldCensus {
                    store: self.clone(),
                    target: target.clone(),
                    matched,
                    checks,
                    budget: budget.clone(),
                    _permit: permit,
                });
            }
            for entry in page {
                if cancel.is_cancelled() || budget.state().closed {
                    return Err(WorkspaceError::Busy);
                }
                if !entry.key.starts_with(HOLD_PREFIX)
                    || after.as_ref().is_some_and(|last| entry.key <= *last)
                {
                    return Err(WorkspaceError::Fenced);
                }
                visited = increment(visited)?;
                if visited > max_rows {
                    return Err(WorkspaceError::Busy);
                }
                let hold = NativeHold::decode(&entry.value)?;
                if entry.key != hold.key() {
                    return Err(WorkspaceError::Fenced);
                }
                if self.hold_matches(&hold, target, &[]).await? {
                    matched = increment(matched)?;
                }
                // These ancestry reads may use later backend snapshots. The
                // exact layer epoch must still be unchanged before proceeding.
                if !self.backend.compare_and_swap(&checks, &[]).await? {
                    return Err(WorkspaceError::Busy);
                }
                after = Some(entry.key);
            }
        }
    }

    pub(super) async fn current_packed_history_census(
        &self,
        target: &PackedLowerBindingRecord,
        epochs: &[KvCheck],
        max_rows: u64,
        budget: &Arc<V3MountBudget>,
        cancel: &CancellationToken,
    ) -> Result<PackedCurrentHistoryCensus, WorkspaceError> {
        self.configure_packed_reader_pin_budget(budget.clone())?;
        let permit = budget
            .admit(&[(V3BudgetPool::Metadata, 2 << 20)])
            .map_err(journal_budget_error)?;
        if max_rows == 0 || max_rows > MAX_OBJECTS || budget.state().closed || cancel.is_cancelled()
        {
            return Err(WorkspaceError::Busy);
        }
        target.encode()?;
        for key in [
            HOLD_FEATURE,
            PACKED_ROOT_GENERATION_KEY,
            LAYER_INVENTORY_GENERATION_KEY,
        ] {
            let mut found = epochs.iter().filter(|check| check.key.as_slice() == key);
            let check = found.next().ok_or(WorkspaceError::Fenced)?;
            if found.next().is_some() {
                return Err(WorkspaceError::Fenced);
            }
            if key == HOLD_FEATURE {
                let gate = NativeHoldGate::decode(
                    check.expected.as_deref().ok_or(WorkspaceError::Fenced)?,
                )?;
                if !gate.active {
                    return Err(WorkspaceError::Fenced);
                }
            } else if key == PACKED_ROOT_GENERATION_KEY {
                next_packed_root_generation(&check.expected)?;
            } else {
                layer_inventory_generation(&check.expected)?;
            }
        }
        let mut after = None;
        let mut visited = 0;
        let mut matched = 0;
        let mut checks = Vec::new();
        let mut drop_target_current = false;
        loop {
            if budget.state().closed || cancel.is_cancelled() {
                return Err(WorkspaceError::Busy);
            }
            if !self.backend.compare_and_swap(epochs, &[]).await? {
                return Err(WorkspaceError::Busy);
            }
            let page = self
                .backend
                .scan_prefix_page_with_byte_limits(
                    CURRENT_PREFIX,
                    after.as_deref(),
                    KvReadLimits {
                        max_records: 32,
                        max_key_bytes: 1024,
                        max_value_bytes: CURRENT_RECORD_LIMIT,
                        max_total_bytes: 512 << 10,
                        max_response_bytes: 512 << 10,
                        max_data_requests: 1024,
                    },
                )
                .await?;
            if !self.backend.compare_and_swap(epochs, &[]).await? {
                return Err(WorkspaceError::Busy);
            }
            if budget.state().closed || cancel.is_cancelled() {
                return Err(WorkspaceError::Busy);
            }
            if page.is_empty() {
                return Ok(PackedCurrentHistoryCensus {
                    checks,
                    matched,
                    drop_target_current,
                    _permit: permit,
                });
            }
            for entry in page {
                if budget.state().closed || cancel.is_cancelled() {
                    return Err(WorkspaceError::Busy);
                }
                if !entry.key.starts_with(CURRENT_PREFIX)
                    || after.as_ref().is_some_and(|last| entry.key <= *last)
                {
                    return Err(WorkspaceError::Fenced);
                }
                visited = increment(visited)?;
                if visited > max_rows {
                    return Err(WorkspaceError::Busy);
                }
                let binding = PackedLowerBindingRecord::decode(&entry.value)?;
                if entry.key != packed_current_key(binding.workspace_id) {
                    return Err(WorkspaceError::Fenced);
                }
                let keys = [
                    entry.key.clone(),
                    packed_claim_key(binding.workspace_id),
                    packed_history_key(binding.workspace_id, binding.binding.binding_version),
                    hot_workspace_key(binding.workspace_id),
                ];
                let (values, _) = self
                    .backend
                    .get_many_consistent_with_time_bounded(
                        &keys,
                        KvReadLimits {
                            max_records: keys.len(),
                            max_key_bytes: 1024,
                            max_value_bytes: CURRENT_RECORD_LIMIT,
                            max_total_bytes: 32 << 10,
                            max_response_bytes: 64 << 10,
                            max_data_requests: keys.len(),
                        },
                    )
                    .await?;
                if values.len() != keys.len() {
                    return Err(journal_error("short current history census read"));
                }
                if values[0].as_deref() != Some(entry.value.as_slice()) {
                    return Err(WorkspaceError::Busy);
                }
                if decode_packed_pair(binding.workspace_id, &values[0], &values[1], &values[2])?
                    .as_ref()
                    != Some(&binding)
                {
                    return Err(WorkspaceError::Fenced);
                }
                let workspace: WorkspaceRecord = decode_open_value(
                    values[3].as_deref().ok_or(WorkspaceError::Fenced)?,
                    OPEN_RECORD_MAX_BYTES,
                )?;
                if workspace.workspace_id != binding.workspace_id {
                    return Err(WorkspaceError::Fenced);
                }
                let allow_deleting_head = workspace.state == WorkspaceState::Deleting;
                let may_drop = &binding == target && allow_deleting_head;
                if allow_deleting_head
                    && (workspace.head_layer_id != binding.head_layer_id
                        || workspace.head_epoch != binding.head_epoch)
                {
                    return Err(WorkspaceError::Fenced);
                }
                if !self
                    .hold_chain_contains_inner(
                        binding.head_layer_id,
                        None,
                        &binding.base_revision,
                        &[],
                        allow_deleting_head,
                    )
                    .await?
                {
                    return Err(WorkspaceError::Fenced);
                }
                // Both walks validate their sources, even when the base alone
                // already proves retention. A malformed head is never zero.
                let base_contains = self
                    .hold_revision_contains(&binding.base_revision, &target.base_revision, &[])
                    .await?;
                let head_contains = self
                    .hold_chain_contains_inner(
                        binding.head_layer_id,
                        None,
                        &target.base_revision,
                        &[],
                        allow_deleting_head,
                    )
                    .await?;
                if !self.backend.compare_and_swap(epochs, &[]).await? {
                    return Err(WorkspaceError::Busy);
                }
                if budget.state().closed || cancel.is_cancelled() {
                    return Err(WorkspaceError::Busy);
                }
                if may_drop {
                    // This one exact current/claim may be consumed by the same
                    // final history CAS. Other Deleting workspaces still retain
                    // every graph their actual current ancestry can depend on.
                    if !base_contains || drop_target_current {
                        return Err(WorkspaceError::Fenced);
                    }
                    merge(
                        &mut checks,
                        keys.into_iter()
                            .zip(values)
                            .map(|(key, expected)| KvCheck { key, expected }),
                    )?;
                    drop_target_current = true;
                } else if base_contains || head_contains {
                    matched = increment(matched)?;
                }
                after = Some(entry.key);
            }
        }
    }

    /// Append sidecars and the root epoch to the *same* actual native CAS.
    /// The owned permit returned here must survive the caller's transport.
    pub(crate) async fn prepare_native_owner_cas(
        &self,
        checks: &mut Vec<KvCheck>,
        writes: &mut Vec<KvWrite>,
    ) -> Result<Option<V3OwnedPermit>, WorkspaceError> {
        let (epochs_match, owner) = self
            .prepare_native_owner_cas_with_epoch_status(checks, writes)
            .await?;
        if !epochs_match {
            return Err(WorkspaceError::Busy);
        }
        Ok(owner)
    }

    /// Return false only for an exact epoch overlap before any submitted
    /// mutation. Every other failure retains its original error classification.
    pub(crate) async fn prepare_native_owner_cas_with_epoch_status(
        &self,
        checks: &mut Vec<KvCheck>,
        writes: &mut Vec<KvWrite>,
    ) -> Result<(bool, Option<V3OwnedPermit>), WorkspaceError> {
        let mut birth_root_visits = 0;
        match self
            .prepare_native_owner_cas_inner(checks, writes, &mut birth_root_visits, None)
            .await?
        {
            NativeOwnerPreparation::Ready(owner) => Ok((true, owner)),
            NativeOwnerPreparation::EpochOverlap => Ok((false, None)),
            NativeOwnerPreparation::BirthCompareFalse => Err(WorkspaceError::Busy),
        }
    }

    async fn prepare_native_owner_cas_inner(
        &self,
        checks: &mut Vec<KvCheck>,
        writes: &mut Vec<KvWrite>,
        birth_root_visits: &mut u64,
        retained_operation_owner: Option<&V3OwnedPermit>,
    ) -> Result<NativeOwnerPreparation, WorkspaceError> {
        // This structural probe performs no decode or clone. Admission precedes
        // decoding the independently stored ownership records.
        if !writes.iter().any(|write| {
            let key = match write {
                KvWrite::Put { key, .. } | KvWrite::Delete { key } => key,
            };
            key.starts_with(HOT_WORKSPACE_PREFIX)
                || key.starts_with(HOT_SNAPSHOT_PREFIX)
                || key.starts_with(HOT_LEASE_PREFIX)
                || key.starts_with(HOT_JOURNAL_PREFIX)
        }) {
            return Ok(NativeOwnerPreparation::Ready(None));
        }
        let owner = if retained_operation_owner.is_some() {
            None
        } else {
            self.packed_reader_pin_budget
                .get()
                .map(|budget| {
                    budget
                        .admit(&[(V3BudgetPool::Metadata, HOLD_OPERATION_BYTES)])
                        .map_err(journal_budget_error)
                })
                .transpose()?
        };
        let keys = [
            HOLD_FEATURE.to_vec(),
            PACKED_ROOT_GENERATION_KEY.to_vec(),
            LAYER_INVENTORY_GENERATION_KEY.to_vec(),
        ];
        let (values, _) = self
            .backend
            .get_many_consistent_with_time_bounded(&keys, hold_limits(keys.len()))
            .await?;
        if values.len() != keys.len() {
            return Err(journal_error("short native owner epoch read"));
        }
        let active = values[0]
            .as_deref()
            .map(NativeHoldGate::decode)
            .transpose()?
            .is_some_and(|gate| gate.active);
        if active && owner.is_none() && retained_operation_owner.is_none() {
            return Err(WorkspaceError::UnsupportedCapability(
                "canonical native hold budget",
            ));
        }
        let changes = owner_changes(checks, writes)?;
        if changes.is_empty() {
            return Ok(NativeOwnerPreparation::Ready(None));
        }
        let next_generation = next_packed_root_generation(&values[1])?;
        layer_inventory_generation(&values[2])?;
        for (key, expected) in keys.iter().zip(&values) {
            if checks
                .iter()
                .find(|old| &old.key == key)
                .is_some_and(|old| old.expected.as_ref() != expected.as_ref())
            {
                #[cfg(test)]
                eprintln!(
                    "[packed-v3-native-begin-diag] stage=native-owner-epoch-overlap key={}",
                    String::from_utf8_lossy(key)
                );
                // No mutation has been sent and no hold writes have been added.
                // Only this explicit stale epoch result can request a reread.
                return Ok(NativeOwnerPreparation::EpochOverlap);
            }
        }
        merge(
            checks,
            keys.iter()
                .cloned()
                .zip(values.iter().cloned())
                .map(|(key, expected)| KvCheck { key, expected }),
        )?;
        // Every native root birth is fenced by the exact layer chain, even
        // before packed hold migration. A census of retiring packed roots is
        // an additional check; it cannot establish this anti-birth invariant.
        for change in &changes {
            if let Some(new) = &change.new {
                self.authenticate_native_hold_birth_layers(new, checks, writes)
                    .await?;
            }
        }
        for change in changes {
            let key = [change.key.clone()];
            let (actual, _) = self
                .backend
                .get_many_consistent_with_time_bounded(&key, hold_limits(1))
                .await?;
            if actual.len() != 1 {
                return Err(journal_error("short native hold read"));
            }
            let observed = actual[0].as_deref().map(NativeHold::decode).transpose()?;
            if observed
                .as_ref()
                .is_some_and(|hold| hold.key() != change.key)
                || (observed.is_some() && observed != change.old)
                || (active && observed != change.old)
            {
                // A lease heartbeat (or another supported topology writer)
                // may have advanced the source hot row and its derived hold
                // after this packet was prepared. Recheck that source row
                // before classifying the hold mismatch: a changed source is
                // an epoch overlap and must rebuild, while an unchanged source
                // with a different hold is genuine authority corruption.
                let source = checks
                    .iter()
                    .find(|check| check.key == change.source_key)
                    .ok_or(WorkspaceError::Fenced)?;
                let (source_values, _) = self
                    .backend
                    .get_many_consistent_with_time_bounded(
                        std::slice::from_ref(&change.source_key),
                        hold_limits(1),
                    )
                    .await?;
                if source_values.len() != 1 {
                    return Err(journal_error("short native hold source recheck"));
                }
                if source_values[0].as_ref() != source.expected.as_ref() {
                    return Ok(NativeOwnerPreparation::EpochOverlap);
                }
                return Err(WorkspaceError::Fenced);
            }
            if active
                && let Some(new) = &change.new
                && !self
                    .check_native_hold_birth(new, writes, checks, birth_root_visits)
                    .await?
            {
                // Only an explicit false from an empty-write census CAS is
                // classified here. Transport and semantic errors propagate.
                return Ok(NativeOwnerPreparation::BirthCompareFalse);
            }
            merge(
                checks,
                [KvCheck {
                    key: change.key.clone(),
                    expected: actual[0].clone(),
                }],
            )?;
            writes.push(match change.new {
                Some(hold) => KvWrite::Put {
                    key: change.key,
                    value: hold.encode()?,
                },
                None => KvWrite::Delete { key: change.key },
            });
        }
        let generation = encode(&next_generation)?;
        if let Some(write) = writes.iter().find(|write| match write {
            KvWrite::Put { key, .. } | KvWrite::Delete { key } => {
                key.as_slice() == PACKED_ROOT_GENERATION_KEY
            }
        }) {
            if !matches!(write, KvWrite::Put { value, .. } if *value == generation) {
                return Err(WorkspaceError::Busy);
            }
        } else {
            writes.push(KvWrite::Put {
                key: PACKED_ROOT_GENERATION_KEY.to_vec(),
                value: generation,
            });
        }
        Ok(NativeOwnerPreparation::Ready(owner))
    }

    async fn authenticate_native_hold_birth_layers(
        &self,
        hold: &NativeHold,
        checks: &mut Vec<KvCheck>,
        writes: &[KvWrite],
    ) -> Result<(), WorkspaceError> {
        let mut starts = Vec::with_capacity(2);
        match hold {
            NativeHold::Workspace { head, fork, .. } => {
                starts.push((*head, None));
                if let Some(revision) = fork {
                    starts.push((revision.layer_id, Some(revision)));
                }
            }
            NativeHold::Snapshot { revision, .. } | NativeHold::Lease { revision, .. } => {
                starts.push((revision.layer_id, Some(revision)));
            }
            NativeHold::Journal { head, .. } | NativeHold::JournalNewHead { head, .. } => {
                starts.push((*head, None));
            }
        }
        for (start, revision) in starts {
            let mut next = Some(start);
            let mut visited = BTreeSet::new();
            for index in 0..LAYER_CHAIN_HARD_LIMIT {
                let Some(id) = next else {
                    break;
                };
                if self
                    .packed_reader_pin_budget
                    .get()
                    .is_some_and(|budget| budget.state().closed)
                {
                    return Err(WorkspaceError::Busy);
                }
                if !visited.insert(id) {
                    return Err(journal_error("native hold birth layer cycle"));
                }
                let key = hot_layer_key(id);
                let prior = if let Some(check) = checks.iter().find(|check| check.key == key) {
                    check.expected.clone()
                } else {
                    // Bound the growing final packet before another transport
                    // or retained layer value. Callers keep their existing cap.
                    if checks.len().saturating_add(writes.len()) >= 256 {
                        return Err(WorkspaceError::Busy);
                    }
                    let (mut values, _) = self
                        .backend
                        .get_many_consistent_with_time_bounded(
                            std::slice::from_ref(&key),
                            hold_limits(1),
                        )
                        .await?;
                    if values.len() != 1 {
                        return Err(journal_error("short native hold birth layer read"));
                    }
                    let raw = values.pop().unwrap();
                    checks.push(KvCheck {
                        key: key.clone(),
                        expected: raw.clone(),
                    });
                    raw
                };
                if let Some(raw) = &prior {
                    let old: LayerRecord = decode_open_value(raw, OPEN_RECORD_MAX_BYTES)?;
                    if old.layer_id != id
                        || old.schema_version != WORKSPACE_SCHEMA_VERSION
                        || old.state == LayerState::Deleting
                    {
                        return Err(WorkspaceError::Fenced);
                    }
                }
                // Read the final value for duplicate-key packets; the actual
                // backend CAS receives these same ordered writes.
                let overlay = writes.iter().rev().find_map(|write| match write {
                    KvWrite::Put {
                        key: candidate,
                        value,
                    } if *candidate == key => Some(Some(value.as_slice())),
                    KvWrite::Delete { key: candidate } if *candidate == key => Some(None),
                    _ => None,
                });
                let selected = overlay.unwrap_or(prior.as_deref());
                if selected.is_none()
                    && index == 0
                    && matches!(hold, NativeHold::JournalNewHead { .. })
                    && overlay.is_none()
                {
                    // Native prepare records the chosen UUID before creating
                    // its layer. Authenticate its absence in the same CAS;
                    // an existing Deleting incarnation was rejected above.
                    next = None;
                    break;
                }
                let raw = selected.ok_or(WorkspaceError::Fenced)?;
                let row: LayerRecord = decode_open_value(raw, OPEN_RECORD_MAX_BYTES)?;
                if row.layer_id != id
                    || row.schema_version != WORKSPACE_SCHEMA_VERSION
                    || row.state == LayerState::Deleting
                {
                    return Err(WorkspaceError::Fenced);
                }
                if index == 0
                    && let Some(revision) = revision
                    && revision_from_layer(&row)? != *revision
                {
                    return Err(WorkspaceError::Fenced);
                }
                next = row.parent_layer_id;
                if self
                    .packed_reader_pin_budget
                    .get()
                    .is_some_and(|budget| budget.state().closed)
                {
                    return Err(WorkspaceError::Busy);
                }
            }
            if next.is_some() {
                return Err(WorkspaceError::Busy);
            }
        }
        Ok(())
    }

    pub(super) async fn hold_chain_contains(
        &self,
        start: LayerId,
        target: &BaseRevision,
        writes: &[KvWrite],
    ) -> Result<bool, WorkspaceError> {
        self.hold_chain_contains_inner(start, None, target, writes, false)
            .await
    }

    pub(super) async fn hold_revision_contains(
        &self,
        source: &BaseRevision,
        target: &BaseRevision,
        writes: &[KvWrite],
    ) -> Result<bool, WorkspaceError> {
        self.hold_chain_contains_inner(source.layer_id, Some(source), target, writes, false)
            .await
    }

    async fn hold_chain_contains_inner(
        &self,
        start: LayerId,
        source: Option<&BaseRevision>,
        target: &BaseRevision,
        writes: &[KvWrite],
        allow_deleting_current_head: bool,
    ) -> Result<bool, WorkspaceError> {
        let mut next = Some(start);
        let mut visited = BTreeSet::new();
        for index in 0..LAYER_CHAIN_HARD_LIMIT {
            let Some(id) = next else {
                return Ok(false);
            };
            if !visited.insert(id) {
                return Err(journal_error("native hold layer cycle"));
            }
            let key = hot_layer_key(id);
            let overlay = writes.iter().find_map(|write| match write {
                KvWrite::Put {
                    key: candidate,
                    value,
                } if *candidate == key => Some(Some(value.as_slice())),
                KvWrite::Delete { key: candidate } if *candidate == key => Some(None),
                _ => None,
            });
            let row: LayerRecord = if let Some(raw) = overlay {
                decode_open_value(raw.ok_or(WorkspaceError::Fenced)?, OPEN_RECORD_MAX_BYTES)?
            } else {
                let (raw, _) = self
                    .backend
                    .get_many_consistent_with_time_bounded(&[key], hold_limits(1))
                    .await?;
                if raw.len() != 1 {
                    return Err(journal_error("short native hold layer read"));
                }
                decode_open_value(
                    raw[0].as_deref().ok_or(WorkspaceError::Fenced)?,
                    OPEN_RECORD_MAX_BYTES,
                )?
            };
            if row.layer_id != id
                || row.schema_version != WORKSPACE_SCHEMA_VERSION
                || (row.state == LayerState::Deleting
                    && !(allow_deleting_current_head && index == 0))
            {
                return Err(WorkspaceError::Fenced);
            }
            if row.state == LayerState::Deleting
                && (source.is_some()
                    || row.parent_layer_id.is_none()
                    || row.owner_workspace_id.is_some()
                    || row.depth <= 1
                    || row.sealed_version.is_some()
                    || row.delta_digest.is_some()
                    || row.root_hash.is_some())
            {
                return Err(WorkspaceError::Fenced);
            }
            // A stale sealed source cannot authenticate an ancestor merely
            // because its layer ID survived. Check version and native hash.
            if index == 0
                && let Some(source) = source
                && revision_from_layer(&row)? != *source
            {
                return Err(WorkspaceError::Fenced);
            }
            if id == target.layer_id {
                return Ok(revision_from_layer(&row)? == *target);
            }
            next = row.parent_layer_id;
        }
        if next.is_some() {
            Err(WorkspaceError::Busy)
        } else {
            Ok(false)
        }
    }

    async fn hold_matches(
        &self,
        hold: &NativeHold,
        target: &BaseRevision,
        writes: &[KvWrite],
    ) -> Result<bool, WorkspaceError> {
        match hold {
            NativeHold::Snapshot { revision, .. } | NativeHold::Lease { revision, .. } => {
                self.hold_revision_contains(revision, target, writes).await
            }
            NativeHold::Workspace { head, fork, .. } => {
                if let Some(fork) = fork
                    && self.hold_revision_contains(fork, target, writes).await?
                {
                    return Ok(true);
                }
                self.hold_chain_contains(*head, target, writes).await
            }
            NativeHold::Journal { head, .. } => {
                self.hold_chain_contains(*head, target, writes).await
            }
            NativeHold::JournalNewHead { head, .. } => {
                let key = hot_layer_key(*head);
                let overlay = writes.iter().rev().find_map(|write| match write {
                    KvWrite::Put { key: candidate, .. } if *candidate == key => Some(true),
                    KvWrite::Delete { key: candidate } if *candidate == key => Some(false),
                    _ => None,
                });
                let exists = if let Some(exists) = overlay {
                    exists
                } else {
                    let (values, _) = self
                        .backend
                        .get_many_consistent_with_time_bounded(
                            std::slice::from_ref(&key),
                            hold_limits(1),
                        )
                        .await?;
                    if values.len() != 1 {
                        return Err(journal_error("short planned journal head read"));
                    }
                    values[0].is_some()
                };
                if !exists {
                    // The caller's retained inventory/root epochs certify
                    // that this planned head was not born during the census.
                    return Ok(false);
                }
                self.hold_chain_contains(*head, target, writes).await
            }
        }
    }

    async fn check_native_hold_birth(
        &self,
        hold: &NativeHold,
        writes: &[KvWrite],
        checks: &[KvCheck],
        visited: &mut u64,
    ) -> Result<bool, WorkspaceError> {
        let mut after = None;
        loop {
            if self
                .packed_reader_pin_budget
                .get()
                .is_some_and(|budget| budget.state().closed)
            {
                return Err(WorkspaceError::Busy);
            }
            if !self.backend.compare_and_swap(checks, &[]).await.inspect_err(|_error| {
                #[cfg(test)]
                eprintln!("[packed-v3-native-begin-diag] stage=native-hold-birth-before-page error={_error}");
            })? {
                #[cfg(test)]
                eprintln!("[packed-v3-native-begin-diag] stage=native-hold-birth-before-page compare=false visited={visited}");
                return Ok(false);
            }
            let page = self
                .backend
                .scan_prefix_page_with_byte_limits(
                    REGISTRY_ROOT_PREFIX.as_bytes(),
                    after.as_deref(),
                    root_pages(),
                )
                .await?;
            if self
                .packed_reader_pin_budget
                .get()
                .is_some_and(|budget| budget.state().closed)
            {
                return Err(WorkspaceError::Busy);
            }
            // Charge fetched rows even when the following read-only CAS loses
            // its epoch, and retain this one cap across bounded rebuilds.
            for _ in &page {
                *visited = increment(*visited)?;
                if *visited > MAX_HOLD_BIRTH_ROOT_VISITS {
                    return Err(WorkspaceError::Busy);
                }
            }
            if !self.backend.compare_and_swap(checks, &[]).await.inspect_err(|_error| {
                #[cfg(test)]
                eprintln!("[packed-v3-native-begin-diag] stage=native-hold-birth-after-page error={_error}");
            })? {
                #[cfg(test)]
                eprintln!("[packed-v3-native-begin-diag] stage=native-hold-birth-after-page compare=false visited={visited}");
                return Ok(false);
            }
            if page.is_empty() {
                return Ok(true);
            }
            for entry in page {
                if !entry.key.starts_with(REGISTRY_ROOT_PREFIX.as_bytes())
                    || after.as_ref().is_some_and(|last| entry.key <= *last)
                {
                    return Err(WorkspaceError::Fenced);
                }
                let root = RootRow::decode(&entry.value)?;
                if entry.key != registry_root_key(root.incarnation) {
                    return Err(WorkspaceError::Fenced);
                }
                if matches!(root.state, RootState::Retiring | RootState::Retired)
                    && let Some(binding) = &root.binding
                    && self
                        .hold_matches(hold, &binding.base_revision, writes)
                        .await?
                {
                    return Err(WorkspaceError::Fenced);
                }
                after = Some(entry.key);
            }
        }
    }

    /// Sidecars become a complete census after every authoritative entity
    /// prefix reaches an empty page with stable owner epochs.
    /// max_rows bounds entity row visits.
    pub(crate) async fn migrate_native_packed_holds(
        &self,
        budget: &Arc<V3MountBudget>,
        max_rows: u64,
        cancel: CancellationToken,
    ) -> Result<u64, WorkspaceError> {
        self.configure_packed_reader_pin_budget(budget.clone())?;
        let _owner = budget
            .admit(&[(V3BudgetPool::Metadata, HOLD_OPERATION_BYTES)])
            .map_err(journal_budget_error)?;
        if max_rows == 0 || max_rows > MAX_OBJECTS || cancel.is_cancelled() || budget.state().closed
        {
            return Err(WorkspaceError::Busy);
        }
        // Activation is the durable certificate that the complete native
        // census was installed. Reauthenticate that certificate and its owner
        // epochs before returning; an already migrated catalog can authenticate
        // the completed census without repeating the entity scan.
        let active_keys = [
            HOLD_FEATURE.to_vec(),
            PACKED_ROOT_GENERATION_KEY.to_vec(),
            LAYER_INVENTORY_GENERATION_KEY.to_vec(),
            HOLD_JOURNAL_HEADS_FEATURE.to_vec(),
        ];
        let (active_values, _) = self
            .backend
            .get_many_consistent_with_time_bounded(&active_keys, hold_limits(active_keys.len()))
            .await?;
        if active_values.len() != active_keys.len() {
            return Err(journal_error("short active native census basis"));
        }
        let active_gate = active_values[0]
            .as_deref()
            .map(NativeHoldGate::decode)
            .transpose()?;
        next_packed_root_generation(&active_values[1])?;
        layer_inventory_generation(&active_values[2])?;
        if active_gate.is_some_and(|gate| gate.active) && active_values[3] == active_values[0] {
            let active_checks = active_keys
                .into_iter()
                .zip(active_values)
                .map(|(key, expected)| KvCheck { key, expected })
                .collect::<Vec<_>>();
            if cancel.is_cancelled()
                || budget.state().closed
                || !self.backend.compare_and_swap(&active_checks, &[]).await?
                || cancel.is_cancelled()
                || budget.state().closed
            {
                return Err(WorkspaceError::Busy);
            }
            return Ok(0);
        }
        let keys = [
            HOLD_FEATURE.to_vec(),
            PACKED_ROOT_GENERATION_KEY.to_vec(),
            LAYER_INVENTORY_GENERATION_KEY.to_vec(),
            HOLD_JOURNAL_HEADS_FEATURE.to_vec(),
        ];
        let (values, _) = self
            .backend
            .get_many_consistent_with_time_bounded(&keys, hold_limits(keys.len()))
            .await?;
        if values.len() != keys.len() {
            return Err(journal_error("short native census basis"));
        }
        let gate = values[0]
            .as_deref()
            .map(NativeHoldGate::decode)
            .transpose()?;
        let mut fence = keys
            .iter()
            .cloned()
            .zip(values.iter().cloned())
            .map(|(key, expected)| KvCheck { key, expected })
            .collect::<Vec<_>>();
        next_packed_root_generation(&values[1])?;
        layer_inventory_generation(&values[2])?;
        if gate.is_some_and(|gate| gate.active) && values[3] == values[0] {
            if cancel.is_cancelled()
                || budget.state().closed
                || !self.backend.compare_and_swap(&fence, &[]).await?
                || cancel.is_cancelled()
                || budget.state().closed
            {
                return Err(WorkspaceError::Busy);
            }
            return Ok(0);
        }
        let mut gate = NativeHoldGate {
            run: Uuid::new_v4(),
            active: false,
        };
        let raw = encode(&gate)?;
        if !self
            .backend
            .compare_and_swap(
                &fence,
                &[KvWrite::Put {
                    key: HOLD_FEATURE.to_vec(),
                    value: raw.clone(),
                }],
            )
            .await?
        {
            return Err(WorkspaceError::Busy);
        }
        fence[0].expected = Some(raw);
        let mut visited = 0;
        for prefix in [HOT_WORKSPACE_PREFIX, HOT_SNAPSHOT_PREFIX, HOT_LEASE_PREFIX] {
            let mut after = None;
            loop {
                if cancel.is_cancelled() || budget.state().closed {
                    return Err(WorkspaceError::Busy);
                }
                if !self.backend.compare_and_swap(&fence, &[]).await? {
                    return Err(WorkspaceError::Busy);
                }
                let page = self
                    .backend
                    .scan_prefix_page_with_byte_limits(prefix, after.as_deref(), hold_pages())
                    .await?;
                if !self.backend.compare_and_swap(&fence, &[]).await? {
                    return Err(WorkspaceError::Busy);
                }
                if page.is_empty() {
                    break;
                }
                for entry in page {
                    if !entry.key.starts_with(prefix)
                        || after.as_ref().is_some_and(|last| entry.key <= *last)
                    {
                        return Err(WorkspaceError::Fenced);
                    }
                    visited = increment(visited)?;
                    if visited > max_rows {
                        return Err(WorkspaceError::Busy);
                    }
                    if let Some(hold) = hot_projection(&entry.key, Some(&entry.value))? {
                        self.install_census_hold(&fence, &hold, Some(entry.clone()))
                            .await?;
                    }
                    after = Some(entry.key);
                }
            }
        }
        // Journals share the same independently stored entity authority as
        // workspaces, snapshots and leases. Page them under the retained root
        // epoch and compare each exact entity when installing either head hold.
        let mut after = None;
        loop {
            if cancel.is_cancelled() || budget.state().closed {
                return Err(WorkspaceError::Busy);
            }
            if !self.backend.compare_and_swap(&fence, &[]).await? {
                return Err(WorkspaceError::Busy);
            }
            let page = self
                .backend
                .scan_prefix_page_with_byte_limits(
                    HOT_JOURNAL_PREFIX,
                    after.as_deref(),
                    hold_pages(),
                )
                .await?;
            if !self.backend.compare_and_swap(&fence, &[]).await? {
                return Err(WorkspaceError::Busy);
            }
            if page.is_empty() {
                break;
            }
            for entry in page {
                if !entry.key.starts_with(HOT_JOURNAL_PREFIX)
                    || after.as_ref().is_some_and(|last| entry.key <= *last)
                {
                    return Err(WorkspaceError::Fenced);
                }
                let journal: SealJournal = decode_open_value(&entry.value, OPEN_RECORD_MAX_BYTES)?;
                if entry.key != hot_journal_key(journal.workspace_id, journal.journal_id) {
                    return Err(WorkspaceError::Fenced);
                }
                visited = increment(visited)?;
                if visited > max_rows {
                    return Err(WorkspaceError::Busy);
                }
                for hold in [
                    NativeHold::journal(&journal),
                    NativeHold::journal_new_head(&journal),
                ]
                .into_iter()
                .flatten()
                {
                    self.install_census_hold(&fence, &hold, Some(entry.clone()))
                        .await?;
                }
                after = Some(entry.key);
            }
        }
        if cancel.is_cancelled() || budget.state().closed {
            return Err(WorkspaceError::Busy);
        }
        gate.active = true;
        if !self
            .backend
            .compare_and_swap(
                &fence,
                &[
                    KvWrite::Put {
                        key: HOLD_FEATURE.to_vec(),
                        value: encode(&gate)?,
                    },
                    KvWrite::Put {
                        key: HOLD_JOURNAL_HEADS_FEATURE.to_vec(),
                        value: encode(&gate)?,
                    },
                ],
            )
            .await?
        {
            return Err(WorkspaceError::Busy);
        }
        Ok(visited)
    }

    async fn install_census_hold(
        &self,
        fence: &[KvCheck],
        hold: &NativeHold,
        row: Option<KvEntry>,
    ) -> Result<(), WorkspaceError> {
        let key = hold.key();
        let (values, _) = self
            .backend
            .get_many_consistent_with_time_bounded(std::slice::from_ref(&key), hold_limits(1))
            .await?;
        if values.len() != 1 {
            return Err(journal_error("short native census hold"));
        }
        if let Some(raw) = &values[0] {
            let old = NativeHold::decode(raw)?;
            if old.key() != key {
                return Err(WorkspaceError::Fenced);
            }
        }
        let mut checks = fence.to_vec();
        checks.push(KvCheck {
            key: key.clone(),
            expected: values[0].clone(),
        });
        if let Some(row) = row {
            checks.push(KvCheck {
                key: row.key,
                expected: Some(row.value),
            });
        }
        if !self
            .backend
            .compare_and_swap(
                &checks,
                &[KvWrite::Put {
                    key,
                    value: hold.encode()?,
                }],
            )
            .await?
        {
            return Err(WorkspaceError::Busy);
        }
        Ok(())
    }
}
