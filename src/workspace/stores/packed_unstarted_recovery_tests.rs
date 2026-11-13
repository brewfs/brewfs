use super::*;
use crate::workspace::packed::snapshot::{SnapshotBudgetLimits, SnapshotMountBudget};
use crate::workspace::stores::binding_tests::{packed, request};
use crate::workspace::stores::kv_store::packed_admin::{
    PackedMountGrantRequest, PackedReleasedMountReference,
};

struct Fixture {
    backend: MemoryBackend,
    store: Arc<KvWorkspaceStore<MemoryBackend>>,
    budget: Arc<SnapshotMountBudget>,
    original: PackedReleasedMountReference,
    original_lease: SnapshotLease,
    failed: LeaseId,
    next: LeaseId,
}

impl Fixture {
    async fn new() -> Self {
        let backend = MemoryBackend::default();
        backend.clock.store(1_000_000_000, Ordering::SeqCst);
        let budget = SnapshotMountBudget::new(SnapshotBudgetLimits {
            bytes: [
                16 << 20,
                256 << 20,
                32 << 20,
                1 << 20,
                32 << 20,
                32 << 20,
                8 << 20,
                32 << 20,
            ],
            max_read_bytes: 4 << 20,
        })
        .unwrap();
        let store = Arc::new(
            KvWorkspaceStore::new(backend.clone()).with_packed_reader_pin_budget(budget.clone()),
        );
        let (_objects, _client, _snapshot, lower, _data) = packed().await;
        let install = request(store.as_ref(), lower).await;
        let binding = store
            .install_packed_lower_binding(install.clone())
            .await
            .unwrap();
        store
            .release_lease(ReleaseLease {
                lease_id: install.guard.lease_id,
                holder_generation: install.guard.holder_generation,
            })
            .await
            .unwrap();
        let mounted = store
            .grant_packed_mounted_session(PackedMountGrantRequest {
                workspace_id: binding.workspace_id,
                lease_id: LeaseId::new(),
                holder_generation: install.guard.holder_generation + 1,
                mount_uid: Uuid::new_v4(),
                pod_uid: Uuid::new_v4(),
                ttl_ns: 1_000,
            })
            .await
            .unwrap();
        let original = mounted.reference();
        let original_lease = mounted.lease();
        // Dropping the local handle leaves the actual durable mounted owner.
        drop(mounted);
        backend
            .clock
            .store(original_lease.expires_at_ns, Ordering::SeqCst);
        backend.cas_checks.lock().await.clear();
        backend.cas_write_keys.lock().await.clear();
        Self {
            backend,
            store,
            budget,
            original,
            original_lease,
            failed: LeaseId::new(),
            next: LeaseId::new(),
        }
    }

    async fn inspect(&self) -> Result<Option<u64>, WorkspaceError> {
        self.store
            .inspect_unstarted_packed_mount_recovery(self.original.clone(), self.failed, self.next)
            .await
    }

    fn other(&self, foreign: bool) -> SnapshotLease {
        SnapshotLease {
            lease_id: LeaseId::new(),
            workspace_id: if foreign {
                WorkspaceId::new()
            } else {
                self.original.guard.workspace_id
            },
            ..self.original_lease.clone()
        }
    }

    async fn set_control(&self, control: &ControlState) {
        test_write_topology_rows(&mut *self.backend.records.lock().await, control);
    }

    async fn control(&self) -> ControlState {
        test_topology_from_rows(&*self.backend.records.lock().await)
    }

    async fn assert_successful_facts(&self) {
        let before = self.backend.records.lock().await.clone();
        assert_eq!(
            self.inspect().await.unwrap(),
            Some(self.original.guard.holder_generation + 1),
        );
        assert_eq!(
            *self.backend.records.lock().await,
            before,
            "facts mutated metadata"
        );
        assert_eq!(
            self.budget.state().used,
            [0; 8],
            "facts retained a budget owner"
        );
        let calls = self.backend.cas_checks.lock().await;
        let authenticated = calls.last().expect("facts omitted final authentication");
        for key in [
            CONTROL_KEY.to_vec(),
            hot_lease_key(
                self.original.guard.workspace_id,
                self.original.guard.lease_id,
            ),
            hot_lease_key(self.original.guard.workspace_id, self.failed),
            hot_lease_key(self.original.guard.workspace_id, self.next),
            packed_writer_authority::packed_writer_key(self.original.guard.workspace_id),
            open_snapshot_key(self.original.guard.workspace_id),
            packed_current_key(self.original.guard.workspace_id),
            packed_claim_key(self.original.guard.workspace_id),
            packed_history_key(self.original.guard.workspace_id, 1),
        ] {
            assert!(authenticated.contains(&key), "final packet omitted {key:?}");
        }
    }
}

#[tokio::test]
async fn unstarted_recovery_api_authenticates_expired_owner_without_expanding_foreign_scope() {
    let fixture = Fixture::new().await;
    fixture.assert_successful_facts().await;

    let mut control = fixture.control().await;
    let unrelated = fixture.other(true);
    control.leases.insert(unrelated.lease_id, unrelated);
    fixture.set_control(&control).await;
    fixture.assert_successful_facts().await;

    // This unrelated foreign identity cannot authorize either attempted lease.
    // Its malformed row stays outside this workspace/attempt-scoped guard.
    control.leases.insert(LeaseId::new(), fixture.other(true));
    fixture.set_control(&control).await;
    fixture.assert_successful_facts().await;
}

#[tokio::test]
async fn unstarted_recovery_api_scopes_aliases_and_rejects_occupied_attempts() {
    let fixture = Fixture::new().await;
    let baseline = fixture.control().await;
    let mut cases = Vec::new();

    cases.push((
        "target source alias",
        vec![(LeaseId::new(), fixture.original_lease.clone())],
        2u8,
    ));
    cases.push((
        "target duplicate identity",
        vec![
            (
                fixture.original.guard.lease_id,
                fixture.original_lease.clone(),
            ),
            (LeaseId::new(), fixture.original_lease.clone()),
        ],
        2u8,
    ));
    cases.push((
        "target unrelated mismatched identity",
        vec![(LeaseId::new(), fixture.other(false))],
        2u8,
    ));
    for (name, id) in [("failed", fixture.failed), ("next", fixture.next)] {
        for foreign in [false, true] {
            let row = SnapshotLease {
                lease_id: id,
                ..fixture.other(foreign)
            };
            cases.push((
                name,
                vec![(id, row.clone())],
                if foreign && name == "next" { 1 } else { 0 },
            ));
            cases.push((name, vec![(LeaseId::new(), row)], 2));
            let mismatched = fixture.other(foreign);
            cases.push((
                name,
                vec![(id, mismatched)],
                if foreign && name == "failed" { 0 } else { 1 },
            ));
        }
    }

    cases.push((
        "declared source mismatch",
        vec![(fixture.original.guard.lease_id, fixture.other(false))],
        1,
    ));
    for (name, rows, expected) in cases {
        let mut control = baseline.clone();
        control.leases.extend(rows);
        fixture.set_control(&control).await;
        let before = fixture.backend.records.lock().await.clone();
        let result = fixture.inspect().await;
        if expected == 1 {
            assert!(
                matches!(result, Err(WorkspaceError::Fenced)),
                "{name}: {result:?}"
            );
        } else if expected == 0 {
            assert_eq!(result.unwrap(), None, "{name}: occupied attempt admitted");
        } else {
            assert_eq!(
                result.unwrap(),
                Some(fixture.original.guard.holder_generation + 1),
                "{name}: an undeclared alias affected the target scope"
            );
            assert!(
                matches!(
                    fixture.store.read_complete_topology_census().await,
                    Err(WorkspaceError::Fenced)
                ),
                "complete census accepted {name}"
            );
        }
        assert_eq!(
            *fixture.backend.records.lock().await,
            before,
            "{name}: facts wrote metadata"
        );
        assert_eq!(
            fixture.budget.state().used,
            [0; 8],
            "{name}: facts retained ownership"
        );
    }
}

#[tokio::test]
async fn unstarted_recovery_api_reauthenticates_declared_source_before_returning_facts() {
    let fixture = Fixture::new().await;
    let mut changed = fixture.original_lease.clone();
    changed.holder_generation += 1;
    fixture
        .backend
        .mutate_on_cas
        .lock()
        .await
        .push(KvWrite::Put {
            key: hot_lease_key(changed.workspace_id, changed.lease_id),
            value: encode(&changed).unwrap(),
        });
    assert!(matches!(
        fixture.inspect().await,
        Err(WorkspaceError::Fenced)
    ));
    assert_eq!(fixture.budget.state().used, [0; 8]);
    let calls = fixture.backend.cas_checks.lock().await;
    assert!(
        calls
            .last()
            .unwrap()
            .contains(&hot_lease_key(changed.workspace_id, changed.lease_id)),
        "source entity changed after its snapshot but escaped final authentication",
    );
}

#[derive(Clone, Default)]
struct StopRecoveryObjects {
    reads: Arc<AtomicU64>,
}

const RECOVERY_READ_STOP: &str = "stop after typed recovery owner claim";

#[async_trait]
impl crate::cadapter::client::ObjectBackend for StopRecoveryObjects {
    async fn put_object(&self, _: &str, _: &[u8]) -> anyhow::Result<()> {
        panic!("failed recovery read must not PUT an object")
    }
    async fn get_object(&self, _: &str) -> anyhow::Result<Option<Vec<u8>>> {
        panic!("manifest recovery must use the bounded range path")
    }
    async fn get_object_range(&self, _: &str, _: u64, _: &mut [u8]) -> anyhow::Result<usize> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        Err(
            crate::workspace::packed::PackedWireError::Backend(
                RECOVERY_READ_STOP.into(),
            )
            .into(),
        )
    }
    async fn get_etag(&self, _: &str) -> anyhow::Result<String> {
        panic!("manifest recovery must not use an ETag fallback")
    }
    async fn delete_object(&self, _: &str) -> anyhow::Result<()> {
        panic!("failed recovery read must not DELETE an object")
    }
}

impl Fixture {
    async fn initialize_recovery_pvc(&self, root: &std::path::Path) {
        let binding = crate::workspace::publish::binding::PackedLowerBindingRecord::decode(
            &self
                .backend
                .get(&packed_current_key(self.original.guard.workspace_id))
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        let config = crate::vfs::config::VFSConfig::new(crate::chunk::ChunkLayout {
            chunk_size: 4096,
            block_size: 4096,
        })
        .workspace_writeback_root(root.to_path_buf())
        .workspace_writer_epoch(self.original.guard.holder_generation);
        crate::workspace::stores::kv_store::packed_mount_writeback::init_packed_mount_writeback_identity(
            &self.original,
            &binding,
            &self.budget,
            &config,
        )
        .await
        .unwrap();
    }

    async fn recover_until_manifest(
        &self,
        root: &std::path::Path,
        lease_id: LeaseId,
        generation: u64,
    ) -> Result<(), WorkspaceError> {
        let objects = StopRecoveryObjects::default();
        let client = crate::cadapter::client::ObjectClient::new(objects.clone());
        let config = crate::vfs::config::VFSConfig::new(crate::chunk::ChunkLayout {
            chunk_size: 4096,
            block_size: 4096,
        })
        .workspace_writeback_root(root.to_path_buf())
        .workspace_writer_epoch(generation);
        let result = self
            .store
            .recover_packed_mounted_session(
                crate::workspace::stores::kv_store::packed_admin::PackedMountedRecoveryRequest {
                    original: self.original.clone(),
                    lease_id,
                    recovery_pod_uid: Uuid::new_v4(),
                    ttl_ns: 300_000_000_000,
                },
                client,
                Arc::new(crate::chunk::store::InMemoryBlockStore::new()),
                config,
            )
            .await;
        match result {
            Err(WorkspaceError::CorruptMetadata(message))
                if message == format!("packed v3 object backend error: {RECOVERY_READ_STOP}") =>
            {
                assert_eq!(objects.reads.load(Ordering::SeqCst), 1);
                Ok(())
            }
            Err(error) => {
                assert_eq!(objects.reads.load(Ordering::SeqCst), 0);
                Err(error)
            }
            Ok(_) => panic!("injected manifest transport failure did not stop recovery"),
        }
    }

    async fn assert_recovery_claim(
        &self,
        previous: &SnapshotLease,
        current: LeaseId,
    ) -> SnapshotLease {
        let workspace_id = self.original.guard.workspace_id;
        let rows = self.backend.records.lock().await;
        let workspace: WorkspaceRecord =
            decode(rows.get(&hot_workspace_key(workspace_id)).unwrap()).unwrap();
        let old: SnapshotLease = decode(
            rows.get(&hot_lease_key(workspace_id, previous.lease_id))
                .unwrap(),
        )
        .unwrap();
        let next: SnapshotLease =
            decode(rows.get(&hot_lease_key(workspace_id, current)).unwrap()).unwrap();
        let route: WorkspaceId = decode(rows.get(&lease_index_key(current)).unwrap()).unwrap();
        assert_eq!(workspace.active_lease, Some(current));
        assert_eq!(route, workspace_id);
        assert_eq!(next.state, LeaseState::Active);
        assert_eq!(next.holder_generation, previous.holder_generation + 1);
        let mut expected_old = previous.clone();
        expected_old.state = LeaseState::Expired;
        expected_old.updated_at_ns = next.created_at_ns;
        assert_eq!(old, expected_old);
        let writer = crate::workspace::stores::kv_store::packed_writer_authority::PackedWriterAuthority::decode(
            rows.get(&packed_writer_authority::packed_writer_key(workspace_id)).unwrap(),
            workspace_id,
        )
        .unwrap();
        assert!(matches!(writer.owner,
            Some(packed_writer_authority::PackedWriterOwner::Administrative {
                lease_id, holder_generation, recovering: true, ..
            }) if lease_id == current && holder_generation == next.holder_generation));
        let hold_key = format!("packed/native-hold/lease/{current}").into_bytes();
        assert!(rows.contains_key(&hold_key));
        drop(rows);
        let writes = self.backend.cas_write_keys.lock().await;
        let index = writes
            .iter()
            .rposition(|keys| keys.contains(&hot_lease_key(workspace_id, current)))
            .expect("typed recovery never submitted the claim packet");
        let checks = self.backend.cas_checks.lock().await;
        assert_eq!(
            checks.len(),
            writes.len(),
            "claim CAS observation logs must share the same start and recording path"
        );
        for key in [
            CONTROL_KEY.to_vec(),
            hot_workspace_key(workspace_id),
            hot_lease_key(workspace_id, previous.lease_id),
            lease_index_key(previous.lease_id),
            hot_lease_key(workspace_id, current),
            lease_index_key(current),
            packed_writer_authority::packed_writer_key(workspace_id),
            open_snapshot_key(workspace_id),
            hold_key,
        ] {
            assert!(checks[index].contains(&key), "claim omitted {key:?}");
        }
        next
    }
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn typed_recovery_after_reap_claims_mounted_then_reaped_administrative_owner() {
    let fixture = Fixture::new().await;
    let pvc = tempfile::tempdir().unwrap();
    fixture.initialize_recovery_pvc(pvc.path()).await;
    let original_hold = format!(
        "packed/native-hold/lease/{}",
        fixture.original_lease.lease_id
    )
    .into_bytes();
    let hold_before = fixture.backend.get(&original_hold).await.unwrap().unwrap();
    assert_eq!(fixture.store.reap_expired_leases().await.unwrap(), 1);
    assert_eq!(
        fixture.backend.get(&original_hold).await.unwrap(),
        Some(hold_before.clone())
    );
    assert!(
        fixture
            .store
            .load_workspace(fixture.original.guard.workspace_id)
            .await
            .unwrap()
            .active_lease
            .is_none()
    );
    fixture
        .recover_until_manifest(
            pvc.path(),
            fixture.failed,
            fixture.original.guard.holder_generation + 1,
        )
        .await
        .unwrap();
    let recovering = fixture
        .assert_recovery_claim(&fixture.original_lease, fixture.failed)
        .await;
    assert_eq!(
        fixture.backend.get(&original_hold).await.unwrap(),
        Some(hold_before)
    );
    let recovery_hold = format!("packed/native-hold/lease/{}", recovering.lease_id).into_bytes();
    let hold_before = fixture.backend.get(&recovery_hold).await.unwrap().unwrap();
    fixture
        .backend
        .clock
        .store(recovering.expires_at_ns, Ordering::SeqCst);
    assert_eq!(fixture.store.reap_expired_leases().await.unwrap(), 1);
    assert_eq!(
        fixture.backend.get(&recovery_hold).await.unwrap(),
        Some(hold_before.clone())
    );
    fixture
        .recover_until_manifest(pvc.path(), fixture.next, recovering.holder_generation + 1)
        .await
        .unwrap();
    fixture
        .assert_recovery_claim(&recovering, fixture.next)
        .await;
    assert_eq!(
        fixture.backend.get(&recovery_hold).await.unwrap(),
        Some(hold_before)
    );
    assert!(!fixture.budget.state().closed);
    assert_eq!(fixture.budget.state().used, [0; 8]);
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn typed_recovery_after_reap_rejects_a_different_active_pointer_without_mutation() {
    let fixture = Fixture::new().await;
    let pvc = tempfile::tempdir().unwrap();
    fixture.initialize_recovery_pvc(pvc.path()).await;
    assert_eq!(fixture.store.reap_expired_leases().await.unwrap(), 1);
    let key = hot_workspace_key(fixture.original.guard.workspace_id);
    let mut rows = fixture.backend.records.lock().await;
    let mut workspace: WorkspaceRecord = decode(rows.get(&key).unwrap()).unwrap();
    workspace.active_lease = Some(LeaseId::new());
    rows.insert(key, encode(&workspace).unwrap());
    let before = rows.clone();
    drop(rows);
    assert!(matches!(
        fixture
            .recover_until_manifest(
                pvc.path(),
                fixture.failed,
                fixture.original.guard.holder_generation + 1
            )
            .await,
        Err(WorkspaceError::Fenced)
    ));
    assert_eq!(*fixture.backend.records.lock().await, before);
    assert_eq!(fixture.budget.state().used, [0; 8]);
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn typed_recovery_after_reap_rejects_unretired_or_unelapsed_owner_without_pointer() {
    for mutation in 0..3 {
        let fixture = Fixture::new().await;
        let pvc = tempfile::tempdir().unwrap();
        fixture.initialize_recovery_pvc(pvc.path()).await;
        assert_eq!(fixture.store.reap_expired_leases().await.unwrap(), 1);
        let workspace_id = fixture.original.guard.workspace_id;
        let lease_key = hot_lease_key(workspace_id, fixture.original.guard.lease_id);
        let mut rows = fixture.backend.records.lock().await;
        let mut lease: SnapshotLease = decode(rows.get(&lease_key).unwrap()).unwrap();
        match mutation {
            0 => lease.state = LeaseState::Active,
            1 => {
                lease.expires_at_ns += 1_000;
                let open_key = open_snapshot_key(workspace_id);
                let mut open: SnapshotOpenRecord = decode(rows.get(&open_key).unwrap()).unwrap();
                open.expires_at_ns = lease.expires_at_ns;
                rows.insert(open_key, encode(&open).unwrap());
            }
            2 => lease.state = LeaseState::Released,
            _ => unreachable!(),
        }
        rows.insert(lease_key, encode(&lease).unwrap());
        let before = rows.clone();
        drop(rows);
        assert!(matches!(
            fixture
                .recover_until_manifest(
                    pvc.path(),
                    fixture.failed,
                    fixture.original.guard.holder_generation + 1,
                )
                .await,
            Err(WorkspaceError::Fenced)
        ));
        assert_eq!(*fixture.backend.records.lock().await, before);
        assert_eq!(fixture.budget.state().used, [0; 8]);
    }
}
