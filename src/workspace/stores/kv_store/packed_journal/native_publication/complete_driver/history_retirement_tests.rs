//! Actual authenticated native publication, reader drain and history DELETE.
//! Redis/TiKV supply all catalog facts and grace-clock CAS; no raw rows are seeded.

use super::*;
use crate::cadapter::localfs::LocalFsBackend;
use crate::workspace::catalog::MarkDeleting;
use crate::workspace::model::WorkspaceState;
use crate::workspace::stores::kv_store::packed_journal::registry::PackedHistoryRetirementOptions;
use std::path::Path;
use std::time::Duration;

const GRACE_NS: u64 = 10_000_000_000;

pub(super) struct Fixture<'a, B> {
    pub(super) store: &'a Arc<KvWorkspaceStore<FinalDelivery<B>>>,
    pub(super) backend: &'a Arc<FinalDelivery<B>>,
    pub(super) client: ObjectClient<LocalFsBackend>,
    pub(super) objects: &'a Path,
    pub(super) budget: Arc<SnapshotMountBudget>,
    pub(super) guard: HeadGuard,
    pub(super) target: &'a PackedLowerBindingRecord,
    pub(super) anchor: &'a PackedLowerBindingRecord,
    pub(super) committed: &'a PackedJournalRecord,
}

impl<B: WorkspaceKvBackend> Fixture<'_, B> {
    fn options(&self) -> PackedHistoryRetirementOptions {
        PackedHistoryRetirementOptions {
            incarnation: self.committed.source.staging_id,
            grace_ns: GRACE_NS,
            max_native_holds: 1000,
            max_current_bindings: 1000,
            max_work: u64::MAX,
            cancel: CancellationToken::new(),
        }
    }

    fn root_key(&self) -> Vec<u8> {
        format!(
            "packed/registry/root/{}",
            self.committed.source.staging_id.simple()
        )
        .into_bytes()
    }

    fn history_root_key(&self) -> Vec<u8> {
        format!(
            "packed/registry/history-root/{}/{:016x}",
            self.target.workspace_id, self.target.binding.binding_version
        )
        .into_bytes()
    }

    async fn assert_retained(&self, original_root: &[u8], references: &[SnapshotObjectRef]) {
        assert_eq!(
            self.backend.get(&self.root_key()).await.unwrap().as_deref(),
            Some(original_root)
        );
        assert_eq!(
            self.backend
                .get(&self.history_root_key())
                .await
                .unwrap()
                .as_deref(),
            Some(original_root)
        );
        assert_eq!(
            self.backend
                .get(&packed_history_key(
                    self.target.workspace_id,
                    self.target.binding.binding_version,
                ))
                .await
                .unwrap(),
            Some(self.target.encode().unwrap())
        );
        assert_eq!(
            self.backend
                .get(&packed_current_key(self.target.workspace_id))
                .await
                .unwrap(),
            Some(self.target.encode().unwrap())
        );
        assert!(
            self.backend
                .get(&packed_claim_key(self.target.workspace_id))
                .await
                .unwrap()
                .is_some()
        );
        for reference in references {
            assert!(
                self.objects.join("objects").join(&reference.key).is_file(),
                "retained actual object disappeared: {}",
                reference.key
            );
        }
    }
}

pub(super) async fn contract<B: WorkspaceKvBackend>(fixture: Fixture<'_, B>) {
    assert!(fixture.target.binding.binding_version > 1);
    assert_eq!(fixture.anchor.binding.binding_version, 1);
    let committed_raw = fixture
        .backend
        .get(&journal_key(fixture.committed.journal_id))
        .await
        .unwrap()
        .unwrap();
    let committed = PackedJournalRecord::decode(&committed_raw).unwrap();
    assert_eq!(committed.phase, PackedJournalPhase::Committed);
    assert_eq!(committed.commit_target.as_ref(), Some(fixture.target));
    let original_root = fixture
        .backend
        .get(&fixture.root_key())
        .await
        .unwrap()
        .unwrap();
    let mut references = Vec::new();
    for ordinal in 0..committed.object_count {
        let actual = fixture
            .backend
            .get(&object_key(committed.journal_id, ordinal))
            .await
            .unwrap()
            .unwrap();
        let object = PackedJournalObject::decode(&actual).unwrap();
        assert_eq!(object.ordinal, ordinal);
        assert!(object.uploaded);
        references.push(object.reference);
    }
    assert!(
        references
            .iter()
            .any(|reference| reference == &fixture.target.binding.manifest)
    );
    fixture.assert_retained(&original_root, &references).await;
    fixture
        .store
        .migrate_native_packed_holds(&fixture.budget, 1000, CancellationToken::new())
        .await
        .unwrap();
    let reader = fixture
        .store
        .clone()
        .open_packed_reader_session(
            fixture.guard.clone(),
            fixture.budget.clone(),
            PackedReaderLeaseOptions::default(),
        )
        .await
        .unwrap();
    let request_owner = reader.retain_request().unwrap();
    fixture
        .store
        .mark_workspace_deleting(MarkDeleting {
            workspace_id: fixture.target.workspace_id,
            force_fence_lease: true,
        })
        .await
        .unwrap();
    let workspace = fixture
        .store
        .load_workspace(fixture.target.workspace_id)
        .await
        .unwrap();
    assert_eq!(workspace.state, WorkspaceState::Deleting);
    assert_eq!(workspace.head_layer_id, fixture.target.head_layer_id);
    assert_eq!(workspace.head_epoch, fixture.target.head_epoch);
    let head = fixture
        .store
        .load_layer(fixture.target.head_layer_id)
        .await
        .unwrap();
    assert_eq!(head.state, LayerState::Deleting);
    assert_eq!(head.owner_workspace_id, None);
    let pins = fixture.store.packed_reader_pin_roots().await.unwrap();
    assert!(pins.bindings.contains(fixture.target));
    drop(pins);
    assert!(matches!(
        fixture
            .store
            .retire_packed_binding_history(
                fixture.client.clone(),
                fixture.budget.clone(),
                fixture.options(),
            )
            .await,
        Err(WorkspaceError::Busy | WorkspaceError::Fenced)
    ));
    fixture.assert_retained(&original_root, &references).await;

    // Poll the actual shutdown future. Its pin cannot be released until the
    // retained request has drained, even after the workspace was marked.
    let mut shutdown = Box::pin(reader.shutdown());
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut shutdown)
            .await
            .is_err()
    );
    assert!(reader.retain_request().is_err());
    let pins = fixture.store.packed_reader_pin_roots().await.unwrap();
    assert!(pins.bindings.contains(fixture.target));
    drop(pins);
    assert!(matches!(
        fixture
            .store
            .retire_packed_binding_history(
                fixture.client.clone(),
                fixture.budget.clone(),
                fixture.options(),
            )
            .await,
        Err(WorkspaceError::Busy | WorkspaceError::Fenced)
    ));
    fixture.assert_retained(&original_root, &references).await;
    drop(request_owner);
    tokio::time::timeout(Duration::from_secs(10), &mut shutdown)
        .await
        .unwrap()
        .unwrap();
    drop(shutdown);
    let pins = fixture.store.packed_reader_pin_roots().await.unwrap();
    assert!(!pins.bindings.contains(fixture.target));
    drop(pins);
    drop(reader);

    let observation = fixture
        .store
        .retire_packed_binding_history(
            fixture.client.clone(),
            fixture.budget.clone(),
            fixture.options(),
        )
        .await
        .unwrap();
    assert!(observation.observing);
    assert!(!observation.retired);
    assert_eq!(observation.released_members, 0);
    assert_eq!(observation.deleted_objects, 0);
    assert_eq!(observation.quarantined_objects, 0);
    fixture.assert_retained(&original_root, &references).await;
    let before_grace = fixture
        .store
        .retire_packed_binding_history(
            fixture.client.clone(),
            fixture.budget.clone(),
            fixture.options(),
        )
        .await
        .unwrap();
    assert!(before_grace.observing);
    assert!(!before_grace.retired);
    assert_eq!(before_grace.not_before_ns, observation.not_before_ns);
    assert_eq!(before_grace.released_members, 0);
    assert_eq!(before_grace.deleted_objects, 0);
    assert_eq!(before_grace.quarantined_objects, 0);
    fixture.assert_retained(&original_root, &references).await;
    // Do not substitute the test host's wall clock for the backend clock.
    tokio::time::timeout(Duration::from_secs(45), async {
        while fixture.backend.server_time_ns().await.unwrap() < observation.not_before_ns {
            fixture.assert_retained(&original_root, &references).await;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
    let retired = fixture
        .store
        .retire_packed_binding_history(
            fixture.client.clone(),
            fixture.budget.clone(),
            fixture.options(),
        )
        .await
        .unwrap();
    assert!(!retired.observing);
    assert!(retired.retired);
    assert_eq!(retired.not_before_ns, observation.not_before_ns);
    assert_eq!(retired.released_members, committed.object_count);
    assert!(retired.deleted_objects > 0);
    assert_eq!(retired.quarantined_objects, 0);
    assert!(
        !fixture
            .objects
            .join("objects")
            .join(&fixture.target.binding.manifest.key)
            .exists()
    );
    assert_eq!(
        references
            .iter()
            .filter(|reference| !fixture
                .objects
                .join("objects")
                .join(&reference.key)
                .exists())
            .count() as u64,
        retired.deleted_objects
    );
    assert!(
        fixture
            .objects
            .join("objects")
            .join(&fixture.anchor.binding.manifest.key)
            .is_file()
    );
    assert_eq!(
        fixture
            .backend
            .get(&packed_history_key(fixture.anchor.workspace_id, 1))
            .await
            .unwrap(),
        Some(fixture.anchor.encode().unwrap())
    );
    for key in [
        packed_current_key(fixture.target.workspace_id),
        packed_claim_key(fixture.target.workspace_id),
        packed_history_key(
            fixture.target.workspace_id,
            fixture.target.binding.binding_version,
        ),
    ] {
        assert!(fixture.backend.get(&key).await.unwrap().is_none());
    }
    let terminal = fixture
        .backend
        .get(&fixture.root_key())
        .await
        .unwrap()
        .unwrap();
    assert_ne!(terminal, original_root);
    assert_eq!(
        fixture
            .backend
            .get(&fixture.history_root_key())
            .await
            .unwrap(),
        Some(terminal.clone())
    );
    let queue_prefix = format!(
        "packed/registry/history-delete-queue/{}/",
        committed.source.staging_id.simple()
    );
    assert!(
        fixture
            .backend
            .scan_prefix(queue_prefix.as_bytes())
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        fixture
            .backend
            .get(&journal_key(committed.journal_id))
            .await
            .unwrap(),
        Some(committed_raw)
    );
    let repeat = fixture
        .store
        .retire_packed_binding_history(
            fixture.client.clone(),
            fixture.budget.clone(),
            fixture.options(),
        )
        .await
        .unwrap();
    assert!(repeat.retired);
    assert!(!repeat.observing);
    assert_eq!(repeat.not_before_ns, retired.not_before_ns);
    assert_eq!(repeat.released_members, 0);
    assert_eq!(repeat.deleted_objects, 0);
    assert_eq!(repeat.quarantined_objects, 0);
    assert_eq!(
        fixture.backend.get(&fixture.root_key()).await.unwrap(),
        Some(terminal)
    );
}

#[tokio::test]
#[ignore = "requires BREWFS_TEST_REDIS_URL; owns UUID namespace and actual LocalFS objects"]
async fn real_redis_authenticated_history_grace_reader_drain_membership_and_delete() {
    redis(Delivery::HistoryRetirement).await
}

#[tokio::test]
#[ignore = "requires BREWFS_TEST_TIKV_PD_ENDPOINTS; owns UUID namespace and actual LocalFS objects"]
async fn real_tikv_authenticated_history_grace_reader_drain_membership_and_delete() {
    tikv(Delivery::HistoryRetirement).await
}
