//! Platform-independent catalog administration shared by Manager and Operator.
//!
//! These helpers never seal a live workspace, acquire a writer lease, or reclaim
//! object data. Callers must persist creation identities before invoking them.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::catalog::{CreateWorkspace, MarkDeleting, WorkspaceStore};
use super::error::{ConflictDetail, WorkspaceError};
use super::ids::WorkspaceId;
use super::model::{
    BaseRevision, LayerRecord, LayerState, SnapshotLease, VolumeHeader, WORKSPACE_SCHEMA_VERSION,
    WorkspaceRecord, WorkspaceState,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkspaceView {
    pub record: WorkspaceRecord,
    pub base_revision: BaseRevision,
    pub layer_depth: u32,
    pub leases: Vec<SnapshotLease>,
}

pub fn validate_volume_header(
    header: &VolumeHeader,
    expected_volume_id: Uuid,
) -> Result<(), WorkspaceError> {
    if header.volume_id != expected_volume_id {
        return Err(conflict("catalog belongs to a different volume"));
    }
    if header.volume_format != "workspace-v1" {
        return Err(WorkspaceError::UnsupportedVolumeFormat(
            header.volume_format.clone(),
        ));
    }
    if header.schema_version != WORKSPACE_SCHEMA_VERSION {
        return Err(WorkspaceError::UnsupportedSchemaVersion(
            header.schema_version,
        ));
    }
    Ok(())
}

pub fn revision_from_layer(layer: &LayerRecord) -> Result<BaseRevision, WorkspaceError> {
    if layer.schema_version != WORKSPACE_SCHEMA_VERSION {
        return Err(WorkspaceError::UnsupportedSchemaVersion(
            layer.schema_version,
        ));
    }
    if layer.state != LayerState::Sealed
        || layer.owner_workspace_id.is_some()
        || layer.parent_layer_id.is_some()
        || layer.depth != 1
    {
        return Err(conflict("revision is not an immutable flat sealed base"));
    }
    if layer.delta_digest.is_none() {
        return Err(WorkspaceError::CorruptMetadata(
            "sealed layer has no delta digest".into(),
        ));
    }
    Ok(BaseRevision {
        layer_id: layer.layer_id,
        sealed_version: layer
            .sealed_version
            .ok_or_else(|| WorkspaceError::CorruptMetadata("sealed layer has no version".into()))?,
        root_hash: layer.root_hash.ok_or_else(|| {
            WorkspaceError::CorruptMetadata("sealed layer has no root hash".into())
        })?,
    })
}

pub async fn verify_revision<W: WorkspaceStore + ?Sized>(
    store: &W,
    revision: &BaseRevision,
) -> Result<(), WorkspaceError> {
    let actual = revision_from_layer(&store.load_layer(revision.layer_id).await?)?;
    if actual != *revision {
        return Err(conflict("sealed revision tuple does not match catalog"));
    }
    Ok(())
}

pub async fn inspect_workspace<W: WorkspaceStore + ?Sized>(
    store: &W,
    id: WorkspaceId,
) -> Result<WorkspaceView, WorkspaceError> {
    let record = store.load_workspace(id).await?;
    let deleting = record.state == WorkspaceState::Deleting;
    // Deleting heads are deliberately rejected by the data-path chain loader.
    // This is an administrative report, not permission to resolve or mount one.
    let chain = if deleting {
        let head = store.load_layer(record.head_layer_id).await?;
        let parent = head.parent_layer_id.ok_or_else(|| {
            WorkspaceError::CorruptMetadata("deleting workspace has no base layer".into())
        })?;
        vec![head, store.load_layer(parent).await?]
    } else {
        store.load_layer_chain(record.head_layer_id).await?
    };
    let [head, base] = chain.as_slice() else {
        return Err(WorkspaceError::CorruptMetadata(
            "workspace must contain one writable and one sealed layer".into(),
        ));
    };
    let valid_head_state = if deleting {
        head.state == LayerState::Deleting
            && head.owner_workspace_id.is_none()
            && record.active_lease.is_none()
    } else {
        head.state == LayerState::Writable && head.owner_workspace_id == Some(id)
    };
    if !valid_head_state
        || head.parent_layer_id != Some(base.layer_id)
        || head.depth != 2
        || head.schema_version != WORKSPACE_SCHEMA_VERSION
    {
        return Err(WorkspaceError::CorruptMetadata(
            "workspace violates the fixed two-layer invariant".into(),
        ));
    }
    let base_revision = revision_from_layer(base)?;
    let leases = store.list_leases(id).await?;
    Ok(WorkspaceView {
        record,
        base_revision,
        layer_depth: head.depth,
        leases,
    })
}

/// Reconcile a previously persisted creation identity against the actual catalog.
pub async fn ensure_workspace<W: WorkspaceStore + ?Sized>(
    store: &W,
    request: CreateWorkspace,
) -> Result<WorkspaceView, WorkspaceError> {
    ensure_workspace_inner(store, request, false).await
}

async fn ensure_workspace_inner<W: WorkspaceStore + ?Sized>(
    store: &W,
    request: CreateWorkspace,
    allow_progress: bool,
) -> Result<WorkspaceView, WorkspaceError> {
    match store.load_workspace(request.workspace_id).await {
        Ok(_) => {}
        Err(WorkspaceError::WorkspaceNotFound(_)) => {
            verify_revision(store, &request.base_revision).await?;
            if let Err(error) = store.create_workspace(request.clone()).await {
                store
                    .load_workspace(request.workspace_id)
                    .await
                    .map_err(|_| error)?;
            }
        }
        Err(error) => return Err(error),
    }
    let view = inspect_workspace(store, request.workspace_id).await?;
    // Always revalidate after creation as well as a failed/ambiguous reply:
    // another creator might have won the same ID with different content.
    if view.record.owner_id != request.owner_id
        || view.record.upper_bucket != request.upper_bucket
        || view.record.state != WorkspaceState::Active
    {
        return Err(conflict(
            "workspace ID has a different owner or upper bucket, or is not active",
        ));
    }
    if !allow_progress && view.record.head_epoch != 0 {
        return Err(conflict(
            "workspace advanced beyond the recorded creation identity",
        ));
    }
    if view.record.head_epoch == 0
        && (view.record.head_layer_id != request.head_layer_id
            || view.record.fork_base.as_ref() != Some(&request.base_revision)
            || view.base_revision != request.base_revision)
    {
        return Err(conflict(
            "workspace differs from the recorded creation identity",
        ));
    }
    Ok(view)
}

/// Operator reconciliation also accepts a workspace which has legitimately
/// advanced since its creation. Its controller still owns the resource ID.
pub async fn ensure_workspace_for_reconcile<W: WorkspaceStore + ?Sized>(
    store: &W,
    request: CreateWorkspace,
) -> Result<WorkspaceView, WorkspaceError> {
    ensure_workspace_inner(store, request, true).await
}

/// Persist a logical deletion marker. Physical reclamation is a separate task.
/// Manager always passes `false`; the explicit fencing option remains for the
/// existing Operator policy and is never inferred from a failed request.
pub async fn mark_workspace_deleting<W: WorkspaceStore + ?Sized>(
    store: &W,
    id: WorkspaceId,
    force: bool,
) -> Result<(), WorkspaceError> {
    match store
        .mark_workspace_deleting(MarkDeleting {
            workspace_id: id,
            force_fence_lease: force,
        })
        .await
    {
        Ok(()) => Ok(()),
        Err(error) => match store.load_workspace(id).await {
            Ok(record) if record.state == WorkspaceState::Deleting => Ok(()),
            _ => Err(error),
        },
    }
}

fn conflict(reason: &str) -> WorkspaceError {
    WorkspaceError::Conflict(ConflictDetail {
        path: Vec::new(),
        reason: reason.into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workspace_overlay::catalog::{
        AcquireLease, CreateSnapshot, CreateVolumeRoot, DeleteLayerMetadata, HeadGuard,
        InodeMutation, InodeQuery, ReleaseLease,
    };
    use crate::workspace_overlay::ids::{LayerId, LeaseId, SnapshotId};
    use crate::workspace_overlay::stores::database::SqliteWorkspaceStore;

    async fn fixture() -> (SqliteWorkspaceStore, WorkspaceView) {
        let store = SqliteWorkspaceStore::connect("sqlite::memory:")
            .await
            .unwrap();
        store.initialize_workspace_schema().await.unwrap();
        let root = store
            .create_volume_root(CreateVolumeRoot {
                upper_bucket: None,
                volume_format: "workspace-v1".into(),
                schema_version: WORKSPACE_SCHEMA_VERSION,
                volume_id: Uuid::new_v4(),
                workspace_id: WorkspaceId::new(),
                root_layer_id: LayerId::new(),
                writable_layer_id: LayerId::new(),
                owner_id: None,
            })
            .await
            .unwrap();
        let root = inspect_workspace(&store, root.workspace_id).await.unwrap();
        (store, root)
    }

    fn fork_request(root: &WorkspaceView) -> CreateWorkspace {
        CreateWorkspace {
            upper_bucket: None,
            workspace_id: WorkspaceId::new(),
            head_layer_id: LayerId::new(),
            base_revision: root.base_revision.clone(),
            owner_id: Some("agent-a".into()),
        }
    }

    #[tokio::test]
    async fn replay_requires_the_persisted_head_identity() {
        let (store, root) = fixture().await;
        let request = fork_request(&root);
        ensure_workspace(&store, request.clone()).await.unwrap();
        let mut different = request;
        different.head_layer_id = LayerId::new();
        assert!(matches!(
            ensure_workspace(&store, different).await,
            Err(WorkspaceError::Conflict(_))
        ));
    }

    #[tokio::test]
    async fn replay_requires_the_persisted_upper_bucket_identity() {
        let (store, root) = fixture().await;
        let mut request = fork_request(&root);
        request.upper_bucket = Some("original-child-bucket".into());
        ensure_workspace(&store, request.clone()).await.unwrap();
        request.upper_bucket = Some("different-child-bucket".into());
        assert!(matches!(
            ensure_workspace(&store, request).await,
            Err(WorkspaceError::Conflict(_))
        ));
    }

    #[tokio::test]
    async fn lost_reply_reconciles_committed_creation_without_another_workspace() {
        let (store, root) = fixture().await;
        let request = fork_request(&root);
        let committed = store.create_workspace(request.clone()).await.unwrap();
        let recovered = ensure_workspace(&store, request.clone()).await.unwrap();
        assert_eq!(recovered.record, committed);
        assert_eq!(
            ensure_workspace(&store, request).await.unwrap().record,
            committed
        );
        assert_eq!(store.list_workspaces().await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn fork_rejects_wrong_owner_changed_revision_and_mutable_base() {
        let (store, root) = fixture().await;
        let request = fork_request(&root);
        ensure_workspace(&store, request.clone()).await.unwrap();
        let mut other_owner = request.clone();
        other_owner.owner_id = Some("agent-b".into());
        assert!(ensure_workspace(&store, other_owner).await.is_err());
        let mut other_base = request.clone();
        other_base.base_revision.root_hash[0] ^= 1;
        assert!(ensure_workspace(&store, other_base).await.is_err());
        let mut mutable = fork_request(&root);
        mutable.base_revision.layer_id = root.record.head_layer_id;
        assert!(ensure_workspace(&store, mutable).await.is_err());
        assert_eq!(store.list_workspaces().await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn recovery_refuses_ambiguous_progress_but_operator_can_reconcile() {
        let (store, root) = fixture().await;
        let store = std::sync::Arc::new(store);
        let request = fork_request(&root);
        ensure_workspace(store.as_ref(), request.clone())
            .await
            .unwrap();
        let advanced = crate::workspace_overlay::test_base_fixture::install_test_base(
            &store,
            request.workspace_id,
        )
        .await;
        assert!(matches!(
            ensure_workspace(store.as_ref(), request.clone()).await,
            Err(WorkspaceError::Conflict(_))
        ));
        let view = ensure_workspace_for_reconcile(store.as_ref(), request)
            .await
            .unwrap();
        assert_eq!(view.base_revision, advanced.revision);
        assert_eq!(view.record.head_epoch, advanced.head_epoch);
    }

    #[tokio::test]
    async fn forks_have_separate_writable_heads_and_the_exact_shared_revision() {
        let (store, root) = fixture().await;
        let a = ensure_workspace(&store, fork_request(&root)).await.unwrap();
        let b = ensure_workspace(&store, fork_request(&root)).await.unwrap();
        assert_ne!(a.record.head_layer_id, b.record.head_layer_id);
        assert_eq!(a.base_revision, root.base_revision);
        assert_eq!(b.base_revision, root.base_revision);
        assert_eq!(
            store
                .load_layer(a.record.head_layer_id)
                .await
                .unwrap()
                .owner_workspace_id,
            Some(a.record.workspace_id)
        );
        assert_eq!(
            store
                .load_layer(b.record.head_layer_id)
                .await
                .unwrap()
                .owner_workspace_id,
            Some(b.record.workspace_id)
        );

        let base_inode = store
            .get_inode_deltas(InodeQuery {
                layer_ids: vec![root.base_revision.layer_id],
                ino: 1,
            })
            .await
            .unwrap()
            .remove(0);
        let lease = store
            .acquire_lease(AcquireLease {
                workspace_id: a.record.workspace_id,
                lease_id: LeaseId::new(),
                holder_generation: 1,
                ttl_ns: 60_000_000_000,
            })
            .await
            .unwrap();
        let mut private_inode = base_inode.clone();
        private_inode.layer_id = a.record.head_layer_id;
        private_inode.mode = 0o700;
        store
            .apply_inode_mutation(InodeMutation {
                guard: HeadGuard {
                    workspace_id: a.record.workspace_id,
                    expected_head_layer_id: a.record.head_layer_id,
                    expected_head_epoch: a.record.head_epoch,
                    lease_id: lease.lease_id,
                    holder_generation: lease.holder_generation,
                },
                inode: private_inode,
            })
            .await
            .unwrap();
        let a_delta = store
            .get_inode_deltas(InodeQuery {
                layer_ids: vec![a.record.head_layer_id],
                ino: 1,
            })
            .await
            .unwrap();
        assert_eq!(a_delta[0].mode, 0o700);
        assert!(
            store
                .get_inode_deltas(InodeQuery {
                    layer_ids: vec![b.record.head_layer_id],
                    ino: 1,
                })
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            store
                .get_inode_deltas(InodeQuery {
                    layer_ids: vec![root.base_revision.layer_id],
                    ino: 1,
                })
                .await
                .unwrap(),
            vec![base_inode]
        );
    }

    #[tokio::test]
    async fn logical_delete_refuses_live_lease_and_keeps_referenced_lower() {
        let (store, root) = fixture().await;
        let child = ensure_workspace(&store, fork_request(&root)).await.unwrap();
        let lease = store
            .acquire_lease(AcquireLease {
                workspace_id: child.record.workspace_id,
                lease_id: LeaseId::new(),
                holder_generation: 1,
                ttl_ns: 60_000_000_000,
            })
            .await
            .unwrap();
        assert!(matches!(
            mark_workspace_deleting(&store, child.record.workspace_id, false).await,
            Err(WorkspaceError::Busy)
        ));
        store
            .release_lease(ReleaseLease {
                lease_id: lease.lease_id,
                holder_generation: 1,
            })
            .await
            .unwrap();
        store
            .create_snapshot(CreateSnapshot {
                snapshot_id: SnapshotId::new(),
                name: Some("retained".into()),
                revision: root.base_revision.clone(),
                owner_id: None,
            })
            .await
            .unwrap();
        mark_workspace_deleting(&store, child.record.workspace_id, false)
            .await
            .unwrap();
        mark_workspace_deleting(&store, child.record.workspace_id, false)
            .await
            .unwrap();
        mark_workspace_deleting(&store, root.record.workspace_id, false)
            .await
            .unwrap();
        assert_eq!(
            store
                .load_workspace(child.record.workspace_id)
                .await
                .unwrap()
                .state,
            WorkspaceState::Deleting
        );
        let deleted = inspect_workspace(&store, child.record.workspace_id)
            .await
            .unwrap();
        assert_eq!(deleted.record.state, WorkspaceState::Deleting);
        assert_eq!(deleted.base_revision, root.base_revision);
        assert_eq!(
            store
                .load_layer(root.base_revision.layer_id)
                .await
                .unwrap()
                .state,
            LayerState::Sealed
        );
        assert!(matches!(
            store
                .delete_layer_metadata(DeleteLayerMetadata {
                    layer_ids: vec![root.base_revision.layer_id],
                    now_ns: i64::MAX,
                    lease_grace_ns: 0,
                })
                .await,
            Err(WorkspaceError::Busy)
        ));
        verify_revision(&store, &root.base_revision).await.unwrap();
    }
}
