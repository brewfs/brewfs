//! Backend-neutral workspace control-plane facade used by the Kubernetes controllers.

use std::sync::Arc;

use anyhow::{anyhow, bail, Context as _};
use async_trait::async_trait;
use brewfs::workspace_overlay::admin as catalog_admin;
pub use brewfs::workspace_overlay::admin::WorkspaceView;
use brewfs::workspace_overlay::catalog::{
    CreateSnapshot, CreateVolumeRoot, CreateWorkspace, WorkspaceStore,
};
use brewfs::workspace_overlay::error::WorkspaceError;
use brewfs::workspace_overlay::ids::{LayerId, LeaseId, SnapshotId, WorkspaceId};
use brewfs::workspace_overlay::model::{
    BaseRevision, VolumeHeader, WorkspaceRecord, WORKSPACE_SCHEMA_VERSION,
};
#[cfg(test)]
use brewfs::workspace_overlay::model::{LayerState, WorkspaceState};
use brewfs::workspace_overlay::stores::kv_store::KvWorkspaceStore;
use uuid::Uuid;

use super::crd::{WorkspaceClusterSpec, WorkspaceRevision};

// This facade bootstraps the native catalog; it does not advertise packed lower support.
const OPERATOR_VOLUME_FORMAT: &str = "workspace-v1";

#[derive(Clone, Debug)]
pub struct VolumeIdentity {
    pub volume_id: Uuid,
    pub root_workspace_id: WorkspaceId,
    pub root_layer_id: LayerId,
    pub writable_layer_id: LayerId,
    pub root_snapshot_id: SnapshotId,
    pub owner_id: String,
}

#[derive(Clone, Debug)]
pub struct VolumeView {
    pub volume_id: Uuid,
    pub root_snapshot_id: SnapshotId,
    pub root_revision: BaseRevision,
    pub packed_binding:
        Option<brewfs::workspace_overlay::publish::binding::PackedLowerBindingRecord>,
}

#[derive(Clone, Debug)]
pub struct EnsureWorkspaceRequest {
    pub workspace_id: WorkspaceId,
    pub head_layer_id: LayerId,
    pub base_revision: BaseRevision,
    pub owner_id: Option<String>,
}

#[derive(Clone, Debug)]
pub struct EnsureSnapshotRequest {
    pub snapshot_id: SnapshotId,
    pub name: String,
    pub revision: BaseRevision,
    pub owner_id: Option<String>,
}

#[derive(Clone, Debug)]
pub struct SnapshotView {
    pub snapshot_id: SnapshotId,
    pub revision: BaseRevision,
}

#[async_trait]
pub trait WorkspaceAdmin: Send + Sync {
    /// Transfer this administrator's owned authenticated scope into a GC owner.
    async fn packed_gc_admin(
        self: Arc<Self>,
    ) -> anyhow::Result<Arc<dyn brewfs::workspace_overlay::packed_admin::PackedGcAdmin>> {
        bail!("this workspace administrator has no packed-v3 GC capability")
    }
    async fn packed_binding(
        &self,
        _id: WorkspaceId,
    ) -> anyhow::Result<Option<brewfs::workspace_overlay::publish::binding::PackedLowerBindingRecord>>
    {
        Ok(None)
    }
    async fn ensure_packed_workspace(
        &self,
        _request: EnsureWorkspaceRequest,
    ) -> anyhow::Result<WorkspaceView> {
        bail!("packed-v3 carrier fork is unsupported")
    }
    async fn verify_clean_packed_mount(
        &self,
        _reference: brewfs::workspace_overlay::packed_admin::PackedReleasedMountReference,
    ) -> anyhow::Result<bool> {
        Ok(false)
    }
    async fn clean_released_packed_mount(
        &self,
        _workspace: WorkspaceId,
    ) -> anyhow::Result<Option<brewfs::workspace_overlay::packed_admin::PackedReleasedMountReference>>
    {
        Ok(None)
    }
    async fn recovered_packed_mount(
        &self,
        _workspace: WorkspaceId,
    ) -> anyhow::Result<Option<brewfs::workspace_overlay::packed_admin::PackedRecoveredMountReport>>
    {
        Ok(None)
    }
    /// Routing report only; the recovery driver must acquire the next owner
    /// with the actual Redis/TiKV same-snapshot expiry/CAS protocol.
    async fn expired_packed_mount_recovery(
        &self,
        _original: brewfs::workspace_overlay::packed_admin::PackedReleasedMountReference,
    ) -> anyhow::Result<
        Option<(
            brewfs::workspace_overlay::packed_admin::PackedReleasedMountReference,
            u64,
        )>,
    > {
        Ok(None)
    }
    /// Facts-only retry hint for an attempt whose lease never acquired authority.
    async fn unstarted_packed_mount_recovery(
        &self,
        _original: brewfs::workspace_overlay::packed_admin::PackedReleasedMountReference,
        _failed_lease: LeaseId,
        _next_lease: LeaseId,
    ) -> anyhow::Result<Option<u64>> {
        Ok(None)
    }
    /// Cleanup-only original PCR verification, including a consumed source.
    async fn original_packed_mount_for_cleanup(
        &self,
        _original: brewfs::workspace_overlay::packed_admin::PackedReleasedMountReference,
    ) -> anyhow::Result<bool> {
        Ok(false)
    }
    /// Cleanup-only proof for this exact original mounted session. This report
    /// does not grant snapshot publication or mutable recovery authority.
    async fn packed_mount_recovery_for_cleanup(
        &self,
        _original: brewfs::workspace_overlay::packed_admin::PackedReleasedMountReference,
    ) -> anyhow::Result<Option<brewfs::workspace_overlay::packed_admin::PackedRecoveredMountReport>>
    {
        Ok(None)
    }
    async fn clean_published_view(
        &self,
        _workspace: WorkspaceId,
    ) -> anyhow::Result<Option<brewfs::workspace_overlay::packed_admin::PackedPublishedViewReport>>
    {
        Ok(None)
    }
    async fn clean_unmounted_packed_epoch(
        &self,
        _workspace: WorkspaceId,
    ) -> anyhow::Result<Option<u64>> {
        Ok(None)
    }
    async fn pin_clean_packed_snapshot(
        &self,
        _workspace: WorkspaceId,
        _request: EnsureSnapshotRequest,
    ) -> anyhow::Result<SnapshotView> {
        bail!("verified packed-v3 source pin is unsupported")
    }
    async fn verify_packed_revision(&self, _revision: &BaseRevision) -> anyhow::Result<()> {
        bail!("packed-v3 carrier inspection is unsupported")
    }
    async fn publish_packed_snapshot(
        &self,
        _reference: brewfs::workspace_overlay::packed_admin::PackedReleasedMountReference,
        _request: EnsureSnapshotRequest,
        _ttl_seconds: u32,
    ) -> anyhow::Result<SnapshotView> {
        bail!("packed-v3 snapshot is unsupported")
    }
    async fn recover_packed_snapshot(
        &self,
        _workspace: WorkspaceId,
        _request: EnsureSnapshotRequest,
        _ttl_seconds: u32,
    ) -> anyhow::Result<SnapshotView> {
        bail!("packed-v3 snapshot recovery is unsupported")
    }
    async fn ensure_volume(&self, identity: VolumeIdentity) -> anyhow::Result<VolumeView>;
    async fn ensure_workspace(
        &self,
        request: EnsureWorkspaceRequest,
    ) -> anyhow::Result<WorkspaceView>;
    async fn inspect_workspace(&self, id: WorkspaceId) -> anyhow::Result<WorkspaceView>;
    async fn verify_revision(&self, revision: &BaseRevision) -> anyhow::Result<()>;
    async fn ensure_snapshot(&self, request: EnsureSnapshotRequest)
        -> anyhow::Result<SnapshotView>;
    async fn load_snapshot(&self, id: SnapshotId) -> anyhow::Result<SnapshotView>;
    async fn delete_snapshot(&self, id: SnapshotId) -> anyhow::Result<()>;
    async fn mark_workspace_deleting(&self, id: WorkspaceId, force: bool) -> anyhow::Result<()>;
    async fn reap_expired_leases(&self) -> anyhow::Result<u64>;
    async fn list_workspaces(&self) -> anyhow::Result<Vec<WorkspaceRecord>>;
    async fn list_snapshots(&self) -> anyhow::Result<Vec<SnapshotView>>;
}

pub fn catalog_namespace(
    cluster_name: &str,
    kubernetes_namespace: &str,
    spec: &WorkspaceClusterSpec,
) -> String {
    spec.namespace
        .clone()
        .unwrap_or_else(|| format!("{kubernetes_namespace}-{cluster_name}"))
}

struct StoreWorkspaceAdmin<W> {
    store: Arc<W>,
}

impl<W> StoreWorkspaceAdmin<W> {
    fn new(store: W) -> Self {
        Self {
            store: Arc::new(store),
        }
    }
}

#[async_trait]
impl<W> WorkspaceAdmin for StoreWorkspaceAdmin<W>
where
    W: WorkspaceStore + 'static,
{
    async fn ensure_volume(&self, identity: VolumeIdentity) -> anyhow::Result<VolumeView> {
        self.store.initialize_workspace_schema().await?;
        match self.store.load_volume_header().await? {
            Some(header) => validate_operator_volume_header(&header, identity.volume_id)?,
            None => {
                self.store
                    .create_volume_root(CreateVolumeRoot {
                        volume_format: OPERATOR_VOLUME_FORMAT.into(),
                        schema_version: WORKSPACE_SCHEMA_VERSION,
                        volume_id: identity.volume_id,
                        workspace_id: identity.root_workspace_id,
                        root_layer_id: identity.root_layer_id,
                        writable_layer_id: identity.writable_layer_id,
                        owner_id: Some(identity.owner_id.clone()),
                    })
                    .await?;
            }
        }

        let root = self
            .inspect_workspace(identity.root_workspace_id)
            .await
            .context("inspect reserved root workspace")?;
        self.ensure_snapshot(EnsureSnapshotRequest {
            snapshot_id: identity.root_snapshot_id,
            name: format!("{}-root", identity.owner_id),
            revision: root.base_revision.clone(),
            owner_id: Some(identity.owner_id),
        })
        .await?;
        Ok(VolumeView {
            volume_id: identity.volume_id,
            root_snapshot_id: identity.root_snapshot_id,
            root_revision: root.base_revision,
            packed_binding: None,
        })
    }

    async fn ensure_workspace(
        &self,
        request: EnsureWorkspaceRequest,
    ) -> anyhow::Result<WorkspaceView> {
        Ok(catalog_admin::ensure_workspace_for_reconcile(
            self.store.as_ref(),
            CreateWorkspace {
                workspace_id: request.workspace_id,
                head_layer_id: request.head_layer_id,
                base_revision: request.base_revision,
                owner_id: request.owner_id,
            },
        )
        .await?)
    }

    async fn inspect_workspace(&self, id: WorkspaceId) -> anyhow::Result<WorkspaceView> {
        Ok(catalog_admin::inspect_workspace(self.store.as_ref(), id).await?)
    }

    async fn verify_revision(&self, revision: &BaseRevision) -> anyhow::Result<()> {
        Ok(catalog_admin::verify_revision(self.store.as_ref(), revision).await?)
    }

    async fn ensure_snapshot(
        &self,
        request: EnsureSnapshotRequest,
    ) -> anyhow::Result<SnapshotView> {
        self.verify_revision(&request.revision).await?;
        if let Some(existing) = self
            .store
            .list_snapshots()
            .await?
            .into_iter()
            .find(|snapshot| snapshot.snapshot_id == request.snapshot_id)
        {
            if existing.revision != request.revision
                || existing.name.as_deref() != Some(&request.name)
            {
                bail!("deterministic snapshot ID exists with different content");
            }
            return Ok(SnapshotView {
                snapshot_id: existing.snapshot_id,
                revision: existing.revision,
            });
        }
        let snapshot = self
            .store
            .create_snapshot(CreateSnapshot {
                snapshot_id: request.snapshot_id,
                name: Some(request.name),
                revision: request.revision,
                owner_id: request.owner_id,
            })
            .await?;
        Ok(SnapshotView {
            snapshot_id: snapshot.snapshot_id,
            revision: snapshot.revision,
        })
    }

    async fn load_snapshot(&self, id: SnapshotId) -> anyhow::Result<SnapshotView> {
        let snapshot = self.store.load_snapshot(id).await?;
        self.verify_revision(&snapshot.revision).await?;
        Ok(SnapshotView {
            snapshot_id: snapshot.snapshot_id,
            revision: snapshot.revision,
        })
    }

    async fn delete_snapshot(&self, id: SnapshotId) -> anyhow::Result<()> {
        match self.store.delete_snapshot(id).await {
            Ok(()) | Err(WorkspaceError::SnapshotNotFound(_)) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    async fn mark_workspace_deleting(&self, id: WorkspaceId, force: bool) -> anyhow::Result<()> {
        Ok(catalog_admin::mark_workspace_deleting(self.store.as_ref(), id, force).await?)
    }

    async fn reap_expired_leases(&self) -> anyhow::Result<u64> {
        Ok(self.store.reap_expired_leases().await?)
    }

    async fn list_workspaces(&self) -> anyhow::Result<Vec<WorkspaceRecord>> {
        Ok(self.store.list_workspaces().await?)
    }

    async fn list_snapshots(&self) -> anyhow::Result<Vec<SnapshotView>> {
        Ok(self
            .store
            .list_snapshots()
            .await?
            .into_iter()
            .map(|snapshot| SnapshotView {
                snapshot_id: snapshot.snapshot_id,
                revision: snapshot.revision,
            })
            .collect())
    }
}

fn validate_operator_volume_header(
    header: &VolumeHeader,
    expected_volume_id: Uuid,
) -> anyhow::Result<()> {
    Ok(catalog_admin::validate_volume_header(
        header,
        expected_volume_id,
    )?)
}

pub fn revision_to_status(revision: &BaseRevision) -> WorkspaceRevision {
    WorkspaceRevision {
        layer_id: revision.layer_id.to_string(),
        sealed_version: revision.sealed_version,
        root_hash: hex::encode(revision.root_hash),
    }
}

pub fn revision_from_status(revision: &WorkspaceRevision) -> anyhow::Result<BaseRevision> {
    let decoded = hex::decode(&revision.root_hash).context("decode workspace root hash")?;
    let root_hash: [u8; 32] = decoded
        .try_into()
        .map_err(|_| anyhow!("workspace root hash must be 32 bytes"))?;
    Ok(BaseRevision {
        layer_id: revision
            .layer_id
            .parse()
            .context("parse workspace layer ID")?,
        sealed_version: revision.sealed_version,
        root_hash,
    })
}

pub fn deterministic_uuid(scope: Uuid, value: &str) -> Uuid {
    Uuid::new_v5(&scope, value.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn native_volume_bootstrap_preserves_header_root_and_snapshot_on_retry() {
        use brewfs::workspace_overlay::stores::database::SqliteWorkspaceStore;

        let admin = StoreWorkspaceAdmin::new(
            SqliteWorkspaceStore::connect("sqlite::memory:")
                .await
                .unwrap(),
        );
        let identity = VolumeIdentity {
            volume_id: Uuid::from_u128(1),
            root_workspace_id: WorkspaceId::from_uuid(Uuid::from_u128(2)),
            root_layer_id: LayerId::from_uuid(Uuid::from_u128(3)),
            writable_layer_id: LayerId::from_uuid(Uuid::from_u128(4)),
            root_snapshot_id: SnapshotId::from_uuid(Uuid::from_u128(5)),
            owner_id: "k8s-cluster/test/native".into(),
        };
        let first = admin.ensure_volume(identity.clone()).await.unwrap();
        let header = admin.store.load_volume_header().await.unwrap().unwrap();
        let workspace = admin
            .store
            .load_workspace(identity.root_workspace_id)
            .await
            .unwrap();
        let root = admin
            .store
            .load_layer(identity.root_layer_id)
            .await
            .unwrap();
        let snapshot = admin
            .store
            .load_snapshot(identity.root_snapshot_id)
            .await
            .unwrap();

        assert_eq!(header.volume_format, OPERATOR_VOLUME_FORMAT);
        assert_eq!(header.schema_version, WORKSPACE_SCHEMA_VERSION);
        assert_eq!(header.volume_id, identity.volume_id);
        assert_eq!(workspace.head_layer_id, identity.writable_layer_id);
        assert_eq!(
            workspace.owner_id.as_deref(),
            Some(identity.owner_id.as_str())
        );
        assert_eq!(root.state, LayerState::Sealed);
        assert_eq!(root.schema_version, WORKSPACE_SCHEMA_VERSION);
        assert_eq!(root.owner_workspace_id, None);
        assert_eq!(root.parent_layer_id, None);
        assert_eq!(root.depth, 1);
        assert_eq!(first.root_revision.layer_id, identity.root_layer_id);
        assert_eq!(snapshot.revision, first.root_revision);
        assert_eq!(
            snapshot.owner_id.as_deref(),
            Some(identity.owner_id.as_str())
        );

        let retry = admin.ensure_volume(identity.clone()).await.unwrap();
        assert_eq!(retry.volume_id, first.volume_id);
        assert_eq!(retry.root_snapshot_id, first.root_snapshot_id);
        assert_eq!(retry.root_revision, first.root_revision);
        assert_eq!(
            admin.store.load_volume_header().await.unwrap(),
            Some(header)
        );
        assert_eq!(
            admin
                .store
                .load_workspace(identity.root_workspace_id)
                .await
                .unwrap(),
            workspace
        );
        assert_eq!(
            admin
                .store
                .load_layer(identity.root_layer_id)
                .await
                .unwrap(),
            root
        );
        assert_eq!(
            admin
                .store
                .load_snapshot(identity.root_snapshot_id)
                .await
                .unwrap(),
            snapshot
        );
        assert_eq!(admin.store.list_workspaces().await.unwrap().len(), 1);
        assert_eq!(admin.store.list_snapshots().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn deletion_finalizer_operations_are_idempotent_after_backend_marker() {
        use brewfs::workspace_overlay::stores::database::SqliteWorkspaceStore;

        let admin = StoreWorkspaceAdmin::new(
            SqliteWorkspaceStore::connect("sqlite::memory:")
                .await
                .unwrap(),
        );
        let identity = VolumeIdentity {
            volume_id: Uuid::from_u128(101),
            root_workspace_id: WorkspaceId::from_uuid(Uuid::from_u128(102)),
            root_layer_id: LayerId::from_uuid(Uuid::from_u128(103)),
            writable_layer_id: LayerId::from_uuid(Uuid::from_u128(104)),
            root_snapshot_id: SnapshotId::from_uuid(Uuid::from_u128(105)),
            owner_id: "k8s-cluster/test/idempotent".into(),
        };
        let volume = admin.ensure_volume(identity.clone()).await.unwrap();

        admin
            .mark_workspace_deleting(identity.root_workspace_id, false)
            .await
            .unwrap();
        // SQLite reports InvalidStateTransition on the second call; the KV
        // backends report WorkspaceNotFound. Both are accepted only after the
        // authoritative record confirms the durable Deleting marker.
        admin
            .mark_workspace_deleting(identity.root_workspace_id, false)
            .await
            .unwrap();
        assert_eq!(
            admin
                .store
                .load_workspace(identity.root_workspace_id)
                .await
                .unwrap()
                .state,
            WorkspaceState::Deleting
        );

        admin
            .delete_snapshot(volume.root_snapshot_id)
            .await
            .unwrap();
        // A retried Kubernetes finalizer may observe a previously deleted
        // snapshot. Missing is terminal success; backend errors still fail.
        admin
            .delete_snapshot(volume.root_snapshot_id)
            .await
            .unwrap();
    }

    #[test]
    fn operator_header_guard_rejects_unsupported_formats_schema_and_owner() {
        let expected_volume_id = Uuid::from_u128(1);
        let header = VolumeHeader {
            volume_format: OPERATOR_VOLUME_FORMAT.into(),
            schema_version: WORKSPACE_SCHEMA_VERSION,
            volume_id: expected_volume_id,
            created_at_ns: 0,
        };
        validate_operator_volume_header(&header, expected_volume_id).unwrap();
        for volume_format in ["packed-metadata-v3", "workspace-native-v2"] {
            let unsupported = VolumeHeader {
                volume_format: volume_format.into(),
                ..header.clone()
            };
            assert!(validate_operator_volume_header(&unsupported, expected_volume_id).is_err());
        }
        let unsupported_schema = VolumeHeader {
            schema_version: WORKSPACE_SCHEMA_VERSION + 1,
            ..header.clone()
        };
        assert!(validate_operator_volume_header(&unsupported_schema, expected_volume_id).is_err());
        assert!(validate_operator_volume_header(&header, Uuid::from_u128(99)).is_err());
    }

    #[test]
    fn revision_status_round_trips_exact_identity() {
        let revision = BaseRevision {
            layer_id: LayerId::from_uuid(Uuid::from_u128(7)),
            sealed_version: 9,
            root_hash: [0xab; 32],
        };
        let status = revision_to_status(&revision);
        assert_eq!(revision_from_status(&status).unwrap(), revision);
    }

    #[test]
    fn deterministic_ids_are_stable_and_scoped() {
        let scope = Uuid::from_u128(11);
        assert_eq!(
            deterministic_uuid(scope, "workspace"),
            deterministic_uuid(scope, "workspace")
        );
        assert_ne!(
            deterministic_uuid(scope, "workspace"),
            deterministic_uuid(scope, "snapshot")
        );
    }
}

#[path = "packed_admin.rs"]
mod packed_admin;
pub use packed_admin::connect_workspace_admin_for_cluster;
