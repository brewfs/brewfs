//! Workspace control-plane read models used by CLI/API adapters.

use std::sync::Arc;

use serde::Serialize;

use super::catalog::WorkspaceStore;
use super::error::WorkspaceError;
use super::ids::{JournalId, LayerId, LeaseId, WorkspaceId};
use super::model::{BaseRevision, LeaseState, SealPhase, WorkspaceState};

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct WorkspaceInspection {
    pub workspace_id: WorkspaceId,
    pub upper_bucket: Option<String>,
    pub state: WorkspaceState,
    pub owner_id: Option<String>,
    pub head_layer_id: LayerId,
    pub head_epoch: u64,
    pub base_revision: Option<BaseRevision>,
    pub layer_depth: u32,
    pub lease_id: Option<LeaseId>,
    pub holder_generation: Option<u64>,
    pub lease_expires_at_ns: Option<i64>,
    pub private_metadata_rows: u64,
    pub private_bytes: u64,
    pub shared_bytes: u64,
    pub pending_writeback_bytes: u64,
    pub last_seal_journal_id: Option<JournalId>,
    pub last_seal_phase: Option<SealPhase>,
    pub last_seal_error: Option<String>,
}

pub struct WorkspaceControl<W> {
    store: Arc<W>,
}

impl<W: WorkspaceStore + 'static> WorkspaceControl<W> {
    pub fn new(store: Arc<W>) -> Self {
        Self { store }
    }

    pub async fn inspect(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<WorkspaceInspection, WorkspaceError> {
        let workspace = self.store.load_workspace(workspace_id).await?;
        // Discard makes the head unresolvable, and GC may later remove its
        // layers. Keep the durable identity available for explicit cleanup;
        // live layer/lease statistics no longer apply to a discarded workspace.
        if workspace.state == WorkspaceState::Deleting {
            return Ok(WorkspaceInspection {
                workspace_id,
                upper_bucket: workspace.upper_bucket,
                state: workspace.state,
                owner_id: workspace.owner_id,
                head_layer_id: workspace.head_layer_id,
                head_epoch: workspace.head_epoch,
                base_revision: workspace.fork_base,
                layer_depth: 0,
                lease_id: None,
                holder_generation: None,
                lease_expires_at_ns: None,
                private_metadata_rows: 0,
                private_bytes: 0,
                shared_bytes: 0,
                pending_writeback_bytes: 0,
                last_seal_journal_id: None,
                last_seal_phase: None,
                last_seal_error: None,
            });
        }
        let chain = self.store.load_layer_chain(workspace.head_layer_id).await?;
        let head = chain.first().ok_or_else(|| {
            WorkspaceError::CorruptMetadata("workspace has an empty layer chain".into())
        })?;
        let delta = self.store.load_layer_delta(head.layer_id).await?;
        let private_metadata_rows = u64::try_from(
            delta.dentries.len()
                + delta.inodes.len()
                + delta.xattrs.len()
                + delta.acls.len()
                + delta.extents.len(),
        )
        .map_err(|_| WorkspaceError::CorruptMetadata("row count exceeds u64".into()))?;
        let mut private_bytes = head.owned_bytes;
        for row in &delta.dentries {
            private_bytes = private_bytes.saturating_add(row.name.len() as u64);
        }
        for row in &delta.xattrs {
            private_bytes = private_bytes
                .saturating_add(row.name.len() as u64)
                .saturating_add(row.value.as_ref().map_or(0, |value| value.len()) as u64);
        }
        for row in &delta.acls {
            private_bytes = private_bytes
                .saturating_add(row.value.as_ref().map_or(0, |value| value.len()) as u64);
        }
        let shared_bytes = chain
            .iter()
            .skip(1)
            .fold(0u64, |total, layer| total.saturating_add(layer.owned_bytes));
        let lease = self
            .store
            .list_leases(workspace_id)
            .await?
            .into_iter()
            .filter(|lease| lease.state == LeaseState::Active)
            .max_by_key(|lease| lease.updated_at_ns);
        let journal = self
            .store
            .list_seal_journals(workspace_id)
            .await?
            .into_iter()
            .max_by_key(|journal| journal.updated_at_ns);
        Ok(WorkspaceInspection {
            workspace_id,
            upper_bucket: workspace.upper_bucket,
            state: workspace.state,
            owner_id: workspace.owner_id,
            head_layer_id: workspace.head_layer_id,
            head_epoch: workspace.head_epoch,
            base_revision: workspace.fork_base,
            layer_depth: head.depth,
            lease_id: lease.as_ref().map(|lease| lease.lease_id),
            holder_generation: lease.as_ref().map(|lease| lease.holder_generation),
            lease_expires_at_ns: lease.as_ref().map(|lease| lease.expires_at_ns),
            private_metadata_rows,
            private_bytes,
            shared_bytes,
            pending_writeback_bytes: journal.as_ref().map_or(0, |journal| journal.pending_bytes),
            last_seal_journal_id: journal.as_ref().map(|journal| journal.journal_id),
            last_seal_phase: journal.as_ref().map(|journal| journal.phase),
            last_seal_error: journal.and_then(|journal| journal.last_error),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use uuid::Uuid;

    use super::*;
    use crate::meta::MetaLayer;
    use crate::workspace_overlay::catalog::{CreateVolumeRoot, WorkspaceStore};
    use crate::workspace_overlay::ids::LayerId;
    use crate::workspace_overlay::lifecycle::{
        DEFAULT_HEARTBEAT_INTERVAL, DEFAULT_LEASE_TTL, WorkspaceLifecycle, WorkspaceMountSession,
    };
    use crate::workspace_overlay::meta_layer::WorkspaceMetaLayer;
    use crate::workspace_overlay::stores::database::SqliteWorkspaceStore;

    #[tokio::test]
    async fn inspect_discarded_workspace_preserves_upper_bucket_identity() {
        let store = Arc::new(
            SqliteWorkspaceStore::connect("sqlite::memory:")
                .await
                .unwrap(),
        );
        store.initialize_workspace_schema().await.unwrap();
        let workspace = store
            .create_volume_root(CreateVolumeRoot {
                upper_bucket: Some("brewfs-discarded-upper".into()),
                volume_format: "workspace-v1".into(),
                schema_version: crate::workspace_overlay::model::WORKSPACE_SCHEMA_VERSION,
                volume_id: Uuid::new_v4(),
                workspace_id: WorkspaceId::new(),
                root_layer_id: LayerId::new(),
                writable_layer_id: LayerId::new(),
                owner_id: Some("owner".into()),
            })
            .await
            .unwrap();
        WorkspaceLifecycle::new(store.clone())
            .discard(workspace.workspace_id, false)
            .await
            .unwrap();
        assert!(
            store
                .load_layer_chain(workspace.head_layer_id)
                .await
                .is_err()
        );
        let inspection = WorkspaceControl::new(store)
            .inspect(workspace.workspace_id)
            .await
            .unwrap();
        assert_eq!(inspection.workspace_id, workspace.workspace_id);
        assert_eq!(inspection.upper_bucket, workspace.upper_bucket);
        assert_eq!(inspection.owner_id, workspace.owner_id);
        assert_eq!(inspection.head_layer_id, workspace.head_layer_id);
        assert_eq!(inspection.head_epoch, workspace.head_epoch);
        assert_eq!(inspection.state, WorkspaceState::Deleting);
        assert_eq!(inspection.lease_id, None);
    }

    #[tokio::test]
    async fn inspect_reports_private_rows_and_active_lease() {
        let store = Arc::new(
            SqliteWorkspaceStore::connect("sqlite::memory:")
                .await
                .unwrap(),
        );
        store.initialize_workspace_schema().await.unwrap();
        let workspace_id = WorkspaceId::from_uuid(Uuid::from_u128(1001));
        store
            .create_volume_root(CreateVolumeRoot {
                upper_bucket: None,
                volume_format: "workspace-v1".into(),
                schema_version: crate::workspace_overlay::model::WORKSPACE_SCHEMA_VERSION,
                volume_id: Uuid::from_u128(1002),
                workspace_id,
                root_layer_id: LayerId::from_uuid(Uuid::from_u128(1003)),
                writable_layer_id: LayerId::from_uuid(Uuid::from_u128(1004)),
                owner_id: Some("agent".into()),
            })
            .await
            .unwrap();
        let session = WorkspaceMountSession::acquire(
            store.clone(),
            workspace_id,
            7,
            DEFAULT_LEASE_TTL,
            DEFAULT_HEARTBEAT_INTERVAL,
        )
        .await
        .unwrap();
        let meta = WorkspaceMetaLayer::new(store.clone(), session.view.clone());
        meta.create_file(meta.root_ino(), "private".into())
            .await
            .unwrap();
        let inspection = WorkspaceControl::new(store)
            .inspect(workspace_id)
            .await
            .unwrap();
        assert_eq!(inspection.owner_id.as_deref(), Some("agent"));
        assert_eq!(inspection.holder_generation, Some(7));
        assert!(inspection.private_metadata_rows >= 2);
        session.release().await.unwrap();
    }
}
