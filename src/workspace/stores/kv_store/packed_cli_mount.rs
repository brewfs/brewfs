//! Private delegation used by the actual CLI WorkspaceMountSession.

use super::packed_admin::{PackedMountGrantRequest, PackedMountedLease};
use super::*;
use crate::workspace::lifecycle::{
    PackedMountAuthority, PackedMountCatalog, PackedMountSessionRequest, PackedNeverAttached,
};
use crate::workspace::packed::snapshot::SnapshotMountBudget;
use crate::workspace::packed_shutdown::VerifiedCleanPackedShutdown;

#[async_trait]
impl<B: WorkspaceKvBackend> PackedMountCatalog for KvWorkspaceStore<B> {
    async fn packed_mount_header(
        &self,
        workspace: WorkspaceId,
    ) -> Result<Option<VolumeHeader>, WorkspaceError> {
        if !self.has_packed_writer_mount_binding(workspace).await? {
            return Ok(None);
        }
        let budget = self
            .packed_reader_pin_budget
            .get()
            .ok_or(WorkspaceError::Fenced)?;
        let _owner = budget
            .admit(&[(
                crate::workspace::packed::snapshot::SnapshotBudgetPool::Metadata,
                1 << 20,
            )])
            .map_err(|error| WorkspaceError::InvalidReadPlan(error.to_string()))?;
        let keys = [
            VOLUME_HEADER_KEY.to_vec(),
            packed_current_key(workspace),
            super::packed_writer_authority::packed_writer_key(workspace),
        ];
        let (values, now) = self
            .backend
            .get_many_consistent_with_time_bounded(
                &keys,
                crate::workspace::stores::kv_backend::KvReadLimits {
                    max_records: 3,
                    max_key_bytes: 256,
                    max_value_bytes: 48 << 10,
                    max_total_bytes: 64 << 10,
                    max_response_bytes: 64 << 10,
                    max_data_requests: 3,
                },
            )
            .await?;
        if values.len() != 3 || now <= 0 {
            return Err(WorkspaceError::Fenced);
        }
        let binding =
            PackedLowerBindingRecord::decode(values[1].as_deref().ok_or(WorkspaceError::Fenced)?)?;
        if binding.workspace_id != workspace {
            return Err(WorkspaceError::Fenced);
        }
        super::packed_writer_authority::PackedWriterAuthority::decode(
            values[2].as_deref().ok_or(WorkspaceError::Fenced)?,
            workspace,
        )?;
        Ok(Some(decode_open_value(
            values[0].as_deref().ok_or(WorkspaceError::Fenced)?,
            48 << 10,
        )?))
    }

    async fn acquire_packed_mount_if_present(
        self: Arc<Self>,
        request: PackedMountSessionRequest,
    ) -> Result<Option<Arc<dyn PackedMountAuthority>>, WorkspaceError> {
        if !self
            .packed_reader_pin_budget
            .get()
            .is_some_and(|budget| Arc::ptr_eq(budget, &request.budget))
        {
            return Err(WorkspaceError::Fenced);
        }
        if !self
            .has_packed_writer_mount_binding(request.workspace_id)
            .await?
        {
            return Ok(None);
        }
        let (mount_uid, pod_uid) = request.identities()?;
        let authority = self
            .grant_packed_mounted_session(PackedMountGrantRequest {
                workspace_id: request.workspace_id,
                lease_id: LeaseId::new(),
                holder_generation: request.holder_generation,
                mount_uid,
                pod_uid,
                ttl_ns: request.ttl_ns,
            })
            .await?;
        Ok(Some(authority))
    }
}

#[async_trait]
impl<B: WorkspaceKvBackend> PackedMountAuthority for PackedMountedLease<B> {
    fn view(&self) -> ViewContext {
        PackedMountedLease::view(self)
    }
    fn lease(&self) -> SnapshotLease {
        PackedMountedLease::lease(self)
    }
    fn reference(&self) -> packed_admin::PackedReleasedMountReference {
        PackedMountedLease::reference(self)
    }
    fn budget(&self) -> &Arc<SnapshotMountBudget> {
        PackedMountedLease::budget(self)
    }
    async fn initialize_writeback_identity(
        &self,
        config: &crate::vfs::config::VFSConfig,
    ) -> Result<(), WorkspaceError> {
        PackedMountedLease::initialize_writeback_identity(self, config).await
    }
    async fn renew(self: Arc<Self>, ttl_ns: u64) -> Result<(), WorkspaceError> {
        PackedMountedLease::renew(&self, ttl_ns).await
    }
    async fn close_renewals_and_drain(&self) -> Result<(), WorkspaceError> {
        PackedMountedLease::close_renewals_and_drain(self).await
    }
    async fn release_original(
        self: Arc<Self>,
        proof: VerifiedCleanPackedShutdown,
    ) -> Result<(), WorkspaceError> {
        PackedMountedLease::release_original(&self, proof).await
    }
    async fn abort_unattached(
        self: Arc<Self>,
        marker: PackedNeverAttached,
    ) -> Result<(), WorkspaceError> {
        PackedMountedLease::abort_unattached(&self, marker).await
    }
}
