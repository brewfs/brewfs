//! Crate-private concrete packed mount delegation and joint heartbeat.

use super::super::packed::snapshot::SnapshotMountBudget;
use super::super::packed_shutdown::VerifiedCleanPackedShutdown;
use super::super::stores::kv_store::packed_admin::PackedReleasedMountReference;
use super::*;
use tokio::sync::Mutex;

pub(crate) struct PackedMountSessionRequest {
    pub(crate) workspace_id: WorkspaceId,
    pub(crate) holder_generation: u64,
    pub(crate) ttl_ns: u64,
    pub(crate) operator_managed: bool,
    pub(crate) budget: Arc<SnapshotMountBudget>,
}

impl PackedMountSessionRequest {
    pub(crate) fn identities(&self) -> Result<(uuid::Uuid, uuid::Uuid), WorkspaceError> {
        if !self.operator_managed {
            return Ok((uuid::Uuid::new_v4(), uuid::Uuid::new_v4()));
        }
        let identity = |key: &str| {
            std::env::var(key)
                .ok()
                .and_then(|value| uuid::Uuid::parse_str(&value).ok())
                .filter(|value| !value.is_nil())
                .ok_or(WorkspaceError::Fenced)
        };
        Ok((
            identity("BREWFS_PACKED_MOUNT_UID")?,
            identity("BREWFS_PACKED_POD_UID")?,
        ))
    }
}

#[async_trait]
pub(crate) trait PackedMountAuthority: Send + Sync {
    fn view(&self) -> ViewContext;
    fn lease(&self) -> SnapshotLease;
    fn reference(&self) -> PackedReleasedMountReference;
    fn budget(&self) -> &Arc<SnapshotMountBudget>;
    async fn initialize_writeback_identity(
        &self,
        config: &crate::vfs::config::VFSConfig,
    ) -> Result<(), WorkspaceError>;
    async fn renew(self: Arc<Self>, ttl_ns: u64) -> Result<(), WorkspaceError>;
    async fn close_renewals_and_drain(&self) -> Result<(), WorkspaceError>;
    async fn release_original(
        self: Arc<Self>,
        proof: VerifiedCleanPackedShutdown,
    ) -> Result<(), WorkspaceError>;
    async fn abort_unattached(
        self: Arc<Self>,
        marker: PackedNeverAttached,
    ) -> Result<(), WorkspaceError>;
}

#[async_trait]
pub(crate) trait PackedMountCatalog: WorkspaceStore + 'static {
    async fn packed_mount_header(
        &self,
        workspace: WorkspaceId,
    ) -> Result<Option<super::super::model::VolumeHeader>, WorkspaceError>;
    async fn acquire_packed_mount_if_present(
        self: Arc<Self>,
        request: PackedMountSessionRequest,
    ) -> Result<Option<Arc<dyn PackedMountAuthority>>, WorkspaceError>;
}

// The SQL catalog retains its existing native session. Packed authority is
// implemented only by the concrete Redis/TiKV KV catalog below this bridge.
#[async_trait]
impl PackedMountCatalog for super::super::stores::database::SqliteWorkspaceStore {
    async fn packed_mount_header(
        &self,
        _workspace: WorkspaceId,
    ) -> Result<Option<super::super::model::VolumeHeader>, WorkspaceError> {
        Ok(None)
    }
    async fn acquire_packed_mount_if_present(
        self: Arc<Self>,
        _request: PackedMountSessionRequest,
    ) -> Result<Option<Arc<dyn PackedMountAuthority>>, WorkspaceError> {
        Ok(None)
    }
}

/// No public constructor, clone or wire conversion. Its private owner only
/// constructs it before the actual FUSE attachment call is ever attempted.
pub(crate) struct PackedNeverAttached {
    reference: PackedReleasedMountReference,
    budget: Arc<SnapshotMountBudget>,
}
impl PackedNeverAttached {
    pub(crate) fn matches(
        &self,
        reference: &PackedReleasedMountReference,
        budget: &Arc<SnapshotMountBudget>,
    ) -> bool {
        self.reference == *reference && Arc::ptr_eq(&self.budget, budget)
    }
}

pub(super) struct PackedMountRuntime {
    authority: Arc<dyn PackedMountAuthority>,
    cancel: CancellationToken,
    heartbeat: Mutex<Option<JoinHandle<()>>>,
    attachment_attempted: bool,
}

impl PackedMountRuntime {
    fn start(
        authority: Arc<dyn PackedMountAuthority>,
        ttl_ns: u64,
        interval: Duration,
        validity: Arc<super::LeaseValidity>,
    ) -> Self {
        let cancel = CancellationToken::new();
        let task_cancel = cancel.clone();
        let renewal = authority.clone();
        let ttl = Duration::from_nanos(ttl_ns);
        let task = tokio::spawn(async move {
            let mut timer = tokio::time::interval(interval);
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            timer.tick().await;
            loop {
                tokio::select! {
                    _ = task_cancel.cancelled() => break,
                    _ = timer.tick() => {}
                }
                // This awaits one real joint CAS. No native lease-only renew or
                // generic open sidecar can substitute for the opaque authority.
                let started = std::time::Instant::now();
                if let Err(error) = renewal.clone().renew(ttl_ns).await {
                    validity.revoke();
                    tracing::warn!(?error, "packed joint heartbeat fenced");
                    break;
                }
                validity.extend(started, ttl);
            }
        });
        Self {
            authority,
            cancel,
            heartbeat: Mutex::new(Some(task)),
            attachment_attempted: false,
        }
    }

    async fn stop_renewals(&self) -> Result<(), WorkspaceError> {
        // The authority gate counts real owned workers, including callers which
        // were cancelled while awaiting their mutation response.
        self.authority.close_renewals_and_drain().await?;
        self.cancel.cancel();
        let mut slot = self.heartbeat.lock().await;
        if let Some(task) = slot.as_mut() {
            task.await.map_err(|error| {
                WorkspaceError::Backend(format!("joint heartbeat join: {error}"))
            })?;
        }
        slot.take();
        Ok(())
    }

    pub(super) async fn release_original(
        self,
        proof: VerifiedCleanPackedShutdown,
    ) -> Result<(), WorkspaceError> {
        self.stop_renewals().await?;
        self.authority.clone().release_original(proof).await
    }

    pub(super) async fn abort_unattached(self) -> Result<(), WorkspaceError> {
        if self.attachment_attempted {
            return Err(WorkspaceError::Fenced);
        }
        self.stop_renewals().await?;
        let marker = PackedNeverAttached {
            reference: self.authority.reference(),
            budget: self.authority.budget().clone(),
        };
        self.authority.clone().abort_unattached(marker).await
    }
}

impl Drop for PackedMountRuntime {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

impl<W: WorkspaceStore + 'static> WorkspaceMountSession<W> {
    pub(crate) fn packed_mount_reference(&self) -> Option<PackedReleasedMountReference> {
        self.packed
            .as_ref()
            .map(|packed| packed.authority.reference())
    }
    pub(crate) fn mark_packed_attachment_attempted(&mut self) {
        if let Some(packed) = self.packed.as_mut() {
            packed.attachment_attempted = true;
        }
    }
    pub(crate) async fn close_packed_renewals_for_shutdown(&self) -> Result<(), WorkspaceError> {
        if let Some(packed) = &self.packed {
            packed.stop_renewals().await?;
        }
        Ok(())
    }
    pub(crate) async fn initialize_packed_writeback_identity(
        &self,
        config: &crate::vfs::config::VFSConfig,
    ) -> Result<(), WorkspaceError> {
        if let Some(packed) = &self.packed {
            packed
                .authority
                .initialize_writeback_identity(config)
                .await?;
        }
        Ok(())
    }
}

impl<W: WorkspaceStore + 'static> WorkspaceMountSession<W> {
    pub(crate) async fn acquire_for_mount(
        store: Arc<W>,
        workspace_id: WorkspaceId,
        holder_generation: u64,
        ttl: Duration,
        heartbeat_interval: Duration,
        operator_managed: bool,
        budget: Arc<SnapshotMountBudget>,
    ) -> Result<Self, WorkspaceError>
    where
        W: PackedMountCatalog,
    {
        store.capabilities().validate_for_v1_mount()?;
        if heartbeat_interval.is_zero() {
            return Err(WorkspaceError::Fenced);
        }
        let request = PackedMountSessionRequest {
            workspace_id,
            holder_generation,
            ttl_ns: duration_ns(ttl)?,
            operator_managed,
            budget: budget.clone(),
        };
        let acquire_started = std::time::Instant::now();
        let Some(authority) = store
            .clone()
            .acquire_packed_mount_if_present(request)
            .await?
        else {
            return Self::acquire(
                store,
                workspace_id,
                holder_generation,
                ttl,
                heartbeat_interval,
            )
            .await;
        };
        if !Arc::ptr_eq(authority.budget(), &budget) {
            return Err(WorkspaceError::Fenced);
        }
        let view = authority.view();
        let lease = authority.lease();
        let validity = Arc::new(super::LeaseValidity::new(acquire_started, ttl));
        let packed = PackedMountRuntime::start(
            authority,
            duration_ns(ttl)?,
            heartbeat_interval,
            validity.clone(),
        );
        let metrics = global_workspace_metrics();
        metrics.record_mount(true);
        metrics.add_active_lease();
        Ok(Self {
            store,
            lease,
            view,
            heartbeat: None,
            packed: Some(packed),
            validity,
        })
    }
}
