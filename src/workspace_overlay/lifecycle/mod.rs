//! Workspace lifecycle orchestration over atomic `WorkspaceStore` primitives.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::catalog::{
    AcquireLease, CreateWorkspace, HeadGuard, MarkDeleting, ReleaseLease, RenewLease,
    WorkspaceStore,
};
use super::error::WorkspaceError;
use super::ids::{LayerId, LeaseId, WorkspaceId};
use super::metrics::{WorkspaceMetrics, global_workspace_metrics};
use super::model::{
    BaseRevision, LayerRecord, LayerState, SnapshotLease, ViewContext, WorkspaceRecord,
};

mod lease_validity;
mod packed_mount_session;
pub use lease_validity::LeaseValidity;
pub(crate) use packed_mount_session::{
    PackedMountAuthority, PackedMountCatalog, PackedMountSessionRequest, PackedNeverAttached,
};

pub const DEFAULT_LEASE_TTL: Duration = Duration::from_secs(30);
pub const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);

pub struct WorkspaceLifecycle<W> {
    store: Arc<W>,
    metrics: Arc<WorkspaceMetrics>,
}

impl<W: WorkspaceStore + 'static> WorkspaceLifecycle<W> {
    pub fn new(store: Arc<W>) -> Self {
        Self {
            store,
            metrics: global_workspace_metrics(),
        }
    }

    pub fn store(&self) -> &Arc<W> {
        &self.store
    }

    pub async fn fork_revision(
        &self,
        revision: BaseRevision,
        count: usize,
        owner_id: Option<String>,
    ) -> Result<Vec<WorkspaceRecord>, WorkspaceError> {
        let started = Instant::now();
        let result = self.fork_revision_inner(revision, count, owner_id).await;
        self.metrics.record_fork(result.is_ok());
        self.metrics.add_fork_control_latency_ns(
            started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64,
        );
        result
    }

    async fn fork_revision_inner(
        &self,
        revision: BaseRevision,
        count: usize,
        owner_id: Option<String>,
    ) -> Result<Vec<WorkspaceRecord>, WorkspaceError> {
        let mut workspaces = Vec::with_capacity(count);
        for _ in 0..count {
            workspaces.push(
                self.store
                    .create_workspace(CreateWorkspace {
                        workspace_id: WorkspaceId::new(),
                        head_layer_id: LayerId::new(),
                        base_revision: revision.clone(),
                        owner_id: owner_id.clone(),
                    })
                    .await?,
            );
        }
        Ok(workspaces)
    }

    pub async fn discard(
        &self,
        workspace_id: WorkspaceId,
        force_fence_lease: bool,
    ) -> Result<(), WorkspaceError> {
        self.store
            .mark_workspace_deleting(MarkDeleting {
                workspace_id,
                force_fence_lease,
            })
            .await
    }
}

pub struct WorkspaceMountSession<W> {
    store: Arc<W>,
    pub lease: SnapshotLease,
    pub view: ViewContext,
    heartbeat: Option<LeaseHeartbeat>,
    packed: Option<packed_mount_session::PackedMountRuntime>,
    /// Shared with the heartbeat; see `LeaseValidity`.
    validity: Arc<LeaseValidity>,
}

impl<W: WorkspaceStore + 'static> WorkspaceMountSession<W> {
    /// Local proof that this session still holds its exclusive lease.
    pub fn lease_validity(&self) -> Arc<LeaseValidity> {
        self.validity.clone()
    }

    pub async fn acquire(
        store: Arc<W>,
        workspace_id: WorkspaceId,
        holder_generation: u64,
        ttl: Duration,
        heartbeat_interval: Duration,
    ) -> Result<Self, WorkspaceError> {
        let metrics = global_workspace_metrics();
        let result = Self::acquire_inner(
            store,
            workspace_id,
            holder_generation,
            ttl,
            heartbeat_interval,
        )
        .await;
        metrics.record_mount(result.is_ok());
        if result.is_ok() {
            metrics.add_active_lease();
        }
        result
    }

    async fn acquire_inner(
        store: Arc<W>,
        workspace_id: WorkspaceId,
        holder_generation: u64,
        ttl: Duration,
        heartbeat_interval: Duration,
    ) -> Result<Self, WorkspaceError> {
        store.capabilities().validate_for_v1_mount()?;
        let workspace = store.load_workspace(workspace_id).await?;
        let chain = store.load_layer_chain(workspace.head_layer_id).await?;
        if !is_fixed_layer_pair(&chain) {
            return Err(WorkspaceError::CorruptMetadata(
                "workspace mount requires one immutable lower and one writable overlay".into(),
            ));
        }
        let ttl_ns = duration_ns(ttl)?;
        let acquire_started = Instant::now();
        let lease = store
            .acquire_lease(AcquireLease {
                workspace_id,
                lease_id: LeaseId::new(),
                holder_generation,
                ttl_ns,
            })
            .await?;
        // The lease acquisition can race a control-plane head switch after
        // the preflight chain read. Never return the old, unprotected view.
        let checked_workspace = store.load_workspace(workspace_id).await;
        let binding = match checked_workspace {
            Ok(current)
                if current.state == super::model::WorkspaceState::Active
                    && current.head_layer_id == workspace.head_layer_id
                    && current.head_epoch == workspace.head_epoch
                    && current.active_lease == Some(lease.lease_id)
                    && lease.base_revision.layer_id == chain[1].layer_id
                    && Some(lease.base_revision.sealed_version) == chain[1].sealed_version
                    && Some(lease.base_revision.root_hash) == chain[1].root_hash =>
            {
                Ok(())
            }
            Ok(_) => Err(WorkspaceError::Fenced),
            Err(error) => Err(error),
        };
        if let Err(error) = binding {
            let _ = store
                .release_lease(ReleaseLease {
                    lease_id: lease.lease_id,
                    holder_generation: lease.holder_generation,
                })
                .await;
            return Err(error);
        }
        let view = ViewContext {
            workspace_id,
            head_layer_id: workspace.head_layer_id,
            head_epoch: workspace.head_epoch,
            lease_id: lease.lease_id,
            holder_generation,
        };
        let validity = Arc::new(LeaseValidity::new(acquire_started, ttl));
        let heartbeat = LeaseHeartbeat::spawn(
            store.clone(),
            lease.lease_id,
            holder_generation,
            ttl,
            heartbeat_interval,
            validity.clone(),
        );
        Ok(Self {
            store,
            lease,
            view,
            heartbeat: Some(heartbeat),
            packed: None,
            validity,
        })
    }

    pub async fn release_clean(
        mut self,
        proof: super::packed_shutdown::VerifiedCleanPackedShutdown,
    ) -> Result<(), WorkspaceError> {
        let expected = HeadGuard {
            workspace_id: self.view.workspace_id,
            expected_head_layer_id: self.view.head_layer_id,
            expected_head_epoch: self.view.head_epoch,
            lease_id: self.view.lease_id,
            holder_generation: self.view.holder_generation,
        };
        if proof.reference().guard != expected || !proof.belongs_to_store(self.store.as_ref()) {
            return Err(WorkspaceError::Fenced);
        }
        if let Some(heartbeat) = self.heartbeat.take() {
            heartbeat.stop().await;
        }
        let result = if let Some(packed) = self.packed.take() {
            packed.release_original(proof).await
        } else {
            self.store.release_clean_packed_shutdown(proof).await
        };
        if result.is_ok() {
            global_workspace_metrics().remove_active_lease();
        }
        result
    }

    pub async fn release(mut self) -> Result<(), WorkspaceError> {
        if let Some(heartbeat) = self.heartbeat.take() {
            heartbeat.stop().await;
        }
        let result = if let Some(packed) = self.packed.take() {
            packed.abort_unattached().await
        } else {
            self.store
                .release_lease(ReleaseLease {
                    lease_id: self.lease.lease_id,
                    holder_generation: self.lease.holder_generation,
                })
                .await
        };
        if result.is_ok() {
            global_workspace_metrics().remove_active_lease();
        }
        result
    }
}

fn is_fixed_layer_pair(chain: &[LayerRecord]) -> bool {
    let [head, base] = chain else {
        return false;
    };
    head.state == LayerState::Writable
        && head.parent_layer_id == Some(base.layer_id)
        && head.depth == 2
        && base.state == LayerState::Sealed
        && base.parent_layer_id.is_none()
        && base.depth == 1
}

struct LeaseHeartbeat {
    cancel: CancellationToken,
    task: JoinHandle<()>,
    validity: Arc<LeaseValidity>,
}

impl Drop for LeaseHeartbeat {
    fn drop(&mut self) {
        self.validity.revoke();
        self.cancel.cancel();
        self.task.abort();
    }
}

impl LeaseHeartbeat {
    fn spawn<W: WorkspaceStore + 'static>(
        store: Arc<W>,
        lease_id: LeaseId,
        holder_generation: u64,
        ttl: Duration,
        interval: Duration,
        validity: Arc<LeaseValidity>,
    ) -> Self {
        let cancel = CancellationToken::new();
        let task_cancel = cancel.clone();
        let owner_validity = validity.clone();
        let task = tokio::spawn(async move {
            let ttl_ns = match duration_ns(ttl) {
                Ok(value) => value,
                Err(_) => {
                    validity.revoke();
                    return;
                }
            };
            let mut timer = tokio::time::interval(interval);
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            timer.tick().await;
            loop {
                tokio::select! {
                    _ = task_cancel.cancelled() => break,
                    _ = timer.tick() => {
                        let started = Instant::now();
                        if let Err(error) = store.renew_lease(RenewLease {
                            lease_id,
                            holder_generation,
                            ttl_ns,
                        }).await {
                            validity.revoke();
                            tracing::warn!(?error, %lease_id, holder_generation,
                                "workspace lease heartbeat failed");
                            break;
                        }
                        validity.extend(started, ttl);
                    }
                }
            }
        });
        Self {
            cancel,
            task,
            validity: owner_validity,
        }
    }

    async fn stop(mut self) {
        self.validity.revoke();
        self.cancel.cancel();
        let _ = (&mut self.task).await;
    }
}

fn duration_ns(duration: Duration) -> Result<u64, WorkspaceError> {
    u64::try_from(duration.as_nanos())
        .map_err(|_| WorkspaceError::CorruptMetadata("duration exceeds u64 nanoseconds".into()))
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod acquire_view_tests;

#[cfg(test)]
mod lease_recovery_tests {
    use super::super::catalog::{CreateVolumeRoot, CreateWorkspace};
    use super::super::model::LeaseState;
    use super::super::stores::database::SqliteWorkspaceStore;
    use super::*;
    use uuid::Uuid;

    async fn root() -> (Arc<SqliteWorkspaceStore>, WorkspaceRecord) {
        let store = Arc::new(
            SqliteWorkspaceStore::connect("sqlite::memory:")
                .await
                .unwrap(),
        );
        store.initialize_workspace_schema().await.unwrap();
        let workspace = store
            .create_volume_root(CreateVolumeRoot {
                volume_format: "workspace-v1".into(),
                schema_version: super::super::model::WORKSPACE_SCHEMA_VERSION,
                volume_id: Uuid::new_v4(),
                workspace_id: WorkspaceId::new(),
                root_layer_id: LayerId::new(),
                writable_layer_id: LayerId::new(),
                owner_id: None,
            })
            .await
            .unwrap();
        (store, workspace)
    }

    #[tokio::test]
    async fn heartbeat_renews_only_its_own_lease() {
        let (store, workspace) = root().await;
        let root_lease = store
            .acquire_lease(AcquireLease {
                workspace_id: workspace.workspace_id,
                lease_id: LeaseId::new(),
                holder_generation: 1,
                ttl_ns: 1_000_000_000,
            })
            .await
            .unwrap();
        let child = store
            .create_workspace(CreateWorkspace {
                workspace_id: WorkspaceId::new(),
                head_layer_id: LayerId::new(),
                base_revision: workspace.fork_base.unwrap(),
                owner_id: None,
            })
            .await
            .unwrap();
        let other = store
            .acquire_lease(AcquireLease {
                workspace_id: child.workspace_id,
                lease_id: LeaseId::new(),
                holder_generation: 2,
                ttl_ns: 10_000_000,
            })
            .await
            .unwrap();
        let heartbeat = LeaseHeartbeat::spawn(
            store.clone(),
            root_lease.lease_id,
            1,
            Duration::from_secs(1),
            Duration::from_millis(10),
            Arc::new(LeaseValidity::new(Instant::now(), Duration::from_secs(1))),
        );
        tokio::time::sleep(Duration::from_millis(60)).await;
        heartbeat.stop().await;
        let leases = store.list_leases(child.workspace_id).await.unwrap();
        assert_eq!(leases.len(), 1);
        assert_eq!(leases[0].lease_id, other.lease_id);
        assert_eq!(leases[0].state, LeaseState::Active);
        assert_eq!(store.reap_expired_leases().await.unwrap(), 1);
    }
}
