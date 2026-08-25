//! Workspace lifecycle orchestration over atomic `WorkspaceStore` primitives.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::catalog::{
    AbortSeal, AcquireLease, AdvanceSeal, BeginSeal, CreateSnapshot, CreateWorkspace,
    FastForwardCommit, HeadGuard, MarkDeleting, ReleaseLease, RenewLease, WorkspaceStore,
};
use super::compaction::WorkspaceCompactor;
use super::error::WorkspaceError;
use super::ids::{JournalId, LayerId, LeaseId, SnapshotId, WorkspaceId};
use super::metrics::{WorkspaceMetrics, global_workspace_metrics};
use super::model::{
    BaseRevision, CommitResult, LayerRecord, LayerState, SealPhase, SealResult, SnapshotLease,
    SnapshotRecord, ViewContext, WorkspaceRecord,
};

pub const DEFAULT_LEASE_TTL: Duration = Duration::from_secs(30);
pub const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);

#[async_trait]
pub trait DurableRemoteBarrier: Send + Sync {
    async fn drain(&self, workspace_id: WorkspaceId, head_epoch: u64)
    -> Result<(), WorkspaceError>;
}

pub struct NoopDurableRemoteBarrier;

#[async_trait]
impl DurableRemoteBarrier for NoopDurableRemoteBarrier {
    async fn drain(
        &self,
        _workspace_id: WorkspaceId,
        _head_epoch: u64,
    ) -> Result<(), WorkspaceError> {
        Ok(())
    }
}

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

    pub async fn seal(
        &self,
        view: &ViewContext,
        barrier: &dyn DurableRemoteBarrier,
    ) -> Result<SealResult, WorkspaceError> {
        let started = Instant::now();
        let result = self.seal_inner(view, barrier).await;
        self.metrics.record_seal(result.is_ok());
        self.metrics
            .add_quiesce_latency_ns(started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64);
        result
    }

    async fn seal_inner(
        &self,
        view: &ViewContext,
        barrier: &dyn DurableRemoteBarrier,
    ) -> Result<SealResult, WorkspaceError> {
        let journal_id = JournalId::new();
        self.store
            .begin_seal(BeginSeal {
                guard: HeadGuard {
                    workspace_id: view.workspace_id,
                    expected_head_layer_id: view.head_layer_id,
                    expected_head_epoch: view.head_epoch,
                    lease_id: view.lease_id,
                    holder_generation: view.holder_generation,
                },
                journal_id,
                new_head_layer_id: LayerId::new(),
            })
            .await?;
        self.store
            .advance_seal(AdvanceSeal {
                journal_id,
                expected_phase: SealPhase::Prepare,
                next_phase: SealPhase::Quiesced,
                pending_bytes: None,
                last_error: None,
            })
            .await?;
        if let Err(error) = barrier.drain(view.workspace_id, view.head_epoch).await {
            let _ = self
                .store
                .abort_recoverable_seal(AbortSeal {
                    journal_id,
                    reason: error.to_string(),
                })
                .await;
            return Err(error);
        }
        self.store
            .advance_seal(AdvanceSeal {
                journal_id,
                expected_phase: SealPhase::Quiesced,
                next_phase: SealPhase::DataDrained,
                pending_bytes: Some(0),
                last_error: None,
            })
            .await?;
        self.store.hash_seal(journal_id).await?;
        self.store.commit_seal(journal_id).await?;
        self.flatten_sealed_workspace(view.workspace_id).await
    }

    async fn flatten_sealed_workspace(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<SealResult, WorkspaceError> {
        let compacted = WorkspaceCompactor::new(self.store.clone())
            .compact(workspace_id)
            .await?;
        Ok(SealResult {
            revision: compacted.revision,
            new_head_layer_id: compacted.replacement_head_layer_id,
            head_epoch: compacted.head_epoch,
        })
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

    pub async fn snapshot_revision(
        &self,
        revision: BaseRevision,
        name: Option<String>,
        owner_id: Option<String>,
    ) -> Result<SnapshotRecord, WorkspaceError> {
        self.store
            .create_snapshot(CreateSnapshot {
                snapshot_id: SnapshotId::new(),
                name,
                revision,
                owner_id,
            })
            .await
    }

    pub async fn seal_and_snapshot(
        &self,
        view: &ViewContext,
        barrier: &dyn DurableRemoteBarrier,
        name: Option<String>,
        owner_id: Option<String>,
    ) -> Result<(SealResult, SnapshotRecord), WorkspaceError> {
        let sealed = self.seal(view, barrier).await?;
        let snapshot = self
            .snapshot_revision(sealed.revision.clone(), name, owner_id)
            .await?;
        Ok((sealed, snapshot))
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

    pub async fn fast_forward(
        &self,
        source_revision: BaseRevision,
        source_fork_base: BaseRevision,
        target: &WorkspaceRecord,
    ) -> Result<CommitResult, WorkspaceError> {
        let result = self
            .store
            .fast_forward_commit(FastForwardCommit {
                source_revision,
                source_fork_base,
                target_workspace_id: target.workspace_id,
                target_expected_head_layer_id: target.head_layer_id,
                target_expected_head_epoch: target.head_epoch,
                new_head_layer_id: LayerId::new(),
            })
            .await;
        self.metrics.record_publish(result.is_ok());
        result
    }

    pub async fn recover_incomplete_seals(&self) -> Result<Vec<SealResult>, WorkspaceError> {
        let journals = self.store.list_incomplete_seal_journals().await?;
        let mut completed = Vec::new();
        for journal in journals {
            match journal.phase {
                SealPhase::Prepare | SealPhase::Quiesced => {
                    self.store
                        .abort_recoverable_seal(AbortSeal {
                            journal_id: journal.journal_id,
                            reason: "recovery restored the pre-switch view".into(),
                        })
                        .await?;
                }
                SealPhase::DataDrained => {
                    self.store.hash_seal(journal.journal_id).await?;
                    self.store.commit_seal(journal.journal_id).await?;
                    completed.push(self.flatten_sealed_workspace(journal.workspace_id).await?);
                }
                SealPhase::Hashed | SealPhase::HeadSwitched => {
                    self.store.commit_seal(journal.journal_id).await?;
                    completed.push(self.flatten_sealed_workspace(journal.workspace_id).await?);
                }
                SealPhase::Completed | SealPhase::Aborted => {}
            }
        }
        Ok(completed)
    }
}

pub struct WorkspaceMountSession<W> {
    store: Arc<W>,
    pub lease: SnapshotLease,
    pub view: ViewContext,
    heartbeat: LeaseHeartbeat,
}

impl<W: WorkspaceStore + 'static> WorkspaceMountSession<W> {
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
        let mut workspace = store.load_workspace(workspace_id).await?;
        let mut chain = store.load_layer_chain(workspace.head_layer_id).await?;
        if !is_fixed_layer_pair(&chain) {
            WorkspaceCompactor::new(store.clone())
                .compact(workspace_id)
                .await?;
            workspace = store.load_workspace(workspace_id).await?;
            chain = store.load_layer_chain(workspace.head_layer_id).await?;
        }
        if !is_fixed_layer_pair(&chain) {
            return Err(WorkspaceError::CorruptMetadata(
                "workspace mount requires one flat sealed base and one writable overlay".into(),
            ));
        }
        let ttl_ns = duration_ns(ttl)?;
        let lease = store
            .acquire_lease(AcquireLease {
                workspace_id,
                lease_id: LeaseId::new(),
                holder_generation,
                ttl_ns,
            })
            .await?;
        let view = ViewContext {
            workspace_id,
            head_layer_id: workspace.head_layer_id,
            head_epoch: workspace.head_epoch,
            lease_id: lease.lease_id,
            holder_generation,
        };
        let heartbeat = LeaseHeartbeat::spawn(
            store.clone(),
            lease.lease_id,
            holder_generation,
            ttl,
            heartbeat_interval,
        );
        Ok(Self {
            store,
            lease,
            view,
            heartbeat,
        })
    }

    pub async fn release(self) -> Result<(), WorkspaceError> {
        self.heartbeat.stop().await;
        let result = self
            .store
            .release_lease(ReleaseLease {
                lease_id: self.lease.lease_id,
                holder_generation: self.lease.holder_generation,
            })
            .await;
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
}

impl LeaseHeartbeat {
    fn spawn<W: WorkspaceStore + 'static>(
        store: Arc<W>,
        lease_id: LeaseId,
        holder_generation: u64,
        ttl: Duration,
        interval: Duration,
    ) -> Self {
        let cancel = CancellationToken::new();
        let task_cancel = cancel.clone();
        let task = tokio::spawn(async move {
            let ttl_ns = match duration_ns(ttl) {
                Ok(value) => value,
                Err(_) => return,
            };
            let mut timer = tokio::time::interval(interval);
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            timer.tick().await;
            loop {
                tokio::select! {
                    _ = task_cancel.cancelled() => break,
                    _ = timer.tick() => {
                        if let Err(error) = store.reap_expired_leases().await {
                            tracing::warn!(?error, "workspace expired-lease reaper failed");
                        }
                        if store.renew_lease(RenewLease {
                            lease_id,
                            holder_generation,
                            ttl_ns,
                        }).await.is_err() {
                            break;
                        }
                    }
                }
            }
        });
        Self { cancel, task }
    }

    async fn stop(self) {
        self.cancel.cancel();
        let _ = self.task.await;
    }
}

fn duration_ns(duration: Duration) -> Result<u64, WorkspaceError> {
    u64::try_from(duration.as_nanos())
        .map_err(|_| WorkspaceError::CorruptMetadata("duration exceeds u64 nanoseconds".into()))
}

#[cfg(test)]
mod tests;
