//! Opaque proof from the actual original mount; never construct from a status flag.
//! Intended placement workspace/packed_shutdown.rs (public type, private mint).

use super::catalog::{HeadGuard, WorkspaceStore};
use super::error::WorkspaceError;
use super::meta_layer::WorkspaceMetaLayer;
use super::packed::snapshot::SnapshotMountBudget;
use super::stores::kv_store::packed_admin::PackedReleasedMountReference;
use crate::chunk::BlockStore;
use crate::cli::packed_mount_cutoff::VerifiedPackedKernelCutoff;
use crate::vfs::fs::PackedVfsDrainFence;
use async_trait::async_trait;
use std::sync::Arc;

#[async_trait]
trait OriginalDrainWitness: Send + Sync {
    async fn validate(&self) -> Result<(), WorkspaceError>;
}

struct OriginalMountDrain<S, W>
where
    S: BlockStore + Send + Sync + 'static,
    W: WorkspaceStore + 'static,
{
    drain: PackedVfsDrainFence<S, WorkspaceMetaLayer<W>>,
    cutoff: VerifiedPackedKernelCutoff<S, WorkspaceMetaLayer<W>>,
    guard: HeadGuard,
}

#[async_trait]
impl<S, W> OriginalDrainWitness for OriginalMountDrain<S, W>
where
    S: BlockStore + Send + Sync + 'static,
    W: WorkspaceStore + 'static,
{
    async fn validate(&self) -> Result<(), WorkspaceError> {
        self.cutoff.validate().map_err(|_| WorkspaceError::Fenced)?;
        if !self.cutoff.matches_drain(&self.drain) {
            return Err(WorkspaceError::Fenced);
        }
        self.drain
            .validate_local()
            .await
            .map_err(|_| WorkspaceError::Fenced)?;
        let (metadata, _, _) = self.drain.frozen_source_components();
        let view = metadata.view_context().await;
        let actual = HeadGuard {
            workspace_id: view.workspace_id,
            expected_head_layer_id: view.head_layer_id,
            expected_head_epoch: view.head_epoch,
            lease_id: view.lease_id,
            holder_generation: view.holder_generation,
        };
        if actual != self.guard {
            return Err(WorkspaceError::Fenced);
        }
        Ok(())
    }
}

/// Public only because WorkspaceStore's narrow clean-release method accepts it.
/// There is no public constructor, Clone, Deserialize or conversion from booleans.
pub struct VerifiedCleanPackedShutdown {
    reference: PackedReleasedMountReference,
    store_address: usize,
    budget: Arc<SnapshotMountBudget>,
    witness: Box<dyn OriginalDrainWitness>,
}

impl Drop for VerifiedCleanPackedShutdown {
    fn drop(&mut self) {
        self.budget.close();
    }
}

impl VerifiedCleanPackedShutdown {
    // This owning task itself executes the actual metadata transport/reader
    // shutdown. The caller cannot mint authority by claiming that step succeeded.
    pub(crate) async fn from_original_shutdown<S, W>(
        reference: PackedReleasedMountReference,
        drain: PackedVfsDrainFence<S, WorkspaceMetaLayer<W>>,
        cutoff: VerifiedPackedKernelCutoff<S, WorkspaceMetaLayer<W>>,
        store: &Arc<W>,
    ) -> Result<Self, WorkspaceError>
    where
        S: BlockStore + Send + Sync + 'static,
        W: WorkspaceStore + 'static,
    {
        let store = store.clone();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let result = Self::finish_original_shutdown(reference, drain, cutoff, &store).await;
            // A cancelled receiver drops any completed proof here, closing the
            // original budget only after the real shutdown reached terminal.
            let _ = sender.send(result);
        });
        let received = receiver.await;
        task.await.map_err(|_| {
            WorkspaceError::CorruptMetadata("owned original packed shutdown stopped".into())
        })?;
        received.map_err(|_| {
            WorkspaceError::CorruptMetadata(
                "owned original packed shutdown result unavailable".into(),
            )
        })?
    }

    async fn finish_original_shutdown<S, W>(
        reference: PackedReleasedMountReference,
        drain: PackedVfsDrainFence<S, WorkspaceMetaLayer<W>>,
        cutoff: VerifiedPackedKernelCutoff<S, WorkspaceMetaLayer<W>>,
        store: &Arc<W>,
    ) -> Result<Self, WorkspaceError>
    where
        S: BlockStore + Send + Sync + 'static,
        W: WorkspaceStore + 'static,
    {
        if reference.mount_uid.is_nil() || reference.pod_uid.is_nil() {
            return Err(WorkspaceError::Fenced);
        }
        let (metadata, _, _) = drain.frozen_source_components();
        if !Arc::ptr_eq(metadata.store(), store) {
            return Err(WorkspaceError::Fenced);
        }
        let budget = metadata
            .packed_shutdown_budget()
            .ok_or(WorkspaceError::Fenced)?;
        if !cutoff.uses_budget(&budget) {
            return Err(WorkspaceError::Fenced);
        }
        let witness = OriginalMountDrain {
            drain,
            cutoff,
            guard: reference.guard.clone(),
        };
        witness.validate().await?;
        metadata
            .shutdown_packed_runtime_for_clean_release()
            .await
            .map_err(|_| WorkspaceError::Fenced)?;
        // No await between successful actual shutdown and installing the owner.
        let proof = Self {
            reference,
            store_address: Arc::as_ptr(store) as usize,
            budget,
            witness: Box::new(witness),
        };
        proof.validate().await?;
        Ok(proof)
    }

    pub(crate) fn reference(&self) -> &PackedReleasedMountReference {
        &self.reference
    }

    pub(crate) fn budget(&self) -> Arc<SnapshotMountBudget> {
        self.budget.clone()
    }

    pub(crate) fn belongs_to_store<W>(&self, store: &W) -> bool {
        self.store_address == store as *const W as usize
    }

    pub(crate) async fn validate(&self) -> Result<(), WorkspaceError> {
        self.witness.validate().await
    }
}
