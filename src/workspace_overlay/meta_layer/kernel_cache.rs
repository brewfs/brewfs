//! Bounded metadata freshness without a cross-mount invalidation channel.
//!
//! Replies expire at one shared deadline measured BEFORE the remote check.
//! Hits never extend it. Once it expires, revalidate the workspace, writer
//! lease, head/base generations and independent packed binding, then discard
//! older upper rows before allowing another cache fill. Local commits already
//! invalidate their upper rows. Kernel mutation replies handle local dentries.

use super::*;
use std::time::{Duration, Instant};

#[derive(Default)]
pub(super) struct KernelCache {
    layers: Option<[LayerRecord; 2]>,
    deadline: Option<Instant>,
    guard: Option<HeadGuard>,
    ttl: Duration,
}

impl<W: WorkspaceStore + 'static> WorkspaceMetaLayer<W> {
    pub(super) async fn kernel_cache_deadline(
        &self,
        ttl: Duration,
    ) -> Result<Option<Instant>, MetaError> {
        if ttl.is_zero() || !self.store.supports_versioned_permissions() {
            return Ok(None);
        }
        // Serialize refreshes, not metadata operations. No background worker
        // or notifications can deadlock a FUSE mutation awaiting its reply.
        let mut cache = self.kernel_cache.lock().await;
        let guard = self.guard().await;
        if let Some(deadline) = cache.deadline
            && deadline > Instant::now()
            && cache.guard.as_ref() == Some(&guard)
            && cache.ttl == ttl
        {
            return Ok(Some(deadline));
        }
        let started = Instant::now();
        // An unrepresentable operator TTL must fail closed, never become an
        // unbounded cache lifetime.
        let Some(deadline) = started.checked_add(ttl) else {
            return Ok(None);
        };
        for _ in 0..8 {
            // layer_pair deliberately retains a mount-lifetime head sequence.
            // Fetch the actual generation, including same-epoch upper writes.
            let chain = self
                .store
                .load_layer_chain(guard.expected_head_layer_id)
                .await
                .map_err(workspace_to_meta)?;
            validate_fixed_layer_pair(&chain).map_err(workspace_to_meta)?;
            let layers: [LayerRecord; 2] = chain
                .try_into()
                .map_err(|_| workspace_to_meta(WorkspaceError::Fenced))?;
            if let Some(lower) = self.packed_lower() {
                if layers[1].layer_id != lower.binding.base_layer_id {
                    return Err(workspace_to_meta(WorkspaceError::Fenced));
                }
                lower
                    .authority
                    .validate(&guard, &lower.binding)
                    .await
                    .map_err(workspace_to_meta)?;
            }
            match self
                .store
                .validate_read_fence(guard.clone(), layers.clone())
                .await
            {
                Err(WorkspaceError::Busy) => {
                    tokio::task::yield_now().await;
                    continue;
                }
                Err(error) => return Err(workspace_to_meta(error)),
                Ok(()) => {}
            }
            if cache.layers.as_ref() != Some(&layers) {
                self.invalidate_upper_rows();
                cache.layers = Some(layers);
            }
            cache.deadline = Some(deadline);
            cache.guard = Some(guard);
            cache.ttl = ttl;
            return Ok(Some(deadline));
        }
        // Contention cannot grant an unchecked cache lifetime.
        Err(workspace_to_meta(WorkspaceError::Busy))
    }
}
