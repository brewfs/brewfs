//! Local packed mutation ownership. This is not a native/source authority.
//!
//! Admission is closed permanently before waiting. Every admitted driver owns
//! its arguments and survives loss of the caller's response future. Nested VFS
//! calls execute in the same driver; they cannot wait behind their own freeze.

use super::*;
use crate::meta::layer::{MetadataMemoryGuard, MetadataMemoryKind};
use std::future::Future;
use tokio::sync::oneshot;

const MAX_MUTATION_DRIVERS: usize = 256;
const MUTATION_DRIVER_BYTES: u64 = 4096;
pub(super) const MAX_PUBLICATION_WRITERS: usize = 256;

tokio::task_local! {
    static MUTATION_OWNER: Arc<PackedMutationGate>;
}

#[derive(Default)]
struct GateState {
    closed: bool,
    active: usize,
    failed: bool,
    drained: bool,
    recovering: bool,
}

pub(super) struct PackedMutationGate {
    enabled: bool,
    state: StdMutex<GateState>,
    notify: Notify,
    _roots_owner: Option<MetadataMemoryGuard>,
}

impl PackedMutationGate {
    pub(super) fn requested_layout_bytes() -> u64 {
        (std::mem::size_of::<Self>() + 2 * std::mem::size_of::<usize>()) as u64
    }

    pub(super) fn new(enabled: bool, roots_owner: Option<MetadataMemoryGuard>) -> Arc<Self> {
        Arc::new(Self {
            enabled,
            state: StdMutex::new(GateState::default()),
            notify: Notify::new(),
            _roots_owner: roots_owner,
        })
    }

    pub(super) fn enabled(&self) -> bool {
        self.enabled
    }

    pub(super) fn requires_driver(self: &Arc<Self>) -> bool {
        self.enabled
            && !MUTATION_OWNER
                .try_with(|owner| Arc::ptr_eq(owner, self))
                .unwrap_or(false)
    }

    pub(super) fn admit(
        self: &Arc<Self>,
        owner: Option<MetadataMemoryGuard>,
    ) -> Result<MutationDriver, VfsError> {
        let mut state = self.state.lock().map_err(|_| VfsError::Other)?;
        if !self.enabled || state.closed || state.failed {
            return Err(VfsError::from(std::io::Error::from_raw_os_error(
                libc::ESTALE,
            )));
        }
        if state.active >= MAX_MUTATION_DRIVERS {
            return Err(VfsError::ResourceBusy);
        }
        if state.recovering {
            return Err(VfsError::ResourceBusy);
        }
        state.active += 1;
        Ok(MutationDriver {
            gate: self.clone(),
            _argument_owner: owner,
            terminal: false,
        })
    }

    pub(super) fn begin_recovery(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.recovering = true;
        }
    }
    pub(super) fn finish_recovery(&self, success: bool) {
        if let Ok(mut state) = self.state.lock() {
            state.recovering = false;
            state.failed |= !success;
        }
        self.notify.notify_waiters();
    }

    fn start_close(&self) -> Result<bool, VfsError> {
        let mut state = self.state.lock().map_err(|_| VfsError::Other)?;
        if !self.enabled || state.failed {
            return Err(VfsError::ResourceBusy);
        }
        if state.closed {
            return Ok(false);
        }
        state.closed = true;
        Ok(true)
    }

    async fn wait_drivers(&self) -> Result<(), VfsError> {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let state = self.state.lock().map_err(|_| VfsError::Other)?;
                if state.failed {
                    return Err(VfsError::Other);
                }
                if state.closed && state.active == 0 {
                    return Ok(());
                }
            }
            notified.await;
        }
    }

    fn finish_drain(&self, success: bool) {
        if let Ok(mut state) = self.state.lock() {
            state.drained = success;
            state.failed |= !success;
        }
        self.notify.notify_waiters();
    }

    async fn wait_finished(&self) -> Result<(), VfsError> {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let state = self.state.lock().map_err(|_| VfsError::Other)?;
                if state.failed {
                    return Err(VfsError::Other);
                }
                if state.drained && state.closed && state.active == 0 {
                    return Ok(());
                }
            }
            notified.await;
        }
    }
}

pub(super) struct MutationDriver {
    gate: Arc<PackedMutationGate>,
    _argument_owner: Option<MetadataMemoryGuard>,
    terminal: bool,
}

struct DrainCompletionOwner {
    gate: Arc<PackedMutationGate>,
    terminal: bool,
}
impl Drop for DrainCompletionOwner {
    fn drop(&mut self) {
        if !self.terminal {
            self.gate.finish_drain(false);
        }
    }
}

impl MutationDriver {
    /// Hot read-only handle operations usually complete without yielding. Poll
    /// once with the same authority/owner as a detached driver. If it waits,
    /// move that *same* future to a task before yielding to the caller, so a
    /// cancelled reply still cannot cancel a half-completed open/close.
    pub(super) async fn run_ready<F, T>(self, future: F) -> Result<T, VfsError>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        use futures_util::FutureExt;
        let gate = self.gate.clone();
        let request = crate::meta::layer::capture_request_scope();
        let owned = MUTATION_OWNER.scope(gate, async move {
            let mut owner = self;
            let result = request.scope(future).await;
            owner.terminal = true;
            result
        });
        // Match run(): a panic poisons the owner and returns an error instead
        // of unwinding the FUSE caller, including a panic in the first poll.
        let mut owned = Box::pin(std::panic::AssertUnwindSafe(owned).catch_unwind());
        if let Some(result) = owned.as_mut().now_or_never() {
            return result.map_err(|_| VfsError::Other);
        }
        tokio::spawn(owned)
            .await
            .map_err(|_| VfsError::Other)?
            .map_err(|_| VfsError::Other)
    }

    pub(super) async fn run<F, T>(self, future: F) -> Result<T, VfsError>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let (sender, receiver) = oneshot::channel();
        let gate = self.gate.clone();
        // The driver executes this same request, so it carries the request's
        // scoped actor, access mask and one-shot authorities; a bare spawn
        // would silently drop them.
        let request = crate::meta::layer::capture_request_scope();
        // Dropping receiver never cancels this driver or releases its owner.
        tokio::spawn(MUTATION_OWNER.scope(gate, async move {
            let mut owner = self;
            let result = request.scope(future).await;
            owner.terminal = true;
            let _ = sender.send(result);
        }));
        receiver.await.map_err(|_| VfsError::Other)
    }
}

impl Drop for MutationDriver {
    fn drop(&mut self) {
        if let Ok(mut state) = self.gate.state.lock() {
            state.active -= 1;
            state.failed |= !self.terminal;
        }
        self.gate.notify.notify_waiters();
    }
}

/// Only records this VFS's closed and completed local I/O boundary. The native
/// workspace identity and effective-view mapping must be checked separately.
#[allow(dead_code)] // Awaiting the typed native effective-view factory.
pub(crate) struct PackedVfsDrainFence<S, M>
where
    S: BlockStore + Send + Sync + 'static,
    M: MetaLayer + Send + Sync + 'static,
{
    vfs: VFS<S, M>,
}

#[allow(dead_code)] // Awaiting the typed native effective-view factory.
impl<S, M> PackedVfsDrainFence<S, M>
where
    S: BlockStore + Send + Sync + 'static,
    M: MetaLayer + Send + Sync + 'static,
{
    pub(crate) async fn validate_local(&self) -> Result<(), VfsError> {
        self.vfs.state.packed_mutation_gate.wait_finished().await
    }

    pub(crate) fn is_same_vfs(&self, vfs: &VFS<S, M>) -> bool {
        Arc::ptr_eq(&self.vfs.state, &vfs.state) && Arc::ptr_eq(&self.vfs.core, &vfs.core)
    }

    pub(crate) fn frozen_source_components(&self) -> (Arc<M>, Arc<S>, ChunkLayout) {
        (
            self.vfs.meta_layer_arc(),
            self.vfs.core.backend.store_arc(),
            self.vfs.core.layout,
        )
    }
}

impl<S, M> VFS<S, M>
where
    S: BlockStore + Send + Sync + 'static,
    M: MetaLayer + Send + Sync + 'static,
{
    pub(super) fn packed_mutation_needs_driver(&self) -> bool {
        self.state.packed_mutation_gate.requires_driver()
    }

    pub(super) fn packed_mutation_admit(
        &self,
        lengths: &[usize],
    ) -> Result<MutationDriver, VfsError> {
        let bytes = lengths
            .iter()
            .try_fold(MUTATION_DRIVER_BYTES, |total, &len| {
                total
                    .checked_add(u64::try_from(len).map_err(|_| VfsError::InvalidInput)?)
                    .ok_or(VfsError::InvalidInput)
            })?;
        let owner = self
            .core
            .meta_layer
            .reserve_memory(MetadataMemoryKind::Request, bytes)
            .map_err(|error| VfsError::from_meta(PathHint::none(), error))?;
        self.state.packed_mutation_gate.admit(owner)
    }

    /// Close local mutation admission, wait for uncancelled owned mutations,
    /// then require metadata commit AND remote upload completion. Dropping the
    /// caller's future leaves admission closed and the drain driver running.
    #[allow(dead_code)] // Awaiting the typed native effective-view factory.
    pub(crate) async fn quiesce_packed_vfs(&self) -> Result<PackedVfsDrainFence<S, M>, VfsError> {
        #[cfg(feature = "native-packed-base")]
        if self.native_runtime().is_some() {
            return Err(VfsError::Unsupported);
        }
        let gate = self.state.packed_mutation_gate.clone();
        if gate.start_close()? {
            let vfs = self.clone();
            let drain_gate = gate.clone();
            tokio::spawn(async move {
                let mut completion = DrainCompletionOwner {
                    gate: drain_gate.clone(),
                    terminal: false,
                };
                let result: Result<(), VfsError> = async {
                    drain_gate.wait_drivers().await?;
                    MUTATION_OWNER
                        .scope(drain_gate.clone(), async {
                            // Writer drain protects cleanup and retains its bounded
                            // inode list owner through timestamp synchronization.
                            let drained = vfs
                                .state
                                .writer
                                .drain_for_packed_publication(MAX_PUBLICATION_WRITERS)
                                .await
                                .map_err(VfsError::from)?;
                            for ino in drained.inodes() {
                                let dirty = vfs.state.handles.take_write_dirty_for_inode(*ino);
                                if dirty.dirty
                                    && let Err(error) = vfs.update_mtime_ctime(*ino).await
                                {
                                    vfs.state.handles.mark_write_dirty_for_inode(*ino);
                                    return Err(error);
                                }
                            }
                            drop(drained);
                            Ok(())
                        })
                        .await
                }
                .await;
                if let Err(error) = &result {
                    tracing::warn!(?error, "packed VFS publication drain failed");
                }
                drain_gate.finish_drain(result.is_ok());
                completion.terminal = true;
            });
        }
        gate.wait_finished().await?;
        Ok(PackedVfsDrainFence { vfs: self.clone() })
    }
}

#[cfg(test)]
mod tests;
