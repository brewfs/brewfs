//! Reader retention from binding-open through heartbeat, drain and release.
//! This does not replace native head/version fencing or complete graph proof.

use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::{Mutex, Notify};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::catalog::{HeadGuard, PackedLowerBinding, WorkspaceStore};
use super::error::WorkspaceError;
use super::model::LayerRecord;
use super::packed_v3::wire005::{V3BudgetPool, V3MountBudget, V3OwnedPermit};
use super::publish::binding::PackedLowerBindingRecord;
use super::stores::kv_backend::WorkspaceKvBackend;
use super::stores::kv_store::KvWorkspaceStore;
use super::stores::kv_store::packed_native_freeze::{
    PackedNativeQuiesceFence, PackedNativeRecoveryReadFence,
};
use super::stores::kv_store::packed_reader_pins::{
    AcquirePackedReaderPin, MAX_PACKED_READER_GRACE_NS, MAX_PACKED_READER_TTL_NS,
    OwnedPackedReaderPin, PACKED_READER_SLOT_COUNT, PackedReaderPin, PackedReaderPinState,
};

#[cfg(test)]
fn reader_lifecycle_diagnostic(stage: &str, error: &WorkspaceError) {
    let variant = match error {
        WorkspaceError::Fenced => "Fenced",
        WorkspaceError::Busy => "Busy",
        WorkspaceError::Backend(_) => "Backend",
        WorkspaceError::CorruptMetadata(_) => "CorruptMetadata",
        WorkspaceError::InvalidReadPlan(_) => "InvalidReadPlan",
        _ => "Other",
    };
    eprintln!("[packed-v3-reader-diag] stage={stage} error={variant}");
}

#[derive(Clone, Debug)]
pub struct PackedReaderLeaseOptions {
    pub owner_id: String,
    pub holder_generation: u64,
    pub ttl_ns: u64,
    pub gc_grace_ns: u64,
}
impl Default for PackedReaderLeaseOptions {
    fn default() -> Self {
        Self {
            owner_id: format!("packed-reader-{}", Uuid::new_v4()),
            holder_generation: 1,
            ttl_ns: 30_000_000_000,
            gc_grace_ns: 30_000_000_000,
        }
    }
}

fn validate_reader_options(options: &PackedReaderLeaseOptions) -> Result<(), WorkspaceError> {
    if options.ttl_ns < 3_000_000
        || options.ttl_ns > MAX_PACKED_READER_TTL_NS
        || options.gc_grace_ns > MAX_PACKED_READER_GRACE_NS
        || options.owner_id.is_empty()
        || options.owner_id.len() > 256
        || options.holder_generation == 0
    {
        return Err(WorkspaceError::InvalidReadPlan(
            "invalid reader heartbeat/lease bounds".into(),
        ));
    }
    Ok(())
}

#[async_trait]
pub trait PackedReaderSession: Send + Sync {
    fn binding(&self) -> &PackedLowerBinding;
    fn mount_budget(&self) -> Arc<V3MountBudget>;
    fn retain_request(&self) -> Result<PackedReaderRequestOwner, WorkspaceError>;
    /// Close new request admission while lease renewal protects draining I/O.
    fn stop_admission(&self);
    async fn validate(&self) -> Result<(), WorkspaceError>;
    /// Stop admission/renewal, join heartbeat, wait all owners, then release.
    async fn shutdown(&self) -> Result<(), WorkspaceError>;
}

struct ReaderLocalState {
    accepting: bool,
    fenced: bool,
    owners: usize,
}
struct ReaderOwners {
    state: StdMutex<ReaderLocalState>,
    changed: Notify,
}

/// Keep this owner in each captured metadata/data generation until all remote
/// work and success validation have ended. Drop wakes shutdown's drain waiter.
pub struct PackedReaderRequestOwner {
    local: Arc<ReaderOwners>,
    _generation: Arc<OwnedPackedReaderPin<PackedReaderPin>>,
    _permit: V3OwnedPermit,
    _recovery: Option<Arc<dyn Send + Sync>>,
}
impl Drop for PackedReaderRequestOwner {
    fn drop(&mut self) {
        let mut state = self.local.state.lock().unwrap();
        assert!(state.owners > 0, "reader owner count underflow");
        state.owners -= 1;
        drop(state);
        self.local.changed.notify_waiters();
    }
}

pub struct KvPackedReaderSession<B> {
    store: Arc<KvWorkspaceStore<B>>,
    binding: PackedLowerBinding,
    budget: Arc<V3MountBudget>,
    current: Arc<StdMutex<Arc<OwnedPackedReaderPin<PackedReaderPin>>>>,
    local: Arc<ReaderOwners>,
    cancel: CancellationToken,
    heartbeat: Mutex<Option<JoinHandle<()>>>,
    shutdown_gate: Mutex<bool>,
    _permit: Arc<V3OwnedPermit>,
    // Retain the durable recovery basis, without rechecking an obsolete native
    // phase during pin renewals. The source owns subsequent phase authority.
    _recovery: Option<Arc<PackedNativeRecoveryReadFence<B>>>,
}

impl<B: WorkspaceKvBackend> KvPackedReaderSession<B> {
    /// Load the current binding and acquire its pin before PM11/object open.
    pub async fn open(
        store: Arc<KvWorkspaceStore<B>>,
        guard: HeadGuard,
        budget: Arc<V3MountBudget>,
        options: PackedReaderLeaseOptions,
    ) -> Result<Arc<Self>, WorkspaceError> {
        validate_reader_options(&options)?;
        let budget = store.resolve_packed_reader_pin_budget(budget);
        let permit = budget
            .admit(&[(V3BudgetPool::Metadata, 1 << 20)])
            .map_err(|error| WorkspaceError::InvalidReadPlan(error.to_string()))?;
        let binding = store
            .load_packed_binding_record(guard.clone())
            .await?
            .ok_or(WorkspaceError::Fenced)?;
        let layers: [LayerRecord; 2] = store
            .load_layer_chain(guard.expected_head_layer_id)
            .await?
            .try_into()
            .map_err(|_| {
                WorkspaceError::InvalidReadPlan("reader binding needs fixed native pair".into())
            })?;
        let acquired =
            Self::acquire_session_pin(&store, &guard, &layers, &binding, &options, None).await?;
        Self::open_from_acquired(
            store,
            binding.binding,
            budget,
            options.ttl_ns,
            acquired,
            permit,
            None,
        )
        .await
    }

    /// Pre-PNB recovery uses the same actual typed pin lifecycle, retaining
    /// the original seed/current owner source through every backend request.
    pub(crate) async fn open_native_prepare(
        store: &Arc<KvWorkspaceStore<B>>,
        native: Arc<PackedNativeQuiesceFence<B>>,
        budget: Arc<V3MountBudget>,
        options: PackedReaderLeaseOptions,
    ) -> Result<Arc<Self>, WorkspaceError> {
        let recovery = store
            .reissue_native_seed_read(native, budget.clone())
            .await?;
        Self::open_native_recovery(recovery, budget, options).await
    }

    /// Reopen only the actual durable frozen view after process recovery.
    /// This grants packed-byte retention, not effective-view or hash proof.
    pub(crate) async fn open_native_recovery(
        recovery: Arc<PackedNativeRecoveryReadFence<B>>,
        budget: Arc<V3MountBudget>,
        options: PackedReaderLeaseOptions,
    ) -> Result<Arc<Self>, WorkspaceError> {
        validate_reader_options(&options)?;
        let store = recovery.store().clone();
        if !Arc::ptr_eq(&budget, &recovery.native_quiesce().mount_budget()) {
            return Err(WorkspaceError::Fenced);
        }
        store.configure_packed_reader_pin_budget(budget.clone())?;
        let permit = budget
            .admit(&[(V3BudgetPool::Metadata, 1 << 20)])
            .map_err(|error| WorkspaceError::InvalidReadPlan(error.to_string()))?;
        let (sender, receiver) = tokio::sync::oneshot::channel();
        // This driver owns the token and admission through every actual
        // acquisition response. Cancelling open cannot detach a CAS future.
        tokio::spawn(async move {
            let result = Self::open_native_recovery_owned(recovery, budget, options, permit).await;
            if let Err(Ok(session)) = sender.send(result) {
                // No caller received the installed pin. Observe heartbeat and
                // release terminal results before dropping the durable basis.
                if let Err(error) = session.shutdown().await {
                    tracing::warn!(?error, "abandoned recovery reader cleanup failed");
                }
            }
        });
        receiver.await.map_err(|error| {
            WorkspaceError::Backend(format!("recovery reader open driver: {error}"))
        })?
    }

    async fn open_native_recovery_owned(
        recovery: Arc<PackedNativeRecoveryReadFence<B>>,
        budget: Arc<V3MountBudget>,
        options: PackedReaderLeaseOptions,
        permit: V3OwnedPermit,
    ) -> Result<Arc<Self>, WorkspaceError> {
        recovery.validate().await?;
        let native = recovery.native_quiesce();
        let store = recovery.store().clone();
        let guard = native.source_guard().clone();
        let binding = native.binding().clone();
        let mut layers = native.mapping().old_layers().clone();
        layers[0].state = super::model::LayerState::Sealing;
        let acquired = Self::acquire_session_pin(
            &store,
            &guard,
            &layers,
            &binding,
            &options,
            Some(recovery.as_ref()),
        )
        .await?;
        Self::open_from_acquired(
            store,
            binding.binding,
            budget,
            options.ttl_ns,
            acquired,
            permit,
            Some(recovery),
        )
        .await
    }

    async fn acquire_session_pin(
        store: &Arc<KvWorkspaceStore<B>>,
        guard: &HeadGuard,
        layers: &[LayerRecord; 2],
        binding: &PackedLowerBindingRecord,
        options: &PackedReaderLeaseOptions,
        recovery: Option<&PackedNativeRecoveryReadFence<B>>,
    ) -> Result<OwnedPackedReaderPin<PackedReaderPin>, WorkspaceError> {
        for _ in 0..64 {
            let slots = store.list_packed_reader_pin_slots().await?;
            let choice = (0..PACKED_READER_SLOT_COUNT)
                .find_map(
                    |slot| match slots.iter().find(|pin| usize::from(pin.slot) == slot) {
                        None => Some((slot as u16, 0)),
                        Some(pin) if pin.state != PackedReaderPinState::Active => {
                            Some((pin.slot, pin.slot_generation))
                        }
                        _ => None,
                    },
                )
                .ok_or(WorkspaceError::Busy)?;
            let request = AcquirePackedReaderPin {
                slot: choice.0,
                expected_slot_generation: choice.1,
                pin_id: Uuid::new_v4(),
                owner_id: options.owner_id.clone(),
                holder_generation: options.holder_generation,
                ttl_ns: options.ttl_ns,
                gc_grace_ns: options.gc_grace_ns,
                guard: guard.clone(),
                expected_layers: layers.clone(),
                expected_binding: binding.clone(),
            };
            // Retry the identical request after transport uncertainty: choosing
            // a new UUID here would leak a second pin for the same mount open.
            let result = match Self::acquire_for_mode(store, &request, recovery).await {
                Err(WorkspaceError::Backend(_)) => {
                    Self::acquire_for_mode(store, &request, recovery).await
                }
                other => other,
            };
            match result {
                Ok(pin) => return Ok(pin),
                Err(WorkspaceError::Busy) => tokio::task::yield_now().await,
                Err(error) => return Err(error),
            }
        }
        Err(WorkspaceError::Busy)
    }

    async fn acquire_for_mode(
        store: &Arc<KvWorkspaceStore<B>>,
        request: &AcquirePackedReaderPin,
        recovery: Option<&PackedNativeRecoveryReadFence<B>>,
    ) -> Result<OwnedPackedReaderPin<PackedReaderPin>, WorkspaceError> {
        match recovery {
            Some(fence) => {
                store
                    .acquire_native_recovery_reader_pin(request, fence)
                    .await
            }
            None => store.acquire_packed_reader_pin(request).await,
        }
    }

    async fn open_from_acquired(
        store: Arc<KvWorkspaceStore<B>>,
        binding: PackedLowerBinding,
        budget: Arc<V3MountBudget>,
        ttl_ns: u64,
        acquired: OwnedPackedReaderPin<PackedReaderPin>,
        permit: V3OwnedPermit,
        recovery: Option<Arc<PackedNativeRecoveryReadFence<B>>>,
    ) -> Result<Arc<Self>, WorkspaceError> {
        let pin = Arc::new(acquired);
        let session = Arc::new(Self {
            store,
            binding,
            budget,
            current: Arc::new(StdMutex::new(pin)),
            local: Arc::new(ReaderOwners {
                state: StdMutex::new(ReaderLocalState {
                    accepting: true,
                    fenced: false,
                    owners: 0,
                }),
                changed: Notify::new(),
            }),
            cancel: CancellationToken::new(),
            heartbeat: Mutex::new(None),
            shutdown_gate: Mutex::new(false),
            _permit: Arc::new(permit),
            _recovery: recovery,
        });
        // Runtime state is independent of the session. The task must not hold
        // the session Arc across network await, which would defer Drop.cancel.
        let current = session.current.clone();
        let store = session.store.clone();
        let local = session.local.clone();
        let cancel = session.cancel.clone();
        let runtime_permit = session._permit.clone();
        let recovery_owner = session._recovery.clone();
        let cache_budget = session.budget.clone();
        let period = Duration::from_nanos(ttl_ns / 3);
        let task = tokio::spawn(async move {
            let _runtime_permit = runtime_permit;
            let _recovery_owner = recovery_owner;
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = tokio::time::sleep(period) => {},
                }
                let expected = current.lock().unwrap().clone();
                let operation = Uuid::new_v4();
                // Join observes this mutation's terminal response. Shutdown
                // does not cancel a CAS future and then claim that it drained.
                let result = match store
                    .renew_packed_reader_pin(&expected, operation, ttl_ns)
                    .await
                {
                    Err(WorkspaceError::Backend(_)) => {
                        store
                            .renew_packed_reader_pin(&expected, operation, ttl_ns)
                            .await
                    }
                    other => other,
                };
                match result {
                    Ok(pin) => *current.lock().unwrap() = Arc::new(pin),
                    Err(error) => {
                        #[cfg(test)]
                        reader_lifecycle_diagnostic("heartbeat-renew-terminal", &error);
                        let mut state = local.state.lock().unwrap();
                        state.fenced = true;
                        state.accepting = false;
                        drop(state);
                        cache_budget.invalidate_read_caches();
                        local.changed.notify_waiters();
                        tracing::warn!(?error, "packed reader heartbeat lost authority");
                        break;
                    }
                }
            }
        });
        *session.heartbeat.lock().await = Some(task);
        Ok(session)
    }
}

impl<B> Drop for KvPackedReaderSession<B> {
    fn drop(&mut self) {
        // A Drop cannot attest asynchronous transport drain or release. The
        // crash/abandonment path relies on the actual backend-clock reaper.
        self.cancel.cancel();
    }
}

#[async_trait]
impl<B: WorkspaceKvBackend> PackedReaderSession for KvPackedReaderSession<B> {
    fn binding(&self) -> &PackedLowerBinding {
        &self.binding
    }
    fn mount_budget(&self) -> Arc<V3MountBudget> {
        self.budget.clone()
    }
    fn retain_request(&self) -> Result<PackedReaderRequestOwner, WorkspaceError> {
        let permit = self
            .budget
            .admit(&[(V3BudgetPool::Control, 4096)])
            .map_err(|error| WorkspaceError::InvalidReadPlan(error.to_string()))?;
        let mut state = self.local.state.lock().unwrap();
        if !state.accepting || state.fenced {
            #[cfg(test)]
            eprintln!(
                "[packed-v3-reader-diag] stage=retain-local error=Fenced accepting={} fenced={}",
                state.accepting, state.fenced,
            );
            return Err(WorkspaceError::Fenced);
        }
        state.owners = state
            .owners
            .checked_add(1)
            .ok_or_else(|| WorkspaceError::InvalidReadPlan("reader owner count overflow".into()))?;
        Ok(PackedReaderRequestOwner {
            local: self.local.clone(),
            _generation: self.current.lock().unwrap().clone(),
            _permit: permit,
            // A captured generation must keep the durable basis even if its
            // caller abandons the session while remote work is still running.
            _recovery: self
                ._recovery
                .as_ref()
                .map(|fence| fence.clone() as Arc<dyn Send + Sync>),
        })
    }
    async fn validate(&self) -> Result<(), WorkspaceError> {
        {
            let state = self.local.state.lock().unwrap();
            if !state.accepting || state.fenced {
                #[cfg(test)]
                eprintln!(
                    "[packed-v3-reader-diag] stage=validate-local-before error=Fenced accepting={} fenced={}",
                    state.accepting, state.fenced,
                );
                return Err(WorkspaceError::Fenced);
            }
        }
        let expected = self.current.lock().unwrap().clone();
        self.store
            .validate_packed_reader_pin(&expected)
            .await
            .inspect_err(|_error| {
                self.budget.invalidate_read_caches();
                #[cfg(test)]
                reader_lifecycle_diagnostic("validate-actual-pin", _error);
            })?;
        let state = self.local.state.lock().unwrap();
        if !state.accepting || state.fenced {
            #[cfg(test)]
            eprintln!(
                "[packed-v3-reader-diag] stage=validate-local-after error=Fenced accepting={} fenced={}",
                state.accepting, state.fenced,
            );
            return Err(WorkspaceError::Fenced);
        }
        Ok(())
    }
    fn stop_admission(&self) {
        self.local.state.lock().unwrap().accepting = false;
        self.budget.invalidate_read_caches();
        self.local.changed.notify_waiters();
    }
    async fn shutdown(&self) -> Result<(), WorkspaceError> {
        let mut closed = self.shutdown_gate.lock().await;
        if *closed {
            return Ok(());
        }
        self.stop_admission();
        self.cancel.cancel();
        let mut heartbeat = self.heartbeat.lock().await;
        // Keep the handle in its slot while waiting. Cancellation of this
        // shutdown future must not detach it or erase a later join obligation.
        let joined = if let Some(task) = heartbeat.as_mut() {
            Some(task.await)
        } else {
            None
        };
        heartbeat.take();
        drop(heartbeat);
        if let Some(result) = joined {
            result.map_err(|error| {
                WorkspaceError::Backend(format!("reader heartbeat join: {error}"))
            })?;
        }
        loop {
            let changed = self.local.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.local.state.lock().unwrap().owners == 0 {
                break;
            }
            changed.await;
        }
        let expected = self.current.lock().unwrap().clone();
        // Inspection can reconcile a lost renewal response after its lease
        // expires, without granting new read authority to the shutdown path.
        let latest = self.store.inspect_packed_reader_pin(&expected).await?;
        if latest.state == PackedReaderPinState::Active {
            let operation = Uuid::new_v4();
            match self
                .store
                .release_packed_reader_pin(&latest, operation)
                .await
            {
                Err(WorkspaceError::Backend(_)) => {
                    self.store
                        .release_packed_reader_pin(&latest, operation)
                        .await?;
                }
                other => {
                    other?;
                }
            }
        }
        *closed = true;
        Ok(())
    }
}
