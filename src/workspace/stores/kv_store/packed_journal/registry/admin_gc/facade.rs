//! Direct bounded operator GC. Public requests carry routes, never authority.
use super::*;
use crate::chunk::ChunkLayout;
use std::sync::atomic::{AtomicBool, Ordering};

#[path = "native.rs"]
mod native;

#[derive(Clone, Copy, Debug)]
pub struct PackedGcPolicy {
    pub lease_ttl_seconds: u64,
    pub grace_seconds: u64,
    pub max_scans: u16,
    /// Candidate quanta per tick. A graph quantum releases one member (and
    /// completes its DELETE); history releases one member or drains one DELETE.
    pub max_operations: u16,
    pub max_protective_rows: u64,
}
impl PackedGcPolicy {
    pub fn validate(self) -> Result<(), WorkspaceError> {
        if self.lease_ttl_seconds < crate::workspace::lifecycle::DEFAULT_LEASE_TTL.as_secs()
            || self.grace_seconds < self.lease_ttl_seconds
            || self.grace_seconds > 86400
            || !(1..=64).contains(&self.max_scans)
            || !(1..=16).contains(&self.max_operations)
            || self.max_protective_rows == 0
            || self.max_protective_rows > 1_048_576
        {
            return Err(WorkspaceError::InvalidReadPlan(
                "invalid packed operator GC policy".into(),
            ));
        }
        Ok(())
    }
    fn grace_ns(self) -> u64 {
        self.grace_seconds * 1_000_000_000
    }
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
enum Tier {
    #[default]
    History,
    Lease,
    Journal,
    Object,
    NativeWorkspace,
    Native,
}
impl Tier {
    fn prefix(self) -> &'static [u8] {
        match self {
            Self::History => b"packed/registry/history-root/",
            Self::Lease => HOT_LEASE_PREFIX,
            Self::Journal => JOURNAL_PREFIX,
            Self::Object => REGISTRY_OBJECT_PREFIX.as_bytes(),
            Self::NativeWorkspace => HOT_WORKSPACE_PREFIX,
            Self::Native => HOT_LAYER_PREFIX,
        }
    }
    fn next(self) -> Self {
        match self {
            Self::History => Self::Lease,
            Self::Lease => Self::Journal,
            Self::Journal => Self::Object,
            Self::Object => Self::NativeWorkspace,
            Self::NativeWorkspace => Self::Native,
            Self::Native => Self::History,
        }
    }
}

/// Persistable pure routing cursor; decoding cannot mint a deletion proof.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PackedGcCursor {
    tier: Tier,
    after: Option<Vec<u8>>,
}
impl PackedGcCursor {
    fn validate(&self) -> Result<(), WorkspaceError> {
        if self.after.as_ref().is_some_and(|key| {
            key.len() > 256 || self.tier.prefix().is_empty() || !key.starts_with(self.tier.prefix())
        }) {
            return Err(WorkspaceError::Fenced);
        }
        Ok(())
    }
    pub fn encode(&self) -> Result<Vec<u8>, WorkspaceError> {
        self.validate()?;
        let bytes = serde_json::to_vec(self).map_err(journal_error)?;
        if bytes.len() > 1024 {
            return Err(WorkspaceError::Fenced);
        }
        Ok(bytes)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, WorkspaceError> {
        if bytes.len() > 1024 {
            return Err(WorkspaceError::Fenced);
        }
        let cursor: Self = serde_json::from_slice(bytes).map_err(journal_error)?;
        cursor.validate()?;
        Ok(cursor)
    }
    fn advance(&mut self) {
        self.tier = self.tier.next();
        self.after = None;
    }
}

pub struct PackedGcTickRequest {
    pub policy: PackedGcPolicy,
    pub cursor: PackedGcCursor,
    pub cancel: CancellationToken,
}
#[derive(Debug)]
pub struct PackedGcTickReport {
    pub next_cursor: PackedGcCursor,
    pub scanned: u16,
    pub attempted: u16,
    pub deferred: u16,
    pub deleted_objects: u64,
    pub native_deleted_layers: u64,
}

#[async_trait]
pub trait PackedGcAdmin: Send + Sync {
    async fn tick(
        &self,
        request: PackedGcTickRequest,
    ) -> Result<PackedGcTickReport, WorkspaceError>;
    /// Stop admission and wait for the complete owned driver before backend shutdown.
    async fn shutdown(&self) -> Result<(), WorkspaceError>;
}

struct Runtime<B: WorkspaceKvBackend, O: ObjectBackend + Clone> {
    store: Arc<KvWorkspaceStore<B>>,
    client: ObjectClient<O>,
    budget: Arc<SnapshotMountBudget>,
    layout: ChunkLayout,
    serial: tokio::sync::Mutex<()>,
    closed: AtomicBool,
}
struct Handle<B: WorkspaceKvBackend, O: ObjectBackend + Clone> {
    runtime: Arc<Runtime<B, O>>,
}

impl<B: WorkspaceKvBackend> KvWorkspaceStore<B> {
    pub async fn open_packed_gc_admin<O: ObjectBackend + Clone + 'static>(
        self: &Arc<Self>,
        client: ObjectClient<O>,
        budget: Arc<SnapshotMountBudget>,
        layout: ChunkLayout,
    ) -> Result<Arc<dyn PackedGcAdmin>, WorkspaceError> {
        self.require_admin_access()?;
        self.configure_packed_reader_pin_budget(budget.clone())?;
        let _owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, OPERATION_BYTES)])
            .map_err(journal_budget_error)?;
        if budget.state().closed || !client.forbids_mutation_replay() || layout.block_size == 0 {
            return Err(WorkspaceError::UnsupportedCapability(
                "GC requires a live no-replay admin object client",
            ));
        }
        // Current raw shared-password/unauthenticated deployments deliberately
        // refuse here. Do not turn an operator flag into a credential proof.
        self.backend.authenticate_gc_admin().await?;
        self.validate_packed_gc_volume_header(&budget).await?;
        Ok(Arc::new(Handle {
            runtime: Arc::new(Runtime {
                store: self.clone(),
                client,
                budget,
                layout,
                serial: tokio::sync::Mutex::new(()),
                closed: AtomicBool::new(false),
            }),
        }))
    }
}

#[async_trait]
impl<B, O> PackedGcAdmin for Handle<B, O>
where
    B: WorkspaceKvBackend,
    O: ObjectBackend + Clone + 'static,
{
    async fn tick(
        &self,
        request: PackedGcTickRequest,
    ) -> Result<PackedGcTickReport, WorkspaceError> {
        request.policy.validate()?;
        request.cursor.validate()?;
        let runtime = self.runtime.clone();
        // A dropped Kubernetes/controller waiter cannot abandon an admitted
        // transport or close its shared ledger while the operation runs.
        tokio::spawn(async move {
            let _serial = runtime.serial.lock().await;
            runtime.tick_owned(request).await
        })
        .await
        .map_err(journal_error)?
    }
    async fn shutdown(&self) -> Result<(), WorkspaceError> {
        self.runtime.closed.store(true, Ordering::Release);
        let _serial = self.runtime.serial.lock().await;
        self.runtime.store.shutdown_metadata_backend().await?;
        self.runtime.budget.close();
        Ok(())
    }
}

enum Candidate {
    History(WorkspaceId, u64),
    Lease(LeaseId),
    Journal(JournalId),
    Object(SnapshotObjectRef),
    NativeLayer(LayerId),
}

#[derive(Default)]
struct Collection {
    deleted_objects: u64,
    native_deleted_layers: u64,
    deferred: bool,
}
impl Collection {
    fn objects(deleted_objects: u64) -> Self {
        Self {
            deleted_objects,
            ..Self::default()
        }
    }
}

fn candidate(tier: Tier, entry: &KvEntry) -> Result<Option<Candidate>, WorkspaceError> {
    let selected = match tier {
        Tier::History => {
            let root = RootRow::decode(&entry.value)?;
            let binding = root.binding.as_ref().ok_or(WorkspaceError::Fenced)?;
            if entry.key != registry_history_root_key(binding) {
                return Err(WorkspaceError::Fenced);
            }
            matches!(root.state, RootState::BindingHistory | RootState::Retiring).then_some(
                Candidate::History(binding.workspace_id, binding.binding.binding_version),
            )
        }
        Tier::Lease => {
            let lease: SnapshotLease = decode_open_value(&entry.value, OPEN_RECORD_MAX_BYTES)?;
            if entry.key != hot_lease_key(lease.workspace_id, lease.lease_id) {
                return Err(WorkspaceError::Fenced);
            }
            (lease.state != LeaseState::Released).then_some(Candidate::Lease(lease.lease_id))
        }
        Tier::Journal => {
            let journal = PackedJournalRecord::decode(&entry.value)?;
            if entry.key != journal_key(journal.journal_id) {
                return Err(WorkspaceError::Fenced);
            }
            (journal.phase == PackedJournalPhase::Aborted)
                .then_some(Candidate::Journal(journal.journal_id))
        }
        Tier::Object => {
            let object = ObjectRow::decode(&entry.value)?;
            if entry.key != registry_object_key(&object.reference) {
                return Err(WorkspaceError::Fenced);
            }
            (object.state == ObjectState::Retiring).then_some(Candidate::Object(object.reference))
        }
        Tier::NativeWorkspace => {
            let workspace: WorkspaceRecord =
                decode_open_value(&entry.value, OPEN_RECORD_MAX_BYTES)?;
            if workspace.workspace_id.as_uuid().is_nil()
                || workspace.head_layer_id.as_uuid().is_nil()
                || workspace.head_epoch == 0
                || entry.key != hot_workspace_key(workspace.workspace_id)
            {
                return Err(WorkspaceError::Fenced);
            }
            (workspace.state == WorkspaceState::Deleting)
                .then_some(Candidate::NativeLayer(workspace.head_layer_id))
        }
        Tier::Native => {
            let layer: LayerRecord = decode_open_value(&entry.value, OPEN_RECORD_MAX_BYTES)?;
            if layer.layer_id.as_uuid().is_nil()
                || entry.key != hot_layer_key(layer.layer_id)
                || layer.schema_version != WORKSPACE_SCHEMA_VERSION
                || layer.created_at_ns <= 0
            {
                return Err(WorkspaceError::Fenced);
            }
            Some(Candidate::NativeLayer(layer.layer_id))
        }
    };
    Ok(selected)
}

impl<B, O> Runtime<B, O>
where
    B: WorkspaceKvBackend,
    O: ObjectBackend + Clone + 'static,
{
    fn live(&self, cancel: &CancellationToken) -> Result<(), WorkspaceError> {
        if self.closed.load(Ordering::Acquire)
            || self.budget.state().closed
            || cancel.is_cancelled()
        {
            return Err(WorkspaceError::Busy);
        }
        Ok(())
    }
    async fn tick_owned(
        &self,
        request: PackedGcTickRequest,
    ) -> Result<PackedGcTickReport, WorkspaceError> {
        self.live(&request.cancel)?;
        let _owner = self
            .budget
            .admit(&[(SnapshotBudgetPool::Metadata, OPERATION_BYTES)])
            .map_err(journal_budget_error)?;
        self.store.backend.authenticate_gc_admin().await?;
        self.live(&request.cancel)?;
        let mut report = PackedGcTickReport {
            next_cursor: request.cursor,
            scanned: 0,
            attempted: 0,
            deferred: 0,
            deleted_objects: 0,
            native_deleted_layers: 0,
        };
        while report.scanned < request.policy.max_scans
            && report.attempted < request.policy.max_operations
        {
            self.live(&request.cancel)?;
            let prefix = report.next_cursor.tier.prefix();
            let page = self
                .store
                .backend
                .scan_prefix_page_with_byte_limits(
                    prefix,
                    report.next_cursor.after.as_deref(),
                    KvReadLimits {
                        max_records: 1,
                        max_key_bytes: 256,
                        max_value_bytes: RECORD_LIMIT,
                        max_total_bytes: RECORD_LIMIT + 256,
                        max_response_bytes: 128 << 10,
                        max_data_requests: 32,
                    },
                )
                .await?;
            report.scanned += 1;
            self.live(&request.cancel)?;
            if page.len() > 1 {
                return Err(WorkspaceError::Fenced);
            }
            let Some(entry) = page.first() else {
                let finished = matches!(report.next_cursor.tier, Tier::Native);
                report.next_cursor.advance();
                // The real empty layer page closes one fair cycle. A short
                // nonempty workspace/layer page never resets its deep cursor.
                if finished {
                    break;
                }
                continue;
            };
            if !entry.key.starts_with(prefix)
                || report
                    .next_cursor
                    .after
                    .as_ref()
                    .is_some_and(|after| entry.key <= *after)
            {
                return Err(WorkspaceError::Fenced);
            }
            // One-row pages allow quota exhaustion without dropping unprocessed
            // rows. Failed protected targets do not starve later routes.
            report.next_cursor.after = Some(entry.key.clone());
            let selected = match candidate(report.next_cursor.tier, entry) {
                Ok(selected) => selected,
                Err(_)
                    if matches!(
                        report.next_cursor.tier,
                        Tier::NativeWorkspace | Tier::Native
                    ) =>
                {
                    // A malformed native row is never deletion authority. Its
                    // key still provides a bounded routing position so a bad
                    // first row cannot prevent unrelated valid layers running.
                    report.deferred += 1;
                    continue;
                }
                Err(error) => return Err(error),
            };
            if let Some(selected) = selected {
                self.live(&request.cancel)?;
                report.attempted += 1;
                match self
                    .collect_candidate(selected, request.policy, request.cancel.clone())
                    .await
                {
                    Ok(collected) => {
                        report.deleted_objects += collected.deleted_objects;
                        report.native_deleted_layers += collected.native_deleted_layers;
                        report.deferred += u16::from(collected.deferred);
                    }
                    Err(WorkspaceError::Fenced | WorkspaceError::Busy)
                        if !request.cancel.is_cancelled() =>
                    {
                        report.deferred += 1
                    }
                    Err(error) => return Err(error),
                }
            }
        }
        self.live(&request.cancel)?;
        Ok(report)
    }
    async fn collect_candidate(
        &self,
        selected: Candidate,
        policy: PackedGcPolicy,
        cancel: CancellationToken,
    ) -> Result<Collection, WorkspaceError> {
        match selected {
            Candidate::History(workspace_id, binding_version) => Ok(Collection::objects(
                self.store
                    .retire_packed_binding_history_version(
                        self.client.clone(),
                        self.budget.clone(),
                        PackedHistoryVersionRetirementOptions {
                            workspace_id,
                            binding_version,
                            grace_ns: policy.grace_ns(),
                            max_native_holds: policy.max_protective_rows,
                            max_current_bindings: policy.max_protective_rows,
                            max_work: 1,
                            cancel,
                        },
                    )
                    .await?
                    .deleted_objects,
            )),
            Candidate::Lease(lease_id) => {
                self.store
                    .reap_packed_native_lease(
                        self.budget.clone(),
                        PackedNativeLeaseReaperOptions {
                            lease_id,
                            grace_ns: policy.grace_ns(),
                            max_protective_rows: policy.max_protective_rows,
                            cancel,
                        },
                    )
                    .await?;
                Ok(Collection::default())
            }
            Candidate::Journal(journal) => Ok(Collection::objects(
                self.store
                    .collect_aborted_packed_journal(journal, &self.client, &self.budget, 1, cancel)
                    .await?,
            )),
            Candidate::Object(reference) => {
                self.store
                    .delete_registered_packed_object(&self.client, reference, &self.budget, &cancel)
                    .await?;
                // This API can return success for an already-completed row.
                // It does not report a confirmed physical DELETE count.
                Ok(Collection::default())
            }
            Candidate::NativeLayer(layer) => {
                let result = native::collect_one(
                    self.store.clone(),
                    self.client.clone(),
                    self.budget.clone(),
                    self.layout,
                    policy,
                    cancel.clone(),
                    layer,
                )
                .await;
                self.live(&cancel)?;
                match result {
                    Ok(deleted) => Ok(Collection {
                        native_deleted_layers: deleted,
                        ..Collection::default()
                    }),
                    // A bounded proof/transport/quarantined range failure grants
                    // no authority. Keep its durable range state and move the
                    // routing cursor past this target so another layer can run.
                    Err(_) => Ok(Collection {
                        deferred: true,
                        ..Collection::default()
                    }),
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "facade_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "native_fair_tests.rs"]
mod native_fair_tests;
