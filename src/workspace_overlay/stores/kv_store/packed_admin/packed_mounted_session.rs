//! Private concrete packed-v3 joint writer/open operations.
//! Depends on mandatory workspace-scoped writer authority; no CONTROL scan.
//! CLI selection and original physical-proof release are separate gated wiring.

use super::*;
#[path = "packed_mounted_recovery.rs"]
mod mounted_recovery;
use crate::workspace_overlay::stores::kv_store::packed_writer_authority::{
    PackedWriterAuthority, PackedWriterOwner, packed_writer_key,
};
pub use mounted_recovery::PackedRecoveredMountReport;
pub(in crate::workspace_overlay::stores::kv_store) use mounted_recovery::authenticate_mounted_recovery_writer;
pub(in crate::workspace_overlay::stores::kv_store::packed_admin) use mounted_recovery::{
    CleanSourceKind, CleanSourceRecord, decode_any_clean_source, decode_clean_source,
    encode_clean_source,
};
pub(crate) use mounted_recovery::{PackedMountedRecoveryRequest, capture_mounted_recovery_owner};

pub(crate) struct PackedMountGrantRequest {
    pub(crate) workspace_id: WorkspaceId,
    pub(crate) lease_id: LeaseId,
    pub(crate) holder_generation: u64,
    pub(crate) mount_uid: uuid::Uuid,
    pub(crate) pod_uid: uuid::Uuid,
    pub(crate) ttl_ns: u64,
}

pub(in crate::workspace_overlay::stores::kv_store::packed_admin) struct PackedOriginalReleaseRows<
    'a,
> {
    pub(in crate::workspace_overlay::stores::kv_store::packed_admin) writer:
        &'a PackedWriterAuthority,
    pub(in crate::workspace_overlay::stores::kv_store::packed_admin) lease: &'a SnapshotLease,
    pub(in crate::workspace_overlay::stores::kv_store::packed_admin) open: &'a V3OpenRecord,
}

/// Only the actual scoped writer/open CAS constructs this authority.
pub(crate) struct PackedMountedLease<B> {
    store: Arc<KvWorkspaceStore<B>>,
    guard: HeadGuard,
    reference: PackedReleasedMountReference,
    binding: PackedLowerBindingRecord,
    lease: SnapshotLease,
    renewals: Arc<PackedMountRenewalGate>,
    writer_incarnation: u64,
    open_owner: String,
    open_generation: u64,
    budget: Arc<V3MountBudget>,
    _owner: V3OwnedPermit,
}

// Admission and terminal accounting share one lock. A cancelled caller
// cannot hide an already-admitted owned worker from original clean shutdown.
#[derive(Default)]
struct PackedMountRenewalGate {
    state: std::sync::Mutex<PackedMountRenewalState>,
    changed: tokio::sync::Notify,
}

#[derive(Default)]
struct PackedMountRenewalState {
    closing: bool,
    active: usize,
}

struct PackedMountRenewalPermit {
    gate: Arc<PackedMountRenewalGate>,
}

impl PackedMountRenewalGate {
    fn enter(self: &Arc<Self>) -> Result<PackedMountRenewalPermit, WorkspaceError> {
        let mut state = self.state.lock().map_err(|_| WorkspaceError::Fenced)?;
        if state.closing {
            return Err(WorkspaceError::Fenced);
        }
        state.active = state.active.checked_add(1).ok_or(WorkspaceError::Fenced)?;
        Ok(PackedMountRenewalPermit { gate: self.clone() })
    }

    fn close_admission(&self) -> Result<(), WorkspaceError> {
        self.state
            .lock()
            .map_err(|_| WorkspaceError::Fenced)?
            .closing = true;
        Ok(())
    }

    fn closed_and_drained(&self) -> Result<bool, WorkspaceError> {
        let state = self.state.lock().map_err(|_| WorkspaceError::Fenced)?;
        Ok(state.closing && state.active == 0)
    }

    async fn wait_for_drain(&self) -> Result<(), WorkspaceError> {
        loop {
            // Register before examining active workers, including concurrent
            // shutdown waiters, so terminal notify_waiters cannot be missed.
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.closed_and_drained()? {
                return Ok(());
            }
            notified.await;
        }
    }

    async fn close_and_drain(&self) -> Result<(), WorkspaceError> {
        self.close_admission()?;
        self.wait_for_drain().await
    }
}

impl Drop for PackedMountRenewalPermit {
    fn drop(&mut self) {
        let mut state = self
            .gate
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        debug_assert!(state.active > 0);
        state.active = state.active.saturating_sub(1);
        let drained = state.closing && state.active == 0;
        drop(state);
        if drained {
            self.gate.changed.notify_waiters();
        }
    }
}

struct ScopedMountView {
    binding: PackedLowerBindingRecord,
    writer: PackedWriterAuthority,
    requested_lease: Option<SnapshotLease>,
    open: Option<V3OpenRecord>,
    checks: Vec<KvCheck>,
    now: i64,
    _registry_owner: Option<V3OwnedPermit>,
}

fn mount_owner(request: &PackedMountGrantRequest) -> Result<String, WorkspaceError> {
    if request.workspace_id.as_uuid().is_nil()
        || request.lease_id.as_uuid().is_nil()
        || request.mount_uid.is_nil()
        || request.pod_uid.is_nil()
        || request.holder_generation == 0
        || request.ttl_ns == 0
        || request.ttl_ns > 15 * 60 * 1_000_000_000
    {
        return Err(WorkspaceError::Fenced);
    }
    let owner = format!(
        "packed-v3/mounted/{}/{}/{}/{}",
        request.mount_uid, request.pod_uid, request.lease_id, request.holder_generation
    );
    if owner.len() > OPEN_OWNER_MAX_BYTES {
        return Err(WorkspaceError::Fenced);
    }
    Ok(owner)
}

fn scoped_raw<'a>(checks: &'a [KvCheck], key: &[u8]) -> Result<Option<&'a [u8]>, WorkspaceError> {
    checks
        .iter()
        .find(|check| check.key.as_slice() == key)
        .map(|check| check.expected.as_deref())
        .ok_or(WorkspaceError::Fenced)
}

fn scoped_required<T: DeserializeOwned>(
    checks: &[KvCheck],
    key: &[u8],
) -> Result<T, WorkspaceError> {
    decode_open_value(
        scoped_raw(checks, key)?.ok_or(WorkspaceError::Fenced)?,
        SOURCE_MAX_BYTES,
    )
}

fn workspace_authenticates_mount_owner(
    workspace: &WorkspaceRecord,
    lease: &SnapshotLease,
    allow_recovery: bool,
    now: i64,
) -> bool {
    workspace.workspace_id == lease.workspace_id
        && match workspace.active_lease {
            Some(id) => id == lease.lease_id,
            // Logical expiry clears this pointer while retaining the exact
            // PWA/open/lease/native hold for typed recovery. It grants no writer.
            None => {
                allow_recovery && lease.state == LeaseState::Expired && lease.expires_at_ns <= now
            }
        }
}

impl<B: WorkspaceKvBackend> KvWorkspaceStore<B> {
    /// A routing read grants nothing. Every returned authority is authenticated
    /// by the subsequent single-version read and the operation's final CAS.
    async fn scoped_packed_mount_view(
        &self,
        workspace: WorkspaceId,
        requested_lease: LeaseId,
        allow_recovery: bool,
        extra_keys: &[Vec<u8>],
    ) -> Result<ScopedMountView, WorkspaceError> {
        let route_keys = vec![packed_current_key(workspace), packed_writer_key(workspace)];
        let (route, now) = self
            .backend
            .get_many_consistent_with_time_bounded(&route_keys, source_limits(route_keys.len()))
            .await?;
        if route.len() != route_keys.len() || now <= 0 {
            return Err(WorkspaceError::Fenced);
        }
        let binding = PackedLowerBindingRecord::decode(route[0].as_deref().ok_or(
            WorkspaceError::UnsupportedCapability("mandatory packed-v3 mount binding"),
        )?)?;
        let writer = PackedWriterAuthority::decode(
            route[1]
                .as_deref()
                .ok_or(WorkspaceError::UnsupportedCapability(
                    "mandatory packed-v3 writer authority",
                ))?,
            workspace,
        )?;
        if binding.workspace_id != workspace {
            return Err(WorkspaceError::Fenced);
        }
        let mut keys = vec![
            CONTROL_KEY.to_vec(),
            hot_workspace_key(workspace),
            hot_layer_key(binding.head_layer_id),
            hot_layer_key(binding.base_revision.layer_id),
            hot_lease_key(workspace, requested_lease),
            packed_current_key(workspace),
            packed_claim_key(workspace),
            packed_history_key(workspace, 1),
            packed_writer_key(workspace),
            open_v3_key(workspace),
            open_v3_recovery_key(workspace),
        ];
        if binding.binding.binding_version != 1 {
            keys.push(packed_history_key(
                workspace,
                binding.binding.binding_version,
            ));
        }
        let mut leases = vec![(workspace, requested_lease)];
        if let Some(owner) = &writer.owner {
            let id = owner.lease_identity().0;
            leases.push((workspace, id));
            let key = hot_lease_key(workspace, id);
            if !keys.contains(&key) {
                keys.push(key);
            }
        }
        for key in extra_keys {
            if !keys.contains(key) {
                keys.push(key.clone());
            }
        }
        let basis = self
            .read_topology_scope(
                &TopologyScope {
                    workspaces: vec![workspace],
                    layers: vec![binding.head_layer_id, binding.base_revision.layer_id],
                    leases,
                    extra_keys: keys,
                    workspace_heads: true,
                    ..TopologyScope::default()
                },
                source_limits(32),
            )
            .await?;
        let now = basis.now_ns;
        let mut checks = basis.checks;
        if scoped_raw(&checks, &route_keys[0])? != route[0].as_deref()
            || scoped_raw(&checks, &route_keys[1])? != route[1].as_deref()
        {
            return Err(WorkspaceError::Busy);
        }
        for lease in basis.state.leases.values() {
            let index_key = hot_lease_index_key(lease.lease_id);
            if !checks.iter().any(|check| check.key == index_key) {
                continue;
            }
            let indexed_workspace: WorkspaceId = scoped_required(&checks, &index_key)?;
            if indexed_workspace != lease.workspace_id {
                return Err(WorkspaceError::Fenced);
            }
        }
        let header = basis.state.header.ok_or(WorkspaceError::Fenced)?;
        if header.schema_version != WORKSPACE_SCHEMA_VERSION
            || header.volume_format != VOLUME_FORMAT
        {
            return Err(WorkspaceError::Fenced);
        }
        let workspace_row: WorkspaceRecord =
            scoped_required(&checks, &hot_workspace_key(workspace))?;
        let head: LayerRecord = scoped_required(&checks, &hot_layer_key(binding.head_layer_id))?;
        let base: LayerRecord =
            scoped_required(&checks, &hot_layer_key(binding.base_revision.layer_id))?;
        if workspace_row.workspace_id != workspace
            || workspace_row.state != WorkspaceState::Active
            || workspace_row.head_layer_id != binding.head_layer_id
            || workspace_row.head_epoch != binding.head_epoch
            || head.layer_id != binding.head_layer_id
            || head.state != LayerState::Writable
            || head.owner_workspace_id != Some(workspace)
            || head.depth != 2
            || head.parent_layer_id != Some(base.layer_id)
            || base.depth != 1
            || base.state != LayerState::Sealed
            || base.parent_layer_id.is_some()
        {
            return Err(WorkspaceError::Fenced);
        }
        let current = decode_packed_pair(
            workspace,
            &checks
                .iter()
                .find(|check| check.key == packed_current_key(workspace))
                .ok_or(WorkspaceError::Fenced)?
                .expected,
            &checks
                .iter()
                .find(|check| check.key == packed_claim_key(workspace))
                .ok_or(WorkspaceError::Fenced)?
                .expected,
            &checks
                .iter()
                .find(|check| {
                    check.key == packed_history_key(workspace, binding.binding.binding_version)
                })
                .ok_or(WorkspaceError::Fenced)?
                .expected,
        )?
        .ok_or(WorkspaceError::Fenced)?;
        if current != binding {
            return Err(WorkspaceError::Fenced);
        }
        let initial = PackedLowerBindingRecord::decode(
            scoped_raw(&checks, &packed_history_key(workspace, 1))?
                .ok_or(WorkspaceError::Fenced)?,
        )?;
        if initial.workspace_id != workspace || initial.binding.binding_version != 1 {
            return Err(WorkspaceError::Fenced);
        }
        let binding_guard = HeadGuard {
            workspace_id: workspace,
            expected_head_layer_id: head.layer_id,
            expected_head_epoch: workspace_row.head_epoch,
            lease_id: requested_lease,
            holder_generation: 1,
        };
        binding.validate_for_guard(&binding_guard, &base)?;
        if let Some(raw) = scoped_raw(&checks, &open_v3_recovery_key(workspace))? {
            let recovery: V3RecoveryRecord = decode_open_value(raw, OPEN_RECOVERY_MAX_BYTES)?;
            if recovery.workspace_id != workspace || (!allow_recovery && recovery.incomplete) {
                return Err(WorkspaceError::Fenced);
            }
        }
        let open = scoped_raw(&checks, &open_v3_key(workspace))?
            .map(|raw| decode_open_value::<V3OpenRecord>(raw, OPEN_RECORD_MAX_BYTES))
            .transpose()?;
        if let Some(open) = &open {
            validate_open_record(open, workspace)?;
            if !allow_recovery && (open.recovery_required || open.state != V3OpenState::Ready) {
                return Err(WorkspaceError::Fenced);
            }
        }
        let requested = scoped_raw(&checks, &hot_lease_key(workspace, requested_lease))?
            .map(|raw| decode_open_value::<SnapshotLease>(raw, SOURCE_MAX_BYTES))
            .transpose()?;
        let requested_index = scoped_raw(&checks, &hot_lease_index_key(requested_lease))?
            .map(decode::<WorkspaceId>)
            .transpose()?;
        if requested.as_ref().is_some_and(|lease| {
            lease.workspace_id != workspace || lease.lease_id != requested_lease
        }) || requested_index != requested.as_ref().map(|_| workspace)
        {
            return Err(WorkspaceError::Fenced);
        }
        let previous = match &writer.owner {
            None => None,
            Some(owner) => {
                let (lease_id, generation) = owner.lease_identity();
                let lease: SnapshotLease =
                    scoped_required(&checks, &hot_lease_key(workspace, lease_id))?;
                let indexed_workspace: WorkspaceId =
                    scoped_required(&checks, &hot_lease_index_key(lease_id))?;
                if lease.workspace_id != workspace
                    || lease.lease_id != lease_id
                    || indexed_workspace != workspace
                    || !workspace_authenticates_mount_owner(
                        &workspace_row,
                        &lease,
                        allow_recovery,
                        now,
                    )
                    || lease.holder_generation != generation
                    || !lease.writable
                    || lease.base_revision != binding.base_revision
                {
                    return Err(WorkspaceError::Fenced);
                }
                Some(lease)
            }
        };
        if let Some(PackedWriterOwner::Mounted {
            open_owner,
            open_generation,
            ..
        }) = &writer.owner
        {
            let previous = previous.as_ref().ok_or(WorkspaceError::Fenced)?;
            let open = open.as_ref().ok_or(WorkspaceError::Fenced)?;
            if open.owner_id != *open_owner
                || open.generation != *open_generation
                || open.expires_at_ns != previous.expires_at_ns
                || (!allow_recovery && previous.state != LeaseState::Active)
                || (allow_recovery
                    && !matches!(previous.state, LeaseState::Active | LeaseState::Expired))
            {
                return Err(WorkspaceError::Fenced);
            }
        }
        if let Some(PackedWriterOwner::Administrative {
            open_owner,
            open_generation,
            recovering,
            ..
        }) = &writer.owner
        {
            let previous = previous.as_ref().ok_or(WorkspaceError::Fenced)?;
            let open = open.as_ref().ok_or(WorkspaceError::Fenced)?;
            if !allow_recovery
                || !*recovering
                || open.owner_id != *open_owner
                || open.generation != *open_generation
                || open.expires_at_ns != previous.expires_at_ns
                || open.state != V3OpenState::Recovering
                || !open.recovery_required
                || !matches!(previous.state, LeaseState::Active | LeaseState::Expired)
            {
                return Err(WorkspaceError::Fenced);
            }
            let recovery: V3RecoveryRecord =
                scoped_required(&checks, &open_v3_recovery_key(workspace))?;
            if recovery.workspace_id != workspace || !recovery.incomplete {
                return Err(WorkspaceError::Fenced);
            }
        }
        let (registry_owner, registry_checks) =
            self.packed_registry_publication_checks(&binding).await?;
        self.merge_borrowed_checks(&mut checks, registry_checks)?;
        let total = checks.iter().try_fold(0usize, |sum, check| {
            sum.checked_add(check.key.len())
                .and_then(|sum| sum.checked_add(check.expected.as_ref().map_or(0, Vec::len)))
        });
        if checks.len() > 32 || total.is_none_or(|bytes| bytes > SOURCE_MAX_BYTES) {
            return Err(WorkspaceError::InvalidReadPlan(
                "packed mount authority exceeds the source plan".into(),
            ));
        }
        Ok(ScopedMountView {
            binding,
            writer,
            requested_lease: requested,
            open,
            checks,
            now,
            _registry_owner: registry_owner,
        })
    }

    // The receipt covers the final packet, including all native derived writes
    // and the topology generation. A lost reply never replays the mutation.
    async fn commit_packed_mount_packet(
        &self,
        checks: Vec<KvCheck>,
        writes: Vec<KvWrite>,
        deadline: i64,
    ) -> Result<(), WorkspaceError> {
        let packet = self
            .prepare_topology_packet(checks, writes, Some(deadline))
            .await?;
        self.commit_prepared_packed_mount_packet(&packet).await
    }

    async fn commit_prepared_packed_mount_packet(
        &self,
        packet: &PreparedTopologyPacket,
    ) -> Result<(), WorkspaceError> {
        if self.try_commit_prepared_packed_mount_packet(packet).await? {
            Ok(())
        } else {
            Err(WorkspaceError::Fenced)
        }
    }

    async fn try_commit_prepared_packed_mount_packet(
        &self,
        packet: &PreparedTopologyPacket,
    ) -> Result<bool, WorkspaceError> {
        let mut bytes = 0usize;
        if packet.checks.len() > 64 || packet.writes.len() > 64 {
            return Err(WorkspaceError::Fenced);
        }
        for row in &packet.checks {
            bytes = bytes
                .checked_add(row.key.len())
                .and_then(|bytes| bytes.checked_add(row.expected.as_ref().map_or(0, Vec::len)))
                .ok_or(WorkspaceError::Fenced)?;
        }
        for row in &packet.writes {
            let (key, len) = match row {
                KvWrite::Put { key, value } => (key, value.len()),
                KvWrite::Delete { key } => (key, 0),
            };
            if !packet.checks.iter().any(|check| check.key == *key) {
                return Err(WorkspaceError::Fenced);
            }
            bytes = bytes
                .checked_add(key.len())
                .and_then(|bytes| bytes.checked_add(len))
                .ok_or(WorkspaceError::Fenced)?;
        }
        // Confirmation owns a successor copy as well as the attempted packet.
        if bytes > SOURCE_ADMISSION_BYTES as usize / 2 {
            return Err(WorkspaceError::Fenced);
        }
        match self.commit_prepared_topology_packet(packet).await {
            Ok(true) => Ok(true),
            Ok(false) => Ok(false),
            Err(original) => match self.confirm_prepared_topology_packet(packet).await {
                Ok(true) => Ok(true),
                _ => Err(original),
            },
        }
    }

    /// Cancellation never replays the attempted grant. If its CAS committed,
    /// the durable joint owner remains until typed recovery retires it; expiry
    /// alone cannot prove that the original writer was never attached.
    pub(crate) async fn grant_packed_mounted_session(
        self: &Arc<Self>,
        request: PackedMountGrantRequest,
    ) -> Result<Arc<PackedMountedLease<B>>, WorkspaceError> {
        let store = self.clone();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let result = store.grant_packed_mounted_session_owned(request).await;
            // This also covers successful send followed by receiver cancellation
            // before polling: dropping the authority cannot retire durable rows.
            let _ = sender.send(result);
        });
        receiver
            .await
            .map_err(|_| WorkspaceError::Backend("owned packed mount grant stopped".into()))?
    }

    async fn grant_packed_mounted_session_owned(
        self: &Arc<Self>,
        request: PackedMountGrantRequest,
    ) -> Result<Arc<PackedMountedLease<B>>, WorkspaceError> {
        let open_owner = mount_owner(&request)?;
        let budget = self.packed_reader_pin_budget.get().cloned().ok_or(
            WorkspaceError::UnsupportedCapability("canonical packed mount budget"),
        )?;
        let owner = budget
            .admit(&[(V3BudgetPool::Metadata, SOURCE_ADMISSION_BYTES)])
            .map_err(|error| WorkspaceError::InvalidReadPlan(error.to_string()))?;
        let view = self
            .scoped_packed_mount_view(request.workspace_id, request.lease_id, false, &[])
            .await?;
        let mut workspace: WorkspaceRecord =
            scoped_required(&view.checks, &hot_workspace_key(request.workspace_id))?;
        // Expiry never proves that an attached original writer drained. A
        // retained owner requires the typed recovery path to retire it first.
        if view.writer.owner.is_some() || workspace.active_lease.is_some() {
            return Err(WorkspaceError::Fenced);
        }
        if view.requested_lease.is_some()
            || view
                .open
                .as_ref()
                .is_some_and(|open| open.expires_at_ns > view.now)
        {
            return Err(WorkspaceError::Busy);
        }
        let expiry = checked_expiry(view.now, request.ttl_ns)?;
        let generation = view.open.as_ref().map_or(Ok(1), |open| {
            open.generation.checked_add(1).ok_or(WorkspaceError::Fenced)
        })?;
        let lease = SnapshotLease {
            lease_id: request.lease_id,
            workspace_id: request.workspace_id,
            base_revision: view.binding.base_revision.clone(),
            holder_generation: request.holder_generation,
            writable: true,
            state: LeaseState::Active,
            expires_at_ns: expiry,
            created_at_ns: view.now,
            updated_at_ns: view.now,
        };
        let open = V3OpenRecord {
            workspace_id: request.workspace_id,
            owner_id: open_owner.clone(),
            generation,
            expires_at_ns: expiry,
            state: V3OpenState::Ready,
            recovery_required: false,
        };
        let next = view.writer.successor(Some(PackedWriterOwner::Mounted {
            lease_id: request.lease_id,
            holder_generation: request.holder_generation,
            open_owner: open_owner.clone(),
            open_generation: generation,
        }))?;
        workspace.active_lease = Some(request.lease_id);
        workspace.updated_at_ns = view.now;
        let writes = vec![
            put(hot_workspace_key(request.workspace_id), &workspace)?,
            put(
                hot_lease_key(request.workspace_id, request.lease_id),
                &lease,
            )?,
            put(hot_lease_index_key(request.lease_id), &request.workspace_id)?,
            put(open_v3_key(request.workspace_id), &open)?,
            KvWrite::Put {
                key: packed_writer_key(request.workspace_id),
                value: next.encode()?,
            },
        ];
        self.commit_packed_mount_packet(view.checks, writes, expiry)
            .await?;
        let guard = HeadGuard {
            workspace_id: request.workspace_id,
            expected_head_layer_id: view.binding.head_layer_id,
            expected_head_epoch: view.binding.head_epoch,
            lease_id: request.lease_id,
            holder_generation: request.holder_generation,
        };
        Ok(Arc::new(PackedMountedLease {
            store: self.clone(),
            reference: PackedReleasedMountReference {
                guard: guard.clone(),
                mount_uid: request.mount_uid,
                pod_uid: request.pod_uid,
            },
            guard,
            binding: view.binding,
            lease,
            renewals: Arc::new(PackedMountRenewalGate::default()),
            writer_incarnation: next.incarnation,
            open_owner,
            open_generation: generation,
            budget,
            _owner: owner,
        }))
    }
}

impl<B: WorkspaceKvBackend> PackedMountedLease<B> {
    pub(crate) fn view(&self) -> ViewContext {
        ViewContext {
            workspace_id: self.guard.workspace_id,
            head_layer_id: self.guard.expected_head_layer_id,
            head_epoch: self.guard.expected_head_epoch,
            lease_id: self.guard.lease_id,
            holder_generation: self.guard.holder_generation,
        }
    }
    pub(crate) fn reference(&self) -> PackedReleasedMountReference {
        self.reference.clone()
    }
    pub(crate) fn budget(&self) -> &Arc<V3MountBudget> {
        &self.budget
    }
    pub(crate) fn lease(&self) -> SnapshotLease {
        self.lease.clone()
    }

    /// Close before physical clean-proof construction. The proof-consuming
    /// release repeats this gate so cancellation cannot leave a late renewal.
    pub(crate) async fn close_renewals_and_drain(&self) -> Result<(), WorkspaceError> {
        self.renewals.close_and_drain().await
    }

    pub(crate) async fn release_original(
        self: &Arc<Self>,
        proof: crate::workspace_overlay::packed_shutdown::VerifiedCleanPackedShutdown,
    ) -> Result<(), WorkspaceError> {
        self.store
            .release_clean_packed_mounted_session(proof, self.clone())
            .await
    }

    pub(in crate::workspace_overlay::stores::kv_store::packed_admin) fn original_release_successor(
        &self,
        store: &Arc<KvWorkspaceStore<B>>,
        reference: &PackedReleasedMountReference,
        rows: PackedOriginalReleaseRows<'_>,
        now: i64,
        confirming_existing_receipt: bool,
    ) -> Result<(V3OpenRecord, PackedWriterAuthority), WorkspaceError> {
        let PackedOriginalReleaseRows {
            writer,
            lease,
            open,
        } = rows;
        let expected = PackedWriterOwner::Mounted {
            lease_id: self.guard.lease_id,
            holder_generation: self.guard.holder_generation,
            open_owner: self.open_owner.clone(),
            open_generation: self.open_generation,
        };
        if !self.renewals.closed_and_drained()?
            || !Arc::ptr_eq(store, &self.store)
            || reference != &self.reference
            || !store
                .packed_reader_pin_budget
                .get()
                .is_some_and(|budget| Arc::ptr_eq(budget, &self.budget))
            || lease.lease_id != self.guard.lease_id
            || lease.workspace_id != self.guard.workspace_id
            || lease.holder_generation != self.guard.holder_generation
            || !lease.writable
            || lease.base_revision != self.binding.base_revision
            || open.owner_id != self.open_owner
            || open.generation != self.open_generation
            || open.workspace_id != self.guard.workspace_id
            || open.state != V3OpenState::Ready
            || open.recovery_required
            || writer.workspace_id != self.guard.workspace_id
        {
            return Err(WorkspaceError::Fenced);
        }
        if confirming_existing_receipt {
            if lease.state != LeaseState::Released
                || open.expires_at_ns != lease.updated_at_ns
                || open.expires_at_ns > now
                || writer.owner.is_some()
                || writer.incarnation
                    != self
                        .writer_incarnation
                        .checked_add(1)
                        .ok_or(WorkspaceError::Fenced)?
            {
                return Err(WorkspaceError::Fenced);
            }
            return Ok((open.clone(), writer.clone()));
        }
        if lease.state != LeaseState::Active
            || lease.expires_at_ns <= now
            || open.expires_at_ns != lease.expires_at_ns
            || writer.incarnation != self.writer_incarnation
            || writer.owner.as_ref() != Some(&expected)
        {
            return Err(WorkspaceError::Fenced);
        }
        let mut closed = open.clone();
        closed.expires_at_ns = now;
        Ok((closed, writer.successor(None)?))
    }

    fn authenticate_view(
        &self,
        view: &ScopedMountView,
    ) -> Result<(SnapshotLease, V3OpenRecord), WorkspaceError> {
        let lease = view.requested_lease.clone().ok_or(WorkspaceError::Fenced)?;
        let open = view.open.clone().ok_or(WorkspaceError::Fenced)?;
        let expected = PackedWriterOwner::Mounted {
            lease_id: self.guard.lease_id,
            holder_generation: self.guard.holder_generation,
            open_owner: self.open_owner.clone(),
            open_generation: self.open_generation,
        };
        if !self
            .store
            .packed_reader_pin_budget
            .get()
            .is_some_and(|budget| Arc::ptr_eq(budget, &self.budget))
            || view.binding != self.binding
            || view.writer.incarnation != self.writer_incarnation
            || view.writer.owner.as_ref() != Some(&expected)
            || lease.lease_id != self.guard.lease_id
            || lease.workspace_id != self.guard.workspace_id
            || lease.holder_generation != self.guard.holder_generation
            || !lease.writable
            || lease.state != LeaseState::Active
            || lease.expires_at_ns <= view.now
            || lease.base_revision != self.binding.base_revision
            || open.owner_id != self.open_owner
            || open.generation != self.open_generation
            || open.workspace_id != self.guard.workspace_id
            || open.state != V3OpenState::Ready
            || open.recovery_required
            || open.expires_at_ns != lease.expires_at_ns
        {
            return Err(WorkspaceError::Fenced);
        }
        Ok((lease, open))
    }

    pub(crate) async fn renew(self: &Arc<Self>, ttl_ns: u64) -> Result<(), WorkspaceError> {
        // Count before spawning, on the same lock that closes admission. The
        // owned permit lives through the actual CAS and response delivery even
        // if the receiver or outer heartbeat future is cancelled meanwhile.
        let renewal = self.renewals.enter()?;
        let authority = self.clone();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let _renewal = renewal;
            let _ = sender.send(authority.renew_owned(ttl_ns).await);
        });
        receiver
            .await
            .map_err(|_| WorkspaceError::Backend("owned packed mount renewal stopped".into()))?
    }

    async fn renew_owned(&self, ttl_ns: u64) -> Result<(), WorkspaceError> {
        if ttl_ns == 0 || ttl_ns > 15 * 60 * 1_000_000_000 {
            return Err(WorkspaceError::Fenced);
        }
        let _owner = self
            .budget
            .admit(&[(V3BudgetPool::Metadata, SOURCE_ADMISSION_BYTES)])
            .map_err(|error| WorkspaceError::InvalidReadPlan(error.to_string()))?;
        // This future belongs to the admitted renewal driver. Cancellation
        // of its receiver cannot release the gate before CAS completion.
        // Queue fairly with local commits before capturing the exact head.
        let _mutation_guard = self.store.packed_mutation_gate.lock().await;
        // A normal head-sequence mutation can invalidate this exact snapshot
        // without replacing the mounted owner. Only definite preparation Busy
        // or CAS false may rebuild the complete packet under the fixed cap.
        for _ in 0..CAS_MAX_RETRIES {
            let prepared = async {
                let view = self
                    .store
                    .scoped_packed_mount_view(
                        self.guard.workspace_id,
                        self.guard.lease_id,
                        false,
                        &[],
                    )
                    .await?;
                let (mut lease, mut open) = self.authenticate_view(&view)?;
                let deadline = lease.expires_at_ns;
                let expiry = checked_expiry(view.now, ttl_ns)?;
                lease.expires_at_ns = expiry;
                lease.updated_at_ns = view.now;
                open.expires_at_ns = expiry;
                let writes = vec![
                    put(hot_lease_key(lease.workspace_id, lease.lease_id), &lease)?,
                    put(open_v3_key(self.guard.workspace_id), &open)?,
                ];
                // The scoped view authenticates the immutable catalog header,
                // but a lease/open heartbeat only mutates its own entity
                // records. Keep that header read as validation data without
                // turning it into a Redis/TiKV CAS authority; otherwise every
                // mounted workspace renewal contends on the global CONTROL
                // key despite PR #141's entity-key contract.
                let checks = view
                    .checks
                    .into_iter()
                    .filter(|check| check.key.as_slice() != CONTROL_KEY)
                    .collect();
                self.store
                    .prepare_topology_packet(checks, writes, Some(deadline.min(expiry)))
                    .await
            }
            .await;
            let packet = match prepared {
                Err(WorkspaceError::Busy) => {
                    tokio::task::yield_now().await;
                    continue;
                }
                other => other?,
            };
            // The shared helper confirms the same successor after any submitted
            // error. Such an error, including Busy, must never enter this loop.
            match self
                .store
                .try_commit_prepared_packed_mount_packet(&packet)
                .await
            {
                Ok(true) => return Ok(()),
                Ok(false) => tokio::task::yield_now().await,
                Err(error) => return Err(error),
            }
        }
        Err(WorkspaceError::Busy)
    }

    /// Only lifecycle can supply this one-use marker, after proving that
    /// it never attempted FUSE attachment for this exact authority and budget.
    /// A cancelled or ambiguous grant without that marker remains for recovery.
    pub(crate) async fn abort_unattached(
        self: &Arc<Self>,
        marker: crate::workspace_overlay::lifecycle::PackedNeverAttached,
    ) -> Result<(), WorkspaceError> {
        let authority = self.clone();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let result = async {
                authority.close_renewals_and_drain().await?;
                if !marker.matches(&authority.reference, &authority.budget) {
                    return Err(WorkspaceError::Fenced);
                }
                authority.abort_unattached_owned().await
            }
            .await;
            let _ = sender.send(result);
        });
        let received = receiver.await;
        task.await.map_err(|_| {
            WorkspaceError::Backend("owned packed mount startup retirement stopped".into())
        })?;
        received.map_err(|_| {
            WorkspaceError::Backend(
                "owned packed mount startup retirement result unavailable".into(),
            )
        })?
    }

    async fn abort_unattached_owned(&self) -> Result<(), WorkspaceError> {
        if !self.renewals.closed_and_drained()? {
            return Err(WorkspaceError::Fenced);
        }
        let _owner = self
            .budget
            .admit(&[(V3BudgetPool::Metadata, SOURCE_ADMISSION_BYTES)])
            .map_err(|error| WorkspaceError::InvalidReadPlan(error.to_string()))?;
        let view = self
            .store
            .scoped_packed_mount_view(self.guard.workspace_id, self.guard.lease_id, false, &[])
            .await?;
        let (mut lease, mut open) = self.authenticate_view(&view)?;
        let deadline = lease.expires_at_ns;
        lease.state = LeaseState::Released;
        lease.updated_at_ns = view.now;
        open.expires_at_ns = view.now;
        let idle = view.writer.successor(None)?;
        let mut workspace: WorkspaceRecord =
            scoped_required(&view.checks, &hot_workspace_key(self.guard.workspace_id))?;
        if workspace.active_lease != Some(lease.lease_id) {
            return Err(WorkspaceError::Fenced);
        }
        workspace.active_lease = None;
        workspace.updated_at_ns = view.now;
        // No PCR is constructed: this is a proven never-attached retirement,
        // not an original-VFS physical drain or clean publication authority.
        let writes = vec![
            put(hot_workspace_key(self.guard.workspace_id), &workspace)?,
            put(hot_lease_key(lease.workspace_id, lease.lease_id), &lease)?,
            put(open_v3_key(self.guard.workspace_id), &open)?,
            KvWrite::Put {
                key: packed_writer_key(self.guard.workspace_id),
                value: idle.encode()?,
            },
        ];
        self.store
            .commit_packed_mount_packet(view.checks, writes, deadline)
            .await
    }
}

#[cfg(test)]
mod renewal_gate_tests {
    use super::*;

    #[tokio::test]
    async fn closing_rejects_new_workers_and_waits_for_the_admitted_worker() {
        let gate = Arc::new(PackedMountRenewalGate::default());
        let permit = gate.enter().unwrap();
        gate.close_admission().unwrap();
        assert!(gate.enter().is_err());
        assert!(!gate.closed_and_drained().unwrap());
        drop(permit);
        gate.wait_for_drain().await.unwrap();
        assert!(gate.closed_and_drained().unwrap());
    }

    #[tokio::test]
    async fn cancelled_receiver_keeps_the_owned_worker_visible_to_drain() {
        let gate = Arc::new(PackedMountRenewalGate::default());
        let permit = gate.enter().unwrap();
        let (release, wait) = tokio::sync::oneshot::channel();
        let (result, caller) = tokio::sync::oneshot::channel::<()>();
        let worker = tokio::spawn(async move {
            let _renewal = permit;
            let _ = wait.await;
            let _ = result.send(());
        });
        drop(caller);
        gate.close_admission().unwrap();
        assert!(!gate.closed_and_drained().unwrap());
        assert!(gate.enter().is_err());
        release.send(()).unwrap();
        gate.wait_for_drain().await.unwrap();
        worker.await.unwrap();
        assert!(gate.closed_and_drained().unwrap());
    }

    #[tokio::test]
    async fn terminal_worker_wakes_every_original_shutdown_waiter() {
        let gate = Arc::new(PackedMountRenewalGate::default());
        let permit = gate.enter().unwrap();
        gate.close_admission().unwrap();
        let mut first = Box::pin(gate.wait_for_drain());
        let mut second = Box::pin(gate.wait_for_drain());
        tokio::select! {
            biased;
            _ = &mut first => panic!("live renewal must block drain"),
            _ = &mut second => panic!("live renewal must block every drain"),
            _ = tokio::task::yield_now() => {}
        }
        drop(permit);
        tokio::join!(async { first.await.unwrap() }, async {
            second.await.unwrap()
        });
    }
}
