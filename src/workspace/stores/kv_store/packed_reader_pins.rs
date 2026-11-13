//! Persistent retention leases for authenticated packed lower generations.
//! A pin retains bytes; it does not certify a graph or freeze native upper data.

use super::packed_native_freeze::PackedNativeRecoveryReadFence;
use super::*;
use crate::workspace::packed::snapshot::{
    SnapshotBudgetPool, SnapshotMountBudget, SnapshotOwnedPermit,
};
use crate::workspace::stores::kv_backend::KvReadLimits;
use sha2::{Digest, Sha256};
use uuid::Uuid;

mod entity_owners;
pub use entity_owners::PackedEntityLowerOwner;

const PIN_PREFIX: &[u8] = b"packed/reader-slot/";
const ACTIVE_PREFIX: &[u8] = b"packed/reader-active/";
const FEATURE_KEY: &[u8] = b"packed/reader-feature";
const COUNT_KEY: &[u8] = b"packed/reader-active-count";
const FEATURE: &[u8] = b"PPR3";
pub const PACKED_READER_SLOT_COUNT: usize = 256;
const RECORD_BYTES: usize = 12 << 10;
const BINDING_BYTES: usize = 8192;
const OWNER_BYTES: usize = 256;
const OPERATION_BYTES: u64 = 1 << 20;
const SCAN_BYTES: u64 = 24 << 20;
pub const MAX_PACKED_READER_TTL_NS: u64 = 15 * 60 * 1_000_000_000;
pub const MAX_PACKED_READER_GRACE_NS: u64 = 15 * 60 * 1_000_000_000;

/// Slots are reused only with a strictly increasing generation. There are at
/// most 256 main rows plus 256 active mirrors, including all terminal records.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum PackedReaderPinState {
    Active = 0,
    Released = 1,
    Reaped = 2,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
enum PinOperation {
    Acquire = 0,
    Renew = 1,
    Release = 2,
    Reap = 3,
}

/// Use the entire token. UUID or slot alone is never an authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PackedReaderPin {
    pub slot: u16,
    pub slot_generation: u64,
    pub pin_id: Uuid,
    pub owner_id: String,
    pub holder_generation: u64,
    pub revision: u64,
    pub state: PackedReaderPinState,
    pub acquired_guard: HeadGuard,
    pub binding: PackedLowerBindingRecord,
    pub created_at_ns: i64,
    pub expires_at_ns: i64,
    pub gc_grace_ns: u64,
    original_ttl_ns: u64,
    operation_id: Uuid,
    operation: PinOperation,
    operation_parent_revision: u64,
    operation_ttl_ns: u64,
}

/// A new request claims a missing slot (generation 0) or an observed terminal
/// slot. Delayed retries cannot claim a newer incarnation of the same slot.
#[derive(Clone, Debug)]
pub struct AcquirePackedReaderPin {
    pub slot: u16,
    pub expected_slot_generation: u64,
    pub pin_id: Uuid,
    pub owner_id: String,
    pub holder_generation: u64,
    pub ttl_ns: u64,
    pub gc_grace_ns: u64,
    pub guard: HeadGuard,
    pub expected_layers: [LayerRecord; 2],
    pub expected_binding: PackedLowerBindingRecord,
}

/// Successful results keep their metadata admission until the owner drops.
#[derive(Debug)]
pub struct OwnedPackedReaderPin<T> {
    value: T,
    _permit: SnapshotOwnedPermit,
}
impl<T> std::ops::Deref for OwnedPackedReaderPin<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.value
    }
}

/// Exact checks and their admission stay alive through destructive native CAS.
/// Full binding refs are also exposed for the future typed object collector.
pub struct PackedReaderPinRoots {
    pub native_roots: BTreeSet<LayerId>,
    pub bindings: Vec<PackedLowerBindingRecord>,
    pub(super) checks: Vec<KvCheck>,
    pub(super) _permit: Option<SnapshotOwnedPermit>,
}

fn pin_error(message: impl std::fmt::Display) -> WorkspaceError {
    WorkspaceError::CorruptMetadata(format!("packed reader pin: {message}"))
}
fn pin_key(slot: u16) -> Vec<u8> {
    format!("packed/reader-slot/{slot:04x}").into_bytes()
}
fn active_key(slot: u16) -> Vec<u8> {
    format!("packed/reader-active/{slot:04x}").into_bytes()
}
fn check_slot(slot: u16) -> Result<(), WorkspaceError> {
    if usize::from(slot) >= PACKED_READER_SLOT_COUNT {
        return Err(WorkspaceError::InvalidReadPlan(
            "reader slot exceeds bound".into(),
        ));
    }
    Ok(())
}
fn check_ttl(ttl: u64) -> Result<(), WorkspaceError> {
    if ttl == 0 || ttl > MAX_PACKED_READER_TTL_NS {
        return Err(WorkspaceError::InvalidReadPlan(
            "reader TTL exceeds bound".into(),
        ));
    }
    Ok(())
}
fn count(raw: &Option<Vec<u8>>) -> Result<u64, WorkspaceError> {
    let value = raw
        .as_deref()
        .map(|bytes| {
            <[u8; 8]>::try_from(bytes)
                .map(u64::from_le_bytes)
                .map_err(pin_error)
        })
        .transpose()?
        .unwrap_or(0);
    if value > PACKED_READER_SLOT_COUNT as u64 {
        return Err(pin_error("active count exceeds bound"));
    }
    Ok(value)
}
fn check_feature(
    raw: &Option<Vec<u8>>,
    count_raw: &Option<Vec<u8>>,
) -> Result<bool, WorkspaceError> {
    match raw.as_deref() {
        None if count_raw.is_none() => Ok(false),
        Some(FEATURE) if count_raw.is_some() => Ok(true),
        _ => Err(pin_error("feature/count disagree")),
    }
}
fn checks_for(keys: &[Vec<u8>], values: &[Option<Vec<u8>>]) -> Vec<KvCheck> {
    keys.iter()
        .cloned()
        .zip(values.iter().cloned())
        .map(|(key, expected)| KvCheck { key, expected })
        .collect()
}
/// Separate bounded reads may observe a root-generation change. Never mix
/// conflicting snapshots in a CAS; retry both reads before installing a pin.
fn merge_reader_authority_checks(checks: &mut Vec<KvCheck>, authority: &[KvCheck]) -> bool {
    for check in authority {
        if let Some(previous) = checks.iter().find(|previous| previous.key == check.key) {
            if previous.expected != check.expected {
                return false;
            }
        } else {
            checks.push(check.clone());
        }
    }
    true
}
fn retain<T>(
    value: T,
    mut permit: SnapshotOwnedPermit,
    bytes: u64,
) -> Result<OwnedPackedReaderPin<T>, WorkspaceError> {
    permit
        .shrink(SnapshotBudgetPool::Metadata, bytes)
        .map_err(pin_error)?;
    Ok(OwnedPackedReaderPin {
        value,
        _permit: permit,
    })
}

impl PackedReaderPin {
    fn validate(&self) -> Result<(), WorkspaceError> {
        check_slot(self.slot)?;
        check_ttl(self.original_ttl_ns)?;
        if self.slot_generation == 0
            || self.revision == 0
            || self.pin_id.is_nil()
            || self.operation_id.is_nil()
            || self.owner_id.is_empty()
            || self.owner_id.len() > OWNER_BYTES
            || self.holder_generation == 0
            || self.expires_at_ns <= self.created_at_ns
            || self.gc_grace_ns > MAX_PACKED_READER_GRACE_NS
            || self.acquired_guard.workspace_id != self.binding.workspace_id
            || self.acquired_guard.expected_head_layer_id != self.binding.head_layer_id
            || self.acquired_guard.expected_head_epoch != self.binding.head_epoch
            || self.operation_parent_revision.checked_add(1) != Some(self.revision)
        {
            return Err(pin_error("invalid record identity or revision"));
        }
        match (self.state, self.operation) {
            (PackedReaderPinState::Active, PinOperation::Acquire)
                if self.revision == 1
                    && self.operation_id == self.pin_id
                    && self.operation_ttl_ns == self.original_ttl_ns => {}
            (PackedReaderPinState::Active, PinOperation::Renew) if self.revision > 1 => {
                check_ttl(self.operation_ttl_ns)?;
            }
            (PackedReaderPinState::Released, PinOperation::Release)
            | (PackedReaderPinState::Reaped, PinOperation::Reap)
                if self.revision > 1 && self.operation_ttl_ns == 0 => {}
            _ => return Err(pin_error("state/operation disagree")),
        }
        self.binding.encode()?;
        self.expires_at_ns
            .checked_add(i64::try_from(self.gc_grace_ns).map_err(pin_error)?)
            .ok_or_else(|| pin_error("expiry/grace overflows"))?;
        Ok(())
    }
    fn same_identity(&self, other: &Self) -> bool {
        self.slot == other.slot
            && self.slot_generation == other.slot_generation
            && self.pin_id == other.pin_id
            && self.owner_id == other.owner_id
            && self.holder_generation == other.holder_generation
            && self.acquired_guard == other.acquired_guard
            && self.binding == other.binding
            && self.created_at_ns == other.created_at_ns
            && self.gc_grace_ns == other.gc_grace_ns
            && self.original_ttl_ns == other.original_ttl_ns
    }
    fn matches_acquire(&self, request: &AcquirePackedReaderPin) -> bool {
        self.slot == request.slot
            && request.expected_slot_generation.checked_add(1) == Some(self.slot_generation)
            && self.pin_id == request.pin_id
            && self.owner_id == request.owner_id
            && self.holder_generation == request.holder_generation
            && self.acquired_guard == request.guard
            && self.binding == request.expected_binding
            && self.original_ttl_ns == request.ttl_ns
            && self.gc_grace_ns == request.gc_grace_ns
    }
    fn exact_retry(
        &self,
        expected: &Self,
        operation_id: Uuid,
        operation: PinOperation,
        ttl_ns: u64,
    ) -> bool {
        self.same_identity(expected)
            && self.revision == expected.revision.saturating_add(1)
            && self.operation_parent_revision == expected.revision
            && self.operation_id == operation_id
            && self.operation == operation
            && self.operation_ttl_ns == ttl_ns
    }
    fn encode(&self) -> Result<Vec<u8>, WorkspaceError> {
        self.validate()?;
        let binding = self.binding.encode()?;
        if binding.len() > BINDING_BYTES {
            return Err(pin_error("binding exceeds bound"));
        }
        let mut bytes = Vec::with_capacity(256 + binding.len() + self.owner_id.len());
        bytes.extend_from_slice(FEATURE);
        bytes.extend_from_slice(&1u32.to_le_bytes());
        bytes.extend_from_slice(&self.slot.to_le_bytes());
        bytes.push(self.state as u8);
        bytes.push(self.operation as u8);
        for value in [
            self.slot_generation,
            self.revision,
            self.holder_generation,
            self.created_at_ns as u64,
            self.expires_at_ns as u64,
            self.gc_grace_ns,
            self.original_ttl_ns,
            self.operation_parent_revision,
            self.operation_ttl_ns,
            self.acquired_guard.holder_generation,
        ] {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        bytes.extend_from_slice(self.pin_id.as_bytes());
        bytes.extend_from_slice(self.operation_id.as_bytes());
        bytes.extend_from_slice(self.acquired_guard.lease_id.as_bytes());
        bytes.extend_from_slice(&(self.owner_id.len() as u32).to_le_bytes());
        bytes.extend_from_slice(self.owner_id.as_bytes());
        bytes.extend_from_slice(&(binding.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&binding);
        if bytes.len() + 32 > RECORD_BYTES {
            return Err(pin_error("record exceeds bound"));
        }
        let digest = Sha256::digest(&bytes);
        bytes.extend_from_slice(&digest);
        Ok(bytes)
    }
    fn decode(bytes: &[u8]) -> Result<Self, WorkspaceError> {
        if bytes.len() < 176 || bytes.len() > RECORD_BYTES {
            return Err(pin_error("record length exceeds bound"));
        }
        let (body, digest) = bytes.split_at(bytes.len() - 32);
        if Sha256::digest(body).as_slice() != digest {
            return Err(pin_error("record digest mismatch"));
        }
        let mut c = PinCursor {
            bytes: body,
            offset: 0,
        };
        if c.take::<4>()? != *FEATURE || u32::from_le_bytes(c.take()?) != 1 {
            return Err(pin_error("unsupported codec"));
        }
        let slot = u16::from_le_bytes(c.take()?);
        let state = match c.take::<1>()?[0] {
            0 => PackedReaderPinState::Active,
            1 => PackedReaderPinState::Released,
            2 => PackedReaderPinState::Reaped,
            _ => return Err(pin_error("unknown state")),
        };
        let operation = match c.take::<1>()?[0] {
            0 => PinOperation::Acquire,
            1 => PinOperation::Renew,
            2 => PinOperation::Release,
            3 => PinOperation::Reap,
            _ => return Err(pin_error("unknown operation")),
        };
        let slot_generation = c.u64()?;
        let revision = c.u64()?;
        let holder_generation = c.u64()?;
        let created_at_ns = c.u64()? as i64;
        let expires_at_ns = c.u64()? as i64;
        let gc_grace_ns = c.u64()?;
        let original_ttl_ns = c.u64()?;
        let operation_parent_revision = c.u64()?;
        let operation_ttl_ns = c.u64()?;
        let acquiring_holder = c.u64()?;
        let pin_id = Uuid::from_bytes(c.take()?);
        let operation_id = Uuid::from_bytes(c.take()?);
        let acquiring_lease = LeaseId::from_uuid(Uuid::from_bytes(c.take()?));
        let owner_id = std::str::from_utf8(c.field(OWNER_BYTES)?)
            .map_err(pin_error)?
            .to_owned();
        let binding = PackedLowerBindingRecord::decode(c.field(BINDING_BYTES)?)?;
        if c.offset != body.len() {
            return Err(pin_error("trailing fields"));
        }
        let acquired_guard = HeadGuard {
            workspace_id: binding.workspace_id,
            expected_head_layer_id: binding.head_layer_id,
            expected_head_epoch: binding.head_epoch,
            lease_id: acquiring_lease,
            holder_generation: acquiring_holder,
        };
        let value = Self {
            slot,
            slot_generation,
            pin_id,
            owner_id,
            holder_generation,
            revision,
            state,
            acquired_guard,
            binding,
            created_at_ns,
            expires_at_ns,
            gc_grace_ns,
            original_ttl_ns,
            operation_id,
            operation,
            operation_parent_revision,
            operation_ttl_ns,
        };
        value.validate()?;
        Ok(value)
    }
}

struct PinCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}
impl<'a> PinCursor<'a> {
    fn take<const N: usize>(&mut self) -> Result<[u8; N], WorkspaceError> {
        let end = self
            .offset
            .checked_add(N)
            .ok_or_else(|| pin_error("cursor overflow"))?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| pin_error("short record"))?
            .try_into()
            .map_err(pin_error)?;
        self.offset = end;
        Ok(value)
    }
    fn u64(&mut self) -> Result<u64, WorkspaceError> {
        Ok(u64::from_le_bytes(self.take()?))
    }
    fn field(&mut self, limit: usize) -> Result<&'a [u8], WorkspaceError> {
        let len = u32::from_le_bytes(self.take()?) as usize;
        if len > limit {
            return Err(pin_error("field exceeds bound"));
        }
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| pin_error("field overflow"))?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| pin_error("short field"))?;
        self.offset = end;
        Ok(value)
    }
}

impl<B: WorkspaceKvBackend> KvWorkspaceStore<B> {
    /// Configure this before wrapping the store in Arc. Reopened GC stores must
    /// supply a budget too; presence of persisted pins fails closed otherwise.
    pub fn with_packed_reader_pin_budget(self, budget: Arc<SnapshotMountBudget>) -> Self {
        self.configure_packed_reader_pin_budget(budget)
            .expect("reader pin budget cannot be replaced");
        self
    }
    /// Binding-open may configure an already-shared store, but cannot replace
    /// the budget while retained pin/root owners still use the old one.
    pub fn configure_packed_reader_pin_budget(
        &self,
        budget: Arc<SnapshotMountBudget>,
    ) -> Result<(), WorkspaceError> {
        let observed = self.packed_reader_pin_budget.get_or_init(|| budget.clone());
        if !Arc::ptr_eq(observed, &budget) {
            return Err(WorkspaceError::Busy);
        }
        Ok(())
    }
    /// Binding-open and admin GC adopt the same first-configured budget; the
    /// returned Arc must also be used for the mount's lower metadata/requests.
    pub fn resolve_packed_reader_pin_budget(
        &self,
        preferred: Arc<SnapshotMountBudget>,
    ) -> Arc<SnapshotMountBudget> {
        self.packed_reader_pin_budget
            .get_or_init(|| preferred)
            .clone()
    }
    pub async fn reap_expired_packed_readers(&self) -> Result<u64, WorkspaceError> {
        self.require_admin_access()?;
        let _probe = self.reader_pin_admission(OPERATION_BYTES)?;
        if self.pin_values(&[FEATURE_KEY.to_vec()]).await?.0[0].is_none() {
            return Ok(0);
        }
        let slots = self.list_packed_reader_pin_slots().await?;
        let mut retired = 0;
        for pin in slots
            .iter()
            .filter(|pin| pin.state == PackedReaderPinState::Active)
        {
            match self.reap_packed_reader_pin(pin, Uuid::new_v4()).await {
                Ok(_) => retired += 1,
                Err(WorkspaceError::Busy | WorkspaceError::Fenced) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(retired)
    }
    fn reader_pin_admission(&self, bytes: u64) -> Result<SnapshotOwnedPermit, WorkspaceError> {
        let budget =
            self.packed_reader_pin_budget
                .get()
                .ok_or(WorkspaceError::UnsupportedCapability(
                    "packed reader pin memory budget",
                ))?;
        budget
            .admit(&[(SnapshotBudgetPool::Metadata, bytes)])
            .map_err(|error| WorkspaceError::InvalidReadPlan(error.to_string()))
    }
    async fn pin_values(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        let limits = KvReadLimits {
            max_records: 16,
            max_key_bytes: 256,
            max_value_bytes: RECORD_BYTES,
            max_total_bytes: 256 << 10,
            max_response_bytes: 256 << 10,
            max_data_requests: 16,
        };
        let (values, now) = self
            .backend
            .get_many_consistent_with_time_bounded(keys, limits)
            .await?;
        if values.len() != keys.len() {
            return Err(pin_error("short consistent read"));
        }
        Ok((values, now))
    }
    /// New acquisition is authorized only by the exact current live view.
    /// A matching already-created pin is a lost-response retry, not a new grant.
    pub async fn acquire_packed_reader_pin(
        &self,
        request: &AcquirePackedReaderPin,
    ) -> Result<OwnedPackedReaderPin<PackedReaderPin>, WorkspaceError> {
        self.acquire_reader_pin_with_authority(request, None).await
    }

    /// Only a store-issued durable recovery fence can open the frozen Sealing
    /// view. The normal acquisition guard remains strictly Writable/Active.
    pub(crate) async fn acquire_native_recovery_reader_pin(
        self: &Arc<Self>,
        request: &AcquirePackedReaderPin,
        recovery: &PackedNativeRecoveryReadFence<B>,
    ) -> Result<OwnedPackedReaderPin<PackedReaderPin>, WorkspaceError> {
        if !Arc::ptr_eq(self, recovery.store()) {
            return Err(WorkspaceError::Fenced);
        }
        self.acquire_reader_pin_with_authority(request, Some(recovery))
            .await
    }

    async fn acquire_reader_pin_with_authority(
        &self,
        request: &AcquirePackedReaderPin,
        recovery: Option<&PackedNativeRecoveryReadFence<B>>,
    ) -> Result<OwnedPackedReaderPin<PackedReaderPin>, WorkspaceError> {
        check_slot(request.slot)?;
        check_ttl(request.ttl_ns)?;
        if request.pin_id.is_nil()
            || request.holder_generation == 0
            || request.owner_id.is_empty()
            || request.owner_id.len() > OWNER_BYTES
            || request.gc_grace_ns > MAX_PACKED_READER_GRACE_NS
        {
            return Err(WorkspaceError::InvalidReadPlan(
                "invalid reader pin request".into(),
            ));
        }
        if let Some(recovery) = recovery {
            let native = recovery.native_quiesce();
            let mapping = native.mapping();
            validate_permission_layers(mapping.old_layers())?;
            let mut frozen_layers = mapping.old_layers().clone();
            frozen_layers[0].state = LayerState::Sealing;
            if !std::ptr::eq(self, recovery.store().as_ref())
                || &request.guard != native.source_guard()
                || request.expected_layers != frozen_layers
                || &request.expected_binding != native.binding()
            {
                return Err(WorkspaceError::Fenced);
            }
        } else {
            validate_permission_layers(&request.expected_layers)?;
        }
        request
            .expected_binding
            .validate_for_guard(&request.guard, &request.expected_layers[1])?;
        let admission = self.reader_pin_admission(OPERATION_BYTES)?;
        let keys = vec![
            pin_key(request.slot),
            active_key(request.slot),
            FEATURE_KEY.to_vec(),
            COUNT_KEY.to_vec(),
            PACKED_ROOT_GENERATION_KEY.to_vec(),
            hot_workspace_key(request.guard.workspace_id),
            hot_layer_key(request.guard.expected_head_layer_id),
            hot_lease_key(request.guard.workspace_id, request.guard.lease_id),
            hot_layer_key(request.expected_binding.base_revision.layer_id),
            packed_current_key(request.guard.workspace_id),
            packed_claim_key(request.guard.workspace_id),
            packed_history_key(
                request.guard.workspace_id,
                request.expected_binding.binding.binding_version,
            ),
            packed_history_key(request.guard.workspace_id, 1),
        ];
        for _ in 0..CAS_MAX_RETRIES {
            let (_registry_owner, registry_checks) = self
                .packed_registry_publication_checks(&request.expected_binding)
                .await?;
            let (values, now) = self.pin_values(&keys).await?;
            // Keep the native lease, current journal and durable original-Q
            // basis in the same CAS as the pin. A preceding validation alone
            // would allow a takeover or rotation before pin installation.
            let recovery_authority = match recovery {
                Some(fence) => Some(fence.authority_checks_before().await?),
                None => None,
            };
            let feature = check_feature(&values[2], &values[3])?;
            let active_count = count(&values[3])?;
            let observed = values[0]
                .as_deref()
                .map(PackedReaderPin::decode)
                .transpose()?;
            if let Some(current) = &observed {
                if current.slot != request.slot {
                    return Err(pin_error("main key/record disagree"));
                }
                if current.matches_acquire(request) {
                    if !feature
                        || current.state != PackedReaderPinState::Active
                        || values[1] != values[0]
                    {
                        return Err(WorkspaceError::Fenced);
                    }
                    let mut checks = checks_for(&keys[..5], &values[..5]);
                    let mut deadline = current.expires_at_ns;
                    if let Some((authority, native_deadline)) = &recovery_authority {
                        checks = checks_for(&keys, &values);
                        if !merge_reader_authority_checks(&mut checks, authority) {
                            tokio::task::yield_now().await;
                            continue;
                        }
                        deadline = deadline.min(*native_deadline);
                    }
                    if !merge_reader_authority_checks(&mut checks, &registry_checks) {
                        tokio::task::yield_now().await;
                        continue;
                    }
                    if self
                        .backend
                        .compare_and_swap_before(&checks, &[], deadline)
                        .await?
                    {
                        return retain(current.clone(), admission, RECORD_BYTES as u64);
                    }
                    continue;
                }
                if current.state == PackedReaderPinState::Active
                    || current.slot_generation != request.expected_slot_generation
                {
                    return Err(WorkspaceError::Busy);
                }
            } else if request.expected_slot_generation != 0 {
                return Err(WorkspaceError::Busy);
            }
            if values[1].is_some() {
                return Err(pin_error("terminal/missing slot has active mirror"));
            }
            if active_count >= PACKED_READER_SLOT_COUNT as u64 {
                return Err(WorkspaceError::Busy);
            }
            let workspace: WorkspaceRecord = decode_required(&values[5])?;
            let head: LayerRecord = decode_required(&values[6])?;
            let lease: SnapshotLease = decode_required(&values[7])?;
            let base: LayerRecord = decode_required(&values[8])?;
            if recovery.is_some() {
                // Remaining identities and exact fields are proved by the
                // opaque token's checks and their merge with these hot rows.
                if workspace.state != WorkspaceState::Sealing
                    || head.state != LayerState::Sealing
                    || lease.expires_at_ns <= now
                {
                    return Err(WorkspaceError::Fenced);
                }
            } else {
                checked_hot_guard(&workspace, &head, &lease, &request.guard, now)?;
            }
            if [head, base] != request.expected_layers {
                return Err(WorkspaceError::Busy);
            }
            let binding = decode_packed_pair(
                request.guard.workspace_id,
                &values[9],
                &values[10],
                &values[11],
            )?
            .ok_or(WorkspaceError::Fenced)?;
            let initial = PackedLowerBindingRecord::decode(
                values[12]
                    .as_deref()
                    .ok_or_else(|| pin_error("initial history anchor missing"))?,
            )?;
            if initial.workspace_id != request.guard.workspace_id
                || initial.binding.binding_version != 1
            {
                return Err(pin_error("initial history anchor identity mismatch"));
            }
            if binding != request.expected_binding {
                return Err(WorkspaceError::Fenced);
            }
            let expires = checked_expiry(now, request.ttl_ns)?;
            expires
                .checked_add(i64::try_from(request.gc_grace_ns).map_err(pin_error)?)
                .ok_or_else(|| pin_error("expiry/grace overflow"))?;
            let pin = PackedReaderPin {
                slot: request.slot,
                slot_generation: request
                    .expected_slot_generation
                    .checked_add(1)
                    .ok_or_else(|| pin_error("slot generation exhausted"))?,
                pin_id: request.pin_id,
                owner_id: request.owner_id.clone(),
                holder_generation: request.holder_generation,
                revision: 1,
                state: PackedReaderPinState::Active,
                acquired_guard: request.guard.clone(),
                binding,
                created_at_ns: now,
                expires_at_ns: expires,
                gc_grace_ns: request.gc_grace_ns,
                original_ttl_ns: request.ttl_ns,
                operation_id: request.pin_id,
                operation: PinOperation::Acquire,
                operation_parent_revision: 0,
                operation_ttl_ns: request.ttl_ns,
            };
            let bytes = pin.encode()?;
            let next_generation = next_packed_root_generation(&values[4])?;
            let writes = vec![
                KvWrite::Put {
                    key: keys[0].clone(),
                    value: bytes.clone(),
                },
                KvWrite::Put {
                    key: keys[1].clone(),
                    value: bytes,
                },
                KvWrite::Put {
                    key: keys[2].clone(),
                    value: FEATURE.to_vec(),
                },
                KvWrite::Put {
                    key: keys[3].clone(),
                    value: (active_count + 1).to_le_bytes().to_vec(),
                },
                put(PACKED_ROOT_GENERATION_KEY.to_vec(), &next_generation)?,
            ];
            let mut checks = checks_for(&keys, &values);
            if !merge_reader_authority_checks(&mut checks, &registry_checks) {
                tokio::task::yield_now().await;
                continue;
            }
            let mut deadline = lease.expires_at_ns.min(expires);
            if let Some((authority, native_deadline)) = &recovery_authority {
                if !merge_reader_authority_checks(&mut checks, authority) {
                    tokio::task::yield_now().await;
                    continue;
                }
                deadline = deadline.min(*native_deadline);
            }
            if self
                .backend
                .compare_and_swap_before(&checks, &writes, deadline)
                .await?
            {
                return retain(pin, admission, RECORD_BYTES as u64);
            }
            tokio::task::yield_now().await;
        }
        Err(WorkspaceError::Busy)
    }

    /// Old binding renewal deliberately has no current-head dependency.
    /// Expired pins cannot be revived, including during their GC grace window.
    pub async fn renew_packed_reader_pin(
        &self,
        expected: &PackedReaderPin,
        operation_id: Uuid,
        ttl_ns: u64,
    ) -> Result<OwnedPackedReaderPin<PackedReaderPin>, WorkspaceError> {
        check_ttl(ttl_ns)?;
        self.change_reader_pin(expected, operation_id, PinOperation::Renew, ttl_ns)
            .await
    }
    /// Call only after all users of this generation have drained. A release
    /// after expiry is allowed; its exact incarnation cannot release a winner.
    pub async fn release_packed_reader_pin(
        &self,
        expected: &PackedReaderPin,
        operation_id: Uuid,
    ) -> Result<OwnedPackedReaderPin<PackedReaderPin>, WorkspaceError> {
        self.change_reader_pin(expected, operation_id, PinOperation::Release, 0)
            .await
    }
    /// Retire a crashed reader only at the backend locked validation point
    /// after expiry plus grace. Reaping uses no client-supplied wall clock.
    pub async fn reap_packed_reader_pin(
        &self,
        expected: &PackedReaderPin,
        operation_id: Uuid,
    ) -> Result<OwnedPackedReaderPin<PackedReaderPin>, WorkspaceError> {
        self.require_admin_access()?;
        self.change_reader_pin(expected, operation_id, PinOperation::Reap, 0)
            .await
    }
    async fn change_reader_pin(
        &self,
        expected: &PackedReaderPin,
        operation_id: Uuid,
        operation: PinOperation,
        ttl_ns: u64,
    ) -> Result<OwnedPackedReaderPin<PackedReaderPin>, WorkspaceError> {
        expected.validate()?;
        if expected.state != PackedReaderPinState::Active
            || operation_id.is_nil()
            || operation_id == expected.operation_id
        {
            return Err(WorkspaceError::Fenced);
        }
        let admission = self.reader_pin_admission(OPERATION_BYTES)?;
        let keys = vec![
            pin_key(expected.slot),
            active_key(expected.slot),
            FEATURE_KEY.to_vec(),
            COUNT_KEY.to_vec(),
            PACKED_ROOT_GENERATION_KEY.to_vec(),
        ];
        for _ in 0..CAS_MAX_RETRIES {
            let (values, now) = self.pin_values(&keys).await?;
            if !check_feature(&values[2], &values[3])? {
                return Err(WorkspaceError::Fenced);
            }
            let active_count = count(&values[3])?;
            let current = values[0]
                .as_deref()
                .map(PackedReaderPin::decode)
                .transpose()?
                .ok_or(WorkspaceError::Fenced)?;
            if current.slot != expected.slot {
                return Err(pin_error("main key/record disagree"));
            }
            let current_is_active = current.state == PackedReaderPinState::Active;
            if (current_is_active && values[1] != values[0])
                || (!current_is_active && values[1].is_some())
            {
                return Err(pin_error("main/active mirror disagree"));
            }
            let checks = checks_for(&keys, &values);
            if current.exact_retry(expected, operation_id, operation, ttl_ns) {
                let success = if operation == PinOperation::Renew {
                    self.backend
                        .compare_and_swap_before(&checks, &[], current.expires_at_ns)
                        .await?
                } else {
                    self.backend.compare_and_swap(&checks, &[]).await?
                };
                if success {
                    return retain(current, admission, RECORD_BYTES as u64);
                }
                continue;
            }
            if !current.same_identity(expected) || !current_is_active {
                return Err(WorkspaceError::Fenced);
            }
            if current != *expected {
                return Err(WorkspaceError::Busy);
            }
            if active_count == 0 {
                return Err(pin_error("active count is zero for live pin"));
            }
            let mut next = current.clone();
            next.revision = next
                .revision
                .checked_add(1)
                .ok_or_else(|| pin_error("revision exhausted"))?;
            next.operation_id = operation_id;
            next.operation = operation;
            next.operation_parent_revision = current.revision;
            next.operation_ttl_ns = ttl_ns;
            match operation {
                PinOperation::Renew => {
                    if now >= current.expires_at_ns {
                        return Err(WorkspaceError::Fenced);
                    }
                    // A backwards clock observation must never shorten a lease.
                    next.expires_at_ns = checked_expiry(now, ttl_ns)?.max(current.expires_at_ns);
                }
                PinOperation::Release => next.state = PackedReaderPinState::Released,
                PinOperation::Reap => next.state = PackedReaderPinState::Reaped,
                PinOperation::Acquire => return Err(pin_error("invalid update operation")),
            }
            let bytes = next.encode()?;
            let mut writes = vec![
                KvWrite::Put {
                    key: keys[0].clone(),
                    value: bytes.clone(),
                },
                put(
                    PACKED_ROOT_GENERATION_KEY.to_vec(),
                    &next_packed_root_generation(&values[4])?,
                )?,
            ];
            if next.state == PackedReaderPinState::Active {
                writes.push(KvWrite::Put {
                    key: keys[1].clone(),
                    value: bytes,
                });
            } else {
                writes.push(KvWrite::Delete {
                    key: keys[1].clone(),
                });
                writes.push(KvWrite::Put {
                    key: keys[3].clone(),
                    value: (active_count - 1).to_le_bytes().to_vec(),
                });
            }
            let success = match operation {
                PinOperation::Renew => {
                    self.backend
                        .compare_and_swap_before(&checks, &writes, current.expires_at_ns)
                        .await?
                }
                PinOperation::Release => self.backend.compare_and_swap(&checks, &writes).await?,
                PinOperation::Reap => {
                    let cutoff = current
                        .expires_at_ns
                        .checked_add(i64::try_from(current.gc_grace_ns).map_err(pin_error)?)
                        .ok_or_else(|| pin_error("reap cutoff overflow"))?;
                    self.backend
                        .compare_and_swap_in_time_window(&checks, &writes, Some(cutoff), None)
                        .await?
                }
                PinOperation::Acquire => unreachable!(),
            };
            if success {
                return retain(next, admission, RECORD_BYTES as u64);
            }
            tokio::task::yield_now().await;
        }
        Err(WorkspaceError::Busy)
    }

    /// This is the reader's live authority check, and is usable after a current
    /// binding change. It checks immutable identity/holder but accepts renewed
    /// revisions of the same incarnation. It never revives an expired pin.
    pub async fn validate_packed_reader_pin(
        &self,
        expected: &PackedReaderPin,
    ) -> Result<OwnedPackedReaderPin<PackedReaderPin>, WorkspaceError> {
        expected.validate()?;
        let admission = self.reader_pin_admission(OPERATION_BYTES)?;
        let keys = vec![
            pin_key(expected.slot),
            active_key(expected.slot),
            FEATURE_KEY.to_vec(),
        ];
        for _ in 0..CAS_MAX_RETRIES {
            let (values, _) = self.pin_values(&keys).await?;
            let current = values[0]
                .as_deref()
                .map(PackedReaderPin::decode)
                .transpose()?
                .ok_or(WorkspaceError::Fenced)?;
            if current.state != PackedReaderPinState::Active
                || !current.same_identity(expected)
                || values[0] != values[1]
                || values[2].as_deref() != Some(FEATURE)
            {
                return Err(WorkspaceError::Fenced);
            }
            if self
                .backend
                .compare_and_swap_before(&checks_for(&keys, &values), &[], current.expires_at_ns)
                .await?
            {
                return retain(current, admission, RECORD_BYTES as u64);
            }
            tokio::task::yield_now().await;
        }
        Err(WorkspaceError::Busy)
    }

    /// Catalog inspection for shutdown/reconciliation, including expired and
    /// terminal records. This is never permission to fetch or deliver bytes.
    pub async fn inspect_packed_reader_pin(
        &self,
        expected: &PackedReaderPin,
    ) -> Result<OwnedPackedReaderPin<PackedReaderPin>, WorkspaceError> {
        expected.validate()?;
        let admission = self.reader_pin_admission(OPERATION_BYTES)?;
        let keys = vec![
            pin_key(expected.slot),
            active_key(expected.slot),
            FEATURE_KEY.to_vec(),
        ];
        for _ in 0..CAS_MAX_RETRIES {
            let (values, _) = self.pin_values(&keys).await?;
            let current = values[0]
                .as_deref()
                .map(PackedReaderPin::decode)
                .transpose()?
                .ok_or(WorkspaceError::Fenced)?;
            if !current.same_identity(expected) || values[2].as_deref() != Some(FEATURE) {
                return Err(WorkspaceError::Fenced);
            }
            if (current.state == PackedReaderPinState::Active && values[0] != values[1])
                || (current.state != PackedReaderPinState::Active && values[1].is_some())
            {
                return Err(pin_error("inspection main/active disagree"));
            }
            if self
                .backend
                .compare_and_swap(&checks_for(&keys, &values), &[])
                .await?
            {
                return retain(current, admission, RECORD_BYTES as u64);
            }
            tokio::task::yield_now().await;
        }
        Err(WorkspaceError::Busy)
    }

    /// Bounded catalog inspection, including terminal slots available for
    /// acquisition. Inspection alone is not an authority to read any binding.
    pub async fn list_packed_reader_pin_slots(
        &self,
    ) -> Result<OwnedPackedReaderPin<Vec<PackedReaderPin>>, WorkspaceError> {
        let admission = self.reader_pin_admission(SCAN_BYTES)?;
        for _ in 0..CAS_MAX_RETRIES {
            let keys = vec![
                FEATURE_KEY.to_vec(),
                COUNT_KEY.to_vec(),
                PACKED_ROOT_GENERATION_KEY.to_vec(),
            ];
            let (values, _) = self.pin_values(&keys).await?;
            let feature = check_feature(&values[0], &values[1])?;
            count(&values[1])?;
            let entries = self
                .backend
                .scan_prefix_with_byte_limits(
                    PIN_PREFIX,
                    KvReadLimits {
                        max_records: PACKED_READER_SLOT_COUNT + 1,
                        max_key_bytes: 256,
                        max_value_bytes: RECORD_BYTES,
                        max_total_bytes: 4 << 20,
                        max_response_bytes: 4 << 20,
                        max_data_requests: 1024,
                    },
                )
                .await?;
            if entries.len() > PACKED_READER_SLOT_COUNT {
                return Err(pin_error("slot namespace exceeds bound"));
            }
            if !feature && !entries.is_empty() {
                return Err(pin_error("slots without feature"));
            }
            let mut records = Vec::with_capacity(entries.len());
            for entry in entries {
                let pin = PackedReaderPin::decode(&entry.value)?;
                if entry.key != pin_key(pin.slot) {
                    return Err(pin_error("slot key/record disagree"));
                }
                records.push(pin);
            }
            if self
                .backend
                .compare_and_swap(&checks_for(&keys, &values), &[])
                .await?
            {
                records.sort_by_key(|pin| pin.slot);
                let bytes = (records.len() as u64 + 1) * RECORD_BYTES as u64;
                return retain(records, admission, bytes);
            }
            tokio::task::yield_now().await;
        }
        Err(WorkspaceError::Busy)
    }

    /// Active expired/grace pins remain roots until a successful backend-clock
    /// reaper CAS. A caller's now_ns never makes them disappear from GC.
    pub async fn packed_reader_pin_roots(&self) -> Result<PackedReaderPinRoots, WorkspaceError> {
        let mut roots = self.packed_reader_lease_roots().await?;
        self.append_packed_entity_lower_roots(&mut roots).await?;
        Ok(roots)
    }

    async fn packed_reader_lease_roots(&self) -> Result<PackedReaderPinRoots, WorkspaceError> {
        // The absent sentinel is checked in the final GC CAS. A first acquire
        // writes it and root generation atomically, fencing the old snapshot.
        // Even the absence probe must own admission and use bounded values:
        // an oversized corrupt feature cannot be read first and checked later.
        let _probe = self.reader_pin_admission(OPERATION_BYTES)?;
        let (probe, _) = self
            .pin_values(&[FEATURE_KEY.to_vec(), COUNT_KEY.to_vec()])
            .await?;
        if probe[0].is_none() {
            if probe[1].is_some() {
                return Err(pin_error("active count without feature sentinel"));
            }
            return Ok(PackedReaderPinRoots {
                native_roots: BTreeSet::new(),
                bindings: Vec::new(),
                checks: vec![
                    KvCheck {
                        key: FEATURE_KEY.to_vec(),
                        expected: None,
                    },
                    KvCheck {
                        key: COUNT_KEY.to_vec(),
                        expected: None,
                    },
                ],
                _permit: None,
            });
        }
        let keys = vec![
            FEATURE_KEY.to_vec(),
            COUNT_KEY.to_vec(),
            PACKED_ROOT_GENERATION_KEY.to_vec(),
        ];
        let (values, _) = self.pin_values(&keys).await?;
        let feature = check_feature(&values[0], &values[1])?;
        let active_count = count(&values[1])?;
        let mut snapshot = PackedReaderPinRoots {
            native_roots: BTreeSet::new(),
            bindings: Vec::new(),
            checks: checks_for(&keys, &values),
            _permit: None,
        };
        if !feature {
            return Ok(snapshot);
        }
        snapshot._permit = Some(self.reader_pin_admission(SCAN_BYTES)?);
        let entries = self
            .backend
            .scan_prefix_with_byte_limits(
                ACTIVE_PREFIX,
                KvReadLimits {
                    max_records: PACKED_READER_SLOT_COUNT + 1,
                    max_key_bytes: 256,
                    max_value_bytes: RECORD_BYTES,
                    max_total_bytes: 4 << 20,
                    max_response_bytes: 4 << 20,
                    max_data_requests: 1024,
                },
            )
            .await?;
        if entries.len() > PACKED_READER_SLOT_COUNT {
            return Err(pin_error("active namespace exceeds bound"));
        }
        if entries.len() as u64 != active_count {
            return Err(WorkspaceError::Busy);
        }
        for entry in entries {
            let pin = PackedReaderPin::decode(&entry.value)?;
            if entry.key != active_key(pin.slot) || pin.state != PackedReaderPinState::Active {
                return Err(pin_error("active key/record disagree"));
            }
            let pair_keys = vec![pin_key(pin.slot), entry.key.clone()];
            let (pair, _) = self.pin_values(&pair_keys).await?;
            if pair[0].as_deref() != Some(entry.value.as_slice()) || pair[1] != pair[0] {
                return Err(WorkspaceError::Busy);
            }
            snapshot.checks.extend(checks_for(&pair_keys, &pair));
            snapshot
                .native_roots
                .insert(pin.binding.base_revision.layer_id);
            snapshot.native_roots.insert(pin.binding.head_layer_id);
            snapshot.bindings.push(pin.binding);
        }
        Ok(snapshot)
    }
}

#[cfg(test)]
mod packed_reader_pin_tests;
