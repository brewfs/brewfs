//! Exact typed object adoption and permanent remote-operation quarantine.
//! The global object row is shared by before-PUT and retirement CAS paths.
//! Native/effective publication and old-root migration remain separate gates.

#[cfg(all(test, target_os = "linux"))]
#[path = "registry/initial_history_real_tests.rs"]
mod initial_history_real_tests;

use super::*;
pub(crate) mod admin_gc;
pub(crate) mod borrowed_alias;
pub(crate) mod collector;
mod history_retirement;
mod native_holds;
pub(crate) use history_retirement::{
    PackedHistoryRetirementOptions, PackedHistoryRetirementReport,
};
pub(crate) use native_holds::lease_reaper::{
    PackedNativeLeaseReaperOptions, PackedNativeLeaseReaperReport,
};
#[cfg(target_os = "linux")]
mod initial_bootstrap;
#[cfg(target_os = "linux")]
mod upload_backend;

const REGISTRY_OBJECT_PREFIX: &str = "packed/registry/object/";
const REGISTRY_ROOT_PREFIX: &str = "packed/registry/root/";
const REGISTRY_MEMBER_PREFIX: &str = "packed/registry/member/";
const REGISTRY_REVERSE_PREFIX: &str = "packed/registry/root-member/";
const REGISTRY_RECORD_LIMIT: usize = REFERENCE_LIMIT + 512;

type RowChange = (Vec<u8>, Option<Vec<u8>>, Option<Vec<u8>>);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
enum ObjectState {
    Live = 0,
    Retiring = 1,
    DeletePending = 2,
    Deleted = 3,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
enum RootState {
    Staging = 0,
    BindingHistory = 1,
    AbortedRetained = 2,
    Retiring = 3,
    Retired = 4,
    AdoptingBinding = 5,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ObjectRow {
    reference: SnapshotObjectRef,
    revision: u64,
    state: ObjectState,
    memberships: u64,
    pending_puts: u64,
    delete_id: Uuid,
    delete_dispatched: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RootRow {
    journal_id: JournalId,
    incarnation: Uuid,
    revision: u64,
    state: RootState,
    members: u64,
    pending_puts: u64,
    binding: Option<PackedLowerBindingRecord>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct MemberRow {
    reference: SnapshotObjectRef,
    journal_id: JournalId,
    incarnation: Uuid,
    ordinal: u64,
    put_id: Uuid,
    adopted: bool,
    pending_put: bool,
    dispatched: bool,
    retained: bool,
}

/// No Clone or public constructor. Issued only after adoption, the typed
/// journal occurrence and the pending PUT hold persist in the same CAS.
pub(crate) struct PackedUploadGuard {
    reference: SnapshotObjectRef,
    incarnation: Uuid,
    journal_id: JournalId,
    ordinal: u64,
    put_id: Uuid,
    _permit: SnapshotOwnedPermit,
}

fn registry_object_key(reference: &SnapshotObjectRef) -> Vec<u8> {
    format!(
        "{REGISTRY_OBJECT_PREFIX}{}",
        hex::encode(Sha256::digest(reference.key.as_bytes()))
    )
    .into_bytes()
}
pub(super) fn registry_root_key(incarnation: Uuid) -> Vec<u8> {
    format!("{REGISTRY_ROOT_PREFIX}{}", incarnation.simple()).into_bytes()
}
fn registry_member_key(reference: &SnapshotObjectRef, incarnation: Uuid) -> Vec<u8> {
    format!(
        "{REGISTRY_MEMBER_PREFIX}{}/{}",
        hex::encode(Sha256::digest(reference.key.as_bytes())),
        incarnation.simple()
    )
    .into_bytes()
}
fn registry_reverse_key(incarnation: Uuid, ordinal: u64) -> Vec<u8> {
    format!(
        "{REGISTRY_REVERSE_PREFIX}{}/{ordinal:016x}",
        incarnation.simple()
    )
    .into_bytes()
}
fn registry_history_root_key(binding: &PackedLowerBindingRecord) -> Vec<u8> {
    format!(
        "packed/registry/history-root/{}/{:016x}",
        binding.workspace_id, binding.binding.binding_version
    )
    .into_bytes()
}
fn increment(value: u64) -> Result<u64, WorkspaceError> {
    value
        .checked_add(1)
        .ok_or_else(|| journal_error("registry revision/count overflow"))
}
fn decrement(value: u64) -> Result<u64, WorkspaceError> {
    value
        .checked_sub(1)
        .ok_or_else(|| journal_error("registry count underflow"))
}
fn boolean(value: u8) -> Result<bool, WorkspaceError> {
    match value {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(journal_error("invalid registry boolean")),
    }
}
fn row_header(magic: &[u8; 4]) -> Vec<u8> {
    [magic.as_slice(), &1u64.to_le_bytes()].concat()
}

impl ObjectRow {
    fn encode(&self) -> Result<Vec<u8>, WorkspaceError> {
        if self.revision == 0
            || self.pending_puts > self.memberships
            || (self.state != ObjectState::Live
                && (self.memberships != 0 || self.pending_puts != 0))
            || (matches!(
                self.state,
                ObjectState::DeletePending | ObjectState::Deleted
            ) != !self.delete_id.is_nil())
            || (self.delete_dispatched
                && !matches!(
                    self.state,
                    ObjectState::DeletePending | ObjectState::Deleted
                ))
            || (self.state == ObjectState::Deleted && !self.delete_dispatched)
        {
            return Err(journal_error("invalid global object registry row"));
        }
        let mut out = row_header(b"PRO3");
        append_bytes(
            &mut out,
            &self.reference.encode_value().map_err(journal_error)?,
            REFERENCE_LIMIT,
        )?;
        out.extend_from_slice(&self.revision.to_le_bytes());
        out.push(self.state as u8);
        out.extend_from_slice(&self.memberships.to_le_bytes());
        out.extend_from_slice(&self.pending_puts.to_le_bytes());
        out.extend_from_slice(self.delete_id.as_bytes());
        out.push(u8::from(self.delete_dispatched));
        finish_record(out, REGISTRY_RECORD_LIMIT)
    }
    fn decode(bytes: &[u8]) -> Result<Self, WorkspaceError> {
        let mut c = JournalCursor::checked(bytes, b"PRO3", REGISTRY_RECORD_LIMIT)?;
        let reference =
            SnapshotObjectRef::decode_value(&c.bytes(REFERENCE_LIMIT)?).map_err(journal_error)?;
        let revision = c.u64()?;
        let state = match c.take::<1>()?[0] {
            0 => ObjectState::Live,
            1 => ObjectState::Retiring,
            2 => ObjectState::DeletePending,
            3 => ObjectState::Deleted,
            _ => return Err(journal_error("unknown object registry state")),
        };
        let memberships = c.u64()?;
        let pending_puts = c.u64()?;
        let delete_id = Uuid::from_bytes(c.take()?);
        let delete_dispatched = boolean(c.take::<1>()?[0])?;
        c.end()?;
        let result = Self {
            reference,
            revision,
            state,
            memberships,
            pending_puts,
            delete_id,
            delete_dispatched,
        };
        result.encode()?;
        Ok(result)
    }
}

impl RootRow {
    fn for_journal(record: &PackedJournalRecord) -> Self {
        Self {
            journal_id: record.journal_id,
            incarnation: record.source.staging_id,
            revision: 1,
            state: RootState::Staging,
            members: 0,
            pending_puts: 0,
            binding: None,
        }
    }
    fn encode(&self) -> Result<Vec<u8>, WorkspaceError> {
        if self.journal_id.as_uuid().is_nil()
            || self.incarnation.is_nil()
            || self.revision == 0
            || self.members > MAX_OBJECTS
            || self.pending_puts > self.members
            || (self.state == RootState::Retired && (self.members != 0 || self.pending_puts != 0))
            || (matches!(
                self.state,
                RootState::BindingHistory | RootState::AdoptingBinding
            ) && self.binding.is_none())
            || (matches!(self.state, RootState::Staging | RootState::AbortedRetained)
                && self.binding.is_some())
            || (matches!(self.state, RootState::Retiring | RootState::Retired)
                && self.pending_puts != 0)
            || (self.state == RootState::AdoptingBinding && self.pending_puts != 0)
        {
            return Err(journal_error("invalid retained graph root"));
        }
        let mut out = row_header(b"PRR3");
        out.extend_from_slice(self.journal_id.as_bytes());
        out.extend_from_slice(self.incarnation.as_bytes());
        out.extend_from_slice(&self.revision.to_le_bytes());
        out.push(self.state as u8);
        out.extend_from_slice(&self.members.to_le_bytes());
        out.extend_from_slice(&self.pending_puts.to_le_bytes());
        let binding = self
            .binding
            .as_ref()
            .map(PackedLowerBindingRecord::encode)
            .transpose()?
            .unwrap_or_default();
        append_bytes(&mut out, &binding, REFERENCE_LIMIT)?;
        finish_record(out, REGISTRY_RECORD_LIMIT)
    }
    fn decode(bytes: &[u8]) -> Result<Self, WorkspaceError> {
        let mut c = JournalCursor::checked(bytes, b"PRR3", REGISTRY_RECORD_LIMIT)?;
        let journal_id = JournalId::from_uuid(Uuid::from_bytes(c.take()?));
        let incarnation = Uuid::from_bytes(c.take()?);
        let revision = c.u64()?;
        let state = match c.take::<1>()?[0] {
            0 => RootState::Staging,
            1 => RootState::BindingHistory,
            2 => RootState::AbortedRetained,
            3 => RootState::Retiring,
            4 => RootState::Retired,
            5 => RootState::AdoptingBinding,
            _ => return Err(journal_error("unknown retained root state")),
        };
        let members = c.u64()?;
        let pending_puts = c.u64()?;
        let binding = c.bytes(REFERENCE_LIMIT)?;
        let binding = if binding.is_empty() {
            None
        } else {
            Some(PackedLowerBindingRecord::decode(&binding)?)
        };
        c.end()?;
        let result = Self {
            journal_id,
            incarnation,
            revision,
            state,
            members,
            pending_puts,
            binding,
        };
        result.encode()?;
        Ok(result)
    }
    fn check_staging(&self, expected: &PackedJournalRecord) -> Result<(), WorkspaceError> {
        if self.journal_id != expected.journal_id
            || self.incarnation != expected.source.staging_id
            || self.state != RootState::Staging
            || self.members != expected.object_count
        {
            return Err(WorkspaceError::Fenced);
        }
        Ok(())
    }
}

impl MemberRow {
    fn encode(&self) -> Result<Vec<u8>, WorkspaceError> {
        if self.journal_id.as_uuid().is_nil()
            || self.incarnation.is_nil()
            || (self.adopted && (!self.put_id.is_nil() || self.pending_put || self.dispatched))
            || (!self.adopted && self.put_id.is_nil())
            || self.ordinal >= MAX_OBJECTS
            || (!self.retained && self.pending_put)
            || (!self.adopted && self.retained && !self.pending_put && !self.dispatched)
        {
            return Err(journal_error("invalid typed registry membership"));
        }
        let mut out = row_header(b"PRM3");
        append_bytes(
            &mut out,
            &self.reference.encode_value().map_err(journal_error)?,
            REFERENCE_LIMIT,
        )?;
        out.extend_from_slice(self.journal_id.as_bytes());
        out.extend_from_slice(self.incarnation.as_bytes());
        out.extend_from_slice(&self.ordinal.to_le_bytes());
        out.extend_from_slice(self.put_id.as_bytes());
        out.push(u8::from(self.adopted));
        out.push(u8::from(self.pending_put));
        out.push(u8::from(self.dispatched));
        out.push(u8::from(self.retained));
        finish_record(out, REGISTRY_RECORD_LIMIT)
    }
    fn decode(bytes: &[u8]) -> Result<Self, WorkspaceError> {
        let mut c = JournalCursor::checked(bytes, b"PRM3", REGISTRY_RECORD_LIMIT)?;
        let reference =
            SnapshotObjectRef::decode_value(&c.bytes(REFERENCE_LIMIT)?).map_err(journal_error)?;
        let journal_id = JournalId::from_uuid(Uuid::from_bytes(c.take()?));
        let incarnation = Uuid::from_bytes(c.take()?);
        let ordinal = c.u64()?;
        let put_id = Uuid::from_bytes(c.take()?);
        let adopted = boolean(c.take::<1>()?[0])?;
        let pending_put = boolean(c.take::<1>()?[0])?;
        let dispatched = boolean(c.take::<1>()?[0])?;
        let retained = boolean(c.take::<1>()?[0])?;
        c.end()?;
        let result = Self {
            reference,
            journal_id,
            incarnation,
            ordinal,
            put_id,
            adopted,
            pending_put,
            dispatched,
            retained,
        };
        result.encode()?;
        Ok(result)
    }
}

pub(super) fn begin_root_change(record: &PackedJournalRecord) -> Result<RowChange, WorkspaceError> {
    Ok((
        registry_root_key(record.source.staging_id),
        None,
        Some(RootRow::for_journal(record).encode()?),
    ))
}

pub(super) fn check_quiesced_staging_root(
    record: &PackedJournalRecord,
    raw: Option<&[u8]>,
) -> Result<(), WorkspaceError> {
    let root = RootRow::decode(raw.ok_or_else(|| journal_error("quiesced staging root missing"))?)?;
    root.check_staging(record)?;
    if root.pending_puts != 0 {
        return Err(WorkspaceError::Busy);
    }
    Ok(())
}

pub(super) fn check_staging_root_bytes(
    record: &PackedJournalRecord,
    raw: Option<&[u8]>,
) -> Result<(), WorkspaceError> {
    let root = RootRow::decode(raw.ok_or_else(|| journal_error("staging graph root missing"))?)?;
    root.check_staging(record)
}

pub(super) fn recovery_root_value(
    expected: &PackedJournalRecord,
    next: &PackedJournalRecord,
    retry: bool,
    raw: &Option<Vec<u8>>,
) -> Result<Option<Vec<u8>>, WorkspaceError> {
    let mut root = RootRow::decode(
        raw.as_deref()
            .ok_or_else(|| journal_error("recovery graph root missing"))?,
    )?;
    if root.journal_id != expected.journal_id
        || root.incarnation != expected.source.staging_id
        || root.members != expected.object_count
    {
        return Err(WorkspaceError::Fenced);
    }
    if next.phase != PackedJournalPhase::Aborted {
        root.check_staging(expected)?;
        return Ok(None);
    }
    if retry {
        if root.state != RootState::AbortedRetained {
            return Err(WorkspaceError::Fenced);
        }
        return Ok(None);
    }
    root.check_staging(expected)?;
    root.revision = increment(root.revision)?;
    root.state = RootState::AbortedRetained;
    Ok(Some(root.encode()?))
}

impl<B: WorkspaceKvBackend> KvWorkspaceStore<B> {
    pub(super) async fn existing_completed_packed_upload(
        &self,
        expected: &PackedJournalRecord,
        reference: &SnapshotObjectRef,
        budget: &Arc<SnapshotMountBudget>,
    ) -> Result<bool, WorkspaceError> {
        self.existing_completed_packed_upload_under(expected, reference, budget, None)
            .await
    }

    pub(super) async fn existing_completed_packed_upload_under(
        &self,
        expected: &PackedJournalRecord,
        reference: &SnapshotObjectRef,
        budget: &Arc<SnapshotMountBudget>,
        native: Option<&super::native_publication::NativeJournalAuthority<'_, B>>,
    ) -> Result<bool, WorkspaceError> {
        let _owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, OPERATION_BYTES)])
            .map_err(journal_budget_error)?;
        let keys = [
            registry_object_key(reference),
            registry_member_key(reference, expected.source.staging_id),
            registry_root_key(expected.source.staging_id),
        ];
        let values = self.packed_journal_values(&keys).await?;
        if values.len() != 3 {
            return Err(journal_error("short existing upload read"));
        }
        let Some(raw_member) = &values[1] else {
            return Ok(false);
        };
        let object = ObjectRow::decode(
            values[0]
                .as_deref()
                .ok_or_else(|| journal_error("existing upload object missing"))?,
        )?;
        let member = MemberRow::decode(raw_member)?;
        let root = RootRow::decode(
            values[2]
                .as_deref()
                .ok_or_else(|| journal_error("existing upload root missing"))?,
        )?;
        root.check_staging(expected)?;
        if object.reference != *reference
            || object.state != ObjectState::Live
            || object.memberships == 0
            || member.reference != *reference
            || member.journal_id != expected.journal_id
            || member.incarnation != expected.source.staging_id
            || !member.retained
            || member.pending_put
            || !member.dispatched
            || member.adopted
        {
            return Err(WorkspaceError::Busy);
        }
        let occurrence = self
            .reopen_packed_object(expected, member.ordinal, budget)
            .await?;
        if occurrence.reference != *reference || !occurrence.uploaded {
            return Err(WorkspaceError::Fenced);
        }
        let mut checks = keys
            .iter()
            .cloned()
            .zip(values)
            .map(|(key, value)| (key, value.clone(), value))
            .collect::<Vec<_>>();
        let reverse_key = registry_reverse_key(expected.source.staging_id, member.ordinal);
        let reverse = self
            .packed_journal_values(std::slice::from_ref(&reverse_key))
            .await?;
        if reverse.len() != 1
            || reverse[0].as_deref()
                != Some(reference.encode_value().map_err(journal_error)?.as_slice())
        {
            return Err(WorkspaceError::Fenced);
        }
        checks.push((reverse_key, reverse[0].clone(), reverse[0].clone()));
        self.packed_journal_write_under(Some(expected), expected, &checks, None, native)
            .await?;
        Ok(true)
    }

    #[cfg(test)]
    pub(crate) async fn test_set_registry_root_pending_puts(
        &self,
        incarnation: Uuid,
        pending_puts: u64,
    ) -> Result<(), WorkspaceError> {
        let key = registry_root_key(incarnation);
        let raw = self
            .backend
            .get(&key)
            .await?
            .ok_or_else(|| journal_error("retained graph root missing"))?;
        let mut root = RootRow::decode(&raw)?;
        root.pending_puts = pending_puts;
        let next = root.encode()?;
        if !self
            .backend
            .compare_and_swap(
                &[KvCheck {
                    key: key.clone(),
                    expected: Some(raw),
                }],
                &[KvWrite::Put { key, value: next }],
            )
            .await?
        {
            return Err(WorkspaceError::Busy);
        }
        Ok(())
    }

    pub(super) async fn registry_transition_root(
        &self,
        expected: &PackedJournalRecord,
        commit: bool,
    ) -> Result<RowChange, WorkspaceError> {
        let key = registry_root_key(expected.source.staging_id);
        let values = self
            .packed_journal_values(&[key.clone(), journal_key(expected.journal_id)])
            .await?;
        if values.len() != 2 {
            return Err(journal_error("short retained-root transition read"));
        }
        let raw = values[0]
            .clone()
            .ok_or_else(|| journal_error("retained graph root missing"))?;
        let actual = PackedJournalRecord::decode(
            values[1]
                .as_deref()
                .ok_or_else(|| journal_error("retained-root journal missing"))?,
        )?;
        let mut root = RootRow::decode(&raw)?;
        let terminal = if commit {
            PackedJournalPhase::Committed
        } else {
            PackedJournalPhase::Aborted
        };
        let terminal_root = if commit {
            RootState::BindingHistory
        } else {
            RootState::AbortedRetained
        };
        if root.state == terminal_root
            && actual.phase == terminal
            && actual.journal_id == expected.journal_id
            && actual.source == expected.source
            && actual.revision == increment(expected.revision)?
            && root.incarnation == expected.source.staging_id
            && root.journal_id == expected.journal_id
            && root.members == expected.object_count
            && (!commit || root.pending_puts == 0)
            && (!commit || root.binding == expected.commit_target)
        {
            return Ok((key, Some(raw.clone()), Some(raw)));
        }
        if actual != *expected {
            return Err(WorkspaceError::Busy);
        }
        root.check_staging(expected)?;
        root.revision = increment(root.revision)?;
        if commit {
            if root.pending_puts != 0 {
                return Err(WorkspaceError::Busy);
            }
            root.state = RootState::BindingHistory;
            root.binding = expected.commit_target.clone();
        } else {
            root.state = RootState::AbortedRetained;
        }
        Ok((key, Some(raw), Some(root.encode()?)))
    }

    pub(super) fn registry_history_root_change(
        target: &PackedLowerBindingRecord,
        root_change: &RowChange,
    ) -> Result<RowChange, WorkspaceError> {
        let value = root_change
            .2
            .clone()
            .ok_or_else(|| journal_error("committed registry root missing"))?;
        let root = RootRow::decode(&value)?;
        if root.state != RootState::BindingHistory || root.binding.as_ref() != Some(target) {
            return Err(WorkspaceError::Fenced);
        }
        let key = registry_history_root_key(target);
        Ok((key, None, Some(value)))
    }

    /// Atomic shared-row adoption before any remote request. Pending PUT holds
    /// remain durable on cancellation/unknown response and block retirement.
    pub(crate) async fn reserve_packed_upload(
        &self,
        expected: &PackedJournalRecord,
        reference: SnapshotObjectRef,
        budget: &Arc<SnapshotMountBudget>,
    ) -> Result<(OwnedPackedJournal<PackedJournalRecord>, PackedUploadGuard), WorkspaceError> {
        self.reserve_packed_upload_under(expected, reference, budget, None)
            .await
    }

    pub(super) async fn reserve_packed_upload_under(
        &self,
        expected: &PackedJournalRecord,
        reference: SnapshotObjectRef,
        budget: &Arc<SnapshotMountBudget>,
        native: Option<&super::native_publication::NativeJournalAuthority<'_, B>>,
    ) -> Result<(OwnedPackedJournal<PackedJournalRecord>, PackedUploadGuard), WorkspaceError> {
        let operation_owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, OPERATION_BYTES)])
            .map_err(journal_budget_error)?;
        let token_owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, 32 << 10)])
            .map_err(journal_budget_error)?;
        if expected.phase != PackedJournalPhase::Building || expected.object_count >= MAX_OBJECTS {
            return Err(WorkspaceError::Busy);
        }
        let keys = [
            registry_object_key(&reference),
            registry_member_key(&reference, expected.source.staging_id),
            registry_root_key(expected.source.staging_id),
            registry_reverse_key(expected.source.staging_id, expected.object_count),
        ];
        let values = self.packed_journal_values(&keys).await?;
        if values.len() != 4 || values[1].is_some() || values[3].is_some() {
            return Err(WorkspaceError::Busy);
        }
        let mut root = RootRow::decode(
            values[2]
                .as_deref()
                .ok_or_else(|| journal_error("staging root missing before PUT"))?,
        )?;
        root.check_staging(expected)?;
        let mut object = values[0]
            .as_deref()
            .map(ObjectRow::decode)
            .transpose()?
            .unwrap_or(ObjectRow {
                reference: reference.clone(),
                revision: 0,
                state: ObjectState::Live,
                memberships: 0,
                pending_puts: 0,
                delete_id: Uuid::nil(),
                delete_dispatched: false,
            });
        if object.reference != reference || object.state != ObjectState::Live {
            return Err(WorkspaceError::Fenced);
        }
        object.revision = increment(object.revision)?;
        object.memberships = increment(object.memberships)?;
        object.pending_puts = increment(object.pending_puts)?;
        root.revision = increment(root.revision)?;
        root.members = increment(root.members)?;
        root.pending_puts = increment(root.pending_puts)?;
        let put_id = Uuid::new_v4();
        let ordinal = expected.object_count;
        let member = MemberRow {
            reference: reference.clone(),
            journal_id: expected.journal_id,
            incarnation: expected.source.staging_id,
            ordinal,
            put_id,
            adopted: false,
            pending_put: true,
            dispatched: false,
            retained: true,
        };
        let occurrence = PackedJournalObject {
            ordinal,
            reference: reference.clone(),
            uploaded: false,
            readback_recorded: false,
        };
        let mut next = expected.next()?;
        next.object_count = increment(next.object_count)?;
        next.inventory_digest = inventory_append(expected.inventory_digest, ordinal, &reference)?;
        let changes = vec![
            (keys[0].clone(), values[0].clone(), Some(object.encode()?)),
            (keys[1].clone(), None, Some(member.encode()?)),
            (keys[2].clone(), values[2].clone(), Some(root.encode()?)),
            (
                keys[3].clone(),
                None,
                Some(reference.encode_value().map_err(journal_error)?),
            ),
            (
                object_key(expected.journal_id, ordinal),
                None,
                Some(occurrence.encode()?),
            ),
            (
                identity_key(expected.journal_id, &reference.key),
                None,
                Some(ordinal.to_le_bytes().to_vec()),
            ),
        ];
        let next = self
            .packed_journal_write_under(Some(expected), &next, &changes, None, native)
            .await
            .retain(operation_owner)?;
        Ok((
            next,
            PackedUploadGuard {
                reference,
                incarnation: expected.source.staging_id,
                journal_id: expected.journal_id,
                ordinal,
                put_id,
                _permit: token_owner,
            },
        ))
    }

    /// A separate single-use dispatch CAS prevents two recovered reserve
    /// handles from issuing concurrent physical requests for one pending hold.
    /// A lost CAS response never authorizes the caller to send the PUT.
    pub(crate) async fn dispatch_packed_upload(
        &self,
        expected: &PackedJournalRecord,
        guard: &PackedUploadGuard,
        budget: &Arc<SnapshotMountBudget>,
    ) -> Result<OwnedPackedJournal<PackedJournalRecord>, WorkspaceError> {
        self.dispatch_packed_upload_under(expected, guard, budget, None)
            .await
    }

    pub(super) async fn dispatch_packed_upload_under(
        &self,
        expected: &PackedJournalRecord,
        guard: &PackedUploadGuard,
        budget: &Arc<SnapshotMountBudget>,
        native: Option<&super::native_publication::NativeJournalAuthority<'_, B>>,
    ) -> Result<OwnedPackedJournal<PackedJournalRecord>, WorkspaceError> {
        let owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, OPERATION_BYTES)])
            .map_err(journal_budget_error)?;
        if expected.phase != PackedJournalPhase::Building
            || expected.journal_id != guard.journal_id
            || expected.source.staging_id != guard.incarnation
        {
            return Err(WorkspaceError::Fenced);
        }
        let keys = [
            registry_object_key(&guard.reference),
            registry_member_key(&guard.reference, guard.incarnation),
            registry_root_key(guard.incarnation),
            registry_reverse_key(guard.incarnation, guard.ordinal),
        ];
        let values = self.packed_journal_values(&keys).await?;
        if values.len() != 4 {
            return Err(journal_error("short registry dispatch read"));
        }
        let object = ObjectRow::decode(
            values[0]
                .as_deref()
                .ok_or_else(|| journal_error("reserved object missing"))?,
        )?;
        let mut member = MemberRow::decode(
            values[1]
                .as_deref()
                .ok_or_else(|| journal_error("reserved member missing"))?,
        )?;
        let root = RootRow::decode(
            values[2]
                .as_deref()
                .ok_or_else(|| journal_error("reserved root missing"))?,
        )?;
        root.check_staging(expected)?;
        if object.reference != guard.reference
            || object.state != ObjectState::Live
            || member.reference != guard.reference
            || member.journal_id != guard.journal_id
            || member.incarnation != guard.incarnation
            || member.ordinal != guard.ordinal
            || member.put_id != guard.put_id
            || member.adopted
            || values[3].as_deref()
                != Some(
                    guard
                        .reference
                        .encode_value()
                        .map_err(journal_error)?
                        .as_slice(),
                )
            || !member.pending_put
            || member.dispatched
            || !member.retained
        {
            return Err(WorkspaceError::Fenced);
        }
        member.dispatched = true;
        let changes = [
            (keys[0].clone(), values[0].clone(), values[0].clone()),
            (keys[1].clone(), values[1].clone(), Some(member.encode()?)),
            (keys[2].clone(), values[2].clone(), values[2].clone()),
            (keys[3].clone(), values[3].clone(), values[3].clone()),
        ];
        self.packed_journal_write_under(Some(expected), &expected.next()?, &changes, None, native)
            .await
            .retain(owner)
    }

    /// Call only after this guard's actual create-only PUT returned success.
    /// An error/cancellation retains the pending hold; HEAD absence is not proof.
    pub(crate) async fn finish_packed_upload(
        &self,
        expected: &PackedJournalRecord,
        guard: PackedUploadGuard,
        budget: &Arc<SnapshotMountBudget>,
    ) -> Result<OwnedPackedJournal<PackedJournalRecord>, WorkspaceError> {
        self.finish_packed_upload_under(expected, guard, budget, None)
            .await
    }

    pub(super) async fn finish_packed_upload_under(
        &self,
        expected: &PackedJournalRecord,
        guard: PackedUploadGuard,
        budget: &Arc<SnapshotMountBudget>,
        native: Option<&super::native_publication::NativeJournalAuthority<'_, B>>,
    ) -> Result<OwnedPackedJournal<PackedJournalRecord>, WorkspaceError> {
        let operation_owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, OPERATION_BYTES)])
            .map_err(journal_budget_error)?;
        if expected.phase != PackedJournalPhase::Building
            || guard.journal_id != expected.journal_id
            || guard.incarnation != expected.source.staging_id
        {
            return Err(WorkspaceError::Fenced);
        }
        let keys = [
            registry_object_key(&guard.reference),
            registry_member_key(&guard.reference, guard.incarnation),
            registry_root_key(guard.incarnation),
            registry_reverse_key(guard.incarnation, guard.ordinal),
        ];
        let values = self.packed_journal_values(&keys).await?;
        if values.len() != 4 {
            return Err(journal_error("short registry completion read"));
        }
        let mut object = ObjectRow::decode(
            values[0]
                .as_deref()
                .ok_or_else(|| journal_error("pending object missing"))?,
        )?;
        let mut member = MemberRow::decode(
            values[1]
                .as_deref()
                .ok_or_else(|| journal_error("pending membership missing"))?,
        )?;
        let mut root = RootRow::decode(
            values[2]
                .as_deref()
                .ok_or_else(|| journal_error("pending root missing"))?,
        )?;
        root.check_staging(expected)?;
        if object.reference != guard.reference
            || object.state != ObjectState::Live
            || member.reference != guard.reference
            || member.journal_id != guard.journal_id
            || member.incarnation != guard.incarnation
            || member.ordinal != guard.ordinal
            || member.put_id != guard.put_id
            || member.adopted
            || values[3].as_deref()
                != Some(
                    guard
                        .reference
                        .encode_value()
                        .map_err(journal_error)?
                        .as_slice(),
                )
            || !member.pending_put
            || !member.dispatched
            || !member.retained
        {
            return Err(WorkspaceError::Fenced);
        }
        let occurrence = self
            .reopen_packed_object(expected, guard.ordinal, budget)
            .await?;
        if occurrence.reference != guard.reference
            || occurrence.uploaded
            || occurrence.readback_recorded
        {
            return Err(WorkspaceError::Fenced);
        }
        let old_occurrence = occurrence.encode()?;
        let mut uploaded = occurrence.value.clone();
        uploaded.uploaded = true;
        object.revision = increment(object.revision)?;
        object.pending_puts = decrement(object.pending_puts)?;
        root.revision = increment(root.revision)?;
        root.pending_puts = decrement(root.pending_puts)?;
        member.pending_put = false;
        let changes = vec![
            (keys[0].clone(), values[0].clone(), Some(object.encode()?)),
            (keys[1].clone(), values[1].clone(), Some(member.encode()?)),
            (keys[2].clone(), values[2].clone(), Some(root.encode()?)),
            (keys[3].clone(), values[3].clone(), values[3].clone()),
            (
                object_key(expected.journal_id, guard.ordinal),
                Some(old_occurrence),
                Some(uploaded.encode()?),
            ),
        ];
        self.packed_journal_write_under(Some(expected), &expected.next()?, &changes, None, native)
            .await
            .retain(operation_owner)
    }

    pub(super) async fn verify_registry_graph_member(
        &self,
        expected: &PackedJournalRecord,
        reference: &SnapshotObjectRef,
        ordinal: u64,
    ) -> Result<(), WorkspaceError> {
        let keys = [
            registry_object_key(reference),
            registry_member_key(reference, expected.source.staging_id),
            registry_root_key(expected.source.staging_id),
            registry_reverse_key(expected.source.staging_id, ordinal),
        ];
        let values = self.packed_journal_values(&keys).await?;
        if values.len() != 4 {
            return Err(journal_error("short registry graph read"));
        }
        let object = ObjectRow::decode(
            values[0]
                .as_deref()
                .ok_or_else(|| journal_error("unregistered graph object"))?,
        )?;
        let member = MemberRow::decode(
            values[1]
                .as_deref()
                .ok_or_else(|| journal_error("unretained graph membership"))?,
        )?;
        let root = RootRow::decode(
            values[2]
                .as_deref()
                .ok_or_else(|| journal_error("unretained graph root"))?,
        )?;
        root.check_staging(expected)?;
        if root.pending_puts != 0
            || object.reference != *reference
            || object.state != ObjectState::Live
            || object.memberships == 0
            || member.reference != *reference
            || member.pending_put
            || !member.retained
            || member.incarnation != expected.source.staging_id
            || member.journal_id != expected.journal_id
            || member.adopted
            || values[3].as_deref()
                != Some(reference.encode_value().map_err(journal_error)?.as_slice())
        {
            return Err(WorkspaceError::Fenced);
        }
        if member.ordinal != ordinal {
            return Err(WorkspaceError::Fenced);
        }
        Ok(())
    }
}

#[cfg(target_os = "linux")]
mod native_quarantine;
#[cfg(target_os = "linux")]
mod native_upload_resume;
