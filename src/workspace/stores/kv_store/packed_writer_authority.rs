//! Private workspace-scoped packed writer incarnation.
//! Initialization joins the genuine initial-install or carrier-fork CAS.
//! This module does not enable any public mount capability.

use super::*;
use crate::workspace::packed::snapshot::{SnapshotBudgetPool, SnapshotOwnedPermit};
use crate::workspace::stores::kv_backend::KvReadLimits;

const WRITER_MAGIC: &[u8; 5] = b"PWA3\x01";
const WRITER_BYTES: usize = 4096;
// Covers both bounded reads, retained exact rows, decoding, and successor copies.
const WRITER_READ_ADMISSION: u64 = 256 << 10;

pub(super) fn packed_writer_key(workspace: WorkspaceId) -> Vec<u8> {
    format!("packed/writer/{workspace}").into_bytes()
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) enum PackedWriterOwner {
    // Only the genuine first install may carry its already-issued native lease.
    // This is not a general native-to-packed writer-grant fallback.
    InitialSource {
        lease_id: LeaseId,
        holder_generation: u64,
    },
    Administrative {
        lease_id: LeaseId,
        holder_generation: u64,
        open_owner: String,
        open_generation: u64,
        recovering: bool,
    },
    Mounted {
        lease_id: LeaseId,
        holder_generation: u64,
        open_owner: String,
        open_generation: u64,
    },
}

impl PackedWriterOwner {
    pub(super) fn lease_identity(&self) -> (LeaseId, u64) {
        match self {
            Self::InitialSource {
                lease_id,
                holder_generation,
            }
            | Self::Mounted {
                lease_id,
                holder_generation,
                ..
            }
            | Self::Administrative {
                lease_id,
                holder_generation,
                ..
            } => (*lease_id, *holder_generation),
        }
    }
    fn validate(&self) -> Result<(), WorkspaceError> {
        let (lease, generation) = self.lease_identity();
        if lease.as_uuid().is_nil() || generation == 0 {
            return Err(WorkspaceError::Fenced);
        }
        if let Self::Mounted {
            open_owner,
            open_generation,
            ..
        }
        | Self::Administrative {
            open_owner,
            open_generation,
            ..
        } = self
            && (open_owner.trim().is_empty()
                || open_owner.len() > OPEN_OWNER_MAX_BYTES
                || *open_generation == 0)
        {
            return Err(WorkspaceError::Fenced);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PackedWriterAuthority {
    pub(super) workspace_id: WorkspaceId,
    pub(super) incarnation: u64,
    pub(super) owner: Option<PackedWriterOwner>,
}

impl PackedWriterAuthority {
    fn validate(&self, workspace: WorkspaceId) -> Result<(), WorkspaceError> {
        if self.workspace_id != workspace || workspace.as_uuid().is_nil() || self.incarnation == 0 {
            return Err(WorkspaceError::Fenced);
        }
        if let Some(owner) = &self.owner {
            owner.validate()?;
        }
        Ok(())
    }
    pub(super) fn encode(&self) -> Result<Vec<u8>, WorkspaceError> {
        self.validate(self.workspace_id)?;
        let mut bytes = WRITER_MAGIC.to_vec();
        bytes.extend(serde_json::to_vec(self).map_err(|_| WorkspaceError::Fenced)?);
        if bytes.len() > WRITER_BYTES {
            return Err(WorkspaceError::Fenced);
        }
        Ok(bytes)
    }
    pub(super) fn decode(bytes: &[u8], workspace: WorkspaceId) -> Result<Self, WorkspaceError> {
        if bytes.len() > WRITER_BYTES || !bytes.starts_with(WRITER_MAGIC) {
            return Err(WorkspaceError::Fenced);
        }
        let value: Self = serde_json::from_slice(&bytes[WRITER_MAGIC.len()..])
            .map_err(|_| WorkspaceError::Fenced)?;
        value.validate(workspace)?;
        if value.encode()? != bytes {
            return Err(WorkspaceError::Fenced);
        }
        Ok(value)
    }
    pub(super) fn successor(
        &self,
        owner: Option<PackedWriterOwner>,
    ) -> Result<Self, WorkspaceError> {
        let next = Self {
            workspace_id: self.workspace_id,
            incarnation: self
                .incarnation
                .checked_add(1)
                .ok_or(WorkspaceError::Fenced)?,
            owner,
        };
        next.validate(self.workspace_id)?;
        Ok(next)
    }
}

impl<B: WorkspaceKvBackend> KvWorkspaceStore<B> {
    async fn read_packed_writer_initial_absence(
        &self,
        workspace: WorkspaceId,
    ) -> Result<(SnapshotOwnedPermit, KvCheck), WorkspaceError> {
        let budget =
            self.packed_reader_pin_budget
                .get()
                .ok_or(WorkspaceError::UnsupportedCapability(
                    "canonical packed writer budget",
                ))?;
        let owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, WRITER_READ_ADMISSION)])
            .map_err(|error| WorkspaceError::InvalidReadPlan(error.to_string()))?;
        let key = packed_writer_key(workspace);
        let (values, now) = self
            .backend
            .get_many_consistent_with_time_bounded(
                std::slice::from_ref(&key),
                KvReadLimits {
                    max_records: 1,
                    max_key_bytes: 256,
                    max_value_bytes: WRITER_BYTES,
                    max_total_bytes: WRITER_BYTES + 256,
                    max_response_bytes: 16 << 10,
                    max_data_requests: 1,
                },
            )
            .await?;
        if values.len() != 1 || now <= 0 || values[0].is_some() {
            return Err(WorkspaceError::Fenced);
        }
        Ok((
            owner,
            KvCheck {
                key,
                expected: None,
            },
        ))
    }

    /// The caller retains the returned owner through its real install CAS.
    /// No new transaction or standalone writer initialization is performed.
    pub(super) async fn prepare_initial_packed_writer_authority(
        &self,
        binding: &PackedLowerBindingRecord,
        guard: &HeadGuard,
        checks: &mut Vec<KvCheck>,
        writes: &mut Vec<KvWrite>,
    ) -> Result<SnapshotOwnedPermit, WorkspaceError> {
        if binding.workspace_id != guard.workspace_id
            || binding.head_layer_id != guard.expected_head_layer_id
            || binding.binding.binding_version != 1
            || binding.head_epoch
                != guard
                    .expected_head_epoch
                    .checked_add(1)
                    .ok_or(WorkspaceError::Fenced)?
        {
            return Err(WorkspaceError::Fenced);
        }
        let lease_key = hot_lease_key(guard.workspace_id, guard.lease_id);
        let lease: SnapshotLease = checks
            .iter()
            .find(|check| check.key == lease_key)
            .and_then(|check| check.expected.as_deref())
            .map(decode)
            .transpose()?
            .ok_or(WorkspaceError::Fenced)?;
        if lease.lease_id != guard.lease_id
            || lease.workspace_id != guard.workspace_id
            || lease.holder_generation != guard.holder_generation
            || lease.state != LeaseState::Active
            || !lease.writable
            || lease.base_revision != binding.base_revision
        {
            return Err(WorkspaceError::Fenced);
        }
        let (owner, check) = self
            .read_packed_writer_initial_absence(guard.workspace_id)
            .await?;
        if checks.iter().any(|existing| existing.key == check.key)
            || writes.iter().any(|write| match write {
                KvWrite::Put { key, .. } | KvWrite::Delete { key } => *key == check.key,
            })
        {
            return Err(WorkspaceError::Fenced);
        }
        let authority = PackedWriterAuthority {
            workspace_id: guard.workspace_id,
            incarnation: 1,
            owner: Some(PackedWriterOwner::InitialSource {
                lease_id: guard.lease_id,
                holder_generation: guard.holder_generation,
            }),
        };
        writes.push(KvWrite::Put {
            key: check.key.clone(),
            value: authority.encode()?,
        });
        checks.push(check);
        Ok(owner)
    }

    /// A genuine packed carrier fork starts with no writable owner. Its native
    /// birth, current/claim/history/alias and this row join one original CAS.
    pub(super) async fn prepare_fork_packed_writer_authority(
        &self,
        workspace: WorkspaceId,
        checks: &mut Vec<KvCheck>,
        writes: &mut Vec<KvWrite>,
    ) -> Result<SnapshotOwnedPermit, WorkspaceError> {
        let (owner, check) = self.read_packed_writer_initial_absence(workspace).await?;
        if checks.iter().any(|existing| existing.key == check.key)
            || writes.iter().any(|write| match write {
                KvWrite::Put { key, .. } | KvWrite::Delete { key } => *key == check.key,
            })
        {
            return Err(WorkspaceError::Fenced);
        }
        let authority = PackedWriterAuthority {
            workspace_id: workspace,
            incarnation: 1,
            owner: None,
        };
        writes.push(KvWrite::Put {
            key: check.key.clone(),
            value: authority.encode()?,
        });
        checks.push(check);
        Ok(owner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idle_regrant_and_retirement_preserve_monotone_writer_incarnation() {
        let workspace = WorkspaceId::new();
        let idle = PackedWriterAuthority {
            workspace_id: workspace,
            incarnation: 1,
            owner: None,
        };
        let first = idle
            .successor(Some(PackedWriterOwner::Mounted {
                lease_id: LeaseId::new(),
                holder_generation: 2,
                open_owner: "packed-mount-owner".into(),
                open_generation: 3,
            }))
            .unwrap();
        let retired = first.successor(None).unwrap();
        let regranted = retired.successor(first.owner.clone()).unwrap();
        assert_eq!(retired.incarnation, 3);
        assert_eq!(regranted.incarnation, 4);
        assert_ne!(regranted.encode().unwrap(), first.encode().unwrap());
        assert_eq!(
            PackedWriterAuthority::decode(&first.encode().unwrap(), workspace).unwrap(),
            first
        );
        assert!(
            PackedWriterAuthority::decode(&first.encode().unwrap(), WorkspaceId::new()).is_err()
        );
    }

    #[test]
    fn writer_corruption_or_exhausted_incarnation_never_becomes_idle() {
        let workspace = WorkspaceId::new();
        let idle = PackedWriterAuthority {
            workspace_id: workspace,
            incarnation: u64::MAX,
            owner: None,
        };
        assert!(idle.successor(None).is_err());
        assert!(PackedWriterAuthority::decode(b"PWA3\x01{}", workspace).is_err());
        assert!(PackedWriterAuthority::decode(&vec![0; WRITER_BYTES + 1], workspace).is_err());
        let nil = PackedWriterAuthority {
            workspace_id: workspace,
            incarnation: 1,
            owner: Some(PackedWriterOwner::InitialSource {
                lease_id: LeaseId::from_uuid(uuid::Uuid::nil()),
                holder_generation: 1,
            }),
        };
        assert!(nil.encode().is_err());
        let malformed = PackedWriterAuthority {
            workspace_id: workspace,
            incarnation: 1,
            owner: Some(PackedWriterOwner::Mounted {
                lease_id: LeaseId::new(),
                holder_generation: 1,
                open_owner: " ".into(),
                open_generation: 1,
            }),
        };
        assert!(malformed.encode().is_err());
    }
}

/// These modes are reachable only from the existing typed source and terminal
/// drivers. They never authorize an ordinary mutation or an expired mount.
#[derive(Clone, Copy)]
pub(super) enum AdministrativeWriterTransition {
    ClaimClean,
    ClaimInitial,
    Update,
    Retire,
}

fn append_writer_check(checks: &mut Vec<KvCheck>, added: KvCheck) -> Result<(), WorkspaceError> {
    if let Some(current) = checks.iter().find(|check| check.key == added.key) {
        if current.expected != added.expected {
            return Err(WorkspaceError::Busy);
        }
    } else {
        checks.push(added);
    }
    Ok(())
}

fn checked_writer_value<'a>(checks: &'a [KvCheck], key: &[u8]) -> Result<&'a [u8], WorkspaceError> {
    checks
        .iter()
        .find(|check| check.key.as_slice() == key)
        .and_then(|check| check.expected.as_deref())
        .ok_or(WorkspaceError::Fenced)
}

fn successor_writer_value<'a>(
    checks: &'a [KvCheck],
    writes: &'a [KvWrite],
    key: &[u8],
) -> Result<&'a [u8], WorkspaceError> {
    match writes.iter().find(|write| match write {
        KvWrite::Put { key: written, .. } | KvWrite::Delete { key: written } => {
            written.as_slice() == key
        }
    }) {
        Some(KvWrite::Put { value, .. }) => Ok(value),
        Some(KvWrite::Delete { .. }) => Err(WorkspaceError::Fenced),
        None => checked_writer_value(checks, key),
    }
}

fn administrative_identity(
    lease: &SnapshotLease,
    open: &SnapshotOpenRecord,
) -> Result<PackedWriterOwner, WorkspaceError> {
    validate_open_record(open, lease.workspace_id)?;
    if !lease.writable || lease.state != LeaseState::Active {
        return Err(WorkspaceError::Fenced);
    }
    let owner = PackedWriterOwner::Administrative {
        lease_id: lease.lease_id,
        holder_generation: lease.holder_generation,
        open_owner: open.owner_id.clone(),
        open_generation: open.generation,
        recovering: open.recovery_required,
    };
    owner.validate()?;
    Ok(owner)
}

impl<B: WorkspaceKvBackend> KvWorkspaceStore<B> {
    async fn read_packed_writer_row(
        &self,
        workspace: WorkspaceId,
        checks: &mut Vec<KvCheck>,
    ) -> Result<(Option<SnapshotOwnedPermit>, Option<PackedWriterAuthority>), WorkspaceError> {
        let Some(budget) = self.packed_reader_pin_budget.get() else {
            // Native public sidecars can only assert absence in their actual
            // CAS. They must not read/decode packed state without its ledger.
            append_writer_check(
                checks,
                KvCheck {
                    key: packed_writer_key(workspace),
                    expected: None,
                },
            )?;
            return Ok((None, None));
        };
        let owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, WRITER_READ_ADMISSION)])
            .map_err(|error| WorkspaceError::InvalidReadPlan(error.to_string()))?;
        let key = packed_writer_key(workspace);
        let (values, now) = self
            .backend
            .get_many_consistent_with_time_bounded(
                std::slice::from_ref(&key),
                KvReadLimits {
                    max_records: 1,
                    max_key_bytes: 256,
                    max_value_bytes: WRITER_BYTES,
                    max_total_bytes: WRITER_BYTES + 256,
                    max_response_bytes: 16 << 10,
                    max_data_requests: 1,
                },
            )
            .await?;
        if values.len() != 1 || now <= 0 {
            return Err(WorkspaceError::Fenced);
        }
        let writer = values[0]
            .as_deref()
            .map(|raw| PackedWriterAuthority::decode(raw, workspace))
            .transpose()?;
        append_writer_check(
            checks,
            KvCheck {
                key,
                expected: values[0].clone(),
            },
        )?;
        Ok((Some(owner), writer))
    }

    /// The exact predecessor PWA, lease and open all join the caller's CAS.
    /// Existing caller checks must agree with this bounded read byte for byte.
    async fn read_administrative_writer_context(
        &self,
        workspace: WorkspaceId,
        checks: &mut Vec<KvCheck>,
        allow_initial: bool,
    ) -> Result<(SnapshotOwnedPermit, PackedWriterAuthority), WorkspaceError> {
        self.packed_reader_pin_budget
            .get()
            .ok_or(WorkspaceError::UnsupportedCapability(
                "canonical packed writer budget",
            ))?;
        let (owner, writer) = self.read_packed_writer_row(workspace, checks).await?;
        let owner = owner.ok_or(WorkspaceError::Fenced)?;
        let writer = writer.ok_or(WorkspaceError::Fenced)?;
        let Some(identity) = &writer.owner else {
            return Ok((owner, writer));
        };
        let (lease_id, generation) = identity.lease_identity();
        let mut keys = vec![
            packed_writer_key(workspace),
            hot_lease_key(workspace, lease_id),
        ];
        match identity {
            PackedWriterOwner::Administrative { .. } => keys.push(open_snapshot_key(workspace)),
            PackedWriterOwner::InitialSource { .. } if allow_initial => {}
            _ => {
                #[cfg(test)]
                eprintln!(
                    "[packed-native-authority-diag] stage=writer-context-owner-kind error=Fenced"
                );
                return Err(WorkspaceError::Fenced);
            }
        }
        let (values, now) = self
            .backend
            .get_many_consistent_with_time_bounded(
                &keys,
                KvReadLimits {
                    max_records: keys.len(),
                    max_key_bytes: 256,
                    max_value_bytes: 12 << 10,
                    max_total_bytes: 48 << 10,
                    max_response_bytes: 64 << 10,
                    max_data_requests: keys.len(),
                },
            )
            .await?;
        if values.len() != keys.len() || now <= 0 {
            return Err(WorkspaceError::Fenced);
        }
        for (key, expected) in keys.into_iter().zip(values) {
            append_writer_check(checks, KvCheck { key, expected }).inspect_err(|_error| {
                #[cfg(test)]
                super::packed_native_freeze::native_authority_diagnostic(
                    "writer-context-overlap",
                    _error,
                );
            })?;
        }
        let lease: SnapshotLease = decode_open_value(
            checked_writer_value(checks, &hot_lease_key(workspace, lease_id))?,
            12 << 10,
        )?;
        if lease.workspace_id != workspace
            || lease.lease_id != lease_id
            || lease.holder_generation != generation
            || !lease.writable
            || !matches!(lease.state, LeaseState::Active | LeaseState::Expired)
        {
            #[cfg(test)]
            eprintln!(
                "[packed-native-authority-diag] stage=writer-context-lease-identity error=Fenced workspace={} lease={} holder={} writable={} retained_state={}",
                lease.workspace_id == workspace,
                lease.lease_id == lease_id,
                lease.holder_generation == generation,
                lease.writable,
                matches!(lease.state, LeaseState::Active | LeaseState::Expired),
            );
            return Err(WorkspaceError::Fenced);
        }
        if let PackedWriterOwner::Administrative {
            open_owner,
            open_generation,
            recovering,
            ..
        } = identity
        {
            let open: SnapshotOpenRecord = decode_open_value(
                checked_writer_value(checks, &open_snapshot_key(workspace))?,
                OPEN_RECORD_MAX_BYTES,
            )?;
            validate_open_record(&open, workspace)?;
            if open.owner_id != *open_owner
                || open.generation != *open_generation
                || open.recovery_required != *recovering
            {
                #[cfg(test)]
                eprintln!(
                    "[packed-native-authority-diag] stage=writer-context-open-identity error=Fenced owner={} generation={} recovering={}",
                    open.owner_id == *open_owner,
                    open.generation == *open_generation,
                    open.recovery_required == *recovering,
                );
                return Err(WorkspaceError::Fenced);
            }
        }
        Ok((owner, writer))
    }

    /// Caller-owned typed source permits cover the returned fixed authority
    /// packet after this read owner is released; mutation callers retain it
    /// through their actual CAS and any exact successor confirmation.
    pub(super) async fn authenticate_administrative_packed_writer(
        &self,
        workspace: WorkspaceId,
        checks: &mut Vec<KvCheck>,
    ) -> Result<SnapshotOwnedPermit, WorkspaceError> {
        let (owner, writer) = self
            .read_administrative_writer_context(workspace, checks, false)
            .await?;
        if !matches!(writer.owner, Some(PackedWriterOwner::Administrative { .. })) {
            #[cfg(test)]
            eprintln!(
                "[packed-native-authority-diag] stage=writer-context-administrative-owner error=Fenced"
            );
            return Err(WorkspaceError::Fenced);
        }
        Ok(owner)
    }

    pub(super) async fn prepare_administrative_packed_writer(
        &self,
        workspace: WorkspaceId,
        transition: AdministrativeWriterTransition,
        checks: &mut Vec<KvCheck>,
        writes: &mut Vec<KvWrite>,
    ) -> Result<SnapshotOwnedPermit, WorkspaceError> {
        let allow_initial = matches!(transition, AdministrativeWriterTransition::ClaimInitial);
        let (owner, writer) = self
            .read_administrative_writer_context(workspace, checks, allow_initial)
            .await?;
        match (transition, &writer.owner) {
            (AdministrativeWriterTransition::ClaimClean, None)
            | (
                AdministrativeWriterTransition::ClaimInitial,
                Some(PackedWriterOwner::InitialSource { .. }),
            )
            | (
                AdministrativeWriterTransition::Update | AdministrativeWriterTransition::Retire,
                Some(PackedWriterOwner::Administrative { .. }),
            ) => {}
            _ => return Err(WorkspaceError::Fenced),
        }
        let open_key = open_snapshot_key(workspace);
        let next_open: SnapshotOpenRecord = decode_open_value(
            successor_writer_value(checks, writes, &open_key)?,
            OPEN_RECORD_MAX_BYTES,
        )?;
        validate_open_record(&next_open, workspace)?;
        let next_owner = if matches!(transition, AdministrativeWriterTransition::Retire) {
            let current = writer.owner.as_ref().ok_or(WorkspaceError::Fenced)?;
            let (lease_id, generation) = current.lease_identity();
            let lease: SnapshotLease = decode_open_value(
                successor_writer_value(checks, writes, &hot_lease_key(workspace, lease_id))?,
                12 << 10,
            )?;
            if lease.workspace_id != workspace
                || lease.lease_id != lease_id
                || !lease.writable
                || lease.holder_generation != generation
                || lease.state != LeaseState::Released
                || next_open.expires_at_ns != lease.updated_at_ns
            {
                return Err(WorkspaceError::Fenced);
            }
            if let PackedWriterOwner::Administrative {
                open_owner,
                open_generation,
                ..
            } = current
                && (next_open.owner_id != *open_owner || next_open.generation != *open_generation)
            {
                return Err(WorkspaceError::Fenced);
            }
            None
        } else {
            let mut active = None;
            for write in writes.iter() {
                if let KvWrite::Put { key, value } = write
                    && key.starts_with(HOT_LEASE_PREFIX)
                {
                    let lease: SnapshotLease = decode_open_value(value, 12 << 10)?;
                    if *key != hot_lease_key(lease.workspace_id, lease.lease_id) {
                        return Err(WorkspaceError::Fenced);
                    }
                    if lease.workspace_id == workspace
                        && lease.writable
                        && lease.state == LeaseState::Active
                        && active.replace(lease).is_some()
                    {
                        return Err(WorkspaceError::Fenced);
                    }
                }
            }
            let lease = match active {
                Some(lease) => lease,
                None => {
                    let (lease_id, _) = writer
                        .owner
                        .as_ref()
                        .ok_or(WorkspaceError::Fenced)?
                        .lease_identity();
                    decode_open_value(
                        successor_writer_value(
                            checks,
                            writes,
                            &hot_lease_key(workspace, lease_id),
                        )?,
                        12 << 10,
                    )?
                }
            };
            if !checks
                .iter()
                .any(|check| check.key == hot_lease_key(lease.workspace_id, lease.lease_id))
            {
                return Err(WorkspaceError::Fenced);
            }
            if matches!(transition, AdministrativeWriterTransition::ClaimInitial) {
                let original = writer
                    .owner
                    .as_ref()
                    .ok_or(WorkspaceError::Fenced)?
                    .lease_identity();
                if (lease.lease_id, lease.holder_generation) != original
                    || next_open.recovery_required
                {
                    return Err(WorkspaceError::Fenced);
                }
                let old_open = checks
                    .iter()
                    .find(|check| check.key == open_key)
                    .ok_or(WorkspaceError::Fenced)?;
                if old_open.expected.is_some() {
                    return Err(WorkspaceError::Fenced);
                }
            }
            Some(administrative_identity(&lease, &next_open)?)
        };
        if writer.owner != next_owner {
            let key = packed_writer_key(workspace);
            if writes.iter().any(|write| match write {
                KvWrite::Put { key: written, .. } | KvWrite::Delete { key: written } => {
                    *written == key
                }
            }) {
                return Err(WorkspaceError::Fenced);
            }
            writes.push(KvWrite::Put {
                key,
                value: writer.successor(next_owner)?.encode()?,
            });
        }
        Ok(owner)
    }

    pub(super) async fn authenticate_initial_packed_writer(
        &self,
        guard: &HeadGuard,
        checks: &mut Vec<KvCheck>,
    ) -> Result<SnapshotOwnedPermit, WorkspaceError> {
        let (owner, writer) = self
            .read_administrative_writer_context(guard.workspace_id, checks, true)
            .await?;
        let identity = writer.owner.as_ref().ok_or(WorkspaceError::Fenced)?;
        if identity.lease_identity() != (guard.lease_id, guard.holder_generation) {
            return Err(WorkspaceError::Fenced);
        }
        Ok(owner)
    }

    /// Completed typed receipts require the persisted idle incarnation.
    /// Missing PWA is not an initialized packed workspace.
    pub(super) async fn authenticate_idle_packed_writer(
        &self,
        workspace: WorkspaceId,
        checks: &mut Vec<KvCheck>,
    ) -> Result<SnapshotOwnedPermit, WorkspaceError> {
        self.packed_reader_pin_budget
            .get()
            .ok_or(WorkspaceError::UnsupportedCapability(
                "canonical packed writer budget",
            ))?;
        let (owner, writer) = self.read_packed_writer_row(workspace, checks).await?;
        let writer = writer.ok_or(WorkspaceError::Fenced)?;
        if writer.owner.is_some() {
            return Err(WorkspaceError::Fenced);
        }
        owner.ok_or(WorkspaceError::Fenced)
    }

    /// Public sidecars cannot close, renew or finish any retained packed writer.
    pub(super) async fn prepare_public_open_idle_writer_check(
        &self,
        workspace: WorkspaceId,
        checks: &mut Vec<KvCheck>,
    ) -> Result<Option<SnapshotOwnedPermit>, WorkspaceError> {
        let (owner, writer) = self.read_packed_writer_row(workspace, checks).await?;
        if writer.is_some_and(|writer| writer.owner.is_some()) {
            return Err(WorkspaceError::Fenced);
        }
        Ok(owner)
    }

    /// A public open can preserve native/idle state, or advance the same typed
    /// administrative owner into a topology-proven recovery state. It cannot
    /// birth a lease, take over an expired mounted writer, or mark an admin ready.
    pub(super) async fn prepare_public_packed_open_transition(
        &self,
        workspace: WorkspaceId,
        context_recovery: bool,
        checks: &mut Vec<KvCheck>,
        writes: &mut Vec<KvWrite>,
    ) -> Result<Option<SnapshotOwnedPermit>, WorkspaceError> {
        let (owner, writer) = self.read_packed_writer_row(workspace, checks).await?;
        match writer.as_ref().and_then(|writer| writer.owner.as_ref()) {
            None => Ok(owner),
            Some(PackedWriterOwner::Administrative { open_owner, .. }) => {
                let open: SnapshotOpenRecord = decode_open_value(
                    successor_writer_value(checks, writes, &open_snapshot_key(workspace))?,
                    OPEN_RECORD_MAX_BYTES,
                )?;
                if !context_recovery
                    || !open.recovery_required
                    || open.state != SnapshotOpenState::Recovering
                    || open.owner_id != *open_owner
                {
                    return Err(WorkspaceError::Fenced);
                }
                let held = self
                    .prepare_administrative_packed_writer(
                        workspace,
                        AdministrativeWriterTransition::Update,
                        checks,
                        writes,
                    )
                    .await?;
                Ok(Some(held))
            }
            _ => Err(WorkspaceError::Fenced),
        }
    }
}
