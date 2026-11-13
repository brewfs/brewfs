//! Mandatory scoped packed writer checks in ordinary native/packed CAS.

use super::packed_writer_authority::{PackedWriterAuthority, PackedWriterOwner, packed_writer_key};
use super::*;
use crate::workspace::packed::snapshot::{SnapshotBudgetPool, SnapshotOwnedPermit};
use crate::workspace::stores::kv_backend::KvReadLimits;

const READ_BYTES: usize = 64 << 10;
const READ_OWNER_BYTES: u64 = 1 << 20;

pub(super) struct OrdinaryWriterChecks {
    pub(super) deadline: Option<i64>,
    pub(super) _owner: Option<SnapshotOwnedPermit>,
}

fn absence_keys(workspace: WorkspaceId) -> [Vec<u8>; 4] {
    [
        packed_current_key(workspace),
        packed_claim_key(workspace),
        packed_history_key(workspace, 1),
        packed_writer_key(workspace),
    ]
}

fn checked_raw<'a>(checks: &'a [KvCheck], key: &[u8]) -> Result<Option<&'a [u8]>, WorkspaceError> {
    checks
        .iter()
        .find(|check| check.key.as_slice() == key)
        .map(|check| check.expected.as_deref())
        .ok_or(WorkspaceError::Fenced)
}

fn merge_checks(target: &mut Vec<KvCheck>, source: Vec<KvCheck>) -> Result<(), WorkspaceError> {
    for check in source {
        if let Some(previous) = target.iter().find(|previous| previous.key == check.key) {
            if previous.expected != check.expected {
                return Err(WorkspaceError::Busy);
            }
        } else {
            target.push(check);
        }
    }
    Ok(())
}

/// Every input row belongs to the operation's final exact-check packet. This
/// validates identity; it never constructs authority or performs a mutation.
pub(super) fn authenticate_ordinary_packed_writer(
    backend_identity: usize,
    budget: Option<&Arc<crate::workspace::packed::snapshot::SnapshotMountBudget>>,
    checks: &[KvCheck],
    guard: &HeadGuard,
    lease: &SnapshotLease,
    now: i64,
) -> Result<PackedWriterAuthority, WorkspaceError> {
    if let Some(writer) = packed_admin::authenticate_mounted_recovery_writer(
        backend_identity,
        budget,
        checks,
        guard,
        lease,
        now,
    )? {
        return Ok(writer);
    }
    let writer = PackedWriterAuthority::decode(
        checked_raw(checks, &packed_writer_key(guard.workspace_id))?.ok_or(
            WorkspaceError::UnsupportedCapability("mandatory packed writer authority"),
        )?,
        guard.workspace_id,
    )?;
    if lease.workspace_id != guard.workspace_id
        || lease.lease_id != guard.lease_id
        || lease.holder_generation != guard.holder_generation
        || !lease.writable
        || lease.state != LeaseState::Active
        || now <= 0
        || lease.expires_at_ns <= now
    {
        return Err(WorkspaceError::Fenced);
    }
    if let Some(raw) = checked_raw(checks, &open_snapshot_recovery_key(guard.workspace_id))? {
        let recovery: SnapshotRecoveryRecord = decode_open_value(raw, OPEN_RECOVERY_MAX_BYTES)?;
        if recovery.workspace_id != guard.workspace_id || recovery.incomplete {
            return Err(WorkspaceError::Fenced);
        }
    }
    let open = checked_raw(checks, &open_snapshot_key(guard.workspace_id))?
        .map(|raw| decode_open_value::<SnapshotOpenRecord>(raw, OPEN_RECORD_MAX_BYTES))
        .transpose()?;
    if let Some(open) = &open {
        validate_open_record(open, guard.workspace_id)?;
        if open.state != SnapshotOpenState::Ready || open.recovery_required {
            return Err(WorkspaceError::Fenced);
        }
    }
    match writer.owner.as_ref() {
        Some(PackedWriterOwner::InitialSource {
            lease_id,
            holder_generation,
        }) if *lease_id == guard.lease_id && *holder_generation == guard.holder_generation => {
            if open.as_ref().is_some_and(|open| open.expires_at_ns > now) {
                return Err(WorkspaceError::Fenced);
            }
        }
        Some(PackedWriterOwner::Mounted {
            lease_id,
            holder_generation,
            open_owner,
            open_generation,
        }) if *lease_id == guard.lease_id && *holder_generation == guard.holder_generation => {
            let open = open.as_ref().ok_or(WorkspaceError::Fenced)?;
            if open.owner_id != *open_owner
                || open.generation != *open_generation
                || open.expires_at_ns != lease.expires_at_ns
            {
                return Err(WorkspaceError::Fenced);
            }
        }
        // Administrative publication/recovery owners may not issue ordinary
        // VFS writes, even if a caller copied their native lease identity.
        _ => return Err(WorkspaceError::Fenced),
    }
    Ok(writer)
}

impl<B: WorkspaceKvBackend> KvWorkspaceStore<B> {
    pub(super) async fn has_packed_writer_mount_binding(
        &self,
        workspace: WorkspaceId,
    ) -> Result<bool, WorkspaceError> {
        let (_owner, _checks, binding, _) = self.ordinary_writer_snapshot(workspace).await?;
        Ok(binding.is_some())
    }

    /// Native callers without a packed ledger only receive exact absence
    /// conditions; they never borrow a default ledger to read packed authority.
    async fn ordinary_writer_snapshot(
        &self,
        workspace: WorkspaceId,
    ) -> Result<
        (
            Option<SnapshotOwnedPermit>,
            Vec<KvCheck>,
            Option<PackedLowerBindingRecord>,
            i64,
        ),
        WorkspaceError,
    > {
        let Some(budget) = self.packed_reader_pin_budget.get() else {
            return Ok((
                None,
                absence_keys(workspace)
                    .into_iter()
                    .map(|key| KvCheck {
                        key,
                        expected: None,
                    })
                    .collect(),
                None,
                0,
            ));
        };
        let owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, READ_OWNER_BYTES)])
            .map_err(|error| WorkspaceError::InvalidReadPlan(error.to_string()))?;
        let mut keys = absence_keys(workspace).to_vec();
        keys.extend([
            open_snapshot_key(workspace),
            open_snapshot_recovery_key(workspace),
        ]);
        let limits = |count| KvReadLimits {
            max_records: count,
            max_key_bytes: 256,
            max_value_bytes: 48 << 10,
            max_total_bytes: READ_BYTES,
            max_response_bytes: 64 << 10,
            max_data_requests: count,
        };
        let (values, now) = self
            .backend
            .get_many_consistent_with_time_bounded(&keys, limits(keys.len()))
            .await?;
        if values.len() != keys.len() || now <= 0 {
            return Err(WorkspaceError::Fenced);
        }
        let mut checks = keys
            .into_iter()
            .zip(values)
            .map(|(key, expected)| KvCheck { key, expected })
            .collect::<Vec<_>>();
        let current = checked_raw(&checks, &packed_current_key(workspace))?
            .map(PackedLowerBindingRecord::decode)
            .transpose()?;
        let Some(binding) = current else {
            if absence_keys(workspace)
                .iter()
                .any(|key| !matches!(checked_raw(&checks, key), Ok(None)))
            {
                return Err(WorkspaceError::Fenced);
            }
            checks.retain(|check| absence_keys(workspace).contains(&check.key));
            return Ok((Some(owner), checks, None, now));
        };
        if binding.workspace_id != workspace {
            return Err(WorkspaceError::Fenced);
        }
        let history_key = packed_history_key(workspace, binding.binding.binding_version);
        if !checks.iter().any(|check| check.key == history_key) {
            let (values, later) = self
                .backend
                .get_many_consistent_with_time_bounded(
                    std::slice::from_ref(&history_key),
                    limits(1),
                )
                .await?;
            if values.len() != 1 || later <= 0 {
                return Err(WorkspaceError::Fenced);
            }
            checks.push(KvCheck {
                key: history_key,
                expected: values.into_iter().next().flatten(),
            });
        }
        let value = |key: Vec<u8>| -> Result<Option<Vec<u8>>, WorkspaceError> {
            Ok(checked_raw(&checks, &key)?.map(<[u8]>::to_vec))
        };
        let actual = decode_packed_pair(
            workspace,
            &value(packed_current_key(workspace))?,
            &value(packed_claim_key(workspace))?,
            &value(packed_history_key(
                workspace,
                binding.binding.binding_version,
            ))?,
        )?
        .ok_or(WorkspaceError::Fenced)?;
        let initial = PackedLowerBindingRecord::decode(
            checked_raw(&checks, &packed_history_key(workspace, 1))?
                .ok_or(WorkspaceError::Fenced)?,
        )?;
        if actual != binding
            || initial.workspace_id != workspace
            || initial.binding.binding_version != 1
        {
            return Err(WorkspaceError::Fenced);
        }
        Ok((Some(owner), checks, Some(binding), now))
    }

    pub(super) async fn prepare_ordinary_writer_checks(
        &self,
        guard: &HeadGuard,
        lease: &SnapshotLease,
        checks: &mut Vec<KvCheck>,
    ) -> Result<OrdinaryWriterChecks, WorkspaceError> {
        let (owner, added, binding, now) =
            self.ordinary_writer_snapshot(guard.workspace_id).await?;
        merge_checks(checks, added)?;
        let deadline = if let Some(binding) = binding {
            if binding.head_layer_id != guard.expected_head_layer_id
                || binding.head_epoch != guard.expected_head_epoch
                || binding.base_revision != lease.base_revision
            {
                return Err(WorkspaceError::Fenced);
            }
            authenticate_ordinary_packed_writer(
                Arc::as_ptr(&self.backend) as usize,
                self.packed_reader_pin_budget.get(),
                checks,
                guard,
                lease,
                now,
            )?;
            Some(lease.expires_at_ns)
        } else {
            None
        };
        Ok(OrdinaryWriterChecks {
            deadline,
            _owner: owner,
        })
    }

    /// Plain lifecycle is available only to the genuine first-install source.
    /// Joint mounted/admin owners must use their typed same-CAS lifecycle.
    pub(super) async fn prepare_plain_lease_transition(
        &self,
        lease: &SnapshotLease,
        checks: &mut Vec<KvCheck>,
        writes: &mut Vec<KvWrite>,
        retire: bool,
    ) -> Result<OrdinaryWriterChecks, WorkspaceError> {
        let (owner, added, binding, now) =
            self.ordinary_writer_snapshot(lease.workspace_id).await?;
        merge_checks(checks, added)?;
        let deadline = if let Some(binding) = binding {
            let guard = HeadGuard {
                workspace_id: lease.workspace_id,
                expected_head_layer_id: binding.head_layer_id,
                expected_head_epoch: binding.head_epoch,
                lease_id: lease.lease_id,
                holder_generation: lease.holder_generation,
            };
            if binding.base_revision != lease.base_revision {
                return Err(WorkspaceError::Fenced);
            }
            let writer = authenticate_ordinary_packed_writer(
                Arc::as_ptr(&self.backend) as usize,
                self.packed_reader_pin_budget.get(),
                checks,
                &guard,
                lease,
                now,
            )?;
            if !matches!(&writer.owner, Some(PackedWriterOwner::InitialSource { .. })) {
                return Err(WorkspaceError::Fenced);
            }
            if retire {
                writes.push(KvWrite::Put {
                    key: packed_writer_key(lease.workspace_id),
                    value: writer.successor(None)?.encode()?,
                });
            }
            Some(lease.expires_at_ns)
        } else {
            None
        };
        Ok(OrdinaryWriterChecks {
            deadline,
            _owner: owner,
        })
    }
}
