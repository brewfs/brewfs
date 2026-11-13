//! A clean finish may rebuild only before sending its sole mutation, after a
//! complete fixed predecessor packet proves an independently advancing root.
use super::*;
use crate::workspace::stores::kv_store::native_read_conflict::{
    FirstRead, MAX_NATIVE_READ_ATTEMPTS, normalize,
};

impl<B: WorkspaceKvBackend> KvWorkspaceStore<B> {
    pub(crate) async fn prepare_clean_publication_native_owner_cas(
        &self,
        checks: &mut Vec<KvCheck>,
        writes: &mut Vec<KvWrite>,
        deadline: i64,
    ) -> Result<Option<SnapshotOwnedPermit>, WorkspaceError> {
        // The clean finish plans only persisted successors. Native sidecar
        // removals may be derived later, under the complete census proof.
        if writes
            .iter()
            .any(|write| matches!(write, KvWrite::Delete { .. }))
        {
            return Err(WorkspaceError::Fenced);
        }
        self.prepare_publication_owner_read_only_rebuild(checks, writes, deadline, false)
            .await
    }

    /// Sole opt-in for the actual Verified native publication final packet.
    /// This cannot be used for collection, physical deletion, or root retirement.
    pub(crate) async fn prepare_native_publication_owner_cas(
        &self,
        record: &PackedJournalRecord,
        checks: &mut Vec<KvCheck>,
        writes: &mut Vec<KvWrite>,
        deadline: i64,
    ) -> Result<Option<SnapshotOwnedPermit>, WorkspaceError> {
        let native = record
            .native_rebind
            .as_ref()
            .ok_or(WorkspaceError::Fenced)?;
        if record.phase != PackedJournalPhase::Verified || native.publication.is_none() {
            return Err(WorkspaceError::Fenced);
        }
        let expected_record = record.encode()?;
        if !checks.iter().any(|row| {
            row.key == journal_key(record.journal_id)
                && row.expected.as_deref() == Some(expected_record.as_slice())
        }) || !checks.iter().any(|row| {
            row.key
                == crate::workspace::stores::kv_store::packed_writer_authority::packed_writer_key(
                    record.guard.workspace_id,
                )
        }) {
            return Err(WorkspaceError::Fenced);
        }
        let deletable = [
            active_key(record.journal_id),
            open_snapshot_recovery_key(record.guard.workspace_id),
            format!("packed/native-recovery-claim/{}", native.native_journal_id).into_bytes(),
        ];
        if writes
            .iter()
            .any(|write| matches!(write, KvWrite::Delete { key } if !deletable.contains(key)))
        {
            return Err(WorkspaceError::Fenced);
        }
        self.prepare_publication_owner_read_only_rebuild(checks, writes, deadline, true)
            .await
    }

    async fn prepare_publication_owner_read_only_rebuild(
        &self,
        checks: &mut Vec<KvCheck>,
        writes: &mut Vec<KvWrite>,
        deadline: i64,
        root_increment_planned: bool,
    ) -> Result<Option<SnapshotOwnedPermit>, WorkspaceError> {
        let budget =
            self.packed_reader_pin_budget
                .get()
                .ok_or(WorkspaceError::UnsupportedCapability(
                    "canonical native hold budget",
                ))?;
        // The original native-owner allowance covers the bounded source,
        // first/fresh proof and planned successor copies before they exist.
        // The same permit survives the caller's eventual mutation transport.
        let rebuild_owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, HOLD_OPERATION_BYTES)])
            .map_err(journal_budget_error)?;
        if writes
            .iter()
            .try_fold(0usize, |sum, write| {
                let (key, value) = match write {
                    KvWrite::Put { key, value } => (key, value.len()),
                    KvWrite::Delete { key } => (key, 0),
                };
                sum.checked_add(key.len())
                    .and_then(|sum| sum.checked_add(value))
            })
            .is_none_or(|bytes| bytes > 2 << 20)
        {
            return Err(WorkspaceError::Busy);
        }
        let mut original = normalize(checks.clone())?;
        // Fix every ancestor before the first predecessor packet. A read-only
        // rebuild may refresh only the root epoch; it must not adopt a new
        // parent chain from a later snapshot after an epoch overlap.
        for change in owner_changes(&original, writes)? {
            if let Some(new) = &change.new {
                self.authenticate_native_hold_birth_layers(new, &mut original, writes)
                    .await?;
            }
        }
        original = normalize(original)?;
        let original_root = original
            .iter()
            .find(|row| row.key.as_slice() == PACKED_ROOT_GENERATION_KEY)
            .ok_or(WorkspaceError::Fenced)?;
        let root_writes = writes
            .iter()
            .filter(|write| match write {
                KvWrite::Put { key, .. } | KvWrite::Delete { key } => {
                    key.as_slice() == PACKED_ROOT_GENERATION_KEY
                }
            })
            .collect::<Vec<_>>();
        if root_increment_planned {
            let expected = encode(&next_packed_root_generation(&original_root.expected)?)?;
            if root_writes.len() != 1
                || !matches!(root_writes[0], KvWrite::Put { value, .. } if value == &expected)
            {
                return Err(WorkspaceError::Fenced);
            }
        } else if !root_writes.is_empty() {
            return Err(WorkspaceError::Fenced);
        }
        let mut keys = original
            .iter()
            .map(|row| row.key.clone())
            .collect::<Vec<_>>();
        // Fix the exact real sidecar keyset before any census. A later packet
        // must retain both absent births and existing lease/feature bytes.
        for key in std::iter::once(HOLD_FEATURE.to_vec()).chain(
            owner_changes(&original, writes)?
                .into_iter()
                .map(|change| change.key),
        ) {
            if !keys.contains(&key) {
                keys.push(key);
            }
        }
        if keys.len() > if root_increment_planned { 64 } else { 32 } || deadline <= 0 {
            return Err(WorkspaceError::Busy);
        }
        let initial_writes = writes.clone();
        let mut first = FirstRead::new();
        let mut previous_root = None;
        // Share the original census visit cap across every discarded attempt.
        let mut birth_root_visits = 0;
        for _attempt in 0..MAX_NATIVE_READ_ATTEMPTS {
            if budget.state().closed {
                return Err(WorkspaceError::Busy);
            }
            let limits = KvReadLimits {
                max_records: keys.len(),
                max_key_bytes: 1024,
                max_value_bytes: 48 << 10,
                max_total_bytes: if root_increment_planned {
                    2 << 20
                } else {
                    48 << 10
                },
                max_response_bytes: if root_increment_planned {
                    2 << 20
                } else {
                    64 << 10
                },
                max_data_requests: keys.len().saturating_add(2).min(if root_increment_planned {
                    64
                } else {
                    32
                }),
            };
            let (values, now) = if root_increment_planned {
                self.backend
                    .get_publication_packet_consistent_with_time_bounded(&keys, limits)
                    .await?
            } else {
                self.backend
                    .get_many_consistent_with_time_bounded(&keys, limits)
                    .await?
            };
            if values.len() != keys.len() || now <= 0 || now >= deadline {
                return Err(WorkspaceError::Fenced);
            }
            if budget.state().closed {
                return Err(WorkspaceError::Busy);
            }
            let fresh = normalize(
                keys.iter()
                    .cloned()
                    .zip(values)
                    .map(|(key, expected)| KvCheck { key, expected })
                    .collect(),
            )?;
            // Every original authority fact (including CONTROL, PWA, open,
            // receipt, source and inventory) must remain exactly unchanged.
            let root: u64 = decode(
                fresh
                    .iter()
                    .find(|row| row.key.as_slice() == PACKED_ROOT_GENERATION_KEY)
                    .and_then(|row| row.expected.as_deref())
                    .ok_or(WorkspaceError::Fenced)?,
            )?;
            let initial_root: u64 = decode(
                original_root
                    .expected
                    .as_deref()
                    .ok_or(WorkspaceError::Fenced)?,
            )?;
            if root < initial_root {
                return Err(WorkspaceError::Fenced);
            }
            if original.iter().any(|old| {
                !fresh.iter().any(|row| {
                    row.key == old.key
                        && (old.key.as_slice() == PACKED_ROOT_GENERATION_KEY
                            || row.expected == old.expected)
                })
            }) {
                return Err(WorkspaceError::Busy);
            }
            if previous_root.is_some_and(|previous| root <= previous) {
                return Err(WorkspaceError::Busy);
            }
            // The complete first packet additionally fixes all native holds;
            // no new key or changed/missing non-root byte can enter a rebuild.
            let (_, original_deadline) = first.observe(fresh.clone(), deadline, false, root)?;
            previous_root = Some(root);
            let mut candidate_checks = fresh;
            let mut candidate_writes = initial_writes.clone();
            if root_increment_planned {
                // The caller receives the whole rebuilt write packet. No
                // derived root successor from the old read may survive.
                for write in &mut candidate_writes {
                    if let KvWrite::Put { key, value } = write
                        && key.as_slice() == PACKED_ROOT_GENERATION_KEY
                    {
                        *value = encode(&root.checked_add(1).ok_or(WorkspaceError::Fenced)?)?;
                    }
                }
            }
            let prepared = self
                .prepare_native_owner_cas_inner(
                    &mut candidate_checks,
                    &mut candidate_writes,
                    &mut birth_root_visits,
                    Some(&rebuild_owner),
                )
                .await?;
            if budget.state().closed || original_deadline != deadline {
                return Err(WorkspaceError::Busy);
            }
            match prepared {
                NativeOwnerPreparation::Ready(owner) => {
                    debug_assert!(
                        owner.is_none(),
                        "inner must borrow the retained operation permit"
                    );
                    *checks = candidate_checks;
                    *writes = candidate_writes;
                    return Ok(Some(rebuild_owner));
                }
                NativeOwnerPreparation::EpochOverlap
                | NativeOwnerPreparation::BirthCompareFalse => {
                    #[cfg(test)]
                    eprintln!(
                        "[packed-native-begin-diag] stage=clean-finish-read-only-rebuild attempt={} root={} visited={}",
                        _attempt + 1,
                        root,
                        birth_root_visits,
                    );
                    // No mutation has been submitted. The next full read must
                    // prove strict root progress and all other bytes equal.
                }
            }
        }
        Err(WorkspaceError::Busy)
    }
}

#[cfg(test)]
#[path = "native_holds_clean_rebuild_tests.rs"]
mod tests;
