//! Distinct packed original-cutoff and original-PVC-recovery sources.
//! Recovery completion is immutable; its publication source owns only the
//! first administrative claim. Neither its bytes nor its facts are a PCR.

use super::*;
use std::ops::{Deref, DerefMut};

const RECOVERED_SOURCE_MAGIC: &[u8; 5] = b"PRS3\x01";
const RECOVERED_SOURCE_BYTES: usize = 12 << 10;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::workspace::stores::kv_store::packed_admin) enum CleanSourceKind {
    OriginalPcr,
    RecoveredPmr,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn completion() -> MountedRecoveryRecord {
        let original = RecoveryReference {
            workspace_id: WorkspaceId::from_uuid(uuid::Uuid::new_v4()),
            head_layer_id: LayerId::from_uuid(uuid::Uuid::new_v4()),
            head_epoch: 2,
            lease_id: LeaseId::from_uuid(uuid::Uuid::new_v4()),
            holder_generation: 2,
            mount_uid: uuid::Uuid::new_v4(),
            pod_uid: uuid::Uuid::new_v4(),
        };
        let current = RecoveryReference {
            lease_id: LeaseId::from_uuid(uuid::Uuid::new_v4()),
            holder_generation: 3,
            pod_uid: uuid::Uuid::new_v4(),
            ..original.clone()
        };
        let base_revision = BaseRevision {
            layer_id: LayerId::from_uuid(uuid::Uuid::new_v4()),
            sealed_version: 1,
            root_hash: [1; 32],
        };
        let binding_digest = [2; 32];
        let open_owner = recovery_open_owner(
            &original.reference(),
            &binding_digest,
            current.lease_id,
            current.pod_uid,
            current.holder_generation,
        )
        .unwrap();
        let original_lease = SnapshotLease {
            lease_id: original.lease_id,
            workspace_id: original.workspace_id,
            base_revision: base_revision.clone(),
            holder_generation: 2,
            writable: true,
            state: LeaseState::Released,
            expires_at_ns: 20,
            created_at_ns: 10,
            updated_at_ns: 30,
        };
        let lease = SnapshotLease {
            lease_id: current.lease_id,
            holder_generation: 3,
            expires_at_ns: 29,
            created_at_ns: 21,
            ..original_lease.clone()
        };
        let open = SnapshotOpenRecord {
            workspace_id: original.workspace_id,
            owner_id: open_owner.clone(),
            generation: 7,
            expires_at_ns: 30,
            state: SnapshotOpenState::Ready,
            recovery_required: false,
        };
        MountedRecoveryRecord {
            original,
            current,
            original_writer_incarnation: 4,
            writer_incarnation: 5,
            binding_digest,
            open_owner,
            open_generation: 7,
            completed: true,
            base_revision,
            binding_version: 1,
            manifest_digest: [3; 32],
            original_released_lease: Some(original_lease),
            released_lease: Some(lease),
            closed_open: Some(open),
            head_sequence: Some(9),
            first_admin_claim: None,
        }
    }

    fn unstarted_fixture() -> (
        PackedReleasedMountReference,
        BaseRevision,
        PackedWriterAuthority,
        SnapshotLease,
        SnapshotOpenRecord,
    ) {
        let complete = completion();
        let original = complete.original.reference();
        let base = complete.base_revision;
        let mut lease = complete.original_released_lease.unwrap();
        lease.state = LeaseState::Active;
        let mut open = complete.closed_open.unwrap();
        open.owner_id = mount_owner(&PackedMountGrantRequest {
            workspace_id: original.guard.workspace_id,
            lease_id: original.guard.lease_id,
            holder_generation: original.guard.holder_generation,
            mount_uid: original.mount_uid,
            pod_uid: original.pod_uid,
            ttl_ns: 1,
        })
        .unwrap();
        open.expires_at_ns = lease.expires_at_ns;
        let writer = PackedWriterAuthority {
            workspace_id: original.guard.workspace_id,
            incarnation: 4,
            owner: Some(PackedWriterOwner::Mounted {
                lease_id: original.guard.lease_id,
                holder_generation: original.guard.holder_generation,
                open_owner: open.owner_id.clone(),
                open_generation: open.generation,
            }),
        };
        (original, base, writer, lease, open)
    }

    fn unstarted_successor_fixture() -> (
        PackedReleasedMountReference,
        MountedRecoveryRecord,
        PackedWriterAuthority,
        SnapshotLease,
        SnapshotLease,
        SnapshotOpenRecord,
    ) {
        let mut record = completion();
        let mut old = record.original_released_lease.take().unwrap();
        old.state = LeaseState::Expired;
        let mut lease = record.released_lease.take().unwrap();
        lease.state = LeaseState::Active;
        let mut open = record.closed_open.take().unwrap();
        open.state = SnapshotOpenState::Recovering;
        open.recovery_required = true;
        open.expires_at_ns = lease.expires_at_ns;
        record.completed = false;
        record.head_sequence = None;
        let writer = PackedWriterAuthority {
            workspace_id: record.original.workspace_id,
            incarnation: record.writer_incarnation,
            owner: Some(PackedWriterOwner::Administrative {
                lease_id: record.current.lease_id,
                holder_generation: record.current.holder_generation,
                open_owner: record.open_owner.clone(),
                open_generation: record.open_generation,
                recovering: true,
            }),
        };
        (
            record.original.reference(),
            record,
            writer,
            old,
            lease,
            open,
        )
    }

    #[test]
    fn unstarted_successor_uses_expired_backend_owner_generation_and_fences_stale_identity() {
        let (original, record, writer, old, lease, open) = unstarted_successor_fixture();
        assert_eq!(
            validate_unstarted_recovery_successor(
                &original,
                &record.base_revision,
                &record.binding_digest,
                UnstartedRecoveryOwnerRows {
                    writer: &writer,
                    original_lease: &old,
                    successor_lease: &lease,
                    open: &open,
                },
                &record,
                29
            )
            .unwrap(),
            Some(4)
        );
        assert_eq!(
            validate_unstarted_recovery_successor(
                &original,
                &record.base_revision,
                &record.binding_digest,
                UnstartedRecoveryOwnerRows {
                    writer: &writer,
                    original_lease: &old,
                    successor_lease: &lease,
                    open: &open,
                },
                &record,
                28
            )
            .unwrap(),
            None
        );
        let mut changed = writer;
        changed.incarnation += 1;
        assert!(
            validate_unstarted_recovery_successor(
                &original,
                &record.base_revision,
                &record.binding_digest,
                UnstartedRecoveryOwnerRows {
                    writer: &changed,
                    original_lease: &old,
                    successor_lease: &lease,
                    open: &open,
                },
                &record,
                29
            )
            .is_err()
        );
        let mut changed = open;
        changed.owner_id.push('x');
        assert!(
            validate_unstarted_recovery_successor(
                &original,
                &record.base_revision,
                &record.binding_digest,
                UnstartedRecoveryOwnerRows {
                    writer: &changed_writer(&record),
                    original_lease: &old,
                    successor_lease: &lease,
                    open: &changed,
                },
                &record,
                29
            )
            .is_err()
        );
    }

    fn changed_writer(record: &MountedRecoveryRecord) -> PackedWriterAuthority {
        PackedWriterAuthority {
            workspace_id: record.original.workspace_id,
            incarnation: record.writer_incarnation,
            owner: Some(PackedWriterOwner::Administrative {
                lease_id: record.current.lease_id,
                holder_generation: record.current.holder_generation,
                open_owner: record.open_owner.clone(),
                open_generation: record.open_generation,
                recovering: true,
            }),
        }
    }

    #[test]
    fn unstarted_successor_fences_foreign_original_and_binding_packet() {
        let (original, record, writer, old, lease, open) = unstarted_successor_fixture();
        let mut changed = original.clone();
        changed.pod_uid = uuid::Uuid::new_v4();
        assert!(
            validate_unstarted_recovery_successor(
                &changed,
                &record.base_revision,
                &record.binding_digest,
                UnstartedRecoveryOwnerRows {
                    writer: &writer,
                    original_lease: &old,
                    successor_lease: &lease,
                    open: &open,
                },
                &record,
                29
            )
            .is_err()
        );
        let mut digest = record.binding_digest;
        digest[0] ^= 1;
        assert!(
            validate_unstarted_recovery_successor(
                &original,
                &record.base_revision,
                &digest,
                UnstartedRecoveryOwnerRows {
                    writer: &writer,
                    original_lease: &old,
                    successor_lease: &lease,
                    open: &open,
                },
                &record,
                29
            )
            .is_err()
        );
        let mut changed = old;
        changed.state = LeaseState::Released;
        assert!(
            validate_unstarted_recovery_successor(
                &original,
                &record.base_revision,
                &record.binding_digest,
                UnstartedRecoveryOwnerRows {
                    writer: &writer,
                    original_lease: &changed,
                    successor_lease: &lease,
                    open: &open,
                },
                &record,
                29
            )
            .is_err()
        );
    }

    #[test]
    fn unstarted_original_uses_actual_expired_generation_once() {
        let (original, base, writer, mut lease, open) = unstarted_fixture();
        assert_eq!(
            validate_unstarted_original_mount(&original, &base, &writer, &lease, &open, 20)
                .unwrap(),
            Some(3)
        );
        lease.state = LeaseState::Expired;
        assert_eq!(
            validate_unstarted_original_mount(&original, &base, &writer, &lease, &open, 21)
                .unwrap(),
            Some(3)
        );
        assert_eq!(
            validate_unstarted_original_mount(&original, &base, &writer, &lease, &open, 19)
                .unwrap(),
            None
        );
    }

    #[test]
    fn unstarted_original_rejects_replacement_mount_pod_and_generation() {
        let (original, base, writer, lease, open) = unstarted_fixture();
        let mut changed = original.clone();
        changed.mount_uid = uuid::Uuid::new_v4();
        assert!(
            validate_unstarted_original_mount(&changed, &base, &writer, &lease, &open, 20).is_err()
        );
        let mut changed = original.clone();
        changed.pod_uid = uuid::Uuid::new_v4();
        assert!(
            validate_unstarted_original_mount(&changed, &base, &writer, &lease, &open, 20).is_err()
        );
        let mut changed = original;
        changed.guard.holder_generation += 1;
        assert!(
            validate_unstarted_original_mount(&changed, &base, &writer, &lease, &open, 20).is_err()
        );
    }

    #[test]
    fn unstarted_original_rejects_closed_or_recovering_owner() {
        let (original, base, mut writer, mut lease, mut open) = unstarted_fixture();
        lease.state = LeaseState::Released;
        assert!(
            validate_unstarted_original_mount(&original, &base, &writer, &lease, &open, 20)
                .is_err()
        );
        lease.state = LeaseState::Active;
        open.state = SnapshotOpenState::Recovering;
        open.recovery_required = true;
        assert!(
            validate_unstarted_original_mount(&original, &base, &writer, &lease, &open, 20)
                .is_err()
        );
        open.state = SnapshotOpenState::Ready;
        open.recovery_required = false;
        writer.owner = None;
        assert!(
            validate_unstarted_original_mount(&original, &base, &writer, &lease, &open, 20)
                .is_err()
        );
    }

    #[test]
    fn unstarted_original_rejects_split_expiry_and_foreign_base() {
        let (original, mut base, writer, lease, mut open) = unstarted_fixture();
        open.expires_at_ns += 1;
        assert!(
            validate_unstarted_original_mount(&original, &base, &writer, &lease, &open, 21)
                .is_err()
        );
        open.expires_at_ns = lease.expires_at_ns;
        base.root_hash[0] ^= 1;
        assert!(
            validate_unstarted_original_mount(&original, &base, &writer, &lease, &open, 20)
                .is_err()
        );
    }

    #[test]
    fn unstarted_original_fences_generation_overflow() {
        let (mut original, base, mut writer, mut lease, mut open) = unstarted_fixture();
        original.guard.holder_generation = u64::MAX;
        lease.holder_generation = u64::MAX;
        open.owner_id = mount_owner(&PackedMountGrantRequest {
            workspace_id: original.guard.workspace_id,
            lease_id: original.guard.lease_id,
            holder_generation: u64::MAX,
            mount_uid: original.mount_uid,
            pod_uid: original.pod_uid,
            ttl_ns: 1,
        })
        .unwrap();
        writer.owner = Some(PackedWriterOwner::Mounted {
            lease_id: original.guard.lease_id,
            holder_generation: u64::MAX,
            open_owner: open.owner_id.clone(),
            open_generation: open.generation,
        });
        assert!(
            validate_unstarted_original_mount(&original, &base, &writer, &lease, &open, 20)
                .is_err()
        );
    }

    #[test]
    fn pmr_source_is_never_an_original_cutoff_receipt() {
        let completion = completion();
        let completion_bytes = encode_recovery(&completion).unwrap();
        let source_bytes = completed_recovery_source(&completion).unwrap();
        assert!(decode_clean_receipt(&completion_bytes).is_err());
        assert!(decode_clean_receipt(&source_bytes).is_err());
        assert!(decode_clean_source(&source_bytes, CleanSourceKind::OriginalPcr).is_err());
        assert!(decode_any_clean_source(&completion_bytes).is_err());
        let source = decode_clean_source(&source_bytes, CleanSourceKind::RecoveredPmr).unwrap();
        assert_eq!(source.kind(), CleanSourceKind::RecoveredPmr);
        assert_eq!(
            source.guard.to_head_guard(),
            completion.current.reference().guard
        );
        assert_eq!(encode_clean_source(&source).unwrap(), source_bytes);
        assert_ne!(
            source.key(),
            clean_receipt_key(&source.guard.to_head_guard())
        );
    }

    #[test]
    fn recovered_facts_cannot_change_the_completed_pvc_identity_or_head() {
        let completion = completion();
        let source = CleanSourceRecord::recovered(completion.clone(), None).unwrap();
        let mut changed = source.clone();
        changed.head_sequence += 1;
        assert!(encode_clean_source(&changed).is_err());
        let mut changed = source;
        changed.guard.holder_generation += 1;
        assert!(encode_clean_source(&changed).is_err());
        let mut changed = completion;
        changed.current.pod_uid = uuid::Uuid::new_v4();
        assert!(encode_recovery(&changed).is_err());
    }

    #[test]
    fn original_cleanup_requires_its_typed_cutoff_and_released_lease() {
        let complete = completion();
        let original = complete.original.reference();
        let lease = complete.original_released_lease.unwrap();
        let mut closed = complete.closed_open.unwrap();
        closed.owner_id = mount_owner(&PackedMountGrantRequest {
            workspace_id: original.guard.workspace_id,
            lease_id: original.guard.lease_id,
            holder_generation: original.guard.holder_generation,
            mount_uid: original.mount_uid,
            pod_uid: original.pod_uid,
            ttl_ns: 1,
        })
        .unwrap();
        let receipt = CleanReleaseReceipt {
            guard: CleanReleaseGuard::from(&original.guard),
            mount_uid: original.mount_uid,
            pod_uid: original.pod_uid,
            head_sequence: 9,
            base_revision: lease.base_revision.clone(),
            binding_version: 1,
            manifest_digest: [3; 32],
            open_owner: Some(closed),
            first_admin_claim: None,
        };
        let receipt = decode_clean_receipt(&encode_clean_receipt(&receipt).unwrap()).unwrap();
        validate_original_packed_cleanup(&receipt, &lease, &original).unwrap();
        let mut active = lease.clone();
        active.state = LeaseState::Active;
        assert!(validate_original_packed_cleanup(&receipt, &active, &original).is_err());
        let mut wrong_cutoff = receipt.clone();
        wrong_cutoff.open_owner.as_mut().unwrap().owner_id = complete.open_owner;
        assert!(validate_original_packed_cleanup(&wrong_cutoff, &lease, &original).is_err());
        let mut replacement = original;
        replacement.mount_uid = uuid::Uuid::new_v4();
        assert!(validate_original_packed_cleanup(&receipt, &lease, &replacement).is_err());
    }

    #[test]
    fn cleanup_completion_is_bound_to_the_exact_original_session() {
        let complete = completion();
        let original = complete.original.reference();
        let report = completed_mount_cleanup_report(&complete, &original)
            .unwrap()
            .unwrap();
        assert_eq!(report.original, original);
        assert_eq!(report.released, complete.current.reference());
        let mut replacement = original;
        replacement.pod_uid = uuid::Uuid::new_v4();
        assert!(completed_mount_cleanup_report(&complete, &replacement).is_err());
    }

    #[test]
    fn incomplete_or_unreleased_pmr_cannot_authorize_cleanup() {
        let mut incomplete = completion();
        let original = incomplete.original.reference();
        incomplete.completed = false;
        incomplete.original_released_lease = None;
        incomplete.released_lease = None;
        incomplete.closed_open = None;
        incomplete.head_sequence = None;
        assert!(
            completed_mount_cleanup_report(&incomplete, &original)
                .unwrap()
                .is_none()
        );
        let mut invalid = completion();
        let original = invalid.original.reference();
        invalid.original_released_lease.as_mut().unwrap().state = LeaseState::Active;
        assert!(completed_mount_cleanup_report(&invalid, &original).is_err());
    }

    #[test]
    fn published_receipt_retains_the_complete_recovered_source() {
        let completion = completion();
        let source = completed_recovery_source(&completion).unwrap();
        let lease = completion.released_lease.as_ref().unwrap().clone();
        let receipt = PublishedCleanReceipt {
            snapshot: SnapshotRecord {
                snapshot_id: SnapshotId::from_uuid(uuid::Uuid::new_v4()),
                name: Some("recovered-source".into()),
                revision: completion.base_revision.clone(),
                owner_id: None,
                created_at_ns: 31,
            },
            binding: Vec::new(),
            native_sealed_source_revision: completion.base_revision,
            released_lease: lease.clone(),
            closed_open: completion.closed_open.unwrap(),
            original_receipt: source.clone(),
            original_released_lease: serde_json::to_vec(&lease).unwrap(),
        };
        let encoded = encode_published_clean(&receipt).unwrap();
        assert!(encoded.len() <= SOURCE_MAX_BYTES);
        let decoded = decode_published_clean(&encoded).unwrap();
        assert_eq!(decoded.original_receipt, source);
        assert_eq!(
            decode_any_clean_source(&decoded.original_receipt)
                .unwrap()
                .kind(),
            CleanSourceKind::RecoveredPmr
        );
    }
}

impl CleanSourceKind {
    pub(in crate::workspace::stores::kv_store::packed_admin) fn key(
        self,
        workspace: WorkspaceId,
        lease: LeaseId,
    ) -> Vec<u8> {
        let prefix = match self {
            Self::OriginalPcr => "packed/clean-release",
            Self::RecoveredPmr => "packed/recovered-source",
        };
        format!("{prefix}/{workspace}/{lease}").into_bytes()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::workspace::stores::kv_store::packed_admin) struct CleanSourceFacts {
    pub(in crate::workspace::stores::kv_store::packed_admin) guard: CleanReleaseGuard,
    pub(in crate::workspace::stores::kv_store::packed_admin) mount_uid: uuid::Uuid,
    pub(in crate::workspace::stores::kv_store::packed_admin) pod_uid: uuid::Uuid,
    pub(in crate::workspace::stores::kv_store::packed_admin) head_sequence: u64,
    pub(in crate::workspace::stores::kv_store::packed_admin) base_revision: BaseRevision,
    pub(in crate::workspace::stores::kv_store::packed_admin) binding_version: u64,
    pub(in crate::workspace::stores::kv_store::packed_admin) manifest_digest: [u8; 32],
    pub(in crate::workspace::stores::kv_store::packed_admin) open_owner: Option<SnapshotOpenRecord>,
    pub(in crate::workspace::stores::kv_store::packed_admin) first_admin_claim:
        Option<FirstAdminCleanClaim>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum CleanSourceEvidence {
    OriginalPcr,
    RecoveredPmr(Box<MountedRecoveryRecord>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::workspace::stores::kv_store::packed_admin) struct CleanSourceRecord {
    facts: CleanSourceFacts,
    evidence: CleanSourceEvidence,
}

impl Deref for CleanSourceRecord {
    type Target = CleanSourceFacts;
    fn deref(&self) -> &Self::Target {
        &self.facts
    }
}
impl DerefMut for CleanSourceRecord {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.facts
    }
}

impl CleanSourceRecord {
    pub(in crate::workspace::stores::kv_store::packed_admin) fn original(
        receipt: CleanReleaseReceipt,
    ) -> Self {
        Self {
            facts: CleanSourceFacts {
                guard: receipt.guard,
                mount_uid: receipt.mount_uid,
                pod_uid: receipt.pod_uid,
                head_sequence: receipt.head_sequence,
                base_revision: receipt.base_revision,
                binding_version: receipt.binding_version,
                manifest_digest: receipt.manifest_digest,
                open_owner: receipt.open_owner,
                first_admin_claim: receipt.first_admin_claim,
            },
            evidence: CleanSourceEvidence::OriginalPcr,
        }
    }

    pub(in crate::workspace::stores::kv_store::packed_admin) fn kind(&self) -> CleanSourceKind {
        match &self.evidence {
            CleanSourceEvidence::OriginalPcr => CleanSourceKind::OriginalPcr,
            CleanSourceEvidence::RecoveredPmr(_) => CleanSourceKind::RecoveredPmr,
        }
    }

    pub(in crate::workspace::stores::kv_store::packed_admin) fn key(&self) -> Vec<u8> {
        self.kind()
            .key(self.guard.workspace_id, self.guard.lease_id)
    }

    fn recovered(
        completion: MountedRecoveryRecord,
        first_admin_claim: Option<FirstAdminCleanClaim>,
    ) -> Result<Self, WorkspaceError> {
        encode_recovery(&completion)?;
        if !completion.completed || completion.first_admin_claim.is_some() {
            return Err(WorkspaceError::Fenced);
        }
        let reference = completion.current.reference();
        Ok(Self {
            facts: CleanSourceFacts {
                guard: CleanReleaseGuard::from(&reference.guard),
                mount_uid: reference.mount_uid,
                pod_uid: reference.pod_uid,
                head_sequence: completion.head_sequence.ok_or(WorkspaceError::Fenced)?,
                base_revision: completion.base_revision.clone(),
                binding_version: completion.binding_version,
                manifest_digest: completion.manifest_digest,
                open_owner: completion.closed_open.clone(),
                first_admin_claim,
            },
            evidence: CleanSourceEvidence::RecoveredPmr(Box::new(completion)),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecoveredPublicationSource {
    completion: MountedRecoveryRecord,
    first_admin_claim: Option<FirstAdminCleanClaim>,
}

pub(in crate::workspace::stores::kv_store::packed_admin) fn encode_clean_source(
    source: &CleanSourceRecord,
) -> Result<Vec<u8>, WorkspaceError> {
    match &source.evidence {
        CleanSourceEvidence::OriginalPcr => encode_clean_receipt(&CleanReleaseReceipt {
            guard: source.guard.clone(),
            mount_uid: source.mount_uid,
            pod_uid: source.pod_uid,
            head_sequence: source.head_sequence,
            base_revision: source.base_revision.clone(),
            binding_version: source.binding_version,
            manifest_digest: source.manifest_digest,
            open_owner: source.open_owner.clone(),
            first_admin_claim: source.first_admin_claim.clone(),
        }),
        CleanSourceEvidence::RecoveredPmr(completion) => {
            if CleanSourceRecord::recovered(
                completion.as_ref().clone(),
                source.first_admin_claim.clone(),
            )? != *source
            {
                return Err(WorkspaceError::Fenced);
            }
            if let Some(claim) = &source.first_admin_claim {
                validate_open_record(&claim.open_owner, source.guard.workspace_id)?;
                let origin = PackedCleanOperationOrigin::parse(&claim.open_owner.owner_id)?
                    .ok_or(WorkspaceError::Fenced)?;
                if origin.source_kind() != CleanSourceKind::RecoveredPmr
                    || origin.source_key(source.guard.workspace_id) != source.key()
                    || !origin.matches_source_reference(&PackedReleasedMountReference {
                        guard: source.guard.to_head_guard(),
                        mount_uid: source.mount_uid,
                        pod_uid: source.pod_uid,
                    })
                    || claim.lease.workspace_id != source.guard.workspace_id
                    || !claim.lease.writable
                    || claim.lease.lease_id.as_uuid().is_nil()
                    || claim.lease.lease_id == source.guard.lease_id
                    || claim.lease.holder_generation <= source.guard.holder_generation
                    || claim.lease.base_revision != source.base_revision
                    || claim.lease.state != LeaseState::Active
                    || claim.lease.created_at_ns <= 0
                    || claim.lease.updated_at_ns != claim.lease.created_at_ns
                    || claim.lease.expires_at_ns <= claim.lease.created_at_ns
                    || claim.open_owner.expires_at_ns != claim.lease.expires_at_ns
                    || claim.open_owner.state != SnapshotOpenState::Ready
                    || claim.open_owner.recovery_required
                    || source
                        .open_owner
                        .as_ref()
                        .is_none_or(|closed| claim.open_owner.generation <= closed.generation)
                {
                    return Err(WorkspaceError::Fenced);
                }
            }
            let body = RecoveredPublicationSource {
                completion: completion.as_ref().clone(),
                first_admin_claim: source.first_admin_claim.clone(),
            };
            let mut bytes = RECOVERED_SOURCE_MAGIC.to_vec();
            bytes.extend(serde_json::to_vec(&body).map_err(|_| WorkspaceError::Fenced)?);
            if bytes.len() > RECOVERED_SOURCE_BYTES {
                return Err(WorkspaceError::Fenced);
            }
            Ok(bytes)
        }
    }
}

pub(in crate::workspace::stores::kv_store::packed_admin) fn decode_clean_source(
    raw: &[u8],
    kind: CleanSourceKind,
) -> Result<CleanSourceRecord, WorkspaceError> {
    let source = match kind {
        CleanSourceKind::OriginalPcr => CleanSourceRecord::original(decode_clean_receipt(raw)?),
        CleanSourceKind::RecoveredPmr => {
            if raw.len() > RECOVERED_SOURCE_BYTES || !raw.starts_with(RECOVERED_SOURCE_MAGIC) {
                return Err(WorkspaceError::Fenced);
            }
            let body: RecoveredPublicationSource =
                serde_json::from_slice(&raw[RECOVERED_SOURCE_MAGIC.len()..])
                    .map_err(|_| WorkspaceError::Fenced)?;
            CleanSourceRecord::recovered(body.completion, body.first_admin_claim)?
        }
    };
    if encode_clean_source(&source)? != raw {
        return Err(WorkspaceError::Fenced);
    }
    Ok(source)
}

pub(in crate::workspace::stores::kv_store::packed_admin) fn decode_any_clean_source(
    raw: &[u8],
) -> Result<CleanSourceRecord, WorkspaceError> {
    let kind = if raw.starts_with(CLEAN_RECEIPT_MAGIC) {
        CleanSourceKind::OriginalPcr
    } else if raw.starts_with(RECOVERED_SOURCE_MAGIC) {
        CleanSourceKind::RecoveredPmr
    } else {
        return Err(WorkspaceError::Fenced);
    };
    decode_clean_source(raw, kind)
}

pub(super) fn recovered_current_key(workspace: WorkspaceId) -> Vec<u8> {
    format!("packed/recovered-current/{workspace}").into_bytes()
}

pub(super) fn completed_recovery_source(
    record: &MountedRecoveryRecord,
) -> Result<Vec<u8>, WorkspaceError> {
    encode_clean_source(&CleanSourceRecord::recovered(record.clone(), None)?)
}

/// Facts from an authenticated completed recovery. These cannot authorize
/// writes, construct a source ticket, or assert an original kernel cutoff.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PackedRecoveredMountReport {
    pub original: PackedReleasedMountReference,
    pub released: PackedReleasedMountReference,
}

fn validate_original_packed_cleanup(
    receipt: &CleanReleaseReceipt,
    lease: &SnapshotLease,
    original: &PackedReleasedMountReference,
) -> Result<(), WorkspaceError> {
    let guard = &original.guard;
    let closed = receipt.open_owner.as_ref().ok_or(WorkspaceError::Fenced)?;
    validate_open_record(closed, guard.workspace_id)?;
    let expected_owner = mount_owner(&PackedMountGrantRequest {
        workspace_id: guard.workspace_id,
        lease_id: guard.lease_id,
        holder_generation: guard.holder_generation,
        mount_uid: original.mount_uid,
        pod_uid: original.pod_uid,
        ttl_ns: 1,
    })?;
    if receipt.guard.to_head_guard() != *guard
        || receipt.mount_uid != original.mount_uid
        || receipt.pod_uid != original.pod_uid
        || lease.workspace_id != guard.workspace_id
        || lease.lease_id != guard.lease_id
        || lease.holder_generation != guard.holder_generation
        || !lease.writable
        || lease.state != LeaseState::Released
        || lease.base_revision != receipt.base_revision
        || lease.updated_at_ns <= 0
        || closed.owner_id != expected_owner
        || closed.state != SnapshotOpenState::Ready
        || closed.recovery_required
        || closed.expires_at_ns != lease.updated_at_ns
    {
        return Err(WorkspaceError::Fenced);
    }
    Ok(())
}

impl<B: WorkspaceKvBackend> KvWorkspaceStore<B> {
    /// Authenticate the exact original cutoff receipt for PVC cleanup only.
    /// A later source claim or head advance cannot revoke an already completed
    /// original drain. Snapshot admission still uses its full current packet.
    pub async fn verify_original_packed_mount_for_cleanup(
        self: &Arc<Self>,
        original: PackedReleasedMountReference,
    ) -> Result<bool, WorkspaceError> {
        let budget = self
            .packed_reader_pin_budget
            .get()
            .cloned()
            .ok_or(WorkspaceError::Fenced)?;
        let _owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, SOURCE_ADMISSION_BYTES)])
            .map_err(|error| WorkspaceError::InvalidReadPlan(error.to_string()))?;
        let key = clean_receipt_key(&original.guard);
        let lease_key = hot_lease_key(original.guard.workspace_id, original.guard.lease_id);
        let keys = [
            key.clone(),
            lease_key.clone(),
            hot_lease_index_key(original.guard.lease_id),
            CONTROL_KEY.to_vec(),
        ];
        let (rows, now) = self
            .backend
            .get_many_consistent_with_time_bounded(&keys, source_limits(keys.len()))
            .await?;
        if rows.len() != keys.len() || now <= 0 {
            return Err(WorkspaceError::Fenced);
        }
        let checks = keys
            .into_iter()
            .zip(rows)
            .map(|(key, expected)| KvCheck { key, expected })
            .collect::<Vec<_>>();
        let Some(raw) = scoped_raw(&checks, &key)? else {
            return Ok(false);
        };
        let receipt = decode_clean_receipt(raw)?;
        let lease: SnapshotLease = scoped_required(&checks, &lease_key)?;
        validate_current_control_raw(scoped_raw(&checks, CONTROL_KEY)?)?;
        let indexed_workspace: WorkspaceId =
            scoped_required(&checks, &hot_lease_index_key(original.guard.lease_id))?;
        if indexed_workspace != original.guard.workspace_id {
            return Err(WorkspaceError::Fenced);
        }
        validate_original_packed_cleanup(&receipt, &lease, &original)?;
        let deadline = now
            .checked_add(30 * 1_000_000_000)
            .ok_or(WorkspaceError::Fenced)?;
        if !self
            .backend
            .authenticate_checks_before_bounded(&checks, deadline, source_limits(checks.len()))
            .await?
        {
            return Err(WorkspaceError::Fenced);
        }
        Ok(true)
    }
}

fn completed_mount_cleanup_report(
    completion: &MountedRecoveryRecord,
    original: &PackedReleasedMountReference,
) -> Result<Option<PackedRecoveredMountReport>, WorkspaceError> {
    encode_recovery(completion)?;
    if completion.original != RecoveryReference::from_reference(original) {
        return Err(WorkspaceError::Fenced);
    }
    if !completion.completed {
        return Ok(None);
    }
    Ok(Some(PackedRecoveredMountReport {
        original: completion.original.reference(),
        released: completion.current.reference(),
    }))
}

impl<B: WorkspaceKvBackend> KvWorkspaceStore<B> {
    /// Authenticate immutable completion of this exact original PVC session.
    /// This cleanup report remains valid after publication consumes the source
    /// or advances the head. It grants no writer or publication authority.
    pub async fn inspect_packed_mount_recovery_for_cleanup(
        self: &Arc<Self>,
        original: PackedReleasedMountReference,
    ) -> Result<Option<PackedRecoveredMountReport>, WorkspaceError> {
        let budget = self
            .packed_reader_pin_budget
            .get()
            .cloned()
            .ok_or(WorkspaceError::Fenced)?;
        let _owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, SOURCE_ADMISSION_BYTES)])
            .map_err(|error| WorkspaceError::InvalidReadPlan(error.to_string()))?;
        let key = recovery_key(&original);
        let (routed, now) = self
            .backend
            .get_many_consistent_with_time_bounded(std::slice::from_ref(&key), source_limits(1))
            .await?;
        if routed.len() != 1 || now <= 0 {
            return Err(WorkspaceError::Fenced);
        }
        let Some(raw) = routed[0].as_deref() else {
            return Ok(None);
        };
        let completion = decode_recovery(raw)?;
        let Some(report) = completed_mount_cleanup_report(&completion, &original)? else {
            return Ok(None);
        };
        let keys = [
            key.clone(),
            hot_lease_key(original.guard.workspace_id, original.guard.lease_id),
            hot_lease_key(
                report.released.guard.workspace_id,
                report.released.guard.lease_id,
            ),
            hot_lease_index_key(original.guard.lease_id),
            hot_lease_index_key(report.released.guard.lease_id),
            CONTROL_KEY.to_vec(),
        ];
        let (rows, now) = self
            .backend
            .get_many_consistent_with_time_bounded(&keys, source_limits(keys.len()))
            .await?;
        if rows.len() != keys.len() || now <= 0 {
            return Err(WorkspaceError::Fenced);
        }
        let checks = keys
            .into_iter()
            .zip(rows)
            .map(|(key, expected)| KvCheck { key, expected })
            .collect::<Vec<_>>();
        if scoped_raw(&checks, &key)? != Some(raw) {
            return Err(WorkspaceError::Busy);
        }
        validate_current_control_raw(scoped_raw(&checks, CONTROL_KEY)?)?;
        let old: SnapshotLease = scoped_required(
            &checks,
            &hot_lease_key(original.guard.workspace_id, original.guard.lease_id),
        )?;
        let released: SnapshotLease = scoped_required(
            &checks,
            &hot_lease_key(
                report.released.guard.workspace_id,
                report.released.guard.lease_id,
            ),
        )?;
        let old_index: WorkspaceId =
            scoped_required(&checks, &hot_lease_index_key(original.guard.lease_id))?;
        let released_index: WorkspaceId = scoped_required(
            &checks,
            &hot_lease_index_key(report.released.guard.lease_id),
        )?;
        if old_index != original.guard.workspace_id
            || released_index != report.released.guard.workspace_id
            || completion.original_released_lease.as_ref() != Some(&old)
            || completion.released_lease.as_ref() != Some(&released)
        {
            return Err(WorkspaceError::Fenced);
        }
        let deadline = now
            .checked_add(30 * 1_000_000_000)
            .ok_or(WorkspaceError::Fenced)?;
        if !self
            .backend
            .authenticate_checks_before_bounded(&checks, deadline, source_limits(checks.len()))
            .await?
        {
            return Err(WorkspaceError::Fenced);
        }
        Ok(Some(report))
    }

    /// Return a scheduling hint only after the backend clock and complete
    /// typed packet confirm an expired incomplete recovery owner. The new
    /// driver must still authenticate the original PVC and win its takeover CAS.
    pub async fn inspect_expired_packed_mount_recovery(
        self: &Arc<Self>,
        original: PackedReleasedMountReference,
    ) -> Result<Option<(PackedReleasedMountReference, u64)>, WorkspaceError> {
        let budget = self
            .packed_reader_pin_budget
            .get()
            .cloned()
            .ok_or(WorkspaceError::Fenced)?;
        let _owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, SOURCE_ADMISSION_BYTES)])
            .map_err(|error| WorkspaceError::InvalidReadPlan(error.to_string()))?;
        let key = recovery_key(&original);
        let (routed, now) = self
            .backend
            .get_many_consistent_with_time_bounded(std::slice::from_ref(&key), source_limits(1))
            .await?;
        if routed.len() != 1 || now <= 0 {
            return Err(WorkspaceError::Fenced);
        }
        let Some(raw) = routed[0].as_deref() else {
            return Ok(None);
        };
        let record = decode_recovery(raw)?;
        if record.original != RecoveryReference::from_reference(&original) {
            return Err(WorkspaceError::Fenced);
        }
        if record.completed {
            return Ok(None);
        }
        let previous = record.current.reference();
        let extra = [
            key.clone(),
            hot_lease_key(original.guard.workspace_id, original.guard.lease_id),
        ];
        let view = self
            .scoped_packed_mount_view(
                original.guard.workspace_id,
                previous.guard.lease_id,
                true,
                &extra,
            )
            .await?;
        let lease = view
            .requested_lease
            .as_ref()
            .ok_or(WorkspaceError::Fenced)?;
        let open = view.open.as_ref().ok_or(WorkspaceError::Fenced)?;
        let old: SnapshotLease = scoped_required(
            &view.checks,
            &hot_lease_key(original.guard.workspace_id, original.guard.lease_id),
        )?;
        let expected = PackedWriterOwner::Administrative {
            lease_id: previous.guard.lease_id,
            holder_generation: previous.guard.holder_generation,
            open_owner: record.open_owner.clone(),
            open_generation: record.open_generation,
            recovering: true,
        };
        if scoped_raw(&view.checks, &key)? != Some(raw)
            || view.binding.workspace_id != original.guard.workspace_id
            || view.binding.head_layer_id != original.guard.expected_head_layer_id
            || view.binding.head_epoch != original.guard.expected_head_epoch
            || binding_digest(&view.binding)? != record.binding_digest
            || view.writer.incarnation != record.writer_incarnation
            || view.writer.owner.as_ref() != Some(&expected)
            || lease.workspace_id != previous.guard.workspace_id
            || lease.lease_id != previous.guard.lease_id
            || lease.holder_generation != previous.guard.holder_generation
            || !lease.writable
            || !matches!(lease.state, LeaseState::Active | LeaseState::Expired)
            || lease.base_revision != record.base_revision
            || open.owner_id != record.open_owner
            || open.generation != record.open_generation
            || open.state != SnapshotOpenState::Recovering
            || !open.recovery_required
            || open.expires_at_ns != lease.expires_at_ns
            || old.workspace_id != original.guard.workspace_id
            || old.lease_id != original.guard.lease_id
            || old.holder_generation != original.guard.holder_generation
            || !old.writable
            || !matches!(old.state, LeaseState::Active | LeaseState::Expired)
            || old.base_revision != record.base_revision
            || old.expires_at_ns > view.now
        {
            return Err(WorkspaceError::Fenced);
        }
        if lease.expires_at_ns > view.now || open.expires_at_ns > view.now {
            return Ok(None);
        }
        let next_generation = previous
            .guard
            .holder_generation
            .checked_add(1)
            .ok_or(WorkspaceError::Fenced)?;
        let deadline = view
            .now
            .checked_add(30 * 1_000_000_000)
            .ok_or(WorkspaceError::Fenced)?;
        if !self
            .backend
            .authenticate_checks_before_bounded(
                &view.checks,
                deadline,
                source_limits(view.checks.len()),
            )
            .await?
        {
            return Err(WorkspaceError::Fenced);
        }
        Ok(Some((previous, next_generation)))
    }
}

fn validate_unstarted_original_mount(
    original: &PackedReleasedMountReference,
    base: &BaseRevision,
    writer: &PackedWriterAuthority,
    lease: &SnapshotLease,
    open: &SnapshotOpenRecord,
    now: i64,
) -> Result<Option<u64>, WorkspaceError> {
    let guard = &original.guard;
    let expected_owner = mount_owner(&PackedMountGrantRequest {
        workspace_id: guard.workspace_id,
        lease_id: guard.lease_id,
        holder_generation: guard.holder_generation,
        mount_uid: original.mount_uid,
        pod_uid: original.pod_uid,
        ttl_ns: 1,
    })?;
    let expected = PackedWriterOwner::Mounted {
        lease_id: guard.lease_id,
        holder_generation: guard.holder_generation,
        open_owner: expected_owner.clone(),
        open_generation: open.generation,
    };
    if now <= 0
        || writer.workspace_id != guard.workspace_id
        || writer.incarnation == 0
        || writer.owner.as_ref() != Some(&expected)
        || lease.workspace_id != guard.workspace_id
        || lease.lease_id != guard.lease_id
        || lease.holder_generation != guard.holder_generation
        || !lease.writable
        || !matches!(lease.state, LeaseState::Active | LeaseState::Expired)
        || lease.base_revision != *base
        || open.workspace_id != guard.workspace_id
        || open.owner_id != expected_owner
        || open.generation == 0
        || open.state != SnapshotOpenState::Ready
        || open.recovery_required
        || open.expires_at_ns != lease.expires_at_ns
    {
        return Err(WorkspaceError::Fenced);
    }
    if lease.expires_at_ns > now || open.expires_at_ns > now {
        return Ok(None);
    }
    Ok(Some(
        guard
            .holder_generation
            .checked_add(1)
            .ok_or(WorkspaceError::Fenced)?,
    ))
}

struct UnstartedRecoveryOwnerRows<'a> {
    writer: &'a PackedWriterAuthority,
    original_lease: &'a SnapshotLease,
    successor_lease: &'a SnapshotLease,
    open: &'a SnapshotOpenRecord,
}

fn validate_unstarted_recovery_successor(
    original: &PackedReleasedMountReference,
    base: &BaseRevision,
    digest: &[u8; 32],
    rows: UnstartedRecoveryOwnerRows<'_>,
    record: &MountedRecoveryRecord,
    now: i64,
) -> Result<Option<u64>, WorkspaceError> {
    let UnstartedRecoveryOwnerRows {
        writer,
        original_lease: old,
        successor_lease: lease,
        open,
    } = rows;
    encode_recovery(record)?;
    if record.original != RecoveryReference::from_reference(original) {
        return Err(WorkspaceError::Fenced);
    }
    if record.completed {
        return Ok(None);
    }
    let previous = record.current.reference();
    let expected = PackedWriterOwner::Administrative {
        lease_id: previous.guard.lease_id,
        holder_generation: previous.guard.holder_generation,
        open_owner: record.open_owner.clone(),
        open_generation: record.open_generation,
        recovering: true,
    };
    if now <= 0
        || record.binding_digest != *digest
        || record.base_revision != *base
        || writer.workspace_id != original.guard.workspace_id
        || writer.incarnation != record.writer_incarnation
        || writer.owner.as_ref() != Some(&expected)
        || lease.workspace_id != previous.guard.workspace_id
        || lease.lease_id != previous.guard.lease_id
        || lease.holder_generation != previous.guard.holder_generation
        || !lease.writable
        || !matches!(lease.state, LeaseState::Active | LeaseState::Expired)
        || lease.base_revision != record.base_revision
        || open.workspace_id != original.guard.workspace_id
        || open.owner_id != record.open_owner
        || open.generation != record.open_generation
        || open.state != SnapshotOpenState::Recovering
        || !open.recovery_required
        || open.expires_at_ns != lease.expires_at_ns
        || old.workspace_id != original.guard.workspace_id
        || old.lease_id != original.guard.lease_id
        || old.holder_generation != original.guard.holder_generation
        || !old.writable
        || !matches!(old.state, LeaseState::Active | LeaseState::Expired)
        || old.base_revision != record.base_revision
        || old.expires_at_ns > now
    {
        return Err(WorkspaceError::Fenced);
    }
    if lease.expires_at_ns > now || open.expires_at_ns > now {
        return Ok(None);
    }
    Ok(Some(
        previous
            .guard
            .holder_generation
            .checked_add(1)
            .ok_or(WorkspaceError::Fenced)?,
    ))
}

impl<B: WorkspaceKvBackend> KvWorkspaceStore<B> {
    /// Facts for retrying a recovery Job that failed before its takeover CAS.
    /// The initial or expired-PMR predecessor must remain exact; both attempt leases must be unused.
    /// It grants no authority; the new driver must verify the original PVC and
    /// authenticate its own fresh packet before winning the real takeover CAS.
    pub async fn inspect_unstarted_packed_mount_recovery(
        self: &Arc<Self>,
        original: PackedReleasedMountReference,
        failed_lease: LeaseId,
        next_lease: LeaseId,
    ) -> Result<Option<u64>, WorkspaceError> {
        if failed_lease.as_uuid().is_nil()
            || next_lease.as_uuid().is_nil()
            || failed_lease == next_lease
            || failed_lease == original.guard.lease_id
            || next_lease == original.guard.lease_id
        {
            return Err(WorkspaceError::Fenced);
        }
        let budget = self
            .packed_reader_pin_budget
            .get()
            .cloned()
            .ok_or(WorkspaceError::Fenced)?;
        let _owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, SOURCE_ADMISSION_BYTES)])
            .map_err(|error| WorkspaceError::InvalidReadPlan(error.to_string()))?;
        let key = recovery_key(&original);
        let extra = [
            key.clone(),
            hot_lease_key(original.guard.workspace_id, original.guard.lease_id),
            hot_lease_key(original.guard.workspace_id, failed_lease),
            hot_lease_index_key(failed_lease),
        ];
        let view = self
            .scoped_packed_mount_view(original.guard.workspace_id, next_lease, true, &extra)
            .await?;
        if view.binding.workspace_id != original.guard.workspace_id
            || view.binding.head_layer_id != original.guard.expected_head_layer_id
            || view.binding.head_epoch != original.guard.expected_head_epoch
        {
            return Err(WorkspaceError::Fenced);
        }
        // Entity decoding authenticates workspace/lease key identities. Both
        // global ID indexes must remain absent for these unused attempts.
        if scoped_raw(
            &view.checks,
            &hot_lease_key(original.guard.workspace_id, failed_lease),
        )?
        .is_some()
            || scoped_raw(&view.checks, &hot_lease_index_key(failed_lease))?.is_some()
            || view.requested_lease.is_some()
        {
            return Ok(None);
        }
        let old: SnapshotLease = scoped_required(
            &view.checks,
            &hot_lease_key(original.guard.workspace_id, original.guard.lease_id),
        )?;
        let open = view.open.as_ref().ok_or(WorkspaceError::Fenced)?;
        let generation = if let Some(raw) = scoped_raw(&view.checks, &key)? {
            let prior = decode_recovery(raw)?;
            if prior.original != RecoveryReference::from_reference(&original) {
                return Err(WorkspaceError::Fenced);
            }
            if prior.completed {
                return Ok(None);
            }
            // scoped_packed_mount_view already includes the actual PWA owner's
            // hot lease and requires its exact Recovering open/recovery marker.
            let lease: SnapshotLease = scoped_required(
                &view.checks,
                &hot_lease_key(original.guard.workspace_id, prior.current.lease_id),
            )?;
            validate_unstarted_recovery_successor(
                &original,
                &view.binding.base_revision,
                &binding_digest(&view.binding)?,
                UnstartedRecoveryOwnerRows {
                    writer: &view.writer,
                    original_lease: &old,
                    successor_lease: &lease,
                    open,
                },
                &prior,
                view.now,
            )?
        } else {
            if let Some(raw) = scoped_raw(
                &view.checks,
                &open_snapshot_recovery_key(original.guard.workspace_id),
            )? {
                let recovery: SnapshotRecoveryRecord =
                    decode_open_value(raw, OPEN_RECOVERY_MAX_BYTES)?;
                if recovery.incomplete {
                    return Err(WorkspaceError::Fenced);
                }
            }
            validate_unstarted_original_mount(
                &original,
                &view.binding.base_revision,
                &view.writer,
                &old,
                open,
                view.now,
            )?
        };
        let Some(generation) = generation else {
            return Ok(None);
        };
        let deadline = view
            .now
            .checked_add(30 * 1_000_000_000)
            .ok_or(WorkspaceError::Fenced)?;
        if !self
            .backend
            .authenticate_checks_before_bounded(
                &view.checks,
                deadline,
                source_limits(view.checks.len()),
            )
            .await?
        {
            return Err(WorkspaceError::Fenced);
        }
        Ok(Some(generation))
    }
}

impl<B: WorkspaceKvBackend> KvWorkspaceStore<B> {
    async fn admit_recovered_packed_source(
        self: &Arc<Self>,
        reference: PackedReleasedMountReference,
        budget: Arc<SnapshotMountBudget>,
    ) -> Result<PackedCleanAdmission<B>, WorkspaceError> {
        if !self
            .packed_reader_pin_budget
            .get()
            .is_some_and(|canonical| Arc::ptr_eq(canonical, &budget))
        {
            return Err(WorkspaceError::Fenced);
        }
        let owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, SOURCE_ADMISSION_BYTES)])
            .map_err(|error| WorkspaceError::InvalidReadPlan(error.to_string()))?;
        let source_key = CleanSourceKind::RecoveredPmr
            .key(reference.guard.workspace_id, reference.guard.lease_id);
        let (routed, _) = self
            .backend
            .get_many_consistent_with_time_bounded(
                std::slice::from_ref(&source_key),
                source_limits(1),
            )
            .await?;
        if routed.len() != 1 {
            return Err(WorkspaceError::Fenced);
        }
        let Some(raw) = routed[0].as_deref() else {
            return Ok(PackedCleanAdmission::RequiresRecovery);
        };
        let source = decode_clean_source(raw, CleanSourceKind::RecoveredPmr)?;
        let CleanSourceEvidence::RecoveredPmr(completion) = &source.evidence else {
            return Err(WorkspaceError::Fenced);
        };
        if completion.current != RecoveryReference::from_reference(&reference) {
            return Err(WorkspaceError::Fenced);
        }
        if source.first_admin_claim.is_some() {
            return Ok(PackedCleanAdmission::RequiresRecovery);
        }
        let original = completion.original.reference();
        let extra = [
            source_key.clone(),
            recovery_key(&original),
            hot_lease_key(original.guard.workspace_id, original.guard.lease_id),
            recovered_current_key(reference.guard.workspace_id),
        ];
        let mut view = self
            .scoped_packed_mount_view(
                reference.guard.workspace_id,
                reference.guard.lease_id,
                true,
                &extra,
            )
            .await?;
        // Retained entity history includes later expired/released attempts.
        // Its membership epoch and every row join the final single-version
        // source read and the source ticket's actual mutation packet.
        let history = self
            .read_workspace_lease_history_checks(
                reference.guard.workspace_id,
                16,
                source_limits(32),
            )
            .await?;
        let mut keys = view
            .checks
            .iter()
            .map(|check| check.key.clone())
            .collect::<Vec<_>>();
        if keys.len() > 32 {
            return Err(WorkspaceError::InvalidReadPlan(
                "packed recovered source historical lease packet exceeds 32 keys".into(),
            ));
        }
        append_workspace_history_keys(&mut keys, &history, 32)?;
        let (values, now) = self
            .backend
            .get_many_consistent_with_time_bounded(&keys, source_limits(keys.len()))
            .await?;
        if values.len() != keys.len() || now <= 0 {
            return Err(WorkspaceError::Fenced);
        }
        authenticate_workspace_history_values(&keys, &values, &history)?;
        if values
            .iter()
            .zip(&view.checks)
            .any(|(value, check)| value != &check.expected)
        {
            return Err(WorkspaceError::Busy);
        }
        view.checks = keys
            .into_iter()
            .zip(values)
            .map(|(key, expected)| KvCheck { key, expected })
            .collect();
        view.now = now;
        let lease = view
            .requested_lease
            .as_ref()
            .ok_or(WorkspaceError::Fenced)?;
        let head: LayerRecord = scoped_required(
            &view.checks,
            &hot_layer_key(reference.guard.expected_head_layer_id),
        )?;
        let original_lease: SnapshotLease = scoped_required(
            &view.checks,
            &hot_lease_key(original.guard.workspace_id, original.guard.lease_id),
        )?;
        let mut control = topology_state_from_checks(&view.checks)?;
        if scoped_raw(&view.checks, &source_key)? != Some(raw)
            || scoped_raw(&view.checks, &recovery_key(&original))?
                != Some(encode_recovery(completion)?.as_slice())
            || scoped_raw(
                &view.checks,
                &recovered_current_key(reference.guard.workspace_id),
            )? != Some(source_key.as_slice())
            || view.writer.owner.is_some()
            || view.writer.incarnation
                != completion
                    .writer_incarnation
                    .checked_add(1)
                    .ok_or(WorkspaceError::Fenced)?
            || binding_digest(&view.binding)? != completion.binding_digest
            || completion.released_lease.as_ref() != Some(lease)
            || completion.original_released_lease.as_ref() != Some(&original_lease)
            || view.open != completion.closed_open
            || head.next_sequence != source.head_sequence
            || view
                .open
                .as_ref()
                .is_none_or(|open| open.expires_at_ns > view.now)
            || control.leases.get(&lease.lease_id) != Some(lease)
            || control.leases.get(&original_lease.lease_id) != Some(&original_lease)
        {
            return Err(WorkspaceError::Fenced);
        }
        for (id, catalog) in control
            .leases
            .iter_mut()
            .filter(|(_, row)| row.workspace_id == reference.guard.workspace_id)
        {
            if !clean_source_routes_other_lease(*id, catalog, lease.lease_id)? {
                continue;
            }
            let hot: SnapshotLease = scoped_required(
                &view.checks,
                &hot_lease_key(reference.guard.workspace_id, *id),
            )?;
            match clean_source_other_lease_invalidates(catalog, Some(&hot), lease) {
                Ok(false) => *catalog = hot,
                Ok(true) | Err(WorkspaceError::Busy) => {
                    return Ok(PackedCleanAdmission::RequiresRecovery);
                }
                Err(error) => return Err(error),
            }
        }
        if control
            .workspaces
            .get(&reference.guard.workspace_id)
            .ok_or(WorkspaceError::Fenced)?
            .active_lease
            .is_some()
            || control.leases.values().any(|other| {
                other.workspace_id == reference.guard.workspace_id
                    && (other.state == LeaseState::Active
                        || (other.writable
                            && other.lease_id != lease.lease_id
                            && (other.created_at_ns >= lease.created_at_ns
                                || other.holder_generation >= lease.holder_generation)))
            })
        {
            return Ok(PackedCleanAdmission::RequiresRecovery);
        }
        let deadline = view
            .now
            .checked_add(30 * 1_000_000_000)
            .ok_or(WorkspaceError::Fenced)?;
        if !self
            .backend
            .authenticate_checks_before_bounded(
                &view.checks,
                deadline,
                source_limits(view.checks.len()),
            )
            .await?
        {
            return Err(WorkspaceError::Fenced);
        }
        Ok(PackedCleanAdmission::Ready(PackedCleanSourceTicket {
            store: self.clone(),
            receipt: Box::new(source),
            checks: view.checks,
            budget,
            _owner: owner,
        }))
    }

    pub async fn inspect_recovered_packed_mount(
        self: &Arc<Self>,
        workspace_id: WorkspaceId,
    ) -> Result<Option<PackedRecoveredMountReport>, WorkspaceError> {
        let budget = self
            .packed_reader_pin_budget
            .get()
            .cloned()
            .ok_or(WorkspaceError::Fenced)?;
        let _owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, SOURCE_ADMISSION_BYTES)])
            .map_err(|error| WorkspaceError::InvalidReadPlan(error.to_string()))?;
        let pointer = recovered_current_key(workspace_id);
        let (rows, _) = self
            .backend
            .get_many_consistent_with_time_bounded(&[pointer], source_limits(1))
            .await?;
        if rows.len() != 1 {
            return Err(WorkspaceError::Fenced);
        }
        let Some(key) = &rows[0] else {
            return Ok(None);
        };
        let prefix = format!("packed/recovered-source/{workspace_id}/");
        let text = std::str::from_utf8(key).map_err(|_| WorkspaceError::Fenced)?;
        let lease_text = text.strip_prefix(&prefix).ok_or(WorkspaceError::Fenced)?;
        let lease = LeaseId::from_uuid(
            uuid::Uuid::parse_str(lease_text).map_err(|_| WorkspaceError::Fenced)?,
        );
        if lease.as_uuid().is_nil()
            || lease.to_string() != lease_text
            || CleanSourceKind::RecoveredPmr.key(workspace_id, lease) != *key
        {
            return Err(WorkspaceError::Fenced);
        }
        let (rows, _) = self
            .backend
            .get_many_consistent_with_time_bounded(std::slice::from_ref(key), source_limits(1))
            .await?;
        if rows.len() != 1 {
            return Err(WorkspaceError::Fenced);
        }
        let Some(raw) = rows[0].as_deref() else {
            return Err(WorkspaceError::Fenced);
        };
        let source = decode_clean_source(raw, CleanSourceKind::RecoveredPmr)?;
        let CleanSourceEvidence::RecoveredPmr(completion) = &source.evidence else {
            return Err(WorkspaceError::Fenced);
        };
        if completion.current.workspace_id != workspace_id || completion.current.lease_id != lease {
            return Err(WorkspaceError::Fenced);
        }
        let released = completion.current.reference();
        let original = completion.original.reference();
        match self
            .admit_recovered_packed_source(released.clone(), budget)
            .await?
        {
            PackedCleanAdmission::Ready(_) => {
                Ok(Some(PackedRecoveredMountReport { original, released }))
            }
            PackedCleanAdmission::RequiresRecovery => Ok(None),
        }
    }

    #[cfg(target_os = "linux")]
    pub async fn publish_recovered_packed_snapshot<O, S>(
        self: &Arc<Self>,
        reference: PackedReleasedMountReference,
        client: ObjectClient<O>,
        upper: Arc<S>,
        layout: ChunkLayout,
        request: PackedHeadlessSnapshotRequest,
    ) -> Result<PackedSnapshotResult, PackedHeadlessSnapshotFailure>
    where
        O: ObjectBackend + Clone + 'static,
        S: BlockStore + Send + Sync + 'static,
    {
        self.require_admin_access()?;
        let handle = tokio::runtime::Handle::try_current().map_err(|_| {
            WorkspaceError::Backend("recovered packed snapshot requires a Tokio runtime".into())
        })?;
        let store = self.clone();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        handle.spawn(async move {
            let result = async {
                let budget = store
                    .packed_reader_pin_budget
                    .get()
                    .cloned()
                    .ok_or(WorkspaceError::Fenced)?;
                let ticket = match store
                    .admit_recovered_packed_source(reference, budget)
                    .await?
                {
                    PackedCleanAdmission::Ready(ticket) => ticket,
                    PackedCleanAdmission::RequiresRecovery => {
                        return Err(WorkspaceError::Fenced.into());
                    }
                };
                store
                    .publish_clean_packed_snapshot(ticket, client, upper, layout, request)
                    .await
            }
            .await;
            let _ = sender.send(result);
        });
        receiver.await.map_err(|_| {
            PackedHeadlessSnapshotFailure::from(WorkspaceError::Backend(
                "owned recovered packed publisher stopped".into(),
            ))
        })?
    }
}
