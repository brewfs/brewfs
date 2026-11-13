//! Durable v3 packed staging on the same Redis/TiKV CAS substrate as PWB3.
//! This sidecar records recovery facts and pins; it cannot create graph proof.

use super::*;

#[cfg(test)]
#[path = "packed_journal/native_gc_target_tests.rs"]
mod native_gc_target_tests;
#[cfg(test)]
#[path = "packed_journal/native_slice_fence_tests.rs"]
mod native_slice_fence_tests;
use crate::cadapter::client::{ObjectBackend, ObjectClient};
use crate::workspace::packed::snapshot::SnapshotIndexAuditLimits;
use crate::workspace::packed::snapshot::SnapshotOwnedPermit;
use crate::workspace::packed::snapshot::{
    SnapshotBudgetPool, SnapshotMountBudget, SnapshotObjectKind, SnapshotObjectRef,
};
#[cfg(target_os = "linux")]
use crate::workspace::packed::snapshot::{SnapshotFinalSourceProof, SnapshotIndexContextAudit};
use crate::workspace::packed::snapshot::{
    SnapshotStagedObjectVerifier, audit_snapshot_staged_index_contexts,
};
use crate::workspace::stores::kv_backend::KvReadLimits;
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
mod native_publication;
mod native_rebind;
mod registry;
pub(crate) use native_publication::PackedNativeRecoveryBasisReceipt;
#[cfg(target_os = "linux")]
pub(crate) use native_publication::{
    NativePublicationBuildOptions, NativePublicationPreparationFailure,
};
pub use registry::admin_gc::facade::{
    PackedGcAdmin, PackedGcCursor, PackedGcPolicy, PackedGcTickReport, PackedGcTickRequest,
};
pub(crate) use registry::collector::{
    PackedRegistryMigrationOptions, PackedRegistryMigrationReport,
};

const JOURNAL_PREFIX: &[u8] = b"packed/journal/";
const ACTIVE_PREFIX: &[u8] = b"packed/journal-active/";
const JOURNAL_FEATURE_KEY: &[u8] = b"packed/journal-feature";
const ACTIVE_COUNT_KEY: &[u8] = b"packed/journal-active-count";
const RECORD_LIMIT: usize = 48 << 10;
const REFERENCE_LIMIT: usize = 8192;
const MAX_ACTIVE_JOURNALS: usize = 64;
const MAX_OBJECTS: u64 = 1 << 20;
// Covers simultaneous codec copies, consistent-read values/checks and writes.
// Successful returned records retain only the bounded 48 KiB result owner.
const OPERATION_BYTES: u64 = 8 << 20;
const INVENTORY_DOMAIN: &[u8] = b"BrewFS packed v3 durable inventory\0";

type JournalChange = (Vec<u8>, Option<Vec<u8>>, Option<Vec<u8>>);

#[cfg(target_os = "linux")]
pub(crate) struct ImportedGraphAuditOptions<'a> {
    pub scratch: &'a std::path::Path,
    pub limits: SnapshotIndexAuditLimits,
    pub cancel: CancellationToken,
}
#[cfg(target_os = "linux")]
type ImportedGraphAuditContext<'a, B> = (
    &'a PackedJournalRecord,
    &'a SnapshotFinalSourceProof,
    Option<
        &'a crate::workspace::stores::kv_store::packed_native_freeze::PackedNativeQuiesceFence<B>,
    >,
);

fn journal_point_limits() -> KvReadLimits {
    KvReadLimits {
        max_records: 32,
        max_key_bytes: 256,
        max_value_bytes: RECORD_LIMIT,
        max_total_bytes: 2 << 20,
        max_response_bytes: 2 << 20,
        max_data_requests: 32,
    }
}

fn journal_probe_limits() -> KvReadLimits {
    KvReadLimits {
        max_records: 3,
        max_key_bytes: 256,
        max_value_bytes: 512,
        max_total_bytes: 4096,
        max_response_bytes: 16 << 10,
        max_data_requests: 3,
    }
}

fn journal_scan_limits() -> KvReadLimits {
    KvReadLimits {
        max_records: MAX_ACTIVE_JOURNALS + 1,
        max_key_bytes: 256,
        max_value_bytes: RECORD_LIMIT,
        max_total_bytes: 4 << 20,
        max_response_bytes: 4 << 20,
        max_data_requests: 1024,
    }
}

#[derive(Debug)]
pub(crate) struct OwnedPackedJournal<T> {
    value: T,
    _permit: SnapshotOwnedPermit,
}
impl<T> std::ops::Deref for OwnedPackedJournal<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.value
    }
}
impl<T: PartialEq> PartialEq for OwnedPackedJournal<T> {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}
impl<T: Eq> Eq for OwnedPackedJournal<T> {}
impl PartialEq<PackedJournalRecord> for OwnedPackedJournal<PackedJournalRecord> {
    fn eq(&self, other: &PackedJournalRecord) -> bool {
        self.value == *other
    }
}
trait RetainJournalAdmission<T> {
    fn retain(self, permit: SnapshotOwnedPermit) -> Result<OwnedPackedJournal<T>, WorkspaceError>;
}
impl<T> RetainJournalAdmission<T> for Result<T, WorkspaceError> {
    fn retain(
        self,
        mut permit: SnapshotOwnedPermit,
    ) -> Result<OwnedPackedJournal<T>, WorkspaceError> {
        let value = self?;
        permit
            .shrink(SnapshotBudgetPool::Metadata, RECORD_LIMIT as u64)
            .map_err(journal_error)?;
        Ok(OwnedPackedJournal {
            value,
            _permit: permit,
        })
    }
}
impl RetainJournalAdmission<PackedJournalRecord> for PackedJournalRecord {
    fn retain(
        self,
        permit: SnapshotOwnedPermit,
    ) -> Result<OwnedPackedJournal<Self>, WorkspaceError> {
        Ok::<Self, WorkspaceError>(self).retain(permit)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum PackedJournalPhase {
    Building = 0,
    Uploading = 1,
    Readback = 2,
    AwaitingFullProof = 3,
    Verified = 4,
    Committed = 5,
    Aborted = 6,
}

impl PackedJournalPhase {
    fn decode(value: u8) -> Result<Self, WorkspaceError> {
        match value {
            0 => Ok(Self::Building),
            1 => Ok(Self::Uploading),
            2 => Ok(Self::Readback),
            3 => Ok(Self::AwaitingFullProof),
            4 => Ok(Self::Verified),
            5 => Ok(Self::Committed),
            6 => Ok(Self::Aborted),
            _ => Err(journal_error("unknown PPJ3 phase")),
        }
    }
    fn terminal(self) -> bool {
        matches!(self, Self::Committed | Self::Aborted)
    }
}

/// Identity of an existing source view, not evidence that the view is atomic.
/// snapshot_backed=false retains the explicit best-effort detection boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PackedSourceView {
    pub snapshot_backed: bool,
    pub effective_view_digest: [u8; 32],
    pub frozen_view_token: [u8; 32],
    pub build_provenance_digest: [u8; 32],
    pub build_owner: String,
    pub staging_id: Uuid,
    pub staging_prefix: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PackedJournalRecord {
    pub journal_id: JournalId,
    pub revision: u64,
    pub phase: PackedJournalPhase,
    pub guard: HeadGuard,
    pub source: PackedSourceView,
    expected_head: Vec<u8>,
    expected_base: Vec<u8>,
    pub expected_binding: PackedLowerBindingRecord,
    pub object_count: u64,
    pub inventory_digest: [u8; 32],
    pub commit_target: Option<PackedLowerBindingRecord>,
    pub full_proof_digest: [u8; 32],
    graph_receipt: Option<PackedGraphReceipt>,
    native_rebind: Option<native_rebind::PackedNativeJournalBasis>,
    pub abort_reason: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PackedJournalObject {
    pub ordinal: u64,
    pub reference: SnapshotObjectRef,
    /// Durable progress observations, never an authentication/publication seal.
    pub uploaded: bool,
    pub readback_recorded: bool,
}

/// Only the native factory's actual graph and verified Hashed source construct
/// this token. A journal digest, readback flag or namespace boundary cannot.
/// Fresh recovery reruns graph/source/hash authority before reissuance.
pub(crate) struct CompletePackedGraphSeal {
    journal_id: JournalId,
    source: PackedSourceView,
    manifest: SnapshotObjectRef,
    object_count: u64,
    inventory_digest: [u8; 32],
    proof_digest: [u8; 32],
    guard: HeadGuard,
    audited_revision: u64,
    staging_incarnation: Uuid,
    graph_receipt_digest: [u8; 32],
}

/// Durable factual receipt. Decoding these bytes does not issue an in-process
/// publication token. Recovery must rerun actual graph/source/fence checks.
#[derive(Clone, Debug, Eq, PartialEq)]
struct PackedGraphReceipt {
    audited_revision: u64,
    staging_incarnation: Uuid,
    manifest: SnapshotObjectRef,
    object_count: u64,
    inventory_digest: [u8; 32],
    physical_graph_digest: [u8; 32],
    final_source_digest: [u8; 32],
    catalog_context_digest: [u8; 32],
    snapshot_backed: bool,
    highest_inode: u64,
}

/// Real importer + full physical/semantic graph proof for an independently
/// imported namespace. It is deliberately distinct from the native/effective
/// workspace view seal consumed by commit_packed_journal.
pub(crate) struct ImportedPackedGraphSeal {
    receipt: PackedGraphReceipt,
    _permit: SnapshotOwnedPermit,
}

fn audit_basis_digest(
    record: &PackedJournalRecord,
    audited_revision: u64,
) -> Result<[u8; 32], WorkspaceError> {
    let mut basis = record.clone();
    basis.revision = audited_revision;
    basis.phase = PackedJournalPhase::AwaitingFullProof;
    if let Some(native) = &mut basis.native_rebind
        && let Some(publication) = &mut native.publication
    {
        publication.final_source_guard = None;
        publication.final_source_root_hash = None;
        publication.final_source_delta_digest = None;
    }
    basis.graph_receipt = None;
    basis.full_proof_digest = [0; 32];
    basis.abort_reason.clear();
    let mut hash = Sha256::new();
    hash.update(b"BrewFS packed v3 graph audit catalog basis\0");
    hash.update(basis.encode()?);
    Ok(hash.finalize().into())
}

impl PackedGraphReceipt {
    fn encode(&self) -> Result<Vec<u8>, WorkspaceError> {
        if self.audited_revision == 0
            || self.staging_incarnation.is_nil()
            || self.object_count == 0
            || self.object_count > MAX_OBJECTS
            || self.manifest.kind != SnapshotObjectKind::Manifest
            || self.highest_inode == 0
            || self.highest_inode >= i64::MAX as u64 - 1
            || [
                self.inventory_digest,
                self.physical_graph_digest,
                self.final_source_digest,
                self.catalog_context_digest,
            ]
            .contains(&[0; 32])
        {
            return Err(journal_error("invalid graph receipt identity"));
        }
        let mut out = Vec::new();
        out.extend_from_slice(b"PGR3");
        out.extend_from_slice(&1u64.to_le_bytes());
        out.extend_from_slice(&self.audited_revision.to_le_bytes());
        out.extend_from_slice(self.staging_incarnation.as_bytes());
        append_bytes(
            &mut out,
            &self.manifest.encode_value().map_err(journal_error)?,
            REFERENCE_LIMIT,
        )?;
        out.extend_from_slice(&self.object_count.to_le_bytes());
        for digest in [
            self.inventory_digest,
            self.physical_graph_digest,
            self.final_source_digest,
            self.catalog_context_digest,
        ] {
            out.extend_from_slice(&digest);
        }
        out.push(u8::from(self.snapshot_backed));
        out.extend_from_slice(&self.highest_inode.to_le_bytes());
        finish_record(out, REFERENCE_LIMIT + 512)
    }

    fn decode(bytes: &[u8]) -> Result<Self, WorkspaceError> {
        let mut c = JournalCursor::checked(bytes, b"PGR3", REFERENCE_LIMIT + 512)?;
        let audited_revision = c.u64()?;
        let staging_incarnation = Uuid::from_bytes(c.take()?);
        let manifest =
            SnapshotObjectRef::decode_value(&c.bytes(REFERENCE_LIMIT)?).map_err(journal_error)?;
        let object_count = c.u64()?;
        let inventory_digest = c.take()?;
        let physical_graph_digest = c.take()?;
        let final_source_digest = c.take()?;
        let catalog_context_digest = c.take()?;
        let snapshot_backed = match c.take::<1>()?[0] {
            0 => false,
            1 => true,
            _ => return Err(journal_error("invalid graph receipt source mode")),
        };
        let highest_inode = c.u64()?;
        c.end()?;
        let result = Self {
            audited_revision,
            staging_incarnation,
            manifest,
            object_count,
            inventory_digest,
            physical_graph_digest,
            final_source_digest,
            catalog_context_digest,
            snapshot_backed,
            highest_inode,
        };
        result.encode()?;
        Ok(result)
    }

    fn digest(&self) -> Result<[u8; 32], WorkspaceError> {
        Ok(Sha256::digest(self.encode()?).into())
    }
}

struct JournalGraphMembers<'a, B: WorkspaceKvBackend> {
    store: &'a KvWorkspaceStore<B>,
    expected: &'a PackedJournalRecord,
    budget: &'a Arc<SnapshotMountBudget>,
}

#[async_trait]
impl<B: WorkspaceKvBackend> SnapshotStagedObjectVerifier for JournalGraphMembers<'_, B> {
    async fn verify_reference(
        &self,
        reference: &SnapshotObjectRef,
    ) -> crate::workspace::packed::PackedResult<()> {
        let result = async {
            let _owner = self
                .budget
                .admit(&[(SnapshotBudgetPool::Metadata, OPERATION_BYTES)])
                .map_err(journal_budget_error)?;
            let values = self
                .store
                .packed_journal_values(&[
                    journal_key(self.expected.journal_id),
                    active_key(self.expected.journal_id),
                    identity_key(self.expected.journal_id, &reference.key),
                ])
                .await?;
            let exact = self.expected.encode()?;
            if values.len() != 3
                || values[0].as_deref() != Some(exact.as_slice())
                || values[1].as_deref() != Some(exact.as_slice())
            {
                return Err(WorkspaceError::Busy);
            }
            let ordinal = u64::from_le_bytes(
                values[2]
                    .as_deref()
                    .ok_or_else(|| {
                        journal_error("graph dependency is absent from durable staging")
                    })?
                    .try_into()
                    .map_err(journal_error)?,
            );
            let object = self
                .store
                .reopen_packed_object(self.expected, ordinal, self.budget)
                .await?;
            if object.reference != *reference || !object.uploaded || !object.readback_recorded {
                return Err(journal_error(
                    "graph dependency differs from exact durable typed object",
                ));
            }
            self.store
                .verify_registry_graph_member(self.expected, reference, ordinal)
                .await?;
            Ok(())
        }
        .await;
        result
            .map_err(|error| crate::workspace::packed::PackedWireError::Backend(error.to_string()))
    }
}

fn journal_error(message: impl std::fmt::Display) -> WorkspaceError {
    WorkspaceError::CorruptMetadata(format!("packed journal: {message}"))
}
fn journal_budget_error(error: crate::workspace::packed::PackedWireError) -> WorkspaceError {
    match error {
        crate::workspace::packed::PackedWireError::LimitExceeded(message) => {
            WorkspaceError::InvalidReadPlan(message)
        }
        crate::workspace::packed::PackedWireError::Backend(message) => {
            WorkspaceError::Backend(message)
        }
        other => journal_error(other),
    }
}
fn journal_key(id: JournalId) -> Vec<u8> {
    [JOURNAL_PREFIX, id.to_string().as_bytes()].concat()
}
fn active_key(id: JournalId) -> Vec<u8> {
    [ACTIVE_PREFIX, id.to_string().as_bytes()].concat()
}
fn object_key(id: JournalId, ordinal: u64) -> Vec<u8> {
    format!("packed/staging/{id}/objects/{ordinal:016x}").into_bytes()
}
fn identity_key(id: JournalId, key: &str) -> Vec<u8> {
    format!(
        "packed/staging/{id}/keys/{}",
        hex::encode(Sha256::digest(key.as_bytes()))
    )
    .into_bytes()
}
fn inventory_start() -> [u8; 32] {
    Sha256::digest(INVENTORY_DOMAIN).into()
}
fn active_count(raw: &Option<Vec<u8>>) -> Result<u64, WorkspaceError> {
    let count = raw
        .as_ref()
        .map(|bytes| {
            bytes
                .as_slice()
                .try_into()
                .map(u64::from_le_bytes)
                .map_err(journal_error)
        })
        .transpose()?
        .unwrap_or(0);
    if count > MAX_ACTIVE_JOURNALS as u64 {
        return Err(journal_error("active journal count exceeds bound"));
    }
    Ok(count)
}
fn inventory_append(
    previous: [u8; 32],
    ordinal: u64,
    reference: &SnapshotObjectRef,
) -> Result<[u8; 32], WorkspaceError> {
    let bytes = reference.encode_value().map_err(journal_error)?;
    let mut hash = Sha256::new();
    hash.update(INVENTORY_DOMAIN);
    hash.update(previous);
    hash.update(ordinal.to_le_bytes());
    hash.update((bytes.len() as u32).to_le_bytes());
    hash.update(bytes);
    Ok(hash.finalize().into())
}
fn append_bytes(out: &mut Vec<u8>, bytes: &[u8], limit: usize) -> Result<(), WorkspaceError> {
    if bytes.len() > limit {
        return Err(journal_error("encoded field exceeds limit"));
    }
    out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    out.extend_from_slice(bytes);
    Ok(())
}
fn finish_record(mut bytes: Vec<u8>, limit: usize) -> Result<Vec<u8>, WorkspaceError> {
    if bytes.len() > limit - 32 {
        return Err(journal_error("sidecar exceeds record limit"));
    }
    let digest = Sha256::digest(&bytes);
    bytes.extend_from_slice(&digest);
    Ok(bytes)
}

struct JournalCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}
impl<'a> JournalCursor<'a> {
    fn checked(bytes: &'a [u8], magic: &[u8; 4], limit: usize) -> Result<Self, WorkspaceError> {
        if bytes.len() < 40 || bytes.len() > limit {
            return Err(journal_error("invalid sidecar length"));
        }
        let (body, digest) = bytes.split_at(bytes.len() - 32);
        if Sha256::digest(body).as_slice() != digest {
            return Err(journal_error("sidecar digest mismatch"));
        }
        let mut cursor = Self {
            bytes: body,
            offset: 0,
        };
        if cursor.take::<4>()? != *magic || cursor.u64()? != 1 {
            return Err(WorkspaceError::UnsupportedVolumeFormat(
                "packed journal v3 sidecar version".into(),
            ));
        }
        Ok(cursor)
    }
    fn take<const N: usize>(&mut self) -> Result<[u8; N], WorkspaceError> {
        let end = self
            .offset
            .checked_add(N)
            .ok_or_else(|| journal_error("cursor overflow"))?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| journal_error("truncated field"))?;
        self.offset = end;
        value.try_into().map_err(journal_error)
    }
    fn u64(&mut self) -> Result<u64, WorkspaceError> {
        Ok(u64::from_le_bytes(self.take()?))
    }
    fn bytes(&mut self, limit: usize) -> Result<Vec<u8>, WorkspaceError> {
        let length = u32::from_le_bytes(self.take()?) as usize;
        if length > limit {
            return Err(journal_error("declared field exceeds bound"));
        }
        let end = self
            .offset
            .checked_add(length)
            .ok_or_else(|| journal_error("field overflow"))?;
        let bytes = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| journal_error("truncated variable field"))?;
        self.offset = end;
        Ok(bytes.to_vec())
    }
    fn string(&mut self, limit: usize) -> Result<String, WorkspaceError> {
        String::from_utf8(self.bytes(limit)?).map_err(journal_error)
    }
    fn end(self) -> Result<(), WorkspaceError> {
        if self.offset != self.bytes.len() {
            return Err(journal_error("sidecar trailing bytes"));
        }
        Ok(())
    }
}

impl PackedJournalRecord {
    pub(crate) fn native_completion_identities(
        &self,
    ) -> Result<(JournalId, LayerId, u64, BaseRevision), WorkspaceError> {
        self.validate()?;
        let native = self.native_rebind.as_ref().ok_or(WorkspaceError::Fenced)?;
        let publication = native.publication.as_ref().ok_or(WorkspaceError::Fenced)?;
        Ok((
            native.native_journal_id,
            native.planned_head_layer_id,
            publication.source_sealed_version,
            publication.carrier_revision(),
        ))
    }

    pub(crate) fn final_source_guard(&self) -> Option<&HeadGuard> {
        self.native_rebind
            .as_ref()?
            .publication
            .as_ref()?
            .final_source_guard
            .as_ref()
    }

    pub(crate) fn final_source_hash_facts(&self) -> Option<([u8; 32], [u8; 32])> {
        let publication = self.native_rebind.as_ref()?.publication.as_ref()?;
        Some((
            publication.final_source_root_hash?,
            publication.final_source_delta_digest?,
        ))
    }

    pub(crate) fn with_final_completion_guard(
        &self,
        guard: HeadGuard,
    ) -> Result<Self, WorkspaceError> {
        self.validate()?;
        if self.phase != PackedJournalPhase::Committed {
            return Err(WorkspaceError::Fenced);
        }
        let mut next = self.clone();
        next.revision = next.revision.checked_add(1).ok_or(WorkspaceError::Fenced)?;
        next.native_rebind
            .as_mut()
            .ok_or(WorkspaceError::Fenced)?
            .publication
            .as_mut()
            .ok_or(WorkspaceError::Fenced)?
            .final_source_guard = Some(guard);
        next.validate()?;
        Ok(next)
    }

    fn validate(&self) -> Result<(), WorkspaceError> {
        if self.journal_id.as_uuid().is_nil()
            || self.revision == 0
            || self.object_count > MAX_OBJECTS
            || self.source.effective_view_digest == [0; 32]
            || self.source.frozen_view_token == [0; 32]
            || self.source.build_provenance_digest == [0; 32]
            || self.source.staging_id.is_nil()
            || self.source.build_owner.is_empty()
            || self.source.build_owner.len() > 256
            || self.source.staging_prefix.is_empty()
            || self.source.staging_prefix.len() > 4096
            || self.abort_reason.len() > 1024
            || (self.phase == PackedJournalPhase::Aborted) != !self.abort_reason.is_empty()
            || (self.object_count == 0 && self.inventory_digest != inventory_start())
            || (matches!(
                self.phase,
                PackedJournalPhase::Verified | PackedJournalPhase::Committed
            ) && self.full_proof_digest == [0; 32])
            || (!matches!(
                self.phase,
                PackedJournalPhase::Verified
                    | PackedJournalPhase::Committed
                    | PackedJournalPhase::Aborted
            ) && self.full_proof_digest != [0; 32])
        {
            return Err(journal_error("invalid journal identity/progress"));
        }
        if let Some(receipt) = &self.graph_receipt {
            receipt.encode()?;
            let allowed_revision = match self.phase {
                PackedJournalPhase::AwaitingFullProof => Some(1),
                PackedJournalPhase::Verified => Some(2),
                PackedJournalPhase::Committed => Some(3),
                PackedJournalPhase::Aborted => None,
                _ => return Err(journal_error("graph receipt preceded completed readback")),
            };
            if receipt.staging_incarnation != self.source.staging_id
                || receipt.object_count != self.object_count
                || receipt.inventory_digest != self.inventory_digest
                || receipt.snapshot_backed != self.source.snapshot_backed
                || self.commit_target.as_ref().is_none_or(|target| {
                    target.binding.manifest != receipt.manifest
                        || target.highest_inode < receipt.highest_inode as i64
                })
                || allowed_revision.is_some_and(|offset| {
                    receipt.audited_revision.checked_add(offset) != Some(self.revision)
                })
                || receipt.catalog_context_digest
                    != audit_basis_digest(self, receipt.audited_revision)?
            {
                return Err(journal_error(
                    "graph receipt no longer binds journal incarnation/revision/context",
                ));
            }
        }
        if matches!(
            self.phase,
            PackedJournalPhase::Verified | PackedJournalPhase::Committed
        ) && self.graph_receipt.is_none()
        {
            return Err(journal_error(
                "publication phase lacks a durable graph receipt",
            ));
        }
        let head: LayerRecord = decode(&self.expected_head)?;
        let base: LayerRecord = decode(&self.expected_base)?;
        if let Some(native) = &self.native_rebind {
            native.validate(self, &head, &base)?;
        }
        validate_layer_chain(
            self.guard.expected_head_layer_id,
            &[head.clone(), base.clone()],
        )?;
        self.expected_binding
            .validate_for_guard(&self.guard, &base)?;
        if head.state != LayerState::Writable
            || head.owner_workspace_id != Some(self.guard.workspace_id)
            || head.layer_id != self.guard.expected_head_layer_id
        {
            return Err(journal_error(
                "journal does not bind its original writable head",
            ));
        }
        if self.phase == PackedJournalPhase::Building && self.commit_target.is_some() {
            return Err(journal_error("building journal has a frozen target"));
        }
        if !matches!(
            self.phase,
            PackedJournalPhase::Building | PackedJournalPhase::Aborted
        ) && self.commit_target.is_none()
        {
            return Err(journal_error("frozen journal is missing its exact target"));
        }
        if let Some(target) = &self.commit_target {
            target.encode()?;
            let native_target = self
                .native_rebind
                .as_ref()
                .and_then(|basis| basis.publication.as_ref());
            if target.workspace_id != self.guard.workspace_id
                || (native_target.is_none() && target.head_layer_id != head.layer_id)
                || (native_target.is_none()
                    && target.base_revision != self.expected_binding.base_revision)
                || target.head_epoch
                    != self
                        .guard
                        .expected_head_epoch
                        .checked_add(1)
                        .ok_or_else(|| journal_error("epoch overflow"))?
                || target.binding.binding_version
                    != self
                        .expected_binding
                        .binding
                        .binding_version
                        .checked_add(1)
                        .ok_or_else(|| journal_error("binding version overflow"))?
                || target.binding.manifest == self.expected_binding.binding.manifest
                || target.highest_inode < self.expected_binding.highest_inode
            {
                return Err(journal_error(
                    "target changed journal topology or did not advance manifest",
                ));
            }
        }
        Ok(())
    }
    pub(crate) fn encode(&self) -> Result<Vec<u8>, WorkspaceError> {
        self.validate()?;
        let mut out = Vec::new();
        out.extend_from_slice(b"PPJ3");
        out.extend_from_slice(&1u64.to_le_bytes());
        out.extend_from_slice(self.journal_id.as_bytes());
        out.extend_from_slice(&self.revision.to_le_bytes());
        out.push(self.phase as u8);
        for id in [
            self.guard.workspace_id.as_bytes(),
            self.guard.expected_head_layer_id.as_bytes(),
            self.guard.lease_id.as_bytes(),
        ] {
            out.extend_from_slice(id);
        }
        out.extend_from_slice(&self.guard.expected_head_epoch.to_le_bytes());
        out.extend_from_slice(&self.guard.holder_generation.to_le_bytes());
        out.push(u8::from(self.source.snapshot_backed));
        for digest in [
            self.source.effective_view_digest,
            self.source.frozen_view_token,
            self.source.build_provenance_digest,
        ] {
            out.extend_from_slice(&digest);
        }
        append_bytes(&mut out, self.source.build_owner.as_bytes(), 256)?;
        out.extend_from_slice(self.source.staging_id.as_bytes());
        append_bytes(&mut out, self.source.staging_prefix.as_bytes(), 4096)?;
        append_bytes(&mut out, &self.expected_head, REFERENCE_LIMIT)?;
        append_bytes(&mut out, &self.expected_base, REFERENCE_LIMIT)?;
        append_bytes(&mut out, &self.expected_binding.encode()?, REFERENCE_LIMIT)?;
        out.extend_from_slice(&self.object_count.to_le_bytes());
        out.extend_from_slice(&self.inventory_digest);
        let target = self
            .commit_target
            .as_ref()
            .map(PackedLowerBindingRecord::encode)
            .transpose()?
            .unwrap_or_default();
        append_bytes(&mut out, &target, REFERENCE_LIMIT)?;
        out.extend_from_slice(&self.full_proof_digest);
        let graph_receipt = self
            .graph_receipt
            .as_ref()
            .map(PackedGraphReceipt::encode)
            .transpose()?
            .unwrap_or_default();
        append_bytes(&mut out, &graph_receipt, REFERENCE_LIMIT + 512)?;
        let native_rebind = self
            .native_rebind
            .as_ref()
            .map(native_rebind::PackedNativeJournalBasis::encode)
            .transpose()?
            .unwrap_or_default();
        append_bytes(&mut out, &native_rebind, 5 * REFERENCE_LIMIT + 384)?;
        append_bytes(&mut out, self.abort_reason.as_bytes(), 1024)?;
        finish_record(out, RECORD_LIMIT)
    }
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, WorkspaceError> {
        let mut c = JournalCursor::checked(bytes, b"PPJ3", RECORD_LIMIT)?;
        let journal_id = JournalId::from_uuid(Uuid::from_bytes(c.take()?));
        let revision = c.u64()?;
        let phase = PackedJournalPhase::decode(c.take::<1>()?[0])?;
        let workspace_id = WorkspaceId::from_uuid(Uuid::from_bytes(c.take()?));
        let expected_head_layer_id = LayerId::from_uuid(Uuid::from_bytes(c.take()?));
        let lease_id = LeaseId::from_uuid(Uuid::from_bytes(c.take()?));
        let expected_head_epoch = c.u64()?;
        let holder_generation = c.u64()?;
        let snapshot_backed = match c.take::<1>()?[0] {
            0 => false,
            1 => true,
            _ => return Err(journal_error("invalid source mode")),
        };
        let effective_view_digest = c.take()?;
        let frozen_view_token = c.take()?;
        let build_provenance_digest = c.take()?;
        let build_owner = c.string(256)?;
        let staging_id = Uuid::from_bytes(c.take()?);
        let staging_prefix = c.string(4096)?;
        let expected_head = c.bytes(REFERENCE_LIMIT)?;
        let expected_base = c.bytes(REFERENCE_LIMIT)?;
        let expected_binding = PackedLowerBindingRecord::decode(&c.bytes(REFERENCE_LIMIT)?)?;
        let object_count = c.u64()?;
        let inventory_digest = c.take()?;
        let target = c.bytes(REFERENCE_LIMIT)?;
        let commit_target = if target.is_empty() {
            None
        } else {
            Some(PackedLowerBindingRecord::decode(&target)?)
        };
        let full_proof_digest = c.take()?;
        let graph_receipt = c.bytes(REFERENCE_LIMIT + 512)?;
        let graph_receipt = if graph_receipt.is_empty() {
            None
        } else {
            Some(PackedGraphReceipt::decode(&graph_receipt)?)
        };
        let native_rebind = c.bytes(5 * REFERENCE_LIMIT + 384)?;
        let native_rebind = if native_rebind.is_empty() {
            None
        } else {
            Some(native_rebind::PackedNativeJournalBasis::decode(
                &native_rebind,
            )?)
        };
        let abort_reason = c.string(1024)?;
        c.end()?;
        let record = Self {
            journal_id,
            revision,
            phase,
            guard: HeadGuard {
                workspace_id,
                expected_head_layer_id,
                expected_head_epoch,
                lease_id,
                holder_generation,
            },
            source: PackedSourceView {
                snapshot_backed,
                effective_view_digest,
                frozen_view_token,
                build_provenance_digest,
                build_owner,
                staging_id,
                staging_prefix,
            },
            expected_head,
            expected_base,
            expected_binding,
            object_count,
            inventory_digest,
            commit_target,
            full_proof_digest,
            graph_receipt,
            native_rebind,
            abort_reason,
        };
        record.validate()?;
        Ok(record)
    }
    fn next(&self) -> Result<Self, WorkspaceError> {
        let mut next = self.clone();
        next.revision = self
            .revision
            .checked_add(1)
            .ok_or_else(|| journal_error("journal revision overflow"))?;
        Ok(next)
    }
}

impl PackedJournalObject {
    fn encode(&self) -> Result<Vec<u8>, WorkspaceError> {
        if self.ordinal >= MAX_OBJECTS || (self.readback_recorded && !self.uploaded) {
            return Err(journal_error("invalid object progress"));
        }
        let mut out = Vec::new();
        out.extend_from_slice(b"PJO3");
        out.extend_from_slice(&1u64.to_le_bytes());
        out.extend_from_slice(&self.ordinal.to_le_bytes());
        out.push(u8::from(self.uploaded));
        out.push(u8::from(self.readback_recorded));
        append_bytes(
            &mut out,
            &self.reference.encode_value().map_err(journal_error)?,
            REFERENCE_LIMIT,
        )?;
        finish_record(out, REFERENCE_LIMIT + 128)
    }
    fn decode(bytes: &[u8]) -> Result<Self, WorkspaceError> {
        let mut c = JournalCursor::checked(bytes, b"PJO3", REFERENCE_LIMIT + 128)?;
        let ordinal = c.u64()?;
        let flags = c.take::<2>()?;
        if flags.iter().any(|flag| *flag > 1) {
            return Err(journal_error("invalid object flags"));
        }
        let reference =
            SnapshotObjectRef::decode_value(&c.bytes(REFERENCE_LIMIT)?).map_err(journal_error)?;
        c.end()?;
        let object = Self {
            ordinal,
            reference,
            uploaded: flags[0] == 1,
            readback_recorded: flags[1] == 1,
        };
        object.encode()?;
        Ok(object)
    }
}

struct JournalAuthorities {
    checks: Vec<KvCheck>,
    values: Vec<Option<Vec<u8>>>,
    authority_deadline_ns: i64,
    lease: SnapshotLease,
    workspace: WorkspaceRecord,
    head: LayerRecord,
    base: LayerRecord,
    now: i64,
}

impl<B: WorkspaceKvBackend> KvWorkspaceStore<B> {
    // Callers admit OPERATION_BYTES before this bounded transport read; the
    // result remains inside that owner while codecs/checks clone its rows.
    async fn packed_journal_values(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<Vec<u8>>>, WorkspaceError> {
        Ok(self
            .backend
            .get_many_consistent_with_time_bounded(keys, journal_point_limits())
            .await?
            .0)
    }
    async fn packed_journal_value(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, WorkspaceError> {
        let mut values = self.packed_journal_values(&[key]).await?;
        if values.len() != 1 {
            return Err(journal_error("short bounded journal point read"));
        }
        Ok(values.remove(0))
    }
    async fn packed_journal_authorities(
        &self,
        record: &PackedJournalRecord,
        extra_keys: &[Vec<u8>],
    ) -> Result<JournalAuthorities, WorkspaceError> {
        if record.native_rebind.is_some() {
            // A durable receipt cannot replace an actual in-process native
            // authority. Rebound records use the dedicated typed CAS path.
            return Err(WorkspaceError::Fenced);
        }
        let base: LayerRecord = decode(&record.expected_base)?;
        let mut keys = vec![
            hot_workspace_key(record.guard.workspace_id),
            hot_layer_key(record.guard.expected_head_layer_id),
            hot_lease_key(record.guard.workspace_id, record.guard.lease_id),
            hot_layer_key(base.layer_id),
            packed_current_key(record.guard.workspace_id),
            packed_claim_key(record.guard.workspace_id),
            packed_history_key(
                record.guard.workspace_id,
                record.expected_binding.binding.binding_version,
            ),
            packed_history_key(record.guard.workspace_id, 1),
            PACKED_ROOT_GENERATION_KEY.to_vec(),
            LAYER_INVENTORY_GENERATION_KEY.to_vec(),
            journal_key(record.journal_id),
            hot_allocator_key("inode"),
        ];
        keys.extend_from_slice(extra_keys);
        let (values, now) = self
            .backend
            .get_many_consistent_with_time_bounded(&keys, journal_point_limits())
            .await?;
        if values.len() != keys.len() {
            return Err(journal_error("short consistent journal read"));
        }
        let workspace: WorkspaceRecord = decode_required(&values[0])?;
        let head: LayerRecord = decode_required(&values[1])?;
        let lease: SnapshotLease = decode_required(&values[2])?;
        let actual_base: LayerRecord = decode_required(&values[3])?;
        let current = values[10]
            .as_deref()
            .map(PackedJournalRecord::decode)
            .transpose()?;
        let effective = current.as_ref().unwrap_or(record);
        if effective.journal_id != record.journal_id
            || effective.source != record.source
            || effective.expected_head != record.expected_head
            || effective.expected_base != record.expected_base
            || effective.expected_binding != record.expected_binding
            || effective.guard != record.guard
        {
            return Err(WorkspaceError::Fenced);
        }
        let mut guard = record.guard.clone();
        let mut expected_head: LayerRecord = decode(&record.expected_head)?;
        let expected_binding = if effective.phase == PackedJournalPhase::Committed {
            let target = effective
                .commit_target
                .as_ref()
                .ok_or_else(|| journal_error("committed target missing"))?;
            guard.expected_head_epoch = target.head_epoch;
            allocate_layer_sequences(&mut expected_head, 1)?;
            target
        } else {
            &record.expected_binding
        };
        checked_hot_guard(&workspace, &head, &lease, &guard, now)?;
        if head != expected_head
            || actual_base != base
            || values[4].as_deref() != Some(expected_binding.encode()?.as_slice())
            || values[5].as_deref() != Some(PACKED_CLAIM)
            || values[6].as_deref() != Some(record.expected_binding.encode()?.as_slice())
        {
            return Err(WorkspaceError::Busy);
        }
        let initial = PackedLowerBindingRecord::decode(
            values[7]
                .as_deref()
                .ok_or_else(|| journal_error("initial history missing"))?,
        )?;
        if initial.workspace_id != guard.workspace_id || initial.binding.binding_version != 1 {
            return Err(journal_error("initial history identity mismatch"));
        }
        next_packed_root_generation(&values[8])?;
        layer_inventory_generation(&values[9])?;
        let checks = keys
            .into_iter()
            .zip(values.iter().cloned())
            .map(|(key, expected)| KvCheck { key, expected })
            .collect();
        Ok(JournalAuthorities {
            checks,
            values,
            authority_deadline_ns: lease.expires_at_ns,
            lease,
            workspace,
            head,
            base: actual_base,
            now,
        })
    }

    /// One transaction for journal, active pin, object progress and root fence.
    /// Exact lost-reply retries issue a timed read-only CAS, never a new phase.
    async fn packed_journal_write(
        &self,
        expected: Option<&PackedJournalRecord>,
        next: &PackedJournalRecord,
        changes: &[JournalChange],
        commit: Option<&PublishPackedLowerBinding>,
    ) -> Result<PackedJournalRecord, WorkspaceError> {
        self.packed_journal_write_under(expected, next, changes, commit, None)
            .await
    }

    async fn packed_journal_write_under(
        &self,
        expected: Option<&PackedJournalRecord>,
        next: &PackedJournalRecord,
        changes: &[JournalChange],
        commit: Option<&PublishPackedLowerBinding>,
        native: Option<&native_publication::NativeJournalAuthority<'_, B>>,
    ) -> Result<PackedJournalRecord, WorkspaceError> {
        const MAX_DEFINITE_CONFLICT_ATTEMPTS: usize = 3;
        let mut _rebuild_owner = None;
        let mut rebuild: Option<(Vec<KvCheck>, i64)> = None;
        for attempt in 0..MAX_DEFINITE_CONFLICT_ATTEMPTS {
            let conflict = self
                .packed_journal_write_attempt_under(
                    expected,
                    next,
                    changes,
                    commit,
                    native,
                    rebuild
                        .as_ref()
                        .map(|(checks, deadline)| (checks.as_slice(), *deadline)),
                )
                .await?;
            let Some(conflict) = conflict else {
                return Ok(next.clone());
            };
            if attempt + 1 == MAX_DEFINITE_CONFLICT_ATTEMPTS {
                return Err(WorkspaceError::Busy);
            }
            if let Some((_, deadline)) = &mut rebuild {
                *deadline = (*deadline).min(conflict.1);
            } else {
                let budget = self.packed_reader_pin_budget.get().ok_or(
                    WorkspaceError::UnsupportedCapability("native journal rebuild budget"),
                )?;
                // Retain the exact original packet while the next complete
                // authority/read/codec packet uses its existing operation tier.
                _rebuild_owner = Some(
                    budget
                        .admit(&[(SnapshotBudgetPool::Metadata, 4 << 20)])
                        .map_err(journal_budget_error)?,
                );
                rebuild = Some(conflict);
            }
            tokio::task::yield_now().await;
        }
        Err(WorkspaceError::Busy)
    }

    // Some is returned only after an actual mutation CAS answered false.
    // Errors, including Busy during reads and uncertain replies, never enter
    // the wrapper's rebuild loop. A successful exact read-only confirmation
    // returns None without resubmitting any writes.
    async fn packed_journal_write_attempt_under(
        &self,
        expected: Option<&PackedJournalRecord>,
        next: &PackedJournalRecord,
        changes: &[JournalChange],
        commit: Option<&PublishPackedLowerBinding>,
        native: Option<&native_publication::NativeJournalAuthority<'_, B>>,
        rebuild: Option<(&[KvCheck], i64)>,
    ) -> Result<Option<(Vec<KvCheck>, i64)>, WorkspaceError> {
        next.validate()?;
        if native.is_some() && commit.is_some() {
            return Err(WorkspaceError::Fenced);
        }
        let basis = expected.unwrap_or(next);
        let mut extras = vec![active_key(next.journal_id), ACTIVE_COUNT_KEY.to_vec()];
        extras.extend(changes.iter().map(|change| change.0.clone()));
        let mut authorities = match native {
            Some(native) => {
                self.native_staged_journal_authorities(basis, &extras, native)
                    .await?
            }
            None => self.packed_journal_authorities(basis, &extras).await?,
        };
        let actual = authorities.values[10]
            .as_deref()
            .map(PackedJournalRecord::decode)
            .transpose()?;
        let retry = actual.as_ref() == Some(next);
        if !retry && actual.as_ref() != expected {
            return Err(WorkspaceError::Busy);
        }
        let expected_active = actual
            .as_ref()
            .filter(|r| !r.phase.terminal())
            .map(PackedJournalRecord::encode)
            .transpose()?;
        if authorities.values[12] != expected_active {
            return Err(journal_error("active pin and journal disagree"));
        }
        let active = active_count(&authorities.values[13])?;
        for (index, (_, old, new)) in changes.iter().enumerate() {
            if &authorities.values[14 + index] != if retry { new } else { old } {
                return Err(WorkspaceError::Busy);
            }
        }
        // The immutable original Q identity remains in PPJ.guard. A recovered
        // seed owner binds this exact PPJ/staging incarnation in the same CAS.
        // Retain the complete handoff, including its owned read permit, until
        // the submitted transaction or exact successor confirmation ends.
        let native_owner_handoff = if expected.is_none() {
            match native {
                Some(native) => native.original().prepare_native_owner_handoff(next).await?,
                None => None,
            }
        } else {
            None
        };
        if let Some(handoff) = &native_owner_handoff {
            native_publication::append_exact_checks(
                &mut authorities.checks,
                handoff.checks.clone(),
            )?;
            authorities.authority_deadline_ns =
                authorities.authority_deadline_ns.min(handoff.deadline_ns);
        }
        let mut writes = Vec::new();
        if let Some(request) = commit {
            let target = next
                .commit_target
                .as_ref()
                .ok_or_else(|| journal_error("commit target missing"))?;
            if request.record()? != *target
                || request.guard != basis.guard
                || request.expected_binding != basis.expected_binding
                || encode(&request.expected_layers[0])? != basis.expected_head
                || encode(&request.expected_layers[1])? != basis.expected_base
            {
                return Err(WorkspaceError::Fenced);
            }
            let allocator: i64 = decode_required(&authorities.values[11])?;
            if retry {
                let guard = request.validate_committed_state(
                    target,
                    &[authorities.head.clone(), authorities.base.clone()],
                    &revision_from_layer(&authorities.base)?,
                    allocator,
                )?;
                if guard.expected_head_epoch != target.head_epoch {
                    return Err(WorkspaceError::Fenced);
                }
            } else {
                request.validate_first_publication_allocator(allocator)?;
                let mut workspace = authorities.workspace.clone();
                workspace.head_epoch = target.head_epoch;
                workspace.updated_at_ns = authorities.now;
                let mut head = authorities.head.clone();
                allocate_layer_sequences(&mut head, 1)?;
                let floor = target
                    .highest_inode
                    .checked_add(1)
                    .ok_or_else(|| journal_error("inode floor overflow"))?
                    .max(2);
                writes.push(put(authorities.checks[0].key.clone(), &workspace)?);
                writes.push(put(authorities.checks[1].key.clone(), &head)?);
                writes.push(put(
                    authorities.checks[11].key.clone(),
                    &allocator.max(floor),
                )?);
                writes.push(KvWrite::Put {
                    key: authorities.checks[4].key.clone(),
                    value: target.encode()?,
                });
            }
        }
        if !retry {
            let next_count = if actual.is_none() {
                active
                    .checked_add(1)
                    .filter(|count| *count <= MAX_ACTIVE_JOURNALS as u64)
                    .ok_or_else(|| {
                        WorkspaceError::InvalidReadPlan(
                            "active packed journal limit reached".into(),
                        )
                    })?
            } else if next.phase.terminal()
                && actual
                    .as_ref()
                    .is_some_and(|record| !record.phase.terminal())
            {
                active
                    .checked_sub(1)
                    .ok_or_else(|| journal_error("active journal count underflow"))?
            } else {
                active
            };
            writes.push(KvWrite::Put {
                key: ACTIVE_COUNT_KEY.to_vec(),
                value: next_count.to_le_bytes().to_vec(),
            });
            writes.push(KvWrite::Put {
                key: journal_key(next.journal_id),
                value: next.encode()?,
            });
            if next.phase.terminal() {
                writes.push(KvWrite::Delete {
                    key: active_key(next.journal_id),
                });
            } else {
                writes.push(KvWrite::Put {
                    key: active_key(next.journal_id),
                    value: next.encode()?,
                });
            }
            writes.push(put(
                PACKED_ROOT_GENERATION_KEY.to_vec(),
                &next_packed_root_generation(&authorities.values[8])?,
            )?);
            for (key, _, value) in changes {
                writes.push(match value {
                    Some(value) => KvWrite::Put {
                        key: key.clone(),
                        value: value.clone(),
                    },
                    None => KvWrite::Delete { key: key.clone() },
                });
            }
        }
        if let Some(handoff) = &native_owner_handoff {
            for added in &handoff.writes {
                let key = match added {
                    KvWrite::Put { key, .. } | KvWrite::Delete { key } => key,
                };
                if let Some(old) = writes.iter().find(|write| match write {
                    KvWrite::Put { key: candidate, .. } | KvWrite::Delete { key: candidate } => {
                        candidate == key
                    }
                }) {
                    if old != added {
                        return Err(WorkspaceError::Fenced);
                    }
                } else {
                    writes.push(added.clone());
                }
            }
        }
        let _native_holds = self
            .prepare_native_owner_cas(&mut authorities.checks, &mut writes)
            .await?;
        let mut topology_packet = self
            .prepare_topology_envelope(
                authorities.checks,
                writes,
                Some(authorities.authority_deadline_ns),
            )
            .await?;
        authorities.checks = std::mem::take(&mut topology_packet.checks);
        writes = std::mem::take(&mut topology_packet.writes);
        let native_attempt = if native_owner_handoff.is_some() {
            let mut exact = authorities.checks.clone();
            for write in &writes {
                let (key, expected) = match write {
                    KvWrite::Put { key, value } => (key, Some(value.clone())),
                    KvWrite::Delete { key } => (key, None),
                };
                let check = exact
                    .iter_mut()
                    .find(|check| check.key == *key)
                    .ok_or(WorkspaceError::Fenced)?;
                check.expected = expected;
            }
            let limits = KvReadLimits {
                max_records: 64,
                max_key_bytes: 1024,
                max_data_requests: 64,
                ..journal_point_limits()
            };
            let bytes = exact.iter().try_fold(0usize, |sum, check| {
                let length = check.expected.as_ref().map_or(0, Vec::len);
                if check.key.len() > limits.max_key_bytes || length > limits.max_value_bytes {
                    return Err(journal_error(
                        "native owner bind successor exceeds fixed tier",
                    ));
                }
                sum.checked_add(check.key.len())
                    .and_then(|sum| sum.checked_add(length))
                    .ok_or_else(|| journal_error("native owner bind successor aggregate overflow"))
            })?;
            if exact.len() > limits.max_records || bytes > limits.max_total_bytes {
                return Err(journal_error(
                    "native owner bind successor aggregate exceeds fixed tier",
                ));
            }
            Some((exact, limits))
        } else {
            None
        };
        if let Some((original, deadline)) = rebuild {
            // The root fence can advance because another reader renewed.
            // Every original PPJ/change, native/source/open/lease, allocator,
            // and hold predecessor remains exact; no new authority is adopted.
            if authorities.checks.len() != original.len()
                || authorities.checks.iter().zip(original).any(|(fresh, old)| {
                    fresh.key != old.key
                        || (fresh.key.as_slice() != PACKED_ROOT_GENERATION_KEY
                            && fresh.expected != old.expected)
                })
            {
                return Err(WorkspaceError::Busy);
            }
            authorities.authority_deadline_ns = authorities.authority_deadline_ns.min(deadline);
        }
        let cas = self
            .backend
            .compare_and_swap_before(
                &authorities.checks,
                &writes,
                authorities.authority_deadline_ns,
            )
            .await;
        match cas {
            Ok(true) => {}
            Ok(false) => {
                #[cfg(test)]
                if std::env::var_os("BREWFS_TEST_NATIVE_CAS_DIAGNOSTICS").is_some() {
                    eprintln!(
                        "packed journal CAS mismatch phase={:?} revision={} checks={} writes={}",
                        next.phase,
                        next.revision,
                        authorities.checks.len(),
                        writes.len(),
                    );
                    // Diagnostic observations grant no authority. Cap their
                    // keys/bytes and own their storage on the existing budget.
                    if authorities.checks.len() <= 64 {
                        if let Some(budget) = self.packed_reader_pin_budget.get() {
                            if let Ok(_diagnostic_owner) =
                                budget.admit(&[(SnapshotBudgetPool::Metadata, 4 << 20)])
                            {
                                let keys = authorities
                                    .checks
                                    .iter()
                                    .map(|check| check.key.clone())
                                    .collect::<Vec<_>>();
                                let limits = KvReadLimits {
                                    max_records: 64,
                                    max_key_bytes: 1024,
                                    max_data_requests: 64,
                                    ..journal_point_limits()
                                };
                                match self
                                    .backend
                                    .get_many_consistent_with_time_bounded(&keys, limits)
                                    .await
                                {
                                    Ok((actual, now)) if actual.len() == keys.len() => {
                                        let mismatches =
                                            authorities.checks.iter().zip(actual.iter()).filter(
                                                |(check, actual)| {
                                                    check.expected.as_deref() != actual.as_deref()
                                                },
                                            );
                                        let mut changed = 0;
                                        for (check, actual) in mismatches {
                                            if changed < 8 {
                                                eprintln!(
                                                    "packed journal post-CAS mismatch key_class={} key_sha256={} expected_len={:?} actual_len={:?}",
                                                    if check.key.as_slice()
                                                        == PACKED_ROOT_GENERATION_KEY
                                                    {
                                                        "root-generation"
                                                    } else if check.key.as_slice()
                                                        == LAYER_INVENTORY_GENERATION_KEY
                                                    {
                                                        "layer-generation"
                                                    } else if check.key.as_slice() == CONTROL_KEY {
                                                        "control"
                                                    } else {
                                                        "other"
                                                    },
                                                    hex::encode(Sha256::digest(&check.key)),
                                                    check.expected.as_ref().map(Vec::len),
                                                    actual.as_ref().map(Vec::len),
                                                );
                                            }
                                            changed += 1;
                                        }
                                        eprintln!(
                                            "packed journal post-CAS observation changed={} sampled_ns={} original_deadline_ns={}",
                                            changed, now, authorities.authority_deadline_ns,
                                        );
                                    }
                                    Ok((actual, _)) => eprintln!(
                                        "packed journal post-CAS diagnostic short_read={} expected={}",
                                        actual.len(),
                                        keys.len(),
                                    ),
                                    Err(error) => eprintln!(
                                        "packed journal post-CAS diagnostic failed: {error}",
                                    ),
                                }
                            } else {
                                eprintln!("packed journal post-CAS diagnostic skipped=budget");
                            }
                        } else {
                            eprintln!("packed journal post-CAS diagnostic skipped=no-budget");
                        }
                    } else {
                        eprintln!("packed journal post-CAS diagnostic skipped=key-cap");
                    }
                }
                if native.is_none() || retry || writes.is_empty() {
                    return Err(WorkspaceError::Busy);
                }
                // Bound the retained original conditions before the wrapper
                // admits their separate owner and builds a fresh full packet.
                let bytes = authorities.checks.iter().try_fold(0usize, |sum, check| {
                    let length = check.expected.as_ref().map_or(0, Vec::len);
                    if check.key.len() > 1024 || length > RECORD_LIMIT {
                        return Err(journal_error(
                            "native journal rebuild point exceeds fixed tier",
                        ));
                    }
                    sum.checked_add(check.key.len())
                        .and_then(|sum| sum.checked_add(length))
                        .ok_or_else(|| journal_error("native journal rebuild aggregate overflow"))
                })?;
                if authorities.checks.len() > 64 || bytes > 2 << 20 {
                    return Err(journal_error("native journal rebuild exceeds fixed tier"));
                }
                return Ok(Some((
                    authorities.checks,
                    authorities.authority_deadline_ns,
                )));
            }
            Err(error @ WorkspaceError::Backend(_)) if native_owner_handoff.is_some() => {
                // Never resubmit an uncertain first bind. Every unchanged
                // authority and every actual attempted successor must match.
                let confirmed = async {
                    let (exact, limits) = native_attempt.as_ref().ok_or(WorkspaceError::Fenced)?;
                    let keys = exact
                        .iter()
                        .map(|check| check.key.clone())
                        .collect::<Vec<_>>();
                    let (actual, now) = self
                        .backend
                        .get_many_consistent_with_time_bounded(&keys, *limits)
                        .await?;
                    if actual.len() != exact.len()
                        || now <= 0
                        || now >= authorities.authority_deadline_ns
                        || actual
                            .iter()
                            .zip(exact.iter())
                            .any(|(actual, check)| *actual != check.expected)
                    {
                        return Ok(false);
                    }
                    self.backend
                        .compare_and_swap_before(exact, &[], authorities.authority_deadline_ns)
                        .await
                }
                .await;
                if !matches!(confirmed, Ok(true)) {
                    return Err(error);
                }
            }
            Err(error) => return Err(error),
        }
        Ok(None)
    }

    pub(crate) async fn begin_packed_journal(
        &self,
        journal_id: JournalId,
        guard: HeadGuard,
        expected_layers: [LayerRecord; 2],
        expected_binding: PackedLowerBindingRecord,
        source: PackedSourceView,
        budget: &Arc<SnapshotMountBudget>,
    ) -> Result<OwnedPackedJournal<PackedJournalRecord>, WorkspaceError> {
        self.configure_packed_reader_pin_budget(budget.clone())?;
        let _owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, OPERATION_BYTES)])
            .map_err(journal_budget_error)?;
        let record = PackedJournalRecord {
            journal_id,
            revision: 1,
            phase: PackedJournalPhase::Building,
            guard,
            source,
            expected_head: encode(&expected_layers[0])?,
            expected_base: encode(&expected_layers[1])?,
            expected_binding,
            object_count: 0,
            inventory_digest: inventory_start(),
            commit_target: None,
            full_proof_digest: [0; 32],
            graph_receipt: None,
            native_rebind: None,
            abort_reason: String::new(),
        };
        let feature = self
            .packed_journal_value(JOURNAL_FEATURE_KEY.to_vec())
            .await?;
        if feature.as_deref().is_some_and(|value| value != b"PPJ3") {
            return Err(journal_error("journal feature sentinel is corrupt"));
        }
        self.packed_journal_write(
            None,
            &record,
            &[
                (
                    JOURNAL_FEATURE_KEY.to_vec(),
                    feature,
                    Some(b"PPJ3".to_vec()),
                ),
                registry::begin_root_change(&record)?,
            ],
            None,
        )
        .await
        .retain(_owner)
    }

    /// Read-only recovery: exact persisted view and root, even after lease
    /// expiry. This does not authorize resume/commit or synthesize a new view.
    pub(crate) async fn reopen_packed_journal(
        &self,
        id: JournalId,
        source: &PackedSourceView,
        budget: &Arc<SnapshotMountBudget>,
    ) -> Result<OwnedPackedJournal<PackedJournalRecord>, WorkspaceError> {
        let _owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, OPERATION_BYTES)])
            .map_err(journal_budget_error)?;
        let bytes = self
            .packed_journal_value(journal_key(id))
            .await?
            .ok_or_else(|| journal_error("journal missing"))?;
        let record = PackedJournalRecord::decode(&bytes)?;
        if record.journal_id != id || &record.source != source {
            return Err(WorkspaceError::Fenced);
        }
        Ok(record).retain(_owner)
    }

    /// One referenced object at a time. The journal revision is read with it,
    /// so a cursor cannot splice two staging inventories or progress versions.
    pub(crate) async fn reopen_packed_object(
        &self,
        expected: &PackedJournalRecord,
        ordinal: u64,
        budget: &Arc<SnapshotMountBudget>,
    ) -> Result<OwnedPackedJournal<PackedJournalObject>, WorkspaceError> {
        let _owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, OPERATION_BYTES)])
            .map_err(journal_budget_error)?;
        if ordinal >= expected.object_count {
            return Err(journal_error("object ordinal outside journal"));
        }
        let values = self
            .packed_journal_values(&[
                journal_key(expected.journal_id),
                object_key(expected.journal_id, ordinal),
            ])
            .await?;
        if values.len() != 2 || values[0].as_deref() != Some(expected.encode()?.as_slice()) {
            return Err(WorkspaceError::Busy);
        }
        let object = PackedJournalObject::decode(
            values[1]
                .as_deref()
                .ok_or_else(|| journal_error("staging object missing"))?,
        )?;
        if object.ordinal != ordinal {
            return Err(journal_error("staging key/ordinal disagree"));
        }
        Ok(object).retain(_owner)
    }

    /// Foundation metadata/CAS fixture. It explicitly simulates successful
    /// remote completion and cannot be called by any production producer.
    #[cfg(test)]
    pub(crate) async fn register_packed_object(
        &self,
        expected: &PackedJournalRecord,
        reference: SnapshotObjectRef,
        budget: &Arc<SnapshotMountBudget>,
    ) -> Result<OwnedPackedJournal<PackedJournalRecord>, WorkspaceError> {
        let (reserved, guard) = self
            .reserve_packed_upload(expected, reference, budget)
            .await?;
        let dispatched = self
            .dispatch_packed_upload(&reserved, &guard, budget)
            .await?;
        self.finish_packed_upload(&dispatched, guard, budget).await
    }

    pub(crate) async fn freeze_packed_candidate(
        &self,
        expected: &PackedJournalRecord,
        target: PackedLowerBindingRecord,
        budget: &Arc<SnapshotMountBudget>,
    ) -> Result<OwnedPackedJournal<PackedJournalRecord>, WorkspaceError> {
        let _owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, OPERATION_BYTES)])
            .map_err(journal_budget_error)?;
        if expected.phase != PackedJournalPhase::Building {
            return Err(WorkspaceError::Busy);
        }
        let mapping = self
            .packed_journal_value(identity_key(
                expected.journal_id,
                &target.binding.manifest.key,
            ))
            .await?
            .ok_or_else(|| journal_error("candidate manifest is not durably pinned"))?;
        let ordinal = u64::from_le_bytes(mapping.as_slice().try_into().map_err(journal_error)?);
        let object = self.reopen_packed_object(expected, ordinal, budget).await?;
        if object.reference != target.binding.manifest
            || object.reference.kind != SnapshotObjectKind::Manifest
        {
            return Err(journal_error("candidate manifest full identity changed"));
        }
        let mut next = expected.next()?;
        next.phase = PackedJournalPhase::Uploading;
        next.commit_target = Some(target);
        self.packed_journal_write(Some(expected), &next, &[], None)
            .await
            .retain(_owner)
    }

    pub(crate) async fn record_packed_object_progress(
        &self,
        expected: &PackedJournalRecord,
        ordinal: u64,
        readback: bool,
        budget: &Arc<SnapshotMountBudget>,
    ) -> Result<OwnedPackedJournal<PackedJournalRecord>, WorkspaceError> {
        let _owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, OPERATION_BYTES)])
            .map_err(journal_budget_error)?;
        if expected.phase
            != if readback {
                PackedJournalPhase::Readback
            } else {
                PackedJournalPhase::Uploading
            }
        {
            return Err(WorkspaceError::Busy);
        }
        // The old object snapshot belongs to the expected journal version.
        let owned_object = self.reopen_packed_object(expected, ordinal, budget).await?;
        let mut object = owned_object.value.clone();
        if readback && !object.uploaded {
            return Err(journal_error("readback preceded upload"));
        }
        let old = object.encode()?;
        if readback {
            object.readback_recorded = true;
        } else {
            object.uploaded = true;
        }
        if old == object.encode()? {
            return self
                .packed_journal_write(Some(expected), expected, &[], None)
                .await
                .retain(_owner);
        }
        self.packed_journal_write(
            Some(expected),
            &expected.next()?,
            &[(
                object_key(expected.journal_id, ordinal),
                Some(old),
                Some(object.encode()?),
            )],
            None,
        )
        .await
        .retain(_owner)
    }

    pub(crate) async fn advance_packed_journal(
        &self,
        expected: &PackedJournalRecord,
        budget: &Arc<SnapshotMountBudget>,
    ) -> Result<OwnedPackedJournal<PackedJournalRecord>, WorkspaceError> {
        let _owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, OPERATION_BYTES)])
            .map_err(journal_budget_error)?;
        let next_phase = match expected.phase {
            PackedJournalPhase::Uploading => PackedJournalPhase::Readback,
            PackedJournalPhase::Readback => PackedJournalPhase::AwaitingFullProof,
            _ => return Err(WorkspaceError::Busy),
        };
        let mut digest = inventory_start();
        let mut manifest_found = false;
        for ordinal in 0..expected.object_count {
            let object = self.reopen_packed_object(expected, ordinal, budget).await?;
            if !object.uploaded
                || (expected.phase == PackedJournalPhase::Readback && !object.readback_recorded)
            {
                return Err(WorkspaceError::Busy);
            }
            digest = inventory_append(digest, ordinal, &object.reference)?;
            manifest_found |= expected
                .commit_target
                .as_ref()
                .is_some_and(|target| target.binding.manifest == object.reference);
        }
        if digest != expected.inventory_digest || !manifest_found {
            return Err(journal_error(
                "reopened inventory does not contain the exact candidate root",
            ));
        }
        let mut next = expected.next()?;
        next.phase = next_phase;
        self.packed_journal_write(Some(expected), &next, &[], None)
            .await
            .retain(_owner)
    }

    /// Actual production constructor for the independently imported graph
    /// sub-proof. It traverses/authenticates all graph bytes and semantic joins
    /// against a real durable journal version. Caller-provided provenance or
    /// object progress cannot create this proof.
    #[cfg(target_os = "linux")]
    pub(crate) async fn audit_imported_packed_graph<O: ObjectBackend + Clone>(
        &self,
        expected: &PackedJournalRecord,
        source: &SnapshotFinalSourceProof,
        client: &ObjectClient<O>,
        budget: &Arc<SnapshotMountBudget>,
        options: ImportedGraphAuditOptions<'_>,
    ) -> Result<ImportedPackedGraphSeal, WorkspaceError> {
        self.audit_imported_graph_with_native((expected, source, None), client, budget, options)
            .await
    }

    #[cfg(target_os = "linux")]
    async fn audit_imported_graph_with_native<O: ObjectBackend + Clone>(
        &self,
        context: ImportedGraphAuditContext<'_, B>,
        client: &ObjectClient<O>,
        budget: &Arc<SnapshotMountBudget>,
        options: ImportedGraphAuditOptions<'_>,
    ) -> Result<ImportedPackedGraphSeal, WorkspaceError> {
        let (expected, source, native) = context;
        let cancel = options.cancel.clone();
        let _operation_owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, OPERATION_BYTES)])
            .map_err(journal_budget_error)?;
        let proof_owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, 32 << 10)])
            .map_err(journal_budget_error)?;
        if expected.phase != PackedJournalPhase::AwaitingFullProof
            || expected.graph_receipt.is_some()
            || cancel.is_cancelled()
            || budget.state().closed
        {
            return Err(WorkspaceError::Busy);
        }
        let target = expected
            .commit_target
            .as_ref()
            .ok_or_else(|| journal_error("graph candidate target missing"))?;
        if target.binding.manifest != *source.manifest_reference()
            || source.snapshot_backed() != expected.source.snapshot_backed
        {
            return Err(WorkspaceError::Fenced);
        }
        // Prove the exact original native/catalog guard is live before I/O.
        match native {
            Some(fence) => {
                self.packed_native_journal_write(expected, expected, fence)
                    .await?
            }
            None => {
                self.packed_journal_write(Some(expected), expected, &[], None)
                    .await?
            }
        };
        let graph = self
            .audit_staged_packed_graph(expected, client, budget, options)
            .await?;
        let (physical_graph_digest, _, highest_inode) = graph.publication_facts();
        if cancel.is_cancelled() || budget.state().closed {
            return Err(WorkspaceError::Busy);
        }
        match native {
            Some(fence) => {
                self.packed_native_journal_write(expected, expected, fence)
                    .await?
            }
            None => {
                self.packed_journal_write(Some(expected), expected, &[], None)
                    .await?
            }
        };
        if cancel.is_cancelled() || budget.state().closed {
            return Err(WorkspaceError::Busy);
        }
        Ok(ImportedPackedGraphSeal {
            _permit: proof_owner,
            receipt: PackedGraphReceipt {
                audited_revision: expected.revision,
                staging_incarnation: expected.source.staging_id,
                manifest: source.manifest_reference().clone(),
                object_count: expected.object_count,
                inventory_digest: expected.inventory_digest,
                physical_graph_digest,
                final_source_digest: source.receipt_digest().map_err(journal_error)?,
                catalog_context_digest: audit_basis_digest(expected, expected.revision)?,
                snapshot_backed: source.snapshot_backed(),
                highest_inode,
            },
        })
    }

    /// Common graph core. This alone does not issue source/native/publication
    /// authority: each caller retains its operation owner and brackets this
    /// complete read with its actual, exact catalog CAS fence.
    #[cfg(target_os = "linux")]
    async fn audit_staged_packed_graph<O: ObjectBackend + Clone>(
        &self,
        expected: &PackedJournalRecord,
        client: &ObjectClient<O>,
        budget: &Arc<SnapshotMountBudget>,
        options: ImportedGraphAuditOptions<'_>,
    ) -> Result<SnapshotIndexContextAudit, WorkspaceError> {
        let ImportedGraphAuditOptions {
            scratch,
            limits,
            cancel,
        } = options;
        expected.validate()?;
        if !matches!(
            expected.phase,
            PackedJournalPhase::AwaitingFullProof | PackedJournalPhase::Verified
        ) || cancel.is_cancelled()
            || budget.state().closed
        {
            return Err(WorkspaceError::Busy);
        }
        let target = expected
            .commit_target
            .as_ref()
            .ok_or_else(|| journal_error("graph candidate target missing"))?;
        let mut inventory = inventory_start();
        for ordinal in 0..expected.object_count {
            if cancel.is_cancelled() || budget.state().closed {
                return Err(WorkspaceError::Busy);
            }
            let object = self.reopen_packed_object(expected, ordinal, budget).await?;
            if !object.uploaded || !object.readback_recorded {
                return Err(journal_error(
                    "full graph audit preceded complete durable readback",
                ));
            }
            inventory = inventory_append(inventory, ordinal, &object.reference)?;
        }
        if inventory != expected.inventory_digest {
            return Err(journal_error(
                "durable staging inventory digest differs from exact rows",
            ));
        }
        let staged = JournalGraphMembers {
            store: self,
            expected,
            budget,
        };
        let graph = audit_snapshot_staged_index_contexts(
            client,
            &target.binding.manifest,
            scratch,
            budget.clone(),
            limits,
            cancel.clone(),
            &staged,
        )
        .await
        .map_err(journal_budget_error)?;
        let (_, root_inode, highest_inode) = graph.publication_facts();
        if graph.counts().objects != expected.object_count
            || root_inode != 1
            || highest_inode >= i64::MAX as u64 - 1
            || target.highest_inode
                != (highest_inode as i64).max(expected.expected_binding.highest_inode)
        {
            return Err(journal_error(
                "full graph does not equal durable candidate/namespace boundary",
            ));
        }
        // Membership + graph's unique-key inventory + equal cardinality prove
        // no staged orphan/extras and no reachable dependency missing its pin.
        if cancel.is_cancelled() || budget.state().closed {
            return Err(WorkspaceError::Busy);
        }
        Ok(graph)
    }

    pub(crate) async fn record_imported_packed_graph(
        &self,
        expected: &PackedJournalRecord,
        seal: &ImportedPackedGraphSeal,
        budget: &Arc<SnapshotMountBudget>,
    ) -> Result<OwnedPackedJournal<PackedJournalRecord>, WorkspaceError> {
        let _owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, OPERATION_BYTES)])
            .map_err(journal_budget_error)?;
        if expected.phase != PackedJournalPhase::AwaitingFullProof
            || expected.graph_receipt.is_some()
            || seal.receipt.audited_revision != expected.revision
            || seal.receipt.catalog_context_digest
                != audit_basis_digest(expected, expected.revision)?
        {
            return Err(WorkspaceError::Fenced);
        }
        let mut next = expected.next()?;
        next.graph_receipt = Some(seal.receipt.clone());
        // Journal, active retained-root snapshot and receipt are one CAS.
        self.packed_journal_write(Some(expected), &next, &[], None)
            .await
            .retain(_owner)
    }

    fn check_complete_seal(
        record: &PackedJournalRecord,
        seal: &CompletePackedGraphSeal,
    ) -> Result<(), WorkspaceError> {
        let receipt = record
            .graph_receipt
            .as_ref()
            .ok_or_else(|| journal_error("complete seal lacks imported graph receipt"))?;
        if seal.journal_id != record.journal_id
            || seal.guard != record.guard
            || seal.source != record.source
            || seal.object_count != record.object_count
            || seal.inventory_digest != record.inventory_digest
            || seal.proof_digest == [0; 32]
            || seal.audited_revision != receipt.audited_revision
            || seal.staging_incarnation != record.source.staging_id
            || seal.graph_receipt_digest != receipt.digest()?
            || record
                .commit_target
                .as_ref()
                .is_none_or(|target| target.binding.manifest != seal.manifest)
        {
            return Err(WorkspaceError::Fenced);
        }
        Ok(())
    }
    pub(crate) async fn record_packed_full_proof(
        &self,
        expected: &PackedJournalRecord,
        seal: &CompletePackedGraphSeal,
        budget: &Arc<SnapshotMountBudget>,
    ) -> Result<OwnedPackedJournal<PackedJournalRecord>, WorkspaceError> {
        let _owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, OPERATION_BYTES)])
            .map_err(journal_budget_error)?;
        if expected.phase != PackedJournalPhase::AwaitingFullProof {
            return Err(WorkspaceError::Busy);
        }
        Self::check_complete_seal(expected, seal)?;
        let mut next = expected.next()?;
        next.phase = PackedJournalPhase::Verified;
        next.full_proof_digest = seal.proof_digest;
        self.packed_journal_write(Some(expected), &next, &[], None)
            .await
            .retain(_owner)
    }
    pub(crate) async fn commit_packed_journal(
        &self,
        expected: &PackedJournalRecord,
        request: &PublishPackedLowerBinding,
        seal: &CompletePackedGraphSeal,
        budget: &Arc<SnapshotMountBudget>,
    ) -> Result<OwnedPackedJournal<PackedJournalRecord>, WorkspaceError> {
        let _owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, OPERATION_BYTES)])
            .map_err(journal_budget_error)?;
        if expected.phase != PackedJournalPhase::Verified {
            return Err(WorkspaceError::Busy);
        }
        Self::check_complete_seal(expected, seal)?;
        if seal.proof_digest != expected.full_proof_digest {
            return Err(WorkspaceError::Fenced);
        }
        let target = request.record()?;
        if expected.commit_target.as_ref() != Some(&target) {
            return Err(WorkspaceError::Fenced);
        }
        let mut next = expected.next()?;
        next.phase = PackedJournalPhase::Committed;
        let root_change = self.registry_transition_root(expected, true).await?;
        let root_mapping = Self::registry_history_root_change(&target, &root_change)?;
        self.packed_journal_write(
            Some(expected),
            &next,
            &[
                (
                    packed_history_key(target.workspace_id, target.binding.binding_version),
                    None,
                    Some(target.encode()?),
                ),
                root_change,
                root_mapping,
            ],
            Some(request),
        )
        .await
        .retain(_owner)
    }
    pub(crate) async fn abort_packed_journal(
        &self,
        expected: &PackedJournalRecord,
        reason: String,
        budget: &Arc<SnapshotMountBudget>,
    ) -> Result<OwnedPackedJournal<PackedJournalRecord>, WorkspaceError> {
        let _owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, OPERATION_BYTES)])
            .map_err(journal_budget_error)?;
        if expected.phase.terminal() || reason.is_empty() || reason.len() > 1024 {
            return Err(WorkspaceError::Busy);
        }
        let mut next = expected.next()?;
        next.phase = PackedJournalPhase::Aborted;
        next.abort_reason = reason;
        let root_change = self.registry_transition_root(expected, false).await?;
        self.packed_journal_write(Some(expected), &next, &[root_change], None)
            .await
            .retain(_owner)
    }

    /// Recovery uses a currently live owner. Resume requires the exact old
    /// head/epoch; a new head can only abort the old candidate, never publish it.
    pub(crate) async fn recover_packed_journal(
        &self,
        expected: &PackedJournalRecord,
        guard: HeadGuard,
        abort_reason: Option<String>,
        budget: &Arc<SnapshotMountBudget>,
    ) -> Result<OwnedPackedJournal<PackedJournalRecord>, WorkspaceError> {
        let _owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, OPERATION_BYTES)])
            .map_err(journal_budget_error)?;
        if expected.phase.terminal() || guard.workspace_id != expected.guard.workspace_id {
            return Err(WorkspaceError::Fenced);
        }
        let mut next = expected.next()?;
        // A recovered lease/owner/revision cannot inherit an in-process proof
        // or its durable authority. Preserve staged typed rows and rerun audit.
        next.graph_receipt = None;
        next.full_proof_digest = [0; 32];
        if expected.phase == PackedJournalPhase::Verified {
            next.phase = PackedJournalPhase::AwaitingFullProof;
        }
        if let Some(reason) = abort_reason {
            if reason.is_empty() || reason.len() > 1024 {
                return Err(journal_error("invalid recovery abort reason"));
            }
            next.phase = PackedJournalPhase::Aborted;
            next.abort_reason = reason;
        } else {
            if guard.expected_head_layer_id != expected.guard.expected_head_layer_id
                || guard.expected_head_epoch != expected.guard.expected_head_epoch
            {
                return Err(WorkspaceError::Fenced);
            }
            next.guard = guard.clone();
        }
        next.validate()?;
        let keys = vec![
            hot_workspace_key(guard.workspace_id),
            hot_layer_key(guard.expected_head_layer_id),
            hot_lease_key(guard.workspace_id, guard.lease_id),
            journal_key(expected.journal_id),
            active_key(expected.journal_id),
            PACKED_ROOT_GENERATION_KEY.to_vec(),
            LAYER_INVENTORY_GENERATION_KEY.to_vec(),
            hot_layer_key(expected.expected_binding.base_revision.layer_id),
            packed_current_key(guard.workspace_id),
            packed_history_key(
                guard.workspace_id,
                expected.expected_binding.binding.binding_version,
            ),
            packed_claim_key(guard.workspace_id),
            ACTIVE_COUNT_KEY.to_vec(),
            registry::registry_root_key(expected.source.staging_id),
        ];
        let (values, now) = self
            .backend
            .get_many_consistent_with_time_bounded(&keys, journal_point_limits())
            .await?;
        if values.len() != keys.len() {
            return Err(journal_error("short recovery read"));
        }
        let workspace: WorkspaceRecord = decode_required(&values[0])?;
        let head: LayerRecord = decode_required(&values[1])?;
        let lease: SnapshotLease = decode_required(&values[2])?;
        checked_hot_guard(&workspace, &head, &lease, &guard, now)?;
        let actual = PackedJournalRecord::decode(
            values[3]
                .as_deref()
                .ok_or_else(|| journal_error("recovery journal missing"))?,
        )?;
        let retry = actual == next;
        if !retry && actual != *expected {
            return Err(WorkspaceError::Busy);
        }
        let active = if actual.phase.terminal() {
            None
        } else {
            Some(actual.encode()?)
        };
        if values[4] != active {
            return Err(journal_error("recovery active pin mismatch"));
        }
        if next.phase != PackedJournalPhase::Aborted
            && (values[1].as_deref() != Some(expected.expected_head.as_slice())
                || values[7].as_deref() != Some(expected.expected_base.as_slice())
                || values[8].as_deref() != Some(expected.expected_binding.encode()?.as_slice())
                || values[9] != values[8]
                || values[10].as_deref() != Some(PACKED_CLAIM))
        {
            return Err(WorkspaceError::Fenced);
        }
        next_packed_root_generation(&values[5])?;
        layer_inventory_generation(&values[6])?;
        let active_count = active_count(&values[11])?;
        let root_next = registry::recovery_root_value(expected, &next, retry, &values[12])?;
        let mut writes = Vec::new();
        if !retry {
            let next_count = if next.phase.terminal() {
                active_count
                    .checked_sub(1)
                    .ok_or_else(|| journal_error("recovery active count underflow"))?
            } else {
                active_count
            };
            writes.push(KvWrite::Put {
                key: ACTIVE_COUNT_KEY.to_vec(),
                value: next_count.to_le_bytes().to_vec(),
            });
            writes.push(KvWrite::Put {
                key: keys[3].clone(),
                value: next.encode()?,
            });
            writes.push(if next.phase.terminal() {
                KvWrite::Delete {
                    key: keys[4].clone(),
                }
            } else {
                KvWrite::Put {
                    key: keys[4].clone(),
                    value: next.encode()?,
                }
            });
            writes.push(put(
                keys[5].clone(),
                &next_packed_root_generation(&values[5])?,
            )?);
            if let Some(value) = root_next {
                writes.push(KvWrite::Put {
                    key: keys[12].clone(),
                    value,
                });
            }
        }
        let checks = keys
            .into_iter()
            .zip(values)
            .map(|(key, expected)| KvCheck { key, expected })
            .collect::<Vec<_>>();
        if !self
            .backend
            .compare_and_swap_before(&checks, &writes, lease.expires_at_ns)
            .await?
        {
            return Err(WorkspaceError::Busy);
        }
        Ok(next).retain(_owner)
    }

    /// Return only additional exact checks after a bounded seed-only census.
    /// The caller retains its owner through the actual takeover/confirmation.
    pub(crate) async fn native_seed_packed_journal_absence(
        &self,
        mut checks: Vec<KvCheck>,
        workspace_id: WorkspaceId,
        native_journal_id: JournalId,
    ) -> Result<(Vec<KvCheck>, Option<SnapshotOwnedPermit>), WorkspaceError> {
        const MAIN_PAGE_RECORDS: usize = 4;
        const MAIN_MAX_RECORDS: usize = 128;
        const MAIN_MAX_PAGES: usize = MAIN_MAX_RECORDS + 1;
        let budget =
            self.packed_reader_pin_budget
                .get()
                .ok_or(WorkspaceError::UnsupportedCapability(
                    "native seed packed journal census budget",
                ))?;
        let live = || -> Result<(), WorkspaceError> {
            if budget.state().closed {
                Err(WorkspaceError::Fenced)
            } else {
                Ok(())
            }
        };
        live()?;
        let initial_len = checks.len();
        let (_, journal_checks, owner) = self.scan_packed_journal_layer_roots().await?;
        live()?;
        let absent_feature = match journal_checks
            .iter()
            .find(|check| check.key.as_slice() == JOURNAL_FEATURE_KEY)
            .ok_or(WorkspaceError::Fenced)?
            .expected
            .as_deref()
        {
            None => true,
            Some(b"PPJ3") => false,
            _ => return Err(WorkspaceError::Fenced),
        };
        for check in &journal_checks {
            if check.key.starts_with(JOURNAL_PREFIX) {
                let record = PackedJournalRecord::decode(
                    check.expected.as_deref().ok_or(WorkspaceError::Fenced)?,
                )?;
                if record.guard.workspace_id == workspace_id
                    || record
                        .native_rebind
                        .as_ref()
                        .is_some_and(|basis| basis.native_journal_id == native_journal_id)
                {
                    // An actual packed incarnation must use its own durable
                    // recovery basis; do not replace it with a seed-only owner.
                    return Err(WorkspaceError::Busy);
                }
            }
        }
        native_publication::append_exact_checks(&mut checks, journal_checks)?;
        let proof_size = |checks: &[KvCheck]| -> Result<usize, WorkspaceError> {
            let total = checks.iter().try_fold(0usize, |total, check| {
                let bytes = check.expected.as_ref().map_or(0, Vec::len);
                if check.key.len() > 1024 || bytes > RECORD_LIMIT {
                    return Err(journal_error("native seed census point exceeds fixed tier"));
                }
                total
                    .checked_add(check.key.len())
                    .and_then(|total| total.checked_add(bytes))
                    .ok_or_else(|| journal_error("native seed census aggregate overflow"))
            })?;
            if total > RECORD_LIMIT {
                return Err(journal_error(
                    "native seed census aggregate exceeds fixed tier",
                ));
            }
            Ok(total)
        };
        let mut proof_bytes = proof_size(&checks)?;
        if !self.backend.compare_and_swap(&checks, &[]).await? {
            return Err(WorkspaceError::Busy);
        }
        live()?;
        // The validated PPJ3 scanner returns a complete 16 MiB owner. Reuse
        // it through MAIN/final CAS/unknown confirmation. A missing sentinel
        // returns only 1 MiB: admit the complete owner before dropping that
        // probe, so ownership never disappears while checks are retained.
        let scanner_owner = owner.ok_or(WorkspaceError::Fenced)?;
        let main_owner = if absent_feature {
            let admitted = budget
                .admit(&[(SnapshotBudgetPool::Metadata, 16 << 20)])
                .map_err(journal_budget_error)?;
            drop(scanner_owner);
            admitted
        } else {
            scanner_owner
        };
        if absent_feature {
            live()?;
            proof_size(&checks)?;
            if !self.backend.compare_and_swap(&checks, &[]).await? {
                return Err(WorkspaceError::Busy);
            }
            let rows = self
                .backend
                .scan_prefix_page_with_byte_limits(
                    ACTIVE_PREFIX,
                    None,
                    KvReadLimits {
                        max_records: 1,
                        max_data_requests: 1024,
                        ..journal_point_limits()
                    },
                )
                .await?;
            live()?;
            proof_size(&checks)?;
            if !self.backend.compare_and_swap(&checks, &[]).await? {
                return Err(WorkspaceError::Busy);
            }
            if !rows.is_empty() {
                return Err(WorkspaceError::Fenced);
            }
        }
        // An ACTIVE census alone cannot prove MAIN absence: an actual
        // Building MAIN may remain with PPJ3/count=0 after ACTIVE loss.
        // Enumerate every bounded MAIN page in all sentinel states. Short
        // pages continue; only an actual empty page completes this proof.
        let mut after: Option<Vec<u8>> = None;
        let mut visited = 0usize;
        let mut complete = false;
        for _ in 0..MAIN_MAX_PAGES {
            live()?;
            proof_size(&checks)?;
            if !self.backend.compare_and_swap(&checks, &[]).await? {
                return Err(WorkspaceError::Busy);
            }
            let rows = self
                .backend
                .scan_prefix_page_with_byte_limits(
                    JOURNAL_PREFIX,
                    after.as_deref(),
                    KvReadLimits {
                        max_records: MAIN_PAGE_RECORDS,
                        max_key_bytes: 256,
                        max_value_bytes: RECORD_LIMIT,
                        max_total_bytes: 256 << 10,
                        max_response_bytes: 256 << 10,
                        max_data_requests: 64,
                    },
                )
                .await?;
            live()?;
            if rows.len() > MAIN_PAGE_RECORDS {
                return Err(WorkspaceError::Fenced);
            }
            let empty = rows.is_empty();
            for entry in rows {
                live()?;
                visited += 1;
                if visited > MAIN_MAX_RECORDS {
                    return Err(WorkspaceError::Busy);
                }
                if !entry.key.starts_with(JOURNAL_PREFIX)
                    || entry.key.len() > 256
                    || entry.value.len() > RECORD_LIMIT
                    || after.as_ref().is_some_and(|cursor| entry.key <= *cursor)
                {
                    return Err(WorkspaceError::Fenced);
                }
                let old = checks.iter().find(|check| check.key == entry.key);
                if let Some(old) = old {
                    if old.expected.as_deref() != Some(entry.value.as_slice()) {
                        return Err(WorkspaceError::Busy);
                    }
                } else {
                    proof_bytes = proof_bytes
                        .checked_add(entry.key.len())
                        .and_then(|total| total.checked_add(entry.value.len()))
                        .filter(|total| *total <= RECORD_LIMIT)
                        .ok_or_else(|| {
                            journal_error("native seed census aggregate exceeds fixed tier")
                        })?;
                }
                let record = PackedJournalRecord::decode(&entry.value)?;
                if entry.key != journal_key(record.journal_id) || record.encode()? != entry.value {
                    return Err(WorkspaceError::Fenced);
                }
                if record.guard.workspace_id == workspace_id
                    || record
                        .native_rebind
                        .as_ref()
                        .is_some_and(|basis| basis.native_journal_id == native_journal_id)
                {
                    return Err(WorkspaceError::Busy);
                }
                if absent_feature
                    || (!record.phase.terminal()
                        && !checks.iter().any(|check| {
                            check.key == active_key(record.journal_id)
                                && check.expected.as_deref() == Some(entry.value.as_slice())
                        }))
                {
                    return Err(WorkspaceError::Fenced);
                }
                after = Some(entry.key.clone());
                native_publication::append_exact_checks(
                    &mut checks,
                    vec![KvCheck {
                        key: entry.key,
                        expected: Some(entry.value),
                    }],
                )?;
            }
            proof_bytes = proof_size(&checks)?;
            if !self.backend.compare_and_swap(&checks, &[]).await? {
                return Err(WorkspaceError::Busy);
            }
            live()?;
            if empty {
                complete = true;
                break;
            }
        }
        if !complete {
            return Err(WorkspaceError::Busy);
        }
        live()?;
        // append_exact_checks preserves the exact seed prefix and rejects
        // conflicting root epochs. Keep the 16/17-key routing layout intact.
        Ok((checks.split_off(initial_len), Some(main_owner)))
    }

    /// Native layer GC must include these roots/checks. Object GC additionally
    /// walks each active journal's ordinal inventory; it must not delete by
    /// origin owner or treat this root list as completed graph authority.
    pub(crate) async fn scan_packed_journal_layer_roots(
        &self,
    ) -> Result<(BTreeSet<LayerId>, Vec<KvCheck>, Option<SnapshotOwnedPermit>), WorkspaceError>
    {
        let budget =
            self.packed_reader_pin_budget
                .get()
                .ok_or(WorkspaceError::UnsupportedCapability(
                    "packed journal GC memory budget",
                ))?;
        let probe_owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, 1 << 20)])
            .map_err(journal_budget_error)?;
        let (mut probe, _) = self
            .backend
            .get_many_consistent_with_time_bounded(
                &[
                    PACKED_ROOT_GENERATION_KEY.to_vec(),
                    JOURNAL_FEATURE_KEY.to_vec(),
                    ACTIVE_COUNT_KEY.to_vec(),
                ],
                journal_probe_limits(),
            )
            .await?;
        if probe.len() != 3 {
            return Err(journal_error("short bounded journal GC probe"));
        }
        let count = probe.pop().unwrap();
        let feature = probe.pop().unwrap();
        let generation = probe.pop().unwrap();
        next_packed_root_generation(&generation)?;
        let mut checks = vec![
            KvCheck {
                key: PACKED_ROOT_GENERATION_KEY.to_vec(),
                expected: generation,
            },
            KvCheck {
                key: JOURNAL_FEATURE_KEY.to_vec(),
                expected: feature.clone(),
            },
            KvCheck {
                key: ACTIVE_COUNT_KEY.to_vec(),
                expected: count.clone(),
            },
        ];
        if feature.is_none() && count.is_some() {
            return Err(journal_error(
                "journal feature missing with persisted count",
            ));
        }
        if feature.is_none() {
            return Ok((BTreeSet::new(), checks, Some(probe_owner)));
        }
        if feature.as_deref() != Some(b"PPJ3") {
            return Err(journal_error("journal feature sentinel is corrupt"));
        }
        // Bound/admit the scan's row bytes and both retained check copies
        // before the backend materializes them. Reopened GC must configure the
        // same budget used by reader pins; no hidden per-operation budget.
        let permit = budget
            .admit(&[(SnapshotBudgetPool::Metadata, 16 << 20)])
            .map_err(journal_budget_error)?;
        let entries = self
            .backend
            .scan_prefix_with_byte_limits(ACTIVE_PREFIX, journal_scan_limits())
            .await?;
        if entries.len() > MAX_ACTIVE_JOURNALS {
            return Err(WorkspaceError::InvalidReadPlan(
                "packed journal GC root limit exceeded".into(),
            ));
        }
        if entries.len() as u64 != active_count(&count)? {
            return Err(WorkspaceError::Busy);
        }
        let mut roots = BTreeSet::new();
        for entry in entries {
            let record = PackedJournalRecord::decode(&entry.value)?;
            if entry.key != active_key(record.journal_id) || record.phase.terminal() {
                return Err(journal_error("active journal key/state mismatch"));
            }
            roots.insert(record.guard.expected_head_layer_id);
            roots.insert(record.expected_binding.base_revision.layer_id);
            checks.push(KvCheck {
                key: journal_key(record.journal_id),
                expected: Some(entry.value.clone()),
            });
            checks.push(KvCheck {
                key: entry.key,
                expected: Some(entry.value),
            });
        }
        Ok((roots, checks, Some(permit)))
    }
}

#[cfg(test)]
#[path = "packed_journal_tests.rs"]
mod tests;
