//! BNCT: the native control-record envelope and all record kinds
//! (spec 20 §3, §11, §12).
//!
//! Envelope: `"BNCT"[4] | control_version:u16=2 | kind:u16 | payload_len:u32
//! | payload | CRC32C:u32` where CRC covers every preceding byte. Field
//! encodings follow spec 02 scalars/Option/vec/RootRef; unknown enum values
//! and unknown kinds are rejected (fail closed), never guessed.

use super::container::crc32c;
use super::error::{WireError, WireResult};
use super::refs::{Hash32, ObjectRef, RootRef};
use super::uvarint::{Reader, Writer};

pub const CONTROL_VERSION: u16 = 2;
pub const ENVELOPE_MAGIC: &[u8; 4] = b"BNCT";
/// Single-record payload limit (spec 20 §3).
pub const MAX_PAYLOAD: usize = 1024 * 1024;

pub type Id16 = [u8; 16];

// ---------------------------------------------------------------------------
// Envelope
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Envelope {
    pub kind: ControlKind,
    pub payload: Vec<u8>,
}

/// Encode a record payload under its kind.
pub fn encode_envelope(kind: ControlKind, payload: &[u8]) -> Vec<u8> {
    let mut w = Writer::new();
    w.put(ENVELOPE_MAGIC);
    w.u16(CONTROL_VERSION);
    w.u16(kind.as_u16());
    w.u32(payload.len() as u32);
    w.put(payload);
    let crc = crc32c(w.as_slice());
    w.u32(crc);
    w.into_bytes()
}

/// Parse and validate the envelope. Fails closed on wrong magic, wrong
/// control version (old draft control implementations must reject v2
/// records, and this reader rejects anything but 2), unknown kind, payload
/// length disagreement, over-limit payloads, and CRC mismatch.
pub fn decode_envelope(bytes: &[u8]) -> WireResult<Envelope> {
    let what = "BNCT envelope";
    let mut r = Reader::new(bytes);
    let magic = r.take(4, what)?;
    if magic != ENVELOPE_MAGIC {
        return Err(WireError::invalid(
            what,
            format!("magic {magic:02x?} is not {ENVELOPE_MAGIC:02x?}"),
        ));
    }
    let version = r.u16(what)?;
    if version != CONTROL_VERSION {
        return Err(WireError::UnsupportedFormat(format!(
            "native control version {version} (this build reads {CONTROL_VERSION} only)"
        )));
    }
    let kind = ControlKind::from_u16(r.u16(what)?)?;
    let payload_len = r.u32(what)? as usize;
    if payload_len > MAX_PAYLOAD {
        return Err(WireError::LimitExceeded(format!(
            "control payload {payload_len} exceeds {MAX_PAYLOAD}"
        )));
    }
    let payload = r.take(payload_len, what)?;
    let crc_stored = r.u32(what)?;
    let crc_computed = crc32c(&bytes[..bytes.len() - 4]);
    if crc_stored != crc_computed {
        return Err(WireError::CrcMismatch {
            what,
            stored: crc_stored,
            computed: crc_computed,
        });
    }
    if !r.is_empty() {
        return Err(WireError::invalid(what, "trailing bytes after CRC"));
    }
    Ok(Envelope {
        kind,
        payload: payload.to_vec(),
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlKind {
    OwnershipDomain,
    ObjectRegistration,
    RetentionReceipt,
    PublishedRevision,
    DomainCloseCertificate,
    CleanupBatch,
    KvBaseRetention,
    NativeWorkspaceHead,
    NativePublicationJournal,
    NativeDrainBatch,
    NativeMutationResult,
}

impl ControlKind {
    pub fn as_u16(self) -> u16 {
        match self {
            ControlKind::OwnershipDomain => 1,
            ControlKind::ObjectRegistration => 2,
            ControlKind::RetentionReceipt => 3,
            ControlKind::PublishedRevision => 4,
            ControlKind::DomainCloseCertificate => 5,
            ControlKind::CleanupBatch => 6,
            ControlKind::KvBaseRetention => 7,
            ControlKind::NativeWorkspaceHead => 16,
            ControlKind::NativePublicationJournal => 17,
            ControlKind::NativeDrainBatch => 18,
            ControlKind::NativeMutationResult => 19,
        }
    }

    pub fn from_u16(v: u16) -> WireResult<ControlKind> {
        match v {
            1 => Ok(ControlKind::OwnershipDomain),
            2 => Ok(ControlKind::ObjectRegistration),
            3 => Ok(ControlKind::RetentionReceipt),
            4 => Ok(ControlKind::PublishedRevision),
            5 => Ok(ControlKind::DomainCloseCertificate),
            6 => Ok(ControlKind::CleanupBatch),
            7 => Ok(ControlKind::KvBaseRetention),
            16 => Ok(ControlKind::NativeWorkspaceHead),
            17 => Ok(ControlKind::NativePublicationJournal),
            18 => Ok(ControlKind::NativeDrainBatch),
            19 => Ok(ControlKind::NativeMutationResult),
            other => Err(WireError::UnsupportedFormat(format!(
                "BNCT kind {other} (expected one of 1..=7, 16..=19)"
            ))),
        }
    }
}

// ---------------------------------------------------------------------------
// Shared value types (spec 20 §11)
// ---------------------------------------------------------------------------

/// `HeadRef = head_id[16] + epoch:u64 + commit_seq:u64`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeadRef {
    pub head_id: Id16,
    pub epoch: u64,
    pub commit_seq: u64,
}

impl HeadRef {
    pub fn encode_into(&self, w: &mut Writer) {
        w.put(&self.head_id);
        w.u64(self.epoch);
        w.u64(self.commit_seq);
    }

    pub fn decode(r: &mut Reader<'_>) -> WireResult<HeadRef> {
        Ok(HeadRef {
            head_id: r.take(16, "head ref")?.try_into().unwrap(),
            epoch: r.u64("head ref")?,
            commit_seq: r.u64("head ref")?,
        })
    }
}

/// `SnapshotRef = volume_id[16] + logical_revision[32] + ObjectRef(manifest)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotRef {
    pub volume_id: Id16,
    pub logical_revision: Hash32,
    pub manifest: ObjectRef,
}

impl SnapshotRef {
    pub fn encode_into(&self, w: &mut Writer) {
        w.put(&self.volume_id);
        w.put(&self.logical_revision);
        self.manifest.encode_into(w);
    }

    pub fn decode(r: &mut Reader<'_>) -> WireResult<SnapshotRef> {
        Ok(SnapshotRef {
            volume_id: r.take(16, "snapshot ref")?.try_into().unwrap(),
            logical_revision: r.take(32, "snapshot ref")?.try_into().unwrap(),
            manifest: ObjectRef::decode(r)?,
        })
    }
}

/// `PublicationResult = snapshot:SnapshotRef, new_head:Option<HeadRef>,
/// publication_id[16]` (spec 20 §11).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicationResult {
    pub snapshot: SnapshotRef,
    pub new_head: Option<HeadRef>,
    pub publication_id: Id16,
}

impl PublicationResult {
    pub fn encode_into(&self, w: &mut Writer) {
        self.snapshot.encode_into(w);
        w.option_tag(self.new_head.is_some());
        if let Some(h) = &self.new_head {
            h.encode_into(w);
        }
        w.put(&self.publication_id);
    }

    pub fn decode(r: &mut Reader<'_>) -> WireResult<PublicationResult> {
        let snapshot = SnapshotRef::decode(r)?;
        let new_head = if r.option_tag("publication result")? {
            Some(HeadRef::decode(r)?)
        } else {
            None
        };
        let publication_id: Id16 = r.take(16, "publication result")?.try_into().unwrap();
        Ok(PublicationResult {
            snapshot,
            new_head,
            publication_id,
        })
    }
}

/// `PlanEntry` for drain plans (spec 20 §11). `source_token` is a recovery
/// locator only, never an authorization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanEntry {
    pub admission_ticket: u64,
    pub inode: u64,
    pub mutation_order: u64,
    pub logical_offset: u64,
    pub logical_len: u64,
    pub operation_id: Id16,
    pub payload_digest: Hash32,
    pub source_token: Vec<u8>,
}

impl PlanEntry {
    pub fn encode_into(&self, w: &mut Writer) {
        w.u64(self.admission_ticket);
        w.u64(self.inode);
        w.u64(self.mutation_order);
        w.u64(self.logical_offset);
        w.u64(self.logical_len);
        w.put(&self.operation_id);
        w.put(&self.payload_digest);
        w.bytes(&self.source_token);
    }

    pub fn decode(r: &mut Reader<'_>) -> WireResult<PlanEntry> {
        Ok(PlanEntry {
            admission_ticket: r.u64("plan entry")?,
            inode: r.u64("plan entry")?,
            mutation_order: r.u64("plan entry")?,
            logical_offset: r.u64("plan entry")?,
            logical_len: r.u64("plan entry")?,
            operation_id: r.take(16, "plan entry")?.try_into().unwrap(),
            payload_digest: r.take(32, "plan entry")?.try_into().unwrap(),
            source_token: r.bytes("plan entry")?.to_vec(),
        })
    }
}

/// `Option<T>` encode/decode helper used by record payloads.
fn option_encode<T>(w: &mut Writer, v: &Option<T>, f: impl FnOnce(&mut Writer, &T)) {
    w.option_tag(v.is_some());
    if let Some(v) = v {
        f(w, v);
    }
}

fn option_decode<T>(
    r: &mut Reader<'_>,
    f: impl FnOnce(&mut Reader<'_>) -> WireResult<T>,
) -> WireResult<Option<T>> {
    if r.option_tag("option field")? {
        Ok(Some(f(r)?))
    } else {
        Ok(None)
    }
}

fn id16(r: &mut Reader<'_>, what: &'static str) -> WireResult<Id16> {
    r.take(16, what)?.try_into().map_err(|_| unreachable!())
}

fn hash32(r: &mut Reader<'_>, what: &'static str) -> WireResult<Hash32> {
    r.take(32, what)?.try_into().map_err(|_| unreachable!())
}

// ---------------------------------------------------------------------------
// Record kinds
// ---------------------------------------------------------------------------

/// Domain states (spec 20 §2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DomainState {
    Active,
    Draining,
    Quarantined,
    Closed,
    Cleaning,
    Cleaned,
}

impl DomainState {
    pub fn as_u8(self) -> u8 {
        match self {
            DomainState::Active => 0,
            DomainState::Draining => 1,
            DomainState::Quarantined => 2,
            DomainState::Closed => 3,
            DomainState::Cleaning => 4,
            DomainState::Cleaned => 5,
        }
    }

    pub fn from_u8(v: u8) -> WireResult<DomainState> {
        match v {
            0 => Ok(DomainState::Active),
            1 => Ok(DomainState::Draining),
            2 => Ok(DomainState::Quarantined),
            3 => Ok(DomainState::Closed),
            4 => Ok(DomainState::Cleaning),
            5 => Ok(DomainState::Cleaned),
            other => Err(WireError::UnsupportedFormat(format!(
                "domain state {other} (expected 0..=5)"
            ))),
        }
    }
}

/// kind 1: OwnershipDomain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnershipDomain {
    pub domain_id: Id16,
    pub volume_id: Id16,
    pub namespace_id: Id16,
    /// 0 = workspace, 1 = build.
    pub domain_kind: DomainKind,
    pub owner_id: Id16,
    pub owner_generation: u64,
    pub state: DomainState,
    pub entity_version: u64,
    pub inventory_seq: u64,
    pub retention_seq: u64,
    pub outstanding_attempts: u64,
    pub open_operations: u64,
    pub accepted_ticket_end: Option<u64>,
    pub close_ref: Option<RootRef>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DomainKind {
    Workspace,
    Build,
}

impl DomainKind {
    pub fn as_u8(self) -> u8 {
        match self {
            DomainKind::Workspace => 0,
            DomainKind::Build => 1,
        }
    }

    pub fn from_u8(v: u8) -> WireResult<DomainKind> {
        match v {
            0 => Ok(DomainKind::Workspace),
            1 => Ok(DomainKind::Build),
            other => Err(WireError::UnsupportedFormat(format!(
                "domain kind {other} (expected 0 or 1)"
            ))),
        }
    }
}

/// ObjectRegistration states (spec 20 §3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegistrationState {
    Registered,
    Dispatched,
    Unknown,
    Verified,
    Abandoned,
    DeletePending,
    Deleted,
}

impl RegistrationState {
    pub fn as_u8(self) -> u8 {
        match self {
            RegistrationState::Registered => 0,
            RegistrationState::Dispatched => 1,
            RegistrationState::Unknown => 2,
            RegistrationState::Verified => 3,
            RegistrationState::Abandoned => 4,
            RegistrationState::DeletePending => 5,
            RegistrationState::Deleted => 6,
        }
    }

    pub fn from_u8(v: u8) -> WireResult<RegistrationState> {
        match v {
            0 => Ok(RegistrationState::Registered),
            1 => Ok(RegistrationState::Dispatched),
            2 => Ok(RegistrationState::Unknown),
            3 => Ok(RegistrationState::Verified),
            4 => Ok(RegistrationState::Abandoned),
            5 => Ok(RegistrationState::DeletePending),
            6 => Ok(RegistrationState::Deleted),
            other => Err(WireError::UnsupportedFormat(format!(
                "registration state {other} (expected 0..=6)"
            ))),
        }
    }
}

/// kind 2: ObjectRegistration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectRegistration {
    pub object_ref: ObjectRef,
    pub domain_id: Id16,
    pub upload_plan_hash: Hash32,
    pub registration_seq: u64,
    pub attempt_generation: u64,
    pub state: RegistrationState,
}

/// kind 3: RetentionReceipt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionReceipt {
    pub domain_id: Id16,
    pub retention_seq: u64,
    pub operation_id: Id16,
    pub candidate_view: Hash32,
    pub retained_objects: RootRef,
    pub evidence_root: RootRef,
    pub verified_subset_digest: Hash32,
}

/// kind 4: PublishedRevision. `retention_policy` is fixed to 1 = forever in
/// this version; any other value is rejected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedRevision {
    pub volume_id: Id16,
    pub storage_view_id: Hash32,
    pub logical_revision: Hash32,
    pub manifest: ObjectRef,
    pub publication_id: Id16,
    pub retained_at_ns: i64,
    pub retention_policy: RetentionPolicy,
    pub evidence_root: RootRef,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetentionPolicy {
    Forever,
}

impl RetentionPolicy {
    pub fn as_u8(self) -> u8 {
        match self {
            RetentionPolicy::Forever => 1,
        }
    }

    pub fn from_u8(v: u8) -> WireResult<RetentionPolicy> {
        match v {
            1 => Ok(RetentionPolicy::Forever),
            other => Err(WireError::UnsupportedFormat(format!(
                "retention policy {other} (expected 1 = forever)"
            ))),
        }
    }
}

/// kind 5: DomainCloseCertificate. `attempts_resolved`/`operations_drained`
/// are fixed to 1 (true) in this version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainCloseCertificate {
    pub domain_id: Id16,
    pub close_generation: u64,
    pub final_inventory_seq: u64,
    pub final_retention_seq: u64,
    pub inventory: Option<RootRef>,
    pub retained_union: Option<RootRef>,
    pub control_evidence: Option<RootRef>,
    pub owner_terminal_proof_hash: Hash32,
    pub attempts_resolved: bool,
    pub operations_drained: bool,
    pub closed_at_ns: i64,
}

/// CleanupBatch states (spec 20 §3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CleanupState {
    Planned,
    Deleting,
    Complete,
    Blocked,
}

impl CleanupState {
    pub fn as_u8(self) -> u8 {
        match self {
            CleanupState::Planned => 0,
            CleanupState::Deleting => 1,
            CleanupState::Complete => 2,
            CleanupState::Blocked => 3,
        }
    }

    pub fn from_u8(v: u8) -> WireResult<CleanupState> {
        match v {
            0 => Ok(CleanupState::Planned),
            1 => Ok(CleanupState::Deleting),
            2 => Ok(CleanupState::Complete),
            3 => Ok(CleanupState::Blocked),
            other => Err(WireError::UnsupportedFormat(format!(
                "cleanup state {other} (expected 0..=3)"
            ))),
        }
    }
}

/// kind 6: CleanupBatch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CleanupBatch {
    pub domain_id: Id16,
    pub close_generation: u64,
    pub cleanup_id: Id16,
    pub batch_number: u64,
    pub candidate_objects: RootRef,
    pub completed_results: Option<RootRef>,
    pub state: CleanupState,
}

/// kind 7: KvBaseRetention (spec 20 §12). P1 published KV baselines are
/// retained forever; the record has no expiry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvBaseRetention {
    pub volume_id: Id16,
    pub layer_id: Id16,
    pub sealed_version: u64,
    pub logical_revision: Hash32,
    pub first_publication_id: Id16,
}

/// Workspace head states (spec 20 §11 kind 16).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkspaceHeadState {
    Running,
    Freezing,
    Sealing,
    Discarding,
    Closed,
}

impl WorkspaceHeadState {
    pub fn as_u8(self) -> u8 {
        match self {
            WorkspaceHeadState::Running => 0,
            WorkspaceHeadState::Freezing => 1,
            WorkspaceHeadState::Sealing => 2,
            WorkspaceHeadState::Discarding => 3,
            WorkspaceHeadState::Closed => 4,
        }
    }

    pub fn from_u8(v: u8) -> WireResult<WorkspaceHeadState> {
        match v {
            0 => Ok(WorkspaceHeadState::Running),
            1 => Ok(WorkspaceHeadState::Freezing),
            2 => Ok(WorkspaceHeadState::Sealing),
            3 => Ok(WorkspaceHeadState::Discarding),
            4 => Ok(WorkspaceHeadState::Closed),
            other => Err(WireError::UnsupportedFormat(format!(
                "workspace head state {other} (expected 0..=4)"
            ))),
        }
    }
}

/// kind 16: NativeWorkspaceHead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeWorkspaceHead {
    pub workspace_id: Id16,
    pub head: HeadRef,
    pub base: SnapshotRef,
    pub writer_generation: u64,
    pub write_domain_id: Id16,
    pub visible_delta_count: u64,
    pub open_orphan_count: u64,
    pub orphan_carry_digest: Option<Hash32>,
    pub state: WorkspaceHeadState,
    pub entity_version: u64,
}

/// Publication phases (spec 20 §11).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublicationPhase {
    Prepare,
    Quiesced,
    DataDrained,
    CandidateVerified,
    RetentionPrepared,
    PublishedRetained,
    Completed,
    Aborted,
}

impl PublicationPhase {
    pub fn as_u8(self) -> u8 {
        match self {
            PublicationPhase::Prepare => 0,
            PublicationPhase::Quiesced => 1,
            PublicationPhase::DataDrained => 2,
            PublicationPhase::CandidateVerified => 3,
            PublicationPhase::RetentionPrepared => 4,
            PublicationPhase::PublishedRetained => 5,
            PublicationPhase::Completed => 6,
            PublicationPhase::Aborted => 7,
        }
    }

    pub fn from_u8(v: u8) -> WireResult<PublicationPhase> {
        match v {
            0 => Ok(PublicationPhase::Prepare),
            1 => Ok(PublicationPhase::Quiesced),
            2 => Ok(PublicationPhase::DataDrained),
            3 => Ok(PublicationPhase::CandidateVerified),
            4 => Ok(PublicationPhase::RetentionPrepared),
            5 => Ok(PublicationPhase::PublishedRetained),
            6 => Ok(PublicationPhase::Completed),
            7 => Ok(PublicationPhase::Aborted),
            other => Err(WireError::UnsupportedFormat(format!(
                "publication phase {other} (expected 0..=7)"
            ))),
        }
    }
}

/// kind 17: NativePublicationJournal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativePublicationJournal {
    pub operation_id: Id16,
    pub workspace_id: Option<Id16>,
    pub expected_head: Option<HeadRef>,
    pub expected_base: Option<SnapshotRef>,
    pub owner_generation: u64,
    pub phase: PublicationPhase,
    pub accepted_ticket_end: u64,
    pub drain_plan: Option<RootRef>,
    pub batch_count: u64,
    pub completed_count: u64,
    pub fixed_commit_seq: Option<u64>,
    pub candidate: Option<SnapshotRef>,
    pub verification_evidence: Option<RootRef>,
    pub retention_evidence: Option<RootRef>,
    pub committed_result: Option<PublicationResult>,
}

/// Drain batch states (spec 20 §11 kind 18).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainBatchState {
    Registered,
    Uploading,
    Committed,
    Failed,
}

impl DrainBatchState {
    pub fn as_u8(self) -> u8 {
        match self {
            DrainBatchState::Registered => 0,
            DrainBatchState::Uploading => 1,
            DrainBatchState::Committed => 2,
            DrainBatchState::Failed => 3,
        }
    }

    pub fn from_u8(v: u8) -> WireResult<DrainBatchState> {
        match v {
            0 => Ok(DrainBatchState::Registered),
            1 => Ok(DrainBatchState::Uploading),
            2 => Ok(DrainBatchState::Committed),
            3 => Ok(DrainBatchState::Failed),
            other => Err(WireError::UnsupportedFormat(format!(
                "drain batch state {other} (expected 0..=3)"
            ))),
        }
    }
}

/// kind 18: NativeDrainBatch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeDrainBatch {
    pub operation_id: Id16,
    pub batch_id: u64,
    pub owner_generation: u64,
    pub plan_digest: Hash32,
    pub plan: RootRef,
    pub state: DrainBatchState,
    pub result_digest: Option<Hash32>,
}

/// Mutation result status (spec 20 §11 kind 19).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutationStatus {
    Committed,
    Failed,
}

impl MutationStatus {
    pub fn as_u8(self) -> u8 {
        match self {
            MutationStatus::Committed => 0,
            MutationStatus::Failed => 1,
        }
    }

    pub fn from_u8(v: u8) -> WireResult<MutationStatus> {
        match v {
            0 => Ok(MutationStatus::Committed),
            1 => Ok(MutationStatus::Failed),
            other => Err(WireError::UnsupportedFormat(format!(
                "mutation status {other} (expected 0 or 1)"
            ))),
        }
    }
}

/// kind 19: NativeMutationResult.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeMutationResult {
    pub operation_id: Id16,
    pub payload_digest: Hash32,
    pub head: HeadRef,
    pub inode: u64,
    pub inode_data_version: u64,
    pub durable_receipts: RootRef,
    pub status: MutationStatus,
    pub stable_error: Option<Vec<u8>>,
}

// ---------------------------------------------------------------------------
// Record encode/decode dispatch
// ---------------------------------------------------------------------------

/// Every control record, tagged by kind.
// The variants are plain decoded records, handled one at a time; boxing them
// would complicate every construction site for no hot-path benefit.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlRecord {
    OwnershipDomain(OwnershipDomain),
    ObjectRegistration(ObjectRegistration),
    RetentionReceipt(RetentionReceipt),
    PublishedRevision(PublishedRevision),
    DomainCloseCertificate(DomainCloseCertificate),
    CleanupBatch(CleanupBatch),
    KvBaseRetention(KvBaseRetention),
    NativeWorkspaceHead(NativeWorkspaceHead),
    NativePublicationJournal(NativePublicationJournal),
    NativeDrainBatch(NativeDrainBatch),
    NativeMutationResult(NativeMutationResult),
}

impl ControlRecord {
    pub fn kind(&self) -> ControlKind {
        match self {
            ControlRecord::OwnershipDomain(_) => ControlKind::OwnershipDomain,
            ControlRecord::ObjectRegistration(_) => ControlKind::ObjectRegistration,
            ControlRecord::RetentionReceipt(_) => ControlKind::RetentionReceipt,
            ControlRecord::PublishedRevision(_) => ControlKind::PublishedRevision,
            ControlRecord::DomainCloseCertificate(_) => ControlKind::DomainCloseCertificate,
            ControlRecord::CleanupBatch(_) => ControlKind::CleanupBatch,
            ControlRecord::KvBaseRetention(_) => ControlKind::KvBaseRetention,
            ControlRecord::NativeWorkspaceHead(_) => ControlKind::NativeWorkspaceHead,
            ControlRecord::NativePublicationJournal(_) => ControlKind::NativePublicationJournal,
            ControlRecord::NativeDrainBatch(_) => ControlKind::NativeDrainBatch,
            ControlRecord::NativeMutationResult(_) => ControlKind::NativeMutationResult,
        }
    }

    /// Encode payload only.
    pub fn encode_payload(&self) -> Vec<u8> {
        let mut w = Writer::new();
        match self {
            ControlRecord::OwnershipDomain(r) => {
                w.put(&r.domain_id);
                w.put(&r.volume_id);
                w.put(&r.namespace_id);
                w.u8(r.domain_kind.as_u8());
                w.put(&r.owner_id);
                w.u64(r.owner_generation);
                w.u8(r.state.as_u8());
                w.u64(r.entity_version);
                w.u64(r.inventory_seq);
                w.u64(r.retention_seq);
                w.u64(r.outstanding_attempts);
                w.u64(r.open_operations);
                option_encode(&mut w, &r.accepted_ticket_end, |w, v| w.u64(*v));
                option_encode(&mut w, &r.close_ref, |w, v| v.encode_into(w));
            }
            ControlRecord::ObjectRegistration(r) => {
                r.object_ref.encode_into(&mut w);
                w.put(&r.domain_id);
                w.put(&r.upload_plan_hash);
                w.u64(r.registration_seq);
                w.u64(r.attempt_generation);
                w.u8(r.state.as_u8());
            }
            ControlRecord::RetentionReceipt(r) => {
                w.put(&r.domain_id);
                w.u64(r.retention_seq);
                w.put(&r.operation_id);
                w.put(&r.candidate_view);
                r.retained_objects.encode_into(&mut w);
                r.evidence_root.encode_into(&mut w);
                w.put(&r.verified_subset_digest);
            }
            ControlRecord::PublishedRevision(r) => {
                w.put(&r.volume_id);
                w.put(&r.storage_view_id);
                w.put(&r.logical_revision);
                r.manifest.encode_into(&mut w);
                w.put(&r.publication_id);
                w.i64(r.retained_at_ns);
                w.u8(r.retention_policy.as_u8());
                r.evidence_root.encode_into(&mut w);
            }
            ControlRecord::DomainCloseCertificate(r) => {
                w.put(&r.domain_id);
                w.u64(r.close_generation);
                w.u64(r.final_inventory_seq);
                w.u64(r.final_retention_seq);
                option_encode(&mut w, &r.inventory, |w, v| v.encode_into(w));
                option_encode(&mut w, &r.retained_union, |w, v| v.encode_into(w));
                option_encode(&mut w, &r.control_evidence, |w, v| v.encode_into(w));
                w.put(&r.owner_terminal_proof_hash);
                w.bool(r.attempts_resolved);
                w.bool(r.operations_drained);
                w.i64(r.closed_at_ns);
            }
            ControlRecord::CleanupBatch(r) => {
                w.put(&r.domain_id);
                w.u64(r.close_generation);
                w.put(&r.cleanup_id);
                w.u64(r.batch_number);
                r.candidate_objects.encode_into(&mut w);
                option_encode(&mut w, &r.completed_results, |w, v| v.encode_into(w));
                w.u8(r.state.as_u8());
            }
            ControlRecord::KvBaseRetention(r) => {
                w.put(&r.volume_id);
                w.put(&r.layer_id);
                w.u64(r.sealed_version);
                w.put(&r.logical_revision);
                w.put(&r.first_publication_id);
            }
            ControlRecord::NativeWorkspaceHead(r) => {
                w.put(&r.workspace_id);
                r.head.encode_into(&mut w);
                r.base.encode_into(&mut w);
                w.u64(r.writer_generation);
                w.put(&r.write_domain_id);
                w.u64(r.visible_delta_count);
                w.u64(r.open_orphan_count);
                option_encode(&mut w, &r.orphan_carry_digest, |w, v| w.put(v));
                w.u8(r.state.as_u8());
                w.u64(r.entity_version);
            }
            ControlRecord::NativePublicationJournal(r) => {
                w.put(&r.operation_id);
                option_encode(&mut w, &r.workspace_id, |w, v| w.put(v));
                option_encode(&mut w, &r.expected_head, |w, v| v.encode_into(w));
                option_encode(&mut w, &r.expected_base, |w, v| v.encode_into(w));
                w.u64(r.owner_generation);
                w.u8(r.phase.as_u8());
                w.u64(r.accepted_ticket_end);
                option_encode(&mut w, &r.drain_plan, |w, v| v.encode_into(w));
                w.u64(r.batch_count);
                w.u64(r.completed_count);
                option_encode(&mut w, &r.fixed_commit_seq, |w, v| w.u64(*v));
                option_encode(&mut w, &r.candidate, |w, v| v.encode_into(w));
                option_encode(&mut w, &r.verification_evidence, |w, v| v.encode_into(w));
                option_encode(&mut w, &r.retention_evidence, |w, v| v.encode_into(w));
                option_encode(&mut w, &r.committed_result, |w, v| v.encode_into(w));
            }
            ControlRecord::NativeDrainBatch(r) => {
                w.put(&r.operation_id);
                w.u64(r.batch_id);
                w.u64(r.owner_generation);
                w.put(&r.plan_digest);
                r.plan.encode_into(&mut w);
                w.u8(r.state.as_u8());
                option_encode(&mut w, &r.result_digest, |w, v| w.put(v));
            }
            ControlRecord::NativeMutationResult(r) => {
                w.put(&r.operation_id);
                w.put(&r.payload_digest);
                r.head.encode_into(&mut w);
                w.u64(r.inode);
                w.u64(r.inode_data_version);
                r.durable_receipts.encode_into(&mut w);
                w.u8(r.status.as_u8());
                option_encode(&mut w, &r.stable_error, |w, v| w.bytes(v));
            }
        }
        w.into_bytes()
    }

    /// Full envelope bytes for KV storage.
    pub fn encode(&self) -> Vec<u8> {
        encode_envelope(self.kind(), &self.encode_payload())
    }

    /// Decode a record payload under `kind`, requiring full consumption.
    pub fn decode_payload(kind: ControlKind, payload: &[u8]) -> WireResult<ControlRecord> {
        let mut r = Reader::new(payload);
        let record = Self::decode_fields(kind, &mut r)?;
        if !r.is_empty() {
            return Err(WireError::invalid(
                "control payload",
                format!(
                    "{} trailing bytes for kind {}",
                    r.remaining(),
                    kind.as_u16()
                ),
            ));
        }
        Ok(record)
    }

    fn decode_fields(kind: ControlKind, r: &mut Reader<'_>) -> WireResult<ControlRecord> {
        let what = "control record";
        Ok(match kind {
            ControlKind::OwnershipDomain => ControlRecord::OwnershipDomain(OwnershipDomain {
                domain_id: id16(r, what)?,
                volume_id: id16(r, what)?,
                namespace_id: id16(r, what)?,
                domain_kind: DomainKind::from_u8(r.u8(what)?)?,
                owner_id: id16(r, what)?,
                owner_generation: r.u64(what)?,
                state: DomainState::from_u8(r.u8(what)?)?,
                entity_version: r.u64(what)?,
                inventory_seq: r.u64(what)?,
                retention_seq: r.u64(what)?,
                outstanding_attempts: r.u64(what)?,
                open_operations: r.u64(what)?,
                accepted_ticket_end: option_decode(r, |r| r.u64(what))?,
                close_ref: option_decode(r, RootRef::decode)?,
            }),
            ControlKind::ObjectRegistration => {
                ControlRecord::ObjectRegistration(ObjectRegistration {
                    object_ref: ObjectRef::decode(r)?,
                    domain_id: id16(r, what)?,
                    upload_plan_hash: hash32(r, what)?,
                    registration_seq: r.u64(what)?,
                    attempt_generation: r.u64(what)?,
                    state: RegistrationState::from_u8(r.u8(what)?)?,
                })
            }
            ControlKind::RetentionReceipt => ControlRecord::RetentionReceipt(RetentionReceipt {
                domain_id: id16(r, what)?,
                retention_seq: r.u64(what)?,
                operation_id: id16(r, what)?,
                candidate_view: hash32(r, what)?,
                retained_objects: RootRef::decode(r)?,
                evidence_root: RootRef::decode(r)?,
                verified_subset_digest: hash32(r, what)?,
            }),
            ControlKind::PublishedRevision => ControlRecord::PublishedRevision(PublishedRevision {
                volume_id: id16(r, what)?,
                storage_view_id: hash32(r, what)?,
                logical_revision: hash32(r, what)?,
                manifest: ObjectRef::decode(r)?,
                publication_id: id16(r, what)?,
                retained_at_ns: r.i64(what)?,
                retention_policy: RetentionPolicy::from_u8(r.u8(what)?)?,
                evidence_root: RootRef::decode(r)?,
            }),
            ControlKind::DomainCloseCertificate => {
                ControlRecord::DomainCloseCertificate(DomainCloseCertificate {
                    domain_id: id16(r, what)?,
                    close_generation: r.u64(what)?,
                    final_inventory_seq: r.u64(what)?,
                    final_retention_seq: r.u64(what)?,
                    inventory: option_decode(r, RootRef::decode)?,
                    retained_union: option_decode(r, RootRef::decode)?,
                    control_evidence: option_decode(r, RootRef::decode)?,
                    owner_terminal_proof_hash: hash32(r, what)?,
                    attempts_resolved: r.bool(what)?,
                    operations_drained: r.bool(what)?,
                    closed_at_ns: r.i64(what)?,
                })
            }
            ControlKind::CleanupBatch => ControlRecord::CleanupBatch(CleanupBatch {
                domain_id: id16(r, what)?,
                close_generation: r.u64(what)?,
                cleanup_id: id16(r, what)?,
                batch_number: r.u64(what)?,
                candidate_objects: RootRef::decode(r)?,
                completed_results: option_decode(r, RootRef::decode)?,
                state: CleanupState::from_u8(r.u8(what)?)?,
            }),
            ControlKind::KvBaseRetention => ControlRecord::KvBaseRetention(KvBaseRetention {
                volume_id: id16(r, what)?,
                layer_id: id16(r, what)?,
                sealed_version: r.u64(what)?,
                logical_revision: hash32(r, what)?,
                first_publication_id: id16(r, what)?,
            }),
            ControlKind::NativeWorkspaceHead => {
                ControlRecord::NativeWorkspaceHead(NativeWorkspaceHead {
                    workspace_id: id16(r, what)?,
                    head: HeadRef::decode(r)?,
                    base: SnapshotRef::decode(r)?,
                    writer_generation: r.u64(what)?,
                    write_domain_id: id16(r, what)?,
                    visible_delta_count: r.u64(what)?,
                    open_orphan_count: r.u64(what)?,
                    orphan_carry_digest: option_decode(r, |r| hash32(r, what))?,
                    state: WorkspaceHeadState::from_u8(r.u8(what)?)?,
                    entity_version: r.u64(what)?,
                })
            }
            ControlKind::NativePublicationJournal => {
                ControlRecord::NativePublicationJournal(NativePublicationJournal {
                    operation_id: id16(r, what)?,
                    workspace_id: option_decode(r, |r| id16(r, what))?,
                    expected_head: option_decode(r, HeadRef::decode)?,
                    expected_base: option_decode(r, SnapshotRef::decode)?,
                    owner_generation: r.u64(what)?,
                    phase: PublicationPhase::from_u8(r.u8(what)?)?,
                    accepted_ticket_end: r.u64(what)?,
                    drain_plan: option_decode(r, RootRef::decode)?,
                    batch_count: r.u64(what)?,
                    completed_count: r.u64(what)?,
                    fixed_commit_seq: option_decode(r, |r| r.u64(what))?,
                    candidate: option_decode(r, SnapshotRef::decode)?,
                    verification_evidence: option_decode(r, RootRef::decode)?,
                    retention_evidence: option_decode(r, RootRef::decode)?,
                    committed_result: option_decode(r, PublicationResult::decode)?,
                })
            }
            ControlKind::NativeDrainBatch => ControlRecord::NativeDrainBatch(NativeDrainBatch {
                operation_id: id16(r, what)?,
                batch_id: r.u64(what)?,
                owner_generation: r.u64(what)?,
                plan_digest: hash32(r, what)?,
                plan: RootRef::decode(r)?,
                state: DrainBatchState::from_u8(r.u8(what)?)?,
                result_digest: option_decode(r, |r| hash32(r, what))?,
            }),
            ControlKind::NativeMutationResult => {
                ControlRecord::NativeMutationResult(NativeMutationResult {
                    operation_id: id16(r, what)?,
                    payload_digest: hash32(r, what)?,
                    head: HeadRef::decode(r)?,
                    inode: r.u64(what)?,
                    inode_data_version: r.u64(what)?,
                    durable_receipts: RootRef::decode(r)?,
                    status: MutationStatus::from_u8(r.u8(what)?)?,
                    stable_error: option_decode(r, |r| Ok(r.bytes(what)?.to_vec()))?,
                })
            }
        })
    }

    /// Decode a full envelope into a record.
    pub fn decode(bytes: &[u8]) -> WireResult<ControlRecord> {
        let envelope = decode_envelope(bytes)?;
        Self::decode_payload(envelope.kind, &envelope.payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native_base::wire::container::{Codec, ObjectKind};
    use crate::native_base::wire::refs::PageAddress;

    fn root_ref() -> RootRef {
        RootRef {
            object: ObjectRef {
                object_id: [5u8; 16],
                kind: ObjectKind::PagedInventory.as_u8(),
                object_len: 512,
                full_hash: [6u8; 32],
                key: b"inv/roots".to_vec(),
            },
            address: PageAddress {
                offset: 64,
                stored_len: 96,
                raw_len: 128,
                codec: Codec::None,
                page_kind: crate::native_base::wire::refs::PageKind::InventoryIndex,
                level: 2,
                entry_count: 9,
                stored_digest: [7u8; 32],
            },
        }
    }

    fn sample_records() -> Vec<ControlRecord> {
        let snapshot = SnapshotRef {
            volume_id: [1u8; 16],
            logical_revision: [2u8; 32],
            manifest: ObjectRef {
                object_id: [3u8; 16],
                kind: ObjectKind::SnapshotManifest.as_u8(),
                object_len: 1024,
                full_hash: [4u8; 32],
                key: b"manifests/m1".to_vec(),
            },
        };
        vec![
            ControlRecord::OwnershipDomain(OwnershipDomain {
                domain_id: [1u8; 16],
                volume_id: [2u8; 16],
                namespace_id: [3u8; 16],
                domain_kind: DomainKind::Workspace,
                owner_id: [4u8; 16],
                owner_generation: 7,
                state: DomainState::Active,
                entity_version: 1,
                inventory_seq: 42,
                retention_seq: 5,
                outstanding_attempts: 0,
                open_operations: 3,
                accepted_ticket_end: Some(99),
                close_ref: Some(root_ref()),
            }),
            ControlRecord::ObjectRegistration(ObjectRegistration {
                object_ref: root_ref().object,
                domain_id: [1u8; 16],
                upload_plan_hash: [8u8; 32],
                registration_seq: 1,
                attempt_generation: 2,
                state: RegistrationState::Verified,
            }),
            ControlRecord::RetentionReceipt(RetentionReceipt {
                domain_id: [1u8; 16],
                retention_seq: 5,
                operation_id: [9u8; 16],
                candidate_view: [1u8; 32],
                retained_objects: root_ref(),
                evidence_root: root_ref(),
                verified_subset_digest: [2u8; 32],
            }),
            ControlRecord::PublishedRevision(PublishedRevision {
                volume_id: [1u8; 16],
                storage_view_id: [3u8; 32],
                logical_revision: [2u8; 32],
                manifest: snapshot.manifest.clone(),
                publication_id: [10u8; 16],
                retained_at_ns: 1_700_000_000_000_000_000,
                retention_policy: RetentionPolicy::Forever,
                evidence_root: root_ref(),
            }),
            ControlRecord::DomainCloseCertificate(DomainCloseCertificate {
                domain_id: [1u8; 16],
                close_generation: 1,
                final_inventory_seq: 42,
                final_retention_seq: 5,
                inventory: Some(root_ref()),
                retained_union: Some(root_ref()),
                control_evidence: None,
                owner_terminal_proof_hash: [11u8; 32],
                attempts_resolved: true,
                operations_drained: true,
                closed_at_ns: 1_700_000_000_000_000_001,
            }),
            ControlRecord::CleanupBatch(CleanupBatch {
                domain_id: [1u8; 16],
                close_generation: 1,
                cleanup_id: [12u8; 16],
                batch_number: 0,
                candidate_objects: root_ref(),
                completed_results: None,
                state: CleanupState::Planned,
            }),
            ControlRecord::KvBaseRetention(KvBaseRetention {
                volume_id: [0x01u8; 16],
                layer_id: [0x02u8; 16],
                sealed_version: 3,
                logical_revision: [0xabu8; 32],
                first_publication_id: [0x04u8; 16],
            }),
            ControlRecord::NativeWorkspaceHead(NativeWorkspaceHead {
                workspace_id: [13u8; 16],
                head: HeadRef {
                    head_id: [14u8; 16],
                    epoch: 2,
                    commit_seq: 77,
                },
                base: snapshot.clone(),
                writer_generation: 4,
                write_domain_id: [1u8; 16],
                visible_delta_count: 10,
                open_orphan_count: 1,
                orphan_carry_digest: Some([15u8; 32]),
                state: WorkspaceHeadState::Running,
                entity_version: 6,
            }),
            ControlRecord::NativePublicationJournal(NativePublicationJournal {
                operation_id: [16u8; 16],
                workspace_id: Some([13u8; 16]),
                expected_head: Some(HeadRef {
                    head_id: [14u8; 16],
                    epoch: 2,
                    commit_seq: 77,
                }),
                expected_base: Some(snapshot.clone()),
                owner_generation: 4,
                phase: PublicationPhase::RetentionPrepared,
                accepted_ticket_end: 99,
                drain_plan: Some(root_ref()),
                batch_count: 2,
                completed_count: 1,
                fixed_commit_seq: Some(100),
                candidate: Some(snapshot.clone()),
                verification_evidence: Some(root_ref()),
                retention_evidence: Some(root_ref()),
                committed_result: Some(PublicationResult {
                    snapshot: snapshot.clone(),
                    new_head: Some(HeadRef {
                        head_id: [17u8; 16],
                        epoch: 3,
                        commit_seq: 0,
                    }),
                    publication_id: [10u8; 16],
                }),
            }),
            ControlRecord::NativeDrainBatch(NativeDrainBatch {
                operation_id: [16u8; 16],
                batch_id: 0,
                owner_generation: 4,
                plan_digest: [18u8; 32],
                plan: root_ref(),
                state: DrainBatchState::Committed,
                result_digest: Some([19u8; 32]),
            }),
            ControlRecord::NativeMutationResult(NativeMutationResult {
                operation_id: [20u8; 16],
                payload_digest: [21u8; 32],
                head: HeadRef {
                    head_id: [14u8; 16],
                    epoch: 2,
                    commit_seq: 78,
                },
                inode: 12345,
                inode_data_version: 4,
                durable_receipts: root_ref(),
                status: MutationStatus::Committed,
                stable_error: None,
            }),
        ]
    }

    #[test]
    fn kind7_golden_byte_exact() {
        // Golden from examples/control-vectors.json.
        let record = ControlRecord::KvBaseRetention(KvBaseRetention {
            volume_id: [0x01; 16],
            layer_id: [0x02; 16],
            sealed_version: 3,
            logical_revision: [0xab; 32],
            first_publication_id: [0x04; 16],
        });
        let enc = record.encode();
        assert_eq!(
            hex::encode(&enc),
            "424e4354020007005800000001010101010101010101010101010101020202020202020202020202020202020300000000000000abababababababababababababababababababababababababababababababab0404040404040404040404040404040489ac824e"
        );
        assert_eq!(enc.len(), 104);
        assert_eq!(ControlRecord::decode(&enc).unwrap(), record);
    }

    #[test]
    fn all_kinds_roundtrip_through_envelope() {
        for record in sample_records() {
            let enc = record.encode();
            assert_eq!(ControlRecord::decode(&enc).unwrap(), record);
            // Envelope kind agrees with the record kind.
            let env = decode_envelope(&enc).unwrap();
            assert_eq!(env.kind, record.kind());
        }
    }

    #[test]
    fn envelope_rejects_wrong_version() {
        // WIRE-006 analogue for control records: control version 1 must be
        // rejected, not guessed by field name.
        let mut enc = sample_records()[0].encode();
        enc[4] = 1;
        enc[5] = 0;
        let crc = crc32c(&enc[..enc.len() - 4]);
        let at = enc.len() - 4;
        enc[at..].copy_from_slice(&crc.to_le_bytes());
        assert!(matches!(
            decode_envelope(&enc),
            Err(WireError::UnsupportedFormat(_))
        ));
    }

    #[test]
    fn envelope_rejects_unknown_kind() {
        let payload = [0u8; 8];
        let mut enc = encode_envelope(ControlKind::KvBaseRetention, &payload);
        enc[6] = 0;
        enc[7] = 8; // kind 8 is unassigned
        let crc = crc32c(&enc[..enc.len() - 4]);
        let at = enc.len() - 4;
        enc[at..].copy_from_slice(&crc.to_le_bytes());
        assert!(matches!(
            decode_envelope(&enc),
            Err(WireError::UnsupportedFormat(_))
        ));
    }

    #[test]
    fn envelope_rejects_crc_and_length_mismatch() {
        let enc = sample_records()[0].encode();
        let mut broken = enc.clone();
        *broken.last_mut().unwrap() ^= 1;
        assert!(matches!(
            decode_envelope(&broken),
            Err(WireError::CrcMismatch { .. })
        ));
        // Payload length one byte larger than provided.
        let mut broken = enc.clone();
        broken[10] += 1; // payload_len byte 0
        let crc = crc32c(&broken[..broken.len() - 4]);
        let at = broken.len() - 4;
        broken[at..].copy_from_slice(&crc.to_le_bytes());
        assert!(matches!(
            decode_envelope(&broken),
            Err(WireError::Truncated { .. })
        ));
    }

    #[test]
    fn envelope_rejects_oversized_payload() {
        let payload = vec![0u8; MAX_PAYLOAD + 1];
        // Manually craft since encode_envelope would truncate u32 fine but
        // the limit check happens on decode.
        let mut w = Writer::new();
        w.put(ENVELOPE_MAGIC);
        w.u16(CONTROL_VERSION);
        w.u16(ControlKind::KvBaseRetention.as_u16());
        w.u32(payload.len() as u32);
        w.put(&payload);
        w.u32(crc32c(w.as_slice()));
        assert!(matches!(
            decode_envelope(w.as_slice()),
            Err(WireError::LimitExceeded(_))
        ));
    }

    #[test]
    fn unknown_enum_values_rejected() {
        // retention_policy != 1 (spec 20 §3: only forever exists).
        let rec = match sample_records()[3].clone() {
            ControlRecord::PublishedRevision(r) => r,
            _ => unreachable!(),
        };
        // Payload layout: volume_id[16] + storage_view_id[32] +
        // logical_revision[32], then manifest ObjectRef (57 fixed bytes +
        // uvarint keylen + key), then publication_id[16] + retained_at_ns[8]
        // + retention_policy:u8.
        let payload = ControlRecord::PublishedRevision(rec).encode_payload();
        let head_len = 16 + 32 + 32;
        let (keylen, keylen_bytes) =
            crate::native_base::wire::uvarint::decode_uvarint(&payload[head_len + 57..]).unwrap();
        let policy_at = head_len + 57 + keylen_bytes + keylen as usize + 16 + 8;
        let mut bad = payload.clone();
        bad[policy_at] = 2;
        let enc = encode_envelope(ControlKind::PublishedRevision, &bad);
        assert!(matches!(
            ControlRecord::decode(&enc),
            Err(WireError::UnsupportedFormat(_))
        ));
    }

    #[test]
    fn other_unknown_enums_rejected() {
        // Domain state 6.
        let rec = match sample_records()[0].clone() {
            ControlRecord::OwnershipDomain(r) => r,
            _ => unreachable!(),
        };
        let payload = ControlRecord::OwnershipDomain(rec).encode_payload();
        // state sits after domain_id[16] + volume_id[16] + namespace_id[16] +
        // kind[1] + owner_id[16] + owner_generation[8] → offset 73.
        let mut bad = payload.clone();
        bad[73] = 6;
        let enc = encode_envelope(ControlKind::OwnershipDomain, &bad);
        assert!(ControlRecord::decode(&enc).is_err());

        // Publication phase 8.
        let minimal = NativePublicationJournal {
            operation_id: [0u8; 16],
            workspace_id: None,
            expected_head: None,
            expected_base: None,
            owner_generation: 0,
            phase: PublicationPhase::Aborted,
            accepted_ticket_end: 0,
            drain_plan: None,
            batch_count: 0,
            completed_count: 0,
            fixed_commit_seq: None,
            candidate: None,
            verification_evidence: None,
            retention_evidence: None,
            committed_result: None,
        };
        let payload = ControlRecord::NativePublicationJournal(minimal).encode_payload();
        // phase byte: operation_id[16] + 3 option tags + owner_generation[8]
        // = offset 27.
        let mut bad = payload.clone();
        bad[27] = 8;
        let enc = encode_envelope(ControlKind::NativePublicationJournal, &bad);
        assert!(matches!(
            ControlRecord::decode(&enc),
            Err(WireError::UnsupportedFormat(_))
        ));
    }

    #[test]
    fn payload_trailing_bytes_rejected() {
        let rec = sample_records()[6].clone(); // KvBaseRetention (fixed 88B)
        let payload = match rec {
            ControlRecord::KvBaseRetention(r) => ControlRecord::KvBaseRetention(r).encode_payload(),
            _ => unreachable!(),
        };
        let mut padded = payload.clone();
        padded.push(0);
        let enc = encode_envelope(ControlKind::KvBaseRetention, &padded);
        assert!(matches!(
            ControlRecord::decode(&enc),
            Err(WireError::Invalid { .. })
        ));
    }

    #[test]
    fn plan_entries_roundtrip() {
        let entries = vec![
            PlanEntry {
                admission_ticket: 1,
                inode: 10,
                mutation_order: 0,
                logical_offset: 0,
                logical_len: 4096,
                operation_id: [1u8; 16],
                payload_digest: [2u8; 32],
                source_token: b"tok-1".to_vec(),
            },
            PlanEntry {
                admission_ticket: 1,
                inode: 10,
                mutation_order: 1,
                logical_offset: 8192,
                logical_len: 100,
                operation_id: [1u8; 16],
                payload_digest: [3u8; 32],
                source_token: vec![],
            },
        ];
        let mut w = Writer::new();
        w.uvarint(entries.len() as u64);
        for e in &entries {
            e.encode_into(&mut w);
        }
        let mut r = Reader::new(w.as_slice());
        let count = r.uvarint("plan").unwrap();
        let mut decoded = Vec::new();
        for _ in 0..count {
            decoded.push(PlanEntry::decode(&mut r).unwrap());
        }
        assert!(r.is_empty());
        assert_eq!(decoded, entries);
    }
}
