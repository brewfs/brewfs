//! Ownership-domain close certificates and private cleanup (spec 10/20).
//!
//! A native-v2 cleaner is deliberately domain-local.  It never walks all
//! PublishedRevision records and it never treats an expired lease or a
//! successful HTTP request as proof that an upload is gone.  A domain first
//! crosses an atomic ACTIVE -> DRAINING boundary, then receives one immutable
//! close certificate.  Cleanup operates only on the certificate's frozen
//! inventory and on the exact retained/control sets supplied by the caller.

use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use sha2::{Digest, Sha256};

use crate::native_base::wire::bnct::{
    CleanupBatch, CleanupState, ControlRecord, DomainCloseCertificate, DomainState,
    OwnershipDomain, RegistrationState,
};
use crate::native_base::wire::refs::{Hash32, ObjectRef, PageKind, RootRef};
use crate::native_base::write::domain::decode_domain;
use crate::native_base::write::keys::Keys;
use crate::native_base::write::store::{ControlStore, Txn};

use super::index::{BuiltObjectIndex, build_single_value_record, open_object_index};
use super::{LifecycleError, LifecycleResult, map_conflict};

/// Hard limits keep cleanup transactions and object-delete loops bounded.
pub const MAX_CLEANUP_BATCH_OBJECTS: usize = 128;
pub const MAX_CLEANUP_BATCH_BYTES: u64 = 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloseStartRequest {
    pub domain_id: [u8; 16],
    pub expected_owner_generation: u64,
    pub expected_entity_version: u64,
    pub expected_inventory_seq: u64,
    pub expected_retention_seq: u64,
    /// Admission tickets at or below this boundary may finish a registered
    /// drain; no new ticket may be admitted after the boundary.
    pub accepted_ticket_end: u64,
}

/// Atomically fence a domain from new application work and enter DRAINING.
pub async fn close_domain_start(
    store: &dyn ControlStore,
    keys: &Keys,
    request: &CloseStartRequest,
) -> LifecycleResult<OwnershipDomain> {
    let key = keys.domain(&request.domain_id);
    let bytes = store
        .get(&key)
        .await?
        .ok_or_else(|| LifecycleError::InvalidState("ownership domain not found".into()))?;
    let domain =
        decode_domain(&bytes).map_err(|error| LifecycleError::Record(error.to_string()))?;
    if domain.volume_id != keys.volume_id() {
        return Err(LifecycleError::InvalidState(
            "ownership domain belongs to another volume".into(),
        ));
    }
    if domain.state != DomainState::Active {
        return Err(LifecycleError::Conflict(format!(
            "domain is {:?}, expected Active",
            domain.state
        )));
    }
    if domain.owner_generation != request.expected_owner_generation
        || domain.entity_version != request.expected_entity_version
        || domain.inventory_seq != request.expected_inventory_seq
        || domain.retention_seq != request.expected_retention_seq
    {
        return Err(LifecycleError::Conflict(
            "domain changed before close_start".into(),
        ));
    }
    if let Some(previous) = domain.accepted_ticket_end
        && request.accepted_ticket_end < previous
    {
        return Err(LifecycleError::InvalidState(
            "close ticket boundary moves backwards".into(),
        ));
    }

    let mut draining = domain.clone();
    draining.state = DomainState::Draining;
    draining.accepted_ticket_end = Some(request.accepted_ticket_end);
    draining.entity_version = draining
        .entity_version
        .checked_add(1)
        .ok_or_else(|| LifecycleError::LimitExceeded("domain entity version overflow".into()))?;
    store
        .run(Txn::new().check_bytes(key.clone(), bytes).put(
            key,
            ControlRecord::OwnershipDomain(draining.clone()).encode(),
        ))
        .await
        .map_err(|error| map_conflict(error, "domain changed during close_start"))?;
    Ok(draining)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloseArtifacts {
    /// Exact I(d). None is valid only for an empty frozen inventory.
    pub inventory: Option<BuiltObjectIndex>,
    /// Every domain-local RetainBatch referenced by committed receipts.
    pub retention_batches: Vec<BuiltObjectIndex>,
    /// Exact union K(d). None is valid only when there are no retained targets.
    pub retained_union: Option<BuiltObjectIndex>,
    /// Exact C(d), excluding the close/control roots that are directly held by
    /// authority records and therefore must not self-reference.
    pub control_evidence: Option<BuiltObjectIndex>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloseCommitRequest {
    pub domain_id: [u8; 16],
    pub expected_owner_generation: u64,
    pub close_generation: u64,
    pub final_inventory_seq: u64,
    pub final_retention_seq: u64,
    pub artifacts: CloseArtifacts,
    /// Reference to the separately persisted type-3 close-certificate
    /// evidence object, copied into OwnershipDomain.close_ref.  When present
    /// it is an assertion that must hash-match the authoritative certificate;
    /// when absent the authority derives the canonical reference itself.
    pub certificate_ref: Option<RootRef>,
    pub owner_terminal_proof_hash: Hash32,
    pub attempts_resolved: bool,
    pub operations_drained: bool,
    pub closed_at_ns: i64,
}

/// The canonical close-certificate evidence object: one type-3 single-value
/// container holding the authoritative certificate record.
///
/// `close_ref` is the only durable pointer to the close evidence, so it must
/// be reproducible from the KV certificate alone.  Rebuilding the container
/// lets the authority (and every later reader) recompute the expected
/// `full_hash`/`stored_digest` instead of trusting a copy.
fn close_evidence_root(
    volume_id: [u8; 16],
    object_id: [u8; 16],
    certificate: &DomainCloseCertificate,
) -> LifecycleResult<RootRef> {
    let record = ControlRecord::DomainCloseCertificate(certificate.clone()).encode();
    let mut built = build_single_value_record(object_id, Vec::new(), record)?;
    built.root.object.key = crate::native_base::write::receipts::object_key(
        &volume_id,
        "frozen",
        &object_id,
        &built.root.object.full_hash,
    );
    Ok(built.root)
}

/// Resolve the close evidence root for a certificate.
///
/// A caller-supplied reference is verified against the canonical container
/// derived from the authoritative certificate: a copy whose object id or
/// content hash disagrees is rejected before anything is written, so a close
/// can never publish an evidence pointer that does not describe the
/// certificate in the KV authority.
fn resolve_certificate_root(
    volume_id: [u8; 16],
    supplied: &Option<RootRef>,
    certificate: &DomainCloseCertificate,
) -> LifecycleResult<RootRef> {
    let derived = match supplied {
        Some(supplied) => {
            let derived = close_evidence_root(volume_id, supplied.object.object_id, certificate)?;
            if derived != *supplied {
                return Err(LifecycleError::Durability(
                    "close copy hash disagrees with the authoritative KV certificate".into(),
                ));
            }
            derived
        }
        None => {
            let object_id = *uuid::Uuid::now_v7().as_bytes();
            close_evidence_root(volume_id, object_id, certificate)?
        }
    };
    Ok(derived)
}

fn validate_certificate_root(root: &Option<RootRef>) -> LifecycleResult<()> {
    let Some(root) = root.as_ref() else {
        // The authority derives the canonical evidence root itself when the
        // caller has not published a copy yet.
        return Ok(());
    };
    if root.object.kind != 3 || root.address.page_kind != PageKind::GenericKeyValue {
        return Err(LifecycleError::Record(
            "certificate_ref must be a type-3 single-value root".into(),
        ));
    }
    Ok(())
}

fn artifact_entries(artifact: &BuiltObjectIndex, label: &str) -> LifecycleResult<Vec<ObjectRef>> {
    let decoded = open_object_index(&artifact.root, &artifact.bytes)?;
    if decoded != artifact.entries {
        return Err(LifecycleError::Record(format!(
            "{label} decoded entries differ from the supplied artifact"
        )));
    }
    Ok(decoded)
}

fn require_optional_artifact(
    artifact: &Option<BuiltObjectIndex>,
    expected: &[ObjectRef],
    label: &str,
) -> LifecycleResult<Option<RootRef>> {
    match (artifact, expected.is_empty()) {
        (None, true) => Ok(None),
        (Some(_), true) => Err(LifecycleError::Record(format!(
            "{label} must be None for an empty set"
        ))),
        (None, false) => Err(LifecycleError::Record(format!(
            "{label} is required for a non-empty set"
        ))),
        (Some(artifact), false) => {
            if artifact_entries(artifact, label)? != expected {
                return Err(LifecycleError::Retention(format!(
                    "{label} does not encode the exact certified set"
                )));
            }
            Ok(Some(artifact.root.clone()))
        }
    }
}

fn certified_artifact_entries(
    certified_root: &Option<RootRef>,
    artifact: &Option<BuiltObjectIndex>,
    label: &str,
) -> LifecycleResult<Vec<ObjectRef>> {
    match (certified_root, artifact) {
        (None, None) => Ok(Vec::new()),
        (Some(root), Some(artifact)) if *root == artifact.root => artifact_entries(artifact, label),
        (Some(_), Some(_)) => Err(LifecycleError::Retention(format!(
            "{label} bytes do not match the root fixed by the close certificate"
        ))),
        (Some(_), None) => Err(LifecycleError::Durability(format!(
            "{label} bytes are unavailable; cleanup must retain all"
        ))),
        (None, Some(_)) => Err(LifecycleError::Record(format!(
            "{label} was supplied but the close certificate encodes None"
        ))),
    }
}

fn close_registration_is_terminal(state: RegistrationState) -> bool {
    matches!(
        state,
        RegistrationState::Verified | RegistrationState::Abandoned | RegistrationState::Deleted
    )
}

async fn quarantine_draining_domain(
    store: &dyn ControlStore,
    key: Vec<u8>,
    bytes: Vec<u8>,
    domain: &OwnershipDomain,
) -> LifecycleResult<()> {
    if domain.state != DomainState::Draining {
        return Ok(());
    }
    let mut quarantined = domain.clone();
    quarantined.state = DomainState::Quarantined;
    quarantined.entity_version = quarantined
        .entity_version
        .checked_add(1)
        .ok_or_else(|| LifecycleError::LimitExceeded("domain entity version overflow".into()))?;
    store
        .run(
            Txn::new()
                .check_bytes(key.clone(), bytes)
                .put(key, ControlRecord::OwnershipDomain(quarantined).encode()),
        )
        .await
        .map_err(|error| map_conflict(error, "domain changed while entering quarantine"))?;
    Ok(())
}

/// Persist an immutable DomainCloseCertificate and move DRAINING -> CLOSED.
/// The certificate and state transition share one conditional transaction.
pub async fn close_domain_commit(
    store: &dyn ControlStore,
    keys: &Keys,
    request: &CloseCommitRequest,
) -> LifecycleResult<DomainCloseCertificate> {
    if request.close_generation == 0 {
        return Err(LifecycleError::InvalidState(
            "close generation must be non-zero".into(),
        ));
    }
    validate_certificate_root(&request.certificate_ref)?;

    let domain_key = keys.domain(&request.domain_id);
    let domain_bytes = store
        .get(&domain_key)
        .await?
        .ok_or_else(|| LifecycleError::InvalidState("ownership domain not found".into()))?;
    let domain =
        decode_domain(&domain_bytes).map_err(|error| LifecycleError::Record(error.to_string()))?;
    if domain.volume_id != keys.volume_id()
        || domain.owner_generation != request.expected_owner_generation
    {
        return Err(LifecycleError::Conflict(
            "domain owner or volume changed before close_commit".into(),
        ));
    }
    if domain.state != DomainState::Draining {
        return Err(LifecycleError::Conflict(format!(
            "domain is {:?}, expected Draining",
            domain.state
        )));
    }
    if !request.attempts_resolved {
        quarantine_draining_domain(store, domain_key.clone(), domain_bytes.clone(), &domain)
            .await?;
        return Err(LifecycleError::Durability(
            "close attempt evidence is unresolved; domain quarantined".into(),
        ));
    }
    if !request.operations_drained {
        return Err(LifecycleError::Durability(
            "close certificate lacks terminal operation drain proof".into(),
        ));
    }
    if domain.inventory_seq != request.final_inventory_seq
        || domain.retention_seq != request.final_retention_seq
    {
        return Err(LifecycleError::Conflict(
            "close certificate sequence does not match frozen domain".into(),
        ));
    }
    if domain.outstanding_attempts != 0 || domain.open_operations != 0 {
        return Err(LifecycleError::Durability(
            "domain still has outstanding attempts or operations".into(),
        ));
    }

    // A close certificate cannot freeze an inventory while an unresolved
    // registration remains.  The scan is outside the transaction; every row
    // is checked again in the transaction below by its exact bytes.
    let registration_rows = store.scan(&keys.objects_prefix(&request.domain_id)).await?;
    let inventory_rows = store
        .scan(&keys.inventory_prefix(&request.domain_id))
        .await?;
    if inventory_rows.len() as u64 != request.final_inventory_seq {
        return Err(LifecycleError::Durability(format!(
            "close inventory has {} rows, expected {}",
            inventory_rows.len(),
            request.final_inventory_seq
        )));
    }
    for (index, (key, value)) in inventory_rows.iter().enumerate() {
        let expected_key = keys.inventory(&request.domain_id, index as u64 + 1);
        if *key != expected_key || value.len() != 16 {
            return Err(LifecycleError::Record(
                "close inventory sequence is not contiguous and canonical".into(),
            ));
        }
    }
    let mut registration_checks = Vec::with_capacity(registration_rows.len());
    let mut registrations_by_seq = BTreeMap::new();
    let mut registrations_by_id = BTreeMap::new();
    for (key, bytes) in &registration_rows {
        let registration = ControlRecord::decode(bytes).map_err(LifecycleError::from)?;
        let ControlRecord::ObjectRegistration(registration) = registration else {
            return Err(LifecycleError::Record(
                "domain object prefix contains a non-registration record".into(),
            ));
        };
        if registration.domain_id != request.domain_id
            || registration.registration_seq > request.final_inventory_seq
        {
            return Err(LifecycleError::Record(
                "registration is outside the close inventory boundary".into(),
            ));
        }
        if registration.state == RegistrationState::Unknown {
            quarantine_draining_domain(store, domain_key.clone(), domain_bytes.clone(), &domain)
                .await?;
            return Err(LifecycleError::Durability(
                "close inventory contains an UNKNOWN upload attempt; domain quarantined".into(),
            ));
        }
        if !close_registration_is_terminal(registration.state) {
            return Err(LifecycleError::Durability(format!(
                "registration {:02x?} is still {:?}",
                registration.object_ref.object_id, registration.state
            )));
        }
        if registrations_by_seq
            .insert(registration.registration_seq, registration.clone())
            .is_some()
            || registrations_by_id
                .insert(registration.object_ref.object_id, registration.clone())
                .is_some()
        {
            return Err(LifecycleError::Record(
                "close inventory has duplicate registration sequence or ObjectId".into(),
            ));
        }
        registration_checks.push((key.clone(), bytes.clone()));
    }

    if registrations_by_seq.len() != inventory_rows.len() {
        return Err(LifecycleError::Durability(
            "close inventory rows and object registrations are not one-to-one".into(),
        ));
    }
    for (index, (_, value)) in inventory_rows.iter().enumerate() {
        let seq = index as u64 + 1;
        let inventory_object_id: [u8; 16] = value
            .as_slice()
            .try_into()
            .map_err(|_| LifecycleError::Record("inventory row ObjectId is not 16 bytes".into()))?;
        let registration = registrations_by_seq.get(&seq).ok_or_else(|| {
            LifecycleError::Durability("inventory sequence has no registration".into())
        })?;
        if registration.object_ref.object_id != inventory_object_id {
            return Err(LifecycleError::Durability(
                "inventory row points at a different registration".into(),
            ));
        }
    }
    let inventory_objects = registrations_by_id
        .values()
        .map(|registration| registration.object_ref.clone())
        .collect::<Vec<_>>();
    let inventory_root = require_optional_artifact(
        &request.artifacts.inventory,
        &inventory_objects,
        "close inventory",
    )?;

    // Reconstruct K(d) and C(d) only from this domain's committed receipt
    // sequence. No PublishedRevision/global-root scan participates here.
    let receipt_rows = store
        .scan(&keys.retention_receipts_prefix(&request.domain_id))
        .await?;
    if receipt_rows.len() as u64 != request.final_retention_seq {
        return Err(LifecycleError::Durability(format!(
            "close retention sequence has {} rows, expected {}",
            receipt_rows.len(),
            request.final_retention_seq
        )));
    }
    let mut retained = BTreeMap::new();
    let mut control = BTreeMap::new();
    let mut used_batches = BTreeSet::new();
    for (index, (key, bytes)) in receipt_rows.iter().enumerate() {
        let seq = index as u64 + 1;
        if *key != keys.retention_receipt(&request.domain_id, seq) {
            return Err(LifecycleError::Record(
                "retention receipt sequence is not contiguous".into(),
            ));
        }
        let receipt = match ControlRecord::decode(bytes)? {
            ControlRecord::RetentionReceipt(receipt) => receipt,
            _ => {
                return Err(LifecycleError::Record(
                    "retention prefix contains another record kind".into(),
                ));
            }
        };
        if receipt.domain_id != request.domain_id || receipt.retention_seq != seq {
            return Err(LifecycleError::Retention(
                "retention receipt identity differs from its authority key".into(),
            ));
        }
        let (batch_index, batch) = request
            .artifacts
            .retention_batches
            .iter()
            .enumerate()
            .find(|(_, batch)| batch.root == receipt.retained_objects)
            .ok_or_else(|| {
                LifecycleError::Retention("committed RetainBatch bytes are missing at close".into())
            })?;
        if !used_batches.insert(batch_index) {
            return Err(LifecycleError::Retention(
                "two receipts reuse one RetainBatch root".into(),
            ));
        }
        for object in artifact_entries(batch, "RetainBatch")? {
            let registration = registrations_by_id.get(&object.object_id).ok_or_else(|| {
                LifecycleError::Retention("retained object is absent from I(d)".into())
            })?;
            if registration.object_ref != object {
                return Err(LifecycleError::Retention(
                    "RetainBatch ObjectRef differs from the domain registration".into(),
                ));
            }
            if let Some(previous) = retained.insert(object.object_id, object.clone())
                && previous != object
            {
                return Err(LifecycleError::Retention(
                    "RetainBatch union has conflicting ObjectRefs".into(),
                ));
            }
        }
        for evidence in [
            &receipt.retained_objects.object,
            &receipt.evidence_root.object,
        ] {
            let registration = registrations_by_id
                .get(&evidence.object_id)
                .ok_or_else(|| {
                    LifecycleError::Retention(
                        "receipt control object is absent from its origin domain".into(),
                    )
                })?;
            if registration.object_ref != *evidence {
                return Err(LifecycleError::Retention(
                    "receipt control ObjectRef differs from its registration".into(),
                ));
            }
            if let Some(previous) = control.insert(evidence.object_id, evidence.clone())
                && previous != *evidence
            {
                return Err(LifecycleError::Retention(
                    "control evidence union has conflicting ObjectRefs".into(),
                ));
            }
        }
    }
    if used_batches.len() != request.artifacts.retention_batches.len() {
        return Err(LifecycleError::Retention(
            "close request contains an unreferenced RetainBatch artifact".into(),
        ));
    }
    let retained_objects = retained.into_values().collect::<Vec<_>>();
    let control_objects = control.into_values().collect::<Vec<_>>();
    let retained_union_root = require_optional_artifact(
        &request.artifacts.retained_union,
        &retained_objects,
        "retained union",
    )?;
    let control_evidence_root = require_optional_artifact(
        &request.artifacts.control_evidence,
        &control_objects,
        "control evidence",
    )?;

    let certificate = DomainCloseCertificate {
        domain_id: request.domain_id,
        close_generation: request.close_generation,
        final_inventory_seq: request.final_inventory_seq,
        final_retention_seq: request.final_retention_seq,
        inventory: inventory_root,
        retained_union: retained_union_root,
        control_evidence: control_evidence_root,
        owner_terminal_proof_hash: request.owner_terminal_proof_hash,
        attempts_resolved: request.attempts_resolved,
        operations_drained: request.operations_drained,
        closed_at_ns: request.closed_at_ns,
    };
    // Fix the evidence pointer from the authoritative certificate before any
    // write: a caller-supplied copy that does not hash-match is rejected here,
    // so no close ever publishes a pointer that disagrees with the KV record.
    let certificate_ref =
        resolve_certificate_root(keys.volume_id(), &request.certificate_ref, &certificate)?;
    let certificate_key = keys.close_certificate(&request.domain_id);
    if let Some(existing_bytes) = store.get(&certificate_key).await? {
        let existing = match ControlRecord::decode(&existing_bytes)? {
            ControlRecord::DomainCloseCertificate(certificate) => certificate,
            _ => {
                return Err(LifecycleError::Record(
                    "close certificate key contains another record kind".into(),
                ));
            }
        };
        if existing != certificate {
            return Err(LifecycleError::OperationIdMismatch(
                "domain already has a different close certificate".into(),
            ));
        }
        return Ok(existing);
    }

    let mut closed = domain.clone();
    closed.state = DomainState::Closed;
    closed.close_ref = Some(certificate_ref);
    closed.entity_version = closed
        .entity_version
        .checked_add(1)
        .ok_or_else(|| LifecycleError::LimitExceeded("domain entity version overflow".into()))?;
    let mut txn = Txn::new()
        .check_bytes(domain_key.clone(), domain_bytes)
        .check_absent(certificate_key.clone())
        .put(
            certificate_key,
            ControlRecord::DomainCloseCertificate(certificate.clone()).encode(),
        )
        .put(domain_key, ControlRecord::OwnershipDomain(closed).encode());
    for (key, bytes) in registration_checks {
        txn = txn.check_bytes(key, bytes);
    }
    store
        .run(txn)
        .await
        .map_err(|error| map_conflict(error, "domain changed during close_commit"))?;
    Ok(certificate)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CleanupPlan {
    pub domain_id: [u8; 16],
    pub close_generation: u64,
    pub final_inventory_seq: u64,
    pub final_retention_seq: u64,
    pub candidate_objects: Vec<ObjectRef>,
    pub candidate_bytes: u64,
    pub plan_digest: Hash32,
    pub total_batches: u64,
    batch_ends: Vec<usize>,
    inventory_root: Option<RootRef>,
    retained_union_root: Option<RootRef>,
    control_evidence_root: Option<RootRef>,
    close_ref: RootRef,
}

impl CleanupPlan {
    pub fn batch(&self, batch_number: u64) -> LifecycleResult<&[ObjectRef]> {
        if batch_number >= self.total_batches {
            return Err(LifecycleError::InvalidState(format!(
                "cleanup batch {batch_number} is outside {} batches",
                self.total_batches
            )));
        }
        let index = usize::try_from(batch_number)
            .map_err(|_| LifecycleError::LimitExceeded("cleanup batch offset overflow".into()))?;
        let start = if index == 0 {
            0
        } else {
            self.batch_ends[index - 1]
        };
        let end = self.batch_ends[index];
        Ok(&self.candidate_objects[start..end])
    }

    pub fn batch_count(objects: usize) -> u64 {
        objects.div_ceil(MAX_CLEANUP_BATCH_OBJECTS) as u64
    }
}

fn encoded_object_ref_len(object: &ObjectRef) -> usize {
    let mut writer = crate::native_base::wire::uvarint::Writer::new();
    object.encode_into(&mut writer);
    writer.as_slice().len()
}

fn cleanup_batch_ends(objects: &[ObjectRef]) -> LifecycleResult<Vec<usize>> {
    let mut ends = Vec::new();
    let mut batch_start = 0usize;
    let mut encoded_bytes = 0usize;
    for (index, object) in objects.iter().enumerate() {
        let descriptor_bytes = encoded_object_ref_len(object);
        if descriptor_bytes > MAX_CLEANUP_BATCH_BYTES as usize {
            return Err(LifecycleError::LimitExceeded(
                "one cleanup ObjectRef exceeds the batch payload limit".into(),
            ));
        }
        let count = index - batch_start;
        if count == MAX_CLEANUP_BATCH_OBJECTS
            || encoded_bytes + descriptor_bytes > MAX_CLEANUP_BATCH_BYTES as usize
        {
            ends.push(index);
            batch_start = index;
            encoded_bytes = 0;
        }
        encoded_bytes += descriptor_bytes;
    }
    if batch_start < objects.len() {
        ends.push(objects.len());
    }
    Ok(ends)
}

fn canonical_object_map(
    objects: &[ObjectRef],
    label: &str,
) -> LifecycleResult<BTreeMap<[u8; 16], ObjectRef>> {
    let mut map = BTreeMap::new();
    for object in objects {
        crate::native_base::wire::refs::validate_object_key(&object.key)?;
        if !(1..=6).contains(&object.kind) {
            return Err(LifecycleError::Cleanup(format!(
                "{label} contains unsupported object kind {}",
                object.kind
            )));
        }
        if let Some(previous) = map.insert(object.object_id, object.clone())
            && previous != *object
        {
            return Err(LifecycleError::Cleanup(format!(
                "{label} has conflicting ObjectRefs for {:02x?}",
                object.object_id
            )));
        }
    }
    Ok(map)
}

fn cleanup_digest(
    domain_id: &[u8; 16],
    close_generation: u64,
    final_inventory_seq: u64,
    final_retention_seq: u64,
    candidates: &[ObjectRef],
    retained: &BTreeMap<[u8; 16], ObjectRef>,
    control: &BTreeMap<[u8; 16], ObjectRef>,
) -> Hash32 {
    let mut hasher = Sha256::new();
    hasher.update(b"BrewFS.PrivateCleanup.v1\0");
    hasher.update(domain_id);
    hasher.update(close_generation.to_be_bytes());
    hasher.update(final_inventory_seq.to_be_bytes());
    hasher.update(final_retention_seq.to_be_bytes());
    let candidate_objects = candidates.to_vec();
    let retained_objects = retained.values().cloned().collect::<Vec<_>>();
    let control_objects = control.values().cloned().collect::<Vec<_>>();
    for (tag, objects) in [
        (b'C', candidate_objects),
        (b'R', retained_objects),
        (b'E', control_objects),
    ] {
        hasher.update([tag]);
        hasher.update((objects.len() as u64).to_be_bytes());
        for object in objects {
            let encoded = {
                let mut writer = crate::native_base::wire::uvarint::Writer::new();
                object.encode_into(&mut writer);
                writer.into_bytes()
            };
            hasher.update((encoded.len() as u64).to_be_bytes());
            hasher.update(encoded);
        }
    }
    hasher.finalize().into()
}

/// A read-only, exact set difference over one CLOSED domain's frozen object
/// registrations. I(d), K(d), and C(d) are opened from roots fixed by the
/// close certificate; none is accepted as an unauthenticated caller set and
/// no global PublishedRevision scan participates.
pub async fn plan_private_cleanup(
    store: &dyn ControlStore,
    keys: &Keys,
    domain_id: &[u8; 16],
    artifacts: &CloseArtifacts,
) -> LifecycleResult<CleanupPlan> {
    let cert_bytes = store
        .get(&keys.close_certificate(domain_id))
        .await?
        .ok_or_else(|| LifecycleError::InvalidState("domain close certificate not found".into()))?;
    let certificate = match ControlRecord::decode(&cert_bytes)? {
        ControlRecord::DomainCloseCertificate(certificate) => certificate,
        _ => {
            return Err(LifecycleError::Record(
                "close certificate key contains another record kind".into(),
            ));
        }
    };
    if certificate.domain_id != *domain_id {
        return Err(LifecycleError::Record(
            "close certificate domain id mismatch".into(),
        ));
    }
    let domain_bytes = store
        .get(&keys.domain(domain_id))
        .await?
        .ok_or_else(|| LifecycleError::InvalidState("ownership domain not found".into()))?;
    let domain =
        decode_domain(&domain_bytes).map_err(|error| LifecycleError::Record(error.to_string()))?;
    if !matches!(
        domain.state,
        DomainState::Closed | DomainState::Cleaning | DomainState::Cleaned
    ) || domain.close_ref.is_none()
    {
        return Err(LifecycleError::InvalidState(
            "private cleanup requires a valid CLOSED domain certificate".into(),
        ));
    }
    if domain.inventory_seq != certificate.final_inventory_seq
        || domain.retention_seq != certificate.final_retention_seq
    {
        return Err(LifecycleError::Retention(
            "domain sequence changed after close certificate".into(),
        ));
    }
    let close_ref = domain.close_ref.clone().ok_or_else(|| {
        LifecycleError::Durability("closed domain lost its close evidence root".into())
    })?;

    let inventory_objects = certified_artifact_entries(
        &certificate.inventory,
        &artifacts.inventory,
        "close inventory",
    )?;
    let retained_objects = certified_artifact_entries(
        &certificate.retained_union,
        &artifacts.retained_union,
        "retained union",
    )?;
    let control_objects = certified_artifact_entries(
        &certificate.control_evidence,
        &artifacts.control_evidence,
        "control evidence",
    )?;
    let inventory = canonical_object_map(&inventory_objects, "inventory")?;
    let retained = canonical_object_map(&retained_objects, "retained set")?;
    let control = canonical_object_map(&control_objects, "control evidence set")?;
    let mut protected = retained.clone();
    for (id, object) in &control {
        if let Some(previous) = protected.insert(*id, object.clone())
            && previous != *object
        {
            return Err(LifecycleError::Cleanup(
                "retained and control sets disagree about an ObjectId".into(),
            ));
        }
    }

    let rows = store.scan(&keys.objects_prefix(domain_id)).await?;
    let mut registrations = BTreeMap::new();
    for (key, bytes) in rows {
        let registration = match ControlRecord::decode(&bytes)? {
            ControlRecord::ObjectRegistration(registration) => registration,
            _ => {
                return Err(LifecycleError::Record(
                    "object prefix contains a non-registration record".into(),
                ));
            }
        };
        if registration.domain_id != *domain_id
            || registration.registration_seq > certificate.final_inventory_seq
        {
            return Err(LifecycleError::Retention(
                "registration is outside the frozen close inventory".into(),
            ));
        }
        if !matches!(
            registration.state,
            RegistrationState::Verified
                | RegistrationState::Abandoned
                | RegistrationState::DeletePending
                | RegistrationState::Deleted
        ) {
            return Err(LifecycleError::Durability(
                "cleanup inventory contains an unresolved upload attempt".into(),
            ));
        }
        if registrations
            .insert(
                registration.object_ref.object_id,
                (registration, key, bytes),
            )
            .is_some()
        {
            return Err(LifecycleError::Record(
                "frozen inventory has duplicate ObjectId registrations".into(),
            ));
        }
    }
    let registered_objects = registrations
        .iter()
        .map(|(id, (registration, _, _))| (*id, registration.object_ref.clone()))
        .collect::<BTreeMap<_, _>>();
    if registered_objects != inventory {
        return Err(LifecycleError::Retention(
            "authority registrations no longer match certified I(d)".into(),
        ));
    }
    for (id, object) in protected.iter() {
        if inventory.get(id) != Some(object) {
            return Err(LifecycleError::Retention(
                "certified K(d)/C(d) contains an object outside I(d)".into(),
            ));
        }
    }
    let mut candidates = Vec::new();
    for (id, (registration, _, _)) in registrations {
        // Keep already-DELETED eligible identities in the plan. This makes a
        // reconstructed plan stable after a crash between batches; apply will
        // skip their backend calls while preserving the original digest.
        if protected.contains_key(&id) {
            continue;
        }
        candidates.push(registration.object_ref);
    }
    candidates.sort_by_key(|object| object.object_id);
    let mut candidate_bytes = 0u64;
    for object in &candidates {
        candidate_bytes = candidate_bytes
            .checked_add(object.object_len)
            .ok_or_else(|| LifecycleError::LimitExceeded("cleanup byte sum overflow".into()))?;
    }
    let batch_ends = cleanup_batch_ends(&candidates)?;
    let total_batches = batch_ends.len() as u64;
    let plan_digest = cleanup_digest(
        domain_id,
        certificate.close_generation,
        certificate.final_inventory_seq,
        certificate.final_retention_seq,
        &candidates,
        &retained,
        &control,
    );
    Ok(CleanupPlan {
        domain_id: *domain_id,
        close_generation: certificate.close_generation,
        final_inventory_seq: certificate.final_inventory_seq,
        final_retention_seq: certificate.final_retention_seq,
        candidate_objects: candidates,
        candidate_bytes,
        plan_digest,
        total_batches,
        batch_ends,
        inventory_root: certificate.inventory,
        retained_union_root: certificate.retained_union,
        control_evidence_root: certificate.control_evidence,
        close_ref,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CleanupApplyRequest {
    pub cleanup_id: [u8; 16],
    pub batch_number: u64,
    pub plan: CleanupPlan,
    pub candidate_batch: Option<BuiltObjectIndex>,
    pub capabilities: CleanupCapabilities,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NamespaceVersioning {
    Disabled,
    Enabled,
    Suspended,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthorityHealth {
    Verified,
    RecoveryRetainAll,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CleanupCapabilities {
    pub authority: AuthorityHealth,
    pub namespace_versioning: NamespaceVersioning,
    pub object_lock_enabled: bool,
    pub delete_supported: bool,
}

impl CleanupCapabilities {
    pub const fn verified_unversioned() -> Self {
        Self {
            authority: AuthorityHealth::Verified,
            namespace_versioning: NamespaceVersioning::Disabled,
            object_lock_enabled: false,
            delete_supported: true,
        }
    }

    fn validate(self) -> LifecycleResult<()> {
        if self.authority != AuthorityHealth::Verified {
            return Err(LifecycleError::Durability(
                "authority is in RECOVERY_RETAIN_ALL".into(),
            ));
        }
        if self.namespace_versioning != NamespaceVersioning::Disabled
            || self.object_lock_enabled
            || !self.delete_supported
        {
            return Err(LifecycleError::Cleanup(
                "cleanup apply requires verified unversioned, unlocked delete capability".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CleanupApplyStatus {
    Complete,
    Blocked,
    AlreadyComplete,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CleanupApplyResult {
    pub status: CleanupApplyStatus,
    pub domain_state: DomainState,
    pub deleted_objects: u64,
    pub deleted_bytes: u64,
    pub failed_objects: u64,
}

#[async_trait]
pub trait PrivateObjectDeleter: Send + Sync {
    async fn delete_private_object(&self, key: &str) -> anyhow::Result<PrivateDeleteResult>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrivateDeleteResult {
    Deleted,
    AlreadyAbsent,
}

fn encode_cleanup_operation(plan: &CleanupPlan) -> Vec<u8> {
    let mut value = Vec::with_capacity(48);
    value.extend_from_slice(&plan.plan_digest);
    value.extend_from_slice(&plan.total_batches.to_be_bytes());
    value.extend_from_slice(&plan.close_generation.to_be_bytes());
    value
}

fn decode_cleanup_operation(value: &[u8]) -> LifecycleResult<([u8; 32], u64, u64)> {
    if value.len() != 48 {
        return Err(LifecycleError::Record(
            "cleanup operation row has an invalid length".into(),
        ));
    }
    Ok((
        value[..32].try_into().unwrap(),
        u64::from_be_bytes(value[32..40].try_into().unwrap()),
        u64::from_be_bytes(value[40..48].try_into().unwrap()),
    ))
}

fn ensure_candidate_batch(
    request: &CleanupApplyRequest,
    batch: &[ObjectRef],
) -> LifecycleResult<u64> {
    if request.plan.domain_id == [0u8; 16] || request.cleanup_id == [0u8; 16] {
        return Err(LifecycleError::InvalidState(
            "cleanup domain and operation ids must be non-zero".into(),
        ));
    }
    let candidate_batch = request.candidate_batch.as_ref().ok_or_else(|| {
        LifecycleError::Record("non-empty cleanup batch has no authenticated index".into())
    })?;
    let decoded = artifact_entries(candidate_batch, "cleanup candidate batch")?;
    if decoded != batch {
        return Err(LifecycleError::Cleanup(
            "cleanup candidate index does not encode this plan batch".into(),
        ));
    }
    let mut descriptor_bytes = 0usize;
    let mut ids = BTreeSet::new();
    for object in batch {
        if !ids.insert(object.object_id) {
            return Err(LifecycleError::Cleanup(
                "cleanup batch contains duplicate ObjectId".into(),
            ));
        }
        descriptor_bytes = descriptor_bytes
            .checked_add(encoded_object_ref_len(object))
            .ok_or_else(|| {
                LifecycleError::LimitExceeded("cleanup batch payload size overflow".into())
            })?;
    }
    if batch.len() > MAX_CLEANUP_BATCH_OBJECTS
        || descriptor_bytes > MAX_CLEANUP_BATCH_BYTES as usize
    {
        return Err(LifecycleError::LimitExceeded(
            "cleanup batch exceeds object or byte limit".into(),
        ));
    }
    batch
        .iter()
        .try_fold(0u64, |sum, object| sum.checked_add(object.object_len))
        .ok_or_else(|| LifecycleError::LimitExceeded("cleanup batch byte sum overflow".into()))
}

async fn load_cleanup_authority(
    store: &dyn ControlStore,
    keys: &Keys,
    plan: &CleanupPlan,
) -> LifecycleResult<(Vec<u8>, OwnershipDomain)> {
    let certificate_bytes = store
        .get(&keys.close_certificate(&plan.domain_id))
        .await?
        .ok_or_else(|| LifecycleError::Durability("close certificate disappeared".into()))?;
    let certificate = match ControlRecord::decode(&certificate_bytes)? {
        ControlRecord::DomainCloseCertificate(certificate) => certificate,
        _ => {
            return Err(LifecycleError::Record(
                "close certificate key contains another record kind".into(),
            ));
        }
    };
    if certificate.domain_id != plan.domain_id
        || certificate.close_generation != plan.close_generation
        || certificate.final_inventory_seq != plan.final_inventory_seq
        || certificate.final_retention_seq != plan.final_retention_seq
        || certificate.inventory != plan.inventory_root
        || certificate.retained_union != plan.retained_union_root
        || certificate.control_evidence != plan.control_evidence_root
    {
        return Err(LifecycleError::Durability(
            "cleanup plan no longer matches the fixed close certificate".into(),
        ));
    }
    // The plan carries the published close copy.  Rebuild the canonical
    // container from the authoritative KV certificate and require an exact
    // hash-level match: a swapped/rolled-back certificate stops cleanup
    // before any delete instead of silently cleaning under stale evidence.
    let rebuilt = close_evidence_root(
        keys.volume_id(),
        plan.close_ref.object.object_id,
        &certificate,
    )?;
    if rebuilt != plan.close_ref {
        return Err(LifecycleError::Durability(
            "close copy hash disagrees with the authoritative KV certificate".into(),
        ));
    }
    let domain_key = keys.domain(&plan.domain_id);
    let domain_bytes = store
        .get(&domain_key)
        .await?
        .ok_or_else(|| LifecycleError::InvalidState("cleanup domain not found".into()))?;
    let domain =
        decode_domain(&domain_bytes).map_err(|error| LifecycleError::Record(error.to_string()))?;
    if !matches!(
        domain.state,
        DomainState::Closed | DomainState::Cleaning | DomainState::Cleaned
    ) || domain.inventory_seq != plan.final_inventory_seq
        || domain.retention_seq != plan.final_retention_seq
        || domain.close_ref.as_ref() != Some(&plan.close_ref)
    {
        return Err(LifecycleError::Durability(
            "cleanup domain authority changed after planning".into(),
        ));
    }
    Ok((domain_bytes, domain))
}

/// Apply one bounded cleanup batch.  Registration rows are marked
/// DELETE_PENDING before any backend call; successful deletes then become
/// DELETED.  Any backend error leaves the batch BLOCKED and preserves the
/// remaining identities for a later, explicit retry.
pub async fn apply_private_cleanup<B: PrivateObjectDeleter + ?Sized>(
    store: &dyn ControlStore,
    keys: &Keys,
    backend: &B,
    request: &CleanupApplyRequest,
) -> LifecycleResult<CleanupApplyResult> {
    request.capabilities.validate()?;
    let operation_key = keys.cleanup_operation(&request.cleanup_id);
    let operation_value = encode_cleanup_operation(&request.plan);
    let existing_operation = store.get(&operation_key).await?;
    if let Some(existing) = &existing_operation {
        let (digest, total_batches, close_generation) = decode_cleanup_operation(existing)?;
        if digest != request.plan.plan_digest
            || total_batches != request.plan.total_batches
            || close_generation != request.plan.close_generation
        {
            return Err(LifecycleError::OperationIdMismatch(
                "cleanup id was reused with another plan".into(),
            ));
        }
    }
    let (domain_bytes, domain) = load_cleanup_authority(store, keys, &request.plan).await?;

    // An empty frozen inventory has no object batch to persist, but it still
    // needs an idempotent CLOSED -> CLEANED transition.
    if request.plan.total_batches == 0 {
        if request.candidate_batch.is_some() {
            return Err(LifecycleError::Record(
                "empty cleanup plan must not carry a candidate index".into(),
            ));
        }
        let domain_key = keys.domain(&request.plan.domain_id);
        if domain.state == DomainState::Cleaned {
            return Ok(CleanupApplyResult {
                status: CleanupApplyStatus::AlreadyComplete,
                domain_state: DomainState::Cleaned,
                deleted_objects: 0,
                deleted_bytes: 0,
                failed_objects: 0,
            });
        }
        let mut cleaned = domain.clone();
        cleaned.state = DomainState::Cleaned;
        cleaned.entity_version = cleaned.entity_version.checked_add(1).ok_or_else(|| {
            LifecycleError::LimitExceeded("domain entity version overflow".into())
        })?;
        let mut txn = Txn::new().check_bytes(domain_key.clone(), domain_bytes);
        txn = match existing_operation {
            Some(existing) => txn.check_bytes(operation_key.clone(), existing),
            None => txn
                .check_absent(operation_key.clone())
                .put(operation_key, operation_value),
        };
        store
            .run(txn.put(domain_key, ControlRecord::OwnershipDomain(cleaned).encode()))
            .await
            .map_err(|error| map_conflict(error, "empty cleanup raced"))?;
        return Ok(CleanupApplyResult {
            status: CleanupApplyStatus::Complete,
            domain_state: DomainState::Cleaned,
            deleted_objects: 0,
            deleted_bytes: 0,
            failed_objects: 0,
        });
    }

    let batch = request.plan.batch(request.batch_number)?.to_vec();
    ensure_candidate_batch(request, &batch)?;

    let domain_key = keys.domain(&request.plan.domain_id);
    let batch_key = keys.cleanup_batch(
        &request.plan.domain_id,
        request.plan.close_generation,
        request.batch_number,
    );
    let existing_batch = store.get(&batch_key).await?;
    if let Some(existing_bytes) = &existing_batch {
        let existing = match ControlRecord::decode(existing_bytes)? {
            ControlRecord::CleanupBatch(batch) => batch,
            _ => {
                return Err(LifecycleError::Record(
                    "cleanup batch key contains another record kind".into(),
                ));
            }
        };
        if existing.cleanup_id != request.cleanup_id
            || existing.candidate_objects
                != request
                    .candidate_batch
                    .as_ref()
                    .expect("non-empty batch checked above")
                    .root
        {
            return Err(LifecycleError::OperationIdMismatch(
                "cleanup batch was reused with another candidate root".into(),
            ));
        }
        if existing.state == CleanupState::Complete {
            return Ok(CleanupApplyResult {
                status: CleanupApplyStatus::AlreadyComplete,
                domain_state: domain.state,
                deleted_objects: 0,
                deleted_bytes: 0,
                failed_objects: 0,
            });
        }
        if existing.state == CleanupState::Deleting {
            return Err(LifecycleError::Conflict(
                "cleanup batch is already owned by an active cleaner".into(),
            ));
        }
        if domain.state == DomainState::Cleaned {
            return Err(LifecycleError::Conflict(
                "cleanup domain is already CLEANED; no incomplete batch may run".into(),
            ));
        }
    }
    if domain.state == DomainState::Cleaned {
        return Err(LifecycleError::Conflict(
            "cleanup domain is already CLEANED; batch journal is missing".into(),
        ));
    }

    let mut registrations = Vec::with_capacity(batch.len());
    for object in &batch {
        let object_key = keys.object(&request.plan.domain_id, &object.object_id);
        let bytes = store
            .get(&object_key)
            .await?
            .ok_or_else(|| LifecycleError::Cleanup("candidate registration disappeared".into()))?;
        let registration = match ControlRecord::decode(&bytes)? {
            ControlRecord::ObjectRegistration(registration) => registration,
            _ => {
                return Err(LifecycleError::Record(
                    "candidate object key contains another record kind".into(),
                ));
            }
        };
        if registration.object_ref != *object
            || registration.domain_id != request.plan.domain_id
            || !matches!(
                registration.state,
                RegistrationState::Verified
                    | RegistrationState::Abandoned
                    | RegistrationState::DeletePending
                    | RegistrationState::Deleted
            )
        {
            return Err(LifecycleError::Cleanup(
                "candidate registration no longer matches the cleanup plan".into(),
            ));
        }
        registrations.push((object_key, bytes, registration));
    }

    let mut cleaning = domain.clone();
    if cleaning.state == DomainState::Closed {
        cleaning.state = DomainState::Cleaning;
        cleaning.entity_version = cleaning.entity_version.checked_add(1).ok_or_else(|| {
            LifecycleError::LimitExceeded("domain entity version overflow".into())
        })?;
    }
    let cleanup_record = CleanupBatch {
        domain_id: request.plan.domain_id,
        close_generation: request.plan.close_generation,
        cleanup_id: request.cleanup_id,
        batch_number: request.batch_number,
        candidate_objects: request
            .candidate_batch
            .as_ref()
            .expect("non-empty batch checked above")
            .root
            .clone(),
        completed_results: None,
        state: CleanupState::Deleting,
    };
    let mut txn = Txn::new().check_bytes(domain_key.clone(), domain_bytes);
    txn = match existing_operation {
        Some(existing) => txn.check_bytes(operation_key.clone(), existing),
        None => txn
            .check_absent(operation_key.clone())
            .put(operation_key.clone(), operation_value),
    };
    txn = match existing_batch {
        Some(existing) => txn.check_bytes(batch_key.clone(), existing).put(
            batch_key.clone(),
            ControlRecord::CleanupBatch(cleanup_record).encode(),
        ),
        None => txn.check_absent(batch_key.clone()).put(
            batch_key.clone(),
            ControlRecord::CleanupBatch(cleanup_record).encode(),
        ),
    };
    txn = txn.put(
        domain_key.clone(),
        ControlRecord::OwnershipDomain(cleaning.clone()).encode(),
    );
    for (object_key, bytes, registration) in &registrations {
        if registration.state == RegistrationState::Deleted {
            continue;
        }
        let mut pending = registration.clone();
        pending.state = RegistrationState::DeletePending;
        txn = txn.check_bytes(object_key.clone(), bytes.clone()).put(
            object_key.clone(),
            ControlRecord::ObjectRegistration(pending).encode(),
        );
    }
    store
        .run(txn)
        .await
        .map_err(|error| map_conflict(error, "cleanup batch raced with another cleaner"))?;

    let mut deleted_objects = 0u64;
    let mut deleted_bytes = 0u64;
    let mut failed_objects = 0u64;
    for (object_key, _bytes, expected_registration) in registrations {
        if expected_registration.state == RegistrationState::Deleted {
            continue;
        }
        // The transaction above may have changed the row from Verified to
        // DeletePending.  Re-read that exact row before the network call so
        // the success transition checks the bytes actually persisted.
        let pending_bytes = store
            .get(&object_key)
            .await?
            .ok_or_else(|| LifecycleError::Cleanup("candidate registration disappeared".into()))?;
        let registration = match ControlRecord::decode(&pending_bytes)? {
            ControlRecord::ObjectRegistration(registration)
                if registration.object_ref == expected_registration.object_ref
                    && registration.domain_id == request.plan.domain_id
                    && registration.state == RegistrationState::DeletePending =>
            {
                registration
            }
            _ => {
                return Err(LifecycleError::Cleanup(
                    "candidate registration is not DELETE_PENDING".into(),
                ));
            }
        };
        let key = std::str::from_utf8(&registration.object_ref.key)
            .map_err(|_| LifecycleError::Cleanup("object key is not valid UTF-8".into()))?;
        match backend.delete_private_object(key).await {
            Ok(result) => {
                let mut deleted = registration.clone();
                deleted.state = RegistrationState::Deleted;
                let next_bytes = ControlRecord::ObjectRegistration(deleted).encode();
                store
                    .run(
                        Txn::new()
                            .check_bytes(object_key.clone(), pending_bytes)
                            .put(object_key, next_bytes),
                    )
                    .await
                    .map_err(|error| {
                        map_conflict(error, "object registration changed during cleanup")
                    })?;
                deleted_objects += 1;
                if result == PrivateDeleteResult::Deleted {
                    deleted_bytes = deleted_bytes
                        .checked_add(registration.object_ref.object_len)
                        .ok_or_else(|| {
                            LifecycleError::LimitExceeded("deleted byte sum overflow".into())
                        })?;
                }
            }
            Err(error) => {
                failed_objects += 1;
                tracing::warn!(
                    domain = ?request.plan.domain_id,
                    object_id = ?registration.object_ref.object_id,
                    error = %error,
                    "private cleanup delete is blocked"
                );
            }
        }
    }

    let status = if failed_objects == 0 {
        CleanupApplyStatus::Complete
    } else {
        CleanupApplyStatus::Blocked
    };
    let batch_state = match status {
        CleanupApplyStatus::Complete => CleanupState::Complete,
        CleanupApplyStatus::Blocked => CleanupState::Blocked,
        CleanupApplyStatus::AlreadyComplete => CleanupState::Complete,
    };
    let current_batch = store
        .get(&batch_key)
        .await?
        .ok_or_else(|| LifecycleError::Record("cleanup batch disappeared".into()))?;
    let mut completed = match ControlRecord::decode(&current_batch)? {
        ControlRecord::CleanupBatch(batch) => batch,
        _ => {
            return Err(LifecycleError::Record(
                "cleanup batch changed to another record kind".into(),
            ));
        }
    };
    completed.state = batch_state;

    let mut final_domain = cleaning.clone();
    if status == CleanupApplyStatus::Complete {
        let rows = store
            .scan(
                &keys
                    .cleanup_batches_prefix(&request.plan.domain_id, request.plan.close_generation),
            )
            .await?;
        let completed_batches = rows
            .iter()
            .filter_map(|(_, bytes)| match ControlRecord::decode(bytes).ok() {
                Some(ControlRecord::CleanupBatch(batch))
                    if batch.state == CleanupState::Complete
                        && batch.cleanup_id == request.cleanup_id =>
                {
                    Some(batch.batch_number)
                }
                _ => None,
            })
            .collect::<BTreeSet<_>>();
        let mut completed_batches = completed_batches;
        // The current batch is still in its old state in the store while the
        // final transaction below is being assembled.
        completed_batches.insert(request.batch_number);
        if completed_batches.len() as u64 >= request.plan.total_batches
            && (0..request.plan.total_batches).all(|number| completed_batches.contains(&number))
        {
            final_domain.state = DomainState::Cleaned;
            final_domain.entity_version =
                final_domain.entity_version.checked_add(1).ok_or_else(|| {
                    LifecycleError::LimitExceeded("domain entity version overflow".into())
                })?;
        }
    }
    store
        .run(
            Txn::new()
                .check_bytes(batch_key.clone(), current_batch)
                .check_bytes(
                    domain_key.clone(),
                    ControlRecord::OwnershipDomain(cleaning.clone()).encode(),
                )
                .put(batch_key, ControlRecord::CleanupBatch(completed).encode())
                .put(
                    domain_key,
                    ControlRecord::OwnershipDomain(final_domain.clone()).encode(),
                ),
        )
        .await
        .map_err(|error| map_conflict(error, "cleanup journal changed during completion"))?;
    Ok(CleanupApplyResult {
        status,
        domain_state: final_domain.state,
        deleted_objects,
        deleted_bytes,
        failed_objects,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use tokio::sync::Semaphore;

    use super::*;
    use crate::native_base::lifecycle::index::build_object_index;
    use crate::native_base::lifecycle::retention::build_retain_batch;
    use crate::native_base::wire::bnct::{DomainKind, ObjectRegistration};
    use crate::native_base::wire::container::{Codec, ObjectKind};
    use crate::native_base::wire::refs::{PageAddress, RootRef};
    use crate::native_base::write::memory::MemoryControlStore;

    #[derive(Default)]
    struct FakeDeleter {
        deleted: Mutex<Vec<String>>,
        fail_once: Mutex<BTreeSet<String>>,
    }

    impl FakeDeleter {
        fn failing_once(key: &str) -> Self {
            Self {
                deleted: Mutex::new(Vec::new()),
                fail_once: Mutex::new(BTreeSet::from([key.to_owned()])),
            }
        }
    }

    #[async_trait]
    impl PrivateObjectDeleter for FakeDeleter {
        async fn delete_private_object(&self, key: &str) -> anyhow::Result<PrivateDeleteResult> {
            if self.fail_once.lock().unwrap().remove(key) {
                anyhow::bail!("injected delete failure");
            }
            self.deleted.lock().unwrap().push(key.to_owned());
            Ok(PrivateDeleteResult::Deleted)
        }
    }

    struct BlockingDeleter {
        entered: Semaphore,
        release: Semaphore,
        deleted: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl PrivateObjectDeleter for BlockingDeleter {
        async fn delete_private_object(&self, key: &str) -> anyhow::Result<PrivateDeleteResult> {
            self.entered.add_permits(1);
            self.release.acquire().await.unwrap().forget();
            self.deleted.lock().unwrap().push(key.to_owned());
            Ok(PrivateDeleteResult::Deleted)
        }
    }

    struct ReadableDeleter {
        present: Mutex<BTreeSet<String>>,
    }

    impl ReadableDeleter {
        fn new(keys: impl IntoIterator<Item = String>) -> Self {
            Self {
                present: Mutex::new(keys.into_iter().collect()),
            }
        }

        fn is_readable(&self, key: &str) -> bool {
            self.present.lock().unwrap().contains(key)
        }
    }

    #[async_trait]
    impl PrivateObjectDeleter for ReadableDeleter {
        async fn delete_private_object(&self, key: &str) -> anyhow::Result<PrivateDeleteResult> {
            Ok(if self.present.lock().unwrap().remove(key) {
                PrivateDeleteResult::Deleted
            } else {
                PrivateDeleteResult::AlreadyAbsent
            })
        }
    }

    fn object(id: u8, key: &str) -> ObjectRef {
        ObjectRef {
            object_id: [id; 16],
            kind: ObjectKind::DataPack.as_u8(),
            object_len: u64::from(id),
            full_hash: [id; 32],
            key: key.as_bytes().to_vec(),
        }
    }

    fn root(id: u8) -> RootRef {
        RootRef {
            object: ObjectRef {
                object_id: [id; 16],
                kind: ObjectKind::PagedInventory.as_u8(),
                object_len: 256,
                full_hash: [id; 32],
                key: format!("control/{id}.brfin").into_bytes(),
            },
            address: PageAddress {
                offset: 64,
                stored_len: 64,
                raw_len: 64,
                codec: Codec::None,
                page_kind: PageKind::InventoryIndex,
                level: 0,
                entry_count: 1,
                stored_digest: [id; 32],
            },
        }
    }

    fn certificate_root(id: u8) -> RootRef {
        let mut root = root(id);
        root.object.kind = ObjectKind::FrozenMetadata.as_u8();
        root.address.page_kind = PageKind::GenericKeyValue;
        root
    }

    fn object_index(objects: &[ObjectRef], id: u8) -> BuiltObjectIndex {
        build_object_index(
            objects,
            [id; 16],
            format!("control/{id}.brfin").into_bytes(),
            256,
        )
        .unwrap()
    }

    fn domain(volume: [u8; 16], id: [u8; 16]) -> OwnershipDomain {
        OwnershipDomain {
            domain_id: id,
            volume_id: volume,
            namespace_id: [2; 16],
            domain_kind: DomainKind::Build,
            owner_id: [3; 16],
            owner_generation: 7,
            state: DomainState::Active,
            entity_version: 1,
            inventory_seq: 0,
            retention_seq: 0,
            outstanding_attempts: 0,
            open_operations: 0,
            accepted_ticket_end: None,
            close_ref: None,
        }
    }

    async fn put(store: &dyn ControlStore, key: Vec<u8>, value: Vec<u8>) {
        store
            .run(Txn::new().check_absent(key.clone()).put(key, value))
            .await
            .unwrap();
    }

    async fn put_registration(
        store: &dyn ControlStore,
        keys: &Keys,
        domain_id: [u8; 16],
        seq: u64,
        item: &ObjectRef,
        state: RegistrationState,
    ) {
        let registration = ObjectRegistration {
            object_ref: item.clone(),
            domain_id,
            upload_plan_hash: [seq as u8; 32],
            registration_seq: seq,
            attempt_generation: 1,
            state,
        };
        put(
            store,
            keys.object(&domain_id, &item.object_id),
            ControlRecord::ObjectRegistration(registration).encode(),
        )
        .await;
        put(
            store,
            keys.inventory(&domain_id, seq),
            item.object_id.to_vec(),
        )
        .await;
    }

    fn apply_request(
        plan: CleanupPlan,
        cleanup_id: [u8; 16],
        batch_number: u64,
        index_id: u8,
    ) -> CleanupApplyRequest {
        let candidate_batch = if plan.total_batches == 0 {
            None
        } else {
            Some(object_index(plan.batch(batch_number).unwrap(), index_id))
        };
        CleanupApplyRequest {
            cleanup_id,
            batch_number,
            plan,
            candidate_batch,
            capabilities: CleanupCapabilities::verified_unversioned(),
        }
    }

    async fn seed_closed_domain(
        store: &MemoryControlStore,
        keys: &Keys,
        domain_id: [u8; 16],
        objects: &[ObjectRef],
    ) -> CloseArtifacts {
        seed_draining_domain(store, keys, domain_id, objects).await;
        let artifacts = CloseArtifacts {
            inventory: (!objects.is_empty()).then(|| object_index(objects, 221)),
            retention_batches: Vec::new(),
            retained_union: None,
            control_evidence: None,
        };
        close_domain_commit(
            store,
            keys,
            &CloseCommitRequest {
                domain_id,
                expected_owner_generation: 7,
                close_generation: 1,
                final_inventory_seq: objects.len() as u64,
                final_retention_seq: 0,
                artifacts: artifacts.clone(),
                certificate_ref: None,
                owner_terminal_proof_hash: [225; 32],
                attempts_resolved: true,
                operations_drained: true,
                closed_at_ns: 1,
            },
        )
        .await
        .unwrap();
        artifacts
    }

    /// Seed a domain with `objects` registered and an admitted drain
    /// boundary, leaving it DRAINING and ready for a close commit.
    async fn seed_draining_domain(
        store: &MemoryControlStore,
        keys: &Keys,
        domain_id: [u8; 16],
        objects: &[ObjectRef],
    ) {
        let mut seeded_domain = domain(keys.volume_id(), domain_id);
        seeded_domain.inventory_seq = objects.len() as u64;
        put(
            store,
            keys.domain(&domain_id),
            ControlRecord::OwnershipDomain(seeded_domain).encode(),
        )
        .await;
        for (index, item) in objects.iter().enumerate() {
            put_registration(
                store,
                keys,
                domain_id,
                index as u64 + 1,
                item,
                RegistrationState::Verified,
            )
            .await;
        }
        let draining = close_domain_start(
            store,
            keys,
            &CloseStartRequest {
                domain_id,
                expected_owner_generation: 7,
                expected_entity_version: 1,
                expected_inventory_seq: objects.len() as u64,
                expected_retention_seq: 0,
                accepted_ticket_end: 9,
            },
        )
        .await
        .unwrap();
        assert_eq!(draining.state, DomainState::Draining);
    }

    async fn read_domain(
        store: &dyn ControlStore,
        keys: &Keys,
        domain_id: &[u8; 16],
    ) -> OwnershipDomain {
        decode_domain(&store.get(&keys.domain(domain_id)).await.unwrap().unwrap()).unwrap()
    }

    #[tokio::test]
    async fn ctrl_003_empty_domain_expresses_the_empty_set_as_option_none() {
        // CTRL-003 / INV-12, INV-21: an empty candidate closure is expressed
        // as Option::None end to end. A fabricated (even empty) index artifact
        // must be rejected instead of being turned into a random zero-length
        // RootRef inside the close certificate.
        let volume = [61; 16];
        let domain_id = [62; 16];
        let keys = Keys::new(&volume);
        let store = MemoryControlStore::new();

        let artifacts = seed_closed_domain(&store, &keys, domain_id, &[]).await;
        assert!(artifacts.inventory.is_none());
        assert!(artifacts.retained_union.is_none());
        assert_eq!(
            require_optional_artifact(&artifacts.retained_union, &[], "retained_union").unwrap(),
            None
        );

        let closed = read_domain(&store, &keys, &domain_id).await;
        assert_eq!(closed.state, DomainState::Closed);
        assert_eq!(closed.retention_seq, 0);

        let certificate_bytes = store
            .get(&keys.close_certificate(&domain_id))
            .await
            .unwrap()
            .unwrap();
        let certificate = match ControlRecord::decode(&certificate_bytes).unwrap() {
            ControlRecord::DomainCloseCertificate(certificate) => certificate,
            other => panic!("unexpected close certificate record: {other:?}"),
        };
        assert_eq!(certificate.domain_id, domain_id);
        assert!(certificate.inventory.is_none());
        assert!(certificate.retained_union.is_none());
        assert!(certificate.control_evidence.is_none());

        // An empty domain never produces a RetainBatch record.
        let error = build_retain_batch(domain_id, &[], [63; 16], b"retain/empty".to_vec())
            .expect_err("empty domains must not build a RetainBatch");
        assert!(
            error
                .to_string()
                .contains("empty domains do not need RetainBatch records")
        );

        // An empty object index cannot even be constructed, so a zero-length
        // RootRef has no way to enter the certificate ...
        let empty_index = build_object_index(&[], [64; 16], b"control/empty.brfin".to_vec(), 256);
        assert!(empty_index.is_err());

        // ... and a fabricated artifact is rejected for an empty set.
        let stray = object(66, "private/stray");
        let fabricated = object_index(std::slice::from_ref(&stray), 64);
        let error = require_optional_artifact(&Some(fabricated), &[], "retained_union")
            .expect_err("an empty set must not carry an artifact");
        assert!(error.to_string().contains("must be None for an empty set"));

        // ... and a missing artifact for a non-empty set is rejected too.
        let kept = object(65, "private/kept");
        let error = require_optional_artifact(&None, std::slice::from_ref(&kept), "retained_union")
            .expect_err("a non-empty set requires an artifact");
        assert!(
            error
                .to_string()
                .contains("is required for a non-empty set")
        );
    }

    /// CTRL-004: a published close copy that does not hash-match the
    /// authoritative KV certificate stops the close before any write.
    #[tokio::test]
    async fn ctrl_004_close_copy_hash_mismatch_is_rejected_before_write() {
        let volume = [71; 16];
        let domain_id = [72; 16];
        let keys = Keys::new(&volume);
        let store = MemoryControlStore::new();
        let doomed = object(73, "private/ctrl004");
        seed_draining_domain(&store, &keys, domain_id, std::slice::from_ref(&doomed)).await;

        let request = CloseCommitRequest {
            domain_id,
            expected_owner_generation: 7,
            close_generation: 1,
            final_inventory_seq: 1,
            final_retention_seq: 0,
            artifacts: CloseArtifacts {
                inventory: Some(object_index(std::slice::from_ref(&doomed), 221)),
                retention_batches: Vec::new(),
                retained_union: None,
                control_evidence: None,
            },
            certificate_ref: Some(certificate_root(224)),
            owner_terminal_proof_hash: [225; 32],
            attempts_resolved: true,
            operations_drained: true,
            closed_at_ns: 1,
        };
        let error = close_domain_commit(&store, &keys, &request)
            .await
            .expect_err("a mismatched close copy must be rejected");
        assert!(matches!(error, LifecycleError::Durability(_)));
        assert!(
            error
                .to_string()
                .contains("disagrees with the authoritative KV certificate")
        );

        // Nothing was published: the domain is still Draining and the
        // authority holds no certificate row.
        assert_eq!(
            read_domain(&store, &keys, &domain_id).await.state,
            DomainState::Draining
        );
        assert!(
            store
                .get(&keys.close_certificate(&domain_id))
                .await
                .unwrap()
                .is_none()
        );
    }

    /// CTRL-004: after a valid close, replacing the authoritative KV
    /// certificate stops cleanup before a single delete.
    #[tokio::test]
    async fn ctrl_004_authority_swap_after_plan_stops_cleanup() {
        let volume = [81; 16];
        let domain_id = [82; 16];
        let keys = Keys::new(&volume);
        let store = MemoryControlStore::new();
        let doomed = object(83, "private/ctrl004-swap");
        let artifacts =
            seed_closed_domain(&store, &keys, domain_id, std::slice::from_ref(&doomed)).await;

        // The authority-derived pointer is exactly the canonical container of
        // the KV certificate that was just written.
        let certificate_bytes = store
            .get(&keys.close_certificate(&domain_id))
            .await
            .unwrap()
            .unwrap();
        let certificate = match ControlRecord::decode(&certificate_bytes).unwrap() {
            ControlRecord::DomainCloseCertificate(certificate) => certificate,
            other => panic!("unexpected close certificate record: {other:?}"),
        };
        let close_ref = read_domain(&store, &keys, &domain_id)
            .await
            .close_ref
            .expect("a closed domain must hold its close evidence root");
        assert_eq!(
            close_ref,
            close_evidence_root(keys.volume_id(), close_ref.object.object_id, &certificate)
                .unwrap()
        );

        let plan = plan_private_cleanup(&store, &keys, &domain_id, &artifacts)
            .await
            .unwrap();

        // Roll the KV authority forward with different evidence.
        let mut tampered = certificate;
        tampered.closed_at_ns += 1;
        store
            .run(Txn::new().put(
                keys.close_certificate(&domain_id),
                ControlRecord::DomainCloseCertificate(tampered).encode(),
            ))
            .await
            .unwrap();

        let deleter = FakeDeleter::default();
        let error = apply_private_cleanup(
            &store,
            &keys,
            &deleter,
            &apply_request(plan, [84; 16], 0, 85),
        )
        .await
        .expect_err("a rolled-forward authority must stop cleanup");
        assert!(matches!(error, LifecycleError::Durability(_)));
        assert!(
            error
                .to_string()
                .contains("disagrees with the authoritative KV certificate")
        );
        assert!(deleter.deleted.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn close_freezes_domain_and_cleanup_protects_retained_object() {
        let volume = [1; 16];
        let domain_id = [4; 16];
        let keys = Keys::new(&volume);
        let store = MemoryControlStore::new();
        let doomed = object(10, "private/doomed");
        let kept = object(11, "private/kept");
        let retain_batch = build_retain_batch(
            domain_id,
            std::slice::from_ref(&kept),
            [12; 16],
            b"control/retain-12.brfin".to_vec(),
        )
        .unwrap();
        let control_object = retain_batch.index.root.object.clone();
        let inventory_objects = vec![doomed.clone(), kept.clone(), control_object.clone()];
        let mut seeded_domain = domain(volume, domain_id);
        seeded_domain.inventory_seq = inventory_objects.len() as u64;
        seeded_domain.retention_seq = 1;
        put(
            &store,
            keys.domain(&domain_id),
            ControlRecord::OwnershipDomain(seeded_domain).encode(),
        )
        .await;
        for (index, item) in inventory_objects.iter().enumerate() {
            put_registration(
                &store,
                &keys,
                domain_id,
                index as u64 + 1,
                item,
                RegistrationState::Verified,
            )
            .await;
        }
        put(
            &store,
            keys.retention_receipt(&domain_id, 1),
            ControlRecord::RetentionReceipt(retain_batch.receipt(1, [13; 16], [14; 32])).encode(),
        )
        .await;
        let draining = close_domain_start(
            &store,
            &keys,
            &CloseStartRequest {
                domain_id,
                expected_owner_generation: 7,
                expected_entity_version: 1,
                expected_inventory_seq: inventory_objects.len() as u64,
                expected_retention_seq: 1,
                accepted_ticket_end: 9,
            },
        )
        .await
        .unwrap();
        assert_eq!(draining.state, DomainState::Draining);
        let artifacts = CloseArtifacts {
            inventory: Some(object_index(&inventory_objects, 20)),
            retention_batches: vec![retain_batch.index.clone()],
            retained_union: Some(object_index(std::slice::from_ref(&kept), 22)),
            control_evidence: Some(object_index(std::slice::from_ref(&control_object), 23)),
        };
        close_domain_commit(
            &store,
            &keys,
            &CloseCommitRequest {
                domain_id,
                expected_owner_generation: 7,
                close_generation: 1,
                final_inventory_seq: inventory_objects.len() as u64,
                final_retention_seq: 1,
                artifacts: artifacts.clone(),
                certificate_ref: None,
                owner_terminal_proof_hash: [25; 32],
                attempts_resolved: true,
                operations_drained: true,
                closed_at_ns: 1,
            },
        )
        .await
        .unwrap();
        let plan = plan_private_cleanup(&store, &keys, &domain_id, &artifacts)
            .await
            .unwrap();
        assert_eq!(plan.candidate_objects, vec![doomed]);
        assert_eq!(plan.total_batches, 1);

        let deleter = FakeDeleter::default();
        let request = apply_request(plan.clone(), [31; 16], 0, 30);
        let applied = apply_private_cleanup(&store, &keys, &deleter, &request)
            .await
            .unwrap();
        assert_eq!(applied.status, CleanupApplyStatus::Complete);
        assert_eq!(applied.domain_state, DomainState::Cleaned);
        assert_eq!(applied.deleted_objects, 1);
        assert_eq!(
            deleter.deleted.lock().unwrap().as_slice(),
            ["private/doomed"]
        );
        let retried = apply_private_cleanup(&store, &keys, &deleter, &request)
            .await
            .unwrap();
        assert_eq!(retried.status, CleanupApplyStatus::AlreadyComplete);
    }

    #[test]
    fn cleanup_batches_are_bounded() {
        assert_eq!(CleanupPlan::batch_count(0), 0);
        assert_eq!(CleanupPlan::batch_count(129), 2);
    }

    #[tokio::test]
    async fn cleanup_reuses_operation_across_batches_and_cleans_after_last_batch() {
        let volume = [41; 16];
        let domain_id = [42; 16];
        let keys = Keys::new(&volume);
        let store = MemoryControlStore::new();
        let objects = (1..=129)
            .map(|id| object(id, &format!("private/object-{id}")))
            .collect::<Vec<_>>();
        let artifacts = seed_closed_domain(&store, &keys, domain_id, &objects).await;
        let plan = plan_private_cleanup(&store, &keys, &domain_id, &artifacts)
            .await
            .unwrap();
        assert_eq!(plan.total_batches, 2);
        let deleter = FakeDeleter::default();
        let first = apply_private_cleanup(
            &store,
            &keys,
            &deleter,
            &apply_request(plan.clone(), [43; 16], 0, 226),
        )
        .await
        .unwrap();
        assert_eq!(first.domain_state, DomainState::Cleaning);
        assert_eq!(first.deleted_objects, 128);
        let second = apply_private_cleanup(
            &store,
            &keys,
            &deleter,
            &apply_request(plan, [43; 16], 1, 227),
        )
        .await
        .unwrap();
        assert_eq!(second.domain_state, DomainState::Cleaned);
        assert_eq!(second.deleted_objects, 1);
        assert_eq!(deleter.deleted.lock().unwrap().len(), 129);
    }

    #[tokio::test]
    async fn blocked_delete_stays_pending_and_same_operation_retries() {
        let volume = [51; 16];
        let domain_id = [52; 16];
        let keys = Keys::new(&volume);
        let store = MemoryControlStore::new();
        let doomed = object(53, "private/retry");
        let artifacts =
            seed_closed_domain(&store, &keys, domain_id, std::slice::from_ref(&doomed)).await;
        let plan = plan_private_cleanup(&store, &keys, &domain_id, &artifacts)
            .await
            .unwrap();
        let request = apply_request(plan, [54; 16], 0, 228);
        let deleter = FakeDeleter::failing_once("private/retry");
        let blocked = apply_private_cleanup(&store, &keys, &deleter, &request)
            .await
            .unwrap();
        assert_eq!(blocked.status, CleanupApplyStatus::Blocked);
        assert_eq!(blocked.domain_state, DomainState::Cleaning);
        let pending = match ControlRecord::decode(
            &store
                .get(&keys.object(&domain_id, &doomed.object_id))
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap()
        {
            ControlRecord::ObjectRegistration(registration) => registration,
            _ => panic!("expected registration"),
        };
        assert_eq!(pending.state, RegistrationState::DeletePending);

        let retried = apply_private_cleanup(&store, &keys, &deleter, &request)
            .await
            .unwrap();
        assert_eq!(retried.status, CleanupApplyStatus::Complete);
        assert_eq!(retried.domain_state, DomainState::Cleaned);
        assert_eq!(
            deleter.deleted.lock().unwrap().as_slice(),
            ["private/retry"]
        );
    }

    #[tokio::test]
    async fn unknown_attempt_quarantines_domain_and_prevents_cleanup() {
        let volume = [61; 16];
        let domain_id = [62; 16];
        let keys = Keys::new(&volume);
        let store = MemoryControlStore::new();
        let unknown = object(63, "private/unknown");
        let mut seeded = domain(volume, domain_id);
        seeded.inventory_seq = 1;
        put(
            &store,
            keys.domain(&domain_id),
            ControlRecord::OwnershipDomain(seeded).encode(),
        )
        .await;
        put_registration(
            &store,
            &keys,
            domain_id,
            1,
            &unknown,
            RegistrationState::Unknown,
        )
        .await;
        close_domain_start(
            &store,
            &keys,
            &CloseStartRequest {
                domain_id,
                expected_owner_generation: 7,
                expected_entity_version: 1,
                expected_inventory_seq: 1,
                expected_retention_seq: 0,
                accepted_ticket_end: 1,
            },
        )
        .await
        .unwrap();
        let result = close_domain_commit(
            &store,
            &keys,
            &CloseCommitRequest {
                domain_id,
                expected_owner_generation: 7,
                close_generation: 1,
                final_inventory_seq: 1,
                final_retention_seq: 0,
                artifacts: CloseArtifacts {
                    inventory: Some(object_index(std::slice::from_ref(&unknown), 64)),
                    retention_batches: Vec::new(),
                    retained_union: None,
                    control_evidence: None,
                },
                certificate_ref: None,
                owner_terminal_proof_hash: [66; 32],
                attempts_resolved: true,
                operations_drained: true,
                closed_at_ns: 1,
            },
        )
        .await;
        assert!(matches!(result, Err(LifecycleError::Durability(_))));
        assert_eq!(
            read_domain(&store, &keys, &domain_id).await.state,
            DomainState::Quarantined
        );
        assert!(
            store
                .get(&keys.close_certificate(&domain_id))
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn lease_expiry_or_unresolved_proof_is_not_delete_authority() {
        let volume = [67; 16];
        let domain_id = [68; 16];
        let keys = Keys::new(&volume);
        let store = MemoryControlStore::new();
        let item = object(69, "private/lease-is-not-proof");
        let mut seeded = domain(volume, domain_id);
        seeded.inventory_seq = 1;
        put(
            &store,
            keys.domain(&domain_id),
            ControlRecord::OwnershipDomain(seeded).encode(),
        )
        .await;
        put_registration(
            &store,
            &keys,
            domain_id,
            1,
            &item,
            RegistrationState::Verified,
        )
        .await;
        close_domain_start(
            &store,
            &keys,
            &CloseStartRequest {
                domain_id,
                expected_owner_generation: 7,
                expected_entity_version: 1,
                expected_inventory_seq: 1,
                expected_retention_seq: 0,
                accepted_ticket_end: 1,
            },
        )
        .await
        .unwrap();
        let result = close_domain_commit(
            &store,
            &keys,
            &CloseCommitRequest {
                domain_id,
                expected_owner_generation: 7,
                close_generation: 1,
                final_inventory_seq: 1,
                final_retention_seq: 0,
                artifacts: CloseArtifacts {
                    inventory: Some(object_index(std::slice::from_ref(&item), 70)),
                    retention_batches: Vec::new(),
                    retained_union: None,
                    control_evidence: None,
                },
                certificate_ref: None,
                owner_terminal_proof_hash: [72; 32],
                attempts_resolved: false,
                operations_drained: true,
                closed_at_ns: 1,
            },
        )
        .await;
        assert!(matches!(result, Err(LifecycleError::Durability(_))));
        assert_eq!(
            read_domain(&store, &keys, &domain_id).await.state,
            DomainState::Quarantined
        );
    }

    #[tokio::test]
    async fn multiple_retention_batches_form_exact_union_and_survive_cleaned() {
        let volume = [73; 16];
        let domain_id = [74; 16];
        let keys = Keys::new(&volume);
        let store = MemoryControlStore::new();
        let kept_a = object(75, "private/kept-a");
        let kept_b = object(76, "private/kept-b");
        let doomed = object(77, "private/doomed-after-two-seals");
        let batch_a = build_retain_batch(
            domain_id,
            std::slice::from_ref(&kept_a),
            [78; 16],
            b"control/retain-78.brfin".to_vec(),
        )
        .unwrap();
        let batch_b = build_retain_batch(
            domain_id,
            std::slice::from_ref(&kept_b),
            [79; 16],
            b"control/retain-79.brfin".to_vec(),
        )
        .unwrap();
        let control_a = batch_a.index.root.object.clone();
        let control_b = batch_b.index.root.object.clone();
        let inventory = vec![
            kept_a.clone(),
            kept_b.clone(),
            doomed.clone(),
            control_a.clone(),
            control_b.clone(),
        ];
        let mut seeded = domain(volume, domain_id);
        seeded.inventory_seq = inventory.len() as u64;
        seeded.retention_seq = 2;
        put(
            &store,
            keys.domain(&domain_id),
            ControlRecord::OwnershipDomain(seeded).encode(),
        )
        .await;
        for (index, item) in inventory.iter().enumerate() {
            put_registration(
                &store,
                &keys,
                domain_id,
                index as u64 + 1,
                item,
                RegistrationState::Verified,
            )
            .await;
        }
        put(
            &store,
            keys.retention_receipt(&domain_id, 1),
            ControlRecord::RetentionReceipt(batch_a.receipt(1, [80; 16], [81; 32])).encode(),
        )
        .await;
        put(
            &store,
            keys.retention_receipt(&domain_id, 2),
            ControlRecord::RetentionReceipt(batch_b.receipt(2, [82; 16], [83; 32])).encode(),
        )
        .await;
        close_domain_start(
            &store,
            &keys,
            &CloseStartRequest {
                domain_id,
                expected_owner_generation: 7,
                expected_entity_version: 1,
                expected_inventory_seq: inventory.len() as u64,
                expected_retention_seq: 2,
                accepted_ticket_end: 1,
            },
        )
        .await
        .unwrap();
        let artifacts = CloseArtifacts {
            inventory: Some(object_index(&inventory, 84)),
            retention_batches: vec![batch_a.index, batch_b.index],
            retained_union: Some(object_index(&[kept_a.clone(), kept_b.clone()], 85)),
            control_evidence: Some(object_index(&[control_a.clone(), control_b.clone()], 86)),
        };
        close_domain_commit(
            &store,
            &keys,
            &CloseCommitRequest {
                domain_id,
                expected_owner_generation: 7,
                close_generation: 1,
                final_inventory_seq: inventory.len() as u64,
                final_retention_seq: 2,
                artifacts: artifacts.clone(),
                certificate_ref: None,
                owner_terminal_proof_hash: [88; 32],
                attempts_resolved: true,
                operations_drained: true,
                closed_at_ns: 1,
            },
        )
        .await
        .unwrap();
        let plan = plan_private_cleanup(&store, &keys, &domain_id, &artifacts)
            .await
            .unwrap();
        assert_eq!(plan.candidate_objects, vec![doomed.clone()]);
        let backend = ReadableDeleter::new(
            inventory
                .iter()
                .map(|object| String::from_utf8(object.key.clone()).unwrap()),
        );
        let result = apply_private_cleanup(
            &store,
            &keys,
            &backend,
            &apply_request(plan, [89; 16], 0, 90),
        )
        .await
        .unwrap();
        assert_eq!(result.domain_state, DomainState::Cleaned);
        assert!(backend.is_readable("private/kept-a"));
        assert!(backend.is_readable("private/kept-b"));
        assert!(backend.is_readable("control/retain-78.brfin"));
        assert!(backend.is_readable("control/retain-79.brfin"));
        assert!(!backend.is_readable("private/doomed-after-two-seals"));
    }

    #[tokio::test]
    async fn missing_or_rollback_evidence_enters_retain_all() {
        let volume = [91; 16];
        let domain_id = [92; 16];
        let keys = Keys::new(&volume);
        let store = MemoryControlStore::new();
        let doomed = object(93, "private/retain-all");
        let artifacts =
            seed_closed_domain(&store, &keys, domain_id, std::slice::from_ref(&doomed)).await;
        let mut missing = artifacts.clone();
        missing.inventory = None;
        assert!(matches!(
            plan_private_cleanup(&store, &keys, &domain_id, &missing).await,
            Err(LifecycleError::Durability(_))
        ));

        let plan = plan_private_cleanup(&store, &keys, &domain_id, &artifacts)
            .await
            .unwrap();
        let deleter = FakeDeleter::default();
        let mut request = apply_request(plan, [94; 16], 0, 95);
        request.capabilities.authority = AuthorityHealth::RecoveryRetainAll;
        assert!(matches!(
            apply_private_cleanup(&store, &keys, &deleter, &request).await,
            Err(LifecycleError::Durability(_))
        ));
        assert!(deleter.deleted.lock().unwrap().is_empty());
        assert_eq!(
            read_domain(&store, &keys, &domain_id).await.state,
            DomainState::Closed
        );
    }

    #[tokio::test]
    async fn versioned_or_locked_namespace_is_rejected_before_delete() {
        let volume = [96; 16];
        let domain_id = [97; 16];
        let keys = Keys::new(&volume);
        let store = MemoryControlStore::new();
        let doomed = object(98, "private/capability-guard");
        let artifacts =
            seed_closed_domain(&store, &keys, domain_id, std::slice::from_ref(&doomed)).await;
        let plan = plan_private_cleanup(&store, &keys, &domain_id, &artifacts)
            .await
            .unwrap();
        let deleter = FakeDeleter::default();

        for capabilities in [
            CleanupCapabilities {
                namespace_versioning: NamespaceVersioning::Enabled,
                ..CleanupCapabilities::verified_unversioned()
            },
            CleanupCapabilities {
                object_lock_enabled: true,
                ..CleanupCapabilities::verified_unversioned()
            },
            CleanupCapabilities {
                delete_supported: false,
                ..CleanupCapabilities::verified_unversioned()
            },
        ] {
            let mut request = apply_request(plan.clone(), [99; 16], 0, 100);
            request.capabilities = capabilities;
            assert!(matches!(
                apply_private_cleanup(&store, &keys, &deleter, &request).await,
                Err(LifecycleError::Cleanup(_))
            ));
        }
        assert!(deleter.deleted.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn two_cleaners_cannot_own_the_same_deleting_batch() {
        let volume = [101; 16];
        let domain_id = [102; 16];
        let keys = Keys::new(&volume);
        let store = Arc::new(MemoryControlStore::new());
        let doomed = object(103, "private/raced");
        let artifacts =
            seed_closed_domain(&store, &keys, domain_id, std::slice::from_ref(&doomed)).await;
        let plan = plan_private_cleanup(store.as_ref(), &keys, &domain_id, &artifacts)
            .await
            .unwrap();
        let request = apply_request(plan, [104; 16], 0, 105);
        let backend = Arc::new(BlockingDeleter {
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
            deleted: Mutex::new(Vec::new()),
        });
        let first_store = Arc::clone(&store);
        let first_keys = keys.clone();
        let first_request = request.clone();
        let first_backend = Arc::clone(&backend);
        let first = tokio::spawn(async move {
            apply_private_cleanup(
                first_store.as_ref(),
                &first_keys,
                first_backend.as_ref(),
                &first_request,
            )
            .await
        });
        backend.entered.acquire().await.unwrap().forget();
        let second = apply_private_cleanup(store.as_ref(), &keys, backend.as_ref(), &request).await;
        assert!(matches!(second, Err(LifecycleError::Conflict(_))));
        backend.release.add_permits(1);
        assert_eq!(
            first.await.unwrap().unwrap().status,
            CleanupApplyStatus::Complete
        );
        assert_eq!(
            backend.deleted.lock().unwrap().as_slice(),
            ["private/raced"]
        );
    }

    #[tokio::test]
    async fn replan_after_partial_delete_preserves_digest_and_skips_deleted_rows() {
        let volume = [106; 16];
        let domain_id = [107; 16];
        let keys = Keys::new(&volume);
        let store = MemoryControlStore::new();
        let first = object(108, "private/partial-first");
        let second = object(109, "private/partial-second");
        let artifacts =
            seed_closed_domain(&store, &keys, domain_id, &[first.clone(), second.clone()]).await;
        let plan = plan_private_cleanup(&store, &keys, &domain_id, &artifacts)
            .await
            .unwrap();
        let request = apply_request(plan.clone(), [110; 16], 0, 111);
        let deleter = FakeDeleter::failing_once("private/partial-second");
        let blocked = apply_private_cleanup(&store, &keys, &deleter, &request)
            .await
            .unwrap();
        assert_eq!(blocked.status, CleanupApplyStatus::Blocked);
        assert_eq!(blocked.deleted_objects, 1);

        let reconstructed = plan_private_cleanup(&store, &keys, &domain_id, &artifacts)
            .await
            .unwrap();
        assert_eq!(reconstructed.plan_digest, plan.plan_digest);
        assert_eq!(reconstructed.candidate_objects, plan.candidate_objects);
        let completed = apply_private_cleanup(
            &store,
            &keys,
            &deleter,
            &apply_request(reconstructed, [110; 16], 0, 111),
        )
        .await
        .unwrap();
        assert_eq!(completed.status, CleanupApplyStatus::Complete);
        assert_eq!(completed.deleted_objects, 1);
        assert_eq!(
            deleter.deleted.lock().unwrap().as_slice(),
            ["private/partial-first", "private/partial-second"]
        );
    }
}
