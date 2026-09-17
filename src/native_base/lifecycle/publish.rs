//! Atomic PublishedRevision + RetainBatch + P1 KV base + head switch.

use std::collections::BTreeSet;

use sha2::{Digest, Sha256};

use crate::native_base::wire::bnct::{
    ControlRecord, DomainState, Id16, KvBaseRetention, NativePublicationJournal,
    NativeWorkspaceHead, ObjectRegistration, PublicationPhase, PublicationResult,
    PublishedRevision, RegistrationState, RetentionPolicy, SnapshotRef, WorkspaceHeadState,
};
use crate::native_base::wire::refs::{Hash32, ObjectRef};
use crate::native_base::wire::uvarint::Writer;
use crate::native_base::write::domain::{decode_domain, decode_registration};
use crate::native_base::write::keys::Keys;
use crate::native_base::write::records::HeadState;
use crate::native_base::write::store::{ControlStore, Txn};

use super::index::{BuiltObjectIndex, open_object_index};
use super::manifest::{BuiltManifest, open_manifest};
use super::retention::RetainBatchArtifact;
use super::seal::{decode_journal, decode_workspace_view, next_head_ref};
use super::{LifecycleError, LifecycleResult, map_conflict};

#[derive(Debug, Clone)]
pub struct DomainRetentionInput {
    pub expected_owner_generation: u64,
    pub expected_retention_seq: u64,
    pub batch: RetainBatchArtifact,
}

#[derive(Debug, Clone)]
pub struct PublishedDependency {
    pub object: ObjectRef,
    pub source_storage_view_id: Hash32,
}

#[derive(Debug, Clone)]
pub struct PublicationRequest {
    pub operation_id: Id16,
    pub publication_id: Id16,
    pub workspace_id: Id16,
    pub expected_head: HeadState,
    pub expected_view: NativeWorkspaceHead,
    pub new_head_id: Id16,
    pub manifest: BuiltManifest,
    pub physical_inventory: BuiltObjectIndex,
    pub publication_evidence: crate::native_base::wire::refs::RootRef,
    pub domains: Vec<DomainRetentionInput>,
    pub source_revisions: Vec<PublishedRevision>,
    pub published_dependencies: Vec<PublishedDependency>,
    pub kv_base: KvBaseRetention,
    pub retained_at_ns: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublicationOutcome {
    Published(PublicationResult),
    AlreadyPublished(PublicationResult),
}

pub fn decode_published_revision(bytes: &[u8]) -> LifecycleResult<PublishedRevision> {
    match ControlRecord::decode(bytes)? {
        ControlRecord::PublishedRevision(record)
            if record.storage_view_id == record.manifest.full_hash =>
        {
            Ok(record)
        }
        ControlRecord::PublishedRevision(_) => Err(LifecycleError::Record(
            "PublishedRevision StorageViewId is not the manifest full hash".into(),
        )),
        other => Err(LifecycleError::Record(format!(
            "expected PublishedRevision, got kind {}",
            other.kind().as_u16()
        ))),
    }
}

pub fn decode_kv_base_retention(bytes: &[u8]) -> LifecycleResult<KvBaseRetention> {
    match ControlRecord::decode(bytes)? {
        ControlRecord::KvBaseRetention(record) => Ok(record),
        other => Err(LifecycleError::Record(format!(
            "expected KvBaseRetention, got kind {}",
            other.kind().as_u16()
        ))),
    }
}

fn encode_object_ref(w: &mut Writer, object: &ObjectRef) {
    object.encode_into(w);
}

/// Canonical request digest stored beside the journal.  It includes every
/// value whose change would alter publication effects, so reusing an
/// OperationId with a different RetainBatch or head fails permanently.
pub fn publication_payload_digest(request: &PublicationRequest) -> Hash32 {
    let mut w = Writer::new();
    w.put(b"BrewFS.PublishCandidate.v1\0");
    w.put(&request.operation_id);
    w.put(&request.publication_id);
    w.put(&request.workspace_id);
    w.bytes(&request.expected_head.encode());
    w.bytes(&ControlRecord::NativeWorkspaceHead(request.expected_view.clone()).encode());
    w.put(&request.new_head_id);
    encode_object_ref(&mut w, &request.manifest.object);
    request.physical_inventory.root.encode_into(&mut w);
    request.publication_evidence.encode_into(&mut w);
    w.uvarint(request.domains.len() as u64);
    for input in &request.domains {
        w.put(&input.batch.domain_id);
        w.u64(input.expected_owner_generation);
        w.u64(input.expected_retention_seq);
        input.batch.index.root.encode_into(&mut w);
        w.put(&input.batch.verified_subset_digest);
    }
    w.uvarint(request.source_revisions.len() as u64);
    for source in &request.source_revisions {
        w.bytes(&ControlRecord::PublishedRevision(source.clone()).encode());
    }
    w.uvarint(request.published_dependencies.len() as u64);
    for dependency in &request.published_dependencies {
        encode_object_ref(&mut w, &dependency.object);
        w.put(&dependency.source_storage_view_id);
    }
    w.bytes(&ControlRecord::KvBaseRetention(request.kv_base.clone()).encode());
    w.i64(request.retained_at_ns);
    Sha256::digest(w.as_slice()).into()
}

async fn require_verified_registration(
    store: &dyn ControlStore,
    keys: &Keys,
    domain_id: &Id16,
    object: &ObjectRef,
) -> LifecycleResult<ObjectRegistration> {
    let bytes = store
        .get(&keys.object(domain_id, &object.object_id))
        .await?
        .ok_or_else(|| {
            LifecycleError::Retention(format!(
                "object {:02x?} is not registered in its origin domain",
                object.object_id
            ))
        })?;
    let registration =
        decode_registration(&bytes).map_err(|error| LifecycleError::Record(error.to_string()))?;
    if registration.domain_id != *domain_id
        || registration.object_ref != *object
        || registration.state != RegistrationState::Verified
    {
        return Err(LifecycleError::Retention(format!(
            "object {:02x?} is not a verified exact registration in its origin domain",
            object.object_id
        )));
    }
    Ok(registration)
}

async fn validate_domain_batch(
    store: &dyn ControlStore,
    keys: &Keys,
    input: &DomainRetentionInput,
) -> LifecycleResult<()> {
    let domain_bytes = store
        .get(&keys.domain(&input.batch.domain_id))
        .await?
        .ok_or_else(|| LifecycleError::Retention("origin domain missing".into()))?;
    let domain =
        decode_domain(&domain_bytes).map_err(|error| LifecycleError::Record(error.to_string()))?;
    if domain.state != DomainState::Active
        || domain.owner_generation != input.expected_owner_generation
        || domain.retention_seq != input.expected_retention_seq
    {
        return Err(LifecycleError::Conflict(
            "origin domain changed before RetainBatch verification".into(),
        ));
    }
    let decoded = open_object_index(&input.batch.index.root, &input.batch.index.bytes)?;
    if decoded != input.batch.index.entries {
        return Err(LifecycleError::Retention(
            "RetainBatch decoded entries differ from verified plan".into(),
        ));
    }
    for object in std::iter::once(&input.batch.index.root.object).chain(decoded.iter()) {
        require_verified_registration(store, keys, &input.batch.domain_id, object).await?;
        let binding = store
            .get(&keys.registry(&domain.namespace_id, &object.key))
            .await?
            .ok_or_else(|| LifecycleError::Retention("object registry binding missing".into()))?;
        if binding.as_slice() != object.object_id.as_slice() {
            return Err(LifecycleError::Retention(
                "object registry binding uses another ObjectId".into(),
            ));
        }
    }
    Ok(())
}

fn publication_result_from_journal(
    journal: NativePublicationJournal,
) -> LifecycleResult<PublicationResult> {
    journal.committed_result.ok_or_else(|| {
        LifecycleError::Record("committed publication journal has no fixed result".into())
    })
}

pub async fn publish_candidate(
    store: &dyn ControlStore,
    keys: &Keys,
    request: &PublicationRequest,
) -> LifecycleResult<PublicationOutcome> {
    let payload_digest = publication_payload_digest(request);
    let operation_key = keys.publication_operation(&request.operation_id);
    if let Some(existing) = store.get(&operation_key).await? {
        if existing.as_slice() != payload_digest.as_slice() {
            return Err(LifecycleError::OperationIdMismatch(
                "publication OperationId was used with another payload".into(),
            ));
        }
        let journal_bytes = store
            .get(&keys.publication_journal(&request.operation_id))
            .await?
            .ok_or_else(|| LifecycleError::Record("operation row without journal".into()))?;
        let journal = decode_journal(&journal_bytes)?;
        if !matches!(
            journal.phase,
            PublicationPhase::PublishedRetained | PublicationPhase::Completed
        ) {
            return Err(LifecycleError::Record(
                "operation row exists before PUBLISHED_RETAINED".into(),
            ));
        }
        return Ok(PublicationOutcome::AlreadyPublished(
            publication_result_from_journal(journal)?,
        ));
    }

    if request.domains.is_empty() || request.domains.len() > 2 {
        return Err(LifecycleError::Retention(
            "publication must own one or two origin domains".into(),
        ));
    }
    if request.new_head_id == [0u8; 16] || request.publication_id == [0u8; 16] {
        return Err(LifecycleError::InvalidState(
            "publication and replacement-head ids must be non-zero".into(),
        ));
    }
    let unique_domains: BTreeSet<_> = request
        .domains
        .iter()
        .map(|input| input.batch.domain_id)
        .collect();
    if unique_domains.len() != request.domains.len() {
        return Err(LifecycleError::Retention(
            "publication repeats an origin domain".into(),
        ));
    }
    let opened = open_manifest(&request.manifest.object, &request.manifest.bytes)?;
    if opened != request.manifest || opened.manifest.volume_id != keys.volume_id() {
        return Err(LifecycleError::Record(
            "candidate manifest is not the verified manifest for this volume".into(),
        ));
    }
    let candidate = SnapshotRef {
        volume_id: opened.manifest.volume_id,
        logical_revision: opened.manifest.logical_revision,
        manifest: opened.object.clone(),
    };
    if request.physical_inventory.root != opened.manifest.physical_inventory {
        return Err(LifecycleError::Retention(
            "physical inventory artifact does not match the manifest root".into(),
        ));
    }
    let physical_entries = open_object_index(
        &request.physical_inventory.root,
        &request.physical_inventory.bytes,
    )?;
    if physical_entries != request.physical_inventory.entries {
        return Err(LifecycleError::Retention(
            "physical inventory decoded entries differ from the verified artifact".into(),
        ));
    }
    if physical_entries
        .iter()
        .any(|object| object.object_id == opened.object.object_id)
    {
        return Err(LifecycleError::Retention(
            "physical inventory must exclude the manifest itself".into(),
        ));
    }
    if !physical_entries
        .iter()
        .any(|object| object == &opened.manifest.data_seal.object)
    {
        return Err(LifecycleError::Retention(
            "physical inventory does not contain the manifest's Data Seal container".into(),
        ));
    }
    if request.kv_base.volume_id != candidate.volume_id
        || request.kv_base.logical_revision != candidate.logical_revision
        || request.kv_base.layer_id != opened.manifest.namespace.layer_id
        || request.kv_base.sealed_version != opened.manifest.namespace.sealed_version
    {
        return Err(LifecycleError::Retention(
            "KvBaseRetention does not exactly bind the P1 manifest namespace".into(),
        ));
    }

    // Full batch/object verification occurs while all origin domains are
    // ACTIVE.  The final transaction rechecks those domain state/version
    // bytes, so no cleaner can enter between verification and commit.
    for input in &request.domains {
        validate_domain_batch(store, keys, input).await?;
    }

    // Reconstruct the exact candidate closure independently from the batch
    // planner: manifest + physical inventory container + inventory targets.
    // Every object must occur exactly once, either in a new domain batch or
    // as a dependency of an explicitly authorized PublishedRevision.
    let mut required = std::collections::BTreeMap::new();
    for object in std::iter::once(&opened.object)
        .chain(std::iter::once(&request.physical_inventory.root.object))
        .chain(physical_entries.iter())
    {
        if let Some(previous) = required.insert(object.object_id, object.clone())
            && previous != *object
        {
            return Err(LifecycleError::Retention(
                "candidate closure contains conflicting ObjectRefs".into(),
            ));
        }
    }
    let source_ids: BTreeSet<Hash32> = request
        .source_revisions
        .iter()
        .map(|source| source.storage_view_id)
        .collect();
    let mut supplied = std::collections::BTreeMap::new();
    for input in &request.domains {
        for object in &input.batch.index.entries {
            if supplied.insert(object.object_id, object.clone()).is_some() {
                return Err(LifecycleError::Retention(
                    "candidate object occurs in more than one RetainBatch".into(),
                ));
            }
        }
    }
    for dependency in &request.published_dependencies {
        if !source_ids.contains(&dependency.source_storage_view_id) {
            return Err(LifecycleError::Retention(
                "published dependency does not cite an authorized source revision".into(),
            ));
        }
        if supplied
            .insert(dependency.object.object_id, dependency.object.clone())
            .is_some()
        {
            return Err(LifecycleError::Retention(
                "candidate object is both newly retained and attributed to a source".into(),
            ));
        }
    }
    if supplied != required {
        return Err(LifecycleError::Retention(format!(
            "RetainBatch/source partition is not the exact candidate closure ({} supplied, {} required)",
            supplied.len(),
            required.len()
        )));
    }

    let journal_key = keys.publication_journal(&request.operation_id);
    let journal_bytes = store
        .get(&journal_key)
        .await?
        .ok_or_else(|| LifecycleError::InvalidState("publication journal not found".into()))?;
    let mut journal = decode_journal(&journal_bytes)?;
    if journal.phase != PublicationPhase::RetentionPrepared
        || journal.owner_generation != request.expected_head.writer_generation
        || journal.workspace_id != Some(request.workspace_id)
        || journal.expected_head.as_ref() != Some(&request.expected_head.head)
        || journal.expected_base.as_ref() != Some(&request.expected_view.base)
        || journal.fixed_commit_seq != Some(request.expected_head.head.commit_seq)
        || journal.verification_evidence.is_none()
        || journal.candidate.as_ref() != Some(&candidate)
        || journal.retention_evidence.as_ref() != Some(&request.publication_evidence)
    {
        return Err(LifecycleError::InvalidState(
            "journal is not the matching RETENTION_PREPARED certificate".into(),
        ));
    }

    let head_key = keys.head(&request.workspace_id);
    let head_bytes = store
        .get(&head_key)
        .await?
        .ok_or_else(|| LifecycleError::InvalidState("workspace head not found".into()))?;
    let current_head = HeadState::decode(&head_bytes)?;
    let view_key = keys.workspace_view(&request.workspace_id);
    let view_bytes = store
        .get(&view_key)
        .await?
        .ok_or_else(|| LifecycleError::InvalidState("workspace view not found".into()))?;
    let current_view = decode_workspace_view(&view_bytes)?;
    if current_head != request.expected_head
        || current_view != request.expected_view
        || current_view.state != WorkspaceHeadState::Sealing
        || current_view.head != current_head.head
        || current_view.write_domain_id != current_head.write_domain_id
        || current_view.writer_generation != current_head.writer_generation
    {
        return Err(LifecycleError::Conflict(
            "workspace head/view moved before publication".into(),
        ));
    }

    let new_head_ref = next_head_ref(&current_head.head, request.new_head_id)?;
    let result = PublicationResult {
        snapshot: candidate.clone(),
        new_head: Some(new_head_ref.clone()),
        publication_id: request.publication_id,
    };
    let publication = PublishedRevision {
        volume_id: candidate.volume_id,
        storage_view_id: candidate.manifest.full_hash,
        logical_revision: candidate.logical_revision,
        manifest: candidate.manifest.clone(),
        publication_id: request.publication_id,
        retained_at_ns: request.retained_at_ns,
        retention_policy: RetentionPolicy::Forever,
        evidence_root: request.publication_evidence.clone(),
    };

    let mut txn = Txn::new()
        .check_absent(operation_key.clone())
        .check_bytes(journal_key.clone(), journal_bytes)
        .check_bytes(head_key.clone(), head_bytes)
        .check_bytes(view_key.clone(), view_bytes)
        .check_absent(keys.published_revision(&publication.storage_view_id));

    for source in &request.source_revisions {
        if source.volume_id != keys.volume_id()
            || source.retention_policy != RetentionPolicy::Forever
        {
            return Err(LifecycleError::Retention(
                "source revision is not an authorized forever-retained version of this volume"
                    .into(),
            ));
        }
        let key = keys.published_revision(&source.storage_view_id);
        let bytes = store
            .get(&key)
            .await?
            .ok_or_else(|| LifecycleError::Retention("source PublishedRevision missing".into()))?;
        if decode_published_revision(&bytes)? != *source {
            return Err(LifecycleError::Retention(
                "source PublishedRevision identity changed".into(),
            ));
        }
        txn = txn.check_bytes(key, bytes);
    }

    for input in &request.domains {
        let domain_key = keys.domain(&input.batch.domain_id);
        let domain_bytes = store
            .get(&domain_key)
            .await?
            .ok_or_else(|| LifecycleError::Retention("origin domain missing".into()))?;
        let mut domain = decode_domain(&domain_bytes)
            .map_err(|error| LifecycleError::Record(error.to_string()))?;
        if domain.state != DomainState::Active
            || domain.owner_generation != input.expected_owner_generation
            || domain.retention_seq != input.expected_retention_seq
        {
            return Err(LifecycleError::Conflict(
                "origin domain closed or retention sequence changed".into(),
            ));
        }
        let next_seq = domain
            .retention_seq
            .checked_add(1)
            .ok_or_else(|| LifecycleError::LimitExceeded("retention sequence overflow".into()))?;
        let receipt =
            input
                .batch
                .receipt(next_seq, request.operation_id, publication.storage_view_id);
        domain.retention_seq = next_seq;
        domain.entity_version = domain.entity_version.checked_add(1).ok_or_else(|| {
            LifecycleError::LimitExceeded("domain entity version overflow".into())
        })?;
        let receipt_key = keys.retention_receipt(&input.batch.domain_id, next_seq);
        txn = txn
            .check_bytes(domain_key.clone(), domain_bytes)
            .check_absent(receipt_key.clone())
            .put(
                receipt_key,
                ControlRecord::RetentionReceipt(receipt).encode(),
            )
            .put(domain_key, ControlRecord::OwnershipDomain(domain).encode());
    }

    let kv_key = keys.kv_base_retention(&request.kv_base.layer_id, request.kv_base.sealed_version);
    let kv_bytes = ControlRecord::KvBaseRetention(request.kv_base.clone()).encode();
    match store.get(&kv_key).await? {
        None => {
            if request.kv_base.first_publication_id != request.publication_id {
                return Err(LifecycleError::Retention(
                    "new KvBaseRetention must name this publication as its first publication"
                        .into(),
                ));
            }
            txn = txn.check_absent(kv_key.clone()).put(kv_key, kv_bytes);
        }
        Some(existing) if existing == kv_bytes => {
            txn = txn.check_bytes(kv_key, existing);
        }
        Some(_) => {
            return Err(LifecycleError::Retention(
                "KvBaseRetention create-only key has different contents".into(),
            ));
        }
    }

    let new_head = HeadState {
        head: new_head_ref.clone(),
        writer_generation: current_head.writer_generation,
        write_domain_id: current_head.write_domain_id,
    };
    let mut new_view = current_view;
    new_view.head = new_head_ref;
    new_view.base = candidate;
    new_view.visible_delta_count = 0;
    // open_orphan_count/digest intentionally stay in the private replacement
    // head and are not part of the published manifest.
    new_view.state = WorkspaceHeadState::Running;
    new_view.entity_version = new_view
        .entity_version
        .checked_add(1)
        .ok_or_else(|| LifecycleError::LimitExceeded("workspace entity version overflow".into()))?;
    journal.phase = PublicationPhase::PublishedRetained;
    journal.committed_result = Some(result.clone());

    txn = txn
        .put(
            keys.published_revision(&publication.storage_view_id),
            ControlRecord::PublishedRevision(publication).encode(),
        )
        .put(head_key, new_head.encode())
        .put(
            view_key,
            ControlRecord::NativeWorkspaceHead(new_view).encode(),
        )
        .put(
            journal_key,
            ControlRecord::NativePublicationJournal(journal).encode(),
        )
        .put(operation_key, payload_digest.to_vec());

    store
        .run(txn)
        .await
        .map_err(|e| map_conflict(e, "publication lost an atomic guard"))?;
    Ok(PublicationOutcome::Published(result))
}

/// Local install completion is monotonic.  It never removes publication or
/// retention records.
pub async fn complete_publication(
    store: &dyn ControlStore,
    keys: &Keys,
    operation_id: &Id16,
) -> LifecycleResult<PublicationResult> {
    let key = keys.publication_journal(operation_id);
    let bytes = store
        .get(&key)
        .await?
        .ok_or_else(|| LifecycleError::InvalidState("publication journal not found".into()))?;
    let mut journal = decode_journal(&bytes)?;
    if journal.phase == PublicationPhase::Completed {
        return publication_result_from_journal(journal);
    }
    if journal.phase != PublicationPhase::PublishedRetained {
        return Err(LifecycleError::InvalidState(
            "only PUBLISHED_RETAINED can complete".into(),
        ));
    }
    let result = publication_result_from_journal(journal.clone())?;
    journal.phase = PublicationPhase::Completed;
    store
        .run(Txn::new().check_bytes(key.clone(), bytes).put(
            key,
            ControlRecord::NativePublicationJournal(journal).encode(),
        ))
        .await
        .map_err(|e| map_conflict(e, "publication completion raced"))?;
    Ok(result)
}

/// Abort is permitted only before the final transaction.  A late abort
/// returns the committed result and cannot undo permanent facts.
pub async fn abort_publication(
    store: &dyn ControlStore,
    keys: &Keys,
    operation_id: &Id16,
) -> LifecycleResult<Option<PublicationResult>> {
    let key = keys.publication_journal(operation_id);
    let bytes = store
        .get(&key)
        .await?
        .ok_or_else(|| LifecycleError::InvalidState("publication journal not found".into()))?;
    let mut journal = decode_journal(&bytes)?;
    if matches!(
        journal.phase,
        PublicationPhase::PublishedRetained | PublicationPhase::Completed
    ) {
        return Ok(Some(publication_result_from_journal(journal)?));
    }
    if journal.phase == PublicationPhase::Aborted {
        return Ok(None);
    }
    journal.phase = PublicationPhase::Aborted;
    let mut txn = Txn::new().check_bytes(key.clone(), bytes).put(
        key,
        ControlRecord::NativePublicationJournal(journal.clone()).encode(),
    );
    if let Some(workspace_id) = journal.workspace_id {
        let view_key = keys.workspace_view(&workspace_id);
        let view_bytes = store
            .get(&view_key)
            .await?
            .ok_or_else(|| LifecycleError::InvalidState("aborted workspace view missing".into()))?;
        let mut view = decode_workspace_view(&view_bytes)?;
        if view.writer_generation != journal.owner_generation
            || !matches!(
                view.state,
                WorkspaceHeadState::Freezing | WorkspaceHeadState::Sealing
            )
        {
            return Err(LifecycleError::Conflict(
                "workspace owner/view changed before abort could unfreeze it".into(),
            ));
        }
        view.state = WorkspaceHeadState::Running;
        view.entity_version = view.entity_version.checked_add(1).ok_or_else(|| {
            LifecycleError::LimitExceeded("workspace entity version overflow".into())
        })?;
        txn = txn
            .check_bytes(view_key.clone(), view_bytes)
            .put(view_key, ControlRecord::NativeWorkspaceHead(view).encode());
    }
    store
        .run(txn)
        .await
        .map_err(|e| map_conflict(e, "publication abort raced"))?;
    Ok(None)
}
