//! Persistent seal journal and bounded drain protocol (spec 09 / spec 18).

use std::collections::{BTreeMap, BTreeSet};

use sha2::{Digest, Sha256};

use crate::native_base::wire::bnct::{
    ControlRecord, DrainBatchState, HeadRef, Id16, NativeDrainBatch, NativePublicationJournal,
    NativeWorkspaceHead, PlanEntry, PublicationPhase, SnapshotRef, WorkspaceHeadState,
};
use crate::native_base::wire::refs::{Hash32, RootRef};
use crate::native_base::wire::uvarint::Writer;
use crate::native_base::write::keys::Keys;
use crate::native_base::write::records::HeadState;
use crate::native_base::write::store::{ControlStore, Txn};

use super::{LifecycleError, LifecycleResult, map_conflict};

pub const MAX_DRAIN_BATCH_ENTRIES: usize = 128;
pub const MAX_DRAIN_BATCH_BYTES: usize = 1024 * 1024;
pub const MAX_DRAIN_BATCHES_PER_SEAL: usize = 128;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SealStage {
    LocalFreezing,
    Prepare,
    Quiesced,
    DataDrained,
    CandidateVerified,
    RetentionPrepared,
    PublishedRetained,
    Completed,
    Aborted,
}

impl From<PublicationPhase> for SealStage {
    fn from(value: PublicationPhase) -> Self {
        match value {
            PublicationPhase::Prepare => SealStage::Prepare,
            PublicationPhase::Quiesced => SealStage::Quiesced,
            PublicationPhase::DataDrained => SealStage::DataDrained,
            PublicationPhase::CandidateVerified => SealStage::CandidateVerified,
            PublicationPhase::RetentionPrepared => SealStage::RetentionPrepared,
            PublicationPhase::PublishedRetained => SealStage::PublishedRetained,
            PublicationPhase::Completed => SealStage::Completed,
            PublicationPhase::Aborted => SealStage::Aborted,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryAction {
    ResumeQuiesce,
    ResumeDrain,
    RebuildCandidate,
    VerifyOrPublish,
    InstallPublishedResult,
    ReturnCompletedResult,
    ReturnAborted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteDurability {
    ExactReadback,
    VerifiedStrongServiceReceipt,
    /// Only valid for a candidate that introduces no new remote writes.
    NoopTestBarrier,
}

#[derive(Debug, Clone)]
pub struct DrainPlanBatch {
    pub batch_id: u64,
    pub plan: RootRef,
    pub entries: Vec<PlanEntry>,
}

pub fn decode_workspace_view(bytes: &[u8]) -> LifecycleResult<NativeWorkspaceHead> {
    match ControlRecord::decode(bytes)? {
        ControlRecord::NativeWorkspaceHead(view) => Ok(view),
        other => Err(LifecycleError::Record(format!(
            "expected NativeWorkspaceHead, got kind {}",
            other.kind().as_u16()
        ))),
    }
}

pub fn decode_journal(bytes: &[u8]) -> LifecycleResult<NativePublicationJournal> {
    match ControlRecord::decode(bytes)? {
        ControlRecord::NativePublicationJournal(journal) => Ok(journal),
        other => Err(LifecycleError::Record(format!(
            "expected NativePublicationJournal, got kind {}",
            other.kind().as_u16()
        ))),
    }
}

pub fn decode_drain_batch(bytes: &[u8]) -> LifecycleResult<NativeDrainBatch> {
    match ControlRecord::decode(bytes)? {
        ControlRecord::NativeDrainBatch(batch) => Ok(batch),
        other => Err(LifecycleError::Record(format!(
            "expected NativeDrainBatch, got kind {}",
            other.kind().as_u16()
        ))),
    }
}

fn encode_plan(entries: &[PlanEntry]) -> LifecycleResult<Vec<u8>> {
    if entries.len() > MAX_DRAIN_BATCH_ENTRIES {
        return Err(LifecycleError::LimitExceeded(format!(
            "drain batch has {} entries (maximum {MAX_DRAIN_BATCH_ENTRIES})",
            entries.len()
        )));
    }
    let mut w = Writer::new();
    w.uvarint(entries.len() as u64);
    for entry in entries {
        entry.encode_into(&mut w);
    }
    let bytes = w.into_bytes();
    if bytes.len() > MAX_DRAIN_BATCH_BYTES {
        return Err(LifecycleError::LimitExceeded(format!(
            "drain batch payload is {} bytes (maximum {MAX_DRAIN_BATCH_BYTES})",
            bytes.len()
        )));
    }
    Ok(bytes)
}

pub fn drain_plan_digest(entries: &[PlanEntry]) -> LifecycleResult<Hash32> {
    Ok(Sha256::digest(encode_plan(entries)?).into())
}

pub(crate) fn validate_plan(
    entries: &[PlanEntry],
    accepted_ticket_end: u64,
) -> LifecycleResult<Hash32> {
    let mut operations = BTreeSet::new();
    let mut inode_orders: BTreeMap<u64, u64> = BTreeMap::new();
    for entry in entries {
        if entry.admission_ticket == 0 || entry.admission_ticket > accepted_ticket_end {
            return Err(LifecycleError::InvalidState(format!(
                "ticket {} is outside frozen acceptance boundary {}",
                entry.admission_ticket, accepted_ticket_end
            )));
        }
        if entry.logical_len == 0 || !operations.insert(entry.operation_id) {
            return Err(LifecycleError::InvalidState(
                "drain entries need non-zero ranges and unique OperationIds".into(),
            ));
        }
        if let Some(previous) = inode_orders.insert(entry.inode, entry.mutation_order)
            && entry.mutation_order <= previous
        {
            return Err(LifecycleError::InvalidState(format!(
                "inode {} drain mutation order is not strictly increasing",
                entry.inode
            )));
        }
    }
    drain_plan_digest(entries)
}

/// Persist PREPARE after the local admission gate has frozen and fixed its
/// ticket boundary.  The compact PR04 head is checked but not reformatted.
#[allow(clippy::too_many_arguments)]
pub async fn begin_seal(
    store: &dyn ControlStore,
    keys: &Keys,
    operation_id: Id16,
    workspace_id: Id16,
    expected_head: &HeadState,
    expected_view: &NativeWorkspaceHead,
    owner_generation: u64,
    accepted_ticket_end: u64,
) -> LifecycleResult<NativePublicationJournal> {
    if expected_view.workspace_id != workspace_id
        || expected_view.head != expected_head.head
        || expected_view.writer_generation != expected_head.writer_generation
        || expected_view.write_domain_id != expected_head.write_domain_id
        || expected_view.state != WorkspaceHeadState::Running
        || owner_generation != expected_view.writer_generation
    {
        return Err(LifecycleError::InvalidState(
            "seal begin guard does not match the running workspace head".into(),
        ));
    }
    let journal = NativePublicationJournal {
        operation_id,
        workspace_id: Some(workspace_id),
        expected_head: Some(expected_head.head.clone()),
        expected_base: Some(expected_view.base.clone()),
        owner_generation,
        phase: PublicationPhase::Prepare,
        accepted_ticket_end,
        drain_plan: None,
        batch_count: 0,
        completed_count: 0,
        fixed_commit_seq: None,
        candidate: None,
        verification_evidence: None,
        retention_evidence: None,
        committed_result: None,
    };
    let mut freezing = expected_view.clone();
    freezing.state = WorkspaceHeadState::Freezing;
    freezing.entity_version = freezing
        .entity_version
        .checked_add(1)
        .ok_or_else(|| LifecycleError::LimitExceeded("workspace entity version overflow".into()))?;
    let journal_key = keys.publication_journal(&operation_id);
    let view_key = keys.workspace_view(&workspace_id);
    if let Some(existing) = store.get(&journal_key).await? {
        let recorded = decode_journal(&existing)?;
        let current_head = store.get(&keys.head(&workspace_id)).await?;
        let current_view = store.get(&view_key).await?;
        if recorded == journal
            && current_head.as_deref() == Some(expected_head.encode().as_slice())
            && current_view.as_deref()
                == Some(
                    ControlRecord::NativeWorkspaceHead(freezing.clone())
                        .encode()
                        .as_slice(),
                )
        {
            return Ok(recorded);
        }
        return Err(LifecycleError::OperationIdMismatch(
            "seal OperationId already names another begin request".into(),
        ));
    }
    store
        .run(
            Txn::new()
                .check_absent(journal_key.clone())
                .check_bytes(keys.head(&workspace_id), expected_head.encode())
                .check_bytes(
                    view_key.clone(),
                    ControlRecord::NativeWorkspaceHead(expected_view.clone()).encode(),
                )
                .put(
                    journal_key,
                    ControlRecord::NativePublicationJournal(journal.clone()).encode(),
                )
                .put(
                    view_key,
                    ControlRecord::NativeWorkspaceHead(freezing).encode(),
                ),
        )
        .await
        .map_err(|e| map_conflict(e, "seal begin raced with another head/journal change"))?;
    Ok(journal)
}

/// Register every bounded batch before any receives a DrainGuard, then move
/// PREPARE -> QUIESCED atomically with the batch records.
pub async fn quiesce_with_batches(
    store: &dyn ControlStore,
    keys: &Keys,
    operation_id: Id16,
    owner_generation: u64,
    drain_plan: Option<RootRef>,
    batches: &[DrainPlanBatch],
) -> LifecycleResult<NativePublicationJournal> {
    let journal_key = keys.publication_journal(&operation_id);
    let journal_bytes = store
        .get(&journal_key)
        .await?
        .ok_or_else(|| LifecycleError::InvalidState("publication journal not found".into()))?;
    let journal = decode_journal(&journal_bytes)?;
    if journal.owner_generation != owner_generation {
        return Err(LifecycleError::InvalidState(
            "only the journal owner can quiesce a seal".into(),
        ));
    }
    if journal.phase == PublicationPhase::Quiesced {
        if journal.drain_plan != drain_plan || journal.batch_count != batches.len() as u64 {
            return Err(LifecycleError::OperationIdMismatch(
                "quiesce replay changed its plan root or batch count".into(),
            ));
        }
        for batch in batches {
            let stored = store
                .get(&keys.drain_batch(&operation_id, batch.batch_id))
                .await?
                .ok_or_else(|| LifecycleError::Record("quiesced batch is missing".into()))?;
            let stored = decode_drain_batch(&stored)?;
            if stored.operation_id != operation_id
                || stored.owner_generation != owner_generation
                || stored.plan != batch.plan
                || stored.plan_digest != validate_plan(&batch.entries, journal.accepted_ticket_end)?
            {
                return Err(LifecycleError::OperationIdMismatch(
                    "quiesce replay changed a persisted batch".into(),
                ));
            }
        }
        return Ok(journal);
    }
    if journal.phase != PublicationPhase::Prepare {
        return Err(LifecycleError::InvalidState(
            "only PREPARE can transition to QUIESCED".into(),
        ));
    }
    if batches.is_empty() != drain_plan.is_none() {
        return Err(LifecycleError::InvalidState(
            "drain_plan root must be present exactly when batches exist".into(),
        ));
    }
    if batches.len() > MAX_DRAIN_BATCHES_PER_SEAL {
        return Err(LifecycleError::LimitExceeded(format!(
            "seal has {} drain batches (maximum {MAX_DRAIN_BATCHES_PER_SEAL})",
            batches.len()
        )));
    }
    let workspace_id = journal
        .workspace_id
        .ok_or_else(|| LifecycleError::InvalidState("online seal lacks workspace id".into()))?;
    let head_key = keys.head(&workspace_id);
    let head_bytes = store
        .get(&head_key)
        .await?
        .ok_or_else(|| LifecycleError::InvalidState("workspace head not found".into()))?;
    let head = HeadState::decode(&head_bytes)?;
    let view_key = keys.workspace_view(&workspace_id);
    let view_bytes = store
        .get(&view_key)
        .await?
        .ok_or_else(|| LifecycleError::InvalidState("workspace view not found".into()))?;
    let view = decode_workspace_view(&view_bytes)?;
    if journal.expected_head.as_ref() != Some(&head.head)
        || head.writer_generation != owner_generation
        || view.head != head.head
        || view.writer_generation != owner_generation
        || view.state != WorkspaceHeadState::Freezing
    {
        return Err(LifecycleError::Conflict(
            "workspace head/owner changed before QUIESCED".into(),
        ));
    }
    let mut ids = BTreeSet::new();
    let mut operations = BTreeSet::new();
    let mut inode_orders: BTreeMap<u64, u64> = BTreeMap::new();
    let mut previous_batch_id = None;
    let mut txn = Txn::new()
        .check_bytes(journal_key.clone(), journal_bytes)
        .check_bytes(head_key, head_bytes)
        .check_bytes(view_key, view_bytes);
    for batch in batches {
        if !ids.insert(batch.batch_id)
            || previous_batch_id.is_some_and(|previous| batch.batch_id <= previous)
        {
            return Err(LifecycleError::InvalidState(
                "drain batch ids must be unique and strictly increasing".into(),
            ));
        }
        previous_batch_id = Some(batch.batch_id);
        let plan_digest = validate_plan(&batch.entries, journal.accepted_ticket_end)?;
        for entry in &batch.entries {
            if !operations.insert(entry.operation_id) {
                return Err(LifecycleError::InvalidState(
                    "an OperationId occurs in more than one drain batch".into(),
                ));
            }
            if let Some(previous) = inode_orders.insert(entry.inode, entry.mutation_order)
                && entry.mutation_order <= previous
            {
                return Err(LifecycleError::InvalidState(format!(
                    "inode {} mutation order goes backwards across drain batches",
                    entry.inode
                )));
            }
        }
        let record = NativeDrainBatch {
            operation_id,
            batch_id: batch.batch_id,
            owner_generation,
            plan_digest,
            plan: batch.plan.clone(),
            state: DrainBatchState::Registered,
            result_digest: None,
        };
        let key = keys.drain_batch(&operation_id, batch.batch_id);
        txn = txn
            .check_absent(key.clone())
            .put(key, ControlRecord::NativeDrainBatch(record).encode());
    }
    let mut next = journal;
    next.phase = PublicationPhase::Quiesced;
    next.drain_plan = drain_plan;
    next.batch_count = batches.len() as u64;
    txn = txn.put(
        journal_key,
        ControlRecord::NativePublicationJournal(next.clone()).encode(),
    );
    store
        .run(txn)
        .await
        .map_err(|e| map_conflict(e, "drain-plan registration raced"))?;
    Ok(next)
}

/// Commit one registered batch.  The entries must reproduce the persisted
/// digest; a claimed low ticket that was not in the batch cannot pass.
#[allow(clippy::too_many_arguments)]
pub async fn commit_drain_batch(
    store: &dyn ControlStore,
    keys: &Keys,
    operation_id: Id16,
    batch_id: u64,
    owner_generation: u64,
    entries: &[PlanEntry],
    payload_durable: bool,
    result_digest: Hash32,
) -> LifecycleResult<NativeDrainBatch> {
    if !payload_durable {
        return Err(LifecycleError::Durability(
            "drain payload is unavailable/not fsync-durable".into(),
        ));
    }
    let batch_key = keys.drain_batch(&operation_id, batch_id);
    let batch_bytes = store
        .get(&batch_key)
        .await?
        .ok_or_else(|| LifecycleError::InvalidState("drain batch not registered".into()))?;
    let mut batch = decode_drain_batch(&batch_bytes)?;
    if batch.operation_id != operation_id
        || batch.owner_generation != owner_generation
        || batch.plan_digest != drain_plan_digest(entries)?
    {
        return Err(LifecycleError::InvalidState(
            "DrainGuard owner or persisted plan digest mismatch".into(),
        ));
    }
    if batch.state == DrainBatchState::Committed {
        if batch.result_digest == Some(result_digest) {
            return Ok(batch);
        }
        return Err(LifecycleError::OperationIdMismatch(
            "drain batch already committed with another result".into(),
        ));
    }
    if !matches!(
        batch.state,
        DrainBatchState::Registered | DrainBatchState::Uploading
    ) {
        return Err(LifecycleError::InvalidState(format!(
            "drain batch is {:?}, not committable",
            batch.state
        )));
    }
    let journal_key = keys.publication_journal(&operation_id);
    let journal_bytes = store
        .get(&journal_key)
        .await?
        .ok_or_else(|| LifecycleError::InvalidState("publication journal not found".into()))?;
    let mut journal = decode_journal(&journal_bytes)?;
    if journal.phase != PublicationPhase::Quiesced
        || journal.owner_generation != owner_generation
        || journal.completed_count >= journal.batch_count
    {
        return Err(LifecycleError::InvalidState(
            "journal does not permit this drain commit".into(),
        ));
    }
    let workspace_id = journal
        .workspace_id
        .ok_or_else(|| LifecycleError::InvalidState("online seal lacks workspace id".into()))?;
    let head_key = keys.head(&workspace_id);
    let head_bytes = store
        .get(&head_key)
        .await?
        .ok_or_else(|| LifecycleError::InvalidState("workspace head not found".into()))?;
    let head = HeadState::decode(&head_bytes)?;
    let expected = journal
        .expected_head
        .as_ref()
        .ok_or_else(|| LifecycleError::InvalidState("drain journal lacks expected head".into()))?;
    let view_key = keys.workspace_view(&workspace_id);
    let view_bytes = store
        .get(&view_key)
        .await?
        .ok_or_else(|| LifecycleError::InvalidState("workspace view not found".into()))?;
    let view = decode_workspace_view(&view_bytes)?;
    if head.head.head_id != expected.head_id
        || head.head.epoch != expected.epoch
        || head.writer_generation != owner_generation
        || view.head.head_id != expected.head_id
        || view.head.epoch != expected.epoch
        || view.writer_generation != owner_generation
        || view.state != WorkspaceHeadState::Freezing
    {
        return Err(LifecycleError::Conflict(
            "stale seal owner cannot commit a drain batch".into(),
        ));
    }
    batch.state = DrainBatchState::Committed;
    batch.result_digest = Some(result_digest);
    journal.completed_count += 1;
    store
        .run(
            Txn::new()
                .check_bytes(batch_key.clone(), batch_bytes)
                .check_bytes(journal_key.clone(), journal_bytes)
                .check_bytes(head_key, head_bytes)
                .check_bytes(view_key, view_bytes)
                .put(
                    batch_key,
                    ControlRecord::NativeDrainBatch(batch.clone()).encode(),
                )
                .put(
                    journal_key,
                    ControlRecord::NativePublicationJournal(journal).encode(),
                ),
        )
        .await
        .map_err(|e| map_conflict(e, "drain batch commit raced"))?;
    Ok(batch)
}

/// Verify every registered batch is committed and fix the actual
/// head_commit_seq used to construct the candidate.
pub async fn finish_data_drain(
    store: &dyn ControlStore,
    keys: &Keys,
    operation_id: Id16,
    owner_generation: u64,
    expected_head: &HeadState,
) -> LifecycleResult<NativePublicationJournal> {
    let journal_key = keys.publication_journal(&operation_id);
    let journal_bytes = store
        .get(&journal_key)
        .await?
        .ok_or_else(|| LifecycleError::InvalidState("publication journal not found".into()))?;
    let mut journal = decode_journal(&journal_bytes)?;
    if journal.phase == PublicationPhase::DataDrained
        && journal.owner_generation == owner_generation
        && journal.fixed_commit_seq == Some(expected_head.head.commit_seq)
    {
        let workspace_id = journal.workspace_id.ok_or_else(|| {
            LifecycleError::InvalidState("online DATA_DRAINED journal lacks workspace id".into())
        })?;
        if store.get(&keys.head(&workspace_id)).await?.as_deref()
            == Some(expected_head.encode().as_slice())
        {
            return Ok(journal);
        }
        return Err(LifecycleError::Conflict(
            "head changed after DATA_DRAINED".into(),
        ));
    }
    if journal.phase != PublicationPhase::Quiesced || journal.owner_generation != owner_generation {
        return Err(LifecycleError::InvalidState(
            "only the QUIESCED owner can finish drain".into(),
        ));
    }
    let rows = store
        .scan(&keys.drain_batches_prefix(&operation_id))
        .await?;
    if rows.len() as u64 != journal.batch_count
        || journal.completed_count != journal.batch_count
        || rows.iter().any(|(_, bytes)| {
            decode_drain_batch(bytes)
                .map(|batch| batch.state != DrainBatchState::Committed)
                .unwrap_or(true)
        })
    {
        return Err(LifecycleError::InvalidState(
            "not every persisted drain batch is committed".into(),
        ));
    }
    let workspace_id = journal
        .workspace_id
        .ok_or_else(|| LifecycleError::InvalidState("online seal lacks workspace id".into()))?;
    let head_key = keys.head(&workspace_id);
    let current_head = store
        .get(&head_key)
        .await?
        .ok_or_else(|| LifecycleError::InvalidState("workspace head not found".into()))?;
    if HeadState::decode(&current_head)? != *expected_head
        || expected_head.writer_generation != owner_generation
    {
        return Err(LifecycleError::Conflict(
            "head/owner changed while draining".into(),
        ));
    }
    let view_key = keys.workspace_view(&workspace_id);
    let view_bytes = store
        .get(&view_key)
        .await?
        .ok_or_else(|| LifecycleError::InvalidState("workspace view not found".into()))?;
    let mut view = decode_workspace_view(&view_bytes)?;
    if view.state != WorkspaceHeadState::Freezing || view.writer_generation != owner_generation {
        return Err(LifecycleError::InvalidState(
            "workspace is not frozen by this seal owner".into(),
        ));
    }
    journal.phase = PublicationPhase::DataDrained;
    journal.fixed_commit_seq = Some(expected_head.head.commit_seq);
    journal.expected_head = Some(expected_head.head.clone());
    view.state = WorkspaceHeadState::Sealing;
    view.head = expected_head.head.clone();
    view.entity_version = view
        .entity_version
        .checked_add(1)
        .ok_or_else(|| LifecycleError::LimitExceeded("workspace entity version overflow".into()))?;
    store
        .run(
            Txn::new()
                .check_bytes(journal_key.clone(), journal_bytes)
                .check_bytes(head_key, current_head)
                .check_bytes(view_key.clone(), view_bytes)
                .put(
                    journal_key,
                    ControlRecord::NativePublicationJournal(journal.clone()).encode(),
                )
                .put(view_key, ControlRecord::NativeWorkspaceHead(view).encode()),
        )
        .await
        .map_err(|e| map_conflict(e, "DATA_DRAINED transition raced"))?;
    Ok(journal)
}

async fn transition_journal(
    store: &dyn ControlStore,
    keys: &Keys,
    operation_id: Id16,
    owner_generation: u64,
    expected_phase: PublicationPhase,
    mutate: impl FnOnce(&mut NativePublicationJournal) -> LifecycleResult<()>,
) -> LifecycleResult<NativePublicationJournal> {
    let key = keys.publication_journal(&operation_id);
    let bytes = store
        .get(&key)
        .await?
        .ok_or_else(|| LifecycleError::InvalidState("publication journal not found".into()))?;
    let mut journal = decode_journal(&bytes)?;
    if journal.owner_generation != owner_generation || journal.phase != expected_phase {
        return Err(LifecycleError::InvalidState(format!(
            "journal is {:?} for generation {}, expected {:?}/{}",
            journal.phase, journal.owner_generation, expected_phase, owner_generation
        )));
    }
    let workspace_id = journal
        .workspace_id
        .ok_or_else(|| LifecycleError::InvalidState("online seal lacks workspace id".into()))?;
    let head_key = keys.head(&workspace_id);
    let head_bytes = store
        .get(&head_key)
        .await?
        .ok_or_else(|| LifecycleError::InvalidState("workspace head not found".into()))?;
    let head = HeadState::decode(&head_bytes)?;
    let view_key = keys.workspace_view(&workspace_id);
    let view_bytes = store
        .get(&view_key)
        .await?
        .ok_or_else(|| LifecycleError::InvalidState("workspace view not found".into()))?;
    let view = decode_workspace_view(&view_bytes)?;
    if journal.expected_head.as_ref() != Some(&head.head)
        || head.writer_generation != owner_generation
        || view.head != head.head
        || view.writer_generation != owner_generation
        || view.state != WorkspaceHeadState::Sealing
    {
        return Err(LifecycleError::Conflict(
            "workspace head/owner no longer matches the sealing journal".into(),
        ));
    }
    mutate(&mut journal)?;
    store
        .run(
            Txn::new()
                .check_bytes(key.clone(), bytes)
                .check_bytes(head_key, head_bytes)
                .check_bytes(view_key, view_bytes)
                .put(
                    key,
                    ControlRecord::NativePublicationJournal(journal.clone()).encode(),
                ),
        )
        .await
        .map_err(|e| map_conflict(e, "publication phase transition raced"))?;
    Ok(journal)
}

async fn ensure_sealing_owner(
    store: &dyn ControlStore,
    keys: &Keys,
    journal: &NativePublicationJournal,
) -> LifecycleResult<()> {
    let workspace_id = journal
        .workspace_id
        .ok_or_else(|| LifecycleError::InvalidState("online seal lacks workspace id".into()))?;
    let head_bytes = store
        .get(&keys.head(&workspace_id))
        .await?
        .ok_or_else(|| LifecycleError::InvalidState("workspace head not found".into()))?;
    let head = HeadState::decode(&head_bytes)?;
    let view_bytes = store
        .get(&keys.workspace_view(&workspace_id))
        .await?
        .ok_or_else(|| LifecycleError::InvalidState("workspace view not found".into()))?;
    let view = decode_workspace_view(&view_bytes)?;
    if journal.expected_head.as_ref() != Some(&head.head)
        || head.writer_generation != journal.owner_generation
        || view.head != head.head
        || view.writer_generation != journal.owner_generation
        || view.state != WorkspaceHeadState::Sealing
    {
        return Err(LifecycleError::Conflict(
            "workspace head/owner no longer matches the sealing journal".into(),
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn mark_candidate_verified(
    store: &dyn ControlStore,
    keys: &Keys,
    operation_id: Id16,
    owner_generation: u64,
    candidate: SnapshotRef,
    evidence: RootRef,
    durability: RemoteDurability,
    contains_new_remote_writes: bool,
) -> LifecycleResult<NativePublicationJournal> {
    if contains_new_remote_writes && durability == RemoteDurability::NoopTestBarrier {
        return Err(LifecycleError::Durability(
            "Noop remote barrier cannot certify a write publication".into(),
        ));
    }
    let existing_bytes = store
        .get(&keys.publication_journal(&operation_id))
        .await?
        .ok_or_else(|| LifecycleError::InvalidState("publication journal not found".into()))?;
    let existing = decode_journal(&existing_bytes)?;
    ensure_sealing_owner(store, keys, &existing).await?;
    if existing.phase == PublicationPhase::CandidateVerified {
        if existing.owner_generation == owner_generation
            && existing.candidate.as_ref() == Some(&candidate)
            && existing.verification_evidence.as_ref() == Some(&evidence)
        {
            return Ok(existing);
        }
        return Err(LifecycleError::OperationIdMismatch(
            "candidate verification replay changed its certificate".into(),
        ));
    }
    transition_journal(
        store,
        keys,
        operation_id,
        owner_generation,
        PublicationPhase::DataDrained,
        |journal| {
            journal.phase = PublicationPhase::CandidateVerified;
            journal.candidate = Some(candidate);
            journal.verification_evidence = Some(evidence);
            Ok(())
        },
    )
    .await
}

pub async fn mark_retention_prepared(
    store: &dyn ControlStore,
    keys: &Keys,
    operation_id: Id16,
    owner_generation: u64,
    retention_evidence: RootRef,
) -> LifecycleResult<NativePublicationJournal> {
    let existing_bytes = store
        .get(&keys.publication_journal(&operation_id))
        .await?
        .ok_or_else(|| LifecycleError::InvalidState("publication journal not found".into()))?;
    let existing = decode_journal(&existing_bytes)?;
    ensure_sealing_owner(store, keys, &existing).await?;
    if existing.phase == PublicationPhase::RetentionPrepared {
        if existing.owner_generation == owner_generation
            && existing.retention_evidence.as_ref() == Some(&retention_evidence)
        {
            return Ok(existing);
        }
        return Err(LifecycleError::OperationIdMismatch(
            "retention preparation replay changed its evidence root".into(),
        ));
    }
    transition_journal(
        store,
        keys,
        operation_id,
        owner_generation,
        PublicationPhase::CandidateVerified,
        |journal| {
            journal.phase = PublicationPhase::RetentionPrepared;
            journal.retention_evidence = Some(retention_evidence);
            Ok(())
        },
    )
    .await
}

pub async fn recovery_action(
    store: &dyn ControlStore,
    keys: &Keys,
    operation_id: &Id16,
) -> LifecycleResult<RecoveryAction> {
    let bytes = store
        .get(&keys.publication_journal(operation_id))
        .await?
        .ok_or_else(|| LifecycleError::InvalidState("publication journal not found".into()))?;
    let journal = decode_journal(&bytes)?;
    Ok(match journal.phase {
        PublicationPhase::Prepare => RecoveryAction::ResumeQuiesce,
        PublicationPhase::Quiesced => RecoveryAction::ResumeDrain,
        PublicationPhase::DataDrained if journal.fixed_commit_seq.is_some() => {
            RecoveryAction::RebuildCandidate
        }
        PublicationPhase::CandidateVerified
            if journal.fixed_commit_seq.is_some()
                && journal.candidate.is_some()
                && journal.verification_evidence.is_some() =>
        {
            RecoveryAction::VerifyOrPublish
        }
        PublicationPhase::RetentionPrepared
            if journal.fixed_commit_seq.is_some()
                && journal.candidate.is_some()
                && journal.verification_evidence.is_some()
                && journal.retention_evidence.is_some() =>
        {
            RecoveryAction::VerifyOrPublish
        }
        PublicationPhase::PublishedRetained if journal.committed_result.is_some() => {
            RecoveryAction::InstallPublishedResult
        }
        PublicationPhase::Completed if journal.committed_result.is_some() => {
            RecoveryAction::ReturnCompletedResult
        }
        PublicationPhase::Aborted if journal.committed_result.is_none() => {
            RecoveryAction::ReturnAborted
        }
        phase => {
            return Err(LifecycleError::Record(format!(
                "publication journal phase {phase:?} is missing required recovery fields"
            )));
        }
    })
}

pub(crate) fn next_head_ref(current: &HeadRef, new_head_id: Id16) -> LifecycleResult<HeadRef> {
    Ok(HeadRef {
        head_id: new_head_id,
        epoch: current
            .epoch
            .checked_add(1)
            .ok_or_else(|| LifecycleError::LimitExceeded("head epoch overflow".into()))?,
        commit_seq: 0,
    })
}
