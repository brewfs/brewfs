//! The atomic commit transaction (spec 07 §2, spec 18 §10, spec 20 §6).
//!
//! One call to [`commit_uploaded_slice`] performs **one** metadata
//! transaction that:
//!
//! 1. checks the workspace head guard (epoch, writer generation, commit
//!    sequence — a stale lease never commits);
//! 2. checks the origin ownership domain is still the expected ACTIVE
//!    record;
//! 3. checks every object registration against the state captured at
//!    dispatch and every registry binding still points at the registered
//!    `ObjectId` (upload intent → remote verified);
//! 4. enforces the per-inode ordering gate: `mutation_order` must be
//!    exactly `committed_order + 1` — a reordered upload cannot land;
//! 5. replaces extents (with expected-value checks on every removed row),
//!    writes block bindings and head placements, updates inode
//!    size/data_version, flips registrations to `Verified`, records the
//!    mutation result with its durable receipts root, and advances the head
//!    commit sequence.
//!
//! There is no path that commits an extent without its placement, and no
//! path that applies any subset of the writes when a check fails: the
//! transaction is all-or-nothing by construction, which is the property the
//! torn-capture counterexample of PR01 demands be impossible.
//!
//! Retrying the same OperationId with the same payload returns the recorded
//! result untouched; the same OperationId with a different payload is
//! rejected (spec 07 §2).

use std::collections::BTreeMap;
use std::ops::Range;

use crate::native_base::wire::bnct::{
    ControlRecord, DomainState, MutationStatus, NativeMutationResult, ObjectRegistration,
    RegistrationState,
};
use crate::native_base::wire::refs::{Hash32, RootRef};

use super::domain::{HeadGuard, decode_domain, decode_registration};
use super::error::WriteError;
use super::keys::Keys;
use super::receipts::stable_error_text;
use super::records::{ExtentKind, HeadPlacement, HeadState, InodeData, NativeExtent};
use super::store::{ControlStore, Txn};

/// One committed block of a data mutation.
#[derive(Debug, Clone)]
pub struct CommittedBlock {
    /// Block index inside the slice (the binding/placement key suffix).
    pub block_index: u64,
    pub binding: crate::native_base::seal::BlockBinding,
    pub placement: HeadPlacement,
    /// Identity used for registration verification and the receipt set.
    pub object_ref: crate::native_base::wire::refs::ObjectRef,
    /// The registration state captured at dispatch (must be `Dispatched`).
    pub registration: ObjectRegistration,
}

/// The mutation a commit applies.
#[derive(Debug, Clone)]
pub enum Mutation {
    /// Block-aligned data write. `blocks.len()` must equal
    /// `logical_len / block_size`.
    Write {
        logical_offset: u64,
        logical_len: u64,
        slice_id: [u8; 16],
        blocks: Vec<CommittedBlock>,
    },
    Truncate {
        new_size: u64,
    },
    PunchHole {
        offset: u64,
        len: u64,
    },
}

/// One object the commit verifies and flips to `Verified`, beyond the
/// per-block objects (the receipts container itself).
#[derive(Debug, Clone)]
pub struct VerifiedObject {
    pub object_ref: crate::native_base::wire::refs::ObjectRef,
    pub registration: ObjectRegistration,
}

/// Everything one commit needs beyond the state it reads from the store.
#[derive(Debug, Clone)]
pub struct CommitRequest {
    pub operation_id: [u8; 16],
    pub payload_digest: Hash32,
    pub inode: u64,
    pub mutation_order: u64,
    pub mutation: Mutation,
    /// Root of the uploaded receipts container (built with
    /// [`super::receipts::build_receipts_container`]).
    pub receipts: RootRef,
    /// The receipts container's own registration (captured at dispatch).
    pub receipts_registration: VerifiedObject,
    /// Volume block size, for block-alignment validation of data writes.
    pub block_size: u64,
    /// Size of the immutable workspace baseline for this inode. A fresh
    /// native inode has no extent row for baseline bytes, so truncate and
    /// write planning must treat this as its initial logical size without
    /// materializing the untouched baseline as Hole extents.
    pub baseline_size: u64,
    /// The origin ownership domain (spec 20 §6: commit fixes it).
    pub domain_id: [u8; 16],
}

/// The committed state of one inode as read from the control plane.
#[derive(Debug, Clone, Default)]
pub struct InodeView {
    /// False when no inode row exists yet (first commit creates it).
    pub had_record: bool,
    pub data: InodeData,
    pub extents: BTreeMap<u64, NativeExtent>,
}

/// Read the authoritative inode view: the inode row plus every extent row,
/// in logical offset order.
pub async fn read_inode_view(
    store: &dyn ControlStore,
    keys: &Keys,
    workspace_id: &[u8; 16],
    inode: u64,
) -> Result<InodeView, WriteError> {
    let ino_key = keys.inode(workspace_id, inode);
    let ino_bytes = store.get(&ino_key).await?;
    let (had_record, data) = match ino_bytes {
        Some(bytes) => (true, InodeData::decode(&bytes)?),
        None => (false, InodeData::default()),
    };

    let prefix = keys.extents_prefix(workspace_id, inode);
    let rows = store.scan(&prefix).await?;
    let mut extents = BTreeMap::new();
    for (key, value) in rows {
        let suffix = &key[prefix.len()..];
        if suffix.len() != 8 {
            return Err(WriteError::Record(format!(
                "extent key {:02x?} does not end in an 8-byte offset",
                key
            )));
        }
        let offset = u64::from_be_bytes(suffix.try_into().unwrap());
        extents.insert(offset, NativeExtent::decode(&value)?);
    }
    Ok(InodeView {
        had_record,
        data,
        extents,
    })
}

/// The plan of extent row changes a mutation implies, derived from the
/// captured view. Partial overlaps split; `replacement` lands at the range
/// start when present.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExtentChange {
    /// Existing rows to remove (checked against their captured bytes).
    pub removed: Vec<(u64, NativeExtent)>,
    /// Rows to write.
    pub added: Vec<(u64, NativeExtent)>,
}

/// Blocks a data extent's `[0, len)` logical range spans, including a
/// partially-covered tail block (a truncate/punch cut may land inside a
/// block; the block stays referenced and the extent's `logical_len` bounds
/// what is readable).
fn blocks_spanning(len: u64, block_size: u64) -> u64 {
    len.div_ceil(block_size)
}

/// Keep the head `[offset, cut)` of a data extent.
fn keep_head(ext: &NativeExtent, offset: u64, cut: u64, block_size: u64) -> NativeExtent {
    debug_assert!(ext.kind == ExtentKind::Data);
    let kept = cut - offset;
    NativeExtent::data(
        kept,
        ext.slice_id,
        ext.first_block,
        blocks_spanning(kept, block_size),
    )
}

/// Keep the tail `[cut, end)` of a data extent: skip every block the cut
/// touched, including a partially-covered one.
fn keep_tail(ext: &NativeExtent, offset: u64, cut: u64, block_size: u64) -> NativeExtent {
    debug_assert!(ext.kind == ExtentKind::Data);
    let skipped = cut - offset;
    let kept = ext.end(offset) - cut;
    NativeExtent::data(
        kept,
        ext.slice_id,
        ext.first_block + skipped.div_ceil(block_size),
        blocks_spanning(kept, block_size),
    )
}

/// Compute the extent row changes for covering `range` with `replacement`.
///
/// Every extent intersecting the range is removed; extents that only
/// partially overlap are split at the cut points. Data writes are
/// block-aligned (enforced at admission), so their splits are aligned;
/// truncate/punch may cut inside a block, leaving a partially-referenced
/// tail block whose extent `logical_len` bounds what is readable.
pub fn plan_extent_change(
    extents: &BTreeMap<u64, NativeExtent>,
    range: &Range<u64>,
    replacement: Option<NativeExtent>,
    block_size: u64,
) -> ExtentChange {
    let mut change = ExtentChange::default();
    for (&offset, ext) in extents.range(..range.end) {
        let end = ext.end(offset);
        if end <= range.start {
            continue; // entirely before the range
        }
        // offset < range.end && end > range.start: intersects.
        change.removed.push((offset, ext.clone()));
        if offset < range.start {
            if ext.kind == ExtentKind::Data {
                change
                    .added
                    .push((offset, keep_head(ext, offset, range.start, block_size)));
            } else {
                change
                    .added
                    .push((offset, NativeExtent::hole(range.start - offset)));
            }
        }
        if end > range.end {
            if ext.kind == ExtentKind::Data {
                change
                    .added
                    .push((range.end, keep_tail(ext, offset, range.end, block_size)));
            } else {
                change
                    .added
                    .push((range.end, NativeExtent::hole(end - range.end)));
            }
        }
    }
    if let Some(extent) = replacement {
        change.added.push((range.start, extent));
    }
    change
}

/// Outcome of a commit attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitOutcome {
    /// The transaction applied; this is the recorded mutation result.
    Committed(NativeMutationResult),
    /// The same OperationId with the same payload had already committed;
    /// the recorded result is returned untouched.
    AlreadyCommitted(NativeMutationResult),
}

/// Decode a kind 19 envelope into a [`NativeMutationResult`].
pub fn decode_mutation_result(bytes: &[u8]) -> Result<NativeMutationResult, WriteError> {
    match ControlRecord::decode(bytes)? {
        ControlRecord::NativeMutationResult(r) => Ok(r),
        other => Err(WriteError::Record(format!(
            "expected mutation result, got kind {}",
            other.kind().as_u16()
        ))),
    }
}

/// Apply one uploaded mutation atomically. See the module docs for the full
/// check list. `guard.expected_head` must be the head state the caller
/// currently holds; the head advances `commit_seq + 1` inside the same
/// transaction.
pub async fn commit_uploaded_slice(
    store: &dyn ControlStore,
    keys: &Keys,
    guard: &HeadGuard,
    req: &CommitRequest,
) -> Result<CommitOutcome, WriteError> {
    // --- Idempotency: the same OperationId is answered from the record.
    let mut_key = keys.mutation(&req.operation_id);
    if let Some(existing) = store.get(&mut_key).await? {
        let recorded = decode_mutation_result(&existing)?;
        if recorded.payload_digest != req.payload_digest {
            return Err(WriteError::OperationIdMismatch(format!(
                "operation {:02x?} recorded with a different payload digest",
                req.operation_id
            )));
        }
        if recorded.inode != req.inode || recorded.status != MutationStatus::Committed {
            return Err(WriteError::OperationIdMismatch(format!(
                "operation {:02x?} recorded for a different mutation",
                req.operation_id
            )));
        }
        return Ok(CommitOutcome::AlreadyCommitted(recorded));
    }

    // --- Read the state this transaction is derived from.
    let head_key = keys.head(&guard.workspace_id);
    let head_bytes = store
        .get(&head_key)
        .await?
        .ok_or_else(|| WriteError::StaleHeadGuard("workspace head not found".into()))?;
    let head: HeadState = HeadState::decode(&head_bytes)?;
    if head != guard.expected_head {
        return Err(WriteError::StaleHeadGuard(format!(
            "head moved: guard expected epoch {} generation {} commit_seq {}, found epoch {} generation {} commit_seq {}",
            guard.expected_head.head.epoch,
            guard.expected_head.writer_generation,
            guard.expected_head.head.commit_seq,
            head.head.epoch,
            head.writer_generation,
            head.head.commit_seq
        )));
    }

    let domain_key = keys.domain(&req.domain_id);
    let domain_bytes = store
        .get(&domain_key)
        .await?
        .ok_or_else(|| WriteError::Record("ownership domain not found".into()))?;
    let domain = decode_domain(&domain_bytes)?;
    if domain.state != DomainState::Active {
        return Err(WriteError::DomainNotActive(format!(
            "domain state is {:?}, must be Active to commit",
            domain.state
        )));
    }

    let view = read_inode_view(store, keys, &guard.workspace_id, req.inode).await?;

    // --- The per-inode ordering gate (spec 18 §10).
    if req.mutation_order != view.data.committed_order + 1 {
        return Err(WriteError::OutOfOrder(format!(
            "inode {} committed_order is {}, mutation_order is {} — {}",
            req.inode,
            view.data.committed_order,
            req.mutation_order,
            if req.mutation_order <= view.data.committed_order {
                "the ordering slot has already been taken or passed (late upload)"
            } else {
                "an earlier mutation on this inode has not committed yet"
            }
        )));
    }

    // --- Derive the extent changes and the new inode row.
    let change = plan_mutation(&req.mutation, &view, req.block_size, req.baseline_size)?;
    let mut new_data = view.data.clone();
    new_data.committed_order = req.mutation_order;
    new_data.data_version += 1;
    if let Some(size) = planned_size(&req.mutation, &view, req.baseline_size) {
        new_data.size = size;
    }

    // --- Assemble the single atomic transaction.
    let mut txn = Txn::new()
        .check_bytes(head_key.clone(), head_bytes)
        .check_bytes(domain_key, domain_bytes)
        .check_absent(mut_key.clone());

    let ino_key = keys.inode(&guard.workspace_id, req.inode);
    if view.had_record {
        txn = txn.check_bytes(ino_key.clone(), view.data.encode());
    } else {
        txn = txn.check_absent(ino_key.clone());
    }

    for (offset, ext) in &change.removed {
        txn = txn.check_bytes(
            keys.extent(&guard.workspace_id, req.inode, *offset),
            ext.encode(),
        );
    }

    // Verified objects: per-block registrations plus the receipts container.
    let mut block_objects: Vec<&CommittedBlock> = Vec::new();
    if let Mutation::Write { blocks, .. } = &req.mutation {
        block_objects.extend(blocks.iter());
    }
    let mut all_objects: Vec<(
        crate::native_base::wire::refs::ObjectRef,
        ObjectRegistration,
    )> = Vec::new();
    for block in &block_objects {
        all_objects.push((block.object_ref.clone(), block.registration.clone()));
    }
    all_objects.push((
        req.receipts_registration.object_ref.clone(),
        req.receipts_registration.registration.clone(),
    ));

    // Every registration is re-read and validated, then guarded against its
    // current bytes inside the transaction. A same-content object may
    // already be `Verified` — content dedup committed it through another
    // operation, and the bytes are durable either way; any other state is a
    // permanent mismatch (spec 20 §6).
    let namespace_id = domain.namespace_id;
    for (object, registration) in &all_objects {
        if registration.domain_id != req.domain_id || registration.object_ref != *object {
            return Err(WriteError::RegistrationMismatch(format!(
                "object {:02x?} does not match the captured origin-domain registration",
                object.object_id
            )));
        }
        let object_key = keys.object(&req.domain_id, &object.object_id);
        let current_bytes = store.get(&object_key).await?.ok_or_else(|| {
            WriteError::RegistrationMismatch(format!(
                "object {:02x?} has no registration",
                object.object_id
            ))
        })?;
        let current = decode_registration(&current_bytes)?;
        let same_attempt = current.object_ref == registration.object_ref
            && current.domain_id == registration.domain_id
            && current.upload_plan_hash == registration.upload_plan_hash
            && current.registration_seq == registration.registration_seq
            && current.attempt_generation == registration.attempt_generation;
        if !same_attempt
            || !matches!(
                current.state,
                RegistrationState::Dispatched | RegistrationState::Verified
            )
        {
            return Err(WriteError::RegistrationMismatch(format!(
                "object {:02x?} registration facts/state changed before commit (current state {:?})",
                object.object_id, current.state
            )));
        }
        let registry_key = keys.registry(&namespace_id, &object.key);
        txn = txn
            .check_bytes(object_key, current_bytes)
            .check_bytes(registry_key, object.object_id.to_vec());
        let mut verified_reg = current;
        verified_reg.state = RegistrationState::Verified;
        txn = txn.put(
            keys.object(&req.domain_id, &object.object_id),
            ControlRecord::ObjectRegistration(verified_reg).encode(),
        );
    }

    // Extent rows.
    for (offset, _) in &change.removed {
        txn = txn.delete(keys.extent(&guard.workspace_id, req.inode, *offset));
    }
    for (offset, ext) in &change.added {
        txn = txn.put(
            keys.extent(&guard.workspace_id, req.inode, *offset),
            ext.encode(),
        );
    }

    // Bindings and placements.
    if let Mutation::Write {
        logical_offset,
        logical_len,
        slice_id,
        blocks,
    } = &req.mutation
    {
        if logical_len / req.block_size != blocks.len() as u64 {
            return Err(WriteError::Record(format!(
                "write of {logical_len} bytes at block size {} needs {} blocks, got {}",
                req.block_size,
                logical_len / req.block_size,
                blocks.len()
            )));
        }
        for block in blocks {
            txn = txn
                .put(
                    keys.binding(slice_id, block.block_index),
                    block.binding.encode(),
                )
                .put(
                    keys.placement(slice_id, block.block_index),
                    block.placement.encode(),
                );
        }
        let _ = logical_offset; // validated in plan_mutation
    }

    // Inode row, mutation result, head advance.
    let new_head = head.next_commit();
    let result = NativeMutationResult {
        operation_id: req.operation_id,
        payload_digest: req.payload_digest,
        head: new_head.head.clone(),
        inode: req.inode,
        inode_data_version: new_data.data_version,
        durable_receipts: req.receipts.clone(),
        status: MutationStatus::Committed,
        stable_error: None,
    };
    txn = txn
        .put(ino_key, new_data.encode())
        .put(
            mut_key,
            ControlRecord::NativeMutationResult(result.clone()).encode(),
        )
        .put(head_key, new_head.encode());

    store.run(txn).await.map_err(|e| match e {
        super::store::StoreError::Conflict => WriteError::Conflict(
            "commit raced with a concurrent writer; re-read and re-derive".into(),
        ),
        other => other.into(),
    })?;

    Ok(CommitOutcome::Committed(result))
}

/// Durably record a mutation that will never commit (e.g. a late upload
/// whose ordering slot passed). Does not touch the head or the inode —
/// failure never advances committed state.
pub async fn record_failed_mutation(
    store: &dyn ControlStore,
    keys: &Keys,
    operation_id: &[u8; 16],
    payload_digest: Hash32,
    head: &HeadState,
    inode: u64,
    error: &WriteError,
) -> Result<(), WriteError> {
    let result = NativeMutationResult {
        operation_id: *operation_id,
        payload_digest,
        head: head.head.clone(),
        inode,
        inode_data_version: 0,
        durable_receipts: RootRef {
            object: crate::native_base::wire::refs::ObjectRef {
                object_id: [0u8; 16],
                kind: 3,
                object_len: 0,
                full_hash: [0u8; 32],
                key: Vec::new(),
            },
            address: crate::native_base::wire::refs::PageAddress {
                offset: 0,
                stored_len: 0,
                raw_len: 0,
                codec: crate::native_base::wire::container::Codec::None,
                page_kind: crate::native_base::wire::refs::PageKind::GenericKeyValue,
                level: 0,
                entry_count: 0,
                stored_digest: [0u8; 32],
            },
        },
        status: MutationStatus::Failed,
        stable_error: Some(stable_error_text(error)),
    };
    let mut_key = keys.mutation(operation_id);
    store
        .run(Txn::new().check_absent(mut_key.clone()).put(
            mut_key,
            ControlRecord::NativeMutationResult(result).encode(),
        ))
        .await?;
    Ok(())
}

/// The post-mutation file size, if it changes.
fn logical_size(view: &InodeView, baseline_size: u64) -> u64 {
    if view.had_record {
        view.data.size
    } else {
        baseline_size
    }
}

fn planned_size(mutation: &Mutation, view: &InodeView, baseline_size: u64) -> Option<u64> {
    let current_size = logical_size(view, baseline_size);
    match mutation {
        Mutation::Write {
            logical_offset,
            logical_len,
            ..
        } => Some(current_size.max(logical_offset + logical_len)),
        Mutation::Truncate { new_size } => Some(*new_size),
        Mutation::PunchHole { .. } => Some(current_size),
    }
}

/// Validate a mutation against the captured view and derive its extent
/// change.
fn plan_mutation(
    mutation: &Mutation,
    view: &InodeView,
    block_size: u64,
    baseline_size: u64,
) -> Result<ExtentChange, WriteError> {
    if block_size == 0 {
        return Err(WriteError::Record("block size must be > 0".into()));
    }
    let current_size = logical_size(view, baseline_size);
    match mutation {
        Mutation::Write {
            logical_offset,
            logical_len,
            slice_id,
            blocks,
        } => {
            if *logical_len == 0 {
                return Err(WriteError::Record("write length must be > 0".into()));
            }
            if logical_offset % block_size != 0 || logical_len % block_size != 0 {
                return Err(WriteError::Record(format!(
                    "write [{logical_offset}, {}) is not block-aligned at block size {block_size}",
                    logical_offset + logical_len
                )));
            }
            if blocks.len() as u64 != logical_len / block_size {
                return Err(WriteError::Record(format!(
                    "write of {logical_len} bytes needs {} blocks, got {}",
                    logical_len / block_size,
                    blocks.len()
                )));
            }
            let extent = NativeExtent::data(
                *logical_len,
                *slice_id,
                blocks.first().map(|b| b.block_index).unwrap_or(0),
                blocks.len() as u64,
            );
            let mut change = plan_extent_change(
                &view.extents,
                &(*logical_offset..*logical_offset + *logical_len),
                Some(extent),
                block_size,
            );
            if *logical_offset > current_size {
                let gap = plan_extent_change(
                    &view.extents,
                    &(current_size..*logical_offset),
                    Some(NativeExtent::hole(*logical_offset - current_size)),
                    block_size,
                );
                change.removed.extend(gap.removed);
                change.added.extend(gap.added);
            }
            Ok(change)
        }
        Mutation::Truncate { new_size } => {
            if *new_size < current_size {
                Ok(plan_extent_change(
                    &view.extents,
                    &(*new_size..current_size),
                    None,
                    block_size,
                ))
            } else if *new_size > current_size {
                let hole = NativeExtent::hole(*new_size - current_size);
                Ok(plan_extent_change(
                    &view.extents,
                    &(current_size..*new_size),
                    Some(hole),
                    block_size,
                ))
            } else {
                Ok(ExtentChange::default())
            }
        }
        Mutation::PunchHole { offset, len } => {
            let end = (*offset + len).min(current_size);
            if *offset >= current_size || end == *offset {
                return Ok(ExtentChange::default());
            }
            let hole = NativeExtent::hole(end - *offset);
            Ok(plan_extent_change(
                &view.extents,
                &(*offset..end),
                Some(hole),
                block_size,
            ))
        }
    }
}
