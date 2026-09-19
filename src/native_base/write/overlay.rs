//! The write overlay: admission, dirty tracking and the ordered drain.
//!
//! This is the PR01 `CEX-ORD` model made real against the control store.
//! Contract (spec 18 §10, spec 07 §12):
//!
//! - every mutation receives an `admission_ticket` (local order) and a
//!   per-inode `mutation_order` at accept time, and a dirty reference the
//!   size of its logical range;
//! - uploads may complete in any order, but a mutation only *commits* when
//!   its upload is durable **and** every earlier mutation on the same inode
//!   has committed — a late upload can never overtake a newer one;
//! - on commit the overlay hands off **only** that operation's dirty
//!   references; a captured view keeps its `Arc` clones alive (already
//!   captured reads finish on the old dirty set, spec 18 §5);
//! - a permanently failed predecessor blocks its successors on the same
//!   inode — they are reported, never skipped.
//!
//! Cross-inode commits interleave freely. The PR04 overlay holds its state
//! lock across control-store awaits (single-writer workspace); the PR10
//! async executor reworks the concurrency shape without touching these
//! ordering contracts.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;

use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use crate::chunk::compress::{Compression, encode_persisted_block};
use crate::native_base::seal::{BlockBinding, NATIVE_LOOSE_KIND};
use crate::native_base::wire::bnct::{
    DomainKind, DomainState, HeadRef, NativeMutationResult, OwnershipDomain,
};
use crate::native_base::wire::refs::{Hash32, ObjectId, ObjectRef, RootRef};

use super::commit::{
    CommitOutcome, CommitRequest, CommittedBlock, Mutation, VerifiedObject, commit_uploaded_slice,
    read_inode_view,
};
use super::domain::{
    HeadGuard, UploadReservation, create_domain, mark_dispatched, register_upload,
};
use super::error::WriteError;
use super::keys::Keys;
use super::lease::{LeaseClock, LeaseGrant, WriterLease};
use super::orphan_receipt::{KvStep, MAX_ORPHAN_REASON, OrphanReceipt, record_orphan_receipt};
use super::receipts::{
    MemorySink, ObjectSink, ReceiptEntry, ReceiptSet, build_receipts_container, object_key,
};
use super::records::{HeadPlacement, HeadState, InodeAttributes, InodeData, NativeExtent};
use super::replace::{Replacement, plan_rename_over};
use super::store::{ControlStore, Txn};

/// Fixed parameters of one overlay instance.
#[derive(Debug, Clone)]
pub struct OverlayParams {
    pub volume_id: [u8; 16],
    pub workspace_id: [u8; 16],
    pub domain_id: [u8; 16],
    pub writer_generation: u64,
    pub block_size: u64,
}

/// A mutation as accepted from the caller, before registration.
#[derive(Debug, Clone)]
pub enum MutationSpec {
    /// Block-aligned data write (`offset` and `data.len()` multiples of the
    /// block size).
    Write {
        offset: u64,
        data: Arc<Vec<u8>>,
    },
    Truncate {
        new_size: u64,
    },
    PunchHole {
        offset: u64,
        len: u64,
    },
    /// Metadata-only mutation (WRITE-010): no data object is created, no
    /// extent row changes.
    SetAttributes {
        mode: u32,
        uid: u32,
        gid: u32,
        ctime_ns: i64,
    },
    /// WRITE-012: publish another inode's complete content as this inode's new
    /// content (a temp file renamed over the destination).  The plan is built
    /// at admission from the source's durable view and carries every one of
    /// its blocks, so the replacement is application-complete and is never
    /// reported as a minimal patch.
    ReplaceInode {
        source_inode: u64,
    },
}

impl MutationSpec {
    /// Whether accepting this mutation dirties a data range.  A metadata-only
    /// mutation dirties nothing: a reader is never told to re-read bytes.
    pub fn touches_data(&self) -> bool {
        !matches!(self, MutationSpec::SetAttributes { .. })
    }
}

/// The admission receipt handed back by [`WriteOverlay::accept`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ticket {
    pub admission_ticket: u64,
    pub inode: u64,
    pub mutation_order: u64,
    pub operation_id: [u8; 16],
}

/// One dirty region: the logical range of an accepted, not-yet-handed-off
/// mutation. Shared as `Arc` so captured views keep reading the dirty set
/// they were built on (spec 18 §5).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirtyExtent {
    pub ticket: u64,
    pub inode: u64,
    pub offset: u64,
    pub len: u64,
    pub generation: u64,
}

/// A consistent view over the overlay: the committed mirrors plus the dirty
/// set as of one `dirty_generation`.
#[derive(Debug, Clone)]
pub struct CapturedView {
    pub dirty_generation: u64,
    pub dirty: Vec<Arc<DirtyExtent>>,
    pub head: HeadState,
    /// Committed inode mirrors: inode -> (data, extents).
    pub inodes: HashMap<u64, (InodeData, BTreeMap<u64, NativeExtent>)>,
}

/// What one drain pass achieved.
#[derive(Debug, Default, Clone)]
pub struct DrainReport {
    pub committed: Vec<NativeMutationResult>,
    pub failed: Vec<(Ticket, String)>,
    pub blocked: Vec<(Ticket, String)>,
    /// Uploads that are durable but could not be committed: their receipt was
    /// recorded in the protected orphan set so the cleaner cannot collect
    /// objects no head references yet (WRITE-006).
    pub orphaned: Vec<OrphanReceipt>,
    /// Set when a commit was refused by the writer's lease: the lease is gone,
    /// so the rest of this writer's private state was rolled back instead of
    /// being left half-published (CONS-004).
    pub rolled_back: Option<RollbackReport>,
}

/// Per-inode overlay state.
#[derive(Debug, Default)]
struct InodeOverlay {
    next_mutation_order: u64,
    data: InodeData,
    extents: BTreeMap<u64, NativeExtent>,
    pending: VecDeque<PendingOp>,
    dirty: Vec<Arc<DirtyExtent>>,
}

#[derive(Debug)]
struct PendingOp {
    ticket: u64,
    mutation_order: u64,
    operation_id: [u8; 16],
    payload_digest: Hash32,
    spec: MutationSpec,
    /// The plan of a rename-over, built at admission (WRITE-012).
    replacement: Option<Arc<Replacement>>,
    /// Blocks this operation already dispatched whose upload never completed
    /// the operation: a fence mid-dispatch leaves them durable but
    /// unpublished, so the rollback has to protect them too.
    partial: Vec<CommittedBlock>,
    state: OpState,
}

#[derive(Debug)]
enum OpState {
    /// Accepted; uploads not yet dispatched.
    Accepted,
    /// Dispatched and uploaded; `receipts` is filled by `complete_upload`.
    Uploaded(Box<UploadedState>),
    Committed,
    Failed(String),
    Blocked(String),
}

#[derive(Debug)]
struct UploadedState {
    blocks: Vec<CommittedBlock>,
    receipts: Option<(RootRef, VerifiedObject)>,
    /// Set for a rename-over: the carried extents the commit publishes.
    replacement: Option<Arc<Replacement>>,
}

/// The write overlay.
pub struct WriteOverlay {
    store: Arc<dyn ControlStore>,
    sink: Arc<dyn ObjectSink>,
    keys: Keys,
    params: OverlayParams,
    /// The lease this writer publishes under, when it holds one.  A writer
    /// without a lease keeps the PR04 behaviour (guards derived from the head
    /// it just read); a writer with one cannot publish a single row once the
    /// lease is gone (CONS-004).
    lease: Option<WriterLease>,
    state: Mutex<OverlayState>,
}

/// What a fence-and-rollback did to the writer's private state.
///
/// `published_rows` is zero by construction: a rollback never writes a head,
/// extent, inode or mutation-result row, so the volume namespace it does not
/// own is left exactly as the last committed writer left it.
#[derive(Debug, Default, Clone)]
pub struct RollbackReport {
    /// Operations that were fenced out of the pipeline, in admission order.
    pub stopped: Vec<(Ticket, String)>,
    /// Dirty references dropped with those operations.  A capture taken
    /// before the rollback keeps its own `Arc` clones and is unaffected.
    pub dropped_dirty: usize,
    /// Uploads that were durable but never published: each is protected by an
    /// orphan receipt so a cleaner cannot collect it (WRITE-006).
    pub protected: Vec<OrphanReceipt>,
    /// Inodes whose local mirrors were discarded, so the next admission
    /// re-derives them from the store instead of trusting a fenced view.
    pub forgotten_inodes: usize,
    /// Rows published outside the writer's private state: always zero.
    pub published_rows: usize,
}

impl RollbackReport {
    /// True when nothing this writer did can still be published: every
    /// in-flight operation stopped and every durable upload is protected.
    pub fn stopped_publishing(&self) -> bool {
        self.published_rows == 0
    }
}

#[derive(Debug, Default)]
struct OverlayState {
    head: Option<HeadState>,
    next_ticket: u64,
    next_dirty_generation: u64,
    baseline_sizes: HashMap<u64, u64>,
    inodes: HashMap<u64, InodeOverlay>,
    /// Every object this overlay put to the sink, in upload order.  Upload
    /// observability: a metadata-only mutation must add only its control
    /// object, never a data block (WRITE-010).
    uploaded_objects: Vec<ObjectId>,
    /// The last fence-and-rollback this writer performed, for the caller that
    /// has to report what a lapsed lease or a rollback cleaned up (CONS-004).
    last_rollback: Option<RollbackReport>,
}

/// Create the head row for a fresh workspace (absent-else-validate) and
/// return the authoritative head state.
pub async fn ensure_workspace_head(
    store: &dyn ControlStore,
    keys: &Keys,
    params: &OverlayParams,
    epoch: u64,
) -> Result<HeadState, WriteError> {
    let head = HeadState {
        head: HeadRef {
            head_id: params.workspace_id,
            epoch,
            commit_seq: 0,
        },
        writer_generation: params.writer_generation,
        write_domain_id: params.domain_id,
    };
    let head_key = keys.head(&params.workspace_id);
    match store.get(&head_key).await? {
        Some(existing) => {
            let found = HeadState::decode(&existing)?;
            if found.head.head_id != head.head.head_id
                || found.write_domain_id != head.write_domain_id
            {
                return Err(WriteError::Record(format!(
                    "workspace head exists with a different identity: {:02x?}",
                    found.head.head_id
                )));
            }
            if found.writer_generation == head.writer_generation {
                return Ok(found);
            }
            let next = HeadState {
                writer_generation: head.writer_generation,
                ..found
            };
            store
                .run(
                    Txn::new()
                        .check_bytes(head_key.clone(), existing)
                        .put(head_key, next.encode()),
                )
                .await
                .map_err(|_| {
                    WriteError::Conflict(
                        "workspace head generation raced with another initializer".into(),
                    )
                })?;
            Ok(next)
        }
        None => {
            store
                .run(
                    Txn::new()
                        .check_absent(head_key.clone())
                        .put(head_key, head.encode()),
                )
                .await
                .map_err(|_| WriteError::Conflict("head raced with another initializer".into()))?;
            Ok(head)
        }
    }
}

impl WriteOverlay {
    pub fn new(
        store: Arc<dyn ControlStore>,
        sink: Arc<dyn ObjectSink>,
        params: OverlayParams,
    ) -> Self {
        let keys = Keys::new(&params.volume_id);
        WriteOverlay {
            store,
            sink,
            keys,
            params,
            lease: None,
            state: Mutex::new(OverlayState::default()),
        }
    }

    /// Publish under a writer lease decided on `clock` (a backend clock).
    /// Every registration and every commit then asks the lease for its guard,
    /// so an expired or superseded lease stops the write before any durable
    /// row is written (CONS-004).
    pub fn with_writer_lease(mut self, grant: LeaseGrant, clock: Arc<dyn LeaseClock>) -> Self {
        self.lease = Some(WriterLease::new(grant, clock));
        self
    }

    /// The lease this writer publishes under, if it holds one.
    pub fn lease(&self) -> Option<&WriterLease> {
        self.lease.as_ref()
    }

    /// The guard a publishing step may run under.  With a lease this is the
    /// lease's decision on backend time; without one it is the head the
    /// caller just read, which is the PR04 behaviour every existing caller
    /// keeps.
    fn publish_guard(&self, head: &HeadState) -> Result<HeadGuard, WriteError> {
        match &self.lease {
            Some(lease) => lease.guard(head),
            None => Ok(HeadGuard {
                workspace_id: self.params.workspace_id,
                expected_head: head.clone(),
            }),
        }
    }

    /// Convenience constructor with the in-memory sink (tests, reference
    /// pipeline).
    pub fn with_memory_sink(store: Arc<dyn ControlStore>, params: OverlayParams) -> Self {
        Self::new(store, Arc::new(MemorySink::default()), params)
    }

    /// Create the ACTIVE ownership domain record (an existing ACTIVE domain
    /// for the same volume is accepted).
    pub async fn ensure_domain(&self, owner_id: [u8; 16]) -> Result<(), WriteError> {
        let domain = OwnershipDomain {
            domain_id: self.params.domain_id,
            volume_id: self.params.volume_id,
            // One object namespace per workspace: registry keys cannot
            // collide across workspaces.
            namespace_id: self.params.workspace_id,
            domain_kind: DomainKind::Workspace,
            owner_id,
            owner_generation: 1,
            state: DomainState::Active,
            entity_version: 1,
            inventory_seq: 0,
            retention_seq: 0,
            outstanding_attempts: 0,
            open_operations: 0,
            accepted_ticket_end: None,
            close_ref: None,
        };
        let key = self.keys.domain(&self.params.domain_id);
        match self.store.get(&key).await? {
            Some(existing) => {
                let found = super::domain::decode_domain(&existing)?;
                if found.volume_id != domain.volume_id || found.state != DomainState::Active {
                    return Err(WriteError::DomainNotActive(format!(
                        "domain exists as {:?} for volume {:02x?}",
                        found.state, found.volume_id
                    )));
                }
                Ok(())
            }
            None => create_domain(&*self.store, &self.keys, domain).await,
        }
    }

    /// Accept a mutation: assign the admission ticket and the per-inode
    /// mutation order, and record the dirty region. The first operation on
    /// an inode continues the durable ordering watermark from the store.
    pub async fn accept(&self, inode: u64, spec: MutationSpec) -> Result<Ticket, WriteError> {
        if self.params.block_size == 0 {
            return Err(WriteError::Record("block size must be > 0".into()));
        }
        if let MutationSpec::Write { offset, data } = &spec {
            if data.is_empty() {
                return Err(WriteError::Record("write length must be > 0".into()));
            }
            if !offset.is_multiple_of(self.params.block_size)
                || !(data.len() as u64).is_multiple_of(self.params.block_size)
            {
                return Err(WriteError::Record(format!(
                    "write [{offset}, {}) is not block-aligned at block size {}",
                    offset + data.len() as u64,
                    self.params.block_size
                )));
            }
        }
        // WRITE-010: a chmod carries a full `st_mode`.  A bare permission mask
        // is refused here, before it takes a mutation order: a rejected input
        // must not poison the inode's ordering watermark for every successor.
        if let MutationSpec::SetAttributes { mode, .. } = &spec {
            InodeAttributes {
                mode: *mode,
                uid: 0,
                gid: 0,
                rdev: 0,
                ctime_ns: 0,
            }
            .validate()
            .map_err(|error| WriteError::Record(format!("invalid inode attributes: {error}")))?;
        }

        // WRITE-012: a rename-over is planned before it takes a mutation order.
        // A source that cannot be published completely (missing binding,
        // placement or registration) must be refused here, not after it has
        // consumed a slot the inode's successors depend on.
        let replacement = match &spec {
            MutationSpec::ReplaceInode { source_inode } => Some(Arc::new(
                plan_rename_over(
                    &*self.store,
                    &self.keys,
                    &self.params.workspace_id,
                    &self.params.domain_id,
                    *source_inode,
                    inode,
                    self.params.block_size,
                )
                .await?,
            )),
            _ => None,
        };

        // Load the durable view before locking (first touch of an inode
        // must continue the committed ordering watermark).
        let known = {
            let state = self.state.lock().await;
            state.inodes.contains_key(&inode)
        };
        let view = if known {
            None
        } else {
            Some(read_inode_view(&*self.store, &self.keys, &self.params.workspace_id, inode).await?)
        };

        let mut state = self.state.lock().await;
        state.next_ticket += 1;
        let ticket_no = state.next_ticket;
        state.next_dirty_generation += 1;
        let generation = state.next_dirty_generation;

        let operation_id = operation_id_for(
            &self.params.volume_id,
            &self.params.workspace_id,
            inode,
            ticket_no,
        );
        let entry = state.inodes.entry(inode).or_insert_with(|| {
            let view = view.clone().unwrap_or_default();
            InodeOverlay {
                next_mutation_order: view.data.committed_order,
                data: view.data,
                extents: view.extents,
                pending: VecDeque::new(),
                dirty: Vec::new(),
            }
        });
        entry.next_mutation_order += 1;
        let mutation_order = entry.next_mutation_order;

        let (offset, len) = match &replacement {
            // A replacement republishes the whole file: every byte the
            // destination can currently read is stale afterwards, so the dirty
            // range is the union of both versions, never a delta.
            Some(plan) => (0, plan.size.max(entry.data.size)),
            None => dirty_range(&spec, entry.data.size),
        };
        if spec.touches_data() {
            entry.dirty.push(Arc::new(DirtyExtent {
                ticket: ticket_no,
                inode,
                offset,
                len,
                generation,
            }));
        }
        entry.pending.push_back(PendingOp {
            ticket: ticket_no,
            mutation_order,
            operation_id,
            payload_digest: payload_digest(&spec, replacement.as_deref()),
            spec,
            replacement,
            partial: Vec::new(),
            state: OpState::Accepted,
        });

        Ok(Ticket {
            admission_ticket: ticket_no,
            inode,
            mutation_order,
            operation_id,
        })
    }

    /// Capture a consistent view: the committed mirrors, the head and the
    /// dirty set at one generation. The returned `Arc` clones keep this
    /// view's dirty set alive across later handoffs.
    pub async fn capture(&self) -> CapturedView {
        let state = self.state.lock().await;
        CapturedView {
            dirty_generation: state.next_dirty_generation,
            dirty: state
                .inodes
                .values()
                .flat_map(|i| i.dirty.iter().cloned())
                .collect(),
            head: state.head.clone().unwrap_or(HeadState {
                head: HeadRef {
                    head_id: self.params.workspace_id,
                    epoch: 0,
                    commit_seq: 0,
                },
                writer_generation: self.params.writer_generation,
                write_domain_id: self.params.domain_id,
            }),
            inodes: state
                .inodes
                .iter()
                .map(|(inode, i)| (*inode, (i.data.clone(), i.extents.clone())))
                .collect(),
        }
    }

    /// Dispatch one operation's uploads: register every block object under
    /// the current head guard, mark the registrations dispatched, and put
    /// the encoded objects to the sink. Completing the upload is a separate
    /// step ([`Self::complete_upload`]) so callers control upload timing —
    /// the reorder counterexamples depend on it.
    pub async fn dispatch(&self, ticket: &Ticket) -> Result<(), WriteError> {
        // Validate and extract the payload under the lock; the registration
        // and upload then run lock-free (they re-lock internally to record
        // the head they registered under).
        let payload = {
            let mut state = self.state.lock().await;
            let inode_state = state.inodes.get_mut(&ticket.inode).ok_or_else(|| {
                WriteError::Record(format!("inode {} has no pending operations", ticket.inode))
            })?;
            let op = inode_state
                .pending
                .iter_mut()
                .find(|op| op.ticket == ticket.admission_ticket)
                .ok_or_else(|| {
                    WriteError::Record(format!("ticket {} not found", ticket.admission_ticket))
                })?;
            if !matches!(op.state, OpState::Accepted) {
                return Err(WriteError::Record(format!(
                    "ticket {} is not in the Accepted state",
                    ticket.admission_ticket
                )));
            }
            // WRITE-012: a rename-over uploads nothing — its blocks are already
            // durable under the source's slice.  The plan built at admission is
            // the complete payload, and the receipts completion step still has
            // to attest every one of its blocks.
            if matches!(op.spec, MutationSpec::ReplaceInode { .. }) {
                let Some(plan) = op.replacement.clone() else {
                    return Err(WriteError::Record(format!(
                        "ticket {} is a rename-over without a plan",
                        ticket.admission_ticket
                    )));
                };
                op.state = OpState::Uploaded(Box::new(UploadedState {
                    blocks: plan.flattened_blocks(),
                    receipts: None,
                    replacement: Some(plan),
                }));
                return Ok(());
            }
            match &op.spec {
                MutationSpec::Write { data, .. } => Some(data.clone()),
                MutationSpec::Truncate { .. }
                | MutationSpec::PunchHole { .. }
                | MutationSpec::SetAttributes { .. }
                | MutationSpec::ReplaceInode { .. } => None,
            }
        };

        let Some(data) = payload else {
            // Truncate/punch have no data objects to upload; the receipts
            // container is built at completion time.
            let mut state = self.state.lock().await;
            let inode_state = state.inodes.get_mut(&ticket.inode).unwrap();
            let op = inode_state
                .pending
                .iter_mut()
                .find(|op| op.ticket == ticket.admission_ticket)
                .unwrap();
            op.state = OpState::Uploaded(Box::new(UploadedState {
                blocks: Vec::new(),
                receipts: None,
                replacement: None,
            }));
            return Ok(());
        };

        let block_size = self.params.block_size;
        let mut prepared = Vec::new();
        for (block_index, content) in data.chunks(block_size as usize).enumerate() {
            // A loose object is the versioned-framed persisted block; the
            // binding hashes the decoded content (spec 03/04).
            let framed = encode_persisted_block(content, Compression::None);
            let object_id: ObjectId = Sha256::digest(content).as_slice()[..16].try_into().unwrap();
            let full_hash: Hash32 = Sha256::digest(&framed).into();
            let object_ref = ObjectRef {
                object_id,
                kind: NATIVE_LOOSE_KIND,
                object_len: framed.len() as u64,
                full_hash,
                key: object_key(&self.params.volume_id, "loose", &object_id, &full_hash),
            };
            // Register under the head guard, then dispatch, then upload
            // (spec 20 §6 order). A fence here stops the operation before it
            // can publish, but the blocks that already landed are recorded so
            // the rollback protects every durable upload.
            let dispatched = async {
                self.register_under_current_head(&object_ref, full_hash)
                    .await?;
                let registration =
                    mark_dispatched(&*self.store, &self.keys, &self.params.domain_id, &object_id)
                        .await?;
                self.sink
                    .put(&object_ref, &framed)
                    .await
                    .map_err(|error| WriteError::Object(error.to_string()))?;
                self.record_upload(object_ref.object_id).await;
                Ok::<_, WriteError>(registration)
            }
            .await;
            match dispatched {
                Ok(registration) => prepared.push(CommittedBlock {
                    block_index: block_index as u64,
                    binding: BlockBinding::of_decoded(content),
                    placement: HeadPlacement::loose(object_id),
                    object_ref,
                    registration,
                }),
                Err(err) => {
                    let landed = std::mem::take(&mut prepared);
                    self.stash_partial_dispatch(ticket, landed).await;
                    return Err(self.fenced(err).await);
                }
            }
        }

        let mut state = self.state.lock().await;
        let inode_state = state.inodes.get_mut(&ticket.inode).unwrap();
        let op = inode_state
            .pending
            .iter_mut()
            .find(|op| op.ticket == ticket.admission_ticket)
            .unwrap();
        op.state = OpState::Uploaded(Box::new(UploadedState {
            blocks: prepared,
            receipts: None,
            replacement: None,
        }));
        Ok(())
    }

    /// Complete an upload: build the receipts container over the dispatched
    /// registrations, upload it, register and dispatch it. After this the
    /// operation is committable in order.
    pub async fn complete_upload(&self, ticket: &Ticket) -> Result<(), WriteError> {
        // The receipt entries are derived under the lock; building,
        // registering and uploading the container run lock-free.
        let (operation_id, entries) = {
            let mut state = self.state.lock().await;
            let inode_state = state.inodes.get_mut(&ticket.inode).ok_or_else(|| {
                WriteError::Record(format!("inode {} has no pending operations", ticket.inode))
            })?;
            let op = inode_state
                .pending
                .iter_mut()
                .find(|op| op.ticket == ticket.admission_ticket)
                .ok_or_else(|| {
                    WriteError::Record(format!("ticket {} not found", ticket.admission_ticket))
                })?;
            let OpState::Uploaded(uploaded) = &op.state else {
                return Err(WriteError::Record(format!(
                    "ticket {} must be dispatched before completion",
                    ticket.admission_ticket
                )));
            };
            // Receipts attest the objects this commit depends on — the data
            // blocks, not the receipt artifact itself.
            let entries: Vec<ReceiptEntry> = uploaded
                .blocks
                .iter()
                .map(|block| ReceiptEntry {
                    object_id: block.object_ref.object_id,
                    domain_id: self.params.domain_id,
                    attempt_generation: block.registration.attempt_generation,
                    registration_seq: block.registration.registration_seq,
                    full_hash: block.object_ref.full_hash,
                })
                .collect();
            (op.operation_id, entries)
        };

        let set = ReceiptSet { entries };
        let mut hasher = Sha256::new();
        hasher.update(operation_id);
        hasher.update(b"receipts");
        let receipts_object_id: ObjectId = hasher.finalize().as_slice()[..16].try_into().unwrap();
        let built = build_receipts_container(&self.params.volume_id, &receipts_object_id, &set);
        let published = async {
            self.register_under_current_head(&built.root.object, built.root.object.full_hash)
                .await?;
            let registration = mark_dispatched(
                &*self.store,
                &self.keys,
                &self.params.domain_id,
                &receipts_object_id,
            )
            .await?;
            self.sink
                .put(&built.root.object, &built.bytes)
                .await
                .map_err(|error| WriteError::Object(error.to_string()))?;
            self.record_upload(built.root.object.object_id).await;
            Ok::<_, WriteError>(registration)
        }
        .await;
        let registration = match published {
            Ok(registration) => registration,
            Err(err) => return Err(self.fenced(err).await),
        };

        let mut state = self.state.lock().await;
        let inode_state = state.inodes.get_mut(&ticket.inode).unwrap();
        let op = inode_state
            .pending
            .iter_mut()
            .find(|op| op.ticket == ticket.admission_ticket)
            .unwrap();
        let OpState::Uploaded(uploaded) = &mut op.state else {
            return Err(WriteError::Record(format!(
                "ticket {} left the Uploaded state during completion",
                ticket.admission_ticket
            )));
        };
        uploaded.receipts = Some((
            built.root.clone(),
            VerifiedObject {
                object_ref: built.root.object.clone(),
                registration,
            },
        ));
        Ok(())
    }

    /// Drain: commit every committable operation in admission order per
    /// inode, handing off dirty references as they commit. Blocked and
    /// failed operations are reported, never skipped.
    pub async fn drain(&self) -> Result<DrainReport, WriteError> {
        let mut report = DrainReport::default();
        let mut state = self.state.lock().await;
        if state.head.is_none() {
            let head = self.read_head().await?;
            state.head = Some(head);
        }

        let inode_ids: Vec<u64> = state.inodes.keys().copied().collect();
        for inode in inode_ids {
            loop {
                // Phase 1: pop terminal operations off the front.
                let terminal = {
                    let inode_state = state.inodes.get_mut(&inode).unwrap();
                    let Some(front) = inode_state.pending.front() else {
                        break;
                    };
                    let ticket = Ticket {
                        admission_ticket: front.ticket,
                        inode,
                        mutation_order: front.mutation_order,
                        operation_id: front.operation_id,
                    };
                    match &front.state {
                        OpState::Committed => {
                            inode_state.pending.pop_front();
                            Some(Either::Left(ticket))
                        }
                        OpState::Failed(reason) => {
                            let reason = reason.clone();
                            inode_state.pending.pop_front();
                            Some(Either::Right((ticket, reason, false)))
                        }
                        OpState::Blocked(reason) => {
                            let reason = reason.clone();
                            inode_state.pending.pop_front();
                            Some(Either::Right((ticket, reason, true)))
                        }
                        OpState::Accepted | OpState::Uploaded(_) => None,
                    }
                };
                match terminal {
                    Some(Either::Left(_)) => continue,
                    Some(Either::Right((ticket, reason, blocked))) => {
                        if blocked {
                            report.blocked.push((ticket, reason));
                        } else {
                            report.failed.push((ticket, reason));
                        }
                        continue;
                    }
                    None => {}
                }

                // Phase 2: the front operation is Accepted (waiting for its
                // upload) or Uploaded (committable now).
                let request = {
                    let inode_state = state.inodes.get(&inode).unwrap();
                    let front = inode_state.pending.front().unwrap();
                    let OpState::Uploaded(uploaded) = &front.state else {
                        break; // Accepted: its upload has not completed.
                    };
                    let Some((receipts_root, receipts_registration)) = &uploaded.receipts else {
                        break; // Dispatched but not completed.
                    };
                    let mutation = match &front.spec {
                        MutationSpec::Write { offset, data } => Mutation::Write {
                            logical_offset: *offset,
                            logical_len: data.len() as u64,
                            slice_id: slice_id_for(&front.operation_id),
                            blocks: uploaded.blocks.clone(),
                        },
                        MutationSpec::Truncate { new_size } => Mutation::Truncate {
                            new_size: *new_size,
                        },
                        MutationSpec::PunchHole { offset, len } => Mutation::PunchHole {
                            offset: *offset,
                            len: *len,
                        },
                        MutationSpec::SetAttributes {
                            mode,
                            uid,
                            gid,
                            ctime_ns,
                        } => Mutation::SetAttributes {
                            mode: *mode,
                            uid: *uid,
                            gid: *gid,
                            ctime_ns: *ctime_ns,
                        },
                        MutationSpec::ReplaceInode { source_inode } => {
                            let Some(plan) = &uploaded.replacement else {
                                return Err(WriteError::Record(format!(
                                    "rename-over operation {:02x?} lost its plan",
                                    front.operation_id
                                )));
                            };
                            Mutation::ReplaceInode {
                                source_inode: *source_inode,
                                logical_size: plan.size,
                                carried: plan.carried.clone(),
                            }
                        }
                    };
                    CommitRequest {
                        operation_id: front.operation_id,
                        payload_digest: front.payload_digest,
                        inode,
                        mutation_order: front.mutation_order,
                        mutation,
                        receipts: receipts_root.clone(),
                        receipts_registration: receipts_registration.clone(),
                        block_size: self.params.block_size,
                        baseline_size: state.baseline_sizes.get(&inode).copied().unwrap_or(0),
                        domain_id: self.params.domain_id,
                    }
                };

                let ticket_no = {
                    let inode_state = state.inodes.get(&inode).unwrap();
                    inode_state.pending.front().unwrap().ticket
                };

                // The commit runs under the lease when the writer holds one: a
                // fenced writer is refused here, before the transaction.
                let outcome = match self.publish_guard(&state.head.clone().unwrap()) {
                    Ok(guard) => {
                        commit_uploaded_slice(&*self.store, &self.keys, &guard, &request).await
                    }
                    Err(err) => Err(err),
                };
                match outcome {
                    Ok(outcome) => {
                        let result = match outcome {
                            CommitOutcome::Committed(r) | CommitOutcome::AlreadyCommitted(r) => r,
                        };
                        self.handoff_committed(&mut state, inode, ticket_no);
                        report.committed.push(result);
                        drop(state);
                        state = self.refresh_state(inode).await?;
                    }
                    Err(err) if err.is_retryable() => {
                        // One re-derivation with fresh state.
                        drop(state);
                        state = self.refresh_state(inode).await?;
                        let retry = match self.publish_guard(&state.head.clone().unwrap()) {
                            Ok(guard) => {
                                commit_uploaded_slice(&*self.store, &self.keys, &guard, &request)
                                    .await
                            }
                            Err(err) => Err(err),
                        };
                        match retry {
                            Ok(outcome) => {
                                let result = match outcome {
                                    CommitOutcome::Committed(r)
                                    | CommitOutcome::AlreadyCommitted(r) => r,
                                };
                                self.handoff_committed(&mut state, inode, ticket_no);
                                report.committed.push(result);
                                drop(state);
                                state = self.refresh_state(inode).await?;
                            }
                            Err(err) => {
                                let err = self
                                    .protect_failed_upload(
                                        &state,
                                        inode,
                                        ticket_no,
                                        err,
                                        &mut report,
                                    )
                                    .await;
                                self.mark_failed(&mut state, inode, ticket_no, err);
                            }
                        }
                    }
                    Err(err) => {
                        let fenced = matches!(err, WriteError::LeaseFence(_));
                        let err = self
                            .protect_failed_upload(&state, inode, ticket_no, err, &mut report)
                            .await;
                        self.mark_failed(&mut state, inode, ticket_no, err);
                        if fenced {
                            // The lease is gone: stop publishing and clean the
                            // rest of this writer's private state instead of
                            // leaving it half-published (CONS-004).
                            let cause =
                                WriteError::LeaseFence("writer lease lapsed during drain".into());
                            report.rolled_back =
                                Some(self.rollback_locked(&mut state, &cause).await?);
                        }
                    }
                }
            }
        }
        Ok(report)
    }

    /// Protect a failed operation's durable upload, folding a protection
    /// failure into the reported reason: a failure to protect must never be
    /// silent, because it would leave live objects collectable.
    async fn protect_failed_upload(
        &self,
        state: &OverlayState,
        inode: u64,
        ticket_no: u64,
        err: WriteError,
        report: &mut DrainReport,
    ) -> WriteError {
        match self
            .protect_uncommitted_upload(state, inode, ticket_no, &err)
            .await
        {
            Ok(Some(receipt)) => {
                report.orphaned.push(receipt);
                err
            }
            Ok(None) => err,
            Err(protection) => WriteError::Record(format!(
                "{err}; protecting the uploaded objects failed: {protection}"
            )),
        }
    }

    /// Record the blocks that landed before a dispatch was refused, so a
    /// rollback can protect them. The operation stays `Accepted`: a retry
    /// re-derives the upload instead of resuming a partial block set.
    async fn stash_partial_dispatch(&self, ticket: &Ticket, landed: Vec<CommittedBlock>) {
        if landed.is_empty() {
            return;
        }
        let mut state = self.state.lock().await;
        if let Some(op) = state.inodes.get_mut(&ticket.inode).and_then(|inode_state| {
            inode_state
                .pending
                .iter_mut()
                .find(|op| op.ticket == ticket.admission_ticket)
        }) {
            op.partial.extend(landed);
        }
    }

    /// Fold a publishing refusal into the private-state rollback the contract
    /// requires: a writer that lost its lease or its head must not leave
    /// half-published state behind (CONS-004/CONS-005). Only fence errors
    /// roll back; an ordinary conflict is re-derived by the caller.
    ///
    /// The refusal is returned unchanged so callers keep classifying it by
    /// variant; what the rollback cleaned up is reported by
    /// [`Self::last_rollback`]. A rollback that itself fails is folded into
    /// the error, because leaving durable uploads unprotected must never be
    /// silent (WRITE-006).
    async fn fenced(&self, err: WriteError) -> WriteError {
        if !matches!(
            err,
            WriteError::LeaseFence(_)
                | WriteError::StaleHeadGuard(_)
                | WriteError::DomainNotActive(_)
        ) {
            return err;
        }
        match self.rollback(&err).await {
            Ok(_) => err,
            Err(cleanup) => WriteError::Record(format!(
                "{err}; rolling back private state failed: {cleanup}"
            )),
        }
    }

    /// Fence this writer and clean every bit of its private state: stop the
    /// in-flight operations, drop the dirty references they hold, protect the
    /// objects they already uploaded, and forget the local mirrors so the
    /// next admission re-derives them from the store.
    ///
    /// Nothing here publishes: no head, extent, inode or mutation-result row
    /// is written, which is why [`RollbackReport::published_rows`] is always
    /// zero. A caller that has to stop a writer (a lapsed lease, an authority
    /// rollback) can therefore call this without a fence of its own.
    pub async fn rollback(&self, cause: &WriteError) -> Result<RollbackReport, WriteError> {
        let mut state = self.state.lock().await;
        self.rollback_locked(&mut state, cause).await
    }

    /// The last rollback this writer performed, if any.  A caller whose
    /// publish was refused inside [`Self::dispatch`] or
    /// [`Self::complete_upload`] reads the cleanup it could not see from the
    /// error alone (CONS-004).
    pub async fn last_rollback(&self) -> Option<RollbackReport> {
        self.state.lock().await.last_rollback.clone()
    }

    async fn rollback_locked(
        &self,
        state: &mut OverlayState,
        cause: &WriteError,
    ) -> Result<RollbackReport, WriteError> {
        let mut report = RollbackReport::default();
        let inode_ids: Vec<u64> = state.inodes.keys().copied().collect();
        for inode in inode_ids {
            let in_flight: Vec<(u64, u64, [u8; 16])> = state.inodes[&inode]
                .pending
                .iter()
                .filter(|op| matches!(op.state, OpState::Accepted | OpState::Uploaded(_)))
                .map(|op| (op.ticket, op.mutation_order, op.operation_id))
                .collect();
            for (ticket_no, mutation_order, operation_id) in in_flight {
                if let Some(receipt) = self
                    .protect_uncommitted_upload(&*state, inode, ticket_no, cause)
                    .await?
                {
                    report.protected.push(receipt);
                }
                let inode_state = state.inodes.get_mut(&inode).unwrap();
                inode_state.pending.retain(|op| op.ticket != ticket_no);
                let before = inode_state.dirty.len();
                inode_state.dirty.retain(|dirty| dirty.ticket != ticket_no);
                report.dropped_dirty += before - inode_state.dirty.len();
                report.stopped.push((
                    Ticket {
                        admission_ticket: ticket_no,
                        inode,
                        mutation_order,
                        operation_id,
                    },
                    cause.to_string(),
                ));
            }
            let drained = state
                .inodes
                .get(&inode)
                .map(|inode_state| inode_state.pending.is_empty())
                .unwrap_or(false);
            if drained {
                state.inodes.remove(&inode);
                state.baseline_sizes.remove(&inode);
                report.forgotten_inodes += 1;
            }
        }
        state.last_rollback = Some(report.clone());
        Ok(report)
    }

    async fn record_upload(&self, object_id: ObjectId) {
        self.state.lock().await.uploaded_objects.push(object_id);
    }

    /// Every object this overlay has uploaded, in upload order.  A
    /// metadata-only mutation adds exactly one control object here and no data
    /// block (WRITE-010).
    pub async fn uploaded_object_ids(&self) -> Vec<ObjectId> {
        self.state.lock().await.uploaded_objects.clone()
    }

    /// Hand off exactly this operation's dirty references — never a range
    /// clear, so newer dirty regions survive (spec 18 §5).
    fn handoff_committed(&self, state: &mut OverlayState, inode: u64, ticket_no: u64) {
        let inode_state = state.inodes.get_mut(&inode).unwrap();
        inode_state.dirty.retain(|d| d.ticket != ticket_no);
        if let Some(op) = inode_state
            .pending
            .iter_mut()
            .find(|op| op.ticket == ticket_no)
        {
            op.state = OpState::Committed;
        }
    }

    /// Record a permanent failure and block every later operation on the
    /// same inode (reported, not skipped — spec 18 §10). Reporting happens
    /// when the drain pops the terminal operations.
    fn mark_failed(&self, state: &mut OverlayState, inode: u64, ticket_no: u64, err: WriteError) {
        let reason = err.to_string();
        let inode_state = state.inodes.get_mut(&inode).unwrap();
        for op in inode_state.pending.iter_mut() {
            if op.ticket == ticket_no {
                op.state = OpState::Failed(reason.clone());
            } else if matches!(op.state, OpState::Accepted | OpState::Uploaded(_)) {
                op.state =
                    OpState::Blocked(format!("predecessor ticket {ticket_no} failed: {reason}"));
            }
        }
    }

    /// WRITE-006: a durable upload whose commit failed has objects that no head
    /// references yet.  Record their receipt in the protected orphan set
    /// before the failure is reported, so a cleaner cannot collect live data
    /// and the upload stays auditable until it is resolved.
    async fn protect_uncommitted_upload(
        &self,
        state: &OverlayState,
        inode: u64,
        ticket_no: u64,
        err: &WriteError,
    ) -> Result<Option<OrphanReceipt>, WriteError> {
        let Some(inode_state) = state.inodes.get(&inode) else {
            return Ok(None);
        };
        let Some(op) = inode_state.pending.iter().find(|op| op.ticket == ticket_no) else {
            return Ok(None);
        };
        // The receipts container is what makes the upload auditable; without it
        // the data blocks are still durable, so both cases are protected. A
        // dispatch that was refused half-way has durable blocks but no
        // `Uploaded` state, and they must be protected just the same.
        let (step, blocks, receipts_root, receipts_object) = match &op.state {
            OpState::Uploaded(uploaded) => match &uploaded.receipts {
                Some((root, registration)) => (
                    KvStep::Commit,
                    uploaded.blocks.clone(),
                    Some(root.clone()),
                    Some(registration.object_ref.clone()),
                ),
                None if !uploaded.blocks.is_empty() => {
                    (KvStep::DataUploaded, uploaded.blocks.clone(), None, None)
                }
                None => return Ok(None),
            },
            _ if !op.partial.is_empty() => (KvStep::DataUploaded, op.partial.clone(), None, None),
            _ => return Ok(None),
        };
        let mut objects: Vec<ObjectRef> = blocks
            .iter()
            .map(|block| block.object_ref.clone())
            .collect();
        if let Some(object) = receipts_object {
            objects.push(object);
        }
        let mut reason = err.to_string();
        reason.truncate(MAX_ORPHAN_REASON);
        let receipt = OrphanReceipt::after_kv_failure(
            op.operation_id,
            self.params.workspace_id,
            self.params.domain_id,
            step,
            reason,
            receipts_root,
            objects,
        )?;
        let Some(receipt) = receipt else {
            return Ok(None);
        };
        record_orphan_receipt(&*self.store, &self.keys, &receipt).await?;
        Ok(Some(receipt))
    }

    /// Number of operations for `inode` that have not reached a terminal
    /// state.  After a successful drain this must be zero: a non-zero count
    /// means the caller would otherwise be dropping uncommitted work.
    pub async fn incomplete_count(&self, inode: u64) -> usize {
        let state = self.state.lock().await;
        state
            .inodes
            .get(&inode)
            .map(|inode_state| {
                inode_state
                    .pending
                    .iter()
                    .filter(|op| matches!(op.state, OpState::Accepted | OpState::Uploaded(_)))
                    .count()
            })
            .unwrap_or(0)
    }

    /// Re-drive operations a previous attempt left half-way: an accepted
    /// operation is dispatched again (registration and upload are idempotent
    /// for one identity) and an upload without receipts is completed.  This
    /// is how a retry waits for the dependency chain instead of skipping it
    /// (spec 18 §10); admission order is unchanged because the same tickets
    /// are reused.
    pub async fn retry_incomplete(&self, inode: u64) -> Result<usize, WriteError> {
        let candidates: Vec<Ticket> = {
            let state = self.state.lock().await;
            let Some(inode_state) = state.inodes.get(&inode) else {
                return Ok(0);
            };
            inode_state
                .pending
                .iter()
                .filter(|op| matches!(op.state, OpState::Accepted | OpState::Uploaded(_)))
                .map(|op| Ticket {
                    admission_ticket: op.ticket,
                    inode,
                    mutation_order: op.mutation_order,
                    operation_id: op.operation_id,
                })
                .collect()
        };

        let mut driven = 0;
        for ticket in candidates {
            let (accepted, needs_receipts) = {
                let state = self.state.lock().await;
                let Some(op) = state.inodes.get(&inode).and_then(|inode_state| {
                    inode_state
                        .pending
                        .iter()
                        .find(|op| op.ticket == ticket.admission_ticket)
                }) else {
                    continue;
                };
                match &op.state {
                    OpState::Accepted => (true, true),
                    OpState::Uploaded(uploaded) => (false, uploaded.receipts.is_none()),
                    _ => (false, false),
                }
            };
            if accepted {
                self.dispatch(&ticket).await?;
            }
            if needs_receipts {
                self.complete_upload(&ticket).await?;
            }
            if accepted || needs_receipts {
                driven += 1;
            }
        }
        Ok(driven)
    }

    /// Re-read the authoritative head and inode view after a commit and
    /// return the re-locked overlay state.
    async fn refresh_state(
        &self,
        inode: u64,
    ) -> Result<tokio::sync::MutexGuard<'_, OverlayState>, WriteError> {
        let head = self.read_head().await?;
        let view =
            read_inode_view(&*self.store, &self.keys, &self.params.workspace_id, inode).await?;
        let mut state = self.state.lock().await;
        state.head = Some(head);
        let entry = state.inodes.entry(inode).or_default();
        entry.data = view.data;
        entry.extents = view.extents;
        Ok(state)
    }

    async fn register_under_current_head(
        &self,
        object_ref: &ObjectRef,
        plan_hash: Hash32,
    ) -> Result<UploadReservation, WriteError> {
        // The head advances with every commit, so the guard is re-derived
        // from the store until the registration lands. The state lock is
        // only held to mirror the head that was registered under.
        for _ in 0..3 {
            let head = self.read_head().await?;
            // A fenced (expired or superseded) lease never reaches the
            // transaction: no registry row, no inventory entry and no domain
            // counter moves on behalf of a writer that lost its lease.
            let guard = self.publish_guard(&head)?;
            match register_upload(
                &*self.store,
                &self.keys,
                &guard,
                &self.params.domain_id,
                object_ref.clone(),
                plan_hash,
            )
            .await
            {
                Ok(reservation) => {
                    let mut state = self.state.lock().await;
                    state.head = Some(head);
                    return Ok(reservation);
                }
                Err(err) if err.is_retryable() => continue,
                Err(err) => return Err(err),
            }
        }
        Err(WriteError::Conflict(
            "upload registration kept conflicting with head movement".into(),
        ))
    }

    async fn read_head(&self) -> Result<HeadState, WriteError> {
        let bytes = self
            .store
            .get(&self.keys.head(&self.params.workspace_id))
            .await?
            .ok_or_else(|| WriteError::Record("workspace head not found".into()))?;
        Ok(HeadState::decode(&bytes)?)
    }

    pub(crate) fn control_store(&self) -> &dyn ControlStore {
        &*self.store
    }

    pub(crate) fn object_sink(&self) -> &dyn ObjectSink {
        &*self.sink
    }

    pub(crate) fn keys(&self) -> &Keys {
        &self.keys
    }

    pub(crate) fn params(&self) -> &OverlayParams {
        &self.params
    }

    /// Record the immutable workspace size used when planning the first
    /// native mutation for an inode. The value is request-local metadata and
    /// is never persisted as a separate control-plane record.
    pub(crate) async fn set_baseline_size(&self, inode: u64, size: u64) {
        self.state.lock().await.baseline_sizes.insert(inode, size);
    }
}

enum Either<L, R> {
    Left(L),
    Right(R),
}

fn dirty_range(spec: &MutationSpec, committed_size: u64) -> (u64, u64) {
    match spec {
        MutationSpec::Write { offset, data } => (*offset, data.len() as u64),
        MutationSpec::Truncate { new_size } => {
            if *new_size < committed_size {
                (*new_size, committed_size - *new_size)
            } else {
                (committed_size, *new_size - committed_size)
            }
        }
        MutationSpec::PunchHole { offset, len } => (*offset, *len),
        // Metadata only: nothing to hand off to a reader.
        MutationSpec::SetAttributes { .. } => (0, 0),
        // A replacement invalidates the whole current file; the caller widens
        // this to the published size, which it knows from the plan.
        MutationSpec::ReplaceInode { .. } => (0, committed_size),
    }
}

fn payload_digest(spec: &MutationSpec, replacement: Option<&Replacement>) -> Hash32 {
    let mut hasher = Sha256::new();
    match spec {
        MutationSpec::Write { offset, data } => {
            hasher.update(b"write");
            hasher.update(offset.to_be_bytes());
            hasher.update((data.len() as u64).to_be_bytes());
            hasher.update(data.as_slice());
        }
        MutationSpec::Truncate { new_size } => {
            hasher.update(b"truncate");
            hasher.update(new_size.to_be_bytes());
        }
        MutationSpec::PunchHole { offset, len } => {
            hasher.update(b"punch");
            hasher.update(offset.to_be_bytes());
            hasher.update(len.to_be_bytes());
        }
        MutationSpec::SetAttributes {
            mode,
            uid,
            gid,
            ctime_ns,
        } => {
            hasher.update(b"attributes");
            hasher.update(mode.to_be_bytes());
            hasher.update(uid.to_be_bytes());
            hasher.update(gid.to_be_bytes());
            hasher.update(ctime_ns.to_be_bytes());
        }
        // The digest names the published content, not just the intent: two
        // rename-overs of the same source at different head states must not
        // collide with the same payload (WRITE-012).
        MutationSpec::ReplaceInode { source_inode } => {
            hasher.update(b"replace");
            hasher.update(source_inode.to_be_bytes());
            if let Some(plan) = replacement {
                hasher.update(plan.size.to_be_bytes());
                for item in &plan.carried {
                    hasher.update(item.logical_offset.to_be_bytes());
                    hasher.update(item.extent.logical_len.to_be_bytes());
                    hasher.update(item.extent.slice_id);
                    hasher.update(item.extent.first_block.to_be_bytes());
                    hasher.update(item.extent.block_count.to_be_bytes());
                    for block in &item.blocks {
                        hasher.update(block.object_ref.object_id);
                    }
                }
            }
        }
    }
    hasher.finalize().into()
}

fn operation_id_for(
    volume_id: &[u8; 16],
    workspace_id: &[u8; 16],
    inode: u64,
    ticket: u64,
) -> [u8; 16] {
    let mut hasher = Sha256::new();
    hasher.update(volume_id);
    hasher.update(workspace_id);
    hasher.update(inode.to_be_bytes());
    hasher.update(ticket.to_be_bytes());
    hasher.finalize().as_slice()[..16].try_into().unwrap()
}

fn slice_id_for(operation_id: &[u8; 16]) -> [u8; 16] {
    let mut hasher = Sha256::new();
    hasher.update(operation_id);
    hasher.update(b"slice");
    hasher.finalize().as_slice()[..16].try_into().unwrap()
}
