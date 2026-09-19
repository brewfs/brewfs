//! PR07V contract tests - a fenced writer publishes nothing.
//!
//! Two hazards, both from spec 07 section 2 / spec 20 section 2:
//!
//! * CONS-004 - a writer whose lease expires or is superseded *while a read
//!   or a commit captures its state* must be fenced, and the coordinator's
//!   rollback (including an authority rollback that takes the ownership
//!   domain away) must stop publishing and clean up every bit of private
//!   state instead of leaving half-uploaded objects behind.
//! * CONS-005 - a *second* writer that bypasses the first writer's local
//!   gate must be refused by the durable head guard.  An in-process `Mutex`
//!   is not a fence: it cannot be seen by another writer at all.
//!
//! Every test drives the real overlay against the in-process control store.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use super::error::WriteError;
use super::keys::Keys;
use super::lease::{LeaseGrant, StepClock};
use super::memory::MemoryControlStore;
use super::orphan_receipt::{KvStep, scan_orphan_receipts};
use super::overlay::{MutationSpec, OverlayParams, WriteOverlay, ensure_workspace_head};
use super::receipts::{MemorySink, ObjectSink};
use super::records::HeadState;
use super::store::{ControlStore, Txn};
use crate::native_base::wire::bnct::{ControlRecord, DomainState, OwnershipDomain};
use crate::native_base::wire::refs::ObjectRef;

const INODE: u64 = 7;
const BLOCK: u64 = 4096;

struct Writer {
    store: Arc<dyn ControlStore>,
    keys: Keys,
    params: OverlayParams,
    overlay: WriteOverlay,
}

fn random_id() -> [u8; 16] {
    use rand::RngCore;
    let mut id = [0u8; 16];
    rand::rng().fill_bytes(&mut id);
    id
}

fn params(block_size: u64) -> OverlayParams {
    OverlayParams {
        volume_id: random_id(),
        workspace_id: random_id(),
        domain_id: random_id(),
        writer_generation: 1,
        block_size,
    }
}

/// A lease owned by `generation`, granted at `granted_at_ns` for `ttl_ns`.
fn grant(generation: u64, granted_at_ns: u64, ttl_ns: u64, params: &OverlayParams) -> LeaseGrant {
    LeaseGrant {
        workspace_id: params.workspace_id,
        owner_generation: generation,
        granted_at_ns,
        ttl_ns,
    }
}

fn write_spec(offset: u64, byte: u8, len: usize) -> MutationSpec {
    MutationSpec::Write {
        offset,
        data: Arc::new(vec![byte; len]),
    }
}

/// A block-aligned write whose consecutive blocks differ, so their
/// content-addressed object ids stay distinct.
fn write_spec_blocks(offset: u64, bytes: &[u8]) -> MutationSpec {
    let mut data = Vec::with_capacity(bytes.len() * BLOCK as usize);
    for byte in bytes {
        data.extend(std::iter::repeat_n(*byte, BLOCK as usize));
    }
    MutationSpec::Write {
        offset,
        data: Arc::new(data),
    }
}

/// Build one writer view over `store`.  Two writers built with the same
/// `params` share the whole volume namespace and *nothing* in process: each
/// has its own state gate and its own object sink.
async fn writer_on(
    store: Arc<dyn ControlStore>,
    params: OverlayParams,
    sink: Arc<dyn ObjectSink>,
    lease: Option<(LeaseGrant, Arc<StepClock>)>,
) -> Writer {
    let keys = Keys::new(&params.volume_id);
    ensure_workspace_head(&*store, &keys, &params, 3)
        .await
        .unwrap();
    let mut overlay = WriteOverlay::new(store.clone(), sink, params.clone());
    if let Some((grant, clock)) = lease {
        overlay = overlay.with_writer_lease(grant, clock);
    }
    overlay.ensure_domain(random_id()).await.unwrap();
    Writer {
        store,
        keys,
        params,
        overlay,
    }
}

async fn head_of(store: &Arc<dyn ControlStore>, keys: &Keys, workspace_id: &[u8; 16]) -> HeadState {
    HeadState::decode(&store.get(&keys.head(workspace_id)).await.unwrap().unwrap()).unwrap()
}

async fn all_rows(store: &Arc<dyn ControlStore>, volume_id: &[u8; 16]) -> Vec<(Vec<u8>, Vec<u8>)> {
    let volume_hex: String = volume_id.iter().map(|b| format!("{b:02x}")).collect();
    store
        .scan(format!("nb2/{volume_hex}/").as_bytes())
        .await
        .unwrap()
}

/// The rows a reader of the volume can see: everything except the write
/// domain's private registry (`dom/`, `reg/`, `obj/`, `inv/`) and its
/// protected-upload receipts (`ufo/`) -- exactly the state a fenced writer
/// may still move while it publishes nothing.
async fn published_rows(
    store: &Arc<dyn ControlStore>,
    volume_id: &[u8; 16],
) -> Vec<(Vec<u8>, Vec<u8>)> {
    all_rows(store, volume_id)
        .await
        .into_iter()
        .filter(|(key, _)| {
            let key = String::from_utf8_lossy(key);
            !["/dom/", "/reg/", "/obj/", "/inv/", "/ufo/"]
                .iter()
                .any(|p| key.contains(p))
        })
        .collect()
}

async fn set_domain_state(
    store: &Arc<dyn ControlStore>,
    keys: &Keys,
    domain_id: &[u8; 16],
    state: DomainState,
) {
    let key = keys.domain(domain_id);
    let bytes = store.get(&key).await.unwrap().unwrap();
    let mut domain = match ControlRecord::decode(&bytes).unwrap() {
        ControlRecord::OwnershipDomain(domain) => domain,
        other => panic!(
            "expected a domain record, got kind {}",
            other.kind().as_u16()
        ),
    };
    domain.state = state;
    let _: OwnershipDomain = domain.clone();
    store
        .run(
            Txn::new()
                .check_bytes(key.clone(), bytes)
                .put(key, ControlRecord::OwnershipDomain(domain).encode()),
        )
        .await
        .unwrap();
}

/// An object sink whose first successful upload makes the writer's lease
/// lapse: the fence then lands *between* the blocks of one operation, which
/// is the case a rollback has to protect partially dispatched uploads for.
struct LeaseLapsingSink {
    inner: MemorySink,
    clock: Arc<StepClock>,
    deadline_ns: u64,
    lapsed: AtomicBool,
}

#[async_trait::async_trait]
impl ObjectSink for LeaseLapsingSink {
    async fn put(&self, object: &ObjectRef, bytes: &[u8]) -> anyhow::Result<()> {
        self.inner.put(object, bytes).await?;
        if !self.lapsed.swap(true, Ordering::SeqCst) {
            self.clock.advance_to(self.deadline_ns);
        }
        Ok(())
    }

    async fn get(&self, object: &ObjectRef) -> anyhow::Result<Vec<u8>> {
        self.inner.get(object).await
    }
}

// ---------------------------------------------------------------------------
// CONS-004 - a lease that is gone stops the write before it publishes
// ---------------------------------------------------------------------------

#[tokio::test]
async fn an_expired_or_superseded_lease_stops_a_dispatch_before_any_row_is_written() {
    let store: Arc<dyn ControlStore> = Arc::new(MemoryControlStore::new());
    let params = params(BLOCK);
    let clock = Arc::new(StepClock::backend(1_000_000));
    let writer = writer_on(
        store.clone(),
        params.clone(),
        Arc::new(MemorySink::default()),
        Some((grant(1, 1_000_000, 5_000_000, &params), clock.clone())),
    )
    .await;

    let ticket = writer
        .overlay
        .accept(INODE, write_spec(0, 0xA1, BLOCK as usize))
        .await
        .unwrap();
    let before = all_rows(&store, &params.volume_id).await;

    // The lease expires on backend time while the operation is in flight.
    clock.advance_to(6_000_000);
    let error = writer.overlay.dispatch(&ticket).await.unwrap_err();
    assert!(
        matches!(error, WriteError::LeaseFence(_)),
        "an expired lease must fence the dispatch, got {error}"
    );
    assert_eq!(
        writer.overlay.uploaded_object_ids().await.len(),
        0,
        "a fenced dispatch uploads no object at all"
    );
    assert_eq!(
        all_rows(&store, &params.volume_id).await,
        before,
        "a fenced dispatch writes no row: no registration, no inventory entry, no domain bump"
    );

    // The writer can be stopped cleanly: the fence already cleaned its
    // private state, so nothing is left for a cleaner to find later.
    let report = writer
        .overlay
        .last_rollback()
        .await
        .expect("a rollback ran");
    assert!(report.stopped_publishing());
    assert_eq!(report.published_rows, 0);
    assert_eq!(report.stopped.len(), 1);
    assert_eq!(report.dropped_dirty, 1);
    assert!(report.protected.is_empty(), "nothing was uploaded");
    assert_eq!(report.forgotten_inodes, 1);
    assert_eq!(writer.overlay.incomplete_count(INODE).await, 0);
    assert!(writer.overlay.capture().await.dirty.is_empty());

    // A superseded generation is fenced just as hard, before the clock is
    // even consulted.
    let other = writer_on(
        store.clone(),
        params.clone(),
        Arc::new(MemorySink::default()),
        Some((grant(2, 1_000_000, 60_000_000, &params), clock.clone())),
    )
    .await;
    let ticket = other
        .overlay
        .accept(INODE, write_spec(0, 0xB2, BLOCK as usize))
        .await
        .unwrap();
    let error = other.overlay.dispatch(&ticket).await.unwrap_err();
    assert!(
        matches!(error, WriteError::LeaseFence(_)),
        "a superseded lease must fence the dispatch, got {error}"
    );
    assert_eq!(
        all_rows(&store, &params.volume_id).await,
        before,
        "a superseded writer publishes nothing either"
    );
}

#[tokio::test]
async fn a_fence_between_two_blocks_protects_the_uploads_that_already_landed() {
    let store: Arc<dyn ControlStore> = Arc::new(MemoryControlStore::new());
    let params = params(BLOCK);
    let clock = Arc::new(StepClock::backend(1_000_000));
    let sink = Arc::new(LeaseLapsingSink {
        inner: MemorySink::default(),
        clock: clock.clone(),
        deadline_ns: 6_000_000,
        lapsed: AtomicBool::new(false),
    });
    let writer = writer_on(
        store.clone(),
        params.clone(),
        sink.clone(),
        Some((grant(1, 1_000_000, 5_000_000, &params), clock.clone())),
    )
    .await;

    // Two blocks in one operation: the first upload lands, then the sink
    // moves backend time past the deadline, so the second block is refused.
    let ticket = writer
        .overlay
        .accept(INODE, write_spec(0, 0xC3, 2 * BLOCK as usize))
        .await
        .unwrap();
    let before = published_rows(&store, &params.volume_id).await;
    let error = writer.overlay.dispatch(&ticket).await.unwrap_err();
    assert!(matches!(error, WriteError::LeaseFence(_)), "got {error}");

    // The block that landed is durable and unpublished, so it is protected
    // exactly once instead of being left collectable (WRITE-006).
    let report = writer
        .overlay
        .last_rollback()
        .await
        .expect("a rollback ran");
    assert!(report.stopped_publishing());
    assert_eq!(report.stopped.len(), 1);
    assert_eq!(report.dropped_dirty, 1);
    assert_eq!(report.protected.len(), 1);
    let uploaded = writer.overlay.uploaded_object_ids().await;
    assert_eq!(uploaded.len(), 1, "only the first block was uploaded");
    let protected = report.protected[0].protected_ids();
    assert_eq!(report.protected[0].step, KvStep::DataUploaded);
    assert!(
        protected.contains(&uploaded[0]),
        "the rolled-back receipt protects the uploaded block"
    );

    // Nothing visible was published, and the receipt is on record for the
    // resolver so the object cannot be collected.
    assert_eq!(published_rows(&store, &params.volume_id).await, before);
    let receipts = scan_orphan_receipts(&*store, &writer.keys).await.unwrap();
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].operation_id, report.protected[0].operation_id);
}

#[tokio::test]
async fn an_authority_rollback_stops_the_writer_and_cleans_all_private_state() {
    let store: Arc<dyn ControlStore> = Arc::new(MemoryControlStore::new());
    let params = params(BLOCK);
    let clock = Arc::new(StepClock::backend(1_000_000));
    let writer = writer_on(
        store.clone(),
        params.clone(),
        Arc::new(MemorySink::default()),
        Some((grant(1, 1_000_000, 600_000_000, &params), clock.clone())),
    )
    .await;

    // A completed upload is in flight: everything is durable except the
    // commit, and the caller still holds a capture over it.
    let ticket = writer
        .overlay
        .accept(INODE, write_spec_blocks(0, &[0xD4, 0xD5]))
        .await
        .unwrap();
    writer.overlay.dispatch(&ticket).await.unwrap();
    writer.overlay.complete_upload(&ticket).await.unwrap();
    let captured = writer.overlay.capture().await;
    assert_eq!(captured.dirty.len(), 1);
    let before = published_rows(&store, &params.volume_id).await;

    // The authority rolls the ownership domain back: this writer may no
    // longer publish, so the coordinator rolls its private state back.
    set_domain_state(
        &store,
        &writer.keys,
        &params.domain_id,
        DomainState::Quarantined,
    )
    .await;
    let cause = WriteError::DomainNotActive("authority rolled the domain back".into());
    let report = writer.overlay.rollback(&cause).await.unwrap();
    assert!(report.stopped_publishing());
    assert_eq!(report.stopped.len(), 1);
    assert_eq!(report.dropped_dirty, 1);
    assert_eq!(report.forgotten_inodes, 1);
    assert_eq!(report.protected.len(), 1, "the upload is protected");
    let protected = report.protected[0].protected_ids();
    assert_eq!(
        protected.len(),
        3,
        "two data blocks plus the receipts container are protected"
    );
    assert_eq!(report.protected[0].step, KvStep::Commit);

    // "Stops publishing" is literal: the rollback wrote no visible row, and
    // the capture the caller already holds keeps its own dirty references.
    assert_eq!(published_rows(&store, &params.volume_id).await, before);
    assert_eq!(captured.dirty.len(), 1);
    assert_eq!(writer.overlay.incomplete_count(INODE).await, 0);
    assert!(writer.overlay.capture().await.dirty.is_empty());

    // A later admission re-derives from the store and is refused as well:
    // the domain is gone, and its own private state is cleaned up too.
    let retry = writer
        .overlay
        .accept(INODE, write_spec(0, 0xE5, BLOCK as usize))
        .await
        .unwrap();
    let after_rollback = published_rows(&store, &params.volume_id).await;
    let error = writer.overlay.dispatch(&retry).await.unwrap_err();
    assert!(
        matches!(error, WriteError::DomainNotActive(_)),
        "a rolled-back domain cannot publish, got {error}"
    );
    assert_eq!(
        published_rows(&store, &params.volume_id).await,
        after_rollback
    );
    let second = writer.overlay.last_rollback().await.unwrap();
    assert_eq!(second.stopped.len(), 1);
    assert!(second.protected.is_empty(), "nothing new was uploaded");
}

// ---------------------------------------------------------------------------
// CONS-005 - the durable head guard, not a local mutex, is the fence
// ---------------------------------------------------------------------------

#[tokio::test]
async fn another_writer_bypassing_the_local_gate_is_refused_by_the_head_guard() {
    let store: Arc<dyn ControlStore> = Arc::new(MemoryControlStore::new());
    let params = params(BLOCK);
    // Two writers over one store.  They share no in-process gate at all: each
    // has its own state mutex, and neither can see the other's lock, which is
    // precisely why the lock cannot be the fence.
    let first = writer_on(
        store.clone(),
        params.clone(),
        Arc::new(MemorySink::default()),
        None,
    )
    .await;
    let bypassing = writer_on(
        store.clone(),
        params.clone(),
        Arc::new(MemorySink::default()),
        None,
    )
    .await;
    assert!(first.overlay.lease().is_none());
    assert!(bypassing.overlay.lease().is_none());

    // The bypassing writer registers, dispatches and completes its operation
    // under the head it read.  Its own gate is unlocked the whole time.
    let bypassing_ticket = bypassing
        .overlay
        .accept(INODE, write_spec(0, 0xB1, BLOCK as usize))
        .await
        .unwrap();
    bypassing.overlay.dispatch(&bypassing_ticket).await.unwrap();
    bypassing
        .overlay
        .complete_upload(&bypassing_ticket)
        .await
        .unwrap();

    // The first writer commits on another inode; cross-inode commits do not
    // wait for each other, so its commit lands and moves the head.
    let first_ticket = first
        .overlay
        .accept(INODE + 1, write_spec(0, 0xA1, BLOCK as usize))
        .await
        .unwrap();
    first.overlay.dispatch(&first_ticket).await.unwrap();
    first.overlay.complete_upload(&first_ticket).await.unwrap();
    let committed = first.overlay.drain().await.unwrap();
    assert_eq!(committed.committed.len(), 1);
    let head_after_winner = head_of(&store, &first.keys, &params.workspace_id).await;
    assert_eq!(head_after_winner.head.commit_seq, 1);

    // The bypassing writer's commit is refused by the head guard: the head it
    // registered under is gone, and no local lock of its own could have known.
    let refused = bypassing.overlay.drain().await.unwrap();
    assert!(refused.committed.is_empty());
    assert_eq!(refused.failed.len(), 1);
    assert!(
        refused.failed[0].1.contains("stale head guard"),
        "the refusal is the durable guard: {}",
        refused.failed[0].1
    );
    assert_eq!(
        head_of(&store, &first.keys, &params.workspace_id).await,
        head_after_winner,
        "the refused writer does not move the head"
    );
    assert!(
        store
            .scan(&first.keys.extents_prefix(&params.workspace_id, INODE))
            .await
            .unwrap()
            .is_empty(),
        "the refused writer publishes no extent"
    );
    assert_eq!(
        refused.orphaned.len(),
        1,
        "its durable upload is protected instead of left collectable"
    );

    // The fence is durable and recoverable, not a ban: a writer that
    // re-derives the head from the store publishes the same bytes.
    let rederived = writer_on(
        store.clone(),
        params.clone(),
        Arc::new(MemorySink::default()),
        None,
    )
    .await;
    let ticket = rederived
        .overlay
        .accept(INODE, write_spec(0, 0xB1, BLOCK as usize))
        .await
        .unwrap();
    rederived.overlay.dispatch(&ticket).await.unwrap();
    rederived.overlay.complete_upload(&ticket).await.unwrap();
    let committed = rederived.overlay.drain().await.unwrap();
    assert_eq!(committed.committed.len(), 1);
    let head_after_recovery = head_of(&store, &first.keys, &params.workspace_id).await;
    assert_eq!(head_after_recovery.head.commit_seq, 2);
    assert!(
        !store
            .scan(&first.keys.extents_prefix(&params.workspace_id, INODE))
            .await
            .unwrap()
            .is_empty(),
        "the re-derived writer publishes the extent the fenced one could not"
    );
}
