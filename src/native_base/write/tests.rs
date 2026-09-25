//! PR04 contract tests — the write pipeline against every control-store
//! backend.
//!
//! These are the executable contracts of spec 07 §2/§12, spec 18 §10 and
//! spec 20 §6: the atomic commit, the per-inode ordering gate (reordered
//! and late uploads cannot land), torn commits leaving no partial state,
//! OperationId idempotency, registry identity uniqueness, dirty handoff
//! and predecessor blocking.
//!
//! Every store-backed test is a *scenario* taking an
//! `Arc<dyn ControlStore>`: the `memory_backend` module below runs them all
//! in the normal test pass, and `tests_redis` / `tests_tikv` run the same
//! list against real instances behind `--ignored` integration gates. Each
//! scenario gets a random volume/workspace/domain, so scenarios never
//! interfere on a shared backend.

use std::sync::Arc;

use sha2::{Digest, Sha256};

use super::commit::{
    CommitOutcome, CommitRequest, Mutation, commit_uploaded_slice, plan_extent_change,
};
use super::domain::{HeadGuard, mark_dispatched, register_upload};
use super::error::WriteError;
use super::keys::Keys;
use super::lease::{FixedClock, LeaseFence, LeaseGrant};
use super::memory::MemoryControlStore;
use super::overlay::{MutationSpec, OverlayParams, WriteOverlay, ensure_workspace_head};
use super::receipts::{MemorySink, ReceiptEntry, ReceiptSet, build_receipts_container};
use super::records::{ExtentKind, HeadPlacement, HeadState, NativeExtent};
use super::replace::plan_rename_over;
use super::store::{ControlStore, Txn};
use crate::native_base::wire::bnct::{ControlRecord, DomainState};
use crate::native_base::wire::page::{IndexPage, PageBody};
use crate::native_base::wire::refs::{Hash32, ObjectId, RootRef};

const INODE: u64 = 7;

struct Env {
    store: Arc<dyn ControlStore>,
    keys: Keys,
    params: OverlayParams,
    overlay: WriteOverlay,
    /// The overlay's object sink, readable so a test can read back what a
    /// commit published (e.g. a receipts container).
    sink: Arc<MemorySink>,
}

fn random_id() -> [u8; 16] {
    use rand::RngCore;
    let mut id = [0u8; 16];
    rand::rng().fill_bytes(&mut id);
    id
}

async fn env_on(store: Arc<dyn ControlStore>, block_size: u64) -> Env {
    let params = OverlayParams {
        volume_id: random_id(),
        workspace_id: random_id(),
        domain_id: random_id(),
        writer_generation: 1,
        block_size,
    };
    let keys = Keys::new(&params.volume_id);
    ensure_workspace_head(&*store, &keys, &params, 3)
        .await
        .unwrap();
    let sink = Arc::new(MemorySink::default());
    let overlay = WriteOverlay::new(store.clone(), sink.clone(), params.clone());
    overlay.ensure_domain(random_id()).await.unwrap();
    Env {
        store,
        keys,
        params,
        overlay,
        sink,
    }
}

async fn read_head(env: &Env) -> HeadState {
    HeadState::decode(
        &env.store
            .get(&env.keys.head(&env.params.workspace_id))
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap()
}

async fn snapshot(env: &Env) -> Vec<(Vec<u8>, Vec<u8>)> {
    // The whole volume namespace — nothing else, so shared-backend runs of
    // other scenarios never leak into a torn-commit comparison.
    let volume_hex: String = env
        .params
        .volume_id
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    env.store
        .scan(format!("nb2/{volume_hex}/").as_bytes())
        .await
        .unwrap()
}

fn digest(bytes: &[u8]) -> Hash32 {
    Sha256::digest(bytes).into()
}

/// Build + register + dispatch an (empty) receipts container for a
/// commit-level request, the way the overlay's completion step does.
async fn receipts_for(
    env: &Env,
    operation_id: &[u8; 16],
) -> (RootRef, super::commit::VerifiedObject) {
    let built = build_receipts_container(
        &env.params.volume_id,
        &Sha256::digest([operation_id.as_slice(), b"receipts"].concat()).as_slice()[..16]
            .try_into()
            .unwrap(),
        &ReceiptSet::default(),
    );
    let head = read_head(env).await;
    register_upload(
        &*env.store,
        &env.keys,
        &HeadGuard {
            workspace_id: env.params.workspace_id,
            expected_head: head,
        },
        &env.params.domain_id,
        built.root.object.clone(),
        built.root.object.full_hash,
    )
    .await
    .unwrap();
    let registration = mark_dispatched(
        &*env.store,
        &env.keys,
        &env.params.domain_id,
        &built.root.object.object_id,
    )
    .await
    .unwrap();
    (
        built.root.clone(),
        super::commit::VerifiedObject {
            object_ref: built.root.object.clone(),
            registration,
        },
    )
}

/// A commit-level truncate request (no data blocks needed).
async fn truncate_request(
    env: &Env,
    operation_id: &[u8; 16],
    order: u64,
    new_size: u64,
) -> CommitRequest {
    let (receipts, receipts_registration) = receipts_for(env, operation_id).await;
    let mut payload = b"truncate".to_vec();
    payload.extend_from_slice(&new_size.to_be_bytes());
    CommitRequest {
        operation_id: *operation_id,
        payload_digest: digest(&payload),
        inode: INODE,
        mutation_order: order,
        mutation: Mutation::Truncate { new_size },
        receipts,
        receipts_registration,
        block_size: env.params.block_size,
        baseline_size: 0,
        domain_id: env.params.domain_id,
    }
}

async fn guard_for(env: &Env) -> HeadGuard {
    HeadGuard {
        workspace_id: env.params.workspace_id,
        expected_head: read_head(env).await,
    }
}

fn write_spec(offset: u64, len: usize) -> MutationSpec {
    MutationSpec::Write {
        offset,
        data: Arc::new(vec![0xa5; len]),
    }
}

/// A block-aligned write of `len` copies of `byte`.
fn write_spec_of(offset: u64, byte: u8, len: usize) -> MutationSpec {
    MutationSpec::Write {
        offset,
        data: Arc::new(vec![byte; len]),
    }
}

/// Every object an inode's data extents reference, followed through the
/// placement rows exactly as a reader would.
async fn extent_objects(env: &Env, inode: u64) -> std::collections::BTreeSet<ObjectId> {
    let view =
        super::commit::read_inode_view(&*env.store, &env.keys, &env.params.workspace_id, inode)
            .await
            .unwrap();
    let mut objects = std::collections::BTreeSet::new();
    for extent in view.extents.values() {
        if extent.kind != ExtentKind::Data {
            continue;
        }
        for index in 0..extent.block_count {
            let bytes = env
                .store
                .get(
                    &env.keys
                        .placement(&extent.slice_id, extent.first_block + index),
                )
                .await
                .unwrap()
                .expect("a data extent's block has a placement row");
            match HeadPlacement::decode(&bytes).unwrap() {
                HeadPlacement::Loose { object_id, .. } => {
                    objects.insert(object_id);
                }
            }
        }
    }
    objects
}

/// Read back the receipt entries of the container a mutation result points at.
fn receipts_of(sink: &MemorySink, root: &RootRef) -> Vec<ReceiptEntry> {
    let objects = sink.0.lock().unwrap();
    let bytes = objects
        .get(&root.object.object_id)
        .expect("the commit's receipts container is uploaded");
    let start = root.address.offset as usize;
    let end = start + root.address.stored_len as usize;
    let page = IndexPage::decode(&bytes[start..end]).expect("the receipts root is a leaf page");
    match page.body {
        PageBody::Leaf(entries) => {
            assert_eq!(entries.len(), 1);
            ReceiptSet::decode(&entries[0].value)
                .expect("the receipts set decodes")
                .entries
        }
        _ => panic!("the receipts root must be a leaf"),
    }
}

// ---------------------------------------------------------------------------
// The atomic commit.
// ---------------------------------------------------------------------------

pub(crate) async fn commit_writes_extent_binding_placement_inode_and_head_together(
    store: Arc<dyn ControlStore>,
) {
    let env = env_on(store, 64).await;
    let ticket = env.overlay.accept(INODE, write_spec(0, 128)).await.unwrap();
    assert_eq!(ticket.mutation_order, 1);
    env.overlay.dispatch(&ticket).await.unwrap();
    env.overlay.complete_upload(&ticket).await.unwrap();
    let report = env.overlay.drain().await.unwrap();
    assert_eq!(report.committed.len(), 1);
    assert!(report.failed.is_empty() && report.blocked.is_empty());

    // One data extent covering [0, 128) with two blocks.
    let view =
        super::commit::read_inode_view(&*env.store, &env.keys, &env.params.workspace_id, INODE)
            .await
            .unwrap();
    assert!(view.had_record);
    assert_eq!(view.data.size, 128);
    assert_eq!(view.data.data_version, 1);
    assert_eq!(view.data.committed_order, 1);
    assert_eq!(view.extents.len(), 1);
    let ext = &view.extents[&0];
    assert_eq!(ext.kind, ExtentKind::Data);
    assert_eq!(ext.logical_len, 128);
    assert_eq!(ext.block_count, 2);
    let slice_id = ext.slice_id;

    // Binding and placement rows for both blocks, in the same transaction.
    for block in 0..2u64 {
        let binding = env
            .store
            .get(&env.keys.binding(&slice_id, block))
            .await
            .unwrap()
            .expect("binding row");
        assert_eq!(binding.len(), 36);
        let placement = env
            .store
            .get(&env.keys.placement(&slice_id, block))
            .await
            .unwrap()
            .expect("placement row");
        assert_eq!(placement[0], 0); // tag 0 = Loose
    }

    // The head advanced exactly one commit sequence.
    assert_eq!(read_head(&env).await.head.commit_seq, 1);

    // Both object registrations are Verified, and the mutation result is
    // recorded with the durable receipts root.
    let mut_key = env.keys.mutation(&ticket.operation_id);
    let recorded =
        super::commit::decode_mutation_result(&env.store.get(&mut_key).await.unwrap().unwrap())
            .unwrap();
    assert_eq!(recorded.inode, INODE);
    assert!(recorded.durable_receipts.object.key.ends_with(b".brfc"));
}

pub(crate) async fn commit_with_tampered_registration_fails_atomically(
    store: Arc<dyn ControlStore>,
) {
    // The registration the commit depends on is flipped to a
    // non-committable state after dispatch: the commit re-reads it,
    // refuses permanently, and writes nothing (spec 20 §6).
    let env = env_on(store, 64).await;
    let operation_id = [9u8; 16];
    let req = truncate_request(&env, &operation_id, 1, 4096).await;
    let object_id = req.receipts_registration.object_ref.object_id;
    let object_key = env.keys.object(&env.params.domain_id, &object_id);
    let mut registration =
        match ControlRecord::decode(&env.store.get(&object_key).await.unwrap().unwrap()).unwrap() {
            ControlRecord::ObjectRegistration(r) => r,
            _ => panic!("registration row"),
        };
    registration.attempt_generation = 99;
    env.store
        .run(super::store::Txn::new().put(
            object_key,
            ControlRecord::ObjectRegistration(registration).encode(),
        ))
        .await
        .unwrap();

    let before = snapshot(&env).await;
    let guard = guard_for(&env).await;
    let err = commit_uploaded_slice(&*env.store, &env.keys, &guard, &req)
        .await
        .unwrap_err();
    assert!(matches!(err, WriteError::RegistrationMismatch(_)));
    assert!(!err.is_retryable());
    assert_eq!(
        snapshot(&env).await,
        before,
        "a failed commit writes nothing"
    );
}

// ---------------------------------------------------------------------------
// Ordering: reordered and late uploads cannot land (spec 07 §12, spec 18 §10).
// ---------------------------------------------------------------------------

pub(crate) async fn reordered_upload_cannot_overtake_its_predecessor(store: Arc<dyn ControlStore>) {
    let env = env_on(store, 64).await;
    let first = env.overlay.accept(INODE, write_spec(0, 64)).await.unwrap();
    let second = env.overlay.accept(INODE, write_spec(64, 64)).await.unwrap();

    // Upload order is inverted: only the second write completes.
    env.overlay.dispatch(&second).await.unwrap();
    env.overlay.complete_upload(&second).await.unwrap();
    let report = env.overlay.drain().await.unwrap();
    assert!(
        report.committed.is_empty(),
        "nothing may commit ahead of order 1"
    );

    let view =
        super::commit::read_inode_view(&*env.store, &env.keys, &env.params.workspace_id, INODE)
            .await
            .unwrap();
    assert_eq!(view.data.committed_order, 0);
    assert!(!view.had_record);

    // The predecessor completes late; both land in admission order.
    env.overlay.dispatch(&first).await.unwrap();
    env.overlay.complete_upload(&first).await.unwrap();
    let report = env.overlay.drain().await.unwrap();
    assert_eq!(report.committed.len(), 2);
    assert_eq!(report.committed[0].inode, INODE);
    let view =
        super::commit::read_inode_view(&*env.store, &env.keys, &env.params.workspace_id, INODE)
            .await
            .unwrap();
    assert_eq!(view.data.committed_order, 2);
    assert_eq!(view.data.size, 128);
    // Two data extents, in offset order.
    assert_eq!(view.extents.len(), 2);
    assert!(view.extents.contains_key(&0));
    assert!(view.extents.contains_key(&64));
}

pub(crate) async fn ordering_gate_rejects_late_and_early_slots_permanently(
    store: Arc<dyn ControlStore>,
) {
    let env = env_on(store, 64).await;
    let guard = guard_for(&env).await;

    // Slot 2 while committed_order is 0: an earlier mutation is missing.
    let early = truncate_request(&env, &[1u8; 16], 2, 100).await;
    let err = commit_uploaded_slice(&*env.store, &env.keys, &guard, &early)
        .await
        .unwrap_err();
    assert!(matches!(err, WriteError::OutOfOrder(_)));
    assert!(err.to_string().contains("earlier mutation"));

    // Slot 1 commits.
    let on_time = truncate_request(&env, &[2u8; 16], 1, 100).await;
    let guard = guard_for(&env).await;
    assert!(matches!(
        commit_uploaded_slice(&*env.store, &env.keys, &guard, &on_time)
            .await
            .unwrap(),
        CommitOutcome::Committed(_)
    ));

    // Slot 1 again (a different operation): the slot has passed; the late
    // upload must not re-land over newer content.
    let late = truncate_request(&env, &[3u8; 16], 1, 200).await;
    let guard = guard_for(&env).await;
    let err = commit_uploaded_slice(&*env.store, &env.keys, &guard, &late)
        .await
        .unwrap_err();
    assert!(matches!(err, WriteError::OutOfOrder(_)));
    assert!(err.to_string().contains("late upload"));
    assert!(
        !err.is_retryable(),
        "out of order is permanent, not a conflict"
    );
}

pub(crate) async fn cross_inode_mutations_do_not_block_each_other(store: Arc<dyn ControlStore>) {
    let env = env_on(store, 64).await;
    // inode 7 has a pending, un-uploaded write; inode 8 commits anyway.
    env.overlay.accept(7, write_spec(0, 64)).await.unwrap();
    let other = env.overlay.accept(8, write_spec(0, 64)).await.unwrap();
    env.overlay.dispatch(&other).await.unwrap();
    env.overlay.complete_upload(&other).await.unwrap();
    let report = env.overlay.drain().await.unwrap();
    assert_eq!(report.committed.len(), 1);
    assert_eq!(report.committed[0].inode, 8);
}

// ---------------------------------------------------------------------------
// Torn commits and guard failures.
// ---------------------------------------------------------------------------

pub(crate) async fn stale_head_guard_fails_and_writes_nothing(store: Arc<dyn ControlStore>) {
    let env = env_on(store, 64).await;
    let req = truncate_request(&env, &[1u8; 16], 1, 512).await;
    let before = snapshot(&env).await;

    let mut stale = guard_for(&env).await;
    stale.expected_head.head.commit_seq += 100; // a lease we never held
    let err = commit_uploaded_slice(&*env.store, &env.keys, &stale, &req)
        .await
        .unwrap_err();
    assert!(matches!(err, WriteError::StaleHeadGuard(_)));
    assert_eq!(snapshot(&env).await, before);
}

/// WRITE-007 / KV-004: a lease guards the commit that follows it.  A lease
/// that expired on backend time, that was superseded by another generation, or
/// whose verdict would have to come from a client clock yields no guard, and
/// the write it would have driven leaves no metadata behind.
pub(crate) async fn an_expired_or_superseded_lease_is_fenced_without_partial_metadata(
    store: Arc<dyn ControlStore>,
) {
    let env = env_on(store, 64).await;
    let head = read_head(&env).await;
    let grant = LeaseGrant {
        workspace_id: env.params.workspace_id,
        owner_generation: head.writer_generation,
        granted_at_ns: 1_000_000,
        ttl_ns: 5_000_000,
    };
    let backend = FixedClock::backend(1_200_000);

    // While the lease is held the guard is issued and the commit lands.
    let guard = grant.guard(&head, &backend).unwrap();
    assert_eq!(guard.expected_head, head);
    let first = truncate_request(&env, &[1u8; 16], 1, 512).await;
    assert!(matches!(
        commit_uploaded_slice(&*env.store, &env.keys, &guard, &first)
            .await
            .unwrap(),
        CommitOutcome::Committed(_)
    ));

    // A skewed client clock cannot authorise anything, not even a reading that
    // would look valid.
    let error = grant
        .guard(&head, &FixedClock::skewed_client(1_200_000))
        .unwrap_err();
    assert!(matches!(error, WriteError::LeaseFence(_)));
    assert!(!error.is_retryable());

    // Backend time passes the deadline: the lease reports Expired and issues
    // no guard at all.
    let expired = FixedClock::backend(6_000_000);
    assert!(matches!(
        grant.evaluate(&expired, head.writer_generation).unwrap(),
        LeaseFence::Expired { .. }
    ));

    // The fenced write is refused and the volume namespace is untouched: no
    // extent, inode, registration, mutation result or head row may appear.
    let stale = HeadGuard {
        workspace_id: env.params.workspace_id,
        expected_head: head.clone(),
    };
    let second = truncate_request(&env, &[2u8; 16], 2, 512).await;
    let before = snapshot(&env).await;
    let error = commit_uploaded_slice(&*env.store, &env.keys, &stale, &second)
        .await
        .unwrap_err();
    assert!(matches!(error, WriteError::StaleHeadGuard(_)));
    assert_eq!(
        snapshot(&env).await,
        before,
        "a fenced write lands no partial metadata"
    );

    // Another writer owning the generation is fenced the same way.
    let superseded = HeadState {
        writer_generation: head.writer_generation + 1,
        ..head.clone()
    };
    let error = grant
        .guard(&superseded, &FixedClock::backend(1_300_000))
        .unwrap_err();
    assert!(matches!(error, WriteError::LeaseFence(_)));
    assert!(error.to_string().contains("superseded"), "{error}");
}

pub(crate) async fn failed_transaction_applies_no_subset_of_its_writes(
    store: Arc<dyn ControlStore>,
) {
    let env = env_on(store, 64).await;
    // Commit order 1 so the inode row exists; then attempt order 2 with a
    // head that has moved on (another writer committed in between).
    let first = truncate_request(&env, &[1u8; 16], 1, 512).await;
    let guard = guard_for(&env).await;
    commit_uploaded_slice(&*env.store, &env.keys, &guard, &first)
        .await
        .unwrap();

    let second = truncate_request(&env, &[2u8; 16], 2, 256).await;
    let stale = HeadGuard {
        workspace_id: env.params.workspace_id,
        expected_head: {
            let mut h = read_head(&env).await;
            h.head.commit_seq -= 1;
            h
        },
    };
    let before = snapshot(&env).await;
    let err = commit_uploaded_slice(&*env.store, &env.keys, &stale, &second)
        .await
        .unwrap_err();
    assert!(matches!(err, WriteError::StaleHeadGuard(_)));
    assert_eq!(
        snapshot(&env).await,
        before,
        "no extent, binding, placement, inode, registration or head row may appear"
    );
}

// ---------------------------------------------------------------------------
// OperationId idempotency (spec 07 §2).
// ---------------------------------------------------------------------------

pub(crate) async fn same_operation_id_is_idempotent_and_payload_mismatch_is_rejected(
    store: Arc<dyn ControlStore>,
) {
    let env = env_on(store, 64).await;
    let operation_id = [7u8; 16];
    let req = truncate_request(&env, &operation_id, 1, 512).await;
    let guard = guard_for(&env).await;
    let first = match commit_uploaded_slice(&*env.store, &env.keys, &guard, &req)
        .await
        .unwrap()
    {
        CommitOutcome::Committed(r) => r,
        other => panic!("expected Committed, got {other:?}"),
    };

    // Same OperationId, same payload: the recorded result, untouched.
    let guard = guard_for(&env).await;
    match commit_uploaded_slice(&*env.store, &env.keys, &guard, &req)
        .await
        .unwrap()
    {
        CommitOutcome::AlreadyCommitted(r) => assert_eq!(r, first),
        other => panic!("expected AlreadyCommitted, got {other:?}"),
    }

    // Same OperationId, different payload: rejected.
    let mut other_payload = truncate_request(&env, &operation_id, 1, 999).await;
    other_payload.payload_digest = digest(b"different payload");
    let guard = guard_for(&env).await;
    let err = commit_uploaded_slice(&*env.store, &env.keys, &guard, &other_payload)
        .await
        .unwrap_err();
    assert!(matches!(err, WriteError::OperationIdMismatch(_)));
}

pub(crate) async fn independent_stores_racing_same_head_commit_once(
    first_store: Arc<dyn ControlStore>,
    second_store: Arc<dyn ControlStore>,
) {
    let first = env_on(first_store, 64).await;
    let params = first.params.clone();
    let keys = first.keys.clone();
    ensure_workspace_head(&*second_store, &keys, &params, 3)
        .await
        .unwrap();
    let second_overlay = WriteOverlay::with_memory_sink(second_store.clone(), params.clone());
    second_overlay.ensure_domain(random_id()).await.unwrap();
    let second = Env {
        store: second_store,
        keys,
        overlay: second_overlay,
        params,
        sink: Arc::new(MemorySink::default()),
    };

    // Both independently connected writers register durable receipt objects,
    // then derive different order-1 mutations from the exact same head guard.
    let left = truncate_request(&first, &[0x71; 16], 1, 111).await;
    let right = truncate_request(&second, &[0x72; 16], 1, 222).await;
    let guard = guard_for(&first).await;
    let other_guard = guard.clone();
    let (left_result, right_result) = tokio::join!(
        commit_uploaded_slice(&*first.store, &first.keys, &guard, &left),
        commit_uploaded_slice(&*second.store, &second.keys, &other_guard, &right),
    );
    let outcomes = [left_result, right_result];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, Ok(CommitOutcome::Committed(_))))
            .count(),
        1,
        "exactly one independent writer may consume the head/order slot"
    );
    for outcome in &outcomes {
        match outcome {
            Ok(CommitOutcome::Committed(_)) => {}
            Err(WriteError::Conflict(_) | WriteError::StaleHeadGuard(_)) => {}
            other => panic!("unexpected same-head race outcome: {other:?}"),
        }
    }

    let view = super::commit::read_inode_view(
        &*first.store,
        &first.keys,
        &first.params.workspace_id,
        INODE,
    )
    .await
    .unwrap();
    assert_eq!(view.data.committed_order, 1);
    assert!(matches!(view.data.size, 111 | 222));
    assert_eq!(read_head(&first).await.head.commit_seq, 1);
    let left_recorded = first
        .store
        .get(&first.keys.mutation(&left.operation_id))
        .await
        .unwrap()
        .is_some();
    let right_recorded = first
        .store
        .get(&first.keys.mutation(&right.operation_id))
        .await
        .unwrap()
        .is_some();
    assert_ne!(
        left_recorded, right_recorded,
        "the losing transaction must not leave a partial mutation result"
    );
}

// ---------------------------------------------------------------------------
// Registry identity and domain state (spec 01 §6, spec 20 §2/§6).
// ---------------------------------------------------------------------------

pub(crate) async fn registry_binds_one_object_key_to_one_identity(store: Arc<dyn ControlStore>) {
    let env = env_on(store, 64).await;
    let mut a = build_receipts_container(&env.params.volume_id, &[1u8; 16], &ReceiptSet::default())
        .root
        .object
        .clone();
    let guard = guard_for(&env).await;
    register_upload(
        &*env.store,
        &env.keys,
        &guard,
        &env.params.domain_id,
        a.clone(),
        a.full_hash,
    )
    .await
    .unwrap();

    // Same key, different ObjectId: a second GC identity for one physical
    // key is a permanent conflict.
    a.object_id = [2u8; 16];
    let guard = guard_for(&env).await;
    let err = register_upload(
        &*env.store,
        &env.keys,
        &guard,
        &env.params.domain_id,
        a.clone(),
        a.full_hash,
    )
    .await
    .unwrap_err();
    assert!(matches!(err, WriteError::RegistryConflict(_)));
    assert!(!err.is_retryable());
}

async fn set_domain_state(env: &Env, state: DomainState) {
    let domain_key = env.keys.domain(&env.params.domain_id);
    let bytes = env.store.get(&domain_key).await.unwrap().unwrap();
    let mut domain = match ControlRecord::decode(&bytes).unwrap() {
        ControlRecord::OwnershipDomain(d) => d,
        other => panic!("expected domain, got kind {}", other.kind().as_u16()),
    };
    domain.state = state;
    env.store
        .run(
            super::store::Txn::new()
                .put(domain_key, ControlRecord::OwnershipDomain(domain).encode()),
        )
        .await
        .unwrap();
}

pub(crate) async fn non_active_domain_rejects_registration(store: Arc<dyn ControlStore>) {
    let env = env_on(store, 64).await;
    set_domain_state(&env, DomainState::Closed).await;

    let object =
        build_receipts_container(&env.params.volume_id, &[5u8; 16], &ReceiptSet::default())
            .root
            .object;
    let guard = guard_for(&env).await;
    let err = register_upload(
        &*env.store,
        &env.keys,
        &guard,
        &env.params.domain_id,
        object,
        digest(b"plan"),
    )
    .await
    .unwrap_err();
    assert!(matches!(err, WriteError::DomainNotActive(_)));
}

pub(crate) async fn non_active_domain_rejects_commit(store: Arc<dyn ControlStore>) {
    // The request is built while the domain is active (its registrations
    // exist); the domain then closes before the commit runs.
    let env = env_on(store, 64).await;
    let req = truncate_request(&env, &[6u8; 16], 1, 64).await;
    set_domain_state(&env, DomainState::Closed).await;
    let guard = guard_for(&env).await;
    let err = commit_uploaded_slice(&*env.store, &env.keys, &guard, &req)
        .await
        .unwrap_err();
    assert!(matches!(err, WriteError::DomainNotActive(_)));
}

// ---------------------------------------------------------------------------
// Dirty handoff and predecessor blocking (spec 18 §5/§10).
// ---------------------------------------------------------------------------

pub(crate) async fn handoff_removes_only_committed_dirty_and_captures_keep_theirs(
    store: Arc<dyn ControlStore>,
) {
    let env = env_on(store, 64).await;
    let first = env.overlay.accept(INODE, write_spec(0, 64)).await.unwrap();
    let second = env.overlay.accept(INODE, write_spec(64, 64)).await.unwrap();

    let before = env.overlay.capture().await;
    assert_eq!(before.dirty.len(), 2);

    env.overlay.dispatch(&first).await.unwrap();
    env.overlay.complete_upload(&first).await.unwrap();
    env.overlay.drain().await.unwrap();

    // Only the committed operation's dirty references were handed off.
    let after_first = env.overlay.capture().await;
    assert_eq!(after_first.dirty.len(), 1);
    assert_eq!(after_first.dirty[0].offset, 64);

    // The earlier capture still holds its Arc clones — its dirty set did
    // not shrink under it.
    assert_eq!(before.dirty.len(), 2);

    env.overlay.dispatch(&second).await.unwrap();
    env.overlay.complete_upload(&second).await.unwrap();
    env.overlay.drain().await.unwrap();
    let after_second = env.overlay.capture().await;
    assert!(after_second.dirty.is_empty());
    assert_eq!(after_first.dirty.len(), 1);
}

pub(crate) async fn failed_predecessor_blocks_successors_instead_of_skipping_them(
    store: Arc<dyn ControlStore>,
) {
    let env = env_on(store, 64).await;
    let first = env.overlay.accept(INODE, write_spec(0, 64)).await.unwrap();
    let second = env.overlay.accept(INODE, write_spec(64, 64)).await.unwrap();

    // The first operation can never commit: its registration is sabotaged
    // after dispatch (the store refuses to verify what we never uploaded).
    env.overlay.dispatch(&first).await.unwrap();
    {
        // Change the first op's registration attempt behind the overlay:
        // the receipt captured attempt 1 and must not verify attempt 2.
        let object_id = {
            let rows = env
                .store
                .scan(&env.keys.inventory(&env.params.domain_id, 1))
                .await
                .unwrap();
            let id: ObjectId = rows[0].1.as_slice().try_into().unwrap();
            id
        };
        let object_key = env.keys.object(&env.params.domain_id, &object_id);
        let mut registration =
            match ControlRecord::decode(&env.store.get(&object_key).await.unwrap().unwrap())
                .unwrap()
            {
                ControlRecord::ObjectRegistration(r) => r,
                _ => panic!(),
            };
        registration.attempt_generation += 1;
        env.store
            .run(super::store::Txn::new().put(
                object_key,
                ControlRecord::ObjectRegistration(registration).encode(),
            ))
            .await
            .unwrap();
    }

    env.overlay.complete_upload(&first).await.unwrap();
    // The sabotage hits at commit time: the re-read registration carries a
    // different upload attempt, a permanent mismatch. Drain must surface it
    // and block the successor.
    let report = env.overlay.drain().await.unwrap();
    assert_eq!(report.failed.len(), 1, "the sabotaged predecessor fails");
    assert_eq!(
        report.blocked.len(),
        1,
        "the successor is blocked, not skipped"
    );
    assert_eq!(
        report.blocked[0].0.admission_ticket,
        second.admission_ticket
    );
    assert!(report.committed.is_empty());

    // Nothing committed for this inode.
    let view =
        super::commit::read_inode_view(&*env.store, &env.keys, &env.params.workspace_id, INODE)
            .await
            .unwrap();
    assert_eq!(view.data.committed_order, 0);
    assert!(!view.had_record);
}

// ---------------------------------------------------------------------------
// Extent algebra.
// ---------------------------------------------------------------------------

#[test]
fn partial_overlap_splits_data_extents_block_aligned() {
    let mut extents = std::collections::BTreeMap::new();
    extents.insert(0u64, NativeExtent::data(256, [1u8; 16], 0, 4));
    let change = plan_extent_change(&extents, &(64..128), None, 64);
    assert_eq!(change.removed.len(), 1);
    assert_eq!(change.added.len(), 2);
    let head = &change.added[0];
    assert_eq!(
        (
            head.0,
            head.1.logical_len,
            head.1.first_block,
            head.1.block_count
        ),
        (0, 64, 0, 1)
    );
    let tail = &change.added[1];
    assert_eq!(
        (
            tail.0,
            tail.1.logical_len,
            tail.1.first_block,
            tail.1.block_count
        ),
        (128, 128, 2, 2)
    );
}

#[test]
fn unaligned_cut_keeps_partial_tail_block() {
    // Truncate into the middle of a block: the kept extent references the
    // partially-covered block and its logical_len bounds readability.
    let mut extents = std::collections::BTreeMap::new();
    extents.insert(0u64, NativeExtent::data(128, [1u8; 16], 0, 2));
    let change = plan_extent_change(&extents, &(40..128), None, 64);
    assert_eq!(change.added.len(), 1);
    let (offset, ext) = &change.added[0];
    assert_eq!(*offset, 0);
    assert_eq!(ext.logical_len, 40);
    assert_eq!(ext.first_block, 0);
    assert_eq!(ext.block_count, 1, "the partial block stays referenced");
}

#[test]
fn hole_splits_and_replacement() {
    let mut extents = std::collections::BTreeMap::new();
    extents.insert(0u64, NativeExtent::hole(256));
    let replacement = NativeExtent::data(64, [2u8; 16], 5, 1);
    let change = plan_extent_change(&extents, &(64..128), Some(replacement.clone()), 64);
    // The plan adds the surviving head split, the tail split, then the
    // replacement — every added row has a distinct extent key.
    assert_eq!(change.added.len(), 3);
    assert_eq!(change.added[0], (0, NativeExtent::hole(64)));
    assert_eq!(change.added[1], (128, NativeExtent::hole(128)));
    assert_eq!(change.added[2], (64, replacement));
}

pub(crate) async fn truncate_down_and_extend_and_punch_produce_holes(store: Arc<dyn ControlStore>) {
    let env = env_on(store, 64).await;
    // A 128-byte data region.
    let ticket = env.overlay.accept(INODE, write_spec(0, 128)).await.unwrap();
    env.overlay.dispatch(&ticket).await.unwrap();
    env.overlay.complete_upload(&ticket).await.unwrap();
    env.overlay.drain().await.unwrap();

    // Extend to 256: a hole covers the growth.
    let ticket = env
        .overlay
        .accept(INODE, MutationSpec::Truncate { new_size: 256 })
        .await
        .unwrap();
    assert_eq!(ticket.mutation_order, 2);
    env.overlay.dispatch(&ticket).await.unwrap();
    env.overlay.complete_upload(&ticket).await.unwrap();
    env.overlay.drain().await.unwrap();
    let view =
        super::commit::read_inode_view(&*env.store, &env.keys, &env.params.workspace_id, INODE)
            .await
            .unwrap();
    assert_eq!(view.data.size, 256);
    assert_eq!(view.extents.len(), 2);
    assert_eq!(view.extents[&128].kind, ExtentKind::Hole);
    assert_eq!(view.extents[&128].logical_len, 128);

    // Punch the middle of the data region: a hole replaces data.
    let ticket = env
        .overlay
        .accept(INODE, MutationSpec::PunchHole { offset: 0, len: 64 })
        .await
        .unwrap();
    env.overlay.dispatch(&ticket).await.unwrap();
    env.overlay.complete_upload(&ticket).await.unwrap();
    env.overlay.drain().await.unwrap();
    let view =
        super::commit::read_inode_view(&*env.store, &env.keys, &env.params.workspace_id, INODE)
            .await
            .unwrap();
    assert_eq!(view.extents.len(), 3);
    assert_eq!(view.extents[&0].kind, ExtentKind::Hole);
    assert_eq!(view.extents[&0].logical_len, 64);
    assert_eq!(view.extents[&64].kind, ExtentKind::Data);
    assert_eq!(view.extents[&128].kind, ExtentKind::Hole);

    // Truncate down: rows beyond EOF are removed entirely.
    let ticket = env
        .overlay
        .accept(INODE, MutationSpec::Truncate { new_size: 64 })
        .await
        .unwrap();
    env.overlay.dispatch(&ticket).await.unwrap();
    env.overlay.complete_upload(&ticket).await.unwrap();
    env.overlay.drain().await.unwrap();
    let view =
        super::commit::read_inode_view(&*env.store, &env.keys, &env.params.workspace_id, INODE)
            .await
            .unwrap();
    assert_eq!(view.data.size, 64);
    assert_eq!(view.extents.len(), 1);
    assert_eq!(view.extents[&0].kind, ExtentKind::Hole);
    assert_eq!(view.data.committed_order, 4);
}

// ---------------------------------------------------------------------------
// Admission validation.
// ---------------------------------------------------------------------------

pub(crate) async fn unaligned_writes_are_refused_at_admission(store: Arc<dyn ControlStore>) {
    let env = env_on(store, 64).await;
    assert!(env.overlay.accept(INODE, write_spec(10, 64)).await.is_err());
    assert!(env.overlay.accept(INODE, write_spec(0, 100)).await.is_err());
    assert!(
        env.overlay
            .accept(
                INODE,
                MutationSpec::Write {
                    offset: 0,
                    data: Arc::new(Vec::new()),
                }
            )
            .await
            .is_err()
    );
}

/// WRITE-010 / INV-09: a `chmod` on a GiB file is a metadata-only mutation.
/// The extent rows, the size and every data object stay exactly as they were,
/// no data object is uploaded, and the only new object is the control receipts
/// container the protocol requires of every commit.  A malformed mode is
/// refused instead of being committed as an inode type.
pub(crate) async fn chmod_on_a_large_file_changes_metadata_and_uploads_no_data(
    store: Arc<dyn ControlStore>,
) {
    let env = env_on(store, 64).await;
    // A GiB-sized file with a data block at the front: the data path this
    // mutation must not touch.
    let ticket = env.overlay.accept(INODE, write_spec(0, 64)).await.unwrap();
    env.overlay.dispatch(&ticket).await.unwrap();
    env.overlay.complete_upload(&ticket).await.unwrap();
    env.overlay.drain().await.unwrap();
    let gigabyte = 1u64 << 30;
    let grow = env
        .overlay
        .accept(INODE, MutationSpec::Truncate { new_size: gigabyte })
        .await
        .unwrap();
    env.overlay.dispatch(&grow).await.unwrap();
    env.overlay.complete_upload(&grow).await.unwrap();
    env.overlay.drain().await.unwrap();

    let before =
        super::commit::read_inode_view(&*env.store, &env.keys, &env.params.workspace_id, INODE)
            .await
            .unwrap();
    assert_eq!(before.data.size, gigabyte);
    assert!(!before.extents.is_empty());
    let uploads_before = env.overlay.uploaded_object_ids().await;
    assert!(
        !uploads_before.is_empty(),
        "the data write really uploaded something"
    );
    assert!(
        env.store
            .get(&env.keys.inode_attributes(&env.params.workspace_id, INODE))
            .await
            .unwrap()
            .is_none(),
        "no attribute row exists yet"
    );

    // A malformed mode has no file-type bits: refused at admission, so it
    // never takes a mutation order and cannot block the inode's queue.
    let refused = env
        .overlay
        .accept(
            INODE,
            MutationSpec::SetAttributes {
                mode: 0o644,
                uid: 1000,
                gid: 1000,
                ctime_ns: 7,
            },
        )
        .await
        .expect_err("a bare permission mask is not an inode mode");
    assert!(
        refused.to_string().contains("file type"),
        "unexpected refusal: {refused}"
    );
    assert!(
        env.overlay.capture().await.dirty.is_empty(),
        "a metadata-only mutation dirties no data range"
    );

    // The valid chmod commits.
    let attributes = super::records::InodeAttributes {
        mode: 0o100_400,
        uid: 1000,
        gid: 1001,
        rdev: 0,
        ctime_ns: 42,
    };
    let before_keys: std::collections::BTreeSet<Vec<u8>> = snapshot(&env)
        .await
        .into_iter()
        .map(|(key, _)| key)
        .collect();
    let ticket = env
        .overlay
        .accept(
            INODE,
            MutationSpec::SetAttributes {
                mode: attributes.mode,
                uid: attributes.uid,
                gid: attributes.gid,
                ctime_ns: attributes.ctime_ns,
            },
        )
        .await
        .unwrap();
    env.overlay.dispatch(&ticket).await.unwrap();
    env.overlay.complete_upload(&ticket).await.unwrap();
    let report = env.overlay.drain().await.unwrap();
    assert_eq!(
        report.committed.len(),
        1,
        "chmod must commit; failed={:?} blocked={:?}",
        report.failed,
        report.blocked
    );
    let added: Vec<Vec<u8>> = {
        let after_keys: std::collections::BTreeSet<Vec<u8>> = snapshot(&env)
            .await
            .into_iter()
            .map(|(key, _)| key)
            .collect();
        after_keys.difference(&before_keys).cloned().collect()
    };

    // The data path is untouched: same extents, same size, same slices.
    let after =
        super::commit::read_inode_view(&*env.store, &env.keys, &env.params.workspace_id, INODE)
            .await
            .unwrap();
    assert_eq!(after.extents, before.extents, "no extent row changed");
    assert_eq!(after.data.size, gigabyte, "the size did not change");
    assert_eq!(after.data.data_version, before.data.data_version + 1);

    // The attribute row is written with exactly the requested bytes.
    let row = env
        .store
        .get(&env.keys.inode_attributes(&env.params.workspace_id, INODE))
        .await
        .unwrap()
        .expect("the attribute row exists");
    assert_eq!(
        super::records::InodeAttributes::decode(&row).unwrap(),
        attributes
    );
    assert_eq!(attributes.permissions(), 0o400);

    // Exactly one new object was uploaded and it is a control object, not a
    // loose data object: a chmod never rewrites (or re-uploads) file content.
    let uploads_after = env.overlay.uploaded_object_ids().await;
    let new_uploads: Vec<ObjectId> = uploads_after[uploads_before.len()..].to_vec();
    assert_eq!(new_uploads.len(), 1, "only the control receipts container");
    let registration = super::domain::decode_registration(
        &env.store
            .get(&env.keys.object(&env.params.domain_id, &new_uploads[0]))
            .await
            .unwrap()
            .expect("the control object is registered"),
    )
    .unwrap();
    assert_eq!(
        registration.object_ref.kind,
        crate::native_base::wire::container::ObjectKind::FrozenMetadata.as_u8(),
        "the new object is control metadata"
    );
    assert_ne!(
        registration.object_ref.kind,
        crate::native_base::seal::NATIVE_LOOSE_KIND,
        "no data block was uploaded"
    );
    assert_eq!(
        report.committed[0].durable_receipts.object.object_id, new_uploads[0],
        "the new object is this mutation's receipt"
    );
    // No data-path row appeared: no extent, no binding, no placement.  The
    // chmod added only control rows (attribute, registration, result).
    assert!(
        added.iter().all(|key| !is_data_path_key(key)),
        "a metadata-only mutation must not add data-path rows: {:?}",
        added
            .iter()
            .filter(|key| is_data_path_key(key))
            .map(|key| String::from_utf8_lossy(key).into_owned())
            .collect::<Vec<_>>()
    );
    assert!(
        added
            .iter()
            .any(|key| key.windows(5).any(|w| w == b"attr/")),
        "the attribute row is the one row this mutation adds"
    );
}

/// Rows that describe file content: extents, block bindings, placements.
fn is_data_path_key(key: &[u8]) -> bool {
    let text = String::from_utf8_lossy(key);
    text.contains("/ext/") || text.contains("/bnd/") || text.contains("/plc/")
}

// ---------------------------------------------------------------------------
// WRITE-012: a temp file renamed over an existing destination is an
// application-complete write, never a minimal patch.
// ---------------------------------------------------------------------------

/// Commit one mutation through the overlay: accept, dispatch, complete, drain.
async fn commit_mutation(env: &Env, inode: u64, spec: MutationSpec) {
    let report = commit_mutation_report(env, inode, spec).await;
    assert_eq!(report.committed.len(), 1, "failed={:?}", report.failed);
}

/// The same, returning the drain report to the caller.
async fn commit_mutation_report(
    env: &Env,
    inode: u64,
    spec: MutationSpec,
) -> super::overlay::DrainReport {
    let ticket = env.overlay.accept(inode, spec).await.unwrap();
    env.overlay.dispatch(&ticket).await.unwrap();
    env.overlay.complete_upload(&ticket).await.unwrap();
    env.overlay.drain().await.unwrap()
}

/// WRITE-012 / INV-02: the destination's old content (its extents, its blocks
/// and its data version) is replaced wholesale by the source's, every carried
/// block is attested by the commit's receipts, and the plan has no
/// patch-shaped form even when both files hold the same number of bytes.
pub(crate) async fn rename_over_publishes_the_complete_file_and_never_a_patch(
    store: Arc<dyn ControlStore>,
) {
    const SOURCE: u64 = 8;
    let env = env_on(store, 64).await;

    // The destination already holds its own content: two distinct blocks.
    commit_mutation(&env, INODE, write_spec_of(0, 0x11, 64)).await;
    commit_mutation(&env, INODE, write_spec_of(64, 0x22, 64)).await;
    // The temporary source file: one data extent followed by a hole, so the
    // replacement has to carry both kinds of extent.
    commit_mutation(&env, SOURCE, write_spec_of(0, 0x33, 128)).await;
    commit_mutation(&env, SOURCE, MutationSpec::Truncate { new_size: 256 }).await;

    let source_before =
        super::commit::read_inode_view(&*env.store, &env.keys, &env.params.workspace_id, SOURCE)
            .await
            .unwrap();
    let dest_before =
        super::commit::read_inode_view(&*env.store, &env.keys, &env.params.workspace_id, INODE)
            .await
            .unwrap();
    assert_eq!(source_before.extents.len(), 2, "data extent + hole");
    assert_eq!(source_before.data.size, 256);
    assert_eq!(dest_before.extents.len(), 2);
    assert_eq!(dest_before.data.size, 128);
    let source_objects = extent_objects(&env, SOURCE).await;
    let dest_objects_before = extent_objects(&env, INODE).await;
    assert!(!source_objects.is_empty() && !dest_objects_before.is_empty());
    assert!(
        dest_objects_before.is_disjoint(&source_objects),
        "the two files hold different content"
    );

    // The plan publishes the whole file -- holes included -- and has no
    // byte-delta form: the changed range is the entire file.
    let plan = plan_rename_over(
        &*env.store,
        &env.keys,
        &env.params.workspace_id,
        &env.params.domain_id,
        SOURCE,
        INODE,
        env.params.block_size,
    )
    .await
    .unwrap();
    assert!(plan.is_application_complete());
    assert_eq!(plan.changed_ranges(), vec![(0, 256)]);
    assert_eq!(plan.carried.len(), 2);
    assert_eq!(
        plan.replaced.len(),
        2,
        "every destination extent is removed"
    );
    assert_eq!(
        plan.carried_object_ids()
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>(),
        source_objects
    );

    // A rename-over needs a real, durable source: itself and an absent inode
    // are both refused before anything is admitted.
    assert!(
        plan_rename_over(
            &*env.store,
            &env.keys,
            &env.params.workspace_id,
            &env.params.domain_id,
            SOURCE,
            SOURCE,
            env.params.block_size,
        )
        .await
        .is_err(),
        "an inode cannot be renamed over itself"
    );
    assert!(
        plan_rename_over(
            &*env.store,
            &env.keys,
            &env.params.workspace_id,
            &env.params.domain_id,
            4096,
            INODE,
            env.params.block_size,
        )
        .await
        .is_err(),
        "an absent source has nothing to publish"
    );

    // The rename-over: one commit, no data upload (the blocks are already
    // durable), and a receipts container attesting every carried block.
    let uploads_before = env.overlay.uploaded_object_ids().await;
    let report = commit_mutation_report(
        &env,
        INODE,
        MutationSpec::ReplaceInode {
            source_inode: SOURCE,
        },
    )
    .await;
    assert_eq!(report.committed.len(), 1, "failed={:?}", report.failed);
    assert!(report.failed.is_empty() && report.blocked.is_empty() && report.orphaned.is_empty());
    let uploads_after = env.overlay.uploaded_object_ids().await;
    assert_eq!(
        uploads_after.len(),
        uploads_before.len() + 1,
        "a rename-over uploads only its receipts container"
    );

    // The destination *is* the source's content now: same extents, same size,
    // a new data version, and no old byte left behind.
    let dest_after =
        super::commit::read_inode_view(&*env.store, &env.keys, &env.params.workspace_id, INODE)
            .await
            .unwrap();
    assert_eq!(dest_after.extents, source_before.extents);
    assert_eq!(dest_after.data.size, 256);
    assert_eq!(
        dest_after.data.data_version,
        dest_before.data.data_version + 1
    );
    assert_eq!(
        dest_after.data.committed_order,
        dest_before.data.committed_order + 1
    );
    assert!(
        env.store
            .get(&env.keys.extent(&env.params.workspace_id, INODE, 64))
            .await
            .unwrap()
            .is_none(),
        "the destination's old data extent row is gone"
    );
    assert_eq!(dest_after.extents[&128].kind, ExtentKind::Hole);
    let dest_objects_after = extent_objects(&env, INODE).await;
    assert_eq!(dest_objects_after, source_objects);
    assert!(dest_objects_before.is_disjoint(&dest_objects_after));

    // The commit's durable receipts attest exactly the carried objects: the
    // publication is complete, not a claim of a small delta.
    let result = &report.committed[0];
    let entries = receipts_of(&env.sink, &result.durable_receipts);
    let attested: std::collections::BTreeSet<ObjectId> =
        entries.iter().map(|entry| entry.object_id).collect();
    assert_eq!(
        attested, source_objects,
        "every carried block is attested by the commit's receipts"
    );
    assert!(
        entries
            .iter()
            .all(|entry| entry.domain_id == env.params.domain_id)
    );

    // The source inode is untouched: publishing content does not consume the
    // source (the VFS unlinks it separately).
    let source_after =
        super::commit::read_inode_view(&*env.store, &env.keys, &env.params.workspace_id, SOURCE)
            .await
            .unwrap();
    assert_eq!(source_after.extents, source_before.extents);
    assert_eq!(source_after.data, source_before.data);

    // A second rename-over of the same content has the same size on both
    // sides.  That is still a whole-file publication: the version moves again
    // and the receipts still name every block, so nothing here can be
    // reported as an "unchanged" minimal patch.
    let same_size = commit_mutation_report(
        &env,
        INODE,
        MutationSpec::ReplaceInode {
            source_inode: SOURCE,
        },
    )
    .await;
    assert_eq!(
        same_size.committed.len(),
        1,
        "failed={:?}",
        same_size.failed
    );
    let dest_after_second =
        super::commit::read_inode_view(&*env.store, &env.keys, &env.params.workspace_id, INODE)
            .await
            .unwrap();
    assert_eq!(dest_after_second.extents, source_before.extents);
    assert_eq!(dest_after_second.data.size, dest_after.data.size);
    assert_eq!(
        dest_after_second.data.data_version,
        dest_after.data.data_version + 1
    );
    let attested_again: std::collections::BTreeSet<ObjectId> =
        receipts_of(&env.sink, &same_size.committed[0].durable_receipts)
            .iter()
            .map(|entry| entry.object_id)
            .collect();
    assert_eq!(attested_again, source_objects);

    // A hand-built partial replacement -- one extent short of the whole file --
    // is refused at the commit boundary.  Without that rule the transaction
    // would keep the destination's old bytes at offset 0 while claiming an
    // application-complete replacement.
    let partial = {
        let (receipts, receipts_registration) = receipts_for(&env, &[0x5a; 16]).await;
        CommitRequest {
            operation_id: [0x5a; 16],
            payload_digest: digest(b"partial replacement"),
            inode: 9,
            mutation_order: 1,
            mutation: Mutation::ReplaceInode {
                source_inode: SOURCE,
                logical_size: 256,
                carried: vec![super::commit::CarriedExtent {
                    logical_offset: 128,
                    extent: source_before.extents[&128].clone(),
                    blocks: Vec::new(),
                }],
            },
            receipts,
            receipts_registration,
            block_size: env.params.block_size,
            baseline_size: 0,
            domain_id: env.params.domain_id,
        }
    };
    let before_partial = snapshot(&env).await;
    let guard = guard_for(&env).await;
    let refused = commit_uploaded_slice(&*env.store, &env.keys, &guard, &partial)
        .await
        .expect_err("a partial replacement is not an application-complete write");
    assert!(refused.to_string().contains("complete file"), "{refused}");
    assert_eq!(
        snapshot(&env).await,
        before_partial,
        "a refused replacement writes nothing"
    );
}

pub(crate) async fn mutation_order_continues_from_the_durable_watermark(
    store: Arc<dyn ControlStore>,
) {
    // A fresh overlay over a store with committed history continues the
    // ordering watermark instead of restarting at 1.
    let env = env_on(store, 64).await;
    let ticket = env.overlay.accept(INODE, write_spec(0, 64)).await.unwrap();
    env.overlay.dispatch(&ticket).await.unwrap();
    env.overlay.complete_upload(&ticket).await.unwrap();
    env.overlay.drain().await.unwrap();

    let params = env.params.clone();
    let reloaded = WriteOverlay::with_memory_sink(env.store.clone(), params);
    let ticket = reloaded.accept(INODE, write_spec(64, 64)).await.unwrap();
    assert_eq!(
        ticket.mutation_order, 2,
        "order continues committed_order + 1"
    );
}

#[tokio::test]
async fn unattributed_head_advance_before_drain_fences_the_commit() {
    let env = env_on(Arc::new(MemoryControlStore::new()), 64).await;
    let ticket = env.overlay.accept(INODE, write_spec(0, 64)).await.unwrap();
    env.overlay.dispatch(&ticket).await.unwrap();
    env.overlay.complete_upload(&ticket).await.unwrap();

    let head_key = env.keys.head(&env.params.workspace_id);
    let head = read_head(&env).await;
    env.store
        .run(
            Txn::new()
                .check_bytes(head_key.clone(), head.encode())
                .put(head_key, head.next_commit().encode()),
        )
        .await
        .unwrap();

    let report = env.overlay.drain().await.unwrap();
    assert!(report.committed.is_empty());
    assert_eq!(report.failed.len(), 1);
    assert!(report.failed[0].1.contains("stale head guard"));
    assert_eq!(read_head(&env).await.head.commit_seq, 1);
}

#[tokio::test]
async fn writer_generation_change_before_drain_still_fences_the_commit() {
    let env = env_on(Arc::new(MemoryControlStore::new()), 64).await;
    let ticket = env.overlay.accept(INODE, write_spec(0, 64)).await.unwrap();
    env.overlay.dispatch(&ticket).await.unwrap();
    env.overlay.complete_upload(&ticket).await.unwrap();

    let head_key = env.keys.head(&env.params.workspace_id);
    let head = read_head(&env).await;
    let changed = HeadState {
        writer_generation: head.writer_generation + 1,
        ..head.clone()
    };
    env.store
        .run(
            Txn::new()
                .check_bytes(head_key.clone(), head.encode())
                .put(head_key, changed.encode()),
        )
        .await
        .unwrap();

    let report = env.overlay.drain().await.unwrap();
    assert!(report.committed.is_empty());
    assert_eq!(report.failed.len(), 1);
    assert_eq!(read_head(&env).await, changed);
}

// ---------------------------------------------------------------------------
// Backends. The scenarios above are store-agnostic: this module runs the
// full list against the in-memory store in the normal test pass, and
// tests_redis / tests_tikv run the identical list against real instances
// behind `--ignored` integration gates.
// ---------------------------------------------------------------------------

macro_rules! run_scenarios_on_memory {
    ($($name:ident),* $(,)?) => {
        mod memory_backend {
            use super::*;

            $(
                #[tokio::test]
                async fn $name() {
                    super::$name(Arc::new(MemoryControlStore::new())).await;
                }
            )*

            #[tokio::test]
            async fn independent_stores_racing_same_head_commit_once() {
                let store = Arc::new(MemoryControlStore::new());
                super::independent_stores_racing_same_head_commit_once(
                    store.clone(),
                    store,
                )
                .await;
            }
        }
    };
}

run_scenarios_on_memory! {
    commit_writes_extent_binding_placement_inode_and_head_together,
    commit_with_tampered_registration_fails_atomically,
    reordered_upload_cannot_overtake_its_predecessor,
    ordering_gate_rejects_late_and_early_slots_permanently,
    cross_inode_mutations_do_not_block_each_other,
    stale_head_guard_fails_and_writes_nothing,
    an_expired_or_superseded_lease_is_fenced_without_partial_metadata,
    failed_transaction_applies_no_subset_of_its_writes,
    same_operation_id_is_idempotent_and_payload_mismatch_is_rejected,
    registry_binds_one_object_key_to_one_identity,
    non_active_domain_rejects_registration,
    non_active_domain_rejects_commit,
    handoff_removes_only_committed_dirty_and_captures_keep_theirs,
    failed_predecessor_blocks_successors_instead_of_skipping_them,
    truncate_down_and_extend_and_punch_produce_holes,
    unaligned_writes_are_refused_at_admission,
    mutation_order_continues_from_the_durable_watermark,
    chmod_on_a_large_file_changes_metadata_and_uploads_no_data,
    rename_over_publishes_the_complete_file_and_never_a_patch,
}

// ---------------------------------------------------------------------------
// WRITE-006: a durable upload whose KV step fails is protected, never left
// collectable.  This scenario needs fault injection, so it is not part of the
// store-agnostic list above.
// ---------------------------------------------------------------------------

mod orphan_protection {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    use async_trait::async_trait;
    use tokio::sync::Mutex;

    use super::super::orphan_receipt::{
        CollectionPlan, KvStep, OrphanResolution, ensure_not_collected, plan_collection,
        protected_object_ids, read_orphan_receipt, record_orphan_receipt, resolve_orphan_receipt,
        scan_orphan_receipts,
    };
    use super::super::store::{StoreError, Txn};

    /// A store that can fail the commit transaction (or every transaction)
    /// while the object uploads in front of it still succeed.
    #[derive(Default)]
    struct FaultyStore {
        inner: Mutex<std::collections::BTreeMap<Vec<u8>, Vec<u8>>>,
        fail_commits: AtomicBool,
        fail_everything: AtomicBool,
    }

    fn is_mutation_result_key(key: &[u8]) -> bool {
        String::from_utf8_lossy(key).contains("/mut/")
    }

    #[async_trait]
    impl ControlStore for FaultyStore {
        async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
            Ok(self.inner.lock().await.get(key).cloned())
        }

        async fn run(&self, txn: Txn) -> Result<(), StoreError> {
            if self.fail_everything.load(Ordering::SeqCst) {
                return Err(StoreError::Backend("simulated KV outage".into()));
            }
            if self.fail_commits.load(Ordering::SeqCst)
                && txn
                    .writes
                    .iter()
                    .any(|(key, _)| is_mutation_result_key(key))
            {
                return Err(StoreError::Backend(
                    "simulated KV failure after a successful upload".into(),
                ));
            }
            let mut map = self.inner.lock().await;
            for (key, expect) in &txn.checks {
                if !crate::native_base::write::store::expect_matches(
                    expect,
                    map.get(key).map(|v| v.as_slice()),
                ) {
                    return Err(StoreError::Conflict);
                }
            }
            for (key, value) in txn.writes {
                match value {
                    Some(bytes) => {
                        map.insert(key, bytes);
                    }
                    None => {
                        map.remove(&key);
                    }
                }
            }
            Ok(())
        }

        async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StoreError> {
            let map = self.inner.lock().await;
            Ok(map
                .range::<[u8], _>((
                    std::ops::Bound::Included(prefix),
                    std::ops::Bound::Unbounded,
                ))
                .take_while(|(key, _)| key.starts_with(prefix))
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect())
        }
    }

    /// WRITE-006 / INV-11: the object bytes and the receipts container are
    /// uploaded, the commit transaction fails, and the receipt is recorded in
    /// the protected orphan set.  The cleaner then cannot collect any of those
    /// objects, the record is idempotent by digest, releasing it happens
    /// exactly once, and a KV failure *before* the upload protects nothing.
    #[tokio::test]
    async fn a_kv_failure_after_a_successful_upload_protects_the_receipt() {
        let store = Arc::new(FaultyStore::default());
        let env = env_on(store.clone(), 64).await;
        store.fail_commits.store(true, Ordering::SeqCst);

        // Two *different* blocks: identical content dedups to one object, and
        // this scenario counts the protected object set explicitly.
        let mut data = vec![0x11u8; 64];
        data.extend_from_slice(&[0x22u8; 64]);
        let ticket = env
            .overlay
            .accept(
                INODE,
                MutationSpec::Write {
                    offset: 0,
                    data: Arc::new(data),
                },
            )
            .await
            .unwrap();
        env.overlay.dispatch(&ticket).await.unwrap();
        env.overlay.complete_upload(&ticket).await.unwrap();
        assert_eq!(
            env.overlay.incomplete_count(INODE).await,
            1,
            "the operation is still uncommitted at the failure point"
        );

        let report = env.overlay.drain().await.unwrap();
        assert!(
            report.committed.is_empty(),
            "the commit transaction failed, so nothing committed"
        );
        assert_eq!(
            report.failed.len(),
            1,
            "the failure is reported, not hidden"
        );
        assert!(report.blocked.is_empty());
        assert_eq!(
            report.orphaned.len(),
            1,
            "the upload is protected; reported failure was {:?}",
            report.failed.first().map(|failure| &failure.1)
        );
        let record = report.orphaned[0].clone();
        assert_eq!(record.operation_id, ticket.operation_id);
        assert_eq!(record.step, KvStep::Commit);
        assert_eq!(record.workspace_id, env.params.workspace_id);
        assert_eq!(record.domain_id, env.params.domain_id);
        assert!(
            record.reason.contains("simulated KV failure"),
            "the reason records why the commit failed: {}",
            record.reason
        );

        // Two data blocks plus the receipts container; the receipts root is
        // itself protected, so the audit trail cannot be collected.
        assert_eq!(record.objects.len(), 3);
        let receipts_root = record.receipts.clone().expect("receipts were uploaded");
        assert!(
            record
                .objects
                .iter()
                .any(|object| object.object_id == receipts_root.object.object_id)
        );
        // Every protected object is registered as an upload this domain owns.
        for object in &record.objects {
            let row = env
                .store
                .get(&env.keys.object(&env.params.domain_id, &object.object_id))
                .await
                .unwrap()
                .expect("the uploaded object is registered");
            let registration = super::super::domain::decode_registration(&row).unwrap();
            assert_eq!(registration.object_ref, *object);
            assert!(
                matches!(
                    registration.state,
                    crate::native_base::wire::bnct::RegistrationState::Dispatched
                        | crate::native_base::wire::bnct::RegistrationState::Verified
                ),
                "the upload was dispatched before the KV step failed"
            );
        }

        // The protection is durable and idempotent.
        let stored = read_orphan_receipt(&*env.store, &env.keys, &ticket.operation_id)
            .await
            .unwrap()
            .expect("the record is persisted");
        assert_eq!(stored, record);
        assert!(
            !record_orphan_receipt(&*env.store, &env.keys, &record)
                .await
                .unwrap()
        );
        assert_eq!(
            scan_orphan_receipts(&*env.store, &env.keys)
                .await
                .unwrap()
                .len(),
            1
        );

        // Protection keeps every uploaded object out of the collectable set,
        // and the delete guard refuses a plan that ignored it.
        let protected = protected_object_ids(std::iter::once(&record));
        assert_eq!(protected.len(), 3);
        let candidates = protected.clone();
        let plan = plan_collection(&candidates, &protected);
        assert!(plan.collectable.is_empty());
        assert_eq!(plan.blocked_by_protection, protected);
        ensure_not_collected(&plan, &protected).unwrap();
        let unsafe_plan = CollectionPlan {
            collectable: protected.clone(),
            blocked_by_protection: std::collections::BTreeSet::new(),
        };
        assert!(ensure_not_collected(&unsafe_plan, &protected).is_err());

        // Abandoning the operation releases the protection exactly once, after
        // which the objects are ordinary garbage.
        let release = resolve_orphan_receipt(
            &*env.store,
            &env.keys,
            &ticket.operation_id,
            OrphanResolution::Abandoned,
        )
        .await
        .unwrap();
        assert_eq!(release.released_objects.len(), 3);
        assert!(
            read_orphan_receipt(&*env.store, &env.keys, &ticket.operation_id)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            resolve_orphan_receipt(
                &*env.store,
                &env.keys,
                &ticket.operation_id,
                OrphanResolution::Committed,
            )
            .await
            .is_err()
        );
        let after = plan_collection(&candidates, &std::collections::BTreeSet::new());
        assert_eq!(after.collectable, candidates);

        // A KV failure before anything was uploaded protects nothing: there is
        // no receipt yet and no durable object.
        let outage = Arc::new(FaultyStore::default());
        let env = env_on(outage.clone(), 64).await;
        outage.fail_everything.store(true, Ordering::SeqCst);
        let ticket = env.overlay.accept(INODE, write_spec(0, 64)).await.unwrap();
        assert!(env.overlay.dispatch(&ticket).await.is_err());
        assert!(
            scan_orphan_receipts(&*env.store, &env.keys)
                .await
                .unwrap()
                .is_empty(),
            "nothing was uploaded, so nothing needs protection"
        );
        assert_eq!(
            env.overlay.incomplete_count(INODE).await,
            1,
            "the operation stayed un-uploaded; nothing was silently dropped"
        );
    }
}
