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
use super::memory::MemoryControlStore;
use super::overlay::{MutationSpec, OverlayParams, WriteOverlay, ensure_workspace_head};
use super::receipts::{ReceiptSet, build_receipts_container};
use super::records::{ExtentKind, HeadState, NativeExtent};
use super::store::ControlStore;
use crate::native_base::wire::bnct::{ControlRecord, DomainState};
use crate::native_base::wire::refs::{Hash32, ObjectId, RootRef};

const INODE: u64 = 7;

struct Env {
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
    let overlay = WriteOverlay::with_memory_sink(store.clone(), params.clone());
    overlay.ensure_domain(random_id()).await.unwrap();
    Env {
        store,
        keys,
        params,
        overlay,
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
}
