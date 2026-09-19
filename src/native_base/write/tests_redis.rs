//! The PR04 scenarios against a real Redis instance.
//!
//! Opt-in integration gate: `BREWFS_REDIS_TEST_URL` must point at a scratch
//! Redis the tests may fill (every run writes under random volume ids, so a
//! shared scratch instance is safe but never cleaned automatically). Run
//! with `cargo test -- --ignored`. Semantics must match the in-memory
//! backend exactly (spec 07 §2).

use std::sync::Arc;

use super::redis::RedisControlStore;
use super::store::ControlStore;
use super::tests;

macro_rules! run_scenarios_on_redis {
    ($($name:ident),* $(,)?) => {
        $(
            #[tokio::test]
            #[ignore = "requires BREWFS_REDIS_TEST_URL pointing at a scratch Redis"]
            async fn $name() {
                let url = std::env::var("BREWFS_REDIS_TEST_URL")
                    .expect("BREWFS_REDIS_TEST_URL must point at a scratch Redis");
                let store: Arc<dyn ControlStore> =
                    Arc::new(RedisControlStore::connect(&url).await.unwrap());
                tests::$name(store).await;
            }
        )*
    };
}

run_scenarios_on_redis! {
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
}

/// KV-001: two *independently connected* store clients racing the same head
/// — the fencing must come from the transaction, not process-local state.
#[tokio::test]
#[ignore = "requires BREWFS_REDIS_TEST_URL pointing at a scratch Redis"]
async fn independent_stores_racing_same_head_commit_once() {
    let url = std::env::var("BREWFS_REDIS_TEST_URL")
        .expect("BREWFS_REDIS_TEST_URL must point at a scratch Redis");
    let first: Arc<dyn ControlStore> = Arc::new(RedisControlStore::connect(&url).await.unwrap());
    let second: Arc<dyn ControlStore> = Arc::new(RedisControlStore::connect(&url).await.unwrap());
    tests::independent_stores_racing_same_head_commit_once(first, second).await;
}
