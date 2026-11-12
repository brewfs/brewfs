//! Independent admission-order case. Existing ten fh tests stay unchanged.
use super::*;
use crate::workspace::packed::snapshot::SnapshotBudgetPool;

#[tokio::test]
async fn packed_dir_namespace_exhaustion_precedes_full_metadata_admission() {
    let fixture = packed_fixture().await;
    let pool = SnapshotBudgetPool::Metadata;
    let idle = fixture.budget.state();
    let capacity = fixture.budget.capacity(pool);
    let available = capacity
        .checked_sub(idle.used[pool as usize])
        .expect("fixture Metadata usage exceeds its actual capacity");
    assert!(
        available > 0,
        "fixture must leave Metadata room before pressure setup"
    );
    // This is the production budget's real owned permit. It charges the exact
    // remaining Metadata capacity without modifying limits or allocating data.
    let pressure = fixture.budget.admit(&[(pool, available)]).unwrap();
    assert_eq!(fixture.budget.state().used[pool as usize], capacity);
    fixture
        .fs
        .state
        .handles
        .next_fh
        .store(u64::MAX, Ordering::Relaxed);
    let registry_before = registry_snapshot(&fixture.fs);
    let budget_before = fixture.budget.state();
    let error = fixture
        .fs
        .opendir(fixture.fs.root_ino())
        .await
        .expect_err("exhausted directory OPEN unexpectedly succeeded");
    let budget_after = fixture.budget.state();
    eprintln!(
        "path=packed_dir_full_metadata actual_error={error} rejections_before={} rejections_after={} metadata_owned_before={} metadata_owned_after={}",
        budget_before.rejections,
        budget_after.rejections,
        budget_before.used[pool as usize],
        budget_after.used[pool as usize],
    );
    assert_eq!(error.to_string(), "FUSE handle namespace exhausted");
    assert_eq!(
        budget_after.rejections, budget_before.rejections,
        "exhausted fh must be rejected before any failing Handle admission"
    );
    assert_eq!(budget_after.used, budget_before.used);
    assert_eq!(budget_after.peak, budget_before.peak);
    assert_eq!(budget_after.closed, budget_before.closed);
    assert_eq!(registry_snapshot(&fixture.fs), registry_before);
    assert_eq!(
        fixture.fs.state.handles.next_fh.load(Ordering::Relaxed),
        u64::MAX
    );
    drop(pressure);
    assert_eq!(
        fixture.budget.state().used,
        idle.used,
        "the actual pressure owner must return its full reservation"
    );
}
