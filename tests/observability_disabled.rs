//! This integration test links the production library without cfg(test), so
//! the legacy test-only counters cannot conceal a production gating mistake.
#![cfg(not(feature = "observability"))]

#[test]
fn production_diagnostics_are_zero_sized_noops() {
    use brewfs::vfs::diagnostic::{Counter, Instant};
    use std::sync::atomic::Ordering::Relaxed;
    assert_eq!(std::mem::size_of::<Counter>(), 0);
    assert_eq!(std::mem::size_of::<Instant>(), 0);
    assert_eq!(std::mem::size_of::<brewfs::vfs::stats::OpTimer<'_>>(), 0);
    assert_eq!(
        std::mem::size_of::<brewfs::vfs::stats::MaybeOpTimer<'_>>(),
        0
    );
    let counter = Counter::new(42);
    counter.fetch_add(1, Relaxed);
    counter.store(100, Relaxed);
    assert_eq!(counter.load(Relaxed), 0);
    assert_eq!(Instant::now().elapsed(), std::time::Duration::ZERO);
    assert!(brewfs::vfs::stats::FsStats::new().render().is_empty());
    assert_eq!(
        brewfs::timed_op!(panic!("count evaluated"), panic!("time evaluated"), { 17 }),
        17
    );
    assert_eq!(brewfs::observe_fuse!(panic!("label evaluated"), { 19 }), 19);
}
