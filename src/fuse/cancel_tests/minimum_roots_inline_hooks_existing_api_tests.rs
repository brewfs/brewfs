//! Real packed metadata -> MetaLayer -> VFS Hooks; unit/provider scope only.
use super::*;

// Ask the existing validator for its smallest supported Roots capacity. Other
// pools keep their existing defaults; Control remains the existing 32768.
// No copy of the validator's internal formula or new admission constant.
fn minimum_supported_roots_budget() -> Arc<SnapshotMountBudget> {
    let mut limits = SnapshotBudgetLimits::default();
    limits.bytes[SnapshotBudgetPool::Control as usize] = 32 << 10;
    let mut lower = 1u64;
    let mut upper = limits.bytes[SnapshotBudgetPool::Roots as usize];
    while lower < upper {
        let middle = lower + (upper - lower) / 2;
        limits.bytes[SnapshotBudgetPool::Roots as usize] = middle;
        if SnapshotMountBudget::new(limits.clone())
            .unwrap()
            .validate_frame_capability(8 << 20)
            .is_ok()
        {
            upper = middle;
        } else {
            lower = middle + 1;
        }
    }
    limits.bytes[SnapshotBudgetPool::Roots as usize] = lower;
    let budget = SnapshotMountBudget::new(limits.clone()).unwrap();
    budget.validate_frame_capability(8 << 20).unwrap();
    limits.bytes[SnapshotBudgetPool::Roots as usize] = lower - 1;
    assert!(
        SnapshotMountBudget::new(limits)
            .unwrap()
            .validate_frame_capability(8 << 20)
            .is_err(),
        "one below the existing supported Roots floor"
    );
    eprintln!(
        "BREWFS_EXISTING_MINIMUM_ROOTS capacity={lower} control_capacity=32768 validator=existing_frame_capability"
    );
    budget
}

#[tokio::test]
async fn packed_minimum_roots_inline_hooks_deny_at_full_then_refund_and_recover() {
    let f = fixture_with_budget(minimum_supported_roots_budget()).await;
    let baseline = f.budget.state().used;
    let future = asyncfuse::raw::Session::<TestVfs>::readonly_ordinary_worker_future_layout();
    let prepare = asyncfuse::raw::Session::<TestVfs>::readonly_prepare_future_layout();
    eprintln!(
        "BREWFS_PACKED_MINIMUM_ROOTS_PROVIDER_LAYOUT ordinary_bytes={} ordinary_align={} prepare_child_bytes={} prepare_holder_bytes={} prepare_outer_bytes={}",
        future.0, future.1, prepare.0.0, prepare.1.0, prepare.2.0
    );
    // This calls actual hook implementations, not the private Session queue.
    for bytes in [513u64, future.0 as u64, (prepare.0.0 + prepare.1.0) as u64] {
        let before_arcs = Arc::strong_count(&f.budget);
        let permit = Filesystem::reserve_inline_root_memory(&f.fs, bytes)
            .unwrap()
            .unwrap();
        let mut expected = baseline;
        expected[SnapshotBudgetPool::Roots as usize] += bytes;
        assert_eq!(f.budget.state().used, expected);
        assert_eq!(
            Arc::strong_count(&f.budget),
            before_arcs + 1,
            "only the existing budget Arc is retained"
        );
        let moved = Some(permit);
        assert_eq!(
            f.budget.state().used,
            expected,
            "moving the inline owner cannot refund"
        );
        drop(moved);
        assert_eq!(f.budget.state().used, baseline);
        assert_eq!(Arc::strong_count(&f.budget), before_arcs);
        let prepare_permit = Filesystem::reserve_inline_prepare_memory(&f.fs, bytes)
            .unwrap()
            .unwrap();
        assert_eq!(f.budget.state().used, expected);
        drop(prepare_permit);
        assert_eq!(f.budget.state().used, baseline);
    }
    let remaining =
        f.budget.capacity(SnapshotBudgetPool::Roots) - baseline[SnapshotBudgetPool::Roots as usize];
    let held = f
        .budget
        .admit(&[(SnapshotBudgetPool::Roots, remaining)])
        .unwrap();
    let full = f.budget.state().used;
    let backend_before = f.gate.payload_ranges.lock().unwrap().len();
    assert_eq!(
        Filesystem::reserve_inline_root_memory(&f.fs, 1).unwrap_err(),
        Errno::from(libc::ENOMEM)
    );
    assert_eq!(
        Filesystem::reserve_inline_prepare_memory(&f.fs, 1).unwrap_err(),
        Errno::from(libc::ENOMEM)
    );
    assert_eq!(f.budget.state().used, full);
    assert_eq!(f.gate.payload_ranges.lock().unwrap().len(), backend_before);
    drop(held);
    assert_eq!(f.budget.state().used, baseline);
    let recovered = Filesystem::reserve_inline_root_memory(&f.fs, 1)
        .unwrap()
        .unwrap();
    drop(recovered);
    assert_eq!(f.budget.state().used, baseline);
}

#[tokio::test]
async fn packed_minimum_roots_first_valid_adapter_read_and_real_release_fit_original_control() {
    let f = fixture_with_budget(minimum_supported_roots_budget()).await;
    let opened = Filesystem::open(&f.fs, request(3901), f.ino, libc::O_RDONLY as u32)
        .await
        .unwrap();
    let request_owner = Filesystem::reserve_request_memory(&f.fs, 40)
        .unwrap()
        .unwrap();
    let reply = Filesystem::read(&f.fs, request(3902), f.ino, opened.fh, 0, 8192).await
        .expect("the existing minimum supported Roots profile cannot serve its first valid adapter read");
    assert_eq!(reply.data.as_ref(), &[0x75; 8192]);
    drop(reply);
    drop(request_owner);
    Filesystem::release(&f.fs, request(3903), f.ino, opened.fh, 0, 0, false)
        .await
        .unwrap();
    assert_eq!(f.fs.open_file_handle_count(), 0);
    assert_eq!(
        f.budget.state().used[SnapshotBudgetPool::Control as usize],
        0
    );
    assert!(f.budget.state().peak[SnapshotBudgetPool::Control as usize] <= 32768);
    assert!(
        f.budget.state().peak[SnapshotBudgetPool::Roots as usize]
            <= f.budget.capacity(SnapshotBudgetPool::Roots)
    );
    Filesystem::prepare_unmount(&f.fs).await.unwrap();
}
