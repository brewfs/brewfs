//! Root compiler-size hook; no fixture/server/mount setup or FS side effect.
use super::*;

#[test]
fn actual_packed_outer_worker_future_layout_receipt() {
    // TestVfs is the existing real VFS<PackedBlockStore<PausedBackend>,
    // PackedReadonlyMeta<PausedBackend>> specialization in cancel_tests.rs.
    // Recompile after every production client-fence/adapter change.
    let (bytes, align) =
        asyncfuse::raw::Session::<TestVfs>::readonly_ordinary_worker_future_layout();
    eprintln!(
        "BREWFS_ACTUAL_OUTER_WORKER_FUTURE bytes={bytes} align={align} specialization=packed_paused_backend_fixture"
    );
    assert!(bytes > 0);
    assert!(bytes <= isize::MAX as usize);
    assert!(align.is_power_of_two());
    // This is a compiler metric, not an 8192-byte coverage or Roots admission
    // assertion. In particular this does not certify private ready node cost.
}

#[test]
fn actual_packed_preparation_factory_fits_fixed_roots_receipt() {
    let (child, holder, outer) =
        asyncfuse::raw::Session::<TestVfs>::readonly_prepare_future_layout();
    eprintln!(
        "BREWFS_ACTUAL_PREPARATION_FACTORY child_bytes={} child_align={} holder_bytes={} holder_align={} outer_bytes={} outer_align={} specialization=packed_paused_backend_fixture",
        child.0, child.1, holder.0, holder.1, outer.0, outer.1
    );
    for (bytes, align) in [child, holder, outer] {
        assert!(bytes > 0);
        assert!(align.is_power_of_two());
    }
    // Exact named production outer factory, retaining the original 512/304/32
    // admission bound. No filesystem value, allocation, or poll is required.
    assert!(outer.0 + 304 + 32 <= 512);
    // The separately admitted complete child still needs its permit's final
    // allocator retirement proof; this size receipt does not certify all Roots.
}
