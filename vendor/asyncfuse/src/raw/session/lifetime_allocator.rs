//! Allocator witness for actual dealloc-return boundaries. UNRUN.
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

pub(crate) static SERIAL: Mutex<()> = Mutex::new(());
// The table's buckets and lane heads add two actual allocations to witness.
static WATCH: [AtomicUsize; 6] = [const { AtomicUsize::new(0) }; 6];
pub(crate) static DEALLOCATED: AtomicUsize = AtomicUsize::new(0);
pub(crate) static EXPECTED: AtomicUsize = AtomicUsize::new(0);
std::thread_local! {
    static CAPTURE_SHARED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}
struct WitnessAllocator;
#[global_allocator]
static ALLOCATOR: WitnessAllocator = WitnessAllocator;

unsafe impl GlobalAlloc for WitnessAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if layout.size() == 3 * std::mem::size_of::<usize>()
            && layout.align() == std::mem::align_of::<usize>()
            && CAPTURE_SHARED
                .try_with(std::cell::Cell::get)
                .unwrap_or(false)
        {
            WATCH[1].store(ptr as usize, Ordering::Release);
        }
        ptr
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // Record only AFTER actual allocator deallocation returns.
        unsafe {
            System.dealloc(ptr, layout);
        }
        for (index, address) in WATCH.iter().enumerate() {
            if address.load(Ordering::Acquire) == ptr as usize {
                DEALLOCATED.fetch_or(1 << index, Ordering::AcqRel);
            }
        }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        unsafe { System.realloc(ptr, layout, size) }
    }
}
pub(crate) fn watch(pointers: [usize; 4]) {
    DEALLOCATED.store(0, Ordering::Release);
    EXPECTED.store(0, Ordering::Release);
    for (index, address) in WATCH.iter().enumerate() {
        address.store(pointers.get(index).copied().unwrap_or(0), Ordering::Release);
    }
}
pub(crate) fn clear() {
    CAPTURE_SHARED.set(false);
    for address in &WATCH {
        address.store(0, Ordering::Release);
    }
}
pub(crate) fn capture_shared(active: bool) {
    CAPTURE_SHARED.set(active);
}
pub(crate) fn captured_shared() -> usize {
    WATCH[1].load(Ordering::Acquire)
}
pub(crate) fn append_watch(pointer: usize) {
    for (index, address) in WATCH.iter().enumerate() {
        if address
            .compare_exchange(0, pointer, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            EXPECTED.fetch_or(1 << index, Ordering::AcqRel);
            return;
        }
    }
    panic!("fixed witness slots exhausted");
}
