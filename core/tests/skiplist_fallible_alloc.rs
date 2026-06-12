//! Skip-list fallible allocation tests.
//!
//! These run as an integration test so this process can install its own
//! Turso allocator backend: `set_allocator` is process-wide and can only be
//! called once, which would interfere with other tests in a shared binary.

use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, Ordering};

use turso_core::alloc::{set_allocator, AllocError, Layout, TursoAllocBackend};
use turso_core::skiplist::{SkipMap, SkipSet};

/// When set, every allocation through the Turso allocator backend fails.
static FAIL_ALLOCATIONS: AtomicBool = AtomicBool::new(false);

struct FailOnDemandBackend;

unsafe impl TursoAllocBackend for FailOnDemandBackend {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        if FAIL_ALLOCATIONS.load(Ordering::Relaxed) {
            return Err(AllocError);
        }
        if layout.size() == 0 {
            let dangling = NonNull::new(layout.align() as *mut u8).expect("align is non-zero");
            return Ok(NonNull::slice_from_raw_parts(dangling, 0));
        }
        let ptr = unsafe { std::alloc::alloc(layout) };
        match NonNull::new(ptr) {
            Some(ptr) => Ok(NonNull::slice_from_raw_parts(ptr, layout.size())),
            None => Err(AllocError),
        }
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        if layout.size() != 0 {
            unsafe { std::alloc::dealloc(ptr.as_ptr(), layout) }
        }
    }
}

// Single test function: the fail flag and the allocator backend are process
// globals, so concurrently running test functions would interfere.
#[test]
fn skiplist_try_insert_surfaces_allocation_failure() {
    unsafe { set_allocator(&FailOnDemandBackend).expect("backend set before any allocation") };

    let map: SkipMap<i32, String> = SkipMap::new();
    map.try_insert(1, "one".to_string()).unwrap();
    assert_eq!(map.len(), 1);

    FAIL_ALLOCATIONS.store(true, Ordering::Relaxed);

    // Inserting a new key needs a node allocation, which now fails. The map
    // must be left unchanged.
    assert!(map.try_insert(2, "two".to_string()).is_err());
    assert!(map
        .try_compare_insert(2, "two".to_string(), |_| true)
        .is_err());
    assert_eq!(map.len(), 1);
    assert!(map.get(&2).is_none());

    // Finding an existing key takes the no-allocation fast path and still
    // succeeds while allocations fail.
    let entry = map.try_get_or_insert(1, "uno".to_string()).unwrap();
    assert_eq!(*entry.value(), "one");
    let entry = map.try_get_or_insert_with(1, || "uno".to_string()).unwrap();
    assert_eq!(*entry.value(), "one");

    FAIL_ALLOCATIONS.store(false, Ordering::Relaxed);

    // The map stays fully usable after a failed insert.
    map.try_insert(2, "two".to_string()).unwrap();
    assert_eq!(map.len(), 2);
    assert_eq!(*map.get(&2).unwrap().value(), "two");

    let set: SkipSet<i32> = SkipSet::new();
    set.try_insert(7).unwrap();

    FAIL_ALLOCATIONS.store(true, Ordering::Relaxed);
    assert!(set.try_insert(8).is_err());
    assert!(set.try_get_or_insert(8).is_err());
    assert_eq!(*set.try_get_or_insert(7).unwrap(), 7);
    FAIL_ALLOCATIONS.store(false, Ordering::Relaxed);

    set.try_insert(8).unwrap();
    assert!(set.contains(&8));
}
