use core::ptr::NonNull;

use crate::alloc::{ApiAllocator as _, Layout, TursoAllocator};

// Local modification (not upstream): node allocations are routed through the
// process-wide Turso allocator backend instead of `std::alloc`, and failures
// are reported to the caller instead of aborting.
//
// Note: unlike alloc::alloc::Global that returns NonNull<[u8]>,
// this returns NonNull<u8>.
pub(crate) struct Global;

#[allow(clippy::unused_self)]
impl Global {
    #[inline]
    #[cfg_attr(miri, track_caller)] // even without panics, this helps for Miri backtraces
    pub(crate) fn allocate(self, layout: Layout) -> Option<NonNull<u8>> {
        TursoAllocator.allocate(layout).ok().map(|ptr| ptr.cast())
    }

    #[inline]
    #[cfg_attr(miri, track_caller)] // even without panics, this helps for Miri backtraces
    pub(crate) unsafe fn deallocate(self, ptr: NonNull<u8>, layout: Layout) {
        // SAFETY: the caller is obligated to pass a block previously returned
        // by `allocate` together with the layout it was allocated with.
        unsafe { TursoAllocator.deallocate(ptr, layout) }
    }
}
