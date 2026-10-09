//! Coordination of the allocator between a host process and the extensions it loads.

use core::alloc::{GlobalAlloc, Layout};
use core::ptr;
use core::sync::atomic::{AtomicPtr, Ordering};

/// Allocates `size` bytes aligned to `align` in the process that loaded this extension.
pub type ExtAllocFn = unsafe extern "C" fn(size: usize, align: usize) -> *mut u8;

/// Grows or shrinks to `new_size` a block that [`ExtAllocFn`] returned.
pub type ExtReallocFn =
    unsafe extern "C" fn(ptr: *mut u8, size: usize, align: usize, new_size: usize) -> *mut u8;

/// Frees a block that [`ExtAllocFn`] returned.
pub type ExtDeallocFn = unsafe extern "C" fn(ptr: *mut u8, size: usize, align: usize);

pub type HostAllocatorFns = (ExtAllocFn, ExtReallocFn, ExtDeallocFn);

static HOST_ALLOC: AtomicPtr<()> = AtomicPtr::new(ptr::null_mut());
static HOST_REALLOC: AtomicPtr<()> = AtomicPtr::new(ptr::null_mut());
static HOST_DEALLOC: AtomicPtr<()> = AtomicPtr::new(ptr::null_mut());

/// Makes this extension allocate from the allocator of the process that loaded it.
///
/// The host and the extension were linked separately, so each of them got its own copy of
/// the allocator they use, and a copy only accepts the pointers it handed out itself. The
/// host calls this before `register_extension`, so that module names, table schemas,
/// returned values, virtual tables and VFS instances all come from one allocator.
///
/// # Safety
/// `alloc`, `realloc` and `dealloc` must be one matching allocator, and they must stay
/// callable for as long as this extension is loaded.
pub unsafe fn install_host_allocator(
    alloc: ExtAllocFn,
    realloc: ExtReallocFn,
    dealloc: ExtDeallocFn,
) {
    HOST_REALLOC.store(realloc as *mut (), Ordering::Relaxed);
    HOST_DEALLOC.store(dealloc as *mut (), Ordering::Relaxed);
    HOST_ALLOC.store(alloc as *mut (), Ordering::Release);
}

fn host_allocator() -> Option<HostAllocatorFns> {
    let alloc = HOST_ALLOC.load(Ordering::Acquire);
    if alloc.is_null() {
        return None;
    }
    unsafe {
        Some((
            core::mem::transmute::<*mut (), ExtAllocFn>(alloc),
            core::mem::transmute::<*mut (), ExtReallocFn>(HOST_REALLOC.load(Ordering::Relaxed)),
            core::mem::transmute::<*mut (), ExtDeallocFn>(HOST_DEALLOC.load(Ordering::Relaxed)),
        ))
    }
}

/// Global allocator of a dynamically loaded extension.
///
/// Every allocation is routed to the allocator of the loading host, so that the host can
/// free it. Until a host installs its allocator the [`GlobalAlloc`] given as `fallback`
/// is used, which is what an extension loaded by a process that never installs one gets.
pub struct HostAllocator<F>(pub F);

unsafe impl<F> Sync for HostAllocator<F> {}

unsafe impl<F: GlobalAlloc> GlobalAlloc for HostAllocator<F> {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        match host_allocator() {
            Some((alloc, _, _)) => unsafe { alloc(layout.size(), layout.align()) },
            None => unsafe { self.0.alloc(layout) },
        }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let Some((alloc, _, _)) = host_allocator() else {
            return unsafe { self.0.alloc_zeroed(layout) };
        };
        let ptr = unsafe { alloc(layout.size(), layout.align()) };
        if !ptr.is_null() {
            unsafe { ptr::write_bytes(ptr, 0, layout.size()) };
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        match host_allocator() {
            Some((_, _, dealloc)) => unsafe { dealloc(ptr, layout.size(), layout.align()) },
            None => unsafe { self.0.dealloc(ptr, layout) },
        }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        match host_allocator() {
            Some((_, realloc, _)) => unsafe {
                realloc(ptr, layout.size(), layout.align(), new_size)
            },
            None => unsafe { self.0.realloc(ptr, layout, new_size) },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::sync::atomic::AtomicUsize;

    static HOST_ALLOC_CALLS: AtomicUsize = AtomicUsize::new(0);
    static HOST_REALLOC_CALLS: AtomicUsize = AtomicUsize::new(0);
    static HOST_DEALLOC_CALLS: AtomicUsize = AtomicUsize::new(0);

    fn host_alloc_calls() -> usize {
        HOST_ALLOC_CALLS.load(Ordering::Relaxed)
    }

    unsafe extern "C" fn recording_alloc(size: usize, align: usize) -> *mut u8 {
        HOST_ALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
        unsafe { std::alloc::alloc(Layout::from_size_align_unchecked(size, align)) }
    }

    unsafe extern "C" fn recording_realloc(
        ptr: *mut u8,
        size: usize,
        align: usize,
        new_size: usize,
    ) -> *mut u8 {
        HOST_REALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
        unsafe {
            std::alloc::realloc(
                ptr,
                Layout::from_size_align_unchecked(size, align),
                new_size,
            )
        }
    }

    unsafe extern "C" fn recording_dealloc(ptr: *mut u8, size: usize, align: usize) {
        HOST_DEALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
        unsafe { std::alloc::dealloc(ptr, Layout::from_size_align_unchecked(size, align)) }
    }

    /// The fallback is checked before installing anything, so this must be a single test.
    #[test]
    fn allocations_reach_the_host_allocator_once_it_is_installed() {
        let layout = Layout::from_size_align(64, 8).unwrap();
        let allocator = HostAllocator(std::alloc::System);

        let before = unsafe { allocator.alloc(layout) };
        assert_eq!(host_alloc_calls(), 0, "the fallback handled the allocation");
        unsafe { allocator.dealloc(before, layout) };

        unsafe { install_host_allocator(recording_alloc, recording_realloc, recording_dealloc) };

        let allocated = unsafe { allocator.alloc(layout) };
        assert_eq!(
            host_alloc_calls(),
            1,
            "the host allocator handled the allocation"
        );
        let zeroed = unsafe { allocator.alloc_zeroed(layout) };
        assert_eq!(
            host_alloc_calls(),
            2,
            "the host allocator handled the zeroed allocation"
        );
        assert_eq!(
            unsafe { core::slice::from_raw_parts(zeroed, 64) },
            [0u8; 64]
        );

        let grown = unsafe { allocator.realloc(allocated, layout, 128) };
        assert_eq!(HOST_REALLOC_CALLS.load(Ordering::Relaxed), 1);

        unsafe { allocator.dealloc(grown, layout) };
        unsafe { allocator.dealloc(zeroed, layout) };
        assert_eq!(HOST_DEALLOC_CALLS.load(Ordering::Relaxed), 2);
    }
}
