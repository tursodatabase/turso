use core::{alloc::Layout, ptr::NonNull};

// Based on unstable alloc::alloc::Global.
//
// Note: unlike alloc::alloc::Global that returns NonNull<[u8]>,
// this returns NonNull<u8>.
pub(crate) struct Global;
#[allow(clippy::unused_self)]
impl Global {
    #[inline]
    #[cfg_attr(miri, track_caller)] // even without panics, this helps for Miri backtraces
    fn alloc_impl(&self, layout: Layout, zeroed: bool) -> Option<NonNull<u8>> {
        // Layout::dangling is unstable
        #[inline]
        #[must_use]
        fn dangling(layout: Layout) -> NonNull<u8> {
            // SAFETY: align is guaranteed to be non-zero
            unsafe { NonNull::new_unchecked(without_provenance_mut::<u8>(layout.align())) }
        }

        match layout.size() {
            0 => Some(dangling(layout)),
            // SAFETY: `layout` is non-zero in size,
            _size => unsafe {
                #[allow(clippy::disallowed_methods)] // we are in alloc_helper
                let raw_ptr = if zeroed {
                    std::alloc::alloc_zeroed(layout)
                } else {
                    std::alloc::alloc(layout)
                };
                NonNull::new(raw_ptr)
            },
        }
    }
    #[allow(dead_code)]
    #[inline]
    #[cfg_attr(miri, track_caller)] // even without panics, this helps for Miri backtraces
    pub(crate) fn allocate(self, layout: Layout) -> Option<NonNull<u8>> {
        self.alloc_impl(layout, false)
    }
    #[allow(dead_code)]
    #[inline]
    #[cfg_attr(miri, track_caller)] // even without panics, this helps for Miri backtraces
    pub(crate) fn allocate_zeroed(self, layout: Layout) -> Option<NonNull<u8>> {
        self.alloc_impl(layout, true)
    }
    #[allow(dead_code)]
    #[inline]
    #[cfg_attr(miri, track_caller)] // even without panics, this helps for Miri backtraces
    pub(crate) unsafe fn deallocate(self, ptr: NonNull<u8>, layout: Layout) {
        if layout.size() != 0 {
            // SAFETY:
            // * We have checked that `layout` is non-zero in size.
            // * The caller is obligated to provide a layout that "fits", and in this case,
            //   "fit" always means a layout that is equal to the original, because our
            //   `allocate()`, `grow()`, and `shrink()` implementations never returns a larger
            //   allocation than requested.
            // * Other conditions must be upheld by the caller, as per `Allocator::deallocate()`'s
            //   safety documentation.
            #[allow(clippy::disallowed_methods)] // we are in alloc_helper
            unsafe {
                std::alloc::dealloc(ptr.as_ptr(), layout)
            }
        }
    }
}

#[inline(always)]
#[must_use]
const fn without_provenance_mut<T>(addr: usize) -> *mut T {
    // An int-to-pointer transmute currently has exactly the intended semantics: it creates a
    // pointer without provenance. Note that this is *not* a stable guarantee about transmute
    // semantics, it relies on sysroot crates having special status.
    // SAFETY: every valid integer is also a valid pointer (as long as you don't dereference that
    // pointer).
    #[cfg(miri)]
    unsafe {
        core::mem::transmute(addr)
    }
    // Using transmute doesn't work with CHERI: https://github.com/kent-weak-memory/rust/blob/0c0ca909de877f889629057e1ddf139527446d75/library/core/src/ptr/mod.rs#L607
    #[cfg(not(miri))]
    {
        addr as *mut T
    }
}
