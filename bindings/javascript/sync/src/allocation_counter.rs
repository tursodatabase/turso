use napi_derive::napi;

#[napi]
pub fn native_allocated_bytes() -> Option<i64> {
    #[cfg(feature = "leak-check")]
    {
        Some(counting::LIVE_BYTES.load(std::sync::atomic::Ordering::Relaxed))
    }
    #[cfg(not(feature = "leak-check"))]
    {
        None
    }
}

#[cfg(feature = "leak-check")]
mod counting {
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::sync::atomic::{AtomicI64, Ordering};

    pub static LIVE_BYTES: AtomicI64 = AtomicI64::new(0);

    struct CountingAllocator;

    unsafe impl GlobalAlloc for CountingAllocator {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            let ptr = System.alloc(layout);
            if !ptr.is_null() {
                LIVE_BYTES.fetch_add(layout.size() as i64, Ordering::Relaxed);
            }
            ptr
        }

        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            let ptr = System.alloc_zeroed(layout);
            if !ptr.is_null() {
                LIVE_BYTES.fetch_add(layout.size() as i64, Ordering::Relaxed);
            }
            ptr
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            System.dealloc(ptr, layout);
            LIVE_BYTES.fetch_sub(layout.size() as i64, Ordering::Relaxed);
        }

        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            let new_ptr = System.realloc(ptr, layout, new_size);
            if !new_ptr.is_null() {
                LIVE_BYTES.fetch_add(new_size as i64 - layout.size() as i64, Ordering::Relaxed);
            }
            new_ptr
        }
    }

    #[global_allocator]
    static ALLOCATOR: CountingAllocator = CountingAllocator;
}
