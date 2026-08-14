//! Thread-local node pools using size-class buckets.
//!
//! Pool access is thread-local (no synchronization on fast path). Nodes may
//! return to a different thread's pool due to seize's batch reclamation.

use std::alloc::{Layout, alloc, dealloc, handle_alloc_error};
use std::cell::UnsafeCell;
use std::mem::size_of;
use std::ptr as StdPtr;

use seize::Collector;

use crate::internode::InternodeNode;
use crate::leaf15::LeafNode15;
use crate::policy::LeafPolicy;

// ============================================================================
//  Constants
// ============================================================================

const CACHE_LINE: usize = 64;

const MAX_SIZE_CLASSES: usize = 20;

/// Max cached nodes per size class per thread.
const POOL_CAPACITY: usize = 512;

/// Batch refill count from global allocator.
const REFILL_BATCH: usize = 128;

// ============================================================================
//  Size Class Computation
// ============================================================================

/// Size class (1-indexed cache-line count), or `None` if too large.
#[cfg(test)]
#[inline]
const fn size_class(layout: Layout) -> Option<usize> {
    let nl: usize = layout.size().div_ceil(CACHE_LINE);

    if nl == 0 || nl > MAX_SIZE_CLASSES {
        None
    } else {
        Some(nl)
    }
}

/// Compile-time size class for a known node type.
///
/// Panics at compile time if the type is too large for the pool.
#[inline(always)]
const fn node_size_class<T>() -> usize {
    let nl: usize = size_of::<T>().div_ceil(CACHE_LINE);

    assert!(
        nl > 0 && nl <= MAX_SIZE_CLASSES,
        "node type too large for pool"
    );

    nl
}

/// `nl * CACHE_LINE` layout with cache-line alignment.
///
/// # Safety
///
/// `nl` must be in `[1, MAX_SIZE_CLASSES]`.
#[inline]
unsafe fn bucket_layout(nl: usize) -> Layout {
    debug_assert!(nl > 0 && nl <= MAX_SIZE_CLASSES);

    // SAFETY: nl in valid range → non-zero size, power-of-2 alignment
    unsafe { Layout::from_size_align_unchecked(nl * CACHE_LINE, CACHE_LINE) }
}

// ============================================================================
//  Freelist
// ============================================================================

/// Intrusive freelist.
struct Freelist {
    head: *mut u8,
    count: usize,
}

impl Freelist {
    const fn new() -> Self {
        Self {
            head: StdPtr::null_mut(),
            count: 0,
        }
    }

    #[inline(always)]
    const fn pop(&mut self) -> Option<*mut u8> {
        if self.head.is_null() {
            return None;
        }

        let ptr: *mut u8 = self.head;

        // SAFETY: ptr from our freelist, first 8 bytes are next ptr
        self.head = unsafe { StdPtr::read(ptr.cast::<*mut u8>()) };
        self.count -= 1;

        Some(ptr)
    }

    /// # Safety
    ///
    /// `ptr` must be valid, cache-line-aligned memory of at least 8 bytes.
    #[inline(always)]
    const unsafe fn push(&mut self, ptr: *mut u8) {
        unsafe { StdPtr::write(ptr.cast::<*mut u8>(), self.head) };
        self.head = ptr;
        self.count += 1;
    }

    /// Slow path: batch-allocate `REFILL_BATCH` blocks.
    #[cold]
    fn refill(&mut self, layout: Layout) {
        for _ in 0..REFILL_BATCH {
            // SAFETY: layout is a valid bucket layout
            let ptr: *mut u8 = unsafe { alloc(layout) };

            if ptr.is_null() {
                break;
            }

            // SAFETY: freshly allocated with correct layout
            unsafe { self.push(ptr) };
        }
    }

    fn drain(&mut self, layout: Layout) {
        while let Some(ptr) = self.pop() {
            // SAFETY: ptr was allocated with this layout
            unsafe { dealloc(ptr, layout) };
        }
    }

    #[inline(always)]
    const fn has_capacity(&self) -> bool {
        self.count < POOL_CAPACITY
    }
}

// ============================================================================
//  ThreadPool
// ============================================================================

/// Per-thread pool. Index `i` = size class `i + 1` (1-indexed).
struct ThreadPool {
    buckets: [Freelist; MAX_SIZE_CLASSES],
}

impl ThreadPool {
    const fn new() -> Self {
        const EMPTY: Freelist = Freelist::new();
        Self {
            buckets: [EMPTY; MAX_SIZE_CLASSES],
        }
    }

    /// Returns null on OOM.
    #[cfg(test)]
    #[inline]
    fn alloc(&mut self, layout: Layout) -> *mut u8 {
        let Some(nl) = size_class(layout) else {
            return unsafe { alloc(layout) };
        };

        self.alloc_from_bucket(nl)
    }

    /// Fast path for known size class.
    #[inline(always)]
    fn alloc_from_bucket(&mut self, nl: usize) -> *mut u8 {
        // SAFETY: nl in [1, MAX_SIZE_CLASSES]
        let bucket: &mut Freelist = unsafe { self.buckets.get_unchecked_mut(nl - 1) };
        let bl: Layout = unsafe { bucket_layout(nl) };

        if let Some(ptr) = bucket.pop() {
            return ptr;
        }

        bucket.refill(bl);
        bucket.pop().unwrap_or(StdPtr::null_mut())
    }

    /// # Safety
    ///
    /// `ptr` must be valid memory allocated with a compatible layout.
    #[cfg(test)]
    #[inline]
    unsafe fn dealloc(&mut self, ptr: *mut u8, layout: Layout) {
        let Some(nl) = size_class(layout) else {
            unsafe { dealloc(ptr, layout) };
            return;
        };

        // SAFETY: nl in [1, MAX_SIZE_CLASSES]
        unsafe { self.dealloc_to_bucket(ptr, nl) };
    }

    /// Fast path for known size class.
    ///
    /// # Safety
    ///
    /// `ptr` must be valid memory, `nl` must be in `[1, MAX_SIZE_CLASSES]`.
    #[inline(always)]
    unsafe fn dealloc_to_bucket(&mut self, ptr: *mut u8, nl: usize) {
        // SAFETY: nl in [1, MAX_SIZE_CLASSES]
        let bucket: &mut Freelist = unsafe { self.buckets.get_unchecked_mut(nl - 1) };

        if bucket.has_capacity() {
            unsafe { bucket.push(ptr) };
        } else {
            unsafe { dealloc(ptr, bucket_layout(nl)) };
        }
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        for (i, bucket) in self.buckets.iter_mut().enumerate() {
            let nl: usize = i + 1;
            // SAFETY: nl in [1, MAX_SIZE_CLASSES]
            bucket.drain(unsafe { bucket_layout(nl) });
        }
    }
}

// ============================================================================
//  Thread-Local Access
// ============================================================================

thread_local! {
    static POOL: UnsafeCell<ThreadPool> = const { UnsafeCell::new(ThreadPool::new()) };
}

/// Allocate from the thread-local pool. Aborts on OOM.
#[cfg(test)]
#[inline]
#[must_use]
pub fn pool_alloc(layout: Layout) -> *mut u8 {
    POOL.with(|cell: &UnsafeCell<ThreadPool>| {
        // SAFETY: thread-local access is single-threaded
        let pool: &mut ThreadPool = unsafe { &mut *cell.get() };
        let ptr: *mut u8 = pool.alloc(layout);

        if ptr.is_null() {
            handle_alloc_error(layout);
        }

        ptr
    })
}

/// Return memory to the thread-local pool.
///
/// # Safety
///
/// - `ptr` must be valid memory with the given layout
/// - `layout.align()` must not exceed `CACHE_LINE` (64)
#[cfg(test)]
#[inline]
pub unsafe fn pool_dealloc(ptr: *mut u8, layout: Layout) {
    debug_assert!(
        layout.align() <= CACHE_LINE,
        "pool_dealloc: layout alignment ({}) exceeds CACHE_LINE ({})",
        layout.align(),
        CACHE_LINE,
    );

    POOL.with(|cell: &UnsafeCell<ThreadPool>| {
        let pool: &mut ThreadPool = unsafe { &mut *cell.get() };
        unsafe { pool.dealloc(ptr, layout) };
    });
}

// ============================================================================
//  Type-Specialized Alloc/Dealloc
// ============================================================================

/// Allocate a `LeafNode15<P>` from the thread-local pool. Aborts on OOM.
#[inline]
#[must_use]
pub fn pool_alloc_leaf<P: LeafPolicy>() -> *mut u8 {
    // node_size_class panics at compile time if LeafNode15<P> is too large.
    let nl: usize = const { node_size_class::<LeafNode15<P>>() };

    POOL.with(|cell: &UnsafeCell<ThreadPool>| {
        let pool: &mut ThreadPool = unsafe { &mut *cell.get() };
        let ptr: *mut u8 = pool.alloc_from_bucket(nl);

        if ptr.is_null() {
            handle_alloc_error(unsafe { bucket_layout(nl) });
        }

        ptr
    })
}

/// Allocate an `InternodeNode` from the thread-local pool. Aborts on OOM.
#[inline]
#[must_use]
pub fn pool_alloc_internode() -> *mut u8 {
    const NL: usize = node_size_class::<InternodeNode>();

    POOL.with(|cell: &UnsafeCell<ThreadPool>| {
        let pool: &mut ThreadPool = unsafe { &mut *cell.get() };
        let ptr: *mut u8 = pool.alloc_from_bucket(NL);

        if ptr.is_null() {
            handle_alloc_error(unsafe { bucket_layout(NL) });
        }

        ptr
    })
}

/// Return a `LeafNode15<P>` to the thread-local pool.
///
/// # Safety
///
/// `ptr` must be valid memory originally allocated via `pool_alloc_leaf`.
#[inline]
pub unsafe fn pool_dealloc_leaf<P: LeafPolicy>(ptr: *mut u8) {
    let nl: usize = const { node_size_class::<LeafNode15<P>>() };

    POOL.with(|cell: &UnsafeCell<ThreadPool>| {
        let pool: &mut ThreadPool = unsafe { &mut *cell.get() };

        unsafe { pool.dealloc_to_bucket(ptr, nl) };
    });
}

/// Return an `InternodeNode` to the thread-local pool.
///
/// # Safety
///
/// `ptr` must be valid memory originally allocated via `pool_alloc_internode`.
#[inline]
pub unsafe fn pool_dealloc_internode(ptr: *mut u8) {
    const NL: usize = node_size_class::<InternodeNode>();

    POOL.with(|cell: &UnsafeCell<ThreadPool>| {
        let pool: &mut ThreadPool = unsafe { &mut *cell.get() };

        unsafe { pool.dealloc_to_bucket(ptr, NL) };
    });
}

// ============================================================================
//  Teardown-Specific Dealloc
// ============================================================================

/// Deallocate during tree teardown.
///
/// # Safety
///
/// - `ptr` must be valid memory allocated via `pool_alloc` with the given layout
/// - `layout.align()` must not exceed `CACHE_LINE` (64)
#[cfg(test)]
#[inline]
pub unsafe fn pool_teardown_dealloc(ptr: *mut u8, layout: Layout) {
    debug_assert!(
        layout.align() <= CACHE_LINE,
        "pool_teardown_dealloc: layout alignment ({}) exceeds CACHE_LINE ({})",
        layout.align(),
        CACHE_LINE,
    );

    let Some(nl) = size_class(layout) else {
        // SAFETY: caller guarantees valid ptr/layout
        unsafe { dealloc(ptr, layout) };
        return;
    };

    // SAFETY: nl in [1, MAX_SIZE_CLASSES], caller guarantees valid ptr
    unsafe { dealloc(ptr, bucket_layout(nl)) };
}

/// Teardown dealloc for `LeafNode15<P>` — bypasses pool, compile-time size class.
///
/// # Safety
///
/// `ptr` must be valid memory allocated via `pool_alloc_leaf`.
#[inline]
pub unsafe fn pool_teardown_dealloc_leaf<P: LeafPolicy>(ptr: *mut u8) {
    let nl: usize = const { node_size_class::<LeafNode15<P>>() };
    // SAFETY: nl validated at compile time, caller guarantees valid ptr
    unsafe { dealloc(ptr, bucket_layout(nl)) };
}

/// Teardown dealloc for `InternodeNode` — bypasses pool, compile-time size class.
///
/// # Safety
///
/// `ptr` must be valid memory allocated via `pool_alloc_internode`.
#[inline]
pub unsafe fn pool_teardown_dealloc_internode(ptr: *mut u8) {
    const NL: usize = node_size_class::<InternodeNode>();
    // SAFETY: NL validated at compile time, caller guarantees valid ptr
    unsafe { dealloc(ptr, bucket_layout(NL)) };
}

// ============================================================================
//  Capture-Free Reclaimers (for `guard.defer_retire()`)
// ============================================================================

/// Reclaim a `LeafNode15<P>`.
///
/// # Safety
///
/// `ptr` must point to a valid `LeafNode15<P>`.
#[inline]
pub unsafe fn reclaim_leaf15<P: LeafPolicy>(ptr: *mut LeafNode15<P>, _collector: &Collector) {
    unsafe { StdPtr::drop_in_place(ptr) };

    unsafe { pool_dealloc_leaf::<P>(ptr.cast()) };
}

/// # Safety
///
/// `ptr` must point to a valid `InternodeNode`.
#[inline]
pub unsafe fn reclaim_internode(ptr: *mut InternodeNode, _collector: &Collector) {
    unsafe { StdPtr::drop_in_place(ptr) };

    unsafe { pool_dealloc_internode(ptr.cast()) };
}

// ============================================================================
//  Tests
// ============================================================================

