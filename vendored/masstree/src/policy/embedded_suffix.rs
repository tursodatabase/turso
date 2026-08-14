//! Embedded suffix storage (always-present, no sidecar indirection).

use std::cell::UnsafeCell;
use std::cmp::Ordering;
use std::ptr as StdPtr;
use std::sync::atomic::{AtomicPtr, Ordering as AtomicOrdering};

use seize::{Guard, LocalGuard};

use crate::TreePermutation;
use crate::suffix::{InlineSuffixBag, SideCarUtils, SuffixBagCell};

use super::SuffixStore;
use super::suffix_ops as SuffixOps;

// ============================================================================
//  EmbeddedSuffix
// ============================================================================

/// Suffix storage embedded directly in the leaf struct.
#[repr(C)]
pub struct EmbeddedSuffix {
    /// Inline suffix storage (256 or 512 bytes data capacity).
    inline_ksuf: UnsafeCell<InlineSuffixBag>,

    /// External overflow for large/many suffixes (wrapped in `SuffixBagCell`
    /// for interior mutability under the leaf lock).
    external_ksuf: AtomicPtr<SuffixBagCell>,
}

// SAFETY: EmbeddedSuffix is Send+Sync.
// - UnsafeCell<InlineSuffixBag>: protected by leaf lock (writes) and OCC (reads).
//   Readers rely on leaf OCC validation; writers mutate only under the leaf lock.
// - AtomicPtr<SuffixBagCell>: thread-safe atomic access.
unsafe impl Send for EmbeddedSuffix {}
unsafe impl Sync for EmbeddedSuffix {}

impl EmbeddedSuffix {
    /// Get a reference to the inline suffix bag.
    ///
    /// # Safety
    ///
    /// For read operations: safe under leaf OCC validation.
    /// For write operations: caller must hold the leaf lock.
    #[inline(always)]
    fn inline_bag(&self) -> &InlineSuffixBag {
        // SAFETY: InlineSuffixBag uses internal atomics for metadata.
        // Read access is safe under leaf OCC validation; write access requires
        // the leaf lock.
        unsafe { &*self.inline_ksuf.get() }
    }
}

impl SuffixStore for EmbeddedSuffix {
    #[inline(always)]
    fn new() -> Self {
        Self {
            inline_ksuf: UnsafeCell::new(InlineSuffixBag::new()),
            external_ksuf: AtomicPtr::new(StdPtr::null_mut()),
        }
    }

    // ========================================================================
    //  Read Operations
    // ========================================================================

    #[inline(always)]
    fn get(&self, slot: usize) -> Option<&[u8]> {
        SuffixOps::get_suffix(self.inline_bag(), &self.external_ksuf, slot)
    }

    #[inline(always)]
    fn suffix_equals(&self, slot: usize, suffix: &[u8]) -> bool {
        SuffixOps::suffix_equals(self.inline_bag(), &self.external_ksuf, slot, suffix)
    }

    #[inline(always)]
    fn suffix_compare(&self, slot: usize, suffix: &[u8]) -> Option<Ordering> {
        SuffixOps::suffix_compare(self.inline_bag(), &self.external_ksuf, slot, suffix)
    }

    #[inline(always)]
    fn has_external(&self) -> bool {
        SuffixOps::has_external(&self.external_ksuf)
    }

    #[inline(always)]
    fn prefetch(&self) {
        // No-op: inline suffix data is already embedded in the leaf node.
    }

    // ========================================================================
    //  Write Operations
    // ========================================================================

    #[inline(always)]
    unsafe fn assign(
        &self,
        slot: usize,
        suffix: &[u8],
        perm: &impl TreePermutation,
        _guard: &LocalGuard<'_>,
    ) -> *mut u8 {
        // SAFETY: Caller holds leaf lock.
        unsafe {
            SuffixOps::assign_suffix(self.inline_bag(), &self.external_ksuf, slot, suffix, perm)
        }
    }

    #[inline(always)]
    unsafe fn assign_init(&self, slot: usize, suffix: &[u8], guard: &LocalGuard<'_>) {
        // SAFETY: Caller holds leaf lock, guard is valid.
        unsafe {
            SuffixOps::assign_suffix_init(
                self.inline_bag(),
                &self.external_ksuf,
                slot,
                suffix,
                guard,
            );
        }
    }

    #[inline(always)]
    unsafe fn assign_prealloc(
        &self,
        slot: usize,
        suffix: &[u8],
        perm: &impl TreePermutation,
        _guard: &LocalGuard<'_>,
        prealloc: Vec<u8>,
    ) -> *mut u8 {
        // SAFETY: Caller holds leaf lock.
        unsafe {
            SuffixOps::assign_suffix_prealloc(
                self.inline_bag(),
                &self.external_ksuf,
                slot,
                suffix,
                perm,
                prealloc,
            )
        }
    }

    #[inline(always)]
    unsafe fn ensure_external(&self) -> *mut SuffixBagCell {
        // SAFETY: Caller holds leaf lock.
        unsafe { SuffixOps::ensure_external_bag(&self.external_ksuf) }
    }

    #[inline(always)]
    unsafe fn clear(&self, slot: usize, _guard: &LocalGuard<'_>) {
        // SAFETY: Caller holds leaf lock.
        unsafe { SuffixOps::clear_suffix(self.inline_bag(), &self.external_ksuf, slot) }
    }

    #[inline(always)]
    unsafe fn retire_bag_ptr(ptr: *mut u8, guard: &LocalGuard<'_>) {
        if ptr.is_null() {
            return;
        }

        // SAFETY: ptr came from assign() and is a valid SuffixBagCell pointer.
        unsafe {
            guard.defer_retire(ptr.cast::<SuffixBagCell>(), |ptr, collector| {
                SideCarUtils::retire_suffix_bag_cell(ptr, collector);
            });
        }
    }

    // ========================================================================
    //  Lifecycle
    // ========================================================================

    unsafe fn drop_storage(&mut self) {
        let external: *mut SuffixBagCell = self.external_ksuf.load(AtomicOrdering::Acquire);

        if !external.is_null() {
            // SAFETY: We have exclusive access (&mut self from Drop).
            // Any previously swapped-out cells were retired separately.
            unsafe {
                drop(Box::from_raw(external));
            }
        }
    }

    unsafe fn init_at_zero(ptr: *mut Self) {
        // SAFETY: InlineSuffixBag needs header initialization.
        // The caller zeroed the memory, but the bag header may need
        // non-zero initial values.
        unsafe {
            let bag_ptr: *mut InlineSuffixBag = StdPtr::addr_of_mut!((*ptr).inline_ksuf).cast();
            StdPtr::write(bag_ptr, InlineSuffixBag::new());
        }
    }
}
