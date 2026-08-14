//! Heap-allocated suffix storage sidecar.

use std::ptr as StdPtr;
use std::sync::atomic::{AtomicPtr, Ordering};

use seize::Collector;

use super::{InlineSuffixBag, SuffixBagCell};

/// Utility functions for suffix sidecar operations.
#[derive(Debug)]
pub struct SideCarUtils;

impl SideCarUtils {
    /// Cleanup function for retiring external suffix bag cells.
    ///
    /// # Safety
    ///
    /// `ptr` must be a valid [`SuffixBagCell`] pointer allocated via [`Box`].
    pub unsafe fn retire_suffix_bag_cell(ptr: *mut SuffixBagCell, _collector: &Collector) {
        // SAFETY: Caller guarantees ptr is a valid Box-allocated `SuffixBagCell`
        unsafe { drop(Box::from_raw(ptr)) };
    }
}

/// Heap-allocated suffix storage for leaves with long keys.
///
/// Contains inline storage plus an atomic pointer to external overflow.
/// Production reads and writes go through `suffix_ops` free functions;
/// the methods below are retained for unit tests only.
#[derive(Debug)]
#[repr(C)]
pub struct SuffixSidecar {
    /// Inline suffix storage.
    pub(crate) inline: InlineSuffixBag,

    /// External overflow for large suffixes (wrapped in `SuffixBagCell` for
    /// interior mutability under the leaf lock).
    pub(crate) external: AtomicPtr<SuffixBagCell>,
}

impl SuffixSidecar {
    /// Create a new empty sidecar.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            inline: InlineSuffixBag::new(),
            external: AtomicPtr::new(StdPtr::null_mut()),
        }
    }
}

// Test-only convenience methods (production code uses suffix_ops).
#[cfg(test)]
impl SuffixSidecar {
    /// Get suffix for slot, checking both inline and external.
    pub fn get(&self, slot: usize) -> Option<&[u8]> {
        if let Some(suffix) = self.inline.get(slot) {
            return Some(suffix);
        }

        let external: *mut SuffixBagCell = self.external.load(Ordering::Acquire);

        if external.is_null() {
            None
        } else {
            // SAFETY: external is valid if non-null (we own it). Test-only,
            // single-threaded access.
            unsafe { (*external).as_ref() }.get(slot)
        }
    }

    /// Get or create external storage.
    ///
    /// Returns a mutable reference to the inner `SuffixBag` for test use.
    ///
    /// # Safety
    ///
    /// Caller must have exclusive access (test-only).
    #[expect(clippy::mut_from_ref, reason = "Interior mutability via UnsafeCell")]
    pub unsafe fn ensure_external(&self) -> &mut super::SuffixBag {
        let ptr: *mut SuffixBagCell = self.external.load(Ordering::Acquire);

        if !ptr.is_null() {
            // SAFETY: Caller has exclusive access.
            return unsafe { (*ptr).as_mut() };
        }

        let new_cell: Box<SuffixBagCell> = Box::default();
        let new_ptr: *mut SuffixBagCell = Box::into_raw(new_cell);
        self.external.store(new_ptr, Ordering::Release);

        // SAFETY: Just allocated, caller has exclusive access.
        unsafe { (*new_ptr).as_mut() }
    }

    /// Check if slot has a suffix (inline or external).
    pub fn has_suffix(&self, slot: usize) -> bool {
        if self.inline.has_suffix(slot) {
            return true;
        }

        let external: *mut SuffixBagCell = self.external.load(Ordering::Acquire);

        if external.is_null() {
            false
        } else {
            // SAFETY: external is valid if non-null. Test-only, single-threaded.
            unsafe { (*external).as_ref() }.has_suffix(slot)
        }
    }
}

impl Default for SuffixSidecar {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for SuffixSidecar {
    fn drop(&mut self) {
        // SAFETY: self.external is either null (no external ever created) or
        // the most recent cell (all previous ones were retired separately via
        // defer_retire during drain-and-rebuild).
        let external: *mut SuffixBagCell = self.external.load(Ordering::Acquire);

        if !external.is_null() {
            // SAFETY: We own this external cell exclusively during drop.
            unsafe {
                drop(Box::from_raw(external));
            }
        }
    }
}

// SAFETY: `SuffixSidecar` is `Send` because `SuffixBagCell` is `Send`.
// The `AtomicPtr` provides thread-safe access to the external bag.
// Concurrent access is serialized by the leaf lock.
unsafe impl Send for SuffixSidecar {}

// SAFETY: `SuffixSidecar` is `Sync` under the leaf's OCC protocol: readers
// validate the leaf version after reads, and writers only mutate under the
// leaf lock while the leaf is marked dirty (INSERTING/SPLITTING), so
// `stable()` readers won't race with writes.
unsafe impl Sync for SuffixSidecar {}
