//! Inline sentinel pointer for true-inline value storage.
//!
//! This module provides `INLINE_SENTINEL_PTR`, a sentinel pointer value used
//! to distinguish terminal inline values from empty slots in true-inline leaves.

use std::ptr as StdPtr;

// ============================================================================
//  Sentinel Definition
// ============================================================================

/// Static byte used as the target for the inline sentinel pointer.
///
/// This is a real memory location (not a fake/punned pointer), ensuring
/// strict provenance compliance.
static INLINE_SENTINEL: u8 = 0;

/// Sentinel handler
#[derive(Debug)]
pub struct InlineSentinel;

impl InlineSentinel {
    /// Get the inline sentinel pointer.
    #[must_use]
    #[inline(always)]
    pub fn inline_sentinel_ptr() -> *mut u8 {
        StdPtr::from_ref(&INLINE_SENTINEL).cast_mut()
    }

    /// Check if a pointer is the inline sentinel.
    #[must_use]
    #[inline(always)]
    pub fn is_inline_sentinel(ptr: *const u8) -> bool {
        StdPtr::eq(ptr, StdPtr::from_ref(&INLINE_SENTINEL))
    }
}
