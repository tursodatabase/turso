//! Compile-time layout assertions for [`InternodeNode`].
//!
//! This module verifies that [`InternodeNode`] maintains its intended cache-line
//! layout at compile time. Any refactoring that changes field offsets will
//! cause a build failure with a clear error message.
//!
//! # Cache Line Strategy
//!
//! ```text
//! CL 0 (0-63):     version (4B) + nkeys (1B) + height (1B) + _pad (2B)
//!                  + parent (8B) + ikey0[0..=5] (6 keys, 48B)
//! CL 1 (64-127):   ikey0[6..=13] (8 keys, 64B)
//! CL 2 (128-191):  ikey0[14] (8B) + child[0..=6] (7 ptrs, 56B)
//! CL 3 (192-255):  child[7..=14] (8 ptrs, 64B)
//! CL 4 (256-319):  child[15] (8B) + 56B tail padding
//! ```
//!
//! # Hot Path (descent) Cache Lines
//!
//! - CL 0: version + nkeys + height + ikey0[0..=5]
//! - CL 1: ikey0[6..=13] (if n > 6)
//! - CL 2: ikey0[14] (if n > 13) + child[0..=6]
//! - CL 3: child[7..=14] (if `child_idx` >= 7)

#![expect(
    clippy::items_after_statements,
    reason = "Looks alright for compilation time checks."
)]

use std::mem as StdMem;

use super::{InternodeNode, NUM_CHILDREN, WIDTH};
use crate::nodeversion::NodeVersion;

// ============================================================================
//  Size and Alignment Assertions
// ============================================================================

/// Verify [`InternodeNode`] size and alignment.
///
/// Note: These assertions assume `target_pointer_width = 64`.
#[cfg(target_pointer_width = "64")]
const _: () = {
    use std::sync::atomic::{AtomicPtr, AtomicU8, AtomicU64};

    const SIZE: usize = StdMem::size_of::<InternodeNode>();
    const ALIGN: usize = StdMem::align_of::<InternodeNode>();

    // Raw size: 16 (header) + 120 (keys) + 128 (children) = 264 bytes
    // Padded to 320 bytes for 64-byte alignment (5 cache lines)
    assert!(SIZE == 320);
    assert!(SIZE == 5 * 64); // Exactly 5 cache lines

    // Must be cache-line aligned
    assert!(ALIGN == 64);

    // Component sizes
    assert!(StdMem::size_of::<NodeVersion>() == 4);
    assert!(StdMem::size_of::<AtomicU8>() == 1);
    assert!(StdMem::size_of::<AtomicU64>() == 8);
    assert!(StdMem::size_of::<AtomicPtr<u8>>() == 8);

    // Key array: 15 keys * 8 bytes = 120 bytes
    assert!(StdMem::size_of::<[AtomicU64; WIDTH]>() == 120);

    // Child array: 16 children * 8 bytes = 128 bytes
    assert!(StdMem::size_of::<[AtomicPtr<u8>; NUM_CHILDREN]>() == 128);
};

// ============================================================================
//  Field Offset Assertions
// ============================================================================

/// Verify critical field offsets for cache line optimization.
#[cfg(target_pointer_width = "64")]
const _: () = {
    use std::mem::offset_of;

    // Cache Line 0: Header fields (16 bytes) + first 6 keys (48 bytes)
    assert!(offset_of!(InternodeNode, version) == 0);
    assert!(offset_of!(InternodeNode, nkeys) == 4);
    assert!(offset_of!(InternodeNode, height) == 5);
    assert!(offset_of!(InternodeNode, _pad) == 6);
    assert!(offset_of!(InternodeNode, parent) == 8);
    assert!(offset_of!(InternodeNode, ikey0) == 16);

    // ikey0[6] should be at offset 64 (exactly CL 1 boundary)
    // ikey0 starts at 16, so ikey0[6] = 16 + 6*8 = 64
    const IKEY6_OFFSET: usize = 16 + 6 * 8;
    assert!(IKEY6_OFFSET == 64);

    // Child array follows keys: 16 + 120 = 136
    assert!(offset_of!(InternodeNode, child) == 136);

    // Verify child[0..=6] fits in CL 2 (128-191)
    // child starts at 136, so child[6] = 136 + 6*8 = 184 (within CL 2)
    const CHILD6_OFFSET: usize = 136 + 6 * 8;
    assert!(CHILD6_OFFSET == 184);
    assert!(CHILD6_OFFSET < 192);

    // child[7] starts CL 3
    const CHILD7_OFFSET: usize = 136 + 7 * 8;
    assert!(CHILD7_OFFSET == 192);
};

// ============================================================================
//  Cache Line Boundary Assertions
// ============================================================================

/// Verify cache line boundaries are respected.
#[cfg(target_pointer_width = "64")]
const _: () = {
    use std::mem::offset_of;

    // Header must fit in first 16 bytes
    const HEADER_END: usize = offset_of!(InternodeNode, ikey0);
    assert!(HEADER_END == 16);

    // First 6 keys (48 bytes) must fit in remainder of CL 0
    // CL 0 has 64 bytes, header uses 16, leaving 48 for keys
    const KEYS_IN_CL0: usize = (64 - HEADER_END) / 8;
    assert!(KEYS_IN_CL0 == 6);

    // ikey0[6] must be at CL 1 boundary (offset 64)
    const IKEY0_START: usize = offset_of!(InternodeNode, ikey0);
    const IKEY6_OFFSET: usize = IKEY0_START + 6 * 8;
    assert!(IKEY6_OFFSET == 64);
};
