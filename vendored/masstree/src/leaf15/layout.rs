//! Compile-time layout assertions for [`LeafNode15`].
//!
//! Verifies size, alignment, and field offsets for both policy configurations.

#![expect(clippy::items_after_statements, reason = "Compile time checks")]

use std::mem as StdMem;

use super::LeafNode15;
use crate::nodeversion::NodeVersion;
use crate::permuter::AtomicPermuter15;
use crate::policy::{BoxPolicy, InlinePolicy};

// ============================================================================
//  Size and Alignment — BoxPolicy
// ============================================================================

#[cfg(target_pointer_width = "64")]
const _: () = {
    use super::WIDTH_15;
    use std::sync::atomic::{AtomicPtr, AtomicU8, AtomicU64};

    // BoxPolicy<u64> leaf: exactly 448 bytes (7 cache lines)
    assert!(StdMem::size_of::<LeafNode15<BoxPolicy<u64>>>() == 448);
    assert!(StdMem::align_of::<LeafNode15<BoxPolicy<u64>>>() == 64);

    // Component sizes
    assert!(StdMem::size_of::<NodeVersion>() == 4);
    assert!(StdMem::size_of::<AtomicPermuter15>() == 8);
    assert!(StdMem::size_of::<[AtomicU64; WIDTH_15]>() == 120);
    assert!(StdMem::size_of::<[AtomicU8; WIDTH_15]>() == 15);
    assert!(StdMem::size_of::<[AtomicPtr<u8>; WIDTH_15]>() == 120);
};

// ============================================================================
//  Field Offsets — BoxPolicy
// ============================================================================

#[cfg(target_pointer_width = "64")]
const _: () = {
    use std::mem::offset_of;

    type Leaf = LeafNode15<BoxPolicy<u64>>;

    // Cache Line 0: Version and metadata
    assert!(offset_of!(Leaf, version) == 0);
    assert!(offset_of!(Leaf, modstate) == 4);
    assert!(offset_of!(Leaf, _pad0) == 5);

    // Cache Line 1: Permutation (isolated for CAS performance)
    assert!(offset_of!(Leaf, permutation) == 64);
    assert!(offset_of!(Leaf, _pad1) == 72);

    // Cache Line 2+: Keys
    assert!(offset_of!(Leaf, ikey0) == 128);
    assert!(offset_of!(Leaf, keylenx) == 248);

    // Values: BoxValueArray starts at 264 (248 + 15 + 1 pad)
    assert!(offset_of!(Leaf, values) == 264);

    // Suffix: SidecarSuffix at 384 (264 + 120)
    assert!(offset_of!(Leaf, suffix) == 384);

    // Navigation pointers in cache line 6
    assert!(offset_of!(Leaf, next) == 392);
    assert!(offset_of!(Leaf, prev) == 400);
    assert!(offset_of!(Leaf, parent) == 408);
};

// ============================================================================
//  Size and Alignment — InlinePolicy (default: EmbeddedSuffix, 512-byte capacity)
// ============================================================================

#[cfg(all(
    target_pointer_width = "64",
    not(feature = "sidecar-suffix"),
    not(feature = "small-suffix-capacity")
))]
const _: () = {
    // InlinePolicy<u64> leaf: 1152 bytes (18 cache lines) with embedded suffix (512-byte inline bag).
    assert!(StdMem::size_of::<LeafNode15<InlinePolicy<u64>>>() == 1152);
    assert!(StdMem::align_of::<LeafNode15<InlinePolicy<u64>>>() == 64);
};

// ============================================================================
//  Size and Alignment — InlinePolicy (EmbeddedSuffix, 256-byte capacity)
// ============================================================================

#[cfg(all(
    target_pointer_width = "64",
    not(feature = "sidecar-suffix"),
    feature = "small-suffix-capacity"
))]
const _: () = {
    // InlinePolicy<u64> leaf: 896 bytes (14 cache lines) with embedded suffix (256-byte inline bag).
    assert!(StdMem::size_of::<LeafNode15<InlinePolicy<u64>>>() == 896);
    assert!(StdMem::align_of::<LeafNode15<InlinePolicy<u64>>>() == 64);
};

// ============================================================================
//  Size and Alignment — InlinePolicy (sidecar-suffix feature)
// ============================================================================

#[cfg(all(target_pointer_width = "64", feature = "sidecar-suffix"))]
const _: () = {
    // InlinePolicy<u64> leaf: 576 bytes (9 cache lines) with sidecar suffix (8-byte pointer).
    assert!(StdMem::size_of::<LeafNode15<InlinePolicy<u64>>>() == 576);
    assert!(StdMem::align_of::<LeafNode15<InlinePolicy<u64>>>() == 64);
};

// ============================================================================
//  Field Offsets — InlinePolicy
// ============================================================================

#[cfg(target_pointer_width = "64")]
const _: () = {
    use std::mem::offset_of;

    type Leaf = LeafNode15<InlinePolicy<u64>>;

    // Shared early fields: identical offsets to BoxPolicy
    assert!(offset_of!(Leaf, version) == 0);
    assert!(offset_of!(Leaf, modstate) == 4);
    assert!(offset_of!(Leaf, permutation) == 64);
    assert!(offset_of!(Leaf, ikey0) == 128);
    assert!(offset_of!(Leaf, keylenx) == 248);

    // Values: InlineValueArray starts at 264 (same as BoxPolicy)
    assert!(offset_of!(Leaf, values) == 264);

    // Suffix always at 504 (264 + 240). Size differs by suffix strategy.
    assert!(offset_of!(Leaf, suffix) == 504);
};

// ============================================================================
//  Cache Line Boundary Assertions (shared early fields only)
// ============================================================================

#[cfg(target_pointer_width = "64")]
const _: () = {
    use std::mem::offset_of;

    type ArcLeaf = LeafNode15<BoxPolicy<u64>>;
    type InlineLeaf = LeafNode15<InlinePolicy<u64>>;

    // Shared early fields: identical offsets across both policies.
    // These precede the policy-specific `values` field.

    // Permutation at cache line 1 (offset 64) — both policies
    assert!(offset_of!(ArcLeaf, permutation) == 64);
    assert!(offset_of!(InlineLeaf, permutation) == 64);

    // ikey0 at cache line 2 (offset 128) — both policies
    assert!(offset_of!(ArcLeaf, ikey0) == 128);
    assert!(offset_of!(InlineLeaf, ikey0) == 128);

    // values base offset — both policies (after keylenx + 1 byte implicit padding)
    assert!(offset_of!(ArcLeaf, values) == 264);
    assert!(offset_of!(InlineLeaf, values) == 264);

    // First 8 ikeys fit in cache line 2 (offsets 128-191)
    const IKEY_OFFSET: usize = 128;
    const IKEY8_END: usize = IKEY_OFFSET + 8 * 8;
    assert!(IKEY8_END == 192);

    // NOTE: `suffix`, `next`, `prev`, `parent` offsets are NOT shared.
    // They differ because `P::Values` size differs (120 bytes for Arc vs 240 for Inline),
    // causing `suffix` and all trailing fields to shift.
};
