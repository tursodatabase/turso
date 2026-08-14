use std::array as StdArray;
use std::sync::atomic::{Ordering as AtomicOrdering, fence};

use seize::Guard;

use crate::{
    internode::{InternodeNode, WIDTH},
    nodeversion::NodeVersion,
    ordering::{READ_ORD, RELAXED, WRITE_ORD},
    prefetch::prefetch_read,
};

impl InternodeNode {
    // ========================================================================
    //  Version Accessors
    // ========================================================================

    /// Get a reference to the node's version.
    #[must_use]
    #[inline(always)]
    pub const fn version(&self) -> &NodeVersion {
        &self.version
    }

    /// Get a mutable reference to the node's version.
    #[inline(always)]
    pub const fn version_mut(&mut self) -> &mut NodeVersion {
        &mut self.version
    }

    // ========================================================================
    //  Key Accessors
    // ========================================================================

    /// Get the number of keys in this internode.
    #[must_use]
    #[inline(always)]
    pub fn nkeys(&self) -> usize {
        self.nkeys.load(READ_ORD) as usize
    }

    /// Alias for [`Self::nkeys`] for API consistency with leaf nodes.
    #[must_use]
    #[inline(always)]
    pub fn size(&self) -> usize {
        self.nkeys()
    }

    /// Check if the internode has no keys.
    #[must_use]
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.nkeys() == 0
    }

    /// Check if the internode is full.
    #[must_use]
    #[inline(always)]
    pub fn is_full(&self) -> bool {
        self.nkeys() >= WIDTH
    }

    /// Get the number of keys using Relaxed ordering.
    #[must_use]
    #[inline(always)]
    pub(crate) fn nkeys_relaxed(&self) -> usize {
        self.nkeys.load(RELAXED) as usize
    }

    /// Get the key at the given index.
    ///
    /// # Panics
    /// Panics in debug mode if `i >= WIDTH`.
    #[must_use]
    #[inline(always)]
    #[expect(clippy::indexing_slicing, reason = "bounds checked via debug_assert")]
    pub fn ikey(&self, i: usize) -> u64 {
        debug_assert!(i < WIDTH, "ikey: index {i} out of bounds (WIDTH={WIDTH})");
        self.ikey0[i].load(READ_ORD)
    }

    /// Batch load all ikeys into a local array.
    #[must_use]
    #[inline(always)]
    #[expect(clippy::indexing_slicing, reason = "i < WIDTH guaranteed by from_fn")]
    pub fn load_all_ikeys(&self) -> [u64; WIDTH] {
        let ikeys: [u64; WIDTH] = StdArray::from_fn(|i| self.ikey0[i].load(RELAXED));

        fence(AtomicOrdering::Acquire);

        ikeys
    }

    /// Set the key at the given index.
    ///
    /// # Panics
    /// Panics in debug mode if `i >= WIDTH`.
    #[inline(always)]
    #[expect(clippy::indexing_slicing, reason = "bounds checked via debug_assert")]
    pub fn set_ikey(&self, i: usize, ikey: u64) {
        debug_assert!(i < WIDTH, "set_ikey: index {i} out of bounds");
        self.ikey0[i].store(ikey, WRITE_ORD);
    }

    /// Set key using Relaxed ordering (for internal shifting during insert).
    #[inline(always)]
    #[expect(clippy::indexing_slicing, reason = "bounds checked by caller")]
    pub(super) fn set_ikey_relaxed(&self, i: usize, ikey: u64) {
        self.ikey0[i].store(ikey, RELAXED);
    }

    /// Get the tree height.
    #[must_use]
    #[inline(always)]
    pub const fn height(&self) -> u32 {
        self.height as u32
    }

    /// Check if children are leaves (height == 0).
    #[must_use]
    #[inline(always)]
    pub const fn children_are_leaves(&self) -> bool {
        self.height == 0
    }

    // ========================================================================
    //  Child Accessors
    // ========================================================================

    /// Get the child pointer at the given index.
    ///
    /// # Panics
    /// Panics in debug mode if `i > WIDTH`.
    #[must_use]
    #[inline(always)]
    #[expect(
        clippy::indexing_slicing,
        reason = "bounds checked via debug_assert; i <= WIDTH (16 children)"
    )]
    pub fn child(&self, i: usize, guard: &impl Guard) -> *mut u8 {
        debug_assert!(i <= WIDTH, "child: index {i} out of bounds");
        guard.protect(&self.child[i], READ_ORD)
    }

    /// Get the child pointer without guard protection.
    ///
    /// # Safety
    ///
    /// Caller must ensure the child pointer's target won't be retired during use.
    /// Valid when:
    /// - Called during `Drop` (no concurrent access)
    /// - Called in teardown after `reclaim_all()`
    #[must_use]
    #[inline(always)]
    #[expect(
        clippy::indexing_slicing,
        reason = "bounds checked via debug_assert; i <= WIDTH (16 children)"
    )]
    pub unsafe fn child_unguarded(&self, i: usize) -> *mut u8 {
        debug_assert!(i <= WIDTH, "child_unguarded: index {i} out of bounds");
        self.child[i].load(READ_ORD)
    }

    /// Get the child pointer with prefetch hint for the next likely child.
    #[must_use]
    #[inline]
    #[expect(clippy::indexing_slicing, reason = "bounds checked via debug_assert")]
    pub fn child_with_prefetch(&self, i: usize, nkeys: usize, guard: &impl Guard) -> *mut u8 {
        debug_assert!(i <= WIDTH, "child_with_prefetch: index out of bounds");

        let ptr = guard.protect(&self.child[i], READ_ORD);

        if i < nkeys {
            let next_child_ptr = self.child[i + 1].load(RELAXED);

            prefetch_read(next_child_ptr);
            prefetch_read(next_child_ptr.wrapping_add(64));
        }

        ptr
    }

    /// Get the child pointer with depth-first prefetch for tree descent.
    #[must_use]
    #[inline]
    #[expect(clippy::indexing_slicing, reason = "bounds checked via debug_assert")]
    pub fn child_with_depth_prefetch(&self, i: usize, guard: &impl Guard) -> *mut u8 {
        debug_assert!(i <= WIDTH, "child_with_depth_prefetch: index out of bounds");

        // Protected load for the child we're actually returning
        let ptr: *mut u8 = guard.protect(&self.child[i], READ_ORD);

        // Prefetch the target child's header and key array
        Self::prefetch_child_internal(ptr);

        ptr
    }

    /// Get the child pointer with full prefetch for tree descent.
    #[must_use]
    #[inline]
    #[expect(clippy::indexing_slicing, reason = "bounds checked via debug_assert")]
    pub fn child_with_full_prefetch(&self, i: usize, guard: &impl Guard) -> *mut u8 {
        debug_assert!(i <= WIDTH, "child_with_full_prefetch: index out of bounds");

        let ptr: *mut u8 = guard.protect(&self.child[i], READ_ORD);

        Self::prefetch_child_full(ptr);

        ptr
    }

    /// Prefetch a child node's header and first two key cache lines.
    #[inline(always)]
    pub(super) fn prefetch_child_internal(ptr: *mut u8) {
        prefetch_read(ptr);
        prefetch_read(ptr.wrapping_add(64));
    }

    /// Prefetch a child node's header and all three key cache lines.
    #[inline(always)]
    pub(super) fn prefetch_child_full(ptr: *mut u8) {
        prefetch_read(ptr);
        prefetch_read(ptr.wrapping_add(64));
        prefetch_read(ptr.wrapping_add(128));
    }

    /// Set the child pointer at the given index.
    ///
    /// # Panics
    /// Panics in debug mode if `i > WIDTH`.
    #[inline(always)]
    #[expect(
        clippy::indexing_slicing,
        reason = "bounds checked via debug_assert; i <= WIDTH (16 children)"
    )]
    pub fn set_child(&self, i: usize, child: *mut u8) {
        debug_assert!(i <= WIDTH, "set_child: index {i} out of bounds");
        self.child[i].store(child, WRITE_ORD);
    }

    /// Assign a key and its right child at position `p`.
    ///
    /// # Panics
    /// Panics in debug mode if `p >= WIDTH`.
    #[inline(always)]
    pub fn assign(&self, p: usize, ikey: u64, right_child: *mut u8) {
        debug_assert!(p < WIDTH, "assign: position {p} out of bounds");

        self.set_ikey(p, ikey);
        self.set_child(p + 1, right_child);
    }

    /// Set the number of keys.
    ///
    /// # Panics
    /// Panics in debug mode if `n > WIDTH`.
    #[inline(always)]
    pub fn set_nkeys(&self, n: u8) {
        debug_assert!((n as usize) <= WIDTH, "set_nkeys: count {n} out of bounds");
        self.nkeys.store(n, WRITE_ORD);
    }

    /// Increment the number of keys by 1.
    ///
    /// # Panics
    /// Panics in debug mode if already at WIDTH.
    #[inline(always)]
    pub fn inc_nkeys(&self) {
        let current: u8 = self.nkeys.load(RELAXED);
        debug_assert!((current as usize) < WIDTH, "inc_nkeys: would overflow");
        self.nkeys.store(current.wrapping_add(1), WRITE_ORD);
    }
}
