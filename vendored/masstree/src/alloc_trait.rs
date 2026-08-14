//! Generic allocator trait for tree nodes.
//!
//! This module defines [`TreeAllocator`] that abstracts over allocators
//! for different leaf policies.

use seize::LocalGuard;

use crate::leaf15::LeafNode15;
use crate::nodeversion::NodeVersion;
use crate::policy::LeafPolicy;

/// Trait for allocating and deallocating tree nodes generically.
///
/// Enables tree operations to work with any leaf policy implementing [`LeafPolicy`].
///
/// # Type Parameters
///
/// - `P`: The leaf policy implementing [`LeafPolicy`]
///
/// # Internode Handling
///
/// Internode pointers use `*mut u8` for type erasure. Implementations must ensure
/// internodes have the same WIDTH as leaves (invariant enforced by construction).
pub trait TreeAllocator<P: LeafPolicy>: Send + Sync {
    // ========================================================================
    // Leaf Allocation
    // ========================================================================

    /// Allocate a leaf node directly from the pool and initialize it in-place.
    fn alloc_leaf_direct(&self, is_root: bool, is_layer_root: bool) -> *mut LeafNode15<P>;

    /// Retire a leaf node for deferred reclamation.
    ///
    /// # Safety
    ///
    /// - `ptr` must point to a valid leaf allocated by this allocator
    /// - `ptr` must be unreachable from the tree (unlinked)
    unsafe fn retire_leaf(&self, ptr: *mut LeafNode15<P>, guard: &LocalGuard<'_>);

    // ========================================================================
    // Internode Allocation
    // ========================================================================

    /// Allocate an internode directly from the pool and initialize it in-place.
    fn alloc_internode_direct(&self, height: u32) -> *mut u8;

    /// Allocate an internode as a root node directly from the pool.
    fn alloc_internode_direct_root(&self, height: u32) -> *mut u8;

    /// Allocate an internode for a split operation directly from the pool.
    fn alloc_internode_direct_for_split(
        &self,
        parent_version: &NodeVersion,
        height: u32,
    ) -> *mut u8;

    /// Retire an internode for deferred reclamation.
    ///
    /// # Safety
    ///
    /// - `ptr` must point to a valid internode
    /// - `ptr` must be unreachable from the tree
    unsafe fn retire_internode_erased(&self, ptr: *mut u8, guard: &LocalGuard<'_>);

    // ========================================================================
    // Tree Lifecycle
    // ========================================================================

    /// Teardown all reachable nodes at tree drop.
    fn teardown_tree(&self, root_ptr: *mut u8);
}

// ============================================================================
// Tests
// ============================================================================

