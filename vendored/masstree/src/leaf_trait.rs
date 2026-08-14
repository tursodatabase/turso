//! Traits for abstracting over leaf node WIDTH variants.
//!
//! This module defines [`TreePermutation`] and [`TreeLeafNode`] traits that
//! enable generic tree operations.

use std::cmp::Ordering;
use std::fmt::Debug;

use crate::nodeversion::NodeVersion;
use crate::policy::{LeafPolicy, RetireHandle, SlotKind, SlotState};
use seize::LocalGuard;

// ============================================================================
//  Split Types
// ============================================================================

/// Split point for leaf node splitting.
#[derive(Debug, Clone, Copy)]
pub struct SplitPoint {
    /// Logical position where to split (in post-insert coordinates).
    pub pos: usize,

    /// The ikey that will be the first key of the new (right) leaf.
    pub split_ikey: u64,
}

/// Which leaf to insert into after a split.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertTarget {
    /// Insert into the original (left) leaf.
    Left,

    /// Insert into the new (right) leaf.
    Right,
}

// ============================================================================
// Split+Insert Types (Atomic Split+Insert Operation)
// ============================================================================

/// Data for inserting a key during a split operation.
#[derive(Debug)]
pub struct SplitInsertData<'a, P: LeafPolicy> {
    /// The 8-byte key to insert.
    pub ikey: u64,

    /// The keylenx value (0-8 for inline, 64 for suffix, >=128 for layer).
    pub keylenx: u8,

    /// The suffix bytes, if any (present when `keylenx == KSUF_KEYLENX`).
    pub suffix: Option<&'a [u8]>,

    /// The typed value to insert.
    pub value: P::Output,
}

/// Result of an atomic split+insert operation.
#[derive(Debug, Clone, Copy)]
pub struct SplitInsertResult {
    /// The key that separates the left and right leaves.
    pub split_ikey: u64,

    /// Which leaf received the new key.
    pub insert_target: InsertTarget,
}

// ============================================================================
//  TreePermutation Trait
// ============================================================================

/// Trait for permutation types used in leaf nodes.
pub trait TreePermutation: Copy + Clone + Eq + Debug + Send + Sync + Sized + 'static {
    /// Raw storage type for atomic operations.
    type Raw: Copy + Clone + Eq + Debug + Send + Sync + 'static;

    /// Number of slots this permutation supports.
    const WIDTH: usize;

    // ========================================================================
    //  Construction
    // ========================================================================

    /// Create an empty permutation with size = 0.
    fn empty() -> Self;

    /// Create a sorted permutation with `n` elements in slots `0..n`.
    fn make_sorted(n: usize) -> Self;

    /// Create a permutation from a raw storage value.
    fn from_value(raw: Self::Raw) -> Self;

    // ========================================================================
    //  Accessors
    // ========================================================================

    /// Get the raw storage value.
    fn value(&self) -> Self::Raw;

    /// Get the number of slots in use.
    fn size(&self) -> usize;

    /// Get the physical slot at logical position `i`.
    fn get(&self, i: usize) -> usize;

    /// Get the slot at the back (next free slot to allocate).
    fn back(&self) -> usize;

    /// Get the slot at `back()` with an offset into the free region.
    fn back_at_offset(&self, offset: usize) -> usize;

    // ========================================================================
    //  Mutation
    // ========================================================================

    /// Allocate a slot from back and insert at position `i`.
    fn insert_from_back(&mut self, i: usize) -> usize;

    /// Compute insert result without mutation (for CAS operations).
    fn insert_from_back_immutable(&self, i: usize) -> (Self, usize);

    /// Swap two slots in the free region (positions >= size).
    fn swap_free_slots(&mut self, pos_i: usize, pos_j: usize);

    /// Set the size without changing slot positions.
    fn set_size(&mut self, n: usize);

    /// Remove the slot at logical position `i`.
    fn remove(&mut self, i: usize);
}

// ============================================================================
//  TreeInternode Trait
// ============================================================================

/// Trait for internode types used in a `MassTree`.
pub trait TreeInternode: Sized + Send + Sync + 'static {
    /// Node width (max number of children).
    const WIDTH: usize;

    // ========================================================================
    //  Version / Locking
    // ========================================================================

    /// Get reference to node version.
    fn version(&self) -> &NodeVersion;

    // ========================================================================
    //  Structure
    // ========================================================================

    /// Get the height of this internode.
    fn height(&self) -> u32;

    /// Check if children are leaves (height == 0).
    fn children_are_leaves(&self) -> bool;

    /// Get number of keys.
    fn nkeys(&self) -> usize;

    /// Get number of keys using Relaxed ordering.
    fn nkeys_relaxed(&self) -> usize;

    /// Set number of keys.
    fn set_nkeys(&self, n: u8);

    /// Increment nkeys by 1.
    fn inc_nkeys(&self);

    /// Check if this internode is full.
    fn is_full(&self) -> bool;

    // ========================================================================
    //  Keys
    // ========================================================================

    /// Get key at index (Acquire ordering).
    fn ikey(&self, idx: usize) -> u64;

    /// Get key at index using Relaxed ordering.
    fn ikey_relaxed(&self, idx: usize) -> u64;

    /// Get raw pointer to the ikey array for SIMD operations.
    fn ikey_ptr(&self) -> *const u64;

    /// Set key at index.
    fn set_ikey(&self, idx: usize, key: u64);

    /// Compare key at position with search key.
    fn compare_key(&self, search_ikey: u64, p: usize) -> Ordering;

    /// Find insert position for a key.
    fn find_insert_position(&self, insert_ikey: u64) -> usize;

    // ========================================================================
    // Children
    // ========================================================================

    /// Get child pointer at index.
    fn child(&self, idx: usize) -> *mut u8;

    /// Set child pointer at index.
    fn set_child(&self, idx: usize, child: *mut u8);

    /// Assign key and right child at position.
    fn assign(&self, p: usize, ikey: u64, right_child: *mut u8);

    /// Insert key and child at position, shifting existing entries.
    fn insert_key_and_child(&self, p: usize, new_ikey: u64, new_child: *mut u8);

    // ========================================================================
    // Navigation
    // ========================================================================

    /// Get parent pointer.
    fn parent(&self) -> *mut u8;

    /// Set parent pointer.
    fn set_parent(&self, parent: *mut u8);

    /// Check if this is a root node.
    fn is_root(&self) -> bool;

    // ========================================================================
    //  Split Support
    // ========================================================================

    /// Shift entries from another internode.
    fn shift_from(&self, dst_pos: usize, src: &Self, src_pos: usize, count: usize);

    /// Split this internode into a new sibling while inserting a key/child.
    #[must_use = "popup_key must be inserted into parent node to complete the split"]
    fn split_into(
        &self,
        new_right: &mut Self,
        new_right_ptr: *mut Self,
        insert_pos: usize,
        insert_ikey: u64,
        insert_child: *mut u8,
    ) -> (u64, bool);
}

// ============================================================================
//  TreeLeafNode Trait
// ============================================================================
/// Trait for abstracting over leaf node WIDTH variants.
pub trait TreeLeafNode<P: LeafPolicy>: Sized + Send + Sync + 'static {
    /// The permutation type for this leaf.
    type Perm: TreePermutation;

    /// The internode type for this tree variant.
    type Internode: TreeInternode;

    /// Node width (number of slots).
    const WIDTH: usize;

    /// Split threshold (trigger split when size >= this).
    const SPLIT_THRESHOLD: usize;

    /// Inline suffix storage capacity in bytes.
    #[cfg(not(feature = "small-suffix-capacity"))]
    const INLINE_KSUF_CAPACITY: usize = 512;

    /// Inline suffix storage capacity (small variant, 256 bytes).
    #[cfg(feature = "small-suffix-capacity")]
    const INLINE_KSUF_CAPACITY: usize = 256;

    // ========================================================================
    //  Construction
    // ========================================================================

    /// Create a new leaf node (boxed, non-root).
    fn new_boxed() -> Box<Self>;

    /// Create a new root leaf node (boxed).
    fn new_root_boxed() -> Box<Self>;

    /// Create a new layer root leaf node (boxed).
    fn new_layer_root_boxed() -> Box<Self>;

    // ========================================================================
    //  Version / Lock
    // ========================================================================

    /// Get reference to the node's version for OCC protocol.
    fn version(&self) -> &NodeVersion;

    // ========================================================================
    //  Permutation
    // ========================================================================

    /// Load permutation with Acquire ordering.
    fn permutation(&self) -> Self::Perm;

    /// Store permutation with Release ordering.
    fn set_permutation(&self, perm: Self::Perm);

    /// Store permutation with Relaxed ordering (for split setup).
    fn set_permutation_relaxed(&self, perm: Self::Perm);

    /// Get raw permutation value for atomic operations.
    fn permutation_raw(&self) -> <Self::Perm as TreePermutation>::Raw;

    // ========================================================================
    //  Key Operations
    // ========================================================================

    /// Get ikey at slot with Acquire ordering.
    fn ikey(&self, slot: usize) -> u64;

    /// Get ikey at slot with Relaxed ordering (after Acquire fence).
    fn ikey_relaxed(&self, slot: usize) -> u64;

    /// Set ikey at slot with Release ordering.
    fn set_ikey(&self, slot: usize, ikey: u64);

    /// Set ikey at slot with Relaxed ordering (for split setup).
    fn set_ikey_relaxed(&self, slot: usize, ikey: u64);

    /// Get the ikey bound (ikey at slot 0, used for B-link routing).
    fn ikey_bound(&self) -> u64;

    /// Find all slots matching target ikey, returning a bitmask.
    fn find_ikey_matches(&self, target_ikey: u64) -> u32;

    /// Get keylenx at slot.
    fn keylenx(&self, slot: usize) -> u8;

    /// Get keylenx with Relaxed ordering (for OCC search loops).
    fn keylenx_relaxed(&self, slot: usize) -> u8;

    /// Set keylenx at slot with Release ordering.
    fn set_keylenx(&self, slot: usize, keylenx: u8);

    /// Set keylenx at slot with Relaxed ordering (for split setup).
    fn set_keylenx_relaxed(&self, slot: usize, keylenx: u8);

    /// Check if slot contains a layer pointer.
    fn is_layer(&self, slot: usize) -> bool;

    /// Check if slot has a key suffix.
    fn has_ksuf(&self, slot: usize) -> bool;

    // ========================================================================
    //  Value Operations (typed, replacing pointer API)
    // ========================================================================

    /// Load the terminal value at a slot, returning `None` if empty.
    fn load_value(&self, slot: usize) -> Option<P::Output>;

    /// Store a terminal value at a slot with Release ordering.
    fn store_value(&self, slot: usize, output: &P::Output);

    /// Store a terminal value at a slot with Relaxed ordering (for split setup).
    fn store_value_relaxed(&self, slot: usize, output: &P::Output);

    /// Update value in place, returning a handle for retiring the old value.
    fn update_value_in_place(&self, slot: usize, output: &P::Output) -> RetireHandle;

    /// Update value in place with Relaxed store ordering (for use under lock).
    fn update_value_in_place_relaxed(&self, slot: usize, output: &P::Output) -> RetireHandle;

    /// Take the terminal value from a slot, leaving it empty.
    fn take_value(&self, slot: usize) -> Option<P::Output>;

    /// Load the layer pointer at a slot.
    fn load_layer(&self, slot: usize) -> *mut u8;

    /// Store a layer pointer at a slot.
    fn store_layer(&self, slot: usize, ptr: *mut u8);

    /// Check if a slot is empty (no value or layer pointer).
    fn is_slot_empty(&self, slot: usize) -> bool;

    /// Classify a slot as empty, value, or layer, returning the value if present.
    fn classify_slot(&self, slot: usize) -> SlotKind<P::Output>;

    /// Classify a slot without extracting the value (lightweight variant).
    fn classify_slot_light(&self, slot: usize) -> SlotState;

    /// Move a value from one leaf/slot to another leaf/slot.
    fn move_value_to(&self, dst: &Self, src_slot: usize, dst_slot: usize);

    /// Clear the value at a slot (set to empty).
    fn clear_value(&self, slot: usize);

    // ========================================================================
    //  Slot Clearing
    // ========================================================================

    /// Clear all data at a slot (value, keylenx, etc.).
    fn clear_slot(&self, slot: usize);

    /// Clear a slot and remove it from the permutation.
    fn clear_slot_and_permutation(&self, slot: usize);

    // ========================================================================
    //  Size Operations (default impls)
    // ========================================================================

    /// Get the number of keys in this leaf.
    #[inline(always)]
    fn size(&self) -> usize {
        self.permutation().size()
    }

    /// Check if the leaf is empty.
    #[inline(always)]
    fn is_empty(&self) -> bool {
        self.size() == 0
    }

    /// Check if the leaf is full.
    #[inline(always)]
    fn is_full(&self) -> bool {
        self.size() >= Self::WIDTH
    }

    // ========================================================================
    //  Navigation
    // ========================================================================

    /// Get the next leaf pointer, masking the mark bit (unguarded).
    fn safe_next(&self) -> *mut Self;

    /// Check if the next pointer is marked (split in progress).
    fn next_is_marked(&self) -> bool;

    /// Set the next leaf pointer.
    fn set_next(&self, next: *mut Self);

    /// Mark the next pointer (set LSB for split coordination).
    fn mark_next(&self);

    /// Unmark the next pointer (clear LSB).
    fn unmark_next(&self);

    /// Get the previous leaf pointer (unguarded).
    fn prev(&self) -> *mut Self;

    /// Set the previous leaf pointer.
    fn set_prev(&self, prev: *mut Self);

    /// Get the parent internode pointer (unguarded).
    fn parent(&self) -> *mut u8;

    /// Set the parent internode pointer.
    fn set_parent(&self, parent: *mut u8);

    /// Unlink this leaf from the doubly-linked chain.
    ///
    /// # Safety
    ///
    /// Caller must hold the lock and ensure neighbors are valid.
    unsafe fn unlink_from_chain(&self);

    /// Get the raw next pointer including mark bit (unguarded).
    fn next_raw(&self) -> *mut Self;

    /// Spin-wait until split completes on this leaf.
    fn wait_for_split(&self);

    // ========================================================================
    //  Slot Assignment
    // ========================================================================

    /// Check if slot 0 can be reused for a new key.
    fn can_reuse_slot0(&self, new_ikey: u64) -> bool;

    // ========================================================================
    //  Split Operations
    // ========================================================================

    /// Calculate the optimal split point for this leaf.
    fn calculate_split_point(&self, insert_pos: usize, insert_ikey: u64) -> Option<SplitPoint>;

    /// Split this leaf into a preallocated sibling.
    ///
    /// # Safety
    /// Caller must hold the lock and `new_leaf_ptr` must be valid.
    unsafe fn split_into_preallocated(
        &self,
        split_pos: usize,
        new_leaf_ptr: *mut Self,
        guard: &LocalGuard<'_>,
    ) -> (u64, InsertTarget);

    /// Split all entries to the right sibling (sequential-append optimization).
    ///
    /// # Safety
    /// Caller must hold the lock and `new_leaf_ptr` must be valid.
    unsafe fn split_all_to_right_preallocated(
        &self,
        new_leaf_ptr: *mut Self,
        guard: &LocalGuard<'_>,
    ) -> (u64, InsertTarget);

    /// Atomically split this leaf and insert a new key.
    ///
    /// # Safety
    /// Caller must hold the lock, `new_leaf_ptr` must be valid.
    unsafe fn split_and_insert(
        &self,
        split_pos: usize,
        new_leaf_ptr: *mut Self,
        insert_pos: usize,
        insert_data: &SplitInsertData<'_, P>,
        guard: &LocalGuard<'_>,
    ) -> SplitInsertResult;

    /// Link a new sibling into the B-link chain.
    ///
    /// # Safety
    /// Caller must hold the lock.
    unsafe fn link_sibling(&self, new_sibling: *mut Self);

    // ========================================================================
    //  Suffix Operations
    // ========================================================================

    /// Get the suffix at a slot, if present.
    fn ksuf(&self, slot: usize) -> Option<&[u8]>;

    /// Assign a suffix to a slot, returning the old suffix bag pointer.
    ///
    /// # Safety
    /// Caller must hold the lock.
    unsafe fn assign_ksuf(&self, slot: usize, suffix: &[u8], guard: &LocalGuard<'_>) -> *mut u8;

    /// Retire a suffix bag pointer for deferred reclamation.
    ///
    /// # Safety
    /// `ptr` must be a valid suffix bag pointer.
    unsafe fn retire_suffix_bag_ptr(ptr: *mut u8, guard: &LocalGuard<'_>);

    /// Assign a suffix during initial slot setup.
    ///
    /// # Safety
    /// Caller must hold the lock.
    unsafe fn assign_ksuf_init(&self, slot: usize, suffix: &[u8], guard: &LocalGuard<'_>);

    /// Clear the suffix at a slot.
    ///
    /// # Safety
    /// Caller must hold the lock.
    unsafe fn clear_ksuf(&self, slot: usize, guard: &LocalGuard<'_>);

    // ========================================================================
    //  Suffix Comparison
    // ========================================================================

    /// Check if the suffix at a slot equals the given suffix.
    fn ksuf_equals(&self, slot: usize, suffix: &[u8]) -> bool;

    /// Compare the suffix at a slot with the given suffix.
    fn ksuf_compare(&self, slot: usize, suffix: &[u8]) -> Option<Ordering>;

    /// Get the suffix at a slot, or empty slice if none.
    fn ksuf_or_empty(&self, slot: usize) -> &[u8];

    /// Check if the suffix at a slot matches the given ikey and suffix.
    fn ksuf_matches(&self, slot: usize, ikey: u64, suffix: &[u8]) -> bool;

    /// Compare suffix, returning match result code.
    fn ksuf_match_result(&self, slot: usize, keylenx: u8, suffix: &[u8]) -> i32;

    // ========================================================================
    //  Cache Optimization
    // ========================================================================

    /// Prefetch leaf data for range scans.
    fn prefetch(&self);

    /// Prefetch the ikey at a slot.
    fn prefetch_ikey(&self, slot: usize);

    /// Prefetch for point lookup (permutation + keys).
    fn prefetch_for_search(&self);

    /// Size-aware prefetch: only fetch cache lines that will be accessed.
    fn prefetch_for_search_adaptive(&self, size: usize);

    // ========================================================================
    //  Modification State
    // ========================================================================

    /// Get the modification state.
    fn modstate(&self) -> u8;

    /// Set the modification state.
    fn set_modstate(&self, state: u8);

    /// Check if the layer has been deleted.
    fn deleted_layer(&self) -> bool;

    /// Mark the layer as deleted.
    fn mark_deleted_layer(&self);

    /// Mark the node for removal.
    fn mark_remove(&self);

    /// Check if the node is being removed.
    fn is_removing(&self) -> bool;

    /// Check if the node is in empty state.
    fn is_empty_state(&self) -> bool;

    /// Mark the node as empty.
    fn mark_empty(&self);

    /// Clear the empty state flag.
    fn clear_empty_state(&self);
}

// ============================================================================
//  Tests
// ============================================================================
