//! Filepath: src/tree.rs
//!
//! This module provides the main `MassTree<V>` type and related type aliases.

use std::fmt as StdFmt;
use std::marker::PhantomData;

use std::sync::atomic::{AtomicPtr, Ordering as AtomicOrdering};

use crate::shard_counter::ShardedCounter;

use crate::alloc_trait::TreeAllocator;
use crate::alloc15::SeizeAllocator;
use crate::inline::bits::InlineBits;
use crate::leaf15::LeafNode15;
use crate::policy::{BoxPolicy, InlinePolicy, LeafPolicy};
use coalesce::CoalesceQueue;
use seize::Collector;

mod batch_utils;
mod coalesce;
mod generic;
mod range;
pub mod remove;
mod split;

pub use generic::{BatchEntry, BatchInsertResult, Entry, OccupiedEntry, VacantEntry};
pub use range::{KeysIter, RangeBound, RangeIter, ScanEntry, ValuesIter};
pub use remove::RemoveError;

/// Batch insert utilities and helpers.
///
/// # Example
///
/// ```rust
/// use masstree::MassTree;
///
/// let tree: MassTree<u64> = MassTree::new();
/// let entries = vec![
///     (b"key1".to_vec(), 1u64),
///     (b"key2".to_vec(), 2u64),
/// ];
/// let result = tree.insert_batch(entries);
/// ```
pub mod batch {
    pub use super::batch_utils::{
        BatchStats, from_iter, sequential_keys, sequential_u64_keys, zip_into_entries,
    };
}

// ============================================================================
//  InsertError
// ============================================================================

/// Errors that can occur during insert operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InsertError {
    /// Leaf node is full and cannot accept more keys.
    LeafFull,

    /// Split required (generic path).
    SplitRequired,

    /// Layer creation required (generic path).
    LayerCreationRequired,

    /// Split operation failed (generic path).
    SplitFailed,

    /// Split propagation to parent failed (generic path).
    SplitPropagationRequired,
}

impl StdFmt::Display for InsertError {
    fn fmt(&self, f: &mut StdFmt::Formatter<'_>) -> StdFmt::Result {
        match self {
            Self::LeafFull => write!(f, "leaf node is full"),

            Self::SplitRequired => {
                write!(f, "split required (leaf full)")
            }

            Self::LayerCreationRequired => {
                write!(f, "layer creation required (key conflict)")
            }

            Self::SplitFailed => {
                write!(f, "split operation failed")
            }

            Self::SplitPropagationRequired => {
                write!(f, "split propagation required (parent full)")
            }
        }
    }
}

impl std::error::Error for InsertError {}

// ============================================================================
//  MassTreeGeneric - Generic over Leaf Type
// ============================================================================

/// A high-performance generic trie of B+trees.
///
/// # Type Parameters
///
/// - `P` - Leaf policy (determines value storage: `BoxPolicy<V>` or `InlinePolicy<V>`)
/// - `A` - Allocator type (must implement [`TreeAllocator<P>`])
pub struct MassTreeGeneric<P, A>
where
    P: LeafPolicy,
    A: TreeAllocator<P>,
{
    /// Memory reclamation collector for safe concurrent access.
    collector: Collector,

    /// Node allocator for leaf and internode allocation.
    allocator: A,

    /// Atomic root pointer for concurrent access.
    root_ptr: AtomicPtr<u8>,

    /// Number of key-value pairs in the tree.
    count: ShardedCounter,

    /// Queue of empty leaves pending cleanup (lazy coalescing).
    coalesce_queue: CoalesceQueue,

    /// Marker to indicate policy type.
    _marker: PhantomData<P>,
}

// SAFETY: `MassTreeGeneric` is `Send` because:
// - `Collector` (seize): Send (epoch-based reclamation, thread-safe by design)
// - `A: TreeAllocator<P>`: bound requires Send
// - `AtomicPtr<u8>`: Send (atomic pointer, no ownership of pointee)
// - `ShardedCounter`: Send (uses AtomicUsize shards)
// - `CoalesceQueue`: Send (lock-free SegQueue, no raw pointers stored)
// - `PhantomData<P>`: Send when P: Send (P: LeafPolicy requires Send)
// All concurrent mutation is serialized by per-node locks; the tree itself
// can safely transfer ownership between threads.
unsafe impl<P, A> Send for MassTreeGeneric<P, A>
where
    P: LeafPolicy,
    A: TreeAllocator<P>,
{
}

// SAFETY: `MassTreeGeneric` is `Sync` because:
// - All read paths use optimistic concurrency control (OCC) with version validation
// - All write paths acquire per-node spin locks before mutation
// - Memory reclamation uses epoch-based protection (seize::Collector)
// - The root pointer is AtomicPtr with proper Acquire/Release ordering
// - ShardedCounter uses per-shard AtomicUsize (no shared mutable state)
// - CoalesceQueue uses lock-free SegQueue (no raw pointers stored)
// Multiple threads can safely share a &MassTreeGeneric reference.
unsafe impl<P, A> Sync for MassTreeGeneric<P, A>
where
    P: LeafPolicy,
    A: TreeAllocator<P>,
{
}

impl<P, A> StdFmt::Debug for MassTreeGeneric<P, A>
where
    P: LeafPolicy,
    A: TreeAllocator<P>,
{
    fn fmt(&self, f: &mut StdFmt::Formatter<'_>) -> StdFmt::Result {
        f.debug_struct("MassTreeGeneric")
            .field("root_ptr", &self.root_ptr.load(AtomicOrdering::Relaxed))
            .field("count", &self.count.load())
            .field("width", &LeafNode15::<P>::WIDTH)
            .field("pending_coalesce", &self.coalesce_queue.len())
            .finish_non_exhaustive()
    }
}

/// Result of searching a leaf for insert position (generic version).
#[derive(Debug)]
pub enum InsertSearchResultGeneric {
    /// Key exists at this slot.
    Found { slot: usize },

    /// Key not found, insert at logical position.
    NotFound { logical_pos: usize },

    /// Same ikey but different suffix - need to create layer.
    Conflict { slot: usize },

    /// Found layer pointer - descend into sublayer.
    Layer { slot: usize },
}

impl<P, A> Drop for MassTreeGeneric<P, A>
where
    P: LeafPolicy,
    A: TreeAllocator<P>,
{
    fn drop(&mut self) {
        self.coalesce_queue.clear();

        // SAFETY: &mut self guarantees no threads are active with guards.
        unsafe { self.collector.reclaim_all() };

        let root: *mut u8 = self.root_ptr.load(AtomicOrdering::Acquire);
        self.allocator.teardown_tree(root);
    }
}

// ============================================================================
//  Type Aliases for MassTreeGeneric
// ============================================================================

/// High-performance inline storage variant for `Copy` types (default tree type).
///
/// # Example
///
/// ```rust
/// use masstree::MassTree;
///
/// let tree: MassTree<u64> = MassTree::new();
/// let guard = tree.guard();
/// tree.insert_with_guard(b"key", 42, &guard);
/// assert_eq!(tree.get_with_guard(b"key", &guard), Some(42));
/// ```
///
/// [`InlineBits`]: crate::inline::bits::InlineBits
pub type MassTree<V> = MassTree15Inline<V>;

/// Box-based storage for non-Copy types (String, Vec<u8>, etc.).
///
/// # Example
///
/// ```rust
/// use masstree::MassTree15;
///
/// let tree: MassTree15<String> = MassTree15::new();
/// let guard = tree.guard();
/// tree.insert_with_guard(b"key", "hello".to_string(), &guard);
/// ```
pub type MassTree15<V> = MassTreeGeneric<BoxPolicy<V>, SeizeAllocator<BoxPolicy<V>>>;

/// True-inline storage for Copy types (u64, i32, *const T, etc.).
pub type MassTree15Inline<V> = MassTreeGeneric<InlinePolicy<V>, SeizeAllocator<InlinePolicy<V>>>;

// ============================================================================
//  Constructor implementations for type aliases
// ============================================================================

impl<V: Send + Sync + 'static> MassTree15<V> {
    /// Create a new empty `MassTree15`.
    #[must_use]
    #[inline(always)]
    pub fn new() -> Self {
        Self::with_allocator(SeizeAllocator::new())
    }

    /// Create a new empty [`MassTree15`] with a custom retirement batch size.
    ///
    /// Larger values reduce `sys_membarrier` syscall frequency at the cost of
    /// slightly delayed memory reclamation.
    #[must_use]
    #[inline(always)]
    pub fn with_batch_size(batch_size: usize) -> Self {
        Self::with_allocator_batch_size(SeizeAllocator::new(), Some(batch_size))
    }
}

impl<V: Send + Sync + 'static> Default for MassTree15<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V: InlineBits> MassTree15Inline<V> {
    /// Create a new empty `MassTree15Inline`.
    #[must_use]
    #[inline(always)]
    pub fn new() -> Self {
        Self::with_allocator(SeizeAllocator::new())
    }

    /// Create a new empty [`MassTree15Inline`] with a custom retirement batch size.
    ///
    /// Larger values reduce `sys_membarrier` syscall frequency at the cost of
    /// slightly delayed memory reclamation.
    #[must_use]
    #[inline(always)]
    pub fn with_batch_size(batch_size: usize) -> Self {
        Self::with_allocator_batch_size(SeizeAllocator::new(), Some(batch_size))
    }
}

impl<V: InlineBits> Default for MassTree15Inline<V> {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
//  Tests
// ============================================================================

