//! Sharded counter for high-throughput concurrent counting.
//!
//! This module provides [`ShardedCounter`], a counter optimized for concurrent
//! increment/decrement operations by distributing updates across multiple
//! cache-line-aligned shards.

use rustc_hash::FxHasher;
use static_assertions::{const_assert, const_assert_eq};
use std::cell::Cell;
use std::fmt as StdFmt;
use std::hash::{Hash, Hasher};
use std::sync::atomic::AtomicIsize;
use std::sync::atomic::Ordering as AtomicOrdering;
use std::thread::{self as StdThread, ThreadId};

/// Number of shards in the counter.
const SHARDS: usize = 16;

/// Cache line size for alignment.
const CACHE_LINE_SIZE: usize = 128;

/// A single cache-line-aligned counter shard.
#[derive(Debug)]
#[repr(C, align(128))]
struct PaddedCounter {
    /// The actual counter value.
    value: AtomicIsize,
}

impl PaddedCounter {
    /// Create a new padded counter initialized to zero.
    const fn new() -> Self {
        Self {
            value: AtomicIsize::new(0),
        }
    }
}

// Compile-time layout verification.
const_assert_eq!(std::mem::align_of::<PaddedCounter>(), CACHE_LINE_SIZE);
const_assert!(std::mem::size_of::<PaddedCounter>() >= CACHE_LINE_SIZE);

thread_local! {
    /// Thread-local cached shard index.
   static CACHED_SHARD: Cell<Option<usize>> = const { Cell::new(None) };
}

/// A sharded counter optimized for concurrent increment/decrement operations.
pub struct ShardedCounter {
    shards: [PaddedCounter; SHARDS],
}

impl ShardedCounter {
    /// Create a new sharded counter initialized to zero.
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        #[expect(
            clippy::declare_interior_mutable_const,
            reason = "Intentional: each array element gets a fresh zeroed copy"
        )]
        const EMPTY: PaddedCounter = PaddedCounter::new();

        Self {
            shards: [EMPTY; SHARDS],
        }
    }

    /// Compute the shard index for the current thread.
    #[cold]
    #[expect(clippy::cast_possible_truncation, reason = "Intentional")]
    fn compute_shard_index() -> usize {
        let thread_id: ThreadId = StdThread::current().id();
        let mut hasher: FxHasher = FxHasher::default();
        thread_id.hash(&mut hasher);

        (hasher.finish() as usize) % SHARDS
    }

    /// Get the shard index for the current thread (cached).
    #[inline(always)]
    fn shard_index() -> usize {
        CACHED_SHARD.with(|cell: &Cell<Option<usize>>| {
            cell.get().unwrap_or_else(|| {
                let index: usize = Self::compute_shard_index();
                cell.set(Some(index));

                index
            })
        })
    }

    /// Get a reference to the shard for the current thread.
    #[inline(always)]
    #[expect(clippy::indexing_slicing, reason = "INVARIANT: index < SHARDS")]
    fn get_shard(&self) -> &AtomicIsize {
        let index: usize = Self::shard_index();

        // SAFETY: `index < SHARDS` - coz modulo in `compute_shard_index`
        &self.shards[index].value
    }

    /// Increment the counter by 1.
    #[inline(always)]
    pub fn increment(&self) {
        self.get_shard().fetch_add(1, AtomicOrdering::Relaxed);
    }

    /// Decrement the counter by 1.
    ///
    /// This operation is lock-free and optimized for concurrent access.
    #[inline(always)]
    pub fn decrement(&self) {
        self.get_shard().fetch_sub(1, AtomicOrdering::Relaxed);
    }

    /// Add a provided value to the counter.
    #[allow(dead_code, reason = "Public API")]
    #[inline(always)]
    pub fn add(&self, val: isize) {
        self.get_shard().fetch_add(val, AtomicOrdering::Relaxed);
    }

    /// Load the current counter value by summing all shards.
    ///
    /// # Panics
    ///
    /// Debug builds will panic if the total is negative (indicating a bug where
    /// decrements exceeded increments). Release builds return 0 in this case.
    pub fn load(&self) -> usize {
        let mut total: isize = 0;

        for shard in &self.shards {
            total += shard.value.load(AtomicOrdering::Relaxed);
        }

        debug_assert!(
            total >= 0,
            "ShardedCounter total is negative ({total}): more decrements than increments"
        );

        if total >= 0 { total.cast_unsigned() } else { 0 }
    }

    /// Reset the counter to zero.
    ///
    /// # Thread Safety
    #[allow(dead_code, reason = "Public API")]
    #[inline]
    pub fn reset(&self) {
        for shard in &self.shards {
            shard.value.store(0, AtomicOrdering::Relaxed);
        }
    }
}

impl Default for ShardedCounter {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl StdFmt::Debug for ShardedCounter {
    fn fmt(&self, f: &mut StdFmt::Formatter<'_>) -> StdFmt::Result {
        f.debug_struct("ShardedCounter")
            .field("total", &self.load())
            .finish()
    }
}

