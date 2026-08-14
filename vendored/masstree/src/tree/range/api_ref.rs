//! Reference-based range scan methods for [`crate::MassTreeGeneric`].
//!
//! These methods require [`RefLeafPolicy`] and return `&V` references.
//! Not available for `MassTree15Inline` (inline storage has no stable address).
//!
//! ```compile_fail
//! use masstree::{MassTree15Inline, RangeBound};
//!
//! let tree: MassTree15Inline<u64> = MassTree15Inline::new();
//! let guard = tree.guard();
//!
//! // Not available for true-inline storage.
//! tree.scan_ref(
//!     RangeBound::Unbounded,
//!     RangeBound::Unbounded,
//!     |_k, _v| true,
//!     &guard,
//! );
//! ```

use seize::LocalGuard;

use crate::alloc_trait::TreeAllocator;
use crate::policy::LeafPolicy;
use crate::policy::RefPolicy as RefLeafPolicy;
use crate::tree::MassTreeGeneric;

use super::iterator::RangeBound;

// ============================================================================
//  Reference-Based Scan API (requires RefLeafPolicy)
// ============================================================================

impl<P, A> MassTreeGeneric<P, A>
where
    P: LeafPolicy + RefLeafPolicy,
    A: TreeAllocator<P>,
{
    /// Scan a range with zero-copy `&V` references.
    ///
    /// Eliminates 2 atomic ops per entry vs [`Self::scan`] (no Arc increment).
    /// Value references are only valid for the callback duration.
    ///
    /// ```ignore
    /// let guard = tree.guard();
    /// let mut sum = 0u64;
    /// tree.scan_ref(
    ///     RangeBound::Unbounded, RangeBound::Unbounded,
    ///     |_key, value| { sum += *value; true },
    ///     &guard
    /// );
    /// ```
    pub fn scan_ref<F>(
        &self,
        start: RangeBound<'_>,
        end: RangeBound<'_>,
        visitor: F,
        guard: &LocalGuard<'_>,
    ) -> usize
    where
        F: FnMut(&[u8], &P::Value) -> bool,
    {
        self.range_forward(start, end, guard).for_each_ref(visitor)
    }

    /// Batch scan with zero-copy references and reduced dispatch overhead.
    ///
    /// Eliminates `match state {}` dispatch per entry vs [`scan_ref`](Self::scan_ref),
    /// inlining the common `FindNext` -> `Emit` path. Same OCC and correctness
    /// guarantees.
    ///
    /// ```ignore
    /// let guard = tree.guard();
    /// let mut sum = 0u64;
    /// tree.scan_batch_ref(
    ///     RangeBound::Unbounded, RangeBound::Unbounded,
    ///     |_key, value| { sum += *value; true },
    ///     &guard
    /// );
    /// ```
    pub fn scan_batch_ref<F>(
        &self,
        start: RangeBound<'_>,
        end: RangeBound<'_>,
        visitor: F,
        guard: &LocalGuard<'_>,
    ) -> usize
    where
        F: FnMut(&[u8], &P::Value) -> bool,
    {
        self.range_forward(start, end, guard)
            .for_each_batch_ref(visitor)
    }

    /// Intra-leaf batch scan with zero-copy references — highest performance.
    ///
    /// Processes entire leaves in tight loops with single OCC validation per
    /// leaf. Falls back to state machine for sublayer transitions.
    ///
    /// ```ignore
    /// let guard = tree.guard();
    /// let mut sum = 0u64;
    /// tree.scan_intra_leaf_batch_ref(
    ///     RangeBound::Unbounded, RangeBound::Unbounded,
    ///     |_key, value| { sum += *value; true },
    ///     &guard
    /// );
    /// ```
    pub fn scan_intra_leaf_batch_ref<F>(
        &self,
        start: RangeBound<'_>,
        end: RangeBound<'_>,
        visitor: F,
        guard: &LocalGuard<'_>,
    ) -> usize
    where
        F: FnMut(&[u8], &P::Value) -> bool,
    {
        self.range_forward(start, end, guard)
            .for_each_intra_leaf_batch_ref(visitor)
    }

    /// Reverse intra-leaf batch scan with zero-copy references.
    ///
    /// Same perf characteristics as [`scan_intra_leaf_batch_ref`](Self::scan_intra_leaf_batch_ref)
    /// but in descending key order.
    ///
    /// ```ignore
    /// let guard = tree.guard();
    /// let mut sum = 0u64;
    /// tree.scan_rev_batch_ref(
    ///     RangeBound::Unbounded, RangeBound::Unbounded,
    ///     |_key, value| { sum += *value; true },
    ///     &guard
    /// );
    /// ```
    pub fn scan_rev_batch_ref<F>(
        &self,
        start: RangeBound<'_>,
        end: RangeBound<'_>,
        visitor: F,
        guard: &LocalGuard<'_>,
    ) -> usize
    where
        F: FnMut(&[u8], &P::Value) -> bool,
    {
        self.range(start, end, guard).rev_for_each_ref(visitor)
    }
}
