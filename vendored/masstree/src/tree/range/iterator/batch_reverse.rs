//! Reverse batch iteration methods. Each method wraps
//! [`ReverseScanCtx::run_batch_reverse`] with a different strategy type,
//! fully inlined after monomorphization.

use crate::alloc_trait::TreeAllocator;
use crate::policy::LeafPolicy;
use crate::policy::RefPolicy as RefLeafPolicy;

use crate::tree::range::reverse_ctx::{
    RevIntraLeafCopyStrategy, RevIntraLeafRefStrategy, RevValuesOnlyStrategy,
};

use super::RangeIter;

impl<P, A> RangeIter<'_, '_, P, A>
where
    P: LeafPolicy,
    A: TreeAllocator<P>,
{
    /// Reverse intra-leaf batch iteration with zero-copy references.
    ///
    /// Processes entire leaves in tight loops with single OCC validation
    /// per leaf. Falls back to state machine for layer transitions.
    #[inline]
    #[must_use = "returns the number of entries visited"]
    pub fn rev_for_each_ref<F>(mut self, mut visitor: F) -> usize
    where
        P: RefLeafPolicy,
        F: FnMut(&[u8], &P::Value) -> bool,
    {
        if self.back_exhausted() {
            return 0;
        }

        if !self.back_initialized() {
            self.initialize_back();
            if self.back_exhausted() {
                return 0;
            }
        }

        let start_bound_ikey: Option<u64> = self.start_bound.extract_ikey();

        // SAFETY: rev is Some after initialize_back
        let rev = unsafe { self.rev.as_mut().unwrap_unchecked() };
        let mut strategy = RevIntraLeafRefStrategy::new(&mut visitor);
        rev.run_batch_reverse(
            &mut strategy,
            &self.start_bound,
            start_bound_ikey,
            self.guard,
        )
    }

    /// Reverse intra-leaf batch iteration returning values by copy.
    ///
    /// Works for ALL storage types including true-inline. Same leaf-level
    /// batching as `rev_for_each_ref` but returns `P::Output` by value.
    #[inline]
    #[must_use = "returns the number of entries visited"]
    pub fn rev_for_each_intra_leaf_batch<F>(mut self, mut visitor: F) -> usize
    where
        F: FnMut(&[u8], P::Output) -> bool,
    {
        if self.back_exhausted() {
            return 0;
        }

        if !self.back_initialized() {
            self.initialize_back();
            if self.back_exhausted() {
                return 0;
            }
        }

        let start_bound_ikey: Option<u64> = self.start_bound.extract_ikey();

        // SAFETY: rev is Some after initialize_back
        let rev = unsafe { self.rev.as_mut().unwrap_unchecked() };
        let mut strategy = RevIntraLeafCopyStrategy::new(&mut visitor);
        rev.run_batch_reverse(
            &mut strategy,
            &self.start_bound,
            start_bound_ikey,
            self.guard,
        )
    }

    /// Reverse value-only batch iteration — fastest when keys aren't needed.
    ///
    /// Skips key materialization, saving up to 56 bytes per entry for long keys.
    ///
    /// Start bound uses ikey comparison only — **approximate** for suffixed keys
    /// (may over-include entries sharing the boundary ikey). Use
    /// `rev_for_each_intra_leaf_batch` when exact bounds matter.
    #[inline]
    #[must_use = "returns the number of entries visited"]
    pub fn rev_for_each_values_batch<F>(mut self, mut visitor: F) -> usize
    where
        F: FnMut(P::Output) -> bool,
    {
        if self.back_exhausted() {
            return 0;
        }

        if !self.back_initialized() {
            self.initialize_back();
            if self.back_exhausted() {
                return 0;
            }
        }

        let start_bound_ikey: Option<u64> = self.start_bound.extract_ikey();

        // SAFETY: rev is Some after initialize_back
        let rev = unsafe { self.rev.as_mut().unwrap_unchecked() };
        let mut strategy = RevValuesOnlyStrategy::new(&mut visitor);
        rev.run_batch_reverse(
            &mut strategy,
            &self.start_bound,
            start_bound_ikey,
            self.guard,
        )
    }
}
