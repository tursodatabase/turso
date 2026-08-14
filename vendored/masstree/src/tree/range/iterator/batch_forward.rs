//! Forward batch iteration methods for maximum performance.

use std::mem::MaybeUninit;

use crate::alloc_trait::TreeAllocator;
use crate::leaf15::LeafNode15;
use crate::policy::LeafPolicy;
use crate::policy::RefPolicy as RefLeafPolicy;

use super::RangeIter;

use crate::tree::range::forward_ctx::{
    IntraLeafCopyStrategy, IntraLeafRefStrategy, ValuesOnlyStrategy,
};
use crate::tree::range::scan_state::{ScanState, StepResult};

impl<P, A> RangeIter<'_, '_, P, A>
where
    P: LeafPolicy,
    A: TreeAllocator<P>,
{
    /// Zero-allocation iteration with a visitor closure.
    #[inline]
    #[must_use = "returns the number of entries visited"]
    pub fn for_each<F>(mut self, mut visitor: F) -> usize
    where
        F: FnMut(&[u8], P::Output) -> bool,
    {
        if self.fwd.flags.exhausted() {
            return 0;
        }

        if !self.fwd.flags.initialized() {
            self.initialize();

            if self.fwd.flags.exhausted() {
                return 0;
            }
        }

        let mut count: usize = 0;

        'l: loop {
            if let Some(entry) = self.fwd.advance_no_alloc(&self.end_bound, self.guard) {
                count += 1;

                if !visitor(entry.0, entry.1) {
                    break 'l;
                }
            } else {
                break 'l;
            }
        }

        count
    }

    /// Zero-copy iteration with borrowed `&P::Value` references.
    ///
    /// Eliminates Arc increment per entry vs [`for_each`](Self::for_each).
    /// References are valid only within the callback scope.
    #[inline]
    #[must_use = "returns the number of entries visited"]
    pub fn for_each_ref<F>(mut self, mut visitor: F) -> usize
    where
        P: RefLeafPolicy,
        F: FnMut(&[u8], &P::Value) -> bool,
    {
        if self.fwd.flags.exhausted() {
            return 0;
        }

        if !self.fwd.flags.initialized() {
            self.initialize();
            if self.fwd.flags.exhausted() {
                return 0;
            }
        }

        let mut count: usize = 0;

        'l: loop {
            if let Some((key, value_ref)) =
                self.fwd.advance_no_alloc_ref(&self.end_bound, self.guard)
            {
                count += 1;
                if !visitor(key, value_ref) {
                    break 'l;
                }
            } else {
                break 'l;
            }
        }

        count
    }

    /// Batch scan with zero-copy references and reduced dispatch overhead.
    ///
    /// Inlines the `FindNext` -> `Emit` hot path, eliminating `match state {}`
    /// dispatch. Uses per-entry OCC validation and handles layer transitions
    /// by dynamically switching to multi-layer mode on `Down`.
    #[inline]
    #[must_use = "returns the number of entries visited"]
    pub fn for_each_batch_ref<F>(mut self, mut visitor: F) -> usize
    where
        P: RefLeafPolicy,
        F: FnMut(&[u8], &P::Value) -> bool,
    {
        if self.fwd.flags.exhausted() {
            return 0;
        }

        if !self.fwd.flags.initialized() {
            self.initialize();
            if self.fwd.flags.exhausted() {
                return 0;
            }
        }

        let mut count: usize = 0;

        // Handle initial Emit state from initialize() if present
        if self.fwd.state == ScanState::Emit {
            if let Some(snapshot) = self.fwd.snapshot.take() {
                // SAFETY: CursorKey invariant guarantees offset + len <= MAX_KEY_LENGTH
                let key: &[u8] = unsafe { self.fwd.cursor_key.full_key_unchecked() };

                if !self.end_bound.contains(key) {
                    self.fwd.flags.mark_exhausted();
                    return 0;
                }

                // SAFETY: Guard protects the output. output_as_ref_sound
                // uses atomic read for write-through types, avoiding
                // aliasing violation with concurrent write_through_update.
                let mut scratch: MaybeUninit<P::Value> = MaybeUninit::uninit();
                let value_ref: &P::Value =
                    unsafe { P::output_as_ref_sound(&snapshot.value, &mut scratch) };
                count += 1;
                let should_continue: bool = visitor(key, value_ref);

                if !should_continue {
                    return count;
                }
            }

            self.fwd.state = ScanState::FindNext;
        }

        // Main batch loop
        loop {
            // Handle rare states (layer transitions, retries, exhaustion)
            match self.fwd.step_transitions(self.guard) {
                StepResult::Exhausted => return count,

                StepResult::Continue => continue,

                StepResult::Ready => {}
            }

            if self.fwd.stack.is_null() {
                if self.fwd.layer_stack.is_empty() {
                    self.fwd.flags.mark_exhausted();
                    return count;
                }

                self.fwd.state = ScanState::Up;
                continue;
            }

            let leaf: &LeafNode15<P> = unsafe { self.fwd.stack.leaf_ref() };

            if leaf.version().is_deleted() {
                self.fwd.state = ScanState::Retry;
                continue;
            }

            // Hot path: FindNext -> Emit (inlined)
            let (new_state, snapshot_ptr) = if self.fwd.flags.needs_duplicate_check() {
                self.fwd.flags.clear_duplicate_check();
                self.fwd.find_next_with_dup_check_ptr(self.guard)
            } else {
                self.fwd.find_next_ptr(self.guard)
            };

            self.fwd.state = new_state;

            match new_state {
                ScanState::Emit => {
                    if let Some(snap) = snapshot_ptr {
                        // SAFETY: CursorKey invariant guarantees offset + len <= MAX_KEY_LENGTH
                        let key: &[u8] = unsafe { self.fwd.cursor_key.full_key_unchecked() };

                        if !self.end_bound.contains(key) {
                            self.fwd.flags.mark_exhausted();
                            return count;
                        }

                        count += 1;
                        self.fwd.state = ScanState::FindNext;

                        // SAFETY: Version validated, guard held, snap pointer valid.
                        let mut scratch: MaybeUninit<P::Value> = MaybeUninit::uninit();
                        let value_ref: &P::Value =
                            unsafe { snap.resolve_value_ref::<P>(&mut scratch) };
                        let should_continue: bool = visitor(key, value_ref);

                        if !should_continue {
                            return count;
                        }
                    }
                }

                ScanState::FindNext | ScanState::Down | ScanState::Up | ScanState::Retry => {}
            }
        }
    }

    /// Intra-leaf batch iteration with zero-copy references.
    ///
    /// Processes entire leaves in tight loops with single OCC validation per
    /// leaf. Falls back to state machine for sublayer transitions.
    #[inline]
    #[must_use = "returns the number of entries visited"]
    pub fn for_each_intra_leaf_batch_ref<F>(mut self, mut visitor: F) -> usize
    where
        P: RefLeafPolicy,
        F: FnMut(&[u8], &P::Value) -> bool,
    {
        if self.fwd.flags.exhausted() {
            return 0;
        }
        if !self.fwd.flags.initialized() {
            self.initialize();
            if self.fwd.flags.exhausted() {
                return 0;
            }
        }
        let end_bound_ikey: Option<u64> = self.end_bound.extract_ikey();
        self.fwd.run_batch(
            &mut IntraLeafRefStrategy::new(&mut visitor, end_bound_ikey),
            &self.end_bound,
            self.guard,
        )
    }

    /// Intra-leaf batch iteration returning values by copy.
    ///
    /// Works for ALL `LeafPolicy` types including true-inline storage.
    /// Same leaf-level batching as `for_each_intra_leaf_batch_ref` but returns
    /// `P::Output` by value. For Arc storage where zero-copy matters, prefer
    /// the `_ref` variant.
    #[inline]
    #[must_use = "returns the number of entries visited"]
    pub fn for_each_intra_leaf_batch<F>(mut self, mut visitor: F) -> usize
    where
        F: FnMut(&[u8], P::Output) -> bool,
    {
        if self.fwd.flags.exhausted() {
            return 0;
        }
        if !self.fwd.flags.initialized() {
            self.initialize();
            if self.fwd.flags.exhausted() {
                return 0;
            }
        }
        let end_bound_ikey: Option<u64> = self.end_bound.extract_ikey();
        self.fwd.run_batch(
            &mut IntraLeafCopyStrategy::new(&mut visitor, end_bound_ikey),
            &self.end_bound,
            self.guard,
        )
    }

    /// Value-only batch iteration.
    ///
    /// ```no_run
    /// use masstree::MassTree;
    /// let tree: MassTree<u64> = MassTree::new();
    /// let guard = tree.guard();
    /// let mut sum = 0u64;
    /// tree.iter(&guard).for_each_values_batch(|value| {
    ///     sum += value;
    ///     true
    /// });
    /// ```
    #[inline]
    #[must_use = "returns the number of entries visited"]
    pub fn for_each_values_batch<F>(mut self, mut visitor: F) -> usize
    where
        F: FnMut(P::Output) -> bool,
    {
        if self.fwd.flags.exhausted() {
            return 0;
        }
        if !self.fwd.flags.initialized() {
            self.initialize();

            if self.fwd.flags.exhausted() {
                return 0;
            }
        }

        self.fwd.run_batch(
            &mut ValuesOnlyStrategy::new(&mut visitor),
            &self.end_bound,
            self.guard,
        )
    }

    /// Fallible iteration with zero-copy references.
    ///
    /// ```ignore
    /// let result = tree.iter(&guard).try_for_each_ref(|key, value| {
    ///     writer.write_entry(key, value)?;
    ///     Ok(true)
    /// });
    /// ```
    ///
    /// # Errors
    ///
    /// Returns the visitor's error.
    #[inline]
    #[must_use = "returns the count or error - check the result"]
    pub fn try_for_each_ref<F, E>(mut self, mut visitor: F) -> Result<usize, E>
    where
        P: RefLeafPolicy,
        F: FnMut(&[u8], &P::Value) -> Result<bool, E>,
    {
        if self.fwd.flags.exhausted() {
            return Ok(0);
        }

        if !self.fwd.flags.initialized() {
            self.initialize();

            if self.fwd.flags.exhausted() {
                return Ok(0);
            }
        }

        let mut count: usize = 0;

        loop {
            if let Some((key, value_ref)) =
                self.fwd.advance_no_alloc_ref(&self.end_bound, self.guard)
            {
                count += 1;

                match visitor(key, value_ref) {
                    Ok(true) => {}

                    Ok(false) => return Ok(count),

                    Err(e) => return Err(e),
                }
            } else {
                return Ok(count);
            }
        }
    }
}
