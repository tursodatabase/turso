//! Filepath: src/tree/range/iterator/mod.rs
//!
//! Range iterator implementation for Masstree.
//!
//! Provides [`RangeIter`], an iterator over key-value pairs in lexicographic order.
//! The iterator yields [`ScanEntry`] items containing owned keys and values.
//!
//! # State Machine
//!
//! The iterator is driven by a state machine that handles Masstree's layered
//! structure (trie of B+ trees) and optimistic concurrency control.

// ============================================================================
//  Submodule declarations
// ============================================================================

mod adapters;
mod batch_forward;
mod batch_reverse;
pub(super) mod iter_flags;
mod range_bound;
mod scan_entry;


// ============================================================================
//  Public re-exports
// ============================================================================

pub use adapters::{KeysIter, ValuesIter};
pub use range_bound::RangeBound;
pub use scan_entry::ScanEntry;

// ============================================================================
//  Internal imports
// ============================================================================

use std::fmt::{self as StdFmt, Debug, Formatter};
use std::iter::FusedIterator;
use std::marker::PhantomData;

use seize::LocalGuard;

use crate::alloc_trait::TreeAllocator;
use crate::key::IKEY_SIZE;

use crate::policy::LeafPolicy;
use crate::tree::MassTreeGeneric;

#[cfg(debug_assertions)]
use super::cursor_key::CursorDebugState;

use super::cursor_key::CursorKey;
use super::forward_ctx::ForwardScanCtx;
use super::reverse_ctx::ReverseScanCtx;
use super::scan_state::{LayerContext, ScanSnapshot, ScanState, ScanStateBack, StepResult};

// ============================================================================
//  RangeIter
// ============================================================================

/// Iterator over a key range in a [`crate::MassTree`].
///
/// Yields entries in lexicographic key order. The iterator maintains internal
/// state for the scan position and handles concurrent modifications via the
/// optimistic concurrency control protocol.
///
/// Implements both [`Iterator`] and [`DoubleEndedIterator`], allowing forward
/// iteration with `next()` and reverse iteration with `next_back()` or `.rev()`.
/// # Example
///
/// ```no_run
/// # use masstree::{MassTree, RangeBound};
/// # let tree: MassTree<u64> = MassTree::new();
/// let guard = tree.guard();
/// let mut count = 0;
///
/// for entry in tree.range(
///     RangeBound::Included(b"prefix:"),
///     RangeBound::Excluded(b"prefix;"), // ';' is after ':' in ASCII
///     &guard
/// ) {
///     count += 1;
/// }
///
/// println!("Found {} entries", count);
/// ```
pub struct RangeIter<'a, 'g, P, A>
where
    P: LeafPolicy,
    A: TreeAllocator<P>,
{
    // ========================================================================
    //  Forward iteration state (bundled into ForwardScanCtx)
    // ========================================================================
    /// Memory reclamation guard.
    pub(super) guard: &'g LocalGuard<'a>,

    /// Forward scan context — owns stack, `layer_stack`, `cursor_key`, state,
    /// snapshot, `last_output`, forward flags, and debug fields.
    pub(super) fwd: ForwardScanCtx<P>,

    // ========================================================================
    //  Reverse iteration state (bundled into ReverseScanCtx)
    // ========================================================================
    /// Reverse scan context — `None` for forward-only iterators.
    ///
    /// Lazily materialized when `next_back()`/`.rev()` is called on a
    /// forward-only iterator. `None` saves ~300+ bytes of stack vs a
    /// default-initialized `ReverseScanCtx`.
    pub(super) rev: Option<ReverseScanCtx<P>>,

    // ========================================================================
    //  Shared state
    // ========================================================================
    /// Tree root pointer (needed for back initialization).
    pub(super) tree_root: *const u8,

    /// Start bound for the range (needed for back bound checking).
    pub(super) start_bound: RangeBound<'a>,

    /// End bound for the range (needed for forward bound checking).
    pub(super) end_bound: RangeBound<'a>,

    /// Marker for lifetime and type parameter covariance.
    _marker: PhantomData<&'a A>,
}

impl<P, A> Debug for RangeIter<'_, '_, P, A>
where
    P: LeafPolicy,
    A: TreeAllocator<P>,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> StdFmt::Result {
        let mut s = f.debug_struct("RangeIter");
        s.field("exhausted", &self.fwd.flags.exhausted())
            .field("initialized", &self.fwd.flags.initialized())
            .field("state", &self.fwd.state);

        if let Some(rev) = &self.rev {
            s.field("back_exhausted", &rev.flags.exhausted())
                .field("back_initialized", &rev.flags.initialized())
                .field("back_state", &rev.state);
        } else {
            s.field("reverse", &"forward-only");
        }

        s.finish_non_exhaustive()
    }
}

impl<'a, 'g, P, A> RangeIter<'a, 'g, P, A>
where
    P: LeafPolicy,
    A: TreeAllocator<P>,
{
    /// Create a forward-only range iterator (no backward state initialization).
    ///
    /// This is cheaper than [`new`](Self::new) because it skips initializing
    /// backward iteration state (~300 bytes of `CursorKey`, `BackStackElement`,
    /// `ReverseScanHelper`, etc.). Use this when you know only forward batch
    /// methods will be called (`for_each`, `for_each_intra_leaf_batch`,
    /// `for_each_values_batch`).
    ///
    /// If reverse iteration is requested later (`next_back` / `.rev()`), reverse
    /// state is lazily initialized on first use to preserve correctness.
    pub(crate) fn new_forward_only(
        tree: &'a MassTreeGeneric<P, A>,
        start: RangeBound<'a>,
        end: RangeBound<'a>,
        guard: &'g LocalGuard<'a>,
    ) -> Self {
        let (start_key, emit_equal) = start.to_start_params();
        let cursor_key = CursorKey::from_slice(start_key);
        let root = tree.load_root_ptr_generic(guard);

        let single_layer_mode = {
            let start_ok = start_key.len() <= IKEY_SIZE;
            let end_ok = match &end {
                RangeBound::Unbounded => true,

                RangeBound::Included(k) | RangeBound::Excluded(k) => k.len() <= IKEY_SIZE,
            };

            start_ok && end_ok
        };

        Self {
            guard,
            fwd: ForwardScanCtx::new(root, cursor_key, emit_equal, single_layer_mode),
            rev: None,
            tree_root: root,
            start_bound: start,
            end_bound: end,
            _marker: PhantomData,
        }
    }

    /// Create a forward-only range iterator rooted at a specific sublayer.
    ///
    /// The `cursor_key` must already be prepared for that layer (offset/len set
    /// as if descent had already occurred).
    pub(crate) fn new_forward_only_from_root(
        layer_root: *const u8,
        cursor_key: CursorKey,
        start_bound: RangeBound<'a>,
        end_bound: RangeBound<'a>,
        guard: &'g LocalGuard<'a>,
    ) -> Self {
        Self {
            guard,
            fwd: ForwardScanCtx::new(layer_root, cursor_key, true, false),
            rev: None,
            tree_root: layer_root,
            start_bound,
            end_bound,
            _marker: PhantomData,
        }
    }

    /// Create a new range iterator.
    pub(crate) fn new(
        tree: &'a MassTreeGeneric<P, A>,
        start: RangeBound<'a>,
        end: RangeBound<'a>,
        guard: &'g LocalGuard<'a>,
    ) -> Self {
        let (start_key, emit_equal) = start.to_start_params();
        let cursor_key = CursorKey::from_slice(start_key);
        let root = tree.load_root_ptr_generic(guard);

        let single_layer_mode = {
            let start_ok = start_key.len() <= IKEY_SIZE;
            let end_ok = match &end {
                RangeBound::Unbounded => true,
                RangeBound::Included(k) | RangeBound::Excluded(k) => k.len() <= IKEY_SIZE,
            };
            start_ok && end_ok
        };

        let back_emit_equal = match &end {
            RangeBound::Unbounded | RangeBound::Included(_) => true,
            RangeBound::Excluded(_) => false,
        };

        Self {
            guard,
            fwd: ForwardScanCtx::new(root, cursor_key, emit_equal, single_layer_mode),
            rev: Some(ReverseScanCtx::new_with_bound(
                root,
                CursorKey::for_reverse_scan(&end),
                back_emit_equal,
            )),
            tree_root: root,
            start_bound: start,
            end_bound: end,
            _marker: PhantomData,
        }
    }

    // ========================================================================
    //  Helper methods for Option<ReverseScanCtx> access
    // ========================================================================

    /// Returns `true` if the back iterator is exhausted.
    #[inline(always)]
    const fn back_exhausted(&self) -> bool {
        match &self.rev {
            Some(rev) => rev.flags.exhausted(),
            None => false,
        }
    }

    /// Returns `true` if the back iterator has been initialized.
    #[inline(always)]
    const fn back_initialized(&self) -> bool {
        match &self.rev {
            Some(rev) => rev.flags.initialized(),
            None => false,
        }
    }

    /// Mark the back iterator as exhausted (if present).
    #[inline(always)]
    const fn mark_back_exhausted(&mut self) {
        if let Some(rev) = &mut self.rev {
            rev.flags.mark_exhausted();
        }
    }

    /// Disable single-layer mode (writes `fwd.flags` only — canonical source).
    #[inline(always)]
    const fn disable_single_layer_mode(&mut self) {
        self.fwd.flags.disable_single_layer_mode();
    }

    /// Initialize the iterator (lazy initialization on first `next()` call).
    pub(super) fn initialize(&mut self) {
        if self.fwd.flags.initialized() {
            return;
        }
        self.fwd.flags.mark_initialized();

        // Handle empty tree
        if self.fwd.stack.root().is_null() {
            self.fwd.flags.mark_exhausted();
            return;
        }

        // Run initial descent loop
        loop {
            let parent_root: *const u8 = self.fwd.stack.root();

            let (state, snapshot) = self.fwd.find_initial(
                self.fwd.stack.root(),
                self.fwd.flags.emit_equal(),
                self.guard,
            );

            match state {
                ScanState::Down => {
                    // Entering a sublayer invalidates single-layer assumptions.
                    self.disable_single_layer_mode();

                    self.fwd
                        .layer_stack
                        .push(LayerContext::new(parent_root, self.fwd.stack.leaf_ptr()));

                    if self.fwd.cursor_key.has_suffix() {
                        self.fwd.cursor_key.shift();
                    } else {
                        self.fwd.cursor_key.shift_clear();
                    }
                }

                ScanState::Retry => {
                    // Version conflict, retry from current root
                }

                _ => {
                    // Ready to iterate (Emit, FindNext, Up)
                    self.fwd.state = state;
                    self.fwd.snapshot = snapshot;
                    break;
                }
            }
        }
    }

    /// Advance the iterator state machine.
    #[inline]
    #[expect(
        clippy::too_many_lines,
        reason = "State machine with fused optimization and debug instrumentation"
    )]
    fn advance(&mut self) -> Option<ScanEntry<P::Output>> {
        loop {
            match self.fwd.state {
                ScanState::Emit => {
                    // SAFETY: CursorKey invariant guarantees offset + len <= MAX_KEY_LENGTH
                    let key = unsafe { self.fwd.cursor_key.full_key_unchecked() };

                    if !self.end_bound.contains(key) {
                        self.fwd.flags.mark_exhausted();
                        return None;
                    }

                    // Check meeting condition: front caught up to back
                    if self.back_initialized() && !self.back_exhausted() {
                        // SAFETY: rev is Some when back_initialized() is true
                        let back_key = unsafe {
                            self.rev
                                .as_ref()
                                .unwrap_unchecked()
                                .cursor_key
                                .full_key_unchecked()
                        };

                        if key >= back_key {
                            self.fwd.flags.mark_exhausted();
                            self.mark_back_exhausted();
                            return None;
                        }
                    }

                    #[cfg(debug_assertions)]
                    #[allow(
                        clippy::panic,
                        reason = "Intentional panic for debug-only ordering violation detection"
                    )]
                    {
                        if let Some(ref last_key) = self.fwd.debug_last_emitted_key
                            && key <= last_key.as_slice()
                        {
                            let current_state = self.fwd.cursor_key.debug_state();
                            let last_state = self.fwd.debug_last_cursor_state.as_ref();

                            eprintln!("\n=== ORDERING VIOLATION DETECTED ===");
                            eprintln!("Current key:  {:?}", String::from_utf8_lossy(key));
                            eprintln!("Last key:     {:?}", String::from_utf8_lossy(last_key));
                            eprintln!("Current key bytes: {key:?}");
                            eprintln!("Last key bytes:    {last_key:?}");
                            eprintln!("Current cursor: {current_state}");
                            if let Some(last) = last_state {
                                eprintln!("Last cursor:    {last}");
                            }
                            eprintln!("=== END ORDERING VIOLATION ===\n");

                            panic!(
                                "Scan ordering violation: emitted key {:?} is not > last emitted key {:?}",
                                String::from_utf8_lossy(key),
                                String::from_utf8_lossy(last_key)
                            );
                        }

                        self.fwd.debug_last_emitted_key = Some(key.to_vec());
                        self.fwd.debug_last_cursor_state = Some(self.fwd.cursor_key.debug_state());
                    }

                    debug_assert!(
                        self.fwd.snapshot.is_some(),
                        "Emit state entered without snapshot - state machine bug"
                    );

                    let snapshot = self.fwd.snapshot.take()?;
                    let entry = ScanEntry::new(key.to_vec(), snapshot.value);
                    self.fwd.state = ScanState::FindNext;

                    return Some(entry);
                }

                ScanState::FindNext => {
                    let (new_state, snapshot) = if self.fwd.flags.needs_duplicate_check() {
                        self.fwd.flags.clear_duplicate_check();
                        self.fwd.find_next_with_dup_check(self.guard)
                    } else {
                        self.fwd.find_next(self.guard)
                    };

                    if new_state == ScanState::Emit {
                        let key: &[u8] = unsafe { self.fwd.cursor_key.full_key_unchecked() };

                        if !self.end_bound.contains(key) {
                            self.fwd.flags.mark_exhausted();
                            return None;
                        }

                        if self.back_initialized() && !self.back_exhausted() {
                            // SAFETY: rev is Some when back_initialized() is true
                            let back_key: &[u8] = unsafe {
                                self.rev
                                    .as_ref()
                                    .unwrap_unchecked()
                                    .cursor_key
                                    .full_key_unchecked()
                            };

                            if key >= back_key {
                                self.fwd.flags.mark_exhausted();
                                self.mark_back_exhausted();
                                return None;
                            }
                        }

                        #[cfg(debug_assertions)]
                        #[allow(
                            clippy::panic,
                            reason = "Intentional panic for debug-only ordering violation detection"
                        )]
                        {
                            if let Some(ref last_key) = self.fwd.debug_last_emitted_key
                                && key <= last_key.as_slice()
                            {
                                let current_state: CursorDebugState =
                                    self.fwd.cursor_key.debug_state();
                                let last_state: Option<&CursorDebugState> =
                                    self.fwd.debug_last_cursor_state.as_ref();

                                eprintln!("\n=== ORDERING VIOLATION DETECTED (FUSED) ===");
                                eprintln!("Current key:  {:?}", String::from_utf8_lossy(key));
                                eprintln!("Last key:     {:?}", String::from_utf8_lossy(last_key));
                                eprintln!("Current key bytes: {key:?}");
                                eprintln!("Last key bytes:    {last_key:?}");
                                eprintln!("Current cursor: {current_state}");
                                if let Some(last) = last_state {
                                    eprintln!("Last cursor:    {last}");
                                }
                                eprintln!("=== END ORDERING VIOLATION ===\n");

                                panic!(
                                    "Scan ordering violation: emitted key {:?} is not > last emitted key {:?}",
                                    String::from_utf8_lossy(key),
                                    String::from_utf8_lossy(last_key)
                                );
                            }

                            self.fwd.debug_last_emitted_key = Some(key.to_vec());
                            self.fwd.debug_last_cursor_state =
                                Some(self.fwd.cursor_key.debug_state());
                        }

                        let snapshot: ScanSnapshot<P> = snapshot?;
                        return Some(ScanEntry::new(key.to_vec(), snapshot.value));
                    }

                    self.fwd.state = new_state;
                    self.fwd.snapshot = snapshot;
                }

                ScanState::Down | ScanState::Up | ScanState::Retry => {
                    if self.fwd.step_transitions(self.guard) == StepResult::Exhausted {
                        return None;
                    }
                }
            }
        }
    }

    // ========================================================================
    //  Reverse Iteration (DoubleEndedIterator support)
    // ========================================================================

    /// Initialize the back cursor for reverse iteration.
    ///
    /// Called lazily on the first `next_back()` call.
    pub(super) fn initialize_back(&mut self) {
        if self.back_initialized() {
            return;
        }

        // Forward-only constructor path: lazily materialize reverse state
        // when callers request `next_back`/`.rev()`.
        if self.rev.is_none() {
            let back_emit_equal = matches!(
                self.end_bound,
                RangeBound::Unbounded | RangeBound::Included(_)
            );

            self.rev = Some(ReverseScanCtx::new_with_bound(
                self.tree_root,
                CursorKey::for_reverse_scan(&self.end_bound),
                back_emit_equal,
            ));
        }

        // SAFETY: rev is always Some after the block above
        let rev = unsafe { self.rev.as_mut().unwrap_unchecked() };
        let back_emit_equal = rev.flags.emit_equal();

        rev.flags.mark_initialized();

        // Handle empty tree
        if self.tree_root.is_null() {
            rev.flags.mark_exhausted();
            return;
        }

        // For unbounded end, set upper_bound so lower_reverse returns last slot
        if self.end_bound.is_unbounded() {
            rev.flags.set_upper_bound();
        }

        // Run initial reverse descent loop
        loop {
            let (state, snapshot) =
                rev.find_initial_reverse(rev.stack.get_root(), back_emit_equal, self.guard);

            match state {
                ScanStateBack::Down => {
                    // Entering a sublayer invalidates single-layer assumptions.
                    self.fwd.flags.disable_single_layer_mode();

                    // Descend into sublayer
                    rev.handle_down_back();

                    // Continue loop to call find_initial_reverse with new layer root
                }

                ScanStateBack::Retry => {
                    // Version conflict, retry
                }

                _ => {
                    if !rev.layer_stack.is_empty() {
                        self.fwd.flags.disable_single_layer_mode();
                    }

                    // Ready to iterate (Emit, FindPrev, Up)
                    rev.state = state;
                    rev.snapshot = snapshot;

                    // Clear upper_bound after first successful positioning
                    if rev.state.is_emit() {
                        rev.flags.clear_upper_bound();
                    }

                    break;
                }
            }
        }
    }

    /// Advance the back iterator state machine.
    #[inline]
    #[expect(
        clippy::too_many_lines,
        reason = "State machine benefits from unified logic"
    )]
    fn advance_back(&mut self) -> Option<ScanEntry<P::Output>> {
        // SAFETY: advance_back is only called after initialize_back, which
        // guarantees self.rev is Some.
        let rev = unsafe { self.rev.as_mut().unwrap_unchecked() };

        loop {
            match rev.state {
                ScanStateBack::Emit => {
                    // Check start bound (reverse of end bound check)
                    // SAFETY: CursorKey invariant guarantees offset + len <= MAX_KEY_LENGTH
                    let key: &[u8] = unsafe { rev.cursor_key.full_key_unchecked() };

                    if !self.start_bound.contains_reverse(key) {
                        rev.flags.mark_exhausted();
                        return None;
                    }

                    // Check meeting condition: back caught up to front
                    if self.fwd.flags.initialized() && !self.fwd.flags.exhausted() {
                        // SAFETY: CursorKey invariant guarantees offset + len <= MAX_KEY_LENGTH
                        let front_key: &[u8] = unsafe { self.fwd.cursor_key.full_key_unchecked() };

                        if key <= front_key {
                            rev.flags.mark_exhausted();
                            self.fwd.flags.mark_exhausted();

                            return None;
                        }
                    }

                    // Take snapshot
                    let snapshot: ScanSnapshot<P> = rev.snapshot.take()?;

                    // Build entry
                    let entry = ScanEntry::new(key.to_vec(), snapshot.value);

                    // CRITICAL: Clear upper_bound on every emission
                    rev.flags.clear_upper_bound();

                    // Transition to FindPrev
                    rev.state = ScanStateBack::FindPrev;

                    return Some(entry);
                }

                ScanStateBack::FindPrev => {
                    if self.fwd.flags.single_layer_mode() {
                        let needs_dup_check = rev.flags.needs_duplicate_check();
                        if needs_dup_check {
                            rev.flags.clear_duplicate_check();
                        }

                        let (new_state, snapshot) =
                            rev.find_prev_single_layer(self.guard, needs_dup_check);

                        rev.state = new_state;
                        rev.snapshot = snapshot;

                        match new_state {
                            ScanStateBack::FindPrev => {
                                if rev.stack.get_leaf_ptr().is_null() {
                                    rev.flags.mark_exhausted();
                                    return None;
                                }
                            }
                            ScanStateBack::Down => {
                                self.fwd.flags.disable_single_layer_mode();
                                rev.state = ScanStateBack::FindPrev;
                            }
                            _ => {}
                        }

                        continue;
                    }

                    let (new_state, snapshot) = if rev.flags.needs_duplicate_check() {
                        rev.flags.clear_duplicate_check();
                        rev.find_prev_with_dup_check(self.guard)
                    } else {
                        rev.find_prev(self.guard)
                    };

                    rev.state = new_state;
                    rev.snapshot = snapshot;
                }

                ScanStateBack::Down => {
                    // Disable single-layer mode when descending into sublayer
                    self.fwd.flags.disable_single_layer_mode();

                    // Handle sublayer descent
                    rev.handle_down_back();

                    // Call find_initial_reverse for the sublayer
                    let (state, snapshot) = rev.find_initial_reverse(
                        rev.stack.get_root(),
                        false, // emit_equal: false for scan-discovered descent
                        self.guard,
                    );

                    rev.state = state;
                    rev.snapshot = snapshot;

                    // After layer descent, we need duplicate check
                    rev.flags.require_duplicate_check();
                }

                ScanStateBack::Up => {
                    if !rev.handle_up_back(self.guard) {
                        // No parent layer, scan complete
                        rev.flags.mark_exhausted();

                        return None;
                    }

                    rev.state = ScanStateBack::FindPrev;

                    // After layer ascent, we need duplicate check
                    rev.flags.require_duplicate_check();
                }

                ScanStateBack::Retry => {
                    let (new_state, _) = rev.reposition_back(self.guard);

                    rev.state = new_state;

                    // After retry, we need duplicate check on next FindPrev
                    rev.flags.require_duplicate_check();
                }
            }
        }
    }

    /// Convert to a keys-only iterator.
    pub const fn keys(self) -> KeysIter<'a, 'g, P, A> {
        KeysIter { inner: self }
    }

    /// Convert to a values-only iterator.
    pub const fn values(self) -> ValuesIter<'a, 'g, P, A> {
        ValuesIter { inner: self }
    }
}

// ============================================================================
//  Iterator Trait Implementations
// ============================================================================

impl<P, A> Iterator for RangeIter<'_, '_, P, A>
where
    P: LeafPolicy,
    A: TreeAllocator<P>,
{
    type Item = ScanEntry<P::Output>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.fwd.flags.exhausted() {
            return None;
        }

        // Lazy initialization
        if !self.fwd.flags.initialized() {
            self.initialize();

            if self.fwd.flags.exhausted() {
                return None;
            }
        }

        // Meeting detection: if back is initialized, check if we've crossed
        if self.back_initialized() && !self.back_exhausted() {
            // SAFETY: CursorKey invariant guarantees offset + len <= MAX_KEY_LENGTH
            let front_key: &[u8] = unsafe { self.fwd.cursor_key.full_key_unchecked() };
            // SAFETY: rev is Some when back_initialized() is true
            let back_key: &[u8] = unsafe {
                self.rev
                    .as_ref()
                    .unwrap_unchecked()
                    .cursor_key
                    .full_key_unchecked()
            };

            if front_key >= back_key {
                // Mark both as exhausted when they meet
                self.fwd.flags.mark_exhausted();
                self.mark_back_exhausted();
                return None;
            }
        }

        self.advance()
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        if self.fwd.flags.exhausted() {
            (0, Some(0))
        } else {
            // We can't know the exact count without iterating
            (0, None)
        }
    }
}

impl<P, A> DoubleEndedIterator for RangeIter<'_, '_, P, A>
where
    P: LeafPolicy,
    A: TreeAllocator<P>,
{
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        // Check if back cursor is exhausted
        if self.back_exhausted() {
            return None;
        }

        // Lazy initialization of back cursor
        if !self.back_initialized() {
            self.initialize_back();

            if self.back_exhausted() {
                return None;
            }
        }

        // Check meeting condition: if front has advanced past where back would be
        if self.fwd.flags.initialized() && !self.fwd.flags.exhausted() {
            // SAFETY: CursorKey invariant guarantees offset + len <= MAX_KEY_LENGTH
            let front_key: &[u8] = unsafe { self.fwd.cursor_key.full_key_unchecked() };

            // SAFETY: rev is Some after initialize_back
            let back_key: &[u8] = unsafe {
                self.rev
                    .as_ref()
                    .unwrap_unchecked()
                    .cursor_key
                    .full_key_unchecked()
            };

            if back_key <= front_key {
                // Mark both as exhausted when they meet
                self.mark_back_exhausted();
                self.fwd.flags.mark_exhausted();

                return None;
            }
        }

        self.advance_back()
    }
}

impl<P, A> FusedIterator for RangeIter<'_, '_, P, A>
where
    P: LeafPolicy,
    A: TreeAllocator<P>,
{
}

// No manual Drop needed — `last_output: Option<P::Output>` drops automatically.
