//! Filepath: `src/tree/range/reverse_ctx.rs`
//!
//! Reverse scan context, owns all reverse iteration state.

use std::cmp::Ordering;
use std::ptr as StdPtr;

use seize::LocalGuard;

use crate::key::IKEY_SIZE;
use crate::leaf_trait::TreeLeafNode;
use crate::leaf15::LeafNode15;
use crate::leaf15::{KSUF_KEYLENX, LAYER_KEYLENX};
use crate::nodeversion::NodeVersion;
use crate::policy::LeafPolicy;
use crate::prefetch::prefetch_read;

use super::batch_common::{
    Backward, BatchCtx, CloneEmitter, CopySlotVisitor, PtrEmitter, RefSlotVisitor, ScanEmitter,
    process_batch_keyed, process_batch_values,
};
use super::cursor_key::CursorKey;
use super::find_rev::LeafBatchResultBack;
use super::helper::ReverseScanHelper;
use super::iterator::RangeBound;
use super::iterator::iter_flags::ReverseFlags;
use super::scan_state::{
    BackStackElement, LayerContext, LayerStack, ScanSnapshot, ScanSnapshotPtr, ScanStateBack,
};
use super::traversal::reach_leaf_for_scan;

// ============================================================================
//  Internal result types (moved from find_rev.rs)
// ============================================================================

/// Result of attempting to find initial position in a single layer.
enum InitialPositionResult<P: LeafPolicy> {
    /// Found a value to emit.
    Emit(ScanSnapshot<P>),

    /// Need to descend into sublayer at the given root.
    LayerDescent(*const u8),

    /// No match at current position, retreat to find previous.
    FindPrev,

    #[expect(dead_code, reason = "Reserved for future use")]
    /// Layer is empty or exhausted, ascend.
    Up,

    /// Version conflict, retry from root.
    Retry,
}

enum EmitResult<P: LeafPolicy> {
    /// Successfully prepared snapshot for emission.
    Emit(ScanSnapshot<P>),

    /// Entry doesn't match criteria (wrong suffix, null value, etc.).
    /// Caller should continue searching.
    NoMatch,

    /// Version changed during read, need retry from root.
    VersionChanged,
}

// ============================================================================
//  ReverseScanCtx
// ============================================================================

/// Reverse scan context, owns all per-direction state for reverse iteration.
///
/// This struct bundles the scan position, layer stack, cursor key, helper,
/// state machine state, and flags that were previously individual fields on
/// `RangeIter`.
pub struct ReverseScanCtx<P: LeafPolicy> {
    /// Current scan position (leaf, version, permutation, signed ki).
    pub(crate) stack: BackStackElement<P>,

    /// Parent layer stack for sublayer navigation (backward).
    pub(crate) layer_stack: LayerStack<P>,

    /// Cursor tracking current key position (backward).
    pub(crate) cursor_key: CursorKey,

    /// Current state machine state (backward).
    pub(crate) state: ScanStateBack,

    /// Captured snapshot for current entry (backward, if in Emit state).
    pub(crate) snapshot: Option<ScanSnapshot<P>>,

    /// Packed reverse-specific boolean flags.
    pub(crate) flags: ReverseFlags,

    // ========================================================================
    //  Debug-only fields
    // ========================================================================
    /// Last emitted key for backward iteration (debug builds only).
    #[cfg(debug_assertions)]
    #[allow(dead_code)]
    pub(crate) debug_last_emitted_key_back: Option<Vec<u8>>,
}

// ============================================================================
//  Construction
// ============================================================================

impl<P: LeafPolicy> ReverseScanCtx<P> {
    /// Create a new reverse scan context positioned for a bounded reverse scan.
    ///
    /// `root` is the tree root pointer, `cursor_key` is positioned for reverse
    /// scanning, and `emit_equal` controls whether the end-bound key itself
    /// is emitted.
    #[inline]
    pub fn new_with_bound(root: *const u8, cursor_key: CursorKey, emit_equal: bool) -> Self {
        Self {
            stack: BackStackElement::new(root),
            layer_stack: LayerStack::new(),
            cursor_key,
            state: ScanStateBack::FindPrev,
            snapshot: None,
            flags: ReverseFlags::with_values(emit_equal),

            #[cfg(debug_assertions)]
            debug_last_emitted_key_back: None,
        }
    }
}

// ============================================================================
//  find_initial_reverse — Initial Positioning
// ============================================================================

impl<P: LeafPolicy> ReverseScanCtx<P> {
    /// Find the initial position for a reverse range scan.
    ///
    /// Positions the scan at the correct leaf and slot for the end bound.
    /// Uses an iterative loop instead of recursion for layer descent.
    ///
    /// # Algorithm
    ///
    /// 1. Loop through layers starting from root
    /// 2. For each layer:
    ///    - Reach target leaf via `reach_leaf_for_scan`
    ///    - Handle concurrent inserts via `stable_reverse`
    ///    - Find position via `lower_reverse`
    ///    - If layer pointer: setup descent and continue loop
    ///    - If value: try to emit
    ///    - Otherwise: return `FindPrev` to retreat
    pub fn find_initial_reverse(
        &mut self,
        root: *const u8,
        emit_equal: bool,
        guard: &LocalGuard<'_>,
    ) -> (ScanStateBack, Option<ScanSnapshot<P>>)
    where
        P::Output: Clone,
    {
        let mut current_root: *const u8 = root;

        // Iterative layer descent loop
        loop {
            self.stack.set_root(current_root);

            // Reach target leaf
            let mut leaf_ptr: *mut LeafNode15<P> =
                reach_leaf_for_scan::<P>(current_root, &self.cursor_key, guard);

            // CRITICAL: Check null BEFORE calling stable_reverse
            if leaf_ptr.is_null() {
                return (ScanStateBack::Up, None);
            }

            // Handle concurrent inserts - may follow forward chain
            let version: u32 =
                ReverseScanHelper::stable_reverse(&mut leaf_ptr, &self.cursor_key, guard);
            self.stack.set_leaf(leaf_ptr);

            // Fast path: check deleted version
            if NodeVersion::is_deleted_version(version) {
                return (ScanStateBack::Retry, None);
            }

            // SAFETY: leaf_ptr is valid (null checked above, stable_reverse ensures validity)
            let leaf: &LeafNode15<P> = unsafe { &*leaf_ptr };
            let perm: <LeafNode15<P> as TreeLeafNode<P>>::Perm = leaf.permutation();
            let size: usize = perm.size();

            // Fast path: empty leaf.
            //
            // TURSO PATCH: an empty target leaf does not mean the layer is
            // exhausted — lazily coalesced leaves stay linked, and the leaf
            // to the LEFT may still hold live entries. Returning `Up` here
            // abandoned every live key left of an emptied rightmost leaf
            // (reverse scans saw an empty layer). Route through the regular
            // retreat instead: `ki = -1` makes `find_prev` step to the
            // previous leaf, and a genuinely empty layer still ends with
            // `Up` when the retreat runs out of leaves.
            if size == 0 {
                self.stack.set_last_ikey(u64::MAX);
                self.stack.update_state(version, perm, -1);
                return (ScanStateBack::FindPrev, None);
            }

            // Try to find initial position in this layer
            match self.try_initial_position_reverse(
                leaf,
                perm,
                size,
                version,
                current_root,
                leaf_ptr,
                emit_equal,
            ) {
                InitialPositionResult::Emit(snapshot) => {
                    return (ScanStateBack::Emit, Some(snapshot));
                }

                InitialPositionResult::LayerDescent(layer_ptr) => {
                    // Continue loop with new layer root
                    current_root = layer_ptr;
                }

                InitialPositionResult::FindPrev => {
                    // Version check before returning
                    if leaf.version().has_changed(version) {
                        return (ScanStateBack::Retry, None);
                    }
                    return (ScanStateBack::FindPrev, None);
                }

                InitialPositionResult::Up => {
                    return (ScanStateBack::Up, None);
                }

                InitialPositionResult::Retry => {
                    return (ScanStateBack::Retry, None);
                }
            }
        }
    }

    /// Attempt to find initial position within a single layer.
    #[expect(clippy::too_many_arguments, reason = "Internal helper")]
    fn try_initial_position_reverse(
        &mut self,
        leaf: &LeafNode15<P>,
        perm: <LeafNode15<P> as TreeLeafNode<P>>::Perm,
        size: usize,
        version: u32,
        current_root: *const u8,
        leaf_ptr: *mut LeafNode15<P>,
        emit_equal: bool,
    ) -> InitialPositionResult<P>
    where
        P::Output: Clone,
    {
        // Find position using reverse helper
        let ki: isize = ReverseScanHelper::lower_reverse(
            &self.cursor_key,
            leaf,
            &perm,
            self.flags.upper_bound(),
        );

        // Check if position is valid: ki must be in [0, size)
        if !(ki >= 0 && ki.cast_unsigned() < size) {
            // Position is -1 or beyond size, need to retreat
            self.stack.update_state(version, perm, ki);
            return InitialPositionResult::FindPrev;
        }

        let slot: usize = perm.get(ki.cast_unsigned());
        let keylenx: u8 = leaf.keylenx_relaxed(slot);
        let slot_ikey: u64 = leaf.ikey_relaxed(slot);

        // Handle layer pointer - must descend
        if keylenx >= LAYER_KEYLENX {
            return self.handle_layer_descent_reverse(
                current_root,
                leaf_ptr,
                slot,
                slot_ikey,
                version,
                perm,
                ki,
                leaf,
            );
        }

        // Try to emit this slot
        match Self::try_emit_slot_reverse(
            leaf,
            slot,
            slot_ikey,
            keylenx,
            version,
            &perm,
            ki,
            &mut self.cursor_key,
            &mut self.stack,
            emit_equal,
            self.flags.upper_bound(),
        ) {
            EmitResult::Emit(snapshot) => InitialPositionResult::Emit(snapshot),

            EmitResult::NoMatch => {
                // Entry doesn't match, update position and retreat
                self.stack.update_state(version, perm, ki - 1);
                InitialPositionResult::FindPrev
            }

            EmitResult::VersionChanged => {
                // Version conflict - must retry from root
                InitialPositionResult::Retry
            }
        }
    }

    /// Set up layer descent for reverse scan.
    ///
    /// Pushes parent context to layer stack, updates cursor, and returns
    /// the sublayer root for the caller to continue the loop.
    ///
    /// # Critical: Initial Descent Distinction
    ///
    /// - If cursor has suffix: use `shift()` to follow user's end bound
    /// - If no suffix: use `shift_clear_reverse()` to scan entire sublayer from max
    #[expect(clippy::too_many_arguments, reason = "Internal helper")]
    fn handle_layer_descent_reverse(
        &mut self,
        current_root: *const u8,
        leaf_ptr: *mut LeafNode15<P>,
        slot: usize,
        slot_ikey: u64,
        version: u32,
        perm: <LeafNode15<P> as TreeLeafNode<P>>::Perm,
        ki: isize,
        leaf: &LeafNode15<P>,
    ) -> InitialPositionResult<P>
    where
        P::Output: Clone,
    {
        // Push parent context for return
        self.layer_stack
            .push(LayerContext::new(current_root, leaf_ptr));

        self.cursor_key.assign_store_ikey(slot_ikey);
        let layer_ptr: *mut u8 = leaf.load_layer_raw(slot);

        // Prefetch layer root before descending (hide memory latency)
        prefetch_read(layer_ptr);

        // Update position to before layer pointer for when we return
        self.stack.update_state(version, perm, ki - 1);

        // CRITICAL: Initial descent distinction (from C++ reference)
        // - If cursor has suffix: use shift() to follow user's end bound
        // - If no suffix: use shift_clear_reverse() to scan entire sublayer from max
        if self.cursor_key.has_suffix() {
            self.cursor_key.shift();
        } else {
            self.cursor_key.shift_clear_reverse();
            self.flags.set_upper_bound();
        }

        // Return layer pointer for iterative descent
        InitialPositionResult::LayerDescent(layer_ptr.cast_const())
    }

    /// Try to emit a slot value for reverse scan.
    ///
    /// Handles both suffix keys (`KSUF_KEYLENX`) and inline keys (0-8).
    #[inline]
    #[expect(clippy::too_many_arguments, reason = "Internal helper")]
    fn try_emit_slot_reverse(
        leaf: &LeafNode15<P>,
        slot: usize,
        slot_ikey: u64,
        keylenx: u8,
        version: u32,
        perm: &<LeafNode15<P> as TreeLeafNode<P>>::Perm,
        ki: isize,
        cursor_key: &mut CursorKey,
        stack: &mut BackStackElement<P>,
        emit_equal: bool,
        upper_bound: bool,
    ) -> EmitResult<P>
    where
        P::Output: Clone,
    {
        leaf.prefetch_value(slot);

        // Handle suffix keys
        if keylenx == KSUF_KEYLENX {
            return Self::try_emit_suffix_slot_reverse(
                leaf,
                slot,
                slot_ikey,
                version,
                perm,
                ki,
                cursor_key,
                stack,
                emit_equal,
                upper_bound,
            );
        }

        // Handle inline keys - only emit if emit_equal or at upper_bound
        if !emit_equal && !upper_bound {
            return EmitResult::NoMatch;
        }

        if leaf.is_value_empty_relaxed(slot) {
            return EmitResult::NoMatch;
        }

        // Version check before read
        if leaf.version().has_changed(version) {
            return EmitResult::VersionChanged;
        }

        let Some(output) = leaf.load_value(slot) else {
            // Concurrent modification removed value between is_value_empty check and load
            return EmitResult::NoMatch;
        };

        #[expect(clippy::cast_possible_truncation, reason = "Known const")]
        let key_len: usize = std::cmp::min(keylenx, IKEY_SIZE as u8) as usize;

        cursor_key.assign_store_ikey(slot_ikey);
        cursor_key.assign_store_length(key_len);

        stack.update_state(version, *perm, ki - 1);

        EmitResult::Emit(ScanSnapshot {
            value: output,
            key_len,
        })
    }

    /// Try to emit a suffix slot for reverse scan.
    #[inline]
    #[expect(clippy::too_many_arguments, reason = "Internal helper")]
    fn try_emit_suffix_slot_reverse(
        leaf: &LeafNode15<P>,
        slot: usize,
        slot_ikey: u64,
        version: u32,
        perm: &<LeafNode15<P> as TreeLeafNode<P>>::Perm,
        ki: isize,
        cursor_key: &mut CursorKey,
        stack: &mut BackStackElement<P>,
        emit_equal: bool,
        upper_bound: bool,
    ) -> EmitResult<P>
    where
        P::Output: Clone,
    {
        let Some(stored_suffix) = leaf.ksuf(slot) else {
            return EmitResult::NoMatch;
        };

        // When upper_bound is true, we're scanning from the maximum position,
        // skip suffix comparison since the 1-byte sentinel [0xFF] can't represent
        // a true maximum for multi-byte suffixes.
        if !upper_bound {
            let cmp: Ordering = stored_suffix.cmp(cursor_key.suffix());

            if !ReverseScanHelper::initial_ksuf_match_reverse(cmp, emit_equal) {
                return EmitResult::NoMatch;
            }
        }

        if leaf.is_value_empty_relaxed(slot) {
            return EmitResult::NoMatch;
        }

        // Version check before read
        if leaf.version().has_changed(version) {
            return EmitResult::VersionChanged;
        }

        let Some(output) = leaf.load_value(slot) else {
            // Concurrent modification removed value between is_value_empty check and load
            return EmitResult::NoMatch;
        };
        let key_len: usize = IKEY_SIZE + stored_suffix.len();

        cursor_key.assign_store_ikey(slot_ikey);
        cursor_key.assign_store_suffix(stored_suffix);
        cursor_key.assign_store_length(key_len);

        stack.update_state(version, *perm, ki - 1);

        EmitResult::Emit(ScanSnapshot {
            value: output,
            key_len,
        })
    }
}

// ============================================================================
//  find_prev - Main Reverse Scan Algorithm
// ============================================================================

impl<P: LeafPolicy> ReverseScanCtx<P> {
    /// Find the previous entry for reverse iteration.
    ///
    /// This is the main workhorse for reverse scanning, called repeatedly
    /// after `find_initial_reverse` positions the scan.
    #[inline]
    pub(crate) fn find_prev(
        &mut self,
        guard: &LocalGuard<'_>,
    ) -> (ScanStateBack, Option<ScanSnapshot<P>>)
    where
        P::Output: Clone,
    {
        self.find_prev_generic::<CloneEmitter>(guard, false)
    }

    /// Find the previous entry with duplicate checking enabled.
    ///
    /// Called after a Retry state to skip already-emitted entries.
    #[inline]
    pub(crate) fn find_prev_with_dup_check(
        &mut self,
        guard: &LocalGuard<'_>,
    ) -> (ScanStateBack, Option<ScanSnapshot<P>>)
    where
        P::Output: Clone,
    {
        self.find_prev_generic::<CloneEmitter>(guard, true)
    }

    /// Find the previous entry, returning a raw pointer instead of cloning.
    #[inline]
    #[allow(dead_code, reason = "Zero-copy reverse scan API")]
    pub(crate) fn find_prev_ptr(
        &mut self,
        guard: &LocalGuard<'_>,
    ) -> (ScanStateBack, Option<ScanSnapshotPtr<P::Value>>)
    where
        P: LeafPolicy,
    {
        self.find_prev_generic::<PtrEmitter>(guard, false)
    }

    /// Find the previous entry with duplicate checking, returning raw pointer.
    #[inline]
    #[allow(dead_code, reason = "Zero-copy reverse scan API")]
    pub(crate) fn find_prev_with_dup_check_ptr(
        &mut self,
        guard: &LocalGuard<'_>,
    ) -> (ScanStateBack, Option<ScanSnapshotPtr<P::Value>>)
    where
        P: LeafPolicy,
    {
        self.find_prev_generic::<PtrEmitter>(guard, true)
    }

    /// Generic inner implementation of `find_prev` with configurable emission strategy.
    #[inline]
    fn find_prev_generic<E: ScanEmitter<P>>(
        &mut self,
        guard: &LocalGuard<'_>,
        needs_duplicate_check: bool,
    ) -> (ScanStateBack, Option<E::Snapshot>) {
        // Fast path: null leaf means we need to go up
        let leaf_ptr: *mut LeafNode15<P> = self.stack.get_leaf_ptr();
        if leaf_ptr.is_null() {
            return (ScanStateBack::Up, None);
        }

        let ki: isize = self.stack.get_ki();

        // Fast path: leaf exhausted (ki went negative)
        // Check BEFORE version to avoid unnecessary atomic load
        if ki < 0 {
            return self.advance_to_prev_leaf_generic::<E>(guard);
        }

        // SAFETY: leaf_ptr is valid (null checked above)
        let leaf: &LeafNode15<P> = unsafe { &*leaf_ptr };
        let version: u32 = self.stack.get_version();

        // Version check - if changed, reposition
        if leaf.version().has_changed(version) {
            return self.reposition_back_generic::<E>(guard);
        }

        let perm: <LeafNode15<P> as TreeLeafNode<P>>::Perm = *self.stack.get_perm_ref();
        let size: usize = perm.size();

        // Defensive check: ki might be >= size due to concurrent deletion
        if ki.unsigned_abs() >= size {
            return self.advance_to_prev_leaf_generic::<E>(guard);
        }

        // Process the current slot
        self.process_slot_generic::<E>(leaf, leaf_ptr, perm, version, ki, needs_duplicate_check)
    }

    /// Process a single slot during reverse scan (generic over emission strategy).
    #[inline]
    #[expect(clippy::too_many_arguments, reason = "Internal helper")]
    fn process_slot_generic<E: ScanEmitter<P>>(
        &mut self,
        leaf: &LeafNode15<P>,
        leaf_ptr: *mut LeafNode15<P>,
        perm: <LeafNode15<P> as TreeLeafNode<P>>::Perm,
        version: u32,
        ki: isize,
        needs_duplicate_check: bool,
    ) -> (ScanStateBack, Option<E::Snapshot>) {
        let slot: usize = perm.get(ki.unsigned_abs());
        let slot_ikey: u64 = leaf.ikey_relaxed(slot);
        let keylenx: u8 = leaf.keylenx_relaxed(slot);

        // Reverse: ikeys should be non-increasing within a leaf
        if slot_ikey > self.stack.last_ikey() {
            return (ScanStateBack::Retry, None);
        }

        // Prefetch next slot's data to hide memory latency
        // For reverse scan, "next" is ki-1
        if ki > 0 {
            let next_slot: usize = perm.get((ki - 1).unsigned_abs());
            leaf.prefetch_value(next_slot);
        }

        // Check for duplicate only when needed (after Retry)
        if needs_duplicate_check
            && ReverseScanHelper::is_duplicate_reverse(
                &self.cursor_key,
                slot_ikey,
                keylenx,
                self.flags.upper_bound(),
            )
        {
            self.stack.set_ki(ReverseScanHelper::prev(ki));
            return (ScanStateBack::FindPrev, None);
        }

        // Handle layer pointer
        if keylenx >= LAYER_KEYLENX {
            return self.handle_layer_pointer_generic::<E>(
                leaf, leaf_ptr, slot, slot_ikey, version, perm, ki,
            );
        }

        // Try to emit this slot's value
        self.try_emit_generic::<E>(leaf, slot, slot_ikey, keylenx, version, &perm, ki)
    }

    /// Handle a layer pointer during reverse scan (`find_prev` path).
    #[inline]
    #[expect(clippy::too_many_arguments, reason = "Internal helper")]
    fn handle_layer_pointer_generic<E: ScanEmitter<P>>(
        &mut self,
        leaf: &LeafNode15<P>,
        leaf_ptr: *mut LeafNode15<P>,
        slot: usize,
        slot_ikey: u64,
        version: u32,
        perm: <LeafNode15<P> as TreeLeafNode<P>>::Perm,
        ki: isize,
    ) -> (ScanStateBack, Option<E::Snapshot>) {
        // Push current context to layer stack for return
        self.layer_stack
            .push(LayerContext::new(self.stack.get_root(), leaf_ptr));

        // Update cursor with layer pointer's ikey
        self.cursor_key.assign_store_ikey(slot_ikey);

        // Get layer root and prefetch
        let layer_ptr: *mut u8 = leaf.load_layer_raw(slot);
        prefetch_read(layer_ptr);

        // Update stack for sublayer (ki-1 for when we return)
        self.stack.set_root(layer_ptr.cast_const());
        self.stack
            .update_state(version, perm, ReverseScanHelper::prev(ki));

        // Return Down state - iterator will call handle_down_back then find_initial_reverse
        (ScanStateBack::Down, None)
    }

    /// Try to emit a value slot during reverse scan (`find_prev` path).
    ///
    /// Generic over emission strategy `E`. Calls `helper.mark_key_complete()`
    /// on successful emission to clear the `upper_bound` flag.
    #[inline]
    #[expect(clippy::too_many_arguments, reason = "Internal helper")]
    fn try_emit_generic<E: ScanEmitter<P>>(
        &mut self,
        leaf: &LeafNode15<P>,
        slot: usize,
        slot_ikey: u64,
        keylenx: u8,
        version: u32,
        perm: &<LeafNode15<P> as TreeLeafNode<P>>::Perm,
        ki: isize,
    ) -> (ScanStateBack, Option<E::Snapshot>) {
        leaf.prefetch_value(slot);

        // Get value pointer first (before version check to pipeline loads)
        if leaf.is_value_empty_relaxed(slot) {
            // Empty value slot - skip to previous slot
            self.stack.set_ki(ReverseScanHelper::prev(ki));
            return (ScanStateBack::FindPrev, None);
        }

        // Version check BEFORE reading value
        if leaf.version().has_changed(version) {
            return (ScanStateBack::Retry, None);
        }

        // Build key from slot data
        self.cursor_key.assign_store_ikey(slot_ikey);

        // CRITICAL: Clear upper_bound after successful emission
        self.flags.clear_upper_bound();

        // Calculate key length and handle suffix
        let key_len: usize = if keylenx == KSUF_KEYLENX {
            leaf.ksuf(slot).map_or(IKEY_SIZE, |suffix| {
                self.cursor_key.assign_store_suffix(suffix);
                IKEY_SIZE + suffix.len()
            })
        } else {
            #[expect(clippy::cast_possible_truncation, reason = "Known const")]
            let len: usize = std::cmp::min(keylenx, IKEY_SIZE as u8) as usize;
            len
        };

        self.cursor_key.assign_store_length(key_len);

        // Emit value via trait — CloneEmitter calls load_value, PtrEmitter calls load_value_raw
        let Some(snapshot) = E::emit_value(leaf, slot, key_len) else {
            // Concurrent modification removed value between is_value_empty check and load
            self.stack.set_ki(ReverseScanHelper::prev(ki));
            return (ScanStateBack::FindPrev, None);
        };

        // Update cached ikey for monotonicity check
        self.stack.set_last_ikey(slot_ikey);

        // Update position for next iteration
        self.stack
            .update_state(version, *perm, ReverseScanHelper::prev(ki));

        (ScanStateBack::Emit, Some(snapshot))
    }
}

// ============================================================================
//  advance_to_prev_leaf - O(1) Local Leaf Retreat
// ============================================================================

impl<P: LeafPolicy> ReverseScanCtx<P> {
    /// Advance to the previous leaf in the B-link chain.
    ///
    /// Called when the current leaf is exhausted (`ki < 0`).
    /// O(1) per leaf — direct pointer follow, no root traversal.
    #[inline]
    fn advance_to_prev_leaf_generic<E: ScanEmitter<P>>(
        &mut self,
        guard: &LocalGuard<'_>,
    ) -> (ScanStateBack, Option<E::Snapshot>) {
        let current_ptr: *mut LeafNode15<P> = self.stack.get_leaf_ptr();

        // SAFETY: current_ptr was validated before calling this function
        let current_leaf: &LeafNode15<P> = unsafe { &*current_ptr };

        // Step 1: Update cursor key with current leaf's ikey_bound
        let ikey_bound: u64 = current_leaf.ikey_bound();
        self.cursor_key.assign_store_ikey(ikey_bound);
        self.cursor_key.assign_store_length(0);

        // Step 2: Get previous leaf pointer (O(1))
        let prev_ptr: *mut LeafNode15<P> = ReverseScanHelper::retreat(current_leaf);

        // Step 3: Check for layer exhaustion
        if prev_ptr.is_null() {
            self.stack.set_leaf(StdPtr::null_mut());
            return (ScanStateBack::Up, None);
        }

        // Step 4: Prefetch previous leaf
        prefetch_read(prev_ptr.cast::<u8>());

        // Step 5: Set up stack with new leaf
        self.stack.set_leaf(prev_ptr);

        // Step 6: Get stable version (may follow forward chain)
        let mut leaf_ptr: *mut LeafNode15<P> = prev_ptr;
        let version: u32 =
            ReverseScanHelper::stable_reverse(&mut leaf_ptr, &self.cursor_key, guard);

        // Step 7: Update stack if stable_reverse followed forward chain
        if leaf_ptr != prev_ptr {
            self.stack.set_leaf(leaf_ptr);
        }

        // Step 8: Get permutation and set position to LAST slot
        // SAFETY: leaf_ptr is valid (stable_reverse ensures it)
        let leaf: &LeafNode15<P> = unsafe { &*leaf_ptr };

        // Step 8a: Prefetch prev-prev leaf for 2-way pipelining
        // (matches the single-layer variant and forward's next-next prefetch)
        let prev_prev: *mut LeafNode15<P> = leaf.prev(guard);
        if !prev_prev.is_null() {
            prefetch_read(prev_prev.cast::<u8>());
        }

        let perm: <LeafNode15<P> as TreeLeafNode<P>>::Perm = leaf.permutation();

        // CRITICAL FIX: When moving to previous leaf via prev_ pointer,
        // we always start from the LAST slot (size - 1), not from lower_reverse.
        let size: usize = perm.size();
        let ki: isize = if size > 0 {
            (size - 1).cast_signed()
        } else {
            -1 // Empty leaf, will trigger another advance_to_prev_leaf
        };

        // Step 9: Reset monotonicity cache for new leaf
        self.stack.set_last_ikey(u64::MAX);

        // Step 10: Update stack state
        self.stack.update_state(version, perm, ki);

        (ScanStateBack::FindPrev, None)
    }
}

// ============================================================================
//  reposition_back - Version Conflict Recovery
// ============================================================================

impl<P: LeafPolicy> ReverseScanCtx<P> {
    /// Reposition after version conflict during reverse scan.
    ///
    /// Traverses from the layer root to find the correct leaf for the
    /// current cursor key. O(height) per call.
    pub(crate) fn reposition_back(
        &mut self,
        guard: &LocalGuard<'_>,
    ) -> (ScanStateBack, Option<ScanSnapshot<P>>)
    where
        P::Output: Clone,
    {
        self.reposition_back_generic::<CloneEmitter>(guard)
    }

    /// Generic reposition after version conflict.
    fn reposition_back_generic<E: ScanEmitter<P>>(
        &mut self,
        guard: &LocalGuard<'_>,
    ) -> (ScanStateBack, Option<E::Snapshot>) {
        const MAX_REPOSITION_RETRIES: u32 = 16;

        let root: *const u8 = self.stack.get_root();

        // Handle null root
        if root.is_null() {
            return (ScanStateBack::Up, None);
        }

        // Iterative retry loop (avoids recursion)
        for _retry in 0..MAX_REPOSITION_RETRIES {
            // Traverse from root to leaf
            let mut leaf_ptr: *mut LeafNode15<P> =
                reach_leaf_for_scan::<P>(root, &self.cursor_key, guard);

            // Check for empty tree / layer
            if leaf_ptr.is_null() {
                self.stack.set_leaf(StdPtr::null_mut());
                return (ScanStateBack::Up, None);
            }

            // Handle concurrent inserts (may follow forward chain)
            let version: u32 =
                ReverseScanHelper::stable_reverse(&mut leaf_ptr, &self.cursor_key, guard);

            // Check for deleted version
            if NodeVersion::is_deleted_version(version) {
                continue;
            }

            // SAFETY: leaf_ptr is valid (null checked, not deleted)
            let leaf: &LeafNode15<P> = unsafe { &*leaf_ptr };
            let perm: <LeafNode15<P> as TreeLeafNode<P>>::Perm = leaf.permutation();
            let ki: isize = ReverseScanHelper::lower_reverse(
                &self.cursor_key,
                leaf,
                &perm,
                self.flags.upper_bound(),
            );

            // Update stack with new position
            self.stack.set_leaf(leaf_ptr);
            self.stack.set_last_ikey(u64::MAX);
            self.stack.update_state(version, perm, ki);

            return (ScanStateBack::FindPrev, None);
        }

        // Internal retries exhausted under heavy contention.
        // Fall back to outer state machine retry (re-traverse from root).
        (ScanStateBack::Retry, None)
    }
}

// ============================================================================
//  handle_down_back / handle_up_back - Layer Navigation
// ============================================================================

impl<P: LeafPolicy> ReverseScanCtx<P> {
    /// Handle descent into sublayer for reverse scan.
    ///
    /// Shifts cursor with `shift_clear_reverse()` and sets `upper_bound = true`.
    #[inline(always)]
    pub(crate) fn handle_down_back(&mut self) {
        self.cursor_key.shift_clear_reverse();
        self.flags.set_upper_bound();
        self.stack.set_last_ikey(u64::MAX);
    }

    /// Handle ascent to parent layer for reverse scan.
    ///
    /// Returns `true` if successfully restored to parent layer,
    /// `false` if layer stack empty (scan complete).
    pub(crate) fn handle_up_back(&mut self, guard: &LocalGuard<'_>) -> bool {
        // Root of the sublayer we just finished scanning.
        let completed_layer_root: *const u8 = self.stack.get_root();

        // Step 1: Pop parent context from layer stack
        let Some(parent_ctx) = self.layer_stack.pop() else {
            return false;
        };

        // Step 2: Restore stack with parent context
        self.stack.set_root(parent_ctx.root);
        self.stack.set_leaf(parent_ctx.leaf.as_ptr());

        // Step 3: Unshift cursor to parent layer
        self.cursor_key.unshift();

        // Step 4: Handle edge case - cursor became empty at root layer
        if self.cursor_key.is_at_empty_root() {
            return self.handle_up_back(guard);
        }

        // Step 5: Get stable version from parent leaf
        let mut leaf_ptr: *mut LeafNode15<P> = parent_ctx.leaf.as_ptr();
        let version: u32 =
            ReverseScanHelper::stable_reverse(&mut leaf_ptr, &self.cursor_key, guard);

        // Update leaf if stable_reverse followed forward chain
        if leaf_ptr != parent_ctx.leaf.as_ptr() {
            self.stack.set_leaf(leaf_ptr);
        }

        // Step 6: Find position in parent leaf
        // SAFETY: leaf_ptr is valid (from NonNull in LayerContext)
        let leaf: &LeafNode15<P> = unsafe { &*leaf_ptr };
        let perm: <LeafNode15<P> as TreeLeafNode<P>>::Perm = leaf.permutation();

        // CRITICAL: Clear upper_bound before position search.
        self.flags.clear_upper_bound();

        // First try to locate the exact layer-pointer slot we just returned from.
        let mut anchored_ki: Option<isize> = None;
        for pos in 0..perm.size() {
            let slot: usize = perm.get(pos);
            if leaf.keylenx_relaxed(slot) >= LAYER_KEYLENX {
                let layer_ptr: *const u8 = leaf.load_layer_raw(slot).cast_const();
                if layer_ptr == completed_layer_root {
                    anchored_ki = Some(pos.cast_signed() - 1);
                    break;
                }
            }
        }

        let ki: isize = anchored_ki.unwrap_or_else(|| {
            ReverseScanHelper::lower_reverse(
                &self.cursor_key,
                leaf,
                &perm,
                self.flags.upper_bound(),
            )
        });

        // Step 7: Reset monotonicity cache for new leaf context
        self.stack.set_last_ikey(u64::MAX);

        // Step 8: Update stack state
        self.stack.update_state(version, perm, ki);

        true
    }
}

// ============================================================================
//  Single-Layer Fast Path for Reverse Scan
// ============================================================================

impl<P: LeafPolicy> ReverseScanCtx<P> {
    /// Fast path for reverse iteration on single-layer trees (keys <= 8 bytes).
    ///
    /// Skips layer pointer handling, suffix comparisons, and layer stack operations.
    #[inline]
    #[expect(clippy::cast_possible_truncation)]
    pub(crate) fn find_prev_single_layer(
        &mut self,
        guard: &LocalGuard<'_>,
        needs_duplicate_check: bool,
    ) -> (ScanStateBack, Option<ScanSnapshot<P>>)
    where
        P::Output: Clone,
    {
        // Check for null leaf
        let leaf_ptr: *mut LeafNode15<P> = self.stack.get_leaf_ptr();
        if leaf_ptr.is_null() {
            return (ScanStateBack::FindPrev, None);
        }

        let ki: isize = self.stack.get_ki();

        // Fast path: leaf exhausted (ki went negative)
        if ki < 0 {
            return self.advance_prev_leaf_single_layer_snapshot(guard);
        }

        // SAFETY: leaf_ptr is valid (null checked above)
        let leaf: &LeafNode15<P> = unsafe { &*leaf_ptr };
        let version: u32 = self.stack.get_version();

        // Check if leaf was concurrently modified since we cached the version.
        // This matches forward's discipline in find_next_single_layer_ptr.
        if leaf.version().has_changed(version) {
            return (ScanStateBack::Retry, None);
        }

        let perm: <LeafNode15<P> as TreeLeafNode<P>>::Perm = *self.stack.get_perm_ref();
        let perm_size: usize = perm.size();

        // Defensive: ki might be >= size due to concurrent deletion
        if ki.unsigned_abs() >= perm_size {
            return self.advance_prev_leaf_single_layer_snapshot(guard);
        }

        // Get current slot
        let slot: usize = perm.get(ki.unsigned_abs());
        let slot_ikey: u64 = leaf.ikey_relaxed(slot);
        let slot_keylenx: u8 = leaf.keylenx_relaxed(slot);

        // Reverse: ikeys should be non-increasing within a leaf
        if slot_ikey > self.stack.last_ikey() {
            return (ScanStateBack::Retry, None);
        }

        // Check for duplicate only when needed (after Retry)
        if needs_duplicate_check
            && ReverseScanHelper::is_duplicate_reverse(
                &self.cursor_key,
                slot_ikey,
                slot_keylenx,
                self.flags.upper_bound(),
            )
        {
            self.stack.set_ki(ki - 1);
            return (ScanStateBack::FindPrev, None);
        }

        // Single-layer mode only handles inline keys (0..=8).
        // This returns Down for BOTH suffix keys (KSUF_KEYLENX=64) and layer pointers
        // (LAYER_KEYLENX=128). The caller (iterator.rs) handles Down by disabling
        // single-layer mode and re-processing via the multi-layer path — it does NOT
        // dereference the slot as a layer pointer, so this is safe for KSUF.
        if slot_keylenx > IKEY_SIZE as u8 {
            return (ScanStateBack::Down, None);
        }

        // Value slot - prepare for emit
        leaf.prefetch_value(slot);
        if leaf.is_value_empty_relaxed(slot) {
            self.stack.set_ki(ki - 1);
            return (ScanStateBack::FindPrev, None);
        }

        // Inline keys have keylenx 0-8 representing the actual length
        let key_len: usize = slot_keylenx as usize;
        self.cursor_key.assign_store_ikey(slot_ikey);
        self.cursor_key.assign_store_length(key_len);
        self.cursor_key.mark_key_complete();

        // Clear upper_bound after successful emit
        self.flags.clear_upper_bound();

        // Update cached ikey for monotonicity check
        self.stack.set_last_ikey(slot_ikey);

        // Advance position for next call (go backwards)
        self.stack.set_ki(ki - 1);

        let Some(output) = leaf.load_value(slot) else {
            return (ScanStateBack::FindPrev, None);
        };

        (
            ScanStateBack::Emit,
            Some(ScanSnapshot {
                value: output,
                key_len,
            }),
        )
    }

    /// Advance to previous leaf in single-layer mode (snapshot version).
    #[inline(always)]
    fn advance_prev_leaf_single_layer_snapshot(
        &mut self,
        guard: &LocalGuard<'_>,
    ) -> (ScanStateBack, Option<ScanSnapshot<P>>) {
        let leaf_ptr: *mut LeafNode15<P> = self.stack.get_leaf_ptr();

        if leaf_ptr.is_null() {
            return (ScanStateBack::FindPrev, None);
        }

        let leaf: &LeafNode15<P> = unsafe { &*leaf_ptr };
        let version: u32 = self.stack.get_version();

        // Check if version changed (concurrent modification)
        if leaf.version().has_changed(version) {
            return (ScanStateBack::Retry, None);
        }

        // Update cursor with current leaf's bound before moving
        let ikey_bound: u64 = leaf.ikey_bound();
        self.cursor_key.assign_store_ikey(ikey_bound);
        self.cursor_key.assign_store_length(0);

        // Get previous leaf
        let prev: *mut LeafNode15<P> = leaf.prev(guard);

        if prev.is_null() {
            self.stack.set_leaf(StdPtr::null_mut());
            return (ScanStateBack::FindPrev, None);
        }

        // Move to previous leaf
        self.stack.set_leaf(prev);

        // SAFETY: prev is non-null
        let prev_leaf: &LeafNode15<P> = unsafe { &*prev };
        prev_leaf.prefetch();

        // Prefetch prev-prev leaf for 2-way pipelining
        let prev_prev: *mut LeafNode15<P> = prev_leaf.prev(guard);
        if !prev_prev.is_null() {
            let prev_prev_leaf: &LeafNode15<P> = unsafe { &*prev_prev };
            prev_prev_leaf.prefetch();
        }

        // Get stable version
        let prev_version: u32 = prev_leaf.version().stable();

        if NodeVersion::is_deleted_version(prev_version) {
            return (ScanStateBack::Retry, None);
        }

        // Load permutation and start from last slot
        let perm: <LeafNode15<P> as TreeLeafNode<P>>::Perm = prev_leaf.permutation();
        let size: usize = perm.size();
        let ki: isize = if size > 0 {
            (size - 1).cast_signed()
        } else {
            -1
        };

        // Reset monotonicity cache for new leaf
        self.stack.set_last_ikey(u64::MAX);

        self.stack.update_state(prev_version, perm, ki);

        (ScanStateBack::FindPrev, None)
    }
}

// ============================================================================
//  Intra-Leaf Batch Processing for Reverse Scan
// ============================================================================

/// Build a [`BatchCtx`] from reverse scan fields (split-borrowed).
#[inline(always)]
fn build_reverse_batch_ctx<'a, P: LeafPolicy>(
    stack: &'a BackStackElement<P>,
    cursor_key: &'a mut CursorKey,
    layer_stack: &'a mut LayerStack<P>,
) -> BatchCtx<'a, P> {
    let leaf_ptr: *mut LeafNode15<P> = stack.get_leaf_ptr();
    // SAFETY: leaf_ptr is valid - protected by guard in caller
    let leaf: &LeafNode15<P> = unsafe { &*leaf_ptr };
    let perm = *stack.get_perm_ref();
    let perm_size = perm.size();
    BatchCtx {
        leaf,
        perm,
        perm_size,
        cached_version: stack.get_version(),
        ki: stack.get_ki(),
        root: stack.get_root(),
        leaf_ptr,
        cursor_key,
        layer_stack,
    }
}

impl<P: LeafPolicy> ReverseScanCtx<P> {
    /// Process remaining entries in current leaf in reverse, returning `&P::Value` references.
    #[inline]
    pub(crate) fn process_prev_leaf_batch_ptr<F>(
        &mut self,
        start_bound: &RangeBound<'_>,
        start_bound_ikey: Option<u64>,
        visitor: &mut F,
        count: &mut usize,
    ) -> LeafBatchResultBack
    where
        F: FnMut(&[u8], &P::Value) -> bool,
    {
        let (result, ki, root) = {
            let mut ctx =
                build_reverse_batch_ctx(&self.stack, &mut self.cursor_key, &mut self.layer_stack);
            let r = process_batch_keyed::<Backward, _, P>(
                &mut ctx,
                start_bound,
                start_bound_ikey,
                &mut RefSlotVisitor(visitor),
                count,
                &mut self.flags,
            );
            (r, ctx.ki, ctx.root)
        };
        self.stack.set_ki(ki);
        self.stack.set_root(root);
        result
    }

    /// Process remaining entries in current leaf in reverse, returning `P::Output` by value.
    #[inline]
    pub(crate) fn process_prev_leaf_batch<F>(
        &mut self,
        start_bound: &RangeBound<'_>,
        start_bound_ikey: Option<u64>,
        visitor: &mut F,
        count: &mut usize,
    ) -> LeafBatchResultBack
    where
        F: FnMut(&[u8], P::Output) -> bool,
    {
        let (result, ki, root) = {
            let mut ctx =
                build_reverse_batch_ctx(&self.stack, &mut self.cursor_key, &mut self.layer_stack);
            let r = process_batch_keyed::<Backward, _, P>(
                &mut ctx,
                start_bound,
                start_bound_ikey,
                &mut CopySlotVisitor(visitor),
                count,
                &mut self.flags,
            );
            (r, ctx.ki, ctx.root)
        };
        self.stack.set_ki(ki);
        self.stack.set_root(root);
        result
    }

    /// Process previous leaf batch without key materialization (values only).
    #[inline]
    pub(crate) fn process_prev_leaf_batch_values<F>(
        &mut self,
        start_bound_ikey: Option<u64>,
        visitor: &mut F,
        count: &mut usize,
    ) -> LeafBatchResultBack
    where
        F: FnMut(P::Output) -> bool,
    {
        let (result, ki, root) = {
            let mut ctx =
                build_reverse_batch_ctx(&self.stack, &mut self.cursor_key, &mut self.layer_stack);
            let r = process_batch_values::<Backward, P>(
                &mut ctx,
                start_bound_ikey,
                visitor,
                count,
                &mut self.flags,
            );
            (r, ctx.ki, ctx.root)
        };
        self.stack.set_ki(ki);
        self.stack.set_root(root);
        result
    }

    /// Advance to previous leaf in the B-link chain (batch processing variant).
    ///
    /// Returns `true` if successfully advanced, `false` if layer exhausted.
    #[inline]
    pub(crate) fn advance_prev_leaf(&mut self, guard: &LocalGuard<'_>) -> bool {
        let current_ptr: *mut LeafNode15<P> = self.stack.get_leaf_ptr();
        if current_ptr.is_null() {
            return false;
        }

        // SAFETY: current_ptr was validated
        let current_leaf: &LeafNode15<P> = unsafe { &*current_ptr };

        // Update cursor key with current leaf's ikey_bound
        let ikey_bound: u64 = current_leaf.ikey_bound();
        self.cursor_key.assign_store_ikey(ikey_bound);
        self.cursor_key.assign_store_length(0);

        // Get previous leaf pointer
        let prev_ptr: *mut LeafNode15<P> = ReverseScanHelper::retreat(current_leaf);

        if prev_ptr.is_null() {
            self.stack.set_leaf(StdPtr::null_mut());
            return false;
        }

        // Prefetch previous leaf
        prefetch_read(prev_ptr.cast::<u8>());

        // Set up stack with new leaf
        self.stack.set_leaf(prev_ptr);

        // Get stable version (may follow forward chain)
        let mut leaf_ptr: *mut LeafNode15<P> = prev_ptr;
        let version: u32 =
            ReverseScanHelper::stable_reverse(&mut leaf_ptr, &self.cursor_key, guard);

        if leaf_ptr != prev_ptr {
            self.stack.set_leaf(leaf_ptr);
        }

        // Prefetch the prev-prev leaf for pipelining
        let leaf: &LeafNode15<P> = unsafe { &*leaf_ptr };
        let prev_prev: *mut LeafNode15<P> = leaf.prev(guard);
        if !prev_prev.is_null() {
            prefetch_read(prev_prev.cast::<u8>());
        }

        let perm: <LeafNode15<P> as TreeLeafNode<P>>::Perm = leaf.permutation();
        let size: usize = perm.size();

        let ki: isize = if size > 0 {
            (size - 1).cast_signed()
        } else {
            -1
        };

        self.stack.update_state(version, perm, ki);
        true
    }
}

// ============================================================================
//  Batch strategy trait + run_batch_reverse unified loop
// ============================================================================

/// Result of a reverse strategy's `emit_initial` call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReverseBatchAction {
    /// Entry emitted (or skipped), continue scanning.
    Continue,

    /// Visitor returned false, stop scanning.
    Stopped,

    /// Start bound exceeded, mark exhausted and stop.
    Exhausted,
}

/// Strategy trait for reverse batch scanning.
///
/// Three implementations:
///
/// - [`RevIntraLeafRefStrategy`]: `&P::Value` refs via `process_prev_leaf_batch_ptr`
/// - [`RevIntraLeafCopyStrategy`]: `P::Output` by value via `process_prev_leaf_batch`
/// - [`RevValuesOnlyStrategy`]: `P::Output` values only via `process_prev_leaf_batch_values`
///
/// After monomorphization, all trait dispatch is fully inlined, zero overhead.
pub trait ReverseBatchStrategy<P: LeafPolicy> {
    /// Emit the initial entry from `initialize_back()`'s snapshot.
    fn emit_initial(
        &mut self,
        ctx: &mut ReverseScanCtx<P>,
        start_bound: &RangeBound<'_>,
        count: &mut usize,
    ) -> ReverseBatchAction;

    /// Process the current leaf's entries in reverse (hot path).
    fn process_leaf(
        &mut self,
        ctx: &mut ReverseScanCtx<P>,
        start_bound: &RangeBound<'_>,
        start_bound_ikey: Option<u64>,
        count: &mut usize,
    ) -> LeafBatchResultBack;
}

impl<P: LeafPolicy> ReverseScanCtx<P> {
    /// Unified reverse batch scan loop driven by a strategy.
    ///
    /// Handles the state machine skeleton (Down/Up/Retry/null/deleted checks)
    /// and delegates entry emission and leaf processing to the strategy.
    ///
    /// Callers must handle exhausted/initialized checks before calling this.
    #[inline]
    pub(crate) fn run_batch_reverse<S: ReverseBatchStrategy<P>>(
        &mut self,
        strategy: &mut S,
        start_bound: &RangeBound<'_>,
        start_bound_ikey: Option<u64>,
        guard: &LocalGuard<'_>,
    ) -> usize {
        let mut count: usize = 0;

        // Handle initial Emit state from initialize_back() if present
        if self.state == ScanStateBack::Emit {
            match strategy.emit_initial(self, start_bound, &mut count) {
                ReverseBatchAction::Continue => {
                    self.state = ScanStateBack::FindPrev;
                }
                ReverseBatchAction::Stopped => return count,
                ReverseBatchAction::Exhausted => {
                    self.flags.mark_exhausted();
                    return count;
                }
            }
        }

        loop {
            // Handle rare states (layer transitions, retries)
            match self.state {
                ScanStateBack::Down => {
                    self.handle_down_back();
                    self.state = ScanStateBack::Retry;
                    self.flags.require_duplicate_check();
                    continue;
                }

                ScanStateBack::Up => {
                    if !self.handle_up_back(guard) {
                        self.flags.mark_exhausted();
                        return count;
                    }

                    self.state = ScanStateBack::FindPrev;
                    self.flags.require_duplicate_check();

                    continue;
                }

                ScanStateBack::Retry => {
                    let (new_state, _) = self.reposition_back(guard);
                    self.state = new_state;
                    self.flags.require_duplicate_check();
                    continue;
                }

                ScanStateBack::Emit | ScanStateBack::FindPrev => {}
            }

            // Check for null stack (layer exhausted)
            if self.stack.get_leaf_ptr().is_null() {
                if self.layer_stack.is_empty() {
                    self.flags.mark_exhausted();
                    return count;
                }

                self.state = ScanStateBack::Up;
                continue;
            }

            // Check leaf deletion
            // SAFETY: null check above ensures leaf_ptr is valid,
            // and the guard protects the node from deallocation.
            let leaf: &LeafNode15<P> = unsafe { &*self.stack.get_leaf_ptr() };
            if leaf.version().is_deleted() {
                self.state = ScanStateBack::Retry;
                continue;
            }

            // Hot path: process leaf batch in reverse
            let result = strategy.process_leaf(self, start_bound, start_bound_ikey, &mut count);

            match result {
                LeafBatchResultBack::LeafExhausted => {
                    if !self.advance_prev_leaf(guard) {
                        if self.layer_stack.is_empty() {
                            self.flags.mark_exhausted();
                            return count;
                        }

                        self.state = ScanStateBack::Up;
                    }
                }

                LeafBatchResultBack::LayerEncountered => {
                    self.state = ScanStateBack::Down;
                }

                LeafBatchResultBack::VersionChanged => {
                    self.state = ScanStateBack::Retry;
                }

                LeafBatchResultBack::Stopped => return count,

                LeafBatchResultBack::StartBoundExceeded => {
                    self.flags.mark_exhausted();
                    return count;
                }
            }
        }
    }
}

// ============================================================================
//  Strategy implementations
// ============================================================================

/// Reverse intra-leaf batch strategy with `&P::Value` references.
///
/// Used by [`super::iterator::RangeIter::rev_for_each_ref`].
pub struct RevIntraLeafRefStrategy<'a, F> {
    visitor: &'a mut F,
}

impl<'a, F> RevIntraLeafRefStrategy<'a, F> {
    #[inline]
    pub const fn new(visitor: &'a mut F) -> Self {
        Self { visitor }
    }
}

impl<P, F> ReverseBatchStrategy<P> for RevIntraLeafRefStrategy<'_, F>
where
    P: crate::policy::RefPolicy,
    F: FnMut(&[u8], &P::Value) -> bool,
{
    #[inline(always)]
    fn emit_initial(
        &mut self,
        ctx: &mut ReverseScanCtx<P>,
        start_bound: &RangeBound<'_>,
        count: &mut usize,
    ) -> ReverseBatchAction {
        let Some(snapshot) = ctx.snapshot.take() else {
            return ReverseBatchAction::Continue;
        };

        // SAFETY: CursorKey invariant guarantees offset + len <= MAX_KEY_LENGTH
        let key: &[u8] = unsafe { ctx.cursor_key.full_key_unchecked() };

        if !start_bound.contains_reverse(key) {
            return ReverseBatchAction::Exhausted;
        }

        *count += 1;

        // SAFETY: Guard protects the output. output_as_ref_sound uses
        // atomic read for write-through types, avoiding aliasing violation.
        let mut scratch = std::mem::MaybeUninit::uninit();
        let value_ref: &P::Value = unsafe { P::output_as_ref_sound(&snapshot.value, &mut scratch) };
        let should_continue = (self.visitor)(key, value_ref);

        if should_continue {
            ReverseBatchAction::Continue
        } else {
            ReverseBatchAction::Stopped
        }
    }

    #[inline(always)]
    fn process_leaf(
        &mut self,
        ctx: &mut ReverseScanCtx<P>,
        start_bound: &RangeBound<'_>,
        start_bound_ikey: Option<u64>,
        count: &mut usize,
    ) -> LeafBatchResultBack {
        ctx.process_prev_leaf_batch_ptr(start_bound, start_bound_ikey, self.visitor, count)
    }
}

/// Reverse intra-leaf batch strategy with `P::Output` by value.
///
/// Used by [`super::iterator::RangeIter::rev_for_each_intra_leaf_batch`].
pub struct RevIntraLeafCopyStrategy<'a, F> {
    visitor: &'a mut F,
}

impl<'a, F> RevIntraLeafCopyStrategy<'a, F> {
    #[inline]
    pub const fn new(visitor: &'a mut F) -> Self {
        Self { visitor }
    }
}

impl<P, F> ReverseBatchStrategy<P> for RevIntraLeafCopyStrategy<'_, F>
where
    P: LeafPolicy,
    F: FnMut(&[u8], P::Output) -> bool,
{
    #[inline(always)]
    fn emit_initial(
        &mut self,
        ctx: &mut ReverseScanCtx<P>,
        start_bound: &RangeBound<'_>,
        count: &mut usize,
    ) -> ReverseBatchAction {
        let Some(snapshot) = ctx.snapshot.take() else {
            return ReverseBatchAction::Continue;
        };

        // SAFETY: CursorKey invariant guarantees offset + len <= MAX_KEY_LENGTH
        let key: &[u8] = unsafe { ctx.cursor_key.full_key_unchecked() };

        if !start_bound.contains_reverse(key) {
            return ReverseBatchAction::Exhausted;
        }

        *count += 1;

        if (self.visitor)(key, snapshot.value) {
            ReverseBatchAction::Continue
        } else {
            ReverseBatchAction::Stopped
        }
    }

    #[inline(always)]
    fn process_leaf(
        &mut self,
        ctx: &mut ReverseScanCtx<P>,
        start_bound: &RangeBound<'_>,
        start_bound_ikey: Option<u64>,
        count: &mut usize,
    ) -> LeafBatchResultBack {
        ctx.process_prev_leaf_batch(start_bound, start_bound_ikey, self.visitor, count)
    }
}

/// Reverse values-only batch strategy — no key materialization.
///
/// Used by [`super::iterator::RangeIter::rev_for_each_values_batch`].
pub struct RevValuesOnlyStrategy<'a, F> {
    visitor: &'a mut F,
}

impl<'a, F> RevValuesOnlyStrategy<'a, F> {
    #[inline]
    pub const fn new(visitor: &'a mut F) -> Self {
        Self { visitor }
    }
}

impl<P, F> ReverseBatchStrategy<P> for RevValuesOnlyStrategy<'_, F>
where
    P: LeafPolicy,
    F: FnMut(P::Output) -> bool,
{
    #[inline(always)]
    fn emit_initial(
        &mut self,
        ctx: &mut ReverseScanCtx<P>,
        _start_bound: &RangeBound<'_>,
        count: &mut usize,
    ) -> ReverseBatchAction {
        let Some(snapshot) = ctx.snapshot.take() else {
            return ReverseBatchAction::Continue;
        };
        // Values-only: skip start bound key check (approximate)
        *count += 1;
        if (self.visitor)(snapshot.value) {
            ReverseBatchAction::Continue
        } else {
            ReverseBatchAction::Stopped
        }
    }

    #[inline(always)]
    fn process_leaf(
        &mut self,
        ctx: &mut ReverseScanCtx<P>,
        start_bound: &RangeBound<'_>,
        _start_bound_ikey: Option<u64>,
        count: &mut usize,
    ) -> LeafBatchResultBack {
        // Layer-aware ikey extraction: align start-bound ikey with the current
        // trie depth so descended scans compare sublayer ikeys correctly.
        let start_bound_ikey: Option<u64> = start_bound.extract_ikey_at(ctx.cursor_key.offset());
        ctx.process_prev_leaf_batch_values(start_bound_ikey, self.visitor, count)
    }
}
