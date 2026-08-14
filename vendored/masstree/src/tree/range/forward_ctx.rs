//! Filepath: `src/tree/range/forward_ctx.rs`
//!
//! Forward scan context, owns all forward iteration state.

use std::cmp::Ordering;
use std::ptr as StdPtr;

use seize::LocalGuard;

use std::mem::MaybeUninit;

use crate::hints::likely;
use crate::key::IKEY_SIZE;
use crate::leaf_trait::TreeLeafNode;
use crate::leaf15::LeafNode15;
use crate::leaf15::{KSUF_KEYLENX, LAYER_KEYLENX};
use crate::link::Linker;
use crate::nodeversion::NodeVersion;
use crate::policy::LeafPolicy;
use crate::policy::RefPolicy as RefLeafPolicy;
use crate::prefetch::prefetch_read;

use super::batch_common::{
    BatchCtx, CloneEmitter, CopySlotVisitor, Forward, PtrEmitter, RefSlotVisitor, ScanEmitter,
    process_batch_keyed, process_batch_values,
};
#[cfg(debug_assertions)]
use super::cursor_key::CursorDebugState;
use super::cursor_key::CursorKey;
use super::find::LeafBatchResult;
use super::helper::{
    KeyIndexedPosition, initial_ksuf_match, lower_with_position, lower_with_suffix,
};
use super::iterator::RangeBound;
use super::iterator::iter_flags::ForwardFlags;
use super::scan_state::{
    LayerContext, LayerStack, ScanSnapshot, ScanSnapshotPtr, ScanStackElement, ScanState,
    StepResult,
};
use super::traversal::reach_leaf_for_scan;

// ============================================================================
//  ForwardScanCtx
// ============================================================================

/// Forward scan context - owns all per-direction state for forward iteration.
///
/// This struct bundles the scan position, layer stack, cursor key, state machine
/// state, and flags that were previously individual fields on `RangeIter`.
///
/// # Design
///
/// Methods on this struct replace the free functions from `find.rs`, giving
/// a natural `self.find_next(guard)` interface instead of
/// `find_next(&mut stack, &mut cursor_key, &mut layer_stack, guard)`.
pub struct ForwardScanCtx<P: LeafPolicy> {
    /// Current scan position (leaf, version, permutation, ki).
    pub(crate) stack: ScanStackElement<P>,

    /// Parent layer stack for sublayer navigation.
    pub(crate) layer_stack: LayerStack<P>,

    /// Cursor tracking current key position.
    pub(crate) cursor_key: CursorKey,

    /// Current state machine state.
    pub(crate) state: ScanState,

    /// Captured snapshot for current entry (if in Emit state).
    pub(crate) snapshot: Option<ScanSnapshot<P>>,

    /// Tracks the output from `initialize()`'s snapshot.
    ///
    /// Only used for the first entry case in `advance_no_alloc_ref`,
    /// where we keep the `P::Output` alive so that `output_as_ref` can
    /// return a borrowed `&P::Value` from it.
    pub(crate) last_output: Option<P::Output>,

    /// Scratch storage for atomic value copies (write-through path).
    ///
    /// Used by `advance_no_alloc_ref` to hold an atomically-read value copy
    /// so a `&P::Value` can be returned without aliasing the mutable Box data.
    /// Only written when `P::CAN_WRITE_THROUGH` is true (V <= 8 bytes).
    pub(crate) scratch_value: MaybeUninit<P::Value>,

    /// Packed forward-specific boolean flags.
    pub(crate) flags: ForwardFlags,

    // ========================================================================
    //  Debug-only fields for ordering violation detection
    // ========================================================================
    /// Last emitted key for forward iteration (debug builds only).
    #[cfg(debug_assertions)]
    pub(crate) debug_last_emitted_key: Option<Vec<u8>>,

    /// Cursor state at last emission (debug builds only).
    #[cfg(debug_assertions)]
    pub(crate) debug_last_cursor_state: Option<CursorDebugState>,

    /// Ring buffer of recent state transitions for debugging.
    #[cfg(debug_assertions)]
    pub(crate) debug_transition_history: [Option<String>; 32],

    /// Write index into `debug_transition_history`.
    #[cfg(debug_assertions)]
    pub(crate) debug_transition_idx: usize,

    /// Number of entries written (saturates at 32).
    #[cfg(debug_assertions)]
    pub(crate) debug_transition_count: usize,
}

// ============================================================================
//  Construction & Debug Helpers
// ============================================================================

impl<P: LeafPolicy> ForwardScanCtx<P> {
    /// Create a new forward scan context.
    #[inline]
    pub fn new(
        root: *const u8,
        cursor_key: CursorKey,
        emit_equal: bool,
        single_layer_mode: bool,
    ) -> Self {
        Self {
            stack: ScanStackElement::new(root),
            layer_stack: LayerStack::new(),
            cursor_key,
            state: ScanState::FindNext,
            snapshot: None,
            last_output: None,
            scratch_value: MaybeUninit::uninit(),
            flags: ForwardFlags::with_values(emit_equal, single_layer_mode),

            #[cfg(debug_assertions)]
            debug_last_emitted_key: None,
            #[cfg(debug_assertions)]
            debug_last_cursor_state: None,
            #[cfg(debug_assertions)]
            debug_transition_history: [const { None }; 32],
            #[cfg(debug_assertions)]
            debug_transition_idx: 0,
            #[cfg(debug_assertions)]
            debug_transition_count: 0,
        }
    }

    /// Assert that keys are emitted in strictly increasing order (debug builds only).
    #[cfg(debug_assertions)]
    #[inline]
    #[allow(
        clippy::panic,
        reason = "Intentional panic for debug-only ordering violation detection"
    )]
    pub(crate) fn assert_ordering(&mut self, key: &[u8]) {
        if let Some(ref last_key) = self.debug_last_emitted_key
            && key <= last_key.as_slice()
        {
            let current_state: CursorDebugState = self.cursor_key.debug_state();
            let last_state: Option<&CursorDebugState> = self.debug_last_cursor_state.as_ref();

            eprintln!("\n=== ORDERING VIOLATION DETECTED (batch path) ===");
            eprintln!("Current key:  {:?}", String::from_utf8_lossy(key));
            eprintln!("Last key:     {:?}", String::from_utf8_lossy(last_key));
            eprintln!("Current key bytes: {key:?}");
            eprintln!("Last key bytes:    {last_key:?}");
            eprintln!("Current cursor: {current_state}");
            if let Some(last) = last_state {
                eprintln!("Last cursor:    {last}");
            }

            eprintln!("\n--- Recent state transitions ---");
            let count = self.debug_transition_count;
            let start = if count < 32 {
                0
            } else {
                self.debug_transition_idx
            };
            for i in 0..count {
                let idx = (start + i) % 32;
                if let Some(ref transition) = self.debug_transition_history[idx] {
                    eprintln!("[{i}] {transition}");
                }
            }
            eprintln!("--- End transitions ---");
            eprintln!("=== END ORDERING VIOLATION ===\n");

            panic!(
                "Scan ordering violation: emitted key {:?} is not > last emitted key {:?}",
                String::from_utf8_lossy(key),
                String::from_utf8_lossy(last_key)
            );
        }

        self.debug_last_emitted_key = Some(key.to_vec());
        self.debug_last_cursor_state = Some(self.cursor_key.debug_state());
        self.record_transition(format!(
            "EMIT: {:?} cursor={}",
            String::from_utf8_lossy(key),
            self.cursor_key.debug_state()
        ));
    }

    /// Record a state transition for debugging (debug builds only).
    #[cfg(debug_assertions)]
    #[inline]
    pub(crate) fn record_transition(&mut self, description: String) {
        self.debug_transition_history[self.debug_transition_idx] = Some(description);
        self.debug_transition_idx = (self.debug_transition_idx + 1) % 32;
        if self.debug_transition_count < 32 {
            self.debug_transition_count += 1;
        }
    }
}

// ============================================================================
//  find_initial — Position at start bound
// ============================================================================

impl<P: LeafPolicy> ForwardScanCtx<P> {
    /// Find the initial position for a range scan.
    ///
    /// Corresponds to the free function `find::find_initial`.
    pub fn find_initial(
        &mut self,
        root: *const u8,
        emit_equal: bool,
        guard: &LocalGuard<'_>,
    ) -> (ScanState, Option<ScanSnapshot<P>>)
    where
        P::Output: Clone,
    {
        self.stack.set_root(root);

        let leaf_ptr: *mut LeafNode15<P> = reach_leaf_for_scan::<P>(root, &self.cursor_key, guard);

        if leaf_ptr.is_null() {
            return (ScanState::Up, None);
        }

        self.stack.set_leaf(leaf_ptr);

        // SAFETY: leaf_ptr is valid (non-null checked above, guard protects it)
        let leaf: &LeafNode15<P> = unsafe { &*leaf_ptr };

        let version: u32 = leaf.version().stable();

        if NodeVersion::is_deleted_version(version) {
            return (ScanState::Retry, None);
        }

        let perm: <LeafNode15<P> as TreeLeafNode<P>>::Perm = leaf.permutation();

        let kx: KeyIndexedPosition = lower_with_position(&self.cursor_key, leaf, &perm);

        let (next_state, snapshot) = match kx.p {
            Some(slot) => Self::handle_initial_match(
                leaf,
                slot,
                &mut self.cursor_key,
                &mut self.stack,
                emit_equal,
                version,
                &perm,
                kx.i,
            ),
            None => (ScanState::FindNext, None),
        };

        if leaf.version().has_changed(version) {
            return (ScanState::Retry, None);
        }

        let final_pos = if kx.p.is_some() { kx.i + 1 } else { kx.i };
        self.stack.update_state(version, perm, final_pos);

        // TURSO PATCH: an initial emission resolves the pending Included
        // start (see ForwardFlags::clear_emit_equal).
        if matches!(next_state, ScanState::Emit) {
            self.flags.clear_emit_equal();
        }

        (next_state, snapshot)
    }

    /// Handle an exact ikey match in `find_initial`.
    #[expect(clippy::too_many_arguments, reason = "Internals")]
    #[inline(always)]
    fn handle_initial_match(
        leaf: &LeafNode15<P>,
        slot: usize,
        cursor_key: &mut CursorKey,
        stack: &mut ScanStackElement<P>,
        emit_equal: bool,
        _version: u32,
        _perm: &<LeafNode15<P> as TreeLeafNode<P>>::Perm,
        _pos: usize,
    ) -> (ScanState, Option<ScanSnapshot<P>>)
    where
        P::Output: Clone,
    {
        let keylenx: u8 = leaf.keylenx_relaxed(slot);

        if keylenx >= LAYER_KEYLENX {
            let slot_ikey: u64 = leaf.ikey_relaxed(slot);
            let layer_ptr: *mut u8 = leaf.load_layer_raw(slot);
            cursor_key.assign_store_ikey(slot_ikey);
            prefetch_read(layer_ptr);
            stack.set_root(layer_ptr);
            return (ScanState::Down, None);
        }

        if keylenx == KSUF_KEYLENX
            && let Some(stored_suffix) = leaf.ksuf(slot)
        {
            let cursor_suffix: &[u8] = cursor_key.suffix();
            let cmp = stored_suffix.cmp(cursor_suffix);

            if initial_ksuf_match(cmp, emit_equal)
                && let Some(output) = leaf.load_value(slot)
            {
                let key_len = IKEY_SIZE + stored_suffix.len();
                cursor_key.assign_store_ikey(leaf.ikey_relaxed(slot));
                let _ = cursor_key.assign_store_suffix(stored_suffix);
                cursor_key.assign_store_length(key_len);

                return (ScanState::Emit, Some(ScanSnapshot::new(output, key_len)));
            }
            return (ScanState::FindNext, None);
        }

        if emit_equal && let Some(output) = leaf.load_value(slot) {
            let key_len = keylenx as usize;
            cursor_key.assign_store_ikey(leaf.ikey_relaxed(slot));
            cursor_key.assign_store_length(key_len);

            return (ScanState::Emit, Some(ScanSnapshot::new(output, key_len)));
        }

        (ScanState::FindNext, None)
    }
}

// ============================================================================
//  find_next variants - Main iteration
// ============================================================================

impl<P: LeafPolicy> ForwardScanCtx<P> {
    /// Find the next entry in the scan sequence (clone path).
    ///
    /// Corresponds to `find::find_next`.
    #[inline]
    pub fn find_next(&mut self, guard: &LocalGuard<'_>) -> (ScanState, Option<ScanSnapshot<P>>)
    where
        P::Output: Clone,
    {
        self.find_next_generic::<CloneEmitter>(guard, false)
    }

    /// Find next with duplicate checking (clone path).
    ///
    /// Corresponds to `find::find_next_with_duplicate_check`.
    #[inline]
    pub fn find_next_with_dup_check(
        &mut self,
        guard: &LocalGuard<'_>,
    ) -> (ScanState, Option<ScanSnapshot<P>>)
    where
        P::Output: Clone,
    {
        self.find_next_generic::<CloneEmitter>(guard, true)
    }

    /// Find next entry, returning raw pointer (zero-copy path).
    ///
    /// Corresponds to `find::find_next_ptr`.
    #[inline]
    pub fn find_next_ptr(
        &mut self,
        guard: &LocalGuard<'_>,
    ) -> (ScanState, Option<ScanSnapshotPtr<P::Value>>) {
        self.find_next_generic::<PtrEmitter>(guard, false)
    }

    /// Find next with duplicate checking, returning raw pointer.
    ///
    /// Corresponds to `find::find_next_with_duplicate_check_ptr`.
    #[inline]
    pub fn find_next_with_dup_check_ptr(
        &mut self,
        guard: &LocalGuard<'_>,
    ) -> (ScanState, Option<ScanSnapshotPtr<P::Value>>) {
        self.find_next_generic::<PtrEmitter>(guard, true)
    }

    /// Generic implementation of `find_next` with configurable duplicate checking
    /// and clone vs zero-copy value emission via [`ScanEmitter`].
    #[inline]
    fn find_next_generic<E: ScanEmitter<P>>(
        &mut self,
        guard: &LocalGuard<'_>,
        needs_duplicate_check: bool,
    ) -> (ScanState, Option<E::Snapshot>) {
        if self.stack.is_null() {
            return (ScanState::Up, None);
        }

        let leaf: &LeafNode15<P> = unsafe { self.stack.leaf_ref() };

        if leaf.version().is_deleted() {
            return (ScanState::Retry, None);
        }

        let Some(slot) = self.stack.kp() else {
            return self.advance_leaf_generic::<E>(guard);
        };

        let slot_ikey: u64 = leaf.ikey_relaxed(slot);
        let slot_keylenx: u8 = leaf.keylenx_relaxed(slot);
        leaf.prefetch_value(slot);

        if slot_ikey < self.stack.last_ikey() {
            return (ScanState::Retry, None);
        }

        if needs_duplicate_check {
            let cmp: Ordering = self.cursor_key.compare(slot_ikey, slot_keylenx as usize);

            let is_dup: bool = if likely(cmp == Ordering::Less) {
                false
            } else if cmp == Ordering::Greater {
                true
            } else if slot_keylenx == KSUF_KEYLENX && self.cursor_key.has_suffix() {
                leaf.ksuf(slot).is_none_or(|stored_suffix| {
                    let suffix_cmp = self.cursor_key.compare_suffix(stored_suffix);
                    // TURSO PATCH: while the Included start key is still
                    // pending, a full-key match with the cursor is the start
                    // key itself, not a duplicate of an emitted key.
                    if suffix_cmp == Ordering::Equal && self.flags.emit_equal() {
                        false
                    } else {
                        suffix_cmp != Ordering::Less
                    }
                })
            } else {
                // TURSO PATCH: same for exact inline matches — the initial
                // descent can land one leaf left of an Included start key,
                // and the repositioned duplicate check must not swallow it.
                !self.flags.emit_equal()
            };

            if is_dup {
                self.stack.next();
                return (ScanState::FindNext, None);
            }
        }

        if slot_keylenx >= LAYER_KEYLENX {
            let layer_ptr: *mut u8 = leaf.load_layer_raw(slot);
            self.layer_stack
                .push(LayerContext::new(self.stack.root(), self.stack.leaf_ptr()));
            self.cursor_key.assign_store_ikey(slot_ikey);
            prefetch_read(layer_ptr);
            self.stack.set_root(layer_ptr);

            return (ScanState::Down, None);
        }

        let key_len: usize = if slot_keylenx == KSUF_KEYLENX {
            if let Some(suffix) = leaf.ksuf(slot) {
                let suffix_len: usize = suffix.len();
                self.cursor_key.assign_store_ikey(slot_ikey);
                let _ = self.cursor_key.assign_store_suffix(suffix);
                self.cursor_key.assign_store_length(IKEY_SIZE + suffix_len);
                IKEY_SIZE + suffix_len
            } else {
                self.cursor_key.assign_store_ikey(slot_ikey);
                self.cursor_key.assign_store_length(IKEY_SIZE);
                IKEY_SIZE
            }
        } else {
            let len: usize = slot_keylenx as usize;
            self.cursor_key.assign_store_ikey(slot_ikey);
            self.cursor_key.assign_store_length(len);
            len
        };

        let Some(snapshot) = E::emit_value(leaf, slot, key_len) else {
            self.stack.next();
            return (ScanState::FindNext, None);
        };

        self.cursor_key.mark_key_complete();
        // TURSO PATCH: first emission resolves the pending Included start.
        self.flags.clear_emit_equal();
        self.stack.set_last_ikey(slot_ikey);
        self.stack.next();

        (ScanState::Emit, Some(snapshot))
    }
}

// ============================================================================
//  Single-layer fast path
// ============================================================================

impl<P: LeafPolicy> ForwardScanCtx<P> {
    /// Single-layer fast path for zero-copy `find_next`.
    ///
    /// Corresponds to `find::find_next_single_layer_ptr`.
    #[inline]
    #[expect(clippy::cast_possible_truncation)]
    pub fn find_next_single_layer_ptr(
        &mut self,
        guard: &LocalGuard<'_>,
        needs_duplicate_check: bool,
    ) -> (ScanState, Option<ScanSnapshotPtr<P::Value>>) {
        if self.stack.is_null() {
            return (ScanState::FindNext, None);
        }

        let leaf: &LeafNode15<P> = unsafe { self.stack.leaf_ref() };

        if leaf.version().is_deleted() {
            return (ScanState::Retry, None);
        }

        // Check if leaf was concurrently modified since we cached the version.
        // Symmetric with reverse's find_prev_single_layer (reverse_ctx.rs).
        let version: u32 = self.stack.version();
        if leaf.version().has_changed(version) {
            return (ScanState::Retry, None);
        }

        let Some(slot) = self.stack.kp() else {
            return self.advance_leaf_single_layer(guard);
        };

        let slot_ikey: u64 = leaf.ikey_relaxed(slot);
        let slot_keylenx: u8 = leaf.keylenx_relaxed(slot);
        leaf.prefetch_value(slot);

        if slot_ikey < self.stack.last_ikey() {
            return (ScanState::Retry, None);
        }

        if needs_duplicate_check {
            let cmp: Ordering = self.cursor_key.compare(slot_ikey, slot_keylenx as usize);
            // TURSO PATCH: a cursor-equal slot is the still-pending Included
            // start key, not a duplicate (see the multi-layer check above).
            let is_dup: bool = match cmp {
                Ordering::Less => false,
                Ordering::Equal => !self.flags.emit_equal(),
                Ordering::Greater => true,
            };

            if is_dup {
                self.stack.next();
                return (ScanState::FindNext, None);
            }
        }

        // True layer pointer — prepare for descent
        if slot_keylenx >= LAYER_KEYLENX {
            self.cursor_key.assign_store_ikey(slot_ikey);
            let layer_ptr: *mut u8 = leaf.load_layer_raw(slot);
            prefetch_read(layer_ptr);
            return (ScanState::Down, None);
        }

        // Suffix key (KSUF) or other non-inline — bail to multi-layer path.
        // Returning FindNext after disabling single-layer mode causes the caller
        // to re-enter the loop and fall through to the multi-layer path which
        // handles KSUF correctly via find_next_ptr.
        if slot_keylenx > IKEY_SIZE as u8 {
            self.flags.disable_single_layer_mode();
            return (ScanState::FindNext, None);
        }

        let slot_ptr: *mut u8 = leaf.load_value_raw(slot);
        if slot_ptr.is_null() {
            self.stack.next();
            return (ScanState::FindNext, None);
        }

        let key_len: usize = slot_keylenx as usize;
        self.cursor_key.assign_store_ikey(slot_ikey);
        self.cursor_key.assign_store_length(key_len);
        self.cursor_key.mark_key_complete();

        self.stack.set_last_ikey(slot_ikey);
        self.stack.next();

        (
            ScanState::Emit,
            Some(ScanSnapshotPtr::from_raw(slot_ptr, key_len)),
        )
    }

    /// Advance to next leaf in single-layer mode.
    #[inline(always)]
    fn advance_leaf_single_layer(
        &mut self,
        guard: &LocalGuard<'_>,
    ) -> (ScanState, Option<ScanSnapshotPtr<P::Value>>) {
        let leaf: &LeafNode15<P> = unsafe { self.stack.leaf_ref() };
        let version: u32 = self.stack.version();

        let next_raw: *mut LeafNode15<P> = leaf.next_raw(guard);

        if Linker::is_marked(next_raw) {
            leaf.wait_for_split();
            return (ScanState::Retry, None);
        }

        if leaf.version().has_changed(version) {
            return (ScanState::Retry, None);
        }

        let next: *mut LeafNode15<P> = next_raw.map_addr(|addr| addr & !1);

        if next.is_null() {
            self.stack.set_leaf(StdPtr::null_mut());
            return (ScanState::FindNext, None);
        }

        self.stack.set_leaf(next);

        // SAFETY: next is non-null and protected by guard
        let next_leaf: &LeafNode15<P> = unsafe { &*next };

        next_leaf.prefetch();

        let next_next: *mut LeafNode15<P> = next_leaf.safe_next(guard);
        if !next_next.is_null() {
            // SAFETY: next_next is non-null and derived from a valid leaf's B-link
            let next_next_leaf: &LeafNode15<P> = unsafe { &*next_next };
            next_next_leaf.prefetch();
        }

        let next_version: u32 = next_leaf.version().stable();

        if NodeVersion::is_deleted_version(next_version) {
            return (ScanState::Retry, None);
        }

        let perm: <LeafNode15<P> as TreeLeafNode<P>>::Perm = next_leaf.permutation();

        let pos: usize = lower_with_suffix(&self.cursor_key, next_leaf, &perm, self.flags.emit_equal());

        self.stack.update_state(next_version, perm, pos);

        (ScanState::FindNext, None)
    }
}

// ============================================================================
//  Leaf advancement, retry, layer transitions
// ============================================================================

impl<P: LeafPolicy> ForwardScanCtx<P> {
    /// Advance to next leaf, zero-copy variant.
    ///
    /// Corresponds to `find::advance_leaf_ptr`.
    #[inline]
    pub fn advance_leaf(
        &mut self,
        guard: &LocalGuard<'_>,
    ) -> (ScanState, Option<ScanSnapshotPtr<P::Value>>) {
        self.advance_leaf_generic::<PtrEmitter>(guard)
    }

    /// Generic advance to next leaf.
    #[inline]
    fn advance_leaf_generic<E: ScanEmitter<P>>(
        &mut self,
        guard: &LocalGuard<'_>,
    ) -> (ScanState, Option<E::Snapshot>) {
        let leaf: &LeafNode15<P> = unsafe { self.stack.leaf_ref() };
        let version: u32 = self.stack.version();

        let next_raw: *mut LeafNode15<P> = leaf.next_raw(guard);

        if Linker::is_marked(next_raw) {
            leaf.wait_for_split();
            return (ScanState::Retry, None);
        }

        if leaf.version().has_changed(version) {
            return (ScanState::Retry, None);
        }

        let next: *mut LeafNode15<P> = next_raw.map_addr(|addr| addr & !1);

        if next.is_null() {
            return (ScanState::Up, None);
        }

        self.stack.set_leaf(next);

        // SAFETY: next is non-null and protected by guard
        let next_leaf: &LeafNode15<P> = unsafe { &*next };

        next_leaf.prefetch();

        let next_next: *mut LeafNode15<P> = next_leaf.safe_next(guard);
        if !next_next.is_null() {
            // SAFETY: next_next is non-null and derived from a valid leaf's B-link
            let next_next_leaf: &LeafNode15<P> = unsafe { &*next_next };
            next_next_leaf.prefetch();
        }

        let next_version: u32 = next_leaf.version().stable();

        if NodeVersion::is_deleted_version(next_version) {
            return (ScanState::Retry, None);
        }

        let perm: <LeafNode15<P> as TreeLeafNode<P>>::Perm = next_leaf.permutation();

        let pos: usize = lower_with_suffix(&self.cursor_key, next_leaf, &perm, self.flags.emit_equal());

        self.stack.update_state(next_version, perm, pos);

        (ScanState::FindNext, None)
    }

    /// Reposition after a conflict or layer transition.
    ///
    /// Corresponds to `find::find_retry`.
    pub fn find_retry(&mut self, guard: &LocalGuard<'_>) -> ScanState {
        let leaf_ptr: *mut LeafNode15<P> =
            reach_leaf_for_scan::<P>(self.stack.root(), &self.cursor_key, guard);

        if leaf_ptr.is_null() {
            return ScanState::Up;
        }

        self.stack.set_leaf(leaf_ptr);

        // SAFETY: leaf_ptr is non-null and protected by guard
        let leaf: &LeafNode15<P> = unsafe { &*leaf_ptr };

        let version: u32 = leaf.version().stable();

        if NodeVersion::is_deleted_version(version) {
            return ScanState::Retry;
        }

        let perm: <LeafNode15<P> as TreeLeafNode<P>>::Perm = leaf.permutation();

        let pos: usize = lower_with_suffix(&self.cursor_key, leaf, &perm, self.flags.emit_equal());

        self.stack.update_state(version, perm, pos);

        ScanState::FindNext
    }

    /// Handle descent into a sublayer (Down state).
    ///
    /// Corresponds to `find::handle_down`.
    pub fn handle_down(&mut self) {
        self.cursor_key.shift_clear();
        self.stack.set_last_ikey(0);
    }

    /// Handle ascent from exhausted sublayer (Up state).
    ///
    /// Corresponds to `find::handle_up`.
    ///
    /// Returns `true` if there's a parent layer, `false` if scan is complete.
    pub fn handle_up(&mut self, _guard: &LocalGuard<'_>) -> bool {
        let Some(parent) = self.layer_stack.pop() else {
            return false;
        };

        self.stack.set_root(parent.root);
        self.stack.set_leaf(parent.leaf_ptr());

        self.cursor_key.unshift();

        // SAFETY: parent.leaf is protected by guard
        let leaf: &LeafNode15<P> = unsafe { parent.leaf.as_ref() };

        let version: u32 = leaf.version().stable();
        let perm: <LeafNode15<P> as TreeLeafNode<P>>::Perm = leaf.permutation();

        let pos: usize = lower_with_suffix(&self.cursor_key, leaf, &perm, self.flags.emit_equal());
        self.stack.update_state(version, perm, pos);

        self.stack.set_last_ikey(0);

        true
    }

    /// Handle Down/Up/Retry transitions in a single method.
    ///
    /// Centralizes the state-machine transition logic shared across all five
    /// forward scan entry points (`run_batch`, `advance_no_alloc`,
    /// `advance_no_alloc_ref`, `RangeIter::advance`, `for_each_batch_ref`).
    #[inline(always)]
    pub(crate) fn step_transitions(&mut self, guard: &LocalGuard<'_>) -> StepResult {
        match self.state {
            ScanState::Down => {
                self.flags.disable_single_layer_mode();
                self.handle_down();
                self.state = ScanState::Retry;
                self.flags.require_duplicate_check();
                StepResult::Continue
            }
            ScanState::Up => {
                if !self.handle_up(guard) {
                    self.flags.mark_exhausted();
                    return StepResult::Exhausted;
                }
                self.state = ScanState::FindNext;
                self.flags.require_duplicate_check();
                StepResult::Continue
            }
            ScanState::Retry => {
                self.state = self.find_retry(guard);
                self.flags.require_duplicate_check();
                StepResult::Continue
            }
            ScanState::Emit | ScanState::FindNext => StepResult::Ready,
        }
    }
}

// ============================================================================
//  Intra-leaf batch processing
// ============================================================================

impl<P: LeafPolicy> ForwardScanCtx<P> {
    /// Process remaining entries in current leaf, returning `&P::Value` references.
    #[inline]
    pub fn process_leaf_batch_ptr<F>(
        &mut self,
        end_bound: &RangeBound<'_>,
        end_bound_ikey: Option<u64>,
        visitor: &mut F,
        count: &mut usize,
    ) -> LeafBatchResult
    where
        F: FnMut(&[u8], &P::Value) -> bool,
    {
        let (result, ki, root) = {
            let mut ctx = self.build_batch_ctx();
            let r = process_batch_keyed::<Forward, _, P>(
                &mut ctx,
                end_bound,
                end_bound_ikey,
                &mut RefSlotVisitor(visitor),
                count,
                &mut (),
            );
            (r, ctx.ki, ctx.root)
        };
        self.stack.set_ki(ki.cast_unsigned());
        self.stack.set_root(root);
        result
    }

    /// Process remaining entries in current leaf, returning `P::Output` by value.
    #[inline]
    pub fn process_leaf_batch<F>(
        &mut self,
        end_bound: &RangeBound<'_>,
        end_bound_ikey: Option<u64>,
        visitor: &mut F,
        count: &mut usize,
    ) -> LeafBatchResult
    where
        F: FnMut(&[u8], P::Output) -> bool,
    {
        let (result, ki, root) = {
            let mut ctx = self.build_batch_ctx();
            let r = process_batch_keyed::<Forward, _, P>(
                &mut ctx,
                end_bound,
                end_bound_ikey,
                &mut CopySlotVisitor(visitor),
                count,
                &mut (),
            );
            (r, ctx.ki, ctx.root)
        };
        self.stack.set_ki(ki.cast_unsigned());
        self.stack.set_root(root);
        result
    }

    /// Process leaf batch without key materialization (values only).
    #[inline]
    pub fn process_leaf_batch_values(
        &mut self,
        end_bound_ikey: Option<u64>,
        visitor: &mut impl FnMut(P::Output) -> bool,
        count: &mut usize,
    ) -> LeafBatchResult {
        let (result, ki, root) = {
            let mut ctx = self.build_batch_ctx();
            let r = process_batch_values::<Forward, P>(
                &mut ctx,
                end_bound_ikey,
                visitor,
                count,
                &mut (),
            );
            (r, ctx.ki, ctx.root)
        };
        self.stack.set_ki(ki.cast_unsigned());
        self.stack.set_root(root);
        result
    }

    /// Build a [`BatchCtx`] from the forward scan stack.
    #[inline(always)]
    fn build_batch_ctx(&mut self) -> BatchCtx<'_, P> {
        let leaf_ptr: *mut LeafNode15<P> = self.stack.leaf_ptr();
        // SAFETY: leaf_ptr is valid - protected by guard in caller
        let leaf: &LeafNode15<P> = unsafe { &*leaf_ptr };
        let perm = self.stack.perm();
        let perm_size = perm.size();
        BatchCtx {
            leaf,
            perm,
            perm_size,
            cached_version: self.stack.version(),
            ki: self.stack.ki().cast_signed(),
            root: self.stack.root(),
            leaf_ptr,
            cursor_key: &mut self.cursor_key,
            layer_stack: &mut self.layer_stack,
        }
    }
}

// ============================================================================
//  Batch strategy trait + run_batch unified loop
// ============================================================================

/// Result of a strategy's `emit_initial` or `dup_check_entry` call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchAction {
    /// Entry emitted (or skipped), continue scanning.
    Continue,
    /// Visitor returned false — stop scanning.
    Stopped,
    /// End bound exceeded — mark exhausted and stop.
    Exhausted,
}

/// Strategy trait for forward batch scanning.
///
/// Defines how to emit the initial entry, handle the dup-check slow path,
/// and process leaf batches. Three implementations:
///
/// - [`IntraLeafRefStrategy`]: `&P::Value` refs via `process_leaf_batch_ptr`
/// - [`IntraLeafCopyStrategy`]: `P::Output` by value via `process_leaf_batch`
/// - [`ValuesOnlyStrategy`]: `P::Output` values only via `process_leaf_batch_values`
///
/// After monomorphization, all trait dispatch is fully inlined — zero overhead.
pub trait ForwardBatchStrategy<P: LeafPolicy> {
    /// Emit the initial entry from `initialize()`'s snapshot.
    fn emit_initial(
        &mut self,
        ctx: &mut ForwardScanCtx<P>,
        end_bound: &RangeBound<'_>,
        count: &mut usize,
    ) -> BatchAction;

    /// Handle one entry via the dup-check slow path after retry/layer transitions.
    fn dup_check_entry(
        &mut self,
        ctx: &mut ForwardScanCtx<P>,
        end_bound: &RangeBound<'_>,
        guard: &LocalGuard<'_>,
        count: &mut usize,
    ) -> BatchAction;

    /// Process the current leaf's entries (hot path).
    fn process_leaf(
        &mut self,
        ctx: &mut ForwardScanCtx<P>,
        end_bound: &RangeBound<'_>,
        guard: &LocalGuard<'_>,
        count: &mut usize,
    ) -> LeafBatchResult;
}

impl<P: LeafPolicy> ForwardScanCtx<P> {
    /// Unified batch scan loop driven by a strategy.
    ///
    /// Handles the state machine skeleton (Down/Up/Retry/null/deleted checks)
    /// and delegates entry emission, duplicate checking, and leaf processing
    /// to the strategy.
    ///
    /// Callers must handle exhausted/initialized checks before calling this.
    #[inline]
    pub(crate) fn run_batch<S: ForwardBatchStrategy<P>>(
        &mut self,
        strategy: &mut S,
        end_bound: &RangeBound<'_>,
        guard: &LocalGuard<'_>,
    ) -> usize {
        let mut count: usize = 0;

        // Handle initial Emit state from initialize() if present
        if self.state == ScanState::Emit {
            match strategy.emit_initial(self, end_bound, &mut count) {
                BatchAction::Continue => {
                    self.state = ScanState::FindNext;
                }

                BatchAction::Stopped => return count,

                BatchAction::Exhausted => {
                    self.flags.mark_exhausted();
                    return count;
                }
            }
        }

        loop {
            // Handle rare states (layer transitions, retries)
            match self.step_transitions(guard) {
                StepResult::Exhausted => return count,
                StepResult::Continue => continue,
                StepResult::Ready => {}
            }

            // Check for null stack (layer exhausted)
            if self.stack.is_null() {
                if self.layer_stack.is_empty() {
                    self.flags.mark_exhausted();
                    return count;
                }
                self.state = ScanState::Up;
                continue;
            }

            // Check leaf deletion
            // SAFETY: stack.is_null() check above ensures leaf_ptr is valid,
            // and the guard protects the node from deallocation.
            let leaf: &LeafNode15<P> = unsafe { self.stack.leaf_ref() };
            if leaf.version().is_deleted() {
                self.state = ScanState::Retry;
                continue;
            }

            // Dup-check slow path (after retry/layer transitions)
            if self.flags.needs_duplicate_check() {
                self.flags.clear_duplicate_check();

                match strategy.dup_check_entry(self, end_bound, guard, &mut count) {
                    BatchAction::Continue => continue,

                    BatchAction::Stopped => return count,

                    BatchAction::Exhausted => {
                        self.flags.mark_exhausted();
                        return count;
                    }
                }
            }

            // Hot path: process leaf batch
            let result = strategy.process_leaf(self, end_bound, guard, &mut count);

            match result {
                LeafBatchResult::LeafExhausted => {
                    let (state, _) = self.advance_leaf(guard);
                    self.state = state;
                }

                LeafBatchResult::LayerEncountered => {
                    self.state = ScanState::Down;
                }

                LeafBatchResult::VersionChanged => {
                    self.state = ScanState::Retry;
                }

                LeafBatchResult::Stopped => return count,

                LeafBatchResult::EndBoundExceeded => {
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

/// Intra-leaf batch strategy with `&P::Value` references.
///
/// Used by [`RangeIter::for_each_intra_leaf_batch_ref`].
pub struct IntraLeafRefStrategy<'a, F> {
    visitor: &'a mut F,
    end_bound_ikey: Option<u64>,
}

impl<'a, F> IntraLeafRefStrategy<'a, F> {
    #[inline]
    pub const fn new(visitor: &'a mut F, end_bound_ikey: Option<u64>) -> Self {
        Self {
            visitor,
            end_bound_ikey,
        }
    }
}

impl<P, F> ForwardBatchStrategy<P> for IntraLeafRefStrategy<'_, F>
where
    P: RefLeafPolicy,
    F: FnMut(&[u8], &P::Value) -> bool,
{
    #[inline(always)]
    fn emit_initial(
        &mut self,
        ctx: &mut ForwardScanCtx<P>,
        end_bound: &RangeBound<'_>,
        count: &mut usize,
    ) -> BatchAction {
        let Some(snapshot) = ctx.snapshot.take() else {
            return BatchAction::Continue;
        };
        // SAFETY: CursorKey invariant guarantees offset + len <= MAX_KEY_LENGTH
        let key: &[u8] = unsafe { ctx.cursor_key.full_key_unchecked() };

        if !end_bound.contains(key) {
            return BatchAction::Exhausted;
        }

        let value_ref: &P::Value =
            unsafe { P::output_as_ref_sound(&snapshot.value, &mut ctx.scratch_value) };
        *count += 1;

        if (self.visitor)(key, value_ref) {
            BatchAction::Continue
        } else {
            BatchAction::Stopped
        }
    }

    #[inline(always)]
    fn dup_check_entry(
        &mut self,
        ctx: &mut ForwardScanCtx<P>,
        end_bound: &RangeBound<'_>,
        guard: &LocalGuard<'_>,
        count: &mut usize,
    ) -> BatchAction {
        let (new_state, snapshot_ptr) = ctx.find_next_with_dup_check_ptr(guard);
        ctx.state = new_state;

        if new_state == ScanState::Emit
            && let Some(snap) = snapshot_ptr
        {
            // SAFETY: CursorKey invariant guarantees offset + len <= MAX_KEY_LENGTH
            let key: &[u8] = unsafe { ctx.cursor_key.full_key_unchecked() };

            if !end_bound.contains(key) {
                return BatchAction::Exhausted;
            }

            let value_ref: &P::Value =
                unsafe { snap.resolve_value_ref::<P>(&mut ctx.scratch_value) };
            *count += 1;
            ctx.state = ScanState::FindNext;

            if !(self.visitor)(key, value_ref) {
                return BatchAction::Stopped;
            }
        }

        BatchAction::Continue
    }

    #[inline(always)]
    fn process_leaf(
        &mut self,
        ctx: &mut ForwardScanCtx<P>,
        end_bound: &RangeBound<'_>,
        _guard: &LocalGuard<'_>,
        count: &mut usize,
    ) -> LeafBatchResult {
        ctx.process_leaf_batch_ptr(end_bound, self.end_bound_ikey, self.visitor, count)
    }
}

/// Intra-leaf batch strategy with `P::Output` by value.
///
/// Used by [`RangeIter::for_each_intra_leaf_batch`].
pub struct IntraLeafCopyStrategy<'a, F> {
    visitor: &'a mut F,
    end_bound_ikey: Option<u64>,
}

impl<'a, F> IntraLeafCopyStrategy<'a, F> {
    #[inline]
    pub const fn new(visitor: &'a mut F, end_bound_ikey: Option<u64>) -> Self {
        Self {
            visitor,
            end_bound_ikey,
        }
    }
}

impl<P, F> ForwardBatchStrategy<P> for IntraLeafCopyStrategy<'_, F>
where
    P: LeafPolicy,
    F: FnMut(&[u8], P::Output) -> bool,
{
    #[inline(always)]
    fn emit_initial(
        &mut self,
        ctx: &mut ForwardScanCtx<P>,
        end_bound: &RangeBound<'_>,
        count: &mut usize,
    ) -> BatchAction {
        let Some(snapshot) = ctx.snapshot.take() else {
            return BatchAction::Continue;
        };

        // SAFETY: CursorKey invariant guarantees offset + len <= MAX_KEY_LENGTH
        let key: &[u8] = unsafe { ctx.cursor_key.full_key_unchecked() };

        if !end_bound.contains(key) {
            return BatchAction::Exhausted;
        }

        *count += 1;

        if (self.visitor)(key, snapshot.value) {
            BatchAction::Continue
        } else {
            BatchAction::Stopped
        }
    }

    #[inline(always)]
    fn dup_check_entry(
        &mut self,
        ctx: &mut ForwardScanCtx<P>,
        end_bound: &RangeBound<'_>,
        guard: &LocalGuard<'_>,
        count: &mut usize,
    ) -> BatchAction {
        let (new_state, snapshot) = ctx.find_next_with_dup_check(guard);
        ctx.state = new_state;

        if new_state == ScanState::Emit
            && let Some(snap) = snapshot
        {
            // SAFETY: CursorKey invariant guarantees offset + len <= MAX_KEY_LENGTH
            let key: &[u8] = unsafe { ctx.cursor_key.full_key_unchecked() };

            if !end_bound.contains(key) {
                return BatchAction::Exhausted;
            }

            *count += 1;
            ctx.state = ScanState::FindNext;

            if !(self.visitor)(key, snap.value) {
                return BatchAction::Stopped;
            }
        }

        BatchAction::Continue
    }

    #[inline(always)]
    fn process_leaf(
        &mut self,
        ctx: &mut ForwardScanCtx<P>,
        end_bound: &RangeBound<'_>,
        _guard: &LocalGuard<'_>,
        count: &mut usize,
    ) -> LeafBatchResult {
        ctx.process_leaf_batch(end_bound, self.end_bound_ikey, self.visitor, count)
    }
}

/// Values-only batch strategy — no key materialization.
///
/// Used by [`RangeIter::for_each_values_batch`].
pub struct ValuesOnlyStrategy<'a, F> {
    visitor: &'a mut F,
}

impl<'a, F> ValuesOnlyStrategy<'a, F> {
    #[inline]
    pub const fn new(visitor: &'a mut F) -> Self {
        Self { visitor }
    }
}

impl<P, F> ForwardBatchStrategy<P> for ValuesOnlyStrategy<'_, F>
where
    P: LeafPolicy,
    F: FnMut(P::Output) -> bool,
{
    #[inline(always)]
    fn emit_initial(
        &mut self,
        ctx: &mut ForwardScanCtx<P>,
        _end_bound: &RangeBound<'_>,
        count: &mut usize,
    ) -> BatchAction {
        let Some(snapshot) = ctx.snapshot.take() else {
            return BatchAction::Continue;
        };

        // Values-only: approximate ikey-based end bound checking only
        *count += 1;

        if (self.visitor)(snapshot.value) {
            BatchAction::Continue
        } else {
            BatchAction::Stopped
        }
    }

    #[inline(always)]
    fn dup_check_entry(
        &mut self,
        ctx: &mut ForwardScanCtx<P>,
        _end_bound: &RangeBound<'_>,
        guard: &LocalGuard<'_>,
        count: &mut usize,
    ) -> BatchAction {
        let (new_state, snapshot) = ctx.find_next_with_dup_check(guard);
        ctx.state = new_state;

        if new_state == ScanState::Emit
            && let Some(snap) = snapshot
        {
            // Values-only: skip end bound key check
            *count += 1;

            ctx.state = ScanState::FindNext;
            if !(self.visitor)(snap.value) {
                return BatchAction::Stopped;
            }
        }

        BatchAction::Continue
    }

    #[inline(always)]
    fn process_leaf(
        &mut self,
        ctx: &mut ForwardScanCtx<P>,
        end_bound: &RangeBound<'_>,
        _guard: &LocalGuard<'_>,
        count: &mut usize,
    ) -> LeafBatchResult {
        // Layer-aware ikey extraction: align end-bound ikey with the current
        // trie depth so descended scans compare sublayer ikeys correctly.
        let end_bound_ikey: Option<u64> = end_bound.extract_ikey_at(ctx.cursor_key.offset());

        ctx.process_leaf_batch_values(end_bound_ikey, self.visitor, count)
    }
}

// ============================================================================
//  Per-entry advance methods (fast paths for for_each / Iterator::next)
// ============================================================================

impl<P: LeafPolicy> ForwardScanCtx<P> {
    /// Advance without allocating key Vec (clone path).
    ///
    /// Returns `(&[u8], P::Output)` where the key slice is borrowed from
    /// the internal `cursor_key` buffer.
    ///
    /// This function inlines the common case `(FindNext → Emit)` to avoid
    /// state machine dispatch overhead.
    #[inline(always)]
    pub fn advance_no_alloc(
        &mut self,
        end_bound: &RangeBound<'_>,
        guard: &LocalGuard<'_>,
    ) -> Option<(&[u8], P::Output)>
    where
        P::Output: Clone,
    {
        // Fast path: if we have a pending emit, process it first
        if self.state == ScanState::Emit
            && let Some(snapshot) = self.snapshot.take()
        {
            #[cfg(debug_assertions)]
            {
                let key_copy = self.cursor_key.full_key().to_vec();
                self.assert_ordering(&key_copy);
            }

            // SAFETY: CursorKey invariant guarantees offset + len <= MAX_KEY_LENGTH
            let key: &[u8] = unsafe { self.cursor_key.full_key_unchecked() };

            if !end_bound.contains(key) {
                self.flags.mark_exhausted();
                return None;
            }

            self.state = ScanState::FindNext;

            return Some((key, snapshot.value));
        }

        loop {
            #[cfg(debug_assertions)]
            let (pre_state, pre_cursor) = (self.state, self.cursor_key.debug_state());

            match self.step_transitions(guard) {
                StepResult::Exhausted => {
                    #[cfg(debug_assertions)]
                    self.record_transition(format!("Up -> Exhausted: pre={pre_cursor}"));
                    return None;
                }
                StepResult::Continue => {
                    #[cfg(debug_assertions)]
                    self.record_transition(format!(
                        "{pre_state:?} -> {:?}: pre={pre_cursor}, post={}",
                        self.state,
                        self.cursor_key.debug_state()
                    ));
                    continue;
                }
                StepResult::Ready => {}
            }

            // Main hot path: FindNext
            let (new_state, snapshot) = if self.flags.needs_duplicate_check() {
                self.flags.clear_duplicate_check();
                self.find_next_with_dup_check(guard)
            } else {
                self.find_next(guard)
            };

            self.state = new_state;

            // Fast path: if Emit, return immediately
            if new_state == ScanState::Emit
                && let Some(snap) = snapshot
            {
                #[cfg(debug_assertions)]
                {
                    let key_copy = self.cursor_key.full_key().to_vec();
                    self.assert_ordering(&key_copy);
                }

                // SAFETY: CursorKey invariant guarantees offset + len <= MAX_KEY_LENGTH
                let key = unsafe { self.cursor_key.full_key_unchecked() };

                if !end_bound.contains(key) {
                    self.flags.mark_exhausted();
                    return None;
                }

                self.state = ScanState::FindNext;
                return Some((key, snap.value));
            }

            self.snapshot = snapshot;
        }
    }

    /// Advance without cloning values (zero-copy path).
    ///
    /// Returns `(&[u8], &P::Value)` where both are borrowed references.
    #[inline(always)]
    #[expect(clippy::too_many_lines, reason = "Complex allocation logic")]
    pub fn advance_no_alloc_ref<'s>(
        &'s mut self,
        end_bound: &RangeBound<'_>,
        guard: &LocalGuard<'_>,
    ) -> Option<(&'s [u8], &'s P::Value)>
    where
        P: RefLeafPolicy,
    {
        // Handle pending emit from initialize() - first entry case
        if self.state == ScanState::Emit && self.snapshot.is_some() {
            // SAFETY: CursorKey invariant guarantees offset + len <= MAX_KEY_LENGTH
            let key = unsafe { self.cursor_key.full_key_unchecked() };

            if !end_bound.contains(key) {
                self.flags.mark_exhausted();
                return None;
            }

            let snapshot: ScanSnapshot<P> = self.snapshot.take()?;

            self.state = ScanState::FindNext;
            self.last_output = Some(snapshot.value);

            // SAFETY: Guard protects the output from retirement.
            // output_as_ref_sound uses atomic read for write-through types.
            let value_ref: &P::Value = unsafe {
                P::output_as_ref_sound(self.last_output.as_ref().unwrap(), &mut self.scratch_value)
            };

            return Some((key, value_ref));
        }

        loop {
            // ================================================================
            // Single-layer fast path (keys ≤ 8 bytes)
            // ================================================================
            if self.flags.single_layer_mode() {
                if self.state == ScanState::Retry {
                    self.state = self.find_retry(guard);
                    self.flags.require_duplicate_check();
                    continue;
                }

                let (new_state, snapshot_ptr) =
                    self.find_next_single_layer_ptr(guard, self.flags.needs_duplicate_check());

                if self.flags.needs_duplicate_check() {
                    self.flags.clear_duplicate_check();
                }

                self.state = new_state;

                match new_state {
                    ScanState::Emit => {
                        if let Some(snap) = snapshot_ptr {
                            // SAFETY: CursorKey invariant guarantees offset + len <= MAX_KEY_LENGTH
                            let key = unsafe { self.cursor_key.full_key_unchecked() };

                            if !end_bound.contains(key) {
                                self.flags.mark_exhausted();
                                return None;
                            }

                            self.state = ScanState::FindNext;
                            // SAFETY: Version validated, guard held, snap pointer valid.
                            let value_ref: &P::Value =
                                unsafe { snap.resolve_value_ref::<P>(&mut self.scratch_value) };

                            return Some((key, value_ref));
                        }
                    }

                    ScanState::FindNext => {
                        if self.stack.is_null() {
                            self.flags.mark_exhausted();
                            return None;
                        }

                        continue;
                    }

                    ScanState::Retry => continue,

                    ScanState::Down => {
                        // Encountered layer pointer - fall back to multi-layer
                        self.flags.disable_single_layer_mode();

                        self.layer_stack
                            .push(LayerContext::new(self.stack.root(), self.stack.leaf_ptr()));

                        let Some(slot) = self.stack.kp() else {
                            debug_assert!(
                                false,
                                "Down state entered without valid slot - state machine bug"
                            );

                            self.state = ScanState::Retry;
                            continue;
                        };

                        // SAFETY: find_next_single_layer_ptr validated the leaf version,
                        // and the guard protects the node from deallocation.
                        let leaf: &LeafNode15<P> = unsafe { self.stack.leaf_ref() };
                        let layer_ptr: *mut u8 = leaf.load_layer_raw(slot);
                        self.stack.set_root(layer_ptr);

                        // Fall through to handle Down below
                    }

                    ScanState::Up => {
                        self.flags.mark_exhausted();

                        return None;
                    }
                }
            }

            // ================================================================
            // Multi-layer path (handles Down/Up transitions)
            // ================================================================

            match self.step_transitions(guard) {
                StepResult::Exhausted => return None,
                StepResult::Continue => continue,
                StepResult::Ready => {}
            }

            let (new_state, snapshot_ptr) = if self.flags.needs_duplicate_check() {
                self.flags.clear_duplicate_check();
                self.find_next_with_dup_check_ptr(guard)
            } else {
                self.find_next_ptr(guard)
            };

            self.state = new_state;

            if new_state == ScanState::Emit
                && let Some(snap) = snapshot_ptr
            {
                // SAFETY: CursorKey invariant guarantees offset + len <= MAX_KEY_LENGTH
                let key = unsafe { self.cursor_key.full_key_unchecked() };

                if !end_bound.contains(key) {
                    self.flags.mark_exhausted();

                    return None;
                }

                self.state = ScanState::FindNext;
                // SAFETY: Version validated, guard held, snap pointer valid.
                let value_ref: &P::Value =
                    unsafe { snap.resolve_value_ref::<P>(&mut self.scratch_value) };

                return Some((key, value_ref));
            }
        }
    }
}
