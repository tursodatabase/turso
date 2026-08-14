//! - `#[inline(always)]` on hot path helpers
//! - `#[cold]` on retry/error paths
//! - Unified slot allocation and value update logic
//! - Strategy-based generic/write-through unification (see `InsertStrategy`)

use crate::Permuter;
use crate::leaf_trait::{SplitInsertData, TreeLeafNode};
use crate::leaf15::KSUF_KEYLENX;
use crate::policy::RetireHandle;

use super::insert_strategy::{GenericInsert, InsertStrategy, WriteThroughInsert};
use super::{
    FindSlotResult, InsertError, InsertSearchResultGeneric, Key, LAYER_KEYLENX, LeafPolicy, Linker,
    LocalGuard, MassTreeGeneric, MembershipError, TreeAllocator, TreePermutation,
};

use crate::leaf15::LeafNode15;
use crate::nodeversion::LockGuard;

/// Retire a deferred suffix bag pointer if non-null.
///
/// # Safety
///
/// `deferred_retire` must be null or a valid suffix bag pointer from `assign_ksuf`.
/// `guard` must be from this tree's collector.
#[inline(always)]
unsafe fn maybe_retire_suffix<P: LeafPolicy>(deferred_retire: *mut u8, guard: &LocalGuard<'_>) {
    if !deferred_retire.is_null() {
        // SAFETY: deferred_retire came from assign_ksuf and guard protects reclamation.
        unsafe {
            LeafNode15::<P>::retire_suffix_bag_ptr(deferred_retire, guard);
        }
    }
}

// ============================================================================
//  Hot Path Helpers
// ============================================================================

impl<P, A> MassTreeGeneric<P, A>
where
    P: LeafPolicy,
    A: TreeAllocator<P>,
{
    /// Find a usable slot for inserting a new key.
    ///
    /// Handles the slot-0 rule: slot-0 stores `ikey_bound()` and cannot be
    /// reused for a different ikey.
    #[inline(always)]
    #[expect(clippy::unused_self, reason = "API consistency with other methods")]
    pub(crate) fn find_usable_slot(
        &self,
        leaf: &LeafNode15<P>,
        perm: &<LeafNode15<P> as TreeLeafNode<P>>::Perm,
        ikey: u64,
    ) -> FindSlotResult {
        // Check if leaf has space
        if perm.size() >= LeafNode15::<P>::WIDTH {
            return FindSlotResult::NeedsSplit;
        }

        // Get next free slot from back
        let slot: usize = perm.back();

        // Handle slot-0 rule: can't reuse slot-0 for different ikey
        if slot == 0 && !leaf.can_reuse_slot0(ikey) {
            let free_count = LeafNode15::<P>::WIDTH - perm.size();

            for offset in 1..free_count {
                let candidate: usize = perm.back_at_offset(offset);

                if candidate != 0 {
                    return FindSlotResult::Found {
                        slot: candidate,
                        back_offset: offset,
                    };
                }
            }

            // Only slot-0 available - trigger split
            return FindSlotResult::NeedsSplit;
        }

        let back_offset: usize = 0;
        FindSlotResult::Found { slot, back_offset }
    }

    /// Update an existing value in a slot.
    ///
    /// Returns the old value that was replaced.
    ///
    /// # Safety
    ///
    /// Caller must hold the lock on `leaf`.
    #[inline(always)]
    #[expect(clippy::unused_self, reason = "API consistency with other methods")]
    pub(crate) fn update_existing_value(
        &self,
        leaf: &LeafNode15<P>,
        lock: &mut LockGuard<'_>,
        slot: usize,
        new_value: &P::Output,
        guard: &LocalGuard<'_>,
    ) -> P::Output {
        if P::NEEDS_RETIREMENT {
            lock.mark_insert();

            let retire: RetireHandle = leaf.update_value_in_place_relaxed(slot, new_value);

            let old_output: P::Output = match retire {
                // SAFETY: ptr was just returned by update_value_in_place_relaxed and
                // points to the previously stored value. Lock provides synchronization.
                RetireHandle::Ptr(ptr) => unsafe { P::output_from_retire_ptr(ptr) },
                RetireHandle::Noop => unreachable!("NEEDS_RETIREMENT implies Ptr retire handle"),
            };

            // SAFETY: handle was produced by update_in_place_relaxed on this leaf.
            unsafe { P::retire_handle(retire, guard) };

            old_output
        } else {
            let old_output: P::Output = leaf
                .load_value_relaxed(slot)
                .expect("slot should have value in Found path");

            lock.mark_insert();

            let retire: RetireHandle = leaf.update_value_in_place_relaxed(slot, new_value);
            debug_assert_eq!(retire, RetireHandle::Noop);

            old_output
        }
    }

    /// Write-through update: modify existing value in place without allocation.
    ///
    /// Returns the old value by copy. No Box allocation or EBR retirement needed.
    /// Only callable when `P::CAN_WRITE_THROUGH` is true.
    ///
    /// # Safety
    ///
    /// Caller must hold the lock on `leaf`.
    #[inline(always)]
    #[expect(clippy::unused_self, reason = "API consistency with other methods")]
    pub(crate) fn update_existing_value_write_through(
        &self,
        leaf: &LeafNode15<P>,
        lock: &mut LockGuard<'_>,
        slot: usize,
        new_value: &P::Value,
    ) -> P::Value {
        debug_assert!(
            P::CAN_WRITE_THROUGH,
            "write-through update called but CAN_WRITE_THROUGH is false"
        );

        lock.mark_insert();

        // SAFETY: Caller holds the lock. Slot contains a terminal value.
        // CAN_WRITE_THROUGH guarantees size_of::<V>() <= 8.
        unsafe { leaf.write_through_update_value(slot, new_value) }
    }

    /// Insert a new value into a slot and update the permutation.
    ///
    /// # Safety
    ///
    /// Caller must hold the lock on `leaf` and have verified slot availability.
    #[inline(always)]
    #[expect(
        clippy::too_many_arguments,
        reason = "Slot assignment requires full context"
    )]
    pub(crate) fn insert_new_value(
        &self,
        leaf: &LeafNode15<P>,
        lock: &mut LockGuard<'_>,
        slot: usize,
        back_offset: usize,
        logical_pos: usize,
        mut perm: <LeafNode15<P> as TreeLeafNode<P>>::Perm,
        key: &Key<'_>,
        value: &P::Output,
        guard: &LocalGuard<'_>,
        pre_allocated: Option<Vec<u8>>,
    ) -> *mut u8 {
        let deferred_retire: *mut u8 =
            self.assign_slot_generic(leaf, lock, slot, key, value, guard, pre_allocated);

        if back_offset > 0 {
            let back_pos: usize = LeafNode15::<P>::WIDTH - 1;
            let chosen_pos: usize = back_pos - back_offset;
            perm.swap_free_slots(back_pos, chosen_pos);
        }

        let allocated: usize = perm.insert_from_back(logical_pos);
        debug_assert_eq!(allocated, slot, "allocated unexpected slot");

        leaf.set_permutation_relaxed(perm);

        deferred_retire
    }

    /// Pre-allocate a suffix vec when the heuristic predicts external storage.
    ///
    /// Returns `Some(Vec)` if the suffix is too large for inline storage or
    /// the leaf is filling up enough that inline overflow is likely.
    #[inline(always)]
    pub(super) fn maybe_pre_allocate_suffix(key: &Key<'_>, perm_size: usize) -> Option<Vec<u8>> {
        let suffix_len: usize = key.suffix().len();
        let inline_capacity: usize = LeafNode15::<P>::INLINE_KSUF_CAPACITY;

        let threshold_exceeded: bool =
            suffix_len > inline_capacity || perm_size * suffix_len >= inline_capacity;

        if threshold_exceeded {
            let estimated_capacity: usize = LeafNode15::<P>::WIDTH * suffix_len;
            let mut v: Vec<u8> = Vec::new();

            if v.try_reserve(estimated_capacity).is_ok() {
                return Some(v);
            }
        }

        None
    }

    // ========================================================================
    //  Empty Leaf Reuse (Lazy Coalescing Optimization)
    // ========================================================================

    /// Check if an empty leaf can be reused for the given key.
    #[inline(always)]
    #[expect(clippy::unused_self, reason = "API consistency with other methods")]
    pub(crate) fn can_reuse_empty_leaf(&self, leaf: &LeafNode15<P>, key: &Key<'_>) -> bool {
        // SAFETY: Called under lock - no concurrent retirement.
        if unsafe { leaf.prev_unguarded() }.is_null() {
            return true;
        }

        leaf.ikey_bound() == key.ikey()
    }

    /// Insert a value into an empty leaf, reusing it instead of allocating.
    #[inline]
    #[expect(
        clippy::type_complexity,
        reason = "Returns result + deferred retire pointer"
    )]
    #[expect(clippy::too_many_arguments, reason = "Internals")]
    fn insert_into_empty_leaf(
        &self,
        leaf: &LeafNode15<P>,
        lock: &mut LockGuard<'_>,
        key: &Key<'_>,
        value: &P::Output,
        guard: &LocalGuard<'_>,
        pre_allocated: Option<Vec<u8>>,
    ) -> (Result<Option<P::Output>, InsertError>, *mut u8) {
        leaf.clear_empty_state();

        let slot: usize = 0;

        let deferred_retire: *mut u8 =
            self.assign_slot_generic(leaf, lock, slot, key, value, guard, pre_allocated);

        let new_perm: Permuter = <LeafNode15<P> as TreeLeafNode<P>>::Perm::make_sorted(1);
        leaf.set_permutation_relaxed(new_perm);

        (Ok(None), deferred_retire)
    }
}

// ============================================================================
//  Write-Through Insert (Value Path) - delegates to unified insert_concurrent
// ============================================================================

impl<P, A> MassTreeGeneric<P, A>
where
    P: LeafPolicy,
    A: TreeAllocator<P>,
{
    /// Insert with deferred allocation and write-through updates.
    ///
    /// Delegates to `insert_concurrent` with `WriteThroughInsert` strategy.
    /// Only called when `P::CAN_WRITE_THROUGH` is true.
    #[inline(always)]
    pub(super) fn insert_concurrent_value(
        &self,
        key: &mut Key<'_>,
        value: P::Value,
        guard: &LocalGuard<'_>,
    ) -> Result<Option<P::Value>, InsertError> {
        self.insert_concurrent::<WriteThroughInsert>(key, value, guard)
    }
}

// ============================================================================
//  Cold Path Helpers (Validation / Retry)
// ============================================================================

impl<P, A> MassTreeGeneric<P, A>
where
    P: LeafPolicy,
    A: TreeAllocator<P>,
{
    /// Validate post-lock state: check if version or permutation changed.
    #[inline(always)]
    #[expect(clippy::unused_self, reason = "API consistency with other methods")]
    pub(crate) fn validate_post_lock(
        &self,
        leaf: &LeafNode15<P>,
        pre_lock_version: u32,
        pre_lock_perm_raw: <<LeafNode15<P> as TreeLeafNode<P>>::Perm as TreePermutation>::Raw,
    ) -> bool {
        !leaf.version().has_changed(pre_lock_version) && leaf.permutation_raw() == pre_lock_perm_raw
    }

    /// Validate membership: check if key should be in this leaf or a sibling.
    #[inline]
    #[expect(clippy::unused_self, reason = "API consistency with other methods")]
    pub(crate) fn validate_membership(
        &self,
        leaf: &LeafNode15<P>,
        key: &Key<'_>,
    ) -> Result<(), MembershipError> {
        // SAFETY: Called under lock - no concurrent retirement.
        let next_raw: *mut LeafNode15<P> = unsafe { leaf.next_raw_unguarded() };

        if Linker::is_marked(next_raw) {
            leaf.wait_for_split();
            return Err(MembershipError::SplitInProgress);
        }

        // SAFETY: Called under lock - no concurrent retirement.
        if !unsafe { leaf.prev_unguarded() }.is_null() {
            let lower_bound: u64 = leaf.ikey_bound();

            if key.ikey() < lower_bound {
                return Err(MembershipError::KeyBelowLowerBound);
            }
        }

        let next_ptr: *mut LeafNode15<P> = Linker::unmark_ptr(next_raw);

        if !next_ptr.is_null() {
            // SAFETY: next_ptr is a valid leaf pointer (protected by the guard).
            let next_bound: u64 = unsafe { (*next_ptr).ikey_bound() };

            if key.ikey() >= next_bound {
                return Err(MembershipError::KeyMovedToSibling);
            }
        }

        Ok(())
    }

    #[cold]
    #[inline(never)]
    #[expect(
        clippy::too_many_arguments,
        reason = "Layer creation requires full context"
    )]
    fn handle_suffix_conflict(
        &self,
        leaf: &LeafNode15<P>,
        lock: &mut LockGuard<'_>,
        slot: usize,
        key: &mut Key<'_>,
        value: P::Output,
        guard: &LocalGuard<'_>,
    ) {
        lock.mark_insert();

        // SAFETY: We hold the lock on `leaf`, guard is from this tree's collector
        let layer_ptr: *mut u8 =
            unsafe { self.create_layer_concurrent_generic(leaf, slot, key, value, guard) };

        // SAFETY: We hold the lock. Slot transitions from terminal → layer.
        let retire: RetireHandle = leaf.take_value_for_layer(slot);

        // SAFETY: handle was produced by take_value_for_layer() on this leaf.
        unsafe { P::retire_handle(retire, guard) };

        // SAFETY: We hold the lock
        unsafe {
            leaf.clear_ksuf(slot, guard);
        };

        leaf.set_keylenx(slot, LAYER_KEYLENX);
        leaf.store_layer(slot, layer_ptr);
    }
}

// ============================================================================
//  Main Insert Implementation - delegates to unified insert_concurrent
// ============================================================================

impl<P, A> MassTreeGeneric<P, A>
where
    P: LeafPolicy,
    A: TreeAllocator<P>,
{
    /// Internal concurrent insert with pre-allocated output.
    ///
    /// Delegates to `insert_concurrent` with `GenericInsert` strategy.
    #[inline(always)]
    pub(super) fn insert_concurrent_generic(
        &self,
        key: &mut Key<'_>,
        value: P::Output,
        guard: &LocalGuard<'_>,
    ) -> Result<Option<P::Output>, InsertError> {
        self.insert_concurrent::<GenericInsert>(key, value, guard)
    }
}

// ============================================================================
//  Unified Insert Implementation (strategy-parameterized)
// ============================================================================

impl<P, A> MassTreeGeneric<P, A>
where
    P: LeafPolicy,
    A: TreeAllocator<P>,
{
    /// Unified concurrent insert with optimistic locking, parameterized by
    /// `InsertStrategy` to handle both the generic (`P::Output`) and
    /// write-through (`P::Value`) paths from a single code path.
    ///
    /// Monomorphization with `GenericInsert` or `WriteThroughInsert` produces
    /// identical machine code to the original hand-duplicated functions.
    #[expect(
        clippy::too_many_lines,
        reason = "Complex concurrency logic with dual-loop structure"
    )]
    pub(super) fn insert_concurrent<S: InsertStrategy<P, A>>(
        &self,
        key: &mut Key<'_>,
        value: S::Input,
        guard: &LocalGuard<'_>,
    ) -> Result<Option<S::OldValue>, InsertError> {
        let single_layer_mode: bool = !key.has_suffix();
        let mut layer_root: *const u8 = self.load_root_ptr_generic(guard);
        let mut in_sublayer: bool = false;

        'retry: loop {
            layer_root = self.maybe_parent_generic(layer_root);

            let mut leaf_ptr: *mut LeafNode15<P> =
                self.reach_leaf_concurrent_generic(layer_root, key, in_sublayer, guard);

            let (advanced_ptr, exceeded_hop_limit) =
                self.advance_to_key_by_bound_generic(leaf_ptr, key, guard);

            if exceeded_hop_limit {
                key.unshift_all();
                layer_root = self.load_root_ptr_generic(guard);
                in_sublayer = false;
                continue 'retry;
            }

            leaf_ptr = advanced_ptr;

            // SAFETY: leaf_ptr is valid, protected by guard
            let mut leaf: &LeafNode15<P> = unsafe { &*leaf_ptr };

            let mut pre_allocated_vec: Option<Vec<u8>> = None;

            'forward: loop {
                let has_suffix: bool = key.has_suffix();

                let pre_lock_version: u32 = leaf.version().stable();
                let pre_lock_perm: Permuter = leaf.permutation();
                let pre_lock_perm_raw: u64 = pre_lock_perm.value();

                let optimistic_search: InsertSearchResultGeneric = if single_layer_mode {
                    self.search_for_insert_single_layer(leaf, key, &pre_lock_perm)
                } else {
                    self.search_for_insert_generic(leaf, key, &pre_lock_perm)
                };

                if pre_allocated_vec.is_none() && has_suffix {
                    pre_allocated_vec = Self::maybe_pre_allocate_suffix(key, pre_lock_perm.size());
                }

                if let InsertSearchResultGeneric::Layer { slot } = optimistic_search {
                    let layer_ptr: *mut u8 = leaf.load_layer_raw(slot);

                    if !layer_ptr.is_null() && !leaf.version().has_changed(pre_lock_version) {
                        key.shift();
                        layer_root = layer_ptr;
                        in_sublayer = true;
                        continue 'retry;
                    }
                }

                let mut lock: LockGuard<'_> = leaf.version().lock_bounded();

                if !self.validate_post_lock(leaf, pre_lock_version, pre_lock_perm_raw) {
                    drop(lock);

                    let (advanced_ptr, exceeded) =
                        self.advance_to_key_by_bound_generic(leaf_ptr, key, guard);

                    if exceeded {
                        key.unshift_all();
                        layer_root = self.load_root_ptr_generic(guard);
                        in_sublayer = false;
                        continue 'retry;
                    }

                    leaf_ptr = advanced_ptr;
                    leaf = unsafe { &*leaf_ptr };
                    continue 'forward;
                }

                if leaf.deleted_layer() {
                    drop(lock);
                    key.unshift_all();
                    layer_root = self.load_root_ptr_generic(guard);
                    in_sublayer = false;
                    continue 'retry;
                }

                // Post-lock membership check
                match self.validate_membership(leaf, key) {
                    Ok(()) => { /* continue to search */ }
                    Err(MembershipError::SplitInProgress | MembershipError::KeyMovedToSibling) => {
                        drop(lock);

                        let (advanced_ptr, exceeded) =
                            self.advance_to_key_by_bound_generic(leaf_ptr, key, guard);

                        if exceeded {
                            key.unshift_all();
                            layer_root = self.load_root_ptr_generic(guard);
                            in_sublayer = false;
                            continue 'retry;
                        }

                        leaf_ptr = advanced_ptr;
                        leaf = unsafe { &*leaf_ptr };
                        continue 'forward;
                    }

                    Err(MembershipError::KeyBelowLowerBound) => {
                        drop(lock);
                        continue 'retry;
                    }
                }

                // Empty leaf reuse (lazy coalescing optimization)
                if pre_lock_perm.size() == 0 && self.can_reuse_empty_leaf(leaf, key) {
                    let output: P::Output = S::into_output(value);
                    let (_result, deferred_retire) = self.insert_into_empty_leaf(
                        leaf,
                        &mut lock,
                        key,
                        &output,
                        guard,
                        pre_allocated_vec.take(),
                    );
                    drop(lock);
                    // SAFETY: deferred_retire from assign_ksuf, guard from tree's collector.
                    unsafe { maybe_retire_suffix::<P>(deferred_retire, guard) };
                    self.count.increment();
                    return Ok(None);
                }

                match optimistic_search {
                    InsertSearchResultGeneric::Found { slot } => {
                        if leaf.is_value_empty(slot) {
                            drop(lock);
                            continue 'forward;
                        }

                        let old_value: S::OldValue =
                            S::update_existing(self, leaf, &mut lock, slot, &value, guard);

                        drop(lock);

                        return Ok(Some(old_value));
                    }

                    InsertSearchResultGeneric::NotFound { logical_pos } => {
                        let ikey: u64 = key.ikey();

                        match self.find_usable_slot(leaf, &pre_lock_perm, ikey) {
                            FindSlotResult::Found { slot, back_offset } => {
                                let output: P::Output = S::into_output(value);
                                let deferred_retire: *mut u8 = self.insert_new_value(
                                    leaf,
                                    &mut lock,
                                    slot,
                                    back_offset,
                                    logical_pos,
                                    pre_lock_perm,
                                    key,
                                    &output,
                                    guard,
                                    pre_allocated_vec.take(),
                                );
                                drop(lock);

                                // SAFETY: deferred_retire from assign_ksuf, guard from tree's collector.
                                unsafe { maybe_retire_suffix::<P>(deferred_retire, guard) };

                                self.count.increment();
                                return Ok(None);
                            }

                            FindSlotResult::NeedsSplit => {
                                let keylenx: u8 = if has_suffix {
                                    KSUF_KEYLENX
                                } else {
                                    #[expect(
                                        clippy::cast_possible_truncation,
                                        reason = "current_len <= 8 when !has_suffix"
                                    )]
                                    {
                                        key.current_len() as u8
                                    }
                                };

                                let suffix: Option<&[u8]> =
                                    if has_suffix { Some(key.suffix()) } else { None };

                                let output: P::Output = S::into_output(value);
                                let insert_data: SplitInsertData<'_, P> = SplitInsertData {
                                    ikey,
                                    keylenx,
                                    suffix,
                                    value: output,
                                };

                                match self.handle_leaf_split_and_insert_generic(
                                    leaf_ptr,
                                    lock,
                                    logical_pos,
                                    &insert_data,
                                    guard,
                                ) {
                                    Ok(_result) => {}

                                    Err(e) => {
                                        return Err(e);
                                    }
                                }

                                self.count.increment();
                                return Ok(None);
                            }
                        }
                    }

                    InsertSearchResultGeneric::Layer { slot, .. } => {
                        debug_assert!(
                            !single_layer_mode,
                            "single-layer search returned Layer variant"
                        );

                        let layer_ptr: *mut u8 = leaf.load_layer_raw(slot);
                        drop(lock);

                        key.shift();
                        layer_root = layer_ptr;
                        in_sublayer = true;
                        continue 'retry;
                    }

                    InsertSearchResultGeneric::Conflict { slot } => {
                        debug_assert!(
                            !single_layer_mode,
                            "single-layer search returned Conflict variant"
                        );

                        let output: P::Output = S::into_output(value);
                        self.handle_suffix_conflict(leaf, &mut lock, slot, key, output, guard);
                        self.count.increment();
                        return Ok(None);
                    }
                }
            } // end 'forward
        } // end 'retry
    }
}
