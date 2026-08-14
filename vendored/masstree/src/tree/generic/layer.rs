//! =============================================================================
//!  Generic Layer Creation
//! =============================================================================

use std::ptr as StdPtr;

// Note: InsertError no longer used here - allocations are infallible

use crate::leaf_trait::TreeLeafNode;
use crate::leaf15::LeafNode15;

use super::{
    Key, LAYER_KEYLENX, LeafPolicy, LocalGuard, MassTreeGeneric, Ordering, TreeAllocator,
    TreePermutation,
};

impl<P, A> MassTreeGeneric<P, A>
where
    P: LeafPolicy,
    A: TreeAllocator<P>,
{
    /// Assign two entries to the final layer leaf in sorted order.
    ///
    /// The entries are ordered by:
    /// 1. ikey comparison (lexicographic via u64 big-endian)
    /// 2. If ikeys equal: shorter key first (prefix before extension)
    ///
    /// # Safety
    ///
    /// - `final_ptr` must be valid and point to an empty leaf
    /// - `guard` must come from this tree's collector
    /// - Caller must ensure no concurrent access to `final_ptr`
    #[expect(clippy::too_many_arguments, reason = "Internal helper")]
    #[expect(clippy::unused_self, reason = "API Consistency")]
    unsafe fn assign_final_layer_entries(
        &self,
        final_ptr: *mut LeafNode15<P>,
        existing_key: &Key<'_>,
        existing_output: Option<P::Output>,
        new_key: &Key<'_>,
        new_arc: Option<P::Output>,
        cmp: Ordering,
        guard: &LocalGuard<'_>,
    ) {
        // SAFETY: final_ptr is valid per caller contract
        let final_leaf: &LeafNode15<P> = unsafe { &*final_ptr };

        match cmp {
            Ordering::Less => {
                // SAFETY: guard requirement passed through from caller
                unsafe {
                    final_leaf.assign_from_key(0, existing_key, existing_output, guard);
                    final_leaf.assign_from_key(1, new_key, new_arc, guard);
                }
            }

            Ordering::Greater => {
                // SAFETY: guard requirement passed through from caller
                unsafe {
                    final_leaf.assign_from_key(0, new_key, new_arc, guard);
                    final_leaf.assign_from_key(1, existing_key, existing_output, guard);
                }
            }

            Ordering::Equal => {
                if existing_key.current_len() <= new_key.current_len() {
                    // SAFETY: guard requirement passed through from caller
                    unsafe {
                        final_leaf.assign_from_key(0, existing_key, existing_output, guard);
                        final_leaf.assign_from_key(1, new_key, new_arc, guard);
                    }
                } else {
                    // SAFETY: guard requirement passed through from caller
                    unsafe {
                        final_leaf.assign_from_key(0, new_key, new_arc, guard);
                        final_leaf.assign_from_key(1, existing_key, existing_output, guard);
                    }
                }
            }
        }

        // Relaxed is sufficient: this leaf is not yet visible to other threads.
        // The store_layer Release in the caller publishes it.
        final_leaf.set_permutation_relaxed(
            <<LeafNode15<P> as TreeLeafNode<P>>::Perm as TreePermutation>::make_sorted(2),
        );
    }

    /// Create a new layer for suffix conflict (generic version).
    ///
    /// # Safety
    ///
    /// - Caller must hold the lock on `parent_leaf`
    /// - Caller must have called `lock.mark_insert()` before calling this
    /// - `guard` must come from this tree's collector
    #[cold]
    #[inline(never)]
    pub(super) unsafe fn create_layer_concurrent_generic(
        &self,
        parent_leaf: &LeafNode15<P>,
        conflict_slot: usize,
        new_key: &mut Key<'_>,
        new_value: P::Output,
        guard: &LocalGuard<'_>,
    ) -> *mut u8 {
        // STEP: 1 - Extract existing key's suffix and value
        debug_assert!(
            parent_leaf.ksuf(conflict_slot).is_some(),
            "conflict slot {conflict_slot} should have a suffix \
             (keylenx={}, ikey=0x{:016x})",
            parent_leaf.keylenx(conflict_slot),
            parent_leaf.ikey(conflict_slot),
        );
        let existing_suffix: &[u8] = parent_leaf.ksuf(conflict_slot).unwrap_or(&[]);
        let mut existing_key: Key<'_> = Key::from_suffix(existing_suffix);
        let existing_output: Option<P::Output> =
            parent_leaf.try_clone_output_relaxed(conflict_slot);

        debug_assert!(
            existing_output.is_some(),
            "try_create_layer_concurrent_generic: conflict slot should contain a value"
        );

        if new_key.has_suffix() {
            new_key.shift();
        }

        let mut cmp: Ordering = existing_key.compare(new_key.ikey(), new_key.current_len());

        let mut twig_head: Option<*mut LeafNode15<P>> = None;
        let mut twig_tail: *mut LeafNode15<P> = StdPtr::null_mut();

        while (cmp == Ordering::Equal) && existing_key.has_suffix() && new_key.has_suffix() {
            let twig_ptr: *mut LeafNode15<P> = self.allocator.alloc_leaf_direct(false, true);

            // SAFETY: twig_ptr is valid, we just allocated it
            unsafe {
                (*twig_ptr).set_ikey(0, existing_key.ikey());
                // Relaxed: twig is not yet visible. store_layer Release publishes it.
                (*twig_ptr).set_permutation_relaxed(
                    <<LeafNode15<P> as TreeLeafNode<P>>::Perm as TreePermutation>::make_sorted(1),
                );
            }

            if twig_head.is_some() {
                // SAFETY: twig_tail is valid from previous iteration
                unsafe {
                    (*twig_tail).set_keylenx(0, LAYER_KEYLENX);
                    (*twig_tail).store_layer(0, twig_ptr.cast::<u8>());
                }
            } else {
                twig_head = Some(twig_ptr);
            }

            twig_tail = twig_ptr;

            // Shift both keys
            existing_key.shift();
            new_key.shift();
            cmp = existing_key.compare(new_key.ikey(), new_key.current_len());
        }

        let final_ptr: *mut LeafNode15<P> = self.allocator.alloc_leaf_direct(false, true);

        // SAFETY: final_ptr is valid, guard is from caller
        unsafe {
            self.assign_final_layer_entries(
                final_ptr,
                &existing_key,
                existing_output,
                new_key,
                Some(new_value),
                cmp,
                guard,
            );
        }

        let result_ptr: *mut u8 = match twig_head {
            Some(head) => {
                unsafe {
                    (*twig_tail).set_keylenx(0, LAYER_KEYLENX);
                    (*twig_tail).store_layer(0, final_ptr.cast::<u8>());
                }
                head.cast::<u8>()
            }
            None => final_ptr.cast::<u8>(),
        };

        result_ptr
    }
}
