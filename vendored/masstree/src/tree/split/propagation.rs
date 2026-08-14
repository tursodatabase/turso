//! Core hand-over-hand split propagation loop.

use std::hint as StdHint;
use std::ptr as StdPtr;
use std::sync::atomic::{AtomicPtr, Ordering as AtomicOrdering};

use seize::LocalGuard;

use crate::TreeAllocator;
use crate::internode::InternodeNode;
use crate::leaf15::LeafNode15;
use crate::nodeversion::LockGuard;
use crate::policy::LeafPolicy;

use super::parent_locking::ParentLocking;
use super::propagation_context::PropagationContext;
use super::root_creation::RootCreation;

/// Maximum spins before backoff caps.
const BACKOFF_CAP: u32 = 64;

/// Unit struct namespace for split propagation operations.
pub struct Propagation;

impl Propagation {
    /// Perform hand-over-hand split propagation for a leaf split.
    #[expect(
        clippy::too_many_arguments,
        reason = "Split propagation requires full context"
    )]
    pub fn make_split_leaf<'op, P, A>(
        root_ptr: &AtomicPtr<u8>,
        allocator: &A,
        left_leaf_ptr: *mut LeafNode15<P>,
        left_lock: LockGuard<'_>,
        right_leaf_ptr: *mut LeafNode15<P>,
        split_ikey: u64,
        is_main_root: bool,
        is_layer_root: bool,
        guard: &'op LocalGuard<'op>,
    ) where
        P: LeafPolicy,
        A: TreeAllocator<P>,
    {
        // DEBUG: Trace leaf split
        #[cfg(feature = "debug-routing")]
        eprintln!(
            "[LEAF_SPLIT] left={left_leaf_ptr:p} right={right_leaf_ptr:p} split_ikey={split_ikey:016x} is_main_root={is_main_root} is_layer_root={is_layer_root}"
        );

        // Create PropagationContext with unified lifetime tied to reclamation guard
        let ctx: PropagationContext<'op> = PropagationContext::new(guard);

        // SAFETY: Lifetime extension is sound because:
        // 1. Reclamation guard prevents deallocation while we hold it
        // 2. Leaf is locked, preventing structural modification
        let left_lock: LockGuard<'op> = unsafe { ctx.unify_guard(left_lock) };
        Self::propagation_loop::<P, A>(
            root_ptr,
            allocator,
            &ctx,
            left_leaf_ptr.cast(),
            left_lock,
            right_leaf_ptr.cast(),
            split_ikey,
            is_main_root,
            is_layer_root,
            true, // at_leaf_level
        );
    }

    /// Core iterative propagation loop with hand-over-hand locking.
    ///
    /// Uses `PropagationContext<'op>` for unified-lifetime lock management,
    /// enabling RAII guard transfer across loop iterations.
    #[expect(clippy::too_many_lines, reason = "Complex state machine with tracing")]
    #[expect(clippy::too_many_arguments, reason = "State passed explicitly")]
    fn propagation_loop<'op, P, A>(
        root_ptr: &AtomicPtr<u8>,
        allocator: &A,
        ctx: &PropagationContext<'op>,
        mut left_ptr: *mut u8,         // Erased pointer (leaf or internode)
        mut left_lock: LockGuard<'op>, // RAII guard with unified lifetime
        mut right_ptr: *mut u8,        // Erased pointer (split-locked)
        mut split_ikey: u64,
        mut is_main_root: bool,
        mut is_layer_root: bool,
        mut at_leaf_level: bool,
    ) where
        P: LeafPolicy,
        A: TreeAllocator<P>,
    {
        // Exponential backoff state for contention reduction.
        let mut backoff: u32 = 1;

        loop {
            // Get left's parent pointer
            let left_parent: *mut u8 = Self::get_parent::<P>(left_ptr, at_leaf_level);

            if left_parent.is_null() && is_layer_root {
                Self::promote_layer_root::<P, A>(
                    allocator,
                    left_ptr,
                    right_ptr,
                    split_ikey,
                    at_leaf_level,
                );

                Self::unlock_right_for_split::<P>(right_ptr, at_leaf_level);
                drop(left_lock);
                return;
            }

            if left_parent.is_null() && is_main_root {
                Self::create_main_root::<P, A>(
                    root_ptr,
                    allocator,
                    left_ptr,
                    right_ptr,
                    split_ikey,
                    at_leaf_level,
                );

                Self::unlock_right_for_split::<P>(right_ptr, at_leaf_level);
                drop(left_lock);
                return;
            }

            let parent: &InternodeNode = unsafe { &*left_parent.cast::<InternodeNode>() };

            // SAFETY: parent is valid (reclamation guard protects for 'op)
            // Use lock_node_yielding to reduce contention under high thread counts
            let mut parent_lock: LockGuard<'op> =
                unsafe { ctx.lock_node_yielding(parent.version().as_ptr()) };

            let current_left_parent: *mut u8 = Self::get_parent::<P>(left_ptr, at_leaf_level);

            if current_left_parent != left_parent {
                drop(parent_lock); // RAII: auto-unlock
                Self::spin_backoff(&mut backoff);

                continue;
            }

            let child_idx: usize =
                if let Some(idx) = ParentLocking::validate_membership(parent, left_ptr) {
                    // Success: reset backoff for next potential retry
                    backoff = 1;
                    idx
                } else {
                    drop(parent_lock);
                    Self::spin_backoff(&mut backoff);

                    continue;
                };

            if !parent.is_full() {
                parent_lock.mark_insert();

                #[cfg(feature = "debug-routing")]
                {
                    eprintln!(
                        "[PARENT_INSERT] parent={:p} height={} child_idx={} split_ikey={:016x} nkeys_before={}",
                        left_parent,
                        parent.height(),
                        child_idx,
                        split_ikey,
                        parent.nkeys()
                    );
                }

                parent.insert_key_and_child(child_idx, split_ikey, right_ptr);

                #[cfg(feature = "debug-routing")]
                {
                    let nkeys = parent.nkeys();

                    eprint!("        parent_keys_after[{nkeys}]: ");

                    for i in 0..nkeys {
                        eprint!("{:016x} ", parent.ikey(i));
                    }

                    eprintln!();
                }

                Self::set_parent::<P>(right_ptr, left_parent, at_leaf_level);

                Self::unlock_right_for_split::<P>(right_ptr, at_leaf_level);
                drop(parent_lock);
                drop(left_lock);

                return;
            }

            parent_lock.mark_split();

            let parent_is_main_root: bool = {
                let current_root: *mut u8 = root_ptr.load(AtomicOrdering::Acquire);

                StdPtr::eq(current_root, left_parent)
            };

            // SAFETY: Called under lock - no concurrent retirement.
            let parent_is_layer_root: bool = !parent_is_main_root
                && unsafe { parent.parent_unguarded() }.is_null()
                && parent.is_root();

            let parent_sibling_ptr: *mut InternodeNode = allocator
                .alloc_internode_direct_for_split(parent.version(), parent.height())
                .cast();

            let (popup_key, child_went_left): (u64, bool) = unsafe {
                parent.split_into(
                    &mut *parent_sibling_ptr,
                    parent_sibling_ptr,
                    child_idx,
                    split_ikey,
                    right_ptr,
                )
            };

            // Only reparent leaf children here. Internode children are already
            // reparented inside `split_into` (when height > 0).
            if parent.children_are_leaves() {
                Self::reparent_sibling_leaf_children::<P>(parent_sibling_ptr);
            }

            let right_new_parent: *mut u8 = if child_went_left {
                left_parent
            } else {
                parent_sibling_ptr.cast()
            };

            Self::set_parent::<P>(right_ptr, right_new_parent, at_leaf_level);

            Self::unlock_right_for_split::<P>(right_ptr, at_leaf_level);

            drop(left_lock);

            left_lock = parent_lock;
            left_ptr = left_parent;
            right_ptr = parent_sibling_ptr.cast();
            split_ikey = popup_key;
            is_main_root = parent_is_main_root;
            is_layer_root = parent_is_layer_root;
            at_leaf_level = false;
        }
    }

    // =========================================================================
    // Helper methods
    // =========================================================================

    /// Returns parent pointer for a type-erased node.
    ///
    /// `ptr` must point to a valid node matching `is_leaf`. Called under lock
    /// or during propagation where nodes are locked.
    #[inline]
    pub(crate) fn get_parent<P>(ptr: *mut u8, is_leaf: bool) -> *mut u8
    where
        P: LeafPolicy,
    {
        // SAFETY: Called under lock or during propagation where nodes are locked.
        if is_leaf {
            unsafe { (*ptr.cast::<LeafNode15<P>>()).parent_unguarded() }
        } else {
            unsafe { (*ptr.cast::<InternodeNode>()).parent_unguarded() }
        }
    }

    /// Sets parent pointer for a type-erased node.
    ///
    /// `ptr` must point to a valid, locked node matching `is_leaf`.
    #[inline]
    pub(crate) fn set_parent<P>(ptr: *mut u8, parent: *mut u8, is_leaf: bool)
    where
        P: LeafPolicy,
    {
        if is_leaf {
            // SAFETY: Caller guarantees ptr is valid leaf
            unsafe { (*ptr.cast::<LeafNode15<P>>()).set_parent(parent) };
        } else {
            // SAFETY: Caller guarantees ptr is valid internode
            unsafe { (*ptr.cast::<InternodeNode>()).set_parent(parent) };
        }
    }

    /// Unlock a split-locked right sibling.
    #[inline]
    fn unlock_right_for_split<P>(ptr: *mut u8, is_leaf: bool)
    where
        P: LeafPolicy,
    {
        if is_leaf {
            // SAFETY: ptr points to a valid split-locked leaf
            unsafe { (*ptr.cast::<LeafNode15<P>>()).version().unlock_for_split() };
        } else {
            // SAFETY: ptr points to a valid split-locked internode
            unsafe { (*ptr.cast::<InternodeNode>()).version().unlock_for_split() };
        }
    }

    /// Reparent leaf children that moved to the split sibling.
    ///
    /// Called only when `parent.children_are_leaves()`. Internode children
    /// are reparented inside `InternodeNode::split_into` directly.
    ///
    /// # Safety
    /// - The original parent must be locked
    /// - `sibling_ptr` must be valid and split-locked
    fn reparent_sibling_leaf_children<P>(sibling_ptr: *mut InternodeNode)
    where
        P: LeafPolicy,
    {
        let sibling: &InternodeNode = unsafe { &*sibling_ptr };
        let nkeys: usize = sibling.nkeys();

        for i in 0..=nkeys {
            // SAFETY: Sibling is split-locked, children are valid leaves.
            let child: *mut u8 = unsafe { sibling.child_unguarded(i) };

            debug_assert!(
                !child.is_null(),
                "reparent_sibling_leaf_children: null child at index {i}"
            );

            // SAFETY: Parent is height 0, so children are leaves.
            unsafe {
                (*child.cast::<LeafNode15<P>>()).set_parent(sibling_ptr.cast());
            }
        }
    }

    #[cold]
    #[inline(never)]
    fn promote_layer_root<P, A>(
        allocator: &A,
        left_ptr: *mut u8,
        right_ptr: *mut u8,
        split_ikey: u64,
        is_leaf: bool,
    ) where
        P: LeafPolicy,
        A: TreeAllocator<P>,
    {
        if is_leaf {
            RootCreation::promote_layer_root_leaves::<P, A>(
                allocator,
                left_ptr.cast(),
                right_ptr.cast(),
                split_ikey,
            );
        } else {
            RootCreation::promote_layer_root_internodes::<P, A>(
                allocator,
                left_ptr.cast(),
                right_ptr.cast(),
                split_ikey,
            );
        }
    }

    #[cold]
    #[inline(never)]
    fn create_main_root<P, A>(
        root_ptr: &AtomicPtr<u8>,
        allocator: &A,
        left_ptr: *mut u8,
        right_ptr: *mut u8,
        split_ikey: u64,
        is_leaf: bool,
    ) where
        P: LeafPolicy,
        A: TreeAllocator<P>,
    {
        if is_leaf {
            RootCreation::create_root_from_leaves::<P, A>(
                root_ptr,
                allocator,
                left_ptr.cast(),
                right_ptr.cast(),
                split_ikey,
            );
        } else {
            RootCreation::create_root_from_internodes::<P, A>(
                root_ptr,
                allocator,
                left_ptr.cast(),
                right_ptr.cast(),
                split_ikey,
            );
        }
    }

    /// Exponential backoff: spin `backoff` times, then double (capped).
    ///
    /// Yields to OS scheduler at cap to avoid wasting cycles under sustained contention.
    #[inline]
    fn spin_backoff(backoff: &mut u32) {
        for _ in 0..*backoff {
            StdHint::spin_loop();
        }

        if *backoff >= BACKOFF_CAP {
            std::thread::yield_now();
        }

        *backoff = (*backoff * 2).min(BACKOFF_CAP);
    }
}
