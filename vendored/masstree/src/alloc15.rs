//! Node allocation for [`LeafNode15`] with WIDTH=15 leaves and WIDTH=15 internodes.
//!
//! This module provides [`SeizeAllocator<P>`], a Miri-compliant allocator for
//! masstree nodes using `seize` for memory reclamation.

use std::marker::PhantomData;
use std::ptr as StdPtr;

use arrayvec::ArrayVec;
use seize::{Guard, LocalGuard};

use crate::alloc_trait::TreeAllocator;
use crate::internode::InternodeNode;
use crate::leaf15::{LeafNode15, WIDTH_15};
use crate::node_pool;
use crate::nodeversion::NodeVersion;
use crate::policy::LeafPolicy;

// =============================================================================
// Iterative Tree Traversal
// =============================================================================

/// `ArrayVec` capacity for traversal stack (raw node pointers to visit).
const STACK_CAPACITY: usize = 128;

/// Overflow Vec initial capacity (only allocated when `ArrayVec` fills).
const OVERFLOW_INITIAL_CAPACITY: usize = 64;

/// Follow parent pointers to find the actual root of a sublayer.
///
/// # Safety
///
/// - `node_ptr` must point to a valid leaf or internode
/// - Caller must have exclusive access (no concurrent readers/writers)
#[inline]
#[expect(clippy::cast_ptr_alignment, reason = "Callers guarantee alignment")]
unsafe fn find_layer_root<P: LeafPolicy>(mut node_ptr: *mut u8) -> *mut u8 {
    loop {
        // SAFETY: Both leaves and internodes have NodeVersion at offset 0
        let version: &NodeVersion = unsafe { &*node_ptr.cast::<NodeVersion>() };

        // SAFETY: Called during teardown with exclusive access
        let parent: *mut u8 = if version.is_leaf() {
            // SAFETY: version.is_leaf() confirmed
            let leaf: &LeafNode15<P> = unsafe { &*node_ptr.cast::<LeafNode15<P>>() };
            unsafe { leaf.parent_unguarded() }
        } else {
            // SAFETY: !version.is_leaf() confirmed
            let inode: &InternodeNode = unsafe { &*node_ptr.cast::<InternodeNode>() };
            unsafe { inode.parent_unguarded() }
        };

        if parent.is_null() {
            return node_ptr;
        }

        node_ptr = parent;
    }
}

/// Iterative tree traversal and deallocation.
///
/// # Safety
///
/// - `root_ptr` must point to a valid leaf or internode
/// - Caller must have exclusive access (no concurrent readers/writers)
/// - Only safe during `Drop` when tree is quiescent
#[expect(clippy::cast_ptr_alignment, reason = "Callers guarantee alignment")]
unsafe fn traverse_and_free_iterative<P: LeafPolicy>(root_ptr: *mut u8) {
    let mut stack: ArrayVec<*mut u8, STACK_CAPACITY> = ArrayVec::new();
    let mut overflow: Option<Vec<*mut u8>> = None;

    stack.push(root_ptr);

    loop {
        let node_ptr: *mut u8 = match stack.pop() {
            Some(p) => p,

            None => match overflow.as_mut().and_then(Vec::pop) {
                Some(p) => p,

                None => break,
            },
        };

        if node_ptr.is_null() {
            continue;
        }

        // SAFETY: Both leaves and internodes have NodeVersion at offset 0
        let version: &NodeVersion = unsafe { &*node_ptr.cast::<NodeVersion>() };

        if version.is_leaf() {
            // SAFETY: version.is_leaf() confirmed
            let leaf: &LeafNode15<P> = unsafe { &*node_ptr.cast::<LeafNode15<P>>() };

            // Extract sublayer pointers by value before freeing the leaf.
            for slot in 0..WIDTH_15 {
                if leaf.is_layer(slot) {
                    let layer_ptr: *mut u8 = leaf.load_layer_raw(slot);

                    if !layer_ptr.is_null() {
                        // SAFETY: layer_ptr is valid, we have exclusive access
                        let layer_root: *mut u8 = unsafe { find_layer_root::<P>(layer_ptr) };

                        push_hybrid(&mut stack, &mut overflow, layer_root);
                    }
                }
            }

            // SAFETY: Sublayer pointers extracted above. Safe to free now.
            unsafe {
                StdPtr::drop_in_place(node_ptr.cast::<LeafNode15<P>>());
                node_pool::pool_teardown_dealloc_leaf::<P>(node_ptr);
            }
        } else {
            // SAFETY: !version.is_leaf() confirmed
            let internode: &InternodeNode = unsafe { &*node_ptr.cast::<InternodeNode>() };
            let nkeys: usize = internode.nkeys();

            // Extract child pointers by value before freeing the internode.
            // SAFETY: During Drop, we have exclusive access.
            for i in (0..=nkeys).rev() {
                let child: *mut u8 = unsafe { internode.child_unguarded(i) };
                if !child.is_null() {
                    push_hybrid(&mut stack, &mut overflow, child);
                }
            }

            // SAFETY: Child pointers extracted above. Safe to free now.
            unsafe { node_pool::pool_teardown_dealloc_internode(node_ptr) };
        }
    }
}

/// Push to hybrid stack: prefer `ArrayVec` (no allocation), fall back to `Vec`.
#[inline]
fn push_hybrid(
    stack: &mut ArrayVec<*mut u8, STACK_CAPACITY>,
    overflow: &mut Option<Vec<*mut u8>>,
    ptr: *mut u8,
) {
    if let Err(cap_err) = stack.try_push(ptr) {
        overflow
            .get_or_insert_with(|| Vec::with_capacity(OVERFLOW_INITIAL_CAPACITY))
            .push(cap_err.element());
    }
}

// =============================================================================
//  SeizeAllocator<P>
// =============================================================================

/// Unified seize-based allocator for all leaf policies.
#[derive(Debug)]
pub struct SeizeAllocator<P: LeafPolicy> {
    _marker: PhantomData<P>,
}

// SAFETY: Allocator is stateless (just PhantomData), safe to send/share.
unsafe impl<P: LeafPolicy> Send for SeizeAllocator<P> {}
unsafe impl<P: LeafPolicy> Sync for SeizeAllocator<P> {}

impl<P: LeafPolicy> SeizeAllocator<P> {
    /// Create a new allocator.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }

    /// Traverse tree structure and free all nodes (iterative).
    ///
    /// # Safety
    ///
    /// - `node_ptr` must point to a valid leaf or internode
    /// - Caller must have exclusive access
    #[expect(clippy::unused_self, reason = "Required by TreeAllocator trait")]
    unsafe fn traverse_and_free(&self, node_ptr: *mut u8) {
        if node_ptr.is_null() {
            return;
        }

        // SAFETY: Caller guarantees valid pointer and exclusive access.
        unsafe {
            traverse_and_free_iterative::<P>(node_ptr);
        }
    }
}

impl<P: LeafPolicy> Default for SeizeAllocator<P> {
    fn default() -> Self {
        Self::new()
    }
}

impl<P: LeafPolicy + 'static> TreeAllocator<P> for SeizeAllocator<P> {
    #[inline(always)]
    unsafe fn retire_leaf(&self, ptr: *mut LeafNode15<P>, guard: &LocalGuard<'_>) {
        // SAFETY: Caller ensures ptr is valid and unreachable from tree.
        unsafe {
            guard.defer_retire(ptr, node_pool::reclaim_leaf15::<P>);
        }
    }

    #[inline(always)]
    #[expect(
        clippy::cast_ptr_alignment,
        reason = "Caller guarantees InternodeNode alignment"
    )]
    unsafe fn retire_internode_erased(&self, ptr: *mut u8, guard: &LocalGuard<'_>) {
        // SAFETY: Caller ensures ptr is valid and unreachable from tree.
        unsafe {
            guard.defer_retire(ptr.cast::<InternodeNode>(), node_pool::reclaim_internode);
        }
    }

    #[expect(
        clippy::not_unsafe_ptr_arg_deref,
        reason = "Trait contract requires valid pointer from tree root"
    )]
    fn teardown_tree(&self, root_ptr: *mut u8) {
        if root_ptr.is_null() {
            return;
        }

        // SAFETY: root_ptr is valid, we have exclusive access during Drop.
        unsafe { self.traverse_and_free(root_ptr) };
    }

    #[inline(always)]
    fn alloc_leaf_direct(&self, is_root: bool, is_layer_root: bool) -> *mut LeafNode15<P> {
        let ptr: *mut LeafNode15<P> = node_pool::pool_alloc_leaf::<P>().cast();

        // SAFETY: ptr is valid, properly aligned, and we have exclusive access
        unsafe {
            LeafNode15::<P>::init_at(ptr, is_root || is_layer_root);
        }

        ptr
    }

    #[inline(always)]
    fn alloc_internode_direct(&self, height: u32) -> *mut u8 {
        let ptr: *mut u8 = node_pool::pool_alloc_internode();

        // SAFETY: ptr is valid, properly aligned, and we have exclusive access
        unsafe {
            InternodeNode::init_at(ptr.cast::<InternodeNode>(), height);
        }

        ptr
    }

    #[inline(always)]
    fn alloc_internode_direct_root(&self, height: u32) -> *mut u8 {
        let ptr: *mut u8 = node_pool::pool_alloc_internode();

        // SAFETY: ptr is valid, properly aligned, and we have exclusive access
        unsafe {
            InternodeNode::init_at_root(ptr.cast::<InternodeNode>(), height);
        }

        ptr
    }

    #[inline(always)]
    fn alloc_internode_direct_for_split(
        &self,
        parent_version: &crate::nodeversion::NodeVersion,
        height: u32,
    ) -> *mut u8 {
        let ptr: *mut u8 = node_pool::pool_alloc_internode();

        // SAFETY: ptr is valid, properly aligned, and we have exclusive access
        unsafe {
            InternodeNode::init_at_for_split(ptr.cast::<InternodeNode>(), parent_version, height);
        }

        ptr
    }
}
