//! Root and layer-root creation helpers.

use std::sync::atomic::{AtomicPtr, Ordering as AtomicOrdering};

use crate::TreeAllocator;
use crate::internode::InternodeNode;
use crate::leaf15::LeafNode15;
use crate::policy::LeafPolicy;

/// Unit struct namespace for root creation operations.
pub struct RootCreation;

impl RootCreation {
    // =========================================================================
    // Main Tree Root Creation
    // =========================================================================

    /// Create a new main tree root internode from two leaves.
    ///
    /// Atomically installs a new root via store on `root_ptr`. Parent pointers
    /// are updated after the store to avoid dangling references.
    pub fn create_root_from_leaves<P, A>(
        root_ptr: &AtomicPtr<u8>,
        allocator: &A,
        left_leaf_ptr: *mut LeafNode15<P>,
        right_leaf_ptr: *mut LeafNode15<P>,
        split_ikey: u64,
    ) -> *mut InternodeNode
    where
        P: LeafPolicy,
        A: TreeAllocator<P>,
    {
        let new_root_ptr: *mut u8 = allocator.alloc_internode_direct_root(0);
        let new_root: &InternodeNode = unsafe { &*new_root_ptr.cast::<InternodeNode>() };

        new_root.set_child(0, left_leaf_ptr.cast());
        new_root.set_ikey(0, split_ikey);
        new_root.set_child(1, right_leaf_ptr.cast());
        new_root.set_nkeys(1);

        root_ptr.store(new_root_ptr, AtomicOrdering::Release);

        // SAFETY: Both leaves are valid and locked (left by caller, right split-locked).
        unsafe {
            (*left_leaf_ptr).set_parent(new_root_ptr);
            (*right_leaf_ptr).set_parent(new_root_ptr);
            (*left_leaf_ptr).version().mark_nonroot();
        }

        new_root_ptr.cast()
    }

    /// Create a new main tree root internode from two internodes.
    pub fn create_root_from_internodes<P, A>(
        root_ptr: &AtomicPtr<u8>,
        allocator: &A,
        left_inode_ptr: *mut InternodeNode,
        right_inode_ptr: *mut InternodeNode,
        split_ikey: u64,
    ) -> *mut InternodeNode
    where
        P: LeafPolicy,
        A: TreeAllocator<P>,
    {
        let left: &InternodeNode = unsafe { &*left_inode_ptr };

        let new_root_ptr: *mut u8 = allocator.alloc_internode_direct_root(left.height() + 1);
        let new_root: &InternodeNode = unsafe { &*new_root_ptr.cast::<InternodeNode>() };

        new_root.set_child(0, left_inode_ptr.cast());
        new_root.set_ikey(0, split_ikey);
        new_root.set_child(1, right_inode_ptr.cast());
        new_root.set_nkeys(1);

        root_ptr.store(new_root_ptr, AtomicOrdering::Release);

        // SAFETY: Both internodes are valid and locked (left by caller, right split-locked).
        unsafe {
            (*left_inode_ptr).set_parent(new_root_ptr);
            (*right_inode_ptr).set_parent(new_root_ptr);
            (*left_inode_ptr).version().mark_nonroot();
        }

        new_root_ptr.cast()
    }

    // =========================================================================
    // Layer Root Creation
    // =========================================================================

    /// Promote a layer root leaf to a new layer internode.
    pub fn promote_layer_root_leaves<P, A>(
        allocator: &A,
        left_leaf_ptr: *mut LeafNode15<P>,
        right_leaf_ptr: *mut LeafNode15<P>,
        split_ikey: u64,
    ) -> *mut InternodeNode
    where
        P: LeafPolicy,
        A: TreeAllocator<P>,
    {
        let new_inode_ptr: *mut u8 = allocator.alloc_internode_direct_root(0);
        let new_inode: &InternodeNode = unsafe { &*new_inode_ptr.cast::<InternodeNode>() };

        new_inode.set_child(0, left_leaf_ptr.cast());
        new_inode.set_ikey(0, split_ikey);
        new_inode.set_child(1, right_leaf_ptr.cast());
        new_inode.set_nkeys(1);

        // SAFETY: Both leaves are valid and locked (left by caller, right split-locked).
        // set_parent uses Release ordering internally.
        unsafe {
            (*left_leaf_ptr).set_parent(new_inode_ptr);
            (*right_leaf_ptr).set_parent(new_inode_ptr);
            (*left_leaf_ptr).version().mark_nonroot();
            (*right_leaf_ptr).version().mark_nonroot();
        }

        new_inode_ptr.cast()
    }

    /// Promote a layer root internode to a new layer internode.
    pub fn promote_layer_root_internodes<P, A>(
        allocator: &A,
        left_inode_ptr: *mut InternodeNode,
        right_inode_ptr: *mut InternodeNode,
        split_ikey: u64,
    ) -> *mut InternodeNode
    where
        P: LeafPolicy,
        A: TreeAllocator<P>,
    {
        let left: &InternodeNode = unsafe { &*left_inode_ptr };

        let new_inode_ptr: *mut u8 = allocator.alloc_internode_direct_root(left.height() + 1);
        let new_inode: &InternodeNode = unsafe { &*new_inode_ptr.cast::<InternodeNode>() };

        new_inode.set_child(0, left_inode_ptr.cast());
        new_inode.set_ikey(0, split_ikey);
        new_inode.set_child(1, right_inode_ptr.cast());
        new_inode.set_nkeys(1);

        // SAFETY: Both internodes are valid and locked (left by caller, right split-locked).
        // set_parent uses Release ordering internally.
        unsafe {
            (*left_inode_ptr).set_parent(new_inode_ptr);
            (*right_inode_ptr).set_parent(new_inode_ptr);
            (*left_inode_ptr).version().mark_nonroot();
        }

        new_inode_ptr.cast()
    }
}
