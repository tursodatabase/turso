//! Shared traversal utilities for range scans.
//!
//! This module contains traversal functions shared between forward
//! and reverse range iteration.

use std::ptr as StdPtr;

use seize::LocalGuard;

use crate::internode::InternodeNode;
use crate::ksearch::upper_bound_internode_generic;
use crate::leaf15::LeafNode15;
use crate::nodeversion::NodeVersion;
use crate::policy::LeafPolicy;
use crate::prefetch::prefetch_read;

use super::cursor_key::CursorKey;

/// Traverse from layer root to target leaf for range scans.
#[inline]
pub fn reach_leaf_for_scan<P>(
    start: *const u8,
    cursor_key: &CursorKey,
    _guard: &LocalGuard<'_>,
) -> *mut LeafNode15<P>
where
    P: LeafPolicy,
{
    if start.is_null() {
        return StdPtr::null_mut();
    }

    // TURSO PATCH: `start` may be a stale layer pointer — a leaf that has
    // since split (parent slots are not eagerly repointed at the layer's
    // internode root). The point-lookup path climbs to the true root before
    // descending (`maybe_parent_generic`); without the same climb here every
    // sublayer scan entered at the pre-split first leaf and walked the leaf
    // chain linearly, making bounded scans O(position in layer).
    let mut root: *const u8 = start;
    loop {
        // SAFETY: root is valid, both node types have NodeVersion as first field
        #[expect(clippy::cast_ptr_alignment, reason = "proper alignment")]
        let version: &NodeVersion = unsafe { &*(root.cast::<NodeVersion>()) };
        let parent: *mut u8 = if version.is_leaf() {
            // SAFETY: version.is_leaf() confirmed
            let leaf: &LeafNode15<P> = unsafe { &*(root.cast::<LeafNode15<P>>()) };
            unsafe { leaf.parent_unguarded() }
        } else {
            // SAFETY: !version.is_leaf() confirmed
            let inode: &InternodeNode = unsafe { &*(root.cast::<InternodeNode>()) };
            unsafe { inode.parent_unguarded() }
        };
        if parent.is_null() {
            break;
        }
        root = parent;
    }

    let target_ikey: u64 = cursor_key.current_ikey();
    let mut node: *const u8 = root;

    loop {
        // SAFETY: node is valid, both node types have NodeVersion as first field
        #[expect(clippy::cast_ptr_alignment, reason = "proper alignment")]
        let version: &NodeVersion = unsafe { &*(node.cast::<NodeVersion>()) };

        // Get stable version (spins if dirty)
        let v: u32 = version.stable();

        if version.is_leaf() {
            // Reached a leaf
            return node.cast_mut().cast::<LeafNode15<P>>();
        }

        // It's an internode - traverse down
        // SAFETY: !is_leaf() confirmed above
        let inode: &InternodeNode = unsafe { &*(node.cast::<InternodeNode>()) };

        // Binary search for child
        let child_idx: usize = upper_bound_internode_generic::<InternodeNode>(target_ikey, inode);
        // SAFETY: We've already established ordering via version check;
        // child pointer is valid during OCC read.
        let child: *mut u8 = unsafe { inode.child_unguarded(child_idx) };

        // Prefetch child node
        prefetch_read(child);

        if child.is_null() {
            // Concurrent split in progress - retry from the climbed root
            node = root;
            continue;
        }

        // Check if internode changed during our read
        if inode.version().has_changed(v) {
            // Version changed - check for split
            if inode.version().has_split(v) {
                // Key might have escaped to sibling - retry from the climbed root
                node = root;
                continue;
            }
            // Just retry this internode
            continue;
        }

        // Descend to child
        node = child;
    }
}
