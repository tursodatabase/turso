//! Deferred cleanup queue for empty leaves.
//!
//! Two queue types: chain entries (pointer-based, for non-sublayer leaves) and
//! sublayer entries (route-based, re-traverse from root). The queued bit in
//! modstate deduplicates enqueue at the source.
//!
//! Deferred queues spread retries across maintenance calls. All requeues target
//! the deferred queue (not the main queue), ensuring each entry gets exactly one
//! attempt per maintenance call. Deferred chain entries store only `ikey_bound`
//! (no pointer) and re-traverse from root on retry, avoiding cross-call pointer
//! lifetime issues.

use std::fmt::{self as StdFmt, Debug, Formatter};
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

use crossbeam_queue::SegQueue;
use seize::LocalGuard;

use crate::alloc_trait::TreeAllocator;
use crate::key::Key;
use crate::leaf15::{LAYER_KEYLENX, LeafNode15};
use crate::link::Linker;
use crate::nodeversion::{LockGuard, NodeVersion};
use crate::policy::LeafPolicy;
use crate::tree::MassTreeGeneric;
use crate::tree::remove::{ImmediateParentResult, NodeCleaner};

// ============================================================================
//  Route type
// ============================================================================

/// Compact route from tree root to a sublayer's parent.
/// Each element is the ikey (u64) at that layer level.
pub type Route = Vec<u64>;

// ============================================================================
//  Entry types
// ============================================================================

/// Maximum number of times an entry can be re-queued before being dropped.
const MAX_REQUEUE_COUNT: u8 = 10;

/// Maximum B-link chain hops during GC re-traversal.
const MAX_GC_BLINK_HOPS: usize = 64;

/// Non-sublayer empty leaf pending chain unlink.
/// Only dereferenced in the same maintenance call that popped the entry.
#[derive(Debug, Clone)]
struct ChainEntry {
    /// Type-erased pointer to the empty leaf.
    leaf_ptr: *mut u8,

    ikey_bound: u64,

    requeue_count: u8,
}

/// Chain entry deferred to the next maintenance call.
/// Stores only the key range identifier, not the leaf pointer.
/// Re-traverses from root on retry to avoid cross-call pointer lifetime issues.
#[derive(Debug, Clone)]
struct DeferredChainEntry {
    ikey_bound: u64,

    requeue_count: u8,
}

/// Sublayer empty leaf pending route-based gc.
#[derive(Debug, Clone)]
struct SublayerEntry {
    /// Per-layer ikey segments, root to parent.
    route: Route,
    requeue_count: u8,
}

// ============================================================================
//  CoalesceQueue
// ============================================================================

/// Lock-free queue of empty leaves pending cleanup.
pub struct CoalesceQueue {
    chains: SegQueue<ChainEntry>,

    sublayers: SegQueue<SublayerEntry>,

    /// Chain entries deferred to the next maintenance call (route-based, no pointer).
    deferred_chains: SegQueue<DeferredChainEntry>,

    /// Sublayer entries deferred to the next maintenance call.
    deferred_sublayers: SegQueue<SublayerEntry>,

    /// Count of entries permanently abandoned (cleanup skipped).
    abandoned: AtomicUsize,
}

// SAFETY: CoalesceQueue requires unsafe Send/Sync because the `chains` field
// contains ChainEntry with *mut u8 (which is !Send). The pointer is only
// dereferenced under proper synchronization (seize guard + leaf lock) within
// the same maintenance call that popped the entry.
// The deferred_chains field contains DeferredChainEntry (u64 + u8), which is
// trivially Send+Sync. The `sublayers` and `deferred_sublayers` fields contain
// SublayerEntry (Vec<u64> + u8), which is trivially Send+Sync.
#[allow(clippy::non_send_fields_in_send_ty)]
unsafe impl Send for CoalesceQueue {}
unsafe impl Sync for CoalesceQueue {}

impl Default for CoalesceQueue {
    fn default() -> Self {
        Self::new()
    }
}

impl CoalesceQueue {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            chains: SegQueue::new(),
            sublayers: SegQueue::new(),
            deferred_chains: SegQueue::new(),
            deferred_sublayers: SegQueue::new(),
            abandoned: AtomicUsize::new(0),
        }
    }

    /// Schedule a non-sublayer empty leaf for chain coalesce.
    #[inline(always)]
    pub fn schedule_chain(&self, leaf_ptr: *mut u8, ikey_bound: u64) {
        self.chains.push(ChainEntry {
            leaf_ptr,
            ikey_bound,
            requeue_count: 0,
        });
    }

    /// Schedule a sublayer for route-based gc.
    #[inline(always)]
    pub fn schedule_sublayer(&self, route: Route) {
        self.sublayers.push(SublayerEntry {
            route,
            requeue_count: 0,
        });
    }

    /// Move deferred sublayer entries into the main sublayer queue for
    /// reprocessing. Deferred chain entries are processed separately
    /// via `try_remove_chain_deferred` (different type, requires re-traversal).
    fn drain_deferred(&self) {
        while let Some(entry) = self.deferred_sublayers.pop() {
            self.sublayers.push(entry);
        }
    }

    #[must_use]
    #[inline]
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.chains.is_empty()
            && self.sublayers.is_empty()
            && self.deferred_chains.is_empty()
            && self.deferred_sublayers.is_empty()
    }

    #[must_use]
    #[inline]
    pub fn len(&self) -> usize {
        self.chains.len()
            + self.sublayers.len()
            + self.deferred_chains.len()
            + self.deferred_sublayers.len()
    }

    /// Number of entries waiting for the next maintenance call.
    #[must_use]
    #[inline]
    pub fn deferred_len(&self) -> usize {
        self.deferred_chains.len() + self.deferred_sublayers.len()
    }

    /// Number of entries permanently abandoned since tree creation.
    #[must_use]
    #[inline]
    pub fn abandoned(&self) -> usize {
        self.abandoned.load(AtomicOrdering::Relaxed)
    }

    /// Clear all pending entries without processing.
    pub fn clear(&self) {
        while self.chains.pop().is_some() {}

        while self.sublayers.pop().is_some() {}

        while self.deferred_chains.pop().is_some() {}

        while self.deferred_sublayers.pop().is_some() {}
    }
}

impl Debug for CoalesceQueue {
    fn fmt(&self, f: &mut Formatter<'_>) -> StdFmt::Result {
        f.debug_struct("CoalesceQueue")
            .field("chains", &self.chains.len())
            .field("sublayers", &self.sublayers.len())
            .field("deferred_chains", &self.deferred_chains.len())
            .field("deferred_sublayers", &self.deferred_sublayers.len())
            .field("abandoned", &self.abandoned())
            .finish()
    }
}

// ============================================================================
//  Route lookup result
// ============================================================================

/// Result of a GC route lookup operation.
enum RouteLookupResult<T> {
    /// Target found successfully.
    Found(T),

    /// OCC validation failed or lock contention. Requeue, keep queued bit.
    Retry,

    /// Route is truly stale (slot absent, leaf version stable). Drop entry.
    /// The queued bit cannot be cleared because the sublayer is unreachable
    /// without the route.
    NotFound,

    /// Exceeded B-link chain walk budget. Requeue, keep queued bit.
    HopLimit,
}

/// Result of re-traversal: only the parent leaf, not the slot.
/// The slot must be re-found under lock to avoid stale-index bugs.
struct FoundParent<P: LeafPolicy> {
    parent_ptr: *mut LeafNode15<P>,
    last_ikey: u64,
}

// ============================================================================
//  Coalesce processor
// ============================================================================

pub struct Coalesce;

impl Coalesce {
    /// Process all queued removals.
    pub fn process_all<P, A>(tree: &MassTreeGeneric<P, A>, guard: &LocalGuard<'_>) -> usize
    where
        P: LeafPolicy,
        A: TreeAllocator<P>,
    {
        tree.coalesce_queue().drain_deferred();

        let mut processed: usize = 0;

        while Self::try_remove_one::<P, A>(tree, guard) {
            processed += 1;
        }

        processed
    }

    /// Process up to `limit` queued removals.
    pub fn process_batch<P, A>(
        tree: &MassTreeGeneric<P, A>,
        guard: &LocalGuard<'_>,
        limit: usize,
    ) -> usize
    where
        P: LeafPolicy,
        A: TreeAllocator<P>,
    {
        tree.coalesce_queue().drain_deferred();

        let mut processed: usize = 0;

        while processed < limit && Self::try_remove_one::<P, A>(tree, guard) {
            processed += 1;
        }

        processed
    }

    /// Try to remove one empty leaf from the queue.
    ///
    /// Processing order: deferred chains (oldest, re-traverse from root),
    /// then sublayers (includes freshly-drained deferred sublayers),
    /// then immediate chains (pointer-based, same guard scope).
    fn try_remove_one<P, A>(tree: &MassTreeGeneric<P, A>, guard: &LocalGuard<'_>) -> bool
    where
        P: LeafPolicy,
        A: TreeAllocator<P>,
    {
        let queue: &CoalesceQueue = tree.coalesce_queue();

        // Step 1: Deferred chain entries (re-traverse from root, no pointer).
        if let Some(entry) = queue.deferred_chains.pop() {
            return Self::try_remove_chain_deferred::<P, A>(
                tree,
                queue,
                entry.ikey_bound,
                entry.requeue_count,
                guard,
            );
        }

        // Step 2: Sublayer entries (includes freshly-drained deferred sublayers).
        // Sublayer priority: dead sublayer trees consume memory without
        // serving any lookup. Chain leaves are still part of the B-link
        // chain and serve as routing nodes, so delayed cleanup is less harmful.
        if let Some(entry) = queue.sublayers.pop() {
            return Self::try_remove_sublayer::<P, A>(
                tree,
                queue,
                entry.route,
                entry.requeue_count,
                guard,
            );
        }

        // Step 3: Immediate chain entries (same guard scope, pointer-based).
        if let Some(entry) = queue.chains.pop() {
            return Self::try_remove_chain::<P, A>(
                tree,
                queue,
                entry.leaf_ptr,
                entry.ikey_bound,
                entry.requeue_count,
                guard,
            );
        }

        false
    }

    // ========================================================================
    //  Chain coalesce (non-sublayer leaves)
    // ========================================================================

    /// Process a chain entry: non-sublayer empty leaf pending unlink.
    ///
    /// Uses parent-before-delete ordering: parent cleanup happens
    /// while the leaf is still alive, linked, and not deleted. On failure the
    /// leaf is unchanged and can be safely requeued without pointer storage.
    fn try_remove_chain<P, A>(
        tree: &MassTreeGeneric<P, A>,
        queue: &CoalesceQueue,
        leaf_ptr_erased: *mut u8,
        ikey_bound: u64,
        requeue_count: u8,
        guard: &LocalGuard<'_>,
    ) -> bool
    where
        P: LeafPolicy,
        A: TreeAllocator<P>,
    {
        let leaf_ptr: *mut LeafNode15<P> = leaf_ptr_erased.cast();

        // SAFETY: leaf_ptr is valid, protected by guard (same maintenance call).
        let leaf: &LeafNode15<P> = unsafe { &*leaf_ptr };

        let Some(mut lock) = leaf.version().try_lock() else {
            Self::requeue_chain(queue, ikey_bound, requeue_count);
            return true;
        };

        if leaf.size() > 0 {
            drop(lock);
            return true;
        }

        if leaf.deleted_layer() {
            drop(lock);
            return true;
        }

        if leaf.prev(guard).is_null() {
            // Leftmost leaf: cannot unlink. Clear queued bit so it is not
            // permanntly stuck on a leaf that can never be coalesced.
            leaf.clear_queued();
            drop(lock);
            return true;
        }

        // PARENT CLEANUP BEFORE DELETE
        // ----------------------------
        // Remove the leaf from its immediate parent while the leaf is still
        // alive, linked and not deleted. On failure, the leaf is unchanged
        // and can be safely requeued.
        Self::finish_chain_coalesce::<P, A>(
            tree,
            queue,
            leaf,
            leaf_ptr,
            ikey_bound,
            requeue_count,
            &mut lock,
            guard,
        );

        true
    }

    /// Process a deferred chain entry by re-traversing from root.
    ///
    /// Deferred entries store only `ikey_bound` (no pointer). Re-traversal
    /// from root finds the leaf under the current guard, avoiding cross-call
    /// pointer lifetime issues.
    #[cold]
    #[inline(never)]
    fn try_remove_chain_deferred<P, A>(
        tree: &MassTreeGeneric<P, A>,
        queue: &CoalesceQueue,
        ikey_bound: u64,
        requeue_count: u8,
        guard: &LocalGuard<'_>,
    ) -> bool
    where
        P: LeafPolicy,
        A: TreeAllocator<P>,
    {
        // Re-traverse from root to find the leaf at this key range.
        let leaf_ptr: *mut LeafNode15<P> =
            match Self::find_chain_leaf::<P, A>(tree, ikey_bound, guard) {
                RouteLookupResult::Found(ptr) => ptr,

                RouteLookupResult::NotFound => {
                    // Leaf was already handled (retired, reused, or restructured).
                    return true;
                }

                RouteLookupResult::Retry | RouteLookupResult::HopLimit => {
                    // Transient failure. Requeue for next maintenance call.
                    Self::requeue_chain(queue, ikey_bound, requeue_count);
                    return true;
                }
            };

        // SAFETY: leaf_ptr was just found via live tree traversal under guard.
        let leaf: &LeafNode15<P> = unsafe { &*leaf_ptr };

        let Some(mut lock) = leaf.version().try_lock() else {
            Self::requeue_chain(queue, ikey_bound, requeue_count);
            return true;
        };

        // Full validation (leaf state may have changed since find_chain_leaf).
        if leaf.size() > 0 || leaf.deleted_layer() {
            drop(lock);
            return true;
        }

        if leaf.prev(guard).is_null() {
            leaf.clear_queued();
            drop(lock);
            return true;
        }

        Self::finish_chain_coalesce::<P, A>(
            tree,
            queue,
            leaf,
            leaf_ptr,
            ikey_bound,
            requeue_count,
            &mut lock,
            guard,
        );

        true
    }

    /// Shared tail for `try_remove_chain` and `try_remove_chain_deferred`.
    ///
    /// Runs the parent-before-delete sequence: immediate parent cleanup, then
    /// `mark_deleted` + unlink + retire on success, or requeue on failure.
    #[expect(
        clippy::too_many_arguments,
        reason = "Shared coalesce tail needs full context"
    )]
    fn finish_chain_coalesce<P, A>(
        tree: &MassTreeGeneric<P, A>,
        queue: &CoalesceQueue,
        leaf: &LeafNode15<P>,
        leaf_ptr: *mut LeafNode15<P>,
        ikey_bound: u64,
        requeue_count: u8,
        lock: &mut LockGuard<'_>,
        guard: &LocalGuard<'_>,
    ) where
        P: LeafPolicy,
        A: TreeAllocator<P>,
    {
        let leaf_ikey_bound: u64 = leaf.ikey_bound();

        let result: ImmediateParentResult<'_> =
            NodeCleaner::remove_leaf_from_immediate_parent::<P, A>(
                tree.allocator(),
                guard,
                leaf_ptr,
                lock,
                leaf_ikey_bound,
            );

        match result {
            ImmediateParentResult::Success => {
                // Parent updated. Now safe to delete and unlink.
                lock.mark_deleted();

                // SAFETY: We hold the lock, and prev is non-null (checked by caller).
                unsafe { leaf.unlink_from_chain() };

                // SAFETY: Leaf is unreachable (deleted, unlinked, removed from parent).
                unsafe { tree.allocator().retire_leaf(leaf_ptr, guard) };
            }

            ImmediateParentResult::SuccessNeedsCascade {
                parent_ptr,
                parent_lock,
            } => {
                // Parent updated but became empty. Delete leaf first, then cascade.
                lock.mark_deleted();

                // SAFETY: We hold the lock, and prev is non-null (checked by caller).
                unsafe { leaf.unlink_from_chain() };

                // SAFETY: Leaf is unreachable (deleted, unlinked, removed from parent).
                unsafe { tree.allocator().retire_leaf(leaf_ptr, guard) };

                // Attempt cascade with ikey context for ancestor redirect.
                NodeCleaner::try_cascade_internodes::<P, A>(
                    tree.allocator(),
                    guard,
                    parent_ptr,
                    parent_lock,
                    leaf_ikey_bound,
                );
            }

            ImmediateParentResult::Failure => {
                // No modiifications were made. Leaf is still alive, linked, not
                // deleted. Safe to requeue for retry on the next maintenance call.
                Self::requeue_chain(queue, ikey_bound, requeue_count);
            }
        }
    }

    /// Push a chain entry to the deferred queue for the next maintenance call.
    /// On exhaustion increments the abandoned counter.
    fn requeue_chain(queue: &CoalesceQueue, ikey_bound: u64, count: u8) {
        if count < MAX_REQUEUE_COUNT {
            queue.deferred_chains.push(DeferredChainEntry {
                ikey_bound,
                requeue_count: count + 1,
            });
        } else {
            // Retry budget exhausted. Cannot clear queued bit (no pointer).
            // The queued bit will be cleared when insert reuses the leaf
            // (clear_empty_state stores 0) or tree is dropped.
            queue.abandoned.fetch_add(1, AtomicOrdering::Relaxed);
        }
    }

    /// Re-traverse from root to find the leaf whose key range contains
    /// `ikey_bound`. Returns `Found` with a validated leaf ptr,
    /// `NotFound` if the leaf was already handled or does not match,
    /// or `Retry`/`HopLimit` for transient fails.
    fn find_chain_leaf<P, A>(
        tree: &MassTreeGeneric<P, A>,
        ikey_bound: u64,
        guard: &LocalGuard<'_>,
    ) -> RouteLookupResult<*mut LeafNode15<P>>
    where
        P: LeafPolicy,
        A: TreeAllocator<P>,
    {
        let root: *const u8 = tree.load_root_ptr_generic(guard);
        let key: Key<'_> = Key::from_ikey(ikey_bound);
        let start: *mut LeafNode15<P> =
            tree.reach_leaf_concurrent_generic(root, &key, false, guard);

        match Self::walk_chain_to_ikey(start, ikey_bound, guard) {
            RouteLookupResult::Found(leaf_ptr) => {
                // SAFETY: leaf_ptr is valid, protected by guard.
                let leaf: &LeafNode15<P> = unsafe { &*leaf_ptr };

                // Verify this is an empty, queued leaf at the expected key range.
                if leaf.ikey_bound() == ikey_bound && leaf.is_empty_state() && leaf.is_queued() {
                    RouteLookupResult::Found(leaf_ptr)
                } else {
                    RouteLookupResult::NotFound
                }
            }

            // Preserve transient failures for caller to handle.
            RouteLookupResult::Retry => RouteLookupResult::Retry,

            RouteLookupResult::HopLimit => RouteLookupResult::HopLimit,

            RouteLookupResult::NotFound => RouteLookupResult::NotFound,
        }
    }

    // ========================================================================
    //  Sublayer GC (route-based re-traversal)
    // ========================================================================

    /// Process a sublayer gc entry by re-traversing from root.
    #[cold]
    #[inline(never)]
    fn try_remove_sublayer<P, A>(
        tree: &MassTreeGeneric<P, A>,
        queue: &CoalesceQueue,
        route: Route,
        requeue_count: u8,
        guard: &LocalGuard<'_>,
    ) -> bool
    where
        P: LeafPolicy,
        A: TreeAllocator<P>,
    {
        // Step 1: Find parent via optimistic re-traversal (B-link walk + OCC).
        let found = match Self::find_sublayer_parent::<P, A>(tree, &route, guard) {
            RouteLookupResult::Found(f) => f,

            RouteLookupResult::Retry | RouteLookupResult::HopLimit => {
                Self::requeue_sublayer(queue, route, requeue_count);
                return true;
            }

            RouteLookupResult::NotFound => {
                // Route truly stale. Cannot clear queued bit (unreachable).
                queue.abandoned.fetch_add(1, AtomicOrdering::Relaxed);
                return true;
            }
        };

        // Step 2: Lock parent.
        // SAFETY: found.parent_ptr is valid, protected by guard.
        let parent: &LeafNode15<P> = unsafe { &*found.parent_ptr };
        let Some(mut parent_lock) = parent.version().try_lock() else {
            Self::requeue_sublayer(queue, route, requeue_count);
            return true;
        };

        // Step 3: Re-search for layer slot under parent lock.
        // The parent could have been split between re-traversal and lock acquisition.
        let Some(parent_slot) = Self::find_layer_slot(parent, found.last_ikey) else {
            drop(parent_lock);
            Self::requeue_sublayer(queue, route, requeue_count);

            return true;
        };

        let layer_ptr: *mut u8 = parent.load_layer_raw(parent_slot);
        if layer_ptr.is_null() {
            drop(parent_lock);
            return true;
        }

        // Step 4: Lock sublayer (parent-first ordering, no deadlock).
        // SAFETY: layer_ptr is protected by guard, validated under parent lock.
        let sublayer: &LeafNode15<P> = unsafe { &*layer_ptr.cast::<LeafNode15<P>>() };
        let Some(sublayer_lock) = sublayer.version().try_lock() else {
            drop(parent_lock);
            Self::requeue_sublayer(queue, route, requeue_count);

            return true;
        };

        // Step 5a: Obsolete (sublayer non-empty or already deleted).
        if sublayer.deleted_layer() || sublayer.size() > 0 {
            sublayer.clear_queued();
            drop(sublayer_lock);
            drop(parent_lock);
            return true;
        }

        // Step 5b: Not isolated (empty but has siblings). Keep queued bit set.
        if !sublayer.prev(guard).is_null() || !sublayer.safe_next(guard).is_null() {
            drop(sublayer_lock);
            drop(parent_lock);
            Self::requeue_sublayer(queue, route, requeue_count);

            return true;
        }

        // Step 6: Clear parent slot (OCC dirty bit BEFORE mutation).
        parent_lock.mark_insert();

        parent.clear_slot_and_permutation(parent_slot);

        let parent_now_empty: bool = parent.size() == 0;
        let parent_newly_queued: bool = if parent_now_empty {
            parent.mark_empty();
            parent.try_mark_queued()
        } else {
            false
        };

        // Capture ikey_bound BEFORE dropping locks (required for cascade).
        let parent_ikey_bound: u64 = parent.ikey_bound();

        // Step 7: Mark sublayer deleted and retire.
        sublayer.mark_deleted_layer();
        drop(sublayer_lock);
        drop(parent_lock);

        // SAFETY: sublayer is unreachable from tree (parent slot cleared, sublayer
        // marked deleted). Guard protects against premature reclamation.
        unsafe {
            tree.allocator()
                .retire_leaf(layer_ptr.cast::<LeafNode15<P>>(), guard);
        }

        // Step 8: Cascade if parent became empty AND was newly queued.
        if parent_newly_queued {
            if route.len() > 1 {
                let mut parent_route: Route = route;
                parent_route.pop();

                queue.schedule_sublayer(parent_route);
            } else {
                queue.schedule_chain(found.parent_ptr.cast::<u8>(), parent_ikey_bound);
            }
        }

        true
    }

    /// Push a sublayer entry to the deferred queue for the next maintenance call.
    /// On exhaustion, increments the abandoned counter.
    fn requeue_sublayer(queue: &CoalesceQueue, route: Route, requeue_count: u8) {
        if requeue_count < MAX_REQUEUE_COUNT {
            queue.deferred_sublayers.push(SublayerEntry {
                route,
                requeue_count: requeue_count + 1,
            });
        } else {
            // Retry budget exhausted. Cannot clear queued bit (route-based,
            // no pointer). Same self-healing property as chain exhaustion.
            queue.abandoned.fetch_add(1, AtomicOrdering::Relaxed);
        }
    }

    // ========================================================================
    //  Re-traversal helpers
    // ========================================================================

    /// Find a slot containing a layer pointer with the given ikey.
    fn find_layer_slot<P: LeafPolicy>(leaf: &LeafNode15<P>, target_ikey: u64) -> Option<usize> {
        let perm = leaf.permutation();
        let size: usize = perm.size();

        for ki in 0..size {
            let kp: usize = perm.get(ki);
            if leaf.ikey_relaxed(kp) == target_ikey && leaf.keylenx(kp) >= LAYER_KEYLENX {
                return Some(kp);
            }
        }

        None
    }

    /// Re-traverse from tree root to find the parent leaf for a sublayer.
    fn find_sublayer_parent<P, A>(
        tree: &MassTreeGeneric<P, A>,
        route: &Route,
        guard: &LocalGuard<'_>,
    ) -> RouteLookupResult<FoundParent<P>>
    where
        P: LeafPolicy,
        A: TreeAllocator<P>,
    {
        debug_assert!(!route.is_empty());

        let mut current_root: *const u8 = tree.root_ptr.load(AtomicOrdering::Acquire);

        // Navigate intermediate layers: route[0..n-1]
        for &ikey in &route[..route.len() - 1] {
            let key: Key<'_> = Key::from_ikey(ikey);
            let leaf_ptr: *mut LeafNode15<P> =
                tree.reach_leaf_concurrent_generic(current_root, &key, true, guard);

            match Self::find_layer_in_chain(leaf_ptr, ikey, guard) {
                RouteLookupResult::Found(ptr) => current_root = ptr.cast_const(),

                RouteLookupResult::Retry => return RouteLookupResult::Retry,

                RouteLookupResult::NotFound => return RouteLookupResult::NotFound,

                RouteLookupResult::HopLimit => return RouteLookupResult::HopLimit,
            }
        }

        // Final layer: find parent leaf containing the last route ikey.
        let last_ikey: u64 = route[route.len() - 1];
        let key: Key<'_> = Key::from_ikey(last_ikey);
        let leaf_ptr: *mut LeafNode15<P> =
            tree.reach_leaf_concurrent_generic(current_root, &key, true, guard);

        match Self::walk_chain_to_ikey(leaf_ptr, last_ikey, guard) {
            RouteLookupResult::Found(parent_ptr) => RouteLookupResult::Found(FoundParent {
                parent_ptr,
                last_ikey,
            }),

            RouteLookupResult::Retry => RouteLookupResult::Retry,

            RouteLookupResult::NotFound => RouteLookupResult::NotFound,

            RouteLookupResult::HopLimit => RouteLookupResult::HopLimit,
        }
    }

    /// Walk B-link chain from `start` until the leaf whose key range contains
    /// `target_ikey`. Handles deleted leaves, split-marked pointers, and
    /// deleted-layer detection. Used by both `find_layer_in_chain` and
    /// `advance_to_ikey`.
    fn walk_chain_to_ikey<P: LeafPolicy>(
        start: *mut LeafNode15<P>,
        target_ikey: u64,
        guard: &LocalGuard<'_>,
    ) -> RouteLookupResult<*mut LeafNode15<P>> {
        let mut leaf_ptr: *mut LeafNode15<P> = start;
        let mut hops: usize = 0;

        loop {
            if hops >= MAX_GC_BLINK_HOPS {
                return RouteLookupResult::HopLimit;
            }

            // SAFETY: leaf_ptr is valid, protected by guard.
            let leaf: &LeafNode15<P> = unsafe { &*leaf_ptr };

            if leaf.version().is_deleted() {
                let next_raw: *mut LeafNode15<P> = leaf.next_raw(guard);
                let next_ptr: *mut LeafNode15<P> = Linker::unmark_ptr(next_raw);

                if next_ptr.is_null() {
                    return RouteLookupResult::NotFound;
                }

                leaf_ptr = next_ptr;
                hops += 1;
                continue;
            }

            if leaf.deleted_layer() {
                return RouteLookupResult::Retry;
            }

            let next_raw: *mut LeafNode15<P> = leaf.next_raw(guard);

            if Linker::is_marked(next_raw) {
                leaf.wait_for_split();
                continue;
            }

            let next_ptr: *mut LeafNode15<P> = Linker::unmark_ptr(next_raw);

            if !next_ptr.is_null() {
                // SAFETY: next_ptr is non-null, protected by guard.
                let next_leaf: &LeafNode15<P> = unsafe { &*next_ptr };

                if target_ikey >= next_leaf.ikey_bound() {
                    leaf_ptr = next_ptr;
                    hops += 1;
                    continue;
                }
            }

            return RouteLookupResult::Found(leaf_ptr);
        }
    }

    /// Walk B-link chain from `start` to find the leaf whose key range contains
    /// `target_ikey`, then search for a layer slot matching that ikey.
    fn find_layer_in_chain<P: LeafPolicy>(
        start: *mut LeafNode15<P>,
        target_ikey: u64,
        guard: &LocalGuard<'_>,
    ) -> RouteLookupResult<*mut u8> {
        let leaf_ptr: *mut LeafNode15<P> = match Self::walk_chain_to_ikey(start, target_ikey, guard)
        {
            RouteLookupResult::Found(ptr) => ptr,
            RouteLookupResult::Retry => return RouteLookupResult::Retry,
            RouteLookupResult::NotFound => return RouteLookupResult::NotFound,
            RouteLookupResult::HopLimit => return RouteLookupResult::HopLimit,
        };

        // SAFETY: leaf_ptr is valid, protected by guard.
        let leaf: &LeafNode15<P> = unsafe { &*leaf_ptr };

        // OCC snapshot to read layer slot.
        let version: u32 = match leaf.version().try_stable() {
            Some(v) => v,
            None => return RouteLookupResult::Retry,
        };

        let slot: Option<usize> = Self::find_layer_slot(leaf, target_ikey);

        if leaf.version().has_changed_or_locked(version) {
            return RouteLookupResult::Retry;
        }

        let Some(kp) = slot else {
            return RouteLookupResult::NotFound;
        };

        let layer_ptr: *mut u8 = leaf.load_layer_raw(kp);

        if leaf.version().has_changed_or_locked(version) {
            return RouteLookupResult::Retry;
        }

        if layer_ptr.is_null() {
            return RouteLookupResult::NotFound;
        }

        // Verify the layer root is not already deleted.
        // SAFETY: layer_ptr points to a valid node, protected by guard.
        #[expect(clippy::cast_ptr_alignment, reason = "NodeVersion is first field")]
        let layer_version: &NodeVersion = unsafe { &*layer_ptr.cast::<NodeVersion>() };

        if layer_version.is_deleted() {
            return RouteLookupResult::Retry;
        }

        RouteLookupResult::Found(layer_ptr)
    }
}

// ============================================================================
//  Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    use std::ptr;

    #[test]
    fn test_queue_basic_operations() {
        let queue: CoalesceQueue = CoalesceQueue::new();

        assert!(queue.is_empty());
        assert_eq!(queue.len(), 0);
        assert_eq!(queue.deferred_len(), 0);
        assert_eq!(queue.abandoned(), 0);

        queue.schedule_chain(ptr::null_mut(), 100);
        queue.schedule_chain(ptr::null_mut(), 200);

        assert!(!queue.is_empty());
        assert_eq!(queue.len(), 2);

        queue.clear();
        assert!(queue.is_empty());
    }

    #[test]
    fn test_debug_impl() {
        let queue = CoalesceQueue::new();
        queue.schedule_chain(ptr::null_mut(), 42);

        let debug_str = format!("{queue:?}");
        assert!(debug_str.contains("CoalesceQueue"));
        assert!(debug_str.contains("chains"));
        assert!(debug_str.contains("deferred_chains"));
        assert!(debug_str.contains("abandoned"));
    }

    #[test]
    fn test_schedule_sublayer() {
        let queue = CoalesceQueue::new();

        queue.schedule_sublayer(vec![0x1234, 0x5678]);
        assert_eq!(queue.len(), 1);

        queue.schedule_chain(ptr::null_mut(), 300);
        assert_eq!(queue.len(), 2);

        queue.clear();
        assert!(queue.is_empty());
    }

    #[test]
    fn test_requeue_count_limit() {
        let entry: ChainEntry = ChainEntry {
            leaf_ptr: ptr::null_mut(),
            ikey_bound: 42,
            requeue_count: 0,
        };

        assert_eq!(entry.requeue_count, 0);

        let sub_entry: SublayerEntry = SublayerEntry {
            route: vec![1, 2, 3],
            requeue_count: 0,
        };
        assert_eq!(sub_entry.requeue_count, 0);

        let deferred_entry: DeferredChainEntry = DeferredChainEntry {
            ikey_bound: 42,
            requeue_count: 5,
        };
        assert_eq!(deferred_entry.requeue_count, 5);

        const {
            assert!(
                MAX_REQUEUE_COUNT >= 5,
                "MAX_REQUEUE_COUNT should be at least 5"
            );
        }
        const {
            assert!(
                MAX_REQUEUE_COUNT <= 20,
                "MAX_REQUEUE_COUNT should be at most 20"
            );
        }
    }

    #[test]
    fn test_deferred_queue_operations() {
        let queue = CoalesceQueue::new();

        // Push to deferred queues directly.
        queue.deferred_chains.push(DeferredChainEntry {
            ikey_bound: 100,
            requeue_count: 1,
        });
        queue.deferred_sublayers.push(SublayerEntry {
            route: vec![0x42],
            requeue_count: 2,
        });

        assert_eq!(queue.deferred_len(), 2);
        assert_eq!(queue.len(), 2);
        assert!(!queue.is_empty());

        // Drain moves sublayers to main queue.
        queue.drain_deferred();
        assert_eq!(queue.sublayers.len(), 1);
        // deferred chains stay (processed separately).
        assert_eq!(queue.deferred_chains.len(), 1);
        assert_eq!(queue.deferred_sublayers.len(), 0);

        queue.clear();
        assert!(queue.is_empty());
    }

    #[test]
    fn test_abandoned_counter() {
        let queue = CoalesceQueue::new();
        assert_eq!(queue.abandoned(), 0);

        queue.abandoned.fetch_add(1, AtomicOrdering::Relaxed);
        assert_eq!(queue.abandoned(), 1);

        queue.abandoned.fetch_add(3, AtomicOrdering::Relaxed);
        assert_eq!(queue.abandoned(), 4);
    }

    #[test]
    fn test_requeue_chain_exhaustion() {
        let queue = CoalesceQueue::new();

        // Below limit: pushes to deferred.
        Coalesce::requeue_chain(&queue, 42, 5);
        assert_eq!(queue.deferred_chains.len(), 1);

        // At limit: increments abandoned, does not push.
        Coalesce::requeue_chain(&queue, 42, MAX_REQUEUE_COUNT);
        assert_eq!(queue.deferred_chains.len(), 1);
        assert_eq!(queue.abandoned(), 1);
    }

    #[test]
    fn test_requeue_sublayer_exhaustion() {
        let queue = CoalesceQueue::new();

        // Below limit: pushes to deferred.
        Coalesce::requeue_sublayer(&queue, vec![1, 2], 5);
        assert_eq!(queue.deferred_sublayers.len(), 1);

        // At limit: increments abandoned, does not push.
        Coalesce::requeue_sublayer(&queue, vec![1, 2], MAX_REQUEUE_COUNT);
        assert_eq!(queue.deferred_sublayers.len(), 1);
        assert_eq!(queue.abandoned(), 1);
    }

    // ====================================================================
    //  Deferred chain transient retry tests
    // ====================================================================

    /// Verify that a deferred chain entry with a stale `ikey_bound` (no matching
    /// leaf in the tree) is consumed without requeuing or incrementing abandoned.
    /// This is the `NotFound` path in `find_chain_leaf`.
    #[test]
    fn test_deferred_chain_stale_entry_dropped() {
        use crate::tree::MassTree15;

        let tree: MassTree15<u64> = MassTree15::new();

        // Insert and remove a key to create a non-trivial tree, then clear
        // the coalesce queue so we control what's in it.
        tree.insert(b"hello", 1);
        let _ = tree.remove(b"hello");
        let guard = tree.guard();
        tree.process_coalesce(&guard);
        drop(guard);

        // Manually push a deferred chain entry with an ikey_bound that does
        // not match any empty+queued leaf (stale entry).
        tree.coalesce_queue()
            .deferred_chains
            .push(DeferredChainEntry {
                ikey_bound: 0xDEAD_BEEF,
                requeue_count: 3,
            });

        assert_eq!(tree.coalesce_queue().deferred_chains.len(), 1);

        // Process. The entry should hit NotFound and be consumed.
        let guard = tree.guard();
        let processed = tree.process_coalesce(&guard);

        assert!(processed >= 1, "stale deferred entry should be processed");
        assert_eq!(
            tree.coalesce_queue().deferred_chains.len(),
            0,
            "stale entry should not be requeued"
        );
        assert_eq!(
            tree.coalesce_abandoned(),
            0,
            "stale entry should not increment abandoned"
        );
    }

    /// Verify that repeated requeuing of a deferred chain entry eventually
    /// exhausts the retry budget and increments the abandoned counter exactly
    /// once. This exercises the `requeue_chain` path that transient retries rely on
    /// for transient failures.
    #[test]
    fn test_deferred_chain_retry_exhaustion_increments_abandoned() {
        let queue = CoalesceQueue::new();

        // Simulate repeated transient failures by calling requeue_chain with
        // incrementing requeue_count until exhaustion.
        for count in 0..MAX_REQUEUE_COUNT {
            Coalesce::requeue_chain(&queue, 0x42, count);
        }

        // All calls below the limit should have pushed to deferred_chains.
        assert_eq!(
            queue.deferred_chains.len(),
            MAX_REQUEUE_COUNT as usize,
            "each requeue below limit should push to deferred_chains"
        );
        assert_eq!(queue.abandoned(), 0, "no abandonment below limit");

        // One more at the limit triggers abandonment.
        Coalesce::requeue_chain(&queue, 0x42, MAX_REQUEUE_COUNT);
        assert_eq!(
            queue.abandoned(),
            1,
            "exhaustion should increment abandoned exactly once"
        );

        // The deferred queue length should not increase.
        assert_eq!(
            queue.deferred_chains.len(),
            MAX_REQUEUE_COUNT as usize,
            "exhausted entry should not be requeued"
        );
    }

    /// Verify that `find_chain_leaf` returns the correct `RouteLookupResult`
    /// variant for a leaf that exists but does not match the expected state
    /// (not empty, not queued, or wrong `ikey_bound`). This should yield
    /// `NotFound`, not a transient failure.
    #[test]
    fn test_deferred_chain_mismatched_leaf_is_not_found() {
        use crate::tree::MassTree15;

        let tree: MassTree15<u64> = MassTree15::new();

        // Insert a key so the tree has a leaf with a known ikey_bound.
        tree.insert(b"testkey1", 100);

        // The leaf has data (size > 0) and is not queued, so find_chain_leaf
        // should return NotFound even though the walk finds the leaf.
        // Push a deferred entry pointing at the first leaf's ikey_bound.
        // Since the leaf is non-empty, the validation in find_chain_leaf
        // should reject it.
        let ikey_bound: u64 = u64::from_be_bytes(*b"testkey1");
        tree.coalesce_queue()
            .deferred_chains
            .push(DeferredChainEntry {
                ikey_bound,
                requeue_count: 0,
            });

        let guard = tree.guard();
        let processed = tree.process_coalesce(&guard);

        assert!(processed >= 1);
        assert_eq!(
            tree.coalesce_queue().deferred_chains.len(),
            0,
            "mismatched entry should be consumed (NotFound), not requeued"
        );
        assert_eq!(tree.coalesce_abandoned(), 0);

        // Original key should still be retrievable.
        assert_eq!(tree.get(b"testkey1"), Some(100));
    }
}
