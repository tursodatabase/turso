use std::{
    cmp::Ordering,
    marker::PhantomData,
    ptr as StdPtr,
    sync::atomic::{AtomicPtr, Ordering as AtomicOrdering},
};

use seize::{Collector, Guard, LocalGuard};

use crate::{
    BatchedRetire, Linker, MassTreeGeneric, TreeAllocator, TreePermutation,
    hints::{likely, unlikely},
    internode::InternodeNode,
    key::Key,
    leaf_trait::TreeLeafNode,
    leaf15::{KSUF_KEYLENX, LAYER_KEYLENX, LeafNode15, MODSTATE_INSERT},
    nodeversion::{LockGuard, NodeVersion},
    policy::LeafPolicy,
    prefetch::prefetch_read,
    shard_counter::ShardedCounter,
    tree::{
        InsertError, InsertSearchResultGeneric,
        coalesce::{Coalesce, CoalesceQueue},
        remove::NodeCleaner,
        split::Propagation,
    },
};

use super::remove::RemoveError;

mod batch;
mod entry;
mod insert;
mod insert_strategy;
mod layer;
mod optimistic_reads;
mod search;
mod split;

#[cfg(feature = "debug-print")]
mod print;

// Re-export batch types
pub use batch::{BatchEntry, BatchInsertResult};
pub use entry::{Entry, OccupiedEntry, VacantEntry};

// ============================================================================
//  Shared Types
// ============================================================================

/// Result of finding a usable slot for insertion.
pub enum FindSlotResult {
    /// Found a usable slot at the given physical index.
    Found { slot: usize, back_offset: usize },

    /// No usable slot available - the leaf must be split.
    NeedsSplit,
}

/// Errors from membership validation.
pub enum MembershipError {
    /// A split is in progress on this leaf.
    SplitInProgress,

    /// Key's ikey is >= the next sibling's lower bound.
    KeyMovedToSibling,

    /// Key's ikey is < this leaf's lower bound (`ikey_bound()`).
    KeyBelowLowerBound,
}

impl<P, A> MassTreeGeneric<P, A>
where
    P: LeafPolicy,
    A: TreeAllocator<P>,
{
    /// Default retirement batch size. Larger values reduce the frequency of
    /// `sys_membarrier` syscalls at the cost of slightly delayed reclamation.
    /// 256 is 8 times the [`seize`] default (32) and amortizes well under write-heavy
    /// workloads while adding negligible peak memory.
    const DEFAULT_RETIRE_BATCH_SIZE: usize = 256;

    /// Create a new empty `MassTreeGeneric` with the given allocator.
    #[must_use]
    pub fn with_allocator(allocator: A) -> Self {
        Self::with_allocator_batch_size(allocator, Some(Self::DEFAULT_RETIRE_BATCH_SIZE))
    }

    /// Create a new empty `MassTreeGeneric` with custom batch size.
    #[must_use]
    pub fn with_allocator_batch_size(allocator: A, batch_size: Option<usize>) -> Self {
        let root_ptr: *mut LeafNode15<P> = allocator.alloc_leaf_direct(true, false);

        let collector: Collector = match batch_size {
            Some(size) => Collector::new().batch_size(size),
            None => Collector::new(),
        };

        Self {
            collector,
            allocator,
            root_ptr: AtomicPtr::new(root_ptr.cast::<u8>()),
            count: ShardedCounter::new(),
            coalesce_queue: CoalesceQueue::new(),
            _marker: PhantomData,
        }
    }

    /// Enter a protected region and return a guard.
    #[must_use]
    #[inline(always)]
    pub fn guard(&self) -> LocalGuard<'_> {
        self.collector.enter()
    }

    /// Flush pending retirements to accelerate memory reclamation.
    #[inline(always)]
    pub fn flush(&self, guard: &LocalGuard<'_>) {
        self.verify_guard(guard);
        BatchedRetire::flush(guard);
    }

    /// Perform maintenance: reclaim retired memory and clean up empty leaves.
    ///
    /// Call periodically during long-running single-threaded workloads or
    /// between batch operations to reclaim memory from deleted entries.
    /// Requires exclusive access to the guard (no nested guards active).
    ///
    /// Internally calls `guard.flush()` + `guard.refresh()` + coalesce processing.
    #[inline]
    pub fn maintenance(&self, guard: &mut LocalGuard<'_>) {
        self.verify_guard(guard);
        guard.flush();
        guard.refresh();
        Coalesce::process_all::<P, A>(self, guard);
    }

    /// Get the approximate number of keys in the tree.
    #[must_use]
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.count.load()
    }

    /// Check if the tree is empty.
    #[must_use]
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    // ========================================================================
    //  Lazy Coalescing (Empty Leaf Cleanup)
    // ========================================================================

    /// Get the number of empty leaves pending cleanup.
    #[must_use]
    #[inline]
    pub fn pending_coalesce(&self) -> usize {
        self.coalesce_queue.len()
    }

    /// Process all pending empty leaf removals.
    #[inline]
    pub fn process_coalesce(&self, guard: &LocalGuard<'_>) -> usize {
        self.verify_guard(guard);

        Coalesce::process_all::<P, A>(self, guard)
    }

    /// Process up to `limit` pending empty leaf removals.
    #[inline]
    pub fn process_coalesce_batch(&self, guard: &LocalGuard<'_>, limit: usize) -> usize {
        self.verify_guard(guard);
        Coalesce::process_batch::<P, A>(self, guard, limit)
    }

    /// Number of coalesce entries permanently abandoned.
    /// Non-zero values indicate cleanup debt from sustained contention.
    #[must_use]
    #[inline]
    pub fn coalesce_abandoned(&self) -> usize {
        self.coalesce_queue.abandoned()
    }

    /// Number of coalesce entries deferred to the next maintenance call.
    #[must_use]
    #[inline]
    pub fn coalesce_deferred(&self) -> usize {
        self.coalesce_queue.deferred_len()
    }

    // ========================================================================
    //  Internal Helpers
    // ========================================================================

    /// Access the coalesce queue.
    #[inline(always)]
    pub(crate) const fn coalesce_queue(&self) -> &CoalesceQueue {
        &self.coalesce_queue
    }

    /// Access the allocator.
    #[inline(always)]
    pub(crate) const fn allocator(&self) -> &A {
        &self.allocator
    }

    /// Load the root pointer atomically.
    #[inline(always)]
    pub(crate) fn load_root_ptr_generic(&self, _guard: &LocalGuard<'_>) -> *const u8 {
        self.root_ptr.load(AtomicOrdering::Acquire)
    }

    /// Returns the root pointer for debugging/testing purposes.
    pub fn debug_root_ptr(&self, _guard: &LocalGuard<'_>) -> *mut u8 {
        self.root_ptr.load(AtomicOrdering::Acquire)
    }

    /// Verify that the guard was created from this tree's collector.
    ///
    /// # Panics
    ///
    /// Panics if the guard's collector does not match this tree's collector.
    /// This indicates the caller passed a guard from a different tree, which
    /// is a bug that could lead to use-after-free.
    #[inline(always)]
    pub(crate) fn verify_guard(&self, guard: &LocalGuard<'_>) {
        if *guard.collector() != self.collector {
            Self::guard_collector_mismatch();
        }
    }

    #[cold]
    #[inline(never)]
    fn guard_collector_mismatch() -> ! {
        panic!(
            "Guard was created from a different collector. \
             Use tree.guard() to create guards for this tree."
        );
    }

    /// Follow parent pointers to find the actual layer root.
    #[inline(always)]
    #[expect(clippy::unused_self, reason = "Method signature pattern")]
    fn maybe_parent_generic(&self, mut node: *const u8) -> *const u8 {
        loop {
            // SAFETY: node is valid, both types have NodeVersion as first field
            #[expect(clippy::cast_ptr_alignment, reason = "proper alignment")]
            let version: &NodeVersion = unsafe { &*(node.cast::<NodeVersion>()) };

            // SAFETY: Dead-code traversal helper, no concurrent retirement of parent pointers.
            let parent = if version.is_leaf() {
                // SAFETY: version.is_leaf() confirmed
                let leaf: &LeafNode15<P> = unsafe { &*(node.cast::<LeafNode15<P>>()) };
                unsafe { leaf.parent_unguarded() }
            } else {
                // SAFETY: !version.is_leaf() confirmed
                let inode: &InternodeNode = unsafe { &*(node.cast::<InternodeNode>()) };
                unsafe { inode.parent_unguarded() }
            };

            if parent.is_null() {
                return node;
            }

            node = parent;
        }
    }

    /// Single-step parent traversal
    #[inline(always)]
    #[expect(clippy::unused_self, reason = "Method signature pattern")]
    fn maybe_parent_one_step(&self, node: *const u8) -> *const u8 {
        // SAFETY: node is valid, both types have NodeVersion as first field
        #[expect(clippy::cast_ptr_alignment, reason = "proper alignment")]
        let version: &NodeVersion = unsafe { &*(node.cast::<NodeVersion>()) };

        // SAFETY: Dead-code traversal helper, no concurrent retirement of parent pointers.
        let parent: *mut u8 = if version.is_leaf() {
            let leaf: &LeafNode15<P> = unsafe { &*(node.cast::<LeafNode15<P>>()) };

            // SAFETY: version.is_leaf() confirmed
            unsafe { leaf.parent_unguarded() }
        } else {
            let inode: &InternodeNode = unsafe { &*(node.cast::<InternodeNode>()) };

            // SAFETY: !version.is_leaf() confirmed
            unsafe { inode.parent_unguarded() }
        };

        if parent.is_null() {
            node
        } else {
            parent.cast_const()
        }
    }

    /// Double-buffer traversal from root to leaf.
    #[expect(clippy::indexing_slicing, reason = "sense is always 0 or 1")]
    #[expect(
        clippy::too_many_lines,
        reason = "hot-path tree traversal, logically cohesive and performance-critical"
    )]
    pub(crate) fn reach_leaf_concurrent_generic(
        &self,
        start: *const u8,
        key: &Key<'_>,
        _is_sublayer: bool,
        guard: &LocalGuard<'_>,
    ) -> *mut LeafNode15<P> {
        use crate::ksearch::upper_bound_internode_generic;
        use crate::prefetch::prefetch_read;

        let target_ikey: u64 = key.ikey();

        let mut n: [*const u8; 2] = [StdPtr::null(); 2];
        let mut v: [u32; 2] = [0; 2];

        #[expect(unused_assignments, reason = "sense is set at top of retry loop")]
        let mut sense: usize = 0;

        'retry: loop {
            sense = 0;
            n[sense] = start;

            loop {
                // SAFETY: n[sense] is valid, NodeVersion is first field
                #[expect(clippy::cast_ptr_alignment, reason = "proper alignment")]
                let version: &NodeVersion = unsafe { &*(n[sense].cast::<NodeVersion>()) };
                v[sense] = version.stable();

                if version.is_root() || version.is_deleted() {
                    break;
                }

                // Walk up to parent
                n[sense] = self.maybe_parent_one_step(n[sense]);
            }

            loop {
                // SAFETY: n[sense] is valid, NodeVersion is first field
                #[expect(clippy::cast_ptr_alignment, reason = "proper alignment")]
                let version: &NodeVersion = unsafe { &*(n[sense].cast::<NodeVersion>()) };

                v[sense] = match version.try_stable() {
                    Some(v) => v,
                    None => continue 'retry, // Node is locked, restart from root
                };

                // Check if we've reached a leaf
                if version.is_leaf() {
                    return n[sense].cast_mut().cast::<LeafNode15<P>>();
                }

                // It's an internode - traverse down
                // SAFETY: !is_leaf() confirmed above
                let inode: &InternodeNode = unsafe { &*(n[sense].cast::<InternodeNode>()) };

                prefetch_read(
                    StdPtr::from_ref::<InternodeNode>(inode)
                        .cast::<u8>()
                        .wrapping_add(64),
                );

                // Binary/linear search for child pointer
                let child_idx: usize =
                    upper_bound_internode_generic::<InternodeNode>(target_ikey, inode);

                // DEBUG: Trace routing path
                #[cfg(feature = "debug-routing")]
                {
                    let nkeys: usize = inode.nkeys();
                    eprintln!(
                        "[ROUTE] inode={:p} height={} nkeys={} target_ikey={:016x} -> child_idx={}",
                        inode,
                        inode.height(),
                        nkeys,
                        target_ikey,
                        child_idx
                    );

                    // Print separator keys around the chosen child
                    if child_idx > 0 && child_idx <= nkeys {
                        let left_sep: u64 = inode.ikey(child_idx - 1);
                        eprintln!("        left_sep[{}]={:016x}", child_idx - 1, left_sep);
                    }

                    if child_idx < nkeys {
                        let right_sep: u64 = inode.ikey(child_idx);
                        eprintln!("        right_sep[{child_idx}]={right_sep:016x}");
                    }
                }

                let other: usize = sense ^ 1;
                let child: *mut u8 = inode.child(child_idx, guard);
                n[other] = child.cast_const();

                if unlikely(child.is_null()) {
                    continue 'retry;
                }

                prefetch_read(child);

                if !inode.children_are_leaves() {
                    // SAFETY: children_are_leaves() is false, so child is an internode
                    let child_inode: &InternodeNode = unsafe { &*(child.cast::<InternodeNode>()) };
                    prefetch_read(
                        StdPtr::from_ref::<InternodeNode>(child_inode)
                            .cast::<u8>()
                            .wrapping_add(64),
                    );

                    // Prefetch is safe even for null/invalid pointers (no-op on x86/ARM)
                    let grandchild_ptr: *mut u8 = child_inode.child(0, guard);
                    prefetch_read(grandchild_ptr);
                }

                // SAFETY: child is non-null and valid
                #[expect(clippy::cast_ptr_alignment, reason = "proper alignment")]
                let child_version: &NodeVersion = unsafe { &*(child.cast::<NodeVersion>()) };
                v[other] = child_version.stable();

                if likely(!inode.version().has_changed(v[sense])) {
                    sense = other;
                    continue;
                }

                let old_v: u32 = v[sense];
                v[sense] = inode.version().stable();

                if unlikely(inode.version().has_split(old_v)) {
                    let nkeys: usize = inode.nkeys();

                    if nkeys > 0 {
                        let last_key: u64 = inode.ikey(nkeys - 1);
                        if target_ikey > last_key {
                            continue 'retry;
                        }
                    }
                }
            }
        }
    }

    /// Compare a key with the last key in a leaf, retrying until stable.
    #[inline]
    fn stable_last_key_compare(leaf: &LeafNode15<P>, key: &Key<'_>, mut version: u32) -> Ordering {
        loop {
            let perm: <LeafNode15<P> as TreeLeafNode<P>>::Perm = leaf.permutation();
            let n: usize = perm.size();

            // If empty, return Greater (key should look elsewhere)
            // If non-empty, compare with last slot in permutation
            let cmp: Ordering = if n == 0 {
                Ordering::Greater
            } else {
                let last_slot: usize = perm.get(n - 1);
                let slot_ikey: u64 = leaf.ikey(last_slot);
                let slot_keylenx: u8 = leaf.keylenx(last_slot);
                key.compare(slot_ikey, slot_keylenx as usize)
            };

            // Check if version is still valid (common case: unchanged)
            if likely(!leaf.version().has_changed(version)) {
                return cmp;
            }

            // Version changed, get new stable version and retry
            version = leaf.version().stable();
        }
    }

    /// Advance to correct leaf via B-link after version change detected.
    #[expect(clippy::unused_self, reason = "API Consistency")]
    fn advance_to_key_generic<'a>(
        &'a self,
        mut leaf: &'a LeafNode15<P>,
        key: &Key<'_>,
        old_version: u32,
        guard: &LocalGuard<'_>,
    ) -> (&'a LeafNode15<P>, u32) {
        let key_ikey: u64 = key.ikey();
        let mut version: u32 = leaf.version().stable();

        let needs_walk: bool = if unlikely(leaf.version().has_split(old_version)) {
            Self::stable_last_key_compare(leaf, key, version) == Ordering::Greater
        } else if unlikely(leaf.version().is_splitting()) {
            version = leaf.version().stable();
            Self::stable_last_key_compare(leaf, key, version) == Ordering::Greater
        } else {
            false
        };

        if !needs_walk {
            return (leaf, version);
        }

        // Walk B-link chain to find correct leaf (common case: not deleted)
        while likely(!leaf.version().is_deleted()) {
            let next_raw: *mut LeafNode15<P> = leaf.next_raw(guard);

            // Check for marked pointer (split in progress OR deleted node)
            if Linker::is_marked(next_raw) {
                leaf.wait_for_split();
                continue;
            }

            let next_ptr: *mut LeafNode15<P> = Linker::unmark_ptr(next_raw);
            if next_ptr.is_null() {
                break;
            }

            // SAFETY: next_ptr protected by guard
            let next: &LeafNode15<P> = unsafe { &*next_ptr };
            let next_bound: u64 = next.ikey_bound();

            if key_ikey >= next_bound {
                // Key belongs in next leaf or further
                leaf = next;
                version = leaf.version().stable();
                continue;
            }

            // Key belongs in current leaf
            break;
        }

        (leaf, version)
    }

    /// Maximum B-link hops before giving up and signaling re-descent.
    const MAX_BLINK_HOPS: usize = 128;

    /// Advance to correct leaf via B-link (generic version).
    /// Used by insert path before locking.
    #[expect(clippy::unused_self, reason = "API Consistency")]
    fn advance_to_key_by_bound_generic(
        &self,
        mut leaf_ptr: *mut LeafNode15<P>,
        key: &Key<'_>,
        guard: &LocalGuard<'_>,
    ) -> (*mut LeafNode15<P>, bool) {
        let key_ikey: u64 = key.ikey();
        let mut hops: usize = 0;

        // SAFETY: leaf_ptr is valid, protected by guard
        let mut leaf: &LeafNode15<P> = unsafe { &*leaf_ptr };

        let version: &NodeVersion = leaf.version();

        if version.is_splitting() {
            let _ = version.stable();
        }

        loop {
            if hops >= Self::MAX_BLINK_HOPS {
                return (leaf_ptr, true);
            }

            if unlikely(leaf.version().is_deleted()) {
                let next_raw: *mut LeafNode15<P> = leaf.next_raw(guard);
                let next_ptr: *mut LeafNode15<P> = Linker::unmark_ptr(next_raw);

                if next_ptr.is_null() {
                    break;
                }

                leaf_ptr = next_ptr;

                // SAFETY: next_ptr is valid, protected by guard
                leaf = unsafe { &*leaf_ptr };
                hops += 1;
                continue;
            }

            let next_raw: *mut LeafNode15<P> = leaf.next_raw(guard);

            if Linker::is_marked(next_raw) {
                leaf.wait_for_split();
                continue;
            }

            let next_ptr: *mut LeafNode15<P> = Linker::unmark_ptr(next_raw);

            if next_ptr.is_null() {
                break;
            }

            // SAFETY: next_ptr is valid
            let next: &LeafNode15<P> = unsafe { &*next_ptr };
            let next_bound: u64 = next.ikey_bound();

            if key_ikey >= next_bound {
                let next_next_raw: *mut LeafNode15<P> = next.next_raw(guard);
                let next_next_ptr: *mut LeafNode15<P> = Linker::unmark_ptr(next_next_raw);
                let prefetch_base: *const u8 = next_next_ptr.cast::<u8>();

                prefetch_read(prefetch_base);
                prefetch_read(prefetch_base.wrapping_add(64));

                leaf_ptr = next_ptr;
                leaf = next;
                hops += 1;
                continue;
            }

            break;
        }

        (leaf_ptr, false)
    }

    // ========================================================================
    //  Generic Locked Insert Path
    // ========================================================================

    /// Insert a key-value pair.
    ///
    /// Creates a guard internally. For bulk operations, prefer
    /// [`insert_with_guard`](Self::insert_with_guard) to amortize guard creation cost.
    ///
    /// Returns an owned clone of the old value if the key already existed.
    /// The internal EBR guard is released before returning, so the value
    /// is cloned to ensure independent ownership.
    ///
    /// # Panics
    ///
    /// Panics on internal tree corruption (should not happen in normal operation).
    #[inline]
    pub fn insert(&self, key: &[u8], value: P::Value) -> Option<P::Value>
    where
        P::Value: Clone,
    {
        let guard = self.guard();

        if P::CAN_WRITE_THROUGH {
            // Optimized path: deferred allocation with write-through for updates.
            // Avoids Box::new + defer_retire on the hot update path.
            let mut key: Key<'_> = Key::new(key);
            match self.insert_concurrent_value(&mut key, value, &guard) {
                Ok(result) => result,
                Err(e) => {
                    panic!(
                        "Insert failed unexpectedly: {e:?}. This indicates a bug in the tree implementation."
                    );
                }
            }
        } else {
            let result = self.insert_with_guard(key, value, &guard);
            result.map(|output| P::clone_value_from_output(&output))
        }
    }

    /// Insert a key-value pair using an explicit guard.
    ///
    /// This is the main public insert API for `MassTreeGeneric`.
    ///
    /// # Panics
    ///
    /// Panics on internal tree corruption (should not happen in normal operation).
    pub fn insert_with_guard(
        &self,
        key: &[u8],
        value: P::Value,
        guard: &LocalGuard<'_>,
    ) -> Option<P::Output> {
        self.verify_guard(guard);
        let mut key: Key<'_> = Key::new(key);
        let output: <P as LeafPolicy>::Output = P::into_output(value);

        // Internal errors indicate bugs, not recoverable conditions
        match self.insert_concurrent_generic(&mut key, output, guard) {
            Ok(result) => result,

            Err(e) => {
                panic!(
                    "Insert failed unexpectedly: {e:?}. This indicates a bug in the tree implementation."
                );
            }
        }
    }

    /// Internal: Insert with a pre-created output value
    ///
    /// Used by entry API to avoid double traversal, the output is created
    /// before insertion so it can be cloned and returned without re-fetching.
    ///
    /// # Panics
    ///
    /// Panics on internal tree corruption.
    pub(crate) fn insert_output_with_guard(
        &self,
        key: &[u8],
        output: P::Output,
        guard: &LocalGuard<'_>,
    ) -> Option<P::Output> {
        self.verify_guard(guard);

        let mut key: Key<'_> = Key::new(key);

        match self.insert_concurrent_generic(&mut key, output, guard) {
            Ok(result) => result,

            Err(e) => {
                panic!(
                    "Insert failed unexpectedly: {e:?}. This indicates a bug in the tree implementation."
                );
            }
        }
    }

    /// Insert a key-value pair using an explicit guard, returning the old value.
    ///
    /// Unlike `insert_with_guard` (which returns `Option<P::Output>`), this
    /// returns `Option<P::Value>` directly. For policies with `CAN_WRITE_THROUGH`
    /// (V <= 8 bytes), this avoids Box allocation and EBR retirement on updates.
    ///
    /// # Panics
    ///
    /// Panics on internal tree corruption.
    pub fn insert_value_with_guard(
        &self,
        key: &[u8],
        value: P::Value,
        guard: &LocalGuard<'_>,
    ) -> Option<P::Value>
    where
        P::Value: Clone,
    {
        self.verify_guard(guard);
        let mut key: Key<'_> = Key::new(key);

        if P::CAN_WRITE_THROUGH {
            // Optimized path: deferred allocation with write-through for updates.
            match self.insert_concurrent_value(&mut key, value, guard) {
                Ok(result) => result,

                Err(e) => {
                    panic!(
                        "Insert failed unexpectedly: {e:?}. This indicates a bug in the tree implementation."
                    );
                }
            }
        } else {
            // Fallback: standard path, extract value from output.
            let output: P::Output = P::into_output(value);

            match self.insert_concurrent_generic(&mut key, output, guard) {
                Ok(result) => result.map(|o| P::clone_value_from_output(&o)),

                Err(e) => {
                    panic!(
                        "Insert failed unexpectedly: {e:?}. This indicates a bug in the tree implementation."
                    );
                }
            }
        }
    }

    // ========================================================================
    //  Remove Operations
    // ========================================================================

    /// Remove a key from the tree.
    ///
    /// Returns an owned clone of the removed value if the key existed.
    /// The internal EBR guard is released before returning, so the value
    /// is cloned to ensure independent ownership.
    ///
    /// # Example
    ///
    /// ```rust
    /// use masstree::MassTree15;
    ///
    /// let tree: MassTree15<u64> = MassTree15::new();
    /// tree.insert(b"key", 42);
    ///
    /// let removed = tree.remove(b"key").unwrap();
    /// assert_eq!(removed, Some(42));
    ///
    /// let not_found = tree.remove(b"key").unwrap();
    /// assert_eq!(not_found, None);
    /// ```
    ///
    /// # Errors
    ///
    /// Returns `RemoveError::RetryLimitExceeded` if the operation cannot
    /// complete after the retry limit (extremely rare, indicates contention).
    pub fn remove(&self, key: &[u8]) -> Result<Option<P::Value>, RemoveError>
    where
        P::Value: Clone,
    {
        let guard: LocalGuard<'_> = self.guard();
        let result: Option<P::Output> = self.remove_with_guard(key, &guard)?;

        Ok(result.map(|output: P::Output| P::clone_value_from_output(&output)))
    }

    /// Remove a key using an existing guard.
    ///
    /// # Errors
    /// If fails to remove
    pub fn remove_with_guard(
        &self,
        key: &[u8],
        guard: &LocalGuard<'_>,
    ) -> Result<Option<P::Output>, RemoveError> {
        self.verify_guard(guard);
        NodeCleaner::remove_concurrent_generic(self, key, guard)
    }

    /// Decrement the entry count after successful removal.
    ///
    /// Called by `finish_remove_generic` after updating the permutation.
    #[inline(always)]
    pub(crate) fn dec_count(&self) {
        self.count.decrement();
    }

    /// Assign a value to a slot in a locked leaf.
    #[inline(always)]
    #[expect(clippy::unused_self, reason = "API Consistency")]
    #[expect(clippy::too_many_arguments, reason = "Internals")]
    fn assign_slot_generic(
        &self,
        leaf: &LeafNode15<P>,
        lock: &mut LockGuard<'_>,
        slot: usize,
        key: &Key<'_>,
        value: &P::Output,
        guard: &LocalGuard<'_>,
        pre_allocated: Option<Vec<u8>>,
    ) -> *mut u8 {
        debug_assert!(
            leaf.prev(guard).is_null() || key.ikey() >= leaf.ikey_bound(),
            "INSERT INVARIANT VIOLATION: key {:016x} < leaf.ikey_bound {:016x}",
            key.ikey(),
            leaf.ikey_bound()
        );

        let ikey: u64 = key.ikey();

        if leaf.modstate_relaxed() != MODSTATE_INSERT {
            lock.mark_insert();
            leaf.set_modstate_relaxed(MODSTATE_INSERT);
        }

        leaf.set_ikey_relaxed(slot, ikey);
        leaf.store_value_relaxed(slot, value);

        if key.has_suffix() {
            leaf.set_keylenx_relaxed(slot, KSUF_KEYLENX);

            match pre_allocated {
                Some(buffer) => unsafe {
                    leaf.assign_ksuf_prealloc(slot, key.suffix(), guard, buffer)
                },

                None => unsafe { leaf.assign_ksuf(slot, key.suffix(), guard) },
            }
        } else {
            // Inline key (0-8 bytes total, no suffix)
            #[expect(clippy::cast_possible_truncation, reason = "current_len() <= 8")]
            let keylenx: u8 = key.current_len() as u8;
            leaf.set_keylenx_relaxed(slot, keylenx);
            StdPtr::null_mut()
        }
    }

    // ========================================================================
    //  Entry API
    // ========================================================================

    /// Gets the given key's corresponding entry for in-place manipulation.
    ///
    /// # Differences from `HashMap::entry`
    ///
    /// - Requires a [`LocalGuard`] for concurrent access
    /// - Returns values by-value, not by mutable reference
    /// - `and_modify` takes a transform function `FnOnce(&Output) -> Value`
    /// - Key is borrowed (zero allocation for entry creation)
    /// - Provides fallible `try_*` variants
    /// # Example
    ///
    /// ```rust
    /// use masstree::{MassTree, Entry};
    ///
    /// let tree: MassTree<u64> = MassTree::new();
    /// let guard = tree.guard();
    ///
    /// // Insert if absent, returning the value
    /// let _value = tree.entry_with_guard(b"key", &guard).or_insert(42);
    ///
    /// // Increment counter, defaulting to 0
    /// let _value = tree.entry_with_guard(b"counter", &guard)
    ///     .and_modify(|v| *v + 1)
    ///     .or_insert(0);
    ///
    /// // Pattern matching
    /// match tree.entry_with_guard(b"maybe", &guard) {
    ///     Entry::Occupied(o) => {
    ///         let _val = o.remove();
    ///     }
    ///     Entry::Vacant(v) => {
    ///         v.insert(100);
    ///     }
    /// }
    /// ```
    #[inline]
    pub fn entry_with_guard<'t, 'e>(
        &'t self,
        key: &'e [u8],
        guard: &'e LocalGuard<'t>,
    ) -> Entry<'t, 'e, P, A>
    where
        P::Output: Clone,
    {
        self.verify_guard(guard);
        Entry::new(self, key, guard)
    }
}

// ============================================================================
//  Standard Collection Traits (FromIterator, Extend)
// ============================================================================

impl<P, A, K, V> FromIterator<(K, V)> for MassTreeGeneric<P, A>
where
    P: LeafPolicy<Value = V>,
    A: TreeAllocator<P> + Default,
    K: AsRef<[u8]>,
{
    /// Creates a tree from an iterator of key-value pairs.
    ///
    /// # Panics
    ///
    /// Panics if memory allocation fails during tree construction.
    /// This matches std collection behavior.
    ///
    /// # Example
    ///
    /// ```rust
    /// use masstree::MassTree;
    ///
    /// let data = vec![
    ///     (b"key1".to_vec(), 1u64),
    ///     (b"key2".to_vec(), 2u64),
    ///     (b"key3".to_vec(), 3u64),
    /// ];
    ///
    /// let tree: MassTree<u64> = data.into_iter().collect();
    ///
    /// assert_eq!(tree.len(), 3);
    /// ```
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
    {
        let allocator: A = A::default();
        let tree: Self = Self::with_allocator(allocator);
        let guard: LocalGuard<'_> = tree.guard();

        for (key, value) in iter {
            // Inserts are infallible (abort on OOM like std collections)
            tree.insert_with_guard(key.as_ref(), value, &guard);
        }

        drop(guard);

        tree
    }
}

impl<P, A, K, V> Extend<(K, V)> for MassTreeGeneric<P, A>
where
    P: LeafPolicy<Value = V>,
    A: TreeAllocator<P>,
    K: AsRef<[u8]>,
{
    /// Extends the tree with key-value pairs from an iterator.
    ///
    /// # Example
    ///
    /// ```rust
    /// use masstree::MassTree;
    ///
    /// let mut tree: MassTree<u64> = MassTree::new();
    /// tree.insert(b"existing", 0);
    ///
    /// // Can use byte literals directly, &[u8; N] implements AsRef<[u8]>
    /// tree.extend([
    ///     (b"key1" as &[u8], 1u64),
    ///     (b"key2" as &[u8], 2u64),
    /// ]);
    ///
    /// assert_eq!(tree.len(), 3);
    /// ```
    fn extend<I>(&mut self, iter: I)
    where
        I: IntoIterator<Item = (K, V)>,
    {
        let guard: LocalGuard<'_> = self.guard();

        for (key, value) in iter {
            // Skip failed inserts, Extend returns () so we can't propagate errors
            let _ = self.insert_with_guard(key.as_ref(), value, &guard);
        }
    }
}
