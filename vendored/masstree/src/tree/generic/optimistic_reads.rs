use std::ptr as StdPtr;

use super::{Key, LeafPolicy, LocalGuard, MassTreeGeneric, NodeVersion, TreeAllocator};

use crate::Permuter;
use crate::hints::unlikely;
use crate::leaf15::KSUF_KEYLENX;
use crate::leaf15::LAYER_KEYLENX;
use crate::leaf15::LeafNode15;
use crate::link::Linker;
use crate::ordering::READ_ORD;
use crate::policy::RefPolicy as RefLeafPolicy;
use crate::policy::ValueRef;
use crate::policy::atomic_read_value;
use crate::prefetch::prefetch_read;

mod get_guarded;

// ============================================================================
//  Version Validation Helpers
// ============================================================================

/// Result of OCC version validation after a read phase.
enum VersionCheck<P: LeafPolicy> {
    /// Version unchanged.
    Valid,

    /// Version changed but same leaf.
    RetrySearch { new_version: u32 },

    /// Leaf changed (split).
    RetryLeaf { new_leaf_ptr: *mut LeafNode15<P> },
}

// ============================================================================
//  LookupResult
// ============================================================================

/// Result of searching a leaf node for a key.
enum LookupResult {
    /// Found a terminal value at the given slot index.
    ValueSlot(usize),

    /// Found a layer pointer. Need to descend into sublayer.
    Layer(*mut u8),

    /// Key not found in this leaf.
    NotFound,
}

/// Result of twig-chain fast descent.
enum TwigDescentResult<Leaf> {
    /// Continue to `'leaf_loop` with the given leaf pointer.
    ContinueLeafLoop {
        layer_root: *const u8,
        leaf_ptr: *mut Leaf,
    },

    /// Restart `'layer_loop` with the given layer root.
    RestartLayerLoop {
        layer_root: *const u8,
        in_sublayer: bool,
    },

    /// Return `None` from `get_impl_multi_layer`.
    ReturnNone,
}

// ============================================================================
//  Search Helpers
// ============================================================================

/// Search a leaf for a key in multi-layer mode (keys > 8 bytes).
#[inline]
#[expect(
    clippy::collapsible_if,
    reason = "Collapsing these branches causes measurable benchmark regressions (branch prediction / code layout effects)"
)]
fn search_leaf_multi_layer<P>(leaf: &LeafNode15<P>, key: &Key<'_>) -> LookupResult
where
    P: LeafPolicy,
{
    // Acquire ordering on permutation synchronizes with writer's Release fence
    let perm: Permuter = leaf.permutation();
    let size: usize = perm.size();
    let target_ikey: u64 = key.ikey();

    #[expect(clippy::cast_possible_truncation, reason = "current_len() <= 8")]
    let search_keylenx: u8 = if key.has_suffix() {
        KSUF_KEYLENX
    } else {
        key.current_len() as u8
    };

    let needs_suffix_check: bool = key.has_suffix();

    let mut i: usize = 0;

    while i + 3 <= size {
        let s0: usize = perm.get(i);
        let s1: usize = perm.get(i + 1);
        let s2: usize = perm.get(i + 2);

        let ikey0: u64 = leaf.ikey_relaxed(s0);
        let ikey1: u64 = leaf.ikey_relaxed(s1);
        let ikey2: u64 = leaf.ikey_relaxed(s2);

        if ikey0 == target_ikey {
            if let Some(result) =
                check_slot_match(leaf, s0, search_keylenx, key, needs_suffix_check)
            {
                return result;
            }
        }

        if ikey1 == target_ikey {
            if let Some(result) =
                check_slot_match(leaf, s1, search_keylenx, key, needs_suffix_check)
            {
                return result;
            }
        }

        if ikey2 == target_ikey {
            if let Some(result) =
                check_slot_match(leaf, s2, search_keylenx, key, needs_suffix_check)
            {
                return result;
            }
        }

        i += 3;
    }

    while i < size {
        let slot: usize = perm.get(i);
        let slot_ikey: u64 = leaf.ikey_relaxed(slot);

        if slot_ikey == target_ikey {
            if let Some(result) =
                check_slot_match(leaf, slot, search_keylenx, key, needs_suffix_check)
            {
                return result;
            }
        }

        i += 1;
    }

    LookupResult::NotFound
}

/// Optimized slot match check with suffix-check bypass.
#[inline(always)]
fn check_slot_match<P>(
    leaf: &LeafNode15<P>,
    slot: usize,
    search_keylenx: u8,
    key: &Key<'_>,
    needs_suffix_check: bool,
) -> Option<LookupResult>
where
    P: LeafPolicy,
{
    let slot_keylenx: u8 = leaf.keylenx_relaxed(slot);

    if leaf.is_value_empty_relaxed(slot) {
        return None;
    }

    leaf.prefetch_value(slot);

    // Prefetch suffix storage (sidecar pointer + external bag) so data
    // is cache-warm by the time ksuf_equals runs below.
    if slot_keylenx == KSUF_KEYLENX {
        leaf.prefetch_suffix();
    }

    if slot_keylenx == search_keylenx {
        if needs_suffix_check
            && slot_keylenx == KSUF_KEYLENX
            && !leaf.ksuf_equals(slot, key.suffix())
        {
            return None;
        }

        return Some(LookupResult::ValueSlot(slot));
    }

    if needs_suffix_check && slot_keylenx >= LAYER_KEYLENX {
        let layer_ptr: *mut u8 = leaf.load_layer_raw(slot);

        if layer_ptr.is_null() {
            return None;
        }

        // Prefetch the next-layer node header (NodeVersion + first cache line)
        // to hide traversal latency before descend_twig_chain runs.
        prefetch_read(layer_ptr);

        return Some(LookupResult::Layer(layer_ptr));
    }

    None
}

// ============================================================================
//  Helper Functions
// ============================================================================

impl<P, A> MassTreeGeneric<P, A>
where
    P: LeafPolicy,
    A: TreeAllocator<P>,
{
    /// Handle version change during optimistic read.
    #[cold]
    #[inline(never)]
    fn handle_version_change(
        &self,
        leaf: &LeafNode15<P>,
        key: &Key<'_>,
        version: u32,
        guard: &LocalGuard<'_>,
    ) -> (*mut LeafNode15<P>, u32, bool) {
        let (advanced, new_version) = self.advance_to_key_generic(leaf, key, version, guard);

        if StdPtr::eq(advanced, leaf) {
            (StdPtr::from_ref(leaf).cast_mut(), new_version, false)
        } else {
            (StdPtr::from_ref(advanced).cast_mut(), new_version, true)
        }
    }

    /// OCC version validation for single-layer read paths.
    #[inline(always)]
    fn validate_version_single(
        &self,
        leaf: &LeafNode15<P>,
        key: &Key<'_>,
        version: u32,
        guard: &LocalGuard<'_>,
    ) -> VersionCheck<P> {
        if unlikely(leaf.version().has_changed(version)) {
            if unlikely(leaf.version().has_split_no_compiler_fence(version)) {
                let (advanced, new_version) =
                    self.advance_to_key_generic(leaf, key, version, guard);

                if !StdPtr::eq(advanced, leaf) {
                    return VersionCheck::RetryLeaf {
                        new_leaf_ptr: StdPtr::from_ref(advanced).cast_mut(),
                    };
                }

                return VersionCheck::RetrySearch { new_version };
            }

            return VersionCheck::RetrySearch {
                new_version: leaf.version().stable(),
            };
        }

        VersionCheck::Valid
    }

    /// OCC version validation for multi-layer read paths.
    #[inline(always)]
    fn validate_version_multi(
        &self,
        leaf: &LeafNode15<P>,
        ver: &NodeVersion,
        key: &Key<'_>,
        version: u32,
        guard: &LocalGuard<'_>,
    ) -> VersionCheck<P> {
        if unlikely(ver.has_changed(version)) {
            if unlikely(ver.has_split_no_compiler_fence(version)) {
                let (new_ptr, new_version, changed_leaf) =
                    self.handle_version_change(leaf, key, version, guard);

                if changed_leaf {
                    return VersionCheck::RetryLeaf {
                        new_leaf_ptr: new_ptr,
                    };
                }

                return VersionCheck::RetrySearch { new_version };
            }

            return VersionCheck::RetrySearch {
                new_version: ver.stable(),
            };
        }

        VersionCheck::Valid
    }

    #[expect(
        clippy::unused_self,
        reason = "method signature kept for API consistency"
    )]
    fn check_blink_chain(
        &self,
        leaf: &LeafNode15<P>,
        target_ikey: u64,
        guard: &LocalGuard<'_>,
    ) -> Option<*mut LeafNode15<P>> {
        let next_raw: *mut LeafNode15<P> = leaf.next_raw(guard);

        if leaf.version().is_deleted() {
            let next_ptr: *mut LeafNode15<P> = Linker::unmark_ptr(next_raw);

            if !next_ptr.is_null() {
                return Some(next_ptr);
            }

            return None;
        }

        let next_ptr: *mut LeafNode15<P> = Linker::unmark_ptr(next_raw);

        if !next_ptr.is_null() && !Linker::is_marked(next_raw) {
            // SAFETY: next_ptr is valid (protected by guard in caller)
            let next_bound: u64 = unsafe { (*next_ptr).ikey_bound() };

            if target_ikey >= next_bound {
                return Some(next_ptr);
            }
        }

        None
    }

    /// Check if sublayer is deleted before descending.
    #[inline(always)]
    #[expect(clippy::unused_self, reason = "API consistency with other methods")]
    fn check_sublayer_valid(&self, layer_ptr: *mut u8) -> bool {
        if layer_ptr.is_null() {
            return false;
        }

        // SAFETY: ptr is non-null (checked above) and protected by guard.
        unsafe { NodeVersion::is_valid_sublayer(layer_ptr) }
    }

    /// Descend through a chain of trivial twigs (single-entry layer leaves).
    #[inline]
    #[expect(
        clippy::too_many_lines,
        reason = "complex twig-chain traversal with safety checks"
    )]
    fn descend_twig_chain(
        &self,
        initial_ptr: *mut u8,
        key: &mut Key<'_>,
        guard: &LocalGuard<'_>,
    ) -> TwigDescentResult<LeafNode15<P>> {
        let mut ptr: *mut u8 = initial_ptr;

        loop {
            key.shift();

            // SAFETY: ptr is non-null (checked by caller or previous iteration)
            #[expect(clippy::cast_ptr_alignment, reason = "NodeVersion is first field")]
            let node_version: &NodeVersion = unsafe { &*ptr.cast::<NodeVersion>() };

            if node_version.is_deleted() {
                return TwigDescentResult::ReturnNone;
            }

            if !node_version.is_leaf() {
                return TwigDescentResult::RestartLayerLoop {
                    layer_root: ptr.cast_const(),
                    in_sublayer: true,
                };
            }

            if !node_version.is_root() {
                return TwigDescentResult::RestartLayerLoop {
                    layer_root: ptr.cast_const(),
                    in_sublayer: true,
                };
            }

            // SAFETY: Verified is_leaf() above, ptr is valid sublayer pointer
            let twig: &LeafNode15<P> = unsafe { &*ptr.cast::<LeafNode15<P>>() };

            if twig.deleted_layer() {
                key.unshift_all();

                let root: *const u8 = self.load_root_ptr_generic(guard);

                return TwigDescentResult::RestartLayerLoop {
                    layer_root: root,
                    in_sublayer: false,
                };
            }

            let twig_version: u32 = twig.version().stable();
            twig.prefetch_for_search();

            let target_ikey: u64 = key.ikey();

            if !twig.prev(guard).is_null() && target_ikey < twig.ikey_bound() {
                return TwigDescentResult::ContinueLeafLoop {
                    layer_root: ptr.cast_const(),
                    leaf_ptr: ptr.cast::<LeafNode15<P>>(),
                };
            }

            let twig_perm = twig.permutation();

            // Exit fast path if not single-entry
            if twig_perm.size() != 1 {
                return TwigDescentResult::ContinueLeafLoop {
                    layer_root: ptr.cast_const(),
                    leaf_ptr: ptr.cast::<LeafNode15<P>>(),
                };
            }

            let slot: usize = twig_perm.get(0);
            let twig_ikey: u64 = twig.ikey_relaxed(slot);

            if twig_ikey != target_ikey {
                return TwigDescentResult::ContinueLeafLoop {
                    layer_root: ptr.cast_const(),
                    leaf_ptr: ptr.cast::<LeafNode15<P>>(),
                };
            }

            let twig_keylenx: u8 = twig.keylenx_relaxed(slot);

            if twig_keylenx < LAYER_KEYLENX {
                return TwigDescentResult::ContinueLeafLoop {
                    layer_root: ptr.cast_const(),
                    leaf_ptr: ptr.cast::<LeafNode15<P>>(),
                };
            }

            if twig.is_value_empty_relaxed(slot) {
                return TwigDescentResult::ContinueLeafLoop {
                    layer_root: ptr.cast_const(),
                    leaf_ptr: ptr.cast::<LeafNode15<P>>(),
                };
            }

            let next_ptr: *mut u8 = twig.load_layer_raw(slot);

            if next_ptr.is_null() {
                return TwigDescentResult::ContinueLeafLoop {
                    layer_root: ptr.cast_const(),
                    leaf_ptr: ptr.cast::<LeafNode15<P>>(),
                };
            }

            if twig.version().has_changed(twig_version) {
                return TwigDescentResult::ContinueLeafLoop {
                    layer_root: ptr.cast_const(),
                    leaf_ptr: ptr.cast::<LeafNode15<P>>(),
                };
            }

            if !key.has_suffix() {
                return TwigDescentResult::ContinueLeafLoop {
                    layer_root: ptr.cast_const(),
                    leaf_ptr: ptr.cast::<LeafNode15<P>>(),
                };
            }

            ptr = next_ptr;
        }
    }
}

// ============================================================================
//  Public API
// ============================================================================

impl<P, A> MassTreeGeneric<P, A>
where
    P: LeafPolicy,
    A: TreeAllocator<P>,
{
    /// Get a value by key.
    #[must_use]
    #[inline]
    pub fn get(&self, key: &[u8]) -> Option<P::Value>
    where
        P::Value: Clone,
    {
        let guard: LocalGuard<'_> = self.guard();

        self.get_with_guard(key, &guard)
            .map(|output| P::clone_value_from_output(&output))
    }

    /// Check if a key exists in the tree.
    #[must_use]
    #[inline]
    pub fn contains_key(&self, key: &[u8]) -> bool {
        let guard = self.guard();
        self.contains_key_with_guard(key, &guard)
    }

    /// Check if a key exists using an existing guard.
    #[must_use]
    #[inline]
    pub fn contains_key_with_guard(&self, key: &[u8], guard: &LocalGuard<'_>) -> bool {
        self.get_with_guard(key, guard).is_some()
    }

    /// Closure-based get implementation used by `get_ref`.
    ///
    /// This is a separate code path from `get_with_guard` (in `get_guarded.rs`)
    /// because `get_ref` must return `&'g P::Value` whose lifetime is tied to the
    /// EBR guard, not an owned `P::Output`. The closure `extract` converts a raw
    /// pointer to the caller's return type, enabling both `get_ref` (zero-copy
    /// reference) and type-erased access without duplicating the OCC loop.
    ///
    /// `get_with_guard` uses a separate implementation that returns `Option<P::Output>`
    /// via `load_value` (which copies the value) and includes an extra
    /// `is_value_empty` re-check after OCC validation for defense-in-depth.
    ///
    /// # Type Parameters
    ///
    /// * `R` - Return type (`P::Output` or `&'g P::Value`)
    /// * `F` - Closure that extracts the value from a raw pointer
    #[inline(always)]
    fn get_impl<R, F>(&self, key: &mut Key<'_>, guard: &LocalGuard<'_>, extract: F) -> Option<R>
    where
        F: Fn(*mut u8) -> R,
    {
        if !key.has_suffix() {
            return self.get_impl_single_layer(key, guard, extract);
        }

        self.get_impl_multi_layer(key, guard, extract)
    }

    /// Handle landing on a deleted leaf during point read.
    #[cold]
    #[inline(never)]
    fn handle_deleted_leaf(
        &self,
        leaf: &LeafNode15<P>,
        layer_root: *const u8,
        key: &Key<'_>,
        is_sublayer: bool,
        guard: &LocalGuard<'_>,
    ) -> *mut LeafNode15<P> {
        // Try to follow B-link to successor
        let next_raw: *mut LeafNode15<P> = leaf.next_raw(guard);
        let next_ptr: *mut LeafNode15<P> = Linker::unmark_ptr(next_raw);

        if !next_ptr.is_null() {
            // Follow to successor leaf
            return next_ptr;
        }

        self.reach_leaf_concurrent_generic(layer_root, key, is_sublayer, guard)
    }

    #[inline(always)]
    fn get_impl_single_layer<R, F>(
        &self,
        key: &Key<'_>,
        guard: &LocalGuard<'_>,
        extract: F,
    ) -> Option<R>
    where
        F: Fn(*mut u8) -> R,
    {
        let mut layer_root: *const u8 = self.load_root_ptr_generic(guard);
        let target_ikey: u64 = key.ikey();

        #[expect(clippy::cast_possible_truncation, reason = "current_len() <= 8")]
        let search_keylenx: u8 = key.current_len() as u8;

        let mut leaf_ptr: *mut LeafNode15<P> =
            self.reach_leaf_concurrent_generic(layer_root, key, false, guard);

        'leaf_loop: loop {
            // SAFETY: leaf_ptr protected by guard
            let leaf: &LeafNode15<P> = unsafe { &*leaf_ptr };

            if leaf.version().is_deleted() {
                leaf_ptr = self.handle_deleted_leaf(leaf, layer_root, key, false, guard);

                continue 'leaf_loop;
            }

            let mut version: u32 = if let Some(v) = leaf.version().try_stable() {
                leaf.prefetch_for_search();
                v
            } else {
                if let Some(next_ptr) = self.check_blink_chain(leaf, target_ikey, guard) {
                    leaf_ptr = next_ptr;

                    continue 'leaf_loop;
                }

                leaf.prefetch_for_search();
                leaf.version().stable()
            };

            if !leaf.prev(guard).is_null() && target_ikey < leaf.ikey_bound() {
                layer_root = self.load_root_ptr_generic(guard);
                leaf_ptr = self.reach_leaf_concurrent_generic(layer_root, key, false, guard);

                continue 'leaf_loop;
            }

            'search_loop: loop {
                let perm = leaf.permutation();
                let size: usize = perm.size();
                let mut found_ptr: *mut u8 = StdPtr::null_mut();

                for i in 0..size {
                    let slot: usize = perm.get(i);

                    if (leaf.ikey_relaxed(slot) == target_ikey)
                        && (leaf.keylenx_relaxed(slot) == search_keylenx)
                        && !leaf.is_value_empty_relaxed(slot)
                    {
                        leaf.prefetch_value(slot);
                        found_ptr = leaf.load_value_raw(slot);

                        break;
                    }
                }

                match self.validate_version_single(leaf, key, version, guard) {
                    VersionCheck::Valid => {}

                    VersionCheck::RetrySearch { new_version } => {
                        version = new_version;
                        continue 'search_loop;
                    }

                    VersionCheck::RetryLeaf { new_leaf_ptr } => {
                        leaf_ptr = new_leaf_ptr;
                        continue 'leaf_loop;
                    }
                }

                if !found_ptr.is_null() {
                    return Some(extract(found_ptr));
                }

                if unlikely(leaf.version().is_dirty()) {
                    version = leaf.version().stable();

                    continue 'search_loop;
                }

                if unlikely(!leaf.prev(guard).is_null() && target_ikey < leaf.ikey_bound()) {
                    layer_root = self.load_root_ptr_generic(guard);
                    leaf_ptr = self.reach_leaf_concurrent_generic(layer_root, key, false, guard);

                    continue 'leaf_loop;
                }

                if let Some(next_ptr) = self.check_blink_chain(leaf, target_ikey, guard) {
                    leaf_ptr = next_ptr;

                    continue 'leaf_loop;
                }

                return None;
            }
        }
    }

    /// Multi-layer path for keys > 8 bytes.
    #[expect(clippy::too_many_lines, reason = "complex multi-layer traversal logic")]
    #[inline]
    fn get_impl_multi_layer<R, F>(
        &self,
        key: &mut Key<'_>,
        guard: &LocalGuard<'_>,
        extract: F,
    ) -> Option<R>
    where
        F: Fn(*mut u8) -> R,
    {
        let mut layer_root: *const u8 = self.load_root_ptr_generic(guard);
        let mut in_sublayer: bool = false;

        'layer_loop: loop {
            layer_root = self.maybe_parent_generic(layer_root);

            let mut leaf_ptr: *mut LeafNode15<P> =
                self.reach_leaf_concurrent_generic(layer_root, key, in_sublayer, guard);

            'leaf_loop: loop {
                let target_ikey: u64 = key.ikey();

                let leaf: &LeafNode15<P> = unsafe { &*leaf_ptr };

                if leaf.version().is_deleted() {
                    if in_sublayer && leaf.deleted_layer() {
                        key.unshift_all();
                        layer_root = self.load_root_ptr_generic(guard);
                        in_sublayer = false;

                        continue 'layer_loop;
                    }

                    leaf_ptr = self.handle_deleted_leaf(leaf, layer_root, key, in_sublayer, guard);

                    continue 'leaf_loop;
                }

                let mut version: u32 = if let Some(v) = leaf.version().try_stable() {
                    leaf.prefetch_for_search();
                    v
                } else {
                    if let Some(next_ptr) = self.check_blink_chain(leaf, target_ikey, guard) {
                        leaf_ptr = next_ptr;

                        continue 'leaf_loop;
                    }

                    leaf.prefetch_for_search();
                    leaf.version().stable()
                };

                if !leaf.prev(guard).is_null() && target_ikey < leaf.ikey_bound() {
                    layer_root = self.load_root_ptr_generic(guard);
                    leaf_ptr =
                        self.reach_leaf_concurrent_generic(layer_root, key, in_sublayer, guard);

                    continue 'leaf_loop;
                }

                'search_loop: loop {
                    if leaf.deleted_layer() {
                        key.unshift_all();
                        layer_root = self.load_root_ptr_generic(guard);
                        in_sublayer = false;

                        continue 'layer_loop;
                    }

                    let result: LookupResult = search_leaf_multi_layer::<P>(leaf, key);

                    // Read value pointer BEFORE validation (inside OCC read phase),
                    // matching the single-layer pattern in get_impl_single_layer.
                    let found_ptr: *mut u8 = match &result {
                        LookupResult::ValueSlot(slot) => leaf.load_value_raw(*slot),
                        _ => StdPtr::null_mut(),
                    };

                    match self.validate_version_multi(leaf, leaf.version(), key, version, guard) {
                        VersionCheck::Valid => {}

                        VersionCheck::RetrySearch { new_version } => {
                            version = new_version;
                            continue 'search_loop;
                        }

                        VersionCheck::RetryLeaf { new_leaf_ptr } => {
                            leaf_ptr = new_leaf_ptr;
                            continue 'leaf_loop;
                        }
                    }

                    match result {
                        LookupResult::ValueSlot(_) => {
                            if !found_ptr.is_null() {
                                return Some(extract(found_ptr));
                            }

                            return None;
                        }

                        LookupResult::Layer(ptr) => {
                            // SAFETY: ptr is non-null (check_slot_match guards
                            // against null before constructing Layer), cast to
                            // read version.
                            #[expect(
                                clippy::cast_ptr_alignment,
                                reason = "NodeVersion is first field"
                            )]
                            let sublayer_version: &NodeVersion =
                                unsafe { &*ptr.cast::<NodeVersion>() };

                            if sublayer_version.is_deleted() {
                                return None;
                            }

                            match self.descend_twig_chain(ptr, key, guard) {
                                TwigDescentResult::ContinueLeafLoop {
                                    layer_root: new_root,
                                    leaf_ptr: new_leaf,
                                } => {
                                    layer_root = new_root;
                                    leaf_ptr = new_leaf;
                                    in_sublayer = true;

                                    continue 'leaf_loop;
                                }

                                TwigDescentResult::RestartLayerLoop {
                                    layer_root: new_root,
                                    in_sublayer: new_in_sublayer,
                                } => {
                                    layer_root = new_root;
                                    in_sublayer = new_in_sublayer;

                                    continue 'layer_loop;
                                }

                                TwigDescentResult::ReturnNone => {
                                    return None;
                                }
                            }
                        }

                        LookupResult::NotFound => {
                            if leaf.version().is_dirty() {
                                version = leaf.version().stable();

                                continue 'search_loop;
                            }

                            if !leaf.prev(guard).is_null() && target_ikey < leaf.ikey_bound() {
                                layer_root = self.load_root_ptr_generic(guard);
                                leaf_ptr = self.reach_leaf_concurrent_generic(
                                    layer_root,
                                    key,
                                    in_sublayer,
                                    guard,
                                );

                                continue 'leaf_loop;
                            }

                            if let Some(next_ptr) = self.check_blink_chain(leaf, target_ikey, guard)
                            {
                                leaf_ptr = next_ptr;

                                continue 'leaf_loop;
                            }

                            return None;
                        }
                    }
                }
            }
        }
    }
}

// ============================================================================
//  Reference-Returning API
// ============================================================================

impl<P, A> MassTreeGeneric<P, A>
where
    P: LeafPolicy + RefLeafPolicy,
    A: TreeAllocator<P>,
{
    /// Get a borrowed reference to a value by key.
    ///
    /// For write-through types (`V` <= 8 bytes), returns `ValueRef::Owned`
    /// from an atomic read. For larger types, returns `ValueRef::Borrowed`
    /// (zero-copy). Use [`Deref`] to access the value in both cases.
    #[must_use]
    #[inline(always)]
    pub fn get_ref<'g>(
        &self,
        key: &[u8],
        guard: &'g LocalGuard<'_>,
    ) -> Option<ValueRef<'g, P::Value>> {
        self.verify_guard(guard);
        let mut search_key: Key<'_> = Key::new(key);

        self.get_impl(&mut search_key, guard, |ptr: *mut u8| {
            if P::CAN_WRITE_THROUGH {
                // SAFETY: CAN_WRITE_THROUGH guarantees size 1/2/4/8 with
                // natural alignment. Atomic read avoids aliasing violation
                // with concurrent write_through_update.
                ValueRef::Owned(unsafe { atomic_read_value::<P::Value>(ptr, READ_ORD) })
            } else {
                // SAFETY: version validated, guard protects from deallocation.
                // No concurrent modification (non-write-through types allocate
                // a new Box on update).
                ValueRef::Borrowed(unsafe { &*(ptr.cast::<P::Value>()) })
            }
        })
    }
}
