use seize::LocalGuard;

use crate::leaf15::LeafNode15;
use crate::{
    LeafPolicy, MassTreeGeneric, NodeVersion, TreeAllocator,
    hints::unlikely,
    key::Key,
    tree::generic::optimistic_reads::{
        LookupResult, TwigDescentResult, VersionCheck, search_leaf_multi_layer,
    },
};

impl<P, A> MassTreeGeneric<P, A>
where
    P: LeafPolicy,
    A: TreeAllocator<P>,
{
    /// Get a value by key, returning a clone of the output.
    ///
    /// Main read path for `get()` and `get_with_guard()`. Returns an owned
    /// `P::Output` via `load_value` (which copies the value). Includes an
    /// `is_value_empty` re-check after OCC validation for defense-in-depth.
    ///
    /// A separate closure-based implementation (`get_impl` in the parent module)
    /// exists for `get_ref`, which must return `&'g P::Value` tied to the EBR
    /// guard lifetime. That path uses `load_value_raw` with an `extract` closure.
    #[inline(always)]
    pub fn get_with_guard(&self, key: &[u8], guard: &LocalGuard<'_>) -> Option<P::Output> {
        self.verify_guard(guard);
        let mut key: Key<'_> = Key::new(key);

        // Find root
        let layer_root: *const u8 = self.load_root_ptr_generic(guard);

        if layer_root.is_null() {
            return None;
        }

        // Dispatch to single-layer or multi-layer path
        if key.has_suffix() {
            self.get_with_guard_multi_layer(&mut key, layer_root, guard)
        } else {
            self.get_with_guard_single_layer(&key, layer_root, guard)
        }
    }

    /// Single-layer fast path for keys <=8 bytes.
    ///
    /// Optimized for the common case of short keys:
    /// - No suffix comparison needed
    /// - No layer pointer detection needed
    /// - Simpler search loop
    /// - Stores pointer directly (matches `get_ref` pattern)
    #[inline]
    fn get_with_guard_single_layer(
        &self,
        key: &Key<'_>,
        layer_root: *const u8,
        guard: &LocalGuard<'_>,
    ) -> Option<P::Output> {
        let target_ikey: u64 = key.ikey();

        #[expect(clippy::cast_possible_truncation, reason = "current_len() <= 8")]
        let search_keylenx: u8 = key.current_len() as u8;

        // Navigate from root to leaf
        let mut leaf_ptr: *mut LeafNode15<P> =
            self.reach_leaf_concurrent_generic(layer_root, key, false, guard);

        'leaf_loop: loop {
            // SAFETY: leaf_ptr protected by guard
            let leaf: &LeafNode15<P> = unsafe { &*leaf_ptr };

            // Handle deleted leaf (concurrent coalesce)
            if leaf.version().is_deleted() {
                leaf_ptr = self.handle_deleted_leaf(leaf, layer_root, key, false, guard);

                continue 'leaf_loop;
            }

            let mut version: u32 = if let Some(v) = leaf.version().try_stable() {
                leaf.prefetch_for_search();
                v
            } else {
                // Leaf is locked, try B-link escape
                if let Some(next_ptr) = self.check_blink_chain(leaf, target_ikey, guard) {
                    leaf_ptr = next_ptr;

                    continue 'leaf_loop;
                }

                // No escape route, must wait - prefetch while spinning
                leaf.prefetch_for_search();
                leaf.version().stable()
            };

            // Early too-right check
            if !leaf.prev(guard).is_null() && target_ikey < leaf.ikey_bound() {
                // Reload root to get latest pointer after concurrent modifications
                leaf_ptr = self.reach_leaf_concurrent_generic(layer_root, key, false, guard);

                continue 'leaf_loop;
            }

            'search_loop: loop {
                let perm = leaf.permutation();
                let size: usize = perm.size();

                // Simple linear search - store slot index directly
                let mut found_slot: Option<usize> = None;
                for i in 0..size {
                    let slot: usize = perm.get(i);

                    // Use Relaxed ordering - permutation() Acquire already synchronizes
                    if (leaf.ikey_relaxed(slot) == target_ikey)
                        && (leaf.keylenx_relaxed(slot) == search_keylenx)
                        && !leaf.is_value_empty_relaxed(slot)
                    {
                        // Policy-specific value prefetch:
                        // - BoxPolicy: prefetch heap allocation to hide pointer-chase latency
                        // - InlinePolicy: no-op
                        leaf.prefetch_value(slot);
                        found_slot = Some(slot);
                        break;
                    }
                }

                // Read output before version validation (OCC pattern)
                let output: Option<P::Output> = found_slot.and_then(|s| leaf.load_value(s));

                // Version validation after all reads (common case: unchanged)
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

                // Version validated, safe to return
                if output.is_some() {
                    return output;
                }

                // Not found, check dirty or B-link
                if unlikely(leaf.version().is_dirty()) {
                    version = leaf.version().stable();

                    continue 'search_loop;
                }

                if let Some(next_ptr) = self.check_blink_chain(leaf, target_ikey, guard) {
                    leaf_ptr = next_ptr;

                    continue 'leaf_loop;
                }

                // Fallback too-right check
                //
                // NOTE: This is defense-in-depth; the early check above catches most cases.
                if unlikely(!leaf.prev(guard).is_null() && target_ikey < leaf.ikey_bound()) {
                    leaf_ptr = self.reach_leaf_concurrent_generic(layer_root, key, false, guard);

                    continue 'leaf_loop;
                }

                return None;
            }
        }
    }

    /// Multi-layer path for keys >8 bytes.
    ///
    /// Handles:
    /// - Suffix comparison for keys with same 8-byte prefix
    /// - Layer pointer detection and descent
    /// - `deleted_layer()` recovery (GC'd sublayer)
    /// - `check_sublayer_valid` before descent
    /// - Stores pointer directly (matches `get_ref` pattern)
    #[inline]
    #[expect(clippy::too_many_lines, reason = "Complex multi-layer logic")]
    fn get_with_guard_multi_layer(
        &self,
        key: &mut Key<'_>,
        initial_root: *const u8,
        guard: &LocalGuard<'_>,
    ) -> Option<P::Output> {
        let mut layer_root: *const u8 = initial_root;
        let mut in_sublayer: bool = false;

        'layer_loop: loop {
            layer_root = self.maybe_parent_generic(layer_root);

            let mut leaf_ptr: *mut LeafNode15<P> =
                self.reach_leaf_concurrent_generic(layer_root, key, in_sublayer, guard);

            'leaf_loop: loop {
                // OPTIM: Compute ikey once per leaf iteration.
                //
                // key.shift() mutates on layer descent, so this must be per-iteration.
                let target_ikey: u64 = key.ikey();

                // SAFETY: leaf_ptr protected by guard
                let leaf: &LeafNode15<P> = unsafe { &*leaf_ptr };

                // Handle deleted leaf (concurrent coalesce) - rare condition
                if unlikely(leaf.version().is_deleted()) {
                    if in_sublayer && leaf.deleted_layer() {
                        key.unshift_all();
                        layer_root = self.load_root_ptr_generic(guard);
                        in_sublayer = false;

                        continue 'layer_loop;
                    }

                    leaf_ptr = self.handle_deleted_leaf(leaf, layer_root, key, in_sublayer, guard);

                    continue 'leaf_loop;
                }

                // OPTIM: Use try_stable() to avoid spinning
                let mut version: u32 = if let Some(v) = leaf.version().try_stable() {
                    leaf.prefetch_for_search();
                    v
                } else {
                    if let Some(next_ptr) = self.check_blink_chain(leaf, target_ikey, guard) {
                        leaf_ptr = next_ptr;

                        continue 'leaf_loop;
                    }

                    // Prefetch while spinning
                    leaf.prefetch_for_search();
                    leaf.version().stable()
                };

                // Early too-right check
                if !leaf.prev(guard).is_null() && target_ikey < leaf.ikey_bound() {
                    // Reload root to get latest pointer after concurrent modifications
                    leaf_ptr =
                        self.reach_leaf_concurrent_generic(layer_root, key, in_sublayer, guard);

                    continue 'leaf_loop;
                }

                'search_loop: loop {
                    // Check for GC'd sublayer - must restart from root
                    if leaf.deleted_layer() {
                        key.unshift_all();
                        layer_root = self.load_root_ptr_generic(guard);
                        in_sublayer = false;

                        continue 'layer_loop;
                    }

                    // target_ikey already computed at start of 'leaf_loop
                    let result: LookupResult = search_leaf_multi_layer::<P>(leaf, key);

                    // Store version reference once for all validation checks below
                    let ver: &NodeVersion = leaf.version();

                    match result {
                        LookupResult::ValueSlot(slot) => {
                            // Read pointer and extract output BEFORE version validation
                            // Store pointer directly - no redundant read via try_load_output
                            let output: Option<P::Output> = leaf.load_value(slot);

                            match self.validate_version_multi(leaf, ver, key, version, guard) {
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

                            return output;
                        }

                        LookupResult::Layer(layer_ptr) => {
                            // Validate version BEFORE descending
                            match self.validate_version_multi(leaf, ver, key, version, guard) {
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

                            // Check sublayer is not deleted before descent
                            if unlikely(!self.check_sublayer_valid(layer_ptr)) {
                                return None;
                            }

                            // Fast-path: descend through single-entry twig
                            // leaves without re-entering reach_leaf.
                            // descend_twig_chain calls key.shift() internally.
                            match self.descend_twig_chain(layer_ptr, key, guard) {
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
                            // Validate before returning None
                            match self.validate_version_multi(leaf, ver, key, version, guard) {
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

                            if unlikely(ver.is_dirty()) {
                                version = ver.stable();

                                continue 'search_loop;
                            }

                            if let Some(next_ptr) = self.check_blink_chain(leaf, target_ikey, guard)
                            {
                                leaf_ptr = next_ptr;

                                continue 'leaf_loop;
                            }

                            if unlikely(
                                !leaf.prev(guard).is_null() && target_ikey < leaf.ikey_bound(),
                            ) {
                                leaf_ptr = self.reach_leaf_concurrent_generic(
                                    layer_root,
                                    key,
                                    in_sublayer,
                                    guard,
                                );

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
