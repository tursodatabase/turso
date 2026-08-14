//! Public API methods for range scans on [`crate::MassTreeGeneric`].

use seize::LocalGuard;

use crate::Permuter;
use crate::alloc_trait::TreeAllocator;
use crate::key::{IKEY_SIZE, MAX_KEY_LENGTH};
use crate::leaf15::{LAYER_KEYLENX, LeafNode15};
use crate::nodeversion::NodeVersion;
use crate::policy::LeafPolicy;
use crate::tree::MassTreeGeneric;

use super::cursor_key::CursorKey;
use super::helper::{KeyIndexedPosition, lower_with_position};
use super::iterator::{KeysIter, RangeBound, RangeIter, ScanEntry, ValuesIter};
use super::traversal::reach_leaf_for_scan;

// ============================================================================
//  Range Scan API for MassTreeGeneric
// ============================================================================

impl<P, A> MassTreeGeneric<P, A>
where
    P: LeafPolicy,
    A: TreeAllocator<P>,
{
    // ========================================================================
    //  Iterator API
    // ========================================================================

    /// Create an iterator over a key range, yielding [`ScanEntry`] items
    /// with owned keys and cloned values in lexicographic order.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let guard = tree.guard();
    ///
    /// for entry in tree.range(
    ///     RangeBound::Included(b"aaa"),
    ///     RangeBound::Excluded(b"zzz"),
    ///     &guard
    /// ) {
    ///     println!("{:?} -> {:?}", entry.key, entry.value);
    /// }
    ///
    pub fn range<'a, 'g>(
        &'a self,
        start: RangeBound<'a>,
        end: RangeBound<'a>,
        guard: &'g LocalGuard<'a>,
    ) -> RangeIter<'a, 'g, P, A> {
        self.verify_guard(guard);
        RangeIter::new(self, start, end, guard)
    }

    /// Forward-only range iterator (skips ~300 bytes of backward state init).
    pub(crate) fn range_forward<'a, 'g>(
        &'a self,
        start: RangeBound<'a>,
        end: RangeBound<'a>,
        guard: &'g LocalGuard<'a>,
    ) -> RangeIter<'a, 'g, P, A> {
        self.verify_guard(guard);
        RangeIter::new_forward_only(self, start, end, guard)
    }

    /// Forward-only iterator rooted at a specific sublayer.
    pub(crate) fn range_forward_from_root<'a, 'g>(
        &'a self,
        layer_root: *const u8,
        cursor_key: CursorKey,
        start: RangeBound<'a>,
        end: RangeBound<'a>,
        guard: &'g LocalGuard<'a>,
    ) -> RangeIter<'a, 'g, P, A> {
        self.verify_guard(guard);
        RangeIter::new_forward_only_from_root(layer_root, cursor_key, start, end, guard)
    }

    /// Iterate over all entries. Equivalent to unbounded `range()`.
    ///
    /// ```ignore
    /// let guard = tree.guard();
    /// let count = tree.iter(&guard).count();
    ///
    pub fn iter<'a, 'g>(&'a self, guard: &'g LocalGuard<'a>) -> RangeIter<'a, 'g, P, A> {
        self.range(RangeBound::Unbounded, RangeBound::Unbounded, guard)
    }

    /// Iterate over all keys, yielding owned `Vec<u8>` values.
    ///
    /// ```ignore
    /// let guard = tree.guard();
    /// let keys: Vec<Vec<u8>> = tree.keys(&guard).collect();
    ///
    pub fn keys<'a, 'g>(&'a self, guard: &'g LocalGuard<'a>) -> KeysIter<'a, 'g, P, A> {
        self.iter(guard).keys()
    }

    /// Iterate over all values, yielding cloned values.
    ///
    /// ```ignore
    /// let guard = tree.guard();
    /// let values: Vec<ValuePtr<String>> = tree.values(&guard).collect();
    ///
    pub fn values<'a, 'g>(&'a self, guard: &'g LocalGuard<'a>) -> ValuesIter<'a, 'g, P, A> {
        self.iter(guard).values()
    }

    // ========================================================================
    //  First / Last Access
    // ========================================================================

    /// Get the first (smallest) key-value pair. Creates a guard internally.
    /// For repeated access, prefer [`first_with_guard`](Self::first_with_guard).
    ///
    /// ```ignore
    /// let first = tree.first().unwrap();
    /// assert_eq!(first.key(), b"apple");
    /// ```
    #[must_use]
    #[inline]
    pub fn first(&self) -> Option<ScanEntry<P::Value>>
    where
        P::Value: Clone,
    {
        let guard = self.guard();
        self.first_with_guard(&guard)
            .map(|entry| ScanEntry::new(entry.key, P::clone_value_from_output(&entry.value)))
    }

    /// Get the first (smallest) key-value pair using an existing guard.
    #[must_use]
    #[inline]
    pub fn first_with_guard<'a>(&'a self, guard: &LocalGuard<'a>) -> Option<ScanEntry<P::Output>> {
        self.iter(guard).next()
    }

    /// Get the last (largest) key-value pair. Creates a guard internally.
    /// For repeated access, prefer [`last_with_guard`](Self::last_with_guard).
    ///
    /// ```ignore
    /// let last = tree.last().unwrap();
    /// assert_eq!(last.key(), b"cherry");
    /// ```
    #[must_use]
    #[inline]
    pub fn last(&self) -> Option<ScanEntry<P::Value>>
    where
        P::Value: Clone,
    {
        let guard = self.guard();
        self.last_with_guard(&guard)
            .map(|entry| ScanEntry::new(entry.key, P::clone_value_from_output(&entry.value)))
    }

    /// Get the last (largest) key-value pair using an existing guard.
    #[must_use]
    #[inline]
    pub fn last_with_guard<'a>(&'a self, guard: &LocalGuard<'a>) -> Option<ScanEntry<P::Output>> {
        self.iter(guard).next_back()
    }

    // ========================================================================
    //  Visitor API
    // ========================================================================

    /// Scan a range with a visitor callback. Return `false` to stop early.
    ///
    /// More efficient than the iterator API when you don't need to own keys
    /// (avoids `Vec<u8>` allocation per entry). Returns entries visited count.
    ///
    /// ```ignore
    /// let guard = tree.guard();
    /// tree.scan(
    ///     RangeBound::Unbounded,
    ///     RangeBound::Unbounded,
    ///     |key, value| { println!("{:?} -> {:?}", key, value); true },
    ///     &guard
    /// );
    ///
    pub fn scan<F>(
        &self,
        start: RangeBound<'_>,
        end: RangeBound<'_>,
        visitor: F,
        guard: &LocalGuard<'_>,
    ) -> usize
    where
        F: FnMut(&[u8], P::Output) -> bool,
    {
        self.range_forward(start, end, guard).for_each(visitor)
    }

    /// Batch-optimized forward range scan — fastest for all storage types.
    ///
    /// Processes all entries per leaf with single OCC validation, reducing
    /// per-entry overhead vs [`scan`](Self::scan). Falls back to state machine
    /// for sublayer transitions.
    ///
    /// For pointer-backed storage that can return references, see
    /// [`scan_intra_leaf_batch_ref`](Self::scan_intra_leaf_batch_ref).
    ///
    /// ```ignore
    /// let guard = tree.guard();
    /// let mut sum = 0u64;
    /// tree.scan_intra_leaf_batch(
    ///     RangeBound::Unbounded, RangeBound::Unbounded,
    ///     |_key, value| { sum += value; true },
    ///     &guard
    /// );
    /// ```
    pub fn scan_intra_leaf_batch<F>(
        &self,
        start: RangeBound<'_>,
        end: RangeBound<'_>,
        visitor: F,
        guard: &LocalGuard<'_>,
    ) -> usize
    where
        F: FnMut(&[u8], P::Output) -> bool,
    {
        self.range_forward(start, end, guard)
            .for_each_intra_leaf_batch(visitor)
    }

    /// Batch-optimized reverse range scan. Same perf characteristics as
    /// [`scan_intra_leaf_batch`](Self::scan_intra_leaf_batch) but descending order.
    ///
    /// ```ignore
    /// let guard = tree.guard();
    /// let mut sum = 0u64;
    /// tree.scan_rev_batch(
    ///     RangeBound::Unbounded, RangeBound::Unbounded,
    ///     |_key, value| { sum += value; true },
    ///     &guard
    /// );
    /// ```
    pub fn scan_rev_batch<F>(
        &self,
        start: RangeBound<'_>,
        end: RangeBound<'_>,
        visitor: F,
        guard: &LocalGuard<'_>,
    ) -> usize
    where
        F: FnMut(&[u8], P::Output) -> bool,
    {
        self.range(start, end, guard)
            .rev_for_each_intra_leaf_batch(visitor)
    }

    /// Value-only scan — fastest when you don't need keys.
    ///
    /// Skips key materialization (~1.5-2x faster than `scan_intra_leaf_batch`
    /// for 64-byte keys). Ideal for aggregations (sum, count, min, max).
    ///
    /// # End Bound Behavior
    ///
    /// Bounded end checks use ikey comparison only. For keys sharing the same
    /// ikey as the bound, entries **may be over-included**. Use
    /// `scan_intra_leaf_batch` when exact bounds matter for long keys.
    ///
    /// ```ignore
    /// let guard = tree.guard();
    /// let mut sum = 0u64;
    /// tree.scan_values(
    ///     RangeBound::Unbounded, RangeBound::Unbounded,
    ///     |value| { sum += value; true },
    ///     &guard
    /// );
    /// ```
    pub fn scan_values<F>(
        &self,
        start: RangeBound<'_>,
        end: RangeBound<'_>,
        visitor: F,
        guard: &LocalGuard<'_>,
    ) -> usize
    where
        F: FnMut(P::Output) -> bool,
    {
        self.range_forward(start, end, guard)
            .for_each_values_batch(visitor)
    }

    /// Reverse value-only scan. Same as [`scan_values`](Self::scan_values) in
    /// descending order.
    ///
    /// Start bound uses ikey comparison only — **approximate** for suffixed keys.
    /// Use `scan_rev_batch` when exact start bounds matter for long keys.
    pub fn scan_values_rev<F>(
        &self,
        start: RangeBound<'_>,
        end: RangeBound<'_>,
        visitor: F,
        guard: &LocalGuard<'_>,
    ) -> usize
    where
        F: FnMut(P::Output) -> bool,
    {
        self.range(start, end, guard)
            .rev_for_each_values_batch(visitor)
    }

    /// Scan all entries matching a prefix.
    ///
    /// ```ignore
    /// let guard = tree.guard();
    /// tree.scan_prefix(b"user:", |key, value| {
    ///     println!("User key: {:?}", key);
    ///     true
    /// }, &guard);
    ///```
    ///
    /// # Panics
    ///
    /// Panics if `prefix.len()` exceeds `MAX_KEY_LENGTH`.
    pub fn scan_prefix<F>(&self, prefix: &[u8], mut visitor: F, guard: &LocalGuard<'_>) -> usize
    where
        F: FnMut(&[u8], P::Output) -> bool,
    {
        self.verify_guard(guard);
        self.scan_prefix_inner(prefix, guard, |exact_value, iter| {
            let mut count = 0;
            if let Some(value) = exact_value {
                count += 1;
                if !visitor(prefix, value) {
                    return count;
                }
            }
            count + iter.for_each_intra_leaf_batch(visitor)
        })
    }

    /// Value-only prefix scan (no key materialization).
    ///
    /// Like [`scan_prefix`](Self::scan_prefix) but skips building key bytes.
    ///
    /// End bound is **exact** for ikey-aligned prefixes (multiples of 8 bytes),
    /// **approximate** for non-aligned (may over-include entries sharing the
    /// boundary ikey).
    ///
    /// # Panics
    ///
    /// Panics if `prefix.len()` exceeds `MAX_KEY_LENGTH`.
    pub fn scan_prefix_values<F>(
        &self,
        prefix: &[u8],
        mut visitor: F,
        guard: &LocalGuard<'_>,
    ) -> usize
    where
        F: FnMut(P::Output) -> bool,
    {
        self.verify_guard(guard);
        self.scan_prefix_inner(prefix, guard, |exact_value, iter| {
            let mut count = 0;
            if let Some(value) = exact_value {
                count += 1;
                if !visitor(value) {
                    return count;
                }
            }
            count + iter.for_each_values_batch(visitor)
        })
    }

    // ========================================================================
    //  Shared Prefix Scan Logic
    // ========================================================================

    /// Shared impl for `scan_prefix` and `scan_prefix_values`.
    ///
    /// Validates prefix, computes upper bound, attempts trie-aware fast-path
    /// descent, then delegates to `scan_fn(exact_value_at_boundary, iter)`.
    #[inline]
    fn scan_prefix_inner(
        &self,
        prefix: &[u8],
        guard: &LocalGuard<'_>,
        scan_fn: impl FnOnce(Option<P::Output>, RangeIter<'_, '_, P, A>) -> usize,
    ) -> usize {
        assert!(
            prefix.len() <= MAX_KEY_LENGTH,
            "key length {} exceeds maximum {}",
            prefix.len(),
            MAX_KEY_LENGTH
        );

        // Exclusive upper bound on stack: "abc" -> "abd", "ab\xff" -> "ac".
        let mut upper_buf = [0u8; MAX_KEY_LENGTH];
        let upper_len = compute_prefix_upper_bound_into(prefix, &mut upper_buf);

        let end: RangeBound<'_> = upper_len.map_or(RangeBound::Unbounded, |len| {
            RangeBound::Excluded(&upper_buf[..len])
        });

        // Trie fast path: descend through exact 8-byte chunks via layer pointers.
        if let Some((layer_root, descended_chunks)) = self.descend_prefix_layers(prefix, guard)
            && descended_chunks > 0
        {
            let mut cursor = CursorKey::from_slice(prefix);
            for _ in 0..descended_chunks {
                if cursor.has_suffix() {
                    cursor.shift();
                } else {
                    cursor.shift_clear();
                }
            }

            let prefix_at_chunk_boundary = prefix.len() == descended_chunks * IKEY_SIZE;

            if prefix_at_chunk_boundary {
                let exact_value = self.get_with_guard(prefix, guard);
                let iter = self.range_forward_from_root(
                    layer_root,
                    cursor,
                    RangeBound::Unbounded,
                    RangeBound::Unbounded,
                    guard,
                );
                return scan_fn(exact_value, iter);
            }

            let iter = self.range_forward_from_root(
                layer_root,
                cursor,
                RangeBound::Included(prefix),
                end,
                guard,
            );
            return scan_fn(None, iter);
        }

        let iter = self.range_forward(RangeBound::Included(prefix), end, guard);
        scan_fn(None, iter)
    }

    // ========================================================================
    //  Convenience Collectors
    // ========================================================================

    /// Collect all entries into a Vec.
    pub fn collect_entries(&self, guard: &LocalGuard<'_>) -> Vec<ScanEntry<P::Output>> {
        self.iter(guard).collect()
    }

    /// Collect all keys into a Vec.
    pub fn collect_keys(&self, guard: &LocalGuard<'_>) -> Vec<Vec<u8>> {
        self.keys(guard).collect()
    }

    /// Collect all values into a Vec.
    pub fn collect_values(&self, guard: &LocalGuard<'_>) -> Vec<P::Output> {
        self.values(guard).collect()
    }
}

impl<P, A> MassTreeGeneric<P, A>
where
    P: LeafPolicy,
    A: TreeAllocator<P>,
{
    /// Descend through full 8-byte prefix chunks via layer pointers.
    /// Returns `(sublayer_root, chunks_descended)`.
    fn descend_prefix_layers(
        &self,
        prefix: &[u8],
        guard: &LocalGuard<'_>,
    ) -> Option<(*const u8, usize)> {
        let full_chunks: usize = prefix.len() / IKEY_SIZE;

        if full_chunks == 0 {
            return None;
        }

        let mut current_root = self.load_root_ptr_generic(guard);
        let mut descended = 0usize;

        while descended < full_chunks {
            let chunk_ikey = read_full_chunk_ikey(prefix, descended);

            let Some(next_root) = find_layer_child_root::<P>(current_root, chunk_ikey, guard)
            else {
                break;
            };

            current_root = next_root;
            descended += 1;
        }

        Some((current_root, descended))
    }
}

// ============================================================================
//  Helper Functions
// ============================================================================

/// Find the sublayer root for an exact ikey match in the leaf reached from `root`.
fn find_layer_child_root<P>(
    root: *const u8,
    chunk_ikey: u64,
    guard: &LocalGuard<'_>,
) -> Option<*const u8>
where
    P: LeafPolicy,
{
    if root.is_null() {
        return None;
    }

    let cursor = CursorKey::from_slice(&chunk_ikey.to_be_bytes());
    let leaf_ptr: *mut LeafNode15<P> = reach_leaf_for_scan::<P>(root, &cursor, guard);

    if leaf_ptr.is_null() {
        return None;
    }

    // SAFETY: leaf_ptr is protected by guard and null-checked above.
    let leaf: &LeafNode15<P> = unsafe { &*leaf_ptr };
    let version: u32 = leaf.version().stable();

    if NodeVersion::is_deleted_version(version) {
        return None;
    }

    let perm: Permuter = leaf.permutation();
    let kx: KeyIndexedPosition = lower_with_position(&cursor, leaf, &perm);
    let _ = kx.p?;

    let mut i: usize = kx.i;

    while i < perm.size() {
        let slot: usize = perm.get(i);
        let slot_ikey: u64 = leaf.ikey_relaxed(slot);

        if slot_ikey != chunk_ikey {
            break;
        }

        if leaf.keylenx_relaxed(slot) >= LAYER_KEYLENX && !leaf.is_value_empty_relaxed(slot) {
            let layer_ptr: *const u8 = leaf.load_layer_raw(slot).cast_const();

            if leaf.version().has_changed(version) {
                return None;
            }

            if !layer_ptr.is_null() {
                return Some(layer_ptr);
            }

            return None;
        }

        i += 1;
    }

    if leaf.version().has_changed(version) {
        return None;
    }

    None
}

#[expect(clippy::indexing_slicing, reason = "chunk bounds are caller-checked")]
fn read_full_chunk_ikey(prefix: &[u8], chunk_idx: usize) -> u64 {
    let start: usize = chunk_idx * IKEY_SIZE;
    let end: usize = start + IKEY_SIZE;

    #[expect(clippy::expect_used, reason = "slice length is guaranteed to be 8")]
    let bytes: [u8; IKEY_SIZE] = prefix[start..end].try_into().expect("slice is 8 bytes");

    u64::from_be_bytes(bytes)
}

/// Compute exclusive upper bound for prefix scan into `buf`.
///
/// Increments the rightmost non-0xFF byte: "abc" -> "abd", "ab\xff" -> "ac".
/// Returns `Some(len)` or `None` if empty/all-0xFF (unbounded).
#[expect(clippy::indexing_slicing, reason = "Checked")]
fn compute_prefix_upper_bound_into(prefix: &[u8], buf: &mut [u8; MAX_KEY_LENGTH]) -> Option<usize> {
    assert!(
        prefix.len() <= MAX_KEY_LENGTH,
        "key length {} exceeds maximum {}",
        prefix.len(),
        MAX_KEY_LENGTH
    );

    if prefix.is_empty() {
        return None;
    }

    buf[..prefix.len()].copy_from_slice(prefix);

    for i in (0..prefix.len()).rev() {
        if buf[i] < 0xFF {
            buf[i] += 1;
            return Some(i + 1);
        }
    }

    None
}

// ============================================================================
//  Tests
// ============================================================================
