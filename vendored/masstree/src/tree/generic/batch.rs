use std::sync::atomic::Ordering as AtomicOrdering;

use seize::LocalGuard;

use std::ptr as StdPtr;

use crate::Permuter;
use crate::leaf15::LeafNode15;
use crate::{
    Linker, MassTreeGeneric, TreeAllocator, key::Key, leaf_trait::TreeLeafNode,
    nodeversion::LockGuard, policy::LeafPolicy,
};

use super::{FindSlotResult, InsertSearchResultGeneric};

/// Max suffix bag retires per locked batch insert.
/// One per slot (WIDTH=15) plus one for empty-leaf reuse = 16.
const MAX_BATCH_RETIRES: usize = crate::leaf15::WIDTH_15 + 1;

/// Buffer for deferred suffix bag retirements during batch insertion.
///
/// Collects pointers to old suffix bags that must be retired outside the
/// leaf lock. Bounded by `MAX_BATCH_RETIRES`.
struct RetireBuf {
    ptrs: [*mut u8; MAX_BATCH_RETIRES],
    count: usize,
}

impl RetireBuf {
    #[inline]
    const fn new() -> Self {
        Self {
            ptrs: [StdPtr::null_mut(); MAX_BATCH_RETIRES],
            count: 0,
        }
    }

    #[inline]
    fn push(&mut self, ptr: *mut u8) {
        debug_assert!(self.count < self.ptrs.len());
        self.ptrs[self.count] = ptr;
        self.count += 1;
    }

    #[inline]
    fn as_slice(&self) -> &[*mut u8] {
        &self.ptrs[..self.count]
    }
}

// ============================================================================
//  Batch Entry Types
// ============================================================================

/// A single entry in a batch insert operation.
#[must_use]
#[expect(
    missing_debug_implementations,
    reason = "Debug on P::Output may not be available"
)]
pub struct BatchEntry<P: LeafPolicy> {
    /// The key bytes (owned for sorting).
    pub key: Vec<u8>,

    /// The pre-converted output value.
    pub output: P::Output,

    /// Cached ikey for the first 8 bytes (used for sorting).
    ikey: u64,
}

impl<P: LeafPolicy> BatchEntry<P> {
    /// Create a new batch entry from key and value.
    ///
    /// Converts the value to output immediately to ensure single allocation.
    #[inline]
    pub fn new(key: Vec<u8>, value: P::Value) -> Self {
        let ikey: u64 = Self::compute_ikey(&key);
        let output: P::Output = P::into_output(value);

        Self { key, output, ikey }
    }

    /// Create a batch entry from key and pre-converted output.
    #[inline(always)]
    pub fn from_output(key: Vec<u8>, output: P::Output) -> Self {
        let ikey: u64 = Self::compute_ikey(&key);

        Self { key, output, ikey }
    }

    /// Compute the ikey (first 8 bytes as big-endian u64).
    #[inline(always)]
    #[expect(
        clippy::indexing_slicing,
        reason = "len is bounded by min(key.len(), 8), so slicing is safe"
    )]
    fn compute_ikey(key: &[u8]) -> u64 {
        let mut buf: [u8; 8] = [0u8; 8];
        let len: usize = key.len().min(8);
        buf[..len].copy_from_slice(&key[..len]);

        u64::from_be_bytes(buf)
    }

    /// Get the cached ikey.
    #[inline(always)]
    pub const fn ikey(&self) -> u64 {
        self.ikey
    }

    /// Check if this key has a suffix (> 8 bytes).
    #[inline(always)]
    pub const fn has_suffix(&self) -> bool {
        self.key.len() > 8
    }
}

/// A single entry in a batch insert operation that defers output allocation.
#[must_use]
struct BatchValueEntry<P: LeafPolicy> {
    /// The key bytes (owned for sorting).
    key: Vec<u8>,

    /// Deferred value payload. Taken only when a new slot is inserted or a
    /// slow-path retry must fall back to the generic insert path.
    value: Option<P::Value>,

    /// Cached ikey for the first 8 bytes (used for sorting).
    ikey: u64,
}

impl<P: LeafPolicy> BatchValueEntry<P> {
    #[inline]
    fn new(key: Vec<u8>, value: P::Value) -> Self {
        let ikey: u64 = BatchEntry::<P>::compute_ikey(&key);

        Self {
            key,
            value: Some(value),
            ikey,
        }
    }

    #[inline(always)]
    const fn ikey(&self) -> u64 {
        self.ikey
    }

    #[inline(always)]
    fn value_ref(&self) -> &P::Value {
        debug_assert!(self.value.is_some(), "batch value already consumed");
        // SAFETY: value is always Some until take_value is called exactly once.
        // The batch processing loop guarantees each entry hits either the update
        // path (value_ref) or the insert path (take_value), never both.
        unsafe { self.value.as_ref().unwrap_unchecked() }
    }

    #[inline(always)]
    fn take_value(&mut self) -> P::Value {
        debug_assert!(self.value.is_some(), "batch value already consumed");
        // SAFETY: see value_ref. Each entry is consumed exactly once.
        unsafe { self.value.take().unwrap_unchecked() }
    }
}

// ============================================================================
//  Batch Insert Result
// ============================================================================

/// Result of a batch insert operation.
///
/// # Type Parameter
///
/// * `O` - The output type (`ValuePtr<V>` for Box mode, `V` for Inline mode)
#[derive(Debug, Clone)]
#[must_use]
pub struct BatchInsertResult<O> {
    /// Number of new keys inserted.
    pub inserted: usize,

    /// Number of existing keys updated.
    pub updated: usize,

    /// Old values from updated keys (in no particular order).
    pub old_values: Vec<O>,

    /// Number of entries that failed and need individual retry.
    pub failed: usize,
}

impl<O> Default for BatchInsertResult<O> {
    fn default() -> Self {
        Self {
            inserted: 0,
            updated: 0,
            old_values: Vec::new(),
            failed: 0,
        }
    }
}

impl<O> BatchInsertResult<O> {
    /// Create a new empty result.
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a result with pre-allocated capacity for old values.
    #[inline(always)]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inserted: 0,
            updated: 0,
            old_values: Vec::with_capacity(capacity),
            failed: 0,
        }
    }

    /// Record a successful new key insertion.
    #[inline(always)]
    pub const fn record_insert(&mut self) {
        self.inserted += 1;
    }

    /// Record a successful update with old value.
    #[inline]
    pub fn record_update(&mut self, old_value: O) {
        self.updated += 1;
        self.old_values.push(old_value);
    }

    /// Record a failed entry.
    #[inline(always)]
    pub const fn record_failure(&mut self) {
        self.failed += 1;
    }

    /// Total entries processed (inserted + updated + failed).
    #[must_use]
    #[inline(always)]
    pub const fn total(&self) -> usize {
        self.inserted + self.updated + self.failed
    }

    /// Check if all entries succeeded.
    #[must_use]
    #[inline(always)]
    pub const fn all_succeeded(&self) -> bool {
        self.failed == 0
    }
}

// ============================================================================
//  Helper Types
// ============================================================================

/// Result of trying to insert a single entry in a batch.
enum BatchEntryResult<O> {
    /// Entry was inserted as a new key. Contains a pointer to a suffix bag
    /// that must be retired after the lock is dropped (null if none).
    Inserted(*mut u8),

    /// Entry updated an existing key, returning the old value.
    Updated(O),

    /// Leaf is full, need to stop batch and retry after split.
    NeedsSplit,

    /// Layer descent needed - mark for individual retry.
    NeedsLayerDescent,

    /// Slot is being modified by another thread, retry.
    Retry,
}

// FindSlotResult and MembershipError are shared with insert.rs, defined in super (generic.rs).

// ============================================================================
//  Batch Operations Trait (unifies generic and write-through batch paths)
// ============================================================================

/// Abstraction over batch entry access patterns.
///
/// `GenericBatch` wraps `&[BatchEntry<P>]` (pre-allocated outputs).
/// `ValueBatch` wraps `&mut [BatchValueEntry<P>]` (deferred allocation).
/// The unified `process_sorted_batch_inner` and `insert_batch_into_locked_leaf_inner`
/// are parameterized over `B: BatchOps` to eliminate duplicated traversal logic.
trait BatchOps<P: LeafPolicy, A: TreeAllocator<P>> {
    type OldValue;

    fn len(&self) -> usize;
    fn entry_ikey(&self, index: usize) -> u64;
    fn entry_key_bytes(&self, index: usize) -> &[u8];

    fn can_reuse_empty_at(
        &self,
        index: usize,
        tree: &MassTreeGeneric<P, A>,
        leaf: &LeafNode15<P>,
    ) -> bool;

    fn insert_empty_leaf_at(
        &mut self,
        index: usize,
        tree: &MassTreeGeneric<P, A>,
        leaf: &LeafNode15<P>,
        lock: &mut LockGuard<'_>,
        guard: &LocalGuard<'_>,
    ) -> *mut u8;

    #[expect(
        clippy::too_many_arguments,
        reason = "Batch entry insertion requires full context"
    )]
    fn try_insert_at(
        &mut self,
        index: usize,
        tree: &MassTreeGeneric<P, A>,
        leaf: &LeafNode15<P>,
        lock: &mut LockGuard<'_>,
        perm: &mut Permuter,
        guard: &LocalGuard<'_>,
        pre_allocated: Option<Vec<u8>>,
    ) -> BatchEntryResult<Self::OldValue>;

    fn fallback_insert_at(
        &mut self,
        index: usize,
        tree: &MassTreeGeneric<P, A>,
        result: &mut BatchInsertResult<Self::OldValue>,
        guard: &LocalGuard<'_>,
    );
}

// ============================================================================
//  GenericBatch: wraps &[BatchEntry<P>] (pre-allocated outputs)
// ============================================================================

struct GenericBatch<'a, P: LeafPolicy>(&'a [BatchEntry<P>]);

impl<P: LeafPolicy, A: TreeAllocator<P>> BatchOps<P, A> for GenericBatch<'_, P> {
    type OldValue = P::Output;

    #[inline(always)]
    fn len(&self) -> usize {
        self.0.len()
    }

    #[inline(always)]
    fn entry_ikey(&self, index: usize) -> u64 {
        self.0[index].ikey()
    }

    #[inline(always)]
    fn entry_key_bytes(&self, index: usize) -> &[u8] {
        &self.0[index].key
    }

    #[inline]
    fn can_reuse_empty_at(
        &self,
        index: usize,
        tree: &MassTreeGeneric<P, A>,
        leaf: &LeafNode15<P>,
    ) -> bool {
        let key: Key<'_> = Key::new(&self.0[index].key);
        tree.can_reuse_empty_leaf(leaf, &key)
    }

    #[inline]
    fn insert_empty_leaf_at(
        &mut self,
        index: usize,
        tree: &MassTreeGeneric<P, A>,
        leaf: &LeafNode15<P>,
        lock: &mut LockGuard<'_>,
        guard: &LocalGuard<'_>,
    ) -> *mut u8 {
        let entry: &BatchEntry<P> = &self.0[index];
        let key: Key<'_> = Key::new(&entry.key);
        tree.insert_into_empty_leaf_batch(leaf, lock, &key, &entry.output, guard)
    }

    #[inline]
    fn try_insert_at(
        &mut self,
        index: usize,
        tree: &MassTreeGeneric<P, A>,
        leaf: &LeafNode15<P>,
        lock: &mut LockGuard<'_>,
        perm: &mut Permuter,
        guard: &LocalGuard<'_>,
        pre_allocated: Option<Vec<u8>>,
    ) -> BatchEntryResult<P::Output> {
        let entry: &BatchEntry<P> = &self.0[index];
        let key: Key<'_> = Key::new(&entry.key);
        let single_layer_mode: bool = !key.has_suffix();

        let search_result: InsertSearchResultGeneric = if single_layer_mode {
            tree.search_for_insert_single_layer(leaf, &key, perm)
        } else {
            tree.search_for_insert_generic(leaf, &key, perm)
        };

        match search_result {
            InsertSearchResultGeneric::Found { slot } => {
                if leaf.is_value_empty(slot) {
                    return BatchEntryResult::Retry;
                }

                let old_value: P::Output =
                    tree.update_existing_value(leaf, lock, slot, &entry.output, guard);

                BatchEntryResult::Updated(old_value)
            }

            InsertSearchResultGeneric::NotFound { logical_pos } => {
                let ikey: u64 = key.ikey();

                match tree.find_usable_slot(leaf, perm, ikey) {
                    FindSlotResult::Found { slot, back_offset } => {
                        let deferred_retire: *mut u8 = tree.insert_new_value(
                            leaf,
                            lock,
                            slot,
                            back_offset,
                            logical_pos,
                            *perm,
                            &key,
                            &entry.output,
                            guard,
                            pre_allocated,
                        );
                        tree.count.increment();
                        *perm = leaf.permutation();

                        BatchEntryResult::Inserted(deferred_retire)
                    }

                    FindSlotResult::NeedsSplit => BatchEntryResult::NeedsSplit,
                }
            }

            InsertSearchResultGeneric::Layer { .. }
            | InsertSearchResultGeneric::Conflict { .. } => {
                if single_layer_mode {
                    BatchEntryResult::Retry
                } else {
                    BatchEntryResult::NeedsLayerDescent
                }
            }
        }
    }

    fn fallback_insert_at(
        &mut self,
        index: usize,
        tree: &MassTreeGeneric<P, A>,
        result: &mut BatchInsertResult<P::Output>,
        guard: &LocalGuard<'_>,
    ) {
        let entry: &BatchEntry<P> = &self.0[index];
        let mut key: Key<'_> = Key::new(&entry.key);

        match tree.insert_concurrent_generic(&mut key, entry.output.clone(), guard) {
            Ok(old) => {
                if let Some(old_value) = old {
                    result.record_update(old_value);
                } else {
                    result.record_insert();
                }
            }

            Err(e) => {
                panic!("Batch insert failed unexpectedly: {e:?}. This indicates a bug.");
            }
        }
    }
}

// ============================================================================
//  ValueBatch: wraps &mut [BatchValueEntry<P>] (deferred allocation)
// ============================================================================

struct ValueBatch<'a, P: LeafPolicy>(&'a mut [BatchValueEntry<P>]);

impl<P: LeafPolicy, A: TreeAllocator<P>> BatchOps<P, A> for ValueBatch<'_, P> {
    type OldValue = P::Value;

    #[inline(always)]
    fn len(&self) -> usize {
        self.0.len()
    }

    #[inline(always)]
    fn entry_ikey(&self, index: usize) -> u64 {
        self.0[index].ikey()
    }

    #[inline(always)]
    fn entry_key_bytes(&self, index: usize) -> &[u8] {
        &self.0[index].key
    }

    #[inline]
    fn can_reuse_empty_at(
        &self,
        index: usize,
        tree: &MassTreeGeneric<P, A>,
        leaf: &LeafNode15<P>,
    ) -> bool {
        let key: Key<'_> = Key::new(&self.0[index].key);
        tree.can_reuse_empty_leaf(leaf, &key)
    }

    #[inline]
    fn insert_empty_leaf_at(
        &mut self,
        index: usize,
        tree: &MassTreeGeneric<P, A>,
        leaf: &LeafNode15<P>,
        lock: &mut LockGuard<'_>,
        guard: &LocalGuard<'_>,
    ) -> *mut u8 {
        let value: P::Value = self.0[index].take_value();
        let key: Key<'_> = Key::new(&self.0[index].key);
        let output: P::Output = P::into_output(value);
        tree.insert_into_empty_leaf_batch(leaf, lock, &key, &output, guard)
    }

    #[inline]
    fn try_insert_at(
        &mut self,
        index: usize,
        tree: &MassTreeGeneric<P, A>,
        leaf: &LeafNode15<P>,
        lock: &mut LockGuard<'_>,
        perm: &mut Permuter,
        guard: &LocalGuard<'_>,
        pre_allocated: Option<Vec<u8>>,
    ) -> BatchEntryResult<P::Value> {
        let key: Key<'_> = Key::new(&self.0[index].key);
        let single_layer_mode: bool = !key.has_suffix();

        let search_result: InsertSearchResultGeneric = if single_layer_mode {
            tree.search_for_insert_single_layer(leaf, &key, perm)
        } else {
            tree.search_for_insert_generic(leaf, &key, perm)
        };

        match search_result {
            InsertSearchResultGeneric::Found { slot } => {
                if leaf.is_value_empty(slot) {
                    return BatchEntryResult::Retry;
                }

                let old_value: P::Value = tree.update_existing_value_write_through(
                    leaf,
                    lock,
                    slot,
                    self.0[index].value_ref(),
                );

                BatchEntryResult::Updated(old_value)
            }

            InsertSearchResultGeneric::NotFound { logical_pos } => {
                let ikey: u64 = key.ikey();

                match tree.find_usable_slot(leaf, perm, ikey) {
                    FindSlotResult::Found { slot, back_offset } => {
                        let output: P::Output = P::into_output(self.0[index].take_value());
                        let insert_key: Key<'_> = Key::new(&self.0[index].key);
                        let deferred_retire: *mut u8 = tree.insert_new_value(
                            leaf,
                            lock,
                            slot,
                            back_offset,
                            logical_pos,
                            *perm,
                            &insert_key,
                            &output,
                            guard,
                            pre_allocated,
                        );
                        tree.count.increment();
                        *perm = leaf.permutation();

                        BatchEntryResult::Inserted(deferred_retire)
                    }

                    FindSlotResult::NeedsSplit => BatchEntryResult::NeedsSplit,
                }
            }

            InsertSearchResultGeneric::Layer { .. }
            | InsertSearchResultGeneric::Conflict { .. } => {
                if single_layer_mode {
                    BatchEntryResult::Retry
                } else {
                    BatchEntryResult::NeedsLayerDescent
                }
            }
        }
    }

    fn fallback_insert_at(
        &mut self,
        index: usize,
        tree: &MassTreeGeneric<P, A>,
        result: &mut BatchInsertResult<P::Value>,
        guard: &LocalGuard<'_>,
    ) {
        let entry: &mut BatchValueEntry<P> = &mut self.0[index];
        let value: P::Value = entry.take_value();
        let mut key: Key<'_> = Key::new(&entry.key);

        match tree.insert_concurrent_value(&mut key, value, guard) {
            Ok(old) => {
                if let Some(old_value) = old {
                    result.record_update(old_value);
                } else {
                    result.record_insert();
                }
            }

            Err(e) => {
                panic!("Batch insert failed unexpectedly: {e:?}. This indicates a bug.");
            }
        }
    }
}

// ============================================================================
//  Batch Insert Implementation
// ============================================================================

impl<P, A> MassTreeGeneric<P, A>
where
    P: LeafPolicy,
    A: TreeAllocator<P>,
{
    // ========================================================================
    //  Public Batch Insert API
    // ========================================================================

    /// Insert multiple key-value pairs in a single batch operation.
    ///
    /// # Example
    ///
    /// ```rust
    /// use masstree::MassTree15;
    ///
    /// let tree: MassTree15<u64> = MassTree15::new();
    ///
    /// let entries = vec![
    ///     (b"key1".to_vec(), 1u64),
    ///     (b"key2".to_vec(), 2u64),
    ///     (b"key3".to_vec(), 3u64),
    /// ];
    ///
    /// let result = tree.insert_batch(entries);
    /// assert_eq!(result.inserted, 3);
    /// assert_eq!(result.updated, 0);
    /// ```
    ///
    /// # Panics
    ///
    /// Panics on internal tree corruption (should not happen in normal operation).
    pub fn insert_batch<I>(&self, entries: I) -> BatchInsertResult<P::Value>
    where
        I: IntoIterator<Item = (Vec<u8>, P::Value)>,
        P::Value: Clone,
    {
        let guard: LocalGuard<'_> = self.guard();
        if P::CAN_WRITE_THROUGH {
            self.insert_batch_values_with_guard(entries, &guard)
        } else {
            let result: BatchInsertResult<P::Output> =
                self.insert_batch_with_guard(entries, &guard);

            BatchInsertResult {
                inserted: result.inserted,
                updated: result.updated,
                old_values: result
                    .old_values
                    .iter()
                    .map(|o: &P::Output| P::clone_value_from_output(o))
                    .collect(),
                failed: result.failed,
            }
        }
    }

    /// Insert multiple key-value pairs using an existing guard.
    ///
    /// Use this when performing multiple batch operations under the same
    /// guard to amortize guard creation overhead.
    ///
    /// # Panics
    ///
    /// Panics on internal tree corruption.
    pub fn insert_batch_with_guard<I>(
        &self,
        entries: I,
        guard: &LocalGuard<'_>,
    ) -> BatchInsertResult<P::Output>
    where
        I: IntoIterator<Item = (Vec<u8>, P::Value)>,
    {
        self.verify_guard(guard);
        if P::CAN_WRITE_THROUGH {
            let value_result: BatchInsertResult<P::Value> =
                self.insert_batch_values_with_guard(entries, guard);

            return BatchInsertResult {
                inserted: value_result.inserted,
                updated: value_result.updated,
                old_values: value_result
                    .old_values
                    .into_iter()
                    .map(P::into_output)
                    .collect(),
                failed: value_result.failed,
            };
        }

        // Convert to BatchEntry (allocates outputs once)
        let mut batch: Vec<BatchEntry<P>> = entries
            .into_iter()
            .map(|(key, value): (Vec<u8>, P::Value)| BatchEntry::new(key, value))
            .collect();

        if batch.is_empty() {
            return BatchInsertResult::new();
        }

        // Sort by ikey for cache locality and leaf clustering
        batch.sort_unstable_by_key(BatchEntry::ikey);

        // Process the sorted batch
        self.process_sorted_batch(&batch, guard)
    }

    fn insert_batch_values_with_guard<I>(
        &self,
        entries: I,
        guard: &LocalGuard<'_>,
    ) -> BatchInsertResult<P::Value>
    where
        I: IntoIterator<Item = (Vec<u8>, P::Value)>,
    {
        let mut batch: Vec<BatchValueEntry<P>> = entries
            .into_iter()
            .map(|(key, value): (Vec<u8>, P::Value)| BatchValueEntry::new(key, value))
            .collect();

        if batch.is_empty() {
            return BatchInsertResult::new();
        }

        batch.sort_unstable_by_key(BatchValueEntry::ikey);
        self.process_sorted_value_batch(&mut batch, guard)
    }

    /// Insert pre-constructed batch entries.
    ///
    /// Takes ownership of the entries to avoid cloning keys and outputs.
    /// Use this when you need finer control over the batch entries,
    /// or when retrying failed entries from a previous batch.
    pub fn insert_batch_entries(
        &self,
        mut entries: Vec<BatchEntry<P>>,
        guard: &LocalGuard<'_>,
    ) -> BatchInsertResult<P::Output> {
        self.verify_guard(guard);
        if entries.is_empty() {
            return BatchInsertResult::new();
        }

        // Sort by ikey for cache locality
        entries.sort_unstable_by_key(BatchEntry::ikey);

        self.process_sorted_batch(&entries, guard)
    }

    // ========================================================================
    //  Internal Batch Processing
    // ========================================================================

    /// Process a sorted batch of entries (generic path).
    ///
    /// Delegates to `process_sorted_batch_inner` via `GenericBatch`.
    fn process_sorted_batch(
        &self,
        batch: &[BatchEntry<P>],
        guard: &LocalGuard<'_>,
    ) -> BatchInsertResult<P::Output> {
        let mut ops: GenericBatch<'_, P> = GenericBatch(batch);
        self.process_sorted_batch_inner(&mut ops, guard)
    }

    /// Process a sorted batch of value entries (write-through path).
    ///
    /// Delegates to `process_sorted_batch_inner` via `ValueBatch`.
    fn process_sorted_value_batch(
        &self,
        batch: &mut [BatchValueEntry<P>],
        guard: &LocalGuard<'_>,
    ) -> BatchInsertResult<P::Value> {
        let mut ops: ValueBatch<'_, P> = ValueBatch(batch);
        self.process_sorted_batch_inner(&mut ops, guard)
    }

    /// Unified sorted batch processing, parameterized by `BatchOps`.
    ///
    /// Handles traversal, OCC validation, locking, locked-leaf insertion,
    /// retirement, and fallback to individual insert on split.
    fn process_sorted_batch_inner<B: BatchOps<P, A>>(
        &self,
        batch: &mut B,
        guard: &LocalGuard<'_>,
    ) -> BatchInsertResult<B::OldValue> {
        let mut result: BatchInsertResult<B::OldValue> =
            BatchInsertResult::with_capacity(batch.len() / 4);
        let mut index: usize = 0;

        while index < batch.len() {
            let key: Key<'_> = Key::new(batch.entry_key_bytes(index));

            let mut layer_root: *const u8 = self.root_ptr.load(AtomicOrdering::Acquire);

            let entries_processed: usize = 'retry: loop {
                layer_root = self.maybe_parent_generic(layer_root);

                let mut leaf_ptr: *mut LeafNode15<P> =
                    self.reach_leaf_concurrent_generic(layer_root, &key, false, guard);

                let (advanced_ptr, exceeded_hop_limit) =
                    self.advance_to_key_by_bound_generic(leaf_ptr, &key, guard);

                if exceeded_hop_limit {
                    layer_root = self.root_ptr.load(AtomicOrdering::Acquire);
                    continue 'retry;
                }

                leaf_ptr = advanced_ptr;
                let leaf: &LeafNode15<P> = unsafe { &*leaf_ptr };

                let pre_lock_version: u32 = leaf.version().stable();
                let pre_lock_perm_raw: u64 = leaf.permutation_raw();
                let pre_lock_perm: Permuter = leaf.permutation();

                let pre_allocated_vec: Option<Vec<u8>> = if key.has_suffix() {
                    Self::maybe_pre_allocate_suffix(&key, pre_lock_perm.size())
                } else {
                    None
                };

                let mut lock = leaf.version().lock_bounded();

                if !self.validate_post_lock(leaf, pre_lock_version, pre_lock_perm_raw) {
                    drop(lock);
                    continue 'retry;
                }

                if leaf.deleted_layer() {
                    drop(lock);
                    layer_root = self.root_ptr.load(AtomicOrdering::Acquire);
                    continue 'retry;
                }

                if self.validate_membership(leaf, &key).is_err() {
                    drop(lock);
                    continue 'retry;
                }

                let mut retire_buf = RetireBuf::new();
                let processed: usize = self.insert_batch_into_locked_leaf_inner(
                    leaf,
                    &mut lock,
                    batch,
                    index,
                    &mut result,
                    &mut retire_buf,
                    guard,
                    pre_allocated_vec,
                );

                drop(lock);

                // Retire old suffix bags OUTSIDE the lock
                for &ptr in retire_buf.as_slice() {
                    // SAFETY: ptr is a valid suffix bag pointer from a completed operation.
                    unsafe {
                        LeafNode15::<P>::retire_suffix_bag_ptr(ptr, guard);
                    }
                }

                if processed == 0 {
                    break 'retry 0;
                }

                break 'retry processed;
            };

            if entries_processed == 0 {
                batch.fallback_insert_at(index, self, &mut result, guard);
                index += 1;
            } else {
                index += entries_processed;
            }
        }

        result
    }

    /// Unified locked-leaf batch insertion, parameterized by `BatchOps`.
    ///
    /// Inserts as many entries as possible into a locked leaf. Handles
    /// empty leaf reuse, upper bound clustering, and per-entry dispatch.
    #[expect(
        clippy::too_many_arguments,
        reason = "Batch insertion requires context"
    )]
    fn insert_batch_into_locked_leaf_inner<B: BatchOps<P, A>>(
        &self,
        leaf: &LeafNode15<P>,
        lock: &mut LockGuard<'_>,
        batch: &mut B,
        start_index: usize,
        result: &mut BatchInsertResult<B::OldValue>,
        retire_buf: &mut RetireBuf,
        guard: &LocalGuard<'_>,
        pre_allocated: Option<Vec<u8>>,
    ) -> usize {
        let mut processed: usize = 0;
        let mut perm: Permuter = leaf.permutation();
        let mut pre_allocated_vec: Option<Vec<u8>> = pre_allocated;

        // Determine the ikey upper bound for this leaf
        // SAFETY: Called under lock - no concurrent retirement.
        let next_raw: *mut LeafNode15<P> = unsafe { leaf.next_raw_unguarded() };
        let next_ptr: *mut LeafNode15<P> = Linker::unmark_ptr(next_raw);
        let upper_bound: Option<u64> = if next_ptr.is_null() {
            None
        } else {
            // SAFETY: next_ptr is valid, protected by guard
            Some(unsafe { (*next_ptr).ikey_bound() })
        };

        if leaf.is_empty()
            && start_index < batch.len()
            && batch.can_reuse_empty_at(start_index, self, leaf)
        {
            let deferred: *mut u8 =
                batch.insert_empty_leaf_at(start_index, self, leaf, lock, guard);

            if !deferred.is_null() {
                retire_buf.push(deferred);
            }

            result.record_insert();
            processed = 1;
            perm = leaf.permutation();
        }

        while start_index + processed < batch.len() {
            let entry_index: usize = start_index + processed;

            if let Some(bound) = upper_bound
                && batch.entry_ikey(entry_index) >= bound
            {
                break;
            }

            if perm.size() >= LeafNode15::<P>::WIDTH {
                break;
            }

            let insert_result = batch.try_insert_at(
                entry_index,
                self,
                leaf,
                lock,
                &mut perm,
                guard,
                pre_allocated_vec.take(),
            );

            match insert_result {
                BatchEntryResult::Inserted(deferred) => {
                    if !deferred.is_null() {
                        retire_buf.push(deferred);
                    }

                    result.record_insert();
                    processed += 1;
                }

                BatchEntryResult::Updated(old_value) => {
                    result.record_update(old_value);
                    processed += 1;
                }

                BatchEntryResult::NeedsSplit
                | BatchEntryResult::NeedsLayerDescent
                | BatchEntryResult::Retry => {
                    break;
                }
            }
        }

        processed
    }

    /// Insert into an empty leaf. Returns a pointer to a suffix bag that must be
    /// retired after the lock is dropped (null if none).
    fn insert_into_empty_leaf_batch(
        &self,
        leaf: &LeafNode15<P>,
        lock: &mut LockGuard<'_>,
        key: &Key<'_>,
        value: &P::Output,
        guard: &LocalGuard<'_>,
    ) -> *mut u8 {
        leaf.clear_empty_state();
        let slot: usize = 0;
        let deferred_retire: *mut u8 =
            self.assign_slot_generic(leaf, lock, slot, key, value, guard, None);

        let new_perm: Permuter = <LeafNode15<P> as TreeLeafNode<P>>::Perm::make_sorted(1);
        leaf.set_permutation_relaxed(new_perm);
        self.count.increment();

        deferred_retire
    }
}
