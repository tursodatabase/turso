//! Concurrent ordered map: an in-memory B+ tree with optimistic lock coupling.
//!
//! Design: Leis, Scheibner, Kemper, Neumann, "The ART of Practical Synchronization"
//! (DaMoN 2016) and Leis, Haubenschild, Neumann, "Optimistic Lock Coupling: A Scalable
//! and Efficient General-Purpose Synchronization Method" (IEEE Data Eng. Bull. 2019).
//!
//! Every node has a version word. Readers never write shared memory: they read the
//! version, read the node through atomics, and read the version again. A changed
//! version means that a writer was there, and the reader starts again. Writers lock only
//! the nodes that they change. Keys and values that leave the tree are dropped through
//! epoch-based reclamation, because a racing reader can still look at them.
//!
//! Nodes are never freed while the tree is alive (no merges). A cursor can thus keep a
//! pointer to its leaf between calls and continue in O(1) when the leaf version did not
//! change.

use std::borrow::Borrow;
use std::hint::spin_loop;
use std::marker::PhantomData;
use std::mem::ManuallyDrop;
use std::ops::{Bound, RangeBounds};
use std::ptr::{self, NonNull};
use std::sync::atomic::{fence, AtomicPtr, AtomicU16, AtomicU64, AtomicUsize, Ordering};

use crossbeam_epoch as epoch;

use crate::alloc::{ConcurrentAllocator, Layout, TryReserveError, TursoAllocator};
use crate::sync::Arc;

const LEAF_CAPACITY: usize = 64;
const INNER_CAPACITY: usize = 64;

/// Storage for one key in a node.
///
/// # Safety
///
/// A zeroed `Slot` must be a valid empty slot. `read` can run at the same time as
/// `write`, `move_from` and `take` on the same slot, so the slot must be made of atomics.
/// A reader that races with a writer must get either some key that was in the slot, or
/// `None`.
pub unsafe trait TreeKey: Ord + KeyPrefix + Clone + Send + Sync + 'static {
    type Slot: Send + Sync;

    /// True if a removed key can own memory that a racing reader still reads.
    const NEEDS_DEFERRED_DROP: bool;

    fn write(slot: &Self::Slot, key: Self);

    fn move_from(slot: &Self::Slot, from: &Self::Slot);

    /// Calls `f` with the key in the slot. Returns `None` for an empty slot.
    fn read<R>(slot: &Self::Slot, f: impl FnOnce(&Self) -> R) -> Option<R>;

    /// Moves the key out of the slot. Only a writer that holds the node lock calls it.
    fn take(slot: &Self::Slot) -> Self;

    /// Empties a slot whose key moved to another slot.
    fn clear(slot: &Self::Slot);

    /// The [`KeyPrefix`] of the key in the slot, if the node stores one.
    fn slot_prefix(_slot: &Self::Slot) -> Option<u64> {
        None
    }
}

/// A 64-bit summary of a key that keeps the order: if `a.prefix() < b.prefix()`, then
/// `a < b`. Equal summaries say nothing about the order. Nodes store the summary next
/// to each key, so a search reads a full key only when the summaries are equal.
///
/// This is the "partial key" idea of Bohannon, McIlroy and Rastogi, "Main-Memory Index
/// Structures with Fixed-Size Partial Keys" (SIGMOD 2001).
pub trait KeyPrefix {
    fn prefix(&self) -> Option<u64> {
        None
    }
}

impl<T: KeyPrefix + ?Sized> KeyPrefix for Arc<T> {
    fn prefix(&self) -> Option<u64> {
        (**self).prefix()
    }
}

/// Storage for one value in a leaf.
///
/// # Safety
///
/// Same rules as [`TreeKey`]. `clone_from_slot` runs while the caller holds an
/// [`epoch::Guard`]. Values that leave the tree are dropped only after every `Guard` that
/// existed at that time is dropped.
pub unsafe trait TreeValue: Clone + Send + Sync + 'static {
    type Slot: Send + Sync;

    fn write(slot: &Self::Slot, value: Self);

    fn move_from(slot: &Self::Slot, from: &Self::Slot);

    fn clone_from_slot(slot: &Self::Slot) -> Option<Self>;

    fn take(slot: &Self::Slot) -> Self;

    /// Empties a slot whose value moved to another slot.
    fn clear(slot: &Self::Slot);

    fn is_same(slot: &Self::Slot, value: &Self) -> bool;

    fn same_value(a: &Self, b: &Self) -> bool;

    /// Asks the CPU to load the memory of the value in the slot, before a clone of it.
    fn prefetch(_slot: &Self::Slot) {}
}

/// Key slot for `Arc<T>`: the key pointer and its [`KeyPrefix`]. The low bit of the
/// pointer is set when the prefix word holds a prefix.
pub struct ArcKeySlot<T> {
    prefix: AtomicU64,
    ptr: AtomicPtr<T>,
}

const HAS_PREFIX: usize = 1;

impl<T> ArcKeySlot<T> {
    fn key_ptr(&self) -> *mut T {
        self.ptr
            .load(Ordering::Acquire)
            .map_addr(|a| a & !HAS_PREFIX)
    }
}

unsafe impl<T: Ord + KeyPrefix + Send + Sync + 'static> TreeKey for Arc<T> {
    type Slot = ArcKeySlot<T>;

    const NEEDS_DEFERRED_DROP: bool = true;

    fn write(slot: &Self::Slot, key: Self) {
        let prefix = key.prefix();
        slot.prefix.store(prefix.unwrap_or(0), Ordering::Release);
        let ptr = Arc::into_raw(key).cast_mut();
        let ptr = ptr.map_addr(|a| a | usize::from(prefix.is_some()));
        slot.ptr.store(ptr, Ordering::Release);
    }

    fn move_from(slot: &Self::Slot, from: &Self::Slot) {
        slot.prefix
            .store(from.prefix.load(Ordering::Acquire), Ordering::Release);
        slot.ptr
            .store(from.ptr.load(Ordering::Acquire), Ordering::Release);
    }

    fn read<R>(slot: &Self::Slot, f: impl FnOnce(&Self) -> R) -> Option<R> {
        let ptr = slot.key_ptr();
        if ptr.is_null() {
            return None;
        }
        // SAFETY: the pointer came from `Arc::into_raw`, and the key is alive: removed
        // keys are dropped only after the epoch `Guard`s that exist now are dropped.
        let key = ManuallyDrop::new(unsafe { Arc::from_raw(ptr) });
        Some(f(&key))
    }

    fn take(slot: &Self::Slot) -> Self {
        let ptr = slot
            .ptr
            .swap(ptr::null_mut(), Ordering::AcqRel)
            .map_addr(|a| a & !HAS_PREFIX);
        assert!(!ptr.is_null(), "take from an empty key slot");
        // SAFETY: the slot owned one strong count. It moves to the returned Arc.
        unsafe { Arc::from_raw(ptr) }
    }

    fn clear(slot: &Self::Slot) {
        slot.ptr.store(ptr::null_mut(), Ordering::Release);
    }

    fn slot_prefix(slot: &Self::Slot) -> Option<u64> {
        let prefix = slot.prefix.load(Ordering::Acquire);
        (slot.ptr.load(Ordering::Acquire).addr() & HAS_PREFIX != 0).then_some(prefix)
    }
}

unsafe impl<T: Send + Sync + 'static> TreeValue for Arc<T> {
    type Slot = AtomicPtr<T>;

    fn write(slot: &Self::Slot, value: Self) {
        slot.store(Arc::into_raw(value).cast_mut(), Ordering::Release);
    }

    fn move_from(slot: &Self::Slot, from: &Self::Slot) {
        slot.store(from.load(Ordering::Acquire), Ordering::Release);
    }

    fn clone_from_slot(slot: &Self::Slot) -> Option<Self> {
        let ptr = slot.load(Ordering::Acquire);
        if ptr.is_null() {
            return None;
        }
        // SAFETY: the value is alive (see `TreeValue`), so its strong count is not zero.
        unsafe {
            Arc::increment_strong_count(ptr);
            Some(Arc::from_raw(ptr))
        }
    }

    fn take(slot: &Self::Slot) -> Self {
        let ptr = slot.swap(ptr::null_mut(), Ordering::AcqRel);
        assert!(!ptr.is_null(), "take from an empty value slot");
        // SAFETY: the slot owned one strong count. It moves to the returned Arc.
        unsafe { Arc::from_raw(ptr) }
    }

    fn clear(slot: &Self::Slot) {
        slot.store(ptr::null_mut(), Ordering::Release);
    }

    fn is_same(slot: &Self::Slot, value: &Self) -> bool {
        ptr::eq(slot.load(Ordering::Acquire), Arc::as_ptr(value))
    }

    fn same_value(a: &Self, b: &Self) -> bool {
        Arc::ptr_eq(a, b)
    }

    fn prefetch(slot: &Self::Slot) {
        let ptr = slot.load(Ordering::Acquire);
        if ptr.is_null() {
            return;
        }
        #[cfg(target_arch = "x86_64")]
        // SAFETY: a prefetch never faults, whatever the address.
        unsafe {
            std::arch::x86_64::_mm_prefetch::<{ std::arch::x86_64::_MM_HINT_T0 }>(ptr.cast::<i8>())
        };
    }
}

const LOCKED: u64 = 0b10;
const OBSOLETE: u64 = 0b01;

struct Restart;

#[repr(C)]
struct Header {
    version: AtomicU64,
    count: AtomicU16,
    is_leaf: bool,
}

impl Header {
    /// Waits while a writer holds the node, then returns the version.
    fn read_lock(&self) -> Result<u64, Restart> {
        let mut spins = 0u32;
        loop {
            let version = self.version.load(Ordering::Acquire);
            if version & OBSOLETE != 0 {
                return Err(Restart);
            }
            if version & LOCKED == 0 {
                return Ok(version);
            }
            backoff(&mut spins);
        }
    }

    fn check(&self, version: u64) -> Result<(), Restart> {
        fence(Ordering::Acquire);
        if self.version.load(Ordering::Relaxed) == version {
            Ok(())
        } else {
            Err(Restart)
        }
    }

    fn upgrade(&self, version: u64) -> Result<(), Restart> {
        match self.version.compare_exchange(
            version,
            version + LOCKED,
            Ordering::Acquire,
            Ordering::Relaxed,
        ) {
            Ok(_) => {
                fence(Ordering::Release);
                Ok(())
            }
            Err(_) => {
                let mut spins = 0u32;
                while self.version.load(Ordering::Relaxed) & LOCKED != 0 {
                    backoff(&mut spins);
                }
                Err(Restart)
            }
        }
    }

    fn write_unlock(&self) {
        self.version.fetch_add(LOCKED, Ordering::Release);
    }

    fn count(&self, capacity: usize) -> usize {
        (self.count.load(Ordering::Acquire) as usize).min(capacity)
    }

    fn set_count(&self, count: usize) {
        self.count.store(count as u16, Ordering::Release);
    }
}

fn backoff(spins: &mut u32) {
    if *spins < 64 {
        for _ in 0..(1 << (*spins / 8)) {
            spin_loop();
        }
    } else {
        std::thread::yield_now();
    }
    *spins += 1;
}

#[repr(C)]
struct Leaf<K: TreeKey, V: TreeValue> {
    header: Header,
    keys: [K::Slot; LEAF_CAPACITY],
    values: [V::Slot; LEAF_CAPACITY],
}

#[repr(C)]
struct Inner<K: TreeKey> {
    header: Header,
    keys: [K::Slot; INNER_CAPACITY],
    children: [AtomicPtr<Header>; INNER_CAPACITY + 1],
}

/// What a search looks for.
///
/// A separator in an inner node is the largest key of the child on its left. Forward
/// targets pick the first key that is not before the target. Backward targets pick the
/// last key before the target.
#[derive(Clone, Copy)]
enum Target<'q, Q: ?Sized> {
    First,
    Last,
    /// First key `>= q`.
    AtOrAfter(&'q Q),
    /// First key `> q`.
    After(&'q Q),
    /// Last key `<= q`.
    UpTo(&'q Q),
    /// Last key `< q`.
    Before(&'q Q),
}

impl<Q: Ord + KeyPrefix + ?Sized> Target<'_, Q> {
    fn prefix(&self) -> Option<u64> {
        match self {
            Target::First | Target::Last => None,
            Target::AtOrAfter(q) | Target::After(q) | Target::UpTo(q) | Target::Before(q) => {
                q.prefix()
            }
        }
    }

    fn is_forward(&self) -> bool {
        matches!(
            self,
            Target::First | Target::AtOrAfter(_) | Target::After(_)
        )
    }

    /// Index of the child to go down to, from the separators `keys[..count]`.
    /// `prefix` is `self.prefix()`.
    fn child_index<K: TreeKey + Borrow<Q>>(
        &self,
        keys: &[K::Slot],
        count: usize,
        prefix: Option<u64>,
    ) -> Option<usize> {
        match self {
            Target::First => Some(0),
            Target::Last => Some(count),
            Target::AtOrAfter(q) | Target::UpTo(q) | Target::Before(q) => {
                partition::<K>(keys, count, prefix, |k| k.borrow() < *q)
            }
            Target::After(q) => partition::<K>(keys, count, prefix, |k| k.borrow() <= *q),
        }
    }

    /// For a forward target, the index of the first match in `keys[..count]`, or `count`.
    /// For a backward target, one past the index of the last match, or `0`.
    /// `prefix` is `self.prefix()`.
    fn leaf_split<K: TreeKey + Borrow<Q>>(
        &self,
        keys: &[K::Slot],
        count: usize,
        prefix: Option<u64>,
    ) -> Option<usize> {
        match self {
            Target::First => Some(0),
            Target::Last => Some(count),
            Target::AtOrAfter(q) | Target::Before(q) => {
                partition::<K>(keys, count, prefix, |k| k.borrow() < *q)
            }
            Target::After(q) | Target::UpTo(q) => {
                partition::<K>(keys, count, prefix, |k| k.borrow() <= *q)
            }
        }
    }
}

/// Index of the first key in `keys[..count]` for which `is_left` is false. `None` means
/// that a slot was empty, so the node changed under the reader.
///
/// `probe_prefix` is the [`KeyPrefix`] of the searched key. When the prefix of a slot
/// differs from it, the order of the prefixes decides, and the key is not read. This is
/// correct for both `k < q` and `k <= q`, because different prefixes mean `k != q`.
fn partition<K: TreeKey>(
    keys: &[K::Slot],
    count: usize,
    probe_prefix: Option<u64>,
    is_left: impl Fn(&K) -> bool,
) -> Option<usize> {
    let mut lo = 0;
    let mut hi = count;
    while lo < hi {
        let mid = (lo + hi) / 2;
        let by_prefix = match (probe_prefix, K::slot_prefix(&keys[mid])) {
            (Some(probe), Some(slot)) if slot != probe => Some(slot < probe),
            _ => None,
        };
        let left = match by_prefix {
            Some(left) => left,
            None => K::read(&keys[mid], &is_left)?,
        };
        if left {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    Some(lo)
}

/// A separator key on the path to a leaf, kept as a place in an inner node.
struct Fence<K: TreeKey> {
    node: *const Inner<K>,
    version: u64,
    index: usize,
}

impl<K: TreeKey> Clone for Fence<K> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<K: TreeKey> Copy for Fence<K> {}

impl<K: TreeKey> Fence<K> {
    fn key(&self) -> Result<K, Restart> {
        // SAFETY: nodes live as long as the tree.
        let node = unsafe { &*self.node };
        let key = K::read(&node.keys[self.index], K::clone).ok_or(Restart)?;
        node.header.check(self.version)?;
        Ok(key)
    }
}

struct LeafVisit<K: TreeKey, V: TreeValue> {
    leaf: *const Leaf<K, V>,
    version: u64,
    count: usize,
    /// All keys in the leaf are `<=` this separator.
    high: Option<Fence<K>>,
    /// All keys in the leaf are `>` this separator.
    low: Option<Fence<K>>,
}

/// A place in a leaf, valid while the leaf version stays the same.
struct Position<K: TreeKey, V: TreeValue> {
    leaf: *const Leaf<K, V>,
    version: u64,
    index: usize,
}

impl<K: TreeKey, V: TreeValue> Clone for Position<K, V> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<K: TreeKey, V: TreeValue> Copy for Position<K, V> {}

enum Seek<K: TreeKey, V: TreeValue> {
    Found(Position<K, V>),
    /// The leaf had no match. Search again from this separator.
    Continue(K),
    End,
}

enum Insert<'a, K: TreeKey, V: TreeValue, A: ConcurrentAllocator> {
    Existing(Entry<'a, K, V, A>),
    Inserted(Entry<'a, K, V, A>),
}

pub struct BPlusTreeMap<K: TreeKey, V: TreeValue, A: ConcurrentAllocator = TursoAllocator> {
    root: AtomicPtr<Header>,
    len: AtomicUsize,
    alloc: A,
    _marker: PhantomData<(K, V)>,
}

// SAFETY: keys and values are Send + Sync, and all shared node state is atomic.
unsafe impl<K: TreeKey, V: TreeValue, A: ConcurrentAllocator> Send for BPlusTreeMap<K, V, A> {}
unsafe impl<K: TreeKey, V: TreeValue, A: ConcurrentAllocator> Sync for BPlusTreeMap<K, V, A> {}

impl<K: TreeKey, V: TreeValue> BPlusTreeMap<K, V, TursoAllocator> {
    pub fn new() -> Self {
        Self::new_in(TursoAllocator)
    }
}

impl<K: TreeKey, V: TreeValue> Default for BPlusTreeMap<K, V, TursoAllocator> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: TreeKey, V: TreeValue, A: ConcurrentAllocator> std::fmt::Debug for BPlusTreeMap<K, V, A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BPlusTreeMap")
            .field("len", &self.len())
            .finish()
    }
}

impl<K: TreeKey, V: TreeValue, A: ConcurrentAllocator> BPlusTreeMap<K, V, A> {
    pub fn new_in(alloc: A) -> Self {
        Self {
            root: AtomicPtr::new(ptr::null_mut()),
            len: AtomicUsize::new(0),
            alloc,
            _marker: PhantomData,
        }
    }

    pub fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn get<Q>(&self, key: &Q) -> Option<Entry<'_, K, V, A>>
    where
        K: Borrow<Q>,
        Q: Ord + KeyPrefix + ?Sized,
    {
        let _guard = epoch::pin();
        let prefix = key.prefix();
        loop {
            match self.try_get(key, prefix) {
                Ok(entry) => return entry,
                Err(Restart) => continue,
            }
        }
    }

    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Ord + KeyPrefix + ?Sized,
    {
        self.get(key).is_some()
    }

    pub fn front(&self) -> Option<Entry<'_, K, V, A>> {
        self.iter().next()
    }

    pub fn back(&self) -> Option<Entry<'_, K, V, A>> {
        self.iter().next_back()
    }

    /// First entry that is inside `bound` as a lower bound.
    pub fn lower_bound<Q>(&self, bound: Bound<&Q>) -> Option<Entry<'_, K, V, A>>
    where
        K: Borrow<Q>,
        Q: Ord + KeyPrefix + ?Sized,
    {
        self.range::<Q, _>((bound, Bound::Unbounded)).next()
    }

    /// Last entry that is inside `bound` as an upper bound.
    pub fn upper_bound<Q>(&self, bound: Bound<&Q>) -> Option<Entry<'_, K, V, A>>
    where
        K: Borrow<Q>,
        Q: Ord + KeyPrefix + ?Sized,
    {
        self.range::<Q, _>((Bound::Unbounded, bound)).next_back()
    }

    pub fn iter(&self) -> Range<'_, K, (Bound<K>, Bound<K>), K, V, A> {
        self.range((Bound::Unbounded, Bound::Unbounded))
    }

    pub fn range<Q, R>(&self, range: R) -> Range<'_, Q, R, K, V, A>
    where
        K: Borrow<Q>,
        R: RangeBounds<Q>,
        Q: Ord + KeyPrefix + ?Sized,
    {
        Range {
            map: self,
            range,
            front: Side::new(),
            back: Side::new(),
            _marker: PhantomData,
        }
    }

    /// Calls `f` for each entry from `start`, in order, until `f` returns false. Holds
    /// one epoch pin for the whole walk, so it is cheaper than an iterator for bulk work.
    pub fn for_each_from<Q>(&self, start: Bound<&Q>, mut f: impl FnMut(&K, &V) -> bool)
    where
        K: Borrow<Q>,
        Q: Ord + KeyPrefix + ?Sized,
    {
        let _guard = epoch::pin();
        let mut next = match start {
            Bound::Unbounded => self.seek::<Q>(Target::First),
            Bound::Included(q) => self.seek(Target::AtOrAfter(q)),
            Bound::Excluded(q) => self.seek(Target::After(q)),
        };
        while let Some(pos) = next {
            // SAFETY: nodes live as long as the tree.
            let leaf = unsafe { &*pos.leaf };
            let Some(key) = K::read(&leaf.keys[pos.index], K::clone) else {
                next = None;
                continue;
            };
            let value_ptr = &leaf.values[pos.index];
            let Some(value) = V::clone_from_slot(value_ptr) else {
                next = self.seek::<K>(Target::After(&key));
                continue;
            };
            if leaf.header.check(pos.version).is_err() {
                next = self.seek::<K>(Target::AtOrAfter(&key));
                continue;
            }
            if !f(&key, &value) {
                return;
            }
            next = match self.step(pos, true) {
                Some(p) => Some(p),
                None => self.seek::<K>(Target::After(&key)),
            };
        }
    }

    /// Inserts `value` for `key`. An existing value for `key` is replaced.
    pub fn insert(&self, key: K, value: V) -> Entry<'_, K, V, A> {
        self.try_insert(key, value)
            .expect(crate::alloc::ALLOC_ERR_MSG)
    }

    /// Inserts `value` for `key`. An existing value for `key` is replaced.
    pub fn try_insert(&self, key: K, value: V) -> Result<Entry<'_, K, V, A>, TryReserveError> {
        let mut value = Some(value);
        match self.insert_with(key, || value.take().expect("value is used once"), true)? {
            Insert::Existing(entry) | Insert::Inserted(entry) => Ok(entry),
        }
    }

    pub fn get_or_insert_with(&self, key: K, value: impl FnOnce() -> V) -> Entry<'_, K, V, A> {
        self.try_get_or_insert_with(key, value)
            .expect(crate::alloc::ALLOC_ERR_MSG)
    }

    pub fn try_get_or_insert_with(
        &self,
        key: K,
        value: impl FnOnce() -> V,
    ) -> Result<Entry<'_, K, V, A>, TryReserveError> {
        match self.insert_with(key, value, false)? {
            Insert::Existing(entry) | Insert::Inserted(entry) => Ok(entry),
        }
    }

    pub fn remove<Q>(&self, key: &Q) -> Option<Entry<'_, K, V, A>>
    where
        K: Borrow<Q>,
        Q: Ord + KeyPrefix + ?Sized,
    {
        self.remove_if(key, |_| true)
    }

    /// Removes every entry. Concurrent inserts can survive.
    pub fn clear(&self) {
        while let Some(entry) = self.front() {
            entry.remove();
        }
    }

    fn try_get<Q>(
        &self,
        key: &Q,
        prefix: Option<u64>,
    ) -> Result<Option<Entry<'_, K, V, A>>, Restart>
    where
        K: Borrow<Q>,
        Q: Ord + KeyPrefix + ?Sized,
    {
        let target = Target::AtOrAfter(key);
        let Some(visit) = self.find_leaf(&target, prefix)? else {
            return Ok(None);
        };
        // SAFETY: nodes live as long as the tree.
        let leaf = unsafe { &*visit.leaf };
        let index = target
            .leaf_split::<K>(&leaf.keys, visit.count, prefix)
            .ok_or(Restart)?;
        let is_match = index < visit.count
            && K::read(&leaf.keys[index], |k| k.borrow() == key).ok_or(Restart)?;
        if !is_match {
            leaf.header.check(visit.version)?;
            return Ok(None);
        }
        self.entry_at(Position {
            leaf: visit.leaf,
            version: visit.version,
            index,
        })
        .map(Some)
    }

    /// Reads the key and value at `pos`, and checks that the leaf did not change.
    fn entry_at(&self, pos: Position<K, V>) -> Result<Entry<'_, K, V, A>, Restart> {
        // SAFETY: nodes live as long as the tree.
        let leaf = unsafe { &*pos.leaf };
        let key = K::read(&leaf.keys[pos.index], K::clone).ok_or(Restart)?;
        let value = V::clone_from_slot(&leaf.values[pos.index]).ok_or(Restart)?;
        leaf.header.check(pos.version)?;
        Ok(Entry {
            key,
            value,
            map: self,
        })
    }

    /// Goes from the root to the leaf where `target` belongs. `None` means an empty tree.
    fn find_leaf<Q>(
        &self,
        target: &Target<'_, Q>,
        prefix: Option<u64>,
    ) -> Result<Option<LeafVisit<K, V>>, Restart>
    where
        K: Borrow<Q>,
        Q: Ord + KeyPrefix + ?Sized,
    {
        let root = self.root.load(Ordering::Acquire);
        if root.is_null() {
            return Ok(None);
        }
        // SAFETY: nodes live as long as the tree.
        let mut node = unsafe { &*root };
        let mut version = node.read_lock()?;
        if !ptr::eq(root, self.root.load(Ordering::Acquire)) {
            return Err(Restart);
        }
        let mut high = None;
        let mut low = None;
        while !node.is_leaf {
            // SAFETY: `is_leaf` is false, so the node is an `Inner`.
            let inner = unsafe { &*ptr::from_ref(node).cast::<Inner<K>>() };
            let count = node.count(INNER_CAPACITY);
            let index = target
                .child_index::<K>(&inner.keys, count, prefix)
                .ok_or(Restart)?;
            let child = inner.children[index].load(Ordering::Acquire);
            node.check(version)?;
            if child.is_null() {
                return Err(Restart);
            }
            if index < count {
                high = Some(Fence {
                    node: inner,
                    version,
                    index,
                });
            }
            if index > 0 {
                low = Some(Fence {
                    node: inner,
                    version,
                    index: index - 1,
                });
            }
            // SAFETY: nodes live as long as the tree.
            node = unsafe { &*child };
            version = node.read_lock()?;
        }
        Ok(Some(LeafVisit {
            leaf: ptr::from_ref(node).cast::<Leaf<K, V>>(),
            version,
            count: node.count(LEAF_CAPACITY),
            high,
            low,
        }))
    }

    /// Finds the entry for `target`, and walks to the next leaf when a leaf has no match.
    fn seek<Q>(&self, target: Target<'_, Q>) -> Option<Position<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + KeyPrefix + ?Sized,
    {
        let prefix = target.prefix();
        let mut fence: Option<(K, Option<u64>)> = None;
        loop {
            let attempt = match (&fence, target.is_forward()) {
                (None, _) => self.try_seek(&target, prefix),
                (Some((f, p)), true) => self.try_seek::<K>(&Target::After(f), *p),
                (Some((f, p)), false) => self.try_seek::<K>(&Target::UpTo(f), *p),
            };
            match attempt {
                Ok(Seek::Found(pos)) => return Some(pos),
                Ok(Seek::End) => return None,
                Ok(Seek::Continue(key)) => {
                    let p = key.prefix();
                    fence = Some((key, p));
                }
                Err(Restart) => continue,
            }
        }
    }

    fn try_seek<Q>(
        &self,
        target: &Target<'_, Q>,
        prefix: Option<u64>,
    ) -> Result<Seek<K, V>, Restart>
    where
        K: Borrow<Q>,
        Q: Ord + KeyPrefix + ?Sized,
    {
        let Some(visit) = self.find_leaf(target, prefix)? else {
            return Ok(Seek::End);
        };
        // SAFETY: nodes live as long as the tree.
        let leaf = unsafe { &*visit.leaf };
        let split = target
            .leaf_split::<K>(&leaf.keys, visit.count, prefix)
            .ok_or(Restart)?;
        let (found, fence) = if target.is_forward() {
            ((split < visit.count).then_some(split), visit.high)
        } else {
            ((split > 0).then(|| split - 1), visit.low)
        };
        if let Some(index) = found {
            leaf.header.check(visit.version)?;
            return Ok(Seek::Found(Position {
                leaf: visit.leaf,
                version: visit.version,
                index,
            }));
        }
        let next = match fence {
            Some(fence) => Some(fence.key()?),
            None => None,
        };
        leaf.header.check(visit.version)?;
        Ok(match next {
            Some(key) => Seek::Continue(key),
            None => Seek::End,
        })
    }

    /// The position after `pos` in the same leaf, when the leaf did not change.
    fn step(&self, pos: Position<K, V>, forward: bool) -> Option<Position<K, V>> {
        // SAFETY: nodes live as long as the tree.
        let leaf = unsafe { &*pos.leaf };
        if leaf.header.version.load(Ordering::Acquire) != pos.version {
            return None;
        }
        let count = leaf.header.count(LEAF_CAPACITY);
        let index = if forward {
            pos.index + 1
        } else {
            pos.index.checked_sub(1)?
        };
        (index < count).then_some(Position {
            leaf: pos.leaf,
            version: pos.version,
            index,
        })
    }

    fn insert_with(
        &self,
        key: K,
        value: impl FnOnce() -> V,
        replace: bool,
    ) -> Result<Insert<'_, K, V, A>, TryReserveError> {
        let guard = epoch::pin();
        let mut value_fn = Some(value);
        let mut value: Option<V> = None;
        loop {
            match self.try_insert_once(&key, &mut value_fn, &mut value, replace, &guard) {
                Ok(Ok(result)) => {
                    if matches!(result, Insert::Inserted(_)) {
                        self.len.fetch_add(1, Ordering::Relaxed);
                    }
                    return Ok(result);
                }
                Ok(Err(err)) => return Err(err),
                Err(Restart) => continue,
            }
        }
    }

    #[allow(clippy::type_complexity)]
    fn try_insert_once<F: FnOnce() -> V>(
        &self,
        key: &K,
        value_fn: &mut Option<F>,
        value: &mut Option<V>,
        replace: bool,
        guard: &epoch::Guard,
    ) -> Result<Result<Insert<'_, K, V, A>, TryReserveError>, Restart> {
        let root = self.root.load(Ordering::Acquire);
        if root.is_null() {
            let leaf = match self.alloc_leaf() {
                Ok(leaf) => leaf,
                Err(err) => return Ok(Err(err)),
            };
            if self
                .root
                .compare_exchange(
                    ptr::null_mut(),
                    leaf.cast(),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_err()
            {
                // SAFETY: the leaf was never published.
                unsafe { self.free_node(leaf.cast()) };
            }
            return Err(Restart);
        }
        // SAFETY: nodes live as long as the tree.
        let mut node = unsafe { &*root };
        let mut version = node.read_lock()?;
        if !ptr::eq(root, self.root.load(Ordering::Acquire)) {
            return Err(Restart);
        }
        let mut parent: Option<(&Header, u64)> = None;
        let mut rightmost = true;
        let target = Target::AtOrAfter(key);
        let prefix = key.prefix();
        while !node.is_leaf {
            // SAFETY: `is_leaf` is false, so the node is an `Inner`.
            let inner = unsafe { &*ptr::from_ref(node).cast::<Inner<K>>() };
            let count = node.count(INNER_CAPACITY);
            if count == INNER_CAPACITY {
                if let Some((p, pv)) = parent {
                    p.upgrade(pv)?;
                }
                if let Err(restart) = node.upgrade(version) {
                    if let Some((p, _)) = parent {
                        p.write_unlock();
                    }
                    return Err(restart);
                }
                if parent.is_none() && !ptr::eq(node, self.root.load(Ordering::Acquire)) {
                    node.write_unlock();
                    return Err(Restart);
                }
                let result = self.split_inner(inner, parent.map(|(p, _)| p));
                node.write_unlock();
                if let Some((p, _)) = parent {
                    p.write_unlock();
                }
                if let Err(err) = result {
                    return Ok(Err(err));
                }
                return Err(Restart);
            }
            if let Some((p, pv)) = parent {
                p.check(pv)?;
            }
            let index = target
                .child_index::<K>(&inner.keys, count, prefix)
                .ok_or(Restart)?;
            let child = inner.children[index].load(Ordering::Acquire);
            node.check(version)?;
            if child.is_null() {
                return Err(Restart);
            }
            parent = Some((node, version));
            rightmost &= index == count;
            // SAFETY: nodes live as long as the tree.
            node = unsafe { &*child };
            version = node.read_lock()?;
        }
        // SAFETY: `is_leaf` is true.
        let leaf = unsafe { &*ptr::from_ref(node).cast::<Leaf<K, V>>() };
        let count = node.count(LEAF_CAPACITY);
        let index = target
            .leaf_split::<K>(&leaf.keys, count, prefix)
            .ok_or(Restart)?;
        let is_match = index < count && K::read(&leaf.keys[index], |k| k == key).ok_or(Restart)?;
        if is_match && !replace {
            let entry = self.entry_at(Position {
                leaf,
                version,
                index,
            })?;
            return Ok(Ok(Insert::Existing(entry)));
        }
        if is_match {
            if value.is_none() {
                node.check(version)?;
                let make_value = value_fn.take().expect("value function is called once");
                *value = Some(make_value());
            }
            node.upgrade(version)?;
            let new_value = value.take().expect("value was made above");
            let entry = Entry {
                key: key.clone(),
                value: new_value.clone(),
                map: self,
            };
            let old_value = V::take(&leaf.values[index]);
            V::write(&leaf.values[index], new_value);
            node.write_unlock();
            // SAFETY: the value is Send and 'static.
            unsafe { guard.defer_unchecked(move || drop(old_value)) };
            return Ok(Ok(Insert::Existing(entry)));
        }
        if count == LEAF_CAPACITY {
            if let Some((p, pv)) = parent {
                p.upgrade(pv)?;
            }
            if let Err(restart) = node.upgrade(version) {
                if let Some((p, _)) = parent {
                    p.write_unlock();
                }
                return Err(restart);
            }
            if parent.is_none() && !ptr::eq(node, self.root.load(Ordering::Acquire)) {
                node.write_unlock();
                return Err(Restart);
            }
            let result = self.split_leaf(leaf, parent.map(|(p, _)| p), rightmost && index == count);
            node.write_unlock();
            if let Some((p, _)) = parent {
                p.write_unlock();
            }
            if let Err(err) = result {
                return Ok(Err(err));
            }
            return Err(Restart);
        }
        if value.is_none() {
            node.check(version)?;
            let make_value = value_fn.take().expect("value function is called once");
            *value = Some(make_value());
        }
        node.upgrade(version)?;
        if let Some((p, pv)) = parent {
            if let Err(restart) = p.check(pv) {
                node.write_unlock();
                return Err(restart);
            }
        }
        for i in (index..count).rev() {
            K::move_from(&leaf.keys[i + 1], &leaf.keys[i]);
            V::move_from(&leaf.values[i + 1], &leaf.values[i]);
        }
        let value = value.take().expect("value was made above");
        let entry = Entry {
            key: key.clone(),
            value: value.clone(),
            map: self,
        };
        K::write(&leaf.keys[index], key.clone());
        V::write(&leaf.values[index], value);
        node.set_count(count + 1);
        node.write_unlock();
        Ok(Ok(Insert::Inserted(entry)))
    }

    /// Splits a full leaf. The caller holds the locks of `leaf` and `parent`.
    /// `append` means that the new key goes after every key in the leaf.
    fn split_leaf(
        &self,
        leaf: &Leaf<K, V>,
        parent: Option<&Header>,
        append: bool,
    ) -> Result<(), TryReserveError> {
        let right = self.alloc_leaf()?;
        let new_root = match parent {
            Some(_) => None,
            None => match self.alloc_inner() {
                Ok(root) => Some(root),
                Err(err) => {
                    // SAFETY: the leaf was never published.
                    unsafe { self.free_node(right.cast()) };
                    return Err(err);
                }
            },
        };
        // SAFETY: the new leaf is not published yet.
        let right_ref = unsafe { &*right };
        let count = LEAF_CAPACITY;
        let mid = if append { count - 1 } else { count / 2 };
        for i in mid..count {
            K::move_from(&right_ref.keys[i - mid], &leaf.keys[i]);
            V::move_from(&right_ref.values[i - mid], &leaf.values[i]);
        }
        right_ref.header.set_count(count - mid);
        let separator = K::read(&leaf.keys[mid - 1], K::clone).expect("locked leaf slot is set");
        leaf.header.set_count(mid);
        for i in mid..count {
            K::clear(&leaf.keys[i]);
            V::clear(&leaf.values[i]);
        }
        self.link_child(
            ptr::from_ref(leaf).cast_mut().cast(),
            parent,
            new_root,
            separator,
            right.cast(),
        );
        Ok(())
    }

    /// Splits a full inner node. The caller holds the locks of `inner` and `parent`.
    fn split_inner(
        &self,
        inner: &Inner<K>,
        parent: Option<&Header>,
    ) -> Result<(), TryReserveError> {
        let right = self.alloc_inner()?;
        let new_root = match parent {
            Some(_) => None,
            None => match self.alloc_inner() {
                Ok(root) => Some(root),
                Err(err) => {
                    // SAFETY: the node was never published.
                    unsafe { self.free_node(right.cast()) };
                    return Err(err);
                }
            },
        };
        // SAFETY: the new node is not published yet.
        let right_ref = unsafe { &*right };
        let count = INNER_CAPACITY;
        let mid = count / 2;
        for i in mid + 1..count {
            K::move_from(&right_ref.keys[i - mid - 1], &inner.keys[i]);
        }
        for i in mid + 1..=count {
            let child = inner.children[i].load(Ordering::Acquire);
            right_ref.children[i - mid - 1].store(child, Ordering::Release);
        }
        right_ref.header.set_count(count - mid - 1);
        let separator = K::take(&inner.keys[mid]);
        inner.header.set_count(mid);
        for i in mid + 1..count {
            K::clear(&inner.keys[i]);
        }
        for i in mid + 1..=count {
            inner.children[i].store(ptr::null_mut(), Ordering::Release);
        }
        self.link_child(
            ptr::from_ref(inner).cast_mut().cast(),
            parent,
            new_root,
            separator,
            right.cast(),
        );
        Ok(())
    }

    /// Puts `right` after `left` in `parent`, or in a new root when `left` is the root.
    fn link_child(
        &self,
        left: *mut Header,
        parent: Option<&Header>,
        new_root: Option<*mut Inner<K>>,
        separator: K,
        right: *mut Header,
    ) {
        match parent {
            Some(parent) => {
                // SAFETY: a parent is an `Inner`, and the caller holds its lock.
                let parent = unsafe { &*ptr::from_ref(parent).cast::<Inner<K>>() };
                let count = parent.header.count(INNER_CAPACITY);
                assert!(count < INNER_CAPACITY, "parent of a split node has space");
                let index = (0..=count)
                    .find(|&i| ptr::eq(parent.children[i].load(Ordering::Acquire), left))
                    .expect("split node is a child of its parent");
                for i in (index..count).rev() {
                    K::move_from(&parent.keys[i + 1], &parent.keys[i]);
                }
                for i in (index + 1..=count).rev() {
                    let child = parent.children[i].load(Ordering::Acquire);
                    parent.children[i + 1].store(child, Ordering::Release);
                }
                K::write(&parent.keys[index], separator);
                parent.children[index + 1].store(right, Ordering::Release);
                parent.header.set_count(count + 1);
            }
            None => {
                let root = new_root.expect("a split of the root allocates a new root");
                // SAFETY: the new root is not published yet.
                let root_ref = unsafe { &*root };
                K::write(&root_ref.keys[0], separator);
                root_ref.children[0].store(left, Ordering::Release);
                root_ref.children[1].store(right, Ordering::Release);
                root_ref.header.set_count(1);
                self.root.store(root.cast(), Ordering::Release);
            }
        }
    }

    fn remove_if<Q>(
        &self,
        key: &Q,
        matches: impl Fn(&V::Slot) -> bool,
    ) -> Option<Entry<'_, K, V, A>>
    where
        K: Borrow<Q>,
        Q: Ord + KeyPrefix + ?Sized,
    {
        let guard = epoch::pin();
        let prefix = key.prefix();
        loop {
            match self.try_remove_once(key, prefix, &matches, &guard) {
                Ok(removed) => {
                    if removed.is_some() {
                        self.len.fetch_sub(1, Ordering::Relaxed);
                    }
                    return removed;
                }
                Err(Restart) => continue,
            }
        }
    }

    fn try_remove_once<Q>(
        &self,
        key: &Q,
        prefix: Option<u64>,
        matches: &impl Fn(&V::Slot) -> bool,
        guard: &epoch::Guard,
    ) -> Result<Option<Entry<'_, K, V, A>>, Restart>
    where
        K: Borrow<Q>,
        Q: Ord + KeyPrefix + ?Sized,
    {
        let target = Target::AtOrAfter(key);
        let Some(visit) = self.find_leaf(&target, prefix)? else {
            return Ok(None);
        };
        // SAFETY: nodes live as long as the tree.
        let leaf = unsafe { &*visit.leaf };
        let count = visit.count;
        let index = target
            .leaf_split::<K>(&leaf.keys, count, prefix)
            .ok_or(Restart)?;
        let is_match = index < count
            && K::read(&leaf.keys[index], |k| k.borrow() == key).ok_or(Restart)?
            && matches(&leaf.values[index]);
        if !is_match {
            leaf.header.check(visit.version)?;
            return Ok(None);
        }
        leaf.header.upgrade(visit.version)?;
        let removed_key = K::take(&leaf.keys[index]);
        let removed_value = V::take(&leaf.values[index]);
        for i in index..count - 1 {
            K::move_from(&leaf.keys[i], &leaf.keys[i + 1]);
            V::move_from(&leaf.values[i], &leaf.values[i + 1]);
        }
        K::clear(&leaf.keys[count - 1]);
        V::clear(&leaf.values[count - 1]);
        leaf.header.set_count(count - 1);
        leaf.header.write_unlock();
        let entry = Entry {
            key: removed_key.clone(),
            value: removed_value.clone(),
            map: self,
        };
        drop_after_readers(guard, removed_key, removed_value);
        Ok(Some(entry))
    }

    fn alloc_leaf(&self) -> Result<*mut Leaf<K, V>, TryReserveError> {
        let node = self
            .alloc_zeroed(Layout::new::<Leaf<K, V>>())?
            .cast::<Leaf<K, V>>();
        // SAFETY: the memory is zeroed, which is a valid empty node, and nobody else sees it.
        unsafe { (*node).header.is_leaf = true };
        Ok(node)
    }

    fn alloc_inner(&self) -> Result<*mut Inner<K>, TryReserveError> {
        Ok(self
            .alloc_zeroed(Layout::new::<Inner<K>>())?
            .cast::<Inner<K>>())
    }

    fn alloc_zeroed(&self, layout: Layout) -> Result<*mut u8, TryReserveError> {
        let ptr = self
            .alloc
            .allocate(layout)
            .map_err(|_| TryReserveError)?
            .cast::<u8>()
            .as_ptr();
        // SAFETY: the allocation has `layout.size()` bytes.
        unsafe { ptr::write_bytes(ptr, 0, layout.size()) };
        Ok(ptr)
    }

    /// # Safety
    ///
    /// No other thread can reach the node, and its slots are empty or already dropped.
    unsafe fn free_node(&self, node: *mut Header) {
        let layout = if unsafe { (*node).is_leaf } {
            Layout::new::<Leaf<K, V>>()
        } else {
            Layout::new::<Inner<K>>()
        };
        unsafe {
            self.alloc
                .deallocate(NonNull::new_unchecked(node.cast::<u8>()), layout)
        };
    }

    /// # Safety
    ///
    /// No other thread can reach the tree.
    unsafe fn drop_subtree(&self, node: *mut Header) {
        // SAFETY: the caller has exclusive access.
        let header = unsafe { &*node };
        if header.is_leaf {
            let leaf = unsafe { &*node.cast::<Leaf<K, V>>() };
            for i in 0..header.count(LEAF_CAPACITY) {
                drop(K::take(&leaf.keys[i]));
                drop(V::take(&leaf.values[i]));
            }
        } else {
            let inner = unsafe { &*node.cast::<Inner<K>>() };
            let count = header.count(INNER_CAPACITY);
            for i in 0..count {
                drop(K::take(&inner.keys[i]));
            }
            for i in 0..=count {
                let child = inner.children[i].load(Ordering::Acquire);
                unsafe { self.drop_subtree(child) };
            }
        }
        unsafe { self.free_node(node) };
    }
}

impl<K: TreeKey, V: TreeValue, A: ConcurrentAllocator> Drop for BPlusTreeMap<K, V, A> {
    fn drop(&mut self) {
        let root = *self.root.get_mut();
        if !root.is_null() {
            // SAFETY: `&mut self` means no other thread can reach the tree.
            unsafe { self.drop_subtree(root) };
        }
    }
}

fn drop_after_readers<K: TreeKey, V: TreeValue>(guard: &epoch::Guard, key: K, value: V) {
    if K::NEEDS_DEFERRED_DROP {
        // SAFETY: the key and the value are Send and 'static.
        unsafe { guard.defer_unchecked(move || drop((key, value))) };
    } else {
        drop(key);
        // SAFETY: the value is Send and 'static.
        unsafe { guard.defer_unchecked(move || drop(value)) };
    }
}

/// A key and value that were in the map when the entry was made. The entry owns a
/// clone of both, so the map can change while the entry is alive.
pub struct Entry<'a, K: TreeKey, V: TreeValue, A: ConcurrentAllocator = TursoAllocator> {
    key: K,
    value: V,
    map: &'a BPlusTreeMap<K, V, A>,
}

impl<K: TreeKey, V: TreeValue, A: ConcurrentAllocator> Entry<'_, K, V, A> {
    pub fn key(&self) -> &K {
        &self.key
    }

    pub fn value(&self) -> &V {
        &self.value
    }

    pub fn into_value(self) -> V {
        self.value
    }

    pub fn into_key_value(self) -> (K, V) {
        (self.key, self.value)
    }

    /// Removes this entry from the map, if the map still maps its key to its value.
    pub fn remove(&self) -> bool {
        self.map
            .remove_if(&self.key, |slot| V::is_same(slot, &self.value))
            .is_some()
    }

    /// True if the map no longer maps this key to this value.
    pub fn is_removed(&self) -> bool {
        !self
            .map
            .get(&self.key)
            .is_some_and(|entry| V::same_value(&entry.value, &self.value))
    }
}

impl<K: TreeKey + std::fmt::Debug, V: TreeValue + std::fmt::Debug, A: ConcurrentAllocator>
    std::fmt::Debug for Entry<'_, K, V, A>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Entry")
            .field("key", &self.key)
            .field("value", &self.value)
            .finish()
    }
}

const MAX_BATCH: usize = 16;

/// Entries that a cursor copied from one leaf with one epoch `Guard`. They stay valid
/// while the leaf version is the same, so the next steps need no `Guard` and no atomic
/// read-modify-write.
struct Batch<K: TreeKey, V: TreeValue> {
    entries: [std::mem::MaybeUninit<(K, V)>; MAX_BATCH],
    start: usize,
    end: usize,
}

impl<K: TreeKey, V: TreeValue> Batch<K, V> {
    fn new() -> Self {
        Self {
            entries: [const { std::mem::MaybeUninit::uninit() }; MAX_BATCH],
            start: 0,
            end: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.start == self.end
    }

    fn push(&mut self, entry: (K, V)) {
        assert!(self.end < MAX_BATCH, "batch is full");
        self.entries[self.end].write(entry);
        self.end += 1;
    }

    fn pop(&mut self) -> Option<(K, V)> {
        if self.is_empty() {
            return None;
        }
        // SAFETY: entries in `start..end` are initialized, and `start` moves past this one.
        let entry = unsafe { self.entries[self.start].assume_init_read() };
        self.start += 1;
        Some(entry)
    }

    fn clear(&mut self) {
        while self.pop().is_some() {}
        self.start = 0;
        self.end = 0;
    }
}

impl<K: TreeKey, V: TreeValue> Drop for Batch<K, V> {
    fn drop(&mut self) {
        self.clear();
    }
}

enum Cursor<K: TreeKey, V: TreeValue> {
    Unstarted,
    /// `key` is the last returned key and `pos` is its place. The batch holds the
    /// entries after `pos` in the same leaf, copied at `pos.version`.
    At {
        key: K,
        pos: Position<K, V>,
    },
    Done,
}

struct Side<K: TreeKey, V: TreeValue> {
    cursor: Cursor<K, V>,
    batch: Batch<K, V>,
    batch_size: usize,
}

impl<K: TreeKey, V: TreeValue> Side<K, V> {
    fn new() -> Self {
        Self {
            cursor: Cursor::Unstarted,
            batch: Batch::new(),
            batch_size: 1,
        }
    }

    fn last_key(&self) -> Option<&K> {
        match &self.cursor {
            Cursor::At { key, .. } => Some(key),
            Cursor::Unstarted | Cursor::Done => None,
        }
    }
}

/// Iterator over a range of the map. It sees entries that are inserted ahead of it
/// while it runs.
pub struct Range<
    'a,
    Q: ?Sized,
    R,
    K: TreeKey,
    V: TreeValue,
    A: ConcurrentAllocator = TursoAllocator,
> {
    map: &'a BPlusTreeMap<K, V, A>,
    range: R,
    front: Side<K, V>,
    back: Side<K, V>,
    _marker: PhantomData<fn(&Q)>,
}

// SAFETY: the cursor positions only point into nodes of the map, which is Sync, and
// the batches own their keys and values, which are Send + Sync.
unsafe impl<Q: ?Sized, R: Send, K: TreeKey, V: TreeValue, A: ConcurrentAllocator> Send
    for Range<'_, Q, R, K, V, A>
{
}
unsafe impl<Q: ?Sized, R: Sync, K: TreeKey, V: TreeValue, A: ConcurrentAllocator> Sync
    for Range<'_, Q, R, K, V, A>
{
}

impl<'a, Q, R, K, V, A> Range<'a, Q, R, K, V, A>
where
    K: TreeKey + Borrow<Q>,
    V: TreeValue,
    A: ConcurrentAllocator,
    R: RangeBounds<Q>,
    Q: Ord + KeyPrefix + ?Sized,
{
    fn advance(&mut self, forward: bool) -> Option<Entry<'a, K, V, A>> {
        if let Some(entry) = self.next_from_batch(forward) {
            return self.accept(entry, forward);
        }
        let _guard = epoch::pin();
        let map = self.map;
        loop {
            let side = if forward { &self.front } else { &self.back };
            let start = match &side.cursor {
                Cursor::Done => return None,
                Cursor::Unstarted => {
                    if forward {
                        match self.range.start_bound() {
                            Bound::Unbounded => map.seek::<Q>(Target::First),
                            Bound::Included(q) => map.seek(Target::AtOrAfter(q)),
                            Bound::Excluded(q) => map.seek(Target::After(q)),
                        }
                    } else {
                        match self.range.end_bound() {
                            Bound::Unbounded => map.seek::<Q>(Target::Last),
                            Bound::Included(q) => map.seek(Target::UpTo(q)),
                            Bound::Excluded(q) => map.seek(Target::Before(q)),
                        }
                    }
                }
                Cursor::At { key, pos } => match map.step(*pos, forward) {
                    Some(next) => Some(next),
                    None if forward => map.seek::<K>(Target::After(key)),
                    None => map.seek::<K>(Target::Before(key)),
                },
            };
            let Some(start) = start else {
                self.finish(forward);
                return None;
            };
            let Ok(first) = self.fill_batch(start, forward) else {
                continue;
            };
            let side = if forward {
                &mut self.front
            } else {
                &mut self.back
            };
            side.cursor = Cursor::At {
                key: first.0.clone(),
                pos: start,
            };
            side.batch_size = (side.batch_size * 4).min(MAX_BATCH);
            return self.accept(first, forward);
        }
    }

    /// Takes the next entry of the batch when the leaf did not change since the batch
    /// was copied.
    fn next_from_batch(&mut self, forward: bool) -> Option<(K, V)> {
        let side = if forward {
            &mut self.front
        } else {
            &mut self.back
        };
        if side.batch.is_empty() {
            return None;
        }
        let Cursor::At { key, pos } = &mut side.cursor else {
            unreachable!("a cursor with a batch has a position");
        };
        // SAFETY: nodes live as long as the tree.
        let leaf = unsafe { &*pos.leaf };
        if leaf.header.version.load(Ordering::Acquire) != pos.version {
            side.batch.clear();
            return None;
        }
        let entry = side.batch.pop().expect("batch is not empty");
        *key = entry.0.clone();
        pos.index = if forward {
            pos.index + 1
        } else {
            pos.index - 1
        };
        Some(entry)
    }

    /// Copies the entry at `start` and up to `batch_size - 1` entries after it in the
    /// same leaf. Returns the entry at `start`. The other copied entries go to the batch.
    fn fill_batch(&mut self, start: Position<K, V>, forward: bool) -> Result<(K, V), Restart> {
        // SAFETY: nodes live as long as the tree.
        let leaf = unsafe { &*start.leaf };
        let count = leaf.header.count(LEAF_CAPACITY);
        let side = if forward { &self.front } else { &self.back };
        let want = side.batch_size;
        let mut copied: Batch<K, V> = Batch::new();
        let last = if forward {
            count.min(start.index + want)
        } else {
            start.index.saturating_sub(want - 1)
        };
        if forward {
            (start.index..last).for_each(|i| V::prefetch(&leaf.values[i]));
        } else {
            (last..=start.index.min(count.saturating_sub(1)))
                .for_each(|i| V::prefetch(&leaf.values[i]));
        }
        let mut index = Some(start.index);
        while let Some(i) = index.filter(|&i| i < count) {
            let key = K::read(&leaf.keys[i], K::clone).ok_or(Restart)?;
            let value = V::clone_from_slot(&leaf.values[i]).ok_or(Restart)?;
            copied.push((key, value));
            if copied.end == want {
                break;
            }
            index = if forward {
                Some(i + 1)
            } else {
                i.checked_sub(1)
            };
        }
        leaf.header.check(start.version)?;
        let first = copied.pop().ok_or(Restart)?;
        let side = if forward {
            &mut self.front
        } else {
            &mut self.back
        };
        side.batch.clear();
        while let Some(entry) = copied.pop() {
            side.batch.push(entry);
        }
        Ok(first)
    }

    /// Checks the range bounds and the other end of the iterator, and returns the entry.
    fn accept(&mut self, entry: (K, V), forward: bool) -> Option<Entry<'a, K, V, A>> {
        if !self.in_range(&entry.0, forward) {
            self.finish(forward);
            return None;
        }
        Some(Entry {
            key: entry.0,
            value: entry.1,
            map: self.map,
        })
    }

    fn in_range(&self, key: &K, forward: bool) -> bool {
        let q = key.borrow();
        let other = if forward { &self.back } else { &self.front };
        if let Some(other) = other.last_key() {
            let other = other.borrow();
            if (forward && q >= other) || (!forward && q <= other) {
                return false;
            }
        }
        if forward {
            match self.range.end_bound() {
                Bound::Unbounded => true,
                Bound::Included(end) => q <= end,
                Bound::Excluded(end) => q < end,
            }
        } else {
            match self.range.start_bound() {
                Bound::Unbounded => true,
                Bound::Included(start) => q >= start,
                Bound::Excluded(start) => q > start,
            }
        }
    }

    fn finish(&mut self, forward: bool) {
        let side = if forward {
            &mut self.front
        } else {
            &mut self.back
        };
        side.cursor = Cursor::Done;
        side.batch.clear();
    }
}

impl<'a, Q, R, K, V, A> Iterator for Range<'a, Q, R, K, V, A>
where
    K: TreeKey + Borrow<Q>,
    V: TreeValue,
    A: ConcurrentAllocator,
    R: RangeBounds<Q>,
    Q: Ord + KeyPrefix + ?Sized,
{
    type Item = Entry<'a, K, V, A>;

    fn next(&mut self) -> Option<Self::Item> {
        self.advance(true)
    }
}

impl<'a, Q, R, K, V, A> DoubleEndedIterator for Range<'a, Q, R, K, V, A>
where
    K: TreeKey + Borrow<Q>,
    V: TreeValue,
    A: ConcurrentAllocator,
    R: RangeBounds<Q>,
    Q: Ord + KeyPrefix + ?Sized,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.advance(false)
    }
}

#[cfg(test)]
mod tests;
