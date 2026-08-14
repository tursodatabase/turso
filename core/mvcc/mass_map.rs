//! Concurrent ordered maps for the MVCC store, backed by a Masstree
//! (a trie of B+trees; Mao, Kohler, Morris — EuroSys 2012) via the
//! [`masstree`] crate.
//!
//! # Why an adapter?
//!
//! Masstree keys are plain byte strings compared lexicographically, and the
//! crate's `insert` is an upsert with no atomic insert-if-absent. The MVCC
//! store needs typed keys, ordered iteration in both directions, and a
//! get-or-insert that never drops a concurrently inserted version chain.
//! [`MassMap`] closes those gaps:
//!
//! * [`MassKey`] encodes typed keys into fixed-width byte strings whose
//!   lexicographic order equals the typed order (big-endian, sign bit
//!   flipped for signed integers).
//! * Structural mutations (insert-if-absent, remove, clear) serialize behind
//!   a small [`Mutex`]. Two racing "insert if missing" upserts would
//!   otherwise silently drop one side's value. Reads and scans never take
//!   the lock.
//! * [`MassMap::range`]/[`MassMap::iter`] return owned, re-seeking
//!   iterators: every step runs a bounded Masstree scan for one entry and
//!   clones the key and value out. Like the skip-list iterators these
//!   replace, they are weakly consistent: an entry inserted ahead of the
//!   iterator's position is seen, an entry inserted at or behind it is not.
//!
//! Values are cloned out of the tree on every read, so value types must be
//! cheap to clone (they are `Arc`s or small `Copy` structs in the MVCC
//! store).
//!
//! Index version chains (`SortableIndexKey`) stay on the skip list: their
//! ordering is collation- and ASC/DESC-aware, which cannot be expressed as a
//! byte encoding, and index keys can exceed Masstree's 256-byte key limit.

use std::marker::PhantomData;
use std::ops::{Bound, RangeBounds};

use turso_masstree::{MassTree15, RangeBound};

use crate::sync::Mutex;

/// Fixed-width, order-preserving byte encoding for [`MassMap`] keys.
///
/// `encode` must be monotonic: `a < b` iff `encode(a) < encode(b)` in
/// lexicographic byte order. `decode` must invert `encode` for every value
/// that is actually stored in a map.
pub trait MassKey: Clone + Send + Sync + 'static {
    /// The encoded representation, e.g. `[u8; 8]`.
    type Bytes: AsRef<[u8]> + Copy + Send + Sync;

    fn encode(&self) -> Self::Bytes;

    fn decode(bytes: &[u8]) -> Self;
}

/// `u64` keys (transaction ids): big-endian preserves unsigned order.
impl MassKey for u64 {
    type Bytes = [u8; 8];

    fn encode(&self) -> [u8; 8] {
        self.to_be_bytes()
    }

    fn decode(bytes: &[u8]) -> u64 {
        u64::from_be_bytes(bytes.try_into().expect("u64 key is 8 encoded bytes"))
    }
}

/// Encode an `i64` so lexicographic byte order equals signed order:
/// big-endian with the sign bit flipped.
pub(crate) fn encode_i64(value: i64) -> [u8; 8] {
    ((value as u64) ^ (1u64 << 63)).to_be_bytes()
}

pub(crate) fn decode_i64(bytes: &[u8]) -> i64 {
    (u64::from_be_bytes(bytes.try_into().expect("i64 key is 8 encoded bytes")) ^ (1u64 << 63))
        as i64
}

/// An owned key/value snapshot of one map entry.
///
/// Unlike a skip-list entry this holds no reference into the map: the value
/// is cloned out under the tree's epoch guard at lookup time. Values are
/// `Arc`s or small `Copy` structs, so the clone is cheap, and `Arc`
/// identity still allows "is this chain still the mapped one" checks.
#[derive(Debug, Clone)]
pub struct MassEntry<K, V> {
    key: K,
    value: V,
}

impl<K, V> MassEntry<K, V> {
    /// Build an entry from an owned key/value pair, e.g. when adapting a
    /// skip-list entry into the owned representation.
    pub fn new(key: K, value: V) -> Self {
        Self { key, value }
    }

    pub fn key(&self) -> &K {
        &self.key
    }

    pub fn value(&self) -> &V {
        &self.value
    }

    pub fn into_value(self) -> V {
        self.value
    }
}

/// A concurrent ordered map from `K` to `V` backed by a Masstree.
pub struct MassMap<K: MassKey, V: Clone + Send + Sync + 'static> {
    tree: MassTree15<V>,
    /// Serializes structural mutations (insert-if-absent, remove, clear).
    /// Masstree's insert is an upsert, so without this two racing
    /// get-or-inserts of the same key would each insert, and one side's
    /// value — possibly already holding row versions — would be dropped.
    /// Reads and scans never take this lock.
    mutation: Mutex<()>,
    _key: PhantomData<K>,
}

impl<K: MassKey, V: Clone + Send + Sync + 'static> Default for MassMap<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: MassKey, V: Clone + Send + Sync + 'static> std::fmt::Debug for MassMap<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MassMap").field("len", &self.len()).finish()
    }
}

impl<K: MassKey, V: Clone + Send + Sync + 'static> MassMap<K, V> {
    pub fn new() -> Self {
        Self {
            tree: MassTree15::new(),
            mutation: Mutex::new(()),
            _key: PhantomData,
        }
    }

    pub fn len(&self) -> usize {
        self.tree.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tree.is_empty()
    }

    /// Whether `key` has a mapping. Lock-free.
    pub fn contains_key(&self, key: &K) -> bool {
        let bytes = key.encode();
        let guard = self.tree.guard();
        self.tree.get_with_guard(bytes.as_ref(), &guard).is_some()
    }

    /// Look up `key`, cloning the value out. Lock-free.
    pub fn get(&self, key: &K) -> Option<MassEntry<K, V>> {
        let bytes = key.encode();
        let guard = self.tree.guard();
        self.tree
            .get_with_guard(bytes.as_ref(), &guard)
            .map(|value| MassEntry {
                key: key.clone(),
                value: V::clone(&value),
            })
    }

    /// Insert `value` under `key`, replacing any existing value (upsert),
    /// like the skip list's `insert`.
    pub fn insert(&self, key: K, value: V) -> MassEntry<K, V> {
        let _structural = self.mutation.lock();
        let bytes = key.encode();
        let guard = self.tree.guard();
        self.tree
            .insert_with_guard(bytes.as_ref(), value.clone(), &guard);
        MassEntry { key, value }
    }

    /// Fallible-allocation variant of [`Self::insert`], mirroring the skip
    /// list API. Masstree allocates through the global allocator and does
    /// not surface allocation failure, so this never returns `Err`.
    pub fn try_insert(
        &self,
        key: K,
        value: V,
    ) -> Result<MassEntry<K, V>, crate::alloc::TryReserveError> {
        Ok(self.insert(key, value))
    }

    /// Return the existing entry for `key`, or insert `value_fn()`.
    /// Never replaces a concurrently inserted value.
    pub fn try_get_or_insert_with<F: FnOnce() -> V>(
        &self,
        key: K,
        value_fn: F,
    ) -> Result<MassEntry<K, V>, crate::alloc::TryReserveError> {
        if let Some(entry) = self.get(&key) {
            return Ok(entry);
        }
        let _structural = self.mutation.lock();
        // Re-check under the lock: another thread may have inserted between
        // the lock-free probe above and the lock acquisition.
        if let Some(entry) = self.get(&key) {
            return Ok(entry);
        }
        let value = value_fn();
        let bytes = key.encode();
        let guard = self.tree.guard();
        let previous = self
            .tree
            .insert_with_guard(bytes.as_ref(), value.clone(), &guard);
        crate::turso_assert!(
            previous.is_none(),
            "insert-if-absent raced despite the structural lock"
        );
        Ok(MassEntry { key, value })
    }

    /// Remove `key`, returning the removed entry.
    pub fn remove(&self, key: &K) -> Option<MassEntry<K, V>> {
        let _structural = self.mutation.lock();
        let bytes = key.encode();
        let guard = self.tree.guard();
        let removed = self
            .tree
            .remove_with_guard(bytes.as_ref(), &guard)
            .expect("masstree remove hit its internal retry limit");
        removed.map(|value| MassEntry {
            key: key.clone(),
            value: V::clone(&value),
        })
    }

    /// Remove every entry.
    pub fn clear(&self) {
        let _structural = self.mutation.lock();
        let guard = self.tree.guard();
        loop {
            let Some(first) = self.tree.first_with_guard(&guard) else {
                return;
            };
            self.tree
                .remove_with_guard(first.key(), &guard)
                .expect("masstree remove hit its internal retry limit");
        }
    }

    /// Iterate over all entries in key order.
    pub fn iter(&self) -> MassRange<'_, K, V> {
        MassRange {
            map: self,
            front: Bound::Unbounded,
            back: Bound::Unbounded,
            exhausted: false,
        }
    }

    /// Iterate over the entries within `range` in key order. The returned
    /// iterator is double-ended; `.rev()` yields descending order.
    pub fn range<R: RangeBounds<K>>(&self, range: R) -> MassRange<'_, K, V> {
        let encode_bound = |bound: Bound<&K>| match bound {
            Bound::Included(key) => Bound::Included(key.encode()),
            Bound::Excluded(key) => Bound::Excluded(key.encode()),
            Bound::Unbounded => Bound::Unbounded,
        };
        MassRange {
            map: self,
            front: encode_bound(range.start_bound()),
            back: encode_bound(range.end_bound()),
            exhausted: false,
        }
    }
}

impl<K: MassKey, V: Clone + Send + Sync + 'static> FromIterator<(K, V)> for MassMap<K, V> {
    fn from_iter<I: IntoIterator<Item = (K, V)>>(iter: I) -> Self {
        let map = Self::new();
        for (key, value) in iter {
            map.insert(key, value);
        }
        map
    }
}

fn as_range_bound<B: AsRef<[u8]>>(bound: &Bound<B>) -> RangeBound<'_> {
    match bound {
        Bound::Included(bytes) => RangeBound::Included(bytes.as_ref()),
        Bound::Excluded(bytes) => RangeBound::Excluded(bytes.as_ref()),
        Bound::Unbounded => RangeBound::Unbounded,
    }
}

/// A double-ended, weakly consistent range iterator over a [`MassMap`].
///
/// Each step re-seeks the tree: one bounded scan finds the next entry past
/// the last one returned, clones it out, and tightens the bound. Nothing is
/// buffered, so like the skip-list iterators this replaces, every step
/// observes the live map beyond the current position.
pub struct MassRange<'a, K: MassKey, V: Clone + Send + Sync + 'static> {
    map: &'a MassMap<K, V>,
    /// Lower edge of the remaining window; tightened by `next`.
    front: Bound<K::Bytes>,
    /// Upper edge of the remaining window; tightened by `next_back`.
    back: Bound<K::Bytes>,
    /// Set once either end runs out of entries; the window edges only track
    /// consumed entries, so crossed bounds cannot be detected from them
    /// alone.
    exhausted: bool,
}

impl<K: MassKey, V: Clone + Send + Sync + 'static> Iterator for MassRange<'_, K, V> {
    type Item = MassEntry<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.exhausted {
            return None;
        }
        let guard = self.map.tree.guard();
        let mut found: Option<(K, V)> = None;
        self.map.tree.scan(
            as_range_bound(&self.front),
            as_range_bound(&self.back),
            |key, value| {
                found = Some((K::decode(key), V::clone(&value)));
                false
            },
            &guard,
        );
        match found {
            Some((key, value)) => {
                self.front = Bound::Excluded(key.encode());
                Some(MassEntry { key, value })
            }
            None => {
                self.exhausted = true;
                None
            }
        }
    }
}

impl<K: MassKey, V: Clone + Send + Sync + 'static> DoubleEndedIterator for MassRange<'_, K, V> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.exhausted {
            return None;
        }
        let guard = self.map.tree.guard();
        let mut found: Option<(K, V)> = None;
        self.map.tree.scan_rev_batch(
            as_range_bound(&self.front),
            as_range_bound(&self.back),
            |key, value| {
                found = Some((K::decode(key), V::clone(&value)));
                false
            },
            &guard,
        );
        match found {
            Some((key, value)) => {
                self.back = Bound::Excluded(key.encode());
                Some(MassEntry { key, value })
            }
            None => {
                self.exhausted = true;
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_insert_remove_roundtrip() {
        let map: MassMap<u64, u64> = MassMap::new();
        assert!(map.is_empty());
        assert!(map.get(&7).is_none());

        map.insert(7, 70);
        map.insert(3, 30);
        map.insert(11, 110);
        assert_eq!(map.len(), 3);
        assert_eq!(*map.get(&7).unwrap().value(), 70);

        // insert is an upsert
        map.insert(7, 71);
        assert_eq!(*map.get(&7).unwrap().value(), 71);
        assert_eq!(map.len(), 3);

        let removed = map.remove(&3).unwrap();
        assert_eq!(*removed.value(), 30);
        assert!(map.get(&3).is_none());
        assert_eq!(map.len(), 2);
        assert!(map.remove(&3).is_none());

        map.clear();
        assert!(map.is_empty());
    }

    #[test]
    fn get_or_insert_keeps_first_value() {
        let map: MassMap<u64, u64> = MassMap::new();
        let first = map.try_get_or_insert_with(5, || 50).unwrap();
        assert_eq!(*first.value(), 50);
        let second = map.try_get_or_insert_with(5, || 51).unwrap();
        assert_eq!(*second.value(), 50, "existing value must win");
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn iteration_is_ordered_and_double_ended() {
        let map: MassMap<u64, u64> = MassMap::new();
        for key in [5u64, 1, 9, 3, 7] {
            map.insert(key, key * 10);
        }
        let forward: Vec<u64> = map.iter().map(|entry| *entry.key()).collect();
        assert_eq!(forward, vec![1, 3, 5, 7, 9]);
        let backward: Vec<u64> = map.iter().rev().map(|entry| *entry.key()).collect();
        assert_eq!(backward, vec![9, 7, 5, 3, 1]);
    }

    #[test]
    fn range_bounds_are_respected() {
        let map: MassMap<u64, u64> = MassMap::new();
        for key in 0u64..10 {
            map.insert(key, key);
        }
        let mid: Vec<u64> = map.range(3..7).map(|entry| *entry.key()).collect();
        assert_eq!(mid, vec![3, 4, 5, 6]);
        let mid_rev: Vec<u64> = map
            .range((Bound::Excluded(3), Bound::Included(7)))
            .rev()
            .map(|entry| *entry.key())
            .collect();
        assert_eq!(mid_rev, vec![7, 6, 5, 4]);
        let both: Vec<u64> = map
            .range((Bound::Included(2), Bound::Included(2)))
            .map(|entry| *entry.key())
            .collect();
        assert_eq!(both, vec![2]);
    }

    #[test]
    fn iterator_sees_entries_inserted_ahead_of_position() {
        let map: MassMap<u64, u64> = MassMap::new();
        map.insert(1, 1);
        map.insert(5, 5);
        let mut iter = map.iter();
        assert_eq!(*iter.next().unwrap().key(), 1);
        // Inserted ahead of the iterator position: must be seen, exactly
        // like a skip-list iterator.
        map.insert(3, 3);
        assert_eq!(*iter.next().unwrap().key(), 3);
        assert_eq!(*iter.next().unwrap().key(), 5);
        assert!(iter.next().is_none());
    }

    /// Two-u64 key: encodes to 16 bytes, so it exercises the trie's sublayer
    /// path the way `RowID` does.
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    struct WideKey(u64, u64);

    impl MassKey for WideKey {
        type Bytes = [u8; 16];

        fn encode(&self) -> [u8; 16] {
            let mut bytes = [0u8; 16];
            bytes[..8].copy_from_slice(&self.0.to_be_bytes());
            bytes[8..].copy_from_slice(&self.1.to_be_bytes());
            bytes
        }

        fn decode(bytes: &[u8]) -> Self {
            Self(
                u64::from_be_bytes(bytes[..8].try_into().unwrap()),
                u64::from_be_bytes(bytes[8..].try_into().unwrap()),
            )
        }
    }

    /// Regression test: masstree 0.9.5's forward scan drops an `Included`
    /// start key that lives past the first leaf of a sublayer, which broke
    /// eq-only point seeks on 16-byte row keys. `MassRange` must stay exact
    /// at every position, on both range edges, in both directions.
    #[test]
    fn included_start_is_exact_past_leaf_boundaries() {
        let map: MassMap<WideKey, u64> = MassMap::new();
        for i in 0..60 {
            map.insert(WideKey(7, i), i);
        }
        for i in 0..60 {
            let key = WideKey(7, i);
            let singleton: Vec<u64> = map
                .range((Bound::Included(key), Bound::Included(key)))
                .map(|entry| *entry.value())
                .collect();
            assert_eq!(singleton, vec![i], "singleton range at suffix {i}");

            let first = map
                .range((Bound::Included(key), Bound::Unbounded))
                .next()
                .unwrap();
            assert_eq!(*first.key(), key, "forward start at suffix {i}");

            let last = map
                .range((Bound::Unbounded, Bound::Included(key)))
                .next_back()
                .unwrap();
            assert_eq!(*last.key(), key, "reverse end at suffix {i}");
        }
        let absent_start: Vec<WideKey> = map
            .range((Bound::Included(WideKey(7, 100)), Bound::Unbounded))
            .map(|entry| *entry.key())
            .collect();
        assert_eq!(absent_start, vec![], "absent Included start past the end");
        let empty: Vec<WideKey> = map
            .range((Bound::Included(WideKey(7, 20)), Bound::Excluded(WideKey(7, 20))))
            .map(|entry| *entry.key())
            .collect();
        assert_eq!(empty, vec![], "inverted-edge range is empty");
    }

    #[test]
    fn signed_encoding_orders_negative_before_positive() {
        assert!(encode_i64(i64::MIN) < encode_i64(-1));
        assert!(encode_i64(-1) < encode_i64(0));
        assert!(encode_i64(0) < encode_i64(1));
        assert!(encode_i64(1) < encode_i64(i64::MAX));
        assert_eq!(decode_i64(&encode_i64(-42)), -42);
        assert_eq!(decode_i64(&encode_i64(i64::MIN)), i64::MIN);
        assert_eq!(decode_i64(&encode_i64(i64::MAX)), i64::MAX);
    }
}
