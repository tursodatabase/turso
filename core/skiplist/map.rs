//! An ordered map based on a lock-free skip list. See [`SkipMap`].

use std::borrow::Borrow;
use std::fmt;
use std::mem::ManuallyDrop;
use std::ops::{Bound, RangeBounds};
use std::ptr;

use super::base::{self, try_pin_loop};
use crossbeam_epoch as epoch;

/// An ordered map based on a lock-free skip list.
///
/// This is an alternative to [`BTreeMap`] which supports
/// concurrent access across multiple threads.
///
/// [`BTreeMap`]: std::collections::BTreeMap
pub struct SkipMap<K, V> {
    inner: base::SkipList<K, V>,
}

impl<K, V> SkipMap<K, V> {
    /// Returns a new, empty map.
    ///
    /// # Example
    ///
    /// ```
    /// use turso_core::skiplist::SkipMap;
    ///
    /// let map: SkipMap<i32, &str> = SkipMap::new();
    /// ```
    pub fn new() -> Self {
        Self {
            inner: base::SkipList::new(epoch::default_collector().clone()),
        }
    }

    /// Returns `true` if the map is empty.
    ///
    /// # Example
    /// ```
    /// use turso_core::skiplist::SkipMap;
    ///
    /// let map: SkipMap<&str, &str> = SkipMap::new();
    /// assert!(map.is_empty());
    ///
    /// map.insert("key", "value");
    /// assert!(!map.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Returns the number of entries in the map.
    ///
    /// If the map is being concurrently modified, consider the returned number just an
    /// approximation without any guarantees.
    ///
    /// # Example
    /// ```
    /// use turso_core::skiplist::SkipMap;
    ///
    /// let map = SkipMap::new();
    /// map.insert(0, 1);
    /// assert_eq!(map.len(), 1);
    ///
    /// for x in 1..=5 {
    ///     map.insert(x, x + 1);
    /// }
    ///
    /// assert_eq!(map.len(), 6);
    /// ```
    pub fn len(&self) -> usize {
        self.inner.len()
    }
}

impl<K, V> SkipMap<K, V>
where
    K: Ord,
{
    /// Returns the entry with the smallest key.
    ///
    /// This function returns an [`Entry`] which
    /// can be used to access the key's associated value.
    ///
    /// # Example
    /// ```
    /// use turso_core::skiplist::SkipMap;
    ///
    /// let numbers = SkipMap::new();
    /// numbers.insert(5, "five");
    /// assert_eq!(*numbers.front().unwrap().value(), "five");
    /// numbers.insert(6, "six");
    /// assert_eq!(*numbers.front().unwrap().value(), "five");
    /// ```
    pub fn front(&self) -> Option<Entry<'_, K, V>> {
        let guard = &epoch::pin();
        try_pin_loop(|| self.inner.front(guard)).map(Entry::new)
    }

    /// Returns the entry with the largest key.
    ///
    /// This function returns an [`Entry`] which
    /// can be used to access the key's associated value.
    ///
    /// # Example
    /// ```
    /// use turso_core::skiplist::SkipMap;
    ///
    /// let numbers = SkipMap::new();
    /// numbers.insert(5, "five");
    /// assert_eq!(*numbers.back().unwrap().value(), "five");
    /// numbers.insert(6, "six");
    /// assert_eq!(*numbers.back().unwrap().value(), "six");
    /// ```
    pub fn back(&self) -> Option<Entry<'_, K, V>> {
        let guard = &epoch::pin();
        try_pin_loop(|| self.inner.back(guard)).map(Entry::new)
    }

    /// Returns `true` if the map contains a value for the specified key.
    ///
    /// # Example
    /// ```
    /// use turso_core::skiplist::SkipMap;
    ///
    /// let ages = SkipMap::new();
    /// ages.insert("Bill Gates", 64);
    ///
    /// assert!(ages.contains_key(&"Bill Gates"));
    /// assert!(!ages.contains_key(&"Steve Jobs"));
    /// ```
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let guard = &epoch::pin();
        self.inner.contains_key(key, guard)
    }

    /// Returns an entry with the specified `key`.
    ///
    /// This function returns an [`Entry`] which
    /// can be used to access the key's associated value.
    ///
    /// # Example
    /// ```
    /// use turso_core::skiplist::SkipMap;
    ///
    /// let numbers: SkipMap<&str, i32> = SkipMap::new();
    /// assert!(numbers.get("six").is_none());
    ///
    /// numbers.insert("six", 6);
    /// assert_eq!(*numbers.get("six").unwrap().value(), 6);
    /// ```
    pub fn get<Q>(&self, key: &Q) -> Option<Entry<'_, K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let guard = &epoch::pin();
        try_pin_loop(|| self.inner.get(key, guard)).map(Entry::new)
    }

    /// Returns an `Entry` pointing to the lowest element whose key is above
    /// the given bound. If no such element is found then `None` is
    /// returned.
    ///
    /// This function returns an [`Entry`] which
    /// can be used to access the key's associated value.
    ///
    /// # Example
    /// ```
    /// use turso_core::skiplist::SkipMap;
    /// use std::ops::Bound::*;
    ///
    /// let numbers = SkipMap::new();
    /// numbers.insert(6, "six");
    /// numbers.insert(7, "seven");
    /// numbers.insert(12, "twelve");
    ///
    /// let greater_than_five = numbers.lower_bound(Excluded(&5)).unwrap();
    /// assert_eq!(*greater_than_five.value(), "six");
    ///
    /// let greater_than_six = numbers.lower_bound(Excluded(&6)).unwrap();
    /// assert_eq!(*greater_than_six.value(), "seven");
    ///
    /// let greater_than_thirteen = numbers.lower_bound(Excluded(&13));
    /// assert!(greater_than_thirteen.is_none());
    /// ```
    pub fn lower_bound<'a, Q>(&'a self, bound: Bound<&Q>) -> Option<Entry<'a, K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let guard = &epoch::pin();
        try_pin_loop(|| self.inner.lower_bound(bound, guard)).map(Entry::new)
    }

    /// Returns an `Entry` pointing to the highest element whose key is below
    /// the given bound. If no such element is found then `None` is
    /// returned.
    ///
    /// This function returns an [`Entry`] which
    /// can be used to access the key's associated value.
    ///
    /// # Example
    /// ```
    /// use turso_core::skiplist::SkipMap;
    /// use std::ops::Bound::*;
    ///
    /// let numbers = SkipMap::new();
    /// numbers.insert(6, "six");
    /// numbers.insert(7, "seven");
    /// numbers.insert(12, "twelve");
    ///
    /// let less_than_eight = numbers.upper_bound(Excluded(&8)).unwrap();
    /// assert_eq!(*less_than_eight.value(), "seven");
    ///
    /// let less_than_six = numbers.upper_bound(Excluded(&6));
    /// assert!(less_than_six.is_none());
    /// ```
    pub fn upper_bound<'a, Q>(&'a self, bound: Bound<&Q>) -> Option<Entry<'a, K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let guard = &epoch::pin();
        try_pin_loop(|| self.inner.upper_bound(bound, guard)).map(Entry::new)
    }

    /// Finds an entry with the specified key, or inserts a new `key`-`value` pair if none exist.
    ////
    /// This function returns an [`Entry`] which
    /// can be used to access the key's associated value.
    ///
    /// # Example
    /// ```
    /// use turso_core::skiplist::SkipMap;
    ///
    /// let ages = SkipMap::new();
    /// let gates_age = ages.get_or_insert("Bill Gates", 64);
    /// assert_eq!(*gates_age.value(), 64);
    ///
    /// ages.insert("Steve Jobs", 65);
    /// let jobs_age = ages.get_or_insert("Steve Jobs", -1);
    /// assert_eq!(*jobs_age.value(), 65);
    /// ```
    pub fn get_or_insert(&self, key: K, value: V) -> Entry<'_, K, V> {
        let guard = &epoch::pin();
        Entry::new(self.inner.get_or_insert(key, value, guard))
    }

    /// Finds an entry with the specified key, or inserts a new `key`-`value` pair if none exist,
    /// where value is calculated with a function.
    ///
    ///
    /// <b>Note:</b> Another thread may write key value first, leading to the result of this closure
    /// discarded. If closure is modifying some other state (such as shared counters or shared
    /// objects), it may lead to <u>undesired behaviour</u> such as counters being changed without
    /// result of closure inserted
    ////
    /// This function returns an [`Entry`] which
    /// can be used to access the key's associated value.
    ///
    ///
    /// # Example
    /// ```
    /// use turso_core::skiplist::SkipMap;
    ///
    /// let ages = SkipMap::new();
    /// let gates_age = ages.get_or_insert_with("Bill Gates", || 64);
    /// assert_eq!(*gates_age.value(), 64);
    ///
    /// ages.insert("Steve Jobs", 65);
    /// let jobs_age = ages.get_or_insert_with("Steve Jobs", || -1);
    /// assert_eq!(*jobs_age.value(), 65);
    /// ```
    pub fn get_or_insert_with<F>(&self, key: K, value_fn: F) -> Entry<'_, K, V>
    where
        F: FnOnce() -> V,
    {
        let guard = &epoch::pin();
        Entry::new(self.inner.get_or_insert_with(key, value_fn, guard))
    }

    /// Returns an iterator over all entries in the map,
    /// sorted by key.
    ///
    /// This iterator returns [`Entry`]s which
    /// can be used to access keys and their associated values.
    ///
    /// # Examples
    /// ```
    /// use turso_core::skiplist::SkipMap;
    ///
    /// let numbers = SkipMap::new();
    /// numbers.insert(6, "six");
    /// numbers.insert(7, "seven");
    /// numbers.insert(12, "twelve");
    ///
    /// // Print then numbers from least to greatest
    /// for entry in numbers.iter() {
    ///     let number = entry.key();
    ///     let number_str = entry.value();
    ///     println!("{} is {}", number, number_str);
    /// }
    /// ```
    pub fn iter(&self) -> Iter<'_, K, V> {
        Iter {
            inner: self.inner.ref_iter(),
        }
    }

    /// Returns an iterator over a subset of entries in the map.
    ///
    /// This iterator returns [`Entry`]s which
    /// can be used to access keys and their associated values.
    ///
    /// # Example
    /// ```
    /// use turso_core::skiplist::SkipMap;
    ///
    /// let numbers = SkipMap::new();
    /// numbers.insert(6, "six");
    /// numbers.insert(7, "seven");
    /// numbers.insert(12, "twelve");
    ///
    /// // Print all numbers in the map between 5 and 8.
    /// for entry in numbers.range(5..=8) {
    ///     let number = entry.key();
    ///     let number_str = entry.value();
    ///     println!("{} is {}", number, number_str);
    /// }
    /// ```
    pub fn range<Q, R>(&self, range: R) -> Range<'_, Q, R, K, V>
    where
        K: Borrow<Q>,
        R: RangeBounds<Q>,
        Q: Ord + ?Sized,
    {
        Range {
            inner: self.inner.ref_range(range),
        }
    }
}

impl<K, V> SkipMap<K, V>
where
    K: Ord + Send + 'static,
    V: Send + 'static,
{
    /// Inserts a `key`-`value` pair into the map and returns the new entry.
    ///
    /// If there is an existing entry with this key, it will be removed before inserting the new
    /// one.
    ///
    /// This function returns an [`Entry`] which
    /// can be used to access the inserted key's associated value.
    ///
    /// # Example
    /// ```
    /// use turso_core::skiplist::SkipMap;
    ///
    /// let map = SkipMap::new();
    /// map.insert("key", "value");
    ///
    /// assert_eq!(*map.get("key").unwrap().value(), "value");
    /// ```
    pub fn insert(&self, key: K, value: V) -> Entry<'_, K, V> {
        let guard = &epoch::pin();
        Entry::new(self.inner.insert(key, value, guard))
    }

    /// Inserts a `key`-`value` pair into the skip list and returns the new entry.
    ///
    /// If there is an existing entry with this key and compare(entry.value) returns true,
    /// it will be removed before inserting the new one.
    /// The closure will not be called if the key is not present.
    ///
    /// This function returns an [`Entry`] which
    /// can be used to access the inserted key's associated value.
    ///
    /// # Example
    /// ```
    /// use turso_core::skiplist::SkipMap;
    ///
    /// let map = SkipMap::new();
    /// map.insert("key", 1);
    /// map.compare_insert("key", 0, |x| x < &0);
    /// assert_eq!(*map.get("key").unwrap().value(), 1);
    /// map.compare_insert("key", 2, |x| x < &2);
    /// assert_eq!(*map.get("key").unwrap().value(), 2);
    /// map.compare_insert("absent_key", 0, |_| false);
    /// assert_eq!(*map.get("absent_key").unwrap().value(), 0);
    /// ```
    pub fn compare_insert<F>(&self, key: K, value: V, compare_fn: F) -> Entry<'_, K, V>
    where
        F: Fn(&V) -> bool,
    {
        let guard = &epoch::pin();
        Entry::new(self.inner.compare_insert(key, value, compare_fn, guard))
    }

    /// Removes an entry with the specified `key` from the map and returns it.
    ///
    /// The value will not actually be dropped until all references to it have gone
    /// out of scope.
    ///
    /// This function returns an [`Entry`] which
    /// can be used to access the removed key's associated value.
    ///
    /// # Example
    /// ```
    /// use turso_core::skiplist::SkipMap;
    ///
    /// let map: SkipMap<&str, &str> = SkipMap::new();
    /// assert!(map.remove("invalid key").is_none());
    ///
    /// map.insert("key", "value");
    /// assert_eq!(*map.remove("key").unwrap().value(), "value");
    /// ```
    pub fn remove<Q>(&self, key: &Q) -> Option<Entry<'_, K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let guard = &epoch::pin();
        self.inner.remove(key, guard).map(Entry::new)
    }

    /// Removes the entry with the lowest key
    /// from the map. Returns the removed entry.
    ///
    /// The value will not actually be dropped until all references to it have gone
    /// out of scope.
    ///
    /// # Example
    /// ```
    /// use turso_core::skiplist::SkipMap;
    ///
    /// let numbers = SkipMap::new();
    /// numbers.insert(6, "six");
    /// numbers.insert(7, "seven");
    /// numbers.insert(12, "twelve");
    ///
    /// assert_eq!(*numbers.pop_front().unwrap().value(), "six");
    /// assert_eq!(*numbers.pop_front().unwrap().value(), "seven");
    /// assert_eq!(*numbers.pop_front().unwrap().value(), "twelve");
    ///
    /// // All entries have been removed now.
    /// assert!(numbers.is_empty());
    /// ```
    pub fn pop_front(&self) -> Option<Entry<'_, K, V>> {
        let guard = &epoch::pin();
        self.inner.pop_front(guard).map(Entry::new)
    }

    /// Removes the entry with the greatest key from the map.
    /// Returns the removed entry.
    ///
    /// The value will not actually be dropped until all references to it have gone
    /// out of scope.
    ///
    /// # Example
    /// ```
    /// use turso_core::skiplist::SkipMap;
    ///
    /// let numbers = SkipMap::new();
    /// numbers.insert(6, "six");
    /// numbers.insert(7, "seven");
    /// numbers.insert(12, "twelve");
    ///
    /// assert_eq!(*numbers.pop_back().unwrap().value(), "twelve");
    /// assert_eq!(*numbers.pop_back().unwrap().value(), "seven");
    /// assert_eq!(*numbers.pop_back().unwrap().value(), "six");
    ///
    /// // All entries have been removed now.
    /// assert!(numbers.is_empty());
    /// ```
    pub fn pop_back(&self) -> Option<Entry<'_, K, V>> {
        let guard = &epoch::pin();
        self.inner.pop_back(guard).map(Entry::new)
    }

    /// Removes all entries from the map.
    ///
    /// # Example
    /// ```
    /// use turso_core::skiplist::SkipMap;
    ///
    /// let people = SkipMap::new();
    /// people.insert("Bill", "Gates");
    /// people.insert("Steve", "Jobs");
    ///
    /// people.clear();
    /// assert!(people.is_empty());
    /// ```
    pub fn clear(&self) {
        let guard = &mut epoch::pin();
        self.inner.clear(guard);
    }
}

impl<K, V> Default for SkipMap<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> fmt::Debug for SkipMap<K, V>
where
    K: Ord + fmt::Debug,
    V: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad("SkipMap { .. }")
    }
}

impl<K, V> IntoIterator for SkipMap<K, V> {
    type Item = (K, V);
    type IntoIter = IntoIter<K, V>;

    fn into_iter(self) -> IntoIter<K, V> {
        IntoIter {
            inner: self.inner.into_iter(),
        }
    }
}

impl<'a, K, V> IntoIterator for &'a SkipMap<K, V>
where
    K: Ord,
{
    type Item = Entry<'a, K, V>;
    type IntoIter = Iter<'a, K, V>;

    fn into_iter(self) -> Iter<'a, K, V> {
        self.iter()
    }
}

impl<K, V> FromIterator<(K, V)> for SkipMap<K, V>
where
    K: Ord,
{
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
    {
        let s = Self::new();
        for (k, v) in iter {
            s.get_or_insert(k, v);
        }
        s
    }
}

/// A reference-counted entry in a map.
pub struct Entry<'a, K, V> {
    inner: ManuallyDrop<base::RefEntry<'a, K, V>>,
}

impl<'a, K, V> Entry<'a, K, V> {
    fn new(inner: base::RefEntry<'a, K, V>) -> Self {
        Self {
            inner: ManuallyDrop::new(inner),
        }
    }

    /// Returns a reference to the key.
    pub fn key(&self) -> &K {
        self.inner.key()
    }

    /// Returns a reference to the value.
    pub fn value(&self) -> &V {
        self.inner.value()
    }

    /// Returns `true` if the entry is removed from the map.
    pub fn is_removed(&self) -> bool {
        self.inner.is_removed()
    }
}

impl<K, V> Drop for Entry<'_, K, V> {
    fn drop(&mut self) {
        unsafe {
            ManuallyDrop::into_inner(ptr::read(&self.inner)).release_with_pin(epoch::pin);
        }
    }
}

impl<'a, K, V> Entry<'a, K, V>
where
    K: Ord,
{
    /// Moves to the next entry in the map.
    pub fn move_next(&mut self) -> bool {
        let guard = &epoch::pin();
        self.inner.move_next(guard)
    }

    /// Moves to the previous entry in the map.
    pub fn move_prev(&mut self) -> bool {
        let guard = &epoch::pin();
        self.inner.move_prev(guard)
    }

    /// Returns the next entry in the map.
    pub fn next(&self) -> Option<Entry<'a, K, V>> {
        let guard = &epoch::pin();
        self.inner.next(guard).map(Entry::new)
    }

    /// Returns the previous entry in the map.
    pub fn prev(&self) -> Option<Entry<'a, K, V>> {
        let guard = &epoch::pin();
        self.inner.prev(guard).map(Entry::new)
    }
}

impl<K, V> Entry<'_, K, V>
where
    K: Ord + Send + 'static,
    V: Send + 'static,
{
    /// Removes the entry from the map.
    ///
    /// Returns `true` if this call removed the entry and `false` if it was already removed.
    pub fn remove(&self) -> bool {
        let guard = &epoch::pin();
        self.inner.remove(guard)
    }
}

impl<K, V> Clone for Entry<'_, K, V> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<K, V> fmt::Debug for Entry<'_, K, V>
where
    K: fmt::Debug,
    V: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Entry")
            .field(self.key())
            .field(self.value())
            .finish()
    }
}

/// An owning iterator over the entries of a `SkipMap`.
pub struct IntoIter<K, V> {
    inner: base::IntoIter<K, V>,
}

impl<K, V> Iterator for IntoIter<K, V> {
    type Item = (K, V);

    fn next(&mut self) -> Option<(K, V)> {
        self.inner.next()
    }
}

impl<K, V> fmt::Debug for IntoIter<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad("IntoIter { .. }")
    }
}

/// An iterator over the entries of a `SkipMap`.
pub struct Iter<'a, K, V> {
    inner: base::RefIter<'a, K, V>,
}

impl<'a, K, V> Iterator for Iter<'a, K, V>
where
    K: Ord,
{
    type Item = Entry<'a, K, V>;

    fn next(&mut self) -> Option<Entry<'a, K, V>> {
        let guard = &epoch::pin();
        self.inner.next(guard).map(Entry::new)
    }
}

impl<'a, K, V> DoubleEndedIterator for Iter<'a, K, V>
where
    K: Ord,
{
    fn next_back(&mut self) -> Option<Entry<'a, K, V>> {
        let guard = &epoch::pin();
        self.inner.next_back(guard).map(Entry::new)
    }
}

impl<K, V> fmt::Debug for Iter<'_, K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad("Iter { .. }")
    }
}

impl<K, V> Drop for Iter<'_, K, V> {
    fn drop(&mut self) {
        let guard = &epoch::pin();
        self.inner.drop_impl(guard);
    }
}

/// An iterator over a subset of entries of a `SkipMap`.
pub struct Range<'a, Q, R, K, V>
where
    K: Ord + Borrow<Q>,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
{
    pub(crate) inner: base::RefRange<'a, Q, R, K, V>,
}

impl<'a, Q, R, K, V> Iterator for Range<'a, Q, R, K, V>
where
    K: Ord + Borrow<Q>,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
{
    type Item = Entry<'a, K, V>;

    fn next(&mut self) -> Option<Entry<'a, K, V>> {
        let guard = &epoch::pin();
        self.inner.next(guard).map(Entry::new)
    }
}

impl<'a, Q, R, K, V> DoubleEndedIterator for Range<'a, Q, R, K, V>
where
    K: Ord + Borrow<Q>,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
{
    fn next_back(&mut self) -> Option<Entry<'a, K, V>> {
        let guard = &epoch::pin();
        self.inner.next_back(guard).map(Entry::new)
    }
}

impl<Q, R, K, V> fmt::Debug for Range<'_, Q, R, K, V>
where
    K: Ord + Borrow<Q> + fmt::Debug,
    V: fmt::Debug,
    R: RangeBounds<Q> + fmt::Debug,
    Q: Ord + ?Sized,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Range")
            .field("range", &self.inner.range)
            .field("head", &self.inner.head)
            .field("tail", &self.inner.tail)
            .finish()
    }
}

impl<Q, R, K, V> Drop for Range<'_, Q, R, K, V>
where
    K: Ord + Borrow<Q>,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
{
    fn drop(&mut self) {
        let guard = &epoch::pin();
        self.inner.drop_impl(guard);
    }
}
