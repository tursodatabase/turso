//! A set based on a lock-free skip list. See [`SkipSet`].

use std::borrow::Borrow;
use std::fmt;
use std::ops::Deref;
use std::ops::{Bound, RangeBounds};

use super::map;

/// A set based on a lock-free skip list.
///
/// This is an alternative to [`BTreeSet`] which supports
/// concurrent access across multiple threads.
///
/// [`BTreeSet`]: std::collections::BTreeSet
pub struct SkipSet<T> {
    inner: map::SkipMap<T, ()>,
}

impl<T> SkipSet<T> {
    /// Returns a new, empty set.
    ///
    /// # Example
    ///
    /// ```
    /// use turso_core::skiplist::SkipSet;
    ///
    /// let set: SkipSet<i32> = SkipSet::new();
    /// ```
    pub fn new() -> Self {
        Self {
            inner: map::SkipMap::new(),
        }
    }

    /// Returns `true` if the set is empty.
    ///
    /// # Example
    ///
    /// ```
    /// use turso_core::skiplist::SkipSet;
    ///
    /// let set = SkipSet::new();
    /// assert!(set.is_empty());
    ///
    /// set.insert(1);
    /// assert!(!set.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Returns the number of entries in the set.
    ///
    /// If the set is being concurrently modified, consider the returned number just an
    /// approximation without any guarantees.
    ///
    /// # Example
    ///
    /// ```
    /// use turso_core::skiplist::SkipSet;
    ///
    /// let set = SkipSet::new();
    /// assert_eq!(set.len(), 0);
    ///
    /// set.insert(1);
    /// assert_eq!(set.len(), 1);
    /// ```
    pub fn len(&self) -> usize {
        self.inner.len()
    }
}

impl<T> SkipSet<T>
where
    T: Ord,
{
    /// Returns the entry with the smallest key.
    ///
    /// # Example
    ///
    /// ```
    /// use turso_core::skiplist::SkipSet;
    ///
    /// let set = SkipSet::new();
    /// set.insert(1);
    /// assert_eq!(*set.front().unwrap(), 1);
    /// set.insert(2);
    /// assert_eq!(*set.front().unwrap(), 1);
    /// ```
    pub fn front(&self) -> Option<Entry<'_, T>> {
        self.inner.front().map(Entry::new)
    }

    /// Returns the entry with the largest key.
    ///
    /// # Example
    ///
    /// ```
    /// use turso_core::skiplist::SkipSet;
    ///
    /// let set = SkipSet::new();
    /// set.insert(1);
    /// assert_eq!(*set.back().unwrap(), 1);
    /// set.insert(2);
    /// assert_eq!(*set.back().unwrap(), 2);
    /// ```
    pub fn back(&self) -> Option<Entry<'_, T>> {
        self.inner.back().map(Entry::new)
    }

    /// Returns `true` if the set contains a value for the specified key.
    ///
    /// # Example
    ///
    /// ```
    /// use turso_core::skiplist::SkipSet;
    ///
    /// let set: SkipSet<_> = (1..=3).collect();
    /// assert!(set.contains(&1));
    /// assert!(!set.contains(&4));
    /// ```
    pub fn contains<Q>(&self, key: &Q) -> bool
    where
        T: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.inner.contains_key(key)
    }

    /// Returns an entry with the specified `key`.
    ///
    /// # Example
    ///
    /// ```
    /// use turso_core::skiplist::SkipSet;
    ///
    /// let set: SkipSet<_> = (1..=3).collect();
    /// assert_eq!(*set.get(&3).unwrap(), 3);
    /// assert!(set.get(&4).is_none());
    /// ```
    pub fn get<Q>(&self, key: &Q) -> Option<Entry<'_, T>>
    where
        T: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.inner.get(key).map(Entry::new)
    }

    /// Returns an `Entry` pointing to the lowest element whose key is above
    /// the given bound. If no such element is found then `None` is
    /// returned.
    ///
    /// # Example
    ///
    /// ```
    /// use turso_core::skiplist::SkipSet;
    /// use std::ops::Bound::*;
    ///
    /// let set = SkipSet::new();
    /// set.insert(6);
    /// set.insert(7);
    /// set.insert(12);
    ///
    /// let greater_than_five = set.lower_bound(Excluded(&5)).unwrap();
    /// assert_eq!(*greater_than_five, 6);
    ///
    /// let greater_than_six = set.lower_bound(Excluded(&6)).unwrap();
    /// assert_eq!(*greater_than_six, 7);
    ///
    /// let greater_than_thirteen = set.lower_bound(Excluded(&13));
    /// assert!(greater_than_thirteen.is_none());
    /// ```
    pub fn lower_bound<'a, Q>(&'a self, bound: Bound<&Q>) -> Option<Entry<'a, T>>
    where
        T: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.inner.lower_bound(bound).map(Entry::new)
    }

    /// Returns an `Entry` pointing to the highest element whose key is below
    /// the given bound. If no such element is found then `None` is
    /// returned.
    ///
    /// # Example
    ///
    /// ```
    /// use turso_core::skiplist::SkipSet;
    /// use std::ops::Bound::*;
    ///
    /// let set = SkipSet::new();
    /// set.insert(6);
    /// set.insert(7);
    /// set.insert(12);
    ///
    /// let less_than_eight = set.upper_bound(Excluded(&8)).unwrap();
    /// assert_eq!(*less_than_eight, 7);
    ///
    /// let less_than_six = set.upper_bound(Excluded(&6));
    /// assert!(less_than_six.is_none());
    /// ```
    pub fn upper_bound<'a, Q>(&'a self, bound: Bound<&Q>) -> Option<Entry<'a, T>>
    where
        T: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.inner.upper_bound(bound).map(Entry::new)
    }

    /// Finds an entry with the specified key, or inserts a new `key`-`value` pair if none exist.
    ///
    /// # Example
    ///
    /// ```
    /// use turso_core::skiplist::SkipSet;
    ///
    /// let set = SkipSet::new();
    /// let entry = set.get_or_insert(2);
    /// assert_eq!(*entry, 2);
    /// ```
    pub fn get_or_insert(&self, key: T) -> Entry<'_, T> {
        Entry::new(self.inner.get_or_insert(key, ()))
    }

    /// Returns an iterator over all entries in the set.
    ///
    /// # Examples
    ///
    /// ```
    /// use turso_core::skiplist::SkipSet;
    ///
    /// let set = SkipSet::new();
    /// set.insert(6);
    /// set.insert(7);
    /// set.insert(12);
    ///
    /// let mut set_iter = set.iter();
    /// assert_eq!(*set_iter.next().unwrap(), 6);
    /// assert_eq!(*set_iter.next().unwrap(), 7);
    /// assert_eq!(*set_iter.next().unwrap(), 12);
    /// assert!(set_iter.next().is_none());
    /// ```
    pub fn iter(&self) -> Iter<'_, T> {
        Iter {
            inner: self.inner.iter(),
        }
    }

    /// Returns an iterator over a subset of entries in the set.
    ///
    /// # Example
    ///
    /// ```
    /// use turso_core::skiplist::SkipSet;
    ///
    /// let set = SkipSet::new();
    /// set.insert(6);
    /// set.insert(7);
    /// set.insert(12);
    ///
    /// let mut set_range = set.range(5..=8);
    /// assert_eq!(*set_range.next().unwrap(), 6);
    /// assert_eq!(*set_range.next().unwrap(), 7);
    /// assert!(set_range.next().is_none());
    /// ```
    pub fn range<Q, R>(&self, range: R) -> Range<'_, Q, R, T>
    where
        T: Borrow<Q>,
        R: RangeBounds<Q>,
        Q: Ord + ?Sized,
    {
        Range {
            inner: self.inner.range(range),
        }
    }
}

impl<T> SkipSet<T>
where
    T: Ord + Send + 'static,
{
    /// Inserts a `key`-`value` pair into the set and returns the new entry.
    ///
    /// If there is an existing entry with this key, it will be removed before inserting the new
    /// one.
    ///
    /// # Example
    ///
    /// ```
    /// use turso_core::skiplist::SkipSet;
    ///
    /// let set = SkipSet::new();
    /// set.insert(2);
    /// assert_eq!(*set.get(&2).unwrap(), 2);
    /// ```
    pub fn insert(&self, key: T) -> Entry<'_, T> {
        Entry::new(self.inner.insert(key, ()))
    }

    /// Removes an entry with the specified key from the set and returns it.
    ///
    /// The value will not actually be dropped until all references to it have gone
    /// out of scope.
    ///
    /// # Example
    ///
    /// ```
    /// use turso_core::skiplist::SkipSet;
    ///
    /// let set = SkipSet::new();
    /// set.insert(2);
    /// assert_eq!(*set.remove(&2).unwrap(), 2);
    /// assert!(set.remove(&2).is_none());
    /// ```
    pub fn remove<Q>(&self, key: &Q) -> Option<Entry<'_, T>>
    where
        T: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.inner.remove(key).map(Entry::new)
    }

    /// Removes an entry from the front of the set.
    /// Returns the removed entry.
    ///
    /// The value will not actually be dropped until all references to it have gone
    /// out of scope.
    ///
    /// # Example
    ///
    /// ```
    /// use turso_core::skiplist::SkipSet;
    ///
    /// let set = SkipSet::new();
    /// set.insert(1);
    /// set.insert(2);
    ///
    /// assert_eq!(*set.pop_front().unwrap(), 1);
    /// assert_eq!(*set.pop_front().unwrap(), 2);
    ///
    /// // All entries have been removed now.
    /// assert!(set.is_empty());
    /// ```
    pub fn pop_front(&self) -> Option<Entry<'_, T>> {
        self.inner.pop_front().map(Entry::new)
    }

    /// Removes an entry from the back of the set.
    /// Returns the removed entry.
    ///
    /// The value will not actually be dropped until all references to it have gone
    /// out of scope.
    ///
    /// # Example
    ///
    /// ```
    /// use turso_core::skiplist::SkipSet;
    ///
    /// let set = SkipSet::new();
    /// set.insert(1);
    /// set.insert(2);
    ///
    /// assert_eq!(*set.pop_back().unwrap(), 2);
    /// assert_eq!(*set.pop_back().unwrap(), 1);
    ///
    /// // All entries have been removed now.
    /// assert!(set.is_empty());
    /// ```
    pub fn pop_back(&self) -> Option<Entry<'_, T>> {
        self.inner.pop_back().map(Entry::new)
    }

    /// Iterates over the set and removes every entry.
    ///
    /// # Example
    ///
    /// ```
    /// use turso_core::skiplist::SkipSet;
    ///
    /// let set = SkipSet::new();
    /// set.insert(1);
    /// set.insert(2);
    ///
    /// set.clear();
    /// assert!(set.is_empty());
    /// ```
    pub fn clear(&self) {
        self.inner.clear();
    }
}

impl<T> Default for SkipSet<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> fmt::Debug for SkipSet<T>
where
    T: Ord + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad("SkipSet { .. }")
    }
}

impl<T> IntoIterator for SkipSet<T> {
    type Item = T;
    type IntoIter = IntoIter<T>;

    fn into_iter(self) -> IntoIter<T> {
        IntoIter {
            inner: self.inner.into_iter(),
        }
    }
}

impl<'a, T> IntoIterator for &'a SkipSet<T>
where
    T: Ord,
{
    type Item = Entry<'a, T>;
    type IntoIter = Iter<'a, T>;

    fn into_iter(self) -> Iter<'a, T> {
        self.iter()
    }
}

impl<T> FromIterator<T> for SkipSet<T>
where
    T: Ord,
{
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = T>,
    {
        let s = Self::new();
        for t in iter {
            s.get_or_insert(t);
        }
        s
    }
}

/// A reference-counted entry in a set.
pub struct Entry<'a, T> {
    inner: map::Entry<'a, T, ()>,
}

impl<'a, T> Entry<'a, T> {
    fn new(inner: map::Entry<'a, T, ()>) -> Self {
        Self { inner }
    }

    /// Returns a reference to the value.
    pub fn value(&self) -> &T {
        self.inner.key()
    }

    /// Returns `true` if the entry is removed from the set.
    pub fn is_removed(&self) -> bool {
        self.inner.is_removed()
    }
}

impl<'a, T> Entry<'a, T>
where
    T: Ord,
{
    /// Moves to the next entry in the set.
    pub fn move_next(&mut self) -> bool {
        self.inner.move_next()
    }

    /// Moves to the previous entry in the set.
    pub fn move_prev(&mut self) -> bool {
        self.inner.move_prev()
    }

    /// Returns the next entry in the set.
    pub fn next(&self) -> Option<Entry<'a, T>> {
        self.inner.next().map(Entry::new)
    }

    /// Returns the previous entry in the set.
    pub fn prev(&self) -> Option<Entry<'a, T>> {
        self.inner.prev().map(Entry::new)
    }
}

impl<T> Entry<'_, T>
where
    T: Ord + Send + 'static,
{
    /// Removes the entry from the set.
    ///
    /// Returns `true` if this call removed the entry and `false` if it was already removed.
    pub fn remove(&self) -> bool {
        self.inner.remove()
    }
}

impl<T> Clone for Entry<'_, T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T> fmt::Debug for Entry<'_, T>
where
    T: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Entry")
            .field("value", self.value())
            .finish()
    }
}

impl<T> Deref for Entry<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.value()
    }
}

/// An owning iterator over the entries of a `SkipSet`.
pub struct IntoIter<T> {
    inner: map::IntoIter<T, ()>,
}

impl<T> Iterator for IntoIter<T> {
    type Item = T;

    fn next(&mut self) -> Option<T> {
        self.inner.next().map(|(k, ())| k)
    }
}

impl<T> fmt::Debug for IntoIter<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad("IntoIter { .. }")
    }
}

/// An iterator over the entries of a `SkipSet`.
pub struct Iter<'a, T> {
    inner: map::Iter<'a, T, ()>,
}

impl<'a, T> Iterator for Iter<'a, T>
where
    T: Ord,
{
    type Item = Entry<'a, T>;

    fn next(&mut self) -> Option<Entry<'a, T>> {
        self.inner.next().map(Entry::new)
    }
}

impl<'a, T> DoubleEndedIterator for Iter<'a, T>
where
    T: Ord,
{
    fn next_back(&mut self) -> Option<Entry<'a, T>> {
        self.inner.next_back().map(Entry::new)
    }
}

impl<T> fmt::Debug for Iter<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad("Iter { .. }")
    }
}

/// An iterator over a subset of entries of a `SkipSet`.
pub struct Range<'a, Q, R, T>
where
    T: Ord + Borrow<Q>,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
{
    inner: map::Range<'a, Q, R, T, ()>,
}

impl<'a, Q, R, T> Iterator for Range<'a, Q, R, T>
where
    T: Ord + Borrow<Q>,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
{
    type Item = Entry<'a, T>;

    fn next(&mut self) -> Option<Entry<'a, T>> {
        self.inner.next().map(Entry::new)
    }
}

impl<'a, Q, R, T> DoubleEndedIterator for Range<'a, Q, R, T>
where
    T: Ord + Borrow<Q>,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
{
    fn next_back(&mut self) -> Option<Entry<'a, T>> {
        self.inner.next_back().map(Entry::new)
    }
}

impl<Q, R, T> fmt::Debug for Range<'_, Q, R, T>
where
    T: Ord + Borrow<Q> + fmt::Debug,
    R: RangeBounds<Q> + fmt::Debug,
    Q: Ord + ?Sized,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Range")
            .field("range", &self.inner.inner.range)
            .field("head", &self.inner.inner.head.as_ref().map(|e| e.key()))
            .field("tail", &self.inner.inner.tail.as_ref().map(|e| e.key()))
            .finish()
    }
}
