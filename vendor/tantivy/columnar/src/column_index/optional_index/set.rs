use std::io;

/// A codec makes it possible to serialize a set of
/// elements, and open the resulting Set representation.
pub trait SetCodec {
    type Item: Copy + TryFrom<usize> + Eq + std::hash::Hash + std::fmt::Debug;
    type Reader<'a>: Set<Self::Item>;

    /// Serializes a set of unique sorted u16 elements.
    ///
    /// May panic if the elements are not sorted.
    fn serialize(els: impl Iterator<Item = Self::Item>, wrt: impl io::Write) -> io::Result<()>;
    fn open(data: &[u8]) -> Self::Reader<'_>;
}

/// Stateful object that makes it possible to compute several select in a row,
/// provided the rank passed as argument are increasing.
pub trait SelectCursor<T> {
    // May panic if rank is greater than the number of elements in the Set,
    // or if rank is < than value provided in the previous call.
    fn select(&mut self, rank: T) -> T;
}

pub trait Set<T> {
    type SelectCursor<'b>: SelectCursor<T>
    where Self: 'b;

    /// Returns true if the elements is contained in the Set
    fn contains(&self, el: T) -> bool;

    /// Returns the element's rank (its position in the set).
    /// If the set does not contain the element, it will return the next existing elements rank.
    fn rank(&self, el: T) -> T;

    /// If the set contains `el`, returns the element's rank (its position in the set).
    /// If the set does not contain the element, it returns `None`.
    fn rank_if_exists(&self, el: T) -> Option<T>;

    /// Return the rank-th value stored in this bitmap.
    ///
    /// # Panics
    ///
    /// May panic if rank is greater or equal to the number of
    /// elements in the Set.
    fn select(&self, rank: T) -> T;

    /// Creates a brand new select cursor.
    fn select_cursor(&self) -> Self::SelectCursor<'_>;
}
