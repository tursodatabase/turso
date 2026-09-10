use std::cell::RefCell;
use std::iter::Peekable;
use std::rc::Rc;

pub trait GroupByIteratorExtended: Iterator {
    /// Return an `Iterator` that groups iterator elements. Consecutive elements that map to the
    /// same key are assigned to the same group.
    ///
    /// The returned Iterator item is `(K, impl Iterator)`, where Iterator are the items of the
    /// group.
    ///
    /// ```
    /// use tantivy_common::GroupByIteratorExtended;
    ///
    /// // group data into blocks of larger than zero or not.
    /// let data: Vec<i32> = vec![1, 3, -2, -2, 1, 0, 1, 2];
    /// // groups:               |---->|------>|--------->|
    ///
    /// let mut data_grouped = Vec::new();
    /// // Note: group is an iterator
    /// for (key, group) in data.into_iter().group_by(|val| *val >= 0) {
    ///     data_grouped.push((key, group.collect()));
    /// }
    /// assert_eq!(data_grouped, vec![(true, vec![1, 3]), (false, vec![-2, -2]), (true, vec![1, 0, 1, 2])]);
    /// ```
    fn group_by<K, F>(self, key: F) -> GroupByIterator<Self, F, K>
    where
        Self: Sized,
        F: FnMut(&Self::Item) -> K,
        K: PartialEq + Clone,
        Self::Item: Clone,
    {
        GroupByIterator::new(self, key)
    }
}
impl<I: Iterator> GroupByIteratorExtended for I {}

pub struct GroupByIterator<I, F, K: Clone>
where
    I: Iterator,
    F: FnMut(&I::Item) -> K,
{
    // I really would like to avoid the Rc<RefCell>, but the Iterator is shared between
    // `GroupByIterator` and `GroupIter`. In practice they are used consecutive and
    // `GroupByIter` is finished before calling next on `GroupByIterator`. I'm not sure there
    // is a solution with lifetimes for that, because we would need to enforce it in the usage
    // somehow.
    //
    // One potential solution would be to replace the iterator approach with something similar.
    inner: Rc<RefCell<GroupByShared<I, F, K>>>,
}

struct GroupByShared<I, F, K: Clone>
where
    I: Iterator,
    F: FnMut(&I::Item) -> K,
{
    iter: Peekable<I>,
    group_by_fn: F,
}

impl<I, F, K> GroupByIterator<I, F, K>
where
    I: Iterator,
    F: FnMut(&I::Item) -> K,
    K: Clone,
{
    fn new(inner: I, group_by_fn: F) -> Self {
        let inner = GroupByShared {
            iter: inner.peekable(),
            group_by_fn,
        };

        Self {
            inner: Rc::new(RefCell::new(inner)),
        }
    }
}

impl<I, F, K> Iterator for GroupByIterator<I, F, K>
where
    I: Iterator,
    I::Item: Clone,
    F: FnMut(&I::Item) -> K,
    K: Clone,
{
    type Item = (K, GroupIterator<I, F, K>);

    fn next(&mut self) -> Option<Self::Item> {
        let mut inner = self.inner.borrow_mut();
        let value = inner.iter.peek()?.clone();
        let key = (inner.group_by_fn)(&value);

        let inner = self.inner.clone();

        let group_iter = GroupIterator {
            inner,
            group_key: key.clone(),
        };
        Some((key, group_iter))
    }
}

pub struct GroupIterator<I, F, K: Clone>
where
    I: Iterator,
    F: FnMut(&I::Item) -> K,
{
    inner: Rc<RefCell<GroupByShared<I, F, K>>>,
    group_key: K,
}

impl<I, F, K: PartialEq + Clone> Iterator for GroupIterator<I, F, K>
where
    I: Iterator,
    I::Item: Clone,
    F: FnMut(&I::Item) -> K,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        let mut inner = self.inner.borrow_mut();
        // peek if next value is in group
        let peek_val = inner.iter.peek()?.clone();
        if (inner.group_by_fn)(&peek_val) == self.group_key {
            inner.iter.next()
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn group_by_collect<I: Iterator<Item = u32>>(iter: I) -> Vec<(I::Item, Vec<I::Item>)> {
        iter.group_by(|val| val / 10)
            .map(|(el, iter)| (el, iter.collect::<Vec<_>>()))
            .collect::<Vec<_>>()
    }

    #[test]
    fn group_by_two_groups() {
        let vals = vec![1u32, 4, 15];
        let grouped_vals = group_by_collect(vals.into_iter());
        assert_eq!(grouped_vals, vec![(0, vec![1, 4]), (1, vec![15])]);
    }

    #[test]
    fn group_by_test_empty() {
        let vals = vec![];
        let grouped_vals = group_by_collect(vals.into_iter());
        assert_eq!(grouped_vals, vec![]);
    }

    #[test]
    fn group_by_three_groups() {
        let vals = vec![1u32, 4, 15, 1];
        let grouped_vals = group_by_collect(vals.into_iter());
        assert_eq!(
            grouped_vals,
            vec![(0, vec![1, 4]), (1, vec![15]), (0, vec![1])]
        );
    }
}
