//! Filepath: src/tree/range/iterator/adapters.rs
//!
//! Key-only and value-only iterator adapters.

use std::fmt::{self as StdFmt, Debug, Formatter};
use std::iter::FusedIterator;

use crate::alloc_trait::TreeAllocator;
use crate::policy::LeafPolicy;

use super::RangeIter;

// ============================================================================
//  KeysIter
// ============================================================================

/// Iterator adapter that yields only keys.
pub struct KeysIter<'a, 'g, P, A>
where
    P: LeafPolicy,
    A: TreeAllocator<P>,
{
    pub(super) inner: RangeIter<'a, 'g, P, A>,
}

impl<P, A> Debug for KeysIter<'_, '_, P, A>
where
    P: LeafPolicy,
    A: TreeAllocator<P>,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> StdFmt::Result {
        f.debug_struct("KeysIter")
            .field("inner", &self.inner)
            .finish()
    }
}

impl<P, A> Iterator for KeysIter<'_, '_, P, A>
where
    P: LeafPolicy,
    A: TreeAllocator<P>,
{
    type Item = Vec<u8>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|entry| entry.key)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<P, A> FusedIterator for KeysIter<'_, '_, P, A>
where
    P: LeafPolicy,
    A: TreeAllocator<P>,
{
}

// ============================================================================
//  ValuesIter
// ============================================================================

/// Iterator adapter that yields only values.
pub struct ValuesIter<'a, 'g, P, A>
where
    P: LeafPolicy,
    A: TreeAllocator<P>,
{
    pub(super) inner: RangeIter<'a, 'g, P, A>,
}

impl<P, A> Debug for ValuesIter<'_, '_, P, A>
where
    P: LeafPolicy,
    A: TreeAllocator<P>,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> StdFmt::Result {
        f.debug_struct("ValuesIter")
            .field("inner", &self.inner)
            .finish()
    }
}

impl<P, A> Iterator for ValuesIter<'_, '_, P, A>
where
    P: LeafPolicy,
    A: TreeAllocator<P>,
{
    type Item = P::Output;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|entry| entry.value)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<P, A> FusedIterator for ValuesIter<'_, '_, P, A>
where
    P: LeafPolicy,
    A: TreeAllocator<P>,
{
}
