//! Small planning-independent utility types shared by storage and execution.
//!
//! The legacy SQL plan lived in this module. SQL meaning and physical plans now
//! live in `semantic::hir` and `physical`; only these general utilities remain.

use crate::{alloc, schema::ROWID_SENTINEL};
use std::marker::PhantomData;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IterationDirection {
    Forwards,
    Backwards,
}

/// Tracks schema columns and keeps the rowid sentinel outside the dense bitset.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct ColumnMask {
    bitset: BitSet,
    has_rowid_sentinel: bool,
}

impl ColumnMask {
    pub fn set(&mut self, index: usize) -> Result<(), alloc::TryReserveError> {
        if index == ROWID_SENTINEL {
            self.has_rowid_sentinel = true;
        } else {
            self.bitset.set(index)?;
        }
        Ok(())
    }

    pub fn union_with(&mut self, other: &Self) -> Result<(), alloc::TryReserveError> {
        self.bitset.union_with(&other.bitset)?;
        self.has_rowid_sentinel |= other.has_rowid_sentinel;
        Ok(())
    }

    pub fn get(&self, index: usize) -> bool {
        if index == ROWID_SENTINEL {
            self.has_rowid_sentinel
        } else {
            self.bitset.get(index)
        }
    }

    pub fn count(&self) -> usize {
        self.bitset.count() + usize::from(self.has_rowid_sentinel)
    }

    pub fn is_empty(&self) -> bool {
        self.bitset.is_empty() && !self.has_rowid_sentinel
    }

    pub fn iter(&self) -> impl Iterator<Item = usize> + '_ {
        self.bitset
            .iter()
            .chain(self.has_rowid_sentinel.then_some(ROWID_SENTINEL))
    }
}

impl std::ops::SubAssign<&Self> for ColumnMask {
    fn sub_assign(&mut self, rhs: &Self) {
        self.bitset -= &rhs.bitset;
        self.has_rowid_sentinel &= !rhs.has_rowid_sentinel;
    }
}

impl alloc::TursoFromIterator<usize> for ColumnMask {
    fn try_from_iter<I: IntoIterator<Item = usize>>(
        iter: I,
    ) -> Result<Self, alloc::TryReserveError> {
        let mut mask = Self::default();
        mask.try_extend(iter)?;
        Ok(mask)
    }

    fn try_extend<I: IntoIterator<Item = usize>>(
        &mut self,
        iter: I,
    ) -> Result<(), alloc::TryReserveError> {
        for index in iter {
            self.set(index)?;
        }
        Ok(())
    }
}

pub struct ColumnMaskIter<B: std::borrow::Borrow<BitSet>> {
    inner: BitSetIter<usize, B>,
    pending_rowid: bool,
}

impl<B: std::borrow::Borrow<BitSet>> Iterator for ColumnMaskIter<B> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().or_else(|| {
            self.pending_rowid.then(|| {
                self.pending_rowid = false;
                ROWID_SENTINEL
            })
        })
    }
}

impl<'a> IntoIterator for &'a ColumnMask {
    type Item = usize;
    type IntoIter = ColumnMaskIter<&'a BitSet>;

    fn into_iter(self) -> Self::IntoIter {
        ColumnMaskIter {
            inner: (&self.bitset).into_iter(),
            pending_rowid: self.has_rowid_sentinel,
        }
    }
}

impl IntoIterator for ColumnMask {
    type Item = usize;
    type IntoIter = ColumnMaskIter<BitSet>;

    fn into_iter(self) -> Self::IntoIter {
        ColumnMaskIter {
            inner: self.bitset.into_iter(),
            pending_rowid: self.has_rowid_sentinel,
        }
    }
}

impl alloc::TryClone for ColumnMask {
    type Error = alloc::TryReserveError;

    fn try_clone(&self) -> Result<Self, Self::Error> {
        Ok(Self {
            bitset: self.bitset.try_clone()?,
            has_rowid_sentinel: self.has_rowid_sentinel,
        })
    }
}

/// Dense bitset with an inline word for the common case.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct BitSet<T = usize> {
    inline: u64,
    overflow: Option<alloc::Vec<u64>>,
    marker: PhantomData<fn() -> T>,
}

impl<T> Default for BitSet<T> {
    fn default() -> Self {
        Self {
            inline: 0,
            overflow: None,
            marker: PhantomData,
        }
    }
}

impl<T> alloc::TryClone for BitSet<T> {
    type Error = alloc::TryReserveError;

    fn try_clone(&self) -> Result<Self, Self::Error> {
        Ok(Self {
            inline: self.inline,
            overflow: self.overflow.try_clone()?,
            marker: PhantomData,
        })
    }
}

pub struct BitSetIter<T, B: std::borrow::Borrow<BitSet<T>>> {
    bitset: B,
    current: u64,
    word: usize,
    marker: PhantomData<fn() -> T>,
}

impl<T: From<usize>, B: std::borrow::Borrow<BitSet<T>>> Iterator for BitSetIter<T, B> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.current != 0 {
                let bit = self.current.trailing_zeros() as usize;
                self.current &= self.current - 1;
                let base = if self.word == 0 {
                    0
                } else {
                    BitSet::<T>::INLINE_BITS + (self.word - 1) * 64
                };
                return Some(T::from(base + bit));
            }
            self.word += 1;
            self.current = *self.bitset.borrow().overflow.as_ref()?.get(self.word - 1)?;
        }
    }
}

impl<'a, T: From<usize>> IntoIterator for &'a BitSet<T> {
    type Item = T;
    type IntoIter = BitSetIter<T, &'a BitSet<T>>;

    fn into_iter(self) -> Self::IntoIter {
        BitSetIter {
            bitset: self,
            current: self.inline,
            word: 0,
            marker: PhantomData,
        }
    }
}

impl<T: From<usize>> IntoIterator for BitSet<T> {
    type Item = T;
    type IntoIter = BitSetIter<T, BitSet<T>>;

    fn into_iter(self) -> Self::IntoIter {
        let current = self.inline;
        BitSetIter {
            bitset: self,
            current,
            word: 0,
            marker: PhantomData,
        }
    }
}

impl<T> BitSet<T> {
    const INLINE_BITS: usize = 64;
}

impl<T: From<usize>> BitSet<T>
where
    usize: From<T>,
{
    pub fn set(&mut self, index: T) -> Result<(), alloc::TryReserveError> {
        let index = usize::from(index);
        if index < Self::INLINE_BITS {
            self.inline |= 1 << index;
            return Ok(());
        }
        let overflow_index = (index - Self::INLINE_BITS) / 64;
        let bit = (index - Self::INLINE_BITS) % 64;
        let overflow = self.overflow.get_or_insert_with(|| alloc::vec![]);
        if overflow_index >= overflow.len() {
            overflow.try_reserve(overflow_index + 1 - overflow.len())?;
            overflow.resize(overflow_index + 1, 0);
        }
        overflow[overflow_index] |= 1 << bit;
        Ok(())
    }

    pub fn get(&self, index: T) -> bool {
        let index = usize::from(index);
        if index < Self::INLINE_BITS {
            return self.inline & (1 << index) != 0;
        }
        let overflow_index = (index - Self::INLINE_BITS) / 64;
        let bit = (index - Self::INLINE_BITS) % 64;
        self.overflow
            .as_ref()
            .and_then(|overflow| overflow.get(overflow_index))
            .is_some_and(|word| word & (1 << bit) != 0)
    }

    pub fn clear(&mut self, index: T) {
        let index = usize::from(index);
        if index < Self::INLINE_BITS {
            self.inline &= !(1 << index);
            return;
        }
        let overflow_index = (index - Self::INLINE_BITS) / 64;
        let bit = (index - Self::INLINE_BITS) % 64;
        if let Some(word) = self
            .overflow
            .as_mut()
            .and_then(|overflow| overflow.get_mut(overflow_index))
        {
            *word &= !(1 << bit);
        }
        self.trim_overflow();
    }

    pub fn contains_all_set_bits_of(&self, other: &Self) -> bool {
        if self.inline & other.inline != other.inline {
            return false;
        }
        match (&self.overflow, &other.overflow) {
            (_, None) => true,
            (None, Some(_)) => false,
            (Some(left), Some(right)) => {
                right.len() <= left.len()
                    && left
                        .iter()
                        .zip(right)
                        .all(|(left, right)| left & right == *right)
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.inline == 0 && self.overflow.is_none()
    }

    pub fn is_only(&self, index: T) -> bool {
        self.count() == 1 && self.get(index)
    }

    pub fn subtract(&mut self, other: &Self) {
        self.inline &= !other.inline;
        if let (Some(left), Some(right)) = (&mut self.overflow, &other.overflow) {
            for (left, right) in left.iter_mut().zip(right) {
                *left &= !right;
            }
        }
        self.trim_overflow();
    }

    pub fn union_with(&mut self, other: &Self) -> Result<(), alloc::TryReserveError> {
        self.inline |= other.inline;
        if let Some(right) = &other.overflow {
            let left = self.overflow.get_or_insert_with(|| alloc::vec![]);
            if left.len() < right.len() {
                left.try_reserve(right.len() - left.len())?;
                left.resize(right.len(), 0);
            }
            for (left, right) in left.iter_mut().zip(right) {
                *left |= right;
            }
        }
        Ok(())
    }

    pub fn iter(&self) -> BitSetIter<T, &Self> {
        self.into_iter()
    }

    pub fn count(&self) -> usize {
        self.inline.count_ones() as usize
            + self.overflow.as_ref().map_or(0, |overflow| {
                overflow.iter().map(|word| word.count_ones() as usize).sum()
            })
    }

    pub fn rank(&self, index: T) -> usize {
        let index = usize::from(index);
        self.iter()
            .map(usize::from)
            .take_while(|set_index| *set_index < index)
            .count()
    }

    pub(crate) fn intersects(&self, other: &Self) -> bool {
        self.inline & other.inline != 0
            || self
                .overflow
                .iter()
                .flatten()
                .zip(other.overflow.iter().flatten())
                .any(|(left, right)| left & right != 0)
    }

    fn trim_overflow(&mut self) {
        if let Some(overflow) = &mut self.overflow {
            while overflow.last() == Some(&0) {
                overflow.pop();
            }
            if overflow.is_empty() {
                self.overflow = None;
            }
        }
    }
}

impl<T: From<usize>> std::ops::SubAssign<&Self> for BitSet<T>
where
    usize: From<T>,
{
    fn sub_assign(&mut self, rhs: &Self) {
        self.subtract(rhs);
    }
}

impl<T: From<usize>> alloc::TursoFromIterator<T> for BitSet<T>
where
    usize: From<T>,
{
    fn try_from_iter<I: IntoIterator<Item = T>>(iter: I) -> Result<Self, alloc::TryReserveError> {
        let mut set = Self::default();
        set.try_extend(iter)?;
        Ok(set)
    }

    fn try_extend<I: IntoIterator<Item = T>>(
        &mut self,
        iter: I,
    ) -> Result<(), alloc::TryReserveError> {
        for index in iter {
            self.set(index)?;
        }
        Ok(())
    }
}

impl<T> TryFrom<u128> for BitSet<T> {
    type Error = alloc::TryReserveError;

    fn try_from(value: u128) -> Result<Self, Self::Error> {
        let high = (value >> 64) as u64;
        Ok(Self {
            inline: value as u64,
            overflow: (high != 0).then(|| alloc::vec![high]),
            marker: PhantomData,
        })
    }
}
