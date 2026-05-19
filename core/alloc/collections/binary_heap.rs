use super::*;
use crate::alloc::*;

#[cfg(not(nightly))]
fn binary_heap<T: Ord>() -> BinaryHeap<T> {
    std::collections::BinaryHeap::new()
}

#[cfg(nightly)]
fn binary_heap<T: Ord>() -> BinaryHeap<T> {
    std::collections::BinaryHeap::new_in(crate::alloc::TursoAllocator)
}

#[cfg(not(nightly))]
impl<T: Ord> TursoAllocExt for BinaryHeap<T> {
    fn new() -> Self {
        binary_heap()
    }
}

#[cfg(nightly)]
impl<T: Ord> TursoAllocExt for BinaryHeap<T> {
    fn new() -> Self {
        binary_heap()
    }
}

impl<T: Ord> TursoBinaryHeapExt<T> for BinaryHeap<T> {
    fn try_push(&mut self, value: T) -> Result<(), TryReserveError> {
        self.try_reserve(1)?;
        self.push(value);
        Ok(())
    }
}

impl<T: Ord> TursoTryWithCapacityExt for BinaryHeap<T> {
    fn try_with_capacity(capacity: usize) -> Result<Self, TryReserveError> {
        let mut values = <Self as TursoAllocExt>::new();
        values.try_reserve(capacity)?;
        Ok(values)
    }
}

// For these 2 impls we use std::vec because on `stable` we can't use allocator on BinaryHeap, but on nightly we can
impl<T: Ord> TursoFromIterator<T> for BinaryHeap<T> {
    fn try_from_iter<I>(iter: I) -> Result<Self, TryReserveError>
    where
        I: IntoIterator<Item = T>,
    {
        let iter = iter.into_iter();
        let (lower, upper) = iter.size_hint();
        // We use std::vec here because on stable we dont have allocators for BinaryHeap. On Nightly, we create a Vec with the TursoAllocator
        #[cfg(nightly)]
        let mut values = super::vec::vec();
        #[cfg(not(nightly))]
        let mut values = std::vec::Vec::new();
        values.try_reserve(upper.unwrap_or(lower))?;
        if upper.is_some() {
            values.extend(iter);
        } else {
            for value in iter {
                values.try_reserve(1)?;
                values.push(value);
            }
        }
        Ok(BinaryHeap::from(values))
    }

    fn try_extend<I>(&mut self, iter: I) -> Result<(), TryReserveError>
    where
        I: IntoIterator<Item = T>,
    {
        let iter = iter.into_iter();
        let (lower, upper) = iter.size_hint();
        let mut values = std::mem::take(self).into_vec();
        values.try_reserve(upper.unwrap_or(lower))?;
        if upper.is_some() {
            values.extend(iter);
        } else {
            for value in iter {
                values.try_reserve(1)?;
                values.push(value);
            }
        }

        *self = BinaryHeap::from(values);
        Ok(())
    }
}

impl<T: Clone + Ord> TryClone for BinaryHeap<T> {
    type Error = TryReserveError;

    fn try_clone(&self) -> Result<Self, Self::Error> {
        #[cfg(not(nightly))]
        let mut cloned = <Self as TursoTryWithCapacityExt>::try_with_capacity(self.len())?;
        #[cfg(nightly)]
        let mut cloned = {
            let alloc = self.allocator().clone();
            Self::new_in(alloc)
        };
        cloned.try_reserve(self.len())?;
        cloned.extend(self.iter().cloned());
        Ok(cloned)
    }
}
