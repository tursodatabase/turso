use super::*;
use crate::alloc::*;

#[cfg(not(nightly))]
const fn binary_heap<T: Ord>() -> BinaryHeap<T> {
    BinaryHeap::new()
}

#[cfg(nightly)]
const fn binary_heap<T: Ord>() -> BinaryHeap<T> {
    BinaryHeap::new_in(crate::alloc::TursoAllocator)
}

#[cfg(not(nightly))]
impl<T: Ord> TursoAllocExt for BinaryHeap<T> {
    #[inline(always)]
    fn new() -> Self {
        binary_heap()
    }
}

#[cfg(nightly)]
impl<T: Ord> TursoAllocExt for BinaryHeap<T> {
    #[inline(always)]
    fn new() -> Self {
        binary_heap()
    }
}

impl<T: Ord> TursoBinaryHeapExt<T> for BinaryHeap<T> {
    #[inline(always)]
    fn try_push(&mut self, value: T) -> Result<(), TryReserveError> {
        self.push(value);
        Ok(())
    }
}

impl<T: Ord> TursoTryWithCapacityExt for BinaryHeap<T> {
    #[inline(always)]
    fn try_with_capacity_ext(capacity: usize) -> Result<Self, TryReserveError> {
        #[cfg(not(nightly))]
        {
            Ok(BinaryHeap::with_capacity(capacity))
        }
        #[cfg(nightly)]
        {
            Ok(BinaryHeap::with_capacity_in(
                capacity,
                crate::alloc::TursoAllocator,
            ))
        }
    }
}

// For these 2 impls we use std::vec because on `stable` we can't use allocator on BinaryHeap, but on nightly we can
impl<T: Ord> TursoFromIterator<T> for BinaryHeap<T> {
    #[inline(always)]
    fn try_from_iter<I>(iter: I) -> Result<Self, TryReserveError>
    where
        I: IntoIterator<Item = T>,
    {
        #[cfg(not(nightly))]
        {
            Ok(iter.into_iter().collect())
        }
        #[cfg(nightly)]
        {
            // We use std::vec here because on stable we dont have allocators for BinaryHeap. On Nightly, we create a Vec with the TursoAllocator
            let mut values = super::vec::vec();
            values.extend(iter);
            Ok(BinaryHeap::from(values))
        }
    }

    #[inline(always)]
    fn try_extend<I>(&mut self, iter: I) -> Result<(), TryReserveError>
    where
        I: IntoIterator<Item = T>,
    {
        self.extend(iter);
        Ok(())
    }
}

impl<T: Clone + Ord> TryClone for BinaryHeap<T> {
    type Error = TryReserveError;

    #[inline(always)]
    fn try_clone(&self) -> Result<Self, Self::Error> {
        #[cfg(not(nightly))]
        let mut cloned = <Self as TursoTryWithCapacityExt>::try_with_capacity_ext(self.len())?;
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
