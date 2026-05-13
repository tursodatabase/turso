use super::{
    TryClone, TursoAllocExt, TursoBinaryHeapExt, TursoFromIterator, TursoTryWithCapacityExt,
};
use crate::alloc::{BinaryHeap, TryReserveError};

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
        self.try_reserve(1).map_err(TryReserveError::from)?;
        self.push(value);
        Ok(())
    }

    fn try_extend<I>(&mut self, iter: I) -> Result<(), TryReserveError>
    where
        I: IntoIterator<Item = T>,
    {
        let iter = iter.into_iter();
        let (lower, upper) = iter.size_hint();
        self.try_reserve(upper.unwrap_or(lower))
            .map_err(TryReserveError::from)?;
        for value in iter {
            self.try_push(value)?;
        }
        Ok(())
    }
}

impl<T: Ord> TursoTryWithCapacityExt for BinaryHeap<T> {
    fn try_with_capacity(capacity: usize) -> Result<Self, TryReserveError> {
        let mut values = <Self as TursoAllocExt>::new();
        values
            .try_reserve(capacity)
            .map_err(TryReserveError::from)?;
        Ok(values)
    }
}

impl<T: Ord> TursoFromIterator<T> for BinaryHeap<T> {
    fn try_from_iter<I>(iter: I) -> Result<Self, TryReserveError>
    where
        I: IntoIterator<Item = T>,
    {
        let iter = iter.into_iter();
        let (lower, upper) = iter.size_hint();
        let capacity = upper.unwrap_or(lower);
        let mut values = <Self as TursoTryWithCapacityExt>::try_with_capacity(capacity)?;
        for value in iter {
            values.try_push(value)?;
        }
        Ok(values)
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
        cloned
            .try_reserve(self.len())
            .map_err(TryReserveError::from)?;
        cloned.extend(self.iter().cloned());
        Ok(cloned)
    }
}
