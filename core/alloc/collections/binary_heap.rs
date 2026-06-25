use super::*;
use crate::alloc::*;

const fn binary_heap<T: Ord>() -> BinaryHeap<T> {
    BinaryHeap::new()
}

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
        Ok(BinaryHeap::with_capacity(capacity))
    }
}

impl<T: Ord> TursoFromIterator<T> for BinaryHeap<T> {
    #[inline(always)]
    fn try_from_iter<I>(iter: I) -> Result<Self, TryReserveError>
    where
        I: IntoIterator<Item = T>,
    {
        Ok(iter.into_iter().collect())
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
