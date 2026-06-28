use super::{TursoAllocExt, TursoFromIterator, TursoTryWithCapacityExt, TursoVecDequeExt};
use crate::alloc::{TryReserveError, VecDeque};

const fn vec_deque<T>() -> VecDeque<T> {
    std::collections::VecDeque::new()
}

fn vec_deque_with_capacity<T>(capacity: usize) -> VecDeque<T> {
    std::collections::VecDeque::with_capacity(capacity)
}

impl<T> TursoAllocExt for VecDeque<T> {
    #[inline(always)]
    fn new() -> Self {
        vec_deque()
    }
}

impl<T> TursoVecDequeExt<T> for VecDeque<T> {
    #[inline(always)]
    fn try_push_back(&mut self, value: T) -> Result<(), TryReserveError> {
        self.push_back(value);
        Ok(())
    }

    #[inline(always)]
    fn try_push_front(&mut self, value: T) -> Result<(), TryReserveError> {
        self.push_front(value);
        Ok(())
    }
}

impl<T> TursoTryWithCapacityExt for VecDeque<T> {
    #[inline(always)]
    fn try_with_capacity_ext(capacity: usize) -> Result<Self, TryReserveError> {
        Ok(vec_deque_with_capacity(capacity))
    }
}

impl<T> TursoFromIterator<T> for VecDeque<T> {
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
