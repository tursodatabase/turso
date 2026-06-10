use super::{
    TryClone, TursoAllocExt, TursoFromIterator, TursoTryWithCapacityExt, TursoVecDequeExt,
};
use crate::{TryReserveError, VecDeque};

#[cfg(not(nightly))]
const fn vec_deque<T>() -> VecDeque<T> {
    std::collections::VecDeque::new()
}

#[cfg(nightly)]
const fn vec_deque<T>() -> VecDeque<T> {
    std::collections::VecDeque::new_in(crate::TursoAllocator)
}

#[cfg(not(nightly))]
fn vec_deque_with_capacity<T>(capacity: usize) -> VecDeque<T> {
    std::collections::VecDeque::with_capacity(capacity)
}

#[cfg(nightly)]
fn vec_deque_with_capacity<T>(capacity: usize) -> VecDeque<T> {
    std::collections::VecDeque::with_capacity_in(capacity, crate::TursoAllocator)
}

#[cfg(not(nightly))]
impl<T> TursoAllocExt for VecDeque<T> {
    #[inline(always)]
    fn new() -> Self {
        vec_deque()
    }
}

#[cfg(nightly)]
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
        #[cfg(not(nightly))]
        {
            Ok(iter.into_iter().collect())
        }
        #[cfg(nightly)]
        {
            let mut values = super::vec::vec();
            values.extend(iter);
            Ok(Self::from(values))
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

impl<T: Clone> TryClone for VecDeque<T> {
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
