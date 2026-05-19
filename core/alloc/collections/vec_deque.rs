use super::{
    TryClone, TursoAllocExt, TursoFromIterator, TursoTryWithCapacityExt, TursoVecDequeExt,
};
use crate::alloc::{TryReserveError, VecDeque};

#[cfg(not(nightly))]
fn vec_deque<T>() -> VecDeque<T> {
    std::collections::VecDeque::new()
}

#[cfg(nightly)]
fn vec_deque<T>() -> VecDeque<T> {
    std::collections::VecDeque::new_in(crate::alloc::TursoAllocator)
}

#[cfg(not(nightly))]
impl<T> TursoAllocExt for VecDeque<T> {
    fn new() -> Self {
        vec_deque()
    }
}

#[cfg(nightly)]
impl<T> TursoAllocExt for VecDeque<T> {
    fn new() -> Self {
        vec_deque()
    }
}

impl<T> TursoVecDequeExt<T> for VecDeque<T> {
    fn try_push_back(&mut self, value: T) -> Result<(), TryReserveError> {
        self.try_reserve(1)?;
        self.push_back(value);
        Ok(())
    }

    fn try_push_front(&mut self, value: T) -> Result<(), TryReserveError> {
        self.try_reserve(1)?;
        self.push_front(value);
        Ok(())
    }
}

impl<T> TursoTryWithCapacityExt for VecDeque<T> {
    fn try_with_capacity(capacity: usize) -> Result<Self, TryReserveError> {
        let mut values = <Self as TursoAllocExt>::new();
        values.try_reserve(capacity)?;
        Ok(values)
    }
}

impl<T> TursoFromIterator<T> for VecDeque<T> {
    fn try_from_iter<I>(iter: I) -> Result<Self, TryReserveError>
    where
        I: IntoIterator<Item = T>,
    {
        let iter = iter.into_iter();
        let (lower, upper) = iter.size_hint();
        let capacity = upper.unwrap_or(lower);
        let mut values = <Self as TursoTryWithCapacityExt>::try_with_capacity(capacity)?;
        if upper.is_some() {
            values.extend(iter);
        } else {
            for value in iter {
                values.try_push_back(value)?;
            }
        }
        Ok(values)
    }

    fn try_extend<I>(&mut self, iter: I) -> Result<(), TryReserveError>
    where
        I: IntoIterator<Item = T>,
    {
        let iter = iter.into_iter();
        let (lower, upper) = iter.size_hint();
        self.try_reserve(upper.unwrap_or(lower))?;
        if upper.is_some() {
            self.extend(iter);
        } else {
            for value in iter {
                self.try_push_back(value)?;
            }
        }
        Ok(())
    }
}

impl<T: Clone> TryClone for VecDeque<T> {
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
