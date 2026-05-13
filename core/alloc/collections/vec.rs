use super::{TryClone, TursoAllocExt, TursoFromIterator, TursoTryWithCapacityExt, TursoVecExt};
use crate::alloc::{TryReserveError, TursoAllocator, Vec};

fn vec<T>() -> Vec<T> {
    Vec::new_in(TursoAllocator)
}

fn vec_with_capacity<T>(capacity: usize) -> Vec<T> {
    Vec::with_capacity_in(capacity, TursoAllocator)
}

impl<T> TursoAllocExt for Vec<T> {
    fn new() -> Self {
        vec()
    }
}

impl<T> TursoVecExt<T> for Vec<T> {
    fn with_capacity(capacity: usize) -> Self {
        vec_with_capacity(capacity)
    }

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

impl<T> TursoTryWithCapacityExt for Vec<T> {
    fn try_with_capacity(capacity: usize) -> Result<Self, TryReserveError> {
        #[cfg(not(nightly))]
        {
            let mut values = vec();
            values
                .try_reserve(capacity)
                .map_err(TryReserveError::from)?;
            Ok(values)
        }
        #[cfg(nightly)]
        {
            std::vec::Vec::try_with_capacity_in(capacity, TursoAllocator)
                .map_err(TryReserveError::from)
        }
    }
}

impl<T> TursoFromIterator<T> for Vec<T> {
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

impl<T: Clone> TryClone for Vec<T> {
    type Error = TryReserveError;

    fn try_clone(&self) -> Result<Self, Self::Error> {
        #[cfg(not(nightly))]
        let mut cloned = <Self as TursoTryWithCapacityExt>::try_with_capacity(self.len())?;
        #[cfg(nightly)]
        let mut cloned = {
            let alloc = self.allocator().clone();
            Self::try_with_capacity_in(self.len(), alloc).map_err(TryReserveError::from)?
        };
        cloned.extend(self.iter().cloned());
        Ok(cloned)
    }
}
