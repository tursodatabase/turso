use super::{TryClone, TursoAllocExt, TursoFromIterator, TursoTryWithCapacityExt, TursoVecExt};
#[cfg(nightly)]
use crate::alloc::TursoAllocator;
use crate::alloc::{TryReserveError, Vec};

#[cfg(not(nightly))]
pub(super) const fn vec<T>() -> Vec<T> {
    Vec::new()
}

#[cfg(nightly)]
pub(super) const fn vec<T>() -> Vec<T> {
    Vec::new_in(TursoAllocator)
}

#[cfg(not(nightly))]
fn vec_with_capacity<T>(capacity: usize) -> Vec<T> {
    Vec::with_capacity(capacity)
}

#[cfg(nightly)]
fn vec_with_capacity<T>(capacity: usize) -> Vec<T> {
    Vec::with_capacity_in(capacity, TursoAllocator)
}

impl<T> TursoAllocExt for Vec<T> {
    #[inline(always)]
    fn new() -> Self {
        vec()
    }
}

impl<T> TursoVecExt<T> for Vec<T> {
    #[inline(always)]
    fn with_capacity(capacity: usize) -> Self {
        vec_with_capacity(capacity)
    }

    #[inline(always)]
    fn try_push(&mut self, value: T) -> Result<(), TryReserveError> {
        self.push(value);
        Ok(())
    }
}

impl<T> TursoTryWithCapacityExt for Vec<T> {
    #[inline(always)]
    fn try_with_capacity_ext(capacity: usize) -> Result<Self, TryReserveError> {
        Ok(vec_with_capacity(capacity))
    }
}

impl<T> TursoFromIterator<T> for Vec<T> {
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
            let mut values = vec();
            values.extend(iter);
            Ok(values)
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

impl<T: Clone> TryClone for Vec<T> {
    type Error = TryReserveError;

    #[inline(always)]
    fn try_clone(&self) -> Result<Self, Self::Error> {
        #[cfg(not(nightly))]
        let mut cloned = <Self as TursoTryWithCapacityExt>::try_with_capacity_ext(self.len())?;
        #[cfg(nightly)]
        let mut cloned = {
            let alloc = self.allocator().clone();
            Self::try_with_capacity_in(self.len(), alloc).map_err(TryReserveError::from)?
        };
        cloned.extend(self.iter().cloned());
        Ok(cloned)
    }
}
