#[cfg(nightly)]
mod nightly;

#[cfg(not(nightly))]
use super::{
    TryClone, TursoAllocExt, TursoFromIterator, TursoSliceExt, TursoTryWithCapacityExt, TursoVecExt,
};
#[cfg(not(nightly))]
use crate::alloc::{TryReserveError, Vec};

#[cfg(not(nightly))]
pub(super) const fn vec<T>() -> Vec<T> {
    Vec::new()
}

#[cfg(not(nightly))]
fn vec_with_capacity<T>(capacity: usize) -> Vec<T> {
    Vec::with_capacity(capacity)
}

#[cfg(not(nightly))]
impl<T> TursoAllocExt for Vec<T> {
    #[inline(always)]
    fn new() -> Self {
        vec()
    }
}

#[cfg(not(nightly))]
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

#[cfg(not(nightly))]
impl<T> TursoTryWithCapacityExt for Vec<T> {
    #[inline(always)]
    fn try_with_capacity_ext(capacity: usize) -> Result<Self, TryReserveError> {
        Ok(vec_with_capacity(capacity))
    }
}

#[cfg(not(nightly))]
impl<T> TursoFromIterator<T> for Vec<T> {
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

#[cfg(not(nightly))]
impl<T: Clone> TursoSliceExt<T> for [T] {
    #[inline(always)]
    fn try_to_vec(&self) -> Result<Vec<T>, TryReserveError> {
        let mut values = <Vec<T> as TursoTryWithCapacityExt>::try_with_capacity_ext(self.len())?;
        values.extend_from_slice(self);
        Ok(values)
    }
}

#[cfg(not(nightly))]
impl<T: Clone> TryClone for Vec<T> {
    type Error = TryReserveError;

    #[inline(always)]
    fn try_clone(&self) -> Result<Self, Self::Error> {
        let mut cloned = <Self as TursoTryWithCapacityExt>::try_with_capacity_ext(self.len())?;
        cloned.extend(self.iter().cloned());
        Ok(cloned)
    }
}
