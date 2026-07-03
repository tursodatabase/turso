#[cfg(nightly)]
mod nightly;

#[cfg(not(nightly))]
use super::{
    TryClone, TursoAllocExt, TursoFromIterator, TursoSliceExt, TursoTryWithCapacityExt,
    TursoVecExt, TursoVecInExt,
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
impl<T, A> TursoVecInExt<T, A> for Vec<T> {
    #[inline(always)]
    fn new_in(_alloc: A) -> Self {
        vec()
    }

    #[inline(always)]
    fn with_capacity_in(capacity: usize, _alloc: A) -> Self {
        vec_with_capacity(capacity)
    }

    #[inline(always)]
    fn try_with_capacity_in(capacity: usize, alloc: A) -> Result<Self, TryReserveError> {
        Ok(<Self as TursoVecInExt<T, A>>::with_capacity_in(
            capacity, alloc,
        ))
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
impl<T: TryClone> TryClone for Vec<T>
where
    TryReserveError: From<T::Error>,
{
    type Error = TryReserveError;

    #[inline(always)]
    fn try_clone(&self) -> Result<Self, Self::Error> {
        // Same `TryClone` bound as the nightly impl so code compiles
        // identically on both cfgs. Elements clone through `TryClone`; on
        // stable this is Clone-forwarded for std-pinned types anyway.
        let mut cloned = <Self as TursoTryWithCapacityExt>::try_with_capacity_ext(self.len())?;
        for item in self {
            // Capacity reserved above; push cannot allocate.
            cloned.push(item.try_clone()?);
        }
        Ok(cloned)
    }
}
