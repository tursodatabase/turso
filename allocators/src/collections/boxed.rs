use super::{TryClone, TursoBoxExt, TursoFromIterator, TursoNewExt, TursoTryNewExt};
#[cfg(nightly)]
use crate::TursoAllocator;
use crate::{AllocError, Box, TryReserveError, Vec};

#[cfg(not(nightly))]
fn boxed<T>(value: T) -> Box<T> {
    Box::new(value)
}

#[cfg(nightly)]
fn boxed<T>(value: T) -> Box<T> {
    Box::new_in(value, TursoAllocator)
}

#[cfg(not(nightly))]
fn try_boxed<T>(value: T) -> Result<Box<T>, AllocError> {
    Ok(Box::new(value))
}

#[cfg(nightly)]
fn try_boxed<T>(value: T) -> Result<Box<T>, AllocError> {
    Box::try_new_in(value, TursoAllocator)
}

#[cfg(not(nightly))]
fn collect_boxed_slice<T, I>(iter: I) -> Box<[T]>
where
    I: IntoIterator<Item = T>,
{
    iter.into_iter().collect::<Vec<_>>().into_boxed_slice()
}

#[cfg(nightly)]
fn collect_boxed_slice<T, I>(iter: I) -> Box<[T]>
where
    I: IntoIterator<Item = T>,
{
    let mut values = Vec::new_in(TursoAllocator);
    values.extend(iter);
    values.into_boxed_slice()
}

#[cfg(not(nightly))]
fn empty_boxed_slice<T>() -> Box<[T]> {
    Vec::new().into_boxed_slice()
}

#[cfg(nightly)]
fn empty_boxed_slice<T>() -> Box<[T]> {
    Vec::new_in(TursoAllocator).into_boxed_slice()
}

impl<T> TursoNewExt<T> for Box<T> {
    fn new(value: T) -> Self {
        boxed(value)
    }
}

impl<T> TursoTryNewExt<T> for Box<T> {
    fn try_new(value: T) -> Result<Self, AllocError> {
        try_boxed(value)
    }
}

impl<T> TursoBoxExt<T> for Box<T> {
    fn into_inner(self) -> T {
        #[cfg(not(nightly))]
        {
            *self
        }
        #[cfg(nightly)]
        {
            *self
        }
    }
}

impl<T: Clone> TryClone for Box<T> {
    type Error = AllocError;

    fn try_clone(&self) -> Result<Self, Self::Error> {
        #[cfg(not(nightly))]
        {
            <Self as TursoTryNewExt<T>>::try_new((**self).clone())
        }
        #[cfg(nightly)]
        {
            let alloc = Self::allocator(self).clone();
            Self::try_clone_from_ref_in(self, alloc)
        }
    }
}

impl<T> TursoFromIterator<T> for Box<[T]> {
    #[inline(always)]
    fn try_from_iter<I>(iter: I) -> Result<Self, TryReserveError>
    where
        I: IntoIterator<Item = T>,
    {
        Ok(collect_boxed_slice(iter))
    }

    #[inline(always)]
    fn try_extend<I>(&mut self, iter: I) -> Result<(), TryReserveError>
    where
        I: IntoIterator<Item = T>,
    {
        let mut values = std::mem::replace(self, empty_boxed_slice()).into_vec();
        values.extend(iter);
        *self = values.into_boxed_slice();
        Ok(())
    }
}
