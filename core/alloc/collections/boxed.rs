use super::{TryClone, TursoBoxExt, TursoFromIterator, TursoNewExt, TursoTryNewExt};
use crate::alloc::{AllocError, Box, TryReserveError};

#[cfg(not(nightly))]
pub type DynBoxedSlice<T> = std::boxed::Box<[T]>;
#[cfg(nightly)]
pub type DynBoxedSlice<T> = std::boxed::Box<[T], crate::alloc::DynAllocator>;

/// A box whose memory comes from a [`DynAllocator`](crate::alloc::DynAllocator).
///
/// Unlike the other `Dyn` collections, it uses its allocator also in stable
/// builds, where it is the `Box` of `allocator_api2`. Its `try_new_in`
/// returns [`AllocError`] when the allocator cannot give the memory.
#[cfg(not(nightly))]
pub type DynBox<T> = allocator_api2::boxed::Box<T, crate::alloc::DynAllocator>;
#[cfg(nightly)]
pub type DynBox<T> = std::boxed::Box<T, crate::alloc::DynAllocator>;

#[cfg(not(nightly))]
impl<T> TursoBoxExt<T> for DynBox<T> {
    fn into_inner(self) -> T {
        allocator_api2::boxed::Box::into_inner(self)
    }
}

#[cfg(nightly)]
impl<T> TursoBoxExt<T> for DynBox<T> {
    fn into_inner(self) -> T {
        *self
    }
}

fn boxed<T>(value: T) -> Box<T> {
    Box::new(value)
}

fn try_boxed<T>(value: T) -> Result<Box<T>, AllocError> {
    Ok(Box::new(value))
}

fn collect_boxed_slice<T, I>(iter: I) -> Box<[T]>
where
    I: IntoIterator<Item = T>,
{
    iter.into_iter()
        .collect::<std::vec::Vec<_>>()
        .into_boxed_slice()
}

fn empty_boxed_slice<T>() -> Box<[T]> {
    std::vec::Vec::new().into_boxed_slice()
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
        *self
    }
}

impl<T: Clone> TryClone for Box<T> {
    type Error = AllocError;

    fn try_clone(&self) -> Result<Self, Self::Error> {
        <Self as TursoTryNewExt<T>>::try_new((**self).clone())
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
