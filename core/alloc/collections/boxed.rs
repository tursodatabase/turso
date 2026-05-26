use super::{TryClone, TursoBoxExt, TursoNewExt, TursoTryNewExt};
#[cfg(nightly)]
use crate::alloc::TursoAllocator;
use crate::alloc::{AllocError, Box};

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
