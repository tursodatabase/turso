use super::TursoNewExt;
use crate::alloc::Rc;

#[cfg(not(nightly))]
fn rc<T>(value: T) -> Rc<T> {
    Rc::new(value)
}

#[cfg(nightly)]
fn rc<T>(value: T) -> Rc<T> {
    Rc::new_in(value, crate::alloc::TursoAllocator)
}

#[cfg(not(nightly))]
impl<T> TursoNewExt<T> for Rc<T> {
    fn new(value: T) -> Self {
        rc(value)
    }
}

#[cfg(nightly)]
impl<T: Clone> super::TryClone for Rc<T> {
    type Error = crate::alloc::AllocError;

    fn try_clone(&self) -> Result<Self, Self::Error> {
        let alloc = Self::allocator(self).clone();
        Self::try_clone_from_ref_in(self, alloc)
    }
}

#[cfg(nightly)]
impl<T> TursoNewExt<T> for Rc<T> {
    fn new(value: T) -> Self {
        rc(value)
    }
}
