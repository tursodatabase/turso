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
impl<T> TursoNewExt<T> for Rc<T> {
    fn new(value: T) -> Self {
        rc(value)
    }
}
