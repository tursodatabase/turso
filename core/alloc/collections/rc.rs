use super::{TryClone, TursoNewExt};
use crate::alloc::Rc;

fn rc<T>(value: T) -> Rc<T> {
    Rc::new(value)
}

impl<T> TursoNewExt<T> for Rc<T> {
    fn new(value: T) -> Self {
        rc(value)
    }
}

impl<T> TryClone for Rc<T> {
    type Error = crate::alloc::AllocError;

    fn try_clone(&self) -> Result<Self, Self::Error> {
        Ok(self.clone())
    }
}
