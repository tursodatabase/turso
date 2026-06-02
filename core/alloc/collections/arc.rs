use super::TursoNewExt;
use crate::alloc::Arc;

fn arc<T>(value: T) -> Arc<T> {
    crate::sync::Arc::new(value)
}

impl<T> TursoNewExt<T> for Arc<T> {
    fn new(value: T) -> Self {
        arc(value)
    }
}

#[cfg(all(nightly, not(shuttle)))]
impl<T: Clone> super::TryClone for Arc<T> {
    type Error = crate::alloc::AllocError;

    fn try_clone(&self) -> Result<Self, Self::Error> {
        let alloc = Self::allocator(self).clone();
        Self::try_clone_from_ref_in(self, alloc)
    }
}
