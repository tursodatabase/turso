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
