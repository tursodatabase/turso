use crate::alloc::{TryReserveError, TursoAllocator};

pub type ArcSlice<T> = std::sync::Arc<[T], TursoAllocator>;

pub fn try_arc_slice_from_slice<T: Clone>(slice: &[T]) -> Result<ArcSlice<T>, TryReserveError> {
    std::sync::Arc::<[T], TursoAllocator>::try_clone_from_ref_in(slice, TursoAllocator)
        .map_err(|_| TryReserveError)
}
