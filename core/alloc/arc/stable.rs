use crate::alloc::TryReserveError;

pub type ArcSlice<T> = crate::sync::Arc<[T]>;

pub fn try_arc_slice_from_slice<T: Clone>(slice: &[T]) -> Result<ArcSlice<T>, TryReserveError> {
    Ok(crate::sync::Arc::from(slice))
}
