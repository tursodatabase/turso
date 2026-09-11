use crate::alloc::TryReserveError;

pub type ArcSlice<T> = crate::sync::Arc<[T]>;

pub fn try_arc_slice_from_slice<T: Clone>(slice: &[T]) -> Result<ArcSlice<T>, TryReserveError> {
    Ok(crate::sync::Arc::from(slice))
}

pub fn try_arc_slice_from_slice_in<T: Clone, A>(
    slice: &[T],
    _alloc: A,
) -> Result<ArcSlice<T>, TryReserveError> {
    try_arc_slice_from_slice(slice)
}
