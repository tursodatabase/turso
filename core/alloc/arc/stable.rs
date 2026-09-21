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

#[derive(Clone, Debug)]
pub struct SharedBytes(crate::sync::Arc<[u8]>);

impl SharedBytes {
    pub fn capacity(&self) -> usize {
        self.0.len()
    }

    pub fn try_from_slice_in<A>(bytes: &[u8], _allocator: A) -> Result<Self, TryReserveError> {
        Ok(Self(crate::sync::Arc::from(bytes)))
    }

    pub fn try_from_vec(buffer: Vec<u8>) -> Result<Self, TryReserveError> {
        Ok(Self(crate::sync::Arc::from(buffer)))
    }
}

impl std::ops::Deref for SharedBytes {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.0
    }
}
