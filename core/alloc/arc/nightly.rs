use crate::alloc::{ConcurrentAllocator, DynAllocator, TryReserveError};

pub type ArcSlice<T, A = DynAllocator> = std::sync::Arc<[T], A>;

pub fn try_arc_slice_from_slice<T: Clone>(slice: &[T]) -> Result<ArcSlice<T>, TryReserveError> {
    try_arc_slice_from_slice_in(slice, DynAllocator::default())
}

pub fn try_dyn_arc_slice_from_slice_in<T: Clone, A: ConcurrentAllocator>(
    slice: &[T],
    alloc: A,
) -> Result<ArcSlice<T>, TryReserveError> {
    try_arc_slice_from_slice_in(slice, DynAllocator::new(alloc))
}

pub fn try_arc_slice_from_slice_in<T: Clone, A: ConcurrentAllocator>(
    slice: &[T],
    alloc: A,
) -> Result<ArcSlice<T, A>, TryReserveError> {
    std::sync::Arc::<[T], A>::try_clone_from_ref_in(slice, alloc).map_err(|_| TryReserveError)
}

#[derive(Clone, Debug)]
pub struct SharedBytes<A: ConcurrentAllocator = DynAllocator>(std::sync::Arc<Vec<u8, A>, A>);

impl<A: ConcurrentAllocator> SharedBytes<A> {
    pub fn capacity(&self) -> usize {
        self.0.capacity()
    }

    pub fn try_from_slice_in(bytes: &[u8], allocator: A) -> Result<Self, TryReserveError> {
        let mut buffer = Vec::try_with_capacity_in(bytes.len(), allocator)?;
        buffer.extend_from_slice(bytes);
        Self::try_from_vec(buffer)
    }

    pub fn try_from_vec(buffer: Vec<u8, A>) -> Result<Self, TryReserveError> {
        let allocator = buffer.allocator().clone();
        std::sync::Arc::try_new_in(buffer, allocator)
            .map(Self)
            .map_err(|_| TryReserveError)
    }
}

impl<A: ConcurrentAllocator> std::ops::Deref for SharedBytes<A> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.0.as_slice()
    }
}
