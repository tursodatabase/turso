mod arc;
mod binary_heap;
mod boxed;
mod btree_map;
mod btree_set;
mod hash_map;
mod hash_set;
mod iterator;
mod linked_list;
mod rc;
mod traits;
mod vec;
mod vec_deque;

#[cfg(nightly)]
#[inline]
fn trusted_len(size_hint: (usize, Option<usize>)) -> Result<usize, super::TryReserveError> {
    let (lower, upper) = size_hint;
    let Some(additional) = upper else {
        return Err(super::TryReserveError);
    };
    debug_assert_eq!(lower, additional);
    Ok(additional)
}

pub use boxed::DynBoxedSlice;
pub(crate) use traits::impl_try_clone_via_clone;
#[cfg(nightly)]
pub use traits::TursoFromIteratorIn;
pub use traits::{
    TryClone, TursoAllocExt, TursoBinaryHeapExt, TursoBoxExt, TursoFromIterator, TursoHashMapExt,
    TursoHashSetExt, TursoIteratorExt, TursoNewExt, TursoSliceExt, TursoTryNewExt,
    TursoTryWithCapacityExt, TursoVecDequeExt, TursoVecExt, TursoVecInExt,
};
pub use vec::DynVec;
