//! Turso-owned allocation namespace.
//!
//! Stable builds use `std` collections where allocator parameters are not
//! available. Builds compiled with `--cfg nightly` use Rust's unstable
//! `allocator_api` collection parameters.

use std::{fmt, ptr::NonNull};

mod allocation_site;
mod api;
mod arc;
mod backend;
mod collections;

pub use allocation_site::{
    current_allocation_site, enter_allocation_site, AllocationSite, AllocationSiteGuard,
    MvStoreAllocationSite, MvccCheckpointAllocationSite, SchemaAllocationSite,
    ValueBlobAllocationSite, VectorAllocationSite,
};
/// The underlying allocator trait: `allocator_api2::alloc::Allocator` on
/// stable, `std::alloc::Allocator` on `--cfg nightly` builds.
pub use api::ApiAllocator;
pub use api::{AllocError, Global, Layout};
pub use arc::{try_arc_slice_from_slice, try_arc_slice_from_slice_in, ArcSlice};
pub use backend::{set_allocator, SetAllocatorError, TursoAllocBackend};
pub(crate) use collections::impl_try_clone_via_clone;
#[cfg(nightly)]
pub use collections::TursoFromIteratorIn;
pub use collections::{
    DynBoxedSlice, DynVec, TryClone, TursoAllocExt, TursoBinaryHeapExt, TursoBoxExt,
    TursoFromIterator, TursoHashMapExt, TursoHashSetExt, TursoIteratorExt, TursoNewExt,
    TursoSliceExt, TursoTryNewExt, TursoTryWithCapacityExt, TursoVecDequeExt, TursoVecExt,
    TursoVecInExt,
};

pub const ALLOC_ERR_MSG: &str = "fallible allocations";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TryReserveError;

impl fmt::Display for TryReserveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("memory allocation failed")
    }
}

impl std::error::Error for TryReserveError {}

#[cfg(not(nightly))]
impl From<api::TryReserveError> for TryReserveError {
    fn from(_: api::TryReserveError) -> Self {
        Self
    }
}

impl From<std::collections::TryReserveError> for TryReserveError {
    fn from(_: std::collections::TryReserveError) -> Self {
        Self
    }
}

/// Lets containers whose elements clone infallibly (`TryClone<Error =
/// Infallible>`) satisfy `TryReserveError: From<T::Error>` bounds.
impl From<std::convert::Infallible> for TryReserveError {
    fn from(never: std::convert::Infallible) -> Self {
        match never {}
    }
}

/// Allocator safe to clone into concurrent data structures and deferred drops.
///
/// Cloning must be cheap and must not panic. The `'static` bound allows
/// deferred reclamation to outlive the container that captured the allocator.
pub trait ConcurrentAllocator: ApiAllocator + Clone + Send + Sync + 'static {}

impl<A: ApiAllocator + Clone + Send + Sync + 'static> ConcurrentAllocator for A {}

#[derive(Clone, Copy, Debug, Default)]
pub struct TursoAllocator;

pub type Allocator = TursoAllocator;

#[derive(Clone)]
pub struct DynAllocator {
    inner: Arc<dyn ApiAllocator + Send + Sync>,
}

impl DynAllocator {
    pub fn new<A>(alloc: A) -> Self
    where
        A: ApiAllocator + Send + Sync + 'static,
    {
        Self {
            inner: Arc::new(alloc),
        }
    }
}

impl Default for DynAllocator {
    fn default() -> Self {
        Self::new(TursoAllocator)
    }
}

impl fmt::Debug for DynAllocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DynAllocator").finish_non_exhaustive()
    }
}

unsafe impl ApiAllocator for DynAllocator {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        self.inner.allocate(layout)
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        unsafe {
            self.inner.deallocate(ptr, layout);
        }
    }
}

pub type Box<T> = std::boxed::Box<T>;

/// Boxed slice that keeps the allocator parameter on nightly.
///
/// `Box<T>` stays allocator-free so `Box::new` keeps working, but
/// `Vec::into_boxed_slice` on an allocator-aware `Vec` produces
/// `Box<[T], TursoAllocator>` on nightly — fields holding such slices must
/// use this alias instead of `Box<[T]>`.
#[cfg(not(nightly))]
pub type BoxedSlice<T> = std::boxed::Box<[T]>;
#[cfg(nightly)]
pub type BoxedSlice<T> = std::boxed::Box<[T], TursoAllocator>;

#[cfg(not(nightly))]
pub type Vec<T> = std::vec::Vec<T>;
#[cfg(nightly)]
pub type Vec<T, A = TursoAllocator> = std::vec::Vec<T, A>;

pub use crate::{__turso_alloc_try_vec as try_vec, __turso_alloc_vec as vec};

#[doc(hidden)]
#[macro_export]
macro_rules! __turso_alloc_vec_count {
    ($($element:expr),*) => {
        <[()]>::len(&[$($crate::__turso_alloc_vec_count!(@sub $element)),*])
    };
    (@sub $element:expr) => {
        ()
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __turso_alloc_vec {
    () => {
        <$crate::alloc::Vec<_> as $crate::alloc::TursoAllocExt>::new()
    };
    ($element:expr; $count:expr) => {{
        let count = $count;
        let mut values =
            <$crate::alloc::Vec<_> as $crate::alloc::TursoVecExt<_>>::with_capacity(count);
        values.resize(count, $element);
        values
    }};
    ($($element:expr),+ $(,)?) => {{
        let mut values =
            <$crate::alloc::Vec<_> as $crate::alloc::TursoVecExt<_>>::with_capacity(
                $crate::__turso_alloc_vec_count!($($element),+),
            );
        $(values.push($element);)+
        values
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __turso_alloc_try_vec {
    () => {
        Ok::<_, $crate::alloc::TryReserveError>(
            <$crate::alloc::Vec<_> as $crate::alloc::TursoAllocExt>::new(),
        )
    };
    ($element:expr; $count:expr; $alloc:expr) => {{
        (|| {
            let count = $count;
            let alloc = $alloc;
            #[cfg(not(nightly))]
            let mut values =
                <$crate::alloc::Vec<_> as $crate::alloc::TursoVecInExt<_, _>>::try_with_capacity_in(
                    count, alloc,
                )?;
            #[cfg(nightly)]
            let mut values = <$crate::alloc::Vec<_, _> as $crate::alloc::TursoVecInExt<
                _,
                _,
            >>::try_with_capacity_in(count, alloc)?;
            values.resize(count, $element);
            Ok::<_, $crate::alloc::TryReserveError>(values)
        })()
    }};
    ($element:expr; $count:expr) => {{
        (|| {
            let count = $count;
            let mut values =
                <$crate::alloc::Vec<_> as $crate::alloc::TursoTryWithCapacityExt>::try_with_capacity_ext(
                    count,
                )?;
            values.resize(count, $element);
            Ok::<_, $crate::alloc::TryReserveError>(values)
        })()
    }};
    ($($element:expr),+ $(,)?) => {{
        (|| {
            let mut values =
                <$crate::alloc::Vec<_> as $crate::alloc::TursoTryWithCapacityExt>::try_with_capacity_ext(
                    $crate::__turso_alloc_vec_count!($($element),+),
                )?;
            $(values.push($element);)+
            Ok::<_, $crate::alloc::TryReserveError>(values)
        })()
    }};
}

pub type String = std::string::String;

pub type HashMap<K, V, S = rustc_hash::FxBuildHasher> = std::collections::HashMap<K, V, S>;

pub type HashSet<T, S = rustc_hash::FxBuildHasher> = std::collections::HashSet<T, S>;

pub type BTreeMap<K, V> = std::collections::BTreeMap<K, V>;

pub type BTreeSet<T> = std::collections::BTreeSet<T>;

pub type VecDeque<T> = std::collections::VecDeque<T>;

pub type BinaryHeap<T> = std::collections::BinaryHeap<T>;

pub type LinkedList<T> = std::collections::LinkedList<T>;

// TODO: design allocator-aware shared-pointer support that still preserves
// shuttle's deterministic sync behavior.
pub type Arc<T> = crate::sync::Arc<T>;
pub type Weak<T> = crate::sync::Weak<T>;

pub type Rc<T> = std::rc::Rc<T>;

pub type RcWeak<T> = std::rc::Weak<T>;

#[cfg(test)]
mod tests;
