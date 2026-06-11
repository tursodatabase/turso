//! Turso-owned allocation namespace.
//!
//! Stable builds use `std` collections where allocator parameters are not
//! available. Builds compiled with `--cfg nightly` use Rust's unstable
//! `allocator_api` collection parameters.

use std::fmt;

mod api;
mod backend;
mod collections;

pub use api::{AllocError, Layout};
pub use backend::{set_allocator, SetAllocatorError, TursoAllocBackend};
pub use collections::{
    TryClone, TursoAllocExt, TursoBinaryHeapExt, TursoBoxExt, TursoFromIterator, TursoHashMapExt,
    TursoHashSetExt, TursoIteratorExt, TursoNewExt, TursoTryNewExt, TursoTryWithCapacityExt,
    TursoVecDequeExt, TursoVecExt,
};

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

#[derive(Clone, Copy, Debug, Default)]
pub struct TursoAllocator;

pub type Allocator = TursoAllocator;

pub type Box<T> = std::boxed::Box<T>;

#[cfg(not(nightly))]
pub type Vec<T> = std::vec::Vec<T>;
#[cfg(nightly)]
pub type Vec<T> = std::vec::Vec<T, TursoAllocator>;

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
            $(values.try_push($element)?;)+
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
