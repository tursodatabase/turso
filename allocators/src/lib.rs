//! Turso-owned allocation namespace.
//!
//! Stable builds use `std` collections where allocator parameters are not
//! available. Builds compiled with `--cfg nightly` use Rust's unstable
//! `allocator_api` collection parameters.
#![cfg_attr(
    nightly,
    feature(allocator_api, btreemap_alloc, clone_from_ref, try_with_capacity)
)]

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

// TODO: change this to use allocator_api2 Box when we finish migrating callsites
#[cfg(not(nightly))]
pub type Box<T> = std::boxed::Box<T>;
#[cfg(nightly)]
pub type Box<T> = std::boxed::Box<T, TursoAllocator>;

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
        <$crate::Vec<_> as $crate::TursoAllocExt>::new()
    };
    ($element:expr; $count:expr) => {{
        let count = $count;
        let mut values =
            <$crate::Vec<_> as $crate::TursoVecExt<_>>::with_capacity(count);
        values.resize(count, $element);
        values
    }};
    ($($element:expr),+ $(,)?) => {{
        let mut values =
            <$crate::Vec<_> as $crate::TursoVecExt<_>>::with_capacity(
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
        Ok::<_, $crate::TryReserveError>(
            <$crate::Vec<_> as $crate::TursoAllocExt>::new(),
        )
    };
    ($element:expr; $count:expr) => {{
        (|| {
            let count = $count;
            let mut values =
                <$crate::Vec<_> as $crate::TursoTryWithCapacityExt>::try_with_capacity_ext(
                    count,
                )?;
            values.resize(count, $element);
            Ok::<_, $crate::TryReserveError>(values)
        })()
    }};
    ($($element:expr),+ $(,)?) => {{
        (|| {
            let mut values =
                <$crate::Vec<_> as $crate::TursoTryWithCapacityExt>::try_with_capacity_ext(
                    $crate::__turso_alloc_vec_count!($($element),+),
                )?;
            $(values.try_push($element)?;)+
            Ok::<_, $crate::TryReserveError>(values)
        })()
    }};
}

pub type String = std::string::String;

pub type HashMap<K, V, S = rustc_hash::FxBuildHasher> = std::collections::HashMap<K, V, S>;

pub type HashSet<T, S = rustc_hash::FxBuildHasher> = std::collections::HashSet<T, S>;

#[cfg(not(nightly))]
pub type BTreeMap<K, V> = std::collections::BTreeMap<K, V>;
#[cfg(nightly)]
pub type BTreeMap<K, V> = std::collections::BTreeMap<K, V, TursoAllocator>;

#[cfg(not(nightly))]
pub type BTreeSet<T> = std::collections::BTreeSet<T>;
#[cfg(nightly)]
pub type BTreeSet<T> = std::collections::BTreeSet<T, TursoAllocator>;

#[cfg(not(nightly))]
pub type VecDeque<T> = std::collections::VecDeque<T>;
#[cfg(nightly)]
pub type VecDeque<T> = std::collections::VecDeque<T, TursoAllocator>;

#[cfg(not(nightly))]
pub type BinaryHeap<T> = std::collections::BinaryHeap<T>;
#[cfg(nightly)]
pub type BinaryHeap<T> = std::collections::BinaryHeap<T, TursoAllocator>;

#[cfg(not(nightly))]
pub type LinkedList<T> = std::collections::LinkedList<T>;
#[cfg(nightly)]
pub type LinkedList<T> = std::collections::LinkedList<T, TursoAllocator>;

// TODO: design allocator-aware shared-pointer support that still preserves
// shuttle's deterministic sync behavior.
//
// These aliases must stay in sync with `turso_core::sync` so that
// `crate::alloc::Arc` and `crate::sync::Arc` remain the same type there.
#[cfg(shuttle)]
pub type Arc<T> = shuttle::sync::Arc<T>;
#[cfg(shuttle)]
pub type Weak<T> = shuttle::sync::Weak<T>;
#[cfg(not(shuttle))]
pub type Arc<T> = std::sync::Arc<T>;
#[cfg(not(shuttle))]
pub type Weak<T> = std::sync::Weak<T>;

#[cfg(not(nightly))]
pub type Rc<T> = std::rc::Rc<T>;
#[cfg(nightly)]
pub type Rc<T> = std::rc::Rc<T, TursoAllocator>;

#[cfg(not(nightly))]
pub type RcWeak<T> = std::rc::Weak<T>;
#[cfg(nightly)]
pub type RcWeak<T> = std::rc::Weak<T, TursoAllocator>;

#[cfg(test)]
mod tests;
