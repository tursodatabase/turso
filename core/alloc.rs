//! Turso-owned allocation namespace.
//!
//! Stable builds use `allocator-api2` where it has allocator-aware types and
//! fall back to `std` for collections that do not have stable allocator-aware
//! equivalents. Builds compiled with `--cfg nightly` use Rust's unstable
//! `allocator_api` collection parameters.

use std::{
    fmt,
    hash::{BuildHasher, Hash},
    ptr::NonNull,
    sync::OnceLock,
};

#[cfg(not(nightly))]
mod api {
    pub use allocator_api2::{
        alloc::{AllocError, Allocator as ApiAllocator, Global, Layout},
        collections::TryReserveError,
    };
}

#[cfg(nightly)]
mod api {
    pub use std::alloc::{AllocError, Allocator as ApiAllocator, Global, Layout};
}

pub use api::{AllocError, Layout};

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

#[cfg(not(nightly))]
pub type Box<T> = allocator_api2::boxed::Box<T, TursoAllocator>;
#[cfg(nightly)]
pub type Box<T> = std::boxed::Box<T, TursoAllocator>;

#[cfg(not(nightly))]
pub type Vec<T> = allocator_api2::vec::Vec<T, TursoAllocator>;
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
                <$crate::alloc::Vec<_> as $crate::alloc::TursoTryWithCapacityExt>::try_with_capacity(
                    count,
                )?;
            values.resize(count, $element);
            Ok::<_, $crate::alloc::TryReserveError>(values)
        })()
    }};
    ($($element:expr),+ $(,)?) => {{
        (|| {
            let mut values =
                <$crate::alloc::Vec<_> as $crate::alloc::TursoTryWithCapacityExt>::try_with_capacity(
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
pub type Arc<T> = crate::sync::Arc<T>;
pub type Weak<T> = crate::sync::Weak<T>;

#[cfg(not(nightly))]
pub type Rc<T> = std::rc::Rc<T>;
#[cfg(nightly)]
pub type Rc<T> = std::rc::Rc<T, TursoAllocator>;

#[cfg(not(nightly))]
pub type RcWeak<T> = std::rc::Weak<T>;
#[cfg(nightly)]
pub type RcWeak<T> = std::rc::Weak<T, TursoAllocator>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetAllocatorError {
    AlreadyInitialized,
}

impl fmt::Display for SetAllocatorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AlreadyInitialized => f.write_str("Turso allocator is already initialized"),
        }
    }
}

impl std::error::Error for SetAllocatorError {}

/// Backend for Turso heap allocations.
///
/// # Safety
///
/// Implementations must uphold the `Allocator` contract for every allocation
/// returned from `allocate`, including zero-sized layouts.
pub unsafe trait TursoAllocBackend: Sync {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError>;

    /// # Safety
    ///
    /// `ptr` and `layout` must describe a live block previously returned by
    /// this backend.
    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout);
}

struct DefaultBackend;

unsafe impl TursoAllocBackend for DefaultBackend {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        <api::Global as api::ApiAllocator>::allocate(&api::Global, layout)
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        unsafe {
            <api::Global as api::ApiAllocator>::deallocate(&api::Global, ptr, layout);
        }
    }
}

static DEFAULT_BACKEND: DefaultBackend = DefaultBackend;
static BACKEND: OnceLock<&'static dyn TursoAllocBackend> = OnceLock::new();

pub fn set_allocator(backend: &'static dyn TursoAllocBackend) -> Result<(), SetAllocatorError> {
    BACKEND
        .set(backend)
        .map_err(|_| SetAllocatorError::AlreadyInitialized)
}

fn backend() -> &'static dyn TursoAllocBackend {
    BACKEND.get().copied().unwrap_or(&DEFAULT_BACKEND)
}

fn vec<T>() -> Vec<T> {
    Vec::new_in(TursoAllocator)
}

fn vec_with_capacity<T>(capacity: usize) -> Vec<T> {
    Vec::with_capacity_in(capacity, TursoAllocator)
}

fn boxed<T>(value: T) -> Box<T> {
    Box::new_in(value, TursoAllocator)
}

fn try_boxed<T>(value: T) -> Result<Box<T>, AllocError> {
    Box::try_new_in(value, TursoAllocator)
}

fn hash_map_with_hasher<K, V, S>(hasher: S) -> HashMap<K, V, S> {
    std::collections::HashMap::with_hasher(hasher)
}

fn hash_set_with_hasher<T, S>(hasher: S) -> HashSet<T, S> {
    std::collections::HashSet::with_hasher(hasher)
}

pub trait TursoAllocExt {
    fn new() -> Self;
}

pub trait TursoTryWithCapacityExt: Sized {
    fn try_with_capacity(capacity: usize) -> Result<Self, TryReserveError>;
}

pub trait TursoNewExt<T> {
    fn new(value: T) -> Self;
}

pub trait TursoTryNewExt<T>: Sized {
    fn try_new(value: T) -> Result<Self, AllocError>;
}

pub trait TursoBoxExt<T>: Sized {
    fn into_inner(self) -> T;
}

pub trait TursoVecExt<T>: Sized {
    fn with_capacity(capacity: usize) -> Self;
    fn try_push(&mut self, value: T) -> Result<(), TryReserveError>;
    fn try_extend<I>(&mut self, iter: I) -> Result<(), TryReserveError>
    where
        I: IntoIterator<Item = T>;
}

pub trait TursoHashMapExt<K, V>: Sized {
    fn try_insert(&mut self, key: K, value: V) -> Result<Option<V>, TryReserveError>;
    fn try_extend<I>(&mut self, iter: I) -> Result<(), TryReserveError>
    where
        I: IntoIterator<Item = (K, V)>;
}

pub trait TursoHashSetExt<T>: Sized {
    fn try_insert(&mut self, value: T) -> Result<bool, TryReserveError>;
    fn try_extend<I>(&mut self, iter: I) -> Result<(), TryReserveError>
    where
        I: IntoIterator<Item = T>;
}

pub trait TursoVecDequeExt<T>: Sized {
    fn try_push_back(&mut self, value: T) -> Result<(), TryReserveError>;
    fn try_push_front(&mut self, value: T) -> Result<(), TryReserveError>;
    fn try_extend<I>(&mut self, iter: I) -> Result<(), TryReserveError>
    where
        I: IntoIterator<Item = T>;
}

pub trait TursoBinaryHeapExt<T>: Sized {
    fn try_push(&mut self, value: T) -> Result<(), TryReserveError>;
    fn try_extend<I>(&mut self, iter: I) -> Result<(), TryReserveError>
    where
        I: IntoIterator<Item = T>;
}

pub trait TursoFromIterator<T>: Sized {
    fn try_from_iter<I>(iter: I) -> Result<Self, TryReserveError>
    where
        I: IntoIterator<Item = T>;
}

pub trait TursoIteratorExt: Iterator + Sized {
    fn try_collect<C>(self) -> Result<C, TryReserveError>
    where
        C: TursoFromIterator<Self::Item>,
    {
        C::try_from_iter(self)
    }
}

impl<T> TursoAllocExt for Vec<T> {
    fn new() -> Self {
        vec()
    }
}

impl<T> TursoVecExt<T> for Vec<T> {
    fn with_capacity(capacity: usize) -> Self {
        vec_with_capacity(capacity)
    }

    fn try_push(&mut self, value: T) -> Result<(), TryReserveError> {
        self.try_reserve(1).map_err(TryReserveError::from)?;
        self.push(value);
        Ok(())
    }

    fn try_extend<I>(&mut self, iter: I) -> Result<(), TryReserveError>
    where
        I: IntoIterator<Item = T>,
    {
        let iter = iter.into_iter();
        let (lower, upper) = iter.size_hint();
        self.try_reserve(upper.unwrap_or(lower))
            .map_err(TryReserveError::from)?;
        for value in iter {
            self.try_push(value)?;
        }
        Ok(())
    }
}

impl<T> TursoTryWithCapacityExt for Vec<T> {
    fn try_with_capacity(capacity: usize) -> Result<Self, TryReserveError> {
        #[cfg(not(nightly))]
        {
            let mut values = vec();
            values
                .try_reserve(capacity)
                .map_err(TryReserveError::from)?;
            Ok(values)
        }
        #[cfg(nightly)]
        {
            std::vec::Vec::try_with_capacity_in(capacity, TursoAllocator)
                .map_err(TryReserveError::from)
        }
    }
}

impl<T> TursoFromIterator<T> for Vec<T> {
    fn try_from_iter<I>(iter: I) -> Result<Self, TryReserveError>
    where
        I: IntoIterator<Item = T>,
    {
        let iter = iter.into_iter();
        let (lower, upper) = iter.size_hint();
        let capacity = upper.unwrap_or(lower);
        let mut values = <Self as TursoTryWithCapacityExt>::try_with_capacity(capacity)?;
        for value in iter {
            values.try_push(value)?;
        }
        Ok(values)
    }
}

impl<T> TursoNewExt<T> for Box<T> {
    fn new(value: T) -> Self {
        boxed(value)
    }
}

impl<T> TursoTryNewExt<T> for Box<T> {
    fn try_new(value: T) -> Result<Self, AllocError> {
        try_boxed(value)
    }
}

impl<T> TursoBoxExt<T> for Box<T> {
    fn into_inner(self) -> T {
        #[cfg(not(nightly))]
        {
            Box::into_inner(self)
        }
        #[cfg(nightly)]
        {
            *self
        }
    }
}

impl<K, V, S> TursoAllocExt for HashMap<K, V, S>
where
    S: Default,
{
    fn new() -> Self {
        hash_map_with_hasher(S::default())
    }
}

impl<T, S> TursoAllocExt for HashSet<T, S>
where
    S: Default,
{
    fn new() -> Self {
        hash_set_with_hasher(S::default())
    }
}

impl<K, V, S> TursoHashMapExt<K, V> for HashMap<K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    fn try_insert(&mut self, key: K, value: V) -> Result<Option<V>, TryReserveError> {
        self.try_reserve(1).map_err(TryReserveError::from)?;
        Ok(self.insert(key, value))
    }

    fn try_extend<I>(&mut self, iter: I) -> Result<(), TryReserveError>
    where
        I: IntoIterator<Item = (K, V)>,
    {
        let iter = iter.into_iter();
        let (lower, upper) = iter.size_hint();
        self.try_reserve(upper.unwrap_or(lower))
            .map_err(TryReserveError::from)?;
        for (key, value) in iter {
            TursoHashMapExt::try_insert(self, key, value)?;
        }
        Ok(())
    }
}

impl<K, V, S> TursoTryWithCapacityExt for HashMap<K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher + Default,
{
    fn try_with_capacity(capacity: usize) -> Result<Self, TryReserveError> {
        let mut values = <Self as TursoAllocExt>::new();
        values
            .try_reserve(capacity)
            .map_err(TryReserveError::from)?;
        Ok(values)
    }
}

impl<K, V, S> TursoFromIterator<(K, V)> for HashMap<K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher + Default,
{
    fn try_from_iter<I>(iter: I) -> Result<Self, TryReserveError>
    where
        I: IntoIterator<Item = (K, V)>,
    {
        let iter = iter.into_iter();
        let (lower, upper) = iter.size_hint();
        let capacity = upper.unwrap_or(lower);
        let mut values = <Self as TursoTryWithCapacityExt>::try_with_capacity(capacity)?;
        for (key, value) in iter {
            TursoHashMapExt::try_insert(&mut values, key, value)?;
        }
        Ok(values)
    }
}

impl<T, S> TursoHashSetExt<T> for HashSet<T, S>
where
    T: Eq + Hash,
    S: BuildHasher,
{
    fn try_insert(&mut self, value: T) -> Result<bool, TryReserveError> {
        self.try_reserve(1).map_err(TryReserveError::from)?;
        Ok(self.insert(value))
    }

    fn try_extend<I>(&mut self, iter: I) -> Result<(), TryReserveError>
    where
        I: IntoIterator<Item = T>,
    {
        let iter = iter.into_iter();
        let (lower, upper) = iter.size_hint();
        self.try_reserve(upper.unwrap_or(lower))
            .map_err(TryReserveError::from)?;
        for value in iter {
            TursoHashSetExt::try_insert(self, value)?;
        }
        Ok(())
    }
}

impl<T, S> TursoTryWithCapacityExt for HashSet<T, S>
where
    T: Eq + Hash,
    S: BuildHasher + Default,
{
    fn try_with_capacity(capacity: usize) -> Result<Self, TryReserveError> {
        let mut values = <Self as TursoAllocExt>::new();
        values
            .try_reserve(capacity)
            .map_err(TryReserveError::from)?;
        Ok(values)
    }
}

impl<T, S> TursoFromIterator<T> for HashSet<T, S>
where
    T: Eq + Hash,
    S: BuildHasher + Default,
{
    fn try_from_iter<I>(iter: I) -> Result<Self, TryReserveError>
    where
        I: IntoIterator<Item = T>,
    {
        let iter = iter.into_iter();
        let (lower, upper) = iter.size_hint();
        let capacity = upper.unwrap_or(lower);
        let mut values = <Self as TursoTryWithCapacityExt>::try_with_capacity(capacity)?;
        for value in iter {
            TursoHashSetExt::try_insert(&mut values, value)?;
        }
        Ok(values)
    }
}

#[cfg(not(nightly))]
fn btree_map<K, V>() -> BTreeMap<K, V> {
    std::collections::BTreeMap::new()
}

#[cfg(nightly)]
fn btree_map<K, V>() -> BTreeMap<K, V> {
    std::collections::BTreeMap::new_in(TursoAllocator)
}

#[cfg(not(nightly))]
impl<K, V> TursoAllocExt for BTreeMap<K, V> {
    fn new() -> Self {
        btree_map()
    }
}

#[cfg(nightly)]
impl<K, V> TursoAllocExt for BTreeMap<K, V> {
    fn new() -> Self {
        btree_map()
    }
}

#[cfg(not(nightly))]
fn btree_set<T>() -> BTreeSet<T> {
    BTreeSet::new()
}

#[cfg(nightly)]
fn btree_set<T>() -> BTreeSet<T> {
    BTreeSet::new_in(TursoAllocator)
}

impl<T> TursoAllocExt for BTreeSet<T> {
    fn new() -> Self {
        btree_set()
    }
}

#[cfg(not(nightly))]
fn vec_deque<T>() -> VecDeque<T> {
    std::collections::VecDeque::new()
}

#[cfg(nightly)]
fn vec_deque<T>() -> VecDeque<T> {
    std::collections::VecDeque::new_in(TursoAllocator)
}

#[cfg(not(nightly))]
impl<T> TursoAllocExt for VecDeque<T> {
    fn new() -> Self {
        vec_deque()
    }
}

#[cfg(nightly)]
impl<T> TursoAllocExt for VecDeque<T> {
    fn new() -> Self {
        vec_deque()
    }
}

impl<T> TursoVecDequeExt<T> for VecDeque<T> {
    fn try_push_back(&mut self, value: T) -> Result<(), TryReserveError> {
        self.try_reserve(1).map_err(TryReserveError::from)?;
        self.push_back(value);
        Ok(())
    }

    fn try_push_front(&mut self, value: T) -> Result<(), TryReserveError> {
        self.try_reserve(1).map_err(TryReserveError::from)?;
        self.push_front(value);
        Ok(())
    }

    fn try_extend<I>(&mut self, iter: I) -> Result<(), TryReserveError>
    where
        I: IntoIterator<Item = T>,
    {
        let iter = iter.into_iter();
        let (lower, upper) = iter.size_hint();
        self.try_reserve(upper.unwrap_or(lower))
            .map_err(TryReserveError::from)?;
        for value in iter {
            self.try_push_back(value)?;
        }
        Ok(())
    }
}

impl<T> TursoTryWithCapacityExt for VecDeque<T> {
    fn try_with_capacity(capacity: usize) -> Result<Self, TryReserveError> {
        let mut values = <Self as TursoAllocExt>::new();
        values
            .try_reserve(capacity)
            .map_err(TryReserveError::from)?;
        Ok(values)
    }
}

impl<T> TursoFromIterator<T> for VecDeque<T> {
    fn try_from_iter<I>(iter: I) -> Result<Self, TryReserveError>
    where
        I: IntoIterator<Item = T>,
    {
        let iter = iter.into_iter();
        let (lower, upper) = iter.size_hint();
        let capacity = upper.unwrap_or(lower);
        let mut values = <Self as TursoTryWithCapacityExt>::try_with_capacity(capacity)?;
        for value in iter {
            values.try_push_back(value)?;
        }
        Ok(values)
    }
}

#[cfg(not(nightly))]
fn binary_heap<T: Ord>() -> BinaryHeap<T> {
    std::collections::BinaryHeap::new()
}

#[cfg(nightly)]
fn binary_heap<T: Ord>() -> BinaryHeap<T> {
    std::collections::BinaryHeap::new_in(TursoAllocator)
}

#[cfg(not(nightly))]
impl<T: Ord> TursoAllocExt for BinaryHeap<T> {
    fn new() -> Self {
        binary_heap()
    }
}

#[cfg(nightly)]
impl<T: Ord> TursoAllocExt for BinaryHeap<T> {
    fn new() -> Self {
        binary_heap()
    }
}

impl<T: Ord> TursoBinaryHeapExt<T> for BinaryHeap<T> {
    fn try_push(&mut self, value: T) -> Result<(), TryReserveError> {
        self.try_reserve(1).map_err(TryReserveError::from)?;
        self.push(value);
        Ok(())
    }

    fn try_extend<I>(&mut self, iter: I) -> Result<(), TryReserveError>
    where
        I: IntoIterator<Item = T>,
    {
        let iter = iter.into_iter();
        let (lower, upper) = iter.size_hint();
        self.try_reserve(upper.unwrap_or(lower))
            .map_err(TryReserveError::from)?;
        for value in iter {
            self.try_push(value)?;
        }
        Ok(())
    }
}

impl<T: Ord> TursoTryWithCapacityExt for BinaryHeap<T> {
    fn try_with_capacity(capacity: usize) -> Result<Self, TryReserveError> {
        let mut values = <Self as TursoAllocExt>::new();
        values
            .try_reserve(capacity)
            .map_err(TryReserveError::from)?;
        Ok(values)
    }
}

impl<T: Ord> TursoFromIterator<T> for BinaryHeap<T> {
    fn try_from_iter<I>(iter: I) -> Result<Self, TryReserveError>
    where
        I: IntoIterator<Item = T>,
    {
        let iter = iter.into_iter();
        let (lower, upper) = iter.size_hint();
        let capacity = upper.unwrap_or(lower);
        let mut values = <Self as TursoTryWithCapacityExt>::try_with_capacity(capacity)?;
        for value in iter {
            values.try_push(value)?;
        }
        Ok(values)
    }
}

impl<I: Iterator> TursoIteratorExt for I {}

#[cfg(not(nightly))]
fn linked_list<T>() -> LinkedList<T> {
    std::collections::LinkedList::new()
}

#[cfg(nightly)]
fn linked_list<T>() -> LinkedList<T> {
    std::collections::LinkedList::new_in(TursoAllocator)
}

#[cfg(not(nightly))]
impl<T> TursoAllocExt for LinkedList<T> {
    fn new() -> Self {
        linked_list()
    }
}

#[cfg(nightly)]
impl<T> TursoAllocExt for LinkedList<T> {
    fn new() -> Self {
        linked_list()
    }
}

fn arc<T>(value: T) -> Arc<T> {
    crate::sync::Arc::new(value)
}

impl<T> TursoNewExt<T> for Arc<T> {
    fn new(value: T) -> Self {
        arc(value)
    }
}

#[cfg(not(nightly))]
fn rc<T>(value: T) -> Rc<T> {
    std::rc::Rc::new(value)
}

#[cfg(nightly)]
fn rc<T>(value: T) -> Rc<T> {
    std::rc::Rc::new_in(value, TursoAllocator)
}

#[cfg(not(nightly))]
impl<T> TursoNewExt<T> for Rc<T> {
    fn new(value: T) -> Self {
        rc(value)
    }
}

#[cfg(nightly)]
impl<T> TursoNewExt<T> for Rc<T> {
    fn new(value: T) -> Self {
        rc(value)
    }
}

#[cfg(not(nightly))]
unsafe impl api::ApiAllocator for TursoAllocator {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        backend().allocate(layout)
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        unsafe {
            backend().deallocate(ptr, layout);
        }
    }
}

#[cfg(nightly)]
unsafe impl api::ApiAllocator for TursoAllocator {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        backend().allocate(layout)
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        unsafe {
            backend().deallocate(ptr, layout);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct LowerBoundOnly {
        next: usize,
        end: usize,
    }

    impl Iterator for LowerBoundOnly {
        type Item = usize;

        fn next(&mut self) -> Option<Self::Item> {
            if self.next == self.end {
                return None;
            }
            let value = self.next;
            self.next += 1;
            Some(value)
        }

        fn size_hint(&self) -> (usize, Option<usize>) {
            (0, None)
        }
    }

    #[test]
    fn try_extend_accepts_exact_size_iterators() {
        let mut values = Vec::new();

        values.try_extend([1, 2, 3]).unwrap();

        assert_eq!(values.as_slice(), &[1, 2, 3]);
    }

    #[test]
    fn try_extend_accepts_iterators_without_upper_bounds() {
        let mut values = Vec::new();

        values
            .try_extend(LowerBoundOnly { next: 0, end: 3 })
            .unwrap();

        assert_eq!(values.as_slice(), &[0, 1, 2]);
    }

    #[test]
    fn hash_map_try_insert_and_extend_reserve_before_mutation() {
        let mut values: HashMap<&str, usize> = TursoAllocExt::new();

        assert_eq!(
            TursoHashMapExt::try_insert(&mut values, "one", 1).unwrap(),
            None
        );
        assert_eq!(
            TursoHashMapExt::try_insert(&mut values, "one", 11).unwrap(),
            Some(1)
        );
        values.try_extend([("two", 2), ("three", 3)]).unwrap();

        assert_eq!(values.get("one"), Some(&11));
        assert_eq!(values.get("two"), Some(&2));
        assert_eq!(values.get("three"), Some(&3));
    }

    #[test]
    fn hash_set_try_insert_and_extend_reserve_before_mutation() {
        let mut values: HashSet<usize> = TursoAllocExt::new();

        assert!(TursoHashSetExt::try_insert(&mut values, 1).unwrap());
        assert!(!TursoHashSetExt::try_insert(&mut values, 1).unwrap());
        values.try_extend([2, 3]).unwrap();

        assert!(values.contains(&1));
        assert!(values.contains(&2));
        assert!(values.contains(&3));
    }

    #[test]
    fn vec_deque_try_push_and_extend_reserve_before_mutation() {
        let mut values: VecDeque<usize> = TursoAllocExt::new();

        values.try_push_back(2).unwrap();
        values.try_push_front(1).unwrap();
        values.try_extend([3, 4]).unwrap();

        assert_eq!(values.pop_front(), Some(1));
        assert_eq!(values.pop_front(), Some(2));
        assert_eq!(values.pop_front(), Some(3));
        assert_eq!(values.pop_front(), Some(4));
        assert_eq!(values.pop_front(), None);
    }

    #[test]
    fn binary_heap_try_push_and_extend_reserve_before_mutation() {
        let mut values: BinaryHeap<usize> = TursoAllocExt::new();

        values.try_push(2).unwrap();
        values.try_extend([1, 3]).unwrap();

        assert_eq!(values.pop(), Some(3));
        assert_eq!(values.pop(), Some(2));
        assert_eq!(values.pop(), Some(1));
        assert_eq!(values.pop(), None);
    }

    #[test]
    fn iterator_try_collect_builds_turso_vec() {
        let values: Vec<_> = [1, 2, 3].into_iter().try_collect().unwrap();

        assert_eq!(values.as_slice(), &[1, 2, 3]);
    }

    #[test]
    fn try_vec_macro_builds_turso_vecs() {
        let empty: Vec<usize> = try_vec![].unwrap();
        let repeated: Vec<_> = try_vec![7; 3].unwrap();
        let listed: Vec<_> = try_vec![1, 2, 3].unwrap();

        assert!(empty.is_empty());
        assert_eq!(repeated.as_slice(), &[7, 7, 7]);
        assert_eq!(listed.as_slice(), &[1, 2, 3]);
    }

    #[test]
    fn iterator_try_collect_builds_turso_collections() {
        let map: HashMap<_, _> = [("one", 1), ("two", 2)].into_iter().try_collect().unwrap();
        let set: HashSet<_> = [1, 2, 3].into_iter().try_collect().unwrap();
        let heap: BinaryHeap<_> = [1, 3, 2].into_iter().try_collect().unwrap();

        assert_eq!(map.get("one"), Some(&1));
        assert!(set.contains(&3));
        assert_eq!(heap.peek(), Some(&3));
    }

    #[test]
    fn try_with_capacity_builds_turso_collections() {
        let values: Vec<usize> = TursoTryWithCapacityExt::try_with_capacity(3).unwrap();
        let map: HashMap<usize, usize> = TursoTryWithCapacityExt::try_with_capacity(3).unwrap();
        let set: HashSet<usize> = TursoTryWithCapacityExt::try_with_capacity(3).unwrap();
        let queue: VecDeque<usize> = TursoTryWithCapacityExt::try_with_capacity(3).unwrap();
        let heap: BinaryHeap<usize> = TursoTryWithCapacityExt::try_with_capacity(3).unwrap();

        assert!(values.capacity() >= 3);
        assert!(map.capacity() >= 3);
        assert!(set.capacity() >= 3);
        assert!(queue.capacity() >= 3);
        assert!(heap.capacity() >= 3);
    }
}
