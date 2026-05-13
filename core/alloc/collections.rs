use std::hash::{BuildHasher, Hash};

use super::{
    AllocError, Arc, BTreeMap, BTreeSet, BinaryHeap, Box, HashMap, HashSet, LinkedList, Rc,
    TryReserveError, TursoAllocator, Vec, VecDeque,
};

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

impl<T, C> TursoFromIterator<Option<T>> for Option<C>
where
    C: TursoFromIterator<T>,
{
    fn try_from_iter<I>(iter: I) -> Result<Self, TryReserveError>
    where
        I: IntoIterator<Item = Option<T>>,
    {
        let mut saw_none = false;
        let values = C::try_from_iter(iter.into_iter().scan((), |(), item| match item {
            Some(value) => Some(value),
            None => {
                saw_none = true;
                None
            }
        }))?;
        if saw_none {
            Ok(None)
        } else {
            Ok(Some(values))
        }
    }
}

impl<T, E, F, C> TursoFromIterator<Result<T, E>> for Result<C, F>
where
    C: TursoFromIterator<T>,
    F: From<E>,
{
    fn try_from_iter<I>(iter: I) -> Result<Self, TryReserveError>
    where
        I: IntoIterator<Item = Result<T, E>>,
    {
        let mut error = None;
        let values = C::try_from_iter(iter.into_iter().scan((), |(), item| match item {
            Ok(value) => Some(value),
            Err(err) => {
                error = Some(F::from(err));
                None
            }
        }))?;
        if let Some(error) = error {
            Ok(Err(error))
        } else {
            Ok(Ok(values))
        }
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
