use super::super::{AllocError, TryReserveError};

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

pub trait TryClone: Sized {
    type Error;

    fn try_clone(&self) -> Result<Self, Self::Error>;
}
