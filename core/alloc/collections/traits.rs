use super::super::{AllocError, TryReserveError};

pub trait TursoAllocExt {
    fn new() -> Self;
}

pub trait TursoTryWithCapacityExt: Sized {
    fn try_with_capacity_ext(capacity: usize) -> Result<Self, TryReserveError>;
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

    /// Appends an element and returns a reference to it if there is sufficient spare capacity,
    /// otherwise an error is returned with the element.
    ///
    /// Unlike `push`, this method does not reallocate when capacity is exhausted. Callers should
    /// reserve capacity before using this when insertion must succeed.
    ///
    /// Mirrors the unstable standard library implementation:
    /// <https://doc.rust-lang.org/src/alloc/vec/mod.rs.html#2786>
    ///
    /// Takes O(1) time.
    fn push_within_capacity(&mut self, value: T) -> Result<&mut T, T>;

    fn try_push(&mut self, value: T) -> Result<(), TryReserveError>;
}

pub trait TursoVecInExt<T, A>: Sized {
    fn new_in(alloc: A) -> Self;
    fn with_capacity_in(capacity: usize, alloc: A) -> Self;
    fn try_with_capacity_in(capacity: usize, alloc: A) -> Result<Self, TryReserveError>;
}

pub trait TursoHashMapExt<K, V>: Sized {
    fn try_insert(&mut self, key: K, value: V) -> Result<Option<V>, TryReserveError>;
}

pub trait TursoHashSetExt<T>: Sized {
    fn try_insert(&mut self, value: T) -> Result<bool, TryReserveError>;
}

pub trait TursoVecDequeExt<T>: Sized {
    fn try_push_back(&mut self, value: T) -> Result<(), TryReserveError>;
    fn try_push_front(&mut self, value: T) -> Result<(), TryReserveError>;
}

pub trait TursoBinaryHeapExt<T>: Sized {
    fn try_push(&mut self, value: T) -> Result<(), TryReserveError>;
}

/// Conversion from a slice into an allocator-aware `Vec`.
///
/// Named `try_to_vec` because the inherent `[T]::to_vec` would always shadow
/// a trait method called `to_vec`, leaving call sites on the global
/// allocator.
pub trait TursoSliceExt<T> {
    fn try_to_vec(&self) -> Result<crate::alloc::Vec<T>, TryReserveError>;
}

pub trait TursoFromIterator<T>: Sized {
    fn try_from_iter<I>(iter: I) -> Result<Self, TryReserveError>
    where
        I: IntoIterator<Item = T>;

    fn try_extend<I>(&mut self, iter: I) -> Result<(), TryReserveError>
    where
        I: IntoIterator<Item = T>;
}

#[cfg(nightly)]
pub trait TursoFromIteratorIn<T, A>: Sized {
    fn try_from_iter_in<I>(iter: I, alloc: A) -> Result<Self, TryReserveError>
    where
        I: IntoIterator<Item = T>;
}

pub trait TursoIteratorExt: Iterator + Sized {
    #[inline(always)]
    fn try_collect<C>(self) -> Result<C, TryReserveError>
    where
        C: TursoFromIterator<Self::Item>,
    {
        C::try_from_iter(self)
    }

    #[inline]
    fn try_unzip<A, B, FromA, FromB>(self) -> Result<(FromA, FromB), TryReserveError>
    where
        (FromA, FromB): TursoFromIterator<(A, B)>,
        Self: Iterator<Item = (A, B)>,
    {
        <(FromA, FromB) as TursoFromIterator<(A, B)>>::try_from_iter(self)
    }

    #[cfg(nightly)]
    #[inline(always)]
    fn try_collect_in<C, A>(self, alloc: A) -> Result<C, TryReserveError>
    where
        C: TursoFromIteratorIn<Self::Item, A>,
    {
        C::try_from_iter_in(self, alloc)
    }
}

pub trait TryClone: Sized {
    type Error;

    fn try_clone(&self) -> Result<Self, Self::Error>;

    /// Replaces `self` with a fallible clone of `source`.
    ///
    /// Implementors can override this to reuse owned resources.
    fn try_clone_from(&mut self, source: &Self) -> Result<(), Self::Error> {
        *self = source.try_clone()?;
        Ok(())
    }
}

/// Forward `TryClone` to `Clone` for element types whose clone either cannot
/// allocate at all (`Copy` primitives) or only allocates through the std
/// global allocator for now (std-pinned types like `String`). The
/// `Infallible` error encodes that these clones never return `Err`. The
/// std-pinned group is a migration marker: once such a type becomes
/// allocator-aware, give it a real `TryClone<Error = TryReserveError>` impl
/// and remove it from this list.
macro_rules! impl_try_clone_via_clone {
    ($($ty:ty),+ $(,)?) => {
        $(
            impl TryClone for $ty {
                type Error = std::convert::Infallible;

                #[inline(always)]
                fn try_clone(&self) -> Result<Self, Self::Error> {
                    Ok(self.clone())
                }
            }
        )+
    };
}

pub(crate) use impl_try_clone_via_clone;

impl_try_clone_via_clone!(
    bool,
    char,
    u8,
    u16,
    u32,
    u64,
    u128,
    usize,
    i8,
    i16,
    i32,
    i64,
    i128,
    isize,
    f32,
    f64,
    std::string::String,
);

impl<A, B> TryClone for (A, B)
where
    A: TryClone,
    B: TryClone<Error = A::Error>,
{
    type Error = A::Error;

    fn try_clone(&self) -> Result<Self, Self::Error> {
        Ok((self.0.try_clone()?, self.1.try_clone()?))
    }
}

/// `Arc` clones are refcount bumps — no allocation, never fails.
impl<T> TryClone for crate::sync::Arc<T> {
    type Error = std::convert::Infallible;

    #[inline(always)]
    fn try_clone(&self) -> Result<Self, Self::Error> {
        Ok(self.clone())
    }
}

impl<T> TryClone for Option<T>
where
    T: TryClone,
{
    type Error = T::Error;

    fn try_clone(&self) -> Result<Self, Self::Error> {
        match self {
            Some(value) => Ok(Some(value.try_clone()?)),
            None => Ok(None),
        }
    }
}

impl<T, E> TryClone for Result<T, E>
where
    T: TryClone,
    E: TryClone<Error = T::Error>,
{
    type Error = T::Error;

    fn try_clone(&self) -> Result<Self, Self::Error> {
        match self {
            Ok(value) => Ok(Ok(value.try_clone()?)),
            Err(err) => Ok(Err(err.try_clone()?)),
        }
    }
}
