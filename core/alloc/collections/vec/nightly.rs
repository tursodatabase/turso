use std::{iter::TrustedLen, ptr, slice};

use super::super::{
    TryClone, TursoAllocExt, TursoFromIterator, TursoSliceExt, TursoTryWithCapacityExt, TursoVecExt,
};
use crate::alloc::{TryReserveError, TursoAllocator, Vec};

pub(super) const fn vec<T>() -> Vec<T> {
    Vec::new_in(TursoAllocator)
}

fn vec_with_capacity<T>(capacity: usize) -> Vec<T> {
    Vec::with_capacity_in(capacity, TursoAllocator)
}

impl<T> TursoAllocExt for Vec<T> {
    #[inline(always)]
    fn new() -> Self {
        vec()
    }
}

impl<T> TursoVecExt<T> for Vec<T> {
    #[inline(always)]
    fn with_capacity(capacity: usize) -> Self {
        vec_with_capacity(capacity)
    }

    #[inline(always)]
    fn try_push(&mut self, value: T) -> Result<(), TryReserveError> {
        match self.push_within_capacity(value) {
            Ok(_) => Ok(()),
            Err(value) => {
                self.try_reserve(1).map_err(TryReserveError::from)?;
                match self.push_within_capacity(value) {
                    Ok(_) => Ok(()),
                    Err(_) => unreachable!("Vec::try_reserve(1) did not make room"),
                }
            }
        }
    }
}

impl<T> TursoTryWithCapacityExt for Vec<T> {
    #[inline(always)]
    fn try_with_capacity_ext(capacity: usize) -> Result<Self, TryReserveError> {
        Self::try_with_capacity_in(capacity, TursoAllocator).map_err(TryReserveError::from)
    }
}

impl<T> TursoFromIterator<T> for Vec<T> {
    #[inline(always)]
    fn try_from_iter<I>(iter: I) -> Result<Self, TryReserveError>
    where
        I: IntoIterator<Item = T>,
    {
        <Self as TrySpecFromIter<T, I::IntoIter>>::try_from_iter(iter.into_iter())
    }

    #[inline(always)]
    fn try_extend<I>(&mut self, iter: I) -> Result<(), TryReserveError>
    where
        I: IntoIterator<Item = T>,
    {
        self.try_spec_extend(iter.into_iter())
    }
}

impl<T: Clone> TursoSliceExt<T> for [T] {
    #[inline(always)]
    fn try_to_vec(&self) -> Result<Vec<T>, TryReserveError> {
        let mut values = <Vec<T> as TursoTryWithCapacityExt>::try_with_capacity_ext(self.len())?;
        values.try_spec_extend_ref(self.iter())?;
        Ok(values)
    }
}

impl<T: Clone> TryClone for Vec<T> {
    type Error = TryReserveError;

    #[inline(always)]
    fn try_clone(&self) -> Result<Self, Self::Error> {
        let allocator = self.allocator().clone();
        let mut cloned =
            Self::try_with_capacity_in(self.len(), allocator).map_err(TryReserveError::from)?;
        cloned.try_spec_extend_ref(self.iter())?;
        Ok(cloned)
    }
}

/// Vec specialization shim for `TursoFromIterator::try_extend`.
///
/// The default implementation handles any iterator and keeps a capacity check in
/// the loop because ordinary `Iterator::size_hint` is only advisory. The
/// `TrustedLen` specialization reserves once and writes directly into spare
/// capacity because the iterator promises the exact remaining length.
trait TrySpecExtend<T, I> {
    fn try_spec_extend(&mut self, iter: I) -> Result<(), TryReserveError>;
}

impl<T, I> TrySpecExtend<T, I> for Vec<T>
where
    I: Iterator<Item = T>,
{
    #[inline]
    default fn try_spec_extend(&mut self, iter: I) -> Result<(), TryReserveError> {
        self.try_extend_desugared(iter, true)
    }
}

impl<T, I> TrySpecExtend<T, I> for Vec<T>
where
    I: TrustedLen<Item = T>,
{
    #[inline]
    default fn try_spec_extend(&mut self, iter: I) -> Result<(), TryReserveError> {
        self.try_extend_trusted(iter)
    }
}

/// Vec specialization shim for reference iterators.
///
/// The default path clones arbitrary reference iterators, then lets the owned
/// specialization pick the best path. The concrete `slice::Iter` specialization
/// for `Copy` values can bulk-copy after a single fallible reservation.
trait TrySpecExtendRef<'a, T, I> {
    fn try_spec_extend_ref(&mut self, iter: I) -> Result<(), TryReserveError>;
}

impl<'a, T, I> TrySpecExtendRef<'a, T, I> for Vec<T>
where
    T: Clone + 'a,
    I: Iterator<Item = &'a T>,
{
    #[inline]
    default fn try_spec_extend_ref(&mut self, iter: I) -> Result<(), TryReserveError> {
        self.try_spec_extend(iter.cloned())
    }
}

impl<'a, T> TrySpecExtendRef<'a, T, slice::Iter<'a, T>> for Vec<T>
where
    T: Copy + 'a,
{
    #[inline]
    fn try_spec_extend_ref(&mut self, iter: slice::Iter<'a, T>) -> Result<(), TryReserveError> {
        self.try_copy_from_slice(iter.as_slice())
    }
}

/// Vec specialization shim for `TursoFromIterator::try_from_iter`.
///
/// Collection has different fast paths from extension, most notably the
/// empty-iterator case and exact preallocation for `TrustedLen`, so route it
/// through its own trait.
trait TrySpecFromIter<T, I>: Sized {
    fn try_from_iter(iter: I) -> Result<Self, TryReserveError>;
}

impl<T, I> TrySpecFromIter<T, I> for Vec<T>
where
    I: Iterator<Item = T>,
{
    #[inline]
    default fn try_from_iter(mut iter: I) -> Result<Self, TryReserveError> {
        let Some(first) = iter.next() else {
            return Ok(vec());
        };

        let mut values = vec();
        let (lower, upper) = iter.size_hint();
        let initial = upper.unwrap_or(lower).saturating_add(1);
        values.try_reserve(initial).map_err(TryReserveError::from)?;

        unsafe {
            ptr::write(values.as_mut_ptr(), first);
            values.set_len(1);
        }

        values.try_extend_desugared(iter, false)?;
        Ok(values)
    }
}

impl<T, I> TrySpecFromIter<T, I> for Vec<T>
where
    I: TrustedLen<Item = T>,
{
    #[inline]
    fn try_from_iter(iter: I) -> Result<Self, TryReserveError> {
        let additional = trusted_len(iter.size_hint())?;
        if additional == 0 {
            return Ok(vec());
        }

        let mut values =
            Vec::try_with_capacity_in(additional, TursoAllocator).map_err(TryReserveError::from)?;
        unsafe {
            values.extend_trusted_unchecked(iter);
        }
        Ok(values)
    }
}

/// Shared implementation details for generic and exact-length Vec paths.
trait TryVecInternal<T> {
    fn try_extend_desugared<I>(
        &mut self,
        iter: I,
        reserve_upper_bound: bool,
    ) -> Result<(), TryReserveError>
    where
        I: Iterator<Item = T>;

    fn try_extend_trusted<I>(&mut self, iter: I) -> Result<(), TryReserveError>
    where
        I: TrustedLen<Item = T>;

    unsafe fn extend_trusted_unchecked<I>(&mut self, iter: I)
    where
        I: TrustedLen<Item = T>;

    fn try_copy_from_slice(&mut self, slice: &[T]) -> Result<(), TryReserveError>
    where
        T: Copy;
}

impl<T> TryVecInternal<T> for Vec<T> {
    #[inline]
    fn try_extend_desugared<I>(
        &mut self,
        mut iter: I,
        reserve_upper_bound: bool,
    ) -> Result<(), TryReserveError>
    where
        I: Iterator<Item = T>,
    {
        if reserve_upper_bound {
            if let (_, Some(upper)) = iter.size_hint() {
                self.try_reserve(upper).map_err(TryReserveError::from)?;
            }
        }

        while let Some(element) = iter.next() {
            let len = self.len();
            if len == self.capacity() {
                let (lower, _) = iter.size_hint();
                self.try_reserve(lower.saturating_add(1))
                    .map_err(TryReserveError::from)?;
            }

            unsafe {
                ptr::write(self.as_mut_ptr().add(len), element);
                self.set_len(len + 1);
            }
        }

        Ok(())
    }

    #[inline]
    fn try_extend_trusted<I>(&mut self, iter: I) -> Result<(), TryReserveError>
    where
        I: TrustedLen<Item = T>,
    {
        let additional = trusted_len(iter.size_hint())?;

        self.try_reserve(additional)
            .map_err(TryReserveError::from)?;
        unsafe {
            self.extend_trusted_unchecked(iter);
        }

        Ok(())
    }

    #[inline]
    unsafe fn extend_trusted_unchecked<I>(&mut self, iter: I)
    where
        I: TrustedLen<Item = T>,
    {
        let ptr = self.as_mut_ptr();
        let mut guard = SetLenOnDrop::new(self);
        iter.for_each(move |element| {
            unsafe {
                ptr::write(ptr.add(guard.current_len()), element);
            }
            guard.increment_len();
        });
    }

    #[inline]
    fn try_copy_from_slice(&mut self, slice: &[T]) -> Result<(), TryReserveError>
    where
        T: Copy,
    {
        self.try_reserve(slice.len())
            .map_err(TryReserveError::from)?;
        unsafe {
            let len = self.len();
            ptr::copy_nonoverlapping(slice.as_ptr(), self.as_mut_ptr().add(len), slice.len());
            self.set_len(len + slice.len());
        }

        Ok(())
    }
}

#[inline]
fn trusted_len(size_hint: (usize, Option<usize>)) -> Result<usize, TryReserveError> {
    let (lower, upper) = size_hint;
    let Some(additional) = upper else {
        let Err(err) = std::vec::Vec::<u8>::new().try_reserve(usize::MAX) else {
            unreachable!("reserving usize::MAX bytes must fail");
        };
        return Err(TryReserveError::from(err));
    };
    debug_assert_eq!(lower, additional);
    Ok(additional)
}

/// Publishes the vector length for direct writes, including during unwinding.
struct SetLenOnDrop<'a, T> {
    vec: &'a mut Vec<T>,
    len: usize,
}

impl<'a, T> SetLenOnDrop<'a, T> {
    #[inline]
    fn new(vec: &'a mut Vec<T>) -> Self {
        Self {
            len: vec.len(),
            vec,
        }
    }

    #[inline]
    fn current_len(&self) -> usize {
        self.len
    }

    #[inline]
    fn increment_len(&mut self) {
        self.len += 1;
    }
}

impl<T> Drop for SetLenOnDrop<'_, T> {
    #[inline]
    fn drop(&mut self) {
        unsafe {
            self.vec.set_len(self.len);
        }
    }
}
