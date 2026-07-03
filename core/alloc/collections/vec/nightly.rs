use std::{alloc::Allocator, iter::TrustedLen, ptr, slice};

use super::super::{
    TryClone, TursoAllocExt, TursoFromIterator, TursoFromIteratorIn, TursoSliceExt,
    TursoTryWithCapacityExt, TursoVecExt, TursoVecInExt,
};
use crate::alloc::{TryReserveError, TursoAllocator, Vec};

pub(super) const fn vec<T>() -> Vec<T> {
    Vec::new_in(TursoAllocator)
}

fn vec_in<T, A: std::alloc::Allocator>(alloc: A) -> Vec<T, A> {
    Vec::new_in(alloc)
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
        Vec::with_capacity_in(capacity, TursoAllocator)
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

impl<T, A> TursoVecInExt<T, A> for Vec<T, A>
where
    A: std::alloc::Allocator,
{
    #[inline(always)]
    fn new_in(alloc: A) -> Self {
        vec_in(alloc)
    }

    #[inline(always)]
    fn with_capacity_in(capacity: usize, alloc: A) -> Self {
        Vec::with_capacity_in(capacity, alloc)
    }

    #[inline(always)]
    fn try_with_capacity_in(capacity: usize, alloc: A) -> Result<Self, TryReserveError> {
        Vec::try_with_capacity_in(capacity, alloc).map_err(TryReserveError::from)
    }
}

impl<T> TursoTryWithCapacityExt for Vec<T> {
    #[inline(always)]
    fn try_with_capacity_ext(capacity: usize) -> Result<Self, TryReserveError> {
        Self::try_with_capacity_in(capacity, TursoAllocator).map_err(TryReserveError::from)
    }
}

impl<T, A> TursoFromIterator<T> for Vec<T, A>
where
    A: Allocator + Default,
{
    #[inline(always)]
    fn try_from_iter<I>(iter: I) -> Result<Self, TryReserveError>
    where
        I: IntoIterator<Item = T>,
    {
        <Self as TursoFromIteratorIn<T, A>>::try_from_iter_in(iter, A::default())
    }

    #[inline(always)]
    fn try_extend<I>(&mut self, iter: I) -> Result<(), TryReserveError>
    where
        I: IntoIterator<Item = T>,
    {
        self.try_spec_extend(iter.into_iter())
    }
}

impl<T, A> TursoFromIteratorIn<T, A> for Vec<T, A>
where
    A: Allocator,
{
    #[inline(always)]
    fn try_from_iter_in<I>(iter: I, alloc: A) -> Result<Self, TryReserveError>
    where
        I: IntoIterator<Item = T>,
    {
        <Self as TrySpecFromIter<T, I::IntoIter, A>>::try_from_iter_in(iter.into_iter(), alloc)
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

impl<T, A> TryClone for Vec<T, A>
where
    T: TryClone,
    TryReserveError: From<T::Error>,
    A: Allocator + Clone,
{
    type Error = TryReserveError;

    #[inline(always)]
    fn try_clone(&self) -> Result<Self, Self::Error> {
        let allocator = self.allocator().clone();
        // `|_| TryReserveError`: `TryReserveError::from` is ambiguous here
        // because the `TryReserveError: From<T::Error>` bound adds a second
        // candidate `From` impl.
        let mut cloned =
            Self::try_with_capacity_in(self.len(), allocator).map_err(|_| TryReserveError)?;
        for item in self {
            // Capacity reserved above; push cannot allocate. Elements clone
            // through `TryClone` so nested allocator-backed allocations fail
            // with `TryReserveError` instead of aborting.
            cloned.push(item.try_clone()?);
        }
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

impl<T, I, A> TrySpecExtend<T, I> for Vec<T, A>
where
    I: Iterator<Item = T>,
    A: Allocator,
{
    #[inline(always)]
    default fn try_spec_extend(&mut self, iter: I) -> Result<(), TryReserveError> {
        self.try_extend_desugared(iter, true)
    }
}

impl<T, I, A> TrySpecExtend<T, I> for Vec<T, A>
where
    I: TrustedLen<Item = T>,
    A: Allocator,
{
    #[inline(always)]
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

impl<'a, T, I, A> TrySpecExtendRef<'a, T, I> for Vec<T, A>
where
    T: Clone + 'a,
    I: Iterator<Item = &'a T>,
    A: Allocator,
{
    #[inline(always)]
    default fn try_spec_extend_ref(&mut self, iter: I) -> Result<(), TryReserveError> {
        self.try_spec_extend(iter.cloned())
    }
}

impl<'a, T, A> TrySpecExtendRef<'a, T, slice::Iter<'a, T>> for Vec<T, A>
where
    T: Copy + 'a,
    A: Allocator,
{
    #[inline(always)]
    fn try_spec_extend_ref(&mut self, iter: slice::Iter<'a, T>) -> Result<(), TryReserveError> {
        self.try_copy_from_slice(iter.as_slice())
    }
}

/// Vec specialization shim for `TursoFromIterator::try_from_iter`.
///
/// Collection has different fast paths from extension, most notably the
/// empty-iterator case and exact preallocation for `TrustedLen`, so route it
/// through its own trait.
trait TrySpecFromIter<T, I, A>: Sized
where
    A: Allocator,
{
    fn try_from_iter_in(iter: I, alloc: A) -> Result<Self, TryReserveError>;
}

impl<T, I, A> TrySpecFromIter<T, I, A> for Vec<T, A>
where
    I: Iterator<Item = T>,
    A: Allocator,
{
    #[inline(always)]
    default fn try_from_iter_in(mut iter: I, alloc: A) -> Result<Self, TryReserveError> {
        let Some(first) = iter.next() else {
            return Ok(Vec::new_in(alloc));
        };

        let mut values = Vec::new_in(alloc);
        let (lower, _) = iter.size_hint();
        let initial = lower.saturating_add(1);
        values.try_reserve(initial).map_err(TryReserveError::from)?;

        unsafe {
            ptr::write(values.as_mut_ptr(), first);
            values.set_len(1);
        }

        if lower != 0 {
            unsafe {
                extend_reserved_lower_bound(&mut values, &mut iter, lower);
            }
        }
        values.try_extend_desugared(iter, false)?;
        Ok(values)
    }
}

impl<T, I, A> TrySpecFromIter<T, I, A> for Vec<T, A>
where
    I: TrustedLen<Item = T>,
    A: Allocator,
{
    #[inline(always)]
    fn try_from_iter_in(iter: I, alloc: A) -> Result<Self, TryReserveError> {
        let additional = trusted_len(iter.size_hint())?;
        if additional == 0 {
            return Ok(Vec::new_in(alloc));
        }

        let mut values =
            Vec::try_with_capacity_in(additional, alloc).map_err(TryReserveError::from)?;
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

impl<T, A> TryVecInternal<T> for Vec<T, A>
where
    A: Allocator,
{
    #[inline(always)]
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
                try_reserve_for_extend(self, &iter)?;
            }

            unsafe {
                ptr::write(self.as_mut_ptr().add(len), element);
                self.set_len(len + 1);
            }
        }

        Ok(())
    }

    #[inline(always)]
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

    #[inline(always)]
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

    #[inline(always)]
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

#[cold]
#[inline(never)]
fn try_reserve_for_extend<T, A, I>(values: &mut Vec<T, A>, iter: &I) -> Result<(), TryReserveError>
where
    A: Allocator,
    I: Iterator<Item = T>,
{
    let (lower, _) = iter.size_hint();
    values
        .try_reserve(lower.saturating_add(1))
        .map_err(TryReserveError::from)
}

#[inline(always)]
unsafe fn extend_reserved_lower_bound<T, A, I>(
    values: &mut Vec<T, A>,
    iter: &mut I,
    additional: usize,
) where
    A: Allocator,
    I: Iterator<Item = T>,
{
    let ptr = values.as_mut_ptr();
    let mut guard = SetLenOnDrop::new(values);
    for _ in 0..additional {
        let Some(element) = iter.next() else {
            return;
        };
        unsafe {
            ptr::write(ptr.add(guard.current_len()), element);
        }
        guard.increment_len();
    }
}

/// Publishes the vector length for direct writes, including during unwinding.
struct SetLenOnDrop<'a, T, A: Allocator> {
    vec: &'a mut Vec<T, A>,
    len: usize,
}

impl<'a, T, A> SetLenOnDrop<'a, T, A>
where
    A: Allocator,
{
    #[inline]
    fn new(vec: &'a mut Vec<T, A>) -> Self {
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

impl<T, A> Drop for SetLenOnDrop<'_, T, A>
where
    A: Allocator,
{
    #[inline]
    fn drop(&mut self) {
        unsafe {
            self.vec.set_len(self.len);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Element type whose fallible clone always fails: proves `Vec::try_clone`
    /// clones elements through `TryClone` instead of the infallible `Clone`.
    #[derive(Clone)]
    struct FallibleElem;

    impl TryClone for FallibleElem {
        type Error = TryReserveError;

        fn try_clone(&self) -> Result<Self, Self::Error> {
            Err(TryReserveError)
        }
    }

    #[test]
    fn vec_try_clone_clones_elements_fallibly() {
        let mut source: Vec<FallibleElem> = vec();
        source.try_push(FallibleElem).unwrap();
        assert!(
            source.try_clone().is_err(),
            "Vec::try_clone must clone elements through TryClone"
        );
    }

    #[test]
    fn vec_try_clone_bulk_copies_copy_elements() {
        let mut source: Vec<u32> = vec();
        source.try_push(7).unwrap();
        assert_eq!(source.try_clone().unwrap().as_slice(), &[7]);
    }
}
