#[cfg(nightly)]
mod nightly;

#[cfg(not(nightly))]
use super::{
    TryClone, TursoAllocExt, TursoFromIterator, TursoSliceExt, TursoTryWithCapacityExt,
    TursoVecExt, TursoVecInExt,
};
#[cfg(not(nightly))]
use crate::alloc::{TryReserveError, Vec};

#[cfg(not(nightly))]
pub(super) const fn vec<T>() -> Vec<T> {
    Vec::new()
}

#[cfg(not(nightly))]
fn vec_with_capacity<T>(capacity: usize) -> Vec<T> {
    Vec::with_capacity(capacity)
}

#[cfg(not(nightly))]
impl<T> TursoAllocExt for Vec<T> {
    #[inline(always)]
    fn new() -> Self {
        vec()
    }
}

#[cfg(not(nightly))]
impl<T> TursoVecExt<T> for Vec<T> {
    #[inline(always)]
    fn with_capacity(capacity: usize) -> Self {
        vec_with_capacity(capacity)
    }

    #[inline(always)]
    fn push_within_capacity(&mut self, value: T) -> Result<&mut T, T> {
        if self.len() == self.capacity() {
            return Err(value);
        }

        unsafe {
            let end = self.as_mut_ptr().add(self.len());
            std::ptr::write(end, value);
            self.set_len(self.len() + 1);

            // SAFETY: We just wrote a value to the pointer that will live the lifetime of the reference.
            Ok(&mut *end)
        }
    }

    #[inline(always)]
    fn try_push(&mut self, value: T) -> Result<(), TryReserveError> {
        self.push(value);
        Ok(())
    }
}

#[cfg(not(nightly))]
impl<T, A> TursoVecInExt<T, A> for Vec<T> {
    #[inline(always)]
    fn new_in(_alloc: A) -> Self {
        vec()
    }

    #[inline(always)]
    fn with_capacity_in(capacity: usize, _alloc: A) -> Self {
        vec_with_capacity(capacity)
    }

    #[inline(always)]
    fn try_with_capacity_in(capacity: usize, alloc: A) -> Result<Self, TryReserveError> {
        Ok(<Self as TursoVecInExt<T, A>>::with_capacity_in(
            capacity, alloc,
        ))
    }
}

#[cfg(not(nightly))]
impl<T> TursoTryWithCapacityExt for Vec<T> {
    #[inline(always)]
    fn try_with_capacity_ext(capacity: usize) -> Result<Self, TryReserveError> {
        Ok(vec_with_capacity(capacity))
    }
}

#[cfg(not(nightly))]
impl<T> TursoFromIterator<T> for Vec<T> {
    #[inline(always)]
    fn try_from_iter<I>(iter: I) -> Result<Self, TryReserveError>
    where
        I: IntoIterator<Item = T>,
    {
        Ok(iter.into_iter().collect())
    }

    #[inline(always)]
    fn try_extend<I>(&mut self, iter: I) -> Result<(), TryReserveError>
    where
        I: IntoIterator<Item = T>,
    {
        self.extend(iter);
        Ok(())
    }
}

#[cfg(not(nightly))]
impl<T: Clone> TursoSliceExt<T> for [T] {
    #[inline(always)]
    fn try_to_vec(&self) -> Result<Vec<T>, TryReserveError> {
        let mut values = <Vec<T> as TursoTryWithCapacityExt>::try_with_capacity_ext(self.len())?;
        values.extend_from_slice(self);
        Ok(values)
    }
}

#[cfg(not(nightly))]
impl<T: TryClone> TryClone for Vec<T>
where
    TryReserveError: From<T::Error>,
{
    type Error = TryReserveError;

    #[inline(always)]
    fn try_clone(&self) -> Result<Self, Self::Error> {
        // Same `TryClone` bound as the nightly impl so code compiles
        // identically on both cfgs. Elements clone through `TryClone`; on
        // stable this is Clone-forwarded for std-pinned types anyway.
        //
        // Write into spare capacity directly instead of `push`: the
        // per-element capacity check defeats vectorization (6x slower for
        // Copy elements, 1.7x for non-Copy in alloc_collections benches).
        // The guard keeps `len` covering exactly the elements written, so an
        // `Err` from an element clone (or a panic) drops a consistent vec.
        struct SetLenOnDrop<'a, T> {
            vec: &'a mut Vec<T>,
            len: usize,
        }
        impl<T> Drop for SetLenOnDrop<'_, T> {
            #[inline]
            fn drop(&mut self) {
                unsafe {
                    self.vec.set_len(self.len);
                }
            }
        }

        let mut cloned = <Self as TursoTryWithCapacityExt>::try_with_capacity_ext(self.len())?;
        let ptr = cloned.as_mut_ptr();
        let mut guard = SetLenOnDrop {
            vec: &mut cloned,
            len: 0,
        };
        for item in self {
            let item = item.try_clone()?;
            unsafe {
                std::ptr::write(ptr.add(guard.len), item);
            }
            guard.len += 1;
        }
        drop(guard);
        Ok(cloned)
    }
}
