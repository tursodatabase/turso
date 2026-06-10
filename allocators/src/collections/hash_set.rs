use std::hash::{BuildHasher, Hash};

use rustc_hash::FxBuildHasher;

use super::{TryClone, TursoFromIterator, TursoHashSetExt, TursoTryWithCapacityExt};
use crate::{HashSet, TryReserveError};

impl<T, S> TursoHashSetExt<T> for HashSet<T, S>
where
    T: Eq + Hash,
    S: BuildHasher,
{
    #[inline(always)]
    fn try_insert(&mut self, value: T) -> Result<bool, TryReserveError> {
        Ok(self.insert(value))
    }
}

impl<T> TursoTryWithCapacityExt for HashSet<T>
where
    T: Eq + Hash,
{
    #[inline(always)]
    fn try_with_capacity_ext(capacity: usize) -> Result<Self, TryReserveError> {
        Ok(HashSet::with_capacity_and_hasher(capacity, FxBuildHasher))
    }
}

impl<T> TursoFromIterator<T> for HashSet<T>
where
    T: Eq + Hash,
{
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

impl<T, S> TryClone for HashSet<T, S>
where
    T: Clone + Eq + Hash,
    S: BuildHasher + Clone,
{
    type Error = TryReserveError;

    #[inline(always)]
    fn try_clone(&self) -> Result<Self, Self::Error> {
        let mut cloned = Self::with_hasher(self.hasher().clone());
        cloned.try_reserve(self.len())?;
        cloned.extend(self.iter().cloned());
        Ok(cloned)
    }
}
