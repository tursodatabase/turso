use std::hash::{BuildHasher, Hash};

use super::{TryClone, TursoAllocExt, TursoFromIterator, TursoHashSetExt, TursoTryWithCapacityExt};
use crate::alloc::{HashSet, TryReserveError};

fn hash_set_with_hasher<T, S>(hasher: S) -> HashSet<T, S> {
    std::collections::HashSet::with_hasher(hasher)
}

impl<T, S> TursoAllocExt for HashSet<T, S>
where
    S: Default,
{
    fn new() -> Self {
        hash_set_with_hasher(S::default())
    }
}

impl<T, S> TursoHashSetExt<T> for HashSet<T, S>
where
    T: Eq + Hash,
    S: BuildHasher,
{
    fn try_insert(&mut self, value: T) -> Result<bool, TryReserveError> {
        self.try_reserve(1)?;
        Ok(self.insert(value))
    }
}

impl<T, S> TursoTryWithCapacityExt for HashSet<T, S>
where
    T: Eq + Hash,
    S: BuildHasher + Default,
{
    fn try_with_capacity(capacity: usize) -> Result<Self, TryReserveError> {
        let mut values = <Self as TursoAllocExt>::new();
        values.try_reserve(capacity)?;
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

    fn try_extend<I>(&mut self, iter: I) -> Result<(), TryReserveError>
    where
        I: IntoIterator<Item = T>,
    {
        let iter = iter.into_iter();
        let (lower, upper) = iter.size_hint();
        self.try_reserve(upper.unwrap_or(lower))?;
        for value in iter {
            TursoHashSetExt::try_insert(self, value)?;
        }
        Ok(())
    }
}

impl<T, S> TryClone for HashSet<T, S>
where
    T: Clone + Eq + Hash,
    S: BuildHasher + Clone,
{
    type Error = TryReserveError;

    fn try_clone(&self) -> Result<Self, Self::Error> {
        let mut cloned = Self::with_hasher(self.hasher().clone());
        cloned.try_reserve(self.len())?;
        cloned.extend(self.iter().cloned());
        Ok(cloned)
    }
}
