use std::hash::{BuildHasher, Hash};

use super::{TryClone, TursoAllocExt, TursoFromIterator, TursoHashMapExt, TursoTryWithCapacityExt};
use crate::alloc::{HashMap, TryReserveError};

fn hash_map_with_hasher<K, V, S>(hasher: S) -> HashMap<K, V, S> {
    std::collections::HashMap::with_hasher(hasher)
}

impl<K, V, S> TursoAllocExt for HashMap<K, V, S>
where
    S: Default,
{
    fn new() -> Self {
        hash_map_with_hasher(S::default())
    }
}

impl<K, V, S> TursoHashMapExt<K, V> for HashMap<K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    fn try_insert(&mut self, key: K, value: V) -> Result<Option<V>, TryReserveError> {
        self.try_reserve(1)?;
        Ok(self.insert(key, value))
    }
}

impl<K, V, S> TursoTryWithCapacityExt for HashMap<K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher + Default,
{
    fn try_with_capacity(capacity: usize) -> Result<Self, TryReserveError> {
        let mut values = <Self as TursoAllocExt>::new();
        values.try_reserve(capacity)?;
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
        if upper.is_some() {
            for (key, value) in iter {
                values.insert(key, value);
            }
        } else {
            for (key, value) in iter {
                TursoHashMapExt::try_insert(&mut values, key, value)?;
            }
        }
        Ok(values)
    }

    fn try_extend<I>(&mut self, iter: I) -> Result<(), TryReserveError>
    where
        I: IntoIterator<Item = (K, V)>,
    {
        let iter = iter.into_iter();
        let (lower, upper) = iter.size_hint();
        self.try_reserve(upper.unwrap_or(lower))?;
        if upper.is_some() {
            for (key, value) in iter {
                self.insert(key, value);
            }
        } else {
            for (key, value) in iter {
                TursoHashMapExt::try_insert(self, key, value)?;
            }
        }
        Ok(())
    }
}

impl<K, V, S> TryClone for HashMap<K, V, S>
where
    K: Clone + Eq + Hash,
    V: Clone,
    S: BuildHasher + Clone,
{
    type Error = TryReserveError;

    fn try_clone(&self) -> Result<Self, Self::Error> {
        let mut cloned = Self::with_hasher(self.hasher().clone());
        cloned.try_reserve(self.len())?;
        // TODO: have a `TryClone` boundary for K and V here instead of `Clone`
        cloned.extend(self.iter().map(|(key, value)| (key.clone(), value.clone())));
        Ok(cloned)
    }
}
