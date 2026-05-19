use std::hash::{BuildHasher, Hash};

use rustc_hash::FxBuildHasher;

use super::{TryClone, TursoFromIterator, TursoHashMapExt, TursoTryWithCapacityExt};
use crate::alloc::{HashMap, TryReserveError};

impl<K, V, S> TursoHashMapExt<K, V> for HashMap<K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    #[inline(always)]
    fn try_insert(&mut self, key: K, value: V) -> Result<Option<V>, TryReserveError> {
        Ok(self.insert(key, value))
    }
}

impl<K, V> TursoTryWithCapacityExt for HashMap<K, V>
where
    K: Eq + Hash,
{
    #[inline(always)]
    fn try_with_capacity(capacity: usize) -> Result<Self, TryReserveError> {
        Ok(HashMap::with_capacity_and_hasher(capacity, FxBuildHasher))
    }
}

impl<K, V> TursoFromIterator<(K, V)> for HashMap<K, V>
where
    K: Eq + Hash,
{
    #[inline(always)]
    fn try_from_iter<I>(iter: I) -> Result<Self, TryReserveError>
    where
        I: IntoIterator<Item = (K, V)>,
    {
        Ok(iter.into_iter().collect())
    }

    #[inline(always)]
    fn try_extend<I>(&mut self, iter: I) -> Result<(), TryReserveError>
    where
        I: IntoIterator<Item = (K, V)>,
    {
        self.extend(iter);
        Ok(())
    }
}

impl<K, V> TryClone for HashMap<K, V>
where
    K: Clone + Eq + Hash,
    V: Clone,
{
    type Error = TryReserveError;

    #[inline(always)]
    fn try_clone(&self) -> Result<Self, Self::Error> {
        let mut cloned = Self::with_hasher(self.hasher().clone());
        cloned.try_reserve(self.len())?;
        // TODO: have a `TryClone` boundary for K and V here instead of `Clone`
        cloned.extend(self.iter().map(|(key, value)| (key.clone(), value.clone())));
        Ok(cloned)
    }
}
