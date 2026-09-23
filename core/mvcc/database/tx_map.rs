//! Map from transaction id to a value, for `MvStore::txs` and
//! `MvStore::finalized_tx_states`.
//!
//! Transaction ids come from one counter, so the transactions that run at the same
//! time fall into different shards. A lookup locks one shard for reading. Each shard
//! holds only a few entries, so a linear search is enough. A bit mask tells which
//! shards hold entries, so a walk over all entries skips the empty shards.

use crate::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use crate::sync::RwLock;
use crossbeam_utils::CachePadded;

use super::TxID;

const SHARDS: usize = 64;

pub struct TxMap<V> {
    shards: Box<[CachePadded<RwLock<Vec<(TxID, V)>>>]>,
    /// Bit `i` is set while shard `i` holds entries. Only a writer that holds the lock
    /// of shard `i` changes bit `i`.
    occupied: AtomicU64,
    len: AtomicUsize,
}

pub struct TxEntry<V> {
    key: TxID,
    value: V,
}

impl<V> TxEntry<V> {
    pub fn key(&self) -> &TxID {
        &self.key
    }

    pub fn value(&self) -> &V {
        &self.value
    }
}

impl<V> std::fmt::Debug for TxMap<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TxMap").field("len", &self.len()).finish()
    }
}

impl<V: Clone> Default for TxMap<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V> TxMap<V> {
    pub fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<V: Clone> TxMap<V> {
    pub fn new() -> Self {
        Self {
            shards: (0..SHARDS)
                .map(|_| CachePadded::new(RwLock::new(Vec::with_capacity(4))))
                .collect(),
            occupied: AtomicU64::new(0),
            len: AtomicUsize::new(0),
        }
    }

    pub fn get(&self, key: &TxID) -> Option<TxEntry<V>> {
        let shard = self.shard(*key).read();
        shard.iter().find(|(k, _)| k == key).map(|(k, v)| TxEntry {
            key: *k,
            value: v.clone(),
        })
    }

    /// Inserts `value` for `key`. An existing value for `key` is replaced.
    pub fn insert(&self, key: TxID, value: V) -> TxEntry<V> {
        let index = Self::shard_index(key);
        let mut shard = self.shards[index].write();
        match shard.iter_mut().find(|(k, _)| *k == key) {
            Some(slot) => slot.1 = value.clone(),
            None => {
                shard.push((key, value.clone()));
                if shard.len() == 1 {
                    self.occupied.fetch_or(1 << index, Ordering::Release);
                }
                self.len.fetch_add(1, Ordering::Relaxed);
            }
        }
        TxEntry { key, value }
    }

    #[cfg(test)]
    pub fn contains_key(&self, key: &TxID) -> bool {
        self.shard(*key).read().iter().any(|(k, _)| k == key)
    }

    #[cfg(test)]
    pub fn try_insert(
        &self,
        key: TxID,
        value: V,
    ) -> Result<TxEntry<V>, crate::alloc::TryReserveError> {
        Ok(self.insert(key, value))
    }

    pub fn remove(&self, key: &TxID) -> Option<TxEntry<V>> {
        let index = Self::shard_index(*key);
        let mut shard = self.shards[index].write();
        let position = shard.iter().position(|(k, _)| k == key)?;
        let (key, value) = shard.swap_remove(position);
        if shard.is_empty() {
            self.occupied.fetch_and(!(1 << index), Ordering::Release);
        }
        self.len.fetch_sub(1, Ordering::Relaxed);
        Some(TxEntry { key, value })
    }

    /// Entries in no particular order. An entry that is inserted or removed during the
    /// call can be seen or not.
    pub fn iter(&self) -> std::vec::IntoIter<TxEntry<V>> {
        let mut entries = Vec::with_capacity(self.len());
        let mut occupied = self.occupied.load(Ordering::Acquire);
        while occupied != 0 {
            let index = occupied.trailing_zeros() as usize;
            occupied &= occupied - 1;
            entries.extend(self.shards[index].read().iter().map(|(k, v)| TxEntry {
                key: *k,
                value: v.clone(),
            }));
        }
        entries.into_iter()
    }

    fn shard(&self, key: TxID) -> &RwLock<Vec<(TxID, V)>> {
        &self.shards[Self::shard_index(key)]
    }

    fn shard_index(key: TxID) -> usize {
        (key as usize) % SHARDS
    }
}

impl<V: Clone> FromIterator<(TxID, V)> for TxMap<V> {
    fn from_iter<I: IntoIterator<Item = (TxID, V)>>(iter: I) -> Self {
        let map = Self::new();
        for (key, value) in iter {
            map.insert(key, value);
        }
        map
    }
}
