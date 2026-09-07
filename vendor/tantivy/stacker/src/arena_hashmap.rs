use super::{Addr, MemoryArena};
use crate::shared_arena_hashmap::SharedArenaHashMap;

/// Customized `HashMap` with `&[u8]` keys
///
/// Its main particularity is that rather than storing its
/// keys in the heap, keys are stored in a memory arena
/// inline with the values.
///
/// The quirky API has the benefit of avoiding
/// the computation of the hash of the key twice,
/// or copying the key as long as there is no insert.
///
/// ArenaHashMap is like SharedArenaHashMap but takes ownership
/// of the memory arena. The memory arena stores the serialized
/// keys and values.
pub struct ArenaHashMap {
    shared_arena_hashmap: SharedArenaHashMap,
    pub memory_arena: MemoryArena,
}

impl Default for ArenaHashMap {
    fn default() -> Self {
        ArenaHashMap::with_capacity(4)
    }
}

impl ArenaHashMap {
    pub fn with_capacity(table_size: usize) -> ArenaHashMap {
        let memory_arena = MemoryArena::default();

        ArenaHashMap {
            shared_arena_hashmap: SharedArenaHashMap::with_capacity(table_size),
            memory_arena,
        }
    }

    #[inline]
    pub fn read<Item: Copy + 'static>(&self, addr: Addr) -> Item {
        self.memory_arena.read(addr)
    }

    #[inline]
    pub fn mem_usage(&self) -> usize {
        self.shared_arena_hashmap.mem_usage() + self.memory_arena.mem_usage()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.shared_arena_hashmap.is_empty()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.shared_arena_hashmap.len()
    }

    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = (&[u8], Addr)> {
        self.shared_arena_hashmap.iter(&self.memory_arena)
    }

    /// Get a value associated to a key.
    #[inline]
    pub fn get<V>(&self, key: &[u8]) -> Option<V>
    where V: Copy + 'static {
        self.shared_arena_hashmap.get(key, &self.memory_arena)
    }

    /// `update` create a new entry for a given key if it does not exist
    /// or updates the existing entry.
    ///
    /// The actual logic for this update is define in the `updater`
    /// argument.
    ///
    /// If the key is not present, `updater` will receive `None` and
    /// will be in charge of returning a default value.
    /// If the key already as an associated value, then it will be passed
    /// `Some(previous_value)`.
    #[inline]
    pub fn mutate_or_create<V>(&mut self, key: &[u8], updater: impl FnMut(Option<V>) -> V)
    where V: Copy + 'static {
        self.shared_arena_hashmap
            .mutate_or_create(key, &mut self.memory_arena, updater);
    }
}

#[cfg(test)]
mod tests {

    use std::collections::HashMap;

    use super::ArenaHashMap;

    #[test]
    fn test_hash_map() {
        let mut hash_map: ArenaHashMap = ArenaHashMap::default();
        hash_map.mutate_or_create(b"abc", |opt_val: Option<u32>| {
            assert_eq!(opt_val, None);
            3u32
        });
        hash_map.mutate_or_create(b"abcd", |opt_val: Option<u32>| {
            assert_eq!(opt_val, None);
            4u32
        });
        hash_map.mutate_or_create(b"abc", |opt_val: Option<u32>| {
            assert_eq!(opt_val, Some(3u32));
            5u32
        });
        let mut vanilla_hash_map = HashMap::new();
        let iter_values = hash_map.iter();
        for (key, addr) in iter_values {
            let val: u32 = hash_map.memory_arena.read(addr);
            vanilla_hash_map.insert(key.to_owned(), val);
        }
        assert_eq!(vanilla_hash_map.len(), 2);
    }
    #[test]
    fn test_empty_hashmap() {
        let hash_map: ArenaHashMap = ArenaHashMap::default();
        assert_eq!(hash_map.get::<u32>(b"abc"), None);
    }

    #[test]
    fn test_many_terms() {
        let mut terms: Vec<String> = (0..20_000).map(|val| val.to_string()).collect();
        let mut hash_map: ArenaHashMap = ArenaHashMap::default();
        for term in terms.iter() {
            hash_map.mutate_or_create(term.as_bytes(), |_opt_val: Option<u32>| 5u32);
        }
        let mut terms_back: Vec<String> = hash_map
            .iter()
            .map(|(bytes, _)| String::from_utf8(bytes.to_vec()).unwrap())
            .collect();
        terms_back.sort();
        terms.sort();

        for pos in 0..terms.len() {
            assert_eq!(terms[pos], terms_back[pos]);
        }
    }
}
