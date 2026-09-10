use std::collections::BinaryHeap;
use std::fmt::Debug;
use std::hash::Hash;

use rustc_hash::FxHashMap;
use smallvec::SmallVec;

use crate::TantivyError;

/// Map backed by a hash map for fast access and a binary heap to track the
/// highest key. The key is an array of fixed size S.
#[derive(Clone, Debug)]
struct ArrayHeapMap<K: Ord, V, const S: usize> {
    pub(crate) buckets: FxHashMap<[K; S], V>,
    pub(crate) heap: BinaryHeap<[K; S]>,
}

impl<K: Ord, V, const S: usize> Default for ArrayHeapMap<K, V, S> {
    fn default() -> Self {
        ArrayHeapMap {
            buckets: FxHashMap::default(),
            heap: BinaryHeap::default(),
        }
    }
}

impl<K: Eq + Hash + Clone + Ord, V, const S: usize> ArrayHeapMap<K, V, S> {
    /// Panics if the length of `key` is not S.
    fn get_or_insert_with<F: FnOnce() -> V>(&mut self, key: &[K], f: F) -> &mut V {
        let key_array: &[K; S] = key.try_into().expect("Key length mismatch");
        self.buckets.entry(key_array.clone()).or_insert_with(|| {
            self.heap.push(key_array.clone());
            f()
        })
    }

    /// Panics if the length of `key` is not S.
    fn get_mut(&mut self, key: &[K]) -> Option<&mut V> {
        let key_array: &[K; S] = key.try_into().expect("Key length mismatch");
        self.buckets.get_mut(key_array)
    }

    fn peek_highest(&self) -> Option<&[K]> {
        self.heap.peek().map(|k_array| k_array.as_slice())
    }

    fn evict_highest(&mut self) {
        if let Some(highest) = self.heap.pop() {
            self.buckets.remove(&highest);
        }
    }

    fn memory_consumption(&self) -> u64 {
        let key_size = std::mem::size_of::<[K; S]>();
        let map_size = (key_size + std::mem::size_of::<V>()) * self.buckets.capacity();
        let heap_size = key_size * self.heap.capacity();
        (map_size + heap_size) as u64
    }
}

impl<K: Copy + Ord + Clone + 'static, V: 'static, const S: usize> ArrayHeapMap<K, V, S> {
    fn into_iter(self) -> Box<dyn Iterator<Item = (SmallVec<[K; MAX_DYN_ARRAY_SIZE]>, V)>> {
        Box::new(
            self.buckets
                .into_iter()
                .map(|(k, v)| (SmallVec::from_slice(&k), v)),
        )
    }
}

pub(super) const MAX_DYN_ARRAY_SIZE: usize = 16;
const MAX_DYN_ARRAY_SIZE_PLUS_ONE: usize = MAX_DYN_ARRAY_SIZE + 1;

/// A map optimized for memory footprint, fast access and efficient eviction of
/// the highest key.
///
/// Keys are inlined arrays of size 1 to [MAX_DYN_ARRAY_SIZE] but for a given
/// instance the key size is fixed. This allows to avoid heap allocations for the
/// keys.
#[derive(Clone, Debug)]
pub(super) struct DynArrayHeapMap<K: Ord, V>(DynArrayHeapMapInner<K, V>);

/// Wrapper around ArrayHeapMap to dynamically dispatch on the array size.
#[derive(Clone, Debug)]
enum DynArrayHeapMapInner<K: Ord, V> {
    Dim1(ArrayHeapMap<K, V, 1>),
    Dim2(ArrayHeapMap<K, V, 2>),
    Dim3(ArrayHeapMap<K, V, 3>),
    Dim4(ArrayHeapMap<K, V, 4>),
    Dim5(ArrayHeapMap<K, V, 5>),
    Dim6(ArrayHeapMap<K, V, 6>),
    Dim7(ArrayHeapMap<K, V, 7>),
    Dim8(ArrayHeapMap<K, V, 8>),
    Dim9(ArrayHeapMap<K, V, 9>),
    Dim10(ArrayHeapMap<K, V, 10>),
    Dim11(ArrayHeapMap<K, V, 11>),
    Dim12(ArrayHeapMap<K, V, 12>),
    Dim13(ArrayHeapMap<K, V, 13>),
    Dim14(ArrayHeapMap<K, V, 14>),
    Dim15(ArrayHeapMap<K, V, 15>),
    Dim16(ArrayHeapMap<K, V, 16>),
}

impl<K: Ord, V> DynArrayHeapMap<K, V> {
    /// Creates a new heap map with dynamic array keys of size `key_dimension`.
    pub(super) fn try_new(key_dimension: usize) -> crate::Result<Self> {
        let inner = match key_dimension {
            0 => {
                return Err(TantivyError::InvalidArgument(
                    "DynArrayHeapMap dimension must be at least 1".to_string(),
                ))
            }
            1 => DynArrayHeapMapInner::Dim1(ArrayHeapMap::default()),
            2 => DynArrayHeapMapInner::Dim2(ArrayHeapMap::default()),
            3 => DynArrayHeapMapInner::Dim3(ArrayHeapMap::default()),
            4 => DynArrayHeapMapInner::Dim4(ArrayHeapMap::default()),
            5 => DynArrayHeapMapInner::Dim5(ArrayHeapMap::default()),
            6 => DynArrayHeapMapInner::Dim6(ArrayHeapMap::default()),
            7 => DynArrayHeapMapInner::Dim7(ArrayHeapMap::default()),
            8 => DynArrayHeapMapInner::Dim8(ArrayHeapMap::default()),
            9 => DynArrayHeapMapInner::Dim9(ArrayHeapMap::default()),
            10 => DynArrayHeapMapInner::Dim10(ArrayHeapMap::default()),
            11 => DynArrayHeapMapInner::Dim11(ArrayHeapMap::default()),
            12 => DynArrayHeapMapInner::Dim12(ArrayHeapMap::default()),
            13 => DynArrayHeapMapInner::Dim13(ArrayHeapMap::default()),
            14 => DynArrayHeapMapInner::Dim14(ArrayHeapMap::default()),
            15 => DynArrayHeapMapInner::Dim15(ArrayHeapMap::default()),
            16 => DynArrayHeapMapInner::Dim16(ArrayHeapMap::default()),
            MAX_DYN_ARRAY_SIZE_PLUS_ONE.. => {
                return Err(TantivyError::InvalidArgument(format!(
                    "DynArrayHeapMap supports maximum {MAX_DYN_ARRAY_SIZE} dimensions, got \
                     {key_dimension}",
                )))
            }
        };
        Ok(DynArrayHeapMap(inner))
    }

    /// Number of elements in the map. This is not the dimension of the keys.
    pub(super) fn size(&self) -> usize {
        match &self.0 {
            DynArrayHeapMapInner::Dim1(map) => map.buckets.len(),
            DynArrayHeapMapInner::Dim2(map) => map.buckets.len(),
            DynArrayHeapMapInner::Dim3(map) => map.buckets.len(),
            DynArrayHeapMapInner::Dim4(map) => map.buckets.len(),
            DynArrayHeapMapInner::Dim5(map) => map.buckets.len(),
            DynArrayHeapMapInner::Dim6(map) => map.buckets.len(),
            DynArrayHeapMapInner::Dim7(map) => map.buckets.len(),
            DynArrayHeapMapInner::Dim8(map) => map.buckets.len(),
            DynArrayHeapMapInner::Dim9(map) => map.buckets.len(),
            DynArrayHeapMapInner::Dim10(map) => map.buckets.len(),
            DynArrayHeapMapInner::Dim11(map) => map.buckets.len(),
            DynArrayHeapMapInner::Dim12(map) => map.buckets.len(),
            DynArrayHeapMapInner::Dim13(map) => map.buckets.len(),
            DynArrayHeapMapInner::Dim14(map) => map.buckets.len(),
            DynArrayHeapMapInner::Dim15(map) => map.buckets.len(),
            DynArrayHeapMapInner::Dim16(map) => map.buckets.len(),
        }
    }
}

impl<K: Ord + Hash + Clone, V> DynArrayHeapMap<K, V> {
    /// Get a mutable reference to the value corresponding to `key` or inserts a new
    /// value created by calling `f`.
    ///
    /// Panics if the length of `key` does not match the key dimension of the map.
    pub(super) fn get_or_insert_with<F: FnOnce() -> V>(&mut self, key: &[K], f: F) -> &mut V {
        match &mut self.0 {
            DynArrayHeapMapInner::Dim1(map) => map.get_or_insert_with(key, f),
            DynArrayHeapMapInner::Dim2(map) => map.get_or_insert_with(key, f),
            DynArrayHeapMapInner::Dim3(map) => map.get_or_insert_with(key, f),
            DynArrayHeapMapInner::Dim4(map) => map.get_or_insert_with(key, f),
            DynArrayHeapMapInner::Dim5(map) => map.get_or_insert_with(key, f),
            DynArrayHeapMapInner::Dim6(map) => map.get_or_insert_with(key, f),
            DynArrayHeapMapInner::Dim7(map) => map.get_or_insert_with(key, f),
            DynArrayHeapMapInner::Dim8(map) => map.get_or_insert_with(key, f),
            DynArrayHeapMapInner::Dim9(map) => map.get_or_insert_with(key, f),
            DynArrayHeapMapInner::Dim10(map) => map.get_or_insert_with(key, f),
            DynArrayHeapMapInner::Dim11(map) => map.get_or_insert_with(key, f),
            DynArrayHeapMapInner::Dim12(map) => map.get_or_insert_with(key, f),
            DynArrayHeapMapInner::Dim13(map) => map.get_or_insert_with(key, f),
            DynArrayHeapMapInner::Dim14(map) => map.get_or_insert_with(key, f),
            DynArrayHeapMapInner::Dim15(map) => map.get_or_insert_with(key, f),
            DynArrayHeapMapInner::Dim16(map) => map.get_or_insert_with(key, f),
        }
    }

    /// Returns a mutable reference to the value corresponding to `key`.
    ///
    /// Panics if the length of `key` does not match the key dimension of the map.
    pub fn get_mut(&mut self, key: &[K]) -> Option<&mut V> {
        match &mut self.0 {
            DynArrayHeapMapInner::Dim1(map) => map.get_mut(key),
            DynArrayHeapMapInner::Dim2(map) => map.get_mut(key),
            DynArrayHeapMapInner::Dim3(map) => map.get_mut(key),
            DynArrayHeapMapInner::Dim4(map) => map.get_mut(key),
            DynArrayHeapMapInner::Dim5(map) => map.get_mut(key),
            DynArrayHeapMapInner::Dim6(map) => map.get_mut(key),
            DynArrayHeapMapInner::Dim7(map) => map.get_mut(key),
            DynArrayHeapMapInner::Dim8(map) => map.get_mut(key),
            DynArrayHeapMapInner::Dim9(map) => map.get_mut(key),
            DynArrayHeapMapInner::Dim10(map) => map.get_mut(key),
            DynArrayHeapMapInner::Dim11(map) => map.get_mut(key),
            DynArrayHeapMapInner::Dim12(map) => map.get_mut(key),
            DynArrayHeapMapInner::Dim13(map) => map.get_mut(key),
            DynArrayHeapMapInner::Dim14(map) => map.get_mut(key),
            DynArrayHeapMapInner::Dim15(map) => map.get_mut(key),
            DynArrayHeapMapInner::Dim16(map) => map.get_mut(key),
        }
    }

    /// Returns a reference to the highest key in the map.
    pub(super) fn peek_highest(&self) -> Option<&[K]> {
        match &self.0 {
            DynArrayHeapMapInner::Dim1(map) => map.peek_highest(),
            DynArrayHeapMapInner::Dim2(map) => map.peek_highest(),
            DynArrayHeapMapInner::Dim3(map) => map.peek_highest(),
            DynArrayHeapMapInner::Dim4(map) => map.peek_highest(),
            DynArrayHeapMapInner::Dim5(map) => map.peek_highest(),
            DynArrayHeapMapInner::Dim6(map) => map.peek_highest(),
            DynArrayHeapMapInner::Dim7(map) => map.peek_highest(),
            DynArrayHeapMapInner::Dim8(map) => map.peek_highest(),
            DynArrayHeapMapInner::Dim9(map) => map.peek_highest(),
            DynArrayHeapMapInner::Dim10(map) => map.peek_highest(),
            DynArrayHeapMapInner::Dim11(map) => map.peek_highest(),
            DynArrayHeapMapInner::Dim12(map) => map.peek_highest(),
            DynArrayHeapMapInner::Dim13(map) => map.peek_highest(),
            DynArrayHeapMapInner::Dim14(map) => map.peek_highest(),
            DynArrayHeapMapInner::Dim15(map) => map.peek_highest(),
            DynArrayHeapMapInner::Dim16(map) => map.peek_highest(),
        }
    }

    /// Removes the entry with the highest key from the map.
    pub(super) fn evict_highest(&mut self) {
        match &mut self.0 {
            DynArrayHeapMapInner::Dim1(map) => map.evict_highest(),
            DynArrayHeapMapInner::Dim2(map) => map.evict_highest(),
            DynArrayHeapMapInner::Dim3(map) => map.evict_highest(),
            DynArrayHeapMapInner::Dim4(map) => map.evict_highest(),
            DynArrayHeapMapInner::Dim5(map) => map.evict_highest(),
            DynArrayHeapMapInner::Dim6(map) => map.evict_highest(),
            DynArrayHeapMapInner::Dim7(map) => map.evict_highest(),
            DynArrayHeapMapInner::Dim8(map) => map.evict_highest(),
            DynArrayHeapMapInner::Dim9(map) => map.evict_highest(),
            DynArrayHeapMapInner::Dim10(map) => map.evict_highest(),
            DynArrayHeapMapInner::Dim11(map) => map.evict_highest(),
            DynArrayHeapMapInner::Dim12(map) => map.evict_highest(),
            DynArrayHeapMapInner::Dim13(map) => map.evict_highest(),
            DynArrayHeapMapInner::Dim14(map) => map.evict_highest(),
            DynArrayHeapMapInner::Dim15(map) => map.evict_highest(),
            DynArrayHeapMapInner::Dim16(map) => map.evict_highest(),
        }
    }

    pub(crate) fn memory_consumption(&self) -> u64 {
        match &self.0 {
            DynArrayHeapMapInner::Dim1(map) => map.memory_consumption(),
            DynArrayHeapMapInner::Dim2(map) => map.memory_consumption(),
            DynArrayHeapMapInner::Dim3(map) => map.memory_consumption(),
            DynArrayHeapMapInner::Dim4(map) => map.memory_consumption(),
            DynArrayHeapMapInner::Dim5(map) => map.memory_consumption(),
            DynArrayHeapMapInner::Dim6(map) => map.memory_consumption(),
            DynArrayHeapMapInner::Dim7(map) => map.memory_consumption(),
            DynArrayHeapMapInner::Dim8(map) => map.memory_consumption(),
            DynArrayHeapMapInner::Dim9(map) => map.memory_consumption(),
            DynArrayHeapMapInner::Dim10(map) => map.memory_consumption(),
            DynArrayHeapMapInner::Dim11(map) => map.memory_consumption(),
            DynArrayHeapMapInner::Dim12(map) => map.memory_consumption(),
            DynArrayHeapMapInner::Dim13(map) => map.memory_consumption(),
            DynArrayHeapMapInner::Dim14(map) => map.memory_consumption(),
            DynArrayHeapMapInner::Dim15(map) => map.memory_consumption(),
            DynArrayHeapMapInner::Dim16(map) => map.memory_consumption(),
        }
    }
}

impl<K: Ord + Clone + Copy + 'static, V: 'static> DynArrayHeapMap<K, V> {
    /// Turns this map into an iterator over key-value pairs.
    pub fn into_iter(self) -> impl Iterator<Item = (SmallVec<[K; MAX_DYN_ARRAY_SIZE]>, V)> {
        match self.0 {
            DynArrayHeapMapInner::Dim1(map) => map.into_iter(),
            DynArrayHeapMapInner::Dim2(map) => map.into_iter(),
            DynArrayHeapMapInner::Dim3(map) => map.into_iter(),
            DynArrayHeapMapInner::Dim4(map) => map.into_iter(),
            DynArrayHeapMapInner::Dim5(map) => map.into_iter(),
            DynArrayHeapMapInner::Dim6(map) => map.into_iter(),
            DynArrayHeapMapInner::Dim7(map) => map.into_iter(),
            DynArrayHeapMapInner::Dim8(map) => map.into_iter(),
            DynArrayHeapMapInner::Dim9(map) => map.into_iter(),
            DynArrayHeapMapInner::Dim10(map) => map.into_iter(),
            DynArrayHeapMapInner::Dim11(map) => map.into_iter(),
            DynArrayHeapMapInner::Dim12(map) => map.into_iter(),
            DynArrayHeapMapInner::Dim13(map) => map.into_iter(),
            DynArrayHeapMapInner::Dim14(map) => map.into_iter(),
            DynArrayHeapMapInner::Dim15(map) => map.into_iter(),
            DynArrayHeapMapInner::Dim16(map) => map.into_iter(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dyn_array_heap_map() {
        let mut map = DynArrayHeapMap::<u32, &str>::try_new(2).unwrap();
        // insert
        let key1 = [1u32, 2u32];
        let key2 = [2u32, 1u32];
        map.get_or_insert_with(&key1, || "a");
        map.get_or_insert_with(&key2, || "b");
        assert_eq!(map.size(), 2);

        // evict highest
        assert_eq!(map.peek_highest(), Some(&key2[..]));
        map.evict_highest();
        assert_eq!(map.size(), 1);
        assert_eq!(map.peek_highest(), Some(&key1[..]));

        // into_iter
        let mut iter = map.into_iter();
        let (k, v) = iter.next().unwrap();
        assert_eq!(k.as_slice(), &key1);
        assert_eq!(v, "a");
        assert_eq!(iter.next(), None);
    }
}
