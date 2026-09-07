use std::io::Write;
use std::{fmt, io};

use ownedbytes::OwnedBytes;

use crate::ByteCount;

#[derive(Clone, Copy, Eq, PartialEq)]
pub struct TinySet(u64);

impl fmt::Debug for TinySet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.into_iter().collect::<Vec<u32>>().fmt(f)
    }
}

pub struct TinySetIterator(TinySet);
impl Iterator for TinySetIterator {
    type Item = u32;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.0.pop_lowest()
    }
}

impl IntoIterator for TinySet {
    type Item = u32;
    type IntoIter = TinySetIterator;
    fn into_iter(self) -> Self::IntoIter {
        TinySetIterator(self)
    }
}

impl TinySet {
    pub fn serialize<T: Write>(&self, writer: &mut T) -> io::Result<()> {
        writer.write_all(self.0.to_le_bytes().as_ref())
    }

    pub fn into_bytes(self) -> [u8; 8] {
        self.0.to_le_bytes()
    }

    #[inline]
    pub fn deserialize(data: [u8; 8]) -> Self {
        let val: u64 = u64::from_le_bytes(data);
        TinySet(val)
    }

    /// Returns an empty `TinySet`.
    #[inline]
    pub fn empty() -> TinySet {
        TinySet(0u64)
    }

    /// Returns a full `TinySet`.
    #[inline]
    pub fn full() -> TinySet {
        TinySet::empty().complement()
    }

    pub fn clear(&mut self) {
        self.0 = 0u64;
    }

    /// Returns the complement of the set in `[0, 64[`.
    ///
    /// Careful on making this function public, as it will break the padding handling in the last
    /// bucket.
    #[inline]
    fn complement(self) -> TinySet {
        TinySet(!self.0)
    }

    /// Returns true iff the `TinySet` contains the element `el`.
    #[inline]
    pub fn contains(self, el: u32) -> bool {
        !self.intersect(TinySet::singleton(el)).is_empty()
    }

    /// Returns the number of elements in the TinySet.
    #[inline]
    pub fn len(self) -> u32 {
        self.0.count_ones()
    }

    /// Returns the intersection of `self` and `other`
    #[inline]
    #[must_use]
    pub fn intersect(self, other: TinySet) -> TinySet {
        TinySet(self.0 & other.0)
    }

    /// Creates a new `TinySet` containing only one element
    /// within `[0; 64[`
    #[inline]
    pub fn singleton(el: u32) -> TinySet {
        TinySet(1u64 << u64::from(el))
    }

    /// Insert a new element within [0..64)
    #[inline]
    #[must_use]
    pub fn insert(self, el: u32) -> TinySet {
        self.union(TinySet::singleton(el))
    }

    /// Removes an element within [0..64)
    #[inline]
    #[must_use]
    pub fn remove(self, el: u32) -> TinySet {
        self.intersect(TinySet::singleton(el).complement())
    }

    /// Insert a new element within [0..64)
    ///
    /// returns true if the set changed
    #[inline]
    pub fn insert_mut(&mut self, el: u32) -> bool {
        let old = *self;
        *self = old.insert(el);
        old != *self
    }

    /// Remove a element within [0..64)
    ///
    /// returns true if the set changed
    #[inline]
    pub fn remove_mut(&mut self, el: u32) -> bool {
        let old = *self;
        *self = old.remove(el);
        old != *self
    }

    /// Returns the union of two tinysets
    #[inline]
    #[must_use]
    pub fn union(self, other: TinySet) -> TinySet {
        TinySet(self.0 | other.0)
    }

    /// Returns true iff the `TinySet` is empty.
    #[inline]
    pub fn is_empty(self) -> bool {
        self.0 == 0u64
    }

    /// Returns the lowest element in the `TinySet`
    /// and removes it.
    #[inline]
    pub fn pop_lowest(&mut self) -> Option<u32> {
        if self.is_empty() {
            None
        } else {
            let lowest = self.0.trailing_zeros();
            // Kernighan's trick: `n &= n - 1` clears the lowest set bit
            // without depending on `lowest`. This lets the CPU execute
            // `trailing_zeros` and the bit-clear in parallel instead of
            // serializing them.
            //
            // The previous form `self.0 ^= 1 << lowest` needs the result of
            // `trailing_zeros` before it can shift, creating a dependency chain:
            //   ARM64: rbit → clz → lsl → eor
            //   x86:   tzcnt → btc
            //
            // With Kernighan's trick the clear path is independent of the count:
            //   ARM64: sub → and  (trailing_zeros runs in parallel)
            //   x86:   blsr       (tzcnt runs in parallel)
            //
            // https://godbolt.org/z/fnfrP1T5f
            self.0 &= self.0 - 1;
            Some(lowest)
        }
    }

    /// Returns a `TinySet` than contains all values up
    /// to limit excluded.
    ///
    /// The limit is assumed to be strictly lower than 64.
    pub fn range_lower(upper_bound: u32) -> TinySet {
        TinySet((1u64 << u64::from(upper_bound % 64u32)) - 1u64)
    }

    /// Returns a `TinySet` that contains all values greater
    /// or equal to the given limit, included. (and up to 63)
    ///
    /// The limit is assumed to be strictly lower than 64.
    pub fn range_greater_or_equal(from_included: u32) -> TinySet {
        TinySet::range_lower(from_included).complement()
    }
}

#[derive(Clone)]
pub struct BitSet {
    tinysets: Box<[TinySet]>,
    len: u64,
    max_value: u32,
}
impl std::fmt::Debug for BitSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BitSet")
            .field("len", &self.len)
            .field("max_value", &self.max_value)
            .finish()
    }
}

fn num_buckets(max_val: u32) -> u32 {
    max_val.div_ceil(64u32)
}

impl BitSet {
    /// serialize a `BitSet`.
    pub fn serialize<T: Write>(&self, writer: &mut T) -> io::Result<()> {
        writer.write_all(self.max_value.to_le_bytes().as_ref())?;
        for tinyset in self.tinysets.iter().cloned() {
            writer.write_all(&tinyset.into_bytes())?;
        }
        writer.flush()?;
        Ok(())
    }

    /// Create a new `BitSet` that may contain elements
    /// within `[0, max_val)`.
    pub fn with_max_value(max_value: u32) -> BitSet {
        let num_buckets = num_buckets(max_value);
        let tinybitsets = vec![TinySet::empty(); num_buckets as usize].into_boxed_slice();
        BitSet {
            tinysets: tinybitsets,
            len: 0,
            max_value,
        }
    }

    /// Create a new `BitSet` that may contain elements. Initially all values will be set.
    /// within `[0, max_val)`.
    pub fn with_max_value_and_full(max_value: u32) -> BitSet {
        let num_buckets = num_buckets(max_value);
        let mut tinybitsets = vec![TinySet::full(); num_buckets as usize].into_boxed_slice();

        // Fix padding
        let lower = max_value % 64u32;
        if lower != 0 {
            tinybitsets[tinybitsets.len() - 1] = TinySet::range_lower(lower);
        }
        BitSet {
            tinysets: tinybitsets,
            len: max_value as u64,
            max_value,
        }
    }

    /// Removes all elements from the `BitSet`.
    pub fn clear(&mut self) {
        for tinyset in self.tinysets.iter_mut() {
            *tinyset = TinySet::empty();
        }
    }

    /// Intersect with serialized bitset
    pub fn intersect_update(&mut self, other: &ReadOnlyBitSet) {
        self.intersect_update_with_iter(other.iter_tinysets());
    }

    /// Intersect with tinysets
    fn intersect_update_with_iter(&mut self, other: impl Iterator<Item = TinySet>) {
        self.len = 0;
        for (left, right) in self.tinysets.iter_mut().zip(other) {
            *left = left.intersect(right);
            self.len += left.len() as u64;
        }
    }

    /// Returns the number of elements in the `BitSet`.
    #[inline]
    pub fn len(&self) -> usize {
        self.len as usize
    }

    /// Inserts an element in the `BitSet`
    #[inline]
    pub fn insert(&mut self, el: u32) {
        // we do not check saturated els.
        let higher = el / 64u32;
        let lower = el % 64u32;
        self.len += u64::from(self.tinysets[higher as usize].insert_mut(lower));
    }

    /// Inserts an element in the `BitSet`
    #[inline]
    pub fn remove(&mut self, el: u32) {
        // we do not check saturated els.
        let higher = el / 64u32;
        let lower = el % 64u32;
        self.len -= u64::from(self.tinysets[higher as usize].remove_mut(lower));
    }

    /// Returns true iff the elements is in the `BitSet`.
    #[inline]
    pub fn contains(&self, el: u32) -> bool {
        self.tinyset(el / 64u32).contains(el % 64)
    }

    /// Returns the first non-empty `TinySet` associated with a bucket lower
    /// or greater than bucket.
    ///
    /// Reminder: the tiny set with the bucket `bucket`, represents the
    /// elements from `bucket * 64` to `(bucket+1) * 64`.
    pub fn first_non_empty_bucket(&self, bucket: u32) -> Option<u32> {
        self.tinysets[bucket as usize..]
            .iter()
            .cloned()
            .position(|tinyset| !tinyset.is_empty())
            .map(|delta_bucket| bucket + delta_bucket as u32)
    }

    #[inline]
    pub fn max_value(&self) -> u32 {
        self.max_value
    }

    /// Returns the tiny bitset representing the
    /// the set restricted to the number range from
    /// `bucket * 64` to `(bucket + 1) * 64`.
    pub fn tinyset(&self, bucket: u32) -> TinySet {
        self.tinysets[bucket as usize]
    }
}

/// Serialized BitSet.
#[derive(Clone)]
pub struct ReadOnlyBitSet {
    data: OwnedBytes,
    max_value: u32,
}

pub fn intersect_bitsets(left: &ReadOnlyBitSet, other: &ReadOnlyBitSet) -> ReadOnlyBitSet {
    assert_eq!(left.max_value(), other.max_value());
    assert_eq!(left.data.len(), other.data.len());
    let union_tinyset_it = left
        .iter_tinysets()
        .zip(other.iter_tinysets())
        .map(|(left_tinyset, right_tinyset)| left_tinyset.intersect(right_tinyset));
    let mut output_dataset: Vec<u8> = Vec::with_capacity(left.data.len());
    for tinyset in union_tinyset_it {
        output_dataset.extend_from_slice(&tinyset.into_bytes());
    }
    ReadOnlyBitSet {
        data: OwnedBytes::new(output_dataset),
        max_value: left.max_value(),
    }
}

impl ReadOnlyBitSet {
    pub fn open(data: OwnedBytes) -> Self {
        let (max_value_data, data) = data.split(4);
        assert_eq!(data.len() % 8, 0);
        let max_value: u32 = u32::from_le_bytes(max_value_data.as_ref().try_into().unwrap());
        ReadOnlyBitSet { data, max_value }
    }

    /// Number of elements in the bitset.
    #[inline]
    pub fn len(&self) -> usize {
        self.iter_tinysets()
            .map(|tinyset| tinyset.len() as usize)
            .sum()
    }

    /// Iterate the tinyset on the fly from serialized data.
    #[inline]
    fn iter_tinysets(&self) -> impl Iterator<Item = TinySet> + '_ {
        self.data.chunks_exact(8).map(move |chunk| {
            let tinyset: TinySet = TinySet::deserialize(chunk.try_into().unwrap());
            tinyset
        })
    }

    /// Iterate over the positions of the elements.
    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = u32> + '_ {
        self.iter_tinysets()
            .enumerate()
            .flat_map(move |(chunk_num, tinyset)| {
                let chunk_base_val = chunk_num as u32 * 64;
                tinyset
                    .into_iter()
                    .map(move |val| val + chunk_base_val)
                    .take_while(move |doc| *doc < self.max_value)
            })
    }

    /// Returns true iff the elements is in the `BitSet`.
    #[inline]
    pub fn contains(&self, el: u32) -> bool {
        let byte_offset = el / 8u32;
        let b: u8 = self.data[byte_offset as usize];
        let shift = (el % 8) as u8;
        b & (1u8 << shift) != 0
    }

    /// Maximum value the bitset may contain.
    /// (Note this is not the maximum value contained in the set.)
    ///
    /// A bitset has an intrinsic capacity.
    /// It only stores elements within [0..max_value).
    #[inline]
    pub fn max_value(&self) -> u32 {
        self.max_value
    }

    /// Number of bytes used in the bitset representation.
    pub fn num_bytes(&self) -> ByteCount {
        self.data.len().into()
    }
}

impl<'a> From<&'a BitSet> for ReadOnlyBitSet {
    fn from(bitset: &'a BitSet) -> ReadOnlyBitSet {
        let mut buffer = Vec::with_capacity(bitset.tinysets.len() * 8 + 4);
        bitset
            .serialize(&mut buffer)
            .expect("serializing into a buffer should never fail");
        ReadOnlyBitSet::open(OwnedBytes::new(buffer))
    }
}

#[cfg(test)]
mod tests {

    use std::collections::HashSet;

    use ownedbytes::OwnedBytes;
    use rand::distr::Bernoulli;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    use super::{BitSet, ReadOnlyBitSet, TinySet};

    #[test]
    fn test_read_serialized_bitset_full_multi() {
        for i in 0..1000 {
            let bitset = BitSet::with_max_value_and_full(i);
            let mut out = vec![];
            bitset.serialize(&mut out).unwrap();

            let bitset = ReadOnlyBitSet::open(OwnedBytes::new(out));
            assert_eq!(bitset.len(), i as usize);
        }
    }

    #[test]
    fn test_read_serialized_bitset_full_block() {
        let bitset = BitSet::with_max_value_and_full(64);
        let mut out = vec![];
        bitset.serialize(&mut out).unwrap();

        let bitset = ReadOnlyBitSet::open(OwnedBytes::new(out));
        assert_eq!(bitset.len(), 64);
    }

    #[test]
    fn test_read_serialized_bitset_full() {
        let mut bitset = BitSet::with_max_value_and_full(5);
        bitset.remove(3);
        let mut out = vec![];
        bitset.serialize(&mut out).unwrap();

        let bitset = ReadOnlyBitSet::open(OwnedBytes::new(out));
        assert_eq!(bitset.len(), 4);
    }

    #[test]
    fn test_bitset_intersect() {
        let bitset_serialized = {
            let mut bitset = BitSet::with_max_value_and_full(5);
            bitset.remove(1);
            bitset.remove(3);
            let mut out = vec![];
            bitset.serialize(&mut out).unwrap();

            ReadOnlyBitSet::open(OwnedBytes::new(out))
        };

        let mut bitset = BitSet::with_max_value_and_full(5);
        bitset.remove(1);
        bitset.intersect_update(&bitset_serialized);

        assert!(bitset.contains(0));
        assert!(!bitset.contains(1));
        assert!(bitset.contains(2));
        assert!(!bitset.contains(3));
        assert!(bitset.contains(4));

        bitset.intersect_update_with_iter(vec![TinySet::singleton(0)].into_iter());

        assert!(bitset.contains(0));
        assert!(!bitset.contains(1));
        assert!(!bitset.contains(2));
        assert!(!bitset.contains(3));
        assert!(!bitset.contains(4));
        assert_eq!(bitset.len(), 1);

        bitset.intersect_update_with_iter(vec![TinySet::singleton(1)].into_iter());
        assert!(!bitset.contains(0));
        assert!(!bitset.contains(1));
        assert!(!bitset.contains(2));
        assert!(!bitset.contains(3));
        assert!(!bitset.contains(4));
        assert_eq!(bitset.len(), 0);
    }

    #[test]
    fn test_read_serialized_bitset_empty() {
        let mut bitset = BitSet::with_max_value(5);
        bitset.insert(3);
        let mut out = vec![];
        bitset.serialize(&mut out).unwrap();

        let bitset = ReadOnlyBitSet::open(OwnedBytes::new(out));
        assert_eq!(bitset.len(), 1);

        {
            let bitset = BitSet::with_max_value(5);
            let mut out = vec![];
            bitset.serialize(&mut out).unwrap();
            let bitset = ReadOnlyBitSet::open(OwnedBytes::new(out));
            assert_eq!(bitset.len(), 0);
        }
    }

    #[test]
    fn test_tiny_set_remove() {
        {
            let mut u = TinySet::empty().insert(63u32).insert(5).remove(63u32);
            assert_eq!(u.pop_lowest(), Some(5u32));
            assert!(u.pop_lowest().is_none());
        }
        {
            let mut u = TinySet::empty()
                .insert(63u32)
                .insert(1)
                .insert(5)
                .remove(63u32);
            assert_eq!(u.pop_lowest(), Some(1u32));
            assert_eq!(u.pop_lowest(), Some(5u32));
            assert!(u.pop_lowest().is_none());
        }
        {
            let mut u = TinySet::empty().insert(1).remove(63u32);
            assert_eq!(u.pop_lowest(), Some(1u32));
            assert!(u.pop_lowest().is_none());
        }
        {
            let mut u = TinySet::empty().insert(1).remove(1u32);
            assert!(u.pop_lowest().is_none());
        }
    }
    #[test]
    fn test_tiny_set() {
        assert!(TinySet::empty().is_empty());
        {
            let mut u = TinySet::empty().insert(1u32);
            assert_eq!(u.pop_lowest(), Some(1u32));
            assert!(u.pop_lowest().is_none())
        }
        {
            let mut u = TinySet::empty().insert(1u32).insert(1u32);
            assert_eq!(u.pop_lowest(), Some(1u32));
            assert!(u.pop_lowest().is_none())
        }
        {
            let mut u = TinySet::empty().insert(2u32);
            assert_eq!(u.pop_lowest(), Some(2u32));
            u.insert_mut(1u32);
            assert_eq!(u.pop_lowest(), Some(1u32));
            assert!(u.pop_lowest().is_none());
        }
        {
            let mut u = TinySet::empty().insert(63u32);
            assert_eq!(u.pop_lowest(), Some(63u32));
            assert!(u.pop_lowest().is_none());
        }
        {
            let mut u = TinySet::empty().insert(63u32).insert(5);
            assert_eq!(u.pop_lowest(), Some(5u32));
            assert_eq!(u.pop_lowest(), Some(63u32));
            assert!(u.pop_lowest().is_none());
        }
        {
            let original = TinySet::empty().insert(63u32).insert(5);
            let after_serialize_deserialize = TinySet::deserialize(original.into_bytes());
            assert_eq!(original, after_serialize_deserialize);
        }
    }

    #[test]
    fn test_bitset() {
        let test_against_hashset = |els: &[u32], max_value: u32| {
            let mut hashset: HashSet<u32> = HashSet::new();
            let mut bitset = BitSet::with_max_value(max_value);
            for &el in els {
                assert!(el < max_value);
                hashset.insert(el);
                bitset.insert(el);
            }
            for el in 0..max_value {
                assert_eq!(hashset.contains(&el), bitset.contains(el));
            }
            assert_eq!(bitset.max_value(), max_value);

            // test deser
            let mut data = vec![];
            bitset.serialize(&mut data).unwrap();
            let ro_bitset = ReadOnlyBitSet::open(OwnedBytes::new(data));
            for el in 0..max_value {
                assert_eq!(hashset.contains(&el), ro_bitset.contains(el));
            }
            assert_eq!(ro_bitset.max_value(), max_value);
            assert_eq!(ro_bitset.len(), els.len());
        };

        test_against_hashset(&[], 0);
        test_against_hashset(&[], 1);
        test_against_hashset(&[0u32], 1);
        test_against_hashset(&[0u32], 100);
        test_against_hashset(&[1u32, 2u32], 4);
        test_against_hashset(&[99u32], 100);
        test_against_hashset(&[63u32], 64);
        test_against_hashset(&[62u32, 63u32], 64);
    }

    #[test]
    fn test_bitset_num_buckets() {
        use super::num_buckets;
        assert_eq!(num_buckets(0u32), 0);
        assert_eq!(num_buckets(1u32), 1);
        assert_eq!(num_buckets(64u32), 1);
        assert_eq!(num_buckets(65u32), 2);
        assert_eq!(num_buckets(128u32), 2);
        assert_eq!(num_buckets(129u32), 3);
    }

    #[test]
    fn test_tinyset_range() {
        assert_eq!(
            TinySet::range_lower(3).into_iter().collect::<Vec<u32>>(),
            [0, 1, 2]
        );
        assert!(TinySet::range_lower(0).is_empty());
        assert_eq!(
            TinySet::range_lower(63).into_iter().collect::<Vec<u32>>(),
            (0u32..63u32).collect::<Vec<_>>()
        );
        assert_eq!(
            TinySet::range_lower(1).into_iter().collect::<Vec<u32>>(),
            [0]
        );
        assert_eq!(
            TinySet::range_lower(2).into_iter().collect::<Vec<u32>>(),
            [0, 1]
        );
        assert_eq!(
            TinySet::range_greater_or_equal(3)
                .into_iter()
                .collect::<Vec<u32>>(),
            (3u32..64u32).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_bitset_len() {
        let mut bitset = BitSet::with_max_value(1_000);
        assert_eq!(bitset.len(), 0);
        bitset.insert(3u32);
        assert_eq!(bitset.len(), 1);
        bitset.insert(103u32);
        assert_eq!(bitset.len(), 2);
        bitset.insert(3u32);
        assert_eq!(bitset.len(), 2);
        bitset.insert(103u32);
        assert_eq!(bitset.len(), 2);
        bitset.insert(104u32);
        assert_eq!(bitset.len(), 3);
        bitset.remove(105u32);
        assert_eq!(bitset.len(), 3);
        bitset.remove(104u32);
        assert_eq!(bitset.len(), 2);
        bitset.remove(3u32);
        assert_eq!(bitset.len(), 1);
        bitset.remove(103u32);
        assert_eq!(bitset.len(), 0);
    }

    pub fn sample_with_seed(n: u32, ratio: f64, seed_val: u8) -> Vec<u32> {
        StdRng::from_seed([seed_val; 32])
            .sample_iter(&Bernoulli::new(ratio).unwrap())
            .take(n as usize)
            .enumerate()
            .filter_map(|(val, keep)| if keep { Some(val as u32) } else { None })
            .collect()
    }

    pub fn sample(n: u32, ratio: f64) -> Vec<u32> {
        sample_with_seed(n, ratio, 4)
    }

    #[test]
    fn test_bitset_clear() {
        let mut bitset = BitSet::with_max_value(1_000);
        let els = sample(1_000, 0.01f64);
        for &el in &els {
            bitset.insert(el);
        }
        assert!(els.iter().all(|el| bitset.contains(*el)));
        bitset.clear();
        for el in 0u32..1000u32 {
            assert!(!bitset.contains(el));
        }
    }
}
