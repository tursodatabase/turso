use std::io;
use std::io::Write;

use common::{intersect_bitsets, BitSet, ByteCount, OwnedBytes, ReadOnlyBitSet};

use crate::DocId;

/// Write an alive `BitSet`
///
/// where `alive_bitset` is the set of alive `DocId`.
/// Warning: this function does not call terminate. The caller is in charge of
/// closing the writer properly.
pub fn write_alive_bitset<T: Write>(alive_bitset: &BitSet, writer: &mut T) -> io::Result<()> {
    alive_bitset.serialize(writer)?;
    Ok(())
}

/// Set of alive `DocId`s.
#[derive(Clone)]
pub struct AliveBitSet {
    num_alive_docs: usize,
    bitset: ReadOnlyBitSet,
}

/// Intersects two AliveBitSets in a new one.
/// The two bitsets need to have the same max_value.
pub fn intersect_alive_bitsets(left: AliveBitSet, right: AliveBitSet) -> AliveBitSet {
    assert_eq!(left.bitset().max_value(), right.bitset().max_value());
    let bitset = intersect_bitsets(left.bitset(), right.bitset());
    let num_alive_docs = bitset.len();
    AliveBitSet {
        num_alive_docs,
        bitset,
    }
}

impl AliveBitSet {
    #[cfg(test)]
    pub(crate) fn for_test_from_deleted_docs(deleted_docs: &[DocId], max_doc: u32) -> AliveBitSet {
        assert!(deleted_docs.iter().all(|&doc| doc < max_doc));
        let mut bitset = BitSet::with_max_value_and_full(max_doc);
        for &doc in deleted_docs {
            bitset.remove(doc);
        }
        let mut alive_bitset_buffer = Vec::new();
        write_alive_bitset(&bitset, &mut alive_bitset_buffer).unwrap();
        let alive_bitset_bytes = OwnedBytes::new(alive_bitset_buffer);
        Self::open(alive_bitset_bytes)
    }

    /// Opens an alive bitset given its file.
    pub fn open(bytes: OwnedBytes) -> AliveBitSet {
        let bitset = ReadOnlyBitSet::open(bytes);
        AliveBitSet::from(bitset)
    }

    /// Returns true if the document is still "alive". In other words, if it has not been deleted.
    #[inline]
    pub fn is_alive(&self, doc: DocId) -> bool {
        self.bitset.contains(doc)
    }

    /// Returns true if the document has been marked as deleted.
    #[inline]
    pub fn is_deleted(&self, doc: DocId) -> bool {
        !self.is_alive(doc)
    }

    /// Iterate over the alive doc_ids.
    #[inline]
    pub fn iter_alive(&self) -> impl Iterator<Item = DocId> + '_ {
        self.bitset.iter()
    }

    /// Get underlying bitset.
    #[inline]
    pub fn bitset(&self) -> &ReadOnlyBitSet {
        &self.bitset
    }

    /// The number of alive documents.
    pub fn num_alive_docs(&self) -> usize {
        self.num_alive_docs
    }

    /// Summarize total space usage of this bitset.
    pub fn space_usage(&self) -> ByteCount {
        self.bitset().num_bytes()
    }
}

impl From<ReadOnlyBitSet> for AliveBitSet {
    fn from(bitset: ReadOnlyBitSet) -> AliveBitSet {
        let num_alive_docs = bitset.len();
        AliveBitSet {
            num_alive_docs,
            bitset,
        }
    }
}

#[cfg(test)]
mod tests {

    use super::AliveBitSet;

    #[test]
    fn test_alive_bitset_empty() {
        let alive_bitset = AliveBitSet::for_test_from_deleted_docs(&[], 10);
        for doc in 0..10 {
            assert_eq!(alive_bitset.is_deleted(doc), !alive_bitset.is_alive(doc));
            assert!(!alive_bitset.is_deleted(doc));
        }
        assert_eq!(alive_bitset.num_alive_docs(), 10);
    }

    #[test]
    fn test_alive_bitset() {
        let alive_bitset = AliveBitSet::for_test_from_deleted_docs(&[1, 9], 10);
        assert!(alive_bitset.is_alive(0));
        assert!(alive_bitset.is_deleted(1));
        assert!(alive_bitset.is_alive(2));
        assert!(alive_bitset.is_alive(3));
        assert!(alive_bitset.is_alive(4));
        assert!(alive_bitset.is_alive(5));
        assert!(alive_bitset.is_alive(6));
        assert!(alive_bitset.is_alive(6));
        assert!(alive_bitset.is_alive(7));
        assert!(alive_bitset.is_alive(8));
        assert!(alive_bitset.is_deleted(9));
        for doc in 0..10 {
            assert_eq!(alive_bitset.is_deleted(doc), !alive_bitset.is_alive(doc));
        }
        assert_eq!(alive_bitset.num_alive_docs(), 8);
    }

    #[test]
    fn test_alive_bitset_iter_minimal() {
        let alive_bitset = AliveBitSet::for_test_from_deleted_docs(&[7], 8);

        let data: Vec<_> = alive_bitset.iter_alive().collect();
        assert_eq!(data, vec![0, 1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn test_alive_bitset_iter_small() {
        let alive_bitset = AliveBitSet::for_test_from_deleted_docs(&[0, 2, 3, 6], 7);

        let data: Vec<_> = alive_bitset.iter_alive().collect();
        assert_eq!(data, vec![1, 4, 5]);
    }
    #[test]
    fn test_alive_bitset_iter() {
        let alive_bitset = AliveBitSet::for_test_from_deleted_docs(&[0, 1, 1000], 1001);

        let data: Vec<_> = alive_bitset.iter_alive().collect();
        assert_eq!(data, (2..=999).collect::<Vec<_>>());
    }
}

#[cfg(all(test, feature = "unstable"))]
mod bench {

    use rand::prelude::IteratorRandom;
    use rand::rng;
    use test::Bencher;

    use super::AliveBitSet;

    fn get_alive() -> Vec<u32> {
        let mut data = (0..1_000_000_u32).collect::<Vec<u32>>();
        for _ in 0..1_000_000 / 8 {
            remove_rand(&mut data);
        }
        data
    }

    fn remove_rand(raw: &mut Vec<u32>) {
        let i = (0..raw.len()).choose(&mut rng()).unwrap();
        raw.remove(i);
    }

    #[bench]
    fn bench_alive_bitset_iter_deser_on_fly(bench: &mut Bencher) {
        let alive_bitset = AliveBitSet::for_test_from_deleted_docs(&[0, 1, 1000, 10000], 1_000_000);

        bench.iter(|| alive_bitset.iter_alive().collect::<Vec<_>>());
    }

    #[bench]
    fn bench_alive_bitset_access(bench: &mut Bencher) {
        let alive_bitset = AliveBitSet::for_test_from_deleted_docs(&[0, 1, 1000, 10000], 1_000_000);

        bench.iter(|| {
            (0..1_000_000_u32)
                .filter(|doc| alive_bitset.is_alive(*doc))
                .collect::<Vec<_>>()
        });
    }

    #[bench]
    fn bench_alive_bitset_iter_deser_on_fly_1_8_alive(bench: &mut Bencher) {
        let alive_bitset = AliveBitSet::for_test_from_deleted_docs(&get_alive(), 1_000_000);

        bench.iter(|| alive_bitset.iter_alive().collect::<Vec<_>>());
    }

    #[bench]
    fn bench_alive_bitset_access_1_8_alive(bench: &mut Bencher) {
        let alive_bitset = AliveBitSet::for_test_from_deleted_docs(&get_alive(), 1_000_000);

        bench.iter(|| {
            (0..1_000_000_u32)
                .filter(|doc| alive_bitset.is_alive(*doc))
                .collect::<Vec<_>>()
        });
    }
}
