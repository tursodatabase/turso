use common::{BitSet, TinySet};

use crate::docset::{DocSet, TERMINATED};
use crate::DocId;

/// A `BitSetDocSet` makes it possible to iterate through a bitset as if it was a `DocSet`.
///
/// # Implementation detail
///
/// Skipping is relatively fast here as we can directly point to the
/// right tiny bitset bucket.
///
/// TODO: Consider implementing a `BitTreeSet` in order to advance faster
/// when the bitset is sparse
pub struct BitSetDocSet {
    docs: BitSet,
    cursor_bucket: u32, //< index associated with the current tiny bitset
    cursor_tinybitset: TinySet,
    doc: u32,
}

impl BitSetDocSet {
    fn go_to_bucket(&mut self, bucket_addr: u32) {
        self.cursor_bucket = bucket_addr;
        self.cursor_tinybitset = self.docs.tinyset(bucket_addr);
    }
}

impl From<BitSet> for BitSetDocSet {
    fn from(docs: BitSet) -> BitSetDocSet {
        let first_tiny_bitset = if docs.max_value() == 0 {
            TinySet::empty()
        } else {
            docs.tinyset(0)
        };
        let mut docset = BitSetDocSet {
            docs,
            cursor_bucket: 0,
            cursor_tinybitset: first_tiny_bitset,
            doc: 0u32,
        };
        docset.advance();
        docset
    }
}

impl DocSet for BitSetDocSet {
    #[inline]
    fn advance(&mut self) -> DocId {
        if let Some(lower) = self.cursor_tinybitset.pop_lowest() {
            self.doc = (self.cursor_bucket * 64u32) | lower;
            return self.doc;
        }
        if let Some(cursor_bucket) = self.docs.first_non_empty_bucket(self.cursor_bucket + 1) {
            self.go_to_bucket(cursor_bucket);
            let lower = self.cursor_tinybitset.pop_lowest().unwrap();
            self.doc = (cursor_bucket * 64u32) | lower;
            self.doc
        } else {
            self.doc = TERMINATED;
            TERMINATED
        }
    }

    fn seek(&mut self, target: DocId) -> DocId {
        if target >= self.docs.max_value() {
            self.doc = TERMINATED;
            return TERMINATED;
        }
        let target_bucket = target / 64u32;
        if target_bucket > self.cursor_bucket {
            self.go_to_bucket(target_bucket);
            let greater_filter: TinySet = TinySet::range_greater_or_equal(target);
            self.cursor_tinybitset = self.cursor_tinybitset.intersect(greater_filter);
            self.advance()
        } else {
            let mut doc = self.doc();
            while doc < target {
                doc = self.advance();
            }
            doc
        }
    }

    /// Returns the current document
    fn doc(&self) -> DocId {
        self.doc
    }

    /// Returns the number of values set in the underlying bitset.
    fn size_hint(&self) -> u32 {
        self.docs.len() as u32
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use common::BitSet;

    use super::BitSetDocSet;
    use crate::docset::{DocSet, TERMINATED};
    use crate::tests::generate_nonunique_unsorted;
    use crate::DocId;

    fn create_docbitset(docs: &[DocId], max_doc: DocId) -> BitSetDocSet {
        let mut docset = BitSet::with_max_value(max_doc);
        for &doc in docs {
            docset.insert(doc);
        }
        BitSetDocSet::from(docset)
    }

    #[test]
    fn test_bitset_large() {
        let arr = generate_nonunique_unsorted(100_000, 5_000);
        let mut btreeset: BTreeSet<u32> = BTreeSet::new();
        let mut bitset = BitSet::with_max_value(100_000);
        for el in arr {
            btreeset.insert(el);
            bitset.insert(el);
        }
        for i in 0..100_000 {
            assert_eq!(btreeset.contains(&i), bitset.contains(i));
        }
        assert_eq!(btreeset.len(), bitset.len());
        let mut bitset_docset = BitSetDocSet::from(bitset);
        let mut remaining = true;
        for el in btreeset.into_iter() {
            assert!(remaining);
            assert_eq!(bitset_docset.doc(), el);
            remaining = bitset_docset.advance() != TERMINATED;
        }
        assert!(!remaining);
    }

    #[test]
    fn test_empty() {
        let bitset = BitSet::with_max_value(1000);
        let mut empty = BitSetDocSet::from(bitset);
        assert_eq!(empty.advance(), TERMINATED)
    }

    #[test]
    fn test_seek_terminated() {
        let bitset = BitSet::with_max_value(1000);
        let mut empty = BitSetDocSet::from(bitset);
        assert_eq!(empty.seek(TERMINATED), TERMINATED)
    }

    fn test_go_through_sequential(docs: &[DocId]) {
        let mut docset = create_docbitset(docs, 1_000u32);
        for &doc in docs {
            assert_eq!(doc, docset.doc());
            docset.advance();
        }
        assert_eq!(docset.advance(), TERMINATED);
    }

    #[test]
    fn test_docbitset_sequential() {
        test_go_through_sequential(&[1, 2, 3]);
        test_go_through_sequential(&[1, 2, 3, 4, 5, 63, 64, 65]);
        test_go_through_sequential(&[63, 64, 65]);
        test_go_through_sequential(&[1, 2, 3, 4, 95, 96, 97, 98, 99]);
    }

    #[test]
    fn test_docbitset_skip() {
        {
            let mut docset = create_docbitset(&[1, 5, 6, 7, 5112], 10_000);
            assert_eq!(docset.seek(7), 7);
            assert_eq!(docset.doc(), 7);
            assert_eq!(docset.advance(), 5112);
            assert_eq!(docset.doc(), 5112);
            assert_eq!(docset.advance(), TERMINATED);
        }
        {
            let mut docset = create_docbitset(&[1, 5, 6, 7, 5112], 10_000);
            assert_eq!(docset.seek(3), 5);
            assert_eq!(docset.doc(), 5);
            assert_eq!(docset.advance(), 6);
        }
        {
            let mut docset = create_docbitset(&[5112], 10_000);
            assert_eq!(docset.seek(5112), 5112);
            assert_eq!(docset.doc(), 5112);
            assert_eq!(docset.advance(), TERMINATED);
        }
        {
            let mut docset = create_docbitset(&[5112], 10_000);
            assert_eq!(docset.seek(5113), TERMINATED);
            assert_eq!(docset.advance(), TERMINATED);
        }
        {
            let mut docset = create_docbitset(&[5112], 10_000);
            assert_eq!(docset.seek(5111), 5112);
            assert_eq!(docset.doc(), 5112);
            assert_eq!(docset.advance(), TERMINATED);
        }
        {
            let mut docset = create_docbitset(&[1, 5, 6, 7, 5112, 5500, 6666], 10_000);
            assert_eq!(docset.seek(5112), 5112);
            assert_eq!(docset.doc(), 5112);
            assert_eq!(docset.advance(), 5500);
            assert_eq!(docset.doc(), 5500);
            assert_eq!(docset.advance(), 6666);
            assert_eq!(docset.doc(), 6666);
            assert_eq!(docset.advance(), TERMINATED);
        }
        {
            let mut docset = create_docbitset(&[1, 5, 6, 7, 5112, 5500, 6666], 10_000);
            assert_eq!(docset.seek(5111), 5112);
            assert_eq!(docset.doc(), 5112);
            assert_eq!(docset.advance(), 5500);
            assert_eq!(docset.doc(), 5500);
            assert_eq!(docset.advance(), 6666);
            assert_eq!(docset.doc(), 6666);
            assert_eq!(docset.advance(), TERMINATED);
        }
        {
            let mut docset = create_docbitset(&[1, 5, 6, 7, 5112, 5513, 6666], 10_000);
            assert_eq!(docset.seek(5111), 5112);
            assert_eq!(docset.doc(), 5112);
            assert_eq!(docset.advance(), 5513);
            assert_eq!(docset.doc(), 5513);
            assert_eq!(docset.advance(), 6666);
            assert_eq!(docset.doc(), 6666);
            assert_eq!(docset.advance(), TERMINATED);
        }
    }
}

#[cfg(all(test, feature = "unstable"))]
mod bench {

    use super::{BitSet, BitSetDocSet};
    use crate::docset::TERMINATED;
    use crate::{test, tests, DocSet};

    #[bench]
    fn bench_bitset_1pct_insert(b: &mut test::Bencher) {
        let els = tests::generate_nonunique_unsorted(1_000_000u32, 10_000);
        b.iter(|| {
            let mut bitset = BitSet::with_max_value(1_000_000);
            for el in els.iter().cloned() {
                bitset.insert(el);
            }
        });
    }

    #[bench]
    fn bench_bitset_1pct_clone(b: &mut test::Bencher) {
        let els = tests::generate_nonunique_unsorted(1_000_000u32, 10_000);
        let mut bitset = BitSet::with_max_value(1_000_000);
        for el in els {
            bitset.insert(el);
        }
        b.iter(|| bitset.clone());
    }

    #[bench]
    fn bench_bitset_1pct_clone_iterate(b: &mut test::Bencher) {
        let els = tests::sample(1_000_000u32, 0.01);
        let mut bitset = BitSet::with_max_value(1_000_000);
        for el in els {
            bitset.insert(el);
        }
        b.iter(|| {
            let mut docset = BitSetDocSet::from(bitset.clone());
            while docset.advance() != TERMINATED {}
        });
    }
}
