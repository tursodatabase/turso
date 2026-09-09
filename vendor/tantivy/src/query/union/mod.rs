mod bitset_union;
mod buffered_union;
mod simple_union;

pub use bitset_union::BitSetPostingUnion;
pub use buffered_union::BufferedUnionScorer;
pub use simple_union::SimpleUnion;

#[cfg(test)]
mod tests {

    use std::collections::BTreeSet;

    use common::BitSet;

    use super::{SimpleUnion, *};
    use crate::docset::{DocSet, SeekDangerResult, TERMINATED};
    use crate::postings::tests::test_skip_against_unoptimized;
    use crate::query::score_combiner::DoNothingCombiner;
    use crate::query::union::bitset_union::BitSetPostingUnion;
    use crate::query::{BitSetDocSet, ConstScorer, VecDocSet};
    use crate::{tests, DocId};

    fn vec_doc_set_from_docs_list(
        docs_list: &[Vec<DocId>],
    ) -> impl Iterator<Item = VecDocSet> + '_ {
        docs_list.iter().cloned().map(VecDocSet::from)
    }
    fn union_from_docs_list(docs_list: &[Vec<DocId>]) -> Box<dyn DocSet> {
        let max_doc = docs_list
            .iter()
            .flat_map(|docs| docs.iter().copied())
            .max()
            .unwrap_or(0);
        Box::new(BufferedUnionScorer::build(
            vec_doc_set_from_docs_list(docs_list)
                .map(|docset| ConstScorer::new(docset, 1.0))
                .collect::<Vec<ConstScorer<VecDocSet>>>(),
            DoNothingCombiner::default,
            max_doc,
        ))
    }

    fn posting_list_union_from_docs_list(docs_list: &[Vec<DocId>]) -> Box<dyn DocSet> {
        Box::new(BitSetPostingUnion::build(
            vec_doc_set_from_docs_list(docs_list).collect::<Vec<VecDocSet>>(),
            bitset_from_docs_list(docs_list),
        ))
    }
    fn simple_union_from_docs_list(docs_list: &[Vec<DocId>]) -> Box<dyn DocSet> {
        Box::new(SimpleUnion::build(
            vec_doc_set_from_docs_list(docs_list).collect::<Vec<VecDocSet>>(),
        ))
    }
    fn bitset_from_docs_list(docs_list: &[Vec<DocId>]) -> BitSetDocSet {
        let max_doc = docs_list
            .iter()
            .flat_map(|docs| docs.iter().copied())
            .max()
            .unwrap_or(0);
        let mut doc_bitset = BitSet::with_max_value(max_doc + 1);
        for docs in docs_list {
            for &doc in docs {
                doc_bitset.insert(doc);
            }
        }
        BitSetDocSet::from(doc_bitset)
    }
    fn aux_test_union(docs_list: &[Vec<DocId>]) {
        for constructor in [
            posting_list_union_from_docs_list,
            simple_union_from_docs_list,
            union_from_docs_list,
        ] {
            aux_test_union_with_constructor(constructor, docs_list);
        }
    }
    fn aux_test_union_with_constructor<F>(constructor: F, docs_list: &[Vec<DocId>])
    where F: Fn(&[Vec<DocId>]) -> Box<dyn DocSet> {
        let mut val_set: BTreeSet<u32> = BTreeSet::new();
        for vs in docs_list {
            for &v in vs {
                val_set.insert(v);
            }
        }
        let union_vals: Vec<u32> = val_set.into_iter().collect();
        let mut union_expected = VecDocSet::from(union_vals);
        let make_union = || constructor(docs_list);
        let mut union = make_union();
        let mut count = 0;
        while union.doc() != TERMINATED {
            assert_eq!(union_expected.doc(), union.doc());
            assert_eq!(union_expected.advance(), union.advance());
            count += 1;
        }
        assert_eq!(union_expected.advance(), TERMINATED);
        assert_eq!(count, make_union().count_including_deleted());
    }

    use proptest::prelude::*;

    proptest! {
        #[test]
        fn test_union_is_same(vecs in prop::collection::vec(
            prop::collection::vec(0u32..100, 1..10)
                .prop_map(|mut inner| {
                    inner.sort_unstable();
                    inner.dedup();
                    inner
                }),
            1..10
        ),
        seek_docids in prop::collection::vec(0u32..100, 0..10).prop_map(|mut inner| {
            inner.sort_unstable();
            inner
        })) {
            test_docid_with_skip(&vecs, &seek_docids);
        }
    }

    fn test_docid_with_skip(vecs: &[Vec<DocId>], skip_targets: &[DocId]) {
        let mut union1 = posting_list_union_from_docs_list(vecs);
        let mut union2 = simple_union_from_docs_list(vecs);
        let mut union3 = union_from_docs_list(vecs);

        // Check initial sequential advance
        while union1.doc() != TERMINATED {
            assert_eq!(union1.doc(), union2.doc());
            assert_eq!(union1.doc(), union3.doc());
            assert_eq!(union1.advance(), union2.advance());
            assert_eq!(union1.doc(), union3.advance());
        }

        // Reset and test seek functionality
        let mut union1 = posting_list_union_from_docs_list(vecs);
        let mut union2 = simple_union_from_docs_list(vecs);
        let mut union3 = union_from_docs_list(vecs);

        for &seek_docid in skip_targets {
            union1.seek(seek_docid);
            union2.seek(seek_docid);
            union3.seek(seek_docid);

            // Verify that all unions have the same document after seeking
            assert_eq!(union3.doc(), union1.doc());
            assert_eq!(union3.doc(), union2.doc());
        }
    }

    #[test]
    fn test_union() {
        aux_test_union(&[
            vec![1, 3333, 100000000u32],
            vec![1, 2, 100000000u32],
            vec![1, 2, 100000000u32],
            vec![],
        ]);
        aux_test_union(&[
            vec![1, 3333, 100000000u32],
            vec![1, 2, 100000000u32],
            vec![1, 2, 100000000u32],
            vec![],
        ]);
        aux_test_union(&[
            tests::sample_with_seed(100_000, 0.01, 1),
            tests::sample_with_seed(100_000, 0.05, 2),
            tests::sample_with_seed(100_000, 0.001, 3),
        ]);
    }

    fn test_aux_union_skip(docs_list: &[Vec<DocId>], skip_targets: Vec<DocId>) {
        for constructor in [
            posting_list_union_from_docs_list,
            simple_union_from_docs_list,
            union_from_docs_list,
        ] {
            test_aux_union_skip_with_constructor(constructor, docs_list, skip_targets.clone());
        }
    }
    fn test_aux_union_skip_with_constructor<F>(
        constructor: F,
        docs_list: &[Vec<DocId>],
        skip_targets: Vec<DocId>,
    ) where
        F: Fn(&[Vec<DocId>]) -> Box<dyn DocSet>,
    {
        let mut btree_set = BTreeSet::new();
        for docs in docs_list {
            btree_set.extend(docs.iter().cloned());
        }
        let docset_factory = || {
            let res: Box<dyn DocSet> = constructor(docs_list);
            res
        };
        let mut docset = constructor(docs_list);
        for el in btree_set {
            assert_eq!(el, docset.doc());
            docset.advance();
        }
        assert_eq!(docset.doc(), TERMINATED);
        test_skip_against_unoptimized(docset_factory, skip_targets);
    }

    #[test]
    fn test_union_skip_corner_case() {
        test_aux_union_skip(&[vec![165132, 167382], vec![25029, 25091]], vec![25029]);
    }

    #[test]
    fn test_union_skip_corner_case2() {
        test_aux_union_skip(
            &[vec![1u32, 1u32 + 100], vec![2u32, 1000u32, 10_000u32]],
            vec![0u32, 1u32, 2u32, 3u32, 1u32 + 100, 2u32 + 100],
        );
    }

    #[test]
    fn test_union_skip_corner_case3() {
        let mut docset = posting_list_union_from_docs_list(&[vec![0u32, 5u32], vec![1u32, 4u32]]);
        assert_eq!(docset.doc(), 0u32);
        assert_eq!(docset.seek(0u32), 0u32);
        assert_eq!(docset.seek(0u32), 0u32);
        assert_eq!(docset.doc(), 0u32)
    }

    #[test]
    fn test_union_skip_random() {
        test_aux_union_skip(
            &[
                vec![1, 2, 3, 7],
                vec![1, 3, 9, 10000],
                vec![1, 3, 8, 9, 100],
            ],
            vec![1, 2, 3, 5, 6, 7, 8, 100],
        );
        test_aux_union_skip(
            &[
                tests::sample_with_seed(100_000, 0.001, 1),
                tests::sample_with_seed(100_000, 0.002, 2),
                tests::sample_with_seed(100_000, 0.005, 3),
            ],
            tests::sample_with_seed(100_000, 0.01, 4),
        );
    }

    #[test]
    fn test_union_skip_specific() {
        test_aux_union_skip(
            &[
                vec![1, 2, 3, 7],
                vec![1, 3, 9, 10000],
                vec![1, 3, 8, 9, 100],
            ],
            vec![1, 2, 3, 7, 8, 9, 99, 100, 101, 500, 20000],
        );
    }

    #[test]
    fn test_buffered_union_seek_into_danger_zone_terminated() {
        let scorer1 = ConstScorer::new(VecDocSet::from(vec![1, 2]), 1.0);
        let scorer2 = ConstScorer::new(VecDocSet::from(vec![2, 3]), 1.0);

        let mut union_scorer =
            BufferedUnionScorer::build(vec![scorer1, scorer2], DoNothingCombiner::default, 100);

        // Advance to end
        while union_scorer.doc() != TERMINATED {
            union_scorer.advance();
        }

        assert_eq!(union_scorer.doc(), TERMINATED);

        assert_eq!(
            union_scorer.seek_danger(TERMINATED),
            SeekDangerResult::SeekLowerBound(TERMINATED)
        );
    }
}

#[cfg(all(test, feature = "unstable"))]
mod bench {

    use test::Bencher;

    use crate::query::score_combiner::DoNothingCombiner;
    use crate::query::{BufferedUnionScorer, ConstScorer, VecDocSet};
    use crate::{tests, DocId, DocSet, TERMINATED};

    #[bench]
    fn bench_union_3_high(bench: &mut Bencher) {
        let union_docset: Vec<Vec<DocId>> = vec![
            tests::sample_with_seed(100_000, 0.1, 0),
            tests::sample_with_seed(100_000, 0.2, 1),
        ];
        bench.iter(|| {
            let mut v = BufferedUnionScorer::build(
                union_docset
                    .iter()
                    .map(|doc_ids| VecDocSet::from(doc_ids.clone()))
                    .map(|docset| ConstScorer::new(docset, 1.0))
                    .collect::<Vec<_>>(),
                DoNothingCombiner::default,
                100_000,
            );
            while v.doc() != TERMINATED {
                v.advance();
            }
        });
    }
    #[bench]
    fn bench_union_3_low(bench: &mut Bencher) {
        let union_docset: Vec<Vec<DocId>> = vec![
            tests::sample_with_seed(100_000, 0.01, 0),
            tests::sample_with_seed(100_000, 0.05, 1),
            tests::sample_with_seed(100_000, 0.001, 2),
        ];
        bench.iter(|| {
            let mut v = BufferedUnionScorer::build(
                union_docset
                    .iter()
                    .map(|doc_ids| VecDocSet::from(doc_ids.clone()))
                    .map(|docset| ConstScorer::new(docset, 1.0))
                    .collect::<Vec<_>>(),
                DoNothingCombiner::default,
                100_000,
            );
            while v.doc() != TERMINATED {
                v.advance();
            }
        });
    }
}
