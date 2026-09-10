use std::cmp::Ordering;
use std::collections::BinaryHeap;

use crate::docset::SeekDangerResult;
use crate::query::score_combiner::DoNothingCombiner;
use crate::query::{ScoreCombiner, Scorer};
use crate::{DocId, DocSet, Score, TERMINATED};

/// `Disjunction` is responsible for merging `DocSet` from multiple
/// source. Specifically, It takes the union of two or more `DocSet`s
/// then filtering out elements that appear fewer times than a
/// specified threshold.
pub struct Disjunction<TScorer, TScoreCombiner = DoNothingCombiner> {
    chains: BinaryHeap<ScorerWrapper<TScorer>>,
    minimum_matches_required: usize,
    score_combiner: TScoreCombiner,

    current_doc: DocId,
    current_score: Score,
}

/// A wrapper around a `Scorer` that caches the current `doc_id` and implements the `DocSet` trait.
/// Also, the `Ord` trait and it's family are implemented reversely. So that we can combine
/// `std::BinaryHeap<ScorerWrapper<T>>` to gain a min-heap with current doc id as key.
struct ScorerWrapper<T> {
    scorer: T,
    current_doc: DocId,
}

#[cfg(test)]
pub(super) fn seek_danger_test_wrapper(scorer: Box<dyn Scorer>) -> impl Scorer {
    ScorerWrapper::new(scorer)
}

#[cfg(test)]
impl<T: Scorer> Scorer for ScorerWrapper<T> {
    fn score(&mut self) -> Score {
        self.scorer.score()
    }
}

impl<T: Scorer> ScorerWrapper<T> {
    fn new(scorer: T) -> Self {
        let current_doc = scorer.doc();
        Self {
            scorer,
            current_doc,
        }
    }
}

impl<T: Scorer> PartialEq for ScorerWrapper<T> {
    fn eq(&self, other: &Self) -> bool {
        self.doc() == other.doc()
    }
}

impl<T: Scorer> Eq for ScorerWrapper<T> {}

impl<T: Scorer> PartialOrd for ScorerWrapper<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: Scorer> Ord for ScorerWrapper<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.doc().cmp(&other.doc()).reverse()
    }
}

impl<T: Scorer> DocSet for ScorerWrapper<T> {
    fn advance(&mut self) -> DocId {
        let doc_id = self.scorer.advance();
        self.current_doc = doc_id;
        doc_id
    }
    fn seek(&mut self, target: DocId) -> DocId {
        let doc_id = self.scorer.seek(target);
        self.current_doc = doc_id;
        doc_id
    }
    fn seek_danger(&mut self, target: DocId) -> SeekDangerResult {
        let result = self.scorer.seek_danger(target);
        if result == SeekDangerResult::Found {
            self.current_doc = target;
        }
        result
    }

    fn doc(&self) -> DocId {
        self.current_doc
    }

    fn size_hint(&self) -> u32 {
        self.scorer.size_hint()
    }

    fn cost(&self) -> u64 {
        self.scorer.cost()
    }
}

impl<TScorer: Scorer, TScoreCombiner: ScoreCombiner> Disjunction<TScorer, TScoreCombiner> {
    pub fn new<T: IntoIterator<Item = TScorer>>(
        docsets: T,
        score_combiner: TScoreCombiner,
        minimum_matches_required: usize,
    ) -> Self {
        debug_assert!(
            minimum_matches_required > 1,
            "union scorer works better if just one matches required"
        );
        let chains = docsets
            .into_iter()
            .map(|doc| ScorerWrapper::new(doc))
            .collect();
        let mut disjunction = Self {
            chains,
            score_combiner,
            current_doc: TERMINATED,
            minimum_matches_required,
            current_score: 0.0,
        };
        if minimum_matches_required > disjunction.chains.len() {
            return disjunction;
        }
        disjunction.advance();
        disjunction
    }
}

impl<TScorer: Scorer, TScoreCombiner: ScoreCombiner> DocSet
    for Disjunction<TScorer, TScoreCombiner>
{
    fn advance(&mut self) -> DocId {
        let mut current_num_matches = 0;
        while let Some(mut candidate) = self.chains.pop() {
            let next = candidate.doc();
            if next != TERMINATED {
                // Peek next doc.
                if self.current_doc != next {
                    if current_num_matches >= self.minimum_matches_required {
                        self.chains.push(candidate);
                        self.current_score = self.score_combiner.score();
                        return self.current_doc;
                    }
                    // Reset current_num_matches and scores.
                    current_num_matches = 0;
                    self.current_doc = next;
                    self.score_combiner.clear();
                }
                current_num_matches += 1;
                self.score_combiner.update(&mut candidate.scorer);
                candidate.advance();
                self.chains.push(candidate);
            }
        }
        if current_num_matches < self.minimum_matches_required {
            self.current_doc = TERMINATED;
        }
        self.current_score = self.score_combiner.score();
        self.current_doc
    }

    #[inline]
    fn doc(&self) -> DocId {
        self.current_doc
    }

    fn size_hint(&self) -> u32 {
        self.chains
            .iter()
            .map(|docset| docset.size_hint())
            .max()
            .unwrap_or(0u32)
    }

    fn cost(&self) -> u64 {
        self.chains
            .iter()
            .map(|docset| docset.cost())
            .max()
            .unwrap_or(0u64)
    }
}

impl<TScorer: Scorer, TScoreCombiner: ScoreCombiner> Scorer
    for Disjunction<TScorer, TScoreCombiner>
{
    #[inline]
    fn score(&mut self) -> Score {
        self.current_score
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::Disjunction;
    use crate::query::score_combiner::DoNothingCombiner;
    use crate::query::{ConstScorer, Scorer, SumCombiner, VecDocSet};
    use crate::{DocId, DocSet, Score, TERMINATED};

    fn conjunct<T: Ord + Copy>(arrays: &[Vec<T>], pass_line: usize) -> Vec<T> {
        let mut counts = BTreeMap::new();
        for array in arrays {
            for &element in array {
                *counts.entry(element).or_insert(0) += 1;
            }
        }
        counts
            .iter()
            .filter_map(|(&element, &count)| {
                if count >= pass_line {
                    Some(element)
                } else {
                    None
                }
            })
            .collect()
    }

    fn aux_test_conjunction(vals: Vec<Vec<u32>>, min_match: usize) {
        let mut union_expected = VecDocSet::from(conjunct(&vals, min_match));
        let make_scorer = || {
            Disjunction::new(
                vals.iter()
                    .cloned()
                    .map(VecDocSet::from)
                    .map(|d| ConstScorer::new(d, 1.0)),
                DoNothingCombiner,
                min_match,
            )
        };
        let mut scorer: Disjunction<_, DoNothingCombiner> = make_scorer();
        let mut count = 0;
        while scorer.doc() != TERMINATED {
            assert_eq!(union_expected.doc(), scorer.doc());
            assert_eq!(union_expected.advance(), scorer.advance());
            count += 1;
        }
        assert_eq!(union_expected.advance(), TERMINATED);
        assert_eq!(count, make_scorer().count_including_deleted());
    }

    #[should_panic]
    #[test]
    fn test_arg_check1() {
        aux_test_conjunction(vec![], 0);
    }

    #[should_panic]
    #[test]
    fn test_arg_check2() {
        aux_test_conjunction(vec![], 1);
    }

    #[test]
    fn test_corner_case() {
        aux_test_conjunction(vec![], 2);
        aux_test_conjunction(vec![vec![]; 1000], 2);
        aux_test_conjunction(vec![vec![]; 100], usize::MAX);
        aux_test_conjunction(vec![vec![0xC0FFEE]; 10000], usize::MAX);
        aux_test_conjunction((1..10000u32).map(|i| vec![i]).collect::<Vec<_>>(), 2);
    }

    #[test]
    fn test_conjunction() {
        aux_test_conjunction(
            vec![
                vec![1, 3333, 100000000u32],
                vec![1, 2, 100000000u32],
                vec![1, 2, 100000000u32],
            ],
            2,
        );
        aux_test_conjunction(
            vec![vec![8], vec![3, 4, 0xC0FFEEu32], vec![1, 2, 100000000u32]],
            2,
        );
        aux_test_conjunction(
            vec![
                vec![1, 3333, 100000000u32],
                vec![1, 2, 100000000u32],
                vec![1, 2, 100000000u32],
            ],
            3,
        )
    }

    // This dummy scorer does nothing but yield doc id increasingly.
    // with constant score 1.0
    #[derive(Clone)]
    struct DummyScorer {
        cursor: usize,
        foo: Vec<(DocId, f32)>,
    }

    impl DummyScorer {
        fn new(doc_score: Vec<(DocId, f32)>) -> Self {
            Self {
                cursor: 0,
                foo: doc_score,
            }
        }
    }

    impl DocSet for DummyScorer {
        fn advance(&mut self) -> DocId {
            self.cursor += 1;
            self.doc()
        }

        fn doc(&self) -> DocId {
            self.foo.get(self.cursor).map(|x| x.0).unwrap_or(TERMINATED)
        }

        fn size_hint(&self) -> u32 {
            self.foo.len() as u32
        }
    }

    impl Scorer for DummyScorer {
        #[inline]
        fn score(&mut self) -> Score {
            self.foo.get(self.cursor).map(|x| x.1).unwrap_or(0.0)
        }
    }

    #[test]
    fn test_score_calculate() {
        let mut scorer = Disjunction::new(
            vec![
                DummyScorer::new(vec![(1, 1f32), (2, 1f32)]),
                DummyScorer::new(vec![(1, 1f32), (3, 1f32)]),
                DummyScorer::new(vec![(1, 1f32), (4, 1f32)]),
                DummyScorer::new(vec![(1, 1f32), (2, 1f32)]),
                DummyScorer::new(vec![(1, 1f32), (2, 1f32)]),
            ],
            SumCombiner::default(),
            3,
        );
        assert_eq!(scorer.score(), 5.0);
        assert_eq!(scorer.advance(), 2);
        assert_eq!(scorer.score(), 3.0);
    }

    #[test]
    fn test_score_calculate_corner_case() {
        let mut scorer = Disjunction::new(
            vec![
                DummyScorer::new(vec![(1, 1f32), (2, 1f32)]),
                DummyScorer::new(vec![(1, 1f32), (3, 1f32)]),
                DummyScorer::new(vec![(1, 1f32), (3, 1f32)]),
            ],
            SumCombiner::default(),
            2,
        );
        assert_eq!(scorer.doc(), 1);
        assert_eq!(scorer.score(), 3.0);
        assert_eq!(scorer.advance(), 3);
        assert_eq!(scorer.score(), 2.0);
    }
}
