use std::marker::PhantomData;

use crate::docset::{DocSet, SeekDangerResult};
use crate::query::score_combiner::ScoreCombiner;
use crate::query::Scorer;
use crate::{DocId, Score};

/// Given a required scorer and an optional scorer
/// matches all document from the required scorer
/// and complements the score using the optional scorer.
///
/// This is useful for queries like `+somethingrequired somethingoptional`.
///
/// Note that `somethingoptional` has no impact on the `DocSet`.
pub struct RequiredOptionalScorer<TReqScorer, TOptScorer, TScoreCombiner: ScoreCombiner> {
    req_scorer: TReqScorer,
    opt_scorer: TOptScorer,
    score_cache: Option<Score>,
    _phantom: PhantomData<TScoreCombiner>,
}

impl<TReqScorer, TOptScorer, TScoreCombiner>
    RequiredOptionalScorer<TReqScorer, TOptScorer, TScoreCombiner>
where
    TOptScorer: DocSet,
    TScoreCombiner: ScoreCombiner,
{
    /// Creates a new `RequiredOptionalScorer`.
    pub fn new(
        req_scorer: TReqScorer,
        opt_scorer: TOptScorer,
    ) -> RequiredOptionalScorer<TReqScorer, TOptScorer, TScoreCombiner> {
        RequiredOptionalScorer {
            req_scorer,
            opt_scorer,
            score_cache: None,
            _phantom: PhantomData,
        }
    }
}

impl<TReqScorer, TOptScorer, TScoreCombiner> DocSet
    for RequiredOptionalScorer<TReqScorer, TOptScorer, TScoreCombiner>
where
    TReqScorer: DocSet,
    TOptScorer: DocSet,
    TScoreCombiner: ScoreCombiner,
{
    fn advance(&mut self) -> DocId {
        self.score_cache = None;
        self.req_scorer.advance()
    }

    fn seek(&mut self, target: DocId) -> DocId {
        self.score_cache = None;
        self.req_scorer.seek(target)
    }

    fn seek_danger(&mut self, target: DocId) -> SeekDangerResult {
        self.score_cache = None;
        self.req_scorer.seek_danger(target)
    }

    fn doc(&self) -> DocId {
        self.req_scorer.doc()
    }

    fn size_hint(&self) -> u32 {
        self.req_scorer.size_hint()
    }

    fn cost(&self) -> u64 {
        self.req_scorer.cost()
    }
}

impl<TReqScorer, TOptScorer, TScoreCombiner> Scorer
    for RequiredOptionalScorer<TReqScorer, TOptScorer, TScoreCombiner>
where
    TReqScorer: Scorer,
    TOptScorer: Scorer,
    TScoreCombiner: ScoreCombiner,
{
    #[inline]
    fn score(&mut self) -> Score {
        if let Some(score) = self.score_cache {
            return score;
        }
        let doc = self.doc();
        let mut score_combiner = TScoreCombiner::default();
        score_combiner.update(&mut self.req_scorer);
        if self.opt_scorer.doc() <= doc && self.opt_scorer.seek(doc) == doc {
            score_combiner.update(&mut self.opt_scorer);
        }
        let score = score_combiner.score();
        self.score_cache = Some(score);
        score
    }
}

#[cfg(test)]
mod tests {
    use super::RequiredOptionalScorer;
    use crate::docset::{DocSet, TERMINATED};
    use crate::postings::tests::test_skip_against_unoptimized;
    use crate::query::score_combiner::{DoNothingCombiner, SumCombiner};
    use crate::query::{ConstScorer, Scorer, VecDocSet};
    use crate::tests::sample_with_seed;

    #[test]
    fn test_reqopt_scorer_empty() {
        let req = vec![1, 3, 7];
        let mut reqoptscorer: RequiredOptionalScorer<_, _, SumCombiner> =
            RequiredOptionalScorer::new(
                ConstScorer::from(VecDocSet::from(req.clone())),
                ConstScorer::from(VecDocSet::from(vec![])),
            );
        let mut docs = vec![];
        while reqoptscorer.doc() != TERMINATED {
            docs.push(reqoptscorer.doc());
            reqoptscorer.advance();
        }
        assert_eq!(docs, req);
    }

    #[test]
    fn test_reqopt_scorer() {
        let mut reqoptscorer: RequiredOptionalScorer<_, _, SumCombiner> =
            RequiredOptionalScorer::new(
                ConstScorer::new(VecDocSet::from(vec![1, 3, 7, 8, 9, 10, 13, 15]), 1.0),
                ConstScorer::new(VecDocSet::from(vec![1, 2, 7, 11, 12, 15]), 1.0),
            );
        {
            assert_eq!(reqoptscorer.doc(), 1);
            assert_eq!(reqoptscorer.score(), 2.0);
        }
        {
            assert_eq!(reqoptscorer.advance(), 3);
            assert_eq!(reqoptscorer.doc(), 3);
            assert_eq!(reqoptscorer.score(), 1.0);
        }
        {
            assert_eq!(reqoptscorer.advance(), 7);
            assert_eq!(reqoptscorer.doc(), 7);
            assert_eq!(reqoptscorer.score(), 2.0);
        }
        {
            assert_eq!(reqoptscorer.advance(), 8);
            assert_eq!(reqoptscorer.doc(), 8);
            assert_eq!(reqoptscorer.score(), 1.0);
        }
        {
            assert_eq!(reqoptscorer.advance(), 9);
            assert_eq!(reqoptscorer.doc(), 9);
            assert_eq!(reqoptscorer.score(), 1.0);
        }
        {
            assert_eq!(reqoptscorer.advance(), 10);
            assert_eq!(reqoptscorer.doc(), 10);
            assert_eq!(reqoptscorer.score(), 1.0);
        }
        {
            assert_eq!(reqoptscorer.advance(), 13);
            assert_eq!(reqoptscorer.doc(), 13);
            assert_eq!(reqoptscorer.score(), 1.0);
        }
        {
            assert_eq!(reqoptscorer.advance(), 15);
            assert_eq!(reqoptscorer.doc(), 15);
            assert_eq!(reqoptscorer.score(), 2.0);
        }
        assert_eq!(reqoptscorer.advance(), TERMINATED);
    }

    #[test]
    fn test_reqopt_scorer_skip() {
        let req_docs = sample_with_seed(10_000, 0.02, 1);
        let opt_docs = sample_with_seed(10_000, 0.02, 2);
        let skip_docs = sample_with_seed(10_000, 0.001, 3);
        test_skip_against_unoptimized(
            || {
                Box::new(RequiredOptionalScorer::<_, _, DoNothingCombiner>::new(
                    ConstScorer::from(VecDocSet::from(req_docs.clone())),
                    ConstScorer::from(VecDocSet::from(opt_docs.clone())),
                ))
            },
            skip_docs,
        );
    }

    #[test]
    fn test_reqopt_scorer_seek() {
        let mut reqoptscorer: RequiredOptionalScorer<_, _, SumCombiner> =
            RequiredOptionalScorer::new(
                ConstScorer::new(VecDocSet::from(vec![1, 3, 7, 8, 9, 10, 13, 15]), 1.0),
                ConstScorer::new(VecDocSet::from(vec![2, 7, 11, 12, 15]), 1.0),
            );
        {
            assert_eq!(reqoptscorer.score(), 1.0);
            assert_eq!(reqoptscorer.seek(7), 7);
            assert_eq!(reqoptscorer.score(), 2.0);
        }
        {
            assert_eq!(reqoptscorer.score(), 2.0);
            assert_eq!(reqoptscorer.seek(12), 13);
            assert_eq!(reqoptscorer.score(), 1.0);
        }
    }
}
