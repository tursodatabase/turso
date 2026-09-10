use crate::docset::{DocSet, SeekDangerResult, TERMINATED};
use crate::query::Scorer;
use crate::{DocId, Score};

/// An exclusion set is a set of documents
/// that should be excluded from a given DocSet.
///
/// It can be a single DocSet, or a Vec of DocSets.
pub trait ExclusionSet: Send {
    /// Returns `true` if the given `doc` is in the exclusion set.
    fn contains(&mut self, doc: DocId) -> bool;
}

impl<TDocSet: DocSet> ExclusionSet for TDocSet {
    #[inline]
    fn contains(&mut self, doc: DocId) -> bool {
        self.seek_danger(doc) == SeekDangerResult::Found
    }
}

impl<TDocSet: DocSet> ExclusionSet for Vec<TDocSet> {
    #[inline]
    fn contains(&mut self, doc: DocId) -> bool {
        for docset in self.iter_mut() {
            if docset.seek_danger(doc) == SeekDangerResult::Found {
                return true;
            }
        }
        false
    }
}

/// Filters a given `DocSet` by removing the docs from an exclusion set.
///
/// The excluding docsets have no impact on scoring.
pub struct Exclude<TDocSet, TExclusionSet> {
    underlying_docset: TDocSet,
    exclusion_set: TExclusionSet,
}

impl<TDocSet, TExclusionSet> Exclude<TDocSet, TExclusionSet>
where
    TDocSet: DocSet,
    TExclusionSet: ExclusionSet,
{
    /// Creates a new `ExcludeScorer`
    pub fn new(
        mut underlying_docset: TDocSet,
        mut exclusion_set: TExclusionSet,
    ) -> Exclude<TDocSet, TExclusionSet> {
        while underlying_docset.doc() != TERMINATED {
            let target = underlying_docset.doc();
            if !exclusion_set.contains(target) {
                break;
            }
            underlying_docset.advance();
        }
        Exclude {
            underlying_docset,
            exclusion_set,
        }
    }
}

impl<TDocSet, TExclusionSet> DocSet for Exclude<TDocSet, TExclusionSet>
where
    TDocSet: DocSet,
    TExclusionSet: ExclusionSet,
{
    fn advance(&mut self) -> DocId {
        loop {
            let candidate = self.underlying_docset.advance();
            if candidate == TERMINATED {
                return TERMINATED;
            }
            if !self.exclusion_set.contains(candidate) {
                return candidate;
            }
        }
    }

    fn seek(&mut self, target: DocId) -> DocId {
        let candidate = self.underlying_docset.seek(target);
        if candidate == TERMINATED {
            return TERMINATED;
        }
        if !self.exclusion_set.contains(candidate) {
            return candidate;
        }
        self.advance()
    }

    fn doc(&self) -> DocId {
        self.underlying_docset.doc()
    }

    /// `.size_hint()` directly returns the size
    /// of the underlying docset without taking in account
    /// the fact that docs might be deleted.
    fn size_hint(&self) -> u32 {
        self.underlying_docset.size_hint()
    }
}

impl<TScorer, TExclusionSet> Scorer for Exclude<TScorer, TExclusionSet>
where
    TScorer: Scorer,
    TExclusionSet: ExclusionSet + 'static,
{
    #[inline]
    fn score(&mut self) -> Score {
        self.underlying_docset.score()
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::postings::tests::test_skip_against_unoptimized;
    use crate::query::VecDocSet;
    use crate::tests::sample_with_seed;

    #[test]
    fn test_exclude() {
        let mut exclude_scorer = Exclude::new(
            VecDocSet::from(vec![1, 2, 5, 8, 10, 15, 24]),
            VecDocSet::from(vec![1, 2, 3, 10, 16, 24]),
        );
        let mut els = vec![];
        while exclude_scorer.doc() != TERMINATED {
            els.push(exclude_scorer.doc());
            exclude_scorer.advance();
        }
        assert_eq!(els, vec![5, 8, 15]);
    }

    #[test]
    fn test_exclude_skip() {
        test_skip_against_unoptimized(
            || {
                Box::new(Exclude::new(
                    VecDocSet::from(vec![1, 2, 5, 8, 10, 15, 24]),
                    VecDocSet::from(vec![1, 2, 3, 10, 16, 24]),
                ))
            },
            vec![5, 8, 10, 15, 24],
        );
    }

    #[test]
    fn test_exclude_skip_random() {
        let sample_include = sample_with_seed(10_000, 0.1, 1);
        let sample_exclude = sample_with_seed(10_000, 0.05, 2);
        let sample_skip = sample_with_seed(10_000, 0.005, 3);
        test_skip_against_unoptimized(
            || {
                Box::new(Exclude::new(
                    VecDocSet::from(sample_include.clone()),
                    VecDocSet::from(sample_exclude.clone()),
                ))
            },
            sample_skip,
        );
    }
}
