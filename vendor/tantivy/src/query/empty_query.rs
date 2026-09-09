use super::Scorer;
use crate::docset::TERMINATED;
use crate::index::SegmentReader;
use crate::query::explanation::does_not_match;
use crate::query::{EnableScoring, Explanation, Query, Weight};
use crate::{DocId, DocSet, Score, Searcher};

/// `EmptyQuery` is a dummy `Query` in which no document matches.
///
/// It is useful for tests and handling edge cases.
#[derive(Clone, Debug)]
pub struct EmptyQuery;

impl Query for EmptyQuery {
    fn weight(&self, _enable_scoring: EnableScoring<'_>) -> crate::Result<Box<dyn Weight>> {
        Ok(Box::new(EmptyWeight))
    }

    fn count(&self, _searcher: &Searcher) -> crate::Result<usize> {
        Ok(0)
    }
}

/// `EmptyWeight` is a dummy `Weight` in which no document matches.
///
/// It is useful for tests and handling edge cases.
pub struct EmptyWeight;
impl Weight for EmptyWeight {
    fn scorer(&self, _reader: &SegmentReader, _boost: Score) -> crate::Result<Box<dyn Scorer>> {
        Ok(Box::new(EmptyScorer))
    }

    fn explain(&self, _reader: &SegmentReader, doc: DocId) -> crate::Result<Explanation> {
        Err(does_not_match(doc))
    }
}

/// `EmptyScorer` is a dummy `Scorer` in which no document matches.
///
/// It is useful for tests and handling edge cases.
pub struct EmptyScorer;

impl DocSet for EmptyScorer {
    fn advance(&mut self) -> DocId {
        TERMINATED
    }

    fn doc(&self) -> DocId {
        TERMINATED
    }

    fn size_hint(&self) -> u32 {
        0
    }
}

impl Scorer for EmptyScorer {
    #[inline]
    fn score(&mut self) -> Score {
        0.0
    }
}

#[cfg(test)]
mod tests {
    use crate::docset::TERMINATED;
    use crate::query::EmptyScorer;
    use crate::DocSet;

    #[test]
    fn test_empty_scorer() {
        let mut empty_scorer = EmptyScorer;
        assert_eq!(empty_scorer.doc(), TERMINATED);
        assert_eq!(empty_scorer.advance(), TERMINATED);
        assert_eq!(empty_scorer.doc(), TERMINATED);
    }
}
