use std::fmt;

use crate::docset::COLLECT_BLOCK_BUFFER_LEN;
use crate::query::{EnableScoring, Explanation, Query, Scorer, Weight};
use crate::{DocId, DocSet, Score, SegmentReader, TantivyError, Term};

/// `ConstScoreQuery` is a wrapper over a query to provide a constant score.
/// It can avoid unnecessary score computation on the wrapped query.
///
/// The document set matched by the `ConstScoreQuery` is strictly the same as the underlying query.
/// The configured score is used for each document.
pub struct ConstScoreQuery {
    query: Box<dyn Query>,
    score: Score,
}

impl ConstScoreQuery {
    /// Builds a const score query.
    pub fn new(query: Box<dyn Query>, score: Score) -> ConstScoreQuery {
        ConstScoreQuery { query, score }
    }
}

impl Clone for ConstScoreQuery {
    fn clone(&self) -> Self {
        ConstScoreQuery {
            query: self.query.box_clone(),
            score: self.score,
        }
    }
}

impl fmt::Debug for ConstScoreQuery {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Const(score={}, query={:?})", self.score, self.query)
    }
}

impl Query for ConstScoreQuery {
    fn weight(&self, enable_scoring: EnableScoring<'_>) -> crate::Result<Box<dyn Weight>> {
        let inner_weight = self.query.weight(enable_scoring)?;
        Ok(if enable_scoring.is_scoring_enabled() {
            Box::new(ConstWeight::new(inner_weight, self.score))
        } else {
            inner_weight
        })
    }

    fn query_terms<'a>(&'a self, visitor: &mut dyn FnMut(&'a Term, bool)) {
        self.query.query_terms(visitor);
    }
}

struct ConstWeight {
    weight: Box<dyn Weight>,
    score: Score,
}

impl ConstWeight {
    pub fn new(weight: Box<dyn Weight>, score: Score) -> Self {
        ConstWeight { weight, score }
    }
}

impl Weight for ConstWeight {
    fn scorer(&self, reader: &SegmentReader, boost: Score) -> crate::Result<Box<dyn Scorer>> {
        let inner_scorer = self.weight.scorer(reader, boost)?;
        Ok(Box::new(ConstScorer::new(inner_scorer, boost * self.score)))
    }

    fn explain(&self, reader: &SegmentReader, doc: u32) -> crate::Result<Explanation> {
        let mut scorer = self.scorer(reader, 1.0)?;
        if scorer.seek(doc) != doc {
            return Err(TantivyError::InvalidArgument(format!(
                "Document #({doc}) does not match"
            )));
        }
        let mut explanation = Explanation::new("Const", self.score);
        let underlying_explanation = self.weight.explain(reader, doc)?;
        explanation.add_detail(underlying_explanation);
        Ok(explanation)
    }

    fn count(&self, reader: &SegmentReader) -> crate::Result<u32> {
        self.weight.count(reader)
    }
}

/// Wraps a `DocSet` and simply returns a constant `Scorer`.
/// The `ConstScorer` is useful if you have a `DocSet` where
/// you needed a scorer.
///
/// The `ConstScorer`'s constant score can be set
/// by calling `.set_score(...)`.
pub struct ConstScorer<TDocSet: DocSet> {
    docset: TDocSet,
    score: Score,
}

impl<TDocSet: DocSet> ConstScorer<TDocSet> {
    /// Creates a new `ConstScorer`.
    pub fn new(docset: TDocSet, score: Score) -> ConstScorer<TDocSet> {
        ConstScorer { docset, score }
    }
}

impl<TDocSet: DocSet> From<TDocSet> for ConstScorer<TDocSet> {
    fn from(docset: TDocSet) -> Self {
        ConstScorer::new(docset, 1.0)
    }
}

impl<TDocSet: DocSet> DocSet for ConstScorer<TDocSet> {
    fn advance(&mut self) -> DocId {
        self.docset.advance()
    }

    fn seek(&mut self, target: DocId) -> DocId {
        self.docset.seek(target)
    }

    fn fill_buffer(&mut self, buffer: &mut [DocId; COLLECT_BLOCK_BUFFER_LEN]) -> usize {
        self.docset.fill_buffer(buffer)
    }

    fn doc(&self) -> DocId {
        self.docset.doc()
    }

    fn size_hint(&self) -> u32 {
        self.docset.size_hint()
    }

    fn cost(&self) -> u64 {
        self.docset.cost()
    }
}

impl<TDocSet: DocSet + 'static> Scorer for ConstScorer<TDocSet> {
    #[inline]
    fn score(&mut self) -> Score {
        self.score
    }
}

#[cfg(test)]
mod tests {
    use super::ConstScoreQuery;
    use crate::query::{AllQuery, Query};
    use crate::schema::Schema;
    use crate::{DocAddress, Index, IndexWriter, TantivyDocument};

    #[test]
    fn test_const_score_query_explain() -> crate::Result<()> {
        let schema = Schema::builder().build();
        let index = Index::create_in_ram(schema);
        let mut index_writer: IndexWriter = index.writer_for_tests()?;
        index_writer.add_document(TantivyDocument::new())?;
        index_writer.commit()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let query = ConstScoreQuery::new(Box::new(AllQuery), 0.42);
        let explanation = query.explain(&searcher, DocAddress::new(0, 0u32)).unwrap();
        assert_eq!(
            explanation.to_pretty_json(),
            r#"{
  "value": 0.42,
  "description": "Const",
  "details": [
    {
      "value": 1.0,
      "description": "AllQuery"
    }
  ]
}"#
        );
        Ok(())
    }
}
