use std::fmt;

use crate::docset::{SeekDangerResult, COLLECT_BLOCK_BUFFER_LEN};
use crate::fastfield::AliveBitSet;
use crate::query::{EnableScoring, Explanation, Query, Scorer, Weight};
use crate::{DocId, DocSet, Score, SegmentReader, Term};

/// `BoostQuery` is a wrapper over a query used to boost its score.
///
/// The document set matched by the `BoostQuery` is strictly the same as the underlying query.
/// The score of each document, is the score of the underlying query multiplied by the `boost`
/// factor.
pub struct BoostQuery {
    query: Box<dyn Query>,
    boost: Score,
}

impl BoostQuery {
    /// Builds a boost query.
    pub fn new(query: Box<dyn Query>, boost: Score) -> BoostQuery {
        BoostQuery { query, boost }
    }
}

impl Clone for BoostQuery {
    fn clone(&self) -> Self {
        BoostQuery {
            query: self.query.box_clone(),
            boost: self.boost,
        }
    }
}

impl fmt::Debug for BoostQuery {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Boost(query={:?}, boost={})", self.query, self.boost)
    }
}

impl Query for BoostQuery {
    fn weight(&self, enable_scoring: EnableScoring<'_>) -> crate::Result<Box<dyn Weight>> {
        let weight_without_boost = self.query.weight(enable_scoring)?;
        let boosted_weight = if enable_scoring.is_scoring_enabled() {
            Box::new(BoostWeight::new(weight_without_boost, self.boost))
        } else {
            weight_without_boost
        };
        Ok(boosted_weight)
    }

    fn query_terms<'a>(&'a self, visitor: &mut dyn FnMut(&'a Term, bool)) {
        self.query.query_terms(visitor)
    }
}

/// Weight associated to the BoostQuery.
pub struct BoostWeight {
    weight: Box<dyn Weight>,
    boost: Score,
}

impl BoostWeight {
    /// Creates a new BoostWeight.
    pub fn new(weight: Box<dyn Weight>, boost: Score) -> Self {
        BoostWeight { weight, boost }
    }
}

impl Weight for BoostWeight {
    fn scorer(&self, reader: &SegmentReader, boost: Score) -> crate::Result<Box<dyn Scorer>> {
        self.weight.scorer(reader, boost * self.boost)
    }

    fn explain(&self, reader: &SegmentReader, doc: u32) -> crate::Result<Explanation> {
        let underlying_explanation = self.weight.explain(reader, doc)?;
        let score = underlying_explanation.value() * self.boost;
        let mut explanation =
            Explanation::new_with_string(format!("Boost x{} of ...", self.boost), score);
        explanation.add_detail(underlying_explanation);
        Ok(explanation)
    }

    fn count(&self, reader: &SegmentReader) -> crate::Result<u32> {
        self.weight.count(reader)
    }
}

pub(crate) struct BoostScorer<S: Scorer> {
    underlying: S,
    boost: Score,
}

impl<S: Scorer> BoostScorer<S> {
    pub fn new(underlying: S, boost: Score) -> BoostScorer<S> {
        BoostScorer { underlying, boost }
    }
}

impl<S: Scorer> DocSet for BoostScorer<S> {
    fn advance(&mut self) -> DocId {
        self.underlying.advance()
    }

    fn seek(&mut self, target: DocId) -> DocId {
        self.underlying.seek(target)
    }
    fn seek_danger(&mut self, target: DocId) -> SeekDangerResult {
        self.underlying.seek_danger(target)
    }

    fn fill_buffer(&mut self, buffer: &mut [DocId; COLLECT_BLOCK_BUFFER_LEN]) -> usize {
        self.underlying.fill_buffer(buffer)
    }

    fn doc(&self) -> u32 {
        self.underlying.doc()
    }

    fn size_hint(&self) -> u32 {
        self.underlying.size_hint()
    }

    fn cost(&self) -> u64 {
        self.underlying.cost()
    }

    fn count(&mut self, alive_bitset: &AliveBitSet) -> u32 {
        self.underlying.count(alive_bitset)
    }

    fn count_including_deleted(&mut self) -> u32 {
        self.underlying.count_including_deleted()
    }
}

impl<S: Scorer> Scorer for BoostScorer<S> {
    #[inline]
    fn score(&mut self) -> Score {
        self.underlying.score() * self.boost
    }
}

#[cfg(test)]
mod tests {
    use super::BoostQuery;
    use crate::query::{AllQuery, Query};
    use crate::schema::Schema;
    use crate::{DocAddress, Index, IndexWriter, TantivyDocument};

    #[test]
    fn test_boost_query_explain() -> crate::Result<()> {
        let schema = Schema::builder().build();
        let index = Index::create_in_ram(schema);
        let mut index_writer: IndexWriter = index.writer_for_tests()?;
        index_writer.add_document(TantivyDocument::new())?;
        index_writer.commit()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let query = BoostQuery::new(Box::new(AllQuery), 0.2);
        let explanation = query.explain(&searcher, DocAddress::new(0, 0u32)).unwrap();
        assert_eq!(
            explanation.to_pretty_json(),
            "{\n  \"value\": 0.2,\n  \"description\": \"Boost x0.2 of ...\",\n  \"details\": [\n    {\n      \"value\": 1.0,\n      \"description\": \"AllQuery\"\n    }\n  ]\n}"
        );
        Ok(())
    }
}
