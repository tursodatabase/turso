use super::PhraseScorer;
use crate::fieldnorm::FieldNormReader;
use crate::index::SegmentReader;
use crate::postings::SegmentPostings;
use crate::query::bm25::Bm25Weight;
use crate::query::explanation::does_not_match;
use crate::query::{EmptyScorer, Explanation, Scorer, Weight};
use crate::schema::{IndexRecordOption, Term};
use crate::{DocId, DocSet, Score};

pub struct PhraseWeight {
    phrase_terms: Vec<(usize, Term)>,
    similarity_weight_opt: Option<Bm25Weight>,
    slop: u32,
}

impl PhraseWeight {
    /// Creates a new phrase weight.
    /// If `similarity_weight_opt` is None, then scoring is disabled
    pub fn new(
        phrase_terms: Vec<(usize, Term)>,
        similarity_weight_opt: Option<Bm25Weight>,
    ) -> PhraseWeight {
        let slop = 0;
        PhraseWeight {
            phrase_terms,
            similarity_weight_opt,
            slop,
        }
    }

    fn fieldnorm_reader(&self, reader: &SegmentReader) -> crate::Result<FieldNormReader> {
        let field = self.phrase_terms[0].1.field();
        if self.similarity_weight_opt.is_some() {
            if let Some(fieldnorm_reader) = reader.fieldnorms_readers().get_field(field)? {
                return Ok(fieldnorm_reader);
            }
        }
        Ok(FieldNormReader::constant(reader.max_doc(), 1))
    }

    pub(crate) fn phrase_scorer(
        &self,
        reader: &SegmentReader,
        boost: Score,
    ) -> crate::Result<Option<PhraseScorer<SegmentPostings>>> {
        let similarity_weight_opt = self
            .similarity_weight_opt
            .as_ref()
            .map(|similarity_weight| similarity_weight.boost_by(boost));
        let fieldnorm_reader = self.fieldnorm_reader(reader)?;
        let mut term_postings_list = Vec::new();
        for &(offset, ref term) in &self.phrase_terms {
            if let Some(postings) = reader
                .inverted_index(term.field())?
                .read_postings(term, IndexRecordOption::WithFreqsAndPositions)?
            {
                term_postings_list.push((offset, postings));
            } else {
                return Ok(None);
            }
        }
        Ok(Some(PhraseScorer::new(
            term_postings_list,
            similarity_weight_opt,
            fieldnorm_reader,
            self.slop,
        )))
    }

    pub fn slop(&mut self, slop: u32) {
        self.slop = slop;
    }
}

impl Weight for PhraseWeight {
    fn scorer(&self, reader: &SegmentReader, boost: Score) -> crate::Result<Box<dyn Scorer>> {
        if let Some(scorer) = self.phrase_scorer(reader, boost)? {
            Ok(Box::new(scorer))
        } else {
            Ok(Box::new(EmptyScorer))
        }
    }

    fn explain(&self, reader: &SegmentReader, doc: DocId) -> crate::Result<Explanation> {
        let scorer_opt = self.phrase_scorer(reader, 1.0)?;
        if scorer_opt.is_none() {
            return Err(does_not_match(doc));
        }
        let mut scorer = scorer_opt.unwrap();
        if scorer.seek(doc) != doc {
            return Err(does_not_match(doc));
        }
        let fieldnorm_reader = self.fieldnorm_reader(reader)?;
        let fieldnorm_id = fieldnorm_reader.fieldnorm_id(doc);
        let phrase_count = scorer.phrase_count();
        let mut explanation = Explanation::new("Phrase Scorer", scorer.score());
        if let Some(similarity_weight) = self.similarity_weight_opt.as_ref() {
            explanation.add_detail(similarity_weight.explain(fieldnorm_id, phrase_count));
        }
        Ok(explanation)
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::create_index;
    use crate::docset::TERMINATED;
    use crate::query::{EnableScoring, PhraseQuery};
    use crate::{DocSet, Term};

    #[test]
    pub fn test_phrase_count() -> crate::Result<()> {
        let index = create_index(&["a c", "a a b d a b c", " a b"])?;
        let schema = index.schema();
        let text_field = schema.get_field("text").unwrap();
        let searcher = index.reader()?.searcher();
        let phrase_query = PhraseQuery::new(vec![
            Term::from_field_text(text_field, "a"),
            Term::from_field_text(text_field, "b"),
        ]);
        let enable_scoring = EnableScoring::enabled_from_searcher(&searcher);
        let phrase_weight = phrase_query.phrase_weight(enable_scoring).unwrap();
        let mut phrase_scorer = phrase_weight
            .phrase_scorer(searcher.segment_reader(0u32), 1.0)?
            .unwrap();
        assert_eq!(phrase_scorer.doc(), 1);
        assert_eq!(phrase_scorer.phrase_count(), 2);
        assert_eq!(phrase_scorer.advance(), 2);
        assert_eq!(phrase_scorer.doc(), 2);
        assert_eq!(phrase_scorer.phrase_count(), 1);
        assert_eq!(phrase_scorer.advance(), TERMINATED);
        Ok(())
    }
}
