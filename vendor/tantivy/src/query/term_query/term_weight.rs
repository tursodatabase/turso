use super::term_scorer::TermScorer;
use crate::docset::{DocSet, COLLECT_BLOCK_BUFFER_LEN};
use crate::fieldnorm::FieldNormReader;
use crate::index::SegmentReader;
use crate::postings::SegmentPostings;
use crate::query::bm25::Bm25Weight;
use crate::query::explanation::does_not_match;
use crate::query::weight::{for_each_docset_buffered, for_each_scorer};
use crate::query::{AllScorer, AllWeight, EmptyScorer, Explanation, Scorer, Weight};
use crate::schema::IndexRecordOption;
use crate::{DocId, Score, TantivyError, Term};

pub struct TermWeight {
    term: Term,
    index_record_option: IndexRecordOption,
    similarity_weight: Bm25Weight,
    scoring_enabled: bool,
}

enum TermOrEmptyOrAllScorer {
    TermScorer(Box<TermScorer>),
    Empty,
    AllMatch(AllScorer),
}

impl TermOrEmptyOrAllScorer {
    pub fn into_boxed_scorer(self) -> Box<dyn Scorer> {
        match self {
            TermOrEmptyOrAllScorer::TermScorer(scorer) => scorer,
            TermOrEmptyOrAllScorer::Empty => Box::new(EmptyScorer),
            TermOrEmptyOrAllScorer::AllMatch(scorer) => Box::new(scorer),
        }
    }
}

impl Weight for TermWeight {
    fn scorer(&self, reader: &SegmentReader, boost: Score) -> crate::Result<Box<dyn Scorer>> {
        Ok(self.specialized_scorer(reader, boost)?.into_boxed_scorer())
    }

    fn explain(&self, reader: &SegmentReader, doc: DocId) -> crate::Result<Explanation> {
        match self.specialized_scorer(reader, 1.0)? {
            TermOrEmptyOrAllScorer::TermScorer(mut term_scorer) => {
                if term_scorer.doc() > doc || term_scorer.seek(doc) != doc {
                    return Err(does_not_match(doc));
                }
                let mut explanation = term_scorer.explain();
                explanation.add_context(format!("Term={:?}", self.term,));
                Ok(explanation)
            }
            TermOrEmptyOrAllScorer::Empty => Err(does_not_match(doc)),
            TermOrEmptyOrAllScorer::AllMatch(_) => AllWeight.explain(reader, doc),
        }
    }

    fn count(&self, reader: &SegmentReader) -> crate::Result<u32> {
        if let Some(alive_bitset) = reader.alive_bitset() {
            Ok(self.scorer(reader, 1.0)?.count(alive_bitset))
        } else {
            let field = self.term.field();
            let inv_index = reader.inverted_index(field)?;
            let term_info = inv_index.get_term_info(&self.term)?;
            Ok(term_info.map(|term_info| term_info.doc_freq).unwrap_or(0))
        }
    }

    /// Iterates through all of the document matched by the DocSet
    /// `DocSet` and push the scored documents to the collector.
    fn for_each(
        &self,
        reader: &SegmentReader,
        callback: &mut dyn FnMut(DocId, Score),
    ) -> crate::Result<()> {
        match self.specialized_scorer(reader, 1.0)? {
            TermOrEmptyOrAllScorer::TermScorer(mut term_scorer) => {
                for_each_scorer(&mut *term_scorer, callback);
            }
            TermOrEmptyOrAllScorer::Empty => {}
            TermOrEmptyOrAllScorer::AllMatch(mut all_scorer) => {
                for_each_scorer(&mut all_scorer, callback);
            }
        }
        Ok(())
    }

    /// Iterates through all of the document matched by the DocSet
    /// `DocSet` and push the scored documents to the collector.
    fn for_each_no_score(
        &self,
        reader: &SegmentReader,
        callback: &mut dyn FnMut(&[DocId]),
    ) -> crate::Result<()> {
        match self.specialized_scorer(reader, 1.0)? {
            TermOrEmptyOrAllScorer::TermScorer(mut term_scorer) => {
                let mut buffer = [0u32; COLLECT_BLOCK_BUFFER_LEN];
                for_each_docset_buffered(&mut term_scorer, &mut buffer, callback);
            }
            TermOrEmptyOrAllScorer::Empty => {}
            TermOrEmptyOrAllScorer::AllMatch(mut all_scorer) => {
                let mut buffer = [0u32; COLLECT_BLOCK_BUFFER_LEN];
                for_each_docset_buffered(&mut all_scorer, &mut buffer, callback);
            }
        };

        Ok(())
    }

    /// Calls `callback` with all of the `(doc, score)` for which score
    /// is exceeding a given threshold.
    ///
    /// This method is useful for the TopDocs collector.
    /// For all docsets, the blanket implementation has the benefit
    /// of prefiltering (doc, score) pairs, avoiding the
    /// virtual dispatch cost.
    ///
    /// More importantly, it makes it possible for scorers to implement
    /// important optimization (e.g. BlockWAND for union).
    fn for_each_pruning(
        &self,
        threshold: Score,
        reader: &SegmentReader,
        callback: &mut dyn FnMut(DocId, Score) -> Score,
    ) -> crate::Result<()> {
        let specialized_scorer = self.specialized_scorer(reader, 1.0)?;
        match specialized_scorer {
            TermOrEmptyOrAllScorer::TermScorer(term_scorer) => {
                crate::query::boolean_query::block_wand_single_scorer(
                    *term_scorer,
                    threshold,
                    callback,
                );
            }
            TermOrEmptyOrAllScorer::Empty => {}
            TermOrEmptyOrAllScorer::AllMatch(_) => {
                return Err(TantivyError::InvalidArgument(
                    "for each pruning should only be called if scoring is enabled".to_string(),
                ));
            }
        }
        Ok(())
    }
}

impl TermWeight {
    pub fn new(
        term: Term,
        index_record_option: IndexRecordOption,
        similarity_weight: Bm25Weight,
        scoring_enabled: bool,
    ) -> TermWeight {
        TermWeight {
            term,
            index_record_option,
            similarity_weight,
            scoring_enabled,
        }
    }

    pub fn term(&self) -> &Term {
        &self.term
    }

    /// We need a method to access the actual `TermScorer` implementation
    /// for `white box` test, checking in particular that the block max
    /// is correct.
    #[cfg(test)]
    pub(crate) fn term_scorer_for_test(
        &self,
        reader: &SegmentReader,
        boost: Score,
    ) -> crate::Result<Option<TermScorer>> {
        let scorer = self.specialized_scorer(reader, boost)?;
        Ok(match scorer {
            TermOrEmptyOrAllScorer::TermScorer(scorer) => Some(*scorer),
            _ => None,
        })
    }

    fn specialized_scorer(
        &self,
        reader: &SegmentReader,
        boost: Score,
    ) -> crate::Result<TermOrEmptyOrAllScorer> {
        let field = self.term.field();
        let inverted_index = reader.inverted_index(field)?;
        let Some(term_info) = inverted_index.get_term_info(&self.term)? else {
            // The term was not found.
            return Ok(TermOrEmptyOrAllScorer::Empty);
        };

        // If we don't care about scores, and our posting lists matches all doc, we can return the
        // AllMatch scorer.
        if !self.scoring_enabled && term_info.doc_freq == reader.max_doc() {
            return Ok(TermOrEmptyOrAllScorer::AllMatch(AllScorer::new(
                reader.max_doc(),
            )));
        }

        let segment_postings: SegmentPostings =
            inverted_index.read_postings_from_terminfo(&term_info, self.index_record_option)?;

        let fieldnorm_reader = self.fieldnorm_reader(reader)?;
        let similarity_weight = self.similarity_weight.boost_by(boost);
        Ok(TermOrEmptyOrAllScorer::TermScorer(Box::new(
            TermScorer::new(segment_postings, fieldnorm_reader, similarity_weight),
        )))
    }

    fn fieldnorm_reader(&self, segment_reader: &SegmentReader) -> crate::Result<FieldNormReader> {
        if self.scoring_enabled {
            if let Some(field_norm_reader) = segment_reader
                .fieldnorms_readers()
                .get_field(self.term.field())?
            {
                return Ok(field_norm_reader);
            }
        }
        Ok(FieldNormReader::constant(segment_reader.max_doc(), 1))
    }
}
