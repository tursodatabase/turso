use super::{prefix_end, PhrasePrefixScorer};
use crate::fieldnorm::FieldNormReader;
use crate::index::SegmentReader;
use crate::postings::SegmentPostings;
use crate::query::bm25::Bm25Weight;
use crate::query::explanation::does_not_match;
use crate::query::{EmptyScorer, Explanation, Scorer, Weight};
use crate::schema::{IndexRecordOption, Term};
use crate::{DocId, DocSet, Score};

pub struct PhrasePrefixWeight {
    phrase_terms: Vec<(usize, Term)>,
    prefix: (usize, Term),
    similarity_weight_opt: Option<Bm25Weight>,
    max_expansions: u32,
}

impl PhrasePrefixWeight {
    /// Creates a new phrase weight.
    /// If `similarity_weight_opt` is None, then scoring is disabled
    pub fn new(
        phrase_terms: Vec<(usize, Term)>,
        prefix: (usize, Term),
        similarity_weight_opt: Option<Bm25Weight>,
        max_expansions: u32,
    ) -> PhrasePrefixWeight {
        PhrasePrefixWeight {
            phrase_terms,
            prefix,
            similarity_weight_opt,
            max_expansions,
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
    ) -> crate::Result<Option<PhrasePrefixScorer<SegmentPostings>>> {
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

        let inv_index = reader.inverted_index(self.prefix.1.field())?;
        let mut stream = inv_index
            .terms()
            .range()
            .ge(self.prefix.1.serialized_value_bytes());
        if let Some(end) = prefix_end(self.prefix.1.serialized_value_bytes()) {
            stream = stream.lt(&end);
        }

        #[cfg(feature = "quickwit")]
        {
            // We don't have this on the fst, hence  we end up needing a feature flag.
            //
            // This is not a problem however as we enforce the limit below too.
            // The point of `stream.limit` is to limit the number of term dictionary
            // blocks being downloaded.
            stream = stream.limit(self.max_expansions as u64);
        }

        let mut stream = stream.into_stream()?;

        let mut suffixes = Vec::with_capacity(self.max_expansions as usize);
        let mut new_term = self.prefix.1.clone();
        while stream.advance() && (suffixes.len() as u32) < self.max_expansions {
            new_term.clear_with_type(new_term.typ());
            new_term.append_bytes(stream.key());
            if reader.has_deletes() {
                if let Some(postings) =
                    inv_index.read_postings(&new_term, IndexRecordOption::WithFreqsAndPositions)?
                {
                    suffixes.push(postings);
                }
            } else if let Some(postings) =
                inv_index.read_postings(&new_term, IndexRecordOption::WithFreqsAndPositions)?
            {
                suffixes.push(postings);
            }
        }

        Ok(Some(PhrasePrefixScorer::new(
            term_postings_list,
            similarity_weight_opt,
            fieldnorm_reader,
            suffixes,
            self.prefix.0,
        )))
    }
}

impl Weight for PhrasePrefixWeight {
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
        let mut explanation = Explanation::new("Phrase Prefix Scorer", scorer.score());
        if let Some(similarity_weight) = self.similarity_weight_opt.as_ref() {
            explanation.add_detail(similarity_weight.explain(fieldnorm_id, phrase_count));
        }
        Ok(explanation)
    }
}

#[cfg(test)]
mod tests {
    use crate::docset::TERMINATED;
    use crate::index::Index;
    use crate::query::{EnableScoring, PhrasePrefixQuery, Query};
    use crate::schema::{Schema, TEXT};
    use crate::{DocSet, IndexWriter, Term};

    pub fn create_index(texts: &[&'static str]) -> crate::Result<Index> {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            let mut index_writer: IndexWriter = index.writer_for_tests()?;
            for &text in texts {
                let doc = doc!(text_field=>text);
                index_writer.add_document(doc)?;
            }
            index_writer.commit()?;
        }
        Ok(index)
    }

    #[test]
    pub fn test_phrase_count_long() -> crate::Result<()> {
        let index = create_index(&[
            "aa bb dd cc",
            "aa aa bb c dd aa bb cc aa bb dc",
            " aa bb cd",
        ])?;
        let schema = index.schema();
        let text_field = schema.get_field("text").unwrap();
        let searcher = index.reader()?.searcher();
        let phrase_query = PhrasePrefixQuery::new(vec![
            Term::from_field_text(text_field, "aa"),
            Term::from_field_text(text_field, "bb"),
            Term::from_field_text(text_field, "c"),
        ]);
        let enable_scoring = EnableScoring::enabled_from_searcher(&searcher);
        let phrase_weight = phrase_query
            .phrase_prefix_query_weight(enable_scoring)
            .unwrap()
            .unwrap();
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

    #[test]
    pub fn test_phrase_count_mid() -> crate::Result<()> {
        let index = create_index(&["aa dd cc", "aa aa bb c dd aa bb cc aa dc", " aa bb cd"])?;
        let schema = index.schema();
        let text_field = schema.get_field("text").unwrap();
        let searcher = index.reader()?.searcher();
        let phrase_query = PhrasePrefixQuery::new(vec![
            Term::from_field_text(text_field, "aa"),
            Term::from_field_text(text_field, "b"),
        ]);
        let enable_scoring = EnableScoring::enabled_from_searcher(&searcher);
        let phrase_weight = phrase_query
            .phrase_prefix_query_weight(enable_scoring)
            .unwrap()
            .unwrap();
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

    #[test]
    pub fn test_phrase_count_short() -> crate::Result<()> {
        let index = create_index(&["aa dd", "aa aa bb c dd aa bb cc aa dc", " aa bb cd"])?;
        let schema = index.schema();
        let text_field = schema.get_field("text").unwrap();
        let searcher = index.reader()?.searcher();
        let phrase_query = PhrasePrefixQuery::new(vec![Term::from_field_text(text_field, "c")]);
        let enable_scoring = EnableScoring::enabled_from_searcher(&searcher);
        assert!(phrase_query
            .phrase_prefix_query_weight(enable_scoring)
            .unwrap()
            .is_none());
        let weight = phrase_query.weight(enable_scoring).unwrap();
        let mut phrase_scorer = weight.scorer(searcher.segment_reader(0u32), 1.0)?;
        assert_eq!(phrase_scorer.doc(), 1);
        assert_eq!(phrase_scorer.advance(), 2);
        assert_eq!(phrase_scorer.doc(), 2);
        assert_eq!(phrase_scorer.advance(), TERMINATED);
        Ok(())
    }

    #[test]
    pub fn test_phrase_no_match() -> crate::Result<()> {
        let index = create_index(&["aa dd", "aa aa bb c dd aa bb cc aa dc", " aa bb cd"])?;
        let schema = index.schema();
        let text_field = schema.get_field("text").unwrap();
        let searcher = index.reader()?.searcher();
        let phrase_query = PhrasePrefixQuery::new(vec![
            Term::from_field_text(text_field, "aa"),
            Term::from_field_text(text_field, "cc"),
            Term::from_field_text(text_field, "d"),
        ]);
        let enable_scoring = EnableScoring::enabled_from_searcher(&searcher);
        let weight = phrase_query.weight(enable_scoring).unwrap();
        let mut phrase_scorer = weight.scorer(searcher.segment_reader(0u32), 1.0)?;
        assert_eq!(phrase_scorer.advance(), TERMINATED);
        Ok(())
    }
}
