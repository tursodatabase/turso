use crate::docset::DocSet;
use crate::fieldnorm::FieldNormReader;
use crate::postings::{FreqReadingOption, Postings, SegmentPostings};
use crate::query::bm25::Bm25Weight;
use crate::query::{Explanation, Scorer};
use crate::{DocId, Score};

#[derive(Clone)]
pub struct TermScorer {
    postings: SegmentPostings,
    fieldnorm_reader: FieldNormReader,
    similarity_weight: Bm25Weight,
}

impl TermScorer {
    pub fn new(
        postings: SegmentPostings,
        fieldnorm_reader: FieldNormReader,
        similarity_weight: Bm25Weight,
    ) -> TermScorer {
        TermScorer {
            postings,
            fieldnorm_reader,
            similarity_weight,
        }
    }

    pub(crate) fn seek_block(&mut self, target_doc: DocId) {
        self.postings.block_cursor.seek_block(target_doc);
    }

    #[cfg(test)]
    pub fn create_for_test(
        doc_and_tfs: &[(DocId, u32)],
        fieldnorms: &[u32],
        similarity_weight: Bm25Weight,
    ) -> TermScorer {
        assert!(!doc_and_tfs.is_empty());
        assert!(
            doc_and_tfs
                .iter()
                .map(|(doc, _tf)| *doc)
                .max()
                .unwrap_or(0u32)
                < fieldnorms.len() as u32
        );
        let segment_postings =
            SegmentPostings::create_from_docs_and_tfs(doc_and_tfs, Some(fieldnorms));
        let fieldnorm_reader = FieldNormReader::for_test(fieldnorms);
        TermScorer::new(segment_postings, fieldnorm_reader, similarity_weight)
    }

    /// See `FreqReadingOption`.
    pub(crate) fn freq_reading_option(&self) -> FreqReadingOption {
        self.postings.block_cursor.freq_reading_option()
    }

    /// Returns the maximum score for the current block.
    ///
    /// In some rare case, the result may not be exact. In this case a lower value is returned,
    /// (and may lead us to return a lesser document).
    ///
    /// At index time, we store the (fieldnorm_id, term frequency) pair that maximizes the
    /// score assuming the average fieldnorm computed on this segment.
    ///
    /// Though extremely rare, it is theoretically possible that the actual average fieldnorm
    /// is different enough from the current segment average fieldnorm that the maximum over a
    /// specific is achieved on a different document.
    ///
    /// (The result is on the other hand guaranteed to be correct if there is only one segment).
    pub fn block_max_score(&mut self) -> Score {
        self.postings
            .block_cursor
            .block_max_score(&self.fieldnorm_reader, &self.similarity_weight)
    }

    pub fn term_freq(&self) -> u32 {
        self.postings.term_freq()
    }

    pub fn fieldnorm_id(&self) -> u8 {
        self.fieldnorm_reader.fieldnorm_id(self.doc())
    }

    pub fn explain(&self) -> Explanation {
        let fieldnorm_id = self.fieldnorm_id();
        let term_freq = self.term_freq();
        self.similarity_weight.explain(fieldnorm_id, term_freq)
    }

    pub fn max_score(&self) -> Score {
        self.similarity_weight.max_score()
    }

    pub fn last_doc_in_block(&self) -> DocId {
        self.postings.block_cursor.skip_reader().last_doc_in_block()
    }
}

impl DocSet for TermScorer {
    #[inline]
    fn advance(&mut self) -> DocId {
        self.postings.advance()
    }

    #[inline]
    fn seek(&mut self, target: DocId) -> DocId {
        debug_assert!(target >= self.doc());
        self.postings.seek(target)
    }

    #[inline]
    fn doc(&self) -> DocId {
        self.postings.doc()
    }

    fn size_hint(&self) -> u32 {
        self.postings.size_hint()
    }
}

impl Scorer for TermScorer {
    #[inline]
    fn score(&mut self) -> Score {
        let fieldnorm_id = self.fieldnorm_id();
        let term_freq = self.term_freq();
        self.similarity_weight.score(fieldnorm_id, term_freq)
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use crate::index::SegmentId;
    use crate::indexer::index_writer::MEMORY_BUDGET_NUM_BYTES_MIN;
    use crate::merge_policy::NoMergePolicy;
    use crate::postings::compression::COMPRESSION_BLOCK_SIZE;
    use crate::query::term_query::TermScorer;
    use crate::query::{Bm25Weight, EnableScoring, Scorer, TermQuery};
    use crate::schema::{IndexRecordOption, Schema, TEXT};
    use crate::{
        assert_nearly_equals, DocId, DocSet, Index, IndexWriter, Score, Searcher, Term, TERMINATED,
    };

    #[test]
    fn test_term_scorer_max_score() -> crate::Result<()> {
        let bm25_weight = Bm25Weight::for_one_term(3, 6, 10.0);
        let mut term_scorer = TermScorer::create_for_test(
            &[(2, 3), (3, 12), (7, 8)],
            &[0, 0, 10, 12, 0, 0, 0, 100],
            bm25_weight,
        );
        let max_scorer = term_scorer.max_score();
        crate::assert_nearly_equals!(max_scorer, 1.3990127);
        assert_eq!(term_scorer.doc(), 2);
        assert_eq!(term_scorer.term_freq(), 3);
        assert_nearly_equals!(term_scorer.block_max_score(), 1.3676447);
        assert_nearly_equals!(term_scorer.score(), 1.0892314);
        assert_eq!(term_scorer.advance(), 3);
        assert_eq!(term_scorer.doc(), 3);
        assert_eq!(term_scorer.term_freq(), 12);
        assert_nearly_equals!(term_scorer.score(), 1.3676447);
        assert_eq!(term_scorer.advance(), 7);
        assert_eq!(term_scorer.doc(), 7);
        assert_eq!(term_scorer.term_freq(), 8);
        assert_nearly_equals!(term_scorer.score(), 0.72015285);
        assert_eq!(term_scorer.advance(), TERMINATED);
        Ok(())
    }

    #[test]
    fn test_term_scorer_shallow_advance() -> crate::Result<()> {
        let bm25_weight = Bm25Weight::for_one_term(300, 1024, 10.0);
        let mut doc_and_tfs = vec![];
        for i in 0u32..300u32 {
            let doc = i * 10;
            doc_and_tfs.push((doc, 1u32 + doc % 3u32));
        }
        let fieldnorms: Vec<u32> = std::iter::repeat_n(10u32, 3_000).collect();
        let mut term_scorer = TermScorer::create_for_test(&doc_and_tfs, &fieldnorms, bm25_weight);
        assert_eq!(term_scorer.doc(), 0u32);
        term_scorer.seek_block(1289);
        assert_eq!(term_scorer.doc(), 0u32);
        term_scorer.seek(1289);
        assert_eq!(term_scorer.doc(), 1290);
        Ok(())
    }

    proptest! {
        #[test]
        fn test_term_scorer_block_max_score(term_freqs_fieldnorms in proptest::collection::vec((1u32..10u32, 0u32..100u32), 80..300)) {
        let term_doc_freq = term_freqs_fieldnorms.len();
         let doc_tfs: Vec<(u32, u32)> = term_freqs_fieldnorms.iter()
                   .cloned()
                  .enumerate()
                  .map(|(doc, (tf, _))| (doc as u32, tf))
                  .collect();

         let mut fieldnorms: Vec<u32> = vec![];
         for item in term_freqs_fieldnorms.iter().take(term_doc_freq) {
             let (tf, num_extra_terms) = item;
             fieldnorms.push(tf + num_extra_terms);
         }
         let average_fieldnorm = fieldnorms
             .iter()
             .cloned()
             .sum::<u32>() as Score / term_doc_freq as Score;
             // Average fieldnorm is over the entire index,
             // not necessarily the docs that are in the posting list.
             // For this reason we multiply by 1.1 to make a realistic value.
         let bm25_weight = Bm25Weight::for_one_term(term_doc_freq as u64,
            term_doc_freq as u64 * 10u64,
            average_fieldnorm);

         let mut term_scorer =
              TermScorer::create_for_test(&doc_tfs[..], &fieldnorms[..], bm25_weight);

         let docs: Vec<DocId> = (0..term_doc_freq).map(|doc| doc as DocId).collect();
         for block in docs.chunks(COMPRESSION_BLOCK_SIZE) {
             let block_max_score: Score = term_scorer.block_max_score();
             let mut block_max_score_computed: Score = 0.0;
             for &doc in block {
                assert_eq!(term_scorer.doc(), doc);
                block_max_score_computed = block_max_score_computed.max(term_scorer.score());
                term_scorer.advance();
             }
             assert_nearly_equals!(block_max_score_computed, block_max_score);
         }
        }
    }

    #[test]
    fn test_block_wand() {
        let mut doc_tfs: Vec<(u32, u32)> = vec![];
        for doc in 0u32..128u32 {
            doc_tfs.push((doc, 1u32));
        }
        for doc in 128u32..256u32 {
            doc_tfs.push((doc, if doc == 200 { 2u32 } else { 1u32 }));
        }
        doc_tfs.push((256, 1u32));
        doc_tfs.push((257, 3u32));
        doc_tfs.push((258, 1u32));

        let fieldnorms: Vec<u32> = std::iter::repeat_n(20u32, 300).collect();
        let bm25_weight = Bm25Weight::for_one_term(10, 129, 20.0);
        let mut docs = TermScorer::create_for_test(&doc_tfs[..], &fieldnorms[..], bm25_weight);
        assert_nearly_equals!(docs.block_max_score(), 2.5161593);
        docs.seek_block(135);
        assert_nearly_equals!(docs.block_max_score(), 3.4597192);
        docs.seek_block(256);
        // the block is not loaded yet.
        assert_nearly_equals!(docs.block_max_score(), 5.2971773);
        assert_eq!(256, docs.seek(256));
        assert_nearly_equals!(docs.block_max_score(), 3.9539647);
    }

    fn test_block_wand_aux(term_query: &TermQuery, searcher: &Searcher) -> crate::Result<()> {
        let term_weight =
            term_query.specialized_weight(EnableScoring::enabled_from_searcher(searcher))?;
        for reader in searcher.segment_readers() {
            let mut block_max_scores = vec![];
            let mut block_max_scores_b = vec![];
            let mut docs = vec![];
            {
                let mut term_scorer = term_weight.term_scorer_for_test(reader, 1.0)?.unwrap();
                while term_scorer.doc() != TERMINATED {
                    let mut score = term_scorer.score();
                    docs.push(term_scorer.doc());
                    for _ in 0..128 {
                        score = score.max(term_scorer.score());
                        if term_scorer.advance() == TERMINATED {
                            break;
                        }
                    }
                    block_max_scores.push(score);
                }
            }
            {
                let mut term_scorer = term_weight.term_scorer_for_test(reader, 1.0)?.unwrap();
                for d in docs {
                    term_scorer.seek_block(d);
                    block_max_scores_b.push(term_scorer.block_max_score());
                }
            }
            for (l, r) in block_max_scores
                .iter()
                .cloned()
                .zip(block_max_scores_b.iter().cloned())
            {
                assert_nearly_equals!(l, r);
            }
        }
        Ok(())
    }

    #[ignore]
    #[test]
    fn test_block_wand_long_test() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut writer: IndexWriter =
            index.writer_with_num_threads(3, 3 * MEMORY_BUDGET_NUM_BYTES_MIN)?;
        use rand::Rng;
        let mut rng = rand::rng();
        writer.set_merge_policy(Box::new(NoMergePolicy));
        for _ in 0..3_000 {
            let term_freq = rng.random_range(1..10000);
            let words: Vec<&str> = std::iter::repeat_n("bbbb", term_freq).collect();
            let text = words.join(" ");
            writer.add_document(doc!(text_field=>text))?;
        }
        writer.commit()?;
        let term_query = TermQuery::new(
            Term::from_field_text(text_field, "bbbb"),
            IndexRecordOption::WithFreqs,
        );
        let segment_ids: Vec<SegmentId>;
        let reader = index.reader()?;
        {
            let searcher = reader.searcher();
            segment_ids = searcher
                .segment_readers()
                .iter()
                .map(|segment| segment.segment_id())
                .collect();
            test_block_wand_aux(&term_query, &searcher)?;
        }
        writer.merge(&segment_ids[..]).wait().unwrap();
        {
            reader.reload()?;
            let searcher = reader.searcher();
            assert_eq!(searcher.segment_readers().len(), 1);
            test_block_wand_aux(&term_query, &searcher)?;
        }
        Ok(())
    }
}
