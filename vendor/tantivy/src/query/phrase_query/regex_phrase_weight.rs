use std::sync::Arc;

use common::BitSet;
use tantivy_fst::Regex;

use super::PhraseScorer;
use crate::fieldnorm::FieldNormReader;
use crate::index::SegmentReader;
use crate::postings::{LoadedPostings, Postings, SegmentPostings, TermInfo};
use crate::query::bm25::Bm25Weight;
use crate::query::explanation::does_not_match;
use crate::query::union::{BitSetPostingUnion, SimpleUnion};
use crate::query::{AutomatonWeight, BitSetDocSet, EmptyScorer, Explanation, Scorer, Weight};
use crate::schema::{Field, IndexRecordOption};
use crate::{DocId, DocSet, InvertedIndexReader, Score};

type UnionType = SimpleUnion<Box<dyn Postings + 'static>>;

/// The `RegexPhraseWeight` is the weight associated to a regex phrase query.
/// See RegexPhraseWeight::get_union_from_term_infos for some design decisions.
pub struct RegexPhraseWeight {
    field: Field,
    phrase_terms: Vec<(usize, String)>,
    similarity_weight_opt: Option<Bm25Weight>,
    slop: u32,
    max_expansions: u32,
}

impl RegexPhraseWeight {
    /// Creates a new phrase weight.
    /// If `similarity_weight_opt` is None, then scoring is disabled
    pub fn new(
        field: Field,
        phrase_terms: Vec<(usize, String)>,
        similarity_weight_opt: Option<Bm25Weight>,
        max_expansions: u32,
        slop: u32,
    ) -> RegexPhraseWeight {
        RegexPhraseWeight {
            field,
            phrase_terms,
            similarity_weight_opt,
            slop,
            max_expansions,
        }
    }

    fn fieldnorm_reader(&self, reader: &SegmentReader) -> crate::Result<FieldNormReader> {
        if self.similarity_weight_opt.is_some() {
            if let Some(fieldnorm_reader) = reader.fieldnorms_readers().get_field(self.field)? {
                return Ok(fieldnorm_reader);
            }
        }
        Ok(FieldNormReader::constant(reader.max_doc(), 1))
    }

    pub(crate) fn phrase_scorer(
        &self,
        reader: &SegmentReader,
        boost: Score,
    ) -> crate::Result<Option<PhraseScorer<UnionType>>> {
        let similarity_weight_opt = self
            .similarity_weight_opt
            .as_ref()
            .map(|similarity_weight| similarity_weight.boost_by(boost));
        let fieldnorm_reader = self.fieldnorm_reader(reader)?;
        let mut posting_lists = Vec::new();
        let inverted_index = reader.inverted_index(self.field)?;
        let mut num_terms = 0;
        for &(offset, ref term) in &self.phrase_terms {
            let regex = Regex::new(term)
                .map_err(|e| crate::TantivyError::InvalidArgument(format!("Invalid regex: {e}")))?;

            let automaton: AutomatonWeight<Regex> =
                AutomatonWeight::new(self.field, Arc::new(regex));
            let term_infos = automaton.get_match_term_infos(reader)?;
            // If term_infos is empty, the phrase can not match any documents.
            if term_infos.is_empty() {
                return Ok(None);
            }
            num_terms += term_infos.len();
            if num_terms > self.max_expansions as usize {
                return Err(crate::TantivyError::InvalidArgument(format!(
                    "Phrase query exceeded max expansions {num_terms}"
                )));
            }
            let union = Self::get_union_from_term_infos(&term_infos, reader, &inverted_index)?;

            posting_lists.push((offset, union));
        }

        Ok(Some(PhraseScorer::new(
            posting_lists,
            similarity_weight_opt,
            fieldnorm_reader,
            self.slop,
        )))
    }

    /// Add all docs of the term to the docset
    fn add_to_bitset(
        inverted_index: &InvertedIndexReader,
        term_info: &TermInfo,
        doc_bitset: &mut BitSet,
    ) -> crate::Result<()> {
        let mut block_segment_postings = inverted_index
            .read_block_postings_from_terminfo(term_info, IndexRecordOption::Basic)?;
        loop {
            let docs = block_segment_postings.docs();
            if docs.is_empty() {
                break;
            }
            for &doc in docs {
                doc_bitset.insert(doc);
            }
            block_segment_postings.advance();
        }
        Ok(())
    }

    /// This function generates a union of document sets from multiple term information
    /// (`TermInfo`).
    ///
    /// It uses bucketing based on term frequency to optimize query performance and memory usage.
    /// The terms are divided into buckets based on their document frequency (the number of
    /// documents they appear in).
    ///
    /// ### Bucketing Strategy:
    /// Once a bucket contains more than 512 terms, it is moved to the end of the list and replaced
    /// with a new empty bucket.
    ///
    /// - **Sparse Term Buckets**: Terms with document frequency `< 100`.
    ///
    ///   Each sparse bucket contains:
    ///   - A `BitSet` to efficiently track which document IDs are present in the bucket, which is
    ///     used to drive the `DocSet`.
    ///   - A `Vec<LoadedPostings>` to store the postings for each term in that bucket.
    ///
    /// - **Other Term Buckets**:
    ///   - **Bucket 0**: Terms appearing in less than `0.1%` of documents.
    ///   - **Bucket 1**: Terms appearing in `0.1%` to `1%` of documents.
    ///   - **Bucket 2**: Terms appearing in `1%` to `10%` of documents.
    ///   - **Bucket 3**: Terms appearing in more than `10%` of documents.
    ///
    ///   Each bucket contains:
    ///   - A `BitSet` to efficiently track which document IDs are present in the bucket.
    ///   - A `Vec<SegmentPostings>` to store the postings for each term in that bucket.
    ///
    /// ### Design Choices:
    /// The main cost for a _unbucketed_ regex phrase query with a medium/high amount of terms is
    /// the `append_positions_with_offset` from `Postings`.
    /// We don't know which docsets hit, so we need to scan all of them to check if they contain the
    /// docid.
    /// The bucketing strategy groups less common DocSets together, so we can rule out the
    /// whole docset group in many cases.
    ///
    /// E.g. consider the phrase "th* world"
    /// It contains the term "the", which may occur in almost all documents.
    /// It may also contain 10_000s very rare terms like "theologian".
    ///
    /// For very low-frequency terms (sparse terms), we use `LoadedPostings` and aggregate
    /// their document IDs into a `BitSet`, which is more memory-efficient than using
    /// `SegmentPostings`. E.g. 100_000 terms with SegmentPostings would consume 184MB.
    /// `SegmentPostings` uses memory equivalent to 460 docids. The 100 docs limit should be
    /// fine as long as a term doesn't have too many positions per doc.
    ///
    /// ### Future Optimization:
    /// A larger performance improvement would be an additional partitioning of the space
    /// vertically of u16::MAX blocks, where we mark which docset ord has values in each block.
    /// E.g. partitioning in a index with 5 million documents this would reduce the number of
    /// docsets to scan to around 1/20 in the sparse term bucket where the terms only have a few
    /// docs. For higher cardinality buckets this is irrelevant as they are in most blocks.
    ///
    /// Use Roaring Bitmaps for sparse terms. The full bitvec is main memory consumer currently.
    pub(crate) fn get_union_from_term_infos(
        term_infos: &[TermInfo],
        reader: &SegmentReader,
        inverted_index: &InvertedIndexReader,
    ) -> crate::Result<UnionType> {
        let max_doc = reader.max_doc();

        // Buckets for sparse terms
        let mut sparse_buckets: Vec<(BitSet, Vec<LoadedPostings>)> =
            vec![(BitSet::with_max_value(max_doc), Vec::new())];

        // Buckets for other terms based on document frequency percentages:
        // - Bucket 0: Terms appearing in less than 0.1% of documents
        // - Bucket 1: Terms appearing in 0.1% to 1% of documents
        // - Bucket 2: Terms appearing in 1% to 10% of documents
        // - Bucket 3: Terms appearing in more than 10% of documents
        let mut buckets: Vec<(BitSet, Vec<SegmentPostings>)> = (0..4)
            .map(|_| (BitSet::with_max_value(max_doc), Vec::new()))
            .collect();

        const SPARSE_TERM_DOC_THRESHOLD: u32 = 100;

        for term_info in term_infos {
            let mut term_posting = inverted_index
                .read_postings_from_terminfo(term_info, IndexRecordOption::WithFreqsAndPositions)?;
            let num_docs = term_posting.doc_freq();

            if num_docs < SPARSE_TERM_DOC_THRESHOLD {
                let current_bucket = &mut sparse_buckets[0];
                Self::add_to_bitset(inverted_index, term_info, &mut current_bucket.0)?;
                let docset = LoadedPostings::load(&mut term_posting);
                current_bucket.1.push(docset);

                // Move the bucket to the end if the term limit is reached
                if current_bucket.1.len() == 512 {
                    sparse_buckets.push((BitSet::with_max_value(max_doc), Vec::new()));
                    let end_index = sparse_buckets.len() - 1;
                    sparse_buckets.swap(0, end_index);
                }
            } else {
                // Calculate the percentage of documents the term appears in
                let doc_freq_percentage = (num_docs as f32) / (max_doc as f32) * 100.0;

                // Determine the appropriate bucket based on percentage thresholds
                let bucket_index = if doc_freq_percentage < 0.1 {
                    0
                } else if doc_freq_percentage < 1.0 {
                    1
                } else if doc_freq_percentage < 10.0 {
                    2
                } else {
                    3
                };
                let bucket = &mut buckets[bucket_index];

                // Add term postings to the appropriate bucket
                Self::add_to_bitset(inverted_index, term_info, &mut bucket.0)?;
                bucket.1.push(term_posting);

                // Move the bucket to the end if the term limit is reached
                if bucket.1.len() == 512 {
                    buckets.push((BitSet::with_max_value(max_doc), Vec::new()));
                    let end_index = buckets.len() - 1;
                    buckets.swap(bucket_index, end_index);
                }
            }
        }

        // Build unions for sparse term buckets
        let sparse_term_docsets: Vec<_> = sparse_buckets
            .into_iter()
            .filter(|(_, postings)| !postings.is_empty())
            .map(|(bitset, postings)| {
                BitSetPostingUnion::build(postings, BitSetDocSet::from(bitset))
            })
            .collect();
        let sparse_term_unions = SimpleUnion::build(sparse_term_docsets);

        // Build unions for other term buckets
        let bitset_unions_per_bucket: Vec<_> = buckets
            .into_iter()
            .filter(|(_, postings)| !postings.is_empty())
            .map(|(bitset, postings)| {
                BitSetPostingUnion::build(postings, BitSetDocSet::from(bitset))
            })
            .collect();
        let other_union = SimpleUnion::build(bitset_unions_per_bucket);

        let union: SimpleUnion<Box<dyn Postings + 'static>> =
            SimpleUnion::build(vec![Box::new(sparse_term_unions), Box::new(other_union)]);

        // Return a union of sparse term unions and other term unions
        Ok(union)
    }
}

impl Weight for RegexPhraseWeight {
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
    use proptest::prelude::*;
    use rand::seq::SliceRandom;

    use super::super::tests::create_index;
    use crate::docset::TERMINATED;
    use crate::query::{wildcard_query_to_regex_str, EnableScoring, RegexPhraseQuery};
    use crate::DocSet;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(50))]
        #[test]
        fn test_phrase_regex_with_random_strings(mut random_strings in proptest::collection::vec("[c-z ]{0,10}", 1..100), num_occurrences in 1..150_usize) {
            let mut rng = rand::rng();

            // Insert "aaa ccc" the specified number of times into the list
            for _ in 0..num_occurrences {
                random_strings.push("aaa ccc".to_string());
            }
            // Shuffle the list, which now contains random strings and the inserted "aaa ccc"
            random_strings.shuffle(&mut rng);

            // Compute the positions of "aaa ccc" after the shuffle
            let aaa_ccc_positions: Vec<usize> = random_strings
                .iter()
                .enumerate()
                .filter_map(|(idx, s)| if s == "aaa ccc" { Some(idx) } else { None })
                .collect();

            // Create the index with random strings and the fixed string "aaa ccc"
            let index = create_index(&random_strings.iter().map(AsRef::as_ref).collect::<Vec<&str>>())?;
            let schema = index.schema();
            let text_field = schema.get_field("text").unwrap();
            let searcher = index.reader()?.searcher();

            let phrase_query = RegexPhraseQuery::new(text_field, vec![wildcard_query_to_regex_str("a*"), wildcard_query_to_regex_str("c*")]);

            let enable_scoring = EnableScoring::enabled_from_searcher(&searcher);
            let phrase_weight = phrase_query.regex_phrase_weight(enable_scoring).unwrap();
            let mut phrase_scorer = phrase_weight
                .phrase_scorer(searcher.segment_reader(0u32), 1.0)?
                .unwrap();

            // Check if the scorer returns the correct document positions for "aaa ccc"
            for expected_doc in aaa_ccc_positions {
                prop_assert_eq!(phrase_scorer.doc(), expected_doc as u32);
                prop_assert_eq!(phrase_scorer.phrase_count(), 1);
                phrase_scorer.advance();
            }
            prop_assert_eq!(phrase_scorer.advance(), TERMINATED);
        }
    }

    #[test]
    pub fn test_phrase_count() -> crate::Result<()> {
        let index = create_index(&["a c", "a a b d a b c", " a b"])?;
        let schema = index.schema();
        let text_field = schema.get_field("text").unwrap();
        let searcher = index.reader()?.searcher();
        let phrase_query = RegexPhraseQuery::new(text_field, vec!["a".into(), "b".into()]);
        let enable_scoring = EnableScoring::enabled_from_searcher(&searcher);
        let phrase_weight = phrase_query.regex_phrase_weight(enable_scoring).unwrap();
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
    pub fn test_phrase_wildcard() -> crate::Result<()> {
        let index = create_index(&["a c", "a aa b d ad b c", " ac b", "bac b"])?;
        let schema = index.schema();
        let text_field = schema.get_field("text").unwrap();
        let searcher = index.reader()?.searcher();
        let phrase_query = RegexPhraseQuery::new(text_field, vec!["a.*".into(), "b".into()]);
        let enable_scoring = EnableScoring::enabled_from_searcher(&searcher);
        let phrase_weight = phrase_query.regex_phrase_weight(enable_scoring).unwrap();
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
    pub fn test_phrase_regex() -> crate::Result<()> {
        let index = create_index(&["ba b", "a aa b d ad b c", "bac b"])?;
        let schema = index.schema();
        let text_field = schema.get_field("text").unwrap();
        let searcher = index.reader()?.searcher();
        let phrase_query = RegexPhraseQuery::new(text_field, vec!["b?a.*".into(), "b".into()]);
        let enable_scoring = EnableScoring::enabled_from_searcher(&searcher);
        let phrase_weight = phrase_query.regex_phrase_weight(enable_scoring).unwrap();
        let mut phrase_scorer = phrase_weight
            .phrase_scorer(searcher.segment_reader(0u32), 1.0)?
            .unwrap();
        assert_eq!(phrase_scorer.doc(), 0);
        assert_eq!(phrase_scorer.phrase_count(), 1);
        assert_eq!(phrase_scorer.advance(), 1);
        assert_eq!(phrase_scorer.phrase_count(), 2);
        assert_eq!(phrase_scorer.advance(), 2);
        assert_eq!(phrase_scorer.doc(), 2);
        assert_eq!(phrase_scorer.phrase_count(), 1);
        assert_eq!(phrase_scorer.advance(), TERMINATED);

        Ok(())
    }

    #[test]
    pub fn test_phrase_regex_with_slop() -> crate::Result<()> {
        let index = create_index(&["aaa bbb ccc ___ abc ddd bbb ccc"])?;
        let schema = index.schema();
        let text_field = schema.get_field("text").unwrap();
        let searcher = index.reader()?.searcher();
        let mut phrase_query = RegexPhraseQuery::new(text_field, vec!["a.*".into(), "c.*".into()]);
        phrase_query.set_slop(1);
        let enable_scoring = EnableScoring::enabled_from_searcher(&searcher);
        let phrase_weight = phrase_query.regex_phrase_weight(enable_scoring).unwrap();
        let mut phrase_scorer = phrase_weight
            .phrase_scorer(searcher.segment_reader(0u32), 1.0)?
            .unwrap();
        assert_eq!(phrase_scorer.doc(), 0);
        assert_eq!(phrase_scorer.phrase_count(), 1);
        assert_eq!(phrase_scorer.advance(), TERMINATED);

        phrase_query.set_slop(2);
        let enable_scoring = EnableScoring::enabled_from_searcher(&searcher);
        let phrase_weight = phrase_query.regex_phrase_weight(enable_scoring).unwrap();
        let mut phrase_scorer = phrase_weight
            .phrase_scorer(searcher.segment_reader(0u32), 1.0)?
            .unwrap();
        assert_eq!(phrase_scorer.doc(), 0);
        assert_eq!(phrase_scorer.phrase_count(), 2);
        assert_eq!(phrase_scorer.advance(), TERMINATED);

        Ok(())
    }

    #[test]
    pub fn test_phrase_regex_double_wildcard() -> crate::Result<()> {
        let index = create_index(&["baaab bccccb"])?;
        let schema = index.schema();
        let text_field = schema.get_field("text").unwrap();
        let searcher = index.reader()?.searcher();
        let phrase_query = RegexPhraseQuery::new(
            text_field,
            vec![
                wildcard_query_to_regex_str("*a*"),
                wildcard_query_to_regex_str("*c*"),
            ],
        );
        let enable_scoring = EnableScoring::enabled_from_searcher(&searcher);
        let phrase_weight = phrase_query.regex_phrase_weight(enable_scoring).unwrap();
        let mut phrase_scorer = phrase_weight
            .phrase_scorer(searcher.segment_reader(0u32), 1.0)?
            .unwrap();
        assert_eq!(phrase_scorer.doc(), 0);
        assert_eq!(phrase_scorer.phrase_count(), 1);
        assert_eq!(phrase_scorer.advance(), TERMINATED);
        Ok(())
    }
}
