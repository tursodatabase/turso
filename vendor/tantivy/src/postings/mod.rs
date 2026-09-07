//! Postings module (also called inverted index)

mod block_search;

pub(crate) use self::block_search::branchless_binary_search;

mod block_segment_postings;
pub(crate) mod compression;
mod indexing_context;
mod json_postings_writer;
mod loaded_postings;
mod per_field_postings_writer;
mod postings;
mod postings_writer;
mod recorder;
mod segment_postings;
/// Serializer module for the inverted index
pub mod serializer;
mod skip;
mod term_info;

pub(crate) use loaded_postings::LoadedPostings;
pub(crate) use stacker::compute_table_memory_size;

pub use self::block_segment_postings::BlockSegmentPostings;
pub(crate) use self::indexing_context::IndexingContext;
pub(crate) use self::per_field_postings_writer::PerFieldPostingsWriter;
pub use self::postings::Postings;
pub(crate) use self::postings_writer::{serialize_postings, IndexingPosition, PostingsWriter};
pub use self::segment_postings::SegmentPostings;
pub use self::serializer::{FieldSerializer, InvertedIndexSerializer};
pub(crate) use self::skip::{BlockInfo, SkipReader};
pub use self::term_info::TermInfo;

#[expect(clippy::enum_variant_names)]
#[derive(Debug, PartialEq, Clone, Copy, Eq)]
pub(crate) enum FreqReadingOption {
    NoFreq,
    SkipFreq,
    ReadFreq,
}

#[cfg(test)]
pub(crate) mod tests {
    use std::mem;

    use super::{InvertedIndexSerializer, Postings};
    use crate::docset::{DocSet, TERMINATED};
    use crate::fieldnorm::FieldNormReader;
    use crate::index::{Index, SegmentComponent, SegmentReader};
    use crate::indexer::operation::AddOperation;
    use crate::indexer::SegmentWriter;
    use crate::query::Scorer;
    use crate::schema::{
        Field, IndexRecordOption, Schema, Term, TextFieldIndexing, TextOptions, INDEXED, TEXT,
    };
    use crate::tokenizer::{SimpleTokenizer, MAX_TOKEN_LEN};
    use crate::{DocId, HasLen, IndexWriter, Score};

    #[test]
    pub fn test_position_write() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut segment = index.new_segment();
        let mut posting_serializer = InvertedIndexSerializer::open(&mut segment)?;
        let mut field_serializer = posting_serializer.new_field(text_field, 120 * 4, None)?;
        field_serializer.new_term("abc".as_bytes(), 12u32, true)?;
        for doc_id in 0u32..120u32 {
            let delta_positions = vec![1, 2, 3, 2];
            field_serializer.write_doc(doc_id, 4, &delta_positions);
        }
        field_serializer.close_term()?;
        mem::drop(field_serializer);
        posting_serializer.close()?;
        let read = segment.open_read(SegmentComponent::Positions)?;
        assert_eq!(read.len(), 207);
        Ok(())
    }

    #[test]
    pub fn test_skip_positions() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let title = schema_builder.add_text_field("title", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_for_tests()?;
        index_writer.add_document(doc!(title => r#"abc abc abc"#))?;
        index_writer.add_document(doc!(title => r#"abc be be be be abc"#))?;
        for _ in 0..1_000 {
            index_writer.add_document(doc!(title => r#"abc abc abc"#))?;
        }
        index_writer.add_document(doc!(title => r#"abc be be be be abc"#))?;
        index_writer.commit()?;

        let searcher = index.reader()?.searcher();
        let inverted_index = searcher.segment_reader(0u32).inverted_index(title)?;
        let term = Term::from_field_text(title, "abc");
        let mut positions = Vec::new();
        {
            let mut postings = inverted_index
                .read_postings(&term, IndexRecordOption::WithFreqsAndPositions)?
                .unwrap();
            assert_eq!(postings.doc(), 0);
            postings.positions(&mut positions);
            assert_eq!(&[0, 1, 2], &positions[..]);
            postings.positions(&mut positions);
            assert_eq!(&[0, 1, 2], &positions[..]);
            assert_eq!(postings.advance(), 1);
            assert_eq!(postings.doc(), 1);
            postings.positions(&mut positions);
            assert_eq!(&[0, 5], &positions[..]);
        }
        {
            let mut postings = inverted_index
                .read_postings(&term, IndexRecordOption::WithFreqsAndPositions)?
                .unwrap();
            assert_eq!(postings.doc(), 0);
            assert_eq!(postings.advance(), 1);
            postings.positions(&mut positions);
            assert_eq!(&[0, 5], &positions[..]);
        }
        {
            let mut postings = inverted_index
                .read_postings(&term, IndexRecordOption::WithFreqsAndPositions)?
                .unwrap();
            assert_eq!(postings.seek(1), 1);
            assert_eq!(postings.doc(), 1);
            postings.positions(&mut positions);
            assert_eq!(&[0, 5], &positions[..]);
        }
        {
            let mut postings = inverted_index
                .read_postings(&term, IndexRecordOption::WithFreqsAndPositions)?
                .unwrap();
            assert_eq!(postings.seek(1002), 1002);
            assert_eq!(postings.doc(), 1002);
            postings.positions(&mut positions);
            assert_eq!(&[0, 5], &positions[..]);
        }
        {
            let mut postings = inverted_index
                .read_postings(&term, IndexRecordOption::WithFreqsAndPositions)?
                .unwrap();
            assert_eq!(postings.seek(100), 100);
            assert_eq!(postings.seek(1002), 1002);
            assert_eq!(postings.doc(), 1002);
            postings.positions(&mut positions);
            assert_eq!(&[0, 5], &positions[..]);
        }
        Ok(())
    }

    #[test]
    pub fn test_index_max_length_token() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let text_options = TextOptions::default().set_indexing_options(
            TextFieldIndexing::default()
                .set_index_option(IndexRecordOption::WithFreqsAndPositions)
                .set_tokenizer("simple_no_truncation"),
        );
        let text_field = schema_builder.add_text_field("text", text_options);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        index
            .tokenizers()
            .register("simple_no_truncation", SimpleTokenizer::default());
        let reader = index.reader()?;
        let mut index_writer = index.writer_for_tests()?;

        let ok_token_text: String = "A".repeat(MAX_TOKEN_LEN);
        index_writer.add_document(doc!(text_field=>ok_token_text.clone()))?;
        index_writer.commit()?;
        reader.reload()?;
        let searcher = reader.searcher();
        let segment_reader = searcher.segment_reader(0u32);
        let inverted_index = segment_reader.inverted_index(text_field)?;
        assert_eq!(inverted_index.terms().num_terms(), 1);
        let mut bytes = vec![];
        assert!(inverted_index.terms().ord_to_term(0, &mut bytes)?);
        assert_eq!(&bytes[..], ok_token_text.as_bytes());

        Ok(())
    }

    #[test]
    pub fn test_drop_token_that_are_too_long() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let text_options = TextOptions::default().set_indexing_options(
            TextFieldIndexing::default()
                .set_index_option(IndexRecordOption::WithFreqsAndPositions)
                .set_tokenizer("simple_no_truncation"),
        );
        let text_field = schema_builder.add_text_field("text", text_options);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        index
            .tokenizers()
            .register("simple_no_truncation", SimpleTokenizer::default());
        let reader = index.reader()?;
        let mut index_writer = index.writer_for_tests()?;

        let mut exceeding_token_text: String = "A".repeat(MAX_TOKEN_LEN + 1);
        exceeding_token_text.push_str(" hello");
        index_writer.add_document(doc!(text_field=>exceeding_token_text))?;
        index_writer.commit()?;
        reader.reload()?;
        let searcher = reader.searcher();
        let segment_reader = searcher.segment_reader(0u32);
        let inverted_index = segment_reader.inverted_index(text_field)?;
        assert_eq!(inverted_index.terms().num_terms(), 1);
        let mut bytes = vec![];
        assert!(inverted_index.terms().ord_to_term(0, &mut bytes)?);
        assert_eq!(&bytes, b"hello");

        Ok(())
    }

    #[test]
    pub fn test_position_and_fieldnorm1() -> crate::Result<()> {
        let mut positions = Vec::new();
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let segment = index.new_segment();

        {
            let mut segment_writer =
                SegmentWriter::for_segment(15_000_000, segment.clone()).unwrap();
            {
                // checking that position works if the field has two values
                let op = AddOperation {
                    opstamp: 0u64,
                    document: doc!(
                       text_field => "a b a c a d a a.",
                       text_field => "d d d d a"
                    ),
                };
                segment_writer.add_document(op)?;
            }
            {
                let op = AddOperation {
                    opstamp: 1u64,
                    document: doc!(text_field => "b a"),
                };
                segment_writer.add_document(op).unwrap();
            }
            for i in 2..1000 {
                let mut text: String = "e ".repeat(i);
                text.push_str(" a");
                let op = AddOperation {
                    opstamp: 2u64,
                    document: doc!(text_field => text),
                };
                segment_writer.add_document(op).unwrap();
            }
            segment_writer.finalize()?;
        }
        {
            let segment_reader = SegmentReader::open(&segment)?;
            {
                let fieldnorm_reader = segment_reader.get_fieldnorms_reader(text_field)?;
                assert_eq!(fieldnorm_reader.fieldnorm(0), 8 + 5);
                assert_eq!(fieldnorm_reader.fieldnorm(1), 2);
                for i in 2..1000 {
                    assert_eq!(
                        fieldnorm_reader.fieldnorm_id(i),
                        FieldNormReader::fieldnorm_to_id(i + 1)
                    );
                }
            }
            {
                let term_a = Term::from_field_text(text_field, "abcdef");
                assert!(segment_reader
                    .inverted_index(term_a.field())?
                    .read_postings(&term_a, IndexRecordOption::WithFreqsAndPositions)?
                    .is_none());
            }
            {
                let term_a = Term::from_field_text(text_field, "a");
                let mut postings_a = segment_reader
                    .inverted_index(term_a.field())?
                    .read_postings(&term_a, IndexRecordOption::WithFreqsAndPositions)?
                    .unwrap();
                assert_eq!(postings_a.len(), 1000);
                assert_eq!(postings_a.doc(), 0);
                assert_eq!(postings_a.term_freq(), 6);
                postings_a.positions(&mut positions);
                assert_eq!(&positions[..], [0, 2, 4, 6, 7, 13]);
                assert_eq!(postings_a.advance(), 1u32);
                assert_eq!(postings_a.doc(), 1u32);
                assert_eq!(postings_a.term_freq(), 1);
                for i in 2u32..1000u32 {
                    assert_eq!(postings_a.advance(), i);
                    assert_eq!(postings_a.term_freq(), 1);
                    postings_a.positions(&mut positions);
                    assert_eq!(&positions[..], [i]);
                    assert_eq!(postings_a.doc(), i);
                }
                assert_eq!(postings_a.advance(), TERMINATED);
            }
            {
                let term_e = Term::from_field_text(text_field, "e");
                let mut postings_e = segment_reader
                    .inverted_index(term_e.field())?
                    .read_postings(&term_e, IndexRecordOption::WithFreqsAndPositions)?
                    .unwrap();
                assert_eq!(postings_e.len(), 1000 - 2);
                for i in 2u32..1000u32 {
                    assert_eq!(postings_e.term_freq(), i);
                    postings_e.positions(&mut positions);
                    assert_eq!(positions.len(), i as usize);
                    for j in 0..positions.len() {
                        assert_eq!(positions[j], (j as u32));
                    }
                    assert_eq!(postings_e.doc(), i);
                    postings_e.advance();
                }
                assert_eq!(postings_e.doc(), TERMINATED);
            }
        }
        Ok(())
    }

    #[test]
    pub fn test_position_and_fieldnorm2() -> crate::Result<()> {
        let mut positions: Vec<u32> = Vec::new();
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            let mut index_writer = index.writer_for_tests()?;
            index_writer.add_document(doc!(text_field => "g b b d c g c"))?;
            index_writer.add_document(doc!(text_field => "g a b b a d c g c"))?;
            index_writer.commit()?;
        }
        let term_a = Term::from_field_text(text_field, "a");
        let searcher = index.reader()?.searcher();
        let segment_reader = searcher.segment_reader(0);
        let mut postings = segment_reader
            .inverted_index(text_field)?
            .read_postings(&term_a, IndexRecordOption::WithFreqsAndPositions)?
            .unwrap();
        assert_eq!(postings.doc(), 1u32);
        postings.positions(&mut positions);
        assert_eq!(&positions[..], &[1u32, 4]);
        Ok(())
    }

    #[test]
    fn test_skip_next() -> crate::Result<()> {
        let term_0 = Term::from_field_u64(Field::from_field_id(0), 0);
        let term_1 = Term::from_field_u64(Field::from_field_id(0), 1);
        let term_2 = Term::from_field_u64(Field::from_field_id(0), 2);

        let num_docs = 300u32;

        let index = {
            let mut schema_builder = Schema::builder();
            let value_field = schema_builder.add_u64_field("value", INDEXED);
            let schema = schema_builder.build();
            let index = Index::create_in_ram(schema);
            {
                let mut index_writer = index.writer_for_tests()?;
                for i in 0u64..num_docs as u64 {
                    let doc = doc!(value_field => 2u64, value_field => i % 2u64);
                    index_writer.add_document(doc)?;
                }
                assert!(index_writer.commit().is_ok());
            }
            index
        };
        let searcher = index.reader()?.searcher();
        let segment_reader = searcher.segment_reader(0);

        // check that the basic usage works
        for i in 0..num_docs - 1 {
            for j in i + 1..num_docs {
                let mut segment_postings = segment_reader
                    .inverted_index(term_2.field())?
                    .read_postings(&term_2, IndexRecordOption::Basic)?
                    .unwrap();
                assert_eq!(segment_postings.seek(i), i);
                assert_eq!(segment_postings.doc(), i);

                assert_eq!(segment_postings.seek(j), j);
                assert_eq!(segment_postings.doc(), j);
            }
        }

        {
            let mut segment_postings = segment_reader
                .inverted_index(term_2.field())?
                .read_postings(&term_2, IndexRecordOption::Basic)?
                .unwrap();

            // check that `skip_next` advances the iterator
            assert_eq!(segment_postings.doc(), 0);

            assert_eq!(segment_postings.seek(1), 1);
            assert_eq!(segment_postings.doc(), 1);

            assert_eq!(segment_postings.seek(1), 1);
            assert_eq!(segment_postings.doc(), 1);

            // check that going beyond the end is handled
            assert_eq!(segment_postings.seek(num_docs), TERMINATED);
        }

        // check that filtering works
        {
            let mut segment_postings = segment_reader
                .inverted_index(term_0.field())?
                .read_postings(&term_0, IndexRecordOption::Basic)?
                .unwrap();

            for i in 0..num_docs / 2 {
                assert_eq!(segment_postings.seek(i * 2), i * 2);
                assert_eq!(segment_postings.doc(), i * 2);
            }

            let mut segment_postings = segment_reader
                .inverted_index(term_0.field())?
                .read_postings(&term_0, IndexRecordOption::Basic)?
                .unwrap();

            for i in 0..num_docs / 2 - 1 {
                assert!(segment_postings.seek(i * 2 + 1) > i * 2);
                assert_eq!(segment_postings.doc(), (i + 1) * 2);
            }
        }

        // delete some of the documents
        {
            let mut index_writer: IndexWriter = index.writer_for_tests()?;
            index_writer.delete_term(term_0);
            assert!(index_writer.commit().is_ok());
        }
        let searcher = index.reader()?.searcher();
        assert_eq!(searcher.segment_readers().len(), 1);
        let segment_reader = searcher.segment_reader(0);

        // make sure seeking still works
        for i in 0..num_docs {
            let mut segment_postings = segment_reader
                .inverted_index(term_2.field())?
                .read_postings(&term_2, IndexRecordOption::Basic)?
                .unwrap();

            assert_eq!(segment_postings.seek(i), i);
            assert_eq!(segment_postings.doc(), i);
            if i % 2 == 0 {
                assert!(segment_reader.is_deleted(i));
            }
        }

        // now try with a longer sequence
        {
            let mut segment_postings = segment_reader
                .inverted_index(term_2.field())?
                .read_postings(&term_2, IndexRecordOption::Basic)?
                .unwrap();

            let mut last = 2; // start from 5 to avoid seeking to 3 twice
            let mut cur = 3;
            loop {
                let seek = segment_postings.seek(cur);
                if seek == TERMINATED {
                    break;
                }
                assert_eq!(seek, segment_postings.doc());
                if seek == cur {
                    assert_eq!(segment_postings.doc(), cur);
                } else {
                    assert_eq!(segment_postings.doc(), cur + 1);
                }
                let next = cur + last;
                last = cur;
                cur = next;
            }
            assert_eq!(cur, 377);
        }

        // delete everything else
        {
            let mut index_writer: IndexWriter = index.writer_for_tests()?;
            index_writer.delete_term(term_1);
            assert!(index_writer.commit().is_ok());
        }
        let searcher = index.reader()?.searcher();

        // finally, check that it's empty
        {
            let searchable_segment_ids = index.searchable_segment_ids()?;
            assert!(searchable_segment_ids.is_empty());
            assert_eq!(searcher.num_docs(), 0);
        }
        Ok(())
    }

    /// Wraps a given docset, and forward all call but the
    /// `.skip_next(...)`. This is useful to test that a specialized
    /// implementation of `.skip_next(...)` is consistent
    /// with the default implementation.
    pub(crate) struct UnoptimizedDocSet<TDocSet: DocSet>(TDocSet);

    impl<TDocSet: DocSet> UnoptimizedDocSet<TDocSet> {
        pub fn wrap(docset: TDocSet) -> UnoptimizedDocSet<TDocSet> {
            UnoptimizedDocSet(docset)
        }
    }

    impl<TDocSet: DocSet> DocSet for UnoptimizedDocSet<TDocSet> {
        fn advance(&mut self) -> DocId {
            self.0.advance()
        }

        fn doc(&self) -> DocId {
            self.0.doc()
        }

        fn size_hint(&self) -> u32 {
            self.0.size_hint()
        }
    }

    impl<TScorer: Scorer> Scorer for UnoptimizedDocSet<TScorer> {
        #[inline]
        fn score(&mut self) -> Score {
            self.0.score()
        }
    }

    pub fn test_skip_against_unoptimized<F: Fn() -> Box<dyn DocSet>>(
        postings_factory: F,
        targets: Vec<u32>,
    ) {
        for target in targets {
            let mut postings_opt = postings_factory();
            if target < postings_opt.doc() {
                continue;
            }
            let mut postings_unopt = UnoptimizedDocSet::wrap(postings_factory());
            let skip_result_opt = postings_opt.seek(target);
            let skip_result_unopt = postings_unopt.seek(target);
            assert_eq!(
                skip_result_unopt, skip_result_opt,
                "Failed while skipping to {target}"
            );
            assert!(skip_result_opt >= target);
            assert_eq!(skip_result_opt, postings_opt.doc());
            if skip_result_opt == TERMINATED {
                return;
            }
            while postings_opt.doc() != TERMINATED {
                assert_eq!(postings_opt.doc(), postings_unopt.doc());
                assert_eq!(postings_opt.advance(), postings_unopt.advance());
            }
        }
    }
}

#[cfg(all(test, feature = "unstable"))]
mod bench {
    use once_cell::sync::Lazy;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use test::{self, Bencher};

    use crate::docset::TERMINATED;
    use crate::query::Intersection;
    use crate::schema::{Field, IndexRecordOption, Schema, TantivyDocument, Term, STRING};
    use crate::{tests, DocSet, Index, IndexWriter};

    pub static TERM_A: Lazy<Term> = Lazy::new(|| {
        let field = Field::from_field_id(0);
        Term::from_field_text(field, "a")
    });
    pub static TERM_B: Lazy<Term> = Lazy::new(|| {
        let field = Field::from_field_id(0);
        Term::from_field_text(field, "b")
    });
    pub static TERM_C: Lazy<Term> = Lazy::new(|| {
        let field = Field::from_field_id(0);
        Term::from_field_text(field, "c")
    });
    pub static TERM_D: Lazy<Term> = Lazy::new(|| {
        let field = Field::from_field_id(0);
        Term::from_field_text(field, "d")
    });

    pub static INDEX: Lazy<Index> = Lazy::new(|| {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", STRING);
        let schema = schema_builder.build();

        let mut rng: StdRng = StdRng::from_seed([1u8; 32]);

        let index = Index::create_in_ram(schema);
        let posting_list_size = 1_000_000;
        {
            let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();
            for _ in 0..posting_list_size {
                let mut doc = TantivyDocument::default();
                if rng.random_bool(1f64 / 15f64) {
                    doc.add_text(text_field, "a");
                }
                if rng.random_bool(1f64 / 10f64) {
                    doc.add_text(text_field, "b");
                }
                if rng.random_bool(1f64 / 5f64) {
                    doc.add_text(text_field, "c");
                }
                doc.add_text(text_field, "d");
                index_writer.add_document(doc).unwrap();
            }
            assert!(index_writer.commit().is_ok());
        }
        index
    });

    #[bench]
    fn bench_segment_postings(b: &mut Bencher) {
        let reader = INDEX.reader().unwrap();
        let searcher = reader.searcher();
        let segment_reader = searcher.segment_reader(0);

        b.iter(|| {
            let mut segment_postings = segment_reader
                .inverted_index(TERM_A.field())
                .unwrap()
                .read_postings(&TERM_A, IndexRecordOption::Basic)
                .unwrap()
                .unwrap();
            while segment_postings.advance() != TERMINATED {}
        });
    }

    #[bench]
    fn bench_segment_intersection(b: &mut Bencher) {
        let reader = INDEX.reader().unwrap();
        let searcher = reader.searcher();
        let segment_reader = searcher.segment_reader(0);
        b.iter(|| {
            let segment_postings_a = segment_reader
                .inverted_index(TERM_A.field())
                .unwrap()
                .read_postings(&TERM_A, IndexRecordOption::Basic)
                .unwrap()
                .unwrap();
            let segment_postings_b = segment_reader
                .inverted_index(TERM_B.field())
                .unwrap()
                .read_postings(&TERM_B, IndexRecordOption::Basic)
                .unwrap()
                .unwrap();
            let segment_postings_c = segment_reader
                .inverted_index(TERM_C.field())
                .unwrap()
                .read_postings(&TERM_C, IndexRecordOption::Basic)
                .unwrap()
                .unwrap();
            let segment_postings_d = segment_reader
                .inverted_index(TERM_D.field())
                .unwrap()
                .read_postings(&TERM_D, IndexRecordOption::Basic)
                .unwrap()
                .unwrap();
            let mut intersection = Intersection::new(
                vec![
                    segment_postings_a,
                    segment_postings_b,
                    segment_postings_c,
                    segment_postings_d,
                ],
                reader.searcher().num_docs() as u32,
            );
            while intersection.advance() != TERMINATED {}
        });
    }

    fn bench_skip_next(p: f64, b: &mut Bencher) {
        let reader = INDEX.reader().unwrap();
        let searcher = reader.searcher();
        let segment_reader = searcher.segment_reader(0);
        let docs = tests::sample(segment_reader.num_docs(), p);

        let mut segment_postings = segment_reader
            .inverted_index(TERM_A.field())
            .unwrap()
            .read_postings(&TERM_A, IndexRecordOption::Basic)
            .unwrap()
            .unwrap();

        let mut existing_docs = Vec::new();
        for doc in &docs {
            if *doc >= segment_postings.doc() {
                existing_docs.push(*doc);
                if segment_postings.seek(*doc) == TERMINATED {
                    break;
                }
            }
        }

        b.iter(|| {
            let mut segment_postings = segment_reader
                .inverted_index(TERM_A.field())
                .unwrap()
                .read_postings(&TERM_A, IndexRecordOption::Basic)
                .unwrap()
                .unwrap();
            for doc in &existing_docs {
                if segment_postings.seek(*doc) == TERMINATED {
                    break;
                }
            }
        });
    }

    #[bench]
    fn bench_skip_next_p01(b: &mut Bencher) {
        bench_skip_next(0.001, b);
    }

    #[bench]
    fn bench_skip_next_p1(b: &mut Bencher) {
        bench_skip_next(0.01, b);
    }

    #[bench]
    fn bench_skip_next_p10(b: &mut Bencher) {
        bench_skip_next(0.1, b);
    }

    #[bench]
    fn bench_skip_next_p90(b: &mut Bencher) {
        bench_skip_next(0.9, b);
    }

    #[bench]
    fn bench_iterate_segment_postings(b: &mut Bencher) {
        let reader = INDEX.reader().unwrap();
        let searcher = reader.searcher();
        let segment_reader = searcher.segment_reader(0);
        b.iter(|| {
            let n: u32 = test::black_box(17);
            let mut segment_postings = segment_reader
                .inverted_index(TERM_A.field())
                .unwrap()
                .read_postings(&TERM_A, IndexRecordOption::Basic)
                .unwrap()
                .unwrap();
            let mut s = 0u32;
            while segment_postings.doc() != TERMINATED {
                s += (segment_postings.doc() & n) % 1024;
                segment_postings.advance();
            }
            s
        });
    }
}
