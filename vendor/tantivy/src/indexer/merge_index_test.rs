#[cfg(test)]
mod tests {
    use crate::collector::TopDocs;
    use crate::fastfield::AliveBitSet;
    use crate::index::Index;
    use crate::postings::Postings;
    use crate::query::QueryParser;
    use crate::schema::{
        self, BytesOptions, Facet, FacetOptions, IndexRecordOption, NumericOptions,
        TextFieldIndexing, TextOptions,
    };
    use crate::{DocAddress, DocSet, IndexSettings, IndexWriter, Term};

    fn create_test_index(index_settings: Option<IndexSettings>) -> crate::Result<Index> {
        let mut schema_builder = schema::Schema::builder();
        let int_options = NumericOptions::default()
            .set_fast()
            .set_stored()
            .set_indexed();
        let int_field = schema_builder.add_u64_field("intval", int_options);

        let bytes_options = BytesOptions::default().set_fast().set_indexed();
        let bytes_field = schema_builder.add_bytes_field("bytes", bytes_options);
        let facet_field = schema_builder.add_facet_field("facet", FacetOptions::default());

        let multi_numbers =
            schema_builder.add_u64_field("multi_numbers", NumericOptions::default().set_fast());
        let text_field_options = TextOptions::default()
            .set_indexing_options(
                TextFieldIndexing::default()
                    .set_index_option(schema::IndexRecordOption::WithFreqsAndPositions),
            )
            .set_stored();
        let text_field = schema_builder.add_text_field("text_field", text_field_options);
        let schema = schema_builder.build();

        let mut index_builder = Index::builder().schema(schema);
        if let Some(settings) = index_settings {
            index_builder = index_builder.settings(settings);
        }
        let index = index_builder.create_in_ram()?;

        {
            let mut index_writer = index.writer_for_tests()?;

            // segment 1 - range 1-3
            index_writer.add_document(doc!(int_field=>1_u64))?;
            index_writer.add_document(
                doc!(int_field=>3_u64, multi_numbers => 3_u64, multi_numbers => 4_u64, bytes_field => vec![1, 2, 3], text_field => "some text", facet_field=> Facet::from("/book/crime")),
            )?;
            index_writer.add_document(
                doc!(int_field=>1_u64, text_field=> "deleteme",  text_field => "ok text more text"),
            )?;
            index_writer.add_document(
                doc!(int_field=>2_u64, multi_numbers => 2_u64, multi_numbers => 3_u64, text_field => "ok text more text"),
            )?;

            index_writer.commit()?;
            index_writer.add_document(doc!(int_field=>20_u64, multi_numbers => 20_u64))?;

            let in_val = 1u64;
            index_writer.add_document(doc!(int_field=>in_val, text_field=> "deleteme" , text_field => "ok text more text", facet_field=> Facet::from("/book/crime")))?;
            index_writer.commit()?;
            let int_vals = [10u64, 5];
            index_writer.add_document( // position of this doc after delete in desc sorting = [2], in disjunct case [1]
                doc!(int_field=>int_vals[0], multi_numbers => 10_u64, multi_numbers => 11_u64, text_field=> "blubber", facet_field=> Facet::from("/book/fantasy")),
            )?;
            index_writer.add_document(doc!(int_field=>int_vals[1], text_field=> "deleteme"))?;
            index_writer.add_document(
                doc!(int_field=>1_000u64, multi_numbers => 1001_u64, multi_numbers => 1002_u64, bytes_field => vec![5, 5],text_field => "the biggest num")
            )?;

            index_writer.delete_term(Term::from_field_text(text_field, "deleteme"));
            index_writer.commit()?;
        }

        // Merging the segments
        {
            let segment_ids = index.searchable_segment_ids()?;
            let mut index_writer: IndexWriter = index.writer_for_tests()?;
            index_writer.merge(&segment_ids).wait()?;
            index_writer.wait_merging_threads()?;
        }
        Ok(index)
    }

    #[test]
    fn test_merge_index() {
        let index = create_test_index(Some(IndexSettings {
            ..Default::default()
        }))
        .unwrap();

        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        assert_eq!(searcher.segment_readers().len(), 1);
        let segment_reader = searcher.segment_readers().last().unwrap();

        let searcher = index.reader().unwrap().searcher();
        {
            let my_text_field = index.schema().get_field("text_field").unwrap();

            let do_search = |term: &str| {
                let query = QueryParser::for_index(&index, vec![my_text_field])
                    .parse_query(term)
                    .unwrap();
                let top_docs: Vec<(f32, DocAddress)> = searcher
                    .search(&query, &TopDocs::with_limit(3).order_by_score())
                    .unwrap();

                top_docs.iter().map(|el| el.1.doc_id).collect::<Vec<_>>()
            };

            assert_eq!(do_search("some"), vec![1]);
            assert_eq!(do_search("blubber"), vec![3]);
            assert_eq!(do_search("biggest"), vec![4]);
        }

        // postings file
        {
            let my_text_field = index.schema().get_field("text_field").unwrap();
            let term_a = Term::from_field_text(my_text_field, "text");
            let inverted_index = segment_reader.inverted_index(my_text_field).unwrap();
            let mut postings = inverted_index
                .read_postings(&term_a, IndexRecordOption::WithFreqsAndPositions)
                .unwrap()
                .unwrap();
            assert_eq!(postings.doc_freq(), 2);
            let fallback_bitset = AliveBitSet::for_test_from_deleted_docs(&[0], 100);
            assert_eq!(
                postings.doc_freq_given_deletes(
                    segment_reader.alive_bitset().unwrap_or(&fallback_bitset)
                ),
                2
            );

            assert_eq!(postings.term_freq(), 1);
            let mut output = vec![];
            postings.positions(&mut output);
            assert_eq!(output, vec![1]);
            postings.advance();

            assert_eq!(postings.term_freq(), 2);
            postings.positions(&mut output);
            assert_eq!(output, vec![1, 3]);
        }
    }
}
