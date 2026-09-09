//! Indexing and merging data.
//!
//! Contains code to create and merge segments.
//! `IndexWriter` is the main entry point for that, which created from
//! [`Index::writer`](crate::Index::writer).

/// Delete queue implementation for broadcasting delete operations to consumers.
pub(crate) mod delete_queue;
pub(crate) mod path_to_unordered_id;

pub(crate) mod doc_id_mapping;
mod doc_opstamp_mapping;
mod flat_map_with_buffer;
pub(crate) mod index_writer;
pub(crate) mod index_writer_status;
pub(crate) mod indexing_term;
mod log_merge_policy;
mod merge_index_test;
mod merge_operation;
pub(crate) mod merge_policy;
pub(crate) mod merger;
pub(crate) mod operation;
pub(crate) mod prepared_commit;
mod segment_entry;
mod segment_manager;
mod segment_register;
pub(crate) mod segment_serializer;
pub(crate) mod segment_updater;
pub(crate) mod segment_writer;
pub(crate) mod single_segment_index_writer;
mod stamper;

use crossbeam_channel as channel;
use smallvec::SmallVec;

pub use self::index_writer::{advance_deletes, IndexWriter, IndexWriterOptions};
pub use self::log_merge_policy::LogMergePolicy;
pub use self::merge_operation::MergeOperation;
pub use self::merge_policy::{MergeCandidate, MergePolicy, NoMergePolicy};
pub use self::operation::{AddOperation, DeleteOperation, UserOperation};
pub use self::prepared_commit::PreparedCommit;
pub use self::segment_entry::SegmentEntry;
pub(crate) use self::segment_serializer::SegmentSerializer;
pub use self::segment_updater::{merge_filtered_segments, merge_indices};
pub use self::segment_writer::SegmentWriter;
pub use self::single_segment_index_writer::SingleSegmentIndexWriter;

/// Alias for the default merge policy, which is the `LogMergePolicy`.
pub type DefaultMergePolicy = LogMergePolicy;

// Batch of documents.
// Most of the time, users will send operation one-by-one, but it can be useful to
// send them as a small block to ensure that
// - all docs in the operation will happen on the same segment and continuous doc_ids.
// - all operations in the group are committed at the same time, making the group
// atomic.
type AddBatch<D> = SmallVec<[AddOperation<D>; 4]>;
type AddBatchSender<D> = channel::Sender<AddBatch<D>>;
type AddBatchReceiver<D> = channel::Receiver<AddBatch<D>>;

#[cfg(feature = "mmap")]
#[cfg(test)]
mod tests_mmap {

    use common::ByteCount;

    use crate::aggregation::agg_req::Aggregations;
    use crate::aggregation::agg_result::AggregationResults;
    use crate::aggregation::AggregationCollector;
    use crate::collector::{Count, TopDocs};
    use crate::index::FieldMetadata;
    use crate::query::{AllQuery, QueryParser};
    use crate::schema::{JsonObjectOptions, Schema, Type, FAST, INDEXED, STORED, TEXT};
    use crate::{Index, IndexWriter, Term};

    #[test]
    fn test_advance_delete_bug() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let index = Index::create_from_tempdir(schema_builder.build())?;
        let mut index_writer: IndexWriter = index.writer_for_tests()?;
        // there must be one deleted document in the segment
        index_writer.add_document(doc!(text_field=>"b"))?;
        index_writer.delete_term(Term::from_field_text(text_field, "b"));
        // we need enough data to trigger the bug (at least 32 documents)
        for _ in 0..32 {
            index_writer.add_document(doc!(text_field=>"c"))?;
        }
        index_writer.commit()?;
        Ok(())
    }

    #[test]
    fn test_json_field_expand_dots_disabled_dot_escaped_required() {
        let mut schema_builder = Schema::builder();
        let json_field = schema_builder.add_json_field("json", TEXT);
        let index = Index::create_in_ram(schema_builder.build());
        let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();
        let json = serde_json::json!({"k8s.container.name": "prometheus", "val": "hello"});
        index_writer.add_document(doc!(json_field=>json)).unwrap();
        index_writer.commit().unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        assert_eq!(searcher.num_docs(), 1);
        let parse_query = QueryParser::for_index(&index, Vec::new());
        {
            let query = parse_query
                .parse_query(r"json.k8s\.container\.name:prometheus")
                .unwrap();
            let num_docs = searcher.search(&query, &Count).unwrap();
            assert_eq!(num_docs, 1);
        }
        {
            let query = parse_query
                .parse_query(r#"json.k8s.container.name:prometheus"#)
                .unwrap();
            let num_docs = searcher.search(&query, &Count).unwrap();
            assert_eq!(num_docs, 0);
        }
    }

    #[test]
    fn test_json_field_number() {
        // this test was added specifically to reach some cases related to using json fields, with
        // frequency enabled, to store integers, with enough documents containing a single integer
        // that the posting list can be bitpacked.
        let mut schema_builder = Schema::builder();

        let json_field = schema_builder.add_json_field("json", TEXT);
        let index = Index::create_in_ram(schema_builder.build());
        let mut index_writer = index.writer_for_tests().unwrap();
        for _ in 0..256 {
            let json = serde_json::json!({"somekey": 1u64, "otherkey": -2i64});
            index_writer.add_document(doc!(json_field=>json)).unwrap();

            let json = serde_json::json!({"somekey": "1str", "otherkey": "2str"});
            index_writer.add_document(doc!(json_field=>json)).unwrap();
        }
        index_writer.commit().unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        assert_eq!(searcher.num_docs(), 512);
        let parse_query = QueryParser::for_index(&index, Vec::new());
        {
            let query = parse_query.parse_query(r"json.somekey:1").unwrap();
            let num_docs = searcher.search(&query, &Count).unwrap();
            assert_eq!(num_docs, 256);
        }
    }
    #[test]
    fn test_json_field_null_byte_is_ignored() {
        let mut schema_builder = Schema::builder();
        let options = JsonObjectOptions::from(TEXT | FAST).set_expand_dots_enabled();
        let field = schema_builder.add_json_field("json", options);
        let index = Index::create_in_ram(schema_builder.build());
        let mut index_writer = index.writer_for_tests().unwrap();
        index_writer
            .add_document(doc!(field=>json!({"key": "test1", "invalidkey\u{0000}": "test2"})))
            .unwrap();
        index_writer.commit().unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let segment_reader = searcher.segment_reader(0);
        let inv_indexer = segment_reader.inverted_index(field).unwrap();
        let term_dict = inv_indexer.terms();
        assert_eq!(term_dict.num_terms(), 1);
        let mut term_bytes = Vec::new();
        term_dict.ord_to_term(0, &mut term_bytes).unwrap();
        assert_eq!(term_bytes, b"key\0stest1");
    }

    #[test]
    fn test_json_field_1byte() {
        // Test when field name contains a '1' byte, which has special meaning in tantivy.
        // The 1 byte can be addressed as '1' byte or '.'.
        let field_name_in = "\u{0001}";
        let field_name_out = "\u{0001}";
        test_json_field_name(field_name_in, field_name_out);

        // Test when field name contains a '1' byte, which has special meaning in tantivy.
        let field_name_in = "\u{0001}";
        let field_name_out = ".";
        test_json_field_name(field_name_in, field_name_out);
    }

    #[test]
    fn test_json_field_dot() {
        // Test when field name contains a '.'
        let field_name_in = ".";
        let field_name_out = ".";
        test_json_field_name(field_name_in, field_name_out);
    }
    fn test_json_field_name(field_name_in: &str, field_name_out: &str) {
        let mut schema_builder = Schema::builder();

        let options = JsonObjectOptions::from(TEXT | FAST).set_expand_dots_enabled();
        let field = schema_builder.add_json_field("json", options);
        let index = Index::create_in_ram(schema_builder.build());
        let mut index_writer = index.writer_for_tests().unwrap();
        index_writer
            .add_document(doc!(field=>json!({format!("{field_name_in}"): "test1", format!("num{field_name_in}"): 10})))
            .unwrap();
        index_writer
            .add_document(doc!(field=>json!({format!("a{field_name_in}"): "test2"})))
            .unwrap();
        index_writer
            .add_document(doc!(field=>json!({format!("a{field_name_in}a"): "test3"})))
            .unwrap();
        index_writer
            .add_document(
                doc!(field=>json!({format!("a{field_name_in}a{field_name_in}"): "test4"})),
            )
            .unwrap();
        index_writer
            .add_document(
                doc!(field=>json!({format!("a{field_name_in}.ab{field_name_in}"): "test5"})),
            )
            .unwrap();
        index_writer
            .add_document(
                doc!(field=>json!({format!("a{field_name_in}"): json!({format!("a{field_name_in}"): "test6"}) })),
            )
            .unwrap();
        index_writer
            .add_document(doc!(field=>json!({format!("{field_name_in}a" ): "test7"})))
            .unwrap();

        index_writer.commit().unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let parse_query = QueryParser::for_index(&index, Vec::new());
        let test_query = |query_str: &str| {
            let query = parse_query.parse_query(query_str).unwrap();
            let num_docs = searcher.search(&query, &Count).unwrap();
            assert_eq!(num_docs, 1, "{query_str}");
        };
        test_query(format!("json.{field_name_out}:test1").as_str());
        test_query(format!("json.a{field_name_out}:test2").as_str());
        test_query(format!("json.a{field_name_out}a:test3").as_str());
        test_query(format!("json.a{field_name_out}a{field_name_out}:test4").as_str());
        test_query(format!("json.a{field_name_out}.ab{field_name_out}:test5").as_str());
        test_query(format!("json.a{field_name_out}.a{field_name_out}:test6").as_str());
        test_query(format!("json.{field_name_out}a:test7").as_str());

        let test_agg = |field_name: &str, expected: &str| {
            let agg_req_str = json!(
            {
              "termagg": {
                "terms": {
                  "field": field_name,
                }
              }
            });

            let agg_req: Aggregations = serde_json::from_value(agg_req_str).unwrap();
            let collector = AggregationCollector::from_aggs(agg_req, Default::default());
            let agg_res: AggregationResults = searcher.search(&AllQuery, &collector).unwrap();
            let res = serde_json::to_value(agg_res).unwrap();
            assert_eq!(res["termagg"]["buckets"][0]["doc_count"], 1);
            assert_eq!(res["termagg"]["buckets"][0]["key"], expected);
        };

        test_agg(format!("json.{field_name_out}").as_str(), "test1");
        test_agg(format!("json.a{field_name_out}").as_str(), "test2");
        test_agg(format!("json.a{field_name_out}a").as_str(), "test3");
        test_agg(
            format!("json.a{field_name_out}a{field_name_out}").as_str(),
            "test4",
        );
        test_agg(
            format!("json.a{field_name_out}.ab{field_name_out}").as_str(),
            "test5",
        );
        test_agg(
            format!("json.a{field_name_out}.a{field_name_out}").as_str(),
            "test6",
        );
        test_agg(format!("json.{field_name_out}a").as_str(), "test7");

        // `.` is stored as `\u{0001}` internally in tantivy
        let field_name_out_internal = if field_name_out == "." {
            "\u{0001}"
        } else {
            field_name_out
        };

        let mut fields: Vec<(String, Type)> = reader.searcher().segment_readers()[0]
            .inverted_index(field)
            .unwrap()
            .list_encoded_json_fields()
            .unwrap()
            .into_iter()
            .map(|field_space| (field_space.field_name, field_space.field_type))
            .collect();
        assert_eq!(fields.len(), 8);
        fields.sort();
        let mut expected_fields = vec![
            (format!("a{field_name_out_internal}"), Type::Str),
            (format!("a{field_name_out_internal}a"), Type::Str),
            (
                format!("a{field_name_out_internal}a{field_name_out_internal}"),
                Type::Str,
            ),
            (
                format!("a{field_name_out_internal}\u{1}ab{field_name_out_internal}"),
                Type::Str,
            ),
            (
                format!("a{field_name_out_internal}\u{1}a{field_name_out_internal}"),
                Type::Str,
            ),
            (format!("{field_name_out_internal}a"), Type::Str),
            (field_name_out_internal.to_string(), Type::Str),
            (format!("num{field_name_out_internal}"), Type::I64),
        ];
        expected_fields.sort();
        assert_eq!(fields, expected_fields);
        // Check columnar reader
        let mut columns = reader.searcher().segment_readers()[0]
            .fast_fields()
            .columnar()
            .list_columns()
            .unwrap()
            .into_iter()
            .map(|(name, _)| name)
            .collect::<Vec<_>>();
        let mut expected_columns = vec![
            format!("json\u{1}{field_name_out_internal}"),
            format!("json\u{1}{field_name_out_internal}a"),
            format!("json\u{1}a{field_name_out_internal}"),
            format!("json\u{1}a{field_name_out_internal}a"),
            format!("json\u{1}a{field_name_out_internal}a{field_name_out_internal}"),
            format!("json\u{1}a{field_name_out_internal}\u{1}ab{field_name_out_internal}"),
            format!("json\u{1}a{field_name_out_internal}\u{1}a{field_name_out_internal}"),
            format!("json\u{1}num{field_name_out_internal}"),
        ];
        columns.sort();
        expected_columns.sort();
        assert_eq!(columns, expected_columns);
    }

    #[test]
    fn test_json_field_expand_dots_enabled_dot_escape_not_required() {
        let mut schema_builder = Schema::builder();
        let json_options: JsonObjectOptions =
            JsonObjectOptions::from(TEXT).set_expand_dots_enabled();
        let json_field = schema_builder.add_json_field("json", json_options);
        let index = Index::create_in_ram(schema_builder.build());
        let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();
        let json = serde_json::json!({"k8s.container.name": "prometheus", "val": "hello"});
        index_writer.add_document(doc!(json_field=>json)).unwrap();
        index_writer.commit().unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        assert_eq!(searcher.num_docs(), 1);
        let parse_query = QueryParser::for_index(&index, Vec::new());
        {
            let query = parse_query
                .parse_query(r#"json.k8s.container.name:prometheus"#)
                .unwrap();
            let num_docs = searcher.search(&query, &Count).unwrap();
            assert_eq!(num_docs, 1);
        }
        {
            let query = parse_query
                .parse_query(r"json.k8s\.container\.name:prometheus")
                .unwrap();
            let num_docs = searcher.search(&query, &Count).unwrap();
            assert_eq!(num_docs, 1);
        }
    }

    #[test]
    fn test_json_field_list_fields() {
        let mut schema_builder = Schema::builder();
        let json_options: JsonObjectOptions = JsonObjectOptions::from(TEXT);
        let json_field = schema_builder.add_json_field("json", json_options);
        let index = Index::create_in_ram(schema_builder.build());
        let mut index_writer = index.writer_for_tests().unwrap();
        let json = serde_json::json!({"k8s.container.name": "prometheus", "val": "hello", "sub": {"a": 1, "b": 2}});
        index_writer.add_document(doc!(json_field=>json)).unwrap();
        let json = serde_json::json!({"k8s.container.name": "prometheus", "val": "hello", "suber": {"a": 1, "b": 2}});
        index_writer.add_document(doc!(json_field=>json)).unwrap();
        let json = serde_json::json!({"k8s.container.name": "prometheus", "val": "hello", "suber": {"a": "mixed", "b": 2}});
        index_writer.add_document(doc!(json_field=>json)).unwrap();
        index_writer.commit().unwrap();
        let reader = index.reader().unwrap();

        let searcher = reader.searcher();
        assert_eq!(searcher.num_docs(), 3);

        let reader = &searcher.segment_readers()[0];
        let inverted_index = reader.inverted_index(json_field).unwrap();
        assert_eq!(
            inverted_index
                .list_encoded_json_fields()
                .unwrap()
                .into_iter()
                .map(|field_space| (field_space.field_name, field_space.field_type))
                .collect::<Vec<_>>(),
            [
                ("k8s.container.name".to_string(), Type::Str),
                ("sub\u{1}a".to_string(), Type::I64),
                ("sub\u{1}b".to_string(), Type::I64),
                ("suber\u{1}a".to_string(), Type::I64),
                ("suber\u{1}a".to_string(), Type::Str),
                ("suber\u{1}b".to_string(), Type::I64),
                ("val".to_string(), Type::Str),
            ]
        );
    }

    #[test]
    fn test_json_fields_metadata_expanded_dots_one_segment() {
        test_json_fields_metadata(true, true);
    }

    #[test]
    fn test_json_fields_metadata_expanded_dots_multi_segment() {
        test_json_fields_metadata(true, false);
    }

    #[test]
    fn test_json_fields_metadata_no_expanded_dots_one_segment() {
        test_json_fields_metadata(false, true);
    }

    #[test]
    fn test_json_fields_metadata_no_expanded_dots_multi_segment() {
        test_json_fields_metadata(false, false);
    }

    #[track_caller]
    fn assert_size_eq(lhs: Option<ByteCount>, rhs: Option<ByteCount>) {
        let ignore_actual_values = |size_opt: Option<ByteCount>| size_opt.map(|val| val > 0);
        assert_eq!(ignore_actual_values(lhs), ignore_actual_values(rhs));
    }

    #[track_caller]
    fn assert_field_metadata_eq_but_ignore_field_size(
        expected: &FieldMetadata,
        actual: &FieldMetadata,
    ) {
        assert_eq!(&expected.field_name, &actual.field_name);
        assert_eq!(&expected.typ, &actual.typ);
        assert_eq!(&expected.stored, &actual.stored);
        assert_size_eq(expected.postings_size, actual.postings_size);
        assert_size_eq(expected.positions_size, actual.positions_size);
        assert_size_eq(expected.fast_size, actual.fast_size);
    }

    fn test_json_fields_metadata(expanded_dots: bool, one_segment: bool) {
        use pretty_assertions::assert_eq;
        let mut schema_builder = Schema::builder();
        let json_options: JsonObjectOptions =
            JsonObjectOptions::from(TEXT).set_fast(None).set_stored();
        let json_options = if expanded_dots {
            json_options.set_expand_dots_enabled()
        } else {
            json_options
        };
        schema_builder.add_json_field("json.confusing", json_options.clone());
        let json_field = schema_builder.add_json_field("json.shadow", json_options.clone());
        let json_field2 = schema_builder.add_json_field("json", json_options.clone());
        schema_builder.add_json_field("empty_json", json_options);
        let number_field = schema_builder.add_u64_field("numbers", FAST);
        schema_builder.add_u64_field("empty", FAST | INDEXED | STORED);
        let index = Index::create_in_ram(schema_builder.build());
        let mut index_writer = index.writer_for_tests().unwrap();
        let json =
            serde_json::json!({"k8s.container.name": "a", "val": "a", "sub": {"a": 1, "b": 1}});
        index_writer.add_document(doc!(json_field=>json)).unwrap();
        let json =
            serde_json::json!({"k8s.container.name": "a", "val": "a", "suber": {"a": 1, "b": 1}});
        if !one_segment {
            index_writer.commit().unwrap();
        }
        index_writer.add_document(doc!(json_field=>json)).unwrap();
        let json = serde_json::json!({"k8s.container.name": "a", "k8s.container.name": "a", "val": "a", "suber": {"a": "a", "b": 1}});
        index_writer
            .add_document(doc!(number_field => 50u64, json_field=>json, json_field2=>json!({"shadow": {"val": "a"}})))
            .unwrap();
        index_writer.commit().unwrap();
        let reader = index.reader().unwrap();

        let searcher = reader.searcher();
        assert_eq!(searcher.num_docs(), 3);

        let fields_metadata = index.fields_metadata().unwrap();

        let expected_fields = &[
            FieldMetadata {
                field_name: "empty".to_string(),
                stored: true,
                typ: Type::U64,
                term_dictionary_size: Some(0u64.into()),
                fast_size: Some(1u64.into()),
                postings_size: Some(0u64.into()),
                positions_size: Some(0u64.into()),
            },
            FieldMetadata {
                field_name: if expanded_dots {
                    "json.shadow.k8s.container.name".to_string()
                } else {
                    "json.shadow.k8s\\.container\\.name".to_string()
                },
                stored: true,
                typ: Type::Str,
                term_dictionary_size: Some(1u64.into()),
                fast_size: Some(1u64.into()),
                postings_size: Some(1u64.into()),
                positions_size: Some(1u64.into()),
            },
            FieldMetadata {
                field_name: "json.shadow.sub.a".to_string(),
                typ: Type::I64,
                stored: true,
                fast_size: Some(1u64.into()),
                term_dictionary_size: Some(1u64.into()),
                postings_size: Some(1u64.into()),
                positions_size: Some(1u64.into()),
            },
            FieldMetadata {
                field_name: "json.shadow.sub.b".to_string(),
                typ: Type::I64,
                stored: true,
                fast_size: Some(1u64.into()),
                term_dictionary_size: Some(1u64.into()),
                postings_size: Some(1u64.into()),
                positions_size: Some(1u64.into()),
            },
            FieldMetadata {
                field_name: "json.shadow.suber.a".to_string(),
                stored: true,
                typ: Type::I64,
                fast_size: Some(1u64.into()),
                term_dictionary_size: Some(1u64.into()),
                postings_size: Some(1u64.into()),
                positions_size: Some(1u64.into()),
            },
            FieldMetadata {
                field_name: "json.shadow.suber.a".to_string(),
                typ: Type::Str,
                stored: true,
                fast_size: Some(1u64.into()),
                term_dictionary_size: Some(1u64.into()),
                postings_size: Some(1u64.into()),
                positions_size: Some(1u64.into()),
            },
            FieldMetadata {
                field_name: "json.shadow.suber.b".to_string(),
                typ: Type::I64,
                stored: true,
                fast_size: Some(1u64.into()),
                term_dictionary_size: Some(1u64.into()),
                postings_size: Some(1u64.into()),
                positions_size: Some(1u64.into()),
            },
            FieldMetadata {
                field_name: "json.shadow.val".to_string(),
                typ: Type::Str,
                stored: true,
                fast_size: Some(1u64.into()),
                term_dictionary_size: Some(1u64.into()),
                postings_size: Some(1u64.into()),
                positions_size: Some(1u64.into()),
            },
            FieldMetadata {
                field_name: "numbers".to_string(),
                stored: false,
                typ: Type::U64,
                fast_size: Some(1u64.into()),
                term_dictionary_size: None,
                postings_size: None,
                positions_size: None,
            },
        ];
        assert_eq!(fields_metadata.len(), expected_fields.len());
        for (expected, value) in expected_fields.iter().zip(fields_metadata.iter()) {
            assert_field_metadata_eq_but_ignore_field_size(expected, value);
        }
        let query_parser = QueryParser::for_index(&index, vec![]);
        // Test if returned field name can be queried
        for indexed_field in fields_metadata.iter().filter(|meta| meta.is_indexed()) {
            let val = if indexed_field.typ == Type::Str {
                "a"
            } else {
                "1"
            };
            let query_str = &format!("{}:{}", indexed_field.field_name, val);
            let query = query_parser.parse_query(query_str).unwrap();
            let count_docs = searcher
                .search(&*query, &TopDocs::with_limit(2).order_by_score())
                .unwrap();
            if indexed_field.field_name.contains("empty") || indexed_field.typ == Type::Json {
                assert_eq!(count_docs.len(), 0);
            } else {
                assert!(!count_docs.is_empty(), "{}", indexed_field.field_name);
            }
        }
        // Test if returned field name can be used for aggregation
        for fast_field in fields_metadata
            .iter()
            .filter(|field_metadata| field_metadata.is_fast())
        {
            let agg_req_str = json!(
            {
              "termagg": {
                "terms": {
                  "field": fast_field.field_name,
                }
              }
            });

            let agg_req: Aggregations = serde_json::from_value(agg_req_str).unwrap();
            let collector = AggregationCollector::from_aggs(agg_req, Default::default());
            let agg_res: AggregationResults = searcher.search(&AllQuery, &collector).unwrap();
            let res = serde_json::to_value(agg_res).unwrap();
            if !fast_field.field_name.contains("empty") && fast_field.typ != Type::Json {
                assert!(
                    !res["termagg"]["buckets"].as_array().unwrap().is_empty(),
                    "{}",
                    fast_field.field_name
                );
            }
        }
    }

    #[test]
    fn test_json_field_shadowing_field_name_bug() {
        /// This test is only there to display a bug on addressing a field if it gets shadowed
        /// The issues only occurs if the field name that shadows contains a dot.
        ///
        /// Happens independently of the `expand_dots` option. Since that option does not
        /// affect the field name itself.
        use pretty_assertions::assert_eq;
        let mut schema_builder = Schema::builder();
        let json_options: JsonObjectOptions =
            JsonObjectOptions::from(TEXT).set_fast(None).set_stored();
        // let json_options = json_options.set_expand_dots_enabled();
        let json_field_shadow = schema_builder.add_json_field("json.shadow", json_options.clone());
        let json_field = schema_builder.add_json_field("json", json_options.clone());
        let index = Index::create_in_ram(schema_builder.build());
        let mut index_writer = index.writer_for_tests().unwrap();
        index_writer
            .add_document(
                doc!(json_field_shadow=>json!({"val": "b"}), json_field=>json!({"shadow": {"val": "a"}})),
            )
            .unwrap();
        index_writer.commit().unwrap();
        let reader = index.reader().unwrap();

        let searcher = reader.searcher();

        let fields_and_vals = [
            ("json.shadow\u{1}val".to_string(), "a"), // Succeeds
            //("json.shadow.val".to_string(), "a"),   // Fails
            ("json.shadow.val".to_string(), "b"),
        ];

        let query_parser = QueryParser::for_index(&index, vec![]);
        // Test if field name can be queried
        for (indexed_field, val) in fields_and_vals.iter() {
            let query_str = &format!("{indexed_field}:{val}");
            let query = query_parser.parse_query(query_str).unwrap();
            let count_docs = searcher
                .search(&*query, &TopDocs::with_limit(2).order_by_score())
                .unwrap();
            assert!(!count_docs.is_empty(), "{indexed_field}:{val}");
        }
        // Test if field name can be used for aggregation
        for (field_name, val) in fields_and_vals.iter() {
            let agg_req_str = json!(
            {
              "termagg": {
                "terms": {
                  "field": field_name,
                }
              }
            });

            let agg_req: Aggregations = serde_json::from_value(agg_req_str).unwrap();
            let collector = AggregationCollector::from_aggs(agg_req, Default::default());
            let agg_res: AggregationResults = searcher.search(&AllQuery, &collector).unwrap();
            let res = serde_json::to_value(agg_res).unwrap();
            assert_eq!(
                res["termagg"]["buckets"].as_array().unwrap()[0]["key"]
                    .as_str()
                    .unwrap(),
                *val,
                "{}",
                field_name
            );
        }
    }
}
