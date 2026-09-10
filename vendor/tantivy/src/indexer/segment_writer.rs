use columnar::MonotonicallyMappableToU64;
use common::JsonPathWriter;
use itertools::Itertools;
use tokenizer_api::BoxTokenStream;

use super::operation::AddOperation;
use crate::fastfield::FastFieldsWriter;
use crate::fieldnorm::{FieldNormReaders, FieldNormsWriter};
use crate::index::{Segment, SegmentComponent};
use crate::indexer::indexing_term::IndexingTerm;
use crate::indexer::segment_serializer::SegmentSerializer;
use crate::json_utils::{index_json_value, IndexingPositionsPerPath};
use crate::postings::{
    compute_table_memory_size, serialize_postings, IndexingContext, IndexingPosition,
    PerFieldPostingsWriter, PostingsWriter,
};
use crate::schema::document::{Document, Value};
use crate::schema::{FieldEntry, FieldType, Schema, DATE_TIME_PRECISION_INDEXED};
use crate::tokenizer::{FacetTokenizer, PreTokenizedStream, TextAnalyzer, Tokenizer};
use crate::{DocId, Opstamp, TantivyError};

/// Computes the initial size of the hash table.
///
/// Returns the recommended initial table size as a power of 2.
///
/// Note this is a very dumb way to compute log2, but it is easier to proofread that way.
fn compute_initial_table_size(per_thread_memory_budget: usize) -> crate::Result<usize> {
    let table_memory_upper_bound = per_thread_memory_budget / 3;
    (10..20) // We cap it at 2^19 = 512K capacity.
        // TODO: There are cases where this limit causes a
        // reallocation in the hashmap. Check if this affects performance.
        .map(|power| 1 << power)
        .take_while(|capacity| compute_table_memory_size(*capacity) < table_memory_upper_bound)
        .last()
        .ok_or_else(|| {
            crate::TantivyError::InvalidArgument(format!(
                "per thread memory budget (={per_thread_memory_budget}) is too small. Raise the \
                 memory budget or lower the number of threads."
            ))
        })
}

/// A `SegmentWriter` is in charge of creating segment index from a
/// set of documents.
///
/// They creates the postings list in anonymous memory.
/// The segment is laid on disk when the segment gets `finalized`.
pub struct SegmentWriter {
    pub(crate) max_doc: DocId,
    pub(crate) ctx: IndexingContext,
    pub(crate) per_field_postings_writers: PerFieldPostingsWriter,
    pub(crate) segment_serializer: SegmentSerializer,
    pub(crate) fast_field_writers: FastFieldsWriter,
    pub(crate) fieldnorms_writer: FieldNormsWriter,
    pub(crate) json_path_writer: JsonPathWriter,
    pub(crate) json_positions_per_path: IndexingPositionsPerPath,
    pub(crate) doc_opstamps: Vec<Opstamp>,
    per_field_text_analyzers: Vec<TextAnalyzer>,
    term_buffer: IndexingTerm,
    schema: Schema,
}

impl SegmentWriter {
    /// Creates a new `SegmentWriter`
    ///
    /// The arguments are defined as follows
    ///
    /// - memory_budget: most of the segment writer data (terms, and postings lists recorders) is
    ///   stored in a memory arena. This makes it possible for the user to define the flushing
    ///   behavior as a memory limit.
    /// - segment: The segment being written
    /// - schema
    pub fn for_segment(memory_budget_in_bytes: usize, segment: Segment) -> crate::Result<Self> {
        let schema = segment.schema();
        let tokenizer_manager = segment.index().tokenizers().clone();
        let tokenizer_manager_fast_field = segment.index().fast_field_tokenizer().clone();
        let table_size = compute_initial_table_size(memory_budget_in_bytes)?;
        let segment_serializer = SegmentSerializer::for_segment(segment)?;
        let per_field_postings_writers = PerFieldPostingsWriter::for_schema(&schema);
        let per_field_text_analyzers = schema
            .fields()
            .map(|(_, field_entry): (_, &FieldEntry)| {
                let text_options = match field_entry.field_type() {
                    FieldType::Str(ref text_options) => text_options.get_indexing_options(),
                    FieldType::JsonObject(ref json_object_options) => {
                        json_object_options.get_text_indexing_options()
                    }
                    _ => None,
                };
                let tokenizer_name = text_options
                    .map(|text_index_option| text_index_option.tokenizer())
                    .unwrap_or("default");

                tokenizer_manager.get(tokenizer_name).ok_or_else(|| {
                    TantivyError::SchemaError(format!(
                        "Error getting tokenizer for field: {}",
                        field_entry.name()
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            max_doc: 0,
            ctx: IndexingContext::new(table_size),
            per_field_postings_writers,
            fieldnorms_writer: FieldNormsWriter::for_schema(&schema),
            json_path_writer: JsonPathWriter::default(),
            json_positions_per_path: IndexingPositionsPerPath::default(),
            segment_serializer,
            fast_field_writers: FastFieldsWriter::from_schema_and_tokenizer_manager(
                &schema,
                tokenizer_manager_fast_field,
            )?,
            doc_opstamps: Vec::with_capacity(1_000),
            per_field_text_analyzers,
            term_buffer: IndexingTerm::with_capacity(16),
            schema,
        })
    }

    /// Lay on disk the current content of the `SegmentWriter`
    ///
    /// Finalize consumes the `SegmentWriter`, so that it cannot
    /// be used afterwards.
    pub fn finalize(mut self) -> crate::Result<Vec<u64>> {
        self.fieldnorms_writer.fill_up_to_max_doc(self.max_doc);
        remap_and_write(
            self.schema,
            &self.per_field_postings_writers,
            self.ctx,
            self.fast_field_writers,
            &self.fieldnorms_writer,
            self.segment_serializer,
        )?;
        Ok(self.doc_opstamps)
    }

    /// Returns an estimation of the current memory usage of the segment writer.
    /// If the mem usage exceeds the `memory_budget`, the segment be serialized.
    pub fn mem_usage(&self) -> usize {
        self.ctx.mem_usage()
            + self.fieldnorms_writer.mem_usage()
            + self.fast_field_writers.mem_usage()
            + self.segment_serializer.mem_usage()
    }

    fn index_document<D: Document>(&mut self, doc: &D) -> crate::Result<()> {
        let doc_id = self.max_doc;

        // TODO: Can this be optimised a bit?
        let vals_grouped_by_field = doc
            .iter_fields_and_values()
            .sorted_by_key(|(field, _)| *field)
            .chunk_by(|(field, _)| *field);

        for (field, field_values) in &vals_grouped_by_field {
            let values = field_values.map(|el| el.1);

            let field_entry = self.schema.get_field_entry(field);
            let make_schema_error = || {
                TantivyError::SchemaError(format!(
                    "Expected a {:?} for field {:?}",
                    field_entry.field_type().value_type(),
                    field_entry.name()
                ))
            };
            if !field_entry.is_indexed() {
                continue;
            }

            let (term_buffer, ctx) = (&mut self.term_buffer, &mut self.ctx);
            let postings_writer: &mut dyn PostingsWriter =
                self.per_field_postings_writers.get_for_field_mut(field);
            term_buffer.clear_with_field(field);

            match field_entry.field_type() {
                FieldType::Facet(_) => {
                    let mut facet_tokenizer = FacetTokenizer::default(); // this can be global
                    for value in values {
                        let value = value.as_value();

                        let facet_str = value.as_facet().ok_or_else(make_schema_error)?;
                        let mut facet_tokenizer = facet_tokenizer.token_stream(facet_str);
                        let mut indexing_position = IndexingPosition::default();
                        postings_writer.index_text(
                            doc_id,
                            &mut facet_tokenizer,
                            term_buffer,
                            ctx,
                            &mut indexing_position,
                        );
                    }
                }
                FieldType::Str(_) => {
                    let mut indexing_position = IndexingPosition::default();
                    for value in values {
                        let value = value.as_value();

                        let mut token_stream = if let Some(text) = value.as_str() {
                            let text_analyzer =
                                &mut self.per_field_text_analyzers[field.field_id() as usize];
                            text_analyzer.token_stream(text)
                        } else if let Some(tok_str) = value.into_pre_tokenized_text() {
                            BoxTokenStream::new(PreTokenizedStream::from(*tok_str.clone()))
                        } else {
                            continue;
                        };

                        assert!(term_buffer.is_empty());
                        postings_writer.index_text(
                            doc_id,
                            &mut *token_stream,
                            term_buffer,
                            ctx,
                            &mut indexing_position,
                        );
                    }
                    if field_entry.has_fieldnorms() {
                        self.fieldnorms_writer
                            .record(doc_id, field, indexing_position.num_tokens);
                    }
                }
                FieldType::U64(_) => {
                    let mut num_vals = 0;
                    for value in values {
                        let value = value.as_value();

                        num_vals += 1;
                        let u64_val = value.as_u64().ok_or_else(make_schema_error)?;
                        term_buffer.set_u64(u64_val);
                        postings_writer.subscribe(doc_id, 0u32, term_buffer, ctx);
                    }
                    if field_entry.has_fieldnorms() {
                        self.fieldnorms_writer.record(doc_id, field, num_vals);
                    }
                }
                FieldType::Date(_) => {
                    let mut num_vals = 0;
                    for value in values {
                        let value = value.as_value();

                        num_vals += 1;
                        let date_val = value.as_datetime().ok_or_else(make_schema_error)?;
                        term_buffer
                            .set_u64(date_val.truncate(DATE_TIME_PRECISION_INDEXED).to_u64());
                        postings_writer.subscribe(doc_id, 0u32, term_buffer, ctx);
                    }
                    if field_entry.has_fieldnorms() {
                        self.fieldnorms_writer.record(doc_id, field, num_vals);
                    }
                }
                FieldType::I64(_) => {
                    let mut num_vals = 0;
                    for value in values {
                        let value = value.as_value();

                        num_vals += 1;
                        let i64_val = value.as_i64().ok_or_else(make_schema_error)?;
                        term_buffer.set_i64(i64_val);
                        postings_writer.subscribe(doc_id, 0u32, term_buffer, ctx);
                    }
                    if field_entry.has_fieldnorms() {
                        self.fieldnorms_writer.record(doc_id, field, num_vals);
                    }
                }
                FieldType::F64(_) => {
                    let mut num_vals = 0;
                    for value in values {
                        let value = value.as_value();
                        num_vals += 1;
                        let f64_val = value.as_f64().ok_or_else(make_schema_error)?;
                        term_buffer.set_f64(f64_val);
                        postings_writer.subscribe(doc_id, 0u32, term_buffer, ctx);
                    }
                    if field_entry.has_fieldnorms() {
                        self.fieldnorms_writer.record(doc_id, field, num_vals);
                    }
                }
                FieldType::Bool(_) => {
                    let mut num_vals = 0;
                    for value in values {
                        let value = value.as_value();
                        num_vals += 1;
                        let bool_val = value.as_bool().ok_or_else(make_schema_error)?;
                        term_buffer.set_bool(bool_val);
                        postings_writer.subscribe(doc_id, 0u32, term_buffer, ctx);
                    }
                    if field_entry.has_fieldnorms() {
                        self.fieldnorms_writer.record(doc_id, field, num_vals);
                    }
                }
                FieldType::Bytes(_) => {
                    let mut num_vals = 0;
                    for value in values {
                        let value = value.as_value();
                        num_vals += 1;
                        let bytes = value.as_bytes().ok_or_else(make_schema_error)?;
                        term_buffer.set_bytes(bytes);
                        postings_writer.subscribe(doc_id, 0u32, term_buffer, ctx);
                    }
                    if field_entry.has_fieldnorms() {
                        self.fieldnorms_writer.record(doc_id, field, num_vals);
                    }
                }
                FieldType::JsonObject(json_options) => {
                    let text_analyzer =
                        &mut self.per_field_text_analyzers[field.field_id() as usize];

                    self.json_positions_per_path.clear();
                    self.json_path_writer
                        .set_expand_dots(json_options.is_expand_dots_enabled());
                    for json_value in values {
                        self.json_path_writer.clear();

                        index_json_value(
                            doc_id,
                            json_value,
                            text_analyzer,
                            term_buffer,
                            &mut self.json_path_writer,
                            postings_writer,
                            ctx,
                            &mut self.json_positions_per_path,
                        );
                    }
                }
                FieldType::IpAddr(_) => {
                    let mut num_vals = 0;
                    for value in values {
                        let value = value.as_value();

                        num_vals += 1;
                        let ip_addr = value.as_ip_addr().ok_or_else(make_schema_error)?;
                        term_buffer.set_ip_addr(ip_addr);
                        postings_writer.subscribe(doc_id, 0u32, term_buffer, ctx);
                    }
                    if field_entry.has_fieldnorms() {
                        self.fieldnorms_writer.record(doc_id, field, num_vals);
                    }
                }
            }
        }
        Ok(())
    }

    /// Indexes a new document
    ///
    /// As a user, you should rather use `IndexWriter`'s add_document.
    pub fn add_document<D: Document>(
        &mut self,
        add_operation: AddOperation<D>,
    ) -> crate::Result<()> {
        let AddOperation { document, opstamp } = add_operation;
        self.doc_opstamps.push(opstamp);
        self.fast_field_writers.add_document(&document)?;
        self.index_document(&document)?;
        let doc_writer = self.segment_serializer.get_store_writer();
        doc_writer.store(&document, &self.schema)?;
        self.max_doc += 1;
        Ok(())
    }

    /// Max doc is
    /// - the number of documents in the segment assuming there is no deletes
    /// - the maximum document id (including deleted documents) + 1
    ///
    /// Currently, **tantivy** does not handle deletes anyway,
    /// so `max_doc == num_docs`
    pub fn max_doc(&self) -> u32 {
        self.max_doc
    }

    /// Number of documents in the index.
    /// Deleted documents are not counted.
    ///
    /// Currently, **tantivy** does not handle deletes anyway,
    /// so `max_doc == num_docs`
    #[allow(dead_code)]
    pub fn num_docs(&self) -> u32 {
        self.max_doc
    }
}

/// This method is used as a trick to workaround the borrow checker
/// Writes a view of a segment by pushing information
/// to the `SegmentSerializer`.
///
/// `doc_id_map` is used to map to the new doc_id order.
fn remap_and_write(
    schema: Schema,
    per_field_postings_writers: &PerFieldPostingsWriter,
    ctx: IndexingContext,
    fast_field_writers: FastFieldsWriter,
    fieldnorms_writer: &FieldNormsWriter,
    mut serializer: SegmentSerializer,
) -> crate::Result<()> {
    debug!("remap-and-write");
    if let Some(fieldnorms_serializer) = serializer.extract_fieldnorms_serializer() {
        fieldnorms_writer.serialize(fieldnorms_serializer)?;
    }
    let fieldnorm_data = serializer
        .segment()
        .open_read(SegmentComponent::FieldNorms)?;
    let fieldnorm_readers = FieldNormReaders::open(fieldnorm_data)?;
    serialize_postings(
        ctx,
        schema,
        per_field_postings_writers,
        fieldnorm_readers,
        serializer.get_postings_serializer(),
    )?;
    debug!("fastfield-serialize");
    fast_field_writers.serialize(serializer.get_fast_field_write())?;

    debug!("serializer-close");
    serializer.close()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::Path;

    use columnar::ColumnType;

    use crate::collector::{Count, TopDocs};
    use crate::directory::RamDirectory;
    use crate::fastfield::FastValue;
    use crate::postings::{Postings, TermInfo};
    use crate::query::{PhraseQuery, QueryParser};
    use crate::schema::{
        Document, IndexRecordOption, OwnedValue, Schema, TextFieldIndexing, TextOptions, Value,
        DATE_TIME_PRECISION_INDEXED, FAST, STORED, STRING, TEXT,
    };
    use crate::store::{Compressor, StoreReader, StoreWriter};
    use crate::time::format_description::well_known::Rfc3339;
    use crate::time::OffsetDateTime;
    use crate::tokenizer::{PreTokenizedString, Token};
    use crate::{
        DateTime, Directory, DocAddress, DocSet, Index, IndexWriter, SegmentReader,
        TantivyDocument, Term, TERMINATED,
    };

    #[test]
    #[cfg(not(feature = "compare_hash_only"))]
    fn test_hashmap_size() {
        use super::compute_initial_table_size;
        assert_eq!(compute_initial_table_size(100_000).unwrap(), 1 << 12);
        assert_eq!(compute_initial_table_size(1_000_000).unwrap(), 1 << 15);
        assert_eq!(compute_initial_table_size(15_000_000).unwrap(), 1 << 19);
        assert_eq!(compute_initial_table_size(1_000_000_000).unwrap(), 1 << 19);
        assert_eq!(compute_initial_table_size(4_000_000_000).unwrap(), 1 << 19);
    }

    #[test]
    fn test_prepare_for_store() {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("title", TEXT | STORED);
        let schema = schema_builder.build();
        let mut doc = TantivyDocument::default();
        let pre_tokenized_text = PreTokenizedString {
            text: String::from("A"),
            tokens: vec![Token {
                offset_from: 0,
                offset_to: 1,
                position: 0,
                text: String::from("A"),
                position_length: 1,
            }],
        };

        doc.add_pre_tokenized_text(text_field, pre_tokenized_text);
        doc.add_text(text_field, "title");

        let path = Path::new("store");
        let directory = RamDirectory::create();
        let store_wrt = directory.open_write(path).unwrap();

        let mut store_writer = StoreWriter::new(store_wrt, Compressor::None, 0, false).unwrap();
        store_writer.store(&doc, &schema).unwrap();
        store_writer.close().unwrap();

        let reader = StoreReader::open(directory.open_read(path).unwrap(), 0).unwrap();
        let doc = reader.get::<TantivyDocument>(0).unwrap();

        assert_eq!(doc.field_values().count(), 2);
        assert_eq!(
            doc.get_all(text_field).next().unwrap().as_value().as_str(),
            Some("A")
        );
        assert_eq!(
            doc.get_all(text_field).nth(1).unwrap().as_value().as_str(),
            Some("title")
        );
    }
    #[test]
    fn test_simple_json_indexing() {
        let mut schema_builder = Schema::builder();
        let json_field = schema_builder.add_json_field("json", STORED | STRING);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema.clone());
        let mut writer = index.writer_for_tests().unwrap();
        writer
            .add_document(doc!(json_field=>json!({"my_field": "b"})))
            .unwrap();
        writer
            .add_document(doc!(json_field=>json!({"my_field": "a"})))
            .unwrap();
        writer
            .add_document(doc!(json_field=>json!({"my_field": "b"})))
            .unwrap();
        writer.commit().unwrap();

        let query_parser = QueryParser::for_index(&index, vec![json_field]);
        let text_query = query_parser.parse_query("my_field:a").unwrap();
        let score_docs: Vec<(_, DocAddress)> = index
            .reader()
            .unwrap()
            .searcher()
            .search(&text_query, &TopDocs::with_limit(4).order_by_score())
            .unwrap();
        assert_eq!(score_docs.len(), 1);

        let text_query = query_parser.parse_query("my_field:b").unwrap();
        let score_docs: Vec<(_, DocAddress)> = index
            .reader()
            .unwrap()
            .searcher()
            .search(&text_query, &TopDocs::with_limit(4).order_by_score())
            .unwrap();
        assert_eq!(score_docs.len(), 2);
    }

    #[test]
    fn test_flat_json_indexing() {
        // A JSON Object that contains mixed values on the first level
        let mut schema_builder = Schema::builder();
        let json_field = schema_builder.add_json_field("json", STORED | STRING);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema.clone());
        let mut writer = index.writer_for_tests().unwrap();
        // Text, i64, u64
        writer.add_document(doc!(json_field=>"b")).unwrap();
        writer
            .add_document(doc!(json_field=>OwnedValue::I64(10i64)))
            .unwrap();
        writer
            .add_document(doc!(json_field=>OwnedValue::U64(55u64)))
            .unwrap();
        writer
            .add_document(doc!(json_field=>json!({"my_field": "a"})))
            .unwrap();
        writer.commit().unwrap();

        let search_and_expect = |query| {
            let query_parser = QueryParser::for_index(&index, vec![json_field]);
            let text_query = query_parser.parse_query(query).unwrap();
            let score_docs: Vec<(_, DocAddress)> = index
                .reader()
                .unwrap()
                .searcher()
                .search(&text_query, &TopDocs::with_limit(4).order_by_score())
                .unwrap();
            assert_eq!(score_docs.len(), 1);
        };

        search_and_expect("my_field:a");
        search_and_expect("b");
        search_and_expect("10");
        search_and_expect("55");
    }

    #[test]
    fn test_json_indexing() {
        let mut schema_builder = Schema::builder();
        let json_field = schema_builder.add_json_field("json", STORED | TEXT);
        let schema = schema_builder.build();
        let json_val: serde_json::Value = serde_json::from_str(
            r#"{
            "toto": "titi",
            "float": -0.2,
            "bool": true,
            "unsigned": 1,
            "signed": -2,
            "complexobject": {
                "field.with.dot": 1
            },
            "date": "1985-04-12T23:20:50.52Z",
            "my_arr": [2, 3, {"my_key": "two tokens"}, 4]
        }"#,
        )
        .unwrap();
        let doc = doc!(json_field=>json_val.clone());
        let index = Index::create_in_ram(schema.clone());
        let mut writer = index.writer_for_tests().unwrap();
        writer.add_document(doc).unwrap();
        writer.commit().unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let doc = searcher
            .doc::<TantivyDocument>(DocAddress {
                segment_ord: 0u32,
                doc_id: 0u32,
            })
            .unwrap();
        let serdeser_json_val = serde_json::from_str::<serde_json::Value>(&doc.to_json(&schema))
            .unwrap()
            .get("json")
            .unwrap()[0]
            .clone();
        assert_eq!(json_val, serdeser_json_val);
        let segment_reader = searcher.segment_reader(0u32);
        let inv_idx = segment_reader.inverted_index(json_field).unwrap();
        let term_dict = inv_idx.terms();

        let mut term_stream = term_dict.stream().unwrap();

        let term_from_path =
            |path: &str| -> Term { Term::from_field_json_path(json_field, path, false) };

        fn set_fast_val<T: FastValue>(val: T, mut term: Term) -> Term {
            term.append_type_and_fast_value(val);
            term
        }
        fn set_str(val: &str, mut term: Term) -> Term {
            term.append_type_and_str(val);
            term
        }

        let term = term_from_path("bool");
        assert!(term_stream.advance());
        assert_eq!(
            term_stream.key(),
            set_fast_val(true, term).serialized_value_bytes()
        );

        let term = term_from_path("complexobject.field\\.with\\.dot");
        assert!(term_stream.advance());
        assert_eq!(
            term_stream.key(),
            set_fast_val(1i64, term).serialized_value_bytes()
        );

        // Date
        let term = term_from_path("date");

        assert!(term_stream.advance());
        assert_eq!(
            term_stream.key(),
            set_fast_val(
                DateTime::from_utc(
                    OffsetDateTime::parse("1985-04-12T23:20:50.52Z", &Rfc3339).unwrap(),
                )
                .truncate(DATE_TIME_PRECISION_INDEXED),
                term
            )
            .serialized_value_bytes()
        );

        // Float
        let term = term_from_path("float");
        assert!(term_stream.advance());
        assert_eq!(
            term_stream.key(),
            set_fast_val(-0.2f64, term).serialized_value_bytes()
        );

        // Number In Array
        let term = term_from_path("my_arr");
        assert!(term_stream.advance());
        assert_eq!(
            term_stream.key(),
            set_fast_val(2i64, term).serialized_value_bytes()
        );

        let term = term_from_path("my_arr");
        assert!(term_stream.advance());
        assert_eq!(
            term_stream.key(),
            set_fast_val(3i64, term).serialized_value_bytes()
        );

        let term = term_from_path("my_arr");
        assert!(term_stream.advance());
        assert_eq!(
            term_stream.key(),
            set_fast_val(4i64, term).serialized_value_bytes()
        );

        // El in Array
        let term = term_from_path("my_arr.my_key");
        assert!(term_stream.advance());
        assert_eq!(
            term_stream.key(),
            set_str("tokens", term).serialized_value_bytes()
        );
        let term = term_from_path("my_arr.my_key");
        assert!(term_stream.advance());
        assert_eq!(
            term_stream.key(),
            set_str("two", term).serialized_value_bytes()
        );

        // Signed
        let term = term_from_path("signed");
        assert!(term_stream.advance());
        assert_eq!(
            term_stream.key(),
            set_fast_val(-2i64, term).serialized_value_bytes()
        );

        let term = term_from_path("toto");
        assert!(term_stream.advance());
        assert_eq!(
            term_stream.key(),
            set_str("titi", term).serialized_value_bytes()
        );
        // Unsigned
        let term = term_from_path("unsigned");
        assert!(term_stream.advance());
        assert_eq!(
            term_stream.key(),
            set_fast_val(1i64, term).serialized_value_bytes()
        );

        assert!(!term_stream.advance());
    }

    #[test]
    fn test_json_tokenized_with_position() {
        let mut schema_builder = Schema::builder();
        let json_field = schema_builder.add_json_field("json", STORED | TEXT);
        let schema = schema_builder.build();
        let mut doc = TantivyDocument::default();
        let json_val: BTreeMap<String, crate::schema::OwnedValue> =
            serde_json::from_str(r#"{"mykey": "repeated token token"}"#).unwrap();
        doc.add_object(json_field, json_val);
        let index = Index::create_in_ram(schema);
        let mut writer = index.writer_for_tests().unwrap();
        writer.add_document(doc).unwrap();
        writer.commit().unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let segment_reader = searcher.segment_reader(0u32);
        let inv_index = segment_reader.inverted_index(json_field).unwrap();
        let mut term = Term::from_field_json_path(json_field, "mykey", false);
        term.append_type_and_str("token");
        let term_info = inv_index.get_term_info(&term).unwrap().unwrap();
        assert_eq!(
            term_info,
            TermInfo {
                doc_freq: 1,
                postings_range: 2..4,
                positions_range: 2..5
            }
        );
        let mut postings = inv_index
            .read_postings(&term, IndexRecordOption::WithFreqsAndPositions)
            .unwrap()
            .unwrap();
        assert_eq!(postings.doc(), 0);
        assert_eq!(postings.term_freq(), 2);
        let mut positions = Vec::new();
        postings.positions(&mut positions);
        assert_eq!(&positions[..], &[1, 2]);
        assert_eq!(postings.advance(), TERMINATED);
    }

    #[test]
    fn test_json_raw_no_position() {
        let mut schema_builder = Schema::builder();
        let json_field = schema_builder.add_json_field("json", STRING);
        let schema = schema_builder.build();
        let json_val: serde_json::Value =
            serde_json::from_str(r#"{"mykey": "two tokens"}"#).unwrap();
        let doc = doc!(json_field=>json_val);
        let index = Index::create_in_ram(schema);
        let mut writer = index.writer_for_tests().unwrap();
        writer.add_document(doc).unwrap();
        writer.commit().unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let segment_reader = searcher.segment_reader(0u32);
        let inv_index = segment_reader.inverted_index(json_field).unwrap();
        let mut term = Term::from_field_json_path(json_field, "mykey", false);
        term.append_type_and_str("two tokens");
        let term_info = inv_index.get_term_info(&term).unwrap().unwrap();
        assert_eq!(
            term_info,
            TermInfo {
                doc_freq: 1,
                postings_range: 0..1,
                positions_range: 0..0
            }
        );
        let mut postings = inv_index
            .read_postings(&term, IndexRecordOption::WithFreqs)
            .unwrap()
            .unwrap();
        assert_eq!(postings.doc(), 0);
        assert_eq!(postings.term_freq(), 1);
        let mut positions = Vec::new();
        postings.positions(&mut positions);
        assert_eq!(postings.advance(), TERMINATED);
    }

    #[test]
    fn test_position_overlapping_path() {
        // This test checks that we do not end up detecting phrase query due
        // to several string literal in the same json object being overlapping.
        let mut schema_builder = Schema::builder();
        let json_field = schema_builder.add_json_field("json", TEXT);
        let schema = schema_builder.build();
        let json_val: serde_json::Value = serde_json::from_str(
            r#"{"mykey": [{"field": "hello happy tax payer"}, {"field": "nothello"}]}"#,
        )
        .unwrap();
        let doc = doc!(json_field=>json_val);
        let index = Index::create_in_ram(schema);
        let mut writer = index.writer_for_tests().unwrap();
        writer.add_document(doc).unwrap();
        writer.commit().unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();

        let term = Term::from_field_json_path(json_field, "mykey.field", false);

        let mut hello_term = term.clone();
        hello_term.append_type_and_str("hello");

        let mut nothello_term = term.clone();
        nothello_term.append_type_and_str("nothello");

        let mut happy_term = term.clone();
        happy_term.append_type_and_str("happy");

        let phrase_query = PhraseQuery::new(vec![hello_term, happy_term.clone()]);
        assert_eq!(searcher.search(&phrase_query, &Count).unwrap(), 1);
        let phrase_query = PhraseQuery::new(vec![nothello_term, happy_term]);
        assert_eq!(searcher.search(&phrase_query, &Count).unwrap(), 0);
    }

    #[test]
    fn test_json_fast() {
        let mut schema_builder = Schema::builder();
        let json_field = schema_builder.add_json_field("json", FAST);
        let schema = schema_builder.build();
        let json_val: serde_json::Value = serde_json::from_str(
            r#"{
            "toto": "titi",
            "float": -0.2,
            "bool": true,
            "unsigned": 1,
            "signed": -2,
            "complexobject": {
                "field.with.dot": 1
            },
            "date": "1985-04-12T23:20:50.52Z",
            "my_arr": [2, 3, {"my_key": "two tokens"}, 4]
        }"#,
        )
        .unwrap();
        let doc = doc!(json_field=>json_val.clone());
        let index = Index::create_in_ram(schema.clone());
        let mut writer = index.writer_for_tests().unwrap();
        writer.add_document(doc).unwrap();
        writer.commit().unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let segment_reader = searcher.segment_reader(0u32);

        fn assert_type(reader: &SegmentReader, field: &str, typ: ColumnType) {
            let cols = reader.fast_fields().dynamic_column_handles(field).unwrap();
            assert_eq!(cols.len(), 1, "{field}");
            assert_eq!(cols[0].column_type(), typ, "{field}");
        }
        assert_type(segment_reader, "json.toto", ColumnType::Str);
        assert_type(segment_reader, "json.float", ColumnType::F64);
        assert_type(segment_reader, "json.bool", ColumnType::Bool);
        assert_type(segment_reader, "json.unsigned", ColumnType::I64);
        assert_type(segment_reader, "json.signed", ColumnType::I64);
        assert_type(
            segment_reader,
            "json.complexobject.field\\.with\\.dot",
            ColumnType::I64,
        );
        assert_type(segment_reader, "json.date", ColumnType::DateTime);
        assert_type(segment_reader, "json.my_arr", ColumnType::I64);
        assert_type(segment_reader, "json.my_arr.my_key", ColumnType::Str);

        fn assert_empty(reader: &SegmentReader, field: &str) {
            let cols = reader.fast_fields().dynamic_column_handles(field).unwrap();
            assert_eq!(cols.len(), 0);
        }
        assert_empty(segment_reader, "unknown");
        assert_empty(segment_reader, "json");
        assert_empty(segment_reader, "json.toto.titi");

        let sub_columns = segment_reader
            .fast_fields()
            .dynamic_subpath_column_handles("json")
            .unwrap();
        assert_eq!(sub_columns.len(), 9);

        let subsub_columns = segment_reader
            .fast_fields()
            .dynamic_subpath_column_handles("json.complexobject")
            .unwrap();
        assert_eq!(subsub_columns.len(), 1);
    }

    #[test]
    fn test_json_term_with_numeric_merge_panic_regression_bug_2283() {
        // https://github.com/quickwit-oss/tantivy/issues/2283
        let mut schema_builder = Schema::builder();
        let json = schema_builder.add_json_field("json", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut writer = index.writer_for_tests().unwrap();
        let doc = json!({"field": "a"});
        writer.add_document(doc!(json=>doc)).unwrap();
        writer.commit().unwrap();
        let doc = json!({"field": "a", "id": 1});
        writer.add_document(doc!(json=>doc.clone())).unwrap();
        writer.commit().unwrap();

        // Force Merge
        writer.wait_merging_threads().unwrap();
        let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();
        let segment_ids = index
            .searchable_segment_ids()
            .expect("Searchable segments failed.");
        index_writer.merge(&segment_ids).wait().unwrap();
        assert!(index_writer.wait_merging_threads().is_ok());
    }

    #[test]
    fn test_bug_regression_1629_position_when_array_with_a_field_value_that_does_not_contain_any_token(
    ) {
        // We experienced a bug where we would have a position underflow when computing position
        // delta in an horrible corner case.
        //
        // See the commit with this unit test if you want the details.
        let mut schema_builder = Schema::builder();
        let text = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let doc = TantivyDocument::parse_json(&schema, r#"{"text": [ "bbb", "aaa", "", "aaa"]}"#)
            .unwrap();
        let index = Index::create_in_ram(schema);
        let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();
        index_writer.add_document(doc).unwrap();
        // On debug this did panic on the underflow
        index_writer.commit().unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let seg_reader = searcher.segment_reader(0);
        let inv_index = seg_reader.inverted_index(text).unwrap();
        let term = Term::from_field_text(text, "aaa");
        let mut postings = inv_index
            .read_postings(&term, IndexRecordOption::WithFreqsAndPositions)
            .unwrap()
            .unwrap();
        assert_eq!(postings.doc(), 0u32);
        let mut positions = Vec::new();
        postings.positions(&mut positions);
        // On release this was [2, 1]. (< note the decreasing values)
        assert_eq!(positions, &[2, 5]);
    }

    #[test]
    fn test_multiple_field_value_and_long_tokens() {
        let mut schema_builder = Schema::builder();
        let text = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let mut doc = TantivyDocument::default();
        // This is a bit of a contrived example.
        let tokens = PreTokenizedString {
            text: "roller-coaster".to_string(),
            tokens: vec![Token {
                offset_from: 0,
                offset_to: 14,
                position: 0,
                text: "rollercoaster".to_string(),
                position_length: 2,
            }],
        };
        doc.add_pre_tokenized_text(text, tokens.clone());
        doc.add_pre_tokenized_text(text, tokens);
        let index = Index::create_in_ram(schema);
        let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();
        index_writer.add_document(doc).unwrap();
        index_writer.commit().unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let seg_reader = searcher.segment_reader(0);
        let inv_index = seg_reader.inverted_index(text).unwrap();
        let term = Term::from_field_text(text, "rollercoaster");
        let mut postings = inv_index
            .read_postings(&term, IndexRecordOption::WithFreqsAndPositions)
            .unwrap()
            .unwrap();
        assert_eq!(postings.doc(), 0u32);
        let mut positions = Vec::new();
        postings.positions(&mut positions);
        assert_eq!(positions, &[0, 3]); //< as opposed to 0, 2 if we had a position length of 1.
    }

    #[test]
    fn test_last_token_not_ending_last() {
        let mut schema_builder = Schema::builder();
        let text = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let mut doc = TantivyDocument::default();
        // This is a bit of a contrived example.
        let tokens = PreTokenizedString {
            text: "contrived-example".to_string(), //< I can't think of a use case where this corner case happens in real life.
            tokens: vec![
                Token {
                    // Not the last token, yet ends after the last token.
                    offset_from: 0,
                    offset_to: 14,
                    position: 0,
                    text: "long_token".to_string(),
                    position_length: 3,
                },
                Token {
                    offset_from: 0,
                    offset_to: 14,
                    position: 1,
                    text: "short".to_string(),
                    position_length: 1,
                },
            ],
        };
        doc.add_pre_tokenized_text(text, tokens);
        doc.add_text(text, "hello");
        let index = Index::create_in_ram(schema);
        let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();
        index_writer.add_document(doc).unwrap();
        index_writer.commit().unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let seg_reader = searcher.segment_reader(0);
        let inv_index = seg_reader.inverted_index(text).unwrap();
        let term = Term::from_field_text(text, "hello");
        let mut postings = inv_index
            .read_postings(&term, IndexRecordOption::WithFreqsAndPositions)
            .unwrap()
            .unwrap();
        assert_eq!(postings.doc(), 0u32);
        let mut positions = Vec::new();
        postings.positions(&mut positions);
        assert_eq!(positions, &[4]); //< as opposed to 3 if we had a position length of 1.
    }

    #[test]
    fn test_show_error_when_tokenizer_not_registered() {
        let text_field_indexing = TextFieldIndexing::default()
            .set_tokenizer("custom_en")
            .set_index_option(IndexRecordOption::WithFreqsAndPositions);
        let text_options = TextOptions::default()
            .set_indexing_options(text_field_indexing)
            .set_stored();
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("title", text_options);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let schema = index.schema();
        let mut index_writer = index.writer(50_000_000).unwrap();
        let title = schema.get_field("title").unwrap();
        let mut document = TantivyDocument::default();
        document.add_text(title, "The Old Man and the Sea");
        index_writer.add_document(document).unwrap();
        let error = index_writer.commit().unwrap_err();
        assert_eq!(
            error.to_string(),
            "Schema error: 'Error getting tokenizer for field: title'"
        );
    }
}
