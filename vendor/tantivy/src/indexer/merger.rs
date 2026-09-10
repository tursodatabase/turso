use std::sync::Arc;

use columnar::{
    ColumnType, ColumnarReader, MergeRowOrder, RowAddr, ShuffleMergeOrder, StackMergeOrder,
};
use common::ReadOnlyBitSet;
use itertools::Itertools;
use measure_time::debug_time;

use crate::directory::WritePtr;
use crate::docset::{DocSet, TERMINATED};
use crate::error::DataCorruption;
use crate::fastfield::AliveBitSet;
use crate::fieldnorm::{FieldNormReader, FieldNormReaders, FieldNormsSerializer, FieldNormsWriter};
use crate::index::{Segment, SegmentComponent, SegmentReader};
use crate::indexer::doc_id_mapping::{MappingType, SegmentDocIdMapping};
use crate::indexer::SegmentSerializer;
use crate::postings::{InvertedIndexSerializer, Postings, SegmentPostings};
use crate::schema::{value_type_to_column_type, Field, FieldType, Schema};
use crate::store::StoreWriter;
use crate::termdict::{TermMerger, TermOrdinal};
use crate::{DocAddress, DocId, InvertedIndexReader};

/// Segment's max doc must be `< MAX_DOC_LIMIT`.
///
/// We do not allow segments with more than
pub const MAX_DOC_LIMIT: u32 = 1 << 31;

fn estimate_total_num_tokens_in_single_segment(
    reader: &SegmentReader,
    field: Field,
) -> crate::Result<u64> {
    // There are no deletes. We can simply use the exact value saved into the posting list.
    // Note that this value is not necessarily exact as it could have been the result of a merge
    // between segments themselves containing deletes.
    if !reader.has_deletes() {
        return Ok(reader.inverted_index(field)?.total_num_tokens());
    }

    // When there are deletes, we use an approximation either
    // by using the fieldnorm.
    if let Some(fieldnorm_reader) = reader.fieldnorms_readers().get_field(field)? {
        let mut count: [usize; 256] = [0; 256];
        for doc in reader.doc_ids_alive() {
            let fieldnorm_id = fieldnorm_reader.fieldnorm_id(doc);
            count[fieldnorm_id as usize] += 1;
        }
        let total_num_tokens = count
            .iter()
            .cloned()
            .enumerate()
            .map(|(fieldnorm_ord, count)| {
                count as u64 * u64::from(FieldNormReader::id_to_fieldnorm(fieldnorm_ord as u8))
            })
            .sum::<u64>();
        return Ok(total_num_tokens);
    }

    // There are no fieldnorms available.
    // Here we just do a pro-rata with the overall number of tokens an the ratio of
    // documents alive.
    let segment_num_tokens = reader.inverted_index(field)?.total_num_tokens();
    if reader.max_doc() == 0 {
        // That supposedly never happens, but let's be a bit defensive here.
        return Ok(0u64);
    }
    let ratio = reader.num_docs() as f64 / reader.max_doc() as f64;
    Ok((segment_num_tokens as f64 * ratio) as u64)
}

fn estimate_total_num_tokens(readers: &[SegmentReader], field: Field) -> crate::Result<u64> {
    let mut total_num_tokens: u64 = 0;
    for reader in readers {
        total_num_tokens += estimate_total_num_tokens_in_single_segment(reader, field)?;
    }
    Ok(total_num_tokens)
}

pub struct IndexMerger {
    schema: Schema,
    pub(crate) readers: Vec<SegmentReader>,
    max_doc: u32,
}

struct DeltaComputer {
    buffer: Vec<u32>,
}

impl DeltaComputer {
    fn new() -> DeltaComputer {
        DeltaComputer {
            buffer: vec![0u32; 512],
        }
    }

    fn compute_delta(&mut self, positions: &[u32]) -> &[u32] {
        if positions.len() > self.buffer.len() {
            self.buffer.resize(positions.len(), 0u32);
        }
        let mut last_pos = 0u32;
        for (cur_pos, dest) in positions.iter().cloned().zip(self.buffer.iter_mut()) {
            *dest = cur_pos - last_pos;
            last_pos = cur_pos;
        }
        &self.buffer[..positions.len()]
    }
}

fn convert_to_merge_order(
    columnars: &[&ColumnarReader],
    doc_id_mapping: SegmentDocIdMapping,
) -> MergeRowOrder {
    match doc_id_mapping.mapping_type() {
        MappingType::Stacked => MergeRowOrder::Stack(StackMergeOrder::stack(columnars)),
        MappingType::StackedWithDeletes => {
            // RUST/LLVM is amazing. The following conversion is actually a no-op:
            // no allocation, no copy.
            let new_row_id_to_old_row_id: Vec<RowAddr> = doc_id_mapping
                .new_doc_id_to_old_doc_addr
                .into_iter()
                .map(|doc_addr| RowAddr {
                    segment_ord: doc_addr.segment_ord,
                    row_id: doc_addr.doc_id,
                })
                .collect();
            MergeRowOrder::Shuffled(ShuffleMergeOrder {
                new_row_id_to_old_row_id,
                alive_bitsets: doc_id_mapping.alive_bitsets,
            })
        }
    }
}

fn extract_fast_field_required_columns(schema: &Schema) -> Vec<(String, ColumnType)> {
    schema
        .fields()
        .map(|(_, field_entry)| field_entry)
        .filter(|field_entry| field_entry.is_fast())
        .filter_map(|field_entry| {
            let column_name = field_entry.name().to_string();
            let column_type = value_type_to_column_type(field_entry.field_type().value_type())?;
            Some((column_name, column_type))
        })
        .collect()
}

impl IndexMerger {
    pub fn open(schema: Schema, segments: &[Segment]) -> crate::Result<IndexMerger> {
        let alive_bitset = segments.iter().map(|_| None).collect_vec();
        Self::open_with_custom_alive_set(schema, segments, alive_bitset)
    }

    // Create merge with a custom delete set.
    // For every Segment, a delete bitset can be provided, which
    // will be merged with the existing bit set. Make sure the index
    // corresponds to the segment index.
    //
    // If `None` is provided for custom alive set, the regular alive set will be used.
    // If a alive_bitset is provided, the union between the provided and regular
    // alive set will be used.
    //
    // This can be used to merge but also apply an additional filter.
    // One use case is demux, which is basically taking a list of
    // segments and partitions them e.g. by a value in a field.
    pub fn open_with_custom_alive_set(
        schema: Schema,
        segments: &[Segment],
        alive_bitset_opt: Vec<Option<AliveBitSet>>,
    ) -> crate::Result<IndexMerger> {
        let mut readers = vec![];
        for (segment, new_alive_bitset_opt) in segments.iter().zip(alive_bitset_opt) {
            if segment.meta().num_docs() > 0 {
                let reader =
                    SegmentReader::open_with_custom_alive_set(segment, new_alive_bitset_opt)?;
                readers.push(reader);
            }
        }

        let max_doc = readers.iter().map(|reader| reader.num_docs()).sum();
        // sort segments by their natural sort setting
        if max_doc >= MAX_DOC_LIMIT {
            let err_msg = format!(
                "The segment resulting from this merge would have {max_doc} docs,which exceeds \
                 the limit {MAX_DOC_LIMIT}."
            );
            return Err(crate::TantivyError::InvalidArgument(err_msg));
        }
        Ok(IndexMerger {
            schema,
            readers,
            max_doc,
        })
    }

    fn write_fieldnorms(
        &self,
        mut fieldnorms_serializer: FieldNormsSerializer,
        doc_id_mapping: &SegmentDocIdMapping,
    ) -> crate::Result<()> {
        let fields = FieldNormsWriter::fields_with_fieldnorm(&self.schema);
        let mut fieldnorms_data = Vec::with_capacity(self.max_doc as usize);
        for field in fields {
            fieldnorms_data.clear();
            let fieldnorms_readers: Vec<FieldNormReader> = self
                .readers
                .iter()
                .map(|reader| reader.get_fieldnorms_reader(field))
                .collect::<Result<_, _>>()?;
            for old_doc_addr in doc_id_mapping.iter_old_doc_addrs() {
                let fieldnorms_reader = &fieldnorms_readers[old_doc_addr.segment_ord as usize];
                let fieldnorm_id = fieldnorms_reader.fieldnorm_id(old_doc_addr.doc_id);
                fieldnorms_data.push(fieldnorm_id);
            }
            fieldnorms_serializer.serialize_field(field, &fieldnorms_data[..])?;
        }
        fieldnorms_serializer.close()?;
        Ok(())
    }

    fn write_fast_fields(
        &self,
        fast_field_wrt: &mut WritePtr,
        doc_id_mapping: SegmentDocIdMapping,
    ) -> crate::Result<()> {
        debug_time!("write-fast-fields");
        let required_columns = extract_fast_field_required_columns(&self.schema);
        let columnars: Vec<&ColumnarReader> = self
            .readers
            .iter()
            .map(|reader| reader.fast_fields().columnar())
            .collect();
        let merge_row_order = convert_to_merge_order(&columnars[..], doc_id_mapping);
        columnar::merge_columnar(
            &columnars[..],
            &required_columns,
            merge_row_order,
            fast_field_wrt,
        )?;
        Ok(())
    }

    /// Creates a mapping if the segments are stacked. this is helpful to merge codelines between
    /// index sorting and the others
    pub(crate) fn get_doc_id_from_concatenated_data(&self) -> crate::Result<SegmentDocIdMapping> {
        let total_num_new_docs = self
            .readers
            .iter()
            .map(|reader| reader.num_docs() as usize)
            .sum();

        let mut mapping: Vec<DocAddress> = Vec::with_capacity(total_num_new_docs);

        mapping.extend(
            self.readers
                .iter()
                .enumerate()
                .flat_map(|(segment_ord, reader)| {
                    reader.doc_ids_alive().map(move |doc_id| DocAddress {
                        segment_ord: segment_ord as u32,
                        doc_id,
                    })
                }),
        );

        let has_deletes: bool = self.readers.iter().any(SegmentReader::has_deletes);
        let mapping_type = if has_deletes {
            MappingType::StackedWithDeletes
        } else {
            MappingType::Stacked
        };
        let alive_bitsets: Vec<Option<ReadOnlyBitSet>> = self
            .readers
            .iter()
            .map(|reader| {
                let alive_bitset = reader.alive_bitset()?;
                Some(alive_bitset.bitset().clone())
            })
            .collect();
        Ok(SegmentDocIdMapping::new(
            mapping,
            mapping_type,
            alive_bitsets,
        ))
    }

    fn write_postings_for_field(
        &self,
        indexed_field: Field,
        _field_type: &FieldType,
        serializer: &mut InvertedIndexSerializer,
        fieldnorm_reader: Option<FieldNormReader>,
        doc_id_mapping: &SegmentDocIdMapping,
    ) -> crate::Result<()> {
        debug_time!("write-postings-for-field");
        let mut positions_buffer: Vec<u32> = Vec::with_capacity(1_000);
        let mut delta_computer = DeltaComputer::new();

        let mut max_term_ords: Vec<TermOrdinal> = Vec::new();

        let field_readers: Vec<Arc<InvertedIndexReader>> = self
            .readers
            .iter()
            .map(|reader| reader.inverted_index(indexed_field))
            .collect::<crate::Result<Vec<_>>>()?;

        let mut field_term_streams = Vec::new();
        for field_reader in &field_readers {
            let terms = field_reader.terms();
            field_term_streams.push(terms.stream()?);
            max_term_ords.push(terms.num_terms() as u64);
        }

        let mut merged_terms = TermMerger::new(field_term_streams);

        // map from segment doc ids to the resulting merged segment doc id.

        let mut merged_doc_id_map: Vec<Vec<Option<DocId>>> = self
            .readers
            .iter()
            .map(|reader| {
                let mut segment_local_map = vec![];
                segment_local_map.resize(reader.max_doc() as usize, None);
                segment_local_map
            })
            .collect();
        for (new_doc_id, old_doc_addr) in doc_id_mapping.iter_old_doc_addrs().enumerate() {
            let segment_map = &mut merged_doc_id_map[old_doc_addr.segment_ord as usize];
            segment_map[old_doc_addr.doc_id as usize] = Some(new_doc_id as DocId);
        }

        // Note that the total number of tokens is not exact.
        // It is only used as a parameter in the BM25 formula.
        let total_num_tokens: u64 = estimate_total_num_tokens(&self.readers, indexed_field)?;

        // Create the total list of doc ids
        // by stacking the doc ids from the different segment.
        //
        // In the new segments, the doc id from the different
        // segment are stacked so that :
        // - Segment 0's doc ids become doc id [0, seg.max_doc]
        // - Segment 1's doc ids become  [seg0.max_doc, seg0.max_doc + seg.max_doc]
        // - Segment 2's doc ids become  [seg0.max_doc + seg1.max_doc, seg0.max_doc + seg1.max_doc +
        //   seg2.max_doc]
        //
        // This stacking applies only when the index is not sorted, in that case the
        // doc_ids are kmerged by their sort property
        let mut field_serializer =
            serializer.new_field(indexed_field, total_num_tokens, fieldnorm_reader)?;

        let field_entry = self.schema.get_field_entry(indexed_field);

        // ... set segment postings option the new field.
        let segment_postings_option = field_entry.field_type().get_index_record_option().expect(
            "Encountered a field that is not supposed to be
                         indexed. Have you modified the schema?",
        );

        let mut segment_postings_containing_the_term: Vec<(usize, SegmentPostings)> = vec![];

        while merged_terms.advance() {
            segment_postings_containing_the_term.clear();
            let term_bytes: &[u8] = merged_terms.key();

            let mut total_doc_freq = 0;

            // Let's compute the list of non-empty posting lists
            for (segment_ord, term_info) in merged_terms.current_segment_ords_and_term_infos() {
                let segment_reader = &self.readers[segment_ord];
                let inverted_index: &InvertedIndexReader = &field_readers[segment_ord];
                let segment_postings = inverted_index
                    .read_postings_from_terminfo(&term_info, segment_postings_option)?;
                let alive_bitset_opt = segment_reader.alive_bitset();
                let doc_freq = if let Some(alive_bitset) = alive_bitset_opt {
                    segment_postings.doc_freq_given_deletes(alive_bitset)
                } else {
                    segment_postings.doc_freq()
                };
                if doc_freq > 0u32 {
                    total_doc_freq += doc_freq;
                    segment_postings_containing_the_term.push((segment_ord, segment_postings));
                }
            }

            // At this point, `segment_postings` contains the posting list
            // of all of the segments containing the given term (and that are non-empty)
            //
            // These segments are non-empty and advance has already been called.
            if total_doc_freq == 0u32 {
                // All docs that used to contain the term have been deleted. The `term` will be
                // entirely removed.
                continue;
            }

            // This should never happen as we early exited for total_doc_freq == 0.
            assert!(!segment_postings_containing_the_term.is_empty());

            let has_term_freq = {
                let has_term_freq = !segment_postings_containing_the_term[0]
                    .1
                    .block_cursor
                    .freqs()
                    .is_empty();
                for (_, postings) in &segment_postings_containing_the_term[1..] {
                    // This may look at a strange way to test whether we have term freq or not.
                    // With JSON object, the schema is not sufficient to know whether a term
                    // has its term frequency encoded or not:
                    // strings may have term frequencies, while number terms never have one.
                    //
                    // Ideally, we should have burnt one bit of two in the `TermInfo`.
                    // However, we preferred not changing the codec too much and detect this
                    // instead by
                    // - looking at the size of the skip data for bitpacked blocks
                    // - observing the absence of remaining data after reading the docs for vint
                    // blocks.
                    //
                    // Overall the reliable way to know if we have actual frequencies loaded or not
                    // is to check whether the actual decoded array is empty or not.
                    if has_term_freq == postings.block_cursor.freqs().is_empty() {
                        return Err(DataCorruption::comment_only(
                            "Term freqs are inconsistent across segments",
                        )
                        .into());
                    }
                }
                has_term_freq
            };

            field_serializer.new_term(term_bytes, total_doc_freq, has_term_freq)?;

            // We can now serialize this postings, by pushing each document to the
            // postings serializer.
            for (segment_ord, mut segment_postings) in
                segment_postings_containing_the_term.drain(..)
            {
                let old_to_new_doc_id = &merged_doc_id_map[segment_ord];

                let mut doc = segment_postings.doc();
                while doc != TERMINATED {
                    // deleted doc are skipped as they do not have a `remapped_doc_id`.
                    if let Some(remapped_doc_id) = old_to_new_doc_id[doc as usize] {
                        // we make sure to only write the term if
                        // there is at least one document.
                        let term_freq = if has_term_freq {
                            segment_postings.positions(&mut positions_buffer);
                            segment_postings.term_freq()
                        } else {
                            // The positions_buffer may contain positions from the previous term
                            // Existence of positions depend on the value type in JSON fields.
                            // https://github.com/quickwit-oss/tantivy/issues/2283
                            positions_buffer.clear();
                            0u32
                        };

                        let delta_positions = delta_computer.compute_delta(&positions_buffer);
                        field_serializer.write_doc(remapped_doc_id, term_freq, delta_positions);
                    }

                    doc = segment_postings.advance();
                }
            }
            // closing the term.
            field_serializer.close_term()?;
        }
        field_serializer.close()?;
        Ok(())
    }

    fn write_postings(
        &self,
        serializer: &mut InvertedIndexSerializer,
        fieldnorm_readers: FieldNormReaders,
        doc_id_mapping: &SegmentDocIdMapping,
    ) -> crate::Result<()> {
        for (field, field_entry) in self.schema.fields() {
            let fieldnorm_reader = fieldnorm_readers.get_field(field)?;
            if field_entry.is_indexed() {
                self.write_postings_for_field(
                    field,
                    field_entry.field_type(),
                    serializer,
                    fieldnorm_reader,
                    doc_id_mapping,
                )?;
            }
        }
        Ok(())
    }

    fn write_storable_fields(&self, store_writer: &mut StoreWriter) -> crate::Result<()> {
        debug_time!("write-storable-fields");
        debug!("write-storable-field");

        for reader in &self.readers {
            let store_reader = reader.get_store_reader(1)?;
            if reader.has_deletes()
                    // If there is not enough data in the store, we avoid stacking in order to
                    // avoid creating many small blocks in the doc store. Once we have 5 full blocks,
                    // we start stacking. In the worst case 2/7 of the blocks would be very small.
                    // [segment 1 - {1 doc}][segment 2 - {fullblock * 5}{1doc}]
                    // => 5 * full blocks, 2 * 1 document blocks
                    //
                    // In a more realistic scenario the segments are of the same size, so 1/6 of
                    // the doc stores would be on average half full, given total randomness (which
                    // is not the case here, but not sure how it behaves exactly).
                    //
                    // https://github.com/quickwit-oss/tantivy/issues/1053
                    //
                    // take 7 in order to not walk over all checkpoints.
                    || store_reader.block_checkpoints().take(7).count() < 6
                    || store_reader.decompressor() != store_writer.compressor().into()
            {
                for doc_bytes_res in store_reader.iter_raw(reader.alive_bitset()) {
                    let doc_bytes = doc_bytes_res?;
                    store_writer.store_bytes(&doc_bytes)?;
                }
            } else {
                store_writer.stack(store_reader)?;
            }
        }
        Ok(())
    }

    /// Writes the merged segment by pushing information
    /// to the `SegmentSerializer`.
    ///
    /// # Returns
    /// The number of documents in the resulting segment.
    pub fn write(&self, mut serializer: SegmentSerializer) -> crate::Result<u32> {
        let doc_id_mapping = self.get_doc_id_from_concatenated_data()?;
        debug!("write-fieldnorms");
        if let Some(fieldnorms_serializer) = serializer.extract_fieldnorms_serializer() {
            self.write_fieldnorms(fieldnorms_serializer, &doc_id_mapping)?;
        }
        debug!("write-postings");
        let fieldnorm_data = serializer
            .segment()
            .open_read(SegmentComponent::FieldNorms)?;
        let fieldnorm_readers = FieldNormReaders::open(fieldnorm_data)?;
        self.write_postings(
            serializer.get_postings_serializer(),
            fieldnorm_readers,
            &doc_id_mapping,
        )?;

        debug!("write-storagefields");
        self.write_storable_fields(serializer.get_store_writer())?;
        debug!("write-fastfields");
        self.write_fast_fields(serializer.get_fast_field_write(), doc_id_mapping)?;

        debug!("close-serializer");
        serializer.close()?;
        Ok(self.max_doc)
    }
}

#[cfg(test)]
mod tests {

    use columnar::Column;
    use proptest::prop_oneof;
    use proptest::strategy::Strategy;
    use schema::FAST;

    use crate::collector::tests::{
        BytesFastFieldTestCollector, FastFieldTestCollector, TEST_COLLECTOR_WITH_SCORE,
    };
    use crate::collector::{Count, FacetCollector};
    use crate::index::{Index, SegmentId};
    use crate::indexer::NoMergePolicy;
    use crate::query::{AllQuery, BooleanQuery, EnableScoring, Scorer, TermQuery};
    use crate::schema::{
        Facet, FacetOptions, IndexRecordOption, NumericOptions, TantivyDocument, Term,
        TextFieldIndexing, Value, INDEXED, TEXT,
    };
    use crate::time::OffsetDateTime;
    use crate::{
        assert_nearly_equals, schema, DateTime, DocAddress, DocId, DocSet, IndexSettings,
        IndexWriter, Searcher,
    };

    #[test]
    fn test_index_merger_no_deletes() -> crate::Result<()> {
        let mut schema_builder = schema::Schema::builder();
        let text_fieldtype = schema::TextOptions::default()
            .set_indexing_options(
                TextFieldIndexing::default().set_index_option(IndexRecordOption::WithFreqs),
            )
            .set_stored();
        let text_field = schema_builder.add_text_field("text", text_fieldtype);
        let date_field = schema_builder.add_date_field("date", INDEXED);
        let score_fieldtype = schema::NumericOptions::default().set_fast();
        let score_field = schema_builder.add_u64_field("score", score_fieldtype);
        let bytes_score_field = schema_builder.add_bytes_field("score_bytes", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        let reader = index.reader()?;
        let curr_time = OffsetDateTime::now_utc();
        {
            let mut index_writer = index.writer_for_tests()?;
            // writing the segment
            index_writer.add_document(doc!(
                text_field => "af b",
                score_field => 3u64,
                date_field => DateTime::from_utc(curr_time),
                bytes_score_field => 3u32.to_be_bytes().as_ref()
            ))?;
            index_writer.add_document(doc!(
                text_field => "a b c",
                score_field => 5u64,
                bytes_score_field => 5u32.to_be_bytes().as_ref()
            ))?;
            index_writer.add_document(doc!(
                text_field => "a b c d",
                score_field => 7u64,
                bytes_score_field => 7u32.to_be_bytes().as_ref()
            ))?;
            index_writer.commit()?;
            // writing the segment
            index_writer.add_document(doc!(
                text_field => "af b",
                date_field => DateTime::from_utc(curr_time),
                score_field => 11u64,
                bytes_score_field => 11u32.to_be_bytes().as_ref()
            ))?;
            index_writer.add_document(doc!(
                text_field => "a b c g",
                score_field => 13u64,
                bytes_score_field => 13u32.to_be_bytes().as_ref()
            ))?;
            index_writer.commit()?;
        }
        {
            let segment_ids = index
                .searchable_segment_ids()
                .expect("Searchable segments failed.");
            let mut index_writer: IndexWriter = index.writer_for_tests()?;
            index_writer.merge(&segment_ids).wait()?;
            index_writer.wait_merging_threads()?;
        }
        {
            reader.reload()?;
            let searcher = reader.searcher();
            let get_doc_ids = |terms: Vec<Term>| {
                let query = BooleanQuery::new_multiterms_query(terms);
                searcher
                    .search(&query, &TEST_COLLECTOR_WITH_SCORE)
                    .map(|top_docs| top_docs.docs().to_vec())
            };
            {
                assert_eq!(
                    get_doc_ids(vec![Term::from_field_text(text_field, "a")])?,
                    vec![
                        DocAddress::new(0, 1),
                        DocAddress::new(0, 2),
                        DocAddress::new(0, 4)
                    ]
                );
                assert_eq!(
                    get_doc_ids(vec![Term::from_field_text(text_field, "af")])?,
                    vec![DocAddress::new(0, 0), DocAddress::new(0, 3)]
                );
                assert_eq!(
                    get_doc_ids(vec![Term::from_field_text(text_field, "g")])?,
                    vec![DocAddress::new(0, 4)]
                );
                assert_eq!(
                    get_doc_ids(vec![Term::from_field_text(text_field, "b")])?,
                    vec![
                        DocAddress::new(0, 0),
                        DocAddress::new(0, 1),
                        DocAddress::new(0, 2),
                        DocAddress::new(0, 3),
                        DocAddress::new(0, 4)
                    ]
                );
                assert_eq!(
                    get_doc_ids(vec![Term::from_field_date_for_search(
                        date_field,
                        DateTime::from_utc(curr_time)
                    )])?,
                    vec![DocAddress::new(0, 0), DocAddress::new(0, 3)]
                );
            }
            {
                let doc = searcher.doc::<TantivyDocument>(DocAddress::new(0, 0))?;
                assert_eq!(
                    doc.get_first(text_field).unwrap().as_value().as_str(),
                    Some("af b")
                );
            }
            {
                let doc = searcher.doc::<TantivyDocument>(DocAddress::new(0, 1))?;
                assert_eq!(
                    doc.get_first(text_field).unwrap().as_value().as_str(),
                    Some("a b c")
                );
            }
            {
                let doc = searcher.doc::<TantivyDocument>(DocAddress::new(0, 2))?;
                assert_eq!(
                    doc.get_first(text_field).unwrap().as_value().as_str(),
                    Some("a b c d")
                );
            }
            {
                let doc = searcher.doc::<TantivyDocument>(DocAddress::new(0, 3))?;
                assert_eq!(doc.get_first(text_field).unwrap().as_str(), Some("af b"));
            }
            {
                let doc = searcher.doc::<TantivyDocument>(DocAddress::new(0, 4))?;
                assert_eq!(doc.get_first(text_field).unwrap().as_str(), Some("a b c g"));
            }

            {
                let get_fast_vals = |terms: Vec<Term>| {
                    let query = BooleanQuery::new_multiterms_query(terms);
                    searcher.search(&query, &FastFieldTestCollector::for_field("score"))
                };
                let get_fast_vals_bytes = |terms: Vec<Term>| {
                    let query = BooleanQuery::new_multiterms_query(terms);
                    searcher.search(
                        &query,
                        &BytesFastFieldTestCollector::for_field("score_bytes"),
                    )
                };
                assert_eq!(
                    get_fast_vals(vec![Term::from_field_text(text_field, "a")])?,
                    vec![5, 7, 13]
                );
                assert_eq!(
                    get_fast_vals_bytes(vec![Term::from_field_text(text_field, "a")])?,
                    vec![0, 0, 0, 5, 0, 0, 0, 7, 0, 0, 0, 13]
                );
            }
        }
        Ok(())
    }

    #[test]
    fn test_index_merger_with_deletes() -> crate::Result<()> {
        let mut schema_builder = schema::Schema::builder();
        let text_fieldtype = schema::TextOptions::default()
            .set_indexing_options(
                TextFieldIndexing::default().set_index_option(IndexRecordOption::WithFreqs),
            )
            .set_stored();
        let text_field = schema_builder.add_text_field("text", text_fieldtype);
        let score_fieldtype = schema::NumericOptions::default().set_fast();
        let score_field = schema_builder.add_u64_field("score", score_fieldtype);
        let bytes_score_field = schema_builder.add_bytes_field("score_bytes", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        let mut index_writer = index.writer_for_tests()?;
        let reader = index.reader().unwrap();
        let search_term = |searcher: &Searcher, term: Term| {
            let collector = FastFieldTestCollector::for_field("score");
            // let bytes_collector = BytesFastFieldTestCollector::for_field(bytes_score_field);
            let term_query = TermQuery::new(term, IndexRecordOption::Basic);
            // searcher
            //     .search(&term_query, &(collector, bytes_collector))
            //     .map(|(scores, bytes)| {
            //         let mut score_bytes = &bytes[..];
            //         for &score in &scores {
            //             assert_eq!(score as u32, score_bytes.read_u32::<BigEndian>().unwrap());
            //         }
            //         scores
            //     })
            searcher.search(&term_query, &collector)
        };

        let empty_vec = Vec::<u64>::new();
        {
            // a first commit
            index_writer.add_document(doc!(
                text_field => "a b d",
                score_field => 1u64,
                bytes_score_field => vec![0u8, 0, 0, 1],
            ))?;
            index_writer.add_document(doc!(
                text_field => "b c",
                score_field => 2u64,
                bytes_score_field => vec![0u8, 0, 0, 2],
            ))?;
            index_writer.delete_term(Term::from_field_text(text_field, "c"));
            index_writer.add_document(doc!(
                text_field => "c d",
                score_field => 3u64,
                bytes_score_field => vec![0u8, 0, 0, 3],
            ))?;
            index_writer.commit()?;
            reader.reload()?;
            let searcher = reader.searcher();
            assert_eq!(searcher.num_docs(), 2);
            assert_eq!(searcher.segment_readers()[0].num_docs(), 2);
            assert_eq!(searcher.segment_readers()[0].max_doc(), 3);
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "a"))?,
                vec![1]
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "b"))?,
                vec![1]
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "c"))?,
                vec![3]
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "d"))?,
                vec![1, 3]
            );
        }
        {
            // a second commit
            index_writer.add_document(doc!(
                text_field => "a d e",
                score_field => 4_000u64,
                bytes_score_field => vec![0u8, 0, 0, 4],
            ))?;
            index_writer.add_document(doc!(
                text_field => "e f",
                score_field => 5_000u64,
                bytes_score_field => vec![0u8, 0, 0, 5],
            ))?;
            index_writer.delete_term(Term::from_field_text(text_field, "a"));
            index_writer.delete_term(Term::from_field_text(text_field, "f"));
            index_writer.add_document(doc!(
                text_field => "f g",
                score_field => 6_000u64,
                bytes_score_field => vec![0u8, 0, 23, 112],
            ))?;
            index_writer.add_document(doc!(
                text_field => "g h",
                score_field => 7_000u64,
                bytes_score_field => vec![0u8, 0, 27, 88],
            ))?;
            index_writer.commit()?;
            reader.reload()?;
            let searcher = reader.searcher();

            assert_eq!(searcher.segment_readers().len(), 2);
            assert_eq!(searcher.num_docs(), 3);
            assert_eq!(searcher.segment_readers()[0].num_docs(), 2);
            assert_eq!(searcher.segment_readers()[0].max_doc(), 4);
            assert_eq!(searcher.segment_readers()[1].num_docs(), 1);
            assert_eq!(searcher.segment_readers()[1].max_doc(), 3);
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "a"))?,
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "b"))?,
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "c"))?,
                vec![3]
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "d"))?,
                vec![3]
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "e"))?,
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "f"))?,
                vec![6_000]
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "g"))?,
                vec![6_000, 7_000]
            );

            let score_field_reader = searcher
                .segment_reader(0)
                .fast_fields()
                .u64("score")
                .unwrap();
            assert_eq!(score_field_reader.min_value(), 4000);
            assert_eq!(score_field_reader.max_value(), 7000);

            let score_field_reader = searcher
                .segment_reader(1)
                .fast_fields()
                .u64("score")
                .unwrap();
            assert_eq!(score_field_reader.min_value(), 1);
            assert_eq!(score_field_reader.max_value(), 3);
        }
        {
            // merging the segments
            let segment_ids = index.searchable_segment_ids()?;
            index_writer.merge(&segment_ids).wait()?;
            reader.reload()?;
            let searcher = reader.searcher();
            assert_eq!(searcher.segment_readers().len(), 1);
            assert_eq!(searcher.num_docs(), 3);
            assert_eq!(searcher.segment_readers()[0].num_docs(), 3);
            assert_eq!(searcher.segment_readers()[0].max_doc(), 3);
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "a"))?,
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "b"))?,
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "c"))?,
                vec![3]
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "d"))?,
                vec![3]
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "e"))?,
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "f"))?,
                vec![6_000]
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "g"))?,
                vec![6_000, 7_000]
            );
            let score_field_reader = searcher
                .segment_reader(0)
                .fast_fields()
                .u64("score")
                .unwrap();
            assert_eq!(score_field_reader.min_value(), 3);
            assert_eq!(score_field_reader.max_value(), 7000);
        }
        {
            // test a commit with only deletes
            index_writer.delete_term(Term::from_field_text(text_field, "c"));
            index_writer.commit()?;

            reader.reload()?;
            let searcher = reader.searcher();
            assert_eq!(searcher.segment_readers().len(), 1);
            assert_eq!(searcher.num_docs(), 2);
            assert_eq!(searcher.segment_readers()[0].num_docs(), 2);
            assert_eq!(searcher.segment_readers()[0].max_doc(), 3);
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "a"))?,
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "b"))?,
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "c"))?,
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "d"))?,
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "e"))?,
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "f"))?,
                vec![6_000]
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "g"))?,
                vec![6_000, 7_000]
            );
            let score_field_reader = searcher
                .segment_reader(0)
                .fast_fields()
                .u64("score")
                .unwrap();
            assert_eq!(score_field_reader.min_value(), 3);
            assert_eq!(score_field_reader.max_value(), 7000);
        }
        {
            // Test merging a single segment in order to remove deletes.
            let segment_ids = index.searchable_segment_ids()?;
            index_writer.merge(&segment_ids).wait()?;
            reader.reload()?;

            let searcher = reader.searcher();
            assert_eq!(searcher.segment_readers().len(), 1);
            assert_eq!(searcher.num_docs(), 2);
            assert_eq!(searcher.segment_readers()[0].num_docs(), 2);
            assert_eq!(searcher.segment_readers()[0].max_doc(), 2);
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "a"))?,
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "b"))?,
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "c"))?,
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "d"))?,
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "e"))?,
                empty_vec
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "f"))?,
                vec![6_000]
            );
            assert_eq!(
                search_term(&searcher, Term::from_field_text(text_field, "g"))?,
                vec![6_000, 7_000]
            );
            let score_field_reader = searcher
                .segment_reader(0)
                .fast_fields()
                .u64("score")
                .unwrap();
            assert_eq!(score_field_reader.min_value(), 6000);
            assert_eq!(score_field_reader.max_value(), 7000);
        }

        {
            // Test removing all docs
            index_writer.delete_term(Term::from_field_text(text_field, "g"));
            index_writer.commit()?;
            let segment_ids = index.searchable_segment_ids()?;
            reader.reload()?;

            let searcher = reader.searcher();
            assert!(segment_ids.is_empty());
            assert!(searcher.segment_readers().is_empty());
            assert_eq!(searcher.num_docs(), 0);
        }
        Ok(())
    }

    #[test]
    fn test_merge_facets_sort_none() {
        test_merge_facets(None, true)
    }

    // force_segment_value_overlap forces the int value for sorting to have overlapping min and max
    // ranges between segments so that merge algorithm can't apply certain optimizations
    fn test_merge_facets(index_settings: Option<IndexSettings>, force_segment_value_overlap: bool) {
        let mut schema_builder = schema::Schema::builder();
        let facet_field = schema_builder.add_facet_field("facet", FacetOptions::default());
        let int_options = NumericOptions::default().set_fast().set_indexed();
        let int_field = schema_builder.add_u64_field("intval", int_options);
        let mut index_builder = Index::builder().schema(schema_builder.build());
        if let Some(settings) = index_settings {
            index_builder = index_builder.settings(settings);
        }
        let index = index_builder.create_in_ram().unwrap();
        // let index = Index::create_in_ram(schema_builder.build());
        let reader = index.reader().unwrap();
        let mut int_val = 0;
        {
            let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();
            let index_doc =
                |index_writer: &mut IndexWriter, doc_facets: &[&str], int_val: &mut u64| {
                    let mut doc = TantivyDocument::default();
                    for facet in doc_facets {
                        doc.add_facet(facet_field, Facet::from(facet));
                    }
                    doc.add_u64(int_field, *int_val);
                    *int_val += 1;
                    index_writer.add_document(doc).unwrap();
                };

            index_doc(
                &mut index_writer,
                &["/top/a/firstdoc", "/top/b"],
                &mut int_val,
            );
            index_doc(
                &mut index_writer,
                &["/top/a/firstdoc", "/top/b", "/top/c"],
                &mut int_val,
            );
            index_doc(&mut index_writer, &["/top/a", "/top/b"], &mut int_val);
            index_doc(&mut index_writer, &["/top/a"], &mut int_val);

            index_doc(&mut index_writer, &["/top/b", "/top/d"], &mut int_val);
            if force_segment_value_overlap {
                index_doc(&mut index_writer, &["/top/d"], &mut 0);
                index_doc(&mut index_writer, &["/top/e"], &mut 10);
                index_writer.commit().expect("committed");
                index_doc(&mut index_writer, &["/top/a"], &mut 5); // 5 is between 0 - 10 so the
                                                                   // segments don' have disjunct
                                                                   // ranges
            } else {
                index_doc(&mut index_writer, &["/top/d"], &mut int_val);
                index_doc(&mut index_writer, &["/top/e"], &mut int_val);
                index_writer.commit().expect("committed");
                index_doc(&mut index_writer, &["/top/a"], &mut int_val);
            }
            index_doc(&mut index_writer, &["/top/b"], &mut int_val);
            index_doc(&mut index_writer, &["/top/c"], &mut int_val);
            index_writer.commit().expect("committed");

            index_doc(&mut index_writer, &["/top/e", "/top/f"], &mut int_val);
            index_writer.commit().expect("committed");
        }

        reader.reload().unwrap();
        let test_searcher = |expected_num_docs: usize, expected: &[(&str, u64)]| {
            let searcher = reader.searcher();
            let mut facet_collector = FacetCollector::for_field("facet");
            facet_collector.add_facet(Facet::from("/top"));
            let (count, facet_counts) = searcher
                .search(&AllQuery, &(Count, facet_collector))
                .unwrap();
            assert_eq!(count, expected_num_docs);
            let facets: Vec<(String, u64)> = facet_counts
                .get("/top")
                .map(|(facet, count)| (facet.to_string(), count))
                .collect();
            assert_eq!(
                facets,
                expected
                    .iter()
                    .map(|&(facet_str, count)| (String::from(facet_str), count))
                    .collect::<Vec<_>>()
            );
        };
        test_searcher(
            11,
            &[
                ("/top/a", 5),
                ("/top/b", 5),
                ("/top/c", 2),
                ("/top/d", 2),
                ("/top/e", 2),
                ("/top/f", 1),
            ],
        );
        // Merging the segments
        {
            let segment_ids = index
                .searchable_segment_ids()
                .expect("Searchable segments failed.");
            let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();
            index_writer
                .merge(&segment_ids)
                .wait()
                .expect("Merging failed");
            index_writer.wait_merging_threads().unwrap();
            reader.reload().unwrap();
            test_searcher(
                11,
                &[
                    ("/top/a", 5),
                    ("/top/b", 5),
                    ("/top/c", 2),
                    ("/top/d", 2),
                    ("/top/e", 2),
                    ("/top/f", 1),
                ],
            );
        }

        // Deleting one term
        {
            let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();
            let facet = Facet::from_path(vec!["top", "a", "firstdoc"]);
            let facet_term = Term::from_facet(facet_field, &facet);
            index_writer.delete_term(facet_term);
            index_writer.commit().unwrap();
            reader.reload().unwrap();
            test_searcher(
                9,
                &[
                    ("/top/a", 3),
                    ("/top/b", 3),
                    ("/top/c", 1),
                    ("/top/d", 2),
                    ("/top/e", 2),
                    ("/top/f", 1),
                ],
            );
        }
    }

    #[test]
    fn test_bug_merge() -> crate::Result<()> {
        let mut schema_builder = schema::Schema::builder();
        let int_field = schema_builder.add_u64_field("intvals", INDEXED);
        let index = Index::create_in_ram(schema_builder.build());
        let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();
        index_writer.add_document(doc!(int_field => 1u64))?;
        index_writer.commit().expect("commit failed");
        index_writer.add_document(doc!(int_field => 1u64))?;
        index_writer.commit().expect("commit failed");
        let reader = index.reader()?;
        let searcher = reader.searcher();
        assert_eq!(searcher.num_docs(), 2);
        index_writer.delete_term(Term::from_field_u64(int_field, 1));
        let segment_ids = index
            .searchable_segment_ids()
            .expect("Searchable segments failed.");
        index_writer.merge(&segment_ids).wait()?;
        reader.reload()?;
        // commit has not been called yet. The document should still be
        // there.
        assert_eq!(reader.searcher().num_docs(), 2);
        Ok(())
    }

    #[test]
    fn test_merge_multivalued_int_fields_all_deleted() -> crate::Result<()> {
        let mut schema_builder = schema::Schema::builder();
        let int_options = NumericOptions::default().set_fast().set_indexed();
        let int_field = schema_builder.add_u64_field("intvals", int_options);
        let index = Index::create_in_ram(schema_builder.build());
        let reader = index.reader()?;
        {
            let mut index_writer = index.writer_for_tests()?;
            let mut doc = TantivyDocument::default();
            doc.add_u64(int_field, 1);
            index_writer.add_document(doc.clone())?;
            index_writer.commit()?;
            index_writer.add_document(doc)?;
            index_writer.commit()?;
            index_writer.delete_term(Term::from_field_u64(int_field, 1));
            let segment_ids = index.searchable_segment_ids()?;
            index_writer.merge(&segment_ids).wait()?;

            // assert delete has not been committed
            reader.reload()?;
            let searcher = reader.searcher();
            assert_eq!(searcher.num_docs(), 2);

            index_writer.commit()?;

            index_writer.wait_merging_threads()?;
        }

        reader.reload()?;
        let searcher = reader.searcher();
        assert_eq!(searcher.num_docs(), 0);
        Ok(())
    }

    #[derive(Debug, Clone, Copy, Eq, PartialEq)]
    enum IndexingOp {
        ZeroVal,
        OneVal { val: u64 },
        TwoVal { val: u64 },
        Commit,
    }

    fn balanced_operation_strategy() -> impl Strategy<Value = IndexingOp> {
        prop_oneof![
            (0u64..1u64).prop_map(|_| IndexingOp::ZeroVal),
            (0u64..1u64).prop_map(|val| IndexingOp::OneVal { val }),
            (0u64..1u64).prop_map(|val| IndexingOp::TwoVal { val }),
            (0u64..1u64).prop_map(|_| IndexingOp::Commit),
        ]
    }

    use proptest::prelude::*;
    proptest! {
        #[test]
        fn test_merge_columnar_int_proptest(ops in proptest::collection::vec(balanced_operation_strategy(), 1..20)) {
            assert!(test_merge_int_fields(&ops[..]).is_ok());
        }
    }
    fn test_merge_int_fields(ops: &[IndexingOp]) -> crate::Result<()> {
        if ops.iter().all(|op| *op == IndexingOp::Commit) {
            return Ok(());
        }
        let expected_doc_and_vals: Vec<(u32, Vec<u64>)> = ops
            .iter()
            .filter(|op| *op != &IndexingOp::Commit)
            .map(|op| match op {
                IndexingOp::ZeroVal => vec![],
                IndexingOp::OneVal { val } => vec![*val],
                IndexingOp::TwoVal { val } => vec![*val, *val],
                IndexingOp::Commit => unreachable!(),
            })
            .enumerate()
            .map(|(id, val)| (id as u32, val))
            .collect();

        let mut schema_builder = schema::Schema::builder();
        let int_options = NumericOptions::default().set_fast().set_indexed();
        let int_field = schema_builder.add_u64_field("intvals", int_options);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut index_writer = index.writer_for_tests()?;
            index_writer.set_merge_policy(Box::new(NoMergePolicy));
            let index_doc = |index_writer: &mut IndexWriter, int_vals: &[u64]| {
                let mut doc = TantivyDocument::default();
                for &val in int_vals {
                    doc.add_u64(int_field, val);
                }
                index_writer.add_document(doc).unwrap();
            };

            for op in ops {
                match op {
                    IndexingOp::ZeroVal => index_doc(&mut index_writer, &[]),
                    IndexingOp::OneVal { val } => index_doc(&mut index_writer, &[*val]),
                    IndexingOp::TwoVal { val } => index_doc(&mut index_writer, &[*val, *val]),
                    IndexingOp::Commit => {
                        index_writer.commit().expect("commit failed");
                    }
                }
            }
            index_writer.commit().expect("commit failed");
        }
        {
            let mut segment_ids = index.searchable_segment_ids()?;
            segment_ids.sort();
            let mut index_writer: IndexWriter = index.writer_for_tests()?;
            index_writer.merge(&segment_ids).wait()?;
            index_writer.wait_merging_threads()?;
        }
        let reader = index.reader()?;
        reader.reload()?;

        let mut vals: Vec<u64> = Vec::new();
        let mut test_vals = move |col: &Column<u64>, doc: DocId, expected: &[u64]| {
            vals.clear();
            vals.extend(col.values_for_doc(doc));
            assert_eq!(&vals[..], expected);
        };

        let mut test_col = move |col: &Column<u64>, column_expected: &[(u32, Vec<u64>)]| {
            for (doc_id, vals) in column_expected.iter() {
                test_vals(col, *doc_id, vals);
            }
        };

        {
            let searcher = reader.searcher();
            let segment = searcher.segment_reader(0u32);
            let col = segment
                .fast_fields()
                .column_opt::<u64>("intvals")
                .unwrap()
                .unwrap();

            test_col(&col, &expected_doc_and_vals);
        }

        Ok(())
    }

    #[test]
    fn test_merge_multivalued_int_fields_simple() -> crate::Result<()> {
        let mut schema_builder = schema::Schema::builder();
        let int_options = NumericOptions::default().set_fast().set_indexed();
        let int_field = schema_builder.add_u64_field("intvals", int_options);
        let index = Index::create_in_ram(schema_builder.build());

        let mut vals: Vec<u64> = Vec::new();
        let mut test_vals = move |col: &Column<u64>, doc: DocId, expected: &[u64]| {
            vals.clear();
            vals.extend(col.values_for_doc(doc));
            assert_eq!(&vals[..], expected);
        };

        {
            let mut index_writer = index.writer_for_tests()?;
            let index_doc = |index_writer: &mut IndexWriter, int_vals: &[u64]| {
                let mut doc = TantivyDocument::default();
                for &val in int_vals {
                    doc.add_u64(int_field, val);
                }
                index_writer.add_document(doc).unwrap();
            };
            index_doc(&mut index_writer, &[1, 2]);
            index_doc(&mut index_writer, &[1, 2, 3]);
            index_doc(&mut index_writer, &[4, 5]);
            index_doc(&mut index_writer, &[1, 2]);
            index_doc(&mut index_writer, &[1, 5]);
            index_doc(&mut index_writer, &[3]);
            index_doc(&mut index_writer, &[17]);
            assert!(index_writer.commit().is_ok());
            index_doc(&mut index_writer, &[20]);
            assert!(index_writer.commit().is_ok());
            index_doc(&mut index_writer, &[28, 27]);
            index_doc(&mut index_writer, &[1_000]);
            assert!(index_writer.commit().is_ok());
        }
        let reader = index.reader()?;
        let searcher = reader.searcher();

        {
            let segment = searcher.segment_reader(0u32);
            let column = segment
                .fast_fields()
                .column_opt::<u64>("intvals")
                .unwrap()
                .unwrap();
            test_vals(&column, 0, &[1, 2]);
            test_vals(&column, 1, &[1, 2, 3]);
            test_vals(&column, 2, &[4, 5]);
            test_vals(&column, 3, &[1, 2]);
            test_vals(&column, 4, &[1, 5]);
            test_vals(&column, 5, &[3]);
            test_vals(&column, 6, &[17]);
        }

        {
            let segment = searcher.segment_reader(1u32);
            let col = segment
                .fast_fields()
                .column_opt::<u64>("intvals")
                .unwrap()
                .unwrap();
            test_vals(&col, 0, &[28, 27]);
            test_vals(&col, 1, &[1000]);
        }

        {
            let segment = searcher.segment_reader(2u32);
            let col = segment
                .fast_fields()
                .column_opt::<u64>("intvals")
                .unwrap()
                .unwrap();
            test_vals(&col, 0, &[20]);
        }

        // Merging the segments
        {
            let segment_ids = index.searchable_segment_ids()?;
            let mut index_writer: IndexWriter = index.writer_for_tests()?;
            index_writer.merge(&segment_ids).wait()?;
            index_writer.wait_merging_threads()?;
        }
        reader.reload()?;

        {
            let searcher = reader.searcher();
            let segment = searcher.segment_reader(0u32);
            let col = segment
                .fast_fields()
                .column_opt::<u64>("intvals")
                .unwrap()
                .unwrap();
            test_vals(&col, 0, &[1, 2]);
            test_vals(&col, 1, &[1, 2, 3]);
            test_vals(&col, 2, &[4, 5]);
            test_vals(&col, 3, &[1, 2]);
            test_vals(&col, 4, &[1, 5]);
            test_vals(&col, 5, &[3]);
            test_vals(&col, 6, &[17]);
            test_vals(&col, 7, &[28, 27]);
            test_vals(&col, 8, &[1000]);
            test_vals(&col, 9, &[20]);
        }
        Ok(())
    }

    #[test]
    fn merges_f64_fast_fields_correctly() -> crate::Result<()> {
        let mut builder = schema::SchemaBuilder::new();

        let fast_multi = NumericOptions::default().set_fast();

        let field = builder.add_f64_field("f64", schema::FAST);
        let multi_field = builder.add_f64_field("f64s", fast_multi);

        let index = Index::create_in_ram(builder.build());

        let mut writer = index.writer_for_tests()?;

        // Make sure we'll attempt to merge every created segment
        let mut policy = crate::indexer::LogMergePolicy::default();
        policy.set_min_num_segments(2);
        writer.set_merge_policy(Box::new(policy));

        for i in 0..100 {
            let mut doc = TantivyDocument::new();
            doc.add_f64(field, 42.0);
            doc.add_f64(multi_field, 0.24);
            doc.add_f64(multi_field, 0.27);
            writer.add_document(doc)?;
            if i % 5 == 0 {
                writer.commit()?;
            }
        }

        writer.commit()?;
        writer.wait_merging_threads()?;

        // If a merging thread fails, we should end up with more
        // than one segment here
        assert_eq!(1, index.searchable_segments()?.len());
        Ok(())
    }

    #[test]
    fn test_merged_index_has_blockwand() -> crate::Result<()> {
        let mut builder = schema::SchemaBuilder::new();
        let text = builder.add_text_field("text", TEXT);
        let index = Index::create_in_ram(builder.build());
        let mut writer = index.writer_for_tests()?;
        let happy_term = Term::from_field_text(text, "happy");
        let term_query = TermQuery::new(happy_term, IndexRecordOption::WithFreqs);
        for _ in 0..62 {
            writer.add_document(doc!(text=>"hello happy tax payer"))?;
        }
        writer.commit()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let mut term_scorer = term_query
            .specialized_weight(EnableScoring::enabled_from_searcher(&searcher))?
            .term_scorer_for_test(searcher.segment_reader(0u32), 1.0)?
            .unwrap();
        assert_eq!(term_scorer.doc(), 0);
        assert_nearly_equals!(term_scorer.block_max_score(), 0.0079681855);
        assert_nearly_equals!(term_scorer.score(), 0.0079681855);
        for _ in 0..81 {
            writer.add_document(doc!(text=>"hello happy tax payer"))?;
        }
        writer.commit()?;
        reader.reload()?;
        let searcher = reader.searcher();

        assert_eq!(searcher.segment_readers().len(), 2);
        for segment_reader in searcher.segment_readers() {
            let mut term_scorer = term_query
                .specialized_weight(EnableScoring::enabled_from_searcher(&searcher))?
                .term_scorer_for_test(segment_reader, 1.0)?
                .unwrap();
            // the difference compared to before is intrinsic to the bm25 formula. no worries
            // there.
            for doc in segment_reader.doc_ids_alive() {
                assert_eq!(term_scorer.doc(), doc);
                assert_nearly_equals!(term_scorer.block_max_score(), 0.003478312);
                assert_nearly_equals!(term_scorer.score(), 0.003478312);
                term_scorer.advance();
            }
        }

        let segment_ids: Vec<SegmentId> = searcher
            .segment_readers()
            .iter()
            .map(|reader| reader.segment_id())
            .collect();
        writer.merge(&segment_ids[..]).wait()?;

        reader.reload()?;
        let searcher = reader.searcher();
        assert_eq!(searcher.segment_readers().len(), 1);

        let segment_reader = searcher.segment_reader(0u32);
        let mut term_scorer = term_query
            .specialized_weight(EnableScoring::enabled_from_searcher(&searcher))?
            .term_scorer_for_test(segment_reader, 1.0)?
            .unwrap();
        // the difference compared to before is intrinsic to the bm25 formula. no worries there.
        for doc in segment_reader.doc_ids_alive() {
            assert_eq!(term_scorer.doc(), doc);
            assert_nearly_equals!(term_scorer.block_max_score(), 0.003478312);
            assert_nearly_equals!(term_scorer.score(), 0.003478312);
            term_scorer.advance();
        }

        Ok(())
    }

    #[test]
    fn test_max_doc() {
        // this is the first time I write a unit test for a constant.
        assert!(((super::MAX_DOC_LIMIT - 1) as i32) >= 0);
        assert!((super::MAX_DOC_LIMIT as i32) < 0);
    }
}
