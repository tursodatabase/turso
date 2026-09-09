use std::io::{self, Write};

use common::{BitSet, CountingWriter, ReadOnlyBitSet};
use sstable::{SSTable, Streamer, TermOrdinal, VoidSSTable};

use super::term_merger::{TermMerger, TermsWithSegmentOrd};
use crate::column::serialize_column_mappable_to_u64;
use crate::column_index::SerializableColumnIndex;
use crate::iterable::Iterable;
use crate::{BytesColumn, MergeRowOrder, ShuffleMergeOrder};

// Serialize [Dictionary, Column, dictionary num bytes U32::LE]
// Column: [Column Index, Column Values, column index num bytes U32::LE]
pub fn merge_bytes_or_str_column(
    column_index: SerializableColumnIndex<'_>,
    bytes_columns: &[Option<BytesColumn>],
    merge_row_order: &MergeRowOrder,
    output: &mut impl Write,
) -> io::Result<()> {
    // Serialize dict and generate mapping for values
    let mut output = CountingWriter::wrap(output);
    // TODO !!! Remove useless terms.
    let term_ord_mapping = serialize_merged_dict(bytes_columns, merge_row_order, &mut output)?;
    let dictionary_num_bytes: u32 = output.written_bytes() as u32;
    let output = output.finish();
    let remapped_term_ordinals_values = RemappedTermOrdinalsValues {
        bytes_columns,
        term_ord_mapping: &term_ord_mapping,
        merge_row_order,
    };
    serialize_column_mappable_to_u64(column_index, &remapped_term_ordinals_values, output)?;
    output.write_all(&dictionary_num_bytes.to_le_bytes())?;
    Ok(())
}

struct RemappedTermOrdinalsValues<'a> {
    bytes_columns: &'a [Option<BytesColumn>],
    term_ord_mapping: &'a TermOrdinalMapping,
    merge_row_order: &'a MergeRowOrder,
}

impl Iterable for RemappedTermOrdinalsValues<'_> {
    fn boxed_iter(&self) -> Box<dyn Iterator<Item = u64> + '_> {
        match self.merge_row_order {
            MergeRowOrder::Stack(_) => self.boxed_iter_stacked(),
            MergeRowOrder::Shuffled(shuffle_merge_order) => {
                self.boxed_iter_shuffled(shuffle_merge_order)
            }
        }
    }
}

impl RemappedTermOrdinalsValues<'_> {
    fn boxed_iter_stacked(&self) -> Box<dyn Iterator<Item = u64> + '_> {
        let iter = self
            .bytes_columns
            .iter()
            .enumerate()
            .flat_map(|(seg_ord, bytes_column_opt)| {
                let bytes_column = bytes_column_opt.as_ref()?;
                Some((seg_ord, bytes_column))
            })
            .flat_map(move |(seg_ord, bytes_column)| {
                let term_ord_after_merge_mapping =
                    self.term_ord_mapping.get_segment(seg_ord as u32);
                bytes_column
                    .ords()
                    .values
                    .iter()
                    .map(move |term_ord| term_ord_after_merge_mapping[term_ord as usize])
            });
        Box::new(iter)
    }

    fn boxed_iter_shuffled<'b>(
        &'b self,
        shuffle_merge_order: &'b ShuffleMergeOrder,
    ) -> Box<dyn Iterator<Item = u64> + 'b> {
        Box::new(
            shuffle_merge_order
                .iter_new_to_old_row_addrs()
                .flat_map(move |old_addr| {
                    let segment_ord = self.term_ord_mapping.get_segment(old_addr.segment_ord);
                    self.bytes_columns[old_addr.segment_ord as usize]
                        .as_ref()
                        .into_iter()
                        .flat_map(move |bytes_column| {
                            bytes_column
                                .term_ords(old_addr.row_id)
                                .map(|old_term_ord: u64| segment_ord[old_term_ord as usize])
                        })
                }),
        )
    }
}

fn compute_term_bitset(column: &BytesColumn, row_bitset: &ReadOnlyBitSet) -> BitSet {
    let num_terms = column.dictionary().num_terms();
    let mut term_bitset = BitSet::with_max_value(num_terms as u32);
    for row_id in row_bitset.iter() {
        for term_ord in column.term_ord_column.values_for_doc(row_id) {
            term_bitset.insert(term_ord as u32);
        }
    }
    term_bitset
}

fn is_term_present(bitsets: &[Option<BitSet>], term_merger: &TermMerger) -> bool {
    for (segment_ord, from_term_ord) in term_merger.matching_segments() {
        if let Some(bitset) = bitsets[segment_ord].as_ref() {
            if bitset.contains(from_term_ord as u32) {
                return true;
            }
        } else {
            return true;
        }
    }
    false
}

fn serialize_merged_dict(
    bytes_columns: &[Option<BytesColumn>],
    merge_row_order: &MergeRowOrder,
    output: &mut impl Write,
) -> io::Result<TermOrdinalMapping> {
    let mut term_ord_mapping = TermOrdinalMapping::default();

    let mut field_term_streams = Vec::new();
    for (segment_ord, column_opt) in bytes_columns.iter().enumerate() {
        if let Some(column) = column_opt {
            term_ord_mapping.add_segment(column.dictionary.num_terms());
            let terms: Streamer<VoidSSTable> = column.dictionary.stream()?;
            field_term_streams.push(TermsWithSegmentOrd { terms, segment_ord });
        } else {
            term_ord_mapping.add_segment(0);
            field_term_streams.push(TermsWithSegmentOrd {
                terms: Streamer::empty(),
                segment_ord,
            });
        }
    }

    let mut merged_terms = TermMerger::new(field_term_streams);
    let mut sstable_builder = sstable::VoidSSTable::writer(output);

    match merge_row_order {
        MergeRowOrder::Stack(_) => {
            let mut current_term_ord = 0;
            while merged_terms.advance() {
                let term_bytes: &[u8] = merged_terms.key();
                sstable_builder.insert(term_bytes, &())?;
                for (segment_ord, from_term_ord) in merged_terms.matching_segments() {
                    term_ord_mapping.register_from_to(segment_ord, from_term_ord, current_term_ord);
                }
                current_term_ord += 1;
            }
            sstable_builder.finish()?;
        }
        MergeRowOrder::Shuffled(shuffle_merge_order) => {
            assert_eq!(shuffle_merge_order.alive_bitsets.len(), bytes_columns.len());
            let mut term_bitsets: Vec<Option<BitSet>> = Vec::with_capacity(bytes_columns.len());
            for (alive_bitset_opt, bytes_column_opt) in shuffle_merge_order
                .alive_bitsets
                .iter()
                .zip(bytes_columns.iter())
            {
                match (alive_bitset_opt, bytes_column_opt) {
                    (Some(alive_bitset), Some(bytes_column)) => {
                        let term_bitset = compute_term_bitset(bytes_column, alive_bitset);
                        term_bitsets.push(Some(term_bitset));
                    }
                    _ => {
                        term_bitsets.push(None);
                    }
                }
            }
            let mut current_term_ord = 0;
            while merged_terms.advance() {
                let term_bytes: &[u8] = merged_terms.key();
                if !is_term_present(&term_bitsets[..], &merged_terms) {
                    continue;
                }
                sstable_builder.insert(term_bytes, &())?;
                for (segment_ord, from_term_ord) in merged_terms.matching_segments() {
                    term_ord_mapping.register_from_to(segment_ord, from_term_ord, current_term_ord);
                }
                current_term_ord += 1;
            }
            sstable_builder.finish()?;
        }
    }
    Ok(term_ord_mapping)
}

#[derive(Default, Debug)]
struct TermOrdinalMapping {
    /// Contains the new term ordinals for each segment.
    per_segment_new_term_ordinals: Vec<Vec<TermOrdinal>>,
}

impl TermOrdinalMapping {
    fn add_segment(&mut self, max_term_ord: usize) {
        self.per_segment_new_term_ordinals
            .push(vec![TermOrdinal::default(); max_term_ord]);
    }

    fn register_from_to(&mut self, segment_ord: usize, from_ord: TermOrdinal, to_ord: TermOrdinal) {
        self.per_segment_new_term_ordinals[segment_ord][from_ord as usize] = to_ord;
    }

    fn get_segment(&self, segment_ord: u32) -> &[TermOrdinal] {
        &self.per_segment_new_term_ordinals[segment_ord as usize]
    }
}
