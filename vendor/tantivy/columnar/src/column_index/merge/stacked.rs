use std::ops::Range;

use crate::column_index::SerializableColumnIndex;
use crate::column_index::multivalued_index::{MultiValueIndex, SerializableMultivalueIndex};
use crate::column_index::serialize::SerializableOptionalIndex;
use crate::iterable::Iterable;
use crate::{Cardinality, ColumnIndex, RowId, StackMergeOrder};

/// Simple case:
/// The new mapping just consists in stacking the different column indexes.
///
/// There are no sort nor deletes involved.
pub fn merge_column_index_stacked<'a>(
    columns: &'a [ColumnIndex],
    cardinality_after_merge: Cardinality,
    stack_merge_order: &'a StackMergeOrder,
) -> SerializableColumnIndex<'a> {
    match cardinality_after_merge {
        Cardinality::Full => SerializableColumnIndex::Full,
        Cardinality::Optional => SerializableColumnIndex::Optional(SerializableOptionalIndex {
            non_null_row_ids: Box::new(StackedOptionalIndex {
                columns,
                stack_merge_order,
            }),
            num_rows: stack_merge_order.num_rows(),
        }),
        Cardinality::Multivalued => {
            let serializable_multivalue_index =
                make_serializable_multivalued_index(columns, stack_merge_order);
            SerializableColumnIndex::Multivalued(serializable_multivalue_index)
        }
    }
}

struct StackedDocIdsWithValues<'a> {
    column_indexes: &'a [ColumnIndex],
    stack_merge_order: &'a StackMergeOrder,
}

impl Iterable<u32> for StackedDocIdsWithValues<'_> {
    fn boxed_iter(&self) -> Box<dyn Iterator<Item = u32> + '_> {
        Box::new((0..self.column_indexes.len()).flat_map(|i| {
            let column_index = &self.column_indexes[i];
            let doc_range = self.stack_merge_order.columnar_range(i);
            get_doc_ids_with_values(column_index, doc_range)
        }))
    }
}

fn get_doc_ids_with_values<'a>(
    column_index: &'a ColumnIndex,
    doc_range: Range<u32>,
) -> Box<dyn Iterator<Item = u32> + 'a> {
    match column_index {
        ColumnIndex::Empty { .. } => Box::new(0..0),
        ColumnIndex::Full => Box::new(doc_range),
        ColumnIndex::Optional(optional_index) => Box::new(
            optional_index
                .iter_non_null_docs()
                .map(move |row| row + doc_range.start),
        ),
        ColumnIndex::Multivalued(multivalued_index) => match multivalued_index {
            MultiValueIndex::MultiValueIndexV1(multivalued_index) => {
                Box::new((0..multivalued_index.num_docs()).filter_map(move |docid| {
                    let range = multivalued_index.range(docid);
                    if range.is_empty() {
                        None
                    } else {
                        Some(docid + doc_range.start)
                    }
                }))
            }
            MultiValueIndex::MultiValueIndexV2(multivalued_index) => Box::new(
                multivalued_index
                    .optional_index
                    .iter_non_null_docs()
                    .map(move |row| row + doc_range.start),
            ),
        },
    }
}

fn stack_doc_ids_with_values<'a>(
    column_indexes: &'a [ColumnIndex],
    stack_merge_order: &'a StackMergeOrder,
) -> SerializableOptionalIndex<'a> {
    let num_rows = stack_merge_order.num_rows();
    SerializableOptionalIndex {
        non_null_row_ids: Box::new(StackedDocIdsWithValues {
            column_indexes,
            stack_merge_order,
        }),
        num_rows,
    }
}

struct StackedStartOffsets<'a> {
    column_indexes: &'a [ColumnIndex],
    stack_merge_order: &'a StackMergeOrder,
}

fn get_num_values_iterator<'a>(
    column_index: &'a ColumnIndex,
    num_docs: u32,
) -> Box<dyn Iterator<Item = u32> + 'a> {
    match column_index {
        ColumnIndex::Empty { .. } => Box::new(std::iter::empty()),
        ColumnIndex::Full => Box::new(std::iter::repeat_n(1u32, num_docs as usize)),
        ColumnIndex::Optional(optional_index) => Box::new(std::iter::repeat_n(
            1u32,
            optional_index.num_non_nulls() as usize,
        )),
        ColumnIndex::Multivalued(multivalued_index) => Box::new(
            multivalued_index
                .get_start_index_column()
                .iter()
                .scan(0u32, |previous_start_offset, current_start_offset| {
                    let num_vals = current_start_offset - *previous_start_offset;
                    *previous_start_offset = current_start_offset;
                    Some(num_vals)
                })
                .skip(1),
        ),
    }
}

impl Iterable<u32> for StackedStartOffsets<'_> {
    fn boxed_iter(&self) -> Box<dyn Iterator<Item = u32> + '_> {
        let num_values_it = (0..self.column_indexes.len()).flat_map(|columnar_id| {
            let num_docs = self.stack_merge_order.columnar_range(columnar_id).len() as u32;
            let column_index = &self.column_indexes[columnar_id];
            get_num_values_iterator(column_index, num_docs)
        });
        Box::new(std::iter::once(0u32).chain(num_values_it.into_iter().scan(
            0u32,
            |cumulated, el| {
                *cumulated += el;
                Some(*cumulated)
            },
        )))
    }
}

fn stack_start_offsets<'a>(
    column_indexes: &'a [ColumnIndex],
    stack_merge_order: &'a StackMergeOrder,
) -> Box<dyn Iterable<u32> + 'a> {
    Box::new(StackedStartOffsets {
        column_indexes,
        stack_merge_order,
    })
}

fn make_serializable_multivalued_index<'a>(
    columns: &'a [ColumnIndex],
    stack_merge_order: &'a StackMergeOrder,
) -> SerializableMultivalueIndex<'a> {
    SerializableMultivalueIndex {
        doc_ids_with_values: stack_doc_ids_with_values(columns, stack_merge_order),
        start_offsets: stack_start_offsets(columns, stack_merge_order),
    }
}

struct StackedOptionalIndex<'a> {
    columns: &'a [ColumnIndex],
    stack_merge_order: &'a StackMergeOrder,
}

impl<'a> Iterable<RowId> for StackedOptionalIndex<'a> {
    fn boxed_iter(&self) -> Box<dyn Iterator<Item = RowId> + 'a> {
        Box::new(
            self.columns
                .iter()
                .enumerate()
                .flat_map(|(columnar_id, column_index_opt)| {
                    let columnar_row_range = self.stack_merge_order.columnar_range(columnar_id);
                    let rows_it: Box<dyn Iterator<Item = RowId>> = match column_index_opt {
                        ColumnIndex::Full => Box::new(columnar_row_range),
                        ColumnIndex::Optional(optional_index) => Box::new(
                            optional_index
                                .iter_non_null_docs()
                                .map(move |row_id: RowId| columnar_row_range.start + row_id),
                        ),
                        ColumnIndex::Multivalued(_) => {
                            panic!("No multivalued index is allowed when stacking column index");
                        }
                        ColumnIndex::Empty { .. } => Box::new(std::iter::empty()),
                    };
                    rows_it
                }),
        )
    }
}
