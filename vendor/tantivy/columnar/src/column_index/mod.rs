//! # `column_index`
//!
//! `column_index` provides rank and select operations to associate positions when not all
//! documents have exactly one element.

mod merge;
mod multivalued_index;
mod optional_index;
mod serialize;

use std::ops::Range;

pub use merge::merge_column_index;
pub(crate) use multivalued_index::SerializableMultivalueIndex;
pub use optional_index::{OptionalIndex, Set};
pub use serialize::{
    SerializableColumnIndex, SerializableOptionalIndex, open_column_index, serialize_column_index,
};

use crate::column_index::multivalued_index::MultiValueIndex;
use crate::{Cardinality, DocId, RowId};

#[derive(Clone, Debug)]
pub enum ColumnIndex {
    Empty {
        num_docs: u32,
    },
    Full,
    Optional(OptionalIndex),
    /// In addition, at index num_rows, an extra value is added
    /// containing the overall number of values.
    Multivalued(MultiValueIndex),
}

impl From<OptionalIndex> for ColumnIndex {
    fn from(optional_index: OptionalIndex) -> ColumnIndex {
        ColumnIndex::Optional(optional_index)
    }
}

impl From<MultiValueIndex> for ColumnIndex {
    fn from(multi_value_index: MultiValueIndex) -> ColumnIndex {
        ColumnIndex::Multivalued(multi_value_index)
    }
}

impl ColumnIndex {
    /// Returns the cardinality of the column index.
    ///
    /// By convention, if the column contains no docs, we consider that it is
    /// full.
    #[inline]
    pub fn get_cardinality(&self) -> Cardinality {
        match self {
            ColumnIndex::Empty { num_docs: 0 } | ColumnIndex::Full => Cardinality::Full,
            ColumnIndex::Empty { .. } => Cardinality::Optional,
            ColumnIndex::Optional(_) => Cardinality::Optional,
            ColumnIndex::Multivalued(_) => Cardinality::Multivalued,
        }
    }

    /// Returns true if and only if there are at least one value associated to the row.
    pub fn has_value(&self, doc_id: DocId) -> bool {
        match self {
            ColumnIndex::Empty { .. } => false,
            ColumnIndex::Full => true,
            ColumnIndex::Optional(optional_index) => optional_index.contains(doc_id),
            ColumnIndex::Multivalued(multivalued_index) => {
                !multivalued_index.range(doc_id).is_empty()
            }
        }
    }

    pub fn value_row_ids(&self, doc_id: DocId) -> Range<RowId> {
        match self {
            ColumnIndex::Empty { .. } => 0..0,
            ColumnIndex::Full => doc_id..doc_id + 1,
            ColumnIndex::Optional(optional_index) => {
                if let Some(val) = optional_index.rank_if_exists(doc_id) {
                    val..val + 1
                } else {
                    0..0
                }
            }
            ColumnIndex::Multivalued(multivalued_index) => multivalued_index.range(doc_id),
        }
    }

    /// Translates a block of docis to row_ids.
    ///
    /// returns the row_ids and the matching docids on the same index
    /// e.g.
    /// DocId In:  [0, 5, 6]
    /// DocId Out: [0, 0, 6, 6]
    /// RowId Out: [0, 1, 2, 3]
    #[inline]
    pub fn docids_to_rowids(
        &self,
        doc_ids: &[DocId],
        doc_ids_out: &mut Vec<DocId>,
        row_ids: &mut Vec<RowId>,
    ) {
        match self {
            ColumnIndex::Empty { .. } => {}
            ColumnIndex::Full => {
                doc_ids_out.extend_from_slice(doc_ids);
                row_ids.extend_from_slice(doc_ids);
            }
            ColumnIndex::Optional(optional_index) => {
                for doc_id in doc_ids {
                    if let Some(row_id) = optional_index.rank_if_exists(*doc_id) {
                        doc_ids_out.push(*doc_id);
                        row_ids.push(row_id);
                    }
                }
            }
            ColumnIndex::Multivalued(multivalued_index) => {
                for doc_id in doc_ids {
                    for row_id in multivalued_index.range(*doc_id) {
                        doc_ids_out.push(*doc_id);
                        row_ids.push(row_id);
                    }
                }
            }
        }
    }

    pub fn docid_range_to_rowids(&self, doc_id_range: Range<DocId>) -> Range<RowId> {
        match self {
            ColumnIndex::Empty { .. } => 0..0,
            ColumnIndex::Full => doc_id_range,
            ColumnIndex::Optional(optional_index) => {
                let row_start = optional_index.rank(doc_id_range.start);
                let row_end = optional_index.rank(doc_id_range.end);
                row_start..row_end
            }
            ColumnIndex::Multivalued(multivalued_index) => match multivalued_index {
                MultiValueIndex::MultiValueIndexV1(index) => {
                    let row_start = index.start_index_column.get_val(doc_id_range.start);
                    let row_end = index.start_index_column.get_val(doc_id_range.end);
                    row_start..row_end
                }
                MultiValueIndex::MultiValueIndexV2(index) => {
                    // In this case we will use the optional_index select the next values
                    // that are valid. There are different cases to consider:
                    // Not exists below means does not exist in the optional
                    // index, because it has no values.
                    // * doc_id_range may cover a range of docids which are non existent
                    // => rank
                    //   will give us the next document outside the range with a value. They both
                    //   get the same rank and therefore return a zero range
                    //
                    // * doc_id_range.start and doc_id_range.end may not exist, but docids in
                    // between may have values
                    // => rank will give us the next document outside the range with a value.
                    //
                    // * doc_id_range.start may be not existent but doc_id_range.end may exist
                    // * doc_id_range.start may exist but doc_id_range.end may not exist
                    // * doc_id_range.start and doc_id_range.end may exist
                    // => rank on doc_id_range.end will give use the next value, which matches
                    // how the `start_index_column` works, so we get the value start of the next
                    // docid which we use to create the exclusive range.
                    //
                    let rank_start = index.optional_index.rank(doc_id_range.start);
                    let row_start = index.start_index_column.get_val(rank_start);
                    let rank_end = index.optional_index.rank(doc_id_range.end);
                    let row_end = index.start_index_column.get_val(rank_end);

                    row_start..row_end
                }
            },
        }
    }

    pub fn select_batch_in_place(&self, doc_id_start: DocId, rank_ids: &mut Vec<RowId>) {
        match self {
            ColumnIndex::Empty { .. } => {
                rank_ids.clear();
            }
            ColumnIndex::Full => {
                // No need to do anything:
                // value_idx and row_idx are the same.
            }
            ColumnIndex::Optional(optional_index) => {
                optional_index.select_batch(&mut rank_ids[..]);
            }
            ColumnIndex::Multivalued(multivalued_index) => {
                multivalued_index.select_batch_in_place(doc_id_start, rank_ids)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{Cardinality, ColumnIndex};

    #[test]
    fn test_column_index_get_cardinality() {
        assert_eq!(
            ColumnIndex::Empty { num_docs: 0 }.get_cardinality(),
            Cardinality::Full
        );
        assert_eq!(ColumnIndex::Full.get_cardinality(), Cardinality::Full);
        assert_eq!(
            ColumnIndex::Empty { num_docs: 1 }.get_cardinality(),
            Cardinality::Optional
        );
    }
}
