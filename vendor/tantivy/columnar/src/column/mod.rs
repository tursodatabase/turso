mod dictionary_encoded;
mod serialize;

use std::fmt::{self, Debug};
use std::io::Write;
use std::ops::{Range, RangeInclusive};
use std::sync::Arc;

use common::BinarySerializable;
pub use dictionary_encoded::{BytesColumn, StrColumn};
pub use serialize::{
    open_column_bytes, open_column_str, open_column_u64, open_column_u128,
    open_column_u128_as_compact_u64, serialize_column_mappable_to_u64,
    serialize_column_mappable_to_u128,
};

use crate::column_index::{ColumnIndex, Set};
use crate::column_values::monotonic_mapping::StrictlyMonotonicMappingToInternal;
use crate::column_values::{ColumnValues, monotonic_map_column};
use crate::{Cardinality, DocId, EmptyColumnValues, MonotonicallyMappableToU64, RowId};

#[derive(Clone)]
pub struct Column<T = u64> {
    pub index: ColumnIndex,
    pub values: Arc<dyn ColumnValues<T>>,
}

impl<T: Debug + PartialOrd + Send + Sync + Copy + 'static> Debug for Column<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let num_docs = self.num_docs();
        let entries = (0..num_docs)
            .map(|i| (i, self.values_for_doc(i).collect::<Vec<_>>()))
            .filter(|(_, vals)| !vals.is_empty());
        f.debug_map().entries(entries).finish()
    }
}

impl<T: PartialOrd + Default> Column<T> {
    pub fn build_empty_column(num_docs: u32) -> Column<T> {
        Column {
            index: ColumnIndex::Empty { num_docs },
            values: Arc::new(EmptyColumnValues),
        }
    }
}

impl<T: MonotonicallyMappableToU64> Column<T> {
    pub fn to_u64_monotonic(self) -> Column<u64> {
        let values = Arc::new(monotonic_map_column(
            self.values,
            StrictlyMonotonicMappingToInternal::<T>::new(),
        ));
        Column {
            index: self.index,
            values,
        }
    }
}

impl<T: PartialOrd + Copy + Debug + Send + Sync + 'static> Column<T> {
    #[inline]
    pub fn get_cardinality(&self) -> Cardinality {
        self.index.get_cardinality()
    }

    pub fn num_docs(&self) -> RowId {
        match &self.index {
            ColumnIndex::Empty { num_docs } => *num_docs,
            ColumnIndex::Full => self.values.num_vals(),
            ColumnIndex::Optional(optional_index) => optional_index.num_docs(),
            ColumnIndex::Multivalued(col_index) => {
                // The multivalued index contains all value start row_id,
                // and one extra value at the end with the overall number of rows.
                col_index.num_docs()
            }
        }
    }

    pub fn min_value(&self) -> T {
        self.values.min_value()
    }

    pub fn max_value(&self) -> T {
        self.values.max_value()
    }

    #[inline]
    pub fn first(&self, doc_id: DocId) -> Option<T> {
        self.values_for_doc(doc_id).next()
    }

    /// Load the first value for each docid in the provided slice.
    #[inline]
    pub fn first_vals(&self, docids: &[DocId], output: &mut [Option<T>]) {
        match &self.index {
            ColumnIndex::Empty { .. } => {}
            ColumnIndex::Full => self.values.get_vals_opt(docids, output),
            ColumnIndex::Optional(optional_index) => {
                for (i, docid) in docids.iter().enumerate() {
                    output[i] = optional_index
                        .rank_if_exists(*docid)
                        .map(|rowid| self.values.get_val(rowid));
                }
            }
            ColumnIndex::Multivalued(multivalued_index) => {
                for (i, docid) in docids.iter().enumerate() {
                    let range = multivalued_index.range(*docid);
                    let is_empty = range.start == range.end;
                    if !is_empty {
                        output[i] = Some(self.values.get_val(range.start));
                    }
                }
            }
        }
    }

    /// Translates a block of docids to row_ids.
    ///
    /// returns the row_ids and the matching docids on the same index
    /// e.g.
    /// DocId In:  [0, 5, 6]
    /// DocId Out: [0, 0, 6, 6]
    /// RowId Out: [0, 1, 2, 3]
    #[inline]
    pub fn row_ids_for_docs(
        &self,
        doc_ids: &[DocId],
        doc_ids_out: &mut Vec<DocId>,
        row_ids: &mut Vec<RowId>,
    ) {
        self.index.docids_to_rowids(doc_ids, doc_ids_out, row_ids)
    }

    /// Get an iterator over the values for the provided docid.
    #[inline]
    pub fn values_for_doc(&self, doc_id: DocId) -> impl Iterator<Item = T> + '_ {
        self.index
            .value_row_ids(doc_id)
            .map(|value_row_id: RowId| self.values.get_val(value_row_id))
    }

    /// Get the docids of values which are in the provided value and docid range.
    #[inline]
    pub fn get_docids_for_value_range(
        &self,
        value_range: RangeInclusive<T>,
        selected_docid_range: Range<u32>,
        doc_ids: &mut Vec<u32>,
    ) {
        // convert passed docid range to row id range
        let rowid_range = self
            .index
            .docid_range_to_rowids(selected_docid_range.clone());

        // Load rows
        self.values
            .get_row_ids_for_value_range(value_range, rowid_range, doc_ids);
        // Convert rows to docids
        self.index
            .select_batch_in_place(selected_docid_range.start, doc_ids);
    }

    pub fn first_or_default_col(self, default_value: T) -> Arc<dyn ColumnValues<T>> {
        Arc::new(FirstValueWithDefault {
            column: self,
            default_value,
        })
    }
}

impl BinarySerializable for Cardinality {
    fn serialize<W: Write + ?Sized>(&self, writer: &mut W) -> std::io::Result<()> {
        self.to_code().serialize(writer)
    }

    fn deserialize<R: std::io::Read>(reader: &mut R) -> std::io::Result<Self> {
        let cardinality_code = u8::deserialize(reader)?;
        let cardinality = Cardinality::try_from_code(cardinality_code)?;
        Ok(cardinality)
    }
}

// TODO simplify or optimize
struct FirstValueWithDefault<T: Copy> {
    column: Column<T>,
    default_value: T,
}

impl<T: PartialOrd + Debug + Send + Sync + Copy + 'static> ColumnValues<T>
    for FirstValueWithDefault<T>
{
    #[inline(always)]
    fn get_val(&self, idx: u32) -> T {
        self.column.first(idx).unwrap_or(self.default_value)
    }

    fn min_value(&self) -> T {
        self.column.values.min_value()
    }

    fn max_value(&self) -> T {
        self.column.values.max_value()
    }

    fn num_vals(&self) -> u32 {
        match &self.column.index {
            ColumnIndex::Empty { .. } => 0u32,
            ColumnIndex::Full => self.column.values.num_vals(),
            ColumnIndex::Optional(optional_idx) => optional_idx.num_docs(),
            ColumnIndex::Multivalued(multivalue_idx) => multivalue_idx.num_docs(),
        }
    }
}
