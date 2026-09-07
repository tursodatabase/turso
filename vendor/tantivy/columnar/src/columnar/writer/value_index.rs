use crate::RowId;
use crate::column_index::{SerializableMultivalueIndex, SerializableOptionalIndex};
use crate::iterable::Iterable;

/// The `IndexBuilder` interprets a sequence of
/// calls of the form:
/// (record_doc,record_value+)*
/// and can then serialize the results into an index to associate docids with their value[s].
///
/// It has different implementation depending on whether the
/// cardinality is required, optional, or multivalued.
pub(crate) trait IndexBuilder {
    fn record_row(&mut self, doc: RowId);
    #[inline]
    fn record_value(&mut self) {}
}

/// The FullIndexBuilder does nothing.
#[derive(Default)]
pub struct FullIndexBuilder;

impl IndexBuilder for FullIndexBuilder {
    #[inline(always)]
    fn record_row(&mut self, _doc: RowId) {}
}

#[derive(Default)]
pub struct OptionalIndexBuilder {
    docs: Vec<RowId>,
}

impl OptionalIndexBuilder {
    pub fn finish(&mut self, num_rows: RowId) -> impl Iterable<RowId> + '_ {
        debug_assert!(
            self.docs
                .last()
                .copied()
                .map(|last_doc| last_doc < num_rows)
                .unwrap_or(true)
        );
        &self.docs[..]
    }

    fn reset(&mut self) {
        self.docs.clear();
    }
}

impl IndexBuilder for OptionalIndexBuilder {
    #[inline(always)]
    fn record_row(&mut self, doc: RowId) {
        debug_assert!(
            self.docs
                .last()
                .copied()
                .map(|prev_doc| doc > prev_doc)
                .unwrap_or(true)
        );
        self.docs.push(doc);
    }
}

#[derive(Default)]
pub struct MultivaluedIndexBuilder {
    doc_with_values: Vec<RowId>,
    start_offsets: Vec<u32>,
    total_num_vals_seen: u32,
    current_row: RowId,
    current_row_has_value: bool,
}

impl MultivaluedIndexBuilder {
    pub fn finish(&mut self, num_docs: RowId) -> SerializableMultivalueIndex<'_> {
        self.start_offsets.push(self.total_num_vals_seen);
        let non_null_row_ids: Box<dyn Iterable<RowId>> = Box::new(&self.doc_with_values[..]);
        SerializableMultivalueIndex {
            doc_ids_with_values: SerializableOptionalIndex {
                non_null_row_ids,
                num_rows: num_docs,
            },
            start_offsets: Box::new(&self.start_offsets[..]),
        }
    }

    fn reset(&mut self) {
        self.doc_with_values.clear();
        self.start_offsets.clear();
        self.total_num_vals_seen = 0;
        self.current_row = 0;
        self.current_row_has_value = false;
    }
}

impl IndexBuilder for MultivaluedIndexBuilder {
    fn record_row(&mut self, row_id: RowId) {
        self.current_row = row_id;
        self.current_row_has_value = false;
    }

    fn record_value(&mut self) {
        if !self.current_row_has_value {
            self.current_row_has_value = true;
            self.doc_with_values.push(self.current_row);
            self.start_offsets.push(self.total_num_vals_seen);
        }
        self.total_num_vals_seen += 1;
    }
}

/// The `SpareIndexBuilders` is there to avoid allocating a
/// new index builder for every single column.
#[derive(Default)]
pub struct PreallocatedIndexBuilders {
    required_index_builder: FullIndexBuilder,
    optional_index_builder: OptionalIndexBuilder,
    multivalued_index_builder: MultivaluedIndexBuilder,
}

impl PreallocatedIndexBuilders {
    pub fn borrow_required_index_builder(&mut self) -> &mut FullIndexBuilder {
        &mut self.required_index_builder
    }

    pub fn borrow_optional_index_builder(&mut self) -> &mut OptionalIndexBuilder {
        self.optional_index_builder.reset();
        &mut self.optional_index_builder
    }

    pub fn borrow_multivalued_index_builder(&mut self) -> &mut MultivaluedIndexBuilder {
        self.multivalued_index_builder.reset();
        &mut self.multivalued_index_builder
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_optional_value_index_builder() {
        let mut opt_value_index_builder = OptionalIndexBuilder::default();
        opt_value_index_builder.record_row(0u32);
        opt_value_index_builder.record_value();
        assert_eq!(
            &opt_value_index_builder
                .finish(1u32)
                .boxed_iter()
                .collect::<Vec<u32>>(),
            &[0]
        );
        opt_value_index_builder.reset();
        opt_value_index_builder.record_row(1u32);
        opt_value_index_builder.record_value();
        assert_eq!(
            &opt_value_index_builder
                .finish(2u32)
                .boxed_iter()
                .collect::<Vec<u32>>(),
            &[1]
        );
    }

    #[test]
    fn test_multivalued_value_index_builder_simple() {
        let mut multivalued_value_index_builder = MultivaluedIndexBuilder::default();
        {
            multivalued_value_index_builder.record_row(0u32);
            multivalued_value_index_builder.record_value();
            multivalued_value_index_builder.record_value();
            let serialized_multivalue_index = multivalued_value_index_builder.finish(1u32);
            let start_offsets: Vec<u32> = serialized_multivalue_index
                .start_offsets
                .boxed_iter()
                .collect();
            assert_eq!(&start_offsets, &[0, 2]);
        }
        multivalued_value_index_builder.reset();
        multivalued_value_index_builder.record_row(0u32);
        multivalued_value_index_builder.record_value();
        multivalued_value_index_builder.record_value();
        let serialized_multivalue_index = multivalued_value_index_builder.finish(1u32);
        let start_offsets: Vec<u32> = serialized_multivalue_index
            .start_offsets
            .boxed_iter()
            .collect();
        assert_eq!(&start_offsets, &[0, 2]);
    }

    #[test]
    fn test_multivalued_value_index_builder() {
        let mut multivalued_value_index_builder = MultivaluedIndexBuilder::default();
        multivalued_value_index_builder.record_row(1u32);
        multivalued_value_index_builder.record_value();
        multivalued_value_index_builder.record_value();
        multivalued_value_index_builder.record_row(2u32);
        multivalued_value_index_builder.record_value();
        let SerializableMultivalueIndex {
            doc_ids_with_values,
            start_offsets,
        } = multivalued_value_index_builder.finish(4u32);
        assert_eq!(doc_ids_with_values.num_rows, 4u32);
        let doc_ids_with_values: Vec<u32> =
            doc_ids_with_values.non_null_row_ids.boxed_iter().collect();
        assert_eq!(&doc_ids_with_values, &[1u32, 2u32]);
        let start_offsets: Vec<u32> = start_offsets.boxed_iter().collect();
        assert_eq!(&start_offsets[..], &[0, 2, 3]);
    }
}
