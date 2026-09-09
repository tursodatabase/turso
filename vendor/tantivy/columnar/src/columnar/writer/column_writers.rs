use std::cmp::Ordering;

use stacker::{ExpUnrolledLinkedList, MemoryArena};

use crate::columnar::writer::column_operation::{ColumnOperation, SymbolValue};
use crate::dictionary::{DictionaryBuilder, UnorderedId};
use crate::{Cardinality, NumericalType, NumericalValue, RowId};

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[repr(u8)]
enum DocumentStep {
    Same = 0,
    Next = 1,
    Skipped = 2,
}

#[inline(always)]
fn delta_with_last_doc(last_doc_opt: Option<u32>, doc: u32) -> DocumentStep {
    let expected_next_doc = last_doc_opt.map(|last_doc| last_doc + 1).unwrap_or(0u32);
    match doc.cmp(&expected_next_doc) {
        Ordering::Less => DocumentStep::Same,
        Ordering::Equal => DocumentStep::Next,
        Ordering::Greater => DocumentStep::Skipped,
    }
}

#[derive(Copy, Clone, Default)]
pub struct ColumnWriter {
    // Detected cardinality of the column so far.
    cardinality: Cardinality,
    // Last document inserted.
    // None if no doc has been added yet.
    last_doc_opt: Option<u32>,
    // Buffer containing the serialized values.
    values: ExpUnrolledLinkedList,
}

impl ColumnWriter {
    /// Returns an iterator over the Symbol that have been recorded
    /// for the given column.
    pub(super) fn operation_iterator<'a, V: SymbolValue>(
        &self,
        arena: &MemoryArena,
        buffer: &'a mut Vec<u8>,
    ) -> impl Iterator<Item = ColumnOperation<V>> + 'a + use<'a, V> {
        buffer.clear();
        self.values.read_to_end(arena, buffer);
        let mut cursor: &[u8] = &buffer[..];
        std::iter::from_fn(move || ColumnOperation::deserialize(&mut cursor))
    }

    /// Records a change of the document being recorded.
    ///
    /// This function will also update the cardinality of the column
    /// if necessary.
    pub(super) fn record<S: SymbolValue>(&mut self, doc: RowId, value: S, arena: &mut MemoryArena) {
        // Difference between `doc` and the last doc.
        match delta_with_last_doc(self.last_doc_opt, doc) {
            DocumentStep::Same => {
                // This is the last encounterred document.
                self.cardinality = Cardinality::Multivalued;
            }
            DocumentStep::Next => {
                self.last_doc_opt = Some(doc);
                self.write_symbol::<S>(ColumnOperation::NewDoc(doc), arena);
            }
            DocumentStep::Skipped => {
                self.cardinality = self.cardinality.max(Cardinality::Optional);
                self.last_doc_opt = Some(doc);
                self.write_symbol::<S>(ColumnOperation::NewDoc(doc), arena);
            }
        }
        self.write_symbol(ColumnOperation::Value(value), arena);
    }

    // Get the cardinality.
    // The overall number of docs in the column is necessary to
    // deal with the case where the all docs contain 1 value, except some documents
    // at the end of the column.
    pub(crate) fn get_cardinality(&self, num_docs: RowId) -> Cardinality {
        match delta_with_last_doc(self.last_doc_opt, num_docs) {
            DocumentStep::Same | DocumentStep::Next => self.cardinality,
            DocumentStep::Skipped => self.cardinality.max(Cardinality::Optional),
        }
    }

    /// Appends a new symbol to the `ColumnWriter`.
    fn write_symbol<V: SymbolValue>(
        &mut self,
        column_operation: ColumnOperation<V>,
        arena: &mut MemoryArena,
    ) {
        self.values
            .writer(arena)
            .extend_from_slice(column_operation.serialize().as_ref());
    }
}

#[derive(Clone, Copy, Default)]
pub(crate) struct NumericalColumnWriter {
    compatible_numerical_types: CompatibleNumericalTypes,
    column_writer: ColumnWriter,
}

impl NumericalColumnWriter {
    pub fn force_numerical_type(&mut self, numerical_type: NumericalType) {
        assert!(
            self.compatible_numerical_types
                .is_type_accepted(numerical_type)
        );
        self.compatible_numerical_types = CompatibleNumericalTypes::StaticType(numerical_type);
    }
}

/// State used to store what types are still acceptable
/// after having seen a set of numerical values.
#[derive(Clone, Copy)]
pub(crate) enum CompatibleNumericalTypes {
    Dynamic {
        all_values_within_i64_range: bool,
        all_values_within_u64_range: bool,
    },
    StaticType(NumericalType),
}

impl Default for CompatibleNumericalTypes {
    fn default() -> CompatibleNumericalTypes {
        CompatibleNumericalTypes::Dynamic {
            all_values_within_i64_range: true,
            all_values_within_u64_range: true,
        }
    }
}

impl CompatibleNumericalTypes {
    pub fn is_type_accepted(&self, numerical_type: NumericalType) -> bool {
        match self {
            CompatibleNumericalTypes::Dynamic {
                all_values_within_i64_range,
                all_values_within_u64_range,
            } => match numerical_type {
                NumericalType::I64 => *all_values_within_i64_range,
                NumericalType::U64 => *all_values_within_u64_range,
                NumericalType::F64 => true,
            },
            CompatibleNumericalTypes::StaticType(static_numerical_type) => {
                *static_numerical_type == numerical_type
            }
        }
    }

    pub fn accept_value(&mut self, numerical_value: NumericalValue) {
        match self {
            CompatibleNumericalTypes::Dynamic {
                all_values_within_i64_range,
                all_values_within_u64_range,
            } => match numerical_value {
                NumericalValue::I64(val_i64) => {
                    let value_within_u64_range = val_i64 >= 0i64;
                    *all_values_within_u64_range &= value_within_u64_range;
                }
                NumericalValue::U64(val_u64) => {
                    let value_within_i64_range = val_u64 < i64::MAX as u64;
                    *all_values_within_i64_range &= value_within_i64_range;
                }
                NumericalValue::F64(_) => {
                    *all_values_within_i64_range = false;
                    *all_values_within_u64_range = false;
                }
            },
            CompatibleNumericalTypes::StaticType(typ) => {
                assert_eq!(
                    numerical_value.numerical_type(),
                    *typ,
                    "Input type forbidden. This column has been forced to type {typ:?}, received \
                     {numerical_value:?}"
                );
            }
        }
    }

    pub fn to_numerical_type(self) -> NumericalType {
        for numerical_type in [NumericalType::I64, NumericalType::U64] {
            if self.is_type_accepted(numerical_type) {
                return numerical_type;
            }
        }
        NumericalType::F64
    }
}

impl NumericalColumnWriter {
    pub fn numerical_type(&self) -> NumericalType {
        self.compatible_numerical_types.to_numerical_type()
    }

    pub fn cardinality(&self, num_docs: RowId) -> Cardinality {
        self.column_writer.get_cardinality(num_docs)
    }

    pub fn record_numerical_value(
        &mut self,
        doc: RowId,
        value: NumericalValue,
        arena: &mut MemoryArena,
    ) {
        self.compatible_numerical_types.accept_value(value);
        self.column_writer.record(doc, value, arena);
    }

    pub(super) fn operation_iterator<'a>(
        self,
        arena: &MemoryArena,
        buffer: &'a mut Vec<u8>,
    ) -> impl Iterator<Item = ColumnOperation<NumericalValue>> + 'a + use<'a> {
        self.column_writer.operation_iterator(arena, buffer)
    }
}

#[derive(Copy, Clone)]
pub(crate) struct StrOrBytesColumnWriter {
    pub(crate) dictionary_id: u32,
    pub(crate) column_writer: ColumnWriter,
    // If true, when facing a multivalued cardinality,
    // values associated to a given document will be sorted.
    //
    // This is useful for facets.
    //
    // If false, the order of appearance in the document will be
    // observed.
    pub(crate) sort_values_within_row: bool,
}

impl StrOrBytesColumnWriter {
    pub(crate) fn with_dictionary_id(dictionary_id: u32) -> StrOrBytesColumnWriter {
        StrOrBytesColumnWriter {
            dictionary_id,
            column_writer: Default::default(),
            sort_values_within_row: false,
        }
    }

    pub(crate) fn record_bytes(
        &mut self,
        doc: RowId,
        bytes: &[u8],
        dictionaries: &mut [DictionaryBuilder],
        arena: &mut MemoryArena,
    ) {
        let unordered_id =
            dictionaries[self.dictionary_id as usize].get_or_allocate_id(bytes, arena);
        self.column_writer.record(doc, unordered_id, arena);
    }

    pub(super) fn operation_iterator<'a>(
        &self,
        arena: &MemoryArena,
        byte_buffer: &'a mut Vec<u8>,
    ) -> impl Iterator<Item = ColumnOperation<UnorderedId>> + 'a + use<'a> {
        self.column_writer.operation_iterator(arena, byte_buffer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delta_with_last_doc() {
        assert_eq!(delta_with_last_doc(None, 0u32), DocumentStep::Next);
        assert_eq!(delta_with_last_doc(None, 1u32), DocumentStep::Skipped);
        assert_eq!(delta_with_last_doc(None, 2u32), DocumentStep::Skipped);
        assert_eq!(delta_with_last_doc(Some(0u32), 0u32), DocumentStep::Same);
        assert_eq!(delta_with_last_doc(Some(1u32), 1u32), DocumentStep::Same);
        assert_eq!(delta_with_last_doc(Some(1u32), 2u32), DocumentStep::Next);
        assert_eq!(delta_with_last_doc(Some(1u32), 3u32), DocumentStep::Skipped);
        assert_eq!(delta_with_last_doc(Some(1u32), 4u32), DocumentStep::Skipped);
    }

    #[track_caller]
    fn test_column_writer_coercion_iter_aux(
        values: impl Iterator<Item = NumericalValue>,
        expected_numerical_type: NumericalType,
    ) {
        let mut compatible_numerical_types = CompatibleNumericalTypes::default();
        for value in values {
            compatible_numerical_types.accept_value(value);
        }
        assert_eq!(
            compatible_numerical_types.to_numerical_type(),
            expected_numerical_type
        );
    }

    #[track_caller]
    fn test_column_writer_coercion_aux(
        values: &[NumericalValue],
        expected_numerical_type: NumericalType,
    ) {
        test_column_writer_coercion_iter_aux(values.iter().copied(), expected_numerical_type);
        test_column_writer_coercion_iter_aux(values.iter().rev().copied(), expected_numerical_type);
    }

    #[test]
    fn test_column_writer_coercion() {
        test_column_writer_coercion_aux(&[], NumericalType::I64);
        test_column_writer_coercion_aux(&[1i64.into()], NumericalType::I64);
        test_column_writer_coercion_aux(&[1u64.into()], NumericalType::I64);
        // We don't detect exact integer at the moment. We could!
        test_column_writer_coercion_aux(&[1f64.into()], NumericalType::F64);
        test_column_writer_coercion_aux(&[u64::MAX.into()], NumericalType::U64);
        test_column_writer_coercion_aux(&[(i64::MAX as u64).into()], NumericalType::U64);
        test_column_writer_coercion_aux(&[(1u64 << 63).into()], NumericalType::U64);
        test_column_writer_coercion_aux(&[1i64.into(), 1u64.into()], NumericalType::I64);
        test_column_writer_coercion_aux(&[u64::MAX.into(), (-1i64).into()], NumericalType::F64);
    }

    #[test]
    #[should_panic]
    fn test_compatible_numerical_types_static_incompatible_type() {
        let mut compatible_numerical_types =
            CompatibleNumericalTypes::StaticType(NumericalType::U64);
        compatible_numerical_types.accept_value(NumericalValue::I64(1i64));
    }

    #[test]
    fn test_compatible_numerical_types_static_different_type_forbidden() {
        let mut compatible_numerical_types =
            CompatibleNumericalTypes::StaticType(NumericalType::U64);
        compatible_numerical_types.accept_value(NumericalValue::U64(u64::MAX));
    }

    #[test]
    fn test_compatible_numerical_types_static() {
        for typ in [NumericalType::I64, NumericalType::I64, NumericalType::F64] {
            let compatible_numerical_types = CompatibleNumericalTypes::StaticType(typ);
            assert_eq!(compatible_numerical_types.to_numerical_type(), typ);
        }
    }
}
