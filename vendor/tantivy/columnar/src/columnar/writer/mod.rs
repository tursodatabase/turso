mod column_operation;
mod column_writers;
mod serializer;
mod value_index;

use std::io;
use std::net::Ipv6Addr;

use column_operation::ColumnOperation;
pub(crate) use column_writers::CompatibleNumericalTypes;
use common::CountingWriter;
use common::json_path_writer::JSON_END_OF_PATH;
pub(crate) use serializer::ColumnarSerializer;
use stacker::{Addr, ArenaHashMap, MemoryArena};

use crate::column_index::{SerializableColumnIndex, SerializableOptionalIndex};
use crate::column_values::{MonotonicallyMappableToU64, MonotonicallyMappableToU128};
use crate::columnar::column_type::ColumnType;
use crate::columnar::writer::column_writers::{
    ColumnWriter, NumericalColumnWriter, StrOrBytesColumnWriter,
};
use crate::columnar::writer::value_index::{IndexBuilder, PreallocatedIndexBuilders};
use crate::dictionary::{DictionaryBuilder, TermIdMapping, UnorderedId};
use crate::value::{Coerce, NumericalType, NumericalValue};
use crate::{Cardinality, RowId};

/// This is a set of buffers that are used to temporarily write the values into before passing them
/// to the fast field codecs.
#[derive(Default)]
struct SpareBuffers {
    value_index_builders: PreallocatedIndexBuilders,
    u64_values: Vec<u64>,
    ip_addr_values: Vec<Ipv6Addr>,
}

/// Makes it possible to create a new columnar.
///
/// ```rust
/// use tantivy_columnar::ColumnarWriter;
///
/// let mut columnar_writer = ColumnarWriter::default();
/// columnar_writer.record_str(0u32 /* doc id */, "product_name", "Red backpack");
/// columnar_writer.record_numerical(0u32 /* doc id */, "price", 10u64);
/// columnar_writer.record_str(1u32 /* doc id */, "product_name", "Apple");
/// columnar_writer.record_numerical(0u32 /* doc id */, "price", 10.5f64); //< uh oh we ended up mixing integer and floats.
/// let mut wrt: Vec<u8> =  Vec::new();
/// columnar_writer.serialize(2u32, &mut wrt).unwrap();
/// ```
#[derive(Default)]
pub struct ColumnarWriter {
    numerical_field_hash_map: ArenaHashMap,
    datetime_field_hash_map: ArenaHashMap,
    bool_field_hash_map: ArenaHashMap,
    ip_addr_field_hash_map: ArenaHashMap,
    bytes_field_hash_map: ArenaHashMap,
    str_field_hash_map: ArenaHashMap,
    arena: MemoryArena,
    // Dictionaries used to store dictionary-encoded values.
    dictionaries: Vec<DictionaryBuilder>,
    buffers: SpareBuffers,
}

impl ColumnarWriter {
    pub fn mem_usage(&self) -> usize {
        self.arena.mem_usage()
            + self.numerical_field_hash_map.mem_usage()
            + self.bool_field_hash_map.mem_usage()
            + self.bytes_field_hash_map.mem_usage()
            + self.str_field_hash_map.mem_usage()
            + self.ip_addr_field_hash_map.mem_usage()
            + self.datetime_field_hash_map.mem_usage()
            + self
                .dictionaries
                .iter()
                .map(|dict| dict.mem_usage())
                .sum::<usize>()
    }

    /// Records a column type. This is useful to bypass the coercion process,
    /// makes sure the empty is present in the resulting columnar, or set
    /// the `sort_values_within_row`.
    ///
    /// `sort_values_within_row` is only allowed for `Bytes` or `Str` columns.
    pub fn record_column_type(
        &mut self,
        column_name: &str,
        column_type: ColumnType,
        sort_values_within_row: bool,
    ) {
        if sort_values_within_row {
            assert!(
                column_type == ColumnType::Bytes || column_type == ColumnType::Str,
                "sort_values_within_row is only allowed for Bytes and Str columns",
            );
        }
        match column_type {
            ColumnType::Str | ColumnType::Bytes => {
                let (hash_map, dictionaries) = (
                    if column_type == ColumnType::Str {
                        &mut self.str_field_hash_map
                    } else {
                        &mut self.bytes_field_hash_map
                    },
                    &mut self.dictionaries,
                );
                hash_map.mutate_or_create(
                    column_name.as_bytes(),
                    |column_opt: Option<StrOrBytesColumnWriter>| {
                        let mut column_writer = if let Some(column_writer) = column_opt {
                            column_writer
                        } else {
                            let dictionary_id = dictionaries.len() as u32;
                            dictionaries.push(DictionaryBuilder::default());
                            StrOrBytesColumnWriter::with_dictionary_id(dictionary_id)
                        };
                        column_writer.sort_values_within_row = sort_values_within_row;
                        column_writer
                    },
                );
            }
            ColumnType::Bool => {
                self.bool_field_hash_map.mutate_or_create(
                    column_name.as_bytes(),
                    |column_opt: Option<ColumnWriter>| column_opt.unwrap_or_default(),
                );
            }
            ColumnType::DateTime => {
                self.datetime_field_hash_map.mutate_or_create(
                    column_name.as_bytes(),
                    |column_opt: Option<ColumnWriter>| column_opt.unwrap_or_default(),
                );
            }
            ColumnType::I64 | ColumnType::F64 | ColumnType::U64 => {
                let numerical_type = column_type.numerical_type().unwrap();
                self.numerical_field_hash_map.mutate_or_create(
                    column_name.as_bytes(),
                    |column_opt: Option<NumericalColumnWriter>| {
                        let mut column: NumericalColumnWriter = column_opt.unwrap_or_default();
                        column.force_numerical_type(numerical_type);
                        column
                    },
                );
            }
            ColumnType::IpAddr => self.ip_addr_field_hash_map.mutate_or_create(
                column_name.as_bytes(),
                |column_opt: Option<ColumnWriter>| column_opt.unwrap_or_default(),
            ),
        }
    }

    pub fn record_numerical<T: Into<NumericalValue> + Copy>(
        &mut self,
        doc: RowId,
        column_name: &str,
        numerical_value: T,
    ) {
        let (hash_map, arena) = (&mut self.numerical_field_hash_map, &mut self.arena);
        hash_map.mutate_or_create(
            column_name.as_bytes(),
            |column_opt: Option<NumericalColumnWriter>| {
                let mut column: NumericalColumnWriter = column_opt.unwrap_or_default();
                column.record_numerical_value(doc, numerical_value.into(), arena);
                column
            },
        );
    }

    pub fn record_ip_addr(&mut self, doc: RowId, column_name: &str, ip_addr: Ipv6Addr) {
        let (hash_map, arena) = (&mut self.ip_addr_field_hash_map, &mut self.arena);
        hash_map.mutate_or_create(
            column_name.as_bytes(),
            |column_opt: Option<ColumnWriter>| {
                let mut column: ColumnWriter = column_opt.unwrap_or_default();
                column.record(doc, ip_addr, arena);
                column
            },
        );
    }

    pub fn record_bool(&mut self, doc: RowId, column_name: &str, val: bool) {
        let (hash_map, arena) = (&mut self.bool_field_hash_map, &mut self.arena);
        hash_map.mutate_or_create(
            column_name.as_bytes(),
            |column_opt: Option<ColumnWriter>| {
                let mut column: ColumnWriter = column_opt.unwrap_or_default();
                column.record(doc, val, arena);
                column
            },
        );
    }

    pub fn record_datetime(&mut self, doc: RowId, column_name: &str, datetime: common::DateTime) {
        let (hash_map, arena) = (&mut self.datetime_field_hash_map, &mut self.arena);
        hash_map.mutate_or_create(
            column_name.as_bytes(),
            |column_opt: Option<ColumnWriter>| {
                let mut column: ColumnWriter = column_opt.unwrap_or_default();
                column.record(
                    doc,
                    NumericalValue::I64(datetime.into_timestamp_nanos()),
                    arena,
                );
                column
            },
        );
    }

    pub fn record_str(&mut self, doc: RowId, column_name: &str, value: &str) {
        let (hash_map, arena, dictionaries) = (
            &mut self.str_field_hash_map,
            &mut self.arena,
            &mut self.dictionaries,
        );
        hash_map.mutate_or_create(
            column_name.as_bytes(),
            |column_opt: Option<StrOrBytesColumnWriter>| {
                let mut column: StrOrBytesColumnWriter = column_opt.unwrap_or_else(|| {
                    // Each column has its own dictionary
                    let dictionary_id = dictionaries.len() as u32;
                    dictionaries.push(DictionaryBuilder::default());
                    StrOrBytesColumnWriter::with_dictionary_id(dictionary_id)
                });
                column.record_bytes(doc, value.as_bytes(), dictionaries, arena);
                column
            },
        );
    }

    pub fn record_bytes(&mut self, doc: RowId, column_name: &str, value: &[u8]) {
        let (hash_map, arena, dictionaries) = (
            &mut self.bytes_field_hash_map,
            &mut self.arena,
            &mut self.dictionaries,
        );
        hash_map.mutate_or_create(
            column_name.as_bytes(),
            |column_opt: Option<StrOrBytesColumnWriter>| {
                let mut column: StrOrBytesColumnWriter = column_opt.unwrap_or_else(|| {
                    // Each column has its own dictionary
                    let dictionary_id = dictionaries.len() as u32;
                    dictionaries.push(DictionaryBuilder::default());
                    StrOrBytesColumnWriter::with_dictionary_id(dictionary_id)
                });
                column.record_bytes(doc, value, dictionaries, arena);
                column
            },
        );
    }
    pub fn serialize(&mut self, num_docs: RowId, wrt: &mut dyn io::Write) -> io::Result<()> {
        let mut serializer = ColumnarSerializer::new(wrt);

        let mut columns: Vec<(&[u8], ColumnType, Addr)> = self
            .numerical_field_hash_map
            .iter()
            .map(|(column_name, addr)| {
                let numerical_column_writer: NumericalColumnWriter =
                    self.numerical_field_hash_map.read(addr);
                let column_type = numerical_column_writer.numerical_type().into();
                (column_name, column_type, addr)
            })
            .collect();
        columns.extend(
            self.bytes_field_hash_map
                .iter()
                .map(|(column_name, addr)| (column_name, ColumnType::Bytes, addr)),
        );
        columns.extend(
            self.str_field_hash_map
                .iter()
                .map(|(column_name, addr)| (column_name, ColumnType::Str, addr)),
        );
        columns.extend(
            self.bool_field_hash_map
                .iter()
                .map(|(column_name, addr)| (column_name, ColumnType::Bool, addr)),
        );
        columns.extend(
            self.ip_addr_field_hash_map
                .iter()
                .map(|(column_name, addr)| (column_name, ColumnType::IpAddr, addr)),
        );
        columns.extend(
            self.datetime_field_hash_map
                .iter()
                .map(|(column_name, addr)| (column_name, ColumnType::DateTime, addr)),
        );
        columns.sort_unstable_by_key(|(column_name, col_type, _)| (*column_name, *col_type));
        let (arena, buffers, dictionaries) = (&self.arena, &mut self.buffers, &self.dictionaries);
        let mut symbol_byte_buffer: Vec<u8> = Vec::new();
        for (column_name, column_type, addr) in columns {
            if column_name.contains(&JSON_END_OF_PATH) {
                // Tantivy uses b'0' as a separator for nested fields in JSON.
                // Column names with a b'0' are not simply ignored by the columnar (and the inverted
                // index).
                continue;
            }
            match column_type {
                ColumnType::Bool => {
                    let column_writer: ColumnWriter = self.bool_field_hash_map.read(addr);
                    let cardinality = column_writer.get_cardinality(num_docs);
                    let mut column_serializer =
                        serializer.start_serialize_column(column_name, column_type);
                    serialize_bool_column(
                        cardinality,
                        num_docs,
                        column_writer.operation_iterator(arena, &mut symbol_byte_buffer),
                        buffers,
                        &mut column_serializer,
                    )?;
                    column_serializer.finalize()?;
                }
                ColumnType::IpAddr => {
                    let column_writer: ColumnWriter = self.ip_addr_field_hash_map.read(addr);
                    let cardinality = column_writer.get_cardinality(num_docs);
                    let mut column_serializer =
                        serializer.start_serialize_column(column_name, ColumnType::IpAddr);
                    serialize_ip_addr_column(
                        cardinality,
                        num_docs,
                        column_writer.operation_iterator(arena, &mut symbol_byte_buffer),
                        buffers,
                        &mut column_serializer,
                    )?;
                    column_serializer.finalize()?;
                }
                ColumnType::Bytes | ColumnType::Str => {
                    let str_or_bytes_column_writer: StrOrBytesColumnWriter =
                        if column_type == ColumnType::Bytes {
                            self.bytes_field_hash_map.read(addr)
                        } else {
                            self.str_field_hash_map.read(addr)
                        };
                    let dictionary_builder =
                        &dictionaries[str_or_bytes_column_writer.dictionary_id as usize];
                    let cardinality = str_or_bytes_column_writer
                        .column_writer
                        .get_cardinality(num_docs);
                    let mut column_serializer =
                        serializer.start_serialize_column(column_name, column_type);
                    serialize_bytes_or_str_column(
                        cardinality,
                        num_docs,
                        str_or_bytes_column_writer.sort_values_within_row,
                        dictionary_builder,
                        str_or_bytes_column_writer
                            .operation_iterator(arena, &mut symbol_byte_buffer),
                        buffers,
                        &self.arena,
                        &mut column_serializer,
                    )?;
                    column_serializer.finalize()?;
                }
                ColumnType::F64 | ColumnType::I64 | ColumnType::U64 => {
                    let numerical_column_writer: NumericalColumnWriter =
                        self.numerical_field_hash_map.read(addr);
                    let cardinality = numerical_column_writer.cardinality(num_docs);
                    let mut column_serializer =
                        serializer.start_serialize_column(column_name, column_type);
                    let numerical_type = column_type.numerical_type().unwrap();
                    serialize_numerical_column(
                        cardinality,
                        num_docs,
                        numerical_type,
                        numerical_column_writer.operation_iterator(arena, &mut symbol_byte_buffer),
                        buffers,
                        &mut column_serializer,
                    )?;
                    column_serializer.finalize()?;
                }
                ColumnType::DateTime => {
                    let column_writer: ColumnWriter = self.datetime_field_hash_map.read(addr);
                    let cardinality = column_writer.get_cardinality(num_docs);
                    let mut column_serializer =
                        serializer.start_serialize_column(column_name, ColumnType::DateTime);
                    serialize_numerical_column(
                        cardinality,
                        num_docs,
                        NumericalType::I64,
                        column_writer.operation_iterator(arena, &mut symbol_byte_buffer),
                        buffers,
                        &mut column_serializer,
                    )?;
                    column_serializer.finalize()?;
                }
            };
        }
        serializer.finalize(num_docs)?;
        Ok(())
    }
}

// Serialize [Dictionary, Column, dictionary num bytes U32::LE]
// Column: [Column Index, Column Values, column index num bytes U32::LE]
#[expect(clippy::too_many_arguments)]
fn serialize_bytes_or_str_column(
    cardinality: Cardinality,
    num_docs: RowId,
    sort_values_within_row: bool,
    dictionary_builder: &DictionaryBuilder,
    operation_it: impl Iterator<Item = ColumnOperation<UnorderedId>>,
    buffers: &mut SpareBuffers,
    arena: &MemoryArena,
    wrt: impl io::Write,
) -> io::Result<()> {
    let SpareBuffers {
        value_index_builders,
        u64_values,
        ..
    } = buffers;
    let mut counting_writer = CountingWriter::wrap(wrt);
    let term_id_mapping: TermIdMapping =
        dictionary_builder.serialize(arena, &mut counting_writer)?;
    let dictionary_num_bytes: u32 = counting_writer.written_bytes() as u32;
    let mut wrt = counting_writer.finish();
    let operation_iterator = operation_it.map(|symbol: ColumnOperation<UnorderedId>| {
        // We map unordered ids to ordered ids.
        match symbol {
            ColumnOperation::Value(unordered_id) => {
                let ordered_id = term_id_mapping.to_ord(unordered_id);
                ColumnOperation::Value(ordered_id.0 as u64)
            }
            ColumnOperation::NewDoc(doc) => ColumnOperation::NewDoc(doc),
        }
    });
    send_to_serialize_column_mappable_to_u64(
        operation_iterator,
        cardinality,
        num_docs,
        sort_values_within_row,
        value_index_builders,
        u64_values,
        &mut wrt,
    )?;
    wrt.write_all(&dictionary_num_bytes.to_le_bytes()[..])?;
    Ok(())
}

fn serialize_numerical_column(
    cardinality: Cardinality,
    num_docs: RowId,
    numerical_type: NumericalType,
    op_iterator: impl Iterator<Item = ColumnOperation<NumericalValue>>,
    buffers: &mut SpareBuffers,
    wrt: &mut impl io::Write,
) -> io::Result<()> {
    let SpareBuffers {
        value_index_builders,
        u64_values,
        ..
    } = buffers;
    match numerical_type {
        NumericalType::I64 => {
            send_to_serialize_column_mappable_to_u64(
                coerce_numerical_symbol::<i64>(op_iterator),
                cardinality,
                num_docs,
                false,
                value_index_builders,
                u64_values,
                wrt,
            )?;
        }
        NumericalType::U64 => {
            send_to_serialize_column_mappable_to_u64(
                coerce_numerical_symbol::<u64>(op_iterator),
                cardinality,
                num_docs,
                false,
                value_index_builders,
                u64_values,
                wrt,
            )?;
        }
        NumericalType::F64 => {
            send_to_serialize_column_mappable_to_u64(
                coerce_numerical_symbol::<f64>(op_iterator),
                cardinality,
                num_docs,
                false,
                value_index_builders,
                u64_values,
                wrt,
            )?;
        }
    };
    Ok(())
}

fn serialize_bool_column(
    cardinality: Cardinality,
    num_docs: RowId,
    column_operations_it: impl Iterator<Item = ColumnOperation<bool>>,
    buffers: &mut SpareBuffers,
    wrt: &mut impl io::Write,
) -> io::Result<()> {
    let SpareBuffers {
        value_index_builders,
        u64_values,
        ..
    } = buffers;
    send_to_serialize_column_mappable_to_u64(
        column_operations_it.map(|bool_column_operation| match bool_column_operation {
            ColumnOperation::NewDoc(doc) => ColumnOperation::NewDoc(doc),
            ColumnOperation::Value(bool_val) => ColumnOperation::Value(bool_val.to_u64()),
        }),
        cardinality,
        num_docs,
        false,
        value_index_builders,
        u64_values,
        wrt,
    )?;
    Ok(())
}

fn serialize_ip_addr_column(
    cardinality: Cardinality,
    num_docs: RowId,
    column_operations_it: impl Iterator<Item = ColumnOperation<Ipv6Addr>>,
    buffers: &mut SpareBuffers,
    wrt: &mut impl io::Write,
) -> io::Result<()> {
    let SpareBuffers {
        value_index_builders,
        ip_addr_values,
        ..
    } = buffers;
    send_to_serialize_column_mappable_to_u128(
        column_operations_it,
        cardinality,
        num_docs,
        value_index_builders,
        ip_addr_values,
        wrt,
    )?;
    Ok(())
}

fn send_to_serialize_column_mappable_to_u128<
    T: Copy + Ord + std::fmt::Debug + Send + Sync + MonotonicallyMappableToU128 + PartialOrd,
>(
    op_iterator: impl Iterator<Item = ColumnOperation<T>>,
    cardinality: Cardinality,
    num_rows: RowId,
    value_index_builders: &mut PreallocatedIndexBuilders,
    values: &mut Vec<T>,
    mut wrt: impl io::Write,
) -> io::Result<()> {
    values.clear();
    // TODO: split index and values
    let serializable_column_index = match cardinality {
        Cardinality::Full => {
            consume_operation_iterator(
                op_iterator,
                value_index_builders.borrow_required_index_builder(),
                values,
            );
            SerializableColumnIndex::Full
        }
        Cardinality::Optional => {
            let optional_index_builder = value_index_builders.borrow_optional_index_builder();
            consume_operation_iterator(op_iterator, optional_index_builder, values);
            let optional_index = optional_index_builder.finish(num_rows);
            SerializableColumnIndex::Optional(SerializableOptionalIndex {
                num_rows,
                non_null_row_ids: Box::new(optional_index),
            })
        }
        Cardinality::Multivalued => {
            let multivalued_index_builder = value_index_builders.borrow_multivalued_index_builder();
            consume_operation_iterator(op_iterator, multivalued_index_builder, values);
            let serializable_multivalued_index = multivalued_index_builder.finish(num_rows);
            SerializableColumnIndex::Multivalued(serializable_multivalued_index)
        }
    };
    crate::column::serialize_column_mappable_to_u128(
        serializable_column_index,
        &&values[..],
        &mut wrt,
    )?;
    Ok(())
}

fn send_to_serialize_column_mappable_to_u64(
    op_iterator: impl Iterator<Item = ColumnOperation<u64>>,
    cardinality: Cardinality,
    num_rows: RowId,
    sort_values_within_row: bool,
    value_index_builders: &mut PreallocatedIndexBuilders,
    values: &mut Vec<u64>,
    mut wrt: impl io::Write,
) -> io::Result<()> {
    values.clear();
    let serializable_column_index = match cardinality {
        Cardinality::Full => {
            consume_operation_iterator(
                op_iterator,
                value_index_builders.borrow_required_index_builder(),
                values,
            );
            SerializableColumnIndex::Full
        }
        Cardinality::Optional => {
            let optional_index_builder = value_index_builders.borrow_optional_index_builder();
            consume_operation_iterator(op_iterator, optional_index_builder, values);
            let optional_index = optional_index_builder.finish(num_rows);
            SerializableColumnIndex::Optional(SerializableOptionalIndex {
                non_null_row_ids: Box::new(optional_index),
                num_rows,
            })
        }
        Cardinality::Multivalued => {
            let multivalued_index_builder = value_index_builders.borrow_multivalued_index_builder();
            consume_operation_iterator(op_iterator, multivalued_index_builder, values);
            let serializable_multivalued_index = multivalued_index_builder.finish(num_rows);
            if sort_values_within_row {
                sort_values_within_row_in_place(
                    serializable_multivalued_index.start_offsets.boxed_iter(),
                    values,
                );
            }
            SerializableColumnIndex::Multivalued(serializable_multivalued_index)
        }
    };
    crate::column::serialize_column_mappable_to_u64(
        serializable_column_index,
        &&values[..],
        &mut wrt,
    )?;
    Ok(())
}

fn sort_values_within_row_in_place(
    multivalued_index: impl Iterator<Item = RowId>,
    values: &mut [u64],
) {
    let mut start_index: usize = 0;
    for end_index in multivalued_index {
        let end_index = end_index as usize;
        values[start_index..end_index].sort_unstable();
        start_index = end_index;
    }
}

fn coerce_numerical_symbol<T>(
    operation_iterator: impl Iterator<Item = ColumnOperation<NumericalValue>>,
) -> impl Iterator<Item = ColumnOperation<u64>>
where T: Coerce + MonotonicallyMappableToU64 {
    operation_iterator.map(|symbol| match symbol {
        ColumnOperation::NewDoc(doc) => ColumnOperation::NewDoc(doc),
        ColumnOperation::Value(numerical_value) => {
            ColumnOperation::Value(T::coerce(numerical_value).to_u64())
        }
    })
}

fn consume_operation_iterator<T: Ord, TIndexBuilder: IndexBuilder>(
    operation_iterator: impl Iterator<Item = ColumnOperation<T>>,
    index_builder: &mut TIndexBuilder,
    values: &mut Vec<T>,
) {
    for symbol in operation_iterator {
        match symbol {
            ColumnOperation::NewDoc(doc) => {
                index_builder.record_row(doc);
            }
            ColumnOperation::Value(value) => {
                index_builder.record_value();
                values.push(value);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use stacker::MemoryArena;

    use crate::columnar::writer::column_operation::ColumnOperation;
    use crate::{Cardinality, NumericalValue};

    #[test]
    fn test_column_writer_required_simple() {
        let mut arena = MemoryArena::default();
        let mut column_writer = super::ColumnWriter::default();
        column_writer.record(0u32, NumericalValue::from(14i64), &mut arena);
        column_writer.record(1u32, NumericalValue::from(15i64), &mut arena);
        column_writer.record(2u32, NumericalValue::from(-16i64), &mut arena);
        assert_eq!(column_writer.get_cardinality(3), Cardinality::Full);
        let mut buffer = Vec::new();
        let symbols: Vec<ColumnOperation<NumericalValue>> = column_writer
            .operation_iterator(&arena, &mut buffer)
            .collect();
        assert_eq!(symbols.len(), 6);
        assert!(matches!(symbols[0], ColumnOperation::NewDoc(0u32)));
        assert!(matches!(
            symbols[1],
            ColumnOperation::Value(NumericalValue::I64(14i64))
        ));
        assert!(matches!(symbols[2], ColumnOperation::NewDoc(1u32)));
        assert!(matches!(
            symbols[3],
            ColumnOperation::Value(NumericalValue::I64(15i64))
        ));
        assert!(matches!(symbols[4], ColumnOperation::NewDoc(2u32)));
        assert!(matches!(
            symbols[5],
            ColumnOperation::Value(NumericalValue::I64(-16i64))
        ));
    }

    #[test]
    fn test_column_writer_optional_cardinality_missing_first() {
        let mut arena = MemoryArena::default();
        let mut column_writer = super::ColumnWriter::default();
        column_writer.record(1u32, NumericalValue::from(15i64), &mut arena);
        column_writer.record(2u32, NumericalValue::from(-16i64), &mut arena);
        assert_eq!(column_writer.get_cardinality(3), Cardinality::Optional);
        let mut buffer = Vec::new();
        let symbols: Vec<ColumnOperation<NumericalValue>> = column_writer
            .operation_iterator(&arena, &mut buffer)
            .collect();
        assert_eq!(symbols.len(), 4);
        assert!(matches!(symbols[0], ColumnOperation::NewDoc(1u32)));
        assert!(matches!(
            symbols[1],
            ColumnOperation::Value(NumericalValue::I64(15i64))
        ));
        assert!(matches!(symbols[2], ColumnOperation::NewDoc(2u32)));
        assert!(matches!(
            symbols[3],
            ColumnOperation::Value(NumericalValue::I64(-16i64))
        ));
    }

    #[test]
    fn test_column_writer_optional_cardinality_missing_last() {
        let mut arena = MemoryArena::default();
        let mut column_writer = super::ColumnWriter::default();
        column_writer.record(0u32, NumericalValue::from(15i64), &mut arena);
        assert_eq!(column_writer.get_cardinality(2), Cardinality::Optional);
        let mut buffer = Vec::new();
        let symbols: Vec<ColumnOperation<NumericalValue>> = column_writer
            .operation_iterator(&arena, &mut buffer)
            .collect();
        assert_eq!(symbols.len(), 2);
        assert!(matches!(symbols[0], ColumnOperation::NewDoc(0u32)));
        assert!(matches!(
            symbols[1],
            ColumnOperation::Value(NumericalValue::I64(15i64))
        ));
    }

    #[test]
    fn test_column_writer_multivalued() {
        let mut arena = MemoryArena::default();
        let mut column_writer = super::ColumnWriter::default();
        column_writer.record(0u32, NumericalValue::from(16i64), &mut arena);
        column_writer.record(0u32, NumericalValue::from(17i64), &mut arena);
        assert_eq!(column_writer.get_cardinality(1), Cardinality::Multivalued);
        let mut buffer = Vec::new();
        let symbols: Vec<ColumnOperation<NumericalValue>> = column_writer
            .operation_iterator(&arena, &mut buffer)
            .collect();
        assert_eq!(symbols.len(), 3);
        assert!(matches!(symbols[0], ColumnOperation::NewDoc(0u32)));
        assert!(matches!(
            symbols[1],
            ColumnOperation::Value(NumericalValue::I64(16i64))
        ));
        assert!(matches!(
            symbols[2],
            ColumnOperation::Value(NumericalValue::I64(17i64))
        ));
    }
}
