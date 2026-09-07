use std::io;
use std::io::Write;
use std::sync::Arc;

use common::OwnedBytes;
use sstable::Dictionary;

use crate::column::{BytesColumn, Column};
use crate::column_index::{SerializableColumnIndex, serialize_column_index};
use crate::column_values::{
    CodecType, MonotonicallyMappableToU64, MonotonicallyMappableToU128,
    load_u64_based_column_values, serialize_column_values_u128, serialize_u64_based_column_values,
};
use crate::iterable::Iterable;
use crate::{StrColumn, Version};

pub fn serialize_column_mappable_to_u128<T: MonotonicallyMappableToU128>(
    column_index: SerializableColumnIndex<'_>,
    iterable: &dyn Iterable<T>,
    output: &mut impl Write,
) -> io::Result<()> {
    let column_index_num_bytes = serialize_column_index(column_index, output)?;
    serialize_column_values_u128(iterable, output)?;
    output.write_all(&column_index_num_bytes.to_le_bytes())?;
    Ok(())
}

pub fn serialize_column_mappable_to_u64<T: MonotonicallyMappableToU64>(
    column_index: SerializableColumnIndex<'_>,
    column_values: &impl Iterable<T>,
    output: &mut impl Write,
) -> io::Result<()> {
    let column_index_num_bytes = serialize_column_index(column_index, output)?;
    serialize_u64_based_column_values(
        column_values,
        &[CodecType::Bitpacked, CodecType::BlockwiseLinear],
        output,
    )?;
    output.write_all(&column_index_num_bytes.to_le_bytes())?;
    Ok(())
}

pub fn open_column_u64<T: MonotonicallyMappableToU64>(
    bytes: OwnedBytes,
    format_version: Version,
) -> io::Result<Column<T>> {
    let (body, column_index_num_bytes_payload) = bytes.rsplit(4);
    let column_index_num_bytes = u32::from_le_bytes(
        column_index_num_bytes_payload
            .as_slice()
            .try_into()
            .unwrap(),
    );
    let (column_index_data, column_values_data) = body.split(column_index_num_bytes as usize);
    let column_index = crate::column_index::open_column_index(column_index_data, format_version)?;
    let column_values = load_u64_based_column_values(column_values_data)?;
    Ok(Column {
        index: column_index,
        values: column_values,
    })
}

pub fn open_column_u128<T: MonotonicallyMappableToU128>(
    bytes: OwnedBytes,
    format_version: Version,
) -> io::Result<Column<T>> {
    let (body, column_index_num_bytes_payload) = bytes.rsplit(4);
    let column_index_num_bytes = u32::from_le_bytes(
        column_index_num_bytes_payload
            .as_slice()
            .try_into()
            .unwrap(),
    );
    let (column_index_data, column_values_data) = body.split(column_index_num_bytes as usize);
    let column_index = crate::column_index::open_column_index(column_index_data, format_version)?;
    let column_values = crate::column_values::open_u128_mapped(column_values_data)?;
    Ok(Column {
        index: column_index,
        values: column_values,
    })
}

/// Open the column as u64.
///
/// See [`open_u128_as_compact_u64`] for more details.
pub fn open_column_u128_as_compact_u64(
    bytes: OwnedBytes,
    format_version: Version,
) -> io::Result<Column<u64>> {
    let (body, column_index_num_bytes_payload) = bytes.rsplit(4);
    let column_index_num_bytes = u32::from_le_bytes(
        column_index_num_bytes_payload
            .as_slice()
            .try_into()
            .unwrap(),
    );
    let (column_index_data, column_values_data) = body.split(column_index_num_bytes as usize);
    let column_index = crate::column_index::open_column_index(column_index_data, format_version)?;
    let column_values = crate::column_values::open_u128_as_compact_u64(column_values_data)?;
    Ok(Column {
        index: column_index,
        values: column_values,
    })
}

pub fn open_column_bytes(data: OwnedBytes, format_version: Version) -> io::Result<BytesColumn> {
    let (body, dictionary_len_bytes) = data.rsplit(4);
    let dictionary_len = u32::from_le_bytes(dictionary_len_bytes.as_slice().try_into().unwrap());
    let (dictionary_bytes, column_bytes) = body.split(dictionary_len as usize);
    let dictionary = Arc::new(Dictionary::from_bytes(dictionary_bytes)?);
    let term_ord_column = crate::column::open_column_u64::<u64>(column_bytes, format_version)?;
    Ok(BytesColumn {
        dictionary,
        term_ord_column,
    })
}

pub fn open_column_str(data: OwnedBytes, format_version: Version) -> io::Result<StrColumn> {
    let bytes_column = open_column_bytes(data, format_version)?;
    Ok(StrColumn::wrap(bytes_column))
}
