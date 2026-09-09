//! This will enhance the request tree with access to the fastfield and metadata.

use std::io;

use columnar::{Column, ColumnType};

use crate::aggregation::{f64_to_fastfield_u64, Key};
use crate::index::SegmentReader;

/// Get the missing value as internal u64 representation
///
/// For terms we use u64::MAX as sentinel value
/// For numerical data we convert the value into the representation
/// we would get from the fast field, when we open it as u64_lenient_for_type.
///
/// That way we can use it the same way as if it would come from the fastfield.
pub(crate) fn get_missing_val_as_u64_lenient(
    column_type: ColumnType,
    column_max_value: u64,
    missing: &Key,
    field_name: &str,
) -> crate::Result<Option<u64>> {
    let missing_val = match missing {
        Key::Str(_) if column_type == ColumnType::Str => Some(column_max_value + 1),
        // Allow fallback to number on text fields
        Key::F64(_) if column_type == ColumnType::Str => Some(column_max_value + 1),
        Key::U64(_) if column_type == ColumnType::Str => Some(column_max_value + 1),
        Key::I64(_) if column_type == ColumnType::Str => Some(column_max_value + 1),
        Key::F64(val) if column_type.numerical_type().is_some() => {
            f64_to_fastfield_u64(*val, &column_type)
        }
        // NOTE: We may loose precision of the passed missing value by casting i64 and u64 to f64.
        Key::I64(val) if column_type.numerical_type().is_some() => {
            f64_to_fastfield_u64(*val as f64, &column_type)
        }
        Key::U64(val) if column_type.numerical_type().is_some() => {
            f64_to_fastfield_u64(*val as f64, &column_type)
        }
        _ => {
            return Err(crate::TantivyError::InvalidArgument(format!(
                "Missing value {missing:?} for field {field_name} is not supported for column \
                 type {column_type:?}"
            )));
        }
    };
    Ok(missing_val)
}

pub(crate) fn get_numeric_or_date_column_types() -> &'static [ColumnType] {
    &[
        ColumnType::F64,
        ColumnType::U64,
        ColumnType::I64,
        ColumnType::DateTime,
    ]
}

/// Get fast field reader or empty as default.
pub(crate) fn get_ff_reader(
    reader: &SegmentReader,
    field_name: &str,
    allowed_column_types: Option<&[ColumnType]>,
) -> crate::Result<(columnar::Column<u64>, ColumnType)> {
    let ff_fields = reader.fast_fields();
    let ff_field_with_type = ff_fields
        .u64_lenient_for_type(allowed_column_types, field_name)?
        .unwrap_or_else(|| {
            (
                Column::build_empty_column(reader.num_docs()),
                ColumnType::U64,
            )
        });
    Ok(ff_field_with_type)
}

pub(crate) fn get_dynamic_columns(
    reader: &SegmentReader,
    field_name: &str,
) -> crate::Result<Vec<columnar::DynamicColumn>> {
    let ff_fields = reader.fast_fields().dynamic_column_handles(field_name)?;
    let cols = ff_fields
        .iter()
        .map(|h| h.open())
        .collect::<io::Result<_>>()?;
    assert!(!ff_fields.is_empty(), "field {field_name} not found");
    Ok(cols)
}

/// Get all fast field reader or empty as default.
///
/// Is guaranteed to return at least one column.
pub(crate) fn get_all_ff_reader_or_empty(
    reader: &SegmentReader,
    field_name: &str,
    allowed_column_types: Option<&[ColumnType]>,
    fallback_type: ColumnType,
) -> crate::Result<Vec<(columnar::Column<u64>, ColumnType)>> {
    let ff_fields = reader.fast_fields();
    let mut ff_field_with_type =
        ff_fields.u64_lenient_for_type_all(allowed_column_types, field_name)?;
    if ff_field_with_type.is_empty() {
        ff_field_with_type.push((Column::build_empty_column(reader.num_docs()), fallback_type));
    }
    Ok(ff_field_with_type)
}
