//! The PostgreSQL text of a value: what the pgwire server sends for a value
//! in text format, and what `COPY ... TO STDOUT` writes before its escaping.

mod pgwire_text;

use std::borrow::Cow;
use turso_core::{ColumnTypeKind, Numeric, Value};

/// How the values of a column are written as PostgreSQL text. turso_pg stores
/// a boolean as an integer, so for these columns the stored value is not the
/// text.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TextType {
    /// The stored value as it is.
    Plain,
    /// An integer prints as `t` or `f`.
    Boolean,
    /// A blob prints quoted, as an element of a bytea array.
    ByteaArray,
}

impl TextType {
    /// The text type of result column `index` of `stmt`.
    pub fn of_column(stmt: &crate::Statement, index: usize) -> Self {
        let info = stmt
            .get_column_type_info(index)
            .expect("the PostgreSQL dialect enables custom types");
        let Some(info) = info else {
            return TextType::Plain;
        };
        if matches!(info.kind, ColumnTypeKind::Struct | ColumnTypeKind::Union) {
            return TextType::Plain;
        }
        std::iter::once(info.declared_name.as_str())
            .chain(info.base_type.as_deref())
            .find_map(
                |name| match (name.to_ascii_uppercase().as_str(), info.array_dimensions) {
                    ("BOOL" | "BOOLEAN", 0) => Some(TextType::Boolean),
                    ("BLOB" | "BYTEA", 1..) => Some(TextType::ByteaArray),
                    _ => None,
                },
            )
            .unwrap_or(TextType::Plain)
    }
}

/// The PostgreSQL text of `value` in a column of type `text_type`, or `None`
/// for NULL.
pub fn value_text(value: &Value, text_type: TextType) -> Option<Cow<'_, str>> {
    let text = match value {
        Value::Null => return None,
        Value::Numeric(Numeric::Integer(i)) if text_type == TextType::Boolean => {
            Cow::Borrowed(pgwire_text::encode_bool(*i != 0))
        }
        Value::Numeric(Numeric::Integer(i)) => Cow::Owned(pgwire_text::encode_integer(*i)),
        Value::Numeric(Numeric::Float(f)) => Cow::Owned(pgwire_text::format_f64(f64::from(*f))),
        Value::Text(text) => Cow::Borrowed(text.as_str()),
        Value::Blob(bytes) => Cow::Owned(pgwire_text::encode_bytea(
            bytes,
            text_type == TextType::ByteaArray,
        )),
    };
    Some(text)
}
