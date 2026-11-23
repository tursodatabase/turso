//! The PostgreSQL text of a value: what the pgwire server sends for a value
//! in text format, and what `COPY ... TO STDOUT` writes before its escaping.

mod pgwire_text;

pub(crate) use pgwire_text::encode_bytea;
use std::borrow::Cow;
use turso_core::{ColumnTypeKind, Numeric, Value};

/// How the values of a column are written as PostgreSQL text. turso_pg stores
/// a boolean as an integer and a timestamptz as text without its offset, so
/// for these columns the stored value is not the text.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TextType {
    /// The stored value as it is.
    Plain,
    /// An integer prints as `t` or `f`.
    Boolean,
    /// Text without an offset gets `+00`.
    TimestampTz,
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
            .find_map(|name| {
                if info.array_dimensions == 0 {
                    Self::of_scalar_name(name)
                } else {
                    None
                }
            })
            .unwrap_or(TextType::Plain)
    }

    /// The text type of the scalar type `name`, or `None` when its values print
    /// as stored.
    pub fn of_scalar_name(name: &str) -> Option<Self> {
        match name.to_ascii_uppercase().as_str() {
            "BOOL" | "BOOLEAN" => Some(TextType::Boolean),
            "TIMESTAMPTZ" => Some(TextType::TimestampTz),
            _ => None,
        }
    }

    /// The suffix that makes the stored text of a value of this type the
    /// PostgreSQL text: a timestamptz stored without an offset is in UTC, and
    /// the `+00` makes clients read it as UTC, not as local time.
    pub fn utc_suffix(self) -> Option<&'static str> {
        (self == TextType::TimestampTz).then_some("+00")
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
        Value::Text(text) => {
            let text = text.as_str();
            if let Some(suffix) = text_type
                .utc_suffix()
                .filter(|_| !text.contains('+') && !text.contains('Z') && !text.ends_with("-00"))
            {
                Cow::Owned(format!("{text}{suffix}"))
            } else {
                Cow::Borrowed(text)
            }
        }
        Value::Blob(bytes) => Cow::Owned(pgwire_text::encode_bytea(bytes, false)),
    };
    Some(text)
}
