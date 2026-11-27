//! PostgreSQL's json type.

use super::{FormatName, RegisteredType};
use crate::error::sqlstate_error;
use crate::functions::{invalid_json, jsonb_text};
use turso_core::ast;
use turso_core::types::{Conversion, StorageClass, TextInput, Type};
use turso_core::{LimboError, Result, Value};
use turso_pg_parser::sqlstate::SQLSTATE_FEATURE_NOT_SUPPORTED;

/// PostgreSQL's json: JSON text, stored exactly as it is written, with its
/// spaces, the order of its fields and fields with the same name. Only the
/// syntax is checked. The json of core keeps the compact form that SQLite's
/// json() gives instead.
#[derive(Debug)]
pub(super) struct Json;

impl Type for Json {
    fn name(&self) -> &str {
        "json"
    }

    fn params(&self) -> Vec<ast::TypeParam> {
        vec![ast::TypeParam {
            name: "value".to_string(),
            ty: Some("text".to_string()),
        }]
    }

    fn base(&self) -> &str {
        "text"
    }

    fn input(&self) -> Option<TextInput> {
        Some(|_, text| json_input(text))
    }

    fn encode(&self) -> Option<Conversion> {
        Some(Conversion::Function(json_encode))
    }

    fn decode(&self) -> Option<Conversion> {
        Some(Conversion::Unchanged)
    }

    fn reads_as(&self) -> Option<StorageClass> {
        Some(StorageClass::Text)
    }
}

impl RegisteredType for Json {
    fn format_name(&self) -> FormatName {
        FormatName::Plain("json")
    }
}

/// The json value that `text` is, as PostgreSQL's json_in reads it: the text
/// itself, when it is JSON.
fn json_input(text: &str) -> Result<Value> {
    check_json_syntax(text)?;
    Ok(Value::build_text(text.to_string()))
}

/// Stores a json value as its text. Text must be JSON. Another value, such
/// as the result of a function that returns json, is stored as its text.
fn json_encode(value: &Value, _params: &[Value]) -> Result<Value> {
    match value {
        Value::Text(text) => json_input(text.as_str()),
        Value::Null => Ok(Value::Null),
        other => json_input(&other.to_string()),
    }
}

/// Fails with 22P02 when `text` is not JSON. As in PostgreSQL, the escapes of
/// strings are not checked further: `"\ud800"` and `"\u0000"` are json.
fn check_json_syntax(text: &str) -> Result<()> {
    serde_json::from_str::<serde::de::IgnoredAny>(text)
        .map(|_| ())
        .map_err(|_| invalid_json())
}

/// PostgreSQL's jsonb: a JSON value stored in the form jsonb_out writes it,
/// as text such as `{"a": 1, "b": [1, 2]}`. An object keeps one field of each
/// name, the last one, and its fields are ordered by the length of their
/// names and then by their bytes. A number is written as numeric writes it.
/// Two equal values thus have the same stored form.
///
/// Older versions stored a jsonb value in the binary form of SQLite's
/// jsonb(), as the jsonb of core does. The frontend cannot read that form, so
/// such a value fails with 0A000 instead of being read as something else.
/// jsonb columns are custom types, which are an experimental feature.
#[derive(Debug)]
pub(super) struct Jsonb;

impl Type for Jsonb {
    fn name(&self) -> &str {
        "jsonb"
    }

    fn params(&self) -> Vec<ast::TypeParam> {
        vec![ast::TypeParam {
            name: "value".to_string(),
            ty: Some("text".to_string()),
        }]
    }

    fn base(&self) -> &str {
        "text"
    }

    fn input(&self) -> Option<TextInput> {
        Some(|_, text| jsonb_input(text))
    }

    fn encode(&self) -> Option<Conversion> {
        Some(Conversion::Function(jsonb_encode))
    }

    fn decode(&self) -> Option<Conversion> {
        Some(Conversion::Function(jsonb_decode))
    }

    fn reads_as(&self) -> Option<StorageClass> {
        Some(StorageClass::Text)
    }

    fn casts_to_numbers_and_booleans(&self) -> bool {
        true
    }
}

impl RegisteredType for Jsonb {
    fn format_name(&self) -> FormatName {
        FormatName::Plain("jsonb")
    }
}

/// The jsonb value that `text` is, as PostgreSQL's jsonb_in reads it.
fn jsonb_input(text: &str) -> Result<Value> {
    Ok(Value::build_text(jsonb_text(text)?))
}

/// Stores a jsonb value in the form jsonb_out writes it.
fn jsonb_encode(value: &Value, _params: &[Value]) -> Result<Value> {
    match value {
        Value::Text(text) => jsonb_input(text.as_str()),
        Value::Null => Ok(Value::Null),
        Value::Blob(_) => Err(binary_jsonb()),
        other => jsonb_input(&other.to_string()),
    }
}

/// Reads a stored jsonb value, which is already in the form jsonb_out
/// writes.
fn jsonb_decode(value: &Value, _params: &[Value]) -> Result<Value> {
    match value {
        Value::Blob(_) => Err(binary_jsonb()),
        other => Ok(other.clone()),
    }
}

/// The error for a value in the binary form of SQLite's jsonb(), which an
/// older version stored in a jsonb column.
fn binary_jsonb() -> LimboError {
    sqlstate_error(
        SQLSTATE_FEATURE_NOT_SUPPORTED,
        "a jsonb value in the binary form of an older version is not supported",
        None,
    )
}
