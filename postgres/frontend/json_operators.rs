//! The functions behind the PostgreSQL JSON operators `->`, `->>`, `#>`, `#>>`,
//! `@>`, `<@`, `?`, `?|` and `?&`. The translator has no catalog, so `@>` and
//! `<@` look at their values to tell a JSON document from an array.

use crate::error::sqlstate_error;
use crate::functions::{pg_scalar_function, Boolean};
use serde_json::Value as Json;
use std::collections::BTreeSet;
use turso_core::native_ext::FunctionArity;
use turso_core::{
    array_values_from_any, get_json, parse_text_array_of_texts, OpenOptions, Result, Value,
};
use turso_pg_parser::sqlstate::SQLSTATE_INVALID_TEXT_REPRESENTATION;

pub(crate) fn register_functions(options: OpenOptions) -> OpenOptions {
    options
        .extension_function(pg_scalar_function(
            "pg_json_key",
            FunctionArity::Exact(1),
            true,
            |_, args| Ok(exec_json_key(args[0].get_value())),
        ))
        .extension_function(pg_scalar_function(
            "pg_json_path",
            FunctionArity::Exact(2),
            true,
            |_, args| exec_json_path(args[0].get_value(), args[1].get_value()),
        ))
        .extension_function(pg_scalar_function(
            "pg_json_text",
            FunctionArity::Exact(1),
            true,
            |_, args| Ok(exec_json_text(args[0].get_value())),
        ))
        .extension_function(pg_scalar_function(
            "pg_contains",
            FunctionArity::Exact(2),
            true,
            |_, args| exec_contains(args[0].get_value(), args[1].get_value()),
        ))
        .extension_function(pg_scalar_function(
            "jsonb_exists",
            FunctionArity::Exact(2),
            true,
            |_, args| {
                let [document, key] = [args[0].get_value(), args[1].get_value()];
                if matches!(document, Value::Null) || matches!(key, Value::Null) {
                    return Ok(None);
                }
                let key = value_to_text(key);
                Ok(Some(Boolean(json_has_key(&parse_json(document)?, &key))))
            },
        ))
        .extension_function(pg_scalar_function(
            "jsonb_exists_any",
            FunctionArity::Exact(2),
            true,
            |_, args| {
                Ok(keys_found(args[0].get_value(), args[1].get_value())?
                    .map(|found| Boolean(found.contains(&true))))
            },
        ))
        .extension_function(pg_scalar_function(
            "jsonb_exists_all",
            FunctionArity::Exact(2),
            true,
            |_, args| {
                Ok(keys_found(args[0].get_value(), args[1].get_value())?
                    .map(|found| Boolean(!found.contains(&false))))
            },
        ))
}

/// The right operand of `->` as core expects it. Core reads a text starting
/// with `$` as a path, but PostgreSQL always reads a text as an object key.
fn exec_json_key(key: &Value) -> Value {
    match key {
        Value::Text(key) => Value::build_text(object_key_path(key.as_str())),
        other => other.clone(),
    }
}

fn object_key_path(key: &str) -> String {
    format!("$.{}", quoted_key(key))
}

fn quoted_key(key: &str) -> String {
    Json::String(key.to_string()).to_string()
}

/// The core path for a `#>` path. Whether an element such as `0` is an array
/// index or an object key depends on the value it is applied to, so this walks
/// the document. NULL if the path does not lead to a value.
fn exec_json_path(document: &Value, path: &Value) -> Result<Option<String>> {
    if matches!(document, Value::Null) || matches!(path, Value::Null) {
        return Ok(None);
    }
    let document = parse_json(document)?;
    let mut current = &document;
    let mut core_path = String::from("$");
    for element in text_array(path)? {
        let Some(element) = element else {
            return Ok(None);
        };
        match current {
            Json::Object(fields) => {
                let Some(field) = fields.get(&element) else {
                    return Ok(None);
                };
                core_path.push('.');
                core_path.push_str(&quoted_key(&element));
                current = field;
            }
            Json::Array(items) => {
                let Ok(index) = element.parse::<i64>() else {
                    return Ok(None);
                };
                let from_start = if index < 0 {
                    items.len() as i64 + index
                } else {
                    index
                };
                let Some(item) = usize::try_from(from_start)
                    .ok()
                    .and_then(|from_start| items.get(from_start))
                else {
                    return Ok(None);
                };
                core_path.push_str(&format!("[{from_start}]"));
                current = item;
            }
            _ => return Ok(None),
        }
    }
    Ok(Some(core_path))
}

/// Converts the JSON text of a value to the text `->>` returns: a JSON string
/// loses its quotes and a JSON null is a SQL NULL. Everything else keeps its
/// text, so `1.50` stays `1.50` and `true` stays `true`.
fn exec_json_text(json: &Value) -> Option<String> {
    let Value::Text(text) = json else {
        return match json {
            Value::Null => None,
            other => Some(other.to_string()),
        };
    };
    let text = text.as_str();
    if text == "null" {
        return None;
    }
    Some(serde_json::from_str::<String>(text).unwrap_or_else(|_| text.to_string()))
}

fn exec_contains(container: &Value, contained: &Value) -> Result<Option<Boolean>> {
    if matches!(container, Value::Null) || matches!(contained, Value::Null) {
        return Ok(None);
    }
    if is_array_blob(container) || is_array_blob(contained) {
        return Ok(array_contains_all(container, contained));
    }
    match (parse_json(container), parse_json(contained)) {
        (Ok(container), Ok(contained)) => Ok(Some(Boolean(json_contains_top_level(
            &container, &contained,
        )))),
        // `{1,2}` is not JSON and `{}` is both, so these can only be arrays.
        (Err(_), Err(_)) => Ok(array_contains_all(container, contained)),
        (Err(_), Ok(other)) | (Ok(other), Err(_)) if is_empty_object(&other) => {
            Ok(array_contains_all(container, contained))
        }
        (Err(invalid), Ok(_)) | (Ok(_), Err(invalid)) => Err(invalid),
    }
}

/// A CHECK or an index reads the stored form of a column, which is a blob for
/// both array and jsonb columns. The stored form of an empty array is also the
/// stored form of the jsonb `true`; it is read as an array here.
fn is_array_blob(value: &Value) -> bool {
    match value {
        Value::Blob(blob) => blob.as_slice() == [1] || parse_json(value).is_err(),
        _ => false,
    }
}

fn is_empty_object(json: &Json) -> bool {
    json.as_object().is_some_and(|fields| fields.is_empty())
}

fn array_contains_all(container: &Value, contained: &Value) -> Option<Boolean> {
    let container = array_values_from_any(container)?;
    let contained = array_values_from_any(contained)?;
    let container: BTreeSet<&Value> = container.iter().collect();
    Some(Boolean(
        contained.iter().all(|element| container.contains(element)),
    ))
}

/// An array also contains a scalar that is one of its elements, but only at
/// the top level of `@>`.
fn json_contains_top_level(container: &Json, contained: &Json) -> bool {
    match (container, contained) {
        (Json::Array(items), scalar) if !is_container(scalar) => {
            items.iter().any(|item| json_contains(item, scalar))
        }
        _ => json_contains(container, contained),
    }
}

fn is_container(json: &Json) -> bool {
    matches!(json, Json::Array(_) | Json::Object(_))
}

fn json_contains(container: &Json, contained: &Json) -> bool {
    match (container, contained) {
        (Json::Object(fields), Json::Object(wanted)) => wanted.iter().all(|(key, value)| {
            fields
                .get(key)
                .is_some_and(|field| json_contains(field, value))
        }),
        (Json::Array(items), Json::Array(wanted)) => wanted
            .iter()
            .all(|value| items.iter().any(|item| json_contains(item, value))),
        (Json::Number(left), Json::Number(right)) => json_numbers_equal(left, right),
        (left, right) if !is_container(left) && !is_container(right) => left == right,
        _ => false,
    }
}

/// `1` and `1.0` are the same jsonb number, but not the same serde_json number.
fn json_numbers_equal(left: &serde_json::Number, right: &serde_json::Number) -> bool {
    left == right || ((left.is_f64() || right.is_f64()) && left.as_f64() == right.as_f64())
}

/// For each non-NULL key of a `text[]`, whether the document has it. NULL if an
/// argument is NULL.
fn keys_found(document: &Value, keys: &Value) -> Result<Option<Vec<bool>>> {
    if matches!(document, Value::Null) || matches!(keys, Value::Null) {
        return Ok(None);
    }
    let document = parse_json(document)?;
    Ok(Some(
        text_array(keys)?
            .into_iter()
            .flatten()
            .map(|key| json_has_key(&document, &key))
            .collect(),
    ))
}

/// True if `key` is a key of an object, a string element of an array, or equal
/// to a string scalar.
fn json_has_key(document: &Json, key: &str) -> bool {
    match document {
        Json::Object(fields) => fields.contains_key(key),
        Json::Array(items) => items.iter().any(|item| item.as_str() == Some(key)),
        Json::String(text) => text == key,
        _ => false,
    }
}

fn parse_json(value: &Value) -> Result<Json> {
    let invalid_json = || {
        sqlstate_error(
            SQLSTATE_INVALID_TEXT_REPRESENTATION,
            "invalid input syntax for type json",
        )
    };
    let text = match value {
        Value::Blob(_) => value_to_text(&get_json(value, None).map_err(|_| invalid_json())?),
        other => value_to_text(other),
    };
    serde_json::from_str(&text).map_err(|_| invalid_json())
}

/// The elements of a `text[]`, with `None` for a NULL element.
fn text_array(value: &Value) -> Result<Vec<Option<String>>> {
    let elements = match value {
        Value::Text(text) => parse_text_array_of_texts(text.as_str()),
        other => array_values_from_any(other),
    };
    let elements = elements.ok_or_else(|| {
        sqlstate_error(
            SQLSTATE_INVALID_TEXT_REPRESENTATION,
            format!("malformed array literal: \"{}\"", value_to_text(value)),
        )
    })?;
    Ok(elements
        .iter()
        .map(|element| match element {
            Value::Null => None,
            element => Some(value_to_text(element)),
        })
        .collect())
}

fn value_to_text(value: &Value) -> String {
    match value {
        Value::Text(text) => text.as_str().to_string(),
        other => other.to_string(),
    }
}
