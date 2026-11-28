use std::collections::BTreeSet;
use std::fmt::Write;

use super::array_value::{encode_array, ArrayValue, ArrayView, DEFAULT_LOWER_BOUND};
use crate::dialect::Dialect;
use crate::numeric::Numeric;
use crate::types::Value;
use crate::Result;

/// Extract values from a record-format array blob.
/// Returns Err if the blob is not a valid record.
/// Uses zero-copy iteration over the blob bytes — no Vec<u8> allocation.
pub(crate) fn array_values_from_blob(blob: &[u8]) -> Result<Vec<Value>> {
    Ok(ArrayValue::decode(blob)?.elements)
}

/// Extract elements from any Value that represents an array.
/// Handles record blobs, JSON text input, and NULL (empty array).
/// Returns None if the value cannot be interpreted as an array.
pub fn array_values_from_any(arr: &Value) -> Option<Vec<Value>> {
    match arr {
        Value::Blob(blob) => array_values_from_blob(blob).ok(),
        Value::Text(text) => parse_text_array(text.as_str()),
        Value::Null => Some(Vec::new()),
        _ => None,
    }
}

/// Parse a text array literal in PG format `{1, hello, NULL}` into a Vec<Value>.
/// Handles integers, floats, strings (quoted and unquoted), and NULL.
pub(crate) fn parse_text_array(text: &str) -> Option<Vec<Value>> {
    parse_text_array_value(text).map(|array| array.elements)
}

/// [parse_text_array] with the lower bound that the text gives.
pub(crate) fn parse_text_array_value(text: &str) -> Option<ArrayValue> {
    parse_text_array_as(text, UnquotedElements::Numbers)
}

/// [parse_text_array] for an array whose elements are texts, such as a
/// `text[]`: an unquoted `1.0` stays the text `1.0`.
pub fn parse_text_array_of_texts(text: &str) -> Option<Vec<Value>> {
    parse_text_array_of_texts_value(text).map(|array| array.elements)
}

/// [parse_text_array_of_texts] with the lower bound that the text gives.
pub(crate) fn parse_text_array_of_texts_value(text: &str) -> Option<ArrayValue> {
    parse_text_array_as(text, UnquotedElements::Texts)
}

/// How an unquoted element other than NULL is read.
#[derive(Clone, Copy, PartialEq, Eq)]
enum UnquotedElements {
    /// As an integer, then a float, then a text.
    Numbers,
    Texts,
}

fn parse_text_array_as(text: &str, unquoted: UnquotedElements) -> Option<ArrayValue> {
    let (bounds, body) = split_bounds(text.trim())?;
    if !(body.starts_with('{') && body.ends_with('}')) {
        return None;
    }
    let elements = parse_pg_text_array(body, unquoted)?;
    let Some((lower_bound, upper_bound)) = bounds else {
        return Some(ArrayValue::new(elements));
    };
    let length = i64::from(upper_bound) - i64::from(lower_bound) + 1;
    if upper_bound < lower_bound || upper_bound == i32::MAX || length != elements.len() as i64 {
        return None;
    }
    Some(ArrayValue {
        lower_bound,
        elements,
    })
}

/// Splits the bounds of `[0:1]={1,2}` from its braces. `[2]` stands for
/// `[1:2]`. The bounds are None for text without them.
fn split_bounds(text: &str) -> Option<(Option<(i32, i32)>, &str)> {
    let Some(rest) = text.strip_prefix('[') else {
        return Some((None, text));
    };
    let (range, body) = rest.split_once(']')?;
    let (lower, upper) = match range.split_once(':') {
        Some((lower, upper)) => (lower.parse().ok()?, upper.parse().ok()?),
        None => (DEFAULT_LOWER_BOUND, range.parse().ok()?),
    };
    let body = body.trim_start().strip_prefix('=')?.trim_start();
    Some((Some((lower, upper)), body))
}

/// Parse a PG-style text array like `{1, hello, NULL, 3.14}` into a Vec<Value>.
/// Unquoted `NULL` (case-insensitive) → Value::Null.
/// Quoted strings use `"..."` with `\"` and `\\` escapes.
/// Unquoted tokens are read as `unquoted` says.
fn parse_pg_text_array(text: &str, unquoted: UnquotedElements) -> Option<Vec<Value>> {
    let inner = text[1..text.len() - 1].trim();
    if inner.is_empty() {
        return Some(Vec::new());
    }
    let bytes = inner.as_bytes();
    let mut pos = 0;
    let mut elements = Vec::new();

    loop {
        // Skip whitespace
        while pos < bytes.len() && bytes[pos].is_ascii_whitespace() {
            pos += 1;
        }
        if pos >= bytes.len() {
            break;
        }

        if bytes[pos] == b'"' {
            // Quoted string
            pos += 1;
            let mut s = String::new();
            loop {
                if pos >= bytes.len() {
                    return None;
                }
                match bytes[pos] {
                    b'\\' => {
                        pos += 1;
                        if pos >= bytes.len() {
                            return None;
                        }
                        match bytes[pos] {
                            b'n' => s.push('\n'),
                            b't' => s.push('\t'),
                            b'r' => s.push('\r'),
                            other => s.push(other as char),
                        }
                    }
                    b'"' => {
                        pos += 1;
                        break;
                    }
                    _ => {
                        let remaining = &inner[pos..];
                        let ch = remaining.chars().next().unwrap_or('\u{FFFD}');
                        s.push(ch);
                        pos += ch.len_utf8();
                        continue;
                    }
                }
                pos += 1;
            }
            elements.push(Value::build_text(s));
        } else {
            // Unquoted token: read until comma, whitespace, or end
            let start = pos;
            while pos < bytes.len() && bytes[pos] != b',' && !bytes[pos].is_ascii_whitespace() {
                pos += 1;
            }
            let token = &inner[start..pos];
            if token.eq_ignore_ascii_case("null") {
                elements.push(Value::Null);
            } else if unquoted == UnquotedElements::Texts {
                elements.push(Value::build_text(token.to_string()));
            } else if let Ok(i) = token.parse::<i64>() {
                elements.push(Value::from_i64(i));
            } else if let Ok(f) = token.parse::<f64>() {
                if !f.is_finite() {
                    return None; // reject Infinity and NaN
                }
                elements.push(Value::from_f64(f));
            } else {
                elements.push(Value::build_text(token.to_string()));
            }
        }

        // Skip whitespace
        while pos < bytes.len() && bytes[pos].is_ascii_whitespace() {
            pos += 1;
        }
        if pos >= bytes.len() {
            break;
        }
        if bytes[pos] == b',' {
            pos += 1;
            // Reject trailing commas: after consuming ',' there must be another element
            let mut peek = pos;
            while peek < bytes.len() && bytes[peek].is_ascii_whitespace() {
                peek += 1;
            }
            if peek >= bytes.len() {
                return None; // trailing comma
            }
        } else if pos < bytes.len() {
            return None;
        }
    }

    Some(elements)
}

/// Serialize a record-format array blob to PostgreSQL text representation.
/// Uses `{...}` delimiters and PG quoting rules:
/// - NULL elements → uppercase `NULL` (unquoted)
/// - Text elements → double-quoted if they contain special chars, unquoted otherwise
/// - Numeric elements → unquoted
/// - Blob elements → as `dialect` writes them
pub(crate) fn serialize_array_from_blob(blob: &[u8], dialect: &dyn Dialect) -> Result<String> {
    let view = ArrayView::parse(blob)?;
    let mut result = String::new();
    if view.lower_bound() != DEFAULT_LOWER_BOUND {
        let count = view.elements().count() as i64;
        let upper_bound = i64::from(view.lower_bound()) + count - 1;
        let _ = write!(result, "[{}:{upper_bound}]=", view.lower_bound());
    }
    result.push('{');
    let mut first = true;
    for vref in view.elements() {
        let vref = vref?;
        if !first {
            result.push(',');
        }
        first = false;
        write_value_ref_pg(&mut result, &vref, dialect);
    }
    result.push('}');
    Ok(result)
}

/// Formats texts as an array literal in the same format, such as
/// `{a,"b c"}`.
pub fn text_array_literal<'a>(elements: impl IntoIterator<Item = &'a str>) -> String {
    let mut result = String::from("{");
    for (i, element) in elements.into_iter().enumerate() {
        if i > 0 {
            result.push(',');
        }
        write_pg_text_element(&mut result, element);
    }
    result.push('}');
    result
}

fn write_value_ref_pg(result: &mut String, val: &crate::ValueRef<'_>, dialect: &dyn Dialect) {
    match val {
        crate::ValueRef::Null => result.push_str("NULL"),
        crate::ValueRef::Numeric(Numeric::Integer(n)) => {
            let _ = write!(result, "{n}");
        }
        crate::ValueRef::Numeric(Numeric::Float(f)) => {
            let fval: f64 = (*f).into();
            // Normalize -0.0 to 0.0 for display
            let fval = if fval == 0.0 { 0.0 } else { fval };
            if fval.fract() == 0.0 && fval.is_finite() {
                let _ = write!(result, "{fval:.1}");
            } else {
                let _ = write!(result, "{fval}");
            }
        }
        crate::ValueRef::Text(t) => {
            write_pg_text_element(result, t.as_str());
        }
        crate::ValueRef::Blob(b) => dialect.write_array_blob_element(result, b),
    }
}

/// Write a text element in PG array format.
/// Simple values are unquoted; values with special chars are double-quoted.
fn write_pg_text_element(result: &mut String, s: &str) {
    let needs_quoting = s.is_empty()
        || s.eq_ignore_ascii_case("null")
        || s.contains(|c: char| {
            c == ','
                || c == '{'
                || c == '}'
                || c == '"'
                || c == '\\'
                || c.is_whitespace()
                || c.is_control()
        });
    if needs_quoting {
        result.push('"');
        for ch in s.chars() {
            match ch {
                '"' => result.push_str("\\\""),
                '\\' => result.push_str("\\\\"),
                '\n' => result.push_str("\\n"),
                '\r' => result.push_str("\\r"),
                '\t' => result.push_str("\\t"),
                c if c.is_control() => {
                    let _ = write!(result, "\\u{:04x}", c as u32);
                }
                c => result.push(c),
            }
        }
        result.push('"');
    } else {
        result.push_str(s);
    }
}

/// Compute the number of elements in an array value. Shared by
/// op_array_length (instruction) and ScalarFunc::ArrayLength (function).
/// Returns None for NULL or non-blob input (maps to SQL NULL).
pub(crate) fn compute_array_length(val: &Value) -> Option<i64> {
    match val {
        Value::Null => None,
        Value::Blob(b) => match ArrayView::parse(b) {
            Ok(view) => Some(view.elements().count() as i64),
            Err(_) => None,
        },
        Value::Text(t) => parse_text_array(t.as_str()).map(|v| v.len() as i64),
        _ => None,
    }
}

/// Compute the element count at array dimension `dim` (1-based). For `dim == 1`,
/// returns the same value as [`compute_array_length`]. For `dim > 1`, the array
/// is assumed to be uniform — each outer element is itself an array — and the
/// walker recurses into element zero `dim - 1` times.
///
/// Returns `None` for NULL or non-array input, for `dim < 1`, for an empty
/// array when `dim > 1` (no element zero to peek into), and for `dim` deeper
/// than the array's actual nesting. Matches PostgreSQL's
/// `array_length(arr, dim)` contract — except Turso doesn't track per-
/// dimension lower bounds, so `array_upper(arr, dim)` equals this function
/// for all valid `dim`.
pub(crate) fn compute_array_length_at_dim(val: &Value, dim: i64) -> Option<i64> {
    if dim < 1 {
        return None;
    }
    if dim == 1 {
        return compute_array_length(val);
    }
    // dim > 1: peek into element zero and recurse. Uniform-shape arrays let
    // us answer "length at depth N" by looking at any element at depth 1;
    // element zero is the cheapest to extract.
    let first = array_values_from_any(val)?.into_iter().next()?;
    compute_array_length_at_dim(&first, dim - 1)
}

pub(crate) fn exec_array_append(arr: &Value, elem: &Value) -> Result<Value> {
    let Some(mut elements) = array_values_from_any(arr) else {
        return Ok(Value::Null);
    };
    elements.push(elem.clone());
    encode_array(DEFAULT_LOWER_BOUND, &elements)
}

pub(crate) fn exec_array_prepend(arr: &Value, elem: &Value) -> Result<Value> {
    let Some(elements) = array_values_from_any(arr) else {
        return Ok(Value::Null);
    };
    // Build new vec with elem first — avoids O(n) shift from Vec::insert(0, ...)
    let mut result = Vec::with_capacity(elements.len() + 1);
    result.push(elem.clone());
    result.extend(elements);
    encode_array(DEFAULT_LOWER_BOUND, &result)
}

pub(crate) fn exec_array_cat(a: &Value, b: &Value) -> Result<Value> {
    if matches!(a, Value::Null) || matches!(b, Value::Null) {
        return Ok(Value::Null);
    }
    let Some(mut elems_a) = array_values_from_any(a) else {
        return Ok(Value::Null);
    };
    let Some(elems_b) = array_values_from_any(b) else {
        return Ok(Value::Null);
    };
    elems_a.extend(elems_b);
    encode_array(DEFAULT_LOWER_BOUND, &elems_a)
}

pub(crate) fn exec_array_remove(arr: &Value, target: &Value) -> Result<Value> {
    if matches!(arr, Value::Null) {
        return Ok(Value::Null);
    }
    let Some(elements) = array_values_from_any(arr) else {
        return Ok(Value::Null);
    };
    let result: Vec<Value> = elements.into_iter().filter(|e| e != target).collect();
    encode_array(DEFAULT_LOWER_BOUND, &result)
}

pub(crate) fn exec_array_contains(arr: &Value, target: &Value) -> Value {
    if matches!(arr, Value::Null) {
        return Value::Null;
    }
    if let Value::Blob(blob) = arr {
        return array_find_streaming(blob, |vref| vref == *target)
            .map(|_| Value::from_i64(1))
            .unwrap_or_else(|| Value::from_i64(0));
    }
    let Some(elements) = array_values_from_any(arr) else {
        return Value::Null;
    };
    let found = elements.iter().any(|e| e == target);
    Value::from_i64(found as i64)
}

pub(crate) fn exec_array_position(arr: &Value, target: &Value) -> Value {
    if matches!(arr, Value::Null) {
        return Value::Null;
    }
    if let Value::Blob(blob) = arr {
        return array_find_streaming(blob, |vref| vref == *target)
            .map(|i| Value::from_i64(i as i64 + 1)) // 1-based (PG convention)
            .unwrap_or(Value::Null);
    }
    let Some(elements) = array_values_from_any(arr) else {
        return Value::Null;
    };
    for (i, elem) in elements.iter().enumerate() {
        if elem == target {
            return Value::from_i64(i as i64 + 1); // 1-based (PG convention)
        }
    }
    Value::Null
}

/// Stream through a record-format blob, calling `predicate` on each element.
/// Returns Some(index) for the first element where the predicate returns true,
/// or None if no match or on error.
fn array_find_streaming(
    blob: &[u8],
    predicate: impl Fn(crate::ValueRef<'_>) -> bool,
) -> Option<usize> {
    let view = ArrayView::parse(blob).ok()?;
    for (i, vref) in view.elements().enumerate() {
        let vref = vref.ok()?;
        if predicate(vref) {
            return Some(i);
        }
    }
    None
}

pub(crate) fn exec_array_slice(arr: &Value, start: &Value, end: &Value) -> Result<Value> {
    if matches!(arr, Value::Null) {
        return Ok(Value::Null);
    }
    let Some(elements) = array_values_from_any(arr) else {
        return Ok(Value::Null);
    };
    // PG convention: 1-based inclusive bounds
    let start_idx = match start {
        Value::Numeric(Numeric::Integer(i)) if *i >= 1 => (*i - 1) as usize,
        _ => 0,
    };
    let end_idx = match end {
        Value::Numeric(Numeric::Integer(i)) if *i >= 1 => *i as usize, // inclusive → exclusive
        _ => 0,
    };
    let end = end_idx.min(elements.len());
    let start = start_idx.min(end);
    encode_array(DEFAULT_LOWER_BOUND, &elements[start..end])
}

/// Split a string into an array using a delimiter.
/// string_to_array(text, delimiter [, null_string])
/// If text is NULL, returns NULL.
/// If delimiter is NULL, splits into individual characters (PostgreSQL behavior).
/// If null_string is provided, any element matching it becomes NULL.
pub(crate) fn exec_string_to_array(
    text: &Value,
    delimiter: &Value,
    null_str: Option<&Value>,
) -> Result<Value> {
    let text_str = match text {
        Value::Text(t) => t.as_str().to_string(),
        Value::Null => return Ok(Value::Null),
        other => other.to_string(),
    };

    let null_match: Option<String> = match null_str {
        Some(Value::Text(t)) => Some(t.as_str().to_string()),
        Some(Value::Null) | None => None,
        Some(other) => Some(other.to_string()),
    };

    // NULL delimiter: split into individual characters (PostgreSQL behavior)
    if matches!(delimiter, Value::Null) {
        let values: Vec<Value> = text_str
            .chars()
            .map(|c| {
                let s = c.to_string();
                if let Some(ref nm) = null_match {
                    if s == *nm {
                        return Value::Null;
                    }
                }
                Value::build_text(s)
            })
            .collect();
        return encode_array(DEFAULT_LOWER_BOUND, &values);
    }

    let delim_str = match delimiter {
        Value::Text(d) => d.as_str().to_string(),
        other => other.to_string(),
    };

    let parts: Vec<&str> = if delim_str.is_empty() {
        // Empty delimiter: return single-element array with the whole string
        vec![&text_str]
    } else {
        text_str.split(&delim_str).collect()
    };

    let values: Vec<Value> = parts
        .into_iter()
        .map(|p| {
            if let Some(ref nm) = null_match {
                if p == nm.as_str() {
                    return Value::Null;
                }
            }
            Value::build_text(p.to_string())
        })
        .collect();

    encode_array(DEFAULT_LOWER_BOUND, &values)
}

/// Join array elements into a string with a delimiter.
/// array_to_string(array, delimiter [, null_string])
/// NULL elements are omitted unless null_string is provided.
pub(crate) fn exec_array_to_string(
    arr: &Value,
    delimiter: &Value,
    null_str: Option<&Value>,
) -> Value {
    if matches!(arr, Value::Null) {
        return Value::Null;
    }

    let delim = match delimiter {
        Value::Text(t) => t.as_str().to_string(),
        Value::Null => return Value::Null,
        other => other.to_string(),
    };

    let null_replacement: Option<String> = match null_str {
        Some(Value::Text(t)) => Some(t.as_str().to_string()),
        Some(Value::Null) | None => None,
        Some(other) => Some(other.to_string()),
    };

    // Fast path: stream from blob without materializing Vec<Value>
    if let Value::Blob(blob) = arr {
        if let Ok(view) = ArrayView::parse(blob) {
            let mut result = String::new();
            let mut first = true;
            for vref in view.elements() {
                let Ok(vref) = vref else {
                    return Value::Null;
                };
                let part = match &vref {
                    crate::ValueRef::Null => {
                        if let Some(ref replacement) = null_replacement {
                            replacement.clone()
                        } else {
                            continue;
                        }
                    }
                    crate::ValueRef::Text(t) => t.as_str().to_string(),
                    other => format!("{other}"),
                };
                if !first {
                    result.push_str(&delim);
                }
                result.push_str(&part);
                first = false;
            }
            return Value::build_text(result);
        }
    }

    let Some(elements) = array_values_from_any(arr) else {
        return Value::Null;
    };

    let mut result = String::new();
    let mut first = true;
    for elem in &elements {
        let part = match elem {
            Value::Null => {
                if let Some(ref replacement) = null_replacement {
                    replacement.clone()
                } else {
                    continue;
                }
            }
            Value::Text(t) => t.as_str().to_string(),
            other => other.to_string(),
        };
        if !first {
            result.push_str(&delim);
        }
        result.push_str(&part);
        first = false;
    }

    Value::build_text(result)
}

/// Check if two arrays have any elements in common.
/// Returns 1 if they share at least one element, 0 otherwise.
/// NULL if either input is not a valid array.
pub(crate) fn exec_array_overlap(a: &Value, b: &Value) -> Value {
    if matches!(a, Value::Null) || matches!(b, Value::Null) {
        return Value::Null;
    }
    let Some(elems_a) = array_values_from_any(a) else {
        return Value::Null;
    };
    let Some(elems_b) = array_values_from_any(b) else {
        return Value::Null;
    };
    // O(n log n + m log n) via BTreeSet instead of O(n*m)
    let set: BTreeSet<&Value> = elems_a.iter().collect();
    let found = elems_b.iter().any(|eb| set.contains(eb));
    Value::from_i64(found as i64)
}

/// Check if array `a` contains all elements of array `b` (@> operator).
/// Returns 1 if every element in `b` appears in `a`, 0 otherwise.
/// NULL if either input is not a valid array.
pub(crate) fn exec_array_contains_all(a: &Value, b: &Value) -> Value {
    match array_contains_all(a, b) {
        Some(all_found) => Value::from_i64(all_found as i64),
        None => Value::Null,
    }
}

/// Whether array `a` contains every element of array `b`, as `a @> b` compares
/// two arrays. None when either value is NULL or not an array.
pub fn array_contains_all(a: &Value, b: &Value) -> Option<bool> {
    if matches!(a, Value::Null) || matches!(b, Value::Null) {
        return None;
    }
    let elems_a = array_values_from_any(a)?;
    let elems_b = array_values_from_any(b)?;
    // O(n log n + m log n) via BTreeSet instead of O(n*m)
    let set: BTreeSet<&Value> = elems_a.iter().collect();
    Some(elems_b.iter().all(|eb| set.contains(eb)))
}

/// Collect values from contiguous registers into a record-format array blob.
pub(crate) fn make_array_from_registers(
    registers: &[super::Register],
    start_reg: usize,
    count: usize,
) -> Result<Value> {
    encode_array(
        DEFAULT_LOWER_BOUND,
        registers[start_reg..start_reg + count]
            .iter()
            .map(|register| register.get_value()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_text_array_multibyte_utf8() {
        let input = r#"{"café","naïve","über"}"#;
        let result = parse_text_array(input).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], Value::build_text("café"));
        assert_eq!(result[1], Value::build_text("naïve"));
        assert_eq!(result[2], Value::build_text("über"));
    }

    #[test]
    fn test_parse_text_array_emoji() {
        let input = r#"{"hello 🌍","test 🚀"}"#;
        let result = parse_text_array(input).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], Value::build_text("hello 🌍"));
        assert_eq!(result[1], Value::build_text("test 🚀"));
    }

    #[test]
    fn test_parse_text_array_cjk() {
        let input = r#"{"你好","世界"}"#;
        let result = parse_text_array(input).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], Value::build_text("你好"));
        assert_eq!(result[1], Value::build_text("世界"));
    }

    #[test]
    fn test_compute_array_length_null_returns_none() {
        assert_eq!(compute_array_length(&Value::Null), None);
    }

    #[test]
    fn test_compute_array_length_valid_array() {
        let blob = encode_array(
            DEFAULT_LOWER_BOUND,
            &[Value::from_i64(1), Value::from_i64(2)],
        )
        .unwrap();
        assert_eq!(compute_array_length(&blob), Some(2));
    }

    #[test]
    fn test_compute_array_length_non_blob_returns_none() {
        assert_eq!(compute_array_length(&Value::from_i64(42)), None,);
    }

    #[test]
    fn test_array_remove_all_occurrences() {
        let arr = encode_array(
            DEFAULT_LOWER_BOUND,
            &[
                Value::from_i64(1),
                Value::from_i64(2),
                Value::from_i64(3),
                Value::from_i64(2),
                Value::from_i64(1),
            ],
        )
        .unwrap();
        let result = exec_array_remove(&arr, &Value::from_i64(2)).unwrap();
        let Value::Blob(blob) = &result else {
            panic!("Expected Blob");
        };
        let elements = array_values_from_blob(blob).unwrap();
        assert_eq!(elements.len(), 3);
        assert_eq!(elements[0], Value::from_i64(1));
        assert_eq!(elements[1], Value::from_i64(3));
        assert_eq!(elements[2], Value::from_i64(1));
    }

    #[test]
    fn test_array_contains_null_array_returns_null() {
        assert_eq!(
            exec_array_contains(&Value::Null, &Value::from_i64(1)),
            Value::Null,
        );
    }

    #[test]
    fn test_array_position_null_array_returns_null() {
        assert_eq!(
            exec_array_position(&Value::Null, &Value::from_i64(1)),
            Value::Null,
        );
    }

    #[test]
    fn test_compute_array_length_invalid_blob_returns_none() {
        // A random blob that is not a valid record should return None
        let invalid = Value::from_slice(&[0xFF, 0xFE, 0xFD]).expect(crate::alloc::ALLOC_ERR_MSG);
        assert_eq!(compute_array_length(&invalid), None);
    }

    #[test]
    fn test_parse_text_array_rejects_json_format() {
        // JSON [1,2,3] format is no longer accepted — only PG {1,2,3}
        assert!(parse_text_array("[1,2,3]").is_none());
        assert!(parse_text_array(r#"["hello"]"#).is_none());
    }

    #[test]
    fn test_parse_text_array_rejects_trailing_comma() {
        assert!(parse_text_array("{1,2,}").is_none());
        assert!(parse_text_array("{1, 2, }").is_none());
    }

    #[test]
    fn test_parse_text_array_rejects_infinity() {
        assert!(parse_text_array("{1e309}").is_none());
        assert!(parse_text_array("{-1e309}").is_none());
    }

    #[test]
    fn test_string_to_array_null_delimiter_splits_chars() {
        let result = exec_string_to_array(&Value::build_text("hello"), &Value::Null, None).unwrap();
        let Value::Blob(blob) = &result else {
            panic!("Expected Blob, got {result:?}");
        };
        let elements = array_values_from_blob(blob).unwrap();
        assert_eq!(elements.len(), 5);
        assert_eq!(elements[0], Value::build_text("h"));
        assert_eq!(elements[1], Value::build_text("e"));
        assert_eq!(elements[4], Value::build_text("o"));
    }

    #[test]
    fn test_exec_array_contains_streaming() {
        let arr = encode_array(
            DEFAULT_LOWER_BOUND,
            &[
                Value::from_i64(10),
                Value::from_i64(20),
                Value::from_i64(30),
            ],
        )
        .unwrap();
        assert_eq!(
            exec_array_contains(&arr, &Value::from_i64(20)),
            Value::from_i64(1)
        );
        assert_eq!(
            exec_array_contains(&arr, &Value::from_i64(99)),
            Value::from_i64(0)
        );
    }

    #[test]
    fn test_exec_array_position_streaming() {
        let arr = encode_array(
            DEFAULT_LOWER_BOUND,
            &[
                Value::from_i64(10),
                Value::from_i64(20),
                Value::from_i64(30),
            ],
        )
        .unwrap();
        // 1-based: element 20 is at position 2
        assert_eq!(
            exec_array_position(&arr, &Value::from_i64(20)),
            Value::from_i64(2)
        );
        assert_eq!(exec_array_position(&arr, &Value::from_i64(99)), Value::Null);
    }

    #[test]
    fn test_dc1_negative_index_preserves_array() {
        let arr = encode_array(
            DEFAULT_LOWER_BOUND,
            &[
                Value::from_i64(10),
                Value::from_i64(20),
                Value::from_i64(30),
            ],
        )
        .unwrap();
        // array_find_streaming with impossible predicate should return None
        let Value::Blob(blob) = &arr else {
            panic!("Expected Blob");
        };
        assert!(array_find_streaming(blob, |_| false).is_none());
    }

    #[test]
    fn test_dc4_array_remove_null_returns_null() {
        assert_eq!(
            exec_array_remove(&Value::Null, &Value::from_i64(1)).unwrap(),
            Value::Null
        );
    }

    #[test]
    fn test_dc4_array_slice_null_returns_null() {
        assert_eq!(
            exec_array_slice(&Value::Null, &Value::from_i64(0), &Value::from_i64(2)).unwrap(),
            Value::Null,
        );
    }

    #[test]
    fn test_dc4_array_cat_null_returns_null() {
        assert_eq!(
            exec_array_cat(&Value::Null, &Value::Null).unwrap(),
            Value::Null
        );
        assert_eq!(
            exec_array_cat(&Value::Null, &Value::from_i64(1)).unwrap(),
            Value::Null,
        );
    }

    fn bounded_blob(lower_bound: i32, elements: &[Value]) -> Vec<u8> {
        match encode_array(lower_bound, elements).unwrap() {
            Value::Blob(blob) => blob.to_vec(),
            other => panic!("expected a blob, got {other:?}"),
        }
    }

    #[test]
    fn serialize_prints_the_bounds_of_an_array_that_does_not_start_at_one() {
        let elements = [Value::from_i64(1), Value::from_i64(2)];
        let dialect = crate::dialect::cannot_parse::DialectThatCannotParse;
        for (lower_bound, expected) in [(1, "{1,2}"), (0, "[0:1]={1,2}"), (-3, "[-3:-2]={1,2}")] {
            let blob = bounded_blob(lower_bound, &elements);
            assert_eq!(
                serialize_array_from_blob(&blob, &dialect).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn parse_reads_the_bounds_of_an_array_text() {
        for (text, lower_bound, count) in [
            ("{1,2}", 1, 2),
            ("[1:2]={1,2}", 1, 2),
            ("[2]={1,2}", 1, 2),
            ("[0:1]={1,2}", 0, 2),
            (" [-2:0] = {1,2,3} ", -2, 3),
            ("[2147483646:2147483646]={1}", 2147483646, 1),
        ] {
            let array = parse_text_array_value(text).unwrap();
            assert_eq!(
                (array.lower_bound, array.elements.len()),
                (lower_bound, count)
            );
        }
    }

    #[test]
    fn parse_refuses_bounds_that_do_not_fit_the_elements() {
        for text in [
            "[0:3]={1,2}",
            "[0:1]={1,2,3}",
            "[1:0]={}",
            "[0:-1]={}",
            "[2147483647:2147483647]={1}",
            "[2147483647:2147483648]={1,2}",
            "[x:1]={1,2}",
            "[ 0:1]={1,2}",
            "[0 :1]={1,2}",
            "[0: 1]={1,2}",
            "[0:1 ]={1,2}",
            "[0:1]",
            "[0:1]{1,2}",
            "[2]={1}",
            "[0]={}",
            "[]={}",
            "[1:2][0:1]={{1,2},{3,4}}",
        ] {
            assert!(parse_text_array_value(text).is_none(), "{text}");
        }
    }

    #[test]
    fn test_serialize_array_from_blob() {
        let arr = encode_array(
            DEFAULT_LOWER_BOUND,
            &[
                Value::from_i64(1),
                Value::build_text("hello"),
                Value::Null,
                Value::Blob(crate::alloc::vec![b'a', b'b']),
            ],
        )
        .unwrap();
        let Value::Blob(blob) = &arr else {
            panic!("Expected Blob");
        };
        let text =
            serialize_array_from_blob(blob, &crate::dialect::cannot_parse::DialectThatCannotParse)
                .unwrap();
        assert_eq!(text, r#"{1,hello,NULL,"X'6162'"}"#);
    }

    #[test]
    fn test_make_array_from_registers() {
        use super::super::Register;
        let registers = vec![
            Register::Value(Value::from_i64(1)),
            Register::Value(Value::build_text("two")),
            Register::Value(Value::from_i64(3)),
        ];
        let result = make_array_from_registers(&registers, 0, 3).unwrap();
        let Value::Blob(blob) = &result else {
            panic!("Expected Blob");
        };
        let elements = array_values_from_blob(blob).unwrap();
        assert_eq!(elements.len(), 3);
        assert_eq!(elements[0], Value::from_i64(1));
        assert_eq!(elements[1], Value::build_text("two"));
        assert_eq!(elements[2], Value::from_i64(3));
    }
}
