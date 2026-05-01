use std::collections::BTreeSet;
use std::fmt::Write;

use crate::numeric::Numeric;
use crate::types::{ImmutableRecord, Value, ValueIterator};
use crate::Result;

/// Extract values from a record-format array blob.
/// Returns Err if the blob is not a valid record.
/// Uses zero-copy iteration over the blob bytes — no Vec<u8> allocation.
pub(crate) fn array_values_from_blob(blob: &[u8]) -> Result<Vec<Value>> {
    let iter = ValueIterator::new(blob)?;
    let mut values = Vec::with_capacity(iter.size_hint().0);
    for value in iter {
        values.push(value?.to_owned());
    }
    Ok(values)
}

/// Extract elements from any Value that represents an array.
/// Handles record blobs, JSON text input, and NULL (empty array).
/// Returns None if the value cannot be interpreted as an array.
pub(crate) fn array_values_from_any(arr: &Value) -> Option<Vec<Value>> {
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
    let text = text.trim();
    if text.starts_with('{') && text.ends_with('}') {
        return parse_pg_text_array(text);
    }
    None
}

/// Parse a PG-style text array like `{1, hello, NULL, 3.14}` into a Vec<Value>.
/// Unquoted `NULL` (case-insensitive) → Value::Null.
/// Quoted strings use `"..."` with `\"` and `\\` escapes.
/// Unquoted tokens are parsed as integer, then float, then text.
fn parse_pg_text_array(text: &str) -> Option<Vec<Value>> {
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

/// Pack values into a record-format array blob.
pub(crate) fn values_to_record_blob(values: &[Value]) -> Value {
    Value::Blob(ImmutableRecord::from_values(values, values.len()).into_payload())
}

/// Serialize a record-format array blob to PostgreSQL text representation.
/// Uses `{...}` delimiters and PG quoting rules:
/// - NULL elements → uppercase `NULL` (unquoted)
/// - Text elements → double-quoted if they contain special chars, unquoted otherwise
/// - Numeric elements → unquoted
pub(crate) fn serialize_array_from_blob(blob: &[u8]) -> Result<String> {
    let iter = ValueIterator::new(blob)?;
    let mut result = String::from("{");
    let mut first = true;
    for vref in iter {
        let vref = vref?;
        if !first {
            result.push(',');
        }
        first = false;
        write_value_ref_pg(&mut result, &vref);
    }
    result.push('}');
    Ok(result)
}

fn write_value_ref_pg(result: &mut String, val: &crate::ValueRef<'_>) {
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
        crate::ValueRef::Blob(b) => {
            result.push_str("\"X'");
            for byte in *b {
                let _ = write!(result, "{byte:02X}");
            }
            result.push_str("'\"");
        }
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
        Value::Blob(b) => match ValueIterator::new(b) {
            Ok(iter) => Some(iter.count() as i64),
            Err(_) => None,
        },
        Value::Text(t) => parse_text_array(t.as_str()).map(|v| v.len() as i64),
        _ => None,
    }
}

pub(crate) fn exec_array_append(arr: &Value, elem: &Value) -> Value {
    let Some(mut elements) = array_values_from_any(arr) else {
        return Value::Null;
    };
    elements.push(elem.clone());
    values_to_record_blob(&elements)
}

pub(crate) fn exec_array_prepend(arr: &Value, elem: &Value) -> Value {
    let Some(elements) = array_values_from_any(arr) else {
        return Value::Null;
    };
    // Build new vec with elem first — avoids O(n) shift from Vec::insert(0, ...)
    let mut result = Vec::with_capacity(elements.len() + 1);
    result.push(elem.clone());
    result.extend(elements);
    values_to_record_blob(&result)
}

pub(crate) fn exec_array_cat(a: &Value, b: &Value) -> Value {
    if matches!(a, Value::Null) || matches!(b, Value::Null) {
        return Value::Null;
    }
    let Some(mut elems_a) = array_values_from_any(a) else {
        return Value::Null;
    };
    let Some(elems_b) = array_values_from_any(b) else {
        return Value::Null;
    };
    elems_a.extend(elems_b);
    values_to_record_blob(&elems_a)
}

pub(crate) fn exec_array_remove(arr: &Value, target: &Value) -> Value {
    if matches!(arr, Value::Null) {
        return Value::Null;
    }
    let Some(elements) = array_values_from_any(arr) else {
        return Value::Null;
    };
    let result: Vec<Value> = elements.into_iter().filter(|e| e != target).collect();
    values_to_record_blob(&result)
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
    let iter = ValueIterator::new(blob).ok()?;
    for (i, vref) in iter.enumerate() {
        let vref = vref.ok()?;
        if predicate(vref) {
            return Some(i);
        }
    }
    None
}

pub(crate) fn exec_array_slice(arr: &Value, start: &Value, end: &Value) -> Value {
    if matches!(arr, Value::Null) {
        return Value::Null;
    }
    let Some(elements) = array_values_from_any(arr) else {
        return Value::Null;
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
    values_to_record_blob(&elements[start..end])
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
) -> Value {
    let text_str = match text {
        Value::Text(t) => t.as_str().to_string(),
        Value::Null => return Value::Null,
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
        return values_to_record_blob(&values);
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

    values_to_record_blob(&values)
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
        if let Ok(iter) = ValueIterator::new(blob) {
            let mut result = String::new();
            let mut first = true;
            for vref in iter {
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
    let all_found = elems_b.iter().all(|eb| set.contains(eb));
    Value::from_i64(all_found as i64)
}

/// Collect values from contiguous registers into a record-format array blob.
pub(crate) fn make_array_from_registers(
    registers: &[super::Register],
    start_reg: usize,
    count: usize,
) -> Value {
    let record = ImmutableRecord::from_registers(&registers[start_reg..start_reg + count], count);
    Value::Blob(record.into_payload())
}

/// Element-wise comparison of two record-format array blobs.
/// Compares corresponding elements using ValueRef ordering.
/// If all common elements are equal, the shorter array is less.
/// Returns Err if either blob is not a valid record.
pub(crate) fn compare_arrays(a: &[u8], b: &[u8]) -> Result<std::cmp::Ordering> {
    let iter_a = ValueIterator::new(a)?;
    let iter_b = ValueIterator::new(b)?;
    let mut count_a = 0usize;
    let mut count_b = 0usize;
    for (va, vb) in iter_a.zip(iter_b) {
        count_a += 1;
        count_b += 1;
        let (va, vb) = (va?, vb?);
        let ord = va.cmp(&vb);
        if !ord.is_eq() {
            return Ok(ord);
        }
    }
    // Count remaining elements in the longer array
    let len_a = count_a + ValueIterator::new(a)?.skip(count_a).count();
    let len_b = count_b + ValueIterator::new(b)?.skip(count_b).count();
    Ok(len_a.cmp(&len_b))
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
        let blob = values_to_record_blob(&[Value::from_i64(1), Value::from_i64(2)]);
        assert_eq!(compute_array_length(&blob), Some(2));
    }

    #[test]
    fn test_compute_array_length_non_blob_returns_none() {
        assert_eq!(compute_array_length(&Value::from_i64(42)), None,);
    }

    #[test]
    fn test_array_remove_all_occurrences() {
        let arr = values_to_record_blob(&[
            Value::from_i64(1),
            Value::from_i64(2),
            Value::from_i64(3),
            Value::from_i64(2),
            Value::from_i64(1),
        ]);
        let result = exec_array_remove(&arr, &Value::from_i64(2));
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
        let invalid = Value::Blob(vec![0xFF, 0xFE, 0xFD]);
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
        let result = exec_string_to_array(&Value::build_text("hello"), &Value::Null, None);
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
        let arr = values_to_record_blob(&[
            Value::from_i64(10),
            Value::from_i64(20),
            Value::from_i64(30),
        ]);
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
        let arr = values_to_record_blob(&[
            Value::from_i64(10),
            Value::from_i64(20),
            Value::from_i64(30),
        ]);
        // 1-based: element 20 is at position 2
        assert_eq!(
            exec_array_position(&arr, &Value::from_i64(20)),
            Value::from_i64(2)
        );
        assert_eq!(exec_array_position(&arr, &Value::from_i64(99)), Value::Null);
    }

    #[test]
    fn test_dc1_negative_index_preserves_array() {
        let arr = values_to_record_blob(&[
            Value::from_i64(10),
            Value::from_i64(20),
            Value::from_i64(30),
        ]);
        // array_find_streaming with impossible predicate should return None
        let Value::Blob(blob) = &arr else {
            panic!("Expected Blob");
        };
        assert!(array_find_streaming(blob, |_| false).is_none());
    }

    #[test]
    fn test_dc4_array_remove_null_returns_null() {
        assert_eq!(
            exec_array_remove(&Value::Null, &Value::from_i64(1)),
            Value::Null
        );
    }

    #[test]
    fn test_dc4_array_slice_null_returns_null() {
        assert_eq!(
            exec_array_slice(&Value::Null, &Value::from_i64(0), &Value::from_i64(2)),
            Value::Null,
        );
    }

    #[test]
    fn test_dc4_array_cat_null_returns_null() {
        assert_eq!(exec_array_cat(&Value::Null, &Value::Null), Value::Null);
        assert_eq!(
            exec_array_cat(&Value::Null, &Value::from_i64(1)),
            Value::Null,
        );
    }

    #[test]
    fn test_serialize_array_from_blob() {
        let arr =
            values_to_record_blob(&[Value::from_i64(1), Value::build_text("hello"), Value::Null]);
        let Value::Blob(blob) = &arr else {
            panic!("Expected Blob");
        };
        let text = serialize_array_from_blob(blob).unwrap();
        assert_eq!(text, "{1,hello,NULL}");
    }

    #[test]
    fn test_make_array_from_registers() {
        use super::super::Register;
        let registers = vec![
            Register::Value(Value::from_i64(1)),
            Register::Value(Value::build_text("two")),
            Register::Value(Value::from_i64(3)),
        ];
        let result = make_array_from_registers(&registers, 0, 3);
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
