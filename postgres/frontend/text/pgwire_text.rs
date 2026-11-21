// Vendored from pgwire 0.36.3:
// src/types/to_sql_text.rs (bool, i64, &[u8])
// src/types/format/float_digits.rs (ExtraFloatDigits::format_f64)
// Copyright (c) 2018 Ning Sun. See licenses/pgwire-mit-license.md.
// Adaptations: functions return the text instead of writing it to a BytesMut;
// only the default hex bytea and shortest float formats; an array flag instead
// of postgres_types::Type; infallible functions instead of the ToSqlText trait
// and IsNull results.

pub(super) fn encode_bool(value: bool) -> &'static str {
    if value {
        "t"
    } else {
        "f"
    }
}

pub(super) fn encode_integer(value: i64) -> String {
    value.to_string()
}

pub(super) fn format_f64(value: f64) -> String {
    if value.is_nan() {
        return "NaN".to_string();
    }
    if value.is_infinite() {
        return if value.is_sign_positive() {
            "inf".to_string()
        } else {
            "-inf".to_string()
        };
    }

    if value == 0.0 {
        return "0".to_string();
    }

    let mut buffer = ryu::Buffer::new();
    buffer.format(value).to_string()
}

/// An array literal such as `{0.1,{0.2,NULL}}` with each element that is not
/// NULL written by `write`, which reads the element as it came from the engine.
pub(super) fn map_array_elements(array: &str, write: impl Fn(&str) -> String) -> String {
    let mut out = String::with_capacity(array.len());
    let mut element = String::new();
    let flush = |element: &mut String, out: &mut String| {
        if element.is_empty() || element == "NULL" {
            out.push_str(element);
        } else {
            out.push_str(&write(element));
        }
        element.clear();
    };
    for c in array.chars() {
        if matches!(c, '{' | '}' | ',') {
            flush(&mut element, &mut out);
            out.push(c);
        } else {
            element.push(c);
        }
    }
    flush(&mut element, &mut out);
    out
}

pub(crate) fn encode_bytea(value: &[u8], array: bool) -> String {
    let mut out = String::new();
    // for a bytea_array, the bytes are enclosed in quotes,
    // and \x is escaped
    if array {
        out.push_str("\"\\");
    }

    out.push_str("\\x");
    out.push_str(&hex::encode(value));

    if array {
        out.push('"');
    }
    out
}
