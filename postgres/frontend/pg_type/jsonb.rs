//! PostgreSQL's input rules for jsonb. A jsonb value is the jsonb of core,
//! which takes any JSON. A cast of text to jsonb checks the text by
//! PostgreSQL's rules first.

use crate::functions::{invalid_json, unsupported_unicode_escape};
use turso_core::{Result, Value};

/// The operand of a cast to jsonb: text has to be JSON, as for PostgreSQL's
/// jsonb_in(). The text itself is the value. A value that is not text is
/// left for the cast.
// FIXME(pg-catalog): PostgreSQL reads the text with the input function of
// jsonb (pg_type.typinput). The translator calls this function in front of
// a cast to jsonb, and a column of jsonb does not call it. A jsonb type of
// the frontend, as pg_type/uuid.rs is for uuid, would replace it.
pub(crate) fn jsonb_in(value: &Value) -> Result<Value> {
    if let Value::Text(text) = value {
        let json = text.as_str();
        serde_json::from_str::<serde::de::IgnoredAny>(json).map_err(|_| invalid_json())?;
        // Ignoring the value does not decode the escapes, so a lone UTF-16
        // surrogate gets through.
        if has_lone_surrogate_escape(json) {
            return Err(invalid_json());
        }
        if has_nul_escape(json) {
            return Err(unsupported_unicode_escape());
        }
    }
    Ok(value.clone())
}

/// Whether JSON text `json`, which is valid JSON apart from its `\u`
/// escapes, has the escape of a UTF-16 surrogate that is not one of a pair:
/// a high surrogate without a low one right after it, or a low one alone.
fn has_lone_surrogate_escape(json: &str) -> bool {
    let mut bytes = json.bytes();
    let unicode_escape = |bytes: &mut std::str::Bytes<'_>| -> Option<u32> {
        let hex: Vec<u8> = bytes.by_ref().take(4).collect();
        u32::from_str_radix(std::str::from_utf8(&hex).ok()?, 16).ok()
    };
    let mut expecting_low_surrogate = false;
    while let Some(byte) = bytes.next() {
        let code = match (byte, expecting_low_surrogate) {
            (b'\\', _) => match bytes.next() {
                Some(b'u') => unicode_escape(&mut bytes),
                _ => None,
            },
            _ => None,
        };
        let is_high = code.is_some_and(|code| (0xD800..=0xDBFF).contains(&code));
        let is_low = code.is_some_and(|code| (0xDC00..=0xDFFF).contains(&code));
        if is_low != expecting_low_surrogate {
            return true;
        }
        expecting_low_surrogate = is_high;
    }
    expecting_low_surrogate
}

/// Whether JSON text `json` has the escape of the NUL character, which
/// PostgreSQL's jsonb cannot store.
pub(crate) fn has_nul_escape(json: &str) -> bool {
    let mut bytes = json.bytes();
    while let Some(byte) = bytes.next() {
        if byte == b'\\' && bytes.next() == Some(b'u') && bytes.by_ref().take(4).eq(*b"0000") {
            return true;
        }
    }
    false
}
