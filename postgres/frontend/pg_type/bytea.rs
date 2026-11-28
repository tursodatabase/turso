//! PostgreSQL's bytea type.

use super::{FormatName, RegisteredType};
use crate::error::sqlstate_error;
use turso_core::ast;
use turso_core::types::{StorageClass, TextInput, Type};
use turso_core::{LimboError, Result, Value};
use turso_pg_parser::sqlstate::{
    SQLSTATE_INVALID_PARAMETER_VALUE, SQLSTATE_INVALID_TEXT_REPRESENTATION,
};

/// PostgreSQL's bytea: bytes, stored as a blob. The bytes compare one by one
/// and then by length, in the stored form as well as in the value.
#[derive(Debug)]
pub(super) struct Bytea;

impl Type for Bytea {
    fn name(&self) -> &str {
        "bytea"
    }

    fn params(&self) -> Vec<ast::TypeParam> {
        vec![ast::TypeParam {
            name: "value".to_string(),
            ty: Some("any".to_string()),
        }]
    }

    fn base(&self) -> &str {
        "blob"
    }

    fn input(&self) -> Option<TextInput> {
        Some(|_, text| bytea_input(text))
    }

    fn operators(&self) -> Vec<ast::TypeOperator> {
        vec![ast::TypeOperator {
            op: "<".to_string(),
            func_name: None,
        }]
    }

    fn stored_form_sorts_in_value_order(&self) -> bool {
        true
    }

    fn reads_as(&self) -> Option<StorageClass> {
        Some(StorageClass::Blob)
    }
}

/// The bytes that `text` is as input of bytea, with the rules of PostgreSQL's
/// byteain. Text that starts with `\x` is hex digits in pairs, with a space,
/// tab, newline or carriage return allowed before a pair. Other text is the escape format, where `\\`
/// is a backslash and a backslash with three octal digits up to `\377` is a
/// byte.
fn bytea_input(text: &str) -> Result<Value> {
    let bytes = match text.strip_prefix("\\x") {
        Some(hex) => read_hex(hex)?,
        None => read_escape_format(text)?,
    };
    Ok(Value::from_blob(bytes))
}

impl RegisteredType for Bytea {
    fn format_name(&self) -> FormatName {
        FormatName::Plain("bytea")
    }
}

fn read_hex(hex: &str) -> Result<Vec<u8>> {
    let bytes = hex.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() / 2);
    let mut at = 0;
    loop {
        while at < bytes.len() && matches!(bytes[at], b' ' | b'\t' | b'\n' | b'\r') {
            at += 1;
        }
        if at == bytes.len() {
            return Ok(out);
        }
        let high = hex_digit(hex, at)?;
        at += 1;
        if at == bytes.len() {
            return Err(sqlstate_error(
                SQLSTATE_INVALID_PARAMETER_VALUE,
                "invalid hexadecimal data: odd number of digits",
                None,
            ));
        }
        let low = hex_digit(hex, at)?;
        at += 1;
        out.push((high << 4) | low);
    }
}

fn hex_digit(hex: &str, at: usize) -> Result<u8> {
    match hex.as_bytes()[at] {
        byte @ b'0'..=b'9' => Ok(byte - b'0'),
        byte @ b'a'..=b'f' => Ok(byte - b'a' + 10),
        byte @ b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => {
            let bad = hex[at..].chars().next().unwrap_or_default();
            Err(sqlstate_error(
                SQLSTATE_INVALID_PARAMETER_VALUE,
                format!("invalid hexadecimal digit: \"{bad}\""),
                None,
            ))
        }
    }
}

fn read_escape_format(text: &str) -> Result<Vec<u8>> {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut at = 0;
    while at < bytes.len() {
        if bytes[at] != b'\\' {
            out.push(bytes[at]);
            at += 1;
            continue;
        }
        match bytes.get(at + 1..at + 4) {
            Some([first @ b'0'..=b'3', second @ b'0'..=b'7', third @ b'0'..=b'7']) => {
                out.push(((first - b'0') << 6) | ((second - b'0') << 3) | (third - b'0'));
                at += 4;
            }
            _ if bytes.get(at + 1) == Some(&b'\\') => {
                out.push(b'\\');
                at += 2;
            }
            _ => return Err(invalid_escape_format()),
        }
    }
    Ok(out)
}

fn invalid_escape_format() -> LimboError {
    sqlstate_error(
        SQLSTATE_INVALID_TEXT_REPRESENTATION,
        "invalid input syntax for type bytea",
        None,
    )
}

#[cfg(test)]
mod tests {
    use super::bytea_input;
    use turso_core::Value;

    fn read(text: &str) -> Result<Vec<u8>, String> {
        match bytea_input(text) {
            Ok(Value::Blob(bytes)) => Ok(bytes),
            Ok(other) => panic!("{other} is not a blob"),
            Err(error) => Err(error.to_string()),
        }
    }

    #[test]
    fn hex_text_is_pairs_of_digits_in_either_case() {
        assert_eq!(read("\\xDEADbeef"), Ok(vec![0xde, 0xad, 0xbe, 0xef]));
        assert_eq!(read("\\x"), Ok(vec![]));
    }

    #[test]
    fn hex_text_allows_white_space_between_pairs_only() {
        assert_eq!(read("\\x de ad\n\tbe"), Ok(vec![0xde, 0xad, 0xbe]));
        assert!(read("\\xa b").unwrap_err().contains("digit: \" \""));
    }

    #[test]
    fn hex_text_does_not_skip_a_form_feed() {
        assert!(read("\\xab\u{c}cd")
            .unwrap_err()
            .contains("digit: \"\u{c}\""));
    }

    #[test]
    fn hex_text_refuses_an_odd_digit_count_and_a_bad_digit() {
        assert!(read("\\xabc").unwrap_err().contains("odd number of digits"));
        assert!(read("\\xabzz").unwrap_err().contains("digit: \"z\""));
        assert!(read("\\x\u{20ac}a")
            .unwrap_err()
            .contains("digit: \"\u{20ac}\""));
    }

    #[test]
    fn escape_text_reads_bytes_octal_and_backslash() {
        assert_eq!(read("abc"), Ok(b"abc".to_vec()));
        assert_eq!(read("a\\101\\000\\377"), Ok(vec![b'a', 0x41, 0, 0xff]));
        assert_eq!(read("a\\\\b"), Ok(b"a\\b".to_vec()));
        assert_eq!(read("\u{e9}"), Ok("\u{e9}".as_bytes().to_vec()));
    }

    #[test]
    fn escape_text_refuses_a_bad_backslash() {
        for text in ["\\400", "a\\12", "abc\\", "\\XAB", "\\8"] {
            assert!(read(text).unwrap_err().contains("type bytea"), "{text}");
        }
    }
}
