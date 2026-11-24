//! PostgreSQL's bytea type.

use crate::error::sqlstate_error;
use turso_core::ast;
use turso_core::types::{Conversion, StorageClass, Type};
use turso_core::{Result, Value};
use turso_pg_parser::sqlstate::{
    SQLSTATE_DATATYPE_MISMATCH, SQLSTATE_INVALID_PARAMETER_VALUE,
    SQLSTATE_INVALID_TEXT_REPRESENTATION,
};

/// PostgreSQL's bytea. A value is stored as a blob, as the bytea of core
/// stores it. It differs from that type in the text that it reads.
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

    fn encode(&self) -> Option<Conversion> {
        Some(Conversion::Function(bytea_encode))
    }

    fn operators(&self) -> Vec<ast::TypeOperator> {
        vec![ast::TypeOperator {
            op: "<".to_string(),
            func_name: None,
        }]
    }

    fn reads_as(&self) -> Option<StorageClass> {
        Some(StorageClass::Blob)
    }
}

/// Text is read as PostgreSQL's bytea input does (see `parse_pg_bytea`). A
/// number is read as its text, because a parameter sent without a type is
/// bound as a number when its text is one. A blob is a bytea already.
fn bytea_encode(value: &Value, _params: &[Value]) -> Result<Value> {
    match value {
        Value::Blob(_) => Ok(value.clone()),
        Value::Text(text) => match parse_pg_bytea(text.as_str()) {
            Ok(bytes) => Ok(Value::from_blob(bytes)),
            Err(error) => Err(sqlstate_error(error.sqlstate, error.message, None)),
        },
        // FIXME(pg-catalog): typed parameters would let this refuse a number with 42804.
        Value::Numeric(_) => Ok(Value::from_blob(value.to_string().into_bytes())),
        other => Err(sqlstate_error(
            SQLSTATE_DATATYPE_MISMATCH,
            format!("the value {other} is not of type bytea"),
            None,
        )),
    }
}

/// The reason a text is not a bytea.
#[derive(Debug, PartialEq, Eq)]
pub struct ByteaInputError {
    pub sqlstate: &'static str,
    pub message: String,
}

/// Reads the text that PostgreSQL's bytea input reads. Text that starts with
/// `\x` is hex digits in pairs, with whitespace allowed before a pair. Other
/// text is the escape format, where `\\` is a backslash and `\` and three
/// octal digits up to `\377` are a byte.
pub fn parse_pg_bytea(text: &str) -> std::result::Result<Vec<u8>, ByteaInputError> {
    match text.strip_prefix("\\x") {
        Some(hex) => parse_hex(hex),
        None => parse_escape_format(text),
    }
}

fn parse_hex(hex: &str) -> std::result::Result<Vec<u8>, ByteaInputError> {
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
            return Err(ByteaInputError {
                sqlstate: SQLSTATE_INVALID_PARAMETER_VALUE,
                message: "invalid hexadecimal data: odd number of digits".to_string(),
            });
        }
        let low = hex_digit(hex, at)?;
        at += 1;
        out.push((high << 4) | low);
    }
}

fn hex_digit(hex: &str, at: usize) -> std::result::Result<u8, ByteaInputError> {
    match hex.as_bytes()[at] {
        byte @ b'0'..=b'9' => Ok(byte - b'0'),
        byte @ b'a'..=b'f' => Ok(byte - b'a' + 10),
        byte @ b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => {
            let bad = hex[at..].chars().next().unwrap_or_default();
            Err(ByteaInputError {
                sqlstate: SQLSTATE_INVALID_PARAMETER_VALUE,
                message: format!("invalid hexadecimal digit: \"{bad}\""),
            })
        }
    }
}

fn parse_escape_format(text: &str) -> std::result::Result<Vec<u8>, ByteaInputError> {
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
            _ => {
                return Err(ByteaInputError {
                    sqlstate: SQLSTATE_INVALID_TEXT_REPRESENTATION,
                    message: "invalid input syntax for type bytea".to_string(),
                })
            }
        }
    }
    Ok(out)
}
