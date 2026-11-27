//! PostgreSQL's uuid type.

use crate::error::sqlstate_error;
use turso_core::ast;
use turso_core::types::{Conversion, StorageClass, Type};
use turso_core::{Result, Value};
use turso_pg_parser::sqlstate::{SQLSTATE_DATATYPE_MISMATCH, SQLSTATE_INVALID_TEXT_REPRESENTATION};

/// PostgreSQL's uuid. A value is stored as 16 bytes, as the uuid of core
/// stores it. It differs from that type in the text that it reads: core
/// reads text with `uuid::Uuid::parse_str`, which accepts `urn:uuid:...` and
/// refuses a hyphen after any group of four digits, as PostgreSQL does not.
/// A cast to uuid reads text as a column does, so the two agree.
#[derive(Debug)]
pub(super) struct Uuid;

impl Type for Uuid {
    fn name(&self) -> &str {
        "uuid"
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
        Some(Conversion::Function(uuid_encode))
    }

    fn decode(&self) -> Option<Conversion> {
        Some(Conversion::Function(uuid_decode))
    }

    /// `OPERATOR '<'`: the stored bytes compare as PostgreSQL compares
    /// uuids.
    fn operators(&self) -> Vec<ast::TypeOperator> {
        vec![ast::TypeOperator {
            op: "<".to_string(),
            func_name: None,
        }]
    }

    fn reads_as(&self) -> Option<StorageClass> {
        Some(StorageClass::Text)
    }
}

/// Text is read as PostgreSQL's uuid input does (see `parse_uuid`). A blob of
/// 16 bytes is a uuid value already.
fn uuid_encode(value: &Value, _params: &[Value]) -> Result<Value> {
    match value {
        Value::Blob(blob) if blob.len() == 16 => Ok(value.clone()),
        Value::Text(text) => {
            let text = text.as_str();
            let bytes = parse_uuid(text).ok_or_else(|| {
                sqlstate_error(
                    SQLSTATE_INVALID_TEXT_REPRESENTATION,
                    format!("invalid input syntax for type uuid: \"{text}\""),
                    None,
                )
            })?;
            Ok(Value::from_slice(&bytes)?)
        }
        other => Err(sqlstate_error(
            SQLSTATE_DATATYPE_MISMATCH,
            format!("the value {other} is not of type uuid"),
            None,
        )),
    }
}

/// The stored 16 bytes in the usual text form, such as
/// `a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11`. NULL for a stored value that is
/// not 16 bytes.
fn uuid_decode(value: &Value, _params: &[Value]) -> Result<Value> {
    let Value::Blob(blob) = value else {
        return Ok(Value::Null);
    };
    Ok(match uuid::Uuid::from_slice(blob) {
        Ok(uuid) => Value::build_text(uuid.hyphenated().to_string()),
        Err(_) => Value::Null,
    })
}

/// The bytes of the UUID that `text` spells, by the rules of PostgreSQL's
/// string_to_uuid(): 32 hexadecimal digits, with an optional hyphen after
/// any group of four digits, optionally in braces.
///
/// `uuid::Uuid::parse_str` is not used because it follows other rules: it
/// accepts `urn:uuid:...` and refuses a hyphen after a group of four digits
/// that is not at one of the usual places.
pub(crate) fn parse_uuid(text: &str) -> Option<[u8; 16]> {
    let mut rest = text.as_bytes();
    let braces = rest.first() == Some(&b'{');
    if braces {
        rest = &rest[1..];
    }
    let mut bytes = [0u8; 16];
    for (i, byte) in bytes.iter_mut().enumerate() {
        let [high, low, tail @ ..] = rest else {
            return None;
        };
        let digit = |c: &u8| (*c as char).to_digit(16);
        *byte = (digit(high)? * 16 + digit(low)?) as u8;
        rest = tail;
        if rest.first() == Some(&b'-') && i % 2 == 1 && i < 15 {
            rest = &rest[1..];
        }
    }
    if braces {
        let [b'}', tail @ ..] = rest else {
            return None;
        };
        rest = tail;
    }
    rest.is_empty().then_some(bytes)
}
