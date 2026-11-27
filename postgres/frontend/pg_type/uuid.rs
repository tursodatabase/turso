//! PostgreSQL's uuid type.

use crate::error::sqlstate_error;
use turso_core::ast;
use turso_core::types::{Conversion, StorageClass, TextInput, Type};
use turso_core::{Result, Value};
use turso_pg_parser::sqlstate::SQLSTATE_INVALID_TEXT_REPRESENTATION;

const UUID_LEN: usize = 16;

/// PostgreSQL's uuid. A value is stored as its 16 bytes and read as text in
/// the canonical form. It differs from the uuid of core in the text that it
/// reads.
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

    fn input(&self) -> Option<TextInput> {
        Some(uuid_input)
    }

    fn encode(&self) -> Option<Conversion> {
        Some(Conversion::Function(uuid_encode))
    }

    fn decode(&self) -> Option<Conversion> {
        Some(Conversion::Function(uuid_decode))
    }

    /// `OPERATOR '<'`: the stored bytes sort as `uuid_cmp` orders the values.
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

/// The 16 bytes of the uuid that `text` is, as `uuid_in` reads it.
fn uuid_input(text: &str) -> Result<Value> {
    match string_to_uuid(text) {
        Some(bytes) => Ok(Value::from_slice(&bytes)?),
        None => Err(invalid_uuid(text)),
    }
}

/// A blob of 16 bytes is stored as it is. Text is read with the INPUT.
fn uuid_encode(value: &Value, _params: &[Value]) -> Result<Value> {
    match value {
        Value::Blob(blob) if blob.len() == UUID_LEN => Ok(value.clone()),
        Value::Text(text) => uuid_input(text.as_str()),
        other => Err(invalid_uuid(&other.to_string())),
    }
}

/// NULL for a stored value that is not 16 bytes.
fn uuid_decode(value: &Value, _params: &[Value]) -> Result<Value> {
    Ok(match value {
        Value::Blob(blob) => match <[u8; UUID_LEN]>::try_from(blob.as_slice()) {
            Ok(bytes) => Value::build_text(uuid_to_string(&bytes)),
            Err(_) => Value::Null,
        },
        _ => Value::Null,
    })
}

fn invalid_uuid(text: &str) -> turso_core::LimboError {
    sqlstate_error(
        SQLSTATE_INVALID_TEXT_REPRESENTATION,
        format!("invalid input syntax for type uuid: \"{text}\""),
        None,
    )
}

/// `string_to_uuid` of PostgreSQL: 32 hexadecimal digits, with an optional
/// hyphen after each group of 4 digits except the last, and optionally
/// inside braces.
fn string_to_uuid(source: &str) -> Option<[u8; UUID_LEN]> {
    let src = source.as_bytes();
    let mut pos = 0;
    let braces = src.first() == Some(&b'{');
    if braces {
        pos += 1;
    }

    let mut uuid = [0u8; UUID_LEN];
    for (i, byte) in uuid.iter_mut().enumerate() {
        let (Some(&high), Some(&low)) = (src.get(pos), src.get(pos + 1)) else {
            return None;
        };
        let high = char::from(high).to_digit(16)?;
        let low = char::from(low).to_digit(16)?;
        *byte = (high * 16 + low) as u8;
        pos += 2;
        if src.get(pos) == Some(&b'-') && i % 2 == 1 && i < UUID_LEN - 1 {
            pos += 1;
        }
    }

    if braces {
        if src.get(pos) != Some(&b'}') {
            return None;
        }
        pos += 1;
    }

    (pos == src.len()).then_some(uuid)
}

/// `uuid_out` of PostgreSQL: groups of 8, 4, 4, 4 and 12 lowercase digits
/// with a hyphen between them.
fn uuid_to_string(uuid: &[u8; UUID_LEN]) -> String {
    const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";
    let mut text = String::with_capacity(2 * UUID_LEN + 4);
    for (i, byte) in uuid.iter().enumerate() {
        if matches!(i, 4 | 6 | 8 | 10) {
            text.push('-');
        }
        text.push(char::from(HEX_CHARS[usize::from(byte >> 4)]));
        text.push(char::from(HEX_CHARS[usize::from(byte & 0x0F)]));
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    const BYTES: [u8; UUID_LEN] = [
        0xa0, 0xee, 0xbc, 0x99, 0x9c, 0x0b, 0x4e, 0xf8, 0xbb, 0x6d, 0x6b, 0xb9, 0xbd, 0x38, 0x0a,
        0x11,
    ];

    #[test]
    fn reads_the_forms_that_postgres_reads() {
        for text in [
            "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
            "A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11",
            "{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}",
            "a0eebc999c0b4ef8bb6d6bb9bd380a11",
            "{a0eebc999c0b4ef8bb6d6bb9bd380a11}",
            "a0ee-bc99-9c0b-4ef8-bb6d-6bb9-bd38-0a11",
            "a0eebc99-9c0b4ef8-bb6d6bb9-bd380a11",
            "a0eebc99-9c0b-4ef8bb6d-6bb9bd380a11",
        ] {
            assert_eq!(string_to_uuid(text), Some(BYTES), "{text}");
        }
    }

    #[test]
    fn refuses_the_forms_that_postgres_refuses() {
        for text in [
            "",
            "{}",
            " a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
            "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11 ",
            "urn:uuid:a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
            "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a1",
            "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a111",
            "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11-",
            "a0eebc99--9c0b-4ef8-bb6d-6bb9bd380a11",
            "a-0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
            "a0eeb-c999-c0b4-ef8b-b6d6-bb9b-d380-a11",
            "{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
            "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}",
            "{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}}",
            "g0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
            "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a1é",
        ] {
            assert_eq!(string_to_uuid(text), None, "{text:?}");
        }
    }

    #[test]
    fn writes_the_canonical_form() {
        assert_eq!(
            uuid_to_string(&BYTES),
            "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
        );
        assert_eq!(
            uuid_to_string(&[0; UUID_LEN]),
            "00000000-0000-0000-0000-000000000000"
        );
    }

    #[test]
    fn input_gives_the_stored_bytes_and_names_the_text_on_failure() {
        let stored = uuid_input("{A0EEBC999C0B4EF8BB6D6BB9BD380A11}").unwrap();
        assert_eq!(stored, Value::from_slice(&BYTES).unwrap());
        let error = uuid_input("nope").unwrap_err().to_string();
        assert!(error.contains("invalid input syntax for type uuid: \"nope\""));
    }

    #[test]
    fn decode_reads_only_sixteen_bytes() {
        let stored = Value::from_slice(&BYTES).unwrap();
        assert_eq!(
            uuid_decode(&stored, &[]).unwrap(),
            Value::build_text("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
        );
        let short = Value::from_slice(&BYTES[..15]).unwrap();
        assert_eq!(uuid_decode(&short, &[]).unwrap(), Value::Null);
    }
}
