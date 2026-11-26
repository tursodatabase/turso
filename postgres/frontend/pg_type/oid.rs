//! PostgreSQL's oid type.

use super::split_sign;
use crate::error::sqlstate_error;
use turso_core::ast;
use turso_core::types::{Conversion, StorageClass, Type};
use turso_core::{Numeric, Result, Value};
use turso_pg_parser::sqlstate::{
    SQLSTATE_INVALID_TEXT_REPRESENTATION, SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE,
};

/// PostgreSQL's oid: a number from 0 to 4294967295, stored as that integer.
#[derive(Debug)]
pub(super) struct Oid;

impl Type for Oid {
    fn name(&self) -> &str {
        "oid"
    }

    fn params(&self) -> Vec<ast::TypeParam> {
        vec![ast::TypeParam {
            name: "value".to_string(),
            ty: Some("any".to_string()),
        }]
    }

    fn base(&self) -> &str {
        "integer"
    }

    fn coerce(&self) -> Option<Conversion> {
        Some(Conversion::Function(oid_coerce))
    }

    fn reads_as(&self) -> Option<StorageClass> {
        Some(StorageClass::Integer)
    }
}

/// The number that `text` is as input of oid, with the rules of PostgreSQL's
/// oid input: white space around the number, a sign, and decimal, hex (`0x`)
/// or octal (leading `0`) digits. A negative number from -2147483648 up wraps
/// around, so -1 is 4294967295.
pub(crate) fn parse_oid(text: &str) -> Result<i64> {
    let invalid = || {
        sqlstate_error(
            SQLSTATE_INVALID_TEXT_REPRESENTATION,
            format!("invalid input syntax for type oid: \"{text}\""),
            None,
        )
    };
    let out_of_range = || {
        sqlstate_error(
            SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE,
            format!("value \"{text}\" is out of range for type oid"),
            None,
        )
    };
    let (negative, unsigned) = split_sign(text);
    let (radix, digits) = match unsigned.as_bytes() {
        [b'0', b'x' | b'X', rest @ ..] if rest.first().is_some_and(u8::is_ascii_hexdigit) => {
            (16, &unsigned[2..])
        }
        [b'0', _, ..] => (8, &unsigned[1..]),
        _ => (10, unsigned),
    };
    if digits.is_empty() || !digits.chars().all(|c| c.is_digit(radix)) {
        return Err(invalid());
    }
    let magnitude = u64::from_str_radix(digits, radix).map_err(|_| out_of_range())?;
    match (negative, magnitude) {
        (_, 0) => Ok(0),
        (true, 1..=0x8000_0000) => Ok((1 << 32) - magnitude as i64),
        (false, 1..=0xFFFF_FFFF) => Ok(magnitude as i64),
        _ => Err(out_of_range()),
    }
}

fn oid_coerce(value: &Value, _params: &[Value]) -> Result<Value> {
    match value {
        Value::Numeric(Numeric::Integer(0..=0xFFFF_FFFF)) => Ok(value.clone()),
        Value::Numeric(Numeric::Integer(_)) => Err(sqlstate_error(
            SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE,
            "OID out of range",
            None,
        )),
        other => Ok(other.clone()),
    }
}
