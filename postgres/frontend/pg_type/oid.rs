//! PostgreSQL's oid type.

use super::integer::{integer_text_out_of_range, read_integer_text, IntegerSpelling};
use crate::error::sqlstate_error;
use turso_core::ast;
use turso_core::types::{Conversion, StorageClass, TextInput, Type};
use turso_core::{Numeric, Result, Value};
use turso_pg_parser::sqlstate::SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE;

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

    fn input(&self) -> Option<TextInput> {
        Some(oid_input)
    }

    fn coerce(&self) -> Option<Conversion> {
        Some(Conversion::Function(oid_coerce))
    }

    fn reads_as(&self) -> Option<StorageClass> {
        Some(StorageClass::Integer)
    }
}

/// The oid that `text` is, with the rules of PostgreSQL's oid input. A
/// negative number from -2147483648 up wraps around, so -1 is 4294967295.
pub(crate) fn oid_input(text: &str) -> Result<Value> {
    let number = read_integer_text(text, "oid", IntegerSpelling::LeadingZeroIsOctal, u64::MAX)?;
    let oid = match (number.is_negative, number.magnitude) {
        (_, 0) => 0,
        (true, magnitude @ 1..=0x8000_0000) => (1 << 32) - magnitude as i64,
        (false, magnitude @ 1..=0xFFFF_FFFF) => magnitude as i64,
        _ => return Err(integer_text_out_of_range(text, "oid")),
    };
    Ok(Value::from_i64(oid))
}

pub(super) fn oid_coerce(value: &Value, _params: &[Value]) -> Result<Value> {
    check_oid_range(value)
}

/// `value` when it is not an integer outside the range of oid, which fails
/// with 22003.
pub(crate) fn check_oid_range(value: &Value) -> Result<Value> {
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
