//! PostgreSQL's boolean type.

use crate::error::sqlstate_error;
use crate::functions::parse_pg_bool;
use turso_core::ast;
use turso_core::types::{Conversion, StorageClass, Type};
use turso_core::{Numeric, Result, Value};
use turso_pg_parser::sqlstate::{SQLSTATE_DATATYPE_MISMATCH, SQLSTATE_INVALID_TEXT_REPRESENTATION};

/// PostgreSQL's boolean. A value is stored as the integer 1 for true and 0
/// for false, as the boolean of core stores it. It differs from that type in
/// the text that it reads.
#[derive(Debug)]
pub(super) struct Boolean;

impl Type for Boolean {
    fn name(&self) -> &str {
        "boolean"
    }

    fn aliases(&self) -> &[&str] {
        &["bool"]
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

    fn encode(&self) -> Option<Conversion> {
        Some(Conversion::Function(bool_encode))
    }

    fn decode(&self) -> Option<Conversion> {
        Some(Conversion::Function(bool_decode))
    }

    fn cast(&self) -> Option<Conversion> {
        Some(Conversion::Function(bool_cast))
    }

    /// `OPERATOR '<'`: false sorts before true, as 0 sorts before 1.
    fn operators(&self) -> Vec<ast::TypeOperator> {
        vec![ast::TypeOperator {
            op: "<".to_string(),
            func_name: None,
        }]
    }

    fn reads_as(&self) -> Option<StorageClass> {
        Some(StorageClass::Integer)
    }

    /// FIXME(pg-catalog): This hand-codes that the comparison operators and the other conditions return boolean, which is `oprresult` of each operator in pg_operator. A lookup of the operator in a pg_operator table replaces this.
    /// A comparison is a boolean in PostgreSQL.
    fn is_type_of_conditions(&self) -> bool {
        true
    }
}

/// Text is read as PostgreSQL's boolean input does (see `parse_pg_bool`). A
/// boolean value is the integer 1 or 0 already.
fn bool_encode(value: &Value, _params: &[Value]) -> Result<Value> {
    match value {
        Value::Numeric(Numeric::Integer(0 | 1)) => Ok(value.clone()),
        Value::Text(text) => match parse_pg_bool(text.as_str()) {
            Some(truth) => Ok(Value::from_i64(i64::from(truth))),
            None => Err(sqlstate_error(
                SQLSTATE_INVALID_TEXT_REPRESENTATION,
                format!(
                    "invalid input syntax for type boolean: \"{}\"",
                    text.as_str()
                ),
                None,
            )),
        },
        other => Err(sqlstate_error(
            SQLSTATE_DATATYPE_MISMATCH,
            format!("the value {other} is not of type boolean"),
            None,
        )),
    }
}

/// FIXME(pg-catalog): This hand-codes the pg_cast entry from integer to boolean (function bool(integer)). pg_cast has no entry from smallint or bigint to boolean, but this cast applies to every integer value. A lookup in a pg_cast table replaces this.
/// PostgreSQL casts an integer to boolean, but does not store one in a boolean
/// column: every nonzero integer is true in a cast. Other values are left
/// as they are, and `bool_encode` reads them as it does for a column.
fn bool_cast(value: &Value, _params: &[Value]) -> Result<Value> {
    match value {
        Value::Numeric(Numeric::Integer(integer)) => Ok(Value::from_i64(i64::from(*integer != 0))),
        other => Ok(other.clone()),
    }
}

/// 1 for a stored value that is true as a condition and 0 for another, as
/// the boolean of core reads it.
fn bool_decode(value: &Value, _params: &[Value]) -> Result<Value> {
    Ok(Value::from_i64(i64::from(value.exec_if(false, false))))
}
