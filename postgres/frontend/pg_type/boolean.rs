//! PostgreSQL's boolean type.

use super::{FormatName, RegisteredType};
use crate::error::sqlstate_error;
use crate::functions::parse_pg_bool;
use turso_core::ast;
use turso_core::types::{Conversion, StorageClass, TextInput, Type};
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

    fn input(&self) -> Option<TextInput> {
        Some(|_, text| bool_input(text))
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

    /// FIXME(pg-catalog): This hand-codes two things. One is `oprresult` of the comparison and pattern operators, which pg_operator now lists for some types only: the engine asks for the type of a condition with a schema in hand, not with the operator and its operand types, and a comparison of types without rows (arrays, jsonb, a subquery, a catalog column) would have no type. The other is the type of AND, OR, NOT, the IS tests, IN, BETWEEN and EXISTS, which are not operators: PostgreSQL's parser gives them the type boolean itself. A result type in the operator the dialect chooses, for every type, replaces the first.
    /// A comparison is a boolean in PostgreSQL.
    fn is_type_of_conditions(&self) -> bool {
        true
    }
}

/// The boolean that `text` is, as PostgreSQL's boolean input reads it (see
/// `parse_pg_bool`).
fn bool_input(text: &str) -> Result<Value> {
    match parse_pg_bool(text) {
        Some(truth) => Ok(Value::from_i64(i64::from(truth))),
        None => Err(sqlstate_error(
            SQLSTATE_INVALID_TEXT_REPRESENTATION,
            format!("invalid input syntax for type boolean: \"{text}\""),
            None,
        )),
    }
}

/// FIXME(pg-catalog): The cast table of casts.rs reads a string literal with the INPUT before the value gets here. Text still gets here from a value whose type core cannot name, such as the result of most functions, and from the elements of an array, and it is read with the INPUT too. A typed expression tree lets the cast table refuse or convert these, and then a boolean stores only 1 and 0 and needs no ENCODE.
/// A boolean value is the integer 1 or 0 already.
fn bool_encode(value: &Value, _params: &[Value]) -> Result<Value> {
    match value {
        Value::Numeric(Numeric::Integer(0 | 1)) => Ok(value.clone()),
        Value::Text(text) => bool_input(text.as_str()),
        other => Err(sqlstate_error(
            SQLSTATE_DATATYPE_MISMATCH,
            format!("the value {other} is not of type boolean"),
            None,
        )),
    }
}

/// FIXME(pg-catalog): This hand-codes the pg_cast entry from integer to boolean (function bool(integer)) for every integer value. The cast table of casts.rs decides a cast to boolean from a value whose type core can name, and refuses smallint and bigint. This function still decides for a value with no known type, such as the result of most functions: `length('ab')::boolean` is true here, as in PostgreSQL, and so is a bigint function result, which PostgreSQL refuses. A typed expression tree, where every expression has a registered type, lets the cast table decide these too and removes this function.
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

impl RegisteredType for Boolean {
    fn format_name(&self) -> FormatName {
        FormatName::Plain("boolean")
    }
}
