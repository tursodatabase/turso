//! PostgreSQL's regclass type: the oid of a relation.

use super::oid::{oid_coerce, oid_input};
use crate::error::sqlstate_error;
use turso_core::ast;
use turso_core::types::{Conversion, StorageClass, TextInput, Type};
use turso_core::{LimboError, Numeric, Result, Value};
use turso_pg_parser::sqlstate::{
    SQLSTATE_FEATURE_NOT_SUPPORTED, SQLSTATE_INVALID_NAME, SQLSTATE_SYNTAX_ERROR,
};

/// A regclass value is the oid of a relation, stored as that integer.
#[derive(Debug)]
pub(super) struct RegClass;

impl Type for RegClass {
    fn name(&self) -> &str {
        "regclass"
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
        Some(regclass_input)
    }

    fn coerce(&self) -> Option<Conversion> {
        Some(Conversion::Function(oid_coerce))
    }

    fn reads_as(&self) -> Option<StorageClass> {
        Some(StorageClass::Integer)
    }
}

/// The oid of the relation that `text` names, as the input function of
/// regclass reads it. It looks the relation up, so it needs a connection.
fn regclass_input(
    connection: Option<&std::sync::Arc<turso_core::Connection>>,
    text: &str,
) -> Result<Value> {
    let connection = connection.ok_or_else(|| {
        LimboError::InternalError("the input of regclass needs a connection".to_string())
    })?;
    crate::catalog::regclass_oid_of_text(connection, text).map(Value::from_i64)
}

/// What the text of a regclass says.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum RegClassInput {
    Oid(i64),
    Name {
        schema: Option<String>,
        name: String,
    },
}

/// Reads `text` as PostgreSQL's regclass input does: digits only are an oid,
/// and anything else is a relation name with an optional schema, where an
/// unquoted part is lowercase and a quoted part keeps its case.
pub(crate) fn parse_regclass(text: &str) -> Result<RegClassInput> {
    if text == "-" {
        return Ok(RegClassInput::Oid(0));
    }
    if !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit()) {
        return match oid_input(text)? {
            Value::Numeric(Numeric::Integer(oid)) => Ok(RegClassInput::Oid(oid)),
            other => unreachable!("oid input gives an integer, not {other:?}"),
        };
    }
    let mut parts = split_name(text)?;
    match parts.len() {
        1 => Ok(RegClassInput::Name {
            schema: None,
            name: parts.remove(0),
        }),
        2 => {
            let name = parts.remove(1);
            Ok(RegClassInput::Name {
                schema: Some(parts.remove(0)),
                name,
            })
        }
        3 => Err(sqlstate_error(
            SQLSTATE_FEATURE_NOT_SUPPORTED,
            format!("cross-database references are not implemented: \"{text}\""),
            None,
        )),
        _ => Err(sqlstate_error(
            SQLSTATE_SYNTAX_ERROR,
            format!("improper relation name (too many dotted names): {text}"),
            None,
        )),
    }
}

/// The identifiers of a dotted name, as PostgreSQL's SplitIdentifierString
/// reads them.
fn split_name(text: &str) -> Result<Vec<String>> {
    match turso_pg_parser::split_identifier_string(text, '.') {
        Some(names) if !names.is_empty() => Ok(names),
        _ => Err(sqlstate_error(
            SQLSTATE_INVALID_NAME,
            "invalid name syntax",
            None,
        )),
    }
}
