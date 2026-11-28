//! PostgreSQL's regclass type: the oid of a relation.

use super::oid::oid_input;
use crate::error::sqlstate_error;
use turso_core::{Numeric, Result, Value};
use turso_pg_parser::sqlstate::{
    SQLSTATE_FEATURE_NOT_SUPPORTED, SQLSTATE_INVALID_NAME, SQLSTATE_SYNTAX_ERROR,
};

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
