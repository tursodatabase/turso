//! PostgreSQL's regtype type: the oid of a type.

use super::oid::oid_input;
use super::{built_in_type, lookup_type_name, type_format_name, NamedTypeKind};
use crate::catalog::{custom_type_name, user_type_oid};
use crate::error::not_supported;
use std::sync::Arc;
use turso_core::{Connection, Numeric, Result, Value};

/// The oid that the text of a regtype names: `-` is 0, digits are an oid,
/// and anything else is a type name, as a statement writes one, which must
/// exist. A type modifier is read and dropped.
pub(super) fn regtype_oid_of_text(conn: &Arc<Connection>, text: &str) -> Result<i64> {
    if text == "-" {
        return Ok(0);
    }
    if !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit()) {
        return match oid_input(text)? {
            Value::Numeric(Numeric::Integer(oid)) => Ok(oid),
            other => unreachable!("oid input gives an integer, not {other:?}"),
        };
    }
    let named = lookup_type_name(conn, text)?;
    let element_oid = match named.kind {
        NamedTypeKind::BuiltIn { oid, .. } => oid,
        NamedTypeKind::Custom => user_type_oid(&named.name),
        NamedTypeKind::RowOfRelation => {
            return Err(not_supported(format!(
                "a regtype of the row type of the relation \"{}\"",
                named.written_name
            )))
        }
    };
    if named.array_dimensions == 0 {
        return Ok(element_oid);
    }
    built_in_type(element_oid)
        .map(|ty| ty.array_oid)
        .filter(|array_oid| *array_oid != 0)
        .ok_or_else(|| {
            not_supported(format!(
                "a regtype of the array of the type \"{}\"",
                named.written_name
            ))
        })
}

/// The text of the regtype value `oid`: the name of the type as `format_type`
/// prints it, the number when no type has that oid, and `-` for 0.
pub(crate) fn regtype_text(conn: &Connection, oid: i64) -> String {
    if oid == 0 {
        return "-".to_string();
    }
    if let Some(name) = type_format_name(oid, -1) {
        return name;
    }
    custom_type_name(conn, oid).unwrap_or_else(|| oid.to_string())
}
