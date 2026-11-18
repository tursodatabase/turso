//! Reading object names and schema names from libpg_query nodes. A qualified
//! name such as `public.t` is a list of string nodes, one for each part.

use crate::sqlstate::SQLSTATE_INVALID_SCHEMA_NAME;
use crate::ParseError;

pub(crate) fn object_name(
    names: &[pg_query::protobuf::Node],
    allowed_schema: fn(&str) -> bool,
    kind: &str,
) -> Result<String, ParseError> {
    let names = extract_key_columns(names)?;
    if names.is_empty() {
        return Err(ParseError::ParseError(format!("missing {kind} name")));
    }
    unqualified_name(&names, allowed_schema)
        .cloned()
        .ok_or_else(|| match names.as_slice() {
            [schema, _] => ParseError::SchemaQualifiedName {
                kind: kind.to_string(),
                schema: schema.clone(),
            },
            _ => ParseError::not_supported(format!("a schema-qualified {kind} name")),
        })
}

pub(crate) fn extract_key_columns(
    keys: &[pg_query::protobuf::Node],
) -> Result<Vec<String>, ParseError> {
    use pg_query::protobuf::node::Node;

    let mut cols = Vec::new();
    for key_node in keys {
        let Some(Node::String(s)) = &key_node.node else {
            return Err(ParseError::ParseError("expected a name".into()));
        };
        cols.push(s.sval.clone());
    }
    Ok(cols)
}

pub(crate) fn unqualified_name(
    names: &[String],
    allowed_schema: fn(&str) -> bool,
) -> Option<&String> {
    match names {
        [name] => Some(name),
        [schema, name] if allowed_schema(schema) => Some(name),
        _ => None,
    }
}

pub(crate) fn is_pg_catalog(schema: &str) -> bool {
    schema == "pg_catalog"
}

/// Whether `schema` names the schema that types and functions are looked
/// up in anyway: the catalog, or public, where PostgreSQL puts user types.
pub(crate) fn is_default_schema(schema: &str) -> bool {
    map_schema_name(schema) == "main" || schema.eq_ignore_ascii_case("public")
}

/// The schema that a qualifier written in a statement names. PostgreSQL has
/// no schema called main or temp: those name databases of the engine, and
/// an object stored under them would be in no schema the catalogs show.
pub(crate) fn schema_qualifier(schema: &str) -> Result<&str, ParseError> {
    if schema.eq_ignore_ascii_case("main") || schema.eq_ignore_ascii_case("temp") {
        return Err(ParseError::Sqlstate {
            sqlstate: SQLSTATE_INVALID_SCHEMA_NAME,
            message: format!("schema \"{schema}\" does not exist"),
        });
    }
    Ok(map_schema_name(schema))
}

/// Maps a PostgreSQL schema name to the Turso schema that holds its tables.
fn map_schema_name(schema: &str) -> &str {
    match schema.to_ascii_lowercase().as_str() {
        "pg_catalog" | "information_schema" => "main",
        _ => schema,
    }
}
