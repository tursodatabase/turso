//! Type names: how to read them, and the names of PostgreSQL's built-in
//! types.

use crate::names::extract_key_columns;
use crate::ParseError;
use pg_query::protobuf::node::Node;
use pg_query::protobuf::TypeName;

/// The kind of a built-in type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BuiltInType {
    /// A type that holds data, such as `int4` or `text`.
    Base,
    /// A type that stands for the types of a function's arguments, such as
    /// `anyelement`.
    Polymorphic,
    /// A type that is not polymorphic and holds no data of its own, such as
    /// `record`, `void` or `trigger`.
    Pseudo,
}

/// The schema and the name of `type_name`, as pg_query gives them: `int` is
/// `pg_catalog.int4`.
pub(crate) fn schema_and_name(type_name: &TypeName) -> Result<(Option<&str>, &str), ParseError> {
    match type_name.names.as_slice() {
        [name] => Ok((None, name_part(name)?)),
        [schema, name] => Ok((Some(name_part(schema)?), name_part(name)?)),
        _ => Err(ParseError::not_supported("a cross-database reference")),
    }
}

/// One part of a qualified name, such as `public` of `public.t`.
fn name_part(node: &pg_query::protobuf::Node) -> Result<&str, ParseError> {
    match &node.node {
        Some(Node::String(part)) => Ok(&part.sval),
        _ => Err(ParseError::ParseError("expected a name".into())),
    }
}

/// The kind of the built-in type that `name` of `schema` names. The built-in
/// types are in pg_catalog, which the search path always has.
pub(crate) fn built_in_type(schema: Option<&str>, name: &str) -> Option<BuiltInType> {
    if schema.is_some_and(|schema| schema != "pg_catalog") {
        None
    } else if POLYMORPHIC_TYPES.contains(&name) {
        Some(BuiltInType::Polymorphic)
    } else if PSEUDO_TYPES.contains(&name) {
        Some(BuiltInType::Pseudo)
    } else if BASE_TYPES.contains(&name) {
        Some(BuiltInType::Base)
    } else {
        None
    }
}

/// The type name as PostgreSQL writes it in an error about a type that does
/// not exist, such as `public.nosuch[]`.
pub(crate) fn type_name_text(type_name: &TypeName) -> Result<String, ParseError> {
    let mut text = extract_key_columns(&type_name.names)?.join(".");
    for _ in &type_name.array_bounds {
        text.push_str("[]");
    }
    Ok(text)
}

// FIXME: the three lists that follow are written by hand, and they must
// follow the PostgreSQL version of pg_query. They are also one of three
// places that say what a type name is. The FIXME in
// postgres/frontend/pg_type.rs gives the plan for one type lookup.

/// The types that stand for the types of a function's arguments.
const POLYMORPHIC_TYPES: &[&str] = &[
    "anyelement",
    "anyarray",
    "anynonarray",
    "anyenum",
    "anyrange",
    "anymultirange",
    "anycompatible",
    "anycompatiblearray",
    "anycompatiblenonarray",
    "anycompatiblerange",
    "anycompatiblemultirange",
];

/// The pseudo-types that are not polymorphic.
const PSEUDO_TYPES: &[&str] = &[
    "any",
    "cstring",
    "internal",
    "language_handler",
    "fdw_handler",
    "table_am_handler",
    "index_am_handler",
    "tsm_handler",
    "record",
    "trigger",
    "event_trigger",
    "pg_ddl_command",
    "void",
    "unknown",
];

/// The built-in base, range and multirange types, by the names pg_query
/// gives them: `int` is `int4` and `double precision` is `float8`, for
/// example.
const BASE_TYPES: &[&str] = &[
    "bool",
    "int2",
    "int4",
    "int8",
    "float4",
    "float8",
    "numeric",
    "money",
    "text",
    "varchar",
    "bpchar",
    "char",
    "name",
    "bytea",
    "uuid",
    "date",
    "time",
    "timetz",
    "timestamp",
    "timestamptz",
    "interval",
    "json",
    "jsonb",
    "xml",
    "inet",
    "cidr",
    "macaddr",
    "macaddr8",
    "bit",
    "varbit",
    "oid",
    "regclass",
    "regtype",
    "regproc",
    "regprocedure",
    "regnamespace",
    "regrole",
    "point",
    "line",
    "lseg",
    "box",
    "path",
    "polygon",
    "circle",
    "tsvector",
    "tsquery",
    "gtsvector",
    "jsonpath",
    "refcursor",
    "aclitem",
    "int2vector",
    "oidvector",
    "tid",
    "xid",
    "xid8",
    "cid",
    "pg_lsn",
    "pg_snapshot",
    "txid_snapshot",
    "regoper",
    "regoperator",
    "regconfig",
    "regdictionary",
    "regcollation",
    "regdatabase",
    "pg_node_tree",
    "pg_ndistinct",
    "pg_dependencies",
    "pg_mcv_list",
    "pg_brin_bloom_summary",
    "pg_brin_minmax_multi_summary",
    "int4range",
    "int8range",
    "numrange",
    "daterange",
    "tsrange",
    "tstzrange",
    "int4multirange",
    "int8multirange",
    "nummultirange",
    "datemultirange",
    "tsmultirange",
    "tstzmultirange",
];
