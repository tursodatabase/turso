//! Type names: how to read them, the names of PostgreSQL's built-in types,
//! and what the other names name.

use crate::names::extract_key_columns;
use crate::translator::is_catalog_table_name;
use crate::ParseError;
use pg_query::protobuf::node::Node;
use pg_query::protobuf::TypeName;

/// What a type name that is not built in names. Only a caller with a
/// connection can look it up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgType {
    /// A custom type: a domain or an enum.
    Custom,
    /// The row type of a table, a view or a materialized view.
    Row,
}

/// What a type name that is not built in names, in a schema or, without
/// one, in the schemas of the search path.
pub type TypeLookup<'a> = dyn Fn(Option<&str>, &str) -> Option<PgType> + 'a;

/// What a type name names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TypeKind {
    /// A built-in type of PostgreSQL.
    BuiltIn(BuiltInType),
    /// The array type of a built-in type that is not polymorphic, written
    /// with an underscore, such as `_int4`.
    BuiltInArray(BuiltInType),
    /// A type that PostgreSQL defines outside the lists of built-in types.
    System,
    /// A type of the catalog, as `type_lookup` finds it by the name as it
    /// is written.
    Catalog(PgType),
    /// The array type of a type of the catalog, written with an
    /// underscore, such as `_mood`. `type_lookup` finds only the element
    /// type, `mood`.
    CatalogArray(PgType),
}

/// What `name` of `schema` names, or None when it names no type.
/// `_int4` names the array type of `int4`, and `_mood` the array type of
/// `mood`, unless a type has the name `_mood`.
pub(crate) fn type_kind(
    type_lookup: &TypeLookup<'_>,
    schema: Option<&str>,
    name: &str,
) -> Option<TypeKind> {
    if let Some(kind) = built_in_type(schema, name) {
        Some(TypeKind::BuiltIn(kind))
    } else if let Some((_, kind)) = built_in_array_element(schema, name) {
        Some(TypeKind::BuiltInArray(kind))
    } else if is_system_type(schema, name) {
        Some(TypeKind::System)
    } else if let Some(pg_type) = type_lookup(schema, name) {
        Some(TypeKind::Catalog(pg_type))
    } else {
        let element = name.strip_prefix('_')?;
        type_lookup(schema, element).map(TypeKind::CatalogArray)
    }
}

/// The built-in type, and its kind, whose array type `name` of `schema`
/// names: `int4` for `_int4`. PostgreSQL has no array type of a polymorphic
/// type.
pub(crate) fn built_in_array_element<'a>(
    schema: Option<&str>,
    name: &'a str,
) -> Option<(&'a str, BuiltInType)> {
    let element = name.strip_prefix('_')?;
    built_in_type(schema, element)
        .filter(|kind| *kind != BuiltInType::Polymorphic)
        .map(|kind| (element, kind))
}

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
fn built_in_type(schema: Option<&str>, name: &str) -> Option<BuiltInType> {
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

/// Whether `name` of `schema` is a type that PostgreSQL defines, but that
/// the lists of built-in types do not have: a type of information_schema,
/// such as `information_schema.cardinal_number`, or the row type of a
/// catalog relation, such as `pg_class`.
fn is_system_type(schema: Option<&str>, name: &str) -> bool {
    match schema {
        Some("information_schema") => true,
        Some(schema) if schema != "pg_catalog" => false,
        _ => is_catalog_table_name(name),
    }
}

/// The type name as PostgreSQL writes it in an error about a type that does
/// not exist, such as `public.nosuch[]`. PostgreSQL writes `[]` one time
/// also for more brackets, such as those of `nosuch[][]`.
pub(crate) fn type_name_text(type_name: &TypeName) -> Result<String, ParseError> {
    let mut text = extract_key_columns(&type_name.names)?.join(".");
    if !type_name.array_bounds.is_empty() {
        text.push_str("[]");
    }
    Ok(text)
}

// FIXME: the three lists that follow are written by hand, and they must
// follow the PostgreSQL version of pg_query.

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
