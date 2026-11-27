//! The types of the PostgreSQL frontend: the built-in types that it registers
//! in place of the types of core, and the lookup of the types that are not
//! built in, which are the custom types of a schema and the row types of its
//! relations.

mod boolean;
mod integer;
mod integer_operators;
mod oid;
mod regclass;
mod uuid;

pub use integer::IntegerType;
pub(crate) use integer_operators::{register_integer_operator_functions, IntegerOperator};
pub(crate) use oid::{check_oid_range, oid_input};
pub(crate) use regclass::{parse_regclass, RegClassInput};
pub(crate) use uuid::canonical_uuid;

use crate::error::sqlstate_error;
use crate::session::relation_lookup_namespaces;
use std::sync::Arc;
use turso_core::schema::{RelationKind, SchemaBuilder};
use turso_core::types::{TextInput, Type};
use turso_core::{Connection, Result};
use turso_pg_parser::sqlstate::{SQLSTATE_FEATURE_NOT_SUPPORTED, SQLSTATE_UNDEFINED_OBJECT};
use turso_pg_parser::WrittenTypeName;

/// A row of `pg_type` for a built-in type that is not an array, and how
/// tursopg has the type.
pub(crate) struct BuiltInType {
    pub(crate) oid: i64,
    pub(crate) name: &'static str,
    pub(crate) category: &'static str,
    pub(crate) is_preferred: bool,
    pub(crate) len: i64,
    pub(crate) by_value: bool,
    pub(crate) align: &'static str,
    pub(crate) storage: &'static str,
    /// The oid of the pg_type row of the array of the type, which is named
    /// `_` and the name of the type. 0 when tursopg has no array of the type.
    pub(crate) array_oid: i64,
    pub(crate) delimiter: &'static str,
    pub(crate) support: Support,
}

/// How tursopg has a built-in type.
pub(crate) enum Support {
    /// The frontend registers the type in the schema, in place of the type
    /// that core registers under the same name.
    Registered(&'static dyn Type),
    /// tursopg takes the type, but the frontend does not register it: it is
    /// a type that core registers, or the translator stores it as a type of
    /// the engine. The FIXME on the row says which.
    NotRegistered,
}

macro_rules! built_in_types {
    ($((
        $oid:literal, $name:literal, $category:literal, $is_preferred:literal, $len:literal,
        $by_value:literal, $align:literal, $storage:literal, $array_oid:literal,
        $delimiter:literal, $support:expr
    ),)*) => {
        [$(BuiltInType {
            oid: $oid,
            name: $name,
            category: $category,
            is_preferred: $is_preferred,
            len: $len,
            by_value: $by_value,
            align: $align,
            storage: $storage,
            array_oid: $array_oid,
            delimiter: $delimiter,
            support: $support,
        },)*]
    };
}

/// The built-in types that tursopg supports, with the values of their pg_type
/// rows in PostgreSQL 18: oid, typname, typcategory, typispreferred, typlen,
/// typbyval, typalign, typstorage, typarray and typdelim. The pg_type table
/// lists these rows and the row of the array of each type that has one.
///
/// A type has a row here only when tursopg supports it, so that pg_type says
/// which types tursopg has. A type that the translator only stores as
/// another type without reading its text, such as point, xml or money, and a
/// type that it refuses as the type of a column, such as interval or time,
/// has no row.
pub(crate) static BUILT_IN_TYPES: [BuiltInType; 21] = built_in_types! {
    (16, "bool", "B", true, 1, true, "c", "p", 1000, ",", Support::Registered(&boolean::Boolean)),
    // FIXME(registered-types): bytea: the frontend uses the type that core registers, which stores text as its bytes and does not read the hex and escape formats. A bytea type registered in the frontend, with its INPUT, replaces this.
    (17, "bytea", "U", false, -1, false, "i", "x", 1001, ",", Support::NotRegistered),
    (20, "int8", "N", false, 8, true, "d", "p", 1016, ",", Support::Registered(&IntegerType::Int8)),
    (21, "int2", "N", false, 2, true, "s", "p", 1005, ",", Support::Registered(&IntegerType::Int2)),
    (23, "int4", "N", false, 4, true, "i", "p", 1007, ",", Support::Registered(&IntegerType::Int4)),
    // FIXME(registered-types): text is the TEXT of the engine and has no registered type, so core cannot name the type of a text value to the dialect. A text type registered in the frontend, with its INPUT, replaces this.
    (25, "text", "S", true, -1, false, "i", "x", 1009, ",", Support::NotRegistered),
    (26, "oid", "N", true, 4, true, "i", "p", 1028, ",", Support::Registered(&oid::Oid)),
    // FIXME(registered-types): json: the frontend uses the type that core registers, which reads text in its ENCODE and has no INPUT. A json type registered in the frontend, with its INPUT, replaces this.
    (114, "json", "U", false, -1, false, "i", "x", 199, ",", Support::NotRegistered),
    // FIXME(registered-types): cidr: the frontend uses the type that core registers, which does not check its text. A cidr type registered in the frontend, with its INPUT, replaces this.
    (650, "cidr", "I", false, -1, false, "i", "m", 651, ",", Support::NotRegistered),
    // FIXME(registered-types): float4 is stored as the REAL of the engine, so it keeps the digits of a float8. A float4 type registered in the frontend, with its INPUT, replaces this.
    (700, "float4", "N", false, 4, true, "i", "p", 1021, ",", Support::NotRegistered),
    // FIXME(registered-types): float8 is the REAL of the engine and has no registered type, so it cannot store NaN. A float8 type registered in the frontend, with its INPUT, replaces this.
    (701, "float8", "N", true, 8, true, "d", "p", 1022, ",", Support::NotRegistered),
    // FIXME(registered-types): macaddr: the frontend uses the type that core registers, which does not check its text. A macaddr type registered in the frontend, with its INPUT, replaces this.
    (829, "macaddr", "U", false, 6, false, "i", "p", 1040, ",", Support::NotRegistered),
    // FIXME(registered-types): inet: the frontend uses the type that core registers, which reads text in its ENCODE and has no INPUT. A inet type registered in the frontend, with its INPUT, replaces this.
    (869, "inet", "I", true, -1, false, "i", "m", 1041, ",", Support::NotRegistered),
    // FIXME(registered-types): varchar with a length is the type that core registers, and without a length it is the TEXT of the engine. A varchar type registered in the frontend, with its INPUT, replaces this.
    (1043, "varchar", "S", false, -1, false, "i", "x", 1015, ",", Support::NotRegistered),
    // FIXME(registered-types): date: the frontend uses the type that core registers, which reads text in its ENCODE and has no INPUT. A date type registered in the frontend, with its INPUT, replaces this. It has no array yet: `date[]` is refused, so typarray is 0 and not the 1182 of PostgreSQL.
    (1082, "date", "D", false, 4, true, "i", "p", 0, ",", Support::NotRegistered),
    // FIXME(registered-types): timestamp: the frontend uses the type that core registers, which reads text in its ENCODE and has no INPUT. A timestamp type registered in the frontend, with its INPUT, replaces this. It has no array yet: `timestamp[]` is refused, so typarray is 0 and not the 1115 of PostgreSQL.
    (1114, "timestamp", "D", false, 8, true, "d", "p", 0, ",", Support::NotRegistered),
    // FIXME(registered-types): timestamptz: the frontend uses the type that core registers, which reads text in its ENCODE and has no INPUT. A timestamptz type registered in the frontend, with its INPUT, replaces this. It has no array yet: `timestamptz[]` is refused, so typarray is 0 and not the 1185 of PostgreSQL.
    (1184, "timestamptz", "D", true, 8, true, "d", "p", 0, ",", Support::NotRegistered),
    // FIXME(registered-types): numeric with a precision is the type that core registers, and without one it is the REAL of the engine, which does not keep all its digits. A numeric type registered in the frontend, with its INPUT, replaces this.
    (1700, "numeric", "N", false, -1, false, "i", "m", 1231, ",", Support::NotRegistered),
    (2205, "regclass", "N", false, 4, true, "i", "p", 2210, ",", Support::Registered(&regclass::RegClass)),
    (2950, "uuid", "U", false, 16, false, "c", "p", 2951, ",", Support::Registered(&uuid::Uuid)),
    // FIXME(registered-types): jsonb: the frontend uses the type that core registers, which reads text in its ENCODE and has no INPUT. A jsonb type registered in the frontend, with its INPUT, replaces this.
    (3802, "jsonb", "U", false, -1, false, "i", "x", 3807, ",", Support::NotRegistered),
};

/// The built-in type whose pg_type row has `oid`.
pub(crate) fn built_in_type(oid: i64) -> Option<&'static BuiltInType> {
    BUILT_IN_TYPES.iter().find(|ty| ty.oid == oid)
}

/// Adds the types that the frontend registers to `builder`. Each one replaces
/// the type that core registered under the same name.
pub(crate) fn register_builtin_types(builder: SchemaBuilder) -> Result<SchemaBuilder> {
    BUILT_IN_TYPES
        .iter()
        .try_fold(builder, |builder, ty| match ty.support {
            Support::Registered(registered) => builder.builtin_type(registered),
            Support::NotRegistered => Ok(builder),
        })
}

/// FIXME(pg-catalog): This finds the input function of a type (`typinput` of its pg_type row) in `BUILT_IN_TYPES`, for a caller that holds only the oid of a built-in type: the conversions of casts.rs, the parameters of the server and the check of a string literal. A caller that resolves a type name finds the INPUT in the definition of the type (`lookup_type_name`). A type without an INPUT is still read by hand (`hand_written_input_error`, the conversions of casts.rs, the parameters of the server). Each of those copies goes away when its type gets an INPUT, and a lookup of `typinput` in a pg_type table replaces this function (see `Type::input`).
/// The INPUT of the built-in type whose pg_type row has `oid`: the function
/// that reads text as a value of the type. None for a type that has no INPUT
/// yet.
pub fn type_input(oid: i64) -> Option<TextInput> {
    match built_in_type(oid)?.support {
        Support::Registered(registered) => registered.input(),
        Support::NotRegistered => None,
    }
}

// FIXME(pg-catalog): `lookup_type_name` is the one place that says what a type name written as text is. The names written in a statement do not come here yet: the translator of turso_pg_parser maps them by its own lists (`map_pg_type`, `pg_type_name_to_ast_type`, and `BASE_TYPES`, `PSEUDO_TYPES` and `POLYMORPHIC_TYPES` in create_function.rs) and passes a name it does not know on, so `1::nosuchtype` is not refused. `pg_type_oid` of catalog.rs is not a lookup of a written name: it reads the type of a stored column, and knows names of SQLite such as `tinyint`. When casts, column types and function signatures resolve their type names here, those lists go away.

/// A type that a name written in SQL stands for.
#[derive(Debug)]
pub(crate) struct NamedType {
    /// The name of the type as PostgreSQL writes it in an error, with its
    /// schema when the name was written with one.
    pub(crate) written_name: String,
    /// The modifiers written after the name, such as 10 and 2 in
    /// `numeric(10, 2)`.
    pub(crate) modifiers: Vec<i64>,
    /// How many array dimensions the name has, for `int[]` and for `_int4`.
    pub(crate) array_dimensions: u32,
    pub(crate) kind: NamedTypeKind,
}

#[derive(Debug)]
pub(crate) enum NamedTypeKind {
    /// A type with a row in the pg_type of tursopg, and the typname of the
    /// row.
    BuiltIn { oid: i64, typname: &'static str },
    /// A type made by CREATE TYPE or CREATE DOMAIN.
    Custom,
    /// The row type of a table, a view or a sequence.
    RowOfRelation,
}

/// The type that `text` names, as PostgreSQL resolves a type name written as
/// text: its grammar reads the name (`parseTypeString`), and the name is
/// looked up in `pg_catalog` and then in the schemas of the search path.
/// Text that is not a type name fails with 42601, a name of no type with
/// 42704, and a type of PostgreSQL that tursopg does not have with 0A000.
pub(crate) fn lookup_type_name(conn: &Arc<Connection>, text: &str) -> Result<NamedType> {
    let written = turso_pg_parser::parse_type_name(text).map_err(crate::error::sql_parse_error)?;
    let written_name = written.qualified_name();
    let Some((kind, array_dimensions)) = lookup_type(conn, &written) else {
        let is_in_pg_catalog = written
            .schema
            .as_deref()
            .is_none_or(|schema| schema == "pg_catalog");
        if is_in_pg_catalog && only_postgres_has_type(&written.name) {
            return Err(sqlstate_error(
                SQLSTATE_FEATURE_NOT_SUPPORTED,
                format!("type \"{written_name}\" is not supported"),
                None,
            ));
        }
        return Err(sqlstate_error(
            SQLSTATE_UNDEFINED_OBJECT,
            format!("type \"{written_name}\" does not exist"),
            None,
        ));
    };
    Ok(NamedType {
        written_name,
        modifiers: written.modifiers,
        array_dimensions,
        kind,
    })
}

/// Whether `typname` is a type of `pg_catalog` in PostgreSQL that tursopg
/// has no pg_type row for, or the array of one.
fn only_postgres_has_type(typname: &str) -> bool {
    let element = typname.strip_prefix('_').unwrap_or(typname);
    TYPES_ONLY_POSTGRES_HAS.contains(&typname) || TYPES_ONLY_POSTGRES_HAS.contains(&element)
}

/// FIXME(pg-catalog): These are the types of `pg_catalog` in PostgreSQL 18 that have no row in `BUILT_IN_TYPES`, so that a name of one is refused as not supported and not as a type that does not exist. A name leaves this list when its type gets a row.
const TYPES_ONLY_POSTGRES_HAS: [&str; 85] = [
    "time",
    "timetz",
    "point",
    "lseg",
    "path",
    "box",
    "polygon",
    "line",
    "circle",
    "regproc",
    "money",
    "regprocedure",
    "regoper",
    "regoperator",
    "regclass",
    "regtype",
    "regconfig",
    "regdictionary",
    "regnamespace",
    "regrole",
    "regcollation",
    "name",
    "interval",
    "tid",
    "xid",
    "cid",
    "xml",
    "macaddr8",
    "aclitem",
    "refcursor",
    "txid_snapshot",
    "pg_lsn",
    "tsvector",
    "tsquery",
    "gtsvector",
    "jsonpath",
    "pg_snapshot",
    "xid8",
    "bit",
    "varbit",
    "char",
    "pg_node_tree",
    "pg_ndistinct",
    "pg_dependencies",
    "pg_brin_bloom_summary",
    "pg_brin_minmax_multi_summary",
    "pg_mcv_list",
    "int4multirange",
    "nummultirange",
    "tsmultirange",
    "tstzmultirange",
    "datemultirange",
    "int8multirange",
    "pg_ddl_command",
    "table_am_handler",
    "index_am_handler",
    "record",
    "cstring",
    "any",
    "anyarray",
    "void",
    "trigger",
    "language_handler",
    "internal",
    "anyelement",
    "_record",
    "anynonarray",
    "fdw_handler",
    "tsm_handler",
    "anyenum",
    "anyrange",
    "event_trigger",
    "anymultirange",
    "anycompatiblemultirange",
    "anycompatible",
    "anycompatiblearray",
    "anycompatiblenonarray",
    "anycompatiblerange",
    "unknown",
    "int4range",
    "numrange",
    "tsrange",
    "tstzrange",
    "daterange",
    "int8range",
];

fn lookup_type(conn: &Arc<Connection>, written: &WrittenTypeName) -> Option<(NamedTypeKind, u32)> {
    let schema = written.schema.as_deref();
    if schema.is_none_or(|schema| schema == "pg_catalog") {
        if let Some((oid, typname, dimensions_of_name)) = built_in_type_named(&written.name) {
            return Some((
                NamedTypeKind::BuiltIn { oid, typname },
                written.array_dimensions + dimensions_of_name,
            ));
        }
    }
    if schema == Some("pg_catalog") {
        return None;
    }
    let kind = match type_in_schemas(conn, schema, &written.name)? {
        TypeInSchema::Registered { is_builtin: false } => NamedTypeKind::Custom,
        TypeInSchema::Registered { is_builtin: true } => return None,
        TypeInSchema::RowOfRelation => NamedTypeKind::RowOfRelation,
    };
    Some((kind, written.array_dimensions))
}

/// FIXME(pg-catalog): pg_type has no row for bpchar, the type of `char(n)`, which tursopg stores as varchar. A bpchar row replaces the second case.
/// The oid and the typname of the built-in type whose pg_type row has the
/// name `typname`, and 1 when the name is that of the row of its array, such
/// as `_int4`: the oid and the typname are then those of the element type.
pub(crate) fn built_in_type_named(typname: &str) -> Option<(i64, &'static str, u32)> {
    const BPCHAR: i64 = 1042;
    let named = |name: &str| BUILT_IN_TYPES.iter().find(|ty| ty.name == name);
    if let Some(ty) = named(typname) {
        return Some((ty.oid, ty.name, 0));
    }
    if typname == "bpchar" {
        return Some((BPCHAR, "bpchar", 0));
    }
    let element = named(typname.strip_prefix('_')?)?;
    Some((element.oid, element.name, 1))
}

/// Whether `name` is a type that the schemas of `schema` have: the row type
/// of a table, view or sequence, or a type of the type registry.
pub(crate) fn type_exists(conn: &Arc<Connection>, schema: Option<&str>, name: &str) -> bool {
    type_in_schemas(conn, schema, name).is_some()
}

enum TypeInSchema {
    /// A type of the type registry: one that core or the frontend registers
    /// (`is_builtin`), or one made by CREATE TYPE or CREATE DOMAIN.
    Registered { is_builtin: bool },
    /// The row type of a table, a view or a sequence.
    RowOfRelation,
}

/// The type that `name` is in the first schema that has it, of the schema
/// `schema` or of the search path when there is none.
fn type_in_schemas(
    conn: &Arc<Connection>,
    schema: Option<&str>,
    name: &str,
) -> Option<TypeInSchema> {
    relation_lookup_namespaces(conn, schema)
        .iter()
        .find_map(|namespace| {
            conn.with_schema(namespace.database_id(), |s| {
                if let Some(type_def) = s.get_type_def_unchecked(name) {
                    return Some(TypeInSchema::Registered {
                        is_builtin: type_def.is_builtin,
                    });
                }
                s.relation_kind(namespace.target.id, name)
                    .filter(|kind| *kind != RelationKind::Index)
                    .map(|_| TypeInSchema::RowOfRelation)
            })
        })
}
