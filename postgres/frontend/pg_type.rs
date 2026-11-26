//! The types of the PostgreSQL frontend: the built-in types that it registers
//! in place of the types of core, and the lookup of the types that are not
//! built in, which are the custom types of a schema and the row types of its
//! relations.

mod boolean;
mod integer;
mod integer_operators;
mod oid;

pub(crate) use integer::split_sign;
pub use integer::IntegerType;
pub(crate) use integer_operators::{register_integer_operator_functions, IntegerOperator};
pub(crate) use oid::parse_oid;

use crate::session::relation_lookup_namespaces;
use std::sync::Arc;
use turso_core::schema::{RelationKind, SchemaBuilder};
use turso_core::types::Type;
use turso_core::{Connection, Result};

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
pub(crate) static BUILT_IN_TYPES: [BuiltInType; 20] = built_in_types! {
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
    // FIXME(registered-types): uuid: the frontend uses the type that core registers, which reads text in its ENCODE and has no INPUT. A uuid type registered in the frontend, with its INPUT, replaces this.
    (2950, "uuid", "U", false, 16, false, "c", "p", 2951, ",", Support::NotRegistered),
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

// FIXME(pg-catalog): three places that do not know about each other say what
// a type name is, in place of a pg_type catalog:
//
// - the built-in lists of turso_pg_parser (`BASE_TYPES`, `PSEUDO_TYPES` and
//   `POLYMORPHIC_TYPES` in create_function.rs),
// - the custom type registry (`get_type_def_unchecked`),
// - the relations of a schema (`relation_kind`), which give the row types.
//
// A new kind of type, such as a composite type, needs a change in each
// place. If one place is missed, nothing fails: the type is reported as
// "does not exist".
//
// The next step is one lookup in this module that says what the name is,
// not only whether it exists. For example:
//
//     pub(crate) enum PgType { Custom, RowOf(RelationKind) }
//     pub(crate) fn lookup_type(conn, schema, name) -> Option<PgType>
//
// CREATE TABLE, casts and functions then call that lookup, and
// `type_exists` becomes `lookup_type(..).is_some()`. A `pg_type` catalog
// can grow from this module the way `pg_class` grows from classes.rs.

/// Whether `name` is a type that is not built in: the row type of a table,
/// view or sequence, or a custom type such as a domain or an enum.
pub(crate) fn type_exists(conn: &Arc<Connection>, schema: Option<&str>, name: &str) -> bool {
    relation_lookup_namespaces(conn, schema)
        .iter()
        .any(|namespace| {
            conn.with_schema(namespace.database_id(), |s| {
                s.get_type_def_unchecked(name).is_some()
                    || s.relation_kind(namespace.target.id, name)
                        .is_some_and(|kind| kind != RelationKind::Index)
            })
        })
}
