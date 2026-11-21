//! The types of a database: the built-in types that tursopg lists in
//! `pg_type`, the custom types of a schema, and the row types of its
//! relations.

use crate::session::relation_lookup_namespaces;
use std::sync::Arc;
use turso_core::schema::{RelationKind, TypeDef};
use turso_core::Connection;

// FIXME: the built-in type lists of turso_pg_parser (`BASE_TYPES`,
// `PSEUDO_TYPES` and `POLYMORPHIC_TYPES` in create_function.rs) and
// `pg_type_oid` in catalog.rs also say what a type name is, and they do not
// know about `lookup_type`. A type name that one of them misses is reported
// as "does not exist".

/// What a type name stands for.
#[allow(dead_code)]
pub(crate) enum PgType {
    Builtin(&'static PgTypeInfo),
    Custom(Arc<TypeDef>),
    RowOf(RelationKind),
}

/// The type called `name`, found in the built-in types, then in the schemas
/// that a lookup of `schema` searches.
pub(crate) fn lookup_type(
    conn: &Arc<Connection>,
    schema: Option<&str>,
    name: &str,
) -> Option<PgType> {
    let in_catalog = schema.is_none_or(|schema| schema == "pg_catalog");
    if in_catalog {
        if let Some(info) = PG_BASE_TYPES.iter().find(|info| info.name == name) {
            return Some(PgType::Builtin(info));
        }
    }
    relation_lookup_namespaces(conn, schema)
        .iter()
        .find_map(|namespace| {
            conn.with_schema(namespace.database_id(), |s| {
                if let Some(type_def) = s.get_type_def_unchecked(name) {
                    return Some(PgType::Custom(Arc::clone(type_def)));
                }
                s.relation_kind(namespace.target.id, name)
                    .filter(|kind| *kind != RelationKind::Index)
                    .map(PgType::RowOf)
            })
        })
}

pub(crate) struct PgTypeInfo {
    pub(crate) oid: i64,
    pub(crate) name: &'static str,
    pub(crate) typtype: &'static str,
    pub(crate) typcategory: &'static str,
    pub(crate) typispreferred: bool,
    pub(crate) typlen: i64,
    pub(crate) typarray: i64,
    pub(crate) typelem: i64,
    pub(crate) typbyval: bool,
    pub(crate) typalign: &'static str,
    pub(crate) typstorage: &'static str,
}

pub(crate) const PG_BASE_TYPES: &[PgTypeInfo] = &[
    PgTypeInfo {
        oid: 16,
        name: "bool",
        typtype: "b",
        typcategory: "B",
        typispreferred: true,
        typlen: 1,
        typarray: 1000,
        typelem: 0,
        typbyval: true,
        typalign: "c",
        typstorage: "p",
    },
    PgTypeInfo {
        oid: 17,
        name: "bytea",
        typtype: "b",
        typcategory: "U",
        typispreferred: false,
        typlen: -1,
        typarray: 1001,
        typelem: 0,
        typbyval: false,
        typalign: "i",
        typstorage: "x",
    },
    PgTypeInfo {
        oid: 20,
        name: "int8",
        typtype: "b",
        typcategory: "N",
        typispreferred: false,
        typlen: 8,
        typarray: 1016,
        typelem: 0,
        typbyval: true,
        typalign: "d",
        typstorage: "p",
    },
    PgTypeInfo {
        oid: 21,
        name: "int2",
        typtype: "b",
        typcategory: "N",
        typispreferred: false,
        typlen: 2,
        typarray: 1005,
        typelem: 0,
        typbyval: true,
        typalign: "s",
        typstorage: "p",
    },
    PgTypeInfo {
        oid: 23,
        name: "int4",
        typtype: "b",
        typcategory: "N",
        typispreferred: false,
        typlen: 4,
        typarray: 1007,
        typelem: 0,
        typbyval: true,
        typalign: "i",
        typstorage: "p",
    },
    PgTypeInfo {
        oid: 25,
        name: "text",
        typtype: "b",
        typcategory: "S",
        typispreferred: true,
        typlen: -1,
        typarray: 1009,
        typelem: 0,
        typbyval: false,
        typalign: "i",
        typstorage: "x",
    },
    PgTypeInfo {
        oid: 26,
        name: "oid",
        typtype: "b",
        typcategory: "N",
        typispreferred: true,
        typlen: 4,
        typarray: 1028,
        typelem: 0,
        typbyval: true,
        typalign: "i",
        typstorage: "p",
    },
    PgTypeInfo {
        oid: 114,
        name: "json",
        typtype: "b",
        typcategory: "U",
        typispreferred: false,
        typlen: -1,
        typarray: 199,
        typelem: 0,
        typbyval: false,
        typalign: "i",
        typstorage: "x",
    },
    PgTypeInfo {
        oid: 650,
        name: "cidr",
        typtype: "b",
        typcategory: "I",
        typispreferred: false,
        typlen: -1,
        typarray: 651,
        typelem: 0,
        typbyval: false,
        typalign: "i",
        typstorage: "m",
    },
    PgTypeInfo {
        oid: 700,
        name: "float4",
        typtype: "b",
        typcategory: "N",
        typispreferred: false,
        typlen: 4,
        typarray: 1021,
        typelem: 0,
        typbyval: true,
        typalign: "i",
        typstorage: "p",
    },
    PgTypeInfo {
        oid: 701,
        name: "float8",
        typtype: "b",
        typcategory: "N",
        typispreferred: true,
        typlen: 8,
        typarray: 1022,
        typelem: 0,
        typbyval: true,
        typalign: "d",
        typstorage: "p",
    },
    PgTypeInfo {
        oid: 829,
        name: "macaddr",
        typtype: "b",
        typcategory: "U",
        typispreferred: false,
        typlen: 6,
        typarray: 1040,
        typelem: 0,
        typbyval: false,
        typalign: "i",
        typstorage: "p",
    },
    PgTypeInfo {
        oid: 869,
        name: "inet",
        typtype: "b",
        typcategory: "I",
        typispreferred: true,
        typlen: -1,
        typarray: 1041,
        typelem: 0,
        typbyval: false,
        typalign: "i",
        typstorage: "m",
    },
    PgTypeInfo {
        oid: 1043,
        name: "varchar",
        typtype: "b",
        typcategory: "S",
        typispreferred: false,
        typlen: -1,
        typarray: 1015,
        typelem: 0,
        typbyval: false,
        typalign: "i",
        typstorage: "x",
    },
    PgTypeInfo {
        oid: 1082,
        name: "date",
        typtype: "b",
        typcategory: "D",
        typispreferred: false,
        typlen: 4,
        typarray: 1182,
        typelem: 0,
        typbyval: true,
        typalign: "i",
        typstorage: "p",
    },
    PgTypeInfo {
        oid: 1083,
        name: "time",
        typtype: "b",
        typcategory: "D",
        typispreferred: false,
        typlen: 8,
        typarray: 1183,
        typelem: 0,
        typbyval: true,
        typalign: "d",
        typstorage: "p",
    },
    PgTypeInfo {
        oid: 1114,
        name: "timestamp",
        typtype: "b",
        typcategory: "D",
        typispreferred: false,
        typlen: 8,
        typarray: 1115,
        typelem: 0,
        typbyval: true,
        typalign: "d",
        typstorage: "p",
    },
    PgTypeInfo {
        oid: 1184,
        name: "timestamptz",
        typtype: "b",
        typcategory: "D",
        typispreferred: true,
        typlen: 8,
        typarray: 1185,
        typelem: 0,
        typbyval: true,
        typalign: "d",
        typstorage: "p",
    },
    PgTypeInfo {
        oid: 1700,
        name: "numeric",
        typtype: "b",
        typcategory: "N",
        typispreferred: false,
        typlen: -1,
        typarray: 1231,
        typelem: 0,
        typbyval: false,
        typalign: "i",
        typstorage: "m",
    },
    PgTypeInfo {
        oid: 2950,
        name: "uuid",
        typtype: "b",
        typcategory: "U",
        typispreferred: false,
        typlen: 16,
        typarray: 2951,
        typelem: 0,
        typbyval: false,
        typalign: "c",
        typstorage: "p",
    },
    PgTypeInfo {
        oid: 3802,
        name: "jsonb",
        typtype: "b",
        typcategory: "U",
        typispreferred: false,
        typlen: -1,
        typarray: 3807,
        typelem: 0,
        typbyval: false,
        typalign: "i",
        typstorage: "x",
    },
];

/// Static PG array type definitions:
/// (oid, name, typelem)
pub(crate) const PG_ARRAY_TYPES: &[(i64, &str, i64)] = &[
    (199, "_json", 114),
    (651, "_cidr", 650),
    (1000, "_bool", 16),
    (1001, "_bytea", 17),
    (1005, "_int2", 21),
    (1007, "_int4", 23),
    (1009, "_text", 25),
    (1015, "_varchar", 1043),
    (1016, "_int8", 20),
    (1021, "_float4", 700),
    (1022, "_float8", 701),
    (1028, "_oid", 26),
    (1040, "_macaddr", 829),
    (1041, "_inet", 869),
    (1115, "_timestamp", 1114),
    (1182, "_date", 1082),
    (1183, "_time", 1083),
    (1185, "_timestamptz", 1184),
    (1231, "_numeric", 1700),
    (2951, "_uuid", 2950),
    (3807, "_jsonb", 3802),
];
