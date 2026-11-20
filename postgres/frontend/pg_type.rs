//! Lookup of the types that are not built in: the custom types of a schema
//! and the row types of its relations.

use crate::session::relation_lookup_namespaces;
use std::sync::Arc;
use turso_core::schema::RelationKind;
use turso_core::Connection;

// FIXME: three places that do not know about each other say what a type
// name is:
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
