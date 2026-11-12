//! Lookup of the types that are not built in: the custom types of a schema
//! and the row types of its relations.

use crate::session::relation_lookup_namespaces;
use std::sync::Arc;
use turso_core::schema::RelationKind;
use turso_core::Connection;

// FIXME: CREATE TABLE and casts do not call `lookup_type`. They give core a
// type name that is not built in as it is written, and core knows only the
// custom types.

/// What a type name that is not built in names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PgType {
    /// A custom type: a domain or an enum.
    Custom,
    /// The row type of a table, a view or a materialized view.
    Row,
}

/// What `name` names among the types that are not built in. A name with
/// `schema` is looked up in that schema. Otherwise it is looked up in the
/// same order as a relation: temp first, then the schemas of the search
/// path.
pub(crate) fn lookup_type(
    conn: &Arc<Connection>,
    schema: Option<&str>,
    name: &str,
) -> Option<PgType> {
    relation_lookup_namespaces(conn, schema)
        .iter()
        .find_map(|namespace| {
            conn.with_schema(namespace.database_id(), |s| {
                if s.get_type_def_unchecked(name)
                    .is_some_and(|type_def| !type_def.is_builtin)
                {
                    return Some(PgType::Custom);
                }
                // An index or a sequence has no row type.
                match s.relation_kind(namespace.target.id, name)? {
                    RelationKind::Table | RelationKind::View | RelationKind::MaterializedView => {
                        Some(PgType::Row)
                    }
                    RelationKind::Sequence | RelationKind::Index => None,
                }
            })
        })
}
