//! The tables that the frontend creates in a database when it opens it.
//!
//! Each table's module lists the statements that create its own table.
//! [`crate::open`] runs them in one transaction.

use turso_core::namespace::NamespaceId;
use turso_core::schema::Schema;
use turso_core::Result;

use crate::descriptions::DESCRIPTION_TABLE_NAME;
use crate::{auth_members, classes, descriptions, roles};

/// The statements that create `table`. They run when the database has no
/// such table.
pub(crate) struct TableInit {
    pub(crate) table: &'static str,
    pub(crate) statements: &'static [&'static str],
}

/// Every table of the frontend, in the order in which they are created.
static TABLE_INITS: [TableInit; 4] = [
    classes::TABLE_INIT,
    roles::TABLE_INIT,
    auth_members::TABLE_INIT,
    descriptions::TABLE_INIT,
];

/// The catalog tables that this frontend stores under their PostgreSQL
/// names.
const STORED_CATALOG_TABLES: [&str; 1] = [DESCRIPTION_TABLE_NAME];

/// The statements that create the frontend's tables that `schema` lacks.
/// A read-only database gets none.
pub(crate) fn initialize_statements(
    schema: &Schema,
    read_only: bool,
) -> Result<impl Iterator<Item = &'static str> + '_> {
    classes::refuse_relations_without_class_table(schema)?;
    let is_missing = |table| schema.get_btree_table(NamespaceId::ROOT, table).is_none();
    Ok(TABLE_INITS
        .iter()
        .filter(move |init| !read_only && is_missing(init.table))
        .flat_map(|init| init.statements.iter().copied()))
}

/// Whether `name` in the root namespace is a catalog table that this
/// frontend stores. Every role reads such a table and no user statement
/// writes it.
pub(crate) fn is_stored_catalog_table(namespace: NamespaceId, name: &str) -> bool {
    namespace.is_root()
        && STORED_CATALOG_TABLES
            .iter()
            .any(|table| name.eq_ignore_ascii_case(table))
}
