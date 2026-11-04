//! Row-level security policies.
//!
//! A table with row-level security limits which of its rows a role may
//! access. The names of those tables are stored in the
//! `__turso_internal_row_security` table, which is created when row-level
//! security is first enabled for a table. A table is stored under the name
//! that its schema row has, so tables of different namespaces with the same
//! name are told apart.
//!
//! Each schema holds a [`PolicyCatalog`], an in-memory copy of the stored
//! names. It is loaded together with the schema, and a statement that changes
//! it bumps the schema cookie so that other connections load the change.
//!
//! Core does not add SQL syntax for row-level security. Each SQL frontend
//! decides how it is exposed. The SQLite frontend does not expose it at all.

use std::collections::HashSet;

use crate::namespace::{mangle_name, NamespaceId};
use crate::util::normalize_ident;
use crate::{LimboError, Result, Value};

pub const ROW_SECURITY_TABLE_NAME: &str = "__turso_internal_row_security";

pub const CREATE_ROW_SECURITY_TABLE_SQL: &str =
    "CREATE TABLE __turso_internal_row_security (table_name TEXT NOT NULL)";

pub const SELECT_ROW_SECURITY_SQL: &str = "SELECT table_name FROM __turso_internal_row_security";

/// The tables of the main database with row-level security. Table names are
/// compared like in the schema, ignoring ASCII case.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PolicyCatalog {
    /// Stored names, as returned by [`PolicyCatalog::stored_table_name`].
    tables: HashSet<String>,
}

impl PolicyCatalog {
    /// Builds the catalog from the rows of [`SELECT_ROW_SECURITY_SQL`].
    pub fn new(rows: &[Vec<Value>]) -> Result<Self> {
        let mut tables = HashSet::with_capacity(rows.len());
        for row in rows {
            let [Value::Text(table_name)] = row.as_slice() else {
                return Err(LimboError::Corrupt(format!(
                    "{ROW_SECURITY_TABLE_NAME} has a malformed row: {row:?}"
                )));
            };
            tables.insert(table_name.as_str().to_string());
        }
        Ok(Self { tables })
    }

    /// The name that table `table_name` of `namespace` is stored under in
    /// [`ROW_SECURITY_TABLE_NAME`].
    pub fn stored_table_name(namespace: NamespaceId, table_name: &str) -> String {
        mangle_name(namespace, &normalize_ident(table_name))
    }

    pub fn is_enabled(&self, namespace: NamespaceId, table_name: &str) -> bool {
        self.tables
            .contains(&Self::stored_table_name(namespace, table_name))
    }

    pub fn enable(&mut self, namespace: NamespaceId, table_name: &str) {
        self.tables
            .insert(Self::stored_table_name(namespace, table_name));
    }

    pub fn disable(&mut self, namespace: NamespaceId, table_name: &str) {
        self.tables
            .remove(&Self::stored_table_name(namespace, table_name));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PUBLIC: NamespaceId = NamespaceId::new(1);

    #[test]
    fn catalog_has_the_tables_of_the_stored_rows() {
        let rows = vec![
            vec![Value::build_text("users")],
            vec![Value::build_text(PolicyCatalog::stored_table_name(
                PUBLIC, "posts",
            ))],
        ];

        let catalog = PolicyCatalog::new(&rows).unwrap();

        assert!(catalog.is_enabled(NamespaceId::ROOT, "users"));
        assert!(catalog.is_enabled(PUBLIC, "posts"));
        assert!(!catalog.is_enabled(NamespaceId::ROOT, "posts"));
        assert!(!catalog.is_enabled(PUBLIC, "comments"));
    }

    #[test]
    fn malformed_row_is_reported_as_corruption() {
        let rows = vec![vec![Value::from_i64(1)]];

        let error = PolicyCatalog::new(&rows).unwrap_err();

        assert!(matches!(error, LimboError::Corrupt(_)), "{error}");
    }

    #[test]
    fn table_names_ignore_ascii_case() {
        let mut catalog = PolicyCatalog::default();

        catalog.enable(PUBLIC, "Users");

        assert!(catalog.is_enabled(PUBLIC, "USERS"));
    }

    #[test]
    fn table_with_same_name_in_another_namespace_is_not_enabled() {
        let mut catalog = PolicyCatalog::default();

        catalog.enable(PUBLIC, "users");

        assert!(!catalog.is_enabled(NamespaceId::new(2), "users"));
    }

    #[test]
    fn disabled_table_is_no_longer_enabled() {
        let mut catalog = PolicyCatalog::default();
        catalog.enable(PUBLIC, "users");

        catalog.disable(PUBLIC, "users");

        assert!(!catalog.is_enabled(PUBLIC, "users"));
    }
}
