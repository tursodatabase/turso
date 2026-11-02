//! Row-level security: which tables hide rows from roles.
//!
//! The catalog is stored in the `__turso_internal_row_security` table, one
//! row per object, with columns `kind`, `name`, `tbl_name`, `condition` and
//! `column_name`:
//!
//! | kind           | name | tbl_name | condition | column_name |
//! |----------------|------|----------|-----------|-------------|
//! | `row_security` | `''` | table    | `''`      | `''`        |
//!
//! At schema load every row is turned into a [RowSecurityChange] and applied
//! to an empty [RowSecurityCatalog]. DDL statements write the row and then
//! apply the same change to the in-memory catalog, so both paths share
//! [RowSecurityCatalog::apply].
//!
//! Row-level security is only supported for tables of the main database.
//! Tables are identified by the name they are stored under, which includes
//! their schema.

use std::collections::HashSet;

use crate::{util::normalize_ident, LimboError, Result};

pub const ROW_SECURITY_TABLE_NAME: &str = "__turso_internal_row_security";

pub const CREATE_ROW_SECURITY_TABLE_SQL: &str = "CREATE TABLE __turso_internal_row_security(kind TEXT, name TEXT, tbl_name TEXT, condition TEXT, column_name TEXT)";

pub(crate) const SELECT_ROW_SECURITY_SQL: &str =
    "SELECT kind, name, tbl_name, condition, column_name FROM __turso_internal_row_security";

#[derive(Debug, Clone, Default)]
pub struct RowSecurityCatalog {
    tables_with_row_security: HashSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowSecurityChange {
    SetRowSecurity { table: String, enabled: bool },
    DropTable(String),
}

/// One row of `__turso_internal_row_security`.
#[derive(Debug, Clone)]
pub struct RowSecurityRow {
    pub kind: String,
    pub name: String,
    pub tbl_name: String,
    pub condition: String,
    pub column_name: String,
}

impl RowSecurityRow {
    pub(crate) fn from_row(row: &crate::Row) -> Result<Self> {
        Ok(Self {
            kind: row.get::<&str>(0)?.to_string(),
            name: row.get::<&str>(1)?.to_string(),
            tbl_name: row.get::<&str>(2)?.to_string(),
            condition: row.get::<&str>(3)?.to_string(),
            column_name: row.get::<&str>(4)?.to_string(),
        })
    }
}

impl RowSecurityCatalog {
    pub fn load(rows: &[RowSecurityRow]) -> Result<Self> {
        let mut catalog = Self::default();
        for row in rows {
            catalog.apply(&RowSecurityChange::from_row(row)?);
        }
        Ok(catalog)
    }

    pub fn apply(&mut self, change: &RowSecurityChange) {
        match change {
            RowSecurityChange::SetRowSecurity { table, enabled } => {
                if *enabled {
                    self.tables_with_row_security.insert(table.clone());
                } else {
                    self.tables_with_row_security.remove(table);
                }
            }
            RowSecurityChange::DropTable(table) => {
                self.tables_with_row_security.remove(table);
            }
        }
    }

    pub fn has_row_security(&self, table: &str) -> bool {
        self.tables_with_row_security
            .contains(&normalize_ident(table))
    }
}

impl RowSecurityChange {
    fn from_row(row: &RowSecurityRow) -> Result<Self> {
        match row.kind.as_str() {
            "row_security" => Ok(Self::SetRowSecurity {
                table: row.tbl_name.clone(),
                enabled: true,
            }),
            kind => Err(LimboError::Corrupt(format!(
                "unknown row security object kind \"{kind}\" in {ROW_SECURITY_TABLE_NAME}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(kind: &str, tbl_name: &str) -> RowSecurityRow {
        RowSecurityRow {
            kind: kind.to_string(),
            name: String::new(),
            tbl_name: tbl_name.to_string(),
            condition: String::new(),
            column_name: String::new(),
        }
    }

    #[test]
    fn load_rejects_unknown_kind() {
        assert!(RowSecurityCatalog::load(&[row("mystery", "x")]).is_err());
    }

    #[test]
    fn load_enables_row_security() {
        let mut catalog = RowSecurityCatalog::load(&[row("row_security", "docs")]).unwrap();
        assert!(catalog.has_row_security("DOCS"));
        catalog.apply(&RowSecurityChange::DropTable("docs".to_string()));
        assert!(!catalog.has_row_security("docs"));
    }
}
