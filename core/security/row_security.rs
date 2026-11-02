//! Row-level security: which tables hide rows from roles, and the policies
//! that show some of them.
//!
//! The catalog is stored in the `__turso_internal_row_security` table, one
//! row per object, with columns `kind`, `name`, `tbl_name`, `condition` and
//! `column_name`:
//!
//! | kind           | name        | tbl_name | condition                  | column_name           |
//! |----------------|-------------|----------|----------------------------|-----------------------|
//! | `row_security` | `''`        | table    | `''`                       | `''`                  |
//! | `policy`       | policy name | table    | `true`, `false` or `owner` | owner column, or `''` |
//!
//! Policies are stored as data rather than SQL: a policy shows every row,
//! no row, or the rows whose owner column equals the current role.
//!
//! At schema load every row is turned into a [RowSecurityChange] and applied
//! to an empty [RowSecurityCatalog]. DDL statements write the row and then
//! apply the same change to the in-memory catalog, so both paths share
//! [RowSecurityCatalog::apply].
//!
//! Row-level security is only supported for tables of the main database.
//! Tables are identified by the name they are stored under, which includes
//! their schema.

use std::collections::{HashMap, HashSet};

use crate::{util::normalize_ident, LimboError, Result};

pub const ROW_SECURITY_TABLE_NAME: &str = "__turso_internal_row_security";

pub const CREATE_ROW_SECURITY_TABLE_SQL: &str = "CREATE TABLE __turso_internal_row_security(kind TEXT, name TEXT, tbl_name TEXT, condition TEXT, column_name TEXT)";

pub(crate) const SELECT_ROW_SECURITY_SQL: &str =
    "SELECT kind, name, tbl_name, condition, column_name FROM __turso_internal_row_security";

#[derive(Debug, Clone, Default)]
pub struct RowSecurityCatalog {
    tables_with_row_security: HashSet<String>,
    policies: HashMap<String, Vec<Policy>>,
}

/// A SELECT policy for every role.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Policy {
    pub name: String,
    pub condition: PolicyCondition,
}

/// Which rows a policy shows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolicyCondition {
    AllRows,
    NoRows,
    /// Rows whose value in this column equals the current role.
    OwnedByRole(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowSecurityChange {
    SetRowSecurity { table: String, enabled: bool },
    CreatePolicy { table: String, policy: Policy },
    DropPolicy { table: String, name: String },
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
            RowSecurityChange::CreatePolicy { table, policy } => {
                self.policies
                    .entry(table.clone())
                    .or_default()
                    .push(policy.clone());
            }
            RowSecurityChange::DropPolicy { table, name } => {
                if let Some(policies) = self.policies.get_mut(table) {
                    policies.retain(|policy| &policy.name != name);
                }
            }
            RowSecurityChange::DropTable(table) => {
                self.tables_with_row_security.remove(table);
                self.policies.remove(table);
            }
        }
    }

    pub fn has_row_security(&self, table: &str) -> bool {
        self.tables_with_row_security
            .contains(&normalize_ident(table))
    }

    pub fn policies(&self, table: &str) -> &[Policy] {
        self.policies
            .get(&normalize_ident(table))
            .map_or(&[], Vec::as_slice)
    }
}

impl RowSecurityChange {
    fn from_row(row: &RowSecurityRow) -> Result<Self> {
        match row.kind.as_str() {
            "row_security" => Ok(Self::SetRowSecurity {
                table: row.tbl_name.clone(),
                enabled: true,
            }),
            "policy" => Ok(Self::CreatePolicy {
                table: row.tbl_name.clone(),
                policy: Policy {
                    name: row.name.clone(),
                    condition: PolicyCondition::from_row(row)?,
                },
            }),
            kind => Err(LimboError::Corrupt(format!(
                "unknown row security object kind \"{kind}\" in {ROW_SECURITY_TABLE_NAME}"
            ))),
        }
    }
}

impl PolicyCondition {
    fn from_row(row: &RowSecurityRow) -> Result<Self> {
        match row.condition.as_str() {
            "true" => Ok(Self::AllRows),
            "false" => Ok(Self::NoRows),
            "owner" => Ok(Self::OwnedByRole(row.column_name.clone())),
            condition => Err(LimboError::Corrupt(format!(
                "unknown policy condition \"{condition}\" in {ROW_SECURITY_TABLE_NAME}"
            ))),
        }
    }

    /// The `condition` and `column_name` values that store the condition.
    pub fn to_row_values(&self) -> (&'static str, &str) {
        match self {
            Self::AllRows => ("true", ""),
            Self::NoRows => ("false", ""),
            Self::OwnedByRole(column) => ("owner", column),
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

    #[test]
    fn load_policies_and_drop_them_with_the_table() {
        let policy_row = |name: &str, condition: &str, column: &str| RowSecurityRow {
            kind: "policy".to_string(),
            name: name.to_string(),
            tbl_name: "docs".to_string(),
            condition: condition.to_string(),
            column_name: column.to_string(),
        };
        let mut catalog = RowSecurityCatalog::load(&[
            policy_row("all", "true", ""),
            policy_row("own", "owner", "owner"),
        ])
        .unwrap();
        assert_eq!(
            catalog.policies("docs")[1].condition,
            PolicyCondition::OwnedByRole("owner".to_string())
        );
        assert!(RowSecurityCatalog::load(&[policy_row("x", "maybe", "")]).is_err());
        catalog.apply(&RowSecurityChange::DropTable("docs".to_string()));
        assert!(catalog.policies("docs").is_empty());
    }
}
