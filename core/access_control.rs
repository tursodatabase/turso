//! Access control: roles and row-level security.
//!
//! The catalog is stored in the `__turso_internal_access_control` table, one
//! row per object, with columns `kind`, `name`, `tbl_name`, `condition` and
//! `column_name`:
//!
//! | kind           | name        | tbl_name | condition                      | column_name |
//! |----------------|-------------|----------|--------------------------------|-------------|
//! | `role`         | role name   | `''`     | `''`                           | `''`        |
//! | `row_security` | `''`        | table    | `''`                           | `''`        |
//! | `policy`       | policy name | table    | `true`, `false` or `owner`     | owner column, or `''` |
//!
//! Policies are stored as data rather than SQL: a policy shows every row,
//! no row, or the rows whose owner column equals the current role.
//!
//! At schema load every row is turned into an [AccessControlChange] and
//! applied to an empty [AccessControlCatalog]. DDL statements write the row
//! and then apply the same change to the in-memory catalog, so both paths
//! share [AccessControlCatalog::apply].

use std::collections::{HashMap, HashSet};

use crate::{util::normalize_ident, LimboError, Result};

pub const ACCESS_CONTROL_TABLE_NAME: &str = "__turso_internal_access_control";

pub const ACCESS_CONTROL_TABLE_SQL: &str = "CREATE TABLE __turso_internal_access_control(kind TEXT, name TEXT, tbl_name TEXT, condition TEXT, column_name TEXT)";

pub(crate) const LOAD_ACCESS_CONTROL_SQL: &str =
    "SELECT kind, name, tbl_name, condition, column_name FROM __turso_internal_access_control";

#[derive(Debug, Clone, Default)]
pub struct AccessControlCatalog {
    roles: HashSet<String>,
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
pub enum AccessControlChange {
    CreateRole(String),
    DropRole(String),
    SetRowSecurity { table: String, enabled: bool },
    CreatePolicy { table: String, policy: Policy },
    DropPolicy { table: String, name: String },
    DropTable(String),
}

/// One row of `__turso_internal_access_control`.
#[derive(Debug, Clone)]
pub struct AccessControlRow {
    pub kind: String,
    pub name: String,
    pub tbl_name: String,
    pub condition: String,
    pub column_name: String,
}

impl AccessControlRow {
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

impl AccessControlCatalog {
    pub fn load(rows: &[AccessControlRow]) -> Result<Self> {
        let mut catalog = Self::default();
        for row in rows {
            catalog.apply(&AccessControlChange::from_row(row)?);
        }
        Ok(catalog)
    }

    pub fn apply(&mut self, change: &AccessControlChange) {
        match change {
            AccessControlChange::CreateRole(role) => {
                self.roles.insert(role.clone());
            }
            AccessControlChange::DropRole(role) => {
                self.roles.remove(role);
            }
            AccessControlChange::SetRowSecurity { table, enabled } => {
                if *enabled {
                    self.tables_with_row_security.insert(table.clone());
                } else {
                    self.tables_with_row_security.remove(table);
                }
            }
            AccessControlChange::CreatePolicy { table, policy } => {
                self.policies
                    .entry(table.clone())
                    .or_default()
                    .push(policy.clone());
            }
            AccessControlChange::DropPolicy { table, name } => {
                if let Some(policies) = self.policies.get_mut(table) {
                    policies.retain(|policy| &policy.name != name);
                }
            }
            AccessControlChange::DropTable(table) => {
                self.tables_with_row_security.remove(table);
                self.policies.remove(table);
            }
        }
    }

    pub fn has_role(&self, role: &str) -> bool {
        self.roles.contains(&normalize_ident(role))
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

impl AccessControlChange {
    fn from_row(row: &AccessControlRow) -> Result<Self> {
        match row.kind.as_str() {
            "role" => Ok(Self::CreateRole(row.name.clone())),
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
                "unknown access control object kind \"{kind}\" in {ACCESS_CONTROL_TABLE_NAME}"
            ))),
        }
    }
}

impl PolicyCondition {
    fn from_row(row: &AccessControlRow) -> Result<Self> {
        match row.condition.as_str() {
            "true" => Ok(Self::AllRows),
            "false" => Ok(Self::NoRows),
            "owner" => Ok(Self::OwnedByRole(row.column_name.clone())),
            condition => Err(LimboError::Corrupt(format!(
                "unknown policy condition \"{condition}\" in {ACCESS_CONTROL_TABLE_NAME}"
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

    fn role_row(name: &str) -> AccessControlRow {
        AccessControlRow {
            kind: "role".to_string(),
            name: name.to_string(),
            tbl_name: String::new(),
            condition: String::new(),
            column_name: String::new(),
        }
    }

    #[test]
    fn load_creates_stored_roles() {
        let catalog = AccessControlCatalog::load(&[role_row("alice"), role_row("bob")]).unwrap();
        assert!(catalog.has_role("alice"));
        assert!(catalog.has_role("BOB"));
        assert!(!catalog.has_role("carol"));
    }

    #[test]
    fn load_rejects_unknown_kind() {
        let row = AccessControlRow {
            kind: "mystery".to_string(),
            name: "x".to_string(),
            tbl_name: String::new(),
            condition: String::new(),
            column_name: String::new(),
        };
        assert!(AccessControlCatalog::load(&[row]).is_err());
    }

    #[test]
    fn load_enables_row_security() {
        let row = AccessControlRow {
            kind: "row_security".to_string(),
            name: String::new(),
            tbl_name: "docs".to_string(),
            condition: String::new(),
            column_name: String::new(),
        };
        let mut catalog = AccessControlCatalog::load(&[row]).unwrap();
        assert!(catalog.has_row_security("DOCS"));
        catalog.apply(&AccessControlChange::DropTable("docs".to_string()));
        assert!(!catalog.has_row_security("docs"));
    }

    #[test]
    fn load_policies_and_drop_them_with_the_table() {
        let policy_row = |name: &str, condition: &str, column: &str| AccessControlRow {
            kind: "policy".to_string(),
            name: name.to_string(),
            tbl_name: "docs".to_string(),
            condition: condition.to_string(),
            column_name: column.to_string(),
        };
        let mut catalog = AccessControlCatalog::load(&[
            policy_row("all", "true", ""),
            policy_row("own", "owner", "owner"),
        ])
        .unwrap();
        assert_eq!(
            catalog.policies("docs")[1].condition,
            PolicyCondition::OwnedByRole("owner".to_string())
        );
        assert!(AccessControlCatalog::load(&[policy_row("x", "maybe", "")]).is_err());
        catalog.apply(&AccessControlChange::DropTable("docs".to_string()));
        assert!(catalog.policies("docs").is_empty());
    }

    #[test]
    fn drop_role_forgets_role() {
        let mut catalog = AccessControlCatalog::load(&[role_row("alice")]).unwrap();
        catalog.apply(&AccessControlChange::DropRole("alice".to_string()));
        assert!(!catalog.has_role("alice"));
    }
}
