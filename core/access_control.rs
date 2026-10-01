//! Access control: roles.
//!
//! The catalog is stored in the `__turso_internal_access_control` table, one
//! row per object, with columns `kind`, `name`, `tbl_name`, `condition` and
//! `column_name`. A role is stored as `('role', <role name>, '', '', '')`;
//! the other columns are for row-level security.
//!
//! At schema load every row is turned into an [AccessControlChange] and
//! applied to an empty [AccessControlCatalog]. DDL statements write the row
//! and then apply the same change to the in-memory catalog, so both paths
//! share [AccessControlCatalog::apply].

use std::collections::HashSet;

use crate::{util::normalize_ident, LimboError, Result};

pub const ACCESS_CONTROL_TABLE_NAME: &str = "__turso_internal_access_control";

pub const ACCESS_CONTROL_TABLE_SQL: &str = "CREATE TABLE __turso_internal_access_control(kind TEXT, name TEXT, tbl_name TEXT, condition TEXT, column_name TEXT)";

pub(crate) const LOAD_ACCESS_CONTROL_SQL: &str =
    "SELECT kind, name FROM __turso_internal_access_control";

#[derive(Debug, Clone, Default)]
pub struct AccessControlCatalog {
    roles: HashSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccessControlChange {
    CreateRole(String),
    DropRole(String),
}

/// One row of `__turso_internal_access_control`.
#[derive(Debug, Clone)]
pub struct AccessControlRow {
    pub kind: String,
    pub name: String,
}

impl AccessControlRow {
    pub(crate) fn from_row(row: &crate::Row) -> Result<Self> {
        Ok(Self {
            kind: row.get::<&str>(0)?.to_string(),
            name: row.get::<&str>(1)?.to_string(),
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
        }
    }

    pub fn has_role(&self, role: &str) -> bool {
        self.roles.contains(&normalize_ident(role))
    }
}

impl AccessControlChange {
    fn from_row(row: &AccessControlRow) -> Result<Self> {
        match row.kind.as_str() {
            "role" => Ok(Self::CreateRole(row.name.clone())),
            kind => Err(LimboError::Corrupt(format!(
                "unknown access control object kind \"{kind}\" in {ACCESS_CONTROL_TABLE_NAME}"
            ))),
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
        };
        assert!(AccessControlCatalog::load(&[row]).is_err());
    }

    #[test]
    fn drop_role_forgets_role() {
        let mut catalog = AccessControlCatalog::load(&[role_row("alice")]).unwrap();
        catalog.apply(&AccessControlChange::DropRole("alice".to_string()));
        assert!(!catalog.has_role("alice"));
    }
}
