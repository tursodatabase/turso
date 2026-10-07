//! Roles for access control.
//!
//! A role is an identity that owns objects and holds privileges. Every
//! database has a built-in superuser role. Roles created with `CREATE ROLE`
//! are stored in the `__turso_internal_roles` table, which is created
//! together with the first such role.
//!
//! Each schema holds a [`RoleCatalog`], an in-memory copy of all roles. It is
//! loaded together with the schema, and a statement that changes roles bumps
//! the schema cookie so that other connections load the change. Looking up a
//! role therefore never reads the database.
//!
//! Core does not add SQL syntax for roles. Each SQL frontend decides how roles
//! are exposed. The SQLite frontend does not expose them at all.

use std::collections::{BTreeMap, HashMap};

use crate::numeric::Numeric;
use crate::{LimboError, Result, Value};

pub const ROLES_TABLE_NAME: &str = "__turso_internal_roles";

pub const CREATE_ROLES_TABLE_SQL: &str = "CREATE TABLE __turso_internal_roles (\
    id INTEGER PRIMARY KEY, \
    name TEXT NOT NULL, \
    superuser INTEGER NOT NULL, \
    can_login INTEGER NOT NULL)";

pub const SELECT_ROLES_SQL: &str =
    "SELECT id, name, superuser, can_login FROM __turso_internal_roles";

pub const SUPERUSER_NAME: &str = "postgres";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RoleId(i64);

impl RoleId {
    /// The built-in superuser. Roles stored in the roles table have positive
    /// ids, so this id is never used by another role.
    pub const SUPERUSER: RoleId = RoleId(0);

    pub fn get(self) -> i64 {
        self.0
    }

    pub(crate) fn from_rowid(rowid: i64) -> Self {
        assert!(rowid > 0, "role rowid must be positive, got {rowid}");
        RoleId(rowid)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Role {
    pub id: RoleId,
    pub name: String,
    /// A superuser passes every privilege check.
    pub superuser: bool,
    /// Whether a session can be started as this role.
    pub can_login: bool,
}

/// All roles of a database, including the built-in superuser.
#[derive(Debug, Clone)]
pub struct RoleCatalog {
    roles: BTreeMap<RoleId, Role>,
    ids_by_name: HashMap<String, RoleId>,
}

impl Default for RoleCatalog {
    fn default() -> Self {
        Self::new()
    }
}

impl RoleCatalog {
    /// Returns a catalog that contains only the built-in superuser.
    pub fn new() -> Self {
        let mut catalog = Self {
            roles: BTreeMap::new(),
            ids_by_name: HashMap::new(),
        };
        catalog.add(Role {
            id: RoleId::SUPERUSER,
            name: SUPERUSER_NAME.to_string(),
            superuser: true,
            can_login: true,
        });
        catalog
    }

    /// Returns a catalog with the built-in superuser and the roles read from
    /// the roles table. Each row is `(id, name, superuser, can_login)`.
    pub fn from_rows(rows: &[Vec<Value>]) -> Result<Self> {
        let mut catalog = Self::new();
        for row in rows {
            let role = role_from_row(row)?;
            if catalog.roles.contains_key(&role.id) || catalog.ids_by_name.contains_key(&role.name)
            {
                return Err(LimboError::Corrupt(format!(
                    "{ROLES_TABLE_NAME} has a duplicate role: {role:?}"
                )));
            }
            catalog.add(role);
        }
        Ok(catalog)
    }

    pub fn get(&self, id: RoleId) -> Option<&Role> {
        self.roles.get(&id)
    }

    /// Role names are case-sensitive.
    pub fn get_by_name(&self, name: &str) -> Option<&Role> {
        self.ids_by_name.get(name).and_then(|id| self.roles.get(id))
    }

    /// Returns all roles in the order of their ids. The built-in superuser
    /// comes first.
    pub fn iter(&self) -> impl Iterator<Item = &Role> {
        self.roles.values()
    }

    /// A role that does not exist is not a superuser.
    pub fn is_superuser(&self, id: RoleId) -> bool {
        self.get(id).is_some_and(|role| role.superuser)
    }

    /// Whether a session whose login role is `member` may switch to `role`
    /// with `SET ROLE`. Role membership does not exist yet, so only a
    /// superuser may switch to another role.
    pub fn can_set_role(&self, member: RoleId, role: RoleId) -> bool {
        member == role || self.is_superuser(member)
    }

    pub fn add(&mut self, role: Role) {
        assert!(
            !self.roles.contains_key(&role.id),
            "role id {:?} is already in use",
            role.id
        );
        let previous = self.ids_by_name.insert(role.name.clone(), role.id);
        assert!(previous.is_none(), "role {} already exists", role.name);
        self.roles.insert(role.id, role);
    }
}

fn role_from_row(row: &[Value]) -> Result<Role> {
    let [id, name, superuser, can_login] = row else {
        return Err(corrupt_row(row));
    };
    match (id, name, superuser, can_login) {
        (
            Value::Numeric(Numeric::Integer(id)),
            Value::Text(name),
            Value::Numeric(Numeric::Integer(superuser @ (0 | 1))),
            Value::Numeric(Numeric::Integer(can_login @ (0 | 1))),
        ) if *id > 0 => Ok(Role {
            id: RoleId::from_rowid(*id),
            name: name.as_str().to_string(),
            superuser: *superuser == 1,
            can_login: *can_login == 1,
        }),
        _ => Err(corrupt_row(row)),
    }
}

fn corrupt_row(row: &[Value]) -> LimboError {
    LimboError::Corrupt(format!("{ROLES_TABLE_NAME} has an invalid row: {row:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_catalog_contains_only_the_superuser() {
        let catalog = RoleCatalog::new();

        let roles: Vec<_> = catalog.iter().collect();
        assert_eq!(roles.len(), 1);
        assert_eq!(roles[0].id, RoleId::SUPERUSER);
        assert_eq!(roles[0].name, SUPERUSER_NAME);
        assert!(catalog.is_superuser(RoleId::SUPERUSER));
    }

    #[test]
    fn from_rows_adds_roles_after_the_superuser() {
        let catalog =
            RoleCatalog::from_rows(&[role_row(1, "alice"), role_row(2, "Alice")]).unwrap();

        let names: Vec<_> = catalog.iter().map(|role| role.name.as_str()).collect();
        assert_eq!(names, vec![SUPERUSER_NAME, "alice", "Alice"]);
        let alice = catalog.get_by_name("alice").unwrap();
        assert!(!alice.superuser);
        assert!(!alice.can_login);
        assert!(!catalog.is_superuser(alice.id));
    }

    #[test]
    fn from_rows_rejects_a_duplicate_role_name() {
        let error =
            RoleCatalog::from_rows(&[role_row(1, "alice"), role_row(2, "alice")]).unwrap_err();

        assert!(matches!(error, LimboError::Corrupt(_)), "{error}");
    }

    #[test]
    fn from_rows_rejects_an_invalid_row() {
        let error = RoleCatalog::from_rows(&[vec![Value::from_i64(1)]]).unwrap_err();

        assert!(matches!(error, LimboError::Corrupt(_)), "{error}");
    }

    #[test]
    fn only_a_superuser_may_set_another_role() {
        let catalog = RoleCatalog::from_rows(&[role_row(1, "alice"), role_row(2, "bob")]).unwrap();
        let alice = catalog.get_by_name("alice").unwrap().id;
        let bob = catalog.get_by_name("bob").unwrap().id;

        assert!(catalog.can_set_role(RoleId::SUPERUSER, alice));
        assert!(catalog.can_set_role(alice, alice));
        assert!(!catalog.can_set_role(alice, bob));
        assert!(!catalog.can_set_role(alice, RoleId::SUPERUSER));
    }

    fn role_row(id: i64, name: &str) -> Vec<Value> {
        vec![
            Value::from_i64(id),
            Value::build_text(name.to_string()),
            Value::from_i64(0),
            Value::from_i64(0),
        ]
    }
}
