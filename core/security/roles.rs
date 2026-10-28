//! Roles for access control.
//!
//! A role is an identity that owns objects and holds privileges. Every
//! database has two built-in roles: a superuser, and `public`, which every
//! role is a member of. The dialect names them. Roles created with
//! `CREATE ROLE` are stored in the `__turso_internal_roles` table, which is
//! created together with the first such role.
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
    ignores_row_policies INTEGER NOT NULL)";

pub const SELECT_ROLES_SQL: &str =
    "SELECT id, name, superuser, ignores_row_policies FROM __turso_internal_roles";

/// The names of the built-in roles. Each dialect chooses them, so that they
/// match the names its users expect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BuiltinRoleNames {
    pub superuser: &'static str,
    pub public: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RoleId(i64);

impl RoleId {
    /// The built-in superuser. Roles stored in the roles table have positive
    /// ids, so this id is never used by another role.
    pub const SUPERUSER: RoleId = RoleId(0);
    /// The built-in role that every role is a member of. No connection can
    /// run as it.
    pub const PUBLIC: RoleId = RoleId(-1);

    pub fn get(self) -> i64 {
        self.0
    }

    pub fn is_builtin(self) -> bool {
        self.0 <= 0
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
    /// A superuser passes every privilege check and ignores row policies.
    pub superuser: bool,
    /// Row policies do not apply to this role. It still needs privileges.
    pub ignores_row_policies: bool,
}

/// All roles of a database, including the built-in ones.
#[derive(Debug, Clone)]
pub struct RoleCatalog {
    roles: BTreeMap<RoleId, Role>,
    ids_by_name: HashMap<String, RoleId>,
}

impl RoleCatalog {
    /// Returns a catalog that contains only the built-in roles.
    pub fn new(names: BuiltinRoleNames) -> Self {
        let mut catalog = Self {
            roles: BTreeMap::new(),
            ids_by_name: HashMap::new(),
        };
        catalog.add(Role {
            id: RoleId::PUBLIC,
            name: names.public.to_string(),
            superuser: false,
            ignores_row_policies: false,
        });
        catalog.add(Role {
            id: RoleId::SUPERUSER,
            name: names.superuser.to_string(),
            superuser: true,
            ignores_row_policies: true,
        });
        catalog
    }

    /// Returns a catalog with the built-in roles of this catalog and the
    /// roles read from the roles table. Each row is
    /// `(id, name, superuser, ignores_row_policies)`.
    pub fn with_stored_roles(&self, rows: &[Vec<Value>]) -> Result<Self> {
        let mut catalog = Self {
            roles: BTreeMap::new(),
            ids_by_name: HashMap::new(),
        };
        for role in self.iter().filter(|role| role.id.is_builtin()) {
            catalog.add(role.clone());
        }
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

    /// Returns all roles in the order of their ids. The built-in roles come
    /// first.
    pub fn iter(&self) -> impl Iterator<Item = &Role> {
        self.roles.values()
    }

    /// A role that does not exist is not a superuser.
    pub fn is_superuser(&self, id: RoleId) -> bool {
        self.get(id).is_some_and(|role| role.superuser)
    }

    /// Whether a session whose login role is `member` may switch to `role`
    /// with `SET ROLE`. Role membership does not exist yet, so only a
    /// superuser may switch to another role. No session may switch to
    /// `public`.
    pub fn can_set_role(&self, member: RoleId, role: RoleId) -> bool {
        role != RoleId::PUBLIC && (member == role || self.is_superuser(member))
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
    let [id, name, superuser, ignores_row_policies] = row else {
        return Err(corrupt_row(row));
    };
    match (id, name, superuser, ignores_row_policies) {
        (
            Value::Numeric(Numeric::Integer(id)),
            Value::Text(name),
            Value::Numeric(Numeric::Integer(superuser @ (0 | 1))),
            Value::Numeric(Numeric::Integer(ignores_row_policies @ (0 | 1))),
        ) if *id > 0 => Ok(Role {
            id: RoleId::from_rowid(*id),
            name: name.as_str().to_string(),
            superuser: *superuser == 1,
            ignores_row_policies: *ignores_row_policies == 1,
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

    const NAMES: BuiltinRoleNames = BuiltinRoleNames {
        superuser: "admin",
        public: "everyone",
    };

    #[test]
    fn new_catalog_contains_only_the_builtin_roles_named_by_the_caller() {
        let catalog = RoleCatalog::new(NAMES);

        let roles: Vec<_> = catalog
            .iter()
            .map(|role| (role.id, role.name.as_str()))
            .collect();
        assert_eq!(
            roles,
            vec![(RoleId::PUBLIC, "everyone"), (RoleId::SUPERUSER, "admin")]
        );
        assert!(catalog.is_superuser(RoleId::SUPERUSER));
        assert!(!catalog.is_superuser(RoleId::PUBLIC));
    }

    #[test]
    fn with_stored_roles_adds_roles_after_the_builtin_roles() {
        let catalog = RoleCatalog::new(NAMES)
            .with_stored_roles(&[role_row(1, "alice", 0), role_row(2, "Alice", 1)])
            .unwrap();

        let names: Vec<_> = catalog.iter().map(|role| role.name.as_str()).collect();
        assert_eq!(names, vec!["everyone", "admin", "alice", "Alice"]);
        let alice = catalog.get_by_name("alice").unwrap();
        assert!(!alice.superuser);
        assert!(!alice.ignores_row_policies);
        assert!(!catalog.is_superuser(alice.id));
        assert!(catalog.get_by_name("Alice").unwrap().ignores_row_policies);
    }

    #[test]
    fn with_stored_roles_drops_roles_that_are_no_longer_stored() {
        let catalog = RoleCatalog::new(NAMES)
            .with_stored_roles(&[role_row(1, "alice", 0)])
            .unwrap();

        let reloaded = catalog.with_stored_roles(&[]).unwrap();

        assert!(reloaded.get_by_name("alice").is_none());
        assert!(reloaded.get(RoleId::SUPERUSER).is_some());
        assert!(reloaded.get(RoleId::PUBLIC).is_some());
    }

    #[test]
    fn with_stored_roles_rejects_a_duplicate_role_name() {
        let error = RoleCatalog::new(NAMES)
            .with_stored_roles(&[role_row(1, "alice", 0), role_row(2, "alice", 0)])
            .unwrap_err();

        assert!(matches!(error, LimboError::Corrupt(_)), "{error}");
    }

    #[test]
    fn with_stored_roles_rejects_a_stored_role_named_like_a_builtin_role() {
        let error = RoleCatalog::new(NAMES)
            .with_stored_roles(&[role_row(1, "everyone", 0)])
            .unwrap_err();

        assert!(matches!(error, LimboError::Corrupt(_)), "{error}");
    }

    #[test]
    fn with_stored_roles_rejects_an_invalid_row() {
        let error = RoleCatalog::new(NAMES)
            .with_stored_roles(&[vec![Value::from_i64(1)]])
            .unwrap_err();

        assert!(matches!(error, LimboError::Corrupt(_)), "{error}");
    }

    #[test]
    fn only_a_superuser_may_set_another_role_and_no_one_may_set_public() {
        let catalog = RoleCatalog::new(NAMES)
            .with_stored_roles(&[role_row(1, "alice", 0), role_row(2, "bob", 0)])
            .unwrap();
        let alice = catalog.get_by_name("alice").unwrap().id;
        let bob = catalog.get_by_name("bob").unwrap().id;

        assert!(catalog.can_set_role(RoleId::SUPERUSER, alice));
        assert!(catalog.can_set_role(alice, alice));
        assert!(!catalog.can_set_role(alice, bob));
        assert!(!catalog.can_set_role(alice, RoleId::SUPERUSER));
        assert!(!catalog.can_set_role(RoleId::SUPERUSER, RoleId::PUBLIC));
    }

    fn role_row(id: i64, name: &str, ignores_row_policies: i64) -> Vec<Value> {
        vec![
            Value::from_i64(id),
            Value::build_text(name.to_string()),
            Value::from_i64(0),
            Value::from_i64(ignores_row_policies),
        ]
    }
}
