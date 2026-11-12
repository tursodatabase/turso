//! Roles for access control.
//!
//! A role is an identity that owns objects and holds privileges. Two role ids
//! are built in: [RoleId::SUPERUSER], which every connection starts as, and
//! [RoleId::PUBLIC], which every role is a member of. Core gives them no
//! names; a frontend names them as its users expect. Roles created with
//! `CREATE ROLE` are stored in the `__turso_internal_roles` table, which is
//! created together with the first such role.
//!
//! Each schema holds a [`RoleCatalog`], an in-memory copy of the stored roles. It is
//! loaded together with the schema, and a statement that changes roles bumps
//! the schema cookie so that other connections load the change. Looking up a
//! role therefore never reads the database.
//!
//! Core does not add SQL syntax for roles. Each SQL frontend decides how roles
//! are exposed. The SQLite frontend does not expose them at all.

use std::collections::{BTreeMap, HashMap};

use crate::numeric::Numeric;
use crate::{LimboError, Result, Value};

/// The table that stores the roles created with `CREATE ROLE`.
pub const ROLES_TABLE_NAME: &str = "__turso_internal_roles";

/// Creates [`ROLES_TABLE_NAME`]. The rowid of a row is the id of its role.
pub const CREATE_ROLES_TABLE_SQL: &str = "CREATE TABLE __turso_internal_roles (\
    id INTEGER PRIMARY KEY, \
    name TEXT NOT NULL, \
    superuser INTEGER NOT NULL)";

/// The unique index on the role names of [`ROLES_TABLE_NAME`].
pub const ROLES_NAME_INDEX_NAME: &str = "__turso_internal_roles_name";

/// Keeps role names unique in the stored table, whatever writes it.
pub const CREATE_ROLES_NAME_INDEX_SQL: &str =
    "CREATE UNIQUE INDEX __turso_internal_roles_name ON __turso_internal_roles (name)";

/// Reads the rows of [`ROLES_TABLE_NAME`] in the form that
/// [`RoleCatalog::new`] takes.
pub const SELECT_ROLES_SQL: &str = "SELECT id, name, superuser FROM __turso_internal_roles";

/// The id of a role: one of the built-in ids, or the rowid of a role stored
/// in [`ROLES_TABLE_NAME`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RoleId(i64);

impl RoleId {
    /// The superuser that every connection starts as. It passes every
    /// privilege check. Roles stored in the roles table have positive ids, so
    /// this id is never used by another role.
    pub const SUPERUSER: RoleId = RoleId(0);
    /// The built-in role that every role is a member of. No connection can
    /// run as it.
    pub const PUBLIC: RoleId = RoleId(-1);

    /// The id as stored, such as in an owner column.
    pub fn get(self) -> i64 {
        self.0
    }

    /// The id of the stored role at `rowid`. Panics unless `rowid` is
    /// positive.
    pub(crate) fn from_rowid(rowid: i64) -> Self {
        assert!(rowid > 0, "role rowid must be positive, got {rowid}");
        RoleId(rowid)
    }

    /// A role id read from a stored column, such as `relowner` in the classes
    /// table: the bootstrap superuser (0) or a stored role's rowid. Panics
    /// for any other value.
    pub fn from_stored(id: i64) -> Self {
        if id == 0 {
            RoleId::SUPERUSER
        } else {
            Self::from_rowid(id)
        }
    }
}

/// A role stored in [`ROLES_TABLE_NAME`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Role {
    /// The id of the role, its rowid in the roles table.
    pub id: RoleId,
    /// The name of the role. Names are case-sensitive and unique.
    pub name: String,
    /// A superuser passes every privilege check.
    pub superuser: bool,
}

/// The roles stored in the roles table of a database, in memory. The
/// built-in role ids have no entry.
///
/// Each [`crate::schema::Schema`] holds one, loaded with the schema, so looking up a
/// role never reads the database. [`crate::Connection::role_catalog`] gives
/// the catalog of a connection.
#[derive(Debug, Clone, Default)]
pub struct RoleCatalog {
    roles: BTreeMap<RoleId, Role>,
    ids_by_name: HashMap<String, RoleId>,
}

impl RoleCatalog {
    /// Returns a catalog of the roles read from the roles table with
    /// [`SELECT_ROLES_SQL`]. Each row is `(id, name, superuser)`.
    ///
    /// Fails with [`LimboError::Corrupt`] for a row that is not a valid role,
    /// or for two rows with the same id or name.
    pub fn new(rows: &[Vec<Value>]) -> Result<Self> {
        let mut catalog = Self::default();
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

    /// The stored role with id `id`. None for a built-in id.
    pub fn get(&self, id: RoleId) -> Option<&Role> {
        self.roles.get(&id)
    }

    /// The stored role named `name`. Role names are case-sensitive.
    pub fn get_by_name(&self, name: &str) -> Option<&Role> {
        self.ids_by_name.get(name).and_then(|id| self.roles.get(id))
    }

    /// Returns all roles in the order of their ids.
    pub fn iter(&self) -> impl Iterator<Item = &Role> {
        self.roles.values()
    }

    /// Whether role `id` passes every privilege check. [RoleId::SUPERUSER]
    /// does. Any other role that is not in the catalog does not.
    pub fn is_superuser(&self, id: RoleId) -> bool {
        id == RoleId::SUPERUSER || self.get(id).is_some_and(|role| role.superuser)
    }

    /// Whether a session whose login role is `member` may switch to `role`
    /// with `SET ROLE`. Role membership does not exist yet, so only a
    /// superuser may switch to another role. No session may switch to
    /// `public`.
    pub fn can_set_role(&self, member: RoleId, role: RoleId) -> bool {
        role != RoleId::PUBLIC && (member == role || self.is_superuser(member))
    }

    /// Adds `role` to the catalog. Panics if its id or name is already in
    /// use.
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
    let [id, name, superuser] = row else {
        return Err(corrupt_row(row));
    };
    match (id, name, superuser) {
        (
            Value::Numeric(Numeric::Integer(id)),
            Value::Text(name),
            Value::Numeric(Numeric::Integer(superuser @ (0 | 1))),
        ) if *id > 0 => Ok(Role {
            id: RoleId::from_rowid(*id),
            name: name.as_str().to_string(),
            superuser: *superuser == 1,
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
    fn superuser_is_a_superuser_without_being_in_the_catalog() {
        let catalog = RoleCatalog::new(&[]).unwrap();

        assert!(catalog.get(RoleId::SUPERUSER).is_none());
        assert!(catalog.is_superuser(RoleId::SUPERUSER));
        assert!(!catalog.is_superuser(RoleId::PUBLIC));
    }

    #[test]
    fn new_reads_the_stored_roles() {
        let catalog =
            RoleCatalog::new(&[role_row(1, "alice", 0), role_row(2, "Alice", 1)]).unwrap();

        let names: Vec<_> = catalog.iter().map(|role| role.name.as_str()).collect();
        assert_eq!(names, vec!["alice", "Alice"]);
        let alice = catalog.get_by_name("alice").unwrap();
        assert!(!alice.superuser);
        assert!(!catalog.is_superuser(alice.id));
        assert!(catalog.is_superuser(catalog.get_by_name("Alice").unwrap().id));
    }

    #[test]
    fn new_rejects_a_duplicate_role_name() {
        let error =
            RoleCatalog::new(&[role_row(1, "alice", 0), role_row(2, "alice", 0)]).unwrap_err();

        assert!(matches!(error, LimboError::Corrupt(_)), "{error}");
    }

    #[test]
    fn new_rejects_an_invalid_row() {
        let error = RoleCatalog::new(&[vec![Value::from_i64(1)]]).unwrap_err();

        assert!(matches!(error, LimboError::Corrupt(_)), "{error}");
    }

    #[test]
    fn only_a_superuser_may_set_another_role_and_no_one_may_set_public() {
        let catalog = RoleCatalog::new(&[role_row(1, "alice", 0), role_row(2, "bob", 0)]).unwrap();
        let alice = catalog.get_by_name("alice").unwrap().id;
        let bob = catalog.get_by_name("bob").unwrap().id;

        assert!(catalog.can_set_role(RoleId::SUPERUSER, alice));
        assert!(catalog.can_set_role(alice, alice));
        assert!(!catalog.can_set_role(alice, bob));
        assert!(!catalog.can_set_role(alice, RoleId::SUPERUSER));
        assert!(!catalog.can_set_role(RoleId::SUPERUSER, RoleId::PUBLIC));
    }

    fn role_row(id: i64, name: &str, superuser: i64) -> Vec<Value> {
        vec![
            Value::from_i64(id),
            Value::build_text(name.to_string()),
            Value::from_i64(superuser),
        ]
    }
}
