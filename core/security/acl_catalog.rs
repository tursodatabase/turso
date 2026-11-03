//! The owners and access control lists of a database's objects.
//!
//! An object that a role granted or revoked something on has a row in the
//! `__turso_internal_acl` table, which is created with the first such row
//! and loaded with the schema, like the roles table. An object without a
//! row has the list its dialect registered for it, if it is a built-in
//! object such as a catalog table, or else the default list of its kind
//! with the superuser as owner.

use std::collections::HashMap;

use crate::namespace::NamespaceId;
use crate::numeric::Numeric;
use crate::security::privilege::{Acl, ObjectAccess, ObjectKind};
use crate::security::roles::RoleId;
use crate::{LimboError, Result, Value};

pub const ACL_TABLE_NAME: &str = "__turso_internal_acl";

pub const CREATE_ACL_TABLE_SQL: &str = "CREATE TABLE __turso_internal_acl (\
    id INTEGER PRIMARY KEY, \
    kind TEXT NOT NULL, \
    namespace INTEGER NOT NULL, \
    name TEXT NOT NULL, \
    owner INTEGER NOT NULL, \
    acl TEXT NOT NULL)";

pub const SELECT_ACL_SQL: &str =
    "SELECT id, kind, namespace, name, owner, acl FROM __turso_internal_acl";

/// Names one object that privileges apply to. Functions, settings, schemas
/// and the database itself are in the root namespace.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ObjectKey {
    pub kind: ObjectKind,
    pub namespace: NamespaceId,
    pub name: String,
}

impl ObjectKey {
    pub fn new(kind: ObjectKind, namespace: NamespaceId, name: impl Into<String>) -> Self {
        Self {
            kind,
            namespace,
            name: name.into(),
        }
    }

    pub fn root(kind: ObjectKind, name: impl Into<String>) -> Self {
        Self::new(kind, NamespaceId::ROOT, name)
    }
}

impl ObjectKind {
    /// The name the kind is stored under in the access control table.
    pub fn stored_name(self) -> &'static str {
        match self {
            ObjectKind::Table => "table",
            ObjectKind::View => "view",
            ObjectKind::Sequence => "sequence",
            ObjectKind::Function => "function",
            ObjectKind::Schema => "schema",
            ObjectKind::Database => "database",
            ObjectKind::Type => "type",
            ObjectKind::Setting => "setting",
        }
    }

    pub fn from_stored_name(name: &str) -> Option<ObjectKind> {
        [
            ObjectKind::Table,
            ObjectKind::View,
            ObjectKind::Sequence,
            ObjectKind::Function,
            ObjectKind::Schema,
            ObjectKind::Database,
            ObjectKind::Type,
            ObjectKind::Setting,
        ]
        .into_iter()
        .find(|kind| kind.stored_name() == name)
    }
}

/// A list read from the access control table, with the rowid of its row.
#[derive(Debug, Clone)]
struct StoredAccess {
    rowid: i64,
    access: ObjectAccess,
}

#[derive(Debug, Clone, Default)]
pub struct AclCatalog {
    /// The lists the dialect registered for built-in objects.
    builtin: HashMap<ObjectKey, ObjectAccess>,
    /// The lists read from the access control table.
    stored: HashMap<ObjectKey, StoredAccess>,
}

impl AclCatalog {
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers the list of a built-in object, such as a catalog table. A
    /// stored list for the same object takes its place.
    pub fn register_builtin(&mut self, key: ObjectKey, access: ObjectAccess) {
        self.builtin.insert(key, access);
    }

    /// Returns the list of `key`, or `None` if the object has neither a
    /// stored nor a registered list and so has the default list of its kind.
    pub fn get(&self, key: &ObjectKey) -> Option<&ObjectAccess> {
        self.stored
            .get(key)
            .map(|stored| &stored.access)
            .or_else(|| self.builtin.get(key))
    }

    /// The rowid of the object's row in the access control table, if it has
    /// one.
    pub fn stored_rowid(&self, key: &ObjectKey) -> Option<i64> {
        self.stored.get(key).map(|stored| stored.rowid)
    }

    /// Records the list that was just written to the row `rowid`.
    pub fn set_stored(&mut self, key: ObjectKey, rowid: i64, access: ObjectAccess) {
        self.stored.insert(key, StoredAccess { rowid, access });
    }

    /// Replaces the lists read from the access control table with the lists
    /// in `rows`. Each row is `(id, kind, namespace, name, owner, acl)`.
    pub fn load_stored(&mut self, rows: &[Vec<Value>]) -> Result<()> {
        let mut stored = HashMap::new();
        for row in rows {
            let (rowid, key, access) = access_from_row(row)?;
            if stored
                .insert(key.clone(), StoredAccess { rowid, access })
                .is_some()
            {
                return Err(LimboError::Corrupt(format!(
                    "{ACL_TABLE_NAME} has two rows for {key:?}"
                )));
            }
        }
        self.stored = stored;
        Ok(())
    }
}

fn access_from_row(row: &[Value]) -> Result<(i64, ObjectKey, ObjectAccess)> {
    let corrupt = || LimboError::Corrupt(format!("{ACL_TABLE_NAME} has an invalid row: {row:?}"));
    let [rowid, kind, namespace, name, owner, acl] = row else {
        return Err(corrupt());
    };
    let (
        Value::Numeric(Numeric::Integer(rowid)),
        Value::Text(kind),
        Value::Numeric(Numeric::Integer(namespace)),
        Value::Text(name),
        Value::Numeric(Numeric::Integer(owner)),
        Value::Text(acl),
    ) = (rowid, kind, namespace, name, owner, acl)
    else {
        return Err(corrupt());
    };
    let kind = ObjectKind::from_stored_name(kind.as_str()).ok_or_else(corrupt)?;
    let namespace = u32::try_from(*namespace).map_err(|_| corrupt())?;
    let owner = RoleId::from_stored(*owner)
        .filter(|owner| *owner != RoleId::PUBLIC)
        .ok_or_else(corrupt)?;
    let acl = Acl::from_stored(acl.as_str())?;
    Ok((
        *rowid,
        ObjectKey::new(kind, NamespaceId::new(namespace), name.as_str()),
        ObjectAccess { owner, acl },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::security::privilege::{Privilege, Privileges};

    fn row(kind: &str, namespace: i64, name: &str, owner: i64, acl: &str) -> Vec<Value> {
        vec![
            Value::from_i64(1),
            Value::build_text(kind.to_string()),
            Value::from_i64(namespace),
            Value::build_text(name.to_string()),
            Value::from_i64(owner),
            Value::build_text(acl.to_string()),
        ]
    }

    #[test]
    fn stored_rows_are_read_and_registered_lists_are_kept() {
        let mut catalog = AclCatalog::new();
        let pg_class = ObjectKey::root(ObjectKind::Table, "pg_class");
        catalog.register_builtin(
            pg_class.clone(),
            ObjectAccess::default_for(ObjectKind::Table, RoleId::SUPERUSER),
        );

        catalog
            .load_stored(&[row("table", 1, "t", 0, "{3=r/0}")])
            .unwrap();

        let key = ObjectKey::new(ObjectKind::Table, NamespaceId::new(1), "t");
        assert_eq!(catalog.stored_rowid(&key), Some(1));
        let t = catalog.get(&key).unwrap();
        assert_eq!(t.owner, RoleId::SUPERUSER);
        assert_eq!(
            t.acl.privileges_of(RoleId::from_stored(3).unwrap()),
            Privileges::of(Privilege::Select)
        );
        assert!(catalog.get(&pg_class).is_some());
        assert!(catalog
            .get(&ObjectKey::root(ObjectKind::Table, "nothing"))
            .is_none());
    }

    #[test]
    fn a_stored_list_replaces_the_registered_list_of_the_same_object() {
        let mut catalog = AclCatalog::new();
        let key = ObjectKey::root(ObjectKind::Table, "pg_class");
        catalog.register_builtin(
            key.clone(),
            ObjectAccess::default_for(ObjectKind::Table, RoleId::SUPERUSER),
        );

        catalog
            .load_stored(&[row("table", 0, "pg_class", 0, "{}")])
            .unwrap();

        assert_eq!(catalog.get(&key).unwrap().acl, Acl::empty());
        catalog.load_stored(&[]).unwrap();
        assert_eq!(
            catalog.get(&key).unwrap().acl,
            Acl::default_for(ObjectKind::Table, RoleId::SUPERUSER)
        );
    }

    #[test]
    fn invalid_rows_are_corrupt() {
        for bad in [
            vec![Value::from_i64(1)],
            row("chair", 0, "t", 0, "{}"),
            row("table", -1, "t", 0, "{}"),
            row("table", 0, "t", -1, "{}"),
            row("table", 0, "t", -2, "{}"),
            row("table", 0, "t", 0, "nope"),
        ] {
            let error = AclCatalog::new().load_stored(&[bad.clone()]).unwrap_err();
            assert!(matches!(error, LimboError::Corrupt(_)), "{bad:?}: {error}");
        }
        let twice = AclCatalog::new()
            .load_stored(&[row("table", 0, "t", 0, "{}"), row("table", 0, "t", 0, "{}")])
            .unwrap_err();
        assert!(matches!(twice, LimboError::Corrupt(_)), "{twice}");
    }
}
