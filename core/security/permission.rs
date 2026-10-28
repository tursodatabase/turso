//! What a role was not allowed to do.
//!
//! Core reports a denied action as data, not as a message. Each frontend turns
//! it into the message its users expect, so core never needs to know how, for
//! example, PostgreSQL words a permission error.

use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PermissionDenied {
    pub action: Action,
    pub object: Object,
    pub reason: Reason,
}

impl PermissionDenied {
    /// The role lacks the privilege for `action` on `object`.
    pub fn privilege(action: Action, object: Object) -> Self {
        Self {
            action,
            object,
            reason: Reason::Privilege,
        }
    }

    /// Only the owner of `object` may do `action` on it.
    pub fn owner(action: Action, object: Object) -> Self {
        Self {
            action,
            object,
            reason: Reason::Owner,
        }
    }
}

impl fmt::Display for PermissionDenied {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.reason {
            Reason::Privilege => write!(f, "permission denied: {} on {}", self.action, self.object),
            Reason::Owner => write!(
                f,
                "permission denied: {} on {} needs its owner",
                self.action, self.object
            ),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reason {
    Privilege,
    Owner,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Read,
    Write,
    Create,
    CreateTemporary,
    CreateTrigger,
    Alter,
    Drop,
    SetRole,
    Analyze,
    Vacuum,
    Reindex,
    Optimize,
    Attach,
    Detach,
}

impl fmt::Display for Action {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Action::Read => "read",
            Action::Write => "write",
            Action::Create => "create",
            Action::CreateTemporary => "create temporary",
            Action::CreateTrigger => "create trigger",
            Action::Alter => "alter",
            Action::Drop => "drop",
            Action::SetRole => "set role",
            Action::Analyze => "analyze",
            Action::Vacuum => "vacuum",
            Action::Reindex => "reindex",
            Action::Optimize => "optimize",
            Action::Attach => "attach",
            Action::Detach => "detach",
        };
        f.write_str(name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Object {
    /// A database by its schema name, such as `main`.
    Database(String),
    /// The schema of a database, by the database's schema name. Objects are
    /// created in it.
    Schema(String),
    Table(String),
    View(String),
    MaterializedView(String),
    Index(String),
    Trigger(String),
    Sequence(String),
    Type(String),
    Role(String),
    /// A connection or database setting, such as a pragma.
    Setting(String),
    /// Storage that does not belong to a known object.
    Storage,
}

impl fmt::Display for Object {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Object::Database(name) => write!(f, "database {name}"),
            Object::Schema(name) => write!(f, "schema {name}"),
            Object::Table(name) => write!(f, "table {name}"),
            Object::View(name) => write!(f, "view {name}"),
            Object::MaterializedView(name) => write!(f, "materialized view {name}"),
            Object::Index(name) => write!(f, "index {name}"),
            Object::Trigger(name) => write!(f, "trigger {name}"),
            Object::Sequence(name) => write!(f, "sequence {name}"),
            Object::Type(name) => write!(f, "type {name}"),
            Object::Role(name) => write!(f, "role {name}"),
            Object::Setting(name) => write!(f, "setting {name}"),
            Object::Storage => f.write_str("database storage"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn denials_name_the_action_and_the_object() {
        let read = PermissionDenied::privilege(Action::Read, Object::Table("t".to_string()));
        let drop = PermissionDenied::owner(Action::Drop, Object::View("v".to_string()));

        assert_eq!(read.to_string(), "permission denied: read on table t");
        assert_eq!(
            drop.to_string(),
            "permission denied: drop on view v needs its owner"
        );
    }
}
