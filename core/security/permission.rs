//! The error for an action that a role may not do.
//!
//! Core reports a denied action as a [`PermissionDenied`] value, not as a
//! message. It travels as [`crate::LimboError::PermissionDenied`], and each
//! frontend turns it into the message its users expect, so core never needs
//! to know how, for example, PostgreSQL words a permission error. The
//! [`Display`](fmt::Display) implementation gives a generic message for
//! frontends that have no wording of their own.

use std::fmt;

/// A role tried to do `action` on `object` and was refused for `reason`.
///
/// ```
/// use turso_core::security::permission::{Action, Object, PermissionDenied};
///
/// let denied = PermissionDenied::privilege(Action::Read, Object::Table("t".to_string()));
/// assert_eq!(denied.to_string(), "permission denied: read on table t");
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PermissionDenied {
    /// What the role tried to do.
    pub action: Action,
    /// What the role tried to do it to.
    pub object: Object,
    /// Why the role may not do it.
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

    /// A row that the role writes to `table` fails a condition of the
    /// table's row-level security policies: of restrictive policy `policy`,
    /// or of the permissive policies together when it is None. The condition
    /// is a USING condition checked on the existing row when
    /// `using_condition` is set, and otherwise one checked on the new row.
    pub fn policy(table: Object, policy: Option<String>, using_condition: bool) -> Self {
        Self {
            action: Action::Write,
            object: table,
            reason: Reason::Policy {
                policy,
                using_condition,
            },
        }
    }
}

impl fmt::Display for PermissionDenied {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.reason {
            Reason::Privilege => write!(f, "permission denied: {} on {}", self.action, self.object),
            Reason::Owner => write!(
                f,
                "permission denied: {} on {} needs its owner",
                self.action, self.object
            ),
            Reason::Policy { .. } => write!(
                f,
                "permission denied: {} on {} violates its row-level security policy",
                self.action, self.object
            ),
        }
    }
}

/// Why a role may not do an action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reason {
    /// The role lacks a privilege, such as SELECT on a table. See
    /// [`PermissionDenied::privilege`].
    Privilege,
    /// Only the owner of the object, or a superuser, may do the action. See
    /// [`PermissionDenied::owner`].
    Owner,
    /// A written row fails a row-level security policy. See
    /// [`PermissionDenied::policy`].
    Policy {
        /// The restrictive policy that the row fails, or None when the row
        /// fails the permissive policies together.
        policy: Option<String>,
        /// Whether the failed condition is a USING condition, checked on the
        /// existing row, rather than a WITH CHECK condition, checked on the
        /// new row.
        using_condition: bool,
    },
}

/// What a role tried to do.
///
/// Actions are coarse on purpose: a frontend picks its own wording, such as
/// PostgreSQL's `permission denied for table t`, from the [`Object`] and the
/// [`Reason`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    /// Read rows or values, as SELECT does, or read a file.
    Read,
    /// Insert, update or delete rows.
    Write,
    /// Create an object.
    Create,
    /// Create a temporary object.
    CreateTemporary,
    /// Create a trigger on a table.
    CreateTrigger,
    /// Change an object, as ALTER does.
    Alter,
    /// Drop an object.
    Drop,
    /// Switch the current role with `SET ROLE`.
    SetRole,
    /// Run ANALYZE.
    Analyze,
    /// Run VACUUM.
    Vacuum,
    /// Run REINDEX.
    Reindex,
    /// Run OPTIMIZE.
    Optimize,
    /// Attach a database.
    Attach,
    /// Detach a database.
    Detach,
    /// Call a function.
    Execute,
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
            Action::Execute => "execute",
        };
        f.write_str(name)
    }
}

/// What a role tried to do an action to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Object {
    /// A database by its schema name, such as `main`.
    Database(String),
    /// The schema of a database, by the database's schema name. Objects are
    /// created in it.
    Schema(String),
    /// A table, by name.
    Table(String),
    /// A view, by name.
    View(String),
    /// An index, by name.
    Index(String),
    /// A trigger, by name.
    Trigger(String),
    /// A sequence, by name.
    Sequence(String),
    /// A type or domain, by name.
    Type(String),
    /// A function, by name, such as `load_extension`.
    Function(String),
    /// A role, by name.
    Role(String),
    /// A connection or database setting, such as a pragma.
    Setting(String),
    /// A file on the host, by path, such as the file of `COPY ... FROM`.
    File(String),
}

impl fmt::Display for Object {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Object::Database(name) => write!(f, "database {name}"),
            Object::Schema(name) => write!(f, "schema {name}"),
            Object::Table(name) => write!(f, "table {name}"),
            Object::View(name) => write!(f, "view {name}"),
            Object::Index(name) => write!(f, "index {name}"),
            Object::Trigger(name) => write!(f, "trigger {name}"),
            Object::Sequence(name) => write!(f, "sequence {name}"),
            Object::Type(name) => write!(f, "type {name}"),
            Object::Function(name) => write!(f, "function {name}"),
            Object::Role(name) => write!(f, "role {name}"),
            Object::Setting(name) => write!(f, "setting {name}"),
            Object::File(name) => write!(f, "file {name}"),
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
