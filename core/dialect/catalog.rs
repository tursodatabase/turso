//! What translation tells the dialect about a statement that changes the
//! dialect's catalog of the main database.

use crate::{namespace::NamespaceId, schema::RelationKind, LimboError};
use std::borrow::Cow;
use turso_parser::ast::{self, Grantee, Privilege, RoleSpec};

/// A relation that a statement created, dropped or renamed. The engine makes
/// the change itself; the dialect records it in its own catalog through
/// [`Dialect::translate_relation_changes`](super::Dialect::translate_relation_changes).
#[derive(Debug, Clone)]
pub enum RelationChange<'a> {
    Created {
        namespace: NamespaceId,
        name: &'a str,
        kind: RelationKind,
        /// For an index, the table it is on, in the same namespace. Its
        /// owner owns the index.
        table: Option<&'a str>,
    },
    Dropped {
        namespace: NamespaceId,
        name: &'a str,
    },
    Renamed {
        namespace: NamespaceId,
        old_name: &'a str,
        /// Owned when the statement had to build the name, as for an
        /// automatic index that is renamed with its table.
        new_name: Cow<'a, str>,
    },
    /// `ALTER TABLE table RENAME COLUMN old_name TO new_name`.
    ColumnRenamed {
        namespace: NamespaceId,
        table: &'a str,
        old_name: &'a str,
        new_name: &'a str,
    },
}

/// A statement about roles, ownership, privileges or row-level security.
/// The engine stores none of these; the dialect does all of the work in
/// [`Dialect::translate_access_statement`](super::Dialect::translate_access_statement).
#[derive(Debug, Clone, Copy)]
pub enum AccessStatement<'a> {
    /// Whom the relation belongs to from now on.
    ChangeOwner {
        namespace: NamespaceId,
        name: &'a str,
        owner: &'a NewOwner,
    },
    /// `GRANT privileges ON relations TO grantees`, or `REVOKE privileges ON
    /// relations FROM grantees` when `revoke` is set.
    Grant {
        revoke: bool,
        relations: &'a [RelationName],
        privileges: &'a [Privilege],
        grantees: &'a [Grantee],
    },
    /// `ALTER TABLE ... ENABLE ROW LEVEL SECURITY`, or `DISABLE` when
    /// `enabled` is false.
    SetRowSecurity {
        namespace: NamespaceId,
        name: &'a str,
        enabled: bool,
    },
    /// `CREATE POLICY` on table `name`. Its expressions refer to the
    /// table's columns by their unqualified names.
    CreatePolicy {
        namespace: NamespaceId,
        name: &'a str,
        policy: &'a ast::CreatePolicy,
    },
    /// `DROP POLICY policy_name ON name`.
    DropPolicy {
        namespace: NamespaceId,
        name: &'a str,
        policy_name: &'a str,
        if_exists: bool,
    },
    /// `CREATE ROLE name`: a role that is not a superuser and cannot log in.
    CreateRole {
        name: &'a str,
        options: &'a ast::RoleOptions,
    },
    /// `GRANT roles TO grantees`, which makes each grantee a member of each
    /// role, or `REVOKE roles FROM grantees` when `revoke` is set.
    GrantRole {
        revoke: bool,
        roles: &'a [String],
        grantees: &'a [Grantee],
    },
}

impl AccessStatement<'_> {
    /// The error of a dialect that stores no roles, owners, privileges or
    /// policies.
    pub fn not_supported(&self) -> LimboError {
        let message = match self {
            Self::ChangeOwner {
                owner: NewOwner::Role(_),
                ..
            } => "ALTER ... OWNER TO is not supported".to_string(),
            Self::ChangeOwner {
                owner: NewOwner::Column(_),
                ..
            } => "ALTER SEQUENCE ... OWNED BY is not supported".to_string(),
            Self::Grant { revoke, .. } => {
                format!("{} is not supported", grant_or_revoke(*revoke))
            }
            Self::SetRowSecurity { .. } | Self::CreatePolicy { .. } | Self::DropPolicy { .. } => {
                "row-level security is not supported".to_string()
            }
            Self::CreateRole { .. } => "CREATE ROLE is not supported".to_string(),
            Self::GrantRole { revoke, .. } => format!(
                "{} of role membership is not supported",
                grant_or_revoke(*revoke)
            ),
        };
        LimboError::ParseError(message)
    }
}

fn grant_or_revoke(revoke: bool) -> &'static str {
    if revoke {
        "REVOKE"
    } else {
        "GRANT"
    }
}

/// A relation of the main database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationName {
    pub namespace: NamespaceId,
    pub name: String,
}

/// A change a statement makes to the dialect's catalog of the main
/// database: to a relation, or to the roles. Translation tells the dialect
/// about it through
/// [`Dialect::translate_catalog_changes`](super::Dialect::translate_catalog_changes), and the dialect's own bytecode
/// tells it back through [`Dialect::apply_catalog_change`](super::Dialect::apply_catalog_change) once the change
/// is written.
#[derive(Debug, Clone)]
pub enum CatalogChange {
    Create {
        namespace: NamespaceId,
        name: String,
        kind: RelationKind,
        /// For an index, the table it is on, in the same namespace. Its
        /// owner owns the index.
        table: Option<String>,
    },
    Drop {
        namespace: NamespaceId,
        name: String,
    },
    Rename {
        namespace: NamespaceId,
        old_name: String,
        new_name: String,
    },
    /// Whom the relation belongs to from now on.
    ChangeOwner {
        namespace: NamespaceId,
        name: String,
        owner: NewOwner,
    },
    /// `ALTER TABLE table RENAME COLUMN old_name TO new_name`.
    RenameColumn {
        namespace: NamespaceId,
        table: String,
        old_name: String,
        new_name: String,
    },
    /// `GRANT privileges ON relation TO grantees`, or `REVOKE privileges ON
    /// relation FROM grantees` when `revoke` is set.
    Grant {
        revoke: bool,
        namespace: NamespaceId,
        name: String,
        privileges: Vec<Privilege>,
        grantees: Vec<Grantee>,
    },
    /// `ALTER TABLE ... ENABLE ROW LEVEL SECURITY`, or `DISABLE` when
    /// `enabled` is false.
    SetRowSecurity {
        namespace: NamespaceId,
        name: String,
        enabled: bool,
    },
    /// `CREATE POLICY` on table `name`. Its expressions refer to the
    /// table's columns by their unqualified names.
    CreatePolicy {
        namespace: NamespaceId,
        name: String,
        policy: Box<ast::CreatePolicy>,
    },
    /// `DROP POLICY policy_name ON name`.
    DropPolicy {
        namespace: NamespaceId,
        name: String,
        policy_name: String,
        if_exists: bool,
    },
    /// `CREATE ROLE name`: a role that is not a superuser and cannot log in.
    CreateRole {
        name: String,
        options: ast::RoleOptions,
    },
    /// `GRANT roles TO grantees`, which makes each grantee a member of each
    /// role, or `REVOKE roles FROM grantees` when `revoke` is set.
    GrantRole {
        revoke: bool,
        roles: Vec<String>,
        grantees: Vec<Grantee>,
    },
}

/// Whom a relation belongs to after an [`AccessStatement::ChangeOwner`].
#[derive(Debug, Clone)]
pub enum NewOwner {
    /// A column of a table of the same namespace, as `ALTER SEQUENCE ...
    /// OWNED BY table.column` names one for a sequence, or no column. What a
    /// table owns changes owner with it, and not on its own.
    Column(Option<OwnerColumn>),
    /// A role, as `ALTER ... OWNER TO` names one.
    Role(RoleSpec),
}

/// The column that owns a sequence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnerColumn {
    pub table: String,
    pub column: String,
}
