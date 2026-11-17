//! The changes a statement makes to the dialect's catalog.

use crate::{namespace::NamespaceId, schema::RelationKind};
use turso_parser::ast::{self, Grantee, Privilege, RoleSpec};

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

/// Whom a relation belongs to after a [`CatalogChange::ChangeOwner`].
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
