//! Row-level security policies.
//!
//! A table with row-level security limits which of its rows a role may
//! access. Which tables have it and their policies are part of the dialect's
//! schema state, like the owner of a table.
//! [`crate::Dialect::table_rls_enabled`] says which tables have it, and
//! [`crate::Dialect::row_security_policies`] gives the policies.
//!
//! The policies are stored but do not decide which rows a role may access
//! yet, so a table with row-level security hides every row from every role
//! it applies to, as a PostgreSQL table with row-level security and no
//! policies does.

use turso_parser::ast;

use crate::security::roles::RoleId;

/// What a statement does to the rows of a table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Command {
    Select,
    Insert,
    Update,
    Delete,
    Truncate,
}

/// Which rows of a table a role may access.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Policy {
    /// Every row.
    Allow,
    /// No row.
    Deny,
}

/// A policy of a table, as `CREATE POLICY` defines it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowPolicy {
    pub name: String,
    /// Permissive policies combine with OR, restrictive ones with AND.
    pub permissive: bool,
    pub command: ast::PolicyCommand,
    /// The roles the policy applies to. [`RoleId::PUBLIC`] stands for every
    /// role.
    pub roles: Vec<RoleId>,
    /// Which existing rows the policy lets the roles access. It refers to the
    /// columns of the table by their unqualified names.
    pub using_expr: Option<ast::Expr>,
    /// Which new rows the policy lets the roles write.
    pub check_expr: Option<ast::Expr>,
}
