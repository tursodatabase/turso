//! Row-level security policies.
//!
//! A table with row-level security limits which of its rows a role may
//! access. Which tables have it and their policies are part of the dialect's
//! schema state, like the owner of a table.
//! [`crate::Dialect::table_rls_enabled`] says which tables have it, and
//! [`crate::Dialect::row_security_policies`] gives the policies.
//!
//! A role reads the rows that pass at least one of the permissive policies
//! and every restrictive policy that apply to it, as in PostgreSQL. Without
//! a permissive policy no row passes. A row that the role writes must pass
//! the policies in the same way, or the statement fails with
//! [`crate::security::permission::PermissionDenied::policy`].
//!
//! Row-level security does not limit superusers or the owner of the table,
//! and it does not apply to TRUNCATE.

use turso_parser::ast;

use crate::security::roles::RoleId;

/// What a statement does to the rows of a table. It picks the policies that
/// apply, see [`RowPolicy::applies_to`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Command {
    /// Read rows.
    Select,
    /// Insert new rows.
    Insert,
    /// Update existing rows.
    Update,
    /// Delete existing rows.
    Delete,
    /// Delete every row. Row-level security does not apply to it.
    Truncate,
}

/// Whether row-level security limits which rows of a table a role may
/// access for a command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Policy {
    /// It does not: the role may access every row.
    Allow,
    /// These policies of the command, the ones that apply to the role,
    /// decide. Without a permissive one among them, no row passes.
    Apply(Vec<RowPolicy>),
}

/// A policy of a table, as `CREATE POLICY` defines it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowPolicy {
    /// The name of the policy, unique among the policies of its table.
    pub name: String,
    /// Permissive policies combine with OR, restrictive ones with AND.
    pub permissive: bool,
    /// The command the policy applies to, or `ALL` for every command.
    pub command: ast::PolicyCommand,
    /// The roles the policy applies to. [`RoleId::PUBLIC`] stands for every
    /// role.
    pub roles: Vec<RoleId>,
    /// The USING condition: which existing rows the policy lets the roles
    /// access. It refers to the columns of the table by their unqualified
    /// names.
    pub using_expr: Option<ast::Expr>,
    /// The WITH CHECK condition: which new rows the policy lets the roles
    /// write. It refers to the columns like `using_expr` does.
    pub check_expr: Option<ast::Expr>,
}

impl RowPolicy {
    /// Whether the policy decides which rows `role` may access for `command`:
    /// the policy is for `command` or for `ALL`, and for `role` or for
    /// [`RoleId::PUBLIC`].
    pub fn applies_to(&self, command: Command, role: RoleId) -> bool {
        let command_matches = match self.command {
            ast::PolicyCommand::All => true,
            ast::PolicyCommand::Select => command == Command::Select,
            ast::PolicyCommand::Insert => command == Command::Insert,
            ast::PolicyCommand::Update => command == Command::Update,
            ast::PolicyCommand::Delete => command == Command::Delete,
        };
        command_matches
            && self
                .roles
                .iter()
                .any(|policy_role| *policy_role == RoleId::PUBLIC || *policy_role == role)
    }
}
