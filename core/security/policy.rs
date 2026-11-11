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
//! a permissive policy no row passes.

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

impl RowPolicy {
    /// Whether the policy decides which rows `role` may access for `command`.
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
