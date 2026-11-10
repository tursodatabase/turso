//! Row-level security policies.
//!
//! A table with row-level security limits which of its rows a role may
//! access. Which tables have it is part of the dialect's schema state, like
//! the owner of a table, and [`crate::Dialect::table_rls_enabled`] says
//! which tables have it.
//!
//! Policies cannot be created yet, so a table with row-level security hides
//! every row from every role it applies to, as a PostgreSQL table with
//! row-level security and no policies does.

/// What a statement does to the rows of a table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Command {
    Select,
    Insert,
    Update,
    Delete,
}

/// Which rows of a table a role may access.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Policy {
    /// Every row.
    Allow,
    /// No row.
    Deny,
}
