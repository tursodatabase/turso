//! Access control: who may do what to which objects and rows.
//!
//! Core provides the building blocks for access control. The SQL frontends
//! decide which parts their users see. The SQLite frontend exposes none of
//! it, so a SQLite database behaves as if every connection may do anything.
//!
//! The subsystem has three parts:
//!
//! - [`roles`]: the identities that own objects and hold privileges. Every
//!   connection runs as a role, starting as [`roles::RoleId::SUPERUSER`].
//!   Roles are stored in a table and kept in memory as a
//!   [`roles::RoleCatalog`] in each schema.
//! - [`policy`]: row-level security. A table with row-level security limits
//!   which of its rows a role may read and write, as its
//!   [`policy::RowPolicy`] values say.
//! - [`permission`]: the error for an action that a role may not do.
//!   [`permission::PermissionDenied`] describes the action as data, so that
//!   each frontend can word the error as its users expect.
//!
//! # Where checks happen
//!
//! Privileges and policies are checked when a statement is prepared, not
//! when it runs. Changing the current role with
//! [`crate::Connection::set_current_role`] therefore prepares statements
//! again.
//!
//! Core does not store ownership, privileges or policies itself. They are
//! part of the dialect's schema state, and core asks the dialect for them
//! through [`crate::Dialect::relation_owner`],
//! [`crate::Dialect::role_has_privilege`],
//! [`crate::Dialect::table_rls_enabled`] and
//! [`crate::Dialect::row_security_policies`]. The defaults of these methods
//! give every role every privilege and no table row-level security.
//!
//! A superuser passes every privilege check, and row-level security does not
//! limit superusers or the owner of a table.

pub mod permission;
pub mod policy;
pub mod roles;
