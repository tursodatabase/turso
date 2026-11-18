//! Access control: who may do what to which objects and rows.
//!
//! Core enforces access control and the dialect stores it. Core compiles
//! the privilege checks and row-level security filters into each statement,
//! and asks the dialect for the facts they need: which roles exist, who owns
//! a relation, which privileges a role has, and which policies a table has.
//! The dialect keeps all of these in its own schema state, loaded with the
//! schema, so a check never reads the database.
//!
//! The SQLite frontend stores none of it, so a SQLite database behaves as if
//! every connection may do anything. The PostgreSQL frontend stores it in
//! tables shaped like PostgreSQL's catalogs: the roles of `pg_authid`, and
//! ownership, privileges and policies with each relation, like `pg_class`
//! and `pg_policy`.
//!
//! The subsystem has three parts:
//!
//! - [`roles`]: the ids of the identities that own objects and hold
//!   privileges. Every connection runs as a role, chosen when it is opened
//!   with a [`roles::SessionRole`].
//! - [`policy`]: row-level security. A table with row-level security limits
//!   which of its rows a role may read and write, as its
//!   [`policy::RowPolicy`] values say.
//! - [`permission`]: the error for an action that a role may not do.
//!   [`permission::PermissionDenied`] describes the action as data, so that
//!   each frontend can word the error as its users expect.
//!
//! # What core asks the dialect
//!
//! - Roles: [`crate::Dialect::role_by_name`],
//!   [`crate::Dialect::role_is_superuser`] and
//!   [`crate::Dialect::can_set_role`].
//! - Relations: [`crate::Dialect::relation_owner`],
//!   [`crate::Dialect::role_has_privilege`],
//!   [`crate::Dialect::table_rls_enabled`] and
//!   [`crate::Dialect::row_security_policies`].
//!
//! The defaults of these methods know only [`roles::RoleId::SUPERUSER`],
//! give every role every privilege and no table row-level security.
//! Statements that change access control, such as `CREATE ROLE`, `GRANT` and
//! `CREATE POLICY`, reach the dialect as a [`crate::CatalogChange`] through
//! [`crate::Dialect::translate_catalog_changes`], which the default refuses.
//!
//! # Where checks happen
//!
//! Privileges and policies are checked when a statement is prepared, not
//! when it runs. Changing the current role with
//! [`crate::Connection::set_current_role`] therefore prepares statements
//! again.
//!
//! A superuser passes every privilege check, and row-level security does not
//! limit superusers or the owner of a table.

pub mod permission;
pub mod policy;
pub mod roles;
