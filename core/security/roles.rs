//! Role ids for access control.
//!
//! A role is an identity that owns objects and holds privileges. Core knows
//! a role only by its [`RoleId`]. Two ids are built in:
//! [RoleId::SUPERUSER], which passes every privilege check, and
//! [RoleId::PUBLIC], which every role is a member of. Core gives them no
//! names; a frontend names them as its users expect.
//!
//! Core does not store roles. The dialect does, as part of its schema state,
//! and core asks it about them through [`crate::Dialect::role_by_name`],
//! [`crate::Dialect::role_is_superuser`] and
//! [`crate::Dialect::can_set_role`]. `CREATE ROLE` reaches the dialect as a
//! [`crate::CatalogChange::CreateRole`]. A dialect that keeps no roles, such
//! as SQLite's, has only the built-in ids and refuses `CREATE ROLE`.

/// The role a new connection runs as. The program that opens the connection
/// chooses it; SQL cannot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionRole {
    Superuser,
    /// A role the dialect stores, by its case-sensitive name.
    Named(String),
}

/// The id of a role: one of the built-in ids, or a positive id that the
/// dialect gave a role it stores.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RoleId(i64);

impl RoleId {
    /// The superuser. It passes every privilege check. Stored roles have
    /// positive ids, so this id is never used by another role.
    pub const SUPERUSER: RoleId = RoleId(0);
    /// The built-in role that every role is a member of. No connection can
    /// run as it.
    pub const PUBLIC: RoleId = RoleId(-1);

    /// The id as stored, such as in an owner column.
    pub fn get(self) -> i64 {
        self.0
    }

    /// A role id as stored, such as in an owner column: the superuser (0) or
    /// a stored role's positive id. Panics for any other value.
    pub fn from_stored(id: i64) -> Self {
        assert!(id >= 0, "a stored role id must not be negative, got {id}");
        RoleId(id)
    }
}
