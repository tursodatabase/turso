//! Per-dialect built-in catalog tables.
//!
//! The single [`sqlite`] module here owns the set of catalog tables that ship
//! with every Turso build (`pragma_*`, `json_each`/`json_tree`,
//! `sqlite_dbpage`, `btree_dump`, `sqlite_turso_types`). `Schema::with_options`
//! reaches them through this module rather than touching the implementations
//! directly, so an alternative catalog set can be substituted — by a
//! downstream crate or a future feature gate — without churning the shared
//! schema-construction path.

pub mod sqlite;
