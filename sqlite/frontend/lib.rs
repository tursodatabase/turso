//! The SQLite frontend.
//!
//! [`SqliteDialect`] is the [`turso_core::Dialect`] for SQLite: statements
//! are SQLite SQL and `sqlite_schema` rows are canonical SQLite text, parsed
//! with the SQLite parser.

pub use turso_core::dialect::{SQLITE_VERSION, SQLITE_VERSION_NUMBER};
pub use turso_dialect_definitions::SqliteDialect;

/// Tells core that a REAL cannot hold NaN in a binary that links this crate.
#[no_mangle]
pub fn turso_frontend_nan_is_a_value() -> bool {
    false
}
