use std::sync::Arc;

mod aliases;
mod catalog;
mod copy;
mod functions;
mod session;

pub use catalog::PostgresDialect;
pub use session::PgConnection as Connection;
pub use session::{split_statements, PgConnection, PgQueryRunner};
pub use turso_core::{
    Database, DatabaseOpts, Func, LimboError, Numeric, OpenFlags, PlatformIO, Result, StepResult,
};

/// The PostgreSQL dialect in the form [`turso_core::OpenOptions::new`]
/// expects. Assemble open options (custom storage, durable storage,
/// database options) with it and open via [`turso_core::Database::open`]:
///
/// ```ignore
/// let db = Database::open(
///     io,
///     path,
///     OpenOptions::new(turso_pg::postgres_dialect())
///         .storage(storage)
///         .db_opts(opts),
/// )?;
/// ```
pub fn postgres_dialect() -> Arc<dyn turso_core::Dialect> {
    Arc::new(PostgresDialect)
}

pub mod vtab {
    pub use turso_core::VirtualTable;
}
