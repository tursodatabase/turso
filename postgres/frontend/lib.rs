mod aliases;
mod catalog;
mod copy;
mod functions;
mod session;

pub use session::PgConnection as Connection;
pub use session::{
    auto_attach_schemas, open_database, open_database_with_io, split_statements, PgConnection,
    PgQueryRunner,
};
pub use turso_core::{
    Database, DatabaseOpts, Func, LimboError, Numeric, OpenFlags, PlatformIO, Result, StepResult,
};

pub mod vtab {
    pub use turso_core::VirtualTable;
}
