mod acl;
mod auth_functions;
mod auth_members;
mod catalog;
mod classes;
mod copy;
mod error;
mod functions;
mod open;
mod options;
mod pg_type;
mod policies;
mod roles;
mod schema_state;
mod session;
mod settings;
mod statement;
mod unnest;

pub use catalog::PostgresDialect;
pub use copy::CopyTextEncoder;
pub use error::{sql_parse_error, Error, ErrorReport, Result};
pub use open::{open_database_async, OpenDatabaseState};
pub use session::PgConnection as Connection;
pub use session::{
    open_database, open_database_with_io, split_statements, PgConnection, PgQueryRunner,
    TransactionState,
};
pub use statement::{Statement, StatementResultKind};
pub use turso_core::{
    types, Database, DatabaseOpts, Func, LimboError, Numeric, OpenFlags, PlatformIO, StepResult,
};

pub mod vtab {
    pub use turso_core::VirtualTable;
}
