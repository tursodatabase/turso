#![cfg_attr(
    nightly,
    feature(
        allocator_api,
        btreemap_alloc,
        clone_from_ref,
        min_specialization,
        try_with_capacity,
        trusted_len,
        vec_push_within_capacity
    )
)]
#![recursion_limit = "256"]

pub mod alloc;
pub mod busy;
pub mod cdc;
#[cfg(feature = "cli_only")]
pub mod dbpage;
#[cfg(any(feature = "fuzz", feature = "bench"))]
pub mod functions;
pub mod index_method;
pub mod io;
#[cfg(all(feature = "json", any(feature = "fuzz", feature = "bench")))]
pub mod json;
#[cfg(all(
    test,
    feature = "fs",
    host_shared_wal,
    any(not(target_os = "windows"), feature = "experimental_win_iocp")
))]
mod multiprocess_tests;
pub mod mvcc;
#[cfg(any(feature = "fuzz", feature = "bench"))]
pub mod numeric;
pub mod schema;
pub mod skiplist;
pub mod state_machine;
pub mod storage;
pub mod types;
#[cfg(any(feature = "fuzz", feature = "bench"))]
pub mod vdbe;
pub mod vector;

#[cfg(feature = "cli_only")]
pub(crate) mod btree_dump;
pub(crate) mod sync;
pub(crate) mod thread;

mod assert;
mod connection;
mod database;
pub mod dialect;
mod error;
mod ext;
mod fast_lock;
mod function;
#[cfg(not(any(feature = "fuzz", feature = "bench")))]
mod functions;
mod incremental;
mod incremental_blob;
pub use incremental_blob::Blob;
mod info;
#[cfg(all(feature = "json", not(any(feature = "fuzz", feature = "bench"))))]
mod json;
#[cfg(not(any(feature = "fuzz", feature = "bench")))]
mod numeric;
mod parameters;
#[cfg(feature = "percentile")]
mod percentile;
mod pragma;
mod progress;
mod pseudo;
mod regexp;
#[cfg(feature = "series")]
mod series;
mod stack;
mod statement;
mod stats;
#[allow(dead_code)]
#[cfg(feature = "time")]
mod time;
mod translate;
mod util;
#[cfg(feature = "uuid")]
mod uuid;
#[cfg(not(any(feature = "fuzz", feature = "bench")))]
mod vdbe;
mod vtab;

pub use function::Func;
#[cfg(any(feature = "fuzz", feature = "bench"))]
pub use function::MathFunc;
/// The printf engine backing the SQL printf()/format() functions, also used
/// by the C API's sqlite3_mprintf/sqlite3_snprintf so both share one
/// formatting implementation.
pub use functions::printf::{exec_printf_values, printf_c_arg_plan, PrintfCArg};

use crate::{
    busy::{BusyHandler, BusyHandlerCallback},
    incremental::view::AllViewsTxState,
    index_method::IndexMethod,
    schema::Trigger,
    storage::encryption::AtomicCipherMode,
    sync::{atomic::AtomicBool, Arc, RwLock},
    translate::emitter::TransactionMode,
    vdbe::metrics::ConnectionMetrics,
};
use core::str;
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};
use schema::Schema;
use std::time::Duration;
use storage::sqlite3_ondisk::PageSize;
use tracing::instrument;
use turso_macros::AtomicEnum;
use turso_parser::{ast, ast::Cmd};

pub use cdc::{
    CaptureDataChangesExt, CaptureDataChangesInfo, CaptureDataChangesMode, CdcVersion,
    CDC_VERSION_CURRENT,
};
#[cfg(feature = "simulator")]
pub use connection::SubqueryUnnestingMode;
pub use connection::{resolve_ext_path, Connection, PrepareOptions, Row, StepResult, SymbolTable};
pub(crate) use connection::{AtomicTransactionState, TransactionState};
#[cfg(feature = "simulator")]
pub use database::{clear_database_registry, SharedWalTestingSnapshot};
pub(crate) use database::{is_memory_like, DatabaseCatalog, InitState};
pub use database::{
    Database, DatabaseOpts, EncryptionOpts, OpenDbAsyncPhase, OpenDbAsyncState, OpenOptions,
    SharedWalCoordinationOpenTelemetryMode, SharedWalOpenTelemetry,
};
#[cfg(test)]
pub(crate) use database::{DatabaseKey, RegistryEntry, DATABASE_MANAGER};
pub use dialect::{Dialect, SqliteDialect};
pub use error::{io_error, CompletionError, LimboError};
pub use function::ContextCollationFunction;
#[cfg(feature = "io_memory_yield")]
pub use io::MemoryYieldIO;
#[cfg(all(feature = "fs", target_family = "unix", not(miri)))]
pub use io::UnixIO;
#[cfg(all(feature = "fs", target_os = "linux", feature = "io_uring", not(miri)))]
pub use io::UringIO;
#[cfg(all(
    feature = "fs",
    target_os = "windows",
    feature = "experimental_win_iocp",
    not(miri)
))]
pub use io::WindowsIOCP;
pub use io::{
    clock::{Clock, MonotonicInstant, WallClockInstant},
    get_registered_io, list_registered_io, register_io, unregister_io, Buffer, Completion,
    CompletionType, File, GroupCompletion, MemoryIO, OpenFlags, PlatformIO, SharedBufferData,
    SyscallIO, WriteCompletion, IO,
};
pub use numeric::{nonnan::NonNan, Numeric};
pub use statement::{ColumnTypeInfo, ColumnTypeKind, Statement, StatementStatusCounter};
#[cfg(any(test, feature = "test_helper"))]
pub use storage::wal::set_checkpoint_start_before_lock_hook;
pub use storage::{
    buffer_pool::BufferPool,
    database::{DatabaseStorage, IOContext},
    encryption::{CipherMode, EncryptionContext, EncryptionKey},
    page_transform::{PageCodec, PageCodecContext, PageCodecHeaderInfo, PageCodecId, PageLocation},
    pager::{Page, PageRef, Pager},
    wal::{CheckpointMode, CheckpointResult, Wal, WalAutoActions, WalFile, WalFileShared},
};
pub use translate::expr::{walk_expr_mut, WalkControl};
pub use turso_ext::ContextDestructor;
pub use turso_macros::{
    turso_assert, turso_assert_all, turso_assert_eq, turso_assert_greater_than,
    turso_assert_greater_than_or_equal, turso_assert_less_than, turso_assert_less_than_or_equal,
    turso_assert_ne, turso_assert_reachable, turso_assert_some, turso_assert_sometimes,
    turso_assert_sometimes_greater_than, turso_assert_sometimes_greater_than_or_equal,
    turso_assert_sometimes_less_than, turso_assert_sometimes_less_than_or_equal,
    turso_assert_unreachable, turso_debug_assert, turso_soft_unreachable,
};
pub use turso_parser::ast::EqpFormat;
pub use types::{IOResult, Value, ValueBlob, ValueRef};
pub use util::IOExt;
pub use vdbe::{
    builder::QueryMode, explain::EXPLAIN_COLUMNS, explain::EXPLAIN_QUERY_PLAN_COLUMNS,
    explain::EXPLAIN_QUERY_PLAN_JSON_COLUMNS, FromValueRow, PrepareContext, PreparedProgram,
    Program, Register,
};
pub use vtab::{InternalVirtualTable, InternalVirtualTableCursor, VirtualTable};

/// Database index for the main database (always 0 in SQLite).
pub const MAIN_DB_ID: usize = 0;

mod turso_types_vtab;

/// Database index for the temp database (always 1 in SQLite).
pub const TEMP_DB_ID: usize = 1;

/// First database index used for ATTACH-ed databases.
/// SQLite reserves 0 for "main" and 1 for "temp", so attached databases
/// start at index 2.
pub const FIRST_ATTACHED_DB_ID: usize = 2;

/// Sentinel used when a SQL schema qualifier references an attached
/// database name that cannot be resolved against the current
/// connection's attached catalog (e.g. after reloading a
/// `CREATE TEMP TRIGGER tr ON aux.x ...` row from `temp.sqlite_schema`
/// without `aux` being attached). Stored in
/// `Trigger::target_database_id` so filters never accidentally match a
/// real database. Never equal to any real db id — guaranteed by
/// `usize::MAX`.
pub const INVALID_DB_ID: usize = usize::MAX;

/// Returns true if the database index refers to "main" or "temp"
pub const fn is_main_or_temp_db(database_id: usize) -> bool {
    database_id == MAIN_DB_ID || database_id == TEMP_DB_ID
}

/// Returns true if the database index refers to an attached database
/// (i.e. not "main" and not "temp").
pub const fn is_attached_db(database_id: usize) -> bool {
    database_id >= FIRST_ATTACHED_DB_ID
}

pub type Result<T, E = LimboError> = std::result::Result<T, E>;

#[derive(Debug, AtomicEnum, Clone, Copy, PartialEq, Eq)]
pub enum SyncMode {
    Off = 0,
    Normal = 1,
    Full = 2,
}

/// Control where temporary tables and indices are stored.
/// Matches SQLite's PRAGMA temp_store values:
/// - 0 = DEFAULT (use compile-time default, which is FILE)
/// - 1 = FILE (always use temp files on disk)
/// - 2 = MEMORY (always use in-memory storage)
#[derive(Debug, AtomicEnum, Clone, Copy, PartialEq, Eq, Default)]
pub enum TempStore {
    #[default]
    Default = 0,
    File = 1,
    Memory = 2,
}

pub(crate) type MvStore = mvcc::MvStore<mvcc::MvccClock, alloc::DynAllocator>;

pub(crate) type MvCursor = mvcc::cursor::MvccLazyCursor<mvcc::MvccClock, alloc::DynAllocator>;

pub struct QueryRunner<'a> {
    conn: &'a Arc<Connection>,
    statements: &'a str,
    pending_error: Option<LimboError>,
    last_offset: usize,
}

impl<'a> QueryRunner<'a> {
    pub(crate) fn new(conn: &'a Arc<Connection>, statements: &'a [u8]) -> Self {
        let (statements, pending_error) = match str::from_utf8(statements) {
            Ok(statements) => (statements, None),
            Err(err) => (
                "",
                Some(LimboError::ParseError(format!(
                    "invalid UTF-8 in SQL input: {err}"
                ))),
            ),
        };
        Self {
            conn,
            statements,
            pending_error,
            last_offset: 0,
        }
    }
}

impl Iterator for QueryRunner<'_> {
    type Item = Result<Option<Statement>>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(err) = self.pending_error.take() {
            return Some(Err(err));
        }

        let remaining = &self.statements[self.last_offset..];
        match self.conn.parse_sql(remaining) {
            Ok((Some(cmd), byte_offset_end)) => {
                let input = remaining[..byte_offset_end].trim();
                self.last_offset += byte_offset_end;
                Some(self.conn.run_cmd(cmd, input))
            }
            Ok((None, _)) => None,
            Err(err) => {
                self.last_offset = self.statements.len();
                Some(Err(err))
            }
        }
    }
}
