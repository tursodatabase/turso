//! JavaScript bindings for the Turso library.
//!
//! These bindings provide a thin layer that exposes Turso's Rust API to JavaScript,
//! maintaining close alignment with the underlying implementation while offering
//! the following core database operations:
//!
//! - Opening and closing database connections
//! - Preparing SQL statements
//! - Binding parameters to prepared statements
//! - Iterating through query results
//! - Managing the I/O event loop

#[cfg(feature = "browser")]
pub mod browser;

use napi::bindgen_prelude::*;
use napi::{Env, Task};
use napi_derive::napi;
use std::sync::{OnceLock, Weak};
use std::{
    cell::{Cell, RefCell},
    num::NonZeroUsize,
    sync::Arc,
};
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::fmt::format::FmtSpan;
use turso_core::SqliteDialect;

/// Shared ownership of a `turso_core::Statement` that can be explicitly finalized.
///
/// Both `Statement`/`BatchExecutor` and `Connection` hold an `Arc` to this
/// handle. When `Connection::close()` is called it sets every live handle to
/// `None`, which drops the `turso_core::Statement` and — crucially — releases the
/// `Arc<Connection>` held inside `Program`, breaking the reference chain that
/// would otherwise keep the `turso_core::Database` alive in the
/// `DATABASE_MANAGER` registry.
type StatementHandle = Arc<RefCell<Option<turso_core::Statement>>>;

/// Step result constants
const STEP_ROW: u32 = 1;
const STEP_DONE: u32 = 2;
const STEP_IO: u32 = 3;

/// The presentation mode for rows.
#[derive(Debug, Clone)]
enum PresentationMode {
    Expanded,
    Raw,
    Pluck,
}

/// A database: the shared per-file state from which connections are created.
/// All per-connection operations live on [`Connection`], returned by
/// `connectSync()`/`connectAsync()`.
#[napi]
#[derive(Clone)]
pub struct Database {
    inner: Option<Arc<DatabaseInner>>,
}

/// database inner is Send to the worker for initial connection
/// that's why we use OnceLock here - in order to make DatabaseInner Send and Sync
pub struct DatabaseInner {
    path: String,
    opts: Option<DatabaseOpts>,
    io: Arc<dyn turso_core::IO>,
    /// The core database, opened lazily by the first connect. Empty for
    /// externally managed databases, which never open the file themselves.
    core: OnceLock<Arc<turso_core::Database>>,
    /// Connection injected by an external owner (the sync engine). Externally
    /// managed databases hand out exactly this one connection: the first
    /// connect returns it, further connects fail.
    external: OnceLock<Arc<turso_core::Connection>>,
    external_taken: std::sync::atomic::AtomicBool,
}

/// A single connection to a database with its own transaction state.
#[napi]
pub struct Connection {
    conn: Option<Arc<turso_core::Connection>>,
    default_safe_integers: Cell<bool>,
    /// Weak refs to every shared statement handle created by this connection.
    /// `close()` upgrades each live handle and sets it to `None`, which
    /// finalizes the statement and releases its `Arc<Connection>`.
    stmts: RefCell<Vec<Weak<RefCell<Option<turso_core::Statement>>>>>,
}

pub(crate) fn is_memory(path: &str) -> bool {
    path == ":memory:"
}

static TRACING_INIT: OnceLock<()> = OnceLock::new();
pub(crate) fn init_tracing(level_filter: &Option<String>) {
    let Some(level_filter) = level_filter else {
        return;
    };
    let level_filter = match level_filter.as_ref() {
        "error" => LevelFilter::ERROR,
        "warn" => LevelFilter::WARN,
        "info" => LevelFilter::INFO,
        "debug" => LevelFilter::DEBUG,
        "trace" => LevelFilter::TRACE,
        _ => return,
    };
    TRACING_INIT.get_or_init(|| {
        tracing_subscriber::fmt()
            .with_ansi(false)
            .with_thread_ids(true)
            .with_span_events(FmtSpan::ACTIVE)
            .with_max_level(level_filter)
            .init();
    });
}

// for now we make DbTask unsound as turso_core::Database and turso_core::Connection are not fully thread-safe
unsafe impl Send for DbTask {}

/// Connection created on the connect worker thread and carried to `resolve`.
pub struct ConnHandle(Arc<turso_core::Connection>);
unsafe impl Send for ConnHandle {}

pub enum DbTask {
    Connect { db: Arc<DatabaseInner> },
}

impl Task for DbTask {
    type Output = ConnHandle;
    type JsValue = Connection;

    fn compute(&mut self) -> Result<Self::Output> {
        match self {
            DbTask::Connect { db } => Ok(ConnHandle(connect(db)?)),
        }
    }

    fn resolve(&mut self, _: Env, output: Self::Output) -> Result<Self::JsValue> {
        Ok(Connection::create(output.0))
    }
}

/// Supported encryption ciphers for local database encryption.
#[napi]
#[derive(Clone, Copy)]
pub enum EncryptionCipher {
    Aes128Gcm,
    Aes256Gcm,
    Aegis256,
    Aegis256x2,
    Aegis128l,
    Aegis128x2,
    Aegis128x4,
}

impl EncryptionCipher {
    fn as_str(&self) -> &'static str {
        match self {
            EncryptionCipher::Aes128Gcm => "aes128gcm",
            EncryptionCipher::Aes256Gcm => "aes256gcm",
            EncryptionCipher::Aegis256 => "aegis256",
            EncryptionCipher::Aegis256x2 => "aegis256x2",
            EncryptionCipher::Aegis128l => "aegis128l",
            EncryptionCipher::Aegis128x2 => "aegis128x2",
            EncryptionCipher::Aegis128x4 => "aegis128x4",
        }
    }
}

/// Encryption configuration for local database encryption.
#[napi(object)]
#[derive(Clone)]
pub struct EncryptionOpts {
    /// The cipher to use for encryption
    pub cipher: EncryptionCipher,
    /// The hex-encoded encryption key
    pub hexkey: String,
}

/// Most of the options are aligned with better-sqlite API
/// (see https://github.com/WiseLibs/better-sqlite3/blob/master/docs/api.md#new-databasepath-options)
#[napi(object)]
#[derive(Clone)]
pub struct DatabaseOpts {
    pub readonly: Option<bool>,
    pub timeout: Option<u32>,
    pub default_query_timeout: Option<u32>,
    pub file_must_exist: Option<bool>,
    pub tracing: Option<String>,
    /// Experimental features to enable
    pub experimental: Option<Vec<String>>,
    /// Optional encryption configuration for local database encryption
    pub encryption: Option<EncryptionOpts>,
}

#[napi(object)]
#[derive(Clone)]
pub struct QueryOptions {
    pub query_timeout: Option<u32>,
}

#[napi(object)]
pub struct TableColumn {
    pub name: String,
    #[napi(ts_type = "string | null")]
    pub r#type: Option<String>,
    pub column: Option<()>,
    pub table: Option<()>,
    pub database: Option<()>,
}

fn step_sync(stmt: &StatementHandle) -> napi::Result<u32> {
    let mut guard = stmt.borrow_mut();
    let core_stmt = guard
        .as_mut()
        .ok_or_else(|| create_generic_error("statement has been finalized"))?;
    match core_stmt.step() {
        Ok(turso_core::StepResult::Row) => Ok(STEP_ROW),
        Ok(turso_core::StepResult::IO | turso_core::StepResult::Yield) => Ok(STEP_IO),
        Ok(turso_core::StepResult::Done) => Ok(STEP_DONE),
        Ok(turso_core::StepResult::Interrupt) => {
            Err(create_generic_error("statement was interrupted"))
        }
        Ok(turso_core::StepResult::Busy) => Err(create_generic_error("database is locked")),
        Err(e) => Err(to_generic_error("step failed", e)),
    }
}

fn to_generic_error<E: std::error::Error>(message: &str, e: E) -> napi::Error {
    Error::new(Status::GenericFailure, format!("{message}: {e}"))
}

fn to_error<E: std::error::Error>(status: napi::Status, message: &str, e: E) -> napi::Error {
    Error::new(status, format!("{message}: {e}"))
}

fn create_generic_error(message: &str) -> napi::Error {
    Error::new(Status::GenericFailure, message)
}

fn create_error(status: napi::Status, message: &str) -> napi::Error {
    Error::new(status, message)
}

fn query_timeout_duration(timeout_ms: u32) -> Option<std::time::Duration> {
    if timeout_ms > 0 {
        Some(std::time::Duration::from_millis(timeout_ms as u64))
    } else {
        None
    }
}

fn query_timeout_override_from_query_options(
    query_options: Option<QueryOptions>,
) -> Option<Option<std::time::Duration>> {
    query_options
        .and_then(|o| o.query_timeout)
        .map(query_timeout_duration)
}

/// Apply the JS-facing experimental feature list (e.g. `["views", "vacuum"]`)
/// to [`turso_core::DatabaseOpts`]. The feature-name tokens match the SDK/CLI
/// names; unknown names are ignored and `"strict"` is a no-op (strict tables
/// are always enabled). This lives in the binding layer (and is shared with the
/// sync binding) rather than in `turso_core`, because the string feature-name
/// representation is a binding concern, not an engine one.
pub fn apply_experimental_features(
    mut opts: turso_core::DatabaseOpts,
    experimental: &[String],
) -> turso_core::DatabaseOpts {
    for feature in experimental {
        opts = match feature.as_str() {
            "views" => opts.with_views(true),
            "strict" => opts, // strict is always enabled, kept for backwards compatibility
            "custom_types" => opts.with_custom_types(true),
            "encryption" => opts.with_encryption(true),
            "index_method" => opts.with_index_method(true),
            "autovacuum" => opts.with_autovacuum(true),
            "vacuum" => opts.with_vacuum(true),
            "attach" => opts.with_attach(true),
            "generated_columns" => opts.with_generated_columns(true),
            "multiprocess_wal" => opts.with_multiprocess_wal(true),
            "without_rowid" => opts.with_without_rowid(true),
            _ => opts,
        };
    }
    opts
}

/// Creates a new connection to the database, opening the core database on the
/// first call. This is the single connect path: the main connection and every
/// pooled connection are created here. Externally managed databases (sync
/// engine) return their injected connection exactly once.
fn connect(db: &DatabaseInner) -> napi::Result<Arc<turso_core::Connection>> {
    if let Some(conn) = db.external.get() {
        if db
            .external_taken
            .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(create_generic_error(
                "externally managed database supports a single connection",
            ));
        }
        return Ok(conn.clone());
    }

    if db.core.get().is_none() {
        let mut flags = turso_core::OpenFlags::Create;
        let mut core_opts = turso_core::DatabaseOpts::new();
        let mut encryption_opts = None;
        if let Some(opts) = &db.opts {
            if opts.readonly == Some(true) {
                flags.set(turso_core::OpenFlags::ReadOnly, true);
                flags.set(turso_core::OpenFlags::Create, false);
            }
            if opts.file_must_exist == Some(true) {
                flags.set(turso_core::OpenFlags::Create, false);
            }
            if let Some(experimental) = &opts.experimental {
                core_opts = apply_experimental_features(core_opts, experimental);
            }
            if let Some(encryption) = &opts.encryption {
                encryption_opts = Some(turso_core::EncryptionOpts {
                    cipher: encryption.cipher.as_str().to_string(),
                    hexkey: encryption.hexkey.clone(),
                });
                // Ensure encryption is enabled if encryption opts are provided
                core_opts = core_opts.with_encryption(true);
            }
        }

        let db_core = turso_core::Database::open_file_with_flags(
            db.io.clone(),
            &db.path,
            flags,
            core_opts,
            encryption_opts,
            Arc::new(SqliteDialect),
        )
        .map_err(|e| to_generic_error(&format!("failed to open database {}", db.path), e))?;

        // there can be races between concurrent connects - the loser drops its
        // database and connects to the winner's (DATABASE_MANAGER dedupes
        // file-backed databases anyway)
        let _ = db.core.set(db_core);
    }
    connect_to_core(db.core.get().unwrap(), &db.opts)
}

/// Creates a connection to an already opened core database, applying the
/// per-connection settings (encryption key, busy/query timeouts) from opts.
fn connect_to_core(
    db_core: &Arc<turso_core::Database>,
    opts: &Option<DatabaseOpts>,
) -> napi::Result<Arc<turso_core::Connection>> {
    let mut busy_timeout = None;
    let mut query_timeout = None;
    let mut encryption_key = None;
    if let Some(opts) = opts {
        if let Some(timeout) = opts.timeout {
            busy_timeout = Some(std::time::Duration::from_millis(timeout as u64));
        }
        if let Some(timeout) = opts.default_query_timeout {
            query_timeout = query_timeout_duration(timeout);
        }
        if let Some(encryption) = &opts.encryption {
            encryption_key = Some(
                turso_core::EncryptionKey::from_hex_string(&encryption.hexkey)
                    .map_err(|e| to_generic_error("invalid encryption key", e))?,
            );
        }
    }

    // Use connect_with_encryption to properly set up encryption context
    // before the pager reads page 1. This is required for encrypted databases.
    let conn = db_core
        .connect_with_encryption(encryption_key)
        .map_err(|e| to_generic_error("failed to connect", e))?;

    if let Some(busy_timeout) = busy_timeout {
        conn.set_busy_timeout(busy_timeout);
    }
    if let Some(query_timeout) = query_timeout {
        conn.set_query_timeout(query_timeout);
    }
    Ok(conn)
}

#[napi]
impl Database {
    /// Creates a new database instance.
    ///
    /// # Arguments
    /// * `path` - The path to the database file.
    #[napi(constructor)]
    pub fn new(path: String, opts: Option<DatabaseOpts>) -> napi::Result<Self> {
        let io: Arc<dyn turso_core::IO> = if is_memory(&path) {
            Arc::new(turso_core::MemoryIO::new())
        } else {
            #[cfg(not(feature = "browser"))]
            {
                Arc::new(
                    turso_core::PlatformIO::new()
                        .map_err(|e| to_generic_error("failed to create IO", e))?,
                )
            }
            #[cfg(feature = "browser")]
            {
                browser::opfs()
            }
        };
        Self::new_with_io(path, io, opts)
    }

    pub fn new_with_io(
        path: String,
        io: Arc<dyn turso_core::IO>,
        opts: Option<DatabaseOpts>,
    ) -> napi::Result<Self> {
        if let Some(opts) = &opts {
            init_tracing(&opts.tracing);
        }
        Ok(Self {
            #[allow(clippy::arc_with_non_send_sync)]
            inner: Some(Arc::new(DatabaseInner {
                path,
                opts,
                io,
                core: OnceLock::new(),
                external: OnceLock::new(),
                external_taken: std::sync::atomic::AtomicBool::new(false),
            })),
        })
    }

    /// Marks the database as externally managed and injects its single
    /// connection (used by the sync engine). The first connect returns this
    /// connection, further connects fail.
    pub fn set_connected(&self, conn: Arc<turso_core::Connection>) -> napi::Result<()> {
        let inner = self.inner()?;
        inner
            .external
            .set(conn)
            .map_err(|_| create_generic_error("database was already connected"))?;

        Ok(())
    }

    fn inner(&self) -> napi::Result<&Arc<DatabaseInner>> {
        let Some(inner) = &self.inner else {
            return Err(create_generic_error("database must be connected"));
        };
        Ok(inner)
    }

    /// Creates a new connection synchronously, opening the database on the
    /// first call.
    #[napi]
    pub fn connect_sync(&self) -> napi::Result<Connection> {
        Ok(Connection::create(connect(self.inner()?)?))
    }

    /// Creates a new connection asynchronously, opening the database on the
    /// first call.
    #[napi(ts_return_type = "Promise<Connection>")]
    pub fn connect_async(&self) -> napi::Result<AsyncTask<DbTask>> {
        Ok(AsyncTask::new(DbTask::Connect {
            db: self.inner()?.clone(),
        }))
    }

    /// Returns whether the database is in memory-only mode.
    #[napi(getter)]
    pub fn memory(&self) -> napi::Result<bool> {
        Ok(is_memory(&self.inner()?.path))
    }

    /// Returns whether the database is in memory-only mode.
    #[napi(getter)]
    pub fn path(&self) -> napi::Result<String> {
        Ok(self.inner()?.path.clone())
    }

    /// Returns whether the database has been opened.
    #[napi(getter)]
    pub fn open(&self) -> napi::Result<bool> {
        let Some(inner) = &self.inner else {
            return Ok(false);
        };
        Ok(inner.core.get().is_some() || inner.external.get().is_some())
    }

    /// Closes the database handle. Connections are closed individually via
    /// `Connection::close()`.
    #[napi]
    pub fn close(&mut self) -> napi::Result<()> {
        self.inner.take();
        Ok(())
    }

    /// Runs the I/O loop synchronously.
    #[napi]
    pub fn io_loop_sync(&self) -> napi::Result<()> {
        let io = &self.inner()?.io;
        io.step().map_err(|e| to_generic_error("IO error", e))?;
        Ok(())
    }

    /// Runs the I/O loop asynchronously, returning a Promise.
    #[napi(ts_return_type = "Promise<void>")]
    pub fn io_loop_async(&self) -> napi::Result<AsyncTask<IoLoopTask>> {
        let io = self.inner()?.io.clone();
        Ok(AsyncTask::new(IoLoopTask { io }))
    }

    /// Classify SQL statement. Returns "read", "write", "begin", "commit", or "rollback".
    #[napi(js_name = "classifySql")]
    pub fn classify_sql(&self, sql: String) -> napi::Result<String> {
        use turso_parser::{ast::Stmt, parser::Parser};
        let mut parser = Parser::new(sql.as_bytes());
        match parser.next_cmd() {
            Ok(Some(cmd)) => {
                if cmd.is_explain() {
                    return Ok("read".to_string());
                }
                let category = match cmd.stmt() {
                    Stmt::Select(..)
                    | Stmt::Pragma { .. }
                    | Stmt::Attach { .. }
                    | Stmt::Detach { .. } => "read",
                    Stmt::Begin { .. } | Stmt::Savepoint { .. } => "begin",
                    Stmt::Commit { .. } | Stmt::Release { .. } => "commit",
                    Stmt::Rollback { .. } => "rollback",
                    _ => "write",
                };
                Ok(category.to_string())
            }
            Ok(None) => Ok("read".to_string()),
            Err(e) => Err(napi::Error::from_reason(format!("classify failed: {e}"))),
        }
    }
}

#[napi]
impl Connection {
    fn create(conn: Arc<turso_core::Connection>) -> Self {
        Self {
            conn: Some(conn),
            default_safe_integers: Cell::new(false),
            stmts: RefCell::new(Vec::new()),
        }
    }

    fn conn(&self) -> Result<&Arc<turso_core::Connection>> {
        let Some(conn) = &self.conn else {
            return Err(create_generic_error("connection is closed"));
        };
        Ok(conn)
    }

    /// Returns whether the connection is open.
    #[napi(getter)]
    pub fn open(&self) -> bool {
        self.conn.is_some()
    }

    /// Returns whether the connection is in readonly-only mode.
    #[napi(getter)]
    pub fn readonly(&self) -> napi::Result<bool> {
        Ok(self.conn()?.is_readonly(0))
    }

    /// Prepares a statement for execution.
    ///
    /// # Arguments
    ///
    /// * `sql` - The SQL statement to prepare.
    ///
    /// # Returns
    ///
    /// A promise resolving to a `Statement` instance.
    #[napi(ts_return_type = "Promise<Statement>")]
    pub fn prepare(&self, sql: String) -> napi::Result<Statement> {
        let stmt = self
            .conn()?
            .prepare(&sql)
            .map_err(|e| to_generic_error("prepare failed", e))?;
        let column_names: Vec<std::ffi::CString> = (0..stmt.num_columns())
            .map(|i| std::ffi::CString::new(stmt.get_column_name(i).to_string()).unwrap())
            .collect();
        #[allow(clippy::arc_with_non_send_sync)]
        let stmt: StatementHandle = Arc::new(RefCell::new(Some(stmt)));
        self.stmts.borrow_mut().push(Arc::downgrade(&stmt));
        Ok(Statement {
            stmt,
            column_names,
            mode: RefCell::new(PresentationMode::Expanded),
            safe_integers: Cell::new(self.default_safe_integers.get()),
        })
    }

    #[napi]
    pub fn executor(
        &self,
        sql: String,
        query_options: Option<QueryOptions>,
    ) -> napi::Result<BatchExecutor> {
        Ok(BatchExecutor {
            conn: Some(self.conn()?.clone()),
            sql,
            position: 0,
            stmt: None,
            query_timeout_override: query_timeout_override_from_query_options(query_options),
        })
    }

    /// Returns the rowid of the last row inserted.
    ///
    /// # Returns
    ///
    /// The rowid of the last row inserted.
    #[napi]
    pub fn last_insert_rowid(&self) -> napi::Result<i64> {
        Ok(self.conn()?.last_insert_rowid())
    }

    /// Returns the number of changes made by the last statement.
    ///
    /// # Returns
    ///
    /// The number of changes made by the last statement.
    #[napi]
    pub fn changes(&self) -> napi::Result<i64> {
        Ok(self.conn()?.changes())
    }

    /// Returns the total number of changes made by all statements.
    ///
    /// # Returns
    ///
    /// The total number of changes made by all statements.
    #[napi]
    pub fn total_changes(&self) -> napi::Result<i64> {
        Ok(self.conn()?.total_changes())
    }

    /// Returns whether the connection is currently inside a transaction.
    ///
    /// This is the inverse of `sqlite3_get_autocommit()`: a connection in
    /// autocommit mode is not in a transaction. It reflects the connection's
    /// real state, including transactions opened with a raw `BEGIN`.
    ///
    /// # Returns
    ///
    /// `true` if a transaction is open, `false` if in autocommit mode.
    #[napi]
    pub fn in_transaction(&self) -> napi::Result<bool> {
        Ok(!self.conn()?.get_auto_commit())
    }

    /// Sets the default safe integers mode for all statements from this connection.
    ///
    /// # Arguments
    ///
    /// * `toggle` - Whether to use safe integers by default.
    #[napi(js_name = "defaultSafeIntegers")]
    pub fn default_safe_integers(&self, toggle: Option<bool>) -> napi::Result<()> {
        self.default_safe_integers.set(toggle.unwrap_or(true));
        Ok(())
    }

    /// Closes the connection.
    #[napi]
    pub fn close(&mut self) -> napi::Result<()> {
        if self.conn.take().is_some() {
            // Finalize all outstanding statements.  Each turso_core::Statement
            // holds Program { connection: Arc<Connection> } which in turn holds
            // Arc<Database>.  If we don't clear them, the Weak in
            // DATABASE_MANAGER can still be upgraded after the file is renamed,
            // causing a stale Database to be returned on the next open().
            for weak in self.stmts.borrow_mut().drain(..) {
                if let Some(stmt) = weak.upgrade() {
                    *stmt.borrow_mut() = None;
                }
            }
        }
        Ok(())
    }
}

#[napi]
pub struct BatchExecutor {
    conn: Option<Arc<turso_core::Connection>>,
    sql: String,
    position: usize,
    stmt: Option<StatementHandle>,
    query_timeout_override: Option<Option<std::time::Duration>>,
}

#[napi]
impl BatchExecutor {
    #[napi]
    pub fn step_sync(&mut self) -> Result<u32> {
        loop {
            if self.stmt.is_none() && self.position >= self.sql.len() {
                return Ok(STEP_DONE);
            }
            if self.stmt.is_none() {
                let conn = self.conn.as_ref().unwrap();
                match conn.consume_stmt(&self.sql[self.position..]) {
                    #[allow(clippy::arc_with_non_send_sync)]
                    Ok(Some((stmt, offset))) => {
                        self.position += offset;
                        let stmt: StatementHandle = Arc::new(RefCell::new(Some(stmt)));
                        stmt.borrow_mut()
                            .as_mut()
                            .unwrap()
                            .set_query_timeout_override(self.query_timeout_override);
                        self.stmt = Some(stmt);
                    }
                    Ok(None) => return Ok(STEP_DONE),
                    Err(err) => return Err(to_generic_error("failed to consume stmt", err)),
                }
            }
            let stmt = self.stmt.as_ref().unwrap();
            match step_sync(stmt) {
                Ok(STEP_DONE) => {
                    let _ = self.stmt.take();
                    continue;
                }
                result => return result,
            }
        }
    }

    #[napi]
    pub fn reset(&mut self) {
        let _ = self.conn.take();
        let _ = self.stmt.take();
    }
}

/// A prepared statement.
#[napi]
pub struct Statement {
    stmt: StatementHandle,
    column_names: Vec<std::ffi::CString>,
    mode: RefCell<PresentationMode>,
    safe_integers: Cell<bool>,
}

#[napi]
impl Statement {
    fn statement_handle(&self) -> napi::Result<&StatementHandle> {
        if self.stmt.borrow().is_none() {
            return Err(create_generic_error("statement has been finalized"));
        }
        Ok(&self.stmt)
    }
    #[napi]
    pub fn reset(&self) -> Result<()> {
        let mut guard = self.statement_handle()?.borrow_mut();
        guard
            .as_mut()
            .ok_or_else(|| create_generic_error("statement has been finalized"))?
            .reset()
            .map_err(|e| to_generic_error("reset failed", e))?;
        Ok(())
    }

    #[napi(js_name = "setQueryTimeout")]
    pub fn set_query_timeout(&self, query_options: Option<QueryOptions>) -> Result<()> {
        let timeout_override = query_timeout_override_from_query_options(query_options);
        let mut guard = self.statement_handle()?.borrow_mut();
        guard
            .as_mut()
            .ok_or_else(|| create_generic_error("statement has been finalized"))?
            .set_query_timeout_override(timeout_override);
        Ok(())
    }

    /// Returns the number of parameters in the statement.
    #[napi]
    pub fn parameter_count(&self) -> Result<u32> {
        let guard = self.statement_handle()?.borrow();
        let stmt = guard
            .as_ref()
            .ok_or_else(|| create_generic_error("statement has been finalized"))?;
        Ok(stmt.parameters_count() as u32)
    }

    /// Returns the name of a parameter at a specific 1-based index.
    ///
    /// # Arguments
    ///
    /// * `index` - The 1-based parameter index.
    #[napi]
    pub fn parameter_name(&self, index: u32) -> Result<Option<String>> {
        let non_zero_idx = NonZeroUsize::new(index as usize).ok_or_else(|| {
            create_error(Status::InvalidArg, "parameter index must be greater than 0")
        })?;

        let guard = self.statement_handle()?.borrow();
        let stmt = guard
            .as_ref()
            .ok_or_else(|| create_generic_error("statement has been finalized"))?;
        Ok(stmt.parameters().name(non_zero_idx))
    }

    /// Binds a parameter at a specific 1-based index with explicit type.
    ///
    /// # Arguments
    ///
    /// * `index` - The 1-based parameter index.
    /// * `value_type` - The type constant (0=null, 1=int, 2=float, 3=text, 4=blob).
    /// * `value` - The value to bind.
    #[napi]
    pub fn bind_at(&self, index: u32, value: Unknown) -> Result<()> {
        let non_zero_idx = NonZeroUsize::new(index as usize).ok_or_else(|| {
            create_error(Status::InvalidArg, "parameter index must be greater than 0")
        })?;
        let value_type = value.get_type()?;
        let turso_value = match value_type {
            ValueType::Null => turso_core::Value::Null,
            ValueType::Number => {
                let n: f64 = unsafe { value.cast()? };
                if n.fract() == 0.0 && n >= i64::MIN as f64 && n <= i64::MAX as f64 {
                    turso_core::Value::from_i64(n as i64)
                } else {
                    turso_core::Value::from_f64(n)
                }
            }
            ValueType::BigInt => {
                let bigint_str = value.coerce_to_string()?.into_utf8()?.as_str()?.to_owned();
                let bigint_value = bigint_str
                    .parse::<i64>()
                    .map_err(|e| to_error(Status::NumberExpected, "failed to parse BigInt", e))?;
                turso_core::Value::from_i64(bigint_value)
            }
            ValueType::String => {
                let s = value.coerce_to_string()?.into_utf8()?;
                turso_core::Value::Text(s.as_str()?.to_owned().into())
            }
            ValueType::Boolean => {
                let b: bool = unsafe { value.cast()? };
                turso_core::Value::from_i64(if b { 1 } else { 0 })
            }
            ValueType::Object => {
                let obj = value.coerce_to_object()?;

                if obj.is_buffer()? || obj.is_typedarray()? {
                    let length = obj.get_named_property::<u32>("length")?;
                    let mut bytes = Vec::with_capacity(length as usize);
                    for i in 0..length {
                        let byte = obj.get_element::<u32>(i)?;
                        bytes.push(byte as u8);
                    }
                    turso_core::Value::Blob(bytes)
                } else {
                    let s = value.coerce_to_string()?.into_utf8()?;
                    turso_core::Value::Text(s.as_str()?.to_owned().into())
                }
            }
            _ => {
                let s = value.coerce_to_string()?.into_utf8()?;
                turso_core::Value::Text(s.as_str()?.to_owned().into())
            }
        };

        self.statement_handle()?
            .borrow_mut()
            .as_mut()
            .ok_or_else(|| create_generic_error("statement has been finalized"))?
            .bind_at(non_zero_idx, turso_value)
            .map_err(|err| create_generic_error(&err.to_string()))?;
        Ok(())
    }

    /// Step the statement and return result code (executed on the main thread):
    /// 1 = Row available, 2 = Done, 3 = I/O needed
    #[napi]
    pub fn step_sync(&self) -> Result<u32> {
        step_sync(self.statement_handle()?)
    }

    /// Get the current row data according to the presentation mode
    #[napi]
    pub fn row<'env>(&self, env: &'env Env) -> Result<Unknown<'env>> {
        let guard = self.statement_handle()?.borrow();
        let stmt = guard
            .as_ref()
            .ok_or_else(|| create_generic_error("statement has been finalized"))?;
        let row_data = stmt
            .row()
            .ok_or_else(|| create_generic_error("no row data available"))?;

        let mode = self.mode.borrow();
        let safe_integers = self.safe_integers.get();
        let row_value = match *mode {
            PresentationMode::Raw => {
                let mut raw_array = env.create_array(row_data.len() as u32)?;
                for (idx, value) in row_data.get_values().enumerate() {
                    let js_value = to_js_value(env, value, safe_integers)?;
                    raw_array.set(idx as u32, js_value)?;
                }
                raw_array.coerce_to_object()?.to_unknown()
            }
            PresentationMode::Pluck => {
                let (_, value) = row_data.get_values().enumerate().next().ok_or_else(|| {
                    create_generic_error("pluck mode requires at least one column in the result")
                })?;
                to_js_value(env, value, safe_integers)?
            }
            PresentationMode::Expanded => {
                let mut row = Object::new(env)?;
                let raw_row = row.raw();
                let raw_env = env.raw();
                let mut positional_properties = Vec::with_capacity(row_data.len());
                for idx in 0..row_data.len() {
                    let value = row_data.get_value(idx);
                    let column_name = &self.column_names[idx];
                    let js_value = to_js_value(env, value, safe_integers)?;
                    check_status!(unsafe {
                        napi::sys::napi_set_named_property(
                            raw_env,
                            raw_row,
                            column_name.as_ptr(),
                            js_value.raw(),
                        )
                    })?;
                    positional_properties.push(
                        Property::new()
                            .with_utf8_name(&idx.to_string())?
                            .with_value(&js_value)
                            .with_property_attributes(
                                PropertyAttributes::Writable | PropertyAttributes::Configurable,
                            ),
                    );
                }
                row.define_properties(&positional_properties)?;
                row.to_unknown()
            }
        };

        Ok(row_value)
    }

    /// Sets the presentation mode to raw.
    #[napi]
    pub fn raw(&mut self, raw: Option<bool>) {
        self.mode = RefCell::new(match raw {
            Some(false) => PresentationMode::Expanded,
            _ => PresentationMode::Raw,
        });
    }

    /// Sets the presentation mode to pluck.
    #[napi]
    pub fn pluck(&mut self, pluck: Option<bool>) {
        self.mode = RefCell::new(match pluck {
            Some(false) => PresentationMode::Expanded,
            _ => PresentationMode::Pluck,
        });
    }

    /// Sets safe integers mode for this statement.
    ///
    /// # Arguments
    ///
    /// * `toggle` - Whether to use safe integers.
    #[napi(js_name = "safeIntegers")]
    pub fn safe_integers(&self, toggle: Option<bool>) {
        self.safe_integers.set(toggle.unwrap_or(true));
    }

    /// Get column information for the statement
    #[napi(ts_return_type = "Promise<TableColumn[]>")]
    pub fn columns<'env>(&self, env: &'env Env) -> Result<Array<'env>> {
        let guard = self.statement_handle()?.borrow();
        let stmt = guard
            .as_ref()
            .ok_or_else(|| create_generic_error("statement has been finalized"))?;
        let column_count = stmt.num_columns();
        let mut js_array = env.create_array(column_count as u32)?;

        for i in 0..column_count {
            let mut js_obj = Object::new(env)?;
            let column_name = stmt.get_column_name(i);
            let column_type = stmt.get_column_type_name(i);

            // Set the name property
            js_obj.set("name", column_name.as_ref())?;

            // Set type property if available
            match column_type {
                Some(type_str) => js_obj.set("type", type_str.as_str())?,
                None => js_obj.set("type", ToNapiValue::into_unknown(Null, env)?)?,
            }

            // For now, set other properties to null since turso_core doesn't provide this metadata
            js_obj.set("column", ToNapiValue::into_unknown(Null, env)?)?;
            js_obj.set("table", ToNapiValue::into_unknown(Null, env)?)?;
            js_obj.set("database", ToNapiValue::into_unknown(Null, env)?)?;

            js_array.set(i as u32, js_obj)?;
        }

        Ok(js_array)
    }

    /// Finalizes the statement.
    #[napi]
    pub fn finalize(&mut self) -> Result<()> {
        *self.stmt.borrow_mut() = None;
        Ok(())
    }
}

/// Async task for running the I/O loop.
pub struct IoLoopTask {
    // this field is public because it is also set in the sync package
    pub io: Arc<dyn turso_core::IO>,
}

impl Task for IoLoopTask {
    type Output = ();
    type JsValue = ();

    fn compute(&mut self) -> napi::Result<Self::Output> {
        self.io
            .step()
            .map_err(|e| to_generic_error("IO error", e))?;
        Ok(())
    }

    fn resolve(&mut self, _env: Env, _output: Self::Output) -> napi::Result<Self::JsValue> {
        Ok(())
    }
}

/// Convert a Turso value to a JavaScript value.
fn to_js_value<'a>(
    env: &'a napi::Env,
    value: &turso_core::Value,
    safe_integers: bool,
) -> napi::Result<Unknown<'a>> {
    match value {
        turso_core::Value::Null => ToNapiValue::into_unknown(Null, env),
        turso_core::Value::Numeric(turso_core::Numeric::Integer(i)) => {
            if safe_integers {
                let bigint = BigInt::from(*i);
                ToNapiValue::into_unknown(bigint, env)
            } else {
                ToNapiValue::into_unknown(*i as f64, env)
            }
        }
        turso_core::Value::Numeric(turso_core::Numeric::Float(f)) => {
            ToNapiValue::into_unknown(f64::from(*f), env)
        }
        turso_core::Value::Text(s) => ToNapiValue::into_unknown(s.as_str(), env),
        turso_core::Value::Blob(b) => {
            #[cfg(not(feature = "browser"))]
            {
                let buffer = Buffer::from(b.as_slice());
                ToNapiValue::into_unknown(buffer, env)
            }
            // emnapi do not support Buffer
            #[cfg(feature = "browser")]
            {
                let buffer = Uint8Array::from(b.as_slice());
                ToNapiValue::into_unknown(buffer, env)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::apply_experimental_features;

    #[test]
    fn apply_experimental_features_maps_feature_list() {
        // No features -> defaults.
        assert_eq!(
            apply_experimental_features(turso_core::DatabaseOpts::new(), &[]),
            turso_core::DatabaseOpts::new()
        );

        let features: Vec<String> = [
            "views",
            "index_method",
            "custom_types",
            "autovacuum",
            "vacuum",
            "encryption",
            "attach",
            "generated_columns",
            "multiprocess_wal",
            "without_rowid",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        let opts = apply_experimental_features(turso_core::DatabaseOpts::new(), &features);
        assert!(opts.enable_views);
        assert!(opts.enable_index_method);
        assert!(opts.enable_custom_types);
        assert!(opts.enable_autovacuum);
        assert!(opts.enable_vacuum);
        assert!(opts.enable_encryption);
        assert!(opts.enable_attach);
        assert!(opts.enable_generated_columns);
        assert!(opts.enable_multiprocess_wal);
        assert!(opts.enable_without_rowid);

        // `strict` and unknown names are no-ops.
        assert_eq!(
            apply_experimental_features(
                turso_core::DatabaseOpts::new(),
                &["strict".to_string(), "unknown".to_string()]
            ),
            turso_core::DatabaseOpts::new()
        );
    }
}
