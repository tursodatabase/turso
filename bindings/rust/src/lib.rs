//! # Turso bindings for Rust
//!
//! Turso is an in-process SQL database engine, compatible with SQLite.
//!
//! ## Getting Started
//!
//! To get started, you first need to create a [`Database`] object and then open a [`Connection`] to it, which you use to query:
//!
//! ```rust,no_run
//! # async fn run() {
//! use turso::Builder;
//!
//! let db = Builder::new_local(":memory:").build().await.unwrap();
//! let conn = db.connect().unwrap();
//! conn.execute("CREATE TABLE IF NOT EXISTS users (email TEXT)", ()).await.unwrap();
//! conn.execute("INSERT INTO users (email) VALUES ('alice@example.org')", ()).await.unwrap();
//! # }
//! ```
//!
//! You can also prepare statements with the [`Connection`] object and then execute the [`Statement`] objects:
//!
//! ```rust,no_run
//! # async fn run() {
//! # use turso::Builder;
//! # let db = Builder::new_local(":memory:").build().await.unwrap();
//! # let conn = db.connect().unwrap();
//! let mut stmt = conn.prepare("SELECT * FROM users WHERE email = ?1").await.unwrap();
//! let mut rows = stmt.query(["foo@example.com"]).await.unwrap();
//! let row = rows.next().await.unwrap().unwrap();
//! let value = row.get_value(0).unwrap();
//! println!("Row: {:?}", value);
//! # }
//! ```

#[cfg(all(feature = "mimalloc", not(target_family = "wasm"), not(miri)))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

pub mod connection;
pub mod params;
mod rows;
pub mod transaction;
pub mod value;

#[cfg(feature = "sync")]
pub mod sync;

pub use connection::Connection;
use turso_sdk_kit::rsapi::TursoError;
pub use value::Value;

pub use params::params_from_iter;
pub use params::IntoParams;

use std::fmt::Debug;
use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use std::task::Poll;

// Re-exports rows
pub use crate::rows::{Row, Rows};

/// Assert that a type implements both Send and Sync at compile time.
/// Usage: assert_send_sync!(MyType);
/// Usage: assert_send_sync!(Type1, Type2, Type3);
macro_rules! assert_send_sync {
    ($($t:ty),+ $(,)?) => {
        #[cfg(test)]
        $(const _: () = {
            const fn _assert_send<T: ?Sized + Send>() {}
            const fn _assert_sync<T: ?Sized + Sync>() {}
            _assert_send::<$t>();
            _assert_sync::<$t>();
        };)+
    };
}

pub(crate) use assert_send_sync;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("SQL conversion failure: `{0}`")]
    ToSqlConversionFailure(BoxError),
    #[error("Query returned no rows")]
    QueryReturnedNoRows,
    #[error("Conversion failure: `{0}`")]
    ConversionFailure(String),
    #[error("{0}")]
    Busy(String),
    #[error("{0}")]
    BusySnapshot(String),
    #[error("{0}")]
    Interrupt(String),
    #[error("{0}")]
    Error(String),
    #[error("{0}")]
    Misuse(String),
    #[error("{0}")]
    Constraint(String),
    #[error("{0}")]
    Readonly(String),
    #[error("{0}")]
    DatabaseFull(String),
    #[error("{0}")]
    NotAdb(String),
    #[error("{0}")]
    Corrupt(String),
    #[error("I/O error ({1}): {0}")]
    IoError(std::io::ErrorKind, &'static str),
}

impl From<turso_sdk_kit::rsapi::TursoError> for Error {
    fn from(value: turso_sdk_kit::rsapi::TursoError) -> Self {
        match value {
            turso_sdk_kit::rsapi::TursoError::Busy(err) => Error::Busy(err),
            turso_sdk_kit::rsapi::TursoError::BusySnapshot(err) => Error::BusySnapshot(err),
            turso_sdk_kit::rsapi::TursoError::Interrupt(err) => Error::Interrupt(err),
            turso_sdk_kit::rsapi::TursoError::Error(err) => Error::Error(err),
            turso_sdk_kit::rsapi::TursoError::Misuse(err) => Error::Misuse(err),
            turso_sdk_kit::rsapi::TursoError::Constraint(err) => Error::Constraint(err),
            turso_sdk_kit::rsapi::TursoError::Readonly(err) => Error::Readonly(err),
            turso_sdk_kit::rsapi::TursoError::DatabaseFull(err) => Error::DatabaseFull(err),
            turso_sdk_kit::rsapi::TursoError::NotAdb(err) => Error::NotAdb(err),
            turso_sdk_kit::rsapi::TursoError::Corrupt(err) => Error::Corrupt(err),
            turso_sdk_kit::rsapi::TursoError::IoError(kind, op) => Error::IoError(kind, op),
        }
    }
}

pub(crate) type BoxError = Box<dyn std::error::Error + Send + Sync>;

pub type Result<T> = std::result::Result<T, Error>;
pub type EncryptionOpts = turso_sdk_kit::rsapi::EncryptionOpts;

const DEFAULT_MAX_IDLE_CONNECTIONS: usize = 16;

/// A builder for `Database`.
pub struct Builder {
    path: String,
    enable_encryption: bool,
    enable_attach: bool,
    enable_custom_types: bool,
    enable_index_method: bool,
    enable_materialized_views: bool,
    max_idle_connections: usize,
    vfs: Option<String>,
    encryption_opts: Option<turso_sdk_kit::rsapi::EncryptionOpts>,
}

impl Builder {
    /// Create a new local database.
    pub fn new_local(path: &str) -> Self {
        Self {
            path: path.to_string(),
            enable_encryption: false,
            enable_attach: false,
            enable_custom_types: false,
            enable_index_method: false,
            enable_materialized_views: false,
            max_idle_connections: DEFAULT_MAX_IDLE_CONNECTIONS,
            vfs: None,
            encryption_opts: None,
        }
    }

    pub fn experimental_encryption(mut self, encryption_enabled: bool) -> Self {
        self.enable_encryption = encryption_enabled;
        self
    }

    pub fn with_encryption(mut self, opts: turso_sdk_kit::rsapi::EncryptionOpts) -> Self {
        self.encryption_opts = Some(opts);
        self
    }

    /// Kept for backwards compatibility. Triggers are now always enabled.
    pub fn experimental_triggers(self, _triggers_enabled: bool) -> Self {
        self
    }

    pub fn experimental_attach(mut self, attach_enabled: bool) -> Self {
        self.enable_attach = attach_enabled;
        self
    }

    /// Kept for backwards compatibility. Strict tables are now always enabled.
    pub fn experimental_strict(self, _strict_enabled: bool) -> Self {
        self
    }

    pub fn experimental_custom_types(mut self, custom_types_enabled: bool) -> Self {
        self.enable_custom_types = custom_types_enabled;
        self
    }

    pub fn experimental_index_method(mut self, index_method_enabled: bool) -> Self {
        self.enable_index_method = index_method_enabled;
        self
    }

    pub fn experimental_materialized_views(mut self, enabled: bool) -> Self {
        self.enable_materialized_views = enabled;
        self
    }

    pub fn with_io(mut self, vfs: String) -> Self {
        self.vfs = Some(vfs);
        self
    }

    /// Maximum number of idle connections kept by `Database::connect_pooled`.
    pub fn max_idle_connections(mut self, max_idle_connections: usize) -> Self {
        self.max_idle_connections = max_idle_connections;
        self
    }

    fn build_features_string(&self) -> Option<String> {
        let mut features = Vec::new();
        if self.enable_encryption {
            features.push("encryption");
        }
        if self.enable_attach {
            features.push("attach");
        }
        if self.enable_custom_types {
            features.push("custom_types");
        }
        if self.enable_index_method {
            features.push("index_method");
        }
        if self.enable_materialized_views {
            features.push("views");
        }
        if features.is_empty() {
            return None;
        }
        Some(features.join(","))
    }

    /// Build the database.
    #[allow(unused_variables, clippy::arc_with_non_send_sync)]
    pub async fn build(self) -> Result<Database> {
        let features = self.build_features_string();
        let db =
            turso_sdk_kit::rsapi::TursoDatabase::new(turso_sdk_kit::rsapi::TursoDatabaseConfig {
                path: self.path,
                experimental_features: features,
                async_io: true,
                encryption: self.encryption_opts,
                vfs: self.vfs,
                io: None,
                db_file: None,
            });
        while let Some(io_c) = db.open()?.io() {
            // At this point IO must already be created
            let io = db
                .io()
                .expect("IO must have been set on the first call to db open");
            io_c.wait_async(io.as_ref())
                .await
                .map_err(TursoError::from)?;
        }
        Ok(Database {
            inner: db,
            pool: Arc::new(ConnectionPool::new(self.max_idle_connections)),
        })
    }
}

/// A database.
///
/// The `Database` object points to a database and allows you to connect to it
#[derive(Clone)]
pub struct Database {
    inner: Arc<turso_sdk_kit::rsapi::TursoDatabase>,
    pool: Arc<ConnectionPool>,
}

impl Debug for Database {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Database").finish()
    }
}

impl Database {
    /// Connect to the database.
    pub fn connect(&self) -> Result<Connection> {
        let conn = self.inner.connect()?;
        Ok(Connection::create(conn, None))
    }

    /// Connect to the database using the built-in connection pool.
    pub fn connect_pooled(&self) -> Result<PooledConnection> {
        let conn = match self.pool.acquire() {
            Some(conn) => conn,
            None => self.connect()?,
        };

        Ok(PooledConnection {
            conn: Some(conn),
            pool: Arc::downgrade(&self.pool),
        })
    }
}

#[derive(Debug)]
struct ConnectionPool {
    max_idle_connections: usize,
    idle: Mutex<Vec<Connection>>,
}

impl ConnectionPool {
    fn new(max_idle_connections: usize) -> Self {
        Self {
            max_idle_connections,
            idle: Mutex::new(Vec::new()),
        }
    }

    fn acquire(&self) -> Option<Connection> {
        self.idle.lock().unwrap().pop()
    }

    fn release(&self, mut conn: Connection) {
        if self.max_idle_connections == 0 || !conn.can_recycle_into_pool() {
            return;
        }

        conn.reset_for_reuse();

        let mut idle = self.idle.lock().unwrap();
        if idle.len() < self.max_idle_connections {
            idle.push(conn);
        }
    }
}

pub struct PooledConnection {
    conn: Option<Connection>,
    pool: Weak<ConnectionPool>,
}

impl PooledConnection {
    pub fn into_inner(mut self) -> Connection {
        self.conn
            .take()
            .expect("pooled connection must always contain a connection")
    }
}

impl Deref for PooledConnection {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        self.conn
            .as_ref()
            .expect("pooled connection must always contain a connection")
    }
}

impl DerefMut for PooledConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.conn
            .as_mut()
            .expect("pooled connection must always contain a connection")
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        let Some(conn) = self.conn.take() else {
            return;
        };

        let Some(pool) = self.pool.upgrade() else {
            return;
        };

        pool.release(conn);
    }
}

/// A prepared statement.
#[derive(Clone)]
pub struct Statement {
    conn: Connection,
    inner: Arc<Mutex<Box<turso_sdk_kit::rsapi::TursoStatement>>>,
}

struct Execute {
    stmt: Statement,
}

assert_send_sync!(Execute);

impl Future for Execute {
    type Output = Result<u64>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match self.stmt.step(None, cx)? {
            Poll::Ready(_) => {
                let n_change = self.stmt.inner.lock().unwrap().n_change();
                Poll::Ready(Ok(n_change as u64))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Statement {
    fn step(
        &self,
        columns: Option<usize>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<Option<Row>>> {
        let mut stmt = self.inner.lock().unwrap();
        match stmt.step(Some(cx.waker()))? {
            turso_sdk_kit::rsapi::TursoStatusCode::Row => {
                if let Some(columns) = columns {
                    let mut values = Vec::with_capacity(columns);
                    for i in 0..columns {
                        let value = stmt.row_value(i)?;
                        values.push(value.to_owned());
                    }
                    Poll::Ready(Ok(Some(Row { values })))
                } else {
                    Poll::Ready(Err(Error::Misuse(
                        "unexpected row during execution".to_string(),
                    )))
                }
            }
            turso_sdk_kit::rsapi::TursoStatusCode::Done => Poll::Ready(Ok(None)),
            turso_sdk_kit::rsapi::TursoStatusCode::Io => {
                stmt.run_io()?;
                if let Some(extra_io) = &self.conn.extra_io {
                    extra_io(cx.waker().clone())?;
                }
                Poll::Pending
            }
        }
    }
    /// Query the database with this prepared statement.
    pub async fn query(&mut self, params: impl IntoParams) -> Result<Rows> {
        self.reset()?;

        let mut stmt = self.inner.lock().unwrap();
        let params = params.into_params()?;
        match params {
            params::Params::None => (),
            params::Params::Positional(values) => {
                for (i, value) in values.into_iter().enumerate() {
                    stmt.bind_positional(i + 1, value.into())?;
                }
            }
            params::Params::Named(values) => {
                for (name, value) in values.into_iter() {
                    let position = stmt.named_position(name)?;
                    stmt.bind_positional(position, value.into())?;
                }
            }
        }
        let rows = Rows::new(self.clone());
        Ok(rows)
    }

    /// Execute this prepared statement.
    pub async fn execute(&mut self, params: impl IntoParams) -> Result<u64> {
        {
            // Reset the statement before executing
            self.inner.lock().unwrap().reset()?;
        }
        let params = params.into_params()?;
        match params {
            params::Params::None => (),
            params::Params::Positional(values) => {
                for (i, value) in values.into_iter().enumerate() {
                    let mut stmt = self.inner.lock().unwrap();
                    stmt.bind_positional(i + 1, value.into())?;
                }
            }
            params::Params::Named(values) => {
                for (name, value) in values.into_iter() {
                    let mut stmt = self.inner.lock().unwrap();
                    let position = stmt.named_position(name)?;
                    stmt.bind_positional(position, value.into())?;
                }
            }
        }

        let execute = Execute { stmt: self.clone() };
        execute.await
    }

    /// Returns the number of columns in the result set.
    pub fn column_count(&self) -> usize {
        self.inner.lock().unwrap().column_count()
    }

    /// Returns the name of the column at the given index.
    pub fn column_name(&self, idx: usize) -> Result<String> {
        let stmt = self.inner.lock().unwrap();
        if idx >= stmt.column_count() {
            return Err(Error::Misuse(format!(
                "column index {idx} out of bounds (statement has {} columns)",
                stmt.column_count()
            )));
        }
        Ok(stmt
            .column_name(idx)
            .expect("column index must be within valid range")
            .into_owned())
    }

    /// Returns the names of all columns in the result set.
    pub fn column_names(&self) -> Vec<String> {
        let stmt = self.inner.lock().unwrap();
        let n = stmt.column_count();
        (0..n)
            .map(|i| {
                stmt.column_name(i)
                    .expect("column index must be within valid range")
                    .into_owned()
            })
            .collect()
    }

    /// Returns the index of the column with the given name.
    pub fn column_index(&self, name: &str) -> Result<usize> {
        let stmt = self.inner.lock().unwrap();
        let n = stmt.column_count();
        for i in 0..n {
            let col_name = stmt
                .column_name(i)
                .expect("column index must be within valid range");
            if col_name.eq_ignore_ascii_case(name) {
                return Ok(i);
            }
        }
        Err(Error::Misuse(format!(
            "column '{name}' not found in result set"
        )))
    }

    /// Returns columns of the result of this prepared statement.
    pub fn columns(&self) -> Vec<Column> {
        let stmt = self.inner.lock().unwrap();

        let n = stmt.column_count();

        let mut cols = Vec::with_capacity(n);

        for i in 0..n {
            let name = stmt
                .column_name(i)
                .expect("column index must be within valid range")
                .into_owned();
            let decl_type = stmt.column_decltype(i);
            cols.push(Column { name, decl_type });
        }

        cols
    }

    /// Reset internal statement state after previous execution so it can be reused again
    pub fn reset(&self) -> Result<()> {
        let mut stmt = self.inner.lock().unwrap();
        stmt.reset()?;
        Ok(())
    }

    /// Execute a query that returns the first [`Row`].
    ///
    /// # Errors
    ///
    /// - Returns `QueryReturnedNoRows` if no rows were returned.
    pub async fn query_row(&mut self, params: impl IntoParams) -> Result<Row> {
        let mut rows = self.query(params).await?;

        let first_row = rows.next().await?.ok_or(Error::QueryReturnedNoRows)?;
        // Discard remaining rows so that the statement is executed to completion
        // Otherwise Drop of the statement will cause transaction rollback
        while rows.next().await?.is_some() {}
        Ok(first_row)
    }
}

/// Column information.
pub struct Column {
    name: String,
    decl_type: Option<String>,
}

impl Column {
    /// Return the name of the column.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the type of the column.
    pub fn decl_type(&self) -> Option<&str> {
        self.decl_type.as_deref()
    }
}

pub trait IntoValue {
    fn into_value(self) -> Result<Value>;
}

#[derive(Debug, Clone)]
pub enum Params {
    None,
    Positional(Vec<Value>),
    Named(Vec<(String, Value)>),
}

pub struct Transaction {}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    async fn cache_size(conn: &Connection) -> Result<i64> {
        let mut rows = conn.query("PRAGMA cache_size", ()).await?;
        let row = rows.next().await?.expect("expected PRAGMA cache_size row");
        row.get(0)
    }

    #[tokio::test]
    async fn test_database_persistence() -> Result<()> {
        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path().to_str().unwrap();

        // First, create the database, a table, and insert some data
        {
            let db = Builder::new_local(db_path).build().await?;
            let conn = db.connect()?;
            conn.execute(
                "CREATE TABLE test_persistence (id INTEGER PRIMARY KEY, name TEXT NOT NULL);",
                (),
            )
            .await?;
            conn.execute("INSERT INTO test_persistence (name) VALUES ('Alice');", ())
                .await?;
            conn.execute("INSERT INTO test_persistence (name) VALUES ('Bob');", ())
                .await?;
        } // db and conn are dropped here, simulating closing

        // Now, re-open the database and check if the data is still there
        let db = Builder::new_local(db_path).build().await?;
        let conn = db.connect()?;

        let mut rows = conn
            .query("SELECT name FROM test_persistence ORDER BY id;", ())
            .await?;

        let row1 = rows.next().await?.expect("Expected first row");
        assert_eq!(row1.get_value(0)?, Value::Text("Alice".to_string()));

        let row2 = rows.next().await?.expect("Expected second row");
        assert_eq!(row2.get_value(0)?, Value::Text("Bob".to_string()));

        assert!(rows.next().await?.is_none(), "Expected no more rows");

        Ok(())
    }

    #[tokio::test]
    async fn test_database_persistence_many_frames() -> Result<()> {
        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path().to_str().unwrap();

        const NUM_INSERTS: usize = 100;
        const TARGET_STRING_LEN: usize = 1024; // 1KB

        let mut original_data = Vec::with_capacity(NUM_INSERTS);
        for i in 0..NUM_INSERTS {
            let prefix = format!("test_string_{i:04}_");
            let padding_len = TARGET_STRING_LEN.saturating_sub(prefix.len());
            let padding: String = "A".repeat(padding_len);
            original_data.push(format!("{prefix}{padding}"));
        }

        // First, create the database, a table, and insert many large strings
        {
            let db = Builder::new_local(db_path).build().await?;
            let conn = db.connect()?;
            conn.execute(
                "CREATE TABLE test_large_persistence (id INTEGER PRIMARY KEY AUTOINCREMENT, data TEXT NOT NULL);",
                (),
            )
            .await?;

            for data_val in &original_data {
                conn.execute(
                    "INSERT INTO test_large_persistence (data) VALUES (?);",
                    params::Params::Positional(vec![Value::Text(data_val.clone())]),
                )
                .await?;
            }
        } // db and conn are dropped here, simulating closing

        {
            // Now, re-open the database and check if the data is still there
            let db = Builder::new_local(db_path).build().await?;
            let conn = db.connect()?;

            let mut rows = conn
                .query("SELECT data FROM test_large_persistence ORDER BY id;", ())
                .await?;

            for (i, value) in original_data.iter().enumerate().take(NUM_INSERTS) {
                let row = rows
                    .next()
                    .await?
                    .unwrap_or_else(|| panic!("Expected row {i} but found None"));
                assert_eq!(
                    row.get_value(0)?,
                    Value::Text(value.clone()),
                    "Mismatch in retrieved data for row {i}"
                );
            }

            assert!(
                rows.next().await?.is_none(),
                "Expected no more rows after retrieving all inserted data"
            );

            // Delete the WAL file only and try to re-open and query
            let wal_path = format!("{db_path}-wal");
            std::fs::remove_file(&wal_path)
                .map_err(|e| eprintln!("Warning: Failed to delete WAL file for test: {e}"))
                .unwrap();
        }

        // Attempt to re-open the database after deleting WAL and assert that table is missing.
        let db_after_wal_delete = Builder::new_local(db_path).build().await?;
        let conn_after_wal_delete = db_after_wal_delete.connect()?;

        let query_result_after_wal_delete = conn_after_wal_delete
            .query("SELECT data FROM test_large_persistence ORDER BY id;", ())
            .await;

        match query_result_after_wal_delete {
            Ok(_) => panic!("Query succeeded after WAL deletion and DB reopen, but was expected to fail because the table definition should have been in the WAL."),
            Err(Error::Error(msg)) => {
                assert!(
                    msg.contains("no such table: test_large_persistence"),
                    "Expected 'test_large_persistence not found' error, but got: {msg}"
                );
            }
            Err(e) => panic!(
                "Expected SqlExecutionFailure for 'no such table', but got a different error: {e:?}"
            ),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_rows_column_names() -> Result<()> {
        let db = Builder::new_local(":memory:").build().await?;
        let conn = db.connect()?;
        conn.execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT);",
            (),
        )
        .await?;
        conn.execute(
            "INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.org');",
            (),
        )
        .await?;

        let rows = conn.query("SELECT id, name, email FROM users;", ()).await?;

        // columns()
        let columns = rows.columns();
        let names: Vec<&str> = columns.iter().map(|c| c.name()).collect();
        assert_eq!(names, vec!["id", "name", "email"]);

        // column_count()
        assert_eq!(rows.column_count(), 3);

        // column_name()
        assert_eq!(rows.column_name(0)?, "id");
        assert_eq!(rows.column_name(1)?, "name");
        assert_eq!(rows.column_name(2)?, "email");
        assert!(rows.column_name(3).is_err());

        // column_names()
        assert_eq!(rows.column_names(), vec!["id", "name", "email"]);

        // column_index()
        assert_eq!(rows.column_index("id")?, 0);
        assert_eq!(rows.column_index("name")?, 1);
        assert_eq!(rows.column_index("email")?, 2);
        assert_eq!(rows.column_index("EMAIL")?, 2); // case-insensitive
        assert!(rows.column_index("nonexistent").is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_database_persistence_write_one_frame_many_times() -> Result<()> {
        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path().to_str().unwrap();

        for i in 0..100 {
            {
                let db = Builder::new_local(db_path).build().await?;
                let conn = db.connect()?;

                conn.execute("CREATE TABLE IF NOT EXISTS test_persistence (id INTEGER PRIMARY KEY, name TEXT NOT NULL);", ()).await?;
                conn.execute("INSERT INTO test_persistence (name) VALUES ('Alice');", ())
                    .await?;
            }
            {
                let db = Builder::new_local(db_path).build().await?;
                let conn = db.connect()?;

                let mut rows_iter = conn
                    .query("SELECT count(*) FROM test_persistence;", ())
                    .await?;
                let rows = rows_iter.next().await?.unwrap();
                assert_eq!(rows.get_value(0)?, Value::Integer(i as i64 + 1));
                assert!(rows_iter.next().await?.is_none());
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_parallel_writes_and_wal_size() -> Result<()> {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let db_path_str = db_path.to_str().unwrap();

        let db = Builder::new_local(db_path_str).build().await?;
        let conn = db.connect()?;
        conn.execute(
            "CREATE TABLE test_data (id INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT NOT NULL);",
            (),
        )
        .await?;

        // Generate a ~200KB payload
        let payload = "X".repeat(200 * 1024);

        // Parallel writes: spawn 8 connections, each inserting 5 rows
        let mut handles = Vec::new();
        for conn_id in 0..8u32 {
            let db = db.clone();
            let payload = payload.clone();
            handles.push(tokio::spawn(async move {
                let conn = db.connect().unwrap();
                for row_id in 0..5u32 {
                    let tag = format!("conn{conn_id}_row{row_id}");
                    let data = format!("{tag}_{payload}");
                    loop {
                        match conn
                            .execute(
                                "INSERT INTO test_data (payload) VALUES (?);",
                                params::Params::Positional(vec![Value::Text(data.clone())]),
                            )
                            .await
                        {
                            Ok(_) => break,
                            Err(Error::Busy(_)) => {
                                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                                continue;
                            }
                            Err(e) => panic!("Insert failed: {e:?}"),
                        }
                    }
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        // Sequential writes: 3 more large inserts
        for i in 0..3 {
            let data = format!("sequential_{i}_{payload}");
            conn.execute(
                "INSERT INTO test_data (payload) VALUES (?);",
                params::Params::Positional(vec![Value::Text(data)]),
            )
            .await?;
        }

        // Verify row count: 8*5 + 3 = 43
        let mut rows = conn.query("SELECT count(*) FROM test_data;", ()).await?;
        let row = rows.next().await?.unwrap();
        assert_eq!(row.get_value(0)?, Value::Integer(43));

        // Report WAL size
        let wal_path = format!("{db_path_str}-wal");
        let wal_size = std::fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);
        eprintln!(
            "WAL size after all writes: {} bytes ({:.2} KB)",
            wal_size,
            wal_size as f64 / 1024.0
        );
        assert!(wal_size > 0, "WAL file should exist and be non-empty");

        Ok(())
    }

    #[tokio::test]
    async fn test_connect_pooled_reuses_connection() -> Result<()> {
        let db = Builder::new_local(":memory:").build().await?;

        {
            let conn = db.connect_pooled()?;
            conn.execute("PRAGMA cache_size = 1234", ()).await?;
        }

        let conn = db.connect_pooled()?;
        assert_eq!(cache_size(&conn).await?, 1234);

        Ok(())
    }

    #[tokio::test]
    async fn test_connect_pooled_does_not_recycle_with_live_statement() -> Result<()> {
        let db = Builder::new_local(":memory:").build().await?;

        let conn = db.connect_pooled()?;
        conn.execute("PRAGMA cache_size = 3456", ()).await?;
        let _stmt = conn.prepare("SELECT 1").await?;
        drop(conn);

        let conn = db.connect_pooled()?;
        assert_ne!(cache_size(&conn).await?, 3456);

        Ok(())
    }

    #[tokio::test]
    async fn test_connect_pooled_respects_max_idle_connections() -> Result<()> {
        let db = Builder::new_local(":memory:")
            .max_idle_connections(0)
            .build()
            .await?;

        {
            let conn = db.connect_pooled()?;
            conn.execute("PRAGMA cache_size = 5678", ()).await?;
        }

        let conn = db.connect_pooled()?;
        assert_ne!(cache_size(&conn).await?, 5678);

        Ok(())
    }

    #[tokio::test]
    async fn test_connect_pooled_capacity_drops_extra_idle_connections() -> Result<()> {
        let db = Builder::new_local(":memory:")
            .max_idle_connections(1)
            .build()
            .await?;

        let conn_a = db.connect_pooled()?;
        conn_a.execute("PRAGMA cache_size = 12345", ()).await?;

        let conn_b = db.connect_pooled()?;
        conn_b.execute("PRAGMA cache_size = 54321", ()).await?;

        // Capacity is 1. Drop A first so A is retained, then B should be dropped.
        drop(conn_a);
        drop(conn_b);

        let conn_from_pool = db.connect_pooled()?;
        assert_eq!(cache_size(&conn_from_pool).await?, 12345);

        // With capacity=1 and first pooled conn still checked out, this must be fresh.
        let fresh_conn = db.connect_pooled()?;
        let fresh_cache = cache_size(&fresh_conn).await?;
        assert_ne!(fresh_cache, 12345);
        assert_ne!(fresh_cache, 54321);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_connect_pooled_concurrency_smoke() -> Result<()> {
        let db = Builder::new_local(":memory:")
            .max_idle_connections(8)
            .build()
            .await?;

        let task_count = 16;
        let iterations = 50;
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(task_count));
        let mut handles = Vec::new();

        for _ in 0..task_count {
            let db = db.clone();
            let barrier = barrier.clone();
            handles.push(tokio::spawn(async move {
                barrier.wait().await;
                for _ in 0..iterations {
                    let conn = db.connect_pooled()?;
                    let mut rows = conn.query("SELECT 1", ()).await?;
                    let row = rows.next().await?.expect("expected row from SELECT 1");
                    assert_eq!(row.get::<i64>(0)?, 1);
                }
                Ok::<(), Error>(())
            }));
        }

        for handle in handles {
            handle.await.expect("task panicked")?;
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_connect_pooled_into_inner_bypasses_pool() -> Result<()> {
        let db = Builder::new_local(":memory:").build().await?;

        let pooled = db.connect_pooled()?;
        pooled.execute("PRAGMA cache_size = 32123", ()).await?;

        let conn = pooled.into_inner();
        drop(conn);

        let conn = db.connect_pooled()?;
        assert_ne!(cache_size(&conn).await?, 32123);

        Ok(())
    }

    #[tokio::test]
    async fn test_connect_pooled_supports_mut_connection_methods() -> Result<()> {
        use crate::transaction::TransactionBehavior;

        let db = Builder::new_local(":memory:").build().await?;
        let mut conn = db.connect_pooled()?;

        conn.set_transaction_behavior(TransactionBehavior::Immediate);
        let tx = conn.transaction().await?;
        tx.rollback().await?;
        assert!(conn.is_autocommit()?);

        Ok(())
    }
}
