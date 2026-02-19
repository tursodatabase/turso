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
pub use value::Value;

pub use params::params_from_iter;
pub use params::IntoParams;

use std::fmt::Debug;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
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
    #[error("I/O error: {0}")]
    IoError(std::io::ErrorKind),
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
            turso_sdk_kit::rsapi::TursoError::IoError(kind) => Error::IoError(kind),
        }
    }
}

pub(crate) type BoxError = Box<dyn std::error::Error + Send + Sync>;

pub type Result<T> = std::result::Result<T, Error>;
pub type EncryptionOpts = turso_sdk_kit::rsapi::EncryptionOpts;

/// A builder for `Database`.
pub struct Builder {
    path: String,
    enable_encryption: bool,
    enable_triggers: bool,
    enable_attach: bool,
    enable_strict: bool,
    enable_index_method: bool,
    enable_materialized_views: bool,
    vfs: Option<String>,
    encryption_opts: Option<turso_sdk_kit::rsapi::EncryptionOpts>,
}

impl Builder {
    /// Create a new local database.
    pub fn new_local(path: &str) -> Self {
        Self {
            path: path.to_string(),
            enable_encryption: false,
            enable_triggers: false,
            enable_attach: false,
            enable_strict: false,
            enable_index_method: false,
            enable_materialized_views: false,
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

    pub fn experimental_triggers(mut self, triggers_enabled: bool) -> Self {
        self.enable_triggers = triggers_enabled;
        self
    }

    pub fn experimental_attach(mut self, attach_enabled: bool) -> Self {
        self.enable_attach = attach_enabled;
        self
    }

    pub fn experimental_strict(mut self, strict_enabled: bool) -> Self {
        self.enable_strict = strict_enabled;
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
    fn build_features_string(&self) -> Option<String> {
        let mut features = Vec::new();
        if self.enable_encryption {
            features.push("encryption");
        }
        if self.enable_triggers {
            features.push("triggers");
        }
        if self.enable_attach {
            features.push("attach");
        }
        if self.enable_strict {
            features.push("strict");
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
                async_io: false,
                encryption: self.encryption_opts,
                vfs: self.vfs,
                io: None,
                db_file: None,
            });
        let result = db.open()?;
        // async_io is false - so db.open() will return result immediately
        assert!(!result.is_io());
        Ok(Database { inner: db })
    }
}

/// A database.
///
/// The `Database` object points to a database and allows you to connect to it
#[derive(Clone)]
pub struct Database {
    inner: Arc<turso_sdk_kit::rsapi::TursoDatabase>,
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
            cols.push(Column {
                name,
                decl_type: None, // TODO
            });
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
}
