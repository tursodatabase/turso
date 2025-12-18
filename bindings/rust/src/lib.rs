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

pub use connection::Connection;
pub use value::Value;

pub use params::params_from_iter;
pub use params::IntoParams;

use std::fmt::Debug;
use std::future::Future;
use std::num::NonZero;
use std::sync::{Arc, Mutex};
use std::task::Poll;
pub use turso_core::EncryptionOpts;
use turso_core::OpenFlags;

// Re-exports rows
pub use crate::rows::{Row, Rows};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("SQL conversion failure: `{0}`")]
    ToSqlConversionFailure(BoxError),
    #[error("Mutex lock error: {0}")]
    MutexError(String),
    #[error("SQL execution failure: `{0}`")]
    SqlExecutionFailure(String),
    #[error("WAL operation error: `{0}`")]
    WalOperationError(String),
    #[error("Query returned no rows")]
    QueryReturnedNoRows,
    #[error("Conversion failure: `{0}`")]
    ConversionFailure(String),
}

impl From<turso_core::LimboError> for Error {
    fn from(err: turso_core::LimboError) -> Self {
        Error::SqlExecutionFailure(err.to_string())
    }
}

pub(crate) type BoxError = Box<dyn std::error::Error + Send + Sync>;

pub type Result<T> = std::result::Result<T, Error>;

/// A builder for `Database`.
pub struct Builder {
    path: String,
    enable_mvcc: bool,
    enable_encryption: bool,
    vfs: Option<String>,
    encryption_opts: Option<EncryptionOpts>,
}

impl Builder {
    /// Create a new local database.
    pub fn new_local(path: &str) -> Self {
        Self {
            path: path.to_string(),
            enable_mvcc: false,
            enable_encryption: false,
            vfs: None,
            encryption_opts: None,
        }
    }

    pub fn with_mvcc(mut self, mvcc_enabled: bool) -> Self {
        self.enable_mvcc = mvcc_enabled;
        self
    }

    pub fn experimental_encryption(mut self, encryption_enabled: bool) -> Self {
        self.enable_encryption = encryption_enabled;
        self
    }

    pub fn with_encryption(mut self, opts: EncryptionOpts) -> Self {
        self.encryption_opts = Some(opts);
        self
    }

    pub fn with_io(mut self, vfs: String) -> Self {
        self.vfs = Some(vfs);
        self
    }

    /// Build the database.
    #[allow(unused_variables, clippy::arc_with_non_send_sync)]
    pub async fn build(self) -> Result<Database> {
        let io = self.get_io()?;
        let opts = turso_core::DatabaseOpts::default()
            .with_mvcc(self.enable_mvcc)
            .with_encryption(self.enable_encryption);
        let db = turso_core::Database::open_file_with_flags(
            io,
            self.path.as_str(),
            OpenFlags::default(),
            opts,
            self.encryption_opts.clone(),
        )?;
        Ok(Database { inner: db })
    }

    fn get_io(&self) -> Result<Arc<dyn turso_core::IO>> {
        let vfs_choice = self.vfs.as_deref().unwrap_or("");

        if self.path == ":memory:" && vfs_choice.is_empty() {
            return Ok(Arc::new(turso_core::MemoryIO::new()));
        }

        match vfs_choice {
            "memory" => Ok(Arc::new(turso_core::MemoryIO::new())),
            "syscall" => {
                #[cfg(all(target_family = "unix", not(miri)))]
                {
                    Ok(Arc::new(
                        turso_core::UnixIO::new()
                            .map_err(|e| Error::SqlExecutionFailure(e.to_string()))?,
                    ))
                }
                #[cfg(any(not(target_family = "unix"), miri))]
                {
                    Ok(Arc::new(
                        turso_core::PlatformIO::new()
                            .map_err(|e| Error::SqlExecutionFailure(e.to_string()))?,
                    ))
                }
            }
            #[cfg(all(target_os = "linux", not(miri)))]
            "io_uring" => Ok(Arc::new(
                turso_core::UringIO::new()
                    .map_err(|e| Error::SqlExecutionFailure(e.to_string()))?,
            )),
            #[cfg(any(not(target_os = "linux"), miri))]
            "io_uring" => Err(Error::SqlExecutionFailure(
                "io_uring is only available on Linux targets".to_string(),
            )),
            "" => {
                // Default behavior: memory for ":memory:", platform IO for files
                if self.path == ":memory:" {
                    Ok(Arc::new(turso_core::MemoryIO::new()))
                } else {
                    Ok(Arc::new(
                        turso_core::PlatformIO::new()
                            .map_err(|e| Error::SqlExecutionFailure(e.to_string()))?,
                    ))
                }
            }
            _ => Ok(Arc::new(
                turso_core::PlatformIO::new()
                    .map_err(|e| Error::SqlExecutionFailure(e.to_string()))?,
            )),
        }
    }
}

/// A database.
///
/// The `Database` object points to a database and allows you to connect to it
#[derive(Clone)]
pub struct Database {
    inner: Arc<turso_core::Database>,
}

unsafe impl Send for Database {}
unsafe impl Sync for Database {}

impl Debug for Database {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Database").finish()
    }
}

impl Database {
    /// Connect to the database.
    pub fn connect(&self) -> Result<Connection> {
        let conn = self.inner.connect()?;
        Ok(Connection::create(conn))
    }
}

/// A prepared statement.
pub struct Statement {
    inner: Arc<Mutex<turso_core::Statement>>,
}

impl Clone for Statement {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

unsafe impl Send for Statement {}
unsafe impl Sync for Statement {}

struct Execute {
    stmt: Arc<Mutex<turso_core::Statement>>,
}

unsafe impl Send for Execute {}
unsafe impl Sync for Execute {}

impl Future for Execute {
    type Output = Result<u64>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let mut stmt = self.stmt.lock().unwrap();
        match stmt.step_with_waker(cx.waker()) {
            Ok(turso_core::StepResult::Row) => Poll::Ready(Err(Error::SqlExecutionFailure(
                "unexpected row during execution".to_string(),
            ))),
            Ok(turso_core::StepResult::Done) => {
                let changes = stmt.n_change();
                assert!(changes >= 0);
                Poll::Ready(Ok(changes as u64))
            }
            Ok(turso_core::StepResult::IO) => {
                stmt.run_once()?;
                Poll::Pending
            }
            Ok(turso_core::StepResult::Busy) => Poll::Ready(Err(Error::SqlExecutionFailure(
                "database is locked".to_string(),
            ))),
            Ok(turso_core::StepResult::Interrupt) => {
                Poll::Ready(Err(Error::SqlExecutionFailure("interrupted".to_string())))
            }
            Err(err) => Poll::Ready(Err(err.into())),
        }
    }
}

impl Statement {
    /// Query the database with this prepared statement.
    pub async fn query(&mut self, params: impl IntoParams) -> Result<Rows> {
        let params = params.into_params()?;
        match params {
            params::Params::None => (),
            params::Params::Positional(values) => {
                for (i, value) in values.into_iter().enumerate() {
                    let mut stmt = self.inner.lock().unwrap();
                    stmt.bind_at(NonZero::new(i + 1).unwrap(), value.into());
                }
            }
            params::Params::Named(values) => {
                for (name, value) in values.into_iter() {
                    let mut stmt = self.inner.lock().unwrap();
                    let i = stmt.parameters().index(name).unwrap();
                    stmt.bind_at(i, value.into());
                }
            }
        }
        let rows = Rows::new(&self.inner);
        Ok(rows)
    }

    /// Execute this prepared statement.
    pub async fn execute(&mut self, params: impl IntoParams) -> Result<u64> {
        {
            // Reset the statement before executing
            self.inner.lock().unwrap().reset();
        }
        let params = params.into_params()?;
        match params {
            params::Params::None => (),
            params::Params::Positional(values) => {
                for (i, value) in values.into_iter().enumerate() {
                    let mut stmt = self.inner.lock().unwrap();
                    stmt.bind_at(NonZero::new(i + 1).unwrap(), value.into());
                }
            }
            params::Params::Named(values) => {
                for (name, value) in values.into_iter() {
                    let mut stmt = self.inner.lock().unwrap();
                    let i = stmt.parameters().index(&name).ok_or(
                        turso_core::LimboError::InvalidArgument(
                            format!("Unknown parameter '{name}' for query '{}'. Make sure you're using the correct parameter syntax - named: (:foo), positional: (?, ?)", stmt.get_sql())
                        ))?;
                    stmt.bind_at(i, value.into());
                }
            }
        }

        let execute = Execute {
            stmt: self.inner.clone(),
        };
        execute.await
    }

    /// Returns columns of the result of this prepared statement.
    pub fn columns(&self) -> Vec<Column> {
        let stmt = self.inner.lock().unwrap();

        let n = stmt.num_columns();

        let mut cols = Vec::with_capacity(n);

        for i in 0..n {
            let name = stmt.get_column_name(i).into_owned();
            cols.push(Column {
                name,
                decl_type: None, // TODO
            });
        }

        cols
    }

    /// Reset internal statement state after previous execution so it can be reused again
    pub fn reset(&self) {
        let mut stmt = self.inner.lock().unwrap();
        stmt.reset();
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
            Err(Error::SqlExecutionFailure(msg)) => {
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
