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

pub mod params;
pub mod value;

use turso_core::types::{DatabaseChange, DatabaseChangeType, ImmutableRecord, RecordCursor};
pub use value::Value;

pub use params::params_from_iter;

use crate::params::*;
use std::collections::{VecDeque};
use std::collections::HashMap;
use std::fmt::Debug;
use std::i64;
use std::num::NonZero;
use std::sync::{Arc, Mutex};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("SQL conversion failure: `{0}`")]
    ToSqlConversionFailure(BoxError),
    #[error("Mutex lock error: {0}")]
    MutexError(String),
    #[error("SQL execution failure: `{0}`")]
    SqlExecutionFailure(String),
    #[error("SQL internal failure: `{0}`")]
    InternalError(String),
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
}

impl Builder {
    /// Create a new local database.
    pub fn new_local(path: &str) -> Self {
        Self {
            path: path.to_string(),
        }
    }

    /// Build the database.
    #[allow(unused_variables, clippy::arc_with_non_send_sync)]
    pub async fn build(self) -> Result<Database> {
        match self.path.as_str() {
            ":memory:" => {
                let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::MemoryIO::new());
                let db = turso_core::Database::open_file(
                    io,
                    self.path.as_str(),
                    false,
                    indexes_enabled(),
                )?;
                Ok(Database { inner: db })
            }
            path => {
                let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new()?);
                let db = turso_core::Database::open_file(io, path, false, indexes_enabled())?;
                Ok(Database { inner: db })
            }
        }
    }
}

fn indexes_enabled() -> bool {
    #[cfg(feature = "experimental_indexes")]
    return true;
    #[cfg(not(feature = "experimental_indexes"))]
    return false;
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
        #[allow(clippy::arc_with_non_send_sync)]
        let connection = Connection {
            inner: Arc::new(Mutex::new(conn)),
        };
        Ok(connection)
    }
}

/// A database connection.
pub struct Connection {
    inner: Arc<Mutex<Arc<turso_core::Connection>>>,
}

impl Clone for Connection {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

unsafe impl Send for Connection {}
unsafe impl Sync for Connection {}

pub struct DatabaseReplayContext {
    cached_delete_stmt: HashMap<String, Statement>,
    cached_insert_stmt: HashMap<(String, usize), Statement>,
}

impl DatabaseReplayContext {
    pub fn new() -> Self {
        DatabaseReplayContext {
            cached_delete_stmt: HashMap::new(),
            cached_insert_stmt: HashMap::new(),
        }
    }
}

pub struct DatabaseChangesIterator {
    query_stmt: Statement,
    previous_change_id: Option<i64>,
    batch: VecDeque<DatabaseChange>,
}

impl DatabaseChangesIterator {
    pub async fn next(&mut self) -> Result<Option<DatabaseChange>> {
        if self.batch.is_empty() {
            let change_id_filter = self.previous_change_id.unwrap_or(-1);
            self.query_stmt.reset();
            let mut rows = self.query_stmt.query((change_id_filter,)).await?;
            while let Some(row) = rows.next().await? {
                let change_id = match row.get_value(0)? {
                    Value::Integer(i) => Ok(i as i64),
                    v => Err(Error::InternalError(format!(
                        "change_id column type mismatch: expected integer, got '{v:?}'"
                    ))),
                }?;
                let change_time = match row.get_value(1)? {
                    Value::Integer(i) => Ok(i as u64),
                    v => Err(Error::InternalError(format!(
                        "change_time column type mismatch: expected integer, got '{v:?}'"
                    ))),
                }?;
                let table_name = match row.get_value(3)? {
                    Value::Text(t) => Ok(t),
                    v => Err(Error::InternalError(format!(
                        "table_name column type mismatch: expected string, got '{v:?}'"
                    ))),
                }?;
                let id = match row.get_value(4)? {
                    Value::Integer(i) => Ok(i),
                    v => Err(Error::InternalError(format!(
                        "id column type mismatch: expected integer, got '{v:?}'"
                    ))),
                }?;
                let change = {
                    let after = || match row.get_value(6) {
                        Ok(Value::Blob(b)) => Ok(b),
                        v => Err(Error::InternalError(format!(
                            "after column type mismatch: expected blob, got '{v:?}'"
                        ))),
                    };
                    match row.get_value(2)? {
                        Value::Integer(-1) => Ok(DatabaseChangeType::Delete),
                        Value::Integer(0) => Ok(DatabaseChangeType::Update {
                            bin_record: after()?,
                        }),
                        Value::Integer(1) => Ok(DatabaseChangeType::Insert {
                            bin_record: after()?,
                        }),
                        v => Err(Error::InternalError(format!(
                            "change_type column type mismatch: expected -1|0|1, got '{v:?}'"
                        ))),
                    }?
                };
                self.batch.push_back(DatabaseChange {
                    change_id,
                    change_time,
                    change,
                    table_name,
                    id,
                });
            }
            let batch_len = self.batch.len();
            if batch_len > 0 {
                self.previous_change_id = Some(self.batch[batch_len - 1].change_id);
            }
        }
        Ok(self.batch.pop_front())
    }
}

impl Connection {
    /// Query the database with SQL.
    pub async fn query(&self, sql: &str, params: impl IntoParams) -> Result<Rows> {
        let mut stmt = self.prepare(sql).await?;
        stmt.query(params).await
    }

    /// Execute SQL statement on the database.
    pub async fn execute(&self, sql: &str, params: impl IntoParams) -> Result<u64> {
        let mut stmt = self.prepare(sql).await?;
        stmt.execute(params).await
    }

    /// Prepare a SQL statement for later execution.
    pub async fn prepare(&self, sql: &str) -> Result<Statement> {
        let conn = self
            .inner
            .lock()
            .map_err(|e| Error::MutexError(e.to_string()))?;

        let stmt = conn.prepare(sql)?;

        #[allow(clippy::arc_with_non_send_sync)]
        let statement = Statement {
            inner: Arc::new(Mutex::new(stmt)),
        };
        Ok(statement)
    }

    pub async fn changes_iterator(
        &self,
        cdc_table_name: &str,
        previous_change_id: Option<i64>,
        batch_size: usize,
    ) -> Result<DatabaseChangesIterator> {
        let query_stmt = self
            .prepare(&format!(
                "SELECT * FROM {} WHERE operation_id > ? LIMIT {}",
                cdc_table_name, batch_size
            ))
            .await?;
        Ok(DatabaseChangesIterator {
            previous_change_id,
            batch: VecDeque::with_capacity(batch_size),
            query_stmt,
        })
    }

    pub async fn replay_change(
        &self,
        ctx: &mut DatabaseReplayContext,
        change: DatabaseChange,
    ) -> Result<()> {
        let table_name = &change.table_name;
        if let DatabaseChangeType::Delete | DatabaseChangeType::Update { .. } = &change.change {
            if !ctx.cached_delete_stmt.contains_key(table_name) {
                let query = format!("DELETE FROM {} WHERE rowid = ?", table_name);
                let stmt = self.prepare(&query).await?;
                ctx.cached_delete_stmt.insert(table_name.clone(), stmt);
            }
            let stmt = ctx.cached_delete_stmt.get_mut(table_name).unwrap();
            stmt.execute((change.id,)).await?;
        }
        if let DatabaseChangeType::Update { bin_record }
        | DatabaseChangeType::Insert { bin_record } = change.change
        {
            let record = ImmutableRecord::from_bin_record(bin_record);
            let mut cursor = RecordCursor::new();
            let columns = cursor.count(&record);
            let key = (table_name.to_string(), columns);
            if !ctx.cached_insert_stmt.contains_key(&key) {
                let placeholders = ["?"].repeat(columns).join(",");
                let query = format!("INSERT INTO {} VALUES ({})", table_name, placeholders);
                let stmt = self.prepare(&query).await?;
                ctx.cached_insert_stmt.insert(key.clone(), stmt);
            }
            let mut values = Vec::with_capacity(columns);
            for i in 0..columns {
                values.push(match cursor.get_value(&record, i)?.to_owned() {
                    turso_core::Value::Null => Value::Null,
                    turso_core::Value::Integer(x) => Value::Integer(x),
                    turso_core::Value::Float(x) => Value::Real(x),
                    turso_core::Value::Text(x) => Value::Text(x.as_str().to_string()),
                    turso_core::Value::Blob(x) => Value::Blob(x),
                });
            }
            let stmt = ctx.cached_insert_stmt.get_mut(&key).unwrap();
            stmt.execute(params::Params::Positional(values)).await?;
        }
        Ok(())
    }

    /// Query a pragma.
    pub fn pragma_query<F>(&self, pragma_name: &str, mut f: F) -> Result<()>
    where
        F: FnMut(&Row) -> turso_core::Result<()>,
    {
        let conn = self
            .inner
            .lock()
            .map_err(|e| Error::MutexError(e.to_string()))?;

        let rows: Vec<Row> = conn
            .pragma_query(pragma_name)
            .map_err(|e| Error::SqlExecutionFailure(e.to_string()))?
            .iter()
            .map(|row| row.iter().collect::<Row>())
            .collect();

        rows.iter().try_for_each(|row| {
            f(row).map_err(|e| {
                Error::SqlExecutionFailure(format!("Error executing user defined function: {e}"))
            })
        })?;
        Ok(())
    }
}

impl Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection").finish()
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
        #[allow(clippy::arc_with_non_send_sync)]
        let rows = Rows {
            inner: Arc::clone(&self.inner),
        };
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
                    let i = stmt.parameters().index(name).unwrap();
                    stmt.bind_at(i, value.into());
                }
            }
        }
        loop {
            let mut stmt = self.inner.lock().unwrap();
            match stmt.step() {
                Ok(turso_core::StepResult::Row) => {
                    // unexpected row during execution, error out.
                    return Ok(2);
                }
                Ok(turso_core::StepResult::Done) => {
                    return Ok(0);
                }
                Ok(turso_core::StepResult::IO) => {
                    let _ = stmt.run_once();
                    //return Ok(1);
                }
                Ok(turso_core::StepResult::Busy) => {
                    return Ok(4);
                }
                Ok(turso_core::StepResult::Interrupt) => {
                    return Ok(3);
                }
                Err(err) => {
                    return Err(err.into());
                }
            }
        }
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

/// Results of a prepared statement query.
pub struct Rows {
    inner: Arc<Mutex<turso_core::Statement>>,
}

impl Clone for Rows {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

unsafe impl Send for Rows {}
unsafe impl Sync for Rows {}

impl Rows {
    /// Fetch the next row of this result set.
    pub async fn next(&mut self) -> Result<Option<Row>> {
        loop {
            let mut stmt = self
                .inner
                .lock()
                .map_err(|e| Error::MutexError(e.to_string()))?;
            match stmt.step() {
                Ok(turso_core::StepResult::Row) => {
                    let row = stmt.row().unwrap();
                    return Ok(Some(Row {
                        values: row.get_values().map(|v| v.to_owned()).collect(),
                    }));
                }
                Ok(turso_core::StepResult::Done) => return Ok(None),
                Ok(turso_core::StepResult::IO) => {
                    if let Err(e) = stmt.run_once() {
                        return Err(e.into());
                    }
                    continue;
                }
                Ok(turso_core::StepResult::Busy) => return Ok(None),
                Ok(turso_core::StepResult::Interrupt) => return Ok(None),
                _ => return Ok(None),
            }
        }
    }
}

/// Query result row.
#[derive(Debug)]
pub struct Row {
    values: Vec<turso_core::Value>,
}

unsafe impl Send for Row {}
unsafe impl Sync for Row {}

impl Row {
    pub fn get_value(&self, index: usize) -> Result<Value> {
        let value = &self.values[index];
        match value {
            turso_core::Value::Integer(i) => Ok(Value::Integer(*i)),
            turso_core::Value::Null => Ok(Value::Null),
            turso_core::Value::Float(f) => Ok(Value::Real(*f)),
            turso_core::Value::Text(text) => Ok(Value::Text(text.to_string())),
            turso_core::Value::Blob(items) => Ok(Value::Blob(items.to_vec())),
        }
    }

    pub fn column_count(&self) -> usize {
        self.values.len()
    }
}

impl<'a> FromIterator<&'a turso_core::Value> for Row {
    fn from_iter<T: IntoIterator<Item = &'a turso_core::Value>>(iter: T) -> Self {
        let values = iter
            .into_iter()
            .map(|v| match v {
                turso_core::Value::Integer(i) => turso_core::Value::Integer(*i),
                turso_core::Value::Null => turso_core::Value::Null,
                turso_core::Value::Float(f) => turso_core::Value::Float(*f),
                turso_core::Value::Text(s) => turso_core::Value::Text(s.clone()),
                turso_core::Value::Blob(b) => turso_core::Value::Blob(b.clone()),
            })
            .collect();

        Row { values }
    }
}

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

    async fn setup_schema(conn: &Connection) {
        conn.execute("CREATE TABLE a(x INTEGER PRIMARY KEY, y);", ())
            .await
            .unwrap();
        conn.execute("CREATE TABLE b(x INTEGER PRIMARY KEY, y, z);", ())
            .await
            .unwrap();
    }

    fn convert_value(value: turso_core::Value) -> Value {
        match value {
            turso_core::Value::Null => Value::Null,
            turso_core::Value::Integer(x) => Value::Integer(x),
            turso_core::Value::Float(x) => Value::Real(x),
            turso_core::Value::Text(x) => Value::Text(x.as_str().to_string()),
            turso_core::Value::Blob(x) => Value::Blob(x),
        }
    }

    async fn fetch_rows(conn: &Connection) -> Vec<Vec<Value>> {
        let mut rows = vec![];
        let mut iterator_a = conn.query("SELECT * FROM a", ()).await.unwrap();
        while let Some(row) = iterator_a.next().await.unwrap() {
            rows.push(row.values.into_iter().map(convert_value).collect());
        }
        let mut iterator_b = conn.query("SELECT * FROM b", ()).await.unwrap();
        while let Some(row) = iterator_b.next().await.unwrap() {
            rows.push(row.values.into_iter().map(convert_value).collect());
        }
        rows
    }

    #[tokio::test]
    async fn test_database_cdc() {
        let temp_file1 = NamedTempFile::new().unwrap();
        let db_path1 = temp_file1.path().to_str().unwrap();

        let temp_file2 = NamedTempFile::new().unwrap();
        let db_path2 = temp_file2.path().to_str().unwrap();

        let db1 = Builder::new_local(db_path1).build().await.unwrap();
        let conn1 = db1.connect().unwrap();

        let db2 = Builder::new_local(db_path2).build().await.unwrap();
        let conn2 = db2.connect().unwrap();

        setup_schema(&conn1).await;
        setup_schema(&conn2).await;

        conn1
            .execute("PRAGMA unstable_capture_data_changes_conn('full')", ())
            .await
            .unwrap();
        conn1
            .execute("INSERT INTO a VALUES (1, 'hello'), (2, 'turso')", ())
            .await
            .unwrap();

        conn1
            .execute(
                "INSERT INTO b VALUES (3, 'bye', 0.1), (4, 'limbo', 0.2)",
                (),
            )
            .await
            .unwrap();

        let mut ctx = DatabaseReplayContext::new();
        let mut changes = conn1.changes_iterator("turso_cdc", None, 10).await.unwrap();
        while let Some(change) = changes.next().await.unwrap() {
            conn2.replay_change(&mut ctx, change).await.unwrap();
        }

        assert_eq!(
            fetch_rows(&conn2).await,
            vec![
                vec![Value::Integer(1), Value::Text("hello".to_string())],
                vec![Value::Integer(2), Value::Text("turso".to_string())],
                vec![
                    Value::Integer(3),
                    Value::Text("bye".to_string()),
                    Value::Real(0.1)
                ],
                vec![
                    Value::Integer(4),
                    Value::Text("limbo".to_string()),
                    Value::Real(0.2)
                ],
            ]
        );

        conn1
            .execute("DELETE FROM b WHERE y = 'limbo'", ())
            .await
            .unwrap();

        while let Some(change) = changes.next().await.unwrap() {
            conn2.replay_change(&mut ctx, change).await.unwrap();
        }

        assert_eq!(
            fetch_rows(&conn2).await,
            vec![
                vec![Value::Integer(1), Value::Text("hello".to_string())],
                vec![Value::Integer(2), Value::Text("turso".to_string())],
                vec![
                    Value::Integer(3),
                    Value::Text("bye".to_string()),
                    Value::Real(0.1)
                ],
            ]
        );

        conn1
            .execute("UPDATE b SET y = x'deadbeef' WHERE x = 3", ())
            .await
            .unwrap();

        while let Some(change) = changes.next().await.unwrap() {
            conn2.replay_change(&mut ctx, change).await.unwrap();
        }

        assert_eq!(
            fetch_rows(&conn2).await,
            vec![
                vec![Value::Integer(1), Value::Text("hello".to_string())],
                vec![Value::Integer(2), Value::Text("turso".to_string())],
                vec![
                    Value::Integer(3),
                    Value::Blob(vec![0xde, 0xad, 0xbe, 0xef]),
                    Value::Real(0.1)
                ],
            ]
        );
    }
}
