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
pub mod transaction;
pub mod value;

use futures_util::StreamExt;
use hyper::body::Bytes;
use hyper::client::conn;
use serde::{Deserialize, Serialize};
use tokio::fs::{self, copy, remove_file, rename, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::OwnedRwLockReadGuard;
use transaction::TransactionBehavior;
use turso_core::types::{DatabaseChange, DatabaseChangeType, ImmutableRecord, RecordCursor};
pub use value::Value;

pub use params::params_from_iter;
use zerocopy::big_endian;

use crate::params::*;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::i64;
use std::num::NonZero;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU32;
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard};

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
    #[error("Turso pull error: checkpoint required: `{0:?}`")]
    PullCheckpointNeeded(DbSyncStatus),
    #[error("Turso push error: wal conflict detected: `{0:?}`")]
    PushConflict(DbSyncStatus),
    #[error("Turso push error: inconsitent state on remote: `{0:?}`")]
    PushInconsistent(DbSyncStatus),
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
            transaction_behavior: TransactionBehavior::Deferred,
        };
        Ok(connection)
    }
}

/// A database connection.
pub struct Connection {
    inner: Arc<Mutex<Arc<turso_core::Connection>>>,
    transaction_behavior: TransactionBehavior,
}

impl Clone for Connection {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            transaction_behavior: self.transaction_behavior,
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
    pub fn set_readonly(&self, readonly: bool) -> Result<()> {
        let conn = self
            .inner
            .lock()
            .map_err(|e| Error::MutexError(e.to_string()))?;
        // conn.is_readonly(readonly);
        Ok(())
    }
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
                "SELECT * FROM {} WHERE change_id > ? LIMIT {}",
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

    /// Flush dirty pages to disk.
    /// This will write the dirty pages to the WAL.
    pub fn cacheflush(&self) -> Result<()> {
        let conn = self
            .inner
            .lock()
            .map_err(|e| Error::MutexError(e.to_string()))?;
        conn.cacheflush()?;
        Ok(())
    }

    pub fn is_autocommit(&self) -> Result<bool> {
        let conn = self
            .inner
            .lock()
            .map_err(|e| Error::MutexError(e.to_string()))?;

        Ok(conn.get_auto_commit())
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
                    let changes = stmt.n_change();
                    assert!(changes >= 0);
                    return Ok(changes as u64);
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

trait SyncServer {
    async fn db_info(&self) -> Result<DbSyncInfo>;
    async fn db_export(
        &self,
        generation_id: usize,
    ) -> Result<impl futures_core::Stream<Item = reqwest::Result<Bytes>>>;
    async fn wal_pull(
        &self,
        generation_id: usize,
        start_frame: usize,
        end_frame: usize,
    ) -> Result<impl futures_core::Stream<Item = reqwest::Result<Bytes>>>;
    async fn wal_push(
        &self,
        generation_id: usize,
        start_frame: usize,
        end_frame: usize,
        frames: Vec<u8>,
    ) -> Result<DbSyncStatus>;
}

struct TursoSyncServer {
    client: reqwest::Client,
    sync_url: String,
    auth_token: Option<String>,
}

impl TursoSyncServer {
    pub fn new(sync_url: String, auth_token: Option<String>) -> Result<Self> {
        let client = reqwest::ClientBuilder::new()
            .build()
            .map_err(|e| Error::InternalError(format!("unable to build client: {}", e)))?;
        Ok(Self {
            client,
            sync_url,
            auth_token,
        })
    }
}

#[derive(Debug, serde::Deserialize)]
pub struct DbSyncInfo {
    pub current_generation: usize,
}

#[derive(Debug, serde::Deserialize)]
pub struct DbSyncStatus {
    pub status: String,
    pub generation: usize,
    pub max_frame_no: usize,
}

impl SyncServer for TursoSyncServer {
    async fn db_info(&self) -> Result<DbSyncInfo> {
        let uri = format!("{}/info", self.sync_url);
        let mut request = self.client.get(uri);
        if let Some(auth_token) = &self.auth_token {
            request = request.bearer_auth(auth_token);
        }
        let request = request
            .build()
            .map_err(|e| Error::InternalError(format!("unable to get sync info: {}", e)))?;

        let response =
            self.client.execute(request).await.map_err(|e| {
                Error::InternalError(format!("unable to send get info request: {}", e))
            })?;

        if !response.status().is_success() {
            let error = response.text().await.unwrap_or(String::new());
            return Err(Error::InternalError(format!(
                "get info response is not successful: {}",
                error,
            )));
        }

        let body = response.bytes().await.map_err(|e| {
            Error::InternalError(format!("unable to read get info response body: {}", e))
        })?;

        let info = serde_json::from_slice(&body).map_err(|e| {
            Error::InternalError(format!("unable to parse get info response: {}", e))
        })?;

        Ok(info)
    }

    async fn db_export(
        &self,
        generation_id: usize,
    ) -> Result<impl futures_core::Stream<Item = reqwest::Result<Bytes>>> {
        let uri = format!("{}/export/{}", self.sync_url, generation_id);
        let mut request = self.client.get(uri);
        if let Some(auth_token) = &self.auth_token {
            request = request.bearer_auth(auth_token);
        }
        let request = request
            .build()
            .map_err(|e| Error::InternalError(format!("unable to bootstrap db: {}", e)))?;

        let response = self.client.execute(request).await.map_err(|e| {
            Error::InternalError(format!("unable to send bootstrap request: {}", e))
        })?;

        if !response.status().is_success() {
            let error = response.text().await.unwrap_or(String::new());
            return Err(Error::InternalError(format!(
                "bootstrap response is not successful: {}",
                error,
            )));
        }
        Ok(response.bytes_stream())
    }

    async fn wal_pull(
        &self,
        generation_id: usize,
        start_frame: usize,
        end_frame: usize,
    ) -> Result<impl futures_core::Stream<Item = reqwest::Result<Bytes>>> {
        let uri = format!(
            "{}/sync/{}/{}/{}",
            self.sync_url, generation_id, start_frame, end_frame
        );
        let mut request = self.client.get(uri);
        if let Some(auth_token) = &self.auth_token {
            request = request.bearer_auth(auth_token);
        }
        let request = request
            .build()
            .map_err(|e| Error::InternalError(format!("unable to pull wall: {}", e)))?;

        let response =
            self.client.execute(request).await.map_err(|e| {
                Error::InternalError(format!("unable to send pull wal request: {}", e))
            })?;

        if !response.status().is_success() {
            let body = response.bytes().await.unwrap_or(Bytes::new());
            let status: DbSyncStatus = serde_json::from_slice(&body).map_err(|e| {
                Error::InternalError(format!(
                    "unable to parse wal pull status: {:?} {}",
                    &body, e
                ))
            })?;
            if status.status == "checkpoint_needed" {
                return Err(Error::PullCheckpointNeeded(status));
            }
            return Err(Error::InternalError(format!(
                "wal pull response is not successful: {:?}",
                status,
            )));
        }
        Ok(response.bytes_stream())
    }

    async fn wal_push(
        &self,
        generation_id: usize,
        start_frame: usize,
        end_frame: usize,
        frames: Vec<u8>,
    ) -> Result<DbSyncStatus> {
        let uri = format!(
            "{}/sync/{}/{}/{}",
            self.sync_url, generation_id, start_frame, end_frame
        );
        let mut request = self.client.post(uri);
        if let Some(auth_token) = &self.auth_token {
            request = request.bearer_auth(auth_token);
        }
        let request = request
            .body(frames)
            .build()
            .map_err(|e| Error::InternalError(format!("unable to push wal: {}", e)))?;

        let response =
            self.client.execute(request).await.map_err(|e| {
                Error::InternalError(format!("unable to send push wal request: {}", e))
            })?;

        if !response.status().is_success() {
            let error = response.text().await.unwrap_or(String::new());
            return Err(Error::InternalError(format!(
                "wal push response is not successful: {}",
                error,
            )));
        }
        let body = response.bytes().await.unwrap_or(Bytes::new());
        let status: DbSyncStatus = serde_json::from_slice(&body)
            .map_err(|e| Error::InternalError(format!("unable to parse push wal status: {}", e)))?;
        if status.status == "ok" {
            Ok(status)
        } else if status.status == "conflict" {
            Err(Error::PushConflict(status))
        } else if status.status == "push_needed" {
            Err(Error::PushInconsistent(status))
        } else {
            Err(Error::InternalError(format!(
                "unexpected sync status: {:?}",
                status
            )))
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct SyncedDatabaseMetadata {
    remote_copy_generation: usize,
    remote_copy_frame_no: usize,
    local_change_id: Option<i64>,
    primary_db: DatabaseTandemSelection,
}

impl SyncedDatabaseMetadata {
    pub async fn read_from(path: &Path) -> Result<Option<Self>> {
        let exists = Path::new(&path)
            .try_exists()
            .map_err(|e| Error::InternalError(format!("metadata file exists error: {}", e)))?;
        if !exists {
            return Ok(None);
        }
        let contents = tokio::fs::read(&path)
            .await
            .map_err(|e| Error::InternalError(format!("metadata read error: {}", e)))?;
        let metadata = serde_json::from_slice::<SyncedDatabaseMetadata>(&contents[..])
            .map_err(|e| Error::InternalError(format!("metadata parse error: {}", e)))?;
        Ok(Some(metadata))
    }
    pub async fn write_to(&self, path: &Path) -> Result<()> {
        let directory = path.parent().ok_or_else(|| {
            Error::InternalError(format!("unable to get parent of the provided path"))
        })?;

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| Error::InternalError(format!("failed to get current time: {}", e)))?;
        let temp_name = format!(".tmp.{}", timestamp.as_micros());
        let temp_path = directory.join(temp_name);

        let mut temp_file = File::create(&temp_path)
            .await
            .map_err(|e| Error::InternalError(format!("temp file create error: {}", e)))?;

        let data = serde_json::to_string(self)
            .map_err(|e| Error::InternalError(format!("failed to serialize metadata: {}", e)))?;

        temp_file
            .write_all(data.as_bytes())
            .await
            .map_err(|e| Error::InternalError(format!("failed to write metadata: {}", e)))?;

        temp_file
            .sync_all()
            .await
            .map_err(|e| Error::InternalError(format!("failed to sync metadata: {}", e)))?;

        drop(temp_file);

        rename(&temp_path, &path)
            .await
            .map_err(|e| Error::InternalError(format!("failed to move metadata: {}", e)))?;
        Ok(())
    }
}

pub struct SyncedDatabase {
    sync_server: TursoSyncServer,
    local_path: PathBuf,
    remote_path: PathBuf,
    meta_path: PathBuf,
    meta: Option<SyncedDatabaseMetadata>,
    database: Arc<RwLock<DatabaseTandem>>,
}

pub struct DatabaseTandem {
    local: Option<Database>,
    remote: Option<Database>,
    primary: DatabaseTandemSelection,
    connections: Vec<Connection>,
}

impl DatabaseTandem {
    pub fn database(&self) -> &Database {
        match self.primary {
            DatabaseTandemSelection::Local => self.local.as_ref().unwrap(),
            DatabaseTandemSelection::Remote => self.remote.as_ref().unwrap(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
enum DatabaseTandemSelection {
    Local,
    Remote,
}

pub struct SyncedConnection {
    database: Arc<RwLock<DatabaseTandem>>,
    connection_idx: usize,
}

pub struct SyncedStatement {
    database: Arc<RwLock<DatabaseTandem>>,
    guard: OwnedRwLockReadGuard<DatabaseTandem>,
    statement: Statement,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SyncStrategy {
    Local,
    Remote,
    Full,
}

impl SyncedDatabase {
    pub async fn new(path: PathBuf, sync_url: String, auth_token: Option<String>) -> Result<Self> {
        let path_str = path.to_str().unwrap();
        let local_path = PathBuf::try_from(path_str.to_string()).unwrap();
        let remote_path = PathBuf::try_from(format!("{}-remote", path_str)).unwrap();
        let meta_path = PathBuf::try_from(format!("{}-info", path_str)).unwrap();
        let sync_server = TursoSyncServer::new(sync_url, auth_token)?;
        let state = SyncedDatabaseMetadata::read_from(&meta_path).await?;
        Ok(Self {
            sync_server,
            local_path,
            remote_path,
            meta_path,
            meta: state,
            database: Arc::new(RwLock::new(DatabaseTandem {
                local: None,
                remote: None,
                primary: DatabaseTandemSelection::Local,
                connections: vec![],
            })),
        })
    }
    pub async fn init(&mut self) -> Result<()> {
        let local_exists = self
            .local_path
            .try_exists()
            .map_err(|e| Error::InternalError(format!("local_exists failed: {}", e)))?;
        let remote_exists = self
            .remote_path
            .try_exists()
            .map_err(|e| Error::InternalError(format!("local_exists failed: {}", e)))?;
        if let Some(meta) = &self.meta {
            if !local_exists || !remote_exists {
                return Err(Error::InternalError(format!(
                    "local or remote files doesn't exists, but metadata is"
                )));
            }
            let mut database = self.database.write().unwrap();
            match meta.primary_db {
                DatabaseTandemSelection::Local => {
                    database.local = Some(
                        Builder::new_local(self.local_path.to_str().unwrap())
                            .build()
                            .await?,
                    );
                }
                DatabaseTandemSelection::Remote => {
                    database.remote = Some(
                        Builder::new_local(self.remote_path.to_str().unwrap())
                            .build()
                            .await?,
                    );
                }
            }
            return Ok(());
        }

        if local_exists {
            remove_file(&self.local_path)
                .await
                .map_err(|e| Error::InternalError(format!("failed to remove local file: {}", e)))?;
        }
        if remote_exists {
            remove_file(&self.remote_path).await.map_err(|e| {
                Error::InternalError(format!("failed to remove remote file: {}", e))
            })?;
        }
        let info = self.sync_server.db_info().await?;

        let mut remote_file = File::create_new(&self.remote_path)
            .await
            .map_err(|e| Error::InternalError(format!("failed to create remote file: {}", e)))?;
        {
            let mut bootstrap = self.sync_server.db_export(info.current_generation).await?;
            while let Some(chunk) = bootstrap.next().await {
                let chunk = chunk.map_err(|e| {
                    Error::InternalError(format!("failed to get bootstrap chunk: {}", e))
                })?;
                println!("bootstrap: fetched chunk of size {}", chunk.len());
                remote_file
                    .write_all(&chunk)
                    .await
                    .map_err(|e| Error::InternalError(format!("failed to write to file: {}", e)))?;
            }
        }
        fs::copy(&self.remote_path, &self.local_path)
            .await
            .map_err(|e| Error::InternalError(format!("failed to copy remote to local: {}", e)))?;
        self.meta = Some(SyncedDatabaseMetadata {
            remote_copy_generation: info.current_generation,
            remote_copy_frame_no: 0,
            primary_db: DatabaseTandemSelection::Local,
            local_change_id: None,
        });
        self.sync_remote().await
    }
    pub async fn connect(&mut self) -> Result<SyncedConnection> {
        self.init().await?;

        let mut database = self.database.write().unwrap();
        let idx = database.connections.len();
        let connection = database.database().connect()?;
        if database.primary == DatabaseTandemSelection::Local {
            connection
                .execute("PRAGMA unstable_capture_data_changes_conn('full')", ())
                .await
                .unwrap();
        }
        database.connections.push(connection);

        Ok(SyncedConnection {
            database: self.database.clone(),
            connection_idx: idx,
        })
    }
    pub async fn sync(&mut self, strategy: SyncStrategy) -> Result<()> {
        match strategy {
            SyncStrategy::Remote => self.sync_remote().await,
            SyncStrategy::Local | SyncStrategy::Full => Err(Error::InternalError(format!(
                "{:?} strategy is not supported",
                strategy
            ))),
        }
    }

    async fn sync_remote(&mut self) -> Result<()> {
        let meta = self.meta.as_mut().unwrap();
        let remote_wal_path_str = format!("{}-wal", self.remote_path.to_str().unwrap());
        let remote_wal_path = PathBuf::from(remote_wal_path_str);
        let local_wal_path_str = format!("{}-wal", self.local_path.to_str().unwrap());
        let local_wal_path = PathBuf::from(local_wal_path_str);
        {
            println!("pull wal changes to the remote copy file");
            let mut frame_no = meta.remote_copy_frame_no;
            let mut wal_file = if remote_wal_path.exists() {
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&remote_wal_path)
                    .await
                    .map_err(|e| {
                        Error::InternalError(format!("failed to open remote file: {}", e))
                    })?
            } else {
                let mut f = File::create_new(&remote_wal_path).await.map_err(|e| {
                    Error::InternalError(format!("failed to open remote file: {}", e))
                })?;
                f.write_all(&[
                    0x37, 0x7f, 0x06, 0x83, 0x00, 0x2d, 0xe2, 0x18, 0x00, 0x00, 0x10, 0x00,
                ])
                .await
                .map_err(|e| Error::InternalError(format!("failed to open remote file: {}", e)))?;
                f
            };
            const WAL_HEADER_SIZE: usize = 32;
            const WAL_FRAME_HEADER_SIZE: usize = 24;
            const PAGE_SIZE: usize = 4096;
            const WAL_FRAME_SIZE: usize = WAL_FRAME_HEADER_SIZE + PAGE_SIZE;
            wal_file
                .set_len((WAL_HEADER_SIZE + frame_no * WAL_FRAME_SIZE) as u64)
                .await
                .unwrap();
            wal_file
                .seek(std::io::SeekFrom::Start(
                    (WAL_HEADER_SIZE + frame_no * WAL_FRAME_SIZE) as u64,
                ))
                .await
                .map_err(|e| {
                    Error::InternalError(format!("failed to seek in remote file: {}", e))
                })?;
            loop {
                let frames = self
                    .sync_server
                    .wal_pull(meta.remote_copy_generation, frame_no + 1, frame_no + 100)
                    .await;
                let mut frames = match frames {
                    Ok(frames) => frames,
                    Err(Error::PullCheckpointNeeded(status))
                        if status.generation == meta.remote_copy_generation
                            && status.max_frame_no == frame_no =>
                    {
                        break;
                    }
                    Err(e) => {
                        return Err(Error::InternalError(format!("unexpected error: {:?}", e)))
                    }
                };
                let mut written = 0;
                while let Some(chunk) = frames.next().await {
                    let chunk = chunk.map_err(|e| {
                        Error::InternalError(format!("failed to pull wal frame: {}", e))
                    })?;
                    println!(
                        "wal pull {}/{}/{}: fetched chunk of size {}",
                        meta.remote_copy_generation,
                        frame_no + 1,
                        frame_no + 100,
                        chunk.len()
                    );
                    wal_file.write_all(&chunk).await.map_err(|e| {
                        Error::InternalError(format!("failed to write wal portion: {}", e))
                    })?;
                    written += chunk.len();
                }
                println!("frame_no: {}, written: {}", frame_no, written);
                frame_no += written / WAL_FRAME_SIZE;
            }

            let wal_file_size = wal_file.metadata().await.unwrap().len();
            let mut buf = [0u8; 8];
            let (mut s0, mut s1) = (0u32, 0u32);
            for i in (0..wal_file_size as usize).step_by(8) {
                if i == 24
                    || (i >= WAL_HEADER_SIZE
                        && matches!((i - WAL_HEADER_SIZE) % WAL_FRAME_SIZE, 8 | 16))
                {
                    continue;
                }
                wal_file
                    .seek(std::io::SeekFrom::Start(i as u64))
                    .await
                    .unwrap();
                wal_file.read_exact(&mut buf[..]).await.unwrap();
                s0 = s0.wrapping_add(
                    big_endian::U32::from_bytes(buf[0..4].try_into().unwrap())
                        .get()
                        .wrapping_add(s1),
                );
                s1 = s1.wrapping_add(
                    big_endian::U32::from_bytes(buf[4..8].try_into().unwrap())
                        .get()
                        .wrapping_add(s0),
                );
                if i == 16
                    || (i >= WAL_HEADER_SIZE
                        && (i - WAL_HEADER_SIZE) % WAL_FRAME_SIZE + 8 == WAL_FRAME_SIZE)
                {
                    let offset = if i == 16 { 24 } else { i - PAGE_SIZE };
                    println!("checksum at {}: {} {}", offset, s0, s1);
                    let mut checksum = [0u8; 8];
                    checksum[0..4].copy_from_slice(&big_endian::U32::new(s0).to_bytes());
                    checksum[4..8].copy_from_slice(&big_endian::U32::new(s1).to_bytes());
                    wal_file
                        .seek(std::io::SeekFrom::Start(offset as u64))
                        .await
                        .unwrap();
                    wal_file.write_all(&checksum).await.unwrap();
                }
            }

            meta.remote_copy_frame_no = frame_no;
            meta.write_to(&self.meta_path).await?;
        }

        {
            println!("block writes to the local connection");
            let database = self.database.write().unwrap();
            for connection in &database.connections {
                connection.set_readonly(true)?;
            }
        }
        {
            println!("transfer changes from local to remote copy");
            let local = Builder::new_local(self.local_path.to_str().unwrap())
                .build()
                .await?;
            let remote = Builder::new_local(self.remote_path.to_str().unwrap())
                .build()
                .await?;

            let local_conn = local.connect()?;
            local_conn
                .execute("PRAGMA unstable_capture_data_changes_conn('full')", ())
                .await
                .unwrap();

            let remote_conn = remote.connect()?;
            remote_conn
                .execute("PRAGMA unstable_capture_data_changes_conn('full')", ())
                .await
                .unwrap();
            let mut replay_ctx = DatabaseReplayContext::new();
            let mut changes = local_conn
                .changes_iterator("turso_cdc", meta.local_change_id, 10)
                .await?;
            while let Some(change) = changes.next().await? {
                println!("replay change: {}", change.id);
                remote_conn.replay_change(&mut replay_ctx, change).await?;
            }
        }

        {
            println!("switch primary to remote copy");
            let mut database = self.database.write().unwrap();
            let remote = Builder::new_local(self.remote_path.to_str().unwrap())
                .build()
                .await?;
            let mut connections = vec![];
            for _ in 0..database.connections.len() {
                let connection = remote.connect()?;
                connection.set_readonly(true)?;
                connections.push(connection);
            }
            database.connections = connections;
            database.primary = DatabaseTandemSelection::Remote;
            database.remote = Some(remote);
            database.local = None;
        }

        {
            println!("transfer remote copy wal to local wal");
            copy(remote_wal_path, local_wal_path).await.unwrap();
        }

        {
            println!("switch primary to local copy");
            let mut database = self.database.write().unwrap();
            let local = Builder::new_local(self.local_path.to_str().unwrap())
                .build()
                .await?;
            let mut connections = vec![];
            for _ in 0..database.connections.len() {
                let connection = local.connect()?;
                connection
                    .execute("PRAGMA unstable_capture_data_changes_conn('full')", ())
                    .await
                    .unwrap();
                connections.push(connection);
            }
            database.primary = DatabaseTandemSelection::Local;
            database.connections = connections;
            database.local = Some(local);
            database.remote = None;
        }

        Ok(())
    }
}

impl SyncedConnection {
    /// Query the database with SQL.
    pub async fn query(&self, sql: &str, params: impl IntoParams) -> Result<Rows> {
        let database = self.database.read().unwrap();
        let connection = &database.connections[self.connection_idx];
        connection.query(sql, params).await
    }

    /// Execute SQL statement on the database.
    pub async fn execute(&self, sql: &str, params: impl IntoParams) -> Result<u64> {
        let database = self.database.read().unwrap();
        let connection = &database.connections[self.connection_idx];
        connection.execute(sql, params).await
    }
}

// impl SyncedStatement {
//     /// Query the database with this prepared statement.
//     pub async fn query(&mut self, params: impl IntoParams) -> Result<Rows> {}

//     /// Execute this prepared statement.
//     pub async fn execute(&mut self, params: impl IntoParams) -> Result<u64> {}

//     /// Returns columns of the result of this prepared statement.
//     pub fn columns(&self) -> Vec<Column> {}

//     /// Reset internal statement state after previous execution so it can be reused again
//     pub fn reset(&self) {}
// }

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

    use std::io;
    use std::io::prelude::*;

    #[tokio::test]
    pub async fn test_sync_server() {
        let mut db = SyncedDatabase::new(
            "local.db".into(),
            "https://flowing-cat-sivukhin.aws-eu-north-1.turso.io".into(),
            Some(todo!()),
        )
        .await
        .unwrap();
        db.init().await.unwrap();

        let conn = db.connect().await.unwrap();

        eprintln!("ready to demo!");
        eprint!("$> ");

        let stdin = io::stdin();
        for line in stdin.lock().lines() {
            let line = line.unwrap();
            if line == ".sync" {
                db.sync_remote().await.unwrap();
                eprint!("$> ");
            } else {
                let mut rows = conn.query(&line, ()).await.unwrap();
                while let Some(row) = rows.next().await.unwrap() {
                    for value in row.values {
                        match value {
                            turso_core::Value::Null => eprint!("NULL "),
                            turso_core::Value::Integer(x) => eprint!("{} ", x),
                            turso_core::Value::Float(x) => eprint!("{} ", x),
                            turso_core::Value::Text(x) => eprint!("'{}' ", x.as_str()),
                            turso_core::Value::Blob(x) => eprint!("x'{:?}' ", x),
                        }
                    }
                    eprintln!("");
                }
                eprint!("$> ");
            }
        }
    }
}
