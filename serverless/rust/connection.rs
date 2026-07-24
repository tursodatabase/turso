use std::collections::HashMap;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

use tokio::sync::Mutex;

use crate::params::{IntoParams, Params};
use crate::protocol::{encode_value, NamedArg, Stmt, StreamRequest, StreamResponse, StreamResult};
use crate::rows::{Row, Rows};
use crate::session::{Session, SharedState};
use crate::statement::Statement;
use crate::transaction::{DropBehavior, Transaction, TransactionBehavior};
use crate::{Column, Error, Result};

/// A connection to a remote database.
///
/// Each connection maps to one server-side stream and holds its own
/// transaction state. Connections are cheaply cloneable; clones share the
/// same stream. Statements on one connection execute one at a time, in
/// order (section 4.4 of the protocol specification).
#[derive(Clone)]
pub struct Connection {
    session: Arc<Mutex<Session>>,
    shared: Arc<SharedState>,
    transaction_behavior: TransactionBehavior,
    /// If a [`Transaction`] was dropped without being finished, this holds
    /// the [`DropBehavior`] to apply, and the corresponding action runs
    /// before the connection's next statement. The work is deferred
    /// because `Drop` cannot perform an HTTP request. [`DropBehavior::Ignore`]
    /// means there is nothing to do. Shared across clones because clones
    /// share the stream that carries the transaction.
    dangling_tx: Arc<AtomicU8>,
    /// Column metadata cached by [`prepare_cached`](Connection::prepare_cached),
    /// keyed by SQL text.
    cached_statements: Arc<std::sync::Mutex<HashMap<String, Vec<Column>>>>,
}

impl std::fmt::Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection").finish()
    }
}

fn build_stmt(sql: &str, params: Params, want_rows: bool) -> Result<Stmt> {
    let mut stmt = Stmt::new(sql, want_rows);
    match params {
        Params::None => {}
        Params::Positional(values) => {
            stmt.args = values
                .iter()
                .map(encode_value)
                .collect::<Result<Vec<_>>>()?;
        }
        Params::Named(values) => {
            stmt.named_args = values
                .iter()
                .map(|(name, value)| {
                    Ok(NamedArg {
                        name: name.to_string(),
                        value: encode_value(value)?,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
        }
    }
    Ok(stmt)
}

impl Connection {
    pub(crate) fn new(url: &str, auth_token: Option<String>) -> Self {
        let (session, shared) = Session::new(url, auth_token);
        Self {
            session: Arc::new(Mutex::new(session)),
            shared,
            transaction_behavior: TransactionBehavior::Deferred,
            dangling_tx: Arc::new(AtomicU8::new(DropBehavior::Ignore.into())),
            cached_statements: Arc::new(std::sync::Mutex::new(HashMap::new())),
        }
    }

    /// Query the database and return the result rows.
    pub async fn query(&self, sql: impl AsRef<str>, params: impl IntoParams) -> Result<Rows> {
        let stmt = build_stmt(sql.as_ref(), params.into_params()?, true)?;
        let mut session = self.session.lock().await;
        self.maybe_handle_dangling_tx(&mut session).await?;
        let output = session.execute_cursor_stmt(stmt).await?;
        Ok(Rows::new(output.columns, output.rows))
    }

    /// Execute a SQL statement and return the number of rows affected.
    pub async fn execute(&self, sql: impl AsRef<str>, params: impl IntoParams) -> Result<u64> {
        let stmt = build_stmt(sql.as_ref(), params.into_params()?, false)?;
        let mut session = self.session.lock().await;
        self.maybe_handle_dangling_tx(&mut session).await?;
        let results = session
            .pipeline(vec![StreamRequest::Execute { stmt }], true)
            .await?;
        match results.into_iter().next() {
            Some(StreamResult::Ok {
                response: StreamResponse::Execute { result },
            }) => {
                if let Some(rowid) = result.last_insert_rowid {
                    let rowid = rowid.parse::<i64>().map_err(|e| {
                        Error::Error(format!("invalid rowid in server response: {e}"))
                    })?;
                    session
                        .shared
                        .last_insert_rowid
                        .store(rowid, Ordering::Relaxed);
                }
                Ok(result.affected_row_count)
            }
            Some(StreamResult::Error { error }) => Err(error.into()),
            _ => Err(Error::Http(
                "missing execute result in pipeline response".to_string(),
            )),
        }
    }

    /// Execute a sequence of SQL statements separated by semicolons.
    /// Execution stops at the first statement that fails.
    pub async fn execute_batch(&self, sql: impl AsRef<str>) -> Result<()> {
        let mut session = self.session.lock().await;
        self.maybe_handle_dangling_tx(&mut session).await?;
        let results = session
            .pipeline(
                vec![StreamRequest::Sequence {
                    sql: sql.as_ref().to_string(),
                }],
                true,
            )
            .await?;
        match results.into_iter().next() {
            Some(StreamResult::Ok { .. }) => Ok(()),
            Some(StreamResult::Error { error }) => Err(error.into()),
            None => Err(Error::Http(
                "missing sequence result in pipeline response".to_string(),
            )),
        }
    }

    /// Prepare a SQL statement.
    ///
    /// The statement is described on the server (section 6.4) to validate
    /// it and fetch its column metadata.
    pub async fn prepare(&self, sql: impl AsRef<str>) -> Result<Statement> {
        let sql = sql.as_ref();
        let mut session = self.session.lock().await;
        self.maybe_handle_dangling_tx(&mut session).await?;
        let results = session
            .pipeline(
                vec![StreamRequest::Describe {
                    sql: sql.to_string(),
                }],
                true,
            )
            .await?;
        drop(session);
        match results.into_iter().next() {
            Some(StreamResult::Ok {
                response: StreamResponse::Describe { result },
            }) => {
                let columns = result
                    .cols
                    .into_iter()
                    .map(|c| Column {
                        name: c.name.unwrap_or_default(),
                        decl_type: c.decltype,
                    })
                    .collect();
                Ok(Statement::new(self.clone(), sql.to_string(), columns))
            }
            Some(StreamResult::Error { error }) => Err(error.into()),
            _ => Err(Error::Http(
                "missing describe result in pipeline response".to_string(),
            )),
        }
    }

    /// Prepare a SQL statement, reusing column metadata cached on the
    /// connection.
    ///
    /// The remote protocol cannot retain a server-side prepared statement,
    /// so this caches the client-side description (the column metadata
    /// fetched by [`prepare`](Connection::prepare)) keyed by SQL text and
    /// skips the describe round trip on a cache hit. Execution always
    /// sends the SQL text, exactly as with a freshly prepared statement.
    pub async fn prepare_cached(&self, sql: impl AsRef<str>) -> Result<Statement> {
        let sql = sql.as_ref();
        let cached = self.cached_statements.lock().unwrap().get(sql).cloned();
        if let Some(columns) = cached {
            return Ok(Statement::new(self.clone(), sql.to_string(), columns));
        }
        let stmt = self.prepare(sql).await?;
        self.cached_statements
            .lock()
            .unwrap()
            .insert(sql.to_string(), stmt.columns());
        Ok(stmt)
    }

    /// Begin a new transaction with the connection's default behavior
    /// (DEFERRED unless changed with
    /// [`set_transaction_behavior`](Connection::set_transaction_behavior)).
    pub async fn transaction(&mut self) -> Result<Transaction<'_>> {
        self.transaction_with_behavior(self.transaction_behavior)
            .await
    }

    /// Begin a new transaction with the specified behavior.
    pub async fn transaction_with_behavior(
        &mut self,
        behavior: TransactionBehavior,
    ) -> Result<Transaction<'_>> {
        Transaction::new(self, behavior).await
    }

    /// Begin a new transaction with the connection's default behavior.
    ///
    /// An attempt to open a nested transaction will result in an error.
    /// [`Connection::transaction`] prevents this at compile time by taking
    /// `&mut self`, but `Connection::unchecked_transaction()` may be used
    /// to defer the checking until runtime.
    ///
    /// See [`Connection::transaction`] and [`Transaction::new_unchecked`]
    /// (which can be used if the default transaction behavior is
    /// undesirable).
    pub async fn unchecked_transaction(&self) -> Result<Transaction<'_>> {
        Transaction::new_unchecked(self, self.transaction_behavior).await
    }

    /// Set the default transaction behavior for the connection.
    ///
    /// This will only apply to transactions initiated by
    /// [`transaction`](Connection::transaction) or
    /// [`unchecked_transaction`](Connection::unchecked_transaction).
    pub fn set_transaction_behavior(&mut self, behavior: TransactionBehavior) {
        self.transaction_behavior = behavior;
    }

    /// Execute a PRAGMA query and call a closure for each result row.
    ///
    /// An error returned by the closure stops the iteration and propagates
    /// to the caller.
    pub async fn pragma_query<F>(&self, pragma_name: &str, mut f: F) -> Result<()>
    where
        F: FnMut(&Row) -> Result<()>,
    {
        let mut rows = self.query(format!("PRAGMA {pragma_name}"), ()).await?;
        while let Some(row) = rows.next().await? {
            f(&row)?;
        }
        Ok(())
    }

    /// Set a PRAGMA value and return the result rows.
    pub async fn pragma_update<V: std::fmt::Display>(
        &self,
        pragma_name: &str,
        pragma_value: V,
    ) -> Result<Vec<Row>> {
        let mut rows = self
            .query(format!("PRAGMA {pragma_name} = {pragma_value}"), ())
            .await?;
        let mut result = Vec::new();
        while let Some(row) = rows.next().await? {
            result.push(row);
        }
        Ok(result)
    }

    /// Returns the rowid of the most recent successful INSERT on this
    /// connection.
    pub fn last_insert_rowid(&self) -> i64 {
        self.shared.last_insert_rowid.load(Ordering::Relaxed)
    }

    /// Returns whether the connection is in autocommit mode, that is,
    /// whether no explicit transaction is open.
    ///
    /// The value reflects the server's answer as of the most recently
    /// completed statement; it does not perform a server round trip.
    pub fn is_autocommit(&self) -> Result<bool> {
        Ok(self.shared.autocommit.load(Ordering::Relaxed))
    }

    /// Close the connection, releasing the server-side stream.
    ///
    /// Any open transaction is rolled back. Closing is optional but
    /// recommended: an unclosed stream holds server-side resources until
    /// it expires.
    pub async fn close(&self) -> Result<()> {
        let mut session = self.session.lock().await;
        session.close().await;
        Ok(())
    }

    pub(crate) fn set_dangling_tx(&self, behavior: DropBehavior) {
        self.dangling_tx.store(behavior.into(), Ordering::SeqCst);
    }

    /// Complete a transaction left open by a dropped [`Transaction`],
    /// applying its [`DropBehavior`]. Runs before the next statement so
    /// the statement does not silently join (or commit as part of) an
    /// abandoned transaction. Rollback errors are ignored: if the stream
    /// is already gone, the server rolled back. Commit errors propagate,
    /// because silently losing a commit would be a durability violation.
    async fn maybe_handle_dangling_tx(&self, session: &mut Session) -> Result<()> {
        let behavior: DropBehavior = self.dangling_tx.load(Ordering::SeqCst).into();
        let sql = match behavior {
            DropBehavior::Ignore => return Ok(()),
            DropBehavior::Panic => panic!("Transaction dropped unexpectedly."),
            DropBehavior::Rollback => "ROLLBACK",
            DropBehavior::Commit => "COMMIT",
        };
        self.set_dangling_tx(DropBehavior::Ignore);
        if self.shared.autocommit.load(Ordering::Relaxed) {
            return Ok(());
        }
        let stmt = Stmt::new(sql, false);
        let results = session
            .pipeline(vec![StreamRequest::Execute { stmt }], true)
            .await;
        if behavior == DropBehavior::Commit {
            match results?.into_iter().next() {
                Some(StreamResult::Ok { .. }) => {}
                Some(StreamResult::Error { error }) => return Err(error.into()),
                None => {
                    return Err(Error::Http(
                        "missing execute result in pipeline response".to_string(),
                    ))
                }
            }
        }
        Ok(())
    }
}
