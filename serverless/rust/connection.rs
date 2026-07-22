use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::sync::Mutex;

use crate::params::{IntoParams, Params};
use crate::protocol::{encode_value, NamedArg, Stmt, StreamRequest, StreamResponse, StreamResult};
use crate::rows::{Row, Rows};
use crate::session::{Session, SharedState};
use crate::statement::Statement;
use crate::transaction::{Transaction, TransactionBehavior};
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
    needs_rollback: Arc<AtomicBool>,
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
            needs_rollback: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Query the database and return the result rows.
    pub async fn query(&self, sql: impl AsRef<str>, params: impl IntoParams) -> Result<Rows> {
        let stmt = build_stmt(sql.as_ref(), params.into_params()?, true)?;
        let mut session = self.session.lock().await;
        self.rollback_if_needed(&mut session).await;
        let output = session.execute_cursor_stmt(stmt).await?;
        Ok(Rows::new(output.columns, output.rows))
    }

    /// Execute a SQL statement and return the number of rows affected.
    pub async fn execute(&self, sql: impl AsRef<str>, params: impl IntoParams) -> Result<u64> {
        let stmt = build_stmt(sql.as_ref(), params.into_params()?, false)?;
        let mut session = self.session.lock().await;
        self.rollback_if_needed(&mut session).await;
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
        self.rollback_if_needed(&mut session).await;
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
        self.rollback_if_needed(&mut session).await;
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

    /// Begin a new transaction with the default behavior (DEFERRED).
    pub async fn transaction(&mut self) -> Result<Transaction<'_>> {
        self.transaction_with_behavior(TransactionBehavior::Deferred)
            .await
    }

    /// Begin a new transaction with the specified behavior.
    pub async fn transaction_with_behavior(
        &mut self,
        behavior: TransactionBehavior,
    ) -> Result<Transaction<'_>> {
        Transaction::new(self, behavior).await
    }

    /// Execute a PRAGMA query and call a closure for each result row.
    pub async fn pragma_query<F>(&self, pragma_name: &str, mut f: F) -> Result<()>
    where
        F: FnMut(&Row),
    {
        let mut rows = self.query(format!("PRAGMA {pragma_name}"), ()).await?;
        while let Some(row) = rows.next().await? {
            f(&row);
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

    pub(crate) fn set_needs_rollback(&self) {
        self.needs_rollback.store(true, Ordering::Relaxed);
    }

    /// Roll back a transaction left open by a dropped [`Transaction`].
    /// Runs before the next statement so the statement does not silently
    /// join (or commit as part of) an abandoned transaction. Errors are
    /// ignored: if the stream is already gone, the server rolled back.
    async fn rollback_if_needed(&self, session: &mut Session) {
        if !self.needs_rollback.swap(false, Ordering::Relaxed) {
            return;
        }
        if !self.shared.autocommit.load(Ordering::Relaxed) {
            let stmt = Stmt::new("ROLLBACK", false);
            let _ = session
                .pipeline(vec![StreamRequest::Execute { stmt }], true)
                .await;
        }
    }
}
