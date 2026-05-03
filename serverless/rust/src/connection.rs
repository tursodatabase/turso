use std::sync::Arc;

use crate::{Column, Error, Params, Result};
use tokio::sync::Mutex;

use crate::protocol::{
    decode_value, encode_value, ExecuteStreamReq, NamedArg, SequenceStreamReq, StmtBody,
    StreamRequest, StreamResponse, StreamResult,
};
use crate::rows::{Row, Rows};
use crate::session::Session;
use crate::statement::Statement;
use crate::transaction::{Transaction, TransactionBehavior};

/// A connection to a remote Turso database.
///
/// Each connection holds its own HTTP session with baton state.
/// Connections are cheaply cloneable — clones share the same session.
#[derive(Clone)]
pub struct Connection {
    pub(crate) session: Arc<Mutex<Session>>,
}

impl std::fmt::Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection").finish()
    }
}

impl Connection {
    pub(crate) fn new(url: String, auth_token: Option<String>) -> Self {
        Self {
            session: Arc::new(Mutex::new(Session::new(url, auth_token))),
        }
    }

    /// Execute a SQL statement and return the number of rows affected.
    pub async fn execute(&self, sql: impl AsRef<str>, params: impl Into<Params>) -> Result<u64> {
        let result = self.execute_inner(sql.as_ref(), params, false).await?;
        Ok(result.rows_affected)
    }

    /// Query the database and return a result set.
    pub async fn query(&self, sql: impl AsRef<str>, params: impl Into<Params>) -> Result<Rows> {
        let result = self.execute_inner(sql.as_ref(), params, true).await?;
        Ok(Rows::new(result.columns, result.rows))
    }

    /// Execute a batch of SQL statements separated by semicolons.
    pub async fn execute_batch(&self, sql: impl AsRef<str>) -> Result<()> {
        let mut session = self.session.lock().await;
        let response = session
            .execute_pipeline(vec![StreamRequest::Sequence(SequenceStreamReq {
                sql: Some(sql.as_ref().to_string()),
                sql_id: None,
            })])
            .await?;

        if let Some(StreamResult::Error { error }) = response.results.first() {
            return Err(Error::Error(error.message.clone()));
        }

        Ok(())
    }

    /// Prepare a SQL statement for later execution.
    pub async fn prepare(&self, sql: impl AsRef<str>) -> Result<Statement> {
        // For remote, describe the statement to get column metadata
        let mut session = self.session.lock().await;
        let response = session
            .execute_pipeline(vec![StreamRequest::Describe(
                turso_remote_protocol::DescribeStreamReq {
                    sql: Some(sql.as_ref().to_string()),
                    sql_id: None,
                },
            )])
            .await?;

        if let Some(result) = response.results.first() {
            match result {
                StreamResult::Error { error } => {
                    return Err(Error::Error(error.message.clone()));
                }
                StreamResult::Ok {
                    response: StreamResponse::Describe(desc_resp),
                } => {
                    let columns: Vec<Column> = desc_resp
                        .result
                        .cols
                        .iter()
                        .map(|c| {
                            Column::new(c.name.clone().unwrap_or_default(), c.decltype.clone())
                        })
                        .collect();

                    return Ok(Statement::new(
                        self.clone(),
                        sql.as_ref().to_string(),
                        columns,
                    ));
                }
                _ => {}
            }
        }

        // Fallback: create statement without column metadata (will be filled on first query)
        Ok(Statement::new(
            self.clone(),
            sql.as_ref().to_string(),
            vec![],
        ))
    }

    /// Begin a new transaction with the default behavior (DEFERRED).
    pub async fn transaction(&mut self) -> Result<Transaction<'_>> {
        Transaction::new(self, TransactionBehavior::Deferred).await
    }

    /// Begin a new transaction with the specified behavior.
    pub async fn transaction_with_behavior(
        &mut self,
        behavior: TransactionBehavior,
    ) -> Result<Transaction<'_>> {
        Transaction::new(self, behavior).await
    }

    /// Begin a new transaction without requiring `&mut`.
    pub async fn unchecked_transaction(&self) -> Result<Transaction<'_>> {
        Transaction::new_unchecked(self, TransactionBehavior::Deferred).await
    }

    /// Set the default transaction behavior.
    pub fn set_transaction_behavior(&mut self, _behavior: TransactionBehavior) {
        // Remote connections don't cache transaction behavior — the behavior
        // is passed directly in transaction()/transaction_with_behavior().
    }

    /// Check if the connection is in autocommit mode.
    ///
    /// Remote connections are always in autocommit mode outside of an explicit
    /// transaction (the server handles this).
    pub fn is_autocommit(&self) -> Result<bool> {
        // Without a server roundtrip, we can't know for sure.
        // But outside of an explicit BEGIN/COMMIT, the server auto-commits.
        Ok(true)
    }

    /// Execute a PRAGMA query and call a closure for each result row.
    pub async fn pragma_query<F>(&self, pragma_name: &str, mut f: F) -> Result<()>
    where
        F: FnMut(&Row),
    {
        let sql = format!("PRAGMA {pragma_name}");
        let mut rows = self.query(&sql, ()).await?;
        while let Some(row) = rows.next().await? {
            f(row);
        }
        Ok(())
    }

    /// Set a PRAGMA value and return the result rows.
    pub async fn pragma_update<V: std::fmt::Display>(
        &self,
        pragma_name: &str,
        pragma_value: V,
    ) -> Result<Vec<Row>> {
        let sql = format!("PRAGMA {pragma_name} = {pragma_value}");
        let mut rows = self.query(&sql, ()).await?;
        let mut result = Vec::new();
        while let Some(row) = rows.next().await? {
            result.push(row.clone());
        }
        Ok(result)
    }

    /// Return the last insert rowid from the most recent execute.
    /// For remote connections, this is tracked per-execute, not per-connection.
    pub fn last_insert_rowid(&self) -> i64 {
        // Remote connections don't have a persistent last_insert_rowid.
        // Users should use the return value of execute() or RETURNING clause.
        0
    }

    /// Close the connection session.
    pub async fn close(&self) -> Result<()> {
        let mut session = self.session.lock().await;
        session.close().await
    }

    pub(crate) async fn execute_inner(
        &self,
        sql: &str,
        params: impl Into<Params>,
        want_rows: bool,
    ) -> Result<ExecuteResult> {
        // Detect transaction boundaries and toggle keep_alive.
        let sql_upper = sql.trim().to_uppercase();
        let is_begin = sql_upper.starts_with("BEGIN") || sql_upper.starts_with("SAVEPOINT");
        let is_end = sql_upper.starts_with("COMMIT")
            || sql_upper.starts_with("ROLLBACK")
            || sql_upper.starts_with("END")
            || sql_upper.starts_with("RELEASE");

        let params: Params = params.into();

        let (args, named_args) = match params {
            crate::Params::None => (vec![], vec![]),
            crate::Params::Positional(values) => {
                let args = values.iter().map(encode_value).collect();
                (args, vec![])
            }
            crate::Params::Named(values) => {
                let named = values
                    .iter()
                    .map(|(name, val)| NamedArg {
                        name: name.to_string(),
                        value: encode_value(val),
                    })
                    .collect();
                (vec![], named)
            }
        };

        let stmt = StmtBody {
            sql: Some(sql.to_string()),
            sql_id: None,
            args,
            named_args,
            want_rows: Some(want_rows),
            replication_index: None,
        };

        let mut session = self.session.lock().await;

        if is_begin {
            session.set_keep_alive(true);
        }

        let response = session
            .execute_pipeline(vec![StreamRequest::Execute(ExecuteStreamReq { stmt })])
            .await;

        // On error during end-of-transaction, reset keep_alive.
        if response.is_err() && is_end {
            session.set_keep_alive(false);
        }

        let response = response?;

        // Find the execute result (skip the close result at the end).
        let result = response
            .results
            .first()
            .ok_or_else(|| Error::Error("No result in pipeline response".to_string()))?;

        match result {
            StreamResult::Error { error } => {
                let msg = error.message.clone();
                if is_end {
                    session.set_keep_alive(false);
                }
                Err(Error::Error(msg))
            }
            StreamResult::Ok {
                response: StreamResponse::Execute(exec_resp),
            } => {
                let exec_result = &exec_resp.result;

                let columns: Vec<Column> = exec_result
                    .cols
                    .iter()
                    .map(|c| Column::new(c.name.clone().unwrap_or_default(), c.decltype.clone()))
                    .collect();

                let rows: Vec<Row> = exec_result
                    .rows
                    .iter()
                    .map(|row| {
                        let decoded: Vec<crate::Value> =
                            row.values.iter().map(decode_value).collect();
                        Row::new(decoded)
                    })
                    .collect();

                let last_insert_rowid = exec_result.last_insert_rowid;

                if is_end {
                    session.set_keep_alive(false);
                }

                Ok(ExecuteResult {
                    columns,
                    rows,
                    rows_affected: exec_result.affected_row_count,
                    last_insert_rowid,
                })
            }
            _ => Err(Error::Error(
                "Unexpected result in pipeline response".to_string(),
            )),
        }
    }
}

pub(crate) struct ExecuteResult {
    pub columns: Vec<Column>,
    pub rows: Vec<Row>,
    pub rows_affected: u64,
    #[allow(dead_code)]
    pub last_insert_rowid: Option<i64>,
}
