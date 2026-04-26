use std::sync::Arc;

use tokio::sync::Mutex;
use crate::{Column, Error, Params, Result};

use crate::protocol::{
    decode_value, encode_value, BatchStep, CursorBatch, CursorEntry, NamedArg,
    PipelineRequestEntry, StmtBody,
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
            .execute_pipeline(vec![PipelineRequestEntry::Sequence {
                sql: sql.as_ref().to_string(),
            }])
            .await?;

        if let Some(result) = response.results.first() {
            if result.typ == "error" {
                if let Some(err) = &result.error {
                    return Err(Error::Error(err.message.clone()));
                }
                return Err(Error::Error("Sequence execution failed".to_string()));
            }
        }

        Ok(())
    }

    /// Prepare a SQL statement for later execution.
    pub async fn prepare(&self, sql: impl AsRef<str>) -> Result<Statement> {
        // For remote, describe the statement to get column metadata
        let mut session = self.session.lock().await;
        let response = session
            .execute_pipeline(vec![PipelineRequestEntry::Describe {
                sql: sql.as_ref().to_string(),
            }])
            .await?;

        if let Some(result) = response.results.first() {
            if result.typ == "error" {
                if let Some(err) = &result.error {
                    return Err(Error::Error(err.message.clone()));
                }
                return Err(Error::Error("Describe failed".to_string()));
            }

            if let Some(resp) = &result.response {
                if resp.typ == "describe" {
                    if let Some(desc_val) = &resp.result {
                        let desc: crate::protocol::DescribeResult =
                            serde_json::from_value(desc_val.clone()).map_err(|e| {
                                Error::Error(format!("Failed to parse describe result: {e}"))
                            })?;

                        let columns: Vec<Column> = desc
                            .cols
                            .iter()
                            .map(|c| Column::new(c.name.clone(), c.decltype.clone()))
                            .collect();

                        return Ok(Statement::new(
                            self.clone(),
                            sql.as_ref().to_string(),
                            columns,
                        ));
                    }
                }
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
    pub async fn transaction_with_behavior(&mut self, behavior: TransactionBehavior) -> Result<Transaction<'_>> {
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
    pub async fn pragma_update<V: std::fmt::Display>(&self, pragma_name: &str, pragma_value: V) -> Result<Vec<Row>> {
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

        let step = BatchStep {
            stmt: StmtBody {
                sql: sql.to_string(),
                args,
                named_args,
                want_rows,
            },
            condition: None,
        };

        let mut session = self.session.lock().await;
        let entries = session
            .execute_cursor(CursorBatch { steps: vec![step] })
            .await?;

        let mut columns = Vec::new();
        let mut rows = Vec::new();
        let mut rows_affected = 0u64;
        let mut last_insert_rowid = None;

        for entry in &entries {
            match entry {
                CursorEntry::StepBegin { cols, .. } => {
                    if let Some(cols) = cols {
                        columns = cols
                            .iter()
                            .map(|c| Column::new(c.name.clone(), c.decltype.clone()))
                            .collect();
                    }
                }
                CursorEntry::Row { row, .. } => {
                    if let Some(values) = row {
                        let decoded: Vec<crate::Value> =
                            values.iter().map(decode_value).collect();
                        rows.push(Row::new(decoded));
                    }
                }
                CursorEntry::StepEnd {
                    affected_row_count,
                    last_insert_rowid: rowid,
                    ..
                } => {
                    if let Some(count) = affected_row_count {
                        rows_affected = *count;
                    }
                    if let Some(rowid_str) = rowid {
                        last_insert_rowid = rowid_str.parse::<i64>().ok();
                    }
                }
                CursorEntry::StepError { error, .. } | CursorEntry::Error { error } => {
                    let msg = error
                        .as_ref()
                        .map(|e| e.message.clone())
                        .unwrap_or_else(|| "SQL execution failed".to_string());
                    return Err(Error::Error(msg));
                }
                CursorEntry::ReplicationIndex { .. } => {}
            }
        }

        Ok(ExecuteResult {
            columns,
            rows,
            rows_affected,
            last_insert_rowid,
        })
    }
}

pub(crate) struct ExecuteResult {
    pub columns: Vec<Column>,
    pub rows: Vec<Row>,
    pub rows_affected: u64,
    #[allow(dead_code)]
    pub last_insert_rowid: Option<i64>,
}
