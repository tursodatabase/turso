use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU8, Ordering};
use std::sync::Arc;

use tokio::sync::Mutex;
use turso_remote_protocol::{Batch, BatchCond, BatchCondList, BatchStep, BatchStreamReq};

use crate::params::IntoParams;
use crate::protocol::{
    decode_value, encode_value, ExecuteStreamReq, NamedArg, SequenceStreamReq, StmtBody,
    StreamRequest, StreamResponse, StreamResult,
};
use crate::rows::{Row, Rows};
use crate::session::{Session, SessionConfig};
use crate::statement::Statement;
use crate::transaction::{Transaction, TransactionBehavior};
use crate::{Column, Error, Params, Result};

/// A connection to a remote Turso database.
///
/// Each connection holds its own HTTP session with baton state.
/// Connections are cheaply cloneable — clones share the same session.
#[derive(Clone)]
pub struct Connection {
    pub(crate) session: Arc<Mutex<Session>>,
    /// Server-authoritative autocommit flag, shared with the session so
    /// `is_autocommit()` reads it without locking.
    autocommit: Arc<AtomicBool>,
    /// Rowid of the last INSERT executed on this connection.
    last_rowid: Arc<AtomicI64>,
    /// Default behavior used by `transaction()`/`unchecked_transaction()`.
    default_txn_behavior: TransactionBehavior,
    /// Action a dropped, unfinished `Transaction` left pending, applied on the
    /// next operation (0 = none, 1 = rollback, 2 = commit). Mirrors the
    /// embedded driver's dangling-transaction handling.
    pub(crate) pending_drop: Arc<AtomicU8>,
}

/// Values for [`Connection::pending_drop`].
pub(crate) const PENDING_NONE: u8 = 0;
pub(crate) const PENDING_ROLLBACK: u8 = 1;
pub(crate) const PENDING_COMMIT: u8 = 2;

impl std::fmt::Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection").finish()
    }
}

/// A single statement in a [`Connection::batch`] call.
pub struct BatchStatement {
    pub sql: String,
    pub params: Params,
}

impl From<&str> for BatchStatement {
    fn from(sql: &str) -> Self {
        Self {
            sql: sql.to_string(),
            params: Params::None,
        }
    }
}

impl From<String> for BatchStatement {
    fn from(sql: String) -> Self {
        Self {
            sql,
            params: Params::None,
        }
    }
}

impl<S: Into<String>> From<(S, Params)> for BatchStatement {
    fn from((sql, params): (S, Params)) -> Self {
        Self {
            sql: sql.into(),
            params,
        }
    }
}

/// Per-statement result of a [`Connection::batch`] call, in input order.
#[derive(Debug, Clone)]
pub struct BatchResult {
    pub columns: Vec<Column>,
    pub rows: Vec<Row>,
    pub rows_affected: u64,
}

impl Connection {
    pub(crate) fn new(url: String, config: SessionConfig) -> Self {
        let autocommit = Arc::new(AtomicBool::new(true));
        let session = Session::new(url, config, autocommit.clone());
        Self {
            session: Arc::new(Mutex::new(session)),
            autocommit,
            last_rowid: Arc::new(AtomicI64::new(0)),
            default_txn_behavior: TransactionBehavior::Deferred,
            pending_drop: Arc::new(AtomicU8::new(PENDING_NONE)),
        }
    }

    /// Apply a transaction-control action left pending by a dropped, unfinished
    /// `Transaction` (embedded-driver parity). Best-effort; a dead stream means
    /// the server already rolled back.
    async fn flush_pending_drop(&self, session: &mut Session) {
        let sql = match self.pending_drop.swap(PENDING_NONE, Ordering::SeqCst) {
            PENDING_ROLLBACK => "ROLLBACK",
            PENDING_COMMIT => "COMMIT",
            _ => return,
        };
        let _ = session
            .execute_pipeline(vec![StreamRequest::Execute(ExecuteStreamReq {
                stmt: StmtBody {
                    sql: Some(sql.to_string()),
                    sql_id: None,
                    args: vec![],
                    named_args: vec![],
                    want_rows: Some(false),
                    replication_index: None,
                },
            })])
            .await;
    }

    /// Execute a SQL statement and return the number of rows affected.
    pub async fn execute(&self, sql: impl AsRef<str>, params: impl IntoParams) -> Result<u64> {
        let result = self.execute_inner(sql.as_ref(), params, false).await?;
        Ok(result.rows_affected)
    }

    /// Query the database and return a result set.
    pub async fn query(&self, sql: impl AsRef<str>, params: impl IntoParams) -> Result<Rows> {
        let result = self.execute_inner(sql.as_ref(), params, true).await?;
        Ok(Rows::new(result.columns, result.rows))
    }

    /// Execute a batch of SQL statements separated by semicolons.
    pub async fn execute_batch(&self, sql: impl AsRef<str>) -> Result<()> {
        let mut session = self.session.lock().await;
        self.flush_pending_drop(&mut session).await;
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

    /// Run several statements in one round trip, returning one [`BatchResult`]
    /// per input statement. When `mode` is set the batch runs atomically via a
    /// server-side BEGIN/COMMIT/ROLLBACK condition chain (ported from the JS
    /// driver's `batch()`).
    pub async fn batch<S: Into<BatchStatement>>(
        &self,
        statements: Vec<S>,
        mode: Option<TransactionBehavior>,
    ) -> Result<Vec<BatchResult>> {
        // Inside an outer transaction the surrounding BEGIN already opened one on
        // this stream; emitting another BEGIN would fail, so drop the mode and run
        // the statements within the existing transaction (matches the JS driver).
        let mode = if self.in_transaction() { None } else { mode };

        let user_steps: Vec<BatchStep> = statements
            .into_iter()
            .map(|s| {
                let s: BatchStatement = s.into();
                let (args, named_args) = encode_params(s.params);
                BatchStep {
                    stmt: StmtBody {
                        sql: Some(s.sql),
                        sql_id: None,
                        args,
                        named_args,
                        want_rows: Some(true),
                        replication_index: None,
                    },
                    condition: None,
                }
            })
            .collect();

        let n = user_steps.len();
        let (steps, first_user_idx, rollback_idx) = build_batch_steps(user_steps, mode);

        let mut session = self.session.lock().await;
        self.flush_pending_drop(&mut session).await;
        let response = session
            .execute_pipeline(vec![StreamRequest::Batch(BatchStreamReq {
                batch: Batch {
                    steps,
                    replication_index: None,
                },
            })])
            .await?;

        let batch_resp = match response.results.first() {
            Some(StreamResult::Error { error }) => return Err(Error::Error(error.message.clone())),
            Some(StreamResult::Ok {
                response: StreamResponse::Batch(b),
            }) => &b.result,
            _ => return Err(Error::Error("Unexpected batch response".to_string())),
        };

        // Surface the first real error (skip the synthetic ROLLBACK step).
        for (i, err) in batch_resp.step_errors.iter().enumerate() {
            if let Some(e) = err {
                if rollback_idx.is_some_and(|r| r == i) {
                    continue;
                }
                return Err(Error::Error(e.message.clone()));
            }
        }

        let mut out = Vec::with_capacity(n);
        for user_idx in 0..n {
            let step_idx = first_user_idx + user_idx;
            let result = batch_resp
                .step_results
                .get(step_idx)
                .and_then(|o| o.as_ref());
            match result {
                Some(sr) => {
                    let columns = sr
                        .cols
                        .iter()
                        .map(|c| {
                            Column::new(c.name.clone().unwrap_or_default(), c.decltype.clone())
                        })
                        .collect();
                    let rows = sr
                        .rows
                        .iter()
                        .map(|r| Row::new(r.values.iter().map(decode_value).collect()))
                        .collect();
                    out.push(BatchResult {
                        columns,
                        rows,
                        rows_affected: sr.affected_row_count,
                    });
                }
                None => out.push(BatchResult {
                    columns: vec![],
                    rows: vec![],
                    rows_affected: 0,
                }),
            }
        }
        Ok(out)
    }

    /// Prepare a SQL statement for later execution.
    pub async fn prepare(&self, sql: impl AsRef<str>) -> Result<Statement> {
        let mut session = self.session.lock().await;
        self.flush_pending_drop(&mut session).await;
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

        Ok(Statement::new(
            self.clone(),
            sql.as_ref().to_string(),
            vec![],
        ))
    }

    /// Begin a new transaction with the connection's default behavior.
    pub async fn transaction(&mut self) -> Result<Transaction<'_>> {
        let behavior = self.default_txn_behavior;
        Transaction::new(self, behavior).await
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
        Transaction::new_unchecked(self, self.default_txn_behavior).await
    }

    /// Set the default transaction behavior used by `transaction()` and
    /// `unchecked_transaction()`.
    pub fn set_transaction_behavior(&mut self, behavior: TransactionBehavior) {
        self.default_txn_behavior = behavior;
    }

    /// Record an action for a dropped, unfinished `Transaction` to be applied on
    /// the next operation (called from `Transaction`'s `Drop`).
    pub(crate) fn set_pending_drop(&self, action: u8) {
        self.pending_drop.store(action, Ordering::SeqCst);
    }

    /// Check if the connection is in autocommit mode.
    ///
    /// Reflects the server's authoritative `get_autocommit` answer, refreshed
    /// after every request.
    pub fn is_autocommit(&self) -> Result<bool> {
        Ok(self.autocommit.load(Ordering::SeqCst))
    }

    /// Whether a transaction is currently open on this connection.
    pub fn in_transaction(&self) -> bool {
        !self.autocommit.load(Ordering::SeqCst)
    }

    /// Execute a PRAGMA query and call a closure for each result row.
    pub async fn pragma_query<F>(&self, pragma_name: &str, mut f: F) -> Result<()>
    where
        F: FnMut(&Row) -> Result<()>,
    {
        let sql = format!("PRAGMA {pragma_name}");
        let rows = self.query(&sql, ()).await?;
        for row in &rows.rows {
            f(row)?;
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
        let rows = self.query(&sql, ()).await?;
        Ok(rows.rows)
    }

    /// Return the rowid of the most recent INSERT on this connection.
    pub fn last_insert_rowid(&self) -> i64 {
        self.last_rowid.load(Ordering::SeqCst)
    }

    /// Close the connection session.
    pub async fn close(&self) -> Result<()> {
        let mut session = self.session.lock().await;
        session.close().await
    }

    pub(crate) async fn execute_inner(
        &self,
        sql: &str,
        params: impl IntoParams,
        want_rows: bool,
    ) -> Result<ExecuteResult> {
        let (args, named_args) = encode_params(params.into_params()?);

        let stmt = StmtBody {
            sql: Some(sql.to_string()),
            sql_id: None,
            args,
            named_args,
            want_rows: Some(want_rows),
            replication_index: None,
        };

        let mut session = self.session.lock().await;
        self.flush_pending_drop(&mut session).await;
        let response = session
            .execute_pipeline(vec![StreamRequest::Execute(ExecuteStreamReq { stmt })])
            .await?;

        let result = response
            .results
            .first()
            .ok_or_else(|| Error::Error("No result in pipeline response".to_string()))?;

        match result {
            StreamResult::Error { error } => Err(Error::Error(error.message.clone())),
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
                    .map(|row| Row::new(row.values.iter().map(decode_value).collect()))
                    .collect();

                if let Some(rowid) = exec_result.last_insert_rowid {
                    self.last_rowid.store(rowid, Ordering::SeqCst);
                }

                Ok(ExecuteResult {
                    columns,
                    rows,
                    rows_affected: exec_result.affected_row_count,
                    last_insert_rowid: exec_result.last_insert_rowid,
                })
            }
            _ => Err(Error::Error(
                "Unexpected result in pipeline response".to_string(),
            )),
        }
    }
}

/// Encode [`Params`] into positional/named protocol args.
fn encode_params(params: Params) -> (Vec<turso_remote_protocol::Value>, Vec<NamedArg>) {
    match params {
        Params::None => (vec![], vec![]),
        Params::Positional(values) => (values.iter().map(encode_value).collect(), vec![]),
        Params::Named(values) => {
            let named = values
                .iter()
                .map(|(name, val)| NamedArg {
                    name: name.to_string(),
                    value: encode_value(val),
                })
                .collect();
            (vec![], named)
        }
    }
}

/// Build the step list for a batch. Returns (steps, first_user_step_index,
/// rollback_step_index). Without a mode the user steps run as-is; with a mode
/// they are wrapped in an atomic BEGIN/COMMIT/ROLLBACK condition chain.
fn build_batch_steps(
    user_steps: Vec<BatchStep>,
    mode: Option<TransactionBehavior>,
) -> (Vec<BatchStep>, usize, Option<usize>) {
    let n = user_steps.len();
    let Some(mode) = mode else {
        return (user_steps, 0, None);
    };

    let begin_idx = 0u32;
    let first_user_idx = 1usize;
    let last_user_idx = n; // user steps occupy 1..=n
    let commit_idx = (last_user_idx + 1) as u32;
    let rollback_idx = last_user_idx + 2;

    let control = |sql: &str, condition: Option<BatchCond>| BatchStep {
        stmt: StmtBody {
            sql: Some(sql.to_string()),
            sql_id: None,
            args: vec![],
            named_args: vec![],
            want_rows: Some(false),
            replication_index: None,
        },
        condition,
    };

    let mut steps = Vec::with_capacity(n + 3);
    steps.push(control(&format!("BEGIN {}", mode.as_sql_mode()), None));
    for (i, mut step) in user_steps.into_iter().enumerate() {
        let prev = if i == 0 {
            begin_idx
        } else {
            (first_user_idx + i - 1) as u32
        };
        step.condition = Some(BatchCond::Ok { step: prev });
        steps.push(step);
    }
    steps.push(control(
        "COMMIT",
        Some(BatchCond::Ok {
            step: last_user_idx as u32,
        }),
    ));
    steps.push(control(
        "ROLLBACK",
        Some(BatchCond::And(BatchCondList {
            conds: vec![
                BatchCond::Ok { step: begin_idx },
                BatchCond::Not {
                    cond: Box::new(BatchCond::Ok { step: commit_idx }),
                },
            ],
        })),
    ));
    (steps, first_user_idx, Some(rollback_idx))
}

pub(crate) struct ExecuteResult {
    pub columns: Vec<Column>,
    pub rows: Vec<Row>,
    pub rows_affected: u64,
    #[allow(dead_code)]
    pub last_insert_rowid: Option<i64>,
}
