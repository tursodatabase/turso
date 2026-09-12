//! Parameterized statement batches.
//!
//! A batch sends multiple statements — each with its own bind parameters —
//! to the server as a single batch request (section 6.2 of the protocol
//! specification), so the whole batch completes in one HTTP round trip.
//! See [`Connection::batch`](crate::Connection::batch) and
//! [`Connection::transactional_batch`](crate::Connection::transactional_batch).

use crate::{
    params::{IntoParams, Params},
    protocol::{decode_value_owned, Batch, BatchCond, BatchStep, Stmt},
    rows::Row,
    transaction::TransactionBehavior,
    Column, Error, Result,
};

mod sealed {
    pub trait Sealed {}
}

use sealed::Sealed;

/// One statement of a batch, with its bind parameters.
///
/// Usually built implicitly from a SQL string or a `(sql, params)` pair
/// (see [`IntoBatchStatement`]). Build `BatchStatement`s explicitly to mix
/// parameter shapes in one batch, since the elements of a single array or
/// `Vec` must share one type:
///
/// ```rust
/// # fn build() -> turso_serverless::Result<Vec<turso_serverless::BatchStatement>> {
/// use turso_serverless::BatchStatement;
/// Ok(vec![
///     BatchStatement::new("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)", ())?,
///     BatchStatement::new("INSERT INTO t (v) VALUES (?1)", ("x",))?,
/// ])
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct BatchStatement {
    pub(crate) sql: String,
    pub(crate) params: Params,
}

impl BatchStatement {
    /// Create a batch statement from SQL text and parameters, accepting the
    /// same parameter forms as [`Connection::execute`](crate::Connection::execute).
    pub fn new(sql: impl Into<String>, params: impl IntoParams) -> Result<Self> {
        Ok(Self {
            sql: sql.into(),
            params: params.into_params()?,
        })
    }
}

/// Converts some type into one statement of a batch.
///
/// Implemented for:
///
/// - SQL strings without parameters: `"DELETE FROM t"`.
/// - `(sql, params)` pairs, with the same parameter forms as
///   [`Connection::execute`](crate::Connection::execute):
///   `("INSERT INTO t (v) VALUES (?1)", ("x",))`.
/// - [`BatchStatement`], for batches that mix parameter shapes.
pub trait IntoBatchStatement: Sealed {
    #[doc(hidden)]
    fn into_batch_statement(self) -> Result<BatchStatement>;
}

impl Sealed for BatchStatement {}
impl IntoBatchStatement for BatchStatement {
    fn into_batch_statement(self) -> Result<BatchStatement> {
        Ok(self)
    }
}

impl Sealed for &str {}
impl IntoBatchStatement for &str {
    fn into_batch_statement(self) -> Result<BatchStatement> {
        BatchStatement::new(self, ())
    }
}

impl Sealed for String {}
impl IntoBatchStatement for String {
    fn into_batch_statement(self) -> Result<BatchStatement> {
        BatchStatement::new(self, ())
    }
}

impl<S: Into<String>, P: IntoParams> Sealed for (S, P) {}
impl<S: Into<String>, P: IntoParams> IntoBatchStatement for (S, P) {
    fn into_batch_statement(self) -> Result<BatchStatement> {
        BatchStatement::new(self.0, self.1)
    }
}

/// The result of one statement of a batch.
#[derive(Debug)]
pub struct BatchResult {
    columns: Vec<Column>,
    rows: Vec<Row>,
    rows_affected: u64,
    last_insert_rowid: Option<i64>,
    rows_read: Option<u64>,
    rows_written: Option<u64>,
    query_duration_ms: Option<f64>,
}

impl BatchResult {
    /// Returns the columns of the statement's result set.
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    /// Returns the rows returned by the statement.
    pub fn rows(&self) -> &[Row] {
        &self.rows
    }

    /// Returns the number of rows changed by the statement.
    pub fn rows_affected(&self) -> u64 {
        self.rows_affected
    }

    /// Returns the rowid inserted by the statement, when the statement was
    /// an INSERT into a table with a rowid.
    pub fn last_insert_rowid(&self) -> Option<i64> {
        self.last_insert_rowid
    }

    /// Returns the number of rows read while executing the statement, as
    /// reported by the server (section 8.4).
    pub fn rows_read(&self) -> Option<u64> {
        self.rows_read
    }

    /// Returns the number of rows written while executing the statement,
    /// as reported by the server (section 8.4).
    pub fn rows_written(&self) -> Option<u64> {
        self.rows_written
    }

    /// Returns the server-side execution time of the statement in
    /// milliseconds (section 8.4).
    pub fn query_duration_ms(&self) -> Option<f64> {
        self.query_duration_ms
    }

    fn from_stmt_result(result: crate::protocol::StmtResult) -> Result<Self> {
        let columns: Vec<Column> = result
            .cols
            .into_iter()
            .map(|c| Column {
                name: c.name.unwrap_or_default(),
                decl_type: c.decltype,
            })
            .collect();
        let rows = result
            .rows
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(decode_value_owned)
                    .collect::<Result<Vec<_>>>()
                    .map(Row::new)
            })
            .collect::<Result<Vec<_>>>()?;
        let last_insert_rowid = result
            .last_insert_rowid
            .map(|rowid| {
                rowid
                    .parse::<i64>()
                    .map_err(|e| Error::Error(format!("invalid rowid in server response: {e}")))
            })
            .transpose()?;
        // Row-returning statements (e.g. INSERT ... RETURNING) report 0,
        // matching the embedded drivers.
        let rows_affected = if columns.is_empty() {
            result.affected_row_count
        } else {
            0
        };
        Ok(Self {
            columns,
            rows,
            rows_affected,
            last_insert_rowid,
            rows_read: result.rows_read,
            rows_written: result.rows_written,
            query_duration_ms: result.query_duration_ms,
        })
    }
}

/// Where the user's statements sit among the steps of a batch on the wire:
/// an atomic batch surrounds them with synthetic `BEGIN`/`COMMIT`/`ROLLBACK`
/// steps.
pub(crate) struct BatchLayout {
    user_offset: usize,
    user_count: usize,
    begin: Option<usize>,
    commit: Option<usize>,
    rollback: Option<usize>,
    total_steps: usize,
}

pub(crate) struct DecodedBatch {
    pub(crate) outcome: Result<Vec<BatchResult>>,
    pub(crate) last_insert_rowid: Option<i64>,
}

/// Build the wire-level batch for the given statements.
///
/// Each statement is gated on its predecessor succeeding, so execution
/// stops at the first failure. With `wrap` set, the statements are
/// additionally surrounded by `BEGIN <behavior>`, a `COMMIT` gated on the
/// last statement succeeding, and a `ROLLBACK` gated on `BEGIN` having
/// succeeded and `COMMIT` not having succeeded. The extra ok(BEGIN) guard
/// prevents the ROLLBACK from aborting a transaction the caller opened on
/// the stream out of band.
pub(crate) fn build_batch(
    stmts: Vec<Stmt>,
    wrap: Option<TransactionBehavior>,
) -> (Batch, BatchLayout) {
    let user_count = stmts.len();
    let mut steps = Vec::with_capacity(user_count + if wrap.is_some() { 3 } else { 0 });
    let user_offset = match wrap {
        None => 0,
        Some(behavior) => {
            steps.push(BatchStep {
                condition: None,
                stmt: Stmt::new(behavior.begin_sql(), false),
            });
            1
        }
    };
    for (i, stmt) in stmts.into_iter().enumerate() {
        let prev = (user_offset + i).checked_sub(1);
        steps.push(BatchStep {
            condition: prev.map(|prev| BatchCond::Ok { step: prev as u32 }),
            stmt,
        });
    }
    let commit = wrap.map(|_| {
        let commit = user_offset + user_count;
        steps.push(BatchStep {
            condition: Some(BatchCond::Ok {
                step: (commit - 1) as u32,
            }),
            stmt: Stmt::new("COMMIT", false),
        });
        steps.push(BatchStep {
            condition: Some(BatchCond::And {
                conds: vec![
                    BatchCond::Ok { step: 0 },
                    BatchCond::Not {
                        cond: Box::new(BatchCond::Ok {
                            step: commit as u32,
                        }),
                    },
                ],
            }),
            stmt: Stmt::new("ROLLBACK", false),
        });
        commit
    });
    let rollback = commit.map(|commit| commit + 1);
    let layout = BatchLayout {
        user_offset,
        user_count,
        begin: wrap.map(|_| 0),
        commit,
        rollback,
        total_steps: steps.len(),
    };
    (Batch { steps }, layout)
}

/// Decode a wire-level batch result into per-statement results, or the
/// error of the step that failed. The decoded rowid is kept separate from
/// the outcome so the connection cache can still advance when a synthetic
/// `COMMIT` or `ROLLBACK` step fails after user statements completed.
///
/// A failing user statement is reported as
/// [`Error::BatchStatementFailed`] with its zero-based index. Failures of
/// the synthetic `BEGIN`/`COMMIT` steps surface as the underlying error. If
/// the automatic rollback also fails, both errors are preserved in
/// [`Error::BatchRollbackFailed`].
pub(crate) fn decode_batch_result(
    result: crate::protocol::BatchResult,
    layout: &BatchLayout,
) -> DecodedBatch {
    if result.step_results.len() != layout.total_steps
        || result.step_errors.len() != layout.total_steps
    {
        return DecodedBatch {
            outcome: Err(Error::Http(format!(
                "batch response has {} results and {} errors for {} steps",
                result.step_results.len(),
                result.step_errors.len(),
                layout.total_steps
            ))),
            last_insert_rowid: None,
        };
    }
    let mut step_results = result.step_results;
    let mut step_errors = result.step_errors;
    if let Some(step) = step_results
        .iter()
        .zip(&step_errors)
        .position(|(result, error)| result.is_some() && error.is_some())
    {
        return DecodedBatch {
            outcome: Err(Error::Http(format!(
                "batch response step {step} has both a result and an error"
            ))),
            last_insert_rowid: None,
        };
    }
    // Decode the results of the statements that executed before looking
    // at the errors, so a failure can still report what completed.
    let mut outputs = Vec::with_capacity(layout.user_count);
    let mut last_insert_rowid = None;
    for i in 0..layout.user_count {
        let output = match step_results[layout.user_offset + i]
            .take()
            .map(BatchResult::from_stmt_result)
            .transpose()
        {
            Ok(output) => output,
            Err(error) => {
                return DecodedBatch {
                    outcome: Err(error),
                    last_insert_rowid,
                };
            }
        };
        if let Some(rowid) = output.as_ref().and_then(BatchResult::last_insert_rowid) {
            last_insert_rowid = Some(rowid);
        }
        outputs.push(output);
    }

    enum Failure {
        Synthetic(Error),
        Statement { index: usize, error: Error },
    }

    let mut failure = layout
        .begin
        .and_then(|begin| step_errors[begin].take().map(Error::from))
        .map(Failure::Synthetic);
    if failure.is_none() {
        for i in 0..layout.user_count {
            if let Some(statement_error) = step_errors[layout.user_offset + i].take() {
                failure = Some(Failure::Statement {
                    index: i,
                    error: statement_error.into(),
                });
                break;
            }
        }
    }
    if failure.is_none() {
        failure = layout
            .commit
            .and_then(|commit| step_errors[commit].take().map(Error::from))
            .map(Failure::Synthetic);
    }
    let failure_into_error = |failure, results| match failure {
        Failure::Synthetic(error) => error,
        Failure::Statement { index, error } => Error::BatchStatementFailed {
            index,
            error: Box::new(error),
            results,
        },
    };
    if let Some(rollback_error) = layout
        .rollback
        .and_then(|rollback| step_errors[rollback].take())
    {
        let cause = failure.map_or_else(
            || Error::Http("batch rollback failed without a preceding batch failure".to_string()),
            |failure| failure_into_error(failure, outputs),
        );
        return DecodedBatch {
            outcome: Err(Error::BatchRollbackFailed {
                error: Box::new(cause),
                rollback_error: Box::new(rollback_error.into()),
            }),
            last_insert_rowid,
        };
    }
    if let Some(failure) = failure {
        return DecodedBatch {
            outcome: Err(failure_into_error(failure, outputs)),
            last_insert_rowid,
        };
    }

    if let Some(begin) = layout.begin {
        if step_results[begin].is_none() {
            return DecodedBatch {
                outcome: Err(Error::Http(
                    "batch response is missing the BEGIN result".to_string(),
                )),
                last_insert_rowid,
            };
        }
    }
    if let Some(commit) = layout.commit {
        if step_results[commit].is_none() {
            return DecodedBatch {
                outcome: Err(Error::Http(
                    "batch response is missing the COMMIT result".to_string(),
                )),
                last_insert_rowid,
            };
        }
    }
    if let Some(rollback) = layout.rollback {
        if step_results[rollback].is_some() {
            return DecodedBatch {
                outcome: Err(Error::Http(
                    "batch response ran ROLLBACK after a successful COMMIT".to_string(),
                )),
                last_insert_rowid,
            };
        }
    }

    let outcome = outputs
        .into_iter()
        .enumerate()
        .map(|(i, output)| {
            output.ok_or_else(|| {
                Error::Http(format!(
                    "batch response is missing the result for statement {i}"
                ))
            })
        })
        .collect();
    DecodedBatch {
        outcome,
        last_insert_rowid,
    }
}

#[cfg(test)]
#[path = "tests/unit/batch/tests.rs"]
mod tests;
