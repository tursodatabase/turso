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
    total_steps: usize,
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
    let layout = BatchLayout {
        user_offset,
        user_count,
        begin: wrap.map(|_| 0),
        commit,
        total_steps: steps.len(),
    };
    (Batch { steps }, layout)
}

/// Decode a wire-level batch result into per-statement results, or the
/// error of the step that failed.
///
/// A failing user statement is reported as
/// [`Error::BatchStatementFailed`] with its zero-based index. Failures of
/// the synthetic `BEGIN`/`COMMIT` steps surface as the underlying error.
/// The synthetic `ROLLBACK` step's error, if any, is ignored: it only runs
/// after a failure that is already being reported, and surfacing it would
/// mask the cause.
pub(crate) fn decode_batch_result(
    result: crate::protocol::BatchResult,
    layout: &BatchLayout,
) -> Result<Vec<BatchResult>> {
    if result.step_results.len() != layout.total_steps
        || result.step_errors.len() != layout.total_steps
    {
        return Err(Error::Http(format!(
            "batch response has {} results and {} errors for {} steps",
            result.step_results.len(),
            result.step_errors.len(),
            layout.total_steps
        )));
    }
    let mut step_results = result.step_results;
    let mut step_errors = result.step_errors;
    // Decode the results of the statements that executed before looking
    // at the errors, so a failure can still report what completed.
    let mut outputs = Vec::with_capacity(layout.user_count);
    for i in 0..layout.user_count {
        outputs.push(
            step_results[layout.user_offset + i]
                .take()
                .map(BatchResult::from_stmt_result)
                .transpose()?,
        );
    }
    if let Some(begin) = layout.begin {
        if let Some(error) = step_errors[begin].take() {
            return Err(error.into());
        }
    }
    for i in 0..layout.user_count {
        if let Some(error) = step_errors[layout.user_offset + i].take() {
            return Err(Error::BatchStatementFailed {
                index: i,
                error: Box::new(error.into()),
                results: outputs,
            });
        }
    }
    if let Some(commit) = layout.commit {
        if let Some(error) = step_errors[commit].take() {
            return Err(error.into());
        }
    }
    outputs
        .into_iter()
        .enumerate()
        .map(|(i, output)| {
            output.ok_or_else(|| {
                Error::Http(format!(
                    "batch response is missing the result for statement {i}"
                ))
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{BatchResult as ProtoBatchResult, ProtoError, ProtoValue, StmtResult};
    use crate::Value;
    use serde_json::json;

    fn user_stmts(n: usize) -> Vec<Stmt> {
        (0..n)
            .map(|i| Stmt::new(format!("SELECT {i}"), true))
            .collect()
    }

    fn stmt_result(rows_affected: u64) -> StmtResult {
        StmtResult {
            cols: Vec::new(),
            rows: Vec::new(),
            affected_row_count: rows_affected,
            last_insert_rowid: None,
            rows_read: None,
            rows_written: None,
            query_duration_ms: None,
        }
    }

    fn proto_error(message: &str) -> ProtoError {
        ProtoError {
            message: message.to_string(),
            code: None,
            extended_code: None,
        }
    }

    #[test]
    fn plain_batch_chains_each_step_on_its_predecessor() {
        let (batch, layout) = build_batch(user_stmts(3), None);
        let json = serde_json::to_value(&batch).unwrap();
        let steps = json["steps"].as_array().unwrap();
        assert_eq!(steps.len(), 3);
        assert_eq!(steps[0].get("condition"), None);
        assert_eq!(steps[1]["condition"], json!({"type": "ok", "step": 0}));
        assert_eq!(steps[2]["condition"], json!({"type": "ok", "step": 1}));
        assert_eq!(layout.user_offset, 0);
        assert_eq!(layout.total_steps, 3);
    }

    #[test]
    fn transactional_batch_wraps_statements_in_begin_commit_rollback() {
        let (batch, layout) = build_batch(user_stmts(2), Some(TransactionBehavior::Immediate));
        let json = serde_json::to_value(&batch).unwrap();
        let steps = json["steps"].as_array().unwrap();
        assert_eq!(steps.len(), 5);

        assert_eq!(steps[0]["stmt"]["sql"], "BEGIN IMMEDIATE");
        assert_eq!(steps[0].get("condition"), None);
        assert_eq!(steps[1]["condition"], json!({"type": "ok", "step": 0}));
        assert_eq!(steps[2]["condition"], json!({"type": "ok", "step": 1}));
        assert_eq!(steps[3]["stmt"]["sql"], "COMMIT");
        assert_eq!(steps[3]["condition"], json!({"type": "ok", "step": 2}));
        assert_eq!(steps[4]["stmt"]["sql"], "ROLLBACK");
        assert_eq!(
            steps[4]["condition"],
            json!({
                "type": "and",
                "conds": [
                    {"type": "ok", "step": 0},
                    {"type": "not", "cond": {"type": "ok", "step": 3}},
                ],
            })
        );

        assert_eq!(layout.user_offset, 1);
        assert_eq!(layout.begin, Some(0));
        assert_eq!(layout.commit, Some(3));
        assert_eq!(layout.total_steps, 5);
    }

    #[test]
    fn decode_maps_results_per_statement_in_order() {
        let (_, layout) = build_batch(user_stmts(2), None);
        let result = ProtoBatchResult {
            step_results: vec![
                Some(StmtResult {
                    rows: vec![vec![ProtoValue::Integer {
                        value: "7".to_string(),
                    }]],
                    ..stmt_result(0)
                }),
                Some(StmtResult {
                    last_insert_rowid: Some("42".to_string()),
                    ..stmt_result(1)
                }),
            ],
            step_errors: vec![None, None],
        };
        let outputs = decode_batch_result(result, &layout).unwrap();
        assert_eq!(outputs.len(), 2);
        assert_eq!(
            outputs[0].rows()[0].get_value(0).unwrap(),
            Value::Integer(7)
        );
        assert_eq!(outputs[1].rows_affected(), 1);
        assert_eq!(outputs[1].last_insert_rowid(), Some(42));
    }

    #[test]
    fn decode_reports_the_failing_statement_index() {
        let (_, layout) = build_batch(user_stmts(3), None);
        let result = ProtoBatchResult {
            step_results: vec![Some(stmt_result(1)), None, None],
            step_errors: vec![None, Some(proto_error("boom")), None],
        };
        let error = decode_batch_result(result, &layout).unwrap_err();
        match error {
            Error::BatchStatementFailed {
                index,
                error,
                results,
            } => {
                assert_eq!(index, 1);
                assert!(matches!(*error, Error::Error(ref m) if m == "boom"));
                // One entry per statement: the completed first statement's
                // result, None for the failing and skipped ones.
                assert_eq!(results.len(), 3);
                assert_eq!(results[0].as_ref().unwrap().rows_affected(), 1);
                assert!(results[1].is_none());
                assert!(results[2].is_none());
            }
            other => panic!("expected BatchStatementFailed, got {other:?}"),
        }
    }

    #[test]
    fn decode_indexes_user_statements_past_the_synthetic_begin() {
        let (_, layout) = build_batch(user_stmts(2), Some(TransactionBehavior::Deferred));
        let result = ProtoBatchResult {
            step_results: vec![
                Some(stmt_result(0)),
                Some(stmt_result(1)),
                None,
                None,
                Some(stmt_result(0)),
            ],
            step_errors: vec![None, None, Some(proto_error("second failed")), None, None],
        };
        let error = decode_batch_result(result, &layout).unwrap_err();
        match error {
            Error::BatchStatementFailed { index, .. } => assert_eq!(index, 1),
            other => panic!("expected BatchStatementFailed, got {other:?}"),
        }
    }

    #[test]
    fn decode_surfaces_commit_failure_without_an_index() {
        let (_, layout) = build_batch(user_stmts(1), Some(TransactionBehavior::Deferred));
        let result = ProtoBatchResult {
            step_results: vec![
                Some(stmt_result(0)),
                Some(stmt_result(1)),
                None,
                Some(stmt_result(0)),
            ],
            step_errors: vec![None, None, Some(proto_error("commit failed")), None],
        };
        let error = decode_batch_result(result, &layout).unwrap_err();
        assert!(matches!(error, Error::Error(ref m) if m == "commit failed"));
    }

    #[test]
    fn decode_ignores_rollback_errors_in_favor_of_the_cause() {
        let (_, layout) = build_batch(user_stmts(1), Some(TransactionBehavior::Deferred));
        let result = ProtoBatchResult {
            step_results: vec![Some(stmt_result(0)), None, None, None],
            step_errors: vec![
                None,
                Some(proto_error("the real cause")),
                None,
                Some(proto_error("cannot rollback")),
            ],
        };
        let error = decode_batch_result(result, &layout).unwrap_err();
        match error {
            Error::BatchStatementFailed { index, error, .. } => {
                assert_eq!(index, 0);
                assert!(matches!(*error, Error::Error(ref m) if m == "the real cause"));
            }
            other => panic!("expected BatchStatementFailed, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_a_skipped_statement_with_no_error() {
        let (_, layout) = build_batch(user_stmts(2), None);
        let result = ProtoBatchResult {
            step_results: vec![Some(stmt_result(0)), None],
            step_errors: vec![None, None],
        };
        let error = decode_batch_result(result, &layout).unwrap_err();
        assert!(matches!(error, Error::Http(_)));
    }

    #[test]
    fn decode_rejects_a_result_count_mismatch() {
        let (_, layout) = build_batch(user_stmts(2), None);
        let result = ProtoBatchResult {
            step_results: vec![Some(stmt_result(0))],
            step_errors: vec![None],
        };
        let error = decode_batch_result(result, &layout).unwrap_err();
        assert!(matches!(error, Error::Http(_)));
    }
}
