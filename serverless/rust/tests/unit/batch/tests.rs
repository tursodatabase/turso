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
    assert_eq!(layout.rollback, Some(4));
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
    let decoded = decode_batch_result(result, &layout);
    assert_eq!(decoded.last_insert_rowid, Some(42));
    let outputs = decoded.outcome.unwrap();
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
    let error = decode_batch_result(result, &layout).outcome.unwrap_err();
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
    let error = decode_batch_result(result, &layout).outcome.unwrap_err();
    match error {
        Error::BatchStatementFailed { index, .. } => assert_eq!(index, 1),
        other => panic!("expected BatchStatementFailed, got {other:?}"),
    }
}

#[test]
fn decode_surfaces_commit_failure_and_the_latest_user_rowid() {
    let (_, layout) = build_batch(user_stmts(1), Some(TransactionBehavior::Deferred));
    let result = ProtoBatchResult {
        step_results: vec![
            Some(stmt_result(0)),
            Some(StmtResult {
                last_insert_rowid: Some("41".to_string()),
                ..stmt_result(1)
            }),
            None,
            Some(stmt_result(0)),
        ],
        step_errors: vec![None, None, Some(proto_error("commit failed")), None],
    };
    let decoded = decode_batch_result(result, &layout);
    assert_eq!(decoded.last_insert_rowid, Some(41));
    let error = decoded.outcome.unwrap_err();
    assert!(matches!(error, Error::Error(ref m) if m == "commit failed"));
}

#[test]
fn decode_preserves_the_cause_and_rollback_error() {
    let (_, layout) = build_batch(user_stmts(2), Some(TransactionBehavior::Deferred));
    let result = ProtoBatchResult {
        step_results: vec![
            Some(stmt_result(0)),
            Some(StmtResult {
                last_insert_rowid: Some("40".to_string()),
                ..stmt_result(1)
            }),
            None,
            None,
            None,
        ],
        step_errors: vec![
            None,
            None,
            Some(proto_error("the real cause")),
            None,
            Some(proto_error("cannot rollback")),
        ],
    };
    let decoded = decode_batch_result(result, &layout);
    assert_eq!(decoded.last_insert_rowid, Some(40));
    let error = decoded.outcome.unwrap_err();
    match error {
        Error::BatchRollbackFailed {
            error,
            rollback_error,
        } => {
            assert!(matches!(
                *error,
                Error::BatchStatementFailed {
                    index: 1,
                    error,
                    ..
                } if matches!(*error, Error::Error(ref m) if m == "the real cause")
            ));
            assert!(matches!(
                *rollback_error,
                Error::Error(ref m) if m == "cannot rollback"
            ));
        }
        other => panic!("expected BatchRollbackFailed, got {other:?}"),
    }
}

#[test]
fn decode_rejects_a_skipped_statement_with_no_error() {
    let (_, layout) = build_batch(user_stmts(2), None);
    let result = ProtoBatchResult {
        step_results: vec![Some(stmt_result(0)), None],
        step_errors: vec![None, None],
    };
    let error = decode_batch_result(result, &layout).outcome.unwrap_err();
    assert!(matches!(error, Error::Http(_)));
}

#[test]
fn decode_rejects_a_result_count_mismatch() {
    let (_, layout) = build_batch(user_stmts(2), None);
    let result = ProtoBatchResult {
        step_results: vec![Some(stmt_result(0))],
        step_errors: vec![None],
    };
    let error = decode_batch_result(result, &layout).outcome.unwrap_err();
    assert!(matches!(error, Error::Http(_)));
}

#[test]
fn decode_rejects_a_step_with_a_result_and_an_error() {
    let (_, layout) = build_batch(user_stmts(1), None);
    let result = ProtoBatchResult {
        step_results: vec![Some(stmt_result(0))],
        step_errors: vec![Some(proto_error("impossible"))],
    };
    let error = decode_batch_result(result, &layout).outcome.unwrap_err();
    assert!(matches!(error, Error::Http(ref message) if message.contains("both")));
}

#[test]
fn decode_rejects_a_missing_commit_result() {
    let (_, layout) = build_batch(user_stmts(1), Some(TransactionBehavior::Deferred));
    let result = ProtoBatchResult {
        step_results: vec![Some(stmt_result(0)), Some(stmt_result(1)), None, None],
        step_errors: vec![None, None, None, None],
    };
    let error = decode_batch_result(result, &layout).outcome.unwrap_err();
    assert!(matches!(error, Error::Http(ref message) if message.contains("COMMIT")));
}
