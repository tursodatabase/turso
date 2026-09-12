use super::*;
use crate::{named_params, BatchStatement};

#[test]
fn batch_preflight_reports_late_infinite_positional_param() {
    let statements = vec![
        BatchStatement::new("SELECT ?1", (1.0,)).unwrap(),
        BatchStatement::new("SELECT ?1", (f64::INFINITY,)).unwrap(),
    ];
    let error = Connection::build_batch_stmts(statements).unwrap_err();

    assert!(matches!(
        error,
        Error::BatchStatementFailed {
            index: 1,
            error,
            results,
        } if matches!(*error, Error::ToSqlConversionFailure(_)) && results.is_empty()
    ));
}

#[test]
fn batch_preflight_reports_infinite_named_param() {
    let stmt = BatchStatement::new("SELECT :x", named_params![":x": f64::NEG_INFINITY]).unwrap();
    let error = Connection::build_batch_stmts([stmt]).unwrap_err();
    assert!(matches!(
        error,
        Error::BatchStatementFailed {
            index: 0,
            error,
            results,
        } if matches!(*error, Error::ToSqlConversionFailure(_)) && results.is_empty()
    ));
}

#[test]
fn transaction_control_detection_skips_space_and_comments() {
    for sql in [
        "BEGIN",
        "  -- why\ncommit transaction",
        "/* first */ SAVEPOINT s",
        "; /* empty statement */ COMMIT",
        "\u{feff}release s",
        "END",
    ] {
        assert!(is_transaction_control_statement(sql), "{sql:?}");
    }
    for sql in [
        "SELECT 'COMMIT'",
        "CREATE TABLE rollback_log (x)",
        "BEGINNING",
        "/* unterminated",
    ] {
        assert!(!is_transaction_control_statement(sql), "{sql:?}");
    }
}

#[test]
fn managed_batch_rejects_transaction_control_with_its_user_index() {
    let stmts =
        Connection::build_batch_stmts(["SELECT 1", "; /* empty statement */ COMMIT", "SELECT 2"])
            .unwrap();
    let error = Connection::validate_managed_batch(&stmts).unwrap_err();
    assert!(matches!(
        error,
        Error::BatchStatementFailed {
            index: 1,
            error,
            results,
        } if matches!(*error, Error::Misuse(_)) && results.is_empty()
    ));
}
