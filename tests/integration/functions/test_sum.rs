use crate::common::{limbo_exec_rows, limbo_exec_rows_fallible, sqlite_exec_rows, TempDatabase};
use turso_core::LimboError;

#[test]
fn sum_errors_on_integer_overflow() {
    let _ = env_logger::try_init();

    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();
    let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

    limbo_exec_rows(&tmp_db, &conn, "CREATE TABLE t(a)");
    sqlite_exec_rows(&sqlite_conn, "CREATE TABLE t(a)");

    limbo_exec_rows(&tmp_db, &conn, "INSERT INTO t VALUES (9223372036854775807)");
    sqlite_exec_rows(&sqlite_conn, "INSERT INTO t VALUES (9223372036854775807)");

    let limbo_before_overflow = limbo_exec_rows(&tmp_db, &conn, "SELECT sum(a) FROM t");
    let sqlite_before_overflow = sqlite_exec_rows(&sqlite_conn, "SELECT sum(a) FROM t");
    assert_eq!(limbo_before_overflow, sqlite_before_overflow);

    limbo_exec_rows(&tmp_db, &conn, "INSERT INTO t VALUES (1)");
    sqlite_exec_rows(&sqlite_conn, "INSERT INTO t VALUES (1)");

    let err = limbo_exec_rows_fallible(&tmp_db, &conn, "SELECT sum(a) FROM t")
        .expect_err("SUM should report integer overflow");
    assert!(matches!(err, LimboError::IntegerOverflow));

    use rusqlite::ffi::Error as FfiError;
    use rusqlite::Error::SqliteFailure;
    use rusqlite::ErrorCode;

    let mut stmt = sqlite_conn
        .prepare("SELECT sum(a) FROM t")
        .expect("prepare should succeed");
    let mut rows = stmt.query([]).expect("query should start");
    let sqlite_err = loop {
        match rows.next() {
            Ok(Some(_)) => continue,
            Ok(None) => panic!("expected overflow error"),
            Err(err) => break err,
        }
    };
    match sqlite_err {
        SqliteFailure(
            FfiError {
                code: ErrorCode::Unknown,
                ..
            },
            Some(message),
        ) => {
            assert_eq!(message, "integer overflow");
        }
        SqliteFailure(
            FfiError {
                code: ErrorCode::Unknown,
                ..
            },
            None,
        ) => {}
        other => panic!("unexpected sqlite error: {other:?}"),
    }
}
