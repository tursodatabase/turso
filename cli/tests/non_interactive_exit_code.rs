use std::io::Write;
use std::process::{Command, Stdio};

// ---------------------------------------------------------------------------
// A. SQL argument mode
// ---------------------------------------------------------------------------

/// A1: Success path returns 0
#[test]
fn sql_argument_returns_exit_code_zero_on_success() {
    let status = Command::new(env!("CARGO_BIN_EXE_tursodb"))
        .arg(":memory:")
        .arg("select 'one'; select 'two';")
        .status()
        .expect("failed to run tursodb");

    assert_eq!(status.code(), Some(0));
}

/// A2: Parse/prepare failure returns non-zero
#[test]
fn sql_argument_returns_exit_code_one_on_query_failure() {
    let status = Command::new(env!("CARGO_BIN_EXE_tursodb"))
        .arg(":memory:")
        .arg("select 'one'; select * from t; select 'two';")
        .status()
        .expect("failed to run tursodb");

    assert_eq!(status.code(), Some(1));
}

/// A3: Fail-fast on parse/prepare failure — statements after error do not execute
#[test]
fn sql_argument_stops_execution_after_first_error() {
    let output = Command::new(env!("CARGO_BIN_EXE_tursodb"))
        .arg(":memory:")
        .arg("select 'one'; select * from t; select 'two';")
        .output()
        .expect("failed to run tursodb");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("one"), "first query should execute");
    assert!(
        !stdout.contains("two"),
        "query after error should not execute"
    );
    assert_eq!(output.status.code(), Some(1));
}

/// A4: Runtime/step failure (constraint violation) returns non-zero
#[test]
fn sql_argument_runtime_error_returns_nonzero() {
    let sql = "create table t(x integer primary key); \
               insert into t values(1); \
               insert into t values(1); \
               select 'after';";
    let status = Command::new(env!("CARGO_BIN_EXE_tursodb"))
        .arg(":memory:")
        .arg(sql)
        .status()
        .expect("failed to run tursodb");

    assert_eq!(status.code(), Some(1));
}

/// A5: Runtime/step failure does NOT stop the rest of the line — matches
/// sqlite3, unlike a prepare/parse failure (A3), which does.
/// Regression test for https://github.com/tursodatabase/turso/issues/8256
#[test]
fn sql_argument_runtime_error_does_not_stop_execution() {
    let sql = "create table t(x integer primary key); \
               insert into t values(1); \
               insert into t values(1); \
               select 'after';";
    let output = Command::new(env!("CARGO_BIN_EXE_tursodb"))
        .arg(":memory:")
        .arg(sql)
        .output()
        .expect("failed to run tursodb");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("after"),
        "query after a runtime error on the same line should still execute, like sqlite3, stdout: {stdout}"
    );
    // The run still had an error, so the exit code must still be non-zero.
    assert_eq!(output.status.code(), Some(1));
}

/// A5b: The issue's own reproducer — an integer-overflow runtime error must
/// not swallow the rest of the line.
/// Regression test for https://github.com/tursodatabase/turso/issues/8256
#[test]
fn sql_argument_runtime_error_reproducer_from_issue_8256() {
    let sql = "SELECT abs(-9223372036854775808); SELECT 'after';";
    let output = Command::new(env!("CARGO_BIN_EXE_tursodb"))
        .arg(":memory:")
        .arg(sql)
        .output()
        .expect("failed to run tursodb");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("after"),
        "query after the overflow error should still execute, stdout: {stdout}"
    );
    assert_eq!(output.status.code(), Some(1));
}

/// A5c: A single-line `BEGIN; ...; COMMIT;` must still reach COMMIT after an
/// earlier statement fails at runtime -- the concrete consequence called out
/// in issue #8256 (losing COMMIT silently rolls the transaction back on exit).
#[test]
fn sql_argument_runtime_error_mid_transaction_still_reaches_commit() {
    let sql = "create table t(x integer primary key); \
               begin; \
               insert into t values(1); \
               insert into t values(1); \
               insert into t values(2); \
               commit;";
    let status = Command::new(env!("CARGO_BIN_EXE_tursodb"))
        .arg(":memory:")
        .arg(sql)
        .status()
        .expect("failed to run tursodb");
    assert_eq!(status.code(), Some(1));

    // Re-open the same on-disk database and confirm the successful insert
    // before the failed one was committed, not rolled back.
    let db_path = std::env::temp_dir().join(format!(
        "limbo_test_runtime_error_commit_{}.db",
        std::process::id()
    ));
    std::fs::remove_file(&db_path).ok();

    let setup_sql = "create table t(x integer primary key); \
                      begin; \
                      insert into t values(1); \
                      insert into t values(1); \
                      insert into t values(2); \
                      commit;";
    Command::new(env!("CARGO_BIN_EXE_tursodb"))
        .arg(&db_path)
        .arg(setup_sql)
        .status()
        .expect("failed to run tursodb");

    let output = Command::new(env!("CARGO_BIN_EXE_tursodb"))
        .arg(&db_path)
        .arg("select x from t order by x;")
        .output()
        .expect("failed to run tursodb");
    std::fs::remove_file(&db_path).ok();

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains('1') && stdout.contains('2'),
        "both successful inserts must have been committed, stdout: {stdout}"
    );
}

/// A6: Syntax error returns non-zero
#[test]
fn sql_argument_syntax_error_returns_nonzero() {
    let status = Command::new(env!("CARGO_BIN_EXE_tursodb"))
        .arg(":memory:")
        .arg("select from;")
        .status()
        .expect("failed to run tursodb");

    assert_eq!(status.code(), Some(1));
}

/// A7: Empty SQL string returns 0
#[test]
fn sql_argument_empty_string_returns_zero() {
    let status = Command::new(env!("CARGO_BIN_EXE_tursodb"))
        .arg(":memory:")
        .arg("")
        .status()
        .expect("failed to run tursodb");

    assert_eq!(status.code(), Some(0));
}

/// A8: sqlite_dbpage updates require unsafe testing mode
#[test]
fn sqlite_dbpage_update_requires_unsafe_testing() {
    let sql = "create table t(x); update sqlite_dbpage set data = data where pgno = 1; select 'after_update';";
    let output = Command::new(env!("CARGO_BIN_EXE_tursodb"))
        .arg(":memory:")
        .arg(sql)
        .output()
        .expect("failed to run tursodb");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        !stdout.contains("after_update"),
        "query after sqlite_dbpage update should not execute without unsafe testing"
    );
    assert_eq!(output.status.code(), Some(1));
}

/// A9: sqlite_dbpage updates succeed with unsafe testing mode
#[test]
fn sqlite_dbpage_update_allows_unsafe_testing() {
    let sql = "create table t(x); update sqlite_dbpage set data = data where pgno = 1; select 'after_update';";
    let output = Command::new(env!("CARGO_BIN_EXE_tursodb"))
        .arg("--unsafe-testing")
        .arg(":memory:")
        .arg(sql)
        .output()
        .expect("failed to run tursodb");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("after_update"),
        "expected query after update to run"
    );
    assert_eq!(output.status.code(), Some(0));
}

// ---------------------------------------------------------------------------
// B. Piped stdin mode
// ---------------------------------------------------------------------------

/// B8: Success path returns 0
#[test]
fn piped_stdin_returns_exit_code_zero_on_success() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_tursodb"))
        .arg(":memory:")
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to run tursodb");

    let mut stdin = child.stdin.take().unwrap();
    stdin.write_all(b"select 1;\n").unwrap();
    drop(stdin);

    let status = child.wait().expect("failed to wait");
    assert_eq!(status.code(), Some(0));
}

/// B9: Parse/prepare failure returns non-zero
#[test]
fn piped_stdin_returns_exit_code_one_on_query_failure() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_tursodb"))
        .arg(":memory:")
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to run tursodb");

    let mut stdin = child.stdin.take().unwrap();
    stdin.write_all(b"select * from nonexistent;\n").unwrap();
    drop(stdin);

    let status = child.wait().expect("failed to wait");
    assert_eq!(status.code(), Some(1));
}

/// B10: Fail-fast in piped multi-statement failure
#[test]
fn piped_stdin_stops_execution_after_first_error() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_tursodb"))
        .arg(":memory:")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to run tursodb");

    let mut stdin = child.stdin.take().unwrap();
    stdin
        .write_all(b"select 'one'; select * from missing; select 'two';\n")
        .unwrap();
    drop(stdin);

    let output = child.wait_with_output().expect("failed to wait");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("one"), "first query should execute");
    assert!(
        !stdout.contains("two"),
        "query after error should not execute"
    );
    assert_eq!(output.status.code(), Some(1));
}

/// B11: Runtime/step failure in piped mode returns non-zero
#[test]
fn piped_stdin_runtime_error_returns_nonzero() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_tursodb"))
        .arg(":memory:")
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to run tursodb");

    let mut stdin = child.stdin.take().unwrap();
    stdin
        .write_all(
            b"create table t(x integer primary key);\n\
              insert into t values(1);\n\
              insert into t values(1);\n",
        )
        .unwrap();
    drop(stdin);

    let status = child.wait().expect("failed to wait");
    assert_eq!(status.code(), Some(1));
}

/// C1: .read handles multi-line CREATE TRIGGER correctly
#[test]
fn dot_read_handles_trigger_statements() {
    let sql = "\
CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);\n\
CREATE TABLE log(msg TEXT);\n\
CREATE TRIGGER tr1 AFTER INSERT ON t BEGIN\n\
    INSERT INTO log VALUES ('inserted ' || NEW.val);\n\
END;\n\
INSERT INTO t VALUES (1, 'hello');\n\
SELECT msg FROM log;\n";

    let sql_path = std::env::temp_dir().join("limbo_test_dot_read_trigger.sql");
    std::fs::write(&sql_path, sql).expect("failed to write sql file");

    let dot_read = format!(".read {}", sql_path.display());
    let mut child = Command::new(env!("CARGO_BIN_EXE_tursodb"))
        .arg(":memory:")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to run tursodb");

    let mut stdin = child.stdin.take().unwrap();
    stdin.write_all(dot_read.as_bytes()).unwrap();
    stdin.write_all(b"\n").unwrap();
    drop(stdin);

    let output = child.wait_with_output().expect("failed to wait");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    std::fs::remove_file(&sql_path).ok();

    assert!(
        !stderr.contains("incomplete input"),
        "trigger should not produce parse errors, stderr: {stderr}"
    );
    assert!(
        !stderr.contains("no such column"),
        "NEW.val should be resolved inside trigger, stderr: {stderr}"
    );
    assert!(
        stdout.contains("inserted hello"),
        "trigger should fire and insert into log, stdout: {stdout}"
    );
}

/// B12: Empty piped stdin returns 0
#[test]
fn piped_stdin_empty_returns_zero() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_tursodb"))
        .arg(":memory:")
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to run tursodb");

    // Close stdin immediately — no input
    drop(child.stdin.take());

    let status = child.wait().expect("failed to wait");
    assert_eq!(status.code(), Some(0));
}
