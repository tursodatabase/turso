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

/// A5: Fail-fast on runtime/step failure — statements after constraint violation do not execute
#[test]
fn sql_argument_runtime_error_stops_execution() {
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
        !stdout.contains("after"),
        "query after runtime error should not execute"
    );
    assert_eq!(output.status.code(), Some(1));
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
