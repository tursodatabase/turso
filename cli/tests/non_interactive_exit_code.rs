use std::io::Write;
use std::process::{Command, Stdio};

#[test]
fn sql_argument_returns_exit_code_one_on_query_failure() {
    let status = Command::new(env!("CARGO_BIN_EXE_tursodb"))
        .arg(":memory:")
        .arg("select 'one'; select * from t; select 'two';")
        .status()
        .expect("failed to run tursodb");

    assert_eq!(status.code(), Some(1));
}

#[test]
fn sql_argument_returns_exit_code_zero_on_success() {
    let status = Command::new(env!("CARGO_BIN_EXE_tursodb"))
        .arg(":memory:")
        .arg("select 'one'; select 'two';")
        .status()
        .expect("failed to run tursodb");

    assert_eq!(status.code(), Some(0));
}

#[test]
fn sql_argument_stops_execution_after_first_error() {
    let output = Command::new(env!("CARGO_BIN_EXE_tursodb"))
        .arg(":memory:")
        .arg("select 'one'; select * from t; select 'two';")
        .output()
        .expect("failed to run tursodb");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("one"), "first query should execute");
    assert!(!stdout.contains("two"), "query after error should not execute");
    assert_eq!(output.status.code(), Some(1));
}

#[test]
fn piped_stdin_returns_exit_code_one_on_query_failure() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_tursodb"))
        .arg(":memory:")
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to run tursodb");

    child
        .stdin
        .take()
        .unwrap()
        .write_all(b"select * from nonexistent;\n")
        .unwrap();

    let status = child.wait().expect("failed to wait");
    assert_eq!(status.code(), Some(1));
}

#[test]
fn piped_stdin_returns_exit_code_zero_on_success() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_tursodb"))
        .arg(":memory:")
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to run tursodb");

    child
        .stdin
        .take()
        .unwrap()
        .write_all(b"select 1;\n")
        .unwrap();

    let status = child.wait().expect("failed to wait");
    assert_eq!(status.code(), Some(0));
}
