use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

fn temp_test_path(name: &str) -> PathBuf {
    static NEXT_ID: AtomicUsize = AtomicUsize::new(0);
    let unique = NEXT_ID.fetch_add(1, Ordering::SeqCst);
    std::env::temp_dir().join(format!("turso-{name}-{}-{unique}", std::process::id()))
}

fn dot_command_path_arg(path: &Path) -> String {
    let path = path.display().to_string();
    let mut arg = String::with_capacity(path.len() + 2);
    arg.push('"');
    for c in path.chars() {
        if matches!(c, '\\' | '"') {
            arg.push('\\');
        }
        arg.push(c);
    }
    arg.push('"');
    arg
}

fn run_piped_script(script: &str) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_tursodb"))
        .arg(":memory:")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to run tursodb");

    let mut stdin = child.stdin.take().unwrap();
    stdin.write_all(script.as_bytes()).unwrap();
    drop(stdin);

    child.wait_with_output().expect("failed to wait")
}

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
    let output = run_piped_script("select 1;\n");
    assert_eq!(output.status.code(), Some(0));
}

/// B9: Parse/prepare failure returns non-zero
#[test]
fn piped_stdin_returns_exit_code_one_on_query_failure() {
    let output = run_piped_script("select * from nonexistent;\n");
    assert_eq!(output.status.code(), Some(1));
}

/// B10: Fail-fast in piped multi-statement failure
#[test]
fn piped_stdin_stops_execution_after_first_error() {
    let output = run_piped_script("select 'one'; select * from missing; select 'two';\n");
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
    let output = run_piped_script(
        "create table t(x integer primary key);\n\
         insert into t values(1);\n\
         insert into t values(1);\n",
    );
    assert_eq!(output.status.code(), Some(1));
}

#[test]
fn piped_stdin_import_row_error_returns_nonzero_and_keeps_successful_rows() {
    let csv_path = temp_test_path("unique-import.csv");
    std::fs::write(&csv_path, "a\na\nb\n").expect("failed to write csv");
    let script = format!(
        ".mode list\nCREATE TABLE t(x UNIQUE);\n.import --csv {} t\nSELECT count(*) FROM t;\n",
        dot_command_path_arg(&csv_path)
    );
    let output = run_piped_script(&script);
    let _ = std::fs::remove_file(&csv_path);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(stdout, "2\n");
    assert_eq!(
        stderr,
        format!(
            "{}:2: INSERT failed: UNIQUE constraint failed: t.x (19)\n",
            csv_path.display()
        )
    );
    assert_eq!(output.status.code(), Some(1));
}

#[test]
fn piped_stdin_dot_command_parse_error_returns_nonzero_and_continues() {
    let csv_path = temp_test_path("bad-import-option.csv");
    std::fs::write(&csv_path, "1\n").expect("failed to write csv");
    let script = format!(
        ".mode list\n.import --badopt {} t\nSELECT 1;\n",
        dot_command_path_arg(&csv_path)
    );
    let output = run_piped_script(&script);
    let _ = std::fs::remove_file(&csv_path);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(stdout, "1\n");
    assert!(
        stderr.contains("unexpected argument '--badopt'"),
        "expected clap parse error, got: {stderr}"
    );
    assert_eq!(output.status.code(), Some(1));
}

#[test]
fn piped_stdin_dot_command_help_returns_zero() {
    let output = run_piped_script(".schema --help\n");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Display schema for a table"),
        "expected .schema help, got: {stdout}"
    );
    assert_eq!(output.status.code(), Some(0));
}

#[test]
fn top_level_help_returns_zero() {
    let output = Command::new(env!("CARGO_BIN_EXE_tursodb"))
        .arg("--help")
        .output()
        .expect("failed to run tursodb");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("The Turso interactive SQL shell"),
        "expected top-level help, got: {stdout}"
    );
    assert_eq!(output.status.code(), Some(0));
}

#[test]
fn piped_stdin_parameter_error_returns_nonzero_and_continues() {
    let output = run_piped_script(".mode list\n.parameter set x 41\nSELECT 1;\n");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("parameter name must start with one of"));
    assert!(
        stdout.ends_with("1\n"),
        "SELECT after dot error should run: {stdout}"
    );
    assert_eq!(output.status.code(), Some(1));
}

#[test]
fn piped_stdin_read_missing_file_reports_stderr_and_continues() {
    let missing_path = temp_test_path("missing-read.sql");
    let script = format!(
        ".mode list\n.read {}\nSELECT 1;\n",
        dot_command_path_arg(&missing_path)
    );
    let output = run_piped_script(&script);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(stdout, "1\n");
    assert!(
        stderr.contains(&format!(
            "Error: cannot open \"{}\"",
            missing_path.display()
        )),
        "expected .read error on stderr, got: {stderr:?}"
    );
    assert_eq!(output.status.code(), Some(1));
}

#[cfg(unix)]
#[test]
fn ctrl_c_interrupts_query_after_dot_open() {
    let db_path = temp_test_path("interrupt-after-open.db");
    let script = format!(
        ".mode list\n.open {}\nSELECT * FROM generate_series(1, 1000000000);\n",
        db_path.display()
    );
    let mut child = Command::new(env!("CARGO_BIN_EXE_tursodb"))
        .arg(":memory:")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to run tursodb");

    let mut stdin = child.stdin.take().unwrap();
    stdin.write_all(script.as_bytes()).unwrap();
    drop(stdin);

    let stdout = child.stdout.take().unwrap();
    let (query_started_tx, query_started_rx) = mpsc::channel();
    let stdout_reader = std::thread::spawn(move || {
        let mut reader = BufReader::new(stdout);
        let mut stdout = String::new();
        let mut line = String::new();
        while reader.read_line(&mut line)? != 0 {
            if line.trim_end() == "1" {
                let _ = query_started_tx.send(());
            }
            stdout.push_str(&line);
            line.clear();
        }
        Ok::<_, std::io::Error>(stdout)
    });

    if let Err(e) = query_started_rx.recv_timeout(Duration::from_secs(3)) {
        let _ = child.kill();
        let _ = child.wait();
        let stdout = stdout_reader
            .join()
            .expect("stdout reader thread panicked")
            .expect("failed to read stdout");
        let mut stderr = String::new();
        child
            .stderr
            .take()
            .unwrap()
            .read_to_string(&mut stderr)
            .expect("failed to read stderr");
        panic!("query did not start before SIGINT: {e}; stdout={stdout:?}, stderr={stderr:?}");
    }

    unsafe {
        libc::kill(child.id() as libc::pid_t, libc::SIGINT);
    }

    let deadline = Instant::now() + Duration::from_secs(3);
    let status = loop {
        if let Some(status) = child.try_wait().expect("failed to poll child") {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            let stdout = stdout_reader
                .join()
                .expect("stdout reader thread panicked")
                .expect("failed to read stdout");
            let mut stderr = String::new();
            child
                .stderr
                .take()
                .unwrap()
                .read_to_string(&mut stderr)
                .expect("failed to read stderr");
            panic!("query did not exit after SIGINT; stdout={stdout:?}, stderr={stderr:?}");
        }
        std::thread::sleep(Duration::from_millis(10));
    };

    let stdout = stdout_reader
        .join()
        .expect("stdout reader thread panicked")
        .expect("failed to read stdout");
    let mut stderr = String::new();
    child
        .stderr
        .take()
        .unwrap()
        .read_to_string(&mut stderr)
        .expect("failed to read stderr");
    let _ = std::fs::remove_file(db_path);
    let combined = format!("{stdout}{stderr}");
    assert_eq!(status.code(), Some(1));
    assert!(
        combined.to_ascii_lowercase().contains("interrupt"),
        "expected interrupt diagnostic, got: {combined:?}"
    );
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

    let sql_path = temp_test_path("dot-read-trigger.sql");
    std::fs::write(&sql_path, sql).expect("failed to write sql file");

    let dot_read = format!(".read {}", dot_command_path_arg(&sql_path));
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
