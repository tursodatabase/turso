use std::io::Write;
use std::process::{Command, Output, Stdio};

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

fn run_cli_with_piped_input(input: &[u8]) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_tursodb"))
        .arg(":memory:")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to run tursodb");

    child
        .stdin
        .take()
        .expect("piped stdin must be available")
        .write_all(input)
        .expect("failed to write CLI input");

    child.wait_with_output().expect("failed to wait")
}

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

    let temp_dir = tempfile::Builder::new()
        .prefix("turso-dot-read-trigger-")
        .tempdir()
        .expect("failed to create temp dir");
    let sql_path = temp_dir.path().join("trigger.sql");
    std::fs::write(&sql_path, sql).expect("failed to write sql file");

    let script = format!(".read \"{}\"\n", sql_path.display());
    let output = run_cli_with_piped_input(script.as_bytes());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

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

#[test]
fn dot_read_handles_quoted_path_with_spaces() {
    let temp_dir = tempfile::Builder::new()
        .prefix("turso-dot-read-")
        .tempdir()
        .expect("failed to create temp dir");
    let sql_path = temp_dir.path().join("input with spaces.sql");
    std::fs::write(&sql_path, "SELECT 'read succeeded';\n").expect("failed to write sql file");

    let script = format!(".read \"{}\"\n", sql_path.display());
    let output = run_cli_with_piped_input(script.as_bytes());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert_eq!(output.status.code(), Some(0), "stderr: {stderr}");
    assert!(stdout.contains("read succeeded"), "stdout: {stdout}");
}

#[test]
fn dot_open_handles_quoted_path_with_spaces() {
    let temp_dir = tempfile::Builder::new()
        .prefix("turso-dot-open-")
        .tempdir()
        .expect("failed to create temp dir");
    let db_path = temp_dir.path().join("database with spaces.db");

    let script = format!(
        ".open \"{}\"\nCREATE TABLE t(value TEXT);\nINSERT INTO t VALUES ('open succeeded');\nSELECT value FROM t;\n",
        db_path.display()
    );
    let output = run_cli_with_piped_input(script.as_bytes());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert_eq!(output.status.code(), Some(0), "stderr: {stderr}");
    assert!(stdout.contains("open succeeded"), "stdout: {stdout}");
}

/// `.open` reads a database URI the same way as the database argument of
/// tursodb: the query selects the cipher and the key, and it is not a part
/// of the file name. In the `path?key=value` form, a `#` or `%` before the
/// query is a part of the file name.
#[test]
fn dot_open_applies_encryption_uri_parameters() {
    let hexkey = "b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327";
    let query = format!("cipher=aegis256&hexkey={hexkey}");
    let marker = "plaintext-marker";
    // The name that .open gets before the query, the file that .open must
    // create, and the path of that file in a `file:` URI.
    for (name, file_name, uri_path) in [
        ("file:secret.db", "secret.db", "secret.db"),
        ("secret.db", "secret.db", "secret.db"),
        ("notes#2.db", "notes#2.db", "notes%232.db"),
        ("100%41.db", "100%41.db", "100%2541.db"),
    ] {
        let name = format!("{name}?{query}");
        let temp_dir = tempfile::Builder::new()
            .prefix("turso-dot-open-uri-")
            .tempdir()
            .expect("failed to create temp dir");
        let dir = temp_dir.path();

        let script = format!(
            ".open \"{name}\"\nCREATE TABLE t(v TEXT);\nINSERT INTO t VALUES ('{marker}');\n"
        );
        let output = run_cli_in_dir(dir, &["--experimental-encryption"], &script);
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(
            output.status.code(),
            Some(0),
            "{name}: stdout: {stdout} stderr: {stderr}"
        );

        let file_names: Vec<String> = std::fs::read_dir(dir)
            .expect("failed to list temp dir")
            .map(|entry| {
                let entry = entry.expect("failed to read dir entry");
                entry.file_name().to_string_lossy().into_owned()
            })
            .collect();
        assert!(
            file_names
                .iter()
                .all(|file_name| !file_name.contains("hexkey")),
            "{name}: a file name holds the URI query and the key: {file_names:?}"
        );
        for file_name in &file_names {
            let contents = std::fs::read(dir.join(file_name)).expect("failed to read file");
            assert!(
                !contents
                    .windows(marker.len())
                    .any(|window| window == marker.as_bytes()),
                "{name}: {file_name} holds the row in plaintext"
            );
        }
        let db = std::fs::read(dir.join(file_name))
            .unwrap_or_else(|e| panic!("{name}: failed to read {file_name}: {e}: {file_names:?}"));
        assert!(
            db.len() >= 16 && &db[..16] != b"SQLite format 3\0",
            "{name}: {file_name} does not start with an encrypted header"
        );

        let uri = format!("file:{uri_path}?{query}");
        let output = run_cli_in_dir(
            dir,
            &[
                "--experimental-encryption",
                "-m",
                "list",
                &uri,
                "SELECT v FROM t;",
            ],
            "",
        );
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(
            stdout.lines().collect::<Vec<_>>(),
            vec![marker],
            "{name}: stderr: {stderr}"
        );
    }
}

/// `.open` reports a URI with incorrect encryption parameters, and it does
/// not create a file.
#[test]
fn dot_open_reports_invalid_encryption_uri() {
    let hexkey = "b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327";
    for (args, name, error) in [
        (
            &["--experimental-encryption", "-m", "list"][..],
            "file:secret.db?cipher=aegis256".to_string(),
            "hexkey is required when cipher is provided",
        ),
        (
            &["-m", "list"][..],
            format!("file:secret.db?cipher=aegis256&hexkey={hexkey}"),
            "encryption is an opt in feature",
        ),
    ] {
        let temp_dir = tempfile::Builder::new()
            .prefix("turso-dot-open-bad-uri-")
            .tempdir()
            .expect("failed to create temp dir");
        let dir = temp_dir.path();

        let output = run_cli_in_dir(dir, args, &format!(".open \"{name}\"\n"));
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stdout.lines().any(|line| line == error),
            "{name}: stdout: {stdout} stderr: {stderr}"
        );
        let entries: Vec<_> = std::fs::read_dir(dir)
            .expect("failed to list temp dir")
            .collect();
        assert!(entries.is_empty(), "{name}: .open created {entries:?}");
    }
}

/// `.open` does not accept a VFS argument together with a URI. The shell
/// keeps the database that is open, and no file is created.
#[test]
fn dot_open_rejects_vfs_argument_with_database_uri() {
    let hexkey = "b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327";
    let temp_dir = tempfile::Builder::new()
        .prefix("turso-dot-open-uri-vfs-")
        .tempdir()
        .expect("failed to create temp dir");
    let dir = temp_dir.path();

    let script = format!(
        ".open \"file:secret.db?cipher=aegis256&hexkey={hexkey}\" syscall\nSELECT 'still open';\n"
    );
    let output = run_cli_in_dir(dir, &["--experimental-encryption", "-m", "list"], &script);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(
        stdout.lines().collect::<Vec<_>>(),
        vec![
            "a VFS argument cannot be used with a database URI",
            "still open"
        ],
        "stderr: {stderr}"
    );
    let entries: Vec<_> = std::fs::read_dir(dir)
        .expect("failed to list temp dir")
        .collect();
    assert!(entries.is_empty(), ".open created {entries:?}");
}

/// `.open` with a VFS argument opens a plain path with that VFS.
#[test]
fn dot_open_with_vfs_argument_opens_plain_path() {
    let temp_dir = tempfile::Builder::new()
        .prefix("turso-dot-open-vfs-")
        .tempdir()
        .expect("failed to create temp dir");
    let dir = temp_dir.path();

    let script = ".open plain.db memory\nCREATE TABLE t(v TEXT);\nINSERT INTO t VALUES ('memory vfs');\nSELECT v FROM t;\n";
    let output = run_cli_in_dir(dir, &["-m", "list"], script);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(
        stdout.lines().collect::<Vec<_>>(),
        vec!["memory vfs"],
        "stderr: {stderr}"
    );
    let entries: Vec<_> = std::fs::read_dir(dir)
        .expect("failed to list temp dir")
        .collect();
    assert!(entries.is_empty(), "the memory VFS created {entries:?}");
}

/// Runs tursodb with `args` in the directory `dir`, writes `input` to its
/// stdin, and waits until it exits.
fn run_cli_in_dir(dir: &std::path::Path, args: &[&str], input: &str) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_tursodb"))
        .args(args)
        .current_dir(dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to run tursodb");

    child
        .stdin
        .take()
        .expect("piped stdin must be available")
        .write_all(input.as_bytes())
        .expect("failed to write CLI input");

    child.wait_with_output().expect("failed to wait")
}

#[test]
fn dot_import_handles_quoted_csv_path_with_spaces() {
    let temp_dir = tempfile::Builder::new()
        .prefix("turso-dot-import-")
        .tempdir()
        .expect("failed to create temp dir");
    let csv_path = temp_dir.path().join("people data.csv");
    std::fs::write(&csv_path, "name\nAda\n").expect("failed to write csv file");

    let script = format!(
        ".import \"{}\" people\nSELECT name FROM people;\n",
        csv_path.display()
    );
    let output = run_cli_with_piped_input(script.as_bytes());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert_eq!(output.status.code(), Some(0), "stderr: {stderr}");
    assert!(
        stdout.contains("Ada"),
        "imported row should be queryable, stdout: {stdout}"
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
