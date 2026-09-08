use crate::common::{limbo_exec_rows, ExecRows, TempDatabase};
use rusqlite::types::Value;
use std::path::{Path, PathBuf};
use std::process::Command;

const EPHEMERAL_CLEANUP_CHILD_TEST: &str =
    "query_processing::test_ephemeral_cleanup::ephemeral_temp_files_cleaned_up_child_process";

/// Directory the child process scans, also handed to it as its system temp dir.
const EPHEMERAL_CLEANUP_TEMP_DIR: &str = "TURSO_EPHEMERAL_CLEANUP_TEMP_DIR";

/// Find directories that contain a `tursodb_temp_file` — these are leaked TempFiles.
fn find_leaked_temp_files(temp_dir: &Path) -> Vec<PathBuf> {
    std::fs::read_dir(temp_dir)
        .expect("failed to read temp dir")
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.is_dir() && p.join("tursodb_temp_file").exists())
        .collect()
}

fn value_as_text(value: &Value) -> Option<&str> {
    match value {
        Value::Text(v) => Some(v.as_str()),
        _ => None,
    }
}

/// `TempFile` puts its directory in `std::env::temp_dir()`, which every test on
/// the machine shares, so scanning that directory here would report the live
/// ephemeral files of tests running alongside this one as leaks. Do the scan in
/// a child process that has its temp dir to itself instead.
#[test]
fn test_ephemeral_temp_files_cleaned_up() {
    let temp_dir = tempfile::tempdir().unwrap();
    let current_exe = std::env::current_exe().unwrap();
    let child_output = Command::new(&current_exe)
        .arg(EPHEMERAL_CLEANUP_CHILD_TEST)
        .arg("--exact")
        .arg("--nocapture")
        .env(EPHEMERAL_CLEANUP_TEMP_DIR, temp_dir.path())
        // `std::env::temp_dir()` reads TMPDIR on unix, TMP and TEMP on windows.
        .env("TMPDIR", temp_dir.path())
        .env("TMP", temp_dir.path())
        .env("TEMP", temp_dir.path())
        .output()
        .unwrap();

    // A filter that matches nothing still exits 0, so check the test really ran.
    let stdout = String::from_utf8_lossy(&child_output.stdout);
    assert!(
        child_output.status.success() && stdout.contains("1 passed"),
        "ephemeral cleanup child process failed: stdout={stdout}; stderr={}",
        String::from_utf8_lossy(&child_output.stderr)
    );
}

#[test]
fn ephemeral_temp_files_cleaned_up_child_process() {
    let Some(temp_dir) = std::env::var_os(EPHEMERAL_CLEANUP_TEMP_DIR) else {
        return;
    };
    let temp_dir = PathBuf::from(temp_dir);

    let db = TempDatabase::new_empty();
    let conn = db.connect_limbo();

    conn.execute("CREATE TABLE t_eph(x INTEGER)").unwrap();
    conn.execute("INSERT INTO t_eph VALUES(3),(1),(2),(1),(3)")
        .unwrap();

    //Sanity check to verify plan uses OpenEphemeral opcode
    let explain_rows = limbo_exec_rows(
        &conn,
        "EXPLAIN SELECT x FROM t_eph UNION SELECT x FROM t_eph",
    );

    let has_open_ephemeral = explain_rows.iter().any(|row| {
        row.get(1)
            .and_then(value_as_text)
            .is_some_and(|op| op == "OpenEphemeral")
    });
    assert!(
        has_open_ephemeral,
        "expected OpenEphemeral in EXPLAIN output"
    );

    let rows: Vec<(i64,)> = conn.exec_rows("SELECT x FROM t_eph UNION SELECT x FROM t_eph");
    assert_eq!(rows, vec![(1,), (2,), (3,)]);

    // Drop connection and database so all ProgramState instances are dropped,
    // which should clean up any TempFile entries.
    drop(conn);
    drop(db);

    let leaked = find_leaked_temp_files(&temp_dir);
    assert!(leaked.is_empty(), "Ephemeral temp files leaked: {leaked:?}");
}
