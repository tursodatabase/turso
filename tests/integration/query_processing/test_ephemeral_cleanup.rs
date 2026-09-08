use crate::common::{limbo_exec_rows, ExecRows, TempDatabase};
use rusqlite::types::Value;
use std::path::PathBuf;

fn value_as_text(value: &Value) -> Option<&str> {
    match value {
        Value::Text(v) => Some(v.as_str()),
        _ => None,
    }
}

/// `TempFile` puts its directory in `std::env::temp_dir()`, which every test on
/// the machine shares, so scanning that directory would report the live
/// ephemeral files of tests running alongside this one as leaks. Ask this
/// database's own IO which temp files it opened instead.
#[test]
fn test_ephemeral_temp_files_cleaned_up() {
    let db = TempDatabase::new_empty();
    let temp_file_dirs = db.temp_file_dirs.clone();
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

    let temp_file_dirs = temp_file_dirs.lock().unwrap();
    assert!(
        !temp_file_dirs.is_empty(),
        "expected the ephemeral table to open a temp file"
    );
    let leaked: Vec<&PathBuf> = temp_file_dirs.iter().filter(|dir| dir.exists()).collect();
    assert!(leaked.is_empty(), "Ephemeral temp files leaked: {leaked:?}");
}
