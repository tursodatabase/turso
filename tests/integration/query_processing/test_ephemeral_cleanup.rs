#![cfg(feature = "test_helper")]

use crate::common::{limbo_exec_rows, TempDatabase};
use rusqlite::types::Value;
use std::path::PathBuf;
use turso_core::StepResult;

#[test]
fn test_ephemeral_temp_files_cleaned_up() {
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

    let mut stmt = conn
        .prepare("SELECT x FROM t_eph UNION SELECT x FROM t_eph")
        .unwrap();
    let mut rows: Vec<i64> = Vec::new();
    let mut temp_file_dirs: Vec<PathBuf> = Vec::new();
    loop {
        match stmt.step().unwrap() {
            StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => db.io.step().unwrap(),
            StepResult::Row => {
                if rows.is_empty() {
                    temp_file_dirs = stmt.ephemeral_temp_file_dirs();
                    assert!(
                        !temp_file_dirs.is_empty(),
                        "expected the ephemeral cursor to own a temp file while the query runs"
                    );
                    for dir in &temp_file_dirs {
                        assert!(
                            dir.exists(),
                            "temp file directory {dir:?} was never created"
                        );
                    }
                }
                rows.push(stmt.row().unwrap().get::<i64>(0).unwrap());
            }
            StepResult::Done => break,
            StepResult::Interrupt | StepResult::Busy => panic!("query did not run to completion"),
        }
    }
    assert_eq!(rows, vec![1, 2, 3]);

    // Drop statement, connection and database so all ProgramState instances are dropped,
    // which should clean up any TempFile entries.
    drop(stmt);
    drop(conn);
    drop(db);

    let leaked: Vec<_> = temp_file_dirs
        .into_iter()
        .filter(|dir| dir.exists())
        .collect();
    assert!(leaked.is_empty(), "Ephemeral temp files leaked: {leaked:?}");
}

fn value_as_text(value: &Value) -> Option<&str> {
    match value {
        Value::Text(v) => Some(v.as_str()),
        _ => None,
    }
}
