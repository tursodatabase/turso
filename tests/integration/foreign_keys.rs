use crate::common::{limbo_exec_rows, TempDatabase};
use anyhow::Result;
use rusqlite::types::Value as SqlValue;
use std::sync::Arc;
use turso_core::{Connection, LimboError, Statement, StepResult};

fn create_self_referencing_chain(conn: &Arc<Connection>, len: usize) -> Result<()> {
    conn.execute("PRAGMA foreign_keys=ON")?;
    conn.execute(
        "CREATE TABLE t(
            id INTEGER PRIMARY KEY,
            parent_id INTEGER REFERENCES t(id) ON DELETE CASCADE
        )",
    )?;
    conn.execute("BEGIN")?;
    for id in 1..=len {
        let sql = if id == 1 {
            "INSERT INTO t VALUES (1, NULL)".to_string()
        } else {
            format!("INSERT INTO t VALUES ({id}, {})", id - 1)
        };
        conn.execute(&sql)?;
    }
    conn.execute("COMMIT")?;
    Ok(())
}

fn assert_table_count(conn: &Arc<Connection>, table: &str, expected: i64) {
    let rows = limbo_exec_rows(conn, &format!("SELECT count(*) FROM {table}"));
    assert_eq!(rows.len(), 1, "expected a single count row");
    assert_eq!(rows[0].len(), 1, "expected a single count column");
    match rows[0].first() {
        Some(SqlValue::Integer(actual)) => assert_eq!(*actual, expected),
        other => panic!("expected integer count result, got {other:?}"),
    }
}

fn assert_trigger_recursion_error(result: Result<(), LimboError>) {
    assert!(
        matches!(result, Err(LimboError::TooManyLevelsOfTriggerRecursion)),
        "expected bounded recursion error, got {result:?}"
    );
}

fn step_statement_to_completion(stmt: &mut Statement) -> Result<(), LimboError> {
    loop {
        match stmt.step()? {
            StepResult::Done => return Ok(()),
            StepResult::IO => stmt._io().step()?,
            StepResult::Row => {}
            StepResult::Busy => continue,
            StepResult::Interrupt => return Err(LimboError::Interrupt),
        }
    }
}

#[turso_macros::test()]
fn fk_cascade_delete_self_reference_honors_runtime_limit(tmp_db: TempDatabase) -> Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.set_trigger_recursion_limit(2);
    create_self_referencing_chain(&conn, 4)?;

    let result = conn.execute("DELETE FROM t WHERE id = 1");
    assert_trigger_recursion_error(result);
    assert_table_count(&conn, "t", 4);

    Ok(())
}

#[turso_macros::test()]
fn fk_cascade_prepare_is_not_limited_by_runtime_depth(tmp_db: TempDatabase) -> Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.set_trigger_recursion_limit(0);
    create_self_referencing_chain(&conn, 4)?;

    let mut stmt = conn.prepare("DELETE FROM t WHERE id = 1")?;
    let result = step_statement_to_completion(&mut stmt);
    assert_trigger_recursion_error(result);
    assert_table_count(&conn, "t", 4);

    Ok(())
}

#[turso_macros::test()]
fn fk_cascade_delete_self_reference_hits_default_depth_limit(tmp_db: TempDatabase) -> Result<()> {
    let conn = tmp_db.connect_limbo();
    let chain_len = (Connection::MAX_TRIGGER_RECURSION_DEPTH + 2) as usize;
    create_self_referencing_chain(&conn, chain_len)?;

    let result = conn.execute("DELETE FROM t WHERE id = 1");
    assert_trigger_recursion_error(result);
    assert_table_count(&conn, "t", chain_len as i64);

    Ok(())
}

#[turso_macros::test()]
fn fk_cascade_delete_two_table_cycle_deletes_both_rows(tmp_db: TempDatabase) -> Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("PRAGMA foreign_keys=OFF")?;
    conn.execute(
        "CREATE TABLE a(
            id INTEGER PRIMARY KEY,
            b_id INTEGER REFERENCES b(id) ON DELETE CASCADE
        )",
    )?;
    conn.execute(
        "CREATE TABLE b(
            id INTEGER PRIMARY KEY,
            a_id INTEGER REFERENCES a(id) ON DELETE CASCADE
        )",
    )?;
    conn.execute("INSERT INTO a VALUES (1, 1)")?;
    conn.execute("INSERT INTO b VALUES (1, 1)")?;
    conn.execute("PRAGMA foreign_keys=ON")?;

    conn.execute("DELETE FROM a WHERE id = 1")?;

    assert_table_count(&conn, "a", 0);
    assert_table_count(&conn, "b", 0);

    Ok(())
}
