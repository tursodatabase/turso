use crate::common::{limbo_exec_rows, TempDatabase};
use rusqlite::types::Value;
use std::time::Duration;
use turso_core::vdbe::StepResult;

fn run_until_terminal(stmt: &mut turso_core::Statement) -> turso_core::Result<StepResult> {
    loop {
        match stmt.step()? {
            StepResult::IO => stmt._io().step()?,
            StepResult::Row | StepResult::Yield => continue,
            result => return Ok(result),
        }
    }
}

#[turso_macros::test]
fn query_timeout_milliseconds(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    assert_eq!(conn.get_query_timeout_ms(), 0u64);

    conn.set_query_timeout(Duration::from_micros(123_456));
    assert_eq!(conn.get_query_timeout_ms(), 123u64);

    conn.set_query_timeout(Duration::MAX);
    assert_eq!(conn.get_query_timeout_ms(), u64::MAX);

    conn.set_query_timeout(Duration::ZERO);
    assert_eq!(conn.get_query_timeout_ms(), 0u64);
}

#[turso_macros::test]
fn query_timeout_interrupts_long_running_query(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t(x INTEGER);")?;
    for i in 0..200 {
        conn.execute(format!("INSERT INTO t VALUES ({i});"))?;
    }
    conn.set_query_timeout(Duration::from_millis(10));

    let mut stmt = conn.prepare("SELECT a.x FROM t a, t b, t c, t d, t e;")?;
    let result = run_until_terminal(&mut stmt)?;
    assert!(
        matches!(result, StepResult::Interrupt),
        "expected interrupt, got {result:?}"
    );
    Ok(())
}

#[turso_macros::test]
fn query_timeout_allows_short_running_query(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.set_query_timeout(Duration::from_millis(10));

    let mut stmt = conn.prepare("SELECT 1 AS value;")?;
    let result = run_until_terminal(&mut stmt)?;
    assert!(
        matches!(result, StepResult::Done),
        "expected done, got {result:?}"
    );
    Ok(())
}

const SLOW_JOIN: &str = "SELECT count(*) FROM tasks a, tasks b, tasks c";

fn create_tasks(conn: &std::sync::Arc<turso_core::Connection>) -> anyhow::Result<()> {
    conn.execute("CREATE TABLE tasks(id INTEGER PRIMARY KEY)")?;
    for i in 0..200 {
        conn.execute(format!("INSERT INTO tasks VALUES ({i})"))?;
    }
    Ok(())
}

#[turso_macros::test(mvcc)]
fn query_timeout_interrupts_trigger_body(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    create_tasks(&conn)?;
    conn.execute("CREATE TABLE outbox(id INTEGER PRIMARY KEY, v INTEGER)")?;
    conn.execute("INSERT INTO outbox VALUES (1, 0)")?;
    conn.execute(format!(
        "CREATE TRIGGER slow BEFORE UPDATE ON outbox BEGIN {SLOW_JOIN}; END"
    ))?;
    conn.execute("BEGIN")?;
    conn.execute("INSERT INTO outbox VALUES (2, 0)")?;
    conn.set_query_timeout(Duration::from_millis(10));

    let mut stmt = conn.prepare("UPDATE outbox SET v = v + 1")?;
    let result = run_until_terminal(&mut stmt)?;
    assert!(
        matches!(result, StepResult::Interrupt),
        "expected interrupt, got {result:?}"
    );
    drop(stmt);

    conn.set_query_timeout(Duration::ZERO);
    conn.execute("COMMIT")?;
    assert_eq!(
        limbo_exec_rows(&conn, "SELECT id, v FROM outbox ORDER BY id"),
        vec![
            vec![Value::Integer(1), Value::Integer(0)],
            vec![Value::Integer(2), Value::Integer(0)],
        ],
        "the interrupted UPDATE must change no row, and the INSERT before it must commit"
    );
    Ok(())
}

#[turso_macros::test]
fn query_timeout_interrupts_trigger_fired_by_foreign_key_action(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    create_tasks(&conn)?;
    conn.execute("PRAGMA foreign_keys = ON")?;
    conn.execute("CREATE TABLE parent(id INTEGER PRIMARY KEY)")?;
    conn.execute(
        "CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON DELETE CASCADE)",
    )?;
    conn.execute("INSERT INTO parent VALUES (1)")?;
    conn.execute("INSERT INTO child VALUES (1, 1)")?;
    conn.execute(format!(
        "CREATE TRIGGER slow BEFORE DELETE ON child BEGIN {SLOW_JOIN}; END"
    ))?;
    conn.set_query_timeout(Duration::from_millis(10));

    let mut stmt = conn.prepare("DELETE FROM parent WHERE id = 1")?;
    let result = run_until_terminal(&mut stmt)?;
    assert!(
        matches!(result, StepResult::Interrupt),
        "expected interrupt, got {result:?}"
    );
    Ok(())
}

#[turso_macros::test]
fn connection_interrupt_inside_trigger_returns_interrupt(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    create_tasks(&conn)?;
    conn.execute("CREATE TABLE outbox(id INTEGER PRIMARY KEY, v INTEGER)")?;
    conn.execute("INSERT INTO outbox VALUES (1, 0)")?;
    conn.execute(format!(
        "CREATE TRIGGER slow BEFORE UPDATE ON outbox BEGIN {SLOW_JOIN}; END"
    ))?;
    // The UPDATE itself runs far fewer than 1000 instructions, so the
    // handler first runs, and calls interrupt(), inside the trigger.
    let conn_for_handler = conn.clone();
    conn.set_progress_handler(
        1000,
        Some(Box::new(move || {
            conn_for_handler.interrupt();
            false
        })),
    );

    let mut stmt = conn.prepare("UPDATE outbox SET v = v + 1 WHERE id = 1")?;
    let result = run_until_terminal(&mut stmt);
    conn.set_progress_handler(0, None);
    assert!(
        matches!(result, Ok(StepResult::Interrupt)),
        "expected interrupt, got {result:?}"
    );
    Ok(())
}
