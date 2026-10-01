use crate::common::TempDatabase;
use anyhow::Context;
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

#[turso_macros::test]
fn interrupt_rolls_back_explicit_write_transaction(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t(x INTEGER);")?;
    conn.execute("CREATE TABLE parent(x INTEGER PRIMARY KEY);")?;
    conn.execute(
        "CREATE TABLE child(
            x INTEGER REFERENCES parent(x) DEFERRABLE INITIALLY DEFERRED
        );",
    )?;
    conn.execute("PRAGMA foreign_keys = ON;")?;
    for i in 0..10 {
        conn.execute(format!("INSERT INTO t VALUES ({i});"))?;
    }
    conn.execute("BEGIN;")?;
    conn.execute("SAVEPOINT before_interrupt;")?;
    conn.execute("CREATE TEMP TABLE temp_rolled_back(x INTEGER);")?;
    conn.execute("INSERT INTO t VALUES (99);")?;
    conn.execute("INSERT INTO child VALUES (1);")?;
    conn.set_query_timeout(Duration::from_millis(10));

    let mut stmt =
        conn.prepare("INSERT INTO t SELECT a.x FROM t a, t b, t c, t d, t e, t f, t g, t h;")?;
    let result = run_until_terminal(&mut stmt)?;
    assert!(matches!(result, StepResult::Interrupt));
    assert!(!stmt.is_busy());
    drop(stmt);
    assert!(conn.get_auto_commit());
    assert!(conn.execute("ROLLBACK TO before_interrupt;").is_err());
    assert!(conn.prepare("SELECT * FROM temp_rolled_back;").is_err());

    conn.execute("BEGIN;")
        .context("begin after interrupted transaction")?;
    conn.execute("COMMIT;")
        .context("commit after interrupted transaction")?;

    let mut count = conn.prepare("SELECT count(*) FROM t;")?;
    loop {
        match count.step()? {
            StepResult::IO => count._io().step()?,
            StepResult::Yield => continue,
            StepResult::Row => break,
            result => panic!("expected count row, got {result:?}"),
        }
    }
    assert_eq!(
        count.row().unwrap().get_value(0),
        &turso_core::Value::from_i64(10)
    );
    Ok(())
}
