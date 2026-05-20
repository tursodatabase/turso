use crate::common::{limbo_exec_rows, TempDatabase};
use rusqlite::types::Value as RValue;
use std::time::Duration;
use turso_core::vdbe::StepResult;

fn run_until_terminal(stmt: &mut turso_core::Statement) -> turso_core::Result<StepResult> {
    loop {
        match stmt.step()? {
            StepResult::IO => stmt._io().step()?,
            StepResult::Row => continue,
            result => return Ok(result),
        }
    }
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
fn query_timeout_allows_generate_series_null_stop(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.set_query_timeout(Duration::from_millis(10));

    let mut stmt = conn.prepare("SELECT count(*) FROM generate_series(1, NULL);")?;
    let result = run_until_terminal(&mut stmt)?;
    assert!(
        matches!(result, StepResult::Done),
        "expected NULL stop to produce an empty series without scanning, got {result:?}"
    );
    assert_eq!(
        limbo_exec_rows(&conn, "SELECT count(*) FROM generate_series(1, NULL);"),
        vec![vec![RValue::Integer(0)]]
    );
    assert_eq!(
        limbo_exec_rows(&conn, "SELECT * FROM generate_series(1, NULL) LIMIT 5;"),
        Vec::<Vec<RValue>>::new()
    );
    Ok(())
}
