use crate::common::{limbo_exec_rows, TempDatabase};
use turso_core::vdbe::StepResult;

/// Helper: step a statement through IO until it returns Row or Done.
fn step_blocking(stmt: &mut turso_core::Statement) -> turso_core::Result<StepResult> {
    loop {
        match stmt.step()? {
            StepResult::IO => stmt._io().step()?,
            other => return Ok(other),
        }
    }
}

/// INSERT ... RETURNING: read one row then drop the statement.
/// The DML should be committed because all DML completes before any
/// RETURNING rows are yielded (ephemeral table buffering).
#[turso_macros::test(init_sql = "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);")]
fn returning_drop_after_partial_read_commits(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    {
        let mut stmt =
            conn.prepare("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c') RETURNING *")?;
        // Read one row — proves all DML completed.
        let result = step_blocking(&mut stmt)?;
        assert!(
            matches!(result, StepResult::Row),
            "expected Row, got {result:?}"
        );
        // Drop without reading remaining rows.
    }

    // All 3 rows should be committed.
    let rows = limbo_exec_rows(&conn, "SELECT id FROM t ORDER BY id");
    assert_eq!(
        rows.len(),
        3,
        "expected 3 rows committed, got {}",
        rows.len()
    );
    Ok(())
}

/// UPDATE ... RETURNING: read one row then drop.
/// Same principle — all updates complete before scan-back.
#[turso_macros::test(init_sql = "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER);")]
fn returning_update_drop_after_partial_read_commits(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")?;

    {
        let mut stmt = conn.prepare("UPDATE t SET v = v + 1 RETURNING *")?;
        let result = step_blocking(&mut stmt)?;
        assert!(matches!(result, StepResult::Row));
        // Drop without reading all rows.
    }

    // All 3 rows should be updated.
    let rows = limbo_exec_rows(&conn, "SELECT v FROM t ORDER BY id");
    assert_eq!(rows.len(), 3);
    let values: Vec<i64> = rows
        .iter()
        .map(|row| match &row[0] {
            rusqlite::types::Value::Integer(i) => *i,
            other => panic!("expected integer, got {other:?}"),
        })
        .collect();
    assert_eq!(values, vec![11, 21, 31], "expected all rows updated");
    Ok(())
}

/// DELETE ... RETURNING: read one row then drop.
#[turso_macros::test(init_sql = "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);")]
fn returning_delete_drop_after_partial_read_commits(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')")?;

    {
        let mut stmt = conn.prepare("DELETE FROM t RETURNING *")?;
        let result = step_blocking(&mut stmt)?;
        assert!(matches!(result, StepResult::Row));
        // Drop without reading all rows.
    }

    // All rows should be deleted.
    let rows = limbo_exec_rows(&conn, "SELECT COUNT(*) FROM t");
    let count = match &rows[0][0] {
        rusqlite::types::Value::Integer(i) => *i,
        other => panic!("expected integer, got {other:?}"),
    };
    assert_eq!(count, 0, "expected 0 rows after DELETE, got {count}");
    Ok(())
}

/// Plain INSERT (no RETURNING): runs to completion via step(), then drop.
/// This is the normal case — Halt commits the transaction.
#[turso_macros::test(init_sql = "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);")]
fn plain_insert_step_to_done_commits(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    {
        let mut stmt = conn.prepare("INSERT INTO t VALUES (1, 'a'), (2, 'b')")?;
        let result = step_blocking(&mut stmt)?;
        assert!(
            matches!(result, StepResult::Done),
            "expected Done, got {result:?}"
        );
    }

    let rows = limbo_exec_rows(&conn, "SELECT COUNT(*) FROM t");
    let count = match &rows[0][0] {
        rusqlite::types::Value::Integer(i) => *i,
        other => panic!("expected integer, got {other:?}"),
    };
    assert_eq!(count, 2);
    Ok(())
}

/// INSERT ... RETURNING: read ALL rows to completion.
/// Normal completion — Halt commits.
#[turso_macros::test(init_sql = "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);")]
fn returning_read_all_rows_commits(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    {
        let mut stmt =
            conn.prepare("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c') RETURNING id")?;
        let mut row_count = 0;
        loop {
            match step_blocking(&mut stmt)? {
                StepResult::Row => row_count += 1,
                StepResult::Done => break,
                other => panic!("unexpected {other:?}"),
            }
        }
        assert_eq!(row_count, 3);
    }

    let rows = limbo_exec_rows(&conn, "SELECT COUNT(*) FROM t");
    let count = match &rows[0][0] {
        rusqlite::types::Value::Integer(i) => *i,
        other => panic!("expected integer, got {other:?}"),
    };
    assert_eq!(count, 3);
    Ok(())
}
