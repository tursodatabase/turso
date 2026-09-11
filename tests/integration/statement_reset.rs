use std::sync::Arc;
use turso_core::SqliteDialect;

use crate::common::{limbo_exec_rows, sqlite_exec_rows, TempDatabase};
use crate::queued_io::QueuedIo;
use rusqlite::StatementStatus;
use turso_core::vdbe::StepResult;
use turso_core::StatementStatusCounter;
use turso_core::{Database, LimboError};

/// Helper: step a statement through IO until it returns Row or Done.
fn step_blocking(stmt: &mut turso_core::Statement) -> turso_core::Result<StepResult> {
    loop {
        match stmt.step()? {
            StepResult::IO => stmt._io().step()?,
            StepResult::Yield => continue,
            other => return Ok(other),
        }
    }
}

#[derive(Clone, Copy)]
enum ResetHook {
    None,
    BusyHandlerAfterPrepare,
}

fn set_busy_handler_if_needed(conn: &Arc<turso_core::Connection>, hook: ResetHook) {
    if matches!(hook, ResetHook::BusyHandlerAfterPrepare) {
        conn.set_busy_handler(Some(Box::new(|_| 0)));
    }
}

fn clear_busy_handler_if_needed(conn: &Arc<turso_core::Connection>, hook: ResetHook) {
    if matches!(hook, ResetHook::BusyHandlerAfterPrepare) {
        conn.set_busy_handler(None);
    }
}

fn exec_with_reset(
    conn: &Arc<turso_core::Connection>,
    sql: &str,
    hook: ResetHook,
) -> anyhow::Result<()> {
    let mut stmt = conn.prepare(sql)?;
    set_busy_handler_if_needed(conn, hook);
    let result = stmt.run_ignore_rows();
    clear_busy_handler_if_needed(conn, hook);
    result?;
    stmt.reset()?;
    Ok(())
}

fn query_rows_with_reset(
    conn: &Arc<turso_core::Connection>,
    sql: &str,
    hook: ResetHook,
) -> anyhow::Result<Vec<Vec<rusqlite::types::Value>>> {
    let mut stmt = conn.prepare(sql)?;
    let mut rows = Vec::new();

    set_busy_handler_if_needed(conn, hook);
    let result = stmt.run_with_row_callback(|row| {
        let row = row
            .get_values()
            .map(|value| match value {
                turso_core::Value::Null => rusqlite::types::Value::Null,
                turso_core::Value::Numeric(turso_core::Numeric::Integer(value)) => {
                    rusqlite::types::Value::Integer(*value)
                }
                turso_core::Value::Numeric(turso_core::Numeric::Float(value)) => {
                    rusqlite::types::Value::Real(f64::from(*value))
                }
                turso_core::Value::Text(value) => {
                    rusqlite::types::Value::Text(value.as_str().to_string())
                }
                turso_core::Value::Blob(value) => rusqlite::types::Value::Blob(value.to_vec()),
            })
            .collect();
        rows.push(row);
        Ok(())
    });
    clear_busy_handler_if_needed(conn, hook);
    result?;
    stmt.reset()?;
    Ok(rows)
}

fn collect_limbo_prepared_rows(
    stmt: &mut turso_core::Statement,
) -> anyhow::Result<Vec<Vec<rusqlite::types::Value>>> {
    let mut rows = Vec::new();
    stmt.run_with_row_callback(|row| {
        let row = row
            .get_values()
            .map(|value| match value {
                turso_core::Value::Null => rusqlite::types::Value::Null,
                turso_core::Value::Numeric(turso_core::Numeric::Integer(value)) => {
                    rusqlite::types::Value::Integer(*value)
                }
                turso_core::Value::Numeric(turso_core::Numeric::Float(value)) => {
                    rusqlite::types::Value::Real(f64::from(*value))
                }
                turso_core::Value::Text(value) => {
                    rusqlite::types::Value::Text(value.as_str().to_string())
                }
                turso_core::Value::Blob(value) => rusqlite::types::Value::Blob(value.to_vec()),
            })
            .collect();
        rows.push(row);
        Ok(())
    })?;
    Ok(rows)
}

fn collect_sqlite_prepared_rows(
    stmt: &mut rusqlite::Statement<'_>,
) -> rusqlite::Result<Vec<Vec<rusqlite::types::Value>>> {
    let mut rows = stmt.query([])?;
    let mut results = Vec::new();
    while let Some(row) = rows.next()? {
        let mut result = Vec::new();
        for i in 0.. {
            let column: rusqlite::types::Value = match row.get(i) {
                Ok(column) => column,
                Err(rusqlite::Error::InvalidColumnIndex(_)) => break,
                Err(err) => return Err(err),
            };
            result.push(column);
        }
        results.push(result);
    }
    Ok(results)
}

fn assert_temp_table_transaction_matches_rusqlite(
    tmp_db: TempDatabase,
    hook: ResetHook,
) -> anyhow::Result<()> {
    let turso = tmp_db.connect_limbo();
    let sqlite = rusqlite::Connection::open_in_memory()?;

    for sql in [
        "CREATE TEMP TABLE temp_reset_probe(id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
        "BEGIN",
        "INSERT INTO temp_reset_probe VALUES (1, 'x')",
    ] {
        exec_with_reset(&turso, sql, hook)?;
        sqlite.execute_batch(sql)?;
    }

    let query = "SELECT id, value FROM temp_reset_probe ORDER BY id";
    assert_eq!(
        query_rows_with_reset(&turso, query, hook)?,
        sqlite_exec_rows(&sqlite, query)
    );

    exec_with_reset(&turso, "COMMIT", hook)?;
    sqlite.execute_batch("COMMIT")?;

    assert_eq!(
        query_rows_with_reset(&turso, query, hook)?,
        sqlite_exec_rows(&sqlite, query)
    );

    Ok(())
}

#[turso_macros::test(mvcc)]
fn temp_table_transaction_survives_statement_reset(tmp_db: TempDatabase) -> anyhow::Result<()> {
    assert_temp_table_transaction_matches_rusqlite(tmp_db, ResetHook::None)
}

#[turso_macros::test]
fn busy_handler_change_after_prepare_does_not_reprepare_temp_transaction(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    assert_temp_table_transaction_matches_rusqlite(tmp_db, ResetHook::BusyHandlerAfterPrepare)
}

#[turso_macros::test]
fn attached_transaction_survives_reprepare_after_schema_change(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let turso = tmp_db.connect_limbo();
    let turso_aux_path = tmp_db.path.with_file_name("attached-reprepare-aux.db");
    turso.execute(format!("ATTACH '{}' AS aux", turso_aux_path.display()))?;

    let sqlite_dir = tempfile::TempDir::new()?;
    let sqlite_main_path = sqlite_dir.path().join("main.db");
    let sqlite_aux_path = sqlite_dir.path().join("aux.db");
    let sqlite = rusqlite::Connection::open(sqlite_main_path)?;
    sqlite.execute(
        &format!("ATTACH '{}' AS aux", sqlite_aux_path.display()),
        [],
    )?;

    for sql in [
        "CREATE TABLE aux.reprepare_probe(id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
        "BEGIN",
        "INSERT INTO aux.reprepare_probe VALUES (1, 'x')",
    ] {
        turso.execute(sql)?;
        sqlite.execute(sql, [])?;
    }

    let query = "SELECT id, value FROM aux.reprepare_probe ORDER BY id";
    let mut turso_stmt = turso.prepare(query)?;
    let mut sqlite_stmt = sqlite.prepare(query)?;

    let schema_change = "CREATE TABLE aux.reprepare_probe_schema_change(marker INTEGER)";
    turso.execute(schema_change)?;
    sqlite.execute(schema_change, [])?;

    let turso_rows = collect_limbo_prepared_rows(&mut turso_stmt)?;
    let sqlite_rows = collect_sqlite_prepared_rows(&mut sqlite_stmt)?;

    assert!(
        turso_stmt.stmt_status(StatementStatusCounter::Reprepare) > 0,
        "expected Turso statement to reprepare after attached schema change"
    );
    assert!(
        sqlite_stmt.get_status(StatementStatus::RePrepare) > 0,
        "expected SQLite statement to reprepare after attached schema change"
    );
    assert_eq!(turso_rows, sqlite_rows);

    turso_stmt.reset()?;
    drop(sqlite_stmt);

    turso.execute("COMMIT")?;
    sqlite.execute("COMMIT", [])?;
    let turso_rows = limbo_exec_rows(&turso, query);
    let sqlite_rows = sqlite_exec_rows(&sqlite, query);
    dbg!(&turso_rows, &sqlite_rows);
    assert_eq!(turso_rows, sqlite_rows);

    Ok(())
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

#[turso_macros::test]
fn unrelated_runtime_error_does_not_rollback_open_returning_statement(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE pending (id INTEGER PRIMARY KEY, v TEXT)")?;
    conn.execute("CREATE TABLE overflow (x INTEGER)")?;
    conn.execute("INSERT INTO overflow VALUES (9223372036854775807), (1)")?;

    {
        let mut owner =
            conn.prepare("INSERT INTO pending VALUES (1, 'a'), (2, 'b') RETURNING id")?;
        let result = step_blocking(&mut owner)?;
        assert!(
            matches!(result, StepResult::Row),
            "expected Row, got {result:?}"
        );

        let err = conn
            .execute("SELECT sum(x) FROM overflow")
            .expect_err("sum overflow should fail");
        assert!(matches!(err, LimboError::IntegerOverflow), "{err:?}");
    }

    let rows = limbo_exec_rows(&conn, "SELECT id FROM pending ORDER BY id");
    let ids: Vec<i64> = rows
        .iter()
        .map(|row| match &row[0] {
            rusqlite::types::Value::Integer(i) => *i,
            other => panic!("expected integer, got {other:?}"),
        })
        .collect();
    assert_eq!(ids, vec![1, 2]);
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

#[test]
fn dropping_explicit_commit_waiting_on_io_rolls_back_transaction() -> anyhow::Result<()> {
    let io = Arc::new(QueuedIo::new());
    let db = Database::open_file(
        io,
        "queued-explicit-commit-drop.db",
        Arc::new(SqliteDialect),
    )?;
    let conn = db.connect()?;

    conn.execute("CREATE TABLE t(x INTEGER)")?;
    conn.execute("BEGIN")?;
    conn.execute("INSERT INTO t VALUES (1)")?;

    {
        let mut commit = conn.prepare("COMMIT")?;
        assert!(matches!(commit.step()?, StepResult::IO));
    }

    conn.execute("INSERT INTO t VALUES (2)")?;

    let mut stmt = conn.prepare("SELECT x FROM t ORDER BY x")?;
    let mut values = Vec::new();
    stmt.run_with_row_callback(|row| {
        values.push(row.get::<i64>(0)?);
        Ok(())
    })?;
    assert_eq!(values, vec![2]);
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
