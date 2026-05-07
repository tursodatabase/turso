use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use anyhow::Result;
use turso_core::{Connection, Database, DatabaseOpts, IO, OpenFlags, StepResult};

use crate::runner::SimIO;
use crate::runner::memory::io::MemorySimIO;

fn make_conn(seed: u64) -> Result<(Arc<Connection>, Arc<MemorySimIO>)> {
    let io = make_io(seed);
    let path = format!("sim_stmt_abandon_{seed}.db");
    let conn = open_conn(io.clone(), &path)?;
    Ok((conn, io))
}

fn make_two_conns(seed: u64) -> Result<(Arc<Connection>, Arc<Connection>, Arc<MemorySimIO>)> {
    let io = make_io(seed);
    let path = format!("sim_stmt_abandon_two_conns_{seed}.db");
    let (conn1, conn2) = open_two_conns(io.clone(), &path)?;
    Ok((conn1, conn2, io))
}

fn make_io(seed: u64) -> Arc<MemorySimIO> {
    make_io_with_page_size(seed, 4096)
}

fn make_io_with_page_size(seed: u64, page_size: usize) -> Arc<MemorySimIO> {
    Arc::new(MemorySimIO::new(
        seed, page_size, 100, // Always schedule operations asynchronously.
        1, 5,
    ))
}

fn open_conn(io: Arc<MemorySimIO>, path: &str) -> Result<Arc<Connection>> {
    let db = Database::open_file_with_flags(
        io as Arc<dyn IO>,
        path,
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
    )?;
    let conn = db.connect()?;
    Ok(conn)
}

fn open_two_conns(io: Arc<MemorySimIO>, path: &str) -> Result<(Arc<Connection>, Arc<Connection>)> {
    let db = Database::open_file_with_flags(
        io as Arc<dyn IO>,
        path,
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
    )?;
    let conn1 = db.connect()?;
    let conn2 = db.connect()?;
    Ok((conn1, conn2))
}

fn query_count(conn: &Arc<Connection>, io: &MemorySimIO) -> Result<i64> {
    query_one_i64(conn, io, "SELECT COUNT(*) FROM t")
}

fn query_one_i64(conn: &Arc<Connection>, io: &MemorySimIO, sql: &str) -> Result<i64> {
    let mut stmt = conn.prepare(sql)?;
    loop {
        match stmt.step()? {
            StepResult::IO => io.step()?,
            StepResult::Row => {
                let row = stmt.row().expect("row should exist for scalar query");
                let value = row.get::<i64>(0).expect("i64 column should exist");
                return Ok(value);
            }
            StepResult::Done => panic!("scalar query ended without a row"),
            other => panic!("unexpected step result: {other:?}"),
        }
    }
}

fn query_one_string(conn: &Arc<Connection>, io: &MemorySimIO, sql: &str) -> Result<String> {
    let mut stmt = conn.prepare(sql)?;
    loop {
        match stmt.step()? {
            StepResult::IO => io.step()?,
            StepResult::Row => {
                let row = stmt.row().expect("row should exist for scalar query");
                let value = row.get::<String>(0).expect("text column should exist");
                return Ok(value);
            }
            StepResult::Done => panic!("scalar query ended without a row"),
            other => panic!("unexpected step result: {other:?}"),
        }
    }
}

fn query_rows(conn: &Arc<Connection>, io: &MemorySimIO) -> Result<Vec<(i64, String)>> {
    let mut stmt = conn.prepare("SELECT id, v FROM t ORDER BY id")?;
    let mut rows = Vec::new();
    loop {
        match stmt.step()? {
            StepResult::IO => io.step()?,
            StepResult::Row => {
                let row = stmt.row().expect("row should exist");
                rows.push((
                    row.get::<i64>(0).expect("id column should exist"),
                    row.get::<String>(1).expect("v column should exist"),
                ));
            }
            StepResult::Done => return Ok(rows),
            other => panic!("unexpected step result: {other:?}"),
        }
    }
}

#[test]
fn sim_statement_rollback_restores_overflow_pages_when_cache_spills() -> Result<()> {
    let io = make_io_with_page_size(9, 512);
    let conn = open_conn(io.clone(), "sim_stmt_overflow_rollback_spill.db")?;

    conn.execute("PRAGMA page_size=512")?;
    conn.execute("PRAGMA cache_size=200")?;
    conn.execute(
        "CREATE TABLE t(
            id INTEGER PRIMARY KEY,
            v TEXT,
            unique_text TEXT UNIQUE,
            r REAL,
            unique_int INTEGER UNIQUE
        )",
    )?;

    let values = (0..8)
        .map(|i| {
            let id = if i == 0 {
                "NULL".to_string()
            } else {
                (1000 + i).to_string()
            };
            let prefix = format!("p{i}:");
            let payload = format!("{prefix}{}", "X".repeat(12_000 - prefix.len()));
            format!("({id}, '{payload}', 'u{i}', 0, {})", 2000 + i)
        })
        .collect::<Vec<_>>()
        .join(", ");
    conn.execute(format!("INSERT INTO t VALUES {values}"))?;
    conn.execute("SAVEPOINT sp")?;
    conn.execute("INSERT INTO t VALUES (999999, 'small', 'ufail', 0, 999999)")?;

    let update_payload = "Y".repeat(16_000);
    let failed_update = conn.execute(format!(
        "UPDATE t
            SET v = '{update_payload}',
                id = CASE WHEN id = 999999 THEN 1 ELSE id END
          WHERE TRUE"
    ));
    assert!(
        failed_update.is_err(),
        "update should fail with a duplicate rowid"
    );

    assert_eq!(
        query_one_i64(&conn, io.as_ref(), "SELECT COUNT(*) FROM t")?,
        9
    );
    assert_eq!(
        query_one_i64(&conn, io.as_ref(), "SELECT sum(length(v)) FROM t")?,
        8 * 12_000 + 5
    );
    assert_eq!(
        query_one_string(&conn, io.as_ref(), "PRAGMA integrity_check")?,
        "ok"
    );

    conn.execute("ROLLBACK TO sp")?;
    conn.execute("RELEASE sp")?;
    assert_eq!(
        query_one_i64(&conn, io.as_ref(), "SELECT COUNT(*) FROM t")?,
        8
    );
    assert_eq!(
        query_one_i64(&conn, io.as_ref(), "SELECT sum(length(v)) FROM t")?,
        8 * 12_000
    );
    Ok(())
}

#[test]
fn sim_statement_rollback_spill_does_not_pollute_parent_savepoint() -> Result<()> {
    let io = make_io_with_page_size(10, 512);
    let conn = open_conn(io.clone(), "sim_stmt_nested_rollback_spill.db")?;

    conn.execute("PRAGMA page_size=512")?;
    conn.execute("PRAGMA journal_mode=WAL")?;
    conn.execute("PRAGMA cache_size=8")?;
    conn.execute(
        "CREATE TABLE t(
            id INTEGER PRIMARY KEY,
            v TEXT,
            unique_text TEXT UNIQUE,
            unique_int INTEGER UNIQUE
        )",
    )?;

    let values = (1..=30)
        .map(|i| {
            let payload = "X".repeat(12_000);
            format!("({i}, '{payload}', 'u{i}', {i})")
        })
        .collect::<Vec<_>>()
        .join(", ");
    conn.execute(format!("INSERT INTO t VALUES {values}"))?;

    conn.execute("SAVEPOINT outer")?;
    let update_payload = "Y".repeat(16_000);
    let failed_update = conn.execute(format!(
        "UPDATE t
            SET v = '{update_payload}',
                id = CASE WHEN id = 30 THEN 1 ELSE id END
          WHERE TRUE"
    ));
    assert!(
        failed_update.is_err(),
        "update should fail with a duplicate rowid"
    );
    assert_eq!(
        query_one_i64(&conn, io.as_ref(), "SELECT sum(length(v)) FROM t")?,
        30 * 12_000
    );
    assert_eq!(
        query_one_string(&conn, io.as_ref(), "PRAGMA integrity_check")?,
        "ok"
    );

    conn.execute("UPDATE t SET v = 'changed' WHERE id = 1")?;
    conn.execute("ROLLBACK TO outer")?;
    conn.execute("RELEASE outer")?;

    assert_eq!(
        query_one_i64(&conn, io.as_ref(), "SELECT count(*) FROM t")?,
        30
    );
    assert_eq!(
        query_one_i64(&conn, io.as_ref(), "SELECT sum(length(v)) FROM t")?,
        30 * 12_000
    );
    assert_eq!(
        query_one_i64(&conn, io.as_ref(), "SELECT length(v) FROM t WHERE id = 1")?,
        12_000
    );
    assert_eq!(
        query_one_string(&conn, io.as_ref(), "PRAGMA integrity_check")?,
        "ok"
    );
    Ok(())
}

#[test]
fn sim_abandon_during_dml_rolls_back() -> Result<()> {
    let (conn, io) = make_conn(1)?;
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")?;

    let mut stmt = conn.prepare("INSERT INTO t VALUES (1, 'a'), (2, 'b') RETURNING id")?;
    let first = stmt.step()?;
    assert!(
        matches!(first, StepResult::IO),
        "expected IO before any RETURNING row, got {first:?}"
    );

    // Abandon while DML is in progress, before scan-back starts.
    drop(stmt);
    io.step()?;

    assert_eq!(query_count(&conn, io.as_ref())?, 0);
    Ok(())
}

#[test]
fn sim_abandon_after_first_returning_row_commits() -> Result<()> {
    let (conn, io) = make_conn(2)?;
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")?;

    let mut stmt =
        conn.prepare("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c') RETURNING id")?;
    let mut saw_io = false;
    loop {
        match stmt.step()? {
            StepResult::IO => {
                saw_io = true;
                io.step()?;
            }
            StepResult::Row => break,
            StepResult::Done => panic!("statement completed before yielding RETURNING row"),
            other => panic!("unexpected step result: {other:?}"),
        }
    }
    assert!(
        saw_io,
        "expected async IO before first RETURNING row in simulated IO"
    );

    // Abandon after scan-back has started; DML is already complete.
    drop(stmt);

    assert_eq!(query_count(&conn, io.as_ref())?, 3);
    Ok(())
}

#[test]
fn sim_reset_error_does_not_hold_locks() -> Result<()> {
    let (conn, io) = make_conn(3)?;
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")?;

    let mut stmt =
        conn.prepare("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c') RETURNING id")?;
    loop {
        match stmt.step()? {
            StepResult::IO => io.step()?,
            StepResult::Row => break,
            StepResult::Done => panic!("statement completed before yielding RETURNING row"),
            other => panic!("unexpected step result: {other:?}"),
        }
    }

    // Force commit/rollback I/O in reset to fail; reset() should return an error
    // but still release transactional resources so subsequent writes can proceed.
    io.inject_fault(true);
    let reset_result = stmt.reset();
    io.inject_fault(false);
    assert!(
        reset_result.is_err(),
        "expected reset to fail under I/O fault"
    );

    conn.execute("INSERT INTO t VALUES (99, 'ok')")?;
    assert_eq!(
        query_rows(&conn, io.as_ref())?,
        vec![(99, "ok".to_string())]
    );
    Ok(())
}

#[test]
fn sim_reset_error_does_not_block_other_connections() -> Result<()> {
    let (conn1, conn2, io) = make_two_conns(4)?;
    conn1.execute("PRAGMA cache_size=10")?;
    conn1.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")?;

    let values = (1..=400)
        .map(|id| format!("({}, '{}')", id, "x".repeat(128)))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!("INSERT INTO t VALUES {values} RETURNING id");
    let mut stmt = conn1.prepare(&sql)?;
    loop {
        match stmt.step()? {
            StepResult::IO => io.step()?,
            StepResult::Row => break,
            StepResult::Done => panic!("statement completed before yielding RETURNING row"),
            other => panic!("unexpected step result: {other:?}"),
        }
    }

    io.inject_fault(true);
    let reset_result = stmt.reset();
    io.inject_fault(false);
    assert!(
        reset_result.is_err(),
        "expected reset to fail under I/O fault"
    );
    assert!(
        !conn1.is_in_write_tx(),
        "write transaction should be cleaned up after reset() error"
    );
    assert!(
        !conn1.get_pager().holds_read_lock(),
        "read lock should be released after reset() error cleanup"
    );

    // If conn1 leaked write-lock/transaction state, this write can fail with Busy.
    conn2.execute("INSERT INTO t VALUES (99, 'from_conn2')")?;
    assert_eq!(
        query_rows(&conn2, io.as_ref())?,
        vec![(99, "from_conn2".to_string())]
    );
    Ok(())
}

#[test]
fn sim_panic_drop_does_not_leave_write_tx_open() -> Result<()> {
    let (conn, io) = make_conn(5)?;
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")?;

    let panic_result = catch_unwind(AssertUnwindSafe({
        let conn = conn.clone();
        move || {
            let mut stmt = conn
                .prepare("INSERT INTO t VALUES (1, 'a'), (2, 'b') RETURNING id")
                .expect("prepare should succeed");
            loop {
                match stmt.step().expect("step should succeed before panic") {
                    StepResult::IO => {
                        io.step().expect("io step should succeed before panic");
                        panic!("intentional panic while statement is running");
                    }
                    StepResult::Row => {}
                    StepResult::Done => panic!("statement completed unexpectedly before panic"),
                    other => panic!("unexpected step result: {other:?}"),
                }
            }
        }
    }));
    assert!(panic_result.is_err(), "expected injected panic");

    assert!(
        !conn.is_in_write_tx(),
        "write transaction should be cleaned up when statement is dropped during panic"
    );
    Ok(())
}

#[test]
fn sim_reset_releases_subjournal_when_abort_called_without_error() -> Result<()> {
    let (conn, io) = make_conn(6)?;
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")?;

    let mut stmt = conn.prepare("INSERT INTO t VALUES (1, 'a'), (2, 'b')")?;
    let first = stmt.step()?;
    assert!(
        matches!(first, StepResult::IO),
        "expected IO while statement is running, got {first:?}"
    );

    // Drop path also routes through reset_internal() via reset_best_effort.
    drop(stmt);
    io.step()?;

    let pager = conn.get_pager();
    assert!(
        !pager.subjournal_in_use(),
        "subjournal should be released after statement drop/reset cleanup"
    );

    conn.execute("INSERT INTO t VALUES (99, 'ok')")?;
    assert_eq!(
        query_rows(&conn, io.as_ref())?,
        vec![(99, "ok".to_string())]
    );
    Ok(())
}

/// Verify that a completed VACUUM INTO does not leak source transaction state.
/// After a successful vacuum, the source connection must be back in auto-commit
/// mode and usable for new writes.
#[test]
fn sim_vacuum_into_cleans_up_source_transaction() -> Result<()> {
    let (conn, io) = make_conn(7)?;

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")?;
    for i in 0..20 {
        conn.execute(format!("INSERT INTO t VALUES ({i}, 'row_{i}')"))?;
    }

    let dest_dir = tempfile::TempDir::new()?;
    let dest_path = dest_dir.path().join("vacuumed.db");
    let dest_path_str = dest_path.to_str().expect("temp dir should be valid UTF-8");

    assert!(
        conn.get_auto_commit(),
        "should be in auto-commit before VACUUM INTO"
    );

    let mut stmt = conn.prepare(format!("VACUUM INTO '{dest_path_str}'"))?;
    loop {
        match stmt.step()? {
            StepResult::IO => io.step()?,
            StepResult::Done => break,
            other => panic!("unexpected step result: {other:?}"),
        }
    }
    drop(stmt);

    // Source connection must be back in auto-commit mode after vacuum.
    assert!(
        conn.get_auto_commit(),
        "source connection should be in auto-commit mode after VACUUM INTO completes"
    );

    // Source connection must be usable for new writes.
    conn.execute("INSERT INTO t VALUES (999, 'after_vacuum')")?;
    let count = query_count(&conn, io.as_ref())?;
    assert_eq!(
        count, 21,
        "should have 20 original rows + 1 new row after vacuum"
    );
    Ok(())
}

/// Abandoning VACUUM INTO while it is still running must roll back the manually
/// owned source transaction.
#[test]
fn sim_abandon_vacuum_into_cleans_up_source_transaction() -> Result<()> {
    let io = make_io(8);
    let path = "sim_stmt_abandon_vacuum_reopen.db";
    // Reopen so VACUUM reads source pages from simulated storage; with warm
    // cache, this can complete synchronously and not exercise abandon cleanup.
    {
        let conn = open_conn(io.clone(), path)?;
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")?;
        let payload = "x".repeat(256);
        for i in 0..400 {
            conn.execute(format!("INSERT INTO t VALUES ({i}, '{payload}')"))?;
        }
    }

    let conn = open_conn(io.clone(), path)?;

    let dest_dir = tempfile::TempDir::new()?;
    let dest_path = dest_dir.path().join("abandoned_vacuum.db");
    let dest_path_str = dest_path.to_str().expect("temp dir should be valid UTF-8");

    let mut stmt = conn.prepare(format!("VACUUM INTO '{dest_path_str}'"))?;
    let first = stmt.step()?;
    assert!(
        matches!(first, StepResult::IO),
        "expected IO while VACUUM INTO is running, got {first:?}"
    );
    assert!(
        !conn.get_auto_commit(),
        "VACUUM INTO should own a source transaction while it is running"
    );

    drop(stmt);
    io.step()?;

    assert!(
        conn.get_auto_commit(),
        "source connection should return to auto-commit after abandoned VACUUM INTO"
    );
    conn.execute("INSERT INTO t VALUES (999, 'after_abandon')")?;
    let count = query_count(&conn, io.as_ref())?;
    assert_eq!(
        count, 401,
        "should have 400 original rows + 1 new row after abandoned vacuum"
    );
    Ok(())
}
