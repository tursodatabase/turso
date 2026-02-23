use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use anyhow::Result;
use turso_core::{Connection, Database, DatabaseOpts, IO, OpenFlags, StepResult};

use crate::runner::SimIO;
use crate::runner::memory::io::MemorySimIO;

fn make_conn(seed: u64) -> Result<(Arc<Connection>, Arc<MemorySimIO>)> {
    let io = Arc::new(MemorySimIO::new(
        seed, 4096, 100, // Always schedule operations asynchronously.
        1, 5,
    ));
    let path = format!("sim_stmt_abandon_{seed}.db");
    let db = Database::open_file_with_flags(
        io.clone() as Arc<dyn IO>,
        &path,
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
    )?;
    let conn = db.connect()?;
    Ok((conn, io))
}

fn make_two_conns(seed: u64) -> Result<(Arc<Connection>, Arc<Connection>, Arc<MemorySimIO>)> {
    let io = Arc::new(MemorySimIO::new(
        seed, 4096, 100, // Always schedule operations asynchronously.
        1, 5,
    ));
    let path = format!("sim_stmt_abandon_two_conns_{seed}.db");
    let db = Database::open_file_with_flags(
        io.clone() as Arc<dyn IO>,
        &path,
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
    )?;
    let conn1 = db.connect()?;
    let conn2 = db.connect()?;
    Ok((conn1, conn2, io))
}

fn query_count(conn: &Arc<Connection>, io: &MemorySimIO) -> Result<i64> {
    let mut stmt = conn.prepare("SELECT COUNT(*) FROM t")?;
    loop {
        match stmt.step()? {
            StepResult::IO => io.step()?,
            StepResult::Row => {
                let row = stmt.row().expect("row should exist for count query");
                let count = row.get::<i64>(0).expect("count column should exist");
                return Ok(count);
            }
            StepResult::Done => panic!("count query ended without a row"),
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
