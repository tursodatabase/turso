use std::sync::Arc;

use anyhow::Result;
use turso_core::{Connection, Database, DatabaseOpts, IO, OpenFlags, StepResult};

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
