use std::sync::Arc;

use anyhow::Result;
use turso_core::{
    ConnectOpts, Connection, Database, DatabaseOpts, IO, OpenFlags, SqliteDialect, StepResult,
};

use crate::runner::SimIO;
use crate::runner::memory::io::MemorySimIO;

fn make_io(seed: u64) -> Arc<MemorySimIO> {
    Arc::new(MemorySimIO::new(seed, 4096, 25, 1, 4))
}

fn open_conn(io: Arc<MemorySimIO>, path: &str) -> Result<Arc<Connection>> {
    let db = Database::open_file_with_flags(
        io as Arc<dyn IO>,
        path,
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )?;
    Ok(db.connect(ConnectOpts::new())?)
}

fn step_done(conn: &Arc<Connection>, io: &MemorySimIO, sql: &str) -> Result<()> {
    let mut stmt = conn.prepare(sql)?;
    loop {
        match stmt.step()? {
            StepResult::IO => io.step()?,
            StepResult::Row => {}
            StepResult::Done => return Ok(()),
            other => panic!("unexpected step result for `{sql}`: {other:?}"),
        }
    }
}

fn assert_integrity_ok(conn: &Arc<Connection>, io: &MemorySimIO) -> Result<()> {
    let mut stmt = conn.prepare("PRAGMA integrity_check")?;
    let mut messages = Vec::new();
    loop {
        match stmt.step()? {
            StepResult::IO => io.step()?,
            StepResult::Row => {
                let row = stmt.row().expect("integrity_check row");
                messages.push(row.get::<String>(0)?);
            }
            StepResult::Done => break,
            other => panic!("unexpected step result for integrity_check: {other:?}"),
        }
    }
    assert_eq!(messages, vec!["ok".to_string()]);
    Ok(())
}

/// Regression test for #8704: reopening a database after `ALTER TABLE ... ADD COLUMN`
/// on a table with `UNIQUE(a), PRIMARY KEY(b)` must map `sqlite_autoindex_t_1` and
/// `sqlite_autoindex_t_2` back to their original root pages and pass `integrity_check`.
#[test]
fn sim_alter_table_preserves_autoindex_order_across_reopen() -> Result<()> {
    let io = make_io(8704);
    let path = "sim_alter_pk_order.db";
    let conn = open_conn(io.clone(), path)?;

    step_done(
        &conn,
        &io,
        "CREATE TABLE t (a TEXT, b TEXT, UNIQUE (a), PRIMARY KEY (b))",
    )?;
    step_done(
        &conn,
        &io,
        "INSERT INTO t VALUES ('alpha', 'one'), ('beta', 'two')",
    )?;
    step_done(&conn, &io, "ALTER TABLE t ADD COLUMN d TEXT DEFAULT 'x'")?;

    step_done(&conn, &io, "CREATE TABLE u (a TEXT UNIQUE, b TEXT)")?;
    step_done(&conn, &io, "INSERT INTO u VALUES ('x', 'y')")?;
    step_done(&conn, &io, "ALTER TABLE u ALTER COLUMN a TO aa TEXT")?;
    step_done(&conn, &io, "ALTER TABLE u ADD COLUMN q TEXT")?;

    drop(conn);
    io.close_files();

    let reopened = open_conn(io.clone(), path)?;
    assert_integrity_ok(&reopened, &io)?;
    Ok(())
}
