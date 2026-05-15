use std::sync::Arc;

use anyhow::Result;
use turso_core::{Connection, Database, DatabaseOpts, IO, OpenFlags, StepResult};

use crate::runner::memory::io::MemorySimIO;

fn make_io(seed: u64) -> Arc<MemorySimIO> {
    Arc::new(MemorySimIO::new(seed, 4096, 100, 1, 5))
}

fn open_conn(io: Arc<MemorySimIO>, path: &str) -> Result<Arc<Connection>> {
    let db = Database::open_file_with_flags(
        io as Arc<dyn IO>,
        path,
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
    )?;
    Ok(db.connect()?)
}

fn query_strings(conn: &Arc<Connection>, io: &MemorySimIO, sql: &str) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(sql)?;
    let mut rows = Vec::new();
    loop {
        match stmt.step()? {
            StepResult::IO => io.step()?,
            StepResult::Row => {
                let row = stmt.row().expect("row should exist");
                rows.push(row.get::<String>(0).expect("text column should exist"));
            }
            StepResult::Done => return Ok(rows),
            other => panic!("unexpected step result: {other:?}"),
        }
    }
}

#[test]
fn sim_alter_column_check_is_enforced_before_reopen() -> Result<()> {
    let seed = 1;
    let io = make_io(seed);
    let path = format!("sim_alter_column_check_{seed}.db");
    let conn = open_conn(io.clone(), &path)?;

    conn.execute("CREATE TABLE t(a INTEGER)")?;
    conn.execute("ALTER TABLE t ALTER COLUMN a TO b INTEGER CHECK(b > 0)")?;

    let schema = query_strings(
        &conn,
        io.as_ref(),
        "SELECT sql FROM sqlite_schema WHERE name = 't'",
    )?;
    assert_eq!(schema, vec!["CREATE TABLE t (b INTEGER CHECK (b > 0))"]);

    let invalid_insert = conn.execute("INSERT INTO t VALUES(-1)");
    assert!(
        invalid_insert.is_err(),
        "ALTER COLUMN must refresh live CHECK metadata before the database is reopened"
    );

    let integrity = query_strings(&conn, io.as_ref(), "PRAGMA integrity_check")?;
    assert_eq!(integrity, vec!["ok"]);
    Ok(())
}
