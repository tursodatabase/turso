use std::sync::Arc;

use anyhow::Result;
use turso_core::{Connection, Database, DatabaseOpts, IO, OpenFlags, StepResult};

use crate::runner::memory::io::MemorySimIO;

fn make_conn(seed: u64) -> Result<(Arc<Connection>, Arc<MemorySimIO>)> {
    let io = Arc::new(MemorySimIO::new(seed, 4096, 100, 1, 5));
    let path = format!("sim_upsert_register_metadata_{seed}.db");
    let db = Database::open_file_with_flags(
        io.clone() as Arc<dyn IO>,
        &path,
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
    )?;
    Ok((db.connect()?, io))
}

fn query_rows(conn: &Arc<Connection>, io: &MemorySimIO) -> Result<Vec<(i64, String, i64)>> {
    let mut stmt = conn.prepare("SELECT id, CAST(a AS TEXT), x FROM t ORDER BY id")?;
    let mut rows = Vec::new();
    loop {
        match stmt.step()? {
            StepResult::IO => io.step()?,
            StepResult::Row => {
                let row = stmt.row().expect("row should exist");
                rows.push((
                    row.get::<i64>(0).expect("id column should exist"),
                    row.get::<String>(1).expect("a column should exist"),
                    row.get::<i64>(2).expect("x column should exist"),
                ));
            }
            StepResult::Done => return Ok(rows),
            other => panic!("unexpected step result: {other:?}"),
        }
    }
}

fn query_integrity(conn: &Arc<Connection>, io: &MemorySimIO) -> Result<String> {
    let mut stmt = conn.prepare("PRAGMA integrity_check")?;
    loop {
        match stmt.step()? {
            StepResult::IO => io.step()?,
            StepResult::Row => {
                let row = stmt.row().expect("row should exist");
                return Ok(row.get::<String>(0).expect("integrity row should exist"));
            }
            StepResult::Done => panic!("integrity_check ended without a row"),
            other => panic!("unexpected step result: {other:?}"),
        }
    }
}

#[test]
fn sim_upsert_where_respects_rewritten_target_column_affinity() -> Result<()> {
    let (conn, io) = make_conn(6866)?;
    conn.execute("PRAGMA journal_mode=WAL")?;
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, x INT)")?;
    conn.execute("CREATE INDEX idx_x ON t(x)")?;
    conn.execute("INSERT INTO t VALUES(1,10,100)")?;
    conn.execute("INSERT INTO t VALUES(2,20,200)")?;

    conn.execute("BEGIN")?;
    conn.execute(
        "INSERT OR FAIL INTO t(id,a,x) VALUES(1,99,999) \
         ON CONFLICT(id) DO UPDATE SET id=2 WHERE a < '2'",
    )?;
    conn.execute("COMMIT")?;

    assert_eq!(
        query_rows(&conn, io.as_ref())?,
        vec![(1, "10".to_string(), 100), (2, "20".to_string(), 200)]
    );
    assert_eq!(query_integrity(&conn, io.as_ref())?, "ok");
    Ok(())
}

#[test]
fn sim_upsert_where_respects_rewritten_target_column_collation() -> Result<()> {
    let (conn, io) = make_conn(6867)?;
    conn.execute("PRAGMA journal_mode=WAL")?;
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT COLLATE NOCASE, x INT)")?;
    conn.execute("CREATE INDEX idx_x ON t(x)")?;
    conn.execute("INSERT INTO t VALUES(1,'A',100)")?;
    conn.execute("INSERT INTO t VALUES(2,'B',200)")?;

    conn.execute("BEGIN")?;
    conn.execute(
        "INSERT OR FAIL INTO t(id,a,x) VALUES(1,'Z',999) \
         ON CONFLICT(id) DO UPDATE SET id=2 WHERE a != 'a'",
    )?;
    conn.execute("COMMIT")?;

    assert_eq!(
        query_rows(&conn, io.as_ref())?,
        vec![(1, "A".to_string(), 100), (2, "B".to_string(), 200)]
    );
    assert_eq!(query_integrity(&conn, io.as_ref())?, "ok");
    Ok(())
}
