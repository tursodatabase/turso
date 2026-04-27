use std::sync::Arc;

use anyhow::Result;
use turso_core::{Connection, Database, DatabaseOpts, IO, OpenFlags, StepResult};

use crate::runner::memory::io::MemorySimIO;

fn make_conn(seed: u64) -> Result<(Arc<Connection>, Arc<MemorySimIO>)> {
    let io = Arc::new(MemorySimIO::new(seed, 4096, 100, 1, 5));
    let db = Database::open_file_with_flags(
        io.clone() as Arc<dyn IO>,
        &format!("sim_savepoint_rollback_{seed}.db"),
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
    )?;
    Ok((db.connect()?, io))
}

fn query_strings(conn: &Arc<Connection>, io: &MemorySimIO, sql: &str) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(sql)?;
    let mut rows = Vec::new();
    loop {
        match stmt.step()? {
            StepResult::IO => io.step()?,
            StepResult::Row => {
                let row = stmt.row().expect("row should exist");
                rows.push(row.get::<String>(0).expect("string column should exist"));
            }
            StepResult::Done => return Ok(rows),
            other => panic!("unexpected step result: {other:?}"),
        }
    }
}

#[test]
fn sim_savepoint_rollback_after_cache_spill_preserves_wal_pages() -> Result<()> {
    let (conn, io) = make_conn(5803)?;

    conn.execute("PRAGMA page_size=512")?;
    conn.execute("PRAGMA journal_mode=WAL")?;
    conn.execute("PRAGMA cache_size=50")?;
    conn.execute("CREATE TABLE filler(x INTEGER PRIMARY KEY)")?;
    conn.execute("INSERT INTO filler(x) VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10)")?;

    conn.execute("BEGIN")?;
    conn.execute("CREATE TABLE t(k INTEGER PRIMARY KEY, v TEXT NOT NULL)")?;
    conn.execute("CREATE TABLE b(k INTEGER PRIMARY KEY, v TEXT NOT NULL)")?;
    conn.execute(
        "INSERT INTO t(v)
         SELECT printf('%0*d', 4000, f1.x*1000 + f2.x)
         FROM filler f1, filler f2
         LIMIT 25",
    )?;
    conn.execute(
        "UPDATE t
         SET v = printf('%0*d', 4000, k + 10000000)
         WHERE k BETWEEN 1 AND 23",
    )?;
    conn.execute("SAVEPOINT s1")?;
    conn.execute(
        "INSERT INTO b(v)
         SELECT printf('%0*d', 4000, f1.x*1000 + f2.x)
         FROM filler f1, filler f2
         LIMIT 40",
    )?;
    conn.execute("ROLLBACK TO s1")?;
    conn.execute("RELEASE s1")?;
    conn.execute("COMMIT")?;

    assert_eq!(
        query_strings(&conn, io.as_ref(), "PRAGMA integrity_check")?,
        vec!["ok"]
    );

    Ok(())
}
