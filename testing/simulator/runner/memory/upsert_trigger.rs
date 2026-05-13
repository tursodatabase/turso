use std::sync::Arc;

use anyhow::Result;
use turso_core::{Connection, Database, DatabaseOpts, IO, OpenFlags, StepResult};

use crate::runner::memory::io::MemorySimIO;

fn make_conn(seed: u64) -> Result<(Arc<Connection>, Arc<MemorySimIO>)> {
    let io = Arc::new(MemorySimIO::new(
        seed, 4096, 100, // Always schedule operations asynchronously.
        1, 5,
    ));
    let db = Database::open_file_with_flags(
        io.clone() as Arc<dyn IO>,
        &format!("sim_upsert_trigger_{seed}.db"),
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
    )?;
    Ok((db.connect()?, io))
}

fn query_rows(conn: &Arc<Connection>, io: &MemorySimIO) -> Result<Vec<(i64, String, String)>> {
    let mut stmt = conn.prepare(
        "SELECT id, k, v FROM t INDEXED BY sqlite_autoindex_t_1 WHERE k = 'mid' AND v = 'new'",
    )?;
    let mut rows = Vec::new();
    loop {
        match stmt.step()? {
            StepResult::IO => io.step()?,
            StepResult::Row => {
                let row = stmt.row().expect("row should exist");
                rows.push((
                    row.get::<i64>(0).expect("id column should exist"),
                    row.get::<String>(1).expect("k column should exist"),
                    row.get::<String>(2).expect("v column should exist"),
                ));
            }
            StepResult::Done => return Ok(rows),
            other => panic!("unexpected step result: {other:?}"),
        }
    }
}

fn integrity_check(conn: &Arc<Connection>, io: &MemorySimIO) -> Result<String> {
    let mut stmt = conn.prepare("PRAGMA integrity_check")?;
    loop {
        match stmt.step()? {
            StepResult::IO => io.step()?,
            StepResult::Row => {
                let row = stmt.row().expect("integrity_check row should exist");
                return Ok(row
                    .get::<String>(0)
                    .expect("integrity_check should return text"));
            }
            StepResult::Done => panic!("integrity_check ended without a row"),
            other => panic!("unexpected step result: {other:?}"),
        }
    }
}

#[test]
fn sim_upsert_before_update_trigger_keeps_index_integrity() -> Result<()> {
    let (conn, io) = make_conn(1)?;

    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, k TEXT, v TEXT, UNIQUE(k, v))")?;
    conn.execute("INSERT INTO t VALUES(1, 'a', 'old')")?;
    conn.execute(
        "CREATE TRIGGER bu BEFORE UPDATE ON t BEGIN UPDATE t SET k = 'mid' WHERE id = OLD.id; END",
    )?;
    conn.execute(
        "INSERT INTO t VALUES(1, 'ignored', 'ignored') \
         ON CONFLICT(id) DO UPDATE SET v = 'new'",
    )?;

    assert_eq!(
        query_rows(&conn, io.as_ref())?,
        vec![(1, "mid".into(), "new".into())]
    );
    assert_eq!(integrity_check(&conn, io.as_ref())?, "ok");
    Ok(())
}
