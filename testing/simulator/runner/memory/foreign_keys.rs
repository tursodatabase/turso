use std::sync::Arc;

use anyhow::Result;
use turso_core::{Connection, Database, DatabaseOpts, IO, OpenFlags, StepResult};

use crate::runner::memory::io::MemorySimIO;

fn make_conn(seed: u64) -> Result<(Arc<Connection>, Arc<MemorySimIO>)> {
    let io = Arc::new(MemorySimIO::new(seed, 4096, 100, 1, 5));
    let db = Database::open_file_with_flags(
        io.clone() as Arc<dyn IO>,
        &format!("sim_fk_pragma_{seed}.db"),
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
    )?;
    Ok((db.connect()?, io))
}

fn query_count(conn: &Arc<Connection>, io: &MemorySimIO, table: &str) -> Result<i64> {
    let mut stmt = conn.prepare(format!("SELECT COUNT(*) FROM {table}"))?;
    loop {
        match stmt.step()? {
            StepResult::IO => io.step()?,
            StepResult::Row => {
                let row = stmt.row().expect("row should exist for count query");
                return Ok(row.get::<i64>(0).expect("count column should exist"));
            }
            StepResult::Done => panic!("count query ended without a row"),
            other => panic!("unexpected step result: {other:?}"),
        }
    }
}

#[test]
fn sim_foreign_keys_pragma_ignored_inside_transaction() -> Result<()> {
    let (conn, io) = make_conn(214)?;

    conn.execute("PRAGMA foreign_keys=OFF")?;
    conn.execute("CREATE TABLE p(id INTEGER PRIMARY KEY)")?;
    conn.execute(
        "CREATE TABLE c(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id) ON DELETE CASCADE)",
    )?;
    conn.execute("INSERT INTO p VALUES(1)")?;
    conn.execute("INSERT INTO c VALUES(10,1)")?;

    conn.execute("BEGIN")?;
    conn.execute("PRAGMA foreign_keys=ON")?;
    conn.execute("DELETE FROM p WHERE id=1")?;
    conn.execute("COMMIT")?;

    assert_eq!(query_count(&conn, io.as_ref(), "p")?, 0);
    assert_eq!(query_count(&conn, io.as_ref(), "c")?, 1);
    Ok(())
}
