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

fn query_log_summary(conn: &Arc<Connection>, io: &MemorySimIO) -> Result<(i64, String)> {
    let mut stmt = conn.prepare("SELECT count(*), group_concat(v, ',') FROM log")?;
    loop {
        match stmt.step()? {
            StepResult::IO => io.step()?,
            StepResult::Row => {
                let row = stmt.row().expect("row should exist for log summary");
                return Ok((
                    row.get::<i64>(0).expect("count column should exist"),
                    row.get::<String>(1)
                        .expect("group_concat column should exist"),
                ));
            }
            StepResult::Done => panic!("summary query ended without a row"),
            other => panic!("unexpected step result: {other:?}"),
        }
    }
}

#[test]
fn sim_create_view_does_not_duplicate_same_named_trigger() -> Result<()> {
    let seed = 301;
    let io = make_io(seed);
    let path = format!("sim_view_trigger_same_name_{seed}.db");

    let conn = open_conn(io.clone(), &path)?;
    for stmt in [
        "CREATE TABLE t(x INTEGER)",
        "CREATE TABLE log(v INTEGER)",
        "CREATE TRIGGER dup AFTER INSERT ON t BEGIN INSERT INTO log VALUES (NEW.x); END",
        "CREATE VIEW dup AS SELECT x FROM t",
        "INSERT INTO t VALUES (1)",
    ] {
        conn.execute(stmt)?;
    }

    assert_eq!(query_log_summary(&conn, io.as_ref())?, (1, "1".to_string()));

    drop(conn);
    let conn = open_conn(io.clone(), &path)?;
    assert_eq!(query_log_summary(&conn, io.as_ref())?, (1, "1".to_string()));
    Ok(())
}
