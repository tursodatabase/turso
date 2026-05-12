use std::sync::Arc;

use anyhow::Result;
use turso_core::{Connection, Database, DatabaseOpts, IO, OpenFlags, StepResult};

use crate::runner::memory::io::MemorySimIO;

fn make_conn(seed: u64) -> Result<(Arc<Connection>, Arc<MemorySimIO>)> {
    let io = Arc::new(MemorySimIO::new(seed, 4096, 100, 1, 5));
    let db = Database::open_file_with_flags(
        io.clone() as Arc<dyn IO>,
        &format!("sim_rowid_rollback_{seed}.db"),
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
    )?;
    let conn = db.connect()?;
    Ok((conn, io))
}

fn query_rows(conn: &Arc<Connection>, io: &MemorySimIO) -> Result<Vec<(i64, String)>> {
    let mut stmt = conn.prepare("SELECT id, val FROM t ORDER BY id")?;
    let mut rows = Vec::new();
    loop {
        match stmt.step()? {
            StepResult::IO => io.step()?,
            StepResult::Row => {
                let row = stmt.row().expect("row should exist");
                rows.push((
                    row.get::<i64>(0).expect("id column should exist"),
                    row.get::<String>(1).expect("val column should exist"),
                ));
            }
            StepResult::Done => return Ok(rows),
            other => panic!("unexpected step result: {other:?}"),
        }
    }
}

#[test]
fn sim_insert_or_fail_rowid_mismatch_rolls_back_statement() -> Result<()> {
    let (conn, io) = make_conn(21)?;
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")?;

    let err = conn
        .execute("INSERT OR FAIL INTO t VALUES (1, 'one'), ('bad', 'bad')")
        .expect_err("rowid mismatch should fail");
    assert_eq!(err.to_string(), "Runtime error: datatype mismatch");
    assert_eq!(query_rows(&conn, io.as_ref())?, Vec::<(i64, String)>::new());
    Ok(())
}

#[test]
fn sim_update_or_fail_rowid_mismatch_rolls_back_statement() -> Result<()> {
    let (conn, io) = make_conn(22)?;
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")?;
    conn.execute("INSERT INTO t VALUES (1, 'one'), (2, 'two')")?;

    let err = conn
        .execute(
            "UPDATE OR FAIL t \
             SET val = 'changed', id = CASE WHEN id = 2 THEN 'bad' ELSE id END \
             WHERE TRUE",
        )
        .expect_err("rowid mismatch should fail");
    assert_eq!(err.to_string(), "Runtime error: datatype mismatch");
    assert_eq!(
        query_rows(&conn, io.as_ref())?,
        vec![(1, "one".to_string()), (2, "two".to_string())]
    );
    Ok(())
}
