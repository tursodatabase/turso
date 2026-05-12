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
        &format!("sim_stmt_rollback_{seed}.db"),
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
    )?;
    Ok((db.connect()?, io))
}

fn query_rows(conn: &Arc<Connection>, io: &MemorySimIO, sql: &str) -> Result<Vec<Vec<i64>>> {
    let mut stmt = conn.prepare(sql)?;
    let mut rows = Vec::new();
    loop {
        match stmt.step()? {
            StepResult::IO => io.step()?,
            StepResult::Row => {
                let row = stmt.row().expect("row should exist");
                rows.push(
                    (0..row.get_values().count())
                        .map(|idx| row.get(idx).unwrap())
                        .collect(),
                );
            }
            StepResult::Done => return Ok(rows),
            other => panic!("unexpected step result: {other:?}"),
        }
    }
}

#[test]
fn sim_update_runtime_expression_error_rolls_back_prior_rows() -> Result<()> {
    let (conn, io) = make_conn(501)?;
    conn.execute("PRAGMA journal_mode = WAL")?;
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, x INT)")?;
    conn.execute("INSERT INTO t VALUES(1,10),(2,20)")?;

    conn.execute("BEGIN")?;
    let update = conn.execute(
        "UPDATE t SET x = CASE WHEN id=1 THEN 11 ELSE (char(120) LIKE char(120) ESCAPE (char(121)||char(121))) END",
    );
    assert!(update.is_err(), "UPDATE should fail on invalid ESCAPE");
    assert_eq!(
        query_rows(&conn, io.as_ref(), "SELECT id,x FROM t ORDER BY id")?,
        vec![vec![1, 10], vec![2, 20]]
    );
    conn.execute("COMMIT")?;
    Ok(())
}

#[test]
fn sim_update_partial_index_expression_error_rolls_back_prior_rows() -> Result<()> {
    let (conn, io) = make_conn(502)?;
    conn.execute("PRAGMA journal_mode = WAL")?;
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b INT)")?;
    conn.execute("INSERT INTO t VALUES(1,1,1),(2,2,1),(3,3,1)")?;
    conn.execute(
        "CREATE INDEX idx ON t(a) WHERE CASE WHEN b=2 THEN 'x' LIKE 'x' ESCAPE 'yy' ELSE 1 END",
    )?;

    conn.execute("BEGIN")?;
    let update = conn.execute("UPDATE t SET b=CASE WHEN id=2 THEN 2 ELSE b+10 END");
    assert!(
        update.is_err(),
        "UPDATE should fail while maintaining the partial index"
    );
    assert_eq!(
        query_rows(&conn, io.as_ref(), "SELECT id,a,b FROM t ORDER BY id")?,
        vec![vec![1, 1, 1], vec![2, 2, 1], vec![3, 3, 1]]
    );
    conn.execute("COMMIT")?;
    Ok(())
}

#[test]
fn sim_delete_runtime_expression_error_rolls_back_prior_rows() -> Result<()> {
    let (conn, io) = make_conn(503)?;
    conn.execute("PRAGMA journal_mode = WAL")?;
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, x INT)")?;
    conn.execute("INSERT INTO t VALUES(1,10),(2,20)")?;

    conn.execute("BEGIN")?;
    let delete = conn.execute(
        "DELETE FROM t WHERE CASE WHEN id=2 THEN (char(120) LIKE char(120) ESCAPE (char(121)||char(121))) ELSE 1 END",
    );
    assert!(delete.is_err(), "DELETE should fail on invalid ESCAPE");
    assert_eq!(
        query_rows(&conn, io.as_ref(), "SELECT id,x FROM t ORDER BY id")?,
        vec![vec![1, 10], vec![2, 20]]
    );
    conn.execute("COMMIT")?;
    Ok(())
}
