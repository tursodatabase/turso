#![cfg(feature = "io_memory_yield")]

use std::sync::Arc;

use turso_core::{Database, MemoryYieldIO, StepResult, IO};

fn exec_sql(conn: &Arc<turso_core::Connection>, io: &dyn IO, sql: &str) -> turso_core::Result<()> {
    let mut stmt = conn.prepare(sql)?;
    loop {
        match stmt.step()? {
            StepResult::IO | StepResult::Yield => io.step()?,
            StepResult::Row => {}
            StepResult::Done => return Ok(()),
            StepResult::Interrupt | StepResult::Busy => return Err(turso_core::LimboError::Busy),
        }
    }
}

fn drop_statement_at_io(
    conn: &Arc<turso_core::Connection>,
    io: &dyn IO,
    sql: &str,
    target_io: usize,
) {
    let mut stmt = conn.prepare(sql).unwrap();
    let mut io_count = 0usize;
    loop {
        match stmt.step().unwrap() {
            StepResult::IO | StepResult::Yield => {
                io_count += 1;
                if io_count == target_io {
                    drop(stmt);
                    return;
                }
                io.step().unwrap();
            }
            StepResult::Row => {}
            StepResult::Done => {
                panic!("statement completed before IO #{target_io}; saw {io_count} IOs");
            }
            StepResult::Interrupt | StepResult::Busy => {
                panic!("statement did not reach IO boundary");
            }
        }
    }
}

#[test]
fn test_abandoned_create_index_does_not_poison_later_allocation() {
    let io = Arc::new(MemoryYieldIO::new());
    let db = Database::open_file(io.clone(), "repro_public_freelist_leaf_exact.db").unwrap();
    let setup_conn = db.connect().unwrap();

    exec_sql(&setup_conn, io.as_ref(), "PRAGMA page_size = 512").unwrap();
    exec_sql(&setup_conn, io.as_ref(), "CREATE TABLE t(v BLOB)").unwrap();
    exec_sql(&setup_conn, io.as_ref(), "CREATE TABLE scratch(v BLOB)").unwrap();
    exec_sql(
        &setup_conn,
        io.as_ref(),
        "INSERT INTO scratch VALUES(zeroblob(50000))",
    )
    .unwrap();
    exec_sql(&setup_conn, io.as_ref(), "DELETE FROM scratch").unwrap();

    for rowid in 1..=80 {
        exec_sql(
            &setup_conn,
            io.as_ref(),
            &format!("INSERT INTO t(rowid, v) VALUES({rowid}, zeroblob(220))"),
        )
        .unwrap();
    }
    drop(setup_conn);

    let conn = db.connect().unwrap();
    exec_sql(&conn, io.as_ref(), "BEGIN").unwrap();
    drop_statement_at_io(&conn, io.as_ref(), "CREATE INDEX idx_t_v ON t(v)", 45);

    exec_sql(&conn, io.as_ref(), "CREATE TABLE u(v BLOB)").unwrap();
    for rowid in 1..=120 {
        exec_sql(
            &conn,
            io.as_ref(),
            &format!("INSERT INTO u(rowid, v) VALUES({rowid}, zeroblob(900))"),
        )
        .unwrap();
    }
}
