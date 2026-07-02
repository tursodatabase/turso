//! Regression test for issue #7694: a reentrant DELETE must not raise
//! SQLITE_CORRUPT when it observes the transient, half-applied index state of an
//! UPDATE on the same connection that has yielded on I/O mid-execution.
//!
//! The reproduction needs `MemoryYieldIO` to deterministically suspend the
//! UPDATE between its Phase-2 `IdxDelete` and the row rewrite. `MemoryYieldIO`
//! is exposed from `turso_core` behind the test-only `io_memory_yield` feature,
//! so this module is compiled only when `core_tester` is built with that
//! feature enabled (`--features io_memory_yield`).

use std::sync::Arc;

use turso_core::{Database, DatabaseOpts, MemoryYieldIO, OpenFlags, StepResult, IO};

fn execute(conn: &Arc<turso_core::Connection>, io: &Arc<MemoryYieldIO>, sql: &str) {
    let mut stmt = conn.prepare(sql).unwrap();
    loop {
        match stmt.step().unwrap() {
            StepResult::IO => io.step().unwrap(),
            StepResult::Yield | StepResult::Row => {}
            StepResult::Done => break,
            step => panic!("unexpected step result: {step:?}"),
        }
    }
}

fn step_until_io(stmt: &mut turso_core::Statement, io: &Arc<MemoryYieldIO>, target: usize) {
    let mut seen = 0;
    loop {
        match stmt.step().unwrap() {
            StepResult::IO => {
                seen += 1;
                io.step().unwrap();
                if seen == target {
                    break;
                }
            }
            StepResult::Yield | StepResult::Row => {}
            StepResult::Done => panic!("statement completed before io yield {target}"),
            step => panic!("unexpected step result: {step:?}"),
        }
    }
}

#[test]
fn test_reentrant_delete_during_yielded_update_does_not_see_partial_index_state() {
    let io = Arc::new(MemoryYieldIO::new());
    let db = Database::open_file_with_flags(
        io.clone(),
        "idxdelete-reentrant-update.db",
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
    )
    .unwrap();
    let conn = db.connect().unwrap();

    for sql in [
        "PRAGMA page_size = 512",
        "PRAGMA cache_size = 7",
        "PRAGMA cache_spill = ON",
        "PRAGMA journal_mode = WAL",
        "CREATE TABLE t(id INTEGER PRIMARY KEY, c UNIQUE, pad BLOB)",
        "BEGIN",
    ] {
        execute(&conn, &io, sql);
    }

    for id in 310..334 {
        let c = if id == 322 {
            "NULL".to_string()
        } else {
            id.to_string()
        };
        execute(
            &conn,
            &io,
            &format!("INSERT INTO t VALUES({id}, {c}, zeroblob(20))"),
        );
    }

    for sql in ["COMMIT", "PRAGMA wal_checkpoint(TRUNCATE)", "BEGIN"] {
        execute(&conn, &io, sql);
    }

    let mut update = conn
        .prepare("UPDATE t SET c = zeroblob(400) WHERE id = 322")
        .unwrap();
    step_until_io(&mut update, &io, 4);

    execute(&conn, &io, "DELETE FROM t WHERE id = 322");
}
