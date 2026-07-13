#[cfg(feature = "io_memory_yield")]
use std::sync::Arc;

#[cfg(feature = "io_memory_yield")]
use turso_core::{Database, DatabaseOpts, OpenFlags, StepResult};

#[cfg(feature = "io_memory_yield")]
#[test]
fn active_journal_mode_mvcc_interleaved_insert_does_not_corrupt_reopen() {
    let io = Arc::new(turso_core::MemoryYieldIO::new());
    let path = "active-mvcc-mode-switch-interleaved-insert.db";

    {
        let db = Database::open_file_with_flags(
            io.clone(),
            path,
            OpenFlags::default(),
            DatabaseOpts::new(),
            None,
        )
        .unwrap();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, x)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES(1,10)").unwrap();

        let mut pragma = conn.prepare("PRAGMA journal_mode = 'mvcc'").unwrap();
        let mut io_count = 0;
        let mut interleaved = false;
        loop {
            match pragma.step().unwrap() {
                StepResult::IO => {
                    io_count += 1;
                    turso_core::IO::step(io.as_ref()).unwrap();
                    if io_count == 6 {
                        interleaved = true;
                        conn.execute("INSERT INTO t VALUES(2,20)").unwrap();
                    }
                }
                StepResult::Row | StepResult::Done => break,
                StepResult::Yield => {}
                other => panic!("unexpected journal_mode=mvcc step result: {other:?}"),
            }
        }
        assert!(
            interleaved,
            "expected PRAGMA journal_mode=mvcc to yield mid-switch so a write could interleave"
        );
    }

    let db =
        Database::open_file_with_flags(io, path, OpenFlags::default(), DatabaseOpts::new(), None)
            .unwrap();
    let conn = db.connect().unwrap();
    let rows = conn
        .prepare("SELECT id,x FROM t ORDER BY id")
        .unwrap()
        .run_collect_rows()
        .unwrap()
        .into_iter()
        .map(|row| (row[0].as_int().unwrap(), row[1].as_int().unwrap()))
        .collect::<Vec<_>>();
    assert_eq!(rows, vec![(1, 10), (2, 20)]);
    conn.execute("BEGIN CONCURRENT").unwrap();
    conn.execute("ROLLBACK").unwrap();
}

#[cfg(feature = "io_memory_yield")]
#[test]
fn writes_after_abandoned_journal_mode_mvcc_survive_fresh_reopen() {
    let io = Arc::new(turso_core::MemoryYieldIO::new());

    {
        let db = Database::open_file_with_flags(
            io.clone(),
            "abandoned-mvcc-followup-write.db",
            OpenFlags::default(),
            DatabaseOpts::new(),
            None,
        )
        .unwrap();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, x)")
            .unwrap();

        let mut stmt = conn.prepare("PRAGMA journal_mode = 'mvcc'").unwrap();
        let mut io_count = 0;
        loop {
            match stmt.step().unwrap() {
                StepResult::IO => {
                    io_count += 1;
                    if io_count == 6 {
                        drop(stmt);
                        break;
                    }
                    turso_core::IO::step(io.as_ref()).unwrap();
                }
                other => panic!("expected sixth IO before completion, got {other:?}"),
            }
        }

        // The switch never reached the final page-1 header flip, so the
        // abandonment guard leaves the connection (and the on-disk header) in
        // plain WAL mode.
        assert!(!conn.is_mvcc_bootstrap_connection());
        let mode = conn
            .prepare("PRAGMA journal_mode")
            .unwrap()
            .run_collect_rows()
            .unwrap()[0][0]
            .to_string();
        assert_eq!(mode, "wal");

        conn.execute("CREATE TABLE u(y)").unwrap();
        conn.execute("INSERT INTO u VALUES(42)").unwrap();

        let rows = conn
            .prepare("SELECT y FROM u")
            .unwrap()
            .run_collect_rows()
            .unwrap()
            .into_iter()
            .map(|row| row[0].as_int().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(rows, vec![42]);
    }

    let db = Database::open_file_with_flags(
        io,
        "abandoned-mvcc-followup-write.db",
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
    )
    .unwrap();
    let conn = db.connect().unwrap();
    let rows = conn
        .prepare("SELECT y FROM u")
        .unwrap()
        .run_collect_rows()
        .unwrap()
        .into_iter()
        .map(|row| row[0].as_int().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(rows, vec![42]);

    // The reopened database is still in WAL mode; re-running the switch to
    // MVCC must complete cleanly and keep the earlier writes visible.
    let mode = conn
        .prepare("PRAGMA journal_mode")
        .unwrap()
        .run_collect_rows()
        .unwrap()[0][0]
        .to_string();
    assert_eq!(mode, "wal");
    conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
    let rows = conn
        .prepare("SELECT y FROM u")
        .unwrap()
        .run_collect_rows()
        .unwrap()
        .into_iter()
        .map(|row| row[0].as_int().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(rows, vec![42]);

    conn.execute("BEGIN CONCURRENT").unwrap();
    conn.execute("ROLLBACK").unwrap();
}
