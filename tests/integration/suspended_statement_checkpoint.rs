#![cfg(feature = "io_memory_yield")]

use std::sync::Arc;

use turso_core::{Connection, Database, LimboError, MemoryYieldIO, SqliteDialect, StepResult, IO};

fn open_conn(io: Arc<MemoryYieldIO>, path: &str) -> Arc<Connection> {
    Database::open_file(io, path, Arc::new(SqliteDialect))
        .unwrap()
        .connect()
        .unwrap()
}

fn seed_freelist_db(io: &Arc<MemoryYieldIO>, path: &str) {
    let conn = open_conn(io.clone(), path);
    conn.execute("PRAGMA page_size=512").unwrap();
    conn.execute("PRAGMA journal_mode = 'wal'").unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, b BLOB)")
        .unwrap();
    conn.execute(
        "INSERT INTO t VALUES
         (1, zeroblob(50000)),
         (2, zeroblob(50000)),
         (3, zeroblob(50000)),
         (4, zeroblob(50000)),
         (5, zeroblob(50000))",
    )
    .unwrap();
    conn.execute("DELETE FROM t WHERE id IN (2,3,4)").unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
}

#[test]
fn test_wal_checkpoint_with_suspended_write_statement_keeps_statement_resumable() {
    for target_io in 1..=512 {
        let io = Arc::new(MemoryYieldIO::new());
        let temp_dir = tempfile::TempDir::new().unwrap();
        let path = temp_dir
            .path()
            .join(format!("suspended-checkpoint-{target_io}.db"));
        let path = path.to_str().unwrap();

        seed_freelist_db(&io, path);
        let conn = open_conn(io.clone(), path);
        let mut stmt = conn
            .prepare("INSERT INTO t VALUES (100, zeroblob(50000))")
            .unwrap();

        let mut io_count = 0;
        let suspended = loop {
            match stmt.step().unwrap() {
                StepResult::IO => {
                    io_count += 1;
                    if io_count == target_io {
                        break true;
                    }
                    io.step().unwrap();
                }
                StepResult::Done => break false,
                StepResult::Yield => {}
                other => panic!("unexpected INSERT step result: {other:?}"),
            }
        };
        if !suspended {
            assert!(
                target_io > 1,
                "INSERT completed without yielding at an I/O boundary"
            );
            break;
        }

        let err = conn
            .prepare("PRAGMA wal_checkpoint(PASSIVE)")
            .unwrap()
            .run_collect_rows()
            .expect_err("checkpoint must reject a concurrent statement");
        assert!(
            matches!(err, LimboError::StatementsInProgress(_)),
            "unexpected checkpoint error: {err:?}"
        );

        loop {
            match stmt
                .step()
                .unwrap_or_else(|err| panic!("target_io={target_io}: resumed INSERT failed: {err}"))
            {
                StepResult::IO => io.step().unwrap(),
                StepResult::Done => break,
                StepResult::Yield => {}
                other => panic!("unexpected resumed INSERT step result: {other:?}"),
            }
        }
        drop(stmt);

        let ids = conn
            .prepare("SELECT id FROM t ORDER BY id")
            .unwrap()
            .run_collect_rows()
            .unwrap()
            .into_iter()
            .map(|row| row[0].as_int().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(ids, vec![1, 5, 100], "target_io={target_io}");

        let integrity = conn
            .prepare("PRAGMA integrity_check")
            .unwrap()
            .run_collect_rows()
            .unwrap();
        assert_eq!(integrity.len(), 1, "target_io={target_io}");
        assert_eq!(integrity[0][0].to_string(), "ok", "target_io={target_io}");
    }
}
