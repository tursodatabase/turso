#![cfg(feature = "io_memory_yield")]

use std::sync::Arc;

use turso_core::{Database, DatabaseOpts, OpenFlags, StepResult};

#[test]
fn abandoned_indexed_update_without_stmt_journal_does_not_corrupt_index() {
    let old = "a".repeat(20_000);
    let abandoned = "b".repeat(20_000);
    let mut saw_abandoned_update = false;

    for abandon_after_io in 1..=160 {
        let io = Arc::new(turso_core::MemoryYieldIO::new());
        let path = format!("abandoned-indexed-update-split-{abandon_after_io}.db");

        {
            let db = Database::open_file_with_flags(
                io.clone(),
                &path,
                OpenFlags::default(),
                DatabaseOpts::new(),
                None,
            )
            .unwrap();
            let conn = db.connect().unwrap();
            conn.execute("PRAGMA journal_mode = 'wal'").unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, b TEXT)")
                .unwrap();
            conn.execute("CREATE INDEX t_b ON t(b)").unwrap();
            for id in 1..=12 {
                conn.execute(format!("INSERT INTO t VALUES({id}, '{old}')"))
                    .unwrap();
            }
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        }

        let db = Database::open_file_with_flags(
            io.clone(),
            &path,
            OpenFlags::default(),
            DatabaseOpts::new(),
            None,
        )
        .unwrap();
        let conn = db.connect().unwrap();
        conn.execute("BEGIN").unwrap();

        let mut update = conn
            .prepare(format!(
                "UPDATE t SET b = '{abandoned}' WHERE id BETWEEN 1 AND 10"
            ))
            .unwrap();

        let mut io_count = 0;
        let mut abandoned_stmt = false;
        loop {
            match update.step().unwrap() {
                StepResult::IO => {
                    io_count += 1;
                    update._io().step().unwrap();
                    if io_count == abandon_after_io {
                        drop(update);
                        abandoned_stmt = true;
                        saw_abandoned_update = true;
                        break;
                    }
                }
                StepResult::Done => break,
                StepResult::Yield => {}
                other => panic!("unexpected UPDATE step result: {other:?}"),
            }
        }

        if !abandoned_stmt {
            conn.execute("ROLLBACK").unwrap();
            break;
        }

        conn.execute("COMMIT").unwrap_or_else(|err| {
            panic!("abandon_after_io={abandon_after_io}: COMMIT failed after abandoned indexed UPDATE: {err}")
        });

        let integrity = conn
            .prepare("PRAGMA integrity_check")
            .unwrap()
            .run_collect_rows()
            .unwrap()
            .into_iter()
            .map(|row| row[0].to_string())
            .collect::<Vec<_>>();
        assert_eq!(
            integrity,
            vec!["ok".to_string()],
            "abandon_after_io={abandon_after_io}: abandoned indexed UPDATE corrupted the index"
        );

        drop(conn);
        drop(db);

        let reopened = Database::open_file_with_flags(
            io,
            &path,
            OpenFlags::default(),
            DatabaseOpts::new(),
            None,
        )
        .unwrap_or_else(|err| panic!("abandon_after_io={abandon_after_io}: reopen failed: {err}"));
        let reopened_conn = reopened.connect().unwrap();
        let integrity = reopened_conn
            .prepare("PRAGMA integrity_check")
            .unwrap()
            .run_collect_rows()
            .unwrap()
            .into_iter()
            .map(|row| row[0].to_string())
            .collect::<Vec<_>>();
        assert_eq!(
            integrity,
            vec!["ok".to_string()],
            "abandon_after_io={abandon_after_io}: abandoned indexed UPDATE corrupted reopened database"
        );
    }

    assert!(
        saw_abandoned_update,
        "UPDATE completed without any IO boundary"
    );
}
