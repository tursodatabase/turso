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

#[test]
fn test_checkpoint_rejected_while_parked_statement_holds_pragma_helper() {
    // A live pragma-vtab cursor stores a nested helper statement, so the
    // connection counts as "inside a nested statement" for the whole time the
    // outer SELECT is parked between rows. The checkpoint guard must not be
    // skipped because of that: the parked root SELECT is still active, and a
    // checkpoint would clear the page cache under its cursors.
    let io = Arc::new(MemoryYieldIO::new());
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("pragma-helper-checkpoint.db");
    let path = path.to_str().unwrap();

    seed_freelist_db(&io, path);
    let conn = open_conn(io.clone(), path);

    let mut stmt = conn
        .prepare("SELECT t.id FROM pragma_table_info('t') p, t")
        .unwrap();
    let mut ids = Vec::new();
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                ids.push(stmt.row().unwrap().get::<i64>(0).unwrap());
                break;
            }
            StepResult::IO => io.step().unwrap(),
            StepResult::Yield => {}
            other => panic!("unexpected SELECT step result: {other:?}"),
        }
    }

    let err = conn
        .prepare("PRAGMA wal_checkpoint(PASSIVE)")
        .unwrap()
        .run_collect_rows()
        .expect_err("checkpoint must reject the parked SELECT");
    assert!(
        matches!(err, LimboError::StatementsInProgress(_)),
        "unexpected checkpoint error: {err:?}"
    );

    // The parked SELECT resumes cleanly and returns the full result:
    // 2 pragma_table_info rows (id, b) crossed with t's rows (1, 5).
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => ids.push(stmt.row().unwrap().get::<i64>(0).unwrap()),
            StepResult::IO => io.step().unwrap(),
            StepResult::Yield => {}
            StepResult::Done => break,
            other => panic!("unexpected resumed SELECT step result: {other:?}"),
        }
    }
    ids.sort_unstable();
    assert_eq!(ids, vec![1, 1, 5, 5]);
    drop(stmt);

    conn.execute("PRAGMA wal_checkpoint(PASSIVE)").unwrap();
}

#[test]
fn test_open_blob_handle_does_not_block_explicit_checkpoints() {
    // A blob handle keeps a Root statement parked at its row until close.
    // That must not count as "a statement in progress" for explicit
    // checkpoints (SQLite checkpoints fine with open blob handles).
    use turso_core::CheckpointMode;
    let io = Arc::new(MemoryYieldIO::new());
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("blob-checkpoint.db");
    let path = path.to_str().unwrap();

    seed_freelist_db(&io, path);
    let conn = open_conn(io.clone(), path);
    conn.execute("UPDATE t SET b = x'00112233445566778899aabbccddeeff' WHERE id = 1")
        .unwrap();

    let mut blob = conn.blob_open("t", "b", 1, false).unwrap();
    let mut buf = [0u8; 4];
    blob.read(0, &mut buf).unwrap();
    assert_eq!(buf, [0x00, 0x11, 0x22, 0x33]);

    conn.prepare("PRAGMA wal_checkpoint(PASSIVE)")
        .unwrap()
        .run_collect_rows()
        .expect("PRAGMA wal_checkpoint must run with an open blob handle");
    conn.checkpoint(CheckpointMode::Passive {
        upper_bound_inclusive: None,
    })
    .expect("Connection::checkpoint must run with an open blob handle");

    // The checkpoint's cache clear may drop the handle's position, in which
    // case the read fails cleanly with BlobHandleExpired (the behavior
    // before the guard existed) — never with a checkpoint refusal, and
    // never wrong bytes.
    match blob.read(4, &mut buf) {
        Ok(()) => assert_eq!(buf, [0x44, 0x55, 0x66, 0x77]),
        Err(err) => assert!(
            matches!(err, LimboError::BlobHandleExpired),
            "unexpected blob read error: {err:?}"
        ),
    }
    blob.close().unwrap();

    // Closing the handle released its statement count: the counts are back
    // in balance and an idle checkpoint still works.
    conn.checkpoint(CheckpointMode::Passive {
        upper_bound_inclusive: None,
    })
    .unwrap();
}

#[test]
fn test_suspended_wal_checkpoint_rejects_new_statement() {
    let io = Arc::new(MemoryYieldIO::new());
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("checkpoint-before-insert.db");
    let path = path.to_str().unwrap();

    seed_freelist_db(&io, path);
    open_conn(io.clone(), path)
        .execute("UPDATE t SET b = zeroblob(51000) WHERE id = 1")
        .unwrap();

    let conn = open_conn(io.clone(), path);
    let mut checkpoint = conn.prepare("PRAGMA wal_checkpoint(PASSIVE)").unwrap();
    loop {
        match checkpoint.step().unwrap() {
            StepResult::IO if checkpoint.get_pager().is_checkpointing() => break,
            StepResult::IO => io.step().unwrap(),
            StepResult::Yield => {}
            other => panic!("checkpoint completed before suspension: {other:?}"),
        }
    }

    let mut insert = conn
        .prepare("INSERT INTO t VALUES (100, zeroblob(50000))")
        .unwrap();
    let err = insert
        .step()
        .expect_err("suspended checkpoint must reject a new statement");
    assert!(
        matches!(err, LimboError::StatementsInProgress(_)),
        "unexpected INSERT error: {err:?}"
    );
    drop(insert);

    loop {
        match checkpoint.step().unwrap() {
            StepResult::IO => io.step().unwrap(),
            StepResult::Row | StepResult::Yield => {}
            StepResult::Done => break,
            other => panic!("unexpected checkpoint result: {other:?}"),
        }
    }
    drop(checkpoint);
    assert!(!conn.get_pager().is_checkpointing());

    conn.execute("INSERT INTO t VALUES (100, zeroblob(50000))")
        .unwrap();
}

#[test]
fn test_checkpoint_api_rejects_suspended_statement_and_keeps_it_resumable() {
    use turso_core::CheckpointMode;
    for target_io in 1..=512 {
        let io = Arc::new(MemoryYieldIO::new());
        let temp_dir = tempfile::TempDir::new().unwrap();
        let path = temp_dir
            .path()
            .join(format!("api-checkpoint-{target_io}.db"));
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

        // The direct checkpoint API must refuse just like PRAGMA
        // wal_checkpoint does: checkpointing invalidates this connection's
        // cursors and page cache, which would break the suspended INSERT.
        let err = conn
            .checkpoint(CheckpointMode::Passive {
                upper_bound_inclusive: None,
            })
            .expect_err("Connection::checkpoint must reject a suspended statement");
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

        // The rejected checkpoint must not leave checkpoint state behind:
        // a fresh checkpoint on the idle connection succeeds.
        conn.checkpoint(CheckpointMode::Passive {
            upper_bound_inclusive: None,
        })
        .unwrap_or_else(|err| panic!("target_io={target_io}: idle checkpoint failed: {err}"));

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
        assert_eq!(integrity[0][0].to_string(), "ok", "target_io={target_io}");
    }
}
