use std::sync::Arc;

use turso_core::{ConnectOpts, Database, OpenFlags, SessionRole, Value, IO};
use turso_sdk_kit::rsapi::{
    SyncBusyGate, SyncBusyGuard, TursoConnection, TursoDatabaseConfig, TursoError, TursoStatusCode,
};
use turso_sdk_kit::IoBackend;
use turso_sqlite::SqliteDialect;

use crate::queued_io::QueuedIo;

/// A sync operation starts while the app's COMMIT waits for I/O, and the next step of the
/// COMMIT either fails with Busy or finishes. Each pass turns the sync busy gate on after one
/// more step of the COMMIT. A COMMIT that is refused must leave nothing committed once the app
/// drops it, also after the database is opened again. A COMMIT that is let through can no longer
/// be undone (an MVCC commit that issued the write of its log record or is in its auto-checkpoint,
/// a WAL commit in its auto-checkpoint), and its rows must be there after the database is opened
/// again. A WAL commit must finish while the sync operation holds a WAL session open on another
/// connection, as a pull does. An MVCC commit must finish without running its auto-checkpoint
/// while the sync operation is active.
#[test]
fn sync_operation_refuses_a_commit_only_while_it_can_be_rolled_back() {
    // An MVCC `BEGIN` transaction writes its own log record. A `BEGIN CONCURRENT` transaction
    // goes through group commit, where the group leader writes the record.
    for (journal_mode, begin) in [
        ("wal", "BEGIN"),
        ("mvcc", "BEGIN"),
        ("mvcc", "BEGIN CONCURRENT"),
    ] {
        let mut commits_refused = 0;
        let mut commits_let_through = 0;
        for steps_before_sync in 1.. {
            let io = Arc::new(QueuedIo::new());
            let db = open_database(&io);
            let conn = db
                .connect(ConnectOpts::new(SessionRole::Superuser))
                .unwrap();
            conn.execute(format!("PRAGMA journal_mode = '{journal_mode}'"))
                .unwrap();
            // Make the COMMIT run an auto-checkpoint after it is durable. A WAL commit runs it
            // after it releases the write lock.
            conn.execute("PRAGMA wal_autocheckpoint = 1").unwrap();
            if journal_mode == "mvcc" {
                conn.execute("PRAGMA mvcc_checkpoint_threshold = 1")
                    .unwrap();
            }
            conn.execute("CREATE TABLE t(x)").unwrap();
            let other = db
                .connect(ConnectOpts::new(SessionRole::Superuser))
                .unwrap();
            let sync_conn = db
                .connect(ConnectOpts::new(SessionRole::Superuser))
                .unwrap();

            let gate = Arc::new(SyncBusyGate::default());
            let app =
                TursoConnection::new_with_sync_busy(&async_io_config(), conn, Some(gate.clone()));
            let other =
                TursoConnection::new_with_sync_busy(&async_io_config(), other, Some(gate.clone()));
            execute(&app, &io, begin);
            // More rows than `MVCC_COMMIT_BATCH_SIZE`, so that an MVCC COMMIT yields while it builds its
            // log record, before it writes it.
            execute(
                &app,
                &io,
                "WITH RECURSIVE c(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM c WHERE i < 2000) \
                 INSERT INTO t SELECT i FROM c",
            );

            let mut commit = app.prepare_single("COMMIT").unwrap();
            let mut done = false;
            for _ in 0..steps_before_sync {
                match commit.step(None).unwrap() {
                    TursoStatusCode::Io => io.step().unwrap(),
                    TursoStatusCode::Done => done = true,
                    TursoStatusCode::Row => unreachable!(),
                }
                if done {
                    break;
                }
            }
            if done {
                break;
            }

            let sync = SyncBusyGuard::new(gate.clone());
            assert!(
                matches!(
                    other.prepare_single("SELECT count(*) FROM t"),
                    Err(TursoError::Busy(_))
                ),
                "{journal_mode} {begin}: a connection that is not committing must still get Busy"
            );
            // A WAL COMMIT that still holds the write lock keeps the sync operation out. An MVCC
            // COMMIT does not take the WAL write lock, so a WAL session here would make its
            // auto-checkpoint fail with Busy, and the test could not see whether it ran.
            let wal_session = journal_mode == "wal" && sync_conn.wal_insert_begin().is_ok();
            let logical_log_offset = || {
                db.get_mv_store()
                    .as_ref()
                    .map(|mv_store| mv_store.logical_log_offset())
            };
            // An auto-checkpoint truncates the logical log, and nothing else makes it shorter.
            let mut last_logical_log_offset = logical_log_offset();
            let mut assert_no_checkpoint = || {
                let offset = logical_log_offset();
                assert!(
                    offset >= last_logical_log_offset,
                    "{journal_mode} {begin}: {steps_before_sync} steps, an MVCC auto-checkpoint \
                     ran while the sync operation was active"
                );
                last_logical_log_offset = offset;
            };
            let committed = loop {
                let result = commit.step(None);
                assert_no_checkpoint();
                match result {
                    Ok(TursoStatusCode::Io) => {
                        io.step().unwrap();
                        assert_no_checkpoint();
                    }
                    Ok(TursoStatusCode::Done) => break true,
                    Err(TursoError::Busy(_)) => break false,
                    result => panic!(
                        "{journal_mode} {begin}: COMMIT returned {result:?} after a sync operation \
                         started {steps_before_sync} steps into it"
                    ),
                }
            };
            drop(commit);
            if wal_session {
                sync_conn.wal_insert_end(false).unwrap();
            }
            drop(sync);
            if committed {
                commits_let_through += 1;
            } else {
                commits_refused += 1;
                assert!(app.get_auto_commit(), "{journal_mode} {begin}");
            }

            let expected = Value::from_i64(if committed { 2000 } else { 0 });
            assert_eq!(
                count_rows(&other, &io),
                expected,
                "{journal_mode} {begin}: {steps_before_sync} steps, COMMIT let through: {committed}"
            );
            drop((app, other, sync_conn, db));
            let db = open_database(&io);
            let conn = db
                .connect(ConnectOpts::new(SessionRole::Superuser))
                .unwrap();
            let conn = TursoConnection::new_with_sync_busy(&async_io_config(), conn, None);
            assert_eq!(
                count_rows(&conn, &io),
                expected,
                "{journal_mode} {begin}: {steps_before_sync} steps, after reopen"
            );
        }
        assert!(
            commits_let_through > 0,
            "{journal_mode} {begin}: no COMMIT was let through"
        );
        if journal_mode == "mvcc" {
            assert!(commits_refused > 0, "mvcc {begin}: no COMMIT was refused");
        }
    }
}

fn open_database(io: &Arc<QueuedIo>) -> Arc<Database> {
    Database::open_file(io.clone(), "sync_busy_gate.db", Arc::new(SqliteDialect)).unwrap()
}

fn async_io_config() -> TursoDatabaseConfig {
    TursoDatabaseConfig {
        path: "sync_busy_gate.db".to_string(),
        experimental_features: None,
        async_io: true,
        encryption: None,
        vfs: IoBackend::Default,
        io: None,
        db_file: None,
        page_codec: None,
        open_flags: OpenFlags::default(),
    }
}

fn execute(conn: &TursoConnection, io: &QueuedIo, sql: &str) {
    let mut stmt = conn.prepare_single(sql).unwrap();
    while stmt.step(None).unwrap() != TursoStatusCode::Done {
        io.step().unwrap();
    }
}

fn count_rows(conn: &TursoConnection, io: &QueuedIo) -> Value {
    let mut stmt = conn.prepare_single("SELECT count(*) FROM t").unwrap();
    while stmt.step(None).unwrap() != TursoStatusCode::Row {
        io.step().unwrap();
    }
    stmt.row_value(0).unwrap()
}
