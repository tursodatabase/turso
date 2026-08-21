//! A COMMIT that fails because of a WAL I/O error must leave the transaction
//! fully rolled back — not half-applied and silently committed by a later
//! statement.
//!
//! Regression test. Inside an explicit `BEGIN … COMMIT`, if the WAL header
//! write or its fsync failed at submit time (how `UnixIO` surfaces a failed
//! `pwrite`/`fsync` syscall — ENOSPC, EIO, …), `COMMIT` returned an I/O error,
//! yet the transaction's rows stayed visible on the same connection and the
//! next statement silently committed them.
//!
//! Root cause: `op_auto_commit` flips the connection back to autocommit
//! *before* the commit succeeds, so when the commit errored, `abort()` keyed
//! its rollback decision off the autocommit flag, treated the still-open write
//! transaction as "not in a transaction", and skipped the rollback — leaving
//! dirty pages in the cache for the next statement to flush. The fix keys the
//! rollback off the open write transaction as well.
//!
//! The submit-time shape matters: an error surfaced on a *completed* async op
//! takes a different path (`commit_state` is already `Committing` after the
//! yield, which triggers the rollback via `can_autocommit_now`). Only a
//! synchronously-failing submit reproduces the bug, hence
//! [`UnreliableIo::arm_io_error`] rather than the queued-completion fault of
//! `queued_io`.

use std::sync::Arc;

use rusqlite::types::Value;
use turso_core::{Database, SqliteDialect};

use crate::common::limbo_exec_rows;
use crate::unreliable_io::{UnreliableIo, UnreliableOp};

/// `commit_target` is the WAL operation whose submit-time failure aborts the
/// commit: the header write, or the header fsync. The fault targets operations
/// on the `-wal` file, which hit the header only because the scenario
/// truncates the WAL first — a fresh WAL generation starts by rewriting and
/// re-syncing the WAL header (via `pwrite`; frames go through `pwritev`).
fn assert_failed_commit_rolls_back(commit_target: UnreliableOp) {
    let io = Arc::new(UnreliableIo::new());
    // Distinct path per test: databases are process-globally registered by
    // file identity, so same-named files must not coexist across tests.
    let db_path = format!("commit-wal-io-error-{commit_target:?}.db");
    let wal_path = format!("{db_path}-wal");
    let db = Database::open_file(io.clone(), &db_path, Arc::new(SqliteDialect)).unwrap();
    let conn = db.connect().unwrap();
    conn.execute("PRAGMA synchronous=FULL").unwrap();
    conn.execute("PRAGMA data_sync_retry=ON").unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES(1, 'committed')")
        .unwrap();
    // Reset the WAL so the next commit rewrites and re-syncs the WAL header.
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    assert_eq!(
        limbo_exec_rows(&conn, "SELECT count(*) FROM t"),
        [[Value::Integer(1)]],
        "baseline before the failing commit"
    );

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t VALUES(2, 'must-not-persist')")
        .unwrap();

    io.arm_io_error(&wal_path, commit_target);
    let commit_result = conn.execute("COMMIT");
    assert!(
        io.io_error_fired(),
        "the injected WAL {commit_target:?} fault must actually be reached"
    );
    io.clear_io_error();

    assert!(
        commit_result.is_err(),
        "COMMIT must report the WAL {commit_target:?} I/O error"
    );
    assert_eq!(
        limbo_exec_rows(&conn, "SELECT count(*) FROM t"),
        [[Value::Integer(1)]],
        "a COMMIT that failed on a WAL {commit_target:?} error must roll back, \
         not leave the row committed"
    );

    // The torn state used to surface here: an unrelated later statement flushed
    // the leaked dirty page, committing the "failed" transaction after the fact.
    conn.execute("INSERT INTO t VALUES(3, 'next')").unwrap();
    assert_eq!(
        limbo_exec_rows(&conn, "SELECT count(*) FROM t"),
        [[Value::Integer(2)]],
        "a later statement must not carry the rolled-back transaction's row"
    );
    assert_eq!(
        limbo_exec_rows(&conn, "PRAGMA integrity_check"),
        [[Value::Text("ok".into())]],
        "database stays consistent"
    );

    // The rolled-back row must never appear, even after reopening.
    drop(conn);
    drop(db);
    let db = Database::open_file(io.clone(), &db_path, Arc::new(SqliteDialect)).unwrap();
    let conn = db.connect().unwrap();
    assert_eq!(
        limbo_exec_rows(&conn, "SELECT id FROM t ORDER BY id"),
        [[Value::Integer(1)], [Value::Integer(3)]],
        "reopened database matches the committed rows"
    );
}

#[test]
fn failed_commit_on_wal_header_write_error_rolls_back() {
    assert_failed_commit_rolls_back(UnreliableOp::Pwrite);
}

#[test]
fn failed_commit_on_wal_header_sync_error_rolls_back() {
    assert_failed_commit_rolls_back(UnreliableOp::Sync);
}
