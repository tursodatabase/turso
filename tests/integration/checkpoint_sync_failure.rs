//! A RESTART or TRUNCATE checkpoint whose database-file fsync fails once, then
//! is retried in the same process.
//!
//! After a failed fsync Linux may drop the dirty pages of the file, so the
//! pages the checkpoint copied into the database file are gone and the WAL
//! frames are their only copy. The WAL must therefore not be restarted before
//! the database file is synced: a retried checkpoint has to find the frames
//! and copy them again. When the WAL was restarted first, the retry found
//! nothing to copy, reported success, and the next writer overwrote the
//! frames, losing every committed row.
//!
//! The fsync failure comes from [`crate::unreliable_io::UnreliableIo`], which
//! drops the unsynced writes of the file whose fsync fails. After the retry
//! and one more commit, a power loss keeps only fsynced bytes, and the
//! recovered database must hold every committed row.

use std::sync::Arc;

use turso_core::{ConnectOpts, Connection, Database, DatabaseOpts, OpenFlags, SessionRole};
use turso_sqlite::SqliteDialect;

use crate::unreliable_io::UnreliableIo;

fn query_rows(conn: &Arc<Connection>, sql: &str) -> anyhow::Result<Vec<String>> {
    let mut stmt = conn.prepare(sql)?;
    let mut rows = Vec::new();
    stmt.run_with_row_callback(|row| {
        let values: Vec<String> = row.get_values().map(|value| format!("{value}")).collect();
        rows.push(values.join("|"));
        Ok(())
    })?;
    Ok(rows)
}

/// Runs `statement` (a statement that runs a RESTART or TRUNCATE checkpoint)
/// with the next fsync of the database file failing, then runs it again and
/// commits one more row. `failed` and `retried` check the result of the two
/// runs.
fn run_scenario(
    name: &str,
    statement: &str,
    failed: impl Fn(anyhow::Result<Vec<String>>),
    retried: impl Fn(anyhow::Result<Vec<String>>),
) -> anyhow::Result<()> {
    // Unique per scenario: databases opened on the same path share state
    // process-wide, so parallel test runs must not collide.
    let db_path = format!("checkpoint-sync-failure-{name}.db");
    let io = Arc::new(UnreliableIo::new());
    let db = Database::open_file_with_flags(
        io.clone(),
        &db_path,
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )?;
    let conn = db.connect(ConnectOpts::new(SessionRole::Superuser))?;
    // Going on after a failed fsync is what data_sync_retry = 1 allows; with
    // the default, a failed fsync is documented as fatal.
    conn.execute("PRAGMA data_sync_retry = 1")?;
    conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT)")?;
    conn.execute("INSERT INTO t VALUES (1, 'x'), (2, 'y'), (3, 'z')")?;

    // The first fsync of the database file is the one after the checkpoint
    // copied the WAL frames into it.
    io.fail_next_sync(&db_path);
    failed(query_rows(&conn, statement));
    retried(query_rows(&conn, statement));

    // The next commit starts a new WAL. Before the fix it overwrote the only
    // remaining copy of the rows.
    conn.execute("INSERT INTO t VALUES (4, 'w')")?;
    assert_eq!(
        query_rows(&conn, "SELECT a, b FROM t ORDER BY a")?,
        vec!["1|x", "2|y", "3|z", "4|w"],
        "{name}: live connection"
    );

    let (_dir, recovered) = io.crash_now_and_recover(&db_path)?;
    assert_eq!(
        query_rows(&recovered, "SELECT a, b FROM t ORDER BY a")?,
        vec!["1|x", "2|y", "3|z", "4|w"],
        "{name}: every committed row must survive a power loss after the retried checkpoint"
    );
    assert_eq!(
        query_rows(&recovered, "PRAGMA integrity_check")?,
        vec!["ok"],
        "{name}: integrity_check"
    );
    Ok(())
}

/// `PRAGMA wal_checkpoint` reports a failed checkpoint in its busy column.
fn checkpoint_failed(rows: anyhow::Result<Vec<String>>) {
    let rows = rows.expect("PRAGMA wal_checkpoint reports a failure as a row");
    assert_eq!(rows.len(), 1);
    assert!(
        rows[0].starts_with("1|"),
        "the injected fsync failure must fail the first checkpoint, got {rows:?}"
    );
}

fn checkpoint_succeeded(rows: anyhow::Result<Vec<String>>) {
    let rows = rows.expect("retried PRAGMA wal_checkpoint");
    assert_eq!(rows.len(), 1);
    assert!(
        rows[0].starts_with("0|"),
        "the retried checkpoint must succeed, got {rows:?}"
    );
}

#[test]
fn test_truncate_checkpoint_retry_after_failed_db_fsync_keeps_rows() -> anyhow::Result<()> {
    run_scenario(
        "truncate",
        "PRAGMA wal_checkpoint(TRUNCATE)",
        checkpoint_failed,
        checkpoint_succeeded,
    )
}

#[test]
fn test_restart_checkpoint_retry_after_failed_db_fsync_keeps_rows() -> anyhow::Result<()> {
    run_scenario(
        "restart",
        "PRAGMA wal_checkpoint(RESTART)",
        checkpoint_failed,
        checkpoint_succeeded,
    )
}

/// Switching from WAL to MVCC runs a TRUNCATE checkpoint first.
#[test]
fn test_wal_to_mvcc_switch_retry_after_failed_db_fsync_keeps_rows() -> anyhow::Result<()> {
    run_scenario(
        "wal-to-mvcc",
        "PRAGMA journal_mode = 'mvcc'",
        |rows| {
            let err = rows.expect_err("the injected fsync failure must fail the switch");
            assert!(err.to_string().contains("sync"), "unexpected error: {err}");
        },
        |rows| assert_eq!(rows.expect("retried switch"), vec!["mvcc"]),
    )
}
