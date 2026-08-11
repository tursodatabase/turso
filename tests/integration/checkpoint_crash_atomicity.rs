//! Regression tests for issue #7952: checkpoint backfill must be crash-atomic
//! under `PRAGMA synchronous=NORMAL`, and the checkpoint's own WAL fsync must
//! not rob a following `synchronous=FULL` commit of its commit fsync.
//!
//! Under NORMAL, commits do not fsync the WAL. If a checkpoint then backfills
//! those committed-but-not-durable frames into the database file without first
//! forcing the WAL to stable storage, a power loss mid-backfill can persist
//! *some* of the backfilled DB pages while WAL recovery drops the unsynced
//! frames — recovering a torn database that matches no committed prefix, and
//! that `PRAGMA integrity_check` reports as "ok".
//!
//! Under FULL, every commit must be on stable storage by the time COMMIT
//! returns. The commit fsync used to be gated on the WAL dirty flag alone,
//! which is only raised after a commit finishes and which a checkpoint's WAL
//! fsync clears — so the first commit of a fresh WAL generation (a new
//! database, or right after a WAL-resetting checkpoint) was acknowledged
//! without any fsync covering its frames.
//!
//! The crash model is the shared [`crate::unreliable_io::UnreliableIo`]: every
//! write is volatile until the file it targets is fsynced; at the simulated
//! power-loss point the durable image plus an arbitrary (alternating) subset
//! of the unsynced DB-file writes survives, and all unsynced WAL writes are
//! lost. This is a legal power-loss outcome (the kernel may write back dirty
//! DB pages while the WAL tail never reaches the platter).

use std::sync::Arc;

use turso_core::{Database, DatabaseOpts, OpenFlags, SqliteDialect};

use crate::unreliable_io::UnreliableIo;

fn query_rows(conn: &Arc<turso_core::Connection>, sql: &str) -> anyhow::Result<Vec<String>> {
    let mut stmt = conn.prepare(sql)?;
    let mut rows = Vec::new();
    stmt.run_with_row_callback(|row| {
        let values: Vec<String> = row.get_values().map(|value| format!("{value}")).collect();
        rows.push(values.join("|"));
        Ok(())
    })?;
    Ok(rows)
}

/// Runs the crash scenario under the given `PRAGMA synchronous` mode and
/// asserts the committed-prefix recovery contract.
fn run_checkpoint_crash_scenario(sync_pragma: &str) -> anyhow::Result<()> {
    // Unique per scenario: databases opened on the same path share state
    // process-wide, so parallel test runs must not collide.
    let db_path_owned = format!(
        "checkpoint-crash-atomicity-{}.db",
        sync_pragma.to_lowercase()
    );
    let db_path_sim: &str = &db_path_owned;
    const N_ROWS: usize = 256;
    let old = "A".repeat(120);
    let new = "B".repeat(120);

    let io = Arc::new(UnreliableIo::new());
    let db = Database::open_file_with_flags(
        io.clone(),
        db_path_sim,
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )?;
    let conn = db.connect()?;

    // S0: a multi-page table, fully checkpointed and durable.
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")?;
    conn.execute("BEGIN")?;
    for i in 0..N_ROWS {
        conn.execute(format!("INSERT INTO t VALUES({i}, '{old}')"))?;
    }
    conn.execute("COMMIT")?;
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
    io.mark_all_durable();

    // T: a committed transaction touching many pages.
    conn.execute(format!("PRAGMA synchronous={sync_pragma}"))?;
    conn.execute(format!("UPDATE t SET v = '{new}'"))?;

    // Checkpoint T's frames into the DB file; power loss strikes while the
    // backfilled pages are being forced to disk.
    io.arm_crash_on_sync(db_path_sim);
    conn.execute("PRAGMA wal_checkpoint(PASSIVE)")?;

    let snapshot = io
        .take_crash_snapshot()
        .expect("checkpoint never issued the post-backfill DB fsync; crash point not reached");
    assert!(
        snapshot.writes_persisted >= 1 && snapshot.writes_dropped >= 1,
        "crash model must persist a strict subset of the backfill writes \
         (persisted={}, dropped={}); grow the workload if the backfill was a single write",
        snapshot.writes_persisted,
        snapshot.writes_dropped,
    );

    // Materialize the post-crash disk state and recover with a fresh database.
    let dir = tempfile::TempDir::new()?;
    let db_path = dir.path().join("recovered.db");
    let wal_path = dir.path().join("recovered.db-wal");
    std::fs::write(
        &db_path,
        snapshot
            .files
            .get(db_path_sim)
            .expect("db file missing from crash snapshot"),
    )?;
    if let Some(wal) = snapshot.files.get(&format!("{db_path_sim}-wal")) {
        std::fs::write(&wal_path, wal)?;
    }

    let recovered_db = Database::open_file(
        Arc::new(turso_core::PlatformIO::new()?),
        db_path.to_str().unwrap(),
        Arc::new(SqliteDialect),
    )?;
    let recovered = recovered_db.connect()?;

    let integrity = query_rows(&recovered, "PRAGMA integrity_check")?;
    assert_eq!(
        integrity,
        vec!["ok".to_string()],
        "recovered database failed integrity_check"
    );
    let count = query_rows(&recovered, "SELECT COUNT(*) FROM t")?;
    assert_eq!(
        count,
        vec![N_ROWS.to_string()],
        "row count changed across crash recovery"
    );
    let distinct = query_rows(&recovered, "SELECT DISTINCT v FROM t ORDER BY v")?;
    let labels: Vec<String> = distinct
        .iter()
        .map(|v| format!("{}x{}", &v[..1], v.len()))
        .collect();
    assert!(
        distinct == vec![old.clone()] || distinct == vec![new.clone()],
        "torn recovery: database is neither S0 (all 'A') nor the committed \
         transaction T (all 'B'); distinct values: {labels:?}",
    );
    Ok(())
}

/// A crash during checkpoint backfill under `synchronous=NORMAL` (the WAL-mode
/// default durability) must recover to a committed prefix: the database equals
/// either the pre-transaction state S0 (all rows 'A...') or the committed
/// transaction T (all rows 'B...'), never a torn mix — and `integrity_check`
/// must be clean.
#[test]
fn checkpoint_backfill_crash_under_synchronous_normal_recovers_committed_prefix(
) -> anyhow::Result<()> {
    run_checkpoint_crash_scenario("NORMAL")
}

/// Control: the same crash point under `synchronous=FULL`, where the
/// commit-time WAL fsync already made T's frames durable, must heal — this
/// validates that the crash model itself is not the source of the tear.
#[test]
fn checkpoint_backfill_crash_under_synchronous_full_control() -> anyhow::Result<()> {
    run_checkpoint_crash_scenario("FULL")
}

/// Simulates a power loss at this exact moment (every unsynced write in every
/// file is lost) and recovers the surviving image with a fresh database.
/// Returns the tempdir alongside the connection to keep the files alive.
fn crash_now_and_recover(
    io: &UnreliableIo,
    db_path_sim: &str,
) -> anyhow::Result<(tempfile::TempDir, Arc<turso_core::Connection>)> {
    let files = io.durable_files();
    let dir = tempfile::TempDir::new()?;
    let db_path = dir.path().join("recovered.db");
    std::fs::write(
        &db_path,
        files
            .get(db_path_sim)
            .map(Vec::as_slice)
            .unwrap_or_default(),
    )?;
    if let Some(wal) = files.get(&format!("{db_path_sim}-wal")) {
        std::fs::write(dir.path().join("recovered.db-wal"), wal)?;
    }
    let recovered_db = Database::open_file(
        Arc::new(turso_core::PlatformIO::new()?),
        db_path.to_str().unwrap(),
        Arc::new(SqliteDialect),
    )?;
    let recovered = recovered_db.connect()?;
    Ok((dir, recovered))
}

/// Under `synchronous=FULL`, a commit that follows a WAL-resetting checkpoint
/// must fsync the WAL before COMMIT returns. The checkpoint's own WAL fsync
/// clears the WAL dirty flag, so a commit fsync gated on that flag alone is
/// silently skipped and a power loss right after the acknowledged COMMIT
/// loses it.
#[test]
fn full_commit_right_after_truncate_checkpoint_survives_power_loss() -> anyhow::Result<()> {
    let db_path_sim = "full-commit-after-truncate-checkpoint.db";
    let io = Arc::new(UnreliableIo::new());
    let db = Database::open_file_with_flags(
        io.clone(),
        db_path_sim,
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )?;
    let conn = db.connect()?;
    conn.execute("PRAGMA synchronous=FULL")?;
    conn.execute("CREATE TABLE t(x)")?;
    conn.execute("INSERT INTO t VALUES (1)")?;
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;

    // The commit under test: the first commit into the reset WAL.
    conn.execute("INSERT INTO t VALUES (2)")?;

    let (_dir, recovered) = crash_now_and_recover(&io, db_path_sim)?;
    let rows = query_rows(&recovered, "SELECT x FROM t ORDER BY x")?;
    assert_eq!(
        rows,
        vec!["1".to_string(), "2".to_string()],
        "a synchronous=FULL commit was acknowledged but did not survive a \
         power loss immediately after COMMIT returned"
    );
    Ok(())
}

/// The same gate for the very first commit of a brand-new database: nothing
/// has raised the WAL dirty flag yet, so before the fix the commit returned
/// with all of its frames still unsynced.
///
/// This asserts WAL durability only; end-to-end crash recovery before the
/// first checkpoint (page 1 durable in the main file, committed data replayed
/// from the WAL) is covered by `wal::test_power_loss_before_first_checkpoint`.
/// This test pins down the commit fsync itself.
#[test]
fn first_full_commit_on_fresh_database_fsyncs_the_wal() -> anyhow::Result<()> {
    let db_path_sim = "first-full-commit-on-fresh-database.db";
    let io = Arc::new(UnreliableIo::new());
    let db = Database::open_file_with_flags(
        io.clone(),
        db_path_sim,
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )?;
    let conn = db.connect()?;
    conn.execute("PRAGMA synchronous=FULL")?;
    conn.execute("CREATE TABLE t(x)")?;

    assert!(
        !io.has_unsynced_writes(&format!("{db_path_sim}-wal")),
        "the first synchronous=FULL commit of a fresh database returned \
         with unsynced WAL frames"
    );
    Ok(())
}
