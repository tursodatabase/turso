use crate::common::{ExecRows, TempDatabase};
use std::path::Path;
use std::sync::Arc;
use turso_core::{Database, DatabaseOpts, OpenFlags, StepResult};

/// Create a new database file at `path` with MVCC journal mode enabled.
/// This is needed because ATTACH requires the attached DB's journal mode
/// to match the main DB's journal mode.
fn create_mvcc_db(io: &Arc<dyn turso_core::io::IO + Send>, path: &Path) -> anyhow::Result<()> {
    let db = Database::open_file_with_flags(
        io.clone(),
        path.to_str().unwrap(),
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
    )?;
    let conn = db.connect()?;
    conn.pragma_update("journal_mode", "'experimental_mvcc'")?;
    conn.close()?;
    Ok(())
}

/// CREATE TABLE on an attached MVCC database must not deadlock.
/// op_parse_schema must release the database_schemas lock before executing the
/// nested statement that reads sqlite_schema, since reprepare() also acquires
/// that lock and parking_lot RwLock is not re-entrant.
#[turso_macros::test]
fn test_mvcc_create_table_on_attached_db(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.pragma_update("journal_mode", "'experimental_mvcc'")?;

    let aux_path = tmp_db.path.with_extension("aux.db");
    create_mvcc_db(&tmp_db.io, &aux_path)?;

    conn.execute(format!("ATTACH '{}' AS aux", aux_path.display()))?;
    conn.execute("CREATE TABLE aux.test_table (id INTEGER PRIMARY KEY, name TEXT)")?;

    // Verify the table works
    conn.execute("INSERT INTO aux.test_table VALUES (1, 'hello')")?;
    let rows: Vec<(i64, String)> = conn.exec_rows("SELECT id, name FROM aux.test_table");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], (1, "hello".to_string()));

    Ok(())
}

#[turso_macros::test(mvcc)]
fn test_newrowid_mvcc_concurrent(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();

    let tmp_db = Arc::new(tmp_db);

    {
        let conn = tmp_db.connect_limbo();
        conn.execute("CREATE TABLE t_concurrent(id INTEGER PRIMARY KEY, value TEXT)")?;
    }

    let mut threads = Vec::new();
    let num_threads = 10;
    let inserts_per_thread = 10;

    for thread_id in 0..num_threads {
        let tmp_db_clone = tmp_db.clone();
        threads.push(std::thread::spawn(move || -> anyhow::Result<()> {
            let conn = tmp_db_clone.connect_limbo();

            for i in 0..inserts_per_thread {
                let mut stmt = conn.prepare(format!(
                    "INSERT INTO t_concurrent VALUES(NULL, 'thread_{thread_id}_value_{i}')",
                ))?;
                // Retry loop for handling busy conditions
                'retry: loop {
                    loop {
                        match stmt.step()? {
                            StepResult::IO => {
                                stmt._io().step()?;
                            }
                            StepResult::Done => {
                                break 'retry;
                            }
                            StepResult::Busy => {
                                // Statement is busy, re-prepare and retry
                                break;
                            }
                            StepResult::Row => {
                                anyhow::bail!("Unexpected row from INSERT");
                            }
                            StepResult::Interrupt => {
                                anyhow::bail!("Unexpected interrupt");
                            }
                        }
                    }
                }
            }
            Ok(())
        }));
    }

    for thread in threads {
        thread.join().unwrap()?;
    }

    // Verify we got the right number of rows
    let conn = tmp_db.connect_limbo();

    // Debug: check what we actually got
    let mut stmt_all = conn.prepare("SELECT id, value FROM t_concurrent ORDER BY id")?;
    let mut all_rows = Vec::new();
    stmt_all.run_with_row_callback(|row| {
        let id = row.get::<i64>(0)?;
        let value = row.get::<String>(1)?;
        all_rows.push((id, value));
        Ok(())
    })?;

    eprintln!("Total rows: {}", all_rows.len());
    eprintln!("Expected: {}", num_threads * inserts_per_thread);

    // Check for duplicates by value
    let mut value_counts = std::collections::HashMap::new();
    for (_id, value) in &all_rows {
        *value_counts.entry(value.clone()).or_insert(0) += 1;
    }

    for (value, count) in value_counts.iter() {
        if *count > 1 {
            eprintln!("Duplicate value '{value}' appears {count} times",);
        }
    }

    let mut stmt = conn.prepare("SELECT COUNT(*) FROM t_concurrent")?;
    let mut count = 0;
    stmt.run_with_row_callback(|row| {
        count = row.get::<i64>(0)?;
        Ok(())
    })?;

    // Assertion disabled - concurrent inserts without transactions cause duplicates
    assert_eq!(count, (num_threads * inserts_per_thread) as i64);
    eprintln!("Test disabled - would need BEGIN CONCURRENT for proper concurrent testing");
    Ok(())
}

/// Statement-level rollback from an FK constraint violation must clean up
/// tx.write_set so that commit validation does not find stale entries pointing
/// to pre-existing committed versions.
#[turso_macros::test]
fn test_stmt_rollback_cleans_write_set(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();

    // Enable MVCC — this test is MVCC-specific (uses BEGIN CONCURRENT)
    let conn = tmp_db.connect_limbo();
    conn.pragma_update("journal_mode", "'experimental_mvcc'")?;

    // Setup: parent/child tables with FK constraint
    conn.execute("PRAGMA foreign_keys = ON")?;
    conn.execute("CREATE TABLE parent(id INTEGER PRIMARY KEY)")?;
    conn.execute(
        "CREATE TABLE child(id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id))",
    )?;
    conn.execute("INSERT INTO parent VALUES (1)")?;
    conn.execute("INSERT INTO child VALUES (1, 1)")?;

    // Open a concurrent transaction on a second connection
    let conn2 = tmp_db.connect_limbo();
    conn2.execute("PRAGMA foreign_keys = ON")?;
    conn2.execute("BEGIN CONCURRENT")?;

    // DELETE from parent fails due to FK constraint, triggering
    // statement-level rollback of the MVCC version changes.
    let result = conn2.execute("DELETE FROM parent WHERE id = 1");
    assert!(result.is_err(), "DELETE should fail due to FK constraint");

    // COMMIT must succeed — the write_set should be clean after the
    // statement rollback.
    conn2.execute("COMMIT")?;

    Ok(())
}

/// Same as test_stmt_rollback_cleans_write_set but with an index on the
/// child table, exercising the index version rollback path as well.
#[turso_macros::test]
fn test_stmt_rollback_cleans_write_set_with_index(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();

    let conn = tmp_db.connect_limbo();
    conn.pragma_update("journal_mode", "'experimental_mvcc'")?;

    conn.execute("PRAGMA foreign_keys = ON")?;
    conn.execute("CREATE TABLE parent(id INTEGER PRIMARY KEY)")?;
    conn.execute(
        "CREATE TABLE child(id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id))",
    )?;
    conn.execute("CREATE INDEX idx_child_parent ON child(parent_id)")?;
    conn.execute("INSERT INTO parent VALUES (1)")?;
    conn.execute("INSERT INTO child VALUES (1, 1)")?;

    let conn2 = tmp_db.connect_limbo();
    conn2.execute("PRAGMA foreign_keys = ON")?;
    conn2.execute("BEGIN CONCURRENT")?;

    // DELETE from parent fails due to FK constraint. With an index on
    // child(parent_id), the rollback must also undo index version changes.
    let result = conn2.execute("DELETE FROM parent WHERE id = 1");
    assert!(result.is_err(), "DELETE should fail due to FK constraint");

    conn2.execute("COMMIT")?;
    Ok(())
}

/// Upgrading an existing MVCC transaction from read to write must not leak an
/// extra blocking-checkpoint read lock.  The sequence BEGIN -> SELECT -> INSERT ->
/// COMMIT must leave checkpoint unblocked.
#[turso_macros::test(mvcc)]
fn test_mvcc_read_to_write_upgrade_does_not_block_checkpoint(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t(x INTEGER)")?;

    conn.execute("BEGIN")?;
    let rows: Vec<(i64,)> = conn.exec_rows("SELECT COUNT(*) FROM t");
    assert_eq!(rows, vec![(0,)]);
    conn.execute("INSERT INTO t VALUES (1)")?;
    conn.execute("COMMIT")?;

    let conn2 = tmp_db.connect_limbo();
    conn2.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;

    Ok(())
}

/// DETACH must clean up any active MVCC transaction on the attached database.
/// An uncommitted insert followed by ROLLBACK + DETACH + re-ATTACH must not
/// show the uncommitted data.
#[turso_macros::test]
fn test_detach_rollbacks_active_mvcc_tx(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.pragma_update("journal_mode", "'experimental_mvcc'")?;

    let aux_path = tmp_db.path.with_extension("aux_detach.db");
    create_mvcc_db(&tmp_db.io, &aux_path)?;

    conn.execute(format!("ATTACH '{}' AS aux", aux_path.display()))?;
    conn.execute("CREATE TABLE aux.t(x INTEGER)")?;

    // Insert a committed row first so we can verify it survives
    conn.execute("INSERT INTO aux.t VALUES (1)")?;

    // Begin explicit transaction and insert into attached DB, then DETACH
    // without committing. DETACH should rollback the uncommitted insert and
    // clean up the MVCC transaction in the attached MvStore.
    conn.execute("BEGIN")?;
    conn.execute("INSERT INTO aux.t VALUES (99)")?;
    // Need to rollback first (SQLite also requires this before DETACH)
    conn.execute("ROLLBACK")?;
    conn.execute("DETACH aux")?;

    // Re-attach and verify only the committed row persists
    conn.execute(format!("ATTACH '{}' AS aux", aux_path.display()))?;
    let rows: Vec<(i64,)> = conn.exec_rows("SELECT x FROM aux.t");
    assert_eq!(
        rows,
        vec![(1,)],
        "Only the committed row should survive; uncommitted insert from rolled-back tx should be gone"
    );

    Ok(())
}

/// ATTACH must reject databases whose journal mode differs from the main database.
/// Here the main DB uses MVCC while the attached DB uses WAL.
#[turso_macros::test]
fn test_attach_rejects_incompatible_journal_mode(tmp_db: TempDatabase) -> anyhow::Result<()> {
    // Main DB uses MVCC
    let conn = tmp_db.connect_limbo();
    conn.pragma_update("journal_mode", "'experimental_mvcc'")?;

    // Attached DB is created in default WAL mode (no MVCC)
    let aux_path = tmp_db.path.with_extension("aux_wal.db");
    let aux_db = Database::open_file_with_flags(
        tmp_db.io.clone(),
        aux_path.to_str().unwrap(),
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
    )?;
    let aux_conn = aux_db.connect()?;
    aux_conn.execute("CREATE TABLE t(x INTEGER)")?;
    aux_conn.close()?;

    // ATTACH should fail because main=MVCC but attached=WAL
    let result = conn.execute(format!("ATTACH '{}' AS aux", aux_path.display()));
    assert!(
        result.is_err(),
        "ATTACH should fail with incompatible journal modes"
    );
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("journal mode"),
        "Error should mention journal mode incompatibility, got: {err}"
    );

    Ok(())
}

/// ATTACH must reject databases whose journal mode differs from the main database.
/// Here the main DB uses WAL while the attached DB uses MVCC.
#[turso_macros::test]
fn test_attach_rejects_mvcc_attached_on_wal_main(tmp_db: TempDatabase) -> anyhow::Result<()> {
    // Main DB stays in default WAL mode
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE main_t(x INTEGER)")?;

    let aux_path = tmp_db.path.with_extension("aux_mvcc.db");
    create_mvcc_db(&tmp_db.io, &aux_path)?;

    // ATTACH should fail because main=WAL but attached=MVCC
    let result = conn.execute(format!("ATTACH '{}' AS aux", aux_path.display()));
    assert!(
        result.is_err(),
        "ATTACH should fail with incompatible journal modes"
    );
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("journal mode"),
        "Error should mention journal mode incompatibility, got: {err}"
    );

    Ok(())
}

/// ROLLBACK must revert changes on attached MVCC databases, not just the main DB.
#[turso_macros::test]
fn test_mvcc_rollback_reverts_attached_db(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.pragma_update("journal_mode", "'experimental_mvcc'")?;

    let aux_path = tmp_db.path.with_extension("aux_rb.db");
    create_mvcc_db(&tmp_db.io, &aux_path)?;

    conn.execute(format!("ATTACH '{}' AS aux", aux_path.display()))?;
    conn.execute("CREATE TABLE aux.t(x INTEGER)")?;

    // Begin explicit transaction, insert, then rollback
    conn.execute("BEGIN")?;
    conn.execute("INSERT INTO aux.t VALUES (1)")?;
    conn.execute("ROLLBACK")?;

    // The insert should have been rolled back — table should be empty
    let rows: Vec<(i64,)> = conn.exec_rows("SELECT x FROM aux.t");
    assert!(
        rows.is_empty(),
        "ROLLBACK should have reverted the INSERT on the attached DB, but found {rows:?}"
    );

    Ok(())
}

/// Dropping a connection with an active MVCC transaction must clean up both main
/// and attached DB transactions, so that subsequent connections can checkpoint and
/// commit without being blocked by stale MVCC state.
#[turso_macros::test]
fn test_drop_cleans_up_mvcc_transactions(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.pragma_update("journal_mode", "'experimental_mvcc'")?;

    let aux_path = tmp_db.path.with_extension("aux_drop.db");
    create_mvcc_db(&tmp_db.io, &aux_path)?;

    conn.execute(format!("ATTACH '{}' AS aux", aux_path.display()))?;
    conn.execute("CREATE TABLE aux.t(x INTEGER)")?;

    // Start an explicit transaction and write to the attached DB, then
    // drop the connection without committing or rolling back.
    conn.execute("BEGIN")?;
    conn.execute("INSERT INTO aux.t VALUES (1)")?;
    drop(conn);

    // A new connection must be able to use the database normally — the
    // dropped connection's MVCC state should have been cleaned up.
    let conn2 = tmp_db.connect_limbo();
    conn2.pragma_update("journal_mode", "'experimental_mvcc'")?;
    conn2.execute(format!("ATTACH '{}' AS aux", aux_path.display()))?;
    conn2.execute("INSERT INTO aux.t VALUES (2)")?;
    conn2.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;

    let rows: Vec<(i64,)> = conn2.exec_rows("SELECT x FROM aux.t");
    assert_eq!(rows, vec![(2,)], "Only post-drop insert should be visible");

    Ok(())
}

/// A single transaction that writes to both the main DB and an attached MVCC
/// database must commit changes to both.  Note: WAL mode does not provide
/// cross-file transactionality — each DB commits independently.
#[turso_macros::test]
fn test_cross_database_transaction(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.pragma_update("journal_mode", "'experimental_mvcc'")?;

    let aux_path = tmp_db.path.with_extension("aux_cross.db");
    create_mvcc_db(&tmp_db.io, &aux_path)?;

    conn.execute(format!("ATTACH '{}' AS aux", aux_path.display()))?;
    conn.execute("CREATE TABLE main_t(x INTEGER)")?;
    conn.execute("CREATE TABLE aux.aux_t(y INTEGER)")?;

    // Write to both databases in a single transaction
    conn.execute("BEGIN")?;
    conn.execute("INSERT INTO main_t VALUES (1)")?;
    conn.execute("INSERT INTO aux.aux_t VALUES (2)")?;
    conn.execute("COMMIT")?;

    // Both tables should have the committed data
    let main_rows: Vec<(i64,)> = conn.exec_rows("SELECT x FROM main_t");
    assert_eq!(main_rows, vec![(1,)]);
    let aux_rows: Vec<(i64,)> = conn.exec_rows("SELECT y FROM aux.aux_t");
    assert_eq!(aux_rows, vec![(2,)]);

    // A second connection should also see the committed data
    let conn2 = tmp_db.connect_limbo();
    conn2.execute(format!("ATTACH '{}' AS aux", aux_path.display()))?;
    let main_rows: Vec<(i64,)> = conn2.exec_rows("SELECT x FROM main_t");
    assert_eq!(main_rows, vec![(1,)]);
    let aux_rows: Vec<(i64,)> = conn2.exec_rows("SELECT y FROM aux.aux_t");
    assert_eq!(aux_rows, vec![(2,)]);

    Ok(())
}

/// Within an explicit transaction, a SELECT on an attached MVCC database followed
/// by an INSERT must upgrade the attached DB's MVCC transaction from read to
/// write mode so that the commit succeeds.
#[turso_macros::test]
fn test_attached_mvcc_read_to_write_upgrade(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.pragma_update("journal_mode", "'experimental_mvcc'")?;

    let aux_path = tmp_db.path.with_extension("aux_upgrade.db");
    create_mvcc_db(&tmp_db.io, &aux_path)?;

    conn.execute(format!("ATTACH '{}' AS aux", aux_path.display()))?;
    conn.execute("CREATE TABLE aux.t(x INTEGER)")?;

    // Begin an explicit transaction, read first (starts Read tx on aux),
    // then write (must upgrade to Write tx on aux).
    conn.execute("BEGIN")?;
    let rows: Vec<(i64,)> = conn.exec_rows("SELECT COUNT(*) FROM aux.t");
    assert_eq!(rows, vec![(0,)]);
    conn.execute("INSERT INTO aux.t VALUES (1)")?;
    conn.execute("COMMIT")?;

    // Verify the insert was committed
    let rows: Vec<(i64,)> = conn.exec_rows("SELECT x FROM aux.t");
    assert_eq!(rows, vec![(1,)]);

    Ok(())
}

/// Statement-level rollback on an attached MVCC database must clean up the
/// MvStore write_set via the savepoint mechanism.  An FK constraint violation
/// triggers a statement rollback; the subsequent COMMIT must succeed because
/// the stale write_set entries were undone by the savepoint rollback.
#[turso_macros::test]
fn test_stmt_rollback_on_attached_mvcc_db(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();

    let conn = tmp_db.connect_limbo();
    conn.pragma_update("journal_mode", "'experimental_mvcc'")?;

    let aux_path = tmp_db.path.with_extension("aux_savepoint.db");
    create_mvcc_db(&tmp_db.io, &aux_path)?;

    conn.execute(format!("ATTACH '{}' AS aux", aux_path.display()))?;
    conn.execute("PRAGMA foreign_keys = ON")?;

    // Setup parent/child tables with FK constraint on the attached DB
    conn.execute("CREATE TABLE aux.parent(id INTEGER PRIMARY KEY)")?;
    conn.execute(
        "CREATE TABLE aux.child(id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id))",
    )?;
    conn.execute("INSERT INTO aux.parent VALUES (1)")?;
    conn.execute("INSERT INTO aux.child VALUES (1, 1)")?;

    // Open a concurrent transaction on a second connection
    let conn2 = tmp_db.connect_limbo();
    conn2.execute("PRAGMA foreign_keys = ON")?;
    conn2.execute(format!("ATTACH '{}' AS aux", aux_path.display()))?;
    conn2.execute("BEGIN CONCURRENT")?;

    // DELETE from parent fails due to FK constraint — triggers statement-level
    // rollback of the MVCC version changes on the attached DB's MvStore.
    let result = conn2.execute("DELETE FROM aux.parent WHERE id = 1");
    assert!(result.is_err(), "DELETE should fail due to FK constraint");

    // COMMIT must succeed — the write_set should be clean after the
    // statement savepoint rollback on the attached MvStore.
    conn2.execute("COMMIT")?;

    // Verify original data is intact
    let rows: Vec<(i64,)> = conn.exec_rows("SELECT id FROM aux.parent");
    assert_eq!(rows, vec![(1,)]);

    Ok(())
}

/// Same as test_stmt_rollback_on_attached_mvcc_db but with an index on the
/// child table, exercising the index version rollback path on the attached DB.
#[turso_macros::test]
fn test_stmt_rollback_on_attached_mvcc_db_with_index(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();

    let conn = tmp_db.connect_limbo();
    conn.pragma_update("journal_mode", "'experimental_mvcc'")?;

    let aux_path = tmp_db.path.with_extension("aux_savepoint_idx.db");
    create_mvcc_db(&tmp_db.io, &aux_path)?;

    conn.execute(format!("ATTACH '{}' AS aux", aux_path.display()))?;
    conn.execute("PRAGMA foreign_keys = ON")?;

    conn.execute("CREATE TABLE aux.parent(id INTEGER PRIMARY KEY)")?;
    conn.execute(
        "CREATE TABLE aux.child(id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id))",
    )?;
    conn.execute("CREATE INDEX aux.idx_child_parent ON child(parent_id)")?;
    conn.execute("INSERT INTO aux.parent VALUES (1)")?;
    conn.execute("INSERT INTO aux.child VALUES (1, 1)")?;

    let conn2 = tmp_db.connect_limbo();
    conn2.execute("PRAGMA foreign_keys = ON")?;
    conn2.execute(format!("ATTACH '{}' AS aux", aux_path.display()))?;
    conn2.execute("BEGIN CONCURRENT")?;

    // DELETE from parent fails due to FK constraint. With the index on
    // child(parent_id), the rollback must also undo index version changes
    // on the attached MvStore.
    let result = conn2.execute("DELETE FROM aux.parent WHERE id = 1");
    assert!(result.is_err(), "DELETE should fail due to FK constraint");

    conn2.execute("COMMIT")?;

    let rows: Vec<(i64,)> = conn.exec_rows("SELECT id FROM aux.parent");
    assert_eq!(rows, vec![(1,)]);

    Ok(())
}

/// A deferred FK constraint violation detected at autocommit time must roll back
/// changes on attached MVCC databases, not just the main DB.  The attached DB
/// should be unchanged after the failed autocommit statement.
#[turso_macros::test]
fn test_deferred_fk_violation_rolls_back_attached_mvcc(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();

    let conn = tmp_db.connect_limbo();
    conn.pragma_update("journal_mode", "'experimental_mvcc'")?;

    let aux_path = tmp_db.path.with_extension("aux_deferred_fk.db");
    create_mvcc_db(&tmp_db.io, &aux_path)?;

    conn.execute(format!("ATTACH '{}' AS aux", aux_path.display()))?;
    conn.execute("PRAGMA foreign_keys = ON")?;

    // Both tables on the attached DB with a DEFERRED FK constraint.
    conn.execute("CREATE TABLE aux.parent(id INTEGER PRIMARY KEY)")?;
    conn.execute(
        "CREATE TABLE aux.child(id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)",
    )?;
    conn.execute("INSERT INTO aux.parent VALUES (1)")?;

    // In autocommit mode, insert a child row referencing a non-existent parent.
    // The deferred FK check fires at commit (halt) and must roll back the
    // insert on the attached DB.
    let result = conn.execute("INSERT INTO aux.child VALUES (1, 999)");
    assert!(
        result.is_err(),
        "INSERT with invalid deferred FK should fail at autocommit"
    );

    // The attached DB must be empty — the deferred FK rollback should have
    // reverted the insert.
    let rows: Vec<(i64,)> = conn.exec_rows("SELECT id FROM aux.child");
    assert!(
        rows.is_empty(),
        "Deferred FK rollback should have reverted the INSERT on the attached DB, but found {rows:?}"
    );

    // The connection should still be usable for subsequent operations.
    conn.execute("INSERT INTO aux.child VALUES (1, 1)")?;
    let rows: Vec<(i64,)> = conn.exec_rows("SELECT id FROM aux.child");
    assert_eq!(rows, vec![(1,)]);

    Ok(())
}

/// ROLLBACK TO a named savepoint must revert changes on attached MVCC databases,
/// not just the main DB.
#[turso_macros::test]
fn test_named_savepoint_rollback_reverts_attached_mvcc(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();

    let conn = tmp_db.connect_limbo();
    conn.pragma_update("journal_mode", "'experimental_mvcc'")?;

    let aux_path = tmp_db.path.with_extension("aux_named_sp.db");
    create_mvcc_db(&tmp_db.io, &aux_path)?;

    conn.execute(format!("ATTACH '{}' AS aux", aux_path.display()))?;
    conn.execute("CREATE TABLE main_t(x INTEGER)")?;
    conn.execute("CREATE TABLE aux.aux_t(y INTEGER)")?;

    // Insert a baseline row before the savepoint
    conn.execute("BEGIN")?;
    conn.execute("INSERT INTO main_t VALUES (1)")?;
    conn.execute("INSERT INTO aux.aux_t VALUES (1)")?;

    // Open a named savepoint, insert more data, then rollback to it
    conn.execute("SAVEPOINT sp1")?;
    conn.execute("INSERT INTO main_t VALUES (2)")?;
    conn.execute("INSERT INTO aux.aux_t VALUES (2)")?;

    conn.execute("ROLLBACK TO sp1")?;

    // The rows inserted after the savepoint should be gone on both DBs
    conn.execute("RELEASE sp1")?;
    conn.execute("COMMIT")?;

    let main_rows: Vec<(i64,)> = conn.exec_rows("SELECT x FROM main_t ORDER BY x");
    assert_eq!(
        main_rows,
        vec![(1,)],
        "Main DB should only have pre-savepoint row"
    );

    let aux_rows: Vec<(i64,)> = conn.exec_rows("SELECT y FROM aux.aux_t ORDER BY y");
    assert_eq!(
        aux_rows,
        vec![(1,)],
        "Attached DB should only have pre-savepoint row"
    );

    Ok(())
}

/// RELEASE of a named savepoint that started a transaction must commit changes
/// on both main and attached MVCC databases.
#[turso_macros::test]
fn test_named_savepoint_release_commits_attached_mvcc(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();

    let conn = tmp_db.connect_limbo();
    conn.pragma_update("journal_mode", "'experimental_mvcc'")?;

    let aux_path = tmp_db.path.with_extension("aux_named_sp_rel.db");
    create_mvcc_db(&tmp_db.io, &aux_path)?;

    conn.execute(format!("ATTACH '{}' AS aux", aux_path.display()))?;
    conn.execute("CREATE TABLE main_t(x INTEGER)")?;
    conn.execute("CREATE TABLE aux.aux_t(y INTEGER)")?;

    // SAVEPOINT in autocommit mode starts a transaction; RELEASE commits it.
    conn.execute("SAVEPOINT sp1")?;
    conn.execute("INSERT INTO main_t VALUES (1)")?;
    conn.execute("INSERT INTO aux.aux_t VALUES (2)")?;
    conn.execute("RELEASE sp1")?;

    // Both databases should have the committed data
    let main_rows: Vec<(i64,)> = conn.exec_rows("SELECT x FROM main_t");
    assert_eq!(main_rows, vec![(1,)]);

    let aux_rows: Vec<(i64,)> = conn.exec_rows("SELECT y FROM aux.aux_t");
    assert_eq!(aux_rows, vec![(2,)]);

    // A second connection should also see the committed data
    let conn2 = tmp_db.connect_limbo();
    conn2.execute(format!("ATTACH '{}' AS aux", aux_path.display()))?;
    let main_rows: Vec<(i64,)> = conn2.exec_rows("SELECT x FROM main_t");
    assert_eq!(main_rows, vec![(1,)]);
    let aux_rows: Vec<(i64,)> = conn2.exec_rows("SELECT y FROM aux.aux_t");
    assert_eq!(aux_rows, vec![(2,)]);

    Ok(())
}

/// Attaching a :memory: database must succeed even when the main DB uses MVCC,
/// since in-memory databases do not have a journal mode to conflict with.
#[turso_macros::test(mvcc)]
fn test_attach_memory_db_always_allowed(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    // Main is MVCC (via test attribute), :memory: should still attach fine
    conn.execute("ATTACH ':memory:' AS mem_aux")?;
    conn.execute("CREATE TABLE mem_aux.t(x INTEGER)")?;
    conn.execute("INSERT INTO mem_aux.t VALUES (42)")?;
    let rows: Vec<(i64,)> = conn.exec_rows("SELECT x FROM mem_aux.t");
    assert_eq!(rows, vec![(42,)]);
    Ok(())
}
