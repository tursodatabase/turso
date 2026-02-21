use crate::common::{ExecRows, TempDatabase};
use std::sync::Arc;
use turso_core::{Database, DatabaseOpts, OpenFlags, StepResult};

/// Regression test: op_parse_schema previously held the database_schemas write
/// lock while executing a nested statement to read sqlite_schema. If that nested
/// statement triggered reprepare() (which also acquires database_schemas.write()),
/// it deadlocked because parking_lot RwLock is not re-entrant.
///
/// Reproducer: CREATE TABLE on an attached database with MVCC enabled on main.
#[turso_macros::test]
fn test_mvcc_create_table_on_attached_db(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.pragma_update("journal_mode", "'experimental_mvcc'")?;

    // Pre-create the attached database with MVCC (modes must match).
    let aux_path = tmp_db.path.with_extension("aux.db");
    let aux_db = Database::open_file_with_flags(
        tmp_db.io.clone(),
        aux_path.to_str().unwrap(),
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
    )?;
    let aux_conn = aux_db.connect()?;
    aux_conn.pragma_update("journal_mode", "'experimental_mvcc'")?;
    aux_conn.close()?;

    conn.execute(format!("ATTACH '{}' AS aux", aux_path.display()))?;

    // This previously caused a deadlock in op_parse_schema
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

// Regression test: statement-level rollback (from FK constraint violation)
// must clean up tx.write_set so that commit validation doesn't find stale
// entries pointing to pre-existing committed versions and panic with
// "there is another row insterted and not updated/deleted from before".
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

    // DELETE from parent fails due to FK constraint. This triggers
    // statement-level rollback which undoes the MVCC version changes,
    // but before the fix, rollback_first_savepoint didn't clean up
    // tx.write_set — leaving stale entries.
    let result = conn2.execute("DELETE FROM parent WHERE id = 1");
    assert!(result.is_err(), "DELETE should fail due to FK constraint");

    // COMMIT should succeed. Before the fix this panicked at commit
    // validation because the stale write_set entry pointed to a
    // pre-existing committed version.
    conn2.execute("COMMIT")?;

    Ok(())
}

// Same as test_stmt_rollback_cleans_write_set but with an index on the
// child table, exercising the index version rollback path as well.
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

/// Regression test: upgrading an existing MVCC transaction from read->write must not
/// leak an extra blocking-checkpoint read lock.
///
/// Before the fix, this sequence left `blocking_checkpoint_lock` with a stale reader:
/// BEGIN (deferred) -> read statement (starts tx) -> write statement (upgrade) -> COMMIT.
/// A following checkpoint then failed with `database is locked`.
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

/// ATTACH must reject databases whose journal mode differs from the main database.
/// Main=MVCC + Attached=WAL (or vice versa) causes WAL write-lock contention that
/// MVCC is designed to eliminate.
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
    assert!(result.is_err(), "ATTACH should fail with incompatible journal modes");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("journal mode"),
        "Error should mention journal mode incompatibility, got: {err}"
    );

    Ok(())
}

/// The reverse case: main DB is WAL, attached DB is MVCC. Should also be rejected.
#[turso_macros::test]
fn test_attach_rejects_mvcc_attached_on_wal_main(tmp_db: TempDatabase) -> anyhow::Result<()> {
    // Main DB stays in default WAL mode
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE main_t(x INTEGER)")?;

    // Create attached DB with MVCC
    let aux_path = tmp_db.path.with_extension("aux_mvcc.db");
    let aux_db = Database::open_file_with_flags(
        tmp_db.io.clone(),
        aux_path.to_str().unwrap(),
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
    )?;
    let aux_conn = aux_db.connect()?;
    aux_conn.pragma_update("journal_mode", "'experimental_mvcc'")?;
    aux_conn.execute("CREATE TABLE t(x INTEGER)")?;
    aux_conn.close()?;

    // ATTACH should fail because main=WAL but attached=MVCC
    let result = conn.execute(format!("ATTACH '{}' AS aux", aux_path.display()));
    assert!(result.is_err(), "ATTACH should fail with incompatible journal modes");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("journal mode"),
        "Error should mention journal mode incompatibility, got: {err}"
    );

    Ok(())
}

/// Attaching a :memory: database should always succeed regardless of journal mode
/// since :memory: databases cannot use MVCC (no file for .db-log).
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
