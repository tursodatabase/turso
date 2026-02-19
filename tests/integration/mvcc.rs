use crate::common::TempDatabase;
use std::sync::Arc;
use turso_core::StepResult;

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

/// Sequential test for issue #5424: single-connection rollback + read
#[turso_macros::test]
fn test_rollback_data_leak_sequential(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();

    let conn = tmp_db.connect_limbo();
    conn.pragma_update("journal_mode", "'experimental_mvcc'")?;
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER, marker TEXT)")?;
    conn.execute("INSERT INTO t VALUES(1, 100, 'original')")?;
    conn.execute("INSERT INTO t VALUES(2, 200, 'original')")?;

    // Update + rollback
    conn.execute("BEGIN CONCURRENT")?;
    conn.execute("UPDATE t SET marker = 'ROLLBACK_GHOST' WHERE id = 1")?;
    conn.execute("UPDATE t SET val = -999 WHERE id = 2")?;
    conn.execute("ROLLBACK")?;

    // Read on the same connection
    conn.execute("BEGIN CONCURRENT")?;
    let mut val: i64 = 0;
    let mut stmt = conn.prepare("SELECT val FROM t WHERE id = 2")?;
    stmt.run_with_row_callback(|row| {
        val = row.get::<i64>(0)?;
        Ok(())
    })?;
    conn.execute("COMMIT")?;
    assert_eq!(
        val, 200,
        "Same-connection reader saw rolled-back val={val} instead of 200"
    );

    // Read on a different connection
    let conn2 = tmp_db.connect_limbo();
    conn2.execute("BEGIN CONCURRENT")?;
    let mut val2: i64 = 0;
    let mut stmt2 = conn2.prepare("SELECT val FROM t WHERE id = 2")?;
    stmt2.run_with_row_callback(|row| {
        val2 = row.get::<i64>(0)?;
        Ok(())
    })?;
    conn2.execute("COMMIT")?;
    assert_eq!(
        val2, 200,
        "Different-connection reader saw rolled-back val={val2} instead of 200"
    );

    Ok(())
}

/// Non-concurrent multi-connection test for issue #5424
#[turso_macros::test]
fn test_rollback_data_leak_multi_conn(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();

    let conn = tmp_db.connect_limbo();
    conn.pragma_update("journal_mode", "'experimental_mvcc'")?;
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER, marker TEXT)")?;
    conn.execute("INSERT INTO t VALUES(1, 100, 'original')")?;
    conn.execute("INSERT INTO t VALUES(2, 200, 'original')")?;

    // Use a separate connection for writing
    let writer = tmp_db.connect_limbo();
    let reader = tmp_db.connect_limbo();

    for i in 0..50 {
        // Writer: update + rollback
        writer.execute("BEGIN CONCURRENT")?;
        writer.execute("UPDATE t SET val = -999 WHERE id = 2")?;
        writer.execute("ROLLBACK")?;

        // Reader: check
        reader.execute("BEGIN CONCURRENT")?;
        let mut val: i64 = 0;
        let mut stmt = reader.prepare("SELECT val FROM t WHERE id = 2")?;
        stmt.run_with_row_callback(|row| {
            val = row.get::<i64>(0)?;
            Ok(())
        })?;
        reader.execute("COMMIT")?;

        assert_eq!(
            val, 200,
            "Iteration {i}: reader saw rolled-back val={val} instead of 200"
        );
    }
    Ok(())
}

/// Regression test for issue #5424: rolled-back UPDATE data visible to concurrent readers.
/// A committed row is updated and then rolled back; concurrent readers must never see the
/// rolled-back value.
#[turso_macros::test]
fn test_rollback_data_leak(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let tmp_db = Arc::new(tmp_db);

    // Setup: enable MVCC, create table, insert initial rows
    {
        let conn = tmp_db.connect_limbo();
        conn.pragma_update("journal_mode", "'experimental_mvcc'")?;
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER, marker TEXT)")?;
        conn.execute("INSERT INTO t VALUES(1, 100, 'original')")?;
        conn.execute("INSERT INTO t VALUES(2, 200, 'original')")?;
    }

    let done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let leak_found = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut handles = vec![];

    // Writer threads: UPDATE then ROLLBACK in a loop
    for _ in 0..4 {
        let tmp_db = tmp_db.clone();
        let done = done.clone();
        handles.push(std::thread::spawn(move || {
            let conn = tmp_db.connect_limbo();
            while !done.load(std::sync::atomic::Ordering::Relaxed) {
                let _ = conn.execute("BEGIN CONCURRENT");
                let _ = conn.execute("UPDATE t SET marker = 'ROLLBACK_GHOST' WHERE id = 1");
                let _ = conn.execute("UPDATE t SET val = -999 WHERE id = 2");
                let _ = conn.execute("ROLLBACK");
            }
        }));
    }

    // Reader threads: check that rolled-back data is never visible
    for _ in 0..4 {
        let tmp_db = tmp_db.clone();
        let done = done.clone();
        let leak_found = leak_found.clone();
        handles.push(std::thread::spawn(move || {
            let conn = tmp_db.connect_limbo();
            while !done.load(std::sync::atomic::Ordering::Relaxed) {
                let _ = conn.execute("BEGIN CONCURRENT");

                // Check marker on row 1
                let mut stmt = conn.prepare("SELECT marker FROM t WHERE id = 1").unwrap();
                let mut marker = String::new();
                let _ = stmt.run_with_row_callback(|row| {
                    marker = row.get::<String>(0)?;
                    Ok(())
                });

                // Check val on row 2
                let mut stmt2 = conn.prepare("SELECT val FROM t WHERE id = 2").unwrap();
                let mut val: i64 = 0;
                let _ = stmt2.run_with_row_callback(|row| {
                    val = row.get::<i64>(0)?;
                    Ok(())
                });

                let _ = conn.execute("COMMIT");

                if marker == "ROLLBACK_GHOST" {
                    leak_found.store(true, std::sync::atomic::Ordering::Relaxed);
                    eprintln!("LEAK: Rolled-back UPDATE visible! marker='ROLLBACK_GHOST' on row 1");
                }
                if val == -999 {
                    leak_found.store(true, std::sync::atomic::Ordering::Relaxed);
                    eprintln!("LEAK: Rolled-back UPDATE visible! val=-999 on row 2");
                }
            }
        }));
    }

    std::thread::sleep(std::time::Duration::from_secs(3));
    done.store(true, std::sync::atomic::Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }

    assert!(
        !leak_found.load(std::sync::atomic::Ordering::Relaxed),
        "Rolled-back transaction data leaked to concurrent readers!"
    );
    Ok(())
}
