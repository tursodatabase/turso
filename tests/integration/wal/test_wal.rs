use crate::common::{compute_dbhash, do_flush, maybe_setup_tracing, TempDatabase};
use std::ops::Deref;
use std::sync::{Arc, Mutex};
use turso_core::{Connection, LimboError, Result};

#[allow(clippy::arc_with_non_send_sync)]
#[turso_macros::test]
fn test_wal_checkpoint_result(tmp_db: TempDatabase) -> Result<()> {
    maybe_setup_tracing();
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t1 (id text);")?;

    let res = execute_and_get_strings(&conn, "pragma journal_mode;")?;
    assert_eq!(res, vec!["wal"]);

    conn.execute("insert into t1(id) values (1), (2);")?;
    do_flush(&conn, &tmp_db).unwrap();
    conn.execute("select * from t1;")?;
    do_flush(&conn, &tmp_db).unwrap();

    // hash BEFORE checkpoint
    let hash_before = compute_dbhash(&tmp_db);

    // checkpoint result should return > 0 num pages now as database has data
    let res = execute_and_get_ints(&conn, "pragma wal_checkpoint;")?;
    println!("'pragma wal_checkpoint;' returns: {res:?}");
    assert_eq!(res.len(), 3);
    assert_eq!(res[0], 0); // checkpoint successfully
    assert!(res[1] > 0); // num pages in wal
    assert!(res[2] > 0); // num pages checkpointed successfully

    do_flush(&conn, &tmp_db).unwrap();

    // hash AFTER checkpoint - must be identical
    let hash_after = compute_dbhash(&tmp_db);
    assert_eq!(
        hash_before.hash, hash_after.hash,
        "checkpoint changed database content!!!!!!"
    );

    Ok(())
}

#[allow(clippy::arc_with_non_send_sync)]
#[turso_macros::test]
fn test_truncate_checkpoint_not_busy_after_rollback(tmp_db: TempDatabase) -> Result<()> {
    maybe_setup_tracing();
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t(x);")?;
    conn.execute("INSERT INTO t VALUES (randomblob(10 * 4096 + 0));")?;
    conn.execute("INSERT INTO t VALUES (randomblob(10 * 4096 + 1));")?;
    conn.execute("INSERT INTO t VALUES (randomblob(10 * 4096 + 2));")?;

    conn.execute("BEGIN;")?;
    conn.execute("INSERT INTO t VALUES (1);")?;
    conn.execute("ROLLBACK;")?;

    let checkpoint = execute_and_get_ints(&conn, "PRAGMA wal_checkpoint(TRUNCATE);")?;
    assert_eq!(
        checkpoint,
        vec![0, 0, 0],
        "truncate checkpoint should not return busy after rollback"
    );
    Ok(())
}

#[test]
#[ignore = "ignored for now because it's flaky"]
fn test_wal_1_writer_1_reader() -> Result<()> {
    maybe_setup_tracing();
    let tmp_db = Arc::new(Mutex::new(TempDatabase::new("test_wal.db")));
    let db = tmp_db.lock().unwrap().limbo_database();

    {
        let conn = db.connect().unwrap();
        match conn.query("CREATE TABLE t (id)")? {
            Some(ref mut rows) => {
                rows.run_with_row_callback(|_| Ok(())).unwrap();
            }
            None => todo!(),
        }
        do_flush(&conn, tmp_db.lock().unwrap().deref()).unwrap();
    }
    let rows = Arc::new(std::sync::Mutex::new(0));
    let rows_ = rows.clone();
    const ROWS_WRITE: usize = 1000;
    let tmp_db_w = db.clone();
    let writer_thread = std::thread::spawn(move || {
        let conn = tmp_db_w.connect().unwrap();
        for i in 0..ROWS_WRITE {
            conn.execute(format!("INSERT INTO t values({i})").as_str())
                .unwrap();
            let mut rows = rows_.lock().unwrap();
            *rows += 1;
        }
    });
    let rows_ = rows.clone();
    let reader_thread = std::thread::spawn(move || {
        let conn = db.connect().unwrap();
        loop {
            let rows = *rows_.lock().unwrap();
            let mut i = 0;
            match conn.query("SELECT * FROM t") {
                Ok(Some(ref mut rows)) => {
                    rows.run_with_row_callback(|row| {
                        let id = row.get::<i64>(0).unwrap();
                        assert_eq!(id, i);
                        i += 1;
                        Ok(())
                    })
                    .unwrap();
                }
                Ok(None) => {}
                Err(err) => {
                    eprintln!("{err}");
                }
            }
            if rows == ROWS_WRITE {
                break;
            }
        }
    });

    writer_thread.join().unwrap();
    reader_thread.join().unwrap();
    Ok(())
}

/// Execute a statement and get strings result
pub(crate) fn execute_and_get_strings(conn: &Arc<Connection>, sql: &str) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(sql)?;
    let mut result = Vec::new();

    stmt.run_with_row_callback(|row| {
        for el in row.get_values() {
            result.push(format!("{el}"));
        }
        Ok(())
    })?;
    Ok(result)
}

/// Execute a statement and get integers
pub(crate) fn execute_and_get_ints(conn: &Arc<Connection>, sql: &str) -> Result<Vec<i64>> {
    let mut stmt = conn.prepare(sql)?;
    let mut result = Vec::new();

    stmt.run_with_row_callback(|row| {
        for value in row.get_values() {
            let out = match value {
                turso_core::Value::Numeric(turso_core::Numeric::Integer(i)) => i,
                _ => {
                    return Err(LimboError::ConversionError(format!(
                        "cannot convert {value} to int"
                    )))
                }
            };
            result.push(*out);
        }
        Ok(())
    })?;

    Ok(result)
}

#[test]
fn test_wal_read_lock_released_on_conn_drop() {
    maybe_setup_tracing();
    let tmp_db = TempDatabase::new("test_wal_read_lock_released.db");
    let db = tmp_db.limbo_database();

    // Setup: create table and insert data so WAL has content
    let setup_conn = db.connect().unwrap();
    setup_conn
        .execute("CREATE TABLE t (id integer primary key)")
        .unwrap();
    setup_conn.execute("INSERT INTO t VALUES (1)").unwrap();

    let conn1 = db.connect().unwrap();
    let conn2 = db.connect().unwrap();

    // conn1 starts a read transaction and panics while holding the read lock
    let join_result = std::thread::spawn(move || {
        conn1.execute("BEGIN").unwrap();
        conn1.execute("SELECT * FROM t").unwrap();
        panic!("intentional panic while holding read tx");
    })
    .join();
    assert!(join_result.is_err(), "conn1 thread should panic");

    // TRUNCATE checkpoint requires that there be no readers - this would hang/fail if read lock wasn't released
    let res = conn2.pragma_update("wal_checkpoint", "TRUNCATE").unwrap();
    let row = res.first().unwrap();
    let truncate_succeeded = row.first().unwrap() == &turso_core::Value::from_i64(0)
        && row.get(1).unwrap() == &turso_core::Value::from_i64(0)
        && row.get(2).unwrap() == &turso_core::Value::from_i64(0);

    // Expect full truncate, i.e. 0 0 0 result.
    assert!(
        truncate_succeeded,
        "truncate should have succeeded, got checkpoint result: {res:?}"
    );
}

#[test]
fn test_wal_write_lock_released_on_conn_drop() {
    maybe_setup_tracing();
    let tmp_db = TempDatabase::new("test_wal_write_lock_released.db");
    let db = tmp_db.limbo_database();

    let conn1 = db.connect().unwrap();
    let conn2 = db.connect().unwrap();

    let join_result = std::thread::spawn(move || {
        conn1.execute("BEGIN IMMEDIATE").unwrap();
        panic!("intentional panic while holding write tx");
    })
    .join();
    assert!(join_result.is_err(), "conn1 thread should panic");

    conn2.set_busy_handler(Some(Box::new(move |_| {
        panic!("Got busy, this should not happen");
    })));

    conn2
        .execute("CREATE TABLE t (id integer primary key)")
        .unwrap();
}

/// Test that the WAL file is deleted when the last connection closes.
/// This matches SQLite behavior: when the last connection to a WAL-mode
/// database closes, the WAL is checkpointed and the file is removed.
#[test]
fn test_wal_file_deleted_on_last_connection_close() {
    maybe_setup_tracing();
    let tmp_db = TempDatabase::new("test_wal_deleted_on_close.db");
    let db_path = tmp_db.path.clone();
    let wal_path = db_path.with_extension("db-wal");

    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'hello')").unwrap();

    // WAL file should exist after writes
    assert!(wal_path.exists(), "WAL file should exist after writes");

    // Close the connection (last connection)
    conn.close().unwrap();

    // WAL file should be deleted after the last connection closes
    assert!(
        !wal_path.exists(),
        "WAL file should be deleted after last connection close"
    );

    // DB file should still exist with data
    assert!(db_path.exists(), "DB file should still exist");

    // Verify data is intact by reopening the database
    let tmp_db2 = TempDatabase::new_with_existent(&db_path);
    let conn2 = tmp_db2.connect_limbo();
    let result = execute_and_get_strings(&conn2, "SELECT val FROM t WHERE id = 1").unwrap();
    assert_eq!(result, vec!["hello"]);
}

/// Test that WAL file is deleted even with multiple statements and data.
#[test]
fn test_wal_file_deleted_after_multiple_writes() {
    maybe_setup_tracing();
    let tmp_db = TempDatabase::new("test_wal_deleted_multi_writes.db");
    let db_path = tmp_db.path.clone();
    let wal_path = db_path.with_extension("db-wal");

    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE t2 (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    for i in 0..100 {
        conn.execute(format!("INSERT INTO t1 VALUES ({i})"))
            .unwrap();
    }
    conn.execute("INSERT INTO t2 VALUES (1, 'test')").unwrap();

    conn.close().unwrap();

    assert!(
        !wal_path.exists(),
        "WAL file should be deleted after close with multiple writes"
    );

    // Verify all data is intact
    let tmp_db2 = TempDatabase::new_with_existent(&db_path);
    let conn2 = tmp_db2.connect_limbo();
    let count = execute_and_get_ints(&conn2, "SELECT COUNT(*) FROM t1").unwrap();
    assert_eq!(count, vec![100]);
    let name = execute_and_get_strings(&conn2, "SELECT name FROM t2").unwrap();
    assert_eq!(name, vec!["test"]);
}

/// Test that WAL file is NOT deleted when other connections are still open.
#[test]
fn test_wal_file_kept_when_other_connections_open() {
    maybe_setup_tracing();
    let tmp_db = TempDatabase::new("test_wal_kept_other_conns.db");
    let db = tmp_db.limbo_database();
    let db_path = tmp_db.path.clone();
    let wal_path = db_path.with_extension("db-wal");

    let conn1 = db.connect().unwrap();
    let conn2 = db.connect().unwrap();

    conn1
        .execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn1.execute("INSERT INTO t VALUES (1)").unwrap();

    // Close conn1 but conn2 is still open
    conn1.close().unwrap();

    // WAL file should still exist since conn2 is open
    assert!(
        wal_path.exists(),
        "WAL file should still exist when other connections are open"
    );

    // Close conn2 (last connection)
    conn2.close().unwrap();

    // Now WAL file should be deleted
    assert!(
        !wal_path.exists(),
        "WAL file should be deleted after ALL connections close"
    );
}

/// Test that WAL file deletion works correctly for an empty database
/// (only schema, no data rows).
#[test]
fn test_wal_file_deleted_empty_table() {
    maybe_setup_tracing();
    let tmp_db = TempDatabase::new("test_wal_deleted_empty.db");
    let db_path = tmp_db.path.clone();
    let wal_path = db_path.with_extension("db-wal");

    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();

    conn.close().unwrap();

    assert!(
        !wal_path.exists(),
        "WAL file should be deleted even for empty tables"
    );
}
