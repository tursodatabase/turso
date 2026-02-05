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
                turso_core::Value::Integer(i) => i,
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
    let truncate_succeeded = row.first().unwrap() == &turso_core::Value::Integer(0)
        && row.get(1).unwrap() == &turso_core::Value::Integer(0)
        && row.get(2).unwrap() == &turso_core::Value::Integer(0);

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
