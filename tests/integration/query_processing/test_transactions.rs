use turso_core::{LimboError, Result, StepResult, Value};

use crate::common::TempDatabase;

// Test a scenario where there are two concurrent deferred transactions:
//
// 1. Both transactions T1 and T2 start at the same time.
// 2. T1 writes to the database succesfully, but does not commit.
// 3. T2 attempts to write to the database, but gets busy error.
// 4. T1 commits
// 5. T2 attempts to write again and succeeds. This is because the transaction
//    was still fresh (no reads or writes happened).
#[test]
fn test_deferred_transaction_restart() {
    let tmp_db = TempDatabase::new("test_deferred_tx.db", true);
    let conn1 = tmp_db.connect_limbo();
    let conn2 = tmp_db.connect_limbo();

    conn1
        .execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
        .unwrap();

    conn1.execute("BEGIN").unwrap();
    conn2.execute("BEGIN").unwrap();

    conn1
        .execute("INSERT INTO test (id, value) VALUES (1, 'first')")
        .unwrap();

    let result = conn2.execute("INSERT INTO test (id, value) VALUES (2, 'second')");
    assert!(matches!(result, Err(LimboError::Busy)));

    conn1.execute("COMMIT").unwrap();

    conn2
        .execute("INSERT INTO test (id, value) VALUES (2, 'second')")
        .unwrap();
    conn2.execute("COMMIT").unwrap();

    let mut stmt = conn1.query("SELECT COUNT(*) FROM test").unwrap().unwrap();
    if let StepResult::Row = stmt.step().unwrap() {
        let row = stmt.row().unwrap();
        assert_eq!(*row.get::<&Value>(0).unwrap(), Value::Integer(2));
    }
}

// Test a scenario where a deferred transaction cannot restart due to prior reads:
//
// 1. Both transactions T1 and T2 start at the same time.
// 2. T2 performs a SELECT (establishes a read snapshot).
// 3. T1 writes to the database successfully, but does not commit.
// 4. T2 attempts to write to the database, but gets busy error.
// 5. T1 commits (invalidating T2's snapshot).
// 6. T2 attempts to write again but still gets BUSY - it cannot restart
//    because it has performed reads and has a committed snapshot.
#[test]
fn test_deferred_transaction_no_restart() {
    let tmp_db = TempDatabase::new("test_deferred_tx_no_restart.db", true);
    let conn1 = tmp_db.connect_limbo();
    let conn2 = tmp_db.connect_limbo();

    conn1
        .execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
        .unwrap();

    conn1.execute("BEGIN").unwrap();
    conn2.execute("BEGIN").unwrap();

    // T2 performs a read - this establishes a snapshot and prevents restart
    let mut stmt = conn2.query("SELECT COUNT(*) FROM test").unwrap().unwrap();
    if let StepResult::Row = stmt.step().unwrap() {
        let row = stmt.row().unwrap();
        assert_eq!(*row.get::<&Value>(0).unwrap(), Value::Integer(0));
    }

    conn1
        .execute("INSERT INTO test (id, value) VALUES (1, 'first')")
        .unwrap();

    let result = conn2.execute("INSERT INTO test (id, value) VALUES (2, 'second')");
    assert!(matches!(result, Err(LimboError::Busy)));

    conn1.execute("COMMIT").unwrap();

    // T2 still cannot write because its snapshot is stale and it cannot restart
    let result = conn2.execute("INSERT INTO test (id, value) VALUES (2, 'second')");
    assert!(matches!(result, Err(LimboError::Busy)));

    // T2 must rollback and start fresh
    conn2.execute("ROLLBACK").unwrap();
    conn2.execute("BEGIN").unwrap();
    conn2
        .execute("INSERT INTO test (id, value) VALUES (2, 'second')")
        .unwrap();
    conn2.execute("COMMIT").unwrap();

    let mut stmt = conn1.query("SELECT COUNT(*) FROM test").unwrap().unwrap();
    if let StepResult::Row = stmt.step().unwrap() {
        let row = stmt.row().unwrap();
        assert_eq!(*row.get::<&Value>(0).unwrap(), Value::Integer(2));
    }
}

#[test]
fn test_txn_error_doesnt_rollback_txn() -> Result<()> {
    let tmp_db = TempDatabase::new_with_rusqlite("create table t (x);", false);
    let conn = tmp_db.connect_limbo();

    conn.execute("begin")?;
    conn.execute("insert into t values (1)")?;
    // should fail
    assert!(conn
        .execute("begin")
        .inspect_err(|e| assert!(matches!(e, LimboError::TxError(_))))
        .is_err());
    conn.execute("insert into t values (1)")?;
    conn.execute("commit")?;
    let mut stmt = conn.query("select sum(x) from t")?.unwrap();
    if let StepResult::Row = stmt.step()? {
        let row = stmt.row().unwrap();
        assert_eq!(*row.get::<&Value>(0).unwrap(), Value::Integer(2));
    }

    Ok(())
}

#[test]
/// Connection 2 should see the initial data (table 'test' in schema + 2 rows). Regression test for #2997
/// It should then see another created table 'test2' in schema, as well.
fn test_transaction_visibility() {
    let tmp_db = TempDatabase::new("test_transaction_visibility.db", true);
    let conn1 = tmp_db.connect_limbo();
    let conn2 = tmp_db.connect_limbo();

    conn1
        .execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
        .unwrap();

    conn1
        .execute("INSERT INTO test (id, value) VALUES (1, 'initial')")
        .unwrap();

    let mut stmt = conn2.query("SELECT COUNT(*) FROM test").unwrap().unwrap();
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                assert_eq!(*row.get::<&Value>(0).unwrap(), Value::Integer(1));
            }
            StepResult::IO => stmt.run_once().unwrap(),
            StepResult::Done => break,
            StepResult::Busy => panic!("database is busy"),
            StepResult::Interrupt => panic!("interrupted"),
        }
    }

    conn1
        .execute("CREATE TABLE test2 (id INTEGER PRIMARY KEY, value TEXT)")
        .unwrap();

    let mut stmt = conn2.query("SELECT COUNT(*) FROM test2").unwrap().unwrap();
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                assert_eq!(*row.get::<&Value>(0).unwrap(), Value::Integer(0));
            }
            StepResult::IO => stmt.run_once().unwrap(),
            StepResult::Done => break,
            StepResult::Busy => panic!("database is busy"),
            StepResult::Interrupt => panic!("interrupted"),
        }
    }
}

#[test]
fn test_mvcc_transactions_autocommit() {
    let tmp_db = TempDatabase::new_with_opts(
        "test_mvcc_transactions_autocommit.db",
        turso_core::DatabaseOpts::new().with_mvcc(true),
    );
    let conn1 = tmp_db.connect_limbo();

    // This should work - basic CREATE TABLE in MVCC autocommit mode
    conn1
        .execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
        .unwrap();
}

#[test]
fn test_mvcc_transactions_immediate() {
    let tmp_db = TempDatabase::new_with_opts(
        "test_mvcc_transactions_immediate.db",
        turso_core::DatabaseOpts::new().with_mvcc(true),
    );
    let conn1 = tmp_db.connect_limbo();
    let conn2 = tmp_db.connect_limbo();

    conn1
        .execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
        .unwrap();

    // Start an immediate transaction
    conn1.execute("BEGIN IMMEDIATE").unwrap();

    // Another immediate transaction fails with BUSY
    let result = conn2.execute("BEGIN IMMEDIATE");
    assert!(matches!(result, Err(LimboError::Busy)));
}

#[test]
fn test_mvcc_transactions_deferred() {
    let tmp_db = TempDatabase::new_with_opts(
        "test_mvcc_transactions_deferred.db",
        turso_core::DatabaseOpts::new().with_mvcc(true),
    );
    let conn1 = tmp_db.connect_limbo();
    let conn2 = tmp_db.connect_limbo();

    conn1
        .execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
        .unwrap();

    conn1.execute("BEGIN DEFERRED").unwrap();
    conn2.execute("BEGIN DEFERRED").unwrap();

    conn1
        .execute("INSERT INTO test (id, value) VALUES (1, 'first')")
        .unwrap();

    let result = conn2.execute("INSERT INTO test (id, value) VALUES (2, 'second')");
    assert!(matches!(result, Err(LimboError::Busy)));

    conn1.execute("COMMIT").unwrap();

    conn2
        .execute("INSERT INTO test (id, value) VALUES (2, 'second')")
        .unwrap();
    conn2.execute("COMMIT").unwrap();

    let mut stmt = conn1.query("SELECT COUNT(*) FROM test").unwrap().unwrap();
    if let StepResult::Row = stmt.step().unwrap() {
        let row = stmt.row().unwrap();
        assert_eq!(*row.get::<&Value>(0).unwrap(), Value::Integer(2));
    }
}

#[test]
fn test_mvcc_insert_select_basic() {
    let tmp_db = TempDatabase::new_with_opts(
        "test_mvcc_update_basic.db",
        turso_core::DatabaseOpts::new().with_mvcc(true),
    );
    let conn1 = tmp_db.connect_limbo();

    conn1
        .execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
        .unwrap();

    conn1
        .execute("INSERT INTO test (id, value) VALUES (1, 'first')")
        .unwrap();

    let stmt = conn1
        .query("SELECT * FROM test WHERE id = 1")
        .unwrap()
        .unwrap();
    let row = helper_read_single_row(stmt);
    assert_eq!(row, vec![Value::Integer(1), Value::build_text("first")]);
}

#[test]
fn test_mvcc_update_basic() {
    let tmp_db = TempDatabase::new_with_opts(
        "test_mvcc_update_basic.db",
        turso_core::DatabaseOpts::new().with_mvcc(true),
    );
    let conn1 = tmp_db.connect_limbo();

    conn1
        .execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
        .unwrap();

    conn1
        .execute("INSERT INTO test (id, value) VALUES (1, 'first')")
        .unwrap();

    let stmt = conn1
        .query("SELECT value FROM test WHERE id = 1")
        .unwrap()
        .unwrap();
    let row = helper_read_single_row(stmt);
    assert_eq!(row, vec![Value::build_text("first")]);

    conn1
        .execute("UPDATE test SET value = 'second' WHERE id = 1")
        .unwrap();

    let stmt = conn1
        .query("SELECT value FROM test WHERE id = 1")
        .unwrap()
        .unwrap();
    let row = helper_read_single_row(stmt);
    assert_eq!(row, vec![Value::build_text("second")]);
}

#[test]
fn test_mvcc_concurrent_insert_basic() {
    let tmp_db = TempDatabase::new_with_opts(
        "test_mvcc_update_basic.db",
        turso_core::DatabaseOpts::new().with_mvcc(true),
    );
    let conn1 = tmp_db.connect_limbo();
    let conn2 = tmp_db.connect_limbo();

    conn1
        .execute("CREATE TABLE test (id INTEGER, value TEXT)")
        .unwrap();

    conn1.execute("BEGIN CONCURRENT").unwrap();
    conn2.execute("BEGIN CONCURRENT").unwrap();

    conn1
        .execute("INSERT INTO test (id, value) VALUES (1, 'first')")
        .unwrap();
    conn2
        .execute("INSERT INTO test (id, value) VALUES (2, 'second')")
        .unwrap();

    conn1.execute("COMMIT").unwrap();
    conn2.execute("COMMIT").unwrap();

    let stmt = conn1.query("SELECT * FROM test").unwrap().unwrap();
    let rows = helper_read_all_rows(stmt);
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(1), Value::build_text("first")],
            vec![Value::Integer(2), Value::build_text("second")],
        ]
    );
}

#[test]
fn test_mvcc_update_same_row_twice() {
    let tmp_db = TempDatabase::new_with_opts(
        "test_mvcc_update_same_row_twice.db",
        turso_core::DatabaseOpts::new().with_mvcc(true),
    );
    let conn1 = tmp_db.connect_limbo();

    conn1
        .execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
        .unwrap();

    conn1
        .execute("INSERT INTO test (id, value) VALUES (1, 'first')")
        .unwrap();

    conn1
        .execute("UPDATE test SET value = 'second' WHERE id = 1")
        .unwrap();

    let stmt = conn1
        .query("SELECT value FROM test WHERE id = 1")
        .unwrap()
        .unwrap();
    let row = helper_read_single_row(stmt);
    let Value::Text(value) = &row[0] else {
        panic!("expected text value");
    };
    assert_eq!(value.as_str(), "second");

    conn1
        .execute("UPDATE test SET value = 'third' WHERE id = 1")
        .unwrap();

    let stmt = conn1
        .query("SELECT value FROM test WHERE id = 1")
        .unwrap()
        .unwrap();
    let row = helper_read_single_row(stmt);
    let Value::Text(value) = &row[0] else {
        panic!("expected text value");
    };
    assert_eq!(value.as_str(), "third");
}

#[test]
fn test_mvcc_concurrent_conflicting_update() {
    let tmp_db = TempDatabase::new_with_opts(
        "test_mvcc_concurrent_conflicting_update.db",
        turso_core::DatabaseOpts::new().with_mvcc(true),
    );
    let conn1 = tmp_db.connect_limbo();
    let conn2 = tmp_db.connect_limbo();

    conn1
        .execute("CREATE TABLE test (id INTEGER, value TEXT)")
        .unwrap();

    conn1
        .execute("INSERT INTO test (id, value) VALUES (1, 'first')")
        .unwrap();

    conn1.execute("BEGIN CONCURRENT").unwrap();
    conn2.execute("BEGIN CONCURRENT").unwrap();

    conn1
        .execute("UPDATE test SET value = 'second' WHERE id = 1")
        .unwrap();
    let err = conn2
        .execute("UPDATE test SET value = 'third' WHERE id = 1")
        .expect_err("expected error");
    assert!(matches!(err, LimboError::WriteWriteConflict));
}

#[test]
fn test_mvcc_concurrent_conflicting_update_2() {
    let tmp_db = TempDatabase::new_with_opts(
        "test_mvcc_concurrent_conflicting_update.db",
        turso_core::DatabaseOpts::new().with_mvcc(true),
    );
    let conn1 = tmp_db.connect_limbo();
    let conn2 = tmp_db.connect_limbo();

    conn1
        .execute("CREATE TABLE test (id INTEGER, value TEXT)")
        .unwrap();

    conn1
        .execute("INSERT INTO test (id, value) VALUES (1, 'first'), (2, 'first')")
        .unwrap();

    conn1.execute("BEGIN CONCURRENT").unwrap();
    conn2.execute("BEGIN CONCURRENT").unwrap();

    conn1
        .execute("UPDATE test SET value = 'second' WHERE id = 1")
        .unwrap();
    let err = conn2
        .execute("UPDATE test SET value = 'third' WHERE id BETWEEN 0 AND 10")
        .expect_err("expected error");
    assert!(matches!(err, LimboError::WriteWriteConflict));
}

#[test]
fn test_mvcc_checkpoint_works() {
    let tmp_db = TempDatabase::new_with_opts(
        "test_mvcc_checkpoint_works.db",
        turso_core::DatabaseOpts::new().with_mvcc(true),
    );

    // Create table
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE test (id INTEGER, value TEXT)")
        .unwrap();

    // Insert rows from multiple connections
    let mut expected_rows = Vec::new();

    // Create 5 connections, each inserting 20 rows
    for conn_id in 0..5 {
        let conn = tmp_db.connect_limbo();
        conn.execute("BEGIN CONCURRENT").unwrap();

        // Each connection inserts rows with its own pattern
        for i in 0..20 {
            let id = conn_id * 100 + i;
            let value = format!("value_conn{conn_id}_row{i}");
            conn.execute(format!(
                "INSERT INTO test (id, value) VALUES ({id}, '{value}')",
            ))
            .unwrap();
            expected_rows.push((id, value));
        }

        conn.execute("COMMIT").unwrap();
    }

    // Before checkpoint: assert that the DB file size is exactly 4096, .db-wal size is exactly 32, and there is a nonzero size .db-lg file
    let db_file_size = std::fs::metadata(&tmp_db.path).unwrap().len();
    assert!(db_file_size == 4096);
    let wal_file_size = std::fs::metadata(tmp_db.path.with_extension("db-wal"))
        .unwrap()
        .len();
    assert!(
        wal_file_size == 0,
        "wal file size should be 0 bytes, but is {wal_file_size} bytes"
    );
    let lg_file_size = std::fs::metadata(tmp_db.path.with_extension("db-lg"))
        .unwrap()
        .len();
    assert!(lg_file_size > 0);

    // Sort expected rows to match ORDER BY id, value
    expected_rows.sort_by(|a, b| match a.0.cmp(&b.0) {
        std::cmp::Ordering::Equal => a.1.cmp(&b.1),
        other => other,
    });

    // Checkpoint
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    // Verify all rows after reopening database
    let tmp_db = TempDatabase::new_with_existent(&tmp_db.path, true);
    let conn = tmp_db.connect_limbo();
    let stmt = conn
        .query("SELECT * FROM test ORDER BY id, value")
        .unwrap()
        .unwrap();
    let rows = helper_read_all_rows(stmt);

    // Build expected results
    let expected: Vec<Vec<Value>> = expected_rows
        .into_iter()
        .map(|(id, value)| vec![Value::Integer(id as i64), Value::build_text(value)])
        .collect();

    assert_eq!(rows, expected);

    // Assert that the db file size is larger than 4096, assert .db-wal size is 32 bytes, assert there is no .db-lg file
    let db_file_size = std::fs::metadata(&tmp_db.path).unwrap().len();
    assert!(db_file_size > 4096);
    assert!(db_file_size % 4096 == 0);
    let wal_size = std::fs::metadata(tmp_db.path.with_extension("db-wal"))
        .unwrap()
        .len();
    assert!(
        wal_size == 0,
        "wal size should be 0 bytes, but is {wal_size} bytes"
    );
    let log_size = std::fs::metadata(tmp_db.path.with_extension("db-lg"))
        .unwrap()
        .len();
    assert!(
        log_size == 0,
        "log size should be 0 bytes, but is {log_size} bytes"
    );
}

fn helper_read_all_rows(mut stmt: turso_core::Statement) -> Vec<Vec<Value>> {
    let mut ret = Vec::new();
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                ret.push(stmt.row().unwrap().get_values().cloned().collect());
            }
            StepResult::IO => stmt.run_once().unwrap(),
            StepResult::Done => break,
            StepResult::Busy => panic!("database is busy"),
            StepResult::Interrupt => panic!("interrupted"),
        }
    }
    ret
}

fn helper_read_single_row(mut stmt: turso_core::Statement) -> Vec<Value> {
    let mut read_count = 0;
    let mut ret = None;
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                assert_eq!(read_count, 0);
                read_count += 1;
                let row = stmt.row().unwrap();
                ret = Some(row.get_values().cloned().collect());
            }
            StepResult::IO => stmt.run_once().unwrap(),
            StepResult::Done => break,
            StepResult::Busy => panic!("database is busy"),
            StepResult::Interrupt => panic!("interrupted"),
        }
    }

    ret.unwrap()
}
