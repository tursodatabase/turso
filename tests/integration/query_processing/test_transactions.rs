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
