use std::sync::Arc;

use turso_core::{Connection, LimboError, Result, Statement, StepResult, Value};

use crate::common::{assert_checkpoint_preserves_content, TempDatabase};

// Test a scenario where there are two concurrent deferred transactions:
//
// 1. Both transactions T1 and T2 start at the same time.
// 2. T1 writes to the database succesfully, but does not commit.
// 3. T2 attempts to write to the database, but gets busy error.
// 4. T1 commits
// 5. T2 attempts to write again and succeeds. This is because the transaction
//    was still fresh (no reads or writes happened).
#[turso_macros::test]
fn test_deferred_transaction_restart(tmp_db: TempDatabase) {
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
#[turso_macros::test]
fn test_deferred_transaction_no_restart(tmp_db: TempDatabase) {
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
    assert!(
        matches!(result, Err(LimboError::Busy)),
        "Expected Busy because write lock is taken, got: {result:?}"
    );

    conn1.execute("COMMIT").unwrap();

    // T2 still cannot write because its snapshot is stale and it cannot restart
    let result = conn2.execute("INSERT INTO test (id, value) VALUES (2, 'second')");
    assert!(matches!(result, Err(LimboError::BusySnapshot)), "Expected BusySnapshot because while write lock is free, the connection's snapshot is stale, got: {result:?}");

    // T2 must rollback and start fresh
    conn2.execute("ROLLBACK").unwrap();
    conn2.execute("BEGIN").unwrap();
    conn2
        .execute("INSERT INTO test (id, value) VALUES (2, 'second')")
        .unwrap();
    conn2.execute("COMMIT").unwrap();
    drop(stmt);

    let mut stmt = conn1.query("SELECT COUNT(*) FROM test").unwrap().unwrap();
    if let StepResult::Row = stmt.step().unwrap() {
        let row = stmt.row().unwrap();
        assert_eq!(*row.get::<&Value>(0).unwrap(), Value::Integer(2));
    }
}

#[turso_macros::test(init_sql = "create table t (x);")]
fn test_txn_error_doesnt_rollback_txn(tmp_db: TempDatabase) -> Result<()> {
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

#[turso_macros::test]
/// Connection 2 should see the initial data (table 'test' in schema + 2 rows). Regression test for #2997
/// It should then see another created table 'test2' in schema, as well.
fn test_transaction_visibility(tmp_db: TempDatabase) {
    let conn1 = tmp_db.connect_limbo();
    let conn2 = tmp_db.connect_limbo();

    conn1
        .execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
        .unwrap();

    conn1
        .execute("INSERT INTO test (id, value) VALUES (1, 'initial')")
        .unwrap();

    let mut stmt = conn2.query("SELECT COUNT(*) FROM test").unwrap().unwrap();
    stmt.run_with_row_callback(|row| {
        assert_eq!(*row.get::<&Value>(0).unwrap(), Value::Integer(1));
        Ok(())
    })
    .unwrap();

    conn1
        .execute("CREATE TABLE test2 (id INTEGER PRIMARY KEY, value TEXT)")
        .unwrap();

    let mut stmt = conn2.query("SELECT COUNT(*) FROM test2").unwrap().unwrap();
    stmt.run_with_row_callback(|row| {
        assert_eq!(*row.get::<&Value>(0).unwrap(), Value::Integer(0));
        Ok(())
    })
    .unwrap();
}

#[turso_macros::test]
/// A constraint error does not rollback the transaction, it rolls back the statement.
fn test_constraint_error_aborts_only_stmt_not_entire_transaction(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();

    // Create table succeeds
    conn.execute("CREATE TABLE t (a INTEGER PRIMARY KEY)")
        .unwrap();

    // Begin succeeds
    conn.execute("BEGIN").unwrap();

    // First insert succeeds
    conn.execute("INSERT INTO t VALUES (1),(2)").unwrap();

    // Second insert fails due to UNIQUE constraint
    let result = conn.execute("INSERT INTO t VALUES (2),(3)");
    assert!(matches!(result, Err(LimboError::Constraint(_))));

    // Third insert is valid again
    conn.execute("INSERT INTO t VALUES (4)").unwrap();

    // Commit succeeds
    conn.execute("COMMIT").unwrap();

    // Make sure table has 3 rows (a=1, a=2, a=4)
    let stmt = conn.query("SELECT a FROM t").unwrap().unwrap();
    let rows = helper_read_all_rows(stmt);
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(1)],
            vec![Value::Integer(2)],
            vec![Value::Integer(4)]
        ]
    );
}

#[turso_macros::test]
/// Regression test for https://github.com/tursodatabase/turso/issues/3784 where dirty pages
/// were flushed to WAL _before_ deferred FK violations were checked. This resulted in the
/// violations being persisted to the database, even though the transaction was aborted.
/// This test ensures that dirty pages are not flushed to WAL until after deferred violations are checked.
fn test_deferred_fk_violation_rollback_in_autocommit(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();

    // Enable foreign keys
    conn.execute("PRAGMA foreign_keys = ON").unwrap();

    // Create parent and child tables with deferred FK constraint
    conn.execute("CREATE TABLE parent(a PRIMARY KEY)").unwrap();
    conn.execute("CREATE TABLE child(a, b, FOREIGN KEY(b) REFERENCES parent(a) DEFERRABLE INITIALLY DEFERRED)")
        .unwrap();

    // This insert should fail because parent(1) doesn't exist
    // and the deferred FK violation should be caught at statement end in autocommit mode
    let result = conn.execute("INSERT INTO child VALUES(1,1)");
    assert!(matches!(result, Err(LimboError::ForeignKeyConstraint(_))));

    // Do a truncating checkpoint with dbhash verification
    assert_checkpoint_preserves_content(&conn, &tmp_db);

    // Verify that the child table is empty (the insert was rolled back)
    let stmt = conn.query("SELECT COUNT(*) FROM child").unwrap().unwrap();
    let row = helper_read_single_row(stmt);
    assert_eq!(row, vec![Value::Integer(0)]);
}

#[turso_macros::test(mvcc)]
fn test_mvcc_transactions_autocommit(tmp_db: TempDatabase) {
    let conn1 = tmp_db.connect_limbo();

    // This should work - basic CREATE TABLE in MVCC autocommit mode
    conn1
        .execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
        .unwrap();
}

#[turso_macros::test(mvcc)]
fn test_mvcc_transactions_immediate(tmp_db: TempDatabase) {
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

#[turso_macros::test(mvcc)]
fn test_mvcc_transactions_deferred(tmp_db: TempDatabase) {
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

#[turso_macros::test(mvcc)]
fn test_mvcc_insert_select_basic(tmp_db: TempDatabase) {
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

#[turso_macros::test(mvcc)]
fn test_mvcc_update_basic(tmp_db: TempDatabase) {
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
    let tmp_db = TempDatabase::new_with_mvcc("test_mvcc_update_basic.db");
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

#[turso_macros::test(mvcc)]
fn test_mvcc_update_same_row_twice(tmp_db: TempDatabase) {
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
    let tmp_db = TempDatabase::new_with_mvcc("test_mvcc_concurrent_conflicting_update.db");
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
    let tmp_db = TempDatabase::new_with_mvcc("test_mvcc_concurrent_conflicting_update.db");
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
    let tmp_db = TempDatabase::new_with_mvcc("test_mvcc_checkpoint_works.db");

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

    // Before checkpoint: assert that the DB file size is exactly 4096, .db-wal size is exactly 32, and there is a nonzero size .db-log file
    let db_file_size = std::fs::metadata(&tmp_db.path).unwrap().len();
    assert!(db_file_size == 4096);
    let wal_file_size = std::fs::metadata(tmp_db.path.with_extension("db-wal"))
        .unwrap()
        .len();
    assert!(
        wal_file_size == 0,
        "wal file size should be 0 bytes, but is {wal_file_size} bytes"
    );
    let lg_file_size = std::fs::metadata(tmp_db.path.with_extension("db-log"))
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
    let tmp_db = TempDatabase::new_with_existent(&tmp_db.path);
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

    // Assert that the db file size is larger than 4096, assert .db-wal size is 32 bytes, assert there is no .db-log file
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
    let log_size = std::fs::metadata(tmp_db.path.with_extension("db-log"))
        .unwrap()
        .len();
    assert!(
        log_size == 0,
        "log size should be 0 bytes, but is {log_size} bytes"
    );
}

fn execute_and_log(conn: &Arc<Connection>, query: &str) -> Result<()> {
    tracing::info!("Executing query: {}", query);
    conn.execute(query)
}

fn query_and_log(conn: &Arc<Connection>, query: &str) -> Result<Option<Statement>> {
    tracing::info!("Executing query: {}", query);
    conn.query(query)
}

#[test]
fn test_mvcc_recovery_of_both_checkpointed_and_noncheckpointed_tables_works() {
    let tmp_db = TempDatabase::new_with_mvcc(
        "test_mvcc_recovery_of_both_checkpointed_and_noncheckpointed_tables_works.db",
    );
    let conn = tmp_db.connect_limbo();

    // Create first table and insert rows
    execute_and_log(
        &conn,
        "CREATE TABLE test1 (id INTEGER PRIMARY KEY, value INTEGER)",
    )
    .unwrap();

    let mut expected_rows1 = Vec::new();
    for i in 0..10 {
        let value = i * 10;
        execute_and_log(
            &conn,
            &format!("INSERT INTO test1 (id, value) VALUES ({i}, {value})"),
        )
        .unwrap();
        expected_rows1.push((i, value));
    }

    // Checkpoint
    execute_and_log(&conn, "PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    // Create second table and insert rows
    execute_and_log(
        &conn,
        "CREATE TABLE test2 (id INTEGER PRIMARY KEY, value INTEGER)",
    )
    .unwrap();

    let mut expected_rows2 = Vec::new();
    for i in 0..5 {
        let value = i * 20;
        execute_and_log(
            &conn,
            &format!("INSERT INTO test2 (id, value) VALUES ({i}, {value})"),
        )
        .unwrap();
        expected_rows2.push((i, value));
    }

    // Sort expected rows
    expected_rows1.sort_by(|a, b| match a.0.cmp(&b.0) {
        std::cmp::Ordering::Equal => a.1.cmp(&b.1),
        other => other,
    });
    expected_rows2.sort_by(|a, b| match a.0.cmp(&b.0) {
        std::cmp::Ordering::Equal => a.1.cmp(&b.1),
        other => other,
    });

    let path = tmp_db.path.clone();
    drop(conn);
    drop(tmp_db);

    // Close and reopen database
    let tmp_db = TempDatabase::new_with_existent(&path);
    let conn = tmp_db.connect_limbo();

    // Verify table1 rows
    let stmt = query_and_log(&conn, "SELECT * FROM test1 ORDER BY id, value")
        .unwrap()
        .unwrap();
    let rows = helper_read_all_rows(stmt);

    let expected1: Vec<Vec<Value>> = expected_rows1
        .into_iter()
        .map(|(id, value)| vec![Value::Integer(id as i64), Value::Integer(value as i64)])
        .collect();

    assert_eq!(rows, expected1);

    // Verify table2 rows
    let stmt = query_and_log(&conn, "SELECT * FROM test2 ORDER BY id, value")
        .unwrap()
        .unwrap();
    let rows = helper_read_all_rows(stmt);

    let expected2: Vec<Vec<Value>> = expected_rows2
        .into_iter()
        .map(|(id, value)| vec![Value::Integer(id as i64), Value::Integer(value as i64)])
        .collect();

    assert_eq!(rows, expected2);
}

#[test]
fn test_non_mvcc_to_mvcc() {
    // Create non-mvcc database
    let tmp_db = TempDatabase::new("test_non_mvcc_to_mvcc.db");
    let conn = tmp_db.connect_limbo();

    // Create table and insert data
    execute_and_log(
        &conn,
        "CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)",
    )
    .unwrap();
    execute_and_log(&conn, "INSERT INTO test VALUES (1, 'hello')").unwrap();

    // Checkpoint to persist changes
    execute_and_log(&conn, "PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let path = tmp_db.path.clone();
    drop(conn);
    drop(tmp_db);

    // Reopen in mvcc mode
    let tmp_db = TempDatabase::new_with_existent(&path);
    let conn = tmp_db.connect_limbo();

    // Query should work
    let stmt = query_and_log(&conn, "SELECT * FROM test").unwrap().unwrap();
    let rows = helper_read_all_rows(stmt);

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Integer(1));
    assert_eq!(rows[0][1], Value::Text("hello".into()));
}

fn helper_read_all_rows(mut stmt: turso_core::Statement) -> Vec<Vec<Value>> {
    let mut ret = Vec::new();
    stmt.run_with_row_callback(|row| {
        ret.push(row.get_values().cloned().collect());
        Ok(())
    })
    .unwrap();

    ret
}

fn helper_read_single_row(mut stmt: turso_core::Statement) -> Vec<Value> {
    let ret = stmt.run_one_step_blocking(|| Ok(()), || Ok(())).unwrap();

    ret.map(|row| row.get_values().cloned().collect()).unwrap()
}

// Helper function to verify table contents
fn verify_table_contents(conn: &Arc<Connection>, expected: Vec<i64>) {
    let stmt = query_and_log(conn, "SELECT x FROM t ORDER BY x")
        .unwrap()
        .unwrap();
    let rows = helper_read_all_rows(stmt);
    let expected_values: Vec<Vec<Value>> = expected
        .into_iter()
        .map(|x| vec![Value::Integer(x)])
        .collect();
    assert_eq!(rows, expected_values);
}

#[test]
fn test_mvcc_recovery_with_index_and_deletes() {
    let tmp_db = TempDatabase::new_with_mvcc("test_mvcc_recovery_with_index_and_deletes.db");
    let conn = tmp_db.connect_limbo();

    // Create table with unique constraint (creates an index)
    execute_and_log(&conn, "CREATE TABLE t (x INTEGER UNIQUE)").unwrap();

    // Insert 5 values
    for i in 1..=5 {
        execute_and_log(&conn, &format!("INSERT INTO t VALUES ({i})")).unwrap();
    }

    // Delete values 2 and 4
    execute_and_log(&conn, "DELETE FROM t WHERE x = 2").unwrap();
    execute_and_log(&conn, "DELETE FROM t WHERE x = 4").unwrap();

    let path = tmp_db.path.clone();
    drop(conn);
    drop(tmp_db);

    // Reopen database (triggers logical log recovery)
    let tmp_db = TempDatabase::new_with_existent(&path);
    let conn = tmp_db.connect_limbo();

    // Verify only rows 1, 3, 5 exist
    let stmt = query_and_log(&conn, "SELECT x FROM t ORDER BY x")
        .unwrap()
        .unwrap();
    let rows = helper_read_all_rows(stmt);

    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(1)],
            vec![Value::Integer(3)],
            vec![Value::Integer(5)],
        ]
    );
}

#[test]
fn test_mvcc_checkpoint_before_delete_then_verify_same_session() {
    let tmp_db = TempDatabase::new_with_mvcc(
        "test_mvcc_checkpoint_before_delete_then_verify_same_session.db",
    );
    let conn = tmp_db.connect_limbo();

    execute_and_log(&conn, "CREATE TABLE t (x)").unwrap();
    execute_and_log(
        &conn,
        "INSERT INTO t SELECT value FROM generate_series(1,3)",
    )
    .unwrap();
    execute_and_log(&conn, "CREATE INDEX lol ON t(x)").unwrap();
    execute_and_log(&conn, "PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    execute_and_log(&conn, "DELETE FROM t WHERE x = 2").unwrap();

    verify_table_contents(&conn, vec![1, 3]);
}

#[test]
fn test_mvcc_checkpoint_before_delete_then_reopen() {
    let tmp_db = TempDatabase::new_with_mvcc("test_mvcc_checkpoint_before_delete_then_reopen.db");
    let conn = tmp_db.connect_limbo();

    execute_and_log(&conn, "CREATE TABLE t (x)").unwrap();
    execute_and_log(
        &conn,
        "INSERT INTO t SELECT value FROM generate_series(1,3)",
    )
    .unwrap();
    execute_and_log(&conn, "CREATE INDEX lol ON t(x)").unwrap();
    execute_and_log(&conn, "PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    execute_and_log(&conn, "DELETE FROM t WHERE x = 2").unwrap();

    let path = tmp_db.path.clone();
    drop(conn);
    drop(tmp_db);

    let tmp_db = TempDatabase::new_with_existent(&path);
    let conn = tmp_db.connect_limbo();

    verify_table_contents(&conn, vec![1, 3]);
}

#[test]
fn test_mvcc_delete_then_checkpoint_then_verify_same_session() {
    let tmp_db =
        TempDatabase::new_with_mvcc("test_mvcc_delete_then_checkpoint_then_verify_same_session.db");
    let conn = tmp_db.connect_limbo();

    execute_and_log(&conn, "CREATE TABLE t (x)").unwrap();
    execute_and_log(
        &conn,
        "INSERT INTO t SELECT value FROM generate_series(1,3)",
    )
    .unwrap();
    execute_and_log(&conn, "CREATE INDEX lol ON t(x)").unwrap();
    execute_and_log(&conn, "DELETE FROM t WHERE x = 2").unwrap();
    execute_and_log(&conn, "PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    verify_table_contents(&conn, vec![1, 3]);
}

#[test]
fn test_mvcc_delete_then_checkpoint_then_reopen() {
    let tmp_db = TempDatabase::new_with_mvcc("test_mvcc_delete_then_checkpoint_then_reopen.db");
    let conn = tmp_db.connect_limbo();

    execute_and_log(&conn, "CREATE TABLE t (x)").unwrap();
    execute_and_log(
        &conn,
        "INSERT INTO t SELECT value FROM generate_series(1,3)",
    )
    .unwrap();
    execute_and_log(&conn, "CREATE INDEX lol ON t(x)").unwrap();
    execute_and_log(&conn, "DELETE FROM t WHERE x = 2").unwrap();
    execute_and_log(&conn, "PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let path = tmp_db.path.clone();
    drop(conn);
    drop(tmp_db);

    let tmp_db = TempDatabase::new_with_existent(&path);
    let conn = tmp_db.connect_limbo();

    verify_table_contents(&conn, vec![1, 3]);
}

#[test]
fn test_mvcc_delete_then_reopen_no_checkpoint() {
    let tmp_db = TempDatabase::new_with_mvcc("test_mvcc_delete_then_reopen_no_checkpoint.db");
    let conn = tmp_db.connect_limbo();

    execute_and_log(&conn, "CREATE TABLE t (x)").unwrap();
    execute_and_log(
        &conn,
        "INSERT INTO t SELECT value FROM generate_series(1,3)",
    )
    .unwrap();
    execute_and_log(&conn, "CREATE INDEX lol ON t(x)").unwrap();
    execute_and_log(&conn, "DELETE FROM t WHERE x = 2").unwrap();

    let path = tmp_db.path.clone();
    drop(conn);
    drop(tmp_db);

    let tmp_db = TempDatabase::new_with_existent(&path);
    let conn = tmp_db.connect_limbo();

    verify_table_contents(&conn, vec![1, 3]);
}

#[test]
fn test_mvcc_checkpoint_delete_checkpoint_then_verify_same_session() {
    let tmp_db = TempDatabase::new_with_mvcc(
        "test_mvcc_checkpoint_delete_checkpoint_then_verify_same_session.db",
    );
    let conn = tmp_db.connect_limbo();

    execute_and_log(&conn, "CREATE TABLE t (x)").unwrap();
    execute_and_log(
        &conn,
        "INSERT INTO t SELECT value FROM generate_series(1,3)",
    )
    .unwrap();
    execute_and_log(&conn, "CREATE INDEX lol ON t(x)").unwrap();
    execute_and_log(&conn, "PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    execute_and_log(&conn, "DELETE FROM t WHERE x = 2").unwrap();
    execute_and_log(&conn, "PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    verify_table_contents(&conn, vec![1, 3]);
}

#[test]
fn test_mvcc_checkpoint_delete_checkpoint_then_reopen() {
    let tmp_db =
        TempDatabase::new_with_mvcc("test_mvcc_checkpoint_delete_checkpoint_then_reopen.db");
    let conn = tmp_db.connect_limbo();

    execute_and_log(&conn, "CREATE TABLE t (x)").unwrap();
    execute_and_log(
        &conn,
        "INSERT INTO t SELECT value FROM generate_series(1,3)",
    )
    .unwrap();
    execute_and_log(&conn, "CREATE INDEX lol ON t(x)").unwrap();
    execute_and_log(&conn, "PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    execute_and_log(&conn, "DELETE FROM t WHERE x = 2").unwrap();
    execute_and_log(&conn, "PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let path = tmp_db.path.clone();
    drop(conn);
    drop(tmp_db);

    let tmp_db = TempDatabase::new_with_existent(&path);
    let conn = tmp_db.connect_limbo();

    verify_table_contents(&conn, vec![1, 3]);
}

#[test]
fn test_mvcc_index_before_checkpoint_delete_after_checkpoint() {
    let tmp_db =
        TempDatabase::new_with_mvcc("test_mvcc_index_before_checkpoint_delete_after_checkpoint.db");
    let conn = tmp_db.connect_limbo();

    execute_and_log(&conn, "CREATE TABLE t (x)").unwrap();
    execute_and_log(
        &conn,
        "INSERT INTO t SELECT value FROM generate_series(1,3)",
    )
    .unwrap();
    execute_and_log(&conn, "CREATE INDEX lol ON t(x)").unwrap();
    execute_and_log(&conn, "PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    execute_and_log(&conn, "DELETE FROM t WHERE x = 2").unwrap();

    let path = tmp_db.path.clone();
    drop(conn);
    drop(tmp_db);

    let tmp_db = TempDatabase::new_with_existent(&path);
    let conn = tmp_db.connect_limbo();

    verify_table_contents(&conn, vec![1, 3]);
}

#[test]
fn test_mvcc_index_after_checkpoint_delete_after_index() {
    let tmp_db =
        TempDatabase::new_with_mvcc("test_mvcc_index_after_checkpoint_delete_after_index.db");
    let conn = tmp_db.connect_limbo();

    execute_and_log(&conn, "CREATE TABLE t (x)").unwrap();
    execute_and_log(
        &conn,
        "INSERT INTO t SELECT value FROM generate_series(1,3)",
    )
    .unwrap();
    execute_and_log(&conn, "PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    execute_and_log(&conn, "CREATE INDEX lol ON t(x)").unwrap();
    execute_and_log(&conn, "DELETE FROM t WHERE x = 2").unwrap();

    let path = tmp_db.path.clone();
    drop(conn);
    drop(tmp_db);

    let tmp_db = TempDatabase::new_with_existent(&path);
    let conn = tmp_db.connect_limbo();

    verify_table_contents(&conn, vec![1, 3]);
}

#[test]
fn test_mvcc_multiple_deletes_with_checkpoints() {
    let tmp_db = TempDatabase::new_with_mvcc("test_mvcc_multiple_deletes_with_checkpoints.db");
    let conn = tmp_db.connect_limbo();

    execute_and_log(&conn, "CREATE TABLE t (x)").unwrap();
    execute_and_log(
        &conn,
        "INSERT INTO t SELECT value FROM generate_series(1,5)",
    )
    .unwrap();
    execute_and_log(&conn, "CREATE INDEX lol ON t(x)").unwrap();
    execute_and_log(&conn, "PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    execute_and_log(&conn, "DELETE FROM t WHERE x = 2").unwrap();
    execute_and_log(&conn, "PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    execute_and_log(&conn, "DELETE FROM t WHERE x = 4").unwrap();

    verify_table_contents(&conn, vec![1, 3, 5]);

    let path = tmp_db.path.clone();
    drop(conn);
    drop(tmp_db);

    let tmp_db = TempDatabase::new_with_existent(&path);
    let conn = tmp_db.connect_limbo();

    verify_table_contents(&conn, vec![1, 3, 5]);
}

#[test]
fn test_mvcc_no_index_checkpoint_delete_reopen() {
    let tmp_db = TempDatabase::new_with_mvcc("test_mvcc_no_index_checkpoint_delete_reopen.db");
    let conn = tmp_db.connect_limbo();

    execute_and_log(&conn, "CREATE TABLE t (x)").unwrap();
    execute_and_log(
        &conn,
        "INSERT INTO t SELECT value FROM generate_series(1,3)",
    )
    .unwrap();
    execute_and_log(&conn, "PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    execute_and_log(&conn, "DELETE FROM t WHERE x = 2").unwrap();

    let path = tmp_db.path.clone();
    drop(conn);
    drop(tmp_db);

    let tmp_db = TempDatabase::new_with_existent(&path);
    let conn = tmp_db.connect_limbo();

    verify_table_contents(&conn, vec![1, 3]);
}

#[test]
fn test_mvcc_checkpoint_before_insert_delete_after_checkpoint() {
    let tmp_db = TempDatabase::new_with_mvcc(
        "test_mvcc_checkpoint_before_insert_delete_after_checkpoint.db",
    );
    let conn = tmp_db.connect_limbo();

    execute_and_log(&conn, "CREATE TABLE t (x)").unwrap();
    execute_and_log(
        &conn,
        "INSERT INTO t SELECT value FROM generate_series(1,2)",
    )
    .unwrap();
    execute_and_log(&conn, "CREATE INDEX lol ON t(x)").unwrap();
    execute_and_log(&conn, "PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    execute_and_log(
        &conn,
        "INSERT INTO t SELECT value FROM generate_series(3,4)",
    )
    .unwrap();
    execute_and_log(&conn, "DELETE FROM t WHERE x = 2").unwrap();
    execute_and_log(&conn, "DELETE FROM t WHERE x = 3").unwrap();

    verify_table_contents(&conn, vec![1, 4]);

    let path = tmp_db.path.clone();
    drop(conn);
    drop(tmp_db);

    let tmp_db = TempDatabase::new_with_existent(&path);
    let conn = tmp_db.connect_limbo();

    verify_table_contents(&conn, vec![1, 4]);
}

// Tests for dual-iteration seek: verifying that seek works correctly
// when there are rows in both btree (after checkpoint) and MVCC store.

/// Test table seek (WHERE rowid = x) with rows in both btree and MVCC.
/// After checkpoint, rows 1-5 are in btree. Then we insert 6-10 into MVCC.
/// Seeking for various rowids should find them in the correct location.
#[test]
fn test_mvcc_dual_seek_table_rowid_basic() {
    let tmp_db = TempDatabase::new_with_mvcc("test_mvcc_dual_seek_table_rowid_basic.db");
    let conn = tmp_db.connect_limbo();

    // Create table and insert initial rows
    execute_and_log(&conn, "CREATE TABLE t (x INTEGER PRIMARY KEY, v TEXT)").unwrap();
    for i in 1..=5 {
        execute_and_log(&conn, &format!("INSERT INTO t VALUES ({i}, 'btree_{i}')")).unwrap();
    }

    // Checkpoint to move rows to btree
    execute_and_log(&conn, "PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let path = tmp_db.path.clone();
    drop(conn);
    drop(tmp_db);

    // Reopen to ensure btree is populated and MV store is empty
    let tmp_db = TempDatabase::new_with_existent(&path);
    let conn = tmp_db.connect_limbo();

    // Insert more rows into MVCC
    for i in 6..=10 {
        execute_and_log(&conn, &format!("INSERT INTO t VALUES ({i}, 'mvcc_{i}')")).unwrap();
    }

    // Seek for a row in btree
    let stmt = query_and_log(&conn, "SELECT v FROM t WHERE x = 3")
        .unwrap()
        .unwrap();
    let row = helper_read_single_row(stmt);
    assert_eq!(row, vec![Value::build_text("btree_3")]);

    // Seek for a row in MVCC
    let stmt = query_and_log(&conn, "SELECT v FROM t WHERE x = 8")
        .unwrap()
        .unwrap();
    let row = helper_read_single_row(stmt);
    assert_eq!(row, vec![Value::build_text("mvcc_8")]);

    // Seek for first row (btree)
    let stmt = query_and_log(&conn, "SELECT v FROM t WHERE x = 1")
        .unwrap()
        .unwrap();
    let row = helper_read_single_row(stmt);
    assert_eq!(row, vec![Value::build_text("btree_1")]);

    // Seek for last row (MVCC)
    let stmt = query_and_log(&conn, "SELECT v FROM t WHERE x = 10")
        .unwrap()
        .unwrap();
    let row = helper_read_single_row(stmt);
    assert_eq!(row, vec![Value::build_text("mvcc_10")]);

    // Seek for non-existent row
    let stmt = query_and_log(&conn, "SELECT v FROM t WHERE x = 100")
        .unwrap()
        .unwrap();
    let rows = helper_read_all_rows(stmt);
    assert!(rows.is_empty());
}

/// Test seek with interleaved rows in btree and MVCC.
/// Btree has odd numbers (1,3,5,7,9), MVCC has even numbers (2,4,6,8,10).
#[test]
fn test_mvcc_dual_seek_interleaved_rows() {
    let tmp_db = TempDatabase::new_with_mvcc("test_mvcc_dual_seek_interleaved_rows.db");
    let conn = tmp_db.connect_limbo();

    // Create table and insert odd rows
    execute_and_log(&conn, "CREATE TABLE t (x INTEGER PRIMARY KEY, v TEXT)").unwrap();
    for i in [1, 3, 5, 7, 9] {
        execute_and_log(&conn, &format!("INSERT INTO t VALUES ({i}, 'btree_{i}')")).unwrap();
    }

    // Checkpoint to move rows to btree
    execute_and_log(&conn, "PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let path = tmp_db.path.clone();
    drop(conn);
    drop(tmp_db);

    // Reopen
    let tmp_db = TempDatabase::new_with_existent(&path);
    let conn = tmp_db.connect_limbo();

    // Insert even rows into MVCC
    for i in [2, 4, 6, 8, 10] {
        execute_and_log(&conn, &format!("INSERT INTO t VALUES ({i}, 'mvcc_{i}')")).unwrap();
    }

    // Full table scan should return all rows in order
    let stmt = query_and_log(&conn, "SELECT x, v FROM t ORDER BY x")
        .unwrap()
        .unwrap();
    let rows = helper_read_all_rows(stmt);
    assert_eq!(rows.len(), 10);
    for (i, row) in rows.iter().enumerate() {
        let expected_x = (i + 1) as i64;
        assert_eq!(row[0], Value::Integer(expected_x));
        let expected_source = if expected_x % 2 == 1 { "btree" } else { "mvcc" };
        assert_eq!(
            row[1],
            Value::build_text(format!("{expected_source}_{expected_x}"))
        );
    }

    // Seek for btree row
    let stmt = query_and_log(&conn, "SELECT v FROM t WHERE x = 5")
        .unwrap()
        .unwrap();
    let row = helper_read_single_row(stmt);
    assert_eq!(row, vec![Value::build_text("btree_5")]);

    // Seek for MVCC row
    let stmt = query_and_log(&conn, "SELECT v FROM t WHERE x = 6")
        .unwrap()
        .unwrap();
    let row = helper_read_single_row(stmt);
    assert_eq!(row, vec![Value::build_text("mvcc_6")]);
}

/// Test index seek with rows in both btree and MVCC.
#[test]
fn test_mvcc_dual_seek_index_basic() {
    let tmp_db = TempDatabase::new_with_mvcc("test_mvcc_dual_seek_index_basic.db");
    let conn = tmp_db.connect_limbo();

    // Create table with index
    execute_and_log(&conn, "CREATE TABLE t (x INTEGER, v TEXT)").unwrap();
    execute_and_log(&conn, "CREATE INDEX idx_x ON t(x)").unwrap();

    // Insert initial rows
    for i in 1..=5 {
        execute_and_log(&conn, &format!("INSERT INTO t VALUES ({i}, 'btree_{i}')")).unwrap();
    }

    // Checkpoint
    execute_and_log(&conn, "PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let path = tmp_db.path.clone();
    drop(conn);
    drop(tmp_db);

    // Reopen
    let tmp_db = TempDatabase::new_with_existent(&path);
    let conn = tmp_db.connect_limbo();

    // Insert more rows into MVCC
    for i in 6..=10 {
        execute_and_log(&conn, &format!("INSERT INTO t VALUES ({i}, 'mvcc_{i}')")).unwrap();
    }

    // Index seek for btree row
    let stmt = query_and_log(&conn, "SELECT v FROM t WHERE x = 3")
        .unwrap()
        .unwrap();
    let row = helper_read_single_row(stmt);
    assert_eq!(row, vec![Value::build_text("btree_3")]);

    // Index seek for MVCC row
    let stmt = query_and_log(&conn, "SELECT v FROM t WHERE x = 8")
        .unwrap()
        .unwrap();
    let row = helper_read_single_row(stmt);
    assert_eq!(row, vec![Value::build_text("mvcc_8")]);

    // Range scan should return all matching rows in order
    let stmt = query_and_log(&conn, "SELECT x FROM t WHERE x >= 4 AND x <= 7 ORDER BY x")
        .unwrap()
        .unwrap();
    let rows = helper_read_all_rows(stmt);
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(4)],
            vec![Value::Integer(5)],
            vec![Value::Integer(6)],
            vec![Value::Integer(7)],
        ]
    );
}

/// Test seek with updates: row exists in btree but is updated in MVCC.
/// The seek should find the MVCC version (which shadows btree).
#[test]
fn test_mvcc_dual_seek_with_update() {
    let tmp_db = TempDatabase::new_with_mvcc("test_mvcc_dual_seek_with_update.db");
    let conn = tmp_db.connect_limbo();

    // Create table and insert rows
    execute_and_log(&conn, "CREATE TABLE t (x INTEGER PRIMARY KEY, v TEXT)").unwrap();
    for i in 1..=5 {
        execute_and_log(
            &conn,
            &format!("INSERT INTO t VALUES ({i}, 'original_{i}')"),
        )
        .unwrap();
    }

    // Checkpoint
    execute_and_log(&conn, "PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let path = tmp_db.path.clone();
    drop(conn);
    drop(tmp_db);

    // Reopen
    let tmp_db = TempDatabase::new_with_existent(&path);
    let conn = tmp_db.connect_limbo();

    // Update row 3 (creates MVCC version that shadows btree)
    execute_and_log(&conn, "UPDATE t SET v = 'updated_3' WHERE x = 3").unwrap();

    // Seek for updated row should return MVCC version
    let stmt = query_and_log(&conn, "SELECT v FROM t WHERE x = 3")
        .unwrap()
        .unwrap();
    let row = helper_read_single_row(stmt);
    assert_eq!(row, vec![Value::build_text("updated_3")]);

    // Seek for non-updated row should still work
    let stmt = query_and_log(&conn, "SELECT v FROM t WHERE x = 2")
        .unwrap()
        .unwrap();
    let row = helper_read_single_row(stmt);
    assert_eq!(row, vec![Value::build_text("original_2")]);

    // Full scan should show correct values
    let stmt = query_and_log(&conn, "SELECT x, v FROM t ORDER BY x")
        .unwrap()
        .unwrap();
    let rows = helper_read_all_rows(stmt);
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(1), Value::build_text("original_1")],
            vec![Value::Integer(2), Value::build_text("original_2")],
            vec![Value::Integer(3), Value::build_text("updated_3")],
            vec![Value::Integer(4), Value::build_text("original_4")],
            vec![Value::Integer(5), Value::build_text("original_5")],
        ]
    );
}

/// Test seek with delete: row exists in btree but is deleted in MVCC.
/// The seek should NOT find the deleted row.
#[test]
fn test_mvcc_dual_seek_with_delete() {
    let tmp_db = TempDatabase::new_with_mvcc("test_mvcc_dual_seek_with_delete.db");
    let conn = tmp_db.connect_limbo();

    // Create table and insert rows
    execute_and_log(&conn, "CREATE TABLE t (x INTEGER PRIMARY KEY, v TEXT)").unwrap();
    for i in 1..=5 {
        execute_and_log(&conn, &format!("INSERT INTO t VALUES ({i}, 'value_{i}')")).unwrap();
    }

    // Checkpoint
    execute_and_log(&conn, "PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let path = tmp_db.path.clone();
    drop(conn);
    drop(tmp_db);

    // Reopen
    let tmp_db = TempDatabase::new_with_existent(&path);
    let conn = tmp_db.connect_limbo();

    // Delete row 3
    execute_and_log(&conn, "DELETE FROM t WHERE x = 3").unwrap();

    // Seek for deleted row should return nothing
    let stmt = query_and_log(&conn, "SELECT v FROM t WHERE x = 3")
        .unwrap()
        .unwrap();
    let rows = helper_read_all_rows(stmt);
    assert!(rows.is_empty());

    // Seek for non-deleted row should still work
    let stmt = query_and_log(&conn, "SELECT v FROM t WHERE x = 2")
        .unwrap()
        .unwrap();
    let row = helper_read_single_row(stmt);
    assert_eq!(row, vec![Value::build_text("value_2")]);

    // Full scan should not include deleted row
    let stmt = query_and_log(&conn, "SELECT x FROM t ORDER BY x")
        .unwrap()
        .unwrap();
    let rows = helper_read_all_rows(stmt);
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(1)],
            vec![Value::Integer(2)],
            vec![Value::Integer(4)],
            vec![Value::Integer(5)],
        ]
    );
}

/// Test range seek (GT, LT operations) with dual iteration.
#[test]
fn test_mvcc_dual_seek_range_operations() {
    let tmp_db = TempDatabase::new_with_mvcc("test_mvcc_dual_seek_range_operations.db");
    let conn = tmp_db.connect_limbo();

    // Create table and insert rows
    execute_and_log(&conn, "CREATE TABLE t (x INTEGER PRIMARY KEY)").unwrap();
    for i in [1, 3, 5] {
        execute_and_log(&conn, &format!("INSERT INTO t VALUES ({i})")).unwrap();
    }

    // Checkpoint
    execute_and_log(&conn, "PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let path = tmp_db.path.clone();
    drop(conn);
    drop(tmp_db);

    // Reopen
    let tmp_db = TempDatabase::new_with_existent(&path);
    let conn = tmp_db.connect_limbo();

    // Insert more rows into MVCC
    for i in [2, 4, 6] {
        execute_and_log(&conn, &format!("INSERT INTO t VALUES ({i})")).unwrap();
    }

    // Range: x > 2 (should include 3,4,5,6)
    let stmt = query_and_log(&conn, "SELECT x FROM t WHERE x > 2 ORDER BY x")
        .unwrap()
        .unwrap();
    let rows = helper_read_all_rows(stmt);
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(3)],
            vec![Value::Integer(4)],
            vec![Value::Integer(5)],
            vec![Value::Integer(6)],
        ]
    );

    // Range: x < 4 (should include 1,2,3)
    let stmt = query_and_log(&conn, "SELECT x FROM t WHERE x < 4 ORDER BY x")
        .unwrap()
        .unwrap();
    let rows = helper_read_all_rows(stmt);
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(1)],
            vec![Value::Integer(2)],
            vec![Value::Integer(3)],
        ]
    );

    // Range: x >= 3 AND x <= 5 (should include 3,4,5)
    let stmt = query_and_log(&conn, "SELECT x FROM t WHERE x >= 3 AND x <= 5 ORDER BY x")
        .unwrap()
        .unwrap();
    let rows = helper_read_all_rows(stmt);
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(3)],
            vec![Value::Integer(4)],
            vec![Value::Integer(5)],
        ]
    );
}

#[test]
fn test_commit_without_mvcc() {
    let tmp_db = TempDatabase::new("test_commit_without_mvcc.db");
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
        .unwrap();

    conn.execute("BEGIN IMMEDIATE").unwrap();

    assert!(
        !conn.get_auto_commit(),
        "should not be in autocommit mode after BEGIN"
    );

    conn.execute("INSERT INTO test (id, value) VALUES (1, 'hello')")
        .unwrap();

    conn.execute("COMMIT")
        .expect("COMMIT should succeed for non-MVCC transactions");

    assert!(
        conn.get_auto_commit(),
        "should be back in autocommit mode after COMMIT"
    );

    let stmt = conn
        .query("SELECT value FROM test WHERE id = 1")
        .unwrap()
        .unwrap();
    let row = helper_read_single_row(stmt);
    assert_eq!(row[0], Value::Text("hello".into()));
}

#[test]
fn test_rollback_without_mvcc() {
    let tmp_db = TempDatabase::new("test_rollback_without_mvcc.db");
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
        .unwrap();

    conn.execute("INSERT INTO test (id, value) VALUES (1, 'initial')")
        .unwrap();

    conn.execute("BEGIN IMMEDIATE").unwrap();

    assert!(
        !conn.get_auto_commit(),
        "should not be in autocommit mode after BEGIN"
    );

    conn.execute("UPDATE test SET value = 'modified' WHERE id = 1")
        .unwrap();

    conn.execute("ROLLBACK")
        .expect("ROLLBACK should succeed for non-MVCC transactions");

    assert!(
        conn.get_auto_commit(),
        "should be back in autocommit mode after ROLLBACK"
    );

    let stmt = conn
        .query("SELECT value FROM test WHERE id = 1")
        .unwrap()
        .unwrap();
    let row = helper_read_single_row(stmt);
    assert_eq!(row[0], Value::Text("initial".into()));
}

#[test]
fn test_wal_savepoint_rollback_on_constraint_violation() {
    let tmp_db = TempDatabase::new("test_90969.db");
    let conn = tmp_db.connect_limbo();

    conn.execute("PRAGMA cache_size = 200").unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, u INTEGER UNIQUE, val TEXT)")
        .unwrap();

    let padding = "x".repeat(2000);
    conn.execute("BEGIN").unwrap();
    for i in 1..=1000 {
        conn.execute(format!("INSERT INTO t VALUES ({i}, {i}, '{padding}')"))
            .unwrap();
    }
    conn.execute("COMMIT").unwrap();

    conn.execute("BEGIN").unwrap();

    let result =
        conn.execute("UPDATE t SET val = 'modified', u = CASE WHEN id = 1000 THEN 1 ELSE u END");
    assert!(
        matches!(result, Err(LimboError::Constraint(_))),
        "Expected UNIQUE constraint violation, got: {result:?}"
    );

    let stmt = conn
        .query("SELECT val FROM t WHERE id = 1")
        .unwrap()
        .unwrap();
    let row = helper_read_single_row(stmt);
    let Value::Text(val) = &row[0] else {
        panic!("Expected text value");
    };
    assert_eq!(
        val.as_str(),
        &padding,
        "Row should have original value after failed UPDATE rollback"
    );

    conn.execute("INSERT INTO t VALUES (1001, 1001, 'new')")
        .unwrap();
    conn.execute("COMMIT").unwrap();

    let rusqlite_conn = rusqlite::Connection::open(tmp_db.path.clone()).unwrap();
    let result: String = rusqlite_conn
        .pragma_query_value(None, "integrity_check", |row| row.get(0))
        .unwrap();
    assert_eq!(
        result, "ok",
        "Database should pass integrity check after savepoint rollback"
    );

    let stmt = conn.query("SELECT COUNT(*) FROM t").unwrap().unwrap();
    let row = helper_read_single_row(stmt);
    assert_eq!(row[0], Value::Integer(1001));
}

#[turso_macros::test]
/// INSERT OR FAIL should keep changes made by the statement before the error.
/// Unlike ABORT (the default), FAIL does not roll back successful inserts within the same statement.
fn test_insert_or_fail_keeps_prior_changes(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'first'), (2, 'second')")
        .unwrap();

    // INSERT OR FAIL with multiple rows - (3, 'third') succeeds, then (1, 'conflict') fails
    let result = conn.execute("INSERT OR FAIL INTO t VALUES (3, 'third'), (1, 'conflict')");
    assert!(matches!(result, Err(LimboError::Constraint(_))));

    // Verify that row 3 was inserted (FAIL keeps prior changes)
    let stmt = conn.query("SELECT id FROM t ORDER BY id").unwrap().unwrap();
    let rows = helper_read_all_rows(stmt);
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(1)],
            vec![Value::Integer(2)],
            vec![Value::Integer(3)], // This row should exist due to FAIL semantics
        ]
    );
}

#[turso_macros::test]
/// INSERT OR ABORT (default) should rollback all changes from the statement on error.
fn test_insert_or_abort_rolls_back_statement(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'first'), (2, 'second')")
        .unwrap();

    // INSERT OR ABORT with multiple rows - (3, 'third') would succeed, but (1, 'conflict') fails
    // and rolls back the entire statement
    let result = conn.execute("INSERT OR ABORT INTO t VALUES (3, 'third'), (1, 'conflict')");
    assert!(matches!(result, Err(LimboError::Constraint(_))));

    // Verify that row 3 was NOT inserted (ABORT rolls back statement)
    let stmt = conn.query("SELECT id FROM t ORDER BY id").unwrap().unwrap();
    let rows = helper_read_all_rows(stmt);
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(1)],
            vec![Value::Integer(2)],
            // Row 3 should NOT be here due to ABORT semantics
        ]
    );
}

#[turso_macros::test]
/// INSERT OR ROLLBACK in a transaction should rollback the entire transaction on error.
fn test_insert_or_rollback_rolls_back_transaction(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'outside')").unwrap();

    // Start a transaction and insert a row
    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'inside')").unwrap();

    // INSERT OR ROLLBACK causes the entire transaction to roll back
    let result = conn.execute("INSERT OR ROLLBACK INTO t VALUES (1, 'conflict')");
    assert!(matches!(result, Err(LimboError::Constraint(_))));

    // Verify that row 2 was rolled back (ROLLBACK affects entire transaction)
    let stmt = conn.query("SELECT id FROM t ORDER BY id").unwrap().unwrap();
    let rows = helper_read_all_rows(stmt);
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(1)], // Only the original row should remain
        ]
    );

    // Verify we're back in autocommit mode
    assert!(conn.get_auto_commit());
}

#[turso_macros::test]
/// INSERT OR ROLLBACK with unique constraint in transaction should rollback the entire transaction.
fn test_insert_or_rollback_unique_constraint_in_transaction(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT UNIQUE)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'alice')").unwrap();

    // Start a transaction
    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'bob')").unwrap();

    // INSERT OR ROLLBACK with unique constraint violation
    let result = conn.execute("INSERT OR ROLLBACK INTO t VALUES (3, 'alice')");
    assert!(matches!(result, Err(LimboError::Constraint(_))));

    // Verify that row 2 (bob) was rolled back
    let stmt = conn
        .query("SELECT name FROM t ORDER BY id")
        .unwrap()
        .unwrap();
    let rows = helper_read_all_rows(stmt);
    assert_eq!(rows, vec![vec![Value::Text("alice".into())],]);

    // Verify we're back in autocommit mode
    assert!(conn.get_auto_commit());
}

#[turso_macros::test]
/// INSERT OR ROLLBACK in autocommit mode should behave like ABORT (single statement = transaction).
fn test_insert_or_rollback_in_autocommit(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'first')").unwrap();

    // INSERT OR ROLLBACK in autocommit mode
    let result = conn.execute("INSERT OR ROLLBACK INTO t VALUES (1, 'conflict')");
    assert!(matches!(result, Err(LimboError::Constraint(_))));

    // Verify the original row is still there
    let stmt = conn
        .query("SELECT val FROM t WHERE id = 1")
        .unwrap()
        .unwrap();
    let rows = helper_read_all_rows(stmt);
    assert_eq!(rows, vec![vec![Value::Text("first".into())],]);
}

#[turso_macros::test]
/// INSERT OR FAIL with unique constraint should keep successfully inserted rows.
fn test_insert_or_fail_unique_constraint(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT UNIQUE)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'alice'), (2, 'bob')")
        .unwrap();

    // INSERT OR FAIL - (3, 'charlie') succeeds, then (4, 'alice') fails
    let result = conn.execute("INSERT OR FAIL INTO t VALUES (3, 'charlie'), (4, 'alice')");
    assert!(matches!(result, Err(LimboError::Constraint(_))));

    // Verify charlie was inserted (FAIL keeps prior changes)
    let stmt = conn
        .query("SELECT name FROM t ORDER BY id")
        .unwrap()
        .unwrap();
    let rows = helper_read_all_rows(stmt);
    assert_eq!(
        rows,
        vec![
            vec![Value::Text("alice".into())],
            vec![Value::Text("bob".into())],
            vec![Value::Text("charlie".into())], // This should exist due to FAIL
        ]
    );
}

#[turso_macros::test]
/// UPDATE OR FAIL should keep changes made by the statement before the error.
/// Unlike ABORT (the default), FAIL does not roll back successful updates within the same statement.
fn test_update_or_fail_keeps_prior_changes(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER UNIQUE)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10), (2, 20)")
        .unwrap();

    // UPDATE OR FAIL - try to set val=20 which conflicts with id=2
    let result = conn.execute("UPDATE OR FAIL t SET val = 20 WHERE id = 1");
    assert!(matches!(result, Err(LimboError::Constraint(_))));

    // Verify original values (single row update, so nothing before the error to keep)
    let stmt = conn
        .query("SELECT id, val FROM t ORDER BY id")
        .unwrap()
        .unwrap();
    let rows = helper_read_all_rows(stmt);
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(1), Value::Integer(10)],
            vec![Value::Integer(2), Value::Integer(20)],
        ]
    );
}

#[turso_macros::test]
/// UPDATE OR ABORT (default) should rollback all changes from the statement on error.
fn test_update_or_abort_rolls_back_statement(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER UNIQUE)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10), (2, 20)")
        .unwrap();

    // UPDATE OR ABORT - try to set val=20 which conflicts with id=2
    let result = conn.execute("UPDATE OR ABORT t SET val = 20 WHERE id = 1");
    assert!(matches!(result, Err(LimboError::Constraint(_))));

    // Verify original values (ABORT rolls back statement)
    let stmt = conn
        .query("SELECT id, val FROM t ORDER BY id")
        .unwrap()
        .unwrap();
    let rows = helper_read_all_rows(stmt);
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(1), Value::Integer(10)],
            vec![Value::Integer(2), Value::Integer(20)],
        ]
    );
}

#[turso_macros::test]
/// UPDATE OR ROLLBACK in a transaction should rollback the entire transaction on error.
fn test_update_or_rollback_rolls_back_transaction(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER UNIQUE)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();

    // Start a transaction and make some changes
    conn.execute("BEGIN").unwrap();
    conn.execute("UPDATE t SET val = 100 WHERE id = 1").unwrap();

    // UPDATE OR ROLLBACK causes the entire transaction to roll back
    let result = conn.execute("UPDATE OR ROLLBACK t SET val = 30 WHERE id = 2");
    assert!(matches!(result, Err(LimboError::Constraint(_))));

    // Verify that all changes were rolled back (including the first UPDATE)
    let stmt = conn
        .query("SELECT id, val FROM t ORDER BY id")
        .unwrap()
        .unwrap();
    let rows = helper_read_all_rows(stmt);
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(1), Value::Integer(10)], // Rolled back
            vec![Value::Integer(2), Value::Integer(20)], // Unchanged
            vec![Value::Integer(3), Value::Integer(30)], // Unchanged
        ]
    );

    // Verify we're back in autocommit mode
    assert!(conn.get_auto_commit());
}

#[turso_macros::test]
/// UPDATE OR ROLLBACK in autocommit mode should work like ABORT.
fn test_update_or_rollback_in_autocommit(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER UNIQUE)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10), (2, 20)")
        .unwrap();

    // UPDATE OR ROLLBACK in autocommit mode
    let result = conn.execute("UPDATE OR ROLLBACK t SET val = 20 WHERE id = 1");
    assert!(matches!(result, Err(LimboError::Constraint(_))));

    // Verify original values
    let stmt = conn
        .query("SELECT id, val FROM t ORDER BY id")
        .unwrap()
        .unwrap();
    let rows = helper_read_all_rows(stmt);
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(1), Value::Integer(10)],
            vec![Value::Integer(2), Value::Integer(20)],
        ]
    );
}
