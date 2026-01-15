use rusqlite::types::Value;
use turso_core::types::ImmutableRecord;

use crate::common::{limbo_exec_rows, TempDatabase};

fn replace_column_with_null(rows: Vec<Vec<Value>>, column: usize) -> Vec<Vec<Value>> {
    rows.into_iter()
        .map(|row| {
            row.into_iter()
                .enumerate()
                .map(|(i, value)| if i == column { Value::Null } else { value })
                .collect()
        })
        .collect()
}

/// Helper to create a CDC INSERT record entry (change_type=1) in 'id' mode
fn insert_entry(change_id: i64, change_txn_id: i64, table_name: &str, id: i64) -> Vec<Value> {
    vec![
        Value::Integer(change_id),
        Value::Null,       // change_time (replaced with null for testing)
        Value::Integer(1), // change_type = INSERT
        Value::Integer(change_txn_id),
        Value::Text(table_name.to_string()),
        Value::Integer(id),
        Value::Null,
        Value::Null,
        Value::Null,
    ]
}

/// Helper to create a CDC INSERT record entry with 'after' blob
fn insert_entry_after(
    change_id: i64,
    change_txn_id: i64,
    table_name: &str,
    id: i64,
    after: Vec<u8>,
) -> Vec<Value> {
    vec![
        Value::Integer(change_id),
        Value::Null,       // change_time
        Value::Integer(1), // change_type = INSERT
        Value::Integer(change_txn_id),
        Value::Text(table_name.to_string()),
        Value::Integer(id),
        Value::Null,
        Value::Blob(after),
        Value::Null,
    ]
}

/// Helper to create a CDC UPDATE record entry (change_type=0) in 'id' mode
fn update_entry(change_id: i64, change_txn_id: i64, table_name: &str, id: i64) -> Vec<Value> {
    vec![
        Value::Integer(change_id),
        Value::Null,       // change_time
        Value::Integer(0), // change_type = UPDATE
        Value::Integer(change_txn_id),
        Value::Text(table_name.to_string()),
        Value::Integer(id),
        Value::Null,
        Value::Null,
        Value::Null,
    ]
}

/// Helper to create a CDC UPDATE record entry with 'before' blob
fn update_entry_before(
    change_id: i64,
    change_txn_id: i64,
    table_name: &str,
    id: i64,
    before: Vec<u8>,
) -> Vec<Value> {
    vec![
        Value::Integer(change_id),
        Value::Null,       // change_time
        Value::Integer(0), // change_type = UPDATE
        Value::Integer(change_txn_id),
        Value::Text(table_name.to_string()),
        Value::Integer(id),
        Value::Blob(before),
        Value::Null,
        Value::Null,
    ]
}

/// Helper to create a CDC UPDATE record entry with 'after' blob
fn update_entry_after(
    change_id: i64,
    change_txn_id: i64,
    table_name: &str,
    id: i64,
    after: Vec<u8>,
) -> Vec<Value> {
    vec![
        Value::Integer(change_id),
        Value::Null,       // change_time
        Value::Integer(0), // change_type = UPDATE
        Value::Integer(change_txn_id),
        Value::Text(table_name.to_string()),
        Value::Integer(id),
        Value::Null,
        Value::Blob(after),
        Value::Null,
    ]
}

/// Helper to create a CDC UPDATE record entry with 'before', 'after', and 'updates' blobs (full mode)
fn update_entry_full(
    change_id: i64,
    change_txn_id: i64,
    table_name: &str,
    id: i64,
    before: Vec<u8>,
    after: Vec<u8>,
    updates: Vec<u8>,
) -> Vec<Value> {
    vec![
        Value::Integer(change_id),
        Value::Null,       // change_time
        Value::Integer(0), // change_type = UPDATE
        Value::Integer(change_txn_id),
        Value::Text(table_name.to_string()),
        Value::Integer(id),
        Value::Blob(before),
        Value::Blob(after),
        Value::Blob(updates),
    ]
}

/// Helper to create a CDC DELETE record entry (change_type=-1) in 'id' mode
fn delete_entry(change_id: i64, change_txn_id: i64, table_name: &str, id: i64) -> Vec<Value> {
    vec![
        Value::Integer(change_id),
        Value::Null,        // change_time
        Value::Integer(-1), // change_type = DELETE
        Value::Integer(change_txn_id),
        Value::Text(table_name.to_string()),
        Value::Integer(id),
        Value::Null,
        Value::Null,
        Value::Null,
    ]
}

/// Helper to create a CDC DELETE record entry with 'before' blob
fn delete_entry_before(
    change_id: i64,
    change_txn_id: i64,
    table_name: &str,
    id: i64,
    before: Vec<u8>,
) -> Vec<Value> {
    vec![
        Value::Integer(change_id),
        Value::Null,        // change_time
        Value::Integer(-1), // change_type = DELETE
        Value::Integer(change_txn_id),
        Value::Text(table_name.to_string()),
        Value::Integer(id),
        Value::Blob(before),
        Value::Null,
        Value::Null,
    ]
}

/// Helper to create a CDC COMMIT record entry (change_type=2)
fn commit_entry(change_id: i64, change_txn_id: i64) -> Vec<Value> {
    vec![
        Value::Integer(change_id),
        Value::Null,       // change_time
        Value::Integer(2), // change_type = COMMIT
        Value::Integer(change_txn_id),
        Value::Null, // table_name
        Value::Null, // id
        Value::Null, // before
        Value::Null, // after
        Value::Null, // updates
    ]
}

/// Helper to create a CDC schema INSERT entry (for CREATE TABLE/INDEX)
fn schema_insert_entry(
    change_id: i64,
    change_txn_id: i64,
    rowid: i64,
    after: Vec<u8>,
) -> Vec<Value> {
    vec![
        Value::Integer(change_id),
        Value::Null,       // change_time
        Value::Integer(1), // change_type = INSERT
        Value::Integer(change_txn_id),
        Value::Text("sqlite_schema".to_string()),
        Value::Integer(rowid),
        Value::Null,
        Value::Blob(after),
        Value::Null,
    ]
}

/// Helper to create a CDC schema DELETE entry (for DROP TABLE/INDEX)
fn schema_delete_entry(
    change_id: i64,
    change_txn_id: i64,
    rowid: i64,
    before: Vec<u8>,
) -> Vec<Value> {
    vec![
        Value::Integer(change_id),
        Value::Null,        // change_time
        Value::Integer(-1), // change_type = DELETE
        Value::Integer(change_txn_id),
        Value::Text("sqlite_schema".to_string()),
        Value::Integer(rowid),
        Value::Blob(before),
        Value::Null,
        Value::Null,
    ]
}

/// Helper to create a CDC schema UPDATE entry (for ALTER TABLE)
fn schema_update_entry(
    change_id: i64,
    change_txn_id: i64,
    rowid: i64,
    before: Vec<u8>,
    after: Vec<u8>,
    updates: Vec<u8>,
) -> Vec<Value> {
    vec![
        Value::Integer(change_id),
        Value::Null,       // change_time
        Value::Integer(0), // change_type = UPDATE
        Value::Integer(change_txn_id),
        Value::Text("sqlite_schema".to_string()),
        Value::Integer(rowid),
        Value::Blob(before),
        Value::Blob(after),
        Value::Blob(updates),
    ]
}

#[turso_macros::test(mvcc)]
fn test_cdc_simple_id(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t (x INTEGER PRIMARY KEY, y)")
        .unwrap();
    conn.execute("PRAGMA unstable_capture_data_changes_conn('id')")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (10, 10), (5, 1)")
        .unwrap();
    let rows = limbo_exec_rows(&conn, "SELECT * FROM t");
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(5), Value::Integer(1)],
            vec![Value::Integer(10), Value::Integer(10)],
        ]
    );
    let rows = replace_column_with_null(limbo_exec_rows(&conn, "SELECT * FROM turso_cdc"), 1);
    assert_eq!(
        rows,
        vec![
            insert_entry(1, 1, "t", 10),
            insert_entry(2, 1, "t", 5),
            commit_entry(3, 1),
        ]
    );
}

fn record<const N: usize>(values: [Value; N]) -> Vec<u8> {
    let values = values
        .into_iter()
        .map(|x| match x {
            Value::Null => turso_core::Value::Null,
            Value::Integer(x) => turso_core::Value::Integer(x),
            Value::Real(x) => turso_core::Value::Float(x),
            Value::Text(x) => turso_core::Value::Text(turso_core::types::Text::new(x)),
            Value::Blob(x) => turso_core::Value::Blob(x),
        })
        .collect::<Vec<_>>();
    ImmutableRecord::from_values(&values, values.len())
        .get_payload()
        .to_vec()
}

#[turso_macros::test(mvcc)]
fn test_cdc_simple_before(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t (x INTEGER PRIMARY KEY, y)")
        .unwrap();
    conn.execute("PRAGMA unstable_capture_data_changes_conn('before')")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 2), (3, 4)").unwrap();
    conn.execute("UPDATE t SET y = 3 WHERE x = 1").unwrap();
    conn.execute("DELETE FROM t WHERE x = 3").unwrap();
    conn.execute("DELETE FROM t WHERE x = 1").unwrap();
    let rows = replace_column_with_null(limbo_exec_rows(&conn, "SELECT * FROM turso_cdc"), 1);

    assert_eq!(
        rows,
        vec![
            // INSERT INTO t VALUES (1, 2), (3, 4) - autocommit transaction with txn_id=1
            insert_entry(1, 1, "t", 1),
            insert_entry(2, 1, "t", 3),
            commit_entry(3, 1),
            // UPDATE t SET y = 3 WHERE x = 1 - autocommit transaction with txn_id=4
            update_entry_before(4, 4, "t", 1, record([Value::Integer(1), Value::Integer(2)])),
            commit_entry(5, 4),
            // DELETE FROM t WHERE x = 3 - autocommit transaction with txn_id=6
            delete_entry_before(6, 6, "t", 3, record([Value::Integer(3), Value::Integer(4)])),
            commit_entry(7, 6),
            // DELETE FROM t WHERE x = 1 - autocommit transaction with txn_id=8
            delete_entry_before(8, 8, "t", 1, record([Value::Integer(1), Value::Integer(3)])),
            commit_entry(9, 8),
        ]
    );
}

#[turso_macros::test(mvcc)]
fn test_cdc_simple_after(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t (x INTEGER PRIMARY KEY, y)")
        .unwrap();
    conn.execute("PRAGMA unstable_capture_data_changes_conn('after')")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 2), (3, 4)").unwrap();
    conn.execute("UPDATE t SET y = 3 WHERE x = 1").unwrap();
    conn.execute("DELETE FROM t WHERE x = 3").unwrap();
    conn.execute("DELETE FROM t WHERE x = 1").unwrap();
    let rows = replace_column_with_null(limbo_exec_rows(&conn, "SELECT * FROM turso_cdc"), 1);

    assert_eq!(
        rows,
        vec![
            insert_entry_after(1, 1, "t", 1, record([Value::Integer(1), Value::Integer(2)])),
            insert_entry_after(2, 1, "t", 3, record([Value::Integer(3), Value::Integer(4)])),
            commit_entry(3, 1),
            update_entry_after(4, 4, "t", 1, record([Value::Integer(1), Value::Integer(3)])),
            commit_entry(5, 4),
            delete_entry(6, 6, "t", 3),
            commit_entry(7, 6),
            delete_entry(8, 8, "t", 1),
            commit_entry(9, 8),
        ]
    );
}

#[turso_macros::test(mvcc)]
fn test_cdc_simple_full(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t (x INTEGER PRIMARY KEY, y)")
        .unwrap();
    conn.execute("PRAGMA unstable_capture_data_changes_conn('full')")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 2), (3, 4)").unwrap();
    conn.execute("UPDATE t SET y = 3 WHERE x = 1").unwrap();
    conn.execute("DELETE FROM t WHERE x = 3").unwrap();
    conn.execute("DELETE FROM t WHERE x = 1").unwrap();
    let rows = replace_column_with_null(limbo_exec_rows(&conn, "SELECT * FROM turso_cdc"), 1);

    assert_eq!(
        rows,
        vec![
            insert_entry_after(1, 1, "t", 1, record([Value::Integer(1), Value::Integer(2)])),
            insert_entry_after(2, 1, "t", 3, record([Value::Integer(3), Value::Integer(4)])),
            commit_entry(3, 1),
            update_entry_full(
                4,
                4,
                "t",
                1,
                record([Value::Integer(1), Value::Integer(2)]),
                record([Value::Integer(1), Value::Integer(3)]),
                record([
                    Value::Integer(0),
                    Value::Integer(1),
                    Value::Null,
                    Value::Integer(3)
                ]),
            ),
            commit_entry(5, 4),
            delete_entry_before(6, 6, "t", 3, record([Value::Integer(3), Value::Integer(4)])),
            commit_entry(7, 6),
            delete_entry_before(8, 8, "t", 1, record([Value::Integer(1), Value::Integer(3)])),
            commit_entry(9, 8),
        ]
    );
}

#[turso_macros::test(mvcc)]
fn test_cdc_crud(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t (x INTEGER PRIMARY KEY, y)")
        .unwrap();
    conn.execute("PRAGMA unstable_capture_data_changes_conn('id')")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (20, 20), (10, 10), (5, 1)")
        .unwrap();
    conn.execute("UPDATE t SET y = 100 WHERE x = 5").unwrap();
    conn.execute("DELETE FROM t WHERE x > 5").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 1)").unwrap();
    conn.execute("UPDATE t SET x = 2 WHERE x = 1").unwrap();

    let rows = limbo_exec_rows(&conn, "SELECT * FROM t");
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(2), Value::Integer(1)],
            vec![Value::Integer(5), Value::Integer(100)],
        ]
    );
    let rows = replace_column_with_null(limbo_exec_rows(&conn, "SELECT * FROM turso_cdc"), 1);
    assert_eq!(
        rows,
        vec![
            insert_entry(1, 1, "t", 20),
            insert_entry(2, 1, "t", 10),
            insert_entry(3, 1, "t", 5),
            commit_entry(4, 1),
            update_entry(5, 5, "t", 5),
            commit_entry(6, 5),
            delete_entry(7, 7, "t", 10),
            delete_entry(8, 7, "t", 20),
            commit_entry(9, 7),
            insert_entry(10, 10, "t", 1),
            commit_entry(11, 10),
            delete_entry(12, 12, "t", 1),
            insert_entry(13, 12, "t", 2),
            commit_entry(14, 12),
        ]
    );
}

// TODO: cannot use mvcc because of indexes
#[turso_macros::test()]
fn test_cdc_failed_op(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t (x INTEGER PRIMARY KEY, y UNIQUE)")
        .unwrap();
    conn.execute("PRAGMA unstable_capture_data_changes_conn('id')")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10), (2, 20)")
        .unwrap();
    assert!(conn
        .execute("INSERT INTO t VALUES (3, 30), (4, 40), (5, 10)")
        .is_err());
    conn.execute("INSERT INTO t VALUES (6, 60), (7, 70)")
        .unwrap();

    let rows = limbo_exec_rows(&conn, "SELECT * FROM t");
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(1), Value::Integer(10)],
            vec![Value::Integer(2), Value::Integer(20)],
            vec![Value::Integer(6), Value::Integer(60)],
            vec![Value::Integer(7), Value::Integer(70)],
        ]
    );
    let rows = replace_column_with_null(limbo_exec_rows(&conn, "SELECT * FROM turso_cdc"), 1);
    assert_eq!(
        rows,
        vec![
            insert_entry(1, 1, "t", 1),
            insert_entry(2, 1, "t", 2),
            commit_entry(3, 1),
            insert_entry(4, 4, "t", 6),
            insert_entry(5, 4, "t", 7),
            commit_entry(6, 4),
        ]
    );
}

// TODO: cannot use mvcc because of indexes
#[turso_macros::test()]
fn test_cdc_uncaptured_connection(db: TempDatabase) {
    let conn1 = db.connect_limbo();
    conn1
        .execute("CREATE TABLE t (x INTEGER PRIMARY KEY, y UNIQUE)")
        .unwrap();
    conn1.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    conn1
        .execute("PRAGMA unstable_capture_data_changes_conn('id')")
        .unwrap();
    conn1.execute("INSERT INTO t VALUES (2, 20)").unwrap(); // captured
    let conn2 = db.connect_limbo();
    conn2.execute("INSERT INTO t VALUES (3, 30)").unwrap();
    conn2
        .execute("PRAGMA unstable_capture_data_changes_conn('id')")
        .unwrap();
    conn2.execute("INSERT INTO t VALUES (4, 40)").unwrap(); // captured
    conn2
        .execute("PRAGMA unstable_capture_data_changes_conn('off')")
        .unwrap();
    conn2.execute("INSERT INTO t VALUES (5, 50)").unwrap();

    conn1.execute("INSERT INTO t VALUES (6, 60)").unwrap(); // captured
    conn1
        .execute("PRAGMA unstable_capture_data_changes_conn('off')")
        .unwrap();
    conn1.execute("INSERT INTO t VALUES (7, 70)").unwrap();

    let rows = limbo_exec_rows(&conn1, "SELECT * FROM t");
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(1), Value::Integer(10)],
            vec![Value::Integer(2), Value::Integer(20)],
            vec![Value::Integer(3), Value::Integer(30)],
            vec![Value::Integer(4), Value::Integer(40)],
            vec![Value::Integer(5), Value::Integer(50)],
            vec![Value::Integer(6), Value::Integer(60)],
            vec![Value::Integer(7), Value::Integer(70)],
        ]
    );
    let rows = replace_column_with_null(limbo_exec_rows(&conn1, "SELECT * FROM turso_cdc"), 1);
    assert_eq!(
        rows,
        vec![
            insert_entry(1, 1, "t", 2),
            commit_entry(2, 1),
            insert_entry(3, 3, "t", 4),
            commit_entry(4, 3),
            insert_entry(5, 5, "t", 6),
            commit_entry(6, 5),
        ]
    );
}

// TODO: cannot use mvcc because of indexes
#[turso_macros::test()]
fn test_cdc_custom_table(db: TempDatabase) {
    let conn1 = db.connect_limbo();
    conn1
        .execute("CREATE TABLE t (x INTEGER PRIMARY KEY, y UNIQUE)")
        .unwrap();
    conn1
        .execute("PRAGMA unstable_capture_data_changes_conn('id,custom_cdc')")
        .unwrap();
    conn1.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    conn1.execute("INSERT INTO t VALUES (2, 20)").unwrap();
    let rows = limbo_exec_rows(&conn1, "SELECT * FROM t");
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(1), Value::Integer(10)],
            vec![Value::Integer(2), Value::Integer(20)],
        ]
    );
    let rows = replace_column_with_null(limbo_exec_rows(&conn1, "SELECT * FROM custom_cdc"), 1);
    assert_eq!(
        rows,
        vec![
            insert_entry(1, 1, "t", 1),
            commit_entry(2, 1),
            insert_entry(3, 3, "t", 2),
            commit_entry(4, 3),
        ]
    );
}

// TODO: cannot use mvcc because of indexes
#[turso_macros::test()]
fn test_cdc_ignore_changes_in_cdc_table(db: TempDatabase) {
    let conn1 = db.connect_limbo();
    conn1
        .execute("CREATE TABLE t (x INTEGER PRIMARY KEY, y UNIQUE)")
        .unwrap();
    conn1
        .execute("PRAGMA unstable_capture_data_changes_conn('id,custom_cdc')")
        .unwrap();
    conn1.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    conn1.execute("INSERT INTO t VALUES (2, 20)").unwrap();
    let rows = limbo_exec_rows(&conn1, "SELECT * FROM t");
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(1), Value::Integer(10)],
            vec![Value::Integer(2), Value::Integer(20)],
        ]
    );
    conn1
        .execute("DELETE FROM custom_cdc WHERE change_id < 2")
        .unwrap();
    let rows = replace_column_with_null(limbo_exec_rows(&conn1, "SELECT * FROM custom_cdc"), 1);
    assert_eq!(
        rows,
        vec![
            commit_entry(2, 1),
            insert_entry(3, 3, "t", 2),
            commit_entry(4, 3),
            // DELETE FROM custom_cdc itself generates a COMMIT record with uninitialized txn_id
            commit_entry(5, -1),
        ]
    );
}

// TODO: cannot use mvcc because of indexes
#[turso_macros::test()]
fn test_cdc_transaction(db: TempDatabase) {
    let conn1 = db.connect_limbo();
    conn1
        .execute("CREATE TABLE t (x INTEGER PRIMARY KEY, y UNIQUE)")
        .unwrap();
    conn1
        .execute("CREATE TABLE q (x INTEGER PRIMARY KEY, y UNIQUE)")
        .unwrap();
    conn1
        .execute("PRAGMA unstable_capture_data_changes_conn('id,custom_cdc')")
        .unwrap();
    conn1.execute("BEGIN").unwrap();
    conn1.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    conn1.execute("INSERT INTO q VALUES (2, 20)").unwrap();
    conn1.execute("INSERT INTO t VALUES (3, 30)").unwrap();
    conn1.execute("DELETE FROM t WHERE x = 1").unwrap();
    conn1.execute("UPDATE q SET y = 200 WHERE x = 2").unwrap();
    conn1.execute("COMMIT").unwrap();
    let rows = limbo_exec_rows(&conn1, "SELECT * FROM t");
    assert_eq!(rows, vec![vec![Value::Integer(3), Value::Integer(30)],]);
    let rows = limbo_exec_rows(&conn1, "SELECT * FROM q");
    assert_eq!(rows, vec![vec![Value::Integer(2), Value::Integer(200)],]);
    let rows = replace_column_with_null(limbo_exec_rows(&conn1, "SELECT * FROM custom_cdc"), 1);
    assert_eq!(
        rows,
        vec![
            insert_entry(1, 1, "t", 1),
            insert_entry(2, 1, "q", 2),
            insert_entry(3, 1, "t", 3),
            delete_entry(4, 1, "t", 1),
            update_entry(5, 1, "q", 2),
            commit_entry(6, 1),
        ]
    );
}

// TODO: cannot use mvcc because of indexes
#[turso_macros::test()]
fn test_cdc_independent_connections(db: TempDatabase) {
    let conn1 = db.connect_limbo();
    let conn2 = db.connect_limbo();
    conn1
        .execute("CREATE TABLE t (x INTEGER PRIMARY KEY, y UNIQUE)")
        .unwrap();
    conn1
        .execute("PRAGMA unstable_capture_data_changes_conn('id,custom_cdc1')")
        .unwrap();
    conn2
        .execute("PRAGMA unstable_capture_data_changes_conn('id,custom_cdc2')")
        .unwrap();
    conn1.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    conn2.execute("INSERT INTO t VALUES (2, 20)").unwrap();
    let rows = limbo_exec_rows(&conn1, "SELECT * FROM t");
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(1), Value::Integer(10)],
            vec![Value::Integer(2), Value::Integer(20)]
        ]
    );
    let rows = replace_column_with_null(limbo_exec_rows(&conn1, "SELECT * FROM custom_cdc1"), 1);
    assert_eq!(rows, vec![insert_entry(1, 1, "t", 1), commit_entry(2, 1),]);
    let rows = replace_column_with_null(limbo_exec_rows(&conn1, "SELECT * FROM custom_cdc2"), 1);
    assert_eq!(rows, vec![insert_entry(1, 1, "t", 2), commit_entry(2, 1),]);
}

// TODO: cannot use mvcc because of indexes
#[turso_macros::test()]
fn test_cdc_independent_connections_different_cdc_not_ignore(db: TempDatabase) {
    let conn1 = db.connect_limbo();
    let conn2 = db.connect_limbo();
    conn1
        .execute("CREATE TABLE t (x INTEGER PRIMARY KEY, y UNIQUE)")
        .unwrap();
    conn1
        .execute("PRAGMA unstable_capture_data_changes_conn('id,custom_cdc1')")
        .unwrap();
    conn2
        .execute("PRAGMA unstable_capture_data_changes_conn('id,custom_cdc2')")
        .unwrap();
    conn1.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    conn1.execute("INSERT INTO t VALUES (2, 20)").unwrap();
    conn2.execute("INSERT INTO t VALUES (3, 30)").unwrap();
    conn2.execute("INSERT INTO t VALUES (4, 40)").unwrap();
    conn1
        .execute("DELETE FROM custom_cdc2 WHERE change_id < 2")
        .unwrap();
    conn2
        .execute("DELETE FROM custom_cdc1 WHERE change_id < 2")
        .unwrap();
    let rows = limbo_exec_rows(&conn1, "SELECT * FROM t");
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(1), Value::Integer(10)],
            vec![Value::Integer(2), Value::Integer(20)],
            vec![Value::Integer(3), Value::Integer(30)],
            vec![Value::Integer(4), Value::Integer(40)],
        ]
    );
    let rows = replace_column_with_null(limbo_exec_rows(&conn1, "SELECT * FROM custom_cdc1"), 1);
    assert_eq!(
        rows,
        vec![
            commit_entry(2, 1),
            insert_entry(3, 3, "t", 2),
            commit_entry(4, 3),
            delete_entry(5, 5, "custom_cdc2", 1),
            commit_entry(6, 5),
        ]
    );
    let rows = replace_column_with_null(limbo_exec_rows(&conn2, "SELECT * FROM custom_cdc2"), 1);
    assert_eq!(
        rows,
        vec![
            commit_entry(2, 1),
            insert_entry(3, 3, "t", 4),
            commit_entry(4, 3),
            delete_entry(5, 5, "custom_cdc1", 1),
            commit_entry(6, 5),
        ]
    );
}

// TODO: cannot use mvcc because of indexes
#[turso_macros::test()]
fn test_cdc_table_columns(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t (a INTEGER PRIMARY KEY, b, c UNIQUE)")
        .unwrap();
    let rows = limbo_exec_rows(&conn, "SELECT table_columns_json_array('t')");
    assert_eq!(
        rows,
        vec![vec![Value::Text(r#"["a","b","c"]"#.to_string())]]
    );
    conn.execute("ALTER TABLE t DROP COLUMN b").unwrap();
    let rows = limbo_exec_rows(&conn, "SELECT table_columns_json_array('t')");
    assert_eq!(rows, vec![vec![Value::Text(r#"["a","c"]"#.to_string())]]);
}

#[turso_macros::test(mvcc)]
fn test_cdc_bin_record(db: TempDatabase) {
    let conn = db.connect_limbo();
    let record = record([
        Value::Null,
        Value::Integer(1),
        // use golden ratio instead of pi because clippy has weird rule that I can't use PI approximation written by hand
        Value::Real(1.61803),
        Value::Text("hello".to_string()),
    ]);
    let mut record_hex = String::new();
    for byte in record {
        record_hex.push_str(&format!("{byte:02X}"));
    }

    let rows = limbo_exec_rows(
        &conn,
        &format!(r#"SELECT bin_record_json_object('["a","b","c","d"]', X'{record_hex}')"#),
    );
    assert_eq!(
        rows,
        vec![vec![Value::Text(
            r#"{"a":null,"b":1,"c":1.61803,"d":"hello"}"#.to_string()
        )]]
    );
}

// TODO: cannot use mvcc because of indexes
#[turso_macros::test()]
fn test_cdc_schema_changes(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("PRAGMA unstable_capture_data_changes_conn('full')")
        .unwrap();
    conn.execute("CREATE TABLE t(x, y, z UNIQUE, q, PRIMARY KEY (x, y))")
        .unwrap();
    conn.execute("CREATE TABLE q(a, b, c)").unwrap();
    conn.execute("CREATE INDEX t_q ON t(q)").unwrap();
    conn.execute("CREATE INDEX q_abc ON q(a, b, c)").unwrap();
    conn.execute("DROP TABLE t").unwrap();
    conn.execute("DROP INDEX q_abc").unwrap();
    let rows = replace_column_with_null(limbo_exec_rows(&conn, "SELECT * FROM turso_cdc"), 1);

    assert_eq!(
        rows,
        vec![
            // CREATE TABLE t
            schema_insert_entry(
                1,
                1,
                3,
                record([
                    Value::Text("table".to_string()),
                    Value::Text("t".to_string()),
                    Value::Text("t".to_string()),
                    Value::Integer(4),
                    Value::Text(
                        "CREATE TABLE t (x, y, z UNIQUE, q, PRIMARY KEY (x, y))".to_string()
                    )
                ])
            ),
            commit_entry(2, 1),
            // CREATE TABLE q
            schema_insert_entry(
                3,
                3,
                6,
                record([
                    Value::Text("table".to_string()),
                    Value::Text("q".to_string()),
                    Value::Text("q".to_string()),
                    Value::Integer(7),
                    Value::Text("CREATE TABLE q (a, b, c)".to_string())
                ])
            ),
            commit_entry(4, 3),
            // CREATE INDEX t_q
            schema_insert_entry(
                5,
                5,
                7,
                record([
                    Value::Text("index".to_string()),
                    Value::Text("t_q".to_string()),
                    Value::Text("t".to_string()),
                    Value::Integer(8),
                    Value::Text("CREATE INDEX t_q ON t (q)".to_string())
                ])
            ),
            commit_entry(6, 5),
            // CREATE INDEX q_abc
            schema_insert_entry(
                7,
                7,
                8,
                record([
                    Value::Text("index".to_string()),
                    Value::Text("q_abc".to_string()),
                    Value::Text("q".to_string()),
                    Value::Integer(9),
                    Value::Text("CREATE INDEX q_abc ON q (a, b, c)".to_string())
                ])
            ),
            commit_entry(8, 7),
            // DROP TABLE t
            schema_delete_entry(
                9,
                9,
                3,
                record([
                    Value::Text("table".to_string()),
                    Value::Text("t".to_string()),
                    Value::Text("t".to_string()),
                    Value::Integer(4),
                    Value::Text(
                        "CREATE TABLE t (x, y, z UNIQUE, q, PRIMARY KEY (x, y))".to_string()
                    )
                ])
            ),
            commit_entry(10, 9),
            // DROP INDEX q_abc
            schema_delete_entry(
                11,
                11,
                8,
                record([
                    Value::Text("index".to_string()),
                    Value::Text("q_abc".to_string()),
                    Value::Text("q".to_string()),
                    Value::Integer(9),
                    Value::Text("CREATE INDEX q_abc ON q (a, b, c)".to_string())
                ])
            ),
            commit_entry(12, 11),
        ]
    );
}

// TODO: cannot use mvcc because of indexes
#[turso_macros::test()]
fn test_cdc_schema_changes_alter_table(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("PRAGMA unstable_capture_data_changes_conn('full')")
        .unwrap();
    conn.execute("CREATE TABLE t(x, y, z UNIQUE, q, PRIMARY KEY (x, y))")
        .unwrap();
    conn.execute("ALTER TABLE t DROP COLUMN q").unwrap();
    conn.execute("ALTER TABLE t ADD COLUMN t").unwrap();
    let rows = replace_column_with_null(limbo_exec_rows(&conn, "SELECT * FROM turso_cdc"), 1);

    assert_eq!(
        rows,
        vec![
            // CREATE TABLE t
            schema_insert_entry(
                1,
                1,
                3,
                record([
                    Value::Text("table".to_string()),
                    Value::Text("t".to_string()),
                    Value::Text("t".to_string()),
                    Value::Integer(4),
                    Value::Text(
                        "CREATE TABLE t (x, y, z UNIQUE, q, PRIMARY KEY (x, y))".to_string()
                    )
                ])
            ),
            commit_entry(2, 1),
            // ALTER TABLE t DROP COLUMN q
            schema_update_entry(
                3,
                3,
                3,
                record([
                    Value::Text("table".to_string()),
                    Value::Text("t".to_string()),
                    Value::Text("t".to_string()),
                    Value::Integer(4),
                    Value::Text(
                        "CREATE TABLE t (x, y, z UNIQUE, q, PRIMARY KEY (x, y))".to_string()
                    )
                ]),
                record([
                    Value::Text("table".to_string()),
                    Value::Text("t".to_string()),
                    Value::Text("t".to_string()),
                    Value::Integer(4),
                    Value::Text("CREATE TABLE t (x, y, z UNIQUE, PRIMARY KEY (x, y))".to_string())
                ]),
                record([
                    Value::Integer(0),
                    Value::Integer(0),
                    Value::Integer(0),
                    Value::Integer(0),
                    Value::Integer(1),
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Text("ALTER TABLE t DROP COLUMN q".to_string())
                ]),
            ),
            commit_entry(4, 3),
            // ALTER TABLE t ADD COLUMN t
            schema_update_entry(
                5,
                5,
                3,
                record([
                    Value::Text("table".to_string()),
                    Value::Text("t".to_string()),
                    Value::Text("t".to_string()),
                    Value::Integer(4),
                    Value::Text("CREATE TABLE t (x, y, z UNIQUE, PRIMARY KEY (x, y))".to_string())
                ]),
                record([
                    Value::Text("table".to_string()),
                    Value::Text("t".to_string()),
                    Value::Text("t".to_string()),
                    Value::Integer(4),
                    Value::Text(
                        "CREATE TABLE t (x, y, z UNIQUE, t, PRIMARY KEY (x, y))".to_string()
                    )
                ]),
                record([
                    Value::Integer(0),
                    Value::Integer(0),
                    Value::Integer(0),
                    Value::Integer(0),
                    Value::Integer(1),
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Text("ALTER TABLE t ADD COLUMN t".to_string())
                ]),
            ),
            commit_entry(6, 5),
        ]
    );
}
