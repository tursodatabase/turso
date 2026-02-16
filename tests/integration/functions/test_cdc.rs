use std::sync::Arc;

use rusqlite::types::Value;
use turso_core::types::ImmutableRecord;
use turso_core::CDC_VERSION_CURRENT;

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

/// Normalize v2 CDC rows by replacing change_id (0), change_time (1), and change_txn_id (2)
/// with Null. This makes assertions work for both MVCC and non-MVCC modes.
fn normalize_cdc_v2_rows(rows: Vec<Vec<Value>>) -> Vec<Vec<Value>> {
    rows.into_iter()
        .map(|row| {
            row.into_iter()
                .enumerate()
                .map(|(i, value)| if i <= 2 { Value::Null } else { value })
                .collect()
        })
        .collect()
}

/// A normalized v2 COMMIT row: change_type=2, everything else Null.
fn v2_commit() -> Vec<Value> {
    vec![
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Integer(2),
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
    ]
}

/// Build a normalized v2 data row (change_id/time/txn_id replaced with Null).
fn v2_row(
    change_type: i64,
    table: &str,
    id: i64,
    before: Option<Vec<u8>>,
    after: Option<Vec<u8>>,
    updates: Option<Vec<u8>>,
) -> Vec<Value> {
    vec![
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Integer(change_type),
        Value::Text(table.to_string()),
        Value::Integer(id),
        before.map_or(Value::Null, Value::Blob),
        after.map_or(Value::Null, Value::Blob),
        updates.map_or(Value::Null, Value::Blob),
    ]
}

#[turso_macros::test]
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
    let rows = normalize_cdc_v2_rows(limbo_exec_rows(&conn, "SELECT * FROM turso_cdc"));
    assert_eq!(
        rows,
        vec![
            v2_row(1, "t", 10, None, None, None),
            v2_row(1, "t", 5, None, None, None),
            v2_commit(),
        ]
    );
}

fn record<const N: usize>(values: [Value; N]) -> Vec<u8> {
    let values = values
        .into_iter()
        .map(|x| match x {
            Value::Null => turso_core::Value::Null,
            Value::Integer(x) => turso_core::Value::from_i64(x),
            Value::Real(x) => turso_core::Value::from_f64(x),
            Value::Text(x) => turso_core::Value::Text(turso_core::types::Text::new(x)),
            Value::Blob(x) => turso_core::Value::Blob(x),
        })
        .collect::<Vec<_>>();
    ImmutableRecord::from_values(&values, values.len())
        .get_payload()
        .to_vec()
}

#[turso_macros::test]
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
    let rows = normalize_cdc_v2_rows(limbo_exec_rows(&conn, "SELECT * FROM turso_cdc"));

    assert_eq!(
        rows,
        vec![
            v2_row(1, "t", 1, None, None, None),
            v2_row(1, "t", 3, None, None, None),
            v2_commit(),
            v2_row(
                0,
                "t",
                1,
                Some(record([Value::Integer(1), Value::Integer(2)])),
                None,
                None,
            ),
            v2_commit(),
            v2_row(
                -1,
                "t",
                3,
                Some(record([Value::Integer(3), Value::Integer(4)])),
                None,
                None,
            ),
            v2_commit(),
            v2_row(
                -1,
                "t",
                1,
                Some(record([Value::Integer(1), Value::Integer(3)])),
                None,
                None,
            ),
            v2_commit(),
        ]
    );
}

#[turso_macros::test]
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
    let rows = normalize_cdc_v2_rows(limbo_exec_rows(&conn, "SELECT * FROM turso_cdc"));

    assert_eq!(
        rows,
        vec![
            v2_row(
                1,
                "t",
                1,
                None,
                Some(record([Value::Integer(1), Value::Integer(2)])),
                None,
            ),
            v2_row(
                1,
                "t",
                3,
                None,
                Some(record([Value::Integer(3), Value::Integer(4)])),
                None,
            ),
            v2_commit(),
            v2_row(
                0,
                "t",
                1,
                None,
                Some(record([Value::Integer(1), Value::Integer(3)])),
                None,
            ),
            v2_commit(),
            v2_row(-1, "t", 3, None, None, None),
            v2_commit(),
            v2_row(-1, "t", 1, None, None, None),
            v2_commit(),
        ]
    );
}

#[turso_macros::test]
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
    let rows = normalize_cdc_v2_rows(limbo_exec_rows(&conn, "SELECT * FROM turso_cdc"));

    assert_eq!(
        rows,
        vec![
            v2_row(
                1,
                "t",
                1,
                None,
                Some(record([Value::Integer(1), Value::Integer(2)])),
                None,
            ),
            v2_row(
                1,
                "t",
                3,
                None,
                Some(record([Value::Integer(3), Value::Integer(4)])),
                None,
            ),
            v2_commit(),
            v2_row(
                0,
                "t",
                1,
                Some(record([Value::Integer(1), Value::Integer(2)])),
                Some(record([Value::Integer(1), Value::Integer(3)])),
                Some(record([
                    Value::Integer(0),
                    Value::Integer(1),
                    Value::Null,
                    Value::Integer(3)
                ])),
            ),
            v2_commit(),
            v2_row(
                -1,
                "t",
                3,
                Some(record([Value::Integer(3), Value::Integer(4)])),
                None,
                None,
            ),
            v2_commit(),
            v2_row(
                -1,
                "t",
                1,
                Some(record([Value::Integer(1), Value::Integer(3)])),
                None,
                None,
            ),
            v2_commit(),
        ]
    );
}

#[turso_macros::test]
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
    let rows = normalize_cdc_v2_rows(limbo_exec_rows(&conn, "SELECT * FROM turso_cdc"));
    assert_eq!(
        rows,
        vec![
            // INSERT INTO t VALUES (20, 20), (10, 10), (5, 1)
            v2_row(1, "t", 20, None, None, None),
            v2_row(1, "t", 10, None, None, None),
            v2_row(1, "t", 5, None, None, None),
            v2_commit(),
            // UPDATE t SET y = 100 WHERE x = 5
            v2_row(0, "t", 5, None, None, None),
            v2_commit(),
            // DELETE FROM t WHERE x > 5
            v2_row(-1, "t", 10, None, None, None),
            v2_row(-1, "t", 20, None, None, None),
            v2_commit(),
            // INSERT INTO t VALUES (1, 1)
            v2_row(1, "t", 1, None, None, None),
            v2_commit(),
            // UPDATE t SET x = 2 WHERE x = 1 (rowid change: DELETE + INSERT + COMMIT)
            v2_row(-1, "t", 1, None, None, None),
            v2_row(1, "t", 2, None, None, None),
            v2_commit(),
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
    let rows = normalize_cdc_v2_rows(limbo_exec_rows(&conn, "SELECT * FROM turso_cdc"));
    assert_eq!(
        rows,
        vec![
            // INSERT INTO t VALUES (1, 10), (2, 20) — succeeds
            v2_row(1, "t", 1, None, None, None),
            v2_row(1, "t", 2, None, None, None),
            v2_commit(),
            // INSERT INTO t VALUES (3, 30), (4, 40), (5, 10) — fails, no CDC
            // INSERT INTO t VALUES (6, 60), (7, 70) — succeeds
            v2_row(1, "t", 6, None, None, None),
            v2_row(1, "t", 7, None, None, None),
            v2_commit(),
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
    let rows = normalize_cdc_v2_rows(limbo_exec_rows(&conn1, "SELECT * FROM turso_cdc"));
    assert_eq!(
        rows,
        vec![
            v2_row(1, "t", 2, None, None, None),
            v2_commit(),
            v2_row(1, "t", 4, None, None, None),
            v2_commit(),
            v2_row(1, "t", 6, None, None, None),
            v2_commit(),
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
    let rows = normalize_cdc_v2_rows(limbo_exec_rows(&conn1, "SELECT * FROM custom_cdc"));
    assert_eq!(
        rows,
        vec![
            v2_row(1, "t", 1, None, None, None),
            v2_commit(),
            v2_row(1, "t", 2, None, None, None),
            v2_commit(),
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
    let rows = normalize_cdc_v2_rows(limbo_exec_rows(&conn1, "SELECT * FROM custom_cdc"));
    assert_eq!(
        rows,
        vec![
            // COMMIT from first INSERT (change_id=2 survived the delete)
            v2_commit(),
            // INSERT (2, 20) + COMMIT
            v2_row(1, "t", 2, None, None, None),
            v2_commit(),
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
    let rows = normalize_cdc_v2_rows(limbo_exec_rows(&conn1, "SELECT * FROM custom_cdc"));
    assert_eq!(
        rows,
        vec![
            // Explicit transaction: no per-row COMMITs, one COMMIT at end
            v2_row(1, "t", 1, None, None, None),
            v2_row(1, "q", 2, None, None, None),
            v2_row(1, "t", 3, None, None, None),
            v2_row(-1, "t", 1, None, None, None),
            v2_row(0, "q", 2, None, None, None),
            v2_commit(),
        ]
    );
}

// TODO: cannot use mvcc because of indexes
#[turso_macros::test()]
fn test_cdc_ddl_in_explicit_transaction(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("PRAGMA unstable_capture_data_changes_conn('id')")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    conn.execute("CREATE TABLE t (x INTEGER PRIMARY KEY, y)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 20)").unwrap();
    conn.execute("CREATE TABLE q (a INTEGER PRIMARY KEY, b)")
        .unwrap();
    conn.execute("COMMIT").unwrap();

    let rows = normalize_cdc_v2_rows(limbo_exec_rows(&conn, "SELECT * FROM turso_cdc"));
    assert_eq!(
        rows,
        vec![
            // All DDL and DML in explicit transaction: no per-statement COMMITs
            v2_row(1, "sqlite_schema", 5, None, None, None), // CREATE TABLE t
            v2_row(1, "t", 1, None, None, None),             // INSERT (1, 10)
            v2_row(1, "t", 2, None, None, None),             // INSERT (2, 20)
            v2_row(1, "sqlite_schema", 6, None, None, None), // CREATE TABLE q
            v2_commit(),                                     // single COMMIT at end
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
    let rows = normalize_cdc_v2_rows(limbo_exec_rows(&conn1, "SELECT * FROM custom_cdc1"));
    assert_eq!(
        rows,
        vec![v2_row(1, "t", 1, None, None, None), v2_commit(),]
    );
    let rows = normalize_cdc_v2_rows(limbo_exec_rows(&conn1, "SELECT * FROM custom_cdc2"));
    assert_eq!(
        rows,
        vec![v2_row(1, "t", 2, None, None, None), v2_commit(),]
    );
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
    let rows = normalize_cdc_v2_rows(limbo_exec_rows(&conn1, "SELECT * FROM custom_cdc1"));
    assert_eq!(
        rows,
        vec![
            // COMMIT from first INSERT (change_id=1 was deleted)
            v2_commit(),
            // INSERT (2, 20) + COMMIT
            v2_row(1, "t", 2, None, None, None),
            v2_commit(),
            // DELETE from custom_cdc2 + COMMIT
            v2_row(-1, "custom_cdc2", 1, None, None, None),
            v2_commit(),
        ]
    );
    let rows = normalize_cdc_v2_rows(limbo_exec_rows(&conn2, "SELECT * FROM custom_cdc2"));
    assert_eq!(
        rows,
        vec![
            // COMMIT from first INSERT (change_id=1 was deleted)
            v2_commit(),
            // INSERT (4, 40) + COMMIT
            v2_row(1, "t", 4, None, None, None),
            v2_commit(),
            // DELETE from custom_cdc1 + COMMIT
            v2_row(-1, "custom_cdc1", 1, None, None, None),
            v2_commit(),
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

#[turso_macros::test]
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
    let rows = normalize_cdc_v2_rows(limbo_exec_rows(&conn, "SELECT * FROM turso_cdc"));

    // turso_cdc_version table + its auto-index add 2 entries to sqlite_schema,
    // shifting rowids and rootpages by +2 compared to before version tracking
    assert_eq!(
        rows,
        vec![
            // CREATE TABLE t
            v2_row(
                1,
                "sqlite_schema",
                5,
                None,
                Some(record([
                    Value::Text("table".to_string()),
                    Value::Text("t".to_string()),
                    Value::Text("t".to_string()),
                    Value::Integer(6),
                    Value::Text(
                        "CREATE TABLE t (x, y, z UNIQUE, q, PRIMARY KEY (x, y))".to_string()
                    )
                ])),
                None,
            ),
            v2_commit(),
            // CREATE TABLE q
            v2_row(
                1,
                "sqlite_schema",
                8,
                None,
                Some(record([
                    Value::Text("table".to_string()),
                    Value::Text("q".to_string()),
                    Value::Text("q".to_string()),
                    Value::Integer(9),
                    Value::Text("CREATE TABLE q (a, b, c)".to_string())
                ])),
                None,
            ),
            v2_commit(),
            // CREATE INDEX t_q
            v2_row(
                1,
                "sqlite_schema",
                9,
                None,
                Some(record([
                    Value::Text("index".to_string()),
                    Value::Text("t_q".to_string()),
                    Value::Text("t".to_string()),
                    Value::Integer(10),
                    Value::Text("CREATE INDEX t_q ON t (q)".to_string())
                ])),
                None,
            ),
            v2_commit(),
            // CREATE INDEX q_abc
            v2_row(
                1,
                "sqlite_schema",
                10,
                None,
                Some(record([
                    Value::Text("index".to_string()),
                    Value::Text("q_abc".to_string()),
                    Value::Text("q".to_string()),
                    Value::Integer(11),
                    Value::Text("CREATE INDEX q_abc ON q (a, b, c)".to_string())
                ])),
                None,
            ),
            v2_commit(),
            // DROP TABLE t
            v2_row(
                -1,
                "sqlite_schema",
                5,
                Some(record([
                    Value::Text("table".to_string()),
                    Value::Text("t".to_string()),
                    Value::Text("t".to_string()),
                    Value::Integer(6),
                    Value::Text(
                        "CREATE TABLE t (x, y, z UNIQUE, q, PRIMARY KEY (x, y))".to_string()
                    )
                ])),
                None,
                None,
            ),
            v2_commit(),
            // DROP INDEX q_abc
            v2_row(
                -1,
                "sqlite_schema",
                10,
                Some(record([
                    Value::Text("index".to_string()),
                    Value::Text("q_abc".to_string()),
                    Value::Text("q".to_string()),
                    Value::Integer(11),
                    Value::Text("CREATE INDEX q_abc ON q (a, b, c)".to_string())
                ])),
                None,
                None,
            ),
            v2_commit(),
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
    let rows = normalize_cdc_v2_rows(limbo_exec_rows(&conn, "SELECT * FROM turso_cdc"));

    // turso_cdc_version table + its auto-index add 2 entries to sqlite_schema,
    // shifting rowids and rootpages by +2 compared to before version tracking
    assert_eq!(
        rows,
        vec![
            // CREATE TABLE t
            v2_row(
                1,
                "sqlite_schema",
                5,
                None,
                Some(record([
                    Value::Text("table".to_string()),
                    Value::Text("t".to_string()),
                    Value::Text("t".to_string()),
                    Value::Integer(6),
                    Value::Text(
                        "CREATE TABLE t (x, y, z UNIQUE, q, PRIMARY KEY (x, y))".to_string()
                    )
                ])),
                None,
            ),
            v2_commit(),
            // ALTER TABLE t DROP COLUMN q
            v2_row(
                0,
                "sqlite_schema",
                5,
                Some(record([
                    Value::Text("table".to_string()),
                    Value::Text("t".to_string()),
                    Value::Text("t".to_string()),
                    Value::Integer(6),
                    Value::Text(
                        "CREATE TABLE t (x, y, z UNIQUE, q, PRIMARY KEY (x, y))".to_string()
                    )
                ])),
                Some(record([
                    Value::Text("table".to_string()),
                    Value::Text("t".to_string()),
                    Value::Text("t".to_string()),
                    Value::Integer(6),
                    Value::Text("CREATE TABLE t (x, y, z UNIQUE, PRIMARY KEY (x, y))".to_string())
                ])),
                Some(record([
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
                ])),
            ),
            v2_commit(),
            // ALTER TABLE t ADD COLUMN t
            v2_row(
                0,
                "sqlite_schema",
                5,
                Some(record([
                    Value::Text("table".to_string()),
                    Value::Text("t".to_string()),
                    Value::Text("t".to_string()),
                    Value::Integer(6),
                    Value::Text("CREATE TABLE t (x, y, z UNIQUE, PRIMARY KEY (x, y))".to_string())
                ])),
                Some(record([
                    Value::Text("table".to_string()),
                    Value::Text("t".to_string()),
                    Value::Text("t".to_string()),
                    Value::Integer(6),
                    Value::Text(
                        "CREATE TABLE t (x, y, z UNIQUE, t, PRIMARY KEY (x, y))".to_string()
                    )
                ])),
                Some(record([
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
                ])),
            ),
            v2_commit(),
        ]
    );
}

#[turso_macros::test]
fn test_cdc_version_table_created(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t (x INTEGER PRIMARY KEY, y)")
        .unwrap();
    conn.execute("PRAGMA unstable_capture_data_changes_conn('full')")
        .unwrap();
    let rows = limbo_exec_rows(&conn, "SELECT table_name, version FROM turso_cdc_version");
    assert_eq!(
        rows,
        vec![vec![
            Value::Text("turso_cdc".to_string()),
            Value::Text(CDC_VERSION_CURRENT.to_string()),
        ]]
    );
}

// TODO: cannot use mvcc because of indexes
#[turso_macros::test()]
fn test_cdc_version_custom_table(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t (x INTEGER PRIMARY KEY, y)")
        .unwrap();
    conn.execute("PRAGMA unstable_capture_data_changes_conn('id,my_cdc')")
        .unwrap();
    let rows = limbo_exec_rows(&conn, "SELECT table_name, version FROM turso_cdc_version");
    assert_eq!(
        rows,
        vec![vec![
            Value::Text("my_cdc".to_string()),
            Value::Text(CDC_VERSION_CURRENT.to_string()),
        ]]
    );
}

#[turso_macros::test]
fn test_cdc_version_not_created_when_exists(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t (x INTEGER PRIMARY KEY, y)")
        .unwrap();
    // First enable creates turso_cdc table and version entry
    conn.execute("PRAGMA unstable_capture_data_changes_conn('full')")
        .unwrap();
    // Disable CDC
    conn.execute("PRAGMA unstable_capture_data_changes_conn('off')")
        .unwrap();
    // Re-enable CDC — turso_cdc table already exists, so no duplicate version row
    conn.execute("PRAGMA unstable_capture_data_changes_conn('full')")
        .unwrap();
    let rows = limbo_exec_rows(&conn, "SELECT table_name, version FROM turso_cdc_version");
    assert_eq!(
        rows,
        vec![vec![
            Value::Text("turso_cdc".to_string()),
            Value::Text(CDC_VERSION_CURRENT.to_string()),
        ]]
    );
}

/// Helper: set up a database with pre-existing v1 CDC table and version table,
/// then enable CDC with the given mode and perform insert/update/delete operations.
fn setup_backward_compat_v1(db: &TempDatabase, mode: &str) -> Arc<turso_core::Connection> {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t (x INTEGER PRIMARY KEY, y)")
        .unwrap();

    // Simulate a database that already has the v1 CDC table and version table
    // (as if created by a previous version of the code)
    conn.execute(
        "CREATE TABLE turso_cdc (change_id INTEGER PRIMARY KEY AUTOINCREMENT, change_time INTEGER, change_type INTEGER, table_name TEXT, id, before BLOB, after BLOB, updates BLOB)",
    ).unwrap();
    conn.execute(
        "CREATE TABLE turso_cdc_version (table_name TEXT PRIMARY KEY, version TEXT NOT NULL)",
    )
    .unwrap();
    conn.execute("INSERT INTO turso_cdc_version (table_name, version) VALUES ('turso_cdc', 'v1')")
        .unwrap();

    // Enable CDC — table already exists, InitCdcVersion reads version from table
    conn.execute(format!(
        "PRAGMA unstable_capture_data_changes_conn('{mode}')"
    ))
    .unwrap();

    // Verify version table is unchanged
    let rows = limbo_exec_rows(&conn, "SELECT table_name, version FROM turso_cdc_version");
    assert_eq!(
        rows,
        vec![vec![
            Value::Text("turso_cdc".to_string()),
            Value::Text("v1".to_string()),
        ]]
    );

    // Perform insert, update, delete
    conn.execute("INSERT INTO t VALUES (1, 2), (3, 4)").unwrap();
    conn.execute("UPDATE t SET y = 3 WHERE x = 1").unwrap();
    conn.execute("DELETE FROM t WHERE x = 3").unwrap();
    conn.execute("DELETE FROM t WHERE x = 1").unwrap();

    conn
}

#[turso_macros::test]
fn test_cdc_version_backward_compat_v1_id(db: TempDatabase) {
    let conn = setup_backward_compat_v1(&db, "id");
    let rows = replace_column_with_null(limbo_exec_rows(&conn, "SELECT * FROM turso_cdc"), 1);
    assert_eq!(
        rows,
        vec![
            vec![
                Value::Integer(1),
                Value::Null,
                Value::Integer(1),
                Value::Text("t".to_string()),
                Value::Integer(1),
                Value::Null,
                Value::Null,
                Value::Null,
            ],
            vec![
                Value::Integer(2),
                Value::Null,
                Value::Integer(1),
                Value::Text("t".to_string()),
                Value::Integer(3),
                Value::Null,
                Value::Null,
                Value::Null,
            ],
            vec![
                Value::Integer(3),
                Value::Null,
                Value::Integer(0),
                Value::Text("t".to_string()),
                Value::Integer(1),
                Value::Null,
                Value::Null,
                Value::Null,
            ],
            vec![
                Value::Integer(4),
                Value::Null,
                Value::Integer(-1),
                Value::Text("t".to_string()),
                Value::Integer(3),
                Value::Null,
                Value::Null,
                Value::Null,
            ],
            vec![
                Value::Integer(5),
                Value::Null,
                Value::Integer(-1),
                Value::Text("t".to_string()),
                Value::Integer(1),
                Value::Null,
                Value::Null,
                Value::Null,
            ],
        ]
    );
}

#[turso_macros::test]
fn test_cdc_version_backward_compat_v1_before(db: TempDatabase) {
    let conn = setup_backward_compat_v1(&db, "before");
    let rows = replace_column_with_null(limbo_exec_rows(&conn, "SELECT * FROM turso_cdc"), 1);
    assert_eq!(
        rows,
        vec![
            vec![
                Value::Integer(1),
                Value::Null,
                Value::Integer(1),
                Value::Text("t".to_string()),
                Value::Integer(1),
                Value::Null,
                Value::Null,
                Value::Null,
            ],
            vec![
                Value::Integer(2),
                Value::Null,
                Value::Integer(1),
                Value::Text("t".to_string()),
                Value::Integer(3),
                Value::Null,
                Value::Null,
                Value::Null,
            ],
            vec![
                Value::Integer(3),
                Value::Null,
                Value::Integer(0),
                Value::Text("t".to_string()),
                Value::Integer(1),
                Value::Blob(record([Value::Integer(1), Value::Integer(2)])),
                Value::Null,
                Value::Null,
            ],
            vec![
                Value::Integer(4),
                Value::Null,
                Value::Integer(-1),
                Value::Text("t".to_string()),
                Value::Integer(3),
                Value::Blob(record([Value::Integer(3), Value::Integer(4)])),
                Value::Null,
                Value::Null,
            ],
            vec![
                Value::Integer(5),
                Value::Null,
                Value::Integer(-1),
                Value::Text("t".to_string()),
                Value::Integer(1),
                Value::Blob(record([Value::Integer(1), Value::Integer(3)])),
                Value::Null,
                Value::Null,
            ]
        ]
    );
}

#[turso_macros::test]
fn test_cdc_version_backward_compat_v1_after(db: TempDatabase) {
    let conn = setup_backward_compat_v1(&db, "after");
    let rows = replace_column_with_null(limbo_exec_rows(&conn, "SELECT * FROM turso_cdc"), 1);
    assert_eq!(
        rows,
        vec![
            vec![
                Value::Integer(1),
                Value::Null,
                Value::Integer(1),
                Value::Text("t".to_string()),
                Value::Integer(1),
                Value::Null,
                Value::Blob(record([Value::Integer(1), Value::Integer(2)])),
                Value::Null,
            ],
            vec![
                Value::Integer(2),
                Value::Null,
                Value::Integer(1),
                Value::Text("t".to_string()),
                Value::Integer(3),
                Value::Null,
                Value::Blob(record([Value::Integer(3), Value::Integer(4)])),
                Value::Null,
            ],
            vec![
                Value::Integer(3),
                Value::Null,
                Value::Integer(0),
                Value::Text("t".to_string()),
                Value::Integer(1),
                Value::Null,
                Value::Blob(record([Value::Integer(1), Value::Integer(3)])),
                Value::Null,
            ],
            vec![
                Value::Integer(4),
                Value::Null,
                Value::Integer(-1),
                Value::Text("t".to_string()),
                Value::Integer(3),
                Value::Null,
                Value::Null,
                Value::Null,
            ],
            vec![
                Value::Integer(5),
                Value::Null,
                Value::Integer(-1),
                Value::Text("t".to_string()),
                Value::Integer(1),
                Value::Null,
                Value::Null,
                Value::Null,
            ]
        ]
    );
}

#[turso_macros::test]
fn test_cdc_version_backward_compat_v1_full(db: TempDatabase) {
    let conn = setup_backward_compat_v1(&db, "full");
    let rows = replace_column_with_null(limbo_exec_rows(&conn, "SELECT * FROM turso_cdc"), 1);
    assert_eq!(
        rows,
        vec![
            vec![
                Value::Integer(1),
                Value::Null,
                Value::Integer(1),
                Value::Text("t".to_string()),
                Value::Integer(1),
                Value::Null,
                Value::Blob(record([Value::Integer(1), Value::Integer(2)])),
                Value::Null,
            ],
            vec![
                Value::Integer(2),
                Value::Null,
                Value::Integer(1),
                Value::Text("t".to_string()),
                Value::Integer(3),
                Value::Null,
                Value::Blob(record([Value::Integer(3), Value::Integer(4)])),
                Value::Null,
            ],
            vec![
                Value::Integer(3),
                Value::Null,
                Value::Integer(0),
                Value::Text("t".to_string()),
                Value::Integer(1),
                Value::Blob(record([Value::Integer(1), Value::Integer(2)])),
                Value::Blob(record([Value::Integer(1), Value::Integer(3)])),
                Value::Blob(record([
                    Value::Integer(0),
                    Value::Integer(1),
                    Value::Null,
                    Value::Integer(3)
                ])),
            ],
            vec![
                Value::Integer(4),
                Value::Null,
                Value::Integer(-1),
                Value::Text("t".to_string()),
                Value::Integer(3),
                Value::Blob(record([Value::Integer(3), Value::Integer(4)])),
                Value::Null,
                Value::Null,
            ],
            vec![
                Value::Integer(5),
                Value::Null,
                Value::Integer(-1),
                Value::Text("t".to_string()),
                Value::Integer(1),
                Value::Blob(record([Value::Integer(1), Value::Integer(3)])),
                Value::Null,
                Value::Null,
            ]
        ]
    );
}

#[turso_macros::test]
fn test_cdc_version_preserves_old_version(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t (x INTEGER PRIMARY KEY, y)")
        .unwrap();

    // Simulate a database with a pre-existing CDC table at a fake old version
    conn.execute(
        "CREATE TABLE turso_cdc (change_id INTEGER PRIMARY KEY AUTOINCREMENT, change_time INTEGER, change_type INTEGER, table_name TEXT, id, before BLOB, after BLOB, updates BLOB)",
    ).unwrap();
    conn.execute(
        "CREATE TABLE turso_cdc_version (table_name TEXT PRIMARY KEY, version TEXT NOT NULL)",
    )
    .unwrap();
    conn.execute("INSERT INTO turso_cdc_version (table_name, version) VALUES ('turso_cdc', 'v0')")
        .unwrap();

    // Enable CDC — should fail because "v0" is an unknown version
    let result = conn.execute("PRAGMA unstable_capture_data_changes_conn('full')");
    assert!(
        result.is_err(),
        "enabling CDC with unknown version 'v0' should fail"
    );
}

#[turso_macros::test]
fn test_cdc_pragma_get_returns_version(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t (x INTEGER PRIMARY KEY, y)")
        .unwrap();

    // Before enabling CDC, PRAGMA GET should return "off" with null table and version
    let rows = limbo_exec_rows(&conn, "PRAGMA unstable_capture_data_changes_conn");
    assert_eq!(
        rows,
        vec![vec![
            Value::Text("off".to_string()),
            Value::Null,
            Value::Null,
        ]]
    );

    // Enable CDC
    conn.execute("PRAGMA unstable_capture_data_changes_conn('full')")
        .unwrap();

    // PRAGMA GET should return mode, table, and version
    let rows = limbo_exec_rows(&conn, "PRAGMA unstable_capture_data_changes_conn");
    assert_eq!(
        rows,
        vec![vec![
            Value::Text("full".to_string()),
            Value::Text("turso_cdc".to_string()),
            Value::Text(CDC_VERSION_CURRENT.to_string()),
        ]]
    );

    // Disable CDC and verify it goes back to "off"
    conn.execute("PRAGMA unstable_capture_data_changes_conn('off')")
        .unwrap();
    let rows = limbo_exec_rows(&conn, "PRAGMA unstable_capture_data_changes_conn");
    assert_eq!(
        rows,
        vec![vec![
            Value::Text("off".to_string()),
            Value::Null,
            Value::Null,
        ]]
    );
}

#[turso_macros::test]
fn test_cdc_pragma_idempotent(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t (x INTEGER PRIMARY KEY, y)")
        .unwrap();

    // Enable CDC
    conn.execute("PRAGMA unstable_capture_data_changes_conn('full')")
        .unwrap();

    // Insert a row — should produce CDC entries (INSERT + COMMIT = 2 rows in v2)
    conn.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    let rows = limbo_exec_rows(&conn, "SELECT COUNT(*) FROM turso_cdc");
    assert_eq!(rows, vec![vec![Value::Integer(2)]]);

    // Enable CDC again (idempotent — should not create extra CDC entries)
    conn.execute("PRAGMA unstable_capture_data_changes_conn('full')")
        .unwrap();

    // CDC count should not have changed (no self-capture)
    let rows = limbo_exec_rows(&conn, "SELECT COUNT(*) FROM turso_cdc");
    assert_eq!(rows, vec![vec![Value::Integer(2)]]);

    // Switch mode — should also be idempotent (no DDL, just mode change)
    conn.execute("PRAGMA unstable_capture_data_changes_conn('id')")
        .unwrap();
    let rows = limbo_exec_rows(&conn, "SELECT COUNT(*) FROM turso_cdc");
    assert_eq!(rows, vec![vec![Value::Integer(2)]]);

    // Verify mode actually changed
    let info = conn.get_capture_data_changes_info();
    let info = info.as_ref().expect("CDC should be enabled");
    assert_eq!(info.mode, turso_core::CaptureDataChangesMode::Id);

    // Insert another row — should still capture (INSERT + COMMIT = 2 more rows)
    conn.execute("INSERT INTO t VALUES (2, 20)").unwrap();
    let rows = limbo_exec_rows(&conn, "SELECT COUNT(*) FROM turso_cdc");
    assert_eq!(rows, vec![vec![Value::Integer(4)]]);
}

#[turso_macros::test]
fn test_cdc_version_helper_defaults_to_v1(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t (x INTEGER PRIMARY KEY, y)")
        .unwrap();

    // Simulate a pre-version-tracking database: CDC table exists but no version table
    conn.execute(
        "CREATE TABLE turso_cdc (change_id INTEGER PRIMARY KEY AUTOINCREMENT, change_time INTEGER, change_type INTEGER, table_name TEXT, id, before BLOB, after BLOB, updates BLOB)",
    ).unwrap();

    // Enable CDC — InitCdcVersion will detect existing v1 table and set version to "v1"
    conn.execute("PRAGMA unstable_capture_data_changes_conn('full')")
        .unwrap();

    // version() helper should return v1 since the pre-existing table has v1 schema
    let info = conn.get_capture_data_changes_info();
    let info = info.as_ref().expect("CDC should be enabled");
    assert_eq!(info.cdc_version(), turso_core::CdcVersion::V1);

    // Now test with None version directly via parse (simulates old code path)
    let parsed = turso_core::CaptureDataChangesInfo::parse("full", None)
        .unwrap()
        .unwrap();
    // version field is None, but version() should return current version
    assert_eq!(parsed.version, None);
    assert_eq!(parsed.cdc_version(), CDC_VERSION_CURRENT);
}

#[turso_macros::test]
fn test_cdc_preexisting_v1_table_writes_v1_format(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t (x INTEGER PRIMARY KEY, y)")
        .unwrap();

    // Simulate a pre-existing v1 CDC table (8 columns, no version table)
    conn.execute(
        "CREATE TABLE turso_cdc (change_id INTEGER PRIMARY KEY AUTOINCREMENT, change_time INTEGER, change_type INTEGER, table_name TEXT, id, before BLOB, after BLOB, updates BLOB)",
    ).unwrap();

    // Enable CDC — should detect v1 schema and use v1 format
    conn.execute("PRAGMA unstable_capture_data_changes_conn('id')")
        .unwrap();

    // Version should be detected as v1
    let rows = limbo_exec_rows(
        &conn,
        "SELECT version FROM turso_cdc_version WHERE table_name = 'turso_cdc'",
    );
    assert_eq!(rows, vec![vec![Value::Text("v1".to_string())]]);

    // Insert multi-row — v1 format has no change_txn_id column and no COMMIT records
    conn.execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();

    // Verify CDC rows are in correct v1 format (8 columns, no COMMIT records)
    let rows = replace_column_with_null(limbo_exec_rows(&conn, "SELECT * FROM turso_cdc"), 1);
    assert_eq!(
        rows,
        vec![
            vec![
                Value::Integer(1),
                Value::Null,
                Value::Integer(1),
                Value::Text("t".to_string()),
                Value::Integer(1),
                Value::Null,
                Value::Null,
                Value::Null,
            ],
            vec![
                Value::Integer(2),
                Value::Null,
                Value::Integer(1),
                Value::Text("t".to_string()),
                Value::Integer(2),
                Value::Null,
                Value::Null,
                Value::Null,
            ],
            vec![
                Value::Integer(3),
                Value::Null,
                Value::Integer(1),
                Value::Text("t".to_string()),
                Value::Integer(3),
                Value::Null,
                Value::Null,
                Value::Null,
            ],
        ]
    );
}

#[turso_macros::test]
fn test_cdc_drop_table_cleans_up_version(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t (x INTEGER PRIMARY KEY, y)")
        .unwrap();

    // Enable CDC — creates turso_cdc and turso_cdc_version tables
    conn.execute("PRAGMA unstable_capture_data_changes_conn('full')")
        .unwrap();

    // Verify version entry exists
    let rows = limbo_exec_rows(&conn, "SELECT table_name, version FROM turso_cdc_version");
    assert_eq!(
        rows,
        vec![vec![
            Value::Text("turso_cdc".into()),
            Value::Text(CDC_VERSION_CURRENT.to_string())
        ]]
    );

    // Drop the CDC table
    conn.execute("DROP TABLE turso_cdc").unwrap();

    // Version entry should be cleaned up
    let rows = limbo_exec_rows(&conn, "SELECT COUNT(*) FROM turso_cdc_version");
    assert_eq!(rows, vec![vec![Value::Integer(0)]]);
}

// ============================================================================
// CDC v2 specific tests
// ============================================================================

#[turso_macros::test]
fn test_cdc_v2_txn_id_autocommit(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t (x INTEGER PRIMARY KEY, y)")
        .unwrap();
    conn.execute("PRAGMA unstable_capture_data_changes_conn('id')")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 20)").unwrap();

    // Each autocommit INSERT gets its own change_txn_id.
    // Records + COMMIT within the same statement share the same change_txn_id.
    let rows = limbo_exec_rows(&conn, "SELECT change_txn_id, change_type FROM turso_cdc");
    // Statement 1: INSERT + COMMIT share same txn_id
    assert_eq!(rows[0][0], rows[1][0]); // INSERT and COMMIT same txn_id
    assert_eq!(rows[0][1], Value::Integer(1)); // INSERT
    assert_eq!(rows[1][1], Value::Integer(2)); // COMMIT
                                               // Statement 2: INSERT + COMMIT share same txn_id
    assert_eq!(rows[2][0], rows[3][0]); // INSERT and COMMIT same txn_id
    assert_eq!(rows[2][1], Value::Integer(1)); // INSERT
    assert_eq!(rows[3][1], Value::Integer(2)); // COMMIT
                                               // Different statements have different txn_ids
    assert_ne!(rows[0][0], rows[2][0]);
}

#[turso_macros::test]
fn test_cdc_v2_txn_id_explicit_transaction(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t (x INTEGER PRIMARY KEY, y)")
        .unwrap();
    conn.execute("PRAGMA unstable_capture_data_changes_conn('id')")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 20)").unwrap();
    conn.execute("INSERT INTO t VALUES (3, 30)").unwrap();
    conn.execute("COMMIT").unwrap();

    // All records in explicit transaction share the same change_txn_id.
    // COMMIT record appears at the end.
    let rows = limbo_exec_rows(&conn, "SELECT change_txn_id, change_type FROM turso_cdc");
    assert_eq!(rows.len(), 4); // 3 INSERTs + 1 COMMIT
    let txn_id = &rows[0][0];
    for row in &rows {
        assert_eq!(&row[0], txn_id); // all share same txn_id
    }
    assert_eq!(rows[0][1], Value::Integer(1)); // INSERT
    assert_eq!(rows[1][1], Value::Integer(1)); // INSERT
    assert_eq!(rows[2][1], Value::Integer(1)); // INSERT
    assert_eq!(rows[3][1], Value::Integer(2)); // COMMIT
}

#[turso_macros::test]
fn test_cdc_v2_commit_record_fields(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t (x INTEGER PRIMARY KEY, y)")
        .unwrap();
    conn.execute("PRAGMA unstable_capture_data_changes_conn('full')")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10)").unwrap();

    // Get the COMMIT record (last row)
    let rows = limbo_exec_rows(&conn, "SELECT * FROM turso_cdc WHERE change_type = 2");
    assert_eq!(rows.len(), 1);
    let commit = &rows[0];
    // change_id is set (not null)
    assert!(matches!(commit[0], Value::Integer(_)));
    // change_time is set
    assert!(matches!(commit[1], Value::Integer(_)));
    // change_txn_id is set
    assert!(matches!(commit[2], Value::Integer(_)));
    // change_type = 2
    assert_eq!(commit[3], Value::Integer(2));
    // table_name, id, before, after, updates are all NULL
    assert_eq!(commit[4], Value::Null);
    assert_eq!(commit[5], Value::Null);
    assert_eq!(commit[6], Value::Null);
    assert_eq!(commit[7], Value::Null);
    assert_eq!(commit[8], Value::Null);
}

// Note: is_autocommit() and conn_txn_id() are internal functions and cannot be
// called from user SQL. They are tested indirectly through the CDC record tests.

#[turso_macros::test]
fn test_cdc_v2_txn_id_reset_after_commit(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t (x INTEGER PRIMARY KEY, y)")
        .unwrap();
    conn.execute("PRAGMA unstable_capture_data_changes_conn('id')")
        .unwrap();

    // First autocommit statement
    conn.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    // Second autocommit statement — should get a different txn_id
    conn.execute("INSERT INTO t VALUES (2, 20)").unwrap();

    let rows = limbo_exec_rows(
        &conn,
        "SELECT change_txn_id FROM turso_cdc WHERE change_type != 2",
    );
    // Two INSERT records with different txn_ids (reset happened between statements)
    assert_eq!(rows.len(), 2);
    assert_ne!(rows[0][0], rows[1][0]);
}

#[turso_macros::test]
fn test_cdc_v2_schema_has_9_columns(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t (x INTEGER PRIMARY KEY, y)")
        .unwrap();
    conn.execute("PRAGMA unstable_capture_data_changes_conn('id')")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10)").unwrap();

    // v2 CDC table should have 9 columns
    let rows = limbo_exec_rows(&conn, "SELECT * FROM turso_cdc LIMIT 1");
    assert_eq!(rows[0].len(), 9);

    // Verify column names via table_columns_json_array
    let rows = limbo_exec_rows(&conn, "SELECT table_columns_json_array('turso_cdc')");
    assert_eq!(
        rows,
        vec![vec![Value::Text(
            r#"["change_id","change_time","change_txn_id","change_type","table_name","id","before","after","updates"]"#
                .to_string()
        )]]
    );
}
