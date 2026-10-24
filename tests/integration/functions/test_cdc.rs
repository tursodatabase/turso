use std::sync::Arc;

use rusqlite::types::Value;
use turso_core::types::ImmutableRecord;
use turso_core::CDC_VERSION_CURRENT;

use crate::assertions::{AssertColumn, Cell};
use crate::common::{limbo_exec_rows, limbo_exec_rows_fallible, TempDatabase};
use asserting::prelude::*;

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
fn test_cdc_internal_inserts_change_counters(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t (x INTEGER PRIMARY KEY, y)")
        .unwrap();
    conn.execute("PRAGMA capture_data_changes_conn('full')")
        .unwrap();

    let total_changes_before_insert = conn.total_changes();
    conn.execute("INSERT INTO t VALUES (42, 'hello')").unwrap();

    assert_eq!(
        (
            conn.changes(),
            conn.total_changes() - total_changes_before_insert
        ),
        (1, 3)
    );

    let total_changes_before_insert = conn.total_changes();
    conn.execute("INSERT INTO t VALUES (43, 'world'), (44, 'again')")
        .unwrap();

    assert_eq!(
        (
            conn.changes(),
            conn.total_changes() - total_changes_before_insert
        ),
        (2, 5)
    );
}

#[turso_macros::test]
fn test_cdc_simple_id(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t (x INTEGER PRIMARY KEY, y)")
        .unwrap();
    conn.execute("PRAGMA capture_data_changes_conn('id')")
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
        .unwrap()
        .get_payload()
        .to_vec()
}

#[turso_macros::test]
fn test_cdc_simple_before(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t (x INTEGER PRIMARY KEY, y)")
        .unwrap();
    conn.execute("PRAGMA capture_data_changes_conn('before')")
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
    conn.execute("PRAGMA capture_data_changes_conn('after')")
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
    conn.execute("PRAGMA capture_data_changes_conn('full')")
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
fn test_cdc_delete_all_keeps_one_record_per_deleted_row(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t (x INTEGER PRIMARY KEY, y)")
        .unwrap();
    conn.execute("PRAGMA capture_data_changes_conn('full')")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 2), (3, 4)").unwrap();

    conn.execute("DELETE FROM t").unwrap();

    assert_eq!(
        normalize_cdc_v2_rows(limbo_exec_rows(&conn, "SELECT * FROM turso_cdc")),
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
                -1,
                "t",
                1,
                Some(record([Value::Integer(1), Value::Integer(2)])),
                None,
                None,
            ),
            v2_row(
                -1,
                "t",
                3,
                Some(record([Value::Integer(3), Value::Integer(4)])),
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
    conn.execute("PRAGMA capture_data_changes_conn('id')")
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
    conn.execute("PRAGMA capture_data_changes_conn('id')")
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
        .execute("PRAGMA capture_data_changes_conn('id')")
        .unwrap();
    conn1.execute("INSERT INTO t VALUES (2, 20)").unwrap(); // captured
    let conn2 = db.connect_limbo();
    conn2.execute("INSERT INTO t VALUES (3, 30)").unwrap();
    conn2
        .execute("PRAGMA capture_data_changes_conn('id')")
        .unwrap();
    conn2.execute("INSERT INTO t VALUES (4, 40)").unwrap(); // captured
    conn2
        .execute("PRAGMA capture_data_changes_conn('off')")
        .unwrap();
    conn2.execute("INSERT INTO t VALUES (5, 50)").unwrap();

    conn1.execute("INSERT INTO t VALUES (6, 60)").unwrap(); // captured
    conn1
        .execute("PRAGMA capture_data_changes_conn('off')")
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
        .execute("PRAGMA capture_data_changes_conn('id,custom_cdc')")
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
        .execute("PRAGMA capture_data_changes_conn('id,custom_cdc')")
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

#[turso_macros::test()]
fn test_cdc_update_cdc_table_in_full_mode(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t (x)").unwrap();
    conn.execute("PRAGMA capture_data_changes_conn('full')")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1)").unwrap();
    conn.execute("UPDATE turso_cdc SET table_name = 'u' WHERE change_type = 1")
        .unwrap();
    let rows = limbo_exec_rows(
        &conn,
        "SELECT change_type, table_name FROM turso_cdc ORDER BY change_id",
    );
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(1), Value::Text("u".to_string())],
            vec![Value::Integer(2), Value::Null],
        ]
    );
}

#[turso_macros::test()]
fn test_cdc_ignore_changes_in_temp_tables(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE m (x)").unwrap();
    conn.execute("PRAGMA capture_data_changes_conn('full')")
        .unwrap();
    for sql in [
        "CREATE TEMP TABLE X AS SELECT 1 AS a",
        "INSERT INTO X VALUES (2)",
        "CREATE TABLE temp.Y (b PRIMARY KEY, c)",
        "CREATE INDEX temp.y_c ON Y (c)",
        "INSERT INTO Y VALUES (3, 3)",
        "INSERT INTO Y VALUES (3, 3) ON CONFLICT (b) DO UPDATE SET c = 4",
        "INSERT OR REPLACE INTO Y VALUES (3, 5)",
        "UPDATE Y SET c = 6",
        "ALTER TABLE Y ADD COLUMN d",
        "ALTER TABLE Y RENAME TO Z",
        "DELETE FROM Z",
        "DROP INDEX y_c",
        "DROP TABLE X",
        "DROP TABLE temp.Z",
        "INSERT INTO m VALUES (1)",
    ] {
        conn.execute(sql).unwrap();
    }
    let rows = limbo_exec_rows(
        &conn,
        "SELECT change_type, table_name FROM turso_cdc ORDER BY change_id",
    );
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(1), Value::Text("m".to_string())],
            vec![Value::Integer(2), Value::Null],
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
        .execute("PRAGMA capture_data_changes_conn('id,custom_cdc')")
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
    conn.execute("PRAGMA capture_data_changes_conn('id')")
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
            v2_row(1, "sqlite_schema", 6, None, None, None), // CREATE TABLE t
            v2_row(1, "t", 1, None, None, None),             // INSERT (1, 10)
            v2_row(1, "t", 2, None, None, None),             // INSERT (2, 20)
            v2_row(1, "sqlite_schema", 7, None, None, None), // CREATE TABLE q
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
        .execute("PRAGMA capture_data_changes_conn('id,custom_cdc1')")
        .unwrap();
    conn2
        .execute("PRAGMA capture_data_changes_conn('id,custom_cdc2')")
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
        .execute("PRAGMA capture_data_changes_conn('id,custom_cdc1')")
        .unwrap();
    conn2
        .execute("PRAGMA capture_data_changes_conn('id,custom_cdc2')")
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
    conn.execute("PRAGMA capture_data_changes_conn('full')")
        .unwrap();
    conn.execute("CREATE TABLE t(x, y, z UNIQUE, q, PRIMARY KEY (x, y))")
        .unwrap();
    conn.execute("CREATE TABLE q(a, b, c)").unwrap();
    conn.execute("CREATE INDEX t_q ON t(q)").unwrap();
    conn.execute("CREATE INDEX q_abc ON q(a, b, c)").unwrap();
    conn.execute("DROP TABLE t").unwrap();
    conn.execute("DROP INDEX q_abc").unwrap();
    let rows = normalize_cdc_v2_rows(limbo_exec_rows(&conn, "SELECT * FROM turso_cdc"));

    // turso_cdc_version table + its auto-index + autoincrement backing table
    // add 3 entries to sqlite_schema, shifting rowids and rootpages by +3
    // compared to before version tracking
    assert_eq!(
        rows,
        vec![
            // CREATE TABLE t
            v2_row(
                1,
                "sqlite_schema",
                6,
                None,
                Some(record([
                    Value::Text("table".to_string()),
                    Value::Text("t".to_string()),
                    Value::Text("t".to_string()),
                    Value::Integer(7),
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
                9,
                None,
                Some(record([
                    Value::Text("table".to_string()),
                    Value::Text("q".to_string()),
                    Value::Text("q".to_string()),
                    Value::Integer(10),
                    Value::Text("CREATE TABLE q (a, b, c)".to_string())
                ])),
                None,
            ),
            v2_commit(),
            // CREATE INDEX t_q
            v2_row(
                1,
                "sqlite_schema",
                10,
                None,
                Some(record([
                    Value::Text("index".to_string()),
                    Value::Text("t_q".to_string()),
                    Value::Text("t".to_string()),
                    Value::Integer(11),
                    Value::Text("CREATE INDEX t_q ON t (q)".to_string())
                ])),
                None,
            ),
            v2_commit(),
            // CREATE INDEX q_abc
            v2_row(
                1,
                "sqlite_schema",
                11,
                None,
                Some(record([
                    Value::Text("index".to_string()),
                    Value::Text("q_abc".to_string()),
                    Value::Text("q".to_string()),
                    Value::Integer(12),
                    Value::Text("CREATE INDEX q_abc ON q (a, b, c)".to_string())
                ])),
                None,
            ),
            v2_commit(),
            // DROP TABLE t
            v2_row(
                -1,
                "sqlite_schema",
                6,
                Some(record([
                    Value::Text("table".to_string()),
                    Value::Text("t".to_string()),
                    Value::Text("t".to_string()),
                    Value::Integer(7),
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
                11,
                Some(record([
                    Value::Text("index".to_string()),
                    Value::Text("q_abc".to_string()),
                    Value::Text("q".to_string()),
                    Value::Integer(12),
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
    conn.execute("PRAGMA capture_data_changes_conn('full')")
        .unwrap();
    conn.execute("CREATE TABLE t(x, y, z UNIQUE, q, PRIMARY KEY (x, y))")
        .unwrap();
    conn.execute("ALTER TABLE t DROP COLUMN q").unwrap();
    conn.execute("ALTER TABLE t ADD COLUMN t").unwrap();
    let rows = normalize_cdc_v2_rows(limbo_exec_rows(&conn, "SELECT * FROM turso_cdc"));

    // turso_cdc_version table + its auto-index + autoincrement backing table
    // add 3 entries to sqlite_schema, shifting rowids and rootpages by +3
    assert_eq!(
        rows,
        vec![
            // CREATE TABLE t
            v2_row(
                1,
                "sqlite_schema",
                6,
                None,
                Some(record([
                    Value::Text("table".to_string()),
                    Value::Text("t".to_string()),
                    Value::Text("t".to_string()),
                    Value::Integer(7),
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
                6,
                Some(record([
                    Value::Text("table".to_string()),
                    Value::Text("t".to_string()),
                    Value::Text("t".to_string()),
                    Value::Integer(7),
                    Value::Text(
                        "CREATE TABLE t (x, y, z UNIQUE, q, PRIMARY KEY (x, y))".to_string()
                    )
                ])),
                Some(record([
                    Value::Text("table".to_string()),
                    Value::Text("t".to_string()),
                    Value::Text("t".to_string()),
                    Value::Integer(7),
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
                6,
                Some(record([
                    Value::Text("table".to_string()),
                    Value::Text("t".to_string()),
                    Value::Text("t".to_string()),
                    Value::Integer(7),
                    Value::Text("CREATE TABLE t (x, y, z UNIQUE, PRIMARY KEY (x, y))".to_string())
                ])),
                Some(record([
                    Value::Text("table".to_string()),
                    Value::Text("t".to_string()),
                    Value::Text("t".to_string()),
                    Value::Integer(7),
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

// TODO: cannot use mvcc because of indexes
#[turso_macros::test()]
fn test_cdc_schema_changes_alter_table_rename(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("PRAGMA capture_data_changes_conn('full')")
        .unwrap();
    conn.execute("CREATE TABLE t(x INTEGER PRIMARY KEY, y)")
        .unwrap();
    conn.execute("CREATE INDEX t_y ON t(y)").unwrap();
    conn.execute("CREATE TRIGGER t_ins AFTER INSERT ON t BEGIN SELECT new.y; END")
        .unwrap();
    conn.execute("CREATE VIEW v AS SELECT y FROM t").unwrap();
    conn.execute("DELETE FROM turso_cdc").unwrap();
    conn.execute("ALTER TABLE t RENAME TO t2").unwrap();
    conn.execute("ALTER TABLE t2 RENAME COLUMN y TO z").unwrap();
    let rows = normalize_cdc_v2_rows(limbo_exec_rows(&conn, "SELECT * FROM turso_cdc"));

    let table_row = |name: &str, sql: &str| {
        record([
            Value::Text("table".to_string()),
            Value::Text(name.to_string()),
            Value::Text(name.to_string()),
            Value::Integer(7),
            Value::Text(sql.to_string()),
        ])
    };
    let sql_updated_to = |statement: &str| {
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
            Value::Text(statement.to_string()),
        ])
    };
    assert_eq!(
        rows,
        vec![
            v2_row(
                0,
                "sqlite_schema",
                6,
                Some(table_row("t", "CREATE TABLE t (x INTEGER PRIMARY KEY, y)")),
                Some(table_row(
                    "t2",
                    "CREATE TABLE t2 (x INTEGER PRIMARY KEY, y)"
                )),
                Some(sql_updated_to("ALTER TABLE t RENAME TO t2")),
            ),
            v2_commit(),
            v2_row(
                0,
                "sqlite_schema",
                6,
                Some(table_row(
                    "t2",
                    "CREATE TABLE t2 (x INTEGER PRIMARY KEY, y)"
                )),
                Some(table_row(
                    "t2",
                    "CREATE TABLE t2 (x INTEGER PRIMARY KEY, z)"
                )),
                Some(sql_updated_to("ALTER TABLE t2 RENAME COLUMN y TO z")),
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
    conn.execute("PRAGMA capture_data_changes_conn('full')")
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
    conn.execute("PRAGMA capture_data_changes_conn('id,my_cdc')")
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
    conn.execute("PRAGMA capture_data_changes_conn('full')")
        .unwrap();
    // Disable CDC
    conn.execute("PRAGMA capture_data_changes_conn('off')")
        .unwrap();
    // Re-enable CDC — turso_cdc table already exists, so no duplicate version row
    conn.execute("PRAGMA capture_data_changes_conn('full')")
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
    conn.execute(format!("PRAGMA capture_data_changes_conn('{mode}')"))
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
    let result = conn.execute("PRAGMA capture_data_changes_conn('full')");
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
    let rows = limbo_exec_rows(&conn, "PRAGMA capture_data_changes_conn");
    assert_eq!(
        rows,
        vec![vec![
            Value::Text("off".to_string()),
            Value::Null,
            Value::Null,
        ]]
    );

    // Enable CDC
    conn.execute("PRAGMA capture_data_changes_conn('full')")
        .unwrap();

    // PRAGMA GET should return mode, table, and version
    let rows = limbo_exec_rows(&conn, "PRAGMA capture_data_changes_conn");
    assert_eq!(
        rows,
        vec![vec![
            Value::Text("full".to_string()),
            Value::Text("turso_cdc".to_string()),
            Value::Text(CDC_VERSION_CURRENT.to_string()),
        ]]
    );

    // Disable CDC and verify it goes back to "off"
    conn.execute("PRAGMA capture_data_changes_conn('off')")
        .unwrap();
    let rows = limbo_exec_rows(&conn, "PRAGMA capture_data_changes_conn");
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
    conn.execute("PRAGMA capture_data_changes_conn('full')")
        .unwrap();

    // Insert a row — should produce CDC entries (INSERT + COMMIT = 2 rows in v2)
    conn.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    let rows = limbo_exec_rows(&conn, "SELECT COUNT(*) FROM turso_cdc");
    assert_eq!(rows, vec![vec![Value::Integer(2)]]);

    // Enable CDC again (idempotent — should not create extra CDC entries)
    conn.execute("PRAGMA capture_data_changes_conn('full')")
        .unwrap();

    // CDC count should not have changed (no self-capture)
    let rows = limbo_exec_rows(&conn, "SELECT COUNT(*) FROM turso_cdc");
    assert_eq!(rows, vec![vec![Value::Integer(2)]]);

    // Switch mode — should also be idempotent (no DDL, just mode change)
    conn.execute("PRAGMA capture_data_changes_conn('id')")
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
    conn.execute("PRAGMA capture_data_changes_conn('full')")
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
    conn.execute("PRAGMA capture_data_changes_conn('id')")
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
    conn.execute("PRAGMA capture_data_changes_conn('full')")
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

    // The cleanup must remove the primary-key index entry as well as the table row.
    let rows = limbo_exec_rows(
        &conn,
        "SELECT COUNT(*) FROM turso_cdc_version \
         INDEXED BY sqlite_autoindex_turso_cdc_version_1",
    );
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
    conn.execute("PRAGMA capture_data_changes_conn('id')")
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
    conn.execute("PRAGMA capture_data_changes_conn('id')")
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
    conn.execute("PRAGMA capture_data_changes_conn('full')")
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
    conn.execute("PRAGMA capture_data_changes_conn('id')")
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
fn test_cdc_mvcc_failed_insert_then_empty_commit(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
    conn.execute("PRAGMA capture_data_changes_conn('full')")
        .unwrap();
    conn.execute("CREATE TABLE t (x INTEGER PRIMARY KEY, y UNIQUE)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    limbo_exec_rows_fallible(&db, &conn, "INSERT INTO t VALUES (3, 30), (4, 30)")
        .expect_err("second row must violate the UNIQUE constraint");

    let rows_before = limbo_exec_rows(&conn, "SELECT change_type FROM turso_cdc");
    conn.execute("BEGIN DEFERRED").unwrap();
    conn.execute("COMMIT").unwrap();
    let rows_after_empty_commit = limbo_exec_rows(&conn, "SELECT change_type FROM turso_cdc");
    assert_eq!(rows_after_empty_commit, rows_before);

    conn.execute("INSERT INTO t VALUES (2, 20)").unwrap();
    let rows = limbo_exec_rows(&conn, "SELECT change_type FROM turso_cdc");
    assert_eq!(
        &rows[rows.len() - 2..],
        &[vec![Value::Integer(1)], vec![Value::Integer(2)],]
    );
}

/// Regression test for https://github.com/tursodatabase/turso/issues/7677.
///
/// With CDC v2 enabled, an explicit COMMIT emits a CDC commit record. For a transaction that
/// captured no changes (empty or read-only) that record used to be written without
/// establishing a write transaction, leaving a dirty CDC page that the commit path never
/// flushed or cleared. The leaked page then tripped the "dirty pages should be empty for read
/// txn" assertion when the *next* transaction was rolled back.
///
/// A committed no-change transaction followed by a rolled-back one must not panic, and
/// no-change transactions must not emit CDC commit records (they stay read-only).
#[turso_macros::test]
fn test_cdc_v2_no_change_commit_then_rollback(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("PRAGMA capture_data_changes_conn('full,turso_cdc')")
        .unwrap();

    // An explicit transaction that captures no changes, committed.
    conn.execute("BEGIN").unwrap();
    conn.execute("COMMIT").unwrap();

    // A subsequent read-only transaction that is rolled back used to panic here.
    conn.execute("BEGIN").unwrap();
    conn.execute("ROLLBACK").unwrap();

    // The connection is still usable...
    conn.execute("CREATE TABLE t (x INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1)").unwrap();
    let rows = limbo_exec_rows(&conn, "SELECT x FROM t");
    assert_eq!(rows, vec![vec![Value::Integer(1)]]);

    // ...and the no-change BEGIN/COMMIT and BEGIN/ROLLBACK contributed no commit records:
    // the only ones come from the autocommit CREATE TABLE and INSERT above.
    let commits = limbo_exec_rows(
        &conn,
        "SELECT change_txn_id FROM turso_cdc WHERE change_type = 2",
    );
    assert_that!(commits)
        .named("commit records")
        .has_length(2)
        .column(0)
        .does_not_contain(Cell::from(-1));
}

#[turso_macros::test]
fn test_cdc_v2_schema_has_9_columns(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t (x INTEGER PRIMARY KEY, y)")
        .unwrap();
    conn.execute("PRAGMA capture_data_changes_conn('id')")
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

// TODO: cannot use mvcc because of indexes
#[turso_macros::test()]
fn test_cdc_no_internal_table_changes(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("PRAGMA capture_data_changes_conn('full')")
        .unwrap();
    conn.execute("CREATE TABLE t(x)").unwrap();
    conn.execute("CREATE INDEX t_idx ON t(x)").unwrap();

    let rows = normalize_cdc_v2_rows(limbo_exec_rows(&conn, "SELECT * FROM turso_cdc"));
    // Only user DDL should appear — no records for turso_cdc, turso_cdc_version,
    // sqlite_sequence, or their auto-indexes. Those are created before CDC is active.
    assert_eq!(
        rows,
        vec![
            // CREATE TABLE t
            v2_row(
                1,
                "sqlite_schema",
                6,
                None,
                Some(record([
                    Value::Text("table".to_string()),
                    Value::Text("t".to_string()),
                    Value::Text("t".to_string()),
                    Value::Integer(7),
                    Value::Text("CREATE TABLE t (x)".to_string())
                ])),
                None,
            ),
            v2_commit(),
            // CREATE INDEX t_idx ON t(x)
            v2_row(
                1,
                "sqlite_schema",
                7,
                None,
                Some(record([
                    Value::Text("index".to_string()),
                    Value::Text("t_idx".to_string()),
                    Value::Text("t".to_string()),
                    Value::Integer(8),
                    Value::Text("CREATE INDEX t_idx ON t (x)".to_string())
                ])),
                None,
            ),
            v2_commit(),
        ]
    );
}

#[turso_macros::test]
fn test_cdc_drop_turso_cdc(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t (x INTEGER PRIMARY KEY, y)")
        .unwrap();
    conn.execute("PRAGMA capture_data_changes_conn('id')")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10)").unwrap();

    // Verify CDC records exist (INSERT + COMMIT)
    let rows = limbo_exec_rows(&conn, "SELECT COUNT(*) FROM turso_cdc");
    assert_eq!(rows, vec![vec![Value::Integer(2)]]);

    // Drop the CDC table — should succeed
    conn.execute("DROP TABLE turso_cdc").unwrap();

    // Verify turso_cdc is gone from sqlite_schema
    let rows = limbo_exec_rows(
        &conn,
        "SELECT COUNT(*) FROM sqlite_schema WHERE name = 'turso_cdc'",
    );
    assert_eq!(rows, vec![vec![Value::Integer(0)]]);
}

#[turso_macros::test]
fn test_cdc_drop_turso_cdc_version(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("PRAGMA capture_data_changes_conn('full')")
        .unwrap();
    conn.execute("CREATE TABLE t(x)").unwrap();

    // Verify turso_cdc_version exists
    let rows = limbo_exec_rows(
        &conn,
        "SELECT COUNT(*) FROM sqlite_schema WHERE name = 'turso_cdc_version'",
    );
    assert_eq!(rows, vec![vec![Value::Integer(1)]]);

    // Drop turso_cdc_version — should succeed
    conn.execute("DROP TABLE turso_cdc_version").unwrap();

    // Verify it's gone from sqlite_schema
    let rows = limbo_exec_rows(
        &conn,
        "SELECT COUNT(*) FROM sqlite_schema WHERE name = 'turso_cdc_version'",
    );
    assert_eq!(rows, vec![vec![Value::Integer(0)]]);
}

/// Count rows in the CDC table.
fn cdc_row_count(conn: &std::sync::Arc<turso_core::Connection>) -> i64 {
    let rows = limbo_exec_rows(conn, "SELECT COUNT(*) FROM turso_cdc");
    match rows[0][0] {
        Value::Integer(n) => n,
        ref other => panic!("expected integer count, got {other:?}"),
    }
}

/// Changing journal_mode while CDC capture is active must fail loudly.
///
/// Regression guard: 0.6-era commit 34a110181 ("block CDC in MVCC mode")
/// rejected `PRAGMA journal_mode = 'mvcc'` under active capture with
/// "cannot enable MVCC while CDC is active". That check was dropped in
/// c04b2c209 (logical log v3 redesign); the engine then accepted the
/// switch and silently stopped capturing while the capture pragma still
/// reported active.
#[turso_macros::test]
fn test_cdc_journal_mode_change_rejected_while_capture_active(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t (x INTEGER PRIMARY KEY, y)")
        .unwrap();
    conn.execute("PRAGMA capture_data_changes_conn('full')")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    let captured_before = cdc_row_count(&conn);
    assert!(
        captured_before > 0,
        "capture must be live before the switch attempt"
    );

    // Re-running the CURRENT mode is a no-op and must stay allowed under CDC.
    let rows = limbo_exec_rows(&conn, "PRAGMA journal_mode = 'wal'");
    assert_eq!(rows, vec![vec![Value::Text("wal".to_string())]]);

    // An actual mode CHANGE must be rejected loudly.
    let err = limbo_exec_rows_fallible(&db, &conn, "PRAGMA journal_mode = 'mvcc'")
        .expect_err("journal_mode change must be rejected while CDC capture is active");
    let msg = err.to_string();
    assert!(
        msg.contains("CDC") && msg.contains("capture_data_changes"),
        "error must name CDC capture: {msg}"
    );

    // The mode did not change and capture still works after the rejection.
    let rows = limbo_exec_rows(&conn, "PRAGMA journal_mode");
    assert_eq!(rows, vec![vec![Value::Text("wal".to_string())]]);
    conn.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
    let captured_after = cdc_row_count(&conn);
    assert!(
        captured_after > captured_before,
        "capture must still record changes after the rejected switch \
         ({captured_before} -> {captured_after})"
    );
}

/// The qualified spelling must hit the same guard.
#[turso_macros::test]
fn test_cdc_qualified_journal_mode_change_rejected(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t (x INTEGER PRIMARY KEY, y)")
        .unwrap();
    conn.execute("PRAGMA capture_data_changes_conn('full')")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    let captured_before = cdc_row_count(&conn);

    let err = limbo_exec_rows_fallible(&db, &conn, "PRAGMA main.journal_mode = 'mvcc'")
        .expect_err("qualified journal_mode change must be rejected while CDC capture is active");
    assert!(
        err.to_string().contains("CDC"),
        "error must name CDC capture: {err}"
    );

    conn.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
    assert!(cdc_row_count(&conn) > captured_before);
}

/// Without CDC capture the mvcc switch must keep working.
#[turso_macros::test]
fn test_journal_mode_mvcc_switch_allowed_without_cdc(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t (x INTEGER PRIMARY KEY, y)")
        .unwrap();
    let rows = limbo_exec_rows(&conn, "PRAGMA journal_mode = 'mvcc'");
    assert_eq!(rows, vec![vec![Value::Text("mvcc".to_string())]]);
    conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    let rows = limbo_exec_rows(&conn, "SELECT COUNT(*) FROM t");
    assert_eq!(rows, vec![vec![Value::Integer(1)]]);
}

/// Regression test for https://github.com/tursodatabase/turso/issues/9069: the
/// `before` image for `ON CONFLICT DO UPDATE` used to hold the NEW row instead
/// of the OLD one, because it was read from the cursor after the UPSERT had
/// already overwritten the row in place.
#[turso_macros::test]
fn test_cdc_upsert_before_image_in_place(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t (x INTEGER PRIMARY KEY, y)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'old')").unwrap();
    conn.execute("PRAGMA capture_data_changes_conn('full')")
        .unwrap();

    conn.execute("INSERT INTO t VALUES (1, 'new') ON CONFLICT(x) DO UPDATE SET y = excluded.y")
        .unwrap();

    let rows = normalize_cdc_v2_rows(limbo_exec_rows(&conn, "SELECT * FROM turso_cdc"));
    assert_eq!(
        rows,
        vec![
            v2_row(
                0,
                "t",
                1,
                Some(record([Value::Integer(1), Value::Text("old".to_string())])),
                Some(record([Value::Integer(1), Value::Text("new".to_string())])),
                None,
            ),
            v2_commit(),
        ]
    );
}

/// Same repro as above, but with capture mode `before`: only the before image
/// is recorded, and it must still be the OLD row.
#[turso_macros::test]
fn test_cdc_upsert_before_image_in_place_before_mode(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t (x INTEGER PRIMARY KEY, y)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'old')").unwrap();
    conn.execute("PRAGMA capture_data_changes_conn('before')")
        .unwrap();

    conn.execute("INSERT INTO t VALUES (1, 'new') ON CONFLICT(x) DO UPDATE SET y = excluded.y")
        .unwrap();

    let rows = normalize_cdc_v2_rows(limbo_exec_rows(&conn, "SELECT * FROM turso_cdc"));
    assert_eq!(
        rows,
        vec![
            v2_row(
                0,
                "t",
                1,
                Some(record([Value::Integer(1), Value::Text("old".to_string())])),
                None,
                None,
            ),
            v2_commit(),
        ]
    );
}

/// A BEFORE UPDATE trigger changes a column (`z`) that is not in the upsert's
/// own SET list. The upsert's before image must reflect the row exactly as it
/// sat on disk right before the upsert's own write: `y` still holds its
/// pre-SET value ('a'), while `z` already holds the trigger's write (999), not
/// the original value ('orig') that was there before the statement started.
///
/// Also a regression test for https://github.com/tursodatabase/turso/issues/10152: under
/// MVCC, the trigger's nested UPDATE and the upsert's own UPDATE used to collide on the same
/// CDC change id (see `test_cdc_trigger_after_insert_captures_trigger_row` for the cause), so
/// only one of the two rows below survived.
#[turso_macros::test(mvcc)]
fn test_cdc_upsert_before_image_reflects_before_update_trigger(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t (x INTEGER PRIMARY KEY, y, z)")
        .unwrap();
    conn.execute(
        "CREATE TRIGGER trg BEFORE UPDATE ON t BEGIN UPDATE t SET z = 999 WHERE x = NEW.x; END",
    )
    .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a', 'orig')")
        .unwrap();
    conn.execute("PRAGMA capture_data_changes_conn('before')")
        .unwrap();

    conn.execute(
        "INSERT INTO t VALUES (1, 'b', 'ignored') ON CONFLICT(x) DO UPDATE SET y = excluded.y",
    )
    .unwrap();

    let rows = normalize_cdc_v2_rows(limbo_exec_rows(&conn, "SELECT * FROM turso_cdc"));
    assert_eq!(
        rows,
        vec![
            // The trigger's own nested UPDATE (z: 'orig' -> 999).
            v2_row(
                0,
                "t",
                1,
                Some(record([
                    Value::Integer(1),
                    Value::Text("a".to_string()),
                    Value::Text("orig".to_string()),
                ])),
                None,
                None,
            ),
            // The upsert's own row: before image is the row as it sat on disk
            // right before the upsert wrote it: y still 'a' (pre-SET), z
            // already 999 (post-trigger).
            v2_row(
                0,
                "t",
                1,
                Some(record([
                    Value::Integer(1),
                    Value::Text("a".to_string()),
                    Value::Integer(999),
                ])),
                None,
                None,
            ),
            v2_commit(),
        ]
    );
}

/// `ON CONFLICT DO UPDATE` that changes the rowid alias takes the DELETE+INSERT
/// path. The DELETE's before image must be the OLD row (old rowid, old `y`),
/// not a row read after the delete+insert already happened.
#[turso_macros::test]
fn test_cdc_upsert_before_image_rowid_change(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t (x INTEGER PRIMARY KEY, y)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'old')").unwrap();
    conn.execute("PRAGMA capture_data_changes_conn('full')")
        .unwrap();

    conn.execute(
        "INSERT INTO t VALUES (1, 'ignored') ON CONFLICT(x) DO UPDATE SET x = 2, y = 'new'",
    )
    .unwrap();

    let rows = normalize_cdc_v2_rows(limbo_exec_rows(&conn, "SELECT * FROM turso_cdc"));
    assert_eq!(
        rows,
        vec![
            v2_row(
                -1,
                "t",
                1,
                Some(record([Value::Integer(1), Value::Text("old".to_string())])),
                None,
                None,
            ),
            v2_row(
                1,
                "t",
                2,
                None,
                Some(record([Value::Integer(2), Value::Text("new".to_string())])),
                None,
            ),
            v2_commit(),
        ]
    );
}

/// A table with a virtual generated column: the upsert's before image must
/// have the same shape as today's plain-UPDATE before image for that table
/// (the virtual column `v` excluded, only stored columns `x`/`y` present).
#[turso_macros::test]
fn test_cdc_upsert_before_image_excludes_virtual_generated_column(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute(
        "CREATE TABLE t (x INTEGER PRIMARY KEY, y, v GENERATED ALWAYS AS (y * 2) VIRTUAL)",
    )
    .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 5)").unwrap();
    conn.execute("PRAGMA capture_data_changes_conn('before')")
        .unwrap();

    conn.execute("INSERT INTO t VALUES (1, 99) ON CONFLICT(x) DO UPDATE SET y = excluded.y")
        .unwrap();
    // A plain UPDATE on the same, now-updated row, for comparison.
    conn.execute("UPDATE t SET y = 100 WHERE x = 1").unwrap();

    let rows = limbo_exec_rows(
        &conn,
        "SELECT change_type, before FROM turso_cdc WHERE change_type = 0 ORDER BY change_id",
    );
    assert_eq!(
        rows,
        vec![
            vec![
                Value::Integer(0),
                Value::Blob(record([Value::Integer(1), Value::Integer(5)])),
            ],
            vec![
                Value::Integer(0),
                Value::Blob(record([Value::Integer(1), Value::Integer(99)])),
            ],
        ]
    );
}

/// MVCC variant of the in-place repro, with the conflicting row still held
/// in the in-memory MVCC version store (not checkpointed) when the upsert
/// runs.
#[turso_macros::test]
fn test_cdc_upsert_before_image_in_place_mvcc(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
    conn.execute("CREATE TABLE t (x INTEGER PRIMARY KEY, y)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'old')").unwrap();
    conn.execute("PRAGMA capture_data_changes_conn('full')")
        .unwrap();

    conn.execute("INSERT INTO t VALUES (1, 'new') ON CONFLICT(x) DO UPDATE SET y = excluded.y")
        .unwrap();

    let rows = normalize_cdc_v2_rows(limbo_exec_rows(&conn, "SELECT * FROM turso_cdc"));
    assert_eq!(
        rows,
        vec![
            v2_row(
                0,
                "t",
                1,
                Some(record([Value::Integer(1), Value::Text("old".to_string())])),
                Some(record([Value::Integer(1), Value::Text("new".to_string())])),
                None,
            ),
            v2_commit(),
        ]
    );
}

/// MVCC variant of the rowid-changing repro.
#[turso_macros::test]
fn test_cdc_upsert_before_image_rowid_change_mvcc(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
    conn.execute("CREATE TABLE t (x INTEGER PRIMARY KEY, y)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'old')").unwrap();
    conn.execute("PRAGMA capture_data_changes_conn('full')")
        .unwrap();

    conn.execute(
        "INSERT INTO t VALUES (1, 'ignored') ON CONFLICT(x) DO UPDATE SET x = 2, y = 'new'",
    )
    .unwrap();

    let rows = normalize_cdc_v2_rows(limbo_exec_rows(&conn, "SELECT * FROM turso_cdc"));
    assert_eq!(
        rows,
        vec![
            v2_row(
                -1,
                "t",
                1,
                Some(record([Value::Integer(1), Value::Text("old".to_string())])),
                None,
                None,
            ),
            v2_row(
                1,
                "t",
                2,
                None,
                Some(record([Value::Integer(2), Value::Text("new".to_string())])),
                None,
            ),
            v2_commit(),
        ]
    );
}

/// A trigger or FK action compiles as its own subprogram; it must never write its own CDC
/// COMMIT record. Only the root statement writes the single COMMIT record for the whole
/// statement, and it must be the last row (https://github.com/tursodatabase/turso/issues/10148).
fn assert_single_trailing_commit(conn: &std::sync::Arc<turso_core::Connection>) {
    let rows = limbo_exec_rows(conn, "SELECT change_type FROM turso_cdc ORDER BY change_id");
    let commit_count = rows
        .iter()
        .filter(|row| row[0] == Value::Integer(2))
        .count();
    assert_eq!(
        commit_count, 1,
        "expected exactly one COMMIT record, got {rows:?}"
    );
    assert_eq!(
        rows.last().unwrap()[0],
        Value::Integer(2),
        "the COMMIT record must be the last row, got {rows:?}"
    );
}

#[turso_macros::test]
fn test_cdc_trigger_after_insert_autocommit_single_commit(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t(x INTEGER PRIMARY KEY, y)")
        .unwrap();
    conn.execute("CREATE TABLE audit(msg)").unwrap();
    conn.execute(
        "CREATE TRIGGER trg AFTER INSERT ON t BEGIN INSERT INTO audit VALUES ('fired'); END",
    )
    .unwrap();
    conn.execute("PRAGMA capture_data_changes_conn('full')")
        .unwrap();

    conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();

    assert_single_trailing_commit(&conn);
}

#[turso_macros::test]
fn test_cdc_fk_cascade_delete_autocommit_single_commit(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("PRAGMA foreign_keys = ON").unwrap();
    conn.execute("CREATE TABLE parent(id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute(
        "CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON DELETE CASCADE)",
    )
    .unwrap();
    conn.execute("INSERT INTO parent VALUES (1)").unwrap();
    conn.execute("INSERT INTO child VALUES (10, 1)").unwrap();
    conn.execute("PRAGMA capture_data_changes_conn('full')")
        .unwrap();

    conn.execute("DELETE FROM parent WHERE id = 1").unwrap();

    assert_single_trailing_commit(&conn);
}

/// Guard, not a regression test for the TEMP-root fix below: `t` is itself captured, so
/// `cdc_cursor_id` is always `Some` here and the TEMP-root fallback path never runs. This
/// passes with or without that fix; it only checks the single-commit invariant still holds
/// for an explicit transaction.
#[turso_macros::test]
fn test_cdc_trigger_after_insert_explicit_transaction_single_commit_guard(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t(x INTEGER PRIMARY KEY, y)")
        .unwrap();
    conn.execute("CREATE TABLE audit(msg)").unwrap();
    conn.execute(
        "CREATE TRIGGER trg AFTER INSERT ON t BEGIN INSERT INTO audit VALUES ('fired'); END",
    )
    .unwrap();
    conn.execute("PRAGMA capture_data_changes_conn('full')")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    conn.execute("COMMIT").unwrap();

    assert_single_trailing_commit(&conn);
}

/// A root statement on a TEMP table is never captured, but a trigger it fires can still
/// capture a row on a main (captured) table. The root statement must still write the one
/// COMMIT record for the whole statement.
///
/// Also a regression test for https://github.com/tursodatabase/turso/issues/10154: under
/// MVCC, each row's trigger-fired audit INSERT used to collide with (and get overwritten by)
/// a later CDC row, because the trigger subprogram allocated its change id with the plain
/// `NewRowid` path instead of the CDC table's AUTOINCREMENT sequence the root statement uses.
/// See `test_cdc_trigger_after_insert_captures_trigger_row` for the single-row case and the
/// underlying cause.
#[turso_macros::test(mvcc)]
fn test_cdc_trigger_temp_root_with_captured_write_single_commit(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE audit(msg)").unwrap();
    conn.execute("CREATE TEMP TABLE src(x)").unwrap();
    conn.execute(
        "CREATE TRIGGER trg AFTER INSERT ON src BEGIN INSERT INTO audit VALUES ('fired'); END",
    )
    .unwrap();
    conn.execute("PRAGMA capture_data_changes_conn('full')")
        .unwrap();

    // Two rows: the trigger subprogram is invoked (and compiles its end-of-statement
    // COMMIT check) once per row. A per-invocation COMMIT would show up as 2 records,
    // one of them before the last row's own write.
    conn.execute("INSERT INTO src VALUES (1), (2)").unwrap();

    let rows = limbo_exec_rows(
        &conn,
        "SELECT change_type, table_name FROM turso_cdc ORDER BY change_id",
    );
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(1), Value::Text("audit".to_string())],
            vec![Value::Integer(1), Value::Text("audit".to_string())],
            vec![Value::Integer(2), Value::Null],
        ],
        "expected two audit INSERT rows followed by exactly one COMMIT, got {rows:?}"
    );
}

/// Single-row variant of the above: a TEMP root statement whose trigger captures exactly one
/// row on a main table. Regression test for
/// https://github.com/tursodatabase/turso/issues/10154.
#[turso_macros::test(mvcc)]
fn test_cdc_trigger_temp_root_with_captured_write_single_row(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE audit(msg)").unwrap();
    conn.execute("CREATE TEMP TABLE src(x)").unwrap();
    conn.execute(
        "CREATE TRIGGER trg AFTER INSERT ON src BEGIN INSERT INTO audit VALUES ('fired'); END",
    )
    .unwrap();
    conn.execute("PRAGMA capture_data_changes_conn('full')")
        .unwrap();

    conn.execute("INSERT INTO src VALUES (1)").unwrap();

    let rows = limbo_exec_rows(
        &conn,
        "SELECT change_id, change_type, table_name FROM turso_cdc ORDER BY change_id",
    );
    assert_eq!(
        rows,
        vec![
            vec![
                Value::Integer(1),
                Value::Integer(1),
                Value::Text("audit".to_string())
            ],
            vec![Value::Integer(2), Value::Integer(2), Value::Null],
        ],
        "expected one audit INSERT row with change_id 1, then one COMMIT with change_id 2, got {rows:?}"
    );
}

/// Regression test for https://github.com/tursodatabase/turso/issues/10154: under MVCC, a
/// trigger subprogram allocated its CDC change id with the plain `NewRowid` path (the one used
/// when CDC is not running under MVCC at all) instead of the CDC table's AUTOINCREMENT
/// sequence, because `ProgramBuilder::new_for_trigger` did not inherit the parent program's
/// MVCC flag and defaulted it to `false`. The trigger's audit row then landed on the same
/// change id the root statement's own end-of-statement COMMIT record computed from the
/// sequence, and the COMMIT's INSERT silently overwrote the audit row.
#[turso_macros::test(mvcc)]
fn test_cdc_trigger_after_insert_captures_trigger_row(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE audit(msg)").unwrap();
    conn.execute("CREATE TABLE t(x)").unwrap();
    conn.execute(
        "CREATE TRIGGER trg AFTER INSERT ON t BEGIN INSERT INTO audit VALUES ('fired'); END",
    )
    .unwrap();
    conn.execute("PRAGMA capture_data_changes_conn('full')")
        .unwrap();

    conn.execute("INSERT INTO t VALUES (1)").unwrap();

    let rows = limbo_exec_rows(
        &conn,
        "SELECT change_id, change_type, table_name FROM turso_cdc ORDER BY change_id",
    );
    assert_eq!(
        rows,
        vec![
            vec![
                Value::Integer(1),
                Value::Integer(1),
                Value::Text("t".to_string())
            ],
            vec![
                Value::Integer(2),
                Value::Integer(1),
                Value::Text("audit".to_string())
            ],
            vec![Value::Integer(3), Value::Integer(2), Value::Null],
        ],
        "expected the root's own INSERT, the trigger's audit INSERT, then one COMMIT, each with a distinct increasing change id, got {rows:?}"
    );
}

/// Same root cause as `test_cdc_trigger_after_insert_captures_trigger_row`, but for an FK
/// ON DELETE CASCADE action subprogram instead of a trigger: `emit_fk_action_subprogram` built
/// the cascade's `ProgramBuilder` the same way, without inheriting the parent's MVCC flag.
/// Regression test for https://github.com/tursodatabase/turso/issues/10154.
#[turso_macros::test(mvcc)]
fn test_cdc_fk_cascade_delete_captures_child_row(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("PRAGMA foreign_keys = ON").unwrap();
    conn.execute("CREATE TABLE parent(id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute(
        "CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON DELETE CASCADE)",
    )
    .unwrap();
    conn.execute("INSERT INTO parent VALUES (1)").unwrap();
    conn.execute("INSERT INTO child VALUES (10, 1)").unwrap();
    conn.execute("PRAGMA capture_data_changes_conn('full')")
        .unwrap();

    conn.execute("DELETE FROM parent WHERE id = 1").unwrap();

    let rows = limbo_exec_rows(
        &conn,
        "SELECT change_id, change_type, table_name, id FROM turso_cdc ORDER BY change_id",
    );
    assert_eq!(
        rows,
        vec![
            vec![
                Value::Integer(1),
                Value::Integer(-1),
                Value::Text("parent".to_string()),
                Value::Integer(1)
            ],
            vec![
                Value::Integer(2),
                Value::Integer(-1),
                Value::Text("child".to_string()),
                Value::Integer(10)
            ],
            vec![
                Value::Integer(3),
                Value::Integer(2),
                Value::Null,
                Value::Null
            ],
        ],
        "expected the DELETE on parent, the cascade DELETE on child, then one COMMIT, each with a distinct increasing change id, got {rows:?}"
    );
}

/// Non-COMMIT rows in `change_id` order, as (change_type, table_name, id). Each CDC row must
/// appear in the order its write actually happened
/// (https://github.com/tursodatabase/turso/issues/10149).
fn cdc_change_order(conn: &std::sync::Arc<turso_core::Connection>) -> Vec<(i64, String, i64)> {
    limbo_exec_rows(
        conn,
        "SELECT change_type, table_name, id FROM turso_cdc WHERE change_type != 2 ORDER BY change_id",
    )
    .into_iter()
    .map(|row| {
        let Value::Integer(change_type) = row[0] else {
            panic!("expected integer change_type, got {:?}", row[0]);
        };
        let Value::Text(ref table_name) = row[1] else {
            panic!("expected text table_name, got {:?}", row[1]);
        };
        let Value::Integer(id) = row[2] else {
            panic!("expected integer id, got {:?}", row[2]);
        };
        (change_type, table_name.clone(), id)
    })
    .collect()
}

#[turso_macros::test]
fn test_cdc_order_after_insert_trigger_follows_the_insert(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, x)")
        .unwrap();
    conn.execute("CREATE TABLE audit(id INTEGER PRIMARY KEY, msg)")
        .unwrap();
    conn.execute(
        "CREATE TRIGGER t_ai AFTER INSERT ON t BEGIN INSERT INTO audit(msg) VALUES ('inserted ' || new.id); END",
    )
    .unwrap();
    conn.execute("PRAGMA capture_data_changes_conn('full')")
        .unwrap();

    conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();

    assert_eq!(
        cdc_change_order(&conn),
        vec![(1, "t".to_string(), 1), (1, "audit".to_string(), 1)]
    );
}

#[turso_macros::test]
fn test_cdc_order_after_update_trigger_follows_the_update(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, x)")
        .unwrap();
    conn.execute("CREATE TABLE audit(id INTEGER PRIMARY KEY, msg)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    conn.execute(
        "CREATE TRIGGER t_au AFTER UPDATE ON t BEGIN INSERT INTO audit(msg) VALUES ('updated ' || new.id); END",
    )
    .unwrap();
    conn.execute("PRAGMA capture_data_changes_conn('full')")
        .unwrap();

    conn.execute("UPDATE t SET x = 'b' WHERE id = 1").unwrap();

    assert_eq!(
        cdc_change_order(&conn),
        vec![(0, "t".to_string(), 1), (1, "audit".to_string(), 1)]
    );
}

#[turso_macros::test]
fn test_cdc_order_after_delete_trigger_follows_the_delete(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, x)")
        .unwrap();
    conn.execute("CREATE TABLE audit(id INTEGER PRIMARY KEY, msg)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    conn.execute(
        "CREATE TRIGGER t_ad AFTER DELETE ON t BEGIN INSERT INTO audit(msg) VALUES ('deleted ' || old.id); END",
    )
    .unwrap();
    conn.execute("PRAGMA capture_data_changes_conn('full')")
        .unwrap();

    conn.execute("DELETE FROM t WHERE id = 1").unwrap();

    assert_eq!(
        cdc_change_order(&conn),
        vec![(-1, "t".to_string(), 1), (1, "audit".to_string(), 1)]
    );
}

#[turso_macros::test]
fn test_cdc_order_before_insert_trigger_precedes_the_insert(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, x)")
        .unwrap();
    conn.execute("CREATE TABLE log(id INTEGER PRIMARY KEY, msg)")
        .unwrap();
    conn.execute(
        "CREATE TRIGGER t_bi BEFORE INSERT ON t BEGIN INSERT INTO log(msg) VALUES ('before insert'); END",
    )
    .unwrap();
    conn.execute("PRAGMA capture_data_changes_conn('full')")
        .unwrap();

    conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();

    assert_eq!(
        cdc_change_order(&conn),
        vec![(1, "log".to_string(), 1), (1, "t".to_string(), 1)]
    );
}

#[turso_macros::test]
fn test_cdc_order_before_update_trigger_precedes_the_update(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, x)")
        .unwrap();
    conn.execute("CREATE TABLE log(id INTEGER PRIMARY KEY, msg)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    conn.execute(
        "CREATE TRIGGER t_bu BEFORE UPDATE ON t BEGIN INSERT INTO log(msg) VALUES ('before update'); END",
    )
    .unwrap();
    conn.execute("PRAGMA capture_data_changes_conn('full')")
        .unwrap();

    conn.execute("UPDATE t SET x = 'b' WHERE id = 1").unwrap();

    assert_eq!(
        cdc_change_order(&conn),
        vec![(1, "log".to_string(), 1), (0, "t".to_string(), 1)]
    );
}

#[turso_macros::test]
fn test_cdc_order_before_delete_trigger_precedes_the_delete(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, x)")
        .unwrap();
    conn.execute("CREATE TABLE log(id INTEGER PRIMARY KEY, msg)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    conn.execute(
        "CREATE TRIGGER t_bd BEFORE DELETE ON t BEGIN INSERT INTO log(msg) VALUES ('before delete'); END",
    )
    .unwrap();
    conn.execute("PRAGMA capture_data_changes_conn('full')")
        .unwrap();

    conn.execute("DELETE FROM t WHERE id = 1").unwrap();

    assert_eq!(
        cdc_change_order(&conn),
        vec![(1, "log".to_string(), 1), (-1, "t".to_string(), 1)]
    );
}

#[turso_macros::test]
fn test_cdc_order_fk_cascade_delete_parent_before_child(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("PRAGMA foreign_keys = ON").unwrap();
    conn.execute("CREATE TABLE parent(id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute(
        "CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON DELETE CASCADE)",
    )
    .unwrap();
    conn.execute("INSERT INTO parent VALUES (1)").unwrap();
    conn.execute("INSERT INTO child VALUES (10, 1)").unwrap();
    conn.execute("PRAGMA capture_data_changes_conn('full')")
        .unwrap();

    conn.execute("DELETE FROM parent WHERE id = 1").unwrap();

    assert_eq!(
        cdc_change_order(&conn),
        vec![(-1, "parent".to_string(), 1), (-1, "child".to_string(), 10)]
    );
}

#[turso_macros::test]
fn test_cdc_order_fk_cascade_update_parent_before_child(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("PRAGMA foreign_keys = ON").unwrap();
    conn.execute("CREATE TABLE parent(id INTEGER PRIMARY KEY, code TEXT UNIQUE)")
        .unwrap();
    conn.execute(
        "CREATE TABLE child(id INTEGER PRIMARY KEY, pcode TEXT REFERENCES parent(code) ON UPDATE CASCADE)",
    )
    .unwrap();
    conn.execute("INSERT INTO parent VALUES (1, 'a')").unwrap();
    conn.execute("INSERT INTO child VALUES (10, 'a')").unwrap();
    conn.execute("PRAGMA capture_data_changes_conn('full')")
        .unwrap();

    conn.execute("UPDATE parent SET code = 'b' WHERE id = 1")
        .unwrap();

    assert_eq!(
        cdc_change_order(&conn),
        vec![(0, "parent".to_string(), 1), (0, "child".to_string(), 10)]
    );
}

#[turso_macros::test]
fn test_cdc_order_upsert_do_update_fk_cascade_parent_before_child(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("PRAGMA foreign_keys = ON").unwrap();
    conn.execute("CREATE TABLE parent(id INTEGER PRIMARY KEY, code TEXT UNIQUE)")
        .unwrap();
    conn.execute(
        "CREATE TABLE child(id INTEGER PRIMARY KEY, pcode TEXT REFERENCES parent(code) ON UPDATE CASCADE)",
    )
    .unwrap();
    conn.execute("INSERT INTO parent VALUES (1, 'a')").unwrap();
    conn.execute("INSERT INTO child VALUES (10, 'a')").unwrap();
    conn.execute("PRAGMA capture_data_changes_conn('full')")
        .unwrap();

    conn.execute(
        "INSERT INTO parent(id, code) VALUES (1, 'b') ON CONFLICT(id) DO UPDATE SET code = excluded.code",
    )
    .unwrap();

    assert_eq!(
        cdc_change_order(&conn),
        vec![(0, "parent".to_string(), 1), (0, "child".to_string(), 10)]
    );
}

// TODO: cannot use mvcc, FK CASCADE writes are not captured by CDC under mvcc at all
#[turso_macros::test()]
fn test_cdc_order_update_rowid_change_fk_cascade_parent_before_child(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("PRAGMA foreign_keys = ON").unwrap();
    conn.execute("CREATE TABLE parent(id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute(
        "CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON UPDATE CASCADE)",
    )
    .unwrap();
    conn.execute("INSERT INTO parent VALUES (1)").unwrap();
    conn.execute("INSERT INTO child VALUES (10, 1)").unwrap();
    conn.execute("PRAGMA capture_data_changes_conn('full')")
        .unwrap();

    conn.execute("UPDATE parent SET id = 2 WHERE id = 1")
        .unwrap();

    assert_eq!(
        cdc_change_order(&conn),
        vec![
            (-1, "parent".to_string(), 1),
            (1, "parent".to_string(), 2),
            (0, "child".to_string(), 10),
        ]
    );
}

// TODO: cannot use mvcc, FK CASCADE writes are not captured by CDC under mvcc at all
#[turso_macros::test()]
fn test_cdc_order_upsert_do_update_rowid_change_fk_cascade_parent_before_child(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("PRAGMA foreign_keys = ON").unwrap();
    conn.execute("CREATE TABLE parent(id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute(
        "CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON UPDATE CASCADE)",
    )
    .unwrap();
    conn.execute("INSERT INTO parent VALUES (1)").unwrap();
    conn.execute("INSERT INTO child VALUES (10, 1)").unwrap();
    conn.execute("PRAGMA capture_data_changes_conn('full')")
        .unwrap();

    conn.execute("INSERT INTO parent(id) VALUES (1) ON CONFLICT(id) DO UPDATE SET id = 2")
        .unwrap();

    assert_eq!(
        cdc_change_order(&conn),
        vec![
            (-1, "parent".to_string(), 1),
            (1, "parent".to_string(), 2),
            (0, "child".to_string(), 10),
        ]
    );
}
