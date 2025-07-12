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

#[test]
fn test_cdc_simple_id() {
    let db = TempDatabase::new_empty(false);
    let conn = db.connect_limbo();
    conn.execute("PRAGMA unstable_capture_data_changes_conn('id')")
        .unwrap();
    conn.execute("CREATE TABLE t(x INTEGER PRIMARY KEY, y)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (10, 10), (5, 1)")
        .unwrap();
    let rows = limbo_exec_rows(&db, &conn, "SELECT * FROM t");
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(5), Value::Integer(1)],
            vec![Value::Integer(10), Value::Integer(10)],
        ]
    );
    let rows = replace_column_with_null(limbo_exec_rows(&db, &conn, "SELECT * FROM turso_cdc"), 1);
    assert_eq!(
        rows,
        vec![
            vec![
                Value::Integer(1),
                Value::Null,
                Value::Integer(1),
                Value::Text("t".to_string()),
                Value::Integer(10),
                Value::Null,
                Value::Null,
            ],
            vec![
                Value::Integer(2),
                Value::Null,
                Value::Integer(1),
                Value::Text("t".to_string()),
                Value::Integer(5),
                Value::Null,
                Value::Null,
            ]
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
            Value::Text(x) => turso_core::Value::Text(turso_core::types::Text::new(&x)),
            Value::Blob(x) => turso_core::Value::Blob(x),
        })
        .collect::<Vec<_>>();
    ImmutableRecord::from_values(&values, values.len())
        .get_payload()
        .to_vec()
}

#[test]
fn test_cdc_simple_before() {
    let db = TempDatabase::new_empty(false);
    let conn = db.connect_limbo();
    conn.execute("PRAGMA unstable_capture_data_changes_conn('before')")
        .unwrap();
    conn.execute("CREATE TABLE t(x INTEGER PRIMARY KEY, y)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 2), (3, 4)").unwrap();
    conn.execute("UPDATE t SET y = 3 WHERE x = 1").unwrap();
    conn.execute("DELETE FROM t WHERE x = 3").unwrap();
    conn.execute("DELETE FROM t WHERE x = 1").unwrap();
    let rows = replace_column_with_null(limbo_exec_rows(&db, &conn, "SELECT * FROM turso_cdc"), 1);

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
            ],
            vec![
                Value::Integer(2),
                Value::Null,
                Value::Integer(1),
                Value::Text("t".to_string()),
                Value::Integer(3),
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
            ],
            vec![
                Value::Integer(4),
                Value::Null,
                Value::Integer(-1),
                Value::Text("t".to_string()),
                Value::Integer(3),
                Value::Blob(record([Value::Integer(3), Value::Integer(4)])),
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
            ]
        ]
    );
}

#[test]
fn test_cdc_simple_after() {
    let db = TempDatabase::new_empty(false);
    let conn = db.connect_limbo();
    conn.execute("PRAGMA unstable_capture_data_changes_conn('after')")
        .unwrap();
    conn.execute("CREATE TABLE t(x INTEGER PRIMARY KEY, y)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 2), (3, 4)").unwrap();
    conn.execute("UPDATE t SET y = 3 WHERE x = 1").unwrap();
    conn.execute("DELETE FROM t WHERE x = 3").unwrap();
    conn.execute("DELETE FROM t WHERE x = 1").unwrap();
    let rows = replace_column_with_null(limbo_exec_rows(&db, &conn, "SELECT * FROM turso_cdc"), 1);

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
            ],
            vec![
                Value::Integer(2),
                Value::Null,
                Value::Integer(1),
                Value::Text("t".to_string()),
                Value::Integer(3),
                Value::Null,
                Value::Blob(record([Value::Integer(3), Value::Integer(4)])),
            ],
            vec![
                Value::Integer(3),
                Value::Null,
                Value::Integer(0),
                Value::Text("t".to_string()),
                Value::Integer(1),
                Value::Null,
                Value::Blob(record([Value::Integer(1), Value::Integer(3)])),
            ],
            vec![
                Value::Integer(4),
                Value::Null,
                Value::Integer(-1),
                Value::Text("t".to_string()),
                Value::Integer(3),
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
            ]
        ]
    );
}

#[test]
fn test_cdc_simple_full() {
    let db = TempDatabase::new_empty(false);
    let conn = db.connect_limbo();
    conn.execute("PRAGMA unstable_capture_data_changes_conn('full')")
        .unwrap();
    conn.execute("CREATE TABLE t(x INTEGER PRIMARY KEY, y)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 2), (3, 4)").unwrap();
    conn.execute("UPDATE t SET y = 3 WHERE x = 1").unwrap();
    conn.execute("DELETE FROM t WHERE x = 3").unwrap();
    conn.execute("DELETE FROM t WHERE x = 1").unwrap();
    let rows = replace_column_with_null(limbo_exec_rows(&db, &conn, "SELECT * FROM turso_cdc"), 1);

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
            ],
            vec![
                Value::Integer(2),
                Value::Null,
                Value::Integer(1),
                Value::Text("t".to_string()),
                Value::Integer(3),
                Value::Null,
                Value::Blob(record([Value::Integer(3), Value::Integer(4)])),
            ],
            vec![
                Value::Integer(3),
                Value::Null,
                Value::Integer(0),
                Value::Text("t".to_string()),
                Value::Integer(1),
                Value::Blob(record([Value::Integer(1), Value::Integer(2)])),
                Value::Blob(record([Value::Integer(1), Value::Integer(3)])),
            ],
            vec![
                Value::Integer(4),
                Value::Null,
                Value::Integer(-1),
                Value::Text("t".to_string()),
                Value::Integer(3),
                Value::Blob(record([Value::Integer(3), Value::Integer(4)])),
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
            ]
        ]
    );
}

#[test]
fn test_cdc_crud() {
    let db = TempDatabase::new_empty(false);
    let conn = db.connect_limbo();
    conn.execute("PRAGMA unstable_capture_data_changes_conn('id')")
        .unwrap();
    conn.execute("CREATE TABLE t(x INTEGER PRIMARY KEY, y)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (20, 20), (10, 10), (5, 1)")
        .unwrap();
    conn.execute("UPDATE t SET y = 100 WHERE x = 5").unwrap();
    conn.execute("DELETE FROM t WHERE x > 5").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 1)").unwrap();
    conn.execute("UPDATE t SET x = 2 WHERE x = 1").unwrap();

    let rows = limbo_exec_rows(&db, &conn, "SELECT * FROM t");
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(2), Value::Integer(1)],
            vec![Value::Integer(5), Value::Integer(100)],
        ]
    );
    let rows = replace_column_with_null(limbo_exec_rows(&db, &conn, "SELECT * FROM turso_cdc"), 1);
    assert_eq!(
        rows,
        vec![
            vec![
                Value::Integer(1),
                Value::Null,
                Value::Integer(1),
                Value::Text("t".to_string()),
                Value::Integer(20),
                Value::Null,
                Value::Null,
            ],
            vec![
                Value::Integer(2),
                Value::Null,
                Value::Integer(1),
                Value::Text("t".to_string()),
                Value::Integer(10),
                Value::Null,
                Value::Null,
            ],
            vec![
                Value::Integer(3),
                Value::Null,
                Value::Integer(1),
                Value::Text("t".to_string()),
                Value::Integer(5),
                Value::Null,
                Value::Null,
            ],
            vec![
                Value::Integer(4),
                Value::Null,
                Value::Integer(0),
                Value::Text("t".to_string()),
                Value::Integer(5),
                Value::Null,
                Value::Null,
            ],
            vec![
                Value::Integer(5),
                Value::Null,
                Value::Integer(-1),
                Value::Text("t".to_string()),
                Value::Integer(10),
                Value::Null,
                Value::Null,
            ],
            vec![
                Value::Integer(6),
                Value::Null,
                Value::Integer(-1),
                Value::Text("t".to_string()),
                Value::Integer(20),
                Value::Null,
                Value::Null,
            ],
            vec![
                Value::Integer(7),
                Value::Null,
                Value::Integer(1),
                Value::Text("t".to_string()),
                Value::Integer(1),
                Value::Null,
                Value::Null,
            ],
            vec![
                Value::Integer(8),
                Value::Null,
                Value::Integer(-1),
                Value::Text("t".to_string()),
                Value::Integer(1),
                Value::Null,
                Value::Null,
            ],
            vec![
                Value::Integer(9),
                Value::Null,
                Value::Integer(1),
                Value::Text("t".to_string()),
                Value::Integer(2),
                Value::Null,
                Value::Null,
            ],
        ]
    );
}

#[test]
fn test_cdc_failed_op() {
    let db = TempDatabase::new_empty(true);
    let conn = db.connect_limbo();
    conn.execute("PRAGMA unstable_capture_data_changes_conn('id')")
        .unwrap();
    conn.execute("CREATE TABLE t(x INTEGER PRIMARY KEY, y UNIQUE)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10), (2, 20)")
        .unwrap();
    assert!(conn
        .execute("INSERT INTO t VALUES (3, 30), (4, 40), (5, 10)")
        .is_err());
    conn.execute("INSERT INTO t VALUES (6, 60), (7, 70)")
        .unwrap();

    let rows = limbo_exec_rows(&db, &conn, "SELECT * FROM t");
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(1), Value::Integer(10)],
            vec![Value::Integer(2), Value::Integer(20)],
            vec![Value::Integer(6), Value::Integer(60)],
            vec![Value::Integer(7), Value::Integer(70)],
        ]
    );
    let rows = replace_column_with_null(limbo_exec_rows(&db, &conn, "SELECT * FROM turso_cdc"), 1);
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
            ],
            vec![
                Value::Integer(2),
                Value::Null,
                Value::Integer(1),
                Value::Text("t".to_string()),
                Value::Integer(2),
                Value::Null,
                Value::Null,
            ],
            vec![
                Value::Integer(3),
                Value::Null,
                Value::Integer(1),
                Value::Text("t".to_string()),
                Value::Integer(6),
                Value::Null,
                Value::Null,
            ],
            vec![
                Value::Integer(4),
                Value::Null,
                Value::Integer(1),
                Value::Text("t".to_string()),
                Value::Integer(7),
                Value::Null,
                Value::Null,
            ],
        ]
    );
}

#[test]
fn test_cdc_uncaptured_connection() {
    let db = TempDatabase::new_empty(true);
    let conn1 = db.connect_limbo();
    conn1
        .execute("CREATE TABLE t(x INTEGER PRIMARY KEY, y UNIQUE)")
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

    let rows = limbo_exec_rows(&db, &conn1, "SELECT * FROM t");
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
    let rows = replace_column_with_null(limbo_exec_rows(&db, &conn1, "SELECT * FROM turso_cdc"), 1);
    assert_eq!(
        rows,
        vec![
            vec![
                Value::Integer(1),
                Value::Null,
                Value::Integer(1),
                Value::Text("t".to_string()),
                Value::Integer(2),
                Value::Null,
                Value::Null,
            ],
            vec![
                Value::Integer(2),
                Value::Null,
                Value::Integer(1),
                Value::Text("t".to_string()),
                Value::Integer(4),
                Value::Null,
                Value::Null,
            ],
            vec![
                Value::Integer(3),
                Value::Null,
                Value::Integer(1),
                Value::Text("t".to_string()),
                Value::Integer(6),
                Value::Null,
                Value::Null,
            ],
        ]
    );
}

#[test]
fn test_cdc_custom_table() {
    let db = TempDatabase::new_empty(true);
    let conn1 = db.connect_limbo();
    conn1
        .execute("CREATE TABLE t(x INTEGER PRIMARY KEY, y UNIQUE)")
        .unwrap();
    conn1
        .execute("PRAGMA unstable_capture_data_changes_conn('id,custom_cdc')")
        .unwrap();
    conn1.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    conn1.execute("INSERT INTO t VALUES (2, 20)").unwrap();
    let rows = limbo_exec_rows(&db, &conn1, "SELECT * FROM t");
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(1), Value::Integer(10)],
            vec![Value::Integer(2), Value::Integer(20)],
        ]
    );
    let rows =
        replace_column_with_null(limbo_exec_rows(&db, &conn1, "SELECT * FROM custom_cdc"), 1);
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
            ],
            vec![
                Value::Integer(2),
                Value::Null,
                Value::Integer(1),
                Value::Text("t".to_string()),
                Value::Integer(2),
                Value::Null,
                Value::Null,
            ],
        ]
    );
}

#[test]
fn test_cdc_ignore_changes_in_cdc_table() {
    let db = TempDatabase::new_empty(true);
    let conn1 = db.connect_limbo();
    conn1
        .execute("CREATE TABLE t(x INTEGER PRIMARY KEY, y UNIQUE)")
        .unwrap();
    conn1
        .execute("PRAGMA unstable_capture_data_changes_conn('id,custom_cdc')")
        .unwrap();
    conn1.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    conn1.execute("INSERT INTO t VALUES (2, 20)").unwrap();
    let rows = limbo_exec_rows(&db, &conn1, "SELECT * FROM t");
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(1), Value::Integer(10)],
            vec![Value::Integer(2), Value::Integer(20)],
        ]
    );
    conn1
        .execute("DELETE FROM custom_cdc WHERE operation_id < 2")
        .unwrap();
    let rows =
        replace_column_with_null(limbo_exec_rows(&db, &conn1, "SELECT * FROM custom_cdc"), 1);
    assert_eq!(
        rows,
        vec![vec![
            Value::Integer(2),
            Value::Null,
            Value::Integer(1),
            Value::Text("t".to_string()),
            Value::Integer(2),
            Value::Null,
            Value::Null,
        ],]
    );
}

#[test]
fn test_cdc_transaction() {
    let db = TempDatabase::new_empty(true);
    let conn1 = db.connect_limbo();
    conn1
        .execute("CREATE TABLE t(x INTEGER PRIMARY KEY, y UNIQUE)")
        .unwrap();
    conn1
        .execute("CREATE TABLE q(x INTEGER PRIMARY KEY, y UNIQUE)")
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
    let rows = limbo_exec_rows(&db, &conn1, "SELECT * FROM t");
    assert_eq!(rows, vec![vec![Value::Integer(3), Value::Integer(30)],]);
    let rows = limbo_exec_rows(&db, &conn1, "SELECT * FROM q");
    assert_eq!(rows, vec![vec![Value::Integer(2), Value::Integer(200)],]);
    let rows =
        replace_column_with_null(limbo_exec_rows(&db, &conn1, "SELECT * FROM custom_cdc"), 1);
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
            ],
            vec![
                Value::Integer(2),
                Value::Null,
                Value::Integer(1),
                Value::Text("q".to_string()),
                Value::Integer(2),
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
            ],
            vec![
                Value::Integer(4),
                Value::Null,
                Value::Integer(-1),
                Value::Text("t".to_string()),
                Value::Integer(1),
                Value::Null,
                Value::Null,
            ],
            vec![
                Value::Integer(5),
                Value::Null,
                Value::Integer(0),
                Value::Text("q".to_string()),
                Value::Integer(2),
                Value::Null,
                Value::Null,
            ],
        ]
    );
}

#[test]
fn test_cdc_independent_connections() {
    let db = TempDatabase::new_empty(true);
    let conn1 = db.connect_limbo();
    let conn2 = db.connect_limbo();
    conn1
        .execute("PRAGMA unstable_capture_data_changes_conn('id,custom_cdc1')")
        .unwrap();
    conn2
        .execute("PRAGMA unstable_capture_data_changes_conn('id,custom_cdc2')")
        .unwrap();
    conn1
        .execute("CREATE TABLE t(x INTEGER PRIMARY KEY, y UNIQUE)")
        .unwrap();
    conn1.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    conn2.execute("INSERT INTO t VALUES (2, 20)").unwrap();
    let rows = limbo_exec_rows(&db, &conn1, "SELECT * FROM t");
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(1), Value::Integer(10)],
            vec![Value::Integer(2), Value::Integer(20)]
        ]
    );
    let rows =
        replace_column_with_null(limbo_exec_rows(&db, &conn1, "SELECT * FROM custom_cdc1"), 1);
    assert_eq!(
        rows,
        vec![vec![
            Value::Integer(1),
            Value::Null,
            Value::Integer(1),
            Value::Text("t".to_string()),
            Value::Integer(1),
            Value::Null,
            Value::Null,
        ]]
    );
    let rows =
        replace_column_with_null(limbo_exec_rows(&db, &conn1, "SELECT * FROM custom_cdc2"), 1);
    assert_eq!(
        rows,
        vec![vec![
            Value::Integer(1),
            Value::Null,
            Value::Integer(1),
            Value::Text("t".to_string()),
            Value::Integer(2),
            Value::Null,
            Value::Null,
        ]]
    );
}

#[test]
fn test_cdc_independent_connections_different_cdc_not_ignore() {
    let db = TempDatabase::new_empty(true);
    let conn1 = db.connect_limbo();
    let conn2 = db.connect_limbo();
    conn1
        .execute("PRAGMA unstable_capture_data_changes_conn('id,custom_cdc1')")
        .unwrap();
    conn2
        .execute("PRAGMA unstable_capture_data_changes_conn('id,custom_cdc2')")
        .unwrap();
    conn1
        .execute("CREATE TABLE t(x INTEGER PRIMARY KEY, y UNIQUE)")
        .unwrap();
    conn1.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    conn1.execute("INSERT INTO t VALUES (2, 20)").unwrap();
    conn2.execute("INSERT INTO t VALUES (3, 30)").unwrap();
    conn2.execute("INSERT INTO t VALUES (4, 40)").unwrap();
    conn1
        .execute("DELETE FROM custom_cdc2 WHERE operation_id < 2")
        .unwrap();
    conn2
        .execute("DELETE FROM custom_cdc1 WHERE operation_id < 2")
        .unwrap();
    let rows = limbo_exec_rows(&db, &conn1, "SELECT * FROM t");
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(1), Value::Integer(10)],
            vec![Value::Integer(2), Value::Integer(20)],
            vec![Value::Integer(3), Value::Integer(30)],
            vec![Value::Integer(4), Value::Integer(40)],
        ]
    );
    let rows =
        replace_column_with_null(limbo_exec_rows(&db, &conn1, "SELECT * FROM custom_cdc1"), 1);
    assert_eq!(
        rows,
        vec![
            vec![
                Value::Integer(2),
                Value::Null,
                Value::Integer(1),
                Value::Text("t".to_string()),
                Value::Integer(2),
                Value::Null,
                Value::Null,
            ],
            vec![
                Value::Integer(3),
                Value::Null,
                Value::Integer(-1),
                Value::Text("custom_cdc2".to_string()),
                Value::Integer(1),
                Value::Null,
                Value::Null,
            ]
        ]
    );
    let rows =
        replace_column_with_null(limbo_exec_rows(&db, &conn2, "SELECT * FROM custom_cdc2"), 1);
    assert_eq!(
        rows,
        vec![
            vec![
                Value::Integer(2),
                Value::Null,
                Value::Integer(1),
                Value::Text("t".to_string()),
                Value::Integer(4),
                Value::Null,
                Value::Null,
            ],
            vec![
                Value::Integer(3),
                Value::Null,
                Value::Integer(-1),
                Value::Text("custom_cdc1".to_string()),
                Value::Integer(1),
                Value::Null,
                Value::Null,
            ]
        ]
    );
}

#[test]
fn test_cdc_table_columns() {
    let db = TempDatabase::new_empty(true);
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY, b, c UNIQUE)")
        .unwrap();
    let rows = limbo_exec_rows(&db, &conn, "SELECT table_columns_json_array('t')");
    assert_eq!(
        rows,
        vec![vec![Value::Text(r#"["a","b","c"]"#.to_string())]]
    );
    conn.execute("ALTER TABLE t DROP COLUMN b").unwrap();
    let rows = limbo_exec_rows(&db, &conn, "SELECT table_columns_json_array('t')");
    assert_eq!(rows, vec![vec![Value::Text(r#"["a","c"]"#.to_string())]]);
}

#[test]
fn test_cdc_bin_record() {
    let db = TempDatabase::new_empty(true);
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
        record_hex.push_str(&format!("{:02X}", byte));
    }

    let rows = limbo_exec_rows(
        &db,
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
