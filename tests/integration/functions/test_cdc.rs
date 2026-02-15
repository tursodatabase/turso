use std::sync::Arc;

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
            vec![
                Value::Integer(1),
                Value::Null,
                Value::Integer(1),
                Value::Text("t".to_string()),
                Value::Integer(10),
                Value::Null,
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
            vec![
                Value::Integer(1),
                Value::Null,
                Value::Integer(1),
                Value::Text("t".to_string()),
                Value::Integer(20),
                Value::Null,
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
                Value::Null,
            ],
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
                Value::Integer(6),
                Value::Null,
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
                Value::Null,
            ],
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
            vec![
                Value::Integer(1),
                Value::Null,
                Value::Integer(1),
                Value::Text("t".to_string()),
                Value::Integer(2),
                Value::Null,
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
                Value::Null,
            ],
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
        vec![vec![
            Value::Integer(2),
            Value::Null,
            Value::Integer(1),
            Value::Text("t".to_string()),
            Value::Integer(2),
            Value::Null,
            Value::Null,
            Value::Null,
        ],]
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
                Value::Text("q".to_string()),
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
            vec![
                Value::Integer(4),
                Value::Null,
                Value::Integer(-1),
                Value::Text("t".to_string()),
                Value::Integer(1),
                Value::Null,
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
                Value::Null,
            ],
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
            Value::Null,
        ]]
    );
    let rows = replace_column_with_null(limbo_exec_rows(&conn1, "SELECT * FROM custom_cdc2"), 1);
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
            Value::Null,
        ]]
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
    let rows = replace_column_with_null(limbo_exec_rows(&conn1, "SELECT * FROM custom_cdc1"), 1);
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
                Value::Null,
            ]
        ]
    );
    let rows = replace_column_with_null(limbo_exec_rows(&conn2, "SELECT * FROM custom_cdc2"), 1);
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
                Value::Null,
            ]
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

    // turso_cdc_version table + its auto-index add 2 entries to sqlite_schema,
    // shifting rowids and rootpages by +2 compared to before version tracking
    assert_eq!(
        rows,
        vec![
            vec![
                Value::Integer(1),
                Value::Null,
                Value::Integer(1),
                Value::Text("sqlite_schema".to_string()),
                Value::Integer(5),
                Value::Null,
                Value::Blob(record([
                    Value::Text("table".to_string()),
                    Value::Text("t".to_string()),
                    Value::Text("t".to_string()),
                    Value::Integer(6),
                    Value::Text(
                        "CREATE TABLE t (x, y, z UNIQUE, q, PRIMARY KEY (x, y))".to_string()
                    )
                ])),
                Value::Null,
            ],
            vec![
                Value::Integer(2),
                Value::Null,
                Value::Integer(1),
                Value::Text("sqlite_schema".to_string()),
                Value::Integer(8),
                Value::Null,
                Value::Blob(record([
                    Value::Text("table".to_string()),
                    Value::Text("q".to_string()),
                    Value::Text("q".to_string()),
                    Value::Integer(9),
                    Value::Text("CREATE TABLE q (a, b, c)".to_string())
                ])),
                Value::Null,
            ],
            vec![
                Value::Integer(3),
                Value::Null,
                Value::Integer(1),
                Value::Text("sqlite_schema".to_string()),
                Value::Integer(9),
                Value::Null,
                Value::Blob(record([
                    Value::Text("index".to_string()),
                    Value::Text("t_q".to_string()),
                    Value::Text("t".to_string()),
                    Value::Integer(10),
                    Value::Text("CREATE INDEX t_q ON t (q)".to_string())
                ])),
                Value::Null,
            ],
            vec![
                Value::Integer(4),
                Value::Null,
                Value::Integer(1),
                Value::Text("sqlite_schema".to_string()),
                Value::Integer(10),
                Value::Null,
                Value::Blob(record([
                    Value::Text("index".to_string()),
                    Value::Text("q_abc".to_string()),
                    Value::Text("q".to_string()),
                    Value::Integer(11),
                    Value::Text("CREATE INDEX q_abc ON q (a, b, c)".to_string())
                ])),
                Value::Null,
            ],
            vec![
                Value::Integer(5),
                Value::Null,
                Value::Integer(-1),
                Value::Text("sqlite_schema".to_string()),
                Value::Integer(5),
                Value::Blob(record([
                    Value::Text("table".to_string()),
                    Value::Text("t".to_string()),
                    Value::Text("t".to_string()),
                    Value::Integer(6),
                    Value::Text(
                        "CREATE TABLE t (x, y, z UNIQUE, q, PRIMARY KEY (x, y))".to_string()
                    )
                ])),
                Value::Null,
                Value::Null,
            ],
            vec![
                Value::Integer(6),
                Value::Null,
                Value::Integer(-1),
                Value::Text("sqlite_schema".to_string()),
                Value::Integer(10),
                Value::Blob(record([
                    Value::Text("index".to_string()),
                    Value::Text("q_abc".to_string()),
                    Value::Text("q".to_string()),
                    Value::Integer(11),
                    Value::Text("CREATE INDEX q_abc ON q (a, b, c)".to_string())
                ])),
                Value::Null,
                Value::Null,
            ]
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

    // turso_cdc_version table + its auto-index add 2 entries to sqlite_schema,
    // shifting rowids and rootpages by +2 compared to before version tracking
    assert_eq!(
        rows,
        vec![
            vec![
                Value::Integer(1),
                Value::Null,
                Value::Integer(1),
                Value::Text("sqlite_schema".to_string()),
                Value::Integer(5),
                Value::Null,
                Value::Blob(record([
                    Value::Text("table".to_string()),
                    Value::Text("t".to_string()),
                    Value::Text("t".to_string()),
                    Value::Integer(6),
                    Value::Text(
                        "CREATE TABLE t (x, y, z UNIQUE, q, PRIMARY KEY (x, y))".to_string()
                    )
                ])),
                Value::Null,
            ],
            vec![
                Value::Integer(2),
                Value::Null,
                Value::Integer(0),
                Value::Text("sqlite_schema".to_string()),
                Value::Integer(5),
                Value::Blob(record([
                    Value::Text("table".to_string()),
                    Value::Text("t".to_string()),
                    Value::Text("t".to_string()),
                    Value::Integer(6),
                    Value::Text(
                        "CREATE TABLE t (x, y, z UNIQUE, q, PRIMARY KEY (x, y))".to_string()
                    )
                ])),
                Value::Blob(record([
                    Value::Text("table".to_string()),
                    Value::Text("t".to_string()),
                    Value::Text("t".to_string()),
                    Value::Integer(6),
                    Value::Text("CREATE TABLE t (x, y, z UNIQUE, PRIMARY KEY (x, y))".to_string())
                ])),
                Value::Blob(record([
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
            ],
            vec![
                Value::Integer(3),
                Value::Null,
                Value::Integer(0),
                Value::Text("sqlite_schema".to_string()),
                Value::Integer(5),
                Value::Blob(record([
                    Value::Text("table".to_string()),
                    Value::Text("t".to_string()),
                    Value::Text("t".to_string()),
                    Value::Integer(6),
                    Value::Text("CREATE TABLE t (x, y, z UNIQUE, PRIMARY KEY (x, y))".to_string())
                ])),
                Value::Blob(record([
                    Value::Text("table".to_string()),
                    Value::Text("t".to_string()),
                    Value::Text("t".to_string()),
                    Value::Integer(6),
                    Value::Text(
                        "CREATE TABLE t (x, y, z UNIQUE, t, PRIMARY KEY (x, y))".to_string()
                    )
                ])),
                Value::Blob(record([
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
            ],
        ]
    );
}

#[turso_macros::test(mvcc)]
fn test_cdc_version_table_created(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t (x INTEGER PRIMARY KEY, y)")
        .unwrap();
    conn.execute("PRAGMA unstable_capture_data_changes_conn('full')")
        .unwrap();
    let rows = limbo_exec_rows(
        &conn,
        "SELECT table_name, version FROM turso_cdc_version",
    );
    assert_eq!(
        rows,
        vec![vec![
            Value::Text("turso_cdc".to_string()),
            Value::Text("v1".to_string()),
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
    let rows = limbo_exec_rows(
        &conn,
        "SELECT table_name, version FROM turso_cdc_version",
    );
    assert_eq!(
        rows,
        vec![vec![
            Value::Text("my_cdc".to_string()),
            Value::Text("v1".to_string()),
        ]]
    );
}

#[turso_macros::test(mvcc)]
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
    let rows = limbo_exec_rows(
        &conn,
        "SELECT table_name, version FROM turso_cdc_version",
    );
    assert_eq!(
        rows,
        vec![vec![
            Value::Text("turso_cdc".to_string()),
            Value::Text("v1".to_string()),
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
    conn.execute(
        "INSERT INTO turso_cdc_version (table_name, version) VALUES ('turso_cdc', 'v1')",
    )
    .unwrap();

    // Enable CDC — table already exists so InitCdcVersion opcode is NOT emitted
    conn.execute(format!("PRAGMA unstable_capture_data_changes_conn('{mode}')"))
        .unwrap();

    // Verify version table is unchanged
    let rows = limbo_exec_rows(
        &conn,
        "SELECT table_name, version FROM turso_cdc_version",
    );
    assert_eq!(
        rows,
        vec![vec![
            Value::Text("turso_cdc".to_string()),
            Value::Text("v1".to_string()),
        ]]
    );

    // Perform insert, update, delete
    conn.execute("INSERT INTO t VALUES (1, 2), (3, 4)")
        .unwrap();
    conn.execute("UPDATE t SET y = 3 WHERE x = 1").unwrap();
    conn.execute("DELETE FROM t WHERE x = 3").unwrap();
    conn.execute("DELETE FROM t WHERE x = 1").unwrap();

    conn
}

#[turso_macros::test(mvcc)]
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

#[turso_macros::test(mvcc)]
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

#[turso_macros::test(mvcc)]
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

#[turso_macros::test(mvcc)]
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
