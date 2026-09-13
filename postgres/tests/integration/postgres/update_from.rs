use crate::common::TempDatabase;
use turso_core::{Numeric, StepResult, Value};
use turso_pg::PgConnection;

fn setup_tables(conn: &PgConnection) {
    conn.execute("CREATE TABLE t1 (id integer PRIMARY KEY, a integer, b text)")
        .unwrap();
    conn.execute("CREATE TABLE t2 (id integer PRIMARY KEY, x integer, y text)")
        .unwrap();
    conn.execute("INSERT INTO t1 VALUES (1, 10, 'one')")
        .unwrap();
    conn.execute("INSERT INTO t1 VALUES (2, 20, 'two')")
        .unwrap();
    conn.execute("INSERT INTO t1 VALUES (3, 30, 'three')")
        .unwrap();
    conn.execute("INSERT INTO t2 VALUES (1, 100, 'alpha')")
        .unwrap();
    conn.execute("INSERT INTO t2 VALUES (2, 200, 'beta')")
        .unwrap();
}

fn query_integer(conn: &PgConnection, sql: &str) -> Vec<i64> {
    let mut rows = conn.query(sql).unwrap().unwrap();
    let mut result = Vec::new();
    loop {
        match rows.step().unwrap() {
            StepResult::Row => {
                let row = rows.row().unwrap();
                match row.get_value(0) {
                    Value::Numeric(Numeric::Integer(v)) => result.push(*v),
                    Value::Null => result.push(-999), // sentinel for NULL
                    other => panic!("expected integer, got {other:?}"),
                }
            }
            StepResult::Done => break,
            _ => {}
        }
    }
    result
}

fn query_text(conn: &PgConnection, sql: &str) -> Vec<String> {
    let mut rows = conn.query(sql).unwrap().unwrap();
    let mut result = Vec::new();
    loop {
        match rows.step().unwrap() {
            StepResult::Row => {
                let row = rows.row().unwrap();
                match row.get_value(0) {
                    Value::Text(v) => result.push(v.value.to_string()),
                    Value::Null => result.push("NULL".to_string()),
                    other => panic!("expected text, got {other:?}"),
                }
            }
            StepResult::Done => break,
            _ => {}
        }
    }
    result
}

#[turso_macros::test(mvcc)]
fn test_pg_update_from_basic(db: TempDatabase) {
    let conn = db.connect_postgres();
    setup_tables(&conn);

    conn.execute("UPDATE t1 SET a = t2.x FROM t2 WHERE t1.id = t2.id")
        .unwrap();

    let vals = query_integer(&conn, "SELECT a FROM t1 ORDER BY id");
    assert_eq!(vals, vec![100, 200, 30]); // id=3 has no match, unchanged
}

#[turso_macros::test(mvcc)]
fn test_pg_update_from_multi_column(db: TempDatabase) {
    let conn = db.connect_postgres();
    setup_tables(&conn);

    conn.execute("UPDATE t1 SET a = t2.x, b = t2.y FROM t2 WHERE t1.id = t2.id")
        .unwrap();

    let vals = query_integer(&conn, "SELECT a FROM t1 ORDER BY id");
    assert_eq!(vals, vec![100, 200, 30]);

    let texts = query_text(&conn, "SELECT b FROM t1 ORDER BY id");
    assert_eq!(texts, vec!["alpha", "beta", "three"]);
}

#[turso_macros::test(mvcc)]
fn test_pg_update_from_with_expression(db: TempDatabase) {
    let conn = db.connect_postgres();
    setup_tables(&conn);

    conn.execute("UPDATE t1 SET a = t1.a + t2.x FROM t2 WHERE t1.id = t2.id")
        .unwrap();

    let vals = query_integer(&conn, "SELECT a FROM t1 ORDER BY id");
    assert_eq!(vals, vec![110, 220, 30]); // 10+100, 20+200, unchanged
}

#[turso_macros::test(mvcc)]
fn test_pg_update_from_no_match(db: TempDatabase) {
    let conn = db.connect_postgres();
    setup_tables(&conn);

    // Update with a condition that matches nothing
    conn.execute("UPDATE t1 SET a = t2.x FROM t2 WHERE t1.id = t2.id AND t2.id = 999")
        .unwrap();

    let vals = query_integer(&conn, "SELECT a FROM t1 ORDER BY id");
    assert_eq!(vals, vec![10, 20, 30]); // all unchanged
}

#[turso_macros::test(mvcc)]
fn test_pg_update_from_subquery(db: TempDatabase) {
    let conn = db.connect_postgres();
    setup_tables(&conn);

    conn.execute(
        "UPDATE t1 SET a = sub.total FROM (SELECT id, x * 2 AS total FROM t2) AS sub WHERE t1.id = sub.id",
    )
    .unwrap();

    let vals = query_integer(&conn, "SELECT a FROM t1 ORDER BY id");
    assert_eq!(vals, vec![200, 400, 30]); // 100*2, 200*2, unchanged
}

#[turso_macros::test(mvcc)]
fn test_pg_update_from_multiple_tables(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE t1 (id integer PRIMARY KEY, val integer)")
        .unwrap();
    conn.execute("CREATE TABLE t2 (id integer PRIMARY KEY, factor integer)")
        .unwrap();
    conn.execute("CREATE TABLE t3 (id integer PRIMARY KEY, bonus integer)")
        .unwrap();

    conn.execute("INSERT INTO t1 VALUES (1, 10)").unwrap();
    conn.execute("INSERT INTO t2 VALUES (1, 5)").unwrap();
    conn.execute("INSERT INTO t3 VALUES (1, 3)").unwrap();

    conn.execute(
        "UPDATE t1 SET val = t2.factor + t3.bonus FROM t2, t3 WHERE t1.id = t2.id AND t1.id = t3.id",
    )
    .unwrap();

    let vals = query_integer(&conn, "SELECT val FROM t1");
    assert_eq!(vals, vec![8]); // 5 + 3
}

#[turso_macros::test(mvcc)]
fn test_pg_update_from_returning(db: TempDatabase) {
    let conn = db.connect_postgres();
    setup_tables(&conn);

    let mut rows = conn
        .query("UPDATE t1 SET a = t2.x FROM t2 WHERE t1.id = t2.id RETURNING t1.id, t1.a")
        .unwrap()
        .unwrap();

    let mut results = Vec::new();
    loop {
        match rows.step().unwrap() {
            StepResult::Row => {
                let row = rows.row().unwrap();
                let Value::Numeric(Numeric::Integer(id)) = row.get_value(0) else {
                    panic!("expected integer id");
                };
                let Value::Numeric(Numeric::Integer(a)) = row.get_value(1) else {
                    panic!("expected integer a");
                };
                results.push((*id, *a));
            }
            StepResult::Done => break,
            _ => {}
        }
    }

    results.sort_by_key(|r| r.0);
    assert_eq!(results, vec![(1, 100), (2, 200)]);
}
