use crate::common::TempDatabase;
use turso_core::Value;

#[turso_macros::test(mvcc)]
fn lateral_inside_parenthesized_right_join_stays_within_left_join(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE a (x INTEGER)").unwrap();
    conn.execute("CREATE TABLE b (y INTEGER)").unwrap();
    conn.execute("INSERT INTO a VALUES (1), (2)").unwrap();
    conn.execute("INSERT INTO b VALUES (1)").unwrap();

    let rows = conn
        .prepare(
            "SELECT a.x, s.v FROM a LEFT JOIN (b JOIN LATERAL (SELECT a.x AS v) s ON true) ON a.x = b.y ORDER BY 1",
        )
        .unwrap()
        .run_collect_rows()
        .unwrap();

    assert_eq!(
        rows,
        vec![
            vec![Value::from_i64(1), Value::from_i64(1)],
            vec![Value::from_i64(2), Value::Null],
        ]
    );
}
