use crate::common::TempDatabase;
use turso_core::Value;

#[turso_macros::test(mvcc)]
fn alias_on_parenthesized_join_is_visible_to_outer_query(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE a (x INTEGER)").unwrap();
    conn.execute("CREATE TABLE b (y INTEGER)").unwrap();
    conn.execute("CREATE TABLE c (z INTEGER)").unwrap();
    conn.execute("INSERT INTO a VALUES (1)").unwrap();
    conn.execute("INSERT INTO b VALUES (2)").unwrap();
    conn.execute("INSERT INTO c VALUES (3)").unwrap();

    let rows = conn
        .prepare("SELECT j.y FROM a JOIN (b JOIN c ON true) AS j ON true")
        .unwrap()
        .run_collect_rows()
        .unwrap();

    assert_eq!(rows, vec![vec![Value::from_i64(2)]]);
}
