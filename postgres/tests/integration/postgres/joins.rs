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

#[turso_macros::test]
fn parenthesized_join_alias_star_preserves_duplicate_columns(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE a (id INTEGER, x INTEGER)")
        .unwrap();
    conn.execute("CREATE TABLE b (id INTEGER, y INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO a VALUES (1, 10)").unwrap();
    conn.execute("INSERT INTO b VALUES (1, 20)").unwrap();
    let expected = vec![vec![
        Value::from_i64(1),
        Value::from_i64(10),
        Value::from_i64(1),
        Value::from_i64(20),
    ]];
    for projection in ["*", "j.*"] {
        let sql = format!("SELECT {projection} FROM (a JOIN b ON a.id = b.id) AS j");
        let rows = conn.prepare(&sql).unwrap().run_collect_rows().unwrap();
        assert_eq!(rows, expected, "{sql}");
    }
}

#[turso_macros::test]
fn parenthesized_join_alias_duplicate_column_is_ambiguous(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE a (id INTEGER, x INTEGER)")
        .unwrap();
    conn.execute("CREATE TABLE b (id INTEGER, y INTEGER)")
        .unwrap();
    for sql in [
        "SELECT j.id FROM (a JOIN b ON true) AS j",
        "SELECT j.id FROM (b JOIN a ON true) AS j",
    ] {
        match conn.prepare(sql) {
            Ok(_) => panic!("must reject ambiguous reference: {sql}"),
            Err(err) => assert!(err.to_string().contains("ambiguous"), "{sql}: {err}"),
        }
    }
}

#[turso_macros::test]
fn parenthesized_join_alias_hides_inner_table_names(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE a (id INTEGER, x INTEGER)")
        .unwrap();
    conn.execute("CREATE TABLE b (id INTEGER, y INTEGER)")
        .unwrap();
    assert!(conn
        .prepare("SELECT a.x FROM (a JOIN b ON a.id = b.id) AS j")
        .is_err());
}

#[turso_macros::test]
fn parenthesized_join_alias_using_outputs_merged_column_once(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE a (id INTEGER, x INTEGER)")
        .unwrap();
    conn.execute("CREATE TABLE b (id INTEGER, y INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO a VALUES (1, 10)").unwrap();
    conn.execute("INSERT INTO b VALUES (1, 20)").unwrap();
    let rows = conn
        .prepare("SELECT j.* FROM (a JOIN b USING (id)) AS j")
        .unwrap()
        .run_collect_rows()
        .unwrap();
    assert_eq!(
        rows,
        vec![vec![
            Value::from_i64(1),
            Value::from_i64(10),
            Value::from_i64(20)
        ]]
    );
}

#[turso_macros::test]
fn duplicate_subquery_output_names_are_ambiguous(db: TempDatabase) {
    let conn = db.connect_postgres();
    for sql in [
        "SELECT s.x FROM (SELECT 1 AS x, 2 AS x) AS s",
        "SELECT x FROM (SELECT 1 AS x, 2 AS x) AS s",
        "WITH t(x, x) AS (VALUES (1, 2)) SELECT t.x FROM t",
    ] {
        match conn.prepare(sql) {
            Ok(_) => panic!("must reject ambiguous reference: {sql}"),
            Err(err) => assert!(err.to_string().contains("ambiguous"), "{sql}: {err}"),
        }
    }
    let rows = conn
        .prepare("SELECT * FROM (SELECT 1 AS x, 2 AS x) AS s")
        .unwrap()
        .run_collect_rows()
        .unwrap();
    assert_eq!(rows, vec![vec![Value::from_i64(1), Value::from_i64(2)]]);
}

#[turso_macros::test]
fn parenthesized_join_alias_keeps_lateral_scope(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE a (x INTEGER)").unwrap();
    conn.execute("CREATE TABLE b (y INTEGER)").unwrap();
    conn.execute("INSERT INTO a VALUES (10)").unwrap();
    conn.execute("INSERT INTO b VALUES (2)").unwrap();
    let rows = conn
        .prepare("SELECT j.z FROM a JOIN (b JOIN LATERAL (SELECT a.x + b.y AS z) s ON true) AS j ON true")
        .unwrap()
        .run_collect_rows()
        .unwrap();
    assert_eq!(rows, vec![vec![Value::from_i64(12)]]);
}

#[turso_macros::test]
fn parenthesized_join_on_cannot_read_outer_table(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE a (x INTEGER)").unwrap();
    conn.execute("CREATE TABLE b (y INTEGER)").unwrap();
    let sql = "SELECT * FROM a JOIN (b JOIN LATERAL (SELECT b.y AS z) s ON a.x = s.z) AS j ON true";
    match conn.prepare(sql) {
        Ok(_) => panic!("ON cannot read a: {sql}"),
        Err(err) => assert!(err.to_string().contains("no such table: a"), "{err}"),
    }
}

#[turso_macros::test]
fn parenthesized_join_on_can_read_enclosing_query(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE a (x INTEGER)").unwrap();
    conn.execute("CREATE TABLE b (y INTEGER)").unwrap();
    conn.execute("CREATE TABLE d (w INTEGER)").unwrap();
    conn.execute("INSERT INTO a VALUES (2)").unwrap();
    conn.execute("INSERT INTO b VALUES (2)").unwrap();
    conn.execute("INSERT INTO d VALUES (1)").unwrap();
    let sql = "SELECT (SELECT j.z FROM d JOIN (b JOIN LATERAL (SELECT a.x + d.w + b.y AS z) s ON a.x = b.y) AS j ON true) FROM a";
    let rows = conn.prepare(sql).unwrap().run_collect_rows().unwrap();
    assert_eq!(rows, vec![vec![Value::from_i64(5)]]);
    let nested = "SELECT (SELECT j.z FROM d JOIN (b JOIN LATERAL (SELECT a.x + d.w + b.y AS z) s ON EXISTS (SELECT a.x)) AS j ON true) FROM a";
    let rows = conn.prepare(nested).unwrap().run_collect_rows().unwrap();
    assert_eq!(rows, vec![vec![Value::from_i64(5)]]);
}

#[turso_macros::test]
fn parenthesized_join_on_subquery_cannot_read_outer_table(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE a (x INTEGER)").unwrap();
    conn.execute("CREATE TABLE b (y INTEGER)").unwrap();
    let sql = "SELECT * FROM a JOIN (b JOIN LATERAL (SELECT b.y AS z) s ON EXISTS (SELECT a.x)) AS j ON true";
    assert!(
        conn.prepare(sql).is_err(),
        "ON subquery cannot read a: {sql}"
    );
}
