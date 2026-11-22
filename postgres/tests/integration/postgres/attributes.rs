//! The stored `pg_attribute` table: what the `.sqltest` files cannot
//! express, such as opening the database again.

use crate::common::TempDatabase;
use turso_core::Value;

/// The `(attname, attnum)` rows of table `relname`, ordered by `attnum`.
fn attributes(conn: &turso_pg::Connection, relname: &str) -> Vec<(String, i64)> {
    let rows = conn
        .prepare(format!(
            "SELECT a.attname, a.attnum FROM pg_attribute a \
             JOIN pg_class c ON c.oid = a.attrelid \
             WHERE c.relname = '{relname}' ORDER BY a.attnum"
        ))
        .unwrap()
        .run_collect_rows()
        .unwrap();
    rows.into_iter()
        .map(|row| {
            let Value::Text(name) = &row[0] else {
                panic!("expected text, got {:?}", row[0]);
            };
            (name.as_str().to_string(), row[1].as_int().unwrap())
        })
        .collect()
}

fn named(columns: &[&str]) -> Vec<(String, i64)> {
    columns
        .iter()
        .zip(1..)
        .map(|(name, number)| (name.to_string(), number))
        .collect()
}

#[turso_macros::test(mvcc)]
fn select_star_has_the_columns_of_postgres(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE t (a int)").unwrap();
    let mut stmt = conn.prepare("SELECT * FROM pg_attribute").unwrap();
    assert_eq!(stmt.num_columns(), 26, "tableoid is a system column");
    assert_eq!(stmt.run_collect_rows().unwrap().len(), 1);
}

#[turso_macros::test(mvcc)]
fn rows_follow_each_statement(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE t (a int, b text)").unwrap();
    assert_eq!(attributes(&conn, "t"), named(&["a", "b"]));
    conn.execute("ALTER TABLE t ADD COLUMN c int").unwrap();
    assert_eq!(attributes(&conn, "t"), named(&["a", "b", "c"]));
    conn.execute("ALTER TABLE t RENAME COLUMN b TO d").unwrap();
    assert_eq!(attributes(&conn, "t"), named(&["a", "d", "c"]));
    conn.execute("ALTER TABLE t DROP COLUMN a").unwrap();
    assert_eq!(attributes(&conn, "t"), named(&["d", "c"]));
    conn.execute("ALTER TABLE t RENAME TO u").unwrap();
    assert_eq!(attributes(&conn, "u"), named(&["d", "c"]));
    conn.execute("DROP TABLE u").unwrap();
    let mut left = conn.prepare("SELECT count(*) FROM pg_attribute").unwrap();
    assert_eq!(
        left.run_collect_rows().unwrap(),
        vec![vec![Value::from_i64(0)]]
    );
}

#[turso_macros::test(mvcc)]
fn a_rolled_back_transaction_leaves_the_rows_as_they_were(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE kept (a int, b text)").unwrap();
    conn.execute("BEGIN").unwrap();
    conn.execute("CREATE TABLE undone (x int)").unwrap();
    conn.execute("ALTER TABLE kept ADD COLUMN c int").unwrap();
    conn.execute("ALTER TABLE kept RENAME COLUMN a TO z")
        .unwrap();
    assert_eq!(attributes(&conn, "undone"), named(&["x"]));
    assert_eq!(attributes(&conn, "kept"), named(&["z", "b", "c"]));
    conn.execute("ROLLBACK").unwrap();
    assert_eq!(attributes(&conn, "undone"), vec![]);
    assert_eq!(attributes(&conn, "kept"), named(&["a", "b"]));

    conn.execute("BEGIN").unwrap();
    conn.execute("DROP TABLE kept").unwrap();
    conn.execute("ROLLBACK").unwrap();
    assert_eq!(attributes(&conn, "kept"), named(&["a", "b"]));
}

#[turso_macros::test(mvcc)]
fn rows_are_there_after_the_database_is_opened_again(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE t (a int, b text)").unwrap();
    conn.execute("CREATE TABLE gone (x int)").unwrap();
    conn.execute("ALTER TABLE t ADD COLUMN c int").unwrap();
    conn.execute("DROP TABLE gone").unwrap();
    conn.close().unwrap();
    drop(conn);

    let db = db.reopen();
    let conn = db.connect_postgres();
    assert_eq!(attributes(&conn, "t"), named(&["a", "b", "c"]));
    let mut all = conn.prepare("SELECT count(*) FROM pg_attribute").unwrap();
    assert_eq!(
        all.run_collect_rows().unwrap(),
        vec![vec![Value::from_i64(3)]]
    );

    // The reopened database keeps writing the rows.
    conn.execute("ALTER TABLE t DROP COLUMN b").unwrap();
    conn.execute("CREATE TABLE later (y int)").unwrap();
    assert_eq!(attributes(&conn, "t"), named(&["a", "c"]));
    assert_eq!(attributes(&conn, "later"), named(&["y"]));
}

#[turso_macros::test(mvcc)]
fn another_connection_sees_the_rows(db: TempDatabase) {
    let writer = db.connect_postgres();
    let reader = db.connect_postgres();
    writer.execute("CREATE TABLE t (a int)").unwrap();
    assert_eq!(attributes(&reader, "t"), named(&["a"]));
    writer.execute("ALTER TABLE t ADD COLUMN b int").unwrap();
    assert_eq!(attributes(&reader, "t"), named(&["a", "b"]));
}

/// Fails unless `PRAGMA integrity_check` finds the database sound.
fn assert_integrity(conn: &turso_pg::Connection) {
    // The check skips a table whose rows are not in the database file yet.
    conn.inner()
        .execute("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
    let rows = conn
        .inner()
        .prepare("PRAGMA integrity_check")
        .unwrap()
        .run_collect_rows()
        .unwrap();
    assert_eq!(rows, vec![vec![Value::build_text("ok")]]);
}

/// The rows are written with bytecode of the frontend's own, which adds
/// and deletes the index entries itself.
#[turso_macros::test(mvcc)]
fn the_index_stays_in_step_with_the_table(db: TempDatabase) {
    let conn = db.connect_postgres();
    for statement in [
        "CREATE TABLE a (a1 int, a2 text, a3 int)",
        "CREATE TABLE b (b1 int)",
        "ALTER TABLE a ADD COLUMN a4 int",
        "ALTER TABLE a DROP COLUMN a2",
        "CREATE TABLE c (c1 int, c2 int)",
        "ALTER TABLE a RENAME COLUMN a3 TO a5",
        "DROP TABLE b",
        "ALTER TABLE c ADD COLUMN c3 text",
        "ALTER TABLE c DROP COLUMN c1",
        "DROP TABLE a",
        "CREATE TABLE d (d1 int)",
    ] {
        conn.execute(statement).unwrap();
        assert_integrity(&conn);
    }
    assert_eq!(attributes(&conn, "c"), named(&["c2", "c3"]));
    assert_eq!(attributes(&conn, "d"), named(&["d1"]));
    let mut all = conn.prepare("SELECT count(*) FROM pg_attribute").unwrap();
    assert_eq!(
        all.run_collect_rows().unwrap(),
        vec![vec![Value::from_i64(3)]]
    );

    conn.execute("BEGIN").unwrap();
    conn.execute("ALTER TABLE c ADD COLUMN c4 int").unwrap();
    conn.execute("DROP TABLE d").unwrap();
    conn.execute("ROLLBACK").unwrap();
    assert_integrity(&conn);
    assert_eq!(attributes(&conn, "c"), named(&["c2", "c3"]));
    assert_eq!(attributes(&conn, "d"), named(&["d1"]));
}

#[turso_macros::test(mvcc)]
fn a_query_by_relation_uses_the_index(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE t (a int)").unwrap();
    let plan = conn
        .prepare(
            "EXPLAIN SELECT attname FROM pg_attribute \
             WHERE attrelid = 16384 ORDER BY attnum",
        )
        .unwrap()
        .run_collect_rows()
        .unwrap();
    let plan = format!("{plan:?}");
    assert!(
        plan.contains("pg_attribute_relid_attnum_index"),
        "the plan does not use the index: {plan}"
    );
}

#[test]
fn vacuum_keeps_the_rows() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE t (a int, b text)").unwrap();
    conn.execute("CREATE TABLE gone (x int)").unwrap();
    conn.execute("DROP TABLE gone").unwrap();
    conn.inner()
        .execute("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
    conn.inner().execute("VACUUM").unwrap();
    assert_eq!(attributes(&conn, "t"), named(&["a", "b"]));
    conn.close().unwrap();
    drop(conn);

    let db = db.reopen();
    let conn = db.connect_postgres();
    assert_eq!(attributes(&conn, "t"), named(&["a", "b"]));
    let stmt = conn.prepare("SELECT * FROM pg_attribute").unwrap();
    assert_eq!(stmt.num_columns(), 26);
    assert_integrity(&conn);

    // The vacuumed database keeps writing the rows.
    conn.execute("ALTER TABLE t RENAME COLUMN a TO z").unwrap();
    assert_eq!(attributes(&conn, "t"), named(&["z", "b"]));
    conn.execute("DROP TABLE t").unwrap();
    assert_eq!(attributes(&conn, "t"), vec![]);
    assert_integrity(&conn);
}

/// The PostgreSQL parser refuses ALTER COLUMN, but the engine's connection
/// takes the engine's form of it.
#[test]
fn alter_column_through_the_engine_connection_is_refused() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE t (a int, c text)").unwrap();

    let error = conn
        .inner()
        .execute("ALTER TABLE t ALTER COLUMN a TO b INTEGER")
        .unwrap_err()
        .to_string();

    assert!(
        error.contains("ALTER COLUMN is not supported in a PostgreSQL database"),
        "{error}"
    );
    assert_eq!(attributes(&conn, "t"), named(&["a", "c"]));
}
