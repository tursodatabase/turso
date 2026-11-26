//! `pg_description`: the table of the comments that `COMMENT ON` stores,
//! and its unique index on `(objoid, classoid, objsubid)`.

use crate::common::TempDatabase;
use turso_core::StepResult;
use turso_pg::Connection;

/// The first column of each row of `sql`, as text.
fn texts(conn: &Connection, sql: &str) -> Vec<String> {
    let mut rows = conn.query(sql).unwrap().unwrap();
    let mut values = Vec::new();
    while let StepResult::Row = rows.step().unwrap() {
        values.push(rows.row().unwrap().get_value(0).to_string());
    }
    values
}

/// The comments of the columns of the relation `relation`, as
/// `number: comment`, in the order of the column numbers. The condition
/// names the three columns of the index, so the statement reads the index.
fn column_comments(conn: &Connection, relation: &str) -> Vec<String> {
    texts(
        conn,
        &format!(
            "SELECT objsubid || ': ' || description FROM pg_description
             WHERE objoid = (SELECT oid FROM pg_class WHERE relname = '{relation}')
               AND classoid = 1259 AND objsubid > 0
             ORDER BY objsubid"
        ),
    )
}

/// The result of `PRAGMA integrity_check`, which compares each index with
/// its table. It runs as a statement of its own, with its own transaction.
fn integrity_check(conn: &Connection) -> Vec<String> {
    conn.inner()
        .prepare_sqlite("PRAGMA integrity_check")
        .unwrap()
        .run_collect_rows()
        .unwrap()
        .into_iter()
        .map(|row| row[0].to_string())
        .collect()
}

/// DROP COLUMN deletes the comment of the column and gives the comments of
/// the later columns their new numbers, in the table and in the index.
#[turso_macros::test(mvcc)]
fn drop_column_keeps_the_index_of_comments_correct(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE items (a int, b int, c int, d int)")
        .unwrap();
    conn.execute("COMMENT ON TABLE items IS 'the items'")
        .unwrap();
    // The comments are stored in another order than the columns have, so
    // that a later column is read before the dropped one.
    for (column, comment) in [
        ("d", "fourth"),
        ("c", "third"),
        ("b", "second"),
        ("a", "first"),
    ] {
        conn.execute(format!("COMMENT ON COLUMN items.{column} IS '{comment}'"))
            .unwrap();
    }
    assert_eq!(
        column_comments(&conn, "items"),
        ["1: first", "2: second", "3: third", "4: fourth"]
    );

    conn.execute("ALTER TABLE items DROP COLUMN b").unwrap();
    assert_eq!(
        column_comments(&conn, "items"),
        ["1: first", "2: third", "3: fourth"]
    );
    assert_eq!(integrity_check(&conn), ["ok"]);

    // A comment on the column that now has the number of the dropped one
    // replaces the comment that moved there.
    conn.execute("COMMENT ON COLUMN items.c IS 'changed'")
        .unwrap();
    assert_eq!(
        column_comments(&conn, "items"),
        ["1: first", "2: changed", "3: fourth"]
    );
    assert_eq!(integrity_check(&conn), ["ok"]);
}

/// DROP TABLE deletes the comments of the table and of its columns, in the
/// table and in the index.
#[turso_macros::test(mvcc)]
fn drop_table_keeps_the_index_of_comments_correct(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE items (a int, b int)").unwrap();
    conn.execute("CREATE TABLE kept (a int)").unwrap();
    conn.execute("COMMENT ON TABLE items IS 'the items'")
        .unwrap();
    conn.execute("COMMENT ON COLUMN items.b IS 'second'")
        .unwrap();
    conn.execute("COMMENT ON TABLE kept IS 'stays'").unwrap();

    let oid = texts(&conn, "SELECT oid FROM pg_class WHERE relname = 'items'").remove(0);
    conn.execute("DROP TABLE items").unwrap();
    assert_eq!(
        texts(
            &conn,
            "SELECT description FROM pg_description WHERE classoid = 1259 ORDER BY description"
        ),
        ["stays"]
    );
    // The index has all the columns of this condition, so the statement
    // counts the keys of the index and not the rows of the table.
    assert_eq!(
        texts(
            &conn,
            &format!(
                "SELECT count(*) FROM pg_description WHERE objoid = {oid} AND classoid = 1259"
            )
        ),
        ["0"]
    );
    assert_eq!(integrity_check(&conn), ["ok"]);
}

/// The index refuses a second comment for one object.
#[turso_macros::test]
fn index_of_comments_is_unique(db: TempDatabase) {
    let conn = db.connect_postgres();
    let mut stmt = conn
        .inner()
        .prepare_engine_schema_statement(
            "INSERT INTO pg_description VALUES (2200, 2615, 0, 'a second comment')",
        )
        .unwrap();
    let err = stmt.run_collect_rows().unwrap_err().to_string();
    assert!(err.contains("UNIQUE constraint failed"), "{err}");
}
