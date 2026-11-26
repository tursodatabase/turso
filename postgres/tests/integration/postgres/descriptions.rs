//! `pg_description`: a seeded catalog. A database has the built-in comments
//! of PostgreSQL before anything is stored, and the first statement that
//! writes a comment stores the table, with its unique index on
//! `(objoid, classoid, objsubid)`.

use crate::common::TempDatabase;
use turso_core::{NamespaceId, StepResult};
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
    // The first comment stores the table and its index.
    conn.execute("COMMENT ON SCHEMA public IS 'mine'").unwrap();
    let mut stmt = conn
        .inner()
        .prepare_engine_schema_statement(
            "INSERT INTO pg_description VALUES (2200, 2615, 0, 'a second comment')",
        )
        .unwrap();
    let err = stmt.run_collect_rows().unwrap_err().to_string();
    assert!(err.contains("UNIQUE constraint failed"), "{err}");
}

/// The number of built-in comments.
fn builtin_count() -> usize {
    turso_pg_catalog::PG_DESCRIPTION.len()
}

/// The number of comments of the database.
fn comment_count(conn: &Connection) -> usize {
    texts(conn, "SELECT count(*) FROM pg_description")[0]
        .parse()
        .unwrap()
}

/// The comment of the schema with oid `oid`, if it has one.
fn schema_comment(conn: &Connection, oid: i64) -> Vec<String> {
    texts(
        conn,
        &format!(
            "SELECT description FROM pg_description
             WHERE objoid = {oid} AND classoid = 2615 AND objsubid = 0"
        ),
    )
}

/// The comment of the relation `relation`, if it has one.
fn relation_comment(conn: &Connection, relation: &str) -> Vec<String> {
    texts(
        conn,
        &format!(
            "SELECT description FROM pg_description
             WHERE objoid = (SELECT oid FROM pg_class WHERE relname = '{relation}')
               AND classoid = 1259 AND objsubid = 0"
        ),
    )
}

/// Whether `pg_description` is a stored table in the schema that `conn`
/// has. It reads the schema and runs no statement, so that it does not
/// change what `conn` sees.
fn is_stored(conn: &Connection) -> bool {
    conn.inner()
        .current_schema()
        .get_btree_table(NamespaceId::ROOT, "pg_description")
        .is_some()
}

/// The oid of the schema `public`, and of `pg_catalog`.
const PUBLIC: i64 = 2200;
const PG_CATALOG: i64 = 11;

#[turso_macros::test(mvcc)]
fn new_database_has_the_builtin_comments_and_stores_nothing(db: TempDatabase) {
    let conn = db.connect_postgres();
    assert!(!is_stored(&conn));
    assert_eq!(comment_count(&conn), builtin_count());
    assert_eq!(schema_comment(&conn, PUBLIC), ["standard public schema"]);
    assert_eq!(schema_comment(&conn, PG_CATALOG), ["system catalog schema"]);
    // A statement that reads, or that changes other objects, stores nothing.
    conn.execute("CREATE TABLE items (id int)").unwrap();
    conn.execute("CREATE SCHEMA app").unwrap();
    conn.execute("DROP SCHEMA app").unwrap();
    conn.execute("DROP TABLE items").unwrap();
    assert!(!is_stored(&conn));
    assert_eq!(comment_count(&conn), builtin_count());
}

#[turso_macros::test(mvcc)]
fn first_comment_stores_the_table_with_the_builtin_comments(db: TempDatabase) {
    let conn = db.connect_postgres();
    let other = db.connect_postgres();
    conn.execute("CREATE TABLE items (id int)").unwrap();
    assert_eq!(comment_count(&other), builtin_count());

    conn.execute("COMMENT ON TABLE items IS 'the items'")
        .unwrap();
    assert!(is_stored(&conn));
    assert_eq!(comment_count(&conn), builtin_count() + 1);
    assert_eq!(relation_comment(&conn, "items"), ["the items"]);
    assert_eq!(schema_comment(&conn, PUBLIC), ["standard public schema"]);
    assert_eq!(integrity_check(&conn), ["ok"]);
    // Another connection reads its schema again and reads the stored table.
    assert_eq!(relation_comment(&other, "items"), ["the items"]);
    assert_eq!(comment_count(&other), builtin_count() + 1);

    // The next comment writes to the stored table.
    other
        .execute("COMMENT ON COLUMN items.id IS 'the key'")
        .unwrap();
    assert_eq!(column_comments(&conn, "items"), ["1: the key"]);
    assert_eq!(comment_count(&conn), builtin_count() + 2);

    drop(other);
    drop(conn);
    let db = db.reopen();
    let conn = db.connect_postgres();
    assert!(is_stored(&conn));
    assert_eq!(comment_count(&conn), builtin_count() + 2);
    assert_eq!(relation_comment(&conn, "items"), ["the items"]);
}

#[turso_macros::test(mvcc)]
fn rollback_of_the_first_comment_stores_nothing(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE items (id int)").unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("COMMENT ON TABLE items IS 'the items'")
        .unwrap();
    assert_eq!(relation_comment(&conn, "items"), ["the items"]);
    conn.execute("ROLLBACK").unwrap();
    assert!(!is_stored(&conn));
    assert_eq!(comment_count(&conn), builtin_count());
    assert_eq!(relation_comment(&conn, "items"), Vec::<String>::new());

    // A later comment stores the table.
    conn.execute("COMMENT ON TABLE items IS 'again'").unwrap();
    assert!(is_stored(&conn));
    assert_eq!(relation_comment(&conn, "items"), ["again"]);
    assert_eq!(comment_count(&conn), builtin_count() + 1);
}

#[turso_macros::test(mvcc)]
fn comment_on_the_public_schema_replaces_the_builtin_comment(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("COMMENT ON SCHEMA public IS 'mine'").unwrap();
    assert!(is_stored(&conn));
    assert_eq!(schema_comment(&conn, PUBLIC), ["mine"]);
    assert_eq!(comment_count(&conn), builtin_count());
    conn.execute("COMMENT ON SCHEMA public IS NULL").unwrap();
    assert_eq!(schema_comment(&conn, PUBLIC), Vec::<String>::new());
    assert_eq!(comment_count(&conn), builtin_count() - 1);
}
