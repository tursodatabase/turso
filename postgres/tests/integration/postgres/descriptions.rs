//! `pg_description`: a stored table `pg_description`, which holds what
//! statements wrote, with an overlay of the same name. A user statement
//! reads the overlay, which returns the built-in comments of PostgreSQL,
//! which are in the binary, and the stored rows. A statement of the engine
//! reads and writes the stored table.

use std::sync::Arc;

use crate::common::TempDatabase;
use turso_core::Value;
use turso_pg::Connection;

/// The stored table, as a statement of the engine names it.
const STORED_TABLE: &str = "pg_description";

/// The rows of `sql` in `conn`, with each value as text.
fn rows(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    conn.query(sql)
        .unwrap()
        .unwrap()
        .run_collect_rows()
        .unwrap()
        .into_iter()
        .map(|row| row.iter().map(|value| value.to_string()).collect())
        .collect()
}

/// The first column of each row of `sql`, as text.
fn texts(conn: &Connection, sql: &str) -> Vec<String> {
    rows(conn, sql)
        .into_iter()
        .map(|row| row[0].clone())
        .collect()
}

/// The rows of a query of the engine, such as a read of the stored table,
/// which a user statement does not read. It runs as a statement of its own,
/// with its own transaction.
fn engine_rows(conn: &Arc<turso_core::Connection>, sql: &str) -> Vec<Vec<Value>> {
    conn.prepare_engine_schema_statement(sql)
        .unwrap()
        .run_collect_rows()
        .unwrap()
}

/// The rows of the stored table, as `(objoid, classoid, objsubid,
/// description)`, in the order of the key.
fn stored_rows(conn: &Connection) -> Vec<Vec<Value>> {
    engine_rows(
        conn.inner(),
        &format!(
            "SELECT objoid, classoid, objsubid, description FROM {STORED_TABLE} \
             ORDER BY objoid, classoid, objsubid"
        ),
    )
}

/// The comments of the columns of the relation `relation`, as
/// `number: comment`, in the order of the column numbers.
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

/// The oid of the schema `public`, and of `pg_catalog`.
const PUBLIC: i64 = 2200;
const PG_CATALOG: i64 = 11;

#[turso_macros::test]
fn new_database_has_the_builtin_comments_and_stores_none(db: TempDatabase) {
    let conn = db.connect_postgres();
    assert_eq!(comment_count(&conn), builtin_count());
    assert_eq!(schema_comment(&conn, PUBLIC), ["standard public schema"]);
    assert_eq!(schema_comment(&conn, PG_CATALOG), ["system catalog schema"]);
    assert_eq!(
        texts(
            &conn,
            "SELECT description FROM pg_description WHERE objoid = 16 AND classoid = 1247"
        ),
        ["boolean, format 't'/'f'"]
    );
    // The stored table exists from the open on, and is empty.
    assert!(stored_rows(&conn).is_empty());
    // Each row has the oid of pg_description as its tableoid.
    assert_eq!(
        texts(
            &conn,
            "SELECT DISTINCT tableoid FROM pg_description WHERE objoid = 2200"
        ),
        ["2609"]
    );
}

#[turso_macros::test]
fn comment_on_adds_a_stored_row_and_survives_a_reopen(db: TempDatabase) {
    let conn = db.connect_postgres();
    let other = db.connect_postgres();
    conn.execute("CREATE TABLE items (id int, name text)")
        .unwrap();
    conn.execute("COMMENT ON TABLE items IS 'the items'")
        .unwrap();
    conn.execute("COMMENT ON COLUMN items.name IS 'it''s the name'")
        .unwrap();
    assert_eq!(comment_count(&conn), builtin_count() + 2);
    assert_eq!(relation_comment(&conn, "items"), ["the items"]);
    assert_eq!(column_comments(&conn, "items"), ["2: it's the name"]);
    assert_eq!(stored_rows(&conn).len(), 2);
    assert_eq!(relation_comment(&other, "items"), ["the items"]);

    // A second comment on the same object replaces the first.
    other
        .execute("COMMENT ON TABLE items IS 'changed'")
        .unwrap();
    assert_eq!(relation_comment(&conn, "items"), ["changed"]);
    assert_eq!(comment_count(&conn), builtin_count() + 2);
    // A removal of a comment of a user object deletes its stored row.
    conn.execute("COMMENT ON COLUMN items.name IS NULL")
        .unwrap();
    assert_eq!(column_comments(&conn, "items"), Vec::<String>::new());
    assert_eq!(stored_rows(&conn).len(), 1);
    assert_eq!(integrity_check(&conn), ["ok"]);

    drop(other);
    drop(conn);
    let db = db.reopen();
    let conn = db.connect_postgres();
    assert_eq!(comment_count(&conn), builtin_count() + 1);
    assert_eq!(relation_comment(&conn, "items"), ["changed"]);
}

/// A statement does not change a built-in row: COMMENT ON a built-in
/// object, such as the schema `public`, is refused, and its built-in
/// comment stays.
#[turso_macros::test]
fn comment_on_a_builtin_object_is_refused(db: TempDatabase) {
    let conn = db.connect_postgres();
    for sql in [
        "COMMENT ON SCHEMA public IS 'mine'",
        "COMMENT ON SCHEMA public IS NULL",
    ] {
        let error = conn.execute(sql).unwrap_err().to_string();
        assert_eq!(
            error, "COMMENT ON a built-in object is not supported",
            "{sql}"
        );
    }
    assert_eq!(schema_comment(&conn, PUBLIC), ["standard public schema"]);
    assert_eq!(
        rows(&conn, "SELECT obj_description(2200, 'pg_namespace')"),
        [["standard public schema".to_string()]]
    );
    assert_eq!(comment_count(&conn), builtin_count());
    assert!(stored_rows(&conn).is_empty());
}

/// DROP SCHEMA deletes the stored comment of the schema. The built-in
/// comment of `public` stays, because a statement does not change a
/// built-in row, so a new schema `public` has it too.
#[turso_macros::test]
fn drop_schema_deletes_its_comment(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE SCHEMA app").unwrap();
    conn.execute("COMMENT ON SCHEMA app IS 'the app'").unwrap();
    let app = texts(&conn, "SELECT oid FROM pg_namespace WHERE nspname = 'app'").remove(0);
    assert_eq!(schema_comment(&conn, app.parse().unwrap()), ["the app"]);
    conn.execute("DROP SCHEMA app").unwrap();
    assert_eq!(
        schema_comment(&conn, app.parse().unwrap()),
        Vec::<String>::new()
    );
    assert!(stored_rows(&conn).is_empty());

    conn.execute("DROP SCHEMA public").unwrap();
    conn.execute("CREATE SCHEMA public").unwrap();
    assert_eq!(schema_comment(&conn, PUBLIC), ["standard public schema"]);
    assert_eq!(comment_count(&conn), builtin_count());
}

/// DROP COLUMN deletes the comment of the column and gives the comments of
/// the later columns their new numbers, in the table and in the index.
#[turso_macros::test]
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
#[turso_macros::test]
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
            "SELECT description FROM pg_description WHERE objoid >= 16384 ORDER BY description"
        ),
        ["stays"]
    );
    assert_eq!(
        texts(
            &conn,
            &format!(
                "SELECT count(*) FROM pg_description WHERE objoid = {oid} AND classoid = 1259"
            )
        ),
        ["0"]
    );
    assert_eq!(stored_rows(&conn).len(), 1);
    assert_eq!(integrity_check(&conn), ["ok"]);
}

/// The stored table has one row for a key.
#[turso_macros::test]
fn index_of_the_stored_table_is_unique(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE items (id int)").unwrap();
    conn.execute("COMMENT ON TABLE items IS 'the items'")
        .unwrap();
    let oid = texts(&conn, "SELECT oid FROM pg_class WHERE relname = 'items'").remove(0);
    let mut stmt = conn
        .inner()
        .prepare_engine_schema_statement(&format!(
            "INSERT INTO {STORED_TABLE} VALUES ({oid}, 1259, 0, 'a second comment')"
        ))
        .unwrap();
    let err = stmt.run_collect_rows().unwrap_err().to_string();
    assert!(err.contains("UNIQUE constraint failed"), "{err}");
}

/// A user statement reads the overlay of `pg_description`, and cannot
/// write the stored table or drop it or its index.
#[turso_macros::test]
fn a_user_statement_cannot_change_the_stored_table(db: TempDatabase) {
    let conn = db.connect_postgres();
    for sql in [
        "INSERT INTO pg_description VALUES (1, 2, 3, 'x')",
        "UPDATE pg_description SET description = 'x'",
        "DELETE FROM pg_description",
        "DROP TABLE pg_description",
        "DROP INDEX pg_description_o_c_o_index",
    ] {
        assert!(conn.execute(sql).is_err(), "{sql}");
    }
    assert_eq!(comment_count(&conn), builtin_count());
    // The index of the stored table has the name of PostgreSQL's index,
    // and no catalog lists it as an index of the user.
    assert!(rows(
        &conn,
        "SELECT indexname FROM pg_indexes WHERE indexname = 'pg_description_o_c_o_index'"
    )
    .is_empty());
    assert!(rows(
        &conn,
        "SELECT relname FROM pg_class WHERE relname = 'pg_description_o_c_o_index'"
    )
    .is_empty());
}

/// The virtual table reads the stored table in the transaction of the
/// statement that reads it: it sees the writes of its own transaction, and
/// not the uncommitted writes of another connection.
#[turso_macros::test]
fn a_read_sees_its_own_transaction(db: TempDatabase) {
    let conn = db.connect_postgres();
    let other = db.connect_postgres();
    conn.execute("CREATE TABLE items (id int)").unwrap();
    conn.execute("CREATE TABLE kept (id int)").unwrap();
    conn.execute("COMMENT ON TABLE kept IS 'kept'").unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("COMMENT ON TABLE items IS 'the items'")
        .unwrap();
    conn.execute("COMMENT ON TABLE kept IS NULL").unwrap();
    assert_eq!(relation_comment(&conn, "items"), ["the items"]);
    assert_eq!(relation_comment(&conn, "kept"), Vec::<String>::new());
    assert_eq!(comment_count(&conn), builtin_count() + 1);
    // The other connection does not see the uncommitted writes.
    assert_eq!(relation_comment(&other, "items"), Vec::<String>::new());
    assert_eq!(relation_comment(&other, "kept"), ["kept"]);
    assert_eq!(comment_count(&other), builtin_count() + 1);
    conn.execute("COMMIT").unwrap();

    assert_eq!(relation_comment(&other, "items"), ["the items"]);
    assert_eq!(relation_comment(&other, "kept"), Vec::<String>::new());

    // A rolled back write leaves nothing.
    conn.execute("BEGIN").unwrap();
    conn.execute("COMMENT ON TABLE items IS 'rolled back'")
        .unwrap();
    conn.execute("COMMENT ON TABLE kept IS 'rolled back'")
        .unwrap();
    assert_eq!(relation_comment(&conn, "items"), ["rolled back"]);
    conn.execute("ROLLBACK").unwrap();
    assert_eq!(relation_comment(&conn, "items"), ["the items"]);
    assert_eq!(relation_comment(&conn, "kept"), Vec::<String>::new());
    assert_eq!(integrity_check(&conn), ["ok"]);
}

/// A snapshot of a read transaction does not change when another
/// connection commits a comment.
#[turso_macros::test]
fn a_read_transaction_keeps_its_snapshot(db: TempDatabase) {
    let conn = db.connect_postgres();
    let other = db.connect_postgres();
    conn.execute("CREATE TABLE items (id int)").unwrap();

    other.execute("BEGIN").unwrap();
    assert_eq!(relation_comment(&other, "items"), Vec::<String>::new());
    conn.execute("COMMENT ON TABLE items IS 'the items'")
        .unwrap();
    assert_eq!(relation_comment(&other, "items"), Vec::<String>::new());
    other.execute("COMMIT").unwrap();
    assert_eq!(relation_comment(&other, "items"), ["the items"]);
}

/// On the inner side of a join, the virtual table is filtered once for
/// each row of the outer side, and each filter reads the stored table
/// again.
#[turso_macros::test]
fn a_join_reads_both_sources_for_each_outer_row(db: TempDatabase) {
    let conn = db.connect_postgres();
    for table in ["a", "b", "c", "d"] {
        conn.execute(format!("CREATE TABLE {table} (x int, y int)"))
            .unwrap();
    }
    conn.execute("COMMENT ON TABLE a IS 'table a'").unwrap();
    conn.execute("COMMENT ON TABLE c IS 'table c'").unwrap();
    conn.execute("COMMENT ON COLUMN c.y IS 'column y of c'")
        .unwrap();

    let expected = [["a", "table a"], ["b", ""], ["c", "table c"], ["d", ""]]
        .map(|row| row.map(str::to_string).to_vec());
    assert_eq!(
        rows(
            &conn,
            "SELECT c.relname, d.description FROM pg_class c
             LEFT JOIN pg_description d
               ON d.objoid = c.oid AND d.classoid = 1259 AND d.objsubid = 0
             WHERE c.relnamespace = 2200 AND c.relkind = 'r'
             ORDER BY c.relname"
        ),
        expected
    );
    assert_eq!(
        rows(
            &conn,
            "SELECT relname, obj_description(oid, 'pg_class') FROM pg_class
             WHERE relnamespace = 2200 AND relkind = 'r' ORDER BY relname"
        ),
        expected
    );
    // A join on the first column of the key only reads all comments of an
    // object, built in or stored.
    assert_eq!(
        rows(
            &conn,
            "SELECT c.relname, d.objsubid, d.description FROM pg_class c
             JOIN pg_description d ON d.objoid = c.oid
             WHERE c.relname IN ('a', 'c') ORDER BY c.relname, d.objsubid"
        ),
        [
            ["a", "0", "table a"],
            ["c", "0", "table c"],
            ["c", "2", "column y of c"]
        ]
        .map(|row| row.map(str::to_string).to_vec())
    );
    assert_eq!(
        rows(
            &conn,
            "SELECT n.nspname, d.description FROM pg_namespace n
             JOIN pg_description d ON d.objoid = n.oid
             WHERE n.nspname IN ('public', 'pg_catalog')
             ORDER BY n.nspname"
        ),
        [
            ["pg_catalog", "system catalog schema"],
            ["public", "standard public schema"]
        ]
        .map(|row| row.map(str::to_string).to_vec())
    );
    assert_eq!(
        rows(
            &conn,
            "SELECT a.attname, col_description(a.attrelid, a.attnum) FROM pg_attribute a
             WHERE a.attrelid = (SELECT oid FROM pg_class WHERE relname = 'c') AND a.attnum > 0
             ORDER BY a.attnum"
        ),
        [["x", ""], ["y", "column y of c"]].map(|row| row.map(str::to_string).to_vec())
    );
}

/// A statement that stops before the end of the catalog, such as one with
/// LIMIT, drops the cursor while it is in the middle of a read of the
/// stored table. That leaves the transaction of the connection as it was.
#[turso_macros::test]
fn a_read_that_stops_early_keeps_the_transaction(db: TempDatabase) {
    let conn = db.connect_postgres();
    let other = db.connect_postgres();
    for table in ["a", "b", "c"] {
        conn.execute(format!("CREATE TABLE {table} (x int)"))
            .unwrap();
        conn.execute(format!("COMMENT ON TABLE {table} IS 'table {table}'"))
            .unwrap();
    }
    let first_user_comment = "SELECT description FROM pg_description WHERE objoid >= 16384 LIMIT 1";
    assert_eq!(texts(&conn, first_user_comment), ["table a"]);

    conn.execute("BEGIN").unwrap();
    conn.execute("COMMENT ON TABLE b IS 'changed'").unwrap();
    assert_eq!(texts(&conn, first_user_comment), ["table a"]);
    assert_eq!(
        rows(
            &conn,
            "SELECT c.relname, d.description FROM pg_class c
             JOIN pg_description d ON d.objoid = c.oid AND d.classoid = 1259
             WHERE c.relnamespace = 2200 ORDER BY c.relname LIMIT 2"
        ),
        [["a", "table a"], ["b", "changed"]].map(|row| row.map(str::to_string).to_vec())
    );
    conn.execute("COMMENT ON TABLE c IS 'changed too'").unwrap();
    assert_eq!(relation_comment(&other, "b"), ["table b"]);
    conn.execute("COMMIT").unwrap();
    assert_eq!(relation_comment(&other, "b"), ["changed"]);
    assert_eq!(relation_comment(&other, "c"), ["changed too"]);
    assert_eq!(integrity_check(&conn), ["ok"]);
}

/// A scan returns the built-in rows and the stored rows, each one time.
#[turso_macros::test]
fn a_scan_returns_each_key_once(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE items (id int)").unwrap();
    conn.execute("COMMENT ON TABLE items IS 'last'").unwrap();
    let keys = rows(
        &conn,
        "SELECT objoid, classoid, objsubid FROM pg_description",
    );
    let mut sorted = keys.clone();
    sorted.sort_by_key(|key| {
        key.iter()
            .map(|value| value.parse::<i64>().unwrap())
            .collect::<Vec<_>>()
    });
    sorted.dedup();
    assert_eq!(keys.len(), builtin_count() + 1);
    assert_eq!(sorted.len(), keys.len(), "each key comes once");
}

/// Many stored rows fill many pages, which the scan reads after a reopen,
/// when no page is in the cache.
#[turso_macros::test]
fn a_scan_reads_a_stored_table_of_many_pages(db: TempDatabase) {
    const COMMENTS: i64 = 2000;
    let conn = db.connect_postgres();
    let long = "x".repeat(200);
    engine_rows(
        conn.inner(),
        &format!(
            "WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM n WHERE i + 1 < {COMMENTS})
             INSERT INTO {STORED_TABLE} SELECT 100000 + i, 1259, 0, i || '{long}' FROM n"
        ),
    );
    drop(conn);
    let db = db.reopen();
    let conn = db.connect_postgres();
    assert_eq!(comment_count(&conn), builtin_count() + COMMENTS as usize);
    assert_eq!(
        texts(
            &conn,
            "SELECT sum(objoid - 100000) FROM pg_description WHERE objoid >= 100000"
        ),
        [(COMMENTS * (COMMENTS - 1) / 2).to_string()]
    );
    assert_eq!(
        texts(
            &conn,
            "SELECT description FROM pg_description WHERE objoid = 101234 AND classoid = 1259"
        ),
        [format!("1234{long}")]
    );
}

/// A role that owns a table writes and reads its comments. When the role
/// drops the table, the comment goes.
#[turso_macros::test]
fn a_role_that_is_not_a_superuser_comments_its_table(db: TempDatabase) {
    let superuser = db.connect_postgres();
    superuser.execute("CREATE ROLE alice").unwrap();
    superuser.execute("CREATE TABLE notes (id int)").unwrap();
    superuser
        .execute("ALTER TABLE notes OWNER TO alice")
        .unwrap();
    let alice = Connection::connect_with(
        &db.db,
        turso_core::ConnectOpts::new(turso_core::SessionRole::Named("alice".to_string())),
    )
    .unwrap();
    alice.execute("COMMENT ON TABLE notes IS 'mine'").unwrap();
    alice
        .execute("COMMENT ON COLUMN notes.id IS 'the key'")
        .unwrap();
    assert_eq!(relation_comment(&alice, "notes"), ["mine"]);
    assert_eq!(comment_count(&alice), builtin_count() + 2);
    assert!(alice.execute("COMMENT ON SCHEMA public IS 'no'").is_err());
    alice.execute("DROP TABLE notes").unwrap();
    assert_eq!(comment_count(&alice), builtin_count());
    assert!(stored_rows(&superuser).is_empty());
}

/// Opens the database `path` of `io` with the frontend.
#[cfg(feature = "io_memory_yield")]
fn open_with_io(io: &Arc<dyn turso_core::IO>, path: &str) -> Arc<turso_core::Database> {
    let file = io
        .open_file(path, turso_core::OpenFlags::default(), true)
        .unwrap();
    let options = turso_core::OpenOptions::new(Arc::new(turso_pg::PostgresDialect))
        .storage(Arc::new(turso_core::storage::database::DatabaseFile::new(
            file,
        )))
        .db_opts(turso_core::DatabaseOpts::new().with_mvcc(true));
    let mut state = turso_pg::OpenDatabaseState::default();
    loop {
        match turso_pg::open_database_async(
            &mut state,
            io.clone(),
            path,
            &options,
            turso_core::ConnectOpts::new(turso_core::SessionRole::Superuser),
        )
        .unwrap()
        {
            turso_core::IOResult::Done(db) => return db,
            turso_core::IOResult::IO(completions) => completions.wait(io.as_ref()).unwrap(),
        }
    }
}

/// With an I/O that makes every read of a page wait, the cursor returns
/// I/O from `filter` and `next` while it reads the stored table, and
/// continues where it stopped when VDBE calls it again.
#[cfg(feature = "io_memory_yield")]
#[test]
fn a_cursor_continues_after_each_read_of_the_stored_table_waits() {
    {
        let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::MemoryYieldIO::new());
        let path = "yield.db";
        let long = "y".repeat(300);
        {
            let db = open_with_io(&io, path);
            let conn = Connection::connect(&db).unwrap();
            for table in ["a", "b", "c"] {
                conn.execute(format!("CREATE TABLE {table} (x int)"))
                    .unwrap();
            }
            conn.execute("COMMENT ON TABLE b IS 'table b'").unwrap();
            engine_rows(
                conn.inner(),
                &format!(
                    "WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM n WHERE i < 499)
                     INSERT INTO {STORED_TABLE} SELECT 100000 + i, 1259, 0, i || '{long}' FROM n"
                ),
            );
            conn.close().unwrap();
        }
        let db = open_with_io(&io, path);
        let conn = Connection::connect(&db).unwrap();
        assert_eq!(comment_count(&conn), builtin_count() + 501);
        assert_eq!(schema_comment(&conn, PUBLIC), ["standard public schema"]);
        assert_eq!(
            rows(
                &conn,
                "SELECT relname, obj_description(oid, 'pg_class') FROM pg_class
                 WHERE relnamespace = 2200 ORDER BY relname"
            ),
            [["a", ""], ["b", "table b"], ["c", ""]].map(|row| row.map(str::to_string).to_vec())
        );
        assert_eq!(
            texts(
                &conn,
                "SELECT description FROM pg_description WHERE objoid = 100321 AND classoid = 1259"
            ),
            [format!("321{long}")]
        );
    }
}
