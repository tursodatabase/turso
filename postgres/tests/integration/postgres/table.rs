use crate::common::TempDatabase;
use turso_core::{Numeric, StepResult, Value};

#[turso_macros::test(mvcc)]
fn test_postgres_read_real_table(db: TempDatabase) {
    let conn = db.connect_postgres();

    // Create a table using SQLite dialect (default)
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")
        .unwrap();
    conn.execute("INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)")
        .unwrap();

    // Switch to PostgreSQL dialect

    // Try to read from the table using PostgreSQL parser
    let result = conn.query("SELECT * FROM users");
    if let Err(ref e) = result {
        panic!("Query failed: {e:?}");
    }
    let mut rows = result.unwrap().unwrap();

    // First row
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    // Check we got 3 columns (id, name, age)

    let Value::Numeric(Numeric::Integer(id)) = row.get_value(0) else {
        panic!("expected integer for id");
    };
    assert_eq!(*id, 1);

    let Value::Text(name) = row.get_value(1) else {
        panic!("expected text for name");
    };
    assert_eq!(name.value, "Alice");

    let Value::Numeric(Numeric::Integer(age)) = row.get_value(2) else {
        panic!("expected integer for age");
    };
    assert_eq!(*age, 30);

    // Second row
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();

    let Value::Numeric(Numeric::Integer(id)) = row.get_value(0) else {
        panic!("expected integer for id");
    };
    assert_eq!(*id, 2);

    let Value::Text(name) = row.get_value(1) else {
        panic!("expected text for name");
    };
    assert_eq!(name.value, "Bob");

    // No more rows
    let StepResult::Done = rows.step().unwrap() else {
        panic!("expected done");
    };
}

#[turso_macros::test(mvcc)]
fn test_postgres_select_columns(db: TempDatabase) {
    let conn = db.connect_postgres();

    // Create and populate table
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")
        .unwrap();

    // Switch to PostgreSQL dialect

    // Select specific columns
    let mut rows = conn.query("SELECT id, name FROM users").unwrap().unwrap();

    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    // Check we got 2 columns (id, name)

    let Value::Numeric(Numeric::Integer(id)) = row.get_value(0) else {
        panic!("expected integer for id");
    };
    assert_eq!(*id, 1);

    let Value::Text(name) = row.get_value(1) else {
        panic!("expected text for name");
    };
    assert_eq!(name.value, "Alice");
}

#[turso_macros::test(mvcc)]
fn test_postgres_where_clause(db: TempDatabase) {
    let conn = db.connect_postgres();

    // Create and populate table
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")
        .unwrap();
    conn.execute("INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)")
        .unwrap();

    // Switch to PostgreSQL dialect

    // Select with WHERE clause
    let mut rows = conn
        .query("SELECT name FROM users WHERE id = 1")
        .unwrap()
        .unwrap();

    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    // Check we got 1 column (name)

    let Value::Text(name) = row.get_value(0) else {
        panic!("expected text for name");
    };
    assert_eq!(name.value, "Alice");

    // Should be only one row
    let StepResult::Done = rows.step().unwrap() else {
        panic!("expected done");
    };
}

#[turso_macros::test(mvcc)]
fn test_postgres_create_table_as(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE users (id INT, name TEXT, age INT)")
        .unwrap();
    conn.execute("INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Carol', 41)")
        .unwrap();

    conn.execute("CREATE TABLE adults AS SELECT id, name FROM users WHERE age >= 30")
        .unwrap();

    let mut rows = conn
        .query("SELECT id, name FROM adults ORDER BY id")
        .unwrap()
        .unwrap();

    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    let Value::Numeric(Numeric::Integer(id)) = row.get_value(0) else {
        panic!("expected integer for id");
    };
    assert_eq!(*id, 1);
    let Value::Text(name) = row.get_value(1) else {
        panic!("expected text for name");
    };
    assert_eq!(name.value, "Alice");

    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    let Value::Numeric(Numeric::Integer(id)) = row.get_value(0) else {
        panic!("expected integer for id");
    };
    assert_eq!(*id, 3);

    let StepResult::Done = rows.step().unwrap() else {
        panic!("expected done");
    };
}

#[turso_macros::test(mvcc)]
fn test_postgres_create_table_as_with_no_data(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE src (id INT, name TEXT)")
        .unwrap();
    conn.execute("INSERT INTO src VALUES (1, 'Alice'), (2, 'Bob')")
        .unwrap();

    conn.execute("CREATE TABLE dst AS SELECT * FROM src WITH NO DATA")
        .unwrap();

    let mut rows = conn.query("SELECT COUNT(*) FROM dst").unwrap().unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    let Value::Numeric(Numeric::Integer(count)) = row.get_value(0) else {
        panic!("expected integer count");
    };
    assert_eq!(*count, 0, "WITH NO DATA must not copy rows");

    // The structure must exist and accept inserts.
    conn.execute("INSERT INTO dst VALUES (7, 'New')").unwrap();
}

#[turso_macros::test(mvcc)]
fn test_postgres_select_into(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE src (id INT, name TEXT)")
        .unwrap();
    conn.execute("INSERT INTO src VALUES (1, 'Alice'), (2, 'Bob')")
        .unwrap();

    conn.execute("SELECT id, name INTO dst FROM src WHERE id = 2")
        .unwrap();

    let mut rows = conn.query("SELECT name FROM dst").unwrap().unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    let Value::Text(name) = row.get_value(0) else {
        panic!("expected text for name");
    };
    assert_eq!(name.value, "Bob");
    let StepResult::Done = rows.step().unwrap() else {
        panic!("expected done");
    };
}

#[turso_macros::test(mvcc)]
fn test_postgres_create_table_as_column_list_rejected(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE src (id INT, name TEXT)")
        .unwrap();

    let err = conn
        .execute("CREATE TABLE dst (a, b) AS SELECT id, name FROM src")
        .unwrap_err();
    assert!(
        err.to_string().contains("column list"),
        "expected column list rejection, got: {err}"
    );
}

#[turso_macros::test(mvcc)]
fn test_postgres_create_table_as_if_not_exists(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE src (id INT)").unwrap();
    conn.execute("INSERT INTO src VALUES (1)").unwrap();
    conn.execute("CREATE TABLE dst AS SELECT * FROM src")
        .unwrap();
    conn.execute("CREATE TABLE IF NOT EXISTS dst AS SELECT * FROM src")
        .unwrap();

    let mut rows = conn.query("SELECT COUNT(*) FROM dst").unwrap().unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    let Value::Numeric(Numeric::Integer(count)) = row.get_value(0) else {
        panic!("expected integer count");
    };
    assert_eq!(*count, 1, "IF NOT EXISTS must not error or re-insert");
}

#[turso_macros::test(mvcc)]
fn test_postgres_create_table_as_compound_select(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE dst AS SELECT 1 AS x UNION SELECT 2")
        .unwrap();

    let mut rows = conn.query("SELECT x FROM dst ORDER BY x").unwrap().unwrap();
    for expected in [1, 2] {
        let StepResult::Row = rows.step().unwrap() else {
            panic!("expected row");
        };
        let row = rows.row().unwrap();
        let Value::Numeric(Numeric::Integer(x)) = row.get_value(0) else {
            panic!("expected integer for x");
        };
        assert_eq!(*x, expected);
    }
    let StepResult::Done = rows.step().unwrap() else {
        panic!("expected done");
    };
}

#[turso_macros::test(mvcc)]
fn test_postgres_select_into_compound_rejected(db: TempDatabase) {
    let conn = db.connect_postgres();

    // PG allows INTO on the first leaf of a top-level compound; Turso
    // rejects it explicitly rather than silently dropping the INTO.
    let err = conn
        .execute("SELECT 1 INTO dst UNION SELECT 2")
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("SELECT ... INTO is not allowed here"),
        "expected compound INTO rejection, got: {err}"
    );

    let err = conn
        .execute("SELECT * FROM (SELECT 1 INTO dst) sub")
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("SELECT ... INTO is not allowed here"),
        "expected nested INTO rejection, got: {err}"
    );
}

#[test]
fn drop_sequence_of_a_serial_column_is_refused() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE owner (id serial, label text)")
        .unwrap();

    let error = conn
        .execute("DROP SEQUENCE owner_id_seq")
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("cannot drop sequence owner_id_seq because other objects depend on it"),
        "{error}"
    );

    conn.execute("INSERT INTO owner (label) VALUES ('a')")
        .unwrap();
    let rows = conn
        .query("SELECT id FROM owner")
        .unwrap()
        .unwrap()
        .run_collect_rows()
        .unwrap();
    assert_eq!(rows[0][0].to_string(), "1");
}

#[test]
fn refused_drop_names_the_view_that_reads_the_relation() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE base (a int)").unwrap();
    conn.execute("INSERT INTO base VALUES (7)").unwrap();
    conn.execute("CREATE VIEW reader AS SELECT a FROM base")
        .unwrap();
    conn.execute("CREATE SCHEMA other").unwrap();
    conn.execute("CREATE VIEW other.reader_of_reader AS SELECT a FROM public.reader")
        .unwrap();

    let error = conn.execute("DROP TABLE base").unwrap_err();
    assert_eq!(
        error.message,
        "cannot drop table base because other objects depend on it"
    );
    assert_eq!(
        error.detail.as_deref(),
        Some(
            "view reader depends on table base\n\
             view other.reader_of_reader depends on view reader"
        )
    );
    let error = conn.execute("DROP VIEW reader").unwrap_err();
    assert_eq!(
        error.message,
        "cannot drop view reader because other objects depend on it"
    );
    assert_eq!(
        error.detail.as_deref(),
        Some("view other.reader_of_reader depends on view reader")
    );

    let rows = conn
        .query("SELECT a FROM other.reader_of_reader")
        .unwrap()
        .unwrap()
        .run_collect_rows()
        .unwrap();
    assert_eq!(rows[0][0].to_string(), "7");
}

#[test]
fn refused_drop_quotes_names_and_names_the_schema_the_search_path_lacks() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute(r#"CREATE TABLE "T" (a int)"#).unwrap();
    conn.execute(r#"CREATE VIEW "my view" AS SELECT a FROM "T""#)
        .unwrap();
    let error = conn.execute(r#"DROP TABLE "T""#).unwrap_err();
    assert_eq!(
        error.message,
        r#"cannot drop table "T" because other objects depend on it"#
    );
    assert_eq!(
        error.detail.as_deref(),
        Some(r#"view "my view" depends on table "T""#)
    );

    conn.execute("CREATE SCHEMA o").unwrap();
    conn.execute("CREATE TABLE o.f (a int, b int)").unwrap();
    conn.execute("CREATE VIEW o.fv AS SELECT b FROM o.f")
        .unwrap();
    let error = conn.execute("ALTER TABLE o.f DROP COLUMN b").unwrap_err();
    assert_eq!(
        error.message,
        "cannot drop column b of table o.f because other objects depend on it"
    );
    assert_eq!(
        error.detail.as_deref(),
        Some("view o.fv depends on column b of table o.f")
    );

    conn.execute("CREATE SEQUENCE o.s").unwrap();
    conn.execute("CREATE TABLE u (n bigint DEFAULT nextval('o.s'))")
        .unwrap();
    let error = conn.execute("DROP SEQUENCE o.s").unwrap_err();
    assert_eq!(
        error.message,
        "cannot drop sequence o.s because other objects depend on it"
    );
}

#[test]
fn failed_drop_of_several_relations_drops_none() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE kept (a int)").unwrap();
    conn.execute("INSERT INTO kept VALUES (6)").unwrap();
    conn.execute("CREATE VIEW kept_view AS SELECT a FROM kept")
        .unwrap();
    conn.execute("CREATE TABLE owner (id serial)").unwrap();
    conn.execute("CREATE TABLE borrower (n bigint DEFAULT nextval('owner_id_seq'))")
        .unwrap();

    let error = conn.execute("DROP TABLE kept, missing").unwrap_err();
    assert_eq!(error.sqlstate, "42P01", "{error}");
    let error = conn.execute("DROP VIEW kept_view, missing").unwrap_err();
    assert_eq!(error.message, "no such view: missing");
    let error = conn.execute("DROP TABLE borrower, kept").unwrap_err();
    assert_eq!(
        error.message,
        "cannot drop desired object(s) because other objects depend on them"
    );
    assert_eq!(
        error.detail.as_deref(),
        Some("view kept_view depends on table kept")
    );
    assert_eq!(
        error.hint.as_deref(),
        Some("Use DROP ... CASCADE to drop the dependent objects too.")
    );
    let error = conn.execute("DROP TABLE kept, owner CASCADE").unwrap_err();
    assert_eq!(error.sqlstate, "0A000", "{error}");

    let rows = conn
        .query("SELECT a FROM kept_view")
        .unwrap()
        .unwrap()
        .run_collect_rows()
        .unwrap();
    assert_eq!(rows[0][0].to_string(), "6");
}

#[test]
fn only_create_hints_that_a_relation_has_a_type_of_its_name() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("CREATE TYPE ty AS ENUM ('a')").unwrap();
    conn.execute("CREATE TABLE t1 (a int)").unwrap();

    let error = conn.execute("CREATE TABLE ty (a int)").unwrap_err();
    assert_eq!(error.sqlstate, "42710", "{error}");
    assert_eq!(
        error.hint.as_deref(),
        Some(
            "A relation has an associated type of the same name, so you must use a name that \
             doesn't conflict with any existing type."
        )
    );
    let error = conn.execute("ALTER TABLE t1 RENAME TO ty").unwrap_err();
    assert_eq!(error.sqlstate, "42710", "{error}");
    assert_eq!(error.message, "type \"ty\" already exists");
    assert_eq!(error.hint, None);
}

#[test]
fn refused_drop_leaves_the_sequence_and_the_other_default_working() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE owner (id serial, label text)")
        .unwrap();
    conn.execute("CREATE TABLE borrower (n bigint DEFAULT nextval('owner_id_seq'))")
        .unwrap();

    let error = conn.execute("DROP TABLE owner").unwrap_err().to_string();
    assert!(
        error.contains("cannot drop table owner because other objects depend on it"),
        "{error}"
    );
    let error = conn
        .execute("ALTER TABLE owner DROP COLUMN id")
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("cannot drop column id of table owner because other objects depend on it"),
        "{error}"
    );

    conn.execute("INSERT INTO owner (label) VALUES ('a')")
        .unwrap();
    conn.execute("INSERT INTO borrower DEFAULT VALUES").unwrap();
    let rows = conn
        .query("SELECT (SELECT id FROM owner), (SELECT n FROM borrower)")
        .unwrap()
        .unwrap()
        .run_collect_rows()
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        (rows[0][0].to_string(), rows[0][1].to_string()),
        ("1".to_string(), "2".to_string())
    );

    conn.execute("DROP TABLE borrower").unwrap();
    conn.execute("DROP TABLE owner").unwrap();
}

#[test]
fn refused_drop_schema_cascade_keeps_the_schema_and_its_objects() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    for sql in [
        "CREATE SCHEMA app",
        "CREATE TABLE app.owner (id serial, label text)",
        "CREATE TABLE app.other (x int)",
        "CREATE TABLE borrower (n bigint DEFAULT nextval('app.owner_id_seq'), label text)",
    ] {
        conn.execute(sql).unwrap();
    }

    let error = conn.execute("DROP SCHEMA app CASCADE").unwrap_err();
    assert_eq!(error.sqlstate, "0A000", "{error}");

    for sql in [
        "INSERT INTO app.other VALUES (7)",
        "INSERT INTO app.owner (label) VALUES ('a')",
        "INSERT INTO borrower (label) VALUES ('b')",
    ] {
        conn.execute(sql).unwrap();
    }
    let rows = conn
        .query("SELECT (SELECT x FROM app.other), (SELECT id FROM app.owner), (SELECT n FROM borrower)")
        .unwrap()
        .unwrap()
        .run_collect_rows()
        .unwrap();
    assert_eq!(
        rows[0].iter().map(ToString::to_string).collect::<Vec<_>>(),
        vec!["7", "1", "2"]
    );

    conn.execute("DROP TABLE borrower").unwrap();
    conn.execute("DROP SCHEMA app CASCADE").unwrap();
}

fn key_rows(conn: &turso_pg::Connection, table: &str) -> Vec<(Option<i64>, String)> {
    let mut rows = conn
        .query(format!("SELECT id, v FROM {table} ORDER BY v"))
        .unwrap()
        .unwrap();
    let mut result = Vec::new();
    while let StepResult::Row = rows.step().unwrap() {
        let row = rows.row().unwrap();
        let id = match row.get_value(0) {
            Value::Numeric(Numeric::Integer(id)) => Some(*id),
            Value::Null => None,
            other => panic!("unexpected id {other:?}"),
        };
        let Value::Text(v) = row.get_value(1) else {
            panic!("expected text for v");
        };
        result.push((id, v.value.to_string()));
    }
    result
}

fn null_key_error(conn: &turso_pg::Connection, table: &str) -> String {
    conn.execute(format!("INSERT INTO {table} (id, v) VALUES (NULL, 'n')"))
        .unwrap_err()
        .to_string()
}

// VACUUM is refused in the PostgreSQL dialect. This test waits for VACUUM to be supported there.
// #[test]
// fn vacuum_keeps_the_keys_of_integer_primary_key_tables() {
//     let db = TempDatabase::builder().build();
//     let conn = db.connect_postgres();
//     conn.execute("CREATE TABLE fresh (id integer PRIMARY KEY DEFAULT 7, v text)")
//         .unwrap();
//     conn.execute("INSERT INTO fresh (id, v) VALUES (200, 'a'), (300, 'b')")
//         .unwrap();
//     conn.execute("DELETE FROM fresh WHERE v = 'a'").unwrap();
//     conn.inner()
//         .execute("PRAGMA wal_checkpoint(TRUNCATE)")
//         .unwrap();
//     conn.inner().execute("VACUUM").unwrap();
//
//     assert_eq!(key_rows(&conn, "fresh"), vec![(Some(300), "b".to_string())]);
//     assert!(null_key_error(&conn, "fresh").contains("null value"));
//     conn.close().unwrap();
//     drop(conn);
//
//     let db = db.reopen();
//     let conn = db.connect_postgres();
//     assert_eq!(key_rows(&conn, "fresh"), vec![(Some(300), "b".to_string())]);
//     assert!(null_key_error(&conn, "fresh").contains("null value"));
// }

#[test]
fn altered_table_still_rejects_a_null_integer_primary_key() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE fresh (id integer PRIMARY KEY, v text)")
        .unwrap();
    conn.execute("ALTER TABLE fresh ADD COLUMN w int").unwrap();
    assert!(null_key_error(&conn, "fresh").contains("null value"));
    conn.execute("ALTER TABLE fresh RENAME TO fresher").unwrap();
    assert!(null_key_error(&conn, "fresher").contains("null value"));
    conn.execute("INSERT INTO fresher (id, v) VALUES (5, 'a')")
        .unwrap();
    conn.close().unwrap();
    drop(conn);

    let db = db.reopen();
    let conn = db.connect_postgres();
    assert_eq!(key_rows(&conn, "fresher"), vec![(Some(5), "a".to_string())]);
    assert!(null_key_error(&conn, "fresher").contains("null value"));
}

#[test]
fn insert_that_fails_on_a_null_integer_primary_key_keeps_none_of_its_rows() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE t (id integer PRIMARY KEY, v text)")
        .unwrap();
    conn.execute("INSERT INTO t (id, v) VALUES (1, 'a')")
        .unwrap();
    let error = conn
        .execute("INSERT INTO t (id, v) VALUES (2, 'b'), (NULL, 'c')")
        .unwrap_err()
        .to_string();
    assert!(error.contains("null value"), "{error}");
    assert_eq!(key_rows(&conn, "t"), vec![(Some(1), "a".to_string())]);
}

#[test]
fn insert_into_a_serial_primary_key_needs_usage_on_its_sequence() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE t (id serial PRIMARY KEY, v text)")
        .unwrap();
    conn.execute("CREATE ROLE alice").unwrap();
    conn.execute("GRANT INSERT ON t TO alice").unwrap();
    conn.execute("SET ROLE alice").unwrap();
    let error = conn
        .execute("INSERT INTO t (v) VALUES ('a')")
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("permission denied for sequence t_id_seq"),
        "{error}"
    );
}

fn always_identity_error(conn: &turso_pg::Connection, table: &str) -> String {
    conn.execute(format!("INSERT INTO {table} (id) VALUES (42)"))
        .unwrap_err()
        .to_string()
}

#[test]
fn always_identity_rule_is_kept_after_reopen() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("CREATE SEQUENCE s").unwrap();
    conn.execute(
        "CREATE TABLE bound (id bigint GENERATED ALWAYS AS IDENTITY, v bigint DEFAULT nextval('s'))",
    )
    .unwrap();
    for table in ["added", "dropped", "renamed", "renamed_column"] {
        conn.execute(format!(
            "CREATE TABLE {table} (id bigint GENERATED ALWAYS AS IDENTITY, v text, w text)"
        ))
        .unwrap();
    }
    conn.execute("ALTER TABLE added ADD COLUMN x text").unwrap();
    conn.execute("ALTER TABLE dropped DROP COLUMN w").unwrap();
    conn.execute("ALTER TABLE renamed RENAME TO renamed2")
        .unwrap();
    conn.execute("ALTER TABLE renamed_column RENAME COLUMN v TO v2")
        .unwrap();
    conn.execute("CREATE TABLE shifted (v text, w text, id bigint GENERATED ALWAYS AS IDENTITY)")
        .unwrap();
    conn.execute("ALTER TABLE shifted DROP COLUMN v").unwrap();
    conn.close().unwrap();
    drop(conn);

    let db = db.reopen();
    let conn = db.connect_postgres();
    for table in [
        "bound",
        "added",
        "dropped",
        "renamed2",
        "renamed_column",
        "shifted",
    ] {
        assert!(
            always_identity_error(&conn, table)
                .contains("cannot insert a non-DEFAULT value into column \"id\""),
            "{table}"
        );
    }
    conn.execute("INSERT INTO bound (id, v) VALUES (DEFAULT, DEFAULT)")
        .unwrap();
    conn.execute("INSERT INTO shifted (w) VALUES ('a')")
        .unwrap();
    let mut rows = conn.query("SELECT id, v FROM bound").unwrap().unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected a row");
    };
    let row = rows.row().unwrap();
    assert_eq!(row.get_value(0), &Value::Numeric(Numeric::Integer(1)));
    assert_eq!(row.get_value(1), &Value::Numeric(Numeric::Integer(1)));
}

#[test]
fn identity_kind_is_kept_after_reopen() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute(
        "CREATE TABLE kinds (a int GENERATED ALWAYS AS IDENTITY, b int GENERATED BY DEFAULT AS IDENTITY, c text)",
    )
    .unwrap();
    conn.execute("ALTER TABLE kinds ADD COLUMN d text").unwrap();
    conn.close().unwrap();
    drop(conn);

    let db = db.reopen();
    let conn = db.connect_postgres();
    let mut rows = conn
        .query(
            "SELECT a.attname, a.attidentity FROM pg_attribute a \
             JOIN pg_class c ON c.oid = a.attrelid \
             WHERE c.relname = 'kinds' AND a.attnum > 0 ORDER BY a.attnum",
        )
        .unwrap()
        .unwrap();
    let mut kinds = Vec::new();
    while let StepResult::Row = rows.step().unwrap() {
        let row = rows.row().unwrap();
        kinds.push((row.get_value(0).to_string(), row.get_value(1).to_string()));
    }
    let kinds: Vec<(&str, &str)> = kinds
        .iter()
        .map(|(n, k)| (n.as_str(), k.as_str()))
        .collect();
    assert_eq!(kinds, [("a", "a"), ("b", "d"), ("c", ""), ("d", "")]);
}

/// VACUUM rebuilds the database from the stored text of its tables, which
/// the PostgreSQL frontend does not support yet.
#[test]
fn vacuum_is_refused() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE t (id int)").unwrap();
    for sql in ["VACUUM", "VACUUM FULL", "VACUUM t", "VACUUM (ANALYZE) t"] {
        let error = conn.execute(sql).unwrap_err();
        assert_eq!(error.sqlstate, "0A000", "{sql}: {error}");
    }
}

/// A primary key of one column of an integer type is the rowid of its table,
/// so it has no index of its own.
#[test]
fn primary_key_of_each_integer_type_is_the_rowid() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    let index_count = || {
        let mut rows = conn
            .inner()
            .prepare_sqlite(
                "SELECT count(*) FROM sqlite_schema WHERE type = 'index' AND tbl_name LIKE '%key_of_%'",
            )
            .unwrap();
        assert!(matches!(rows.step().unwrap(), StepResult::Row));
        rows.row().unwrap().get_value(0).to_string()
    };
    for ty in [
        "smallint",
        "integer",
        "bigint",
        "smallserial",
        "serial",
        "bigserial",
    ] {
        conn.execute(format!(
            "CREATE TABLE key_of_{ty} (id {ty} PRIMARY KEY, n {ty})"
        ))
        .unwrap();
        assert_eq!(index_count(), "0", "{ty}");
        conn.execute(format!("DROP TABLE key_of_{ty}")).unwrap();
    }

    conn.execute("CREATE TABLE key_of_two (a smallint, b bigint, PRIMARY KEY (a, b))")
        .unwrap();
    assert_eq!(index_count(), "1");
    conn.execute("CREATE INDEX key_of_two_b ON key_of_two (b)")
        .unwrap();
    assert_eq!(index_count(), "2");
}

// VACUUM is refused in the PostgreSQL dialect. This test waits for VACUUM to be supported there.
// #[test]
// fn vacuum_keeps_identity_values_and_the_always_rule() {
//     let db = TempDatabase::builder().build();
//     let conn = db.connect_postgres();
//     conn.execute("CREATE TABLE ident (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, v text)")
//         .unwrap();
//     conn.execute("INSERT INTO ident (v) VALUES ('a'), ('b')")
//         .unwrap();
//     conn.execute("DELETE FROM ident WHERE v = 'a'").unwrap();
//     conn.inner()
//         .execute("PRAGMA wal_checkpoint(TRUNCATE)")
//         .unwrap();
//     conn.inner().execute("VACUUM").unwrap();
//
//     assert_eq!(key_rows(&conn, "ident"), vec![(Some(2), "b".to_string())]);
//     conn.execute("INSERT INTO ident (v) VALUES ('c')").unwrap();
//     assert!(always_identity_error(&conn, "ident")
//         .contains("cannot insert a non-DEFAULT value into column \"id\""));
//     conn.close().unwrap();
//     drop(conn);
//
//     let db = db.reopen();
//     let conn = db.connect_postgres();
//     conn.execute("INSERT INTO ident (v) VALUES ('d')").unwrap();
//     assert_eq!(
//         key_rows(&conn, "ident"),
//         vec![
//             (Some(2), "b".to_string()),
//             (Some(3), "c".to_string()),
//             (Some(4), "d".to_string())
//         ]
//     );
// }
