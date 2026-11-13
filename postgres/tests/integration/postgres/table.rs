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

#[test]
fn vacuum_keeps_the_keys_of_integer_primary_key_tables() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE fresh (id integer PRIMARY KEY DEFAULT 7, v text)")
        .unwrap();
    conn.execute("INSERT INTO fresh (id, v) VALUES (200, 'a'), (300, 'b')")
        .unwrap();
    conn.execute("DELETE FROM fresh WHERE v = 'a'").unwrap();
    conn.inner()
        .execute("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
    conn.inner().execute("VACUUM").unwrap();

    assert_eq!(key_rows(&conn, "fresh"), vec![(Some(300), "b".to_string())]);
    assert!(null_key_error(&conn, "fresh").contains("null value"));
    conn.close().unwrap();
    drop(conn);

    let db = db.reopen();
    let conn = db.connect_postgres();
    assert_eq!(key_rows(&conn, "fresh"), vec![(Some(300), "b".to_string())]);
    assert!(null_key_error(&conn, "fresh").contains("null value"));
}

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
