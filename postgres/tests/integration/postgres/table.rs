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
