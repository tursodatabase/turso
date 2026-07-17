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
