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
fn integer_primary_key_tables_written_before_the_column_rule_keep_their_rowid() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("old.db");
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute_batch(
            "CREATE TABLE __turso_internal_classes (oid INTEGER PRIMARY KEY, \
                 relname TEXT NOT NULL, relnamespace INTEGER NOT NULL, \
                 relkind TEXT NOT NULL, relowner INTEGER NOT NULL, \
                 relownedby INTEGER NOT NULL, relacl TEXT, \
                 relrowsecurity INTEGER NOT NULL DEFAULT 0, relpolicies TEXT);
             CREATE TABLE __turso_ns1_plain (id integer PRIMARY KEY, v text);
             CREATE TABLE __turso_ns1_canonical (id INTEGER PRIMARY KEY, v TEXT);
             CREATE TABLE __turso_ns1_renamed (id integer PRIMARY KEY, v text);
             CREATE TABLE sequence_seed (id INTEGER PRIMARY KEY AUTOINCREMENT);
             DROP TABLE sequence_seed;
             INSERT INTO sqlite_sequence (name, seq) VALUES ('__turso_internal_classes', 16386);
             CREATE UNIQUE INDEX __turso_internal_classes_name
                 ON __turso_internal_classes (relnamespace, relname);
             INSERT INTO __turso_internal_classes
                 (oid, relname, relnamespace, relkind, relowner, relownedby)
             VALUES (16384, 'plain', 1, 'r', 0, 0),
                    (16385, 'canonical', 1, 'r', 0, 0),
                    (16386, 'renamed', 1, 'r', 0, 0);
             INSERT INTO __turso_ns1_plain (id, v) VALUES (1, 'a'), (7, 'b');
             INSERT INTO __turso_ns1_canonical (id, v) VALUES (3, 'a'), (9, 'b');
             INSERT INTO __turso_ns1_renamed (id, v) VALUES (4, 'a');
             PRAGMA writable_schema = ON;
             UPDATE sqlite_schema SET sql = '/* turso_frontend:postgres */ ' || sql
              WHERE name IN ('__turso_ns1_plain', '__turso_ns1_renamed');",
        )
        .unwrap();

    let io: std::sync::Arc<dyn turso_core::IO + Send> =
        std::sync::Arc::new(turso_core::PlatformIO::new().unwrap());
    let opts = turso_core::DatabaseOpts::new()
        .with_encryption(true)
        .with_custom_types(true);
    let db = turso_pg::open_database_with_io(
        io.clone(),
        path.to_str().unwrap(),
        turso_core::OpenFlags::default(),
        opts,
    )
    .unwrap();
    let db = TempDatabase { path, io, db };
    let conn = db.connect_postgres();

    assert_eq!(
        key_rows(&conn, "plain"),
        vec![(Some(1), "a".to_string()), (Some(7), "b".to_string())]
    );
    assert_eq!(
        key_rows(&conn, "canonical"),
        vec![(Some(3), "a".to_string()), (Some(9), "b".to_string())]
    );

    conn.execute("INSERT INTO plain (id, v) VALUES (8, 'c')")
        .unwrap();
    conn.execute("ALTER TABLE plain ADD COLUMN w int").unwrap();
    conn.execute("ALTER TABLE renamed RENAME TO renamed2")
        .unwrap();
    conn.execute("INSERT INTO canonical (id, v) VALUES (10, 'c')")
        .unwrap();
    conn.inner()
        .execute("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
    conn.close().unwrap();
    drop(conn);

    let db = db.reopen();
    let conn = db.connect_postgres();
    assert_eq!(
        key_rows(&conn, "plain"),
        vec![
            (Some(1), "a".to_string()),
            (Some(7), "b".to_string()),
            (Some(8), "c".to_string())
        ]
    );
    assert_eq!(
        key_rows(&conn, "canonical"),
        vec![
            (Some(3), "a".to_string()),
            (Some(9), "b".to_string()),
            (Some(10), "c".to_string())
        ]
    );
    assert_eq!(
        key_rows(&conn, "renamed2"),
        vec![(Some(4), "a".to_string())]
    );

    conn.execute("CREATE TABLE fresh (id integer PRIMARY KEY, v text)")
        .unwrap();
    assert!(null_key_error(&conn, "fresh").contains("null value"));
}

#[test]
fn vacuum_keeps_the_keys_of_integer_primary_key_tables() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE fresh (id integer PRIMARY KEY DEFAULT 7, v text)")
        .unwrap();
    conn.execute("INSERT INTO fresh (id, v) VALUES (200, 'a')")
        .unwrap();
    conn.execute("CREATE TABLE ident (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, v text)")
        .unwrap();
    conn.execute("INSERT INTO ident (v) VALUES ('a'), ('b')")
        .unwrap();
    conn.execute("DELETE FROM ident WHERE v = 'a'").unwrap();
    conn.inner()
        .execute("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
    conn.inner().execute("VACUUM").unwrap();

    assert_eq!(key_rows(&conn, "fresh"), vec![(Some(200), "a".to_string())]);
    assert_eq!(key_rows(&conn, "ident"), vec![(Some(2), "b".to_string())]);
    assert!(null_key_error(&conn, "fresh").contains("null value"));
    conn.execute("INSERT INTO ident (v) VALUES ('c')").unwrap();
    conn.close().unwrap();
    drop(conn);

    let db = db.reopen();
    let conn = db.connect_postgres();
    assert_eq!(key_rows(&conn, "fresh"), vec![(Some(200), "a".to_string())]);
    conn.execute("INSERT INTO ident (v) VALUES ('d')").unwrap();
    assert_eq!(
        key_rows(&conn, "ident"),
        vec![
            (Some(2), "b".to_string()),
            (Some(3), "c".to_string()),
            (Some(4), "d".to_string())
        ]
    );
    assert!(null_key_error(&conn, "fresh").contains("null value"));
}

#[test]
fn integer_primary_key_column_rule_is_kept_when_the_table_is_altered() {
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

#[test]
fn drop_sequence_of_an_identity_column_is_refused() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE owner (id int GENERATED ALWAYS AS IDENTITY, label text)")
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
fn refused_drop_leaves_the_sequence_and_the_other_default_working() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE owner (id int GENERATED ALWAYS AS IDENTITY, label text)")
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
