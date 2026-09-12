use super::*;
use crate::{Database, Numeric, PlatformIO, StepResult};
use tempfile::tempdir;

#[test]

fn test_pg_namespace_query() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let io = Arc::new(PlatformIO::new().unwrap());
    let db = crate::session::open_database_with_io(
        io,
        db_path.to_str().unwrap(),
        crate::OpenFlags::default(),
        crate::DatabaseOpts::new(),
    )
    .unwrap();
    let conn = crate::Connection::new(db.connect().unwrap());

    // Query pg_namespace
    let mut stmt = conn.prepare("SELECT * FROM pg_namespace").unwrap();

    let mut found_pg_catalog = false;
    let mut found_public = false;
    let mut found_information_schema = false;

    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                if let Value::Text(nspname) = row.get_value(1) {
                    match nspname.value.as_ref() {
                        "pg_catalog" => found_pg_catalog = true,
                        "public" => found_public = true,
                        "information_schema" => found_information_schema = true,
                        _ => {}
                    }
                }
            }
            StepResult::Done => break,
            _ => {}
        }
    }

    assert!(found_pg_catalog, "pg_catalog namespace not found");
    assert!(found_public, "public namespace not found");
    assert!(
        found_information_schema,
        "information_schema namespace not found"
    );
}

#[test]

fn test_pg_class_lists_user_tables() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let io = Arc::new(PlatformIO::new().unwrap());
    let db = crate::session::open_database_with_io(
        io,
        db_path.to_str().unwrap(),
        crate::OpenFlags::default(),
        crate::DatabaseOpts::new(),
    )
    .unwrap();
    let conn = db.connect().unwrap();

    // Create test tables in SQLite mode (default)
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute("CREATE TABLE products (id INTEGER, title TEXT, price REAL)")
        .unwrap();
    conn.execute("CREATE TABLE orders (id INTEGER, user_id INTEGER, product_id INTEGER)")
        .unwrap();

    let conn = crate::Connection::new(conn);

    // Query pg_class for regular tables
    let mut stmt = conn
        .prepare("SELECT relname FROM pg_class WHERE relkind = 'r' AND relnamespace = 2200")
        .unwrap();

    let mut tables = Vec::new();
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                if let Value::Text(relname) = row.get_value(0) {
                    tables.push(relname.to_string());
                }
            }
            StepResult::Done => break,
            _ => {}
        }
    }

    // Should find our three tables
    assert!(
        tables.contains(&"users".to_string()),
        "users table not found"
    );
    assert!(
        tables.contains(&"products".to_string()),
        "products table not found"
    );
    assert!(
        tables.contains(&"orders".to_string()),
        "orders table not found"
    );
    assert_eq!(tables.len(), 3, "Expected exactly 3 tables");
}

#[test]

fn test_pg_class_table_details() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let io = Arc::new(PlatformIO::new().unwrap());
    let db = crate::session::open_database_with_io(
        io,
        db_path.to_str().unwrap(),
        crate::OpenFlags::default(),
        crate::DatabaseOpts::new(),
    )
    .unwrap();
    let conn = db.connect().unwrap();

    // Create a test table with known columns
    conn.execute("CREATE TABLE test_table (id INTEGER, name TEXT, value REAL)")
        .unwrap();

    let conn = crate::Connection::new(conn);

    // Query pg_class for table details
    let mut stmt = conn
        .prepare(
            "SELECT oid, relname, relkind, relnatts
             FROM pg_class
             WHERE relname = 'test_table'",
        )
        .unwrap();

    if let StepResult::Row = stmt.step().unwrap() {
        let row = stmt.row().unwrap();
        let oid = if let Value::Numeric(Numeric::Integer(v)) = row.get_value(0) {
            *v
        } else {
            panic!("Expected OID")
        };
        let relname = if let Value::Text(v) = row.get_value(1) {
            v
        } else {
            panic!("Expected relname")
        };
        let relkind = if let Value::Text(v) = row.get_value(2) {
            v
        } else {
            panic!("Expected relkind")
        };
        let relnatts = if let Value::Numeric(Numeric::Integer(v)) = row.get_value(3) {
            *v
        } else {
            panic!("Expected relnatts")
        };

        assert!(oid >= 16384, "OID should be >= 16384 for user tables");
        assert_eq!(relname.value, "test_table", "Table name should match");
        assert_eq!(
            relkind.value, "r",
            "relkind should be 'r' for regular table"
        );
        assert_eq!(relnatts, 3, "Table should have 3 columns");
    } else {
        panic!("test_table not found in pg_class");
    }
}

#[test]

fn test_sqlite_tables_hidden_in_postgres_mode() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let io = Arc::new(PlatformIO::new().unwrap());
    let db = crate::session::open_database_with_io(
        io,
        db_path.to_str().unwrap(),
        crate::OpenFlags::default(),
        crate::DatabaseOpts::new(),
    )
    .unwrap();
    let conn = db.connect().unwrap();

    // Create a test table
    conn.execute("CREATE TABLE test_table (id INTEGER)")
        .unwrap();

    let conn = crate::Connection::new(conn);

    // Try to query sqlite_master - should fail
    let result = conn.prepare("SELECT * FROM sqlite_master");
    assert!(
        result.is_err(),
        "sqlite_master should not be accessible in PostgreSQL mode"
    );

    // Try to query sqlite_schema - should also fail
    let result = conn.prepare("SELECT * FROM sqlite_schema");
    assert!(
        result.is_err(),
        "sqlite_schema should not be accessible in PostgreSQL mode"
    );
}

#[test]

fn test_postgres_tables_hidden_in_sqlite_mode() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let io = Arc::new(PlatformIO::new().unwrap());
    // Open with the SQLite dialect, whose catalog has no pg_* tables.
    let db = Database::open_file_with_flags(
        io,
        db_path.to_str().unwrap(),
        crate::OpenFlags::default(),
        crate::DatabaseOpts::new(),
        None,
        Arc::new(turso_core::SqliteDialect),
    )
    .unwrap();
    let conn = db.connect().unwrap();

    // Try to query pg_class - should fail
    let result = conn.prepare("SELECT * FROM pg_class");
    assert!(
        result.is_err(),
        "pg_class should not be accessible in SQLite mode"
    );

    // Try to query pg_namespace - should fail
    let result = conn.prepare("SELECT * FROM pg_namespace");
    assert!(
        result.is_err(),
        "pg_namespace should not be accessible in SQLite mode"
    );

    // sqlite_master should work
    let result = conn.prepare("SELECT * FROM sqlite_master");
    assert!(
        result.is_ok(),
        "sqlite_master should be accessible in SQLite mode"
    );
}

/// User-created tables are visible through both the SQLite-side catalog
/// (`sqlite_master`) on a SQLite-mode connection and the PG-side catalog
/// (`pg_class`) on a PostgreSQL-mode connection. Each direction uses its
/// own connection — dialect is fixed per connection and we don't
/// hot-swap at runtime.
#[test]
fn user_table_is_listed_in_dialect_specific_catalog() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let io = Arc::new(PlatformIO::new().unwrap());
    let db = crate::session::open_database_with_io(
        io,
        db_path.to_str().unwrap(),
        crate::OpenFlags::default(),
        crate::DatabaseOpts::new(),
    )
    .unwrap();

    // SQLite-mode connection: sqlite_master sees the user table.
    let sqlite_conn = db.connect().unwrap();
    sqlite_conn
        .execute("CREATE TABLE users (id INTEGER, name TEXT)")
        .unwrap();
    let mut stmt = sqlite_conn
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table'")
        .unwrap();
    let mut found = false;
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                if let Value::Text(name) = stmt.row().unwrap().get_value(0) {
                    if name.value == "users" {
                        found = true;
                    }
                }
            }
            StepResult::Done => break,
            _ => {}
        }
    }
    assert!(found, "users table not found in sqlite_master");

    // PostgreSQL-mode connection: pg_class sees the user table.
    let pg_conn = crate::Connection::new(db.connect().unwrap());
    let mut stmt = pg_conn
        .prepare("SELECT relname FROM pg_class WHERE relkind = 'r'")
        .unwrap();
    let mut found = false;
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                if let Value::Text(name) = stmt.row().unwrap().get_value(0) {
                    if name.value == "users" {
                        found = true;
                    }
                }
            }
            StepResult::Done => break,
            _ => {}
        }
    }
    assert!(found, "users table not found in pg_class");
}

#[test]

fn test_pg_class_with_where_constraints() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let io = Arc::new(PlatformIO::new().unwrap());
    let db = crate::session::open_database_with_io(
        io,
        db_path.to_str().unwrap(),
        crate::OpenFlags::default(),
        crate::DatabaseOpts::new(),
    )
    .unwrap();
    let conn = db.connect().unwrap();

    // Create multiple tables
    conn.execute("CREATE TABLE table1 (id INTEGER)").unwrap();
    conn.execute("CREATE TABLE table2 (id INTEGER, name TEXT)")
        .unwrap();
    conn.execute("CREATE TABLE table3 (id INTEGER, name TEXT, value REAL)")
        .unwrap();

    let conn = crate::Connection::new(conn);

    // Test various WHERE clause combinations

    // Test 1: Filter by relkind = 'r'
    let mut stmt = conn
        .prepare("SELECT COUNT(*) FROM pg_class WHERE relkind = 'r'")
        .unwrap();
    match stmt.step().unwrap() {
        StepResult::Row => {
            let row = stmt.row().unwrap();
            if let Value::Numeric(Numeric::Integer(count)) = row.get_value(0) {
                assert_eq!(*count, 3, "Should have 3 regular tables");
            }
        }
        _ => panic!("Expected row from COUNT query"),
    }

    // Test 2: Filter by relnamespace = 2200 (public schema)
    let mut stmt = conn
        .prepare("SELECT COUNT(*) FROM pg_class WHERE relnamespace = 2200")
        .unwrap();
    match stmt.step().unwrap() {
        StepResult::Row => {
            let row = stmt.row().unwrap();
            if let Value::Numeric(Numeric::Integer(count)) = row.get_value(0) {
                assert_eq!(*count, 3, "Should have 3 tables in public schema");
            }
        }
        _ => panic!("Expected row from COUNT query"),
    }

    // Test 3: Combined filters
    let mut stmt = conn.prepare("SELECT relname FROM pg_class WHERE relkind = 'r' AND relnamespace = 2200 ORDER BY relname").unwrap();
    let mut tables = Vec::new();
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                if let Value::Text(name) = row.get_value(0) {
                    tables.push(name.to_string());
                }
            }
            StepResult::Done => break,
            _ => {}
        }
    }
    assert_eq!(tables, vec!["table1", "table2", "table3"]);
}

#[test]
fn test_pg_tables_lists_user_tables() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let io = Arc::new(PlatformIO::new().unwrap());
    let db = crate::session::open_database_with_io(
        io,
        db_path.to_str().unwrap(),
        crate::OpenFlags::default(),
        crate::DatabaseOpts::new(),
    )
    .unwrap();
    let conn = db.connect().unwrap();

    // Create test tables
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute("CREATE TABLE orders (id INTEGER, user_id INTEGER)")
        .unwrap();

    let conn = crate::Connection::new(conn);

    // Query pg_tables
    let mut stmt = conn
        .prepare("SELECT schemaname, tablename FROM pg_tables WHERE schemaname = 'public'")
        .unwrap();

    let mut tables = Vec::new();
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                if let (Value::Text(schema), Value::Text(name)) =
                    (row.get_value(0), row.get_value(1))
                {
                    assert_eq!(schema.as_str(), "public");
                    tables.push(name.to_string());
                }
            }
            StepResult::Done => break,
            _ => {}
        }
    }

    tables.sort();
    assert_eq!(tables, vec!["orders", "users"]);
}

#[test]
fn test_pg_tables_excludes_internal_tables() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let io = Arc::new(PlatformIO::new().unwrap());
    let db = crate::session::open_database_with_io(
        io,
        db_path.to_str().unwrap(),
        crate::OpenFlags::default(),
        crate::DatabaseOpts::new(),
    )
    .unwrap();
    let conn = db.connect().unwrap();

    conn.execute("CREATE TABLE mydata (id INTEGER PRIMARY KEY)")
        .unwrap();

    let conn = crate::Connection::new(conn);

    let mut stmt = conn.prepare("SELECT tablename FROM pg_tables").unwrap();

    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                if let Value::Text(name) = row.get_value(0) {
                    assert!(
                        !name.as_str().starts_with("sqlite_"),
                        "internal table {} should not appear in pg_tables",
                        name.as_str()
                    );
                }
            }
            StepResult::Done => break,
            _ => {}
        }
    }
}
