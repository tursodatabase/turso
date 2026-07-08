use crate::common::TempDatabase;
use turso_core::{Numeric, StepResult, Value};

#[turso_macros::test]
fn test_postgres_pg_namespace(db: TempDatabase) {
    let conn = db.connect_postgres();

    // Switch to PostgreSQL dialect

    // Query pg_namespace virtual table
    let mut stmt = conn.prepare("SELECT * FROM pg_namespace").unwrap();

    // Should have at least pg_catalog and public namespaces
    let mut found_pg_catalog = false;
    let mut found_public = false;

    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                if let Value::Text(nspname) = row.get_value(1) {
                    if nspname.as_str() == "pg_catalog" {
                        found_pg_catalog = true;
                    } else if nspname.as_str() == "public" {
                        found_public = true;
                    }
                }
            }
            StepResult::Done => break,
            _ => {}
        }
    }

    assert!(found_pg_catalog, "pg_catalog namespace not found");
    assert!(found_public, "public namespace not found");
}

#[turso_macros::test]
fn test_postgres_pg_class(db: TempDatabase) {
    let conn = db.connect_postgres();

    // Create a test table in SQLite dialect first
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();

    // Switch to PostgreSQL dialect

    // Query pg_class virtual table
    let mut stmt = conn
        .prepare("SELECT relname, relkind FROM pg_class WHERE relkind = 'r'")
        .unwrap();

    // Should see our users table (once we implement the mapping)
    let mut _found_users_table = false;
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                if let (Value::Text(relname), Value::Text(relkind)) =
                    (row.get_value(0), row.get_value(1))
                {
                    if relname.as_str() == "users" && relkind.as_str() == "r" {
                        _found_users_table = true;
                    }
                }
            }
            StepResult::Done => break,
            _ => {}
        }
    }

    // For now this won't find the users table as we haven't implemented
    // the actual mapping from sqlite_master to pg_class yet
    // This is just testing that the virtual table exists and can be queried
}

#[turso_macros::test]
fn test_postgres_pg_attribute(db: TempDatabase) {
    let conn = db.connect_postgres();

    // Switch to PostgreSQL dialect

    // Query pg_attribute virtual table
    let mut stmt = conn.prepare("SELECT COUNT(*) FROM pg_attribute").unwrap();

    match stmt.step().unwrap() {
        StepResult::Row => {
            let row = stmt.row().unwrap();
            if let Value::Numeric(Numeric::Integer(count)) = row.get_value(0) {
                // For now should be 0 since we haven't implemented the mapping yet
                assert_eq!(*count, 0);
            }
        }
        _ => panic!("Expected row from COUNT query"),
    }
}

#[turso_macros::test]
fn test_postgres_pg_tables(db: TempDatabase) {
    let conn = db.connect_postgres();

    // Create test tables in SQLite dialect first
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER)")
        .unwrap();

    // Switch to PostgreSQL dialect

    // Query pg_tables — the standard PG way to list tables
    let mut stmt = conn
        .prepare("SELECT schemaname, tablename FROM pg_tables WHERE schemaname = 'public'")
        .unwrap();

    let mut found_users = false;
    let mut found_orders = false;

    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                let Value::Text(schemaname) = row.get_value(0) else {
                    panic!("expected text for schemaname");
                };
                let Value::Text(tablename) = row.get_value(1) else {
                    panic!("expected text for tablename");
                };
                assert_eq!(schemaname.as_str(), "public");
                if tablename.as_str() == "users" {
                    found_users = true;
                } else if tablename.as_str() == "orders" {
                    found_orders = true;
                }
            }
            StepResult::Done => break,
            _ => {}
        }
    }

    assert!(found_users, "users table not found in pg_tables");
    assert!(found_orders, "orders table not found in pg_tables");
}

#[turso_macros::test]
fn test_postgres_pg_tables_no_internal_tables(db: TempDatabase) {
    let conn = db.connect_postgres();

    // Create a user table
    conn.execute("CREATE TABLE mydata (id INTEGER PRIMARY KEY)")
        .unwrap();

    // Switch to PostgreSQL dialect

    // pg_tables should not expose internal sqlite_* tables
    let mut stmt = conn.prepare("SELECT tablename FROM pg_tables").unwrap();

    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                let Value::Text(tablename) = row.get_value(0) else {
                    panic!("expected text for tablename");
                };
                assert!(
                    !tablename.as_str().starts_with("sqlite_"),
                    "internal table {} should not appear in pg_tables",
                    tablename.as_str()
                );
            }
            StepResult::Done => break,
            _ => {}
        }
    }
}

// ──────────────────────────────────────────────────────────────────────
// pg_type tests
// ──────────────────────────────────────────────────────────────────────

#[turso_macros::test]
fn test_pg_type_has_builtin_types(db: TempDatabase) {
    let conn = db.connect_postgres();

    // Check well-known types exist with correct OIDs
    let cases = [("int4", 23), ("text", 25), ("bool", 16), ("uuid", 2950)];
    for (type_name, expected_oid) in cases {
        let mut stmt = conn
            .prepare(format!(
                "SELECT oid FROM pg_type WHERE typname = '{type_name}'"
            ))
            .unwrap();
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                let Value::Numeric(Numeric::Integer(oid)) = row.get_value(0) else {
                    panic!("expected integer oid for {type_name}");
                };
                assert_eq!(*oid, expected_oid, "wrong OID for {type_name}");
            }
            _ => panic!("{type_name} not found in pg_type"),
        }
    }
}

#[turso_macros::test]
fn test_pg_type_array_types(db: TempDatabase) {
    let conn = db.connect_postgres();

    // _int4 should exist with typelem pointing to int4 (oid=23)
    let mut stmt = conn
        .prepare("SELECT oid, typelem FROM pg_type WHERE typname = '_int4'")
        .unwrap();
    match stmt.step().unwrap() {
        StepResult::Row => {
            let row = stmt.row().unwrap();
            let Value::Numeric(Numeric::Integer(oid)) = row.get_value(0) else {
                panic!("expected integer oid");
            };
            let Value::Numeric(Numeric::Integer(typelem)) = row.get_value(1) else {
                panic!("expected integer typelem");
            };
            assert_eq!(*oid, 1007, "_int4 should have oid 1007");
            assert_eq!(*typelem, 23, "_int4 typelem should be 23 (int4)");
        }
        _ => panic!("_int4 not found in pg_type"),
    }

    // _text should exist with typelem pointing to text (oid=25)
    let mut stmt = conn
        .prepare("SELECT typelem FROM pg_type WHERE typname = '_text'")
        .unwrap();
    match stmt.step().unwrap() {
        StepResult::Row => {
            let row = stmt.row().unwrap();
            let Value::Numeric(Numeric::Integer(typelem)) = row.get_value(0) else {
                panic!("expected integer typelem");
            };
            assert_eq!(*typelem, 25, "_text typelem should be 25 (text)");
        }
        _ => panic!("_text not found in pg_type"),
    }
}

// ──────────────────────────────────────────────────────────────────────
// pg_index tests
// ──────────────────────────────────────────────────────────────────────

#[turso_macros::test]
fn test_pg_index_populated(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX idx_items_name ON items(name)")
        .unwrap();

    // Join pg_index with pg_class to get index name
    let mut stmt = conn
        .prepare(
            "SELECT c.relname, i.indkey, i.indisunique, i.indisprimary
             FROM pg_index i
             JOIN pg_class c ON c.oid = i.indexrelid
             WHERE c.relname = 'idx_items_name'",
        )
        .unwrap();

    match stmt.step().unwrap() {
        StepResult::Row => {
            let row = stmt.row().unwrap();
            let Value::Text(relname) = row.get_value(0) else {
                panic!("expected text relname");
            };
            let Value::Text(indkey) = row.get_value(1) else {
                panic!("expected text indkey");
            };
            let Value::Numeric(Numeric::Integer(indisunique)) = row.get_value(2) else {
                panic!("expected integer indisunique");
            };
            assert_eq!(relname.as_str(), "idx_items_name");
            // name is column 2 (1-based), so indkey should be "2"
            assert_eq!(
                indkey.as_str(),
                "2",
                "indkey should be 2 (name is 2nd column)"
            );
            assert_eq!(*indisunique, 0, "non-unique index");
        }
        _ => panic!("idx_items_name not found in pg_index join pg_class"),
    }
}

#[turso_macros::test]
fn test_pg_index_primary_key(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE pk_test (a TEXT, b TEXT, PRIMARY KEY (a, b))")
        .unwrap();

    let mut stmt = conn
        .prepare(
            "SELECT i.indisprimary, i.indisunique, i.indkey
             FROM pg_index i
             JOIN pg_class ct ON ct.oid = i.indrelid
             WHERE ct.relname = 'pk_test' AND i.indisprimary = 1",
        )
        .unwrap();

    match stmt.step().unwrap() {
        StepResult::Row => {
            let row = stmt.row().unwrap();
            let Value::Numeric(Numeric::Integer(indisprimary)) = row.get_value(0) else {
                panic!("expected integer");
            };
            let Value::Numeric(Numeric::Integer(indisunique)) = row.get_value(1) else {
                panic!("expected integer");
            };
            let Value::Text(indkey) = row.get_value(2) else {
                panic!("expected text indkey");
            };
            assert_eq!(*indisprimary, 1);
            assert_eq!(*indisunique, 1);
            assert_eq!(indkey.as_str(), "1 2", "PK columns a=1, b=2");
        }
        _ => panic!("primary key index not found for pk_test"),
    }
}

// ──────────────────────────────────────────────────────────────────────
// pg_constraint tests
// ──────────────────────────────────────────────────────────────────────

#[turso_macros::test]
fn test_pg_constraint_pk_and_fk(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.pragma_update("foreign_keys", "ON").unwrap();
    conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute(
        "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id) ON DELETE CASCADE)",
    )
    .unwrap();

    // Check PK constraint on parent
    let mut stmt = conn
        .prepare(
            "SELECT conname, contype FROM pg_constraint
             JOIN pg_class c ON c.oid = conrelid
             WHERE c.relname = 'parent' AND contype = 'p'",
        )
        .unwrap();

    match stmt.step().unwrap() {
        StepResult::Row => {
            let row = stmt.row().unwrap();
            let Value::Text(conname) = row.get_value(0) else {
                panic!("expected text conname");
            };
            let Value::Text(contype) = row.get_value(1) else {
                panic!("expected text contype");
            };
            assert_eq!(conname.as_str(), "parent_pkey");
            assert_eq!(contype.as_str(), "p");
        }
        _ => panic!("PK constraint not found for parent table"),
    }

    // Check FK constraint on child
    let mut stmt = conn
        .prepare(
            "SELECT conname, contype, confdeltype FROM pg_constraint
             JOIN pg_class c ON c.oid = conrelid
             WHERE c.relname = 'child' AND contype = 'f'",
        )
        .unwrap();

    match stmt.step().unwrap() {
        StepResult::Row => {
            let row = stmt.row().unwrap();
            let Value::Text(conname) = row.get_value(0) else {
                panic!("expected text conname");
            };
            let Value::Text(contype) = row.get_value(1) else {
                panic!("expected text contype");
            };
            let Value::Text(confdeltype) = row.get_value(2) else {
                panic!("expected text confdeltype");
            };
            assert!(
                conname.as_str().contains("fkey"),
                "FK name should contain 'fkey'"
            );
            assert_eq!(contype.as_str(), "f");
            assert_eq!(confdeltype.as_str(), "c", "ON DELETE CASCADE = 'c'");
        }
        _ => panic!("FK constraint not found for child table"),
    }
}

#[turso_macros::test]
fn test_pg_constraint_check(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE checked (id INTEGER, val INTEGER CHECK(val > 0))")
        .unwrap();

    let mut stmt = conn
        .prepare(
            "SELECT contype, conbin FROM pg_constraint
             JOIN pg_class c ON c.oid = conrelid
             WHERE c.relname = 'checked' AND contype = 'c'",
        )
        .unwrap();

    match stmt.step().unwrap() {
        StepResult::Row => {
            let row = stmt.row().unwrap();
            let Value::Text(contype) = row.get_value(0) else {
                panic!("expected text contype");
            };
            assert_eq!(contype.as_str(), "c", "should be CHECK constraint");
            // conbin should contain the check expression
            if let Value::Text(conbin) = row.get_value(1) {
                assert!(
                    conbin.as_str().contains("val") && conbin.as_str().contains("0"),
                    "conbin should reference val and 0, got: {}",
                    conbin.as_str()
                );
            }
        }
        _ => panic!("CHECK constraint not found for checked table"),
    }
}

// ──────────────────────────────────────────────────────────────────────
// pg_class index row tests
// ──────────────────────────────────────────────────────────────────────

#[turso_macros::test]
fn test_pg_class_includes_indexes(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE indexed (id INTEGER PRIMARY KEY, data TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX idx_indexed_data ON indexed(data)")
        .unwrap();

    // pg_class should have relkind='i' rows for indexes
    let mut stmt = conn
        .prepare(
            "SELECT relname, relkind, relam FROM pg_class
             WHERE relname = 'idx_indexed_data' AND relkind = 'i'",
        )
        .unwrap();

    match stmt.step().unwrap() {
        StepResult::Row => {
            let row = stmt.row().unwrap();
            let Value::Text(relname) = row.get_value(0) else {
                panic!("expected text");
            };
            let Value::Text(relkind) = row.get_value(1) else {
                panic!("expected text");
            };
            let Value::Numeric(Numeric::Integer(relam)) = row.get_value(2) else {
                panic!("expected integer relam");
            };
            assert_eq!(relname.as_str(), "idx_indexed_data");
            assert_eq!(relkind.as_str(), "i");
            assert_eq!(*relam, 403, "index relam should be 403 (btree)");
        }
        _ => panic!("idx_indexed_data not found in pg_class with relkind='i'"),
    }
}

/// Test that schema-qualified pg_catalog references work (e.g. `pg_catalog.pg_class`).
/// psql's `\dt` command sends queries like:
///   SELECT ... FROM pg_catalog.pg_class c
///     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
///   WHERE ... AND pg_catalog.pg_table_is_visible(c.oid)
/// This must not fail with "no such database: pg_catalog".
#[turso_macros::test]
fn test_pg_catalog_schema_qualified_tables(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE widgets (id INTEGER PRIMARY KEY, label TEXT)")
        .unwrap();

    // pg_catalog.pg_class — the core of psql \dt
    let mut stmt = conn
        .prepare("SELECT c.relname FROM pg_catalog.pg_class c WHERE c.relkind = 'r'")
        .unwrap();
    let mut found = false;
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                if let Value::Text(name) = row.get_value(0) {
                    if name.as_str() == "widgets" {
                        found = true;
                    }
                }
            }
            StepResult::Done => break,
            _ => {}
        }
    }
    assert!(found, "widgets table not found via pg_catalog.pg_class");
    drop(stmt);

    // pg_catalog.pg_namespace
    let mut stmt = conn
        .prepare("SELECT nspname FROM pg_catalog.pg_namespace")
        .unwrap();
    let mut found_public = false;
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                if let Value::Text(ns) = row.get_value(0) {
                    if ns.as_str() == "public" {
                        found_public = true;
                    }
                }
            }
            StepResult::Done => break,
            _ => {}
        }
    }
    assert!(
        found_public,
        "public namespace not found via pg_catalog.pg_namespace"
    );
    drop(stmt);

    // JOIN across schema-qualified catalog tables (simplified \dt query)
    let mut stmt = conn
        .prepare(
            "SELECT n.nspname, c.relname \
             FROM pg_catalog.pg_class c \
             LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
             WHERE c.relkind = 'r'",
        )
        .unwrap();
    let mut found = false;
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                if let Value::Text(name) = row.get_value(1) {
                    if name.as_str() == "widgets" {
                        found = true;
                    }
                }
            }
            StepResult::Done => break,
            _ => {}
        }
    }
    assert!(
        found,
        "widgets not found via pg_catalog.pg_class JOIN pg_catalog.pg_namespace"
    );
}

/// Test that `public.tablename` also resolves correctly (not as an ATTACH db).
#[turso_macros::test]
fn test_public_schema_qualified_tables(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE gadgets (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute("INSERT INTO gadgets (id, name) VALUES (1, 'phone')")
        .unwrap();

    let mut stmt = conn
        .prepare("SELECT name FROM public.gadgets WHERE id = 1")
        .unwrap();
    match stmt.step().unwrap() {
        StepResult::Row => {
            let row = stmt.row().unwrap();
            assert_eq!(row.get_value(0).to_string(), "phone");
        }
        _ => panic!("expected row from public.gadgets"),
    }
}

#[turso_macros::test]
fn test_format_type_expanded(db: TempDatabase) {
    let conn = db.connect_postgres();

    // Basic types
    let cases = vec![
        (16, "boolean"),
        (23, "integer"),
        (25, "text"),
        (114, "json"),
        (3802, "jsonb"),
        (2950, "uuid"),
        (1082, "date"),
        (1114, "timestamp without time zone"),
        (1184, "timestamp with time zone"),
        (1186, "interval"),
        (2278, "void"),
        (2205, "regclass"),
        (2206, "regtype"),
        (1000, "boolean[]"),
        (1007, "integer[]"),
        (1009, "text[]"),
    ];

    for (oid, expected) in cases {
        let mut stmt = conn
            .prepare(format!("SELECT format_type({oid}, -1)"))
            .unwrap();
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                assert_eq!(
                    row.get_value(0).to_string(),
                    expected,
                    "format_type({oid}, -1) should return '{expected}'"
                );
            }
            _ => panic!("expected row for format_type({oid}, -1)"),
        }
    }

    // varchar with typemod
    let mut stmt = conn.prepare("SELECT format_type(1043, 54)").unwrap();
    match stmt.step().unwrap() {
        StepResult::Row => {
            let row = stmt.row().unwrap();
            assert_eq!(row.get_value(0).to_string(), "character varying(50)");
        }
        _ => panic!("expected row for format_type with typemod"),
    }
}

#[turso_macros::test]
fn test_pg_type_is_visible(db: TempDatabase) {
    let conn = db.connect_postgres();

    let mut stmt = conn.prepare("SELECT pg_type_is_visible(23)").unwrap();
    match stmt.step().unwrap() {
        StepResult::Row => {
            let row = stmt.row().unwrap();
            assert_eq!(*row.get_value(0), Value::from_i64(1));
        }
        _ => panic!("expected row"),
    }
}

#[turso_macros::test]
fn test_lpad_rpad(db: TempDatabase) {
    let conn = db.connect_postgres();

    // lpad with fill char
    let mut stmt = conn.prepare("SELECT lpad('hi', 5, '*')").unwrap();
    match stmt.step().unwrap() {
        StepResult::Row => {
            let row = stmt.row().unwrap();
            assert_eq!(row.get_value(0).to_string(), "***hi");
        }
        _ => panic!("expected row"),
    }
    drop(stmt);

    // rpad with fill char
    let mut stmt = conn.prepare("SELECT rpad('hi', 5, '-')").unwrap();
    match stmt.step().unwrap() {
        StepResult::Row => {
            let row = stmt.row().unwrap();
            assert_eq!(row.get_value(0).to_string(), "hi---");
        }
        _ => panic!("expected row"),
    }
    drop(stmt);

    // lpad with default space fill
    let mut stmt = conn.prepare("SELECT lpad('hi', 5)").unwrap();
    match stmt.step().unwrap() {
        StepResult::Row => {
            let row = stmt.row().unwrap();
            assert_eq!(row.get_value(0).to_string(), "   hi");
        }
        _ => panic!("expected row"),
    }
    drop(stmt);

    // Truncation when string is longer than length
    let mut stmt = conn.prepare("SELECT lpad('hello world', 5)").unwrap();
    match stmt.step().unwrap() {
        StepResult::Row => {
            let row = stmt.row().unwrap();
            assert_eq!(row.get_value(0).to_string(), "hello");
        }
        _ => panic!("expected row"),
    }
}

#[turso_macros::test]
fn test_pg_get_constraintdef(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT UNIQUE)")
        .unwrap();
    conn.execute("CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id) ON DELETE CASCADE, age INTEGER CHECK(age > 0))")
        .unwrap();

    // Collect all constraint definitions
    let mut stmt = conn
        .prepare("SELECT conname, pg_get_constraintdef(oid) FROM pg_constraint")
        .unwrap();
    let mut defs: Vec<(String, String)> = Vec::new();
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                let name = row.get_value(0).to_string();
                let def = row.get_value(1).to_string();
                defs.push((name, def));
            }
            StepResult::Done => break,
            _ => {}
        }
    }

    // Check we got real definitions, not NULLs
    assert!(
        !defs.is_empty(),
        "expected constraint definitions, got: {defs:?}"
    );
    for (name, def) in &defs {
        assert!(
            !def.is_empty(),
            "constraint '{name}' should have a definition"
        );
    }

    // Find a PK constraint
    let has_pk = defs.iter().any(|(_, d)| d.starts_with("PRIMARY KEY"));
    assert!(
        has_pk,
        "should have a PRIMARY KEY constraint, got: {defs:?}"
    );

    // Find the FK constraint
    let has_fk = defs
        .iter()
        .any(|(_, d)| d.contains("FOREIGN KEY") && d.contains("REFERENCES"));
    assert!(
        has_fk,
        "should have a FOREIGN KEY constraint, got: {defs:?}"
    );
}

#[turso_macros::test]
fn test_pg_get_indexdef(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price REAL)")
        .unwrap();
    conn.execute("CREATE INDEX idx_items_name ON items(name)")
        .unwrap();
    conn.execute("CREATE UNIQUE INDEX idx_items_price ON items(price)")
        .unwrap();

    // Get index definitions via pg_class (indexes have relkind='i')
    let mut stmt = conn
        .prepare("SELECT pg_get_indexdef(oid) FROM pg_class WHERE relkind = 'i'")
        .unwrap();
    let mut defs: Vec<String> = Vec::new();
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                let def = row.get_value(0).to_string();
                defs.push(def);
            }
            StepResult::Done => break,
            _ => {}
        }
    }

    assert!(!defs.is_empty(), "expected index definitions");

    let has_name_idx = defs
        .iter()
        .any(|d| d.contains("idx_items_name") && d.contains("items") && d.contains("name"));
    assert!(has_name_idx, "should have idx_items_name definition");

    let has_unique_idx = defs
        .iter()
        .any(|d| d.contains("UNIQUE") && d.contains("idx_items_price"));
    assert!(
        has_unique_idx,
        "should have UNIQUE idx_items_price definition"
    );
}

#[turso_macros::test]
fn test_pg_attrdef_populated(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE defaults_test (id INTEGER PRIMARY KEY, name TEXT DEFAULT 'unnamed', score INTEGER DEFAULT 0)")
        .unwrap();

    let mut stmt = conn.prepare("SELECT adnum, adbin FROM pg_attrdef").unwrap();
    let mut rows: Vec<(i64, String)> = Vec::new();
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                let adnum = match row.get_value(0) {
                    Value::Numeric(Numeric::Integer(n)) => *n,
                    _ => panic!("expected integer adnum"),
                };
                let adbin = row.get_value(1).to_string();
                rows.push((adnum, adbin));
            }
            StepResult::Done => break,
            _ => {}
        }
    }

    assert!(
        rows.len() >= 2,
        "expected at least 2 default values, got {}",
        rows.len()
    );
}

// ---------------------------------------------------------------------------
// Tests for tables created in PG mode (not SQLite mode).
// These catch regressions where PG CREATE TABLE compiles but the bytecode
// is never executed (e.g. when DDL statements with 0 result columns are
// not stepped through).
// ---------------------------------------------------------------------------

#[turso_macros::test]
fn test_pg_create_table_visible_in_pg_tables(db: TempDatabase) {
    let conn = db.connect_postgres();

    // Create table purely in PG mode
    conn.execute("CREATE TABLE items (id INT PRIMARY KEY, name TEXT)")
        .unwrap();

    // Verify it appears in pg_tables
    let mut stmt = conn
        .prepare("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
        .unwrap();
    let mut found = false;
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                if let Value::Text(name) = row.get_value(0) {
                    if name.as_str() == "items" {
                        found = true;
                    }
                }
            }
            StepResult::Done => break,
            _ => {}
        }
    }
    assert!(found, "table created in PG mode not found in pg_tables");
}

#[turso_macros::test]
fn test_pg_create_table_visible_in_pg_class(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE widgets (id INT, label TEXT)")
        .unwrap();

    let mut stmt = conn
        .prepare("SELECT relname FROM pg_class WHERE relkind = 'r'")
        .unwrap();
    let mut found = false;
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                if let Value::Text(name) = row.get_value(0) {
                    if name.as_str() == "widgets" {
                        found = true;
                    }
                }
            }
            StepResult::Done => break,
            _ => {}
        }
    }
    assert!(found, "table created in PG mode not found in pg_class");
}

#[turso_macros::test]
fn test_pg_create_table_columns_in_pg_attribute(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price INT)")
        .unwrap();

    // Join pg_attribute + pg_class + pg_type to get column info (same query pgmicro \d uses)
    let mut stmt = conn
        .prepare(
            "SELECT a.attname, t.typname \
             FROM pg_attribute a \
             JOIN pg_class c ON a.attrelid = c.oid \
             JOIN pg_type t ON a.atttypid = t.oid \
             WHERE c.relname = 'products' AND a.attnum > 0 AND a.attisdropped = 0 \
             ORDER BY a.attnum",
        )
        .unwrap();

    let mut columns = Vec::new();
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                let name = row.get_value(0).to_string();
                let typ = row.get_value(1).to_string();
                columns.push((name, typ));
            }
            StepResult::Done => break,
            _ => {}
        }
    }

    assert_eq!(columns.len(), 3, "expected 3 columns, got {columns:?}");
    assert_eq!(columns[0].0, "id");
    assert_eq!(columns[1].0, "name");
    assert_eq!(columns[1].1, "text");
    assert_eq!(columns[2].0, "price");
}

#[turso_macros::test]
fn test_pg_create_table_then_insert_and_select(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE kv (k TEXT, v INT)").unwrap();
    conn.execute("INSERT INTO kv VALUES ('hello', 42)").unwrap();

    let mut stmt = conn.prepare("SELECT k, v FROM kv").unwrap();
    match stmt.step().unwrap() {
        StepResult::Row => {
            let row = stmt.row().unwrap();
            let Value::Text(k) = row.get_value(0) else {
                panic!("expected text");
            };
            assert_eq!(k.as_str(), "hello");
            let Value::Numeric(Numeric::Integer(v)) = row.get_value(1) else {
                panic!("expected integer");
            };
            assert_eq!(*v, 42);
        }
        _ => panic!("expected a row"),
    }
    assert!(
        matches!(stmt.step().unwrap(), StepResult::Done),
        "expected exactly one row"
    );
}

#[turso_macros::test]
fn test_pg_create_table_in_pg_database(db: TempDatabase) {
    let conn = db.connect_postgres();

    // pg_database should return at least one row
    let mut stmt = conn.prepare("SELECT datname FROM pg_database").unwrap();
    let mut found = false;
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                found = true;
            }
            StepResult::Done => break,
            _ => {}
        }
    }
    assert!(found, "pg_database should return at least one row");
}
