use crate::common::TempDatabase;
use turso_core::{Numeric, StepResult, Value};

#[turso_macros::test(mvcc)]
fn test_postgres_frontend_rejects_pragma(db: TempDatabase) {
    let conn = db.connect_postgres();

    // PostgreSQL sessions route through the frontend directly.
    let mut rows = conn.query("SELECT 42").unwrap().unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    let Value::Numeric(Numeric::Integer(value)) = row.get_value(0) else {
        panic!("expected integer value");
    };
    assert_eq!(*value, 42);
    drop(rows);

    // Test that PostgreSQL parser rejects PRAGMA statements
    let result = conn.query("PRAGMA table_info(test)");
    assert!(
        result.is_err(),
        "PostgreSQL parser should reject PRAGMA statements"
    );
}

#[turso_macros::test(mvcc)]
fn test_postgres_simple_select_literal(db: TempDatabase) {
    let conn = db.connect_postgres();

    // Test the simplest possible PostgreSQL query - SELECT literal
    let mut rows = conn.query("SELECT 1").unwrap().unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    let Value::Numeric(Numeric::Integer(value)) = row.get_value(0) else {
        panic!("expected integer value");
    };
    assert_eq!(*value, 1);
}

#[turso_macros::test(mvcc)]
fn test_postgres_comment_on_supported_object_forms_are_noops(db: TempDatabase) {
    let conn = db.connect_postgres();

    // COMMENT ON is intentionally accepted without validating or storing the
    // referenced object, so cover the distinct PostgreSQL grammar forms here.
    for (object_kind, sql) in [
        ("table", "COMMENT ON TABLE comment_docs IS 'documentation'"),
        (
            "column",
            "COMMENT ON COLUMN comment_docs.title IS 'column documentation'",
        ),
        (
            "constraint",
            "COMMENT ON CONSTRAINT comment_docs_pkey ON comment_docs IS 'constraint documentation'",
        ),
        (
            "database",
            "COMMENT ON DATABASE main IS 'database documentation'",
        ),
        (
            "domain",
            "COMMENT ON DOMAIN email IS 'domain documentation'",
        ),
        ("type", "COMMENT ON TYPE status IS 'type documentation'"),
        (
            "index",
            "COMMENT ON INDEX comment_docs_idx IS 'index documentation'",
        ),
        (
            "view",
            "COMMENT ON VIEW comment_view IS 'view documentation'",
        ),
        (
            "materialized view",
            "COMMENT ON MATERIALIZED VIEW comment_matview IS 'materialized view documentation'",
        ),
        (
            "schema",
            "COMMENT ON SCHEMA accounting IS 'schema documentation'",
        ),
        (
            "sequence",
            "COMMENT ON SEQUENCE comment_docs_id_seq IS 'sequence documentation'",
        ),
        (
            "trigger",
            "COMMENT ON TRIGGER audit_changes ON comment_docs IS 'trigger documentation'",
        ),
        (
            "function",
            "COMMENT ON FUNCTION format_comment(integer) IS 'function documentation'",
        ),
        (
            "cast",
            "COMMENT ON CAST (text AS integer) IS 'cast documentation'",
        ),
        (
            "operator",
            "COMMENT ON OPERATOR + (integer, integer) IS 'operator documentation'",
        ),
        (
            "operator class",
            "COMMENT ON OPERATOR CLASS int4_ops USING btree IS 'operator class documentation'",
        ),
        (
            "policy",
            "COMMENT ON POLICY access_policy ON comment_docs IS 'policy documentation'",
        ),
        (
            "rule",
            "COMMENT ON RULE update_rule ON comment_docs IS 'rule documentation'",
        ),
        (
            "language",
            "COMMENT ON LANGUAGE plpgsql IS 'language documentation'",
        ),
    ] {
        conn.execute(sql)
            .unwrap_or_else(|err| panic!("COMMENT ON {object_kind} should be accepted: {err}"));
    }
}

#[turso_macros::test(mvcc)]
fn test_postgres_arithmetic_expression(db: TempDatabase) {
    let conn = db.connect_postgres();

    // Test simple arithmetic in PostgreSQL dialect
    let mut rows = conn.query("SELECT 2 + 3").unwrap().unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    let Value::Numeric(Numeric::Integer(result)) = row.get_value(0) else {
        panic!("expected integer value");
    };
    assert_eq!(*result, 5);
}

#[turso_macros::test(mvcc)]
fn test_postgres_parser_integration(db: TempDatabase) {
    let conn = db.connect_postgres();

    // Test that PostgreSQL parser rejects PRAGMA statements (PostgreSQL doesn't support them)
    let result = conn.query("PRAGMA table_info(test)");
    assert!(
        result.is_err(),
        "PostgreSQL parser should reject PRAGMA statements"
    );

    // PRAGMA sql_dialect should also be rejected in PG mode
    let result = conn.query("PRAGMA sql_dialect");
    assert!(
        result.is_err(),
        "PRAGMA sql_dialect should be rejected in PostgreSQL mode"
    );

    // But should accept PostgreSQL-style comments
    let mut rows = conn
        .query("SELECT 42 -- PostgreSQL comment")
        .unwrap()
        .unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    let Value::Numeric(Numeric::Integer(value)) = row.get_value(0) else {
        panic!("expected integer value");
    };
    assert_eq!(*value, 42);
}

#[turso_macros::test]
fn test_postgres_create_schema(db: TempDatabase) {
    let conn = db.connect_postgres();

    // Create a new schema
    conn.execute("CREATE SCHEMA myschema").unwrap();

    // Create a table in the new schema
    conn.execute("CREATE TABLE myschema.test (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();

    // Insert data
    conn.execute("INSERT INTO myschema.test (id, name) VALUES (1, 'alice')")
        .unwrap();

    // Query the table
    let mut rows = conn
        .query("SELECT name FROM myschema.test WHERE id = 1")
        .unwrap()
        .unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    let Value::Text(name) = row.get_value(0) else {
        panic!("expected text value");
    };
    assert_eq!(name.as_str(), "alice");
}

/// Test all forms of qualified name resolution in PG dialect:
///   column           — unqualified
///   table.column     — singly qualified
///   schema.table.column — doubly qualified (DoublyQualified)
///   schema.table     — schema-qualified table in FROM / DML
#[turso_macros::test]
fn test_postgres_qualified_name_forms(db: TempDatabase) {
    let conn = db.connect_postgres();

    // ── public schema (unqualified and table.column) ──
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT)")
        .unwrap();
    conn.execute("INSERT INTO items (id, label) VALUES (1, 'pen')")
        .unwrap();

    // column only
    let mut rows = conn.query("SELECT label FROM items").unwrap().unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    assert_eq!(rows.row().unwrap().get_value(0).to_string(), "pen");
    drop(rows);

    // table.column in SELECT and WHERE
    let mut rows = conn
        .query("SELECT items.label FROM items WHERE items.id = 1")
        .unwrap()
        .unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    assert_eq!(rows.row().unwrap().get_value(0).to_string(), "pen");
    drop(rows);

    // ── named schema (schema.table and schema.table.column) ──
    conn.execute("CREATE SCHEMA s").unwrap();
    conn.execute("CREATE TABLE s.t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute("INSERT INTO s.t (id, name) VALUES (1, 'alice')")
        .unwrap();

    // schema.table in FROM, unqualified column
    let mut rows = conn.query("SELECT name FROM s.t").unwrap().unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    assert_eq!(rows.row().unwrap().get_value(0).to_string(), "alice");
    drop(rows);

    // schema.table.column in SELECT
    let mut rows = conn
        .query("SELECT s.t.name FROM s.t WHERE s.t.id = 1")
        .unwrap()
        .unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    assert_eq!(rows.row().unwrap().get_value(0).to_string(), "alice");
    drop(rows);

    // schema.table.column in WHERE only
    let mut rows = conn
        .query("SELECT name FROM s.t WHERE s.t.id = 1")
        .unwrap()
        .unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    assert_eq!(rows.row().unwrap().get_value(0).to_string(), "alice");
    drop(rows);

    // schema.table.column in both SELECT and WHERE
    let mut rows = conn
        .query("SELECT s.t.id, s.t.name FROM s.t WHERE s.t.name = 'alice'")
        .unwrap()
        .unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    let Value::Numeric(Numeric::Integer(id)) = row.get_value(0) else {
        panic!("expected integer value");
    };
    assert_eq!(*id, 1);
    assert_eq!(row.get_value(1).to_string(), "alice");
}

#[turso_macros::test]
fn test_postgres_create_schema_if_not_exists(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE SCHEMA myschema").unwrap();

    // Should succeed (no-op)
    conn.execute("CREATE SCHEMA IF NOT EXISTS myschema")
        .unwrap();

    // Without IF NOT EXISTS should fail
    let result = conn.execute("CREATE SCHEMA myschema");
    assert!(result.is_err());
}

#[turso_macros::test]
fn test_postgres_create_schema_public_error(db: TempDatabase) {
    let conn = db.connect_postgres();

    // CREATE SCHEMA public should fail (already exists)
    let result = conn.execute("CREATE SCHEMA public");
    assert!(result.is_err());

    // IF NOT EXISTS should succeed (no-op)
    conn.execute("CREATE SCHEMA IF NOT EXISTS public").unwrap();
}

#[turso_macros::test]
fn test_postgres_drop_schema(db: TempDatabase) {
    let conn = db.connect_postgres();

    // Create and then drop
    conn.execute("CREATE SCHEMA testschema").unwrap();
    conn.execute("DROP SCHEMA testschema").unwrap();

    // Re-creating should succeed (proves it was dropped)
    conn.execute("CREATE SCHEMA testschema").unwrap();
}

#[turso_macros::test]
fn test_postgres_drop_schema_if_exists(db: TempDatabase) {
    let conn = db.connect_postgres();

    // Drop a non-existent schema with IF EXISTS should not error
    conn.execute("DROP SCHEMA IF EXISTS nonexistent").unwrap();

    // Drop a non-existent schema without IF EXISTS should error
    let result = conn.execute("DROP SCHEMA nonexistent");
    assert!(result.is_err());
}

#[turso_macros::test]
fn test_postgres_drop_schema_cascade(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE SCHEMA dropme").unwrap();

    // Schema-qualified DDL works in PG dialect
    conn.execute("CREATE TABLE dropme.t1 (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE dropme.t2 (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO dropme.t1 (id) VALUES (1)")
        .unwrap();

    conn.execute("DROP SCHEMA dropme CASCADE").unwrap();

    // Re-creating should succeed (proves the schema was fully dropped)
    conn.execute("CREATE SCHEMA dropme").unwrap();
}

#[turso_macros::test]
fn test_postgres_drop_schema_public_cascade(db: TempDatabase) {
    let conn = db.connect_postgres();

    // Create some tables in public schema
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute("CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT)")
        .unwrap();
    conn.execute("INSERT INTO users VALUES (1, 'alice')")
        .unwrap();

    // DROP SCHEMA public CASCADE should drop all user tables
    conn.execute("DROP SCHEMA public CASCADE").unwrap();

    // Tables should be gone
    let result = conn.query("SELECT * FROM users");
    assert!(result.is_err());
    let result = conn.query("SELECT * FROM posts");
    assert!(result.is_err());

    // But we should still be able to create new tables (public schema still exists)
    conn.execute("CREATE TABLE newtable (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO newtable VALUES (1)").unwrap();
    let mut rows = conn.query("SELECT id FROM newtable").unwrap().unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
}

#[turso_macros::test]
fn test_postgres_drop_schema_public_no_cascade_error(db: TempDatabase) {
    let conn = db.connect_postgres();

    // Create a table in public schema
    conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY)")
        .unwrap();

    // DROP SCHEMA public without CASCADE should fail when tables exist
    let result = conn.execute("DROP SCHEMA public");
    assert!(result.is_err());

    // Table should still be there
    conn.execute("INSERT INTO t1 VALUES (1)").unwrap();
    let mut rows = conn.query("SELECT id FROM t1").unwrap().unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
}

// ==================== ALTER TABLE ====================

#[turso_macros::test(mvcc)]
fn test_postgres_alter_table_rename(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE old_name (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute("INSERT INTO old_name (id, val) VALUES (1, 'hello')")
        .unwrap();

    conn.execute("ALTER TABLE old_name RENAME TO new_name")
        .unwrap();

    // Old name should fail
    let result = conn.query("SELECT * FROM old_name");
    assert!(result.is_err());

    // New name should work
    let mut rows = conn.query("SELECT val FROM new_name").unwrap().unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    let Value::Text(val) = row.get_value(0) else {
        panic!("expected text");
    };
    assert_eq!(val.value, "hello");
}

#[turso_macros::test(mvcc)]
fn test_postgres_alter_table_add_column(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO t (id) VALUES (1)").unwrap();

    conn.execute("ALTER TABLE t ADD COLUMN name TEXT").unwrap();

    // Insert with new column
    conn.execute("INSERT INTO t (id, name) VALUES (2, 'alice')")
        .unwrap();

    // Query - old row should have NULL for new column
    let mut rows = conn
        .query("SELECT id, name FROM t ORDER BY id")
        .unwrap()
        .unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    let Value::Null = row.get_value(1) else {
        panic!("expected null for old row");
    };

    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected second row");
    };
    let row = rows.row().unwrap();
    let Value::Text(name) = row.get_value(1) else {
        panic!("expected text");
    };
    assert_eq!(name.value, "alice");
}

#[turso_macros::test(mvcc)]
fn test_postgres_alter_table_drop_column(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t (id, name, age) VALUES (1, 'alice', 30)")
        .unwrap();

    conn.execute("ALTER TABLE t DROP COLUMN age").unwrap();

    // age column should no longer exist
    let result = conn.query("SELECT age FROM t");
    assert!(result.is_err());

    // Other columns should still work
    let mut rows = conn.query("SELECT id, name FROM t").unwrap().unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    let Value::Text(name) = row.get_value(1) else {
        panic!("expected text");
    };
    assert_eq!(name.value, "alice");
}

#[turso_macros::test(mvcc)]
fn test_postgres_alter_table_rename_column(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, old_name TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t (id, old_name) VALUES (1, 'hello')")
        .unwrap();

    conn.execute("ALTER TABLE t RENAME COLUMN old_name TO new_name")
        .unwrap();

    // Old column name should fail
    let result = conn.query("SELECT old_name FROM t");
    assert!(result.is_err());

    // New column name should work
    let mut rows = conn.query("SELECT new_name FROM t").unwrap().unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    let Value::Text(val) = row.get_value(0) else {
        panic!("expected text");
    };
    assert_eq!(val.value, "hello");
}

// ==================== CREATE INDEX / DROP INDEX ====================

#[turso_macros::test(mvcc)]
fn test_postgres_create_index(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
        .unwrap();

    // Simple index
    conn.execute("CREATE INDEX idx_name ON t (name)").unwrap();

    // Verify data still works with index
    conn.execute("INSERT INTO t (id, name, age) VALUES (1, 'alice', 30)")
        .unwrap();
    let mut rows = conn
        .query("SELECT name FROM t WHERE name = 'alice'")
        .unwrap()
        .unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
}

#[turso_macros::test(mvcc)]
fn test_postgres_create_unique_index(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT)")
        .unwrap();

    conn.execute("CREATE UNIQUE INDEX idx_email ON t (email)")
        .unwrap();

    conn.execute("INSERT INTO t (id, email) VALUES (1, 'a@b.com')")
        .unwrap();

    // Duplicate should fail
    let result = conn.execute("INSERT INTO t (id, email) VALUES (2, 'a@b.com')");
    assert!(result.is_err());
}

#[turso_macros::test(mvcc)]
fn test_postgres_create_index_multi_column(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b TEXT)")
        .unwrap();

    conn.execute("CREATE INDEX idx_ab ON t (a, b)").unwrap();

    conn.execute("INSERT INTO t (id, a, b) VALUES (1, 'x', 'y')")
        .unwrap();
    let mut rows = conn
        .query("SELECT a, b FROM t WHERE a = 'x' AND b = 'y'")
        .unwrap()
        .unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
}

#[turso_macros::test(mvcc)]
fn test_postgres_create_index_partial(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, status TEXT, val INTEGER)")
        .unwrap();

    conn.execute("CREATE INDEX idx_active ON t (val) WHERE status = 'active'")
        .unwrap();

    conn.execute("INSERT INTO t (id, status, val) VALUES (1, 'active', 100)")
        .unwrap();
    conn.execute("INSERT INTO t (id, status, val) VALUES (2, 'inactive', 200)")
        .unwrap();

    let mut rows = conn
        .query("SELECT val FROM t WHERE status = 'active'")
        .unwrap()
        .unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
}

#[turso_macros::test(mvcc)]
fn test_postgres_drop_index(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX idx_name ON t (name)").unwrap();

    conn.execute("DROP INDEX idx_name").unwrap();

    // Creating same index again should work since it was dropped
    conn.execute("CREATE INDEX idx_name ON t (name)").unwrap();
}

#[turso_macros::test(mvcc)]
fn test_postgres_drop_index_if_exists(db: TempDatabase) {
    let conn = db.connect_postgres();

    // Should not error even though index doesn't exist
    conn.execute("DROP INDEX IF EXISTS nonexistent").unwrap();
}

// ==================== TYPE CASTING (::type) ====================

#[turso_macros::test(mvcc)]
fn test_postgres_cast_int_to_text(db: TempDatabase) {
    let conn = db.connect_postgres();

    let mut rows = conn.query("SELECT 42::text").unwrap().unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    let Value::Text(val) = row.get_value(0) else {
        panic!("expected text, got {:?}", row.get_value(0));
    };
    assert_eq!(val.value, "42");
}

#[turso_macros::test(mvcc)]
fn test_postgres_cast_text_to_int(db: TempDatabase) {
    let conn = db.connect_postgres();

    let mut rows = conn.query("SELECT '123'::integer").unwrap().unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    let Value::Numeric(Numeric::Integer(val)) = row.get_value(0) else {
        panic!("expected integer, got {:?}", row.get_value(0));
    };
    assert_eq!(*val, 123);
}

#[turso_macros::test(mvcc)]
fn test_postgres_cast_float_to_text(db: TempDatabase) {
    let conn = db.connect_postgres();

    let mut rows = conn.query("SELECT 3.14::text").unwrap().unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    let Value::Text(val) = row.get_value(0) else {
        panic!("expected text, got {:?}", row.get_value(0));
    };
    assert_eq!(val.value, "3.14");
}

#[turso_macros::test(mvcc)]
fn test_postgres_cast_in_expression(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t (id, val) VALUES (1, '42')")
        .unwrap();

    // Cast column value in WHERE clause
    let mut rows = conn
        .query("SELECT id FROM t WHERE val::integer > 10")
        .unwrap()
        .unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    let Value::Numeric(Numeric::Integer(val)) = row.get_value(0) else {
        panic!("expected integer");
    };
    assert_eq!(*val, 1);
}

#[turso_macros::test(mvcc)]
fn test_postgres_cast_boolean(db: TempDatabase) {
    let conn = db.connect_postgres();

    // Boolean cast should produce integer (0 or 1)
    let mut rows = conn.query("SELECT 1::boolean").unwrap().unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    let Value::Numeric(Numeric::Integer(val)) = row.get_value(0) else {
        panic!("expected integer, got {:?}", row.get_value(0));
    };
    assert_eq!(*val, 1);
}

// ==================== FOREIGN KEYS ====================

#[turso_macros::test(mvcc)]
fn test_postgres_foreign_key_column_level(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.pragma_update("foreign_keys", "ON").unwrap();

    conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute(
        "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id))",
    )
    .unwrap();

    // Valid insert
    conn.execute("INSERT INTO parent (id, name) VALUES (1, 'p1')")
        .unwrap();
    conn.execute("INSERT INTO child (id, parent_id) VALUES (1, 1)")
        .unwrap();

    // Invalid FK reference should fail
    let result = conn.execute("INSERT INTO child (id, parent_id) VALUES (2, 999)");
    assert!(result.is_err(), "FK violation should fail");
}

#[turso_macros::test(mvcc)]
fn test_postgres_foreign_key_table_level(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.pragma_update("foreign_keys", "ON").unwrap();

    conn.execute("CREATE TABLE parent (a INTEGER, b INTEGER, PRIMARY KEY (a, b))")
        .unwrap();
    conn.execute(
        "CREATE TABLE child (id INTEGER PRIMARY KEY, pa INTEGER, pb INTEGER, FOREIGN KEY (pa, pb) REFERENCES parent(a, b))",
    )
    .unwrap();

    conn.execute("INSERT INTO parent (a, b) VALUES (1, 2)")
        .unwrap();
    conn.execute("INSERT INTO child (id, pa, pb) VALUES (1, 1, 2)")
        .unwrap();

    // Invalid composite FK
    let result = conn.execute("INSERT INTO child (id, pa, pb) VALUES (2, 1, 99)");
    assert!(result.is_err(), "composite FK violation should fail");
}

#[turso_macros::test(mvcc)]
fn test_postgres_foreign_key_cascade(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.pragma_update("foreign_keys", "ON").unwrap();

    conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON DELETE CASCADE)")
        .unwrap();

    conn.execute("INSERT INTO parent (id) VALUES (1)").unwrap();
    conn.execute("INSERT INTO child (id, pid) VALUES (1, 1)")
        .unwrap();

    // Delete parent should cascade to child
    conn.execute("DELETE FROM parent WHERE id = 1").unwrap();

    let mut rows = conn.query("SELECT COUNT(*) FROM child").unwrap().unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    let Value::Numeric(Numeric::Integer(count)) = row.get_value(0) else {
        panic!("expected integer");
    };
    assert_eq!(*count, 0, "child should be deleted by cascade");
}

// ==================== CHECK CONSTRAINTS ====================

#[turso_macros::test(mvcc)]
fn test_postgres_check_constraint(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, age INTEGER, CHECK (age >= 0))")
        .unwrap();

    // Valid insert
    conn.execute("INSERT INTO t (id, age) VALUES (1, 25)")
        .unwrap();

    // Violating check should fail
    let result = conn.execute("INSERT INTO t (id, age) VALUES (2, -1)");
    assert!(result.is_err(), "CHECK violation should fail");
}

// ==================== WINDOW FUNCTIONS ====================

#[turso_macros::test(mvcc)]
fn test_postgres_window_count_over(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, dept TEXT, salary INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'eng', 100)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'eng', 200)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (3, 'sales', 150)")
        .unwrap();

    // COUNT(*) OVER (PARTITION BY dept) - count per department
    let mut rows = conn
        .query("SELECT dept, COUNT(*) OVER (PARTITION BY dept) FROM t ORDER BY id")
        .unwrap()
        .unwrap();

    // eng has 2 rows
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    let Value::Numeric(Numeric::Integer(cnt)) = row.get_value(1) else {
        panic!("expected integer, got {:?}", row.get_value(1));
    };
    assert_eq!(*cnt, 2);

    // eng again
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row 2");
    };
    let row = rows.row().unwrap();
    let Value::Numeric(Numeric::Integer(cnt)) = row.get_value(1) else {
        panic!("expected integer");
    };
    assert_eq!(*cnt, 2);

    // sales has 1 row
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row 3");
    };
    let row = rows.row().unwrap();
    let Value::Numeric(Numeric::Integer(cnt)) = row.get_value(1) else {
        panic!("expected integer");
    };
    assert_eq!(*cnt, 1);
}

#[turso_macros::test(mvcc)]
fn test_postgres_window_sum_partition(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, dept TEXT, salary INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'eng', 100)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'eng', 200)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (3, 'sales', 150)")
        .unwrap();

    let mut rows = conn
        .query("SELECT dept, salary, SUM(salary) OVER (PARTITION BY dept) FROM t ORDER BY id")
        .unwrap()
        .unwrap();

    // eng dept total = 300
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    let Value::Numeric(Numeric::Integer(sum)) = row.get_value(2) else {
        panic!("expected integer, got {:?}", row.get_value(2));
    };
    assert_eq!(*sum, 300);
}

#[turso_macros::test(mvcc)]
fn test_postgres_window_min_max_over(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, dept TEXT, salary INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'eng', 100)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'eng', 200)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (3, 'sales', 150)")
        .unwrap();

    // MIN and MAX per partition
    let mut rows = conn
        .query("SELECT dept, salary, MIN(salary) OVER (PARTITION BY dept), MAX(salary) OVER (PARTITION BY dept) FROM t ORDER BY id")
        .unwrap()
        .unwrap();

    // eng: min=100, max=200
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    let Value::Numeric(Numeric::Integer(min_val)) = row.get_value(2) else {
        panic!("expected integer, got {:?}", row.get_value(2));
    };
    assert_eq!(*min_val, 100);
    let Value::Numeric(Numeric::Integer(max_val)) = row.get_value(3) else {
        panic!("expected integer");
    };
    assert_eq!(*max_val, 200);

    // eng again: min=100, max=200
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row 2");
    };
    let row = rows.row().unwrap();
    let Value::Numeric(Numeric::Integer(min_val)) = row.get_value(2) else {
        panic!("expected integer");
    };
    assert_eq!(*min_val, 100);

    // sales: min=150, max=150
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row 3");
    };
    let row = rows.row().unwrap();
    let Value::Numeric(Numeric::Integer(min_val)) = row.get_value(2) else {
        panic!("expected integer");
    };
    assert_eq!(*min_val, 150);
    let Value::Numeric(Numeric::Integer(max_val)) = row.get_value(3) else {
        panic!("expected integer");
    };
    assert_eq!(*max_val, 150);
}

#[turso_macros::test(mvcc)]
fn test_postgres_window_sum_order_by(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 20)").unwrap();
    conn.execute("INSERT INTO t VALUES (3, 30)").unwrap();

    // Running total: SUM(val) OVER (ORDER BY id)
    let mut rows = conn
        .query("SELECT val, SUM(val) OVER (ORDER BY id) FROM t ORDER BY id")
        .unwrap()
        .unwrap();

    // id=1: 10
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    let Value::Numeric(Numeric::Integer(sum)) = row.get_value(1) else {
        panic!("expected integer, got {:?}", row.get_value(1));
    };
    assert_eq!(*sum, 10);

    // id=2: 10+20=30
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row 2");
    };
    let row = rows.row().unwrap();
    let Value::Numeric(Numeric::Integer(sum)) = row.get_value(1) else {
        panic!("expected integer");
    };
    assert_eq!(*sum, 30);

    // id=3: 10+20+30=60
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row 3");
    };
    let row = rows.row().unwrap();
    let Value::Numeric(Numeric::Integer(sum)) = row.get_value(1) else {
        panic!("expected integer");
    };
    assert_eq!(*sum, 60);
}

#[turso_macros::test(mvcc)]
fn test_postgres_window_empty_over(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 20)").unwrap();
    conn.execute("INSERT INTO t VALUES (3, 30)").unwrap();

    // SUM(val) OVER () - total across all rows
    let mut rows = conn
        .query("SELECT val, SUM(val) OVER () FROM t ORDER BY id")
        .unwrap()
        .unwrap();

    // Every row should see total = 60
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    let Value::Numeric(Numeric::Integer(sum)) = row.get_value(1) else {
        panic!("expected integer, got {:?}", row.get_value(1));
    };
    assert_eq!(*sum, 60);

    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row 2");
    };
    let row = rows.row().unwrap();
    let Value::Numeric(Numeric::Integer(sum)) = row.get_value(1) else {
        panic!("expected integer");
    };
    assert_eq!(*sum, 60);
}

#[turso_macros::test(mvcc)]
fn test_postgres_window_avg_partition(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, dept TEXT, salary INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'eng', 100)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'eng', 200)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (3, 'sales', 150)")
        .unwrap();

    // AVG per partition
    let mut rows = conn
        .query("SELECT dept, AVG(salary) OVER (PARTITION BY dept) FROM t ORDER BY id")
        .unwrap()
        .unwrap();

    // eng: avg = 150.0
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    match row.get_value(1) {
        Value::Numeric(Numeric::Float(avg)) => assert_eq!(*avg, 150.0),
        Value::Numeric(Numeric::Integer(avg)) => assert_eq!(*avg, 150),
        other => panic!("expected numeric, got {other:?}"),
    }
}

// ==================== OPERATORS: ||, ILIKE, JSON ====================

#[turso_macros::test(mvcc)]
fn test_postgres_concat_operator(db: TempDatabase) {
    let conn = db.connect_postgres();

    let mut rows = conn
        .query("SELECT 'hello' || ' ' || 'world'")
        .unwrap()
        .unwrap();

    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    let Value::Text(val) = row.get_value(0) else {
        panic!("expected text, got {:?}", row.get_value(0));
    };
    assert_eq!(val.as_str(), "hello world");
}

#[turso_macros::test(mvcc)]
fn test_postgres_concat_with_column(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, first_name TEXT, last_name TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'Alice', 'Smith')")
        .unwrap();

    let mut rows = conn
        .query("SELECT first_name || ' ' || last_name FROM t")
        .unwrap()
        .unwrap();

    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    let Value::Text(val) = row.get_value(0) else {
        panic!("expected text, got {:?}", row.get_value(0));
    };
    assert_eq!(val.as_str(), "Alice Smith");
}

#[turso_macros::test(mvcc)]
fn test_postgres_modulus_operator(db: TempDatabase) {
    let conn = db.connect_postgres();

    let mut rows = conn.query("SELECT 10 % 3").unwrap().unwrap();

    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    let Value::Numeric(Numeric::Integer(val)) = row.get_value(0) else {
        panic!("expected integer, got {:?}", row.get_value(0));
    };
    assert_eq!(*val, 1);
}

#[turso_macros::test(mvcc)]
fn test_postgres_ilike(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'Alice')").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'Bob')").unwrap();
    conn.execute("INSERT INTO t VALUES (3, 'ALICE')").unwrap();

    // ILIKE is case-insensitive
    let mut rows = conn
        .query("SELECT name FROM t WHERE name ILIKE '%alice%' ORDER BY id")
        .unwrap()
        .unwrap();

    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    let Value::Text(val) = row.get_value(0) else {
        panic!("expected text");
    };
    assert_eq!(val.as_str(), "Alice");

    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row 2");
    };
    let row = rows.row().unwrap();
    let Value::Text(val) = row.get_value(0) else {
        panic!("expected text");
    };
    assert_eq!(val.as_str(), "ALICE");

    // No more rows
    let StepResult::Done = rows.step().unwrap() else {
        panic!("expected done");
    };
}

#[turso_macros::test(mvcc)]
fn test_postgres_not_ilike(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'Alice')").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'Bob')").unwrap();
    conn.execute("INSERT INTO t VALUES (3, 'ALICE')").unwrap();

    // NOT ILIKE should exclude case-insensitive matches
    let mut rows = conn
        .query("SELECT name FROM t WHERE name NOT ILIKE '%alice%' ORDER BY id")
        .unwrap()
        .unwrap();

    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    let Value::Text(val) = row.get_value(0) else {
        panic!("expected text");
    };
    assert_eq!(val.as_str(), "Bob");

    let StepResult::Done = rows.step().unwrap() else {
        panic!("expected done");
    };
}

#[turso_macros::test(mvcc)]
fn test_postgres_ilike_pattern(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'user@EXAMPLE.com')")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'admin@test.org')")
        .unwrap();

    let mut rows = conn
        .query("SELECT email FROM t WHERE email ILIKE '%example%'")
        .unwrap()
        .unwrap();

    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    let Value::Text(val) = row.get_value(0) else {
        panic!("expected text");
    };
    assert_eq!(val.as_str(), "user@EXAMPLE.com");

    let StepResult::Done = rows.step().unwrap() else {
        panic!("expected done");
    };
}

// =====================================================================
// Network types (cidr, macaddr, macaddr8)
// =====================================================================

#[turso_macros::test(mvcc)]
fn test_postgres_network_types(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute(
        "CREATE TABLE network_table (
            inet inet NOT NULL,
            cidr cidr NOT NULL,
            macaddr macaddr NOT NULL,
            macaddr8 macaddr8 NOT NULL
        )",
    )
    .unwrap();

    conn.execute(
        "INSERT INTO network_table (inet, cidr, macaddr, macaddr8) VALUES ('127.0.0.1', '192.168.100.128/25', '08:00:2b:01:02:03', '08:00:2b:01:02:03:04:05')",
    )
    .unwrap();

    let mut rows = conn
        .query("SELECT inet, cidr, macaddr, macaddr8 FROM network_table")
        .unwrap()
        .unwrap();

    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();

    let Value::Text(inet) = row.get_value(0) else {
        panic!("expected text for inet");
    };
    assert_eq!(inet.as_str(), "127.0.0.1");

    let Value::Text(cidr) = row.get_value(1) else {
        panic!("expected text for cidr");
    };
    assert_eq!(cidr.as_str(), "192.168.100.128/25");

    let Value::Text(macaddr) = row.get_value(2) else {
        panic!("expected text for macaddr");
    };
    assert_eq!(macaddr.as_str(), "08:00:2b:01:02:03");

    let Value::Text(macaddr8) = row.get_value(3) else {
        panic!("expected text for macaddr8");
    };
    assert_eq!(macaddr8.as_str(), "08:00:2b:01:02:03:04:05");
}

// =====================================================================
// Array types
// =====================================================================

#[turso_macros::test(mvcc)]
fn test_postgres_array_integer(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute(
        "CREATE TABLE sal_emp (
            name text NOT NULL,
            pay_by_quarter integer[] NOT NULL
        )",
    )
    .unwrap();

    conn.execute(
        "INSERT INTO sal_emp (name, pay_by_quarter) VALUES ('John', '{10000,10000,10000,10000}')",
    )
    .unwrap();

    let mut rows = conn
        .query("SELECT name, pay_by_quarter FROM sal_emp")
        .unwrap()
        .unwrap();

    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();

    let Value::Text(name) = row.get_value(0) else {
        panic!("expected text for name");
    };
    assert_eq!(name.as_str(), "John");

    let Value::Text(pay) = row.get_value(1) else {
        panic!("expected text for pay_by_quarter");
    };
    assert_eq!(pay.as_str(), "{10000,10000,10000,10000}");
}

#[turso_macros::test(mvcc)]
fn test_postgres_array_text(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute(
        "CREATE TABLE arr_test (
            id integer PRIMARY KEY,
            tags text[] NOT NULL
        )",
    )
    .unwrap();

    conn.execute("INSERT INTO arr_test (id, tags) VALUES (1, '{\"hello\",\"world\"}')")
        .unwrap();

    let mut rows = conn
        .query("SELECT tags FROM arr_test WHERE id = 1")
        .unwrap()
        .unwrap();

    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    let Value::Text(tags) = row.get_value(0) else {
        panic!("expected text for tags");
    };
    assert_eq!(tags.as_str(), "{hello,world}");
}

#[turso_macros::test(mvcc)]
fn test_postgres_array_nested(db: TempDatabase) {
    let conn = db.connect_postgres();

    // Native arrays are 1-dimensional; verify that multi-element text arrays
    // with quoted strings round-trip correctly.
    conn.execute(
        "CREATE TABLE nested_arr (
            id integer PRIMARY KEY,
            schedule text[] NOT NULL
        )",
    )
    .unwrap();

    conn.execute(
        "INSERT INTO nested_arr (id, schedule) VALUES (1, '{\"meeting\",\"lunch\",\"training\",\"presentation\"}')",
    )
    .unwrap();

    let mut rows = conn
        .query("SELECT schedule FROM nested_arr WHERE id = 1")
        .unwrap()
        .unwrap();

    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    let Value::Text(schedule) = row.get_value(0) else {
        panic!("expected text for schedule");
    };
    assert_eq!(schedule.as_str(), "{meeting,lunch,training,presentation}");
}

// =====================================================================
// Array operators (@>, <@, &&)
// =====================================================================

#[turso_macros::test(mvcc)]
fn test_postgres_array_contains(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute(
        "CREATE TABLE posts (
            id integer PRIMARY KEY,
            tags text[]
        )",
    )
    .unwrap();

    conn.execute("INSERT INTO posts (id, tags) VALUES (1, '{\"ORM\"}')")
        .unwrap();
    conn.execute("INSERT INTO posts (id, tags) VALUES (2, '{\"Typescript\"}')")
        .unwrap();
    conn.execute("INSERT INTO posts (id, tags) VALUES (3, '{\"Typescript\",\"ORM\"}')")
        .unwrap();
    conn.execute(
        "INSERT INTO posts (id, tags) VALUES (4, '{\"Typescript\",\"Frontend\",\"React\"}')",
    )
    .unwrap();
    conn.execute(
        "INSERT INTO posts (id, tags) VALUES (5, '{\"Typescript\",\"ORM\",\"Database\",\"Postgres\"}')",
    )
    .unwrap();
    conn.execute("INSERT INTO posts (id, tags) VALUES (6, '{\"Java\",\"Spring\",\"OOP\"}')")
        .unwrap();

    // @> : left contains all elements of right
    let mut rows = conn
        .query("SELECT id FROM posts WHERE tags @> '{\"Typescript\",\"ORM\"}'")
        .unwrap()
        .unwrap();
    let mut ids = Vec::new();
    loop {
        match rows.step().unwrap() {
            StepResult::Row => {
                let row = rows.row().unwrap();
                let Value::Numeric(Numeric::Integer(id)) = row.get_value(0) else {
                    panic!("expected integer");
                };
                ids.push(*id);
            }
            StepResult::Done => break,
            _ => panic!("unexpected step result"),
        }
    }
    assert_eq!(ids, vec![3, 5]);
    drop(rows);

    // <@ : left elements are all in right
    let mut rows = conn
        .query("SELECT id FROM posts WHERE tags <@ '{\"Typescript\",\"ORM\"}'")
        .unwrap()
        .unwrap();
    let mut ids = Vec::new();
    loop {
        match rows.step().unwrap() {
            StepResult::Row => {
                let row = rows.row().unwrap();
                let Value::Numeric(Numeric::Integer(id)) = row.get_value(0) else {
                    panic!("expected integer");
                };
                ids.push(*id);
            }
            StepResult::Done => break,
            _ => panic!("unexpected step result"),
        }
    }
    assert_eq!(ids, vec![1, 2, 3]);
    drop(rows);

    // && : arrays share any element
    let mut rows = conn
        .query("SELECT id FROM posts WHERE tags && '{\"Typescript\",\"ORM\"}'")
        .unwrap()
        .unwrap();
    let mut ids = Vec::new();
    loop {
        match rows.step().unwrap() {
            StepResult::Row => {
                let row = rows.row().unwrap();
                let Value::Numeric(Numeric::Integer(id)) = row.get_value(0) else {
                    panic!("expected integer");
                };
                ids.push(*id);
            }
            StepResult::Done => break,
            _ => panic!("unexpected step result"),
        }
    }
    assert_eq!(ids, vec![1, 2, 3, 4, 5]);
}

// =====================================================================
// Native array type validation
// =====================================================================

#[turso_macros::test(mvcc)]
fn test_postgres_array_type_validation(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE typed_arr (id integer PRIMARY KEY, nums integer[] NOT NULL)")
        .unwrap();

    // Valid integer array should succeed
    conn.execute("INSERT INTO typed_arr (id, nums) VALUES (1, '{1,2,3}')")
        .unwrap();

    // Invalid: text element in integer array should fail
    let result = conn.execute("INSERT INTO typed_arr (id, nums) VALUES (2, '{1, hello, 3}')");
    assert!(
        result.is_err(),
        "expected type validation error for non-integer element"
    );
}

#[turso_macros::test(mvcc)]
fn test_postgres_array_length(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE len_arr (id integer PRIMARY KEY, vals text[])")
        .unwrap();
    conn.execute("INSERT INTO len_arr (id, vals) VALUES (1, '{\"a\",\"b\",\"c\"}')")
        .unwrap();

    let mut rows = conn
        .query("SELECT array_length(vals, 1) FROM len_arr WHERE id = 1")
        .unwrap()
        .unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    let Value::Numeric(Numeric::Integer(v)) = row.get_value(0) else {
        panic!("expected integer, got {:?}", row.get_value(0));
    };
    assert_eq!(*v, 3);
}

#[turso_macros::test(mvcc)]
fn test_postgres_array_append(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE append_arr (id integer PRIMARY KEY, vals integer[])")
        .unwrap();
    conn.execute("INSERT INTO append_arr (id, vals) VALUES (1, '{1,2,3}')")
        .unwrap();

    let mut rows = conn
        .query("SELECT array_append(vals, 4) FROM append_arr WHERE id = 1")
        .unwrap()
        .unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    let Value::Text(v) = row.get_value(0) else {
        panic!("expected text, got {:?}", row.get_value(0));
    };
    assert_eq!(v.as_str(), "{1,2,3,4}");
}

#[turso_macros::test(mvcc)]
fn test_postgres_enum_create_and_use(db: TempDatabase) {
    let conn = db.connect_postgres();

    // Create an enum type
    conn.execute("CREATE TYPE mood AS ENUM ('happy', 'sad', 'neutral')")
        .unwrap();

    // Create a table using the enum
    conn.execute("CREATE TABLE people (name TEXT, current_mood mood)")
        .unwrap();

    // Insert valid enum values
    conn.execute("INSERT INTO people VALUES ('Alice', 'happy')")
        .unwrap();
    conn.execute("INSERT INTO people VALUES ('Bob', 'sad')")
        .unwrap();

    // Select and verify
    let mut rows = conn
        .query("SELECT name, current_mood FROM people ORDER BY name")
        .unwrap()
        .unwrap();

    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    let Value::Text(name) = row.get_value(0) else {
        panic!("expected text");
    };
    let Value::Text(mood) = row.get_value(1) else {
        panic!("expected text, got {:?}", row.get_value(1));
    };
    assert_eq!(name.as_str(), "Alice");
    assert_eq!(mood.as_str(), "happy");

    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    let Value::Text(name) = row.get_value(0) else {
        panic!("expected text");
    };
    let Value::Text(mood) = row.get_value(1) else {
        panic!("expected text, got {:?}", row.get_value(1));
    };
    assert_eq!(name.as_str(), "Bob");
    assert_eq!(mood.as_str(), "sad");
}

#[turso_macros::test(mvcc)]
fn test_postgres_enum_validation(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TYPE color AS ENUM ('red', 'green', 'blue')")
        .unwrap();
    conn.execute("CREATE TABLE items (name TEXT, item_color color)")
        .unwrap();

    // Valid value should work
    conn.execute("INSERT INTO items VALUES ('apple', 'red')")
        .unwrap();

    // Invalid value should fail
    let result = conn.execute("INSERT INTO items VALUES ('banana', 'yellow')");
    assert!(result.is_err(), "inserting invalid enum value should fail");
}

#[turso_macros::test(mvcc)]
fn test_postgres_enum_if_not_exists(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TYPE status AS ENUM ('active', 'inactive')")
        .unwrap();

    // Creating same type again should fail without IF NOT EXISTS
    // (PG syntax doesn't have IF NOT EXISTS for types, but test the type exists check)
    let result = conn.execute("CREATE TYPE status AS ENUM ('active', 'inactive')");
    assert!(result.is_err(), "duplicate type creation should fail");
}

#[turso_macros::test(mvcc)]
fn test_postgres_enum_null(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TYPE mood AS ENUM ('happy', 'sad')")
        .unwrap();
    conn.execute("CREATE TABLE people (name TEXT, m mood)")
        .unwrap();

    // NULL should be accepted in an enum column
    conn.execute("INSERT INTO people VALUES ('Alice', 'happy')")
        .unwrap();
    conn.execute("INSERT INTO people VALUES ('Nobody', NULL)")
        .unwrap();

    // Verify NULL round-trips
    let mut rows = conn
        .query("SELECT name, m FROM people ORDER BY name")
        .unwrap()
        .unwrap();

    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    assert_eq!(row.get_value(0).to_string(), "Alice");
    assert_eq!(row.get_value(1).to_string(), "happy");

    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    assert_eq!(row.get_value(0).to_string(), "Nobody");
    assert!(matches!(row.get_value(1), Value::Null));
}

#[turso_macros::test(mvcc)]
fn test_postgres_enum_case_sensitive(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TYPE priority AS ENUM ('Low', 'Medium', 'High')")
        .unwrap();
    conn.execute("CREATE TABLE tasks (name TEXT, p priority)")
        .unwrap();

    // Exact case should work
    conn.execute("INSERT INTO tasks VALUES ('t1', 'Low')")
        .unwrap();

    // Wrong case should fail (PG enums are case-sensitive)
    let result = conn.execute("INSERT INTO tasks VALUES ('t2', 'low')");
    assert!(
        result.is_err(),
        "enum values should be case-sensitive: 'low' != 'Low'"
    );
}

#[turso_macros::test(mvcc)]
fn test_postgres_enum_where_clause(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TYPE color AS ENUM ('red', 'green', 'blue')")
        .unwrap();
    conn.execute("CREATE TABLE items (name TEXT, c color)")
        .unwrap();
    conn.execute("INSERT INTO items VALUES ('apple', 'red')")
        .unwrap();
    conn.execute("INSERT INTO items VALUES ('sky', 'blue')")
        .unwrap();
    conn.execute("INSERT INTO items VALUES ('leaf', 'green')")
        .unwrap();

    // Filter by enum value
    let mut rows = conn
        .query("SELECT name FROM items WHERE c = 'blue'")
        .unwrap()
        .unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    assert_eq!(row.get_value(0).to_string(), "sky");
    assert!(matches!(rows.step().unwrap(), StepResult::Done));
}

#[turso_macros::test(mvcc)]
fn test_postgres_enum_update(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TYPE status AS ENUM ('pending', 'done')")
        .unwrap();
    conn.execute("CREATE TABLE jobs (id INTEGER PRIMARY KEY, s status)")
        .unwrap();
    conn.execute("INSERT INTO jobs VALUES (1, 'pending')")
        .unwrap();

    // Update to valid value
    conn.execute("UPDATE jobs SET s = 'done' WHERE id = 1")
        .unwrap();
    let mut rows = conn
        .query("SELECT s FROM jobs WHERE id = 1")
        .unwrap()
        .unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    assert_eq!(rows.row().unwrap().get_value(0).to_string(), "done");
    drop(rows);

    // Update to invalid value should fail
    let result = conn.execute("UPDATE jobs SET s = 'cancelled' WHERE id = 1");
    assert!(
        result.is_err(),
        "update with invalid enum value should fail"
    );
}

#[turso_macros::test(mvcc)]
fn test_postgres_enum_group_by(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TYPE dept AS ENUM ('eng', 'sales', 'hr')")
        .unwrap();
    conn.execute("CREATE TABLE employees (name TEXT, d dept)")
        .unwrap();
    conn.execute("INSERT INTO employees VALUES ('Alice', 'eng')")
        .unwrap();
    conn.execute("INSERT INTO employees VALUES ('Bob', 'eng')")
        .unwrap();
    conn.execute("INSERT INTO employees VALUES ('Carol', 'sales')")
        .unwrap();

    let mut rows = conn
        .query("SELECT d, COUNT(*) FROM employees GROUP BY d ORDER BY d")
        .unwrap()
        .unwrap();

    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    assert_eq!(row.get_value(0).to_string(), "eng");
    assert_eq!(row.get_value(1).to_string(), "2");

    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    assert_eq!(row.get_value(0).to_string(), "sales");
    assert_eq!(row.get_value(1).to_string(), "1");
}

#[turso_macros::test(mvcc)]
fn test_postgres_enum_distinct(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TYPE tier AS ENUM ('free', 'pro', 'enterprise')")
        .unwrap();
    conn.execute("CREATE TABLE accounts (name TEXT, t tier)")
        .unwrap();
    conn.execute("INSERT INTO accounts VALUES ('a', 'free')")
        .unwrap();
    conn.execute("INSERT INTO accounts VALUES ('b', 'free')")
        .unwrap();
    conn.execute("INSERT INTO accounts VALUES ('c', 'pro')")
        .unwrap();

    let mut rows = conn
        .query("SELECT DISTINCT t FROM accounts ORDER BY t")
        .unwrap()
        .unwrap();

    let mut values = Vec::new();
    while let StepResult::Row = rows.step().unwrap() {
        values.push(rows.row().unwrap().get_value(0).to_string());
    }
    assert_eq!(values, vec!["free", "pro"]);
}

#[turso_macros::test]
fn test_postgres_enum_multiple_types_one_table(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TYPE color AS ENUM ('red', 'blue')")
        .unwrap();
    conn.execute("CREATE TYPE size AS ENUM ('S', 'M', 'L')")
        .unwrap();
    conn.execute("CREATE TABLE products (name TEXT, c color, s size)")
        .unwrap();

    conn.execute("INSERT INTO products VALUES ('shirt', 'red', 'M')")
        .unwrap();

    // Each column validates independently
    let result = conn.execute("INSERT INTO products VALUES ('hat', 'yellow', 'S')");
    assert!(result.is_err(), "invalid color should fail");

    let result = conn.execute("INSERT INTO products VALUES ('hat', 'blue', 'XL')");
    assert!(result.is_err(), "invalid size should fail");
}

#[turso_macros::test(mvcc)]
fn test_postgres_enum_single_label(db: TempDatabase) {
    let conn = db.connect_postgres();

    // Enum with only one label
    conn.execute("CREATE TYPE singleton AS ENUM ('only')")
        .unwrap();
    conn.execute("CREATE TABLE single (v singleton)").unwrap();

    conn.execute("INSERT INTO single VALUES ('only')").unwrap();

    let result = conn.execute("INSERT INTO single VALUES ('other')");
    assert!(result.is_err(), "non-matching value should fail");
}

#[turso_macros::test(mvcc)]
fn test_postgres_enum_shared_across_tables(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TYPE direction AS ENUM ('north', 'south', 'east', 'west')")
        .unwrap();

    // Same enum used in two tables
    conn.execute("CREATE TABLE routes (id INTEGER PRIMARY KEY, heading direction)")
        .unwrap();
    conn.execute("CREATE TABLE winds (id INTEGER PRIMARY KEY, dir direction)")
        .unwrap();

    conn.execute("INSERT INTO routes VALUES (1, 'north')")
        .unwrap();
    conn.execute("INSERT INTO winds VALUES (1, 'south')")
        .unwrap();

    // Both validate against same type
    let result = conn.execute("INSERT INTO routes VALUES (2, 'up')");
    assert!(result.is_err());
    let result = conn.execute("INSERT INTO winds VALUES (2, 'down')");
    assert!(result.is_err());
}

#[turso_macros::test(mvcc)]
fn test_postgres_enum_label_with_spaces(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TYPE event AS ENUM ('sign up', 'log in', 'log out')")
        .unwrap();
    conn.execute("CREATE TABLE logs (e event)").unwrap();

    conn.execute("INSERT INTO logs VALUES ('sign up')").unwrap();

    let mut rows = conn.query("SELECT e FROM logs").unwrap().unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    assert_eq!(rows.row().unwrap().get_value(0).to_string(), "sign up");
}

#[turso_macros::test(mvcc)]
fn test_postgres_drop_type(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TYPE mood AS ENUM ('happy', 'sad')")
        .unwrap();
    conn.execute("CREATE TABLE people (name TEXT, m mood)")
        .unwrap();
    conn.execute("INSERT INTO people VALUES ('Alice', 'happy')")
        .unwrap();

    // Drop the table first (type is in use)
    conn.execute("DROP TABLE people").unwrap();
    conn.execute("DROP TYPE mood").unwrap();

    // Type should no longer exist — creating a table with it should fail
    let result = conn.execute("CREATE TABLE people2 (name TEXT, m mood)");
    assert!(result.is_err(), "expected error using dropped type");
}

#[turso_macros::test(mvcc)]
fn test_postgres_drop_type_if_exists(db: TempDatabase) {
    let conn = db.connect_postgres();

    // Should not error on nonexistent type
    conn.execute("DROP TYPE IF EXISTS nonexistent").unwrap();

    // Create then drop
    conn.execute("CREATE TYPE color AS ENUM ('red', 'blue')")
        .unwrap();
    conn.execute("DROP TYPE IF EXISTS color").unwrap();
}

#[turso_macros::test]
fn test_postgres_similar_to(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE words (w TEXT)").unwrap();
    conn.execute("INSERT INTO words VALUES ('hello')").unwrap();
    conn.execute("INSERT INTO words VALUES ('help')").unwrap();
    conn.execute("INSERT INTO words VALUES ('world')").unwrap();
    conn.execute("INSERT INTO words VALUES ('helm')").unwrap();

    // SIMILAR TO with % wildcard
    let mut stmt = conn
        .prepare("SELECT w FROM words WHERE w SIMILAR TO 'hel%' ORDER BY w")
        .unwrap();
    let mut results: Vec<String> = Vec::new();
    loop {
        match stmt.step().unwrap() {
            turso_core::StepResult::Row => {
                let row = stmt.row().unwrap();
                results.push(row.get_value(0).to_string());
            }
            turso_core::StepResult::Done => break,
            _ => {}
        }
    }
    assert_eq!(results, vec!["hello", "helm", "help"]);
}

#[turso_macros::test(mvcc)]
fn test_postgres_generate_series(db: TempDatabase) {
    let conn = db.connect_postgres();

    // Basic generate_series(start, stop)
    let mut stmt = conn
        .prepare("SELECT value FROM generate_series(1, 5)")
        .unwrap();
    let mut results: Vec<i64> = Vec::new();
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                if let Value::Numeric(Numeric::Integer(v)) = row.get_value(0) {
                    results.push(*v);
                }
            }
            StepResult::Done => break,
            _ => {}
        }
    }
    assert_eq!(results, vec![1, 2, 3, 4, 5]);
    drop(stmt);

    // generate_series with step
    let mut stmt = conn
        .prepare("SELECT value FROM generate_series(0, 10, 3)")
        .unwrap();
    let mut results: Vec<i64> = Vec::new();
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                if let Value::Numeric(Numeric::Integer(v)) = row.get_value(0) {
                    results.push(*v);
                }
            }
            StepResult::Done => break,
            _ => {}
        }
    }
    assert_eq!(results, vec![0, 3, 6, 9]);
    drop(stmt);

    // generate_series with alias
    let mut stmt = conn
        .prepare("SELECT value FROM generate_series(1, 3) AS s")
        .unwrap();
    let mut results: Vec<i64> = Vec::new();
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                if let Value::Numeric(Numeric::Integer(v)) = row.get_value(0) {
                    results.push(*v);
                }
            }
            StepResult::Done => break,
            _ => {}
        }
    }
    assert_eq!(results, vec![1, 2, 3]);
    drop(stmt);

    // generate_series in comma-join with a table
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute("INSERT INTO items VALUES (1, 'alpha')")
        .unwrap();
    conn.execute("INSERT INTO items VALUES (2, 'beta')")
        .unwrap();

    let mut stmt = conn
        .prepare("SELECT items.name, gs.value FROM items, generate_series(1, 2) AS gs ORDER BY items.id, gs.value")
        .unwrap();
    let mut results: Vec<String> = Vec::new();
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                let name = row.get_value(0).to_string();
                let val = row.get_value(1).to_string();
                results.push(format!("{name},{val}"));
            }
            StepResult::Done => break,
            _ => {}
        }
    }
    assert_eq!(results, vec!["alpha,1", "alpha,2", "beta,1", "beta,2"]);
}

#[turso_macros::test(mvcc)]
fn test_postgres_natural_join(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)")
        .unwrap();
    conn.execute("CREATE TABLE t2 (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t1 VALUES (1, 'alice', 10)")
        .unwrap();
    conn.execute("INSERT INTO t1 VALUES (2, 'bob', 20)")
        .unwrap();
    conn.execute("INSERT INTO t2 VALUES (1, 'alice', 100)")
        .unwrap();
    conn.execute("INSERT INTO t2 VALUES (3, 'carol', 300)")
        .unwrap();

    // NATURAL JOIN — matches on common columns (id, name)
    let mut stmt = conn
        .prepare("SELECT t1.val, t2.score FROM t1 NATURAL JOIN t2 ORDER BY t1.val")
        .unwrap();
    let mut results: Vec<String> = Vec::new();
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                results.push(format!("{},{}", row.get_value(0), row.get_value(1)));
            }
            StepResult::Done => break,
            _ => {}
        }
    }
    assert_eq!(results, vec!["10,100"]);
    drop(stmt);

    // NATURAL LEFT JOIN — all left rows, NULLs for non-matching right
    let mut stmt = conn
        .prepare("SELECT t1.name, t2.score FROM t1 NATURAL LEFT JOIN t2 ORDER BY t1.id")
        .unwrap();
    let mut results: Vec<String> = Vec::new();
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                results.push(format!("{},{}", row.get_value(0), row.get_value(1)));
            }
            StepResult::Done => break,
            _ => {}
        }
    }
    assert_eq!(results, vec!["alice,100", "bob,"]);
    drop(stmt);

    // NATURAL JOIN with no common columns → cross join
    conn.execute("CREATE TABLE t3 (x INTEGER)").unwrap();
    conn.execute("CREATE TABLE t4 (y INTEGER)").unwrap();
    conn.execute("INSERT INTO t3 VALUES (1)").unwrap();
    conn.execute("INSERT INTO t3 VALUES (2)").unwrap();
    conn.execute("INSERT INTO t4 VALUES (10)").unwrap();
    let mut stmt = conn
        .prepare("SELECT t3.x, t4.y FROM t3 NATURAL JOIN t4 ORDER BY t3.x")
        .unwrap();
    let mut results: Vec<String> = Vec::new();
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                results.push(format!("{},{}", row.get_value(0), row.get_value(1)));
            }
            StepResult::Done => break,
            _ => {}
        }
    }
    assert_eq!(results, vec!["1,10", "2,10"]);
    drop(stmt);

    // JOIN ... USING (col)
    let mut stmt = conn
        .prepare("SELECT t1.val, t2.score FROM t1 JOIN t2 USING (id, name) ORDER BY t1.val")
        .unwrap();
    let mut results: Vec<String> = Vec::new();
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                results.push(format!("{},{}", row.get_value(0), row.get_value(1)));
            }
            StepResult::Done => break,
            _ => {}
        }
    }
    assert_eq!(results, vec!["10,100"]);
}

#[turso_macros::test]
fn test_postgres_compound_order_by(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE t1 (a INTEGER, b TEXT)").unwrap();
    conn.execute("INSERT INTO t1 VALUES (3, 'c'), (1, 'a'), (2, 'b')")
        .unwrap();
    conn.execute("CREATE TABLE t2 (a INTEGER, b TEXT)").unwrap();
    conn.execute("INSERT INTO t2 VALUES (5, 'e'), (4, 'd'), (1, 'a')")
        .unwrap();

    // UNION ALL with ORDER BY
    let mut stmt = conn
        .prepare("SELECT a, b FROM t1 UNION ALL SELECT a, b FROM t2 ORDER BY a")
        .unwrap();
    let mut results: Vec<String> = Vec::new();
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                results.push(format!("{},{}", row.get_value(0), row.get_value(1)));
            }
            StepResult::Done => break,
            _ => {}
        }
    }
    assert_eq!(results, vec!["1,a", "1,a", "2,b", "3,c", "4,d", "5,e"]);

    // UNION (dedup) with ORDER BY DESC
    let mut stmt = conn
        .prepare("SELECT a, b FROM t1 UNION SELECT a, b FROM t2 ORDER BY a DESC")
        .unwrap();
    let mut results: Vec<String> = Vec::new();
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                results.push(format!("{},{}", row.get_value(0), row.get_value(1)));
            }
            StepResult::Done => break,
            _ => {}
        }
    }
    assert_eq!(results, vec!["5,e", "4,d", "3,c", "2,b", "1,a"]);

    // UNION ALL with ORDER BY and LIMIT
    let mut stmt = conn
        .prepare("SELECT a FROM t1 UNION ALL SELECT a FROM t2 ORDER BY a LIMIT 3")
        .unwrap();
    let mut results: Vec<i64> = Vec::new();
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                if let Value::Numeric(Numeric::Integer(v)) = row.get_value(0) {
                    results.push(*v);
                }
            }
            StepResult::Done => break,
            _ => {}
        }
    }
    assert_eq!(results, vec![1, 1, 2]);

    // INTERSECT with ORDER BY
    let mut stmt = conn
        .prepare("SELECT a FROM t1 INTERSECT SELECT a FROM t2 ORDER BY a")
        .unwrap();
    let mut results: Vec<i64> = Vec::new();
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                if let Value::Numeric(Numeric::Integer(v)) = row.get_value(0) {
                    results.push(*v);
                }
            }
            StepResult::Done => break,
            _ => {}
        }
    }
    assert_eq!(results, vec![1]);

    // EXCEPT with ORDER BY
    let mut stmt = conn
        .prepare("SELECT a FROM t1 EXCEPT SELECT a FROM t2 ORDER BY a DESC")
        .unwrap();
    let mut results: Vec<i64> = Vec::new();
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                if let Value::Numeric(Numeric::Integer(v)) = row.get_value(0) {
                    results.push(*v);
                }
            }
            StepResult::Done => break,
            _ => {}
        }
    }
    assert_eq!(results, vec![3, 2]);
}

#[turso_macros::test]
fn test_postgres_insert_default(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT DEFAULT 'unknown', score INTEGER DEFAULT 0)")
        .unwrap();

    // Single row with DEFAULT
    conn.execute("INSERT INTO t (id, name, score) VALUES (1, DEFAULT, 42)")
        .unwrap();

    // Multi-row with mixed DEFAULT
    conn.execute("INSERT INTO t (id, name, score) VALUES (2, 'alice', DEFAULT), (3, DEFAULT, 99)")
        .unwrap();

    // All columns DEFAULT
    conn.execute("INSERT INTO t (id, name, score) VALUES (DEFAULT, DEFAULT, DEFAULT)")
        .unwrap();

    // Verify results
    let mut stmt = conn
        .prepare("SELECT id, name, score FROM t ORDER BY id")
        .unwrap();
    let mut results: Vec<String> = Vec::new();
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                results.push(format!(
                    "{},{},{}",
                    row.get_value(0),
                    row.get_value(1),
                    row.get_value(2)
                ));
            }
            StepResult::Done => break,
            _ => {}
        }
    }
    // id=DEFAULT → NULL (no default defined), all others get their defaults
    // INTEGER PRIMARY KEY with NULL → auto-assigned rowid
    assert_eq!(
        results,
        vec![
            "1,unknown,42", // name=DEFAULT → 'unknown'
            "2,alice,0",    // score=DEFAULT → 0
            "3,unknown,99", // name=DEFAULT → 'unknown'
            "4,unknown,0",  // all DEFAULT
        ]
    );
}

/// Tests DEFAULT with multiple data types and verifies DEFAULT is distinct from NULL.
#[turso_macros::test]
fn test_postgres_insert_default_vs_null(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute(
        "CREATE TABLE items (
            id INTEGER PRIMARY KEY,
            qty INTEGER DEFAULT 15,
            label TEXT DEFAULT 'pending'
        )",
    )
    .unwrap();

    // DEFAULT should use the column default; explicit NULL should insert NULL
    conn.execute(
        "INSERT INTO items (id, qty, label) VALUES (1, DEFAULT, 'custom'), (2, NULL, DEFAULT), (3, DEFAULT, NULL)",
    )
    .unwrap();

    let mut stmt = conn
        .prepare("SELECT id, qty, label FROM items ORDER BY id")
        .unwrap();
    let mut results: Vec<String> = Vec::new();
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                results.push(format!(
                    "{},{},{}",
                    row.get_value(0),
                    row.get_value(1),
                    row.get_value(2)
                ));
            }
            StepResult::Done => break,
            _ => {}
        }
    }
    assert_eq!(
        results,
        vec![
            "1,15,custom", // qty=DEFAULT → 15
            "2,,pending",  // qty=NULL (explicit), label=DEFAULT → 'pending'
            "3,15,",       // qty=DEFAULT → 15, label=NULL (explicit)
        ]
    );
}

/// Tests DEFAULT in a multi-row INSERT where DEFAULT appears in every row for a column.
/// The PG translator optimizes this by removing the column entirely from the column list.
#[turso_macros::test]
fn test_postgres_insert_default_all_rows_same_column(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE kv (k INTEGER PRIMARY KEY, v TEXT DEFAULT 'empty', n INTEGER)")
        .unwrap();

    // 'v' is DEFAULT in BOTH rows — the translator strips the column entirely
    conn.execute("INSERT INTO kv (k, v, n) VALUES (1, DEFAULT, 10), (2, DEFAULT, 20)")
        .unwrap();
    // 'v' is DEFAULT in only ONE row — kept as Expr::Default, resolved at execution
    conn.execute("INSERT INTO kv (k, v, n) VALUES (3, 'hello', 30), (4, DEFAULT, 40)")
        .unwrap();

    let mut stmt = conn.prepare("SELECT k, v, n FROM kv ORDER BY k").unwrap();
    let mut results: Vec<String> = Vec::new();
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                results.push(format!(
                    "{},{},{}",
                    row.get_value(0),
                    row.get_value(1),
                    row.get_value(2)
                ));
            }
            StepResult::Done => break,
            _ => {}
        }
    }
    assert_eq!(
        results,
        vec![
            "1,empty,10", // v=DEFAULT (all-rows optimization)
            "2,empty,20", // v=DEFAULT (all-rows optimization)
            "3,hello,30", // v='hello' (explicit)
            "4,empty,40", // v=DEFAULT (Expr::Default resolution)
        ]
    );
}

/// Tests compound SELECT ORDER BY with multiple sort columns and OFFSET.
#[turso_macros::test]
fn test_postgres_compound_order_by_multi_column(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE employees (dept TEXT, name TEXT, salary INTEGER)")
        .unwrap();
    conn.execute(
        "INSERT INTO employees VALUES
            ('eng', 'alice', 100),
            ('eng', 'bob', 90),
            ('sales', 'carol', 80)",
    )
    .unwrap();
    conn.execute("CREATE TABLE contractors (dept TEXT, name TEXT, salary INTEGER)")
        .unwrap();
    conn.execute(
        "INSERT INTO contractors VALUES
            ('eng', 'dave', 95),
            ('sales', 'eve', 85)",
    )
    .unwrap();

    // Multi-column ORDER BY on UNION ALL
    let mut stmt = conn
        .prepare(
            "SELECT dept, name, salary FROM employees
             UNION ALL
             SELECT dept, name, salary FROM contractors
             ORDER BY dept, salary DESC",
        )
        .unwrap();
    let mut results: Vec<String> = Vec::new();
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                results.push(format!(
                    "{},{},{}",
                    row.get_value(0),
                    row.get_value(1),
                    row.get_value(2)
                ));
            }
            StepResult::Done => break,
            _ => {}
        }
    }
    assert_eq!(
        results,
        vec![
            "eng,alice,100",
            "eng,dave,95",
            "eng,bob,90",
            "sales,eve,85",
            "sales,carol,80",
        ]
    );

    // ORDER BY with LIMIT and OFFSET
    let mut stmt = conn
        .prepare(
            "SELECT name FROM employees
             UNION ALL
             SELECT name FROM contractors
             ORDER BY name
             LIMIT 2 OFFSET 1",
        )
        .unwrap();
    let mut results: Vec<String> = Vec::new();
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                results.push(format!("{}", row.get_value(0)));
            }
            StepResult::Done => break,
            _ => {}
        }
    }
    // Sorted: alice, bob, carol, dave, eve → offset 1, limit 2 → bob, carol
    assert_eq!(results, vec!["bob", "carol"]);
}

/// Tests three-way compound with ORDER BY (a pattern ORMs commonly generate).
#[turso_macros::test]
fn test_postgres_compound_three_way_order_by(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE a (x INTEGER)").unwrap();
    conn.execute("CREATE TABLE b (x INTEGER)").unwrap();
    conn.execute("CREATE TABLE c (x INTEGER)").unwrap();
    conn.execute("INSERT INTO a VALUES (3), (1)").unwrap();
    conn.execute("INSERT INTO b VALUES (2), (4)").unwrap();
    conn.execute("INSERT INTO c VALUES (5), (1)").unwrap();

    // Three-way UNION ALL with ORDER BY
    let mut stmt = conn
        .prepare("SELECT x FROM a UNION ALL SELECT x FROM b UNION ALL SELECT x FROM c ORDER BY x")
        .unwrap();
    let mut results: Vec<i64> = Vec::new();
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                if let Value::Numeric(Numeric::Integer(v)) = row.get_value(0) {
                    results.push(*v);
                }
            }
            StepResult::Done => break,
            _ => {}
        }
    }
    assert_eq!(results, vec![1, 1, 2, 3, 4, 5]);

    // Three-way UNION (dedup) with ORDER BY DESC
    let mut stmt = conn
        .prepare("SELECT x FROM a UNION SELECT x FROM b UNION SELECT x FROM c ORDER BY x DESC")
        .unwrap();
    let mut results: Vec<i64> = Vec::new();
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                if let Value::Numeric(Numeric::Integer(v)) = row.get_value(0) {
                    results.push(*v);
                }
            }
            StepResult::Done => break,
            _ => {}
        }
    }
    assert_eq!(results, vec![5, 4, 3, 2, 1]);
}

/// Tests DEFAULT now() produces a valid timestamp string.
#[turso_macros::test]
fn test_postgres_default_now(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute(
        "CREATE TABLE events (
            id INTEGER PRIMARY KEY,
            created_at TEXT DEFAULT now()
        )",
    )
    .unwrap();

    conn.execute("INSERT INTO events (id) VALUES (1)").unwrap();

    let mut stmt = conn
        .prepare("SELECT created_at FROM events WHERE id = 1")
        .unwrap();
    let StepResult::Row = stmt.step().unwrap() else {
        panic!("expected row");
    };
    let row = stmt.row().unwrap();
    let Value::Text(ts) = row.get_value(0) else {
        panic!(
            "expected text value for created_at, got {:?}",
            row.get_value(0)
        );
    };
    // Should be a timestamp like "2024-01-15 14:30:45.123"
    assert!(ts.value.len() >= 19, "timestamp too short: '{}'", ts.value);
    assert!(
        ts.value.contains('-') && ts.value.contains(':'),
        "timestamp format wrong: '{}'",
        ts.value
    );
}

/// Tests DEFAULT gen_random_uuid() produces a valid UUID string.
#[turso_macros::test]
fn test_postgres_default_gen_random_uuid(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute(
        "CREATE TABLE tokens (
            id INTEGER PRIMARY KEY,
            token TEXT DEFAULT gen_random_uuid()
        )",
    )
    .unwrap();

    conn.execute("INSERT INTO tokens (id) VALUES (1)").unwrap();
    conn.execute("INSERT INTO tokens (id) VALUES (2)").unwrap();

    let mut stmt = conn
        .prepare("SELECT token FROM tokens ORDER BY id")
        .unwrap();
    let mut uuids: Vec<String> = Vec::new();
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                let Value::Text(t) = row.get_value(0) else {
                    panic!("expected text value for token");
                };
                uuids.push(t.value.to_string());
            }
            StepResult::Done => break,
            _ => {}
        }
    }
    assert_eq!(uuids.len(), 2);
    // UUID v4 format: 8-4-4-4-12 hex chars
    for uuid in &uuids {
        let parts: Vec<&str> = uuid.split('-').collect();
        assert_eq!(parts.len(), 5, "UUID format wrong: '{uuid}'");
        assert_eq!(parts[0].len(), 8);
        assert_eq!(parts[1].len(), 4);
        assert_eq!(parts[2].len(), 4);
        assert_eq!(parts[3].len(), 4);
        assert_eq!(parts[4].len(), 12);
    }
    // Two inserts should produce different UUIDs
    assert_ne!(uuids[0], uuids[1]);
}

/// Tests DEFAULT clock_timestamp() works as a PG timestamp alias.
#[turso_macros::test]
fn test_postgres_default_clock_timestamp(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute(
        "CREATE TABLE logs (
            id INTEGER PRIMARY KEY,
            ts TEXT DEFAULT clock_timestamp()
        )",
    )
    .unwrap();

    conn.execute("INSERT INTO logs (id) VALUES (1)").unwrap();

    let mut stmt = conn.prepare("SELECT ts FROM logs WHERE id = 1").unwrap();
    let StepResult::Row = stmt.step().unwrap() else {
        panic!("expected row");
    };
    let row = stmt.row().unwrap();
    let Value::Text(ts) = row.get_value(0) else {
        panic!("expected text value for ts");
    };
    assert!(ts.value.len() >= 19, "timestamp too short: '{}'", ts.value);
}

/// Tests DEFAULT transaction_timestamp() and statement_timestamp() work.
#[turso_macros::test]
fn test_postgres_default_transaction_and_statement_timestamp(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute(
        "CREATE TABLE audit (
            id INTEGER PRIMARY KEY,
            txn_ts TEXT DEFAULT transaction_timestamp(),
            stmt_ts TEXT DEFAULT statement_timestamp()
        )",
    )
    .unwrap();

    conn.execute("INSERT INTO audit (id) VALUES (1)").unwrap();

    let mut stmt = conn
        .prepare("SELECT txn_ts, stmt_ts FROM audit WHERE id = 1")
        .unwrap();
    let StepResult::Row = stmt.step().unwrap() else {
        panic!("expected row");
    };
    let row = stmt.row().unwrap();
    let Value::Text(txn_ts) = row.get_value(0) else {
        panic!("expected text for txn_ts");
    };
    let Value::Text(stmt_ts) = row.get_value(1) else {
        panic!("expected text for stmt_ts");
    };
    // Both should be valid timestamps
    assert!(txn_ts.value.len() >= 19);
    assert!(stmt_ts.value.len() >= 19);
}

/// Tests that now() and gen_random_uuid() also work in SELECT (not just DEFAULT).
#[turso_macros::test]
fn test_postgres_select_now_and_uuid(db: TempDatabase) {
    let conn = db.connect_postgres();

    // SELECT now()
    let mut stmt = conn.prepare("SELECT now()").unwrap();
    let StepResult::Row = stmt.step().unwrap() else {
        panic!("expected row");
    };
    let row = stmt.row().unwrap();
    let Value::Text(ts) = row.get_value(0) else {
        panic!("expected text from now()");
    };
    assert!(
        ts.value.len() >= 19,
        "now() result too short: '{}'",
        ts.value
    );
    drop(stmt);

    // SELECT gen_random_uuid()
    let mut stmt = conn.prepare("SELECT gen_random_uuid()").unwrap();
    let StepResult::Row = stmt.step().unwrap() else {
        panic!("expected row");
    };
    let row = stmt.row().unwrap();
    let Value::Text(uuid) = row.get_value(0) else {
        panic!("expected text from gen_random_uuid()");
    };
    assert_eq!(
        uuid.value.split('-').count(),
        5,
        "UUID format wrong: '{}'",
        uuid.value
    );
}

/// Tests multiple DEFAULT functions in the same table.
#[turso_macros::test]
fn test_postgres_default_multiple_functions(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute(
        "CREATE TABLE records (
            id INTEGER PRIMARY KEY,
            uuid TEXT DEFAULT gen_random_uuid(),
            created TEXT DEFAULT now(),
            status TEXT DEFAULT 'active'
        )",
    )
    .unwrap();

    conn.execute("INSERT INTO records (id) VALUES (1)").unwrap();

    let mut stmt = conn
        .prepare("SELECT uuid, created, status FROM records WHERE id = 1")
        .unwrap();
    let StepResult::Row = stmt.step().unwrap() else {
        panic!("expected row");
    };
    let row = stmt.row().unwrap();

    // uuid should be a valid UUID
    let Value::Text(uuid) = row.get_value(0) else {
        panic!("expected text for uuid");
    };
    assert_eq!(uuid.value.split('-').count(), 5);

    // created should be a timestamp
    let Value::Text(created) = row.get_value(1) else {
        panic!("expected text for created");
    };
    assert!(created.value.len() >= 19);

    // status should be the literal default
    let Value::Text(status) = row.get_value(2) else {
        panic!("expected text for status");
    };
    assert_eq!(status.value, "active");
}

/// Tests DEFAULT with type cast syntax (e.g. '{}'::jsonb).
#[turso_macros::test]
fn test_postgres_default_casted_expression(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute(
        "CREATE TABLE config (
            id INTEGER PRIMARY KEY,
            data jsonb DEFAULT '{}'::jsonb,
            tags jsonb DEFAULT '[]'::jsonb,
            name TEXT DEFAULT 'unnamed'
        )",
    )
    .unwrap();

    conn.execute("INSERT INTO config (id) VALUES (1)").unwrap();

    let mut stmt = conn
        .prepare("SELECT data, tags, name FROM config WHERE id = 1")
        .unwrap();
    let StepResult::Row = stmt.step().unwrap() else {
        panic!("expected row");
    };
    let row = stmt.row().unwrap();

    let Value::Text(data) = row.get_value(0) else {
        panic!("expected text for data, got {:?}", row.get_value(0));
    };
    assert_eq!(
        data.value, "{}",
        "casted default '{{}}'::jsonb should produce '{{}}'"
    );

    let Value::Text(tags) = row.get_value(1) else {
        panic!("expected text for tags, got {:?}", row.get_value(1));
    };
    assert_eq!(
        tags.value, "[]",
        "casted default '[]'::jsonb should produce '[]'"
    );

    let Value::Text(name) = row.get_value(2) else {
        panic!("expected text for name, got {:?}", row.get_value(2));
    };
    assert_eq!(name.value, "unnamed");
}

#[turso_macros::test(mvcc)]
fn test_postgres_delete_with_alias(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE del_test (id INTEGER PRIMARY KEY, a INT)")
        .unwrap();
    conn.execute("INSERT INTO del_test VALUES (1, 10)").unwrap();
    conn.execute("INSERT INTO del_test VALUES (2, 50)").unwrap();
    conn.execute("INSERT INTO del_test VALUES (3, 100)")
        .unwrap();

    // DELETE with alias — WHERE references the alias
    conn.execute("DELETE FROM del_test AS dt WHERE dt.a > 75")
        .unwrap();

    let mut rows = conn
        .query("SELECT id, a FROM del_test ORDER BY id")
        .unwrap()
        .unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    assert_eq!(row.get_value(0).to_string(), "1");
    assert_eq!(row.get_value(1).to_string(), "10");

    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected second row");
    };
    let row = rows.row().unwrap();
    assert_eq!(row.get_value(0).to_string(), "2");
    assert_eq!(row.get_value(1).to_string(), "50");

    let StepResult::Done = rows.step().unwrap() else {
        panic!("expected done after two rows");
    };
}

#[turso_macros::test(mvcc)]
fn test_postgres_update_with_alias(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE upd_test (id INTEGER PRIMARY KEY, a INT)")
        .unwrap();
    conn.execute("INSERT INTO upd_test VALUES (1, 10)").unwrap();
    conn.execute("INSERT INTO upd_test VALUES (2, 50)").unwrap();

    // UPDATE with alias — SET and WHERE reference the alias
    conn.execute("UPDATE upd_test AS ut SET a = ut.a + 1 WHERE ut.id = 1")
        .unwrap();

    let mut rows = conn
        .query("SELECT a FROM upd_test WHERE id = 1")
        .unwrap()
        .unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    assert_eq!(rows.row().unwrap().get_value(0).to_string(), "11");
}

#[turso_macros::test(mvcc)]
fn test_postgres_booleq_boolne(db: TempDatabase) {
    let conn = db.connect_postgres();

    conn.execute("CREATE TABLE booltbl (f1 INTEGER)").unwrap();
    conn.execute("INSERT INTO booltbl VALUES (1)").unwrap();
    conn.execute("INSERT INTO booltbl VALUES (0)").unwrap();
    conn.execute("INSERT INTO booltbl VALUES (1)").unwrap();

    // booleq: select rows where f1 equals 0 (false)
    let mut rows = conn
        .query("SELECT f1 FROM booltbl WHERE booleq(0, f1)")
        .unwrap()
        .unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    assert_eq!(rows.row().unwrap().get_value(0).to_string(), "0");
    let StepResult::Done = rows.step().unwrap() else {
        panic!("expected done");
    };
    drop(rows);

    // boolne: select rows where f1 is not equal to 0
    let mut rows = conn
        .query("SELECT f1 FROM booltbl WHERE boolne(f1, 0)")
        .unwrap()
        .unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    assert_eq!(rows.row().unwrap().get_value(0).to_string(), "1");
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected second row");
    };
    assert_eq!(rows.row().unwrap().get_value(0).to_string(), "1");
    let StepResult::Done = rows.step().unwrap() else {
        panic!("expected done");
    };
}

#[turso_macros::test(mvcc)]
fn test_postgres_pg_input_error_info(db: TempDatabase) {
    let conn = db.connect_postgres();

    // Invalid boolean input → error row with message and sql_error_code
    let mut rows = conn
        .query("SELECT message, sql_error_code FROM pg_input_error_info('junk', 'bool')")
        .unwrap()
        .unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    assert_eq!(
        row.get_value(0).to_string(),
        "invalid input syntax for type boolean: \"junk\""
    );
    assert_eq!(row.get_value(1).to_string(), "22P02");
    let StepResult::Done = rows.step().unwrap() else {
        panic!("expected done");
    };
    drop(rows);

    // Out-of-range int4 → error with 22003 code
    let mut rows = conn
        .query("SELECT message, sql_error_code FROM pg_input_error_info('1000000000000', 'int4')")
        .unwrap()
        .unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    assert_eq!(
        row.get_value(0).to_string(),
        "value \"1000000000000\" is out of range for type integer"
    );
    assert_eq!(row.get_value(1).to_string(), "22003");
    let StepResult::Done = rows.step().unwrap() else {
        panic!("expected done");
    };
    drop(rows);

    // Out-of-range int8 → error with 22003 code
    let mut rows = conn
        .query(
            "SELECT message, sql_error_code FROM pg_input_error_info('10000000000000000000', 'int8')",
        )
        .unwrap()
        .unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    assert_eq!(
        row.get_value(0).to_string(),
        "value \"10000000000000000000\" is out of range for type bigint"
    );
    assert_eq!(row.get_value(1).to_string(), "22003");
    let StepResult::Done = rows.step().unwrap() else {
        panic!("expected done");
    };
    drop(rows);

    // Valid input → all NULLs
    let mut rows = conn
        .query(
            "SELECT message, detail, hint, sql_error_code FROM pg_input_error_info('123', 'int4')",
        )
        .unwrap()
        .unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    assert!(matches!(row.get_value(0), Value::Null));
    assert!(matches!(row.get_value(1), Value::Null));
    assert!(matches!(row.get_value(2), Value::Null));
    assert!(matches!(row.get_value(3), Value::Null));
    let StepResult::Done = rows.step().unwrap() else {
        panic!("expected done");
    };
}

#[turso_macros::test(mvcc)]
fn test_pg_input_error_info_float_overflow(db: TempDatabase) {
    let conn = db.connect_postgres();

    // float4: 1e400 parses as infinity in Rust, but PG treats it as out-of-range
    let mut rows = conn
        .query("SELECT message, sql_error_code FROM pg_input_error_info('1e400', 'float4')")
        .unwrap()
        .unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    assert_eq!(
        row.get_value(0).to_string(),
        "value \"1e400\" is out of range for type real"
    );
    assert_eq!(row.get_value(1).to_string(), "22003");
    drop(rows);

    // float8: 1e4000 out-of-range
    let mut rows = conn
        .query("SELECT message, sql_error_code FROM pg_input_error_info('1e4000', 'float8')")
        .unwrap()
        .unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    assert_eq!(
        row.get_value(0).to_string(),
        "value \"1e4000\" is out of range for type double precision"
    );
    assert_eq!(row.get_value(1).to_string(), "22003");
    drop(rows);

    // float4: valid
    let mut rows = conn
        .query("SELECT pg_input_is_valid('3.14', 'float4')")
        .unwrap()
        .unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    assert_eq!(rows.row().unwrap().get_value(0).to_string(), "1");
    drop(rows);

    // float4: invalid syntax
    let mut rows = conn
        .query("SELECT pg_input_is_valid('xyz', 'float4')")
        .unwrap()
        .unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    assert_eq!(rows.row().unwrap().get_value(0).to_string(), "0");
}

#[turso_macros::test(mvcc)]
fn test_pg_input_error_info_extended_types(db: TempDatabase) {
    let conn = db.connect_postgres();

    // numeric: invalid
    let mut rows = conn
        .query("SELECT message, sql_error_code FROM pg_input_error_info('not_a_number', 'numeric')")
        .unwrap()
        .unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    assert_eq!(
        row.get_value(0).to_string(),
        "invalid input syntax for type numeric: \"not_a_number\""
    );
    assert_eq!(row.get_value(1).to_string(), "22P02");
    drop(rows);

    // numeric: valid
    let mut rows = conn
        .query("SELECT pg_input_is_valid('123.456', 'numeric')")
        .unwrap()
        .unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    assert_eq!(rows.row().unwrap().get_value(0).to_string(), "1");
    drop(rows);

    // uuid: invalid
    let mut rows = conn
        .query("SELECT message, sql_error_code FROM pg_input_error_info('11', 'uuid')")
        .unwrap()
        .unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    assert_eq!(
        row.get_value(0).to_string(),
        "invalid input syntax for type uuid: \"11\""
    );
    assert_eq!(row.get_value(1).to_string(), "22P02");
    drop(rows);

    // uuid: valid
    let mut rows = conn
        .query("SELECT pg_input_is_valid('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'uuid')")
        .unwrap()
        .unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    assert_eq!(rows.row().unwrap().get_value(0).to_string(), "1");
    drop(rows);

    // varchar(4): too long → 22001
    let mut rows = conn
        .query("SELECT message, sql_error_code FROM pg_input_error_info('abcde', 'varchar(4)')")
        .unwrap()
        .unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    assert_eq!(
        row.get_value(0).to_string(),
        "value too long for type character varying(4)"
    );
    assert_eq!(row.get_value(1).to_string(), "22001");
    drop(rows);

    // varchar(4): valid
    let mut rows = conn
        .query("SELECT pg_input_is_valid('abcd', 'varchar(4)')")
        .unwrap()
        .unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    assert_eq!(rows.row().unwrap().get_value(0).to_string(), "1");
    drop(rows);

    // date: invalid
    let mut rows = conn
        .query("SELECT message, sql_error_code FROM pg_input_error_info('not-a-date', 'date')")
        .unwrap()
        .unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    assert_eq!(
        row.get_value(0).to_string(),
        "invalid input syntax for type date: \"not-a-date\""
    );
    assert_eq!(row.get_value(1).to_string(), "22007");
    drop(rows);

    // date: valid
    let mut rows = conn
        .query("SELECT pg_input_is_valid('2024-01-15', 'date')")
        .unwrap()
        .unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    assert_eq!(rows.row().unwrap().get_value(0).to_string(), "1");
    drop(rows);

    // timestamp: invalid
    let mut rows = conn
        .query(
            "SELECT message, sql_error_code FROM pg_input_error_info('not-a-timestamp', 'timestamp')",
        )
        .unwrap()
        .unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    assert_eq!(
        row.get_value(0).to_string(),
        "invalid input syntax for type timestamp: \"not-a-timestamp\""
    );
    assert_eq!(row.get_value(1).to_string(), "22007");
    drop(rows);

    // timestamp: valid
    let mut rows = conn
        .query("SELECT pg_input_is_valid('2024-01-15 10:30:00', 'timestamp')")
        .unwrap()
        .unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    assert_eq!(rows.row().unwrap().get_value(0).to_string(), "1");
    drop(rows);

    // json: invalid
    let mut rows = conn
        .query("SELECT message, sql_error_code FROM pg_input_error_info('not json', 'json')")
        .unwrap()
        .unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    assert_eq!(
        row.get_value(0).to_string(),
        "invalid input syntax for type json: \"not json\""
    );
    assert_eq!(row.get_value(1).to_string(), "22P02");
    drop(rows);

    // json: valid
    let mut rows = conn
        .query("SELECT pg_input_is_valid('{\"key\": \"value\"}', 'json')")
        .unwrap()
        .unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    assert_eq!(rows.row().unwrap().get_value(0).to_string(), "1");
    drop(rows);

    // unknown type: error
    let mut rows = conn
        .query("SELECT message, sql_error_code FROM pg_input_error_info('x', 'nonexistent_type')")
        .unwrap()
        .unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    let row = rows.row().unwrap();
    assert_eq!(
        row.get_value(0).to_string(),
        "type \"nonexistent_type\" does not exist"
    );
    assert_eq!(row.get_value(1).to_string(), "42704");
    drop(rows);

    // oid: valid
    let mut rows = conn
        .query("SELECT pg_input_is_valid('12345', 'oid')")
        .unwrap()
        .unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    assert_eq!(rows.row().unwrap().get_value(0).to_string(), "1");
    drop(rows);

    // oid: invalid
    let mut rows = conn
        .query("SELECT pg_input_is_valid('not_oid', 'oid')")
        .unwrap()
        .unwrap();
    let StepResult::Row = rows.step().unwrap() else {
        panic!("expected row");
    };
    assert_eq!(rows.row().unwrap().get_value(0).to_string(), "0");
}

#[turso_macros::test(mvcc)]
fn test_pg_catalog_dml_rejection(db: TempDatabase) {
    let conn = db.connect_postgres();

    // INSERT into pg_catalog table should produce a clear error
    let err = conn
        .execute("INSERT INTO pg_input_error_info VALUES ('a', 'b', 'c', 'd')")
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("cannot insert into pg_catalog table"),
        "unexpected error: {err}"
    );

    // INSERT into pg_class should also be rejected
    let err = conn
        .execute("INSERT INTO pg_class (oid, relname) VALUES (1, 'test')")
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("cannot insert into pg_catalog table"),
        "unexpected error: {err}"
    );

    // DELETE from pg_catalog table
    let err = conn
        .execute("DELETE FROM pg_type WHERE oid = 1")
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("cannot delete from pg_catalog table"),
        "unexpected error: {err}"
    );

    // UPDATE pg_catalog table
    let err = conn
        .execute("UPDATE pg_namespace SET nspname = 'x' WHERE oid = 1")
        .unwrap_err();
    assert!(
        err.to_string().contains("cannot update pg_catalog table"),
        "unexpected error: {err}"
    );
}

#[turso_macros::test(mvcc)]
fn test_postgres_statement_reprepare_uses_pg_dialect(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE t (x int)").unwrap();
    conn.execute("INSERT INTO t VALUES (41)").unwrap();

    // PostgreSQL-only cast syntax: re-preparation must route the original
    // statement text back through the PostgreSQL dialect, not the SQLite
    // parser.
    let mut stmt = conn.prepare("SELECT (x + 1)::text FROM t").unwrap();

    // DDL on another connection invalidates the schema cookie, forcing the
    // prepared statement to reprepare on its next run.
    let other = db.connect_postgres();
    other.execute("CREATE TABLE u (y int)").unwrap();

    let rows = stmt.run_collect_rows().unwrap();
    assert_eq!(rows, vec![vec![Value::build_text("42")]]);
    assert_eq!(
        stmt.stmt_status(turso_core::StatementStatusCounter::Reprepare),
        1
    );
}
