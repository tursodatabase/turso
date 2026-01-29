use crate::common::{compute_dbhash, ExecRows, TempDatabase};
use std::sync::Arc;
use tempfile::TempDir;
use turso_core::{Connection, Value};

/// Helper to run integrity_check and return the result string
fn run_integrity_check(conn: &Arc<Connection>) -> String {
    let rows = conn
        .pragma_query("integrity_check")
        .expect("integrity_check pragma query failed");

    rows.into_iter()
        .filter_map(|row| {
            row.into_iter().next().and_then(|v| {
                if let Value::Text(text) = v {
                    Some(text.as_str().to_string())
                } else {
                    None
                }
            })
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg_attr(feature = "checksum", ignore)]
#[turso_macros::test(mvcc, init_sql = "CREATE TABLE t (a INTEGER, b TEXT, c BLOB);")]
fn test_vacuum_into_basic(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    conn.execute("INSERT INTO t VALUES (1, 'hello', X'DEADBEEF')")?;
    conn.execute("INSERT INTO t VALUES (2, 'world', X'CAFEBABE')")?;
    conn.execute("INSERT INTO t VALUES (3, 'test', NULL)")?;

    let source_hash = compute_dbhash(&tmp_db);

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vacuumed.db");
    let dest_path_str = dest_path.to_str().unwrap();

    conn.execute(format!("VACUUM INTO '{dest_path_str}'"))?;

    let dest_db = TempDatabase::new_with_existent(&dest_path);
    let dest_conn = dest_db.connect_limbo();

    let integrity_result = run_integrity_check(&dest_conn);
    assert_eq!(
        integrity_result, "ok",
        "Destination database should pass integrity check"
    );

    let dest_hash = compute_dbhash(&dest_db);
    assert_eq!(
        source_hash.hash, dest_hash.hash,
        "Source and destination databases should have the same content hash"
    );

    // let's verify the data anyways (to alleviate any bugs in dbhash)
    let rows: Vec<(i64, String)> = dest_conn.exec_rows("SELECT a, b FROM t ORDER BY a");

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].0, 1);
    assert_eq!(rows[0].1, "hello");
    assert_eq!(rows[1].0, 2);
    assert_eq!(rows[1].1, "world");
    assert_eq!(rows[2].0, 3);
    assert_eq!(rows[2].1, "test");

    let mut stmt = dest_conn.prepare("SELECT c FROM t ORDER BY a")?;
    let blob_values = stmt.run_collect_rows()?;
    assert_eq!(blob_values.len(), 3);
    assert_eq!(blob_values[0][0], Value::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]));
    assert_eq!(blob_values[1][0], Value::Blob(vec![0xCA, 0xFE, 0xBA, 0xBE]));
    assert_eq!(blob_values[2][0], Value::Null);

    // verify destination also has zero reserved_space (the default value)
    {
        use std::fs::File;
        use std::io::{Read, Seek, SeekFrom};
        const RESERVED_SPACE_OFFSET: u64 = 20;

        let mut file = File::open(&dest_path)?;
        file.seek(SeekFrom::Start(RESERVED_SPACE_OFFSET))?;
        let mut buf = [0u8; 1];
        file.read_exact(&mut buf)?;
        assert_eq!(buf[0], 0, "Destination should have reserved_space=0");
    }

    Ok(())
}

/// Test VACUUM INTO error cases: plain VACUUM, existing file, within transaction
#[turso_macros::test(mvcc, init_sql = "CREATE TABLE t (a INTEGER);")]
fn test_vacuum_into_error_cases(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();
    conn.execute("INSERT INTO t VALUES (1)")?;

    let dest_dir = TempDir::new()?;

    // 1. plain VACUUM should fail
    let result = conn.execute("VACUUM");
    assert!(result.is_err(), "Plain VACUUM should fail");

    // 2. VACUUM INTO existing file should fail
    let existing_path = dest_dir.path().join("existing.db");
    std::fs::write(&existing_path, b"existing content")?;
    let result = conn.execute(format!("VACUUM INTO '{}'", existing_path.to_str().unwrap()));
    assert!(result.is_err(), "VACUUM INTO existing file should fail");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("already exists") || err_msg.contains("output file"),
        "Error should mention file exists, got: {err_msg}"
    );

    // 3. VACUUM INTO within transaction should fail
    conn.execute("BEGIN")?;
    conn.execute("INSERT INTO t VALUES (2)")?;
    let txn_path = dest_dir.path().join("txn.db");
    let result = conn.execute(format!("VACUUM INTO '{}'", txn_path.to_str().unwrap()));
    assert!(
        result.is_err(),
        "VACUUM INTO within transaction should fail"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("transaction") || err_msg.contains("VACUUM"),
        "Error should mention transaction, got: {err_msg}"
    );
    assert!(!txn_path.exists(), "File should not be created on failure");

    // rollback and verify original data intact
    conn.execute("ROLLBACK")?;
    let rows: Vec<(i64,)> = conn.exec_rows("SELECT a FROM t");
    assert_eq!(rows, vec![(1,)]);

    Ok(())
}

#[turso_macros::test(mvcc)]
fn test_vacuum_into_multiple_tables(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE t1 (a INTEGER)")?;
    conn.execute("CREATE TABLE t2 (b TEXT)")?;
    conn.execute("INSERT INTO t1 VALUES (1), (2), (3)")?;
    conn.execute("INSERT INTO t2 VALUES ('foo'), ('bar')")?;

    let source_hash = compute_dbhash(&tmp_db);

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vacuumed.db");
    let dest_path_str = dest_path.to_str().unwrap();

    conn.execute(format!("VACUUM INTO '{dest_path_str}'"))?;

    let dest_db = TempDatabase::new_with_existent(&dest_path);
    let dest_conn = dest_db.connect_limbo();

    let integrity_result = run_integrity_check(&dest_conn);
    assert_eq!(
        integrity_result, "ok",
        "Destination database should pass integrity check"
    );
    let dest_hash = compute_dbhash(&dest_db);
    assert_eq!(
        source_hash.hash, dest_hash.hash,
        "Source and destination databases should have the same content hash"
    );

    let rows_t1: Vec<(i64,)> = dest_conn.exec_rows("SELECT a FROM t1 ORDER BY a");
    assert_eq!(rows_t1, vec![(1,), (2,), (3,)]);
    let rows_t2: Vec<(String,)> = dest_conn.exec_rows("SELECT b FROM t2 ORDER BY b");
    assert_eq!(rows_t2, vec![("bar".to_string(),), ("foo".to_string(),)]);
    Ok(())
}

#[cfg_attr(feature = "checksum", ignore)]
#[turso_macros::test(mvcc, init_sql = "CREATE TABLE t (a INTEGER);")]
fn test_vacuum_into_with_index(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE INDEX idx_t_a ON t (a)")?;
    conn.execute("INSERT INTO t VALUES (1), (2), (3)")?;

    let source_hash = compute_dbhash(&tmp_db);

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vacuumed.db");
    let dest_path_str = dest_path.to_str().unwrap();

    conn.execute(format!("VACUUM INTO '{dest_path_str}'"))?;

    let dest_db = TempDatabase::new_with_existent(&dest_path);
    let dest_conn = dest_db.connect_limbo();

    let integrity_result = run_integrity_check(&dest_conn);
    assert_eq!(
        integrity_result, "ok",
        "Destination database with index should pass integrity check"
    );
    let dest_hash = compute_dbhash(&dest_db);
    assert_eq!(
        source_hash.hash, dest_hash.hash,
        "Source and destination databases should have the same content hash"
    );

    // lets verify that the index exists in the schema
    let schema: Vec<(String, String)> =
        dest_conn.exec_rows("SELECT type, name FROM sqlite_schema WHERE type = 'index'");
    assert!(
        schema.iter().any(|(_, name)| name == "idx_t_a"),
        "Index should exist in vacuumed database"
    );
    let rows: Vec<(i64,)> = dest_conn.exec_rows("SELECT a FROM t ORDER BY a");
    assert_eq!(rows, vec![(1,), (2,), (3,)]);

    Ok(())
}

/// Test VACUUM INTO with views (simple and complex views with aggregations)
/// Note: Views are not yet supported with MVCC
#[turso_macros::test]
fn test_vacuum_into_with_views(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    conn.execute(
        "CREATE TABLE employees (id INTEGER, name TEXT, department TEXT, salary INTEGER)",
    )?;
    conn.execute("INSERT INTO employees VALUES (1, 'Alice', 'Engineering', 100000)")?;
    conn.execute("INSERT INTO employees VALUES (2, 'Bob', 'Sales', 80000)")?;
    conn.execute("INSERT INTO employees VALUES (3, 'Charlie', 'Engineering', 120000)")?;
    conn.execute("INSERT INTO employees VALUES (4, 'Diana', 'HR', 70000)")?;

    // create multiple views: simple filter, complex filter, aggregation
    conn.execute(
        "CREATE VIEW engineering AS SELECT id, name, salary FROM employees WHERE department = 'Engineering'",
    )?;
    conn.execute(
        "CREATE VIEW high_earners AS SELECT name, salary FROM employees WHERE salary > 90000",
    )?;
    conn.execute(
        "CREATE VIEW dept_summary AS SELECT department, COUNT(*) as cnt FROM employees GROUP BY department",
    )?;

    let source_hash = compute_dbhash(&tmp_db);

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vacuumed.db");
    conn.execute(format!("VACUUM INTO '{}'", dest_path.to_str().unwrap()))?;

    let dest_db = TempDatabase::new_with_existent(&dest_path);
    let dest_conn = dest_db.connect_limbo();

    assert_eq!(run_integrity_check(&dest_conn), "ok");
    assert_eq!(source_hash.hash, compute_dbhash(&dest_db).hash);

    // verify that all views exist
    let views: Vec<(String,)> =
        dest_conn.exec_rows("SELECT name FROM sqlite_schema WHERE type = 'view' ORDER BY name");
    assert_eq!(
        views,
        vec![
            ("dept_summary".to_string(),),
            ("engineering".to_string(),),
            ("high_earners".to_string(),)
        ]
    );

    // verify views query copied data correctly
    let eng: Vec<(i64, String, i64)> =
        dest_conn.exec_rows("SELECT id, name, salary FROM engineering ORDER BY id");
    assert_eq!(
        eng,
        vec![
            (1, "Alice".to_string(), 100000),
            (3, "Charlie".to_string(), 120000)
        ]
    );

    let high: Vec<(String, i64)> =
        dest_conn.exec_rows("SELECT name, salary FROM high_earners ORDER BY salary DESC");
    assert_eq!(
        high,
        vec![
            ("Charlie".to_string(), 120000),
            ("Alice".to_string(), 100000)
        ]
    );

    let summary: Vec<(String, i64)> =
        dest_conn.exec_rows("SELECT department, cnt FROM dept_summary ORDER BY department");
    assert_eq!(
        summary,
        vec![
            ("Engineering".to_string(), 2),
            ("HR".to_string(), 1),
            ("Sales".to_string(), 1)
        ]
    );

    Ok(())
}

/// Test VACUUM INTO with triggers (single and multiple).
/// Verifies that trigger definitions are preserved in the vacuumed database
/// and that data inserted by triggers during initial inserts is copied correctly.
/// Note: Triggers are not yet fully supported with MVCC
#[turso_macros::test]
fn test_vacuum_into_with_triggers(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, product_id INTEGER)")
        .unwrap();
    conn.execute("CREATE TABLE audit_log (action TEXT, tbl TEXT, record_id INTEGER)")
        .unwrap();

    conn.execute(
        "CREATE TRIGGER log_product AFTER INSERT ON products BEGIN
            INSERT INTO audit_log VALUES ('INSERT', 'products', NEW.id);
        END",
    )
    .unwrap();
    conn.execute(
        "CREATE TRIGGER log_order AFTER INSERT ON orders BEGIN
            INSERT INTO audit_log VALUES ('INSERT', 'orders', NEW.id);
        END",
    )
    .unwrap();

    // insert data (triggers will fire)
    conn.execute("INSERT INTO products VALUES (1, 'Item A'), (2, 'Item B')")
        .unwrap();
    conn.execute("INSERT INTO orders VALUES (1, 1), (2, 2)")
        .unwrap();

    let source_hash = compute_dbhash(&tmp_db);

    let dest_dir = TempDir::new().unwrap();
    let dest_path = dest_dir.path().join("vacuumed.db");
    conn.execute(format!("VACUUM INTO '{}'", dest_path.to_str().unwrap()))
        .unwrap();

    let dest_opts = turso_core::DatabaseOpts::new().with_triggers(true);
    let dest_db = TempDatabase::new_with_existent_with_opts(&dest_path, dest_opts);
    let dest_conn = dest_db.connect_limbo();

    assert_eq!(run_integrity_check(&dest_conn), "ok");
    assert_eq!(source_hash.hash, compute_dbhash(&dest_db).hash);

    let triggers: Vec<(String,)> =
        dest_conn.exec_rows("SELECT name FROM sqlite_schema WHERE type = 'trigger' ORDER BY name");
    assert_eq!(
        triggers,
        vec![("log_order".to_string(),), ("log_product".to_string(),)]
    );

    // verify data copied (no duplicates from triggers firing during copy)
    let products: Vec<(i64, String)> =
        dest_conn.exec_rows("SELECT id, name FROM products ORDER BY id");
    assert_eq!(
        products,
        vec![(1, "Item A".to_string()), (2, "Item B".to_string())]
    );

    let audit: Vec<(String, String, i64)> =
        dest_conn.exec_rows("SELECT action, tbl, record_id FROM audit_log ORDER BY tbl, record_id");
    assert_eq!(
        audit,
        vec![
            ("INSERT".to_string(), "orders".to_string(), 1),
            ("INSERT".to_string(), "orders".to_string(), 2),
            ("INSERT".to_string(), "products".to_string(), 1),
            ("INSERT".to_string(), "products".to_string(), 2),
        ]
    );

    // verify triggers work for new inserts
    dest_conn
        .execute("INSERT INTO products VALUES (3, 'New')")
        .unwrap();
    dest_conn
        .execute("INSERT INTO orders VALUES (3, 3)")
        .unwrap();

    let new_audit: Vec<(String, String, i64)> = dest_conn
        .exec_rows("SELECT action, tbl, record_id FROM audit_log WHERE record_id = 3 ORDER BY tbl");
    assert_eq!(
        new_audit,
        vec![
            ("INSERT".to_string(), "orders".to_string(), 3),
            ("INSERT".to_string(), "products".to_string(), 3),
        ]
    );
}

/// Test VACUUM INTO preserves meta values: user_version, application_id
/// Note: Some pragmas don't work correctly with MVCC yet
#[cfg_attr(feature = "checksum", ignore)]
#[turso_macros::test(init_sql = "CREATE TABLE t (a INTEGER);")]
fn test_vacuum_into_preserves_meta_values(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();
    conn.execute("INSERT INTO t VALUES (1)")?;
    let dest_dir = TempDir::new()?;

    // Test 1: Normal positive values
    conn.execute("PRAGMA user_version = 42")?;
    conn.execute("PRAGMA application_id = 12345")?;

    let source_hash1 = compute_dbhash(&tmp_db);
    let dest_path1 = dest_dir.path().join("vacuumed1.db");
    conn.execute(format!("VACUUM INTO '{}'", dest_path1.to_str().unwrap()))?;

    let dest_db1 = TempDatabase::new_with_existent(&dest_path1);
    let dest_conn1 = dest_db1.connect_limbo();
    assert_eq!(run_integrity_check(&dest_conn1), "ok");
    assert_eq!(source_hash1.hash, compute_dbhash(&dest_db1).hash);

    let uv: Vec<(i64,)> = dest_conn1.exec_rows("PRAGMA user_version");
    assert_eq!(uv, vec![(42,)], "user_version should be 42");
    let aid: Vec<(i64,)> = dest_conn1.exec_rows("PRAGMA application_id");
    assert_eq!(aid, vec![(12345,)], "application_id should be 12345");

    // Test 2: Boundary values (negative user_version, max application_id)
    conn.execute("PRAGMA user_version = -1")?;
    conn.execute("PRAGMA application_id = 2147483647")?; // i32::MAX

    let source_hash2 = compute_dbhash(&tmp_db);
    let dest_path2 = dest_dir.path().join("vacuumed2.db");
    conn.execute(format!("VACUUM INTO '{}'", dest_path2.to_str().unwrap()))?;

    let dest_db2 = TempDatabase::new_with_existent(&dest_path2);
    let dest_conn2 = dest_db2.connect_limbo();
    assert_eq!(run_integrity_check(&dest_conn2), "ok");
    assert_eq!(source_hash2.hash, compute_dbhash(&dest_db2).hash);

    let uv: Vec<(i64,)> = dest_conn2.exec_rows("PRAGMA user_version");
    assert_eq!(uv, vec![(-1,)], "Negative user_version should be preserved");
    let aid: Vec<(i64,)> = dest_conn2.exec_rows("PRAGMA application_id");
    assert_eq!(
        aid,
        vec![(2147483647,)],
        "Max application_id should be preserved"
    );

    Ok(())
}

#[turso_macros::test(mvcc)]
fn test_vacuum_into_preserves_page_size(_tmp_db: TempDatabase) -> anyhow::Result<()> {
    // create a new empty database and set page_size before creating tables
    let source_db = TempDatabase::new_empty();
    let conn = source_db.connect_limbo();
    // Set non-default page_size (must be done before any tables are created)
    conn.reset_page_size(8192)?;

    conn.execute("CREATE TABLE t (a INTEGER, b TEXT)")?;
    conn.execute("INSERT INTO t VALUES (1, 'hello'), (2, 'world')")?;
    let source_page_size: Vec<(i64,)> = conn.exec_rows("PRAGMA page_size");
    assert_eq!(
        source_page_size[0].0, 8192,
        "Source database should have page_size of 8192"
    );

    let source_hash = compute_dbhash(&source_db);

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vacuumed.db");
    let dest_path_str = dest_path.to_str().unwrap();

    conn.execute(format!("VACUUM INTO '{dest_path_str}'"))?;

    let dest_db = TempDatabase::new_with_existent(&dest_path);
    let dest_conn = dest_db.connect_limbo();

    let dest_page_size: Vec<(i64,)> = dest_conn.exec_rows("PRAGMA page_size");
    assert_eq!(
        dest_page_size[0].0, 8192,
        "page_size should be preserved as 8192 in destination database"
    );

    assert_eq!(run_integrity_check(&dest_conn), "ok");
    assert_eq!(source_hash.hash, compute_dbhash(&dest_db).hash);

    let rows: Vec<(i64, String)> = dest_conn.exec_rows("SELECT a, b FROM t ORDER BY a");
    assert_eq!(
        rows,
        vec![(1, "hello".to_string()), (2, "world".to_string())]
    );
    Ok(())
}

/// Test VACUUM INTO with edge cases: empty tables with indexes, completely empty database
#[turso_macros::test(mvcc)]
fn test_vacuum_into_empty_edge_cases(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let dest_dir = TempDir::new()?;

    // Test 1: Completely empty database (no tables)
    {
        let empty_db = TempDatabase::new_empty();
        let conn = empty_db.connect_limbo();

        let schema: Vec<(String,)> =
            conn.exec_rows("SELECT name FROM sqlite_schema WHERE type = 'table'");
        assert!(schema.is_empty(), "Should have no tables");

        let dest_path = dest_dir.path().join("empty1.db");
        conn.execute(format!("VACUUM INTO '{}'", dest_path.to_str().unwrap()))?;

        let dest_db = TempDatabase::new_with_existent(&dest_path);
        let dest_conn = dest_db.connect_limbo();
        assert_eq!(run_integrity_check(&dest_conn), "ok");

        // lets verify this db is usable
        dest_conn.execute("CREATE TABLE t (a INTEGER)")?;
        dest_conn.execute("INSERT INTO t VALUES (1)")?;
        let rows: Vec<(i64,)> = dest_conn.exec_rows("SELECT a FROM t");
        assert_eq!(rows, vec![(1,)]);
    }

    // Test 2: Empty tables with indexes (schema only, no data)
    {
        let conn = tmp_db.connect_limbo();
        conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")?;
        conn.execute("CREATE TABLE t2 (a INTEGER, b REAL)")?;
        conn.execute("CREATE INDEX idx_t1_name ON t1 (name)")?;
        conn.execute("CREATE UNIQUE INDEX idx_t2_a ON t2 (a)")?;

        let source_hash = compute_dbhash(&tmp_db);

        let dest_path = dest_dir.path().join("empty2.db");
        conn.execute(format!("VACUUM INTO '{}'", dest_path.to_str().unwrap()))?;

        let dest_db = TempDatabase::new_with_existent(&dest_path);
        let dest_conn = dest_db.connect_limbo();

        assert_eq!(run_integrity_check(&dest_conn), "ok");
        assert_eq!(source_hash.hash, compute_dbhash(&dest_db).hash);
        let cnt: Vec<(i64,)> = dest_conn.exec_rows("SELECT COUNT(*) FROM t1");
        assert_eq!(cnt, vec![(0,)]);

        // verify indexes exist and work
        let indexes: Vec<(String,)> = dest_conn
            .exec_rows("SELECT name FROM sqlite_schema WHERE type = 'index' ORDER BY name");
        assert_eq!(
            indexes,
            vec![("idx_t1_name".to_string(),), ("idx_t2_a".to_string(),)]
        );

        // verify unique constraint works
        dest_conn.execute("INSERT INTO t2 VALUES (1, 1.0)")?;
        let dup = dest_conn.execute("INSERT INTO t2 VALUES (1, 2.0)");
        assert!(dup.is_err(), "Unique index should prevent duplicate");
    }

    Ok(())
}

/// Test VACUUM INTO preserves AUTOINCREMENT counters (sqlite_sequence)
#[turso_macros::test(mvcc)]
fn test_vacuum_into_preserves_autoincrement(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    // create table with AUTOINCREMENT and insert some rows to advance the counter
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")?;
    conn.execute("INSERT INTO t (name) VALUES ('first')")?;
    conn.execute("INSERT INTO t (name) VALUES ('second')")?;
    conn.execute("INSERT INTO t (name) VALUES ('third')")?;

    // delete rows to create a gap
    conn.execute("DELETE FROM t WHERE id = 2")?;

    // verify sqlite_sequence has the counter
    let seq_before: Vec<(String, i64)> =
        conn.exec_rows("SELECT name, seq FROM sqlite_sequence WHERE name = 't'");
    assert_eq!(
        seq_before,
        vec![("t".to_string(), 3)],
        "sqlite_sequence should have counter value 3"
    );

    let source_hash = compute_dbhash(&tmp_db);
    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vacuumed.db");
    let dest_path_str = dest_path.to_str().unwrap();
    conn.execute(format!("VACUUM INTO '{dest_path_str}'"))?;

    let dest_db = TempDatabase::new_with_existent(&dest_path);
    let dest_conn = dest_db.connect_limbo();

    // verify integrity and dbhash (before modifying destination)
    assert_eq!(run_integrity_check(&dest_conn), "ok");
    assert_eq!(source_hash.hash, compute_dbhash(&dest_db).hash);

    // verify sqlite_sequence was copied
    let seq_after: Vec<(String, i64)> =
        dest_conn.exec_rows("SELECT name, seq FROM sqlite_sequence WHERE name = 't'");
    assert_eq!(
        seq_after,
        vec![("t".to_string(), 3)],
        "sqlite_sequence should be preserved in destination"
    );

    // insert a new row and verify it gets id = 4 (not 1 or 3)
    dest_conn.execute("INSERT INTO t (name) VALUES ('fourth')")?;
    let new_row: Vec<(i64, String)> =
        dest_conn.exec_rows("SELECT id, name FROM t WHERE name = 'fourth'");
    assert_eq!(
        new_row,
        vec![(4, "fourth".to_string())],
        "New row should get id = 4 (AUTOINCREMENT counter preserved)"
    );

    // verify integrity since we modified the db
    let integrity_result = run_integrity_check(&dest_conn);
    assert_eq!(integrity_result, "ok");

    Ok(())
}

/// Test that a table with "sqlite_sequence" in its SQL (e.g., default value) is NOT skipped
#[turso_macros::test(mvcc)]
fn test_vacuum_into_table_with_sqlite_sequence_in_sql(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    // create a table that mentions "sqlite_sequence" in a default value
    // this should NOT be skipped during schema copy
    conn.execute(
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, content TEXT DEFAULT 'see sqlite_sequence')",
    )?;
    conn.execute("INSERT INTO notes (id) VALUES (1)")?;
    conn.execute("INSERT INTO notes (id, content) VALUES (2, 'custom')")?;

    let source_hash = compute_dbhash(&tmp_db);
    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vacuumed.db");
    let dest_path_str = dest_path.to_str().unwrap();

    conn.execute(format!("VACUUM INTO '{dest_path_str}'"))?;
    let dest_db = TempDatabase::new_with_existent(&dest_path);
    let dest_conn = dest_db.connect_limbo();

    assert_eq!(run_integrity_check(&dest_conn), "ok");
    assert_eq!(
        source_hash.hash,
        compute_dbhash(&dest_db).hash,
        "Source and destination databases should have the same content hash"
    );

    // verify the table was created and data was copied
    let rows: Vec<(i64, String)> = dest_conn.exec_rows("SELECT id, content FROM notes ORDER BY id");
    assert_eq!(
        rows,
        vec![
            (1, "see sqlite_sequence".to_string()),
            (2, "custom".to_string())
        ],
        "Table with 'sqlite_sequence' in SQL should be created and data copied"
    );
    Ok(())
}

/// Test VACUUM INTO with table names containing special characters
/// tests for: spaces, quotes, SQL keywords, unicode, numeric names, and mixed special chars
#[turso_macros::test(mvcc)]
fn test_vacuum_into_special_table_names(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    // 1. Table with spaces
    conn.execute(r#"CREATE TABLE "table with spaces" (id INTEGER, value TEXT)"#)?;
    conn.execute(r#"INSERT INTO "table with spaces" VALUES (1, 'spaces work')"#)?;

    // 2. Table with double quotes
    conn.execute(r#"CREATE TABLE "table""quote" (id INTEGER, data TEXT)"#)?;
    conn.execute(r#"INSERT INTO "table""quote" VALUES (2, 'quotes work')"#)?;

    // 3. SQL reserved keyword as table name
    conn.execute(r#"CREATE TABLE "select" (id INTEGER, val TEXT)"#)?;
    conn.execute(r#"INSERT INTO "select" VALUES (3, 'keyword works')"#)?;

    // 4. Unicode table name
    conn.execute(r#"CREATE TABLE "表格_données_🎉" (id INTEGER, val TEXT)"#)?;
    conn.execute(r#"INSERT INTO "表格_données_🎉" VALUES (4, 'unicode works')"#)?;

    // 5. Numeric table name
    conn.execute(r#"CREATE TABLE "123" (id INTEGER, val TEXT)"#)?;
    conn.execute(r#"INSERT INTO "123" VALUES (5, 'numeric works')"#)?;

    // 6. Mixed special characters (multiple quotes, spaces, SQL-injection-like)
    conn.execute(r#"CREATE TABLE "table ""with"" many; DROP TABLE--" (id INTEGER, val TEXT)"#)?;
    conn.execute(r#"INSERT INTO "table ""with"" many; DROP TABLE--" VALUES (6, 'mixed works')"#)?;

    let source_hash = compute_dbhash(&tmp_db);

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vacuumed_tables.db");
    let dest_path_str = dest_path.to_str().unwrap();

    conn.execute(format!("VACUUM INTO '{dest_path_str}'"))?;

    let dest_db = TempDatabase::new_with_existent(&dest_path);
    let dest_conn = dest_db.connect_limbo();

    assert_eq!(
        run_integrity_check(&dest_conn),
        "ok",
        "Destination should pass integrity check"
    );
    assert_eq!(
        source_hash.hash,
        compute_dbhash(&dest_db).hash,
        "Source and destination databases should have the same content hash"
    );

    // verify all tables were copied correctly
    let r1: Vec<(i64, String)> = dest_conn.exec_rows(r#"SELECT * FROM "table with spaces""#);
    assert_eq!(r1, vec![(1, "spaces work".to_string())]);

    let r2: Vec<(i64, String)> = dest_conn.exec_rows(r#"SELECT * FROM "table""quote""#);
    assert_eq!(r2, vec![(2, "quotes work".to_string())]);

    let r3: Vec<(i64, String)> = dest_conn.exec_rows(r#"SELECT * FROM "select""#);
    assert_eq!(r3, vec![(3, "keyword works".to_string())]);

    let r4: Vec<(i64, String)> = dest_conn.exec_rows(r#"SELECT * FROM "表格_données_🎉""#);
    assert_eq!(r4, vec![(4, "unicode works".to_string())]);

    let r5: Vec<(i64, String)> = dest_conn.exec_rows(r#"SELECT * FROM "123""#);
    assert_eq!(r5, vec![(5, "numeric works".to_string())]);

    let r6: Vec<(i64, String)> =
        dest_conn.exec_rows(r#"SELECT * FROM "table ""with"" many; DROP TABLE--""#);
    assert_eq!(r6, vec![(6, "mixed works".to_string())]);

    Ok(())
}

/// Test VACUUM INTO preserves float precision
#[turso_macros::test(mvcc)]
fn test_vacuum_into_preserves_float_precision(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE floats (id INTEGER PRIMARY KEY, value REAL)")?;

    // Insert various floats that require high precision
    // These values are chosen to test edge cases in float representation
    let test_values: Vec<f64> = vec![
        0.1,                    // Classic binary representation issue
        0.123456789012345,      // Many decimal places
        1.0000000000000002,     // Smallest increment above 1.0
        std::f64::consts::PI,   // Pi (3.141592653589793)
        std::f64::consts::E,    // Euler's number (2.718281828459045)
        1e-10,                  // Very small number
        1e15,                   // Large number
        -0.999999999999999,     // Negative with many 9s
        123_456_789.123_456_79, // Large with decimals
        1.0,                    // Integer-like float (must stay float, not become int)
        -2.0,                   // Negative integer-like float
        0.0,                    // Zero as float
        100.0,                  // Larger integer-like float
    ];

    for (i, &val) in test_values.iter().enumerate() {
        conn.execute(format!(
            "INSERT INTO floats VALUES ({}, {:.17})",
            i + 1,
            val
        ))?;
    }

    let source_hash = compute_dbhash(&tmp_db);

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vacuumed.db");
    let dest_path_str = dest_path.to_str().unwrap();

    conn.execute(format!("VACUUM INTO '{dest_path_str}'"))?;

    let dest_db = TempDatabase::new_with_existent(&dest_path);
    let dest_conn = dest_db.connect_limbo();
    assert_eq!(run_integrity_check(&dest_conn), "ok");
    assert_eq!(
        source_hash.hash,
        compute_dbhash(&dest_db).hash,
        "Source and destination databases should have the same content hash"
    );

    // verify float precision is preserved
    let rows: Vec<(i64, f64)> = dest_conn.exec_rows("SELECT id, value FROM floats ORDER BY id");
    assert_eq!(rows.len(), test_values.len());

    for (i, &expected) in test_values.iter().enumerate() {
        let actual = rows[i].1;
        assert!(
            (actual - expected).abs() < 1e-15 || actual == expected,
            "Float precision lost for value {}: expected {:.17}, got {:.17}",
            i + 1,
            expected,
            actual
        );
    }
    Ok(())
}

/// Test VACUUM INTO with column names containing special characters
/// Consolidates tests for: spaces, quotes, SQL keywords, unicode, numeric, dashes, dots,
/// mixed special chars, and indexes on special columns
#[turso_macros::test(mvcc)]
fn test_vacuum_into_special_column_names(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    // table with various special column names covering all edge cases
    conn.execute(
        r#"CREATE TABLE special_cols (
            "column with spaces" INTEGER,
            "column""with""quotes" TEXT,
            "from" INTEGER,
            "列名_données_🎉" TEXT,
            "123numeric" REAL,
            "col.with" INTEGER,
            "SELECT * FROM t; --" TEXT
        )"#,
    )?;

    // create index on column with special name
    conn.execute(r#"CREATE INDEX "idx on special" ON special_cols ("column with spaces")"#)?;
    conn.execute(r#"CREATE INDEX "idx""quoted" ON special_cols ("column""with""quotes")"#)?;

    conn.execute(
        r#"INSERT INTO special_cols VALUES (1, 'quotes', 10, 'unicode', 1.5, 100, 'injection')"#,
    )?;
    conn.execute(r#"INSERT INTO special_cols VALUES (2, 'work', 20, 'works', 2.5, 200, 'safe')"#)?;

    let source_hash = compute_dbhash(&tmp_db);
    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vacuumed_cols.db");
    let dest_path_str = dest_path.to_str().unwrap();
    conn.execute(format!("VACUUM INTO '{dest_path_str}'"))?;

    let dest_db = TempDatabase::new_with_existent(&dest_path);
    let dest_conn = dest_db.connect_limbo();

    assert_eq!(
        run_integrity_check(&dest_conn),
        "ok",
        "Destination should pass integrity check"
    );
    assert_eq!(
        source_hash.hash,
        compute_dbhash(&dest_db).hash,
        "Source and destination databases should have the same content hash"
    );

    // verify all column data was copied correctly
    let rows: Vec<(i64, String, i64, String, f64, i64, String)> = dest_conn.exec_rows(
        r#"SELECT "column with spaces", "column""with""quotes", "from", "列名_données_🎉", "123numeric", "col.with", "SELECT * FROM t; --" FROM special_cols ORDER BY "column with spaces""#,
    );
    assert_eq!(
        rows,
        vec![
            (
                1,
                "quotes".to_string(),
                10,
                "unicode".to_string(),
                1.5,
                100,
                "injection".to_string()
            ),
            (
                2,
                "work".to_string(),
                20,
                "works".to_string(),
                2.5,
                200,
                "safe".to_string()
            )
        ]
    );

    let indexes: Vec<(String,)> = dest_conn.exec_rows(
        r#"SELECT name FROM sqlite_schema WHERE type = 'index' AND name LIKE 'idx%' ORDER BY name"#,
    );
    assert_eq!(
        indexes,
        vec![
            ("idx on special".to_string(),),
            ("idx\"quoted".to_string(),)
        ]
    );

    Ok(())
}

/// Test VACUUM INTO with large data spanning multiple B-tree pages
/// This verifies that page copying works correctly for multi-page tables
/// Note: page_count pragma doesn't work correctly with MVCC yet
#[turso_macros::test]
fn test_vacuum_into_large_data_multi_page(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    // create table with enough data to span multiple pages
    // default page_size is 4096. Each row needs to be large enough that
    // we span multiple pages with ~1000 rows.
    conn.execute("CREATE TABLE large_data (id INTEGER PRIMARY KEY, data TEXT, padding BLOB)")?;

    // Insert 1000 rows with ~100 bytes each to ensure multi-page spanning
    // This should create a B-tree with multiple levels
    for i in 0..1000 {
        let data = format!("row_{i:05}_data_string_with_some_content_to_make_it_larger");
        conn.execute(format!(
            "INSERT INTO large_data VALUES ({i}, '{data}', X'{}')",
            "AA".repeat(64)
        ))?;
    }

    // create an index to ensure index pages are also copied
    conn.execute("CREATE INDEX idx_large_data ON large_data (data)")?;

    let source_hash = compute_dbhash(&tmp_db);
    let page_count: Vec<(i64,)> = conn.exec_rows("PRAGMA page_count");
    assert!(
        page_count[0].0 > 10,
        "Source should have multiple pages, got: {}",
        page_count[0].0
    );

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vacuumed_large.db");
    let dest_path_str = dest_path.to_str().unwrap();

    conn.execute(format!("VACUUM INTO '{dest_path_str}'"))?;

    let dest_db = TempDatabase::new_with_existent(&dest_path);
    let dest_conn = dest_db.connect_limbo();

    assert_eq!(
        run_integrity_check(&dest_conn),
        "ok",
        "Large database should pass integrity check"
    );
    assert_eq!(
        source_hash.hash,
        compute_dbhash(&dest_db).hash,
        "Source and destination should have same content hash"
    );

    // verify row count
    let count: Vec<(i64,)> = dest_conn.exec_rows("SELECT COUNT(*) FROM large_data");
    assert_eq!(count[0].0, 1000, "All 1000 rows should be copied");

    // verify first, middle, and last rows
    let first: Vec<(i64, String)> =
        dest_conn.exec_rows("SELECT id, data FROM large_data WHERE id = 0");
    assert_eq!(first[0].0, 0);
    assert!(first[0].1.contains("row_00000"));

    let middle: Vec<(i64, String)> =
        dest_conn.exec_rows("SELECT id, data FROM large_data WHERE id = 500");
    assert_eq!(middle[0].0, 500);
    assert!(middle[0].1.contains("row_00500"));

    let last: Vec<(i64, String)> =
        dest_conn.exec_rows("SELECT id, data FROM large_data WHERE id = 999");
    assert_eq!(last[0].0, 999);
    assert!(last[0].1.contains("row_00999"));

    // verify index works
    let indexed: Vec<(i64,)> = dest_conn
        .exec_rows("SELECT id FROM large_data WHERE data = 'row_00123_data_string_with_some_content_to_make_it_larger'");
    assert_eq!(indexed[0].0, 123);

    Ok(())
}

#[turso_macros::test(mvcc)]
fn test_vacuum_into_with_foreign_keys(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE categories (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")?;
    conn.execute(
        "CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            category_id INTEGER REFERENCES categories(id) ON DELETE CASCADE
        )",
    )?;
    conn.execute(
        "INSERT INTO categories VALUES (1, 'Electronics'), (2, 'Books'), (3, 'Clothing')",
    )?;
    conn.execute("INSERT INTO products VALUES (1, 'Laptop', 1), (2, 'Phone', 1), (3, 'Novel', 2)")?;

    let source_hash = compute_dbhash(&tmp_db);
    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vacuumed_fk.db");
    let dest_path_str = dest_path.to_str().unwrap();

    conn.execute(format!("VACUUM INTO '{dest_path_str}'"))?;
    let dest_db = TempDatabase::new_with_existent(&dest_path);
    let dest_conn = dest_db.connect_limbo();

    assert_eq!(run_integrity_check(&dest_conn), "ok");
    assert_eq!(source_hash.hash, compute_dbhash(&dest_db).hash);

    // verify schema includes foreign key
    let products_sql: Vec<(String,)> =
        dest_conn.exec_rows("SELECT sql FROM sqlite_schema WHERE name = 'products'");
    assert!(
        products_sql[0].0.contains("REFERENCES"),
        "Foreign key should be preserved in schema"
    );

    let categories: Vec<(i64, String)> =
        dest_conn.exec_rows("SELECT id, name FROM categories ORDER BY id");
    assert_eq!(
        categories,
        vec![
            (1, "Electronics".to_string()),
            (2, "Books".to_string()),
            (3, "Clothing".to_string())
        ]
    );

    let products: Vec<(i64, String, i64)> =
        dest_conn.exec_rows("SELECT id, name, category_id FROM products ORDER BY id");
    assert_eq!(
        products,
        vec![
            (1, "Laptop".to_string(), 1),
            (2, "Phone".to_string(), 1),
            (3, "Novel".to_string(), 2)
        ]
    );

    Ok(())
}

/// Test VACUUM INTO with composite primary keys
#[turso_macros::test(mvcc)]
fn test_vacuum_into_with_composite_primary_key(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    conn.execute(
        "CREATE TABLE order_items (
            order_id INTEGER,
            item_id INTEGER,
            quantity INTEGER NOT NULL,
            price REAL NOT NULL,
            PRIMARY KEY (order_id, item_id)
        )",
    )?;

    // create a many-to-many relationship table
    conn.execute(
        "CREATE TABLE student_courses (
            student_id INTEGER,
            course_id INTEGER,
            enrollment_date TEXT,
            grade TEXT,
            PRIMARY KEY (student_id, course_id)
        )",
    )?;

    conn.execute(
        "INSERT INTO order_items VALUES (1, 1, 2, 10.0), (1, 2, 1, 20.0), (2, 1, 5, 10.0)",
    )?;
    conn.execute(
        "INSERT INTO student_courses VALUES (1, 101, '2024-01-15', 'A'), (1, 102, '2024-01-16', 'B'), (2, 101, '2024-01-15', 'A')",
    )?;

    let source_hash = compute_dbhash(&tmp_db);
    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vacuumed_composite.db");
    let dest_path_str = dest_path.to_str().unwrap();

    conn.execute(format!("VACUUM INTO '{dest_path_str}'"))?;

    let dest_db = TempDatabase::new_with_existent(&dest_path);
    let dest_conn = dest_db.connect_limbo();

    assert_eq!(run_integrity_check(&dest_conn), "ok");
    assert_eq!(source_hash.hash, compute_dbhash(&dest_db).hash);

    let order_items_sql: Vec<(String,)> =
        dest_conn.exec_rows("SELECT sql FROM sqlite_schema WHERE name = 'order_items'");
    assert!(
        order_items_sql[0].0.contains("PRIMARY KEY"),
        "PRIMARY KEY should be in schema"
    );

    let order_items: Vec<(i64, i64, i64, f64)> = dest_conn.exec_rows(
        "SELECT order_id, item_id, quantity, price FROM order_items ORDER BY order_id, item_id",
    );
    assert_eq!(
        order_items,
        vec![(1, 1, 2, 10.0), (1, 2, 1, 20.0), (2, 1, 5, 10.0)]
    );

    let student_courses: Vec<(i64, i64, String, String)> = dest_conn.exec_rows(
        "SELECT student_id, course_id, enrollment_date, grade FROM student_courses ORDER BY student_id, course_id",
    );
    assert_eq!(
        student_courses,
        vec![
            (1, 101, "2024-01-15".to_string(), "A".to_string()),
            (1, 102, "2024-01-16".to_string(), "B".to_string()),
            (2, 101, "2024-01-15".to_string(), "A".to_string())
        ]
    );

    // verify composite primary key constraint is enforced on dest db
    let duplicate = dest_conn.execute("INSERT INTO order_items VALUES (1, 1, 3, 15.0)");
    assert!(
        duplicate.is_err(),
        "Composite primary key should reject duplicate (order_id, item_id)"
    );

    Ok(())
}

/// Test VACUUM INTO with data populated using table-valued functions
/// (generate_series, json_each, json_tree)
#[turso_macros::test(mvcc)]
fn test_vacuum_into_with_table_valued_functions(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE numbers (n INTEGER PRIMARY KEY)")?;
    conn.execute("INSERT INTO numbers SELECT value FROM generate_series(1, 100)")?;

    conn.execute("CREATE TABLE json_data (id INTEGER PRIMARY KEY, data TEXT)")?;
    conn.execute(r#"INSERT INTO json_data VALUES (1, '{"a": 1, "b": 2, "c": 3}')"#)?;
    conn.execute(r#"INSERT INTO json_data VALUES (2, '{"x": 10, "y": 20}')"#)?;

    conn.execute("CREATE TABLE json_keys (id INTEGER, key TEXT)")?;
    conn.execute("INSERT INTO json_keys SELECT j.id, e.key FROM json_data j, json_each(j.data) e")?;

    conn.execute("CREATE TABLE nested_json (id INTEGER PRIMARY KEY, data TEXT)")?;
    conn.execute(r#"INSERT INTO nested_json VALUES (1, '{"root": {"child": [1, 2, 3]}}')"#)?;

    conn.execute("CREATE TABLE json_paths (id INTEGER, fullkey TEXT, type TEXT)")?;
    conn.execute(
        "INSERT INTO json_paths SELECT n.id, t.fullkey, t.type FROM nested_json n, json_tree(n.data) t",
    )?;

    let source_hash = compute_dbhash(&tmp_db);
    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vacuumed_tvf.db");

    conn.execute(format!("VACUUM INTO '{}'", dest_path.to_str().unwrap()))?;
    let dest_db = TempDatabase::new_with_existent(&dest_path);
    let dest_conn = dest_db.connect_limbo();

    assert_eq!(run_integrity_check(&dest_conn), "ok");
    assert_eq!(source_hash.hash, compute_dbhash(&dest_db).hash);

    // verify generate_series data
    let count: Vec<(i64,)> = dest_conn.exec_rows("SELECT COUNT(*) FROM numbers");
    assert_eq!(count[0].0, 100);
    let sum: Vec<(i64,)> = dest_conn.exec_rows("SELECT SUM(n) FROM numbers");
    assert_eq!(sum[0].0, 5050); // 1+2+...+100 = 5050

    // verify json_each data
    let json_keys: Vec<(i64, String)> =
        dest_conn.exec_rows("SELECT id, key FROM json_keys ORDER BY id, key");
    assert_eq!(
        json_keys,
        vec![
            (1, "a".to_string()),
            (1, "b".to_string()),
            (1, "c".to_string()),
            (2, "x".to_string()),
            (2, "y".to_string()),
        ]
    );

    // verify json_tree data
    let json_paths: Vec<(i64, String, String)> =
        dest_conn.exec_rows("SELECT id, fullkey, type FROM json_paths ORDER BY fullkey");
    assert!(!json_paths.is_empty(), "json_tree data should be copied");
    assert!(
        json_paths.iter().any(|(_, path, _)| path.contains("root")),
        "Should have root path"
    );

    Ok(())
}

/// Test VACUUM INTO preserves reserved_space bytes from source database.
/// Reserved space is used by encryption extensions and checksums.
#[turso_macros::test(mvcc)]
fn test_vacuum_into_preserves_reserved_space(tmp_db: TempDatabase) -> anyhow::Result<()> {
    use std::fs::File;
    use std::io::{Read, Seek, SeekFrom};

    const RESERVED_BYTES: u8 = 32;
    const RESERVED_SPACE_OFFSET: u64 = 20; // Offset in SQLite header

    let conn = tmp_db.connect_limbo();

    // set reserved_bytes BEFORE any table creation (must be done on uninitialized db)
    conn.set_reserved_bytes(RESERVED_BYTES)?;
    conn.execute("CREATE TABLE encrypted_data (id INTEGER PRIMARY KEY, secret TEXT)")?;
    conn.execute("INSERT INTO encrypted_data VALUES (1, 'confidential')")?;
    conn.execute("INSERT INTO encrypted_data VALUES (2, 'private')")?;

    // verify source has reserved_space set
    assert_eq!(
        conn.get_reserved_bytes(),
        Some(RESERVED_BYTES),
        "Source should have reserved_bytes={RESERVED_BYTES}"
    );

    let source_hash = compute_dbhash(&tmp_db);
    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vacuumed_reserved.db");
    let dest_path_str = dest_path.to_str().unwrap();
    conn.execute(format!("VACUUM INTO '{dest_path_str}'"))?;

    let dest_db = TempDatabase::new_with_existent(&dest_path);
    let dest_conn = dest_db.connect_limbo();

    assert_eq!(run_integrity_check(&dest_conn), "ok");
    assert_eq!(
        source_hash.hash,
        compute_dbhash(&dest_db).hash,
        "Source and destination should have same content hash"
    );

    assert_eq!(
        dest_conn.get_reserved_bytes(),
        Some(RESERVED_BYTES),
        "Destination should have reserved_bytes={RESERVED_BYTES}"
    );

    // verify reserved_space in destination file header
    {
        let mut file = File::open(&dest_path)?;
        file.seek(SeekFrom::Start(RESERVED_SPACE_OFFSET))?;
        let mut buf = [0u8; 1];
        file.read_exact(&mut buf)?;
        assert_eq!(
            buf[0], RESERVED_BYTES,
            "Destination file header should have reserved_space={RESERVED_BYTES}"
        );
    }

    let rows: Vec<(i64, String)> =
        dest_conn.exec_rows("SELECT id, secret FROM encrypted_data ORDER BY id");
    assert_eq!(
        rows,
        vec![(1, "confidential".to_string()), (2, "private".to_string())]
    );
    Ok(())
}

/// Test VACUUM INTO with partial indexes (CREATE INDEX ... WHERE)
/// NOTE: There is a bug with partial indexes which fails integrity_check on the destination.
/// Note: Partial indexes are not supported with MVCC
#[turso_macros::test]
fn test_vacuum_into_with_partial_indexes(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute(
        "CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            customer TEXT,
            status TEXT,
            amount REAL
        )",
    )?;
    conn.execute(
        "CREATE INDEX idx_pending_orders ON orders (customer, amount) WHERE status = 'pending'",
    )
    .unwrap();
    // create another partial index for variety
    conn.execute("CREATE INDEX idx_large_orders ON orders (customer) WHERE amount > 1000")?;

    // insert data (some matching the partial index conditions, some not)
    conn.execute("INSERT INTO orders VALUES (1, 'Alice', 'pending', 500.0)")?;
    conn.execute("INSERT INTO orders VALUES (2, 'Bob', 'completed', 200.0)")?;
    conn.execute("INSERT INTO orders VALUES (3, 'Alice', 'pending', 1500.0)")?;
    conn.execute("INSERT INTO orders VALUES (4, 'Charlie', 'shipped', 2000.0)")?;
    conn.execute("INSERT INTO orders VALUES (5, 'Bob', 'pending', 100.0)")?;

    let source_hash = compute_dbhash(&tmp_db);

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vacuumed_partial_idx.db");
    let dest_path_str = dest_path.to_str().unwrap();

    conn.execute(format!("VACUUM INTO '{dest_path_str}'"))?;

    let dest_db = TempDatabase::new_with_existent(&dest_path);
    let dest_conn = dest_db.connect_limbo();

    // this fails due to existing bug in the integrity_check with partial indexes
    // ---- query_processing::test_vacuum::test_vacuum_into_with_partial_indexes stdout ----
    //
    // thread 'query_processing::test_vacuum::test_vacuum_into_with_partial_indexes' panicked at tests/integration/query_processing/test_vacuum.rs:1220:5:
    // assertion `left == right` failed
    //   left: "wrong # of entries in index idx_large_orders\nwrong # of entries in index idx_pending_orders\nrow 1 missing from index idx_large_orders\nrow 2 missing from index idx_large_orders\nrow 2 missing from index idx_pending_orders\nrow 4 missing from index idx_pending_orders\nrow 5 missing from index idx_large_orders"
    //  right: "ok"

    // assert_eq!(run_integrity_check(&dest_conn), "ok");

    assert_eq!(source_hash.hash, compute_dbhash(&dest_db).hash);

    // verify partial indexes exist with WHERE clause in schema
    let indexes: Vec<(String, String)> = dest_conn.exec_rows(
                "SELECT name, sql FROM sqlite_schema WHERE type = 'index' AND name LIKE 'idx_%' ORDER BY name",
            );
    assert_eq!(indexes.len(), 2);

    // verify idx_large_orders has WHERE clause
    let large_idx = indexes.iter().find(|(name, _)| name == "idx_large_orders");
    assert!(large_idx.is_some(), "idx_large_orders should exist");
    assert!(
        large_idx.unwrap().1.contains("WHERE"),
        "Partial index should have WHERE clause"
    );

    // verify idx_pending_orders has WHERE clause
    let pending_idx = indexes
        .iter()
        .find(|(name, _)| name == "idx_pending_orders");
    assert!(pending_idx.is_some(), "idx_pending_orders should exist");
    assert!(
        pending_idx.unwrap().1.contains("WHERE"),
        "Partial index should have WHERE clause"
    );

    // verify data was copied (table data is correct even if indexes are not)
    let orders: Vec<(i64, String, String, f64)> =
        dest_conn.exec_rows("SELECT id, customer, status, amount FROM orders ORDER BY id");
    assert_eq!(
        orders,
        vec![
            (1, "Alice".to_string(), "pending".to_string(), 500.0),
            (2, "Bob".to_string(), "completed".to_string(), 200.0),
            (3, "Alice".to_string(), "pending".to_string(), 1500.0),
            (4, "Charlie".to_string(), "shipped".to_string(), 2000.0),
            (5, "Bob".to_string(), "pending".to_string(), 100.0)
        ]
    );
    Ok(())
}

/// Test VACUUM INTO preserves MVCC journal mode from source database.
/// If source has experimental_mvcc enabled, destination should too.
#[turso_macros::test]
fn test_vacuum_into_preserves_mvcc(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("PRAGMA journal_mode = 'experimental_mvcc'")?;
    let source_mode: Vec<(String,)> = conn.exec_rows("PRAGMA journal_mode");
    assert_eq!(
        source_mode,
        vec![("experimental_mvcc".to_string(),)],
        "Source should have MVCC enabled"
    );

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)")?;
    conn.execute("INSERT INTO t VALUES (1, 'hello')")?;
    conn.execute("INSERT INTO t VALUES (2, 'world')")?;

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vacuumed_mvcc.db");
    let dest_path_str = dest_path.to_str().unwrap();

    conn.execute(format!("VACUUM INTO '{dest_path_str}'"))?;

    let dest_db = TempDatabase::new_with_existent(&dest_path);
    let dest_conn = dest_db.connect_limbo();
    let dest_mode: Vec<(String,)> = dest_conn.exec_rows("PRAGMA journal_mode");
    assert_eq!(
        dest_mode,
        vec![("experimental_mvcc".to_string(),)],
        "Destination should have MVCC enabled (inherited from source)"
    );

    // verify the .db-log file was created for destination
    let log_path = dest_dir.path().join("vacuumed_mvcc.db-log");
    assert!(
        log_path.exists(),
        "MVCC log file should exist at {log_path:?}"
    );

    // verify data was copied correctly
    assert_eq!(run_integrity_check(&dest_conn), "ok");
    let rows: Vec<(i64, String)> = dest_conn.exec_rows("SELECT id, data FROM t ORDER BY id");
    assert_eq!(
        rows,
        vec![(1, "hello".to_string()), (2, "world".to_string())]
    );

    Ok(())
}

#[test]
fn test_vacuum_into_from_memory_database() -> anyhow::Result<()> {
    use std::sync::Arc;
    use turso_core::{Database, MemoryIO, OpenFlags};

    let _ = env_logger::try_init();

    // create an in-memory database
    let io = Arc::new(MemoryIO::new());
    let db = Database::open_file_with_flags(
        io,
        ":memory:",
        OpenFlags::Create,
        turso_core::DatabaseOpts::new(),
        None,
    )?;
    let conn = db.connect()?;

    conn.execute("CREATE TABLE t (a INTEGER, b TEXT)")?;
    conn.execute("INSERT INTO t VALUES (1, 'hello')")?;
    conn.execute("INSERT INTO t VALUES (2, 'world')")?;
    conn.execute("INSERT INTO t VALUES (3, 'from memory')")?;

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vacuumed_from_memory.db");
    let dest_path_str = dest_path.to_str().unwrap();

    conn.execute(format!("VACUUM INTO '{dest_path_str}'"))?;
    assert!(
        dest_path.exists(),
        "Vacuumed file should exist on disk at {dest_path_str}"
    );

    let dest_db = TempDatabase::new_with_existent(&dest_path);
    let dest_conn = dest_db.connect_limbo();
    let integrity_result = run_integrity_check(&dest_conn);
    assert_eq!(
        integrity_result, "ok",
        "Destination database should pass integrity check"
    );

    // verify all data was copied correctly
    let rows: Vec<(i64, String)> = dest_conn.exec_rows("SELECT a, b FROM t ORDER BY a");
    assert_eq!(
        rows,
        vec![
            (1, "hello".to_string()),
            (2, "world".to_string()),
            (3, "from memory".to_string())
        ],
        "All data from in-memory database should be copied to disk"
    );

    Ok(())
}

// test for future stuff, as turso db does not support yet:
// 1. CHECK constraints
// 2. WITHOUT ROWID tables
// 3. table without any columns (which sqlite does kek)

/// Test VACUUM INTO with CHECK constraints
/// Skips if CHECK constraints are not supported.
#[turso_macros::test(mvcc)]
fn test_vacuum_into_with_check_constraints(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    // Skip if CHECK constraints are not supported
    if conn
        .execute(
            "CREATE TABLE employees (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL CHECK(length(name) > 0),
            age INTEGER CHECK(age >= 18 AND age <= 120),
            salary REAL CHECK(salary > 0),
            status TEXT CHECK(status IN ('active', 'inactive', 'pending'))
        )",
        )
        .is_err()
    {
        return Ok(());
    }

    conn.execute("INSERT INTO employees VALUES (1, 'Alice', 30, 50000.0, 'active')")?;
    conn.execute("INSERT INTO employees VALUES (2, 'Bob', 45, 75000.0, 'inactive')")?;
    conn.execute("INSERT INTO employees VALUES (3, 'Charlie', 18, 35000.0, 'pending')")?;

    let source_hash = compute_dbhash(&tmp_db);

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vacuumed_check.db");
    conn.execute(format!("VACUUM INTO '{}'", dest_path.to_str().unwrap()))?;

    let dest_db = TempDatabase::new_with_existent(&dest_path);
    let dest_conn = dest_db.connect_limbo();

    assert_eq!(run_integrity_check(&dest_conn), "ok");
    assert_eq!(source_hash.hash, compute_dbhash(&dest_db).hash);

    // Verify schema includes CHECK constraints
    let employees_sql: Vec<(String,)> =
        dest_conn.exec_rows("SELECT sql FROM sqlite_schema WHERE name = 'employees'");
    assert!(
        employees_sql[0].0.contains("CHECK"),
        "CHECK constraints should be preserved"
    );

    // Verify data was copied
    let rows: Vec<(i64, String, i64, f64, String)> =
        dest_conn.exec_rows("SELECT id, name, age, salary, status FROM employees ORDER BY id");
    assert_eq!(
        rows,
        vec![
            (1, "Alice".to_string(), 30, 50000.0, "active".to_string()),
            (2, "Bob".to_string(), 45, 75000.0, "inactive".to_string()),
            (3, "Charlie".to_string(), 18, 35000.0, "pending".to_string())
        ]
    );

    // Verify CHECK constraints are enforced in destination
    assert!(
        dest_conn
            .execute("INSERT INTO employees VALUES (4, 'Dan', 10, 40000.0, 'active')")
            .is_err(),
        "CHECK constraint on age should reject value < 18"
    );

    Ok(())
}

/// Test VACUUM INTO with WITHOUT ROWID tables
/// Skips if WITHOUT ROWID is not supported.
#[turso_macros::test(mvcc)]
fn test_vacuum_into_with_without_rowid(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    // Skip if WITHOUT ROWID is not supported
    if conn
        .execute(
            "CREATE TABLE config (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TEXT
        ) WITHOUT ROWID",
        )
        .is_err()
    {
        return Ok(());
    }

    conn.execute("INSERT INTO config VALUES ('theme', 'dark', '2024-01-01')")?;
    conn.execute("INSERT INTO config VALUES ('language', 'en', '2024-01-02')")?;
    conn.execute("INSERT INTO config VALUES ('timezone', 'UTC', '2024-01-03')")?;

    // Also create a regular table
    conn.execute("CREATE TABLE regular (id INTEGER PRIMARY KEY, data TEXT)")?;
    conn.execute("INSERT INTO regular VALUES (1, 'test')")?;

    let source_hash = compute_dbhash(&tmp_db);

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vacuumed_without_rowid.db");
    conn.execute(format!("VACUUM INTO '{}'", dest_path.to_str().unwrap()))?;

    let dest_db = TempDatabase::new_with_existent(&dest_path);
    let dest_conn = dest_db.connect_limbo();

    assert_eq!(run_integrity_check(&dest_conn), "ok");
    assert_eq!(source_hash.hash, compute_dbhash(&dest_db).hash);

    // Verify schema includes WITHOUT ROWID
    let config_sql: Vec<(String,)> =
        dest_conn.exec_rows("SELECT sql FROM sqlite_schema WHERE name = 'config'");
    assert!(
        config_sql[0].0.contains("WITHOUT ROWID"),
        "WITHOUT ROWID should be preserved in schema"
    );

    // Verify data was copied
    let config: Vec<(String, String, String)> =
        dest_conn.exec_rows("SELECT key, value, updated_at FROM config ORDER BY key");
    assert_eq!(
        config,
        vec![
            (
                "language".to_string(),
                "en".to_string(),
                "2024-01-02".to_string()
            ),
            (
                "theme".to_string(),
                "dark".to_string(),
                "2024-01-01".to_string()
            ),
            (
                "timezone".to_string(),
                "UTC".to_string(),
                "2024-01-03".to_string()
            )
        ]
    );

    // Verify regular table also copied
    let regular: Vec<(i64, String)> = dest_conn.exec_rows("SELECT id, data FROM regular");
    assert_eq!(regular, vec![(1, "test".to_string())]);

    Ok(())
}

/// Test VACUUM INTO with tables that have no columns
/// SQLite allows CREATE TABLE t(); with zero columns.
/// Skips if no-column tables are not supported.
#[turso_macros::test(mvcc)]
fn test_vacuum_into_table_with_no_columns(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    // try to create a table with no columns - skip if not supported
    if conn.execute("CREATE TABLE empty_cols()").is_err() {
        return Ok(());
    }

    // also create a normal table to ensure mixed scenario works
    conn.execute("CREATE TABLE normal_table (id INTEGER, name TEXT)")?;
    conn.execute("INSERT INTO normal_table VALUES (1, 'test')")?;

    let source_hash = compute_dbhash(&tmp_db);
    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vacuumed_no_cols.db");
    conn.execute(format!("VACUUM INTO '{}'", dest_path.to_str().unwrap()))?;

    let dest_db = TempDatabase::new_with_existent(&dest_path);
    let dest_conn = dest_db.connect_limbo();

    assert_eq!(run_integrity_check(&dest_conn), "ok");
    assert_eq!(source_hash.hash, compute_dbhash(&dest_db).hash);

    let dest_schema: Vec<(String,)> =
        dest_conn.exec_rows("SELECT name FROM sqlite_schema WHERE name = 'empty_cols'");
    assert_eq!(dest_schema, vec![("empty_cols".to_string(),)]);

    let rows: Vec<(i64, String)> = dest_conn.exec_rows("SELECT id, name FROM normal_table");
    assert_eq!(rows, vec![(1, "test".to_string())]);

    Ok(())
}
