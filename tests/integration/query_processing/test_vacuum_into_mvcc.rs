use crate::common::{compute_dbhash, ExecRows, TempDatabase};
use std::sync::Arc;
use tempfile::TempDir;
use turso_core::Connection;

fn run_integrity_check(conn: &Arc<Connection>) -> String {
    let rows: Vec<(String,)> = conn.exec_rows("PRAGMA integrity_check");
    rows.into_iter()
        .map(|(text,)| text)
        .collect::<Vec<_>>()
        .join("\n")
}

fn checkpoint_if_mvcc(tmp_db: &TempDatabase, conn: &Arc<Connection>) -> anyhow::Result<()> {
    if tmp_db.enable_mvcc {
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
    }
    Ok(())
}

fn assert_same_content_unless_mvcc(source: &TempDatabase, dest: &TempDatabase) {
    if !source.enable_mvcc {
        assert_eq!(compute_dbhash(source).hash, compute_dbhash(dest).hash);
    }
}

#[turso_macros::test(mvcc)]
fn test_vacuum_into_with_views(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    conn.execute(
        "CREATE TABLE employees (id INTEGER, name TEXT, department TEXT, salary INTEGER)",
    )?;
    conn.execute("INSERT INTO employees VALUES (1, 'Alice', 'Engineering', 100000)")?;
    conn.execute("INSERT INTO employees VALUES (2, 'Bob', 'Sales', 80000)")?;
    conn.execute("INSERT INTO employees VALUES (3, 'Charlie', 'Engineering', 120000)")?;
    conn.execute("INSERT INTO employees VALUES (4, 'Diana', 'HR', 70000)")?;

    conn.execute(
        "CREATE VIEW engineering AS SELECT id, name, salary FROM employees WHERE department = 'Engineering'",
    )?;
    conn.execute(
        "CREATE VIEW high_earners AS SELECT name, salary FROM employees WHERE salary > 90000",
    )?;
    conn.execute(
        "CREATE VIEW dept_summary AS SELECT department, COUNT(*) as cnt FROM employees GROUP BY department",
    )?;

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vacuumed.db");
    conn.execute(format!("VACUUM INTO '{}'", dest_path.to_str().unwrap()))?;

    let dest_db = TempDatabase::new_with_existent_with_opts(&dest_path, tmp_db.db_opts);
    let dest_conn = dest_db.connect_limbo();

    assert_eq!(run_integrity_check(&dest_conn), "ok");
    assert_same_content_unless_mvcc(&tmp_db, &dest_db);

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

#[turso_macros::test(mvcc)]
fn test_vacuum_into_with_triggers(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT)")?;
    conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, product_id INTEGER)")?;
    conn.execute("CREATE TABLE audit_log (action TEXT, tbl TEXT, record_id INTEGER)")?;

    conn.execute(
        "CREATE TRIGGER log_product AFTER INSERT ON products BEGIN
            INSERT INTO audit_log VALUES ('INSERT', 'products', NEW.id);
        END",
    )?;
    conn.execute(
        "CREATE TRIGGER log_order AFTER INSERT ON orders BEGIN
            INSERT INTO audit_log VALUES ('INSERT', 'orders', NEW.id);
        END",
    )?;

    conn.execute("INSERT INTO products VALUES (1, 'Item A'), (2, 'Item B')")?;
    conn.execute("INSERT INTO orders VALUES (1, 1), (2, 2)")?;

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vacuumed.db");
    conn.execute(format!("VACUUM INTO '{}'", dest_path.to_str().unwrap()))?;

    let dest_db = TempDatabase::new_with_existent_with_opts(&dest_path, tmp_db.db_opts);
    let dest_conn = dest_db.connect_limbo();

    assert_eq!(run_integrity_check(&dest_conn), "ok");
    assert_same_content_unless_mvcc(&tmp_db, &dest_db);

    let triggers: Vec<(String,)> =
        dest_conn.exec_rows("SELECT name FROM sqlite_schema WHERE type = 'trigger' ORDER BY name");
    assert_eq!(
        triggers,
        vec![("log_order".to_string(),), ("log_product".to_string(),)]
    );

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

    dest_conn.execute("INSERT INTO products VALUES (3, 'New')")?;
    dest_conn.execute("INSERT INTO orders VALUES (3, 3)")?;

    let new_audit: Vec<(String, String, i64)> = dest_conn
        .exec_rows("SELECT action, tbl, record_id FROM audit_log WHERE record_id = 3 ORDER BY tbl");
    assert_eq!(
        new_audit,
        vec![
            ("INSERT".to_string(), "orders".to_string(), 3),
            ("INSERT".to_string(), "products".to_string(), 3),
        ]
    );

    Ok(())
}

#[cfg_attr(feature = "checksum", ignore)]
#[turso_macros::test(mvcc, init_sql = "CREATE TABLE t (a INTEGER);")]
fn test_vacuum_into_preserves_meta_values(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("INSERT INTO t VALUES (1)")?;
    let dest_dir = TempDir::new()?;

    conn.execute("PRAGMA user_version = 42")?;
    conn.execute("PRAGMA application_id = 12345")?;
    let source_schema_version: Vec<(i64,)> = conn.exec_rows("PRAGMA schema_version");

    let dest_path1 = dest_dir.path().join("vacuumed1.db");
    conn.execute(format!("VACUUM INTO '{}'", dest_path1.to_str().unwrap()))?;

    let dest_db1 = TempDatabase::new_with_existent_with_opts(&dest_path1, tmp_db.db_opts);
    let dest_conn1 = dest_db1.connect_limbo();
    assert_eq!(run_integrity_check(&dest_conn1), "ok");
    assert_same_content_unless_mvcc(&tmp_db, &dest_db1);

    let uv: Vec<(i64,)> = dest_conn1.exec_rows("PRAGMA user_version");
    assert_eq!(uv, vec![(42,)], "user_version should be 42");
    let aid: Vec<(i64,)> = dest_conn1.exec_rows("PRAGMA application_id");
    assert_eq!(aid, vec![(12345,)], "application_id should be 12345");
    let schema_version: Vec<(i64,)> = dest_conn1.exec_rows("PRAGMA schema_version");
    assert_eq!(
        schema_version,
        vec![(source_schema_version[0].0 + 1,)],
        "schema_version should be source schema_version + 1"
    );

    conn.execute("PRAGMA user_version = -1")?;
    conn.execute("PRAGMA application_id = 2147483647")?;

    let dest_path2 = dest_dir.path().join("vacuumed2.db");
    conn.execute(format!("VACUUM INTO '{}'", dest_path2.to_str().unwrap()))?;

    let dest_db2 = TempDatabase::new_with_existent_with_opts(&dest_path2, tmp_db.db_opts);
    let dest_conn2 = dest_db2.connect_limbo();
    assert_eq!(run_integrity_check(&dest_conn2), "ok");
    assert_same_content_unless_mvcc(&tmp_db, &dest_db2);

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
fn test_vacuum_into_large_data_multi_page(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE large_data (id INTEGER PRIMARY KEY, data BLOB)")?;
    for i in 0..100 {
        conn.execute(format!(
            "INSERT INTO large_data VALUES ({i}, randomblob(8192))"
        ))?;
    }

    checkpoint_if_mvcc(&tmp_db, &conn)?;
    let page_count: Vec<(i64,)> = conn.exec_rows("PRAGMA page_count");
    assert!(
        page_count[0].0 > 10,
        "Source should have multiple pages, got: {}",
        page_count[0].0
    );

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vacuumed_large.db");
    conn.execute(format!("VACUUM INTO '{}'", dest_path.to_str().unwrap()))?;

    let dest_db = TempDatabase::new_with_existent_with_opts(&dest_path, tmp_db.db_opts);
    let dest_conn = dest_db.connect_limbo();

    assert_eq!(run_integrity_check(&dest_conn), "ok");
    assert_same_content_unless_mvcc(&tmp_db, &dest_db);

    let count: Vec<(i64,)> = dest_conn.exec_rows("SELECT COUNT(*) FROM large_data");
    assert_eq!(count[0].0, 100, "All 100 rows should be copied");

    let sizes: Vec<(i64,)> =
        dest_conn.exec_rows("SELECT length(data) FROM large_data WHERE id IN (0, 50, 99)");
    assert_eq!(sizes.len(), 3);
    for (size,) in sizes {
        assert_eq!(size, 8192, "Blob size should be preserved");
    }

    Ok(())
}

#[turso_macros::test(mvcc)]
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
    )?;
    conn.execute("CREATE INDEX idx_large_orders ON orders (customer) WHERE amount > 1000")?;

    conn.execute("INSERT INTO orders VALUES (1, 'Alice', 'pending', 500.0)")?;
    conn.execute("INSERT INTO orders VALUES (2, 'Bob', 'completed', 200.0)")?;
    conn.execute("INSERT INTO orders VALUES (3, 'Alice', 'pending', 1500.0)")?;
    conn.execute("INSERT INTO orders VALUES (4, 'Charlie', 'shipped', 2000.0)")?;
    conn.execute("INSERT INTO orders VALUES (5, 'Bob', 'pending', 100.0)")?;

    assert_eq!(run_integrity_check(&conn), "ok");

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vacuumed_partial_idx.db");
    conn.execute(format!("VACUUM INTO '{}'", dest_path.to_str().unwrap()))?;

    let dest_db = TempDatabase::new_with_existent_with_opts(&dest_path, tmp_db.db_opts);
    let dest_conn = dest_db.connect_limbo();

    assert_eq!(run_integrity_check(&dest_conn), "ok");
    assert_same_content_unless_mvcc(&tmp_db, &dest_db);

    let indexes: Vec<(String, String)> = dest_conn.exec_rows(
        "SELECT name, sql FROM sqlite_schema WHERE type = 'index' AND name LIKE 'idx_%' ORDER BY name",
    );
    assert_eq!(indexes.len(), 2);
    for (name, sql) in &indexes {
        assert!(sql.contains("WHERE"), "{name} should keep its WHERE clause");
    }

    let orders: Vec<(i64, String, String, f64)> =
        dest_conn.exec_rows("SELECT id, customer, status, amount FROM orders ORDER BY id");
    assert_eq!(orders.len(), 5);

    let pending: Vec<(String, f64)> = dest_conn.exec_rows(
        "SELECT customer, amount FROM orders WHERE status = 'pending' ORDER BY customer, amount",
    );
    assert_eq!(
        pending,
        vec![
            ("Alice".to_string(), 500.0),
            ("Alice".to_string(), 1500.0),
            ("Bob".to_string(), 100.0)
        ]
    );

    let large: Vec<(String,)> =
        dest_conn.exec_rows("SELECT customer FROM orders WHERE amount > 1000 ORDER BY customer");
    assert_eq!(
        large,
        vec![("Alice".to_string(),), ("Charlie".to_string(),)]
    );

    Ok(())
}
