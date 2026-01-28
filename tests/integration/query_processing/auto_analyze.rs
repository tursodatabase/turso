use crate::common::{limbo_exec_rows, ExecRows, TempDatabase};
use rusqlite::types::Value;
use std::time::Instant;

/// Extract table access order from EXPLAIN QUERY PLAN.
/// Returns table names in the order they appear in the plan (SCAN/SEARCH lines).
fn extract_table_order(eqp_rows: &[Vec<Value>]) -> Vec<String> {
    let mut tables = Vec::new();
    for row in eqp_rows {
        if let Value::Text(detail) = &row[3] {
            if let Some(rest) = detail.strip_prefix("SCAN ") {
                let table = rest.split_whitespace().next().unwrap();
                tables.push(table.to_string());
            } else if let Some(rest) = detail.strip_prefix("SEARCH ") {
                let table = rest.split_whitespace().next().unwrap();
                tables.push(table.to_string());
            }
        }
    }
    tables
}

fn plan_has_detail(eqp_rows: &[Vec<Value>], needle: &str) -> bool {
    eqp_rows.iter().any(|row| match &row[3] {
        Value::Text(detail) => detail.contains(needle),
        _ => false,
    })
}

#[test]
fn auto_analyze_disabled_by_default() -> anyhow::Result<()> {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE t (id INTEGER)")?;
    conn.execute("INSERT INTO t VALUES (1), (2)")?;

    let rows: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM t");
    assert_eq!(rows, vec![(2,)]);
    assert!(conn.auto_analyze_stats_snapshot().is_none());

    Ok(())
}

#[test]
fn auto_analyze_is_per_connection() -> anyhow::Result<()> {
    let tmp_db = TempDatabase::new_empty();
    let conn1 = tmp_db.connect_limbo();
    let conn2 = tmp_db.connect_limbo();

    conn1.execute("CREATE TABLE t (id INTEGER)")?;
    conn1.execute("INSERT INTO t VALUES (1), (2), (3)")?;
    conn1.execute("PRAGMA autoanalyze = 1")?;

    let rows1: Vec<(i64,)> = conn1.exec_rows("SELECT count(*) FROM t");
    assert_eq!(rows1, vec![(3,)]);

    let stats1 = conn1
        .auto_analyze_stats_snapshot()
        .expect("autoanalyze enabled");
    assert_eq!(stats1.row_count("t"), Some(3));
    assert!(conn2.auto_analyze_stats_snapshot().is_none());

    conn2.execute("PRAGMA autoanalyze = 1")?;
    let stats2 = conn2
        .auto_analyze_stats_snapshot()
        .expect("autoanalyze enabled");
    assert_eq!(stats2.row_count("t"), None);

    let rows2: Vec<(i64,)> = conn2.exec_rows("SELECT count(*) FROM t");
    assert_eq!(rows2, vec![(3,)]);
    let stats2 = conn2
        .auto_analyze_stats_snapshot()
        .expect("autoanalyze enabled");
    assert_eq!(stats2.row_count("t"), Some(3));

    Ok(())
}

#[test]
fn auto_analyze_disable_clears_stats() -> anyhow::Result<()> {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();

    conn.execute("PRAGMA autoanalyze = 1")?;
    conn.execute("CREATE TABLE t (id INTEGER)")?;
    conn.execute("INSERT INTO t VALUES (1), (2), (3)")?;

    let rows: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM t");
    assert_eq!(rows, vec![(3,)]);
    let stats = conn
        .auto_analyze_stats_snapshot()
        .expect("autoanalyze enabled");
    assert_eq!(stats.row_count("t"), Some(3));

    conn.execute("PRAGMA autoanalyze = 0")?;
    assert!(conn.auto_analyze_stats_snapshot().is_none());

    conn.execute("PRAGMA autoanalyze = 1")?;
    let stats = conn
        .auto_analyze_stats_snapshot()
        .expect("autoanalyze enabled");
    assert_eq!(stats.row_count("t"), None);

    Ok(())
}

#[test]
fn auto_analyze_index_full_scan_updates_row_count() -> anyhow::Result<()> {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();

    conn.execute("PRAGMA autoanalyze = 1")?;
    conn.execute("CREATE TABLE t (a INTEGER, b INTEGER)")?;
    conn.execute("CREATE INDEX idx_t_a ON t(a)")?;
    conn.execute("INSERT INTO t SELECT value, value * 10 FROM generate_series(1, 2000)")?;

    let query = "SELECT a FROM t ORDER BY a";
    let eqp_rows = limbo_exec_rows(&conn, &format!("EXPLAIN QUERY PLAN {query}"));
    assert!(
        plan_has_detail(&eqp_rows, "USING COVERING INDEX idx_t_a"),
        "expected covering index scan, got {eqp_rows:?}"
    );

    let rows: Vec<(i64,)> = conn.exec_rows(query);
    assert_eq!(rows.len(), 2000);
    assert_eq!(rows.first().copied(), Some((1,)));
    assert_eq!(rows.last().copied(), Some((2000,)));

    let stats = conn
        .auto_analyze_stats_snapshot()
        .expect("autoanalyze enabled");
    assert_eq!(stats.row_count("t"), Some(2000));

    Ok(())
}

#[test]
fn auto_analyze_index_range_scan_tracks_rows() -> anyhow::Result<()> {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();

    conn.execute("PRAGMA autoanalyze = 1")?;
    conn.execute("CREATE TABLE t (a INTEGER)")?;
    conn.execute("CREATE INDEX idx_t_a ON t(a)")?;
    conn.execute("INSERT INTO t SELECT value FROM generate_series(1, 2000)")?;

    let query = "SELECT a FROM t WHERE a = 300";
    let eqp_rows = limbo_exec_rows(&conn, &format!("EXPLAIN QUERY PLAN {query}"));
    assert!(
        plan_has_detail(&eqp_rows, "SEARCH t USING INDEX idx_t_a"),
        "expected index range scan, got {eqp_rows:?}"
    );

    let rows: Vec<(i64,)> = conn.exec_rows(query);
    assert_eq!(rows, vec![(300,)]);

    let stats = conn
        .auto_analyze_stats_snapshot()
        .expect("autoanalyze enabled");
    assert_eq!(stats.row_count("t"), None);
    let range_count = stats
        .index_range_row_count("idx_t_a")
        .expect("expected range scan count");
    let expected_min = rows.len() as u64;
    let expected_max = expected_min.saturating_add(1);
    assert!(
        range_count >= expected_min && range_count <= expected_max,
        "expected range scan count between {expected_min} and {expected_max}, got {range_count}"
    );

    Ok(())
}

#[test]
fn auto_analyze_row_count_incremented_on_insert() -> anyhow::Result<()> {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();

    conn.execute("PRAGMA autoanalyze = 1")?;
    conn.execute("CREATE TABLE t (id INTEGER)")?;
    conn.execute("INSERT INTO t VALUES (1), (2), (3)")?;

    let rows: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM t");
    assert_eq!(rows, vec![(3,)]);

    let stats = conn
        .auto_analyze_stats_snapshot()
        .expect("autoanalyze enabled");
    assert_eq!(stats.row_count("t"), Some(3));

    // INSERT should increment the row count, not invalidate it
    conn.execute("INSERT INTO t VALUES (4)")?;
    let stats = conn
        .auto_analyze_stats_snapshot()
        .expect("autoanalyze enabled");
    assert_eq!(
        stats.row_count("t"),
        Some(4),
        "INSERT should increment row count"
    );

    // Multiple inserts should accumulate
    conn.execute("INSERT INTO t VALUES (5), (6)")?;
    let stats = conn
        .auto_analyze_stats_snapshot()
        .expect("autoanalyze enabled");
    assert_eq!(
        stats.row_count("t"),
        Some(6),
        "multiple INSERTs should accumulate"
    );

    Ok(())
}

#[test]
fn auto_analyze_reorders_join_after_stats() -> anyhow::Result<()> {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();

    conn.execute("PRAGMA autoanalyze = 1")?;
    conn.execute("CREATE TABLE t_big (id INTEGER)")?;
    conn.execute("CREATE TABLE t_small (id INTEGER)")?;
    conn.execute("INSERT INTO t_big SELECT value FROM generate_series(1, 2000)")?;
    conn.execute("INSERT INTO t_small SELECT value FROM generate_series(1, 3)")?;

    let query = "SELECT * FROM t_big JOIN t_small ON t_big.id = t_small.id";
    let eqp_before = limbo_exec_rows(&conn, &format!("EXPLAIN QUERY PLAN {query}"));
    let order_before = extract_table_order(&eqp_before);
    assert_eq!(
        order_before.first().map(String::as_str),
        Some("t_big"),
        "expected planner to keep FROM order without stats, got {order_before:?}"
    );

    let big_count: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM t_big");
    assert_eq!(big_count, vec![(2000,)]);
    let small_count: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM t_small");
    assert_eq!(small_count, vec![(3,)]);

    let stats = conn
        .auto_analyze_stats_snapshot()
        .expect("autoanalyze enabled");
    assert_eq!(stats.row_count("t_big"), Some(2000));
    assert_eq!(stats.row_count("t_small"), Some(3));

    let eqp_after = limbo_exec_rows(&conn, &format!("EXPLAIN QUERY PLAN {query}"));
    let order_after = extract_table_order(&eqp_after);
    assert_eq!(
        order_after.first().map(String::as_str),
        Some("t_small"),
        "expected planner to start with t_small after auto stats, got {order_after:?}"
    );

    Ok(())
}

// ============================================================================
// Comprehensive auto_analyze demonstration
// ============================================================================
// This demonstrates how auto_analyze progressively improves query
// planning by collecting statistics during normal query execution.

/// This test shows how auto_analyze collects statistics from various query types:
/// - Full table scans (COUNT(*), SELECT *)
/// - Index full scans (ORDER BY on indexed column)
/// - Index range scans (WHERE indexed_col = value)
///
/// And how these stats improve query planning for:
/// - Join ordering (smaller table first)
/// - Index selection
/// - Selectivity estimation
#[test]
fn progressive_stats_collection() -> anyhow::Result<()> {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();

    conn.execute("PRAGMA autoanalyze = 1")?;

    // schema with multiple tables of varying sizes
    conn.execute(
        "CREATE TABLE customers (
            id INTEGER PRIMARY KEY,
            name TEXT,
            region TEXT,
            created_at INTEGER
        )",
    )?;
    conn.execute(
        "CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            product_id INTEGER,
            quantity INTEGER,
            order_date INTEGER
        )",
    )?;
    conn.execute(
        "CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            name TEXT,
            category TEXT,
            price REAL
        )",
    )?;
    conn.execute(
        "CREATE TABLE order_items (
            id INTEGER PRIMARY KEY,
            order_id INTEGER,
            product_id INTEGER,
            quantity INTEGER,
            unit_price REAL
        )",
    )?;

    // indexes for common query patterns
    conn.execute("CREATE INDEX idx_orders_customer ON orders(customer_id)")?;
    conn.execute("CREATE INDEX idx_orders_product ON orders(product_id)")?;
    conn.execute("CREATE INDEX idx_orders_date ON orders(order_date)")?;
    conn.execute("CREATE INDEX idx_items_order ON order_items(order_id)")?;
    conn.execute("CREATE INDEX idx_items_product ON order_items(product_id)")?;
    conn.execute("CREATE INDEX idx_products_category ON products(category)")?;
    conn.execute("CREATE INDEX idx_customers_region ON customers(region)")?;

    // Populate with varying cardinalities:
    // - customers: 100 rows (small dimension table)
    // - products: 50 rows (small dimension table)
    // - orders: 5000 rows (medium fact table)
    // - order_items: 15000 rows (large detail table)
    conn.execute(
        "INSERT INTO customers
         SELECT value, 'Customer ' || value,
                CASE value % 5
                    WHEN 0 THEN 'North'
                    WHEN 1 THEN 'South'
                    WHEN 2 THEN 'East'
                    WHEN 3 THEN 'West'
                    ELSE 'Central'
                END,
                1700000000 + value * 86400
         FROM generate_series(1, 100)",
    )?;
    conn.execute(
        "INSERT INTO products
         SELECT value, 'Product ' || value,
                CASE value % 4
                    WHEN 0 THEN 'Electronics'
                    WHEN 1 THEN 'Clothing'
                    WHEN 2 THEN 'Food'
                    ELSE 'Other'
                END,
                9.99 + (value % 100) * 0.5
         FROM generate_series(1, 50)",
    )?;
    conn.execute(
        "INSERT INTO orders
         SELECT value,
                (value % 100) + 1,
                (value % 50) + 1,
                (value % 10) + 1,
                1700000000 + (value % 365) * 86400
         FROM generate_series(1, 5000)",
    )?;
    conn.execute(
        "INSERT INTO order_items
         SELECT value,
                (value % 5000) + 1,
                (value % 50) + 1,
                (value % 5) + 1,
                9.99 + (value % 20) * 0.25
         FROM generate_series(1, 15000)",
    )?;

    // No stats yet - optimizer uses defaults
    let stats = conn
        .auto_analyze_stats_snapshot()
        .expect("autoanalyze enabled");
    assert_eq!(stats.row_count("customers"), None);
    assert_eq!(stats.row_count("orders"), None);
    assert_eq!(stats.row_count("products"), None);
    assert_eq!(stats.row_count("order_items"), None);

    // A 4-way join without stats - optimizer uses FROM order
    let query = "SELECT c.name, o.id, p.name, oi.quantity
                 FROM orders o
                 JOIN customers c ON c.id = o.customer_id
                 JOIN order_items oi ON oi.order_id = o.id
                 JOIN products p ON p.id = oi.product_id
                 WHERE c.region = 'North'";
    let plan_before = extract_table_order(&limbo_exec_rows(
        &conn,
        &format!("EXPLAIN QUERY PLAN {query}"),
    ));
    // Without stats, the optimizer keeps FROM clause order
    assert!(
        !plan_before.is_empty(),
        "expected plan to have tables, got empty"
    );

    // Collect stats via COUNT(*)
    let _: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM customers");
    let _: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM orders");
    let _: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM products");
    let _: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM order_items");

    let stats = conn
        .auto_analyze_stats_snapshot()
        .expect("autoanalyze enabled");
    assert_eq!(stats.row_count("customers"), Some(100));
    assert_eq!(stats.row_count("orders"), Some(5000));
    assert_eq!(stats.row_count("products"), Some(50));
    assert_eq!(stats.row_count("order_items"), Some(15000));

    // After stats, optimizer should reorder joins
    let plan_after = extract_table_order(&limbo_exec_rows(
        &conn,
        &format!("EXPLAIN QUERY PLAN {query}"),
    ));

    // With stats, optimizer should prefer smaller tables first
    // products (50) or customers (100) should come before order_items (15000)
    let order_items_pos = plan_after.iter().position(|t| t == "order_items");
    let products_pos = plan_after.iter().position(|t| t == "products");
    let customers_pos = plan_after.iter().position(|t| t == "customers");

    if let (Some(items_pos), Some(prod_pos)) = (order_items_pos, products_pos) {
        assert!(
            prod_pos < items_pos,
            "Expected products before order_items, got plan: {plan_after:?}"
        );
    }

    // Also verify customers (100 rows) comes before order_items (15000 rows)
    if let (Some(items_pos), Some(cust_pos)) = (order_items_pos, customers_pos) {
        assert!(
            cust_pos < items_pos,
            "Expected customers before order_items, got plan: {plan_after:?}"
        );
    }

    // Phase 4: Index range scan stats
    // Execute some point lookups to gather index selectivity info
    let _: Vec<(i64, i64)> =
        conn.exec_rows("SELECT customer_id, quantity FROM orders WHERE customer_id = 42");
    let _: Vec<(i64, i64)> =
        conn.exec_rows("SELECT order_id, quantity FROM order_items WHERE order_id = 100");

    let stats = conn
        .auto_analyze_stats_snapshot()
        .expect("autoanalyze enabled");

    // These index range scans should have recorded some stats
    // The exact counts depend on data distribution
    let customer_idx_count = stats.index_range_row_count("idx_orders_customer");
    let order_idx_count = stats.index_range_row_count("idx_items_order");

    // Each customer has ~50 orders (5000/100), each order has ~3 items (15000/5000)
    if let Some(count) = customer_idx_count {
        assert!(
            count > 0 && count <= 100,
            "Expected reasonable index range count for customer lookup, got {count}"
        );
    }
    if let Some(count) = order_idx_count {
        assert!(
            count > 0 && count <= 10,
            "Expected reasonable index range count for order lookup, got {count}"
        );
    }

    Ok(())
}

/// Stats collection from different scan types
#[test]
fn scan_type_stats_collection() -> anyhow::Result<()> {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();

    conn.execute("PRAGMA autoanalyze = 1")?;

    conn.execute(
        "CREATE TABLE t (
            id INTEGER PRIMARY KEY,
            a INTEGER,
            b TEXT,
            c REAL
        )",
    )?;
    conn.execute("CREATE INDEX idx_a ON t(a)")?;
    conn.execute("CREATE INDEX idx_b ON t(b)")?;
    conn.execute("CREATE INDEX idx_a_b ON t(a, b)")?;

    conn.execute("INSERT INTO t SELECT value, value % 100, 'text' || value, value * 0.1 FROM generate_series(1, 10000)")?;

    let stats = conn
        .auto_analyze_stats_snapshot()
        .expect("autoanalyze enabled");
    assert_eq!(stats.row_count("t"), None, "no stats before any queries");

    // Method 1: COUNT(*) - uses OP_Count for exact count
    let count: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM t");
    assert_eq!(count, vec![(10000,)]);

    let stats = conn
        .auto_analyze_stats_snapshot()
        .expect("autoanalyze enabled");
    assert_eq!(
        stats.row_count("t"),
        Some(10000),
        "COUNT(*) should set row count"
    );

    // INSERT should increment row count (not invalidate)
    conn.execute("INSERT INTO t VALUES (10001, 1, 'new', 0.1)")?;
    let stats = conn
        .auto_analyze_stats_snapshot()
        .expect("autoanalyze enabled");
    assert_eq!(
        stats.row_count("t"),
        Some(10001),
        "INSERT should increment row count"
    );

    // Method 2: Full table scan via SELECT * (no LIMIT - must complete scan to record stats)
    // Note: LIMIT queries stop early and don't record stats since the scan is incomplete
    let rows: Vec<(i64, i64, String, f64)> = conn.exec_rows("SELECT * FROM t");
    assert_eq!(rows.len(), 10001);

    let stats = conn
        .auto_analyze_stats_snapshot()
        .expect("autoanalyze enabled");
    assert_eq!(
        stats.row_count("t"),
        Some(10001),
        "full scan should set row count"
    );

    // DELETE should decrement row count (not invalidate)
    conn.execute("DELETE FROM t WHERE id = 10001")?;
    let stats = conn
        .auto_analyze_stats_snapshot()
        .expect("autoanalyze enabled");
    assert_eq!(
        stats.row_count("t"),
        Some(10000),
        "DELETE should decrement row count"
    );

    // Method 3: Covering index scan (ORDER BY on indexed column)
    let rows: Vec<(i64,)> = conn.exec_rows("SELECT a FROM t ORDER BY a");
    assert_eq!(rows.len(), 10000);

    let stats = conn
        .auto_analyze_stats_snapshot()
        .expect("autoanalyze enabled");
    assert_eq!(
        stats.row_count("t"),
        Some(10000),
        "covering index scan should set row count"
    );

    Ok(())
}

#[test]
/// Index range scan selectivity tracking
fn index_range_selectivity() -> anyhow::Result<()> {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();

    conn.execute("PRAGMA autoanalyze = 1")?;

    // Create table with known data distribution
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")?;
    conn.execute("CREATE INDEX idx_val ON t(val)")?;

    // Insert 1000 rows with val ranging from 1-100 (each value appears ~10 times)
    conn.execute("INSERT INTO t SELECT value, (value % 100) + 1 FROM generate_series(1, 1000)")?;

    // Get baseline row count
    let _: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM t");
    let stats = conn
        .auto_analyze_stats_snapshot()
        .expect("autoanalyze enabled");
    assert_eq!(stats.row_count("t"), Some(1000));

    // Point lookup: val = 50 should find ~10 rows
    let rows: Vec<(i64,)> = conn.exec_rows("SELECT id FROM t WHERE val = 50");
    assert_eq!(rows.len(), 10);

    let stats = conn
        .auto_analyze_stats_snapshot()
        .expect("autoanalyze enabled");
    let range_count = stats
        .index_range_row_count("idx_val")
        .expect("expected range scan count");
    assert!(
        (10..=11).contains(&range_count),
        "expected ~10 rows from point lookup, got {range_count}"
    );

    // Range lookup: val BETWEEN 10 AND 20 should find ~110 rows (11 distinct values * 10 rows each)
    let rows: Vec<(i64,)> = conn.exec_rows("SELECT id FROM t WHERE val >= 10 AND val <= 20");
    let len = rows.len();
    assert!(
        (100..=120).contains(&len),
        "expected ~110 rows from range lookup, got {len}"
    );

    let stats = conn
        .auto_analyze_stats_snapshot()
        .expect("autoanalyze enabled");
    let range_count = stats.index_range_row_count("idx_val");
    // Note: range scan tracking may overwrite previous stats
    // The count should reflect the most recent scan
    if let Some(count) = range_count {
        assert!(count > 0, "expected positive range scan count, got {count}");
    }

    Ok(())
}

///  Multi-table join optimization with progressive stats
#[test]
fn join_optimization() -> anyhow::Result<()> {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();

    conn.execute("PRAGMA autoanalyze = 1")?;

    // Create tables with dramatically different sizes
    conn.execute("CREATE TABLE tiny (id INTEGER PRIMARY KEY, val INTEGER)")?;
    conn.execute("CREATE TABLE small (id INTEGER PRIMARY KEY, tiny_id INTEGER)")?;
    conn.execute("CREATE TABLE medium (id INTEGER PRIMARY KEY, small_id INTEGER)")?;
    conn.execute("CREATE TABLE large (id INTEGER PRIMARY KEY, medium_id INTEGER)")?;

    // tiny: 5 rows, small: 50 rows, medium: 500 rows, large: 5000 rows
    conn.execute("INSERT INTO tiny SELECT value, value FROM generate_series(1, 5)")?;
    conn.execute("INSERT INTO small SELECT value, (value % 5) + 1 FROM generate_series(1, 50)")?;
    conn.execute("INSERT INTO medium SELECT value, (value % 50) + 1 FROM generate_series(1, 500)")?;
    conn.execute(
        "INSERT INTO large SELECT value, (value % 500) + 1 FROM generate_series(1, 5000)",
    )?;

    // Query: join all four tables
    let query = "SELECT t.val, s.id, m.id, l.id
                 FROM large l
                 JOIN medium m ON m.id = l.medium_id
                 JOIN small s ON s.id = m.small_id
                 JOIN tiny t ON t.id = s.tiny_id";

    // Without stats, plan follows FROM order (large first - suboptimal)
    let plan_before = extract_table_order(&limbo_exec_rows(
        &conn,
        &format!("EXPLAIN QUERY PLAN {query}"),
    ));

    // Collect stats for all tables
    let _: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM tiny");
    let _: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM small");
    let _: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM medium");
    let _: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM large");

    let stats = conn
        .auto_analyze_stats_snapshot()
        .expect("autoanalyze enabled");
    assert_eq!(stats.row_count("tiny"), Some(5));
    assert_eq!(stats.row_count("small"), Some(50));
    assert_eq!(stats.row_count("medium"), Some(500));
    assert_eq!(stats.row_count("large"), Some(5000));

    // With stats, optimizer should reorder: tiny -> small -> medium -> large
    let plan_after = extract_table_order(&limbo_exec_rows(
        &conn,
        &format!("EXPLAIN QUERY PLAN {query}"),
    ));

    // Verify tiny comes before large in the plan
    let tiny_pos = plan_after.iter().position(|t| t == "tiny");
    let large_pos = plan_after.iter().position(|t| t == "large");

    if let (Some(t), Some(l)) = (tiny_pos, large_pos) {
        assert!(
            t < l,
            "Expected tiny before large after stats, got plan: {plan_after:?}"
        );
    }

    // The plan order should change when we have stats
    // (either first table differs, or more tables reordered optimally)
    let first_before = plan_before.first();
    let first_after = plan_after.first();

    // With stats, the first table should not be 'large' anymore
    if first_before.map(String::as_str) == Some("large") {
        assert_ne!(
            first_after.map(String::as_str),
            Some("large"),
            "Optimizer should not start with largest table when stats available"
        );
    }

    Ok(())
}

///  Incremental row count updates on INSERT/DELETE
#[test]
fn incremental_row_count() -> anyhow::Result<()> {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();

    conn.execute("PRAGMA autoanalyze = 1")?;

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")?;
    conn.execute("CREATE INDEX idx_val ON t(val)")?;
    conn.execute("INSERT INTO t SELECT value, value FROM generate_series(1, 1000)")?;

    // Collect initial stats
    let _: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM t");
    let stats = conn
        .auto_analyze_stats_snapshot()
        .expect("autoanalyze enabled");
    assert_eq!(stats.row_count("t"), Some(1000));

    // INSERT increments row count
    conn.execute("INSERT INTO t VALUES (1001, 1001)")?;
    let stats = conn
        .auto_analyze_stats_snapshot()
        .expect("autoanalyze enabled");
    assert_eq!(
        stats.row_count("t"),
        Some(1001),
        "INSERT should increment row count"
    );

    // DELETE decrements row count
    conn.execute("DELETE FROM t WHERE id = 1001")?;
    let stats = conn
        .auto_analyze_stats_snapshot()
        .expect("autoanalyze enabled");
    assert_eq!(
        stats.row_count("t"),
        Some(1000),
        "DELETE should decrement row count"
    );

    // Multiple INSERTs accumulate
    conn.execute("INSERT INTO t VALUES (1001, 1001), (1002, 1002), (1003, 1003)")?;
    let stats = conn
        .auto_analyze_stats_snapshot()
        .expect("autoanalyze enabled");
    assert_eq!(
        stats.row_count("t"),
        Some(1003),
        "multiple INSERTs should accumulate"
    );

    // Multiple DELETEs accumulate
    conn.execute("DELETE FROM t WHERE id > 1000")?;
    let stats = conn
        .auto_analyze_stats_snapshot()
        .expect("autoanalyze enabled");
    assert_eq!(
        stats.row_count("t"),
        Some(1000),
        "multiple DELETEs should accumulate"
    );

    // UPDATE doesn't change row count (it's delete + insert of same row)
    conn.execute("UPDATE t SET val = val + 1 WHERE id = 1")?;
    let stats = conn
        .auto_analyze_stats_snapshot()
        .expect("autoanalyze enabled");
    assert_eq!(
        stats.row_count("t"),
        Some(1000),
        "UPDATE should not change row count"
    );

    Ok(())
}

/// Performance comparison with and without auto_analyze
/// Note: This test measures execution and demonstrates that stats collection
/// has minimal overhead while significantly improving query planning.
#[test]
fn performance_comparison() -> anyhow::Result<()> {
    // Test 1: Without auto_analyze
    let tmp_db1 = TempDatabase::new_empty();
    let conn1 = tmp_db1.connect_limbo();

    conn1.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER)")?;
    conn1.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)")?;
    conn1.execute("CREATE INDEX idx_customer ON orders(customer_id)")?;

    conn1.execute(
        "INSERT INTO customers SELECT value, 'Customer ' || value FROM generate_series(1, 100)",
    )?;
    conn1.execute(
        "INSERT INTO orders SELECT value, (value % 100) + 1 FROM generate_series(1, 10000)",
    )?;

    // Query without stats - suboptimal plan likely
    let query = "SELECT c.name, COUNT(*) FROM orders o JOIN customers c ON c.id = o.customer_id GROUP BY c.id";

    let start = Instant::now();
    let _: Vec<(String, i64)> = conn1.exec_rows(query);
    let without_stats_time = start.elapsed();

    // Test 2: With auto_analyze
    let tmp_db2 = TempDatabase::new_empty();
    let conn2 = tmp_db2.connect_limbo();

    conn2.execute("PRAGMA autoanalyze = 1")?;
    conn2.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER)")?;
    conn2.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)")?;
    conn2.execute("CREATE INDEX idx_customer ON orders(customer_id)")?;

    conn2.execute(
        "INSERT INTO customers SELECT value, 'Customer ' || value FROM generate_series(1, 100)",
    )?;
    conn2.execute(
        "INSERT INTO orders SELECT value, (value % 100) + 1 FROM generate_series(1, 10000)",
    )?;

    // Warm up stats with simple queries
    let _: Vec<(i64,)> = conn2.exec_rows("SELECT count(*) FROM orders");
    let _: Vec<(i64,)> = conn2.exec_rows("SELECT count(*) FROM customers");

    let stats = conn2
        .auto_analyze_stats_snapshot()
        .expect("autoanalyze enabled");
    assert_eq!(stats.row_count("orders"), Some(10000));
    assert_eq!(stats.row_count("customers"), Some(100));

    // Same query with stats - potentially better plan
    let start = Instant::now();
    let _: Vec<(String, i64)> = conn2.exec_rows(query);
    let with_stats_time = start.elapsed();

    // Compare query plans
    let plan1 = extract_table_order(&limbo_exec_rows(
        &conn1,
        &format!("EXPLAIN QUERY PLAN {query}"),
    ));
    let plan2 = extract_table_order(&limbo_exec_rows(
        &conn2,
        &format!("EXPLAIN QUERY PLAN {query}"),
    ));

    // With stats, customers (100 rows) should come before orders (10000 rows)
    let customers_pos = plan2.iter().position(|t| t == "customers");
    let orders_pos = plan2.iter().position(|t| t == "orders");

    if let (Some(c), Some(o)) = (customers_pos, orders_pos) {
        assert!(
            c < o,
            "With stats, customers should come before orders. Plan: {plan2:?}"
        );
    }

    assert!(
        with_stats_time <= without_stats_time,
        "With auto_analyze should be faster or equal in time"
    );
    // Log timing for manual inspection
    eprintln!("Without auto_analyze: {without_stats_time:?}");
    eprintln!("With auto_analyze: {with_stats_time:?}");
    eprintln!("Plan without stats: {plan1:?}");
    eprintln!("Plan with stats: {plan2:?}");

    Ok(())
}
