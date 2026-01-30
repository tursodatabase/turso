use turso_core::{Result, Value};

use crate::common::TempDatabase;

/// Test that SchemaUpdated error reprepares the statement.
///
/// Scenario:
/// 1. Connection 1 starts a transaction and prepares a SELECT statement
/// 2. Connection 2 changes the schema (ALTER TABLE)
/// 3. Connection 1 tries to execute the prepared statement - gets SchemaUpdated
/// 4. Verify the transaction is still active (can execute other statements)
/// 5. Verify the statement can be retried and will succeed after reprepare
#[turso_macros::test]
fn test_schema_update_reprepares_statement(tmp_db: TempDatabase) -> Result<()> {
    let conn1 = tmp_db.connect_limbo();
    let conn2 = tmp_db.connect_limbo();

    // Create initial table
    conn1.execute("CREATE TABLE t (a INTEGER, b TEXT)")?;
    conn1.execute("INSERT INTO t VALUES (1, 'first'), (2, 'second')")?;

    // Connection 1 starts a transaction
    conn1.execute("BEGIN")?;

    // Prepare a SELECT statement (this captures the schema cookie at prepare time)
    let mut stmt = conn1.prepare("SELECT a, b FROM t WHERE a = 1")?;

    // Connection 2 changes the schema (this increments the schema cookie)
    conn2.execute("ALTER TABLE t ADD COLUMN c INTEGER")?;

    // Connection 1 tries to execute the prepared statement
    // Note: Statement::step() will automatically reprepare and retry on SchemaUpdated
    // for statements that access the database. However, we can still verify that
    // the transaction is not rolled back even if SchemaUpdated occurs.
    // For this test, we'll use a statement that should trigger SchemaUpdated
    // but the automatic reprepare should handle it.

    // First, let's verify the statement can execute (it will be automatically reprepared)
    let mut found_row = false;
    stmt.run_with_row_callback(|row| {
        let a = row.get::<&Value>(0).unwrap();
        let b = row.get::<&Value>(1).unwrap();
        assert_eq!(*a, Value::Integer(1));
        assert_eq!(*b, Value::build_text("first"));
        found_row = true;
        Ok(())
    })?;
    assert!(found_row, "Expected to find a row");

    // Verify the transaction is still active by executing another statement
    conn1.execute("INSERT INTO t (a, b) VALUES (3, 'third')")?;

    // Verify we can still query within the transaction
    let mut stmt2 = conn1.prepare("SELECT COUNT(*) FROM t")?;
    stmt2.run_with_row_callback(|row| {
        let count = row.get::<&Value>(0).unwrap();
        assert_eq!(*count, Value::Integer(3));
        Ok(())
    })?;

    // Commit the transaction
    conn1.execute("COMMIT")?;

    // Verify all changes are committed
    let mut stmt3 = conn1.prepare("SELECT COUNT(*) FROM t")?;
    stmt3.run_with_row_callback(|row| {
        let count = row.get::<&Value>(0).unwrap();
        assert_eq!(*count, Value::Integer(3));
        Ok(())
    })?;

    Ok(())
}

/// Test that deferred_seeks vector is properly resized when a statement is reprepared
/// with a larger cursor count due to schema changes (e.g., new index creation).
///
/// This is a regression test for the bug where ProgramState::reset() didn't resize
/// the deferred_seeks vector when resizing cursors and cursor_seqs, causing an
/// index out of bounds panic in op_column when accessing deferred_seeks with a
/// cursor_id larger than the original allocation.
///
/// Scenario:
/// 1. Create tables with data
/// 2. Prepare a JOIN query (multiple cursors)
/// 3. Another connection creates indexes on the tables (schema change)
/// 4. Execute the prepared statement - triggers reprepare
/// 5. The reprepared query uses the new indexes with deferred seeks
/// 6. If deferred_seeks wasn't resized, accessing a higher cursor_id would panic
#[turso_macros::test]
fn test_deferred_seeks_resize_on_reprepare(tmp_db: TempDatabase) -> Result<()> {
    let conn1 = tmp_db.connect_limbo();
    let conn2 = tmp_db.connect_limbo();

    // Create two tables to JOIN - this gives us multiple table cursors
    conn1.execute(
        "CREATE TABLE orders (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            total REAL
        )",
    )?;

    conn1.execute(
        "CREATE TABLE customers (
            customer_id INTEGER PRIMARY KEY,
            name TEXT,
            email TEXT
        )",
    )?;

    // Insert test data
    conn1.execute(
        "INSERT INTO customers VALUES
            (1, 'Alice', 'alice@example.com'),
            (2, 'Bob', 'bob@example.com'),
            (3, 'Charlie', 'charlie@example.com')",
    )?;

    conn1.execute(
        "INSERT INTO orders VALUES
            (100, 1, 150.00),
            (101, 2, 250.00),
            (102, 1, 75.00),
            (103, 3, 500.00)",
    )?;

    // Prepare a JOIN query that selects columns from both tables
    // Initially no indexes on customer_id, so it's a nested loop scan
    // This uses 2 cursors (one for each table)
    let mut stmt = conn1.prepare(
        "SELECT c.name, o.total FROM orders o
         JOIN customers c ON o.customer_id = c.customer_id
         WHERE o.total > 100.0",
    )?;

    // Create indexes on both tables (schema change) - causes reprepare
    // After reprepare, the optimizer may use these indexes, requiring more cursors
    // and potentially using deferred seeks
    conn2.execute("CREATE INDEX idx_orders_customer ON orders(customer_id)")?;
    conn2.execute("CREATE INDEX idx_orders_total ON orders(total)")?;

    // Execute the statement - triggers reprepare with potentially more cursors
    // Without the fix, this could panic with index out of bounds on deferred_seeks
    let mut results = Vec::new();
    stmt.run_with_row_callback(|row| {
        let name: String = row.get(0)?;
        let total: f64 = row.get(1)?;
        results.push((name, total));
        Ok(())
    })?;

    // Verify we got the expected results (orders with total > 100.0)
    assert_eq!(results.len(), 3);

    Ok(())
}
