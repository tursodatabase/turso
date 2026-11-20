use super::test_db::MvccTestDbNoConn;
use turso_core::sync::Arc;

#[test]
fn test_mvcc_dual_cursor_transaction_isolation() {
    let res = tracing_subscriber::fmt::try_init();
    drop(res);
    let mut db = MvccTestDbNoConn::new_with_random_db();

    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 100)").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 200)").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    }

    db.restart();

    // Tx1: Update B-tree row
    let conn1 = db.connect();
    conn1.execute("BEGIN CONCURRENT").unwrap();
    conn1
        .execute("UPDATE t SET val = 999 WHERE id = 1")
        .unwrap();

    // Tx2: Should see old B-tree value, not Tx1's MVCC change
    let conn2 = db.connect();
    conn2.execute("BEGIN CONCURRENT").unwrap();
    let rows = get_rows(&conn2, "SELECT val FROM t WHERE id = 1");
    assert_eq!(rows[0][0].as_int().unwrap(), 100); // Original B-tree value

    conn1.execute("COMMIT").unwrap();
    conn2.execute("COMMIT").unwrap();
}

fn get_rows(conn: &Arc<turso_core::Connection>, sql: &str) -> Vec<Vec<turso_core::Value>> {
    let mut stmt = conn.prepare(sql).unwrap();
    let mut rows = Vec::new();
    stmt.run_with_row_callback(|row| {
        rows.push(row.get_values().cloned().collect());
        Ok(())
    })
    .unwrap();
    rows
}
