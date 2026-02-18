use crate::common::{limbo_exec_rows, TempDatabase};
use std::sync::Arc;
use turso_core::LimboError;

fn setup_fk_tables(conn: &Arc<turso_core::Connection>) {
    conn.execute("PRAGMA foreign_keys=ON;").unwrap();
    conn.execute("CREATE TABLE parent (id INT PRIMARY KEY);")
        .unwrap();
    conn.execute("CREATE TABLE child (id INT, pid INT REFERENCES parent(id));")
        .unwrap();
}

/// FK violation in an explicit transaction should only rollback the failing
/// statement. The transaction remains open and committable.
#[test]
fn test_fk_violation_in_explicit_txn_continues() {
    let tmp_db = TempDatabase::builder().build();
    let conn = tmp_db.connect_limbo();

    setup_fk_tables(&conn);

    conn.execute("BEGIN;").unwrap();
    conn.execute("INSERT INTO parent VALUES (2);")
        .unwrap();

    // FK violation - this statement should be rolled back but the txn stays open
    let err = conn
        .execute("INSERT INTO child VALUES (999, 999);")
        .unwrap_err();
    assert!(
        matches!(err, LimboError::ForeignKeyConstraint(_)),
        "expected FK constraint error, got: {err:?}"
    );

    // Transaction should still be usable
    conn.execute("INSERT INTO parent VALUES (3);")
        .unwrap();
    conn.execute("COMMIT;").unwrap();

    let rows = limbo_exec_rows(&conn, "SELECT id FROM parent ORDER BY id;");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], rusqlite::types::Value::Integer(2));
    assert_eq!(rows[1][0], rusqlite::types::Value::Integer(3));
}

/// FK violation in autocommit mode should rollback the implicit transaction.
#[test]
fn test_fk_violation_autocommit_rollback() {
    let tmp_db = TempDatabase::builder().build();
    let conn = tmp_db.connect_limbo();

    setup_fk_tables(&conn);

    conn.execute("INSERT INTO parent VALUES (1);")
        .unwrap();

    let err = conn
        .execute("INSERT INTO child VALUES (999, 999);")
        .unwrap_err();
    assert!(matches!(err, LimboError::ForeignKeyConstraint(_)));

    // Parent row (1) should still exist since it was committed in a separate autocommit txn
    let rows = limbo_exec_rows(&conn, "SELECT id FROM parent ORDER BY id;");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], rusqlite::types::Value::Integer(1));
}

/// After an FK violation and subsequent successful transaction, the database
/// should be in a clean state with no WAL lock leaks.
#[test]
fn test_fk_violation_no_wal_lock_leak() {
    let tmp_db = TempDatabase::builder().build();
    let conn = tmp_db.connect_limbo();

    setup_fk_tables(&conn);

    // Cause FK violation in autocommit mode
    let _ = conn.execute("INSERT INTO child VALUES (999, 999);");

    // Should be able to do normal operations after the FK violation
    conn.execute("INSERT INTO parent VALUES (1);")
        .unwrap();

    let rows = limbo_exec_rows(&conn, "SELECT id FROM parent ORDER BY id;");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], rusqlite::types::Value::Integer(1));

    // Connection close should succeed without WAL checkpoint errors.
    // The close is implicit via drop, but we can verify by doing a checkpoint.
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
        .unwrap();
}
