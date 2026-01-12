use tempfile::TempDir;

use crate::common::{ExecRows, TempDatabase};

#[turso_macros::test()]
fn test_snapshot_basic(db: TempDatabase) {
    let conn = db.connect_limbo();

    // Create table and insert data
    conn.execute("CREATE TABLE t(x INTEGER PRIMARY KEY, y TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'hello'), (2, 'world')")
        .unwrap();

    // Create snapshot
    let temp_dir = TempDir::new().unwrap();
    let snapshot_path = temp_dir.path().join("snapshot.db");

    conn.snapshot(snapshot_path.to_str().unwrap()).unwrap();

    // Verify snapshot exists
    assert!(snapshot_path.exists());

    // Open snapshot and verify data
    let snapshot_db = TempDatabase::new_with_existent(&snapshot_path);
    let snapshot_conn = snapshot_db.connect_limbo();
    let rows: Vec<(i64, String)> = snapshot_conn.exec_rows("SELECT x, y FROM t ORDER BY x");
    assert_eq!(
        rows,
        vec![(1, "hello".to_string()), (2, "world".to_string())]
    );
}

#[turso_macros::test()]
fn test_snapshot_with_pending_wal_data(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t(x INTEGER PRIMARY KEY, y BLOB)")
        .unwrap();

    // Insert enough data to create multiple WAL frames
    for i in 0..100 {
        conn.execute(format!("INSERT INTO t VALUES ({i}, randomblob(1000))"))
            .unwrap();
    }

    // Verify WAL has frames before snapshot
    let wal_state = conn.wal_state().unwrap();
    assert!(
        wal_state.max_frame > 0,
        "WAL should have frames before snapshot"
    );

    // Create snapshot
    let temp_dir = TempDir::new().unwrap();
    let snapshot_path = temp_dir.path().join("snapshot.db");

    conn.snapshot(snapshot_path.to_str().unwrap()).unwrap();

    // Verify all data is in snapshot
    let snapshot_db = TempDatabase::new_with_existent(&snapshot_path);
    let snapshot_conn = snapshot_db.connect_limbo();
    let count: Vec<(i64,)> = snapshot_conn.exec_rows("SELECT COUNT(*) FROM t");
    assert_eq!(count[0].0, 100);
}
