use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use turso_core::ConnectOpts;

use super::common::{ExecRows, TempDatabase};

#[test]
fn concurrent_view_expansion_is_not_spuriously_circular() {
    let tmp_db = TempDatabase::builder().with_views(true).build();
    {
        let conn = tmp_db.connect_limbo();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .unwrap();
        conn.execute(
            "CREATE VIEW v AS SELECT id FROM t a \
             WHERE NOT EXISTS (SELECT 1 FROM t b WHERE b.id = a.id + 1)",
        )
        .unwrap();
    }

    let db = tmp_db.db.clone();
    let failed = Arc::new(AtomicBool::new(false));
    let handles: Vec<_> = (0..4)
        .map(|_| {
            let db = db.clone();
            let failed = failed.clone();
            std::thread::spawn(move || {
                let conn = db.connect(ConnectOpts::new()).unwrap();
                for _ in 0..5000 {
                    if let Err(e) = conn.prepare("SELECT id FROM v") {
                        assert!(
                            e.to_string().contains("circularly defined"),
                            "unexpected prepare error: {e}"
                        );
                        failed.store(true, Ordering::Relaxed);
                        return;
                    }
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    assert!(
        !failed.load(Ordering::Relaxed),
        "concurrent view expansion produced a spurious 'circularly defined' error"
    );
}

#[test]
fn create_materialized_view_is_rejected_in_mvcc_mode() {
    let tmp_db = TempDatabase::builder()
        .with_views(true)
        .with_mvcc(true)
        .build();
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INT)")
        .unwrap();

    let err = conn
        .execute("CREATE MATERIALIZED VIEW mv AS SELECT v, count(*) c FROM t GROUP BY v")
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("Materialized views are not supported in MVCC mode"),
        "unexpected error: {err}"
    );

    conn.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    let rows: Vec<(i64, i64)> = conn.exec_rows("SELECT id, v FROM t");
    assert_eq!(rows, vec![(1, 10)]);
}

#[test]
fn view_columns_are_worked_out_after_the_whole_schema_loads() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("view_columns_reopen.db");
    {
        let db = TempDatabase::new_with_existent(&path);
        let conn = db.connect_limbo();
        conn.execute("CREATE TABLE t(a, b)").unwrap();
        conn.execute("CREATE VIEW v AS SELECT * FROM t").unwrap();
        conn.execute("CREATE VIEW v2 AS SELECT * FROM v").unwrap();
        conn.execute("CREATE TABLE u(a, b)").unwrap();
        conn.execute("CREATE VIEW w AS SELECT * FROM u").unwrap();
        conn.execute("DROP TABLE t").unwrap();
        // The row of `u` now comes after the row of `w`.
        conn.execute("DROP TABLE u").unwrap();
        conn.execute("CREATE TABLE u(x, y, z)").unwrap();
        conn.close().unwrap();
    }

    let db = TempDatabase::new_with_existent(&path);
    let conn = db.connect_limbo();
    let views: Vec<(String, i64)> = conn
        .exec_rows("SELECT name, ncol FROM pragma_table_list WHERE type = 'view' ORDER BY name");
    assert_eq!(
        views,
        vec![("v".into(), 0), ("v2".into(), 0), ("w".into(), 3)]
    );
    let columns: Vec<(String,)> = conn.exec_rows("SELECT name FROM pragma_table_info('w')");
    assert_eq!(columns, vec![("x".into(),), ("y".into(),), ("z".into(),)]);
}

#[test]
fn views_over_a_dropped_table_have_no_columns_after_reopen() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("views_over_dropped_table.db");
    {
        let db = TempDatabase::new_with_existent(&path);
        let conn = db.connect_limbo();
        conn.execute("CREATE TABLE t(a, b)").unwrap();
        conn.execute("CREATE VIEW v AS SELECT * FROM t").unwrap();
        conn.execute("CREATE VIEW w AS SELECT 1 FROM t").unwrap();
        conn.execute("DROP TABLE t").unwrap();
        conn.close().unwrap();
    }

    let db = TempDatabase::new_with_existent(&path);
    let conn = db.connect_limbo();
    let views: Vec<(String, i64)> = conn
        .exec_rows("SELECT name, ncol FROM pragma_table_list WHERE type = 'view' ORDER BY name");
    assert_eq!(views, vec![("v".into(), 0), ("w".into(), 0)]);
}

#[test]
fn view_columns_are_worked_out_after_an_mvcc_database_is_reopened() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("view_columns_reparse.db");
    {
        let db = TempDatabase::builder()
            .with_db_path(&path)
            .with_mvcc(true)
            .build();
        let conn = db.connect_limbo();
        conn.execute("CREATE TABLE t(a, b)").unwrap();
        conn.execute("CREATE VIEW v AS SELECT * FROM t").unwrap();
        conn.execute("CREATE VIEW v2 AS SELECT * FROM v").unwrap();
        conn.execute("DROP TABLE t").unwrap();
        // The row of `t` now comes after the rows of `v` and `v2`.
        conn.execute("CREATE TABLE t(x, y, z)").unwrap();
        conn.close().unwrap();
    }

    // Opening an MVCC database builds the schema from the recovered
    // sqlite_schema rows.
    let db = TempDatabase::builder()
        .with_db_path(&path)
        .with_mvcc(true)
        .build();
    let conn = db.connect_limbo();
    let views: Vec<(String, i64)> = conn
        .exec_rows("SELECT name, ncol FROM pragma_table_list WHERE type = 'view' ORDER BY name");
    assert_eq!(views, vec![("v".into(), 3), ("v2".into(), 3)]);
}
