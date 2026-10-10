use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

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
                let conn = db.connect().unwrap();
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
