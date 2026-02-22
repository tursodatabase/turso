use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use turso::Builder;

async fn setup_mvcc_db(schema: &str) -> (turso::Database, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let db = Builder::new_local(db_path.to_str().unwrap())
        .build()
        .await
        .unwrap();
    let conn = db.connect().unwrap();
    let mut rows = conn
        .query("PRAGMA journal_mode = 'experimental_mvcc'", ())
        .await
        .unwrap();
    while let Ok(Some(_)) = rows.next().await {}
    drop(rows);
    if !schema.is_empty() {
        conn.execute_batch(schema).await.unwrap();
    }
    (db, dir)
}

async fn query_i64(conn: &turso::Connection, sql: &str) -> i64 {
    let mut rows = conn.query(sql, ()).await.unwrap();
    let row = rows.next().await.unwrap().unwrap();
    row.get::<i64>(0).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_snapshot_isolation_violation() {
    let (db, _dir) = setup_mvcc_db("CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER)").await;

    let done = Arc::new(AtomicBool::new(false));
    let violation_found = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::new();

    // 4 writers: continuously insert batches of 5 rows
    for w in 0..4i64 {
        let conn = db.connect().unwrap();
        let done = done.clone();
        handles.push(tokio::spawn(async move {
            let mut i = 0i64;
            while !done.load(Ordering::Relaxed) {
                if conn.execute("BEGIN CONCURRENT", ()).await.is_err() {
                    continue;
                }
                let mut ok = true;
                for j in 0..5i64 {
                    let id = w * 100_000 + i * 5 + j;
                    if conn
                        .execute(&format!("INSERT INTO t VALUES({id}, {id})"), ())
                        .await
                        .is_err()
                    {
                        ok = false;
                        break;
                    }
                }
                if ok {
                    if conn.execute("COMMIT", ()).await.is_err() {
                        let _ = conn.execute("ROLLBACK", ()).await;
                    }
                } else {
                    let _ = conn.execute("ROLLBACK", ()).await;
                }
                i += 1;
            }
        }));
    }

    // 4 readers: open snapshot, read COUNT(*) twice, assert they match
    for _ in 0..4 {
        let conn = db.connect().unwrap();
        let done = done.clone();
        let violation_found = violation_found.clone();
        handles.push(tokio::spawn(async move {
            while !done.load(Ordering::Relaxed) {
                if conn.execute("BEGIN CONCURRENT", ()).await.is_err() {
                    continue;
                }
                let count1 = query_i64(&conn, "SELECT COUNT(*) FROM t").await;
                tokio::task::yield_now().await; // Let writers commit between reads
                let count2 = query_i64(&conn, "SELECT COUNT(*) FROM t").await;
                let _ = conn.execute("COMMIT", ()).await;
                if count1 != count2 {
                    violation_found.store(true, Ordering::Relaxed);
                    eprintln!(
                        "VIOLATION: COUNT changed {} -> {} within same txn (delta={})",
                        count1,
                        count2,
                        count2 - count1
                    );
                }
            }
        }));
    }

    tokio::time::sleep(Duration::from_secs(3)).await;
    done.store(true, Ordering::Relaxed);
    for handle in handles {
        let _ = handle.await;
    }

    assert!(
        !violation_found.load(Ordering::Relaxed),
        "Snapshot isolation violated: COUNT(*) changed within a single BEGIN CONCURRENT txn"
    );
}
