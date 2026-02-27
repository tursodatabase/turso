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
        .query("PRAGMA journal_mode = 'mvcc'", ())
        .await
        .unwrap();
    while let Ok(Some(_)) = rows.next().await {}
    drop(rows);
    if !schema.is_empty() {
        conn.execute_batch(schema).await.unwrap();
    }
    (db, dir)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_deadlock_join_during_writes() {
    let (db, _dir) = setup_mvcc_db(
        "CREATE TABLE orders(id INTEGER PRIMARY KEY, customer_id INTEGER, amount INTEGER);
         CREATE TABLE customers(id INTEGER PRIMARY KEY, name TEXT);
         INSERT INTO customers VALUES(1, 'alice');
         INSERT INTO customers VALUES(2, 'bob');
         INSERT INTO customers VALUES(3, 'charlie');",
    )
    .await;

    let done = Arc::new(AtomicBool::new(false));
    let mut handles = vec![];

    // Writers: insert orders for various customers
    for w in 0..4 {
        let db = db.clone();
        let done = done.clone();
        handles.push(tokio::spawn(async move {
            let conn = db.connect().unwrap();
            let mut i = 0u64;
            while !done.load(Ordering::Relaxed) {
                let id = (w as u64) * 100000 + i;
                let cust = (i % 3) + 1;
                let _ = conn.execute("BEGIN CONCURRENT", ()).await;
                let _ = conn
                    .execute(
                        &format!("INSERT INTO orders VALUES({}, {}, {})", id, cust, 10),
                        (),
                    )
                    .await;
                let _ = conn.execute("COMMIT", ()).await;
                i += 1;
            }
        }));
    }

    // Readers: do JOINs (THIS IS WHAT TRIGGERS THE HANG)
    for _ in 0..4 {
        let db = db.clone();
        let done = done.clone();
        handles.push(tokio::spawn(async move {
            let conn = db.connect().unwrap();
            while !done.load(Ordering::Relaxed) {
                let _ = conn.execute("BEGIN CONCURRENT", ()).await;
                let _orphans = match conn
                    .query(
                        "SELECT COUNT(*) FROM orders o LEFT JOIN customers c ON o.customer_id = c.id WHERE c.id IS NULL",
                        (),
                    )
                    .await
                {
                    Ok(mut rows) => match rows.next().await {
                        Ok(Some(row)) => row.get::<i64>(0).unwrap_or(0),
                        _ => 0,
                    },
                    Err(_) => 0,
                };
                let _ = conn.execute("COMMIT", ()).await;
            }
        }));
    }

    // If this test hangs here, the bug is confirmed.
    tokio::time::sleep(Duration::from_secs(3)).await;
    done.store(true, Ordering::Relaxed);
    for handle in handles {
        // This await will never return if threads are deadlocked
        handle.await.unwrap();
    }
}
