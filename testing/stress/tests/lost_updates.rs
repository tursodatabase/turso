#![cfg(shuttle)]

use shuttle::scheduler::RandomScheduler;
use shuttle::sync::Barrier;
use turso::Builder;
use turso_stress::sync::atomic::{AtomicI64, Ordering};
use turso_stress::sync::Arc;

fn shuttle_config() -> shuttle::Config {
    let mut config = shuttle::Config::default();
    config.stack_size *= 10;
    config.max_steps = shuttle::MaxSteps::FailAfter(10_000_000);
    config
}

async fn setup_mvcc_db(schema: &str) -> (turso::Database, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
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

async fn lost_updates_scenario(num_workers: usize, rounds: usize) {
    let (db, _dir) = setup_mvcc_db(
        "CREATE TABLE counter(id INTEGER PRIMARY KEY, val INTEGER);
         INSERT INTO counter VALUES(1, 0);",
    )
    .await;

    let total_committed = Arc::new(AtomicI64::new(0));

    for _round in 0..rounds {
        let barrier = Arc::new(Barrier::new(num_workers));
        let mut handles = Vec::new();

        for _ in 0..num_workers {
            let conn = db.connect().unwrap();
            let barrier = barrier.clone();
            let total_committed = total_committed.clone();
            handles.push(turso_stress::future::spawn(async move {
                barrier.wait();
                if conn.execute("BEGIN CONCURRENT", ()).await.is_err() {
                    return;
                }
                if conn
                    .execute("UPDATE counter SET val = val + 1 WHERE id = 1", ())
                    .await
                    .is_err()
                {
                    let _ = conn.execute("ROLLBACK", ()).await;
                    return;
                }
                match conn.execute("COMMIT", ()).await {
                    Ok(_) => {
                        total_committed.fetch_add(1, Ordering::SeqCst);
                    }
                    Err(_) => {
                        let _ = conn.execute("ROLLBACK", ()).await;
                    }
                }
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }
    }

    let conn = db.connect().unwrap();
    let val = query_i64(&conn, "SELECT val FROM counter WHERE id = 1").await;
    let committed = total_committed.load(Ordering::SeqCst);
    assert_eq!(
        val, committed,
        "Lost updates! counter={val} but {committed} transactions committed successfully"
    );
}

#[test]
fn shuttle_test_lost_updates() {
    let scheduler = RandomScheduler::new(100);
    let runner = shuttle::Runner::new(scheduler, shuttle_config());
    runner.run(|| shuttle::future::block_on(lost_updates_scenario(2, 3)));
}

#[test]
fn shuttle_test_lost_updates_slow() {
    let scheduler = RandomScheduler::new(10);
    let runner = shuttle::Runner::new(scheduler, shuttle_config());
    runner.run(|| shuttle::future::block_on(lost_updates_scenario(4, 20)));
}
