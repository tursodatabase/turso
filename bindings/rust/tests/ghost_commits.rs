use std::collections::HashSet;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::sync::Barrier;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_ghost_commits() {
    for run in 0..20 {
        let (db, _dir) = setup_mvcc_db("CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER)").await;

        let num_workers: usize = 8;
        let ops_per_worker: i64 = 100;
        let barrier = Arc::new(Barrier::new(num_workers));
        let mut handles = Vec::new();

        for worker_id in 0..num_workers as i64 {
            let conn = db.connect().unwrap();
            let barrier = barrier.clone();
            handles.push(tokio::spawn(async move {
                barrier.wait().await;
                let mut successful_ids: Vec<i64> = Vec::new();
                let mut error_ids: Vec<i64> = Vec::new();
                for i in 0..ops_per_worker {
                    let id = worker_id * 10_000 + i;
                    match conn
                        .execute(&format!("INSERT INTO t VALUES({id}, {i})"), ())
                        .await
                    {
                        Ok(_) => successful_ids.push(id),
                        Err(_) => error_ids.push(id),
                    }
                }
                (worker_id, successful_ids, error_ids)
            }));
        }

        let mut all_success_ids = HashSet::new();
        let mut all_error_ids = HashSet::new();
        for handle in handles {
            let (_wid, success_ids, error_ids) = handle.await.unwrap();
            for id in success_ids {
                all_success_ids.insert(id);
            }
            for id in error_ids {
                all_error_ids.insert(id);
            }
        }

        // Query all IDs from the database
        let conn = db.connect().unwrap();
        let mut db_ids = HashSet::new();
        let mut rows = conn
            .query("SELECT id FROM t ORDER BY id", ())
            .await
            .unwrap();
        while let Ok(Some(row)) = rows.next().await {
            let id: i64 = row.get(0).unwrap();
            db_ids.insert(id);
        }
        drop(rows);

        // Find ghost commits: IDs in DB that were reported as errors
        let ghost_ids: Vec<i64> = db_ids.intersection(&all_error_ids).copied().collect();
        // Find missing: IDs reported as success but not in DB
        let missing_ids: Vec<i64> = all_success_ids.difference(&db_ids).copied().collect();

        if !ghost_ids.is_empty() || !missing_ids.is_empty() {
            eprintln!("run={run}: Ghost commit IDs: {ghost_ids:?}");
            eprintln!("run={run}: Missing IDs: {missing_ids:?}");

            // Identify which workers the ghost commits belong to
            for &id in &ghost_ids {
                let worker = id / 10_000;
                let op = id % 10_000;
                eprintln!("  Ghost: worker={worker} op={op} id={id}");
            }

            panic!(
                "Run {}: {} ghost commits, {} missing commits",
                run,
                ghost_ids.len(),
                missing_ids.len()
            );
        }
        eprint!(
            "run={} ok={} err={} rows={} ",
            run,
            all_success_ids.len(),
            all_error_ids.len(),
            db_ids.len()
        );
    }
    eprintln!();
}
