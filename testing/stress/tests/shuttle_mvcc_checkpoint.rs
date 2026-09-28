#![cfg(shuttle)]

use shuttle::scheduler::PctScheduler;
use shuttle::sync::Barrier;
use turso::Builder;
use turso_stress::sync::Arc;

fn shuttle_config() -> shuttle::Config {
    turso_stress::shuttle_config()
}

async fn query_i64s(conn: &turso::Connection, sql: &str) -> Vec<i64> {
    let mut rows = conn.query(sql, ()).await.unwrap();
    let mut values = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        values.push(row.get::<i64>(0).unwrap());
    }
    values
}

async fn truncate_checkpoint(conn: &turso::Connection) -> turso::Result<bool> {
    let mut rows = conn.query("PRAGMA wal_checkpoint(TRUNCATE)", ()).await?;
    let row = rows
        .next()
        .await?
        .expect("wal_checkpoint must return its status row");
    let succeeded = row.get::<i64>(0)? == 0;
    assert!(
        rows.next().await?.is_none(),
        "wal_checkpoint must return exactly one status row"
    );
    Ok(succeeded)
}

async fn passive_truncate_late_commit_scenario() -> bool {
    const CHECKPOINT_ROWS: i64 = 256;
    const LATE_WRITES: i64 = 128;

    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let path = db_path.to_str().unwrap();
    let db = Builder::new_local(path)
        .experimental_mvcc_passive_checkpoint(true)
        .build()
        .await
        .unwrap();
    let setup = db.connect().unwrap();
    let mut rows = setup
        .query("PRAGMA journal_mode = 'experimental_mvcc'", ())
        .await
        .unwrap();
    while rows.next().await.unwrap().is_some() {}
    drop(rows);

    setup
        .execute("PRAGMA mvcc_checkpoint_threshold = -1", ())
        .await
        .unwrap();
    setup
        .execute_batch(
            "CREATE TABLE t(id INTEGER PRIMARY KEY, value TEXT);
             INSERT INTO t VALUES(1, 'baseline');",
        )
        .await
        .unwrap();
    assert!(
        truncate_checkpoint(&setup).await.unwrap(),
        "baseline TRUNCATE must succeed"
    );

    use std::fmt::Write as _;
    let mut checkpoint_work = String::from("BEGIN CONCURRENT;");
    for id in 2..=CHECKPOINT_ROWS + 1 {
        write!(
            checkpoint_work,
            "INSERT INTO t VALUES({id}, 'checkpoint-target-{id}');"
        )
        .unwrap();
    }
    checkpoint_work.push_str("COMMIT;");
    setup.execute_batch(&checkpoint_work).await.unwrap();

    let start = Arc::new(Barrier::new(2));
    let checkpoint_conn = db.connect().unwrap();
    let checkpoint_start = start.clone();
    let checkpoint = turso_stress::future::spawn(async move {
        checkpoint_start.wait();
        truncate_checkpoint(&checkpoint_conn).await
    });

    let writer_conn = db.connect().unwrap();
    let writer = turso_stress::future::spawn(async move {
        start.wait();
        let mut acknowledged = Vec::new();
        for id in CHECKPOINT_ROWS + 2..CHECKPOINT_ROWS + 2 + LATE_WRITES {
            match writer_conn
                .execute(
                    &format!("INSERT INTO t VALUES({id}, 'after-snapshot-{id}')"),
                    (),
                )
                .await
            {
                Ok(_) => acknowledged.push(id),
                Err(turso::Error::Busy(_) | turso::Error::BusySnapshot(_)) => {}
                Err(err) => panic!("late writer failed unexpectedly: {err:?}"),
            }
        }
        acknowledged
    });

    let checkpoint_succeeded = match checkpoint.await.unwrap() {
        Ok(succeeded) => succeeded,
        Err(turso::Error::Busy(_) | turso::Error::BusySnapshot(_)) => false,
        Err(err) => panic!("checkpoint failed unexpectedly: {err:?}"),
    };
    let acknowledged = writer.await.unwrap();
    assert!(
        !acknowledged.is_empty(),
        "writer must commit at least one row"
    );

    let mut expected_ids = (1..=CHECKPOINT_ROWS + 1).collect::<Vec<_>>();
    expected_ids.extend(acknowledged);
    assert_eq!(
        query_i64s(&setup, "SELECT id FROM t ORDER BY id").await,
        expected_ids,
        "every acknowledged row must be visible before restart"
    );

    drop(setup);
    drop(db);

    let reopened = Builder::new_local(path)
        .experimental_mvcc_passive_checkpoint(true)
        .build()
        .await
        .unwrap();
    let reopened_conn = reopened.connect().unwrap();
    assert_eq!(
        query_i64s(&reopened_conn, "SELECT id FROM t ORDER BY id").await,
        expected_ids,
        "TRUNCATE discarded an acknowledged concurrent commit \
         (checkpoint succeeded: {checkpoint_succeeded})"
    );

    checkpoint_succeeded
}

#[test]
fn shuttle_test_passive_truncate_preserves_late_commit() {
    let scheduler = PctScheduler::new(3, 100);
    let runner = shuttle::Runner::new(scheduler, shuttle_config());
    let saw_successful_checkpoint = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let completed = saw_successful_checkpoint.clone();
    runner.run(move || {
        if shuttle::future::block_on(passive_truncate_late_commit_scenario()) {
            completed.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    });
    assert!(
        saw_successful_checkpoint.load(std::sync::atomic::Ordering::SeqCst),
        "at least one explored schedule must complete TRUNCATE"
    );
}
