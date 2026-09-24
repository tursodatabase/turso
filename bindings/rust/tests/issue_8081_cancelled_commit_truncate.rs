use tempfile::tempdir;
use turso::Builder;

async fn setup_mvcc_db() -> (turso::Database, tempfile::TempDir) {
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
    (db, dir)
}

async fn truncate_checkpoint(conn: &turso::Connection) {
    let mut rows = conn
        .query("PRAGMA wal_checkpoint(TRUNCATE)", ())
        .await
        .unwrap();
    while rows.next().await.unwrap().is_some() {}
}

async fn query_i64(conn: &turso::Connection, sql: &str) -> i64 {
    let mut rows = conn.query(sql, ()).await.unwrap();
    let row = rows.next().await.unwrap().unwrap();
    row.get::<i64>(0).unwrap()
}

#[tokio::test]
async fn test_mvcc_dropped_published_commit_survives_truncate_restart() {
    const ROW_COUNT: i64 = 1_500;
    const MAX_CANCEL_YIELDS: usize = 32;

    for cancel_after in 1..=MAX_CANCEL_YIELDS {
        let (db, dir) = setup_mvcc_db().await;
        let path = dir.path().join("test.db").to_str().unwrap().to_string();
        let conn1 = db.connect().unwrap();
        conn1
            .execute("PRAGMA mvcc_checkpoint_threshold = -1", ())
            .await
            .unwrap();
        conn1
            .execute("PRAGMA mvcc_gc_threshold = -1", ())
            .await
            .unwrap();
        conn1
            .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, value TEXT)", ())
            .await
            .unwrap();
        truncate_checkpoint(&conn1).await;

        let conn2 = db.connect().unwrap();
        conn2.execute("BEGIN CONCURRENT", ()).await.unwrap();
        conn2
            .execute(
                &format!(
                    "INSERT INTO t SELECT value, 'published' FROM generate_series(1, {ROW_COUNT})"
                ),
                (),
            )
            .await
            .unwrap();

        let completed = tokio::select! {
            biased;
            result = conn2.execute("COMMIT", ()) => {
                result.unwrap();
                true
            }
            _ = async {
                for _ in 0..cancel_after {
                    tokio::task::yield_now().await;
                }
            } => false,
        };

        let before_restart = query_i64(&conn1, "SELECT count(*) FROM t").await;
        assert!(
            before_restart == 0 || before_restart == ROW_COUNT,
            "cancelling COMMIT must not expose a partial transaction"
        );
        if completed || before_restart == 0 {
            continue;
        }

        truncate_checkpoint(&conn1).await;
        drop(conn2);
        drop(conn1);
        drop(db);

        let reopened = Builder::new_local(&path).build().await.unwrap();
        let conn3 = reopened.connect().unwrap();
        let after_restart = query_i64(&conn3, "SELECT count(*) FROM t").await;
        assert_eq!(
            after_restart, ROW_COUNT,
            "a published commit cancelled after scheduler yield {cancel_after} must survive TRUNCATE and restart"
        );
        return;
    }

    panic!("no public COMMIT cancellation cut reached the published rewrite window");
}
