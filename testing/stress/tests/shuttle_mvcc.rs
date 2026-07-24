#![cfg(shuttle)]

use shuttle::scheduler::{PctScheduler, RandomScheduler};
use shuttle::sync::Barrier;
use std::future::Future;
use turso::Builder;
use turso_stress::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use turso_stress::sync::Arc;

fn shuttle_config() -> shuttle::Config {
    turso_stress::shuttle_config()
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

async fn query_string(conn: &turso::Connection, sql: &str) -> String {
    let mut rows = conn.query(sql, ()).await.unwrap();
    let row = rows.next().await.unwrap().unwrap();
    row.get::<String>(0).unwrap()
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
                conn.execute("BEGIN CONCURRENT", ()).await.unwrap();
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
    runner.run(|| shuttle::future::block_on(lost_updates_scenario(4, 6)));
}

#[test]
fn shuttle_test_lost_updates_slow() {
    let scheduler = RandomScheduler::new(10);
    let runner = shuttle::Runner::new(scheduler, shuttle_config());
    runner.run(|| shuttle::future::block_on(lost_updates_scenario(12, 60)));
}

async fn snapshot_isolation_scenario(num_writers: usize, num_readers: usize, writer_rounds: i64) {
    let (db, _dir) = setup_mvcc_db("CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER)").await;

    let done = Arc::new(AtomicBool::new(false));
    let violation_count = Arc::new(AtomicI64::new(0));
    let mut handles = Vec::new();

    // Writers: continuously insert batches of 5 rows
    for w in 0..num_writers as i64 {
        let conn = db.connect().unwrap();
        let done = done.clone();
        handles.push(turso_stress::future::spawn(async move {
            let mut i = 0i64;
            while !done.load(Ordering::Relaxed) && i < writer_rounds {
                conn.execute("BEGIN CONCURRENT", ()).await.unwrap();
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

    // Readers: open snapshot, read COUNT(*) twice, assert they match
    for _ in 0..num_readers {
        let conn = db.connect().unwrap();
        let done = done.clone();
        let violation_count = violation_count.clone();
        handles.push(turso_stress::future::spawn(async move {
            let mut i = 0;
            while !done.load(Ordering::Relaxed) && i < writer_rounds {
                conn.execute("BEGIN CONCURRENT", ())
                    .await
                    .unwrap();
                let count1 = query_i64(&conn, "SELECT COUNT(*) FROM t").await;
                shuttle::future::yield_now().await; // Let writers commit between reads
                let count2 = query_i64(&conn, "SELECT COUNT(*) FROM t").await;
                conn.execute("COMMIT", ()).await.unwrap();
                if count1 != count2 {
                    let n = violation_count.fetch_add(1, Ordering::Relaxed) + 1;
                    eprintln!(
                        "VIOLATION #{n}: COUNT changed {count1} -> {count2} within same txn (delta={})",
                        count2 - count1
                    );
                }
                i += 1;
            }
        }));
    }

    for handle in handles {
        let _ = handle.await;
    }

    let violations = violation_count.load(Ordering::Relaxed);
    assert_eq!(
        violations, 0,
        "Snapshot isolation violated: COUNT(*) changed within a BEGIN CONCURRENT txn ({violations} violations)"
    );
}

#[test]
fn shuttle_test_snapshot_isolation_violation() {
    let scheduler = RandomScheduler::new(100);
    let runner = shuttle::Runner::new(scheduler, shuttle_config());
    runner.run(|| shuttle::future::block_on(snapshot_isolation_scenario(4, 4, 6)));
}

#[test]
fn shuttle_test_snapshot_isolation_violation_slow() {
    let scheduler = RandomScheduler::new(10);
    let runner = shuttle::Runner::new(scheduler, shuttle_config());
    runner.run(|| shuttle::future::block_on(snapshot_isolation_scenario(12, 12, 15)));
}

async fn ghost_commits_scenario(num_workers: usize, ops_per_worker: i64) {
    let (db, _dir) = setup_mvcc_db("CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER)").await;

    let barrier = Arc::new(Barrier::new(num_workers));
    let mut handles = Vec::new();

    for worker_id in 0..num_workers as i64 {
        let conn = db.connect().unwrap();
        let barrier = barrier.clone();
        handles.push(turso_stress::future::spawn(async move {
            barrier.wait();
            let mut successes = 0i64;
            let mut errors = 0i64;
            for i in 0..ops_per_worker {
                let id = worker_id * 10_000 + i;
                // Autocommit INSERT (no explicit BEGIN/COMMIT)
                match conn
                    .execute(&format!("INSERT INTO t VALUES({id}, {i})"), ())
                    .await
                {
                    Ok(_) => successes += 1,
                    Err(turso::Error::Busy(_) | turso::Error::BusySnapshot(_)) => errors += 1,
                    Err(e) => panic!("unexpected error: {e:?}"),
                }
            }
            (successes, errors)
        }));
    }

    let mut total_successes = 0i64;
    let mut total_errors = 0i64;
    for handle in handles {
        let (s, e) = handle.await.unwrap();
        total_successes += s;
        total_errors += e;
    }

    let conn = db.connect().unwrap();
    let actual_rows = query_i64(&conn, "SELECT COUNT(*) FROM t").await;
    assert_eq!(
        actual_rows,
        total_successes,
        "Ghost commits! {actual_rows} rows in DB but only {total_successes} reported as Ok ({total_errors} errors). \
         {} inserts committed despite returning Busy.",
        total_successes - actual_rows,
    );
}

#[test]
fn shuttle_test_ghost_commits() {
    let scheduler = RandomScheduler::new(100);
    let runner = shuttle::Runner::new(scheduler, shuttle_config());
    runner.run(|| shuttle::future::block_on(ghost_commits_scenario(4, 20)));
}

#[test]
fn shuttle_test_ghost_commits_slow() {
    let scheduler = RandomScheduler::new(10);
    let runner = shuttle::Runner::new(scheduler, shuttle_config());
    runner.run(|| shuttle::future::block_on(ghost_commits_scenario(12, 60)));
}

/// Disjoint Writes No False Conflict: N workers each own a distinct row and update it
/// concurrently. Since no two workers touch the same row, all commits must succeed —
/// no WriteWriteConflict should ever fire.
async fn disjoint_writes_no_false_conflict_scenario(num_workers: usize, rounds: i64) {
    // Create one row per worker: (1, 0), (2, 0), ..., (N, 0)
    let mut schema = String::from("CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER);");
    for i in 1..=num_workers {
        schema.push_str(&format!("INSERT INTO t VALUES({i}, 0);"));
    }
    let (db, _dir) = setup_mvcc_db(&schema).await;

    let barrier = Arc::new(Barrier::new(num_workers));
    let mut handles = Vec::new();

    for worker_id in 1..=num_workers as i64 {
        let conn = db.connect().unwrap();
        let barrier = barrier.clone();
        handles.push(turso_stress::future::spawn(async move {
            barrier.wait();
            for i in 0..rounds {
                let new_val = worker_id * 1000 + i + 1;
                conn.execute("BEGIN CONCURRENT", ()).await.unwrap();
                conn.execute(
                    &format!("UPDATE t SET val = {new_val} WHERE id = {worker_id}"),
                    (),
                )
                .await
                .unwrap();
                conn.execute("COMMIT", ()).await.unwrap();
            }
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    // Verify every worker's final update landed
    let conn = db.connect().unwrap();
    for worker_id in 1..=num_workers as i64 {
        let expected = worker_id * 1000 + rounds;
        let actual = query_i64(&conn, &format!("SELECT val FROM t WHERE id = {worker_id}")).await;
        assert_eq!(
            actual, expected,
            "worker {worker_id}: expected val={expected}, got val={actual}"
        );
    }
}

#[test]
fn shuttle_test_disjoint_writes_no_false_conflict() {
    let scheduler = RandomScheduler::new(100);
    let runner = shuttle::Runner::new(scheduler, shuttle_config());
    runner.run(|| shuttle::future::block_on(disjoint_writes_no_false_conflict_scenario(6, 6)));
}

#[test]
fn shuttle_test_disjoint_writes_no_false_conflict_slow() {
    let scheduler = RandomScheduler::new(10);
    let runner = shuttle::Runner::new(scheduler, shuttle_config());
    runner.run(|| shuttle::future::block_on(disjoint_writes_no_false_conflict_scenario(12, 15)));
}

/// OTV: Observed Transaction Vanishes
/// A writer updates multiple rows in one transaction. Concurrent readers must see
/// either ALL or NONE of the writer's changes — never a partial state.
async fn otv_scenario(num_writers: usize, num_readers: usize, rounds: i64) {
    let (db, _dir) = setup_mvcc_db(
        "CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER);
         INSERT INTO t VALUES(1, 0);
         INSERT INTO t VALUES(2, 0);",
    )
    .await;

    let violation_found = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::new();

    // Writers: atomically update both rows to the same new value
    for w in 0..num_writers {
        let conn = db.connect().unwrap();
        let violation_found = violation_found.clone();
        handles.push(turso_stress::future::spawn(async move {
            for i in 0..rounds {
                let new_val = (w as i64) * 1000 + i + 1;
                conn.execute("BEGIN CONCURRENT", ()).await.unwrap();
                let r1 = conn
                    .execute(&format!("UPDATE t SET val = {new_val} WHERE id = 1"), ())
                    .await;
                if r1.is_err() {
                    let _ = conn.execute("ROLLBACK", ()).await;
                    continue;
                }
                let r2 = conn
                    .execute(&format!("UPDATE t SET val = {new_val} WHERE id = 2"), ())
                    .await;
                if r2.is_err() {
                    let _ = conn.execute("ROLLBACK", ()).await;
                    continue;
                }
                if conn.execute("COMMIT", ()).await.is_err() {
                    let _ = conn.execute("ROLLBACK", ()).await;
                }
            }
            // Silence unused variable warning — violation_found kept alive for readers
            let _ = &violation_found;
        }));
    }

    // Readers: read both rows within a snapshot, assert they match
    for _ in 0..num_readers {
        let conn = db.connect().unwrap();
        let violation_found = violation_found.clone();
        handles.push(turso_stress::future::spawn(async move {
            for _ in 0..rounds {
                conn.execute("BEGIN CONCURRENT", ()).await.unwrap();
                let val1 = query_i64(&conn, "SELECT val FROM t WHERE id = 1").await;
                shuttle::future::yield_now().await;
                let val2 = query_i64(&conn, "SELECT val FROM t WHERE id = 2").await;
                let _ = conn.execute("COMMIT", ()).await;
                if val1 != val2 {
                    violation_found.store(true, Ordering::Relaxed);
                    eprintln!(
                        "OTV VIOLATION: row1={val1}, row2={val2} — partial transaction visible"
                    );
                }
            }
        }));
    }

    for handle in handles {
        let _ = handle.await;
    }

    assert!(
        !violation_found.load(Ordering::Relaxed),
        "OTV: reader saw partial transaction state (row1 != row2 within snapshot)"
    );
}

#[test]
fn shuttle_test_otv() {
    let scheduler = RandomScheduler::new(100);
    let runner = shuttle::Runner::new(scheduler, shuttle_config());
    runner.run(|| shuttle::future::block_on(otv_scenario(4, 4, 6)));
}

#[test]
fn shuttle_test_otv_slow() {
    let scheduler = RandomScheduler::new(10);
    let runner = shuttle::Runner::new(scheduler, shuttle_config());
    runner.run(|| shuttle::future::block_on(otv_scenario(9, 9, 15)));
}

/// Rollback Isolation: rolled-back writes must never be visible to concurrent readers.
/// Writers repeatedly update a row then ROLLBACK. Readers must only ever see the
/// originally committed value.
async fn rollback_isolation_scenario(num_writers: usize, num_readers: usize, rounds: i64) {
    let (db, _dir) = setup_mvcc_db(
        "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);
         INSERT INTO t VALUES(1, 'committed');",
    )
    .await;

    let violation_found = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::new();

    // Writers: update then always rollback
    for w in 0..num_writers {
        let conn = db.connect().unwrap();
        handles.push(turso_stress::future::spawn(async move {
            for i in 0..rounds {
                conn.execute("BEGIN CONCURRENT", ()).await.unwrap();
                let aborted_val = format!("aborted_w{w}_i{i}");
                let _ = conn
                    .execute(
                        &format!("UPDATE t SET val = '{aborted_val}' WHERE id = 1"),
                        (),
                    )
                    .await;
                let _ = conn.execute("ROLLBACK", ()).await;
            }
        }));
    }

    // Readers: verify we only ever see 'committed'
    for _ in 0..num_readers {
        let conn = db.connect().unwrap();
        let violation_found = violation_found.clone();
        handles.push(turso_stress::future::spawn(async move {
            for _ in 0..rounds {
                let val = query_string(&conn, "SELECT val FROM t WHERE id = 1").await;
                if val != "committed" {
                    violation_found.store(true, Ordering::Relaxed);
                    eprintln!("ROLLBACK ISOLATION VIOLATION: saw '{val}' instead of 'committed'");
                }
            }
        }));
    }

    for handle in handles {
        let _ = handle.await;
    }

    assert!(
        !violation_found.load(Ordering::Relaxed),
        "Rollback isolation violated: reader saw data from a rolled-back transaction"
    );
}

#[test]
fn shuttle_test_rollback_isolation() {
    let scheduler = RandomScheduler::new(100);
    let runner = shuttle::Runner::new(scheduler, shuttle_config());
    runner.run(|| shuttle::future::block_on(rollback_isolation_scenario(4, 4, 6)));
}

#[test]
fn shuttle_test_rollback_isolation_slow() {
    let scheduler = RandomScheduler::new(10);
    let runner = shuttle::Runner::new(scheduler, shuttle_config());
    runner.run(|| shuttle::future::block_on(rollback_isolation_scenario(9, 9, 15)));
}

/// Phantom Prevention: predicate query results must not change within a snapshot,
/// even as other transactions insert matching rows and commit.
async fn phantom_prevention_scenario(num_writers: usize, num_readers: usize, ops_per_writer: i64) {
    let (db, _dir) = setup_mvcc_db("CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER)").await;

    let violation_found = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(num_writers + num_readers));
    let mut handles = Vec::new();

    // Writers: insert rows with values divisible by 3
    for w in 0..num_writers as i64 {
        let conn = db.connect().unwrap();
        let barrier = barrier.clone();
        handles.push(turso_stress::future::spawn(async move {
            barrier.wait();
            for i in 0..ops_per_writer {
                let id = w * 10_000 + i;
                let val = (i + 1) * 3; // always divisible by 3
                conn.execute("BEGIN CONCURRENT", ()).await.unwrap();
                if conn
                    .execute(&format!("INSERT INTO t VALUES({id}, {val})"), ())
                    .await
                    .is_err()
                {
                    let _ = conn.execute("ROLLBACK", ()).await;
                    continue;
                }
                if conn.execute("COMMIT", ()).await.is_err() {
                    let _ = conn.execute("ROLLBACK", ()).await;
                }
            }
        }));
    }

    // Readers: within a snapshot, predicate COUNT must be stable
    for _ in 0..num_readers {
        let conn = db.connect().unwrap();
        let violation_found = violation_found.clone();
        let barrier = barrier.clone();
        handles.push(turso_stress::future::spawn(async move {
            barrier.wait();
            for _ in 0..ops_per_writer {
                conn.execute("BEGIN CONCURRENT", ()).await.unwrap();
                let count1 = query_i64(&conn, "SELECT COUNT(*) FROM t WHERE val % 3 = 0").await;
                shuttle::future::yield_now().await;
                let count2 = query_i64(&conn, "SELECT COUNT(*) FROM t WHERE val % 3 = 0").await;
                let _ = conn.execute("COMMIT", ()).await;
                if count1 != count2 {
                    violation_found.store(true, Ordering::Relaxed);
                    eprintln!(
                        "PHANTOM: predicate COUNT changed {} -> {} within same txn",
                        count1, count2
                    );
                }
            }
        }));
    }

    for handle in handles {
        let _ = handle.await;
    }

    assert!(
        !violation_found.load(Ordering::Relaxed),
        "Phantom prevention violated: predicate query results changed within a single txn"
    );
}

#[test]
fn shuttle_test_phantom_prevention() {
    let scheduler = RandomScheduler::new(100);
    let runner = shuttle::Runner::new(scheduler, shuttle_config());
    runner.run(|| shuttle::future::block_on(phantom_prevention_scenario(4, 4, 10)));
}

#[test]
fn shuttle_test_phantom_prevention_slow() {
    let scheduler = RandomScheduler::new(10);
    let runner = shuttle::Runner::new(scheduler, shuttle_config());
    runner.run(|| shuttle::future::block_on(phantom_prevention_scenario(9, 9, 24)));
}

/// Speculative visibility abort: concurrent UPDATE + ROLLBACK on MVCC-store rows.
async fn speculative_abort_delete_scenario(num_workers: usize, rounds: i64) {
    let (db, _dir) = setup_mvcc_db(
        "CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER);
         INSERT INTO t VALUES(1, 0);
         INSERT INTO t VALUES(2, 0);
         INSERT INTO t VALUES(3, 0);",
    )
    .await;

    let barrier = Arc::new(Barrier::new(num_workers));
    let mut handles = Vec::new();

    for w in 0..num_workers as i64 {
        let conn = db.connect().unwrap();
        let barrier = barrier.clone();
        let should_rollback = w % 2 == 0;
        handles.push(turso_stress::future::spawn(async move {
            barrier.wait();
            for i in 0..rounds {
                conn.execute("BEGIN CONCURRENT", ()).await.unwrap();

                // Mix of self-update (SET val=val) and actual changes
                let result = if i % 3 == 0 {
                    conn.execute("UPDATE t SET val = val", ()).await
                } else {
                    let new_val = w * 1000 + i + 1;
                    conn.execute(&format!("UPDATE t SET val = {new_val}"), ())
                        .await
                };

                match result {
                    Ok(_) => {
                        if should_rollback {
                            // Force abort to create the Preparing → Aborted window
                            let _ = conn.execute("ROLLBACK", ()).await;
                        } else {
                            match conn.execute("COMMIT", ()).await {
                                Ok(_) => {}
                                Err(_) => {
                                    let _ = conn.execute("ROLLBACK", ()).await;
                                }
                            }
                        }
                    }
                    Err(_) => {
                        let _ = conn.execute("ROLLBACK", ()).await;
                    }
                }
            }
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    // All rows must survive — no corruption
    let conn = db.connect().unwrap();
    let count = query_i64(&conn, "SELECT COUNT(*) FROM t").await;
    assert_eq!(
        count, 3,
        "Rows disappeared! Expected 3 rows but found {count}. \
         Likely hit the speculative visibility abort bug in MvccLazyCursor::delete()."
    );
}

#[test]
fn shuttle_test_speculative_abort_delete() {
    let scheduler = RandomScheduler::new(100);
    let runner = shuttle::Runner::new(scheduler, shuttle_config());
    runner.run(|| shuttle::future::block_on(speculative_abort_delete_scenario(6, 6)));
}

#[test]
fn shuttle_test_speculative_abort_delete_slow() {
    let scheduler = RandomScheduler::new(10);
    let runner = shuttle::Runner::new(scheduler, shuttle_config());
    runner.run(|| shuttle::future::block_on(speculative_abort_delete_scenario(30, 60)));
}

#[test]
fn shuttle_test_begin_publish_window_gc_hazard() {
    let scheduler = PctScheduler::new(5, 10000);
    let runner = shuttle::Runner::new(scheduler, shuttle_config());
    runner.run(|| shuttle::future::block_on(begin_publish_window_gc_scenario(3, 5)));
}

/// Begin-publish-window GC hazard (regression test for PR #7493).
///
/// With aggressive inline GC enabled (`PRAGMA mvcc_gc_threshold = 1`), every
/// committed write runs `gc_incremental` on the commit path. A reader that has
/// allocated its begin timestamp but is not yet published into `txs` is invisible
/// to `compute_lwm`; if a writer commits in that window, inline GC could free the
/// version the reader still needs. The reader then resumes, reads at its old
/// snapshot, finds the new version invisible, falls through to an empty B-tree
/// (nothing is checkpointed at this size), and observes NO ROW.
///
/// The fix publishes a transaction into `txs` atomically with its begin
/// timestamp under the clock lock, so a committing writer always sees the
/// reader's snapshot (or assigns a strictly newer commit timestamp the reader
/// will see). Row id=1 is inserted once at setup and never deleted, so every
/// snapshot MUST observe exactly one row for it; a reader seeing zero rows is
/// the violation.
async fn begin_publish_window_gc_scenario(num_readers: usize, rounds: i64) {
    let (db, _dir) = setup_mvcc_db(
        "CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER);
         INSERT INTO t VALUES(1, 0);",
    )
    .await;

    // Make every committed write trigger an inline GC pass.
    {
        db.connect()
            .unwrap()
            .execute("PRAGMA mvcc_gc_threshold = 1", ())
            .await
            .unwrap();
    }

    let row_disappeared = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::new();

    {
        let conn = db.connect().unwrap();
        handles.push(turso_stress::future::spawn(async move {
            for i in 0..rounds {
                let _ = conn
                    .execute(&format!("UPDATE t SET val = {} WHERE id = 1", i + 1), ())
                    .await;
            }
        }));
    }

    for _ in 0..num_readers {
        let conn = db.connect().unwrap();
        let row_disappeared = row_disappeared.clone();
        handles.push(turso_stress::future::spawn(async move {
            let mut saw_one = false;

            for _ in 0..rounds {
                if row_disappeared.load(Ordering::Acquire) {
                    return;
                }
                match conn
                    .query("SELECT val FROM t WHERE id = 1", ())
                    .await
                    .unwrap()
                    .next()
                    .await
                {
                    Ok(Some(_)) => saw_one = true,
                    Ok(None) if saw_one => {
                        row_disappeared.store(true, Ordering::Release);
                    }
                    _ => {}
                }
            }
        }));
    }

    for handle in handles {
        let _ = handle.await;
    }

    assert!(
        !row_disappeared.load(Ordering::Acquire),
        "Observed a row, and then no row. This violates Snapshot Isolation"
    );
}

async fn passive_checkpoint_stale_table_id_aliases_index_root_scenario() {
    const WRITES: i64 = 4;

    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("passive-checkpoint-table-index-alias.db");
    let db_path = db_path.to_str().unwrap();

    let db = Builder::new_local(db_path)
        .experimental_mvcc_passive_checkpoint(true)
        .build()
        .await
        .unwrap();

    let setup = db.connect().unwrap();

    {
        let mut rows = setup
            .query("PRAGMA journal_mode = 'experimental_mvcc'", ())
            .await
            .unwrap();

        while rows.next().await.unwrap().is_some() {}
    }

    setup
        .execute("PRAGMA mvcc_checkpoint_threshold = -1", ())
        .await
        .unwrap();

    setup
        .execute_batch(
            "CREATE TABLE anchor(
                 id  INTEGER PRIMARY KEY,
                 pad BLOB
             );

             INSERT INTO anchor
             SELECT value, zeroblob(1000)
             FROM generate_series(1, 8);",
        )
        .await
        .unwrap();

    async fn passive_checkpoint(conn: &turso::Connection) {
        let mut rows = conn
            .query("PRAGMA wal_checkpoint(PASSIVE)", ())
            .await
            .unwrap();

        while rows.next().await.unwrap().is_some() {}
    }

    passive_checkpoint(&setup).await;
    turso_stress::note_progress();

    assert_eq!(query_i64(&setup, "PRAGMA page_count").await, 5);

    setup
        .execute_batch(
            "CREATE TABLE a(
                 id INTEGER PRIMARY KEY,
                 v  TEXT
             );

             CREATE INDEX a_v ON a(v);

             CREATE TABLE dummy(
                 id INTEGER PRIMARY KEY
             );

             CREATE TABLE b(
                 id INTEGER PRIMARY KEY,
                 v  TEXT
             );

             INSERT INTO anchor
             SELECT value + 100, zeroblob(1)
             FROM generate_series(1, 1025);",
        )
        .await
        .unwrap();
    turso_stress::note_progress();

    assert!(
        query_i64(
            &setup,
            "SELECT rootpage
               FROM sqlite_schema
              WHERE name = 'a_v'",
        )
        .await
            < 0
    );

    let b_temporary_root = query_i64(
        &setup,
        "SELECT rootpage
           FROM sqlite_schema
          WHERE name = 'b'",
    )
    .await;

    assert!(b_temporary_root < 0);

    let writer_conn = db.connect().unwrap();
    writer_conn.execute("BEGIN CONCURRENT", ()).await.unwrap();
    writer_conn
        .execute("INSERT INTO b VALUES(100000, 'ack-100000')", ())
        .await
        .unwrap();
    turso_stress::note_progress();
    let mut writer_commit_stmt = writer_conn.prepare("COMMIT").await.unwrap();

    // The trigger's commit is visible before its retained auto-checkpoint
    // future first parks. At that point PrepareCheckpoint has frozen its
    // snapshot and raised the temporary-ID commit gate.
    setup
        .execute("PRAGMA mvcc_checkpoint_threshold = 0", ())
        .await
        .unwrap();
    let checkpoint_parked = Arc::new(AtomicBool::new(false));
    let checkpoint_completed = Arc::new(AtomicBool::new(false));
    let writer_commit_polled = Arc::new(AtomicBool::new(false));

    let trigger_conn = db.connect().unwrap();
    let observer = db.connect().unwrap();
    let checkpoint_config = db.connect().unwrap();
    let mut trigger_stmt = trigger_conn
        .prepare("INSERT INTO anchor VALUES(9, zeroblob(1000))")
        .await
        .unwrap();
    let checkpoint_parked_task = checkpoint_parked.clone();
    let checkpoint_completed_task = checkpoint_completed.clone();
    let writer_commit_polled_task = writer_commit_polled.clone();

    let checkpoint = turso_stress::future::spawn(async move {
        let mut trigger_future = std::pin::pin!(trigger_stmt.execute(()));
        let observer_future = async move {
            loop {
                let mut rows = observer
                    .query("SELECT count(*) FROM anchor WHERE id = 9", ())
                    .await
                    .unwrap();
                let mut count = None;
                while let Some(row) = rows.next().await.unwrap() {
                    count = Some(row.get::<i64>(0).unwrap());
                }
                if count == Some(1) {
                    return;
                }
                shuttle::future::yield_now().await;
            }
        };
        let mut observer_future = std::pin::pin!(observer_future);
        let mut parked_after_commit = false;

        for _ in 0..1_000 {
            let trigger_poll = std::future::poll_fn(|cx| {
                std::task::Poll::Ready(match trigger_future.as_mut().poll(cx) {
                    std::task::Poll::Pending => None,
                    std::task::Poll::Ready(result) => Some(result),
                })
            })
            .await;

            if let Some(result) = trigger_poll {
                result.unwrap();
                panic!("auto-checkpoint completed before it could be parked");
            }

            let observer_done = std::future::poll_fn(|cx| {
                std::task::Poll::Ready(matches!(
                    observer_future.as_mut().poll(cx),
                    std::task::Poll::Ready(())
                ))
            })
            .await;
            if observer_done {
                parked_after_commit = true;
                break;
            }

            shuttle::future::yield_now().await;
        }

        assert!(
            parked_after_commit,
            "trigger commit never reached its retained auto-checkpoint"
        );
        turso_stress::note_progress();

        checkpoint_parked_task.store(true, Ordering::SeqCst);

        let mut writer_was_polled = false;
        for _ in 0..1_000 {
            if writer_commit_polled_task.load(Ordering::SeqCst) {
                writer_was_polled = true;
                break;
            }
            shuttle::future::yield_now().await;
        }
        assert!(
            writer_was_polled,
            "writer never polled COMMIT after the checkpoint parked"
        );

        trigger_future.await.unwrap();
        turso_stress::note_progress();

        // The threshold is store-global. The writer waits for this post-
        // checkpoint handoff before resuming COMMIT, so its durable frame stays
        // in the logical log for cold recovery.
        checkpoint_config
            .execute("PRAGMA mvcc_checkpoint_threshold = -1", ())
            .await
            .unwrap();
        checkpoint_completed_task.store(true, Ordering::SeqCst);
        turso_stress::note_progress();
    });

    let writer_checkpoint_parked = checkpoint_parked.clone();
    let writer_checkpoint_completed = checkpoint_completed.clone();
    let writer_commit_polled_task = writer_commit_polled.clone();

    let writer = turso_stress::future::spawn(async move {
        let mut saw_parked_checkpoint = false;
        for _ in 0..1_000 {
            if writer_checkpoint_parked.load(Ordering::SeqCst) {
                saw_parked_checkpoint = true;
                break;
            }
            shuttle::future::yield_now().await;
        }
        assert!(
            saw_parked_checkpoint,
            "writer never observed the parked checkpoint"
        );

        // Preparation and the row write happened before the checkpoint. The
        // only remaining work is COMMIT, so Pending here is the temporary-ID
        // checkpoint fence rather than statement preparation or pager IO.
        let mut writer_commit = std::pin::pin!(writer_commit_stmt.execute(()));
        let first_poll = std::future::poll_fn(|cx| {
            std::task::Poll::Ready(match writer_commit.as_mut().poll(cx) {
                std::task::Poll::Pending => None,
                std::task::Poll::Ready(result) => Some(result),
            })
        })
        .await;
        assert!(
            first_poll.is_none(),
            "prepared writer COMMIT bypassed the active checkpoint fence"
        );
        writer_commit_polled_task.store(true, Ordering::SeqCst);

        let mut saw_completed_checkpoint = false;
        for _ in 0..1_000 {
            if writer_checkpoint_completed.load(Ordering::SeqCst) {
                saw_completed_checkpoint = true;
                break;
            }
            shuttle::future::yield_now().await;
        }
        assert!(
            saw_completed_checkpoint,
            "writer never observed checkpoint completion"
        );

        writer_commit.await.unwrap();
        turso_stress::note_progress();

        let mut acknowledged = 1;
        for id in 100_001..100_000 + WRITES {
            match writer_conn
                .execute(format!("INSERT INTO b VALUES({id}, 'ack-{id}')"), ())
                .await
            {
                Ok(_) => {
                    acknowledged += 1;
                }

                Err(turso::Error::Busy(_) | turso::Error::BusySnapshot(_)) => {}

                Err(error) => {
                    panic!("unexpected INSERT error: {error:?}");
                }
            }

            shuttle::future::yield_now().await;
        }

        acknowledged
    });

    checkpoint.await.unwrap();
    let acknowledged = writer.await.unwrap();
    turso_stress::note_progress();

    assert!(acknowledged > 0);
    assert!(
        writer_commit_polled.load(Ordering::SeqCst),
        "the prepared writer commit must be polled while the checkpoint is parked"
    );

    drop(setup);
    drop(db);

    let cold_db = Builder::new_local(db_path)
        .experimental_mvcc_passive_checkpoint(true)
        .build()
        .await
        .unwrap();

    let cold = cold_db.connect().unwrap();

    cold.execute("PRAGMA mvcc_checkpoint_threshold = -1", ())
        .await
        .unwrap();

    let cold_index_root = query_i64(
        &cold,
        "SELECT rootpage
           FROM sqlite_schema
          WHERE name = 'a_v'",
    )
    .await;

    assert_eq!(
        cold_index_root, -b_temporary_root,
        "fixture must preserve the stale-table-ID/index-root alias"
    );

    let recovered = query_i64(&cold, "SELECT count(*) FROM b WHERE id >= 100000").await;

    assert_eq!(
        recovered, acknowledged,
        "cold recovery lost acknowledged writes to b"
    );

    assert_eq!(query_string(&cold, "PRAGMA integrity_check").await, "ok");
    turso_stress::note_progress();

    passive_checkpoint(&cold).await;

    let recovered_after_checkpoint =
        query_i64(&cold, "SELECT count(*) FROM b WHERE id >= 100000").await;

    assert_eq!(
        recovered_after_checkpoint, acknowledged,
        "post-recovery checkpoint lost acknowledged writes to b"
    );

    assert_eq!(query_string(&cold, "PRAGMA integrity_check").await, "ok");
}

#[test]
fn shuttle_test_passive_checkpoint_stale_table_id_aliases_index_root() {
    // The explicit checkpoint/commit handshake above fixes the relevant
    // ordering, so one deterministic execution covers the regression.
    let scheduler = PctScheduler::new_from_seed(0xa903_398d_4995_8e14, 5, 1);

    let runner = shuttle::Runner::new(scheduler, shuttle_config());

    runner.run(|| {
        shuttle::future::block_on(passive_checkpoint_stale_table_id_aliases_index_root_scenario())
    });
}
