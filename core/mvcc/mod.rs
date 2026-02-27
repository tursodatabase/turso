//! Multiversion concurrency control (MVCC) for Rust.
//!
//! This module implements the main memory MVCC method outlined in the paper
//! "High-Performance Concurrency Control Mechanisms for Main-Memory Databases"
//! by Per-Åke Larson et al (VLDB, 2011).
//!
//! ## Data anomalies
//!
//! * A *dirty write* occurs when transaction T_m updates a value that is written by
//!   transaction T_n but not yet committed. The MVCC algorithm prevents dirty
//!   writes by validating that a row version is visible to transaction T_m before
//!   allowing update to it.
//!
//! * A *dirty read* occurs when transaction T_m reads a value that was written by
//!   transaction T_n but not yet committed. The MVCC algorithm prevents dirty
//!   reads by validating that a row version is visible to transaction T_m.
//!
//! * A *fuzzy read* (non-repeatable read) occurs when transaction T_m reads a
//!   different value in the course of the transaction because another
//!   transaction T_n has updated the value.
//!
//! * A *lost update* occurs when transactions T_m and T_n both attempt to update
//!   the same value, resulting in one of the updates being lost. The MVCC algorithm
//!   prevents lost updates by detecting the write-write conflict and letting the
//!   first-writer win by aborting the later transaction.
//!
//! TODO: phantom reads, cursor lost updates, read skew, write skew.
//!
//! ## TODO
//!
//! * Optimistic reads and writes
//! * Garbage collection

pub mod clock;
pub mod cursor;
pub mod database;
pub mod persistent_storage;

pub use clock::{LocalClock, MvccClock};
pub use database::MvStore;

#[cfg(test)]
mod tests {
    use crate::mvcc::database::tests::{
        commit_tx_no_conn, generate_simple_string_row, MvccTestDbNoConn,
    };
    use crate::mvcc::database::{RowID, RowKey};
    use crate::sync::atomic::AtomicI64;
    use crate::sync::atomic::Ordering;
    use crate::sync::Arc;
    use crate::LimboError;

    static IDS: AtomicI64 = AtomicI64::new(1);

    /// What this test checks: MVCC transaction visibility and conflict handling follow the intended isolation behavior.
    /// Why this matters: Concurrency bugs are correctness bugs: they create anomalies users can observe as wrong query results.
    #[test]
    #[ignore = "FIXME: This test fails because there is write busy lock yet to be fixed"]
    fn test_non_overlapping_concurrent_inserts() {
        // Two threads insert to the database concurrently using non-overlapping
        // row IDs.
        let db = Arc::new(MvccTestDbNoConn::new());
        {
            let conn = db.connect();
            conn.execute("CREATE TABLE t(v TEXT)").unwrap();
        }
        let iterations = 10_000;

        let th1 = {
            let db = db.clone();
            std::thread::spawn(move || {
                let conn = db.get_db().connect().unwrap();
                let mvcc_store = db.get_db().get_mv_store().clone().unwrap();
                for _ in 0..iterations {
                    let tx = loop {
                        match mvcc_store.begin_tx(conn.pager.load().clone()) {
                            Ok(tx) => break tx,
                            Err(LimboError::Busy) => {
                                std::thread::yield_now();
                            }
                            Err(e) => panic!("unexpected begin_tx error: {e:?}"),
                        }
                    };
                    let id = IDS.fetch_add(1, Ordering::SeqCst);
                    let id = RowID {
                        table_id: (-2).into(),
                        row_id: RowKey::Int(id),
                    };
                    let row = generate_simple_string_row(
                        (-2).into(),
                        id.row_id.to_int_or_panic(),
                        "Hello",
                    );
                    mvcc_store.insert(tx, row.clone()).unwrap();
                    loop {
                        match commit_tx_no_conn(&db, tx, &conn) {
                            Ok(()) => break,
                            Err(LimboError::Busy) => {
                                mvcc_store.rollback_tx(tx, conn.pager.load().clone(), &conn);
                                std::thread::yield_now();
                                continue;
                            }
                            Err(e) => panic!("unexpected commit error: {e:?}"),
                        }
                    }
                    let tx = loop {
                        match mvcc_store.begin_tx(conn.pager.load().clone()) {
                            Ok(tx) => break tx,
                            Err(LimboError::Busy) => {
                                std::thread::yield_now();
                            }
                            Err(e) => panic!("unexpected begin_tx error: {e:?}"),
                        }
                    };
                    let committed_row = mvcc_store.read(tx, &id).unwrap();
                    loop {
                        match commit_tx_no_conn(&db, tx, &conn) {
                            Ok(()) => break,
                            Err(LimboError::Busy) => {
                                mvcc_store.rollback_tx(tx, conn.pager.load().clone(), &conn);
                                std::thread::yield_now();
                                continue;
                            }
                            Err(e) => panic!("unexpected commit error: {e:?}"),
                        }
                    }
                    assert_eq!(committed_row, Some(row));
                }
            })
        };
        let th2 = {
            std::thread::spawn(move || {
                let conn = db.get_db().connect().unwrap();
                let mvcc_store = db.get_db().get_mv_store().clone().unwrap();
                for _ in 0..iterations {
                    let tx = loop {
                        match mvcc_store.begin_tx(conn.pager.load().clone()) {
                            Ok(tx) => break tx,
                            Err(LimboError::Busy) => {
                                std::thread::yield_now();
                            }
                            Err(e) => panic!("unexpected begin_tx error: {e:?}"),
                        }
                    };
                    let id = IDS.fetch_add(1, Ordering::SeqCst);
                    let id = RowID {
                        table_id: (-2).into(),
                        row_id: RowKey::Int(id),
                    };
                    let row = generate_simple_string_row(
                        (-2).into(),
                        id.row_id.to_int_or_panic(),
                        "World",
                    );
                    mvcc_store.insert(tx, row.clone()).unwrap();
                    loop {
                        match commit_tx_no_conn(&db, tx, &conn) {
                            Ok(()) => break,
                            Err(LimboError::Busy) => {
                                mvcc_store.rollback_tx(tx, conn.pager.load().clone(), &conn);
                                std::thread::yield_now();
                                continue;
                            }
                            Err(e) => panic!("unexpected commit error: {e:?}"),
                        }
                    }
                    let tx = loop {
                        match mvcc_store.begin_tx(conn.pager.load().clone()) {
                            Ok(tx) => break tx,
                            Err(LimboError::Busy) => {
                                std::thread::yield_now();
                            }
                            Err(e) => panic!("unexpected begin_tx error: {e:?}"),
                        }
                    };
                    let committed_row = mvcc_store.read(tx, &id).unwrap();
                    loop {
                        match commit_tx_no_conn(&db, tx, &conn) {
                            Ok(()) => break,
                            Err(LimboError::Busy) => {
                                mvcc_store.rollback_tx(tx, conn.pager.load().clone(), &conn);
                                std::thread::yield_now();
                                continue;
                            }
                            Err(e) => panic!("unexpected commit error: {e:?}"),
                        }
                    }
                    assert_eq!(committed_row, Some(row));
                }
            })
        };
        th1.join().unwrap();
        th2.join().unwrap();
    }

    // FIXME: This test fails sporadically.
    #[test]
    #[ignore]
    fn test_overlapping_concurrent_inserts_read_your_writes() {
        let db = Arc::new(MvccTestDbNoConn::new());
        {
            let conn = db.connect();
            conn.execute("CREATE TABLE t(v TEXT)").unwrap();
        }
        let iterations = 20_000;

        let work = |prefix: &'static str| {
            let db = db.clone();
            std::thread::spawn(move || {
                let conn = db.get_db().connect().unwrap();
                let mvcc_store = db.get_db().get_mv_store().clone().unwrap();
                let mut failed_upserts = 0;
                let mut failed_commits = 0;
                let mut busy_retries = 0;
                for i in 0..iterations {
                    if i % 1000 == 0 {
                        tracing::debug!("{prefix}: {i}");
                    }
                    if i % 10000 == 0 {
                        let dropped = mvcc_store.drop_unused_row_versions();
                        tracing::debug!("garbage collected {dropped} versions");
                    }
                    let tx = loop {
                        match mvcc_store.begin_tx(conn.pager.load().clone()) {
                            Ok(tx) => break tx,
                            Err(LimboError::Busy) => {
                                busy_retries += 1;
                                std::thread::yield_now();
                            }
                            Err(e) => panic!("unexpected begin_tx error: {e:?}"),
                        }
                    };
                    let id = i % 16;
                    let id = RowID {
                        table_id: (-2).into(),
                        row_id: RowKey::Int(id),
                    };
                    let row = generate_simple_string_row(
                        (-2).into(),
                        id.row_id.to_int_or_panic(),
                        &format!("{prefix} @{tx}"),
                    );
                    if let Err(e) = mvcc_store.upsert(tx, row.clone()) {
                        tracing::trace!("upsert failed: {e}");
                        failed_upserts += 1;
                        mvcc_store.rollback_tx(tx, conn.pager.load().clone(), &conn);
                        continue;
                    }
                    let committed_row = mvcc_store.read(tx, &id).unwrap();
                    match commit_tx_no_conn(&db, tx, &conn) {
                        Ok(()) => {}
                        Err(LimboError::Busy | LimboError::WriteWriteConflict) => {
                            failed_commits += 1;
                            mvcc_store.rollback_tx(tx, conn.pager.load().clone(), &conn);
                            continue;
                        }
                        Err(e) => panic!("unexpected commit error: {e:?}"),
                    }
                    assert_eq!(committed_row, Some(row));
                }
                tracing::info!(
                    "{prefix}: failed_upserts={failed_upserts}, failed_commits={failed_commits}, busy_retries={busy_retries} of {iterations}",
                );
            })
        };

        let threads = vec![work("A"), work("B"), work("C"), work("D")];
        for th in threads {
            th.join().unwrap();
        }
    }

    /// What this test checks: MVCC transaction visibility and conflict handling follow the intended isolation behavior.
    /// Why this matters: Concurrency bugs are correctness bugs: they create anomalies users can observe as wrong query results.
    #[test]
    fn test_mvcc_dual_cursor_transaction_isolation() {
        let res = tracing_subscriber::fmt::try_init();
        drop(res);
        let mut db = MvccTestDbNoConn::new_with_random_db();

        {
            let conn = db.connect();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER)")
                .unwrap();
            conn.execute("INSERT INTO t VALUES (1, 100)").unwrap();
            conn.execute("INSERT INTO t VALUES (2, 200)").unwrap();
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        }

        db.restart();

        // Tx1: Update B-tree row
        let conn1 = db.connect();
        conn1.execute("BEGIN CONCURRENT").unwrap();
        conn1
            .execute("UPDATE t SET val = 999 WHERE id = 1")
            .unwrap();

        // Tx2: Should see old B-tree value, not Tx1's MVCC change
        let conn2 = db.connect();
        conn2.execute("BEGIN CONCURRENT").unwrap();
        let rows = get_rows(&conn2, "SELECT val FROM t WHERE id = 1");
        assert_eq!(rows[0][0].as_int().unwrap(), 100); // Original B-tree value

        conn1.execute("COMMIT").unwrap();
        conn2.execute("COMMIT").unwrap();
    }
    fn get_rows(conn: &Arc<crate::Connection>, sql: &str) -> Vec<Vec<crate::Value>> {
        let mut stmt = conn.prepare(sql).unwrap();
        let mut rows = Vec::new();
        stmt.run_with_row_callback(|row| {
            rows.push(row.get_values().cloned().collect());
            Ok(())
        })
        .unwrap();
        rows
    }
}
