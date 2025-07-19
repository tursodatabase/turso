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
pub mod errors;

pub use clock::LocalClock;
pub use database::MvStore;

#[cfg(test)]
mod tests {
    use crate::mvcc::clock::LocalClock;
    use crate::mvcc::database::{MvStore, Row, RowID};
    use std::sync::atomic::AtomicI64;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::rc::Rc;
    use crate::storage::sqlite3_ondisk::DatabaseHeader;
    use crate::storage::pager::Pager;
    use crate::storage::wal::DummyWAL;
    use crate::storage::page_cache::DumbLruPageCache;
    use crate::storage::database::FileMemoryStorage;
    use crate::OpenFlags;

    fn dummy_pager() -> Rc<Pager> {
        let storage = FileMemoryStorage::new();
        let cache = DumbLruPageCache::new(1024);
        let wal = DummyWAL::new();
        let header = DatabaseHeader::new(1024, 1, 1);
        let pager = Pager::new(header, storage, cache, wal, OpenFlags::ReadWrite).unwrap();
        Rc::new(pager)
    }

    fn dummy_connection() -> crate::Connection {
        let storage = FileMemoryStorage::new();
        let cache = DumbLruPageCache::new(1024);
        let wal = DummyWAL::new();
        let header = DatabaseHeader::new(1024, 1, 1);
        let pager = Pager::new(header, storage, cache, wal, OpenFlags::ReadWrite).unwrap();
        crate::Connection::new(pager).unwrap()
    }

    static IDS: AtomicI64 = AtomicI64::new(1);

    #[test]
    fn test_non_overlapping_concurrent_inserts() {
        // Two threads insert to the database concurrently using non-overlapping
        // row IDs.
        let clock = LocalClock::default();
        let db = Arc::new(MvStore::new(clock));
        let iterations = 100000;

        let th1 = {
            let db = db.clone();
            std::thread::spawn(move || {
                for _ in 0..iterations {
                    let tx = db.begin_tx();
                    let id = IDS.fetch_add(1, Ordering::SeqCst);
                    let id = RowID {
                        table_id: 1,
                        row_id: id,
                    };
                    let row = Row {
                        id,
                        data: "Hello".to_string().into_bytes(),
                        column_count: 1,
                    };
                    db.insert(tx, row.clone()).unwrap();
                    db.commit_tx(tx, dummy_pager(), &dummy_connection()).unwrap();
                    let tx = db.begin_tx();
                    let committed_row = db.read(tx, id).unwrap();
                    db.commit_tx(tx, dummy_pager(), &dummy_connection()).unwrap();
                    assert_eq!(committed_row, Some(row));
                }
            })
        };
        let th2 = {
            std::thread::spawn(move || {
                for _ in 0..iterations {
                    let tx = db.begin_tx();
                    let id = IDS.fetch_add(1, Ordering::SeqCst);
                    let id = RowID {
                        table_id: 1,
                        row_id: id,
                    };
                    let row = Row {
                        id,
                        data: "World".to_string().into_bytes(),
                        column_count: 1,
                    };
                    db.insert(tx, row.clone()).unwrap();
                    db.commit_tx(tx, dummy_pager(), &dummy_connection()).unwrap();
                    let tx = db.begin_tx();
                    let committed_row = db.read(tx, id).unwrap();
                    db.commit_tx(tx, dummy_pager(), &dummy_connection()).unwrap();
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
        let clock = LocalClock::default();
        let db = Arc::new(MvStore::new(clock));
        let iterations = 100000;

        let work = |prefix: &'static str| {
            let db = db.clone();
            std::thread::spawn(move || {
                let mut failed_upserts = 0;
                for i in 0..iterations {
                    if i % 1000 == 0 {
                        tracing::debug!("{prefix}: {i}");
                    }
                    if i % 10000 == 0 {
                        let dropped = db.drop_unused_row_versions();
                        tracing::debug!("garbage collected {dropped} versions");
                    }
                    let tx = db.begin_tx();
                    let id = i % 16;
                    let id = RowID {
                        table_id: 1,
                        row_id: id,
                    };
                    let row = Row {
                        id,
                        data: format!("{prefix} @{tx}").into_bytes(),
                    };
                    if let Err(e) = db.upsert(tx, row.clone()) {
                        tracing::trace!("upsert failed: {e}");
                        failed_upserts += 1;
                        continue;
                    }
                    let committed_row = db.read(tx, id).unwrap();
                    db.commit_tx(tx, dummy_pager(), &dummy_connection()).unwrap();
                    assert_eq!(committed_row, Some(row));
                }
                tracing::info!(
                    "{prefix}'s failed upserts: {failed_upserts}/{iterations} {:.2}%",
                    (failed_upserts * 100) as f64 / iterations as f64
                );
            })
        };

        let threads = vec![work("A"), work("B"), work("C"), work("D")];
        for th in threads {
            th.join().unwrap();
        }
    }
}
