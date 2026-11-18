use super::checkpoint::get_rows;
use super::database::MvccTestDb;
use crate::mvcc::test_db::MvccTestDbNoConn;
use std::sync::Arc;
use turso_core::mvcc::clock::MvccClock;
use turso_core::mvcc::database::*;
use turso_core::types::IOResult;
use turso_core::types::ImmutableRecord;
use turso_core::Connection;
use turso_core::Result;
use turso_core::{ConnectOpts, LimboError};

pub(crate) fn generate_simple_string_row(table_id: MVTableId, id: i64, data: &str) -> Row {
    let record =
        ImmutableRecord::from_values(&[Value::Text(Text::new(data.to_string()))], 1).unwrap();
    Row::new_table_row(RowID::new(table_id, RowKey::Int(id)), record.as_blob()).unwrap()
}

/// What this test checks: Startup recovery reconciles WAL/log artifacts into one consistent MVCC state and replay boundary.
/// Why this matters: This path runs automatically after crashes; errors here can duplicate effects or drop durable data.
#[test]
fn test_recovery_clock_monotonicity() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let max_commit_ts = {
        let conn = db.connect();
        conn.execute("CREATE TABLE test(id INTEGER PRIMARY KEY, data TEXT)")
            .unwrap();
        conn.execute("INSERT INTO test(id, data) VALUES (1, 'foo')")
            .unwrap();
        let mvcc_store = db.get_mvcc_store();
        mvcc_store.watermarks().last_committed_tx_ts
    };

    db.restart();
    let conn = db.connect();
    let pager = conn.get_pager();
    let mvcc_store = db.get_mvcc_store();
    let tx_id = mvcc_store.begin_tx(pager).unwrap();
    let tx = mvcc_store
        .tx_snapshot(tx_id)
        .expect("transaction should exist");
    assert!(
        tx.begin_ts > max_commit_ts,
        "expected begin_ts {} to be > max_commit_ts {}",
        tx.begin_ts,
        max_commit_ts
    );
}

/// What this test checks: Recovery after checkpoint (empty log) seeds new tx timestamps above durable metadata boundary.
/// Why this matters: Timestamp rewind below checkpointed boundary would break MVCC ordering.
#[test]
fn test_empty_log_recovery_loads_checkpoint_watermark() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let persistent_tx_ts_max = {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
        let mvcc_store = db.get_mvcc_store();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        assert_eq!(
            mvcc_store.get_logical_log_file().size().unwrap(),
            0,
            "logical log should be truncated to 0 after checkpoint"
        );
        let meta = get_rows(
            &conn,
            "SELECT v FROM __turso_internal_mvcc_meta WHERE k = 'persistent_tx_ts_max'",
        );
        assert_eq!(meta.len(), 1);
        meta[0][0].as_int().unwrap() as u64
    };

    db.restart();
    let conn = db.connect();
    let pager = conn.get_pager();
    let mvcc_store = db.get_mvcc_store();
    let tx_id = mvcc_store.begin_tx(pager).unwrap();
    let tx = mvcc_store
        .tx_snapshot(tx_id)
        .expect("transaction should exist");
    assert!(
        tx.begin_ts > persistent_tx_ts_max,
        "expected begin_ts {} > persistent_tx_ts_max {}",
        tx.begin_ts,
        persistent_tx_ts_max
    );
}

/// What this test checks: Core MVCC read/write semantics hold for this operation sequence.
/// Why this matters: These are foundational invariants; regressions here invalidate higher-level SQL behavior.
#[test]
fn test_insert_read() {
    let db = MvccTestDb::new();

    let tx1 = db.mvcc_store.begin_tx(db.conn.get_pager()).unwrap();
    let tx1_row = generate_simple_string_row((-2).into(), 1, "Hello");
    db.mvcc_store.insert(tx1, tx1_row.clone()).unwrap();
    let row = db
        .mvcc_store
        .read(
            tx1,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    let tx2 = db.mvcc_store.begin_tx(db.conn.get_pager()).unwrap();
    let row = db
        .mvcc_store
        .read(
            tx2,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);
}

/// What this test checks: Core MVCC read/write semantics hold for this operation sequence.
/// Why this matters: These are foundational invariants; regressions here invalidate higher-level SQL behavior.
#[test]
fn test_read_nonexistent() {
    let db = MvccTestDb::new();
    let tx = db.mvcc_store.begin_tx(db.conn.get_pager()).unwrap();
    let row = db.mvcc_store.read(
        tx,
        &RowID {
            table_id: (-2).into(),
            row_id: RowKey::Int(1),
        },
    );
    assert!(row.unwrap().is_none());
}

/// What this test checks: Core MVCC read/write semantics hold for this operation sequence.
/// Why this matters: These are foundational invariants; regressions here invalidate higher-level SQL behavior.
#[test]
fn test_delete() {
    let db = MvccTestDb::new();

    let tx1 = db.mvcc_store.begin_tx(db.conn.get_pager()).unwrap();
    let tx1_row = generate_simple_string_row((-2).into(), 1, "Hello");
    db.mvcc_store.insert(tx1, tx1_row.clone()).unwrap();
    let row = db
        .mvcc_store
        .read(
            tx1,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);
    db.mvcc_store
        .delete(
            tx1,
            RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap();
    let row = db
        .mvcc_store
        .read(
            tx1,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap();
    assert!(row.is_none());
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    let tx2 = db.mvcc_store.begin_tx(db.conn.get_pager()).unwrap();
    let row = db
        .mvcc_store
        .read(
            tx2,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap();
    assert!(row.is_none());
}

/// What this test checks: Core MVCC read/write semantics hold for this operation sequence.
/// Why this matters: These are foundational invariants; regressions here invalidate higher-level SQL behavior.
#[test]
fn test_delete_nonexistent() {
    let db = MvccTestDb::new();
    let tx = db.mvcc_store.begin_tx(db.conn.get_pager()).unwrap();
    assert!(!db
        .mvcc_store
        .delete(
            tx,
            RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1)
            },
        )
        .unwrap());
}

/// What this test checks: Core MVCC read/write semantics hold for this operation sequence.
/// Why this matters: These are foundational invariants; regressions here invalidate higher-level SQL behavior.
#[test]
fn test_commit() {
    let db = MvccTestDb::new();
    let tx1 = db.mvcc_store.begin_tx(db.conn.get_pager()).unwrap();
    let tx1_row = generate_simple_string_row((-2).into(), 1, "Hello");
    db.mvcc_store.insert(tx1, tx1_row.clone()).unwrap();
    let row = db
        .mvcc_store
        .read(
            tx1,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);
    let tx1_updated_row = generate_simple_string_row((-2).into(), 1, "World");
    db.mvcc_store.update(tx1, tx1_updated_row.clone()).unwrap();
    let row = db
        .mvcc_store
        .read(
            tx1,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_updated_row, row);
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    let tx2 = db.mvcc_store.begin_tx(db.conn.get_pager()).unwrap();
    let row = db
        .mvcc_store
        .read(
            tx2,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    commit_tx(db.mvcc_store.clone(), &db.conn, tx2).unwrap();
    assert_eq!(tx1_updated_row, row);
    db.mvcc_store.drop_unused_row_versions();
}

/// What this test checks: Rollback/savepoint behavior restores exactly the intended state when statements or transactions fail.
/// Why this matters: Partial rollback mistakes leave data in impossible intermediate states.
#[test]
fn test_rollback() {
    let db = MvccTestDb::new();
    let tx1 = db.mvcc_store.begin_tx(db.conn.get_pager()).unwrap();
    let row1 = generate_simple_string_row((-2).into(), 1, "Hello");
    db.mvcc_store.insert(tx1, row1.clone()).unwrap();
    let row2 = db
        .mvcc_store
        .read(
            tx1,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(row1, row2);
    let row3 = generate_simple_string_row((-2).into(), 1, "World");
    db.mvcc_store.update(tx1, row3.clone()).unwrap();
    let row4 = db
        .mvcc_store
        .read(
            tx1,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(row3, row4);
    db.mvcc_store
        .rollback_tx(tx1, db.conn.get_pager(), &db.conn, turso_core::MAIN_DB_ID);
    let tx2 = db.mvcc_store.begin_tx(db.conn.get_pager()).unwrap();
    let row5 = db
        .mvcc_store
        .read(
            tx2,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap();
    assert_eq!(row5, None);
}

/// What this test checks: MVCC transaction visibility and conflict handling follow the intended isolation behavior.
/// Why this matters: Concurrency bugs are correctness bugs: they create anomalies users can observe as wrong query results.
#[test]
fn test_dirty_write() {
    let db = MvccTestDb::new();

    // T1 inserts a row with ID 1, but does not commit.
    let tx1 = db.mvcc_store.begin_tx(db.conn.get_pager()).unwrap();
    let tx1_row = generate_simple_string_row((-2).into(), 1, "Hello");
    db.mvcc_store.insert(tx1, tx1_row.clone()).unwrap();
    let row = db
        .mvcc_store
        .read(
            tx1,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);

    let conn2 = db
        .db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    // T2 attempts to delete row with ID 1, but fails because T1 has not committed.
    let tx2 = db.mvcc_store.begin_tx(conn2.get_pager()).unwrap();
    let tx2_row = generate_simple_string_row((-2).into(), 1, "World");
    assert!(!db.mvcc_store.update(tx2, tx2_row).unwrap());

    let row = db
        .mvcc_store
        .read(
            tx1,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);
}

/// What this test checks: MVCC transaction visibility and conflict handling follow the intended isolation behavior.
/// Why this matters: Concurrency bugs are correctness bugs: they create anomalies users can observe as wrong query results.
#[test]
fn test_dirty_read() {
    let db = MvccTestDb::new();

    // T1 inserts a row with ID 1, but does not commit.
    let tx1 = db.mvcc_store.begin_tx(db.conn.get_pager()).unwrap();
    let row1 = generate_simple_string_row((-2).into(), 1, "Hello");
    db.mvcc_store.insert(tx1, row1).unwrap();

    // T2 attempts to read row with ID 1, but doesn't see one because T1 has not committed.
    let conn2 = db
        .db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    let tx2 = db.mvcc_store.begin_tx(conn2.get_pager()).unwrap();
    let row2 = db
        .mvcc_store
        .read(
            tx2,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap();
    assert_eq!(row2, None);
}

/// What this test checks: MVCC transaction visibility and conflict handling follow the intended isolation behavior.
/// Why this matters: Concurrency bugs are correctness bugs: they create anomalies users can observe as wrong query results.
#[test]
fn test_dirty_read_deleted() {
    let db = MvccTestDb::new();

    // T1 inserts a row with ID 1 and commits.
    let tx1 = db.mvcc_store.begin_tx(db.conn.get_pager()).unwrap();
    let tx1_row = generate_simple_string_row((-2).into(), 1, "Hello");
    db.mvcc_store.insert(tx1, tx1_row.clone()).unwrap();
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    // T2 deletes row with ID 1, but does not commit.
    let conn2 = db
        .db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    let tx2 = db.mvcc_store.begin_tx(conn2.get_pager()).unwrap();
    assert!(db
        .mvcc_store
        .delete(
            tx2,
            RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1)
            },
        )
        .unwrap());

    // T3 reads row with ID 1, but doesn't see the delete because T2 hasn't committed.
    let conn3 = db
        .db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    let tx3 = db.mvcc_store.begin_tx(conn3.get_pager()).unwrap();
    let row = db
        .mvcc_store
        .read(
            tx3,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);
}

/// What this test checks: Core MVCC read/write semantics hold for this operation sequence.
/// Why this matters: These are foundational invariants; regressions here invalidate higher-level SQL behavior.
#[test]
fn test_fuzzy_read() {
    let db = MvccTestDb::new();

    // T1 inserts a row with ID 1 and commits.
    let tx1 = db.mvcc_store.begin_tx(db.conn.get_pager()).unwrap();
    let tx1_row = generate_simple_string_row((-2).into(), 1, "First");
    db.mvcc_store.insert(tx1, tx1_row.clone()).unwrap();
    let row = db
        .mvcc_store
        .read(
            tx1,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    // T2 reads the row with ID 1 within an active transaction.
    let conn2 = db
        .db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    let tx2 = db.mvcc_store.begin_tx(conn2.get_pager()).unwrap();
    let row = db
        .mvcc_store
        .read(
            tx2,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);

    // T3 updates the row and commits.
    let conn3 = db
        .db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    let tx3 = db.mvcc_store.begin_tx(conn3.get_pager()).unwrap();
    let tx3_row = generate_simple_string_row((-2).into(), 1, "Second");
    db.mvcc_store.update(tx3, tx3_row).unwrap();
    commit_tx(db.mvcc_store.clone(), &conn3, tx3).unwrap();

    // T2 still reads the same version of the row as before.
    let row = db
        .mvcc_store
        .read(
            tx2,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);

    // T2 tries to update the row, but fails because T3 has already committed an update to the row,
    // so T2 trying to write would violate snapshot isolation if it succeeded.
    let tx2_newrow = generate_simple_string_row((-2).into(), 1, "Third");
    let update_result = db.mvcc_store.update(tx2, tx2_newrow);
    assert!(matches!(update_result, Err(LimboError::WriteWriteConflict)));
}

/// What this test checks: MVCC transaction visibility and conflict handling follow the intended isolation behavior.
/// Why this matters: Concurrency bugs are correctness bugs: they create anomalies users can observe as wrong query results.
#[test]
fn test_lost_update() {
    let db = MvccTestDb::new();

    // T1 inserts a row with ID 1 and commits.
    let tx1 = db.mvcc_store.begin_tx(db.conn.get_pager()).unwrap();
    let tx1_row = generate_simple_string_row((-2).into(), 1, "Hello");
    db.mvcc_store.insert(tx1, tx1_row.clone()).unwrap();
    let row = db
        .mvcc_store
        .read(
            tx1,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    // T2 attempts to update row ID 1 within an active transaction.
    let conn2 = db
        .db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    let tx2 = db.mvcc_store.begin_tx(conn2.get_pager()).unwrap();
    let tx2_row = generate_simple_string_row((-2).into(), 1, "World");
    assert!(db.mvcc_store.update(tx2, tx2_row.clone()).unwrap());

    // T3 also attempts to update row ID 1 within an active transaction.
    let conn3 = db
        .db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    let tx3 = db.mvcc_store.begin_tx(conn3.get_pager()).unwrap();
    let tx3_row = generate_simple_string_row((-2).into(), 1, "Hello, world!");
    assert!(matches!(
        db.mvcc_store.update(tx3, tx3_row),
        Err(LimboError::WriteWriteConflict)
    ));
    // hack: in the actual tursodb database we rollback the mvcc tx ourselves, so manually roll it back here
    db.mvcc_store
        .rollback_tx(tx3, conn3.get_pager(), &conn3, turso_core::MAIN_DB_ID);

    commit_tx(db.mvcc_store.clone(), &conn2, tx2).unwrap();
    assert!(matches!(
        commit_tx(db.mvcc_store.clone(), &conn3, tx3),
        Err(LimboError::TxTerminated)
    ));

    let conn4 = db
        .db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    let tx4 = db.mvcc_store.begin_tx(conn4.get_pager()).unwrap();
    let row = db
        .mvcc_store
        .read(
            tx4,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx2_row, row);
}

// Test for the visibility to check if a new transaction can see old committed values.
// This test checks for the typo present in the paper, explained in https://github.com/penberg/mvcc-rs/issues/15
#[test]
fn test_committed_visibility() {
    let db = MvccTestDb::new();

    // let's add $10 to my account since I like money
    let tx1 = db.mvcc_store.begin_tx(db.conn.get_pager()).unwrap();
    let tx1_row = generate_simple_string_row((-2).into(), 1, "10");
    db.mvcc_store.insert(tx1, tx1_row.clone()).unwrap();
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    // but I like more money, so let me try adding $10 more
    let conn2 = db
        .db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    let tx2 = db.mvcc_store.begin_tx(conn2.get_pager()).unwrap();
    let tx2_row = generate_simple_string_row((-2).into(), 1, "20");
    assert!(db.mvcc_store.update(tx2, tx2_row.clone()).unwrap());
    let row = db
        .mvcc_store
        .read(
            tx2,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(row, tx2_row);

    // can I check how much money I have?
    let conn3 = db
        .db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    let tx3 = db.mvcc_store.begin_tx(conn3.get_pager()).unwrap();
    let row = db
        .mvcc_store
        .read(
            tx3,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);
}

// Test to check if a older transaction can see (un)committed future rows
#[test]
fn test_future_row() {
    let db = MvccTestDb::new();

    let tx1 = db.mvcc_store.begin_tx(db.conn.get_pager()).unwrap();

    let conn2 = db
        .db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    let tx2 = db.mvcc_store.begin_tx(conn2.get_pager()).unwrap();
    let tx2_row = generate_simple_string_row((-2).into(), 1, "Hello");
    db.mvcc_store.insert(tx2, tx2_row).unwrap();

    // transaction in progress, so tx1 shouldn't be able to see the value
    let row = db
        .mvcc_store
        .read(
            tx1,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap();
    assert_eq!(row, None);

    // lets commit the transaction and check if tx1 can see it
    commit_tx(db.mvcc_store.clone(), &conn2, tx2).unwrap();
    let row = db
        .mvcc_store
        .read(
            tx1,
            &RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap();
    assert_eq!(row, None);
}

use turso_core::mvcc::database::{MvStore, Row, RowID};

use turso_core::security::roles::SessionRole;

use turso_core::types::Text;

use turso_core::Value;

use turso_core::ValueRef;

pub(crate) fn commit_tx(
    mv_store: Arc<
        turso_core::mvcc::MvStore<turso_core::mvcc::MvccClock, turso_core::alloc::DynAllocator>,
    >,
    conn: &Arc<Connection>,
    tx_id: u64,
) -> Result<()> {
    let mut sm = mv_store
        .commit_tx(tx_id, conn, turso_core::MAIN_DB_ID)
        .unwrap();
    // TODO: sync IO hack
    loop {
        let res = sm.step(&mv_store)?;
        match res {
            IOResult::IO(io) => {
                io.wait(conn.db.io.as_ref())?;
            }
            IOResult::Done(_) => break,
        }
    }
    assert!(sm.is_finalized());
    Ok(())
}

#[test]
fn test_sequence_watermark_tracks_lowest_active_allocation() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    let mv_store = db.get_mvcc_store();
    let pager = conn.get_pager();

    mv_store.set_sequence_watermark("turso_cdc_pk_autoincrement", 13);
    let tx1 = mv_store.begin_tx(pager.clone()).unwrap();
    let tx2 = mv_store.begin_tx(pager.clone()).unwrap();
    mv_store
        .register_sequence_allocation(tx1, "turso_cdc_pk_autoincrement", 10)
        .unwrap();
    mv_store
        .register_sequence_allocation(tx2, "turso_cdc_pk_autoincrement", 12)
        .unwrap();
    mv_store
        .register_sequence_allocation(tx1, "turso_cdc_pk_autoincrement", 11)
        .unwrap();

    assert_eq!(
        mv_store.sequence_watermark("turso_cdc_pk_autoincrement"),
        Some(10)
    );

    commit_tx(mv_store.clone(), &conn, tx1).unwrap();
    assert_eq!(
        mv_store.sequence_watermark("turso_cdc_pk_autoincrement"),
        Some(12)
    );

    mv_store.rollback_tx(tx2, pager, conn.as_ref(), turso_core::MAIN_DB_ID);
    assert_eq!(
        mv_store.sequence_watermark("turso_cdc_pk_autoincrement"),
        Some(13)
    );
}

#[test]
fn test_sequence_watermark_function_returns_current_watermark_without_active_allocations() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    let mv_store = db.get_mvcc_store();
    let pager = conn.get_pager();

    conn.execute("CREATE SEQUENCE s").unwrap();
    mv_store.set_sequence_watermark("s", 42);
    let rows = get_rows(&conn, "SELECT sequence_watermark_experimental('s')");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 42);

    let tx_id = mv_store.begin_tx(pager.clone()).unwrap();
    mv_store
        .register_sequence_allocation(tx_id, "s", 10)
        .unwrap();
    let rows = get_rows(&conn, "SELECT sequence_watermark_experimental('s')");

    assert_eq!(rows[0][0].as_int().unwrap(), 10);

    mv_store.rollback_tx(tx_id, pager, conn.as_ref(), turso_core::MAIN_DB_ID);
    let rows = get_rows(&conn, "SELECT sequence_watermark_experimental('s')");

    assert_eq!(rows[0][0].as_int().unwrap(), 42);
}

/// Insert a synthetic table and a single row via the MVCC store, then commit.
/// Used by restart / recovery tests to seed durable state before a restart cycle.
pub(super) fn write_synthetic_row(db: &MvccTestDbNoConn, value: &str) {
    let conn = db.connect();
    let mvcc_store = db.get_mvcc_store();
    let max_root_page = get_rows(
        &conn,
        "SELECT COALESCE(MAX(rootpage), 0) FROM sqlite_schema WHERE rootpage > 0",
    )[0][0]
        .as_int()
        .unwrap();
    let next_schema_rowid = get_rows(
        &conn,
        "SELECT COALESCE(MAX(rowid), 0) + 1 FROM sqlite_schema",
    )[0][0]
        .as_int()
        .unwrap();
    let synthetic_root = -(max_root_page + 100);
    let synthetic_table_id = MVTableId::new(synthetic_root);
    let tx_id = mvcc_store.begin_tx(conn.get_pager()).unwrap();
    let data = ImmutableRecord::from_values(
        &[
            Value::Text(Text::new("table")),
            Value::Text(Text::new("test")),
            Value::Text(Text::new("test")),
            Value::from_i64(synthetic_root),
            Value::Text(Text::new(
                "CREATE TABLE test(id INTEGER PRIMARY KEY, data TEXT)",
            )),
        ],
        5,
    )
    .unwrap();
    mvcc_store
        .insert(
            tx_id,
            Row::new_table_row(
                RowID::new((-1).into(), RowKey::Int(next_schema_rowid)),
                data.as_blob(),
            )
            .unwrap(),
        )
        .unwrap();
    let row = generate_simple_string_row(synthetic_table_id, 1, value);
    mvcc_store.insert(tx_id, row).unwrap();
    commit_tx(mvcc_store, &conn, tx_id).unwrap();
    conn.close().unwrap();
}

/// What this test checks: Startup recovery reconciles WAL/log artifacts into one consistent MVCC state and replay boundary.
/// Why this matters: This path runs automatically after crashes; errors here can duplicate effects or drop durable data.
#[test]
fn test_restart() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    write_synthetic_row(&db, "foo");
    db.restart();

    {
        let conn = db.connect();
        let mvcc_store = db.get_mvcc_store();
        let max_root_page = get_rows(
            &conn,
            "SELECT COALESCE(MAX(rootpage), 0) FROM sqlite_schema WHERE rootpage > 0",
        )[0][0]
            .as_int()
            .unwrap();
        let synthetic_table_id = MVTableId::new(-(max_root_page + 100));
        let tx_id = mvcc_store.begin_tx(conn.get_pager()).unwrap();
        let row = generate_simple_string_row(synthetic_table_id, 2, "bar");

        mvcc_store.insert(tx_id, row).unwrap();
        commit_tx(mvcc_store.clone(), &conn, tx_id).unwrap();

        let tx_id = mvcc_store.begin_tx(conn.get_pager()).unwrap();
        let row = mvcc_store
            .read(tx_id, &RowID::new(synthetic_table_id, RowKey::Int(2)))
            .unwrap()
            .unwrap();
        let record = get_record_value(&row);
        match record.get_value(0).unwrap() {
            ValueRef::Text(text) => {
                assert_eq!(text.as_str(), "bar");
            }
            _ => panic!("Expected Text value"),
        }
        conn.close().unwrap();
    }
}

pub(super) fn get_record_value(row: &Row) -> ImmutableRecord {
    let mut record = ImmutableRecord::new(1024).unwrap();
    record.start_serialization(row.payload()).unwrap();
    record
}

/// What this test checks: Checkpoint transitions preserve DB/WAL/log ordering and watermark updates for the tested edge case.
/// Why this matters: Incorrect ordering breaks crash safety, replay boundaries, or durability guarantees.
#[test]
fn test_auto_checkpoint_busy_is_ignored() {
    let db = MvccTestDb::new();
    db.mvcc_store.set_checkpoint_threshold(0);

    // Keep a second transaction open to hold the checkpoint read lock.
    let tx1 = db.mvcc_store.begin_tx(db.conn.get_pager()).unwrap();
    let tx2 = db.mvcc_store.begin_tx(db.conn.get_pager()).unwrap();

    let row = generate_simple_string_row((-2).into(), 1, "Hello");
    db.mvcc_store.insert(tx1, row).unwrap();

    // Regression: auto-checkpoint returning Busy used to bubble up and cause
    // statement abort/rollback after the tx was removed.
    // Commit should succeed even if the auto-checkpoint is busy.
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    // Cleanup: release the read lock held by tx2.
    db.mvcc_store
        .rollback_tx(tx2, db.conn.get_pager(), &db.conn, turso_core::MAIN_DB_ID);
}

/// Garbage collection removes only versions that are provably unreachable and keeps versions still required for visibility and safety.
#[test]
/// End-to-end at the MvStore level: insert a row, commit, and run GC. Without a
/// checkpoint the version is not yet in the B-tree, so Rule 3 doesn't fire and
/// the version survives. Verifies the full insert→commit→GC pipeline.
fn test_gc_integration_insert_commit_gc() {
    let db = MvccTestDb::new();

    // Insert and commit a row.
    let tx1 = db.mvcc_store.begin_tx(db.conn.get_pager()).unwrap();
    let row = generate_simple_string_row((-2).into(), 1, "gc_test");
    db.mvcc_store.insert(tx1, row).unwrap();
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    // Row should be in the MvStore.
    assert!(!db.mvcc_store.rows.is_empty());

    // No active transactions → LWM = u64::MAX.
    // ckpt_max = 0 (no checkpoint yet), so rule 3 won't fire (b > ckpt_max).
    let dropped = db.mvcc_store.drop_unused_row_versions();
    assert_eq!(dropped, 0);
    assert!(!db.mvcc_store.rows.is_empty());
}

#[test]
fn transaction_rollback_removes_created_versions_immediately() {
    let db = MvccTestDb::new();
    let row_id = RowID::new((-2).into(), RowKey::Int(1));

    for _ in 0..100 {
        let tx = db.mvcc_store.begin_tx(db.conn.get_pager()).unwrap();
        let row = generate_simple_string_row((-2).into(), 1, "will_rollback");
        db.mvcc_store.insert(tx, row).unwrap();
        db.mvcc_store
            .rollback_tx(tx, db.conn.get_pager(), &db.conn, turso_core::MAIN_DB_ID);
    }

    let entry = db.mvcc_store.rows.get(&row_id);
    assert!(entry.is_some());
    assert!(entry.unwrap().value().read().is_empty());
    assert_eq!(db.mvcc_store.live_version_count_approx(), 0);
    let dropped = db.mvcc_store.drop_unused_row_versions();
    assert_eq!(dropped, 0);
}

/// Most chains hold one or two versions, so a short chain must not keep the
/// four-slot minimum capacity of amortized `Vec` growth.
#[test]
fn test_short_version_chain_has_no_unused_capacity() {
    let db = MvccTestDb::new();
    let table_id: MVTableId = (-2).into();
    let row_id = RowID::new(table_id, RowKey::Int(1));
    let tx = db.mvcc_store.begin_tx(db.conn.get_pager()).unwrap();
    db.mvcc_store
        .insert(tx, generate_simple_string_row(table_id, 1, "version_0"))
        .unwrap();
    let chain_len_and_capacity = || {
        let entry = db.mvcc_store.rows.get(&row_id).unwrap();
        let versions = entry.value().read();
        (versions.len(), versions.capacity())
    };
    assert_eq!(chain_len_and_capacity(), (1, 1));

    for version in 1..4 {
        let row = generate_simple_string_row(table_id, 1, &format!("version_{version}"));
        assert!(db.mvcc_store.update(tx, row).unwrap());
        assert_eq!(chain_len_and_capacity(), (version + 1, version + 1));
    }
}

/// The low-water mark (LWM) is the minimum begin_ts of all active readers. GC
/// must not remove any version that an active reader might still need. This test
/// opens a reader, writes a new version that supersedes the reader's snapshot,
/// and runs GC — the old version must survive. After the reader closes, GC runs
/// again and reclaims it. This is the core safety property of LWM-based GC.
#[test]
fn test_gc_active_reader_pins_lwm() {
    let db = MvccTestDb::new();
    let table_id: MVTableId = (-2).into();

    // T1 inserts a row and commits.
    let tx1 = db.mvcc_store.begin_tx(db.conn.get_pager()).unwrap();
    let row_v1 = generate_simple_string_row(table_id, 1, "version_1");
    db.mvcc_store.insert(tx1, row_v1.clone()).unwrap();
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    // T2 begins a read transaction — pins LWM at T2's begin_ts.
    let conn2 = db
        .db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    let tx2 = db.mvcc_store.begin_tx(conn2.get_pager()).unwrap();
    let tx2_begin_ts = db.mvcc_store.tx_snapshot(tx2).unwrap().begin_ts;

    // T3 updates the row and commits, creating a superseded version.
    let conn3 = db
        .db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    let tx3 = db.mvcc_store.begin_tx(conn3.get_pager()).unwrap();
    let row_v2 = generate_simple_string_row(table_id, 1, "version_2");
    db.mvcc_store.update(tx3, row_v2).unwrap();
    commit_tx(db.mvcc_store.clone(), &conn3, tx3).unwrap();

    // LWM should be T2's begin_ts (the active reader).
    let lwm = db.mvcc_store.compute_lwm();
    assert_eq!(
        lwm, tx2_begin_ts,
        "LWM should equal the active reader's begin_ts"
    );

    // GC should NOT remove the superseded version (its end_ts > lwm).
    let row_id = RowID::new(table_id, RowKey::Int(1));
    let dropped = db.mvcc_store.drop_unused_row_versions();
    assert_eq!(
        dropped, 0,
        "GC should not remove versions visible to active reader"
    );
    {
        let entry = db.mvcc_store.rows.get(&row_id).unwrap();
        let versions = entry.value().read();
        assert_eq!(versions.len(), 2, "both versions should be retained");
    }

    // T2 still sees the old version.
    let read_row = db.mvcc_store.read(tx2, &row_id).unwrap().unwrap();
    assert_eq!(
        read_row, row_v1,
        "active reader should still see the old version"
    );

    // Close the reader transaction.
    db.mvcc_store.remove_tx(tx2);

    // LWM should now be u64::MAX.
    assert_eq!(db.mvcc_store.compute_lwm(), u64::MAX);

    // GC should now remove the superseded version.
    let dropped = db.mvcc_store.drop_unused_row_versions();
    assert_eq!(
        dropped, 1,
        "superseded version should be reclaimed after reader closes"
    );
    {
        let entry = db.mvcc_store.rows.get(&row_id).unwrap();
        let versions = entry.value().read();
        assert_eq!(versions.len(), 1, "only current version should remain");
    }
}

/// The live-version counter is the heuristic that gates inline GC. It must
/// increment on every committed insert/update and decrement when GC reclaims a
/// version. Exactness here is convenient for the test but only approximate
/// accuracy is required by the engine.
#[test]
fn test_live_version_count_approx_tracks_inserts_and_gc() {
    let db = MvccTestDb::new();
    let table_id: MVTableId = (-2).into();
    let start = db.mvcc_store.live_version_count_approx();

    // Insert a row + commit -> one live version.
    let tx = db.mvcc_store.begin_tx(db.conn.get_pager()).unwrap();
    db.mvcc_store
        .insert(tx, generate_simple_string_row(table_id, 1, "v1"))
        .unwrap();
    commit_tx(db.mvcc_store.clone(), &db.conn, tx).unwrap();
    assert_eq!(db.mvcc_store.live_version_count_approx(), start + 1);

    // Update the row + commit -> a second version (old one superseded, not
    // removed, so the count grows to two).
    let tx = db.mvcc_store.begin_tx(db.conn.get_pager()).unwrap();
    db.mvcc_store
        .update(tx, generate_simple_string_row(table_id, 1, "v2"))
        .unwrap();
    commit_tx(db.mvcc_store.clone(), &db.conn, tx).unwrap();
    assert_eq!(db.mvcc_store.live_version_count_approx(), start + 2);

    // No active readers -> the superseded version is reclaimable. GC drops it
    // and the counter follows.
    let dropped = db.mvcc_store.drop_unused_row_versions();
    assert_eq!(dropped, 1);
    assert_eq!(db.mvcc_store.live_version_count_approx(), start + 1);
}

/// `should_gc` fires once live-version growth since the last pass crosses the
/// threshold; a pass resets the baseline; -1 disables it entirely.
#[test]
fn test_should_gc_threshold_and_reset() {
    let db = MvccTestDb::new();
    let table_id: MVTableId = (-2).into();

    db.mvcc_store.set_gc_threshold(5);
    assert_eq!(db.mvcc_store.gc_threshold(), 5);
    assert!(!db.mvcc_store.should_gc());

    // Insert 5 versions in a single open (uncommitted) transaction so we drive
    // the counter directly without going through the commit path's own inline
    // GC trigger.
    let tx = db.mvcc_store.begin_tx(db.conn.get_pager()).unwrap();
    for i in 1..=5 {
        db.mvcc_store
            .insert(tx, generate_simple_string_row(table_id, i, "x"))
            .unwrap();
    }
    assert!(
        db.mvcc_store.should_gc(),
        "growth of 5 versions should reach the threshold"
    );

    // A pass resets the baseline (regardless of how much it reclaimed — the
    // open txn pins the LWM, so nothing is reclaimable here).
    db.mvcc_store
        .gc_incremental(MvStore::<MvccClock>::MAX_CHAINS_PER_GC);
    assert!(
        !db.mvcc_store.should_gc(),
        "baseline should reset after a GC pass"
    );

    // A negative threshold disables inline GC even past the old threshold.
    db.mvcc_store.set_gc_threshold(-1);
    for i in 6..=20 {
        db.mvcc_store
            .insert(tx, generate_simple_string_row(table_id, i, "x"))
            .unwrap();
    }
    assert!(
        !db.mvcc_store.should_gc(),
        "negative threshold disables inline GC"
    );
}

/// Incremental GC must reclaim exactly what the full sweep would, just spread
/// across resumable bounded passes whose cursor wraps at the end.
#[test]
fn test_gc_incremental_reclaims_like_full_sweep() {
    let db = MvccTestDb::new();
    let table_id: MVTableId = (-2).into();

    // Build many chains, each with one reclaimable superseded version
    // (insert+commit then update+commit).
    let n: i64 = 20;
    for i in 1..=n {
        let tx = db.mvcc_store.begin_tx(db.conn.get_pager()).unwrap();
        db.mvcc_store
            .insert(tx, generate_simple_string_row(table_id, i, "v1"))
            .unwrap();
        commit_tx(db.mvcc_store.clone(), &db.conn, tx).unwrap();

        let tx = db.mvcc_store.begin_tx(db.conn.get_pager()).unwrap();
        db.mvcc_store
            .update(tx, generate_simple_string_row(table_id, i, "v2"))
            .unwrap();
        commit_tx(db.mvcc_store.clone(), &db.conn, tx).unwrap();
    }

    // No active readers -> every superseded version is reclaimable.
    assert_eq!(db.mvcc_store.compute_lwm(), u64::MAX);

    // Drive GC in tiny chunks (3 chains/pass) until it converges: no versions
    // reclaimed AND the cursor has wrapped back to the start.
    let mut total = 0;
    let mut passes = 0;
    loop {
        let dropped = db.mvcc_store.gc_incremental(3);
        total += dropped;
        passes += 1;
        if dropped == 0 && !db.mvcc_store.debug_gc_snapshot().table_sweep_unfinished {
            break;
        }
        assert!(passes < 10_000, "incremental GC failed to converge");
    }
    assert!(
        total >= n as usize,
        "incremental GC should reclaim at least the {n} superseded versions, got {total}"
    );

    // A full sweep now finds nothing: incremental GC already reclaimed
    // everything the full sweep would have.
    assert_eq!(
        db.mvcc_store.drop_unused_row_versions(),
        0,
        "full sweep should find nothing left after incremental GC converges"
    );

    // Each chain keeps exactly its surviving current version.
    for i in 1..=n {
        let entry = db
            .mvcc_store
            .rows
            .get(&RowID::new(table_id, RowKey::Int(i)))
            .unwrap();
        assert_eq!(entry.value().read().len(), 1);
    }
}

/// Concurrency safety. Many connections share one `MvStore` and each commit
/// calls `gc_incremental` with no global lock held, so several threads can run
/// GC at once. The single-flight gate keeps only one pass active, but the
/// stronger guarantee is that concurrent passes can never corrupt a chain or
/// the live-version counter. Hammer GC from many threads, then assert the
/// outcome equals a single full sweep and that the (heuristic) counter still
/// exactly matches the live versions — proving no double/missed decrement.
#[test]
fn test_gc_incremental_concurrent_is_safe() {
    let db = MvccTestDb::new();
    let table_id: MVTableId = (-2).into();

    // Each row: insert+commit (v1) then update+commit (v2) -> one reclaimable
    // superseded version per chain. No readers stay open, so LWM = MAX.
    let n: i64 = 200;
    for i in 1..=n {
        let tx = db.mvcc_store.begin_tx(db.conn.get_pager()).unwrap();
        db.mvcc_store
            .insert(tx, generate_simple_string_row(table_id, i, "v1"))
            .unwrap();
        commit_tx(db.mvcc_store.clone(), &db.conn, tx).unwrap();

        let tx = db.mvcc_store.begin_tx(db.conn.get_pager()).unwrap();
        db.mvcc_store
            .update(tx, generate_simple_string_row(table_id, i, "v2"))
            .unwrap();
        commit_tx(db.mvcc_store.clone(), &db.conn, tx).unwrap();
    }
    assert_eq!(db.mvcc_store.compute_lwm(), u64::MAX);

    // 8 threads each run many small bounded passes concurrently.
    let mut handles = Vec::new();
    for _ in 0..8 {
        let store = db.mvcc_store.clone();
        handles.push(std::thread::spawn(move || {
            for _ in 0..500 {
                store.gc_incremental(8);
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    // Drain anything the bounded passes didn't reach (single-threaded now).
    while db
        .mvcc_store
        .gc_incremental(MvStore::<MvccClock>::MAX_CHAINS_PER_GC)
        > 0
    {}

    // A full sweep finds nothing left, and each chain kept exactly its current
    // version — concurrent GC reclaimed precisely what one sweep would.
    assert_eq!(db.mvcc_store.drop_unused_row_versions(), 0);
    for i in 1..=n {
        let entry = db
            .mvcc_store
            .rows
            .get(&RowID::new(table_id, RowKey::Int(i)))
            .unwrap();
        assert_eq!(entry.value().read().len(), 1);
    }

    // The heuristic counter exactly matches the live versions still in memory:
    // no decrement was lost or double-applied under concurrency.
    let table_live: usize = db
        .mvcc_store
        .rows
        .iter()
        .map(|e| e.value().read().len())
        .sum();
    let index_live: usize = db
        .mvcc_store
        .index_rows
        .iter()
        .map(|outer| {
            outer
                .value()
                .iter()
                .map(|inner| inner.value().read().len())
                .sum::<usize>()
        })
        .sum();
    assert_eq!(
        db.mvcc_store.live_version_count_approx(),
        table_live + index_live,
        "live-version counter drifted from actual chain contents"
    );
}

/// Overlapping transactions: a committed-and-superseded version is reclaimed by
/// inline incremental GC (no checkpoint), but only after the snapshot that
/// pinned the LWM ends. While the reader is open, GC must not touch its version.
#[test]
fn test_gc_incremental_respects_held_snapshot() {
    let db = MvccTestDb::new();
    let table_id: MVTableId = (-2).into();
    let row_id = RowID::new(table_id, RowKey::Int(1));

    // T1 inserts and commits v1.
    let tx1 = db.mvcc_store.begin_tx(db.conn.get_pager()).unwrap();
    let row_v1 = generate_simple_string_row(table_id, 1, "version_1");
    db.mvcc_store.insert(tx1, row_v1.clone()).unwrap();
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    // T2 opens a read snapshot and pins the LWM.
    let conn2 = db
        .db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    let tx2 = db.mvcc_store.begin_tx(conn2.get_pager()).unwrap();

    // T3 updates and commits v2, superseding v1.
    let conn3 = db
        .db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    let tx3 = db.mvcc_store.begin_tx(conn3.get_pager()).unwrap();
    db.mvcc_store
        .update(tx3, generate_simple_string_row(table_id, 1, "version_2"))
        .unwrap();
    commit_tx(db.mvcc_store.clone(), &conn3, tx3).unwrap();

    // Incremental GC while T2 is open must NOT reclaim v1.
    assert_eq!(
        db.mvcc_store
            .gc_incremental(MvStore::<MvccClock>::MAX_CHAINS_PER_GC),
        0,
        "held snapshot pins the LWM; nothing reclaimable"
    );
    assert_eq!(
        db.mvcc_store
            .rows
            .get(&row_id)
            .unwrap()
            .value()
            .read()
            .len(),
        2
    );
    // T2 still sees its snapshot version.
    assert_eq!(db.mvcc_store.read(tx2, &row_id).unwrap().unwrap(), row_v1);

    // Close the snapshot; now incremental GC reclaims the superseded v1
    // without any checkpoint having run.
    db.mvcc_store.remove_tx(tx2);
    assert_eq!(
        db.mvcc_store
            .gc_incremental(MvStore::<MvccClock>::MAX_CHAINS_PER_GC),
        1,
        "superseded version reclaimed by inline GC after snapshot ends"
    );
    assert_eq!(
        db.mvcc_store
            .rows
            .get(&row_id)
            .unwrap()
            .value()
            .read()
            .len(),
        1,
        "only the current version remains"
    );
}

/// Regression test: speculative delete by an active transaction must not hide a
/// committed version from commit-time conflict checks.
///
/// Previously, `check_version_conflicts` skipped any version with `end.is_some()`,
/// including versions where `end` was `TxID` of an active (uncommitted) transaction.
/// This allowed T2 to commit without detecting the write-write conflict with T1.
///
/// Minimal reproduction using the MvStore API directly (no SQL, no restart, no UPSERT).
/// Note: T2 begins after T1 commits, so T2 *can* see T1 under SI. We call
/// `insert_btree_resident` directly to simulate the UPSERT code path where the
/// cursor doesn't go through normal read visibility.
///
/// Timeline:
///   T1: insert row 1, commit
///   T2: begin (will write later)
///   T3: begin, update row 1 → sets end=TxID(T3) on T1's version
///   T2: insert_btree_resident row 1 (via API, bypassing read visibility)
///   T2: commit → must detect conflict with T1
#[test]
fn test_speculative_delete_hides_committed_version() {
    let db = MvccTestDb::new();
    let table_id: MVTableId = (-2).into();

    // T1: insert row 1 and commit.
    let tx1 = db.mvcc_store.begin_tx(db.conn.get_pager()).unwrap();
    let row_v1 = generate_simple_string_row(table_id, 1, "v1");
    db.mvcc_store.insert(tx1, row_v1).unwrap();
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    // T2: begin (will write later).
    let conn2 = db
        .db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    let tx2 = db.mvcc_store.begin_tx(conn2.get_pager()).unwrap();

    // T3: begin, update row 1 → delete sets end=TxID(T3) on T1's version.
    let conn3 = db
        .db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    let tx3 = db.mvcc_store.begin_tx(conn3.get_pager()).unwrap();
    let row_v3 = generate_simple_string_row(table_id, 1, "v3");
    assert!(db.mvcc_store.update(tx3, row_v3).unwrap());

    // T2: insert_btree_resident for the same row (called directly via API to
    // simulate the UPSERT code path that bypasses eager conflict detection).
    let row_v2 = generate_simple_string_row(table_id, 1, "v2");
    db.mvcc_store
        .insert_btree_resident_to_table_or_index(tx2, row_v2, None)
        .unwrap();

    // T2: commit → must fail with WriteWriteConflict.
    let result = commit_tx(db.mvcc_store, &conn2, tx2);
    assert!(
        matches!(&result, Err(LimboError::WriteWriteConflict)),
        "Expected WriteWriteConflict, got: {result:?}. \
         T3's speculative delete (end=TxID) on T1's version must not hide it from conflict checks."
    );
}

/// Verify that a committed pure delete (tombstone) is detected as a conflict.
///
/// Scenario: Td deletes a row and commits. Between Td's Commit and CommitEnd
/// (when TxID→Timestamp conversion happens), the tombstone still has
/// end=TxID(Td). T2 does insert_btree_resident for the same row and tries to
/// commit. The tombstone's begin=None, end=TxID(Td) should be caught by the
/// B-tree tombstone check in check_version_conflicts.
///
/// Timeline:
///   T1: insert row 1, commit
///   T2: begin (will write later)
///   Td: delete row 1, commit
///   T2: insert_btree_resident row 1
///   T2: commit → must detect conflict with Td's tombstone
#[test]
fn test_committed_delete_tombstone_conflict() {
    let db = MvccTestDb::new();
    let table_id: MVTableId = (-2).into();

    // T1: insert row 1 and commit.
    let tx1 = db.mvcc_store.begin_tx(db.conn.get_pager()).unwrap();
    let row_v1 = generate_simple_string_row(table_id, 1, "v1");
    db.mvcc_store.insert(tx1, row_v1).unwrap();
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    // T2: begin (will write later).
    let conn2 = db
        .db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    let tx2 = db.mvcc_store.begin_tx(conn2.get_pager()).unwrap();

    // Td: delete row 1 and commit.
    let conn_d = db
        .db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    let tx_d = db.mvcc_store.begin_tx(conn_d.get_pager()).unwrap();
    assert!(db
        .mvcc_store
        .delete(tx_d, RowID::new(table_id, RowKey::Int(1)))
        .unwrap());
    commit_tx(db.mvcc_store.clone(), &conn_d, tx_d).unwrap();

    // T2: insert_btree_resident for the same row.
    let row_v2 = generate_simple_string_row(table_id, 1, "v2");
    db.mvcc_store
        .insert_btree_resident_to_table_or_index(tx2, row_v2, None)
        .unwrap();

    // T2: commit → must detect conflict with Td.
    let result = commit_tx(db.mvcc_store, &conn2, tx2);
    assert!(
        matches!(&result, Err(LimboError::WriteWriteConflict)),
        "Expected WriteWriteConflict, got: {result:?}. \
         Td's committed delete (tombstone) must be detected as a conflict."
    );
}

/// Verify that when a transaction (Td) updates a row and commits, another
/// transaction (T2) that also writes to the same row detects the conflict —
/// even though T1's version has end=TxID(Td) with Td committed.
///
/// This tests the `Committed(_) => continue` branch in `check_version_conflicts`:
/// skipping T1's version is safe because Td's NEW version (begin=TxID(Td)) catches
/// the conflict.
///
/// Timeline:
///   T1: insert row 1, commit
///   T2: begin (will write later)
///   Td: update row 1 (sets end=TxID(Td) on T1's version, creates new version), commit
///   T2: insert_btree_resident row 1
///   T2: commit → must detect conflict with Td's new version
#[test]
fn test_committed_update_version_conflict() {
    let db = MvccTestDb::new();
    let table_id: MVTableId = (-2).into();

    // T1: insert row 1 and commit.
    let tx1 = db.mvcc_store.begin_tx(db.conn.get_pager()).unwrap();
    let row_v1 = generate_simple_string_row(table_id, 1, "v1");
    db.mvcc_store.insert(tx1, row_v1).unwrap();
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    // T2: begin (will write later).
    let conn2 = db
        .db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    let tx2 = db.mvcc_store.begin_tx(conn2.get_pager()).unwrap();

    // Td: update row 1 and commit.
    let conn_d = db
        .db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    let tx_d = db.mvcc_store.begin_tx(conn_d.get_pager()).unwrap();
    let row_vd = generate_simple_string_row(table_id, 1, "vd");
    assert!(db.mvcc_store.update(tx_d, row_vd).unwrap());
    commit_tx(db.mvcc_store.clone(), &conn_d, tx_d).unwrap();

    // T2: insert_btree_resident for the same row.
    let row_v2 = generate_simple_string_row(table_id, 1, "v2");
    db.mvcc_store
        .insert_btree_resident_to_table_or_index(tx2, row_v2, None)
        .unwrap();

    // T2: commit → must detect conflict with Td.
    let result = commit_tx(db.mvcc_store, &conn2, tx2);
    assert!(
        matches!(&result, Err(LimboError::WriteWriteConflict)),
        "Expected WriteWriteConflict, got: {result:?}. \
         Td's committed update must be detected via Td's new version."
    );
}
