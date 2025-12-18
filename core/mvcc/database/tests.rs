use std::collections::HashSet;

use super::*;
use crate::io::PlatformIO;
use crate::mvcc::clock::LocalClock;
use crate::mvcc::cursor::MvccCursorType;
use crate::storage::sqlite3_ondisk::DatabaseHeader;
use parking_lot::RwLock;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

pub(crate) struct MvccTestDbNoConn {
    pub(crate) db: Option<Arc<Database>>,
    path: Option<String>,
    // Stored mainly to not drop the temp dir before the test is done.
    _temp_dir: Option<tempfile::TempDir>,
}
pub(crate) struct MvccTestDb {
    pub(crate) mvcc_store: Arc<MvStore<LocalClock>>,
    pub(crate) db: Arc<Database>,
    pub(crate) conn: Arc<Connection>,
}

impl MvccTestDb {
    pub fn new() -> Self {
        let io = Arc::new(MemoryIO::new());
        let db = Database::open_file(io.clone(), ":memory:", true).unwrap();
        let conn = db.connect().unwrap();
        let mvcc_store = db.get_mv_store().clone().unwrap();
        Self {
            mvcc_store,
            db,
            conn,
        }
    }
}

impl MvccTestDbNoConn {
    pub fn new() -> Self {
        let io = Arc::new(MemoryIO::new());
        let db = Database::open_file(io.clone(), ":memory:", true).unwrap();
        Self {
            db: Some(db),
            path: None,
            _temp_dir: None,
        }
    }

    /// Opens a database with a file
    pub fn new_with_random_db() -> Self {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let path = temp_dir
            .path()
            .join(format!("test_{}", rand::random::<u64>()));
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let io = Arc::new(PlatformIO::new().unwrap());
        println!("path: {}", path.as_os_str().to_str().unwrap());
        let db = Database::open_file(io.clone(), path.as_os_str().to_str().unwrap(), true).unwrap();
        Self {
            db: Some(db),
            path: Some(path.to_str().unwrap().to_string()),
            _temp_dir: Some(temp_dir),
        }
    }

    /// Restarts the database, make sure there is no connection to the database open before calling this!
    pub fn restart(&mut self) {
        // First let's clear any entries in database manager in order to force restart.
        // If not, we will load the same database instance again.
        {
            let mut manager = DATABASE_MANAGER.lock();
            manager.clear();
        }

        // Now open again.
        let io = Arc::new(PlatformIO::new().unwrap());
        let path = self.path.as_ref().unwrap();
        let db = Database::open_file(io.clone(), path, true).unwrap();
        self.db.replace(db);
    }

    /// Asumes there is a database open
    pub fn get_db(&self) -> Arc<Database> {
        self.db.as_ref().unwrap().clone()
    }

    pub fn connect(&self) -> Arc<Connection> {
        self.get_db().connect().unwrap()
    }

    pub fn get_mvcc_store(&self) -> Arc<MvStore<LocalClock>> {
        self.get_db().get_mv_store().clone().unwrap()
    }
}

pub(crate) fn generate_simple_string_row(table_id: MVTableId, id: i64, data: &str) -> Row {
    let record = ImmutableRecord::from_values(&[Value::Text(Text::new(data.to_string()))], 1);
    Row::new_table_row(
        RowID::new(table_id, RowKey::Int(id)),
        record.as_blob().to_vec(),
        1,
    )
}

pub(crate) fn generate_simple_string_record(data: &str) -> ImmutableRecord {
    ImmutableRecord::from_values(&[Value::Text(Text::new(data.to_string()))], 1)
}

#[test]
fn test_insert_read() {
    let db = MvccTestDb::new();

    let tx1 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let tx1_row = generate_simple_string_row((-2).into(), 1, "Hello");
    db.mvcc_store.insert(tx1, tx1_row.clone()).unwrap();
    let row = db
        .mvcc_store
        .read(
            tx1,
            RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    let tx2 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let row = db
        .mvcc_store
        .read(
            tx2,
            RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);
}

#[test]
fn test_read_nonexistent() {
    let db = MvccTestDb::new();
    let tx = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let row = db.mvcc_store.read(
        tx,
        RowID {
            table_id: (-2).into(),
            row_id: RowKey::Int(1),
        },
    );
    assert!(row.unwrap().is_none());
}

#[test]
fn test_delete() {
    let db = MvccTestDb::new();

    let tx1 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let tx1_row = generate_simple_string_row((-2).into(), 1, "Hello");
    db.mvcc_store.insert(tx1, tx1_row.clone()).unwrap();
    let row = db
        .mvcc_store
        .read(
            tx1,
            RowID {
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
            RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap();
    assert!(row.is_none());
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    let tx2 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let row = db
        .mvcc_store
        .read(
            tx2,
            RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap();
    assert!(row.is_none());
}

#[test]
fn test_delete_nonexistent() {
    let db = MvccTestDb::new();
    let tx = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
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

#[test]
fn test_commit() {
    let db = MvccTestDb::new();
    let tx1 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let tx1_row = generate_simple_string_row((-2).into(), 1, "Hello");
    db.mvcc_store.insert(tx1, tx1_row.clone()).unwrap();
    let row = db
        .mvcc_store
        .read(
            tx1,
            RowID {
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
            RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_updated_row, row);
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    let tx2 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let row = db
        .mvcc_store
        .read(
            tx2,
            RowID {
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

#[test]
fn test_rollback() {
    let db = MvccTestDb::new();
    let tx1 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let row1 = generate_simple_string_row((-2).into(), 1, "Hello");
    db.mvcc_store.insert(tx1, row1.clone()).unwrap();
    let row2 = db
        .mvcc_store
        .read(
            tx1,
            RowID {
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
            RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(row3, row4);
    db.mvcc_store
        .rollback_tx(tx1, db.conn.pager.load().clone(), &db.conn);
    let tx2 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let row5 = db
        .mvcc_store
        .read(
            tx2,
            RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap();
    assert_eq!(row5, None);
}

#[test]
fn test_dirty_write() {
    let db = MvccTestDb::new();

    // T1 inserts a row with ID 1, but does not commit.
    let tx1 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let tx1_row = generate_simple_string_row((-2).into(), 1, "Hello");
    db.mvcc_store.insert(tx1, tx1_row.clone()).unwrap();
    let row = db
        .mvcc_store
        .read(
            tx1,
            RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);

    let conn2 = db.db.connect().unwrap();
    // T2 attempts to delete row with ID 1, but fails because T1 has not committed.
    let tx2 = db.mvcc_store.begin_tx(conn2.pager.load().clone()).unwrap();
    let tx2_row = generate_simple_string_row((-2).into(), 1, "World");
    assert!(!db.mvcc_store.update(tx2, tx2_row).unwrap());

    let row = db
        .mvcc_store
        .read(
            tx1,
            RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);
}

#[test]
fn test_dirty_read() {
    let db = MvccTestDb::new();

    // T1 inserts a row with ID 1, but does not commit.
    let tx1 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let row1 = generate_simple_string_row((-2).into(), 1, "Hello");
    db.mvcc_store.insert(tx1, row1).unwrap();

    // T2 attempts to read row with ID 1, but doesn't see one because T1 has not committed.
    let conn2 = db.db.connect().unwrap();
    let tx2 = db.mvcc_store.begin_tx(conn2.pager.load().clone()).unwrap();
    let row2 = db
        .mvcc_store
        .read(
            tx2,
            RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap();
    assert_eq!(row2, None);
}

#[test]
fn test_dirty_read_deleted() {
    let db = MvccTestDb::new();

    // T1 inserts a row with ID 1 and commits.
    let tx1 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let tx1_row = generate_simple_string_row((-2).into(), 1, "Hello");
    db.mvcc_store.insert(tx1, tx1_row.clone()).unwrap();
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    // T2 deletes row with ID 1, but does not commit.
    let conn2 = db.db.connect().unwrap();
    let tx2 = db.mvcc_store.begin_tx(conn2.pager.load().clone()).unwrap();
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
    let conn3 = db.db.connect().unwrap();
    let tx3 = db.mvcc_store.begin_tx(conn3.pager.load().clone()).unwrap();
    let row = db
        .mvcc_store
        .read(
            tx3,
            RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);
}

#[test]
fn test_fuzzy_read() {
    let db = MvccTestDb::new();

    // T1 inserts a row with ID 1 and commits.
    let tx1 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let tx1_row = generate_simple_string_row((-2).into(), 1, "First");
    db.mvcc_store.insert(tx1, tx1_row.clone()).unwrap();
    let row = db
        .mvcc_store
        .read(
            tx1,
            RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    // T2 reads the row with ID 1 within an active transaction.
    let conn2 = db.db.connect().unwrap();
    let tx2 = db.mvcc_store.begin_tx(conn2.pager.load().clone()).unwrap();
    let row = db
        .mvcc_store
        .read(
            tx2,
            RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);

    // T3 updates the row and commits.
    let conn3 = db.db.connect().unwrap();
    let tx3 = db.mvcc_store.begin_tx(conn3.pager.load().clone()).unwrap();
    let tx3_row = generate_simple_string_row((-2).into(), 1, "Second");
    db.mvcc_store.update(tx3, tx3_row).unwrap();
    commit_tx(db.mvcc_store.clone(), &conn3, tx3).unwrap();

    // T2 still reads the same version of the row as before.
    let row = db
        .mvcc_store
        .read(
            tx2,
            RowID {
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

#[test]
fn test_lost_update() {
    let db = MvccTestDb::new();

    // T1 inserts a row with ID 1 and commits.
    let tx1 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let tx1_row = generate_simple_string_row((-2).into(), 1, "Hello");
    db.mvcc_store.insert(tx1, tx1_row.clone()).unwrap();
    let row = db
        .mvcc_store
        .read(
            tx1,
            RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    // T2 attempts to update row ID 1 within an active transaction.
    let conn2 = db.db.connect().unwrap();
    let tx2 = db.mvcc_store.begin_tx(conn2.pager.load().clone()).unwrap();
    let tx2_row = generate_simple_string_row((-2).into(), 1, "World");
    assert!(db.mvcc_store.update(tx2, tx2_row.clone()).unwrap());

    // T3 also attempts to update row ID 1 within an active transaction.
    let conn3 = db.db.connect().unwrap();
    let tx3 = db.mvcc_store.begin_tx(conn3.pager.load().clone()).unwrap();
    let tx3_row = generate_simple_string_row((-2).into(), 1, "Hello, world!");
    assert!(matches!(
        db.mvcc_store.update(tx3, tx3_row),
        Err(LimboError::WriteWriteConflict)
    ));
    // hack: in the actual tursodb database we rollback the mvcc tx ourselves, so manually roll it back here
    db.mvcc_store
        .rollback_tx(tx3, conn3.pager.load().clone(), &conn3);

    commit_tx(db.mvcc_store.clone(), &conn2, tx2).unwrap();
    assert!(matches!(
        commit_tx(db.mvcc_store.clone(), &conn3, tx3),
        Err(LimboError::TxTerminated)
    ));

    let conn4 = db.db.connect().unwrap();
    let tx4 = db.mvcc_store.begin_tx(conn4.pager.load().clone()).unwrap();
    let row = db
        .mvcc_store
        .read(
            tx4,
            RowID {
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
    let tx1 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let tx1_row = generate_simple_string_row((-2).into(), 1, "10");
    db.mvcc_store.insert(tx1, tx1_row.clone()).unwrap();
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    // but I like more money, so let me try adding $10 more
    let conn2 = db.db.connect().unwrap();
    let tx2 = db.mvcc_store.begin_tx(conn2.pager.load().clone()).unwrap();
    let tx2_row = generate_simple_string_row((-2).into(), 1, "20");
    assert!(db.mvcc_store.update(tx2, tx2_row.clone()).unwrap());
    let row = db
        .mvcc_store
        .read(
            tx2,
            RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(row, tx2_row);

    // can I check how much money I have?
    let conn3 = db.db.connect().unwrap();
    let tx3 = db.mvcc_store.begin_tx(conn3.pager.load().clone()).unwrap();
    let row = db
        .mvcc_store
        .read(
            tx3,
            RowID {
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

    let tx1 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();

    let conn2 = db.db.connect().unwrap();
    let tx2 = db.mvcc_store.begin_tx(conn2.pager.load().clone()).unwrap();
    let tx2_row = generate_simple_string_row((-2).into(), 1, "Hello");
    db.mvcc_store.insert(tx2, tx2_row).unwrap();

    // transaction in progress, so tx1 shouldn't be able to see the value
    let row = db
        .mvcc_store
        .read(
            tx1,
            RowID {
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
            RowID {
                table_id: (-2).into(),
                row_id: RowKey::Int(1),
            },
        )
        .unwrap();
    assert_eq!(row, None);
}

use crate::mvcc::cursor::MvccLazyCursor;
use crate::mvcc::database::{MvStore, Row, RowID};
use crate::types::Text;
use crate::Value;
use crate::{Database, StepResult};
use crate::{MemoryIO, Statement};
use crate::{ValueRef, DATABASE_MANAGER};

// Simple atomic clock implementation for testing

fn setup_test_db() -> (MvccTestDb, u64) {
    let db = MvccTestDb::new();
    let tx_id = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();

    let table_id = MVTableId::new(-1);
    let test_rows = [
        (5, "row5"),
        (10, "row10"),
        (15, "row15"),
        (20, "row20"),
        (30, "row30"),
    ];

    for (row_id, data) in test_rows.iter() {
        let id = RowID::new(table_id, RowKey::Int(*row_id));
        let record = ImmutableRecord::from_values(&[Value::Text(Text::new(data.to_string()))], 1);
        let row = Row::new_table_row(id, record.as_blob().to_vec(), 1);
        db.mvcc_store.insert(tx_id, row).unwrap();
    }

    commit_tx(db.mvcc_store.clone(), &db.conn, tx_id).unwrap();

    let tx_id = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    (db, tx_id)
}

fn setup_lazy_db(initial_keys: &[i64]) -> (MvccTestDb, u64) {
    let db = MvccTestDb::new();
    let tx_id = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();

    let table_id = -1;
    for i in initial_keys {
        let id = RowID::new(table_id.into(), RowKey::Int(*i));
        let data = format!("row{i}");
        let record = ImmutableRecord::from_values(&[Value::Text(Text::new(data))], 1);
        let row = Row::new_table_row(id, record.as_blob().to_vec(), 1);
        db.mvcc_store.insert(tx_id, row).unwrap();
    }

    commit_tx(db.mvcc_store.clone(), &db.conn, tx_id).unwrap();

    let tx_id = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    (db, tx_id)
}

pub(crate) fn commit_tx(
    mv_store: Arc<MvStore<LocalClock>>,
    conn: &Arc<Connection>,
    tx_id: u64,
) -> Result<()> {
    let mut sm = mv_store.commit_tx(tx_id, conn).unwrap();
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

pub(crate) fn commit_tx_no_conn(
    db: &MvccTestDbNoConn,
    tx_id: u64,
    conn: &Arc<Connection>,
) -> Result<(), LimboError> {
    let mv_store = db.get_mvcc_store();
    let mut sm = mv_store.commit_tx(tx_id, conn).unwrap();
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
fn test_lazy_scan_cursor_basic() {
    let (db, tx_id) = setup_lazy_db(&[1, 2, 3, 4, 5]);
    let table_id = -1;

    let mut cursor = MvccLazyCursor::new(
        db.mvcc_store.clone(),
        tx_id,
        table_id,
        MvccCursorType::Table,
        Box::new(BTreeCursor::new(db.conn.pager.load().clone(), -table_id, 1)),
    )
    .unwrap();

    // Check first row
    assert!(matches!(cursor.next().unwrap(), IOResult::Done(true)));
    assert!(!cursor.is_empty());
    let row = cursor.read_mvcc_current_row().unwrap().unwrap();
    assert_eq!(row.id.row_id.to_int_or_panic(), 1);

    // Iterate through all rows
    let mut count = 1;
    loop {
        let res = cursor.next().unwrap();
        let IOResult::Done(res) = res else {
            panic!("unexpected next result {res:?}");
        };
        if !res {
            break;
        }
        count += 1;
        let row = cursor.read_mvcc_current_row().unwrap().unwrap();
        assert_eq!(row.id.row_id.to_int_or_panic(), count);
    }

    // Should have found 5 rows
    assert_eq!(count, 5);

    // After the last row, is_empty should return true
    assert!(!matches!(cursor.next().unwrap(), IOResult::Done(true)));
    assert!(cursor.is_empty());
}

#[test]
fn test_lazy_scan_cursor_with_gaps() {
    let (db, tx_id) = setup_test_db();
    let table_id = -1;

    let mut cursor = MvccLazyCursor::new(
        db.mvcc_store.clone(),
        tx_id,
        table_id,
        MvccCursorType::Table,
        Box::new(BTreeCursor::new(db.conn.pager.load().clone(), -table_id, 1)),
    )
    .unwrap();

    // Check first row
    assert!(matches!(cursor.next().unwrap(), IOResult::Done(true)));
    assert!(!cursor.is_empty());
    let row = cursor.read_mvcc_current_row().unwrap().unwrap();
    assert_eq!(row.id.row_id.to_int_or_panic(), 5);

    // Test moving forward and checking IDs
    let expected_ids = [5, 10, 15, 20, 30];
    let mut index = 0;

    let IOResult::Done(rowid) = cursor.rowid().unwrap() else {
        unreachable!();
    };
    let rowid = rowid.unwrap();
    assert_eq!(rowid, expected_ids[index]);

    loop {
        let res = cursor.next().unwrap();
        let IOResult::Done(res) = res else {
            panic!("unexpected next result {res:?}");
        };
        if !res {
            break;
        }
        index += 1;
        if index < expected_ids.len() {
            let IOResult::Done(rowid) = cursor.rowid().unwrap() else {
                unreachable!();
            };
            let rowid = rowid.unwrap();
            assert_eq!(rowid, expected_ids[index]);
        }
    }

    // Should have found all 5 rows
    assert_eq!(index, expected_ids.len() - 1);
}

#[test]
fn test_cursor_basic() {
    let (db, tx_id) = setup_lazy_db(&[1, 2, 3, 4, 5]);
    let table_id = -1;

    let mut cursor = MvccLazyCursor::new(
        db.mvcc_store.clone(),
        tx_id,
        table_id,
        MvccCursorType::Table,
        Box::new(BTreeCursor::new(db.conn.pager.load().clone(), -table_id, 1)),
    )
    .unwrap();

    let _ = cursor.next().unwrap();

    // Check first row
    assert!(!cursor.is_empty());
    let row = cursor.read_mvcc_current_row().unwrap().unwrap();
    assert_eq!(row.id.row_id.to_int_or_panic(), 1);

    // Iterate through all rows
    let mut count = 1;
    loop {
        let res = cursor.next().unwrap();
        let IOResult::Done(res) = res else {
            panic!("unexpected next result {res:?}");
        };
        if !res {
            break;
        }
        count += 1;
        let row = cursor.read_mvcc_current_row().unwrap().unwrap();
        assert_eq!(row.id.row_id.to_int_or_panic(), count);
    }

    // Should have found 5 rows
    assert_eq!(count, 5);

    // After the last row, is_empty should return true
    assert!(!matches!(cursor.next().unwrap(), IOResult::Done(true)));
    assert!(cursor.is_empty());
}

#[test]
fn test_cursor_with_empty_table() {
    let db = MvccTestDb::new();
    {
        // FIXME: force page 1 initialization
        let pager = db.conn.pager.load().clone();
        let tx_id = db.mvcc_store.begin_tx(pager.clone()).unwrap();
        commit_tx(db.mvcc_store.clone(), &db.conn, tx_id).unwrap();
    }
    let tx_id = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let table_id = -1; // Empty table

    // Test LazyScanCursor with empty table
    let cursor = MvccLazyCursor::new(
        db.mvcc_store.clone(),
        tx_id,
        table_id,
        MvccCursorType::Table,
        Box::new(BTreeCursor::new(db.conn.pager.load().clone(), -table_id, 1)),
    )
    .unwrap();
    assert!(cursor.is_empty());
    let rowid = cursor.rowid().unwrap();
    assert!(matches!(rowid, IOResult::Done(None)));
}

#[test]
fn test_cursor_modification_during_scan() {
    let _ = tracing_subscriber::fmt::try_init();
    let (db, tx_id) = setup_lazy_db(&[1, 2, 4, 5]);
    let table_id = -1;

    let mut cursor = MvccLazyCursor::new(
        db.mvcc_store.clone(),
        tx_id,
        table_id,
        MvccCursorType::Table,
        Box::new(BTreeCursor::new(db.conn.pager.load().clone(), -table_id, 1)),
    )
    .unwrap();

    // Read first row
    assert!(matches!(cursor.next().unwrap(), IOResult::Done(true)));
    let first_row = cursor.read_mvcc_current_row().unwrap().unwrap();
    assert_eq!(first_row.id.row_id.to_int_or_panic(), 1);

    // Insert a new row with ID between existing rows
    let new_row_id = RowID::new(table_id.into(), RowKey::Int(3));
    let new_row = generate_simple_string_record("new_row");

    let _ = cursor
        .insert(&BTreeKey::TableRowId((
            new_row_id.row_id.to_int_or_panic(),
            Some(&new_row),
        )))
        .unwrap();

    let mut read_rowids = vec![];
    loop {
        let res = cursor.next().unwrap();
        let IOResult::Done(res) = res else {
            panic!("unexpected next result {res:?}");
        };
        if !res {
            break;
        }
        read_rowids.push(
            cursor
                .read_mvcc_current_row()
                .unwrap()
                .unwrap()
                .id
                .row_id
                .to_int_or_panic(),
        );
    }
    assert_eq!(read_rowids, vec![2, 3, 4, 5]);
    assert!(!matches!(cursor.next().unwrap(), IOResult::Done(true)));
    assert!(cursor.is_empty());
}

/* States described in the Hekaton paper *for serializability*:

Table 1: Case analysis of action to take when version V’s
Begin field contains the ID of transaction TB
------------------------------------------------------------------------------------------------------
TB’s state   | TB’s end timestamp | Action to take when transaction T checks visibility of version V.
------------------------------------------------------------------------------------------------------
Active       | Not set            | V is visible only if TB=T and V’s end timestamp equals infinity.
------------------------------------------------------------------------------------------------------
Preparing    | TS                 | V’s begin timestamp will be TS ut V is not yet committed. Use TS
                                  | as V’s begin time when testing visibility. If the test is true,
                                  | allow T to speculatively read V. Committed TS V’s begin timestamp
                                  | will be TS and V is committed. Use TS as V’s begin time to test
                                  | visibility.
------------------------------------------------------------------------------------------------------
Committed    | TS                 | V’s begin timestamp will be TS and V is committed. Use TS as V’s
                                  | begin time to test visibility.
------------------------------------------------------------------------------------------------------
Aborted      | Irrelevant         | Ignore V; it’s a garbage version.
------------------------------------------------------------------------------------------------------
Terminated   | Irrelevant         | Reread V’s Begin field. TB has terminated so it must have finalized
or not found |                    | the timestamp.
------------------------------------------------------------------------------------------------------

Table 2: Case analysis of action to take when V's End field
contains a transaction ID TE.
------------------------------------------------------------------------------------------------------
TE’s state   | TE’s end timestamp | Action to take when transaction T checks visibility of a version V
             |                    | as of read time RT.
------------------------------------------------------------------------------------------------------
Active       | Not set            | V is visible only if TE is not T.
------------------------------------------------------------------------------------------------------
Preparing    | TS                 | V’s end timestamp will be TS provided that TE commits. If TS > RT,
                                  | V is visible to T. If TS < RT, T speculatively ignores V.
------------------------------------------------------------------------------------------------------
Committed    | TS                 | V’s end timestamp will be TS and V is committed. Use TS as V’s end
                                  | timestamp when testing visibility.
------------------------------------------------------------------------------------------------------
Aborted      | Irrelevant         | V is visible.
------------------------------------------------------------------------------------------------------
Terminated   | Irrelevant         | Reread V’s End field. TE has terminated so it must have finalized
or not found |                    | the timestamp.
*/

fn new_tx(tx_id: TxID, begin_ts: u64, state: TransactionState) -> Transaction {
    let state = state.into();
    Transaction {
        state,
        tx_id,
        begin_ts,
        write_set: SkipSet::new(),
        read_set: SkipSet::new(),
        header: RwLock::new(DatabaseHeader::default()),
    }
}

#[test]
fn test_snapshot_isolation_tx_visible1() {
    let txs: SkipMap<TxID, Transaction> = SkipMap::from_iter([
        (1, new_tx(1, 1, TransactionState::Committed(2))),
        (2, new_tx(2, 2, TransactionState::Committed(5))),
        (3, new_tx(3, 3, TransactionState::Aborted)),
        (5, new_tx(5, 5, TransactionState::Preparing)),
        (6, new_tx(6, 6, TransactionState::Committed(10))),
        (7, new_tx(7, 7, TransactionState::Active)),
    ]);

    let current_tx = new_tx(4, 4, TransactionState::Preparing);

    let rv_visible = |begin: Option<TxTimestampOrID>, end: Option<TxTimestampOrID>| {
        let row_version = RowVersion {
            begin,
            end,
            row: generate_simple_string_row((-2).into(), 1, "testme"),
            btree_resident: false,
        };
        tracing::debug!("Testing visibility of {row_version:?}");
        row_version.is_visible_to(&current_tx, &txs)
    };

    // begin visible:   transaction committed with ts < current_tx.begin_ts
    // end visible:     inf
    assert!(rv_visible(Some(TxTimestampOrID::TxID(1)), None));

    // begin invisible: transaction committed with ts > current_tx.begin_ts
    assert!(!rv_visible(Some(TxTimestampOrID::TxID(2)), None));

    // begin invisible: transaction aborted
    assert!(!rv_visible(Some(TxTimestampOrID::TxID(3)), None));

    // begin visible:   timestamp < current_tx.begin_ts
    // end invisible:   transaction committed with ts > current_tx.begin_ts
    assert!(!rv_visible(
        Some(TxTimestampOrID::Timestamp(0)),
        Some(TxTimestampOrID::TxID(1))
    ));

    // begin visible:   timestamp < current_tx.begin_ts
    // end visible:     transaction committed with ts < current_tx.begin_ts
    assert!(rv_visible(
        Some(TxTimestampOrID::Timestamp(0)),
        Some(TxTimestampOrID::TxID(2))
    ));

    // begin visible:   timestamp < current_tx.begin_ts
    // end invisible:   transaction aborted
    assert!(!rv_visible(
        Some(TxTimestampOrID::Timestamp(0)),
        Some(TxTimestampOrID::TxID(3))
    ));

    // begin invisible: transaction preparing
    assert!(!rv_visible(Some(TxTimestampOrID::TxID(5)), None));

    // begin invisible: transaction committed with ts > current_tx.begin_ts
    assert!(!rv_visible(Some(TxTimestampOrID::TxID(6)), None));

    // begin invisible: transaction active
    assert!(!rv_visible(Some(TxTimestampOrID::TxID(7)), None));

    // begin invisible: transaction committed with ts > current_tx.begin_ts
    assert!(!rv_visible(Some(TxTimestampOrID::TxID(6)), None));

    // begin invisible:   transaction active
    assert!(!rv_visible(Some(TxTimestampOrID::TxID(7)), None));

    // begin visible:   timestamp < current_tx.begin_ts
    // end invisible:     transaction preparing
    assert!(!rv_visible(
        Some(TxTimestampOrID::Timestamp(0)),
        Some(TxTimestampOrID::TxID(5))
    ));

    // begin invisible: timestamp > current_tx.begin_ts
    assert!(!rv_visible(
        Some(TxTimestampOrID::Timestamp(6)),
        Some(TxTimestampOrID::TxID(6))
    ));

    // begin visible:   timestamp < current_tx.begin_ts
    // end visible:     some active transaction will eventually overwrite this version,
    //                  but that hasn't happened
    //                  (this is the https://avi.im/blag/2023/hekaton-paper-typo/ case, I believe!)
    assert!(rv_visible(
        Some(TxTimestampOrID::Timestamp(0)),
        Some(TxTimestampOrID::TxID(7))
    ));

    assert!(!rv_visible(None, None));
}

#[test]
fn test_restart() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn = db.connect();
        let mvcc_store = db.get_mvcc_store();
        let tx_id = mvcc_store.begin_tx(conn.pager.load().clone()).unwrap();
        // insert table id -2 into sqlite_schema table (table_id -1)
        let data = ImmutableRecord::from_values(
            &[
                Value::Text(Text::new("table")), // type
                Value::Text(Text::new("test")),  // name
                Value::Text(Text::new("test")),  // tbl_name
                Value::Integer(-2),              // rootpage
                Value::Text(Text::new(
                    "CREATE TABLE test(id INTEGER PRIMARY KEY, data TEXT)",
                )), // sql
            ],
            5,
        );
        mvcc_store
            .insert(
                tx_id,
                Row::new_table_row(
                    RowID::new((-1).into(), RowKey::Int(1)),
                    data.as_blob().to_vec(),
                    5,
                ),
            )
            .unwrap();
        // now insert a row into table -2
        let row = generate_simple_string_row((-2).into(), 1, "foo");
        mvcc_store.insert(tx_id, row).unwrap();
        commit_tx(mvcc_store.clone(), &conn, tx_id).unwrap();
        conn.close().unwrap();
    }
    db.restart();

    {
        let conn = db.connect();
        let mvcc_store = db.get_mvcc_store();
        let tx_id = mvcc_store.begin_tx(conn.pager.load().clone()).unwrap();
        let row = generate_simple_string_row((-2).into(), 2, "bar");

        mvcc_store.insert(tx_id, row).unwrap();
        commit_tx(mvcc_store.clone(), &conn, tx_id).unwrap();

        let tx_id = mvcc_store.begin_tx(conn.pager.load().clone()).unwrap();
        let row = mvcc_store
            .read(tx_id, RowID::new((-2).into(), RowKey::Int(2)))
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

#[test]
fn test_connection_sees_other_connection_changes() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn0 = db.connect();
    conn0
        .execute("CREATE TABLE IF NOT EXISTS test_table (id INTEGER PRIMARY KEY, text TEXT)")
        .unwrap();
    let conn1 = db.connect();
    conn1
        .execute("CREATE TABLE IF NOT EXISTS test_table (id INTEGER PRIMARY KEY, text TEXT)")
        .unwrap();
    conn0
        .execute("INSERT INTO test_table (id, text) VALUES (965, 'text_877')")
        .unwrap();
    let mut stmt = conn1.query("SELECT * FROM test_table").unwrap().unwrap();
    loop {
        let res = stmt.step().unwrap();
        match res {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                let text = row.get_value(1).to_text().unwrap();
                assert_eq!(text, "text_877");
            }
            StepResult::Done => break,
            StepResult::IO => {
                stmt.run_once().unwrap();
            }
            _ => panic!("Expected Row"),
        }
    }
}

#[test]
fn test_delete_with_conn() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn0 = db.connect();
    conn0.execute("CREATE TABLE test(t)").unwrap();

    let mut inserts = vec![1, 2, 3, 4, 5, 6, 7];

    for t in &inserts {
        conn0
            .execute(format!("INSERT INTO test(t) VALUES ({t})"))
            .unwrap();
    }

    conn0.execute("DELETE FROM test WHERE t = 5").unwrap();
    inserts.remove(4);

    let mut stmt = conn0.prepare("SELECT * FROM test").unwrap();
    let mut pos = 0;
    loop {
        let res = stmt.step().unwrap();
        match res {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                let t = row.get_value(0).as_int().unwrap();
                assert_eq!(t, inserts[pos]);
                pos += 1;
            }
            StepResult::Done => break,
            StepResult::IO => {
                stmt.run_once().unwrap();
            }
            _ => panic!("Expected Row"),
        }
    }
}

fn get_record_value(row: &Row) -> ImmutableRecord {
    let mut record = ImmutableRecord::new(1024);
    record.start_serialization(row.payload());
    record
}

#[test]
fn test_interactive_transaction() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    // do some transaction
    conn.execute("BEGIN").unwrap();
    conn.execute("CREATE TABLE test (x)").unwrap();
    conn.execute("INSERT INTO test (x) VALUES (1)").unwrap();
    conn.execute("INSERT INTO test (x) VALUES (2)").unwrap();
    conn.execute("COMMIT").unwrap();

    // expect other transaction to see the changes
    let rows = get_rows(&conn, "SELECT * FROM test");
    assert_eq!(rows, vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]);
}

#[test]
fn test_commit_without_tx() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    // do not start interactive transaction
    conn.execute("CREATE TABLE test (x)").unwrap();
    conn.execute("INSERT INTO test (x) VALUES (1)").unwrap();

    // expect error on trying to commit a non-existent interactive transaction
    let err = conn.execute("COMMIT").unwrap_err();
    if let LimboError::TxError(e) = err {
        assert_eq!(e.to_string(), "cannot commit - no transaction is active");
    } else {
        panic!("Expected TxError");
    }
}

fn get_rows(conn: &Arc<Connection>, query: &str) -> Vec<Vec<Value>> {
    let mut stmt = conn.prepare(query).unwrap();
    let mut rows = Vec::new();
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                let values = row.get_values().cloned().collect::<Vec<_>>();
                rows.push(values);
            }
            StepResult::Done => break,
            StepResult::IO => {
                stmt.run_once().unwrap();
            }
            StepResult::Interrupt | StepResult::Busy => {
                panic!("unexpected step result");
            }
        }
    }
    rows
}

#[test]
#[ignore]
fn test_concurrent_writes() {
    struct ConnectionState {
        conn: Arc<Connection>,
        inserts: Vec<i64>,
        current_statement: Option<Statement>,
    }
    let db = MvccTestDbNoConn::new_with_random_db();
    let mut connections = Vec::new();
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE test (x)").unwrap();
        conn.close().unwrap();
    }
    let num_connections = 20;
    let num_inserts_per_connection = 10000;
    for i in 0..num_connections {
        let conn = db.connect();
        let mut inserts = ((num_inserts_per_connection * i)
            ..(num_inserts_per_connection * (i + 1)))
            .collect::<Vec<i64>>();
        inserts.reverse();
        connections.push(ConnectionState {
            conn,
            inserts,
            current_statement: None,
        });
    }

    loop {
        let mut all_finished = true;
        for conn in &mut connections {
            if !conn.inserts.is_empty() || conn.current_statement.is_some() {
                all_finished = false;
                break;
            }
        }
        for (conn_id, conn) in connections.iter_mut().enumerate() {
            // println!("connection {conn_id} inserts: {:?}", conn.inserts);
            if conn.current_statement.is_none() && !conn.inserts.is_empty() {
                let write = conn.inserts.pop().unwrap();
                println!("inserting row {write} from connection {conn_id}");
                conn.current_statement = Some(
                    conn.conn
                        .prepare(format!("INSERT INTO test (x) VALUES ({write})"))
                        .unwrap(),
                );
            }
            if conn.current_statement.is_none() {
                continue;
            }
            println!("connection step {conn_id}");
            let stmt = conn.current_statement.as_mut().unwrap();
            match stmt.step().unwrap() {
                // These you be only possible cases in write concurrency.
                // No rows because insert doesn't return
                // No interrupt because insert doesn't interrupt
                // No busy because insert in mvcc should be multi concurrent write
                StepResult::Done => {
                    println!("connection {conn_id} done");
                    conn.current_statement = None;
                }
                StepResult::IO => {
                    // let's skip doing I/O here, we want to perform io only after all the statements are stepped
                }
                StepResult::Busy => {
                    println!("connection {conn_id} busy");
                    // stmt.reprepare().unwrap();
                    unreachable!();
                }
                _ => {
                    unreachable!()
                }
            }
        }
        db.get_db().io.step().unwrap();

        if all_finished {
            println!("all finished");
            break;
        }
    }

    // Now let's find out if we wrote everything we intended to write.
    let conn = db.connect();
    let rows = get_rows(&conn, "SELECT * FROM test ORDER BY x ASC");
    assert_eq!(
        rows.len() as i64,
        num_connections * num_inserts_per_connection
    );
    for (row_id, row) in rows.iter().enumerate() {
        assert_eq!(row[0].as_int().unwrap(), row_id as i64);
    }
    conn.close().unwrap();
}

fn generate_batched_insert(num_inserts: usize) -> String {
    let mut inserts = String::from("INSERT INTO test (x) VALUES ");
    for i in 0..num_inserts {
        inserts.push_str(&format!("({i})"));
        if i < num_inserts - 1 {
            inserts.push(',');
        }
    }
    inserts.push(';');
    inserts
}
#[test]
#[ignore]
fn test_batch_writes() {
    let mut start = 0;
    let mut end = 5000;
    while start < end {
        let i = ((end - start) / 2) + start;
        let db = MvccTestDbNoConn::new_with_random_db();
        let conn = db.connect();
        conn.execute("CREATE TABLE test (x)").unwrap();
        let inserts = generate_batched_insert(i);
        if conn.execute(inserts.clone()).is_err() {
            end = i;
        } else {
            start = i + 1;
        }
    }
    println!("start: {start} end: {end}");
}

#[test]
fn transaction_display() {
    let state = AtomicTransactionState::from(TransactionState::Preparing);
    let tx_id = 42;
    let begin_ts = 20250914;

    let write_set = SkipSet::new();
    write_set.insert(RowID::new((-2).into(), RowKey::Int(11)));
    write_set.insert(RowID::new((-2).into(), RowKey::Int(13)));

    let read_set = SkipSet::new();
    read_set.insert(RowID::new((-2).into(), RowKey::Int(17)));
    read_set.insert(RowID::new((-2).into(), RowKey::Int(19)));

    let tx = Transaction {
        state,
        tx_id,
        begin_ts,
        write_set,
        read_set,
        header: RwLock::new(DatabaseHeader::default()),
    };

    let expected = "{ state: Preparing, id: 42, begin_ts: 20250914, write_set: [RowID { table_id: MVTableId(-2), row_id: Int(11) }, RowID { table_id: MVTableId(-2), row_id: Int(13) }], read_set: [RowID { table_id: MVTableId(-2), row_id: Int(17) }, RowID { table_id: MVTableId(-2), row_id: Int(19) }] }";
    let output = format!("{tx}");
    assert_eq!(output, expected);
}

#[test]
fn test_should_checkpoint() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let mv_store = db.get_mvcc_store();
    assert!(!mv_store.storage.should_checkpoint());
    mv_store.set_checkpoint_threshold(0);
    assert!(mv_store.storage.should_checkpoint());
}

#[test]
fn test_insert_with_checkpoint() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let mv_store = db.get_mvcc_store();
    // force checkpoint on every transaction
    mv_store.set_checkpoint_threshold(0);
    let conn = db.connect();
    conn.execute("CREATE TABLE t(x)").unwrap();
    conn.execute("INSERT INTO t VALUES (1)").unwrap();
    let rows = get_rows(&conn, "SELECT * FROM t");
    assert_eq!(rows.len(), 1);
    let row = rows.first().unwrap();
    assert_eq!(row.len(), 1);
    let value = row.first().unwrap();
    match value {
        Value::Integer(i) => assert_eq!(*i, 1),
        _ => unreachable!(),
    }
}

#[test]
fn test_select_empty_table() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let mv_store = db.get_mvcc_store();
    // force checkpoint on every transaction
    mv_store.set_checkpoint_threshold(0);
    let conn = db.connect();
    conn.execute("CREATE TABLE t(x integer primary key)")
        .unwrap();
    let rows = get_rows(&conn, "SELECT * FROM t where x > 100");
    assert!(rows.is_empty());
}

#[test]
fn test_cursor_with_btree_and_mvcc() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    // First write some rows and checkpoint so data is flushed to BTree file (.db)
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(x integer primary key)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1)").unwrap();
        conn.execute("INSERT INTO t VALUES (2)").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    }
    // Now restart so new connection will have to read data from BTree instead of MVCC.
    db.restart();
    let conn = db.connect();
    println!("getting rows");
    let rows = get_rows(&conn, "SELECT * FROM t");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], vec![Value::Integer(1)]);
    assert_eq!(rows[1], vec![Value::Integer(2)]);
}

#[test]
fn test_cursor_with_btree_and_mvcc_2() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    // First write some rows and checkpoint so data is flushed to BTree file (.db)
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(x integer primary key)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1)").unwrap();
        conn.execute("INSERT INTO t VALUES (3)").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    }
    // Now restart so new connection will have to read data from BTree instead of MVCC.
    db.restart();
    let conn = db.connect();
    // Insert a new row so that we have a gap in the BTree.
    conn.execute("INSERT INTO t VALUES (2)").unwrap();
    println!("getting rows");
    let rows = get_rows(&conn, "SELECT * FROM t");
    dbg!(&rows);
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], vec![Value::Integer(1)]);
    assert_eq!(rows[1], vec![Value::Integer(2)]);
    assert_eq!(rows[2], vec![Value::Integer(3)]);
}

#[test]
fn test_cursor_with_btree_and_mvcc_with_backward_cursor() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    // First write some rows and checkpoint so data is flushed to BTree file (.db)
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(x integer primary key)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1)").unwrap();
        conn.execute("INSERT INTO t VALUES (3)").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    }
    // Now restart so new connection will have to read data from BTree instead of MVCC.
    db.restart();
    let conn = db.connect();
    // Insert a new row so that we have a gap in the BTree.
    conn.execute("INSERT INTO t VALUES (2)").unwrap();
    let rows = get_rows(&conn, "SELECT * FROM t order by x desc");
    dbg!(&rows);
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], vec![Value::Integer(3)]);
    assert_eq!(rows[1], vec![Value::Integer(2)]);
    assert_eq!(rows[2], vec![Value::Integer(1)]);
}

#[test]
fn test_cursor_with_btree_and_mvcc_with_backward_cursor_with_delete() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    // First write some rows and checkpoint so data is flushed to BTree file (.db)
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(x integer primary key)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1)").unwrap();
        conn.execute("INSERT INTO t VALUES (2)").unwrap();
        conn.execute("INSERT INTO t VALUES (4)").unwrap();
        conn.execute("INSERT INTO t VALUES (5)").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    }
    // Now restart so new connection will have to read data from BTree instead of MVCC.
    db.restart();
    let conn = db.connect();
    // Insert a new row so that we have a gap in the BTree.
    conn.execute("INSERT INTO t VALUES (3)").unwrap();
    conn.execute("DELETE FROM t WHERE x = 2").unwrap();
    println!("getting rows");
    let rows = get_rows(&conn, "SELECT * FROM t order by x desc");
    dbg!(&rows);
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0], vec![Value::Integer(5)]);
    assert_eq!(rows[1], vec![Value::Integer(4)]);
    assert_eq!(rows[2], vec![Value::Integer(3)]);
    assert_eq!(rows[3], vec![Value::Integer(1)]);
}

#[test]
fn test_cursor_with_btree_and_mvcc_fuzz() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let mut rows_in_db = sorted_vec::SortedVec::new();
    let mut seen = HashSet::new();
    let (mut rng, _seed) = rng_from_time_or_env();
    println!("seed: {_seed}");

    let mut maybe_conn = Some(db.connect());
    {
        maybe_conn
            .as_mut()
            .unwrap()
            .execute("CREATE TABLE t(x integer primary key)")
            .unwrap();
    }

    #[repr(u8)]
    #[derive(Debug)]
    enum Op {
        Insert = 0,
        Delete = 1,
        SelectForward = 2,
        SelectBackward = 3,
        SeekForward = 4,
        SeekBackward = 5,
        Checkpoint = 6,
    }

    impl From<u8> for Op {
        fn from(value: u8) -> Self {
            match value {
                0 => Op::Insert,
                1 => Op::Delete,
                2 => Op::SelectForward,
                3 => Op::SelectBackward,
                4 => Op::SeekForward,
                5 => Op::SeekBackward,
                6 => Op::Checkpoint,
                _ => unreachable!(),
            }
        }
    }

    for i in 0..10000 {
        let conn = maybe_conn.as_mut().unwrap();
        let op = rng.random_range(0..=Op::Checkpoint as usize);
        let op = Op::from(op as u8);
        println!("tick: {i} op: {op:?} ");
        match op {
            Op::Insert => {
                let value = loop {
                    let value = rng.random_range(0..10000);
                    if !seen.contains(&value) {
                        seen.insert(value);
                        break value;
                    }
                };
                let query = format!("INSERT INTO t VALUES ({value})");
                println!("inserting: {query}");
                conn.execute(query.as_str()).unwrap();
                rows_in_db.push(value);
            }
            Op::Delete => {
                if rows_in_db.is_empty() {
                    continue;
                }
                let index = rng.random_range(0..rows_in_db.len());
                let value = rows_in_db[index];
                let query = format!("DELETE FROM t WHERE x = {value}");
                println!("deleting: {query}");
                conn.execute(query.as_str()).unwrap();
                rows_in_db.remove_index(index);
                seen.remove(&value);
            }
            Op::SelectForward => {
                let rows = get_rows(conn, "SELECT * FROM t order by x asc");
                assert_eq!(
                    rows.len(),
                    rows_in_db.len(),
                    "expected {} rows, got {}",
                    rows_in_db.len(),
                    rows.len()
                );
                for (row, expected_rowid) in rows.iter().zip(rows_in_db.iter()) {
                    assert_eq!(
                        row[0].as_int().unwrap(),
                        *expected_rowid,
                        "expected row id {}  got {}",
                        *expected_rowid,
                        row[0].as_int().unwrap()
                    );
                }
            }
            Op::SelectBackward => {
                let rows = get_rows(conn, "SELECT * FROM t order by x desc");
                assert_eq!(
                    rows.len(),
                    rows_in_db.len(),
                    "expected {} rows, got {}",
                    rows_in_db.len(),
                    rows.len()
                );
                for (row, expected_rowid) in rows.iter().zip(rows_in_db.iter().rev()) {
                    assert_eq!(
                        row[0].as_int().unwrap(),
                        *expected_rowid,
                        "expected row id {}  got {}",
                        *expected_rowid,
                        row[0].as_int().unwrap()
                    );
                }
            }
            Op::SeekForward => {
                let value = rng.random_range(0..10000);
                let rows = get_rows(
                    conn,
                    format!("SELECT * FROM t where x > {value} order by x asc").as_str(),
                );
                let filtered_rows_in_db = rows_in_db
                    .iter()
                    .filter(|&id| *id > value)
                    .cloned()
                    .collect::<Vec<i64>>();

                assert_eq!(
                    rows.len(),
                    filtered_rows_in_db.len(),
                    "expected {} rows, got {}",
                    filtered_rows_in_db.len(),
                    rows.len()
                );
                for (row, expected_rowid) in rows.iter().zip(filtered_rows_in_db.iter()) {
                    assert_eq!(
                        row[0].as_int().unwrap(),
                        *expected_rowid,
                        "expected row id {}  got {}",
                        *expected_rowid,
                        row[0].as_int().unwrap()
                    );
                }
            }
            Op::SeekBackward => {
                let value = rng.random_range(0..10000);
                let rows = get_rows(
                    conn,
                    format!("SELECT * FROM t where x > {value} order by x desc").as_str(),
                );
                let filtered_rows_in_db = rows_in_db
                    .iter()
                    .filter(|&id| *id > value)
                    .cloned()
                    .collect::<Vec<i64>>();

                assert_eq!(
                    rows.len(),
                    filtered_rows_in_db.len(),
                    "expected {} rows, got {}",
                    filtered_rows_in_db.len(),
                    rows.len()
                );
                for (row, expected_rowid) in rows.iter().zip(filtered_rows_in_db.iter().rev()) {
                    assert_eq!(
                        row[0].as_int().unwrap(),
                        *expected_rowid,
                        "expected row id {}  got {}",
                        *expected_rowid,
                        row[0].as_int().unwrap()
                    );
                }
            }
            Op::Checkpoint => {
                // This forces things to move to the BTree file (.db)
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
                // This forces MVCC to be cleared
                db.restart();
                maybe_conn = Some(db.connect());
            }
        }
    }
}

pub fn rng_from_time_or_env() -> (ChaCha8Rng, u64) {
    let seed = std::env::var("SEED").map_or(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
        |v| {
            v.parse()
                .expect("Failed to parse SEED environment variable as u64")
        },
    );
    let rng = ChaCha8Rng::seed_from_u64(seed as u64);
    (rng, seed as u64)
}

#[test]
fn test_cursor_with_btree_and_mvcc_insert_after_checkpoint_repeated_key() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    // First write some rows and checkpoint so data is flushed to BTree file (.db)
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(x integer primary key)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1)").unwrap();
        conn.execute("INSERT INTO t VALUES (2)").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    }
    // Now restart so new connection will have to read data from BTree instead of MVCC.
    db.restart();
    let conn = db.connect();
    // Insert a new row so that we have a gap in the BTree.
    let res = conn.execute("INSERT INTO t VALUES (2)");
    assert!(res.is_err(), "Expected error because key 2 already exists");
}

#[test]
fn test_cursor_with_btree_and_mvcc_seek_after_checkpoint() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    // First write some rows and checkpoint so data is flushed to BTree file (.db)
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(x integer primary key)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1)").unwrap();
        conn.execute("INSERT INTO t VALUES (2)").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    }
    // Now restart so new connection will have to read data from BTree instead of MVCC.
    db.restart();
    let conn = db.connect();
    // Seek to the second row.
    let res = get_rows(&conn, "SELECT * FROM t WHERE x = 2");
    assert_eq!(res.len(), 1);
    assert_eq!(res[0][0].as_int().unwrap(), 2);
}

#[test]
fn test_cursor_with_btree_and_mvcc_delete_after_checkpoint() {
    tracing_subscriber::fmt()
        .with_ansi(false)
        .with_max_level(tracing_subscriber::filter::LevelFilter::TRACE)
        .init();
    let mut db = MvccTestDbNoConn::new_with_random_db();
    // First write some rows and checkpoint so data is flushed to BTree file (.db)
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(x integer primary key)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1)").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    }
    // Now restart so new connection will have to read data from BTree instead of MVCC.
    db.restart();
    let conn = db.connect();
    conn.execute("DELETE FROM t WHERE x = 1").unwrap();
    let rows = get_rows(&conn, "SELECT * FROM t order by x desc");
    assert_eq!(rows.len(), 0);
}

#[test]
fn test_skips_updated_rowid() {
    tracing_subscriber::fmt()
        .with_ansi(false)
        .with_max_level(tracing_subscriber::filter::LevelFilter::TRACE)
        .init();
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY AUTOINCREMENT)")
        .unwrap();

    // we insert with default values
    conn.execute("INSERT INTO t DEFAULT VALUES").unwrap();
    let rows = get_rows(&conn, "SELECT * FROM sqlite_sequence");
    dbg!(&rows);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1].as_int().unwrap(), 1);

    // we update the rowid to +1
    conn.execute("UPDATE t SET a = a + 1").unwrap();
    let rows = get_rows(&conn, "SELECT * FROM sqlite_sequence");
    dbg!(&rows);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1].as_int().unwrap(), 1);

    // we insert with default values again
    conn.execute("INSERT INTO t DEFAULT VALUES").unwrap();
    let rows = get_rows(&conn, "SELECT * FROM sqlite_sequence");
    dbg!(&rows);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1].as_int().unwrap(), 3);
}
