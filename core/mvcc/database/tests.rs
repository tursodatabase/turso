use rustc_hash::FxHashSet as HashSet;

use super::*;
use crate::io::PlatformIO;
use crate::mvcc::clock::LocalClock;
use crate::mvcc::cursor::MvccCursorType;
use crate::storage::sqlite3_ondisk::DatabaseHeader;
use crate::sync::RwLock;
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
        let db = Database::open_file(io.clone(), ":memory:").unwrap();
        let conn = db.connect().unwrap();
        // Enable MVCC via PRAGMA
        conn.execute("PRAGMA journal_mode = 'experimental_mvcc'")
            .unwrap();
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
        let db = Database::open_file(io.clone(), ":memory:").unwrap();
        // Enable MVCC via PRAGMA
        let conn = db.connect().unwrap();
        conn.execute("PRAGMA journal_mode = 'experimental_mvcc'")
            .unwrap();
        conn.close().unwrap();
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
        let db = Database::open_file(io.clone(), path.as_os_str().to_str().unwrap()).unwrap();
        // Enable MVCC via PRAGMA
        let conn = db.connect().unwrap();
        conn.execute("PRAGMA journal_mode = 'experimental_mvcc'")
            .unwrap();
        conn.close().unwrap();
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
        let db = Database::open_file(io.clone(), path).unwrap();
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
    let res = cursor.next().unwrap();
    assert!(matches!(res, IOResult::Done(())));
    assert!(cursor.has_record());
    assert!(!cursor.is_empty());
    let row = cursor.read_mvcc_current_row().unwrap().unwrap();
    assert_eq!(row.id.row_id.to_int_or_panic(), 1);

    // Iterate through all rows
    let mut count = 1;
    loop {
        let res = cursor.next().unwrap();
        let IOResult::Done(()) = res else {
            panic!("unexpected next result {res:?}");
        };
        if !cursor.has_record() {
            break;
        }
        count += 1;
        let row = cursor.read_mvcc_current_row().unwrap().unwrap();
        assert_eq!(row.id.row_id.to_int_or_panic(), count);
    }

    // Should have found 5 rows
    assert_eq!(count, 5);

    // After the last row, is_empty should return true
    let res = cursor.next().unwrap();
    assert!(matches!(res, IOResult::Done(())));
    assert!(!cursor.has_record());
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
    let res = cursor.next().unwrap();
    assert!(matches!(res, IOResult::Done(())));
    assert!(cursor.has_record());
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
        let IOResult::Done(()) = res else {
            panic!("unexpected next result {res:?}");
        };
        if !cursor.has_record() {
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
        let IOResult::Done(()) = res else {
            panic!("unexpected next result {res:?}");
        };
        if !cursor.has_record() {
            break;
        }
        count += 1;
        let row = cursor.read_mvcc_current_row().unwrap().unwrap();
        assert_eq!(row.id.row_id.to_int_or_panic(), count);
    }

    // Should have found 5 rows
    assert_eq!(count, 5);

    // After the last row, is_empty should return true
    let res = cursor.next().unwrap();
    assert!(matches!(res, IOResult::Done(())));
    assert!(!cursor.has_record());
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
    let mut cursor = MvccLazyCursor::new(
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
    let res = cursor.next().unwrap();
    assert!(matches!(res, IOResult::Done(())));
    assert!(cursor.has_record());
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
        let IOResult::Done(()) = res else {
            panic!("unexpected next result {res:?}");
        };
        if !cursor.has_record() {
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
    let res = cursor.next().unwrap();
    assert!(matches!(res, IOResult::Done(())));
    assert!(!cursor.has_record());
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
        savepoint_stack: RwLock::new(Vec::new()),
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
            id: 0, // Dummy ID for visibility tests
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
    stmt.run_with_row_callback(|row| {
        let text = row.get_value(1).to_text().unwrap();
        assert_eq!(text, "text_877");
        Ok(())
    })
    .unwrap();
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
    stmt.run_with_row_callback(|row| {
        let t = row.get_value(0).as_int().unwrap();
        assert_eq!(t, inserts[pos]);
        pos += 1;
        Ok(())
    })
    .unwrap();
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
    stmt.run_with_row_callback(|row| {
        let values = row.get_values().cloned().collect::<Vec<_>>();
        rows.push(values);
        Ok(())
    })
    .unwrap();
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
        savepoint_stack: RwLock::new(Vec::new()),
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
fn test_auto_checkpoint_busy_is_ignored() {
    let db = MvccTestDb::new();
    db.mvcc_store.set_checkpoint_threshold(0);

    // Keep a second transaction open to hold the checkpoint read lock.
    let tx1 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();
    let tx2 = db
        .mvcc_store
        .begin_tx(db.conn.pager.load().clone())
        .unwrap();

    let row = generate_simple_string_row((-2).into(), 1, "Hello");
    db.mvcc_store.insert(tx1, row).unwrap();

    // Regression: auto-checkpoint returning Busy used to bubble up and cause
    // statement abort/rollback after the tx was removed.
    // Commit should succeed even if the auto-checkpoint is busy.
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    // Cleanup: release the read lock held by tx2.
    db.mvcc_store
        .rollback_tx(tx2, db.conn.pager.load().clone(), &db.conn);
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
#[ignore] // FIXME: This fails constantly on main and is really annoying, disabling for now :]
fn test_cursor_with_btree_and_mvcc_fuzz() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let mut rows_in_db = sorted_vec::SortedVec::new();
    let mut seen = HashSet::default();
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

#[test]
fn test_mvcc_integrity_check() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY)")
        .unwrap();

    // we insert with default values
    conn.execute("INSERT INTO t values(1)").unwrap();

    let ensure_integrity = || {
        let rows = get_rows(&conn, "PRAGMA integrity_check");
        assert_eq!(rows.len(), 1);
        assert_eq!(&rows[0][0].cast_text().unwrap(), "ok");
    };

    ensure_integrity();

    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    ensure_integrity();
}

/// Test that integrity_check passes after DROP TABLE but before checkpoint.
/// Issue #4975: After checkpointing a table and then dropping it, integrity_check
/// would fail because the dropped table's btree pages still exist but aren't
/// tracked by the schema. The fix is to track dropped root pages until checkpoint.
#[test]
fn test_integrity_check_after_drop_table_before_checkpoint() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX idx_t_data ON t(data)").unwrap();

    // Insert data to force page allocation
    for i in 0..10 {
        let data = format!("data_{i}");
        conn.execute(format!("INSERT INTO t VALUES ({i}, '{data}')"))
            .unwrap();
    }
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    drop(conn);

    db.restart();

    let conn = db.connect();

    // Now drop table. Before the fix, this would make integrity_check fail because
    // we dropped the table before checkpointing, meaning integrity_check would find
    // pages not being used since we didn't provide root page of table t for checks.
    conn.execute("DROP TABLE t").unwrap();
    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

/// Test that integrity_check passes after DROP INDEX but before checkpoint.
/// Issue #4975: After checkpointing an index and then dropping it, integrity_check
/// would fail because the dropped index's btree pages still exist but aren't
/// tracked by the schema. The fix is to track dropped root pages until checkpoint.
#[test]
fn test_integrity_check_after_drop_index_before_checkpoint() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX idx_t_data ON t(data)").unwrap();

    // Insert data to force page allocation
    for i in 0..10 {
        let data = format!("data_{i}");
        conn.execute(format!("INSERT INTO t VALUES ({i}, '{data}')"))
            .unwrap();
    }
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    drop(conn);

    db.restart();

    let conn = db.connect();

    // Now drop index. Before the fix, this would make integrity_check fail because
    // we dropped the index before checkpointing, meaning integrity_check would find
    // pages not being used since we didn't provide root page of index idx_t_data for checks.
    conn.execute("DROP INDEX idx_t_data").unwrap();
    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

#[test]
fn test_rollback_with_index() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY, b INTEGER UNIQUE)")
        .unwrap();

    // we insert with default values
    conn.execute("BEGIN CONCURRENT").unwrap();
    conn.execute("INSERT INTO t values (1, 1)").unwrap();
    conn.execute("ROLLBACK").unwrap();

    // This query will try to use index to find the row, if we rollback correctly it shouldn't panic
    let rows = get_rows(&conn, "SELECT * FROM t where b = 1");
    assert_eq!(rows.len(), 0);

    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}
/// 1. BEGIN CONCURRENT (start interactive transaction)
/// 2. UPDATE modifies col_a's index, then fails constraint check on col_b
/// 3. The partial index changes are NOT rolled back (this is the bug!)
/// 4. COMMIT succeeds, persisting the inconsistent state
/// 5. Later UPDATE on same row fails: "IdxDelete: no matching index entry found"
///    because table row has old value but index has new value
#[test]
fn test_update_multiple_unique_columns_partial_rollback() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    // Create table with multiple unique columns (like blue_sun_77 in the bug)
    conn.execute(
        "CREATE TABLE t(
            id INTEGER PRIMARY KEY,
            col_a TEXT UNIQUE,
            col_b REAL UNIQUE
        )",
    )
    .unwrap();

    // Insert two rows - one to update, one to cause conflict
    conn.execute("INSERT INTO t VALUES (1, 'original_a', 1.0)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'other_a', 2.0)")
        .unwrap();

    // Start an INTERACTIVE transaction - this is KEY to reproducing the bug!
    // In auto-commit mode, the entire transaction is rolled back on error.
    // In interactive mode, only the statement should be rolled back.
    conn.execute("BEGIN CONCURRENT").unwrap();

    // Try to UPDATE row 1 with:
    // - col_a = 'new_a' (index modification happens first)
    // - col_b = 2.0 (should FAIL - conflicts with row 2)
    //
    // The UPDATE bytecode does:
    // 1. Delete old index entry for col_a ('original_a', 1)
    // 2. Insert new index entry for col_a ('new_a', 1)
    // 3. Delete old index entry for col_b (1.0, 1)
    // 4. Check constraint for col_b (2.0) - FAIL with Halt err_code=1555!
    //
    // BUG: Without proper statement rollback, steps 1-3 are committed!
    let result = conn.execute("UPDATE t SET col_a = 'new_a', col_b = 2.0 WHERE id = 1");
    assert!(
        result.is_err(),
        "Expected unique constraint violation on col_b"
    );

    // COMMIT the transaction - this is what the stress test does after the error!
    // In the buggy case, this commits the partial index changes from the failed UPDATE.
    conn.execute("COMMIT").unwrap();

    // Now in a NEW transaction, try to UPDATE the same row.
    // If the previous statement's partial changes were committed:
    // - Table row still has col_a = 'original_a' (UPDATE didn't complete)
    // - But index for col_a now has 'new_a' instead of 'original_a'!
    // - This UPDATE reads 'original_a' from table, tries to delete that index entry
    // - CRASH: "IdxDelete: no matching index entry found for key ['original_a', 1]"
    conn.execute("UPDATE t SET col_a = 'updated_a', col_b = 3.0 WHERE id = 1")
        .unwrap();

    // Verify the update worked
    let rows = get_rows(&conn, "SELECT * FROM t WHERE id = 1");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1].cast_text().unwrap(), "updated_a");

    // Integrity check
    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

/// Test that a transaction cannot see uncommitted changes from another transaction.
/// This verifies snapshot isolation.
#[test]
fn test_mvcc_snapshot_isolation() {
    let db = MvccTestDbNoConn::new_with_random_db();

    let conn1 = db.connect();
    conn1
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, value INTEGER)")
        .unwrap();
    conn1
        .execute("INSERT INTO t VALUES (1, 100), (2, 200), (3, 300)")
        .unwrap();

    // Start tx1 and read initial values
    conn1.execute("BEGIN CONCURRENT").unwrap();
    let rows1 = get_rows(&conn1, "SELECT value FROM t WHERE id = 2");
    assert_eq!(rows1[0][0].to_string(), "200");

    // Start tx2 and modify the same row
    let conn2 = db.connect();
    conn2.execute("BEGIN CONCURRENT").unwrap();
    conn2
        .execute("UPDATE t SET value = 999 WHERE id = 2")
        .unwrap();
    conn2.execute("COMMIT").unwrap();

    // Tx1 should still see the old value (snapshot isolation)
    let rows1_again = get_rows(&conn1, "SELECT value FROM t WHERE id = 2");
    assert_eq!(
        rows1_again[0][0].to_string(),
        "200",
        "Tx1 should not see tx2's committed changes"
    );

    conn1.execute("COMMIT").unwrap();

    // After tx1 commits, new reads should see tx2's changes
    let rows_after = get_rows(&conn1, "SELECT value FROM t WHERE id = 2");
    assert_eq!(rows_after[0][0].to_string(), "999");
}
/// Similar test but with the constraint error happening on the third unique column.
/// This tests that ALL previous index modifications are rolled back.
/// Uses interactive transaction (BEGIN CONCURRENT) to reproduce the bug.
#[test]
fn test_update_three_unique_columns_partial_rollback() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    // Create table with three unique columns
    conn.execute(
        "CREATE TABLE t(
            id INTEGER PRIMARY KEY,
            col_a TEXT UNIQUE,
            col_b REAL UNIQUE,
            col_c INTEGER UNIQUE
        )",
    )
    .unwrap();

    // Insert two rows
    conn.execute("INSERT INTO t VALUES (1, 'a1', 1.0, 100)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'a2', 2.0, 200)")
        .unwrap();

    // Start interactive transaction
    conn.execute("BEGIN CONCURRENT").unwrap();

    // Try to UPDATE row 1 with:
    // - col_a = 'new_a' (index modified)
    // - col_b = 3.0 (index modified)
    // - col_c = 200 (FAIL - conflicts with row 2)
    // BUG: col_a and col_b index changes are NOT rolled back!
    let result =
        conn.execute("UPDATE t SET col_a = 'new_a', col_b = 3.0, col_c = 200 WHERE id = 1");
    assert!(
        result.is_err(),
        "Expected unique constraint violation on col_c"
    );

    // COMMIT - in buggy case, this commits partial index changes
    conn.execute("COMMIT").unwrap();

    // Now try to UPDATE the same row - this should work but may crash
    // if col_a or col_b index entries are inconsistent
    conn.execute("UPDATE t SET col_a = 'updated_a', col_b = 5.0, col_c = 500 WHERE id = 1")
        .unwrap();

    // Verify index lookups work
    let rows = get_rows(&conn, "SELECT * FROM t WHERE col_a = 'updated_a'");
    assert_eq!(rows.len(), 1);

    let rows = get_rows(&conn, "SELECT * FROM t WHERE col_b = 5.0");
    assert_eq!(rows.len(), 1);

    let rows = get_rows(&conn, "SELECT * FROM t WHERE col_c = 500");
    assert_eq!(rows.len(), 1);

    // Integrity check
    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

/// Test that simulates the exact sequence from the stress test bug:
/// Multiple interactive transactions updating the same row, with constraint errors.
///
/// From the log:
/// - tx 248: UPDATE row with pk=1.37, sets unique_col='sweet_wind_280' -> COMMIT
/// - tx 1149: BEGIN, UPDATE same row (modifies unique_col index, fails on other_unique), COMMIT
///   BUG: partial index changes from failed UPDATE are committed!
/// - tx 1324: UPDATE same row -> CRASH "IdxDelete: no matching index entry found"
#[test]
fn test_sequential_updates_with_constraint_errors() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute(
        "CREATE TABLE t(
            pk REAL PRIMARY KEY,
            unique_col TEXT UNIQUE,
            other_unique REAL UNIQUE
        )",
    )
    .unwrap();

    // Insert initial rows (simulating the stress test setup)
    conn.execute("INSERT INTO t VALUES (1.37, 'sweet_wind_280', 9.05)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (2.13, 'other_value', 2.13)")
        .unwrap();

    // First successful update (like tx 248 in the bug)
    conn.execute("UPDATE t SET unique_col = 'cold_grass_813', other_unique = 3.90 WHERE pk = 1.37")
        .unwrap();

    // Verify the update
    let rows = get_rows(&conn, "SELECT unique_col FROM t WHERE pk = 1.37");
    assert_eq!(rows[0][0].cast_text().unwrap(), "cold_grass_813");

    // Like tx 1149: Start interactive transaction
    conn.execute("BEGIN CONCURRENT").unwrap();

    // Try an update that will fail on other_unique (conflicts with row 2)
    // The UPDATE will:
    // 1. Delete old index entry for unique_col ('cold_grass_813')
    // 2. Insert new index entry for unique_col ('new_value')
    // 3. Delete old index entry for other_unique (3.90)
    // 4. Check constraint for other_unique (2.13) -> FAIL!
    // BUG: Steps 1-3 are NOT rolled back!
    let result =
        conn.execute("UPDATE t SET unique_col = 'new_value', other_unique = 2.13 WHERE pk = 1.37");
    assert!(result.is_err(), "Expected unique constraint violation");

    // COMMIT the transaction (like the stress test does after the error)
    // BUG: This commits the partial index changes!
    conn.execute("COMMIT").unwrap();

    // Like tx 1324: Try another update on the same row
    // If partial changes were committed:
    // - Table row has unique_col = 'cold_grass_813'
    // - But unique_col index has 'new_value' (not 'cold_grass_813')!
    // - This UPDATE reads 'cold_grass_813' from table, tries to delete that index entry
    // - CRASH: "IdxDelete: no matching index entry found"
    conn.execute("UPDATE t SET unique_col = 'fresh_sun_348', other_unique = 5.0 WHERE pk = 1.37")
        .unwrap();

    // Verify final state
    let rows = get_rows(
        &conn,
        "SELECT unique_col, other_unique FROM t WHERE pk = 1.37",
    );
    assert_eq!(rows[0][0].cast_text().unwrap(), "fresh_sun_348");

    // Verify index lookups work
    let rows = get_rows(&conn, "SELECT * FROM t WHERE unique_col = 'fresh_sun_348'");
    assert_eq!(rows.len(), 1);

    // Integrity check
    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

/// Test that multiple successful statements in an interactive transaction
/// have their changes preserved when a subsequent statement fails.
/// This tests the statement-level savepoint functionality.
#[test]
fn test_savepoint_multiple_statements_last_fails() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY)")
        .unwrap();

    // Start interactive transaction
    conn.execute("BEGIN CONCURRENT").unwrap();

    // Statement 1: Insert row 1 - success
    conn.execute("INSERT INTO t VALUES (1)").unwrap();

    // Statement 2: Insert row 2 - success
    conn.execute("INSERT INTO t VALUES (2)").unwrap();

    // Statement 3: Insert row 1 again - fails with PK violation
    let result = conn.execute("INSERT INTO t VALUES (1)");
    assert!(result.is_err(), "Expected primary key violation");

    // COMMIT - should preserve statements 1 and 2
    conn.execute("COMMIT").unwrap();

    // Verify rows 1 and 2 exist
    let rows = get_rows(&conn, "SELECT * FROM t ORDER BY id");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[1][0].as_int().unwrap(), 2);

    // Integrity check
    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

/// Test that when the same row is modified by multiple statements,
/// and the second modification fails, the first modification is preserved.
#[test]
fn test_savepoint_same_row_multiple_statements() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER, other_unique INTEGER UNIQUE)")
        .unwrap();

    // Insert initial row and a row to cause conflict
    conn.execute("INSERT INTO t VALUES (1, 100, 1)").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 200, 2)").unwrap();

    // Start interactive transaction
    conn.execute("BEGIN CONCURRENT").unwrap();

    // Statement 1: Update row 1's value to 150 - success
    conn.execute("UPDATE t SET v = 150 WHERE id = 1").unwrap();

    // Statement 2: Try to update row 1 with conflicting other_unique - fails
    let result = conn.execute("UPDATE t SET v = 175, other_unique = 2 WHERE id = 1");
    assert!(result.is_err(), "Expected unique constraint violation");

    // COMMIT - should preserve statement 1's change (v = 150)
    conn.execute("COMMIT").unwrap();

    // Verify row 1 has v = 150 (from statement 1), not 175 (from failed statement 2)
    let rows = get_rows(&conn, "SELECT v, other_unique FROM t WHERE id = 1");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 150);
    assert_eq!(rows[0][1].as_int().unwrap(), 1); // other_unique unchanged

    // Integrity check
    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

/// Test that index operations are properly tracked per-statement.
/// When a statement fails after partially modifying indexes,
/// only that statement's index changes are rolled back.
#[test]
fn test_savepoint_index_multiple_statements() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute(
        "CREATE TABLE t(
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE,
            value INTEGER UNIQUE
        )",
    )
    .unwrap();

    // Insert rows
    conn.execute("INSERT INTO t VALUES (1, 'a', 10)").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'b', 20)").unwrap();

    // Start interactive transaction
    conn.execute("BEGIN CONCURRENT").unwrap();

    // Statement 1: Successfully change name for row 1
    conn.execute("UPDATE t SET name = 'c' WHERE id = 1")
        .unwrap();

    // Statement 2: Try to change name to 'b' (conflict with row 2) - fails
    let result = conn.execute("UPDATE t SET name = 'b' WHERE id = 1");
    assert!(
        result.is_err(),
        "Expected unique constraint violation on name"
    );

    // COMMIT
    conn.execute("COMMIT").unwrap();

    // Verify row 1 has name 'c' from statement 1
    let rows = get_rows(&conn, "SELECT name FROM t WHERE id = 1");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].cast_text().unwrap(), "c");

    // Verify index lookups work correctly
    let rows = get_rows(&conn, "SELECT id FROM t WHERE name = 'c'");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);

    // 'a' should no longer be in the index
    let rows = get_rows(&conn, "SELECT id FROM t WHERE name = 'a'");
    assert_eq!(rows.len(), 0);

    // 'b' should still point to row 2
    let rows = get_rows(&conn, "SELECT id FROM t WHERE name = 'b'");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 2);

    // Integrity check
    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

/// Test INSERT followed by DELETE of same row, then another statement fails.
/// The insert+delete should be preserved (row shouldn't exist).
#[test]
fn test_savepoint_insert_delete_then_fail() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER UNIQUE)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (2, 200)").unwrap();

    // Start interactive transaction
    conn.execute("BEGIN CONCURRENT").unwrap();

    // Statement 1: Insert row 1
    conn.execute("INSERT INTO t VALUES (1, 100)").unwrap();

    // Statement 2: Delete row 1
    conn.execute("DELETE FROM t WHERE id = 1").unwrap();

    // Statement 3: Try to insert with conflicting unique value - fails
    let result = conn.execute("INSERT INTO t VALUES (3, 200)");
    assert!(result.is_err(), "Expected unique constraint violation");

    // COMMIT
    conn.execute("COMMIT").unwrap();

    // Verify row 1 does not exist (was deleted in statement 2)
    let rows = get_rows(&conn, "SELECT * FROM t WHERE id = 1");
    assert_eq!(rows.len(), 0);

    // Row 2 should still exist
    let rows = get_rows(&conn, "SELECT * FROM t WHERE id = 2");
    assert_eq!(rows.len(), 1);

    // Integrity check
    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

/// Test DELETE all B-tree rows and re-insert with same IDs in MVCC.
/// Verifies tombstones correctly shadow B-tree and new rows are visible.
///
/// This test was initially failing with "UNIQUE constraint failed: t.id"
/// Fixed by implementing dual-peek in the exists() method to check MVCC tombstones.
#[test]
fn test_mvcc_dual_cursor_delete_all_btree_reinsert() {
    let _ = tracing_subscriber::fmt::try_init();
    let mut db = MvccTestDbNoConn::new_with_random_db();

    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'old1')").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'old2')").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    }

    db.restart();

    let conn = db.connect();
    // Delete all B-tree rows
    conn.execute("DELETE FROM t WHERE id IN (1, 2)").unwrap();
    // Re-insert with new values
    conn.execute("INSERT INTO t VALUES (1, 'new1')").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'new2')").unwrap();

    // Should see new values, not old B-tree values
    let rows = get_rows(&conn, "SELECT id, val FROM t ORDER BY id");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][1].to_string(), "new1");
    assert_eq!(rows[1][1].to_string(), "new2");
}

#[test]
fn test_checkpoint_root_page_mismatch_with_index() {
    // Strategy:
    // 1. Create table1 with index, insert many rows to allocate many pages (e.g., pages 2-30)
    // 2. Create table2 with index (will get negative IDs like -35, -36)
    // 3. Insert into table2
    // 4. Checkpoint - table2 will be allocated to pages 32, 33 (after table1's pages)
    // 5. But schema update will do abs(-35) = 35, abs(-36) = 36 (WRONG!)
    // 6. Query table2 using index - will look for page 36 but data is in page 33

    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("PRAGMA journal_mode = 'experimental_mvcc'")
        .unwrap();

    // Create MULTIPLE tables to consume enough page numbers
    // so that test_table's allocated pages diverge from abs(negative_id)
    for table_num in 1..=30 {
        conn.execute(format!(
            "CREATE TABLE tbl{table_num} (id INTEGER PRIMARY KEY, data TEXT)",
        ))
        .unwrap();
        conn.execute(format!(
            "CREATE INDEX idx{table_num} ON tbl{table_num}(data)",
        ))
        .unwrap();

        // Insert data to force page allocation
        for i in 0..10 {
            let data = format!("data_{table_num}_{i}");
            conn.execute(format!("INSERT INTO tbl{table_num} VALUES ({i}, '{data}')",))
                .unwrap();
        }
    }

    println!("Created 30 tables with indexes and data");

    // Create test_table with UNIQUE index (auto-created for the key)
    conn.execute("CREATE TABLE test_table (key TEXT PRIMARY KEY, value TEXT)")
        .unwrap();

    // Check test_table's root pages (should be negative)
    let rows = get_rows(
        &conn,
        "SELECT name, rootpage FROM sqlite_schema WHERE tbl_name = 'test_table' ORDER BY name",
    );
    let table_root: i64 = rows
        .iter()
        .find(|r| r[0].to_string() == "test_table")
        .unwrap()[1]
        .to_string()
        .parse()
        .unwrap();
    let index_root: i64 = rows
        .iter()
        .find(|r| r[0].to_string().contains("autoindex"))
        .unwrap()[1]
        .to_string()
        .parse()
        .unwrap();
    assert!(
        table_root < 0,
        "test_table should have negative root before checkpoint"
    );
    assert!(
        index_root < 0,
        "test_table index should have negative root before checkpoint"
    );

    // Insert a row into test_table
    conn.execute("INSERT INTO test_table (key, value) VALUES ('test_key', 'test_value')")
        .unwrap();

    // Verify row exists before checkpoint
    let rows = get_rows(&conn, "SELECT value FROM test_table WHERE key = 'test_key'");
    assert_eq!(rows.len(), 1, "Row should exist before checkpoint");
    assert_eq!(rows[0][0].to_string(), "test_value");

    println!("Inserted row into test_table, verified it exists");

    // Run checkpoint
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    println!("Checkpoint complete");

    // Now try to query using the index - this is where the bug manifests
    // The query will use root_page from schema (e.g., abs(index_root) if bug exists)
    // But data is actually in the correct allocated page
    let rows = get_rows(&conn, "SELECT value FROM test_table WHERE key = 'test_key'");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].to_string(), "test_value", "Value should match");

    println!("Test passed - row found correctly after checkpoint");
}

#[test]
fn test_checkpoint_drop_table() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX idx_t_data ON t(data)").unwrap();

    // Insert data to force page allocation
    for i in 0..10 {
        let data = format!("data_{i}");
        conn.execute(format!("INSERT INTO t VALUES ({i}, '{data}')",))
            .unwrap();
    }
    conn.execute("DROP TABLE t").unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    drop(conn);

    db.restart();

    let conn = db.connect();
    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}
