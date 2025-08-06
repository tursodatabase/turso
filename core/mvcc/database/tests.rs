use super::*;
use crate::io::PlatformIO;
use crate::mvcc::clock::LocalClock;

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
        let db = Database::open_file(io.clone(), ":memory:", true, true).unwrap();
        let conn = db.connect().unwrap();
        let mvcc_store = db.mv_store.as_ref().unwrap().clone();
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
        let db = Database::open_file(io.clone(), ":memory:", true, true).unwrap();
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
        let db = Database::open_file(io.clone(), path.as_os_str().to_str().unwrap(), true, true)
            .unwrap();
        Self {
            db: Some(db),
            path: Some(path.to_str().unwrap().to_string()),
            _temp_dir: Some(temp_dir),
        }
    }

    /// Restarts the database, make sure there is no connection to the database open before calling this!
    pub fn restart(&mut self) {
        let io = Arc::new(PlatformIO::new().unwrap());
        let path = self.path.as_ref().unwrap();
        let db = Database::open_file(io.clone(), path, true, true).unwrap();
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
        self.get_db().mv_store.as_ref().unwrap().clone()
    }
}

pub(crate) fn generate_simple_string_row(table_id: u64, id: i64, data: &str) -> Row {
    let record = ImmutableRecord::from_values(&[Value::Text(Text::new(data))], 1);
    Row {
        id: RowID {
            table_id,
            row_id: id,
        },
        column_count: 1,
        data: record.as_blob().to_vec(),
    }
}

#[test]
fn test_insert_read() {
    let db = MvccTestDb::new();

    let tx1 = db.mvcc_store.begin_tx(db.conn.pager.borrow().clone());
    let tx1_row = generate_simple_string_row(1, 1, "Hello");
    db.mvcc_store.insert(tx1, tx1_row.clone()).unwrap();
    let row = db
        .mvcc_store
        .read(
            tx1,
            RowID {
                table_id: 1,
                row_id: 1,
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    let tx2 = db.mvcc_store.begin_tx(db.conn.pager.borrow().clone());
    let row = db
        .mvcc_store
        .read(
            tx2,
            RowID {
                table_id: 1,
                row_id: 1,
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);
}

#[test]
fn test_read_nonexistent() {
    let db = MvccTestDb::new();
    let tx = db.mvcc_store.begin_tx(db.conn.pager.borrow().clone());
    let row = db.mvcc_store.read(
        tx,
        RowID {
            table_id: 1,
            row_id: 1,
        },
    );
    assert!(row.unwrap().is_none());
}

#[test]
fn test_delete() {
    let db = MvccTestDb::new();

    let tx1 = db.mvcc_store.begin_tx(db.conn.pager.borrow().clone());
    let tx1_row = generate_simple_string_row(1, 1, "Hello");
    db.mvcc_store.insert(tx1, tx1_row.clone()).unwrap();
    let row = db
        .mvcc_store
        .read(
            tx1,
            RowID {
                table_id: 1,
                row_id: 1,
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);
    db.mvcc_store
        .delete(
            tx1,
            RowID {
                table_id: 1,
                row_id: 1,
            },
            db.conn.pager.borrow().clone(),
        )
        .unwrap();
    let row = db
        .mvcc_store
        .read(
            tx1,
            RowID {
                table_id: 1,
                row_id: 1,
            },
        )
        .unwrap();
    assert!(row.is_none());
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    let tx2 = db.mvcc_store.begin_tx(db.conn.pager.borrow().clone());
    let row = db
        .mvcc_store
        .read(
            tx2,
            RowID {
                table_id: 1,
                row_id: 1,
            },
        )
        .unwrap();
    assert!(row.is_none());
}

#[test]
fn test_delete_nonexistent() {
    let db = MvccTestDb::new();
    let tx = db.mvcc_store.begin_tx(db.conn.pager.borrow().clone());
    assert!(!db
        .mvcc_store
        .delete(
            tx,
            RowID {
                table_id: 1,
                row_id: 1
            },
            db.conn.pager.borrow().clone(),
        )
        .unwrap());
}

#[test]
fn test_commit() {
    let db = MvccTestDb::new();
    let tx1 = db.mvcc_store.begin_tx(db.conn.pager.borrow().clone());
    let tx1_row = generate_simple_string_row(1, 1, "Hello");
    db.mvcc_store.insert(tx1, tx1_row.clone()).unwrap();
    let row = db
        .mvcc_store
        .read(
            tx1,
            RowID {
                table_id: 1,
                row_id: 1,
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);
    let tx1_updated_row = generate_simple_string_row(1, 1, "World");
    db.mvcc_store
        .update(tx1, tx1_updated_row.clone(), db.conn.pager.borrow().clone())
        .unwrap();
    let row = db
        .mvcc_store
        .read(
            tx1,
            RowID {
                table_id: 1,
                row_id: 1,
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_updated_row, row);
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    let tx2 = db.mvcc_store.begin_tx(db.conn.pager.borrow().clone());
    let row = db
        .mvcc_store
        .read(
            tx2,
            RowID {
                table_id: 1,
                row_id: 1,
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
    let tx1 = db.mvcc_store.begin_tx(db.conn.pager.borrow().clone());
    let row1 = generate_simple_string_row(1, 1, "Hello");
    db.mvcc_store.insert(tx1, row1.clone()).unwrap();
    let row2 = db
        .mvcc_store
        .read(
            tx1,
            RowID {
                table_id: 1,
                row_id: 1,
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(row1, row2);
    let row3 = generate_simple_string_row(1, 1, "World");
    db.mvcc_store
        .update(tx1, row3.clone(), db.conn.pager.borrow().clone())
        .unwrap();
    let row4 = db
        .mvcc_store
        .read(
            tx1,
            RowID {
                table_id: 1,
                row_id: 1,
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(row3, row4);
    db.mvcc_store
        .rollback_tx(tx1, db.conn.pager.borrow().clone());
    let tx2 = db.mvcc_store.begin_tx(db.conn.pager.borrow().clone());
    let row5 = db
        .mvcc_store
        .read(
            tx2,
            RowID {
                table_id: 1,
                row_id: 1,
            },
        )
        .unwrap();
    assert_eq!(row5, None);
}

#[test]
fn test_dirty_write() {
    let db = MvccTestDb::new();

    // T1 inserts a row with ID 1, but does not commit.
    let tx1 = db.mvcc_store.begin_tx(db.conn.pager.borrow().clone());
    let tx1_row = generate_simple_string_row(1, 1, "Hello");
    db.mvcc_store.insert(tx1, tx1_row.clone()).unwrap();
    let row = db
        .mvcc_store
        .read(
            tx1,
            RowID {
                table_id: 1,
                row_id: 1,
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);

    let conn2 = db.db.connect().unwrap();
    // T2 attempts to delete row with ID 1, but fails because T1 has not committed.
    let tx2 = db.mvcc_store.begin_tx(conn2.pager.borrow().clone());
    let tx2_row = generate_simple_string_row(1, 1, "World");
    assert!(!db
        .mvcc_store
        .update(tx2, tx2_row, conn2.pager.borrow().clone())
        .unwrap());

    let row = db
        .mvcc_store
        .read(
            tx1,
            RowID {
                table_id: 1,
                row_id: 1,
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
    let tx1 = db.mvcc_store.begin_tx(db.conn.pager.borrow().clone());
    let row1 = generate_simple_string_row(1, 1, "Hello");
    db.mvcc_store.insert(tx1, row1).unwrap();

    // T2 attempts to read row with ID 1, but doesn't see one because T1 has not committed.
    let conn2 = db.db.connect().unwrap();
    let tx2 = db.mvcc_store.begin_tx(conn2.pager.borrow().clone());
    let row2 = db
        .mvcc_store
        .read(
            tx2,
            RowID {
                table_id: 1,
                row_id: 1,
            },
        )
        .unwrap();
    assert_eq!(row2, None);
}

#[test]
fn test_dirty_read_deleted() {
    let db = MvccTestDb::new();

    // T1 inserts a row with ID 1 and commits.
    let tx1 = db.mvcc_store.begin_tx(db.conn.pager.borrow().clone());
    let tx1_row = generate_simple_string_row(1, 1, "Hello");
    db.mvcc_store.insert(tx1, tx1_row.clone()).unwrap();
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    // T2 deletes row with ID 1, but does not commit.
    let conn2 = db.db.connect().unwrap();
    let tx2 = db.mvcc_store.begin_tx(conn2.pager.borrow().clone());
    assert!(db
        .mvcc_store
        .delete(
            tx2,
            RowID {
                table_id: 1,
                row_id: 1
            },
            conn2.pager.borrow().clone(),
        )
        .unwrap());

    // T3 reads row with ID 1, but doesn't see the delete because T2 hasn't committed.
    let conn3 = db.db.connect().unwrap();
    let tx3 = db.mvcc_store.begin_tx(conn3.pager.borrow().clone());
    let row = db
        .mvcc_store
        .read(
            tx3,
            RowID {
                table_id: 1,
                row_id: 1,
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
    let tx1 = db.mvcc_store.begin_tx(db.conn.pager.borrow().clone());
    let tx1_row = generate_simple_string_row(1, 1, "First");
    db.mvcc_store.insert(tx1, tx1_row.clone()).unwrap();
    let row = db
        .mvcc_store
        .read(
            tx1,
            RowID {
                table_id: 1,
                row_id: 1,
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    // T2 reads the row with ID 1 within an active transaction.
    let conn2 = db.db.connect().unwrap();
    let tx2 = db.mvcc_store.begin_tx(conn2.pager.borrow().clone());
    let row = db
        .mvcc_store
        .read(
            tx2,
            RowID {
                table_id: 1,
                row_id: 1,
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);

    // T3 updates the row and commits.
    let conn3 = db.db.connect().unwrap();
    let tx3 = db.mvcc_store.begin_tx(conn3.pager.borrow().clone());
    let tx3_row = generate_simple_string_row(1, 1, "Second");
    db.mvcc_store
        .update(tx3, tx3_row, conn3.pager.borrow().clone())
        .unwrap();
    commit_tx(db.mvcc_store.clone(), &conn3, tx3).unwrap();

    // T2 still reads the same version of the row as before.
    let row = db
        .mvcc_store
        .read(
            tx2,
            RowID {
                table_id: 1,
                row_id: 1,
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);

    // T2 tries to update the row, but fails because T3 has already committed an update to the row,
    // so T2 trying to write would violate snapshot isolation if it succeeded.
    let tx2_newrow = generate_simple_string_row(1, 1, "Third");
    let update_result = db
        .mvcc_store
        .update(tx2, tx2_newrow, conn2.pager.borrow().clone());
    assert!(matches!(update_result, Err(LimboError::WriteWriteConflict)));
}

#[test]
fn test_lost_update() {
    let db = MvccTestDb::new();

    // T1 inserts a row with ID 1 and commits.
    let tx1 = db.mvcc_store.begin_tx(db.conn.pager.borrow().clone());
    let tx1_row = generate_simple_string_row(1, 1, "Hello");
    db.mvcc_store.insert(tx1, tx1_row.clone()).unwrap();
    let row = db
        .mvcc_store
        .read(
            tx1,
            RowID {
                table_id: 1,
                row_id: 1,
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    // T2 attempts to update row ID 1 within an active transaction.
    let conn2 = db.db.connect().unwrap();
    let tx2 = db.mvcc_store.begin_tx(conn2.pager.borrow().clone());
    let tx2_row = generate_simple_string_row(1, 1, "World");
    assert!(db
        .mvcc_store
        .update(tx2, tx2_row.clone(), conn2.pager.borrow().clone())
        .unwrap());

    // T3 also attempts to update row ID 1 within an active transaction.
    let conn3 = db.db.connect().unwrap();
    let tx3 = db.mvcc_store.begin_tx(conn3.pager.borrow().clone());
    let tx3_row = generate_simple_string_row(1, 1, "Hello, world!");
    assert!(matches!(
        db.mvcc_store
            .update(tx3, tx3_row, conn3.pager.borrow().clone(),),
        Err(LimboError::WriteWriteConflict)
    ));

    commit_tx(db.mvcc_store.clone(), &conn2, tx2).unwrap();
    assert!(matches!(
        commit_tx(db.mvcc_store.clone(), &conn3, tx3),
        Err(LimboError::TxTerminated)
    ));

    let conn4 = db.db.connect().unwrap();
    let tx4 = db.mvcc_store.begin_tx(conn4.pager.borrow().clone());
    let row = db
        .mvcc_store
        .read(
            tx4,
            RowID {
                table_id: 1,
                row_id: 1,
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
    let tx1 = db.mvcc_store.begin_tx(db.conn.pager.borrow().clone());
    let tx1_row = generate_simple_string_row(1, 1, "10");
    db.mvcc_store.insert(tx1, tx1_row.clone()).unwrap();
    commit_tx(db.mvcc_store.clone(), &db.conn, tx1).unwrap();

    // but I like more money, so let me try adding $10 more
    let conn2 = db.db.connect().unwrap();
    let tx2 = db.mvcc_store.begin_tx(conn2.pager.borrow().clone());
    let tx2_row = generate_simple_string_row(1, 1, "20");
    assert!(db
        .mvcc_store
        .update(tx2, tx2_row.clone(), conn2.pager.borrow().clone())
        .unwrap());
    let row = db
        .mvcc_store
        .read(
            tx2,
            RowID {
                table_id: 1,
                row_id: 1,
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(row, tx2_row);

    // can I check how much money I have?
    let conn3 = db.db.connect().unwrap();
    let tx3 = db.mvcc_store.begin_tx(conn3.pager.borrow().clone());
    let row = db
        .mvcc_store
        .read(
            tx3,
            RowID {
                table_id: 1,
                row_id: 1,
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

    let tx1 = db.mvcc_store.begin_tx(db.conn.pager.borrow().clone());

    let conn2 = db.db.connect().unwrap();
    let tx2 = db.mvcc_store.begin_tx(conn2.pager.borrow().clone());
    let tx2_row = generate_simple_string_row(1, 1, "Hello");
    db.mvcc_store.insert(tx2, tx2_row).unwrap();

    // transaction in progress, so tx1 shouldn't be able to see the value
    let row = db
        .mvcc_store
        .read(
            tx1,
            RowID {
                table_id: 1,
                row_id: 1,
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
                table_id: 1,
                row_id: 1,
            },
        )
        .unwrap();
    assert_eq!(row, None);
}

use crate::mvcc::cursor::MvccLazyCursor;
use crate::mvcc::database::{MvStore, Row, RowID};
use crate::types::Text;
use crate::Database;
use crate::MemoryIO;
use crate::RefValue;
use crate::Value;

// Simple atomic clock implementation for testing

fn setup_test_db() -> (MvccTestDb, u64) {
    let db = MvccTestDb::new();
    let tx_id = db.mvcc_store.begin_tx(db.conn.pager.borrow().clone());

    let table_id = 1;
    let test_rows = [
        (5, "row5"),
        (10, "row10"),
        (15, "row15"),
        (20, "row20"),
        (30, "row30"),
    ];

    for (row_id, data) in test_rows.iter() {
        let id = RowID::new(table_id, *row_id);
        let record = ImmutableRecord::from_values(&[Value::Text(Text::new(data))], 1);
        let row = Row::new(id, record.as_blob().to_vec(), 1);
        db.mvcc_store.insert(tx_id, row).unwrap();
    }

    commit_tx(db.mvcc_store.clone(), &db.conn, tx_id).unwrap();

    let tx_id = db.mvcc_store.begin_tx(db.conn.pager.borrow().clone());
    (db, tx_id)
}

fn setup_lazy_db(initial_keys: &[i64]) -> (MvccTestDb, u64) {
    let db = MvccTestDb::new();
    let tx_id = db.mvcc_store.begin_tx(db.conn.pager.borrow().clone());

    let table_id = 1;
    for i in initial_keys {
        let id = RowID::new(table_id, *i);
        let data = format!("row{i}");
        let record = ImmutableRecord::from_values(&[Value::Text(Text::new(&data))], 1);
        let row = Row::new(id, record.as_blob().to_vec(), 1);
        db.mvcc_store.insert(tx_id, row).unwrap();
    }

    commit_tx(db.mvcc_store.clone(), &db.conn, tx_id).unwrap();

    let tx_id = db.mvcc_store.begin_tx(db.conn.pager.borrow().clone());
    (db, tx_id)
}

pub(crate) fn commit_tx(
    mv_store: Arc<MvStore<LocalClock>>,
    conn: &Arc<Connection>,
    tx_id: u64,
) -> Result<()> {
    let mut sm = mv_store
        .commit_tx(tx_id, conn.pager.borrow().clone(), conn)
        .unwrap();
    let result = sm.step(&mv_store)?;
    assert!(sm.is_finalized());
    match result {
        TransitionResult::Done(()) => Ok(()),
        _ => unreachable!(),
    }
}

pub(crate) fn commit_tx_no_conn(
    db: &MvccTestDbNoConn,
    tx_id: u64,
    conn: &Arc<Connection>,
) -> Result<(), LimboError> {
    let mut sm = db
        .get_db()
        .get_mv_store()
        .unwrap()
        .commit_tx(tx_id, conn.pager.borrow().clone(), conn)
        .unwrap();
    let result = sm.step(&db.get_mvcc_store())?;
    assert!(sm.is_finalized());
    match result {
        TransitionResult::Done(()) => Ok(()),
        _ => unreachable!(),
    }
}

#[test]
fn test_lazy_scan_cursor_basic() {
    let (db, tx_id) = setup_lazy_db(&[1, 2, 3, 4, 5]);
    let table_id = 1;

    let mut cursor = MvccLazyCursor::new(
        db.mvcc_store.clone(),
        tx_id,
        table_id,
        db.conn.pager.borrow().clone(),
    )
    .unwrap();

    // Check first row
    assert!(cursor.forward());
    assert!(!cursor.is_empty());
    let row = cursor.current_row().unwrap().unwrap();
    assert_eq!(row.id.row_id, 1);

    // Iterate through all rows
    let mut count = 1;
    while cursor.forward() {
        count += 1;
        let row = cursor.current_row().unwrap().unwrap();
        assert_eq!(row.id.row_id, count);
    }

    // Should have found 5 rows
    assert_eq!(count, 5);

    // After the last row, is_empty should return true
    assert!(!cursor.forward());
    assert!(cursor.is_empty());
}

#[test]
fn test_lazy_scan_cursor_with_gaps() {
    let (db, tx_id) = setup_test_db();
    let table_id = 1;

    let mut cursor = MvccLazyCursor::new(
        db.mvcc_store.clone(),
        tx_id,
        table_id,
        db.conn.pager.borrow().clone(),
    )
    .unwrap();

    // Check first row
    assert!(cursor.forward());
    assert!(!cursor.is_empty());
    let row = cursor.current_row().unwrap().unwrap();
    assert_eq!(row.id.row_id, 5);

    // Test moving forward and checking IDs
    let expected_ids = [5, 10, 15, 20, 30];
    let mut index = 0;

    assert_eq!(cursor.current_row_id().unwrap().row_id, expected_ids[index]);

    while cursor.forward() {
        index += 1;
        if index < expected_ids.len() {
            assert_eq!(cursor.current_row_id().unwrap().row_id, expected_ids[index]);
        }
    }

    // Should have found all 5 rows
    assert_eq!(index, expected_ids.len() - 1);
}

#[test]
fn test_cursor_basic() {
    let (db, tx_id) = setup_lazy_db(&[1, 2, 3, 4, 5]);
    let table_id = 1;

    let mut cursor = MvccLazyCursor::new(
        db.mvcc_store.clone(),
        tx_id,
        table_id,
        db.conn.pager.borrow().clone(),
    )
    .unwrap();

    cursor.forward();

    // Check first row
    assert!(!cursor.is_empty());
    let row = cursor.current_row().unwrap().unwrap();
    assert_eq!(row.id.row_id, 1);

    // Iterate through all rows
    let mut count = 1;
    while cursor.forward() {
        count += 1;
        let row = cursor.current_row().unwrap().unwrap();
        assert_eq!(row.id.row_id, count);
    }

    // Should have found 5 rows
    assert_eq!(count, 5);

    // After the last row, is_empty should return true
    assert!(!cursor.forward());
    assert!(cursor.is_empty());
}

#[test]
fn test_cursor_with_empty_table() {
    let db = MvccTestDb::new();
    {
        // FIXME: force page 1 initialization
        let pager = db.conn.pager.borrow().clone();
        let tx_id = db.mvcc_store.begin_tx(pager.clone());
        commit_tx(db.mvcc_store.clone(), &db.conn, tx_id).unwrap();
    }
    let tx_id = db.mvcc_store.begin_tx(db.conn.pager.borrow().clone());
    let table_id = 1; // Empty table

    // Test LazyScanCursor with empty table
    let mut cursor = MvccLazyCursor::new(
        db.mvcc_store.clone(),
        tx_id,
        table_id,
        db.conn.pager.borrow().clone(),
    )
    .unwrap();
    assert!(cursor.is_empty());
    assert!(cursor.current_row_id().is_none());
}

#[test]
fn test_cursor_modification_during_scan() {
    let (db, tx_id) = setup_lazy_db(&[1, 2, 4, 5]);
    let table_id = 1;

    let mut cursor = MvccLazyCursor::new(
        db.mvcc_store.clone(),
        tx_id,
        table_id,
        db.conn.pager.borrow().clone(),
    )
    .unwrap();

    // Read first row
    assert!(cursor.forward());
    let first_row = cursor.current_row().unwrap().unwrap();
    assert_eq!(first_row.id.row_id, 1);

    // Insert a new row with ID between existing rows
    let new_row_id = RowID::new(table_id, 3);
    let new_row = generate_simple_string_row(table_id, new_row_id.row_id, "new_row");

    cursor.insert(new_row).unwrap();
    let row = db.mvcc_store.read(tx_id, new_row_id).unwrap().unwrap();
    let mut record = ImmutableRecord::new(1024);
    record.start_serialization(&row.data);
    let value = record.get_value(0).unwrap();
    match value {
        RefValue::Text(text) => {
            assert_eq!(text.as_str(), "new_row");
        }
        _ => panic!("Expected Text value"),
    }
    assert_eq!(row.id.row_id, 3);

    // Continue scanning - the cursor should still work correctly
    cursor.forward(); // Move to 4
    let row = db
        .mvcc_store
        .read(tx_id, RowID::new(table_id, 4))
        .unwrap()
        .unwrap();
    assert_eq!(row.id.row_id, 4);

    cursor.forward(); // Move to 5 (our new row)
    let row = db
        .mvcc_store
        .read(tx_id, RowID::new(table_id, 5))
        .unwrap()
        .unwrap();
    assert_eq!(row.id.row_id, 5);
    assert!(!cursor.forward());
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

fn new_tx(tx_id: TxID, begin_ts: u64, state: TransactionState) -> RwLock<Transaction> {
    let state = state.into();
    RwLock::new(Transaction {
        state,
        tx_id,
        begin_ts,
        write_set: SkipSet::new(),
        read_set: SkipSet::new(),
    })
}

#[test]
fn test_snapshot_isolation_tx_visible1() {
    let txs: SkipMap<TxID, RwLock<Transaction>> = SkipMap::from_iter([
        (1, new_tx(1, 1, TransactionState::Committed(2))),
        (2, new_tx(2, 2, TransactionState::Committed(5))),
        (3, new_tx(3, 3, TransactionState::Aborted)),
        (5, new_tx(5, 5, TransactionState::Preparing)),
        (6, new_tx(6, 6, TransactionState::Committed(10))),
        (7, new_tx(7, 7, TransactionState::Active)),
    ]);

    let current_tx = new_tx(4, 4, TransactionState::Preparing);
    let current_tx = current_tx.read();

    let rv_visible = |begin: TxTimestampOrID, end: Option<TxTimestampOrID>| {
        let row_version = RowVersion {
            begin,
            end,
            row: generate_simple_string_row(1, 1, "testme"),
        };
        tracing::debug!("Testing visibility of {row_version:?}");
        row_version.is_visible_to(&current_tx, &txs)
    };

    // begin visible:   transaction committed with ts < current_tx.begin_ts
    // end visible:     inf
    assert!(rv_visible(TxTimestampOrID::TxID(1), None));

    // begin invisible: transaction committed with ts > current_tx.begin_ts
    assert!(!rv_visible(TxTimestampOrID::TxID(2), None));

    // begin invisible: transaction aborted
    assert!(!rv_visible(TxTimestampOrID::TxID(3), None));

    // begin visible:   timestamp < current_tx.begin_ts
    // end invisible:   transaction committed with ts > current_tx.begin_ts
    assert!(!rv_visible(
        TxTimestampOrID::Timestamp(0),
        Some(TxTimestampOrID::TxID(1))
    ));

    // begin visible:   timestamp < current_tx.begin_ts
    // end visible:     transaction committed with ts < current_tx.begin_ts
    assert!(rv_visible(
        TxTimestampOrID::Timestamp(0),
        Some(TxTimestampOrID::TxID(2))
    ));

    // begin visible:   timestamp < current_tx.begin_ts
    // end invisible:   transaction aborted
    assert!(!rv_visible(
        TxTimestampOrID::Timestamp(0),
        Some(TxTimestampOrID::TxID(3))
    ));

    // begin invisible: transaction preparing
    assert!(!rv_visible(TxTimestampOrID::TxID(5), None));

    // begin invisible: transaction committed with ts > current_tx.begin_ts
    assert!(!rv_visible(TxTimestampOrID::TxID(6), None));

    // begin invisible: transaction active
    assert!(!rv_visible(TxTimestampOrID::TxID(7), None));

    // begin invisible: transaction committed with ts > current_tx.begin_ts
    assert!(!rv_visible(TxTimestampOrID::TxID(6), None));

    // begin invisible:   transaction active
    assert!(!rv_visible(TxTimestampOrID::TxID(7), None));

    // begin visible:   timestamp < current_tx.begin_ts
    // end invisible:     transaction preparing
    assert!(!rv_visible(
        TxTimestampOrID::Timestamp(0),
        Some(TxTimestampOrID::TxID(5))
    ));

    // begin invisible: timestamp > current_tx.begin_ts
    assert!(!rv_visible(
        TxTimestampOrID::Timestamp(6),
        Some(TxTimestampOrID::TxID(6))
    ));

    // begin visible:   timestamp < current_tx.begin_ts
    // end visible:     some active transaction will eventually overwrite this version,
    //                  but that hasn't happened
    //                  (this is the https://avi.im/blag/2023/hekaton-paper-typo/ case, I believe!)
    assert!(rv_visible(
        TxTimestampOrID::Timestamp(0),
        Some(TxTimestampOrID::TxID(7))
    ));
}

#[test]
fn test_restart() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn = db.connect();
        let mvcc_store = db.get_mvcc_store();
        let tx_id = mvcc_store.begin_tx(conn.pager.borrow().clone());
        let row = generate_simple_string_row(1, 1, "foo");

        mvcc_store.insert(tx_id, row).unwrap();
        commit_tx(mvcc_store.clone(), &conn, tx_id).unwrap();
        conn.close().unwrap();
    }
    db.restart();

    {
        let conn = db.connect();
        let mvcc_store = db.get_mvcc_store();
        let tx_id = mvcc_store.begin_tx(conn.pager.borrow().clone());
        let row = generate_simple_string_row(1, 2, "bar");

        mvcc_store.insert(tx_id, row).unwrap();
        commit_tx(mvcc_store.clone(), &conn, tx_id).unwrap();

        let tx_id = mvcc_store.begin_tx(conn.pager.borrow().clone());
        let row = mvcc_store.read(tx_id, RowID::new(1, 2)).unwrap().unwrap();
        let record = get_record_value(&row);
        match record.get_value(0).unwrap() {
            RefValue::Text(text) => {
                assert_eq!(text.as_str(), "bar");
            }
            _ => panic!("Expected Text value"),
        }
        conn.close().unwrap();
    }
}

fn get_record_value(row: &Row) -> ImmutableRecord {
    let mut record = ImmutableRecord::new(1024);
    record.start_serialization(&row.data);
    record
}
