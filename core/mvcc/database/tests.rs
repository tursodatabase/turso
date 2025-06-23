use super::*;
use crate::mvcc::clock::LocalClock;

fn test_db() -> MvStore<LocalClock> {
    let clock = LocalClock::new();
    let storage = crate::mvcc::persistent_storage::Storage::new_noop();
    MvStore::new(clock, storage)
}

#[test]
fn test_insert_read() {
    let db = test_db();

    let tx1 = db.begin_tx();
    let tx1_row = Row {
        id: RowID {
            table_id: 1,
            row_id: 1,
        },
        data: "Hello".to_string().into_bytes(),
    };
    db.insert(tx1, tx1_row.clone()).unwrap();
    let row = db
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
    db.commit_tx(tx1).unwrap();

    let tx2 = db.begin_tx();
    let row = db
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
    let db = test_db();
    let tx = db.begin_tx();
    let row = db.read(
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
    let db = test_db();

    let tx1 = db.begin_tx();
    let tx1_row = Row {
        id: RowID {
            table_id: 1,
            row_id: 1,
        },
        data: "Hello".to_string().into_bytes(),
    };
    db.insert(tx1, tx1_row.clone()).unwrap();
    let row = db
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
    db.delete(
        tx1,
        RowID {
            table_id: 1,
            row_id: 1,
        },
    )
    .unwrap();
    let row = db
        .read(
            tx1,
            RowID {
                table_id: 1,
                row_id: 1,
            },
        )
        .unwrap();
    assert!(row.is_none());
    db.commit_tx(tx1).unwrap();

    let tx2 = db.begin_tx();
    let row = db
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
    let db = test_db();
    let tx = db.begin_tx();
    assert!(!db
        .delete(
            tx,
            RowID {
                table_id: 1,
                row_id: 1
            }
        )
        .unwrap());
}

#[test]
fn test_commit() {
    let db = test_db();
    let tx1 = db.begin_tx();
    let tx1_row = Row {
        id: RowID {
            table_id: 1,
            row_id: 1,
        },
        data: "Hello".to_string().into_bytes(),
    };
    db.insert(tx1, tx1_row.clone()).unwrap();
    let row = db
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
    let tx1_updated_row = Row {
        id: RowID {
            table_id: 1,
            row_id: 1,
        },
        data: "World".to_string().into_bytes(),
    };
    db.update(tx1, tx1_updated_row.clone()).unwrap();
    let row = db
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
    db.commit_tx(tx1).unwrap();

    let tx2 = db.begin_tx();
    let row = db
        .read(
            tx2,
            RowID {
                table_id: 1,
                row_id: 1,
            },
        )
        .unwrap()
        .unwrap();
    db.commit_tx(tx2).unwrap();
    assert_eq!(tx1_updated_row, row);
    db.drop_unused_row_versions();
}

#[test]
fn test_rollback() {
    let db = test_db();
    let tx1 = db.begin_tx();
    let row1 = Row {
        id: RowID {
            table_id: 1,
            row_id: 1,
        },
        data: "Hello".to_string().into_bytes(),
    };
    db.insert(tx1, row1.clone()).unwrap();
    let row2 = db
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
    let row3 = Row {
        id: RowID {
            table_id: 1,
            row_id: 1,
        },
        data: "World".to_string().into_bytes(),
    };
    db.update(tx1, row3.clone()).unwrap();
    let row4 = db
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
    db.rollback_tx(tx1);
    let tx2 = db.begin_tx();
    let row5 = db
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
    let db = test_db();

    // T1 inserts a row with ID 1, but does not commit.
    let tx1 = db.begin_tx();
    let tx1_row = Row {
        id: RowID {
            table_id: 1,
            row_id: 1,
        },
        data: "Hello".to_string().into_bytes(),
    };
    db.insert(tx1, tx1_row.clone()).unwrap();
    let row = db
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

    // T2 attempts to delete row with ID 1, but fails because T1 has not committed.
    let tx2 = db.begin_tx();
    let tx2_row = Row {
        id: RowID {
            table_id: 1,
            row_id: 1,
        },
        data: "World".to_string().into_bytes(),
    };
    assert!(!db.update(tx2, tx2_row).unwrap());

    let row = db
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
    let db = test_db();

    // T1 inserts a row with ID 1, but does not commit.
    let tx1 = db.begin_tx();
    let row1 = Row {
        id: RowID {
            table_id: 1,
            row_id: 1,
        },
        data: "Hello".to_string().into_bytes(),
    };
    db.insert(tx1, row1).unwrap();

    // T2 attempts to read row with ID 1, but doesn't see one because T1 has not committed.
    let tx2 = db.begin_tx();
    let row2 = db
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
    let db = test_db();

    // T1 inserts a row with ID 1 and commits.
    let tx1 = db.begin_tx();
    let tx1_row = Row {
        id: RowID {
            table_id: 1,
            row_id: 1,
        },
        data: "Hello".to_string().into_bytes(),
    };
    db.insert(tx1, tx1_row.clone()).unwrap();
    db.commit_tx(tx1).unwrap();

    // T2 deletes row with ID 1, but does not commit.
    let tx2 = db.begin_tx();
    assert!(db
        .delete(
            tx2,
            RowID {
                table_id: 1,
                row_id: 1
            }
        )
        .unwrap());

    // T3 reads row with ID 1, but doesn't see the delete because T2 hasn't committed.
    let tx3 = db.begin_tx();
    let row = db
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
    let db = test_db();

    // T1 inserts a row with ID 1 and commits.
    let tx1 = db.begin_tx();
    let tx1_row = Row {
        id: RowID {
            table_id: 1,
            row_id: 1,
        },
        data: "First".to_string().into_bytes(),
    };
    db.insert(tx1, tx1_row.clone()).unwrap();
    let row = db
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
    db.commit_tx(tx1).unwrap();

    // T2 reads the row with ID 1 within an active transaction.
    let tx2 = db.begin_tx();
    let row = db
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
    let tx3 = db.begin_tx();
    let tx3_row = Row {
        id: RowID {
            table_id: 1,
            row_id: 1,
        },
        data: "Second".to_string().into_bytes(),
    };
    db.update(tx3, tx3_row).unwrap();
    db.commit_tx(tx3).unwrap();

    // T2 still reads the same version of the row as before.
    let row = db
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
    let tx2_newrow = Row {
        id: RowID {
            table_id: 1,
            row_id: 1,
        },
        data: "Third".to_string().into_bytes(),
    };
    let update_result = db.update(tx2, tx2_newrow);
    assert_eq!(Err(DatabaseError::WriteWriteConflict), update_result);
}

#[test]
fn test_lost_update() {
    let db = test_db();

    // T1 inserts a row with ID 1 and commits.
    let tx1 = db.begin_tx();
    let tx1_row = Row {
        id: RowID {
            table_id: 1,
            row_id: 1,
        },
        data: "Hello".to_string().into_bytes(),
    };
    db.insert(tx1, tx1_row.clone()).unwrap();
    let row = db
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
    db.commit_tx(tx1).unwrap();

    // T2 attempts to update row ID 1 within an active transaction.
    let tx2 = db.begin_tx();
    let tx2_row = Row {
        id: RowID {
            table_id: 1,
            row_id: 1,
        },
        data: "World".to_string().into_bytes(),
    };
    assert!(db.update(tx2, tx2_row.clone()).unwrap());

    // T3 also attempts to update row ID 1 within an active transaction.
    let tx3 = db.begin_tx();
    let tx3_row = Row {
        id: RowID {
            table_id: 1,
            row_id: 1,
        },
        data: "Hello, world!".to_string().into_bytes(),
    };
    assert_eq!(
        Err(DatabaseError::WriteWriteConflict),
        db.update(tx3, tx3_row)
    );

    db.commit_tx(tx2).unwrap();
    assert_eq!(Err(DatabaseError::TxTerminated), db.commit_tx(tx3));

    let tx4 = db.begin_tx();
    let row = db
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
    let db = test_db();

    // let's add $10 to my account since I like money
    let tx1 = db.begin_tx();
    let tx1_row = Row {
        id: RowID {
            table_id: 1,
            row_id: 1,
        },
        data: "10".to_string().into_bytes(),
    };
    db.insert(tx1, tx1_row.clone()).unwrap();
    db.commit_tx(tx1).unwrap();

    // but I like more money, so let me try adding $10 more
    let tx2 = db.begin_tx();
    let tx2_row = Row {
        id: RowID {
            table_id: 1,
            row_id: 1,
        },
        data: "20".to_string().into_bytes(),
    };
    assert!(db.update(tx2, tx2_row.clone()).unwrap());
    let row = db
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
    let tx3 = db.begin_tx();
    let row = db
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
    let db = test_db();

    let tx1 = db.begin_tx();

    let tx2 = db.begin_tx();
    let tx2_row = Row {
        id: RowID {
            table_id: 1,
            row_id: 1,
        },
        data: "10".to_string().into_bytes(),
    };
    db.insert(tx2, tx2_row).unwrap();

    // transaction in progress, so tx1 shouldn't be able to see the value
    let row = db
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
    db.commit_tx(tx2).unwrap();
    let row = db
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

use crate::mvcc::clock::LogicalClock;
use crate::mvcc::cursor::{BucketScanCursor, LazyScanCursor, ScanCursor};
use crate::mvcc::database::{MvStore, Row, RowID};
use crate::mvcc::persistent_storage::Storage;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};

// Simple atomic clock implementation for testing
struct TestClock {
    counter: AtomicU64,
}

impl TestClock {
    fn new(start: u64) -> Self {
        Self {
            counter: AtomicU64::new(start),
        }
    }
}

impl LogicalClock for TestClock {
    fn get_timestamp(&self) -> u64 {
        self.counter.fetch_add(1, Ordering::SeqCst)
    }

    fn reset(&self, ts: u64) {
        let current = self.counter.load(Ordering::SeqCst);
        if ts > current {
            self.counter.store(ts, Ordering::SeqCst);
        }
    }
}

fn setup_test_db() -> (Rc<MvStore<TestClock>>, u64) {
    let clock = TestClock::new(1);
    let storage = Storage::new_noop();
    let db = Rc::new(MvStore::new(clock, storage));
    let tx_id = db.begin_tx();

    let table_id = 1;
    let test_rows = [
        (5, b"row5".to_vec()),
        (10, b"row10".to_vec()),
        (15, b"row15".to_vec()),
        (20, b"row20".to_vec()),
        (30, b"row30".to_vec()),
    ];

    for (row_id, data) in test_rows.iter() {
        let id = RowID::new(table_id, *row_id);
        let row = Row::new(id, data.clone());
        db.insert(tx_id, row).unwrap();
    }

    db.commit_tx(tx_id).unwrap();

    let tx_id = db.begin_tx();
    (db, tx_id)
}

fn setup_sequential_db() -> (Rc<MvStore<TestClock>>, u64) {
    let clock = TestClock::new(1);
    let storage = Storage::new_noop();
    let db = Rc::new(MvStore::new(clock, storage));
    let tx_id = db.begin_tx();

    let table_id = 1;
    for i in 1..6 {
        let id = RowID::new(table_id, i);
        let data = format!("row{i}").into_bytes();
        let row = Row::new(id, data);
        db.insert(tx_id, row).unwrap();
    }

    db.commit_tx(tx_id).unwrap();

    let tx_id = db.begin_tx();
    (db, tx_id)
}

#[test]
fn test_lazy_scan_cursor_basic() {
    let (db, tx_id) = setup_sequential_db();
    let table_id = 1;

    let mut cursor = LazyScanCursor::new(db.clone(), tx_id, table_id).unwrap();

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
fn test_lazy_scan_cursor_with_gaps() {
    let (db, tx_id) = setup_test_db();
    let table_id = 1;

    let mut cursor = LazyScanCursor::new(db.clone(), tx_id, table_id).unwrap();

    // Check first row
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
fn test_bucket_scan_cursor_basic() {
    let (db, tx_id) = setup_sequential_db();
    let table_id = 1;

    // Create a bucket size that's smaller than the total rows
    let mut cursor = BucketScanCursor::new(db.clone(), tx_id, table_id, 3).unwrap();

    // Check first row
    assert!(!cursor.is_empty());
    let row = cursor.current_row().unwrap().unwrap();
    assert_eq!(row.id.row_id, 1);

    // Iterate through all rows
    let mut count = 1;
    let mut row_ids = Vec::new();
    row_ids.push(row.id.row_id);

    while cursor.forward() {
        count += 1;
        let row = cursor.current_row().unwrap().unwrap();
        row_ids.push(row.id.row_id);
    }

    // Should have found 5 rows
    assert_eq!(count, 5);
    assert_eq!(row_ids, vec![1, 2, 3, 4, 5]);

    // After the last row, is_empty should return true
    assert!(cursor.is_empty());
}

#[test]
fn test_bucket_scan_cursor_with_gaps() {
    let (db, tx_id) = setup_test_db();
    let table_id = 1;

    // Create a bucket size of 2 to force multiple bucket loads
    let mut cursor = BucketScanCursor::new(db.clone(), tx_id, table_id, 2).unwrap();

    // Check first row
    assert!(!cursor.is_empty());
    let row = cursor.current_row().unwrap().unwrap();
    assert_eq!(row.id.row_id, 5);

    // Test moving forward and checking IDs
    let expected_ids = [5, 10, 15, 20, 30];
    let mut row_ids = Vec::new();
    row_ids.push(row.id.row_id);

    while cursor.forward() {
        let row = cursor.current_row().unwrap().unwrap();
        row_ids.push(row.id.row_id);
    }

    // Should have all expected IDs
    assert_eq!(row_ids, expected_ids);

    // After the last row, is_empty should return true
    assert!(cursor.is_empty());
}

#[test]
fn test_scan_cursor_basic() {
    let (db, tx_id) = setup_sequential_db();
    let table_id = 1;

    let mut cursor = ScanCursor::new(db.clone(), tx_id, table_id).unwrap();

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
    let clock = TestClock::new(1);
    let storage = Storage::new_noop();
    let db = Rc::new(MvStore::new(clock, storage));
    let tx_id = db.begin_tx();
    let table_id = 1; // Empty table

    // Test LazyScanCursor with empty table
    let cursor = LazyScanCursor::new(db.clone(), tx_id, table_id).unwrap();
    assert!(cursor.is_empty());
    assert!(cursor.current_row_id().is_none());

    // Test BucketScanCursor with empty table
    let cursor = BucketScanCursor::new(db.clone(), tx_id, table_id, 10).unwrap();
    assert!(cursor.is_empty());
    assert!(cursor.current_row_id().is_none());

    // Test ScanCursor with empty table
    let cursor = ScanCursor::new(db.clone(), tx_id, table_id).unwrap();
    assert!(cursor.is_empty());
    assert!(cursor.current_row_id().is_none());
}

#[test]
fn test_cursor_modification_during_scan() {
    let (db, tx_id) = setup_sequential_db();
    let table_id = 1;

    let mut cursor = LazyScanCursor::new(db.clone(), tx_id, table_id).unwrap();

    // Read first row
    let first_row = cursor.current_row().unwrap().unwrap();
    assert_eq!(first_row.id.row_id, 1);

    // Insert a new row with ID between existing rows
    let new_row_id = RowID::new(table_id, 3);
    let new_row_data = b"new_row".to_vec();
    let new_row = Row::new(new_row_id, new_row_data);

    cursor.insert(new_row).unwrap();

    // Continue scanning - the cursor should still work correctly
    cursor.forward(); // Move to 2
    let row = cursor.current_row().unwrap().unwrap();
    assert_eq!(row.id.row_id, 2);

    cursor.forward(); // Move to 3 (our new row)
    let row = cursor.current_row().unwrap().unwrap();
    assert_eq!(row.id.row_id, 3);
    assert_eq!(row.data, b"new_row".to_vec());

    cursor.forward(); // Move to 4
    let row = cursor.current_row().unwrap().unwrap();
    assert_eq!(row.id.row_id, 4);
}

#[test]
fn test_bucket_scan_cursor_next_bucket() {
    let (db, tx_id) = setup_test_db();
    let table_id = 1;

    // Create a bucket size of 1 to force bucket loading for each row
    let mut cursor = BucketScanCursor::new(db.clone(), tx_id, table_id, 1).unwrap();

    // Get the first row
    assert!(!cursor.is_empty());
    let row = cursor.current_row().unwrap().unwrap();
    assert_eq!(row.id.row_id, 5);

    // Move to the next row - this should trigger next_bucket()
    assert!(cursor.forward());
    let row = cursor.current_row().unwrap().unwrap();
    assert_eq!(row.id.row_id, 10);

    // Move to the next row again
    assert!(cursor.forward());
    let row = cursor.current_row().unwrap().unwrap();
    assert_eq!(row.id.row_id, 15);

    // Continue to the end
    assert!(cursor.forward());
    assert_eq!(cursor.current_row().unwrap().unwrap().id.row_id, 20);

    assert!(cursor.forward());
    assert_eq!(cursor.current_row().unwrap().unwrap().id.row_id, 30);

    // Should be no more rows
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
    let current_tx = current_tx.read().unwrap();

    let rv_visible = |begin: TxTimestampOrID, end: Option<TxTimestampOrID>| {
        let row_version = RowVersion {
            begin,
            end,
            row: Row {
                id: RowID {
                    table_id: 1,
                    row_id: 1,
                },
                data: "testme".to_string().into_bytes(),
            },
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
