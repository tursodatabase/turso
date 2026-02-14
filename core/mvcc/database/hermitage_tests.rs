use crate::mvcc::database::tests::MvccTestDbNoConn;
use crate::Connection;
use crate::LimboError;
use crate::Value;
use std::sync::Arc;

//        /\
//       /  \
//      /    \
//     /______\
//     | .  . |    Hermitage
//     |  __  |    Transaction Isolation Tests
//     |_|  |_|
//
// These tests are adapted from https://github.com/ept/hermitage
// Turso MVCC implements snapshot isolation with eager write-write conflict detection:
//
//   - Snapshot is taken at BEGIN (not at first read like FoundationDB)
//   - Write-write conflicts are detected immediately at write time (WriteWriteConflict),
//     NOT deferred to commit (like FoundationDB)
//   - Transactions never see uncommitted changes from other active transactions (no dirty reads)
//   - Isolation level: snapshot isolation (prevents G0, G1a, G1b, G1c, OTV, PMP, P4, G-single)
//   - Does NOT prevent G2-item (write skew) or G2 (anti-dependency cycles) — those require serializable
//
// Comparison with hermitage reference databases:
//   FoundationDB (serializable): writes succeed locally, conflict checked at commit time.
//   Turso: fails eagerly at write time (WriteWriteConflict), no blocking.
//   FoundationDB also prevents G2-item and G2 (serializable) → Turso does not (snapshot isolation).
//   Postgres behavior varies by isolation level — see individual test comments.
//

trait FromValue {
    fn from_value(v: &Value) -> Self;
}

impl FromValue for i64 {
    fn from_value(v: &Value) -> Self {
        v.as_int().unwrap()
    }
}

impl FromValue for String {
    fn from_value(v: &Value) -> Self {
        v.to_text().unwrap().to_string()
    }
}

fn setup_hermitage_test() -> MvccTestDbNoConn {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    // Setup: create table test (id int primary key, value int);
    // insert into test (id, value) values (1, 10), (2, 20);
    conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value INT)")
        .unwrap();
    conn.execute("INSERT INTO test (id, value) VALUES (1, 10), (2, 20)")
        .unwrap();

    db
}

fn get_rows<T: FromValue>(conn: &Arc<Connection>, query: &str) -> Vec<Vec<T>> {
    let mut stmt = conn.prepare(query).unwrap();
    let mut rows = Vec::new();
    stmt.run_with_row_callback(|row| {
        let values = row.get_values().map(|v| T::from_value(v)).collect();
        rows.push(values);
        Ok(())
    })
    .unwrap();
    rows
}

fn read_value(conn: &Arc<Connection>, id: i64) -> i64 {
    let rows: Vec<Vec<i64>> = get_rows(conn, &format!("SELECT value FROM test WHERE id = {id}"));
    assert!(!rows.is_empty(), "No row found with id = {id}");
    rows[0][0]
}

/// Assert that SELECT * FROM test returns exactly the expected (id, value) pairs.
fn assert_rows(conn: &Arc<Connection>, expected: &[(i64, i64)]) {
    let rows: Vec<Vec<i64>> = get_rows(conn, "SELECT id, value FROM test ORDER BY id");
    let actual: Vec<(i64, i64)> = rows.iter().map(|r| (r[0], r[1])).collect();
    assert_eq!(actual, expected);
}

/// Verify the final committed state of rows in the database.
/// Takes a list of (row_key, expected_value) pairs.
fn verify_final_state(db: &MvccTestDbNoConn, expected: &[(i64, i64)]) {
    let conn = db.connect();
    for &(key, expected_value) in expected {
        assert_eq!(read_value(&conn, key), expected_value);
    }
}

/// G0: Write Cycles — https://jepsen.io/consistency/phenomena/g0
/// If two transactions try to update the same row, one should fail with a write-write conflict,
/// preventing a cycle of uncommitted updates that could lead to non-serializable behavior.
///
/// Turso: T2's write to the same row as T1 fails immediately with WriteWriteConflict.
/// Postgres read committed: T2 BLOCKS until T1 commits, then T2 overwrites T1. Both succeed.
/// FoundationDB: T2 writes locally, T2's commit is rejected.
#[test]
fn test_hermitage_g0_write_cycles_prevented() {
    let db = setup_hermitage_test();

    // T1 and T2 try to update the same row - second should fail
    let conn1 = db.connect();
    let conn2 = db.connect();

    conn1.execute("BEGIN CONCURRENT").unwrap();
    conn2.execute("BEGIN CONCURRENT").unwrap();

    // T1: update test set value = 11 where id = 1
    conn1
        .execute("UPDATE test SET value = 11 WHERE id = 1")
        .unwrap();

    // T2: update test set value = 12 where id = 1 -- should fail with write-write conflict
    let result = conn2.execute("UPDATE test SET value = 12 WHERE id = 1");
    assert!(matches!(result, Err(LimboError::WriteWriteConflict)));
    // T2: rollback (write-write conflict auto-rolls back the transaction)

    // T1: update test set value = 21 where id = 2
    conn1
        .execute("UPDATE test SET value = 21 WHERE id = 2")
        .unwrap();

    // T1: commit
    conn1.execute("COMMIT").unwrap();

    {
        // select * from test -- Shows 1 => 11, 2 => 21
        let conn_read = db.connect();
        assert_rows(&conn_read, &[(1, 11), (2, 21)]);
    }
}

/// G1a: Aborted Reads — https://jepsen.io/consistency/phenomena/g1a
/// If a transaction T1 updates a row but then aborts, another transaction T2 should never see
/// T1's uncommitted changes, even if T2 reads the same row before T1 aborts.
///
/// Since we don't allow reading uncommited data ever, we prevent g1a and also dirty update phenomena
/// (mentioned in the jepsen page)
///
/// Turso: T2 never sees T1's uncommitted write
/// Postgres read committed: T2 never sees T1's uncommitted write.
/// FoundationDB: T2 never sees T1's uncommitted write.
#[test]
fn test_hermitage_g1a_aborted_reads() {
    let db = setup_hermitage_test();

    let conn1 = db.connect();
    let conn2 = db.connect();

    conn1.execute("BEGIN CONCURRENT").unwrap();
    conn2.execute("BEGIN CONCURRENT").unwrap();

    // T1: update test set value = 101 where id = 1
    conn1
        .execute("UPDATE test SET value = 101 WHERE id = 1")
        .unwrap();

    // T2: select * from test -- Should still show 1 => 10 (original value)
    assert_rows(&conn2, &[(1, 10), (2, 20)]); // Should see original values, not 101

    // T1: abort/rollback
    conn1.execute("ROLLBACK").unwrap();

    // T2: select * from test -- Should still show 1 => 10 (original value)
    assert_rows(&conn2, &[(1, 10), (2, 20)]); // Still should see original values

    // T2: commit
    conn2.execute("COMMIT").unwrap();

    // Verify final state - should still have original values since T1 aborted
    verify_final_state(&db, &[(1, 10), (2, 20)]);
}

/// G1b: Intermediate Reads — https://jepsen.io/consistency/phenomena/g1b
/// If a transaction T1 writes a value, then overwrites it with a second value, another committed
/// transaction T2 should never see T1's first (intermediate) write — only the final committed value
/// or the original value from before T1 (depending on when T2's snapshot is taken).
/// Under snapshot isolation, T2 sees the value from its snapshot, not T1's intermediate or final write.
///
/// G1b: Intermediate Update - this is not present in hermitage but Jepsen has it:
///
/// "If writes are arbitrary transformations of values (rather than blind writes),
/// this definition of G1b is insufficient. Imagine an aborted transaction Ti writes an
/// intermediate version xi, and a committed transaction Tj writes xj = f(xi). Finally, a
/// committed transaction Tk reads xj. Clearly, intermediate state has leaked from Ti into Tk."
///
/// Turso never allows reading uncommitted data, so Tj would never be able to read xi from Ti, and
/// thus Tk would never see xj = f(xi) if Ti aborted. So we prevent this form of G1b as well.
///
/// Turso: T2 sees its snapshot value (10), not T1's intermediate (101) or final (11) write.
/// Postgres read committed: T2 sees T1's committed final value (11) after T1 commits.
/// FoundationDB: T2 sees its snapshot value (10).
#[test]
fn test_hermitage_g1b_intermediate_reads() {
    let db = setup_hermitage_test();

    let conn1 = db.connect();
    let conn2 = db.connect();

    conn1.execute("BEGIN CONCURRENT").unwrap();
    conn2.execute("BEGIN CONCURRENT").unwrap();

    // T1: update test set value = 101 where id = 1 (intermediate value)
    conn1
        .execute("UPDATE test SET value = 101 WHERE id = 1")
        .unwrap();

    // T2: select * from test -- Should still show 1 => 10 (original value)
    assert_rows(&conn2, &[(1, 10), (2, 20)]); // Should NOT see 101

    // T1: update test set value = 11 where id = 1 (final value)
    conn1
        .execute("UPDATE test SET value = 11 WHERE id = 1")
        .unwrap();

    // T1: commit
    conn1.execute("COMMIT").unwrap();

    // T2: select * from test
    // Turso (snapshot isolation): T2 still sees 10 from its snapshot.
    // FoundationDB: same — T2 still sees 10.
    // Postgres read committed: T2 sees 11 (T1's committed final value).
    assert_rows(&conn2, &[(1, 10), (2, 20)]);

    // T2: commit
    conn2.execute("COMMIT").unwrap();

    // Verify final state - should show final committed value of 11
    verify_final_state(&db, &[(1, 11)]);
}

/// G1c: Circular Information Flow — https://jepsen.io/consistency/phenomena/g1c
/// If two transactions T1 and T2 both update different rows but then read each other's updated
/// rows before either commits, they should not see each other's uncommitted changes,
/// preventing a cycle of intermediate reads.
///
/// Turso / Postgres (Read Committed) / FoundationDB: Neither transaction sees the other's
/// uncommitted write
#[test]
fn test_hermitage_g1c_circular_information_flow() {
    let db = setup_hermitage_test();

    let conn1 = db.connect();
    let conn2 = db.connect();

    conn1.execute("BEGIN CONCURRENT").unwrap();
    conn2.execute("BEGIN CONCURRENT").unwrap();

    // T1: update test set value = 11 where id = 1
    conn1
        .execute("UPDATE test SET value = 11 WHERE id = 1")
        .unwrap();

    // T2: update test set value = 22 where id = 2
    conn2
        .execute("UPDATE test SET value = 22 WHERE id = 2")
        .unwrap();

    // T1: select * from test where id = 2
    // Should still show 2 => 20 (original value), NOT T2's uncommitted 22
    assert_eq!(read_value(&conn1, 2), 20); // Should NOT see T2's update

    // T2: select * from test where id = 1
    // Should still show 1 => 10 (original value), NOT T1's uncommitted 11
    assert_eq!(read_value(&conn2, 1), 10); // Should NOT see T1's update

    // T1: commit
    conn1.execute("COMMIT").unwrap();

    // T2: commit
    conn2.execute("COMMIT").unwrap();

    // Verify final state - both updates should be committed
    verify_final_state(&db, &[(1, 11), (2, 22)]);
}

/// OTV: Observed Transaction Vanishes
/// if a transaction T1 updates a row but has not yet committed, and another transaction T2 tries
/// to update the same row and fails with a write-write conflict, then if a third transaction
/// T3 tries to read that row, it should not see T1's uncommitted changes "vanish" due to T2's
/// failed update. Instead, T3 should see the original value (since T1 is not committed yet)
///
/// IOW once a transaction's effects become visible to another transaction, they must not "vanish".
///
/// Turso: T2's write fails immediately with WriteWriteConflict. T3 sees original values
/// Postgres read committed: T2 BLOCKS until T1 commits, then succeeds. T3 sees committed values change.
/// FoundationDB: T2 writes locally, snapshot at first read, T2's commit is rejected.
#[test]
fn test_hermitage_otv_observed_transaction_vanishes() {
    // T3 should not see T1's changes "vanish"
    let db = setup_hermitage_test();

    let conn1 = db.connect();
    let conn2 = db.connect();
    let conn3 = db.connect();

    conn1.execute("BEGIN CONCURRENT").unwrap();
    conn2.execute("BEGIN CONCURRENT").unwrap();
    conn3.execute("BEGIN CONCURRENT").unwrap();

    // T1: update test set value = 11 where id = 1
    conn1
        .execute("UPDATE test SET value = 11 WHERE id = 1")
        .unwrap();

    // T1: update test set value = 19 where id = 2
    conn1
        .execute("UPDATE test SET value = 19 WHERE id = 2")
        .unwrap();

    // T2: update test set value = 12 where id = 1
    // (following blocks in Postgres)
    let result = conn2.execute("UPDATE test SET value = 12 WHERE id = 1");
    assert!(matches!(result, Err(LimboError::WriteWriteConflict)));
    // T2: rollback (write-write conflict auto-rolls back the transaction)

    // T1: commit
    conn1.execute("COMMIT").unwrap();

    // T3: select * from test where id = 1
    // With snapshot isolation, T3 sees the original value (10)
    // (with read committed, like in postgres, T3 would see T1's committed value (11))
    assert_eq!(read_value(&conn3, 1), 10); // Snapshot isolation

    // Start a new T2 transaction after T1 commits
    let conn2_new = db.connect();
    conn2_new.execute("BEGIN CONCURRENT").unwrap();

    // T2_new: update test set value = 12 where id = 1 (now succeeds)
    conn2_new
        .execute("UPDATE test SET value = 12 WHERE id = 1")
        .unwrap();

    // T2_new: update test set value = 18 where id = 2
    conn2_new
        .execute("UPDATE test SET value = 18 WHERE id = 2")
        .unwrap();

    // T3: select * from test where id = 2
    // Should still see original value with snapshot isolation
    assert_eq!(read_value(&conn3, 2), 20); // Snapshot isolation

    // T2_new: commit
    conn2_new.execute("COMMIT").unwrap();

    // T3: select * from test where id = 2
    // Still sees snapshot from T3's start
    assert_eq!(read_value(&conn3, 2), 20); // Consistent snapshot

    // T3: select * from test where id = 1
    // Still sees snapshot from T3's start
    assert_eq!(read_value(&conn3, 1), 10); // Consistent snapshot

    // T3: commit
    conn3.execute("COMMIT").unwrap();

    // Verify final state - T2_new's values are committed
    verify_final_state(&db, &[(1, 12), (2, 18)]);
}

/// PMP: Predicate-Many-Preceders (read predicates) — https://jepsen.io/consistency/phenomena/p3
/// If a transaction T1 reads rows matching a predicate, and another transaction T2 inserts a new
/// row that matches T1's predicate and commits, then if T1 tries to read again with the same
/// predicate, it should not see the new row (phantom prevention).
///
/// Turso: T1 does not see T2's inserted row (snapshot isolation prevents phantoms).
/// Postgres repeatable read: T1 does not see T2's inserted row.
/// FoundationDB: T1 does not see T2's inserted row.
#[test]
fn test_hermitage_pmp_predicate_many_preceders_read() {
    // T1 should not see T2's inserted row that matches T1's predicate
    let db = setup_hermitage_test();

    let conn1 = db.connect();
    let conn2 = db.connect();

    conn1.execute("BEGIN CONCURRENT").unwrap();
    conn2.execute("BEGIN CONCURRENT").unwrap();

    // T1: select * from test where value = 30 -- Returns nothing
    let rows: Vec<Vec<i64>> = get_rows(&conn1, "SELECT * FROM test WHERE value = 30");
    assert!(rows.is_empty()); // Should find nothing

    // T2: insert into test (id, value) values(3, 30)
    conn2
        .execute("INSERT INTO test (id, value) VALUES (3, 30)")
        .unwrap();

    // T2: commit
    conn2.execute("COMMIT").unwrap();

    // T1: select * from test where value % 3 = 0
    // This would match both value=30 (newly inserted) and any existing divisible by 3
    // With repeatable read/snapshot isolation, T1 should not see the new row
    let rows: Vec<Vec<i64>> = get_rows(&conn1, "SELECT * FROM test WHERE value % 3 = 0");
    assert!(rows.is_empty()); // Should still find nothing (phantom prevention)

    // T1: commit
    conn1.execute("COMMIT").unwrap();

    // Verify final state - the new row exists
    verify_final_state(&db, &[(3, 30)]);
}

/// PMP: Predicate-Many-Preceders (write predicates) — https://jepsen.io/consistency/phenomena/p3
/// If a transaction T1 updates rows matching a predicate, and another transaction T2 tries to
/// delete rows matching the same predicate, then T2 should fail with a write-write conflict,
/// even if T1 has not committed yet. This prevents lost updates and ensures that T2
/// does not delete rows that T1 is in the process of updating.
///
/// Turso: T2's delete fails immediately with WriteWriteConflict (T1 has uncommitted write on row 2).
/// Postgres repeatable read: T2's delete BLOCKS until T1 commits, then fails with serialization error.
/// FoundationDB: T2's delete succeeds locally, T2's commit is rejected.
#[test]
fn test_hermitage_pmp_predicate_many_preceders_write() {
    // T2's delete based on predicate conflicts with T1's update
    let db = setup_hermitage_test();

    let conn1 = db.connect();
    let conn2 = db.connect();

    conn1.execute("BEGIN CONCURRENT").unwrap();
    conn2.execute("BEGIN CONCURRENT").unwrap();

    // T1: update test set value = value + 10
    // Updates: 1 => 20, 2 => 30
    conn1.execute("UPDATE test SET value = value + 10").unwrap();

    // T2: select * from test where value = 20
    // T2 should see the original values (snapshot isolation)
    assert_eq!(read_value(&conn2, 2), 20); // Original value

    // T2: delete from test where value = 20
    // T2 tries to delete row 2 (which originally had value 20)
    let delete_result = conn2.execute("DELETE FROM test WHERE value = 20");
    assert!(matches!(delete_result, Err(LimboError::WriteWriteConflict)));

    // T1: commit
    conn1.execute("COMMIT").unwrap();

    // T2: rollback (write-write conflict auto-rolls back the transaction)

    // Verify final state - T1's updates are committed
    verify_final_state(&db, &[(1, 20), (2, 30)]);
}

/// P4: Lost Update — https://jepsen.io/consistency/phenomena/p4
/// If two transactions T1 and T2 both read the same row and then both try to update it, one of them
/// should fail with a write-write conflict, preventing the "lost update" scenario where one
/// transaction's update overwrites the other's without either being aware of the conflict.
///
/// Turso: T2's update fails immediately with WriteWriteConflict (T1 has uncommitted write).
/// Postgres repeatable read: T2's update BLOCKS until T1 commits, then fails with serialization error.
/// FoundationDB: T2's update succeeds locally, T2's commit is rejected.
#[test]
fn test_hermitage_p4_lost_update() {
    // T2's update should not be lost when T1 also updates
    let db = setup_hermitage_test();

    let conn1 = db.connect();
    let conn2 = db.connect();

    conn1.execute("BEGIN CONCURRENT").unwrap();
    conn2.execute("BEGIN CONCURRENT").unwrap();

    // T1: select * from test where id = 1
    assert_eq!(read_value(&conn1, 1), 10);

    // T2: select * from test where id = 1
    assert_eq!(read_value(&conn2, 1), 10);

    // T1: update test set value = 11 where id = 1
    conn1
        .execute("UPDATE test SET value = 11 WHERE id = 1")
        .unwrap();

    // T2: update test set value = 11 where id = 1
    let result = conn2.execute("UPDATE test SET value = 11 WHERE id = 1");
    assert!(matches!(result, Err(LimboError::WriteWriteConflict)));

    // T1: commit
    conn1.execute("COMMIT").unwrap();

    // T2: abort (write-write conflict auto-rolls back the transaction)

    // Verify final state - only T1's update is committed
    verify_final_state(&db, &[(1, 11)]);
}

/// G-single: Read Skew — https://jepsen.io/consistency/phenomena/g-single
/// If a transaction T1 reads a row and then another transaction T2 updates that row and commits,
/// then if T1 tries to read the same row again, it should still see the original value,
/// not an inconsistent state where T1 sees the updated value for some rows but not others.
///
/// Turso / Postgres (repeatable read) / FoundationDB: T1 sees original values throughout (snapshot isolation).
#[test]
fn test_hermitage_g_single_read_skew() {
    // T1 should see a consistent snapshot
    let db = setup_hermitage_test();

    let conn1 = db.connect();
    let conn2 = db.connect();

    conn1.execute("BEGIN CONCURRENT").unwrap();
    conn2.execute("BEGIN CONCURRENT").unwrap();

    // T1: select * from test where id = 1 -- Shows 1 => 10
    assert_eq!(read_value(&conn1, 1), 10);

    // T2: select * from test
    assert_rows(&conn2, &[(1, 10), (2, 20)]);

    // T2: update test set value = 12 where id = 1
    conn2
        .execute("UPDATE test SET value = 12 WHERE id = 1")
        .unwrap();

    // T2: update test set value = 18 where id = 2
    conn2
        .execute("UPDATE test SET value = 18 WHERE id = 2")
        .unwrap();

    // T2: commit
    conn2.execute("COMMIT").unwrap();

    // T1: select * from test
    // With snapshot isolation, T1 should still see the original values
    // This prevents read skew - T1 sees a consistent snapshot
    assert_rows(&conn1, &[(1, 10), (2, 20)]); // Original values, not 12/18

    // T1: commit
    conn1.execute("COMMIT").unwrap();

    // Verify final state - T2's updates are committed
    verify_final_state(&db, &[(1, 12), (2, 18)]);
}

/// G-single: Read Skew (predicate dependencies) — https://jepsen.io/consistency/phenomena/g-single
/// If a transaction T1 reads rows matching a predicate, and another transaction T2 updates those rows
/// and commits, then if T1 tries to read again with the same predicate, it should
/// still see the original values that matched the predicate, not the updated values,
/// preventing a skew where T1 sees some updated values but not others.
///
/// Turso / Postgres (repeatable read) / FoundationDB: T1 sees original values from its snapshot (snapshot isolation).
#[test]
fn test_hermitage_g_single_read_skew_predicate_dependencies() {
    // T1's snapshot should not see T2's predicate-changing update
    let db = setup_hermitage_test();

    let conn1 = db.connect();
    let conn2 = db.connect();

    conn1.execute("BEGIN CONCURRENT").unwrap();
    conn2.execute("BEGIN CONCURRENT").unwrap();

    // T1: select * from test where value % 5 = 0
    // Both 10 and 20 are divisible by 5, so returns both rows
    let rows: Vec<Vec<i64>> = get_rows(
        &conn1,
        "SELECT value FROM test WHERE value % 5 = 0 ORDER BY value",
    );
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], 10);
    assert_eq!(rows[1][0], 20);

    // T2: update test set value = 12 where value = 10
    conn2
        .execute("UPDATE test SET value = 12 WHERE value = 10")
        .unwrap();

    // T2: commit
    conn2.execute("COMMIT").unwrap();

    // T1: select * from test where value % 3 = 0
    // With snapshot isolation, T1 still sees (1, 10) and (2, 20) from its snapshot.
    // Neither 10 nor 20 is divisible by 3, so this should return nothing.
    // (The newly committed value 12 IS divisible by 3, but T1 must not see it.)
    let rows: Vec<Vec<i64>> = get_rows(&conn1, "SELECT * FROM test WHERE value % 3 = 0");
    assert!(rows.is_empty()); // Should find nothing (snapshot isolation)

    // T1: commit
    conn1.execute("COMMIT").unwrap();

    // Verify final state - T2's update is visible
    verify_final_state(&db, &[(1, 12)]);
}

/// G-single: Read Skew (write predicate) — https://jepsen.io/consistency/phenomena/g-single
/// If a transaction T1 reads rows matching a predicate, and another transaction T2 updates those rows
/// and commits, then if T1 tries to update rows matching the same predicate, it should fail with
/// a write-write conflict. This prevents T1 from updating rows based on a stale snapshot that has
/// been changed by T2, ensuring that T1 does not overwrite T2's changes without being aware of the conflict.
///
/// Turso: T1's delete fails immediately with WriteWriteConflict (T2 already committed a write to row 2).
/// Postgres repeatable read: T1's delete fails with serialization error at commit time (T1 blocks until T2 commits, then fails).
/// FoundationDB: T1's delete succeeds locally, T1's commit is rejected.
#[test]
fn test_hermitage_g_single_read_skew_write_predicate() {
    // T1's delete based on predicate should conflict
    let db = setup_hermitage_test();

    let conn1 = db.connect();
    let conn2 = db.connect();

    conn1.execute("BEGIN CONCURRENT").unwrap();
    conn2.execute("BEGIN CONCURRENT").unwrap();

    // T1: select * from test where id = 1 -- Shows 1 => 10
    assert_eq!(read_value(&conn1, 1), 10);

    // T2: select * from test
    assert_rows(&conn2, &[(1, 10), (2, 20)]);

    // T2: update test set value = 12 where id = 1
    conn2
        .execute("UPDATE test SET value = 12 WHERE id = 1")
        .unwrap();

    // T2: update test set value = 18 where id = 2
    conn2
        .execute("UPDATE test SET value = 18 WHERE id = 2")
        .unwrap();

    // T2: commit
    conn2.execute("COMMIT").unwrap();

    // T1: delete from test where value = 20
    // T1 sees row 2 with value 20 in its snapshot and tries to delete it
    // But T2 has already updated this row, causing a write-write conflict
    let delete_result = conn1.execute("DELETE FROM test WHERE value = 20");
    assert!(matches!(delete_result, Err(LimboError::WriteWriteConflict)));

    // T1: abort (write-write conflict auto-rolls back the transaction)

    // Verify final state - T2's updates are preserved
    verify_final_state(&db, &[(1, 12), (2, 18)]);
}

/// G-single: Read Skew (write interleaved) — https://jepsen.io/consistency/phenomena/g-single
/// Based on FoundationDB's g-single-write-2 test scenario. If a transaction T1 reads rows matching
/// a predicate, and another transaction T2 updates those rows and commits, then if T1 tries to
/// update rows matching the same predicate, it should fail with a write-write conflict.
/// If T1 then rolls back, it should not affect T2's committed changes, and
/// the final state should reflect T2's updates without any inconsistencies.
///
/// Turso: T2's update fails immediately with WriteWriteConflict (T1 has uncommitted delete).
///        Both roll back. Final state: original values (10, 20).
/// Postgres repeatable read: T2's update BLOCKS until T1 commits, then fails with serialization error.
/// FoundationDB: T2's update succeeds locally, T1 rolls back, T2 commits.
///        Final state: T2's values (12, 18) — conflicts are checked at commit time.
#[test]
fn test_hermitage_g_single_read_skew_write_interleaved() {
    // Interleaved operations with rollback
    let db = setup_hermitage_test();

    let conn1 = db.connect();
    let conn2 = db.connect();

    conn1.execute("BEGIN CONCURRENT").unwrap();
    conn2.execute("BEGIN CONCURRENT").unwrap();

    // T1: select * from test where id = 1 -- Shows 1 => 10
    assert_eq!(read_value(&conn1, 1), 10);

    // T2: select * from test
    assert_rows(&conn2, &[(1, 10), (2, 20)]);

    // T2: update test set value = 12 where id = 1
    conn2
        .execute("UPDATE test SET value = 12 WHERE id = 1")
        .unwrap();

    // T1: delete from test where value = 20 -- Tries to delete row 2
    conn1.execute("DELETE FROM test WHERE value = 20").unwrap(); // Should succeed since T2 hasn't updated row 2 yet

    // T2: update test set value = 18 where id = 2
    // This should fail with write-write conflict since T1 already deleted row 2
    let update_result = conn2.execute("UPDATE test SET value = 18 WHERE id = 2");
    assert!(matches!(update_result, Err(LimboError::WriteWriteConflict)));

    // T1: rollback
    conn1.execute("ROLLBACK").unwrap();

    // T2: rollback (write-write conflict auto-rolls back the transaction)

    // Verify final state - everything remains unchanged
    verify_final_state(&db, &[(1, 10), (2, 20)]);
}

/// G2-item: Write Skew — https://jepsen.io/consistency/phenomena/g2-item
/// If two transactions T1 and T2 both read the same rows and then both try to update different
/// rows based on that same snapshot, both should be able to commit successfully,
/// even though this can lead to an inconsistent state (write skew).
/// This is ALLOWED under snapshot isolation but would not be allowed under serializable.
///
/// Turso / Postgres (repeatable read): Both commit successfully — no write-write conflict
///     (different rows). Write skew ALLOWED.
/// FoundationDB: T2's commit is REJECTED — serializable prevents write skew via read-conflict tracking.
#[test]
fn test_hermitage_g2_item_write_skew() {
    // This test explicitly verifies that write skew DOES occur with snapshot isolation
    // This is the expected behavior unless serializable isolation is implemented
    let db = setup_hermitage_test();

    let conn1 = db.connect();
    let conn2 = db.connect();

    conn1.execute("BEGIN CONCURRENT").unwrap();
    conn2.execute("BEGIN CONCURRENT").unwrap();

    // Both transactions read both rows
    assert_rows(&conn1, &[(1, 10), (2, 20)]);
    assert_rows(&conn2, &[(1, 10), (2, 20)]);

    // Each updates a different row (no write-write conflict)
    conn1
        .execute("UPDATE test SET value = 11 WHERE id = 1")
        .unwrap();
    conn2
        .execute("UPDATE test SET value = 21 WHERE id = 2")
        .unwrap();

    // Both should successfully commit with snapshot isolation
    // (This demonstrates write skew is allowed)
    conn1.execute("COMMIT").unwrap();
    conn2.execute("COMMIT").unwrap();

    // Verify both updates succeeded (write skew occurred)
    verify_final_state(&db, &[(1, 11), (2, 21)]);
}

/// G2: Anti-Dependency Cycles (Fekete et al's two-edge example) — https://jepsen.io/consistency/phenomena/g2
/// If three transactions T1, T2, and T3 all read the same rows, and then T2 updates one of those
/// rows and commits, and then T3 reads (seeing T2's update) and commits, then if T1 tries to update
/// a different row based on its original snapshot, it should succeed under snapshot isolation (since there
/// are no write-write conflicts), even though this creates a dependency cycle
/// (T1 --rw--> T2 --wr--> T3 --rw--> T1) that would not be allowed under serializable.
///
/// T1 reads both rows, T2 updates row 2 and commits, T3 reads (sees T2's update) and commits,
/// then T1 updates row 1.
/// Turso: T1's update succeeds and commits — no write-write conflict (disjoint rows). G2 ALLOWED.
/// Postgres repeatable read: T1's update succeeds. G2 ALLOWED.
/// Postgres serializable: T1's update fails.
/// FoundationDB: T1's commit is REJECTED (serializable, detects read-write conflict on row 2).
#[test]
fn test_hermitage_g2_two_edges_fekete() {
    // G2-two-edges: Fekete et al's example with two anti-dependency edges
    let db = setup_hermitage_test();

    let conn1 = db.connect();
    let conn2 = db.connect();
    let conn3 = db.connect();

    // T1: begin (snapshot isolation)
    conn1.execute("BEGIN CONCURRENT").unwrap();

    // T1: select * from test -- Shows 1 => 10, 2 => 20
    // T1 reads row 2 (sees 20) — forms edge T1 --rw--> T2 (T2 will write row 2 later)
    assert_rows(&conn1, &[(1, 10), (2, 20)]);

    // T2: begin
    conn2.execute("BEGIN CONCURRENT").unwrap();

    // T2: update test set value = value + 5 where id = 2
    // T2 writes row 2 — completes edge T1 --rw--> T2
    conn2
        .execute("UPDATE test SET value = value + 5 WHERE id = 2")
        .unwrap();

    // T2: commit
    conn2.execute("COMMIT").unwrap();

    // T3: begin (after T2 committed)
    conn3.execute("BEGIN CONCURRENT").unwrap();

    // T3: select * from test -- sees T2's committed changes
    // T3 reads row 2 (sees 25, T2's write) — forms edge T2 --wr--> T3
    // T3 reads row 1 (sees 10) — forms edge T3 --rw--> T1 (T1 will write row 1 later)
    assert_rows(&conn3, &[(1, 10), (2, 25)]); // T2's update to row 2 visible

    // T3: commit
    conn3.execute("COMMIT").unwrap();

    // T1: update test set value = 0 where id = 1
    // T1 writes row 1 — completes edge T3 --rw--> T1
    // Cycle formed: T1 --rw--> T2 --wr--> T3 --rw--> T1
    // Snapshot isolation: succeeds (no write-write conflict, T1 writes row 1, T2 wrote row 2)
    // Serializable: would fail (cycle not allowed)
    conn1
        .execute("UPDATE test SET value = 0 WHERE id = 1")
        .unwrap();

    // T1: commit — succeeds under snapshot isolation
    conn1.execute("COMMIT").unwrap();

    // Verify final state - T1's update to row 1, T2's update to row 2
    verify_final_state(&db, &[(1, 0), (2, 25)]);
}

/// G2: Anti-Dependency Cycles — https://jepsen.io/consistency/phenomena/g2
/// If two transactions T1 and T2 both read using the same predicate and find no matching rows,
/// and then each inserts a new row that would match the other's predicate,
/// both should be able to commit successfully under snapshot isolation (no write-write conflict
/// since they insert different rows), even though this creates a dependency cycle
/// (T1 --rw--> T2 --rw--> T1) that would not be allowed under serializable isolation.
///
/// Both T1 and T2 read using a predicate (value % 3 = 0) and find no matching rows. Then each
/// inserts a new row that WOULD match the other's predicate (T1 inserts 30, T2 inserts 42).
/// Turso: Both commit successfully — inserts to different row IDs have no write-write conflict. G2 ALLOWED.
/// Postgres repeatable read: Both commit successfully — same behavior (snapshot isolation allows G2).
/// FoundationDB: T2's commit is REJECTED (serializable, detects predicate read-write conflict).
#[test]
fn test_hermitage_g2_anti_dependency_cycles() {
    // This test verifies that G2 anomalies DO occur with snapshot isolation
    let db = setup_hermitage_test();

    let conn1 = db.connect();
    let conn2 = db.connect();

    conn1.execute("BEGIN CONCURRENT").unwrap();
    conn2.execute("BEGIN CONCURRENT").unwrap();

    // select * from test where value % 3 = 0; -- T1. Returns nothing
    // select * from test where value % 3 = 0; -- T2. Returns nothing
    // T1's read misses T2's future insert — forms edge T1 --rw--> T2
    // T2's read misses T1's future insert — forms edge T2 --rw--> T1
    let rows: Vec<Vec<i64>> = get_rows(&conn1, "SELECT * FROM test WHERE value % 3 = 0");
    assert!(rows.is_empty());
    let rows: Vec<Vec<i64>> = get_rows(&conn2, "SELECT * FROM test WHERE value % 3 = 0");
    assert!(rows.is_empty());

    // T1 inserts row 3 (value 30, divisible by 3) — completes edge T2 --rw--> T1
    // T2 inserts row 4 (value 42, divisible by 3) — completes edge T1 --rw--> T2
    // Cycle formed: T1 --rw--> T2 --rw--> T1
    // Snapshot isolation: succeeds (different row IDs, no write-write conflict)
    // Serializable: would fail (cycle not allowed)
    conn1
        .execute("INSERT INTO test (id, value) VALUES (3, 30)")
        .unwrap();
    conn2
        .execute("INSERT INTO test (id, value) VALUES (4, 42)")
        .unwrap();

    conn1.execute("COMMIT").unwrap();
    conn2.execute("COMMIT").unwrap();

    // Verify both inserts succeeded — cycle occurred
    verify_final_state(&db, &[(3, 30), (4, 42)]);
}

/// Aborted Transaction Not Visible (simplified G1a variant)
/// A transaction that updates a row but then aborts does not make its changes visible
/// to other transactions. Unlike the G1a test where T2 reads before T1 aborts,
/// here T2 reads after T1 has already rolled back.
#[test]
fn test_hermitage_aborted_transaction_not_visible() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
        .unwrap();

    // T1 inserts and commits
    conn.execute("INSERT INTO test (id, value) VALUES (1, 'committed_data')")
        .unwrap();

    // T2 updates but aborts
    let conn2 = db.connect();
    conn2.execute("BEGIN CONCURRENT").unwrap();
    conn2
        .execute("UPDATE test SET value = 'aborted_data' WHERE id = 1")
        .unwrap();
    conn2.execute("ROLLBACK").unwrap();

    // T3 should see original committed data, not aborted changes
    let conn3 = db.connect();
    let rows: Vec<Vec<String>> = get_rows(&conn3, "SELECT value FROM test WHERE id = 1");
    // should see T1's data, not T2's
    assert_eq!(rows[0][0], "committed_data");
}

/// Write-Write Conflict (simplified G0 variant)
/// If two transactions try to update the same row, the second one should fail with a
/// write-write conflict, even if the first transaction has not committed yet.
#[test]
fn test_hermitage_write_write_conflict() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
        .unwrap();

    // setup initial data x=1
    conn.execute("INSERT INTO test (id, value) VALUES (1, 'x=1')")
        .unwrap();

    // conn1: start transaction and update x=2 (but don't commit yet)
    let conn1 = db.connect();
    conn1.execute("BEGIN CONCURRENT").unwrap();
    conn1
        .execute("UPDATE test SET value = 'x=2' WHERE id = 1")
        .unwrap();

    // conn2: start transaction and try to update x=3 - should fail with write-write conflict
    let conn2 = db.connect();
    conn2.execute("BEGIN CONCURRENT").unwrap();
    let update_result = conn2.execute("UPDATE test SET value = 'x=3' WHERE id = 1");
    assert!(matches!(update_result, Err(LimboError::WriteWriteConflict)));

    // conn1 should be able to commit successfully since it was first
    conn1.execute("COMMIT").unwrap();

    // verify final state - should see conn1's update (x=2)
    let conn3 = db.connect();
    let rows: Vec<Vec<String>> = get_rows(&conn3, "SELECT value FROM test WHERE id = 1");
    assert_eq!(rows[0][0], "x=2");
}
