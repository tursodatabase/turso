//! Tests that an MVCC cursor does not walk past the row that it returns.
//!
//! A cursor merges the rows in the B-tree with the row versions in the MvStore. A source with
//! many entries that the transaction cannot see must not slow down each step that the other
//! source decides. The tests count the raw entries that cursors look at. The counters exist
//! only in debug builds.

use super::checkpoint::get_rows;
use super::test_db::MvccTestDbNoConn;
use std::sync::Arc;
use std::time::{Duration, Instant};
use turso_core::Connection;

/// Runs `work` and returns the MvStore entries and B-tree rows that cursors looked at during it,
/// and how long it took.
fn examined_during(db: &MvccTestDbNoConn, work: impl FnOnce()) -> (u64, u64, Duration) {
    let store = db.get_mvcc_store();
    let (mvcc_before, btree_before) = store.examined_entries();
    let start = Instant::now();
    work();
    let elapsed = start.elapsed();
    let (mvcc_after, btree_after) = store.examined_entries();
    (
        mvcc_after - mvcc_before,
        btree_after - btree_before,
        elapsed,
    )
}

fn run(conn: &Arc<Connection>, sql: &str) {
    conn.execute(sql).unwrap();
}

/// A join probes an index with an equality seek for each outer row and then steps past the
/// match. The rows after the match in the MvStore are all deleted by an earlier transaction, and
/// the B-tree still holds the next row that wins the merge.
#[test]
fn test_join_probe_does_not_walk_deleted_rows_in_the_mvstore() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    run(&conn, "CREATE TABLE t(id INTEGER PRIMARY KEY, a)");
    run(&conn, "CREATE INDEX ta ON t(a)");
    run(
        &conn,
        "INSERT INTO t SELECT value, value FROM generate_series(1, 20010)",
    );
    run(&conn, "CREATE TABLE s(id INTEGER PRIMARY KEY, x)");
    run(
        &conn,
        "INSERT INTO s SELECT value, 5 FROM generate_series(1, 500)",
    );
    run(&conn, "PRAGMA wal_checkpoint(TRUNCATE)");
    run(&conn, "DELETE FROM t WHERE a > 10");

    let (mvcc, btree, elapsed) = examined_during(&db, || {
        let rows = get_rows(&conn, "SELECT count(*) FROM s JOIN t ON t.a = s.x");
        assert_eq!(rows[0][0].as_int(), Some(500));
    });
    eprintln!("join probe: {mvcc} MvStore entries, {btree} B-tree rows, {elapsed:?}");
    // Each of the 500 probes may look at a few entries. Before the merge walk, each probe
    // looked at the 20000 deleted entries: 10 million in all.
    assert!(
        mvcc <= 1_000,
        "the join looked at {mvcc} MvStore entries, in proportion to the deleted rows"
    );
}

/// Equality lookups while another transaction holds many uncommitted rows with larger keys.
#[test]
fn test_lookups_do_not_walk_uncommitted_rows_above() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    run(&conn, "CREATE TABLE t(id INTEGER PRIMARY KEY, v)");
    run(&conn, "CREATE INDEX tv ON t(v)");
    run(
        &conn,
        "INSERT INTO t SELECT value, value FROM generate_series(1, 2000)",
    );
    run(&conn, "PRAGMA wal_checkpoint(TRUNCATE)");

    let other = db.connect();
    run(&other, "BEGIN CONCURRENT");
    run(
        &other,
        "INSERT INTO t SELECT value, value FROM generate_series(100001, 150000)",
    );

    let (mvcc, btree, elapsed) = examined_during(&db, || {
        for v in 1..=1000 {
            let rows = get_rows(&conn, &format!("SELECT id FROM t WHERE v = {v}"));
            assert_eq!(rows.len(), 1);
        }
    });
    eprintln!("lookups above: {mvcc} MvStore entries, {btree} B-tree rows, {elapsed:?}");
    assert!(
        mvcc <= 20_000,
        "1000 lookups looked at {mvcc} MvStore entries, in proportion to the uncommitted rows"
    );
    run(&other, "ROLLBACK");
}

/// Backwards lookups while another transaction holds many uncommitted rows with smaller keys.
#[test]
fn test_backwards_lookups_do_not_walk_uncommitted_rows_below() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    run(&conn, "CREATE TABLE t(id INTEGER PRIMARY KEY, v)");
    run(&conn, "CREATE INDEX tv ON t(v)");
    run(
        &conn,
        "INSERT INTO t SELECT value, value + 100000 FROM generate_series(1, 2000)",
    );
    run(&conn, "PRAGMA wal_checkpoint(TRUNCATE)");

    let other = db.connect();
    run(&other, "BEGIN CONCURRENT");
    run(
        &other,
        "INSERT INTO t SELECT value + 10000, value FROM generate_series(1, 50000)",
    );

    let (mvcc, btree, elapsed) = examined_during(&db, || {
        for v in 1..=1000 {
            let rows = get_rows(
                &conn,
                &format!(
                    "SELECT v FROM t WHERE v <= {} ORDER BY v DESC LIMIT 2",
                    v + 100001
                ),
            );
            assert_eq!(rows.len(), 2, "v = {v}: {rows:?}");
        }
    });
    eprintln!("lookups below: {mvcc} MvStore entries, {btree} B-tree rows, {elapsed:?}");
    assert!(
        mvcc <= 20_000,
        "1000 lookups looked at {mvcc} MvStore entries, in proportion to the uncommitted rows"
    );
    run(&other, "ROLLBACK");
}
