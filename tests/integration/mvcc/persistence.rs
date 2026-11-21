use super::checkpoint::{assert_integrity_ok, get_rows, wal_path_for_db, FixedYieldInjector};
use crate::mvcc::test_db::MvccTestDbNoConn;
use turso_core::mvcc::database::checkpoint::CheckpointState;
use turso_core::mvcc::database::checkpoint::CheckpointStateMachine;
use turso_core::mvcc::database::checkpoint::CheckpointYieldPoint;
use turso_core::mvcc::yield_hooks::YieldPointMarker;
use turso_core::state_machine::{StateTransition, TransitionResult};
use turso_core::CheckpointMode;

/// Truncate checkpoint must collect commits that land while waiting for `AcquireLock`, and zero the logical log.
#[test]
fn test_blocking_truncate_zeros_log_when_commit_races_acquire_lock() {
    use turso_core::StepResult;

    let _ = tracing_subscriber::fmt::try_init();
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t1 VALUES (0, 'seed')").unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    conn.execute("PRAGMA mvcc_checkpoint_threshold = 1000000")
        .unwrap();
    conn.execute("INSERT INTO t1 VALUES (1, 'pending')")
        .unwrap();

    let mvcc_store = db.get_mvcc_store();
    conn.execute("PRAGMA mvcc_checkpoint_threshold = 0")
        .unwrap();

    let injector = FixedYieldInjector::new([CheckpointYieldPoint::BeforeAcquireLock.point()]);
    conn.set_yield_injector(Some(injector.clone()));
    let mut insert_stmt = conn
        .prepare("INSERT INTO t1 VALUES (2, 'trigger')")
        .unwrap();
    let mut parked = false;
    for _ in 0..10_000 {
        match insert_stmt.step().unwrap() {
            StepResult::IO | StepResult::Yield if injector.is_empty() => {
                parked = true;
                break;
            }
            StepResult::IO | StepResult::Yield => {}
            StepResult::Done => {
                panic!("INSERT completed before the checkpoint acquire-lock yield fired")
            }
            other => panic!("unexpected INSERT step result before yield: {other:?}"),
        }
    }
    assert!(
        parked,
        "blocking TRUNCATE auto-checkpoint should yield before acquiring the lock"
    );

    let sibling = db.connect();
    sibling
        .execute("PRAGMA mvcc_checkpoint_threshold = 1000000")
        .unwrap();
    sibling
        .execute("CREATE TABLE t2(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    sibling.execute("INSERT INTO t2 VALUES (1, 'x')").unwrap();

    let mut finished = false;
    for _ in 0..100_000 {
        match insert_stmt.step().unwrap() {
            StepResult::Done => {
                finished = true;
                break;
            }
            StepResult::IO | StepResult::Yield => {}
            other => panic!("unexpected resume step result: {other:?}"),
        }
    }
    conn.set_yield_injector(None);
    assert!(
        finished,
        "TRUNCATE checkpoint must complete after sibling commit"
    );
    drop(insert_stmt);

    assert_eq!(
        mvcc_store.get_logical_log_file().size().unwrap(),
        0,
        "blocking TRUNCATE must zero the logical log even when a sibling commit raced acquire-lock"
    );
    let wal_path = wal_path_for_db(db.path());
    assert_eq!(
        wal_path.metadata().map(|m| m.len()).unwrap_or(0),
        0,
        "blocking TRUNCATE must zero the WAL"
    );

    let tables = get_rows(
        &conn,
        "SELECT name FROM sqlite_schema WHERE type='table' AND name IN ('t1','t2') ORDER BY name",
    );
    assert_eq!(tables.len(), 2, "both tables must exist: {tables:?}");
    let t2_rows = get_rows(&conn, "SELECT id, v FROM t2");
    assert_eq!(t2_rows.len(), 1);
    assert_eq!(t2_rows[0][0].as_int().unwrap(), 1);
    assert_eq!(t2_rows[0][1].to_string(), "x");

    assert_integrity_ok(&conn);

    drop(mvcc_store);
    drop(sibling);
    drop(conn);
    db.restart();
    let conn = db.connect();
    assert_integrity_ok(&conn);
    let t2_after_reopen = get_rows(&conn, "SELECT id, v FROM t2");
    assert_eq!(t2_after_reopen.len(), 1);
    assert_eq!(t2_after_reopen[0][1].to_string(), "x");
    assert_eq!(
        db.get_mvcc_store().get_logical_log_file().size().unwrap(),
        0,
        "logical log must stay empty after reopen following TRUNCATE checkpoint"
    );
}

/// Regression for https://github.com/tursodatabase/turso/issues/8076.
///
/// With `experimental_mvcc_passive_checkpoint` enabled, an explicit TRUNCATE checkpoint
/// collects rows without the blocking checkpoint lock and writers never take it, so another
/// connection can commit after the checkpoint snapshot. That commit is not in the B-tree
/// yet, so the checkpoint must keep it in the logical log instead of clearing the whole
/// file.
#[test]
fn test_passive_truncate_keeps_log_frames_committed_after_snapshot() {
    let mut db = MvccTestDbNoConn::new_with_random_db_passive();
    let conn = db.connect();
    conn.execute("PRAGMA mvcc_checkpoint_threshold = -1")
        .unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'collected')")
        .unwrap();

    let mvcc_store = db.get_mvcc_store();
    let pager = conn.get_pager();
    let mut checkpoint_sm = CheckpointStateMachine::new(
        pager.clone(),
        mvcc_store.clone(),
        conn.clone(),
        true,
        conn.get_sync_mode(),
        turso_core::MAIN_DB_ID,
        CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        },
    );

    while checkpoint_sm.state() != CheckpointState::BeginPagerTxn {
        match checkpoint_sm.step(&()).unwrap() {
            TransitionResult::Io(io) => io.wait(pager.io.as_ref()).unwrap(),
            TransitionResult::Continue => {}
            TransitionResult::Done(_) => panic!("checkpoint finished before collecting rows"),
        }
    }

    let sibling = db.connect();
    sibling
        .execute("INSERT INTO t VALUES (2, 'after-snapshot')")
        .unwrap();

    let mut finished = false;
    for _ in 0..50_000 {
        match checkpoint_sm.step(&()).unwrap() {
            TransitionResult::Io(io) => io.wait(pager.io.as_ref()).unwrap(),
            TransitionResult::Continue => {}
            TransitionResult::Done(_) => {
                finished = true;
                break;
            }
        }
    }
    assert!(
        finished,
        "TRUNCATE checkpoint must complete after the sibling commit"
    );
    assert!(
        mvcc_store.get_logical_log_file().size().unwrap() > 0,
        "logical log must keep the commit that landed after the checkpoint snapshot"
    );

    let rows = get_rows(&sibling, "SELECT id FROM t ORDER BY id");
    assert_eq!(rows.len(), 2, "both rows must be visible before restart");
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[1][0].as_int().unwrap(), 2);

    drop(checkpoint_sm);
    drop(mvcc_store);
    drop(pager);
    drop(sibling);
    drop(conn);
    db.restart();
    let conn = db.connect();
    let rows = get_rows(&conn, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(
        rows.len(),
        2,
        "commit that landed after the TRUNCATE snapshot must survive reopen"
    );
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].to_string(), "collected");
    assert_eq!(rows[1][0].as_int().unwrap(), 2);
    assert_eq!(rows[1][1].to_string(), "after-snapshot");
    assert_integrity_ok(&conn);
}

/// These cases came from an adversarial DDL/DML matrix. They did not find a
/// failure, but they cover schema-before/schema-after combinations that the
/// frame-level recovery code must keep working: dropped indexes, newly-created
/// indexes, table recreation, and both checkpointed and uncheckpointed base
/// schemas.
#[test]
fn test_schema_frame_recovery_drop_index_with_remaining_index_matrix() {
    for checkpoint_base in [false, true] {
        let mut db = MvccTestDbNoConn::new_with_random_db();
        {
            let conn = db.connect();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)")
                .unwrap();
            conn.execute("CREATE INDEX idx_a ON t(a)").unwrap();
            conn.execute("CREATE INDEX idx_b ON t(b)").unwrap();
            conn.execute("INSERT INTO t VALUES (1, 10, 100)").unwrap();
            conn.execute("INSERT INTO t VALUES (2, 20, 200)").unwrap();
            conn.execute("INSERT INTO t VALUES (3, 30, 300)").unwrap();
            if checkpoint_base {
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
            }
            conn.close().unwrap();
        }
        {
            db.get_mvcc_store().set_checkpoint_threshold(-1);
            let conn = db.connect();
            conn.execute("BEGIN").unwrap();
            conn.execute("DELETE FROM t WHERE id = 1").unwrap();
            conn.execute("DROP INDEX idx_a").unwrap();
            conn.execute("UPDATE t SET b = 250 WHERE id = 2").unwrap();
            conn.execute("INSERT INTO t VALUES (4, 40, 400)").unwrap();
            conn.execute("COMMIT").unwrap();
            conn.close().unwrap();
        }

        db.restart();
        let conn = db.connect();
        let names: Vec<String> = get_rows(
            &conn,
            "SELECT name FROM sqlite_schema WHERE tbl_name = 't' ORDER BY rowid",
        )
        .iter()
        .map(|r| r[0].to_string())
        .collect();
        assert!(names.contains(&"t".to_string()), "table missing: {names:?}");
        assert!(
            !names.contains(&"idx_a".to_string()),
            "dropped idx_a still present after reopen: {names:?}"
        );
        assert!(
            names.contains(&"idx_b".to_string()),
            "remaining idx_b missing after reopen: {names:?}"
        );
        let rows = get_rows(
            &conn,
            "SELECT id, b FROM t INDEXED BY idx_b WHERE b >= 250 ORDER BY b",
        );
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0][0].as_int().unwrap(), 2);
        assert_eq!(rows[0][1].as_int().unwrap(), 250);
        assert_eq!(rows[1][0].as_int().unwrap(), 3);
        assert_eq!(rows[1][1].as_int().unwrap(), 300);
        assert_eq!(rows[2][0].as_int().unwrap(), 4);
        assert_eq!(rows[2][1].as_int().unwrap(), 400);
        let rows = get_rows(&conn, "PRAGMA integrity_check");
        assert_eq!(rows.len(), 1);
        assert_eq!(&rows[0][0].to_string(), "ok");
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        conn.close().unwrap();

        drop(conn);
        db.restart();
        let conn = db.connect();
        let rows = get_rows(
            &conn,
            "SELECT id, b FROM t INDEXED BY idx_b WHERE b >= 250 ORDER BY b",
        );
        assert_eq!(rows.len(), 3);
    }
}

/// Reproducer: DROP TABLE ghost data after restart without explicit checkpoint.
/// Session 1: create + insert + checkpoint. Session 2: drop. Session 3: reopen.
#[test]
fn test_close_persists_drop_table() {
    // Session 1: create table, insert data, checkpoint to DB file
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    conn.execute("CREATE TABLE todrop(id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute("INSERT INTO todrop VALUES (1, 'data')")
        .unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    conn.close().unwrap();
    drop(conn);

    // Session 2: drop table (no explicit checkpoint, rely on close)
    let conn = db.connect();
    conn.execute("DROP TABLE todrop").unwrap();
    conn.close().unwrap();
    drop(conn);

    // Session 3: reopen — table must be gone
    db.restart();
    let conn = db.connect();

    // The table must not exist — CREATE should succeed
    let create_result = conn.execute("CREATE TABLE todrop(id INTEGER PRIMARY KEY, newval TEXT)");
    assert!(
        create_result.is_ok(),
        "CREATE TABLE should succeed after DROP, but got: {:?}",
        create_result.unwrap_err()
    );

    // No ghost data from old table
    let rows = get_rows(&conn, "SELECT * FROM todrop");
    assert!(rows.is_empty(), "New table should be empty, got {rows:?}");

    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

/// Reproducer: DROP INDEX ghost pages after restart without explicit checkpoint.
/// Session 1: create table + index + insert + checkpoint. Session 2: drop index. Session 3: reopen.
#[test]
fn test_close_persists_drop_index() {
    // Session 1: create table/index, insert data, checkpoint to DB file
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    conn.execute("CREATE TABLE tdropidx(id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX idx_tdropidx_val ON tdropidx(val)")
        .unwrap();
    conn.execute("INSERT INTO tdropidx VALUES (1, 'data')")
        .unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    conn.close().unwrap();
    drop(conn);

    // Session 2: drop index (no explicit checkpoint, rely on close)
    let conn = db.connect();
    conn.execute("DROP INDEX idx_tdropidx_val").unwrap();
    conn.close().unwrap();
    drop(conn);

    // Session 3: reopen - index must be gone and integrity check must pass
    db.restart();
    let conn = db.connect();

    let recreate_index = conn.execute("CREATE INDEX idx_tdropidx_val ON tdropidx(val)");
    assert!(
        recreate_index.is_ok(),
        "CREATE INDEX should succeed after DROP INDEX, but got: {:?}",
        recreate_index.unwrap_err()
    );

    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

/// Descending sequence compaction must keep the most-advanced (lowest) value.
///
/// A descending sequence (INCREMENT BY -1, START WITH 100) produces values 100, 99, 98...
/// In MVCC mode, each commit appends a new sqlite_sequence row. On checkpoint, compaction
/// should keep the minimum (most advanced for descending) and delete the rest.
/// After restart, nextval should resume from the most advanced value.
#[test]
fn test_descending_sequence_compaction() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    // Create an autoincrement table to force sqlite_sequence table creation.
    conn.execute("CREATE TABLE dummy(id INTEGER PRIMARY KEY AUTOINCREMENT)")
        .unwrap();
    conn.execute("CREATE SEQUENCE desc_seq START WITH 100 INCREMENT BY -1 MINVALUE 1 MAXVALUE 100")
        .unwrap();

    // Call nextval 5 times across separate transactions.
    // Descending: produces 100, 99, 98, 97, 96
    for _ in 0..5 {
        conn.execute("BEGIN CONCURRENT").unwrap();
        let rows = get_rows(&conn, "SELECT nextval('desc_seq')");
        assert_eq!(rows.len(), 1);
        conn.execute("COMMIT").unwrap();
    }

    // Verify last nextval returned 96
    let rows = get_rows(&conn, "SELECT nextval('desc_seq')");
    let last_val = rows[0][0].as_int().unwrap();
    assert_eq!(last_val, 95, "6th call should return 95");

    // Checkpoint → compaction runs. Should keep the most advanced (lowest) value.
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");

    // After compaction, the backing table should have exactly 1 row with the current value
    let rows = get_rows(&conn, "SELECT value FROM __turso_internal_seq_desc_seq");
    assert_eq!(rows.len(), 1, "compaction should leave exactly 1 row");
    let compacted_val = rows[0][0].as_int().unwrap();
    assert_eq!(
        compacted_val, 95,
        "compaction should keep the most advanced (lowest) value for descending"
    );

    // Close and restart the database
    conn.close().unwrap();
    drop(conn);
    db.restart();
    let conn = db.connect();

    // After restart, nextval should resume from the most advanced value (95)
    let rows = get_rows(&conn, "SELECT nextval('desc_seq')");
    let resumed_val = rows[0][0].as_int().unwrap();
    assert_eq!(
        resumed_val, 94,
        "after restart, descending seq should resume from most advanced value"
    );
}
