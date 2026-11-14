//! Tests of the MVCC store that come from core.
//!
//! They run SQL and then check what the store recorded: the state of a transaction, how far
//! commits and checkpoints have come, and what garbage collection left behind. They read this
//! through read-only methods of `MvStore` such as `tx_snapshot()`, `watermarks()` and
//! `debug_gc_snapshot()`.

use super::checkpoint::{
    assert_integrity_ok, get_rows, wal_path_for_db, FixedFailureInjector, FixedYieldInjector,
};
use super::test_db::MvccTestDbNoConn;
use std::sync::Arc;
use turso_core::alloc::DynAllocator;
use turso_core::io::IO;
use turso_core::mvcc::clock::MvccClock;
use turso_core::mvcc::database::checkpoint::{
    CheckpointState, CheckpointStateMachine, CheckpointYieldPoint,
};
use turso_core::mvcc::database::*;
use turso_core::mvcc::persistent_storage::logical_log::LOG_HDR_SIZE;
use turso_core::mvcc::yield_hooks::YieldPointMarker;
use turso_core::mvcc::yield_points::{YieldInjector, YieldPoint};
use turso_core::namespace::NamespaceId;
use turso_core::state_machine::{StateTransition, TransitionResult};
use turso_core::sync::atomic::{AtomicBool, Ordering};
use turso_core::sync::Mutex;
use turso_core::{
    CheckpointMode, ConnectOpts, Connection, LimboError, SessionRole, StepResult, Value,
};

/// Auto passive checkpoint retries publish when a reader holds the checkpoint lock.
#[test]
fn mvcc_passive_auto_checkpoint_retries_publish_while_reader_pinned() {
    let db = MvccTestDbNoConn::new_with_random_db_passive();
    let mv_store = db.get_mvcc_store();
    mv_store.set_checkpoint_threshold(0);

    let writer = db.connect();
    let reader = db.connect();

    writer
        .execute("CREATE TABLE t(k TEXT PRIMARY KEY, v TEXT)")
        .unwrap();
    writer.execute("PRAGMA wal_checkpoint(PASSIVE)").unwrap();

    reader.execute("BEGIN CONCURRENT").unwrap();

    let durable_before = mv_store.watermarks().durable_txid_max;
    let writer_thread = writer;
    let commit_handle = std::thread::spawn(move || {
        writer_thread.execute("BEGIN CONCURRENT").unwrap();
        writer_thread
            .execute("INSERT INTO t VALUES ('hello', 'hello')")
            .unwrap();
        writer_thread.execute("COMMIT").unwrap();
    });

    for _ in 0..10_000 {
        if mv_store.watermarks().durable_txid_max > durable_before {
            break;
        }
        std::thread::yield_now();
    }
    if mv_store.watermarks().durable_txid_max == durable_before {
        reader.execute("COMMIT").unwrap();
    }
    commit_handle.join().unwrap();

    let durable_after = mv_store.watermarks().durable_txid_max;
    assert!(
        durable_after > durable_before,
        "auto passive checkpoint should publish durable boundary despite pinned reader (before={durable_before}, after={durable_after})",
    );

    if reader.get_tx_state() != turso_core::connection::TransactionState::None {
        reader.execute("COMMIT").unwrap();
    }
    assert_eq!(
        get_rows(&db.connect(), "SELECT v FROM t WHERE k = 'hello'"),
        vec![vec![Value::from_text("hello".to_string())]],
    );
}

/// What this test checks: Startup recovery reconciles WAL/log artifacts into one consistent MVCC state and replay boundary.
/// Why this matters: This path runs automatically after crashes; errors here can duplicate effects or drop durable data.
#[test]
fn test_bootstrap_completes_interrupted_checkpoint_with_committed_wal() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let db_path = db.path().to_string();
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
        let mvcc_store = db.get_mvcc_store();
        advance_checkpoint_until_wal_has_commit_frame(mvcc_store, &conn);

        let pager = conn.get_pager();
        assert!(pager.wal_max_frame().expect("wal must exist") > 0);
        let log_file = db.get_mvcc_store().get_logical_log_file();
        assert!(log_file.size().unwrap() > LOG_HDR_SIZE as u64);
    }

    db.restart();

    let conn = db.connect();
    let rows = get_rows(&conn, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].to_string(), "a");
    assert_eq!(rows[1][0].as_int().unwrap(), 2);
    assert_eq!(rows[1][1].to_string(), "b");

    let log_size = db.get_mvcc_store().get_logical_log_file().size().unwrap();
    assert!(
        log_size >= LOG_HDR_SIZE as u64,
        "logical log must be at least {LOG_HDR_SIZE} bytes after interrupted-checkpoint reconciliation"
    );
    let wal_path = wal_path_for_db(&db_path);
    let wal_len = wal_path.metadata().map(|m| m.len()).unwrap_or(0);
    assert_eq!(wal_len, 0);
}

/// What this test checks: Checkpoint pager commit writes data pages and metadata row in the same WAL transaction.
/// Why this matters: This is the atomicity mechanism replacing logical-log header checkpoint timestamps.
#[test]
fn test_meta_checkpoint_case_10_metadata_upsert_is_atomic_with_pager_commit() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        let mvcc_store = db.get_mvcc_store();
        let committed_ts = mvcc_store.watermarks().last_committed_tx_ts;
        assert!(committed_ts > 0);

        let pager = conn.get_pager();
        let mut checkpoint_sm = CheckpointStateMachine::new(
            pager.clone(),
            mvcc_store,
            conn.clone(),
            true,
            conn.get_sync_mode(),
            turso_core::MAIN_DB_ID,
            CheckpointMode::Truncate {
                upper_bound_inclusive: None,
            },
        );

        for _ in 0..50_000 {
            if checkpoint_sm.state() == CheckpointState::CheckpointWal {
                break;
            }
            match checkpoint_sm.step(&()).unwrap() {
                TransitionResult::Io(io) => io.wait(pager.io.as_ref()).unwrap(),
                TransitionResult::Continue => {}
                TransitionResult::Done(_) => {
                    panic!("checkpoint finished before expected stop window")
                }
            }
        }
    }

    db.restart();
    let conn = db.connect();
    let rows = get_rows(&conn, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].to_string(), "a");

    let meta = get_rows(
        &conn,
        "SELECT v FROM __turso_internal_mvcc_meta WHERE k = 'persistent_tx_ts_max'",
    );
    assert_eq!(meta.len(), 1);
    assert!(
        meta[0][0].as_int().unwrap() >= 1,
        "expected metadata boundary to persist with pager commit"
    );
}

/// What this test checks: Auto-checkpoint post-commit failure does not invalidate committed transaction visibility on restart.
/// Why this matters: Commit contract must remain stable even when checkpoint cleanup fails mid-flight.
#[test]
fn test_meta_checkpoint_case_11_auto_checkpoint_failure_after_commit_remains_recoverable() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();

    let mvcc_store = db.get_mvcc_store();
    let ts1 = mvcc_store.watermarks().last_committed_tx_ts;
    assert!(ts1 > 0, "expected committed timestamp for first insert");

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
    let mut reached_truncate = false;
    for _ in 0..50_000 {
        if checkpoint_sm.state() == CheckpointState::TruncateLogicalLog {
            reached_truncate = true;
            break; // Simulate checkpoint aborting before log truncation
        }
        match checkpoint_sm.step(&()).unwrap() {
            TransitionResult::Io(io) => io.wait(pager.io.as_ref()).unwrap(),
            TransitionResult::Continue => {}
            TransitionResult::Done(_) => {
                panic!("checkpoint finished before reaching truncate state")
            }
        }
    }
    assert!(
        reached_truncate,
        "expected to reach TruncateLogicalLog state"
    );

    // Pager commit already succeeded before log truncation.
    // Same-process retries must advance from this durable boundary.
    let durable_boundary = mvcc_store.watermarks().durable_txid_max;
    assert!(
        durable_boundary >= ts1,
        "expected in-memory durable checkpoint boundary to advance after pager commit: boundary={durable_boundary} ts1={ts1}"
    );

    let sync_mode = conn.get_sync_mode();
    let checkpoint_sm2 = CheckpointStateMachine::new(
        pager,
        mvcc_store,
        conn,
        true,
        sync_mode,
        turso_core::MAIN_DB_ID,
        CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        },
    );
    let (old_boundary, _) = checkpoint_sm2.bounds();
    assert!(
        old_boundary.unwrap_or_default() >= ts1,
        "expected retry checkpoint to start from durable boundary: old={old_boundary:?} ts1={ts1}"
    );
}

/// What this test checks: a checkpoint state machine created before another checkpoint
/// advances the durable boundary must resample that boundary after taking the checkpoint lock.
/// Why this matters: otherwise a delayed checkpoint can replay an already-durable unique-index
/// delete and fail. This test uses raw APIs, check test_checkpoint_resamples_boundary_before_starting_with_yield_injection
/// which uses only user facing APIs to simulate the same error.
#[test]
fn test_checkpoint_resamples_boundary_before_starting() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    conn.execute("PRAGMA mvcc_checkpoint_threshold = -1")
        .unwrap();
    conn.execute(
        "CREATE TABLE dry_floor_846 (
            sour_sand_972 BLOB UNIQUE,
            sour_river_140 REAL,
            sweet_wall_518 BLOB,
            fast_grass_379 TEXT,
            dark_wave_139 REAL UNIQUE,
            sad_wind_216 INTEGER UNIQUE PRIMARY KEY
        )",
    )
    .unwrap();

    conn.execute(
        "INSERT INTO dry_floor_846 (
            sour_sand_972, sour_river_140, sweet_wall_518,
            fast_grass_379, dark_wave_139, sad_wind_216
        ) VALUES (
            zeroblob(16), 6.85, x'736d6172745f6c6561665f353637',
            'wild_hill_714', 8.43, 788
        )",
    )
    .unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let mvcc_store = db.get_mvcc_store();
    let first_boundary = mvcc_store.watermarks().durable_txid_max;
    assert!(first_boundary > 0);

    conn.execute(
        "UPDATE dry_floor_846
            SET sour_sand_972 = x'66756c6c5f737461725f333732',
                sour_river_140 = 5.75,
                sweet_wall_518 = zeroblob(32),
                fast_grass_379 = 'old_moon_16',
                dark_wave_139 = 2.90
          WHERE sad_wind_216 = 788",
    )
    .unwrap();
    let update_ts = mvcc_store.watermarks().last_committed_tx_ts;
    assert!(update_ts > first_boundary);

    let delayed_conn = db.connect();
    let delayed_pager = delayed_conn.get_pager();
    let mut delayed_checkpoint = CheckpointStateMachine::new(
        delayed_pager.clone(),
        mvcc_store.clone(),
        delayed_conn.clone(),
        true,
        delayed_conn.get_sync_mode(),
        turso_core::MAIN_DB_ID,
        CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        },
    );
    let (old_boundary, _) = delayed_checkpoint.bounds();
    assert_eq!(old_boundary, Some(first_boundary));

    let interrupted_conn = db.connect();
    let interrupted_pager = interrupted_conn.get_pager();
    let mut interrupted_checkpoint = CheckpointStateMachine::new(
        interrupted_pager.clone(),
        mvcc_store.clone(),
        interrupted_conn.clone(),
        true,
        interrupted_conn.get_sync_mode(),
        turso_core::MAIN_DB_ID,
        CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        },
    );
    let mut reached_wal_checkpoint = false;
    for _ in 0..50_000 {
        if interrupted_checkpoint.state() == CheckpointState::CheckpointWal {
            reached_wal_checkpoint = true;
            break;
        }
        match interrupted_checkpoint.step(&()).unwrap() {
            TransitionResult::Io(io) => io.wait(interrupted_pager.io.as_ref()).unwrap(),
            TransitionResult::Continue => {}
            TransitionResult::Done(_) => {
                panic!("checkpoint finished before reaching WAL checkpoint")
            }
        }
    }
    assert!(
        reached_wal_checkpoint,
        "expected checkpoint to reach WAL checkpoint"
    );
    assert_eq!(mvcc_store.watermarks().durable_txid_max, update_ts);
    interrupted_checkpoint
        .cleanup_after_external_io_error(LimboError::Interrupt)
        .unwrap();

    let mut finished = false;
    for _ in 0..50_000 {
        match delayed_checkpoint.step(&()).unwrap() {
            TransitionResult::Io(io) => io.wait(delayed_pager.io.as_ref()).unwrap(),
            TransitionResult::Continue => {}
            TransitionResult::Done(_) => {
                finished = true;
                break;
            }
        }
    }
    assert!(finished, "delayed checkpoint did not finish");

    let rows = get_rows(
        &conn,
        "SELECT sad_wind_216, dark_wave_139, hex(sour_sand_972)
           FROM dry_floor_846
          WHERE sad_wind_216 = 788",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 788);
    assert_eq!(rows[0][1].to_string(), "2.9");
    assert_eq!(&rows[0][2].to_string(), "66756C6C5F737461725F333732");

    let integrity = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(integrity.len(), 1);
    assert_eq!(&integrity[0][0].to_string(), "ok");
}

#[test]
fn test_read_only_commit_does_not_cache_finalized_state() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    let mvcc_store = db.get_mvcc_store();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();

    // Establish a clean baseline after schema/setup writes.
    mvcc_store.drop_unused_row_versions();
    let baseline = mvcc_store.debug_gc_snapshot().finalized_tx_states;

    conn.execute("BEGIN CONCURRENT").unwrap();
    let _ = get_rows(&conn, "SELECT 1");
    conn.execute("COMMIT").unwrap();

    assert_eq!(
        mvcc_store.debug_gc_snapshot().finalized_tx_states,
        baseline,
        "read-only commit should not add finalized tx cache entries"
    );
}

#[test]
fn test_drop_unused_row_versions_prunes_unreferenced_finalized_tx_states() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    let mvcc_store = db.get_mvcc_store();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();

    // Establish a clean baseline after schema/setup writes.
    mvcc_store.drop_unused_row_versions();
    let baseline = mvcc_store.debug_gc_snapshot().finalized_tx_states;

    conn.execute("INSERT INTO t VALUES (1, 1)").unwrap();
    let after_write = mvcc_store.debug_gc_snapshot().finalized_tx_states;
    assert!(
        after_write > baseline,
        "write commit should add at least one finalized tx cache entry"
    );

    mvcc_store.drop_unused_row_versions();

    assert_eq!(
        mvcc_store.debug_gc_snapshot().finalized_tx_states,
        baseline,
        "GC scan should prune finalized tx cache entries with no remaining TxID references"
    );
}

/// What this test checks: the committed timestamp cache is a monotonic watermark even when independent commits finish out of timestamp order.
/// Why this matters: checkpoints use this cache as a durable replay boundary; lowering it can make DB pages advance past MVCC metadata.
#[test]
fn test_last_committed_timestamp_is_monotonic_for_out_of_order_commits() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let setup = db.connect();
    setup
        .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    setup.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    setup.execute("INSERT INTO t VALUES (2, 'b')").unwrap();

    let mvcc_store = db.get_mvcc_store();
    let conn_a = db.connect();
    let conn_b = db.connect();

    conn_a.execute("BEGIN CONCURRENT").unwrap();
    conn_a
        .execute("UPDATE t SET v = 'a1' WHERE id = 1")
        .unwrap();
    let tx_a_id = conn_a
        .get_mv_tx()
        .map(|(tx_id, _)| tx_id)
        .expect("tx_a should be active");
    conn_a.set_yield_injector(Some(FixedYieldInjector::new([
        CommitYieldPoint::LogRecordPrepared.point(),
    ])));

    let mut commit_a = conn_a.prepare("COMMIT").unwrap();
    assert!(
        matches!(commit_a.step().unwrap(), StepResult::Yield),
        "tx_a should yield after getting its commit timestamp"
    );
    let tx_a_end_ts = match mvcc_store
        .tx_snapshot(tx_a_id)
        .expect("tx_a should still be tracked")
        .state
    {
        TransactionState::Preparing(ts) => ts,
        state => panic!("expected tx_a to be Preparing, got {state:?}"),
    };

    conn_b.execute("BEGIN CONCURRENT").unwrap();
    conn_b
        .execute("UPDATE t SET v = 'b1' WHERE id = 2")
        .unwrap();
    conn_b.execute("COMMIT").unwrap();
    let tx_b_committed = mvcc_store.watermarks().last_committed_tx_ts;
    assert!(
        tx_b_committed > tx_a_end_ts,
        "tx_b should commit at a newer timestamp than the yielded tx_a"
    );

    commit_a.run_ignore_rows().unwrap();
    let final_watermark = mvcc_store.watermarks().last_committed_tx_ts;
    assert_eq!(
        final_watermark, tx_b_committed,
        "finishing an older commit must not lower the committed timestamp watermark"
    );
}

/// A transaction that begins after a DROP INDEX has taken its commit timestamp, but before
/// the new schema cookie is published, would see the commit's row changes with the old schema.
/// Here the old schema still has the index while the snapshot has no rows left in the table.
#[test]
fn test_integrity_check_in_middle_commit_of_drop_index_and_delete() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn_a = db.connect();
    let conn_b = db.connect();
    conn_a
        .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c INTEGER)")
        .unwrap();
    conn_a.execute("CREATE INDEX i ON t(c)").unwrap();
    conn_a
        .execute("INSERT INTO t VALUES (1, 10), (2, 20)")
        .unwrap();
    conn_a.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let mut integrity_check = conn_b.prepare("PRAGMA integrity_check").unwrap();

    conn_a.execute("BEGIN").unwrap();
    conn_a.execute("DROP INDEX i").unwrap();
    conn_a.execute("DELETE FROM t").unwrap();
    let store = db.get_mvcc_store();
    let tx_a = conn_a.get_mv_tx().map(|(tx_id, _)| tx_id).unwrap();
    let prepared = FixedYieldInjector::new([CommitYieldPoint::LogRecordPrepared.point()]);
    conn_a.set_yield_injector(Some(prepared.clone()));
    let mut commit = conn_a.prepare("COMMIT").unwrap();
    while !prepared.is_empty() {
        assert!(matches!(
            commit.step().unwrap(),
            StepResult::Yield | StepResult::IO
        ));
    }
    conn_a.set_yield_injector(None);
    assert!(matches!(
        store.tx_snapshot(tx_a).unwrap().state,
        TransactionState::Preparing(_)
    ));

    let first_row = loop {
        match integrity_check.step().unwrap() {
            StepResult::IO | StepResult::Yield => {}
            StepResult::Busy => break None,
            StepResult::Row => break Some(integrity_check.row().unwrap().get_value(0).clone()),
            other => panic!("unexpected step result {other:?}"),
        }
    };
    assert_eq!(
        first_row, None,
        "integrity_check began during the DROP INDEX commit"
    );

    commit.run_ignore_rows().unwrap();
    drop(commit);

    integrity_check.reset().unwrap();
    assert_eq!(
        integrity_check.run_collect_rows().unwrap(),
        vec![vec![Value::build_text("ok")]]
    );
}

/// What this test checks: Checkpoint transitions preserve DB/WAL/log ordering and watermark updates for the tested edge case.
/// Why this matters: Incorrect ordering breaks crash safety, replay boundaries, or durability guarantees.
#[test]
fn test_should_checkpoint() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let mv_store = db.get_mvcc_store();
    assert!(!mv_store.should_checkpoint());
    mv_store.set_checkpoint_threshold(0);
    assert!(mv_store.should_checkpoint());
}

/// What this test checks: After restart recovery, checkpoint-threshold checks use the recovered log offset.
/// Why this matters: Shadow-offset drift can suppress auto-checkpoint despite a large recovered log tail.
#[test]
fn test_should_checkpoint_after_recovery_uses_recovered_offset() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(x)").unwrap();
        conn.execute("INSERT INTO t VALUES (1)").unwrap();
    }

    db.restart();
    let _conn = db.connect();
    let mv_store = db.get_mvcc_store();

    // We used to assert on the concrete logical-log offset here, but MVCC durable storage
    // is now abstracted behind a trait object (to allow injecting custom implementations).
    // Validate behavior instead: after recovery, the recovered offset should be reflected
    // in should_checkpoint() when the threshold is set very low.
    mv_store.set_checkpoint_threshold(1);
    assert!(
        mv_store.should_checkpoint(),
        "expected should_checkpoint() to reflect the recovered logical-log offset"
    );
}

/// What this test checks: Core MVCC read/write semantics hold for this operation sequence.
/// Why this matters: These are foundational invariants; regressions here invalidate higher-level SQL behavior.
#[test]
fn test_mvcc_read_tx_lifecycle() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("CREATE TABLE t(x)").unwrap();
    conn.execute("BEGIN").unwrap();
    conn.execute("SELECT * FROM t").unwrap();

    let pager = conn.get_pager();
    assert!(pager.holds_read_lock());

    conn.execute("COMMIT").unwrap();
    assert!(!pager.holds_read_lock());
}

/// What this test checks: Core MVCC read/write semantics hold for this operation sequence.
/// Why this matters: These are foundational invariants; regressions here invalidate higher-level SQL behavior.
#[test]
fn test_mvcc_conn_drop_releases_read_tx() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("CREATE TABLE t(x)").unwrap();

    let pager = conn.get_pager();
    pager.begin_read_tx().unwrap();
    assert!(pager.holds_read_lock());

    drop(conn);
    assert!(!pager.holds_read_lock());
}

/// Regression guard for the `mv_store.txs` ↔ `connection.mv_tx_id` divergence
/// originally observed in production as `Transaction <id> not found while
/// releasing savepoint` (panic) and `NoSuchTransactionID(<id>)` (read-path
/// error) — see Antithesis Limbo run, 2026-04-27.
///
/// **Bug shape (pre-fix):** `CommitStateMachine` called `mvcc_store.remove_tx(tx_id)`
/// directly. The connection-cache clear (`conn.set_mv_tx(None)`) lived at the
/// caller (vdbe/mod.rs:1898) and only ran on the success path. If anything
/// between `remove_tx` and that caller-side clear failed or yielded I/O and
/// then the runtime abandoned the task before re-entering, the cache was
/// stranded pointing at a tx that was already gone from `txs`. The natural
/// trigger in production was an IO yield from `CheckpointStateMachine::step`
/// (called after `remove_tx` at the EndCommitLogicalLog site) followed by
/// task abandonment under network partition.
///
/// **Fix:** `MvStore::finish_committed_tx(tx_id, conn, db_id)` clears the
/// connection's mv_tx cache and removes the tx from `txs` together,
/// atomically. All three commit sites that previously called `remove_tx`
/// directly now call `finish_committed_tx`. After this, no in-flight state
/// (Err propagation, IO yield + abandon, success) can produce the divergent
/// `(cache=Some, txs=None)` pair — they're mutated as a single act.
///
/// **What this test exercises:** we inject a `TxError` at the historical
/// post-`remove_tx` boundary (`CommitYieldPoint::AfterRemoveTx`). The abort
/// handler at vdbe/mod.rs:2204 explicitly skips rollback for `TxError`, so
/// pre-fix nothing else would have cleared the cache — the divergence would
/// surface. Post-fix, `finish_committed_tx` already cleared the cache before
/// the injection point fires, so both stores are gone in lock-step and a
/// follow-up read on the connection sees a clean state.
#[test]
fn test_commit_failure_after_remove_tx_does_not_strand_conn_cache() {
    let db = MvccTestDbNoConn::new();
    let conn = db.connect();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("BEGIN CONCURRENT").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'new')").unwrap();

    let tx_id = conn
        .get_mv_tx()
        .map(|(tx_id, _)| tx_id)
        .expect("tx should be open after BEGIN");
    let mv_store = db.get_mvcc_store();
    assert!(
        mv_store.tx_snapshot(tx_id).is_some(),
        "precondition: tx must be live in txs before COMMIT"
    );

    conn.set_failure_injector(Some(FixedFailureInjector::new([(
        CommitYieldPoint::AfterRemoveTx.point(),
        // `TxError` is in the no-rollback list at vdbe/mod.rs:2204, so the abort
        // handler will not rescue stranded state on its own — the only thing
        // keeping the connection coherent here is `finish_committed_tx`.
        LimboError::TxError("synthetic post-remove_tx failure".to_string()),
    )])));

    let commit_err = conn
        .execute("COMMIT")
        .expect_err("commit must fail at the injected boundary");
    tracing::info!("injected commit failure: {commit_err}");

    // The pairing invariant: `finish_committed_tx` clears both atomically, so
    // after the injected Err we see them gone together — no half-state.
    assert!(
        mv_store.tx_snapshot(tx_id).is_none(),
        "fix: tx must be gone from txs (finish_committed_tx ran before the \
         injection point)"
    );
    assert_eq!(
        conn.get_mv_tx().map(|(tx_id, _)| tx_id),
        None,
        "fix: connection mv_tx cache must be cleared in lock-step with the \
         txs removal — pre-fix this stranded the cache"
    );

    // NOTE: we deliberately do not assert anything about the *visibility* of
    // the failed-commit's INSERT here. By the time the injection fires at
    // `AfterRemoveTx`, the commit pipeline has already published
    // `tx.state = Committed(end_ts)` and timestamp-rewritten live versions
    // (mod.rs:1901-1903) — so the row IS visible to subsequent readers. That's
    // a separate "Err on COMMIT but data is durable" semantic concern that
    // predates this fix and applies equally to the production network-partition
    // scenario; it's not what this regression test is guarding. This test
    // verifies only the pairing invariant: `mv_store.txs` and
    // `conn.mv_tx_id` mutate in lock-step, leaving the connection reusable.

    conn.close().unwrap();
}

/// Regression for #6757: a Statement driving a CONCURRENT `COMMIT` that
/// yields at `LogRecordPrepared` and is then dropped used to leave the tx
/// in `Preparing`. The next statement on that connection would trip a
/// `turso_assert_eq!(Active)` in `read_from_table_or_index` (process
/// panic). The abort-side `cleanup_abandoned_mvcc_commit` hook now rolls
/// back the orphan tx so a follow-up INSERT works against a fresh tx.
#[test]
fn dropped_concurrent_commit_does_not_strand_connection() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let setup = db.connect();
    setup
        .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    setup.close().unwrap();

    let conn = db.connect();
    conn.execute("BEGIN CONCURRENT").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();

    let mv_store = db.get_mvcc_store();
    let tx_id = conn
        .get_mv_tx()
        .map(|(tx_id, _)| tx_id)
        .expect("tx must be open after INSERT inside BEGIN CONCURRENT");

    conn.set_yield_injector(Some(FixedYieldInjector::new([
        CommitYieldPoint::LogRecordPrepared.point(),
    ])));
    {
        let mut commit = conn.prepare("COMMIT").unwrap();
        match commit.step().unwrap() {
            turso_core::StepResult::Yield => {}
            other => panic!("expected yield at LogRecordPrepared; got {other:?}"),
        }
    }
    conn.set_yield_injector(None);

    // Abort hook ran cleanup_abandoned_mvcc_commit → rollback_tx → tx is
    // gone from `txs`, connection's mv_tx slot is cleared, AND
    // transaction_state is reset to None so the next op_transaction takes
    // the fresh-tx path instead of inheriting stale Write state.
    assert!(
        mv_store.tx_snapshot(tx_id).is_none(),
        "orphan tx must be rolled back by abort-side cleanup"
    );
    assert!(
        conn.get_mv_tx().map(|(tx_id, _)| tx_id).is_none(),
        "connection's mv_tx slot must be cleared"
    );
    assert_eq!(
        conn.get_tx_state(),
        turso_core::connection::TransactionState::None,
        "transaction_state must be reset after abort-side rollback"
    );

    // The next op must not panic on the would-be-Preparing tx — it should
    // start a fresh autocommit tx instead.
    conn.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
    let rows = get_rows(&conn, "SELECT id FROM t ORDER BY id");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 2);
}

#[test]
fn abandoned_committed_writer_notifies_dependents() {
    for group_commit in [false, true] {
        for (reset, n_rows) in [
            (false, 1),
            (true, 1),
            (false, MVCC_COMMIT_BATCH_SIZE + 1),
            (true, MVCC_COMMIT_BATCH_SIZE + 1),
        ] {
            let db = MvccTestDbNoConn::new();
            let writer = db.connect();
            let reader = db.connect();
            writer
                .execute(format!(
                    "PRAGMA mvcc_group_commit = {}",
                    if group_commit { "on" } else { "off" }
                ))
                .unwrap();
            writer
                .execute("CREATE TABLE t(id INTEGER PRIMARY KEY)")
                .unwrap();
            writer.execute("BEGIN CONCURRENT").unwrap();
            let values = (1..=n_rows)
                .map(|id| format!("({id})"))
                .collect::<Vec<_>>()
                .join(",");
            writer
                .execute(format!("INSERT INTO t VALUES {values}"))
                .unwrap();
            let store = db.get_db().get_mv_store().clone().unwrap();
            let writer_id = writer.get_mv_tx().map(|(tx_id, _)| tx_id).unwrap();
            let prepared = FixedYieldInjector::new([CommitYieldPoint::LogRecordPrepared.point()]);
            writer.set_yield_injector(Some(prepared.clone()));
            let mut commit = writer.prepare("COMMIT").unwrap();
            for _ in 0..100 {
                assert!(matches!(
                    commit.step().unwrap(),
                    StepResult::Yield | StepResult::IO
                ));
                if prepared.is_empty() {
                    break;
                }
            }
            assert!(prepared.is_empty());
            writer.set_yield_injector(None);
            assert!(matches!(
                store.tx_snapshot(writer_id).unwrap().state,
                TransactionState::Preparing(_)
            ));

            reader.execute("BEGIN CONCURRENT").unwrap();
            assert_eq!(
                get_rows(&reader, "SELECT count(*) FROM t")[0][0].as_int(),
                Some(n_rows as i64)
            );
            let reader_id = reader.get_mv_tx().map(|(tx_id, _)| tx_id).unwrap();
            assert_eq!(store.tx_snapshot(reader_id).unwrap().commit_dep_count, 1);
            let mut reader_commit = reader.prepare("COMMIT").unwrap();
            assert!(matches!(reader_commit.step().unwrap(), StepResult::Yield));

            if n_rows == 1 {
                writer.set_yield_injector(Some(FixedYieldInjector::new([
                    CommitYieldPoint::LogRecordMarkedWritten.point(),
                ])));
            }
            for _ in 0..100 {
                assert!(matches!(
                    commit.step().unwrap(),
                    StepResult::Yield | StepResult::IO
                ));
                let writer_tx = store.tx_snapshot(writer_id).unwrap();
                if n_rows == 1 && writer_tx.log_appended
                    || matches!(writer_tx.state, TransactionState::Committed(_))
                {
                    break;
                }
            }
            let writer_tx = store.tx_snapshot(writer_id).unwrap();
            assert!(writer_tx.log_appended);
            if n_rows == 1 {
                writer.set_yield_injector(None);
                assert!(matches!(writer_tx.state, TransactionState::Preparing(_)));
            } else {
                assert!(matches!(writer_tx.state, TransactionState::Committed(_)));
            }
            assert_eq!(store.tx_snapshot(reader_id).unwrap().commit_dep_count, 1);
            if reset {
                commit.reset().unwrap();
            }
            drop(commit);

            let reader_tx = store.tx_snapshot(reader_id).unwrap();
            assert_eq!(reader_tx.commit_dep_count, 0);
            assert!(!reader_tx.abort_now);
            assert!(store.tx_snapshot(writer_id).is_none());
            assert!(writer.get_mv_tx().map(|(tx_id, _)| tx_id).is_none());
            assert!(matches!(reader_commit.step().unwrap(), StepResult::Done));
            drop(reader_commit);
            assert!(store.tx_snapshot(reader_id).is_none());
            assert!(reader.get_mv_tx().map(|(tx_id, _)| tx_id).is_none());
            reader.execute("INSERT INTO t VALUES (0)").unwrap();
            reader.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
            let rows = get_rows(&writer, "SELECT count(*), min(id), max(id) FROM t");
            assert_eq!(rows[0][0].as_int(), Some(n_rows as i64 + 1));
            assert_eq!(rows[0][1].as_int(), Some(0));
            assert_eq!(rows[0][2].as_int(), Some(n_rows as i64));
            assert_eq!(store.debug_gc_snapshot().active_txs, 0);
        }
    }
}

/// Regression: out-of-order MVCC commit finalization must not let an older
/// transaction replace `global_header` with a stale header.
///
/// tx_a is paused in FinalizeCommit after it has been marked Committed but
/// before publishing its header/watermark. tx_b then commits newer DDL and
/// publishes a bumped schema cookie. When tx_a resumes, its older header must
/// not move `global_header.schema_cookie` backward.
#[test]
fn test_global_header_cookie_no_regression_on_out_of_order_finalize() {
    let db = MvccTestDbNoConn::new_with_random_db();
    {
        let setup = db.connect();
        setup
            .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        setup
            .execute("INSERT INTO t VALUES (1, 'initial')")
            .unwrap();
        setup.close().unwrap();
    }
    let mvcc_store = db.get_mvcc_store();
    let cookie_before = mvcc_store
        .with_header(|h| h.schema_cookie.get(), None)
        .unwrap();

    let conn_a = db.connect();
    let conn_b = db.connect();

    // tx_a: CONCURRENT update. Pin its commit inside FinalizeCommit, after
    // CommitEnd has already marked the tx Committed but before the
    // watermark / global_header writes. tx_b begins after tx_a's commit
    // timestamp, so `acquire_exclusive_tx` lets tx_b take the slot.
    conn_a.execute("BEGIN CONCURRENT").unwrap();
    conn_a
        .execute("UPDATE t SET v = 'a-mod' WHERE id = 1")
        .unwrap();
    let tx_a_id = conn_a
        .get_mv_tx()
        .map(|(tx_id, _)| tx_id)
        .expect("tx_a should be active");

    conn_a.set_yield_injector(Some(FixedYieldInjector::new([
        CommitYieldPoint::BeforeGlobalHeaderUpdate.point(),
    ])));
    let mut commit_a = conn_a.prepare("COMMIT").unwrap();
    let mut yielded = false;
    for _ in 0..200 {
        match commit_a.step().unwrap() {
            StepResult::Yield => {
                yielded = true;
                break;
            }
            StepResult::Done => break,
            _ => {}
        }
    }
    assert!(
        yielded,
        "tx_a's COMMIT should yield before publishing global_header"
    );
    assert!(
        matches!(
            mvcc_store
                .tx_snapshot(tx_a_id)
                .expect("tx_a should be tracked")
                .state,
            TransactionState::Committed(_)
        ),
        "tx_a should be Committed (set by CommitEnd) by the time we yield in FinalizeCommit"
    );

    // tx_b: exclusive DDL. tx_a committed before tx_b began, so
    // `acquire_exclusive_tx` succeeds. tx_b runs end-to-end and its
    // FinalizeCommit writes the bumped cookie into global_header.
    conn_b.execute("BEGIN").unwrap();
    conn_b.execute("CREATE TABLE foo(x INTEGER)").unwrap();
    conn_b.execute("COMMIT").unwrap();
    let cookie_after_b = mvcc_store
        .with_header(|h| h.schema_cookie.get(), None)
        .unwrap();
    assert!(
        cookie_after_b > cookie_before,
        "tx_b's CREATE TABLE should bump global_header.schema_cookie \
         (before={cookie_before} after_b={cookie_after_b})"
    );

    // Resume tx_a. Its FinalizeCommit's global_header write must not
    // overwrite tx_b's newer cookie.
    conn_a.set_yield_injector(None);
    commit_a.run_ignore_rows().unwrap();
    drop(commit_a);

    let cookie_final = mvcc_store
        .with_header(|h| h.schema_cookie.get(), None)
        .unwrap();
    assert_eq!(
        cookie_final, cookie_after_b,
        "global_header.schema_cookie regressed after older tx_a finalized \
         after newer DDL tx_b — before={cookie_before} after_b={cookie_after_b} \
         final={cookie_final}"
    );
}

#[test]
fn test_create_index_exclusive_acquire_rechecks_timestamp_after_cas() {
    let db = MvccTestDbNoConn::new_with_random_db();

    let setup = db.connect();
    setup
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    setup.execute("INSERT INTO t VALUES (1, 100)").unwrap();
    setup.close().unwrap();

    let writer = db.connect();
    writer.execute("BEGIN CONCURRENT").unwrap();
    writer.execute("INSERT INTO t VALUES (2, 200)").unwrap();

    let ddl = db.connect();
    ddl.execute("BEGIN DEFERRED").unwrap();
    let mut read = ddl.prepare("SELECT COUNT(*) FROM t").unwrap();
    read.run_ignore_rows().unwrap();
    drop(read);

    let mvcc_store = db.get_mvcc_store();
    let ddl_tx_id = ddl
        .get_mv_tx()
        .map(|(tx_id, _)| tx_id)
        .expect("DDL connection should have an active MVCC transaction");
    let ddl_begin_ts = mvcc_store
        .tx_snapshot(ddl_tx_id)
        .expect("DDL transaction should be tracked")
        .begin_ts;
    assert!(
        ddl_begin_ts >= mvcc_store.watermarks().last_committed_tx_ts,
        "pre-CAS timestamp check should pass before the injected writer commit"
    );

    let injector = CommitWriterOnExclusiveAcquireInjector::new(
        ExclusiveTxYieldPoint::AfterTimestampCheckBeforeCas.point(),
        ddl_tx_id,
        writer,
    );
    ddl.set_yield_injector(Some(injector.clone()));

    let result = ddl.execute("CREATE INDEX idx_v ON t(v)");
    assert!(
        injector.fired(),
        "writer should commit in the exclusive-acquire CAS window"
    );
    assert!(
        matches!(result, Err(turso_core::LimboError::Busy)),
        "stale DDL transaction should release exclusive and return Busy after post-CAS recheck: {result:?}"
    );
    assert!(
        !mvcc_store.is_exclusive_tx(&ddl_tx_id),
        "failed stale DDL should not keep the exclusive slot"
    );
    ddl.set_yield_injector(None);
    if ddl.get_mv_tx().map(|(tx_id, _)| tx_id).is_some() {
        ddl.execute("ROLLBACK").unwrap();
    }

    let observer = db.connect();
    observer.execute("CREATE INDEX idx_v ON t(v)").unwrap();
    observer.execute("DELETE FROM t WHERE id = 2").unwrap();

    let integrity = get_rows(&observer, "PRAGMA integrity_check");
    assert_eq!(integrity.len(), 1);
    assert_eq!(integrity[0][0].to_string(), "ok");
}

/// Concurrent DROP of a checkpointed table during a parked passive checkpoint must not panic.
#[test]
fn test_passive_checkpoint_truncate_wal_tolerates_concurrent_drop_of_checkpointed_table() {
    use turso_core::StepResult;
    let _ = tracing_subscriber::fmt::try_init();
    let db = MvccTestDbNoConn::new_with_random_db_passive();
    let conn_keep = db.connect();
    conn_keep
        .execute("CREATE TABLE keep(x INTEGER PRIMARY KEY)")
        .unwrap();
    conn_keep.execute("INSERT INTO keep VALUES (1)").unwrap();
    conn_keep
        .execute("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
    let keep_root = get_rows(
        &conn_keep,
        "SELECT rootpage FROM sqlite_schema WHERE type = 'table' AND name = 'keep'",
    )[0][0]
        .as_int()
        .unwrap();
    assert!(
        keep_root > 0,
        "keep must be checkpointed (positive root) for the DROP to record it, got {keep_root}"
    );
    let conn_c = db.connect();
    conn_c
        .execute("CREATE TABLE other(y INTEGER PRIMARY KEY)")
        .unwrap();
    conn_c
        .execute("PRAGMA mvcc_checkpoint_threshold = 0")
        .unwrap();
    let injector =
        FixedYieldInjector::new([CheckpointYieldPoint::AfterDurableBoundaryAdvanced.point()]);
    conn_c.set_yield_injector(Some(injector.clone()));
    let mut checkpoint = conn_c.prepare("INSERT INTO other VALUES (1)").unwrap();
    let pager_io = conn_c.get_pager().io.clone();
    let step_to_next_yield = |checkpoint: &mut turso_core::Statement, expect_remaining: usize| {
        for _ in 0..200_000 {
            match checkpoint.step().unwrap() {
                StepResult::IO | StepResult::Yield => {
                    if injector.remaining_len() == expect_remaining {
                        return true;
                    }
                    pager_io.step().unwrap();
                }
                StepResult::Done => return false,
                other => panic!("unexpected checkpoint step: {other:?}"),
            }
        }
        false
    };
    assert!(
        step_to_next_yield(&mut checkpoint, 0),
        "passive checkpoint must yield at AfterDurableBoundaryAdvanced (publish window done)"
    );
    assert!(
        conn_c.db.clone_schema().dropped_root_pages.is_empty(),
        "parked checkpoint should have published its own pages; live set must be clean"
    );
    let conn_d = db.connect();
    conn_d.execute("DROP TABLE keep").unwrap();
    assert!(
        conn_c
            .db
            .clone_schema()
            .dropped_root_pages
            .contains(&keep_root),
        "concurrent DROP must record keep's root in the live shared dropped_root_pages"
    );
    for _ in 0..200_000 {
        match checkpoint.step().unwrap() {
            StepResult::Done => break,
            StepResult::IO | StepResult::Yield => pager_io.step().unwrap(),
            other => panic!("unexpected checkpoint step after resume: {other:?}"),
        }
    }
    assert_integrity_ok(&conn_c);
}

/// Regression test for a host that schedules every connection cooperatively
/// on one thread (turso-server). `Database::connect` runs the connect-time
/// ANALYZE stats refresh through a blocking loop that pumps `io.step()`. When
/// that refresh's `sqlite_stat1` scan speculatively read a version written by
/// a transaction still in `Preparing`, its own end-of-statement commit waited
/// for that writer with an explicit yield, and `io.step()` can never let the
/// writer run, so `connect` spun forever. `connect_async` must hand that wait
/// to the caller instead and finish once the writer commits.
#[test]
fn connect_async_yields_instead_of_spinning_on_preparing_commit_dependency() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let database = db.get_db();
    let writer = db.connect();
    writer
        .execute("CREATE TABLE t1 (x INTEGER PRIMARY KEY, y)")
        .unwrap();
    writer
        .execute("CREATE TABLE t2 (x INTEGER PRIMARY KEY, y)")
        .unwrap();
    writer
        .execute("INSERT INTO t1 VALUES (1, 1), (2, 2)")
        .unwrap();
    writer
        .execute("INSERT INTO t2 VALUES (1, 1), (2, 2)")
        .unwrap();
    // Populate sqlite_stat1 so the connect-time refresh has rows to scan.
    writer.execute("ANALYZE").unwrap();

    // Park a second ANALYZE inside its commit: its new sqlite_stat1 versions
    // stay in `Preparing` until the statement is stepped again.
    writer.set_yield_injector(Some(FixedYieldInjector::new([
        CommitYieldPoint::LogRecordMarkedWritten.point(),
    ])));
    let mut parked = writer.prepare("ANALYZE t2").unwrap();
    assert!(
        matches!(
            step_until_yield_or_done_pumping_io(&mut parked, database.io.as_ref()),
            turso_core::StepResult::Yield
        ),
        "ANALYZE t2 should park inside its commit"
    );
    writer.set_yield_injector(None);

    // While the writer is parked, connect_async must keep handing the wait
    // back instead of completing or spinning. (`connect()` never returns here.)
    let mut state = turso_core::ConnectAsyncState::new();
    let mut yielded = false;
    for _ in 0..1_000 {
        match database
            .connect_async(ConnectOpts::new(SessionRole::Superuser), &mut state)
            .unwrap()
        {
            turso_core::IOResult::Done(_) => panic!(
                "connect must not complete while the commit its stats scan depends on is parked"
            ),
            turso_core::IOResult::IO(io) => {
                yielded |= io.is_explicit_yield();
                io.wait(database.io.as_ref()).unwrap();
            }
        }
    }
    assert!(
        yielded,
        "the commit-dependency wait should surface as an explicit yield"
    );

    // Let the parked commit finish. The dependency resolves and the same
    // connect state runs to completion with fresh stats.
    loop {
        match parked.step().unwrap() {
            turso_core::StepResult::Done => break,
            turso_core::StepResult::IO | turso_core::StepResult::Yield => {
                if let Some(io) = parked.take_io_completions() {
                    io.wait(database.io.as_ref()).unwrap();
                }
            }
            other => panic!("unexpected step result finishing ANALYZE: {other:?}"),
        }
    }
    let conn = loop {
        match database
            .connect_async(ConnectOpts::new(SessionRole::Superuser), &mut state)
            .unwrap()
        {
            turso_core::IOResult::Done(conn) => break conn,
            turso_core::IOResult::IO(io) => io.wait(database.io.as_ref()).unwrap(),
        }
    };
    let schema = conn.current_schema();
    assert!(
        schema
            .analyze_stats
            .table_stats(NamespaceId::ROOT, "t1")
            .is_some()
            && schema
                .analyze_stats
                .table_stats(NamespaceId::ROOT, "t2")
                .is_some(),
        "connect_async should load the committed ANALYZE stats: {:?}",
        schema.analyze_stats
    );
}

/// Abandoning a connect whose stats refresh is parked on another transaction
/// must not block: the state drops its half-built connection and the nested
/// `sqlite_stat1` statement, whose reset only rolls a read transaction back.
#[test]
fn dropping_connect_async_state_mid_wait_does_not_block() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let database = db.get_db();
    let writer = db.connect();
    writer
        .execute("CREATE TABLE t1 (x INTEGER PRIMARY KEY, y)")
        .unwrap();
    writer
        .execute("INSERT INTO t1 VALUES (1, 1), (2, 2)")
        .unwrap();
    writer.execute("ANALYZE").unwrap();

    writer.set_yield_injector(Some(FixedYieldInjector::new([
        CommitYieldPoint::LogRecordMarkedWritten.point(),
    ])));
    let mut parked = writer.prepare("ANALYZE t1").unwrap();
    assert!(matches!(
        step_until_yield_or_done_pumping_io(&mut parked, database.io.as_ref()),
        turso_core::StepResult::Yield
    ));
    writer.set_yield_injector(None);

    let mut state = turso_core::ConnectAsyncState::new();
    let mut yielded = false;
    for _ in 0..100 {
        match database
            .connect_async(ConnectOpts::new(SessionRole::Superuser), &mut state)
            .unwrap()
        {
            turso_core::IOResult::Done(_) => panic!("connect must wait for the parked commit"),
            turso_core::IOResult::IO(io) => {
                yielded |= io.is_explicit_yield();
                io.wait(database.io.as_ref()).unwrap();
            }
        }
    }
    assert!(yielded);
    // The abandonment under test. If it blocked, this test would hang.
    drop(state);

    loop {
        match parked.step().unwrap() {
            turso_core::StepResult::Done => break,
            turso_core::StepResult::IO | turso_core::StepResult::Yield => {
                if let Some(io) = parked.take_io_completions() {
                    io.wait(database.io.as_ref()).unwrap();
                }
            }
            other => panic!("unexpected step result finishing ANALYZE: {other:?}"),
        }
    }
    let conn = writer;
    drop(conn);
    // A fresh blocking connect works once nothing is parked.
    let conn = db.connect();
    assert!(conn
        .current_schema()
        .analyze_stats
        .table_stats(NamespaceId::ROOT, "t1")
        .is_some());
}

/// Ignored Truncate-vs-Passive GC metrics harness.
///
/// ```console
/// cargo test -p turso_core --lib debug_gc_metrics_truncate_vs_passive -- --ignored --nocapture
/// ```
#[test]
#[ignore = "manual GC metrics debug harness"]
fn debug_gc_metrics_truncate_vs_passive() {
    fn sample_line(
        mode: &str,
        step: usize,
        updates: usize,
        snap: &turso_core::mvcc::database::GcDebugSnapshot,
    ) {
        println!(
            "{mode},step={step},updates={updates},\
             rows_slots={},rows_empty={},rows_versions={},\
             index_slots={},index_empty={},index_versions={},\
             live_approx={},live_at_last_gc={},\
             lwm={},durable_max={},\
             log_offset={},log_size={},txs={},\
             min_reader=({},{}),backfill=({},{})",
            snap.rows_slots,
            snap.rows_empty_slots,
            snap.rows_versions,
            snap.index_slots,
            snap.index_empty_slots,
            snap.index_versions,
            snap.live_version_count_approx,
            snap.live_versions_at_last_gc,
            snap.lwm,
            snap.durable_txid_max,
            snap.logical_log_offset,
            snap.logical_log_size,
            snap.active_txs,
            snap.min_reader_mark.checkpoint_seq,
            { snap.min_reader_mark.frame },
            snap.backfill_floor.checkpoint_seq,
            { snap.backfill_floor.frame },
        );
    }

    /// Pinned-reader UPDATE load; explicit checkpoints.
    fn run_mode(passive: bool, keys: usize, rounds: usize, updates_per_round: usize) {
        let mode = if passive { "passive" } else { "truncate" };
        let db = if passive {
            MvccTestDbNoConn::new_with_random_db_passive()
        } else {
            MvccTestDbNoConn::new_with_random_db()
        };
        let mv = db.get_mvcc_store();
        let bootstrap = db.connect();
        bootstrap
            .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, data TEXT NOT NULL)")
            .unwrap();
        bootstrap
            .execute("PRAGMA mvcc_checkpoint_threshold = -1")
            .unwrap();
        bootstrap.execute("PRAGMA mvcc_gc_threshold = 64").unwrap();

        bootstrap.execute("BEGIN CONCURRENT").unwrap();
        for i in 0..keys {
            bootstrap
                .execute(format!("INSERT INTO t VALUES ({i}, 'seed')"))
                .unwrap();
        }
        bootstrap.execute("COMMIT").unwrap();
        let ckpt = if passive { "PASSIVE" } else { "TRUNCATE" };
        let _ = bootstrap.execute(format!("PRAGMA wal_checkpoint({ckpt})"));

        let reader = db.connect();
        reader.execute("BEGIN CONCURRENT").unwrap();
        reader.execute("SELECT count(*) FROM t").unwrap();

        let writer = db.connect();
        let checkpointer = db.connect();

        println!("# mode={mode} keys={keys} rounds={rounds} updates_per_round={updates_per_round}");
        sample_line(mode, 0, 0, &mv.debug_gc_snapshot());

        let mut updates = 0usize;
        for step in 1..=rounds {
            writer.execute("BEGIN CONCURRENT").unwrap();
            for u in 0..updates_per_round {
                let id = (updates + u) % keys;
                writer
                    .execute(format!("UPDATE t SET data = 'r{step}_{u}' WHERE id = {id}"))
                    .unwrap();
            }
            writer.execute("COMMIT").unwrap();
            updates += updates_per_round;

            let ckpt_res = checkpointer.execute(format!("PRAGMA wal_checkpoint({ckpt})"));
            let ckpt_status = match &ckpt_res {
                Ok(_) => "ok".to_string(),
                Err(e) => format!("err:{e}"),
            };
            let snap = mv.debug_gc_snapshot();
            println!(
                "{mode},step={step},updates={updates},ckpt={ckpt_status},\
                 rows_slots={},rows_empty={},rows_versions={},\
                 live_approx={},lwm={},durable_max={},\
                 log_offset={},log_size={},txs={},\
                 min_reader=({},{}),backfill=({},{})",
                snap.rows_slots,
                snap.rows_empty_slots,
                snap.rows_versions,
                snap.live_version_count_approx,
                snap.lwm,
                snap.durable_txid_max,
                snap.logical_log_offset,
                snap.logical_log_size,
                snap.active_txs,
                snap.min_reader_mark.checkpoint_seq,
                { snap.min_reader_mark.frame },
                snap.backfill_floor.checkpoint_seq,
                { snap.backfill_floor.frame },
            );
        }

        reader.execute("COMMIT").unwrap();
        let _ = checkpointer.execute(format!("PRAGMA wal_checkpoint({ckpt})"));
        sample_line(mode, rounds + 1, updates, &mv.debug_gc_snapshot());
    }

    /// Same load without a pinned reader.
    fn run_mode_unpinned(passive: bool, keys: usize, rounds: usize, updates_per_round: usize) {
        let mode = if passive {
            "passive_unpinned"
        } else {
            "truncate_unpinned"
        };
        let db = if passive {
            MvccTestDbNoConn::new_with_random_db_passive()
        } else {
            MvccTestDbNoConn::new_with_random_db()
        };
        let mv = db.get_mvcc_store();
        let bootstrap = db.connect();
        bootstrap
            .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, data TEXT NOT NULL)")
            .unwrap();
        bootstrap
            .execute("PRAGMA mvcc_checkpoint_threshold = -1")
            .unwrap();
        bootstrap.execute("PRAGMA mvcc_gc_threshold = 64").unwrap();

        bootstrap.execute("BEGIN CONCURRENT").unwrap();
        for i in 0..keys {
            bootstrap
                .execute(format!("INSERT INTO t VALUES ({i}, 'seed')"))
                .unwrap();
        }
        bootstrap.execute("COMMIT").unwrap();
        let ckpt = if passive { "PASSIVE" } else { "TRUNCATE" };
        let _ = bootstrap.execute(format!("PRAGMA wal_checkpoint({ckpt})"));

        let writer = db.connect();
        let checkpointer = db.connect();
        println!("# mode={mode} keys={keys} rounds={rounds} updates_per_round={updates_per_round}");
        sample_line(mode, 0, 0, &mv.debug_gc_snapshot());

        let mut updates = 0usize;
        for step in 1..=rounds {
            writer.execute("BEGIN CONCURRENT").unwrap();
            for u in 0..updates_per_round {
                let id = (updates + u) % keys;
                writer
                    .execute(format!("UPDATE t SET data = 'r{step}_{u}' WHERE id = {id}"))
                    .unwrap();
            }
            writer.execute("COMMIT").unwrap();
            updates += updates_per_round;

            let ckpt_res = checkpointer.execute(format!("PRAGMA wal_checkpoint({ckpt})"));
            let ckpt_status = match &ckpt_res {
                Ok(_) => "ok".to_string(),
                Err(e) => format!("err:{e}"),
            };
            let snap = mv.debug_gc_snapshot();
            let (wal_seq, wal_max) = checkpointer.get_pager().wal_pos();
            let wal_bf = checkpointer.get_pager().wal_backfill_frame().unwrap_or(0);
            println!(
                "{mode},step={step},updates={updates},ckpt={ckpt_status},\
                 rows_slots={},rows_empty={},rows_versions={},\
                 live_approx={},lwm={},durable_max={},\
                 log_offset={},log_size={},txs={},\
                 min_reader=({},{}),backfill=({},{}),\
                 wal_pos=({wal_seq},{wal_max}),wal_nbackfill={wal_bf}",
                snap.rows_slots,
                snap.rows_empty_slots,
                snap.rows_versions,
                snap.live_version_count_approx,
                snap.lwm,
                snap.durable_txid_max,
                snap.logical_log_offset,
                snap.logical_log_size,
                snap.active_txs,
                snap.min_reader_mark.checkpoint_seq,
                { snap.min_reader_mark.frame },
                snap.backfill_floor.checkpoint_seq,
                { snap.backfill_floor.frame },
            );
        }
    }

    const KEYS: usize = 200;
    const ROUNDS: usize = 5;
    const UPDATES_PER_ROUND: usize = 50;
    run_mode_unpinned(false, KEYS, ROUNDS, UPDATES_PER_ROUND);
    run_mode_unpinned(true, KEYS, ROUNDS, UPDATES_PER_ROUND);
    run_mode(false, KEYS, ROUNDS, UPDATES_PER_ROUND);
    run_mode(true, KEYS, ROUNDS, UPDATES_PER_ROUND);
}

/// Passive checkpoint publishes nbackfills and reclaims superseded versions.
#[test]
fn mvcc_passive_checkpoint_publishes_backfill_and_reclaims_versions() {
    let db = MvccTestDbNoConn::new_with_random_db_passive();
    let mv = db.get_mvcc_store();
    let conn = db.connect();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, data TEXT NOT NULL)")
        .unwrap();
    conn.execute("PRAGMA mvcc_checkpoint_threshold = -1")
        .unwrap();

    conn.execute("BEGIN CONCURRENT").unwrap();
    for i in 0..50 {
        conn.execute(format!("INSERT INTO t VALUES ({i}, 'seed')"))
            .unwrap();
    }
    conn.execute("COMMIT").unwrap();
    conn.execute("PRAGMA wal_checkpoint(PASSIVE)").unwrap();

    let after_seed = mv.debug_gc_snapshot();
    assert!(
        after_seed.backfill_floor.frame > 0 || after_seed.rows_versions == 0,
        "passive checkpoint must advance backfill_floor or reclaim seed versions: {after_seed:?}"
    );

    for round in 0..5 {
        conn.execute("BEGIN CONCURRENT").unwrap();
        for i in 0..50 {
            conn.execute(format!("UPDATE t SET data = 'r{round}' WHERE id = {i}"))
                .unwrap();
        }
        conn.execute("COMMIT").unwrap();
        conn.execute("PRAGMA wal_checkpoint(PASSIVE)").unwrap();
    }

    let snap = mv.debug_gc_snapshot();
    let wal_bf = conn.get_pager().wal_backfill_frame().unwrap_or(0);
    assert!(
        wal_bf > 0,
        "passive checkpoint must publish nbackfills, got wal_nbackfill={wal_bf}, snap={snap:?}"
    );
    assert_eq!(
        snap.rows_empty_slots, 0,
        "passive Finalize must drain empty SkipMap slots: {snap:?}"
    );
    assert_eq!(
        { snap.backfill_floor.frame },
        wal_bf,
        "backfill_floor must track published nbackfills: {snap:?}"
    );

    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    let after_truncate = mv.debug_gc_snapshot();
    assert_eq!(
        after_truncate.rows_versions, 0,
        "truncate Finalize must reclaim SkipMap versions: {after_truncate:?}"
    );
    assert_eq!(
        after_truncate.rows_slots, 0,
        "truncate Finalize must unlink SkipMap slots: {after_truncate:?}"
    );
}

/// Snapshot isolation after Passive Finalize reclaims a materialized current
/// version: a reader whose snapshot predates a later write must still see the
/// pre-write value. Idle-only Rule 3 (`lwm == MAX`) is what keeps a positioned
/// cursor valid while a snapshot is open; this test covers the re-read after
/// GC has already run.
#[test]
fn passive_reader_snapshot_survives_later_write_after_row_versions_gc() {
    let db = MvccTestDbNoConn::new_with_random_db_passive();
    let mv = db.get_mvcc_store();
    let conn = db.connect();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, bal INTEGER NOT NULL)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 1000)").unwrap();
    conn.execute("PRAGMA mvcc_checkpoint_threshold = 0")
        .unwrap();
    // Seed via an actual PASSIVE checkpoint (not TRUNCATE, which always runs the
    // blocking protocol regardless of `experimental_mvcc_passive_checkpoint`): this
    // materializes row 1 into the B-tree and runs Passive Finalize GC on it.
    conn.execute("PRAGMA wal_checkpoint(PASSIVE)").unwrap();
    // With no transaction open, Rule 3 clears the sole stamped current in the
    // same Finalize sweep. The reader below must not notice either way.
    assert_eq!(
        mv.debug_gc_snapshot().rows_versions,
        0,
        "with no reader open, Passive Finalize reclaims the materialized version"
    );

    // Reader opens a snapshot BEFORE any further write.
    let reader = db.connect();
    reader.execute("BEGIN CONCURRENT").unwrap();
    let before = get_rows(&reader, "SELECT bal FROM t WHERE id = 1");
    assert_eq!(before, vec![vec![Value::from_i64(1000)]]);

    // Writer updates + immediately passive-checkpoints (threshold=0 => on commit).
    let writer = db.connect();
    writer
        .execute("PRAGMA mvcc_checkpoint_threshold = 0")
        .unwrap();
    writer
        .execute("UPDATE t SET bal = 2000 WHERE id = 1")
        .unwrap();

    // Reader re-reads the SAME row within its ALREADY-OPEN snapshot: must still see 1000.
    let after = get_rows(&reader, "SELECT bal FROM t WHERE id = 1");
    reader.execute("COMMIT").unwrap();

    assert_eq!(
        after, before,
        "reader's snapshot must not change mid-transaction"
    );
}

#[test]
fn test_passive_finalize_reclaim_text_pk_unique_lookup_still_sees_key() {
    let db = MvccTestDbNoConn::new_with_random_db_passive();
    let mv = db.get_mvcc_store();
    let conn = db.connect();
    conn.execute("CREATE TABLE t(key TEXT PRIMARY KEY, vals TEXT NOT NULL DEFAULT '')")
        .unwrap();
    conn.execute("INSERT INTO t VALUES ('k0', '11')").unwrap();
    conn.execute("PRAGMA mvcc_checkpoint_threshold = 0")
        .unwrap();
    conn.execute("PRAGMA wal_checkpoint(PASSIVE)").unwrap();
    assert_eq!(
        mv.debug_gc_snapshot().rows_versions,
        0,
        "with no reader open, Passive Finalize must reclaim the materialized table current"
    );

    let read = get_rows(&conn, "SELECT vals FROM t WHERE key = 'k0'");
    assert_eq!(
        read,
        vec![vec![Value::from_text("11")]],
        "unique lookup must see k0 after SkipMap reclaim, got {read:?}"
    );

    let newer = db.connect();
    newer.execute("BEGIN CONCURRENT").unwrap();
    let newer_read = get_rows(&newer, "SELECT vals FROM t WHERE key = 'k0'");
    assert_eq!(
        newer_read,
        vec![vec![Value::from_text("11")]],
        "a later snapshot must see k0 after reclaim, got {newer_read:?}"
    );
    newer
        .execute(
            "INSERT INTO t (key, vals) VALUES ('k0', '62') \
             ON CONFLICT(key) DO UPDATE SET vals = CASE \
               WHEN vals = '' THEN '62' \
               ELSE vals || ',' || '62' \
             END",
        )
        .unwrap();
    let after = get_rows(&newer, "SELECT vals FROM t WHERE key = 'k0'");
    assert_eq!(
        after,
        vec![vec![Value::from_text("11,62")]],
        "same txn must append onto the reclaimed unique row, got {after:?}"
    );
    newer.execute("COMMIT").unwrap();

    conn.execute("PRAGMA wal_checkpoint(PASSIVE)").unwrap();
    assert_eq!(
        mv.debug_gc_snapshot().rows_versions,
        0,
        "second Passive Finalize must reclaim the upserted table current"
    );
    let after_second = get_rows(&conn, "SELECT vals FROM t WHERE key = 'k0'");
    assert_eq!(
        after_second,
        vec![vec![Value::from_text("11,62")]],
        "unique lookup must see both appends after the second reclaim, got {after_second:?}"
    );
}

fn advance_checkpoint_until_wal_has_commit_frame(mvcc_store: Arc<Store>, conn: &Arc<Connection>) {
    let pager = conn.get_pager();
    let initial_wal_max_frame = pager.wal_max_frame().expect("mvcc mode requires wal");
    let mut checkpoint_sm = CheckpointStateMachine::new(
        pager.clone(),
        mvcc_store,
        conn.clone(),
        true,
        conn.get_sync_mode(),
        turso_core::MAIN_DB_ID,
        CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        },
    );

    for _ in 0..10_000 {
        if pager.wal_max_frame().expect("mvcc mode requires wal") > initial_wal_max_frame {
            return;
        }

        match checkpoint_sm.step(&()).unwrap() {
            TransitionResult::Io(io) => io.wait(pager.io.as_ref()).unwrap(),
            TransitionResult::Continue => {}
            TransitionResult::Done(_) => {
                panic!("checkpoint finalized before WAL had committed frames")
            }
        }
    }

    panic!("checkpoint did not produce committed WAL frame in bounded steps");
}

/// Steps `writer`'s COMMIT until it finishes or yields when the transaction
/// `selection_key` reaches `point`. A yielded COMMIT is kept unfinished.
struct CommitWriterOnExclusiveAcquireInjector {
    point: YieldPoint,
    selection_key: u64,
    writer: Arc<Connection>,
    writer_commit: Mutex<Option<turso_core::Statement>>,
    fired: AtomicBool,
}

impl CommitWriterOnExclusiveAcquireInjector {
    fn new(point: YieldPoint, selection_key: u64, writer: Arc<Connection>) -> Arc<Self> {
        Arc::new(Self {
            point,
            selection_key,
            writer,
            writer_commit: Mutex::new(None),
            fired: AtomicBool::new(false),
        })
    }

    fn fired(&self) -> bool {
        self.fired.load(Ordering::Acquire)
    }
}

impl std::fmt::Debug for CommitWriterOnExclusiveAcquireInjector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommitWriterOnExclusiveAcquireInjector")
            .field("point", &self.point)
            .field("selection_key", &self.selection_key)
            .field("fired", &self.fired())
            .finish_non_exhaustive()
    }
}

impl YieldInjector for CommitWriterOnExclusiveAcquireInjector {
    fn should_yield(&self, _instance_id: u64, selection_key: u64, point: YieldPoint) -> bool {
        if point != self.point
            || selection_key != self.selection_key
            || self.fired.swap(true, Ordering::AcqRel)
        {
            return false;
        }
        let mut commit = self.writer.prepare("COMMIT").unwrap();
        loop {
            match commit.step().unwrap() {
                StepResult::Done => break,
                StepResult::Yield => {
                    *self.writer_commit.lock() = Some(commit);
                    break;
                }
                _ => {}
            }
        }
        true
    }
}

/// Drive a statement until it yields or finishes; IO is pumped, an explicit
/// yield is handed back so the test can observe it.
fn step_until_yield_or_done_pumping_io(
    stmt: &mut turso_core::Statement,
    io: &dyn IO,
) -> turso_core::StepResult {
    for _ in 0..100_000 {
        match stmt.step().unwrap() {
            turso_core::StepResult::IO => {
                if let Some(pending) = stmt.take_io_completions() {
                    pending.wait(io).unwrap();
                }
            }
            other => return other,
        }
    }
    panic!("statement kept returning IO")
}

type Store = MvStore<MvccClock, DynAllocator>;
