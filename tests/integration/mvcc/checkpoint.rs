//! Tests of the MVCC checkpoint.
//!
//! The first tests step a checkpoint by hand. Each commits rows with SQL, starts a checkpoint
//! and steps it up to the point of interest. A test that needs a commit in the middle of a
//! checkpoint uses a passive checkpoint, because that is the only kind that lets writers
//! commit while it collects rows.
//!
//! The tests after them use SQL only: they checkpoint with `PRAGMA wal_checkpoint` and check
//! what queries return, often after the database was closed and opened again.

use super::test_db::MvccTestDbNoConn;
use crate::common::{ExecRows, TempDatabase};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use turso_core::mvcc::database::checkpoint::COLLECT_PREEMPTION_THRESHOLD;
use turso_core::mvcc::database::{
    CheckpointState, CheckpointStateMachine, MVTableId, MvStore, RowID, RowKey,
    SQLITE_SCHEMA_MVCC_TABLE_ID,
};
use turso_core::state_machine::{StateTransition, TransitionResult};
use turso_core::{
    CheckpointMode, ConnectOpts, Connection, DatabaseOpts, LimboError, SessionRole,
    StatementStatusCounter, Value, IO,
};

/// More rows than a checkpoint visits before it yields.
const MANY_ROWS: usize = COLLECT_PREEMPTION_THRESHOLD + 10;

#[test]
fn collection_yields_while_walking_many_table_rows() {
    for walk in [Walk::EveryRow, Walk::ChangedRows] {
        let db = TempDatabase::new_with_mvcc("collect_many_table_rows");
        let conn = connect_without_auto_checkpoint(&db);
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        prepare_walk(&db, &conn, walk);
        insert_rows(&conn, "t", 1, MANY_ROWS);
        let table = btree_id(&conn, "t");

        let mut checkpoint = SteppedCheckpoint::start(&db, &conn, TRUNCATE);
        checkpoint.step_until(CheckpointState::CollectTableRows);
        assert_eq!(
            checkpoint.step(),
            Step::Yielded,
            "{walk:?}: walking more than COLLECT_PREEMPTION_THRESHOLD rows must yield"
        );
        assert_eq!(checkpoint.state(), CheckpointState::CollectTableRows);
        assert!(
            checkpoint.collected_table_rowids(table).len() < MANY_ROWS,
            "{walk:?}: the yield must come before the walk is over"
        );

        checkpoint.step_until(CheckpointState::CollectIndexRows);
        assert_eq!(
            checkpoint.collected_table_rowids(table),
            (1..=MANY_ROWS as i64).collect::<Vec<_>>(),
            "{walk:?}: every row is collected once across the yields"
        );
        checkpoint.finish();
    }
}

#[test]
fn collection_yields_while_walking_many_schema_rows() {
    let db = TempDatabase::new_with_mvcc("collect_many_schema_rows");
    let conn = connect_without_auto_checkpoint(&db);
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    insert_rows(&conn, "t", 1, 1);
    let table = btree_id(&conn, "t");
    for i in 0..MANY_ROWS {
        conn.execute(format!("CREATE VIEW view_{i} AS SELECT {i}"))
            .unwrap();
    }

    let mut checkpoint = SteppedCheckpoint::start(&db, &conn, TRUNCATE);
    checkpoint.step_until(CheckpointState::CollectTableRows);
    assert_eq!(
        checkpoint.step(),
        Step::Yielded,
        "walking more than COLLECT_PREEMPTION_THRESHOLD schema rows must yield"
    );
    assert!(
        checkpoint
            .machine
            .collected_table_rows()
            .all(|(row_id, _)| row_id.table_id == SQLITE_SCHEMA_MVCC_TABLE_ID),
        "schema rows are walked before the rows of any table"
    );

    checkpoint.step_until(CheckpointState::CollectIndexRows);
    assert_eq!(
        checkpoint.collected_table_rowids(table),
        vec![1],
        "table rows are still collected after a yield among the schema rows"
    );
    checkpoint.finish();
}

#[test]
fn collection_yields_while_walking_many_index_rows() {
    for walk in [Walk::EveryRow, Walk::ChangedRows] {
        let db = TempDatabase::new_with_mvcc("collect_many_index_rows");
        let conn = connect_without_auto_checkpoint(&db);
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("CREATE INDEX t_v ON t(v)").unwrap();
        prepare_walk(&db, &conn, walk);
        insert_rows(&conn, "t", 1, MANY_ROWS);
        let index = btree_id(&conn, "t_v");

        let mut checkpoint = SteppedCheckpoint::start(&db, &conn, TRUNCATE);
        checkpoint.step_until(CheckpointState::CollectIndexRows);
        assert_eq!(
            checkpoint.step(),
            Step::Yielded,
            "{walk:?}: walking more than COLLECT_PREEMPTION_THRESHOLD index rows must yield"
        );
        assert_eq!(checkpoint.state(), CheckpointState::CollectIndexRows);

        checkpoint.step_while(CheckpointState::CollectIndexRows);
        assert_eq!(
            checkpoint.collected_index_row_count(index),
            MANY_ROWS,
            "{walk:?}: every index row is collected once across the yields"
        );
        checkpoint.finish();
    }
}

#[test]
fn rows_of_a_table_dropped_before_its_first_checkpoint_are_not_collected() {
    let db = TempDatabase::new_with_mvcc("collect_dropped_table");
    let conn = connect_without_auto_checkpoint(&db);
    conn.execute("CREATE TABLE dropped(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("CREATE TABLE kept(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    insert_rows(&conn, "dropped", 1, 2);
    insert_rows(&conn, "kept", 1, 1);
    let dropped = btree_id(&conn, "dropped");
    let kept = btree_id(&conn, "kept");
    conn.execute("DROP TABLE dropped").unwrap();

    let mut checkpoint = SteppedCheckpoint::start(&db, &conn, TRUNCATE);
    checkpoint.step_until(CheckpointState::CollectIndexRows);

    assert!(
        checkpoint.machine.collected_table_drop(dropped),
        "the checkpoint must learn of the DROP from the schema rows"
    );
    assert_eq!(
        checkpoint.collected_table_rowids(dropped),
        Vec::<i64>::new(),
        "rows of a dropped table must not be written"
    );
    assert_eq!(checkpoint.collected_table_rowids(kept), vec![1]);
    checkpoint.finish();
}

#[test]
fn only_table_rows_changed_since_the_last_checkpoint_are_collected() {
    let db = TempDatabase::new_with_mvcc("collect_changed_table_rows");
    let conn = connect_without_auto_checkpoint(&db);
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    insert_rows(&conn, "t", 1, 200);
    SteppedCheckpoint::start(&db, &conn, TRUNCATE).finish();
    let table = btree_id(&conn, "t");
    // Rows next to each other and rows far apart: the walk has to find both.
    conn.execute("UPDATE t SET v = 'changed' WHERE id IN (1, 2, 3, 150, 200)")
        .unwrap();

    let mut checkpoint = SteppedCheckpoint::start(&db, &conn, TRUNCATE);
    checkpoint.step_until(CheckpointState::CollectIndexRows);

    assert_eq!(
        checkpoint.collected_table_rowids(table),
        vec![1, 2, 3, 150, 200]
    );
    checkpoint.finish();
}

#[test]
fn only_index_rows_changed_since_the_last_checkpoint_are_collected() {
    let db = TempDatabase::new_with_mvcc("collect_changed_index_rows");
    let conn = connect_without_auto_checkpoint(&db);
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX t_v ON t(v)").unwrap();
    insert_rows(&conn, "t", 1, 10);
    SteppedCheckpoint::start(&db, &conn, TRUNCATE).finish();
    let index = btree_id(&conn, "t_v");
    conn.execute("INSERT INTO t VALUES (11, 'new')").unwrap();

    let mut checkpoint = SteppedCheckpoint::start(&db, &conn, TRUNCATE);
    checkpoint.step_until_collected();

    assert_eq!(checkpoint.collected_index_row_count(index), 1);
    checkpoint.finish();
}

#[test]
fn first_checkpoint_after_reopen_collects_rows_that_no_commit_marked() {
    let db = TempDatabase::new_with_mvcc("collect_after_reopen");
    {
        let conn = connect_without_auto_checkpoint(&db);
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        insert_rows(&conn, "t", 1, 10);
        conn.close().unwrap();
    }
    let path = db.path.clone();
    drop(db);
    let db = TempDatabase::new_with_existent(&path);
    let conn = connect_without_auto_checkpoint(&db);
    let table = btree_id(&conn, "t");
    assert!(
        store(&conn).checkpoint_dirty_table_keys.is_empty(),
        "recovery marks no row as changed"
    );

    let mut checkpoint = SteppedCheckpoint::start(&db, &conn, TRUNCATE);
    checkpoint.step_until(CheckpointState::CollectIndexRows);

    assert_eq!(
        checkpoint.collected_table_rowids(table),
        (1..=10).collect::<Vec<_>>(),
        "the first checkpoint after an open walks every row, marked or not"
    );
    checkpoint.finish();
}

#[test]
fn collection_finishes_while_table_rows_are_appended() {
    for walk in [Walk::EveryRow, Walk::ChangedRows] {
        let db = passive_checkpoint_database("collect_appended_table_rows");
        let conn = connect_without_auto_checkpoint(&db);
        let writer = connect_without_auto_checkpoint(&db);
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("CREATE TABLE other(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        prepare_walk(&db, &conn, walk);
        insert_rows(&conn, "t", 1, MANY_ROWS);
        insert_rows(&conn, "other", 1, 1);
        let table = btree_id(&conn, "t");
        let other = btree_id(&conn, "other");

        let mut checkpoint = SteppedCheckpoint::start(&db, &conn, PASSIVE);
        checkpoint.step_until(CheckpointState::CollectTableRows);
        let mut next_rowid = MANY_ROWS + 1;
        let mut yields = 0;
        while checkpoint.state() == CheckpointState::CollectTableRows {
            if checkpoint.step() == Step::Yielded {
                yields += 1;
                assert!(
                    yields <= 4,
                    "{walk:?}: collection kept walking rows committed after its snapshot"
                );
                insert_rows(&writer, "t", next_rowid, COLLECT_PREEMPTION_THRESHOLD);
                next_rowid += COLLECT_PREEMPTION_THRESHOLD;
            }
        }
        assert!(yields > 0, "{walk:?}: the test needs a yield to append at");

        assert_eq!(
            checkpoint.collected_table_rowids(table),
            (1..=MANY_ROWS as i64).collect::<Vec<_>>(),
            "{walk:?}: only the rows committed before the snapshot are collected"
        );
        assert_eq!(checkpoint.collected_table_rowids(other), vec![1]);
        checkpoint.finish();
    }
}

#[test]
fn collection_finishes_while_index_rows_are_appended() {
    for walk in [Walk::EveryRow, Walk::ChangedRows] {
        let db = passive_checkpoint_database("collect_appended_index_rows");
        let conn = connect_without_auto_checkpoint(&db);
        let writer = connect_without_auto_checkpoint(&db);
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("CREATE INDEX t_v ON t(v)").unwrap();
        prepare_walk(&db, &conn, walk);
        insert_rows(&conn, "t", 1, MANY_ROWS);
        let index = btree_id(&conn, "t_v");

        let mut checkpoint = SteppedCheckpoint::start(&db, &conn, PASSIVE);
        checkpoint.step_until(CheckpointState::CollectIndexRows);
        let mut next_rowid = MANY_ROWS + 1;
        let mut yields = 0;
        while checkpoint.state() == CheckpointState::CollectIndexRows {
            if checkpoint.step() == Step::Yielded {
                yields += 1;
                assert!(
                    yields <= 4,
                    "{walk:?}: collection kept walking index rows committed after its snapshot"
                );
                insert_rows(&writer, "t", next_rowid, COLLECT_PREEMPTION_THRESHOLD);
                next_rowid += COLLECT_PREEMPTION_THRESHOLD;
            }
        }
        assert!(yields > 0, "{walk:?}: the test needs a yield to append at");

        assert_eq!(
            checkpoint.collected_index_row_count(index),
            MANY_ROWS,
            "{walk:?}: only the index rows committed before the snapshot are collected"
        );
        checkpoint.finish();
    }
}

#[test]
fn rows_committed_after_the_snapshot_stay_marked_for_the_next_checkpoint() {
    for walk in [Walk::EveryRow, Walk::ChangedRows] {
        let db = passive_checkpoint_database("prune_keeps_later_commits");
        let conn = connect_without_auto_checkpoint(&db);
        let writer = connect_without_auto_checkpoint(&db);
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("CREATE INDEX t_v ON t(v)").unwrap();
        prepare_walk(&db, &conn, walk);
        conn.execute("INSERT INTO t VALUES (1, 'written'), (3, 'deleted later')")
            .unwrap();
        let table = btree_id(&conn, "t");
        let store = store(&conn);

        let mut checkpoint = SteppedCheckpoint::start(&db, &conn, PASSIVE);
        checkpoint.step_until(CheckpointState::CollectTableRows);
        writer
            .execute("INSERT INTO t VALUES (2, 'inserted later')")
            .unwrap();
        writer.execute("DELETE FROM t WHERE id = 3").unwrap();
        checkpoint.finish();

        assert!(
            !is_marked(&store, table, 1),
            "{walk:?}: a row the checkpoint wrote in full is no longer marked"
        );
        assert!(
            is_marked(&store, table, 2),
            "{walk:?}: a row inserted after the snapshot is still marked"
        );
        assert!(
            is_marked(&store, table, 3),
            "{walk:?}: a row deleted after the snapshot is still marked"
        );
        assert_eq!(
            store.checkpoint_dirty_index_keys.len(),
            2,
            "{walk:?}: the index rows of the later insert and the later delete are still marked"
        );

        SteppedCheckpoint::start(&db, &conn, PASSIVE).finish();
        assert!(store.checkpoint_dirty_table_keys.is_empty());
        assert!(store.checkpoint_dirty_index_keys.is_empty());
    }
}

#[test]
fn row_changed_after_collection_stays_marked_for_the_next_checkpoint() {
    let db = passive_checkpoint_database("prune_keeps_row_marked_again");
    let conn = connect_without_auto_checkpoint(&db);
    let writer = connect_without_auto_checkpoint(&db);
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (7, 'first')").unwrap();
    let table = btree_id(&conn, "t");
    let store = store(&conn);

    let mut checkpoint = SteppedCheckpoint::start(&db, &conn, PASSIVE);
    checkpoint.step_until_collected();
    assert_eq!(checkpoint.collected_table_rowids(table), vec![7]);
    writer
        .execute("UPDATE t SET v = 'second' WHERE id = 7")
        .unwrap();
    checkpoint.finish();

    assert!(
        is_marked(&store, table, 7),
        "a row marked again after collection must stay marked"
    );
}

#[test]
fn clearing_the_marks_yields_on_many_rows() {
    let db = TempDatabase::new_with_mvcc("prune_many_rows");
    let conn = connect_without_auto_checkpoint(&db);
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    insert_rows(&conn, "t", 1, MANY_ROWS);
    let store = store(&conn);

    let mut checkpoint = SteppedCheckpoint::start(&db, &conn, TRUNCATE);
    checkpoint.step_until(CheckpointState::PruneDirtyKeys);
    assert_eq!(
        checkpoint.step(),
        Step::Yielded,
        "clearing more than COLLECT_PREEMPTION_THRESHOLD marks must yield"
    );
    assert_eq!(checkpoint.state(), CheckpointState::PruneDirtyKeys);
    checkpoint.finish();

    assert!(store.checkpoint_dirty_table_keys.is_empty());
}

#[test]
fn removing_written_table_versions_yields_on_many_rows() {
    let db = TempDatabase::new_with_mvcc("gc_many_table_rows");
    let conn = connect_without_auto_checkpoint(&db);
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    insert_rows(&conn, "t", 1, MANY_ROWS);
    let table = btree_id(&conn, "t");
    let store = store(&conn);
    assert_eq!(table_version_count(&store, table), MANY_ROWS);

    let mut checkpoint = SteppedCheckpoint::start(&db, &conn, TRUNCATE);
    checkpoint.step_until_state(|state| matches!(state, CheckpointState::GcTableRows { .. }));
    assert_eq!(
        checkpoint.step(),
        Step::Yielded,
        "removing the versions of more than COLLECT_PREEMPTION_THRESHOLD rows must yield"
    );
    assert!(matches!(
        checkpoint.state(),
        CheckpointState::GcTableRows { .. }
    ));
    checkpoint.finish();

    assert_eq!(
        table_version_count(&store, table),
        0,
        "the versions the checkpoint wrote to the database file are removed"
    );
    let rows: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM t");
    assert_eq!(rows, vec![(MANY_ROWS as i64,)]);
}

#[test]
fn removing_written_index_versions_yields_on_many_rows() {
    let db = TempDatabase::new_with_mvcc("gc_many_index_rows");
    let conn = connect_without_auto_checkpoint(&db);
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX t_v ON t(v)").unwrap();
    insert_rows(&conn, "t", 1, MANY_ROWS);
    let index = btree_id(&conn, "t_v");
    let store = store(&conn);
    assert_eq!(index_version_count(&store, index), MANY_ROWS);

    let mut checkpoint = SteppedCheckpoint::start(&db, &conn, TRUNCATE);
    checkpoint.step_until_state(|state| matches!(state, CheckpointState::GcIndexRows { .. }));
    assert_eq!(
        checkpoint.step(),
        Step::Yielded,
        "removing the versions of more than COLLECT_PREEMPTION_THRESHOLD index rows must yield"
    );
    assert!(matches!(
        checkpoint.state(),
        CheckpointState::GcIndexRows { .. }
    ));
    checkpoint.finish();

    assert_eq!(
        index_version_count(&store, index),
        0,
        "the index versions the checkpoint wrote to the database file are removed"
    );
    let rows: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM t INDEXED BY t_v WHERE v > ''");
    assert_eq!(rows, vec![(MANY_ROWS as i64,)]);
}

/// Passive checkpoint may run btree writes while a pinned reader is active.
#[test]
fn mvcc_passive_checkpoint_busy_under_pinned_reader_no_corruption() {
    let db = MvccTestDbNoConn::new_with_random_db_passive();
    let writer = db.connect();
    let reader = db.connect();

    writer
        .execute("CREATE TABLE t(k TEXT PRIMARY KEY, v TEXT)")
        .unwrap();
    writer.execute("PRAGMA wal_checkpoint(PASSIVE)").unwrap();

    for i in 0..5 {
        writer
            .execute(format!("INSERT INTO t VALUES ('k{i}', 'v{i}')"))
            .unwrap();
    }

    // Reader opens a snapshot and sees all 5 committed rows.
    reader.execute("BEGIN CONCURRENT").unwrap();
    assert_eq!(
        get_rows(&reader, "SELECT count(*) FROM t"),
        vec![vec![Value::from_i64(5)]],
        "reader must see all 5 committed rows",
    );

    // Passive checkpoint may complete while the reader is pinned; it must not
    // corrupt the reader's snapshot.
    writer.execute("PRAGMA wal_checkpoint(PASSIVE)").unwrap();

    assert_eq!(
        get_rows(&reader, "SELECT count(*) FROM t"),
        vec![vec![Value::from_i64(5)]],
        "reader snapshot must be unchanged after checkpoint under pinned reader",
    );
    reader.execute("COMMIT").unwrap();

    assert_eq!(
        get_rows(&db.connect(), "SELECT count(*) FROM t"),
        vec![vec![Value::from_i64(5)]],
    );
}

#[turso_macros::test(encryption)]
fn test_recovery_checkpoint_then_more_writes() {
    let mut db = MvccTestDbNoConn::new_maybe_encrypted(encrypted);
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
        conn.execute("INSERT INTO t VALUES (3, 'c')").unwrap();
    }

    db.restart();
    let conn = db.connect();
    let rows = get_rows(&conn, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].to_string(), "a");
    assert_eq!(rows[1][0].as_int().unwrap(), 2);
    assert_eq!(rows[1][1].to_string(), "b");
    assert_eq!(rows[2][0].as_int().unwrap(), 3);
    assert_eq!(rows[2][1].to_string(), "c");
}

#[turso_macros::test(encryption)]
fn test_btree_resident_recovery_then_checkpoint_delete_stays_deleted() {
    let mut db = MvccTestDbNoConn::new_maybe_encrypted(encrypted);
    {
        let conn = db.connect();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'keep')").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'gone')").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    }

    // Delete a B-tree resident row and crash/restart before checkpoint.
    {
        let conn = db.connect();
        conn.execute("DELETE FROM t WHERE id = 2").unwrap();
    }

    db.restart();
    {
        let conn = db.connect();
        // Recovery tombstone must hide stale B-tree row before checkpoint.
        let rows = get_rows(&conn, "SELECT id FROM t ORDER BY id");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0].as_int().unwrap(), 1);

        // After checkpoint + GC, row must stay deleted (B-tree delete persisted).
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        let rows = get_rows(&conn, "SELECT id FROM t ORDER BY id");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0].as_int().unwrap(), 1);

        let rows = get_rows(&conn, "PRAGMA integrity_check");
        assert_eq!(rows.len(), 1);
        assert_eq!(&rows[0][0].to_string(), "ok");
    }
}

/// What this test checks: Checkpoint accepts sqlite_schema index-row updates for already-checkpointed indexes
/// (e.g. column rename), without requiring create/destroy special writes.
/// Why this matters: RENAME COLUMN on indexed tables rewrites sqlite_schema index SQL text while preserving rootpage.
/// Treating that as an impossible state crashes checkpoint.
#[test]
fn test_checkpoint_allows_index_schema_update_after_rename_column() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("CREATE TABLE t(a INTEGER, b INTEGER)")
        .unwrap();
    conn.execute("CREATE INDEX idx_t_a ON t(a)").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 2)").unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    // Rewrites sqlite_schema entry for the existing index while keeping positive rootpage.
    conn.execute("ALTER TABLE t RENAME COLUMN a TO c").unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let rows = get_rows(&conn, "SELECT c, b FROM t");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].as_int().unwrap(), 2);
}

/// What this test checks: Header-only commits are durably replayed from the logical log and
/// then persisted into the database header by checkpoint.
/// Why this matters: PRAGMA header mutations (for example user_version) must survive restart
/// both before and after log truncation, including implicit autocommit statement transactions.
#[turso_macros::test(encryption)]
fn test_header_only_mutation_is_replayed_and_checkpointed() {
    let mut db = MvccTestDbNoConn::new_maybe_encrypted(encrypted);

    {
        let conn = db.connect();
        conn.execute("PRAGMA user_version = 42").unwrap();
        let rows = get_rows(&conn, "PRAGMA user_version");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0].as_int().unwrap(), 42);
    }

    db.restart();
    {
        let conn = db.connect();
        let rows = get_rows(&conn, "PRAGMA user_version");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0][0].as_int().unwrap(),
            42,
            "header mutation should recover from logical log before checkpoint",
        );
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    }

    db.restart();
    {
        let conn = db.connect();
        let rows = get_rows(&conn, "PRAGMA user_version");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0][0].as_int().unwrap(),
            42,
            "header mutation should persist in DB header after checkpoint truncates logical log",
        );
    }
}

/// What this test checks: an idle BEGIN CONCURRENT (no statement yet) holds
/// no snapshot, no checkpoint gate, and no WAL read mark, so a blocking
/// TRUNCATE checkpoint on another connection succeeds.
#[test]
fn test_idle_begin_concurrent_does_not_block_checkpoint() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let setup = db.connect();
    setup
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY)")
        .unwrap();
    setup.execute("INSERT INTO t VALUES (1)").unwrap();
    setup.close().unwrap();

    let holder = db.connect();
    holder.execute("BEGIN CONCURRENT").unwrap();

    let checkpointer = db.connect();
    checkpointer
        .execute("PRAGMA wal_checkpoint(TRUNCATE)")
        .expect("an idle BEGIN CONCURRENT must not block a blocking checkpoint");

    let rows = get_rows(&holder, "SELECT count(*) FROM t");
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    holder.execute("COMMIT").unwrap();
}

/// What this test checks: ordinary prepared reads recompile when checkpoint publishes a table root page.
/// Why this matters: root publication invalidates compiled bytecode generally, not only PRAGMA integrity_check.
#[test]
fn test_prepared_select_reprepares_after_checkpoint_root_publish() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();

    let mut stmt = conn.prepare("SELECT id, v FROM t ORDER BY id").unwrap();
    assert_eq!(stmt.stmt_status(StatementStatusCounter::Reprepare), 0);

    conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let rows = stmt.run_collect_rows().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(&rows[0][1].to_string(), "a");
    assert_eq!(stmt.stmt_status(StatementStatusCounter::Reprepare), 1);
}

/// What this test checks: data-only MVCC checkpoints do not invalidate already-prepared statements.
/// Why this matters: only root/drop publication should force reprepare; ordinary row checkpointing should leave prepared bytecode reusable.
#[test]
fn test_prepared_select_does_not_reprepare_after_data_only_checkpoint() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let mut stmt = conn.prepare("SELECT id, v FROM t ORDER BY id").unwrap();
    assert_eq!(stmt.stmt_status(StatementStatusCounter::Reprepare), 0);

    conn.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let rows = stmt.run_collect_rows().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(&rows[0][1].to_string(), "a");
    assert_eq!(rows[1][0].as_int().unwrap(), 2);
    assert_eq!(&rows[1][1].to_string(), "b");
    assert_eq!(stmt.stmt_status(StatementStatusCounter::Reprepare), 0);
}

/// What this test checks: prepared index lookups recompile when checkpoint publishes an index root page.
/// Why this matters: table and index roots are published independently, and stale index bytecode must not survive checkpoint.
#[test]
fn test_prepared_index_lookup_reprepares_after_checkpoint_root_publish() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, payload TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX idx_t_v ON t(v)").unwrap();

    let mut stmt = conn
        .prepare("SELECT id, payload FROM t INDEXED BY idx_t_v WHERE v = 'b'")
        .unwrap();
    assert_eq!(stmt.stmt_status(StatementStatusCounter::Reprepare), 0);

    conn.execute("INSERT INTO t VALUES (1, 'a', 'one')")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'b', 'two')")
        .unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let rows = stmt.run_collect_rows().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 2);
    assert_eq!(&rows[0][1].to_string(), "two");
    assert_eq!(stmt.stmt_status(StatementStatusCounter::Reprepare), 1);
}

/// Concurrent checkpoint + WWC abort stress; oracle is `integrity_check` only.
#[test]
fn test_conflict_abort_ckpt_indexed_update_savepoint_integrity_check() {
    conflict_abort_ckpt_indexed_update_body(MvccTestDbNoConn::new_with_random_db());
}

/// Same workload with passive checkpoint enabled.
#[test]
fn test_conflict_abort_ckpt_indexed_update_savepoint_integrity_check_passive() {
    conflict_abort_ckpt_indexed_update_body(MvccTestDbNoConn::new_with_random_db_passive());
}

/// What this test checks: Checkpoint transitions preserve DB/WAL/log ordering and watermark updates for the tested edge case.
/// Why this matters: Incorrect ordering breaks crash safety, replay boundaries, or durability guarantees.
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

/// What this test checks: Checkpoint transitions preserve DB/WAL/log ordering and watermark updates for the tested edge case.
/// Why this matters: Incorrect ordering breaks crash safety, replay boundaries, or durability guarantees.
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

/// What this test checks: Checkpoint transitions preserve DB/WAL/log ordering and watermark updates for the tested edge case.
/// Why this matters: Incorrect ordering breaks crash safety, replay boundaries, or durability guarantees.
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
fn test_mvcc_checkpoint_insert_or_replace_then_delete_removes_checkpointed_index_entries() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    setup_mvcc_checkpointed_indexed_table(&conn, false);

    conn.execute("INSERT OR REPLACE INTO t(id,a,b) VALUES(5,25,325)")
        .unwrap();
    conn.execute("DELETE FROM t WHERE id=5").unwrap();

    assert_checkpointed_replace_delete_result(&conn);
}

#[test]
fn test_mvcc_checkpoint_update_or_replace_then_delete_removes_checkpointed_index_entries() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    setup_mvcc_checkpointed_indexed_table(&conn, true);

    conn.execute("UPDATE OR REPLACE t SET a=25,b=325 WHERE id=5")
        .unwrap();
    conn.execute("DELETE FROM t WHERE id=5").unwrap();

    assert_checkpointed_replace_delete_result(&conn);
}

#[test]
fn test_mvcc_repeated_delete_after_replace_delete_checkpoint_is_noop() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    setup_mvcc_checkpointed_indexed_table(&conn, false);

    conn.execute("INSERT OR REPLACE INTO t(id,a,b) VALUES(5,25,325)")
        .unwrap();
    conn.execute("DELETE FROM t WHERE id=5").unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    conn.execute("DELETE FROM t WHERE id=5").unwrap();

    assert_integrity_ok(&conn);
    let rows = get_rows(&conn, "SELECT id,a,b FROM t ORDER BY id");
    assert_eq!(rows.len(), 4);
}

#[test]
fn test_mvcc_checkpoint_reopen_text_pk_upsert_delete_removes_autoindex_entry() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let mut conn = db.connect();
    conn.execute("PRAGMA journal_mode=mvcc").unwrap();
    conn.execute("CREATE TABLE t(k TEXT PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES('k1','orig')").unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t VALUES('k1','u1') ON CONFLICT(k) DO UPDATE SET v=excluded.v")
        .unwrap();
    conn.execute("INSERT INTO t VALUES('k1','u2') ON CONFLICT(k) DO UPDATE SET v=excluded.v")
        .unwrap();
    conn.execute("COMMIT").unwrap();

    conn.close().unwrap();
    drop(conn);
    db.restart();
    conn = db.connect();
    conn.execute("PRAGMA journal_mode=mvcc").unwrap();
    conn.execute("DELETE FROM t WHERE k='k1'").unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    assert_eq!(
        get_rows(&conn, "SELECT count(*) FROM t WHERE k='k1'"),
        vec![vec![Value::from_i64(0)]]
    );
    assert!(get_rows(&conn, "SELECT quote(k), quote(v) FROM t").is_empty());
    assert!(get_rows(
        &conn,
        "SELECT quote(k), quote(v) FROM t NOT INDEXED WHERE k='k1'"
    )
    .is_empty());
    assert_integrity_ok(&conn);
}

#[test]
fn test_mvcc_checkpoint_integrity_after_upsert_with_secondary_indexes() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute(
        "CREATE TABLE t(\
            id INTEGER PRIMARY KEY,\
            owner TEXT NOT NULL,\
            portable_changes TEXT NOT NULL,\
            rev INTEGER NOT NULL DEFAULT 0,\
            note TEXT,\
            bucket INTEGER NOT NULL DEFAULT 0,\
            tag TEXT,\
            status INTEGER NOT NULL DEFAULT 0\
        )",
    )
    .unwrap();
    conn.execute("CREATE INDEX t_owner_rev_idx ON t(owner, rev)")
        .unwrap();
    conn.execute("CREATE INDEX t_note_idx ON t(note)").unwrap();
    conn.execute("CREATE INDEX t_bucket_idx ON t(bucket)")
        .unwrap();
    conn.execute("CREATE INDEX t_tag_idx ON t(tag)").unwrap();
    conn.execute("CREATE INDEX t_status_idx ON t(status)")
        .unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    for id in 1..=200 {
        conn.execute(format!(
            "INSERT INTO t(id, owner, portable_changes, rev, note, bucket, tag, status) \
             VALUES ({id}, 'owner-{id}', 'portable_changes-{id}', 1, 'note-{id}', {bucket}, 'tag-{tag}', {status}) \
             ON CONFLICT(id) DO UPDATE SET \
                id = excluded.id, \
                owner = excluded.owner, \
                portable_changes = excluded.portable_changes, \
                rev = excluded.rev, \
                note = excluded.note, \
                bucket = excluded.bucket, \
                tag = excluded.tag, \
                status = excluded.status",
            bucket = id % 17,
            tag = id % 7,
            status = id % 9,
        ))
        .unwrap();
    }

    for id in 1..=100 {
        conn.execute(format!(
            "INSERT INTO t(id, owner, portable_changes, rev, note, bucket, tag, status) \
             VALUES ({id}, 'owner-{id}', 'portable_changes-{id}-updated', 2, 'note-{id}-updated', {bucket}, 'tag-{tag}-updated', {status}) \
             ON CONFLICT(id) DO UPDATE SET \
                id = excluded.id, \
                owner = excluded.owner, \
                portable_changes = excluded.portable_changes, \
                rev = excluded.rev, \
                note = excluded.note, \
                bucket = excluded.bucket, \
                tag = excluded.tag, \
                status = excluded.status",
            bucket = (id + 3) % 17,
            tag = (id + 2) % 7,
            status = (id + 1) % 9,
        ))
        .unwrap();
    }

    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
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
fn test_integrity_check_after_drop_autoincrement_table_before_checkpoint() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, data TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t(data) VALUES ('a')").unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    conn.execute("DROP TABLE t").unwrap();
    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

#[test]
fn test_idxdelete_after_truncate_clears_checkpointed_index() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, x)")
        .unwrap();
    conn.execute("CREATE INDEX t_x ON t(x)").unwrap();
    conn.execute("INSERT INTO t VALUES (1,1),(2,2)").unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    conn.execute("INSERT OR REPLACE INTO t(id,x) VALUES(2,2)")
        .unwrap();
    conn.execute("DELETE FROM t WHERE x>=1").unwrap();
    assert_eq!(
        get_rows(&conn, "SELECT count(*) FROM t"),
        vec![vec![Value::from_i64(0)]],
        "table rows must be gone after DELETE"
    );
    assert_eq!(
        get_rows(&conn, "SELECT count(*) FROM t WHERE x>=1"),
        vec![vec![Value::from_i64(0)]],
        "checkpointed index currents must not survive IdxDelete"
    );
}

#[test]
fn test_delete_via_unique_index_removes_checkpointed_rows() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, c INTEGER UNIQUE)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 400), (2, 500), (3, 600)")
        .unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    assert_eq!(
        get_rows(&conn, "SELECT count(*) FROM t"),
        vec![vec![Value::from_i64(3)]],
        "truncate must leave the three checkpointed rows"
    );
    assert_eq!(
        get_rows(&conn, "SELECT id FROM t WHERE id = 1"),
        vec![vec![Value::from_i64(1)]],
        "point lookup of checkpointed INTEGER PK must work"
    );
    conn.execute("INSERT INTO t VALUES (4, 100), (5, 200)")
        .unwrap();
    conn.execute("DELETE FROM t WHERE c < 1000").unwrap();
    assert_eq!(
        get_rows(&conn, "SELECT count(*) FROM t"),
        vec![vec![Value::from_i64(0)]],
        "unique-index DELETE must still visit checkpointed B-tree rows"
    );
}

/// Index rows live in a separate SkipMap from table rows and go through their own
/// GC path (gc_index_row_versions). This SQL-level test creates an indexed table,
/// checkpoints, updates the row (creating superseded index versions), checkpoints
/// again, and verifies the index still returns correct results. Catches regressions
/// where index GC removes versions that the dual cursor still needs.
#[test]
fn test_gc_e2e_index_rows_collected_after_checkpoint() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX idx_val ON t(val)").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'alpha')").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'beta')").unwrap();

    // Checkpoint flushes to B-tree and triggers GC on both table and index rows.
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    // After GC, reads should still work via B-tree fallthrough.
    let rows = get_rows(&conn, "SELECT val FROM t ORDER BY val");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0].to_string(), "alpha");
    assert_eq!(rows[1][0].to_string(), "beta");

    // Index scan should also work.
    let rows = get_rows(&conn, "SELECT id FROM t WHERE val = 'alpha'");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);

    // Update a row — creates new index versions.
    conn.execute("UPDATE t SET val = 'gamma' WHERE id = 1")
        .unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    // Old index entry ('alpha') should be gone, new entry ('gamma') visible.
    let rows = get_rows(&conn, "SELECT id FROM t WHERE val = 'alpha'");
    assert_eq!(rows.len(), 0);
    let rows = get_rows(&conn, "SELECT id FROM t WHERE val = 'gamma'");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);

    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

/// What this test checks: Checkpoint transitions preserve DB/WAL/log ordering and watermark updates for the tested edge case.
/// Why this matters: Incorrect ordering breaks crash safety, replay boundaries, or durability guarantees.
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

    conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();

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

/// What this test checks: Checkpoint transitions preserve DB/WAL/log ordering and watermark updates for the tested edge case.
/// Why this matters: Incorrect ordering breaks crash safety, replay boundaries, or durability guarantees.
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

/// After a DROP TABLE frees pages and a CREATE INDEX reuses one of those
/// freed pages as its new root, a subsequent checkpoint must not use the
/// stale table cursor (which lacks index_info) when writing index rows.
#[test]
fn test_checkpoint_drop_table_then_create_index_page_reuse() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("CREATE TABLE a(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("CREATE TABLE b(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("INSERT INTO a VALUES(1,'x')").unwrap();
    conn.execute("INSERT INTO b VALUES(1,'y')").unwrap();
    // First checkpoint writes both tables to the B-tree.
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    // DROP TABLE a frees its root page; CREATE INDEX may reuse it.
    conn.execute("DROP TABLE a").unwrap();
    conn.execute("CREATE INDEX new_b_v ON b(v)").unwrap();
    // Second checkpoint must handle the page reuse without panicking.
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    drop(conn);

    db.restart();

    let conn = db.connect();
    let rows = get_rows(&conn, "SELECT * FROM b");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].to_string(), "1");
    assert_eq!(rows[0][1].to_string(), "y");

    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

/// After checkpoint + GC, checkpointed current versions are removed from
/// the SkipMap. Readers must still see the data via B-tree fallthrough.
#[test]
fn test_gc_e2e_checkpointed_row_readable_after_gc() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'hello')").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'world')").unwrap();

    // Checkpoint flushes to B-tree and triggers GC.
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    // After GC, SkipMap currents that Truncate already wrote to the B-tree are
    // gone; reads fall through to the B-tree.
    let rows = get_rows(&conn, "SELECT id, val FROM t ORDER BY id");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].to_string(), "hello");
    assert_eq!(rows[1][0].as_int().unwrap(), 2);
    assert_eq!(rows[1][1].to_string(), "world");

    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

/// Multiple checkpoints with interleaved writes. Each checkpoint triggers GC.
/// Verifies cumulative correctness across GC cycles.
#[test]
fn test_gc_e2e_multiple_checkpoint_gc_cycles() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER)")
        .unwrap();

    for i in 1..=5 {
        conn.execute(format!("INSERT INTO t VALUES ({i}, {i})"))
            .unwrap();
    }
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    // Delete rows 2, 4 and update row 3.
    conn.execute("DELETE FROM t WHERE id IN (2, 4)").unwrap();
    conn.execute("UPDATE t SET val = 30 WHERE id = 3").unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    // Insert row 6, delete row 1.
    conn.execute("INSERT INTO t VALUES (6, 6)").unwrap();
    conn.execute("DELETE FROM t WHERE id = 1").unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let rows = get_rows(&conn, "SELECT id, val FROM t ORDER BY id");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][0].as_int().unwrap(), 3);
    assert_eq!(rows[0][1].as_int().unwrap(), 30);
    assert_eq!(rows[1][0].as_int().unwrap(), 5);
    assert_eq!(rows[1][1].as_int().unwrap(), 5);
    assert_eq!(rows[2][0].as_int().unwrap(), 6);
    assert_eq!(rows[2][1].as_int().unwrap(), 6);

    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

/// After concurrent autoincrement inserts and a checkpoint, sqlite_sequence
/// must reflect the true maximum rowid.
#[test]
fn test_autoincrement_sqlite_sequence_after_checkpoint() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn1 = db.connect();

    conn1
        .execute("CREATE TABLE t(a INTEGER PRIMARY KEY AUTOINCREMENT, b TEXT)")
        .unwrap();

    // Insert several rows from separate transactions
    conn1.execute("BEGIN CONCURRENT").unwrap();
    conn1.execute("INSERT INTO t(b) VALUES ('row1')").unwrap();
    conn1.execute("COMMIT").unwrap();

    let conn2 = db.connect();
    conn2.execute("BEGIN CONCURRENT").unwrap();
    conn2.execute("INSERT INTO t(b) VALUES ('row2')").unwrap();
    conn2.execute("COMMIT").unwrap();

    // Force a checkpoint to flush autoincrement entries
    conn1.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    // After checkpoint, sqlite_sequence should have the correct max (2)
    let rows = get_rows(&conn1, "SELECT seq FROM sqlite_sequence WHERE name = 't'");
    assert_eq!(rows.len(), 1, "sqlite_sequence should have entry for 't'");
    let seq = rows[0][0].as_int().unwrap();
    assert_eq!(seq, 2, "sqlite_sequence should reflect the max rowid");

    // A subsequent insert should get rowid 3
    conn1.execute("INSERT INTO t(b) VALUES ('row3')").unwrap();
    let rows = get_rows(&conn1, "SELECT MAX(a) FROM t");
    assert_eq!(rows[0][0].as_int().unwrap(), 3);
}

/// Reproducer for a bug where log replay after checkpoint-restart-checkpoint-restart
/// panics with "table id that does not exist in the table_id_to_rootpage map".
///
/// The scenario from the simulator:
/// 1. Create many tables, insert data, checkpoint (tables get positive root pages)
/// 2. Restart (recovery rebuilds table_id_to_rootpage from btree schema)
/// 3. Create more tables + insert into old and new tables
/// 4. Checkpoint (all tables now have positive root pages, log is truncated)
/// 5. Insert more data into all tables (un-checkpointed, written to log with
///    table IDs assigned in this server incarnation)
/// 6. Restart → bootstrap rebuilds map from btree root pages, then log replay
///    sees row inserts for table IDs that may not match the bootstrap mapping
#[test]
fn test_recovery_many_tables_checkpoint_restart_checkpoint_restart() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let num_initial_tables = 50;
    let num_extra_tables = 30;

    // Step 1: Create many tables, insert data, checkpoint
    {
        let conn = db.connect();
        for i in 0..num_initial_tables {
            conn.execute(format!("CREATE TABLE t{i}(id INTEGER PRIMARY KEY, v TEXT)"))
                .unwrap();
            conn.execute(format!("INSERT INTO t{i} VALUES (1, 'init')"))
                .unwrap();
        }
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        conn.close().unwrap();
    }

    // Step 2: Restart (simulates server redeploy)
    db.restart();

    // Step 3: Create more tables + insert into old tables, then checkpoint
    {
        let conn = db.connect();
        // Create new tables (these get new negative table IDs)
        for i in 0..num_extra_tables {
            conn.execute(format!(
                "CREATE TABLE extra{i}(id INTEGER PRIMARY KEY, v TEXT)"
            ))
            .unwrap();
            conn.execute(format!("INSERT INTO extra{i} VALUES (1, 'extra')"))
                .unwrap();
        }
        // Insert into the original tables
        for i in 0..num_initial_tables {
            conn.execute(format!("INSERT INTO t{i} VALUES (2, 'after_restart')"))
                .unwrap();
        }
        // Step 4: Checkpoint - all tables get positive root pages, log truncated
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

        // Step 5: More writes after checkpoint (un-checkpointed, in the log)
        for i in 0..num_initial_tables {
            conn.execute(format!("INSERT INTO t{i} VALUES (3, 'post_ckpt2')"))
                .unwrap();
        }
        for i in 0..num_extra_tables {
            conn.execute(format!(
                "INSERT INTO extra{i} VALUES (2, 'extra_post_ckpt')"
            ))
            .unwrap();
        }
        conn.close().unwrap();
    }

    // Step 6: Restart again - log replay should not panic
    db.restart();

    // Verify data integrity
    {
        let conn = db.connect();
        for i in 0..num_initial_tables {
            let rows = get_rows(&conn, &format!("SELECT id, v FROM t{i} ORDER BY id"));
            assert_eq!(
                rows.len(),
                3,
                "table t{i} should have 3 rows, got {}",
                rows.len()
            );
        }
        for i in 0..num_extra_tables {
            let rows = get_rows(&conn, &format!("SELECT id, v FROM extra{i} ORDER BY id"));
            assert_eq!(
                rows.len(),
                2,
                "table extra{i} should have 2 rows, got {}",
                rows.len()
            );
        }
    }
}

/// What this test checks: CREATE SEQUENCE + DROP SEQUENCE between checkpoints must not
/// crash the checkpoint when it tries to delete the sqlite_schema row from the B-tree.
///
/// Why this matters: Sequence schema rows have type="sequence" and rootpage=0, so they
/// are not recognized by `sqlite_schema_btree_identity()`. Without a fix, the checkpoint
/// adds them to the write_set via the `is_schema_delete` path (for tracking destroyed
/// tables), but the WriteRow handler then tries to B-tree-delete a row that was never
/// checkpointed, causing "MVCC delete: rowid N not found".
#[test]
fn test_checkpoint_after_create_and_drop_sequence() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    conn.execute("CREATE SEQUENCE seq1").unwrap();
    conn.execute("DROP SEQUENCE seq1").unwrap();

    // This checkpoint should not crash. The sqlite_schema row for seq1 was
    // created and deleted without an intervening checkpoint, so it does not
    // exist in the B-tree.
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let rows = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][0].to_string(), "ok");
}

#[test]
fn test_checkpoint_seek_skip_divider_reinsert_loses_row() {
    let _ = tracing_subscriber::fmt::try_init();
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    conn.execute("PRAGMA mvcc_checkpoint_threshold = 0")
        .unwrap();
    // Same shape as the stress table hot_floor_424: tiny rows, rowid-alias PK.
    conn.execute("CREATE TABLE t (v REAL NOT NULL, pk INTEGER PRIMARY KEY)")
        .unwrap();
    // Bulk-load in ONE transaction => one checkpoint pass bulk-inserts ascending and
    // splits the btree, creating dividers (~every 250 rows at this record size).
    conn.execute("BEGIN").unwrap();
    for i in 0..1000 {
        conn.execute(format!("INSERT INTO t VALUES ({}.5, {})", i % 7, i))
            .unwrap();
    }
    conn.execute("COMMIT").unwrap();

    conn.execute("BEGIN").unwrap();
    for i in (0..1000).filter(|i| i % 5 == 3) {
        conn.execute(format!("DELETE FROM t WHERE pk = {i}"))
            .unwrap();
    }
    conn.execute("COMMIT").unwrap();

    for g in 1..400i64 {
        conn.execute(format!("DELETE FROM t WHERE pk = {g}"))
            .unwrap();
        conn.execute("BEGIN").unwrap();
        conn.execute(format!("INSERT OR REPLACE INTO t VALUES (1.25, {})", g - 1))
            .unwrap();
        conn.execute(format!("INSERT INTO t VALUES (2.5, {g})"))
            .unwrap();
        conn.execute("COMMIT").unwrap();
        let rows = get_rows(&conn, &format!("SELECT pk FROM t WHERE pk = {g}"));
        assert_eq!(
            rows.len(),
            1,
            "rowid {g} vanished from point lookup after re-insert \
             (checkpoint seek-skip wrote it on the wrong side of the divider)"
        );
    }
}

/// Regression test for https://github.com/tursodatabase/turso/issues/7642.
///
/// A checkpoint that hits `max_page_count` must not leave the connection with
/// a physical root mapping for a page that was never written to the database.
#[test]
fn test_read_after_database_full_checkpoint_remains_usable() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    conn.execute("PRAGMA mvcc_checkpoint_threshold = -1")
        .unwrap();
    conn.execute("PRAGMA max_page_count = 5").unwrap();
    conn.execute("CREATE TABLE t(x TEXT)").unwrap();
    conn.execute(
        "INSERT INTO t
         SELECT printf('%.*c', 1800, 'x')
           FROM generate_series(1, 60)",
    )
    .unwrap();

    expect_database_full_checkpoint(conn.execute("PRAGMA wal_checkpoint(TRUNCATE)"));

    let mut read = conn.prepare("SELECT count(*) FROM t").unwrap();
    let rows = read
        .run_collect_rows()
        .expect("read after failed checkpoint must not short-read an unwritten root page");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_int().unwrap(), 60);
}

/// Regression test for https://github.com/tursodatabase/turso/issues/7642.
///
/// A failed automatic checkpoint must not persist an UPDATE against a recycled
/// root belonging to a different table when the database is reopened.
#[test]
fn test_auto_checkpoint_update_stays_with_source_table_after_restart() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn = db.connect();
        prepare_recycled_root_pages_for_failed_checkpoint(&conn);
        conn.execute("PRAGMA max_page_count = 6").unwrap();
        conn.execute("PRAGMA mvcc_checkpoint_threshold = 0")
            .unwrap();
        conn.execute("BEGIN").unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, x TEXT, b BLOB)")
            .unwrap();
        conn.execute("CREATE TABLE u(id INTEGER PRIMARY KEY, x TEXT, b BLOB)")
            .unwrap();
        conn.execute(
            "INSERT INTO t
             SELECT value, 't' || value, zeroblob(1800)
               FROM generate_series(1, 60)",
        )
        .unwrap();
        conn.execute("INSERT INTO t VALUES(1000, 't_old', zeroblob(1800))")
            .unwrap();
        conn.execute("INSERT INTO u VALUES(1000, 'u_old', zeroblob(1800))")
            .unwrap();
        conn.execute("COMMIT").unwrap();
        conn.execute("UPDATE t SET x = 't_updated_by_auto_case' WHERE id = 1000")
            .unwrap();

        assert_table_row(&conn, "t", 1000, Some("t_updated_by_auto_case"));
        assert_table_row(&conn, "u", 1000, Some("u_old"));
    }

    db.restart();
    let conn = db.connect();
    assert_table_row(&conn, "t", 1000, Some("t_updated_by_auto_case"));
    assert_table_row(&conn, "u", 1000, Some("u_old"));
    assert_table_counts(&conn, 61, 1);
    assert_integrity_ok(&conn);
}

/// Regression test for https://github.com/tursodatabase/turso/issues/7642.
///
/// A DELETE issued after a failed checkpoint must remain attached to its
/// source table both before and after recovery.
#[test]
fn test_delete_after_failed_checkpoint_stays_with_source_table() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn = db.connect();
        prepare_recycled_root_pages_for_failed_checkpoint(&conn);
        conn.execute("PRAGMA max_page_count = 6").unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, x TEXT, b BLOB)")
            .unwrap();
        conn.execute("CREATE TABLE u(id INTEGER PRIMARY KEY, x TEXT, b BLOB)")
            .unwrap();
        conn.execute(
            "INSERT INTO t
             SELECT value, 't' || value, zeroblob(1800)
               FROM generate_series(1, 60)",
        )
        .unwrap();
        conn.execute("INSERT INTO t VALUES(1000, 't_victim', zeroblob(1800))")
            .unwrap();
        conn.execute("INSERT INTO u VALUES(1000, 'u_victim', zeroblob(1800))")
            .unwrap();

        expect_database_full_checkpoint(conn.execute("PRAGMA wal_checkpoint(TRUNCATE)"));
        conn.execute("DELETE FROM t WHERE id = 1000").unwrap();

        assert_table_row(&conn, "t", 1000, None);
        assert_table_row(&conn, "u", 1000, Some("u_victim"));
        assert_table_counts(&conn, 60, 1);
    }

    db.restart();
    let conn = db.connect();
    assert_table_row(&conn, "t", 1000, None);
    assert_table_row(&conn, "u", 1000, Some("u_victim"));
    assert_table_counts(&conn, 60, 1);
    assert_integrity_ok(&conn);
}

/// Regression test for https://github.com/tursodatabase/turso/issues/7642.
///
/// A failed checkpoint on one connection must not poison the shared root map
/// used by a subsequent writer connection.
#[test]
fn test_failed_checkpoint_does_not_move_other_connection_insert() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn1 = db.connect();
        let conn2 = db.connect();
        prepare_recycled_root_pages_for_failed_checkpoint(&conn1);
        conn1.execute("PRAGMA max_page_count = 6").unwrap();
        conn1
            .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, x TEXT, b BLOB)")
            .unwrap();
        conn1
            .execute("CREATE TABLE u(id INTEGER PRIMARY KEY, x TEXT, b BLOB)")
            .unwrap();
        conn1
            .execute(
                "INSERT INTO t
                 SELECT value, 't' || value, zeroblob(1800)
                   FROM generate_series(1, 60)",
            )
            .unwrap();

        expect_database_full_checkpoint(conn1.execute("PRAGMA wal_checkpoint(TRUNCATE)"));
        conn2
            .execute("INSERT INTO t VALUES(1000, 'conn2_after_leak', zeroblob(1800))")
            .unwrap();

        assert_table_row(&conn2, "t", 1000, Some("conn2_after_leak"));
        assert_table_row(&conn2, "u", 1000, None);
        assert_table_counts(&conn2, 61, 0);
    }

    db.restart();
    let conn = db.connect();
    assert_table_row(&conn, "t", 1000, Some("conn2_after_leak"));
    assert_table_row(&conn, "u", 1000, None);
    assert_table_counts(&conn, 61, 0);
    assert_integrity_ok(&conn);
}

/// Regression test for https://github.com/tursodatabase/turso/issues/7642.
///
/// Replaying a row against the wrong table must not create a table/index
/// split-brain that only becomes visible after a later successful checkpoint.
#[test]
fn test_failed_checkpoint_preserves_secondary_index_consistency() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    {
        let conn = db.connect();
        prepare_recycled_root_pages_for_failed_checkpoint(&conn);
        conn.execute("PRAGMA max_page_count = 6").unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, x TEXT, b BLOB)")
            .unwrap();
        conn.execute("CREATE TABLE u(id INTEGER PRIMARY KEY, x TEXT, b BLOB)")
            .unwrap();
        conn.execute("CREATE INDEX ux ON u(x)").unwrap();
        conn.execute(
            "INSERT INTO t
             SELECT value, 't' || value, zeroblob(1800)
               FROM generate_series(1, 60)",
        )
        .unwrap();

        expect_database_full_checkpoint(conn.execute("PRAGMA wal_checkpoint(TRUNCATE)"));
        conn.execute("INSERT INTO t VALUES(1000, 'missing_u_index', zeroblob(1800))")
            .unwrap();
    }

    db.restart();
    let conn = db.connect();
    let indexed = get_rows(
        &conn,
        "SELECT id, x FROM u INDEXED BY ux WHERE x = 'missing_u_index'",
    );
    let scanned = get_rows(
        &conn,
        "SELECT id, x FROM u NOT INDEXED WHERE x = 'missing_u_index'",
    );
    assert_eq!(
        indexed, scanned,
        "secondary-index lookup and table scan must agree after recovery"
    );
    assert!(
        scanned.is_empty(),
        "a row inserted into t must not recover under u: {scanned:?}"
    );
    assert_table_row(&conn, "t", 1000, Some("missing_u_index"));
    assert_table_row(&conn, "u", 1000, None);
    assert_integrity_ok(&conn);

    conn.execute("PRAGMA max_page_count = 1000").unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    assert_integrity_ok(&conn);
}

/// Documents *why* Truncate Finalize always sees an idle LWM, which is what lets its
/// Rule 3 pass drop sole current versions: acquiring `blocking_checkpoint_lock` for
/// write cannot succeed while any MVCC transaction — even a read-only one — is still
/// open, because `begin_tx` holds the read side of that same lock for the
/// transaction's entire lifetime. Passive never takes this lock around its write phase
/// (that exclusion is exactly the throughput it exists to avoid), so it has to test
/// `lwm == u64::MAX` directly instead — see
/// `passive_reader_snapshot_survives_later_write_after_row_versions_gc` and the
/// `gc_version_chain` doc comment.
#[test]
fn truncate_checkpoint_is_busy_while_a_reader_transaction_is_open() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, bal INTEGER NOT NULL)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 1000)").unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let reader = db.connect();
    reader.execute("BEGIN CONCURRENT").unwrap();
    assert_eq!(
        get_rows(&reader, "SELECT bal FROM t WHERE id = 1"),
        vec![vec![Value::from_i64(1000)]]
    );

    let writer = db.connect();
    writer.execute("BEGIN CONCURRENT").unwrap();
    writer
        .execute("UPDATE t SET bal = 2000 WHERE id = 1")
        .unwrap();
    writer.execute("COMMIT").unwrap();
    assert!(
        matches!(
            writer.execute("PRAGMA wal_checkpoint(TRUNCATE)"),
            Err(LimboError::Busy)
        ),
        "a second Truncate must not be able to run while the earlier reader transaction is open"
    );

    reader.execute("COMMIT").unwrap();
    // Now that the reader is gone, Truncate can proceed.
    writer.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
}

#[test]
fn late_commit_of_low_rowid_is_written_by_the_next_checkpoint() {
    let mut db = MvccTestDbNoConn::new_with_random_db_passive();
    {
        let setup = db.connect();
        setup
            .execute("PRAGMA mvcc_checkpoint_threshold = -1")
            .unwrap();
        setup
            .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        setup.execute("PRAGMA wal_checkpoint(PASSIVE)").unwrap();

        let slow = db.connect();
        slow.execute("BEGIN CONCURRENT").unwrap();
        slow.execute("INSERT INTO t VALUES (100, 'slow')").unwrap();

        let fast = db.connect();
        fast.execute("INSERT INTO t VALUES (101, 'fast')").unwrap();
        fast.execute("PRAGMA wal_checkpoint(PASSIVE)").unwrap();

        slow.execute("COMMIT").unwrap();
        fast.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    }

    db.restart();
    let conn = db.connect();
    let ids: Vec<i64> = get_rows(&conn, "SELECT id FROM t ORDER BY id")
        .iter()
        .map(|row| row[0].as_int().unwrap())
        .collect();
    assert_eq!(ids, vec![100, 101]);
}

#[test]
fn index_row_updated_after_checkpoint_is_written_by_the_next_checkpoint() {
    let mut db = MvccTestDbNoConn::new_with_random_db_passive();
    {
        let conn = db.connect();
        conn.execute("PRAGMA mvcc_checkpoint_threshold = -1")
            .unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("CREATE INDEX t_v ON t(v)").unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        conn.execute("PRAGMA wal_checkpoint(PASSIVE)").unwrap();
        conn.execute("UPDATE t SET v = 'b' WHERE id = 1").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    }

    db.restart();
    let conn = db.connect();
    let by_new = get_rows(&conn, "SELECT id FROM t INDEXED BY t_v WHERE v = 'b'");
    assert_eq!(
        by_new.len(),
        1,
        "index entry for the updated value is missing"
    );
    let by_old = get_rows(&conn, "SELECT id FROM t INDEXED BY t_v WHERE v = 'a'");
    assert!(by_old.is_empty(), "stale index entry survived: {by_old:?}");
    let integrity = get_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(integrity[0][0].to_string(), "ok");
}

#[test]
fn recovered_rows_are_written_by_the_first_checkpoint_after_restart() {
    let mut db = MvccTestDbNoConn::new_with_random_db_passive();
    {
        let conn = db.connect();
        conn.execute("PRAGMA mvcc_checkpoint_threshold = -1")
            .unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    }
    db.restart();
    {
        let conn = db.connect();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    }
    db.restart();
    let conn = db.connect();
    let rows = get_rows(&conn, "SELECT id, v FROM t");
    assert_eq!(
        rows.len(),
        1,
        "row replayed from the logical log was not checkpointed: {rows:?}"
    );
}

fn conflict_abort_ckpt_indexed_update_body(db: MvccTestDbNoConn) {
    let conn = db.connect();
    // NUMERIC UNIQUE column => autoindex, mirroring empty_leaf_594 in the trace.
    conn.execute("CREATE TABLE t (pk INTEGER PRIMARY KEY, u NUMERIC UNIQUE)")
        .unwrap();
    for i in 0..120 {
        conn.execute(format!("INSERT INTO t VALUES ({}, {})", i, 700 + i))
            .unwrap();
    }
    // Checkpoint so the seeded index/table values are btree-resident.
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    // Passive auto-checkpoint on every commit.
    conn.execute("PRAGMA mvcc_checkpoint_threshold = 0")
        .unwrap();

    // Retry transient MVCC concurrency errors (Busy / BusySnapshot / a snapshot whose
    // dependency aborted) so they aren't mistaken for a repro — only a non-"ok" integrity
    // result or a genuine error should fail the test.
    fn is_transient(e: &LimboError) -> bool {
        matches!(
            e,
            LimboError::Busy | LimboError::BusySnapshot | LimboError::CommitDependencyAborted
        )
    }
    fn read_retry(conn: &Arc<Connection>, query: &str) -> Option<Vec<Vec<Value>>> {
        for _ in 0..100_000 {
            let mut stmt = match conn.prepare(query) {
                Ok(s) => s,
                Err(e) if is_transient(&e) => {
                    std::thread::yield_now();
                    continue;
                }
                Err(e) => panic!("prepare failed: {e:?}"),
            };
            let mut rows = Vec::new();
            let res = stmt.run_with_row_callback(|row| {
                rows.push(row.get_values().cloned().collect::<Vec<_>>());
                Ok(())
            });
            match res {
                Ok(()) => return Some(rows),
                Err(e) if is_transient(&e) => {
                    std::thread::yield_now();
                    continue;
                }
                Err(e) => panic!("read query {query:?} failed: {e:?}"),
            }
        }
        None
    }

    let db_arc = db.get_db();
    let stop = Arc::new(AtomicBool::new(false));

    let reader_stop = stop.clone();
    let reader_db = db_arc.clone();
    let reader_handle = std::thread::spawn(move || {
        let reader = reader_db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        reader
            .execute("PRAGMA mvcc_checkpoint_threshold = 0")
            .unwrap();
        let mut iters = 0u64;
        while !reader_stop.load(Ordering::Acquire) {
            // Single-snapshot integrity_check: the only sound concurrent oracle — it checks
            // table rows against index entries in one consistent read, no cross-snapshot
            // assumptions.
            if let Some(ic) = read_retry(&reader, "PRAGMA integrity_check") {
                assert_eq!(
                    ic.len(),
                    1,
                    "reader iter {iters}: integrity_check rows: {ic:?}"
                );
                assert_eq!(
                    &ic[0][0].to_string(),
                    "ok",
                    "reader iter {iters}: integrity_check failed: {:?}",
                    ic[0][0].to_string()
                );
            }
            iters += 1;
        }
    });

    let writer_db = db_arc;
    let writer_handle = std::thread::spawn(move || {
        let conn1 = writer_db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        conn1
            .execute("PRAGMA mvcc_checkpoint_threshold = 0")
            .unwrap();
        let conn2 = writer_db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        conn2
            .execute("PRAGMA mvcc_checkpoint_threshold = 0")
            .unwrap();
        let exec_retry = |c: &Arc<Connection>, sql: &str| -> Result<(), LimboError> {
            for _ in 0..1000 {
                match c.execute(sql) {
                    Ok(_) => return Ok(()),
                    Err(LimboError::Busy) => std::thread::yield_now(),
                    Err(e) => return Err(e),
                }
            }
            Err(LimboError::Busy)
        };
        for round in 0..400i64 {
            let survivor_pk = round % 120;
            let survivor_u = 700 + survivor_pk;

            conn1.execute("BEGIN CONCURRENT").unwrap();
            if exec_retry(
                &conn1,
                &format!(
                    "UPDATE t SET u = {} WHERE pk = {survivor_pk}",
                    90000 + round
                ),
            )
            .is_err()
                || exec_retry(&conn1, "SAVEPOINT sp").is_err()
                || exec_retry(
                    &conn1,
                    &format!(
                        "UPDATE t SET u = {} WHERE pk = {survivor_pk}",
                        91000 + round
                    ),
                )
                .is_err()
                || exec_retry(&conn1, "ROLLBACK TO sp").is_err()
            {
                let _ = conn1.execute("ROLLBACK");
                continue;
            }

            conn2.execute("BEGIN CONCURRENT").unwrap();
            let mut update_ok = false;
            for _ in 0..1000 {
                match conn2.execute(format!(
                    "UPDATE t SET u = {survivor_u} WHERE pk = {survivor_pk}"
                )) {
                    Ok(_) => {
                        update_ok = true;
                        break;
                    }
                    Err(LimboError::Busy) => std::thread::yield_now(),
                    Err(LimboError::WriteWriteConflict) | Err(LimboError::TxError(_)) => break,
                    Err(e) => panic!("conn2 update failed: {e:?}"),
                }
            }
            if !update_ok {
                let _ = conn2.execute("ROLLBACK");
                let _ = conn1.execute("COMMIT");
                let _ = conn1.execute("ROLLBACK");
                continue;
            }
            for _ in 0..1000 {
                match conn2.execute("COMMIT") {
                    Ok(_) => break,
                    Err(LimboError::Busy) => std::thread::yield_now(),
                    Err(_) => {
                        let _ = conn2.execute("ROLLBACK");
                        break;
                    }
                }
            }
            let _ = conn1.execute("COMMIT");
            let _ = conn1.execute("ROLLBACK");
        }
    });

    writer_handle.join().unwrap();
    stop.store(true, Ordering::Release);
    reader_handle.join().unwrap();

    let mut swept = false;
    for _ in 0..1000 {
        match conn.execute("PRAGMA wal_checkpoint(TRUNCATE)") {
            Ok(_) => {
                swept = true;
                break;
            }
            Err(LimboError::Busy) => std::thread::yield_now(),
            Err(e) => panic!("final checkpoint failed: {e:?}"),
        }
    }
    assert!(swept, "final checkpoint never succeeded");
    let ic2 = read_retry(&conn, "PRAGMA integrity_check").expect("final integrity_check");
    assert_eq!(ic2.len(), 1);
    assert_eq!(
        &ic2[0][0].to_string(),
        "ok",
        "post-checkpoint integrity_check: {ic2:?}"
    );
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

fn assert_integrity_ok(conn: &Arc<Connection>) {
    let rows = get_rows(conn, "PRAGMA integrity_check");
    assert_eq!(rows.len(), 1, "integrity_check rows: {rows:?}");
    assert_eq!(rows[0][0].to_string(), "ok");
}

fn setup_mvcc_checkpointed_indexed_table(conn: &Arc<Connection>, with_b_index: bool) {
    conn.execute("PRAGMA mvcc_gc_threshold = 1").unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, a, b)")
        .unwrap();
    conn.execute("CREATE INDEX t_a ON t(a)").unwrap();
    if with_b_index {
        conn.execute("CREATE INDEX t_b ON t(b)").unwrap();
    }
    conn.execute(
        "INSERT INTO t VALUES
            (1,10,100),(2,20,200),(3,30,300),(4,40,400),(5,50,500)",
    )
    .unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
}

fn assert_checkpointed_replace_delete_result(conn: &Arc<Connection>) {
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    assert_integrity_ok(conn);

    let rows = get_rows(conn, "SELECT id,a,b FROM t ORDER BY id");
    assert_eq!(rows.len(), 4);
    for (idx, row) in rows.iter().enumerate() {
        let id = (idx as i64) + 1;
        assert_eq!(
            row,
            &vec![
                Value::from_i64(id),
                Value::from_i64(id * 10),
                Value::from_i64(id * 100)
            ]
        );
    }
}

/// Creates and drops a checkpointed table so the following schema changes reuse
/// physical roots and expose a root mapping left behind by a failed checkpoint.
fn prepare_recycled_root_pages_for_failed_checkpoint(conn: &Arc<Connection>) {
    conn.execute("PRAGMA mvcc_checkpoint_threshold = -1")
        .unwrap();
    conn.execute("CREATE TABLE old(id INTEGER PRIMARY KEY, x TEXT, b BLOB)")
        .unwrap();
    conn.execute(
        "INSERT INTO old
         SELECT value, 'old' || value, zeroblob(1000)
           FROM generate_series(1, 50)",
    )
    .unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    conn.execute("DROP TABLE old").unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
}

fn expect_database_full_checkpoint(result: turso_core::Result<()>) {
    let error = result.expect_err("checkpoint must fail when max_page_count is exhausted");
    assert!(
        error.to_string().contains("database or disk is full"),
        "checkpoint must report that the database is full, got {error:?}"
    );
}

fn assert_table_row(conn: &Arc<Connection>, table: &str, id: i64, expected_x: Option<&str>) {
    let rows = get_rows(conn, &format!("SELECT id, x FROM {table} WHERE id = {id}"));
    match expected_x {
        Some(expected_x) => {
            assert_eq!(
                rows.len(),
                1,
                "expected {table} to contain id {id}, got {rows:?}"
            );
            assert_eq!(rows[0][0].as_int().unwrap(), id);
            assert_eq!(rows[0][1].to_string(), expected_x);
        }
        None => assert!(
            rows.is_empty(),
            "expected {table} not to contain id {id}, got {rows:?}"
        ),
    }
}

fn assert_table_counts(conn: &Arc<Connection>, expected_t: i64, expected_u: i64) {
    let rows = get_rows(
        conn,
        "SELECT (SELECT count(*) FROM t), (SELECT count(*) FROM u)",
    );
    assert_eq!(rows.len(), 1, "count query returned {rows:?}");
    assert_eq!(rows[0][0].as_int().unwrap(), expected_t, "t row count");
    assert_eq!(rows[0][1].as_int().unwrap(), expected_u, "u row count");
}

type Store = MvStore<turso_core::mvcc::MvccClock, turso_core::alloc::DynAllocator>;

const TRUNCATE: CheckpointMode = CheckpointMode::Truncate {
    upper_bound_inclusive: None,
};
const PASSIVE: CheckpointMode = CheckpointMode::Passive {
    upper_bound_inclusive: None,
};

/// Which rows the checkpoint under test walks to find what to write.
#[derive(Debug, Clone, Copy)]
enum Walk {
    /// Every row in the store. The first checkpoint after an open does this.
    EveryRow,
    /// Only the rows that commits marked as changed. Later checkpoints do this.
    ChangedRows,
}

/// Completes one checkpoint first when the test wants the next one to walk changed rows only.
fn prepare_walk(db: &TempDatabase, conn: &Arc<Connection>, walk: Walk) {
    match walk {
        Walk::EveryRow => {}
        Walk::ChangedRows => SteppedCheckpoint::start(db, conn, TRUNCATE).finish(),
    }
}

/// What one step of a checkpoint did.
#[derive(Debug, PartialEq, Eq)]
enum Step {
    /// Moved on without waiting.
    Continued,
    /// Waited for I/O.
    WaitedForIo,
    /// Gave control back to the caller although no I/O was pending.
    Yielded,
    Done,
}

/// A checkpoint that the test steps by hand.
struct SteppedCheckpoint {
    machine: CheckpointStateMachine<turso_core::mvcc::MvccClock, turso_core::alloc::DynAllocator>,
    io: Arc<dyn IO + Send>,
}

impl SteppedCheckpoint {
    fn start(db: &TempDatabase, conn: &Arc<Connection>, mode: CheckpointMode) -> Self {
        let machine = CheckpointStateMachine::new(
            conn.get_pager(),
            store(conn),
            conn.clone(),
            true,
            conn.get_sync_mode(),
            turso_core::MAIN_DB_ID,
            mode,
        );
        Self {
            machine,
            io: db.io.clone(),
        }
    }

    fn state(&self) -> CheckpointState {
        self.machine.state()
    }

    fn step(&mut self) -> Step {
        match self.machine.step(&()).unwrap() {
            TransitionResult::Continue => Step::Continued,
            TransitionResult::Done(_) => Step::Done,
            TransitionResult::Io(io) => {
                let yielded = io.is_explicit_yield();
                io.wait(self.io.as_ref()).unwrap();
                if yielded {
                    Step::Yielded
                } else {
                    Step::WaitedForIo
                }
            }
        }
    }

    /// Steps until the checkpoint is about to do the work of `state`.
    fn step_until(&mut self, state: CheckpointState) {
        self.step_until_state(|current| current == state);
    }

    fn step_until_state(&mut self, reached: impl Fn(CheckpointState) -> bool) {
        while !reached(self.state()) {
            assert_ne!(
                self.step(),
                Step::Done,
                "the checkpoint finished before it reached the expected state"
            );
        }
    }

    /// Steps until the checkpoint has done all the work of `state`.
    fn step_while(&mut self, state: CheckpointState) {
        self.step_until_state(|current| current != state);
    }

    /// Steps until the checkpoint has chosen every table and index row it writes.
    fn step_until_collected(&mut self) {
        self.step_until(CheckpointState::CollectIndexRows);
        self.step_while(CheckpointState::CollectIndexRows);
    }

    fn finish(mut self) {
        while self.step() != Step::Done {}
    }

    /// Rowids of the rows of `table` the checkpoint has chosen to write, in collection order.
    fn collected_table_rowids(&self, table: MVTableId) -> Vec<i64> {
        self.machine
            .collected_table_rows()
            .filter(|(row_id, _)| row_id.table_id == table)
            .map(|(row_id, _)| row_id.row_id.to_int_or_panic())
            .collect()
    }

    fn collected_index_row_count(&self, index: MVTableId) -> usize {
        self.machine
            .collected_index_rows()
            .filter(|(row_id, _)| row_id.table_id == index)
            .count()
    }
}

fn passive_checkpoint_database(name: &str) -> TempDatabase {
    TempDatabase::builder()
        .with_db_name(name)
        .with_mvcc(true)
        .with_opts(DatabaseOpts::new().with_experimental_mvcc_passive_checkpoint(true))
        .build()
}

/// The tests start every checkpoint themselves, so a commit must not start one.
fn connect_without_auto_checkpoint(db: &TempDatabase) -> Arc<Connection> {
    let conn = db.connect_limbo();
    conn.disable_mvcc_auto_checkpoint().unwrap();
    conn
}

/// Inserts `count` rows with rowids from `first_rowid` up, in one transaction.
///
/// The text column sorts like the rowid, so in an index on it a row with a higher rowid
/// comes after every row with a lower one.
fn insert_rows(conn: &Arc<Connection>, table: &str, first_rowid: usize, count: usize) {
    let last_rowid = first_rowid + count - 1;
    conn.execute(format!(
        "INSERT INTO {table} SELECT value, printf('v%08d', value) \
         FROM generate_series({first_rowid}, {last_rowid})"
    ))
    .unwrap();
}

fn store(conn: &Arc<Connection>) -> Arc<Store> {
    conn.mv_store()
        .as_ref()
        .expect("the database uses MVCC")
        .clone()
}

/// The id the MVCC store uses for the table or index called `name`.
fn btree_id(conn: &Arc<Connection>, name: &str) -> MVTableId {
    let rows: Vec<(i64,)> = conn.exec_rows(&format!(
        "SELECT rootpage FROM sqlite_schema WHERE name = '{name}'"
    ));
    store(conn).get_table_id_from_root_page(rows[0].0)
}

/// Whether the row is marked as changed since the last checkpoint that wrote it.
fn is_marked(store: &Store, table: MVTableId, rowid: i64) -> bool {
    store
        .checkpoint_dirty_table_keys
        .contains_key(&RowID::new(table, RowKey::Int(rowid)))
}

fn table_version_count(store: &Store, table: MVTableId) -> usize {
    store
        .rows
        .iter()
        .filter(|entry| entry.key().table_id == table)
        .map(|entry| entry.value().read().len())
        .sum()
}

fn index_version_count(store: &Store, index: MVTableId) -> usize {
    store.index_rows.get(&index).map_or(0, |rows| {
        rows.value()
            .iter()
            .map(|entry| entry.value().read().len())
            .sum()
    })
}
