//! Tests of the MVCC checkpoint that step it by hand.
//!
//! Each test commits rows with SQL, starts a checkpoint and steps it up to the point of
//! interest.

use crate::common::{ExecRows, TempDatabase};
use std::sync::Arc;
use turso_core::mvcc::database::checkpoint::COLLECT_PREEMPTION_THRESHOLD;
use turso_core::mvcc::database::{
    CheckpointState, CheckpointStateMachine, MVTableId, MvStore, SQLITE_SCHEMA_MVCC_TABLE_ID,
};
use turso_core::state_machine::{StateTransition, TransitionResult};
use turso_core::{CheckpointMode, Connection, IO};

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

type Store = MvStore<turso_core::mvcc::MvccClock, turso_core::alloc::DynAllocator>;

const TRUNCATE: CheckpointMode = CheckpointMode::Truncate {
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
