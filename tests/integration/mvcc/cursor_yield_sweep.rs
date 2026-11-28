//! A sweep over MVCC cursor yields. Each cell of the sweep runs one cursor operation, makes
//! the cursor yield at one occurrence of one `CursorYieldPoint`, makes one change to the
//! database while the cursor waits, and then checks the result of the operation against a
//! model of snapshot isolation.
//!
//! The data is split between the B-tree (checkpointed rows) and the MvStore (rows that are
//! not checkpointed, and writes of the transaction that runs the operation). Both sides
//! have hidden rows: deleted rows, and rows that a newer version replaces.
//!
//! The oracle:
//! - A change of another transaction (a commit, a write that is not committed, a rollback,
//!   a checkpoint or GC) must not change the result. The result must be equal to the result
//!   of the same operation without a yield.
//! - A write of the transaction that runs the operation acts as if it came between two
//!   steps of the statement. The rows that the statement returned before the yield do not
//!   change. Each row after the yield shows the data after the write. A row that existed
//!   before the write and still exists after it must come back. A row that the write
//!   inserted, or moved to a new index key, can come back or not, because SQLite does not
//!   define this. The rows must come in strict cursor order, so a row never comes back two
//!   times.

use super::test_db::MvccTestDbNoConn;
use std::cmp::Ordering as CmpOrdering;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use turso_core::mvcc::cursor::CursorYieldPoint;
use turso_core::mvcc::yield_hooks::YieldPointMarker;
use turso_core::mvcc::yield_points::{YieldInjector, YieldPoint};
use turso_core::{Connection, LimboError, StepResult};

/// The variants of `CursorYieldPoint`, used only to give names to ordinals. The sweep walks
/// all ordinals up to `POINT_COUNT`, so it also covers a point that this list does not name.
const NAMED_POINTS: &[CursorYieldPoint] = &[
    CursorYieldPoint::NextStart,
    CursorYieldPoint::SeekStart,
    CursorYieldPoint::SeekBtreeProgress,
    CursorYieldPoint::ExistsBtreeFallback,
    CursorYieldPoint::CountProgress,
    CursorYieldPoint::AdvanceBtreeForwardProgress,
    CursorYieldPoint::AdvanceBtreeBackwardProgress,
];

fn point_of(ordinal: u8) -> YieldPoint {
    let family = CursorYieldPoint::NextStart.point().family;
    YieldPoint {
        ordinal,
        point_count: CursorYieldPoint::POINT_COUNT,
        family,
    }
}

fn point_name(ordinal: u8) -> String {
    NAMED_POINTS
        .iter()
        .find(|p| p.ordinal() == ordinal)
        .map(|p| format!("{p:?}"))
        .unwrap_or_else(|| format!("CursorYieldPoint#{ordinal}"))
}

/// The value that stands for a NULL in a returned row.
const NULL_VALUE: i64 = i64::MIN;

/// A row of table `t`: `(id, v, w)`. `w` is `None` when the operation does not read it.
type Row = (i64, i64, Option<i64>);

/// The rows that a transaction sees: `id -> (v, w)`.
type State = BTreeMap<i64, (i64, i64)>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Order {
    IdAsc,
    IdDesc,
    VAsc,
    VDesc,
}

impl Order {
    fn key(self, row: &Row) -> (i64, i64) {
        match self {
            Order::IdAsc | Order::IdDesc => (row.0, 0),
            Order::VAsc | Order::VDesc => (row.1, row.0),
        }
    }

    fn dir(self) -> i64 {
        match self {
            Order::IdAsc | Order::VAsc => 1,
            Order::IdDesc | Order::VDesc => -1,
        }
    }

    /// Compares two keys in the order of the scan.
    fn cmp(self, a: (i64, i64), b: (i64, i64)) -> CmpOrdering {
        if self.dir() > 0 {
            a.cmp(&b)
        } else {
            b.cmp(&a)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OpKind {
    /// A SELECT that returns `id, v` and, if `with_w`, `w`, in `order`.
    Rows { order: Order, with_w: bool },
    /// `SELECT count(*)`.
    Count,
    /// A write statement. The check compares the rows of the table after it.
    Write,
}

#[derive(Debug, Clone, Copy)]
struct Op {
    name: &'static str,
    sql: &'static str,
    kind: OpKind,
    /// The rows that the operation reads or writes.
    filter: fn(i64, i64) -> bool,
    /// A free `(id, v)` past the last row of the operation, in its range. A change inserts
    /// it when the operation has no next row.
    hint: (i64, i64),
}

impl Op {
    fn order(&self) -> Order {
        match self.kind {
            OpKind::Rows { order, .. } => order,
            OpKind::Count | OpKind::Write => Order::IdAsc,
        }
    }

    fn with_w(&self) -> bool {
        match self.kind {
            OpKind::Rows { with_w, .. } => with_w,
            OpKind::Count | OpKind::Write => true,
        }
    }

    /// The rows of `state` that the operation returns, in the order of the operation.
    fn expected(&self, state: &State) -> Vec<Row> {
        let order = self.order();
        let mut rows: Vec<Row> = state
            .iter()
            .filter(|(id, (v, _))| (self.filter)(**id, *v))
            .map(|(id, (v, w))| (*id, *v, self.with_w().then_some(*w)))
            .collect();
        rows.sort_by(|a, b| order.cmp(order.key(a), order.key(b)));
        rows
    }
}

fn all(_: i64, _: i64) -> bool {
    true
}

const OPS: &[Op] = &[
    Op {
        name: "scan_fwd",
        sql: "SELECT id, v, w FROM t",
        kind: OpKind::Rows {
            order: Order::IdAsc,
            with_w: true,
        },
        filter: all,
        hint: (301, 10),
    },
    Op {
        name: "scan_bwd",
        sql: "SELECT id, v, w FROM t ORDER BY id DESC",
        kind: OpKind::Rows {
            order: Order::IdDesc,
            with_w: true,
        },
        filter: all,
        hint: (1, 0),
    },
    Op {
        name: "rowid_eq_btree",
        sql: "SELECT id, v, w FROM t WHERE id = 20",
        kind: OpKind::Rows {
            order: Order::IdAsc,
            with_w: true,
        },
        filter: |id, _| id == 20,
        hint: (20, 0),
    },
    Op {
        name: "rowid_eq_shadowed",
        sql: "SELECT id, v, w FROM t WHERE id = 110",
        kind: OpKind::Rows {
            order: Order::IdAsc,
            with_w: true,
        },
        filter: |id, _| id == 110,
        hint: (110, 3),
    },
    Op {
        name: "rowid_eq_mvstore",
        sql: "SELECT id, v, w FROM t WHERE id = 85",
        kind: OpKind::Rows {
            order: Order::IdAsc,
            with_w: true,
        },
        filter: |id, _| id == 85,
        hint: (85, 2),
    },
    Op {
        name: "rowid_eq_deleted",
        sql: "SELECT id, v, w FROM t WHERE id = 50",
        kind: OpKind::Rows {
            order: Order::IdAsc,
            with_w: true,
        },
        filter: |id, _| id == 50,
        hint: (50, 1),
    },
    Op {
        name: "rowid_eq_own_deleted",
        sql: "SELECT id, v, w FROM t WHERE id = 140",
        kind: OpKind::Rows {
            order: Order::IdAsc,
            with_w: true,
        },
        filter: |id, _| id == 140,
        hint: (140, 4),
    },
    Op {
        name: "rowid_gt",
        sql: "SELECT id, v, w FROM t WHERE id > 100",
        kind: OpKind::Rows {
            order: Order::IdAsc,
            with_w: true,
        },
        filter: |id, _| id > 100,
        hint: (301, 10),
    },
    Op {
        name: "rowid_ge_lt",
        sql: "SELECT id, v, w FROM t WHERE id >= 50 AND id < 160",
        kind: OpKind::Rows {
            order: Order::IdAsc,
            with_w: true,
        },
        filter: |id, _| (50..160).contains(&id),
        hint: (159, 5),
    },
    Op {
        name: "rowid_lt_desc",
        sql: "SELECT id, v, w FROM t WHERE id < 100 ORDER BY id DESC",
        kind: OpKind::Rows {
            order: Order::IdDesc,
            with_w: true,
        },
        filter: |id, _| id < 100,
        hint: (1, 0),
    },
    Op {
        name: "rowid_le_gt_desc",
        sql: "SELECT id, v, w FROM t WHERE id <= 160 AND id > 40 ORDER BY id DESC",
        kind: OpKind::Rows {
            order: Order::IdDesc,
            with_w: true,
        },
        filter: |id, _| id <= 160 && id > 40,
        hint: (41, 1),
    },
    Op {
        name: "index_eq",
        sql: "SELECT id, v, w FROM t INDEXED BY tv WHERE v = 3",
        kind: OpKind::Rows {
            order: Order::VAsc,
            with_w: true,
        },
        filter: |_, v| v == 3,
        hint: (299, 3),
    },
    Op {
        name: "index_eq_covering",
        sql: "SELECT id, v FROM t INDEXED BY tv WHERE v = 1",
        kind: OpKind::Rows {
            order: Order::VAsc,
            with_w: false,
        },
        filter: |_, v| v == 1,
        hint: (299, 1),
    },
    Op {
        name: "index_gt",
        sql: "SELECT id, v, w FROM t INDEXED BY tv WHERE v > 2 ORDER BY v",
        kind: OpKind::Rows {
            order: Order::VAsc,
            with_w: true,
        },
        filter: |_, v| v > 2,
        hint: (299, 9),
    },
    Op {
        name: "index_lt_desc",
        sql: "SELECT id, v, w FROM t INDEXED BY tv WHERE v < 5 ORDER BY v DESC",
        kind: OpKind::Rows {
            order: Order::VDesc,
            with_w: true,
        },
        filter: |_, v| v < 5,
        hint: (1, -1),
    },
    Op {
        name: "index_scan_covering_fwd",
        sql: "SELECT id, v FROM t INDEXED BY tv WHERE v >= 0 ORDER BY v",
        kind: OpKind::Rows {
            order: Order::VAsc,
            with_w: false,
        },
        filter: |_, v| v >= 0,
        hint: (299, 9),
    },
    Op {
        name: "index_scan_covering_bwd",
        sql: "SELECT id, v FROM t INDEXED BY tv WHERE v >= 0 ORDER BY v DESC",
        kind: OpKind::Rows {
            order: Order::VDesc,
            with_w: false,
        },
        filter: |_, v| v >= 0,
        hint: (1, 0),
    },
    Op {
        name: "count",
        sql: "SELECT count(*) FROM t",
        kind: OpKind::Count,
        filter: all,
        hint: (301, 10),
    },
    Op {
        name: "update_rowid",
        sql: "UPDATE t SET w = w + 1000 WHERE id = 20",
        kind: OpKind::Write,
        filter: |id, _| id == 20,
        hint: (20, 0),
    },
    Op {
        name: "delete_rowid",
        sql: "DELETE FROM t WHERE id = 40",
        kind: OpKind::Write,
        filter: |id, _| id == 40,
        hint: (40, 1),
    },
    Op {
        name: "insert_or_ignore",
        sql: "INSERT OR IGNORE INTO t VALUES (20, 0, 777)",
        kind: OpKind::Write,
        filter: |id, _| id == 20,
        hint: (20, 0),
    },
    Op {
        name: "update_index_eq",
        sql: "UPDATE t SET w = w + 1000 WHERE v = 3",
        kind: OpKind::Write,
        filter: |_, v| v == 3,
        hint: (299, 3),
    },
    Op {
        name: "update_all",
        sql: "UPDATE t SET w = w + 1000",
        kind: OpKind::Write,
        filter: all,
        hint: (301, 10),
    },
];

/// A row relative to the position of the cursor at the yield.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Target {
    /// The row before the last row that the statement returned.
    Before,
    /// The last row that the statement returned.
    At,
    /// The row that the statement returns next.
    Next,
    /// The row after the next row.
    After,
}

const TARGETS: [Target; 4] = [Target::Before, Target::At, Target::Next, Target::After];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Change {
    /// The same transaction inserts a row between the last returned row and the next row.
    OwnInsertAhead,
    /// The same transaction inserts a row just before the last returned row.
    OwnInsertBehind,
    /// The same transaction updates `w` of a row.
    OwnUpdate(Target),
    /// The same transaction deletes a row.
    OwnDelete(Target),
    /// The same transaction updates `v` of a row, which moves its index entry.
    OwnMoveKey(Target),
    /// Another transaction writes rows near the cursor and commits.
    OtherCommit,
    /// Another transaction writes rows near the cursor and does not commit.
    OtherPendingWrite,
    /// Another transaction wrote rows near the cursor before the statement started, and
    /// rolls back at the yield.
    OtherRollback,
    /// Another transaction commits, and then GC runs.
    OtherCommitGc,
    /// GC runs.
    Gc,
    /// Another connection runs a passive checkpoint.
    PassiveCheckpoint,
    /// Another transaction commits, and then another connection runs a passive checkpoint.
    OtherCommitPassiveCheckpoint,
    /// Another connection runs a blocking checkpoint. It is expected to fail with Busy.
    TruncateCheckpoint,
}

impl Change {
    fn all() -> Vec<Change> {
        let mut changes = vec![Change::OwnInsertAhead, Change::OwnInsertBehind];
        for target in TARGETS {
            changes.push(Change::OwnUpdate(target));
            changes.push(Change::OwnDelete(target));
            changes.push(Change::OwnMoveKey(target));
        }
        changes.extend([
            Change::OtherCommit,
            Change::OtherPendingWrite,
            Change::OtherRollback,
            Change::OtherCommitGc,
            Change::Gc,
            Change::PassiveCheckpoint,
            Change::OtherCommitPassiveCheckpoint,
            Change::TruncateCheckpoint,
        ]);
        changes
    }

    fn is_own(self) -> bool {
        matches!(
            self,
            Change::OwnInsertAhead
                | Change::OwnInsertBehind
                | Change::OwnUpdate(_)
                | Change::OwnDelete(_)
                | Change::OwnMoveKey(_)
        )
    }

    fn passive_db(self) -> bool {
        matches!(
            self,
            Change::PassiveCheckpoint | Change::OtherCommitPassiveCheckpoint
        )
    }
}

/// The rows near the position of the cursor at the yield.
struct Targets {
    before: Option<Row>,
    at: Option<Row>,
    next: Option<Row>,
    after: Option<Row>,
    /// A free `(id, v)` between `at` and `next`, or past `at` if there is no next row.
    insert_ahead: (i64, i64),
    /// A free `(id, v)` just before `at`.
    insert_behind: Option<(i64, i64)>,
}

impl Targets {
    fn new(op: &Op, expected: &[Row], pos: usize) -> Self {
        let dir = op.order().dir();
        let get = |i: Option<usize>| i.and_then(|i| expected.get(i)).copied();
        let at = get(pos.checked_sub(1));
        let next = get(Some(pos));
        Targets {
            before: get(pos.checked_sub(2)),
            at,
            next,
            after: get(Some(pos + 1)),
            // Live ids are multiples of 5, so `id - dir` is free.
            insert_ahead: next.map_or(op.hint, |n| (n.0 - dir, n.1)),
            insert_behind: at.map(|a| (a.0 - dir, a.1)),
        }
    }

    fn get(&self, target: Target) -> Option<Row> {
        match target {
            Target::Before => self.before,
            Target::At => self.at,
            Target::Next => self.next,
            Target::After => self.after,
        }
    }

    /// The SQL of a change by the same transaction, or `None` if the target does not exist.
    fn own_sql(&self, change: Change) -> Option<String> {
        Some(match change {
            Change::OwnInsertAhead => {
                let (id, v) = self.insert_ahead;
                format!("INSERT INTO t VALUES ({id}, {v}, 900)")
            }
            Change::OwnInsertBehind => {
                let (id, v) = self.insert_behind?;
                format!("INSERT INTO t VALUES ({id}, {v}, 901)")
            }
            Change::OwnUpdate(t) => {
                format!("UPDATE t SET w = w + 1 WHERE id = {}", self.get(t)?.0)
            }
            Change::OwnDelete(t) => format!("DELETE FROM t WHERE id = {}", self.get(t)?.0),
            Change::OwnMoveKey(t) => {
                format!("UPDATE t SET v = v + 1 WHERE id = {}", self.get(t)?.0)
            }
            _ => unreachable!("{change:?} is not a change of the same transaction"),
        })
    }

    /// The writes of another transaction near the cursor.
    fn other_sql(&self) -> Vec<String> {
        let mut sql = Vec::new();
        let (id, v) = self.insert_ahead;
        sql.push(format!("INSERT INTO t VALUES ({id}, {v}, 500)"));
        if let Some(next) = self.next {
            sql.push(format!(
                "UPDATE t SET w = w + 500, v = v + 1 WHERE id = {}",
                next.0
            ));
        }
        if let Some(row) = self.after.or(self.at) {
            sql.push(format!("DELETE FROM t WHERE id = {}", row.0));
        }
        sql
    }
}

/// Counts the occurrences of each cursor yield point, and yields at one occurrence of one
/// point. It records how many rows the statement returned at each occurrence.
#[derive(Debug)]
struct SweepInjector {
    target: Option<(u8, usize)>,
    rows: AtomicUsize,
    fired: AtomicBool,
    /// For each ordinal, the number of returned rows at each occurrence.
    occurrences: Mutex<HashMap<u8, Vec<usize>>>,
}

impl SweepInjector {
    fn new(target: Option<(u8, usize)>) -> Arc<Self> {
        Arc::new(Self {
            target,
            rows: AtomicUsize::new(0),
            fired: AtomicBool::new(false),
            occurrences: Mutex::new(HashMap::new()),
        })
    }
}

impl YieldInjector for SweepInjector {
    fn should_yield(&self, _instance_id: u64, _selection_key: u64, point: YieldPoint) -> bool {
        if point.family != point_of(0).family || self.fired.load(Ordering::SeqCst) {
            return false;
        }
        let mut occurrences = self.occurrences.lock().unwrap();
        let seen = occurrences.entry(point.ordinal).or_default();
        let index = seen.len();
        seen.push(self.rows.load(Ordering::SeqCst));
        if self.target == Some((point.ordinal, index)) {
            self.fired.store(true, Ordering::SeqCst);
            return true;
        }
        false
    }
}

/// The SQL that creates the data. The B-tree holds ids 10..=200 (step 10). The MvStore
/// holds committed rows that are not checkpointed: new rows, new versions of B-tree rows,
/// deleted B-tree rows at the edge and in the middle, a row that was inserted and deleted,
/// a row with two newer versions, and a moved index key.
const LAYOUT: &[&str] = &[
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT, w INT)",
    "CREATE INDEX tv ON t(v)",
    "INSERT INTO t SELECT value * 10, value * 10 / 30, 0 FROM generate_series(1, 20)",
    "PRAGMA wal_checkpoint(TRUNCATE)",
    "INSERT INTO t VALUES (15, 0, 0), (85, 2, 0), (175, 5, 0), (215, 7, 0)",
    "UPDATE t SET w = 1 WHERE id IN (30, 110)",
    "UPDATE t SET w = 1 WHERE id = 160",
    "UPDATE t SET w = 2 WHERE id = 160",
    "DELETE FROM t WHERE id IN (10, 50)",
    "INSERT INTO t VALUES (95, 3, 0)",
    "DELETE FROM t WHERE id = 95",
    "UPDATE t SET v = 6 WHERE id = 70",
];

/// Writes of the transaction that runs the operation, made before the operation.
const OWN_PENDING: &[&str] = &[
    "BEGIN CONCURRENT",
    "UPDATE t SET w = 3 WHERE id = 60",
    "INSERT INTO t VALUES (105, 3, 0)",
    "DELETE FROM t WHERE id = 140",
    "DELETE FROM t WHERE id = 175",
];

struct Fixture {
    db: MvccTestDbNoConn,
    conn: Arc<Connection>,
}

impl Fixture {
    fn new(passive: bool) -> Self {
        let db = if passive {
            MvccTestDbNoConn::new_with_random_db_passive()
        } else {
            MvccTestDbNoConn::new_with_random_db()
        };
        let conn = db.connect();
        for sql in LAYOUT.iter().chain(OWN_PENDING) {
            conn.execute(sql).unwrap();
        }
        Fixture { db, conn }
    }
}

fn read_state(conn: &Arc<Connection>) -> State {
    let mut stmt = conn.prepare("SELECT id, v, w FROM t ORDER BY id").unwrap();
    let mut state = State::new();
    stmt.run_with_row_callback(|row| {
        let int = |i| row.get_value(i).as_int().unwrap();
        state.insert(int(0), (int(1), int(2)));
        Ok(())
    })
    .unwrap();
    state
}

/// The result of a statement: the rows, and the number of rows before the yield.
struct Outcome {
    rows: Vec<Vec<i64>>,
    rows_at_yield: Option<usize>,
}

/// Runs `sql` to the end. At the first yield it calls `on_yield`.
fn drive(
    conn: &Arc<Connection>,
    sql: &str,
    injector: Option<&Arc<SweepInjector>>,
    on_yield: &mut dyn FnMut() -> Result<(), String>,
) -> Result<Outcome, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| format!("prepare: {e}"))?;
    let mut outcome = Outcome {
        rows: Vec::new(),
        rows_at_yield: None,
    };
    loop {
        match stmt.step().map_err(|e| format!("step: {e:?}"))? {
            StepResult::IO => stmt._io().step().map_err(|e| format!("io: {e}"))?,
            StepResult::Yield => {
                if outcome.rows_at_yield.is_none() {
                    outcome.rows_at_yield = Some(outcome.rows.len());
                    on_yield()?;
                }
            }
            StepResult::Row => {
                let row = stmt.row().unwrap();
                outcome.rows.push(
                    row.get_values()
                        // A NULL comes back as `NULL_VALUE`, which no row of the model has.
                        .map(|v| v.as_int().unwrap_or(NULL_VALUE))
                        .collect(),
                );
                if let Some(injector) = injector {
                    injector.rows.store(outcome.rows.len(), Ordering::SeqCst);
                }
            }
            StepResult::Done => break,
            other => return Err(format!("unexpected step result {other:?}")),
        }
    }
    Ok(outcome)
}

fn to_row(op: &Op, values: &[i64]) -> Row {
    (values[0], values[1], op.with_w().then(|| values[2]))
}

/// For each ordinal, the number of returned rows at each occurrence of the point.
fn probe(op: &Op) -> HashMap<u8, Vec<usize>> {
    let fixture = Fixture::new(false);
    let injector = SweepInjector::new(None);
    fixture.conn.set_yield_injector(Some(injector.clone()));
    drive(&fixture.conn, op.sql, Some(&injector), &mut || Ok(()))
        .unwrap_or_else(|e| panic!("{}: {e}", op.name));
    fixture.conn.set_yield_injector(None);
    let occurrences = injector.occurrences.lock().unwrap().clone();
    occurrences
}

/// The occurrences of a point that the sweep yields at.
fn picked_occurrences(count: usize) -> BTreeSet<usize> {
    if count <= 6 {
        return (0..count).collect();
    }
    [0, 1, 2, count / 2, count - 2, count - 1]
        .into_iter()
        .collect()
}

#[derive(Debug, Clone)]
struct Cell {
    op: Op,
    ordinal: u8,
    occurrence: usize,
    rows_at_yield: usize,
    change: Change,
}

impl Cell {
    fn label(&self) -> String {
        format!(
            "{} x {} #{} (after {} rows) x {:?}",
            point_name(self.ordinal),
            self.op.name,
            self.occurrence,
            self.rows_at_yield,
            self.change
        )
    }
}

enum CellResult {
    Pass,
    /// The change does not apply here, for example the target row does not exist.
    NotApplicable,
    /// The point did not fire at this occurrence.
    NoYield,
    Fail(String),
}

/// Runs `sql` statements of the other transaction, and ignores the errors: a write-write
/// conflict with the transaction under test is a valid outcome.
fn run_other(conn: &Arc<Connection>, sql: &[String]) {
    for sql in sql {
        let _ = conn.execute(sql);
    }
}

fn run_gc(fixture: &Fixture) {
    let store = fixture.db.get_mvcc_store();
    store.gc_incremental(usize::MAX);
    store.drop_unused_row_versions();
}

/// The rows of the table after `first` and then `second` run on a new fixture, or `None`
/// if a statement fails.
fn serial_state(first: &str, second: &str) -> Option<State> {
    let fixture = Fixture::new(false);
    fixture.conn.execute(first).ok()?;
    fixture.conn.execute(second).ok()?;
    Some(read_state(&fixture.conn))
}

type SerialCache = Mutex<HashMap<(String, String), Option<State>>>;

fn cached_serial_state(cache: &SerialCache, first: &str, second: &str) -> Option<State> {
    let key = (first.to_string(), second.to_string());
    if let Some(state) = cache.lock().unwrap().get(&key) {
        return state.clone();
    }
    let state = serial_state(first, second);
    cache.lock().unwrap().insert(key, state.clone());
    state
}

fn run_cell(cell: &Cell, serial_cache: &SerialCache) -> CellResult {
    let op = &cell.op;
    let fixture = Fixture::new(cell.change.passive_db());
    let conn = &fixture.conn;
    let s0 = read_state(conn);
    let expected = op.expected(&s0);
    let pos = match op.kind {
        OpKind::Rows { .. } => cell.rows_at_yield,
        OpKind::Count | OpKind::Write => cell.occurrence.min(expected.len()),
    };
    let targets = Targets::new(op, &expected, pos);

    let own_sql = if cell.change.is_own() {
        match targets.own_sql(cell.change) {
            Some(sql) => Some(sql),
            None => return CellResult::NotApplicable,
        }
    } else {
        None
    };
    let other = fixture.db.connect();
    let other_sql = targets.other_sql();
    if cell.change == Change::OtherRollback {
        other.execute("BEGIN CONCURRENT").unwrap();
        run_other(&other, &other_sql);
    }

    // The same operation without a yield must agree with the model.
    if let OpKind::Rows { .. } | OpKind::Count = op.kind {
        let plain = match drive(conn, op.sql, None, &mut || Ok(())) {
            Ok(outcome) => outcome.rows,
            Err(e) => return CellResult::Fail(format!("the run without a yield failed: {e}")),
        };
        let model_ok = match op.kind {
            OpKind::Count => plain == vec![vec![expected.len() as i64]],
            _ => plain.iter().map(|r| to_row(op, r)).collect::<Vec<_>>() == expected,
        };
        if !model_ok {
            return CellResult::Fail(format!(
                "the run without a yield does not agree with the model: {plain:?} vs {expected:?}"
            ));
        }
    }

    let injector = SweepInjector::new(Some((cell.ordinal, cell.occurrence)));
    conn.set_yield_injector(Some(injector.clone()));
    let mut change_error = None;
    // Turso does not let a write statement start while another write statement of the
    // connection is in progress. Then the write at the yield does not happen.
    let mut own_rejected = false;
    let mut on_yield = || -> Result<(), String> {
        match cell.change {
            Change::OwnInsertAhead
            | Change::OwnInsertBehind
            | Change::OwnUpdate(_)
            | Change::OwnDelete(_)
            | Change::OwnMoveKey(_) => {
                let sql = own_sql.as_deref().unwrap();
                match conn.execute(sql) {
                    Ok(_) => {}
                    Err(LimboError::StatementsInProgress(_)) if op.kind == OpKind::Write => {
                        own_rejected = true;
                    }
                    Err(e) => return Err(format!("the write `{sql}` at the yield failed: {e}")),
                }
            }
            Change::OtherCommit | Change::OtherCommitGc | Change::OtherCommitPassiveCheckpoint => {
                other.execute("BEGIN CONCURRENT").unwrap();
                run_other(&other, &other_sql);
                let _ = other.execute("COMMIT");
                match cell.change {
                    Change::OtherCommitGc => run_gc(&fixture),
                    Change::OtherCommitPassiveCheckpoint => {
                        change_error = other.execute("PRAGMA wal_checkpoint(PASSIVE)").err();
                    }
                    _ => {}
                }
            }
            Change::OtherPendingWrite => {
                other.execute("BEGIN CONCURRENT").unwrap();
                run_other(&other, &other_sql);
            }
            Change::OtherRollback => {
                let _ = other.execute("ROLLBACK");
            }
            Change::Gc => run_gc(&fixture),
            Change::PassiveCheckpoint => {
                change_error = other.execute("PRAGMA wal_checkpoint(PASSIVE)").err();
            }
            Change::TruncateCheckpoint => {
                change_error = other.execute("PRAGMA wal_checkpoint(TRUNCATE)").err();
            }
        }
        Ok(())
    };
    let outcome = drive(conn, op.sql, Some(&injector), &mut on_yield);
    conn.set_yield_injector(None);
    if let Some(e) = &change_error {
        // A checkpoint can fail with Busy while the transaction under test is open.
        if !matches!(e, LimboError::Busy | LimboError::BusySnapshot) {
            return CellResult::Fail(format!("the change failed: {e}"));
        }
    }
    let outcome = match outcome {
        Ok(outcome) => outcome,
        Err(e) => {
            if op.kind == OpKind::Write && !cell.change.is_own() && e.contains("Conflict") {
                return CellResult::Pass;
            }
            return CellResult::Fail(e);
        }
    };
    let Some(rows_at_yield) = outcome.rows_at_yield else {
        return CellResult::NoYield;
    };
    if op.kind != OpKind::Write && rows_at_yield != cell.rows_at_yield {
        return CellResult::Fail(format!(
            "the yield came after {rows_at_yield} rows, the probe said {}",
            cell.rows_at_yield
        ));
    }
    let s1 = read_state(conn);
    let _ = other.execute("ROLLBACK");

    let result = match op.kind {
        OpKind::Rows { .. } => {
            let actual: Vec<Row> = outcome.rows.iter().map(|r| to_row(op, r)).collect();
            check_rows(
                op,
                cell.change.is_own(),
                &expected,
                &s0,
                &s1,
                &actual,
                rows_at_yield,
            )
        }
        OpKind::Count => check_count(op, cell.change.is_own(), &s0, &s1, &outcome.rows),
        OpKind::Write => {
            if cell.change.is_own() && !own_rejected {
                let own_sql = own_sql.as_deref().unwrap();
                let a = cached_serial_state(serial_cache, own_sql, op.sql);
                let b = cached_serial_state(serial_cache, op.sql, own_sql);
                if Some(&s1) == a.as_ref() || Some(&s1) == b.as_ref() {
                    Ok(())
                } else {
                    Err(format!(
                        "the table after the statement matches no serial order: {s1:?}, \
                         change first {a:?}, statement first {b:?}"
                    ))
                }
            } else {
                let c = cached_serial_state(serial_cache, "SELECT 1", op.sql);
                if Some(&s1) == c.as_ref() {
                    Ok(())
                } else {
                    Err(format!(
                        "a change of another transaction changed the result: {s1:?} vs {c:?}"
                    ))
                }
            }
        }
    };
    match result {
        Ok(()) => CellResult::Pass,
        Err(e) => CellResult::Fail(e),
    }
}

fn check_rows(
    op: &Op,
    own: bool,
    expected: &[Row],
    s0: &State,
    s1: &State,
    actual: &[Row],
    rows_at_yield: usize,
) -> Result<(), String> {
    if !own {
        if actual != expected {
            return Err(format!(
                "a change of another transaction changed the result: {actual:?} vs {expected:?}"
            ));
        }
        if s0 != s1 {
            return Err("a change of another transaction changed the view".to_string());
        }
        return Ok(());
    }
    let order = op.order();
    let p = rows_at_yield;
    if actual.len() < p || actual[..p] != expected[..p] {
        return Err(format!(
            "the rows before the yield differ: {actual:?} vs {expected:?}"
        ));
    }
    for pair in actual.windows(2) {
        if order.cmp(order.key(&pair[0]), order.key(&pair[1])) != CmpOrdering::Less {
            return Err(format!(
                "rows out of order or repeated: {:?} then {:?} in {actual:?}",
                pair[0], pair[1]
            ));
        }
    }
    let last = p.checked_sub(1).map(|i| order.key(&expected[i]));
    let is_past =
        |row: &Row| last.is_none_or(|l| order.cmp(order.key(row), l) == CmpOrdering::Greater);
    let after: Vec<Row> = op.expected(s1).into_iter().filter(is_past).collect();
    let old_keys: BTreeSet<(i64, i64)> = expected.iter().map(|r| order.key(r)).collect();
    for row in &actual[p..] {
        if !after.contains(row) {
            let why = if expected.contains(row) {
                "a stale row, the write at the yield changed or deleted it"
            } else {
                "a row that the view does not have"
            };
            return Err(format!("{why}: {row:?} in {actual:?}, expected {after:?}"));
        }
    }
    for row in &after {
        if old_keys.contains(&order.key(row)) && !actual[p..].contains(row) {
            return Err(format!(
                "missing row {row:?}: {actual:?}, expected {after:?} after the yield"
            ));
        }
    }
    Ok(())
}

fn check_count(
    op: &Op,
    own: bool,
    s0: &State,
    s1: &State,
    rows: &[Vec<i64>],
) -> Result<(), String> {
    let [row] = rows else {
        return Err(format!("count returned {rows:?}"));
    };
    let count = row[0] as usize;
    // `count(*)` can walk the index, where a write that moves a key adds an entry ahead of
    // the cursor. So the bounds use index keys.
    let k0: BTreeSet<(i64, i64)> = op.expected(s0).iter().map(|r| (r.1, r.0)).collect();
    let k1: BTreeSet<(i64, i64)> = op.expected(s1).iter().map(|r| (r.1, r.0)).collect();
    let (low, high) = if own {
        (k0.intersection(&k1).count(), k0.union(&k1).count())
    } else {
        (k0.len(), k0.len())
    };
    if count < low || count > high {
        return Err(format!("count {count} is not in {low}..={high}"));
    }
    Ok(())
}

/// A cell that fails also on main, because of a bug that this test does not fix. The sweep
/// reports these cells, but they do not fail the test.
fn known_failure(cell: &Cell) -> Option<&'static str> {
    let seek_point = cell.ordinal == CursorYieldPoint::SeekStart.ordinal()
        || cell.ordinal == CursorYieldPoint::SeekBtreeProgress.ordinal();
    let writes_next_row = matches!(
        cell.change,
        Change::OwnUpdate(Target::Next)
            | Change::OwnDelete(Target::Next)
            | Change::OwnMoveKey(Target::Next)
    );
    if seek_point && writes_next_row {
        // FIXME: a seek that yields at SeekStart or SeekBtreeProgress does not see a write
        // of its own transaction made at the yield. When the write gives a B-tree row a new
        // MvStore version, the seek finds the B-tree row hidden and the stale MvStore head
        // empty, so it misses the row (`rowid_eq_btree`), or the table lookup of an index
        // scan returns NULL columns. Also, an index scan reads the index entry of the next
        // row before the table lookup yields. If the write deletes the row or moves its
        // index key, the scan still returns the row from the old entry, with NULL or stale
        // columns. Both fail on main too.
        return Some("a seek does not see a write of its own transaction made at the yield");
    }
    None
}

fn run_sweep(ops: &[Op]) -> BTreeMap<u8, usize> {
    let serial_cache = SerialCache::default();
    let mut fired: BTreeMap<u8, usize> = BTreeMap::new();
    let mut failures = Vec::new();
    let mut known = Vec::new();
    let mut ran = 0usize;
    for op in ops {
        let occurrences = probe(op);
        for ordinal in 0..CursorYieldPoint::POINT_COUNT {
            let Some(seen) = occurrences.get(&ordinal) else {
                continue;
            };
            for occurrence in picked_occurrences(seen.len()) {
                for change in Change::all() {
                    let cell = Cell {
                        op: *op,
                        ordinal,
                        occurrence,
                        rows_at_yield: seen[occurrence],
                        change,
                    };
                    let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
                        run_cell(&cell, &serial_cache)
                    }))
                    .unwrap_or_else(|panic| {
                        let msg = panic
                            .downcast_ref::<String>()
                            .cloned()
                            .or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()))
                            .unwrap_or_default();
                        CellResult::Fail(format!("panic: {msg}"))
                    });
                    match result {
                        CellResult::Pass => {
                            ran += 1;
                            *fired.entry(ordinal).or_default() += 1;
                        }
                        CellResult::Fail(e) => {
                            ran += 1;
                            *fired.entry(ordinal).or_default() += 1;
                            match known_failure(&cell) {
                                Some(bug) => known.push(format!("{}: {bug}", cell.label())),
                                None => failures.push(format!("{}: {e}", cell.label())),
                            }
                        }
                        CellResult::NotApplicable | CellResult::NoYield => {}
                    }
                }
            }
        }
    }
    println!(
        "sweep: {ran} cells, {} failed, {} known failures",
        failures.len(),
        known.len()
    );
    for line in &known {
        println!("KNOWN {line}");
    }
    for line in &failures {
        println!("FAIL {line}");
    }
    assert!(
        failures.is_empty(),
        "{} of {ran} cells failed, see the output",
        failures.len()
    );
    fired
}

fn ops_named(prefixes: &[&str]) -> Vec<Op> {
    OPS.iter()
        .filter(|op| prefixes.iter().any(|p| op.name.starts_with(p)))
        .copied()
        .collect()
}

#[test]
fn cursor_yield_sweep_table_scans() {
    run_sweep(&ops_named(&["scan_"]));
}

#[test]
fn cursor_yield_sweep_rowid_seeks() {
    run_sweep(&ops_named(&["rowid_"]));
}

#[test]
fn cursor_yield_sweep_index() {
    run_sweep(&ops_named(&["index_"]));
}

#[test]
fn cursor_yield_sweep_count_and_writes() {
    run_sweep(&ops_named(&["count", "update_", "delete_", "insert_"]));
}

/// Each yield point fires in at least one operation of the sweep, so a new point gets an
/// operation that reaches it.
#[test]
fn cursor_yield_sweep_reaches_every_point() {
    let mut reached = BTreeSet::new();
    for op in OPS {
        reached.extend(probe(op).into_keys());
    }
    let missing: Vec<String> = (0..CursorYieldPoint::POINT_COUNT)
        .filter(|o| !reached.contains(o))
        .map(point_name)
        .collect();
    assert!(missing.is_empty(), "no operation reaches {missing:?}");
}

/// Yields one time, at the first occurrence of a point.
#[derive(Debug)]
struct YieldOnceAt(YieldPoint, AtomicBool);

impl YieldInjector for YieldOnceAt {
    fn should_yield(&self, _instance: u64, _key: u64, point: YieldPoint) -> bool {
        point == self.0 && !self.1.swap(true, Ordering::SeqCst)
    }
}

/// A seek yields after it positioned its MvStore head on a chain. GC then unlinks that
/// chain, and the transaction of the seek writes the row with another statement, which
/// makes a new chain. The seek must see the write. When the seek kept its head, it decided
/// the B-tree row from the old chain: it returned a row that the transaction deleted, and a
/// moved index row two times.
#[test]
fn test_seek_sees_own_write_after_gc_unlinked_its_chain() {
    /// A query, the write at the yield, and the rows that the query must return.
    type Case = (&'static str, &'static str, &'static [(i64, i64)]);
    let cases: &[Case] = &[
        (
            "SELECT v, id FROM t INDEXED BY tv WHERE v >= 45 ORDER BY v",
            "DELETE FROM t WHERE id = 50",
            &[(60, 60), (70, 70), (80, 80), (90, 90), (100, 100)],
        ),
        (
            "SELECT v, id FROM t INDEXED BY tv WHERE v = 50",
            "DELETE FROM t WHERE id = 50",
            &[],
        ),
        (
            "SELECT id, v FROM t WHERE id >= 45",
            "DELETE FROM t WHERE id = 50",
            &[(60, 60), (70, 70), (80, 80), (90, 90), (100, 100)],
        ),
        (
            "SELECT id, v FROM t WHERE id = 50",
            "DELETE FROM t WHERE id = 50",
            &[],
        ),
        (
            "SELECT v, id FROM t INDEXED BY tv WHERE v >= 45 ORDER BY v",
            "UPDATE t SET v = 55 WHERE id = 50",
            &[(55, 50), (60, 60), (70, 70), (80, 80), (90, 90), (100, 100)],
        ),
    ];
    for point in [
        CursorYieldPoint::SeekStart,
        CursorYieldPoint::SeekBtreeProgress,
    ] {
        for (sql, write, expected) in cases {
            let db = MvccTestDbNoConn::new_with_random_db();
            let conn = db.connect();
            for setup in [
                "PRAGMA mvcc_checkpoint_threshold = -1",
                "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT)",
                "CREATE INDEX tv ON t(v)",
                "INSERT INTO t SELECT value * 10, value * 10 FROM generate_series(1, 10)",
                "PRAGMA wal_checkpoint(TRUNCATE)",
            ] {
                conn.execute(setup).unwrap();
            }
            // A rolled-back delete leaves chains of row 50 that hold only an aborted
            // version, so GC can unlink them.
            let other = db.connect();
            other.execute("BEGIN CONCURRENT").unwrap();
            other.execute("DELETE FROM t WHERE id = 50").unwrap();
            other.execute("ROLLBACK").unwrap();

            conn.execute("BEGIN CONCURRENT").unwrap();
            conn.set_yield_injector(Some(Arc::new(YieldOnceAt(
                point.point(),
                AtomicBool::new(false),
            ))));
            let mut stmt = conn.prepare(sql).unwrap();
            let (mut rows, mut yields) = (Vec::new(), 0);
            loop {
                match stmt.step().unwrap() {
                    StepResult::IO => stmt._io().step().unwrap(),
                    StepResult::Yield => {
                        yields += 1;
                        db.get_mvcc_store().gc_incremental(usize::MAX);
                        conn.execute(write).unwrap();
                    }
                    StepResult::Row => {
                        let row = stmt.row().unwrap();
                        let (a, b) = (row.get_value(0), row.get_value(1));
                        rows.push((a.as_int().unwrap(), b.as_int().unwrap()));
                    }
                    StepResult::Done => break,
                    other => panic!("unexpected {other:?}"),
                }
            }
            conn.set_yield_injector(None);
            assert_eq!(yields, 1, "{point:?} `{sql}`");
            assert_eq!(&rows, expected, "{point:?} `{sql}` after `{write}`");
        }
    }
}
