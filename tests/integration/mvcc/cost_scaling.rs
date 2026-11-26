//! Tests that the cursor work of MVCC queries grows within its complexity class.
//!
//! An MVCC cursor merges the B-tree with the MvStore. Entries that the transaction cannot
//! see (tombstones, shadowed B-tree rows, uncommitted rows, rows committed after the
//! snapshot) stay in the sources, and a cursor must step over them. A cursor that walks
//! them at each probe makes a loop of probes quadratic. CPU time on ordinary data does not
//! show this, and the opcode counters count one `Next` once however many entries it walks.
//!
//! These tests read the cursor work counters of each statement
//! ([`turso_core::CursorWork`]): the raw MvStore entries and B-tree rows that cursors
//! looked at, the seeks into each source and the visibility checks. Each cell of a table
//! is a database state of size N and a query. The test runs the cell at N and at 4N and
//! checks that each counter grows within the class of the cell:
//!
//! - [`Class::Constant`]: the work does not depend on N (point reads, probes, short
//!   ranges, writes of a fixed number of rows).
//! - [`Class::Linear`]: the work grows at most in proportion to N (full scans,
//!   `PRAGMA integrity_check`, replay of N changes).
//!
//! A quadratic cost grows 16 times from N to 4N. It fails both classes.
//!
//! There are three tests:
//!
//! - [`test_mvcc_query_cost_scaling`]: a state of N hidden or visible rows of one shape,
//!   placed before, after or around the probe rows, and 24 queries. The indexes include a
//!   NOCASE index with long keys (so hidden entries are in interior pages), a DESC index
//!   and an index whose first column is NULL. Each index query checks that its plan
//!   searches the index that its name says.
//! - [`test_mvcc_replay_cost_scaling`]: N changes in one transaction with the statement
//!   shapes of the sync pull replay, on a rowid and a composite primary key.
//! - [`test_mvcc_pull_cost_scaling`]: the large-update pull workload of the sync engine,
//!   with and without an earlier pull of the same rows.
//!
//! In the replay and pull tests the base table grows with N too, so a change whose cost
//! grows with the size of the table also fails.
//!
//! All runs are single-threaded and seeded, so the counts are the same at each run. The
//! tables of cells and their classes are [`QUERY_CELLS`], [`REPLAY_CELLS`] and
//! [`PULL_CELLS`]. A cell that
//! is known to fail with the current cursor is listed in [`KNOWN_SUPER_LINEAR`]. The test
//! fails if such a cell starts to pass, so the list stays correct, and a fix removes the
//! cells that it fixes from the list.
//!
//! Run with `MVCC_COST_REPORT=1` and `--nocapture` to print the counts of each cell.

use super::test_db::MvccTestDbNoConn;
use rand::seq::SliceRandom;
use rand_chacha::rand_core::SeedableRng;
use rand_chacha::ChaCha8Rng;
use std::fmt::Write as _;
use std::num::NonZero;
use std::sync::Arc;
use turso_core::{Connection, CursorWork, Value};

/// The size of the state at the smaller run. The larger run uses `4 * N`.
const N: usize = 200;
/// The number of probe keys. Each probe key has two rows with the same `k`.
const PROBES: usize = 16;

/// How fast the work of a cell can grow from N to 4N.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Class {
    /// At most 1.5 times the work at N, plus 16.
    Constant,
    /// At most 5 times the work at N, plus 4N (one unit for each row of the larger
    /// state). A counter that is zero at N can grow to one unit per row.
    Linear,
}

impl Class {
    fn allows(self, small: u64, large: u64) -> bool {
        match self {
            Class::Constant => large * 2 <= small * 3 + 32,
            Class::Linear => large <= small * 5 + 4 * N as u64,
        }
    }
}

/// The counters of [`CursorWork`] with their names.
fn counters(work: &CursorWork) -> [(&'static str, u64); 5] {
    [
        ("mvcc_entries", work.mvcc_entries_examined),
        ("btree_rows", work.btree_rows_examined),
        ("mvcc_seeks", work.mvcc_seeks),
        ("btree_seeks", work.btree_seeks),
        ("visibility_checks", work.visibility_checks),
    ]
}

/// Returns the counters that grow faster than `class` allows.
fn violations(class: Class, small: &CursorWork, large: &CursorWork) -> Vec<String> {
    counters(small)
        .into_iter()
        .zip(counters(large))
        .filter(|((_, s), (_, l))| !class.allows(*s, *l))
        .map(|((name, s), (_, l))| format!("{name} {s} -> {l}"))
        .collect()
}

// ---------------------------------------------------------------------------------------
// Statement helpers
// ---------------------------------------------------------------------------------------

fn run(conn: &Arc<Connection>, sql: &str) {
    conn.execute(sql)
        .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"));
}

/// Runs `sql` once for each list of parameters with one prepared statement, and returns
/// the rows and the cursor work of all the runs.
fn run_measured(
    conn: &Arc<Connection>,
    sql: &str,
    params: &[Vec<Value>],
) -> (Vec<Vec<Value>>, CursorWork) {
    let mut stmt = conn
        .prepare(sql)
        .unwrap_or_else(|e| panic!("prepare `{sql}` failed: {e}"));
    let mut rows = Vec::new();
    let no_params = [Vec::new()];
    let params = if params.is_empty() {
        &no_params[..]
    } else {
        params
    };
    for values in params {
        stmt.reset().unwrap();
        for (i, value) in values.iter().enumerate() {
            stmt.bind_at(NonZero::new(i + 1).unwrap(), value.clone())
                .unwrap();
        }
        stmt.run_with_row_callback(|row| {
            rows.push(row.get_values().cloned().collect());
            Ok(())
        })
        .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"));
    }
    let work = stmt.metrics().cursor_work;
    (rows, work)
}

fn int(v: i64) -> Value {
    Value::from_i64(v)
}

// ---------------------------------------------------------------------------------------
// Query cells: a state of size N and one query
// ---------------------------------------------------------------------------------------
//
// The table `t(id INTEGER PRIMARY KEY, u UNIQUE, k, v)` has an index on `k`. Its rows are
// in three key ranges, the same for `id`, `u` and `k`:
//
//   BEFORE (N rows of the state)  <  SENTINEL_LOW  <  probe rows  <  SENTINEL_HIGH  <  AFTER
//
// The probe rows and the two sentinels are visible. The state puts its N rows in BEFORE,
// in AFTER or half in each (`Placement`). A sentinel next to the probe rows ends each walk
// from a probe in the same source as the probes, so no probe query must look at the rows
// of the state. A cursor that walks them anyway costs work in proportion to N.

const BEFORE: i64 = 1_000_000;
const SENTINEL_LOW: i64 = 2_000_000;
const PROBE_BASE: i64 = 2_000_100;
const PROBE_GAP: i64 = 100;
const SENTINEL_HIGH: i64 = 2_900_000;
const AFTER: i64 = 3_000_000;
/// UPDATE moves the keys of the state by this offset, inside the same key range.
const MOVE: i64 = 500_000;

/// The kind of the N rows of the state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Shape {
    /// Visible rows in the B-tree.
    CheckpointedRows,
    /// Visible rows in the MvStore, committed but not checkpointed.
    CommittedMvRows,
    /// B-tree rows that a committed transaction deleted: shadowed B-tree rows and MvStore
    /// tombstones.
    Tombstones,
    /// Rows inserted and deleted in the MvStore only.
    DeletedMvRows,
    /// B-tree rows whose `u` and `k` a committed transaction changed: shadowed index
    /// entries in the B-tree and new ones in the MvStore.
    UpdatedIndexKeys,
    /// Rows that another connection inserted and did not commit.
    Uncommitted,
    /// Rows that another connection committed after the snapshot of the reader.
    CommittedAfterSnapshot,
}

const SHAPES: [Shape; 7] = [
    Shape::CheckpointedRows,
    Shape::CommittedMvRows,
    Shape::Tombstones,
    Shape::DeletedMvRows,
    Shape::UpdatedIndexKeys,
    Shape::Uncommitted,
    Shape::CommittedAfterSnapshot,
];

/// Where the N rows of the state are, relative to the probe rows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Placement {
    Before,
    After,
    Around,
}

const PLACEMENTS: [Placement; 3] = [Placement::Before, Placement::After, Placement::Around];

/// Which source holds the probe rows and the sentinels.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProbeSource {
    Btree,
    MvStore,
}

const PROBE_SOURCES: [ProbeSource; 2] = [ProbeSource::Btree, ProbeSource::MvStore];

/// The keys of the N rows of a state.
fn state_keys(placement: Placement, n: usize) -> Vec<i64> {
    let n = n as i64;
    match placement {
        Placement::Before => (BEFORE..BEFORE + n).collect(),
        Placement::After => (AFTER..AFTER + n).collect(),
        Placement::Around => (BEFORE..BEFORE + n / 2)
            .chain(AFTER..AFTER + (n - n / 2))
            .collect(),
    }
}

/// The `k` value of probe key `i`. Rows `2i` and `2i + 1` share it.
fn probe_k(i: usize) -> i64 {
    PROBE_BASE + PROBE_GAP * i as i64
}

/// Makes the keys of the NOCASE index `t_w` long, so that the index entries of N rows fill
/// several pages and some hidden entries are in interior pages of the index B-tree.
const W_PAD: &str = "wwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwww";

/// The value of `t.w` of a row with `k`. Rows store upper case; probes use lower case, so
/// a probe finds its rows through the NOCASE collation only.
fn w_value(k: i64, upper: bool) -> String {
    let prefix = if upper { "K" } else { "k" };
    format!("{prefix}{k:09}{W_PAD}")
}

/// One row of `t`: `(id, u, k, v)`. `w` and `kd` follow from `k`, and `nk` is NULL.
fn t_row(id: i64, u: i64, k: i64, v: &str) -> String {
    format!("({id}, {u}, {k}, '{v}', '{}', {k})", w_value(k, true))
}

/// SQL that inserts the probe rows and the sentinels of `t`.
fn probe_rows_sql() -> String {
    let mut values = vec![
        t_row(SENTINEL_LOW, SENTINEL_LOW, SENTINEL_LOW, "s"),
        t_row(SENTINEL_HIGH, SENTINEL_HIGH, SENTINEL_HIGH, "s"),
    ];
    for i in 0..PROBES {
        let k = probe_k(i);
        values.push(t_row(k, k, k, "p"));
        values.push(t_row(k + 1, k + 1, k, "p"));
    }
    format!(
        "INSERT INTO t(id, u, k, v, w, kd) VALUES {}",
        values.join(", ")
    )
}

/// SQL that inserts the rows of the state with keys `keys`.
fn state_rows_sql(keys: &[i64]) -> String {
    let values = keys
        .iter()
        .map(|&x| t_row(x, x, x - x % 2, "h"))
        .collect::<Vec<_>>();
    format!(
        "INSERT INTO t(id, u, k, v, w, kd) VALUES {}",
        values.join(", ")
    )
}

fn in_list(keys: &[i64]) -> String {
    keys.iter()
        .map(i64::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

/// A database in a state: the reader connection has an open transaction that sees the
/// state. The writer connection may hold uncommitted rows.
struct State {
    _db: MvccTestDbNoConn,
    reader: Arc<Connection>,
    _writer: Arc<Connection>,
}

fn build_state(shape: Shape, placement: Placement, source: ProbeSource, n: usize) -> State {
    let db = MvccTestDbNoConn::new_with_random_db();
    let reader = db.connect();
    let writer = db.connect();
    for conn in [&reader, &writer] {
        run(conn, "PRAGMA mvcc_checkpoint_threshold = -1");
    }
    run(
        &reader,
        "CREATE TABLE t(id INTEGER PRIMARY KEY, u INTEGER UNIQUE, k INTEGER, v TEXT, \
         w TEXT COLLATE NOCASE, nk INTEGER, kd INTEGER)",
    );
    run(&reader, "CREATE INDEX t_k ON t(k)");
    // A NOCASE index with long keys, a DESC index on `kd` (a copy of `k`) and an index
    // whose first column is NULL in all rows.
    run(&reader, "CREATE INDEX t_w ON t(w)");
    run(&reader, "CREATE INDEX t_kd ON t(kd DESC, u)");
    run(&reader, "CREATE INDEX t_nk ON t(nk, k)");
    run(&reader, "CREATE TABLE p(id INTEGER PRIMARY KEY, x INTEGER)");
    let probe_values = (0..PROBES)
        .map(|i| format!("({i}, {})", probe_k(i)))
        .collect::<Vec<_>>();
    run(
        &reader,
        &format!("INSERT INTO p VALUES {}", probe_values.join(", ")),
    );

    let keys = state_keys(placement, n);
    let checkpoint = |conn: &Arc<Connection>| run(conn, "PRAGMA wal_checkpoint(TRUNCATE)");

    if source == ProbeSource::Btree {
        run(&reader, &probe_rows_sql());
    }
    // The rows of the state that must be in the B-tree.
    if matches!(
        shape,
        Shape::CheckpointedRows | Shape::Tombstones | Shape::UpdatedIndexKeys
    ) {
        run(&reader, &state_rows_sql(&keys));
    }
    checkpoint(&reader);
    match shape {
        Shape::CheckpointedRows | Shape::Uncommitted | Shape::CommittedAfterSnapshot => {}
        Shape::CommittedMvRows => run(&reader, &state_rows_sql(&keys)),
        Shape::Tombstones => run(
            &reader,
            &format!("DELETE FROM t WHERE id IN ({})", in_list(&keys)),
        ),
        Shape::DeletedMvRows => {
            run(&reader, &state_rows_sql(&keys));
            run(
                &reader,
                &format!("DELETE FROM t WHERE id IN ({})", in_list(&keys)),
            );
        }
        Shape::UpdatedIndexKeys => run(
            &reader,
            &format!(
                "UPDATE t SET u = u + {MOVE}, k = k + {MOVE}, kd = kd + {MOVE}, \
                 w = 'K' || substr('000000000' || (k + {MOVE}), -9, 9) || '{W_PAD}' \
                 WHERE id IN ({})",
                in_list(&keys)
            ),
        ),
    }
    if source == ProbeSource::MvStore {
        run(&reader, &probe_rows_sql());
    }

    if shape == Shape::Uncommitted {
        run(&writer, "BEGIN CONCURRENT");
        run(&writer, &state_rows_sql(&keys));
    }
    run(&reader, "BEGIN CONCURRENT");
    // The first read takes the snapshot of the reader.
    run(&reader, "SELECT count(*) FROM p");
    if shape == Shape::CommittedAfterSnapshot {
        run(&writer, &state_rows_sql(&keys));
    }
    State {
        _db: db,
        reader,
        _writer: writer,
    }
}

/// A query of a query cell. The queries run in this order in one transaction of the
/// reader. The writes come last; they change a fixed number of rows in the probe range,
/// so the state stays the same between N and 4N.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Query {
    PointRowid,
    PointUnique,
    PointNonUnique,
    PointNocase,
    PointDescIndex,
    PointNullPrefix,
    EqualityLoop,
    RangeRowidAsc,
    RangeRowidDesc,
    RangeIndexAsc,
    RangeIndexDesc,
    RangeDescIndexAsc,
    RangeDescIndexDesc,
    JoinRowid,
    JoinUnique,
    JoinNonUnique,
    FullScanAsc,
    FullScanDesc,
    FullIndexScanAsc,
    IntegrityCheck,
    InsertUnique,
    UpdateThroughIndex,
    UpdateIndexKey,
    DeleteThroughIndex,
}

const QUERIES: [Query; 24] = [
    Query::PointRowid,
    Query::PointUnique,
    Query::PointNonUnique,
    Query::PointNocase,
    Query::PointDescIndex,
    Query::PointNullPrefix,
    Query::EqualityLoop,
    Query::RangeRowidAsc,
    Query::RangeRowidDesc,
    Query::RangeIndexAsc,
    Query::RangeIndexDesc,
    Query::RangeDescIndexAsc,
    Query::RangeDescIndexDesc,
    Query::JoinRowid,
    Query::JoinUnique,
    Query::JoinNonUnique,
    Query::FullScanAsc,
    Query::FullScanDesc,
    Query::FullIndexScanAsc,
    Query::IntegrityCheck,
    Query::InsertUnique,
    Query::UpdateThroughIndex,
    Query::UpdateIndexKey,
    Query::DeleteThroughIndex,
];

/// The class of each query. The same for all states.
const QUERY_CELLS: &[(Query, Class)] = &[
    (Query::PointRowid, Class::Constant),
    (Query::PointUnique, Class::Constant),
    (Query::PointNonUnique, Class::Constant),
    (Query::PointNocase, Class::Constant),
    (Query::PointDescIndex, Class::Constant),
    (Query::PointNullPrefix, Class::Constant),
    (Query::EqualityLoop, Class::Constant),
    (Query::RangeRowidAsc, Class::Constant),
    (Query::RangeRowidDesc, Class::Constant),
    (Query::RangeIndexAsc, Class::Constant),
    (Query::RangeIndexDesc, Class::Constant),
    (Query::RangeDescIndexAsc, Class::Constant),
    (Query::RangeDescIndexDesc, Class::Constant),
    (Query::JoinRowid, Class::Constant),
    (Query::JoinUnique, Class::Constant),
    (Query::JoinNonUnique, Class::Constant),
    (Query::FullScanAsc, Class::Linear),
    (Query::FullScanDesc, Class::Linear),
    (Query::FullIndexScanAsc, Class::Linear),
    (Query::IntegrityCheck, Class::Linear),
    (Query::InsertUnique, Class::Constant),
    (Query::UpdateThroughIndex, Class::Constant),
    (Query::UpdateIndexKey, Class::Constant),
    (Query::DeleteThroughIndex, Class::Constant),
];

fn query_class(query: Query) -> Class {
    QUERY_CELLS
        .iter()
        .find(|(q, _)| *q == query)
        .map(|(_, class)| *class)
        .expect("each query has a class")
}

fn probe_ks() -> Vec<i64> {
    (0..PROBES).map(probe_k).collect()
}

/// Runs `query` on the reader and returns its cursor work. Checks the rows that the query
/// returns where they do not depend on the state.
fn run_query(state: &State, query: Query) -> CursorWork {
    let conn = &state.reader;
    let one_param = |keys: Vec<i64>| keys.into_iter().map(|k| vec![int(k)]).collect::<Vec<_>>();
    let range = format!("BETWEEN {} AND {}", SENTINEL_LOW + 1, SENTINEL_HIGH - 1);
    let (sql, params, expected_rows): (String, Vec<Vec<Value>>, Option<usize>) = match query {
        Query::PointNocase => (
            "SELECT id FROM t WHERE w = ?".into(),
            probe_ks()
                .into_iter()
                .map(|k| vec![Value::from_text(w_value(k, false))])
                .collect(),
            Some(2 * PROBES),
        ),
        Query::PointDescIndex => (
            "SELECT u FROM t WHERE kd = ?".into(),
            one_param(probe_ks()),
            Some(2 * PROBES),
        ),
        Query::PointNullPrefix => (
            "SELECT id FROM t WHERE nk IS NULL AND k = ?".into(),
            one_param(probe_ks()),
            Some(2 * PROBES),
        ),
        Query::RangeDescIndexAsc => (
            format!("SELECT kd FROM t WHERE kd {range} ORDER BY kd"),
            vec![],
            Some(2 * PROBES),
        ),
        Query::RangeDescIndexDesc => (
            format!("SELECT kd FROM t WHERE kd {range} ORDER BY kd DESC"),
            vec![],
            Some(2 * PROBES),
        ),
        Query::PointRowid => (
            "SELECT v FROM t WHERE id = ?".into(),
            one_param(probe_ks()),
            Some(PROBES),
        ),
        Query::PointUnique => (
            "SELECT v FROM t WHERE u = ?".into(),
            one_param(probe_ks()),
            Some(PROBES),
        ),
        Query::PointNonUnique => (
            "SELECT id FROM t WHERE k = ?".into(),
            one_param(probe_ks()),
            Some(2 * PROBES),
        ),
        Query::EqualityLoop => (
            format!("SELECT id FROM t WHERE k IN ({})", in_list(&probe_ks())),
            vec![],
            Some(2 * PROBES),
        ),
        Query::RangeRowidAsc => (
            format!("SELECT id FROM t WHERE id {range} ORDER BY id"),
            vec![],
            Some(2 * PROBES),
        ),
        Query::RangeRowidDesc => (
            format!("SELECT id FROM t WHERE id {range} ORDER BY id DESC"),
            vec![],
            Some(2 * PROBES),
        ),
        Query::RangeIndexAsc => (
            format!("SELECT k FROM t WHERE k {range} ORDER BY k"),
            vec![],
            Some(2 * PROBES),
        ),
        Query::RangeIndexDesc => (
            format!("SELECT k FROM t WHERE k {range} ORDER BY k DESC"),
            vec![],
            Some(2 * PROBES),
        ),
        Query::JoinRowid => (
            "SELECT t.v FROM p CROSS JOIN t ON t.id = p.x".into(),
            vec![],
            Some(PROBES),
        ),
        Query::JoinUnique => (
            "SELECT t.v FROM p CROSS JOIN t ON t.u = p.x".into(),
            vec![],
            Some(PROBES),
        ),
        Query::JoinNonUnique => (
            "SELECT t.id FROM p CROSS JOIN t ON t.k = p.x".into(),
            vec![],
            Some(2 * PROBES),
        ),
        Query::FullScanAsc => ("SELECT id, v FROM t".into(), vec![], None),
        Query::FullScanDesc => ("SELECT id, v FROM t ORDER BY id DESC".into(), vec![], None),
        Query::FullIndexScanAsc => ("SELECT k FROM t ORDER BY k".into(), vec![], None),
        Query::IntegrityCheck => ("PRAGMA integrity_check".into(), vec![], Some(1)),
        // New rows between the probe rows. The UNIQUE check of `u` and the rowid check
        // probe both sources.
        Query::InsertUnique => (
            "INSERT INTO t(id, u, k, v) VALUES (?, ?, ?, 'n')".into(),
            probe_ks()
                .into_iter()
                .map(|k| vec![int(k + 50), int(k + 50), int(k + 50)])
                .collect(),
            Some(0),
        ),
        Query::UpdateThroughIndex => (
            "UPDATE t SET v = 'w' WHERE u = ?".into(),
            one_param(probe_ks()),
            Some(0),
        ),
        Query::UpdateIndexKey => (
            "UPDATE t SET k = k + 2 WHERE u = ?".into(),
            one_param(probe_ks().into_iter().map(|k| k + 50).collect()),
            Some(0),
        ),
        Query::DeleteThroughIndex => (
            "DELETE FROM t WHERE u = ?".into(),
            one_param(probe_ks().into_iter().map(|k| k + 50).collect()),
            Some(0),
        ),
    };
    if let Some(index) = query_index(query) {
        assert_plan_uses(conn, &sql, index);
    }
    let (rows, work) = run_measured(conn, &sql, &params);
    if let Some(expected) = expected_rows {
        assert_eq!(rows.len(), expected, "{query:?}: `{sql}` returned {rows:?}");
    }
    if query == Query::IntegrityCheck {
        assert_eq!(rows[0][0].to_string(), "ok");
    }
    work
}

/// The index that a query must use, so that the cell measures the access path that its name
/// says.
fn query_index(query: Query) -> Option<&'static str> {
    match query {
        Query::PointUnique | Query::JoinUnique => Some("sqlite_autoindex_t_1"),
        Query::PointNonUnique
        | Query::EqualityLoop
        | Query::RangeIndexAsc
        | Query::RangeIndexDesc
        | Query::JoinNonUnique => Some("t_k"),
        Query::PointNocase => Some("t_w"),
        Query::PointDescIndex | Query::RangeDescIndexAsc | Query::RangeDescIndexDesc => {
            Some("t_kd")
        }
        Query::PointNullPrefix => Some("t_nk"),
        _ => None,
    }
}

/// Checks that the plan of `sql` reads index `index`.
fn assert_plan_uses(conn: &Arc<Connection>, sql: &str, index: &str) {
    let (rows, _) = run_measured(conn, &format!("EXPLAIN QUERY PLAN {sql}"), &[]);
    let plan = rows
        .iter()
        .map(|row| row.last().map(|v| v.to_string()).unwrap_or_default())
        .collect::<Vec<_>>()
        .join("\n");
    // The index must be searched, not scanned: a scan of the index with a filter does not
    // measure the lookups that the cell names.
    let uses = plan.lines().any(|line| {
        line.contains("SEARCH")
            && line
                .split(|c: char| !(c.is_alphanumeric() || c == '_'))
                .any(|word| word == index)
    });
    assert!(uses, "`{sql}` must search index {index}, plan:\n{plan}");
}

/// The cursor work of each query on one state.
fn measure_state(
    shape: Shape,
    placement: Placement,
    source: ProbeSource,
    n: usize,
) -> Vec<CursorWork> {
    let state = build_state(shape, placement, source, n);
    let work = QUERIES.iter().map(|&q| run_query(&state, q)).collect();
    run(&state.reader, "ROLLBACK");
    work
}

// ---------------------------------------------------------------------------------------
// Replay cells: N changes in one transaction, as a sync pull replays them
// ---------------------------------------------------------------------------------------
//
// A pull replays the remote changes in one local transaction, one statement for each
// change, with the statement shapes of `sync/engine/src/database_replay_generator.rs`.
// Each change makes the old B-tree entries of its row hidden behind newer MvStore
// versions, so the later changes do their lookups next to many hidden entries. The base
// table is checkpointed and has 4 rows for each change, so a change whose cost grows with
// the size of the table makes the replay quadratic too.

/// The number of rows of the base table of a replay of `n` changes.
fn replay_base(n: usize) -> i64 {
    4 * n as i64
}

/// Moves a non-unique `s` past all the values of the base table.
const S_OFFSET: i64 = 1_000_000;

/// The primary key of the replayed table.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReplayTable {
    /// `id INTEGER PRIMARY KEY`.
    Rowid,
    /// `PRIMARY KEY (a, c)`, with 10 rows for each `a`.
    CompositePk,
}

/// The secondary index on column `s`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SecondaryIndex {
    /// `s` has two rows for each value.
    NonUnique,
    /// UNIQUE, with no NULL.
    Unique,
    /// UNIQUE, NULL in the rows with an even key.
    NullableUnique,
}

/// The order in which the changes visit their keys.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Order {
    /// Keys `1..=n`: each change is next to the hidden entries of the change before it.
    KeyOrder,
    /// `n` keys picked at random from the base table, in random order.
    Random,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Change {
    /// `UPDATE .. SET b = ? WHERE <pk>`: no indexed column changes.
    UpdateUnindexed,
    /// `UPDATE .. SET s = ? WHERE <pk>`: the key of the secondary index changes.
    UpdateIndexed,
    /// `DELETE FROM .. WHERE <pk>`.
    Delete,
    /// All the deletes, then `INSERT` of the same rows again.
    DeleteThenInsert,
    /// `INSERT .. ON CONFLICT(<pk>) DO UPDATE SET ..` of existing rows, with a new `s`.
    Upsert,
    /// `INSERT OR REPLACE` of existing rows, with a new `s`.
    InsertOrReplace,
    /// `UPDATE .. SET s = ?, b = ? WHERE <pk>` with the old `s`: the index entry is deleted
    /// and inserted again at the same key.
    UpdateSameIndexValue,
    /// `INSERT .. ON CONFLICT(<pk>) DO UPDATE SET ..` of all the columns with the old `s`, as
    /// a pull replays a full-row update.
    UpsertSameIndexValue,
}

const REPLAY_TABLES: [ReplayTable; 2] = [ReplayTable::Rowid, ReplayTable::CompositePk];
const SECONDARY_INDEXES: [SecondaryIndex; 3] = [
    SecondaryIndex::NonUnique,
    SecondaryIndex::Unique,
    SecondaryIndex::NullableUnique,
];
const ORDERS: [Order; 2] = [Order::KeyOrder, Order::Random];

/// The class of each change: the replay of N changes must cost work linear in N, that is,
/// about constant work for each change.
const REPLAY_CELLS: &[(Change, Class)] = &[
    (Change::UpdateUnindexed, Class::Linear),
    (Change::UpdateIndexed, Class::Linear),
    (Change::Delete, Class::Linear),
    (Change::DeleteThenInsert, Class::Linear),
    (Change::Upsert, Class::Linear),
    (Change::InsertOrReplace, Class::Linear),
    (Change::UpdateSameIndexValue, Class::Linear),
    (Change::UpsertSameIndexValue, Class::Linear),
];

/// The values of the base row with key `key`: `(pk values, s, b)`.
fn replay_row(table: ReplayTable, index: SecondaryIndex, key: i64) -> (Vec<Value>, Value, Value) {
    let pk = match table {
        ReplayTable::Rowid => vec![int(key)],
        ReplayTable::CompositePk => vec![int(key / 10), int(key % 10)],
    };
    let s = match index {
        SecondaryIndex::NonUnique => int(key / 2),
        SecondaryIndex::Unique => int(key * 2),
        SecondaryIndex::NullableUnique if key % 2 == 0 => Value::Null,
        SecondaryIndex::NullableUnique => int(key * 2),
    };
    (pk, s, int(key))
}

/// The new value of `s` that a change writes. It keeps the index valid: the new value of a
/// UNIQUE index is not used by another row.
fn changed_s(index: SecondaryIndex, key: i64) -> Value {
    match index {
        SecondaryIndex::NonUnique => int(key / 2 + S_OFFSET),
        SecondaryIndex::Unique => int(key * 2 + 1),
        SecondaryIndex::NullableUnique if key % 2 == 0 => Value::Null,
        SecondaryIndex::NullableUnique => int(key * 2 + 1),
    }
}

fn build_replay_base(
    table: ReplayTable,
    index: SecondaryIndex,
    n: usize,
) -> (MvccTestDbNoConn, Arc<Connection>) {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    run(&conn, "PRAGMA mvcc_checkpoint_threshold = -1");
    match table {
        ReplayTable::Rowid => run(
            &conn,
            "CREATE TABLE r(id INTEGER PRIMARY KEY, s INTEGER, b INTEGER)",
        ),
        ReplayTable::CompositePk => run(
            &conn,
            "CREATE TABLE r(a INTEGER, c INTEGER, s INTEGER, b INTEGER, PRIMARY KEY (a, c))",
        ),
    }
    match index {
        SecondaryIndex::NonUnique => run(&conn, "CREATE INDEX r_s ON r(s)"),
        SecondaryIndex::Unique | SecondaryIndex::NullableUnique => {
            run(&conn, "CREATE UNIQUE INDEX r_s ON r(s)")
        }
    }
    let insert = insert_sql(table, "INSERT");
    let rows = (1..=replay_base(n))
        .map(|key| {
            let (mut values, s, b) = replay_row(table, index, key);
            values.extend([s, b]);
            values
        })
        .collect::<Vec<_>>();
    run(&conn, "BEGIN");
    run_measured(&conn, &insert, &rows);
    run(&conn, "COMMIT");
    run(&conn, "PRAGMA wal_checkpoint(TRUNCATE)");
    (db, conn)
}

fn pk_columns(table: ReplayTable) -> &'static [&'static str] {
    match table {
        ReplayTable::Rowid => &["id"],
        ReplayTable::CompositePk => &["a", "c"],
    }
}

/// The predicate of the replayed UPDATE and DELETE statements on the primary key.
fn pk_predicate(table: ReplayTable) -> String {
    match table {
        // A rowid table without a declared non-rowid key uses `<rowid alias> = ?`.
        ReplayTable::Rowid => "\"id\" = ?".into(),
        ReplayTable::CompositePk => "\"a\" IS ? AND \"c\" IS ?".into(),
    }
}

fn insert_sql(table: ReplayTable, verb: &str) -> String {
    let mut columns = pk_columns(table).to_vec();
    columns.extend(["s", "b"]);
    let quoted = columns
        .iter()
        .map(|c| format!("\"{c}\""))
        .collect::<Vec<_>>()
        .join(", ");
    let placeholders = vec!["?"; columns.len()].join(",");
    format!("{verb} INTO \"r\"({quoted}) VALUES ({placeholders})")
}

fn upsert_sql(table: ReplayTable) -> String {
    let pk = pk_columns(table)
        .iter()
        .map(|c| format!("\"{c}\""))
        .collect::<Vec<_>>()
        .join(",");
    let mut columns = pk_columns(table).to_vec();
    columns.extend(["s", "b"]);
    let updates = columns
        .iter()
        .map(|c| format!("\"{c}\" = excluded.\"{c}\""))
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "{} ON CONFLICT({pk}) DO UPDATE SET {updates}",
        insert_sql(table, "INSERT")
    )
}

fn replay_keys(order: Order, n: usize) -> Vec<i64> {
    match order {
        Order::KeyOrder => (1..=n as i64).collect(),
        Order::Random => {
            let mut rng = ChaCha8Rng::seed_from_u64(0x5eed + n as u64);
            let mut keys = (1..=replay_base(n)).collect::<Vec<_>>();
            keys.shuffle(&mut rng);
            keys.truncate(n);
            keys
        }
    }
}

/// Replays `n` changes in one transaction, rolls it back and returns the cursor work.
fn measure_replay(
    conn: &Arc<Connection>,
    table: ReplayTable,
    index: SecondaryIndex,
    change: Change,
    order: Order,
    n: usize,
) -> CursorWork {
    let keys = replay_keys(order, n);
    let pk_of = |key: i64| replay_row(table, index, key).0;
    let full_row = |key: i64, s: Value| {
        let (mut values, _, b) = replay_row(table, index, key);
        values.extend([s, b]);
        values
    };
    let predicate = pk_predicate(table);
    run(conn, "BEGIN");
    let mut work = CursorWork::default();
    let mut replay = |sql: &str, params: Vec<Vec<Value>>| {
        let (_, w) = run_measured(conn, sql, &params);
        work.merge(&w);
    };
    match change {
        Change::UpdateUnindexed => replay(
            &format!("UPDATE \"r\" SET \"b\" = ? WHERE {predicate}"),
            keys.iter()
                .map(|&key| [vec![int(-key)], pk_of(key)].concat())
                .collect(),
        ),
        Change::UpdateIndexed => replay(
            &format!("UPDATE \"r\" SET \"s\" = ? WHERE {predicate}"),
            keys.iter()
                .map(|&key| [vec![changed_s(index, key)], pk_of(key)].concat())
                .collect(),
        ),
        Change::Delete => replay(
            &format!("DELETE FROM \"r\" WHERE {predicate}"),
            keys.iter().map(|&key| pk_of(key)).collect(),
        ),
        Change::DeleteThenInsert => {
            replay(
                &format!("DELETE FROM \"r\" WHERE {predicate}"),
                keys.iter().map(|&key| pk_of(key)).collect(),
            );
            replay(
                &insert_sql(table, "INSERT"),
                keys.iter()
                    .map(|&key| full_row(key, replay_row(table, index, key).1))
                    .collect(),
            );
        }
        Change::Upsert => replay(
            &upsert_sql(table),
            keys.iter()
                .map(|&key| full_row(key, changed_s(index, key)))
                .collect(),
        ),
        Change::InsertOrReplace => replay(
            &insert_sql(table, "INSERT OR REPLACE"),
            keys.iter()
                .map(|&key| full_row(key, changed_s(index, key)))
                .collect(),
        ),
        Change::UpdateSameIndexValue => replay(
            &format!("UPDATE \"r\" SET \"s\" = ?, \"b\" = ? WHERE {predicate}"),
            keys.iter()
                .map(|&key| {
                    let s = replay_row(table, index, key).1;
                    [vec![s, int(-key)], pk_of(key)].concat()
                })
                .collect(),
        ),
        Change::UpsertSameIndexValue => replay(
            &upsert_sql(table),
            keys.iter()
                .map(|&key| {
                    let (mut values, s, _) = replay_row(table, index, key);
                    values.extend([s, int(-key)]);
                    values
                })
                .collect(),
        ),
    }
    run(conn, "ROLLBACK");
    work
}

// ---------------------------------------------------------------------------------------
// Pull workload cells: the large-update pull of the sync engine
// ---------------------------------------------------------------------------------------
//
// The workload of `sync/engine/tests/mvcc_logical_pull/pull_scaling.rs` (PR #12102): a
// table with large rows and five indexes, and a remote transaction that sets
// `version = version + 1, c0 = c0 + 1` on the rows in key order, so the new `c0` of a
// row is the old `c0` of the next row. A pull can follow an earlier pull of the same rows
// that is still in the MvStore or that a checkpoint moved to the B-tree.
//
// `PullChange::RecordedPull` runs the statements that a replica ran in `pull()` of that
// test, recorded with a trace of each statement and its parameters: in one
// `BEGIN IMMEDIATE` transaction, the sync bookkeeping, a `DELETE .. WHERE "id" IS ?` of
// each changed row in key order, then an `INSERT .. ON CONFLICT("id") DO UPDATE` of each
// row in key order (the rows were deleted, so each is a plain insert into all indexes),
// and an update of `turso_sync_last_change_id`. The reads of `turso_cdc` are left out:
// the table does not exist here and they do not touch `items`.

/// The number of rows of the base table of a pull of `n` changes. It grows with `n`, so a
/// change whose cost grows with the size of the table makes the pull quadratic too.
fn pull_base(n: usize) -> i64 {
    2 * n as i64
}
/// The length of the `payload` column, as in the reproduction of the slow pull.
const PULL_PAYLOAD: usize = 1400;

/// The table of a pull cell.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PullTable {
    /// `items(id INTEGER PRIMARY KEY, payload, version, c0..c6)` with indexes on `c0..c4`.
    Indexed,
    /// `Indexed` with 23 VIRTUAL generated columns.
    IndexedGenerated,
}

/// How the pull replays a change.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PullChange {
    /// `INSERT .. ON CONFLICT(id) DO UPDATE SET` all the columns: the change record has
    /// no list of changed columns.
    FullRowUpsert,
    /// `UPDATE items SET version = ?, c0 = ? WHERE id = ?`: the change record lists the
    /// changed columns.
    UpdateChangedColumns,
    /// The statement sequence that the sync engine ran in a recorded pull. See the
    /// comment at the start of this section.
    RecordedPull,
}

/// What happened to the same rows before the measured pull.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PriorPull {
    None,
    /// An earlier pull of the same rows committed, and its versions are in the MvStore.
    Committed,
    /// An earlier pull of the same rows committed, and a checkpoint moved it to the B-tree.
    Checkpointed,
}

/// The pull cells. Each must cost work linear in the number of changes.
const PULL_CELLS: &[(PullTable, PullChange, PriorPull)] = &[
    (
        PullTable::Indexed,
        PullChange::FullRowUpsert,
        PriorPull::None,
    ),
    (
        PullTable::Indexed,
        PullChange::FullRowUpsert,
        PriorPull::Committed,
    ),
    (
        PullTable::Indexed,
        PullChange::FullRowUpsert,
        PriorPull::Checkpointed,
    ),
    (
        PullTable::Indexed,
        PullChange::UpdateChangedColumns,
        PriorPull::None,
    ),
    (
        PullTable::Indexed,
        PullChange::UpdateChangedColumns,
        PriorPull::Committed,
    ),
    (
        PullTable::Indexed,
        PullChange::RecordedPull,
        PriorPull::None,
    ),
    (
        PullTable::Indexed,
        PullChange::RecordedPull,
        PriorPull::Committed,
    ),
    (
        PullTable::Indexed,
        PullChange::RecordedPull,
        PriorPull::Checkpointed,
    ),
    (
        PullTable::IndexedGenerated,
        PullChange::RecordedPull,
        PriorPull::None,
    ),
    (
        PullTable::IndexedGenerated,
        PullChange::FullRowUpsert,
        PriorPull::None,
    ),
    (
        PullTable::IndexedGenerated,
        PullChange::UpdateChangedColumns,
        PriorPull::None,
    ),
];

const PULL_COLUMNS: [&str; 10] = [
    "id", "payload", "version", "c0", "c1", "c2", "c3", "c4", "c5", "c6",
];

fn build_pull_base(table: PullTable, n: usize) -> (MvccTestDbNoConn, Arc<Connection>) {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    run(&conn, "PRAGMA mvcc_checkpoint_threshold = -1");
    let mut columns = vec![
        "id INTEGER PRIMARY KEY".to_string(),
        "payload TEXT".to_string(),
        "version INTEGER".to_string(),
    ];
    columns.extend((0..7).map(|c| format!("c{c} INTEGER")));
    if table == PullTable::IndexedGenerated {
        columns.extend((0..23).map(|g| format!("g{g} TEXT AS (CAST(c{} AS TEXT)) VIRTUAL", g % 7)));
    }
    run(
        &conn,
        &format!("CREATE TABLE items ({})", columns.join(", ")),
    );
    for c in 0..5 {
        run(&conn, &format!("CREATE INDEX items_{c} ON items (c{c})"));
    }
    run(
        &conn,
        "CREATE TABLE IF NOT EXISTS turso_sync_last_change_id \
         (client_id TEXT PRIMARY KEY, pull_gen INTEGER, change_id INTEGER)",
    );
    let rows = (1..=pull_base(n))
        .map(|id| pull_row(id, 0))
        .collect::<Vec<_>>();
    run(&conn, "BEGIN");
    run_measured(&conn, &pull_insert_sql(""), &rows);
    run(&conn, "COMMIT");
    run(&conn, "PRAGMA wal_checkpoint(TRUNCATE)");
    (db, conn)
}

/// The row `id` after `pulls` remote updates: `version` and `c0` grew by `pulls`.
fn pull_row(id: i64, pulls: i64) -> Vec<Value> {
    let mut values = vec![
        int(id),
        Value::from_text(format!("{id:0PULL_PAYLOAD$}")),
        int(pulls),
        int(id + pulls),
    ];
    values.extend((1..7).map(|c| int(id + c)));
    values
}

fn pull_insert_sql(conflict: &str) -> String {
    let columns = PULL_COLUMNS.join(", ");
    let placeholders = vec!["?"; PULL_COLUMNS.len()].join(",");
    format!("INSERT INTO items({columns}) VALUES ({placeholders}){conflict}")
}

/// Replays a pull of `pulls`-th updates of the rows `keys` in one transaction and returns
/// the cursor work. The caller commits or rolls back.
fn replay_pull(conn: &Arc<Connection>, change: PullChange, keys: &[i64], pulls: i64) -> CursorWork {
    let (_, work) = match change {
        PullChange::FullRowUpsert => {
            let updates = PULL_COLUMNS
                .iter()
                .map(|c| format!("{c} = excluded.{c}"))
                .collect::<Vec<_>>()
                .join(",");
            run_measured(
                conn,
                &pull_insert_sql(&format!(" ON CONFLICT(id) DO UPDATE SET {updates}")),
                &keys
                    .iter()
                    .map(|&id| pull_row(id, pulls))
                    .collect::<Vec<_>>(),
            )
        }
        PullChange::UpdateChangedColumns => run_measured(
            conn,
            "UPDATE items SET version = ?, c0 = ? WHERE id = ?",
            &keys
                .iter()
                .map(|&id| vec![int(pulls), int(id + pulls), int(id)])
                .collect::<Vec<_>>(),
        ),
        PullChange::RecordedPull => return recorded_pull(conn, keys, pulls),
    };
    work
}

/// Runs the statements of a recorded pull of the rows `keys`, inside the caller's
/// transaction, and returns the cursor work of all of them.
fn recorded_pull(conn: &Arc<Connection>, keys: &[i64], pulls: i64) -> CursorWork {
    let mut work = CursorWork::default();
    let mut step = |sql: &str, params: Vec<Vec<Value>>| {
        let (_, w) = run_measured(conn, sql, &params);
        work.merge(&w);
    };
    let client = || Value::from_text("client".to_string());
    step(
        "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name = ?",
        vec![vec![Value::from_text("turso_cdc".to_string())]],
    );
    step(
        "SELECT cid, name, type, pk FROM pragma_table_info('items')",
        vec![],
    );
    step("PRAGMA main.table_info='items'", vec![]);
    step(
        "DELETE FROM \"items\" WHERE \"id\" IS ?",
        keys.iter().map(|&id| vec![int(id)]).collect(),
    );
    step(
        "SELECT cid, name, type, pk FROM pragma_table_info('items')",
        vec![],
    );
    step("PRAGMA main.table_info='items'", vec![]);
    let columns = PULL_COLUMNS
        .iter()
        .map(|c| format!("\"{c}\""))
        .collect::<Vec<_>>();
    let updates = columns
        .iter()
        .map(|c| format!("{c} = excluded.{c}"))
        .collect::<Vec<_>>()
        .join(",");
    step(
        &format!(
            "INSERT INTO \"items\"({}) VALUES ({}) ON CONFLICT(\"id\") DO UPDATE SET {updates}",
            columns.join(", "),
            vec!["?"; columns.len()].join(",")
        ),
        keys.iter().map(|&id| pull_row(id, pulls)).collect(),
    );
    step(
        "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name = ?",
        vec![vec![Value::from_text(
            "turso_sync_last_change_id".to_string(),
        )]],
    );
    step(
        "SELECT pull_gen, change_id FROM turso_sync_last_change_id WHERE client_id = ?",
        vec![vec![client()]],
    );
    step(
        "INSERT INTO turso_sync_last_change_id(client_id, pull_gen, change_id) VALUES (?, ?, ?) \
         ON CONFLICT(client_id) DO UPDATE SET pull_gen = excluded.pull_gen, \
         change_id = excluded.change_id",
        vec![vec![client(), int(pulls), int(pulls)]],
    );
    work
}

/// Measures a pull of `n` changes in `order`, after `prior`. A cell with an earlier pull
/// builds its own database; the others share `base`, built for `n`, and roll back.
fn measure_pull(
    base: &Arc<Connection>,
    table: PullTable,
    change: PullChange,
    order: Order,
    prior: PriorPull,
    n: usize,
) -> CursorWork {
    let mut keys = (1..=n as i64).collect::<Vec<_>>();
    if order == Order::Random {
        let mut rng = ChaCha8Rng::seed_from_u64(0xb0b + n as u64);
        keys = (1..=pull_base(n)).collect();
        keys.shuffle(&mut rng);
        keys.truncate(n);
    }
    if prior == PriorPull::None {
        run(base, "BEGIN");
        let work = replay_pull(base, change, &keys, 1);
        run(base, "ROLLBACK");
        return work;
    }
    let (_db, conn) = build_pull_base(table, n);
    run(&conn, "BEGIN");
    replay_pull(&conn, change, &keys, 1);
    run(&conn, "COMMIT");
    if prior == PriorPull::Checkpointed {
        run(&conn, "PRAGMA wal_checkpoint(TRUNCATE)");
    }
    run(&conn, "BEGIN");
    let work = replay_pull(&conn, change, &keys, 2);
    run(&conn, "ROLLBACK");
    work
}

fn pull_cell_name(table: PullTable, change: PullChange, order: Order, prior: PriorPull) -> String {
    format!("pull/{table:?}/{change:?}/{order:?}/prior-{prior:?}")
}

// ---------------------------------------------------------------------------------------
// Known failures
// ---------------------------------------------------------------------------------------

/// Cells that grow faster than their class with the current cursor, grouped by cause. A
/// change that makes a cell of this list pass must remove the cell, and a change that makes
/// another cell fail must not be merged. So each fix of the cursor shows what it fixes, and
/// no fix can bring back a cost that an earlier fix removed. The names are the text of
/// [`query_cell_name`], [`replay_cell_name`] and [`pull_cell_name`].
const KNOWN_SUPER_LINEAR: &[(&str, &[&str])] = &[
    (
        "A step of the cursor finds the next visible row of each source on its own. It \
         walks all the hidden entries after the current key in one source, even when the \
         other source has the next row close by. A range read walks them after its last \
         row, and an equality loop or an index write walks them at each probe. The merge \
         walk of PR #12556 fixes all these cells.",
        &[
            "Tombstones/Before/probes-in-Btree/RangeRowidDesc",
            "Tombstones/Before/probes-in-Btree/RangeIndexDesc",
            "Tombstones/Before/probes-in-Btree/RangeDescIndexDesc",
            "Tombstones/Before/probes-in-MvStore/RangeRowidDesc",
            "Tombstones/Before/probes-in-MvStore/RangeIndexDesc",
            "Tombstones/Before/probes-in-MvStore/RangeDescIndexDesc",
            "Tombstones/After/probes-in-Btree/EqualityLoop",
            "Tombstones/After/probes-in-Btree/RangeRowidAsc",
            "Tombstones/After/probes-in-Btree/RangeIndexAsc",
            "Tombstones/After/probes-in-Btree/RangeDescIndexAsc",
            "Tombstones/After/probes-in-Btree/UpdateIndexKey",
            "Tombstones/After/probes-in-Btree/DeleteThroughIndex",
            "Tombstones/After/probes-in-MvStore/EqualityLoop",
            "Tombstones/After/probes-in-MvStore/RangeRowidAsc",
            "Tombstones/After/probes-in-MvStore/RangeIndexAsc",
            "Tombstones/After/probes-in-MvStore/RangeDescIndexAsc",
            "Tombstones/Around/probes-in-Btree/EqualityLoop",
            "Tombstones/Around/probes-in-Btree/RangeRowidAsc",
            "Tombstones/Around/probes-in-Btree/RangeRowidDesc",
            "Tombstones/Around/probes-in-Btree/RangeIndexAsc",
            "Tombstones/Around/probes-in-Btree/RangeIndexDesc",
            "Tombstones/Around/probes-in-Btree/RangeDescIndexAsc",
            "Tombstones/Around/probes-in-Btree/RangeDescIndexDesc",
            "Tombstones/Around/probes-in-Btree/UpdateIndexKey",
            "Tombstones/Around/probes-in-Btree/DeleteThroughIndex",
            "Tombstones/Around/probes-in-MvStore/EqualityLoop",
            "Tombstones/Around/probes-in-MvStore/RangeRowidAsc",
            "Tombstones/Around/probes-in-MvStore/RangeRowidDesc",
            "Tombstones/Around/probes-in-MvStore/RangeIndexAsc",
            "Tombstones/Around/probes-in-MvStore/RangeIndexDesc",
            "Tombstones/Around/probes-in-MvStore/RangeDescIndexAsc",
            "Tombstones/Around/probes-in-MvStore/RangeDescIndexDesc",
            "DeletedMvRows/Before/probes-in-Btree/RangeRowidDesc",
            "DeletedMvRows/Before/probes-in-Btree/RangeIndexDesc",
            "DeletedMvRows/Before/probes-in-Btree/RangeDescIndexDesc",
            "DeletedMvRows/After/probes-in-Btree/EqualityLoop",
            "DeletedMvRows/After/probes-in-Btree/RangeRowidAsc",
            "DeletedMvRows/After/probes-in-Btree/RangeIndexAsc",
            "DeletedMvRows/After/probes-in-Btree/RangeDescIndexAsc",
            "DeletedMvRows/After/probes-in-Btree/UpdateIndexKey",
            "DeletedMvRows/After/probes-in-Btree/DeleteThroughIndex",
            "DeletedMvRows/Around/probes-in-Btree/EqualityLoop",
            "DeletedMvRows/Around/probes-in-Btree/RangeRowidAsc",
            "DeletedMvRows/Around/probes-in-Btree/RangeRowidDesc",
            "DeletedMvRows/Around/probes-in-Btree/RangeIndexAsc",
            "DeletedMvRows/Around/probes-in-Btree/RangeIndexDesc",
            "DeletedMvRows/Around/probes-in-Btree/RangeDescIndexAsc",
            "DeletedMvRows/Around/probes-in-Btree/RangeDescIndexDesc",
            "DeletedMvRows/Around/probes-in-Btree/UpdateIndexKey",
            "DeletedMvRows/Around/probes-in-Btree/DeleteThroughIndex",
            "UpdatedIndexKeys/Before/probes-in-MvStore/RangeRowidDesc",
            "UpdatedIndexKeys/Before/probes-in-MvStore/RangeIndexDesc",
            "UpdatedIndexKeys/Before/probes-in-MvStore/RangeDescIndexDesc",
            "UpdatedIndexKeys/After/probes-in-Btree/EqualityLoop",
            "UpdatedIndexKeys/After/probes-in-Btree/RangeIndexAsc",
            "UpdatedIndexKeys/After/probes-in-Btree/RangeDescIndexAsc",
            "UpdatedIndexKeys/After/probes-in-Btree/UpdateIndexKey",
            "UpdatedIndexKeys/After/probes-in-Btree/DeleteThroughIndex",
            "UpdatedIndexKeys/After/probes-in-MvStore/EqualityLoop",
            "UpdatedIndexKeys/After/probes-in-MvStore/RangeRowidAsc",
            "UpdatedIndexKeys/After/probes-in-MvStore/RangeIndexAsc",
            "UpdatedIndexKeys/After/probes-in-MvStore/RangeDescIndexAsc",
            "UpdatedIndexKeys/Around/probes-in-Btree/EqualityLoop",
            "UpdatedIndexKeys/Around/probes-in-Btree/RangeIndexAsc",
            "UpdatedIndexKeys/Around/probes-in-Btree/RangeDescIndexAsc",
            "UpdatedIndexKeys/Around/probes-in-Btree/UpdateIndexKey",
            "UpdatedIndexKeys/Around/probes-in-Btree/DeleteThroughIndex",
            "UpdatedIndexKeys/Around/probes-in-MvStore/EqualityLoop",
            "UpdatedIndexKeys/Around/probes-in-MvStore/RangeRowidAsc",
            "UpdatedIndexKeys/Around/probes-in-MvStore/RangeRowidDesc",
            "UpdatedIndexKeys/Around/probes-in-MvStore/RangeIndexAsc",
            "UpdatedIndexKeys/Around/probes-in-MvStore/RangeIndexDesc",
            "UpdatedIndexKeys/Around/probes-in-MvStore/RangeDescIndexAsc",
            "UpdatedIndexKeys/Around/probes-in-MvStore/RangeDescIndexDesc",
            "Uncommitted/Before/probes-in-Btree/RangeRowidDesc",
            "Uncommitted/Before/probes-in-Btree/RangeIndexDesc",
            "Uncommitted/Before/probes-in-Btree/RangeDescIndexDesc",
            "Uncommitted/After/probes-in-Btree/EqualityLoop",
            "Uncommitted/After/probes-in-Btree/RangeRowidAsc",
            "Uncommitted/After/probes-in-Btree/RangeIndexAsc",
            "Uncommitted/After/probes-in-Btree/RangeDescIndexAsc",
            "Uncommitted/After/probes-in-Btree/UpdateIndexKey",
            "Uncommitted/After/probes-in-Btree/DeleteThroughIndex",
            "Uncommitted/Around/probes-in-Btree/EqualityLoop",
            "Uncommitted/Around/probes-in-Btree/RangeRowidAsc",
            "Uncommitted/Around/probes-in-Btree/RangeRowidDesc",
            "Uncommitted/Around/probes-in-Btree/RangeIndexAsc",
            "Uncommitted/Around/probes-in-Btree/RangeIndexDesc",
            "Uncommitted/Around/probes-in-Btree/RangeDescIndexAsc",
            "Uncommitted/Around/probes-in-Btree/RangeDescIndexDesc",
            "Uncommitted/Around/probes-in-Btree/UpdateIndexKey",
            "Uncommitted/Around/probes-in-Btree/DeleteThroughIndex",
            "CommittedAfterSnapshot/Before/probes-in-Btree/RangeRowidDesc",
            "CommittedAfterSnapshot/Before/probes-in-Btree/RangeIndexDesc",
            "CommittedAfterSnapshot/Before/probes-in-Btree/RangeDescIndexDesc",
            "CommittedAfterSnapshot/After/probes-in-Btree/EqualityLoop",
            "CommittedAfterSnapshot/After/probes-in-Btree/RangeRowidAsc",
            "CommittedAfterSnapshot/After/probes-in-Btree/RangeIndexAsc",
            "CommittedAfterSnapshot/After/probes-in-Btree/RangeDescIndexAsc",
            "CommittedAfterSnapshot/After/probes-in-Btree/UpdateIndexKey",
            "CommittedAfterSnapshot/After/probes-in-Btree/DeleteThroughIndex",
            "CommittedAfterSnapshot/Around/probes-in-Btree/EqualityLoop",
            "CommittedAfterSnapshot/Around/probes-in-Btree/RangeRowidAsc",
            "CommittedAfterSnapshot/Around/probes-in-Btree/RangeRowidDesc",
            "CommittedAfterSnapshot/Around/probes-in-Btree/RangeIndexAsc",
            "CommittedAfterSnapshot/Around/probes-in-Btree/RangeIndexDesc",
            "CommittedAfterSnapshot/Around/probes-in-Btree/RangeDescIndexAsc",
            "CommittedAfterSnapshot/Around/probes-in-Btree/RangeDescIndexDesc",
            "CommittedAfterSnapshot/Around/probes-in-Btree/UpdateIndexKey",
            "CommittedAfterSnapshot/Around/probes-in-Btree/DeleteThroughIndex",
            "replay/CompositePk/NonUnique/Delete/Random",
            "replay/CompositePk/NonUnique/DeleteThenInsert/Random",
            "replay/CompositePk/NonUnique/UpdateSameIndexValue/KeyOrder",
            "replay/CompositePk/NonUnique/UpdateSameIndexValue/Random",
            "replay/CompositePk/Unique/Delete/Random",
            "replay/CompositePk/Unique/DeleteThenInsert/Random",
            "replay/CompositePk/Unique/UpdateSameIndexValue/KeyOrder",
            "replay/CompositePk/Unique/UpdateSameIndexValue/Random",
            "replay/CompositePk/NullableUnique/Delete/Random",
            "replay/CompositePk/NullableUnique/DeleteThenInsert/Random",
            "replay/CompositePk/NullableUnique/UpdateSameIndexValue/KeyOrder",
            "replay/CompositePk/NullableUnique/UpdateSameIndexValue/Random",
        ],
    ),
    (
        "An exact (GE eq_only) seek on a full index key, as IdxInsert does, finds no \
         visible MvStore entry and finds a B-tree entry at the key that MVCC hides. The \
         cursor then steps the B-tree past the key until it finds a visible entry: for an \
         index key it does not stop at the end of the key (only exact rowid seeks do). A \
         replay that deletes and inserts the same index key does this at each change, and \
         walks all the hidden B-tree entries after the key: the entries of the rows that \
         it deleted before (DeleteThenInsert), or the entries of the rows that an earlier \
         pull rewrote and no checkpoint moved to the B-tree yet (pull prior-Committed, in \
         the four indexes whose values do not change). A real sync pull replays updates \
         as all the deletes, then all the inserts (RecordedPull), so it does this in all \
         the indexes at each change.",
        &[
            "pull/Indexed/RecordedPull/KeyOrder/prior-None",
            "pull/Indexed/RecordedPull/KeyOrder/prior-Committed",
            "pull/Indexed/RecordedPull/KeyOrder/prior-Checkpointed",
            "pull/IndexedGenerated/RecordedPull/KeyOrder/prior-None",
            "replay/Rowid/NonUnique/DeleteThenInsert/KeyOrder",
            "replay/Rowid/Unique/DeleteThenInsert/KeyOrder",
            "replay/Rowid/NullableUnique/DeleteThenInsert/KeyOrder",
            "replay/CompositePk/NonUnique/DeleteThenInsert/KeyOrder",
            "replay/CompositePk/Unique/DeleteThenInsert/KeyOrder",
            "replay/CompositePk/NullableUnique/DeleteThenInsert/KeyOrder",
            "pull/Indexed/FullRowUpsert/KeyOrder/prior-Committed",
        ],
    ),
];

fn known_reason(name: &str) -> Option<&'static str> {
    KNOWN_SUPER_LINEAR
        .iter()
        .find(|(_, cells)| cells.contains(&name))
        .map(|(reason, _)| *reason)
}

fn query_cell_name(
    shape: Shape,
    placement: Placement,
    source: ProbeSource,
    query: Query,
) -> String {
    format!("{shape:?}/{placement:?}/probes-in-{source:?}/{query:?}")
}

fn replay_cell_name(
    table: ReplayTable,
    index: SecondaryIndex,
    change: Change,
    order: Order,
) -> String {
    format!("replay/{table:?}/{index:?}/{change:?}/{order:?}")
}

/// Checks one cell. Returns a line for the report, and a failure if the cell does not
/// match the table.
fn check_cell(
    name: &str,
    class: Class,
    small: &CursorWork,
    large: &CursorWork,
    failures: &mut Vec<String>,
    report: &mut String,
) {
    let bad = violations(class, small, large);
    let known = known_reason(name);
    let status = match (bad.is_empty(), known) {
        (true, None) => "ok",
        (false, Some(_)) => "known",
        (false, None) => {
            failures.push(format!(
                "{name}: grows faster than {class:?}: {}",
                bad.join(", ")
            ));
            "FAIL"
        }
        (true, Some(_)) => {
            failures.push(format!(
                "{name}: listed in KNOWN_SUPER_LINEAR but grows within {class:?}; remove it"
            ));
            "FIXED"
        }
    };
    let _ = writeln!(
        report,
        "{status:5} {name} ({class:?}) entries {}+{} -> {}+{}, seeks {}+{} -> {}+{}, checks {} -> {} {}",
        small.mvcc_entries_examined,
        small.btree_rows_examined,
        large.mvcc_entries_examined,
        large.btree_rows_examined,
        small.mvcc_seeks,
        small.btree_seeks,
        large.mvcc_seeks,
        large.btree_seeks,
        small.visibility_checks,
        large.visibility_checks,
        bad.join(", "),
    );
}

/// Prints the report if `MVCC_COST_REPORT` is set, and fails with the failures.
fn finish(report: &str, failures: &[String]) {
    if std::env::var_os("MVCC_COST_REPORT").is_some() {
        eprintln!("{report}");
    }
    assert!(
        failures.is_empty(),
        "{} cells do not match the table:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

#[test]
fn test_mvcc_query_cost_scaling() {
    let mut failures = Vec::new();
    let mut report = String::new();
    for shape in SHAPES {
        for placement in PLACEMENTS {
            for source in PROBE_SOURCES {
                let small = measure_state(shape, placement, source, N);
                let large = measure_state(shape, placement, source, 4 * N);
                for (i, &query) in QUERIES.iter().enumerate() {
                    let name = query_cell_name(shape, placement, source, query);
                    check_cell(
                        &name,
                        query_class(query),
                        &small[i],
                        &large[i],
                        &mut failures,
                        &mut report,
                    );
                }
            }
        }
    }
    finish(&report, &failures);
}

#[test]
fn test_mvcc_replay_cost_scaling() {
    let mut failures = Vec::new();
    let mut report = String::new();
    for table in REPLAY_TABLES {
        for index in SECONDARY_INDEXES {
            let measure = |n: usize| {
                let (_db, conn) = build_replay_base(table, index, n);
                let mut work = Vec::new();
                for &(change, _) in REPLAY_CELLS {
                    for order in ORDERS {
                        work.push(measure_replay(&conn, table, index, change, order, n));
                    }
                }
                work
            };
            let small = measure(N);
            let large = measure(4 * N);
            let mut i = 0;
            for &(change, class) in REPLAY_CELLS {
                for order in ORDERS {
                    let name = replay_cell_name(table, index, change, order);
                    check_cell(
                        &name,
                        class,
                        &small[i],
                        &large[i],
                        &mut failures,
                        &mut report,
                    );
                    i += 1;
                }
            }
        }
    }
    finish(&report, &failures);
}

#[test]
fn test_mvcc_pull_cost_scaling() {
    let mut failures = Vec::new();
    let mut report = String::new();
    for table in [PullTable::Indexed, PullTable::IndexedGenerated] {
        let cells = PULL_CELLS
            .iter()
            .filter(|(cell_table, _, _)| *cell_table == table)
            .flat_map(|&(_, change, prior)| ORDERS.map(|order| (change, order, prior)))
            .collect::<Vec<_>>();
        let measure = |n: usize| {
            let (_db, base) = build_pull_base(table, n);
            cells
                .iter()
                .map(|&(change, order, prior)| measure_pull(&base, table, change, order, prior, n))
                .collect::<Vec<_>>()
        };
        let small = measure(N);
        let large = measure(4 * N);
        for (i, &(change, order, prior)) in cells.iter().enumerate() {
            let name = pull_cell_name(table, change, order, prior);
            check_cell(
                &name,
                Class::Linear,
                &small[i],
                &large[i],
                &mut failures,
                &mut report,
            );
        }
    }
    finish(&report, &failures);
}
