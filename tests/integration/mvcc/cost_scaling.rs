//! Tests that the cursor work of MVCC queries grows within its complexity class.
//!
//! An MVCC cursor merges the B-tree with the MvStore. Entries that the transaction cannot
//! see (tombstones, shadowed B-tree rows, uncommitted rows, rows committed after the
//! snapshot) stay in the sources, and a cursor must step over them. A cursor that walks
//! them at each probe makes a loop of probes quadratic. CPU time on ordinary data does not
//! show this, and the opcode counters count one `Next` once however many entries it walks.
//!
//! These tests read the cursor work counters of each statement
//! ([`turso_core::CursorWork`], debug builds only): the raw MvStore entries and B-tree rows that cursors
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
//!   placed before, after or around the probe rows, and 34 queries. The indexes include a
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
//! cells that it fixes from the list. A cell whose work is linear in the hidden entries by
//! design is listed in [`HIDDEN_AT_EDGE`] and checked as [`Class::Linear`].

//!
//! Run with `MVCC_COST_REPORT=1` and `--nocapture` to print the counts of each cell.

use super::test_db::MvccTestDbNoConn;
use rand::seq::SliceRandom;
use rand_chacha::rand_core::SeedableRng;
use rand_chacha::ChaCha8Rng;
use std::collections::BTreeMap;
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
fn counters(work: &CursorWork) -> [(&'static str, u64); 6] {
    [
        ("mvcc_entries", work.mvcc_entries_examined),
        ("btree_rows", work.btree_rows_examined),
        ("mvcc_seeks", work.mvcc_seeks),
        ("btree_seeks", work.btree_seeks),
        ("visibility_checks", work.visibility_checks),
        ("mvcc_point_lookups", work.mvcc_point_lookups),
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
/// The `u` and `k` of the rows that [`Query::InsertAutoRowid`] inserts: above all others.
const AUTO_KEYS: i64 = 4_000_000;
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
    FullIndexScanDesc,
    CountAll,
    MaxRowid,
    MinRowid,
    MaxIndex,
    MinIndex,
    LimitRowidDesc,
    LimitIndexAsc,
    LimitIndexDesc,
    IntegrityCheck,
    InsertUnique,
    UpdateThroughIndex,
    UpdateIndexKey,
    DeleteThroughIndex,
    InsertAutoRowid,
}

const QUERIES: [Query; 34] = [
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
    Query::FullIndexScanDesc,
    Query::CountAll,
    Query::MaxRowid,
    Query::MinRowid,
    Query::MaxIndex,
    Query::MinIndex,
    Query::LimitRowidDesc,
    Query::LimitIndexAsc,
    Query::LimitIndexDesc,
    Query::IntegrityCheck,
    Query::InsertUnique,
    Query::UpdateThroughIndex,
    Query::UpdateIndexKey,
    Query::DeleteThroughIndex,
    Query::InsertAutoRowid,
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
    (Query::FullIndexScanDesc, Class::Linear),
    (Query::CountAll, Class::Linear),
    (Query::MaxRowid, Class::Constant),
    (Query::MinRowid, Class::Constant),
    (Query::MaxIndex, Class::Constant),
    (Query::MinIndex, Class::Constant),
    (Query::LimitRowidDesc, Class::Constant),
    (Query::LimitIndexAsc, Class::Constant),
    (Query::LimitIndexDesc, Class::Constant),
    (Query::IntegrityCheck, Class::Linear),
    (Query::InsertUnique, Class::Constant),
    (Query::UpdateThroughIndex, Class::Constant),
    (Query::UpdateIndexKey, Class::Constant),
    (Query::DeleteThroughIndex, Class::Constant),
    (Query::InsertAutoRowid, Class::Constant),
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
        Query::FullIndexScanDesc => ("SELECT k FROM t ORDER BY k DESC".into(), vec![], None),
        Query::CountAll => ("SELECT count(*) FROM t".into(), vec![], Some(1)),
        Query::MaxRowid => ("SELECT max(id) FROM t".into(), vec![], Some(1)),
        Query::MinRowid => ("SELECT min(id) FROM t".into(), vec![], Some(1)),
        Query::MaxIndex => ("SELECT max(k) FROM t".into(), vec![], Some(1)),
        Query::MinIndex => ("SELECT min(k) FROM t".into(), vec![], Some(1)),
        Query::LimitRowidDesc => (
            "SELECT id FROM t ORDER BY id DESC LIMIT 1".into(),
            vec![],
            Some(1),
        ),
        Query::LimitIndexAsc => ("SELECT k FROM t ORDER BY k LIMIT 1".into(), vec![], Some(1)),
        Query::LimitIndexDesc => (
            "SELECT k FROM t ORDER BY k DESC LIMIT 1".into(),
            vec![],
            Some(1),
        ),
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
        // New rows without an id: the cursor allocates the rowid after the largest one.
        Query::InsertAutoRowid => (
            "INSERT INTO t(u, k, v) VALUES (?, ?, 'a')".into(),
            (0..PROBES as i64)
                .map(|i| vec![int(AUTO_KEYS + i), int(AUTO_KEYS + i)])
                .collect(),
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
const KNOWN_SUPER_LINEAR: &[(&str, &[&str])] = &[];

/// Cells whose work is linear in the hidden entries of the state, by design. To find the
/// first visible entry from one end of a table or index (max(), min(), ORDER BY .. LIMIT
/// 1, and the first rowid allocation of a table, which reads the largest visible rowid), a
/// cursor must step over all the hidden entries at that end. No cursor can skip them
/// without a visibility check:
///
/// - Uncommitted rows and rows committed after the snapshot are hidden only from this
///   snapshot. They end when the other transaction ends or the snapshot ends.
/// - Deleted rows (Tombstones, DeletedMvRows, the old keys of UpdatedIndexKeys) stay until
///   a checkpoint or GC removes them.
///
/// These cells use [`Class::Linear`]. A cell that grows within its own class must be
/// removed from this list.
const HIDDEN_AT_EDGE: &[&str] = &[
    "Tombstones/Before/probes-in-Btree/MinRowid",
    "Tombstones/Before/probes-in-Btree/MinIndex",
    "Tombstones/Before/probes-in-Btree/LimitIndexAsc",
    "Tombstones/Before/probes-in-MvStore/MinRowid",
    "Tombstones/Before/probes-in-MvStore/MinIndex",
    "Tombstones/Before/probes-in-MvStore/LimitIndexAsc",
    "Tombstones/After/probes-in-Btree/MaxRowid",
    "Tombstones/After/probes-in-Btree/MaxIndex",
    "Tombstones/After/probes-in-Btree/LimitRowidDesc",
    "Tombstones/After/probes-in-Btree/LimitIndexDesc",
    "Tombstones/After/probes-in-MvStore/MaxRowid",
    "Tombstones/After/probes-in-MvStore/MaxIndex",
    "Tombstones/After/probes-in-MvStore/LimitRowidDesc",
    "Tombstones/After/probes-in-MvStore/LimitIndexDesc",
    "Tombstones/Around/probes-in-Btree/MaxRowid",
    "Tombstones/Around/probes-in-Btree/MinRowid",
    "Tombstones/Around/probes-in-Btree/MaxIndex",
    "Tombstones/Around/probes-in-Btree/MinIndex",
    "Tombstones/Around/probes-in-Btree/LimitRowidDesc",
    "Tombstones/Around/probes-in-Btree/LimitIndexAsc",
    "Tombstones/Around/probes-in-Btree/LimitIndexDesc",
    "Tombstones/Around/probes-in-MvStore/MaxRowid",
    "Tombstones/Around/probes-in-MvStore/MinRowid",
    "Tombstones/Around/probes-in-MvStore/MaxIndex",
    "Tombstones/Around/probes-in-MvStore/MinIndex",
    "Tombstones/Around/probes-in-MvStore/LimitRowidDesc",
    "Tombstones/Around/probes-in-MvStore/LimitIndexAsc",
    "Tombstones/Around/probes-in-MvStore/LimitIndexDesc",
    "DeletedMvRows/Before/probes-in-Btree/MinRowid",
    "DeletedMvRows/Before/probes-in-Btree/MinIndex",
    "DeletedMvRows/Before/probes-in-Btree/LimitIndexAsc",
    "DeletedMvRows/Before/probes-in-MvStore/MinRowid",
    "DeletedMvRows/Before/probes-in-MvStore/MinIndex",
    "DeletedMvRows/Before/probes-in-MvStore/LimitIndexAsc",
    "DeletedMvRows/After/probes-in-Btree/MaxRowid",
    "DeletedMvRows/After/probes-in-Btree/MaxIndex",
    "DeletedMvRows/After/probes-in-Btree/LimitRowidDesc",
    "DeletedMvRows/After/probes-in-Btree/LimitIndexDesc",
    "DeletedMvRows/After/probes-in-MvStore/MaxRowid",
    "DeletedMvRows/After/probes-in-MvStore/MaxIndex",
    "DeletedMvRows/After/probes-in-MvStore/LimitRowidDesc",
    "DeletedMvRows/After/probes-in-MvStore/LimitIndexDesc",
    "DeletedMvRows/Around/probes-in-Btree/MaxRowid",
    "DeletedMvRows/Around/probes-in-Btree/MinRowid",
    "DeletedMvRows/Around/probes-in-Btree/MaxIndex",
    "DeletedMvRows/Around/probes-in-Btree/MinIndex",
    "DeletedMvRows/Around/probes-in-Btree/LimitRowidDesc",
    "DeletedMvRows/Around/probes-in-Btree/LimitIndexAsc",
    "DeletedMvRows/Around/probes-in-Btree/LimitIndexDesc",
    "DeletedMvRows/Around/probes-in-MvStore/MaxRowid",
    "DeletedMvRows/Around/probes-in-MvStore/MinRowid",
    "DeletedMvRows/Around/probes-in-MvStore/MaxIndex",
    "DeletedMvRows/Around/probes-in-MvStore/MinIndex",
    "DeletedMvRows/Around/probes-in-MvStore/LimitRowidDesc",
    "DeletedMvRows/Around/probes-in-MvStore/LimitIndexAsc",
    "DeletedMvRows/Around/probes-in-MvStore/LimitIndexDesc",
    "UpdatedIndexKeys/Before/probes-in-Btree/MinIndex",
    "UpdatedIndexKeys/Before/probes-in-Btree/LimitIndexAsc",
    "UpdatedIndexKeys/Before/probes-in-MvStore/MinIndex",
    "UpdatedIndexKeys/Before/probes-in-MvStore/LimitIndexAsc",
    "UpdatedIndexKeys/Around/probes-in-Btree/MinIndex",
    "UpdatedIndexKeys/Around/probes-in-Btree/LimitIndexAsc",
    "UpdatedIndexKeys/Around/probes-in-MvStore/MinIndex",
    "UpdatedIndexKeys/Around/probes-in-MvStore/LimitIndexAsc",
    "Uncommitted/Before/probes-in-Btree/MinRowid",
    "Uncommitted/Before/probes-in-Btree/MinIndex",
    "Uncommitted/Before/probes-in-Btree/LimitIndexAsc",
    "Uncommitted/Before/probes-in-MvStore/MinRowid",
    "Uncommitted/Before/probes-in-MvStore/MinIndex",
    "Uncommitted/Before/probes-in-MvStore/LimitIndexAsc",
    "Uncommitted/After/probes-in-Btree/MaxRowid",
    "Uncommitted/After/probes-in-Btree/MaxIndex",
    "Uncommitted/After/probes-in-Btree/LimitRowidDesc",
    "Uncommitted/After/probes-in-Btree/LimitIndexDesc",
    "Uncommitted/After/probes-in-MvStore/MaxRowid",
    "Uncommitted/After/probes-in-MvStore/MaxIndex",
    "Uncommitted/After/probes-in-MvStore/LimitRowidDesc",
    "Uncommitted/After/probes-in-MvStore/LimitIndexDesc",
    "Uncommitted/Around/probes-in-Btree/MaxRowid",
    "Uncommitted/Around/probes-in-Btree/MinRowid",
    "Uncommitted/Around/probes-in-Btree/MaxIndex",
    "Uncommitted/Around/probes-in-Btree/MinIndex",
    "Uncommitted/Around/probes-in-Btree/LimitRowidDesc",
    "Uncommitted/Around/probes-in-Btree/LimitIndexAsc",
    "Uncommitted/Around/probes-in-Btree/LimitIndexDesc",
    "Uncommitted/Around/probes-in-MvStore/MaxRowid",
    "Uncommitted/Around/probes-in-MvStore/MinRowid",
    "Uncommitted/Around/probes-in-MvStore/MaxIndex",
    "Uncommitted/Around/probes-in-MvStore/MinIndex",
    "Uncommitted/Around/probes-in-MvStore/LimitRowidDesc",
    "Uncommitted/Around/probes-in-MvStore/LimitIndexAsc",
    "Uncommitted/Around/probes-in-MvStore/LimitIndexDesc",
    "CommittedAfterSnapshot/Before/probes-in-Btree/MinRowid",
    "CommittedAfterSnapshot/Before/probes-in-Btree/MinIndex",
    "CommittedAfterSnapshot/Before/probes-in-Btree/LimitIndexAsc",
    "CommittedAfterSnapshot/Before/probes-in-MvStore/MinRowid",
    "CommittedAfterSnapshot/Before/probes-in-MvStore/MinIndex",
    "CommittedAfterSnapshot/Before/probes-in-MvStore/LimitIndexAsc",
    "CommittedAfterSnapshot/After/probes-in-Btree/MaxRowid",
    "CommittedAfterSnapshot/After/probes-in-Btree/MaxIndex",
    "CommittedAfterSnapshot/After/probes-in-Btree/LimitRowidDesc",
    "CommittedAfterSnapshot/After/probes-in-Btree/LimitIndexDesc",
    "CommittedAfterSnapshot/After/probes-in-MvStore/MaxRowid",
    "CommittedAfterSnapshot/After/probes-in-MvStore/MaxIndex",
    "CommittedAfterSnapshot/After/probes-in-MvStore/LimitRowidDesc",
    "CommittedAfterSnapshot/After/probes-in-MvStore/LimitIndexDesc",
    "CommittedAfterSnapshot/Around/probes-in-Btree/MaxRowid",
    "CommittedAfterSnapshot/Around/probes-in-Btree/MinRowid",
    "CommittedAfterSnapshot/Around/probes-in-Btree/MaxIndex",
    "CommittedAfterSnapshot/Around/probes-in-Btree/MinIndex",
    "CommittedAfterSnapshot/Around/probes-in-Btree/LimitRowidDesc",
    "CommittedAfterSnapshot/Around/probes-in-Btree/LimitIndexAsc",
    "CommittedAfterSnapshot/Around/probes-in-Btree/LimitIndexDesc",
    "CommittedAfterSnapshot/Around/probes-in-MvStore/MaxRowid",
    "CommittedAfterSnapshot/Around/probes-in-MvStore/MinRowid",
    "CommittedAfterSnapshot/Around/probes-in-MvStore/MaxIndex",
    "CommittedAfterSnapshot/Around/probes-in-MvStore/MinIndex",
    "CommittedAfterSnapshot/Around/probes-in-MvStore/LimitRowidDesc",
    "CommittedAfterSnapshot/Around/probes-in-MvStore/LimitIndexAsc",
    "CommittedAfterSnapshot/Around/probes-in-MvStore/LimitIndexDesc",
    "Tombstones/After/probes-in-Btree/InsertAutoRowid",
    "Tombstones/After/probes-in-MvStore/InsertAutoRowid",
    "Tombstones/Around/probes-in-Btree/InsertAutoRowid",
    "Tombstones/Around/probes-in-MvStore/InsertAutoRowid",
    "DeletedMvRows/After/probes-in-Btree/InsertAutoRowid",
    "DeletedMvRows/After/probes-in-MvStore/InsertAutoRowid",
    "DeletedMvRows/Around/probes-in-Btree/InsertAutoRowid",
    "DeletedMvRows/Around/probes-in-MvStore/InsertAutoRowid",
    "Uncommitted/After/probes-in-Btree/InsertAutoRowid",
    "Uncommitted/After/probes-in-MvStore/InsertAutoRowid",
    "Uncommitted/Around/probes-in-Btree/InsertAutoRowid",
    "Uncommitted/Around/probes-in-MvStore/InsertAutoRowid",
    "CommittedAfterSnapshot/After/probes-in-Btree/InsertAutoRowid",
    "CommittedAfterSnapshot/After/probes-in-MvStore/InsertAutoRowid",
    "CommittedAfterSnapshot/Around/probes-in-Btree/InsertAutoRowid",
    "CommittedAfterSnapshot/Around/probes-in-MvStore/InsertAutoRowid",
];

/// The work of each query, replay change and pull change, summed over all its cells at
/// size N, in the order of [`counters`]. A change to the cursor must not raise a total above
/// its ceiling: the growth classes do not see a cost that grows by a constant factor, such
/// as one more seek for each probe. When a change lowers a total, the ceiling can stay. Run
/// with `MVCC_COST_REPORT=1` to print the current totals. The ceilings cover the first five
/// counters. Main did not count MvStore point lookups, so they have only a growth class.
const WORK_CEILINGS: &[(&str, [u64; 5])] = &[
    ("CountAll", [12025, 4314, 72, 42, 13428]),
    ("DeleteThroughIndex", [4695, 0, 4704, 0, 4032]),
    ("EqualityLoop", [1392, 1104, 1008, 672, 2016]),
    ("FullIndexScanAsc", [12025, 4314, 72, 42, 13428]),
    ("FullIndexScanDesc", [9114, 4314, 42, 42, 13428]),
    ("FullScanAsc", [7914, 4314, 42, 42, 11028]),
    ("FullScanDesc", [7914, 4314, 42, 42, 11028]),
    ("InsertAutoRowid", [2442, 629, 4074, 4074, 3042]),
    ("InsertUnique", [0, 0, 4704, 4704, 672]),
    ("IntegrityCheck", [87891, 37704, 20752, 8076, 110453]),
    ("JoinNonUnique", [903, 1680, 1050, 714, 2016]),
    ("JoinRowid", [375, 1008, 714, 378, 1344]),
    ("JoinUnique", [711, 1680, 1386, 714, 2016]),
    ("LimitIndexAsc", [4247, 1228, 67, 42, 4242]),
    ("LimitIndexDesc", [2435, 629, 42, 42, 3042]),
    ("LimitRowidDesc", [2435, 629, 42, 42, 3042]),
    ("MaxIndex", [2435, 629, 42, 42, 3042]),
    ("MaxRowid", [2435, 629, 42, 42, 3042]),
    ("MinIndex", [4247, 1228, 67, 42, 4242]),
    ("MinRowid", [2435, 629, 42, 42, 3042]),
    ("PointDescIndex", [864, 1008, 1008, 672, 1344]),
    ("PointNocase", [864, 1008, 1008, 672, 1344]),
    ("PointNonUnique", [864, 1008, 1008, 672, 1344]),
    ("PointNullPrefix", [864, 1008, 1008, 672, 1344]),
    ("PointRowid", [336, 336, 672, 336, 672]),
    ("PointUnique", [672, 1008, 1344, 672, 1344]),
    ("RangeDescIndexAsc", [705, 699, 42, 42, 1386]),
    ("RangeDescIndexDesc", [717, 699, 63, 42, 1386]),
    ("RangeIndexAsc", [717, 699, 63, 42, 1386]),
    ("RangeIndexDesc", [705, 699, 42, 42, 1386]),
    ("RangeRowidAsc", [705, 699, 42, 42, 1386]),
    ("RangeRowidDesc", [705, 699, 42, 42, 1386]),
    ("UpdateIndexKey", [3351, 0, 4704, 1344, 2688]),
    ("UpdateThroughIndex", [672, 1008, 1344, 672, 1344]),
    ("pull/FullRowUpsert", [13935, 14000, 19200, 16800, 38735]),
    ("pull/RecordedPull", [13949, 13752, 19248, 15240, 29299]),
    ("pull/UpdateChangedColumns", [3200, 1600, 3600, 2800, 4800]),
    ("replay/Delete", [5577, 7200, 7200, 6000, 12189]),
    (
        "replay/DeleteThenInsert",
        [12645, 10788, 14388, 11988, 23247],
    ),
    ("replay/InsertOrReplace", [10881, 6206, 13188, 13188, 22093]),
    ("replay/UpdateIndexed", [3688, 7612, 10788, 9588, 10512]),
    (
        "replay/UpdateSameIndexValue",
        [14547, 9600, 9600, 8400, 23544],
    ),
    ("replay/UpdateUnindexed", [378, 4800, 3600, 3600, 3978]),
    ("replay/Upsert", [13188, 7612, 11988, 11988, 29612]),
    (
        "replay/UpsertSameIndexValue",
        [12594, 9600, 10800, 10800, 33588],
    ),
];

/// The [`WORK_CEILINGS`] entry of a cell: its query, replay change or pull change.
fn ceiling_group(name: &str) -> String {
    let parts = name.split('/').collect::<Vec<_>>();
    match parts[0] {
        "replay" => format!("replay/{}", parts[3]),
        "pull" => format!("pull/{}", parts[2]),
        _ => parts[parts.len() - 1].to_string(),
    }
}

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
    totals: &mut BTreeMap<String, CursorWork>,
) {
    totals.entry(ceiling_group(name)).or_default().merge(small);
    let class = if HIDDEN_AT_EDGE.contains(&name) {
        if violations(class, small, large).is_empty() {
            failures.push(format!(
                "{name}: listed in HIDDEN_AT_EDGE but grows within {class:?}; remove it"
            ));
        }
        Class::Linear
    } else {
        class
    };
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
fn finish(report: &str, failures: &[String], totals: &BTreeMap<String, CursorWork>) {
    if std::env::var_os("MVCC_COST_REPORT").is_some() {
        eprintln!("{report}");
    }
    let mut failures = failures.to_vec();
    let mut table = String::new();
    for (group, work) in totals {
        let counts = counters(work).map(|(_, count)| count);
        let _ = writeln!(table, "    (\"{group}\", {counts:?}),");
        match WORK_CEILINGS.iter().find(|(name, _)| name == group) {
            Some((_, ceiling)) => {
                for ((name, count), max) in counters(work).into_iter().zip(ceiling) {
                    if count > *max {
                        failures.push(format!(
                            "{group}: {name} is {count} in total, above its ceiling {max}"
                        ));
                    }
                }
            }
            None => failures.push(format!("{group}: no entry in WORK_CEILINGS")),
        }
    }
    if std::env::var_os("MVCC_COST_REPORT").is_some() || !failures.is_empty() {
        eprintln!("Current totals for WORK_CEILINGS:\n{table}");
    }
    assert!(
        failures.is_empty(),
        "{} checks failed:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

#[test]
fn test_mvcc_query_cost_scaling() {
    let mut failures = Vec::new();
    let mut report = String::new();
    let mut totals = BTreeMap::new();
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
                        &mut totals,
                    );
                    if source == ProbeSource::MvStore && READS_ONE_MVSTORE_ROW.contains(&query) {
                        for work in [&small[i], &large[i]] {
                            // A join may rewind its outer table once.
                            if work.btree_seeks > 1 {
                                failures.push(format!(
                                    "{name}: seeks the B-tree {} times, but the MvStore holds \
                                     the only row of each probe",
                                    work.btree_seeks
                                ));
                            }
                        }
                    }
                }
            }
        }
    }
    finish(&report, &failures, &totals);
}

/// Queries whose probes each match at most one row (a rowid or a UNIQUE key). When the
/// MvStore holds a visible row with that key, the B-tree cannot hold another visible match,
/// so the probe must not seek the B-tree. The `Next` of a UNIQUE index loop does not seek it
/// either: the loop keeps the bound of its seek.
const READS_ONE_MVSTORE_ROW: [Query; 4] = [
    Query::PointRowid,
    Query::PointUnique,
    Query::JoinRowid,
    Query::JoinUnique,
];

#[test]
fn test_mvcc_replay_cost_scaling() {
    let mut failures = Vec::new();
    let mut report = String::new();
    let mut totals = BTreeMap::new();
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
                        &mut totals,
                    );
                    i += 1;
                }
            }
        }
    }
    finish(&report, &failures, &totals);
}

#[test]
fn test_mvcc_pull_cost_scaling() {
    let mut failures = Vec::new();
    let mut report = String::new();
    let mut totals = BTreeMap::new();
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
                &mut totals,
            );
        }
    }
    finish(&report, &failures, &totals);
}

/// The counters count each MvStore iterator once when it is made and each entry that it
/// returns once, whichever cursor operation uses it.
#[test]
fn test_cursor_work_counts_mvstore_iterators_exactly() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    run(&conn, "PRAGMA mvcc_checkpoint_threshold = -1");
    run(&conn, "CREATE TABLE t(id INTEGER PRIMARY KEY, k INTEGER)");
    run(&conn, "CREATE INDEX t_k ON t(k)");
    run(
        &conn,
        "INSERT INTO t SELECT value, value FROM generate_series(1, 34)",
    );
    // The rows of all tables share one map, so a table scan also reads the first entry
    // of the next table, to see that its own table ended. An index has a map of its own.
    for (sql, entries) in [
        ("SELECT id FROM t", 34..=35),
        ("SELECT k FROM t ORDER BY k", 34..=34),
    ] {
        let (rows, work) = run_measured(&conn, sql, &[]);
        assert_eq!(rows.len(), 34);
        assert_eq!(work.mvcc_seeks, 1, "`{sql}`: {work:?}");
        assert!(
            entries.contains(&work.mvcc_entries_examined),
            "`{sql}`: {work:?}"
        );
    }
}

/// Runs `sql` at state size N and 4N with `build`, and returns the work of each run.
fn work_at_n_and_4n(
    build: impl Fn(usize) -> (MvccTestDbNoConn, Arc<Connection>),
    sql: &str,
) -> (CursorWork, CursorWork) {
    let mut works = [N, 4 * N].map(|n| {
        let (_db, conn) = build(n);
        run_measured(&conn, sql, &[]).1
    });
    let large = std::mem::take(&mut works[1]);
    let small = std::mem::take(&mut works[0]);
    (small, large)
}

/// An index seek that walks B-tree entries whose MvStore chains hide them must decide each
/// one from the chain that the MvStore head holds. A skiplist lookup for each entry made a
/// job queue query (`WHERE status = 'pending'` after the jobs moved to 'done') 4.5 times
/// slower than main.
#[test]
fn test_index_seek_over_hidden_btree_entries_does_no_point_lookups() {
    let moved_keys = |n: usize| {
        let db = MvccTestDbNoConn::new_with_random_db();
        let conn = db.connect();
        run(&conn, "PRAGMA mvcc_checkpoint_threshold = -1");
        run(
            &conn,
            "CREATE TABLE t(id INTEGER PRIMARY KEY, k INT, v TEXT)",
        );
        run(&conn, "CREATE INDEX t_k ON t(k)");
        run(
            &conn,
            &format!("INSERT INTO t SELECT value, 7, 'a' FROM generate_series(1, {n})"),
        );
        run(
            &conn,
            &format!(
                "INSERT INTO t SELECT value + {n}, value % 50 + 100, 'b' \
                 FROM generate_series(1, 100)"
            ),
        );
        run(&conn, "PRAGMA wal_checkpoint(TRUNCATE)");
        run(&conn, &format!("UPDATE t SET k = 8 WHERE id < {n}"));
        (db, conn)
    };
    let deleted_rows = |n: usize| {
        let (db, conn) = moved_keys(n);
        run(&conn, &format!("DELETE FROM t WHERE id <= {n}"));
        (db, conn)
    };
    let eq_seek = "SELECT count(*) FROM t INDEXED BY t_k WHERE k = 7";
    let range_seek = "SELECT k FROM t INDEXED BY t_k WHERE k > 0 ORDER BY k LIMIT 1";
    for (sql, (small, large)) in [
        (eq_seek, work_at_n_and_4n(moved_keys, eq_seek)),
        (range_seek, work_at_n_and_4n(deleted_rows, range_seek)),
    ] {
        assert!(
            Class::Constant.allows(small.mvcc_point_lookups, large.mvcc_point_lookups),
            "`{sql}`: MvStore point lookups grow with the hidden entries: {small:?} -> {large:?}"
        );
    }
}
