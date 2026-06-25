use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[cfg(not(feature = "codspeed"))]
use criterion::{
    async_executor::FuturesExecutor, criterion_group, criterion_main, Criterion, Throughput,
};
#[cfg(not(feature = "codspeed"))]
use pprof::criterion::{Output, PProfProfiler};

#[cfg(feature = "codspeed")]
use codspeed_criterion_compat::{
    async_executor::FuturesExecutor, criterion_group, criterion_main, Criterion, Throughput,
};

use turso_core::mvcc::clock::MvccClock;
use turso_core::mvcc::database::{MvStore, Row, RowID, RowKey};
use turso_core::types::{IOResult, ImmutableRecord, Text};
use turso_core::{Connection, Database, MemoryIO, Statement, StepResult, Value};

struct BenchDb {
    _db: Arc<Database>,
    conn: Arc<Connection>,
    mvcc_store: Arc<MvStore<MvccClock>>,
}

fn bench_db() -> BenchDb {
    let io = Arc::new(MemoryIO::new());
    let db = Database::open_file(io, ":memory:").unwrap();
    let conn = db.connect().unwrap();
    // Enable MVCC via PRAGMA
    conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
    let mvcc_store = db.get_mv_store().clone().unwrap();
    BenchDb {
        _db: db,
        conn,
        mvcc_store,
    }
}

#[turso_macros::codspeed_criterion_benchmark]
fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("mvcc-ops-throughput");
    group.throughput(Throughput::Elements(1));

    group.bench_function("begin_tx + rollback_tx", |b| {
        let db = bench_db();
        b.to_async(FuturesExecutor).iter(|| async {
            let conn = db.conn.clone();
            let tx_id = db.mvcc_store.begin_tx(conn.get_pager()).unwrap();
            db.mvcc_store
                .rollback_tx(tx_id, conn.get_pager(), &conn, turso_core::MAIN_DB_ID);
        })
    });

    let db = bench_db();
    group.bench_function("begin_tx + commit_tx", |b| {
        b.to_async(FuturesExecutor).iter(|| async {
            let conn = &db.conn;
            let tx_id = db.mvcc_store.begin_tx(conn.get_pager()).unwrap();
            let mv_store = &db.mvcc_store;
            let mut sm = mv_store
                .commit_tx(tx_id, conn, turso_core::MAIN_DB_ID)
                .unwrap();
            // TODO: sync IO hack
            loop {
                let res = sm.step(mv_store).unwrap();
                match res {
                    IOResult::IO(io) => io.wait(db._db.io.as_ref()).unwrap(),
                    IOResult::Done(_) => break,
                }
            }
        })
    });

    let db = bench_db();
    group.bench_function("begin_tx-read-commit_tx", |b| {
        b.to_async(FuturesExecutor).iter(|| async {
            let conn = &db.conn;
            let tx_id = db.mvcc_store.begin_tx(conn.get_pager()).unwrap();
            db.mvcc_store
                .read(
                    tx_id,
                    &RowID {
                        table_id: (-2).into(),
                        row_id: RowKey::Int(1),
                    },
                )
                .unwrap();
            let mv_store = &db.mvcc_store;
            let mut sm = mv_store
                .commit_tx(tx_id, conn, turso_core::MAIN_DB_ID)
                .unwrap();
            // TODO: sync IO hack
            loop {
                let res = sm.step(mv_store).unwrap();
                match res {
                    IOResult::IO(io) => io.wait(db._db.io.as_ref()).unwrap(),
                    IOResult::Done(_) => break,
                }
            }
        })
    });

    let db = bench_db();
    let record = ImmutableRecord::from_values(&vec![Value::Text(Text::new("World"))], 1).unwrap();
    let record_data = record.as_blob();
    group.bench_function("begin_tx-update-commit_tx", |b| {
        b.to_async(FuturesExecutor).iter(|| async {
            let conn = &db.conn;
            let tx_id = db.mvcc_store.begin_tx(conn.get_pager()).unwrap();
            db.mvcc_store
                .update(
                    tx_id,
                    Row::new_table_row(RowID::new((-2).into(), RowKey::Int(1)), record_data, 1)
                        .unwrap(),
                )
                .unwrap();
            let mv_store = &db.mvcc_store;
            let mut sm = mv_store
                .commit_tx(tx_id, conn, turso_core::MAIN_DB_ID)
                .unwrap();
            // TODO: sync IO hack
            loop {
                let res = sm.step(mv_store).unwrap();
                match res {
                    IOResult::IO(io) => io.wait(db._db.io.as_ref()).unwrap(),
                    IOResult::Done(_) => break,
                }
            }
        })
    });

    let db = bench_db();
    let tx_id = db.mvcc_store.begin_tx(db.conn.get_pager()).unwrap();
    db.mvcc_store
        .insert(
            tx_id,
            Row::new_table_row(RowID::new((-2).into(), RowKey::Int(1)), record_data, 1).unwrap(),
        )
        .unwrap();
    group.bench_function("read", |b| {
        b.to_async(FuturesExecutor).iter(|| async {
            db.mvcc_store
                .read(
                    tx_id,
                    &RowID {
                        table_id: (-2).into(),
                        row_id: RowKey::Int(1),
                    },
                )
                .unwrap();
        })
    });

    let db = bench_db();
    let tx_id = db.mvcc_store.begin_tx(db.conn.get_pager()).unwrap();
    db.mvcc_store
        .insert(
            tx_id,
            Row::new_table_row(RowID::new((-2).into(), RowKey::Int(1)), record_data, 1).unwrap(),
        )
        .unwrap();
    group.bench_function("update", |b| {
        b.to_async(FuturesExecutor).iter(|| async {
            db.mvcc_store
                .update(
                    tx_id,
                    Row::new_table_row(RowID::new((-2).into(), RowKey::Int(1)), record_data, 1)
                        .unwrap(),
                )
                .unwrap();
        })
    });
}

// ---------------------------------------------------------------------------
// "Huge multi-write" batch-insert benchmark.
//
// Models the shape of a workload that hit a pathological MVCC slowdown: a single
// batch `INSERT ... SELECT` that joins a `VALUES` list of new rows against a
// one-row `metadata` table and inserts them into a table with a UNIQUE index,
// with `RETURNING`. The hot path is a per-row eq-only probe (`NoConflict` on the
// UNIQUE `seq` index, and `NotExists` on the INTEGER PRIMARY KEY rowid).
//
// The `pending_invisible` parameter opens a second connection that holds an
// uncommitted `BEGIN CONCURRENT` transaction full of entries that are invisible
// to the measured connection — reproducing concurrent in-flight batch inserts.
// Before the eq-only seek bound was added, each probe scanned O(pending) invisible
// skiplist entries, so the benchmark's time grew with `pending`; after the fix it
// should stay roughly flat.
//
// Two variants isolate the two seek paths via `GhostPlacement`:
// - `IndexProbe`  (`mvcc-huge-multi-write`): ghost rows have LOW rowids and HIGH
//   `seq`, so only the eq-only *index* probe (`seek_index`) walks toward them.
// - `RowidProbe`  (`mvcc-huge-multi-write-rowid`): ghost rows have HIGH rowids and
//   LOW `seq`, so only the eq-only *table-rowid* probe (`seek_rowid`) walks toward
//   them.
// ---------------------------------------------------------------------------

/// Selects which per-row eq-only probe the invisible ghost rows force to scan,
/// by placing the ghosts in that probe's forward seek path (and out of the
/// other's).
#[derive(Clone, Copy)]
enum GhostPlacement {
    /// Low rowid, high `seq`: exercises `seek_index` (the UNIQUE-index probe).
    IndexProbe,
    /// High rowid, low `seq`: exercises `seek_rowid` (the INTEGER PRIMARY KEY probe).
    RowidProbe,
}

/// Number of anonymized payload (TEXT) columns on `core`, mirroring wide rows
/// without reproducing any real schema.
const HUGE_WRITE_PAYLOAD_COLS: usize = 8;
/// Rows inserted per measured batch (kept small so the benchmark stays quick while
/// exercising the same per-row index-probe path as a much larger batch).
const HUGE_WRITE_ROWS_PER_BATCH: usize = 32;
/// First rowid/`seq` the measured batches use. Kept above every ghost *rowid* so
/// the table's rowid-uniqueness seek never walks into the ghost rows.
const HUGE_WRITE_SEQ_BASE: i64 = 1_000_000;
/// The ghost connection's invisible index entries use `seq` values far above
/// anything the measured batch inserts, so every measured eq-only index probe
/// scans toward them (pre-fix) rather than stopping at a visible neighbor.
const GHOST_SEQ_BASE: i64 = 100_000_000;
/// For `GhostPlacement::RowidProbe`, the ghost rows' *rowids* sit far above any
/// rowid the measured batch reaches (which climbs from `HUGE_WRITE_SEQ_BASE` over
/// a run), so every measured table-rowid probe scans toward them (pre-fix) rather
/// than stopping at a visible neighbor.
const GHOST_ROWID_BASE: i64 = 1_000_000_000;
/// Rebuild the database (and ghost) every this many measured batches to keep the
/// committed-row footprint bounded over a long Criterion run.
const HUGE_WRITE_RESET_EVERY: u64 = 2_000;

fn payload_columns() -> String {
    (0..HUGE_WRITE_PAYLOAD_COLS)
        .map(|c| format!("c{c}"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn create_core_table_sql() -> String {
    let cols = (0..HUGE_WRITE_PAYLOAD_COLS)
        .map(|c| format!("c{c} TEXT"))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "CREATE TABLE core(rowid_pk INTEGER PRIMARY KEY, seq INTEGER, {cols}, \
         rank INTEGER, row_number INTEGER, created_ts INTEGER, modified_ts INTEGER)"
    )
}

/// The anonymized, shrunken batch write. `?1` is the rolling base for the unique
/// `seq` (and rowid), so successive iterations insert disjoint, ascending keys.
fn build_huge_multi_write(rows: usize) -> String {
    let col_list = payload_columns();
    let mut values = String::new();
    for off in 0..rows {
        if off > 0 {
            values.push(',');
        }
        let payload = (0..HUGE_WRITE_PAYLOAD_COLS)
            .map(|c| format!("'v{c}'"))
            .collect::<Vec<_>>()
            .join(", ");
        // (payload..., rank, off)
        values.push_str(&format!("({payload}, 0, {off})"));
    }
    format!(
        "WITH metadata_values AS (SELECT max_row_number, latest_op_timestamp FROM metadata), \
         new_rows({col_list}, rank, off) AS (VALUES {values}) \
         INSERT INTO core (rowid_pk, seq, {col_list}, rank, row_number, created_ts, modified_ts) \
         SELECT ?1 + off, ?1 + off, {col_list}, rank, max_row_number + off, \
                latest_op_timestamp, latest_op_timestamp \
         FROM new_rows, metadata_values \
         RETURNING rowid_pk, row_number"
    )
}

fn ghost_insert_sql(pending: i64, placement: GhostPlacement) -> String {
    let col_list = payload_columns();
    let payload = (0..HUGE_WRITE_PAYLOAD_COLS)
        .map(|_| "'ghost'")
        .collect::<Vec<_>>()
        .join(", ");
    let mut values = String::new();
    for j in 0..pending {
        if j > 0 {
            values.push(',');
        }
        let (rowid, seq) = match placement {
            // Low rowid (out of the measured rowid seek's forward path), high seq
            // (in the measured index probe's forward path).
            GhostPlacement::IndexProbe => (j + 1, GHOST_SEQ_BASE + j),
            // High rowid (in the measured rowid seek's forward path), low seq
            // (out of the measured index probe's forward path).
            GhostPlacement::RowidProbe => (GHOST_ROWID_BASE + j, j + 1),
        };
        values.push_str(&format!("({rowid}, {seq}, {payload}, 0, {seq}, 0, 0)"));
    }
    format!(
        "INSERT INTO core (rowid_pk, seq, {col_list}, rank, row_number, created_ts, modified_ts) \
         VALUES {values}"
    )
}

/// A fresh MVCC database with the benchmark schema and, optionally, a ghost
/// connection holding `pending` uncommitted (invisible) index entries.
struct HugeMultiWriteHarness {
    db: Arc<Database>,
    conn: Arc<Connection>,
    // Held only to keep the BEGIN CONCURRENT transaction (and its invisible rows)
    // alive for the lifetime of the harness.
    _ghost: Option<Arc<Connection>>,
}

impl HugeMultiWriteHarness {
    fn new(pending: i64, placement: GhostPlacement) -> Self {
        // In-memory IO: no fsync per commit, so the measurement reflects the
        // CPU-bound index-probe path the eq-only seek bound affects.
        let io = Arc::new(MemoryIO::new());
        let db = Database::open_file(io, ":memory:").unwrap();
        let conn = db.connect().unwrap();
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        // Keep everything in MvStore so the measurement reflects the in-memory
        // index path rather than checkpoint I/O.
        conn.execute("PRAGMA mvcc_checkpoint_threshold = -1")
            .unwrap();
        conn.wal_auto_actions_disable();
        conn.execute("CREATE TABLE metadata(max_row_number INTEGER, latest_op_timestamp INTEGER)")
            .unwrap();
        conn.execute("INSERT INTO metadata VALUES (1000000, 1700000000)")
            .unwrap();
        conn.execute(create_core_table_sql()).unwrap();
        conn.execute("CREATE UNIQUE INDEX idx_core_seq ON core(seq)")
            .unwrap();

        let ghost = (pending > 0).then(|| {
            let ghost = db.connect().unwrap();
            ghost.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
            ghost.execute("BEGIN CONCURRENT").unwrap();
            ghost.execute(ghost_insert_sql(pending, placement)).unwrap();
            // Intentionally not committed: these rows stay invisible to `conn`.
            ghost
        });

        Self {
            db,
            conn,
            _ghost: ghost,
        }
    }
}

fn run_to_completion(db: &Arc<Database>, stmt: &mut Statement) {
    loop {
        match stmt.step().unwrap() {
            StepResult::IO | StepResult::Yield => {
                db.io.step().unwrap();
            }
            // Drain the RETURNING rows.
            StepResult::Row => {}
            StepResult::Done => break,
            StepResult::Busy => panic!("huge multi-write batch insert returned Busy"),
            StepResult::Interrupt => panic!("huge multi-write batch insert was interrupted"),
        }
    }
}

fn run_huge_multi_write(c: &mut Criterion, group_name: &str, placement: GhostPlacement) {
    let idx1 = NonZeroUsize::new(1).unwrap();
    let sql = build_huge_multi_write(HUGE_WRITE_ROWS_PER_BATCH);

    let mut group = c.benchmark_group(group_name);
    group.sample_size(10);
    group.throughput(Throughput::Elements(HUGE_WRITE_ROWS_PER_BATCH as u64));

    for &pending in &[0i64, 1_000, 4_000] {
        group.bench_function(
            format!("rows={HUGE_WRITE_ROWS_PER_BATCH}/pending_invisible={pending}"),
            |b| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    let mut harness: Option<HugeMultiWriteHarness> = None;
                    let mut stmt: Option<Statement> = None;
                    let mut base = HUGE_WRITE_SEQ_BASE;
                    for i in 0..iters {
                        if i % HUGE_WRITE_RESET_EVERY == 0 {
                            // Rebuild from scratch (setup time is excluded from `total`).
                            let h = HugeMultiWriteHarness::new(pending, placement);
                            stmt = Some(h.conn.prepare(&sql).unwrap());
                            harness = Some(h);
                            base = HUGE_WRITE_SEQ_BASE;
                        }
                        let db = &harness.as_ref().unwrap().db;
                        let stmt = stmt.as_mut().unwrap();
                        stmt.bind_at(idx1, Value::from_i64(base)).unwrap();
                        let start = Instant::now();
                        run_to_completion(db, stmt);
                        total += start.elapsed();
                        stmt.reset().unwrap();
                        base += HUGE_WRITE_ROWS_PER_BATCH as i64;
                    }
                    total
                })
            },
        );
    }
    group.finish();
}

/// Guards the eq-only bound on the UNIQUE-index probe (`seek_index`): the ghost
/// rows' `seq` values sit above the measured batch, so each per-row `NoConflict`
/// probe would scan O(pending) invisible entries without the bound.
#[turso_macros::codspeed_criterion_benchmark]
fn bench_huge_multi_write(c: &mut Criterion) {
    run_huge_multi_write(c, "mvcc-huge-multi-write", GhostPlacement::IndexProbe);
}

/// Guards the eq-only bound on the table-rowid probe (`seek_rowid`): the ghost
/// rows' rowids sit above the measured batch, so each per-row `NotExists` probe
/// would scan O(pending) invisible entries without the bound. Time should stay
/// flat across `pending`; pre-fix it grew ~linearly (O(rows * pending)).
#[turso_macros::codspeed_criterion_benchmark]
fn bench_huge_multi_write_rowid(c: &mut Criterion) {
    run_huge_multi_write(c, "mvcc-huge-multi-write-rowid", GhostPlacement::RowidProbe);
}

#[cfg(not(feature = "codspeed"))]
criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench, bench_huge_multi_write, bench_huge_multi_write_rowid
}

#[cfg(feature = "codspeed")]
criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = bench, bench_huge_multi_write, bench_huge_multi_write_rowid
}

criterion_main!(benches);
