//! Benchmarks for the bytecode interpreter (core/vdbe/execute.rs).
//!
//! Every workload runs on an in-memory database so disk I/O never shows up in
//! the numbers, and every workload also runs on SQLite (rusqlite, in-memory)
//! so there is a fixed reference point to compare against.
//!
//! Groups:
//!
//! - `interp/series`: expressions over `generate_series(1, N)`. The row source
//!   is a cheap virtual table, so almost all of the time is the interpreter
//!   loop itself: arithmetic, comparisons, branches, function calls, string
//!   building, aggregates.
//! - `interp/rcte`: the same count over a recursive CTE, which adds the
//!   coroutine and ephemeral-queue opcodes to the mix.
//! - `interp/scan`: full scans of an in-memory table, which adds cursor
//!   stepping and record decoding (Column, Next, ResultRow).
//! - `interp/stmt`: one tiny statement run over and over (bind, step, reset)
//!   to measure the fixed cost of executing a statement.
//!
//! Run:  cargo bench -p turso_core --bench interpreter_benchmark

#[cfg(feature = "codspeed")]
use codspeed_criterion_compat::{
    black_box, criterion_group, criterion_main, measurement::WallTime, BenchmarkGroup, BenchmarkId,
    Criterion,
};
#[cfg(not(feature = "codspeed"))]
use criterion::{
    black_box, criterion_group, criterion_main, measurement::WallTime, BenchmarkGroup, BenchmarkId,
    Criterion,
};

use std::num::NonZero;
use std::sync::Arc;
use std::time::Duration;
use turso_core::{Connection, Database, MemoryIO, SqliteDialect, Statement, StepResult, Value};

#[cfg(not(target_family = "wasm"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Rows produced by the series / recursive CTE workloads.
const SERIES_N: usize = 100_000;

/// Rows in the scanned table.
const SCAN_N: usize = 100_000;

/// Expression workloads over `generate_series(1, N)`. Each one aggregates to
/// a single row so the measurement is the per-row interpreter work, not the
/// cost of handing rows back to the caller.
const SERIES_QUERIES: &[(&str, &str)] = &[
    ("count", "SELECT count(*) FROM generate_series(1, {N})"),
    (
        "arith",
        "SELECT sum(value*3 + value/2 - value%7) FROM generate_series(1, {N})",
    ),
    (
        "compare",
        "SELECT count(*) FROM generate_series(1, {N}) WHERE value % 3 = 0 AND value % 5 <> 0",
    ),
    (
        "case",
        "SELECT sum(CASE WHEN value % 2 = 0 THEN 1 WHEN value % 3 = 0 THEN 2 ELSE 0 END) \
         FROM generate_series(1, {N})",
    ),
    (
        "scalar_funcs",
        "SELECT sum(abs(value - 50000) + length(value) + max(value, 10)) FROM generate_series(1, {N})",
    ),
    (
        "string_concat",
        "SELECT sum(length('row-' || value || '-end')) FROM generate_series(1, {N})",
    ),
    (
        "float_math",
        "SELECT sum(value * 1.5 + value / 3.0) FROM generate_series(1, {N})",
    ),
];

const RCTE_QUERIES: &[(&str, &str)] = &[(
    "count",
    "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < {N}) \
     SELECT count(*) FROM c",
)];

/// Scans of table `t(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, s TEXT)`.
const SCAN_QUERIES: &[(&str, &str)] = &[
    ("sum_two_cols", "SELECT sum(a + b) FROM t"),
    (
        "filter_count",
        "SELECT count(*) FROM t WHERE b > 500 AND a % 2 = 0",
    ),
    (
        "text_filter",
        "SELECT count(*) FROM t WHERE s LIKE 'row-1%'",
    ),
    // Returns every row, so this one also measures ResultRow and the
    // step() round trip back to the caller.
    ("all_rows", "SELECT id, a, b, s FROM t"),
];

fn open_turso() -> (Arc<Database>, Arc<Connection>) {
    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(MemoryIO::new());
    let db = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
    let conn = db.connect().unwrap();
    (db, conn)
}

fn open_sqlite() -> rusqlite::Connection {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    rusqlite::vtab::series::load_module(&conn).unwrap();
    conn
}

fn scan_table_sql(n: usize) -> Vec<String> {
    let mut sql =
        vec!["CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, s TEXT)".to_string()];
    // Deterministic, low-cost values: `a` cycles 0..999, `b` cycles 0..1023.
    sql.push(format!(
        "INSERT INTO t(id, a, b, s) \
         SELECT value, value % 1000, value % 1024, 'row-' || value \
         FROM generate_series(1, {n})"
    ));
    sql
}

fn run_turso(db: &Database, stmt: &mut Statement) {
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                black_box(stmt.row().unwrap());
            }
            StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => db.io.step().unwrap(),
            StepResult::Done => break,
            StepResult::Interrupt | StepResult::Busy => unreachable!(),
        }
    }
    stmt.reset().unwrap();
}

fn run_sqlite(stmt: &mut rusqlite::Statement) {
    let mut rows = stmt.query([]).unwrap();
    while let Some(row) = rows.next().unwrap() {
        black_box(row.get_ref(0).unwrap());
    }
}

fn bench_query_pair(
    group: &mut BenchmarkGroup<'_, WallTime>,
    label: &str,
    sql: &str,
    turso: &(Arc<Database>, Arc<Connection>),
    sqlite: &rusqlite::Connection,
) {
    let (db, conn) = turso;
    group.bench_function(BenchmarkId::new("turso", label), |b| {
        let mut stmt = conn.prepare(sql).unwrap();
        b.iter(|| run_turso(db, &mut stmt));
    });
    group.bench_function(BenchmarkId::new("sqlite", label), |b| {
        let mut stmt = sqlite.prepare(sql).unwrap();
        b.iter(|| run_sqlite(&mut stmt));
    });
}

#[turso_macros::codspeed_criterion_benchmark]
fn bench_series(criterion: &mut Criterion) {
    let turso = open_turso();
    let sqlite = open_sqlite();

    let mut group = criterion.benchmark_group("interp/series");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(5));
    for (label, template) in SERIES_QUERIES {
        let sql = template.replace("{N}", &SERIES_N.to_string());
        bench_query_pair(&mut group, label, &sql, &turso, &sqlite);
    }
    group.finish();
}

#[turso_macros::codspeed_criterion_benchmark]
fn bench_rcte(criterion: &mut Criterion) {
    let turso = open_turso();
    let sqlite = open_sqlite();

    let mut group = criterion.benchmark_group("interp/rcte");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(5));
    for (label, template) in RCTE_QUERIES {
        let sql = template.replace("{N}", &SERIES_N.to_string());
        bench_query_pair(&mut group, label, &sql, &turso, &sqlite);
    }
    group.finish();
}

#[turso_macros::codspeed_criterion_benchmark]
fn bench_scan(criterion: &mut Criterion) {
    let turso = open_turso();
    let sqlite = open_sqlite();
    for sql in scan_table_sql(SCAN_N) {
        let mut stmt = turso.1.prepare(&sql).unwrap();
        run_turso(&turso.0, &mut stmt);
        sqlite.execute_batch(&sql).unwrap();
    }

    let mut group = criterion.benchmark_group("interp/scan");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(5));
    for (label, sql) in SCAN_QUERIES {
        bench_query_pair(&mut group, label, sql, &turso, &sqlite);
    }
    group.finish();
}

#[turso_macros::codspeed_criterion_benchmark]
fn bench_stmt(criterion: &mut Criterion) {
    let turso = open_turso();
    let sqlite = open_sqlite();
    for sql in scan_table_sql(1000) {
        let mut stmt = turso.1.prepare(&sql).unwrap();
        run_turso(&turso.0, &mut stmt);
        sqlite.execute_batch(&sql).unwrap();
    }

    let mut group = criterion.benchmark_group("interp/stmt");

    // No table access at all: this is the floor for running one statement.
    group.bench_function(BenchmarkId::new("turso", "select_param"), |b| {
        let (db, conn) = &turso;
        let mut stmt = conn.prepare("SELECT ?1 + 1").unwrap();
        let mut i = 0i64;
        b.iter(|| {
            i += 1;
            stmt.bind_at(NonZero::new(1).unwrap(), Value::from_i64(i))
                .unwrap();
            run_turso(db, &mut stmt);
        });
    });
    group.bench_function(BenchmarkId::new("sqlite", "select_param"), |b| {
        let mut stmt = sqlite.prepare("SELECT ?1 + 1").unwrap();
        let mut i = 0i64;
        b.iter(|| {
            i += 1;
            let mut rows = stmt.query([i]).unwrap();
            while let Some(row) = rows.next().unwrap() {
                black_box(row.get_ref(0).unwrap());
            }
        });
    });

    // One rowid lookup per statement.
    group.bench_function(BenchmarkId::new("turso", "point_lookup"), |b| {
        let (db, conn) = &turso;
        let mut stmt = conn.prepare("SELECT b FROM t WHERE id = ?1").unwrap();
        let mut i = 0i64;
        b.iter(|| {
            i = i % 1000 + 1;
            stmt.bind_at(NonZero::new(1).unwrap(), Value::from_i64(i))
                .unwrap();
            run_turso(db, &mut stmt);
        });
    });
    group.bench_function(BenchmarkId::new("sqlite", "point_lookup"), |b| {
        let mut stmt = sqlite.prepare("SELECT b FROM t WHERE id = ?1").unwrap();
        let mut i = 0i64;
        b.iter(|| {
            i = i % 1000 + 1;
            let mut rows = stmt.query([i]).unwrap();
            while let Some(row) = rows.next().unwrap() {
                black_box(row.get_ref(0).unwrap());
            }
        });
    });

    group.finish();
}

criterion_group!(benches, bench_series, bench_rcte, bench_scan, bench_stmt);
criterion_main!(benches);
