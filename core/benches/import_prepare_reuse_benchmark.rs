//! Isolates the exact mechanism behind the `.import --csv` batch-insert fix
//! (tursodatabase/turso#2087): the CLI's CSV importer used to build a fresh
//! `INSERT INTO t VALUES (...),(...)...` string and call `Connection::prepare`
//! on it for *every* batch of rows, even though every batch after the first
//! reuses the exact same SQL shape. The fix prepares that statement once and
//! reuses it across batches via `bind_at` / `run_with_row_callback` / `reset`.
//!
//! This benchmark reproduces that difference directly, with no CSV file, no
//! disk I/O, and no CLI process involved: both arms bind and execute the same
//! multi-row `VALUES` batches against the same in-memory table, and differ
//! ONLY in whether `Connection::prepare` is called once or once per batch.
//!
//! Column counts (9 / 16 / 105) mirror the real datasets exercised in the
//! issue's own before/after measurements (TPC-H `orders` / `lineitem`,
//! ClickBench `hits.csv`), since the fixed cost `prepare` re-pays each batch
//! scales with the number of columns (and thus placeholders) in the INSERT.
//!
//! Run with:
//!   cargo bench --bench import_prepare_reuse_benchmark

#[cfg(not(feature = "codspeed"))]
use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput,
};
use turso_core::SqliteDialect;

#[cfg(feature = "codspeed")]
use codspeed_criterion_compat::{
    black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput,
};

use std::num::NonZero;
use std::sync::Arc;
use turso_core::{Database, MemoryIO, StepResult, Value};

#[cfg(not(target_family = "wasm"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Rows per batch, matching `CSV_INSERT_BATCH_SIZE` in `cli/commands/import.rs`.
const BATCH_ROWS: usize = 1000;
/// Number of batches simulated per benchmark iteration (i.e. per timed sample).
const NUM_BATCHES: usize = 10;

fn run_to_completion(stmt: &mut turso_core::Statement, db: &Arc<Database>) {
    loop {
        match stmt.step().unwrap() {
            StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => db.io.step().unwrap(),
            StepResult::Done => break,
            StepResult::Row => panic!("unexpected row from INSERT"),
            StepResult::Interrupt | StepResult::Busy => panic!("unexpected step result"),
        }
    }
}

fn open_with_table(cols: usize) -> (Arc<Database>, Arc<turso_core::Connection>) {
    let io = Arc::new(MemoryIO::new());
    let db = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
    let conn = db.connect().unwrap();
    let col_defs = (0..cols)
        .map(|c| format!("c{c}"))
        .collect::<Vec<_>>()
        .join(", ");
    let mut stmt = conn
        .query(format!("CREATE TABLE t ({col_defs})"))
        .unwrap()
        .unwrap();
    run_to_completion(&mut stmt, &db);
    (db, conn)
}

/// Builds `INSERT INTO t VALUES (?1,...,?N),(?N+1,...)...` for `rows` tuples
/// of `cols` columns each, the same shape `build_values_placeholder_sql`
/// produces in the patched `.import` implementation.
fn build_insert(cols: usize, rows: usize) -> String {
    let mut sql = String::with_capacity(rows * cols * 4 + 32);
    sql.push_str("INSERT INTO t VALUES ");
    let mut p = 1usize;
    for r in 0..rows {
        if r > 0 {
            sql.push(',');
        }
        sql.push('(');
        for c in 0..cols {
            if c > 0 {
                sql.push(',');
            }
            sql.push_str(&format!("?{p}"));
            p += 1;
        }
        sql.push(')');
    }
    sql
}

/// Binds one batch's worth of values (everything as text, matching how CSV
/// field values are bound in `.import`).
fn bind_batch(stmt: &mut turso_core::Statement, cols: usize, rows: usize) {
    let mut idx = 1usize;
    for _ in 0..rows {
        for _ in 0..cols {
            stmt.bind_at(NonZero::new(idx).unwrap(), Value::from_text("value"))
                .unwrap();
            idx += 1;
        }
    }
}

#[turso_macros::codspeed_criterion_benchmark]
fn bench_import_prepare_reuse(c: &mut Criterion) {
    let mut group = c.benchmark_group("import_prepare_reuse");
    group.sample_size(20);

    // Column counts mirror TPC-H `orders` (9), TPC-H `lineitem` (16), and
    // ClickBench `hits.csv` (105).
    for cols in [9usize, 16, 105] {
        group.throughput(Throughput::Elements((NUM_BATCHES * BATCH_ROWS) as u64));

        // Baseline: rebuild + re-prepare the identical INSERT for every batch,
        // reproducing the pre-fix `.import` behavior.
        group.bench_function(BenchmarkId::new("prepare_per_batch", cols), |b| {
            b.iter_batched(
                || {
                    let (db, conn) = open_with_table(cols);
                    let sql = build_insert(cols, BATCH_ROWS);
                    (db, conn, sql)
                },
                |(db, conn, sql)| {
                    for _ in 0..NUM_BATCHES {
                        let mut stmt = conn.prepare(black_box(&sql)).unwrap();
                        bind_batch(&mut stmt, cols, BATCH_ROWS);
                        run_to_completion(&mut stmt, &db);
                    }
                },
                BatchSize::LargeInput,
            );
        });

        // Patched: prepare once, then bind_at/run/reset per batch.
        group.bench_function(BenchmarkId::new("prepare_once_reuse", cols), |b| {
            b.iter_batched(
                || {
                    let (db, conn) = open_with_table(cols);
                    let sql = build_insert(cols, BATCH_ROWS);
                    (db, conn, sql)
                },
                |(db, conn, sql)| {
                    let mut stmt = conn.prepare(black_box(&sql)).unwrap();
                    for _ in 0..NUM_BATCHES {
                        bind_batch(&mut stmt, cols, BATCH_ROWS);
                        run_to_completion(&mut stmt, &db);
                        stmt.reset().unwrap();
                    }
                },
                BatchSize::LargeInput,
            );
        });
    }

    group.finish();
}

criterion_group!(import_prepare_reuse_benches, bench_import_prepare_reuse);
criterion_main!(import_prepare_reuse_benches);
