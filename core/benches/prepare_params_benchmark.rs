//! Prepare-time cost of multi-row `INSERT ... VALUES` with bound parameters.
//!
//! Reproduces a production pain point: drivers/ORMs batch inserts into a single
//! `INSERT INTO t (...) VALUES (?,?,..),(?,?,..),...` with thousands of bound
//! parameters. Each parameter occurrence is registered during translation, and
//! a naive linear dedup made `prepare()` O(P^2) in the number of parameters P.
//!
//! This benchmark times ONLY `Connection::prepare` (no execution) for a 5-column
//! INSERT with a growing number of value rows, so the parameter count scales as
//! `5 * rows`. ~1000 parameters (200 rows) is enough to surface the scaling.
//!
//! Run with:
//!   cargo bench --bench prepare_params_benchmark

#[cfg(not(feature = "codspeed"))]
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use turso_core::SqliteDialect;

#[cfg(feature = "codspeed")]
use codspeed_criterion_compat::{
    black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput,
};

use std::sync::Arc;
use turso_core::{Database, MemoryIO, StepResult};

#[cfg(not(target_family = "wasm"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn run_to_completion(stmt: &mut turso_core::Statement, db: &Arc<Database>) {
    loop {
        match stmt.step().unwrap() {
            StepResult::IO | StepResult::Yield => db.io.step().unwrap(),
            StepResult::Done => break,
            StepResult::Row => {}
            StepResult::Interrupt | StepResult::Busy => panic!("unexpected step result"),
        }
    }
}

fn open_with_table() -> (Arc<Database>, Arc<turso_core::Connection>) {
    let io = Arc::new(MemoryIO::new());
    let db = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
    let conn = db.connect().unwrap();
    let mut stmt = conn
        .query(
            "CREATE TABLE at_refs (source_dbt_doc_id, source_column_id, \
             source_row_id, target_row_id, source_row_number)",
        )
        .unwrap()
        .unwrap();
    run_to_completion(&mut stmt, &db);
    (db, conn)
}

/// Build `INSERT INTO at_refs (5 cols) VALUES (?,?,?,?,?),(...)...` with `rows`
/// tuples, i.e. `5 * rows` distinct positional parameters.
fn build_insert(rows: usize) -> String {
    let mut sql = String::with_capacity(rows * 32);
    sql.push_str(
        "INSERT INTO at_refs (source_dbt_doc_id, source_column_id, \
         source_row_id, target_row_id, source_row_number) VALUES ",
    );
    let mut p = 1;
    for r in 0..rows {
        if r > 0 {
            sql.push(',');
        }
        sql.push('(');
        for c in 0..5 {
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

#[turso_macros::codspeed_criterion_benchmark]
fn bench_prepare_params(c: &mut Criterion) {
    let mut group = c.benchmark_group("prepare_multirow_insert");

    // Parameter counts: 5 columns * {40, 100, 200} rows = {200, 500, 1000}.
    for rows in [40usize, 100, 200] {
        let params = rows * 5;
        let (_db, conn) = open_with_table();
        let sql = build_insert(rows);

        group.throughput(Throughput::Elements(params as u64));
        group.bench_function(BenchmarkId::new("params", params), |b| {
            b.iter(|| {
                let stmt = conn.prepare(black_box(&sql)).unwrap();
                black_box(stmt);
            });
        });
    }

    group.finish();
}

criterion_group!(prepare_params_benches, bench_prepare_params);
criterion_main!(prepare_params_benches);
