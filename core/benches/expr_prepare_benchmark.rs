//! Prepare-time cost of expression-heavy statements.
//!
//! Expression translation now decomposes supported shapes into a value IR
//! before lowering to bytecode (core/translate/expr/ir.rs). These benchmarks
//! time ONLY `Connection::prepare` for statements dominated by expression
//! translation, so any build/lower overhead versus the previous eager path
//! shows up directly:
//!
//! - `literal_arith`: a long constant arithmetic chain (deep Binary nesting)
//! - `column_exprs`: many column-based arithmetic/comparison result columns
//! - `case_when`: a CASE expression with many WHEN arms
//! - `where_conditions`: a WHERE clause with many AND-ed comparisons
//!
//! Run with:
//!   cargo bench --bench expr_prepare_benchmark

#[cfg(not(feature = "codspeed"))]
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use turso_core::SqliteDialect;

#[cfg(feature = "codspeed")]
use codspeed_criterion_compat::{black_box, criterion_group, criterion_main, Criterion};

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
        .query("CREATE TABLE t (a, b, c TEXT COLLATE NOCASE, d INTEGER, e)")
        .unwrap()
        .unwrap();
    run_to_completion(&mut stmt, &db);
    (db, conn)
}

/// `SELECT 1 + 2 * 3 - 4 + 5 * 6 - ...` with `terms` literal operands.
fn literal_arith_sql(terms: usize) -> String {
    let mut sql = String::from("SELECT 1");
    for i in 2..=terms {
        let op = ["+", "*", "-"][i % 3];
        sql.push_str(&format!(" {op} {i}"));
    }
    sql
}

/// `SELECT a + 1 * 2, b < 3, ... FROM t` with `cols` expression result columns.
fn column_exprs_sql(cols: usize) -> String {
    let mut sql = String::from("SELECT ");
    for i in 0..cols {
        if i > 0 {
            sql.push_str(", ");
        }
        match i % 4 {
            0 => sql.push_str(&format!("a + {i} * b")),
            1 => sql.push_str(&format!("b < {i}")),
            2 => sql.push_str(&format!("c = 'x{i}'")),
            _ => sql.push_str(&format!("-(d + {i})")),
        }
    }
    sql.push_str(" FROM t");
    sql
}

/// `SELECT CASE WHEN a = 1 THEN 1 WHEN a = 2 THEN 2 ... ELSE 0 END FROM t`.
fn case_when_sql(arms: usize) -> String {
    let mut sql = String::from("SELECT CASE");
    for i in 0..arms {
        sql.push_str(&format!(" WHEN a = {i} THEN {i}"));
    }
    sql.push_str(" ELSE 0 END FROM t");
    sql
}

/// `SELECT * FROM t WHERE a + 0 > 0 AND b * 1 < 1 AND ...` with `terms` comparisons.
fn where_conditions_sql(terms: usize) -> String {
    let mut sql = String::from("SELECT * FROM t WHERE a + 0 > 0");
    for i in 1..terms {
        sql.push_str(&format!(" AND a + {i} > b * {i}"));
    }
    sql
}

#[turso_macros::codspeed_criterion_benchmark]
fn bench_expr_prepare(c: &mut Criterion) {
    let mut group = c.benchmark_group("prepare_expr");

    let cases = [
        ("literal_arith", literal_arith_sql(100)),
        ("column_exprs", column_exprs_sql(50)),
        ("case_when", case_when_sql(50)),
        ("where_conditions", where_conditions_sql(50)),
    ];

    for (name, sql) in cases {
        let (_db, conn) = open_with_table();
        group.bench_function(name, |b| {
            b.iter(|| {
                let stmt = conn.prepare(black_box(&sql)).unwrap();
                black_box(stmt);
            });
        });
    }

    group.finish();
}

criterion_group!(expr_prepare_benches, bench_expr_prepare);
criterion_main!(expr_prepare_benches);
