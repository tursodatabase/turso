//! Prepare-time cost of common statement shapes (issue #220).
//!
//! `Connection::prepare` has historically been ~3-4x slower than SQLite's
//! `sqlite3_prepare_v2` and the gap was not tracked by CI, so it regressed
//! silently. This benchmark times ONLY `prepare` (no execution) for a small
//! fixed set of statement shapes against a two-table schema.
//!
//! A parse-only group measures the same statements through `turso_parser`
//! alone, so the parse vs translate/emit split stays visible over time:
//! as of 2026-07 parsing is ~12-20% of prepare and translation dominates.
//!
//! Run with:
//!   cargo bench --bench prepare_benchmark

#[cfg(not(feature = "codspeed"))]
use criterion::{black_box, criterion_group, criterion_main, Criterion};

#[cfg(feature = "codspeed")]
use codspeed_criterion_compat::{black_box, criterion_group, criterion_main, Criterion};

use std::sync::Arc;
use turso_core::{Database, MemoryIO, StepResult};
use turso_parser::parser::Parser;

#[cfg(not(target_family = "wasm"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const STATEMENTS: &[(&str, &str)] = &[
    ("select_1", "SELECT 1"),
    ("select_by_pk", "SELECT * FROM users WHERE id = ?"),
    (
        "select_join_group_order",
        "SELECT u.name, COUNT(o.id), SUM(o.amount) AS total FROM users u \
         JOIN orders o ON o.user_id = u.id WHERE o.amount > 10.0 \
         GROUP BY u.name ORDER BY total DESC",
    ),
    (
        "insert_params",
        "INSERT INTO users (id, name) VALUES (?, ?)",
    ),
];

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

fn open_with_schema() -> (Arc<Database>, Arc<turso_core::Connection>) {
    let io = Arc::new(MemoryIO::new());
    let db = Database::open_file(io, ":memory:").unwrap();
    let conn = db.connect().unwrap();
    for sql in [
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)",
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, \
         amount REAL, created_at TEXT)",
        "CREATE INDEX idx_orders_user_id ON orders (user_id)",
    ] {
        let mut stmt = conn.query(sql).unwrap().unwrap();
        run_to_completion(&mut stmt, &db);
    }
    (db, conn)
}

#[turso_macros::codspeed_criterion_benchmark]
fn bench_prepare(c: &mut Criterion) {
    let (_db, conn) = open_with_schema();

    let mut group = c.benchmark_group("prepare_statement");
    for (name, sql) in STATEMENTS {
        group.bench_function(*name, |b| {
            b.iter(|| {
                let stmt = conn.prepare(black_box(sql)).unwrap();
                black_box(stmt);
            });
        });
    }
    group.finish();
}

#[turso_macros::codspeed_criterion_benchmark]
fn bench_parse_only(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_statement");
    for (name, sql) in STATEMENTS {
        group.bench_function(*name, |b| {
            b.iter(|| {
                let cmd = Parser::new(black_box(sql.as_bytes())).next_cmd();
                black_box(cmd).unwrap();
            });
        });
    }
    group.finish();
}

criterion_group!(prepare_benches, bench_prepare, bench_parse_only);
criterion_main!(prepare_benches);
