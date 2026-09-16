//! Callgrind driver for the deferred-seek Column path.
//!
//! Usage: callgrind_deferred_seek [scan|yield|insert|explain] [rows] [iterations]
//!
//! `yield` runs the scan on an I/O that yields on every operation, so every
//! deferred seek suspends and resumes.

use std::sync::Arc;
use turso_core::{
    Database, MemoryIO, MemoryYieldIO, Numeric, SqliteDialect, StepResult, Value, IO,
};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mode = args.get(1).map(String::as_str).unwrap_or("scan");
    let rows: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(10_000);
    let iterations: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(5);

    let io: Arc<dyn IO> = if mode == "yield" {
        Arc::new(MemoryYieldIO::new())
    } else {
        Arc::new(MemoryIO::new())
    };
    let db = Database::open_file(io.clone(), "callgrind.db", Arc::new(SqliteDialect)).unwrap();
    let conn = db.connect().unwrap();
    conn.execute("CREATE TABLE t(a INTEGER, b INTEGER, c TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX ta ON t(a)").unwrap();
    // Yield mode: wide rows so few fit a page, and an index order that visits
    // table pages in a scattered order. With a 200-page cache most deferred
    // seeks then read a page and yield.
    let filler = if mode == "yield" {
        "x".repeat(1000)
    } else {
        "row".to_string()
    };
    conn.execute("BEGIN").unwrap();
    for i in 0..rows {
        let a = if mode == "yield" {
            (i * 7919) % rows
        } else {
            i
        };
        conn.execute(format!("INSERT INTO t VALUES ({a}, {}, '{filler}')", i * 2))
            .unwrap();
    }
    conn.execute("COMMIT").unwrap();

    let scan_sql = "SELECT b FROM t WHERE a >= 0";
    let conn = if mode == "yield" {
        drop(conn);
        drop(db);
        let db = Database::open_file(io.clone(), "callgrind.db", Arc::new(SqliteDialect)).unwrap();
        let conn = db.connect().unwrap();
        conn.execute("PRAGMA cache_size = 200").unwrap();
        conn
    } else {
        conn
    };
    match mode {
        "explain" => {
            let mut stmt = conn.prepare(format!("EXPLAIN {scan_sql}")).unwrap();
            loop {
                match stmt.step().unwrap() {
                    StepResult::Row => {
                        let row = stmt.row().unwrap();
                        let cols: Vec<String> = (0..row.len())
                            .map(|i| format!("{}", row.get_value(i)))
                            .collect();
                        println!("{}", cols.join(" | "));
                    }
                    StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => {
                        io.step().unwrap()
                    }
                    StepResult::Done => break,
                    other => panic!("unexpected step result {other:?}"),
                }
            }
        }
        "scan" | "yield" => {
            let mut stmt = conn.prepare(scan_sql).unwrap();
            let expected: i64 = (0..rows as i64).map(|i| i * 2).sum();
            let mut io_yields = 0usize;
            for _ in 0..iterations {
                let (sum, yields) = run_scan(&mut stmt, &io);
                assert_eq!(sum, expected);
                io_yields += yields;
            }
            println!("{mode} ok: {iterations} x {rows} rows, {io_yields} io yields");
        }
        "insert" => {
            conn.execute("CREATE TABLE u(a INTEGER, b INTEGER)")
                .unwrap();
            let mut stmt = conn.prepare("INSERT INTO u VALUES (1, 2)").unwrap();
            conn.execute("BEGIN").unwrap();
            run_insert(&mut stmt, &io, iterations * rows);
            conn.execute("COMMIT").unwrap();
            println!("insert ok: {} rows", iterations * rows);
        }
        _ => panic!("unknown mode {mode}"),
    }
}

#[inline(never)]
fn run_scan(stmt: &mut turso_core::Statement, io: &Arc<dyn IO>) -> (i64, usize) {
    let mut sum = 0i64;
    let mut yields = 0usize;
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                match row.get_value(0) {
                    Value::Numeric(Numeric::Integer(n)) => sum += *n,
                    other => panic!("unexpected value {other:?}"),
                }
            }
            StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => {
                yields += 1;
                io.step().unwrap()
            }
            StepResult::Done => break,
            other => panic!("unexpected step result {other:?}"),
        }
    }
    stmt.reset().unwrap();
    (sum, yields)
}

#[inline(never)]
fn run_insert(stmt: &mut turso_core::Statement, io: &Arc<dyn IO>, count: usize) {
    for _ in 0..count {
        loop {
            match stmt.step().unwrap() {
                StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => io.step().unwrap(),
                StepResult::Done => break,
                other => panic!("unexpected step result {other:?}"),
            }
        }
        stmt.reset().unwrap();
    }
}
