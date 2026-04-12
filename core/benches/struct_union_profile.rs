//! Standalone profiling harness for struct/union operations.
//! Not a criterion benchmark — runs operations in a tight loop so external
//! profilers (samply, instruments, dhat) can capture the hot path.
//!
//! Build: cargo build --release --bench struct_union_profile
//! Profile: samply record target/release/deps/struct_union_profile-*

use std::sync::Arc;
use turso_core::io::MemoryIO;
use turso_core::{Database, DatabaseOpts, OpenFlags, StepResult};

fn setup_db() -> (Arc<Database>, Arc<turso_core::Connection>) {
    let opts = DatabaseOpts::new().with_custom_types(true);
    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(MemoryIO::new());
    let db =
        Database::open_file_with_flags(io, ":memory:", OpenFlags::default(), opts, None).unwrap();
    let conn = db.connect().unwrap();
    (db, conn)
}

fn execute(db: &Database, conn: &Arc<turso_core::Connection>, sql: &str) {
    let mut stmt = conn.prepare(sql).unwrap();
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {}
            StepResult::IO => {
                db.io.step().unwrap();
            }
            StepResult::Done => break,
            StepResult::Interrupt | StepResult::Busy => unreachable!(),
        }
    }
}

fn run_scan(db: &Database, stmt: &mut turso_core::Statement) {
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                std::hint::black_box(stmt.row());
            }
            StepResult::IO => {
                db.io.step().unwrap();
            }
            StepResult::Done => break,
            StepResult::Interrupt | StepResult::Busy => unreachable!(),
        }
    }
    stmt.reset().unwrap();
}

const N: usize = 10_000;
const ITERS: usize = 5000;

fn main() {
    let mode = std::env::args().nth(1).unwrap_or_else(|| "all".to_string());

    match mode.as_str() {
        "struct_field" | "all" => {
            eprintln!("=== struct_field: SELECT val.x FROM structs ({N} rows x {ITERS} iters) ===");
            let (db, conn) = setup_db();
            execute(&db, &conn, "CREATE TYPE point AS STRUCT(x INT, y INT)");
            execute(&db, &conn, "CREATE TABLE structs(val point) STRICT");
            for i in 1..=N {
                execute(
                    &db,
                    &conn,
                    &format!("INSERT INTO structs VALUES (struct_pack({i}, {}))", i * 2),
                );
            }
            let mut stmt = conn.prepare("SELECT val.x FROM structs").unwrap();
            for _ in 0..ITERS {
                run_scan(&db, &mut stmt);
            }
            if mode != "all" {
                return;
            }
        }
        _ => {}
    }

    match mode.as_str() {
        "union_extract" | "all" => {
            eprintln!("=== union_extract: SELECT val.i FROM unions ({N} rows x {ITERS} iters) ===");
            let (db, conn) = setup_db();
            execute(&db, &conn, "CREATE TYPE number AS UNION(i INT, f REAL)");
            execute(&db, &conn, "CREATE TABLE unions(val number) STRICT");
            for i in 1..=N {
                execute(
                    &db,
                    &conn,
                    &format!("INSERT INTO unions VALUES (union_value('i', {i}))"),
                );
            }
            let mut stmt = conn.prepare("SELECT val.i FROM unions").unwrap();
            for _ in 0..ITERS {
                run_scan(&db, &mut stmt);
            }
            if mode != "all" {
                return;
            }
        }
        _ => {}
    }

    match mode.as_str() {
        "flat" | "all" => {
            eprintln!("=== flat baseline: SELECT x FROM flat ({N} rows x {ITERS} iters) ===");
            let (db, conn) = setup_db();
            execute(&db, &conn, "CREATE TABLE flat(x INTEGER, y INTEGER) STRICT");
            for i in 1..=N {
                execute(
                    &db,
                    &conn,
                    &format!("INSERT INTO flat VALUES ({i}, {})", i * 2),
                );
            }
            let mut stmt = conn.prepare("SELECT x FROM flat").unwrap();
            for _ in 0..ITERS {
                run_scan(&db, &mut stmt);
            }
        }
        _ => {}
    }
}
