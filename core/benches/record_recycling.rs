//! Instruction-focused benchmarks for VDBE value and record recycling paths.
//!
//! Setup, data loading, statement preparation, and opcode verification happen
//! outside the measured closure. Each benchmark repeatedly drains and resets a
//! prepared statement so CodSpeed simulation measures the steady-state VDBE
//! path rather than parsing or fixture construction.

use divan::{black_box, AllocProfiler, Bencher};
use mimalloc::MiMalloc;
use std::sync::Arc;
use turso_core::{Connection, Database, MemoryIO, SqliteDialect, Statement, StepResult, Value};

#[global_allocator]
static ALLOC: AllocProfiler<MiMalloc> = AllocProfiler::new(MiMalloc);

const ROWS: usize = 2_048;

#[cfg(not(feature = "codspeed"))]
fn main() {
    divan::Divan::default().sample_count(20).main();
}

#[cfg(feature = "codspeed")]
fn main() {
    divan::main();
}

fn open_database(mvcc: bool) -> (Arc<Database>, Arc<Connection>) {
    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(MemoryIO::new());
    let db = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
    let conn = db.connect().unwrap();
    if mvcc {
        execute(&db, &conn, "PRAGMA journal_mode = 'mvcc'");
    }
    (db, conn)
}

fn drain(db: &Database, stmt: &mut Statement) {
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                black_box(stmt.row());
            }
            StepResult::IO | StepResult::Yield => db.io.step().unwrap(),
            StepResult::Done => break,
            StepResult::Interrupt | StepResult::Busy => unreachable!(),
        }
    }
    stmt.reset().unwrap();
}

fn execute(db: &Database, conn: &Arc<Connection>, sql: &str) {
    let mut stmt = conn.prepare(sql).unwrap();
    drain(db, &mut stmt);
}

fn seed_database(mvcc: bool) -> (Arc<Database>, Arc<Connection>) {
    let (db, conn) = open_database(mvcc);
    execute(
        &db,
        &conn,
        "CREATE TABLE rows(
            id INTEGER PRIMARY KEY,
            sort_key INTEGER NOT NULL,
            txt TEXT NOT NULL,
            payload BLOB NOT NULL
        )",
    );
    execute(&db, &conn, "BEGIN");
    let mut insert = conn
        .prepare(
            "INSERT INTO rows(id, sort_key, txt, payload)
             VALUES (?1, ?2, ?3, zeroblob(128))",
        )
        .unwrap();
    for i in 0..ROWS {
        insert
            .bind_at(1.try_into().unwrap(), Value::from_i64(i as i64))
            .unwrap();
        insert
            .bind_at(
                2.try_into().unwrap(),
                Value::from_i64(((i * 1_103) % ROWS) as i64),
            )
            .unwrap();
        insert
            .bind_at(
                3.try_into().unwrap(),
                Value::build_text(format!("value-{i:06}-{}", "x".repeat(96))),
            )
            .unwrap();
        drain(&db, &mut insert);
    }
    execute(&db, &conn, "COMMIT");
    (db, conn)
}

fn assert_opcodes(db: &Database, conn: &Arc<Connection>, sql: &str, expected: &[&str]) {
    let mut explain = conn.prepare(format!("EXPLAIN {sql}")).unwrap();
    let mut found = vec![false; expected.len()];
    loop {
        match explain.step().unwrap() {
            StepResult::Row => {
                let opcode = explain.row().unwrap().get::<String>(1).unwrap();
                for (seen, expected) in found.iter_mut().zip(expected) {
                    *seen |= opcode == *expected;
                }
            }
            StepResult::IO | StepResult::Yield => db.io.step().unwrap(),
            StepResult::Done => break,
            StepResult::Interrupt | StepResult::Busy => unreachable!(),
        }
    }
    for (seen, expected) in found.into_iter().zip(expected) {
        assert!(seen, "benchmark query must contain {expected}: {sql}");
    }
}

fn benchmark_query(bencher: Bencher, mvcc: bool, sql: &str, expected_opcodes: &[&str]) {
    let (db, conn) = seed_database(mvcc);
    assert_opcodes(&db, &conn, sql, expected_opcodes);
    let mut stmt = conn.prepare(sql).unwrap();
    drain(&db, &mut stmt);
    bencher.bench_local(|| drain(black_box(&db), black_box(&mut stmt)));
}

#[turso_macros::divan_bench]
fn sorter_record_round_trip(bencher: Bencher) {
    benchmark_query(
        bencher,
        false,
        "SELECT txt, payload FROM rows ORDER BY sort_key DESC",
        &["MakeRecord", "SorterInsert", "SorterData"],
    );
}

#[turso_macros::divan_bench]
fn group_concat_text(bencher: Bencher) {
    benchmark_query(
        bencher,
        false,
        "SELECT group_concat(txt, '|') FROM rows",
        &["AggStep"],
    );
}

#[turso_macros::divan_bench]
fn max_text_replacement(bencher: Bencher) {
    benchmark_query(bencher, false, "SELECT max(txt) FROM rows", &["AggStep"]);
}

#[turso_macros::divan_bench]
fn last_value_blob_capture(bencher: Bencher) {
    benchmark_query(
        bencher,
        false,
        "SELECT last_value(payload) OVER (ORDER BY id) FROM rows",
        &["AggStep"],
    );
}

#[turso_macros::divan_bench]
fn mvcc_repeated_index_seek(bencher: Bencher) {
    let (db, conn) = seed_database(true);
    execute(&db, &conn, "CREATE INDEX rows_txt ON rows(txt)");
    execute(&db, &conn, "CREATE TABLE probes(txt TEXT NOT NULL)");
    execute(
        &db,
        &conn,
        "INSERT INTO probes SELECT txt FROM rows WHERE id % 2 = 0",
    );
    const SQL: &str = "SELECT (SELECT id FROM rows WHERE rows.txt = probes.txt) FROM probes";
    assert_opcodes(&db, &conn, SQL, &["SeekGE"]);
    let mut stmt = conn.prepare(SQL).unwrap();
    drain(&db, &mut stmt);
    bencher.bench_local(|| drain(black_box(&db), black_box(&mut stmt)));
}
