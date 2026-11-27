//! Benchmarks for reads that seek a B-tree instead of scanning it.
//!
//!   - point_rowid : SELECT v FROM t WHERE id = ?   (seek on the table B-tree)
//!   - point_index : SELECT v FROM t WHERE k = ?    (seek on an index, then on the table)
//!   - range_rowid : SELECT v FROM t WHERE id >= ? AND id < ?  (10, 100 and 1000 rows)
//!   - range_index : SELECT v FROM t WHERE k >= ? AND k < ?    (10, 100 and 1000 rows)
//!
//! Each call of the measured closure binds a different key, steps the prepared
//! statement to the end and resets it. The keys are spread over the table, so the
//! benchmarks do not read the same leaf page each time. The page cache holds the
//! whole database, so the benchmarks measure CPU work and not IO.
//!
//! The index benchmarks read `v` from the table, so each row found in the index
//! also needs a seek on the table B-tree. In range_index, most of the cost is these
//! table seeks.
//!
//! Run:  cargo bench -p turso_benches --bench seek_benchmark

use divan::{black_box, AllocProfiler};
use mimalloc::MiMalloc;
use std::sync::Arc;
use turso_benches::Bencher;
use turso_core::{
    ConnectOpts, Connection, Database, MemoryIO, SessionRole, Statement, StepResult, Value,
};
use turso_sqlite::SqliteDialect;

#[global_allocator]
static ALLOC: AllocProfiler<MiMalloc> = AllocProfiler::new(MiMalloc);

/// Number of rows in the table. With 100,000 rows the table B-tree and the index
/// have interior pages, so a seek goes through more than one level.
const ROWS: i64 = 100_000;

/// Row `id` has `k = id * K_FACTOR % ROWS`. The factor is prime and does not divide
/// `ROWS`, so each `k` occurs once, and rows that are next to each other in the index
/// are far apart in the table.
const K_FACTOR: i64 = 7_919;

/// The key of the next lookup is the previous key plus this step, modulo the number
/// of keys. The step is prime and does not divide the number of keys, so the lookups
/// visit all keys before one repeats, and two lookups in a row are far apart in the
/// B-tree. The step is not `K_FACTOR`, because with that step the lookups by `k` would
/// read the table rows in order.
const KEY_STEP: i64 = 4_099;

#[cfg(not(feature = "codspeed"))]
fn main() {
    divan::Divan::default().sample_count(50).main();
}

#[cfg(feature = "codspeed")]
fn main() {
    divan::main();
}

#[turso_macros::divan_bench]
fn point_rowid(bencher: Bencher) {
    bench_point(
        bencher,
        "SELECT v FROM t WHERE id = ?1",
        "SEARCH t USING INTEGER PRIMARY KEY (rowid=?)",
    );
}

#[turso_macros::divan_bench]
fn point_index(bencher: Bencher) {
    bench_point(
        bencher,
        "SELECT v FROM t WHERE k = ?1",
        "SEARCH t USING INDEX t_k (k=?)",
    );
}

#[turso_macros::divan_bench(args = [10, 100, 1000])]
fn range_rowid(bencher: Bencher, rows: i64) {
    bench_range(
        bencher,
        "SELECT v FROM t WHERE id >= ?1 AND id < ?2",
        "SEARCH t USING INTEGER PRIMARY KEY (rowid>=? AND rowid<?)",
        rows,
    );
}

#[turso_macros::divan_bench(args = [10, 100, 1000])]
fn range_index(bencher: Bencher, rows: i64) {
    bench_range(
        bencher,
        "SELECT v FROM t WHERE k >= ?1 AND k < ?2",
        "SEARCH t USING INDEX t_k (k>=? AND k<?)",
        rows,
    );
}

fn bench_point(bencher: Bencher, sql: &str, expected_plan: &str) {
    let (db, conn) = seed_database();
    assert_plan(&db, &conn, sql, expected_plan);
    let mut stmt = conn.prepare(sql).unwrap();
    // Run every key once before the measurement, to check that each lookup finds
    // exactly one row.
    for key in 0..ROWS {
        bind_int(&mut stmt, 1, key);
        assert_eq!(drain(&db, &mut stmt), 1, "{sql} with key {key}");
    }
    let mut key = 0;
    bencher.bench_local(|| {
        key = (key + KEY_STEP) % ROWS;
        bind_int(&mut stmt, 1, key);
        drain(black_box(&db), black_box(&mut stmt))
    });
}

fn bench_range(bencher: Bencher, sql: &str, expected_plan: &str, rows: i64) {
    let (db, conn) = seed_database();
    assert_plan(&db, &conn, sql, expected_plan);
    let mut stmt = conn.prepare(sql).unwrap();
    // The range starts at most at `ROWS - rows`, so that every range is full.
    let starts = ROWS - rows + 1;
    // Before the measurement, run the last range and the first 100 ranges that the
    // measurement uses, to check that each range returns `rows` rows.
    assert_eq!(read_range(&db, &mut stmt, starts - 1, rows), rows, "{sql}");
    let mut start = 0;
    for _ in 0..100 {
        start = (start + KEY_STEP) % starts;
        assert_eq!(read_range(&db, &mut stmt, start, rows), rows, "{sql}");
    }
    let mut start = 0;
    bencher.bench_local(|| {
        start = (start + KEY_STEP) % starts;
        read_range(black_box(&db), black_box(&mut stmt), start, rows)
    });
}

/// Reads the range of `rows` keys from `start` and returns the number of rows.
fn read_range(db: &Database, stmt: &mut Statement, start: i64, rows: i64) -> i64 {
    bind_int(stmt, 1, start);
    bind_int(stmt, 2, start + rows);
    drain(db, stmt) as i64
}

/// Creates `t(id, k, v)` with `ROWS` rows and an index on `k`. `id` and `k` both take
/// each value from 0 to `ROWS - 1` once, but in a different order, so the index order
/// is not the table order.
fn seed_database() -> (Arc<Database>, Arc<Connection>) {
    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(MemoryIO::new());
    let db = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    // 64 MiB. The database is about 8 MiB, so after the first read no page leaves
    // the cache.
    execute(&db, &conn, "PRAGMA cache_size = -65536");
    execute(
        &db,
        &conn,
        "CREATE TABLE t(id INTEGER PRIMARY KEY, k INTEGER NOT NULL, v TEXT NOT NULL)",
    );
    execute(&db, &conn, "BEGIN");
    let mut insert = conn.prepare("INSERT INTO t VALUES (?1, ?2, ?3)").unwrap();
    for id in 0..ROWS {
        bind_int(&mut insert, 1, id);
        bind_int(&mut insert, 2, (id * K_FACTOR) % ROWS);
        insert
            .bind_at(
                3.try_into().unwrap(),
                Value::build_text(format!("value-{id:06}-{}", "x".repeat(48))),
            )
            .unwrap();
        drain(&db, &mut insert);
    }
    execute(&db, &conn, "COMMIT");
    execute(&db, &conn, "CREATE INDEX t_k ON t(k)");
    (db, conn)
}

/// Fails if the query plan of `sql` is not `expected`. A plan that seeks with only one
/// bound, or that scans, has a different plan text, so a change to the planner fails
/// the benchmark instead of changing its numbers.
fn assert_plan(db: &Database, conn: &Arc<Connection>, sql: &str, expected: &str) {
    let mut explain = conn.prepare(format!("EXPLAIN QUERY PLAN {sql}")).unwrap();
    let mut plan = Vec::new();
    loop {
        match explain.step().unwrap() {
            StepResult::Row => plan.push(explain.row().unwrap().get::<String>(3).unwrap()),
            StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => db.io.step().unwrap(),
            StepResult::Done => break,
            StepResult::Interrupt | StepResult::Busy => unreachable!(),
        }
    }
    assert_eq!(plan, [expected], "query plan of {sql}");
}

fn execute(db: &Database, conn: &Arc<Connection>, sql: &str) {
    let mut stmt = conn.prepare(sql).unwrap();
    drain(db, &mut stmt);
}

fn bind_int(stmt: &mut Statement, index: usize, value: i64) {
    stmt.bind_at(index.try_into().unwrap(), Value::from_i64(value))
        .unwrap();
}

/// Steps `stmt` to the end, resets it and returns the number of rows.
fn drain(db: &Database, stmt: &mut Statement) -> usize {
    let mut rows = 0;
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                black_box(stmt.row());
                rows += 1;
            }
            StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => db.io.step().unwrap(),
            StepResult::Done => break,
            StepResult::Interrupt | StepResult::Busy => unreachable!(),
        }
    }
    stmt.reset().unwrap();
    rows
}
