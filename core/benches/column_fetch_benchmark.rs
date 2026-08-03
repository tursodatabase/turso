//! Microbenchmark isolating the per-row record-header walk in op_column.
//!
//! Every `Insn::Column` re-parses the record header from byte zero, so a
//! `SELECT *` on an N-column table decodes O(N^2) serial-type varints per row.
//! These benches time the shapes that matter for that walk:
//!   - select_star_106 : SELECT * over a ClickBench-hits-width table (max pressure)
//!   - select_star_21  : SELECT * over a TPC-C-customer-width table
//!   - select_star_10  : SELECT * over a typical narrow OLTP table
//!   - late_single_10  : SELECT c9 FROM the 10-column table -- a narrow
//!     projection of one late column. This is the dominant OLTP access pattern
//!     and must NOT regress when the wide-table walk is optimized.
//!
//! The next three shapes exist because a header cache can lose in ways a
//! `SELECT *` bench never shows. A cache that parses the whole header on first
//! touch makes a two-column read of a 106-column table far slower than no cache
//! at all, which is how the last attempt at this regressed ClickBench:
//!   - narrow_two_106  : two far-apart columns of the wide table. Reading only
//!     c2 and c90 must not pay for the other 104 serial types.
//!   - scatter_106     : four columns read out of ascending order. Instruction
//!     fusion cannot help here at all, so this is the shape a cache has to win.
//!   - repeat_one_106  : one column read 32 times per row, the shape of the
//!     ClickBench `SUM(ResolutionWidth + N)` query that a cache exists for.
//!
//! Columns alternate INTEGER and TEXT so header varint widths and serial types
//! are non-uniform, like real schemas.
//!
//! Run:  cargo bench -p turso_core --bench column_fetch_benchmark

#[cfg(feature = "codspeed")]
use codspeed_criterion_compat::{
    black_box, criterion_group, criterion_main, BenchmarkId, Criterion,
};
#[cfg(not(feature = "codspeed"))]
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};

use std::sync::Arc;
use std::sync::LazyLock;
use std::time::Duration;
use tempfile::TempDir;
use turso_core::{Database, PlatformIO, SqliteDialect, StepResult};

#[cfg(not(target_family = "wasm"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// (table name, column count, row count). Row counts are sized so each scan is
/// long enough to measure while the whole database stays cache-resident.
const TABLES: &[(&str, usize, usize)] = &[
    ("t106", 106, 20_000),
    ("t21", 21, 50_000),
    ("t10", 10, 100_000),
];

/// The ClickBench query this whole line of work exists for reads one column
/// under many different aggregates, so the row's header is walked once per
/// aggregate. 32 repeats is enough to show the effect without a giant SQL string.
static REPEAT_ONE_SQL: LazyLock<String> = LazyLock::new(|| {
    let sums = (0..32)
        .map(|n| format!("SUM(c4 + {n})"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("SELECT {sums} FROM t106")
});

fn queries() -> Vec<(&'static str, String, usize)> {
    vec![
        ("select_star_106", "SELECT * FROM t106".into(), 20_000),
        ("select_star_21", "SELECT * FROM t21".into(), 50_000),
        ("select_star_10", "SELECT * FROM t10".into(), 100_000),
        ("late_single_10", "SELECT c9 FROM t10".into(), 100_000),
        ("narrow_two_106", "SELECT c2, c90 FROM t106".into(), 20_000),
        (
            "scatter_106",
            "SELECT c90, c2, c50, c7 FROM t106".into(),
            20_000,
        ),
        ("repeat_one_106", REPEAT_ONE_SQL.clone(), 20_000),
    ]
}

/// Seed a self-contained db file via rusqlite. Even columns are INTEGER, odd
/// columns are short TEXT, so serial types (and their varint sizes) vary
/// across the header.
fn seed_db() -> TempDir {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("columns.db");
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute_batch("PRAGMA journal_mode=DELETE;").unwrap();
    for (table, ncols, nrows) in TABLES {
        let cols = (0..*ncols)
            .map(|c| {
                if c % 2 == 0 {
                    format!("c{c} INTEGER")
                } else {
                    format!("c{c} TEXT")
                }
            })
            .collect::<Vec<_>>()
            .join(", ");
        conn.execute_batch(&format!("CREATE TABLE {table}({cols});"))
            .unwrap();
        let placeholders = (1..=*ncols)
            .map(|i| format!("?{i}"))
            .collect::<Vec<_>>()
            .join(", ");
        let tx = conn.unchecked_transaction().unwrap();
        {
            let mut ins = tx
                .prepare(&format!("INSERT INTO {table} VALUES ({placeholders})"))
                .unwrap();
            for i in 0..*nrows as i64 {
                let row = (0..*ncols)
                    .map(|c| {
                        if c % 2 == 0 {
                            rusqlite::types::Value::Integer(i.wrapping_mul(c as i64 + 1))
                        } else {
                            rusqlite::types::Value::Text(format!("v{}-{c}", i % 97))
                        }
                    })
                    .collect::<Vec<_>>();
                ins.execute(rusqlite::params_from_iter(row)).unwrap();
            }
        }
        tx.commit().unwrap();
    }
    dir
}

fn drain_turso(db: &Database, stmt: &mut turso_core::Statement) {
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                black_box(stmt.row());
            }
            StepResult::IO | StepResult::Yield => {
                db.io.step().unwrap();
            }
            StepResult::Done => break,
            StepResult::Interrupt | StepResult::Busy => unreachable!(),
        }
    }
    stmt.reset().unwrap();
}

#[turso_macros::codspeed_criterion_benchmark]
fn bench_column_fetch(criterion: &mut Criterion) {
    let dir = seed_db();
    let path = dir.path().join("columns.db");

    let mut group = criterion.benchmark_group("column_fetch");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(10));
    group.warm_up_time(Duration::from_secs(3));

    for (label, sql, nrows) in queries() {
        #[allow(clippy::arc_with_non_send_sync)]
        let io = Arc::new(PlatformIO::new().unwrap());
        let db = Database::open_file(io, path.to_str().unwrap(), Arc::new(SqliteDialect)).unwrap();
        let conn = db.connect().unwrap();
        // The whole db is ~30MB; a 64MB page cache keeps every scan CPU-bound
        // so the measurement isolates record decoding rather than IO.
        {
            let mut p = conn.prepare("PRAGMA cache_size=-65536").unwrap();
            while !matches!(p.step().unwrap(), StepResult::Done) {
                db.io.step().unwrap();
            }
        }
        group.bench_with_input(BenchmarkId::new(label, nrows), &nrows, |b, _| {
            let mut stmt = conn.prepare(sql.as_str()).unwrap();
            b.iter(|| drain_turso(&db, &mut stmt));
        });
    }
    group.finish();
}

criterion_group!(benches, bench_column_fetch);
criterion_main!(benches);
