//! Benchmark for SELECT * over different shapes of tables
//!
//!   - select_star_106 : SELECT * over a 106-column table
//!   - select_star_21  : SELECT * over a 21-column table
//!   - select_star_10  : SELECT * over a 10-column table
//!
//! Each scan runs in both journal modes, suffixed `_wal` and `_mvcc`.
//!
//! Run:  cargo bench -p turso_core --bench select_star_benchmark

#[cfg(feature = "codspeed")]
use codspeed_criterion_compat::{
    black_box, criterion_group, criterion_main, BenchmarkId, Criterion,
};
#[cfg(not(feature = "codspeed"))]
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};

use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use turso_core::{Connection, Database, PlatformIO, SqliteDialect, Statement, StepResult, Value};

#[cfg(not(target_family = "wasm"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// (table name, column count, row count)
const TABLES: &[(&str, usize, usize)] = &[
    ("t106", 106, 20_000),
    ("t21", 21, 50_000),
    ("t10", 10, 100_000),
];

#[derive(Clone, Copy)]
enum Mode {
    Wal,
    Mvcc,
}

impl Mode {
    fn suffix(self) -> &'static str {
        match self {
            Mode::Wal => "wal",
            Mode::Mvcc => "mvcc",
        }
    }
}

struct Fixture {
    _dir: TempDir,
    db: Arc<Database>,
    conn: Arc<Connection>,
}

fn seed_db(mode: Mode) -> Fixture {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("columns.db");
    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io, path.to_str().unwrap(), Arc::new(SqliteDialect)).unwrap();
    let conn = db.connect().unwrap();

    if matches!(mode, Mode::Mvcc) {
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
    }
    conn.execute("PRAGMA cache_size=-65536").unwrap(); //negative means kibibytes (https://sqlite.org/pragma.html#pragma_cache_size)

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
        conn.execute(format!("CREATE TABLE {table}({cols})"))
            .unwrap();

        let placeholders = (1..=*ncols)
            .map(|i| format!("?{i}"))
            .collect::<Vec<_>>()
            .join(", ");
        conn.execute("BEGIN").unwrap();
        let mut insert = conn
            .prepare(format!("INSERT INTO {table} VALUES ({placeholders})"))
            .unwrap();
        for i in 0..*nrows as i64 {
            for c in 0..*ncols {
                let value = if c % 2 == 0 {
                    Value::from_i64(i.wrapping_mul(c as i64 + 1))
                } else {
                    Value::build_text(format!("v{}-{c}", i % 97))
                };
                insert.bind_at((c + 1).try_into().unwrap(), value).unwrap();
            }
            drive_stmt_to_completion(&db, &mut insert);
        }
        conn.execute("COMMIT").unwrap();
    }

    Fixture {
        _dir: dir,
        db,
        conn,
    }
}

fn drive_stmt_to_completion(db: &Database, stmt: &mut Statement) -> usize {
    let mut rows = 0;
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                black_box(stmt.row());
                rows += 1;
            }
            StepResult::IO | StepResult::Yield => {
                db.io.step().unwrap();
            }
            StepResult::Done => break,
            StepResult::Interrupt | StepResult::Busy => unreachable!(),
        }
    }
    stmt.reset().unwrap();
    rows
}

#[turso_macros::codspeed_criterion_benchmark]
fn bench_select_star(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("column_fetch");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(10));
    group.warm_up_time(Duration::from_secs(3));

    for mode in [Mode::Wal, Mode::Mvcc] {
        let fixture = seed_db(mode);
        for (table, ncols, nrows) in TABLES {
            let label = format!("select_star_{ncols}_{}", mode.suffix());
            group.bench_with_input(BenchmarkId::new(label, nrows), nrows, |b, _| {
                let mut stmt = fixture
                    .conn
                    .prepare(format!("SELECT * FROM {table}"))
                    .unwrap();
                assert_eq!(
                    drive_stmt_to_completion(&fixture.db, &mut stmt),
                    *nrows,
                    "{table} should have been seeded with {nrows} rows"
                );
                b.iter(|| drive_stmt_to_completion(&fixture.db, &mut stmt));
            });
        }
    }
    group.finish();
}

criterion_group!(benches, bench_select_star);
criterion_main!(benches);
