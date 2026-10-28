//! Scans the small columns of a table whose rows also hold a large TEXT value
//! stored in overflow pages. The queries never read the large value, so they
//! should copy only the bytes of the columns they read.
//!
//!   - leading_columns : SELECT doc_id, rev, saved_at FROM revisions
//!   - one_column      : SELECT saved_at FROM revisions
//!   - after_large     : SELECT rev, tail FROM revisions  (tail is stored after the large value)
//!
//! Each query runs on turso and on SQLite (set DISABLE_RUSQLITE_BENCHMARK to
//! skip SQLite).
//!
//! Run:  cargo bench -p turso_core --bench overflow_scan_benchmark

#[cfg(feature = "codspeed")]
use codspeed_criterion_compat::{
    black_box, criterion_group, criterion_main, BenchmarkId, Criterion,
};
#[cfg(not(feature = "codspeed"))]
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};

use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use turso_core::{ConnectOpts, Database, PlatformIO, SqliteDialect, Statement, StepResult};

#[cfg(not(target_family = "wasm"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const ROWS: i64 = 1_000;
/// Size of the large value in KB, picked by row; about 25 KB on average.
const BODY_KB: [usize; 10] = [8, 10, 12, 14, 16, 18, 24, 32, 48, 72];

const QUERIES: &[(&str, &str)] = &[
    (
        "leading_columns",
        "SELECT doc_id, rev, saved_at FROM revisions",
    ),
    ("one_column", "SELECT saved_at FROM revisions"),
    ("after_large", "SELECT rev, tail FROM revisions"),
];

#[turso_macros::codspeed_criterion_benchmark]
fn bench_overflow_scan(criterion: &mut Criterion) {
    let enable_rusqlite = std::env::var("DISABLE_RUSQLITE_BENCHMARK").is_err();
    let dir = seed_db();
    let path = dir.path().join("revisions.db");

    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io, path.to_str().unwrap(), Arc::new(SqliteDialect)).unwrap();
    let conn = db.connect(ConnectOpts::new()).unwrap();
    conn.execute("PRAGMA cache_size=-65536").unwrap();

    let sqlite_conn = rusqlite::Connection::open(&path).unwrap();
    sqlite_conn
        .execute_batch("PRAGMA cache_size=-65536")
        .unwrap();

    let mut group = criterion.benchmark_group("overflow_scan");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(5));
    group.warm_up_time(Duration::from_secs(1));

    for (label, sql) in QUERIES {
        group.bench_function(BenchmarkId::new("turso", label), |b| {
            let mut stmt = conn.prepare(*sql).unwrap();
            assert_eq!(drain_turso(&db, &mut stmt), ROWS as usize);
            b.iter(|| drain_turso(&db, &mut stmt));
        });

        if enable_rusqlite {
            group.bench_function(BenchmarkId::new("sqlite", label), |b| {
                let mut stmt = sqlite_conn.prepare(sql).unwrap();
                let columns = stmt.column_count();
                b.iter(|| {
                    let mut rows = stmt.raw_query();
                    while let Some(row) = rows.next().unwrap() {
                        for i in 0..columns {
                            black_box(row.get_ref_unwrap(i));
                        }
                    }
                });
            });
        }
    }
    group.finish();
}

/// Writes the table with SQLite and checkpoints it, so both engines read the
/// same database file.
fn seed_db() -> TempDir {
    let dir = TempDir::new().unwrap();
    let conn = rusqlite::Connection::open(dir.path().join("revisions.db")).unwrap();
    conn.pragma_update(None, "journal_mode", "WAL").unwrap();
    conn.execute_batch(
        "CREATE TABLE revisions (
            doc_id TEXT NOT NULL, rev INTEGER NOT NULL, saved_at INTEGER NOT NULL,
            body_sha256 TEXT NOT NULL, body TEXT NOT NULL, tail TEXT NOT NULL,
            PRIMARY KEY (doc_id, rev));",
    )
    .unwrap();
    let tx = conn.unchecked_transaction().unwrap();
    {
        let mut insert = tx
            .prepare("INSERT INTO revisions VALUES (?1, ?2, ?3, ?4, ?5, 'tail')")
            .unwrap();
        for i in 0..ROWS {
            let body = "x".repeat(BODY_KB[i as usize % BODY_KB.len()] * 1024);
            insert
                .execute((
                    format!("doc-{:08}", i / 10),
                    i % 10,
                    i * 100,
                    "0".repeat(64),
                    body,
                ))
                .unwrap();
        }
    }
    tx.commit().unwrap();
    conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()))
        .unwrap();
    dir
}

fn drain_turso(db: &Database, stmt: &mut Statement) -> usize {
    let mut rows = 0;
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                for value in row.get_values() {
                    black_box(value);
                }
                rows += 1;
            }
            StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => {
                db.io.step().unwrap();
            }
            StepResult::Done => break,
            StepResult::Interrupt | StepResult::Busy => unreachable!(),
        }
    }
    stmt.reset().unwrap();
    rows
}

criterion_group!(benches, bench_overflow_scan);
criterion_main!(benches);
