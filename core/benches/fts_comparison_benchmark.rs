//! End-to-end comparison of Turso FTS and SQLite FTS5.
//!
//! Both engines receive the same deterministic corpus through identical
//! batched INSERT statements. SQLite uses an external-content FTS5 table with
//! triggers, so both sides maintain a base table and a separate search index.
//!
//! The query benchmarks include SQL preparation and row materialization. The
//! bulk-ingest benchmark excludes database/schema creation but includes the
//! transaction and index maintenance.
//!
//! Run with:
//!   cargo bench -p turso_core --bench fts_comparison_benchmark --features fts
//!
//! Criterion's `change` percentages compare each benchmark ID with its previous
//! local run; they do not compare Turso with SQLite. For source-change
//! regressions, preserve and name the baseline explicitly:
//!   cargo bench -p turso_core --bench fts_comparison_benchmark --features fts \
//!     -- --save-baseline before
//!   cargo bench -p turso_core --bench fts_comparison_benchmark --features fts \
//!     -- --baseline before
//! Compare Turso and SQLite directly using their absolute times from the same run.

#[cfg(not(feature = "codspeed"))]
use criterion::{
    criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode, Throughput,
};
#[cfg(not(feature = "codspeed"))]
use pprof::criterion::{Output, PProfProfiler};
use turso_core::SqliteDialect;

#[cfg(feature = "codspeed")]
use codspeed_criterion_compat::{
    criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode, Throughput,
};

use std::sync::Arc;
use tempfile::TempDir;
use turso_core::{Database, DatabaseOpts, OpenFlags, PlatformIO, StepResult};

#[cfg(not(target_family = "wasm"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[cfg(not(feature = "codspeed"))]
macro_rules! iter_custom_or_iter {
    ($b:expr, |$iters:ident| $body:block) => {
        $b.iter_custom(|$iters| $body)
    };
}

#[cfg(feature = "codspeed")]
macro_rules! iter_custom_or_iter {
    ($b:expr, |$iters:ident| $body:block) => {
        $b.iter(|| {
            let $iters = 1;
            $body
        })
    };
}

const QUERY_ROW_COUNT: usize = 20_000;
const INGEST_ROW_COUNT: usize = 5_000;
const INSERT_BATCH_SIZE: usize = 500;

struct TursoDatabase {
    _temp_dir: TempDir,
    db: Arc<Database>,
    conn: Arc<turso_core::Connection>,
}

struct SqliteDatabase {
    _temp_dir: TempDir,
    conn: rusqlite::Connection,
}

struct QueryWorkload {
    name: &'static str,
    turso_sql: &'static str,
    sqlite_sql: &'static str,
    expected_hit_count: usize,
}

const QUERY_WORKLOADS: &[QueryWorkload] = &[
    QueryWorkload {
        name: "rare_term",
        turso_sql: "SELECT id, title FROM docs \
                    WHERE (title, body) MATCH 'traceidentifier'",
        sqlite_sql: "SELECT rowid, title FROM docs_fts \
                     WHERE docs_fts MATCH 'traceidentifier'",
        expected_hit_count: QUERY_ROW_COUNT / 1_000,
    },
    QueryWorkload {
        name: "common_term",
        turso_sql: "SELECT id, title FROM docs \
                    WHERE (title, body) MATCH 'database'",
        sqlite_sql: "SELECT rowid, title FROM docs_fts \
                     WHERE docs_fts MATCH 'database'",
        expected_hit_count: QUERY_ROW_COUNT / 5,
    },
    QueryWorkload {
        name: "phrase",
        turso_sql: "SELECT id, title FROM docs \
                    WHERE (title, body) MATCH '\"distributed systems\"'",
        sqlite_sql: "SELECT rowid, title FROM docs_fts \
                     WHERE docs_fts MATCH '\"distributed systems\"'",
        expected_hit_count: QUERY_ROW_COUNT / 20,
    },
    QueryWorkload {
        name: "ranked_top_10",
        turso_sql: "SELECT id, title, \
                           fts_score(title, body, 'database') AS score \
                    FROM docs \
                    WHERE fts_match(title, body, 'database') \
                    ORDER BY score DESC LIMIT 10",
        sqlite_sql: "SELECT rowid, title, bm25(docs_fts) AS score \
                     FROM docs_fts \
                     WHERE docs_fts MATCH 'database' \
                     ORDER BY score LIMIT 10",
        // The engines use different BM25 implementations, so ranking equality
        // is not a compatibility invariant. Both must still return a full top-k.
        expected_hit_count: 10,
    },
];

fn run_turso_statement(
    stmt: &mut turso_core::Statement,
    db: &Arc<Database>,
) -> turso_core::Result<usize> {
    let mut row_count = 0;
    loop {
        match stmt.step()? {
            StepResult::IO | StepResult::Yield => db.io.step()?,
            StepResult::Done => break,
            StepResult::Row => row_count += 1,
            StepResult::Interrupt | StepResult::Busy => {
                panic!("unexpected Turso step result")
            }
        }
    }
    Ok(row_count)
}

fn execute_turso(db: &Arc<Database>, conn: &Arc<turso_core::Connection>, sql: &str) {
    let mut stmt = conn.query(sql).unwrap().unwrap();
    run_turso_statement(&mut stmt, db).unwrap();
}

fn query_turso(db: &Arc<Database>, conn: &Arc<turso_core::Connection>, sql: &str) -> usize {
    let mut stmt = conn.query(sql).unwrap().unwrap();
    run_turso_statement(&mut stmt, db).unwrap()
}

fn query_sqlite(conn: &rusqlite::Connection, sql: &str) -> usize {
    let mut stmt = conn.prepare(sql).unwrap();
    let mut rows = stmt.query([]).unwrap();
    let mut row_count = 0;
    while rows.next().unwrap().is_some() {
        row_count += 1;
    }
    row_count
}

fn open_turso() -> TursoDatabase {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("turso_fts.db");
    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(PlatformIO::new().unwrap());
    let opts = DatabaseOpts::new().with_index_method(true);
    let db = Database::open_file_with_flags(
        io,
        db_path.to_str().unwrap(),
        OpenFlags::default(),
        opts,
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db.connect().unwrap();
    execute_turso(
        &db,
        &conn,
        "CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT, body TEXT)",
    );
    execute_turso(
        &db,
        &conn,
        "CREATE INDEX docs_fts ON docs USING fts (title, body)",
    );

    TursoDatabase {
        _temp_dir: temp_dir,
        db,
        conn,
    }
}

fn open_sqlite() -> SqliteDatabase {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("sqlite_fts5.db");
    let conn = rusqlite::Connection::open(db_path).unwrap();
    conn.execute_batch(
        "
        CREATE TABLE docs (
            id INTEGER PRIMARY KEY,
            title TEXT,
            body TEXT
        );
        CREATE VIRTUAL TABLE docs_fts USING fts5(
            title,
            body,
            content = 'docs',
            content_rowid = 'id',
            tokenize = 'unicode61'
        );
        CREATE TRIGGER docs_ai AFTER INSERT ON docs BEGIN
            INSERT INTO docs_fts(rowid, title, body)
            VALUES (new.id, new.title, new.body);
        END;
        ",
    )
    .unwrap();

    SqliteDatabase {
        _temp_dir: temp_dir,
        conn,
    }
}

fn insert_batches(row_count: usize) -> Vec<String> {
    let topics = [
        "database",
        "database",
        "storage",
        "networking",
        "compiler",
        "security",
        "analytics",
        "observability",
        "reliability",
        "runtime",
    ];
    let domains = [
        "cloud",
        "mobile",
        "backend",
        "embedded",
        "distributed",
        "transactional",
        "streaming",
    ];

    (0..row_count)
        .step_by(INSERT_BATCH_SIZE)
        .map(|batch_start| {
            let batch_end = (batch_start + INSERT_BATCH_SIZE).min(row_count);
            let mut sql = String::with_capacity((batch_end - batch_start) * 180);
            sql.push_str("INSERT INTO docs (id, title, body) VALUES ");
            for id in batch_start..batch_end {
                if id > batch_start {
                    sql.push(',');
                }
                let topic = topics[id % topics.len()];
                let domain = domains[(id * 3 + 1) % domains.len()];
                let phrase = if id % 20 == 0 {
                    "distributed systems"
                } else {
                    "production services"
                };
                let rare_term = if id % 1_000 == 0 {
                    " traceidentifier"
                } else {
                    ""
                };
                sql.push_str(&format!(
                    "({id}, '{topic} {domain} field guide {id}', \
                     'A practical guide for {domain} teams covering {topic} \
                      workflows, {phrase}, operational tradeoffs, failure \
                      recovery, and performance analysis.{rare_term}')"
                ));
            }
            sql
        })
        .collect()
}

fn populate_turso(database: &TursoDatabase, batches: &[String]) {
    execute_turso(&database.db, &database.conn, "BEGIN");
    for sql in batches {
        execute_turso(&database.db, &database.conn, sql);
    }
    execute_turso(&database.db, &database.conn, "COMMIT");
}

fn populate_sqlite(database: &SqliteDatabase, batches: &[String]) {
    database.conn.execute_batch("BEGIN").unwrap();
    for sql in batches {
        database.conn.execute(sql, []).unwrap();
    }
    database.conn.execute_batch("COMMIT").unwrap();
}

fn assert_workload_parity(turso: &TursoDatabase, sqlite: &SqliteDatabase) {
    assert_eq!(
        query_turso(&turso.db, &turso.conn, "SELECT id FROM docs"),
        QUERY_ROW_COUNT
    );
    assert_eq!(
        query_sqlite(&sqlite.conn, "SELECT id FROM docs"),
        QUERY_ROW_COUNT
    );

    for workload in QUERY_WORKLOADS {
        let turso_count = query_turso(&turso.db, &turso.conn, workload.turso_sql);
        let sqlite_count = query_sqlite(&sqlite.conn, workload.sqlite_sql);
        assert_eq!(
            turso_count, workload.expected_hit_count,
            "Turso {} hit count",
            workload.name
        );
        assert_eq!(
            sqlite_count, workload.expected_hit_count,
            "SQLite {} hit count",
            workload.name
        );
    }
}

#[turso_macros::codspeed_criterion_benchmark]
fn bench_bulk_ingest(criterion: &mut Criterion) {
    let batches = insert_batches(INGEST_ROW_COUNT);
    let mut group = criterion.benchmark_group("FTS comparison - bulk ingest");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(INGEST_ROW_COUNT as u64));

    group.bench_function(BenchmarkId::new("turso", INGEST_ROW_COUNT), |bencher| {
        iter_custom_or_iter!(bencher, |iterations| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iterations {
                let database = open_turso();
                let start = std::time::Instant::now();
                populate_turso(&database, &batches);
                total += start.elapsed();
            }
            total
        });
    });

    if !cfg!(feature = "codspeed") {
        group.bench_function(
            BenchmarkId::new("sqlite_fts5", INGEST_ROW_COUNT),
            |bencher| {
                iter_custom_or_iter!(bencher, |iterations| {
                    let mut total = std::time::Duration::ZERO;
                    for _ in 0..iterations {
                        let database = open_sqlite();
                        let start = std::time::Instant::now();
                        populate_sqlite(&database, &batches);
                        total += start.elapsed();
                    }
                    total
                });
            },
        );
    }

    group.finish();
}

#[turso_macros::codspeed_criterion_benchmark]
fn bench_queries(criterion: &mut Criterion) {
    let batches = insert_batches(QUERY_ROW_COUNT);
    let turso = open_turso();
    let sqlite = open_sqlite();
    populate_turso(&turso, &batches);
    populate_sqlite(&sqlite, &batches);
    assert_workload_parity(&turso, &sqlite);

    let mut group = criterion.benchmark_group("FTS comparison - warm queries");
    for workload in QUERY_WORKLOADS {
        group.bench_function(BenchmarkId::new(workload.name, "turso"), |bencher| {
            iter_custom_or_iter!(bencher, |iterations| {
                let mut total = std::time::Duration::ZERO;
                for _ in 0..iterations {
                    let start = std::time::Instant::now();
                    let row_count = query_turso(&turso.db, &turso.conn, workload.turso_sql);
                    std::hint::black_box(row_count);
                    total += start.elapsed();
                }
                total
            });
        });

        if !cfg!(feature = "codspeed") {
            group.bench_function(BenchmarkId::new(workload.name, "sqlite_fts5"), |bencher| {
                iter_custom_or_iter!(bencher, |iterations| {
                    let mut total = std::time::Duration::ZERO;
                    for _ in 0..iterations {
                        let start = std::time::Instant::now();
                        let row_count = query_sqlite(&sqlite.conn, workload.sqlite_sql);
                        std::hint::black_box(row_count);
                        total += start.elapsed();
                    }
                    total
                });
            });
        }
    }

    group.finish();
}

#[cfg(not(feature = "codspeed"))]
criterion_group! {
    name = fts_comparison_benches;
    config = Criterion::default()
        .with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_bulk_ingest, bench_queries
}

#[cfg(feature = "codspeed")]
criterion_group! {
    name = fts_comparison_benches;
    config = Criterion::default();
    targets = bench_bulk_ingest, bench_queries
}

criterion_main!(fts_comparison_benches);
