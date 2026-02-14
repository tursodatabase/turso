//! FTS Query Performance Benchmarks
//!
//! Measures full-text search query performance including:
//! - Cold query (first query after index creation, no cached directory)
//! - Warm query (repeated queries with cached directory)
//! - Insert + query lifecycle (write, commit, query)
//!
//! Run with: cargo bench --bench fts_benchmark --features fts

#[cfg(not(feature = "codspeed"))]
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
#[cfg(not(feature = "codspeed"))]
use pprof::criterion::{Output, PProfProfiler};

#[cfg(feature = "codspeed")]
use codspeed_criterion_compat::{criterion_group, criterion_main, BenchmarkId, Criterion};

use std::sync::Arc;
use tempfile::TempDir;
use turso_core::{Database, DatabaseOpts, OpenFlags, PlatformIO, StepResult};

#[cfg(not(target_family = "wasm"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Helper to execute a statement to completion, stepping through IO.
fn run_to_completion(
    stmt: &mut turso_core::Statement,
    db: &Arc<Database>,
) -> turso_core::Result<()> {
    loop {
        match stmt.step()? {
            StepResult::IO => {
                db.io.step()?;
            }
            StepResult::Done => break,
            StepResult::Row => {}
            StepResult::Interrupt | StepResult::Busy => {
                panic!("Unexpected step result");
            }
        }
    }
    Ok(())
}

/// Helper to step a statement and count result rows.
fn run_and_count_rows(
    stmt: &mut turso_core::Statement,
    db: &Arc<Database>,
) -> turso_core::Result<usize> {
    let mut count = 0;
    loop {
        match stmt.step()? {
            StepResult::IO => {
                db.io.step()?;
            }
            StepResult::Done => break,
            StepResult::Row => {
                count += 1;
            }
            StepResult::Interrupt | StepResult::Busy => {
                panic!("Unexpected step result");
            }
        }
    }
    Ok(count)
}

/// Setup a database with an FTS-indexed table populated with `row_count` rows.
fn setup_fts_db(temp_dir: &TempDir, row_count: usize) -> Arc<Database> {
    let db_path = temp_dir.path().join("fts_bench.db");
    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(PlatformIO::new().unwrap());
    let opts = DatabaseOpts::new().with_index_method(true);
    let db = Database::open_file_with_flags(
        io,
        db_path.to_str().unwrap(),
        OpenFlags::default(),
        opts,
        None,
    )
    .unwrap();
    let conn = db.connect().unwrap();

    // Create table and FTS index
    conn.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts (title, body)")
        .unwrap();

    // Insert rows in batches of 500
    let batch_size = 500;
    for batch_start in (0..row_count).step_by(batch_size) {
        let batch_end = (batch_start + batch_size).min(row_count);
        let mut sql = String::from("INSERT INTO docs (id, title, body) VALUES ");
        for i in batch_start..batch_end {
            if i > batch_start {
                sql.push(',');
            }
            // Vary content so term dictionaries have realistic distribution
            let word_a = match i % 7 {
                0 => "database",
                1 => "performance",
                2 => "optimization",
                3 => "benchmark",
                4 => "storage",
                5 => "indexing",
                _ => "computing",
            };
            let word_b = match i % 5 {
                0 => "systems",
                1 => "analysis",
                2 => "engineering",
                3 => "architecture",
                _ => "design",
            };
            sql.push_str(&format!(
                "({i}, '{word_a} document {i}', 'This is the body of document {i} about {word_a} and {word_b} with additional text for realistic content size')"
            ));
        }
        conn.execute(&sql).unwrap();
    }

    db
}

/// Benchmark: Cold FTS query (no cached directory — measures full loading pipeline)
///
/// This measures the worst-case: open_read must scan the BTree catalog,
/// load hot files, create the Tantivy Index, build a Reader+Searcher,
/// parse the query, and execute the search. Each iteration uses a fresh
/// connection to avoid directory cache hits.
fn bench_fts_cold_query(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("FTS Cold Query");
    group.sample_size(20); // Cold queries are slow; reduce samples

    for row_count in [1000, 5000, 10000] {
        let temp_dir = tempfile::tempdir().unwrap();
        let db = setup_fts_db(&temp_dir, row_count);

        group.bench_function(
            BenchmarkId::new("cold_query", format!("{row_count}_rows")),
            |b| {
                b.iter_custom(|iters| {
                    let mut total = std::time::Duration::ZERO;
                    for _ in 0..iters {
                        // Fresh connection = no cached directory
                        let conn = db.connect().unwrap();
                        let start = std::time::Instant::now();
                        let mut stmt = conn
                            .query(
                                "SELECT id, title FROM docs WHERE (title, body) MATCH 'database'",
                            )
                            .unwrap()
                            .unwrap();
                        let _rows = run_and_count_rows(&mut stmt, &db).unwrap();
                        total += start.elapsed();
                    }
                    total
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: Warm FTS query (cached directory — measures query-only path)
///
/// After the first query loads and caches the directory, subsequent queries
/// skip the catalog scan and PreloadingEssentials entirely. This measures
/// the pure query execution path: Index::open (from cached directory),
/// Reader+Searcher creation, query parsing, and search.
fn bench_fts_warm_query(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("FTS Warm Query");

    for row_count in [1000, 5000, 10000] {
        let temp_dir = tempfile::tempdir().unwrap();
        let db = setup_fts_db(&temp_dir, row_count);
        let conn = db.connect().unwrap();

        // Warm up: run one query to populate the directory cache
        let mut stmt = conn
            .query("SELECT id FROM docs WHERE (title, body) MATCH 'database'")
            .unwrap()
            .unwrap();
        run_to_completion(&mut stmt, &db).unwrap();

        group.bench_function(
            BenchmarkId::new("warm_query", format!("{row_count}_rows")),
            |b| {
                b.iter_custom(|iters| {
                    let mut total = std::time::Duration::ZERO;
                    for _ in 0..iters {
                        let start = std::time::Instant::now();
                        let mut stmt = conn
                            .query(
                                "SELECT id, title FROM docs WHERE (title, body) MATCH 'database'",
                            )
                            .unwrap()
                            .unwrap();
                        let _rows = run_and_count_rows(&mut stmt, &db).unwrap();
                        total += start.elapsed();
                    }
                    total
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: FTS query with different search selectivity
///
/// Measures how the number of matching documents affects query time.
/// "database" matches ~1/7 of docs, "performance" matches ~1/7,
/// "database performance" (AND) matches fewer.
fn bench_fts_query_selectivity(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("FTS Query Selectivity");

    let row_count = 10000;
    let temp_dir = tempfile::tempdir().unwrap();
    let db = setup_fts_db(&temp_dir, row_count);
    let conn = db.connect().unwrap();

    // Warm up
    let mut stmt = conn
        .query("SELECT id FROM docs WHERE (title, body) MATCH 'database'")
        .unwrap()
        .unwrap();
    run_to_completion(&mut stmt, &db).unwrap();

    let queries = [
        ("single_common_term", "database"),
        ("single_uncommon_term", "optimization"),
        ("two_term_and", "database engineering"),
        ("phrase_query", "\"database document\""),
    ];

    for (name, query_term) in queries {
        let sql = format!("SELECT id, title FROM docs WHERE (title, body) MATCH '{query_term}'");

        group.bench_function(BenchmarkId::new("selectivity", name), |b| {
            b.iter_custom(|iters| {
                let mut total = std::time::Duration::ZERO;
                for _ in 0..iters {
                    let start = std::time::Instant::now();
                    let mut stmt = conn.query(&sql).unwrap().unwrap();
                    let _rows = run_and_count_rows(&mut stmt, &db).unwrap();
                    total += start.elapsed();
                }
                total
            });
        });
    }

    group.finish();
}

/// Benchmark: Insert + query lifecycle
///
/// Measures the cost of inserting new rows, committing, and then querying.
/// This exercises the full write path (IndexWriter, segment creation, BTree flush)
/// followed by directory cache invalidation and a cold re-query.
fn bench_fts_insert_then_query(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("FTS Insert+Query Lifecycle");
    group.sample_size(20);

    for row_count in [1000, 5000] {
        let temp_dir = tempfile::tempdir().unwrap();
        let db = setup_fts_db(&temp_dir, row_count);
        let conn = db.connect().unwrap();

        // Use a shared counter that persists across warmup + sampling invocations
        let counter = std::cell::Cell::new(row_count + 1_000_000);

        group.bench_function(
            BenchmarkId::new("insert_query", format!("{row_count}_rows")),
            |b| {
                b.iter_custom(|iters| {
                    let mut total = std::time::Duration::ZERO;
                    for _ in 0..iters {
                        let start = std::time::Instant::now();

                        // Insert 10 new rows (use rowid=NULL to auto-assign)
                        let c = counter.get();
                        let mut sql = String::from("INSERT INTO docs (id, title, body) VALUES ");
                        for j in 0..10 {
                            if j > 0 {
                                sql.push(',');
                            }
                            let id = c + j;
                            sql.push_str(&format!(
                                "({id}, 'new document {id}', 'freshly inserted content about database systems')"
                            ));
                        }
                        counter.set(c + 10);
                        conn.execute(&sql).unwrap();

                        // Query (exercises cache invalidation + re-query)
                        let mut stmt = conn
                            .query(
                                "SELECT id, title FROM docs WHERE (title, body) MATCH 'database'",
                            )
                            .unwrap()
                            .unwrap();
                        let _rows = run_and_count_rows(&mut stmt, &db).unwrap();

                        total += start.elapsed();
                    }
                    total
                });
            },
        );
    }

    group.finish();
}

#[cfg(not(feature = "codspeed"))]
criterion_group! {
    name = fts_benches;
    config = Criterion::default()
        .with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)))
        .sample_size(50);
    targets = bench_fts_cold_query, bench_fts_warm_query, bench_fts_query_selectivity, bench_fts_insert_then_query
}

#[cfg(feature = "codspeed")]
criterion_group! {
    name = fts_benches;
    config = Criterion::default().sample_size(50);
    targets = bench_fts_cold_query, bench_fts_warm_query, bench_fts_query_selectivity, bench_fts_insert_then_query
}

criterion_main!(fts_benches);
