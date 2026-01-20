//! Write Performance Benchmarks
//!
//! This module contains benchmarks specifically designed to measure and identify
//! write/INSERT performance bottlenecks, including:
//! - Index overhead impact
//! - Transaction size impact
//! - Sequential vs random key patterns
//! - UPDATE vs INSERT performance
//!
//! Run with: cargo bench --bench write_perf_benchmark

#[cfg(not(feature = "codspeed"))]
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
#[cfg(not(feature = "codspeed"))]
use pprof::criterion::{Output, PProfProfiler};

#[cfg(feature = "codspeed")]
use codspeed_criterion_compat::{
    criterion_group, criterion_main, BenchmarkId, Criterion, Throughput,
};

use std::sync::Arc;
use tempfile::TempDir;
use turso_core::{Database, PlatformIO, StepResult};

#[cfg(not(target_family = "wasm"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Helper to execute a statement to completion
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

/// Helper to setup a limbo database with the given schema
fn setup_limbo(temp_dir: &TempDir, schema: &str) -> Arc<Database> {
    setup_limbo_with_sync(temp_dir, schema, true)
}

/// Helper to setup a limbo database with optional sync
fn setup_limbo_with_sync(temp_dir: &TempDir, schema: &str, sync_on: bool) -> Arc<Database> {
    let db_path = temp_dir.path().join("bench.db");
    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io, db_path.to_str().unwrap()).unwrap();
    let conn = db.connect().unwrap();

    // Set synchronous mode
    let sync_mode = if sync_on { "FULL" } else { "OFF" };
    let mut stmt = conn
        .query(format!("PRAGMA synchronous = {sync_mode}"))
        .unwrap()
        .unwrap();
    run_to_completion(&mut stmt, &db).unwrap();

    // Execute schema
    let mut stmt = conn.query(schema).unwrap().unwrap();
    run_to_completion(&mut stmt, &db).unwrap();

    db
}

/// Helper to setup rusqlite with the given schema
fn setup_rusqlite(temp_dir: &TempDir, schema: &str) -> rusqlite::Connection {
    let db_path = temp_dir.path().join("bench.db");
    let conn = rusqlite::Connection::open(db_path).unwrap();
    conn.pragma_update(None, "synchronous", "FULL").unwrap();
    conn.pragma_update(None, "journal_mode", "WAL").unwrap();
    conn.pragma_update(None, "locking_mode", "EXCLUSIVE")
        .unwrap();
    // Use execute_batch to handle multiple statements (e.g., CREATE TABLE + CREATE INDEX)
    conn.execute_batch(schema).unwrap();
    conn
}

/// Benchmark: Impact of indexes on INSERT performance
///
/// This benchmark measures how adding indexes affects INSERT throughput.
/// Each index adds overhead due to:
/// 1. Seek operations for uniqueness checks
/// 2. Additional B-tree insertions
/// 3. Page splits on index pages
fn bench_index_impact(criterion: &mut Criterion) {
    let enable_rusqlite =
        std::env::var("DISABLE_RUSQLITE_BENCHMARK").is_err() && !cfg!(feature = "codspeed");
    let batch_size = 100;

    let mut group = criterion.benchmark_group("Index Impact on INSERT");
    group.throughput(Throughput::Elements(batch_size as u64));

    // Test configurations: (name, schema, insert_sql)
    let configs = [
        (
            "0_indexes",
            "CREATE TABLE test (id INTEGER, val1 TEXT, val2 INTEGER, val3 REAL)",
            "INSERT INTO test VALUES ",
        ),
        (
            "1_index_pk",
            "CREATE TABLE test (id INTEGER PRIMARY KEY, val1 TEXT, val2 INTEGER, val3 REAL)",
            "INSERT INTO test VALUES ",
        ),
        (
            "2_indexes",
            "CREATE TABLE test (id INTEGER PRIMARY KEY, val1 TEXT, val2 INTEGER, val3 REAL); \
             CREATE INDEX idx_val2 ON test(val2)",
            "INSERT INTO test VALUES ",
        ),
        (
            "3_indexes",
            "CREATE TABLE test (id INTEGER PRIMARY KEY, val1 TEXT, val2 INTEGER, val3 REAL); \
             CREATE INDEX idx_val2 ON test(val2); \
             CREATE INDEX idx_val3 ON test(val3)",
            "INSERT INTO test VALUES ",
        ),
    ];

    for (name, schema, insert_prefix) in configs {
        // Build multi-row insert statement
        let mut values = String::from(insert_prefix);
        for i in 0..batch_size {
            if i > 0 {
                values.push(',');
            }
            values.push_str(&format!("({}, 'value_{}', {}, {}.5)", i, i, i * 10, i));
        }

        // Limbo benchmark
        let temp_dir = tempfile::tempdir().unwrap();
        let db = setup_limbo(&temp_dir, schema);
        let conn = db.connect().unwrap();

        group.bench_function(BenchmarkId::new("limbo", name), |b| {
            let mut insert_stmt = conn.prepare(&values).unwrap();
            let mut delete_stmt = conn.query("DELETE FROM test").unwrap().unwrap();
            b.iter_custom(|iters| {
                let mut total = std::time::Duration::ZERO;
                for _ in 0..iters {
                    let start = std::time::Instant::now();
                    run_to_completion(&mut insert_stmt, &db).unwrap();
                    total += start.elapsed();
                    insert_stmt.reset();
                    // Clear table for next iteration (not timed)
                    run_to_completion(&mut delete_stmt, &db).unwrap();
                    delete_stmt.reset();
                }
                total
            });
        });

        // SQLite benchmark
        if enable_rusqlite {
            let temp_dir = tempfile::tempdir().unwrap();
            let sqlite_conn = setup_rusqlite(&temp_dir, schema);

            group.bench_function(BenchmarkId::new("sqlite", name), |b| {
                let mut stmt = sqlite_conn.prepare(&values).unwrap();
                b.iter_custom(|iters| {
                    let mut total = std::time::Duration::ZERO;
                    for _ in 0..iters {
                        let start = std::time::Instant::now();
                        stmt.raw_execute().unwrap();
                        total += start.elapsed();
                        // Clear table for next iteration (not timed)
                        sqlite_conn.execute("DELETE FROM test", []).unwrap();
                    }
                    total
                });
            });
        }
    }

    group.finish();
}

/// Benchmark: Transaction size impact on INSERT throughput
///
/// Measures how the number of rows per transaction affects throughput.
/// Larger transactions amortize commit overhead but increase memory pressure.
fn bench_transaction_size(criterion: &mut Criterion) {
    let enable_rusqlite =
        std::env::var("DISABLE_RUSQLITE_BENCHMARK").is_err() && !cfg!(feature = "codspeed");

    let mut group = criterion.benchmark_group("Transaction Size Impact");

    // Different transaction sizes (rows per transaction)
    let tx_sizes = [1, 10, 50, 100, 500, 1000];

    for tx_size in tx_sizes {
        group.throughput(Throughput::Elements(tx_size as u64));

        // Build multi-row insert
        let mut values = String::from("INSERT INTO test VALUES ");
        for i in 0..tx_size {
            if i > 0 {
                values.push(',');
            }
            values.push_str(&format!("({i}, 'data_{i}')"));
        }

        // Limbo benchmark
        let temp_dir = tempfile::tempdir().unwrap();
        let db = setup_limbo(
            &temp_dir,
            "CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)",
        );
        let conn = db.connect().unwrap();

        group.bench_function(BenchmarkId::new("limbo", format!("{tx_size}_rows")), |b| {
            let mut begin = conn.query("BEGIN").unwrap().unwrap();
            let mut commit = conn.query("COMMIT").unwrap().unwrap();
            let mut insert_stmt = conn.prepare(&values).unwrap();
            let mut delete_stmt = conn.query("DELETE FROM test").unwrap().unwrap();

            b.iter_custom(|iters| {
                let mut total = std::time::Duration::ZERO;
                for _ in 0..iters {
                    let start = std::time::Instant::now();
                    run_to_completion(&mut begin, &db).unwrap();
                    begin.reset();
                    run_to_completion(&mut insert_stmt, &db).unwrap();
                    insert_stmt.reset();
                    run_to_completion(&mut commit, &db).unwrap();
                    commit.reset();
                    total += start.elapsed();
                    // Clear for next iteration (not timed)
                    run_to_completion(&mut delete_stmt, &db).unwrap();
                    delete_stmt.reset();
                }
                total
            });
        });

        // SQLite benchmark
        if enable_rusqlite {
            let temp_dir = tempfile::tempdir().unwrap();
            let sqlite_conn = setup_rusqlite(
                &temp_dir,
                "CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)",
            );

            group.bench_function(BenchmarkId::new("sqlite", format!("{tx_size}_rows")), |b| {
                b.iter_custom(|iters| {
                    let mut total = std::time::Duration::ZERO;
                    for _ in 0..iters {
                        let start = std::time::Instant::now();
                        sqlite_conn.execute("BEGIN", []).unwrap();
                        sqlite_conn.execute(&values, []).unwrap();
                        sqlite_conn.execute("COMMIT", []).unwrap();
                        total += start.elapsed();
                        // Clear for next iteration (not timed)
                        sqlite_conn.execute("DELETE FROM test", []).unwrap();
                    }
                    total
                });
            });
        }
    }

    group.finish();
}

/// Benchmark: Sequential vs Random key insertion patterns
///
/// Sequential keys (monotonically increasing) are typically faster because:
/// 1. Balance quick path can be used (appending to rightmost leaf)
/// 2. Better cache locality
/// 3. Fewer page splits
fn bench_key_pattern(criterion: &mut Criterion) {
    let enable_rusqlite =
        std::env::var("DISABLE_RUSQLITE_BENCHMARK").is_err() && !cfg!(feature = "codspeed");
    let batch_size = 100;

    let mut group = criterion.benchmark_group("Key Pattern Impact");
    group.throughput(Throughput::Elements(batch_size as u64));

    // Generate random keys (pre-computed for consistency)
    let random_keys: Vec<i64> = (0..batch_size)
        .map(|i| {
            // Simple LCG for reproducible "random" keys
            let mut x = (i as i64 * 1103515245 + 12345) % (1 << 31);
            x = x.abs();
            x
        })
        .collect();

    // Sequential insert
    let mut seq_values = String::from("INSERT INTO test VALUES ");
    for i in 0..batch_size {
        if i > 0 {
            seq_values.push(',');
        }
        seq_values.push_str(&format!("({i}, 'data_{i}')"));
    }

    // Random insert
    let mut rand_values = String::from("INSERT INTO test VALUES ");
    for (i, key) in random_keys.iter().enumerate() {
        if i > 0 {
            rand_values.push(',');
        }
        rand_values.push_str(&format!("({key}, 'data_{i}')"));
    }

    // Limbo sequential
    let temp_dir = tempfile::tempdir().unwrap();
    let db = setup_limbo(
        &temp_dir,
        "CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)",
    );
    let conn = db.connect().unwrap();

    group.bench_function(BenchmarkId::new("limbo", "sequential_keys"), |b| {
        let mut stmt = conn.prepare(&seq_values).unwrap();
        let mut delete_stmt = conn.query("DELETE FROM test").unwrap().unwrap();
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let start = std::time::Instant::now();
                run_to_completion(&mut stmt, &db).unwrap();
                total += start.elapsed();
                stmt.reset();
                run_to_completion(&mut delete_stmt, &db).unwrap();
                delete_stmt.reset();
            }
            total
        });
    });

    // Limbo random
    let temp_dir = tempfile::tempdir().unwrap();
    let db = setup_limbo(
        &temp_dir,
        "CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)",
    );
    let conn = db.connect().unwrap();

    group.bench_function(BenchmarkId::new("limbo", "random_keys"), |b| {
        let mut stmt = conn.prepare(&rand_values).unwrap();
        let mut delete_stmt = conn.query("DELETE FROM test").unwrap().unwrap();
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let start = std::time::Instant::now();
                run_to_completion(&mut stmt, &db).unwrap();
                total += start.elapsed();
                stmt.reset();
                run_to_completion(&mut delete_stmt, &db).unwrap();
                delete_stmt.reset();
            }
            total
        });
    });

    if enable_rusqlite {
        // SQLite sequential
        let temp_dir = tempfile::tempdir().unwrap();
        let sqlite_conn = setup_rusqlite(
            &temp_dir,
            "CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)",
        );

        group.bench_function(BenchmarkId::new("sqlite", "sequential_keys"), |b| {
            let mut stmt = sqlite_conn.prepare(&seq_values).unwrap();
            b.iter_custom(|iters| {
                let mut total = std::time::Duration::ZERO;
                for _ in 0..iters {
                    let start = std::time::Instant::now();
                    stmt.raw_execute().unwrap();
                    total += start.elapsed();
                    sqlite_conn.execute("DELETE FROM test", []).unwrap();
                }
                total
            });
        });

        // SQLite random
        let temp_dir = tempfile::tempdir().unwrap();
        let sqlite_conn = setup_rusqlite(
            &temp_dir,
            "CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)",
        );

        group.bench_function(BenchmarkId::new("sqlite", "random_keys"), |b| {
            let mut stmt = sqlite_conn.prepare(&rand_values).unwrap();
            b.iter_custom(|iters| {
                let mut total = std::time::Duration::ZERO;
                for _ in 0..iters {
                    let start = std::time::Instant::now();
                    stmt.raw_execute().unwrap();
                    total += start.elapsed();
                    sqlite_conn.execute("DELETE FROM test", []).unwrap();
                }
                total
            });
        });
    }

    group.finish();
}

/// Benchmark: UPDATE vs INSERT performance
///
/// Updates may have different performance characteristics due to:
/// 1. Required seek to find existing row
/// 2. Potential in-place update vs delete+insert
/// 3. Index maintenance on modified columns
fn bench_update_performance(criterion: &mut Criterion) {
    let enable_rusqlite =
        std::env::var("DISABLE_RUSQLITE_BENCHMARK").is_err() && !cfg!(feature = "codspeed");

    let mut group = criterion.benchmark_group("UPDATE Performance");

    // Pre-populate table and measure update throughput
    let row_count = 1000;
    let update_count = 100;

    group.throughput(Throughput::Elements(update_count as u64));

    // Build initial data insert
    let mut initial_insert = String::from("INSERT INTO test VALUES ");
    for i in 0..row_count {
        if i > 0 {
            initial_insert.push(',');
        }
        initial_insert.push_str(&format!("({i}, 'initial_{i}')"));
    }

    // Build batch update (updates middle rows)
    let mut batch_update = String::new();
    let start = row_count / 2 - update_count / 2;
    for i in 0..update_count {
        if i > 0 {
            batch_update.push_str("; ");
        }
        batch_update.push_str(&format!(
            "UPDATE test SET data = 'updated_{}' WHERE id = {}",
            i,
            start + i
        ));
    }

    // Limbo benchmark
    let temp_dir = tempfile::tempdir().unwrap();
    let db = setup_limbo(
        &temp_dir,
        "CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)",
    );
    let conn = db.connect().unwrap();

    // Insert initial data
    let mut stmt = conn.prepare(&initial_insert).unwrap();
    run_to_completion(&mut stmt, &db).unwrap();

    group.bench_function(BenchmarkId::new("limbo", "batch_update"), |b| {
        let mut stmt = conn.prepare(&batch_update).unwrap();
        b.iter(|| {
            run_to_completion(&mut stmt, &db).unwrap();
            stmt.reset();
        });
    });

    // SQLite benchmark
    if enable_rusqlite {
        let temp_dir = tempfile::tempdir().unwrap();
        let sqlite_conn = setup_rusqlite(
            &temp_dir,
            "CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)",
        );
        sqlite_conn.execute(&initial_insert, []).unwrap();

        group.bench_function(BenchmarkId::new("sqlite", "batch_update"), |b| {
            b.iter(|| {
                sqlite_conn.execute_batch(&batch_update).unwrap();
            });
        });
    }

    group.finish();
}

/// Benchmark: DELETE performance
///
/// Measures DELETE throughput with different patterns
fn bench_delete_performance(criterion: &mut Criterion) {
    let enable_rusqlite =
        std::env::var("DISABLE_RUSQLITE_BENCHMARK").is_err() && !cfg!(feature = "codspeed");

    let mut group = criterion.benchmark_group("DELETE Performance");

    let row_count = 1000;
    let delete_count = 100;

    group.throughput(Throughput::Elements(delete_count as u64));

    // Build initial data insert
    let mut initial_insert = String::from("INSERT INTO test VALUES ");
    for i in 0..row_count {
        if i > 0 {
            initial_insert.push(',');
        }
        initial_insert.push_str(&format!("({i}, 'data_{i}')"));
    }

    // Build range delete
    let start = row_count / 2 - delete_count / 2;
    let end = start + delete_count;
    let range_delete = format!("DELETE FROM test WHERE id >= {start} AND id < {end}");

    // Limbo benchmark
    let temp_dir = tempfile::tempdir().unwrap();
    let db = setup_limbo(
        &temp_dir,
        "CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)",
    );
    let conn = db.connect().unwrap();

    group.bench_function(BenchmarkId::new("limbo", "range_delete"), |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                // Re-insert data
                let mut stmt = conn.prepare(&initial_insert).unwrap();
                run_to_completion(&mut stmt, &db).unwrap();

                // Time only the delete
                let start = std::time::Instant::now();
                let mut stmt = conn.prepare(&range_delete).unwrap();
                run_to_completion(&mut stmt, &db).unwrap();
                total += start.elapsed();

                // Clean up for next iteration
                let mut stmt = conn.query("DELETE FROM test").unwrap().unwrap();
                run_to_completion(&mut stmt, &db).unwrap();
            }
            total
        });
    });

    // SQLite benchmark
    if enable_rusqlite {
        let temp_dir = tempfile::tempdir().unwrap();
        let sqlite_conn = setup_rusqlite(
            &temp_dir,
            "CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)",
        );

        group.bench_function(BenchmarkId::new("sqlite", "range_delete"), |b| {
            b.iter_custom(|iters| {
                let mut total = std::time::Duration::ZERO;
                for _ in 0..iters {
                    // Re-insert data
                    sqlite_conn.execute(&initial_insert, []).unwrap();

                    // Time only the delete
                    let start = std::time::Instant::now();
                    sqlite_conn.execute(&range_delete, []).unwrap();
                    total += start.elapsed();

                    // Clean up for next iteration
                    sqlite_conn.execute("DELETE FROM test", []).unwrap();
                }
                total
            });
        });
    }

    group.finish();
}

/// Benchmark: Large transaction commit (many dirty pages)
///
/// Specifically targets the commit_dirty_pages path with many pages
fn bench_large_transaction_commit(criterion: &mut Criterion) {
    let enable_rusqlite =
        std::env::var("DISABLE_RUSQLITE_BENCHMARK").is_err() && !cfg!(feature = "codspeed");

    let mut group = criterion.benchmark_group("Large Transaction Commit");

    // Insert enough rows to dirty many pages (assuming 4KB pages, ~100 rows per page for small rows)
    let row_counts = [100, 1000, 5000, 10000];

    for row_count in row_counts {
        group.throughput(Throughput::Elements(row_count as u64));

        // Build large insert
        let mut values = String::from("INSERT INTO test VALUES ");
        for i in 0..row_count {
            if i > 0 {
                values.push(',');
            }
            // Use larger row size to dirty more pages
            values.push_str(&format!(
                "({}, '{}', {})",
                i,
                "x".repeat(100), // 100 byte string per row
                i * 10
            ));
        }

        // Limbo benchmark
        let temp_dir = tempfile::tempdir().unwrap();
        let db = setup_limbo(
            &temp_dir,
            "CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT, val INTEGER)",
        );
        let conn = db.connect().unwrap();

        group.bench_function(
            BenchmarkId::new("limbo", format!("{row_count}_rows")),
            |b| {
                b.iter_custom(|iters| {
                    let mut total = std::time::Duration::ZERO;
                    for _ in 0..iters {
                        // BEGIN
                        let mut stmt = conn.query("BEGIN").unwrap().unwrap();
                        run_to_completion(&mut stmt, &db).unwrap();

                        // INSERT (not timed separately - we want full transaction)
                        let start = std::time::Instant::now();
                        let mut stmt = conn.prepare(&values).unwrap();
                        run_to_completion(&mut stmt, &db).unwrap();

                        // COMMIT
                        let mut stmt = conn.query("COMMIT").unwrap().unwrap();
                        run_to_completion(&mut stmt, &db).unwrap();
                        total += start.elapsed();

                        // Clean up
                        let mut stmt = conn.query("DELETE FROM test").unwrap().unwrap();
                        run_to_completion(&mut stmt, &db).unwrap();
                    }
                    total
                });
            },
        );

        // SQLite benchmark
        if enable_rusqlite {
            let temp_dir = tempfile::tempdir().unwrap();
            let sqlite_conn = setup_rusqlite(
                &temp_dir,
                "CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT, val INTEGER)",
            );

            group.bench_function(
                BenchmarkId::new("sqlite", format!("{row_count}_rows")),
                |b| {
                    b.iter_custom(|iters| {
                        let mut total = std::time::Duration::ZERO;
                        for _ in 0..iters {
                            sqlite_conn.execute("BEGIN", []).unwrap();

                            let start = std::time::Instant::now();
                            sqlite_conn.execute(&values, []).unwrap();
                            sqlite_conn.execute("COMMIT", []).unwrap();
                            total += start.elapsed();

                            sqlite_conn.execute("DELETE FROM test", []).unwrap();
                        }
                        total
                    });
                },
            );
        }
    }

    group.finish();
}

/// Benchmark: Fsync overhead isolation
///
/// Compares INSERT performance with sync=FULL vs sync=OFF to isolate fsync cost
fn bench_fsync_overhead(criterion: &mut Criterion) {
    let enable_rusqlite =
        std::env::var("DISABLE_RUSQLITE_BENCHMARK").is_err() && !cfg!(feature = "codspeed");
    let batch_size = 100;

    let mut group = criterion.benchmark_group("Fsync Overhead");
    group.throughput(Throughput::Elements(batch_size as u64));

    // Build insert statement
    let mut values = String::from("INSERT INTO test VALUES ");
    for i in 0..batch_size {
        if i > 0 {
            values.push(',');
        }
        values.push_str(&format!("({i}, 'data_{i}')"));
    }

    // Limbo with sync=FULL
    let temp_dir = tempfile::tempdir().unwrap();
    let db = setup_limbo_with_sync(
        &temp_dir,
        "CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)",
        true,
    );
    let conn = db.connect().unwrap();

    group.bench_function(BenchmarkId::new("limbo", "sync_FULL"), |b| {
        let mut insert_stmt = conn.prepare(&values).unwrap();
        let mut delete_stmt = conn.query("DELETE FROM test").unwrap().unwrap();
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let start = std::time::Instant::now();
                run_to_completion(&mut insert_stmt, &db).unwrap();
                total += start.elapsed();
                insert_stmt.reset();
                run_to_completion(&mut delete_stmt, &db).unwrap();
                delete_stmt.reset();
            }
            total
        });
    });

    // Limbo with sync=OFF
    let temp_dir = tempfile::tempdir().unwrap();
    let db = setup_limbo_with_sync(
        &temp_dir,
        "CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)",
        false,
    );
    let conn = db.connect().unwrap();

    group.bench_function(BenchmarkId::new("limbo", "sync_OFF"), |b| {
        let mut insert_stmt = conn.prepare(&values).unwrap();
        let mut delete_stmt = conn.query("DELETE FROM test").unwrap().unwrap();
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let start = std::time::Instant::now();
                run_to_completion(&mut insert_stmt, &db).unwrap();
                total += start.elapsed();
                insert_stmt.reset();
                run_to_completion(&mut delete_stmt, &db).unwrap();
                delete_stmt.reset();
            }
            total
        });
    });

    if enable_rusqlite {
        // SQLite with sync=FULL
        let temp_dir = tempfile::tempdir().unwrap();
        let sqlite_conn = setup_rusqlite(
            &temp_dir,
            "CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)",
        );

        group.bench_function(BenchmarkId::new("sqlite", "sync_FULL"), |b| {
            let mut stmt = sqlite_conn.prepare(&values).unwrap();
            b.iter_custom(|iters| {
                let mut total = std::time::Duration::ZERO;
                for _ in 0..iters {
                    let start = std::time::Instant::now();
                    stmt.raw_execute().unwrap();
                    total += start.elapsed();
                    sqlite_conn.execute("DELETE FROM test", []).unwrap();
                }
                total
            });
        });

        // SQLite with sync=OFF
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("bench.db");
        let sqlite_conn = rusqlite::Connection::open(db_path).unwrap();
        sqlite_conn
            .pragma_update(None, "synchronous", "OFF")
            .unwrap();
        sqlite_conn
            .pragma_update(None, "journal_mode", "WAL")
            .unwrap();
        sqlite_conn
            .pragma_update(None, "locking_mode", "EXCLUSIVE")
            .unwrap();
        sqlite_conn
            .execute("CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)", [])
            .unwrap();

        group.bench_function(BenchmarkId::new("sqlite", "sync_OFF"), |b| {
            let mut stmt = sqlite_conn.prepare(&values).unwrap();
            b.iter_custom(|iters| {
                let mut total = std::time::Duration::ZERO;
                for _ in 0..iters {
                    let start = std::time::Instant::now();
                    stmt.raw_execute().unwrap();
                    total += start.elapsed();
                    sqlite_conn.execute("DELETE FROM test", []).unwrap();
                }
                total
            });
        });
    }

    group.finish();
}

#[cfg(not(feature = "codspeed"))]
criterion_group! {
    name = write_perf_benches;
    config = Criterion::default()
        .with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)))
        .sample_size(50);
    targets = bench_index_impact, bench_transaction_size, bench_key_pattern, bench_update_performance, bench_delete_performance, bench_large_transaction_commit, bench_fsync_overhead
}

#[cfg(feature = "codspeed")]
criterion_group! {
    name = write_perf_benches;
    config = Criterion::default().sample_size(50);
    targets = bench_index_impact, bench_transaction_size, bench_key_pattern, bench_update_performance, bench_delete_performance, bench_large_transaction_commit, bench_fsync_overhead
}

criterion_main!(write_perf_benches);
