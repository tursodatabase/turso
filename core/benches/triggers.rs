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

fn setup_limbo(temp_dir: &TempDir, stmts: &[&str]) -> Arc<Database> {
    let db_path = temp_dir.path().join("bench.db");
    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io, db_path.to_str().unwrap()).unwrap();
    let conn = db.connect().unwrap();

    let mut stmt = conn.query("PRAGMA synchronous = OFF").unwrap().unwrap();
    run_to_completion(&mut stmt, &db).unwrap();

    for ddl in stmts {
        let mut stmt = conn.query(ddl.trim()).unwrap().unwrap();
        run_to_completion(&mut stmt, &db).unwrap();
    }

    db
}

fn setup_rusqlite(temp_dir: &TempDir, schema: &str) -> rusqlite::Connection {
    let db_path = temp_dir.path().join("bench.db");
    let conn = rusqlite::Connection::open(db_path).unwrap();
    conn.pragma_update(None, "synchronous", "OFF").unwrap();
    conn.pragma_update(None, "journal_mode", "WAL").unwrap();
    conn.pragma_update(None, "locking_mode", "EXCLUSIVE")
        .unwrap();
    conn.execute_batch(schema).unwrap();
    conn
}

/// Multi-row INSERT with AFTER INSERT trigger (tests statement caching + step_subprogram)
fn bench_multirow_insert_with_trigger(criterion: &mut Criterion) {
    let enable_rusqlite =
        std::env::var("DISABLE_RUSQLITE_BENCHMARK").is_err() && !cfg!(feature = "codspeed");

    let mut group = criterion.benchmark_group("Multi-Row INSERT with Trigger");

    let row_counts = [10, 100, 500, 1000];

    for row_count in row_counts {
        group.throughput(Throughput::Elements(row_count as u64));

        let stmts: &[&str] = &[
            "CREATE TABLE src (id INTEGER PRIMARY KEY, val TEXT, num INTEGER)",
            "CREATE TABLE audit (src_id INTEGER, val TEXT, inserted_at TEXT)",
            "CREATE TRIGGER trg_after_insert AFTER INSERT ON src BEGIN INSERT INTO audit VALUES (NEW.id, NEW.val, 'now'); END",
        ];
        let schema_batch = "CREATE TABLE src (id INTEGER PRIMARY KEY, val TEXT, num INTEGER); \
            CREATE TABLE audit (src_id INTEGER, val TEXT, inserted_at TEXT); \
            CREATE TRIGGER trg_after_insert AFTER INSERT ON src BEGIN INSERT INTO audit VALUES (NEW.id, NEW.val, 'now'); END";

        let mut values = String::from("INSERT INTO src VALUES ");
        for i in 0..row_count {
            if i > 0 {
                values.push(',');
            }
            values.push_str(&format!("({i}, 'val_{i}', {i})"));
        }

        // Limbo
        let temp_dir = tempfile::tempdir().unwrap();
        let db = setup_limbo(&temp_dir, stmts);
        let conn = db.connect().unwrap();

        group.bench_function(
            BenchmarkId::new("limbo", format!("{row_count}_rows")),
            |b| {
                let mut insert_stmt = conn.prepare(&values).unwrap();
                let mut del_src = conn.query("DELETE FROM src").unwrap().unwrap();
                let mut del_audit = conn.query("DELETE FROM audit").unwrap().unwrap();
                b.iter_custom(|iters| {
                    let mut total = std::time::Duration::ZERO;
                    for _ in 0..iters {
                        let start = std::time::Instant::now();
                        run_to_completion(&mut insert_stmt, &db).unwrap();
                        total += start.elapsed();
                        insert_stmt.reset().unwrap();
                        run_to_completion(&mut del_src, &db).unwrap();
                        del_src.reset().unwrap();
                        run_to_completion(&mut del_audit, &db).unwrap();
                        del_audit.reset().unwrap();
                    }
                    total
                });
            },
        );

        // SQLite
        if enable_rusqlite {
            let temp_dir = tempfile::tempdir().unwrap();
            let sqlite_conn = setup_rusqlite(&temp_dir, schema_batch);

            group.bench_function(
                BenchmarkId::new("sqlite", format!("{row_count}_rows")),
                |b| {
                    let mut stmt = sqlite_conn.prepare(&values).unwrap();
                    b.iter_custom(|iters| {
                        let mut total = std::time::Duration::ZERO;
                        for _ in 0..iters {
                            let start = std::time::Instant::now();
                            stmt.raw_execute().unwrap();
                            total += start.elapsed();
                            sqlite_conn
                                .execute_batch("DELETE FROM src; DELETE FROM audit")
                                .unwrap();
                        }
                        total
                    });
                },
            );
        }
    }

    group.finish();
}

/// Wide table where trigger references only a few columns (tests sparse parameter allocation)
fn bench_wide_table_sparse_trigger(criterion: &mut Criterion) {
    let enable_rusqlite =
        std::env::var("DISABLE_RUSQLITE_BENCHMARK").is_err() && !cfg!(feature = "codspeed");

    let mut group = criterion.benchmark_group("Wide Table Sparse Trigger");

    // Test with different column counts to show scaling advantage
    let column_counts = [10, 20, 50];
    let row_count = 500;

    for col_count in column_counts {
        group.throughput(Throughput::Elements(row_count as u64));

        // Build wide table schema: id + N text columns
        let cols: Vec<String> = (0..col_count).map(|i| format!("c{i} TEXT")).collect();
        let create_table = format!(
            "CREATE TABLE wide (id INTEGER PRIMARY KEY, {})",
            cols.join(", ")
        );
        let create_audit = "CREATE TABLE audit_wide (src_id INTEGER, first_col TEXT)".to_string();
        let create_trigger = "CREATE TRIGGER trg_wide AFTER INSERT ON wide BEGIN INSERT INTO audit_wide VALUES (NEW.id, NEW.c0); END".to_string();

        let stmts_owned = [&create_table, &create_audit, &create_trigger];
        let stmts: Vec<&str> = stmts_owned.iter().map(|s| s.as_str()).collect();
        let schema_batch = format!("{create_table}; {create_audit}; {create_trigger}");

        // Build multi-row insert with all columns populated
        let col_names: Vec<String> = std::iter::once("id".to_string())
            .chain((0..col_count).map(|i| format!("c{i}")))
            .collect();
        let mut values = format!("INSERT INTO wide ({}) VALUES ", col_names.join(", "));
        for i in 0..row_count {
            if i > 0 {
                values.push(',');
            }
            let col_vals: Vec<String> = std::iter::once(format!("{i}"))
                .chain((0..col_count).map(|c| format!("'row{i}_col{c}'")))
                .collect();
            values.push_str(&format!("({})", col_vals.join(",")));
        }

        // Limbo
        let temp_dir = tempfile::tempdir().unwrap();
        let db = setup_limbo(&temp_dir, &stmts);
        let conn = db.connect().unwrap();

        group.bench_function(
            BenchmarkId::new("limbo", format!("{col_count}_cols")),
            |b| {
                let mut insert_stmt = conn.prepare(&values).unwrap();
                let mut del1 = conn.query("DELETE FROM wide").unwrap().unwrap();
                let mut del2 = conn.query("DELETE FROM audit_wide").unwrap().unwrap();
                b.iter_custom(|iters| {
                    let mut total = std::time::Duration::ZERO;
                    for _ in 0..iters {
                        let start = std::time::Instant::now();
                        run_to_completion(&mut insert_stmt, &db).unwrap();
                        total += start.elapsed();
                        insert_stmt.reset().unwrap();
                        run_to_completion(&mut del1, &db).unwrap();
                        del1.reset().unwrap();
                        run_to_completion(&mut del2, &db).unwrap();
                        del2.reset().unwrap();
                    }
                    total
                });
            },
        );

        // SQLite
        if enable_rusqlite {
            let temp_dir = tempfile::tempdir().unwrap();
            let sqlite_conn = setup_rusqlite(&temp_dir, &schema_batch);

            group.bench_function(
                BenchmarkId::new("sqlite", format!("{col_count}_cols")),
                |b| {
                    let mut stmt = sqlite_conn.prepare(&values).unwrap();
                    b.iter_custom(|iters| {
                        let mut total = std::time::Duration::ZERO;
                        for _ in 0..iters {
                            let start = std::time::Instant::now();
                            stmt.raw_execute().unwrap();
                            total += start.elapsed();
                            sqlite_conn
                                .execute_batch("DELETE FROM wide; DELETE FROM audit_wide")
                                .unwrap();
                        }
                        total
                    });
                },
            );
        }
    }

    group.finish();
}

/// Multiple triggers on the same table (tests pre-trigger affinity emission)
fn bench_multiple_triggers(criterion: &mut Criterion) {
    let enable_rusqlite =
        std::env::var("DISABLE_RUSQLITE_BENCHMARK").is_err() && !cfg!(feature = "codspeed");

    let mut group = criterion.benchmark_group("Multiple Triggers per Table");

    // Compare 1, 2, and 4 triggers on same table
    let trigger_counts = [1, 2, 4];
    let row_count = 500;

    for trigger_count in trigger_counts {
        group.throughput(Throughput::Elements(row_count as u64));

        let mut stmts: Vec<&str> = vec![
            "CREATE TABLE src (id INTEGER PRIMARY KEY, a TEXT, b INTEGER, c REAL, d TEXT)",
            "CREATE TABLE log1 (src_id INTEGER, val TEXT)",
            "CREATE TRIGGER trg1 AFTER INSERT ON src BEGIN INSERT INTO log1 VALUES (NEW.id, NEW.a); END",
        ];
        let mut schema_batch = String::from(
            "CREATE TABLE src (id INTEGER PRIMARY KEY, a TEXT, b INTEGER, c REAL, d TEXT); \
             CREATE TABLE log1 (src_id INTEGER, val TEXT); ",
        );

        if trigger_count >= 2 {
            stmts.insert(2, "CREATE TABLE log2 (src_id INTEGER, val INTEGER)");
            stmts.push("CREATE TRIGGER trg2 AFTER INSERT ON src BEGIN INSERT INTO log2 VALUES (NEW.id, NEW.b); END");
            schema_batch.push_str("CREATE TABLE log2 (src_id INTEGER, val INTEGER); ");
        }
        if trigger_count >= 4 {
            stmts.insert(3, "CREATE TABLE log3 (src_id INTEGER, val REAL)");
            stmts.insert(4, "CREATE TABLE log4 (src_id INTEGER, val TEXT)");
            stmts.push("CREATE TRIGGER trg3 AFTER INSERT ON src BEGIN INSERT INTO log3 VALUES (NEW.id, NEW.c); END");
            stmts.push("CREATE TRIGGER trg4 AFTER INSERT ON src BEGIN INSERT INTO log4 VALUES (NEW.id, NEW.d); END");
            schema_batch.push_str("CREATE TABLE log3 (src_id INTEGER, val REAL); ");
            schema_batch.push_str("CREATE TABLE log4 (src_id INTEGER, val TEXT); ");
        }

        // Append trigger DDL to schema_batch
        schema_batch.push_str("CREATE TRIGGER trg1 AFTER INSERT ON src BEGIN INSERT INTO log1 VALUES (NEW.id, NEW.a); END; ");
        if trigger_count >= 2 {
            schema_batch.push_str("CREATE TRIGGER trg2 AFTER INSERT ON src BEGIN INSERT INTO log2 VALUES (NEW.id, NEW.b); END; ");
        }
        if trigger_count >= 4 {
            schema_batch.push_str("CREATE TRIGGER trg3 AFTER INSERT ON src BEGIN INSERT INTO log3 VALUES (NEW.id, NEW.c); END; ");
            schema_batch.push_str("CREATE TRIGGER trg4 AFTER INSERT ON src BEGIN INSERT INTO log4 VALUES (NEW.id, NEW.d); END; ");
        }

        let mut values = String::from("INSERT INTO src VALUES ");
        for i in 0..row_count {
            if i > 0 {
                values.push(',');
            }
            values.push_str(&format!("({i}, 'text_{i}', {i}, {i}.5, 'extra_{i}')"));
        }

        let mut delete_tables = vec!["src", "log1"];
        if trigger_count >= 2 {
            delete_tables.push("log2");
        }
        if trigger_count >= 4 {
            delete_tables.push("log3");
            delete_tables.push("log4");
        }
        let delete_batch: String = delete_tables
            .iter()
            .map(|t| format!("DELETE FROM {t}"))
            .collect::<Vec<_>>()
            .join("; ");

        // Limbo
        let temp_dir = tempfile::tempdir().unwrap();
        let db = setup_limbo(&temp_dir, &stmts);
        let conn = db.connect().unwrap();

        group.bench_function(
            BenchmarkId::new("limbo", format!("{trigger_count}_triggers")),
            |b| {
                let mut insert_stmt = conn.prepare(&values).unwrap();
                let mut del_stmts: Vec<_> = delete_tables
                    .iter()
                    .map(|t| conn.query(format!("DELETE FROM {t}")).unwrap().unwrap())
                    .collect();
                b.iter_custom(|iters| {
                    let mut total = std::time::Duration::ZERO;
                    for _ in 0..iters {
                        let start = std::time::Instant::now();
                        run_to_completion(&mut insert_stmt, &db).unwrap();
                        total += start.elapsed();
                        insert_stmt.reset().unwrap();
                        for del in &mut del_stmts {
                            run_to_completion(del, &db).unwrap();
                            del.reset().unwrap();
                        }
                    }
                    total
                });
            },
        );

        // SQLite
        if enable_rusqlite {
            let temp_dir = tempfile::tempdir().unwrap();
            let sqlite_conn = setup_rusqlite(&temp_dir, &schema_batch);

            group.bench_function(
                BenchmarkId::new("sqlite", format!("{trigger_count}_triggers")),
                |b| {
                    let mut stmt = sqlite_conn.prepare(&values).unwrap();
                    b.iter_custom(|iters| {
                        let mut total = std::time::Duration::ZERO;
                        for _ in 0..iters {
                            let start = std::time::Instant::now();
                            stmt.raw_execute().unwrap();
                            total += start.elapsed();
                            sqlite_conn.execute_batch(&delete_batch).unwrap();
                        }
                        total
                    });
                },
            );
        }
    }

    group.finish();
}

/// Baseline: INSERT with vs without triggers to isolate trigger overhead
fn bench_trigger_overhead(criterion: &mut Criterion) {
    let enable_rusqlite =
        std::env::var("DISABLE_RUSQLITE_BENCHMARK").is_err() && !cfg!(feature = "codspeed");

    let mut group = criterion.benchmark_group("Trigger Overhead");

    let row_count = 500;
    group.throughput(Throughput::Elements(row_count as u64));

    let stmts_no_trigger: &[&str] =
        &["CREATE TABLE src (id INTEGER PRIMARY KEY, val TEXT, num INTEGER)"];
    let stmts_with_trigger: &[&str] = &[
        "CREATE TABLE src (id INTEGER PRIMARY KEY, val TEXT, num INTEGER)",
        "CREATE TABLE audit (src_id INTEGER, val TEXT)",
        "CREATE TRIGGER trg AFTER INSERT ON src BEGIN INSERT INTO audit VALUES (NEW.id, NEW.val); END",
    ];
    let batch_no_trigger = "CREATE TABLE src (id INTEGER PRIMARY KEY, val TEXT, num INTEGER)";
    let batch_with_trigger = "CREATE TABLE src (id INTEGER PRIMARY KEY, val TEXT, num INTEGER); \
        CREATE TABLE audit (src_id INTEGER, val TEXT); \
        CREATE TRIGGER trg AFTER INSERT ON src BEGIN INSERT INTO audit VALUES (NEW.id, NEW.val); END";

    let mut values = String::from("INSERT INTO src VALUES ");
    for i in 0..row_count {
        if i > 0 {
            values.push(',');
        }
        values.push_str(&format!("({i}, 'val_{i}', {i})"));
    }

    for (label, stmts, batch, has_audit) in [
        ("no_trigger", stmts_no_trigger, batch_no_trigger, false),
        ("with_trigger", stmts_with_trigger, batch_with_trigger, true),
    ] {
        // Limbo
        let temp_dir = tempfile::tempdir().unwrap();
        let db = setup_limbo(&temp_dir, stmts);
        let conn = db.connect().unwrap();

        group.bench_function(BenchmarkId::new("limbo", label), |b| {
            let mut insert_stmt = conn.prepare(&values).unwrap();
            let mut del_src = conn.query("DELETE FROM src").unwrap().unwrap();
            let mut del_audit = if has_audit {
                Some(conn.query("DELETE FROM audit").unwrap().unwrap())
            } else {
                None
            };
            b.iter_custom(|iters| {
                let mut total = std::time::Duration::ZERO;
                for _ in 0..iters {
                    let start = std::time::Instant::now();
                    run_to_completion(&mut insert_stmt, &db).unwrap();
                    total += start.elapsed();
                    insert_stmt.reset().unwrap();
                    run_to_completion(&mut del_src, &db).unwrap();
                    del_src.reset().unwrap();
                    if let Some(del) = &mut del_audit {
                        run_to_completion(del, &db).unwrap();
                        del.reset().unwrap();
                    }
                }
                total
            });
        });

        // SQLite
        if enable_rusqlite {
            let temp_dir = tempfile::tempdir().unwrap();
            let sqlite_conn = setup_rusqlite(&temp_dir, batch);

            group.bench_function(BenchmarkId::new("sqlite", label), |b| {
                let mut stmt = sqlite_conn.prepare(&values).unwrap();
                let delete_sql = if has_audit {
                    "DELETE FROM src; DELETE FROM audit"
                } else {
                    "DELETE FROM src"
                };
                b.iter_custom(|iters| {
                    let mut total = std::time::Duration::ZERO;
                    for _ in 0..iters {
                        let start = std::time::Instant::now();
                        stmt.raw_execute().unwrap();
                        total += start.elapsed();
                        sqlite_conn.execute_batch(delete_sql).unwrap();
                    }
                    total
                });
            });
        }
    }

    group.finish();
}

/// BEFORE INSERT trigger that modifies NEW values
fn bench_before_trigger(criterion: &mut Criterion) {
    let enable_rusqlite =
        std::env::var("DISABLE_RUSQLITE_BENCHMARK").is_err() && !cfg!(feature = "codspeed");

    let mut group = criterion.benchmark_group("BEFORE INSERT Trigger");

    let row_counts = [100, 500, 1000];

    for row_count in row_counts {
        group.throughput(Throughput::Elements(row_count as u64));

        let stmts: &[&str] = &[
            "CREATE TABLE src (id INTEGER PRIMARY KEY, val TEXT, status TEXT)",
            "CREATE TRIGGER trg_before BEFORE INSERT ON src BEGIN SELECT CASE WHEN NEW.val IS NULL THEN RAISE(ABORT, 'val required') END; END",
        ];
        let schema_batch = "CREATE TABLE src (id INTEGER PRIMARY KEY, val TEXT, status TEXT); \
            CREATE TRIGGER trg_before BEFORE INSERT ON src BEGIN SELECT CASE WHEN NEW.val IS NULL THEN RAISE(ABORT, 'val required') END; END";

        let mut values = String::from("INSERT INTO src VALUES ");
        for i in 0..row_count {
            if i > 0 {
                values.push(',');
            }
            values.push_str(&format!("({i}, 'val_{i}', 'active')"));
        }

        // Limbo
        let temp_dir = tempfile::tempdir().unwrap();
        let db = setup_limbo(&temp_dir, stmts);
        let conn = db.connect().unwrap();

        group.bench_function(
            BenchmarkId::new("limbo", format!("{row_count}_rows")),
            |b| {
                let mut insert_stmt = conn.prepare(&values).unwrap();
                let mut del = conn.query("DELETE FROM src").unwrap().unwrap();
                b.iter_custom(|iters| {
                    let mut total = std::time::Duration::ZERO;
                    for _ in 0..iters {
                        let start = std::time::Instant::now();
                        run_to_completion(&mut insert_stmt, &db).unwrap();
                        total += start.elapsed();
                        insert_stmt.reset().unwrap();
                        run_to_completion(&mut del, &db).unwrap();
                        del.reset().unwrap();
                    }
                    total
                });
            },
        );

        // SQLite
        if enable_rusqlite {
            let temp_dir = tempfile::tempdir().unwrap();
            let sqlite_conn = setup_rusqlite(&temp_dir, schema_batch);

            group.bench_function(
                BenchmarkId::new("sqlite", format!("{row_count}_rows")),
                |b| {
                    let mut stmt = sqlite_conn.prepare(&values).unwrap();
                    b.iter_custom(|iters| {
                        let mut total = std::time::Duration::ZERO;
                        for _ in 0..iters {
                            let start = std::time::Instant::now();
                            stmt.raw_execute().unwrap();
                            total += start.elapsed();
                            sqlite_conn.execute("DELETE FROM src", []).unwrap();
                        }
                        total
                    });
                },
            );
        }
    }

    group.finish();
}

#[cfg(not(feature = "codspeed"))]
criterion_group! {
    name = trigger_benches;
    config = Criterion::default()
        .with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)))
        .sample_size(30);
    targets =
        bench_multirow_insert_with_trigger,
        bench_wide_table_sparse_trigger,
        bench_multiple_triggers,
        bench_trigger_overhead,
        bench_before_trigger
}

#[cfg(feature = "codspeed")]
criterion_group! {
    name = trigger_benches;
    config = Criterion::default().sample_size(30);
    targets =
        bench_multirow_insert_with_trigger,
        bench_wide_table_sparse_trigger,
        bench_multiple_triggers,
        bench_trigger_overhead,
        bench_before_trigger
}

criterion_main!(trigger_benches);
