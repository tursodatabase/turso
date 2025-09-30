use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use pprof::criterion::{Output, PProfProfiler};
use regex::Regex;
use std::{sync::Arc, time::Instant};
use turso_core::{Database, LimboError, PlatformIO, StepResult};

#[cfg(not(target_family = "wasm"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn rusqlite_open() -> rusqlite::Connection {
    let sqlite_conn = rusqlite::Connection::open("../testing/testing.db").unwrap();
    sqlite_conn
        .pragma_update(None, "locking_mode", "EXCLUSIVE")
        .unwrap();
    sqlite_conn
}

fn bench_open(criterion: &mut Criterion) {
    // https://github.com/tursodatabase/turso/issues/174
    // The rusqlite benchmark crashes on Mac M1 when using the flamegraph features
    let enable_rusqlite = std::env::var("DISABLE_RUSQLITE_BENCHMARK").is_err();

    if !std::fs::exists("../testing/schema_5k.db").unwrap() {
        #[allow(clippy::arc_with_non_send_sync)]
        let io = Arc::new(PlatformIO::new().unwrap());
        let db = Database::open_file(io.clone(), "../testing/schema_5k.db", false, false).unwrap();
        let conn = db.connect().unwrap();

        for i in 0..5000 {
            conn.execute(
                format!("CREATE TABLE table_{i} ( id INTEGER PRIMARY KEY, name TEXT, value INTEGER, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP )")
            ).unwrap();
        }
    }

    let mut group = criterion.benchmark_group("Open/Connect");

    group.bench_function(BenchmarkId::new("limbo_schema", ""), |b| {
        b.iter(|| {
            #[allow(clippy::arc_with_non_send_sync)]
            let io = Arc::new(PlatformIO::new().unwrap());
            let db =
                Database::open_file(io.clone(), "../testing/schema_5k.db", false, false).unwrap();
            let conn = db.connect().unwrap();
            conn.execute("SELECT * FROM table_0").unwrap();
        });
    });

    if enable_rusqlite {
        group.bench_function(BenchmarkId::new("sqlite_schema", ""), |b| {
            b.iter(|| {
                let conn = rusqlite::Connection::open("../testing/schema_5k.db").unwrap();
                conn.execute("SELECT * FROM table_0", ()).unwrap();
            });
        });
    }

    group.finish();
}

fn bench_alter(criterion: &mut Criterion) {
    // https://github.com/tursodatabase/turso/issues/174
    // The rusqlite benchmark crashes on Mac M1 when using the flamegraph features
    let enable_rusqlite = std::env::var("DISABLE_RUSQLITE_BENCHMARK").is_err();

    if !std::fs::exists("../testing/schema_5k.db").unwrap() {
        #[allow(clippy::arc_with_non_send_sync)]
        let io = Arc::new(PlatformIO::new().unwrap());
        let db = Database::open_file(io.clone(), "../testing/schema_5k.db", false, false).unwrap();
        let conn = db.connect().unwrap();

        for i in 0..5000 {
            conn.execute(
                format!("CREATE TABLE table_{i} ( id INTEGER PRIMARY KEY, name TEXT, value INTEGER, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP )")
            ).unwrap();
        }
    }

    let mut group = criterion.benchmark_group("`ALTER TABLE _ RENAME TO _`");

    group.bench_function(BenchmarkId::new("limbo_rename_table", ""), |b| {
        #[allow(clippy::arc_with_non_send_sync)]
        let io = Arc::new(PlatformIO::new().unwrap());
        let db = Database::open_file(io.clone(), "../testing/schema_5k.db", false, false).unwrap();
        let conn = db.connect().unwrap();
        b.iter_custom(|iters| {
            (0..iters)
                .map(|_| {
                    conn.execute("CREATE TABLE x(a)").unwrap();
                    let elapsed = {
                        let start = Instant::now();
                        conn.execute("ALTER TABLE x RENAME TO y").unwrap();
                        start.elapsed()
                    };
                    conn.execute("DROP TABLE y").unwrap();
                    elapsed
                })
                .sum()
        });
    });

    if enable_rusqlite {
        group.bench_function(BenchmarkId::new("sqlite_rename_table", ""), |b| {
            let conn = rusqlite::Connection::open("../testing/schema_5k.db").unwrap();
            b.iter_custom(|iters| {
                (0..iters)
                    .map(|_| {
                        conn.execute("CREATE TABLE x(a)", ()).unwrap();
                        let elapsed = {
                            let start = Instant::now();
                            conn.execute("ALTER TABLE x RENAME TO y", ()).unwrap();
                            start.elapsed()
                        };
                        conn.execute("DROP TABLE y", ()).unwrap();
                        elapsed
                    })
                    .sum()
            });
        });
    }

    group.finish();

    let mut group = criterion.benchmark_group("`ALTER TABLE _ RENAME COLUMN _ TO _`");

    group.bench_function(BenchmarkId::new("limbo_rename_column", ""), |b| {
        #[allow(clippy::arc_with_non_send_sync)]
        let io = Arc::new(PlatformIO::new().unwrap());
        let db = Database::open_file(io.clone(), "../testing/schema_5k.db", false, false).unwrap();
        let conn = db.connect().unwrap();
        b.iter_custom(|iters| {
            (0..iters)
                .map(|_| {
                    conn.execute("CREATE TABLE x(a)").unwrap();
                    let elapsed = {
                        let start = Instant::now();
                        conn.execute("ALTER TABLE x RENAME COLUMN a TO b").unwrap();
                        start.elapsed()
                    };
                    conn.execute("DROP TABLE x").unwrap();
                    elapsed
                })
                .sum()
        });
    });

    if enable_rusqlite {
        group.bench_function(BenchmarkId::new("sqlite_rename_column", ""), |b| {
            let conn = rusqlite::Connection::open("../testing/schema_5k.db").unwrap();
            b.iter_custom(|iters| {
                (0..iters)
                    .map(|_| {
                        conn.execute("CREATE TABLE x(a)", ()).unwrap();
                        let elapsed = {
                            let start = Instant::now();
                            conn.execute("ALTER TABLE x RENAME COLUMN a TO b", ())
                                .unwrap();
                            start.elapsed()
                        };
                        conn.execute("DROP TABLE x", ()).unwrap();
                        elapsed
                    })
                    .sum()
            });
        });
    }

    group.finish();

    let mut group = criterion.benchmark_group("`ALTER TABLE _ ADD COLUMN _`");

    group.bench_function(BenchmarkId::new("limbo_add_column", ""), |b| {
        #[allow(clippy::arc_with_non_send_sync)]
        let io = Arc::new(PlatformIO::new().unwrap());
        let db = Database::open_file(io.clone(), "../testing/schema_5k.db", false, false).unwrap();
        let conn = db.connect().unwrap();
        b.iter_custom(|iters| {
            (0..iters)
                .map(|_| {
                    conn.execute("CREATE TABLE x(a)").unwrap();
                    let elapsed = {
                        let start = Instant::now();
                        conn.execute("ALTER TABLE x ADD COLUMN b").unwrap();
                        start.elapsed()
                    };
                    conn.execute("DROP TABLE x").unwrap();
                    elapsed
                })
                .sum()
        });
    });

    if enable_rusqlite {
        group.bench_function(BenchmarkId::new("sqlite_add_column", ""), |b| {
            let conn = rusqlite::Connection::open("../testing/schema_5k.db").unwrap();
            b.iter_custom(|iters| {
                (0..iters)
                    .map(|_| {
                        conn.execute("CREATE TABLE x(a)", ()).unwrap();
                        let elapsed = {
                            let start = Instant::now();
                            conn.execute("ALTER TABLE x ADD COLUMN b", ()).unwrap();
                            start.elapsed()
                        };
                        conn.execute("DROP TABLE x", ()).unwrap();
                        elapsed
                    })
                    .sum()
            });
        });
    }

    group.finish();

    let mut group = criterion.benchmark_group("`ALTER TABLE _ DROP COLUMN _`");

    group.bench_function(BenchmarkId::new("limbo_drop_column", ""), |b| {
        #[allow(clippy::arc_with_non_send_sync)]
        let io = Arc::new(PlatformIO::new().unwrap());
        let db = Database::open_file(io.clone(), "../testing/schema_5k.db", false, false).unwrap();
        let conn = db.connect().unwrap();
        b.iter_custom(|iters| {
            (0..iters)
                .map(|_| {
                    conn.execute("CREATE TABLE x(a, b)").unwrap();
                    let elapsed = {
                        let start = Instant::now();
                        conn.execute("ALTER TABLE x DROP COLUMN b").unwrap();
                        start.elapsed()
                    };
                    conn.execute("DROP TABLE x").unwrap();
                    elapsed
                })
                .sum()
        });
    });

    if enable_rusqlite {
        group.bench_function(BenchmarkId::new("sqlite_drop_column", ""), |b| {
            let conn = rusqlite::Connection::open("../testing/schema_5k.db").unwrap();
            b.iter_custom(|iters| {
                (0..iters)
                    .map(|_| {
                        conn.execute("CREATE TABLE x(a, b)", ()).unwrap();
                        let elapsed = {
                            let start = Instant::now();
                            conn.execute("ALTER TABLE x DROP COLUMN b", ()).unwrap();
                            start.elapsed()
                        };
                        conn.execute("DROP TABLE x", ()).unwrap();
                        elapsed
                    })
                    .sum()
            });
        });
    }

    group.finish();
}

fn bench_prepare_query(criterion: &mut Criterion) {
    // https://github.com/tursodatabase/turso/issues/174
    // The rusqlite benchmark crashes on Mac M1 when using the flamegraph features
    let enable_rusqlite = std::env::var("DISABLE_RUSQLITE_BENCHMARK").is_err();

    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io.clone(), "../testing/testing.db", false, false).unwrap();
    let limbo_conn = db.connect().unwrap();

    let queries = [
        "SELECT 1",
        "SELECT * FROM users LIMIT 1",
        "SELECT first_name, count(1) FROM users GROUP BY first_name HAVING count(1) > 1 ORDER BY count(1)  LIMIT 1",
        "SELECT
            first_name,
            last_name,
            state,
            city,
            age + 10,
            LENGTH(email),
            UPPER(first_name),
            LOWER(last_name),
            SUBSTR(phone_number, 1, 3),
            zipcode || '-' || state,
            AVG(age) + 5,
            MAX(age) - MIN(age),
            ROUND(AVG(age), 1),
            SUM(age) / COUNT(*),
            COUNT(*),
            COUNT(email),
            SUM(age),
            AVG(age),
            MIN(age),
            MAX(age),
            SUM(CASE WHEN age >= 18 THEN 1 ELSE 0 END),
            SUM(CASE WHEN age < 18 THEN 1 ELSE 0 END),
            AVG(CASE WHEN age >= 18 THEN age ELSE NULL END),
            MAX(CASE WHEN age >= 18 THEN age ELSE NULL END)
        FROM users
        GROUP BY state, city",
    ];

    let whitespace_re = Regex::new(r"\s+").unwrap();
    for query in queries.iter() {
        // Normalize whitespace in the query string by replacing all sequences of whitespace with a single space.
        let query = whitespace_re.replace_all(query, " ").to_string();
        let query = query.as_str();

        let mut group = criterion.benchmark_group(format!("Prepare `{query}`"));

        group.bench_with_input(
            BenchmarkId::new("limbo_parse_query", query),
            query,
            |b, query| {
                b.iter(|| {
                    limbo_conn.prepare(query).unwrap();
                });
            },
        );

        if enable_rusqlite {
            let sqlite_conn = rusqlite_open();

            group.bench_with_input(
                BenchmarkId::new("sqlite_parse_query", query),
                query,
                |b, query| {
                    b.iter(|| {
                        sqlite_conn.prepare(query).unwrap();
                    });
                },
            );
        }

        group.finish();
    }
}

fn bench_execute_select_rows(criterion: &mut Criterion) {
    // https://github.com/tursodatabase/turso/issues/174
    // The rusqlite benchmark crashes on Mac M1 when using the flamegraph features
    let enable_rusqlite = std::env::var("DISABLE_RUSQLITE_BENCHMARK").is_err();

    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io.clone(), "../testing/testing.db", false, false).unwrap();
    let limbo_conn = db.connect().unwrap();

    let mut group = criterion.benchmark_group("Execute `SELECT * FROM users LIMIT ?`");

    for i in [1, 10, 50, 100] {
        group.bench_with_input(
            BenchmarkId::new("limbo_execute_select_rows", i),
            &i,
            |b, i| {
                // TODO: LIMIT doesn't support query parameters.
                let mut stmt = limbo_conn
                    .prepare(format!("SELECT * FROM users LIMIT {}", *i))
                    .unwrap();
                b.iter(|| {
                    loop {
                        match stmt.step().unwrap() {
                            turso_core::StepResult::Row => {
                                black_box(stmt.row());
                            }
                            turso_core::StepResult::IO => {
                                stmt.run_once().unwrap();
                            }
                            turso_core::StepResult::Done => {
                                break;
                            }
                            turso_core::StepResult::Interrupt | turso_core::StepResult::Busy => {
                                unreachable!();
                            }
                        }
                    }
                    stmt.reset();
                });
            },
        );

        if enable_rusqlite {
            let sqlite_conn = rusqlite_open();

            group.bench_with_input(
                BenchmarkId::new("sqlite_execute_select_rows", i),
                &i,
                |b, i| {
                    // TODO: Use parameters once we fix the above.
                    let mut stmt = sqlite_conn
                        .prepare(&format!("SELECT * FROM users LIMIT {}", *i))
                        .unwrap();
                    b.iter(|| {
                        let mut rows = stmt.raw_query();
                        while let Some(row) = rows.next().unwrap() {
                            black_box(row);
                        }
                    });
                },
            );
        }
    }

    group.finish();
}

fn bench_execute_select_1(criterion: &mut Criterion) {
    // https://github.com/tursodatabase/turso/issues/174
    // The rusqlite benchmark crashes on Mac M1 when using the flamegraph features
    let enable_rusqlite = std::env::var("DISABLE_RUSQLITE_BENCHMARK").is_err();

    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io.clone(), "../testing/testing.db", false, false).unwrap();
    let limbo_conn = db.connect().unwrap();

    let mut group = criterion.benchmark_group("Execute `SELECT 1`");

    group.bench_function("limbo_execute_select_1", |b| {
        let mut stmt = limbo_conn.prepare("SELECT 1").unwrap();
        b.iter(|| {
            loop {
                match stmt.step().unwrap() {
                    turso_core::StepResult::Row => {
                        black_box(stmt.row());
                    }
                    turso_core::StepResult::IO => {
                        stmt.run_once().unwrap();
                    }
                    turso_core::StepResult::Done => {
                        break;
                    }
                    turso_core::StepResult::Interrupt | turso_core::StepResult::Busy => {
                        unreachable!();
                    }
                }
            }
            stmt.reset();
        });
    });

    if enable_rusqlite {
        let sqlite_conn = rusqlite_open();

        group.bench_function("sqlite_execute_select_1", |b| {
            let mut stmt = sqlite_conn.prepare("SELECT 1").unwrap();
            b.iter(|| {
                let mut rows = stmt.raw_query();
                while let Some(row) = rows.next().unwrap() {
                    black_box(row);
                }
            });
        });
    }

    group.finish();
}

fn bench_execute_select_count(criterion: &mut Criterion) {
    // https://github.com/tursodatabase/turso/issues/174
    // The rusqlite benchmark crashes on Mac M1 when using the flamegraph features
    let enable_rusqlite = std::env::var("DISABLE_RUSQLITE_BENCHMARK").is_err();

    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io.clone(), "../testing/testing.db", false, false).unwrap();
    let limbo_conn = db.connect().unwrap();

    let mut group = criterion.benchmark_group("Execute `SELECT count() FROM users`");

    group.bench_function("limbo_execute_select_count", |b| {
        let mut stmt = limbo_conn.prepare("SELECT count() FROM users").unwrap();
        b.iter(|| {
            loop {
                match stmt.step().unwrap() {
                    turso_core::StepResult::Row => {
                        black_box(stmt.row());
                    }
                    turso_core::StepResult::IO => {
                        stmt.run_once().unwrap();
                    }
                    turso_core::StepResult::Done => {
                        break;
                    }
                    turso_core::StepResult::Interrupt | turso_core::StepResult::Busy => {
                        unreachable!();
                    }
                }
            }
            stmt.reset();
        });
    });

    if enable_rusqlite {
        let sqlite_conn = rusqlite_open();

        group.bench_function("sqlite_execute_select_count", |b| {
            let mut stmt = sqlite_conn.prepare("SELECT count() FROM users").unwrap();
            b.iter(|| {
                let mut rows = stmt.raw_query();
                while let Some(row) = rows.next().unwrap() {
                    black_box(row);
                }
            });
        });
    }

    group.finish();
}

fn bench_insert_rows(criterion: &mut Criterion) {
    // The rusqlite benchmark crashes on Mac M1 when using the flamegraph features
    let enable_rusqlite = std::env::var("DISABLE_RUSQLITE_BENCHMARK").is_err();

    let mut group = criterion.benchmark_group("Insert rows in batches");

    // Test different batch sizes
    for batch_size in [1, 10, 100] {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("bench.db");

        #[allow(clippy::arc_with_non_send_sync)]
        let io = Arc::new(PlatformIO::new().unwrap());
        let db = Database::open_file(io.clone(), db_path.to_str().unwrap(), false, false).unwrap();
        let limbo_conn = db.connect().unwrap();

        let mut stmt = limbo_conn
            .query("CREATE TABLE test (id INTEGER, value TEXT)")
            .unwrap()
            .unwrap();

        loop {
            match stmt.step().unwrap() {
                turso_core::StepResult::IO => {
                    stmt.run_once().unwrap();
                }
                turso_core::StepResult::Done => {
                    break;
                }
                turso_core::StepResult::Row => {
                    unreachable!();
                }
                turso_core::StepResult::Interrupt | turso_core::StepResult::Busy => {
                    unreachable!();
                }
            }
        }

        group.bench_function(format!("limbo_insert_{batch_size}_rows"), |b| {
            let mut values = String::from("INSERT INTO test VALUES ");
            for i in 0..batch_size {
                if i > 0 {
                    values.push(',');
                }
                values.push_str(&format!("({}, '{}')", i, format_args!("value_{i}")));
            }
            let mut stmt = limbo_conn.prepare(&values).unwrap();
            b.iter(|| {
                loop {
                    match stmt.step().unwrap() {
                        turso_core::StepResult::IO => {
                            stmt.run_once().unwrap();
                        }
                        turso_core::StepResult::Done => {
                            break;
                        }
                        turso_core::StepResult::Row => {
                            unreachable!();
                        }
                        turso_core::StepResult::Interrupt | turso_core::StepResult::Busy => {
                            unreachable!();
                        }
                    }
                }
                stmt.reset();
            });
        });

        if enable_rusqlite {
            let temp_dir = tempfile::tempdir().unwrap();
            let db_path = temp_dir.path().join("bench.db");
            let sqlite_conn = rusqlite::Connection::open(db_path).unwrap();
            sqlite_conn
                .pragma_update(None, "synchronous", "FULL")
                .unwrap();
            sqlite_conn
                .pragma_update(None, "journal_mode", "WAL")
                .unwrap();
            sqlite_conn
                .pragma_update(None, "locking_mode", "EXCLUSIVE")
                .unwrap();
            let journal_mode = sqlite_conn
                .pragma_query_value(None, "journal_mode", |row| row.get::<_, String>(0))
                .unwrap();
            assert_eq!(journal_mode.to_lowercase(), "wal");
            let synchronous = sqlite_conn
                .pragma_query_value(None, "synchronous", |row| row.get::<_, usize>(0))
                .unwrap();
            const FULL: usize = 2;
            assert_eq!(synchronous, FULL);

            // Create test table
            sqlite_conn
                .execute("CREATE TABLE test (id INTEGER, value TEXT)", [])
                .unwrap();
            sqlite_conn
                .pragma_update(None, "locking_mode", "EXCLUSIVE")
                .unwrap();

            group.bench_function(format!("sqlite_insert_{batch_size}_rows"), |b| {
                let mut values = String::from("INSERT INTO test VALUES ");
                for i in 0..batch_size {
                    if i > 0 {
                        values.push(',');
                    }
                    values.push_str(&format!("({}, '{}')", i, format_args!("value_{i}")));
                }
                let mut stmt = sqlite_conn.prepare(&values).unwrap();
                b.iter(|| {
                    let mut rows = stmt.raw_query();
                    while let Some(row) = rows.next().unwrap() {
                        black_box(row);
                    }
                });
            });
        }
    }

    group.finish();
}

#[inline(never)]
fn bench_limbo(
    mvcc: bool,
    num_connections: i64,
    num_batch_inserts: i64,
    num_inserts_per_batch: usize,
) {
    struct ConnectionState {
        conn: Arc<turso_core::Connection>,
        inserts: Vec<String>,
        current_statement: Option<turso_core::Statement>,
    }
    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(PlatformIO::new().unwrap());
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path().join("bench.db");
    let db = Database::open_file(io.clone(), path.to_str().unwrap(), mvcc, false).unwrap();
    let mut connecitons = Vec::new();
    {
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE test (x)").unwrap();
        conn.close().unwrap();
    }
    let inserts =
        generate_inserts_per_connection(num_connections, num_batch_inserts, num_inserts_per_batch);
    for i in 0..num_connections {
        let conn = db.connect().unwrap();
        let inserts = inserts[i as usize].clone();
        connecitons.push(ConnectionState {
            conn,
            inserts,
            current_statement: None,
        });
    }
    loop {
        let mut all_finished = true;
        for conn in &mut connecitons {
            if !conn.inserts.is_empty() || conn.current_statement.is_some() {
                all_finished = false;
                break;
            }
        }
        for conn in connecitons.iter_mut() {
            if conn.current_statement.is_none() && !conn.inserts.is_empty() {
                let write = conn.inserts.pop().unwrap();
                conn.current_statement = Some(conn.conn.prepare(&write).unwrap());
            }
            if conn.current_statement.is_none() {
                continue;
            }
            let stmt = conn.current_statement.as_mut().unwrap();
            match stmt.step().unwrap() {
                // These you be only possible cases in write concurrency.
                // No rows because insert doesn't return
                // No interrupt because insert doesn't interrupt
                // No busy because insert in mvcc should be multi concurrent write
                StepResult::Done => {
                    conn.current_statement = None;
                }
                StepResult::IO => {
                    // let's skip doing I/O here, we want to perform io only after all the statements are stepped
                }
                StepResult::Busy => {
                    // We need to restart statement
                    if mvcc {
                        unreachable!();
                    }
                    stmt.reset();
                }
                _ => {
                    unreachable!()
                }
            }
        }
        db.io.step().unwrap();

        if all_finished {
            break;
        }
    }
}

#[inline(never)]
fn bench_limbo_mvcc(
    mvcc: bool,
    num_connections: i64,
    num_batch_inserts: i64,
    num_inserts_per_batch: usize,
) {
    struct ConnectionState {
        conn: Arc<turso_core::Connection>,
        inserts: Vec<String>,
        current_statement: Option<turso_core::Statement>,
        current_insert: Option<String>,
    }
    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(PlatformIO::new().unwrap());
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path().join("bench.db");
    let db = Database::open_file(io.clone(), path.to_str().unwrap(), mvcc, false).unwrap();
    let mut connecitons = Vec::new();
    let conn0 = db.connect().unwrap();
    conn0.execute("CREATE TABLE test (x)").unwrap();

    let inserts =
        generate_inserts_per_connection(num_connections, num_batch_inserts, num_inserts_per_batch);
    for i in 0..num_connections {
        let conn = db.connect().unwrap();
        let inserts = inserts[i as usize].clone();
        connecitons.push(ConnectionState {
            conn,
            inserts,
            current_statement: None,
            current_insert: None,
        });
    }
    loop {
        let mut all_finished = true;
        for conn in &mut connecitons {
            if !conn.inserts.is_empty() || conn.current_statement.is_some() {
                all_finished = false;
                break;
            }
        }
        for conn in connecitons.iter_mut() {
            if conn.current_statement.is_none() && !conn.inserts.is_empty() {
                let write = conn.inserts.pop().unwrap();
                conn.conn.execute("BEGIN CONCURRENT").unwrap();
                conn.current_statement = Some(conn.conn.prepare(&write).unwrap());
                conn.current_insert = Some(write);
            }
            if conn.current_statement.is_none() {
                continue;
            }
            let stmt = conn.current_statement.as_mut().unwrap();
            match stmt.step() {
                // These you be only possible cases in write concurrency.
                // No rows because insert doesn't return
                // No interrupt because insert doesn't interrupt
                // No busy because insert in mvcc should be multi concurrent write
                Ok(StepResult::Done) => {
                    // Now do commit
                    conn.current_statement = Some(conn.conn.prepare("COMMIT").unwrap());
                }
                Ok(StepResult::IO) => {
                    stmt.step().unwrap();
                    // let's skip doing I/O here, we want to perform io only after all the statements are stepped
                }
                Ok(StepResult::Busy) => {
                    // We need to restart statement
                    if mvcc {
                        unreachable!();
                    }
                    println!("resetting statement");
                    stmt.reset();
                }
                Err(err) => {
                    if let LimboError::SchemaUpdated = err {
                        conn.current_statement = Some(
                            conn.conn
                                .prepare(conn.current_insert.clone().as_ref().unwrap())
                                .unwrap(),
                        );
                        continue;
                    }
                    panic!("unexpected error: {err:?}");
                }
                _ => {
                    unreachable!()
                }
            }
        }
        db.io.step().unwrap();

        if all_finished {
            break;
        }
    }
}

fn generate_inserts_per_connection(
    num_connections: i64,
    num_batch_inserts: i64,
    num_inserts_per_batch: usize,
) -> Vec<Vec<String>> {
    let mut inserts = vec![];
    for i in 0..num_connections {
        let mut inserts_per_connection = vec![];
        for j in 0..num_batch_inserts {
            inserts_per_connection.push(generate_batch_insert(
                num_batch_inserts * (i + j),
                num_inserts_per_batch,
            ));
        }
        inserts.push(inserts_per_connection);
    }
    inserts
}

fn generate_batch_insert(start: i64, num: usize) -> String {
    let mut inserts = String::from("INSERT INTO test (x) VALUES ");
    for i in 0..num {
        inserts.push_str(&format!("({})", start + i as i64));
        if i < num - 1 {
            inserts.push(',');
        }
    }
    inserts
}

fn bench_concurrent_writes(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("Concurrent writes");

    let num_connections = 10;
    let num_batch_inserts = 100;
    let num_inserts_per_batch = 200_usize;

    group.bench_function("limbo_wal_concurrent_writes", |b| {
        b.iter(|| {
            bench_limbo(
                false,
                num_connections,
                num_batch_inserts,
                num_inserts_per_batch,
            );
        });
    });
    group.bench_function("limbo_mvcc_concurrent_writes", |b| {
        b.iter(|| {
            bench_limbo_mvcc(
                true,
                num_connections,
                num_batch_inserts,
                num_inserts_per_batch,
            );
        });
    });
    group.bench_function("sqlite_concurrent_writes", |b| {
        let inserts = generate_inserts_per_connection(
            num_connections,
            num_batch_inserts,
            num_inserts_per_batch,
        );
        b.iter(|| {
            let temp_dir = tempfile::tempdir().unwrap();
            let path = temp_dir.path().join("bench.db");
            {
                let conn = rusqlite::Connection::open(path.to_str().unwrap()).unwrap();
                conn.pragma_update(None, "synchronous", "FULL").unwrap();
                conn.pragma_update(None, "journal_mode", "WAL").unwrap();
                conn.pragma_update(None, "locking_mode", "EXCLUSIVE")
                    .unwrap();
                conn.execute("CREATE TABLE test (x INTEGER)", []).unwrap();
            }

            for i in 0..num_connections {
                let conn = rusqlite::Connection::open(path.to_str().unwrap()).unwrap();
                for j in 0..num_batch_inserts {
                    conn.execute(&inserts[i as usize][j as usize], []).unwrap();
                }
            }
        });
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_open, bench_alter, bench_prepare_query, bench_execute_select_1, bench_execute_select_rows, bench_execute_select_count, bench_insert_rows, bench_concurrent_writes
}
criterion_main!(benches);
