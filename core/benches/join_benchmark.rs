use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use limbo_core::{Database, PlatformIO, IO};


use pprof::criterion::{Output, PProfProfiler};
use tempfile::TempDir;
use std::{fs::File, path::PathBuf, rc::Rc, sync::Arc};

pub struct TempDatabase {
    _dir: TempDir,
    path: PathBuf,
}

impl TempDatabase {
    pub fn new_empty() -> Self {
        let dir = TempDir::new().expect("failed to create tempdir");
        let path = dir.path().join("database.db");
        File::create(&path).expect("failed to create temp db file");
        TempDatabase { _dir: dir, path }
    }

    pub fn connect_limbo(&self) -> Rc<limbo_core::Connection> {
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
        if let Some(db_path) = self.path.to_str() {
            let db = Database::open_file(io, db_path, false)
            .expect("opening limbo database failed");
            db.connect().expect("limbo connect failed")
        }
        else {
            panic!("DB path not provided")
        }
    }

    pub fn connect_rusqlite(&self) -> rusqlite::Connection {
        let conn = rusqlite::Connection::open(&self.path).unwrap();
        conn.pragma_update(None, "locking_mode", "EXCLUSIVE").unwrap();
        conn
    }
}



fn rusqlite_open() -> rusqlite::Connection {
    let sqlite_conn = rusqlite::Connection::open("../testing/database.db").unwrap();
    sqlite_conn
        .pragma_update(None, "locking_mode", "EXCLUSIVE")
        .unwrap();
    sqlite_conn
}

fn bench_join_query(criterion: &mut Criterion) {
    // Skip rusqlite if disabled via env var
    let enable_rusqlite = std::env::var("DISABLE_RUSQLITE_BENCHMARK").is_err();

    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io.clone(), "../testing/database.db", false).unwrap();
    let limbo_conn = db.connect().unwrap();

    // Setup tables
    // TODO: this should theoretically be done once externally
    limbo_conn.execute("DROP TABLE IF EXISTS users").unwrap();
    limbo_conn.execute("DROP TABLE IF EXISTS orders").unwrap();

    limbo_conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY)").unwrap();
    limbo_conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER)").unwrap();

    for i in 0..10_000 {
        limbo_conn.execute(&format!("INSERT INTO users VALUES ({})", i)).unwrap();
        limbo_conn.execute(&format!("INSERT INTO orders VALUES ({}, {})", i, i)).unwrap();
    }

    // The join query
    let query = "SELECT u.id, o.id FROM users u JOIN orders o ON u.id = o.user_id";

    let mut group = criterion.benchmark_group("join_query");

    group.bench_with_input(
        BenchmarkId::new("limbo_prepare_join", query),
        &query,
        |b, query| {
            b.iter(|| {
                limbo_conn.prepare(query).unwrap();
            });
        },
    );

    // Benchmark Limbo execution
    group.bench_with_input(
        BenchmarkId::new("limbo_execute", query),
        &query,
        |b, query| {
            let mut stmt = limbo_conn.prepare(query).unwrap();
            let io = io.clone();
            b.iter(|| {
                loop {
                    match stmt.step().unwrap() {
                        limbo_core::StepResult::Row => {
                            black_box(stmt.row());
                        }
                        limbo_core::StepResult::IO => {
                            let _ = io.run_once();
                        }
                        limbo_core::StepResult::Done => {
                            break;
                        }
                        limbo_core::StepResult::Interrupt | limbo_core::StepResult::Busy => {
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
            BenchmarkId::new("sqlite_prepare_join", query),
            &query,
            |b, query| {
                b.iter(|| {
                    sqlite_conn.prepare(query).unwrap();
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("sqlite_execute", query),
            &query,
            |b, query| {
                let mut stmt = sqlite_conn.prepare(query).unwrap();
                let mut rows = stmt.query([]).unwrap();
                b.iter(|| {
                    while let Some(row) = rows.next().unwrap() {
                        black_box(row);
                    }
                });
            },
        );
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_join_query
}
criterion_main!(benches);
