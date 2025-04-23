use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use limbo_core::{Database, PlatformIO, IO};


use pprof::criterion::{Output, PProfProfiler};
use std::sync::Arc;

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

    pub fn connect_limbo(&self) -> limbo_core::Connection {
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
        let db = Database::open_file(io, &self.path, false)
            .expect("opening limbo database failed");
        db.connect().expect("limbo connect failed")
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
    // https://github.com/tursodatabase/limbo/issues/174
    // The rusqlite benchmark crashes on Mac M1 when using the flamegraph features
    let enable_rusqlite = std::env::var("DISABLE_RUSQLITE_BENCHMARK").is_err();

    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io.clone(), "../testing/database.db", false).unwrap();
    let limbo_conn = db.connect().unwrap();
    
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();

    let create_table_query = "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY)";
    limbo_conn.execute(create_table_query).unwrap();

    for i in 0..100000 {
        let insert_query = format!("INSERT INTO users VALUES ({})", i);
        limbo_conn.execute(insert_query).unwrap();
    }


    // let queries = [
    //     "SELECT u.first_name, u.last_name, p.name AS product_name, o.quantity, o.order_date \
    //      FROM users u \
    //      JOIN orders o ON u.id = o.user_id \
    //      JOIN products p ON o.product_id = p.id \
    //      LIMIT 10",
    // ];

    let queries = [
        "SELECT u.id \
         FROM users u"
    ];

    for query in queries.iter() {
        let mut group = criterion.benchmark_group(format!("Prepare `{}`", query));

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

criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_join_query
}
criterion_main!(benches);
