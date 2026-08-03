use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;

#[cfg(feature = "codspeed")]
use codspeed_criterion_compat::{
    black_box, criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode,
};
#[cfg(not(feature = "codspeed"))]
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode};

use turso_core::{Database, PlatformIO, SqliteDialect, StepResult};

struct Scenario {
    name: &'static str,
    query: &'static str,
    dir: TempDir,
}

impl Scenario {
    fn db_path(&self) -> PathBuf {
        self.dir.path().join("bench.db")
    }
}

fn create_db_with_schema(schema: &str) -> (TempDir, rusqlite::Connection) {
    let dir = TempDir::new().unwrap();
    let conn = rusqlite::Connection::open(dir.path().join("bench.db")).unwrap();
    conn.execute_batch("PRAGMA journal_mode=DELETE;").unwrap();
    conn.execute_batch(schema).unwrap();
    (dir, conn)
}

fn seed_pk_pk_join() -> Scenario {
    let (dir, conn) = create_db_with_schema(
        "CREATE TABLE a(id INTEGER PRIMARY KEY, x INT);
         CREATE TABLE b(id INTEGER PRIMARY KEY, y INT);",
    );
    let tx = conn.unchecked_transaction().unwrap();
    {
        let mut insert_a = tx.prepare("INSERT INTO a(id, x) VALUES (?1, ?2)").unwrap();
        for id in 1..=100_000i64 {
            insert_a.execute((id, id % 1_000)).unwrap();
        }
        let mut insert_b = tx.prepare("INSERT INTO b(id, y) VALUES (?1, ?2)").unwrap();
        for id in 50_001..=150_000i64 {
            insert_b.execute((id, id % 1_000)).unwrap();
        }
    }
    tx.commit().unwrap();
    Scenario {
        name: "pk_pk_join",
        query: "SELECT count(*), sum(a.x + b.y) FROM a JOIN b ON a.id = b.id",
        dir,
    }
}

fn seed_pk_composite_fanout() -> Scenario {
    let (dir, conn) = create_db_with_schema(
        "CREATE TABLE orders(o_id INTEGER PRIMARY KEY, o_v INT);
         CREATE TABLE lineitem(l_oid INT, l_ln INT, l_q INT, PRIMARY KEY(l_oid, l_ln));",
    );
    let tx = conn.unchecked_transaction().unwrap();
    {
        let mut insert_order = tx
            .prepare("INSERT INTO orders(o_id, o_v) VALUES (?1, ?2)")
            .unwrap();
        let mut insert_line = tx
            .prepare("INSERT INTO lineitem(l_oid, l_ln, l_q) VALUES (?1, ?2, ?3)")
            .unwrap();
        for o_id in 1..=50_000i64 {
            insert_order.execute((o_id, o_id % 100)).unwrap();
            for l_ln in 1..=4i64 {
                insert_line
                    .execute((o_id, l_ln, (o_id + l_ln) % 50))
                    .unwrap();
            }
        }
    }
    tx.commit().unwrap();
    Scenario {
        name: "pk_composite_fanout",
        query: "SELECT count(*), sum(o_v) FROM orders JOIN lineitem ON l_oid = o_id",
        dir,
    }
}

fn seed_secondary_index_dups() -> Scenario {
    let (dir, conn) = create_db_with_schema(
        "CREATE TABLE l(id INTEGER PRIMARY KEY, k INT);
         CREATE TABLE r(id INTEGER PRIMARY KEY, k INT);",
    );
    let tx = conn.unchecked_transaction().unwrap();
    {
        let mut insert_l = tx.prepare("INSERT INTO l(id, k) VALUES (?1, ?2)").unwrap();
        let mut insert_r = tx.prepare("INSERT INTO r(id, k) VALUES (?1, ?2)").unwrap();
        for id in 1..=100_000i64 {
            let k = (id - 1) % 25_000 + 1;
            insert_l.execute((id, k)).unwrap();
            insert_r.execute((id, k)).unwrap();
        }
    }
    tx.commit().unwrap();
    conn.execute_batch(
        "CREATE INDEX idx_l_k ON l(k);
         CREATE INDEX idx_r_k ON r(k);",
    )
    .unwrap();
    Scenario {
        name: "secondary_index_dups",
        query: "SELECT count(*) FROM l JOIN r ON l.k = r.k",
        dir,
    }
}

fn seed_three_way_chain() -> Scenario {
    let (dir, conn) = create_db_with_schema(
        "CREATE TABLE customer(c_custkey INTEGER PRIMARY KEY, c_v INT);
         CREATE TABLE orders(o_id INTEGER PRIMARY KEY, o_custkey INT, o_v INT);
         CREATE TABLE lineitem(l_oid INT, l_ln INT, l_q INT, PRIMARY KEY(l_oid, l_ln));",
    );
    let tx = conn.unchecked_transaction().unwrap();
    {
        let mut insert_customer = tx
            .prepare("INSERT INTO customer(c_custkey, c_v) VALUES (?1, ?2)")
            .unwrap();
        for c_custkey in 1..=10_000i64 {
            insert_customer
                .execute((c_custkey, c_custkey % 100))
                .unwrap();
        }
        let mut insert_order = tx
            .prepare("INSERT INTO orders(o_id, o_custkey, o_v) VALUES (?1, ?2, ?3)")
            .unwrap();
        let mut insert_line = tx
            .prepare("INSERT INTO lineitem(l_oid, l_ln, l_q) VALUES (?1, ?2, ?3)")
            .unwrap();
        for o_id in 1..=50_000i64 {
            let o_custkey = (o_id - 1) % 10_000 + 1;
            insert_order.execute((o_id, o_custkey, o_id % 100)).unwrap();
            for l_ln in 1..=4i64 {
                insert_line
                    .execute((o_id, l_ln, (o_id + l_ln) % 50))
                    .unwrap();
            }
        }
    }
    tx.commit().unwrap();
    conn.execute_batch("CREATE INDEX idx_orders_custkey ON orders(o_custkey);")
        .unwrap();
    Scenario {
        name: "three_way_chain",
        query: "SELECT count(*) FROM customer JOIN orders ON o_custkey = c_custkey JOIN lineitem ON l_oid = o_id",
        dir,
    }
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
fn bench_merge_join(criterion: &mut Criterion) {
    let enable_rusqlite = std::env::var("DISABLE_RUSQLITE_BENCHMARK").is_err();

    let scenarios = [
        seed_pk_pk_join(),
        seed_pk_composite_fanout(),
        seed_secondary_index_dups(),
        seed_three_way_chain(),
    ];

    for scenario in &scenarios {
        let path = scenario.db_path();

        #[allow(clippy::arc_with_non_send_sync)]
        let io = Arc::new(PlatformIO::new().unwrap());
        let db = Database::open_file(io, path.to_str().unwrap(), Arc::new(SqliteDialect)).unwrap();
        let limbo_conn = db.connect().unwrap();
        {
            let mut pragma = limbo_conn.prepare("PRAGMA cache_size=-32768").unwrap();
            drain_turso(&db, &mut pragma);
        }

        let mut group = criterion.benchmark_group(format!("merge_join_{}", scenario.name));
        group.sampling_mode(SamplingMode::Flat);
        group.sample_size(10);

        group.bench_with_input(
            BenchmarkId::new("limbo_merge_join", scenario.name),
            &scenario.query,
            |b, query| {
                let mut stmt = limbo_conn.prepare(query).unwrap();
                b.iter(|| drain_turso(&db, &mut stmt));
            },
        );

        if enable_rusqlite {
            let sqlite_conn = rusqlite::Connection::open(&path).unwrap();
            sqlite_conn
                .pragma_update(None, "locking_mode", "EXCLUSIVE")
                .unwrap();

            group.bench_with_input(
                BenchmarkId::new("sqlite_merge_join", scenario.name),
                &scenario.query,
                |b, query| {
                    let mut stmt = sqlite_conn.prepare(query).unwrap();
                    b.iter(|| {
                        let mut rows = stmt.raw_query();
                        while let Some(row) = rows.next().unwrap() {
                            black_box(row);
                        }
                    });
                },
            );
        }

        group.finish();
    }
}

criterion_group!(benches, bench_merge_join);
criterion_main!(benches);
