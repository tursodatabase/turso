use std::cell::RefCell;
use std::sync::Arc;

use criterion::async_executor::FuturesExecutor;
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use pprof::criterion::{Output, PProfProfiler};
use turso_core::mvcc::clock::LocalClock;
use turso_core::mvcc::database::{MvStore, Row, RowID};
use turso_core::types::{ImmutableRecord, Text};
use turso_core::{Connection, Database, MemoryIO, Value};

struct BenchDb {
    _db: Arc<Database>,
    conn: Arc<Connection>,
    mvcc_store: Arc<MvStore<LocalClock>>,
}

fn bench_db() -> BenchDb {
    let io = Arc::new(MemoryIO::new());
    let db = Database::open_file(io.clone(), ":memory:", true, true).unwrap();
    let conn = db.connect().unwrap();
    let mvcc_store = db.get_mv_store().unwrap().clone();
    BenchDb {
        _db: db,
        conn,
        mvcc_store,
    }
}

macro_rules! prepare_tx_statements {
    ($db:ident, $begin_stmt:ident, $commit_stmt:ident) => {
        let $begin_stmt = RefCell::new($db.conn.prepare("BEGIN TRANSACTION;").unwrap());
        let $commit_stmt = RefCell::new($db.conn.prepare("COMMIT;").unwrap());
    };
}

fn prepare_table(conn: Arc<Connection>) {
    conn.execute("CREATE TABLE dummy (id INTEGER PRIMARY KEY, text TEXT);")
        .unwrap();
    conn.execute("INSERT INTO dummy (id, text) VALUES (1, 'World');")
        .unwrap();
}

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("mvcc-ops-throughput");
    group.throughput(Throughput::Elements(1));

    let db = bench_db();
    let conn = db.conn.clone();
    let begin_stmt = RefCell::new(conn.prepare("BEGIN TRANSACTION;").unwrap());
    let rollback_stmt = RefCell::new(conn.prepare("ROLLBACK;").unwrap());
    group.bench_function("begin_tx + rollback_tx", |b| {
        b.to_async(FuturesExecutor).iter(|| async {
            begin_stmt.borrow_mut().step().unwrap();
            rollback_stmt.borrow_mut().step().unwrap();
        })
    });

    let db = bench_db();
    prepare_tx_statements!(db, begin_stmt, commit_stmt);
    group.bench_function("begin_tx-commit_tx", |b| {
        b.to_async(FuturesExecutor).iter(|| async {
            begin_stmt.borrow_mut().step().unwrap();
            commit_stmt.borrow_mut().step().unwrap();
        })
    });

    let db = bench_db();
    prepare_tx_statements!(db, begin_stmt, commit_stmt);
    let read_stmt = RefCell::new(db.conn.prepare("SELECT 1").unwrap());
    group.bench_function("begin_tx-read-commit_tx", |b| {
        b.to_async(FuturesExecutor).iter(|| async {
            begin_stmt.borrow_mut().step().unwrap();
            read_stmt.borrow_mut().step().unwrap();
            commit_stmt.borrow_mut().step().unwrap();
        })
    });

    let db = bench_db();
    prepare_table(db.conn.clone());
    prepare_tx_statements!(db, begin_stmt, commit_stmt);
    let update_stmt = RefCell::new(
        db.conn
            .prepare("UPDATE dummy SET id = 1 WHERE id = 1;")
            .unwrap(),
    );
    group.bench_function("begin_tx-update-commit_tx", |b| {
        b.to_async(FuturesExecutor).iter(|| async {
            begin_stmt.borrow_mut().step().unwrap();
            update_stmt.borrow_mut().step().unwrap();
            commit_stmt.borrow_mut().step().unwrap();
        })
    });

    let db = bench_db();
    prepare_table(db.conn.clone());
    db.conn.execute("BEGIN TRANSACTION;").unwrap();
    let read_stmt = RefCell::new(
        db.conn
            .prepare("SELECT * FROM dummy WHERE id = 1;")
            .unwrap(),
    );
    group.bench_function("read", |b| {
        b.to_async(FuturesExecutor).iter(|| async {
            read_stmt.borrow_mut().step().unwrap();
        })
    });

    let db = bench_db();
    prepare_table(db.conn.clone());
    db.conn.execute("BEGIN TRANSACTION;").unwrap();
    let update_stmt = RefCell::new(
        db.conn
            .prepare("UPDATE dummy SET text = 'World' WHERE id = 1;")
            .unwrap(),
    );
    group.bench_function("update", |b| {
        b.to_async(FuturesExecutor).iter(|| async {
            update_stmt.borrow_mut().step().unwrap();
        })
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench
}
criterion_main!(benches);
