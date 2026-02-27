use std::sync::Arc;

#[cfg(not(feature = "codspeed"))]
use criterion::{
    async_executor::FuturesExecutor, criterion_group, criterion_main, Criterion, Throughput,
};
#[cfg(not(feature = "codspeed"))]
use pprof::criterion::{Output, PProfProfiler};

#[cfg(feature = "codspeed")]
use codspeed_criterion_compat::{
    async_executor::FuturesExecutor, criterion_group, criterion_main, Criterion, Throughput,
};

use turso_core::mvcc::clock::MvccClock;
use turso_core::mvcc::database::{MvStore, Row, RowID, RowKey};
use turso_core::types::{IOResult, ImmutableRecord, Text};
use turso_core::{Connection, Database, MemoryIO, Value};

struct BenchDb {
    _db: Arc<Database>,
    conn: Arc<Connection>,
    mvcc_store: Arc<MvStore<MvccClock>>,
}

fn bench_db() -> BenchDb {
    let io = Arc::new(MemoryIO::new());
    let db = Database::open_file(io, ":memory:").unwrap();
    let conn = db.connect().unwrap();
    // Enable MVCC via PRAGMA
    conn.execute("PRAGMA journal_mode = 'experimental_mvcc'")
        .unwrap();
    let mvcc_store = db.get_mv_store().clone().unwrap();
    BenchDb {
        _db: db,
        conn,
        mvcc_store,
    }
}

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("mvcc-ops-throughput");
    group.throughput(Throughput::Elements(1));

    group.bench_function("begin_tx + rollback_tx", |b| {
        let db = bench_db();
        b.to_async(FuturesExecutor).iter(|| async {
            let conn = db.conn.clone();
            let tx_id = db.mvcc_store.begin_tx(conn.get_pager()).unwrap();
            db.mvcc_store.rollback_tx(tx_id, conn.get_pager(), &conn);
        })
    });

    let db = bench_db();
    group.bench_function("begin_tx + commit_tx", |b| {
        b.to_async(FuturesExecutor).iter(|| async {
            let conn = &db.conn;
            let tx_id = db.mvcc_store.begin_tx(conn.get_pager()).unwrap();
            let mv_store = &db.mvcc_store;
            let mut sm = mv_store.commit_tx(tx_id, conn).unwrap();
            // TODO: sync IO hack
            loop {
                let res = sm.step(mv_store).unwrap();
                match res {
                    IOResult::IO(io) => io.wait(db._db.io.as_ref()).unwrap(),
                    IOResult::Done(_) => break,
                }
            }
        })
    });

    let db = bench_db();
    group.bench_function("begin_tx-read-commit_tx", |b| {
        b.to_async(FuturesExecutor).iter(|| async {
            let conn = &db.conn;
            let tx_id = db.mvcc_store.begin_tx(conn.get_pager()).unwrap();
            db.mvcc_store
                .read(
                    tx_id,
                    &RowID {
                        table_id: (-2).into(),
                        row_id: RowKey::Int(1),
                    },
                )
                .unwrap();
            let mv_store = &db.mvcc_store;
            let mut sm = mv_store.commit_tx(tx_id, conn).unwrap();
            // TODO: sync IO hack
            loop {
                let res = sm.step(mv_store).unwrap();
                match res {
                    IOResult::IO(io) => io.wait(db._db.io.as_ref()).unwrap(),
                    IOResult::Done(_) => break,
                }
            }
        })
    });

    let db = bench_db();
    let record = ImmutableRecord::from_values(&vec![Value::Text(Text::new("World"))], 1);
    let record_data = record.as_blob();
    group.bench_function("begin_tx-update-commit_tx", |b| {
        b.to_async(FuturesExecutor).iter(|| async {
            let conn = &db.conn;
            let tx_id = db.mvcc_store.begin_tx(conn.get_pager()).unwrap();
            db.mvcc_store
                .update(
                    tx_id,
                    Row::new_table_row(
                        RowID::new((-2).into(), RowKey::Int(1)),
                        record_data.clone(),
                        1,
                    ),
                )
                .unwrap();
            let mv_store = &db.mvcc_store;
            let mut sm = mv_store.commit_tx(tx_id, conn).unwrap();
            // TODO: sync IO hack
            loop {
                let res = sm.step(mv_store).unwrap();
                match res {
                    IOResult::IO(io) => io.wait(db._db.io.as_ref()).unwrap(),
                    IOResult::Done(_) => break,
                }
            }
        })
    });

    let db = bench_db();
    let tx_id = db.mvcc_store.begin_tx(db.conn.get_pager()).unwrap();
    db.mvcc_store
        .insert(
            tx_id,
            Row::new_table_row(
                RowID::new((-2).into(), RowKey::Int(1)),
                record_data.clone(),
                1,
            ),
        )
        .unwrap();
    group.bench_function("read", |b| {
        b.to_async(FuturesExecutor).iter(|| async {
            db.mvcc_store
                .read(
                    tx_id,
                    &RowID {
                        table_id: (-2).into(),
                        row_id: RowKey::Int(1),
                    },
                )
                .unwrap();
        })
    });

    let db = bench_db();
    let tx_id = db.mvcc_store.begin_tx(db.conn.get_pager()).unwrap();
    db.mvcc_store
        .insert(
            tx_id,
            Row::new_table_row(
                RowID::new((-2).into(), RowKey::Int(1)),
                record_data.clone(),
                1,
            ),
        )
        .unwrap();
    group.bench_function("update", |b| {
        b.to_async(FuturesExecutor).iter(|| async {
            db.mvcc_store
                .update(
                    tx_id,
                    Row::new_table_row(
                        RowID::new((-2).into(), RowKey::Int(1)),
                        record_data.clone(),
                        1,
                    ),
                )
                .unwrap();
        })
    });
}

#[cfg(not(feature = "codspeed"))]
criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench
}

#[cfg(feature = "codspeed")]
criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = bench
}

criterion_main!(benches);
