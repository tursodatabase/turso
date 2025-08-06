use std::sync::Arc;

use criterion::async_executor::FuturesExecutor;
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use pprof::criterion::{Output, PProfProfiler};
use turso_core::mvcc::clock::LocalClock;
use turso_core::mvcc::database::{MvStore, Row, RowID};
use turso_core::types::{ImmutableRecord, Text};
use turso_core::{Connection, Database, MemoryIO, Value};
use turso_core::{StateTransition, TransitionResult};

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

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("mvcc-ops-throughput");
    group.throughput(Throughput::Elements(1));

    let db = bench_db();
    group.bench_function("begin_tx + rollback_tx", |b| {
        let db = bench_db();
        b.to_async(FuturesExecutor).iter(|| async {
            let conn = db.conn.clone();
            let tx_id = db.mvcc_store.begin_tx(conn.get_pager().clone());
            db.mvcc_store.rollback_tx(tx_id, conn.get_pager().clone())
        })
    });

    let db = bench_db();
    group.bench_function("begin_tx + commit_tx", |b| {
        b.to_async(FuturesExecutor).iter(|| async {
            let conn = &db.conn;
            let tx_id = db.mvcc_store.begin_tx(conn.get_pager().clone());
            let mut sm = db
                .mvcc_store
                .commit_tx(tx_id, conn.get_pager().clone(), conn)
                .unwrap();

            let result = sm.step(&db.mvcc_store.clone()).unwrap();
            assert!(sm.is_finalized());
            match result {
                TransitionResult::Done(()) => {}
                _ => unreachable!(),
            }
        })
    });

    let db = bench_db();
    group.bench_function("begin_tx-read-commit_tx", |b| {
        b.to_async(FuturesExecutor).iter(|| async {
            let conn = &db.conn;
            let tx_id = db.mvcc_store.begin_tx(conn.get_pager().clone());
            db.mvcc_store
                .read(
                    tx_id,
                    RowID {
                        table_id: 1,
                        row_id: 1,
                    },
                )
                .unwrap();
            db.mvcc_store
                .commit_tx(tx_id, conn.get_pager().clone(), conn)
        })
    });

    let db = bench_db();
    let record = ImmutableRecord::from_values(&vec![Value::Text(Text::new("World"))], 1);
    let record_data = record.as_blob();
    group.bench_function("begin_tx-update-commit_tx", |b| {
        b.to_async(FuturesExecutor).iter(|| async {
            let conn = &db.conn;
            let tx_id = db.mvcc_store.begin_tx(conn.get_pager().clone());
            db.mvcc_store
                .update(
                    tx_id,
                    Row {
                        id: RowID {
                            table_id: 1,
                            row_id: 1,
                        },
                        data: record_data.clone(),
                        column_count: 1,
                    },
                    conn.get_pager().clone(),
                )
                .unwrap();
            db.mvcc_store
                .commit_tx(tx_id, conn.get_pager().clone(), conn)
                .unwrap();
        })
    });

    let db = bench_db();
    let tx_id = db.mvcc_store.begin_tx(db.conn.get_pager().clone());
    db.mvcc_store
        .insert(
            tx_id,
            Row {
                id: RowID {
                    table_id: 1,
                    row_id: 1,
                },
                data: record_data.clone(),
                column_count: 1,
            },
        )
        .unwrap();
    group.bench_function("read", |b| {
        b.to_async(FuturesExecutor).iter(|| async {
            db.mvcc_store
                .read(
                    tx_id,
                    RowID {
                        table_id: 1,
                        row_id: 1,
                    },
                )
                .unwrap();
        })
    });

    let db = bench_db();
    let tx_id = db.mvcc_store.begin_tx(db.conn.get_pager().clone());
    let conn = &db.conn;
    db.mvcc_store
        .insert(
            tx_id,
            Row {
                id: RowID {
                    table_id: 1,
                    row_id: 1,
                },
                data: record_data.clone(),
                column_count: 1,
            },
        )
        .unwrap();
    group.bench_function("update", |b| {
        b.to_async(FuturesExecutor).iter(|| async {
            db.mvcc_store
                .update(
                    tx_id,
                    Row {
                        id: RowID {
                            table_id: 1,
                            row_id: 1,
                        },
                        data: record_data.clone(),
                        column_count: 1,
                    },
                    conn.get_pager().clone(),
                )
                .unwrap();
        })
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench
}
criterion_main!(benches);
