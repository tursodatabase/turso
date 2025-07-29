use criterion::async_executor::FuturesExecutor;
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use pprof::criterion::{Output, PProfProfiler};
use std::rc::Rc;
use turso_core::mvcc::clock::LocalClock;
use turso_core::mvcc::database::{MvStore, Row, RowID};
use turso_core::storage::database::FileMemoryStorage;
use turso_core::storage::page_cache::DumbLruPageCache;
use turso_core::storage::pager::Pager;
use turso_core::storage::sqlite3_ondisk::DatabaseHeader;
use turso_core::storage::wal::DummyWAL;
use turso_core::OpenFlags;

fn bench_db() -> MvStore<LocalClock> {
    let clock = LocalClock::default();
    MvStore::new(clock)
}

fn dummy_pager() -> Rc<Pager> {
    let storage = FileMemoryStorage::new();
    let cache = DumbLruPageCache::new(1024);
    let wal = DummyWAL::new();
    let header = DatabaseHeader::new(1024, 1, 1);
    let pager = Pager::new(header, storage, cache, wal, OpenFlags::ReadWrite).unwrap();
    Rc::new(pager)
}

fn dummy_connection() -> turso_core::Connection {
    let storage = FileMemoryStorage::new();
    let cache = DumbLruPageCache::new(1024);
    let wal = DummyWAL::new();
    let header = DatabaseHeader::new(1024, 1, 1);
    let pager = Pager::new(header, storage, cache, wal, OpenFlags::ReadWrite).unwrap();
    turso_core::Connection::new(pager).unwrap()
}

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("mvcc-ops-throughput");
    group.throughput(Throughput::Elements(1));

    let db = bench_db();
    group.bench_function("begin_tx + rollback_tx", |b| {
        b.to_async(FuturesExecutor).iter(|| async {
            let tx_id = db.begin_tx();
            db.rollback_tx(tx_id)
        })
    });

    let db = bench_db();
    group.bench_function("begin_tx + commit_tx", |b| {
        b.to_async(FuturesExecutor).iter(|| async {
            let tx_id = db.begin_tx();
            db.commit_tx(tx_id, dummy_pager(), &dummy_connection())
        })
    });

    let db = bench_db();
    group.bench_function("begin_tx-read-commit_tx", |b| {
        b.to_async(FuturesExecutor).iter(|| async {
            let tx_id = db.begin_tx();
            db.read(
                tx_id,
                RowID {
                    table_id: 1,
                    row_id: 1,
                },
                dummy_pager(),
            )
            .unwrap();
            db.commit_tx(tx_id, dummy_pager(), &dummy_connection())
        })
    });

    let db = bench_db();
    group.bench_function("begin_tx-update-commit_tx", |b| {
        b.to_async(FuturesExecutor).iter(|| async {
            let tx_id = db.begin_tx();
            db.update(
                tx_id,
                Row {
                    id: RowID {
                        table_id: 1,
                        row_id: 1,
                    },
                    data: "World".to_string().into_bytes(),
                },
            )
            .unwrap();
            db.commit_tx(tx_id, dummy_pager(), &dummy_connection())
        })
    });

    let db = bench_db();
    let tx = db.begin_tx();
    db.insert(
        tx,
        Row {
            id: RowID {
                table_id: 1,
                row_id: 1,
            },
            data: "Hello".to_string().into_bytes(),
        },
    )
    .unwrap();
    group.bench_function("read", |b| {
        b.to_async(FuturesExecutor).iter(|| async {
            db.read(
                tx,
                RowID {
                    table_id: 1,
                    row_id: 1,
                },
                dummy_pager(),
            )
            .unwrap();
        })
    });

    let db = bench_db();
    let tx = db.begin_tx();
    db.insert(
        tx,
        Row {
            id: RowID {
                table_id: 1,
                row_id: 1,
            },
            data: "Hello".to_string().into_bytes(),
        },
    )
    .unwrap();
    group.bench_function("update", |b| {
        b.to_async(FuturesExecutor).iter(|| async {
            db.update(
                tx,
                Row {
                    id: RowID {
                        table_id: 1,
                        row_id: 1,
                    },
                    data: "World".to_string().into_bytes(),
                },
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
