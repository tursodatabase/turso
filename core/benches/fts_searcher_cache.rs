#[cfg(feature = "codspeed")]
use codspeed_criterion_compat::{criterion_group, criterion_main, BenchmarkId, Criterion};
#[cfg(not(feature = "codspeed"))]
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use std::sync::Arc;
use turso_core::index_method::{fts::set_fts_retained_cache_bytes_for_test, IndexMethodContext};
use turso_core::{
    Connection, Database, DatabaseOpts, IOResult, OpenFlags, PlatformIO, SqliteDialect, StepResult,
};

#[cfg(not(target_family = "wasm"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[turso_macros::codspeed_criterion_benchmark]
fn bench_fts_searcher_cache(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("FTS Searcher Cache");
    for budget in [1, 192 * 1024 * 1024] {
        set_fts_retained_cache_bytes_for_test(Some(budget));
        let temp_dir = tempfile::tempdir().unwrap();
        #[allow(clippy::arc_with_non_send_sync)]
        let io = Arc::new(PlatformIO::new().unwrap());
        let db = Database::open_file_with_flags(
            io,
            temp_dir.path().join("fts.db").to_str().unwrap(),
            OpenFlags::default(),
            DatabaseOpts::new().with_index_method(true),
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap();
        let conn = db.connect().unwrap();
        conn.execute("PRAGMA fts_merge_threshold = 0").unwrap();
        conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
            .unwrap();
        conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
            .unwrap();
        for segment in 0..3 {
            let values = (0..1000)
                .map(|offset| {
                    let id = segment * 1000 + offset;
                    let term = if offset == 0 { "needle" } else { "haystack" };
                    format!("({id}, '{term} document {id} about database storage and indexing with additional searchable content')")
                })
                .collect::<Vec<_>>()
                .join(",");
            conn.execute(format!("INSERT INTO docs VALUES {values}"))
                .unwrap();
        }
        check_query(&conn, &db);
        let attachment = conn
            .with_schema_mut(|schema| {
                schema
                    .get_index("docs", "docs_fts")
                    .unwrap()
                    .index_method
                    .clone()
                    .unwrap()
            })
            .unwrap();
        conn.execute("BEGIN").unwrap();
        conn.execute("SELECT count(*) FROM docs").unwrap();
        let mut cursor = attachment.init().unwrap();
        let context =
            IndexMethodContext::for_test(&conn, turso_core::MAIN_DB_ID, attachment.as_ref())
                .unwrap();
        loop {
            match cursor.open_read(&context).unwrap() {
                IOResult::Done(()) => break,
                IOResult::IO(completions) => {
                    while !completions.finished() {
                        db.io.step().unwrap();
                    }
                }
            }
        }
        let stats = cursor.test_stats().unwrap().unwrap();
        assert_eq!(stats.segment_count, Some(3));
        assert!(stats.cached_connection_count.unwrap() > 0);
        if budget == 1 {
            assert!(stats.full_snapshot_loads.unwrap() > 0);
            assert!(stats.cached_bytes.unwrap() > budget);
        }
        drop(cursor);
        conn.execute("COMMIT").unwrap();
        let counters = attachment.init().unwrap();
        let before = counters.test_stats().unwrap().unwrap();
        for _ in 0..10 {
            check_query(&conn, &db);
        }
        let after = counters.test_stats().unwrap().unwrap();
        let loads = after.full_snapshot_loads.unwrap() - before.full_snapshot_loads.unwrap();
        let hits = after.read_cache_hits.unwrap() - before.read_cache_hits.unwrap();
        assert_eq!(hits, 10);
        if budget > 1 {
            assert_eq!(loads, 0);
            assert!(after.cached_bytes.unwrap() < budget);
        }
        eprintln!("budget={budget} rows=3000 segments=3 retained_bytes={} queries=10 segment_loads={loads} searcher_hits={hits}", after.cached_bytes.unwrap());
        group.bench_function(BenchmarkId::new("3000_rows_3_segments", budget), |b| {
            b.iter(|| check_query(&conn, &db));
        });
        set_fts_retained_cache_bytes_for_test(None);
    }
    group.finish();
}

fn check_query(conn: &Arc<Connection>, db: &Arc<Database>) {
    let mut stmt = conn
        .query("SELECT id FROM docs WHERE body MATCH 'needle' ORDER BY id")
        .unwrap()
        .unwrap();
    let mut count = 0;
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                assert_eq!(stmt.row().unwrap().get::<i64>(0).unwrap(), count * 1000);
                count += 1;
            }
            StepResult::Done => break,
            StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => db.io.step().unwrap(),
            StepResult::Interrupt | StepResult::Busy => panic!("unexpected query interruption"),
        }
    }
    assert_eq!(count, 3);
}

criterion_group!(fts_searcher_cache, bench_fts_searcher_cache);
criterion_main!(fts_searcher_cache);
