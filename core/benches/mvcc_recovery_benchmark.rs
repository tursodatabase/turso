//! Benchmarks MVCC startup recovery: replaying the logical log into in-memory
//! MVCC state when a database is opened.

use std::hint::black_box;
use std::sync::Arc;
use turso_core::SqliteDialect;

#[cfg(not(feature = "codspeed"))]
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

#[cfg(feature = "codspeed")]
use codspeed_criterion_compat::{
    criterion_group, criterion_main, BenchmarkId, Criterion, Throughput,
};

use turso_core::{ConnectOpts, Database, DatabaseOpts, OpenFlags, PlatformIO};

/// A prepared, file-backed MVCC database whose logical log holds the frames to
/// be replayed.
struct RecoveryFixture {
    _temp_dir: tempfile::TempDir,
    path: String,
    io: Arc<PlatformIO>,
    file_size: u64,
}

impl RecoveryFixture {
    fn open_and_recover(&self) {
        let db = Database::open_file_with_flags(
            self.io.clone(),
            &self.path,
            OpenFlags::default(),
            DatabaseOpts::new(),
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap();
        black_box(&db);
    }
}

fn build_mvcc_db_with_log(populate: impl FnOnce(&Arc<turso_core::Connection>)) -> RecoveryFixture {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir
        .path()
        .join("recovery.db")
        .to_str()
        .unwrap()
        .to_string();
    let io = Arc::new(PlatformIO::new().unwrap());

    let db = Database::open_file_with_flags(
        io.clone(),
        &path,
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db.connect(ConnectOpts::new()).unwrap();
    conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
    conn.execute("PRAGMA mvcc_checkpoint_threshold = -1")
        .unwrap();
    conn.execute("PRAGMA synchronous = OFF").unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB)")
        .unwrap();
    populate(&conn);
    conn.close().unwrap();
    drop(db);

    let file_size = std::fs::metadata(format!("{path}-log"))
        .map(|m| m.len())
        .unwrap_or(0);

    RecoveryFixture {
        _temp_dir: temp_dir,
        path,
        io,
        file_size,
    }
}

/// Many tiny single-row transactions: `num_frames` frames, each ~minimal.
fn build_small_frames(num_frames: u64) -> RecoveryFixture {
    build_mvcc_db_with_log(|conn| {
        for i in 0..num_frames {
            // Auto-commit => one logical-log frame per statement.
            conn.execute(format!("INSERT INTO t(id, v) VALUES ({i}, zeroblob(16))"))
                .unwrap();
        }
    })
}

/// A handful of transactions, each inserting one very large blob.
fn frames_with_one_blob_op_each(num_frames: u64, payload_bytes: u64) -> RecoveryFixture {
    build_mvcc_db_with_log(|conn| {
        for i in 0..num_frames {
            conn.execute(format!(
                "INSERT INTO t(id, v) VALUES ({i}, zeroblob({payload_bytes}))"
            ))
            .unwrap();
        }
    })
}

/// A single transaction containing `num_ops` row inserts: one frame, arbitrary op count.
fn single_frame_with_num_ops(num_ops: u64) -> RecoveryFixture {
    build_mvcc_db_with_log(|conn| {
        conn.execute("BEGIN").unwrap();
        for i in 0..num_ops {
            conn.execute(format!("INSERT INTO t(id, v) VALUES ({i}, zeroblob(16))"))
                .unwrap();
        }
        conn.execute("COMMIT").unwrap();
    })
}

/// `num_updates` single-row UPDATE transactions on 1000 rows of about 300 bytes
/// that a checkpoint wrote to the B-tree. Each UPDATE changes one integer
/// column. With `update_deltas`, the log has one `OP_UPDATE_TABLE` op for each
/// update, and recovery reads the base of the first update of each row from the
/// B-tree.
fn updates_of_checkpointed_rows(num_updates: u64, update_deltas: bool) -> RecoveryFixture {
    build_mvcc_db_with_log(|conn| {
        conn.execute("CREATE TABLE u(id INTEGER PRIMARY KEY, n INTEGER, v BLOB)")
            .unwrap();
        conn.execute(
            "INSERT INTO u SELECT value, 0, randomblob(300) FROM generate_series(1, 1000)",
        )
        .unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        if update_deltas {
            conn.execute("PRAGMA mvcc_update_deltas = on").unwrap();
        }
        for i in 0..num_updates {
            conn.execute(format!(
                "UPDATE u SET n = n + 1 WHERE id = {}",
                i % 1000 + 1
            ))
            .unwrap();
        }
    })
}

#[turso_macros::codspeed_criterion_benchmark]
fn bench_recovery(c: &mut Criterion) {
    {
        let mut group = c.benchmark_group("mvcc-recovery/small-frames");
        for &num_frames in &[1, 100, 1000, 10_000, 100_000] {
            let fixture = build_small_frames(num_frames);
            group.throughput(Throughput::Elements(num_frames));
            group.bench_with_input(
                BenchmarkId::from_parameter(num_frames),
                &fixture,
                |b, fixture| b.iter(|| fixture.open_and_recover()),
            );
        }
        group.finish();
    }

    {
        let mut group = c.benchmark_group("mvcc-recovery/large-frames");
        const NUM_FRAMES: u64 = 8;
        for &payload_bytes in &[64 * 1024, 1024 * 1024, 8 * 1024 * 1024] {
            let fixture = frames_with_one_blob_op_each(NUM_FRAMES, payload_bytes);
            group.throughput(Throughput::Bytes(fixture.file_size));
            group.bench_with_input(
                BenchmarkId::from_parameter(payload_bytes),
                &fixture,
                |b, fixture| b.iter(|| fixture.open_and_recover()),
            );
        }
        group.finish();
    }

    {
        let mut group = c.benchmark_group("mvcc-recovery/updates");
        for &num_updates in &[1000, 10_000] {
            for update_deltas in [false, true] {
                let fixture = updates_of_checkpointed_rows(num_updates, update_deltas);
                let setting = if update_deltas { "deltas" } else { "full-rows" };
                group.throughput(Throughput::Elements(num_updates));
                group.bench_with_input(
                    BenchmarkId::new(setting, num_updates),
                    &fixture,
                    |b, fixture| b.iter(|| fixture.open_and_recover()),
                );
            }
        }
        group.finish();
    }

    {
        let mut group = c.benchmark_group("mvcc-recovery/wide-frame");
        for &num_ops in &[1, 100, 1000, 10_000, 100_000] {
            let fixture = single_frame_with_num_ops(num_ops);
            group.throughput(Throughput::Elements(num_ops));
            group.bench_with_input(
                BenchmarkId::from_parameter(num_ops),
                &fixture,
                |b, fixture| b.iter(|| fixture.open_and_recover()),
            );
        }
        group.finish();
    }
}

#[cfg(not(feature = "codspeed"))]
criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(10);
    targets = bench_recovery
}

#[cfg(feature = "codspeed")]
criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = bench_recovery
}

criterion_main!(benches);
