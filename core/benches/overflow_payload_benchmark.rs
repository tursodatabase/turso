//! Reads of cell payloads that spill onto overflow pages.
//!
//! A payload larger than the per-page local limit is stored as a chain of
//! overflow pages, and every read of such a cell walks the chain and copies
//! it back into one buffer. That copy is the cost this file measures, so the
//! payload sizes step from just past the local limit to the 512 KiB the FTS
//! index method uses for its segment chunks.
//!
//! Each size reads the same total number of bytes, so the three numbers are
//! directly comparable: what changes between them is the length of the chain
//! behind one cell, not how much data the scan returns.
//!
//! Run:  cargo bench -p turso_core --bench overflow_payload_benchmark

#[cfg(feature = "codspeed")]
use codspeed_criterion_compat::{
    black_box, criterion_group, criterion_main, BenchmarkId, Criterion,
};
#[cfg(not(feature = "codspeed"))]
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};

use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use turso_core::{Connection, Database, PlatformIO, SqliteDialect, Statement, StepResult, Value};

#[cfg(not(target_family = "wasm"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Bytes each variant reads in total, split into rows of the payload size
/// under test.
const TOTAL_PAYLOAD_BYTES: usize = 2 * 1024 * 1024;

/// Payload sizes per row. 4 KiB is the first size that does not fit in a page
/// and so needs a chain at all; 512 KiB matches `fts::DEFAULT_CHUNK_SIZE`.
const PAYLOAD_SIZES: [usize; 3] = [4 * 1024, 64 * 1024, 512 * 1024];

struct Fixture {
    _dir: TempDir,
    db: Arc<Database>,
    conn: Arc<Connection>,
    row_count: usize,
}

/// A payload of `size` bytes that never repeats across rows, so no layer can
/// serve a later row from an earlier row's bytes.
fn payload_text(row: usize, size: usize) -> String {
    let seed = format!("row-{row:08}-");
    seed.repeat(size.div_ceil(seed.len()))[..size].to_string()
}

fn seed_db(payload_size: usize) -> Fixture {
    let row_count = TOTAL_PAYLOAD_BYTES / payload_size;
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("overflow.db");
    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io, path.to_str().unwrap(), Arc::new(SqliteDialect)).unwrap();
    let conn = db.connect().unwrap();

    // Hold the whole table resident so the measurement is the chain walk and
    // the copy, not page reads.
    conn.execute("PRAGMA cache_size=-65536").unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    let mut insert = conn.prepare("INSERT INTO t VALUES (?1, ?2)").unwrap();
    for row in 0..row_count {
        insert
            .bind_at(1usize.try_into().unwrap(), Value::from_i64(row as i64))
            .unwrap();
        insert
            .bind_at(
                2usize.try_into().unwrap(),
                Value::build_text(payload_text(row, payload_size)),
            )
            .unwrap();
        drive_stmt_to_completion(&db, &mut insert);
    }
    conn.execute("COMMIT").unwrap();

    Fixture {
        _dir: dir,
        db,
        conn,
        row_count,
    }
}

fn drive_stmt_to_completion(db: &Database, stmt: &mut Statement) -> usize {
    let mut rows = 0;
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                black_box(stmt.row());
                rows += 1;
            }
            StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => {
                db.io.step().unwrap();
            }
            StepResult::Done => break,
            StepResult::Interrupt | StepResult::Busy => unreachable!(),
        }
    }
    stmt.reset().unwrap();
    rows
}

#[turso_macros::codspeed_criterion_benchmark]
fn bench_overflow_payload_scan(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("overflow_payload");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(5));
    group.warm_up_time(Duration::from_secs(3));

    for payload_size in PAYLOAD_SIZES {
        let fixture = seed_db(payload_size);
        group.bench_function(
            BenchmarkId::new("scan", format!("{}KiB_payload", payload_size / 1024)),
            |b| {
                let mut stmt = fixture.conn.prepare("SELECT payload FROM t").unwrap();
                assert_eq!(
                    drive_stmt_to_completion(&fixture.db, &mut stmt),
                    fixture.row_count,
                    "t should have been seeded with {} rows",
                    fixture.row_count
                );
                b.iter(|| drive_stmt_to_completion(&fixture.db, &mut stmt));
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_overflow_payload_scan);
criterion_main!(benches);
