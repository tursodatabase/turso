//! Record decoding benchmarks (core/storage/sqlite3_ondisk.rs).
//!
//! Focus: the cost of UTF-8 validation when decoding TEXT serial types.
//! `read_value` used to build `&str` values with `from_utf8_unchecked`,
//! which is undefined behavior for non-UTF-8 payloads such as
//! `CAST(X'FF' AS TEXT)` (issue #5164). It now validates with
//! `simdutf8::basic::from_utf8` and demotes invalid payloads to blobs.
//! These benchmarks quantify the validation overhead:
//!
//! - `read_value/blob` is the no-validation baseline: the same payload
//!   sizes decoded with a BLOB serial type, which just borrows the bytes.
//!   The delta between `text_*` and `blob` isolates the validation cost.
//! - `read_value/text_ascii` exercises the common all-ASCII fast path.
//! - `read_value/text_multibyte` exercises the general UTF-8 path.
//! - `read_value/text_invalid` exercises the demote-to-blob path.
//! - `scan_text_rows` measures the end-to-end impact on a full-table scan
//!   where every row's TEXT column is decoded into a register.
//!
//! Run with: cargo bench --bench record_benchmark

#[cfg(not(feature = "codspeed"))]
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
#[cfg(all(not(feature = "codspeed"), not(target_family = "windows")))]
use pprof::criterion::{Output, PProfProfiler};

#[cfg(feature = "codspeed")]
use codspeed_criterion_compat::{
    black_box, criterion_group, criterion_main, BenchmarkId, Criterion,
};

use std::sync::Arc;
use turso_core::io::MemoryIO;
use turso_core::storage::sqlite3_ondisk::read_value;
use turso_core::types::SerialType;
use turso_core::{Database, DatabaseOpts, OpenFlags, StepResult};

#[cfg(not(target_family = "wasm"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const PAYLOAD_SIZES: &[usize] = &[8, 64, 1024];

#[turso_macros::codspeed_criterion_benchmark]
fn bench_read_value_text(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("read_value");

    for &size in PAYLOAD_SIZES {
        // Baseline: BLOB serial type of the same size borrows the bytes
        // without any validation.
        let blob = vec![0xABu8; size];
        let blob_serial = SerialType::blob(size as u64);
        group.bench_with_input(BenchmarkId::new("blob", size), &blob, |b, data| {
            b.iter(|| black_box(read_value(black_box(data), blob_serial).unwrap()));
        });

        let text_serial = SerialType::text(size as u64);

        // Common case: all-ASCII text (UTF-8 validation fast path).
        let ascii = vec![b'a'; size];
        group.bench_with_input(BenchmarkId::new("text_ascii", size), &ascii, |b, data| {
            b.iter(|| black_box(read_value(black_box(data), text_serial).unwrap()));
        });

        // Multi-byte UTF-8 ('é' is two bytes), general validation path.
        let multibyte: Vec<u8> = "é".repeat(size / 2).into_bytes();
        assert_eq!(multibyte.len(), size / 2 * 2);
        let multibyte_serial = SerialType::text(multibyte.len() as u64);
        group.bench_with_input(
            BenchmarkId::new("text_multibyte", size),
            &multibyte,
            |b, data| {
                b.iter(|| black_box(read_value(black_box(data), multibyte_serial).unwrap()));
            },
        );

        // Invalid UTF-8: validation fails and the value is demoted to a blob.
        let invalid = vec![0xFFu8; size];
        group.bench_with_input(
            BenchmarkId::new("text_invalid", size),
            &invalid,
            |b, data| {
                b.iter(|| black_box(read_value(black_box(data), text_serial).unwrap()));
            },
        );
    }

    group.finish();
}

fn setup_db() -> (Arc<Database>, Arc<turso_core::Connection>) {
    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(MemoryIO::new());
    let db = Database::open_file_with_flags(
        io,
        ":memory:",
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
    )
    .unwrap();
    let conn = db.connect().unwrap();
    (db, conn)
}

fn execute(db: &Database, conn: &Arc<turso_core::Connection>, sql: &str) {
    let mut stmt = conn.prepare(sql).unwrap();
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {}
            StepResult::IO | StepResult::Yield => {
                db.io.step().unwrap();
            }
            StepResult::Done => break,
            StepResult::Interrupt | StepResult::Busy => unreachable!(),
        }
    }
}

fn run_to_completion(db: &Database, stmt: &mut turso_core::Statement) {
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

// End-to-end: full-table scan that decodes a TEXT column for every row,
// so per-row record decoding (including UTF-8 validation) dominates.
#[turso_macros::codspeed_criterion_benchmark]
fn bench_scan_text_rows(criterion: &mut Criterion) {
    const ROWS: usize = 10_000;

    let mut group = criterion.benchmark_group("scan_text_rows");

    let (db, conn) = setup_db();
    execute(&db, &conn, "CREATE TABLE t(val TEXT)");
    execute(&db, &conn, "BEGIN");
    for i in 0..ROWS {
        execute(
            &db,
            &conn,
            &format!(
                "INSERT INTO t VALUES ('{}')",
                format_args!("payload-{i:0>90}")
            ),
        );
    }
    execute(&db, &conn, "COMMIT");

    let mut stmt = conn.prepare("SELECT sum(length(val)) FROM t").unwrap();
    group.bench_function(BenchmarkId::new("sum_length", ROWS), |b| {
        b.iter(|| run_to_completion(&db, &mut stmt));
    });

    group.finish();
}

#[cfg(all(not(feature = "codspeed"), not(target_family = "windows")))]
criterion_group! {
    name = grp_read_value;
    config = Criterion::default().with_profiler(PProfProfiler::new(1000, Output::Flamegraph(None)));
    targets = bench_read_value_text
}
#[cfg(all(not(feature = "codspeed"), not(target_family = "windows")))]
criterion_group! {
    name = grp_scan_text;
    config = Criterion::default().with_profiler(PProfProfiler::new(1000, Output::Flamegraph(None)));
    targets = bench_scan_text_rows
}

#[cfg(all(not(feature = "codspeed"), target_family = "windows"))]
criterion_group!(grp_read_value, bench_read_value_text);
#[cfg(all(not(feature = "codspeed"), target_family = "windows"))]
criterion_group!(grp_scan_text, bench_scan_text_rows);

#[cfg(feature = "codspeed")]
criterion_group!(grp_read_value, bench_read_value_text);
#[cfg(feature = "codspeed")]
criterion_group!(grp_scan_text, bench_scan_text_rows);

criterion_main!(grp_read_value, grp_scan_text);
