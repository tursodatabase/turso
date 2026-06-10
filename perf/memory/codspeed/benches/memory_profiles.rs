//! CodSpeed-instrumented benchmarks over the memory-benchmark workload
//! profiles. Each benchmark runs one (journal mode, workload) combination
//! against a fresh database so CodSpeed's memory instrument can track the
//! allocations of the whole workload.
//!
//! Workload sizes are deliberately much smaller than the CLI defaults: under
//! CodSpeed instrumentation every run executes roughly 30-50x slower than
//! native, and each benchmark in the suite runs in its own CI shard.

#[cfg(not(feature = "codspeed"))]
use criterion::{Criterion, criterion_group, criterion_main};

#[cfg(feature = "codspeed")]
use codspeed_criterion_compat::{Criterion, criterion_group, criterion_main};

use std::time::Duration;

use memory_benchmark::workload::{
    JournalMode, WorkloadConfig, WorkloadProfile, clean_db_files, run_workload,
};

const MODES: [JournalMode; 2] = [JournalMode::Wal, JournalMode::Mvcc];

const WORKLOADS: [WorkloadProfile; 5] = [
    WorkloadProfile::InsertHeavy,
    WorkloadProfile::ReadHeavy,
    WorkloadProfile::Mixed,
    WorkloadProfile::ScanHeavy,
    WorkloadProfile::SeriesBlob,
];

/// Per-workload sizing. Scans are quadratic in practice (each query walks the
/// whole 10k-row seed table), so scan-heavy gets far fewer operations.
fn workload_size(workload: WorkloadProfile) -> (usize, usize) {
    match workload {
        WorkloadProfile::ScanHeavy => (5, 4),
        _ => (20, 50),
    }
}

fn bench_memory_profiles(c: &mut Criterion) {
    let work_dir = std::env::temp_dir().join(format!("turso-memory-bench-{}", std::process::id()));
    std::fs::create_dir_all(&work_dir).expect("failed to create bench work dir");

    for mode in MODES {
        for workload in WORKLOADS {
            let (iterations, batch_size) = workload_size(workload);
            let cfg = WorkloadConfig {
                mode,
                workload,
                iterations,
                batch_size,
                connections: 1,
                timeout: Duration::from_millis(30_000),
                cache_size: None,
                checkpoint: false,
            };
            let db_path = work_dir
                .join(format!("{mode}_{workload}.db"))
                .to_string_lossy()
                .into_owned();

            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .build()
                .expect("failed to build tokio runtime");

            c.bench_function(&format!("{mode}/{workload}"), |b| {
                b.iter(|| {
                    clean_db_files(&db_path);
                    rt.block_on(run_workload(&db_path, &cfg, &mut ()))
                        .expect("workload failed");
                });
            });

            clean_db_files(&db_path);
        }
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = bench_memory_profiles
}
criterion_main!(benches);
