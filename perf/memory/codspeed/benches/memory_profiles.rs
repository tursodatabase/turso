//! CodSpeed-instrumented benchmarks over the memory-benchmark workload
//! profiles. Each benchmark runs one (journal mode, workload, size)
//! combination against a fresh database so CodSpeed's memory instrument can
//! track the allocations of the whole workload.
//!
//! Every (mode, workload) pair runs at 1x/2x/4x scale (more iterations, same
//! batch size) and the benchmark name carries the total operation count, e.g.
//! `mvcc/insert-heavy/2000`. Comparing the sizes shows how memory scales with
//! workload volume: total allocated bytes should grow roughly linearly, while
//! peak memory staying flat-ish indicates memory is being reclaimed rather
//! than accumulated.
//!
//! Workload sizes are deliberately much smaller than the CLI defaults: under
//! CodSpeed instrumentation every run executes slower than native, and each
//! workload's size series runs in its own CI shard.

#[cfg(not(feature = "codspeed"))]
use criterion::{Criterion, criterion_group, criterion_main};

#[cfg(feature = "codspeed")]
use codspeed_criterion_compat::{Criterion, criterion_group, criterion_main};

use std::time::Duration;

use memory_benchmark::workload::{
    JournalMode, WorkloadConfig, WorkloadProfile, clean_db_files, run_workload,
};

const MODES: [JournalMode; 2] = [JournalMode::Wal, JournalMode::Mvcc];

const WORKLOADS: [WorkloadProfile; 6] = [
    WorkloadProfile::InsertHeavy,
    WorkloadProfile::ReadHeavy,
    WorkloadProfile::Mixed,
    WorkloadProfile::ScanHeavy,
    WorkloadProfile::SeriesBlob,
    WorkloadProfile::UpdateChurn,
];

/// Workload scale series: the base iteration count is multiplied by each of
/// these, holding the batch (transaction) size constant, so the benchmarks
/// expose how memory grows with the amount of work done.
const SIZE_MULTIPLIERS: [usize; 3] = [1, 2, 4];

/// Scale of the extra checkpointed variant of each workload.
const CHECKPOINT_SIZE_MULTIPLIER: usize = 8;

/// MVCC logical-log auto-checkpoint threshold for the checkpointed variant.
/// The default is ~4 MB, far more than these workloads write, so without
/// lowering it the MVCC benchmarks would never checkpoint. 16 KiB is below
/// what every workload writes (the lightest is read-heavy's 10% inserts;
/// seeded workloads cross it during setup alone), so automatic checkpoints
/// are guaranteed to fire during the run.
const MVCC_CHECKPOINT_THRESHOLD_BYTES: i64 = 16 * 1024;

/// Per-workload base sizing as (iterations, batch_size). Scans are quadratic
/// in practice (each query walks the whole 10k-row seed table), so scan-heavy
/// gets far fewer operations.
fn base_workload_size(workload: WorkloadProfile) -> (usize, usize) {
    match workload {
        WorkloadProfile::ScanHeavy => (5, 2),
        _ => (10, 50),
    }
}

fn bench_memory_profiles(c: &mut Criterion) {
    let work_dir = std::env::temp_dir().join(format!("turso-memory-bench-{}", std::process::id()));
    std::fs::create_dir_all(&work_dir).expect("failed to create bench work dir");

    for mode in MODES {
        for workload in WORKLOADS {
            for multiplier in SIZE_MULTIPLIERS {
                bench_workload(c, &work_dir, mode, workload, multiplier, false);
            }
            // Bigger checkpointed variant: low MVCC auto-checkpoint threshold
            // plus a final explicit `PRAGMA wal_checkpoint(TRUNCATE)` (the
            // only guaranteed checkpoint in WAL mode, whose 1000-frame
            // auto-checkpoint threshold is not configurable), so checkpoint
            // memory behavior is part of the measurement.
            bench_workload(
                c,
                &work_dir,
                mode,
                workload,
                CHECKPOINT_SIZE_MULTIPLIER,
                true,
            );
        }
    }
}

fn bench_workload(
    c: &mut Criterion,
    work_dir: &std::path::Path,
    mode: JournalMode,
    workload: WorkloadProfile,
    multiplier: usize,
    checkpoint: bool,
) {
    let (base_iterations, batch_size) = base_workload_size(workload);
    let iterations = base_iterations * multiplier;
    let total_ops = iterations * batch_size;
    let suffix = if checkpoint { "-checkpoint" } else { "" };
    let cfg = WorkloadConfig {
        mode,
        workload,
        iterations,
        batch_size,
        connections: 1,
        timeout: Duration::from_millis(30_000),
        cache_size: None,
        checkpoint,
        mvcc_checkpoint_threshold: (checkpoint && matches!(mode, JournalMode::Mvcc))
            .then_some(MVCC_CHECKPOINT_THRESHOLD_BYTES),
        mvcc_gc_threshold: (matches!(mode, JournalMode::Mvcc)).then_some(16384i64),
    };
    let db_path = work_dir
        .join(format!("{mode}_{workload}_{total_ops}{suffix}.db"))
        .to_string_lossy()
        .into_owned();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .build()
        .expect("failed to build tokio runtime");

    c.bench_function(&format!("{mode}/{workload}/{total_ops}{suffix}"), |b| {
        b.iter(|| {
            clean_db_files(&db_path);
            rt.block_on(run_workload(&db_path, &cfg, &mut ()))
                .expect("workload failed");
        });
    });

    clean_db_files(&db_path);
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = bench_memory_profiles
}
criterion_main!(benches);
