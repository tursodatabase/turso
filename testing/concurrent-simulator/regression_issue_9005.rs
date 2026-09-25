#![cfg(all(any(unix, target_os = "windows"), target_pointer_width = "64"))]

use turso_whopper::chaotic_btree::BtreeRebalanceProfile;
use turso_whopper::chaotic_elle::ChaoticWorkloadProfile;
use turso_whopper::multiprocess::{MultiprocessOpts, MultiprocessWhopper};
use turso_whopper::properties::{IntegrityCheckProperty, Property};
use turso_whopper::workloads::{IntegrityCheckWorkload, WalCheckpointWorkload, Workload};

/// Regression test for https://github.com/tursodatabase/turso/issues/9005
///
/// With three worker processes that get killed and respawned at random, a
/// write on step 2443 panicked with "shared WAL frame ids must increase
/// monotonically: new_frame_id=157, previous_frame_id=344, slot=344,
/// shared_max_frame=156". Every respawned worker before the panic reported
/// reopened_max_frame=156, and the last few respawns reported
/// loaded_from_disk_scan=true.
///
/// Same as running:
///   SEED=2000027 turso_whopper --mode btree-rebalance --max-steps 3000 \
///     --multiprocess --processes 3 --connections-per-process 2 \
///     --kill-probability 0.01
#[test]
fn issue_9005_respawned_worker_does_not_panic_on_shared_wal_frame_order() {
    unsafe {
        std::env::set_var(
            "TURSO_WHOPPER_WORKER_EXE",
            env!("CARGO_BIN_EXE_turso_whopper"),
        );
    }

    let workloads: Vec<(u32, Box<dyn Workload>)> = vec![
        (20, Box::new(IntegrityCheckWorkload)),
        (
            5,
            Box::new(WalCheckpointWorkload {
                allow_passive: false,
            }),
        ),
    ];
    let properties: Vec<Box<dyn Property>> = vec![Box::new(IntegrityCheckProperty)];
    let chaotic_profiles: Vec<(f64, &'static str, Box<dyn ChaoticWorkloadProfile>)> = vec![(
        1.0,
        "btree-rebalance",
        Box::new(BtreeRebalanceProfile::default()),
    )];

    let mut whopper = MultiprocessWhopper::new(MultiprocessOpts {
        seed: Some(2000027),
        enable_mvcc: false,
        process_count: 3,
        connections_per_process: 2,
        max_steps: 3000,
        elle_tables: vec![],
        workloads,
        properties,
        chaotic_profiles,
        kill_probability: 0.01,
        restart_probability: 0.0,
        history_output: None,
        keep_files: false,
    })
    .expect("create multiprocess whopper");

    while !whopper.is_done() {
        whopper
            .step()
            .expect("multiprocess whopper step must not fail");
    }
    whopper.finalize().expect("finalize multiprocess whopper");
}
