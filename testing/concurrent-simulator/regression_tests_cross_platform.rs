use rand_chacha::ChaCha8Rng;
use rand_chacha::rand_core::SeedableRng;
use std::sync::Arc;
use turso_core::{Database, DatabaseOpts, IO, OpenFlags, SqliteDialect, Statement};
use turso_whopper::{IOFaultConfig, SimulatorIO};

fn run_to_done(stmt: &mut Statement, io: &SimulatorIO) {
    loop {
        match stmt.step().expect("step") {
            turso_core::StepResult::Done => return,
            turso_core::StepResult::IO => io.step().expect("io step"),
            _ => {}
        }
    }
}

/// Regression test for MVCC concurrent commit yield-spin deadlock.
///
/// Under round-robin cooperative scheduling, when two BEGIN CONCURRENT
/// transactions commit simultaneously, the VDBE must yield (return
/// StepResult::IO) when pager_commit_lock is held by the other connection.
///
/// Before the fix in core/vdbe/mod.rs, Completion::new_yield() had
/// finished()==true, so the VDBE inner loop retried without ever returning
/// and both commits could starve.
#[test]
fn test_concurrent_commit_no_yield_spin() {
    let io_rng = ChaCha8Rng::seed_from_u64(42);
    let fault_config = IOFaultConfig {
        cosmic_ray_probability: 0.0,
    };
    let io = Arc::new(SimulatorIO::new(false, io_rng, fault_config));

    let db_path = format!("test-yield-spin-{}.db", std::process::id());
    let db = Database::open_file_with_flags(
        io.clone(),
        &db_path,
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )
    .expect("open db");

    let setup = db.connect().expect("setup conn");
    setup
        .execute("PRAGMA journal_mode = 'mvcc'")
        .expect("enable mvcc");
    setup
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .expect("create table");
    setup.close().expect("close setup");

    let conn1 = db.connect().expect("conn1");
    let conn2 = db.connect().expect("conn2");

    let mut stmt = conn1.prepare("BEGIN CONCURRENT").expect("prepare");
    run_to_done(&mut stmt, &io);
    let mut stmt = conn2.prepare("BEGIN CONCURRENT").expect("prepare");
    run_to_done(&mut stmt, &io);

    let mut stmt = conn1
        .prepare("INSERT INTO t VALUES (1, 'a')")
        .expect("prepare");
    run_to_done(&mut stmt, &io);
    let mut stmt = conn2
        .prepare("INSERT INTO t VALUES (2, 'b')")
        .expect("prepare");
    run_to_done(&mut stmt, &io);

    let mut commit1 = conn1.prepare("COMMIT").expect("prepare commit1");
    let mut commit2 = conn2.prepare("COMMIT").expect("prepare commit2");

    let mut done1 = false;
    let mut done2 = false;
    let max_steps = 10_000;

    for step in 0..max_steps {
        if done1 && done2 {
            break;
        }

        if !done1 {
            match commit1.step().expect("commit1 step") {
                turso_core::StepResult::Done => done1 = true,
                turso_core::StepResult::IO => {}
                _ => {}
            }
        }
        io.step().expect("io step");

        if !done2 {
            match commit2.step().expect("commit2 step") {
                turso_core::StepResult::Done => done2 = true,
                turso_core::StepResult::IO => {}
                _ => {}
            }
        }
        io.step().expect("io step");

        assert!(
            step < max_steps - 1,
            "concurrent commits did not complete within {max_steps} steps: done1={done1}, done2={done2}"
        );
    }

    assert!(done1, "commit1 should have completed");
    assert!(done2, "commit2 should have completed");

    let verify = db.connect().expect("verify conn");
    let mut stmt = verify.prepare("SELECT COUNT(*) FROM t").expect("prepare");
    let mut count = 0i64;
    loop {
        match stmt.step().expect("step") {
            turso_core::StepResult::Row => {
                if let Some(row) = stmt.row() {
                    count = row
                        .get_values()
                        .next()
                        .expect("count value")
                        .as_int()
                        .expect("count int");
                }
            }
            turso_core::StepResult::Done => break,
            turso_core::StepResult::IO => io.step().expect("io"),
            _ => {}
        }
    }
    assert_eq!(count, 2, "both inserts should be visible");
}

/// End-to-end coverage for checkpoints racing suspended statements: with the
/// probe probability at 1.0, every simulation step that leaves a statement
/// suspended mid-execution fires a same-connection checkpoint attempt
/// (PRAGMA wal_checkpoint or the Connection::checkpoint API) and the run
/// fails unless the engine rejects it with StatementsInProgress/TableLocked.
/// On unguarded builds the checkpoint runs and the suspended statement
/// panics on resume, loses its write, or silently returns wrong rows.
#[test]
fn test_checkpoint_probe_rejects_checkpoints_while_statements_are_suspended() {
    use turso_whopper::properties::{IntegrityCheckProperty, Property};
    use turso_whopper::workloads::{
        BeginWorkload, CommitWorkload, InsertWorkload, IntegrityCheckWorkload, RollbackWorkload,
        SelectWorkload, UpdateWorkload, WalCheckpointWorkload, Workload,
    };
    use turso_whopper::{Whopper, WhopperOpts};

    let workloads: Vec<(u32, Box<dyn Workload>)> = vec![
        (30, Box::new(InsertWorkload)),
        (20, Box::new(UpdateWorkload)),
        (15, Box::new(SelectWorkload)),
        (10, Box::new(BeginWorkload)),
        (8, Box::new(CommitWorkload)),
        (4, Box::new(RollbackWorkload)),
        (5, Box::new(IntegrityCheckWorkload)),
        (
            5,
            Box::new(WalCheckpointWorkload {
                allow_passive: true,
            }),
        ),
    ];
    let properties: Vec<Box<dyn Property>> = vec![Box::new(IntegrityCheckProperty)];

    let opts = WhopperOpts {
        seed: Some(0xC0FFEE),
        max_connections: 3,
        max_steps: 5_000,
        workloads,
        properties,
        checkpoint_probe_probability: 1.0,
        ..WhopperOpts::default()
    };
    let mut whopper = Whopper::new(opts).expect("create whopper");
    whopper
        .run()
        .expect("simulation must complete without contract violations");
    assert!(
        whopper.stats.checkpoint_probes > 0,
        "the run never left a statement suspended, so the probe tested nothing"
    );
}

/// While a COMMIT was suspended inside its post-commit auto-checkpoint, a
/// failed statement on the same connection (a rejected checkpoint probe) was
/// dropped. Its teardown decided it was the last active statement — a failed
/// statement leaves the active count on its step error, so the count held
/// only the suspended sibling — and "rolled back" the connection: it reset
/// the pager's in-flight commit and checkpoint state and closed the shared
/// attached write transaction. The resumed COMMIT then started over and
/// panicked releasing a WAL write lock it no longer held.
///
/// The seed replays a CI failure of the btree-rebalance whopper profile; the
/// panic fired around step 14700.
#[test]
fn test_dropped_failed_statement_keeps_suspended_sibling_commit_intact() {
    use rand::Rng;
    use turso_whopper::chaotic_btree::BtreeRebalanceProfile;
    use turso_whopper::chaotic_elle::ChaoticWorkloadProfile;
    use turso_whopper::properties::{IntegrityCheckProperty, Property};
    use turso_whopper::workloads::{IntegrityCheckWorkload, WalCheckpointWorkload, Workload};
    use turso_whopper::{Whopper, WhopperOpts};

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
    let chaotic: Vec<(f64, &'static str, Box<dyn ChaoticWorkloadProfile>)> = vec![(
        1.0,
        "btree-rebalance",
        Box::new(BtreeRebalanceProfile::default()),
    )];

    let opts = WhopperOpts::btree_rebalance()
        .with_seed(16427142037514425436)
        .with_max_steps(20_000)
        .with_max_connections(4)
        .with_workloads(workloads)
        .with_properties(properties)
        .with_chaotic_profiles(chaotic)
        .with_allocation_fault_probability(0.0);
    let mut whopper = Whopper::new(opts).expect("create whopper");
    // Mirror main.rs's run_inprocess loop instead of Whopper::run: the CLI
    // burns one rng draw per step on its reopen check, and this seed replays
    // a CLI failure, so the draw must stay in the stream for the schedule to
    // match.
    while !whopper.is_done() {
        let _ = whopper.rng.random_bool(0.0);
        match whopper.step() {
            Ok(_) => {}
            Err(e) => panic!("statement teardown must not clobber a suspended commit: {e}"),
        }
    }
}
