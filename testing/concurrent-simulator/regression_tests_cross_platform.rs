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

/// Regression test for the production panic
/// "transaction should exist in txs map"
/// (https://github.com/tursodatabase/turso-server/issues/2972).
///
/// A client that holds a rows handle open (a read statement paused
/// mid-rows) can run other statements on the same connection, including
/// COMMIT. COMMIT removes the MVCC transaction from the txs map while the
/// paused statement's cursors still reference it, and resuming the
/// statement then panics inside turso_core instead of returning an error.
///
/// This drives the whopper simulator with a fixed operation script:
/// BEGIN CONCURRENT, pause a SELECT after its first row, COMMIT, resume the
/// SELECT. The run must complete without panicking; finishing the scan or
/// failing the resumed statement with a clean error are both acceptable.
/// While the bug exists this test FAILS with the panic.
#[test]
fn paused_read_across_commit_must_not_panic() {
    use std::sync::Mutex;
    use turso_whopper::workloads::{Workload, WorkloadContext};
    use turso_whopper::{Operation, TxMode, Whopper, WhopperOpts};

    /// Emits a fixed sequence of operations, one per completed operation.
    struct ScriptedWorkload {
        ops: Vec<Operation>,
        next: Mutex<usize>,
    }

    impl Workload for ScriptedWorkload {
        fn generate(
            &self,
            _ctx: &WorkloadContext,
            _rng: &mut rand_chacha::ChaCha8Rng,
        ) -> Option<Operation> {
            let mut next = self.next.lock().unwrap();
            let op = self.ops.get(*next).cloned();
            if op.is_some() {
                *next += 1;
            }
            op
        }
    }

    let script = ScriptedWorkload {
        ops: vec![
            Operation::Execute {
                sql: "CREATE TABLE kv (key TEXT PRIMARY KEY, value BLOB)".to_string(),
            },
            Operation::Execute {
                sql: "INSERT INTO kv VALUES ('k1', zeroblob(8)), ('k2', zeroblob(8)), \
                      ('k3', zeroblob(8))"
                    .to_string(),
            },
            Operation::Begin {
                mode: TxMode::Concurrent,
            },
            Operation::PausedRead {
                sql: "SELECT key, length(value) FROM kv ORDER BY key".to_string(),
            },
            Operation::Commit,
            Operation::ResumePausedRead,
        ],
        next: Mutex::new(0),
    };

    let opts = WhopperOpts {
        seed: Some(7),
        max_connections: 1,
        max_steps: 5_000,
        enable_mvcc: true,
        workloads: vec![(1, Box::new(script))],
        ..Default::default()
    };
    let mut whopper = Whopper::new(opts).expect("create whopper");
    whopper.run().expect("run must complete without panicking");
}
