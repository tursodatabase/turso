use rand_chacha::ChaCha8Rng;
use rand_chacha::rand_core::SeedableRng;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use turso_core::{
    Database, DatabaseOpts, IO, LimboError, OpenFlags, PageCodec, PageCodecHeaderInfo, PageCodecId,
    PageCodecLocation, SqliteDialect, Statement,
};
use turso_whopper::{
    IOFaultConfig, SimulatorIO, Whopper, WhopperOpts,
    properties::{IntegrityCheckProperty, Property},
    workloads::{
        BeginWorkload, CommitWorkload, CreateSimpleTableWorkload, IntegrityCheckWorkload,
        OverflowInsertWorkload, RollbackWorkload, SimpleInsertWorkload, SimpleSelectWorkload,
        WalCheckpointWorkload,
    },
};

fn run_to_done(stmt: &mut Statement, io: &SimulatorIO) {
    loop {
        match stmt.step().expect("step") {
            turso_core::StepResult::Done => return,
            turso_core::StepResult::IO => io.step().expect("io step"),
            _ => {}
        }
    }
}

fn run_to_error(stmt: &mut Statement, io: &SimulatorIO) -> LimboError {
    loop {
        match stmt.step() {
            Ok(turso_core::StepResult::Done) => {
                panic!("statement unexpectedly completed without a codec error")
            }
            Ok(turso_core::StepResult::IO) => io.step().expect("io step"),
            Ok(_) => {}
            Err(err) => return err,
        }
    }
}

#[derive(Debug)]
struct FailOnceCheckpointDecodeCodec {
    armed: AtomicBool,
}

impl FailOnceCheckpointDecodeCodec {
    fn arm(&self) {
        self.armed.store(true, Ordering::Release);
    }
}

impl PageCodec for FailOnceCheckpointDecodeCodec {
    fn codec_id(&self) -> PageCodecId {
        PageCodecId::new(*b"fail-once-codec-")
    }

    fn probe_header(
        &self,
        raw_page1_prefix: &[u8],
    ) -> turso_core::Result<Option<PageCodecHeaderInfo>> {
        if raw_page1_prefix.len() < 21 || raw_page1_prefix[..16] != *b"SQLite format 3\0" {
            return Ok(None);
        }
        let raw_page_size = u16::from_be_bytes([raw_page1_prefix[16], raw_page1_prefix[17]]);
        Ok(Some(PageCodecHeaderInfo {
            page_size: if raw_page_size == 1 {
                65_536
            } else {
                raw_page_size as usize
            },
            reserved_space: raw_page1_prefix[20],
        }))
    }

    fn required_reserved_bytes(&self) -> u8 {
        0
    }

    fn encode_page(
        &self,
        page: &[u8],
        output: &mut [u8],
        _page_idx: usize,
        _location: PageCodecLocation,
    ) -> turso_core::Result<()> {
        output.copy_from_slice(page);
        Ok(())
    }

    fn decode_page(
        &self,
        page: &[u8],
        output: &mut [u8],
        _page_idx: usize,
        location: PageCodecLocation,
    ) -> turso_core::Result<()> {
        if location == PageCodecLocation::Wal && self.armed.swap(false, Ordering::AcqRel) {
            return Err(LimboError::InternalError(
                "simulated WAL codec decode failure".into(),
            ));
        }
        output.copy_from_slice(page);
        Ok(())
    }
}

struct AbortRecorder {
    aborts: Arc<AtomicUsize>,
}

impl Property for AbortRecorder {
    fn finish_op(
        &mut self,
        _step: usize,
        _fiber_id: usize,
        _txn_id: Option<u64>,
        _start_exec_id: u64,
        _end_exec_id: u64,
        _op: &turso_whopper::operations::Operation,
        _result: &turso_whopper::operations::OpResult,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn abort_fiber(&mut self, _fiber_id: usize, _txn_id: Option<u64>) -> anyhow::Result<()> {
        self.aborts.fetch_add(1, Ordering::Relaxed);
        Ok(())
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

#[test]
fn test_page_codec_simulator_checkpoint_and_reopen() {
    let opts = WhopperOpts::fast()
        .with_seed(0xface_feed)
        .with_max_connections(3)
        .with_max_steps(2_000)
        .with_enable_page_codec(true)
        .with_workloads(vec![
            (
                5,
                Box::new(WalCheckpointWorkload {
                    allow_passive: true,
                }),
            ),
            (10, Box::new(IntegrityCheckWorkload)),
            (10, Box::new(CreateSimpleTableWorkload)),
            (25, Box::new(SimpleSelectWorkload)),
            (25, Box::new(SimpleInsertWorkload)),
            (30, Box::new(BeginWorkload)),
            (15, Box::new(CommitWorkload)),
            (10, Box::new(RollbackWorkload)),
        ])
        .with_properties(vec![Box::new(IntegrityCheckProperty)]);
    let mut whopper = Whopper::new(opts).expect("create page codec simulator");

    while !whopper.is_done() {
        whopper.step().expect("simulator step");
        if whopper.current_step % 127 == 0 {
            whopper.reopen().expect("reopen codec database");
        }
    }

    whopper
        .finalize_properties()
        .expect("finalize page codec simulator");
}

/// Exercise the page codec across multi-page overflow chains, WAL checkpoints,
/// and repeated database reopen under concurrent transactions. The simulator
/// codec is keyed by page id and transform location, so a mismatched page index
/// or `PageCodecLocation` threaded through any overflow, WAL, or checkpoint path
/// surfaces as a corrupted page rather than silent success.
#[test]
fn test_page_codec_simulator_overflow_checkpoint_and_reopen() {
    let opts = WhopperOpts::fast()
        .with_seed(0xc0de_c0ffee)
        .with_max_connections(3)
        .with_max_steps(4_000)
        .with_enable_page_codec(true)
        .with_workloads(vec![
            (
                5,
                Box::new(WalCheckpointWorkload {
                    allow_passive: false,
                }),
            ),
            (10, Box::new(IntegrityCheckWorkload)),
            (10, Box::new(CreateSimpleTableWorkload)),
            (35, Box::new(OverflowInsertWorkload)),
            (20, Box::new(SimpleSelectWorkload)),
            (15, Box::new(SimpleInsertWorkload)),
            (30, Box::new(BeginWorkload)),
            (15, Box::new(CommitWorkload)),
            (10, Box::new(RollbackWorkload)),
        ])
        .with_properties(vec![Box::new(IntegrityCheckProperty)]);
    let mut whopper = Whopper::new(opts).expect("create overflow page codec simulator");

    while !whopper.is_done() {
        whopper.step().expect("simulator step");
        if whopper.current_step % 97 == 0 {
            whopper.reopen().expect("reopen overflow codec database");
        }
    }

    whopper
        .finalize_properties()
        .expect("finalize overflow page codec simulator");
}

#[test]
fn test_page_codec_simulator_checkpoint_decode_failure_recovers() {
    let io_rng = ChaCha8Rng::seed_from_u64(0x5eed);
    let io = Arc::new(SimulatorIO::new(
        false,
        io_rng,
        IOFaultConfig {
            cosmic_ray_probability: 0.0,
        },
    ));
    let db_path = format!("test-codec-checkpoint-failure-{}.db", std::process::id());
    let codec = Arc::new(FailOnceCheckpointDecodeCodec {
        armed: AtomicBool::new(false),
    });
    let db = Database::open_file_with_flags_and_page_codec(
        io.clone(),
        &db_path,
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
        None,
        codec.clone(),
        Arc::new(SqliteDialect),
    )
    .expect("open database");
    let conn = db
        .connect_with_page_codec(codec.clone())
        .expect("open connection");

    conn.execute("PRAGMA journal_mode = 'wal'")
        .expect("enable WAL");
    conn.execute(
        "CREATE TABLE t(id INTEGER PRIMARY KEY, value TEXT);
         INSERT INTO t(value) VALUES ('checkpoint failure recovery');",
    )
    .expect("create committed WAL content");
    drop(conn);
    drop(db);
    turso_core::clear_database_registry();

    let db = Database::open_file_with_flags_and_page_codec(
        io.clone(),
        &db_path,
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
        None,
        codec.clone(),
        Arc::new(SqliteDialect),
    )
    .expect("reopen with live WAL");
    let conn = db
        .connect_with_page_codec(codec.clone())
        .expect("reconnect with live WAL");

    codec.arm();
    let mut checkpoint = conn
        .prepare("PRAGMA wal_checkpoint(FULL)")
        .expect("prepare checkpoint");
    let err = run_to_error(&mut checkpoint, &io);
    assert!(
        err.to_string()
            .contains("Checkpoint failed: Page codec failed for page="),
        "unexpected checkpoint error: {err}"
    );
    drop(checkpoint);

    let mut retry = conn
        .prepare("PRAGMA wal_checkpoint(FULL)")
        .expect("prepare checkpoint retry");
    run_to_done(&mut retry, &io);
    drop(retry);
    drop(conn);
    drop(db);
    turso_core::clear_database_registry();

    let reopened = Database::open_file_with_flags_and_page_codec(
        io.clone(),
        &db_path,
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
        None,
        codec.clone(),
        Arc::new(SqliteDialect),
    )
    .expect("reopen database after failed checkpoint");
    let conn = reopened
        .connect_with_page_codec(codec)
        .expect("reconnect after failed checkpoint");
    let mut stmt = conn.prepare("SELECT value FROM t").expect("prepare read");
    let mut values = Vec::new();
    loop {
        match stmt.step().expect("step read after failed checkpoint") {
            turso_core::StepResult::Row => {
                values.push(stmt.row().expect("read row").get::<String>(0).unwrap());
            }
            turso_core::StepResult::Done => break,
            turso_core::StepResult::IO => io.step().expect("io step"),
            _ => {}
        }
    }
    assert_eq!(values, ["checkpoint failure recovery"]);
}

#[test]
fn test_page_codec_simulator_crash_reopens_checkpoint_io() {
    let aborts = Arc::new(AtomicUsize::new(0));
    let opts = WhopperOpts::fast()
        .with_seed(0xc0de_cafe)
        .with_max_connections(1)
        .with_max_steps(32)
        .with_enable_page_codec(true)
        .with_workloads(vec![(
            1,
            Box::new(WalCheckpointWorkload {
                allow_passive: false,
            }),
        )])
        .with_properties(vec![Box::new(AbortRecorder {
            aborts: Arc::clone(&aborts),
        })]);
    let mut whopper = Whopper::new(opts).expect("create page codec simulator");

    for _ in 0..8 {
        whopper
            .step_without_completing_io()
            .expect("step checkpoint before crash");
        if whopper.pending_io_count() > 0 {
            break;
        }
    }
    assert!(
        whopper.pending_io_count() > 0,
        "checkpoint must have submitted codec-backed I/O before the crash"
    );

    whopper
        .crash_reopen()
        .expect("recover after interrupted checkpoint");
    assert_eq!(aborts.load(Ordering::Relaxed), 1);
    whopper
        .assert_integrity_check()
        .expect("verify integrity after checkpoint crash");
}
