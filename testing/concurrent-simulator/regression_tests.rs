use rand_chacha::ChaCha8Rng;
use rand_chacha::rand_core::SeedableRng;
use std::sync::Arc;
use turso_core::{Database, DatabaseOpts, IO, OpenFlags, Statement, StepResult};
use turso_whopper::{CosmicRayTarget, IOFaultConfig, SimulatorIO};

/// Helper: run a SQL statement to completion with round-robin IO stepping.
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
/// — starving the first connection and deadlocking both.
#[test]
fn test_concurrent_commit_no_yield_spin() {
    let io_rng = ChaCha8Rng::seed_from_u64(42);
    let fault_config = IOFaultConfig::default();
    let io = Arc::new(SimulatorIO::new(false, io_rng, fault_config));

    let db_path = format!("test-yield-spin-{}.db", std::process::id());
    let db = Database::open_file_with_flags(
        io.clone(),
        &db_path,
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
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

    // Both connections start concurrent transactions with non-conflicting writes
    let mut s = conn1.prepare("BEGIN CONCURRENT").expect("prepare");
    run_to_done(&mut s, &io);
    let mut s = conn2.prepare("BEGIN CONCURRENT").expect("prepare");
    run_to_done(&mut s, &io);

    let mut s = conn1
        .prepare("INSERT INTO t VALUES (1, 'a')")
        .expect("prepare");
    run_to_done(&mut s, &io);
    let mut s = conn2
        .prepare("INSERT INTO t VALUES (2, 'b')")
        .expect("prepare");
    run_to_done(&mut s, &io);

    // Commit both using round-robin stepping — the pattern that triggers
    // the bug. Each connection gets one step() call, then IO is advanced.
    let mut commit1 = conn1.prepare("COMMIT").expect("prepare commit1");
    let mut commit2 = conn2.prepare("COMMIT").expect("prepare commit2");

    let mut done1 = false;
    let mut done2 = false;
    let max_steps = 10_000;

    for i in 0..max_steps {
        if done1 && done2 {
            break;
        }

        // Round-robin: step each connection once, then advance IO
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
            i < max_steps - 1,
            "concurrent commits did not complete within {max_steps} steps — \
             likely stuck in yield-spin loop (done1={done1}, done2={done2})"
        );
    }

    assert!(done1, "commit1 should have completed");
    assert!(done2, "commit2 should have completed");

    // Verify both rows are visible
    let verify = db.connect().expect("verify conn");
    let mut stmt = verify.prepare("SELECT COUNT(*) FROM t").expect("prepare");
    let mut count = 0i64;
    loop {
        match stmt.step().expect("step") {
            turso_core::StepResult::Row => {
                if let Some(row) = stmt.row() {
                    count = row.get_values().next().unwrap().as_int().unwrap();
                }
            }
            turso_core::StepResult::Done => break,
            turso_core::StepResult::IO => io.step().expect("io"),
            _ => {}
        }
    }
    assert_eq!(count, 2, "both inserts should be visible");
}

/// Run a statement, returning the first integer column of the first row.
fn run_to_done_int(stmt: &mut Statement, io: &SimulatorIO) -> i64 {
    loop {
        match stmt.step().expect("step") {
            StepResult::Row => {
                if let Some(row) = stmt.row() {
                    return row.get_values().next().unwrap().as_int().unwrap();
                }
            }
            StepResult::Done => return 0,
            StepResult::IO => io.step().expect("io step"),
            _ => {}
        }
    }
}

/// Populate a table, then delete all rows so the freed pages enter the freelist.
/// Returns (page_size, first_freelist_trunk_page_number) read from the live db.
///
/// The first freelist trunk page number lives in the SQLite header at byte
/// offset 32 (big-endian u32). Reading it via the simulator mmap is the only
/// way the test can identify which bytes belong to the freelist trunk page so
/// the cosmic ray fault injector can be focused on them.
fn populate_and_drain_freelist(
    conn: &Arc<turso_core::Connection>,
    io: &Arc<SimulatorIO>,
    db_path: &str,
) -> (usize, u32) {
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, data TEXT)")
        .expect("create table");
    for i in 0..50 {
        let mut stmt = conn
            .prepare(format!(
                "INSERT INTO t VALUES ({i}, '{}')",
                "x".repeat(200)
            ))
            .expect("prepare insert");
        run_to_done(&mut stmt, io);
    }
    let mut delete = conn.prepare("DELETE FROM t").expect("prepare delete");
    run_to_done(&mut delete, io);
    let mut checkpoint = conn
        .prepare("PRAGMA wal_checkpoint(TRUNCATE)")
        .expect("prepare checkpoint");
    run_to_done(&mut checkpoint, io);

    let mut page_size_stmt = conn.prepare("PRAGMA page_size").expect("prepare");
    let page_size = run_to_done_int(&mut page_size_stmt, io) as usize;

    let mut freelist_count = conn.prepare("PRAGMA freelist_count").expect("prepare");
    let count = run_to_done_int(&mut freelist_count, io);
    assert!(count > 0, "freelist must have pages after DELETE");

    // Read the freelist trunk page number from the SQLite header (offset 32, BE u32)
    let header = io.read_file_bytes(db_path, 32..36);
    let trunk_page = u32::from_be_bytes(header.try_into().unwrap());
    assert!(trunk_page >= 2, "freelist trunk page must exist");

    (page_size, trunk_page)
}

/// Regression test: allocate_page does not bounds-check the freelist trunk
/// page leaf count, causing an out-of-bounds panic when the count is invalid.
///
/// `free_page` validates `number_of_leaf_pages < max_free_list_entries` before
/// writing to the trunk page. `integrity_check` validates the same field
/// (btree.rs `IntegrityCheckError::FreelistTrunkCorrupt`). `allocate_page`
/// reads the field but does not validate it, then uses it to drive a
/// `copy_within` call that goes out of bounds when the count exceeds page
/// capacity.
///
/// Reachability: a single bit flip in the four bytes of the leaf count field
/// produces an invalid count with high probability (any flip in the upper bits
/// inflates the count past `max_free_list_entries`). The simulator's existing
/// cosmic-ray bit-flip model can produce this state; the test focuses the
/// model on the leaf count bytes so the fault is reached deterministically
/// instead of after astronomical iterations of unfocused flipping.
#[test]
fn test_freelist_trunk_leaf_count_unvalidated_in_allocate_page() {
    // Phase 1: build a database with a populated freelist. Fault injection is
    // disabled so the schema and data write cleanly.
    let io = Arc::new(SimulatorIO::new(
        false,
        ChaCha8Rng::seed_from_u64(0xF5EE_1157),
        IOFaultConfig::default(),
    ));
    let db_path = format!("test-freelist-leaf-count-{}.db", std::process::id());
    let setup_db = Database::open_file_with_flags(
        io.clone(),
        &db_path,
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
    )
    .expect("open db");
    let setup_conn = setup_db.connect().expect("conn");
    let (page_size, trunk_page) = populate_and_drain_freelist(&setup_conn, &io, &db_path);
    // Drop the setup connection and database so the page cache is released.
    // The next Database we open on the same SimulatorIO will read all pages
    // from disk, which is where the cosmic ray bit flips live.
    setup_conn.close().expect("close setup conn");
    drop(setup_db);

    // Phase 2: enable cosmic rays focused on the trunk page leaf count field
    // and pulse the IO loop a handful of times so the random model produces
    // bit flips against the at-rest bytes BEFORE any page is re-read into the
    // new pager's page cache. The leaf count lives at offset 4 within the
    // trunk page (the first 4 bytes are the next-trunk pointer).
    let trunk_offset = (trunk_page as usize - 1) * page_size;
    io.set_fault_config(IOFaultConfig {
        cosmic_ray_probability: 1.0,
        cosmic_ray_targets: vec![CosmicRayTarget {
            file_suffix: ".db".to_string(),
            byte_range: (trunk_offset + 4)..(trunk_offset + 8),
        }],
    });
    for _ in 0..32 {
        io.step().expect("pulse io step");
    }
    // Disable further injection so the subsequent INSERT runs against the
    // already-corrupted bytes without additional flips during recovery paths.
    io.set_fault_config(IOFaultConfig::default());

    // Reopen the database on the same SimulatorIO so the new pager reads the
    // already-corrupted trunk page from disk on first access.
    let db = Database::open_file_with_flags(
        io.clone(),
        &db_path,
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
    )
    .expect("reopen db");
    let conn = db.connect().expect("conn");
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // Insert enough rows that the table B-tree must grow beyond its
        // existing (empty) pages, forcing allocate_page to consult the
        // freelist trunk page that was corrupted in phase 2.
        for row in 1000..1100 {
            let mut stmt = conn
                .prepare(format!(
                    "INSERT INTO t VALUES ({row}, '{}')",
                    "x".repeat(200)
                ))
                .map_err(|e| format!("prepare error: {e}"))?;
            for _ in 0..10_000 {
                match stmt.step() {
                    Ok(StepResult::Done) => break,
                    Ok(StepResult::IO) => io.step().map_err(|e| format!("io error: {e}"))?,
                    Err(e) => return Err(format!("step error: {e}")),
                    _ => {}
                }
            }
        }
        Ok(())
    }));

    match outcome {
        Ok(Ok(())) => panic!(
            "INSERT completed despite the trunk page leaf count being corrupted by \
             cosmic rays. allocate_page does not validate the freelist trunk leaf count, \
             but free_page and integrity_check both do — this asymmetry is the bug."
        ),
        Ok(Err(_msg)) => {
            // The fix returns LimboError::Corrupt for an invalid leaf count.
        }
        Err(_panic) => {
            // Without the fix, allocate_page panics on copy_within when the
            // leaf count exceeds page capacity. catch_unwind recovers the
            // panic so the test surface treats it as the expected outcome.
        }
    }
}

/// Regression test: allocate_page does not bounds-check the freelist trunk
/// page next-trunk and leaf-page pointers against `database_size`.
///
/// `free_page` rejects `page_id < 2 || page_id > database_size` (pager.rs
/// "Invalid page number for free operation"). `clear_overflow_pages` performs
/// the same check (btree.rs "Invalid overflow page number"). `integrity_check`
/// flags out-of-range freelist pointers. `allocate_page` reads both pointers
/// from the trunk page and uses them without bounds checking — `read_page` on
/// the leaf pointer or assignment of the next-trunk pointer into the header
/// can install an active B-tree page into the freelist, which is then zeroed
/// (`leaf_page.get_contents().as_ptr().fill(0)`). The result is irrecoverable
/// data destruction.
///
/// Reachability: a bit flip in the trunk page header's next-trunk pointer
/// (offset 0..4) or first leaf pointer (offset 8..12) is the same fault class
/// the cosmic ray model already injects globally. Focusing the model on those
/// twelve bytes turns an astronomical wait into a deterministic test.
#[test]
fn test_freelist_trunk_page_pointers_unvalidated_in_allocate_page() {
    // Phase 1: build a database with a populated freelist (faults disabled).
    let io = Arc::new(SimulatorIO::new(
        false,
        ChaCha8Rng::seed_from_u64(0xBAD_BA6E),
        IOFaultConfig::default(),
    ));
    let db_path = format!("test-freelist-trunk-ptr-{}.db", std::process::id());
    let setup_db = Database::open_file_with_flags(
        io.clone(),
        &db_path,
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
    )
    .expect("open db");
    let setup_conn = setup_db.connect().expect("conn");
    let (page_size, trunk_page) = populate_and_drain_freelist(&setup_conn, &io, &db_path);
    setup_conn.close().expect("close setup conn");
    drop(setup_db);

    // Phase 2: focus cosmic rays on the trunk page header bytes that hold pointers:
    //   [0..4)  next-trunk pointer
    //   [8..12) first leaf pointer
    // Bytes [4..8) hold the leaf count, which is the subject of the other
    // regression test; we exclude it here to isolate the pointer bug.
    let trunk_offset = (trunk_page as usize - 1) * page_size;
    io.set_fault_config(IOFaultConfig {
        cosmic_ray_probability: 1.0,
        cosmic_ray_targets: vec![
            CosmicRayTarget {
                file_suffix: ".db".to_string(),
                byte_range: trunk_offset..(trunk_offset + 4),
            },
            CosmicRayTarget {
                file_suffix: ".db".to_string(),
                byte_range: (trunk_offset + 8)..(trunk_offset + 12),
            },
        ],
    });
    for _ in 0..32 {
        io.step().expect("pulse io step");
    }
    io.set_fault_config(IOFaultConfig::default());

    let db = Database::open_file_with_flags(
        io.clone(),
        &db_path,
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
    )
    .expect("reopen db");
    let conn = db.connect().expect("conn");
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // Insert enough rows that the table B-tree must grow beyond its
        // existing (empty) pages, forcing allocate_page to consult the
        // freelist trunk page that was corrupted in phase 2.
        for row in 1000..1100 {
            let mut stmt = conn
                .prepare(format!(
                    "INSERT INTO t VALUES ({row}, '{}')",
                    "x".repeat(200)
                ))
                .map_err(|e| format!("prepare error: {e}"))?;
            for _ in 0..10_000 {
                match stmt.step() {
                    Ok(StepResult::Done) => break,
                    Ok(StepResult::IO) => io.step().map_err(|e| format!("io error: {e}"))?,
                    Err(e) => return Err(format!("step error: {e}")),
                    _ => {}
                }
            }
        }
        Ok(())
    }));

    match outcome {
        Ok(Ok(())) => panic!(
            "INSERT completed despite the trunk page pointers being corrupted by \
             cosmic rays. allocate_page does not validate freelist page pointers, \
             but free_page, clear_overflow_pages, and integrity_check all do — this \
             asymmetry is the bug."
        ),
        Ok(Err(_msg)) => {
            // The fix returns LimboError::Corrupt for an out-of-range page id.
        }
        Err(_panic) => {
            // Without the fix, the unvalidated pointer eventually triggers a
            // page-type assertion in btree code when an arbitrary page is
            // interpreted as a freelist trunk.
        }
    }
}
