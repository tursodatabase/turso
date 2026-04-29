use std::sync::Arc;

use core_tester::{
    common::ExecRows,
    queued_io::{QueuedIo, QueuedIoEvent},
};
use rand::{Rng, RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
use turso_core::{Connection, Database};

use super::helpers;

#[derive(Debug, Clone, PartialEq, Eq)]
struct VacuumSnapshot {
    rows: Vec<(i64, String)>,
    schema: Vec<(String, String, String, String)>,
}

fn open_queued_db(io: Arc<QueuedIo>, path: &str) -> anyhow::Result<Arc<Database>> {
    Ok(Database::open_file(io, path)?)
}

fn run_integrity_check(conn: &Arc<Connection>) -> String {
    let rows: Vec<(String,)> = conn.exec_rows("PRAGMA integrity_check");
    rows.into_iter()
        .map(|(text,)| text)
        .collect::<Vec<_>>()
        .join("\n")
}

fn snapshot(conn: &Arc<Connection>) -> VacuumSnapshot {
    VacuumSnapshot {
        rows: conn.exec_rows("SELECT id, payload FROM t ORDER BY id"),
        schema: conn.exec_rows(
            "SELECT type, name, tbl_name, COALESCE(sql, '') \
             FROM sqlite_schema \
             ORDER BY type, name, tbl_name, COALESCE(sql, '')",
        ),
    }
}

fn populate_workload(
    conn: &Arc<Connection>,
    rows: usize,
    payload_len: usize,
    delete_mod: usize,
    delete_rem: usize,
) -> anyhow::Result<()> {
    conn.execute("PRAGMA page_size = 512")?;
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT NOT NULL)")?;
    conn.execute("CREATE INDEX idx_t_payload ON t(payload)")?;

    for id in 0..rows {
        let tag = (b'a' + (id % 26) as u8) as char;
        let payload = tag.to_string().repeat(payload_len);
        conn.execute(format!("INSERT INTO t VALUES({id}, '{payload}')"))?;
    }
    conn.execute(format!(
        "DELETE FROM t WHERE id % {delete_mod} = {delete_rem}"
    ))?;
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
    Ok(())
}

fn successful_vacuum_io_count(
    rows: usize,
    payload_len: usize,
    delete_mod: usize,
    delete_rem: usize,
    path: &str,
) -> anyhow::Result<usize> {
    let io = Arc::new(QueuedIo::new());
    let db = open_queued_db(io.clone(), path)?;
    let conn = db.connect()?;
    populate_workload(&conn, rows, payload_len, delete_mod, delete_rem)?;

    let history_start = io.history_len();
    conn.execute("VACUUM")?;
    Ok(io.history_since(history_start).len())
}

fn drain_queued_io(io: &QueuedIo) -> anyhow::Result<()> {
    while io.step_one()?.is_some() {}
    Ok(())
}

fn assert_failed_vacuum_cleanup_and_data(
    db: &Arc<Database>,
    conn: &Arc<Connection>,
    expected: &VacuumSnapshot,
    context: &str,
) -> anyhow::Result<()> {
    assert!(
        conn.get_auto_commit(),
        "{context}: VACUUM left auto-commit off"
    );
    let pager = conn.get_pager();
    assert!(!pager.holds_read_lock(), "{context}: leaked read lock");
    assert!(!pager.holds_write_lock(), "{context}: leaked write lock");
    assert_eq!(
        run_integrity_check(conn),
        "ok",
        "{context}: integrity_check"
    );
    assert_eq!(
        snapshot(conn),
        *expected,
        "{context}: failed VACUUM changed logical data"
    );

    let verifier = db.connect()?;
    assert_eq!(
        run_integrity_check(&verifier),
        "ok",
        "{context}: verifier integrity_check"
    );
    assert_eq!(
        snapshot(&verifier),
        *expected,
        "{context}: verifier saw different logical data"
    );

    conn.execute("INSERT INTO t VALUES(1000000, 'after-failed-vacuum')")?;
    conn.execute("DELETE FROM t WHERE id = 1000000")?;
    assert_eq!(
        snapshot(conn),
        *expected,
        "{context}: connection not reusable after failed VACUUM"
    );
    Ok(())
}

#[test]
fn queued_io_plain_vacuum_failure_fuzz() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let seed = rand::rng().next_u64();
    println!("queued_io_plain_vacuum_failure_fuzz seed: {seed}");
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let iterations = helpers::fuzz_iterations(8);

    for iter in 0..iterations {
        let rows = rng.random_range(96..=260);
        let payload_len = rng.random_range(96..=420);
        let delete_mod = rng.random_range(3..=9);
        let delete_rem = rng.random_range(0..delete_mod);
        let baseline_path = format!("queued-vacuum-fuzz-baseline-{seed}-{iter}.db");
        let event_count =
            successful_vacuum_io_count(rows, payload_len, delete_mod, delete_rem, &baseline_path)?;
        assert!(
            event_count > 0,
            "seed={seed} iter={iter}: VACUUM did not queue any I/O"
        );

        let fail_after_successes = rng.random_range(0..event_count);
        let io = Arc::new(QueuedIo::new());
        let path = format!("queued-vacuum-fuzz-failure-{seed}-{iter}.db");
        let db = open_queued_db(io.clone(), &path)?;
        let conn = db.connect()?;
        populate_workload(&conn, rows, payload_len, delete_mod, delete_rem)?;
        let expected = snapshot(&conn);
        let history_start = io.history_len();

        io.fail_any_after_successes(fail_after_successes);
        let err = conn
            .execute("VACUUM")
            .expect_err("fault injector should abort the selected queued I/O during plain VACUUM");
        io.clear_fault();
        let context = format!(
            "seed={seed} iter={iter} rows={rows} payload_len={payload_len} \
             delete_mod={delete_mod} delete_rem={delete_rem} \
             event_count={event_count} fail_after_successes={fail_after_successes}"
        );
        assert!(
            err.to_string().contains("aborted"),
            "{context}: unexpected VACUUM error: {err}"
        );

        let observed_events: Vec<QueuedIoEvent> = io.history_since(history_start);
        assert!(
            observed_events.len() > fail_after_successes,
            "{context}: selected failure point was not reached; events={observed_events:?}"
        );

        drain_queued_io(io.as_ref())?;
        assert_failed_vacuum_cleanup_and_data(&db, &conn, &expected, &context)?;
    }

    Ok(())
}
