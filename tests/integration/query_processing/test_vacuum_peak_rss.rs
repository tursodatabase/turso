//! Memory-bound regression tests for `VACUUM INTO` (upstream PR #11503).
//!
//! The batched `VACUUM INTO` build must copy a database in bounded memory,
//! whatever its size — the non-batched build committed the whole target in one
//! transaction and re-read every spilled page on commit, so copying a database
//! cost about twice its size resident, and a backup of a database larger than
//! half the host's RAM was an OOM kill.
//!
//! These run real SQL against a file-backed fixture and read the process
//! high-water mark from `/proc/self/status`, so they are Linux-only, `#[ignore]`
//! (they write ~500 MB and take minutes unoptimised), and live in the
//! integration crate (where a parse-capable `turso_sqlite` dialect is
//! available). Run them with:
//!
//! ```text
//! cargo test -p core_tester --release --test integration_tests \
//!     vacuum_into_peak_rss -- --ignored --nocapture
//! ```

#![cfg(target_os = "linux")]

use std::sync::Arc;

use turso_core::{ConnectOpts, Database, DatabaseOpts, OpenFlags, PlatformIO, Result, SessionRole, IO};
use turso_sqlite::SqliteDialect;

/// `VmHWM` of this process from `/proc/self/status`, in MiB: the kernel's
/// high-water mark of resident memory, which no later free lowers.
fn process_peak_rss_mib() -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").expect("/proc/self/status");
    let line = status
        .lines()
        .find(|l| l.starts_with("VmHWM:"))
        .expect("VmHWM in /proc/self/status");
    let kib: u64 = line
        .split_whitespace()
        .nth(1)
        .expect("VmHWM value")
        .parse()
        .expect("VmHWM is kB");
    kib / 1024
}

/// `VACUUM INTO` must run in bounded memory: copying a database must not need
/// the whole database resident, let alone twice it, or a backup of a database
/// larger than the host's RAM budget is an OOM kill. Builds a ~250 MiB
/// file-backed fixture (a table of 1 KB blobs with a secondary index on a key
/// in rowid order), runs the plain public statement, prints the process
/// high-water mark before and after, and asserts the peak stays under a bound
/// well below twice the file.
#[test]
#[ignore = "writes ~500 MB and takes minutes unoptimised; run with: \
            cargo test -p core_tester --release --test integration_tests \
            vacuum_into_peak_rss -- --ignored --nocapture"]
fn vacuum_into_peak_rss_is_bounded() -> Result<()> {
    vacuum_into_peak_rss_fixture_is_bounded("'key-' || printf('%012d', {row})", 256)
}

/// The same bound with the index on keys in no rowid order (random text, as a
/// UUID column is): the index is then built after the rows by one CREATE INDEX,
/// sorted, rather than taking a key per row wherever it falls — the bound holds
/// either way, and this is the shape whose time regressed threefold when the
/// index was created first. Prints the peak and the time to compare by hand.
#[test]
#[ignore = "writes ~500 MB and takes minutes unoptimised; run with: \
            cargo test -p core_tester --release --test integration_tests \
            vacuum_into_peak_rss -- --ignored --nocapture"]
fn vacuum_into_peak_rss_is_bounded_with_a_random_key_index() -> Result<()> {
    vacuum_into_peak_rss_fixture_is_bounded("hex(randomblob(16))", 256)
}

/// Builds the fixture with `key_expr` (a SQL expression; `{row}` is the row
/// number) as the indexed key, copies it and asserts the process peak RSS stays
/// under `bound_mib`.
fn vacuum_into_peak_rss_fixture_is_bounded(key_expr: &str, bound_mib: u64) -> Result<()> {
    const ROWS: usize = 200_000;
    const ROWS_PER_TXN: usize = 1_000;
    const PAYLOAD_BYTES: usize = 1_000;

    let io: Arc<dyn IO> = Arc::new(PlatformIO::new()?);
    let dir = tempfile::tempdir().unwrap();
    let source_path = dir.path().join("source.db");
    let dest_path = dir.path().join("copy.db");
    let db = Database::open_file_with_flags(
        io,
        source_path.to_str().unwrap(),
        OpenFlags::Create,
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )?;
    let conn = db.connect(ConnectOpts::new(SessionRole::Superuser))?;
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, k TEXT NOT NULL, v BLOB NOT NULL)")?;
    conn.execute("CREATE INDEX t_k ON t(k)")?;
    for row in 0..ROWS {
        if row % ROWS_PER_TXN == 0 {
            conn.execute("BEGIN")?;
        }
        let key = key_expr.replace("{row}", &row.to_string());
        conn.execute(format!(
            "INSERT INTO t(k, v) VALUES ({key}, randomblob({PAYLOAD_BYTES}))"
        ))?;
        if row % ROWS_PER_TXN == ROWS_PER_TXN - 1 {
            conn.execute("COMMIT")?;
        }
    }
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
    let file_mib = std::fs::metadata(&source_path).unwrap().len() / (1024 * 1024);
    assert!(file_mib >= 150, "fixture is only {file_mib} MiB");

    let before = process_peak_rss_mib();
    let started = std::time::Instant::now();
    conn.execute(format!("VACUUM INTO '{}'", dest_path.to_str().unwrap()))?;
    let elapsed = started.elapsed();
    let after = process_peak_rss_mib();
    let copy_mib = std::fs::metadata(&dest_path).unwrap().len() / (1024 * 1024);
    println!(
        "VACUUM INTO of a {file_mib} MiB database (key {key_expr}): peak RSS {before} MiB \
         before, {after} MiB after (+{} MiB); the copy is {copy_mib} MiB, took {elapsed:.1?}",
        after.saturating_sub(before)
    );
    assert!(
        copy_mib.abs_diff(file_mib) <= file_mib / 10,
        "the copy is {copy_mib} MiB for a {file_mib} MiB source"
    );
    assert!(
        after <= bound_mib,
        "VACUUM INTO of a {file_mib} MiB database drove the process peak RSS to \
         {after} MiB (bound {bound_mib} MiB)"
    );
    Ok(())
}
