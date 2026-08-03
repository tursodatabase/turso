//! Tests that move the pending byte live in their own test binary.
//!
//! The pending byte is process-global twice over: turso keeps it in a static,
//! and real SQLite moves it with `sqlite3_test_control`, which is only allowed
//! while the process has no open SQLite connections. Tests in other binaries
//! never see the moved value because `cargo test` runs test binaries one at a
//! time and nextest runs every test in its own process. Inside this binary,
//! `#[serial_test::serial]` keeps the tests from overlapping each other.
//!
//! Do not add tests here unless they must move the pending byte.
#![cfg(feature = "test_helper")]

#[allow(dead_code)]
#[path = "../fuzz/helpers.rs"]
mod helpers;

use core_tester::common::{rusqlite_integrity_check, TempDatabase};
use rand::Rng;

/// Restores the previous pending byte when dropped, so a failing test cannot
/// leave the moved value behind for the next test in this binary.
struct PendingByteGuard(u32);

impl PendingByteGuard {
    fn set(offset: u32) -> Self {
        let previous = TempDatabase::get_pending_byte();
        TempDatabase::set_pending_byte(offset);
        Self(previous)
    }
}

impl Drop for PendingByteGuard {
    fn drop(&mut self) {
        TempDatabase::set_pending_byte(self.0);
    }
}

#[turso_macros::test(mvcc)]
#[serial_test::serial]
pub fn fuzz_pending_byte_database(db: TempDatabase) -> anyhow::Result<()> {
    let (mut rng, _seed) = helpers::init_fuzz_test_tracing("fuzz_pending_byte_database");

    // TODO: currently assume that page size is 4096 bytes (4 Kib)
    const PAGE_SIZE: u32 = 4 * 2u32.pow(10);

    /// 100 Mib
    const MAX_DB_SIZE_BYTES: u32 = 100 * 2u32.pow(20);

    const MAX_PAGENO: u32 = MAX_DB_SIZE_BYTES / PAGE_SIZE;

    let builder = helpers::builder_from_db(&db);

    for _ in 0..helpers::fuzz_iterations(10) {
        // generate a random pending page that is smaller than the 100 MB mark
        let pending_byte_pgno = rng.random_range(2..MAX_PAGENO);
        let pending_byte = pending_byte_pgno * PAGE_SIZE;

        tracing::debug!(pending_byte_pgno, pending_byte);

        let db_path = tempfile::NamedTempFile::new()?;

        // The guard must outlive the integrity check: rusqlite has to read
        // the file with the same moved pending byte turso wrote it with.
        let pending_byte_guard = {
            let db = builder.clone().with_db_path(db_path.path()).build();

            let pending_byte_guard = PendingByteGuard::set(pending_byte);

            // Insert more than enough to pass the PENDING_BYTE
            let query = format!(
                "insert into t select replace(zeroblob({PAGE_SIZE}), x'00', 'A') from generate_series(1, {});",
                MAX_PAGENO * 2
            );

            let conn = db.connect_limbo();

            conn.execute("create table t(x);")?;

            conn.execute(&query)?;

            conn.close()?;

            pending_byte_guard
        };

        rusqlite_integrity_check(db_path.path())?;
        drop(pending_byte_guard);
    }

    Ok(())
}
