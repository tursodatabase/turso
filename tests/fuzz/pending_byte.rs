pub mod helpers;

#[cfg(feature = "test_helper")]
use core_tester::common::TempDatabase;

#[turso_macros::test(mvcc)]
#[cfg(feature = "test_helper")]
#[serial_test::file_serial]
pub fn fuzz_pending_byte_database(db: TempDatabase) -> anyhow::Result<()> {
    use core_tester::common::rusqlite_integrity_check;
    use rand::Rng;

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

        let temp_dir = tempfile::tempdir()?;
        let db_path = temp_dir.path().join("test.db");

        {
            let db = builder.clone().with_db_path(&db_path).build();

            let prev_pending_byte = TempDatabase::get_pending_byte();
            tracing::debug!(prev_pending_byte);

            TempDatabase::set_pending_byte(pending_byte);

            let new_pending_byte = TempDatabase::get_pending_byte();
            tracing::debug!(new_pending_byte);

            // Insert more than enough to pass the PENDING_BYTE
            let query = format!(
                "insert into t select replace(zeroblob({PAGE_SIZE}), x'00', 'A') from generate_series(1, {});",
                MAX_PAGENO * 2
            );

            let conn = db.connect_limbo();
            conn.execute("create table t(x);")?;
            conn.execute(&query)?;
            conn.close()?;
        }

        rusqlite_integrity_check(&db_path)?;

        TempDatabase::reset_pending_byte();
    }

    Ok(())
}
