use crate::common::{do_flush, run_query, run_query_on_row, TempDatabase};
use rand::{rng, RngCore};
use turso_core::Row;

#[test]
fn test_per_page_checksum() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let db_name = format!("test-{}.db", rng().next_u32());
    let tmp_db = TempDatabase::new(&db_name);
    let db_path = tmp_db.path.clone();

    {
        let conn = tmp_db.connect_limbo();
        run_query(
            &tmp_db,
            &conn,
            "CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT);",
        )?;
        run_query(
            &tmp_db,
            &conn,
            "INSERT INTO test (value) VALUES ('Hello, World!')",
        )?;
        let mut row_count = 0;
        run_query_on_row(&tmp_db, &conn, "SELECT * FROM test", |row: &Row| {
            assert_eq!(row.get::<i64>(0).unwrap(), 1);
            assert_eq!(row.get::<String>(1).unwrap(), "Hello, World!");
            row_count += 1;
        })?;
        assert_eq!(row_count, 1);
        do_flush(&conn, &tmp_db)?;
    }

    {
        let metadata = std::fs::metadata(&db_path)?;
        assert_eq!(metadata.len(), 4096, "db file should be exactly 4096 bytes");
    }

    // let's test that page actually contains checksum bytes
    {
        let file_contents = std::fs::read(&db_path)?;
        assert_eq!(
            file_contents.len(),
            4096,
            "file contents should be 4096 bytes"
        );

        // split the page: first 4088 bytes are actual page, last 8 bytes are checksum
        let actual_page = &file_contents[..4096 - 8];
        let checksum_bytes = &file_contents[4096 - 8..];
        let stored_checksum = u64::from_le_bytes(checksum_bytes.try_into().unwrap());

        let expected_checksum = twox_hash::XxHash3_64::oneshot(actual_page);
        assert_eq!(
            stored_checksum, expected_checksum,
            "Stored checksum should match manually calculated checksum"
        );
    }

    Ok(())
}

#[test]
fn test_checksum_detects_corruption() {
    let _ = env_logger::try_init();
    let db_name = format!("test-corruption-{}.db", rng().next_u32());
    let tmp_db = TempDatabase::new(&db_name);
    let db_path = tmp_db.path.clone();

    // Create and populate the database
    {
        let conn = tmp_db.connect_limbo();
        run_query(
            &tmp_db,
            &conn,
            "CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT);",
        )
        .unwrap();
        run_query(
            &tmp_db,
            &conn,
            "INSERT INTO test (value) VALUES ('Hello, World!')",
        )
        .unwrap();

        do_flush(&conn, &tmp_db).unwrap();
        run_query(&tmp_db, &conn, "PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    }

    {
        let mut file_contents = std::fs::read(&db_path).unwrap();
        assert_eq!(file_contents.len(), 8192, "File should be 8192 bytes");

        // lets corrupt the db at byte 2025, the year of Turso DB
        file_contents[2025] = !file_contents[2025];
        std::fs::write(&db_path, file_contents).unwrap();
    }

    {
        let existing_db = TempDatabase::new_with_existent(&db_path);
        // Connecting should fail with a checksum error because the header page is corrupted
        let result = existing_db.db.connect();

        match result {
            Ok(_) => panic!("should return error when connecting to corrupted DB"),
            Err(e) => {
                let err_msg = e.to_string();
                assert!(
                    err_msg.contains("Checksum mismatch"),
                    "error should indicate checksum mismatch, got: {err_msg}"
                );
            }
        }
    }
}
