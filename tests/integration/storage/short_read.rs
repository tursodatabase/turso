use crate::common::{do_flush, limbo_exec_rows_fallible, run_query, TempDatabase};
use asserting::prelude::*;
use rand::{rng, RngCore};
use std::fs::OpenOptions;

/// Test that truncating a database file results in a ShortRead error.
#[test]
fn test_truncated_database_returns_short_read_error() {
    let _ = env_logger::try_init();
    let db_name = format!("test-truncated-{}.db", rng().next_u32());
    let tmp_db = TempDatabase::new(&db_name);
    let db_path = tmp_db.path.clone();

    // Create and populate the database with multiple pages
    {
        let conn = tmp_db.connect_limbo();
        run_query(
            &tmp_db,
            &conn,
            "CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT);",
        )
        .unwrap();

        for _ in 0..100 {
            run_query(
                &tmp_db,
                &conn,
                &format!("INSERT INTO test (value) VALUES ('{}');", "x".repeat(100)),
            )
            .unwrap();
        }

        do_flush(&conn, &tmp_db).unwrap();
        run_query(&tmp_db, &conn, "PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    }

    // The database must be larger than one page.
    assert_that!(std::fs::metadata(&db_path).unwrap().len()).is_greater_than(4096);

    // Truncate to 1.5 pages - reading page 2 will get 2048 bytes instead of 4096
    let truncated_size = 4096 + 2048;
    {
        let file = OpenOptions::new()
            .write(true)
            .open(&db_path)
            .expect("Failed to open database file for truncation");
        file.set_len(truncated_size)
            .expect("Failed to truncate database file");
    }

    {
        let existing_db = TempDatabase::new_with_existent(&db_path);
        let conn = existing_db.connect_limbo();

        let result = limbo_exec_rows_fallible(&existing_db, &conn, "SELECT * FROM test");

        assert_that!(result)
            .err()
            .display_string()
            .contains("short read");
    }
}

/// Test that truncating a WAL file results in a ShortRead error.
#[test]
fn test_truncated_wal_returns_short_read_error() {
    let _ = env_logger::try_init();
    let db_name = format!("test-truncated-wal-{}.db", rng().next_u32());
    let tmp_db = TempDatabase::new(&db_name);
    let db_path = tmp_db.path.clone();
    let wal_path = format!("{}-wal", db_path.display());

    {
        let conn = tmp_db.connect_limbo();
        run_query(
            &tmp_db,
            &conn,
            "CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT);",
        )
        .unwrap();

        for i in 0..100 {
            run_query(
                &tmp_db,
                &conn,
                &format!(
                    "INSERT INTO test (id, value) VALUES ({i}, '{}');",
                    "x".repeat(100)
                ),
            )
            .unwrap();
        }

        // Flush to WAL but do NOT checkpoint
        do_flush(&conn, &tmp_db).unwrap();
    }

    // The WAL must contain data.
    assert_that!(std::fs::metadata(&wal_path)
        .expect("WAL file should exist")
        .len())
    .is_greater_than(4096);

    // Truncate WAL mid-frame: header (32) + 1 full frame (24+4096) + partial frame
    let truncated_wal_size = 32 + (24 + 4096) + 2048;
    {
        let file = OpenOptions::new()
            .write(true)
            .open(&wal_path)
            .expect("Failed to open WAL file for truncation");
        file.set_len(truncated_wal_size)
            .expect("Failed to truncate WAL file");
    }

    {
        let existing_db = TempDatabase::new_with_existent(&db_path);

        // Error occurs during connect() when reading WAL frames
        match existing_db.db.connect() {
            Ok(_) => panic!("Connection to database with truncated WAL must fail"),
            Err(err) => {
                let err_string = err.to_string();
                assert!(
                    err_string.contains("short read"),
                    "Expected 'short read' error, got: {err_string}",
                );
            }
        }
    }
}

/// Test that truncating the database header results in a ShortRead error.
#[test]
fn test_truncated_header_returns_short_read_error() {
    let _ = env_logger::try_init();
    let db_name = format!("test-truncated-header-{}.db", rng().next_u32());
    let tmp_db = TempDatabase::new(&db_name);
    let db_path = tmp_db.path.clone();

    // Create a minimal database
    {
        let conn = tmp_db.connect_limbo();
        run_query(
            &tmp_db,
            &conn,
            "CREATE TABLE test (id INTEGER PRIMARY KEY);",
        )
        .unwrap();
        do_flush(&conn, &tmp_db).unwrap();
        run_query(&tmp_db, &conn, "PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    }

    // Truncate the database to only 50 bytes (less than a full header read)
    {
        let file = OpenOptions::new()
            .write(true)
            .open(&db_path)
            .expect("Failed to open database file for truncation");
        file.set_len(50).expect("Failed to truncate database file");
    }

    // Opening the database should fail with a short read error
    {
        let existing_db = TempDatabase::new_with_existent(&db_path);
        match existing_db.db.connect() {
            Ok(_) => panic!("Connection to database with truncated header must fail"),
            Err(err) => {
                let err_string = err.to_string();
                assert!(
                    err_string.contains("short read"),
                    "Expected 'short read' error, got: {err_string}",
                );
            }
        }
    }
}

/// Test that zeroing a database page results in a Corrupt error.
#[test]
fn test_zeroed_page_returns_corrupt_error() {
    let _ = env_logger::try_init();
    let db_name = format!("test-zeroed-page-{}.db", rng().next_u32());
    let tmp_db = TempDatabase::new(&db_name);
    let db_path = tmp_db.path.clone();

    {
        let conn = tmp_db.connect_limbo();
        run_query(
            &tmp_db,
            &conn,
            "CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT);",
        )
        .unwrap();

        for _ in 0..100 {
            run_query(
                &tmp_db,
                &conn,
                &format!("INSERT INTO test (value) VALUES ('{}');", "x".repeat(100)),
            )
            .unwrap();
        }

        do_flush(&conn, &tmp_db).unwrap();
        run_query(&tmp_db, &conn, "PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    }

    // Zero out page 2 (bytes 4096-8191)
    {
        let mut file_contents = std::fs::read(&db_path).unwrap();
        // The database must have at least two pages.
        assert_that!(&file_contents).has_at_least_length(8192);
        file_contents[4096..8192].fill(0);
        std::fs::write(&db_path, file_contents).unwrap();
    }

    {
        let existing_db = TempDatabase::new_with_existent(&db_path);
        let conn = existing_db.connect_limbo();

        let result = limbo_exec_rows_fallible(&existing_db, &conn, "SELECT * FROM test");

        assert_that!(result)
            .err()
            .display_string()
            .contains("Corrupt")
            .contains("Invalid page type: 0");
    }
}
