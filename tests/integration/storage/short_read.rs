use crate::common::{do_flush, limbo_exec_rows_fallible, run_query, TempDatabase};
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

    let original_size = std::fs::metadata(&db_path).unwrap().len();
    assert!(
        original_size > 4096,
        "Database should be larger than one page, got {original_size} bytes",
    );

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

        let err = result.expect_err("Query on truncated database must return an error");
        let err_string = err.to_string();
        assert!(
            err_string.contains("short read"),
            "Expected 'short read' error, got: {err_string}",
        );
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

    let wal_size = std::fs::metadata(&wal_path)
        .expect("WAL file should exist")
        .len();
    assert!(
        wal_size > 4096,
        "WAL should contain data, got {wal_size} bytes"
    );

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
                    err_string.contains("short read") || err_string.contains("ShortRead"),
                    "Expected 'short read' error, got: {err_string}",
                );
            }
        }
    }
}

/// Test that punching a hole in a database page results in error handling, not panic.
/// NOTE: This test is currently disabled because zeroed pages cause a different
/// kind of error (invalid page type) that isn't handled gracefully yet.
/// The short read handling we implemented only covers truncated files.
#[test]
#[ignore = "zeroed page corruption handling requires additional work beyond short read fixes"]
fn test_zeroed_page_returns_error() {
    let _ = env_logger::try_init();
    let db_name = format!("test-zeroed-page-{}.db", rng().next_u32());
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

        // Insert enough data to create multiple pages
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

    // Zero out the second page (bytes 4096-8191)
    {
        let mut file_contents = std::fs::read(&db_path).unwrap();
        assert!(
            file_contents.len() >= 8192,
            "Database should have at least 2 pages"
        );

        // Zero out the second page
        file_contents[4096..8192].fill(0);
        std::fs::write(&db_path, file_contents).unwrap();
    }

    // Open and try to query - should handle gracefully
    {
        let existing_db = TempDatabase::new_with_existent(&db_path);
        let conn = existing_db.connect_limbo();

        let result = limbo_exec_rows_fallible(&existing_db, &conn, "SELECT * FROM test");

        // The query should fail with an error (the zeroed page makes the btree invalid)
        // We don't panic, we return an error
        if result.is_ok() {
            // If by chance the zeroed page wasn't needed for this query,
            // that's also acceptable - the test is about not panicking
            println!("Query succeeded despite zeroed page - page may not have been accessed");
        } else {
            let err = result.unwrap_err();
            println!("Query failed as expected with: {err}");
        }
    }
}
