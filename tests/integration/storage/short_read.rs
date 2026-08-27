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

/// Test that a page pointer larger than the database size fails as a
/// Corrupt error, not a ShortRead I/O error (SQLite: btree.c
/// getAndInitPage). Regression for
/// https://github.com/tursodatabase/turso/issues/8488.
#[test]
fn test_out_of_range_page_pointer_returns_corrupt_error() {
    let _ = env_logger::try_init();
    let db_name = format!("test-oor-pointer-{}.db", rng().next_u32());
    let tmp_db = TempDatabase::new(&db_name);
    let db_path = tmp_db.path.clone();

    // 100 ~100-byte rows split the leaf so the root becomes an interior page.
    let root_page = {
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

        let rows = limbo_exec_rows_fallible(
            &tmp_db,
            &conn,
            "SELECT rootpage FROM sqlite_schema WHERE name = 'test'",
        )
        .unwrap();
        match rows[0][0] {
            rusqlite::types::Value::Integer(n) => n as u64,
            ref other => panic!("rootpage should be an integer, got {other:?}"),
        }
    };

    // Overwrite the root's right-most child pointer (bytes 8..12 of an
    // interior page header) with the page number from issue #8488.
    {
        let mut file_contents = std::fs::read(&db_path).unwrap();
        let page_size = 4096usize;
        let off = (root_page as usize - 1) * page_size;
        assert_eq!(
            file_contents[off], 0x05,
            "table root must be an interior page (0x05) for this test to \
             corrupt a child pointer; got page type 0x{:02x}",
            file_contents[off]
        );
        file_contents[off + 8..off + 12].copy_from_slice(&0x9BC41735u32.to_be_bytes());
        std::fs::write(&db_path, file_contents).unwrap();
    }

    {
        let existing_db = TempDatabase::new_with_existent(&db_path);
        let conn = existing_db.connect_limbo();

        let result = limbo_exec_rows_fallible(&existing_db, &conn, "SELECT * FROM test");

        let err = result.expect_err("Full scan through an out-of-range page pointer must fail");
        let err_string = err.to_string();
        assert!(
            err_string.contains("Corrupt"),
            "An out-of-range page pointer is corruption, not an I/O error \
             (SQLite returns SQLITE_CORRUPT here); got: {err_string}",
        );
    }
}

/// Test that a stale in-header size is ignored when the change counter
/// (offset 24) does not match version-valid-for (offset 92): the file was
/// last written by pre-3.7.0 SQLite, so the size must not reject pages that
/// exist. SQLite recomputes the size from the file (btree.c lockBtree).
#[test]
fn test_stale_header_size_with_counter_mismatch_still_reads() {
    let _ = env_logger::try_init();
    let db_name = format!("test-stale-mismatch-{}.db", rng().next_u32());
    let db_path = build_db_then_patch_header(&db_name, |bytes| {
        bytes[24..28].copy_from_slice(&7u32.to_be_bytes()); // != version-valid-for
        bytes[28..32].copy_from_slice(&1u32.to_be_bytes()); // stale size: 1 page
    });

    {
        let existing_db = TempDatabase::new_with_existent(&db_path);
        let conn = existing_db.connect_limbo();
        let rows = limbo_exec_rows_fallible(&existing_db, &conn, "SELECT * FROM test").unwrap();
        assert_eq!(rows.len(), 100);
    }

    // Modern SQLite serves the same file the same way.
    let sqlite = rusqlite::Connection::open(&db_path).unwrap();
    let count: i64 = sqlite
        .query_row("SELECT count(*) FROM test", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 100);
}

/// Test that a stale in-header size IS trusted when the counters match:
/// pages beyond it are corrupt references for both engines.
#[test]
fn test_stale_header_size_with_counters_matching_is_corrupt() {
    let _ = env_logger::try_init();
    let db_name = format!("test-stale-match-{}.db", rng().next_u32());
    let db_path = build_db_then_patch_header(&db_name, |bytes| {
        bytes[24..28].copy_from_slice(&7u32.to_be_bytes());
        bytes[92..96].copy_from_slice(&7u32.to_be_bytes()); // == change counter
        bytes[28..32].copy_from_slice(&1u32.to_be_bytes()); // stale size: 1 page
    });

    {
        let existing_db = TempDatabase::new_with_existent(&db_path);
        let conn = existing_db.connect_limbo();
        let err = limbo_exec_rows_fallible(&existing_db, &conn, "SELECT * FROM test")
            .expect_err("pages beyond a trusted size must be corrupt references");
        assert!(
            err.to_string().contains("Corrupt"),
            "expected a Corrupt error, got: {err}"
        );
    }

    let sqlite = rusqlite::Connection::open(&db_path).unwrap();
    let err = sqlite
        .query_row("SELECT count(*) FROM test", [], |row| row.get::<_, i64>(0))
        .expect_err("SQLite must also reject pages beyond a trusted size");
    assert!(
        err.to_string().contains("malformed"),
        "expected 'malformed', got: {err}"
    );
}

fn build_db_then_patch_header(db_name: &str, patch: impl FnOnce(&mut [u8])) -> std::path::PathBuf {
    let tmp_db = TempDatabase::new(db_name);
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
    let mut file_contents = std::fs::read(&db_path).unwrap();
    patch(&mut file_contents);
    std::fs::write(&db_path, file_contents).unwrap();
    db_path
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
        assert!(
            file_contents.len() >= 8192,
            "Database should have at least 2 pages"
        );
        file_contents[4096..8192].fill(0);
        std::fs::write(&db_path, file_contents).unwrap();
    }

    {
        let existing_db = TempDatabase::new_with_existent(&db_path);
        let conn = existing_db.connect_limbo();

        let result = limbo_exec_rows_fallible(&existing_db, &conn, "SELECT * FROM test");

        let err = result.expect_err("Query on database with zeroed page must return an error");
        let err_string = err.to_string();
        assert!(
            err_string.contains("Corrupt") && err_string.contains("Invalid page type: 0"),
            "Expected 'Corrupt database: Invalid page type: 0' error, got: {err_string}",
        );
    }
}
