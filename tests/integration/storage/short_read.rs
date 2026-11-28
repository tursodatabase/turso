use crate::common::{do_flush, limbo_exec_rows_fallible, run_query, TempDatabase};
use asserting::prelude::*;
use rand::{rng, RngCore};
use std::fs::OpenOptions;
use turso_core::{ConnectOpts, SessionRole};

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
        match existing_db
            .db
            .connect(ConnectOpts::new(SessionRole::Superuser))
        {
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
        match existing_db
            .db
            .connect(ConnectOpts::new(SessionRole::Superuser))
        {
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

#[test]
fn test_wal_short_reads_fail_open_without_losing_rows() {
    use crate::common::ExecRows;
    use crate::queued_io::QueuedIo;
    use std::sync::Arc;
    use turso_core::{
        Buffer, Completion, CompletionError, Database, IOResult, LimboError, OpenFlags,
        OpenOptions, WalFileShared, IO,
    };
    use turso_sqlite::SqliteDialect;

    let dir = tempfile::tempdir().unwrap();
    let source_path = dir.path().join("source.db");
    let source = rusqlite::Connection::open(&source_path).unwrap();
    source
        .execute_batch(
            "PRAGMA page_size=512;
             PRAGMA journal_mode=WAL;
             CREATE TABLE t(x);
             PRAGMA wal_checkpoint(TRUNCATE);
             INSERT INTO t VALUES (1);
             INSERT INTO t VALUES (2);
             INSERT INTO t VALUES (3);
             INSERT INTO t VALUES (4);",
        )
        .unwrap();
    let db_bytes = std::fs::read(&source_path).unwrap();
    let wal_bytes = std::fs::read(dir.path().join("source.db-wal")).unwrap();

    for limit in (1..=32).chain([33, 100, 535, 536, 600, 1100, 2143]) {
        let io = Arc::new(QueuedIo::new());
        let path = dir.path().join(format!("short-{limit}.db"));
        let path = path.to_str().unwrap();
        for (name, bytes) in [
            (path.to_owned(), &db_bytes),
            (format!("{path}-wal"), &wal_bytes),
        ] {
            let file = io.open_file(&name, OpenFlags::Create, false).unwrap();
            let c = file
                .pwrite(
                    0,
                    Arc::new(Buffer::new(bytes.clone())),
                    Completion::new_write(|_| {}),
                )
                .unwrap();
            io.wait_for_completion(c).unwrap();
        }
        io.limit_wal_reads(limit);
        let recovery_io: Arc<dyn IO> = io.clone();
        let mut recovery = WalFileShared::open_shared_if_exists_begin(
            &recovery_io,
            &format!("{path}-wal"),
            OpenFlags::Create,
        )
        .unwrap();
        let err = 'recovery: {
            for _ in 0..100 {
                match recovery.poll() {
                    Err(err) => break 'recovery err,
                    Ok(IOResult::Done(_)) => panic!("WAL read limit {limit}: recovery succeeded"),
                    Ok(IOResult::IO(c)) => {
                        while !c.finished() {
                            io.step().unwrap();
                        }
                    }
                }
            }
            panic!("WAL read limit {limit}: recovery did not finish");
        };
        assert!(
            matches!(
                *err,
                LimboError::CompletionError(CompletionError::ShortReadWalFrame { .. })
            ),
            "WAL read limit {limit}: unexpected recovery error {err:?}"
        );
        match Database::open(io.clone(), path, OpenOptions::new(Arc::new(SqliteDialect))) {
            Err(LimboError::CompletionError(CompletionError::ShortReadWalFrame { .. })) => {}
            Err(err) => panic!("WAL read limit {limit}: unexpected open error {err:?}"),
            Ok(db) => {
                io.limit_wal_reads(usize::MAX);
                let conn = db
                    .connect(ConnectOpts::new(SessionRole::Superuser))
                    .unwrap();
                let rows: Vec<(i64,)> = conn.exec_rows("SELECT x FROM t ORDER BY x");
                panic!("WAL read limit {limit}: open succeeded with rows {rows:?}");
            }
        }
        io.limit_wal_reads(usize::MAX);
        let db =
            Database::open(io.clone(), path, OpenOptions::new(Arc::new(SqliteDialect))).unwrap();
        let conn = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        let rows: Vec<(i64,)> = conn.exec_rows("SELECT x FROM t ORDER BY x");
        assert_eq!(rows, vec![(1,), (2,), (3,), (4,)], "WAL read limit {limit}");
        if limit == 600 {
            conn.execute("INSERT INTO t VALUES (5)").unwrap();
            drop(conn);
            drop(db);
            let db = Database::open(io, path, OpenOptions::new(Arc::new(SqliteDialect))).unwrap();
            let conn = db
                .connect(ConnectOpts::new(SessionRole::Superuser))
                .unwrap();
            let rows: Vec<(i64,)> = conn.exec_rows("SELECT x FROM t ORDER BY x");
            assert_eq!(
                rows,
                vec![(1,), (2,), (3,), (4,), (5,)],
                "rows after writing and reopening"
            );
        }
    }
}
