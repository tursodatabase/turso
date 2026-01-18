use crate::common::{do_flush, run_query, run_query_on_row, TempDatabase};
use rand::{rng, RngCore};
use std::panic;
use turso_core::{DatabaseOpts, Row};

const ENABLE_ENCRYPTION: bool = true;

// TODO: mvcc does not error here
#[turso_macros::test]
fn test_per_page_encryption(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let db_path = tmp_db.path.clone();
    let opts = tmp_db.db_opts;

    {
        let conn = tmp_db.connect_limbo();
        run_query(
            &tmp_db,
            &conn,
            "PRAGMA hexkey = 'b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327';",
        )?;
        run_query(&tmp_db, &conn, "PRAGMA cipher = 'aegis256';")?;
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
        //test connecting to the encrypted db using correct URI
        let uri = format!(
            "file:{}?cipher=aegis256&hexkey=b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327",
            db_path.to_str().unwrap()
        );
        let (_io, conn) = turso_core::Connection::from_uri(&uri, opts)?;
        let mut row_count = 0;
        run_query_on_row(&tmp_db, &conn, "SELECT * FROM test", |row: &Row| {
            assert_eq!(row.get::<i64>(0).unwrap(), 1);
            assert_eq!(row.get::<String>(1).unwrap(), "Hello, World!");
            row_count += 1;
        })?;
        assert_eq!(row_count, 1);
    }
    {
        //Try to create a table after reopening the encrypted db.
        let uri = format!(
            "file:{}?cipher=aegis256&hexkey=b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327",
            db_path.to_str().unwrap()
        );
        let (_io, conn) = turso_core::Connection::from_uri(&uri, opts)?;
        run_query(
            &tmp_db,
            &conn,
            "CREATE TABLE test1 (id INTEGER PRIMARY KEY, value TEXT);",
        )?;
        do_flush(&conn, &tmp_db)?;
    }
    {
        //Try to create a table after reopening the encrypted db.
        let uri = format!(
            "file:{}?cipher=aegis256&hexkey=b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327",
            db_path.to_str().unwrap()
        );
        let (_io, conn) = turso_core::Connection::from_uri(&uri, opts)?;
        run_query(
            &tmp_db,
            &conn,
            "INSERT INTO test1 (value) VALUES ('Hello, World!')",
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
        // test connecting to encrypted db using wrong key(key is ending with 77.The correct key is ending with 27).
        // With schema parsing during open, the wrong key causes decryption failure when reading page 1.
        let uri = format!(
            "file:{}?cipher=aegis256&hexkey=b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76377",
            db_path.to_str().unwrap()
        );
        let should_fail = panic::catch_unwind(panic::AssertUnwindSafe(|| {
            turso_core::Connection::from_uri(&uri, opts).unwrap();
        }));
        assert!(
            should_fail.is_err(),
            "should fail when opening encrypted DB with wrong key"
        );
    }
    {
        //test connecting to encrypted db using insufficient encryption parameters in URI.This should panic.
        let uri = format!("file:{}?cipher=aegis256", db_path.to_str().unwrap());
        let should_panic = panic::catch_unwind(panic::AssertUnwindSafe(|| {
            turso_core::Connection::from_uri(&uri, opts).unwrap();
        }));
        assert!(
            should_panic.is_err(),
            "should panic when accessing encrypted DB without passing hexkey in URI"
        );
    }
    {
        let uri = format!(
            "file:{}?hexkey=b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327",
            db_path.to_str().unwrap()
        );
        let should_panic = panic::catch_unwind(panic::AssertUnwindSafe(|| {
            turso_core::Connection::from_uri(&uri, opts).unwrap();
        }));
        assert!(
            should_panic.is_err(),
            "should panic when accessing encrypted DB without passing cipher in URI"
        );
    }
    {
        // Testing connecting to db without using URI.This should panic.
        let conn = tmp_db.connect_limbo();
        let should_panic = panic::catch_unwind(panic::AssertUnwindSafe(|| {
            run_query_on_row(&tmp_db, &conn, "SELECT * FROM test", |_row: &Row| {}).unwrap();
        }));
        assert!(
            should_panic.is_err(),
            "should panic when accessing encrypted DB without using URI"
        );
    }

    Ok(())
}

#[turso_macros::test(mvcc)]
fn test_non_4k_page_size_encryption(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let db_path = tmp_db.path.clone();

    {
        let conn = tmp_db.connect_limbo();
        // Set page size to 8k (8192 bytes) and test encryption. Default page size is 4k.
        run_query(&tmp_db, &conn, "PRAGMA page_size = 8192;")?;
        run_query(
            &tmp_db,
            &conn,
            "PRAGMA hexkey = 'b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327';",
        )?;
        run_query(&tmp_db, &conn, "PRAGMA cipher = 'aegis256';")?;
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
        // Reopen the existing db with 8k page size and test encryption
        let uri = format!(
            "file:{}?cipher=aegis256&hexkey=b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327",
            db_path.to_str().unwrap()
        );
        let (_io, conn) = turso_core::Connection::from_uri(
            &uri,
            DatabaseOpts::new().with_encryption(ENABLE_ENCRYPTION),
        )?;
        run_query_on_row(&tmp_db, &conn, "SELECT * FROM test", |row: &Row| {
            assert_eq!(row.get::<i64>(0).unwrap(), 1);
            assert_eq!(row.get::<String>(1).unwrap(), "Hello, World!");
        })?;
    }

    Ok(())
}

// TODO: mvcc for some reason does not error on corruption here
#[turso_macros::test]
fn test_corruption_turso_magic_bytes(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let db_path = tmp_db.path.clone();

    let opts = tmp_db.db_opts;

    {
        let conn = tmp_db.connect_limbo();
        run_query(
            &tmp_db,
            &conn,
            "PRAGMA hexkey = 'b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327';",
        )?;
        run_query(&tmp_db, &conn, "PRAGMA cipher = 'aegis256';")?;
        run_query(
            &tmp_db,
            &conn,
            "CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT);",
        )?;
        run_query(
            &tmp_db,
            &conn,
            "INSERT INTO test (value) VALUES ('Test corruption')",
        )?;
        run_query(&tmp_db, &conn, "PRAGMA wal_checkpoint(TRUNCATE);")?;
        do_flush(&conn, &tmp_db)?;
    }

    // corrupt the Turso magic bytes by changing "Turso" to "Vurso" (the db name as it was intended)
    {
        use std::fs::OpenOptions;
        use std::io::{Seek, SeekFrom, Write};

        let mut file = OpenOptions::new().write(true).open(&db_path)?;

        file.seek(SeekFrom::Start(0))?;
        file.write_all(b"V")?;
    }

    // try to connect to the corrupted database - this should return a decryption error
    // With schema parsing during open, the decryption failure happens when opening the database
    {
        let uri = format!(
            "file:{}?cipher=aegis256&hexkey=b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327",
            db_path.to_str().unwrap()
        );

        let result = turso_core::Connection::from_uri(&uri, opts);

        match result {
            Ok(_) => panic!(
                "should return error when opening encrypted DB with corrupted Turso magic bytes"
            ),
            Err(e) => {
                let err_msg = e.to_string();
                assert!(
                    err_msg.contains("Decryption failed"),
                    "error should indicate decryption failure, got: {err_msg}"
                );
            }
        }
    }

    Ok(())
}

#[turso_macros::test(mvcc)]
fn test_corruption_associated_data_bytes(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let db_path = tmp_db.path.clone();

    {
        let conn = tmp_db.connect_limbo();
        run_query(
            &tmp_db,
            &conn,
            "PRAGMA hexkey = 'b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327';",
        )?;
        run_query(&tmp_db, &conn, "PRAGMA cipher = 'aegis256';")?;
        run_query(
            &tmp_db,
            &conn,
            "CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT);",
        )?;
        run_query(
            &tmp_db,
            &conn,
            "INSERT INTO test (value) VALUES ('Test AD corruption')",
        )?;
        run_query(&tmp_db, &conn, "PRAGMA wal_checkpoint(TRUNCATE);")?;
        do_flush(&conn, &tmp_db)?;
    }

    // test corruption at different positions in the header (the first 100 bytes)
    let corruption_positions = [3, 7, 16, 30, 50, 70, 99];

    for &corrupt_pos in &corruption_positions {
        let test_db_name = format!(
            "test-corruption-ad-pos-{}-{}.db",
            corrupt_pos,
            rng().next_u32()
        );
        let test_tmp_db = TempDatabase::new(&test_db_name);
        let test_db_path = test_tmp_db.path.clone();
        std::fs::copy(&db_path, &test_db_path)?;

        // corrupt one byte
        {
            use std::fs::OpenOptions;
            use std::io::{Read, Seek, SeekFrom, Write};

            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&test_db_path)?;

            file.seek(SeekFrom::Start(corrupt_pos as u64))?;
            let mut original_byte = [0u8; 1];
            file.read_exact(&mut original_byte)?;

            // corrupt it by flipping all bits
            let corrupted_byte = [!original_byte[0]];

            file.seek(SeekFrom::Start(corrupt_pos as u64))?;
            file.write_all(&corrupted_byte)?;
        }

        // this should fail
        {
            let uri = format!(
                "file:{}?cipher=aegis256&hexkey=b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327",
                test_db_path.to_str().unwrap()
            );

            let should_panic = panic::catch_unwind(panic::AssertUnwindSafe(|| {
                let (_io, conn) = turso_core::Connection::from_uri(
                    &uri,
                    DatabaseOpts::new().with_encryption(ENABLE_ENCRYPTION),
                )
                .unwrap();
                run_query_on_row(&test_tmp_db, &conn, "SELECT * FROM test", |_row: &Row| {})
                    .unwrap();
            }));

            assert!(
                should_panic.is_err(),
                "should panic when accessing encrypted DB with corrupted associated data at position {corrupt_pos}",
            );
        }
    }

    Ok(())
}

#[turso_macros::test(mvcc)]
fn test_turso_header_structure(db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();

    let verify_header =
        |db_path: &str, expected_cipher_id: u8, description: &str| -> anyhow::Result<()> {
            use std::fs::File;
            use std::io::{Read, Seek, SeekFrom};

            let mut file = File::open(db_path)?;
            let mut header = [0u8; 16];
            file.seek(SeekFrom::Start(0))?;
            file.read_exact(&mut header)?;

            assert_eq!(
                &header[0..5],
                b"Turso",
                "Magic bytes should be 'Turso' for {description}"
            );
            assert_eq!(header[5], 0x00, "Version should be 0x00 for {description}");
            assert_eq!(
                header[6], expected_cipher_id,
                "Cipher ID should be {expected_cipher_id} for {description}"
            );

            // the unused bytes should be zeroed
            for (i, &byte) in header[7..16].iter().enumerate() {
                assert_eq!(
                    byte,
                    0,
                    "Unused byte at position {} should be 0 for {}",
                    i + 7,
                    description
                );
            }

            println!("Verified {} header: cipher ID = {}", description, header[6]);
            Ok(())
        };

    let test_cases = [
        (
            "aes128gcm",
            1,
            "AES-128-GCM",
            "b1bbfda4f589dc9daaf004fe21111e00",
        ),
        (
            "aes256gcm",
            2,
            "AES-256-GCM",
            "b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327",
        ),
        (
            "aegis256",
            3,
            "AEGIS-256",
            "b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327",
        ),
        (
            "aegis256x2",
            4,
            "AEGIS-256X2",
            "b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327",
        ),
        (
            "aegis128l",
            6,
            "AEGIS-128L",
            "b1bbfda4f589dc9daaf004fe21111e00",
        ),
        (
            "aegis128x2",
            7,
            "AEGIS-128X2",
            "b1bbfda4f589dc9daaf004fe21111e00",
        ),
        (
            "aegis128x4",
            8,
            "AEGIS-128X4",
            "b1bbfda4f589dc9daaf004fe21111e00",
        ),
    ];
    let opts = db.db_opts;
    let flags = db.db_flags;

    for (cipher_name, expected_id, description, hexkey) in test_cases {
        let tmp_db = TempDatabase::builder()
            .with_opts(opts)
            .with_flags(flags)
            .build();
        let db_path = tmp_db.path.clone();

        {
            let conn = tmp_db.connect_limbo();
            run_query(&tmp_db, &conn, &format!("PRAGMA hexkey = '{hexkey}';"))?;
            run_query(&tmp_db, &conn, &format!("PRAGMA cipher = '{cipher_name}';"))?;
            run_query(
                &tmp_db,
                &conn,
                "CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT);",
            )?;
            do_flush(&conn, &tmp_db)?;
        }

        verify_header(db_path.to_str().unwrap(), expected_id, description)?;
    }
    Ok(())
}

/// Regression test for database registry caching bug with encryption.
///
/// Previously, the DATABASE_MANAGER would cache Database instances by path and return
/// them on subsequent opens without validating encryption options. This meant:
/// 1. Open database with correct key -> Database cached with correct encryption_key
/// 2. Open database with WRONG key -> Cached Database returned, wrong key ignored!
/// 3. Decryption succeeds because cached Database has correct key
///
/// This test ensures that opening with wrong encryption key fails even after
/// the database has been opened with the correct key (which populates the cache).
///
/// Note: This test uses Database::open_file_with_flags directly (like JS bindings do)
/// rather than Connection::from_uri (which sets encryption via PRAGMA after connect,
/// accidentally working around the bug).
#[turso_macros::test]
fn test_encryption_key_validation_with_cached_database(tmp_db: TempDatabase) -> anyhow::Result<()> {
    use turso_core::{Database, EncryptionOpts, OpenFlags, PlatformIO};

    let _ = env_logger::try_init();
    let db_path = tmp_db.path.clone();
    let db_path_str = db_path.to_str().unwrap();

    let correct_key = "b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327";
    let wrong_key = "aaaaaaa4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327";

    let io = std::sync::Arc::new(PlatformIO::new()?);
    let opts = turso_core::DatabaseOpts::new().with_encryption(ENABLE_ENCRYPTION);

    // Step 1: Create encrypted database with correct key
    {
        let encryption_opts = Some(EncryptionOpts {
            cipher: "aegis256".to_string(),
            hexkey: correct_key.to_string(),
        });

        let db = Database::open_file_with_flags(
            io.clone(),
            db_path_str,
            OpenFlags::Create,
            opts,
            encryption_opts,
        )?;

        let conn = db.connect()?;
        conn.execute("CREATE TABLE secret_data (id INTEGER PRIMARY KEY, value TEXT)")?;
        conn.execute("INSERT INTO secret_data (value) VALUES ('top secret')")?;
        conn.query("PRAGMA wal_checkpoint(TRUNCATE)")?;
        do_flush(&conn, &tmp_db)?;
    }

    // Step 2: Re-open with correct key (this populates the DATABASE_MANAGER cache)
    {
        let encryption_opts = Some(EncryptionOpts {
            cipher: "aegis256".to_string(),
            hexkey: correct_key.to_string(),
        });

        let db = Database::open_file_with_flags(
            io.clone(),
            db_path_str,
            OpenFlags::default(),
            opts,
            encryption_opts,
        )?;

        let conn = db.connect()?;
        let mut row_count = 0;
        run_query_on_row(&tmp_db, &conn, "SELECT * FROM secret_data", |row: &Row| {
            assert_eq!(row.get::<String>(1).unwrap(), "top secret");
            row_count += 1;
        })?;
        assert_eq!(row_count, 1, "Should read data with correct key");
    }

    // Step 3: Try to open with WRONG key - this MUST fail
    // Before the fix, this would succeed because the cached Database had the correct key
    {
        let encryption_opts = Some(EncryptionOpts {
            cipher: "aegis256".to_string(),
            hexkey: wrong_key.to_string(),
        });

        let result = Database::open_file_with_flags(
            io.clone(),
            db_path_str,
            OpenFlags::default(),
            opts,
            encryption_opts,
        );

        match result {
            Ok(db) => {
                // Database opened, but query should fail with wrong key
                let conn = db.connect()?;
                let query_result =
                    run_query_on_row(&tmp_db, &conn, "SELECT * FROM secret_data", |_| {});

                assert!(
                    query_result.is_err(),
                    "Query should fail with wrong encryption key, but succeeded! \
                     This indicates the DATABASE_MANAGER cache bypass for encryption is broken."
                );
            }
            Err(e) => {
                // Database open failed - this is also acceptable
                let err_msg = e.to_string();
                assert!(
                    err_msg.contains("Decryption failed") || err_msg.contains("encryption"),
                    "Expected decryption error, got: {err_msg}"
                );
            }
        }
    }

    // Step 4: Verify correct key still works after wrong key attempt
    {
        let encryption_opts = Some(EncryptionOpts {
            cipher: "aegis256".to_string(),
            hexkey: correct_key.to_string(),
        });

        let db = Database::open_file_with_flags(
            io.clone(),
            db_path_str,
            OpenFlags::default(),
            opts,
            encryption_opts,
        )?;

        let conn = db.connect()?;
        let mut row_count = 0;
        run_query_on_row(&tmp_db, &conn, "SELECT * FROM secret_data", |row: &Row| {
            assert_eq!(row.get::<String>(1).unwrap(), "top secret");
            row_count += 1;
        })?;
        assert_eq!(
            row_count, 1,
            "Should still read data with correct key after wrong key attempt"
        );
    }

    Ok(())
}
