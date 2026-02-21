use crate::common::{do_flush, run_query, run_query_on_row, TempDatabase};
use rand::{rng, RngCore};
use std::sync::Arc;
use turso_core::{
    CipherMode, Database, DatabaseOpts, EncryptionKey, EncryptionOpts, OpenFlags, PlatformIO, Row,
    IO,
};

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
        // test connecting to encrypted db using wrong key (key ends with 77, correct key ends with 27).
        let uri = format!(
            "file:{}?cipher=aegis256&hexkey=b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76377",
            db_path.to_str().unwrap()
        );
        let (_io, conn) = turso_core::Connection::from_uri(&uri, opts)?;
        let result = run_query_on_row(&tmp_db, &conn, "SELECT * FROM test", |_row: &Row| {});
        assert!(
            result.is_err(),
            "should return error when accessing encrypted DB with wrong key"
        );
    }
    {
        // test connecting to encrypted db using insufficient encryption parameters in URI.
        let uri = format!("file:{}?cipher=aegis256", db_path.to_str().unwrap());
        let result = turso_core::Connection::from_uri(&uri, opts);
        assert!(
            result.is_err(),
            "should return error when accessing encrypted DB without passing hexkey in URI"
        );
    }
    {
        let uri = format!(
            "file:{}?hexkey=b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327",
            db_path.to_str().unwrap()
        );
        let result = turso_core::Connection::from_uri(&uri, opts);
        assert!(
            result.is_err(),
            "should return error when accessing encrypted DB without passing cipher in URI"
        );
    }
    {
        // test connecting to encrypted db without using URI.
        let conn = tmp_db.connect_limbo();
        let result = run_query_on_row(&tmp_db, &conn, "SELECT * FROM test", |_row: &Row| {});
        assert!(
            result.is_err(),
            "should return error when accessing encrypted DB without using URI"
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
    {
        let uri = format!(
            "file:{}?cipher=aegis256&hexkey=b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327",
            db_path.to_str().unwrap()
        );

        let (_io, conn) = turso_core::Connection::from_uri(&uri, opts)?;
        let result = run_query_on_row(&tmp_db, &conn, "SELECT * FROM test", |_row: &Row| {});

        assert!(
            result.is_err(),
            "should return error when accessing encrypted DB with corrupted Turso magic bytes"
        );
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Decryption failed"),
            "error should indicate decryption failure, got: {err_msg}"
        );
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

            let result = (|| -> anyhow::Result<()> {
                let (_io, conn) = turso_core::Connection::from_uri(
                    &uri,
                    DatabaseOpts::new().with_encryption(ENABLE_ENCRYPTION),
                )?;
                run_query_on_row(&test_tmp_db, &conn, "SELECT * FROM test", |_row: &Row| {})?;
                Ok(())
            })();

            assert!(
                result.is_err(),
                "should return error when accessing encrypted DB with corrupted associated data at position {corrupt_pos}",
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

/// this is a smoll test for database registry caching encryption keys
///
/// Previously, the DATABASE_MANAGER cached Database instances with keys. Which led to:
/// 1. Open database with correct key -> Database cached with correct encryption_key
/// 2. Open database with WRONG key or no key -> Cached Database returned
/// 3. Decryption succeeds because cached Database has correct key
///
/// This test ensures that opening with wrong encryption key (or no key) fails even after
/// the database has been opened with the correct key (which populates the cache).
#[turso_macros::test(mvcc)]
fn test_encryption_key_validation_with_cached_database(_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();

    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir
        .path()
        .join(format!("test-enc-cache-{}.db", rng().next_u32()));
    let db_path_str = db_path.to_str().unwrap();

    let correct_key = "b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327";
    let wrong_key = "aaaaaaa4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327";

    let io = Arc::new(PlatformIO::new()?);
    let opts = DatabaseOpts::new().with_encryption(ENABLE_ENCRYPTION);

    let correct_encryption_opts = Some(EncryptionOpts {
        cipher: "aegis256".to_string(),
        hexkey: correct_key.to_string(),
    });

    let main_db = Database::open_file_with_flags(
        io.clone(),
        db_path_str,
        OpenFlags::Create,
        opts,
        correct_encryption_opts.clone(),
    )?;

    // step 1: Create encrypted database with correct key
    {
        let correct_encryption_key =
            turso_core::EncryptionKey::from_hex_string(correct_key).unwrap();

        let conn = main_db.connect()?;
        conn.set_encryption_cipher(turso_core::CipherMode::Aegis256)?;
        conn.set_encryption_key(correct_encryption_key)?;

        conn.execute("CREATE TABLE secret_data (id INTEGER PRIMARY KEY, value TEXT)")?;
        conn.execute("INSERT INTO secret_data (value) VALUES ('top secret')")?;
        conn.query("PRAGMA wal_checkpoint(TRUNCATE)")?;
        for completion in conn.cacheflush()? {
            io.wait_for_completion(completion)?;
        }
    }

    // Step 2: re-open with correct key (this uses the DATABASE_MANAGER cache)
    {
        let correct_encryption_key =
            turso_core::EncryptionKey::from_hex_string(correct_key).unwrap();

        let db = Database::open_file_with_flags(
            io.clone(),
            db_path_str,
            OpenFlags::default(),
            opts,
            correct_encryption_opts.clone(),
        )?;

        let conn = db.connect()?;
        conn.set_encryption_cipher(turso_core::CipherMode::Aegis256)?;
        conn.set_encryption_key(correct_encryption_key)?;

        let rows = conn.query("SELECT * FROM secret_data")?;
        let mut row_count = 0;
        if let Some(mut rows) = rows {
            loop {
                match rows.step()? {
                    turso_core::StepResult::Row => {
                        let row = rows.row().unwrap();
                        assert_eq!(row.get::<String>(1).unwrap(), "top secret");
                        row_count += 1;
                    }
                    turso_core::StepResult::Done => break,
                    turso_core::StepResult::Interrupt => break,
                    turso_core::StepResult::Busy | turso_core::StepResult::IO => continue,
                }
            }
        }
        assert_eq!(row_count, 1, "Should read data with correct key");
    }

    // Step 3: Opening with wrong key succeeds, but reading data fails with decryption error
    {
        let wrong_encryption_key = turso_core::EncryptionKey::from_hex_string(wrong_key).unwrap();

        let db = Database::open_file_with_flags(
            io.clone(),
            db_path_str,
            OpenFlags::default(),
            opts,
            Some(EncryptionOpts {
                cipher: "aegis256".to_string(),
                hexkey: wrong_key.to_string(),
            }),
        )?;

        // opening succeeds - the key is not validated at open time
        let conn = db.connect()?;
        conn.set_encryption_cipher(turso_core::CipherMode::Aegis256)?;
        conn.set_encryption_key(wrong_encryption_key)?;

        // Reading data should fail with a decryption error
        let read_failed = match conn.query("SELECT * FROM secret_data") {
            Err(_) => true,
            Ok(Some(mut rows)) => loop {
                match rows.step() {
                    Err(_) => break true,                            // Error - read failed
                    Ok(turso_core::StepResult::Done) => break false, // Completed without error
                    Ok(turso_core::StepResult::Interrupt) => break false,
                    Ok(turso_core::StepResult::Row) => break false, // Got data - unexpected!!
                    Ok(turso_core::StepResult::Busy) | Ok(turso_core::StepResult::IO) => continue,
                }
            },
            Ok(None) => false,
        };
        assert!(
            read_failed,
            "Reading data with wrong key should fail with decryption error"
        );
    }

    // Step 4: Opening without encryption options should fail immediately
    {
        let result = Database::open_file_with_flags(
            io.clone(),
            db_path_str,
            OpenFlags::default(),
            opts,
            None,
        );

        assert!(
            result.is_err(),
            "Opening encrypted database without encryption options should fail"
        );
        let err = result.unwrap_err();
        assert!(
            err.to_string()
                .contains("Database is encrypted but no encryption options provided"),
            "Error message should indicate missing encryption options"
        );
    }

    // Step 5: verify correct key still works after wrong key attempt
    {
        let correct_encryption_key =
            turso_core::EncryptionKey::from_hex_string(correct_key).unwrap();

        let db = Database::open_file_with_flags(
            io.clone(),
            db_path_str,
            OpenFlags::default(),
            opts,
            correct_encryption_opts.clone(),
        )?;

        let conn = db.connect()?;
        conn.set_encryption_cipher(turso_core::CipherMode::Aegis256)?;
        conn.set_encryption_key(correct_encryption_key)?;

        let rows = conn.query("SELECT * FROM secret_data")?;
        let mut row_count = 0;
        if let Some(mut rows) = rows {
            loop {
                match rows.step()? {
                    turso_core::StepResult::Row => {
                        let row = rows.row().unwrap();
                        assert_eq!(row.get::<String>(1).unwrap(), "top secret");
                        row_count += 1;
                    }
                    turso_core::StepResult::Done => break,
                    turso_core::StepResult::Interrupt => break,
                    turso_core::StepResult::Busy | turso_core::StepResult::IO => continue,
                }
            }
        }
        assert_eq!(
            row_count, 1,
            "Should still read data with correct key after wrong key attempt"
        );
    }
    Ok(())
}

// Two different keys/ciphers for the two attached databases
const KEY_A: &str = "b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327";
const CIPHER_A: &str = "aegis256";
const KEY_B: &str = "a2ccfeb5f690ed0ebf01a843f22222f11ed11d9348213f6d8113b677ae84ad38";
const CIPHER_B: &str = "aes256gcm";

/// Helper: create an encrypted database file with the given cipher, hexkey, table name, and value.
/// Returns the file path.
fn create_encrypted_db(
    cipher: &str,
    hexkey: &str,
    table_name: &str,
    value: &str,
) -> anyhow::Result<std::path::PathBuf> {
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir
        .path()
        .join(format!("enc-{}-{}.db", table_name, rng().next_u32()));
    let db_path_str = db_path.to_str().unwrap();

    let io: Arc<dyn IO + Send> = Arc::new(PlatformIO::new()?);
    let opts = DatabaseOpts::new().with_encryption(true);
    let encryption_opts = Some(EncryptionOpts {
        cipher: cipher.to_string(),
        hexkey: hexkey.to_string(),
    });

    let db = Database::open_file_with_flags(
        io.clone(),
        db_path_str,
        OpenFlags::Create,
        opts,
        encryption_opts,
    )?;

    let conn = db.connect()?;
    let cipher_mode = CipherMode::try_from(cipher)?;
    let key = EncryptionKey::from_hex_string(hexkey)?;
    conn.set_encryption_cipher(cipher_mode)?;
    conn.set_encryption_key(key)?;

    conn.execute(format!(
        "CREATE TABLE {table_name} (id INTEGER PRIMARY KEY, value TEXT)"
    ))?;
    conn.execute(format!(
        "INSERT INTO {table_name} (value) VALUES ('{value}')"
    ))?;
    conn.query("PRAGMA wal_checkpoint(TRUNCATE)")?;
    for c in conn.cacheflush()? {
        io.wait_for_completion(c)?;
    }

    // Keep the temp dir alive by leaking it (the test process will clean up)
    std::mem::forget(temp_dir);
    Ok(db_path)
}

/// Helper: open a plain (unencrypted) main database with attach + encryption enabled.
fn open_main_db() -> anyhow::Result<(TempDatabase, Arc<turso_core::Connection>)> {
    let tmp_db = TempDatabase::builder()
        .with_opts(DatabaseOpts::new().with_encryption(true).with_attach(true))
        .build();
    let conn = tmp_db.connect_limbo();
    Ok((tmp_db, conn))
}

#[turso_macros::test(mvcc)]
fn test_attach_encrypted_database(_tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();

    // Create two encrypted databases with different keys and ciphers
    let path_a = create_encrypted_db(CIPHER_A, KEY_A, "secret_a", "data from A")?;
    let path_b = create_encrypted_db(CIPHER_B, KEY_B, "secret_b", "data from B")?;

    // --- Test 1: Happy path — attach both with correct keys ---
    {
        let (main_db, conn) = open_main_db()?;
        let attach_a = format!(
            "ATTACH 'file:{}?cipher={}&hexkey={}' AS aux_a",
            path_a.to_str().unwrap(),
            CIPHER_A,
            KEY_A
        );
        let attach_b = format!(
            "ATTACH 'file:{}?cipher={}&hexkey={}' AS aux_b",
            path_b.to_str().unwrap(),
            CIPHER_B,
            KEY_B
        );
        run_query(&main_db, &conn, &attach_a)?;
        run_query(&main_db, &conn, &attach_b)?;

        let mut row_count = 0;
        run_query_on_row(
            &main_db,
            &conn,
            "SELECT value FROM aux_a.secret_a",
            |row: &Row| {
                assert_eq!(row.get::<String>(0).unwrap(), "data from A");
                row_count += 1;
            },
        )?;
        assert_eq!(row_count, 1, "Should read one row from aux_a");

        let mut row_count = 0;
        run_query_on_row(
            &main_db,
            &conn,
            "SELECT value FROM aux_b.secret_b",
            |row: &Row| {
                assert_eq!(row.get::<String>(0).unwrap(), "data from B");
                row_count += 1;
            },
        )?;
        assert_eq!(row_count, 1, "Should read one row from aux_b");
    }

    // --- Test 2: Wrong key for db_a — use key_b instead ---
    {
        let (main_db, conn) = open_main_db()?;
        let attach_wrong = format!(
            "ATTACH 'file:{}?cipher={}&hexkey={}' AS aux_a",
            path_a.to_str().unwrap(),
            CIPHER_A,
            KEY_B // wrong key!
        );
        // _init() decrypts page 1 eagerly, so wrong key fails at attach time.
        let result = run_query(&main_db, &conn, &attach_wrong);
        assert!(
            result.is_err(),
            "ATTACH with wrong key should fail during page 1 decryption"
        );
    }

    // --- Test 3: Swapped keys — db_a gets key_b, db_b gets key_a ---
    {
        let (main_db, conn) = open_main_db()?;
        let attach_a_wrong = format!(
            "ATTACH 'file:{}?cipher={}&hexkey={}' AS aux_a",
            path_a.to_str().unwrap(),
            CIPHER_A,
            KEY_B
        );
        let attach_b_wrong = format!(
            "ATTACH 'file:{}?cipher={}&hexkey={}' AS aux_b",
            path_b.to_str().unwrap(),
            CIPHER_B,
            KEY_A
        );

        // Both attaches should fail eagerly during page 1 decryption.
        let result_a = run_query(&main_db, &conn, &attach_a_wrong);
        let result_b = run_query(&main_db, &conn, &attach_b_wrong);
        assert!(
            result_a.is_err(),
            "ATTACH db_a with key_b should fail during page 1 decryption"
        );
        assert!(
            result_b.is_err(),
            "ATTACH db_b with key_a should fail during page 1 decryption"
        );
    }

    // --- Test 4: Missing hexkey in URI ---
    {
        let (main_db, conn) = open_main_db()?;
        let attach_no_hexkey = format!(
            "ATTACH 'file:{}?cipher={}' AS aux_a",
            path_a.to_str().unwrap(),
            CIPHER_A
        );
        let result = run_query(&main_db, &conn, &attach_no_hexkey);
        assert!(
            result.is_err(),
            "ATTACH with cipher but no hexkey should fail"
        );
    }

    // --- Test 5: Missing cipher in URI ---
    {
        let (main_db, conn) = open_main_db()?;
        let attach_no_cipher = format!(
            "ATTACH 'file:{}?hexkey={}' AS aux_a",
            path_a.to_str().unwrap(),
            KEY_A
        );
        let result = run_query(&main_db, &conn, &attach_no_cipher);
        assert!(
            result.is_err(),
            "ATTACH with hexkey but no cipher should fail"
        );
    }

    // --- Test 6: No encryption params at all ---
    {
        let (main_db, conn) = open_main_db()?;
        let attach_no_enc = format!("ATTACH '{}' AS aux_a", path_a.to_str().unwrap());
        let result = run_query(&main_db, &conn, &attach_no_enc);
        // Opening an encrypted DB without key should fail
        assert!(
            result.is_err(),
            "ATTACH encrypted DB without key should fail"
        );
    }

    // --- Test 7: Correct key after wrong key attempt ---
    {
        let (main_db, conn) = open_main_db()?;

        // First, try wrong key
        let attach_wrong = format!(
            "ATTACH 'file:{}?cipher={}&hexkey={}' AS aux_a",
            path_a.to_str().unwrap(),
            CIPHER_A,
            KEY_B
        );
        let _ = run_query(&main_db, &conn, &attach_wrong);
        // Detach (ignore error if attach failed)
        let _ = run_query(&main_db, &conn, "DETACH aux_a");

        // Now attach with correct key
        let attach_correct = format!(
            "ATTACH 'file:{}?cipher={}&hexkey={}' AS aux_a",
            path_a.to_str().unwrap(),
            CIPHER_A,
            KEY_A
        );
        run_query(&main_db, &conn, &attach_correct)?;

        let mut row_count = 0;
        run_query_on_row(
            &main_db,
            &conn,
            "SELECT value FROM aux_a.secret_a",
            |row: &Row| {
                assert_eq!(row.get::<String>(0).unwrap(), "data from A");
                row_count += 1;
            },
        )?;
        assert_eq!(
            row_count, 1,
            "Should read data after re-attaching with correct key"
        );
    }

    // --- Test 8: Write to encrypted attached DB ---
    {
        let (main_db, conn) = open_main_db()?;
        let attach_a = format!(
            "ATTACH 'file:{}?cipher={}&hexkey={}' AS aux_a",
            path_a.to_str().unwrap(),
            CIPHER_A,
            KEY_A
        );
        run_query(&main_db, &conn, &attach_a)?;

        run_query(
            &main_db,
            &conn,
            "INSERT INTO aux_a.secret_a (value) VALUES ('new data')",
        )?;
        do_flush(&conn, &main_db)?;

        let mut values = Vec::new();
        run_query_on_row(
            &main_db,
            &conn,
            "SELECT value FROM aux_a.secret_a ORDER BY id",
            |row: &Row| {
                values.push(row.get::<String>(0).unwrap());
            },
        )?;
        assert_eq!(values.len(), 2, "Should have original + new row");
        assert_eq!(values[0], "data from A");
        assert_eq!(values[1], "new data");
    }

    // --- Test 9: Wrong cipher for db_a — use CIPHER_B instead of CIPHER_A ---
    {
        let (main_db, conn) = open_main_db()?;
        let attach_wrong_cipher = format!(
            "ATTACH 'file:{}?cipher={}&hexkey={}' AS aux_a",
            path_a.to_str().unwrap(),
            CIPHER_B, // wrong cipher!
            KEY_A
        );
        // _init() decrypts page 1 eagerly, so wrong cipher fails at attach time.
        let result = run_query(&main_db, &conn, &attach_wrong_cipher);
        assert!(
            result.is_err(),
            "ATTACH with wrong cipher should fail during page 1 decryption"
        );
    }

    // --- Test 10: Write to encrypted attached DB with wrong key ---
    {
        let (main_db, conn) = open_main_db()?;
        let attach_wrong = format!(
            "ATTACH 'file:{}?cipher={}&hexkey={}' AS aux_a",
            path_a.to_str().unwrap(),
            CIPHER_A,
            KEY_B // wrong key!
        );
        // _init() decrypts page 1 eagerly, so wrong key fails at attach time.
        let result = run_query(&main_db, &conn, &attach_wrong);
        assert!(
            result.is_err(),
            "ATTACH with wrong key should fail during page 1 decryption"
        );
    }

    // --- Test 11: Different ciphers on same connection ---
    {
        let (main_db, conn) = open_main_db()?;
        let attach_a = format!(
            "ATTACH 'file:{}?cipher={}&hexkey={}' AS aux_a",
            path_a.to_str().unwrap(),
            CIPHER_A,
            KEY_A
        );
        let attach_b = format!(
            "ATTACH 'file:{}?cipher={}&hexkey={}' AS aux_b",
            path_b.to_str().unwrap(),
            CIPHER_B,
            KEY_B
        );
        run_query(&main_db, &conn, &attach_a)?;
        run_query(&main_db, &conn, &attach_b)?;

        // Verify both are readable with their respective keys/ciphers
        let mut val_a = String::new();
        let mut val_b = String::new();
        run_query_on_row(
            &main_db,
            &conn,
            "SELECT value FROM aux_a.secret_a WHERE id = 1",
            |row: &Row| {
                val_a = row.get::<String>(0).unwrap();
            },
        )?;
        run_query_on_row(
            &main_db,
            &conn,
            "SELECT value FROM aux_b.secret_b WHERE id = 1",
            |row: &Row| {
                val_b = row.get::<String>(0).unwrap();
            },
        )?;
        assert_eq!(val_a, "data from A");
        assert_eq!(val_b, "data from B");
    }

    Ok(())
}
