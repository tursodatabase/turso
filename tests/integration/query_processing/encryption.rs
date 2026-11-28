use crate::common::{
    compute_dbhash, compute_dbhash_with_options, do_flush, run_query, run_query_on_row, ExecRows,
    TempDatabase, TempDatabaseBuilder,
};
use asserting::prelude::*;
use rand::{rng, RngCore};
use std::sync::Arc;
use turso_core::{
    CipherMode, ConnectOpts, Database, DatabaseOpts, EncryptionKey, EncryptionOpts, OpenFlags,
    OpenOptions, PlatformIO, Row, SessionRole, IO,
};
use turso_sqlite::SqliteDialect;

const ENABLE_ENCRYPTION: bool = true;

#[test]
fn invalid_encryption_options_are_rejected_before_creating_a_file() -> anyhow::Result<()> {
    let cases = [
        (CIPHER_A, "", "Hex string must decode"),
        (CIPHER_A, "   ", "Hex string must decode"),
        (CIPHER_A, "zz", "Invalid hex string"),
        // 16 bytes, but aegis256 needs a 32-byte key.
        (
            CIPHER_A,
            "000102030405060708090a0b0c0d0e0f",
            "Invalid key size",
        ),
        ("unknown-cipher", KEY_A, "Unknown cipher name"),
    ];
    for (index, (cipher, hexkey, expected)) in cases.into_iter().enumerate() {
        let directory = tempfile::tempdir()?;
        let encryption = EncryptionOpts {
            cipher: cipher.into(),
            hexkey: hexkey.into(),
        };

        let path = directory.path().join(format!("open-{index}.db"));
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new()?);
        let error = match Database::open(
            io,
            path.to_str().unwrap(),
            OpenOptions::new(Arc::new(SqliteDialect))
                .db_opts(DatabaseOpts::new().with_encryption(ENABLE_ENCRYPTION))
                .encryption(encryption.clone()),
        ) {
            Ok(_) => panic!("Database::open accepted cipher {cipher:?} with key {hexkey:?}"),
            Err(error) => error.to_string(),
        };
        assert!(error.contains(expected), "{error}");
        assert_no_database_files(&path);

        let path = directory.path().join(format!("uri-{index}.db"));
        let uri = format!(
            "file:{}?cipher={cipher}&hexkey={}",
            path.display(),
            hexkey.replace(' ', "%20")
        );
        let error = match turso_core::Connection::from_uri(
            &uri,
            DatabaseOpts::new().with_encryption(ENABLE_ENCRYPTION),
            Arc::new(SqliteDialect),
        ) {
            Ok(_) => panic!("Connection::from_uri accepted {uri}"),
            Err(error) => error.to_string(),
        };
        assert!(error.contains(expected), "{error}");
        assert_no_database_files(&path);

        // The SDK kit, which most bindings use, opens the file before it calls core.
        let path = directory.path().join(format!("sdk-kit-{index}.db"));
        let database =
            turso_sdk_kit::rsapi::TursoDatabase::new(turso_sdk_kit::rsapi::TursoDatabaseConfig {
                path: path.to_str().unwrap().to_string(),
                experimental_features: Some("encryption".to_string()),
                async_io: false,
                encryption: Some(encryption),
                vfs: turso_sdk_kit::IoBackend::Default,
                io: None,
                db_file: None,
                page_codec: None,
                open_flags: OpenFlags::default(),
            });
        let error = match database.open() {
            Ok(_) => panic!("the SDK kit accepted cipher {cipher:?} with key {hexkey:?}"),
            Err(error) => error.to_string(),
        };
        assert!(error.contains(expected), "{error}");
        assert_no_database_files(&path);
    }
    Ok(())
}

fn assert_no_database_files(path: &std::path::Path) {
    let wal = std::path::PathBuf::from(format!("{}-wal", path.display()));
    assert!(!path.exists(), "{} was created", path.display());
    assert!(!wal.exists(), "{} was created", wal.display());
}

fn attach_matrix_options(cipher: Option<&str>, mvcc: bool, key: &str) -> OpenOptions {
    OpenOptions::new(Arc::new(SqliteDialect))
        .db_opts(
            DatabaseOpts::new()
                .with_attach(true)
                .with_mvcc(mvcc)
                .with_encryption(cipher.is_some()),
        )
        .encryption(cipher.map(|cipher| EncryptionOpts {
            cipher: cipher.into(),
            hexkey: key.into(),
        }))
}

#[test]
fn authenticated_wal_import_preserves_target_encryption() -> anyhow::Result<()> {
    for cipher in [CIPHER_A, CIPHER_B] {
        let source = TempDatabaseBuilder::new()
            .with_encryption(cipher, KEY_A)
            .build();
        let writer = source.connect_limbo();
        writer.execute("CREATE TABLE secrets(value TEXT)")?;
        writer.execute("INSERT INTO secrets VALUES ('imported secret')")?;
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("imported.db");
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new()?);
        let target = Database::open(
            io.clone(),
            path.to_str().unwrap(),
            attach_matrix_options(Some(cipher), false, KEY_B),
        )?;
        let importer = target.connect(
            ConnectOpts::new(SessionRole::Superuser)
                .with_encryption_key(Some(EncryptionKey::from_hex_string(KEY_B)?)),
        )?;
        importer.execute("CREATE TABLE old(value)")?;
        importer.checkpoint(turso_core::CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        })?;
        importer.wal_insert_begin()?;
        let mut frame = vec![0; 24 + writer.get_page_size().get() as usize];
        for index in 1..=writer.wal_state()?.max_frame {
            writer.wal_get_frame(index, &mut frame)?;
            if u32::from_be_bytes(frame[..4].try_into().unwrap()) == 1 {
                let before = std::fs::read(format!("{}-wal", path.display()))?;
                // The SQLite magic, the page size, the reserved-space size, and
                // the reserved tail, where encryption writes its nonce and tag.
                for offset in [24, 24 + 16, 24 + 20, frame.len() - 1] {
                    frame[offset] ^= 1;
                    assert!(importer.wal_insert_frame(index, &frame).is_err());
                    assert_eq!(std::fs::read(format!("{}-wal", path.display()))?, before);
                    frame[offset] ^= 1;
                }
            }
            importer.wal_insert_frame(index, &frame)?;
        }
        importer.wal_insert_end(false)?;
        importer.force_reparse_schema()?;
        let rows: Vec<(String,)> = importer.exec_rows("SELECT value FROM secrets");
        assert_eq!(rows, [("imported secret".into(),)]);
        importer.close()?;
        drop((importer, target));
        let bytes = std::fs::read(&path)?;
        assert!(!bytes
            .windows(b"imported secret".len())
            .any(|value| value == b"imported secret"));
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new()?);
        assert!(Database::open(
            io.clone(),
            path.to_str().unwrap(),
            attach_matrix_options(Some(cipher), false, KEY_A)
        )
        .is_err());
        let reopened = Database::open(
            io,
            path.to_str().unwrap(),
            attach_matrix_options(Some(cipher), false, KEY_B),
        )?;
        let connection = reopened.connect(
            ConnectOpts::new(SessionRole::Superuser)
                .with_encryption_key(Some(EncryptionKey::from_hex_string(KEY_B)?)),
        )?;
        let rows: Vec<(String,)> = connection.exec_rows("SELECT value FROM secrets");
        assert_eq!(rows, [("imported secret".into(),)]);
    }
    Ok(())
}

fn run_non_4k_page_size_encryption_test(
    tmp_db: &TempDatabase,
    enable_mvcc: bool,
) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let db_path = tmp_db.path.clone();

    {
        let conn = tmp_db.connect_limbo();
        // Set page size to 8k (8192 bytes) and test encryption. Default page size is 4k.
        run_query(tmp_db, &conn, "PRAGMA page_size = 8192;")?;
        run_query(
            tmp_db,
            &conn,
            "PRAGMA hexkey = 'b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327';",
        )?;
        run_query(tmp_db, &conn, "PRAGMA cipher = 'aegis256';")?;
        if enable_mvcc {
            run_query(tmp_db, &conn, "PRAGMA journal_mode = 'mvcc';")?;
        }
        run_query(
            tmp_db,
            &conn,
            "CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT);",
        )?;
        run_query(
            tmp_db,
            &conn,
            "INSERT INTO test (value) VALUES ('Hello, World!')",
        )?;
        let mut row_count = 0;
        run_query_on_row(tmp_db, &conn, "SELECT * FROM test", |row: &Row| {
            assert_eq!(row.get::<i64>(0).unwrap(), 1);
            assert_eq!(row.get::<String>(1).unwrap(), "Hello, World!");
            row_count += 1;
        })?;

        assert_eq!(row_count, 1);
        do_flush(&conn, tmp_db)?;
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
            Arc::new(SqliteDialect),
        )?;
        run_query_on_row(tmp_db, &conn, "SELECT * FROM test", |row: &Row| {
            assert_eq!(row.get::<i64>(0).unwrap(), 1);
            assert_eq!(row.get::<String>(1).unwrap(), "Hello, World!");
        })?;
    }

    Ok(())
}

fn run_corruption_associated_data_bytes_test(
    tmp_db: &TempDatabase,
    enable_mvcc: bool,
) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let db_path = tmp_db.path.clone();

    {
        let conn = tmp_db.connect_limbo();
        run_query(
            tmp_db,
            &conn,
            "PRAGMA hexkey = 'b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327';",
        )?;
        run_query(tmp_db, &conn, "PRAGMA cipher = 'aegis256';")?;
        if enable_mvcc {
            run_query(tmp_db, &conn, "PRAGMA journal_mode = 'mvcc';")?;
        }
        run_query(
            tmp_db,
            &conn,
            "CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT);",
        )?;
        run_query(
            tmp_db,
            &conn,
            "INSERT INTO test (value) VALUES ('Test AD corruption')",
        )?;
        run_query(tmp_db, &conn, "PRAGMA wal_checkpoint(TRUNCATE);")?;
        do_flush(&conn, tmp_db)?;
    }

    // test corruption at different positions in the header (the first 100 bytes)
    let corruption_positions = [3, 7, 16, 30, 50, 70, 99];

    for &corrupt_pos in &corruption_positions {
        let test_tmp_db = TempDatabaseBuilder::new().build();
        let test_db_path = test_tmp_db.path.clone();
        std::fs::copy(&db_path, &test_db_path)?;

        {
            // corrupt one byte
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

        let uri = format!(
            "file:{}?cipher=aegis256&hexkey=b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327",
            test_db_path.to_str().unwrap()
        );

        let (_io, conn) = turso_core::Connection::from_uri(
            &uri,
            DatabaseOpts::new().with_encryption(ENABLE_ENCRYPTION),
            Arc::new(SqliteDialect),
        )
        .expect("opening the corrupted DB should not fail at the URI level");

        let result = run_query_on_row(tmp_db, &conn, "SELECT * FROM test", |_row: &Row| {});

        assert_that!(result)
            .described_as(format!("accessing an encrypted database with corrupted associated data at position {corrupt_pos}"))
            .is_err();
    }

    Ok(())
}

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
        let (_io, conn) = turso_core::Connection::from_uri(&uri, opts, Arc::new(SqliteDialect))?;
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
        let (_io, conn) = turso_core::Connection::from_uri(&uri, opts, Arc::new(SqliteDialect))?;
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
        let (_io, conn) = turso_core::Connection::from_uri(&uri, opts, Arc::new(SqliteDialect))?;
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
        let (_io, conn) = turso_core::Connection::from_uri(&uri, opts, Arc::new(SqliteDialect))?;
        let result = run_query_on_row(&tmp_db, &conn, "SELECT * FROM test", |_row: &Row| {});
        assert_that!(result)
            .described_as("accessing an encrypted database with the wrong key")
            .is_err();
    }
    {
        // test connecting to encrypted db using insufficient encryption parameters in URI.
        let uri = format!("file:{}?cipher=aegis256", db_path.to_str().unwrap());
        // `from_uri` returns a connection handle that is not Debug, so the
        // result cannot go through `assert_that!`.
        assert!(
            turso_core::Connection::from_uri(&uri, opts, Arc::new(SqliteDialect)).is_err(),
            "opening an encrypted database without a hexkey in the URI must fail"
        );
    }
    {
        let uri = format!(
            "file:{}?hexkey=b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327",
            db_path.to_str().unwrap()
        );
        assert!(
            turso_core::Connection::from_uri(&uri, opts, Arc::new(SqliteDialect)).is_err(),
            "opening an encrypted database without a cipher in the URI must fail"
        );
    }
    {
        // test connecting to encrypted db without using URI.
        let conn = tmp_db.connect_limbo();
        let result = run_query_on_row(&tmp_db, &conn, "SELECT * FROM test", |_row: &Row| {});
        assert_that!(result)
            .described_as("accessing an encrypted database without using the URI")
            .is_err();
    }

    Ok(())
}

#[turso_macros::test]
fn test_non_4k_page_size_encryption(tmp_db: TempDatabase) -> anyhow::Result<()> {
    run_non_4k_page_size_encryption_test(&tmp_db, false)
}

#[turso_macros::test]
fn test_non_4k_page_size_encryption_mvcc(tmp_db: TempDatabase) -> anyhow::Result<()> {
    run_non_4k_page_size_encryption_test(&tmp_db, true)
}

#[turso_macros::test]
fn test_mvcc_rejects_late_encryption_pragmas(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    run_query(&tmp_db, &conn, "PRAGMA journal_mode = 'mvcc';")?;

    // Insert data before the (rejected) late encryption pragmas.
    run_query(
        &tmp_db,
        &conn,
        "CREATE TABLE pre (id INTEGER PRIMARY KEY, v TEXT);",
    )?;
    run_query(
        &tmp_db,
        &conn,
        "INSERT INTO pre (v) VALUES ('before_late_pragma')",
    )?;

    assert_that!(run_query(
        &tmp_db,
        &conn,
        "PRAGMA hexkey = 'b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327';",
    ))
    .err()
    .display_string()
    .contains("configure encryption before PRAGMA journal_mode='mvcc'");

    assert_that!(run_query(&tmp_db, &conn, "PRAGMA cipher = 'aegis256';"))
        .err()
        .display_string()
        .contains("configure encryption before PRAGMA journal_mode='mvcc'");

    // Data inserted before the rejected pragmas must still be readable.
    let mut pre_count = 0;
    run_query_on_row(&tmp_db, &conn, "SELECT v FROM pre", |row: &Row| {
        assert_eq!(row.get::<String>(0).unwrap(), "before_late_pragma");
        pre_count += 1;
    })?;
    assert_eq!(pre_count, 1);

    run_query(
        &tmp_db,
        &conn,
        "CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT);",
    )?;
    run_query(
        &tmp_db,
        &conn,
        "INSERT INTO test (value) VALUES ('still plaintext')",
    )?;
    do_flush(&conn, &tmp_db)?;

    let reopened = tmp_db.connect_limbo();
    let mut row_count = 0;
    run_query_on_row(&tmp_db, &reopened, "SELECT value FROM test", |row: &Row| {
        assert_eq!(row.get::<String>(0).unwrap(), "still plaintext");
        row_count += 1;
    })?;
    assert_eq!(row_count, 1);

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

        let (_io, conn) = turso_core::Connection::from_uri(&uri, opts, Arc::new(SqliteDialect))?;
        let result = run_query_on_row(&tmp_db, &conn, "SELECT * FROM test", |_row: &Row| {});

        assert_that!(result)
            .described_as("accessing an encrypted database with corrupted Turso magic bytes")
            .err()
            .display_string()
            .contains("Decryption failed");
    }

    Ok(())
}

#[turso_macros::test]
fn test_corruption_associated_data_bytes(tmp_db: TempDatabase) -> anyhow::Result<()> {
    run_corruption_associated_data_bytes_test(&tmp_db, false)
}

#[turso_macros::test]
fn test_corruption_associated_data_bytes_mvcc(tmp_db: TempDatabase) -> anyhow::Result<()> {
    run_corruption_associated_data_bytes_test(&tmp_db, true)
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
        Arc::new(SqliteDialect),
    )?;

    // step 1: Create encrypted database with correct key
    {
        let correct_encryption_key =
            turso_core::EncryptionKey::from_hex_string(correct_key).unwrap();

        let conn = main_db.connect(ConnectOpts::new(SessionRole::Superuser))?;
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
            Arc::new(SqliteDialect),
        )?;

        let conn = db.connect(ConnectOpts::new(SessionRole::Superuser))?;
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
                    turso_core::StepResult::Busy
                    | turso_core::StepResult::IO
                    | turso_core::StepResult::Yield
                    | turso_core::StepResult::Sleep { .. } => continue,
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
            Arc::new(SqliteDialect),
        )?;

        // opening succeeds - the key is not validated at open time
        let conn = db.connect(ConnectOpts::new(SessionRole::Superuser))?;
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
                    Ok(turso_core::StepResult::Busy)
                    | Ok(turso_core::StepResult::IO)
                    | Ok(turso_core::StepResult::Yield)
                    | Ok(turso_core::StepResult::Sleep { .. }) => continue,
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
            Arc::new(SqliteDialect),
        );

        assert_that!(result)
            .described_as("opening an encrypted database with no encryption options")
            .err()
            .display_string()
            .contains("Database is encrypted but no encryption options provided");
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
            Arc::new(SqliteDialect),
        )?;

        let conn = db.connect(ConnectOpts::new(SessionRole::Superuser))?;
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
                    turso_core::StepResult::Busy
                    | turso_core::StepResult::IO
                    | turso_core::StepResult::Yield
                    | turso_core::StepResult::Sleep { .. } => continue,
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
/// The returned `TempDir` deletes the database directory when it drops, so
/// callers must hold it for as long as they use the database.
fn create_encrypted_db(
    cipher: &str,
    hexkey: &str,
    table_name: &str,
    value: &str,
) -> anyhow::Result<(std::path::PathBuf, tempfile::TempDir)> {
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
        Arc::new(SqliteDialect),
    )?;

    let conn = db.connect(ConnectOpts::new(SessionRole::Superuser))?;
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

    Ok((db_path, temp_dir))
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
    let (path_a, _dir_a) = create_encrypted_db(CIPHER_A, KEY_A, "secret_a", "data from A")?;
    let (path_b, _dir_b) = create_encrypted_db(CIPHER_B, KEY_B, "secret_b", "data from B")?;

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
        // Attaching may succeed (key is not validated at open time), but reading must fail
        let attach_result = run_query(&main_db, &conn, &attach_wrong);
        if attach_result.is_ok() {
            let read_result = run_query_on_row(
                &main_db,
                &conn,
                "SELECT value FROM aux_a.secret_a",
                |_: &Row| {},
            );
            assert_that!(read_result)
                .described_as("reading an attached database with the wrong key")
                .is_err();
        }
        // If attach itself failed, that's also acceptable
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

        // Attach may succeed at open time; reading should fail for both
        let _ = run_query(&main_db, &conn, &attach_a_wrong);
        let _ = run_query(&main_db, &conn, &attach_b_wrong);

        let read_a = run_query_on_row(
            &main_db,
            &conn,
            "SELECT value FROM aux_a.secret_a",
            |_: &Row| {},
        );
        let read_b = run_query_on_row(
            &main_db,
            &conn,
            "SELECT value FROM aux_b.secret_b",
            |_: &Row| {},
        );
        assert!(
            read_a.is_err() || read_b.is_err(),
            "At least one read with swapped keys must fail"
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
        assert_that!(result)
            .described_as("ATTACH with a cipher but no hexkey")
            .is_err();
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
        assert_that!(result)
            .described_as("ATTACH with a hexkey but no cipher")
            .is_err();
    }

    // --- Test 6: No encryption params at all ---
    {
        let (main_db, conn) = open_main_db()?;
        let attach_no_enc = format!("ATTACH '{}' AS aux_a", path_a.to_str().unwrap());
        let result = run_query(&main_db, &conn, &attach_no_enc);
        // Opening an encrypted DB without key should fail
        assert_that!(result)
            .described_as("ATTACH of an encrypted database with no key at all")
            .is_err();
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

    // --- Test 9: Different ciphers on same connection ---
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

/// Test that VACUUM INTO on an encrypted database results in a clean, unencrypted
/// database that can be read without any encryption keys.
#[turso_macros::test]
fn test_vacuum_into_unencrypts(tmp_db: TempDatabase) -> anyhow::Result<()> {
    use tempfile::TempDir;

    let _ = env_logger::try_init();
    let hexkey = "b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327";
    let cipher = "aegis256";

    // 1. Create an encrypted source database and insert data
    {
        let conn = tmp_db.connect_limbo();
        conn.execute(format!("PRAGMA hexkey = '{hexkey}'"))?;
        conn.execute(format!("PRAGMA cipher = '{cipher}'"))?;

        conn.execute("CREATE TABLE secret_data (id INTEGER PRIMARY KEY, content TEXT)")?;
        conn.execute("INSERT INTO secret_data (content) VALUES ('this was encrypted')")?;
        do_flush(&conn, &tmp_db)?;
    }

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("exported.db");
    let dest_path_str = dest_path.to_str().unwrap();

    // 2. Demonstrate that the encrypted source CANNOT be read or vacuumed without keys
    {
        let unauthorized_db = TempDatabase::new_with_existent(&tmp_db.path);
        let unauthorized_conn = unauthorized_db.connect_limbo();

        // Reading should fail
        let result = unauthorized_conn.execute("SELECT * FROM secret_data");
        assert_that!(result)
            .described_as("reading an encrypted database as plaintext")
            .err()
            .display_string()
            .contains("Corrupt database");

        // VACUUM INTO should also fail because it cannot read the source schema/data
        let fail_path = dest_dir.path().join("should_fail.db");
        let fail_path_str = fail_path.to_str().unwrap();
        let result = unauthorized_conn.execute(format!("VACUUM INTO '{fail_path_str}'"));
        assert_that!(result)
            .described_as("VACUUM INTO from an encrypted database with no keys")
            .err()
            .display_string()
            .contains("Corrupt database");
    }

    // 3. Execute VACUUM INTO using an authorized connection
    {
        let conn = tmp_db.connect_limbo();
        conn.execute(format!("PRAGMA hexkey = '{hexkey}'"))?;
        conn.execute(format!("PRAGMA cipher = '{cipher}'"))?;
        conn.execute(format!("VACUUM INTO '{dest_path_str}'"))?;
    }

    // 4. Prove the destination IS readable without keys (it was unencrypted)
    {
        let dest_db = TempDatabase::new_with_existent(&dest_path);
        let dest_conn = dest_db.connect_limbo();

        let rows: Vec<(i64, String)> = dest_conn.exec_rows("SELECT id, content FROM secret_data");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1, "this was encrypted");
    }

    // 5. Prove the destination has the same content as the encrypted source.
    // Close the source first: it was encrypted with PRAGMAs, so its open `Database`
    // has no cipher set and cannot take a key from a new connection.
    let source_path = tmp_db.path.clone();
    drop(tmp_db);
    let source_hash = turso_dbhash::hash_database(
        source_path.to_str().unwrap(),
        &turso_dbhash::DbHashOptions {
            encryption: Some(EncryptionOpts {
                cipher: cipher.to_string(),
                hexkey: hexkey.to_string(),
            }),
            ..Default::default()
        },
    )?;
    assert_eq!(source_hash.rows_hashed, 1);
    let dest_db = TempDatabase::new_with_existent(&dest_path);
    assert_eq!(compute_dbhash(&dest_db).hash, source_hash.hash);

    Ok(())
}

#[test]
fn test_encrypted_db_then_enable_mvcc_large_payload_chunked() -> anyhow::Result<()> {
    let _ = env_logger::try_init();

    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("test.db");
    let db_path_str = db_path.to_str().unwrap();

    let hex_key = "b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327";
    let large_value = "x".repeat(100_000);
    let io = Arc::new(PlatformIO::new()?);
    let opts = DatabaseOpts::new().with_encryption(true);
    let enc_opts = Some(EncryptionOpts {
        cipher: "aes256gcm".to_string(),
        hexkey: hex_key.to_string(),
    });

    {
        let db = Database::open_file_with_flags(
            io.clone(),
            db_path_str,
            OpenFlags::Create,
            opts,
            enc_opts.clone(),
            Arc::new(SqliteDialect),
        )?;
        let key = EncryptionKey::from_hex_string(hex_key)?;
        let conn =
            db.connect(ConnectOpts::new(SessionRole::Superuser).with_encryption_key(Some(key)))?;
        conn.execute("PRAGMA journal_mode = 'mvcc'")?;
        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")?;
        conn.execute(format!("INSERT INTO test (value) VALUES ('{large_value}')"))?;
        for c in conn.cacheflush()? {
            io.wait_for_completion(c)?;
        }
    }

    let log_path = db_path.with_extension("db-log");
    let log_bytes = std::fs::read(&log_path)?;
    assert!(
        log_bytes.len() > 64 * 1024,
        "multi-chunk encrypted MVCC log should exceed 64 KiB"
    );

    {
        let db = Database::open_file_with_flags(
            io.clone(),
            db_path_str,
            OpenFlags::default(),
            opts,
            enc_opts,
            Arc::new(SqliteDialect),
        )?;
        let key = EncryptionKey::from_hex_string(hex_key)?;
        let conn =
            db.connect(ConnectOpts::new(SessionRole::Superuser).with_encryption_key(Some(key)))?;

        let rows: Vec<(i64, i64, String, String)> = conn.exec_rows(
            "SELECT id, length(value), substr(value, 1, 16), substr(value, length(value) - 15, 16) FROM test",
        );
        assert_eq!(
            rows.len(),
            1,
            "Should recover multi-chunk MVCC payload after restart"
        );
        assert_eq!(rows[0].0, 1);
        assert_eq!(rows[0].1, large_value.len() as i64);
        assert_eq!(rows[0].2, "xxxxxxxxxxxxxxxx");
        assert_eq!(rows[0].3, "xxxxxxxxxxxxxxxx");
    }

    Ok(())
}

/// Create an encrypted database with existing data, then switch to MVCC mode.
/// Verifies that pre-existing rows survive the journal mode switch and that
/// new MVCC writes produce an encrypted log.
#[test]
fn test_encrypted_db_with_data_then_enable_mvcc() -> anyhow::Result<()> {
    let _ = env_logger::try_init();

    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("test.db");
    let db_path_str = db_path.to_str().unwrap();

    let hex_key = "b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327";
    let io = Arc::new(PlatformIO::new()?);
    let opts = DatabaseOpts::new().with_encryption(true);
    let enc_opts = Some(EncryptionOpts {
        cipher: "aes256gcm".to_string(),
        hexkey: hex_key.to_string(),
    });

    // Phase 1: Create encrypted DB in WAL mode, insert data
    {
        let db = Database::open_file_with_flags(
            io.clone(),
            db_path_str,
            OpenFlags::Create,
            opts,
            enc_opts.clone(),
            Arc::new(SqliteDialect),
        )?;
        let key = EncryptionKey::from_hex_string(hex_key)?;
        let conn =
            db.connect(ConnectOpts::new(SessionRole::Superuser).with_encryption_key(Some(key)))?;
        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")?;
        conn.execute("INSERT INTO test (value) VALUES ('before mvcc')")?;
        for c in conn.cacheflush()? {
            io.wait_for_completion(c)?;
        }
    }

    // Phase 2: Reopen, switch to MVCC, insert more data
    {
        let db = Database::open_file_with_flags(
            io.clone(),
            db_path_str,
            OpenFlags::default(),
            opts,
            enc_opts.clone(),
            Arc::new(SqliteDialect),
        )?;
        let key = EncryptionKey::from_hex_string(hex_key)?;
        let conn =
            db.connect(ConnectOpts::new(SessionRole::Superuser).with_encryption_key(Some(key)))?;

        // Pre-existing row should be readable
        let rows: Vec<(String,)> = conn.exec_rows("SELECT value FROM test");
        assert_eq!(rows.len(), 1, "pre-existing row must be readable");
        assert_eq!(rows[0].0, "before mvcc");

        // Switch to MVCC
        conn.execute("PRAGMA journal_mode = 'mvcc'")?;
        conn.execute("INSERT INTO test (value) VALUES ('after mvcc')")?;
        for c in conn.cacheflush()? {
            io.wait_for_completion(c)?;
        }
    }

    // Verify the MVCC log is encrypted
    {
        let log_path = db_path.with_extension("db-log");
        let log_bytes = std::fs::read(&log_path)?;
        assert!(
            log_bytes.len() > 56,
            "MVCC log should contain data beyond the header"
        );
        for plaintext in [b"before mvcc" as &[u8], b"after mvcc"] {
            assert!(
                !log_bytes.windows(plaintext.len()).any(|w| w == plaintext),
                "MVCC log must not contain plaintext '{}' when encryption is enabled",
                std::str::from_utf8(plaintext).unwrap()
            );
        }
    }

    // Phase 3: Reopen — MVCC recovery should replay, both rows visible
    {
        let db = Database::open_file_with_flags(
            io.clone(),
            db_path_str,
            OpenFlags::default(),
            opts,
            enc_opts.clone(),
            Arc::new(SqliteDialect),
        )?;
        let key = EncryptionKey::from_hex_string(hex_key)?;
        let conn =
            db.connect(ConnectOpts::new(SessionRole::Superuser).with_encryption_key(Some(key)))?;

        let rows: Vec<(String,)> = conn.exec_rows("SELECT value FROM test ORDER BY id");
        assert_eq!(
            rows.iter().map(|r| r.0.as_str()).collect::<Vec<_>>(),
            vec!["before mvcc", "after mvcc"]
        );
    }

    // Phase 4: dbhash reads the encrypted MVCC database and gets the same hash for `test`
    // as a plain database with the same rows. The filter leaves out the MVCC metadata table.
    let plain_db = TempDatabase::new_empty();
    let plain_conn = plain_db.connect_limbo();
    plain_conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")?;
    plain_conn.execute("INSERT INTO test (value) VALUES ('before mvcc'), ('after mvcc')")?;
    let hash_opts = turso_dbhash::DbHashOptions {
        table_filter: Some("test".to_string()),
        ..Default::default()
    };
    let encrypted_hash = turso_dbhash::hash_database(
        db_path_str,
        &turso_dbhash::DbHashOptions {
            encryption: enc_opts,
            ..hash_opts.clone()
        },
    )?;
    assert_eq!(encrypted_hash.rows_hashed, 2);
    assert_eq!(
        encrypted_hash.hash,
        compute_dbhash_with_options(&plain_db, &hash_opts).hash
    );

    Ok(())
}

#[turso_macros::test]
fn test_non_4k_page_size_encryption_enable_mvcc_after_encryption(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    [
        "PRAGMA page_size = 8192;",
        "PRAGMA hexkey = 'b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327';",
        "PRAGMA cipher = 'aegis256';",
        "PRAGMA journal_mode = 'mvcc';",
        "CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT);",
        "INSERT INTO test (value) VALUES ('Hello, World!')",
    ]
    .iter()
    .try_for_each(|query| run_query(&tmp_db, &conn, query))?;
    do_flush(&conn, &tmp_db)
}

// Regression coverage for https://github.com/tursodatabase/turso/issues/7375.

fn assert_encrypted_page_size_after_key_and_cipher(
    page_size: i64,
    cipher: &str,
    hexkey: &str,
) -> anyhow::Result<()> {
    let tmp_db = TempDatabaseBuilder::new()
        .with_opts(DatabaseOpts::new().with_encryption(true))
        .build();

    let conn = tmp_db.connect_limbo();
    conn.execute(format!("PRAGMA cipher = '{cipher}'"))?;
    conn.execute(format!("PRAGMA hexkey = '{hexkey}'"))?;
    conn.execute(format!("PRAGMA page_size = {page_size}"))?;
    conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY, b BLOB)")?;
    conn.execute("INSERT INTO t VALUES(1, randomblob(300))")?;

    let rows: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM t");
    assert_eq!(rows[0].0, 1, "row count mismatch for page_size={page_size}");
    let ps: Vec<(i64,)> = conn.exec_rows("PRAGMA page_size");
    assert_eq!(ps[0].0, page_size, "page_size readback mismatch");

    do_flush(&conn, &tmp_db)?;

    let uri = format!(
        "file:{}?cipher={cipher}&hexkey={hexkey}",
        tmp_db.path.to_str().unwrap()
    );
    let (_io, reopened) = turso_core::Connection::from_uri(
        &uri,
        DatabaseOpts::new().with_encryption(true),
        Arc::new(SqliteDialect),
    )?;
    let rows: Vec<(i64,)> = reopened.exec_rows("SELECT count(*) FROM t");
    assert_eq!(
        rows[0].0, 1,
        "row count after reopen mismatch for page_size={page_size}"
    );

    Ok(())
}

#[turso_macros::test]
fn test_encrypted_page_size_after_key_and_cipher(_tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    for page_size in [512, 4096, 65536] {
        assert_encrypted_page_size_after_key_and_cipher(page_size, CIPHER_A, KEY_A)?;
    }
    // Exercise a cipher that uses a 16-byte key and different metadata size.
    let aes128_key = &KEY_A[..32];
    assert_encrypted_page_size_after_key_and_cipher(512, "aes128gcm", aes128_key)?;
    Ok(())
}

#[turso_macros::test]
fn test_uri_encryption_then_page_size(_tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let tmp_db = TempDatabaseBuilder::new()
        .with_opts(DatabaseOpts::new().with_encryption(true))
        .build();

    let uri = format!(
        "file:{}?cipher={CIPHER_A}&hexkey={KEY_A}",
        tmp_db.path.to_str().unwrap()
    );

    {
        let (io, conn) = turso_core::Connection::from_uri(
            &uri,
            DatabaseOpts::new().with_encryption(true),
            Arc::new(SqliteDialect),
        )?;
        conn.execute("PRAGMA page_size = 512")?;
        conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY, b BLOB)")?;
        conn.execute("INSERT INTO t VALUES(1, randomblob(300))")?;

        let rows: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM t");
        assert_eq!(rows[0].0, 1);
        let ps: Vec<(i64,)> = conn.exec_rows("PRAGMA page_size");
        assert_eq!(ps[0].0, 512);

        for c in conn.cacheflush()? {
            io.wait_for_completion(c)?;
        }
    }

    let (_io, reopened) = turso_core::Connection::from_uri(
        &uri,
        DatabaseOpts::new().with_encryption(true),
        Arc::new(SqliteDialect),
    )?;
    let rows: Vec<(i64,)> = reopened.exec_rows("SELECT count(*) FROM t");
    assert_eq!(rows[0].0, 1);
    let ps: Vec<(i64,)> = reopened.exec_rows("PRAGMA page_size");
    assert_eq!(ps[0].0, 512);

    Ok(())
}

#[turso_macros::test]
fn test_fresh_attach_encrypted_non_4k_page_size(_tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let aux_dir = tempfile::tempdir()?;
    let aux_path = aux_dir.path().join("aux.db");
    let aux_uri = format!(
        "file:{}?cipher={CIPHER_A}&hexkey={KEY_A}",
        aux_path.to_str().unwrap()
    );

    {
        let main_db = TempDatabaseBuilder::new()
            .with_opts(DatabaseOpts::new().with_encryption(true).with_attach(true))
            .build();
        let conn = main_db.connect_limbo();
        conn.execute("PRAGMA page_size = 512")?;
        conn.execute(format!("ATTACH '{aux_uri}' AS aux"))?;
        conn.execute("CREATE TABLE aux.t(a INTEGER PRIMARY KEY, b BLOB)")?;
        conn.execute("INSERT INTO aux.t VALUES(1, randomblob(300))")?;

        let rows: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM aux.t");
        assert_eq!(rows[0].0, 1);
        // Attached pager must inherit main's page size, not the 4096 default.
        let aux_ps: Vec<(i64,)> = conn.exec_rows("PRAGMA aux.page_size");
        assert_eq!(aux_ps[0].0, 512);

        // Force aux WAL onto its main file so the standalone reopen sees the row.
        conn.execute("PRAGMA aux.wal_checkpoint(TRUNCATE)")?;
        do_flush(&conn, &main_db)?;
    }

    let (_io, aux_conn) = turso_core::Connection::from_uri(
        &aux_uri,
        DatabaseOpts::new().with_encryption(true),
        Arc::new(SqliteDialect),
    )?;
    let rows: Vec<(i64,)> = aux_conn.exec_rows("SELECT count(*) FROM t");
    assert_eq!(rows[0].0, 1);
    let ps: Vec<(i64,)> = aux_conn.exec_rows("PRAGMA page_size");
    assert_eq!(ps[0].0, 512);

    Ok(())
}

#[turso_macros::test]
fn test_inplace_vacuum_non_4k_encryption(_tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let tmp_db = TempDatabaseBuilder::new()
        .with_opts(DatabaseOpts::new().with_encryption(true))
        .build();

    let conn = tmp_db.connect_limbo();
    // This test covers VACUUM, so create the source DB without using the issue
    // #7375 PRAGMA order.
    conn.execute("PRAGMA page_size = 512")?;
    conn.execute(format!("PRAGMA cipher = '{CIPHER_A}'"))?;
    conn.execute(format!("PRAGMA hexkey = '{KEY_A}'"))?;

    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, payload BLOB)")?;
    for i in 0..100i64 {
        conn.execute(format!("INSERT INTO t VALUES({i}, randomblob(300))"))?;
    }
    conn.execute("DELETE FROM t WHERE id % 3 = 0")?;

    conn.execute("VACUUM")?;

    let rows: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM t");
    assert_eq!(rows[0].0, 66);
    let ps: Vec<(i64,)> = conn.exec_rows("PRAGMA page_size");
    assert_eq!(ps[0].0, 512);

    do_flush(&conn, &tmp_db)?;

    let uri = format!(
        "file:{}?cipher={CIPHER_A}&hexkey={KEY_A}",
        tmp_db.path.to_str().unwrap()
    );
    let (_io, reopened) = turso_core::Connection::from_uri(
        &uri,
        DatabaseOpts::new().with_encryption(true),
        Arc::new(SqliteDialect),
    )?;
    let rows: Vec<(i64,)> = reopened.exec_rows("SELECT count(*) FROM t");
    assert_eq!(rows[0].0, 66);

    Ok(())
}

// Refusing a wrong key must release the WAL read lock so a valid connection
// can truncate the WAL. Found by the encryption fuzzer; the refusal leaked
// this lock on main (4b3befbd6) as well.

#[test]
fn encrypted_reload_accepts_an_unwritten_database() -> anyhow::Result<()> {
    for cipher in [CIPHER_A, CIPHER_B] {
        let tmp = TempDatabaseBuilder::new()
            .with_encryption(cipher, KEY_A)
            .build();
        let owner = tmp.connect_limbo();
        assert_eq!(std::fs::metadata(&tmp.path)?.len(), 0);
        // An unwritten database has no page that can check a key, so this test
        // cannot expect a reload with another key to fail.
        assert!(tmp.db.reload_wal_after_external_restore().is_err());
        std::fs::write(&tmp.path, [0; 16])?;
        assert!(owner.reload_wal_after_external_restore().is_err());
        assert_eq!(std::fs::read(&tmp.path)?, [0; 16]);
        std::fs::write(&tmp.path, [])?;
        let foreign = TempDatabaseBuilder::new()
            .with_encryption(cipher, KEY_B)
            .build();
        let foreign_owner = foreign.connect_limbo();
        foreign_owner.execute("CREATE TABLE secrets(value TEXT)")?;
        foreign_owner.execute("INSERT INTO secrets VALUES ('foreign')")?;
        let wal_path = format!("{}-wal", tmp.path.display());
        let original_wal = std::fs::read(&wal_path)?;
        let foreign_wal = std::fs::read(format!("{}-wal", foreign.path.display()))?;
        assert!(!foreign_wal.is_empty());
        std::fs::write(&wal_path, &foreign_wal)?;
        let error = owner.reload_wal_after_external_restore().unwrap_err();
        assert!(
            error.to_string().to_lowercase().contains("decrypt"),
            "{error}"
        );
        assert_eq!(std::fs::read(&wal_path)?, foreign_wal);
        std::fs::write(&wal_path, original_wal)?;
        owner.reload_wal_after_external_restore()?;
        assert_eq!(std::fs::metadata(&tmp.path)?.len(), 0);
        owner.execute("CREATE TABLE secrets(value TEXT)")?;
        owner.execute("INSERT INTO secrets VALUES ('after reload')")?;
        owner.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
        let rows: Vec<(String,)> = owner.exec_rows("SELECT value FROM secrets");
        assert_eq!(rows, [("after reload".into(),)]);
    }
    Ok(())
}

#[test]
fn encrypted_reload_with_retained_completions() -> anyhow::Result<()> {
    for cipher in [CIPHER_A, CIPHER_B] {
        for mvcc in [false, true] {
            let database = Database::open(
                Arc::new(crate::queued_io::QueuedIo::new()),
                "retained-completions.db",
                attach_matrix_options(Some(cipher), mvcc, KEY_A),
            )?;
            let owner = database.connect(
                ConnectOpts::new(SessionRole::Superuser)
                    .with_encryption_key(Some(EncryptionKey::from_hex_string(KEY_A)?)),
            )?;
            owner.execute("CREATE TABLE secrets(value TEXT)")?;
            owner.execute("INSERT INTO secrets VALUES ('before reload')")?;
            owner.reload_wal_after_external_restore()?;
            owner.publish_schema_after_external_restore()?;
            let rows: Vec<(String,)> = owner.exec_rows("SELECT value FROM secrets");
            assert_eq!(rows, [("before reload".into(),)]);
            owner.execute("INSERT INTO secrets VALUES ('after reload')")?;
            owner.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
            let rows: Vec<(i64,)> = owner.exec_rows("SELECT count(*) FROM secrets");
            assert_eq!(rows, [(2,)]);
        }
    }
    Ok(())
}

#[test]
fn encrypted_restore_recovers_torn_sidecar_tail() -> anyhow::Result<()> {
    for cipher in [CIPHER_A, CIPHER_B] {
        for mvcc in [false, true] {
            let target = TempDatabaseBuilder::new()
                .with_encryption(cipher, KEY_A)
                .with_mvcc(mvcc)
                .build();
            let owner = target.connect_limbo();
            owner.execute("CREATE TABLE secrets(value TEXT)")?;
            owner.execute("INSERT INTO secrets VALUES ('checkpointed')")?;
            owner.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
            let committed = "committed".repeat(16_384);
            owner.execute(format!("INSERT INTO secrets VALUES ('{committed}')"))?;
            owner.reload_wal_after_external_restore()?;
            owner.publish_schema_after_external_restore()?;
            owner.execute(format!("INSERT INTO secrets VALUES ('{committed}')"))?;
            let suffix = if mvcc { "-log" } else { "-wal" };
            let path = format!("{}{suffix}", target.path.display());
            let mut bytes = std::fs::read(&path)?;
            let damaged_byte = bytes.len() - 16;
            bytes[damaged_byte] ^= 1;
            std::fs::write(path, bytes)?;
            let fresh_dir = tempfile::tempdir()?;
            let fresh_path = fresh_dir.path().join("fresh.db");
            copy_encrypted_test_files(&target, &fresh_path)?;
            assert_eq!(fresh_encrypted_rows(&fresh_path, cipher)?.len(), 2);
            owner.reload_wal_after_external_restore()?;
            owner.publish_schema_after_external_restore()?;
            let rows: Vec<(String,)> = owner.exec_rows("SELECT value FROM secrets");
            assert_eq!(rows, [("checkpointed".into(),), (committed,)]);
            owner.execute("INSERT INTO secrets VALUES ('after recovery')")?;
            owner.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
            let rows: Vec<(String,)> = owner.exec_rows("SELECT value FROM secrets");
            assert_eq!(rows.len(), 3);
        }
    }
    Ok(())
}

#[test]
fn encrypted_recovery_rejects_files_encrypted_with_another_key() -> anyhow::Result<()> {
    for cipher in [CIPHER_A, CIPHER_B] {
        for mvcc in [false, true] {
            let target = TempDatabaseBuilder::new()
                .with_encryption(cipher, KEY_A)
                .with_mvcc(mvcc)
                .build();
            let source = TempDatabaseBuilder::new()
                .with_encryption(cipher, KEY_B)
                .with_mvcc(mvcc)
                .build();
            let owner = target.connect_limbo();
            let foreign = source.connect_limbo();
            for conn in [&owner, &foreign] {
                conn.execute("CREATE TABLE secrets(value TEXT)")?;
                conn.execute("INSERT INTO secrets VALUES ('protected value')")?;
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
                conn.execute("INSERT INTO secrets VALUES ('in sidecar'), ('in sidecar2')")?;
            }
            let sidecar_suffix = if mvcc { "-log" } else { "-wal" };
            let sidecar_path = format!("{}{sidecar_suffix}", target.path.display());
            let mut incomplete_tail = std::fs::read(&sidecar_path)?;
            incomplete_tail.extend_from_slice(&[0; 7]);
            std::fs::write(&sidecar_path, incomplete_tail)?;
            owner.reload_wal_after_external_restore()?;
            owner.publish_schema_after_external_restore()?;
            let rows: Vec<(String,)> = owner.exec_rows("SELECT value FROM secrets");
            assert_eq!(rows.len(), 3, "incomplete tail lost committed rows");
            for suffix in ["", sidecar_suffix] {
                let target_path = format!("{}{suffix}", target.path.display());
                let source_path = format!("{}{suffix}", source.path.display());
                let original = std::fs::read(&target_path)?;
                let restored = std::fs::read(source_path)?;
                std::fs::write(&target_path, &restored)?;
                let result = owner.reload_wal_after_external_restore();
                let after_reload = std::fs::read(&target_path)?;
                let fresh_dir = tempfile::tempdir()?;
                let fresh_path = fresh_dir.path().join("fresh.db");
                copy_encrypted_test_files(&target, &fresh_path)?;
                std::fs::write(&target_path, original)?;
                let error = result.expect_err(&format!("accepted {cipher} mvcc={mvcc} {suffix}"));
                assert!(
                    error.to_string().to_ascii_lowercase().contains("decrypt"),
                    "{error}"
                );
                assert_eq!(after_reload, restored, "rejected restore changed the file");
                let error = fresh_encrypted_rows(&fresh_path, cipher)
                    .expect_err("fresh recovery accepted files encrypted under another key");
                assert!(error.to_string().to_ascii_lowercase().contains("decrypt"));
                assert_eq!(
                    std::fs::read(format!("{}{suffix}", fresh_path.display()))?,
                    restored,
                    "rejected fresh recovery changed the file"
                );
                let rows: Vec<(String,)> = owner.exec_rows("SELECT value FROM secrets");
                assert_eq!(rows.len(), 3, "rejected restore changed the live database");
                owner.reload_wal_after_external_restore()?;
                owner.publish_schema_after_external_restore()?;
                let rows: Vec<(String,)> = owner.exec_rows("SELECT value FROM secrets");
                assert_eq!(
                    rows,
                    [
                        ("protected value".into(),),
                        ("in sidecar".into(),),
                        ("in sidecar2".into(),)
                    ]
                );
            }
        }
    }
    Ok(())
}

fn copy_encrypted_test_files(source: &TempDatabase, path: &std::path::Path) -> anyhow::Result<()> {
    for suffix in ["", "-wal", "-log"] {
        let source_path = format!("{}{suffix}", source.path.display());
        if std::path::Path::new(&source_path).exists() {
            std::fs::copy(source_path, format!("{}{suffix}", path.display()))?;
        }
    }
    Ok(())
}

fn fresh_encrypted_rows(
    path: &std::path::Path,
    cipher: &str,
) -> anyhow::Result<Vec<Vec<turso_core::Value>>> {
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new()?);
    let db = Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        OpenFlags::None,
        DatabaseOpts::new().with_encryption(true),
        Some(EncryptionOpts {
            cipher: cipher.into(),
            hexkey: KEY_A.into(),
        }),
        Arc::new(SqliteDialect),
    )?;
    let conn = db.connect(
        ConnectOpts::new(SessionRole::Superuser)
            .with_encryption_key(Some(EncryptionKey::from_hex_string(KEY_A)?)),
    )?;
    Ok(conn
        .prepare("SELECT value FROM secrets")?
        .run_collect_rows()?)
}

#[test]
fn encrypted_restore_requires_the_current_key_and_preserves_page_layout() -> anyhow::Result<()> {
    for cipher in [CIPHER_A, CIPHER_B] {
        for mvcc in [false, true] {
            let tmp = TempDatabaseBuilder::new()
                .with_encryption(cipher, KEY_A)
                .with_mvcc(mvcc)
                .build();
            let owner = tmp.connect_limbo();
            owner.execute("CREATE TABLE secrets(value TEXT)")?;
            owner.execute("INSERT INTO secrets VALUES ('protected value')")?;
            owner.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
            let before: Vec<_> = [
                tmp.path.clone(),
                std::path::PathBuf::from(format!("{}-wal", tmp.path.display())),
                std::path::PathBuf::from(format!("{}-log", tmp.path.display())),
            ]
            .into_iter()
            .map(|path| (path.clone(), std::fs::read(path).ok()))
            .collect();
            assert!(tmp.db.reload_wal_after_external_restore().is_err());
            assert!(tmp
                .db
                .reload_wal_after_external_restore_with_encryption_key(Some(
                    EncryptionKey::from_hex_string(KEY_B)?,
                ))
                .is_err());
            let original = std::fs::read(&tmp.path)?;
            for change_page_size in [false, true] {
                let mut replaced = original.clone();
                if change_page_size {
                    replaced[16..18].copy_from_slice(&8192u16.to_be_bytes());
                } else {
                    replaced[20] ^= 1;
                }
                std::fs::write(&tmp.path, replaced)?;
                let error = owner.reload_wal_after_external_restore().unwrap_err();
                assert!(error.to_string().contains("page layout"), "{error}");
                std::fs::write(&tmp.path, &original)?;
            }
            owner.reload_wal_after_external_restore()?;
            owner.publish_schema_after_external_restore()?;
            for (path, bytes) in before {
                assert_eq!(std::fs::read(path).ok(), bytes);
            }
            let rows: Vec<(String,)> = owner.exec_rows("SELECT value FROM secrets");
            assert_eq!(rows, [("protected value".into(),)]);
        }
    }
    Ok(())
}

#[cfg(all(feature = "fs", feature = "encryption", feature = "conn_raw_api"))]
#[test]
fn encrypted_mvcc_reload_preserves_key_and_log_encryption() -> anyhow::Result<()> {
    let dir = tempfile::TempDir::new().expect("temporary directory");
    let path = dir.path().join("reload.db");
    let path = path.to_str().expect("UTF-8 path");
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new()?);
    let encryption = EncryptionOpts {
        cipher: "aes256gcm".into(),
        hexkey: "17".repeat(32),
    };
    let key = EncryptionKey::from_hex_string(&encryption.hexkey)?;
    let options = DatabaseOpts::new().with_encryption(true);
    {
        let db = Database::open_file_with_flags(
            io.clone(),
            path,
            OpenFlags::Create,
            options,
            Some(encryption.clone()),
            Arc::new(SqliteDialect),
        )?;
        let conn = db.connect(
            ConnectOpts::new(SessionRole::Superuser).with_encryption_key(Some(key.clone())),
        )?;
        conn.execute("PRAGMA journal_mode = 'mvcc'")?;
        conn.execute("CREATE TABLE t(value TEXT)")?;
        conn.execute("INSERT INTO t VALUES ('before reload')")?;
        conn.reload_wal_after_external_restore()?;
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
        let store = conn.mv_store();
        let store = store.as_ref().expect("MVCC store");
        io.wait_for_completion(store.reset_logical_log_after_external_restore()?)?;
        if let Some(completion) = store.sync_logical_log(&conn)? {
            io.wait_for_completion(completion)?;
        }
        drop(conn);
        db.reload_wal_after_external_restore_with_encryption_key(Some(key.clone()))?;
        let conn = db.connect(
            ConnectOpts::new(SessionRole::Superuser).with_encryption_key(Some(key.clone())),
        )?;
        conn.execute("INSERT INTO t VALUES ('after reload')")?;
    }
    {
        let db = Database::open_file_with_flags(
            io.clone(),
            path,
            OpenFlags::None,
            options,
            Some(encryption.clone()),
            Arc::new(SqliteDialect),
        )?;
        let conn = db.connect(
            ConnectOpts::new(SessionRole::Superuser).with_encryption_key(Some(key.clone())),
        )?;
        let mut stmt = conn.prepare("SELECT value FROM t ORDER BY rowid")?;
        for expected in ["before reload", "after reload"] {
            assert!(matches!(stmt.step()?, turso_core::StepResult::Row));
            assert_eq!(stmt.row().expect("row").get::<String>(0)?, expected);
        }
        assert!(matches!(stmt.step()?, turso_core::StepResult::Done));
        drop(stmt);
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
    }
    let db = Database::open_file_with_flags(
        io,
        path,
        OpenFlags::None,
        options,
        Some(encryption),
        Arc::new(SqliteDialect),
    )?;
    let conn =
        db.connect(ConnectOpts::new(SessionRole::Superuser).with_encryption_key(Some(key)))?;
    let rows = conn.prepare("SELECT count(*) FROM t")?.run_collect_rows()?;
    assert_eq!(rows, vec![vec![turso_core::Value::from_i64(2)]]);
    Ok(())
}
