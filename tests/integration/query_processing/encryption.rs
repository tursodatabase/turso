use crate::common::{
    compute_dbhash, compute_dbhash_with_options, do_flush, run_query, run_query_on_row, ExecRows,
    TempDatabase, TempDatabaseBuilder,
};
use asserting::prelude::*;
use rand::{rng, RngCore};
use std::sync::Arc;
use turso_core::{
    CompletionError, ConnectOpts, Database, DatabaseOpts, EncryptionKey, EncryptionOpts, IOResult,
    LimboError, OpenFlags, OpenOptions, PlatformIO, Row, SessionRole, IO,
};
use turso_sqlite::SqliteDialect;

const ENABLE_ENCRYPTION: bool = true;

#[test]
fn attach_encryption_matrix() -> anyhow::Result<()> {
    let (mut rng, seed) = crate::common::rng_from_time_or_env();
    eprintln!("encryption ATTACH SEED={seed}");
    for main_encrypted in [false, true] {
        for attached_cipher in [None, Some(CIPHER_A), Some(CIPHER_B)] {
            let mut cases = Vec::new();
            for populate_main in [false, true] {
                if populate_main && !main_encrypted {
                    continue;
                }
                for main_memory in [false, true] {
                    for mvcc in [false, true] {
                        for (existing, cached) in [(false, false), (true, false), (true, true)] {
                            cases.push((populate_main, main_memory, mvcc, existing, cached));
                        }
                    }
                }
            }
            for (populate_main, main_memory, mvcc, existing, cached) in
                crate::common::sample_encryption_cases(cases, 1, &mut rng)
            {
                let directory = tempfile::tempdir()?;
                let main_path = directory.path().join("main.db");
                let attached_path = directory.path().join("attached.db");
                let disk: Arc<dyn IO> = Arc::new(PlatformIO::new()?);
                let main_io: Arc<dyn IO> = if main_memory {
                    Arc::new(turso_core::MemoryIO::new())
                } else {
                    disk.clone()
                };
                let main = Database::open(
                    main_io,
                    if main_memory {
                        ":memory:"
                    } else {
                        main_path.to_str().unwrap()
                    },
                    attach_matrix_options(main_encrypted.then_some(CIPHER_A), mvcc, KEY_A),
                )?;
                let connection = main.connect(
                    ConnectOpts::new(SessionRole::Superuser).with_encryption_key(
                        main_encrypted.then(|| EncryptionKey::from_hex_string(KEY_A).unwrap()),
                    ),
                )?;
                if populate_main {
                    connection.execute("CREATE TABLE main_data(value TEXT)")?;
                    connection.execute("INSERT INTO main_data VALUES ('main value')")?;
                }
                let retained = if existing {
                    let attached = Database::open(
                        disk.clone(),
                        attached_path.to_str().unwrap(),
                        attach_matrix_options(attached_cipher, mvcc, KEY_B),
                    )?;
                    let owner = attached.connect(
                        ConnectOpts::new(SessionRole::Superuser).with_encryption_key(
                            attached_cipher.map(|_| EncryptionKey::from_hex_string(KEY_B).unwrap()),
                        ),
                    )?;
                    owner.execute("CREATE TABLE data(value TEXT)")?;
                    owner.execute("INSERT INTO data VALUES ('existing')")?;
                    if cached {
                        Some((attached, owner))
                    } else {
                        owner.close()?;
                        let weak = Arc::downgrade(&attached);
                        drop((owner, attached));
                        assert_eq!(weak.strong_count(), 0);
                        None
                    }
                } else {
                    None
                };
                if existing && attached_cipher.is_some() {
                    for uri in [
                        attached_path.display().to_string(),
                        format!(
                            "file:{}?cipher={}&hexkey={}",
                            attached_path.display(),
                            attached_cipher.unwrap(),
                            KEY_A
                        ),
                    ] {
                        assert!(connection
                            .execute(format!("ATTACH '{uri}' AS aux"))
                            .is_err());
                        assert!(connection
                            .attached_database_names()
                            .iter()
                            .all(|name| name != "aux"));
                        if populate_main {
                            let rows: Vec<(String,)> =
                                connection.exec_rows("SELECT value FROM main_data");
                            assert_eq!(rows, [("main value".into(),)]);
                        }
                    }
                }
                let uri = match attached_cipher {
                    Some(cipher) => format!(
                        "file:{}?cipher={cipher}&hexkey={KEY_B}",
                        attached_path.display()
                    ),
                    None => attached_path.to_str().unwrap().to_string(),
                };
                connection.execute(format!("ATTACH '{uri}' AS aux"))?;
                connection.execute("CREATE TABLE IF NOT EXISTS aux.data(value TEXT)")?;
                connection.execute("INSERT INTO aux.data VALUES ('attached')")?;
                let rows: Vec<(String,)> =
                    connection.exec_rows("SELECT value FROM aux.data ORDER BY value");
                let expected = if existing {
                    vec![("attached".into(),), ("existing".into(),)]
                } else {
                    vec![("attached".into(),)]
                };
                assert_eq!(rows, expected, "main_encrypted={main_encrypted}, populate_main={populate_main}, main_memory={main_memory}, mvcc={mvcc}, attached_cipher={attached_cipher:?}, existing={existing}, cached={cached}");
                assert_eq!(
                    connection.get_pager().is_encryption_ctx_set(),
                    main_encrypted
                );
                connection.execute("DETACH aux")?;
                if attached_cipher.is_some() {
                    assert!(connection
                        .execute(format!("ATTACH '{}' AS aux", attached_path.display()))
                        .is_err());
                    assert!(connection
                        .attached_database_names()
                        .iter()
                        .all(|name| name != "aux"));
                }
                if populate_main {
                    let rows: Vec<(String,)> = connection.exec_rows("SELECT value FROM main_data");
                    assert_eq!(rows, [("main value".into(),)]);
                }
                let bytes = std::fs::read(&attached_path)?;
                assert_eq!(
                    bytes.starts_with(b"SQLite format 3\0"),
                    attached_cipher.is_none()
                );
                drop(retained);
                let reopened = Database::open(
                    disk.clone(),
                    attached_path.to_str().unwrap(),
                    attach_matrix_options(attached_cipher, mvcc, KEY_B),
                )?;
                let reader = reopened.connect(
                    ConnectOpts::new(SessionRole::Superuser).with_encryption_key(
                        attached_cipher.map(|_| EncryptionKey::from_hex_string(KEY_B).unwrap()),
                    ),
                )?;
                let recovered: Vec<(String,)> =
                    reader.exec_rows("SELECT value FROM data ORDER BY value");
                assert_eq!(recovered, expected);
            }
        }
    }
    Ok(())
}

#[test]
fn attach_key_rejects_non_null_values_before_creating_a_file() -> anyhow::Result<()> {
    for main_encrypted in [false, true] {
        let directory = tempfile::tempdir()?;
        let main_path = directory.path().join("main.db");
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new()?);
        let db = Database::open(
            io,
            main_path.to_str().unwrap(),
            attach_matrix_options(main_encrypted.then_some(CIPHER_A), false, KEY_A),
        )?;
        let connection = db.connect(
            ConnectOpts::new(SessionRole::Superuser).with_encryption_key(
                main_encrypted.then(|| EncryptionKey::from_hex_string(KEY_A).unwrap()),
            ),
        )?;
        for (index, expression) in ["'secret'", "42", "x'AB'", "lower('SECRET')"]
            .into_iter()
            .enumerate()
        {
            let path = directory.path().join(format!("refused-{index}.db"));
            let error = connection
                .execute(format!(
                    "ATTACH '{}' AS aux KEY {expression}",
                    path.display()
                ))
                .unwrap_err();
            assert!(
                error.to_string().contains("ATTACH KEY is not supported"),
                "{error}"
            );
            assert!(!path.exists());
            connection.execute(format!("ATTACH '{}' AS aux KEY NULL", path.display()))?;
            connection.execute("CREATE TABLE aux.data(value)")?;
            connection.execute("DETACH aux")?;
            assert!(std::fs::read(&path)?.starts_with(b"SQLite format 3\0"));
        }
    }
    Ok(())
}

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

#[test]
fn synchronous_connect_keeps_statistics_read_failures_best_effort() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("statistics.db");
    let queued = Arc::new(crate::queued_io::QueuedIo::new_with_io(Arc::new(
        PlatformIO::new()?,
    )));
    let io = Arc::new(ConnectFaultIo {
        inner: queued,
        fail_read_offset: std::sync::atomic::AtomicU64::new(u64::MAX),
    });
    let db = Database::open(
        io.clone(),
        path.to_str().unwrap(),
        OpenOptions::new(Arc::new(SqliteDialect)),
    )?;
    let owner = db.connect(ConnectOpts::new(SessionRole::Superuser))?;
    owner.execute("CREATE TABLE data(value)")?;
    owner.execute("CREATE INDEX data_value ON data(value)")?;
    owner.execute("INSERT INTO data VALUES (1), (2), (3)")?;
    owner.execute("ANALYZE")?;
    let roots: Vec<(i64,)> =
        owner.exec_rows("SELECT rootpage FROM sqlite_schema WHERE name = 'sqlite_stat1'");
    owner.checkpoint(turso_core::CheckpointMode::Truncate {
        upper_bound_inclusive: None,
    })?;
    let offset = (roots[0].0 as u64 - 1) * owner.get_page_size().get() as u64;
    io.fail_read_offset
        .store(offset, std::sync::atomic::Ordering::SeqCst);
    let connection = db.connect(ConnectOpts::new(SessionRole::Superuser));
    assert_eq!(
        io.fail_read_offset
            .load(std::sync::atomic::Ordering::SeqCst),
        u64::MAX,
        "statistics fault did not trigger"
    );
    let rows: Vec<(i64,)> = connection?.exec_rows("SELECT count(*) FROM data");
    assert_eq!(rows, [(3,)]);
    Ok(())
}

struct ConnectFaultIo {
    inner: Arc<crate::queued_io::QueuedIo>,
    fail_read_offset: std::sync::atomic::AtomicU64,
}
impl turso_core::Clock for ConnectFaultIo {
    fn current_time_monotonic(&self) -> turso_core::MonotonicInstant {
        self.inner.current_time_monotonic()
    }
    fn current_time_wall_clock(&self) -> turso_core::WallClockInstant {
        self.inner.current_time_wall_clock()
    }
}
impl IO for ConnectFaultIo {
    fn open_file(
        &self,
        path: &str,
        flags: OpenFlags,
        direct: bool,
    ) -> turso_core::Result<Arc<dyn turso_core::File>> {
        self.inner.open_file(path, flags, direct)
    }
    fn remove_file(&self, path: &str) -> turso_core::Result<()> {
        self.inner.remove_file(path)
    }
    fn file_id(&self, path: &str) -> turso_core::Result<turso_core::io::FileId> {
        self.inner.file_id(path)
    }
    fn step(&self) -> turso_core::Result<()> {
        let offset = self
            .fail_read_offset
            .load(std::sync::atomic::Ordering::SeqCst);
        let events = self.inner.pending_events();
        if let Some(index) = events.iter().position(|event| {
            event.kind == crate::queued_io::QueuedIoOpKind::Pread
                && event.offset == offset
                && !event.path.ends_with("-wal")
        }) {
            self.fail_read_offset
                .store(u64::MAX, std::sync::atomic::Ordering::SeqCst);
            self.inner.step_at(index, true)?;
            Ok(())
        } else {
            self.inner.step()
        }
    }
}

#[test]
fn cached_open_authenticates_bootstrap_page_before_returning_database() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("encrypted.db");
    let io = Arc::new(PlatformIO::new().unwrap());
    let options = |key: &str| {
        OpenOptions::new(Arc::new(SqliteDialect))
            .db_opts(DatabaseOpts::new().with_encryption(true))
            .encryption(EncryptionOpts {
                cipher: "aegis256".into(),
                hexkey: key.into(),
            })
    };
    let db = Database::open(
        io.clone(),
        path.to_str().unwrap(),
        options(&"01".repeat(32)),
    )
    .unwrap();
    let refused = Database::open(
        io.clone(),
        path.to_str().unwrap(),
        options(&"02".repeat(32)),
    );
    assert!(
        refused.is_err(),
        "cached open returned a Database with a wrong key"
    );
    let correct = Database::open(io, path.to_str().unwrap(), options(&"01".repeat(32))).unwrap();
    assert!(Arc::ptr_eq(&db, &correct));
}

fn encrypted_fixture(source: &TempDatabase, size: u32) -> TempDatabase {
    TempDatabaseBuilder::new()
        .with_opts(source.db_opts)
        .with_mvcc(source.enable_mvcc)
        .with_encryption(CIPHER_A, KEY_A)
        .with_page_size(size)
        .build()
}

fn run_non_4k_page_size_encryption_test(
    tmp_db: &TempDatabase,
    enable_mvcc: bool,
) -> anyhow::Result<()> {
    // Set page size to 8k (8192 bytes) and test encryption. Default page size is 4k.
    let encrypted = encrypted_fixture(tmp_db, 8192);
    let tmp_db = &encrypted;
    let _ = env_logger::try_init();
    let db_path = tmp_db.path.clone();

    {
        let conn = tmp_db.connect_limbo();
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
    let encrypted = encrypted_fixture(tmp_db, 4096);
    let tmp_db = &encrypted;
    let _ = env_logger::try_init();
    let db_path = tmp_db.path.clone();

    {
        let conn = tmp_db.connect_limbo();
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

        assert!(
            turso_core::Connection::from_uri(
                &uri,
                DatabaseOpts::new().with_encryption(true),
                Arc::new(SqliteDialect)
            )
            .is_err(),
            "corrupted associated data at {corrupt_pos}"
        );
    }

    Ok(())
}

// TODO: mvcc does not error here
#[turso_macros::test]
fn test_per_page_encryption(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let tmp_db = encrypted_fixture(&tmp_db, 4096);
    let _ = env_logger::try_init();
    let db_path = tmp_db.path.clone();
    let opts = tmp_db.db_opts;

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
        assert!(matches!(
            turso_core::Connection::from_uri(&uri, opts, Arc::new(SqliteDialect)),
            Err(LimboError::CompletionError(
                CompletionError::DecryptionError { .. }
            ))
        ));
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
        assert!(tmp_db
            .db
            .connect(ConnectOpts::new(turso_core::SessionRole::Superuser))
            .is_err());
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

    assert!(Database::open(
        tmp_db.io.clone(),
        tmp_db.path.to_str().unwrap(),
        OpenOptions::new(Arc::new(SqliteDialect))
            .db_opts(tmp_db.db_opts)
            .encryption(EncryptionOpts {
                cipher: CIPHER_A.to_string(),
                hexkey: KEY_A.to_string(),
            }),
    )
    .is_err());

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
    .contains("encryption must be configured when opening the database");

    assert_that!(run_query(&tmp_db, &conn, "PRAGMA cipher = 'aegis256';"))
        .err()
        .display_string()
        .contains("encryption must be configured when opening the database");

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
    let tmp_db = encrypted_fixture(&tmp_db, 4096);
    let _ = env_logger::try_init();
    let db_path = tmp_db.path.clone();

    let opts = tmp_db.db_opts;

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

        assert!(matches!(
            turso_core::Connection::from_uri(&uri, opts, Arc::new(SqliteDialect)),
            Err(LimboError::CompletionError(
                CompletionError::DecryptionError { .. }
            ))
        ));
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
            .with_encryption(cipher_name, hexkey)
            .build();
        let db_path = tmp_db.path.clone();

        {
            let conn = tmp_db.connect_limbo();
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

        let conn = main_db.connect(
            ConnectOpts::new(turso_core::SessionRole::Superuser)
                .with_encryption_key(Some(correct_encryption_key)),
        )?;

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

        let conn = db.connect(
            ConnectOpts::new(turso_core::SessionRole::Superuser)
                .with_encryption_key(Some(correct_encryption_key)),
        )?;

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

    // Step 3: Opening a cached database must still validate the key.
    {
        let result = Database::open_file_with_flags(
            io.clone(),
            db_path_str,
            OpenFlags::default(),
            opts,
            Some(EncryptionOpts {
                cipher: "aegis256".to_string(),
                hexkey: wrong_key.to_string(),
            }),
            Arc::new(SqliteDialect),
        );

        assert!(matches!(
            result,
            Err(LimboError::CompletionError(
                CompletionError::DecryptionError { .. }
            ))
        ));
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
            .contains("encrypted database requires an encryption key");
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

        let conn = db.connect(
            ConnectOpts::new(turso_core::SessionRole::Superuser)
                .with_encryption_key(Some(correct_encryption_key)),
        )?;

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

    let key = EncryptionKey::from_hex_string(hexkey)?;
    let conn = db.connect(
        ConnectOpts::new(turso_core::SessionRole::Superuser).with_encryption_key(Some(key)),
    )?;

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
        assert_that!(run_query(&main_db, &conn, &attach_wrong))
            .described_as("ATTACH with the wrong key")
            .is_err();
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

        assert_that!(run_query(&main_db, &conn, &attach_a_wrong))
            .described_as("ATTACH of the first database with swapped keys")
            .is_err();
        assert_that!(run_query(&main_db, &conn, &attach_b_wrong))
            .described_as("ATTACH of the second database with swapped keys")
            .is_err();
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
        assert_that!(run_query(&main_db, &conn, &attach_wrong))
            .described_as("ATTACH with the wrong key before retrying")
            .is_err();

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
    let tmp_db = encrypted_fixture(&tmp_db, 4096);
    use tempfile::TempDir;

    let _ = env_logger::try_init();
    let hexkey = "b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327";

    // 1. Create an encrypted source database and insert data
    {
        let conn = tmp_db.connect_limbo();

        conn.execute("CREATE TABLE secret_data (id INTEGER PRIMARY KEY, content TEXT)")?;
        conn.execute("INSERT INTO secret_data (content) VALUES ('this was encrypted')")?;
        do_flush(&conn, &tmp_db)?;
    }

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("exported.db");
    let dest_path_str = dest_path.to_str().unwrap();

    // 2. Demonstrate that the encrypted source CANNOT be read or vacuumed without keys
    {
        let result = Database::open(
            tmp_db.io.clone(),
            tmp_db.path.to_str().unwrap(),
            OpenOptions::new(Arc::new(SqliteDialect)).db_opts(tmp_db.db_opts),
        );
        assert_that!(result)
            .err()
            .display_string()
            .contains("encrypted database requires an encryption key");
    }

    // 3. Execute VACUUM INTO using an authorized connection
    {
        let conn = tmp_db.db.connect(
            ConnectOpts::new(turso_core::SessionRole::Superuser)
                .with_encryption_key(Some(EncryptionKey::from_hex_string(hexkey)?)),
        )?;
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
    // Close the source first.
    let source_path = tmp_db.path.clone();
    drop(tmp_db);
    let source_hash = turso_dbhash::hash_database(
        source_path.to_str().unwrap(),
        &turso_dbhash::DbHashOptions {
            encryption: Some(EncryptionOpts {
                cipher: CIPHER_A.to_string(),
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
    let tmp_db = encrypted_fixture(&tmp_db, 8192);
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    [
        "PRAGMA journal_mode = 'mvcc';",
        "CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT);",
        "INSERT INTO test (value) VALUES ('Hello, World!')",
    ]
    .iter()
    .try_for_each(|query| run_query(&tmp_db, &conn, query))?;
    do_flush(&conn, &tmp_db)
}

// Regression coverage for https://github.com/tursodatabase/turso/issues/7375.

fn assert_encrypted_page_size_selected_at_open(
    page_size: i64,
    cipher: &str,
    hexkey: &str,
) -> anyhow::Result<()> {
    let tmp_db = TempDatabaseBuilder::new()
        .with_opts(DatabaseOpts::new().with_encryption(true))
        .with_encryption(cipher, hexkey)
        .with_page_size(page_size as u32)
        .build();

    let conn = tmp_db.connect_limbo();
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
fn test_encrypted_page_size_selected_at_open(_tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    for page_size in [512, 4096, 65536] {
        assert_encrypted_page_size_selected_at_open(page_size, CIPHER_A, KEY_A)?;
    }
    // Exercise a cipher that uses a 16-byte key and different metadata size.
    let aes128_key = &KEY_A[..32];
    assert_encrypted_page_size_selected_at_open(512, "aes128gcm", aes128_key)?;
    Ok(())
}

#[turso_macros::test]
fn test_uri_encryption_rejects_page_size_changes(_tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let tmp_db = TempDatabaseBuilder::new()
        .with_opts(DatabaseOpts::new().with_encryption(true))
        .with_encryption(CIPHER_A, KEY_A)
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
        assert!(conn.execute("PRAGMA page_size = 512").is_err());
        conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY, b BLOB)")?;
        conn.execute("INSERT INTO t VALUES(1, randomblob(300))")?;

        let rows: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM t");
        assert_eq!(rows[0].0, 1);
        let ps: Vec<(i64,)> = conn.exec_rows("PRAGMA page_size");
        assert_eq!(ps[0].0, 4096);

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
    assert_eq!(ps[0].0, 4096);

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
        .with_encryption(CIPHER_A, KEY_A)
        .with_page_size(512)
        .build();

    let conn = tmp_db.connect_limbo();
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

#[test]
fn test_attach_encrypted_database_without_key_is_refused() -> anyhow::Result<()> {
    for mvcc in [false, true] {
        for cached in [false, true] {
            let dir = tempfile::tempdir()?;
            let path = dir.path().join("encrypted-attach.db");
            let io: Arc<dyn IO> = Arc::new(PlatformIO::new()?);
            let encrypted = Database::open(
                io,
                path.to_str().unwrap(),
                OpenOptions::new(Arc::new(SqliteDialect))
                    .db_opts(DatabaseOpts::new().with_encryption(true))
                    .encryption(EncryptionOpts {
                        cipher: CIPHER_A.to_string(),
                        hexkey: KEY_A.to_string(),
                    }),
            )?;
            let writer = encrypted.connect(
                ConnectOpts::new(turso_core::SessionRole::Superuser)
                    .with_encryption_key(Some(EncryptionKey::from_hex_string(KEY_A)?)),
            )?;
            if mvcc {
                writer.execute("PRAGMA journal_mode = 'mvcc'")?;
            }
            writer.execute("CREATE TABLE secrets(value TEXT)")?;
            writer.execute("INSERT INTO secrets VALUES ('protected value')")?;
            let keep_open = cached.then_some((encrypted, writer));
            let main = TempDatabase::builder()
                .with_opts(DatabaseOpts::new().with_encryption(true).with_attach(true))
                .with_mvcc(mvcc)
                .build();
            let conn = main.connect_limbo();
            assert!(conn
                .execute(format!("ATTACH '{}' AS secret", path.display()))
                .is_err());
            let databases: Vec<(i64, String, String)> = conn.exec_rows("PRAGMA database_list");
            assert!(databases.iter().all(|(_, name, _)| name != "secret"));
            drop(keep_open);
        }
    }
    Ok(())
}

#[test]
fn test_cached_encrypted_database_validates_each_connection() -> anyhow::Result<()> {
    for mvcc in [false, true] {
        let tmp = TempDatabaseBuilder::new()
            .with_encryption(CIPHER_A, KEY_A)
            .build();
        let writer = tmp.connect_limbo();
        if mvcc {
            writer.execute("PRAGMA journal_mode = 'mvcc'")?;
        }
        writer.execute("CREATE TABLE secrets(value TEXT)")?;
        writer.execute("INSERT INTO secrets VALUES ('protected value')")?;

        let open = |encryption| {
            Database::open(
                tmp.io.clone(),
                tmp.path.to_str().unwrap(),
                OpenOptions::new(Arc::new(SqliteDialect))
                    .db_opts(tmp.db_opts)
                    .encryption(encryption),
            )
        };
        assert!(open(None).is_err());
        assert!(open(Some(EncryptionOpts {
            cipher: CIPHER_B.to_string(),
            hexkey: KEY_A.to_string(),
        }))
        .is_err());

        let cached = open(Some(EncryptionOpts {
            cipher: CIPHER_A.to_string(),
            hexkey: KEY_A.to_string(),
        }))?;
        assert!(Arc::ptr_eq(&cached, &tmp.db));
        assert!(cached
            .connect(ConnectOpts::new(turso_core::SessionRole::Superuser))
            .is_err());
        let wrong = cached.connect(
            ConnectOpts::new(turso_core::SessionRole::Superuser)
                .with_encryption_key(Some(EncryptionKey::from_hex_string(KEY_B)?)),
        );
        assert!(matches!(
            wrong,
            Err(LimboError::CompletionError(
                CompletionError::DecryptionError { .. }
            ))
        ));
        let reader = cached.connect(
            ConnectOpts::new(turso_core::SessionRole::Superuser)
                .with_encryption_key(Some(EncryptionKey::from_hex_string(KEY_A)?)),
        )?;
        let rows: Vec<(String,)> = reader.exec_rows("SELECT value FROM secrets");
        assert_eq!(rows, [("protected value".to_string(),)]);
        assert!(Arc::ptr_eq(
            &reader.current_schema(),
            &writer.current_schema()
        ));
        let connect_async = |key: Option<EncryptionKey>| -> turso_core::Result<_> {
            let mut state = turso_core::ConnectAsyncState::new();
            let options =
                ConnectOpts::new(turso_core::SessionRole::Superuser).with_encryption_key(key);
            loop {
                match cached
                    .connect_async(options.clone(), &mut state)
                    .map_err(|err| *err)?
                {
                    IOResult::Done(conn) => break Ok(conn),
                    IOResult::IO(pending) => pending.wait(&*tmp.io)?,
                }
            }
        };
        assert!(connect_async(None).is_err());
        assert!(connect_async(Some(EncryptionKey::from_hex_string(KEY_B)?)).is_err());
        let async_reader = connect_async(Some(EncryptionKey::from_hex_string(KEY_A)?))?;
        let rows: Vec<(String,)> = async_reader.exec_rows("SELECT value FROM secrets");
        assert_eq!(rows, [("protected value".to_string(),)]);
        let wrong_uri = format!(
            "file:{}?cipher={CIPHER_A}&hexkey={KEY_B}",
            tmp.path.display()
        );
        assert!(
            turso_core::Connection::from_uri(&wrong_uri, tmp.db_opts, Arc::new(SqliteDialect),)
                .is_err()
        );
    }
    Ok(())
}

#[test]
fn test_encrypted_database_open_in_progress_rejects_invalid_credentials() -> anyhow::Result<()> {
    use crate::queued_io::QueuedIo;

    for creator_finishes in [false, true] {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("opening-encrypted.db");
        let path_str = path.to_str().unwrap();
        let io: Arc<dyn IO> = Arc::new(QueuedIo::new_with_io(Arc::new(PlatformIO::new()?)));
        let file = io.open_file(path_str, OpenFlags::Create, true)?;
        let storage = Arc::new(turso_core::storage::database::DatabaseFile::new(file));
        let base = OpenOptions::new(Arc::new(SqliteDialect))
            .storage(storage)
            .db_opts(DatabaseOpts::new().with_encryption(true));
        let encrypted = base.clone().encryption(EncryptionOpts {
            cipher: CIPHER_A.to_string(),
            hexkey: KEY_A.to_string(),
        });
        let creator_options = encrypted
            .clone()
            .db_opts(DatabaseOpts::new().with_encryption(true).with_mvcc(true));
        let wrong = base.clone().encryption(EncryptionOpts {
            cipher: CIPHER_A.to_string(),
            hexkey: KEY_B.to_string(),
        });
        let mut creator = turso_core::OpenDbAsyncState::new();
        loop {
            match Database::open_async(&mut creator, io.clone(), path_str, &creator_options)? {
                IOResult::Done(_) => panic!("creator must yield before publishing the database"),
                IOResult::IO(pending) => pending.wait(io.as_ref())?,
            }
            if std::fs::metadata(&path)?.len() > 0 {
                break;
            }
        }
        assert_eq!(&std::fs::read(&path)?[..5], b"Turso");

        for options in [&base, &wrong] {
            let mut reader = turso_core::OpenDbAsyncState::new();
            let IOResult::IO(pending) =
                Database::open_async(&mut reader, io.clone(), path_str, options)?
            else {
                panic!("reader must wait while the creator is opening the database");
            };
            assert!(pending.is_explicit_yield());
        }
        let drive = |state: &mut turso_core::OpenDbAsyncState,
                     options: &OpenOptions|
         -> turso_core::Result<Arc<Database>> {
            loop {
                match Database::open_async(state, io.clone(), path_str, options)
                    .map_err(|err| *err)?
                {
                    IOResult::Done(db) => break Ok(db),
                    IOResult::IO(pending) => pending.wait(io.as_ref())?,
                }
            }
        };
        let owner = if creator_finishes {
            Some(drive(&mut creator, &creator_options)?)
        } else {
            None
        };
        drop(creator);
        assert!(drive(&mut turso_core::OpenDbAsyncState::new(), &base).is_err());
        if let Ok(db) = drive(&mut turso_core::OpenDbAsyncState::new(), &wrong) {
            assert!(db
                .connect(
                    ConnectOpts::new(turso_core::SessionRole::Superuser)
                        .with_encryption_key(Some(EncryptionKey::from_hex_string(KEY_B)?)),
                )
                .is_err());
        }
        let db = drive(&mut turso_core::OpenDbAsyncState::new(), &encrypted)?;
        if let Some(owner) = owner {
            assert!(Arc::ptr_eq(&db, &owner));
        }
        let conn = db.connect(
            ConnectOpts::new(turso_core::SessionRole::Superuser)
                .with_encryption_key(Some(EncryptionKey::from_hex_string(KEY_A)?)),
        )?;
        conn.execute("CREATE TABLE correct_key(value TEXT)")?;
    }
    Ok(())
}

#[test]
fn test_new_encrypted_database_checks_keys_after_first_write() -> anyhow::Result<()> {
    let tmp = TempDatabaseBuilder::new()
        .with_encryption(CIPHER_A, KEY_A)
        .with_page_size(8192)
        .build();
    let writer = tmp.connect_limbo();
    assert!(tmp
        .db
        .connect(ConnectOpts::new(turso_core::SessionRole::Superuser))
        .is_err());
    assert_wrong_key_refusal(
        tmp.db.connect(
            ConnectOpts::new(turso_core::SessionRole::Superuser)
                .with_encryption_key(Some(EncryptionKey::from_hex_string(KEY_B)?)),
        ),
    );
    let reader = tmp.connect_limbo();
    assert_eq!(std::fs::metadata(&tmp.path)?.len(), 0);
    reader.execute("PRAGMA journal_mode = 'mvcc'")?;
    writer.execute("CREATE TABLE secrets(value TEXT)")?;
    writer.execute("INSERT INTO secrets VALUES ('protected value')")?;
    let bytes = std::fs::read(&tmp.path)?;
    assert_eq!(&bytes[..5], b"Turso");
    assert_eq!(u16::from_be_bytes([bytes[16], bytes[17]]), 8192);
    assert!(tmp
        .db
        .connect(ConnectOpts::new(turso_core::SessionRole::Superuser))
        .is_err());
    assert_wrong_key_refusal(
        tmp.db.connect(
            ConnectOpts::new(turso_core::SessionRole::Superuser)
                .with_encryption_key(Some(EncryptionKey::from_hex_string(KEY_B)?)),
        ),
    );
    let rows: Vec<(String,)> = reader.exec_rows("SELECT value FROM secrets");
    assert_eq!(rows, [("protected value".to_string(),)]);
    assert!(Arc::ptr_eq(
        &reader.current_schema(),
        &writer.current_schema()
    ));
    Ok(())
}

#[test]
fn test_first_encrypted_page_write_is_not_replaced_by_another_connection() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("encrypted.db");
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new()?);
    let db = Database::open(
        io.clone(),
        path.to_str().unwrap(),
        OpenOptions::new(Arc::new(SqliteDialect))
            .db_opts(DatabaseOpts::new().with_encryption(true))
            .encryption(EncryptionOpts {
                cipher: CIPHER_A.into(),
                hexkey: KEY_A.into(),
            }),
    )?;
    let first = db.connect(
        ConnectOpts::new(turso_core::SessionRole::Superuser)
            .with_encryption_key(Some(EncryptionKey::from_hex_string(KEY_A)?)),
    )?;
    let IOResult::IO(pending) = first.get_pager().maybe_allocate_page1()? else {
        panic!("first page allocation must write page 1");
    };
    pending.wait(&*io)?;
    assert_wrong_key_refusal(
        db.connect(
            ConnectOpts::new(turso_core::SessionRole::Superuser)
                .with_encryption_key(Some(EncryptionKey::from_hex_string(KEY_B)?)),
        ),
    );
    first.execute("CREATE TABLE secrets(value TEXT)")?;
    first.execute("INSERT INTO secrets VALUES ('protected value')")?;
    let rows: Vec<(String,)> = first.exec_rows("SELECT value FROM secrets");
    assert_eq!(rows, [("protected value".to_string(),)]);
    Ok(())
}

#[test]
fn test_failed_encryption_setup_preserves_plaintext_database() -> anyhow::Result<()> {
    let tmp = TempDatabase::new_empty();
    let conn = tmp.connect_limbo();
    conn.execute("CREATE TABLE data(value TEXT)")?;
    conn.execute("INSERT INTO data VALUES ('original value')")?;
    assert!(conn.pragma_update("cipher", "'aegis256'").is_err());
    assert!(conn.pragma_update("hexkey", format!("'{KEY_A}'")).is_err());
    let rows: Vec<(String,)> = conn.exec_rows("SELECT value FROM data");
    assert_eq!(rows, [("original value".to_string(),)]);
    Ok(())
}

#[test]
fn test_failed_initial_write_keeps_encryption_identity() -> anyhow::Result<()> {
    use crate::queued_io::{QueuedIo, QueuedIoOpKind};

    for failure in [QueuedIoOpKind::Pwrite, QueuedIoOpKind::Sync] {
        let io = Arc::new(QueuedIo::new());
        let path = ":memory:encrypted-initialization-error";
        let db = Database::open(
            io.clone(),
            path,
            OpenOptions::new(Arc::new(SqliteDialect))
                .flags(OpenFlags::Create)
                .db_opts(DatabaseOpts::new().with_encryption(true))
                .encryption(EncryptionOpts {
                    cipher: CIPHER_A.to_string(),
                    hexkey: KEY_A.to_string(),
                }),
        )?;
        let conn = db.connect(
            ConnectOpts::new(turso_core::SessionRole::Superuser)
                .with_encryption_key(Some(EncryptionKey::from_hex_string(KEY_A)?)),
        )?;
        io.fault_after(path, failure, 0);
        assert!(conn.execute("CREATE TABLE secrets(value TEXT)").is_err());
        io.clear_fault();
        let page_size = conn.get_page_size().get();
        assert!(conn.execute("PRAGMA page_size = 8192").is_err());
        assert_eq!(conn.get_page_size().get(), page_size);
        assert!(db
            .connect(
                ConnectOpts::new(turso_core::SessionRole::Superuser)
                    .with_encryption_key(Some(EncryptionKey::from_hex_string(KEY_B)?)),
            )
            .is_err());
    }
    Ok(())
}

#[test]
fn test_encryption_validation_recovers_from_read_error() -> anyhow::Result<()> {
    use crate::queued_io::{QueuedIo, QueuedIoOpKind};
    for asynchronous in [false, true] {
        let io = Arc::new(QueuedIo::new());
        let db = Database::open(
            io.clone(),
            ":memory:encryption-validation-error",
            ReadLockDatabase::options_for(KEY_A),
        )?;
        let opts = ConnectOpts::new(turso_core::SessionRole::Superuser)
            .with_encryption_key(Some(EncryptionKey::from_hex_string(KEY_A)?));
        let writer = db.connect(opts.clone())?;
        writer.execute("CREATE TABLE secrets(value TEXT)")?;
        writer.execute("INSERT INTO secrets VALUES ('protected value')")?;
        io.fault_after("", QueuedIoOpKind::Pread, 0);
        if asynchronous {
            let mut state = turso_core::ConnectAsyncState::new();
            let result: turso_core::Result<()> = loop {
                match db.connect_async(opts.clone(), &mut state) {
                    Ok(IOResult::Done(_)) => break Ok(()),
                    Ok(IOResult::IO(pending)) => {
                        if let Err(error) = pending.wait(io.as_ref()) {
                            break Err(error);
                        }
                    }
                    Err(error) => break Err(*error),
                }
            };
            assert!(result.is_err());
        } else {
            assert!(db.connect(opts.clone()).is_err());
        }
        io.clear_fault();
        let reader = db.connect(opts)?;
        let rows: Vec<(String,)> = reader.exec_rows("SELECT value FROM secrets");
        assert_eq!(rows, [("protected value".to_string(),)]);
        writer.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
    }
    Ok(())
}

#[test]
fn test_wrong_key_is_rejected_after_dropping_initial_writer() -> anyhow::Result<()> {
    use crate::queued_io::QueuedIo;

    let io = Arc::new(QueuedIo::new());
    let db = Database::open(
        io.clone(),
        ":memory:encrypted-initialization-drop",
        OpenOptions::new(Arc::new(SqliteDialect))
            .flags(OpenFlags::Create)
            .db_opts(DatabaseOpts::new().with_encryption(true))
            .encryption(EncryptionOpts {
                cipher: CIPHER_A.to_string(),
                hexkey: KEY_A.to_string(),
            }),
    )?;
    let first = db.connect(
        ConnectOpts::new(turso_core::SessionRole::Superuser)
            .with_encryption_key(Some(EncryptionKey::from_hex_string(KEY_A)?)),
    )?;
    let IOResult::IO(pending) = first.get_pager().maybe_allocate_page1()? else {
        panic!("first page allocation must write page 1");
    };
    drop(first);
    pending.wait(&*io)?;
    assert!(db
        .connect(ConnectOpts::new(turso_core::SessionRole::Superuser))
        .is_err());
    assert_wrong_key_refusal(
        db.connect(
            ConnectOpts::new(turso_core::SessionRole::Superuser)
                .with_encryption_key(Some(EncryptionKey::from_hex_string(KEY_B)?)),
        ),
    );
    Ok(())
}

// Refusing a wrong key must release the WAL read lock so a valid connection
// can truncate the WAL. Found by the encryption fuzzer; the refusal leaked
// this lock on main (4b3befbd6) as well.

#[test]
fn test_wrong_key_connection_releases_its_read_lock() -> anyhow::Result<()> {
    let database = ReadLockDatabase::new("wrong-key-connection")?;
    let refused = database.db.connect(
        ConnectOpts::new(turso_core::SessionRole::Superuser)
            .with_encryption_key(Some(EncryptionKey::from_hex_string(KEY_B)?)),
    );
    assert_wrong_key_refusal(refused);
    database.assert_truncate_checkpoint_succeeds("after a wrong-key connection was refused");
    Ok(())
}

#[test]
fn test_wrong_key_connection_releases_its_read_lock_when_page_one_is_in_the_main_file(
) -> anyhow::Result<()> {
    let database = ReadLockDatabase::new("wrong-key-connection-main-file")?;
    database.assert_truncate_checkpoint_succeeds("before any refusal");
    let refused = database.db.connect(
        ConnectOpts::new(turso_core::SessionRole::Superuser)
            .with_encryption_key(Some(EncryptionKey::from_hex_string(KEY_B)?)),
    );
    assert_wrong_key_refusal(refused);
    database
        .owner
        .execute("INSERT INTO secrets VALUES ('more')")?;
    database.assert_truncate_checkpoint_succeeds("after a wrong-key connection was refused");
    Ok(())
}

#[test]
fn test_wrong_key_open_of_a_cached_database_releases_its_read_lock() -> anyhow::Result<()> {
    let database = ReadLockDatabase::new("wrong-key-cached-open")?;
    let refused = Database::open(database.io.clone(), &database.path, database.options(KEY_B));
    assert_wrong_key_refusal(refused);
    database.assert_truncate_checkpoint_succeeds("after a wrong-key open was refused");
    Ok(())
}

#[test]
fn test_wrong_key_attach_releases_its_read_lock() -> anyhow::Result<()> {
    let database = ReadLockDatabase::new("wrong-key-attach")?;
    let main = Database::open(
        database.io.clone(),
        &format!("{}-main", database.path),
        OpenOptions::new(Arc::new(SqliteDialect)).db_opts(
            DatabaseOpts::new()
                .with_encryption(ENABLE_ENCRYPTION)
                .with_attach(true),
        ),
    )?;
    let main = main.connect(ConnectOpts::new(turso_core::SessionRole::Superuser))?;
    let refused = main.execute(format!(
        "ATTACH 'file:{}?cipher=aegis256&hexkey={KEY_B}' AS aux",
        database.path
    ));
    assert!(
        matches!(
            refused,
            Err(LimboError::CompletionError(
                CompletionError::DecryptionError { .. }
            ))
        ),
        "ATTACH with a wrong key must fail to decrypt page 1: {refused:?}"
    );
    database.assert_truncate_checkpoint_succeeds("after a wrong-key ATTACH was refused");
    Ok(())
}

#[test]
fn test_missing_key_refusals_leave_no_read_lock() -> anyhow::Result<()> {
    let database = ReadLockDatabase::new("missing-key")?;
    let refused = database
        .db
        .connect(ConnectOpts::new(turso_core::SessionRole::Superuser));
    assert!(
        matches!(&refused, Err(LimboError::InvalidArgument(message)) if message.contains("requires an encryption key")),
        "a keyless connection must be refused: {:?}",
        refused.err()
    );
    let main = Database::open(
        database.io.clone(),
        &format!("{}-main", database.path),
        OpenOptions::new(Arc::new(SqliteDialect)).db_opts(
            DatabaseOpts::new()
                .with_encryption(ENABLE_ENCRYPTION)
                .with_attach(true),
        ),
    )?;
    let main = main.connect(ConnectOpts::new(turso_core::SessionRole::Superuser))?;
    assert!(main
        .execute(format!("ATTACH '{}' AS aux", database.path))
        .is_err());
    database.assert_truncate_checkpoint_succeeds("after missing-key refusals");
    Ok(())
}

/// An encrypted WAL database (AEGIS-256, KEY_A) with one row, kept open
/// through a valid connection.
struct ReadLockDatabase {
    inner: Arc<turso_core::MemoryIO>,
    io: Arc<crate::queued_io::QueuedIo>,
    path: String,
    db: Arc<Database>,
    owner: Arc<turso_core::Connection>,
}

impl ReadLockDatabase {
    fn new(name: &str) -> anyhow::Result<Self> {
        let inner = Arc::new(turso_core::MemoryIO::new());
        let io = Arc::new(crate::queued_io::QueuedIo::new_with_io(inner.clone()));
        // Not a `:memory:` name: ATTACH of such a name opens a new empty database.
        let path = format!(":read-lock-{name}:");
        let db = Database::open(io.clone(), &path, Self::options_for(KEY_A))?;
        let owner = db.connect(
            ConnectOpts::new(turso_core::SessionRole::Superuser)
                .with_encryption_key(Some(EncryptionKey::from_hex_string(KEY_A)?)),
        )?;
        owner.execute("CREATE TABLE secrets(value TEXT)")?;
        owner.execute("INSERT INTO secrets VALUES ('protected value')")?;
        Ok(Self {
            inner,
            io,
            path,
            db,
            owner,
        })
    }

    fn options(&self, hexkey: &str) -> OpenOptions {
        Self::options_for(hexkey)
    }

    fn options_for(hexkey: &str) -> OpenOptions {
        OpenOptions::new(Arc::new(SqliteDialect))
            .db_opts(
                DatabaseOpts::new()
                    .with_encryption(ENABLE_ENCRYPTION)
                    .with_attach(true),
            )
            .encryption(EncryptionOpts {
                cipher: "aegis256".to_string(),
                hexkey: hexkey.to_string(),
            })
    }

    /// No other connection may hold a read lock: the checkpoint must not be
    /// busy and must leave an empty WAL.
    fn assert_truncate_checkpoint_succeeds(&self, when: &str) {
        let rows = self
            .owner
            .prepare("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap()
            .run_collect_rows()
            .unwrap();
        let busy = rows[0][0].to_string();
        assert_eq!(busy, "0", "TRUNCATE checkpoint was busy {when}: {rows:?}");
        let wal = self
            .inner
            .open_file(&format!("{}-wal", self.path), OpenFlags::Create, false)
            .unwrap();
        assert_eq!(wal.size().unwrap(), 0, "the WAL was not truncated {when}");
    }
}

#[test]
fn test_cached_database_open_async_rejects_wrong_encryption_key() -> anyhow::Result<()> {
    let tmp = TempDatabaseBuilder::new()
        .with_encryption(CIPHER_A, KEY_A)
        .with_mvcc(true)
        .build();
    let owner = tmp.connect_limbo();
    owner.execute("CREATE TABLE secrets(value TEXT)")?;
    owner.execute("INSERT INTO secrets VALUES ('protected value')")?;
    let path = tmp.path.to_str().unwrap();
    let file = tmp.io.open_file(path, OpenFlags::default(), false)?;
    let options = OpenOptions::new(Arc::new(SqliteDialect))
        .storage(Arc::new(turso_core::storage::database::DatabaseFile::new(
            file,
        )))
        .db_opts(tmp.db_opts)
        .encryption(EncryptionOpts {
            cipher: CIPHER_A.to_string(),
            hexkey: KEY_B.to_string(),
        });
    let mut state = turso_core::OpenDbAsyncState::new();
    let opened: turso_core::Result<Arc<Database>> = loop {
        match Database::open_async(&mut state, tmp.io.clone(), path, &options)
            .map_err(|error| *error)
        {
            Ok(IOResult::Done(database)) => break Ok(database),
            Ok(IOResult::IO(pending)) => pending.wait(tmp.io.as_ref())?,
            Err(error) => break Err(error),
        }
    };

    assert_wrong_key_refusal(opened);
    Ok(())
}

fn assert_wrong_key_refusal<T>(refused: turso_core::Result<T>) {
    assert!(
        matches!(
            refused,
            Err(LimboError::CompletionError(
                CompletionError::DecryptionError { .. }
            ))
        ),
        "a wrong key must fail to decrypt page 1"
    );
}

#[test]
fn cached_open_with_different_io_uses_only_the_verifier() -> anyhow::Result<()> {
    use crate::queued_io::QueuedIo;
    let tmp = TempDatabaseBuilder::new()
        .with_encryption(CIPHER_A, KEY_A)
        .with_page_size(8192)
        .build();
    let owner = tmp.connect_limbo();
    owner.execute("PRAGMA journal_mode = 'mvcc'")?;
    owner.execute("CREATE TABLE secrets(value TEXT)")?;
    owner.execute("INSERT INTO secrets VALUES ('protected value')")?;
    owner.execute("PRAGMA user_version = 42")?;
    let other_io = Arc::new(QueuedIo::new_with_io(Arc::new(PlatformIO::new()?)));
    let options = |key: &str| {
        OpenOptions::new(Arc::new(SqliteDialect))
            .db_opts(tmp.db_opts)
            .encryption(EncryptionOpts {
                cipher: CIPHER_A.into(),
                hexkey: key.into(),
            })
    };
    assert_wrong_key_refusal(Database::open(
        other_io.clone(),
        tmp.path.to_str().unwrap(),
        options(KEY_B),
    ));
    let cached = Database::open(other_io.clone(), tmp.path.to_str().unwrap(), options(KEY_A))?;
    assert!(Arc::ptr_eq(&cached, &tmp.db));
    let mut state = turso_core::OpenDbAsyncState::new();
    let options = options(KEY_A).storage(tmp.db.db_file.clone());
    let IOResult::Done(cached) = Database::open_async(
        &mut state,
        other_io.clone(),
        tmp.path.to_str().unwrap(),
        &options,
    )?
    else {
        panic!("cached authentication must not yield");
    };
    assert!(Arc::ptr_eq(&cached, &tmp.db));
    assert!(other_io.records().is_empty());
    assert!(other_io.pending_events().is_empty());
    assert_eq!(owner.get_page_size().get(), 8192);
    let version: Vec<(i64,)> = owner.exec_rows("PRAGMA user_version");
    assert_eq!(version, [(42,)]);
    Ok(())
}

#[test]
fn connect_async_yields_and_cancellation_releases_read_transactions() -> anyhow::Result<()> {
    for wrong in [false, true] {
        let database = ReadLockDatabase::new(if wrong {
            "cancel-wrong"
        } else {
            "cancel-correct"
        })?;
        let opts = ConnectOpts::new(turso_core::SessionRole::Superuser).with_encryption_key(Some(
            EncryptionKey::from_hex_string(if wrong { KEY_B } else { KEY_A })?,
        ));
        let mut cancelled = 0;
        for stop_after in 0..16 {
            let mut state = turso_core::ConnectAsyncState::new();
            let mut finished = false;
            for step in 0..=stop_after {
                match database.db.connect_async(opts.clone(), &mut state) {
                    Ok(IOResult::IO(pending)) if step < stop_after => {
                        let _ = pending.wait(database.io.as_ref());
                    }
                    Ok(IOResult::IO(pending)) => {
                        assert!(
                            !pending.finished(),
                            "async connect must return before page I/O completes"
                        );
                        cancelled += 1;
                    }
                    Ok(IOResult::Done(_)) => {
                        assert!(!wrong);
                        finished = true;
                        break;
                    }
                    Err(error) => {
                        assert!(wrong);
                        assert!(matches!(
                            *error,
                            LimboError::CompletionError(CompletionError::DecryptionError { .. })
                        ));
                        finished = true;
                        break;
                    }
                }
            }
            drop(state);
            database.io.drain_all()?;
            database.assert_truncate_checkpoint_succeeds("after connect state cancellation");
            let rows: Vec<(String,)> = database.owner.exec_rows("SELECT value FROM secrets");
            assert_eq!(rows, [("protected value".into(),)]);
            if finished {
                break;
            }
        }
        if wrong {
            assert_eq!(cancelled, 0, "a cached wrong key must fail before page I/O");
        } else {
            assert!(cancelled > 0, "reading page 1 must yield");
        }
    }
    Ok(())
}

#[test]
fn encrypted_reload_accepts_an_unwritten_database() -> anyhow::Result<()> {
    for cipher in [CIPHER_A, CIPHER_B] {
        let tmp = TempDatabaseBuilder::new()
            .with_encryption(cipher, KEY_A)
            .build();
        let owner = tmp.connect_limbo();
        assert_eq!(std::fs::metadata(&tmp.path)?.len(), 0);
        assert!(tmp.db.reload_wal_after_external_restore().is_err());
        assert!(tmp
            .db
            .reload_wal_after_external_restore_with_encryption_key(Some(
                EncryptionKey::from_hex_string(KEY_B)?,
            ))
            .is_err());
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

#[test]
fn memory_uri_credentials_and_attach_feature_gate_are_checked() -> anyhow::Result<()> {
    let opts = DatabaseOpts::new().with_encryption(true);
    for uri in ["file::memory:?cipher=aegis256", "file::memory:?hexkey=01"] {
        assert!(turso_core::Connection::from_uri(uri, opts, Arc::new(SqliteDialect)).is_err());
    }
    let uri = format!("file::memory:?cipher={CIPHER_A}&hexkey={KEY_A}");
    let (_, connection) = turso_core::Connection::from_uri(&uri, opts, Arc::new(SqliteDialect))?;
    assert_eq!(
        connection.get_encryption_cipher_mode().unwrap().to_string(),
        CIPHER_A
    );
    let tmp = TempDatabaseBuilder::new()
        .with_opts(DatabaseOpts::new().with_attach(true))
        .build();
    let conn = tmp.connect_limbo();
    let path = tmp.path.with_file_name("disabled-attachment.db");
    conn.execute(format!(
        "ATTACH 'file:{}?cipher={CIPHER_A}&hexkey={KEY_A}' AS aux",
        path.display()
    ))?;
    conn.execute("CREATE TABLE aux.data(value)")?;
    conn.execute("DETACH aux")?;
    assert!(!std::fs::read(&path)?.starts_with(b"SQLite format 3\0"));
    Ok(())
}

#[test]
fn vacuum_rejects_encrypted_attachments_and_preserves_plaintext_attachments() -> anyhow::Result<()>
{
    let tmp = TempDatabaseBuilder::new()
        .with_opts(DatabaseOpts::new().with_encryption(true).with_attach(true))
        .with_encryption(CIPHER_A, KEY_A)
        .build();
    let owner = tmp.connect_limbo();
    let encrypted = tmp.path.with_file_name("encrypted-aux.db");
    owner.execute(format!(
        "ATTACH 'file:{}?cipher={CIPHER_B}&hexkey={KEY_B}' AS encrypted_aux",
        encrypted.display()
    ))?;
    owner.execute("CREATE TABLE encrypted_aux.t(x)")?;
    let before = std::fs::read(&encrypted)?;
    assert!(owner.execute("VACUUM encrypted_aux").is_err());
    assert_eq!(std::fs::read(&encrypted)?, before);
    let plain = tmp.path.with_file_name("plain-aux.db");
    owner.execute(format!("ATTACH '{}' AS plain_aux", plain.display()))?;
    owner.execute("CREATE TABLE plain_aux.t(x)")?;
    owner.execute("INSERT INTO plain_aux.t VALUES (42)")?;
    assert!(owner.execute("VACUUM plain_aux").is_err());
    let rows: Vec<(i64,)> = owner.exec_rows("SELECT x FROM plain_aux.t");
    assert_eq!(rows, [(42,)]);
    assert_eq!(&std::fs::read(&plain)?[..16], b"SQLite format 3\0");
    Ok(())
}

#[test]
fn cold_encrypted_attach_authenticates_before_catalog_publication() -> anyhow::Result<()> {
    use crate::queued_io::QueuedIo;
    let mut yielded = false;
    let mut completed = false;
    for stop_after in 0..32 {
        let io = Arc::new(QueuedIo::new());
        let path = format!(":cold-attach-{stop_after}:");
        let target = Database::open(io.clone(), &path, ReadLockDatabase::options_for(KEY_A))?;
        let owner = target.connect(
            ConnectOpts::new(turso_core::SessionRole::Superuser)
                .with_encryption_key(Some(EncryptionKey::from_hex_string(KEY_A)?)),
        )?;
        owner.execute("CREATE TABLE secrets(value TEXT)")?;
        owner.execute("INSERT INTO secrets VALUES ('protected value')")?;
        owner.close()?;
        drop((owner, target));
        let main = Database::open(
            io.clone(),
            &format!("{path}-main"),
            OpenOptions::new(Arc::new(SqliteDialect))
                .db_opts(DatabaseOpts::new().with_encryption(true).with_attach(true)),
        )?;
        let conn = main.connect(ConnectOpts::new(turso_core::SessionRole::Superuser))?;
        let mut statement = conn.prepare(format!(
            "ATTACH 'file:{path}?cipher={CIPHER_A}&hexkey={KEY_A}' AS aux"
        ))?;
        let mut done = false;
        for step in 0..=stop_after {
            match statement.step()? {
                turso_core::StepResult::Done => {
                    done = true;
                    break;
                }
                turso_core::StepResult::IO => {
                    yielded = true;
                    assert!(!conn.attached_database_names().contains(&"aux".to_string()));
                    if step < stop_after {
                        io.step_one()?;
                    }
                }
                other => panic!("unexpected ATTACH result: {other:?}"),
            }
        }
        drop(statement);
        io.drain_all()?;
        if done {
            completed = true;
            let rows: Vec<(String,)> = conn.exec_rows("SELECT value FROM aux.secrets");
            assert_eq!(rows, [("protected value".into(),)]);
            break;
        }
        assert!(!conn.attached_database_names().contains(&"aux".to_string()));
    }
    assert!(
        yielded && completed,
        "ATTACH pager authentication must yield and eventually complete"
    );
    Ok(())
}

#[test]
fn regression_async_connect_state_cannot_change_database() -> anyhow::Result<()> {
    use crate::queued_io::QueuedIo;
    let io = Arc::new(QueuedIo::new());
    let a = Database::open(
        io.clone(),
        ":async-state-origin:",
        ReadLockDatabase::options_for(KEY_A),
    )?;
    let owner_a = a.connect(
        ConnectOpts::new(turso_core::SessionRole::Superuser)
            .with_encryption_key(Some(EncryptionKey::from_hex_string(KEY_A)?)),
    )?;
    owner_a.execute("CREATE TABLE a_data(value TEXT)")?;
    let b = Database::open(
        io.clone(),
        ":async-state-other:",
        ReadLockDatabase::options_for(KEY_B),
    )?;
    let owner_b = b.connect(
        ConnectOpts::new(turso_core::SessionRole::Superuser)
            .with_encryption_key(Some(EncryptionKey::from_hex_string(KEY_B)?)),
    )?;
    owner_b.execute("PRAGMA journal_mode = 'mvcc'")?;
    owner_b.execute("CREATE TABLE secrets(value TEXT)")?;
    owner_b.execute("INSERT INTO secrets VALUES ('protected value')")?;
    let opts = ConnectOpts::new(turso_core::SessionRole::Superuser)
        .with_encryption_key(Some(EncryptionKey::from_hex_string(KEY_A)?));
    let mut state = turso_core::ConnectAsyncState::new();
    let IOResult::IO(pending) = a.connect_async(opts.clone(), &mut state)? else {
        panic!("page-1 read must yield");
    };
    pending.wait(io.as_ref())?;
    assert!(
        b.connect_async(
            ConnectOpts::new(turso_core::SessionRole::Superuser),
            &mut state
        )
        .is_err(),
        "a pending connect state must reject another database"
    );
    let connected = loop {
        match a.connect_async(opts.clone(), &mut state)? {
            IOResult::Done(conn) => break conn,
            IOResult::IO(pending) => pending.wait(io.as_ref())?,
        }
    };
    assert!(Arc::ptr_eq(&connected.get_pager().db_file, &a.db_file));
    assert!(!connected
        .current_schema()
        .has_table(turso_core::NamespaceId::ROOT, "secrets"));
    let rows: Vec<(String,)> = owner_b.exec_rows("SELECT value FROM secrets");
    assert_eq!(rows, [("protected value".into(),)]);
    Ok(())
}

#[test]
fn regression_public_header_mutation_preserves_encrypted_layout() -> anyhow::Result<()> {
    use turso_core::storage::sqlite3_ondisk::PageSize;
    use turso_core::IOExt;
    let tmp = TempDatabaseBuilder::new()
        .with_encryption(CIPHER_A, KEY_A)
        .build();
    let owner = tmp.connect_limbo();
    owner.execute("CREATE TABLE secrets(value TEXT)")?;
    owner.execute("BEGIN IMMEDIATE")?;
    owner.execute("INSERT INTO secrets VALUES ('protected value')")?;
    let pager = owner.get_pager();
    for change_size in [false, true] {
        let result = tmp.io.block(|| {
            pager.with_header_mut(|header| {
                header.user_version = 42.into();
                if change_size {
                    header.page_size = PageSize::new(8192).unwrap();
                } else {
                    header.reserved_space = 0;
                }
            })
        });
        assert!(
            result.is_err(),
            "a public header mutation must reject encrypted layout changes"
        );
        let header = tmp.io.block(|| {
            pager.with_header(|header| {
                (
                    header.page_size.get(),
                    header.reserved_space,
                    header.user_version.get(),
                )
            })
        })?;
        assert_eq!(header, (4096, 48, 0));
    }
    tmp.io
        .block(|| pager.with_header_mut(|header| header.user_version = 7.into()))?;
    owner.execute("COMMIT")?;
    let peer = tmp.connect_limbo();
    let rows: Vec<(String,)> = peer.exec_rows("SELECT value FROM secrets");
    assert_eq!(rows, [("protected value".into(),)]);
    assert_eq!(
        tmp.io.block(|| peer
            .get_pager()
            .with_header(|header| header.user_version.get()))?,
        7
    );
    Ok(())
}

#[test]
fn async_connect_authenticates_before_resolving_the_original_role() -> anyhow::Result<()> {
    use crate::queued_io::QueuedIo;
    let io = Arc::new(QueuedIo::new());
    let db = Database::open(
        io.clone(),
        ":async-encrypted-session-role:",
        ReadLockDatabase::options_for(KEY_A),
    )?;
    let owner = db.connect(
        ConnectOpts::new(SessionRole::Superuser)
            .with_encryption_key(Some(EncryptionKey::from_hex_string(KEY_A)?)),
    )?;
    owner.execute("CREATE TABLE secrets(value TEXT)")?;
    let named = |key: &str| -> anyhow::Result<ConnectOpts> {
        Ok(ConnectOpts::new(SessionRole::Named("missing_role".into()))
            .with_encryption_key(Some(EncryptionKey::from_hex_string(key)?)))
    };
    let wrong = db.connect(named(KEY_B)?).err().unwrap();
    assert!(matches!(
        wrong,
        LimboError::CompletionError(CompletionError::DecryptionError { .. })
    ));
    let correct = db.connect(named(KEY_A)?).err().unwrap();
    assert!(correct.to_string().contains("missing_role"));
    let mut state = turso_core::ConnectAsyncState::new();
    let IOResult::IO(pending) = db.connect_async(named(KEY_A)?, &mut state)? else {
        panic!("page-1 authentication must yield");
    };
    pending.wait(io.as_ref())?;
    loop {
        match db.connect_async(ConnectOpts::new(SessionRole::Superuser), &mut state) {
            Ok(IOResult::IO(pending)) => pending.wait(io.as_ref())?,
            Ok(IOResult::Done(_)) => panic!("resumption must retain the original named role"),
            Err(error) => {
                assert!(error.to_string().contains("missing_role"));
                break;
            }
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

/// A TEMP database chooses its encryption when it is created, so ATTACH of an encrypted
/// database must not leave an unencrypted TEMP file in use. An empty unencrypted TEMP file is
/// dropped, so that its next use creates it encrypted. One that holds objects, or that a
/// transaction uses, makes the ATTACH fail before the target file is created.
#[test]
fn encrypted_attach_drops_an_empty_unencrypted_temp_file_and_refuses_a_used_one(
) -> anyhow::Result<()> {
    let tmp = TempDatabaseBuilder::new()
        .with_opts(DatabaseOpts::new().with_attach(true))
        .build();
    let target = |name: &str| tmp.path.with_file_name(format!("{name}.db"));
    let attach = |conn: &Arc<turso_core::Connection>, name: &str| {
        conn.execute(format!(
            "ATTACH 'file:{}?cipher={CIPHER_A}&hexkey={KEY_A}' AS {name}",
            target(name).display()
        ))
    };

    // A read of the TEMP schema creates an empty TEMP file.
    let conn = tmp.connect_limbo();
    let _: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM temp.sqlite_schema");
    assert!(conn.has_temp_database());
    attach(&conn, "after_read")?;
    assert!(!conn.has_temp_database());
    conn.execute("CREATE TEMP TABLE copied AS SELECT 'secret' AS x")?;
    let rows: Vec<(String,)> = conn.exec_rows("SELECT x FROM temp.copied");
    assert_eq!(rows, [("secret".to_string(),)]);

    // A TEMP table that was dropped leaves an empty TEMP file.
    let conn = tmp.connect_limbo();
    conn.execute("CREATE TEMP TABLE scratch(x)")?;
    conn.execute("DROP TABLE temp.scratch")?;
    attach(&conn, "after_drop")?;
    assert!(!conn.has_temp_database());

    // The first use of TEMP after a DETACH creates an unencrypted TEMP file.
    let conn = tmp.connect_limbo();
    attach(&conn, "detached")?;
    conn.execute("DETACH detached")?;
    let _: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM temp.sqlite_schema");
    attach(&conn, "after_detach")?;
    assert!(!conn.has_temp_database());

    // A dropped AUTOINCREMENT table leaves an empty sqlite_sequence, which the user cannot
    // drop. An empty sqlite_sequence or sqlite_stat1 holds no data.
    let conn = tmp.connect_limbo();
    conn.execute("CREATE TEMP TABLE counted(id INTEGER PRIMARY KEY AUTOINCREMENT, x)")?;
    conn.execute("INSERT INTO counted(x) VALUES (1)")?;
    conn.execute("CREATE INDEX temp.counted_x ON counted(x)")?;
    conn.execute("ANALYZE temp")?;
    let error = attach(&conn, "after_counters").unwrap_err();
    assert!(error.to_string().contains("TEMP objects"), "{error}");
    conn.execute("DROP TABLE temp.counted")?;
    // DROP TABLE does not delete the statistics of the table, as SQLite does.
    conn.execute("DELETE FROM temp.sqlite_stat1")?;
    let tables: Vec<(String,)> =
        conn.exec_rows("SELECT name FROM temp.sqlite_schema ORDER BY name");
    assert_eq!(
        tables,
        [
            ("sqlite_sequence".to_string(),),
            ("sqlite_stat1".to_string(),)
        ]
    );
    attach(&conn, "after_counters")?;
    assert!(!conn.has_temp_database());

    // Rows in sqlite_sequence or sqlite_stat1 are data, so the ATTACH fails and keeps them.
    for (table, name_column, row) in [
        ("sqlite_sequence", "name", "('kept', 42)"),
        ("sqlite_stat1", "tbl", "('kept', NULL, '42')"),
    ] {
        let conn = tmp.connect_limbo();
        conn.execute("CREATE TEMP TABLE counted(id INTEGER PRIMARY KEY AUTOINCREMENT, x)")?;
        conn.execute("INSERT INTO counted(x) VALUES (1)")?;
        conn.execute("CREATE INDEX temp.counted_x ON counted(x)")?;
        conn.execute("ANALYZE temp")?;
        conn.execute("DROP TABLE temp.counted")?;
        conn.execute("DELETE FROM temp.sqlite_stat1")?;
        conn.execute(format!("INSERT INTO temp.{table} VALUES {row}"))?;
        let name = format!("with_{table}_rows");
        let error = attach(&conn, &name).unwrap_err();
        assert!(error.to_string().contains(table), "{error}");
        assert!(!target(&name).exists());
        let rows: Vec<(String,)> =
            conn.exec_rows(&format!("SELECT {name_column} FROM temp.{table}"));
        assert_eq!(rows, [("kept".to_string(),)]);
        conn.execute(format!("DELETE FROM temp.{table}"))?;
        attach(&conn, &name)?;
        assert!(!conn.has_temp_database());
    }

    // The user_version and application_id of the TEMP database would be lost.
    for pragma in ["user_version", "application_id"] {
        let conn = tmp.connect_limbo();
        conn.execute(format!("PRAGMA temp.{pragma} = 7"))?;
        let name = format!("after_{pragma}");
        let error = attach(&conn, &name).unwrap_err();
        assert!(error.to_string().contains(pragma), "{error}");
        assert!(!target(&name).exists());
        let rows: Vec<(i64,)> = conn.exec_rows(&format!("PRAGMA temp.{pragma}"));
        assert_eq!(rows, [(7,)]);
        conn.execute(format!("PRAGMA temp.{pragma} = 0"))?;
        attach(&conn, &name)?;
        assert!(!conn.has_temp_database());
    }

    // The TEMP database that replaces the dropped one keeps the page size that PRAGMA
    // temp.page_size set. `after_read` is an encrypted database that exists.
    let conn = tmp.connect_limbo();
    let temp_page_size = |conn: &Arc<turso_core::Connection>| {
        let rows: Vec<(i64,)> = conn.exec_rows("PRAGMA temp.page_size");
        rows[0].0
    };
    conn.execute("PRAGMA temp.page_size = 8192")?;
    attach(&conn, "after_read")?;
    assert!(!conn.has_temp_database());
    assert_eq!(temp_page_size(&conn), 8192);
    conn.execute("CREATE TEMP TABLE kept_page_size(a)")?;
    assert_eq!(temp_page_size(&conn), 8192);

    // A transaction that uses the empty TEMP file keeps it until the transaction ends.
    let conn = tmp.connect_limbo();
    conn.execute("BEGIN")?;
    let _: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM temp.sqlite_schema");
    let error = attach(&conn, "in_transaction").unwrap_err();
    assert!(error.to_string().contains("transaction"), "{error}");
    assert!(!target("in_transaction").exists());
    conn.execute("COMMIT")?;
    attach(&conn, "in_transaction")?;

    // TEMP objects in an unencrypted file make the ATTACH fail.
    let conn = tmp.connect_limbo();
    conn.execute("CREATE TEMP TABLE scratch(x)")?;
    conn.execute("INSERT INTO scratch VALUES (1)")?;
    let error = attach(&conn, "with_objects").unwrap_err();
    assert!(error.to_string().contains("TEMP objects"), "{error}");
    assert!(!target("with_objects").exists());
    // A URI with a key but no cipher fails with its own error.
    let error = conn
        .execute(format!(
            "ATTACH 'file:{}?hexkey={KEY_A}' AS with_objects",
            target("with_objects").display()
        ))
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("cipher is required when hexkey is provided"),
        "{error}"
    );
    // A plaintext database can still be attached.
    conn.execute(format!(
        "ATTACH '{}' AS plain",
        target("plaintext").display()
    ))?;
    let rows: Vec<(i64,)> = conn.exec_rows("SELECT x FROM temp.scratch");
    assert_eq!(rows, [(1,)]);
    // A view and a trigger are objects too.
    conn.execute("DROP TABLE temp.scratch")?;
    conn.execute("CREATE TEMP VIEW numbers AS SELECT 1 AS x")?;
    let error = attach(&conn, "with_objects").unwrap_err();
    assert!(error.to_string().contains("TEMP objects"), "{error}");
    conn.execute("DROP VIEW temp.numbers")?;
    conn.execute("CREATE TABLE main.watched(x)")?;
    conn.execute("CREATE TEMP TRIGGER watcher AFTER INSERT ON main.watched BEGIN SELECT 1; END")?;
    let error = attach(&conn, "with_objects").unwrap_err();
    assert!(error.to_string().contains("TEMP objects"), "{error}");
    conn.execute("DROP TRIGGER temp.watcher")?;
    attach(&conn, "with_objects")?;
    Ok(())
}

/// The name of the test that `temp_storage_of_encrypted_database_holds_no_plaintext` runs in a
/// child process.
#[cfg(unix)]
const TEMP_STORAGE_CHILD_TEST: &str =
    "query_processing::encryption::temp_storage_of_encrypted_database_child_process";
/// The directory that holds all temporary files of the child process. The child scans it.
#[cfg(unix)]
const TEMP_STORAGE_SCAN_DIR_ENV: &str = "TURSO_TEST_TEMP_STORAGE_SCAN_DIR";
/// The directory that holds the databases of the child process. It is not scanned.
#[cfg(unix)]
const TEMP_STORAGE_DB_DIR_ENV: &str = "TURSO_TEST_TEMP_STORAGE_DB_DIR";
/// Every value in the encrypted table starts with this text.
#[cfg(unix)]
const TEMP_STORAGE_MARKER: &str = "plaintext-marker-7d41c9e2";
/// The number of rows in the encrypted table.
#[cfg(unix)]
const TEMP_STORAGE_ROWS: i64 = 2000;

/// Rows that an encrypted database copies to temporary files on disk must not be in plaintext
/// there, both when the main database is encrypted and when only an attached database is.
/// This test runs the child process with TMPDIR and TURSO_TMPDIR set to a directory of the
/// test, so that the child can scan every temporary file that the engine creates.
#[cfg(unix)]
#[test]
fn temp_storage_of_encrypted_database_holds_no_plaintext() -> anyhow::Result<()> {
    let scan_dir = tempfile::tempdir()?;
    let db_dir = tempfile::tempdir()?;
    let output = std::process::Command::new(std::env::current_exe()?)
        .args([TEMP_STORAGE_CHILD_TEST, "--exact", "--nocapture"])
        .env(TEMP_STORAGE_SCAN_DIR_ENV, scan_dir.path())
        .env(TEMP_STORAGE_DB_DIR_ENV, db_dir.path())
        .env("TMPDIR", scan_dir.path())
        .env("TURSO_TMPDIR", scan_dir.path())
        .output()?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success() && stdout.contains("1 passed"),
        "child process failed\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    Ok(())
}

/// The body of `temp_storage_of_encrypted_database_holds_no_plaintext`. It does nothing
/// unless that test starts it.
#[cfg(unix)]
#[test]
fn temp_storage_of_encrypted_database_child_process() -> anyhow::Result<()> {
    let (Some(scan_dir), Some(db_dir)) = (
        std::env::var_os(TEMP_STORAGE_SCAN_DIR_ENV),
        std::env::var_os(TEMP_STORAGE_DB_DIR_ENV),
    ) else {
        return Ok(());
    };
    let scan_dir = std::path::PathBuf::from(scan_dir);
    let db_dir = std::path::PathBuf::from(db_dir);
    let spills = Arc::new(SpillWrites::default());
    let io: Arc<dyn IO> = Arc::new(SpillWatchingIo {
        inner: Arc::new(PlatformIO::new()?),
        spills: spills.clone(),
    });
    let mut failures = Vec::new();

    // The main database is encrypted.
    let (db, conn) = open_temp_storage_database(
        &io,
        &db_dir.join("encrypted-main.db"),
        Some((CIPHER_A, KEY_A)),
    )?;
    fill_temp_storage_table(&conn, "main")?;
    check_spills(&conn, "main", "secrets", &spills, &mut failures)?;
    check_temp_table(&conn, "main", &scan_dir, &mut failures)?;
    // Dropping the connection deletes its TEMP database, so that the next scans see only the
    // files of the next connection.
    conn.close()?;
    drop(conn);
    drop(db);

    // Only an attached database is encrypted. The TEMP database exists, empty, before the
    // ATTACH.
    let (db, conn) = open_temp_storage_database(&io, &db_dir.join("plaintext-main.db"), None)?;
    let _: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM temp.sqlite_schema");
    conn.execute(format!(
        "ATTACH 'file:{}?cipher={CIPHER_A}&hexkey={KEY_B}' AS aux",
        db_dir.join("encrypted-aux.db").display()
    ))?;
    fill_temp_storage_table(&conn, "aux")?;
    check_spills(&conn, "aux", "secrets", &spills, &mut failures)?;
    check_temp_table(&conn, "aux", &scan_dir, &mut failures)?;

    // After DETACH, the TEMP database still holds rows of the encrypted database.
    conn.execute("DETACH aux")?;
    check_spills(&conn, "temp", "copied", &spills, &mut failures)?;
    check_temp_files(
        &scan_dir,
        "TEMP database after DETACH",
        Some("tursodb-temp.db"),
        &mut failures,
    )?;
    conn.close()?;
    drop(conn);
    drop(db);

    // The TEMP database has a page size other than that of the main database.
    let (db, conn) = open_temp_storage_database(
        &io,
        &db_dir.join("encrypted-main.db"),
        Some((CIPHER_A, KEY_A)),
    )?;
    conn.execute("PRAGMA temp.page_size = 8192")?;
    conn.execute("CREATE TEMP TABLE large_pages AS SELECT value FROM main.secrets")?;
    let rows: Vec<(i64,)> = conn.exec_rows("PRAGMA temp.page_size");
    assert_eq!(rows, [(8192,)]);
    check_temp_files(
        &scan_dir,
        "TEMP database with 8 KiB pages",
        Some("tursodb-temp.db"),
        &mut failures,
    )?;
    check_spills(&conn, "temp", "large_pages", &spills, &mut failures)?;
    conn.close()?;
    drop(conn);
    drop(db);

    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

/// Opens the database at `path`, encrypted with `encryption` if it is set.
#[cfg(unix)]
fn open_temp_storage_database(
    io: &Arc<dyn IO>,
    path: &std::path::Path,
    encryption: Option<(&str, &str)>,
) -> anyhow::Result<(Arc<Database>, Arc<turso_core::Connection>)> {
    // Small pages make an ephemeral table spill to its file with less data.
    let mut options = OpenOptions::new(Arc::new(SqliteDialect))
        .db_opts(
            DatabaseOpts::new()
                .with_encryption(encryption.is_some())
                .with_attach(true),
        )
        .page_size(turso_core::storage::sqlite3_ondisk::PageSize::new(1024).unwrap());
    let mut key = None;
    if let Some((cipher, hexkey)) = encryption {
        options = options.encryption(EncryptionOpts {
            cipher: cipher.into(),
            hexkey: hexkey.into(),
        });
        key = Some(EncryptionKey::from_hex_string(hexkey)?);
    }
    let db = Database::open(io.clone(), path.to_str().unwrap(), options)?;
    let conn = db.connect(ConnectOpts::new(SessionRole::Superuser).with_encryption_key(key))?;
    Ok((db, conn))
}

/// Creates the table `secrets` in the database `schema`. Every value starts with
/// `TEMP_STORAGE_MARKER`.
#[cfg(unix)]
fn fill_temp_storage_table(conn: &Arc<turso_core::Connection>, schema: &str) -> anyhow::Result<()> {
    conn.execute(format!(
        "CREATE TABLE {schema}.secrets(id INTEGER PRIMARY KEY, value TEXT)"
    ))?;
    conn.execute(format!(
        "INSERT INTO {schema}.secrets(value) SELECT '{TEMP_STORAGE_MARKER}' \
         || printf('%06d', value) || hex(randomblob(600)) \
         FROM generate_series(1, {TEMP_STORAGE_ROWS})"
    ))?;
    Ok(())
}

/// Makes a sorter, a hash table and an ephemeral index write the rows of `schema.table` to
/// their files, and records in `failures` each statement that wrote a row in plaintext or
/// wrote nothing.
#[cfg(unix)]
fn check_spills(
    conn: &Arc<turso_core::Connection>,
    schema: &str,
    table: &str,
    spills: &SpillWrites,
    failures: &mut Vec<String>,
) -> anyhow::Result<()> {
    let source = format!("{schema}.{table}");
    let mut check = |statement: &str, spill_expected: bool| {
        let (bytes, plaintext_writes) = spills.take();
        if spill_expected && bytes == 0 {
            failures.push(format!("{source}: {statement} wrote no spill file"));
        }
        if plaintext_writes > 0 {
            failures.push(format!(
                "{source}: {statement} wrote {plaintext_writes} times in plaintext"
            ));
        }
    };
    spills.take();

    // UNION keeps its rows in an ephemeral index, which is larger than its page cache.
    let rows: Vec<(i64,)> = conn.exec_rows(&format!(
        "SELECT count(*) FROM (SELECT value FROM {source} UNION SELECT value FROM {source})"
    ));
    assert_eq!(rows, [(TEMP_STORAGE_ROWS,)]);
    check("UNION", true);

    // DISTINCT keeps its keys in a hash table. Debug builds give a hash table 32 KiB of
    // memory, so it writes partitions to its file.
    let rows: Vec<(String,)> = conn.exec_rows(&format!("SELECT DISTINCT value FROM {source}"));
    assert_eq!(rows.len() as i64, TEMP_STORAGE_ROWS);
    check("DISTINCT", cfg!(debug_assertions));

    // A 64 KiB sort buffer makes ORDER BY and CREATE INDEX write sorted runs to their files.
    let cache_size: Vec<(i64,)> = conn.exec_rows("PRAGMA cache_size");
    conn.execute("PRAGMA cache_size = -64")?;
    let rows: Vec<(String,)> =
        conn.exec_rows(&format!("SELECT value FROM {source} ORDER BY value DESC"));
    assert_eq!(rows.len() as i64, TEMP_STORAGE_ROWS);
    assert!(rows[0]
        .0
        .starts_with(&format!("{TEMP_STORAGE_MARKER}{TEMP_STORAGE_ROWS:06}")));
    check("ORDER BY", true);

    conn.execute(format!(
        "CREATE INDEX {schema}.{table}_value ON {table}(value)"
    ))?;
    let rows: Vec<(i64,)> = conn.exec_rows(&format!(
        "SELECT count(*) FROM {source} WHERE value > '{TEMP_STORAGE_MARKER}'"
    ));
    assert_eq!(rows, [(TEMP_STORAGE_ROWS,)]);
    check("CREATE INDEX", true);
    conn.execute(format!("PRAGMA cache_size = {}", cache_size[0].0))?;
    Ok(())
}

/// Copies the rows of `schema.secrets` to a TEMP table, and records in `failures` each file
/// of the TEMP database that holds one of them in plaintext.
#[cfg(unix)]
fn check_temp_table(
    conn: &Arc<turso_core::Connection>,
    schema: &str,
    scan_dir: &std::path::Path,
    failures: &mut Vec<String>,
) -> anyhow::Result<()> {
    conn.execute(format!(
        "CREATE TEMP TABLE copied AS SELECT value FROM {schema}.secrets"
    ))?;
    let rows: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM temp.copied");
    assert_eq!(rows, [(TEMP_STORAGE_ROWS,)]);
    check_temp_files(
        scan_dir,
        &format!("{schema}: CREATE TEMP TABLE ... AS SELECT"),
        Some("tursodb-temp.db"),
        failures,
    )
}

/// Records in `failures` each file under `dir` that contains `TEMP_STORAGE_MARKER`. Also
/// records a missing file whose name starts with `expected_file`, because then `step` did not
/// write to disk and the scan proves nothing.
#[cfg(unix)]
fn check_temp_files(
    dir: &std::path::Path,
    step: &str,
    expected_file: Option<&str>,
    failures: &mut Vec<String>,
) -> anyhow::Result<()> {
    let mut found_expected_file = false;
    let mut dirs = vec![dir.to_path_buf()];
    while let Some(dir) = dirs.pop() {
        for entry in std::fs::read_dir(&dir)? {
            let path = entry?.path();
            if path.is_dir() {
                dirs.push(path);
                continue;
            }
            found_expected_file |= path.file_name().is_some_and(|name| {
                expected_file.is_some_and(|expected| name.to_string_lossy().starts_with(expected))
            });
            if contains_temp_storage_marker(&std::fs::read(&path)?) {
                failures.push(format!("{step}: plaintext in {}", path.display()));
            }
        }
    }
    if let Some(expected) = expected_file.filter(|_| !found_expected_file) {
        failures.push(format!("{step}: no {expected} file in {}", dir.display()));
    }
    Ok(())
}

#[cfg(unix)]
fn contains_temp_storage_marker(bytes: &[u8]) -> bool {
    bytes
        .windows(TEMP_STORAGE_MARKER.len())
        .any(|window| window == TEMP_STORAGE_MARKER.as_bytes())
}

/// What `SpillWatchingIo` saw written to spill files.
#[cfg(unix)]
#[derive(Default)]
struct SpillWrites {
    bytes: std::sync::atomic::AtomicUsize,
    plaintext_writes: std::sync::atomic::AtomicUsize,
}

#[cfg(unix)]
impl SpillWrites {
    fn record(&self, bytes: &[u8]) {
        use std::sync::atomic::Ordering;
        self.bytes.fetch_add(bytes.len(), Ordering::SeqCst);
        if contains_temp_storage_marker(bytes) {
            self.plaintext_writes.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// Returns the bytes written and the writes in plaintext since the last call.
    fn take(&self) -> (usize, usize) {
        use std::sync::atomic::Ordering;
        (
            self.bytes.swap(0, Ordering::SeqCst),
            self.plaintext_writes.swap(0, Ordering::SeqCst),
        )
    }
}

/// An IO that records in `spills` every write to a spill file of a sorter, hash table or
/// ephemeral table. These files are deleted when their statement ends, so a scan of the
/// directory after the statement cannot see them.
#[cfg(unix)]
struct SpillWatchingIo {
    inner: Arc<dyn IO>,
    spills: Arc<SpillWrites>,
}

#[cfg(unix)]
impl turso_core::Clock for SpillWatchingIo {
    fn current_time_monotonic(&self) -> turso_core::MonotonicInstant {
        self.inner.current_time_monotonic()
    }

    fn current_time_wall_clock(&self) -> turso_core::WallClockInstant {
        self.inner.current_time_wall_clock()
    }
}

#[cfg(unix)]
impl IO for SpillWatchingIo {
    fn open_file(
        &self,
        path: &str,
        flags: OpenFlags,
        direct: bool,
    ) -> turso_core::Result<Arc<dyn turso_core::File>> {
        let file = self.inner.open_file(path, flags, direct)?;
        if !path.ends_with("tursodb_temp_file") {
            return Ok(file);
        }
        Ok(Arc::new(SpillWatchingFile {
            inner: file,
            spills: self.spills.clone(),
        }))
    }

    fn remove_file(&self, path: &str) -> turso_core::Result<()> {
        self.inner.remove_file(path)
    }

    fn file_id(&self, path: &str) -> turso_core::Result<turso_core::io::FileId> {
        self.inner.file_id(path)
    }

    fn step(&self) -> turso_core::Result<()> {
        self.inner.step()
    }

    fn cancel(&self, completions: &[turso_core::Completion]) -> turso_core::Result<()> {
        self.inner.cancel(completions)
    }

    fn drain_completions(&self, completions: &[turso_core::Completion]) -> turso_core::Result<()> {
        self.inner.drain_completions(completions)
    }

    fn wait_for_completion(&self, completion: turso_core::Completion) -> turso_core::Result<()> {
        self.inner.wait_for_completion(completion)
    }

    fn generate_random_number(&self) -> i64 {
        self.inner.generate_random_number()
    }

    fn fill_bytes(&self, dest: &mut [u8]) {
        self.inner.fill_bytes(dest);
    }

    fn yield_now(&self) {
        self.inner.yield_now();
    }
}

#[cfg(unix)]
struct SpillWatchingFile {
    inner: Arc<dyn turso_core::File>,
    spills: Arc<SpillWrites>,
}

#[cfg(unix)]
impl turso_core::File for SpillWatchingFile {
    fn lock_file(&self, exclusive: bool) -> turso_core::Result<()> {
        self.inner.lock_file(exclusive)
    }

    fn unlock_file(&self) -> turso_core::Result<()> {
        self.inner.unlock_file()
    }

    fn pread(
        &self,
        pos: u64,
        c: turso_core::Completion,
    ) -> turso_core::Result<turso_core::Completion> {
        self.inner.pread(pos, c)
    }

    fn pwrite(
        &self,
        pos: u64,
        buffer: Arc<turso_core::Buffer>,
        c: turso_core::Completion,
    ) -> turso_core::Result<turso_core::Completion> {
        self.spills.record(buffer.as_slice());
        self.inner.pwrite(pos, buffer, c)
    }

    fn pwritev(
        &self,
        pos: u64,
        buffers: Vec<Arc<turso_core::Buffer>>,
        c: turso_core::Completion,
    ) -> turso_core::Result<turso_core::Completion> {
        for buffer in &buffers {
            self.spills.record(buffer.as_slice());
        }
        self.inner.pwritev(pos, buffers, c)
    }

    fn sync(
        &self,
        c: turso_core::Completion,
        sync_type: turso_core::io::FileSyncType,
    ) -> turso_core::Result<turso_core::Completion> {
        self.inner.sync(c, sync_type)
    }

    fn size(&self) -> turso_core::Result<u64> {
        self.inner.size()
    }

    fn truncate(
        &self,
        len: u64,
        c: turso_core::Completion,
    ) -> turso_core::Result<turso_core::Completion> {
        self.inner.truncate(len, c)
    }
}

/// While a database of the connection is encrypted, the TEMP database is encrypted too. Still,
/// PRAGMA temp.page_size sets its page size before any TEMP object is created, as for an
/// unencrypted TEMP database, and does nothing once the TEMP database holds data.
#[test]
fn pragma_temp_page_size_sets_the_page_size_of_an_encrypted_temp_database() -> anyhow::Result<()> {
    let tmp = TempDatabaseBuilder::new().build();
    let open = |name: &str| -> anyhow::Result<Arc<turso_core::Connection>> {
        let uri = format!(
            "file:{}?cipher={CIPHER_A}&hexkey={KEY_A}",
            tmp.path.with_file_name(format!("{name}.db")).display()
        );
        let opts = DatabaseOpts::new().with_encryption(true);
        Ok(turso_core::Connection::from_uri(&uri, opts, Arc::new(SqliteDialect))?.1)
    };
    let temp_page_size = |conn: &Arc<turso_core::Connection>| {
        let rows: Vec<(i64,)> = conn.exec_rows("PRAGMA temp.page_size");
        rows[0].0
    };

    // Before the first use of TEMP.
    let conn = open("before_first_use")?;
    conn.execute("PRAGMA temp.page_size = 8192")?;
    assert_eq!(temp_page_size(&conn), 8192);
    conn.execute(
        "CREATE TEMP TABLE large_pages AS SELECT value AS id, hex(randomblob(300)) AS payload \
         FROM generate_series(1, 3000)",
    )?;
    assert_eq!(temp_page_size(&conn), 8192);
    // Small page caches make the TEMP database read its pages back from its file, and make
    // the sort, the hash table of DISTINCT and the index of UNION spill to their files.
    conn.execute("PRAGMA temp.cache_size = -64")?;
    conn.execute("PRAGMA cache_size = -64")?;
    let rows: Vec<(i64, i64)> = conn.exec_rows(
        "SELECT count(DISTINCT payload), count(*) \
         FROM (SELECT payload FROM temp.large_pages ORDER BY payload)",
    );
    assert_eq!(rows, [(3000, 3000)]);
    let rows: Vec<(i64,)> = conn.exec_rows(
        "SELECT count(*) FROM \
         (SELECT payload FROM temp.large_pages UNION SELECT payload FROM temp.large_pages)",
    );
    assert_eq!(rows, [(3000,)]);
    let rows: Vec<(String,)> = conn.exec_rows("PRAGMA temp.integrity_check");
    assert_eq!(rows, [("ok".to_string(),)]);

    // The TEMP database exists but has not written its first page, also inside a transaction.
    let conn = open("before_first_page")?;
    conn.execute("BEGIN")?;
    let _: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM temp.sqlite_schema");
    conn.execute("PRAGMA temp.page_size = 16384")?;
    conn.execute("CREATE TEMP TABLE t(x)")?;
    conn.execute("INSERT INTO t VALUES (1)")?;
    conn.execute("COMMIT")?;
    assert_eq!(temp_page_size(&conn), 16384);
    let rows: Vec<(i64,)> = conn.exec_rows("SELECT x FROM temp.t");
    assert_eq!(rows, [(1,)]);

    // Once the TEMP database holds data, the PRAGMA does nothing. Only the TEMP database can
    // take a page size after it was opened encrypted.
    let conn = open("after_first_page")?;
    conn.execute("CREATE TEMP TABLE t(x)")?;
    conn.execute("PRAGMA temp.page_size = 8192")?;
    assert_eq!(temp_page_size(&conn), 4096);
    let error = conn.execute("PRAGMA main.page_size = 8192").unwrap_err();
    assert!(error.to_string().contains("page size is fixed"), "{error}");
    Ok(())
}
