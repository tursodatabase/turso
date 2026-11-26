//! Tests for `PRAGMA cipher` and `PRAGMA hexkey`, which encrypt a new,
//! empty database. Most tests also check that a defect of the old PRAGMA
//! path does not come back: a keyless connection next to an encrypted
//! database, a cached database that ignores the PRAGMA encryption, keys that
//! apply inside a transaction or to the wrong database, and plaintext writes
//! after a setup that failed or is not complete.

use crate::common::ExecRows;
use std::sync::Arc;
use turso_core::{
    ConnectOpts, Connection, Database, DatabaseOpts, EncryptionKey, EncryptionOpts, OpenFlags,
    OpenOptions, PlatformIO, SessionRole, StepResult, IO,
};
use turso_sqlite::SqliteDialect;

const CIPHER: &str = "aegis256";
const KEY: &str = "b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327";
const OTHER_KEY: &str = "a2ccfeb5f690ed0ebf01a843f22222f11ed11d9348213f6d8113b677ae84ad38";
const KEY_128: &str = "b1bbfda4f589dc9daaf004fe21111e00";
const MARKER: &str = "pragma-encryption-marker";
const REFUSED: &str = "encryption must be configured when opening the database";
const KEY_SET: &str = "encryption key is set for this session";
const KEY_NOT_SET: &str = "encryption key is not set for this session";

struct Env {
    directory: tempfile::TempDir,
    io: Arc<dyn IO>,
    path: String,
}

impl Env {
    fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("test.db");
        Self {
            path: path.to_str().unwrap().to_string(),
            directory,
            io: Arc::new(PlatformIO::new().unwrap()),
        }
    }

    fn path_of(&self, name: &str) -> String {
        self.directory
            .path()
            .join(name)
            .to_str()
            .unwrap()
            .to_string()
    }

    fn options(&self, db_opts: DatabaseOpts) -> OpenOptions {
        OpenOptions::new(Arc::new(SqliteDialect)).db_opts(db_opts)
    }

    /// Opens the database without encryption options, like `tursodb test.db`.
    fn open(&self) -> Arc<Database> {
        Database::open(
            self.io.clone(),
            &self.path,
            self.options(DatabaseOpts::new().with_encryption(true)),
        )
        .unwrap()
    }

    fn open_keyed(&self, cipher: &str, hexkey: &str) -> turso_core::Result<Arc<Database>> {
        Database::open(
            self.io.clone(),
            &self.path,
            self.options(DatabaseOpts::new().with_encryption(true))
                .encryption(EncryptionOpts {
                    cipher: cipher.into(),
                    hexkey: hexkey.into(),
                }),
        )
    }

    fn size(&self) -> u64 {
        std::fs::metadata(&self.path).unwrap().len()
    }

    fn header(&self) -> Vec<u8> {
        std::fs::read(&self.path).unwrap()[..100].to_vec()
    }

    /// Returns true if any file of the database directory holds `text`.
    fn holds(&self, text: &str) -> bool {
        std::fs::read_dir(self.directory.path())
            .unwrap()
            .map(|entry| std::fs::read(entry.unwrap().path()).unwrap())
            .any(|bytes| {
                bytes
                    .windows(text.len())
                    .any(|window| window == text.as_bytes())
            })
    }
}

fn connect(db: &Arc<Database>, hexkey: Option<&str>) -> turso_core::Result<Arc<Connection>> {
    db.connect(
        ConnectOpts::new(SessionRole::Superuser).with_encryption_key(
            hexkey.map(|hexkey| EncryptionKey::from_hex_string(hexkey).unwrap()),
        ),
    )
}

fn cipher_sql(cipher: &str) -> String {
    format!("PRAGMA cipher = '{cipher}'")
}

fn key_sql(hexkey: &str) -> String {
    format!("PRAGMA hexkey = '{hexkey}'")
}

fn encrypt(conn: &Arc<Connection>) {
    conn.execute(cipher_sql(CIPHER)).unwrap();
    conn.execute(key_sql(KEY)).unwrap();
}

fn fill(conn: &Arc<Connection>) {
    conn.execute("CREATE TABLE t(v TEXT)").unwrap();
    conn.execute(format!("INSERT INTO t VALUES ('{MARKER}')"))
        .unwrap();
}

fn markers(conn: &Arc<Connection>) -> Vec<(String,)> {
    conn.exec_rows("SELECT v FROM t")
}

fn hexkey_state(conn: &Arc<Connection>) -> String {
    let rows: Vec<(String,)> = conn.exec_rows("PRAGMA hexkey");
    rows[0].0.clone()
}

fn active_cipher(conn: &Arc<Connection>) -> Vec<(String,)> {
    conn.exec_rows("PRAGMA cipher")
}

#[track_caller]
fn assert_refused(result: turso_core::Result<()>, reason: &str) {
    let error = result.unwrap_err().to_string();
    assert!(error.contains(REFUSED), "{error}");
    assert!(error.contains(reason), "{error}");
}

/// Reopens the closed database cold and checks that only the right
/// credentials open it and that the rows survived.
fn assert_reopens_encrypted(env: &Env, cipher: &str, hexkey: &str) {
    assert!(env.header().starts_with(b"Turso"));
    assert!(!env.holds(MARKER), "plaintext row found on disk");
    assert!(Database::open(
        env.io.clone(),
        &env.path,
        env.options(DatabaseOpts::new().with_encryption(true))
    )
    .is_err());
    let wrong_key = if hexkey == OTHER_KEY { KEY } else { OTHER_KEY };
    let wrong_key = &wrong_key[..hexkey.len()];
    assert!(env.open_keyed(cipher, wrong_key).is_err());
    let db = env.open_keyed(cipher, hexkey).unwrap();
    let conn = connect(&db, Some(hexkey)).unwrap();
    assert_eq!(markers(&conn), [(MARKER.to_string(),)]);
    assert_eq!(active_cipher(&conn), [(cipher.to_string(),)]);
}

#[test]
fn pragma_cipher_and_hexkey_encrypt_a_new_database() {
    for cipher_first in [true, false] {
        let env = Env::new();
        {
            let db = env.open();
            let conn = connect(&db, None).unwrap();
            assert_eq!(hexkey_state(&conn), KEY_NOT_SET);
            if cipher_first {
                conn.execute(cipher_sql(CIPHER)).unwrap();
                conn.execute(key_sql(KEY)).unwrap();
            } else {
                conn.execute(key_sql(KEY)).unwrap();
                conn.execute(cipher_sql(CIPHER)).unwrap();
            }
            assert_eq!(hexkey_state(&conn), KEY_SET);
            assert_eq!(active_cipher(&conn), [(CIPHER.to_string(),)]);
            assert_eq!(env.size(), 0, "the PRAGMAs wrote the database");
            fill(&conn);
            assert!(!env.holds(MARKER), "plaintext row found in the WAL");
            conn.close().unwrap();
        }
        assert_reopens_encrypted(&env, CIPHER, KEY);
    }
}

#[test]
fn page_size_set_before_the_encryption_pragmas_is_kept() {
    let env = Env::new();
    {
        let db = env.open();
        let conn = connect(&db, None).unwrap();
        conn.execute("PRAGMA page_size = 8192").unwrap();
        encrypt(&conn);
        // The page layout of an encrypted database does not change after that.
        assert!(conn.execute("PRAGMA page_size = 4096").is_err());
        fill(&conn);
        conn.close().unwrap();
    }
    let header = env.header();
    assert_eq!(u16::from_be_bytes([header[16], header[17]]), 8192);
    // AEGIS-256 stores a 32-byte nonce and a 16-byte tag in each page.
    assert_eq!(header[20], 48);
    assert_reopens_encrypted(&env, CIPHER, KEY);
    let db = env.open_keyed(CIPHER, KEY).unwrap();
    let conn = connect(&db, Some(KEY)).unwrap();
    let page_size: Vec<(i64,)> = conn.exec_rows("PRAGMA page_size");
    assert_eq!(page_size, [(8192,)]);
}

#[test]
fn encryption_pragmas_before_mvcc_encrypt_the_logical_log() {
    let env = Env::new();
    {
        let db = env.open();
        let conn = connect(&db, None).unwrap();
        encrypt(&conn);
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        fill(&conn);
        assert!(!env.holds(MARKER), "plaintext row found in the logical log");
        conn.close().unwrap();
    }
    assert_reopens_encrypted(&env, CIPHER, KEY);
}

/// A key PRAGMA on a database that has pages used to install the key
/// without checking it, and every later read failed.
#[test]
fn encryption_pragmas_are_refused_on_a_database_with_pages() {
    let env = Env::new();
    {
        let db = env.open();
        let conn = connect(&db, None).unwrap();
        fill(&conn);
        assert_refused(conn.execute(cipher_sql(CIPHER)), "not empty");
        assert_refused(conn.execute(key_sql(KEY)), "not empty");
        assert_eq!(markers(&conn), [(MARKER.to_string(),)]);
        conn.close().unwrap();
    }
    let db = env.open();
    let conn = connect(&db, None).unwrap();
    assert_refused(conn.execute(key_sql(KEY)), "not empty");
    assert_eq!(markers(&conn), [(MARKER.to_string(),)]);
    assert!(env.header().starts_with(b"SQLite format 3\0"));
}

#[test]
fn encryption_pragmas_are_refused_on_an_encrypted_database() {
    let env = Env::new();
    let db = env.open_keyed(CIPHER, KEY).unwrap();
    let conn = connect(&db, Some(KEY)).unwrap();
    assert_refused(conn.execute(key_sql(OTHER_KEY)), "already encrypted");
    assert_refused(conn.execute(cipher_sql("aes256gcm")), "already encrypted");
    assert_eq!(active_cipher(&conn), [(CIPHER.to_string(),)]);

    // The same holds after the PRAGMAs encrypted a database.
    let env = Env::new();
    let db = env.open();
    let conn = connect(&db, None).unwrap();
    encrypt(&conn);
    assert_refused(conn.execute(key_sql(OTHER_KEY)), "already encrypted");
    assert_refused(conn.execute(cipher_sql("aes256gcm")), "already encrypted");
    fill(&conn);
    assert_eq!(markers(&conn), [(MARKER.to_string(),)]);
}

/// The old PRAGMA path encrypted only the connection that ran it: other
/// connections kept plaintext pagers, and the process-wide registry gave
/// the database to keyless openers and refused the right key.
#[test]
fn encryption_pragmas_wait_for_other_connections_and_bind_the_database() {
    let env = Env::new();
    let db = env.open();
    let owner = connect(&db, None).unwrap();
    let peer = connect(&db, None).unwrap();
    assert_refused(
        owner.execute(cipher_sql(CIPHER)),
        "other connections use the database",
    );
    assert_eq!(hexkey_state(&owner), KEY_NOT_SET);
    peer.close().unwrap();
    drop(peer);

    encrypt(&owner);
    assert!(connect(&db, None).is_err());
    assert!(connect(&db, Some(OTHER_KEY)).is_err());
    assert!(Database::open(
        env.io.clone(),
        &env.path,
        env.options(DatabaseOpts::new().with_encryption(true))
    )
    .is_err());
    assert!(env.open_keyed(CIPHER, OTHER_KEY).is_err());
    assert!(env.open_keyed("aes256gcm", KEY).is_err());
    let cached = env.open_keyed(CIPHER, KEY).unwrap();
    assert!(Arc::ptr_eq(&cached, &db));
    let second = connect(&cached, Some(KEY)).unwrap();
    fill(&owner);
    assert_eq!(markers(&second), [(MARKER.to_string(),)]);
    let third = connect(&cached, Some(KEY)).unwrap();
    assert_eq!(markers(&third), [(MARKER.to_string(),)]);
}

#[test]
fn encryption_pragmas_wait_for_an_attached_copy_of_the_database() {
    let env = Env::new();
    let target = env.open();
    let owner = connect(&target, None).unwrap();
    let main = Database::open(
        env.io.clone(),
        &env.path_of("main.db"),
        env.options(DatabaseOpts::new().with_encryption(true).with_attach(true)),
    )
    .unwrap();
    let main_conn = connect(&main, None).unwrap();
    main_conn
        .execute(format!("ATTACH '{}' AS aux", env.path))
        .unwrap();
    assert_refused(
        owner.execute(cipher_sql(CIPHER)),
        "other connections use the database",
    );
    main_conn.execute("DETACH aux").unwrap();

    encrypt(&owner);
    fill(&owner);
    assert!(main_conn
        .execute(format!("ATTACH '{}' AS aux", env.path))
        .is_err());
    main_conn
        .execute(format!(
            "ATTACH 'file:{}?cipher={CIPHER}&hexkey={KEY}' AS aux",
            env.path
        ))
        .unwrap();
    let rows: Vec<(String,)> = main_conn.exec_rows("SELECT v FROM aux.t");
    assert_eq!(rows, [(MARKER.to_string(),)]);
}

/// The old PRAGMA path applied a key inside an open transaction, which lost
/// the transaction, and under a running scan, which ended the scan early.
#[test]
fn encryption_pragmas_are_refused_in_a_transaction_or_under_a_running_statement() {
    let env = Env::new();
    let db = env.open();
    let conn = connect(&db, None).unwrap();
    conn.execute("BEGIN").unwrap();
    assert_refused(conn.execute(cipher_sql(CIPHER)), "a transaction is open");
    conn.execute("ROLLBACK").unwrap();

    let mut scan = conn
        .prepare("SELECT value FROM generate_series(1, 50)")
        .unwrap();
    let mut rows = 0;
    let step = |scan: &mut turso_core::Statement| loop {
        match scan.step().unwrap() {
            StepResult::Row => return true,
            StepResult::IO => scan._io().step().unwrap(),
            StepResult::Done => return false,
            result => panic!("unexpected step result: {result:?}"),
        }
    };
    assert!(step(&mut scan));
    rows += 1;
    // The scan holds a read transaction, so either refusal reason is right.
    let error = conn.execute(cipher_sql(CIPHER)).unwrap_err().to_string();
    assert!(error.contains(REFUSED), "{error}");
    assert!(
        error.contains("a transaction is open") || error.contains("another statement is running"),
        "{error}"
    );
    while step(&mut scan) {
        rows += 1;
    }
    assert_eq!(rows, 50);
    drop(scan);

    encrypt(&conn);
    fill(&conn);
    conn.close().unwrap();
    drop(conn);
    drop(db);
    assert_reopens_encrypted(&env, CIPHER, KEY);
}

/// The old PRAGMA path ignored the schema name and encrypted the main
/// database when the user meant an attached one.
#[test]
fn schema_qualified_encryption_pragmas_apply_only_to_main() {
    let env = Env::new();
    let aux_path = env.path_of("aux.db");
    {
        let db = Database::open(
            env.io.clone(),
            &env.path,
            env.options(DatabaseOpts::new().with_encryption(true).with_attach(true)),
        )
        .unwrap();
        let conn = connect(&db, None).unwrap();
        conn.execute(format!("ATTACH '{aux_path}' AS aux")).unwrap();
        assert_refused(conn.execute("PRAGMA aux.cipher = 'aegis256'"), "ATTACH URI");
        assert_refused(
            conn.execute(format!("PRAGMA aux.hexkey = '{KEY}'")),
            "ATTACH URI",
        );
        assert_refused(
            conn.execute(format!("PRAGMA temp.hexkey = '{KEY}'")),
            "ATTACH URI",
        );
        assert_eq!(hexkey_state(&conn), KEY_NOT_SET);
        conn.execute(format!("PRAGMA main.cipher = '{CIPHER}'"))
            .unwrap();
        conn.execute(format!("PRAGMA main.hexkey = '{KEY}'"))
            .unwrap();
        fill(&conn);
        conn.execute("CREATE TABLE aux.t(v TEXT)").unwrap();
        conn.execute("INSERT INTO aux.t VALUES ('aux plaintext')")
            .unwrap();
        conn.close().unwrap();
    }
    let aux = std::fs::read(&aux_path).unwrap();
    assert!(aux.starts_with(b"SQLite format 3\0"));
    assert_reopens_encrypted(&env, CIPHER, KEY);
}

/// The old PRAGMA path let `PRAGMA cipher` without `PRAGMA hexkey` write a
/// plaintext database that the user thought was encrypted.
#[test]
fn incomplete_encryption_setup_refuses_other_statements() {
    let env = Env::new();
    {
        let db = env.open();
        let conn = connect(&db, None).unwrap();
        let mut prepared_before = conn.prepare("CREATE TABLE early(v TEXT)").unwrap();
        conn.execute(cipher_sql(CIPHER)).unwrap();

        let error = conn.execute("CREATE TABLE t(v TEXT)").unwrap_err();
        assert!(
            error
                .to_string()
                .contains("encryption setup is incomplete: set PRAGMA hexkey"),
            "{error}"
        );
        assert!(conn.execute("SELECT 1").is_err());
        assert!(prepared_before.step().is_err());
        drop(prepared_before);
        // Read-back shows that the database is not encrypted yet.
        assert_eq!(hexkey_state(&conn), KEY_NOT_SET);
        assert!(active_cipher(&conn).is_empty());
        assert_eq!(env.size(), 0);

        assert!(conn.prepare("CREATE TABLE late(v TEXT)").is_err());

        conn.execute(key_sql(KEY)).unwrap();
        fill(&conn);
        conn.close().unwrap();
    }
    assert_reopens_encrypted(&env, CIPHER, KEY);

    // A statement prepared before the setup runs again after it completes.
    let env = Env::new();
    {
        let db = env.open();
        let conn = connect(&db, None).unwrap();
        let mut create = conn.prepare("CREATE TABLE t(v TEXT)").unwrap();
        encrypt(&conn);
        create.run_ignore_rows().unwrap();
        drop(create);
        conn.execute(format!("INSERT INTO t VALUES ('{MARKER}')"))
            .unwrap();
        conn.close().unwrap();
    }
    assert_reopens_encrypted(&env, CIPHER, KEY);
}

/// The old PRAGMA path kept a refused key, so `PRAGMA hexkey` said that a
/// key was set while the database stayed plaintext.
#[test]
fn refused_key_keeps_the_encryption_setup_incomplete() {
    let env = Env::new();
    {
        let db = env.open();
        let conn = connect(&db, None).unwrap();
        conn.execute(cipher_sql("aes128gcm")).unwrap();
        let error = conn.execute(key_sql(KEY)).unwrap_err().to_string();
        assert!(error.contains("Invalid key size"), "{error}");
        assert_eq!(hexkey_state(&conn), KEY_NOT_SET);
        assert!(active_cipher(&conn).is_empty());
        assert!(conn.execute("CREATE TABLE t(v TEXT)").is_err());
        assert_eq!(env.size(), 0);

        conn.execute(key_sql(KEY_128)).unwrap();
        fill(&conn);
        conn.close().unwrap();
    }
    assert_reopens_encrypted(&env, "aes128gcm", KEY_128);
}

/// The old PRAGMA path kept the reserved space that a statement read before
/// the key, and the B-tree then wrote cells into the space of the cipher.
#[test]
fn encryption_pragmas_after_a_read_use_the_cipher_reserved_space() {
    let env = Env::new();
    {
        let db = env.open();
        let conn = connect(&db, None).unwrap();
        let tables: Vec<(String,)> = conn.exec_rows("SELECT name FROM sqlite_schema");
        assert!(tables.is_empty());
        encrypt(&conn);
        fill(&conn);
        for i in 0..300 {
            conn.execute(format!("INSERT INTO t VALUES ('{}')", "x".repeat(200 + i)))
                .unwrap();
        }
        let check: Vec<(String,)> = conn.exec_rows("PRAGMA integrity_check");
        assert_eq!(check, [("ok".to_string(),)]);
        conn.close().unwrap();
    }
    assert_eq!(env.header()[20], 48);
    let db = env.open_keyed(CIPHER, KEY).unwrap();
    let conn = connect(&db, Some(KEY)).unwrap();
    let count: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM t");
    assert_eq!(count, [(301,)]);
    let check: Vec<(String,)> = conn.exec_rows("PRAGMA integrity_check");
    assert_eq!(check, [("ok".to_string(),)]);
}

#[test]
fn encryption_pragmas_need_the_encryption_feature_and_no_mvcc() {
    let env = Env::new();
    let db = Database::open(env.io.clone(), &env.path, env.options(DatabaseOpts::new())).unwrap();
    let conn = connect(&db, None).unwrap();
    let error = conn.execute(cipher_sql(CIPHER)).unwrap_err().to_string();
    assert!(error.contains("opt in feature"), "{error}");

    let env = Env::new();
    let db = Database::open(
        env.io.clone(),
        &env.path,
        env.options(DatabaseOpts::new().with_encryption(true).with_mvcc(true)),
    )
    .unwrap();
    let conn = connect(&db, None).unwrap();
    assert_refused(conn.execute(cipher_sql(CIPHER)), "MVCC is enabled");
    assert_refused(conn.execute(key_sql(KEY)), "MVCC is enabled");
}

#[test]
fn invalid_encryption_pragma_values_are_refused() {
    let env = Env::new();
    let db = env.open();
    let conn = connect(&db, None).unwrap();
    let error = conn.execute(cipher_sql("rot13")).unwrap_err().to_string();
    assert!(error.contains("Unknown cipher name"), "{error}");
    let error = conn.execute(key_sql("not hex")).unwrap_err().to_string();
    assert!(error.contains("Invalid hex string"), "{error}");
    let error = conn.execute(key_sql("abcd")).unwrap_err().to_string();
    assert!(error.contains("16 or 32 bytes"), "{error}");
    // A key in the wrong quotes is refused without showing it.
    let error = conn
        .execute(format!("PRAGMA hexkey = \"{KEY}\""))
        .unwrap_err()
        .to_string();
    assert!(error.contains("single quotes"), "{error}");
    assert!(!error.contains(KEY), "the error showed the key: {error}");
    // None of them started a setup.
    fill(&conn);
}

/// The value applies when the statement runs. `EXPLAIN` and a statement that
/// is never run change nothing, and `EXPLAIN` does not show the key.
#[test]
fn explained_or_unexecuted_encryption_pragmas_change_nothing() {
    let env = Env::new();
    {
        let db = env.open();
        let conn = connect(&db, None).unwrap();
        let mut explain = conn
            .prepare(format!("EXPLAIN PRAGMA hexkey = '{KEY}'"))
            .unwrap();
        let mut text = String::new();
        loop {
            match explain.step().unwrap() {
                StepResult::Row => {
                    for value in explain.row().unwrap().get_values() {
                        text.push_str(&value.to_string());
                        text.push(' ');
                    }
                }
                StepResult::IO => explain._io().step().unwrap(),
                StepResult::Done => break,
                result => panic!("unexpected step result: {result:?}"),
            }
        }
        drop(explain);
        assert!(text.contains("SetEncryption"), "{text}");
        assert!(!text.contains(KEY), "EXPLAIN showed the key: {text}");
        drop(conn.prepare(cipher_sql(CIPHER)).unwrap());
        assert_eq!(hexkey_state(&conn), KEY_NOT_SET);
        fill(&conn);
        conn.close().unwrap();
    }
    assert!(env.header().starts_with(b"SQLite format 3\0"));
}

/// In-place VACUUM builds its temporary database with the key of the
/// connection, which the PRAGMAs set.
#[test]
fn vacuum_keeps_the_encryption_that_the_pragmas_set() {
    let env = Env::new();
    {
        let db = Database::open(
            env.io.clone(),
            &env.path,
            env.options(DatabaseOpts::new().with_encryption(true).with_vacuum(true)),
        )
        .unwrap();
        let conn = connect(&db, None).unwrap();
        encrypt(&conn);
        fill(&conn);
        conn.execute("VACUUM").unwrap();
        assert_eq!(markers(&conn), [(MARKER.to_string(),)]);
        conn.close().unwrap();
    }
    assert_reopens_encrypted(&env, CIPHER, KEY);
}

/// Each PRAGMA makes the statements prepared before it prepare again when
/// they run, so the two PRAGMAs also work when both are prepared first.
#[test]
fn encryption_pragmas_prepared_before_they_run() {
    let env = Env::new();
    {
        let db = env.open();
        let conn = connect(&db, None).unwrap();
        let mut cipher = conn.prepare(cipher_sql(CIPHER)).unwrap();
        let mut key = conn.prepare(key_sql(KEY)).unwrap();
        cipher.run_ignore_rows().unwrap();
        key.run_ignore_rows().unwrap();
        drop((cipher, key));
        assert_eq!(hexkey_state(&conn), KEY_SET);
        fill(&conn);
        conn.close().unwrap();
    }
    assert_reopens_encrypted(&env, CIPHER, KEY);
}

/// Raw WAL writes do not run SQL, so they check for an incomplete setup too.
#[test]
fn incomplete_encryption_setup_refuses_raw_wal_writes() {
    let env = Env::new();
    {
        let db = env.open();
        let conn = connect(&db, None).unwrap();
        conn.execute(cipher_sql(CIPHER)).unwrap();
        let error = conn.wal_insert_begin().unwrap_err().to_string();
        assert!(error.contains("encryption setup is incomplete"), "{error}");
        assert_eq!(env.size(), 0);
        conn.execute(key_sql(KEY)).unwrap();
        fill(&conn);
        conn.close().unwrap();
    }
    assert_reopens_encrypted(&env, CIPHER, KEY);
}

/// Another connection can write the database while one value waits. Then
/// the setup can never finish, and the error says so.
#[test]
fn incomplete_encryption_setup_that_cannot_finish_says_so() {
    let env = Env::new();
    let db = env.open();
    let owner = connect(&db, None).unwrap();
    owner.execute(cipher_sql(CIPHER)).unwrap();
    let peer = connect(&db, None).unwrap();
    peer.execute("CREATE TABLE t(v TEXT)").unwrap();
    peer.close().unwrap();
    drop(peer);
    assert_refused(owner.execute(key_sql(KEY)), "not empty");
    let error = owner.execute("SELECT 1").unwrap_err().to_string();
    assert!(error.contains("cannot finish"), "{error}");
    assert!(env.header().starts_with(b"SQLite format 3\0"));
}

/// Other processes can use the same new file with the multiprocess WAL.
#[test]
fn encryption_pragmas_are_refused_with_multiprocess_wal() {
    let env = Env::new();
    let db = Database::open(
        env.io.clone(),
        &env.path,
        env.options(
            DatabaseOpts::new()
                .with_encryption(true)
                .with_multiprocess_wal(true),
        ),
    )
    .unwrap();
    let conn = connect(&db, None).unwrap();
    assert_refused(conn.execute(cipher_sql(CIPHER)), "multiprocess WAL");
    assert_refused(conn.execute(key_sql(KEY)), "multiprocess WAL");
}

/// The PRAGMAs write nothing when they run, but they decide how each later
/// write is encrypted, also the writes of other connections. So a connection
/// that must not write refuses them.
#[test]
fn encryption_pragmas_are_refused_on_a_connection_that_must_not_write() {
    let env = Env::new();
    {
        let db = env.open();
        let conn = connect(&db, None).unwrap();
        conn.execute("PRAGMA query_only = 1").unwrap();
        assert_refused(conn.execute(cipher_sql(CIPHER)), "PRAGMA query_only is on");
        assert_refused(conn.execute(key_sql(KEY)), "PRAGMA query_only is on");
        assert_eq!(hexkey_state(&conn), KEY_NOT_SET);
        conn.execute("PRAGMA query_only = 0").unwrap();
        encrypt(&conn);
        fill(&conn);
        conn.close().unwrap();
    }
    assert_reopens_encrypted(&env, CIPHER, KEY);

    let env = Env::new();
    std::fs::File::create(&env.path).unwrap();
    let db = Database::open(
        env.io.clone(),
        &env.path,
        env.options(DatabaseOpts::new().with_encryption(true))
            .flags(OpenFlags::ReadOnly),
    )
    .unwrap();
    let conn = connect(&db, None).unwrap();
    assert_refused(
        conn.execute(cipher_sql(CIPHER)),
        "the database is read-only",
    );
    assert_refused(conn.execute(key_sql(KEY)), "the database is read-only");
    assert_eq!(hexkey_state(&conn), KEY_NOT_SET);
}

/// The file format requires at least 480 usable bytes in each page after
/// the bytes that the cipher reserves. On 512-byte pages, the AEGIS-256
/// ciphers reserve 48 bytes and leave only 464.
#[test]
fn a_cipher_that_leaves_less_than_480_usable_bytes_is_refused() {
    let env = Env::new();
    {
        let db = env.open();
        let conn = connect(&db, None).unwrap();
        conn.execute("PRAGMA page_size = 512").unwrap();
        for cipher in ["aegis256", "aegis256x2", "aegis256x4"] {
            let error = conn.execute(cipher_sql(cipher)).unwrap_err().to_string();
            assert!(error.contains("less than 480 usable bytes"), "{error}");
        }
        // A refused cipher does not wait for a key, so other statements run.
        assert_eq!(hexkey_state(&conn), KEY_NOT_SET);
        conn.execute(key_sql(KEY_128)).unwrap();
        // The cipher is refused when the key waits for it, too.
        let error = conn
            .execute(cipher_sql("aegis256"))
            .unwrap_err()
            .to_string();
        assert!(error.contains("less than 480 usable bytes"), "{error}");
        // AEGIS-128L reserves 32 bytes and leaves exactly 480.
        conn.execute(cipher_sql("aegis128l")).unwrap();
        fill(&conn);
        conn.close().unwrap();
    }
    let header = env.header();
    assert_eq!(u16::from_be_bytes([header[16], header[17]]), 512);
    assert_eq!(header[20], 32);
    assert_reopens_encrypted(&env, "aegis128l", KEY_128);
}
