use std::sync::Arc;
use turso_core::alloc::DynAllocator;
use turso_core::io::PlatformIO;
use turso_core::mvcc::database::MvStore;
use turso_core::mvcc::MvccClock;
use turso_core::{
    ConnectOpts, Connection, Database, DatabaseOpts, EncryptionKey, EncryptionOpts, OpenFlags,
    SessionRole,
};
use turso_sqlite::SqliteDialect;

/// A file-backed MVCC database that a test can close and open again.
pub struct MvccTestDbNoConn {
    db: Option<Arc<Database>>,
    path: String,
    opts: DatabaseOpts,
    enc_opts: Option<EncryptionOpts>,
    _temp_dir: tempfile::TempDir,
}

impl MvccTestDbNoConn {
    pub fn new_with_random_db() -> Self {
        Self::new_with_random_db_with_opts(DatabaseOpts::new())
    }

    /// Opens a database whose automatic checkpoints do not block writers.
    pub fn new_with_random_db_passive() -> Self {
        Self::new_with_random_db_with_opts(
            DatabaseOpts::new().with_experimental_mvcc_passive_checkpoint(true),
        )
    }

    pub fn new_with_random_db_with_opts(opts: DatabaseOpts) -> Self {
        Self::create(opts, None)
    }

    /// Opens a database that is encrypted with a randomly picked cipher when `encrypted` is true.
    pub fn new_maybe_encrypted(encrypted: bool) -> Self {
        if !encrypted {
            return Self::new_with_random_db();
        }
        const KEY128: &str = "b1bbfda4f589dc9daaf004fe21111e00";
        const KEY256: &str = "b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327";
        let ciphers: &[(&str, &str)] = &[
            ("aes128gcm", KEY128),
            ("aes256gcm", KEY256),
            ("aegis128l", KEY128),
            ("aegis128x2", KEY128),
            ("aegis128x4", KEY128),
            ("aegis256", KEY256),
            ("aegis256x2", KEY256),
            ("aegis256x4", KEY256),
        ];
        let (cipher, hexkey) = ciphers[rand::random_range(0..ciphers.len())];
        Self::new_encrypted_with_cipher(hexkey, cipher)
    }

    fn new_encrypted_with_cipher(hex_key: &str, cipher: &str) -> Self {
        Self::create(
            DatabaseOpts::new().with_encryption(true),
            Some(EncryptionOpts {
                cipher: cipher.to_string(),
                hexkey: hex_key.to_string(),
            }),
        )
    }

    fn create(opts: DatabaseOpts, enc_opts: Option<EncryptionOpts>) -> Self {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let path = temp_dir
            .path()
            .join(format!("test_{}", rand::random::<u64>()))
            .to_str()
            .unwrap()
            .to_string();
        let mut db = Self {
            db: None,
            path,
            opts,
            enc_opts,
            _temp_dir: temp_dir,
        };
        db.open().unwrap();
        let conn = db.connect();
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        conn.close().unwrap();
        db
    }

    /// Closes the database and opens it again, which recovers it from its files.
    /// The test must have dropped every connection first.
    pub fn restart(&mut self) {
        self.restart_result().unwrap();
    }

    /// Like `restart`, but returns the error of the open instead of panicking.
    pub fn restart_result(&mut self) -> turso_core::Result<()> {
        self.close();
        self.open()
    }

    fn open(&mut self) -> turso_core::Result<()> {
        let io = Arc::new(PlatformIO::new().unwrap());
        let db = Database::open_file_with_flags(
            io,
            &self.path,
            OpenFlags::default(),
            self.opts,
            self.enc_opts.clone(),
            Arc::new(SqliteDialect),
        )?;
        self.db = Some(db);
        Ok(())
    }

    /// Closes the database without opening it again, so that a test can change its files.
    /// The test must have dropped every connection first.
    pub fn close(&mut self) {
        if let Some(db) = self.db.take() {
            assert_eq!(
                Arc::strong_count(&db),
                1,
                "a connection is still open, so the database would not be closed"
            );
        }
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn get_mvcc_store(&self) -> Arc<MvStore<MvccClock, DynAllocator>> {
        self.get_db().get_mv_store().clone().unwrap()
    }

    pub fn get_db(&self) -> Arc<Database> {
        self.db.as_ref().unwrap().clone()
    }

    pub fn connect(&self) -> Arc<Connection> {
        let enc_key = self
            .enc_opts
            .as_ref()
            .map(|e| EncryptionKey::from_hex_string(&e.hexkey).unwrap());
        self.get_db()
            .connect(ConnectOpts::new(SessionRole::Superuser).with_encryption_key(enc_key))
            .unwrap()
    }
}
