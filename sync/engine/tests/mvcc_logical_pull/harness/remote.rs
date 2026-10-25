use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use tempfile::TempDir;
use turso_core::{
    ConnectOpts, Connection, Database, DatabaseOpts, OpenOptions, PlatformIO, SqliteDialect,
};
use turso_sync_engine::Result;

use super::replica::Replica;
use super::snapshot::Snapshot;

pub struct Remote {
    conn: Arc<Connection>,
    db_path: PathBuf,
    replace_base_snapshot: Mutex<Option<(Vec<u8>, usize)>>,
    _db: Arc<Database>,
    _dir: TempDir,
}

impl Remote {
    pub fn new(setup: &[&str]) -> Result<Arc<Self>> {
        let dir = tempfile::tempdir()?;
        let db_path = dir.path().join("remote.db");
        let db = Database::open(
            Arc::new(PlatformIO::new()?),
            &db_path.to_string_lossy(),
            OpenOptions::new(Arc::new(SqliteDialect)).db_opts(database_opts()),
        )?;
        let conn = db.connect(ConnectOpts::new())?;
        conn.execute("PRAGMA journal_mode = 'mvcc'")?;
        conn.set_portable_logical_changes_enabled(true);
        for sql in setup {
            conn.execute(sql)?;
        }
        Ok(Arc::new(Self {
            conn,
            db_path,
            replace_base_snapshot: Mutex::new(None),
            _db: db,
            _dir: dir,
        }))
    }

    pub fn bootstrap_replica(self: &Arc<Self>) -> Result<Replica> {
        Replica::bootstrap(self.clone())
    }

    pub fn execute_transaction(&self, statements: &[&str]) -> Result<()> {
        self.conn.execute("BEGIN")?;
        for sql in statements {
            self.conn.execute(sql)?;
        }
        self.conn.execute("COMMIT")?;
        Ok(())
    }

    pub fn answer_next_pull_with_replace_base_snapshot(&self) -> Result<()> {
        let database = self.checkpoint_and_read_database_file()?;
        let log_end = self.read_logical_log()?.len();
        *self.replace_base_snapshot.lock().unwrap() = Some((database, log_end));
        Ok(())
    }

    pub fn snapshot(&self) -> Result<Snapshot> {
        Snapshot::capture(&self.conn)
    }

    pub(super) fn checkpoint_and_read_database_file(&self) -> Result<Vec<u8>> {
        self.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
        Ok(std::fs::read(&self.db_path)?)
    }

    pub(super) fn take_replace_base_snapshot(&self) -> Option<(Vec<u8>, usize)> {
        self.replace_base_snapshot.lock().unwrap().take()
    }

    pub(super) fn read_logical_log(&self) -> Result<Vec<u8>> {
        match std::fs::read(self.db_path.with_extension("db-log")) {
            Ok(log) => Ok(log),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
            Err(error) => Err(error.into()),
        }
    }
}

pub fn database_opts() -> DatabaseOpts {
    DatabaseOpts::new().with_generated_columns(true)
}
