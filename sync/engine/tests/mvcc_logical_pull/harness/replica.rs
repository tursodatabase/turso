use std::sync::Arc;

use genawaiter::GeneratorState;
use tempfile::TempDir;
use turso_core::{Database, DatabaseOpts, OpenOptions, PlatformIO, IO};
use turso_sqlite::SqliteDialect;
use turso_sync_engine::database_sync_engine::{DatabaseSyncEngine, DatabaseSyncEngineOpts};
use turso_sync_engine::database_sync_operations::SyncEngineIoStats;
use turso_sync_engine::types::{Coro, DatabaseSyncEngineProtocolVersion};
use turso_sync_engine::Result;

use super::remote::{database_opts, Remote};
use super::server::InProcessServer;
use super::snapshot::Snapshot;

pub struct Replica {
    io: Arc<dyn IO>,
    engine: DatabaseSyncEngine<InProcessServer>,
    _dir: Arc<TempDir>,
}

impl Replica {
    pub(super) fn bootstrap(remote: Arc<Remote>) -> Result<Self> {
        Self::open(
            &Arc::new(tempfile::tempdir()?),
            InProcessServer::new(remote),
            database_opts(),
        )
    }

    pub fn open(
        dir: &Arc<TempDir>,
        server: InProcessServer,
        db_opts: DatabaseOpts,
    ) -> Result<Self> {
        Self::open_with_options(dir, server, replica_opts(db_opts))
    }

    pub fn open_encrypted(
        dir: &Arc<TempDir>,
        server: InProcessServer,
        local_key: Option<[u8; 32]>,
    ) -> Result<Self> {
        Self::open_encrypted_with_io(dir, server, local_key, Arc::new(PlatformIO::new()?))
    }

    pub fn open_encrypted_with_io(
        dir: &Arc<TempDir>,
        server: InProcessServer,
        local_key: Option<[u8; 32]>,
        io: Arc<dyn IO>,
    ) -> Result<Self> {
        let db_path = dir.path().join("replica.db").to_string_lossy().into_owned();
        Self::open_encrypted_at_path(dir, server, local_key, io, &db_path)
    }

    pub fn open_encrypted_at_path(
        dir: &Arc<TempDir>,
        server: InProcessServer,
        local_key: Option<[u8; 32]>,
        io: Arc<dyn IO>,
        db_path: &str,
    ) -> Result<Self> {
        let mut opts = replica_opts(database_opts().with_encryption(local_key.is_some()));
        opts.encryption = Some(turso_sync_engine::sync_encryption::SyncEncryption::new(
            turso_core::CipherMode::Aes256Gcm,
            local_key.map(turso_core::EncryptionKey::new_256),
        )?);
        opts.remote_encryption_key = Some(turso_sync_engine::types::Secret::new("cloud-test-key"));
        Self::open_with_io_at_path(dir, server, opts, io, db_path)
    }

    fn open_with_options(
        dir: &Arc<TempDir>,
        server: InProcessServer,
        opts: DatabaseSyncEngineOpts,
    ) -> Result<Self> {
        Self::open_with_io(dir, server, opts, Arc::new(PlatformIO::new()?))
    }

    fn open_with_io(
        dir: &Arc<TempDir>,
        server: InProcessServer,
        opts: DatabaseSyncEngineOpts,
        io: Arc<dyn IO>,
    ) -> Result<Self> {
        let db_path = dir.path().join("replica.db").to_string_lossy().into_owned();
        Self::open_with_io_at_path(dir, server, opts, io, &db_path)
    }

    fn open_with_io_at_path(
        dir: &Arc<TempDir>,
        server: InProcessServer,
        opts: DatabaseSyncEngineOpts,
        io: Arc<dyn IO>,
        db_path: &str,
    ) -> Result<Self> {
        let server = SyncEngineIoStats::new(Arc::new(server));
        let engine = drive(&io, async |coro| {
            DatabaseSyncEngine::create_db(coro, io.clone(), server, db_path, opts).await
        })?;
        Ok(Self {
            io,
            engine,
            _dir: dir.clone(),
        })
    }

    pub fn open_existing(
        dir: &Arc<TempDir>,
        server: InProcessServer,
        db_opts: DatabaseOpts,
    ) -> Result<Self> {
        let db_path = dir.path().join("replica.db").to_string_lossy().into_owned();
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new()?);
        let server = SyncEngineIoStats::new(Arc::new(server));
        let main_db = Database::open(
            io.clone(),
            &db_path,
            OpenOptions::new(Arc::new(SqliteDialect)).db_opts(db_opts),
        )?;
        let engine = drive(&io, async |coro| {
            DatabaseSyncEngine::open_db(coro, io.clone(), server, main_db, replica_opts(db_opts))
                .await
        })?;
        Ok(Self {
            io,
            engine,
            _dir: dir.clone(),
        })
    }

    pub fn pull(&self) -> Result<()> {
        drive(&self.io, async |coro| {
            self.engine.pull_changes_from_remote(coro).await
        })
    }

    pub fn is_mvcc(&self) -> bool {
        drive(&self.io, async |coro| self.engine.connect_rw(coro).await)
            .unwrap()
            .mv_store()
            .is_some()
    }

    pub fn execute(&self, sql: &str) -> Result<()> {
        let conn = drive(&self.io, async |coro| self.engine.connect_rw(coro).await)?;
        conn.execute(sql)?;
        Ok(())
    }

    pub fn pull_with_tampered_staging(&self) -> Result<()> {
        self.pull_with_tampered_staging_at(Some(40))
    }

    // None corrupts the final nonce byte, after all preceding records/chunks.
    pub fn pull_with_tampered_staging_at(&self, offset: Option<usize>) -> Result<()> {
        drive(&self.io, async |coro| {
            let changes = self.engine.wait_changes_from_remote(coro).await?;
            let path = self._dir.path().join("replica.db-changes");
            let mut bytes = std::fs::read(&path)?;
            let offset = offset.unwrap_or(bytes.len() - 1);
            bytes[offset] ^= 1;
            std::fs::write(path, bytes)?;
            self.engine.apply_changes_from_remote(coro, changes).await
        })
    }

    pub fn snapshot(&self) -> Result<Snapshot> {
        let conn = drive(&self.io, async |coro| self.engine.connect_rw(coro).await)?;
        Snapshot::capture(&conn)
    }
}

fn replica_opts(db_opts: DatabaseOpts) -> DatabaseSyncEngineOpts {
    DatabaseSyncEngineOpts {
        encryption: None,
        remote_url: Some("http://remote".to_string()),
        client_name: "replica".to_string(),
        tables_ignore: vec![],
        use_transform: false,
        wal_pull_batch_size: 0,
        long_poll_timeout: None,
        protocol_version_hint: DatabaseSyncEngineProtocolVersion::V1,
        bootstrap_if_empty: true,
        reserved_bytes: 0,
        db_opts,
        partial_sync_opts: None,
        remote_encryption_key: None,
        push_operations_threshold: None,
        pull_bytes_threshold: None,
        logical_mvcc_pull: None,
    }
}

fn drive<T>(io: &Arc<dyn IO>, work: impl AsyncFnOnce(&Coro<()>) -> Result<T>) -> Result<T> {
    let mut generator = genawaiter::sync::Gen::new(|co| async move {
        let coro: Coro<()> = co.into();
        work(&coro).await
    });
    loop {
        match generator.resume_with(Ok(())) {
            GeneratorState::Yielded(_) => io.step()?,
            GeneratorState::Complete(result) => return result,
        }
    }
}
