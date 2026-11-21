use std::sync::Arc;

use genawaiter::GeneratorState;
use tempfile::TempDir;
use turso_core::{Connection, Database, DatabaseOpts, OpenOptions, PlatformIO, IO};
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
        let db_path = dir.path().join("replica.db").to_string_lossy().into_owned();
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new()?);
        let server = SyncEngineIoStats::new(Arc::new(server));
        let engine = drive(&io, async |coro| {
            DatabaseSyncEngine::create_db(coro, io.clone(), server, &db_path, replica_opts(db_opts))
                .await
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

    /// Pulls, and calls `on_yield` each time the pull returns control to its caller, as the SDK
    /// does when the pull waits for IO or for a lock. Returns how many times that happened.
    pub fn pull_calling_on_yield(&self, mut on_yield: impl FnMut()) -> (usize, Result<()>) {
        let mut generator = genawaiter::sync::Gen::new(|co| async move {
            let coro: Coro<()> = co.into();
            self.engine.pull_changes_from_remote(&coro).await
        });
        let mut yields = 0;
        loop {
            match generator.resume_with(Ok(())) {
                GeneratorState::Yielded(_) => {
                    yields += 1;
                    on_yield();
                    if let Err(error) = self.io.step() {
                        return (yields, Err(error.into()));
                    }
                }
                GeneratorState::Complete(result) => return (yields, result),
            }
        }
    }

    pub fn snapshot(&self) -> Result<Snapshot> {
        Snapshot::capture(&self.connect()?)
    }

    pub fn connect(&self) -> Result<Arc<Connection>> {
        drive(&self.io, async |coro| self.engine.connect_rw(coro).await)
    }

    /// How many local changes are waiting to be pushed.
    pub fn pending_local_changes(&self) -> Result<i64> {
        drive(&self.io, async |coro| self.engine.stats(coro).await).map(|s| s.cdc_operations)
    }

    pub fn checkpoint(&self) -> Result<()> {
        drive(&self.io, async |coro| self.engine.checkpoint(coro).await)
    }
}

fn replica_opts(db_opts: DatabaseOpts) -> DatabaseSyncEngineOpts {
    DatabaseSyncEngineOpts {
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
        pull_parallel_requests: None,
        logical_mvcc_pull: None,
        busy_timeout: turso_sync_engine::database_sync_engine::DEFAULT_SYNC_BUSY_TIMEOUT,
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
