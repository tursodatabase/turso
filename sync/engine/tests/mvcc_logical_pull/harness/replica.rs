use std::sync::Arc;

use genawaiter::GeneratorState;
use tempfile::TempDir;
use turso_core::{PlatformIO, IO};
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
    _dir: TempDir,
}

impl Replica {
    pub(super) fn bootstrap(remote: Arc<Remote>) -> Result<Self> {
        let dir = tempfile::tempdir()?;
        let db_path = dir.path().join("replica.db").to_string_lossy().into_owned();
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new()?);
        let server = SyncEngineIoStats::new(Arc::new(InProcessServer::new(remote)));
        let engine = drive(&io, async |coro| {
            DatabaseSyncEngine::create_db(coro, io.clone(), server, &db_path, replica_opts()).await
        })?;
        Ok(Self {
            io,
            engine,
            _dir: dir,
        })
    }

    pub fn pull(&self) -> Result<()> {
        drive(&self.io, async |coro| {
            self.engine.pull_changes_from_remote(coro).await
        })
    }

    pub fn snapshot(&self) -> Result<Snapshot> {
        let conn = drive(&self.io, async |coro| self.engine.connect_rw(coro).await)?;
        Snapshot::capture(&conn)
    }
}

fn replica_opts() -> DatabaseSyncEngineOpts {
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
        db_opts: database_opts(),
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
