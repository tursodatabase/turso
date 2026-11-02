use std::path::Path;
use std::sync::Arc;

use genawaiter::GeneratorState;
use tempfile::TempDir;
use turso_core::{Database, DatabaseOpts, OpenOptions, PlatformIO, SqliteDialect, Value, IO};
use turso_sync_engine::database_sync_engine::{DatabaseSyncEngine, DatabaseSyncEngineOpts};
use turso_sync_engine::database_sync_operations::SyncEngineIoStats;
use turso_sync_engine::types::{Coro, DatabaseSyncEngineProtocolVersion};
use turso_sync_engine::Result;

use super::remote::{database_opts, Remote};
use super::server::InProcessServer;
use super::snapshot::Snapshot;

pub struct Replica {
    remote: Arc<Remote>,
    server: Arc<InProcessServer>,
    db_opts: DatabaseOpts,
    io: Arc<dyn IO>,
    engine: Option<DatabaseSyncEngine<InProcessServer>>,
    dir: Arc<TempDir>,
}

impl Replica {
    pub(super) fn bootstrap(remote: Arc<Remote>) -> Result<Self> {
        Self::open(
            &Arc::new(tempfile::tempdir()?),
            InProcessServer::new(remote),
            database_opts(),
        )
    }

    pub fn clone_from_backup(remote: Arc<Remote>, backup_dir: &Path) -> Result<Self> {
        let dir = Arc::new(tempfile::tempdir()?);
        copy_matching_files(backup_dir, dir.path(), |_| true)?;
        Self::open(&dir, InProcessServer::new(remote), database_opts())
    }

    pub fn open(
        dir: &Arc<TempDir>,
        server: InProcessServer,
        db_opts: DatabaseOpts,
    ) -> Result<Self> {
        let db_path = dir.path().join("replica.db").to_string_lossy().into_owned();
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new()?);
        let remote = server.remote().clone();
        let server = Arc::new(server);
        let stats = SyncEngineIoStats::new(server.clone());
        let engine = drive(&io, async |coro| {
            DatabaseSyncEngine::create_db(coro, io.clone(), stats, &db_path, replica_opts(db_opts))
                .await
        })?;
        Ok(Self {
            remote,
            server,
            db_opts,
            io,
            engine: Some(engine),
            dir: dir.clone(),
        })
    }

    pub fn open_existing(
        dir: &Arc<TempDir>,
        server: InProcessServer,
        db_opts: DatabaseOpts,
    ) -> Result<Self> {
        let db_path = dir.path().join("replica.db").to_string_lossy().into_owned();
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new()?);
        let remote = server.remote().clone();
        let server = Arc::new(server);
        let stats = SyncEngineIoStats::new(server.clone());
        let main_db = Database::open(
            io.clone(),
            &db_path,
            OpenOptions::new(Arc::new(SqliteDialect)).db_opts(db_opts),
        )?;
        let engine = drive(&io, async |coro| {
            DatabaseSyncEngine::open_db(coro, io.clone(), stats, main_db, replica_opts(db_opts))
                .await
        })?;
        Ok(Self {
            remote,
            server,
            db_opts,
            io,
            engine: Some(engine),
            dir: dir.clone(),
        })
    }

    fn engine(&self) -> &DatabaseSyncEngine<InProcessServer> {
        self.engine.as_ref().expect("replica engine must be open")
    }

    pub fn execute(&self, sql: &str) -> Result<()> {
        let conn = drive(&self.io, async |coro| self.engine().connect_rw(coro).await)?;
        conn.execute(sql)?;
        Ok(())
    }

    pub fn query_rows(&self, sql: &str) -> Result<Vec<Vec<Value>>> {
        let conn = drive(&self.io, async |coro| self.engine().connect_rw(coro).await)?;
        Ok(conn.prepare(sql)?.run_collect_rows()?)
    }

    pub fn push(&self) -> Result<()> {
        drive(&self.io, async |coro| {
            self.engine().push_changes_to_remote(coro).await
        })
    }

    pub fn pull(&self) -> Result<()> {
        drive(&self.io, async |coro| {
            self.engine().pull_changes_from_remote(coro).await
        })
    }

    pub fn fail_next_push_after_remote_commit(&self) {
        self.server.fail_next_push_after_remote_commit();
    }

    pub fn reopen(&mut self) -> Result<()> {
        self.engine = None;
        let db_path = self
            .dir
            .path()
            .join("replica.db")
            .to_string_lossy()
            .into_owned();
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new()?);
        let server = Arc::new(InProcessServer::new(self.remote.clone()));
        let stats = SyncEngineIoStats::new(server.clone());
        let db_opts = self.db_opts;
        let engine = drive(&io, async |coro| {
            DatabaseSyncEngine::create_db(coro, io.clone(), stats, &db_path, replica_opts(db_opts))
                .await
        })?;
        self.io = io;
        self.server = server;
        self.engine = Some(engine);
        Ok(())
    }

    pub fn backup_all_files(&self) -> Result<TempDir> {
        let backup = tempfile::tempdir()?;
        copy_matching_files(self.dir.path(), backup.path(), |name| {
            name.starts_with("replica.db")
        })?;
        Ok(backup)
    }

    pub fn restore_all_files(&mut self, backup_dir: &Path) -> Result<()> {
        self.engine = None;
        remove_matching_files(self.dir.path(), |name| name.starts_with("replica.db"))?;
        copy_matching_files(backup_dir, self.dir.path(), |name| {
            name.starts_with("replica.db")
        })?;
        self.reopen()
    }

    pub fn restore_db_files_only(&mut self, backup_dir: &Path) -> Result<()> {
        self.engine = None;
        let is_db_data_file =
            |name: &str| name.starts_with("replica.db") && !name.ends_with("-info");
        remove_matching_files(self.dir.path(), is_db_data_file)?;
        copy_matching_files(backup_dir, self.dir.path(), is_db_data_file)?;
        self.reopen()
    }

    pub fn snapshot(&self) -> Result<Snapshot> {
        let conn = drive(&self.io, async |coro| self.engine().connect_rw(coro).await)?;
        Snapshot::capture(&conn)
    }
}

fn copy_matching_files(src: &Path, dst: &Path, keep: impl Fn(&str) -> bool) -> Result<()> {
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let file_name = entry.file_name();
        let Some(name) = file_name.to_str() else {
            continue;
        };
        if entry.file_type()?.is_file() && keep(name) {
            std::fs::copy(entry.path(), dst.join(&file_name))?;
        }
    }
    Ok(())
}

fn remove_matching_files(dir: &Path, remove: impl Fn(&str) -> bool) -> Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let file_name = entry.file_name();
        let Some(name) = file_name.to_str() else {
            continue;
        };
        if entry.file_type()?.is_file() && remove(name) {
            std::fs::remove_file(entry.path())?;
        }
    }
    Ok(())
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
