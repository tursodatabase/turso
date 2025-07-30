use std::{
    future::Future,
    io::ErrorKind,
    path::{Path, PathBuf},
    sync::Arc,
};

use hyper_rustls::{ConfigBuilderExt, HttpsConnector, HttpsConnectorBuilder};
use hyper_util::{client::legacy::connect::HttpConnector, rt::TokioExecutor};
use turso_sync_protocol::{
    database_inner::DatabaseInner,
    errors::Error,
    types::{DbSyncInfo, ProtocolResume, ProtocolYield},
    Result,
};

use crate::{
    filesystem::{tokio::TokioFilesystem, Filesystem},
    sync_server::{
        turso::{TursoSyncServer, TursoSyncServerOpts},
        Stream, SyncServer,
    },
};

/// [Database] expose public interface for synced database from [DatabaseInner] private implementation
///
/// This layer also serves a purpose of "gluing" together all component for real use,
/// because [DatabaseInner] abstracts things away in order to simplify testing
pub struct Database {
    inner: DatabaseInner,
    io: Arc<dyn turso_core::IO>,
    filesystem: TokioFilesystem,
    sync_server: TursoSyncServer,
}

pub struct Builder {
    path: String,
    sync_url: String,
    auth_token: Option<String>,
    encryption_key: Option<String>,
    connector: Option<HttpsConnector<HttpConnector>>,
}

async fn bootstrap(
    fs: &impl Filesystem,
    sync_server: &impl SyncServer,
    synced_path: &Path,
    draft_path: &Path,
) -> Result<DbSyncInfo> {
    let info = sync_server.db_info().await?;
    let mut synced_file = fs.create_file(synced_path).await?;

    let start_time = std::time::Instant::now();
    let mut written_bytes = 0;
    tracing::debug!("start bootstrapping Synced file from remote");

    let mut bootstrap = sync_server.db_export(info.current_generation).await?;
    while let Some(chunk) = bootstrap.read_chunk().await? {
        fs.write_file(&mut synced_file, &chunk).await?;
        written_bytes += chunk.len();
    }

    let elapsed = std::time::Instant::now().duration_since(start_time);
    tracing::debug!(
        "finish bootstrapping Synced file from remote: written_bytes={}, elapsed={:?}",
        written_bytes,
        elapsed
    );
    fs.copy_file(&synced_path, &draft_path).await?;
    Ok(info)
}

async fn write(fs: &impl Filesystem, path: &Path, data: &[u8]) -> Result<()> {
    tracing::debug!("write to {:?}", path);
    let directory = path.parent().ok_or_else(|| {
        Error::MetadataError(format!(
            "unable to get parent of the provided path: {path:?}",
        ))
    })?;
    let filename = path
        .file_name()
        .and_then(|x| x.to_str())
        .ok_or_else(|| Error::MetadataError(format!("unable to get filename: {path:?}")))?;

    let timestamp = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH);
    let timestamp = timestamp.map_err(|e| {
        Error::MetadataError(format!("failed to get current time for temp file: {e}"))
    })?;
    let temp_name = format!("{}.tmp.{}", filename, timestamp.as_nanos());
    let temp_path = directory.join(temp_name);

    let mut temp_file = fs.create_file(&temp_path).await?;
    let mut result = fs.write_file(&mut temp_file, &data).await;
    if result.is_ok() {
        result = fs.sync_file(&temp_file).await;
    }
    drop(temp_file);
    if result.is_ok() {
        result = fs.rename_file(&temp_path, path).await;
    }
    if result.is_err() {
        let _ = fs.remove_file(&temp_path).await.inspect_err(|e| {
            tracing::warn!("failed to remove temp file at {:?}: {}", temp_path, e)
        });
    }
    result
}

async fn truncate(fs: &impl Filesystem, path: &Path, size: usize) -> Result<()> {
    let file = match fs.open_file(path).await {
        Ok(file) => file,
        Err(Error::FilesystemError(err)) if err.kind() == ErrorKind::NotFound && size == 0 => {
            return Ok(())
        }
        Err(err) => return Err(err),
    };
    fs.truncate_file(&file, size).await
}

async fn run<Output, F: Future<Output = Result<Output>>>(
    mut g: genawaiter::sync::Gen<ProtocolYield, Result<ProtocolResume>, F>,
    io: &Arc<dyn turso_core::IO>,
    fs: &impl Filesystem,
    sync_server: &impl SyncServer,
) -> Result<Output> {
    let mut resume = Ok(ProtocolResume::None);
    let mut consuming_stream = None;
    'outer: loop {
        let yield_result = g.resume_with(resume);
        resume = Ok(ProtocolResume::None);
        if let genawaiter::GeneratorState::Yielded(y) = &yield_result {
            tracing::debug!("sync protocol request: {:?}", y);
        }
        match yield_result {
            genawaiter::GeneratorState::Yielded(ProtocolYield::IO) => {
                if let Err(e) = io.run_once() {
                    resume = Err(e.into());
                }
            }
            genawaiter::GeneratorState::Yielded(ProtocolYield::ReadFile { path }) => {
                resume = match fs.read_file(&path).await {
                    Ok(data) => Ok(ProtocolResume::Content(Some(data))),
                    Err(Error::FilesystemError(err)) if err.kind() == ErrorKind::NotFound => {
                        Ok(ProtocolResume::Content(None))
                    }
                    Err(err) => Err(err),
                }
            }
            genawaiter::GeneratorState::Yielded(ProtocolYield::ExistsFile { path }) => {
                resume = fs.exists_file(&path).await.map(ProtocolResume::Exists);
            }
            genawaiter::GeneratorState::Yielded(ProtocolYield::Bootstrap {
                synced_path,
                draft_path,
            }) => {
                resume = bootstrap(fs, sync_server, &synced_path, &draft_path)
                    .await
                    .map(|x| ProtocolResume::Bootstrapped {
                        generation: x.current_generation,
                    });
            }
            genawaiter::GeneratorState::Yielded(ProtocolYield::WriteFile { path, data }) => {
                resume = write(fs, &path, &data).await.map(|_| ProtocolResume::None);
            }
            genawaiter::GeneratorState::Yielded(ProtocolYield::RemoveFile { path }) => {
                resume = fs.remove_file(&path).await.map(|_| ProtocolResume::None);
            }
            genawaiter::GeneratorState::Yielded(ProtocolYield::TruncateFile { path, size }) => {
                resume = truncate(fs, &path, size)
                    .await
                    .map(|_| ProtocolResume::None);
            }
            genawaiter::GeneratorState::Yielded(ProtocolYield::WalPull {
                generation,
                start_frame,
            }) => {
                assert!(consuming_stream.is_none());

                let mut stream = match sync_server.wal_pull(generation, start_frame).await {
                    Ok(result) => result,
                    Err(err) => {
                        resume = Err(err);
                        continue 'outer;
                    }
                };
                let chunk = stream.read_chunk().await;
                let chunk = match chunk {
                    Ok(chunk) => chunk,
                    Err(err) => {
                        resume = Err(err);
                        continue 'outer;
                    }
                };
                if chunk.is_some() {
                    consuming_stream = Some(stream);
                }
                resume = Ok(ProtocolResume::Content(chunk.map(|x| x.to_vec())));
            }
            genawaiter::GeneratorState::Yielded(ProtocolYield::Continue) => {
                assert!(consuming_stream.is_some());
                let mut stream = consuming_stream.take().unwrap();
                let chunk = stream.read_chunk().await;
                let chunk = match chunk {
                    Ok(chunk) => chunk,
                    Err(err) => {
                        resume = Err(err);
                        continue 'outer;
                    }
                };
                if chunk.is_some() {
                    consuming_stream = Some(stream);
                }
                resume = Ok(ProtocolResume::Content(chunk.map(|x| x.to_vec())));
            }
            genawaiter::GeneratorState::Complete(result) => return result,
            genawaiter::GeneratorState::Yielded(y) => panic!("not implemented: {:?}", y),
        }
    }
}

impl Builder {
    pub fn new_synced(path: &str, sync_url: &str, auth_token: Option<String>) -> Self {
        Self {
            path: path.to_string(),
            sync_url: sync_url.to_string(),
            auth_token,
            encryption_key: None,
            connector: None,
        }
    }
    pub fn with_encryption_key(self, encryption_key: &str) -> Self {
        Self {
            encryption_key: Some(encryption_key.to_string()),
            ..self
        }
    }
    pub fn with_connector(self, connector: HttpsConnector<HttpConnector>) -> Self {
        Self {
            connector: Some(connector),
            ..self
        }
    }
    pub async fn build(self) -> Result<Database> {
        let path = PathBuf::from(self.path);
        let connector = self.connector.map(Ok).unwrap_or_else(default_connector)?;
        let executor = TokioExecutor::new();
        let client = hyper_util::client::legacy::Builder::new(executor).build(connector);
        let sync_server = TursoSyncServer::new(
            client,
            TursoSyncServerOpts {
                sync_url: self.sync_url,
                auth_token: self.auth_token,
                encryption_key: self.encryption_key,
                pull_batch_size: None,
            },
        )?;
        let filesystem = TokioFilesystem();
        let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
        let init = genawaiter::sync::Gen::new({
            let io = io.clone();
            |coro| async move {
                let inner = DatabaseInner::new(&coro, io, &path).await?;
                Result::Ok(inner)
            }
        });
        let inner = run(init, &io, &filesystem, &sync_server).await?;
        Ok(Database {
            inner,
            io,
            filesystem,
            sync_server,
        })
    }
}

impl Database {
    pub async fn sync(&mut self) -> Result<()> {
        let inner = &mut self.inner;
        let sync = genawaiter::sync::Gen::new(|coro| async move { inner.sync(&coro).await });
        run(sync, &self.io, &self.filesystem, &self.sync_server).await
    }
    pub async fn pull(&mut self) -> Result<()> {
        let inner = &mut self.inner;
        let pull = genawaiter::sync::Gen::new(|coro| async move { inner.pull(&coro).await });
        run(pull, &self.io, &self.filesystem, &self.sync_server).await
    }
    pub async fn push(&mut self) -> Result<()> {
        let inner = &mut self.inner;
        let push = genawaiter::sync::Gen::new(|coro| async move { inner.push(&coro).await });
        run(push, &self.io, &self.filesystem, &self.sync_server).await
    }
    pub async fn connect(&self) -> Result<turso::Connection> {
        let inner = &self.inner;
        let connect = genawaiter::sync::Gen::new(|coro| async move { inner.connect(&coro).await });
        let conn = run(connect, &self.io, &self.filesystem, &self.sync_server).await?;
        Ok(turso::Connection::create(conn))
    }
}

pub async fn default_sync_server(
    sync_url: String,
    auth_token: Option<String>,
) -> Result<TursoSyncServer> {
    let connector = default_connector()?;
    let executor = TokioExecutor::new();
    let client = hyper_util::client::legacy::Builder::new(executor).build(connector);
    TursoSyncServer::new(
        client,
        TursoSyncServerOpts {
            sync_url: sync_url,
            auth_token: auth_token,
            encryption_key: None,
            pull_batch_size: None,
        },
    )
}

pub fn default_connector() -> Result<HttpsConnector<HttpConnector>> {
    let tls_config = rustls::ClientConfig::builder()
        .with_native_roots()
        .map_err(|e| Error::DatabaseSyncError(format!("unable to configure CA roots: {e}")))?
        .with_no_client_auth();
    Ok(HttpsConnectorBuilder::new()
        .with_tls_config(tls_config)
        .https_or_http()
        .enable_http1()
        .build())
}
