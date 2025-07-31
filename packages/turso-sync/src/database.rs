use std::path::PathBuf;

use hyper_rustls::{ConfigBuilderExt, HttpsConnector, HttpsConnectorBuilder};
use hyper_util::{client::legacy::connect::HttpConnector, rt::TokioExecutor};

use crate::{
    database_inner::DatabaseInner,
    errors::Error,
    filesystem::tokio::TokioFilesystem,
    sync_server::turso::{TursoSyncServer, TursoSyncServerOpts},
    Result,
};

/// [Database] expose public interface for synced database from [DatabaseInner] private implementation
///
/// This layer also serves a purpose of "gluing" together all component for real use,
/// because [DatabaseInner] abstracts things away in order to simplify testing
pub struct Database(DatabaseInner<TursoSyncServer, TokioFilesystem>);

pub struct Builder {
    path: String,
    sync_url: String,
    auth_token: Option<String>,
    encryption_key: Option<String>,
    connector: Option<HttpsConnector<HttpConnector>>,
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
        let inner = DatabaseInner::new(filesystem, sync_server, &path).await?;
        Ok(Database(inner))
    }
}

impl Database {
    pub async fn sync(&mut self) -> Result<()> {
        self.0.sync().await
    }
    pub async fn pull(&mut self) -> Result<()> {
        self.0.pull().await
    }
    pub async fn push(&mut self) -> Result<()> {
        self.0.push().await
    }
    pub async fn connect(&self) -> Result<turso::Connection> {
        self.0.connect().await
    }
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
