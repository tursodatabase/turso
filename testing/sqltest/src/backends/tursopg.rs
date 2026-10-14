use super::pg_connection::PgConnection;
use super::{BackendError, DatabaseFileHandle, DatabaseInstance, QueryResult, SqlBackend};
use crate::backends::DefaultDatabaseResolver;
use crate::parser::ast::{Backend, Capability, DatabaseConfig, DatabaseLocation};
use async_trait::async_trait;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::NamedTempFile;
use turso_pg_client::ConnParams;

/// How long to wait for a spawned tursopg server to accept connections.
const SERVER_STARTUP_TIMEOUT: Duration = Duration::from_secs(10);

/// Backend that runs tests on tursopg. Each database instance spawns its own
/// `tursopg --server` on an ephemeral port and drives it over one
/// [PgConnection].
pub struct TursopgBackend {
    /// Path to the tursopg binary
    binary_path: PathBuf,
    /// Timeout for query execution
    timeout: Duration,
    /// Resolver for default database paths
    default_db_resolver: Option<Arc<dyn DefaultDatabaseResolver>>,
}

impl TursopgBackend {
    /// Create a new tursopg backend with the given tursopg binary path
    pub fn new(binary_path: impl Into<PathBuf>) -> Self {
        Self {
            binary_path: binary_path.into(),
            timeout: Duration::from_secs(30),
            default_db_resolver: None,
        }
    }

    /// Set the timeout for query execution
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Set the default database resolver
    pub fn with_default_db_resolver(mut self, resolver: Arc<dyn DefaultDatabaseResolver>) -> Self {
        self.default_db_resolver = Some(resolver);
        self
    }
}

#[async_trait]
impl SqlBackend for TursopgBackend {
    fn name(&self) -> &str {
        "tursopg"
    }

    fn backend_type(&self) -> Backend {
        Backend::Tursopg
    }

    fn capabilities(&self) -> HashSet<Capability> {
        // The pg frontend's feature surface is distinct from the sqlite
        // capabilities tests can require; grow this set as pg coverage
        // starts needing capability gating.
        HashSet::new()
    }

    async fn create_database(
        &self,
        config: &DatabaseConfig,
    ) -> Result<Box<dyn DatabaseInstance>, BackendError> {
        let (db_path, temp_file) = match &config.location {
            DatabaseLocation::Memory => (":memory:".to_string(), None),
            DatabaseLocation::TempFile => {
                let temp = NamedTempFile::new()
                    .map_err(|e| BackendError::CreateDatabase(e.to_string()))?;
                let path = temp.path().to_string_lossy().to_string();
                (path, Some(temp))
            }
            DatabaseLocation::Path(path) => (path.to_string_lossy().to_string(), None),
            DatabaseLocation::Default | DatabaseLocation::DefaultNoRowidAlias => {
                let resolved = self
                    .default_db_resolver
                    .as_ref()
                    .and_then(|r| r.resolve(&config.location))
                    .ok_or_else(|| {
                        BackendError::CreateDatabase(
                            "default database not generated - no resolver configured".to_string(),
                        )
                    })?;
                (resolved.to_string_lossy().to_string(), None)
            }
        };

        let mut cmd = tokio::process::Command::new(&self.binary_path);
        cmd.arg(&db_path)
            .arg("--server")
            .arg("127.0.0.1:0")
            .arg("-q");
        if config.readonly {
            cmd.arg("--readonly");
        }
        cmd.stdin(std::process::Stdio::null());
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());
        cmd.kill_on_drop(true);
        let mut child = cmd.spawn().map_err(|e| {
            BackendError::CreateDatabase(format!(
                "failed to spawn {}: {e}",
                self.binary_path.display()
            ))
        })?;

        let port = match tokio::time::timeout(
            SERVER_STARTUP_TIMEOUT,
            read_listening_port(&mut child),
        )
        .await
        {
            Ok(Some(port)) => port,
            Ok(None) => {
                let stderr = read_stderr(&mut child).await;
                return Err(BackendError::CreateDatabase(format!(
                    "tursopg did not report its listening address: {stderr}"
                )));
            }
            Err(_) => {
                return Err(BackendError::CreateDatabase(format!(
                    "tursopg did not report its listening address within {SERVER_STARTUP_TIMEOUT:?}"
                )));
            }
        };

        let params = ConnParams {
            host: "127.0.0.1".to_string(),
            port,
            user: "sqltest".to_string(),
            password: None,
            database: "main".to_string(),
        };

        // Wait for the server to accept the startup handshake.
        let deadline = Instant::now() + SERVER_STARTUP_TIMEOUT;
        let conn = loop {
            match PgConnection::connect(&params, self.timeout).await {
                Ok(conn) => break conn,
                Err(e) => {
                    if let Some(status) = child.try_wait().ok().flatten() {
                        let stderr = read_stderr(&mut child).await;
                        return Err(BackendError::CreateDatabase(format!(
                            "tursopg exited with {status} before accepting connections: {stderr}"
                        )));
                    }
                    if Instant::now() >= deadline {
                        return Err(BackendError::CreateDatabase(format!(
                            "tursopg did not accept connections within {SERVER_STARTUP_TIMEOUT:?}: {e}"
                        )));
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
        };

        Ok(Box::new(TursopgDatabaseInstance {
            child,
            conn,
            _temp_file: temp_file,
        }))
    }
}

/// Reads the server's `PostgreSQL server listening on <host>:<port> ...`
/// line and returns the port. Returns `None` if the server exits first.
async fn read_listening_port(child: &mut tokio::process::Child) -> Option<u16> {
    use tokio::io::AsyncBufReadExt;
    let stdout = child.stdout.take()?;
    let mut lines = tokio::io::BufReader::new(stdout).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let Some(rest) = line.strip_prefix("PostgreSQL server listening on ") else {
            continue;
        };
        let address = rest.split_whitespace().next()?;
        let port = address.rsplit_once(':')?.1.parse().ok()?;
        tokio::spawn(async move { while let Ok(Some(_)) = lines.next_line().await {} });
        return Some(port);
    }
    None
}

async fn read_stderr(child: &mut tokio::process::Child) -> String {
    use tokio::io::AsyncReadExt;
    let mut buf = String::new();
    if let Some(mut stderr) = child.stderr.take() {
        let _ = stderr.read_to_string(&mut buf).await;
    }
    buf.trim().to_string()
}

/// A database instance backed by a dedicated tursopg server process.
pub struct TursopgDatabaseInstance {
    child: tokio::process::Child,
    /// The single connection to the server.
    conn: PgConnection,
    /// Keep temp file alive - it's deleted when this is dropped
    _temp_file: Option<NamedTempFile>,
}

#[async_trait]
impl DatabaseInstance for TursopgDatabaseInstance {
    async fn execute(&mut self, sql: &str) -> Result<QueryResult, BackendError> {
        self.conn.execute(sql).await
    }

    async fn close(mut self: Box<Self>) -> Result<DatabaseFileHandle, BackendError> {
        let _ = self.child.kill().await;
        match self._temp_file {
            Some(tf) => Ok(DatabaseFileHandle::temp(tf)),
            None => Ok(DatabaseFileHandle::none()),
        }
    }
}
