use super::{BackendError, DatabaseFileHandle, DatabaseInstance, QueryResult, SqlBackend};
use crate::backends::DefaultDatabaseResolver;
use crate::parser::ast::{Backend, Capability, DatabaseConfig, DatabaseLocation};
use async_trait::async_trait;
use std::collections::HashSet;
use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::NamedTempFile;
use turso_pg_client::{BackendEvent, ConnParams, PgConn, error_message};

/// How long to wait for a spawned tursopg server to accept connections.
const SERVER_STARTUP_TIMEOUT: Duration = Duration::from_secs(10);

/// PostgreSQL wire-protocol backend. Each database instance spawns its own
/// `tursopg --server` on an ephemeral port and drives it over one connection
/// using the simple query protocol — the same path any PostgreSQL client
/// exercises, so results reflect the server's own value encoding rather than
/// a rendering this runner would have to reimplement.
pub struct PgBackend {
    /// Path to the tursopg binary
    binary_path: PathBuf,
    /// Timeout for query execution
    timeout: Duration,
    /// Resolver for default database paths
    default_db_resolver: Option<Arc<dyn DefaultDatabaseResolver>>,
}

impl PgBackend {
    /// Create a new pg backend with the given tursopg binary path
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
impl SqlBackend for PgBackend {
    fn name(&self) -> &str {
        "pg"
    }

    fn backend_type(&self) -> Backend {
        Backend::Pg
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

        // Reserve an ephemeral port, then hand it to the server. The port
        // could in principle be claimed between the drop and the spawn, in
        // which case the server fails to bind and the readiness loop below
        // reports the exit.
        let port = TcpListener::bind("127.0.0.1:0")
            .and_then(|l| l.local_addr())
            .map_err(|e| BackendError::CreateDatabase(format!("allocating port: {e}")))?
            .port();

        let mut cmd = tokio::process::Command::new(&self.binary_path);
        cmd.arg(&db_path)
            .arg("--server")
            .arg(format!("127.0.0.1:{port}"))
            .arg("-q");
        if config.readonly {
            cmd.arg("--readonly");
        }
        cmd.stdin(std::process::Stdio::null());
        cmd.stdout(std::process::Stdio::null());
        cmd.stderr(std::process::Stdio::piped());
        cmd.kill_on_drop(true);
        let mut child = cmd.spawn().map_err(|e| {
            BackendError::CreateDatabase(format!(
                "failed to spawn {}: {e}",
                self.binary_path.display()
            ))
        })?;

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
            match connect(&params).await {
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
        conn.set_read_timeout(Some(self.timeout))
            .map_err(|e| BackendError::CreateDatabase(e.to_string()))?;

        Ok(Box::new(PgDatabaseInstance {
            child,
            conn: Some(conn),
            timeout: self.timeout,
            _temp_file: temp_file,
        }))
    }
}

async fn connect(params: &ConnParams) -> Result<PgConn, turso_pg_client::Error> {
    let params = params.clone();
    tokio::task::spawn_blocking(move || {
        PgConn::connect(&params, &[("application_name", "sqltest")])
    })
    .await
    .expect("connect task panicked")
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
pub struct PgDatabaseInstance {
    child: tokio::process::Child,
    /// The single connection to the server. Taken while a query runs on the
    /// blocking pool and put back afterwards; left empty after a timeout,
    /// since the wire state is indeterminate mid-protocol.
    conn: Option<PgConn>,
    timeout: Duration,
    /// Keep temp file alive - it's deleted when this is dropped
    _temp_file: Option<NamedTempFile>,
}

#[async_trait]
impl DatabaseInstance for PgDatabaseInstance {
    async fn execute(&mut self, sql: &str) -> Result<QueryResult, BackendError> {
        let mut conn = self
            .conn
            .take()
            .ok_or_else(|| BackendError::Execute("connection lost by earlier timeout".into()))?;
        let sql = sql.to_string();
        let (conn, events) = tokio::task::spawn_blocking(move || {
            let events = conn.simple_query(&sql);
            (conn, events)
        })
        .await
        .expect("query task panicked");

        let events = match events {
            Ok(events) => {
                self.conn = Some(conn);
                events
            }
            Err(turso_pg_client::Error::Io(e))
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                return Err(BackendError::Timeout(self.timeout));
            }
            Err(e) => return Err(BackendError::Execute(e.to_string())),
        };

        // The simple query protocol runs statements in order and aborts the
        // rest of the string after an error, so the first ErrorResponse is
        // the query's outcome. NULL renders as an empty string, matching the
        // list-mode convention the comparison layer expects.
        let mut rows = Vec::new();
        for event in events {
            match event {
                BackendEvent::DataRow(row) => rows.push(
                    row.into_iter()
                        .map(|v| v.unwrap_or_default())
                        .collect::<Vec<String>>(),
                ),
                BackendEvent::ErrorResponse(fields) => {
                    return Ok(QueryResult::error(error_message(&fields).to_string()));
                }
                _ => {}
            }
        }
        Ok(QueryResult::success(rows))
    }

    async fn close(mut self: Box<Self>) -> Result<DatabaseFileHandle, BackendError> {
        self.conn.take();
        let _ = self.child.kill().await;
        match self._temp_file {
            Some(tf) => Ok(DatabaseFileHandle::temp(tf)),
            None => Ok(DatabaseFileHandle::none()),
        }
    }
}
