use super::{BackendError, DatabaseFileHandle, DatabaseInstance, QueryResult, SqlBackend};
use crate::backends::DefaultDatabaseResolver;
use crate::parser::ast::{Backend, Capability, DatabaseConfig, DatabaseLocation};
use async_trait::async_trait;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::NamedTempFile;
use turso_pg_client::{BackendEvent, ConnParams, PgConn, error_message};

/// How long to wait for a spawned tursopg server to accept connections.
const SERVER_STARTUP_TIMEOUT: Duration = Duration::from_secs(10);

/// How many ports to try before giving up on starting a server.
const PORT_ATTEMPTS: u32 = 20;

/// Hands out a distinct port to every server this process starts.
///
/// Asking the OS for an ephemeral port means binding a listener, reading the
/// port, and closing it again — and between the close and the server's own
/// bind, a sibling test doing the same thing can be handed the same port. One
/// server then loses the race and exits "Address already in use", while its
/// client happily connects to the *other* test's server and fails later when
/// that one is torn down. Walking a counter instead means two tests here
/// never aim at the same port; a clash with an unrelated process is still
/// possible, and the caller retries for that.
fn next_port() -> u16 {
    use std::sync::atomic::{AtomicU16, Ordering};
    /// Above the range Linux uses for ephemeral ports by default, so the
    /// counter does not fight with unrelated connections on the machine.
    const FIRST_PORT: u16 = 21000;
    const LAST_PORT: u16 = 40000;
    static NEXT: AtomicU16 = AtomicU16::new(FIRST_PORT);
    NEXT.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |p| {
        Some(if p >= LAST_PORT { FIRST_PORT } else { p + 1 })
    })
    .unwrap_or(FIRST_PORT)
}

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

        // Try a fresh port for each attempt: a clash with an unrelated
        // process makes the server exit rather than fall back, and there is
        // nothing to inspect until it has bound.
        let mut last_error = String::new();
        for _ in 0..PORT_ATTEMPTS {
            let port = next_port();
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

            // Wait for the server to accept the startup handshake. The exit
            // check comes before the connect attempt, not after: a server
            // that failed to bind is already gone, and connecting anyway
            // would reach whoever else holds the port.
            let deadline = Instant::now() + SERVER_STARTUP_TIMEOUT;
            let mut conn = None;
            loop {
                if let Some(status) = child.try_wait().ok().flatten() {
                    let stderr = read_stderr(&mut child).await;
                    last_error = format!(
                        "tursopg exited with {status} before accepting connections on port {port}: {stderr}"
                    );
                    break;
                }
                match connect(&params).await {
                    Ok(c) => {
                        conn = Some(c);
                        break;
                    }
                    Err(e) => {
                        if Instant::now() >= deadline {
                            last_error = format!(
                                "tursopg did not accept connections within {SERVER_STARTUP_TIMEOUT:?}: {e}"
                            );
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                }
            }
            let Some(conn) = conn else {
                continue;
            };

            conn.set_read_timeout(Some(self.timeout))
                .map_err(|e| BackendError::CreateDatabase(e.to_string()))?;

            return Ok(Box::new(PgDatabaseInstance {
                child,
                conn: Some(conn),
                timeout: self.timeout,
                _temp_file: temp_file,
            }));
        }
        Err(BackendError::CreateDatabase(format!(
            "no tursopg server started after {PORT_ATTEMPTS} ports; last: {last_error}"
        )))
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

impl PgDatabaseInstance {
    /// The server's stderr if it has exited, or None if it is still up. Waits
    /// briefly, because the socket error usually reaches us a moment before
    /// the process is reaped.
    async fn server_died(&mut self) -> Option<String> {
        for _ in 0..25 {
            if self.child.try_wait().ok().flatten().is_some() {
                let stderr = read_stderr(&mut self.child).await;
                return Some(if stderr.is_empty() {
                    "no output on stderr".to_string()
                } else {
                    stderr
                });
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        None
    }
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
            Err(e) => {
                // A protocol error usually means the server died. Its stderr
                // holds the panic that explains why, and this is the only
                // place it can still be read — without it the failure reads
                // as "server closed the connection" and says nothing.
                return Err(BackendError::Execute(match self.server_died().await {
                    Some(stderr) => format!("{e}; tursopg died: {stderr}"),
                    None => e.to_string(),
                }));
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    /// The whole point of the counter: two servers started concurrently must
    /// never be told to bind the same port. Reusing one meant a test could
    /// connect to another test's server and fail when that one shut down.
    #[test]
    fn ports_are_never_handed_out_twice() {
        let threads: Vec<_> = (0..8)
            .map(|_| std::thread::spawn(|| (0..250).map(|_| next_port()).collect::<Vec<u16>>()))
            .collect();
        let ports: Vec<u16> = threads
            .into_iter()
            .flat_map(|t| t.join().expect("thread panicked"))
            .collect();

        let unique: HashSet<u16> = ports.iter().copied().collect();
        assert_eq!(
            unique.len(),
            ports.len(),
            "handed out a duplicate port across threads"
        );
        // Above the range Linux picks ephemeral ports from, so the counter
        // does not collide with unrelated connections.
        assert!(
            ports.iter().all(|&p| (21000..=40000).contains(&p)),
            "a port fell outside the range reserved for servers"
        );
    }
}
