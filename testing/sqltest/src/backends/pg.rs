use super::{BackendError, DatabaseFileHandle, DatabaseInstance, QueryResult, SqlBackend};
use crate::backends::DefaultDatabaseResolver;
use crate::parser::ast::{Backend, Capability, DatabaseConfig, DatabaseLocation};
use async_trait::async_trait;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
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
///
/// With [PgBackend::postgres], the same tests run against a real PostgreSQL
/// server instead: each database instance is a fresh database on that server.
/// This checks that the expectations in the PostgreSQL corpus are what
/// PostgreSQL itself does.
pub struct PgBackend {
    server: Server,
    /// Timeout for query execution
    timeout: Duration,
    /// Resolver for default database paths
    default_db_resolver: Option<Arc<dyn DefaultDatabaseResolver>>,
}

enum Server {
    /// Spawn this tursopg binary for each database instance.
    Tursopg { binary_path: PathBuf },
    /// Create a database for each instance on this PostgreSQL server.
    Postgres {
        admin: ConnParams,
        next_database: AtomicUsize,
    },
}

impl PgBackend {
    /// Create a new pg backend with the given tursopg binary path
    pub fn new(binary_path: impl Into<PathBuf>) -> Self {
        Self {
            server: Server::Tursopg {
                binary_path: binary_path.into(),
            },
            timeout: Duration::from_secs(30),
            default_db_resolver: None,
        }
    }

    /// Create a backend that runs tests against the PostgreSQL server at `dsn`.
    /// The DSN's user must be allowed to create and drop databases.
    pub fn postgres(dsn: &str) -> Result<Self, BackendError> {
        let admin =
            ConnParams::parse(dsn).map_err(|e| BackendError::NotAvailable(e.to_string()))?;
        Ok(Self {
            server: Server::Postgres {
                admin,
                next_database: AtomicUsize::new(0),
            },
            timeout: Duration::from_secs(30),
            default_db_resolver: None,
        })
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
        match self.server {
            Server::Tursopg { .. } => "pg",
            Server::Postgres { .. } => "postgres",
        }
    }

    fn backend_type(&self) -> Backend {
        Backend::Pg
    }

    fn is_postgres(&self) -> bool {
        matches!(self.server, Server::Postgres { .. })
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
        match &self.server {
            Server::Tursopg { binary_path } => {
                self.create_tursopg_database(binary_path, config).await
            }
            Server::Postgres {
                admin,
                next_database,
            } => {
                let number = next_database.fetch_add(1, Ordering::Relaxed);
                self.create_postgres_database(admin, number, config).await
            }
        }
    }
}

impl PgBackend {
    async fn create_tursopg_database(
        &self,
        binary_path: &PathBuf,
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

        let mut cmd = tokio::process::Command::new(binary_path);
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
            BackendError::CreateDatabase(format!("failed to spawn {}: {e}", binary_path.display()))
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
            owner: InstanceOwner::Tursopg {
                child,
                _temp_file: temp_file,
            },
            conn: Some(conn),
            timeout: self.timeout,
        }))
    }

    async fn create_postgres_database(
        &self,
        admin: &ConnParams,
        number: usize,
        config: &DatabaseConfig,
    ) -> Result<Box<dyn DatabaseInstance>, BackendError> {
        if !matches!(
            config.location,
            DatabaseLocation::Memory | DatabaseLocation::TempFile
        ) || config.readonly
        {
            return Err(BackendError::NotAvailable(
                "the PostgreSQL server backend only creates fresh :memory: and :temp: databases"
                    .to_string(),
            ));
        }
        let name = format!("sqltest_{}_{number}", std::process::id());
        run_admin_statement(admin, &format!("DROP DATABASE IF EXISTS {name}")).await?;
        run_admin_statement(admin, &format!("CREATE DATABASE {name}")).await?;
        let mut params = admin.clone();
        params.database = name.clone();
        let conn = connect(&params)
            .await
            .map_err(|e| BackendError::CreateDatabase(e.to_string()))?;
        conn.set_read_timeout(Some(self.timeout))
            .map_err(|e| BackendError::CreateDatabase(e.to_string()))?;
        Ok(Box::new(PgDatabaseInstance {
            owner: InstanceOwner::PostgresDatabase {
                admin: admin.clone(),
                name,
            },
            conn: Some(conn),
            timeout: self.timeout,
        }))
    }
}

/// Runs one statement on the server's admin database and fails on an error.
async fn run_admin_statement(admin: &ConnParams, sql: &str) -> Result<(), BackendError> {
    let mut conn = connect(admin)
        .await
        .map_err(|e| BackendError::CreateDatabase(e.to_string()))?;
    let sql = sql.to_string();
    let events = tokio::task::spawn_blocking(move || conn.simple_query(&sql))
        .await
        .expect("admin statement task panicked")
        .map_err(|e| BackendError::CreateDatabase(e.to_string()))?;
    for event in events {
        if let BackendEvent::ErrorResponse(fields) = event {
            return Err(BackendError::CreateDatabase(
                error_message(&fields).to_string(),
            ));
        }
    }
    Ok(())
}

async fn connect(params: &ConnParams) -> Result<PgConn, turso_pg_client::Error> {
    let params = params.clone();
    tokio::task::spawn_blocking(move || {
        PgConn::connect(&params, &[("application_name", "sqltest")])
    })
    .await
    .expect("connect task panicked")
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

/// What a database instance owns, and must clean up when it closes.
enum InstanceOwner {
    /// A dedicated tursopg server process.
    Tursopg {
        child: tokio::process::Child,
        /// Keep temp file alive - it's deleted when this is dropped
        _temp_file: Option<NamedTempFile>,
    },
    /// A database created for this instance on a PostgreSQL server.
    PostgresDatabase { admin: ConnParams, name: String },
}

/// A database instance reached over one PostgreSQL wire-protocol connection.
pub struct PgDatabaseInstance {
    owner: InstanceOwner,
    /// The single connection to the server. Taken while a query runs on the
    /// blocking pool and put back afterwards; left empty after a timeout,
    /// since the wire state is indeterminate mid-protocol.
    conn: Option<PgConn>,
    timeout: Duration,
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
        match self.owner {
            InstanceOwner::Tursopg {
                mut child,
                _temp_file,
            } => {
                let _ = child.kill().await;
                match _temp_file {
                    Some(tf) => Ok(DatabaseFileHandle::temp(tf)),
                    None => Ok(DatabaseFileHandle::none()),
                }
            }
            InstanceOwner::PostgresDatabase { admin, name } => {
                run_admin_statement(
                    &admin,
                    &format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"),
                )
                .await
                .map_err(|e| BackendError::Close(e.to_string()))?;
                Ok(DatabaseFileHandle::none())
            }
        }
    }
}
