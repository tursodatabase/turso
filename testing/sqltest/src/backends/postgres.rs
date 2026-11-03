use super::pg_connection::PgConnection;
use super::{BackendError, DatabaseFileHandle, DatabaseInstance, QueryResult, SqlBackend};
use crate::parser::ast::{Backend, Capability, DatabaseConfig};
use async_trait::async_trait;
use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Semaphore;
use turso_pg_client::{BackendEvent, ConnParams, PgConn, error_message};

/// How many CREATE DATABASE and DROP DATABASE statements run at the same time.
const MAX_ADMIN_STATEMENTS: usize = 4;

/// Backend that runs tests on a real PostgreSQL server. Each database instance is a
/// new database on the server, so tests that run in parallel are isolated.
pub struct PostgresBackend {
    server: Arc<PostgresServer>,
    timeout: Duration,
}

impl PostgresBackend {
    /// Connects to the server at `dsn`.
    ///
    /// The role in the DSN must be able to create databases. The tests of
    /// roles expect it to be the bootstrap superuser `postgres`, as the
    /// clients of tursopg are.
    pub async fn connect(dsn: &str, timeout: Duration) -> Result<Self, BackendError> {
        let params =
            ConnParams::parse(dsn).map_err(|e| BackendError::NotAvailable(e.to_string()))?;
        let role = params.user.clone();
        let mut server = PostgresServer {
            params,
            database_prefix: format!("sqltest_{:08x}", rand::random::<u32>()),
            next_database: AtomicUsize::new(0),
            admin_statements: Semaphore::new(MAX_ADMIN_STATEMENTS),
            idle_admin_conns: Mutex::new(Vec::new()),
            roles_before_the_run: HashSet::new(),
            created_roles: Mutex::new(HashSet::new()),
            timeout,
        };
        let rows = server
            .run_admin_query(
                "SELECT rolsuper OR rolcreatedb FROM pg_roles WHERE rolname = current_user",
            )
            .await
            .map_err(|e| BackendError::NotAvailable(format!("cannot use {dsn}: {e}")))?;
        assert_eq!(rows.len(), 1, "pg_roles must have one row for current_user");
        if rows[0][0] != "t" {
            return Err(BackendError::NotAvailable(format!(
                "role {role} cannot create databases: ALTER ROLE {role} CREATEDB"
            )));
        }
        server.roles_before_the_run = server
            .role_names()
            .await
            .map_err(|e| BackendError::NotAvailable(format!("cannot use {dsn}: {e}")))?;
        Ok(Self {
            server: Arc::new(server),
            timeout,
        })
    }

    /// Returns what drops the roles that the tests create. Roles belong to
    /// the whole server, not to the databases of the tests, so without it a
    /// later run on the same server fails to create them again. A role is
    /// dropped only after the run, because the tests that run in parallel
    /// may still use the roles they created. Only roles that the tests
    /// created with CREATE ROLE are dropped, so roles that someone else
    /// creates on the server during the run are kept. A role that existed
    /// before the run is kept too, since a test can try to create a role that
    /// exists, such as `postgres`, to check that it fails.
    pub fn role_cleanup(&self) -> PostgresRoleCleanup {
        PostgresRoleCleanup {
            server: Arc::clone(&self.server),
        }
    }
}

/// Drops the roles that the tests of the run created and that did not exist
/// when the run started.
pub struct PostgresRoleCleanup {
    server: Arc<PostgresServer>,
}

impl PostgresRoleCleanup {
    pub async fn drop_roles_created_by_the_run(self) -> Result<(), String> {
        let names = std::mem::take(&mut *self.server.created_roles.lock().unwrap());
        for name in names.difference(&self.server.roles_before_the_run) {
            self.server
                .run_admin_query(&format!(
                    "DROP ROLE IF EXISTS \"{}\"",
                    name.replace('"', "\"\"")
                ))
                .await?;
        }
        Ok(())
    }
}

#[async_trait]
impl SqlBackend for PostgresBackend {
    fn name(&self) -> &str {
        "postgres"
    }

    fn backend_type(&self) -> Backend {
        Backend::Postgres
    }

    fn capabilities(&self) -> HashSet<Capability> {
        HashSet::new()
    }

    fn reports_sqlstate(&self) -> bool {
        true
    }

    async fn create_database(
        &self,
        config: &DatabaseConfig,
    ) -> Result<Box<dyn DatabaseInstance>, BackendError> {
        if !config.is_writable() {
            return Err(BackendError::CreateDatabase(format!(
                "the postgres backend only creates new :memory: and :temp: databases, not {}",
                config.location
            )));
        }
        let name = self
            .server
            .create_database()
            .await
            .map_err(BackendError::CreateDatabase)?;
        let mut params = self.server.params.clone();
        params.database = name.clone();
        match PgConnection::connect(&params, self.timeout).await {
            Ok(conn) => Ok(Box::new(PostgresDatabaseInstance {
                conn,
                server: Arc::clone(&self.server),
                name,
            })),
            Err(e) => {
                let _ = self.server.drop_database(&name).await;
                Err(BackendError::CreateDatabase(e.to_string()))
            }
        }
    }
}

/// A database that the backend made for one test on the PostgreSQL server.
pub struct PostgresDatabaseInstance {
    conn: PgConnection,
    server: Arc<PostgresServer>,
    name: String,
}

#[async_trait]
impl DatabaseInstance for PostgresDatabaseInstance {
    async fn execute(&mut self, sql: &str) -> Result<QueryResult, BackendError> {
        self.server.remember_created_roles(sql);
        self.conn.execute(sql).await
    }

    async fn close(self: Box<Self>) -> Result<DatabaseFileHandle, BackendError> {
        let PostgresDatabaseInstance { conn, server, name } = *self;
        drop(conn);
        server
            .drop_database(&name)
            .await
            .map_err(BackendError::Close)?;
        Ok(DatabaseFileHandle::none())
    }
}

/// The PostgreSQL server, and the connections to its maintenance database
/// that create and drop the databases of the tests.
struct PostgresServer {
    params: ConnParams,
    /// The names of all databases of this run start with this prefix. It has
    /// a random part, so two runs can use the same server at the same time.
    database_prefix: String,
    next_database: AtomicUsize,
    admin_statements: Semaphore,
    idle_admin_conns: Mutex<Vec<PgConn>>,
    /// The roles on the server when the run started.
    roles_before_the_run: HashSet<String>,
    /// The roles that the tests of the run created with CREATE ROLE.
    created_roles: Mutex<HashSet<String>>,
    timeout: Duration,
}

impl PostgresServer {
    async fn create_database(&self) -> Result<String, String> {
        let number = self.next_database.fetch_add(1, Ordering::Relaxed);
        let name = format!("{}_{number}", self.database_prefix);
        self.run_admin_query(&format!(
            "CREATE DATABASE {name} TEMPLATE template0 STRATEGY FILE_COPY"
        ))
        .await?;
        Ok(name)
    }

    async fn drop_database(&self, name: &str) -> Result<(), String> {
        self.run_admin_query(&format!("DROP DATABASE {name} WITH (FORCE)"))
            .await?;
        Ok(())
    }

    async fn role_names(&self) -> Result<HashSet<String>, String> {
        let rows = self.run_admin_query("SELECT rolname FROM pg_roles").await?;
        Ok(rows.into_iter().map(|mut row| row.remove(0)).collect())
    }

    /// Remembers the roles that `sql` creates, so that they are dropped
    /// after the run. SQL that does not parse creates no role.
    fn remember_created_roles(&self, sql: &str) {
        let Ok(parsed) = pg_query::parse(sql) else {
            return;
        };
        let mut created_roles = self.created_roles.lock().unwrap();
        for (node, ..) in parsed.protobuf.nodes() {
            if let pg_query::NodeRef::CreateRoleStmt(stmt) = node {
                created_roles.insert(stmt.role.clone());
            }
        }
    }

    /// Runs `sql` on the maintenance database of the server and returns the
    /// rows. NULL is an empty string.
    async fn run_admin_query(&self, sql: &str) -> Result<Vec<Vec<String>>, String> {
        let _permit = self
            .admin_statements
            .acquire()
            .await
            .expect("admin statement semaphore closed");
        let idle_conn = self.idle_admin_conns.lock().unwrap().pop();
        let mut conn = match idle_conn {
            Some(conn) => conn,
            None => self.connect_admin().await.map_err(|e| e.to_string())?,
        };
        let sql = sql.to_string();
        let (conn, events) = tokio::task::spawn_blocking(move || {
            let events = conn.simple_query(&sql);
            (conn, events)
        })
        .await
        .expect("admin query task panicked");
        let events = events.map_err(|e| e.to_string())?;
        self.idle_admin_conns.lock().unwrap().push(conn);
        let mut rows = Vec::new();
        for event in events {
            match event {
                BackendEvent::DataRow(row) => {
                    rows.push(row.into_iter().map(Option::unwrap_or_default).collect());
                }
                BackendEvent::ErrorResponse(fields) => {
                    return Err(error_message(&fields).to_string());
                }
                _ => {}
            }
        }
        Ok(rows)
    }

    async fn connect_admin(&self) -> Result<PgConn, turso_pg_client::Error> {
        let params = self.params.clone();
        let timeout = self.timeout;
        tokio::task::spawn_blocking(move || {
            let conn = PgConn::connect(&params, &[("application_name", "sqltest")])?;
            conn.set_read_timeout(Some(timeout))?;
            Ok(conn)
        })
        .await
        .expect("connect task panicked")
    }
}
