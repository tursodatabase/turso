use super::pg_connection::{copy_text_columns, split_statements};
use super::tursopg::database_path;
use super::{BackendError, DatabaseFileHandle, DatabaseInstance, QueryResult, SqlBackend};
use crate::backends::DefaultDatabaseResolver;
use crate::parser::ast::{Backend, Capability, DatabaseConfig};
use async_trait::async_trait;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tempfile::NamedTempFile;
use turso_pg::types::Value;
use turso_pg::{Connection, StatementResultKind, TextType};

/// Backend that runs tests on turso_pg in this process, with no pgwire code.
/// It runs each statement with [turso_pg::Connection] and prints the values
/// with [turso_pg::value_text].
///
/// It expects the same results as the tursopg backend: tests and `expect`
/// blocks for tursopg apply to it too.
pub struct TursopgEmbeddedBackend {
    /// Timeout for query execution
    timeout: Duration,
    /// Resolver for default database paths
    default_db_resolver: Option<Arc<dyn DefaultDatabaseResolver>>,
}

impl TursopgEmbeddedBackend {
    pub fn new() -> Self {
        Self {
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

impl Default for TursopgEmbeddedBackend {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SqlBackend for TursopgEmbeddedBackend {
    fn name(&self) -> &str {
        "tursopg-embedded"
    }

    fn backend_type(&self) -> Backend {
        Backend::Tursopg
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
        let (path, temp_file) = database_path(config, self.default_db_resolver.as_deref())?;
        let flags = if config.readonly {
            turso_pg::OpenFlags::default().union(turso_pg::OpenFlags::ReadOnly)
        } else {
            turso_pg::OpenFlags::default()
        };
        let opts = turso_pg::tursopg_database_opts();
        let session = tokio::task::spawn_blocking(move || {
            let (_, db) = turso_pg::open_database(&path, None, flags, opts)?;
            let conn = Connection::connect(&db)?;
            Ok::<_, turso_pg::Error>(Session { _db: db, conn })
        })
        .await
        .expect("open task panicked")
        .map_err(|e| BackendError::CreateDatabase(e.to_string()))?;
        Ok(Box::new(TursopgEmbeddedInstance {
            session: Some(session),
            timeout: self.timeout,
            _temp_file: temp_file,
        }))
    }
}

struct Session {
    _db: Arc<turso_pg::Database>,
    conn: Connection,
}

pub struct TursopgEmbeddedInstance {
    /// The open database and connection; `None` while a query runs and after
    /// a query timed out
    session: Option<Session>,
    timeout: Duration,
    /// Keep temp file alive - it's deleted when this is dropped
    _temp_file: Option<NamedTempFile>,
}

#[async_trait]
impl DatabaseInstance for TursopgEmbeddedInstance {
    async fn execute(&mut self, sql: &str) -> Result<QueryResult, BackendError> {
        // Each statement runs on its own, like a query that the tursopg
        // backend sends (see PgConnection::execute).
        let mut rows = Vec::new();
        for statement in split_statements(sql) {
            let result = self.execute_statement(statement).await?;
            if result.is_error() {
                return Ok(result);
            }
            rows.extend(result.rows);
        }
        Ok(QueryResult::success(rows))
    }

    async fn close(mut self: Box<Self>) -> Result<DatabaseFileHandle, BackendError> {
        if let Some(session) = self.session.take() {
            session
                .conn
                .close()
                .map_err(|e| BackendError::Close(e.to_string()))?;
        }
        match self._temp_file {
            Some(tf) => Ok(DatabaseFileHandle::temp(tf)),
            None => Ok(DatabaseFileHandle::none()),
        }
    }
}

impl TursopgEmbeddedInstance {
    async fn execute_statement(&mut self, sql: String) -> Result<QueryResult, BackendError> {
        let session = self
            .session
            .take()
            .ok_or_else(|| BackendError::Execute("connection lost by earlier timeout".into()))?;
        let task = tokio::task::spawn_blocking(move || {
            let result = run_query(&session.conn, &sql);
            (session, result)
        });
        let Ok(joined) = tokio::time::timeout(self.timeout, task).await else {
            return Err(BackendError::Timeout(self.timeout));
        };
        let (session, result) = joined.expect("query task panicked");
        self.session = Some(session);
        Ok(result)
    }
}

/// Runs the statements of `sql` and returns their rows as text, with NULL as
/// an empty string. An error discards the rows, as a query that the tursopg
/// backend sends does.
fn run_query(conn: &Connection, sql: &str) -> QueryResult {
    let mut rows = Vec::new();
    match run_statements(conn, sql, &mut rows) {
        Ok(()) => QueryResult::success(rows),
        Err(error) => QueryResult::error_with_sqlstate(
            error.message.clone(),
            Some(error.sqlstate.to_string()),
        ),
    }
}

fn run_statements(
    conn: &Connection,
    sql: &str,
    rows: &mut Vec<Vec<String>>,
) -> turso_pg::Result<()> {
    for stmt in conn.query_runner(sql.as_bytes()) {
        let mut stmt = stmt?.expect("the query runner prepares every statement");
        match stmt.result_kind() {
            StatementResultKind::Command => stmt.run_ignore_rows()?,
            StatementResultKind::Rows => {
                let text_types: Vec<TextType> = (0..stmt.num_columns())
                    .map(|index| TextType::of_column(&stmt, index))
                    .collect();
                while let Some(row) = stmt.run_one_step_blocking(|| Ok(()), || Ok(()))? {
                    rows.push(
                        row.get_values()
                            .zip(&text_types)
                            .map(|(value, text_type)| value_text(value, *text_type))
                            .collect(),
                    );
                }
            }
            // The encoder is created after the first step, as in the tursopg
            // server's copy_to_stdout.
            StatementResultKind::CopyOut => {
                let mut has_row = stmt.run_one_step_blocking(|| Ok(()), || Ok(()))?.is_some();
                let encoder = turso_pg::CopyTextEncoder::new(&stmt);
                while has_row {
                    let mut line = Vec::new();
                    encoder.encode_row(stmt.row().expect("COPY row is present"), &mut line);
                    rows.push(
                        copy_text_columns(line)
                            .expect("turso_pg writes COPY rows as lines of text"),
                    );
                    has_row = stmt.run_one_step_blocking(|| Ok(()), || Ok(()))?.is_some();
                }
            }
        }
    }
    Ok(())
}

/// The PostgreSQL text of a value, with NULL as an empty string.
fn value_text(value: &Value, text_type: TextType) -> String {
    match turso_pg::value_text(value, text_type) {
        Some(text) => text.into_owned(),
        None => String::new(),
    }
}
