use serde::{Deserialize, Serialize};
use std::{path::Path, time::Duration};

pub use turso::Value;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("SQLite: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("Turso: {0}")]
    Turso(#[from] turso::Error),
    #[error("{0}")]
    Workload(String),
    #[error("cancelled")]
    Cancelled,
}

impl Error {
    pub fn is_conflict(&self) -> bool {
        match self {
            Self::Sqlite(rusqlite::Error::SqliteFailure(e, _)) => {
                e.code == rusqlite::ErrorCode::DatabaseBusy
            }
            Self::Turso(turso::Error::Busy(_) | turso::Error::BusySnapshot(_)) => true,
            Self::Turso(turso::Error::Error(message)) => message == "Write-write conflict",
            _ => false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
pub enum Engine {
    Sqlite,
    Turso,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
pub enum Journal {
    Wal,
    Mvcc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
pub enum Transaction {
    Deferred,
    Immediate,
    Concurrent,
}

impl Transaction {
    pub fn sql(self) -> &'static str {
        match self {
            Self::Deferred => "BEGIN",
            Self::Immediate => "BEGIN IMMEDIATE",
            Self::Concurrent => "BEGIN CONCURRENT",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Settings {
    pub engine: Engine,
    pub journal: Journal,
    pub transaction: Transaction,
    pub busy_timeout_ms: u64,
    pub io: String,
    pub wal_autocheckpoint_pages: u32,
    pub mvcc_checkpoint_bytes: i64,
    pub mvcc_passive_checkpoint: bool,
}

impl Settings {
    pub fn validate(&self) -> Result<()> {
        if self.engine == Engine::Sqlite && self.journal == Journal::Mvcc {
            return Err(Error::Workload(
                "SQLite does not support MVCC journal mode".into(),
            ));
        }
        if self.transaction == Transaction::Concurrent
            && (self.engine != Engine::Turso || self.journal != Journal::Mvcc)
        {
            return Err(Error::Workload(
                "BEGIN CONCURRENT requires Turso MVCC".into(),
            ));
        }
        if self.journal == Journal::Mvcc && self.transaction == Transaction::Immediate {
            return Err(Error::Workload(
                "use deferred or concurrent transactions with MVCC".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone)]
pub enum Database {
    Sqlite(std::path::PathBuf, Settings),
    Turso(turso::Database, Settings),
}

impl Database {
    pub async fn create(path: &Path, settings: Settings) -> Result<Self> {
        settings.validate()?;
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|e| Error::Workload(format!("{}: {e}", path.display())))?;
        let db = match settings.engine {
            Engine::Sqlite => Self::Sqlite(path.into(), settings),
            Engine::Turso => {
                let db = turso::Builder::new_local(path.to_string_lossy().as_ref())
                    .with_io(&settings.io)
                    .experimental_mvcc_passive_checkpoint(settings.mvcc_passive_checkpoint)
                    .build()
                    .await?;
                Self::Turso(db, settings)
            }
        };
        let conn = db.connect().await?;
        let journal = match db.settings().journal {
            Journal::Wal => "wal",
            Journal::Mvcc => "mvcc",
        };
        let rows = conn
            .query(&format!("PRAGMA journal_mode = {journal}"), &[])
            .await?;
        if rows != vec![vec![Value::Text(journal.into())]] {
            return Err(Error::Workload(format!(
                "journal mode was not applied: {rows:?}"
            )));
        }
        if db.settings().journal == Journal::Mvcc {
            conn.execute(
                &format!(
                    "PRAGMA mvcc_checkpoint_threshold = {}",
                    db.settings().mvcc_checkpoint_bytes
                ),
                &[],
            )
            .await?;
        }
        Ok(db)
    }

    pub fn settings(&self) -> &Settings {
        match self {
            Self::Sqlite(_, s) | Self::Turso(_, s) => s,
        }
    }

    pub async fn connect(&self) -> Result<Connection> {
        let conn = match self {
            Self::Sqlite(path, _) => Connection::Sqlite(rusqlite::Connection::open(path)?),
            Self::Turso(db, _) => Connection::Turso(db.connect()?),
        };
        let settings = self.settings();
        let timeout = Duration::from_millis(settings.busy_timeout_ms);
        match &conn {
            Connection::Sqlite(c) => c.busy_timeout(timeout)?,
            Connection::Turso(c) => c.busy_timeout(timeout)?,
        }
        conn.execute("PRAGMA synchronous = FULL", &[]).await?;
        if settings.journal == Journal::Wal {
            conn.query(
                &format!(
                    "PRAGMA wal_autocheckpoint = {}",
                    settings.wal_autocheckpoint_pages
                ),
                &[],
            )
            .await?;
        }
        Ok(conn)
    }
}

pub enum Connection {
    Sqlite(rusqlite::Connection),
    Turso(turso::Connection),
}

impl Connection {
    pub async fn prepare(&self, sql: &str) -> Result<Statement<'_>> {
        Ok(match self {
            Self::Sqlite(c) => Statement::Sqlite(c.prepare(sql)?),
            Self::Turso(c) => Statement::Turso(c.prepare(sql).await?),
        })
    }

    pub async fn execute(&self, sql: &str, values: &[Value]) -> Result<u64> {
        self.prepare(sql).await?.execute(values).await
    }

    pub async fn query(&self, sql: &str, values: &[Value]) -> Result<Vec<Vec<Value>>> {
        let mut stmt = self.prepare(sql).await?;
        let mut rows = stmt.query(values).await?;
        let mut result = Vec::new();
        while let Some(row) = rows.next().await? {
            result.push(row);
        }
        Ok(result)
    }

    pub fn is_autocommit(&self) -> Result<bool> {
        match self {
            Self::Sqlite(c) => Ok(c.is_autocommit()),
            Self::Turso(c) => Ok(c.is_autocommit()?),
        }
    }
}

pub enum Statement<'a> {
    Sqlite(rusqlite::Statement<'a>),
    Turso(turso::Statement),
}

impl Statement<'_> {
    pub async fn execute(&mut self, values: &[Value]) -> Result<u64> {
        match self {
            Self::Sqlite(s) => {
                Ok(s.execute(rusqlite::params_from_iter(values.iter().map(sqlite_value)))? as u64)
            }
            Self::Turso(s) => Ok(s
                .execute(turso::params::Params::Positional(values.to_vec()))
                .await?),
        }
    }

    pub async fn query(&mut self, values: &[Value]) -> Result<Rows<'_>> {
        Ok(match self {
            Self::Sqlite(s) => {
                Rows::Sqlite(s.query(rusqlite::params_from_iter(values.iter().map(sqlite_value)))?)
            }
            Self::Turso(s) => Rows::Turso(
                s.query(turso::params::Params::Positional(values.to_vec()))
                    .await?,
            ),
        })
    }
}

pub enum Rows<'a> {
    Sqlite(rusqlite::Rows<'a>),
    Turso(turso::Rows),
}

impl Rows<'_> {
    pub async fn next(&mut self) -> Result<Option<Vec<Value>>> {
        match self {
            Self::Sqlite(rows) => rows
                .next()?
                .map(|row| {
                    (0..row.as_ref().column_count())
                        .map(|i| {
                            Ok(match row.get::<_, rusqlite::types::Value>(i)? {
                                rusqlite::types::Value::Null => Value::Null,
                                rusqlite::types::Value::Integer(v) => Value::Integer(v),
                                rusqlite::types::Value::Real(v) => Value::Real(v),
                                rusqlite::types::Value::Text(v) => Value::Text(v),
                                rusqlite::types::Value::Blob(v) => Value::Blob(v),
                            })
                        })
                        .collect()
                })
                .transpose(),
            Self::Turso(rows) => rows
                .next()
                .await?
                .map(|row| {
                    (0..row.column_count())
                        .map(|i| Ok(row.get_value(i)?))
                        .collect()
                })
                .transpose(),
        }
    }
}

fn sqlite_value(value: &Value) -> rusqlite::types::Value {
    match value {
        Value::Null => rusqlite::types::Value::Null,
        Value::Integer(v) => rusqlite::types::Value::Integer(*v),
        Value::Real(v) => rusqlite::types::Value::Real(*v),
        Value::Text(v) => rusqlite::types::Value::Text(v.clone()),
        Value::Blob(v) => rusqlite::types::Value::Blob(v.clone()),
    }
}
