pub mod params;
pub mod value;

pub use value::Value;

pub use params::params_from_iter;

use crate::params::*;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("SQL conversion failure: `{0}`")]
    ToSqlConversionFailure(BoxError),
    #[error("Mutex lock error: {0}")]
    MutexError(String),
}

impl From<limbo_core::LimboError> for Error {
    fn from(_err: limbo_core::LimboError) -> Self {
        todo!();
    }
}

pub(crate) type BoxError = Box<dyn std::error::Error + Send + Sync>;

pub type Result<T> = std::result::Result<T, Error>;
pub struct Builder {
    path: String,
}

impl Builder {
    pub fn new_local(path: &str) -> Self {
        Self {
            path: path.to_string(),
        }
    }

    #[allow(unused_variables, clippy::arc_with_non_send_sync)]
    pub async fn build(self) -> Result<Database> {
        match self.path.as_str() {
            ":memory:" => {
                let io: Arc<dyn limbo_core::IO> = Arc::new(limbo_core::MemoryIO::new());
                let db = limbo_core::Database::open_file(io, self.path.as_str())?;
                Ok(Database { inner: db })
            }
            _ => todo!(),
        }
    }
}

pub struct Database {
    inner: Arc<limbo_core::Database>,
}

unsafe impl Send for Database {}
unsafe impl Sync for Database {}

impl Database {
    pub fn connect(&self) -> Result<Connection> {
        let conn = self.inner.connect().unwrap();
        #[allow(clippy::arc_with_non_send_sync)]
        let connection = Connection {
            inner: Arc::new(Mutex::new(conn)),
        };
        Ok(connection)
    }
}

pub struct Connection {
    inner: Arc<Mutex<Rc<limbo_core::Connection>>>,
}

impl Clone for Connection {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

unsafe impl Send for Connection {}
unsafe impl Sync for Connection {}

impl Connection {
    pub async fn query(&self, sql: &str, params: impl IntoParams) -> Result<Rows> {
        let mut stmt = self.prepare(sql).await?;
        stmt.query(params).await
    }

    pub async fn execute(&self, sql: &str, params: impl IntoParams) -> Result<u64> {
        let mut stmt = self.prepare(sql).await?;
        stmt.execute(params).await
    }

    pub async fn prepare(&self, sql: &str) -> Result<Statement> {
        let conn = self
            .inner
            .lock()
            .map_err(|e| Error::MutexError(e.to_string()))?;

        let stmt = conn.prepare(sql)?;

        #[allow(clippy::arc_with_non_send_sync)]
        let statement = Statement {
            inner: Arc::new(Mutex::new(stmt)),
        };
        Ok(statement)
    }
}

pub struct Statement {
    inner: Arc<Mutex<limbo_core::Statement>>,
}

unsafe impl Send for Statement {}
unsafe impl Sync for Statement {}

impl Statement {
    pub async fn query(&mut self, params: impl IntoParams) -> Result<Rows> {
        let params = params.into_params()?;
        match params {
            crate::params::Params::None => {}
            _ => todo!(),
        }
        #[allow(clippy::arc_with_non_send_sync)]
        let rows = Rows {
            inner: Arc::clone(&self.inner),
        };
        Ok(rows)
    }

    pub async fn execute(&mut self, params: impl IntoParams) -> Result<u64> {
        let _params = params.into_params()?;
        todo!();
    }
}

pub trait IntoValue {
    fn into_value(self) -> Result<Value>;
}

#[derive(Debug, Clone)]
pub enum Params {
    None,
    Positional(Vec<Value>),
    Named(Vec<(String, Value)>),
}
pub struct Transaction {}

pub struct Rows {
    inner: Arc<Mutex<limbo_core::Statement>>,
}

unsafe impl Send for Rows {}
unsafe impl Sync for Rows {}

impl Rows {
    pub async fn next(&mut self) -> Result<Option<Row>> {
        let mut stmt = self
            .inner
            .lock()
            .map_err(|e| Error::MutexError(e.to_string()))?;

        match stmt.step() {
            Ok(limbo_core::StepResult::Row) => {
                let row = stmt.row().unwrap();
                Ok(Some(Row {
                    values: row.get_values().to_vec(),
                }))
            }
            _ => Ok(None),
        }
    }
}

pub struct Row {
    values: Vec<limbo_core::OwnedValue>,
}

unsafe impl Send for Row {}
unsafe impl Sync for Row {}

impl Row {
    pub fn get_value(&self, index: usize) -> Result<Value> {
        let value = &self.values[index];
        match value {
            limbo_core::OwnedValue::Integer(i) => Ok(Value::Integer(*i)),
            _ => todo!(),
        }
    }
}
