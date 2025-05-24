pub mod params;
pub mod value;

pub use value::Value;

pub use params::params_from_iter;

use crate::params::*;
use std::fmt::Debug;
use std::num::NonZero;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("SQL conversion failure: `{0}`")]
    ToSqlConversionFailure(BoxError),
    #[error("Mutex lock error: {0}")]
    MutexError(String),
    #[error("SQL execution failure: `{0}`")]
    SqlExecutionFailure(String),
}

impl From<limbo_core::LimboError> for Error {
    fn from(err: limbo_core::LimboError) -> Self {
        Error::SqlExecutionFailure(err.to_string())
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
                let db = limbo_core::Database::open_file(io, self.path.as_str(), false)?;
                Ok(Database { inner: db })
            }
            path => {
                let io: Arc<dyn limbo_core::IO> = Arc::new(limbo_core::PlatformIO::new()?);
                let db = limbo_core::Database::open_file(io, path, false)?;
                Ok(Database { inner: db })
            }
        }
    }
}

#[derive(Clone)]
pub struct Database {
    inner: Arc<limbo_core::Database>,
}

unsafe impl Send for Database {}
unsafe impl Sync for Database {}

impl Debug for Database {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Database").finish()
    }
}

impl Database {
    pub fn connect(&self) -> Result<Connection> {
        let conn = self.inner.connect()?;
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

    pub fn pragma_query<F>(&self, pragma_name: &str, mut f: F) -> Result<()>
    where
        F: FnMut(&Row) -> limbo_core::Result<()>,
    {
        let conn = self
            .inner
            .lock()
            .map_err(|e| Error::MutexError(e.to_string()))?;

        let rows: Vec<Row> = conn
            .pragma_query(pragma_name)
            .map_err(|e| Error::SqlExecutionFailure(e.to_string()))?
            .iter()
            .map(|row| row.iter().collect::<Row>())
            .collect();

        rows.iter().try_for_each(|row| {
            f(row).map_err(|e| {
                Error::SqlExecutionFailure(format!("Error executing user defined function: {}", e))
            })
        })?;
        Ok(())
    }
}

pub struct Statement {
    inner: Arc<Mutex<limbo_core::Statement>>,
}

impl Clone for Statement {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

unsafe impl Send for Statement {}
unsafe impl Sync for Statement {}

impl Statement {
    pub async fn query(&mut self, params: impl IntoParams) -> Result<Rows> {
        let params = params.into_params()?;
        match params {
            params::Params::None => (),
            params::Params::Positional(values) => {
                for (i, value) in values.into_iter().enumerate() {
                    let mut stmt = self.inner.lock().unwrap();
                    stmt.bind_at(NonZero::new(i + 1).unwrap(), value.into());
                }
            }
            params::Params::Named(_items) => todo!(),
        }
        #[allow(clippy::arc_with_non_send_sync)]
        let rows = Rows {
            inner: Arc::clone(&self.inner),
        };
        Ok(rows)
    }

    pub async fn execute(&mut self, params: impl IntoParams) -> Result<u64> {
        {
            // Reset the statement before executing
            self.inner.lock().unwrap().reset();
        }
        let params = params.into_params()?;
        match params {
            params::Params::None => (),
            params::Params::Positional(values) => {
                for (i, value) in values.into_iter().enumerate() {
                    let mut stmt = self.inner.lock().unwrap();
                    stmt.bind_at(NonZero::new(i + 1).unwrap(), value.into());
                }
            }
            params::Params::Named(_items) => todo!(),
        }
        loop {
            let mut stmt = self.inner.lock().unwrap();
            match stmt.step() {
                Ok(limbo_core::StepResult::Row) => {
                    // unexpected row during execution, error out.
                    return Ok(2);
                }
                Ok(limbo_core::StepResult::Done) => {
                    return Ok(0);
                }
                Ok(limbo_core::StepResult::IO) => {
                    let _ = stmt.run_once();
                    //return Ok(1);
                }
                Ok(limbo_core::StepResult::Busy) => {
                    return Ok(4);
                }
                Ok(limbo_core::StepResult::Interrupt) => {
                    return Ok(3);
                }
                Err(err) => {
                    return Err(err.into());
                }
            }
        }
    }

    pub fn columns(&self) -> Vec<Column> {
        let stmt = self.inner.lock().unwrap();

        let n = stmt.num_columns();

        let mut cols = Vec::with_capacity(n);

        for i in 0..n {
            let name = stmt.get_column_name(i).into_owned();
            cols.push(Column {
                name,
                decl_type: None, // TODO
            });
        }

        cols
    }
}

pub struct Column {
    name: String,
    decl_type: Option<String>,
}

impl Column {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn decl_type(&self) -> Option<&str> {
        self.decl_type.as_deref()
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

impl Clone for Rows {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
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
                    values: row.get_values().map(|v| v.to_owned()).collect(),
                }))
            }
            _ => Ok(None),
        }
    }
}

#[derive(Debug)]
pub struct Row {
    values: Vec<limbo_core::Value>,
}

unsafe impl Send for Row {}
unsafe impl Sync for Row {}

impl Row {
    pub fn get_value(&self, index: usize) -> Result<Value> {
        let value = &self.values[index];
        match value {
            limbo_core::Value::Integer(i) => Ok(Value::Integer(*i)),
            limbo_core::Value::Null => Ok(Value::Null),
            limbo_core::Value::Float(f) => Ok(Value::Real(*f)),
            limbo_core::Value::Text(text) => Ok(Value::Text(text.to_string())),
            limbo_core::Value::Blob(items) => Ok(Value::Blob(items.to_vec())),
        }
    }

    pub fn column_count(&self) -> usize {
        self.values.len()
    }
}

impl<'a> FromIterator<&'a limbo_core::Value> for Row {
    fn from_iter<T: IntoIterator<Item = &'a limbo_core::Value>>(iter: T) -> Self {
        let values = iter
            .into_iter()
            .map(|v| match v {
                limbo_core::Value::Integer(i) => limbo_core::Value::Integer(*i),
                limbo_core::Value::Null => limbo_core::Value::Null,
                limbo_core::Value::Float(f) => limbo_core::Value::Float(*f),
                limbo_core::Value::Text(s) => limbo_core::Value::Text(s.clone()),
                limbo_core::Value::Blob(b) => limbo_core::Value::Blob(b.clone()),
            })
            .collect();

        Row { values }
    }
}
