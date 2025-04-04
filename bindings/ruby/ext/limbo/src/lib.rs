mod params;
mod value;

use magnus::value::ReprValue;
use params::Params;
use value::Value;

use magnus::{function, method, IntoValue, Module, Object, Ruby, TryConvert, Value as RValue};

use std::num::NonZero;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

// // Converts an async function into a synchronous function using Tokio's runtime.
// fn block_on_async<F, T>(future: F) -> T
// where
//     F: std::future::Future<Output = T>,
// {
//     let rt = Runtime::new().expect("Failed to create Tokio runtime");
//     rt.block_on(future)
// }

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("ruby value conversion failure: `{0}`")]
    ConversionFailure(RValue),
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
impl magnus::error::IntoError for Error {
    fn into_error(self, ruby: &Ruby) -> magnus::error::Error {
        magnus::error::Error::new(ruby.exception_standard_error(), self.to_string())
    }
}
type BoxError = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, Error>;

#[magnus::wrap(class = "Limbo::Builder")]
struct Builder {
    path: String,
}
unsafe impl Send for Builder {}

impl Builder {
    fn new(path: String) -> Self {
        Self { path }
    }

    #[allow(unused_variables, clippy::arc_with_non_send_sync)]
    fn build(&self) -> Result<Database> {
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

#[magnus::wrap(class = "Limbo::Database")]
struct Database {
    inner: Arc<limbo_core::Database>,
}
unsafe impl Send for Database {}
impl Database {
    fn connect(&self) -> Result<Connection> {
        let conn = self.inner.connect()?;
        #[allow(clippy::arc_with_non_send_sync)]
        let connection = Connection {
            inner: Arc::new(Mutex::new(conn)),
        };
        Ok(connection)
    }
}

#[magnus::wrap(class = "Limbo::Connection")]
struct Connection {
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
    fn query(&self, sql: String, params_: RValue) -> Result<Rows> {
        let stmt = self.prepare(sql)?;
        stmt.query(params_)
    }

    fn execute(&self, sql: String, params: Params) -> Result<u64> {
        let stmt = self.prepare(sql)?;
        stmt.execute(params)
    }

    fn prepare(&self, sql: String) -> Result<Statement> {
        let conn = self
            .inner
            .lock()
            .map_err(|e| Error::MutexError(e.to_string()))?;

        let stmt = conn.prepare(&sql)?;

        #[allow(clippy::arc_with_non_send_sync)]
        let statement = Statement {
            inner: Arc::new(Mutex::new(stmt)),
        };
        Ok(statement)
    }
}

#[magnus::wrap(class = "Limbo::Statement")]
struct Statement {
    inner: Arc<Mutex<limbo_core::Statement>>,
}
unsafe impl Send for Statement {}
unsafe impl Sync for Statement {}

impl Statement {
    fn query(&self, params: RValue) -> Result<Rows> {
        let params: Params =
            Params::try_convert(params).map_err(|_| Error::ConversionFailure(params))?;
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

    fn execute(&self, params: Params) -> Result<u64> {
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
}

#[magnus::wrap(class = "Limbo::Rows")]
struct Rows {
    inner: Arc<Mutex<limbo_core::Statement>>,
}

unsafe impl Send for Rows {}
unsafe impl Sync for Rows {}

impl Rows {
    fn next(&self) -> Result<Option<Row>> {
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

#[magnus::wrap(class = "Limbo::Row")]
struct Row {
    values: Vec<limbo_core::OwnedValue>,
}

unsafe impl Send for Row {}
unsafe impl Sync for Row {}

impl Row {
    fn get_value(ruby: &Ruby, rb_self: &Self, index: usize) -> Result<RValue> {
        if index >= rb_self.values.len() {
            return Ok(ruby.qnil().as_value());
        }
        let value = &rb_self.values[index];
        match value {
            limbo_core::OwnedValue::Integer(i) => Ok((*i).into_value_with(ruby)),
            limbo_core::OwnedValue::Null => Ok(ruby.qnil().as_value()),
            limbo_core::OwnedValue::Float(f) => Ok((*f).into_value_with(ruby)),
            limbo_core::OwnedValue::Text(text) => Ok((*text).to_string().into_value_with(ruby)),
            limbo_core::OwnedValue::Blob(items) => Ok((*items).to_vec().into_value_with(ruby)),
        }
    }
}

#[magnus::init(name = "limbo")]
fn init(ruby: &Ruby) -> std::result::Result<(), magnus::Error> {
    let module = ruby.define_module("Limbo")?;
    module.const_set("CORE_VERSION", "0.0.14")?;

    let builder_class = module.define_class("Builder", ruby.class_object())?;
    builder_class.define_singleton_method("new", function!(Builder::new, 1))?;
    builder_class.define_method("build", method!(Builder::build, 0))?;

    let database_class = module.define_class("Database", ruby.class_object())?;
    database_class.define_method("connect", method!(Database::connect, 0))?;

    let connection_class = module.define_class("Connection", ruby.class_object())?;
    connection_class.define_method("query", method!(Connection::query, 2))?;
    connection_class.define_method("execute", method!(Connection::execute, 2))?;
    connection_class.define_method("prepare", method!(Connection::prepare, 1))?;

    let statement_class = module.define_class("Statement", ruby.class_object())?;
    statement_class.define_method("query", method!(Statement::query, 1))?;
    statement_class.define_method("execute", method!(Statement::execute, 1))?;

    let rows_class = module.define_class("Rows", ruby.class_object())?;
    rows_class.define_method("next", method!(Rows::next, 0))?;

    let row_class = module.define_class("Row", ruby.class_object())?;
    row_class.define_method("get_value", method!(Row::get_value, 1))?;

    Ok(())
}
