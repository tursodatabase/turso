use magnus::{function, method, Module, Object, Ruby};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("SQL conversion failure: `{0}`")]
    ToSqlConversionFailure(BoxError),
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
pub struct Builder {
    path: String,
}
unsafe impl Send for Builder {}

impl Builder {
    fn new(path: String) -> Self {
        Self { path }
    }
    fn build(&self) -> Result<Database> {
        let io: Arc<dyn limbo_core::IO> = match self.path.as_str() {
            ":memory:" => Arc::new(limbo_core::MemoryIO::new()?),
            _ => Arc::new(limbo_core::PlatformIO::new()?),
        };
        let inner = limbo_core::Database::open_file(io.clone(), &self.path.as_str())?;
        Ok(Database { inner, io })
    }
}

#[magnus::wrap(class = "Limbo::Database")]
pub struct Database {
    inner: Arc<limbo_core::Database>,
    io: Arc<dyn limbo_core::IO>,
}
unsafe impl Send for Database {}
impl Database {
    fn connect(&self) -> Connection {
        let inner = self.inner.connect();
        Connection { inner, io: self.io.clone() }
    }
}

#[magnus::wrap(class = "Limbo::Connection")]
struct Connection {
    inner: Rc<limbo_core::Connection>,
    io: Arc<dyn limbo_core::IO>,
}
unsafe impl Send for Connection {}
impl Connection {
    fn prepare(&self, sql: &str) -> Result<Statement> {
        let stmt = self.inner.prepare(sql)?;
        Ok(Statement {
            inner: Rc::new(RefCell::new(stmt)),
            io: self.io.clone(),
        })
    }
}

#[magnus::wrap(class = "Limbo::Statement")]
struct Statement {
    inner: Rc<RefCell<limbo_core::Statement>>,
    io: Arc<dyn limbo_core::IO>,
}
unsafe impl Send for Statement {}
impl Statement {
    // fn bind(&mut self, index: NonZero<usize>, value: limbo_core::Value<'_>) {
    //     self.inner.bind_at(index, value);
    // }
    // fn query(&mut self) -> Result<Option<&limbo_core::types::Row>> {
    //     loop {
    //         match self.inner.borrow_mut().step().unwrap() {
    //             limbo_core::StepResult::IO => {
    //                self.io.run_once().unwrap();
    //             }
    //             limbo_core::StepResult::Row => {
    //                 return Ok(self.inner.borrow().row());
    //             }
    //             limbo_core::StepResult::Interrupt => return Ok(None),
    //             limbo_core::StepResult::Done => return Ok(None),
    //             limbo_core::StepResult::Busy => return Ok(None),
    //         }
    //     }
    // }
}

/// Converts an async function into a synchronous function using Tokio's runtime.
// fn block_on_async<F, T>(future: F) -> T
// where
//     F: std::future::Future<Output = T>,
// {
//     let rt = Runtime::new().expect("Failed to create Tokio runtime");
//     rt.block_on(future)
// }

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
    // connection_class.define_method("prepare", method!(Connection::prepare, 1))?;

    let statement_class = module.define_class("Statement", ruby.class_object())?;
    // statement_class.define_method("query", method!(Statement::query, 1))?;
    Ok(())
}
