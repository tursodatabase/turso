//! JavaScript bindings for the Turso library.
//!
//! These bindings provide a thin layer that exposes Turso's Rust API to JavaScript,
//! maintaining close alignment with the underlying implementation while offering
//! the following core database operations:
//!
//! - Opening and closing database connections
//! - Preparing SQL statements
//! - Binding parameters to prepared statements
//! - Iterating through query results
//! - Managing the I/O event loop

use napi::bindgen_prelude::*;
use napi::{Env, Task};
use napi_derive::napi;
use std::{
    cell::{Cell, RefCell},
    num::NonZeroUsize,
    sync::Arc,
};
use turso_core::IOBuilder;

/// Step result constants
const STEP_ROW: u32 = 1;
const STEP_DONE: u32 = 2;
const STEP_IO: u32 = 3;

/// The presentation mode for rows.
#[derive(Debug, Clone)]
enum PresentationMode {
    Expanded,
    Raw,
    Pluck,
}

/// A database connection.
#[napi]
#[derive(Clone)]
pub struct Database {
    _db: Option<Arc<turso_core::Database>>,
    io: Arc<dyn turso_core::IO>,
    conn: Arc<turso_core::Connection>,
    is_memory: bool,
    is_open: Cell<bool>,
    default_safe_integers: Cell<bool>,
}

#[napi]
impl Database {
    /// Creates a new database instance.
    ///
    /// # Arguments
    /// * `path` - The path to the database file.
    #[napi(constructor)]
    pub fn new(path: String) -> Result<Self> {
        let is_memory = path == ":memory:";
        let io: Arc<dyn turso_core::IO> = if is_memory {
            Arc::new(turso_core::MemoryIO::new())
        } else {
            Arc::new(turso_core::PlatformIO::new().map_err(|e| {
                Error::new(Status::GenericFailure, format!("Failed to create IO: {e}"))
            })?)
        };

        let file = io
            .open_file(&path, turso_core::OpenFlags::Create, false)
            .map_err(|e| Error::new(Status::GenericFailure, format!("Failed to open file: {e}")))?;

        let db_file = Arc::new(DatabaseFile::new(file));
        let db =
            turso_core::Database::open(io.clone(), &path, db_file, false, true).map_err(|e| {
                Error::new(
                    Status::GenericFailure,
                    format!("Failed to open database: {e}"),
                )
            })?;

        let conn = db
            .connect()
            .map_err(|e| Error::new(Status::GenericFailure, format!("Failed to connect: {e}")))?;

        Ok(Self::create(Some(db), io, conn, is_memory))
    }

    pub fn create(
        db: Option<Arc<turso_core::Database>>,
        io: Arc<dyn turso_core::IO>,
        conn: Arc<turso_core::Connection>,
        is_memory: bool,
    ) -> Self {
        Database {
            _db: db,
            io,
            conn,
            is_memory,
            is_open: Cell::new(true),
            default_safe_integers: Cell::new(false),
        }
    }

    /// Returns whether the database is in memory-only mode.
    #[napi(getter)]
    pub fn memory(&self) -> bool {
        self.is_memory
    }

    /// Returns whether the database connection is open.
    #[napi(getter)]
    pub fn open(&self) -> bool {
        self.is_open.get()
    }

    /// Executes a batch of SQL statements.
    ///
    /// # Arguments
    ///
    /// * `sql` - The SQL statements to execute.
    ///
    /// # Returns
    #[napi]
    pub fn batch(&self, sql: String) -> Result<()> {
        self.conn.prepare_execute_batch(&sql).map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Failed to execute batch: {e}"),
            )
        })?;
        Ok(())
    }

    /// Prepares a statement for execution.
    ///
    /// # Arguments
    ///
    /// * `sql` - The SQL statement to prepare.
    ///
    /// # Returns
    ///
    /// A `Statement` instance.
    #[napi]
    pub fn prepare(&self, sql: String) -> Result<Statement> {
        let stmt = self
            .conn
            .prepare(&sql)
            .map_err(|e| Error::new(Status::GenericFailure, format!("{e}")))?;
        let column_names: Vec<std::ffi::CString> = (0..stmt.num_columns())
            .map(|i| std::ffi::CString::new(stmt.get_column_name(i).to_string()).unwrap())
            .collect();
        Ok(Statement {
            stmt: RefCell::new(Some(stmt)),
            column_names,
            mode: RefCell::new(PresentationMode::Expanded),
            safe_integers: Cell::new(self.default_safe_integers.get()),
        })
    }

    /// Returns the rowid of the last row inserted.
    ///
    /// # Returns
    ///
    /// The rowid of the last row inserted.
    #[napi]
    pub fn last_insert_rowid(&self) -> Result<i64> {
        Ok(self.conn.last_insert_rowid())
    }

    /// Returns the number of changes made by the last statement.
    ///
    /// # Returns
    ///
    /// The number of changes made by the last statement.
    #[napi]
    pub fn changes(&self) -> Result<i64> {
        Ok(self.conn.changes())
    }

    /// Returns the total number of changes made by all statements.
    ///
    /// # Returns
    ///
    /// The total number of changes made by all statements.
    #[napi]
    pub fn total_changes(&self) -> Result<i64> {
        Ok(self.conn.total_changes())
    }

    /// Closes the database connection.
    ///
    /// # Returns
    ///
    /// `Ok(())` if the database is closed successfully.
    #[napi]
    pub fn close(&self) -> Result<()> {
        self.is_open.set(false);
        // Database close is handled automatically when dropped
        Ok(())
    }

    /// Sets the default safe integers mode for all statements from this database.
    ///
    /// # Arguments
    ///
    /// * `toggle` - Whether to use safe integers by default.
    #[napi(js_name = "defaultSafeIntegers")]
    pub fn default_safe_integers(&self, toggle: Option<bool>) {
        self.default_safe_integers.set(toggle.unwrap_or(true));
    }

    /// Runs the I/O loop synchronously.
    #[napi]
    pub fn io_loop_sync(&self) -> Result<()> {
        self.io
            .run_once()
            .map_err(|e| Error::new(Status::GenericFailure, format!("IO error: {e}")))?;
        Ok(())
    }

    /// Runs the I/O loop asynchronously, returning a Promise.
    #[napi(ts_return_type = "Promise<void>")]
    pub fn io_loop_async(&self) -> AsyncTask<IoLoopTask> {
        let io = self.io.clone();
        AsyncTask::new(IoLoopTask { io })
    }
}

/// A prepared statement.
#[napi]
pub struct Statement {
    stmt: RefCell<Option<turso_core::Statement>>,
    column_names: Vec<std::ffi::CString>,
    mode: RefCell<PresentationMode>,
    safe_integers: Cell<bool>,
}

#[napi]
impl Statement {
    #[napi]
    pub fn reset(&self) -> Result<()> {
        let mut stmt = self.stmt.borrow_mut();
        let stmt = stmt
            .as_mut()
            .ok_or_else(|| Error::new(Status::GenericFailure, "Statement has been finalized"))?;
        stmt.reset();
        Ok(())
    }

    /// Returns the number of parameters in the statement.
    #[napi]
    pub fn parameter_count(&self) -> Result<u32> {
        let stmt = self.stmt.borrow();
        let stmt = stmt
            .as_ref()
            .ok_or_else(|| Error::new(Status::GenericFailure, "Statement has been finalized"))?;
        Ok(stmt.parameters_count() as u32)
    }

    /// Returns the name of a parameter at a specific 1-based index.
    ///
    /// # Arguments
    ///
    /// * `index` - The 1-based parameter index.
    #[napi]
    pub fn parameter_name(&self, index: u32) -> Result<Option<String>> {
        let stmt = self.stmt.borrow();
        let stmt = stmt
            .as_ref()
            .ok_or_else(|| Error::new(Status::GenericFailure, "Statement has been finalized"))?;

        let non_zero_idx = NonZeroUsize::new(index as usize).ok_or_else(|| {
            Error::new(Status::InvalidArg, "Parameter index must be greater than 0")
        })?;

        Ok(stmt.parameters().name(non_zero_idx).map(|s| s.to_string()))
    }

    /// Binds a parameter at a specific 1-based index with explicit type.
    ///
    /// # Arguments
    ///
    /// * `index` - The 1-based parameter index.
    /// * `value_type` - The type constant (0=null, 1=int, 2=float, 3=text, 4=blob).
    /// * `value` - The value to bind.
    #[napi]
    pub fn bind_at(&self, index: u32, value: Unknown) -> Result<()> {
        let mut stmt = self.stmt.borrow_mut();
        let stmt = stmt
            .as_mut()
            .ok_or_else(|| Error::new(Status::GenericFailure, "Statement has been finalized"))?;

        let non_zero_idx = NonZeroUsize::new(index as usize).ok_or_else(|| {
            Error::new(Status::InvalidArg, "Parameter index must be greater than 0")
        })?;
        let value_type = value.get_type()?;
        let turso_value = match value_type {
            ValueType::Null => turso_core::Value::Null,
            ValueType::Number => {
                let n: f64 = unsafe { value.cast()? };
                if n.fract() == 0.0 && n >= i64::MIN as f64 && n <= i64::MAX as f64 {
                    turso_core::Value::Integer(n as i64)
                } else {
                    turso_core::Value::Float(n)
                }
            }
            ValueType::BigInt => {
                let bigint_str = value.coerce_to_string()?.into_utf8()?.as_str()?.to_owned();
                let bigint_value = bigint_str.parse::<i64>().map_err(|e| {
                    Error::new(
                        Status::NumberExpected,
                        format!("Failed to parse BigInt: {e}"),
                    )
                })?;
                turso_core::Value::Integer(bigint_value)
            }
            ValueType::String => {
                let s = value.coerce_to_string()?.into_utf8()?;
                turso_core::Value::Text(s.as_str()?.to_owned().into())
            }
            ValueType::Boolean => {
                let b: bool = unsafe { value.cast()? };
                turso_core::Value::Integer(if b { 1 } else { 0 })
            }
            ValueType::Object => {
                let obj = value.coerce_to_object()?;

                if obj.is_buffer()? || obj.is_typedarray()? {
                    let length = obj.get_named_property::<u32>("length")?;
                    let mut bytes = Vec::with_capacity(length as usize);
                    for i in 0..length {
                        let byte = obj.get_element::<u32>(i)?;
                        bytes.push(byte as u8);
                    }
                    turso_core::Value::Blob(bytes)
                } else {
                    let s = value.coerce_to_string()?.into_utf8()?;
                    turso_core::Value::Text(s.as_str()?.to_owned().into())
                }
            }
            _ => {
                let s = value.coerce_to_string()?.into_utf8()?;
                turso_core::Value::Text(s.as_str()?.to_owned().into())
            }
        };

        stmt.bind_at(non_zero_idx, turso_value);
        Ok(())
    }

    /// Step the statement and return result code:
    /// 1 = Row available, 2 = Done, 3 = I/O needed
    #[napi]
    pub fn step(&self) -> Result<u32> {
        let mut stmt_ref = self.stmt.borrow_mut();
        let stmt = stmt_ref
            .as_mut()
            .ok_or_else(|| Error::new(Status::GenericFailure, "Statement has been finalized"))?;

        match stmt.step() {
            Ok(turso_core::StepResult::Row) => Ok(STEP_ROW),
            Ok(turso_core::StepResult::Done) => Ok(STEP_DONE),
            Ok(turso_core::StepResult::IO) => Ok(STEP_IO),
            Ok(turso_core::StepResult::Interrupt) => Err(Error::new(
                Status::GenericFailure,
                "Statement was interrupted",
            )),
            Ok(turso_core::StepResult::Busy) => {
                Err(Error::new(Status::GenericFailure, "Database is busy"))
            }
            Err(e) => Err(Error::new(
                Status::GenericFailure,
                format!("Step failed: {e}"),
            )),
        }
    }

    /// Get the current row data according to the presentation mode
    #[napi]
    pub fn row<'env>(&self, env: &'env Env) -> Result<Unknown<'env>> {
        let stmt_ref = self.stmt.borrow();
        let stmt = stmt_ref
            .as_ref()
            .ok_or_else(|| Error::new(Status::GenericFailure, "Statement has been finalized"))?;

        let row_data = stmt
            .row()
            .ok_or_else(|| Error::new(Status::GenericFailure, "No row data available"))?;

        let mode = self.mode.borrow();
        let safe_integers = self.safe_integers.get();
        let row_value = match *mode {
            PresentationMode::Raw => {
                let mut raw_array = env.create_array(row_data.len() as u32)?;
                for (idx, value) in row_data.get_values().enumerate() {
                    let js_value = to_js_value(env, value, safe_integers)?;
                    raw_array.set(idx as u32, js_value)?;
                }
                raw_array.coerce_to_object()?.to_unknown()
            }
            PresentationMode::Pluck => {
                let (_, value) =
                    row_data
                        .get_values()
                        .enumerate()
                        .next()
                        .ok_or(napi::Error::new(
                            napi::Status::GenericFailure,
                            "Pluck mode requires at least one column in the result",
                        ))?;
                to_js_value(env, value, safe_integers)?
            }
            PresentationMode::Expanded => {
                let row = Object::new(env)?;
                let raw_row = row.raw();
                let raw_env = env.raw();
                for idx in 0..row_data.len() {
                    let value = row_data.get_value(idx);
                    let column_name = &self.column_names[idx];
                    let js_value = to_js_value(env, value, safe_integers)?;
                    unsafe {
                        napi::sys::napi_set_named_property(
                            raw_env,
                            raw_row,
                            column_name.as_ptr(),
                            js_value.raw(),
                        );
                    }
                }
                row.to_unknown()
            }
        };

        Ok(row_value)
    }

    /// Sets the presentation mode to raw.
    #[napi]
    pub fn raw(&mut self, raw: Option<bool>) {
        self.mode = RefCell::new(match raw {
            Some(false) => PresentationMode::Expanded,
            _ => PresentationMode::Raw,
        });
    }

    /// Sets the presentation mode to pluck.
    #[napi]
    pub fn pluck(&mut self, pluck: Option<bool>) {
        self.mode = RefCell::new(match pluck {
            Some(false) => PresentationMode::Expanded,
            _ => PresentationMode::Pluck,
        });
    }

    /// Sets safe integers mode for this statement.
    ///
    /// # Arguments
    ///
    /// * `toggle` - Whether to use safe integers.
    #[napi(js_name = "safeIntegers")]
    pub fn safe_integers(&self, toggle: Option<bool>) {
        self.safe_integers.set(toggle.unwrap_or(true));
    }

    /// Get column information for the statement
    #[napi]
    pub fn columns<'env>(&self, env: &'env Env) -> Result<Array<'env>> {
        let stmt_ref = self.stmt.borrow();
        let stmt = stmt_ref
            .as_ref()
            .ok_or_else(|| Error::new(Status::GenericFailure, "Statement has been finalized"))?;

        let column_count = stmt.num_columns();
        let mut js_array = env.create_array(column_count as u32)?;

        for i in 0..column_count {
            let mut js_obj = Object::new(env)?;
            let column_name = stmt.get_column_name(i);
            let column_type = stmt.get_column_type(i);

            // Set the name property
            js_obj.set("name", column_name.as_ref())?;

            // Set type property if available
            match column_type {
                Some(type_str) => js_obj.set("type", type_str.as_str())?,
                None => js_obj.set("type", ToNapiValue::into_unknown(Null, env)?)?,
            }

            // For now, set other properties to null since turso_core doesn't provide this metadata
            js_obj.set("column", ToNapiValue::into_unknown(Null, env)?)?;
            js_obj.set("table", ToNapiValue::into_unknown(Null, env)?)?;
            js_obj.set("database", ToNapiValue::into_unknown(Null, env)?)?;

            js_array.set(i as u32, js_obj)?;
        }

        Ok(js_array)
    }

    /// Finalizes the statement.
    #[napi]
    pub fn finalize(&self) -> Result<()> {
        self.stmt.borrow_mut().take();
        Ok(())
    }
}

/// Async task for running the I/O loop.
pub struct IoLoopTask {
    // this field is set in the turso-sync-engine package
    pub io: Arc<dyn turso_core::IO>,
}

impl Task for IoLoopTask {
    type Output = ();
    type JsValue = ();

    fn compute(&mut self) -> napi::Result<Self::Output> {
        self.io.run_once().map_err(|e| {
            napi::Error::new(napi::Status::GenericFailure, format!("IO error: {e}"))
        })?;
        Ok(())
    }

    fn resolve(&mut self, _env: Env, _output: Self::Output) -> napi::Result<Self::JsValue> {
        Ok(())
    }
}

/// Convert a Turso value to a JavaScript value.
fn to_js_value<'a>(
    env: &'a napi::Env,
    value: &turso_core::Value,
    safe_integers: bool,
) -> napi::Result<Unknown<'a>> {
    match value {
        turso_core::Value::Null => ToNapiValue::into_unknown(Null, env),
        turso_core::Value::Integer(i) => {
            if safe_integers {
                let bigint = BigInt::from(*i);
                ToNapiValue::into_unknown(bigint, env)
            } else {
                ToNapiValue::into_unknown(*i as f64, env)
            }
        }
        turso_core::Value::Float(f) => ToNapiValue::into_unknown(*f, env),
        turso_core::Value::Text(s) => ToNapiValue::into_unknown(s.as_str(), env),
        turso_core::Value::Blob(b) => {
            let buffer = Buffer::from(b.as_slice());
            ToNapiValue::into_unknown(buffer, env)
        }
    }
}

struct DatabaseFile {
    file: Arc<dyn turso_core::File>,
}

unsafe impl Send for DatabaseFile {}
unsafe impl Sync for DatabaseFile {}

impl DatabaseFile {
    pub fn new(file: Arc<dyn turso_core::File>) -> Self {
        Self { file }
    }
}

impl turso_core::DatabaseStorage for DatabaseFile {
    fn read_header(
        &self,
        c: turso_core::Completion,
    ) -> turso_core::Result<turso_core::CompletionBuilder> {
        Ok(IOBuilder::pread(self.file.clone(), 0, c))
    }
    fn read_page(
        &self,
        page_idx: usize,
        _io_ctx: &turso_core::IOContext,
        c: turso_core::Completion,
    ) -> turso_core::Result<turso_core::CompletionBuilder> {
        let r = c.as_read();
        let size = r.buf().len();
        assert!(page_idx > 0);
        if !(512..=65536).contains(&size) || size & (size - 1) != 0 {
            return Err(turso_core::LimboError::NotADB);
        }
        let pos = (page_idx as u64 - 1) * size as u64;
        Ok(IOBuilder::pread(self.file.clone(), pos, c))
    }

    fn write_page(
        &self,
        page_idx: usize,
        buffer: Arc<turso_core::Buffer>,
        _io_ctx: &turso_core::IOContext,
        c: turso_core::Completion,
    ) -> turso_core::Result<turso_core::CompletionBuilder> {
        let size = buffer.len();
        let pos = (page_idx as u64 - 1) * size as u64;
        Ok(IOBuilder::pwrite(self.file.clone(), pos, buffer, c))
    }

    fn write_pages(
        &self,
        first_page_idx: usize,
        page_size: usize,
        buffers: Vec<Arc<turso_core::Buffer>>,
        _io_ctx: &turso_core::IOContext,
        c: turso_core::Completion,
    ) -> turso_core::Result<turso_core::CompletionBuilder> {
        let pos = first_page_idx.saturating_sub(1) as u64 * page_size as u64;
        let c = IOBuilder::pwritev(self.file.clone(), pos, buffers, c);
        Ok(c)
    }

    fn sync(&self, c: turso_core::Completion) -> turso_core::Result<turso_core::CompletionBuilder> {
        Ok(IOBuilder::sync(self.file.clone(), c))
    }

    fn size(&self) -> turso_core::Result<u64> {
        self.file.size()
    }

    fn truncate(
        &self,
        len: u64,
        c: turso_core::Completion,
    ) -> turso_core::Result<turso_core::CompletionBuilder> {
        let c = IOBuilder::truncate(self.file.clone(), len, c);
        Ok(c)
    }
}
