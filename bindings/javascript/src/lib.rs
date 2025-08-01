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
use std::{cell::RefCell, num::NonZeroUsize, sync::Arc};

/// The presentation mode for rows.
#[derive(Debug, Clone)]
enum PresentationMode {
    Expanded,
    Raw,
    Pluck,
}

/// A database connection.
#[napi]
pub struct Database {
    _db: Arc<turso_core::Database>,
    io: Arc<dyn turso_core::IO>,
    conn: Arc<turso_core::Connection>,
    is_memory: bool,
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
            turso_core::Database::open(io.clone(), &path, db_file, false, false).map_err(|e| {
                Error::new(
                    Status::GenericFailure,
                    format!("Failed to open database: {e}"),
                )
            })?;

        let conn = db
            .connect()
            .map_err(|e| Error::new(Status::GenericFailure, format!("Failed to connect: {e}")))?;

        Ok(Database {
            _db: db,
            io,
            conn,
            is_memory,
        })
    }

    /// Returns whether the database is in memory-only mode.
    #[napi(getter)]
    pub fn memory(&self) -> bool {
        self.is_memory
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
        let stmt = self.conn.prepare(&sql).map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Failed to prepare statement: {e}"),
            )
        })?;
        let column_names: Vec<std::ffi::CString> = (0..stmt.num_columns())
            .map(|i| std::ffi::CString::new(stmt.get_column_name(i).to_string()).unwrap())
            .collect();
        Ok(Statement {
            stmt: RefCell::new(Some(stmt)),
            column_names,
            mode: RefCell::new(PresentationMode::Expanded),
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
        // Database close is handled automatically when dropped
        Ok(())
    }

    /// Runs the I/O loop synchronously.
    #[napi]
    pub fn io_loop_sync(&self) -> Result<()> {
        self.io
            .step()
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
                if n.fract() == 0.0 {
                    turso_core::Value::Integer(n as i64)
                } else {
                    turso_core::Value::Float(n)
                }
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
                // Try to cast as Buffer first, fallback to string conversion
                if let Ok(buffer) = unsafe { value.cast::<Buffer>() } {
                    turso_core::Value::Blob(buffer.to_vec())
                } else {
                    let s = value.coerce_to_string()?.into_utf8()?;
                    turso_core::Value::Text(s.as_str()?.to_owned().into())
                }
            }
            _ => {
                // Fallback to string conversion for unknown types
                let s = value.coerce_to_string()?.into_utf8()?;
                turso_core::Value::Text(s.as_str()?.to_owned().into())
            }
        };

        stmt.bind_at(non_zero_idx, turso_value);
        Ok(())
    }

    #[napi]
    pub fn step<'env>(&self, env: &'env Env) -> Result<Unknown<'env>> {
        let mut stmt_ref = self.stmt.borrow_mut();
        let stmt = stmt_ref
            .as_mut()
            .ok_or_else(|| Error::new(Status::GenericFailure, "Statement has been finalized"))?;

        let mut result = Object::new(env)?;

        match stmt.step() {
            Ok(turso_core::StepResult::Row) => {
                result.set_named_property("done", false)?;

                let row_data = stmt
                    .row()
                    .ok_or_else(|| Error::new(Status::GenericFailure, "No row data available"))?;

                let mode = self.mode.borrow();
                let row_value =
                    match *mode {
                        PresentationMode::Raw => {
                            let mut raw_array = env.create_array(row_data.len() as u32)?;
                            for (idx, value) in row_data.get_values().enumerate() {
                                let js_value = to_js_value(env, value)?;
                                raw_array.set(idx as u32, js_value)?;
                            }
                            raw_array.coerce_to_object()?.to_unknown()
                        }
                        PresentationMode::Pluck => {
                            let (_, value) = row_data.get_values().enumerate().next().ok_or(
                                napi::Error::new(
                                    napi::Status::GenericFailure,
                                    "Pluck mode requires at least one column in the result",
                                ),
                            )?;
                            to_js_value(env, value)?
                        }
                        PresentationMode::Expanded => {
                            let row = Object::new(env)?;
                            let raw_row = row.raw();
                            let raw_env = env.raw();
                            for idx in 0..row_data.len() {
                                let value = row_data.get_value(idx);
                                let column_name = &self.column_names[idx];
                                let js_value = to_js_value(env, value)?;
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

                result.set_named_property("value", row_value)?;
            }
            Ok(turso_core::StepResult::Done) => {
                result.set_named_property("done", true)?;
                result.set_named_property("value", Null)?;
            }
            Ok(turso_core::StepResult::IO) => {
                result.set_named_property("io", true)?;
                result.set_named_property("value", Null)?;
            }
            Ok(turso_core::StepResult::Interrupt) => {
                return Err(Error::new(
                    Status::GenericFailure,
                    "Statement was interrupted",
                ));
            }
            Ok(turso_core::StepResult::Busy) => {
                return Err(Error::new(Status::GenericFailure, "Database is busy"));
            }
            Err(e) => {
                return Err(Error::new(
                    Status::GenericFailure,
                    format!("Step failed: {e}"),
                ))
            }
        }

        Ok(result.to_unknown())
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

    /// Finalizes the statement.
    #[napi]
    pub fn finalize(&self) -> Result<()> {
        self.stmt.borrow_mut().take();
        Ok(())
    }
}

/// Async task for running the I/O loop.
pub struct IoLoopTask {
    io: Arc<dyn turso_core::IO>,
}

impl Task for IoLoopTask {
    type Output = ();
    type JsValue = ();

    fn compute(&mut self) -> napi::Result<Self::Output> {
        self.io.step().map_err(|e| {
            napi::Error::new(napi::Status::GenericFailure, format!("IO error: {e}"))
        })?;
        Ok(())
    }

    fn resolve(&mut self, _env: Env, _output: Self::Output) -> napi::Result<Self::JsValue> {
        Ok(())
    }
}

/// Convert a Turso value to a JavaScript value.
fn to_js_value<'a>(env: &'a napi::Env, value: &turso_core::Value) -> napi::Result<Unknown<'a>> {
    match value {
        turso_core::Value::Null => ToNapiValue::into_unknown(Null, env),
        turso_core::Value::Integer(i) => ToNapiValue::into_unknown(i, env),
        turso_core::Value::Float(f) => ToNapiValue::into_unknown(f, env),
        turso_core::Value::Text(s) => ToNapiValue::into_unknown(s.as_str(), env),
        turso_core::Value::Blob(b) => ToNapiValue::into_unknown(b, env),
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
    fn read_page(
        &self,
        page_idx: usize,
        c: turso_core::Completion,
    ) -> turso_core::Result<turso_core::Completion> {
        let r = c.as_read();
        let size = r.buf().len();
        assert!(page_idx > 0);
        if !(512..=65536).contains(&size) || size & (size - 1) != 0 {
            return Err(turso_core::LimboError::NotADB);
        }
        let pos = (page_idx - 1) * size;
        self.file.pread(pos, c)
    }

    fn write_page(
        &self,
        page_idx: usize,
        buffer: Arc<RefCell<turso_core::Buffer>>,
        c: turso_core::Completion,
    ) -> turso_core::Result<turso_core::Completion> {
        let size = buffer.borrow().len();
        let pos = (page_idx - 1) * size;
        self.file.pwrite(pos, buffer, c)
    }

    fn write_pages(
        &self,
        page_idx: usize,
        page_size: usize,
        buffers: Vec<Arc<RefCell<turso_core::Buffer>>>,
        c: turso_core::Completion,
    ) -> turso_core::Result<turso_core::Completion> {
        let pos = page_idx.saturating_sub(1) * page_size;
        let c = self.file.pwritev(pos, buffers, c)?;
        Ok(c)
    }

    fn sync(&self, c: turso_core::Completion) -> turso_core::Result<turso_core::Completion> {
        self.file.sync(c)
    }

    fn size(&self) -> turso_core::Result<u64> {
        self.file.size()
    }

    fn truncate(
        &self,
        len: usize,
        c: turso_core::Completion,
    ) -> turso_core::Result<turso_core::Completion> {
        let c = self.file.truncate(len, c)?;
        Ok(c)
    }
}
