use std::{
    borrow::Cow,
    sync::{Arc, Mutex, Once, RwLock},
};

use tracing::level_filters::LevelFilter;
use tracing_subscriber::{
    fmt::{self, format::Writer},
    layer::{Context, SubscriberExt},
    util::SubscriberInitExt,
    EnvFilter, Layer,
};
use turso_core::{
    types::AsValueRef, Connection, Database, DatabaseOpts, LimboError, OpenFlags, Statement,
    StepResult, IO,
};

use crate::{assert_send, assert_sync, capi};

assert_send!(TursoDatabase, TursoConnection, TursoStatement);
assert_sync!(TursoDatabase);

pub struct TursoLog<'a> {
    pub message: &'a str,
    pub target: &'a str,
    pub file: &'a str,
    pub timestamp: u64,
    pub line: usize,
    pub level: tracing::Level,
}

pub struct TursoSetupConfig {
    pub logger: Option<fn(log: TursoLog)>,
    pub log_level: Option<String>,
}

impl TursoSetupConfig {
    pub fn from_capi(value: capi::c::turso_config_t) -> Result<Self, TursoError> {
        Ok(Self {
            log_level: None,
            logger: None,
        })
    }
}

pub struct TursoDatabaseConfig {
    /// path to the database file or ":memory:" for in-memory connection
    pub path: String,

    /// comma-separated list of experimental features to enable
    /// this field is intentionally just a string in order to make enablement of experimental features as flexible as possible
    pub experimental_features: Option<String>,

    /// optional custom IO provided by the caller
    pub io: Option<Arc<dyn IO>>,

    /// if true, library methods will return Io status code and delegate Io loop to the caller
    /// if false, library will spin IO itself in case of Io status code and never return it to the caller
    pub async_io: bool,
}

pub fn value_from_c_value(value: capi::c::turso_value_t) -> Result<turso_core::Value, TursoError> {
    match value.type_ {
        capi::c::turso_type_t::TURSO_TYPE_NULL => Ok(turso_core::Value::Null),
        capi::c::turso_type_t::TURSO_TYPE_INTEGER => {
            Ok(turso_core::Value::Integer(unsafe { value.value.integer }))
        }
        capi::c::turso_type_t::TURSO_TYPE_REAL => {
            Ok(turso_core::Value::Float(unsafe { value.value.real }))
        }
        capi::c::turso_type_t::TURSO_TYPE_TEXT => {
            let text = std::str::from_utf8(bytes_from_turso_slice(unsafe { value.value.text }));
            let text = match text {
                Ok(text) => text,
                Err(err) => {
                    return Err(TursoError {
                        code: TursoStatusCode::Misuse,
                        message: Some(format!("expected utf-8 string: {err}")),
                    })
                }
            };
            Ok(turso_core::Value::Text(turso_core::types::Text::new(text)))
        }
        capi::c::turso_type_t::TURSO_TYPE_BLOB => {
            let blob = bytes_from_turso_slice(unsafe { value.value.blob });
            Ok(turso_core::Value::Blob(blob.to_vec()))
        }
    }
}

pub fn turso_slice_from_bytes(bytes: &[u8]) -> capi::c::turso_slice_ref_t {
    capi::c::turso_slice_ref_t {
        ptr: bytes.as_ptr() as *const std::ffi::c_void,
        len: bytes.len(),
    }
}

pub fn str_from_turso_slice(slice: capi::c::turso_slice_ref_t) -> Result<&'static str, TursoError> {
    let s = unsafe { std::slice::from_raw_parts(slice.ptr as *const u8, slice.len) };
    match std::str::from_utf8(s) {
        Ok(s) => Ok(s),
        Err(err) => Err(TursoError {
            code: TursoStatusCode::Misuse,
            message: Some(format!("expected slice representing utf-8 value: {err}")),
        }),
    }
}

pub fn bytes_from_turso_slice(slice: capi::c::turso_slice_ref_t) -> &'static [u8] {
    unsafe { std::slice::from_raw_parts(slice.ptr as *const u8, slice.len) }
}

pub fn str_from_c_str(ptr: *const std::ffi::c_char) -> Result<&'static str, TursoError> {
    if ptr.is_null() {
        return Err(TursoError {
            code: TursoStatusCode::Misuse,
            message: Some(format!(
                "expected zero terminated c string, got null pointer"
            )),
        });
    }
    let c_str = unsafe { std::ffi::CStr::from_ptr(ptr) };
    match c_str.to_str() {
        Ok(s) => Ok(s),
        Err(err) => Err(TursoError {
            code: TursoStatusCode::Misuse,
            message: Some(format!(
                "expected zero terminated c-string representing utf-8 value: {err}"
            )),
        }),
    }
}

impl TursoDatabaseConfig {
    pub fn from_capi(value: capi::c::turso_database_config_t) -> Result<Self, TursoError> {
        Ok(Self {
            path: str_from_c_str(value.path)?.to_string(),
            experimental_features: if !value.experimental_features.is_null() {
                Some(str_from_c_str(value.experimental_features)?.to_string())
            } else {
                None
            },
            async_io: value.async_io,
            io: None,
        })
    }
}

struct TursoDatabaseInner {
    config: TursoDatabaseConfig,
    db: Arc<Mutex<Option<Arc<Database>>>>,
}

#[derive(Clone)]
pub struct TursoDatabase {
    inner: Arc<TursoDatabaseInner>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum TursoStatusCode {
    Ok = 0,
    Done = 1,
    Row = 2,
    Io = 3,
    Busy = 4,
    Interrupt = 5,
    Error = 127,
    Misuse = 128,
    Constraint = 129,
    Readonly = 130,
    DatabaseFull = 131,
    NotAdb = 132,
    Corrupt = 133,
}

impl TursoStatusCode {
    pub fn to_capi(self) -> capi::c::turso_status_t {
        capi::c::turso_status_t {
            code: unsafe { std::mem::transmute(self as u32) },
            error: std::ptr::null(),
        }
    }
}

#[derive(Debug)]
pub struct TursoError {
    pub code: TursoStatusCode,
    pub message: Option<String>,
}

pub fn str_to_c_string(message: &str) -> *const std::ffi::c_char {
    let message = std::ffi::CString::new(message).expect("string must be zero terminated");
    message.into_raw()
}

pub fn c_string_to_str(ptr: *const std::ffi::c_char) -> std::ffi::CString {
    unsafe { std::ffi::CString::from_raw(ptr as *mut std::ffi::c_char) }
}

impl TursoError {
    pub fn to_capi(self) -> capi::c::turso_status_t {
        capi::c::turso_status_t {
            code: unsafe { std::mem::transmute(self.code as u32) },
            error: match self.message {
                Some(message) => str_to_c_string(&message),
                None => std::ptr::null(),
            },
        }
    }
}

fn turso_error_from_limbo_error(err: LimboError) -> TursoError {
    TursoError {
        code: match &err {
            LimboError::Constraint(_) => TursoStatusCode::Constraint,
            LimboError::Corrupt(..) => TursoStatusCode::Corrupt,
            LimboError::NotADB => TursoStatusCode::NotAdb,
            LimboError::DatabaseFull(_) => TursoStatusCode::DatabaseFull,
            LimboError::ReadOnly => TursoStatusCode::Readonly,
            LimboError::Busy => TursoStatusCode::Busy,
            _ => TursoStatusCode::Error,
        },
        message: Some(format!("{err}")),
    }
}
static LOGGER: RwLock<Option<fn(TursoLog)>> = RwLock::new(None);
static SETUP: Once = Once::new();

struct CallbackLayer<F>
where
    F: Fn(TursoLog) + Send + Sync + 'static,
{
    callback: F,
}

impl<S, F> tracing_subscriber::Layer<S> for CallbackLayer<F>
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    F: Fn(TursoLog) + Send + Sync + 'static,
{
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        let mut buffer = String::new();
        let mut visitor = fmt::format::DefaultVisitor::new(Writer::new(&mut buffer), true);

        event.record(&mut visitor);

        let log = TursoLog {
            level: event.metadata().level().clone(),
            target: event.metadata().target(),
            message: &buffer,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|t| t.as_secs())
                .unwrap_or(0),
            file: event.metadata().file().unwrap_or(""),
            line: event.metadata().line().unwrap_or(0) as usize,
        };

        (self.callback)(log);
    }
}

pub fn turso_setup(config: TursoSetupConfig) -> Result<(), TursoError> {
    fn callback(log: TursoLog<'_>) {
        let Ok(logger) = LOGGER.try_read() else {
            return;
        };

        if let Some(logger) = *logger {
            logger(log)
        }
    }

    if let Some(logger) = config.logger.as_ref() {
        let mut guard = LOGGER.write().unwrap();
        *guard = Some(*logger);
    }

    let level_filter = if let Some(log_level) = &config.log_level {
        match log_level.as_ref() {
            "error" => Some(LevelFilter::ERROR),
            "warn" => Some(LevelFilter::WARN),
            "info" => Some(LevelFilter::INFO),
            "debug" => Some(LevelFilter::DEBUG),
            "trace" => Some(LevelFilter::TRACE),
            _ => {
                return Err(TursoError {
                    code: TursoStatusCode::Error,
                    message: Some(format!("unknown log level")),
                })
            }
        }
    } else {
        None
    };

    SETUP.call_once(|| {
        if let Some(level_filter) = level_filter {
            tracing_subscriber::registry()
                .with(CallbackLayer { callback }.with_filter(level_filter))
                .init();
        } else {
            tracing_subscriber::registry()
                .with(CallbackLayer { callback }.with_filter(EnvFilter::from_default_env()))
                .init();
        }
    });

    Ok(())
}

impl TursoDatabase {
    /// create database holder struct but do not initialize it yet
    /// this can be useful for some environments, where IO operations must be executed in certain fashion (and open do IO under the hood)
    pub fn create(config: TursoDatabaseConfig) -> Self {
        Self {
            inner: Arc::new(TursoDatabaseInner {
                config,
                db: Arc::new(Mutex::new(None)),
            }),
        }
    }
    /// open the database
    /// this method must be called only once
    pub fn open(&self) -> Result<(), TursoError> {
        let io: Arc<dyn turso_core::IO> = if let Some(io) = &self.inner.config.io {
            io.clone()
        } else {
            match self.inner.config.path.as_str() {
                ":memory:" => Arc::new(turso_core::MemoryIO::new()),
                _ => match turso_core::PlatformIO::new() {
                    Ok(io) => Arc::new(io),
                    Err(err) => {
                        return Err(turso_error_from_limbo_error(err));
                    }
                },
            }
        };
        let mut opts = DatabaseOpts::new();
        if let Some(experimental_features) = &self.inner.config.experimental_features {
            for features in experimental_features.split(",").map(|s| s.trim()) {
                opts = match features {
                    "views" => opts.with_views(true),
                    "mvcc" => opts.with_mvcc(true),
                    "index_method" => opts.with_index_method(true),
                    "strict" => opts.with_strict(true),
                    "autovacuum" => opts.with_autovacuum(true),
                    _ => opts,
                };
            }
        }
        match turso_core::Database::open_file_with_flags(
            io.clone(),
            &self.inner.config.path,
            OpenFlags::default(),
            opts,
            None,
        ) {
            Ok(db) => {
                let mut inner_db = self.inner.db.lock().unwrap();
                *inner_db = Some(db);
                Ok(())
            }
            Err(err) => Err(turso_error_from_limbo_error(err)),
        }
    }

    /// creates database connection
    /// database must be already opened with [Self::open] method
    pub fn connect(&self) -> Result<TursoConnection, TursoError> {
        let inner_db = self.inner.db.lock().unwrap();
        let Some(db) = inner_db.as_ref() else {
            return Err(TursoError {
                code: TursoStatusCode::Misuse,
                message: Some(format!("database must be opened first")),
            });
        };
        let connection = db.connect();

        match connection {
            Ok(connection) => Ok(TursoConnection {
                async_io: self.inner.config.async_io,
                connection,
            }),
            Err(err) => Err(turso_error_from_limbo_error(err)),
        }
    }

    /// helper method to get C raw container with TursoDatabase instance
    /// this method is used in the capi wrappers
    pub fn to_capi(self) -> capi::c::turso_database_t {
        capi::c::turso_database_t {
            inner: Arc::into_raw(self.inner) as *mut std::ffi::c_void,
        }
    }

    /// helper method to restore TursoDatabase instance from C raw container
    /// this method is used in the capi wrappers
    pub unsafe fn from_capi(value: capi::c::turso_database_t) -> Self {
        Self {
            inner: Arc::from_raw(value.inner as *const TursoDatabaseInner),
        }
    }
}

pub struct TursoConnection {
    async_io: bool,
    connection: Arc<Connection>,
}

impl TursoConnection {
    /// prepares single SQL statement
    pub fn prepare_single(&self, sql: impl AsRef<str>) -> Result<TursoStatement, TursoError> {
        match self.connection.prepare(sql) {
            Ok(statement) => Ok(TursoStatement {
                async_io: self.async_io,
                statement: Box::new(statement),
            }),
            Err(err) => Err(turso_error_from_limbo_error(err)),
        }
    }
    /// prepares first SQL statement from the string and return prepared statement and position after the end of the parsed statement
    /// this method can be useful if SDK provides an execute(...) method which run all statements from the provided input in sequence
    pub fn prepare_first(
        &self,
        sql: impl AsRef<str>,
    ) -> Result<Option<(TursoStatement, usize)>, TursoError> {
        match self.connection.consume_stmt(sql) {
            Ok(Some((statement, position))) => Ok(Some((
                TursoStatement {
                    async_io: self.async_io,
                    statement: Box::new(statement),
                },
                position,
            ))),
            Ok(None) => Ok(None),
            Err(err) => Err(turso_error_from_limbo_error(err)),
        }
    }

    /// helper method to get C raw container to the TursoConnection instance
    /// this method is used in the capi wrappers
    pub fn to_capi(self) -> capi::c::turso_connection_t {
        capi::c::turso_connection_t {
            async_io: self.async_io,
            inner: Arc::into_raw(self.connection) as *mut std::ffi::c_void,
        }
    }

    /// helper method to restore TursoConnection instance from C raw container
    /// this method is used in the capi wrappers
    pub unsafe fn from_capi(value: capi::c::turso_connection_t) -> Self {
        Self {
            async_io: value.async_io,
            connection: Arc::from_raw(value.inner as *const Connection),
        }
    }
}

pub struct TursoStatement {
    async_io: bool,
    statement: Box<Statement>,
}

pub struct TursoExecutionResult {
    pub status: TursoStatusCode,
    pub rows_changed: u64,
}

impl TursoStatement {
    pub fn bind_positional(
        &mut self,
        index: usize,
        value: turso_core::Value,
    ) -> Result<(), TursoError> {
        let Ok(index) = index.try_into() else {
            return Err(TursoError {
                code: TursoStatusCode::Misuse,
                message: Some(format!("bind index must be non-zero")),
            });
        };
        self.statement.bind_at(index, value);
        Ok(())
    }
    pub fn bind_named(
        &mut self,
        name: impl AsRef<str>,
        value: turso_core::Value,
    ) -> Result<(), TursoError> {
        let parameters = self.statement.parameters();
        for i in 1..parameters.next_index().get() {
            // i is positive - so conversion to NonZero<> type will always succeed
            let index = i.try_into().unwrap();
            let Some(parameter) = parameters.name(index) else {
                continue;
            };
            assert!(
                parameter.starts_with(":")
                    || parameter.starts_with("@")
                    || parameter.starts_with("$")
                    || parameter.starts_with("?")
            );
            if name.as_ref() == &parameter[1..] {
                self.statement.bind_at(index, value);
                return Ok(());
            }
        }

        Err(TursoError {
            code: TursoStatusCode::Error,
            message: Some(format!("named parameter {} not found", name.as_ref())),
        })
    }
    pub fn step(&mut self) -> Result<TursoStatusCode, TursoError> {
        let async_io = self.async_io;
        loop {
            return match self.statement.step() {
                Ok(StepResult::Done) => Ok(TursoStatusCode::Done),
                Ok(StepResult::Row) => Ok(TursoStatusCode::Row),
                Ok(StepResult::Busy) => Err(TursoError {
                    code: TursoStatusCode::Busy,
                    message: None,
                }),
                Ok(StepResult::Interrupt) => Err(TursoError {
                    code: TursoStatusCode::Interrupt,
                    message: None,
                }),
                Ok(StepResult::IO) => {
                    if async_io {
                        Ok(TursoStatusCode::Io)
                    } else {
                        self.run_io()?;
                        continue;
                    }
                }
                Err(err) => return Err(turso_error_from_limbo_error(err)),
            };
        }
    }
    pub fn execute(&mut self) -> Result<TursoExecutionResult, TursoError> {
        loop {
            let status = self.step()?;
            if status == TursoStatusCode::Row {
                continue;
            } else if status == TursoStatusCode::Io {
                return Ok(TursoExecutionResult {
                    status,
                    rows_changed: 0,
                });
            } else if status == TursoStatusCode::Done {
                return Ok(TursoExecutionResult {
                    status: TursoStatusCode::Ok,
                    rows_changed: self.statement.n_change() as u64,
                });
            }
            panic!("unexpected status code: {status:?}");
        }
    }
    pub fn run_io(&self) -> Result<(), TursoError> {
        self.statement
            .run_once()
            .map_err(|err| turso_error_from_limbo_error(err))
    }
    pub fn row_value(&self, index: usize) -> Result<turso_core::ValueRef, TursoError> {
        let Some(row) = self.statement.row() else {
            return Err(TursoError {
                code: TursoStatusCode::Misuse,
                message: Some(format!("statement holds no row")),
            });
        };
        if index >= row.len() {
            return Err(TursoError {
                code: TursoStatusCode::Misuse,
                message: Some(format!("attempt to access row value out of bounds")),
            });
        }
        let value = row.get_value(index);
        Ok(value.as_value_ref())
    }
    pub fn column_count(&self) -> usize {
        self.statement.num_columns()
    }
    pub fn column_name(&self, index: usize) -> Cow<'_, str> {
        self.statement.get_column_name(index)
    }
    pub fn finalize(&mut self) -> Result<TursoStatusCode, TursoError> {
        while self.statement.execution_state().is_running() {
            let status = self.step()?;
            if status == TursoStatusCode::Io {
                return Ok(status);
            }
        }
        Ok(TursoStatusCode::Ok)
    }
    pub fn reset(&mut self) -> Result<(), TursoError> {
        self.statement.reset();
        self.statement.clear_bindings();
        Ok(())
    }

    /// helper method to get C raw container to the TursoStatement instance
    /// this method is used in the capi wrappers
    pub fn to_capi(self) -> capi::c::turso_statement_t {
        capi::c::turso_statement_t {
            async_io: self.async_io,
            inner: Box::into_raw(self.statement) as *mut std::ffi::c_void,
        }
    }

    /// helper method to restore TursoStatement instance from C raw container
    /// this method is used in the capi wrappers
    pub unsafe fn from_capi(value: capi::c::turso_statement_t) -> Self {
        Self {
            async_io: value.async_io,
            statement: Box::from_raw(value.inner as *mut Statement),
        }
    }
}
