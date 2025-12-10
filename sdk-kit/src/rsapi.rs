use std::{
    borrow::Cow,
    fmt::Display,
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
    storage::database::DatabaseFile, types::AsValueRef, Connection, Database, DatabaseOpts,
    DatabaseStorage, LimboError, OpenFlags, Statement, StepResult, IO,
};

use crate::{
    assert_send, assert_sync,
    capi::{self, c},
    ConcurrentGuard,
};

assert_send!(TursoDatabase, TursoConnection, TursoStatement);
assert_sync!(TursoDatabase);

pub type Value = turso_core::Value;
pub type ValueRef<'a> = turso_core::types::ValueRef<'a>;
pub type Text = turso_core::types::Text;
pub type TextRef<'a> = turso_core::types::TextRef<'a>;

pub struct TursoLog<'a> {
    pub message: &'a str,
    pub target: &'a str,
    pub file: &'a str,
    pub timestamp: u64,
    pub line: usize,
    pub level: &'a str,
}

type Logger = dyn Fn(TursoLog) + Send + Sync + 'static;
pub struct TursoSetupConfig {
    pub logger: Option<Box<Logger>>,
    pub log_level: Option<String>,
}

fn logger_wrap(log: TursoLog<'_>, logger: unsafe extern "C" fn(*const c::turso_log_t)) {
    let Ok(message_cstr) = std::ffi::CString::new(log.message) else {
        return;
    };
    let Ok(target_cstr) = std::ffi::CString::new(log.target) else {
        return;
    };
    let Ok(file_cstr) = std::ffi::CString::new(log.file) else {
        return;
    };
    unsafe {
        logger(&c::turso_log_t {
            message: message_cstr.as_ptr(),
            target: target_cstr.as_ptr(),
            file: file_cstr.as_ptr(),
            timestamp: log.timestamp,
            line: log.line,
            level: match log.level {
                "TRACE" => capi::c::turso_tracing_level_t::TURSO_TRACING_LEVEL_TRACE,
                "DEBUG" => capi::c::turso_tracing_level_t::TURSO_TRACING_LEVEL_DEBUG,
                "INFO" => capi::c::turso_tracing_level_t::TURSO_TRACING_LEVEL_INFO,
                "WARN" => capi::c::turso_tracing_level_t::TURSO_TRACING_LEVEL_WARN,
                _ => capi::c::turso_tracing_level_t::TURSO_TRACING_LEVEL_ERROR,
            },
        })
    };
}

impl TursoSetupConfig {
    /// helper method to restore [TursoSetupConfig] instance from C representation
    /// this method is used in the capi wrappers
    ///
    /// # Safety
    /// [c::turso_config_t::log_level] field must be valid C-string pointer or null
    pub unsafe fn from_capi(config: *const c::turso_config_t) -> Result<Self, TursoError> {
        if config.is_null() {
            return Err(TursoError {
                code: TursoStatusCode::Misuse,
                message: Some("config pointer must be not null".to_string()),
            });
        }
        let config = *config;
        Ok(Self {
            log_level: if !config.log_level.is_null() {
                Some(str_from_c_str(config.log_level)?.to_string())
            } else {
                None
            },
            logger: if let Some(logger) = config.logger {
                Some(Box::new(move |log| logger_wrap(log, logger)))
            } else {
                None
            },
        })
    }
}

#[derive(Clone)]
pub struct TursoDatabaseConfig {
    /// path to the database file or ":memory:" for in-memory connection
    pub path: String,

    /// comma-separated list of experimental features to enable
    /// this field is intentionally just a string in order to make enablement of experimental features as flexible as possible
    pub experimental_features: Option<String>,

    /// if true, library methods will return Io status code and delegate Io loop to the caller
    /// if false, library will spin IO itself in case of Io status code and never return it to the caller
    pub async_io: bool,

    /// optional custom IO provided by the caller
    pub io: Option<Arc<dyn IO>>,

    /// optional custom DatabaseStorage provided by the caller
    /// if provided, caller must guarantee that IO used by the TursoDatabase will be consistent with underlying DatabaseStorage IO
    pub db_file: Option<Arc<dyn DatabaseStorage>>,
}

pub fn turso_slice_from_bytes(bytes: &[u8]) -> capi::c::turso_slice_ref_t {
    capi::c::turso_slice_ref_t {
        ptr: bytes.as_ptr() as *const std::ffi::c_void,
        len: bytes.len(),
    }
}

/// # Safety
/// ptr must be valid C-string pointer or null
pub unsafe fn str_from_c_str<'a>(ptr: *const std::ffi::c_char) -> Result<&'a str, TursoError> {
    if ptr.is_null() {
        return Err(TursoError {
            code: TursoStatusCode::Misuse,
            message: Some("expected zero terminated c string, got null pointer".to_string()),
        });
    }
    let c_str = std::ffi::CStr::from_ptr(ptr);
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

/// # Safety
/// memory range [ptr..ptr + len) must be valid
pub unsafe fn str_from_slice<'a>(
    ptr: *const std::ffi::c_char,
    len: usize,
) -> Result<&'a str, TursoError> {
    let slice = bytes_from_slice(ptr, len)?;
    match std::str::from_utf8(slice) {
        Ok(s) => Ok(s),
        Err(err) => Err(TursoError {
            code: TursoStatusCode::Misuse,
            message: Some(format!(
                "expected string slice representing utf-8 value: {err}"
            )),
        }),
    }
}

/// # Safety
/// memory range [ptr..ptr + len) must be valid
pub unsafe fn bytes_from_slice<'a>(
    ptr: *const std::ffi::c_char,
    len: usize,
) -> Result<&'a [u8], TursoError> {
    if len == 0 {
        return Ok(&[]);
    }
    if ptr.is_null() {
        return Err(TursoError {
            code: TursoStatusCode::Misuse,
            message: Some("expected slice, got null pointer".to_string()),
        });
    }
    Ok(std::slice::from_raw_parts(ptr as *const u8, len))
}

/// SAFETY: slice must points to the valid memory
pub fn bytes_from_turso_slice<'a>(
    slice: capi::c::turso_slice_ref_t,
) -> Result<&'a [u8], TursoError> {
    if slice.ptr.is_null() {
        return Err(TursoError {
            code: TursoStatusCode::Misuse,
            message: Some("expected slice representing utf-8 value, got null".to_string()),
        });
    }
    Ok(unsafe { std::slice::from_raw_parts(slice.ptr as *const u8, slice.len) })
}

/// SAFETY: slice must points to the valid memory
pub fn str_from_turso_slice<'a>(slice: capi::c::turso_slice_ref_t) -> Result<&'a str, TursoError> {
    if slice.ptr.is_null() {
        return Err(TursoError {
            code: TursoStatusCode::Misuse,
            message: Some("expected slice representing utf-8 value, got null".to_string()),
        });
    }
    let s = unsafe { std::slice::from_raw_parts(slice.ptr as *const u8, slice.len) };
    match std::str::from_utf8(s) {
        Ok(s) => Ok(s),
        Err(err) => Err(TursoError {
            code: TursoStatusCode::Misuse,
            message: Some(format!("expected slice representing utf-8 value: {err}")),
        }),
    }
}

impl TursoDatabaseConfig {
    /// helper method to restore [TursoSetupConfig] instance from C representation
    /// this method is used in the capi wrappers
    ///
    /// # Safety
    /// [c::turso_database_config_t::path] field must be valid C-string pointer
    /// [c::turso_database_config_t::experimental_features] field must be valid C-string pointer or null
    pub unsafe fn from_capi(config: *const c::turso_database_config_t) -> Result<Self, TursoError> {
        if config.is_null() {
            return Err(TursoError {
                code: TursoStatusCode::Misuse,
                message: Some("config pointer must be not null".to_string()),
            });
        }
        let config = *config;
        Ok(Self {
            path: str_from_c_str(config.path)?.to_string(),
            experimental_features: if !config.experimental_features.is_null() {
                Some(str_from_c_str(config.experimental_features)?.to_string())
            } else {
                None
            },
            async_io: config.async_io,
            io: None,
            db_file: None,
        })
    }
}

pub struct TursoDatabase {
    config: TursoDatabaseConfig,
    db: Arc<Mutex<Option<Arc<Database>>>>,
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
    pub fn to_capi(self) -> capi::c::turso_status_code_t {
        match self {
            TursoStatusCode::Ok => capi::c::turso_status_code_t::TURSO_OK,
            TursoStatusCode::Done => capi::c::turso_status_code_t::TURSO_DONE,
            TursoStatusCode::Row => capi::c::turso_status_code_t::TURSO_ROW,
            TursoStatusCode::Io => capi::c::turso_status_code_t::TURSO_IO,
            TursoStatusCode::Busy => capi::c::turso_status_code_t::TURSO_BUSY,
            TursoStatusCode::Interrupt => capi::c::turso_status_code_t::TURSO_INTERRUPT,
            TursoStatusCode::Error => capi::c::turso_status_code_t::TURSO_ERROR,
            TursoStatusCode::Misuse => capi::c::turso_status_code_t::TURSO_MISUSE,
            TursoStatusCode::Constraint => capi::c::turso_status_code_t::TURSO_CONSTRAINT,
            TursoStatusCode::Readonly => capi::c::turso_status_code_t::TURSO_READONLY,
            TursoStatusCode::DatabaseFull => capi::c::turso_status_code_t::TURSO_DATABASE_FULL,
            TursoStatusCode::NotAdb => capi::c::turso_status_code_t::TURSO_NOTADB,
            TursoStatusCode::Corrupt => capi::c::turso_status_code_t::TURSO_CORRUPT,
        }
    }
}

#[derive(Debug)]
pub struct TursoError {
    pub code: TursoStatusCode,
    pub message: Option<String>,
}

impl TursoError {
    /// # Safety
    /// error_opt_out must be a valid pointer or null
    pub unsafe fn to_capi(
        &self,
        error_opt_out: *mut *const std::ffi::c_char,
    ) -> capi::c::turso_status_code_t {
        if !error_opt_out.is_null() {
            let message = if let Some(message) = &self.message {
                str_to_c_string(message)
            } else {
                std::ptr::null_mut()
            };
            unsafe { *error_opt_out = message };
        }
        self.code.to_capi()
    }
}

impl Display for TursoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{:?}: {:?}", self.code, self.message))
    }
}

pub fn str_to_c_string(message: &str) -> *const std::ffi::c_char {
    let Ok(message) = std::ffi::CString::new(message) else {
        return std::ptr::null();
    };
    message.into_raw()
}

pub fn c_string_to_str(ptr: *const std::ffi::c_char) -> std::ffi::CString {
    unsafe { std::ffi::CString::from_raw(ptr as *mut std::ffi::c_char) }
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
static LOGGER: RwLock<Option<Box<Logger>>> = RwLock::new(None);
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
            level: event.metadata().level().as_str(),
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

        if let Some(logger) = logger.as_ref() {
            logger(log)
        }
    }

    if let Some(logger) = config.logger {
        let mut guard = LOGGER.write().unwrap();
        *guard = Some(logger);
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
                    message: Some("unknown log level".to_string()),
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
    /// return turso version
    pub const fn version() -> &'static str {
        env!("CARGO_PKG_VERSION")
    }
    /// method to get [turso_core::Database] instance which can be useful for code which integrates with sdk-kit
    pub fn db_core(&self) -> Result<Arc<turso_core::Database>, TursoError> {
        let db = self.db.lock().unwrap();
        match &*db {
            Some(db) => Ok(db.clone()),
            None => Err(TursoError {
                code: TursoStatusCode::Misuse,
                message: Some("database must be opened".to_string()),
            }),
        }
    }
    /// create database holder struct but do not initialize it yet
    /// this can be useful for some environments, where IO operations must be executed in certain fashion (and open do IO under the hood)
    pub fn new(config: TursoDatabaseConfig) -> Arc<Self> {
        Arc::new(Self {
            config,
            db: Arc::new(Mutex::new(None)),
        })
    }
    /// open the database
    /// this method must be called only once
    pub fn open(&self) -> Result<(), TursoError> {
        let mut inner_db = self.db.lock().unwrap();
        if inner_db.is_some() {
            return Err(TursoError {
                code: TursoStatusCode::Misuse,
                message: Some("database must be opened only once".to_string()),
            });
        }
        let io: Arc<dyn turso_core::IO> = if let Some(io) = &self.config.io {
            io.clone()
        } else {
            match self.config.path.as_str() {
                ":memory:" => Arc::new(turso_core::MemoryIO::new()),
                _ => match turso_core::PlatformIO::new() {
                    Ok(io) => Arc::new(io),
                    Err(err) => {
                        return Err(turso_error_from_limbo_error(err));
                    }
                },
            }
        };
        let open_flags = OpenFlags::default();
        let db_file = if let Some(db_file) = &self.config.db_file {
            db_file.clone()
        } else {
            let file = io
                .open_file(&self.config.path, open_flags, true)
                .map_err(turso_error_from_limbo_error)?;
            Arc::new(DatabaseFile::new(file))
        };
        let mut opts = DatabaseOpts::new();
        if let Some(experimental_features) = &self.config.experimental_features {
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
        match turso_core::Database::open_with_flags(
            io.clone(),
            &self.config.path,
            db_file,
            open_flags,
            opts,
            None,
        ) {
            Ok(db) => {
                *inner_db = Some(db);
                Ok(())
            }
            Err(err) => Err(turso_error_from_limbo_error(err)),
        }
    }

    /// creates database connection
    /// database must be already opened with [Self::open] method
    pub fn connect(&self) -> Result<Arc<TursoConnection>, TursoError> {
        let inner_db = self.db.lock().unwrap();
        let Some(db) = inner_db.as_ref() else {
            return Err(TursoError {
                code: TursoStatusCode::Misuse,
                message: Some("database must be opened first".to_string()),
            });
        };
        let connection = db.connect();

        match connection {
            Ok(connection) => Ok(TursoConnection::new(&self.config, connection)),
            Err(err) => Err(turso_error_from_limbo_error(err)),
        }
    }

    /// helper method to get C raw container with TursoDatabase instance
    /// this method is used in the capi wrappers
    pub fn to_capi(self: Arc<Self>) -> *mut capi::c::turso_database_t {
        Arc::into_raw(self) as *mut capi::c::turso_database_t
    }

    /// helper method to restore TursoDatabase ref from C raw container
    /// this method is used in the capi wrappers
    ///
    /// # Safety
    /// value must be a pointer returned from [Self::to_capi] method
    pub unsafe fn ref_from_capi<'a>(
        value: *const capi::c::turso_database_t,
    ) -> Result<&'a Self, TursoError> {
        if value.is_null() {
            Err(TursoError {
                code: TursoStatusCode::Misuse,
                message: Some("got null pointer".to_string()),
            })
        } else {
            Ok(&*(value as *const Self))
        }
    }

    /// helper method to restore TursoDatabase instance from C raw container
    /// this method is used in the capi wrappers
    ///
    /// # Safety
    /// value must be a pointer returned from [Self::to_capi] method
    pub unsafe fn arc_from_capi(value: *const capi::c::turso_database_t) -> Arc<Self> {
        Arc::from_raw(value as *const Self)
    }
}

pub struct TursoConnection {
    async_io: bool,
    concurrent_guard: Arc<ConcurrentGuard>,
    connection: Arc<Connection>,
}

impl TursoConnection {
    pub fn new(config: &TursoDatabaseConfig, connection: Arc<Connection>) -> Arc<Self> {
        Arc::new(Self {
            async_io: config.async_io,
            connection,
            concurrent_guard: Arc::new(ConcurrentGuard::new()),
        })
    }
    pub fn get_auto_commit(&self) -> bool {
        self.connection.get_auto_commit()
    }
    pub fn last_insert_rowid(&self) -> i64 {
        self.connection.last_insert_rowid()
    }
    /// prepares single SQL statement
    pub fn prepare_single(&self, sql: impl AsRef<str>) -> Result<Box<TursoStatement>, TursoError> {
        match self.connection.prepare(sql) {
            Ok(statement) => Ok(Box::new(TursoStatement {
                concurrent_guard: self.concurrent_guard.clone(),
                async_io: self.async_io,
                statement,
            })),
            Err(err) => Err(turso_error_from_limbo_error(err)),
        }
    }
    /// prepares first SQL statement from the string and return prepared statement and position after the end of the parsed statement
    /// this method can be useful if SDK provides an execute(...) method which run all statements from the provided input in sequence
    pub fn prepare_first(
        &self,
        sql: impl AsRef<str>,
    ) -> Result<Option<(Box<TursoStatement>, usize)>, TursoError> {
        match self.connection.consume_stmt(sql) {
            Ok(Some((statement, position))) => Ok(Some((
                Box::new(TursoStatement {
                    async_io: self.async_io,
                    concurrent_guard: Arc::new(ConcurrentGuard::new()),
                    statement,
                }),
                position,
            ))),
            Ok(None) => Ok(None),
            Err(err) => Err(turso_error_from_limbo_error(err)),
        }
    }

    /// close the connection preventing any further operations executed over it
    /// SAFETY: caller must guarantee that no ongoing operations are running over connection before calling close(...) method
    pub fn close(&self) -> Result<(), TursoError> {
        self.connection
            .close()
            .map_err(turso_error_from_limbo_error)
    }

    /// helper method to get C raw container to the TursoConnection instance
    /// this method is used in the capi wrappers
    pub fn to_capi(self: Arc<Self>) -> *mut capi::c::turso_connection_t {
        Arc::into_raw(self) as *mut capi::c::turso_connection_t
    }

    /// helper method to restore TursoConnection ref from C raw container
    /// this method is used in the capi wrappers
    ///
    /// # Safety
    /// value must be a pointer returned from [Self::to_capi] method
    pub unsafe fn ref_from_capi<'a>(
        value: *const capi::c::turso_connection_t,
    ) -> Result<&'a Self, TursoError> {
        if value.is_null() {
            Err(TursoError {
                code: TursoStatusCode::Misuse,
                message: Some("got null pointer".to_string()),
            })
        } else {
            Ok(&*(value as *const Self))
        }
    }

    /// helper method to restore TursoConnection instance from C raw container
    /// this method is used in the capi wrappers
    ///
    /// # Safety
    /// value must be a pointer returned from [Self::to_capi] method
    pub unsafe fn arc_from_capi(value: *const capi::c::turso_connection_t) -> Arc<Self> {
        Arc::from_raw(value as *const Self)
    }
}

pub struct TursoStatement {
    async_io: bool,
    concurrent_guard: Arc<ConcurrentGuard>,
    statement: Statement,
}

#[derive(Debug)]
pub struct TursoExecutionResult {
    pub status: TursoStatusCode,
    pub rows_changed: u64,
}

impl TursoStatement {
    /// returns parameters count for the statement
    pub fn parameters_count(&self) -> usize {
        self.statement.parameters_count()
    }
    /// binds positional parameter at the corresponding index (1-based)
    pub fn bind_positional(
        &mut self,
        index: usize,
        value: turso_core::Value,
    ) -> Result<(), TursoError> {
        let Ok(index) = index.try_into() else {
            return Err(TursoError {
                code: TursoStatusCode::Misuse,
                message: Some("bind index must be non-zero".to_string()),
            });
        };
        // bind_at is safe to call with any index as it will put pair (index, value) into the map
        self.statement.bind_at(index, value);
        Ok(())
    }
    /// named parameter position (name MUST omit named-parameter control character, e.g. '@', '$' or ':')
    pub fn named_position(&mut self, name: impl AsRef<str>) -> Result<usize, TursoError> {
        let parameters = self.statement.parameters();
        for i in 1..parameters.next_index().get() {
            // i is positive - so conversion to NonZero<> type will always succeed
            let index = i.try_into().unwrap();
            let Some(parameter) = parameters.name(index) else {
                continue;
            };
            if !(parameter.starts_with(":")
                || parameter.starts_with("@")
                || parameter.starts_with("$")
                || parameter.starts_with("?"))
            {
                return Err(TursoError {
                    code: TursoStatusCode::Error,
                    message: Some(format!(
                        "internal error: unexpected internal parameter name: {parameter}"
                    )),
                });
            }
            if name.as_ref() == &parameter[1..] {
                return Ok(index.into());
            }
        }

        Err(TursoError {
            code: TursoStatusCode::Error,
            message: Some(format!("named parameter {} not found", name.as_ref())),
        })
    }
    /// make one execution step of the statement
    /// method returns [TursoStatusCode::Done] if execution is finished
    /// method returns [TursoStatusCode::Row] if execution generated a row
    /// method returns [TursoStatusCode::Io] if async_io was set and execution needs IO in order to make progress
    pub fn step(&mut self) -> Result<TursoStatusCode, TursoError> {
        let guard = self.concurrent_guard.clone();
        let _guard = guard.try_use()?;
        self.step_no_guard()
    }

    fn step_no_guard(&mut self) -> Result<TursoStatusCode, TursoError> {
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
    /// execute statement to completion
    /// method returns [TursoStatusCode::Done] if execution completed
    /// method returns [TursoStatusCode::Io] if async_io was set and execution needs IO in order to make progress
    pub fn execute(&mut self) -> Result<TursoExecutionResult, TursoError> {
        let guard = self.concurrent_guard.clone();
        let _guard = guard.try_use()?;

        loop {
            let status = self.step_no_guard()?;
            if status == TursoStatusCode::Row {
                continue;
            } else if status == TursoStatusCode::Io {
                return Ok(TursoExecutionResult {
                    status,
                    rows_changed: 0,
                });
            } else if status == TursoStatusCode::Done {
                return Ok(TursoExecutionResult {
                    status: TursoStatusCode::Done,
                    rows_changed: self.statement.n_change() as u64,
                });
            }
            return Err(TursoError {
                code: TursoStatusCode::Error,
                message: Some(format!(
                    "internal error: unexpected status code: {status:?}",
                )),
            });
        }
    }
    /// run iteration of the IO backend
    pub fn run_io(&self) -> Result<(), TursoError> {
        self.statement
            .run_once()
            .map_err(turso_error_from_limbo_error)
    }
    /// get row value reference currently pointed by the statement
    /// note, that this row will no longer be valid after execution of methods like [Self::step]/[Self::execute]/[Self::finalize]/[Self::reset]
    pub fn row_value(&self, index: usize) -> Result<turso_core::ValueRef, TursoError> {
        let Some(row) = self.statement.row() else {
            return Err(TursoError {
                code: TursoStatusCode::Misuse,
                message: Some("statement holds no row".to_string()),
            });
        };
        if index >= row.len() {
            return Err(TursoError {
                code: TursoStatusCode::Misuse,
                message: Some("attempt to access row value out of bounds".to_string()),
            });
        }
        let value = row.get_value(index);
        Ok(value.as_value_ref())
    }
    /// returns column count
    pub fn column_count(&self) -> usize {
        self.statement.num_columns()
    }
    /// returns column name
    pub fn column_name(&self, index: usize) -> Result<Cow<'_, str>, TursoError> {
        if index >= self.column_count() {
            return Err(TursoError {
                code: TursoStatusCode::Misuse,
                message: Some("column index out of bounds".to_string()),
            });
        }
        Ok(self.statement.get_column_name(index))
    }
    /// finalize statement execution
    /// this method must be called in the end of statement execution (either successfull or not)
    pub fn finalize(&mut self) -> Result<TursoStatusCode, TursoError> {
        let guard = self.concurrent_guard.clone();
        let _guard = guard.try_use()?;

        while self.statement.execution_state().is_running() {
            let status = self.step_no_guard()?;
            if status == TursoStatusCode::Io {
                return Ok(status);
            }
        }
        Ok(TursoStatusCode::Ok)
    }
    /// reset internal statement state and bindings
    pub fn reset(&mut self) -> Result<(), TursoError> {
        self.statement.reset();
        self.statement.clear_bindings();
        Ok(())
    }

    /// helper method to get C raw container to the TursoStatement instance
    /// this method is used in the capi wrappers
    pub fn to_capi(self: Box<Self>) -> *mut capi::c::turso_statement_t {
        Box::into_raw(self) as *mut capi::c::turso_statement_t
    }

    /// helper method to restore TursoStatement ref from C raw container
    /// this method is used in the capi wrappers
    ///
    /// # Safety
    /// value must be a pointer returned from [Self::to_capi] method
    pub unsafe fn ref_from_capi<'a>(
        value: *const capi::c::turso_statement_t,
    ) -> Result<&'a mut Self, TursoError> {
        if value.is_null() {
            Err(TursoError {
                code: TursoStatusCode::Misuse,
                message: Some("got null pointer".to_string()),
            })
        } else {
            Ok(&mut *(value as *mut Self))
        }
    }

    /// helper method to restore TursoStatement instance from C raw container
    /// this method is used in the capi wrappers
    ///
    /// # Safety
    /// value must be a pointer returned from [Self::to_capi] method
    pub unsafe fn box_from_capi(value: *const capi::c::turso_statement_t) -> Box<Self> {
        Box::from_raw(value as *mut Self)
    }
}

#[cfg(test)]
mod tests {
    use crate::rsapi::{TursoDatabase, TursoDatabaseConfig, TursoStatusCode};

    #[test]
    pub fn test_db_concurrent_use() {
        let db = TursoDatabase::new(TursoDatabaseConfig {
            path: ":memory:".to_string(),
            experimental_features: None,
            async_io: false,
            io: None,
            db_file: None,
        });
        db.open().unwrap();
        let conn = db.connect().unwrap();
        let stmt1 = conn
            .prepare_single("SELECT * FROM generate_series(1, 10000)")
            .unwrap();
        let stmt2 = conn
            .prepare_single("SELECT * FROM generate_series(1, 10000)")
            .unwrap();

        let mut threads = Vec::new();
        for mut stmt in [stmt1, stmt2] {
            let thread = std::thread::spawn(move || stmt.execute());
            threads.push(thread);
        }
        let mut results = Vec::new();
        for thread in threads {
            results.push(thread.join().unwrap());
        }
        assert!(
            results[0].is_err() && results[1].is_ok() || results[0].is_ok() && results[1].is_err()
        );
    }

    #[test]
    pub fn test_db_rsapi_use() {
        let db = TursoDatabase::new(TursoDatabaseConfig {
            path: ":memory:".to_string(),
            experimental_features: None,
            async_io: false,
            io: None,
            db_file: None,
        });
        db.open().unwrap();
        let conn = db.connect().unwrap();
        let mut stmt = conn
            .prepare_single("SELECT * FROM generate_series(1, 10000)")
            .unwrap();
        assert_eq!(stmt.execute().unwrap().status, TursoStatusCode::Done);
    }
}
