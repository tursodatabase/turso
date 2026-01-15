use std::{
    borrow::Cow,
    collections::HashMap,
    fmt::Display,
    sync::{Arc, Mutex, Once, RwLock},
    task::Waker,
    time::Duration,
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
    DatabaseStorage, LimboError, OpenFlags, Pager, QueryMode, Statement, StepResult, IO,
};

use crate::{
    assert_send, assert_sync,
    capi::{self, c},
    ConcurrentGuard,
};

assert_send!(TursoDatabase, TursoConnection, TursoStatement);
assert_sync!(TursoDatabase);

pub use turso_core::types::FromValue;
pub type EncryptionOpts = turso_core::EncryptionOpts;
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
            return Err(TursoError::Misuse(
                "config pointer must be not null".to_string(),
            ));
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

    /// optional encryption parameters for local data encryption
    /// as encryption is experimental - [Self::experimental_features] must have "encryption" in the list
    pub encryption: Option<EncryptionOpts>,

    /// optional VFS parameter explicitly specifying FS backend for the database.
    /// Available options are:
    /// - "memory": in-memory backend
    /// - "syscall": generic syscall backend
    /// - "io_uring": IO uring (supported only on Linux)
    pub vfs: Option<String>,

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

pub fn turso_slice_null() -> capi::c::turso_slice_ref_t {
    capi::c::turso_slice_ref_t {
        ptr: std::ptr::null(),
        len: 0,
    }
}

/// # Safety
/// ptr must be valid C-string pointer or null
pub unsafe fn str_from_c_str<'a>(ptr: *const std::ffi::c_char) -> Result<&'a str, TursoError> {
    if ptr.is_null() {
        return Err(TursoError::Misuse(
            "expected zero terminated c string, got null pointer".to_string(),
        ));
    }
    let c_str = std::ffi::CStr::from_ptr(ptr);
    match c_str.to_str() {
        Ok(s) => Ok(s),
        Err(err) => Err(TursoError::Misuse(format!(
            "expected zero terminated c-string representing utf-8 value: {err}"
        ))),
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
        Err(err) => Err(TursoError::Misuse(format!(
            "expected string slice representing utf-8 value: {err}"
        ))),
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
        return Err(TursoError::Misuse(
            "expected slice, got null pointer".to_string(),
        ));
    }
    Ok(std::slice::from_raw_parts(ptr as *const u8, len))
}

/// SAFETY: slice must points to the valid memory
pub fn bytes_from_turso_slice<'a>(
    slice: capi::c::turso_slice_ref_t,
) -> Result<&'a [u8], TursoError> {
    if slice.ptr.is_null() {
        return Err(TursoError::Misuse(
            "expected slice representing utf-8 value, got null".to_string(),
        ));
    }
    Ok(unsafe { std::slice::from_raw_parts(slice.ptr as *const u8, slice.len) })
}

/// SAFETY: slice must points to the valid memory
pub fn str_from_turso_slice<'a>(slice: capi::c::turso_slice_ref_t) -> Result<&'a str, TursoError> {
    if slice.ptr.is_null() {
        return Err(TursoError::Misuse(
            "expected slice representing utf-8 value, got null".to_string(),
        ));
    }
    let s = unsafe { std::slice::from_raw_parts(slice.ptr as *const u8, slice.len) };
    match std::str::from_utf8(s) {
        Ok(s) => Ok(s),
        Err(err) => Err(TursoError::Misuse(format!(
            "expected slice representing utf-8 value: {err}"
        ))),
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
            return Err(TursoError::Misuse(
                "config pointer must be not null".to_string(),
            ));
        }
        let config = *config;
        let encryption_cipher = if !config.encryption_cipher.is_null() {
            Some(str_from_c_str(config.encryption_cipher)?.to_string())
        } else {
            None
        };
        let encryption_hexkey = if !config.encryption_hexkey.is_null() {
            Some(str_from_c_str(config.encryption_hexkey)?.to_string())
        } else {
            None
        };
        if encryption_cipher.is_some() != encryption_hexkey.is_some() {
            return Err(TursoError::Misuse(
                "either both encryption cipher and key must be set or no".to_string(),
            ));
        }
        Ok(Self {
            path: str_from_c_str(config.path)?.to_string(),
            experimental_features: if !config.experimental_features.is_null() {
                Some(str_from_c_str(config.experimental_features)?.to_string())
            } else {
                None
            },
            async_io: config.async_io != 0,
            encryption: encryption_cipher.map(|encryption_cipher| EncryptionOpts {
                cipher: encryption_cipher,
                hexkey: encryption_hexkey.unwrap(),
            }),
            vfs: if !config.vfs.is_null() {
                Some(str_from_c_str(config.vfs)?.to_string())
            } else {
                None
            },
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
    Done,
    Row,
    Io,
}

#[derive(Debug, Clone)]
pub enum TursoError {
    Busy(String),
    BusySnapshot(String),
    Interrupt(String),
    Error(String),
    Misuse(String),
    Constraint(String),
    Readonly(String),
    DatabaseFull(String),
    NotAdb(String),
    Corrupt(String),
    IoError(std::io::ErrorKind),
}

impl TursoStatusCode {
    pub fn to_capi(self) -> capi::c::turso_status_code_t {
        match self {
            TursoStatusCode::Done => capi::c::turso_status_code_t::TURSO_DONE,
            TursoStatusCode::Row => capi::c::turso_status_code_t::TURSO_ROW,
            TursoStatusCode::Io => capi::c::turso_status_code_t::TURSO_IO,
        }
    }
}

impl TursoError {
    /// # Safety
    /// error_opt_out must be a valid pointer or null
    pub unsafe fn to_capi(
        &self,
        error_opt_out: *mut *const std::ffi::c_char,
    ) -> capi::c::turso_status_code_t {
        if !error_opt_out.is_null() {
            let message = str_to_c_string(&self.to_string());
            unsafe { *error_opt_out = message };
        }
        self.to_capi_code()
    }
    pub fn to_capi_code(&self) -> capi::c::turso_status_code_t {
        match self {
            TursoError::Busy(_) => capi::c::turso_status_code_t::TURSO_BUSY,
            TursoError::BusySnapshot(_) => capi::c::turso_status_code_t::TURSO_BUSY_SNAPSHOT,
            TursoError::Interrupt(_) => capi::c::turso_status_code_t::TURSO_INTERRUPT,
            TursoError::Error(_) => capi::c::turso_status_code_t::TURSO_ERROR,
            TursoError::Misuse(_) => capi::c::turso_status_code_t::TURSO_MISUSE,
            TursoError::Constraint(_) => capi::c::turso_status_code_t::TURSO_CONSTRAINT,
            TursoError::Readonly(_) => capi::c::turso_status_code_t::TURSO_READONLY,
            TursoError::DatabaseFull(_) => capi::c::turso_status_code_t::TURSO_DATABASE_FULL,
            TursoError::NotAdb(_) => capi::c::turso_status_code_t::TURSO_NOTADB,
            TursoError::Corrupt(_) => capi::c::turso_status_code_t::TURSO_CORRUPT,
            TursoError::IoError(_) => capi::c::turso_status_code_t::TURSO_IOERR,
        }
    }
}

impl Display for TursoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TursoError::Busy(s)
            | TursoError::BusySnapshot(s)
            | TursoError::Interrupt(s)
            | TursoError::Error(s)
            | TursoError::Misuse(s)
            | TursoError::Constraint(s)
            | TursoError::Readonly(s)
            | TursoError::DatabaseFull(s)
            | TursoError::NotAdb(s)
            | TursoError::Corrupt(s) => f.write_str(s),
            TursoError::IoError(kind) => write!(f, "I/O error: {kind}"),
        }
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

impl From<LimboError> for TursoError {
    fn from(value: LimboError) -> Self {
        match value {
            LimboError::Constraint(e) => TursoError::Constraint(e),
            LimboError::Corrupt(e) => TursoError::Corrupt(e),
            LimboError::NotADB => TursoError::NotAdb("file is not a database".to_string()),
            LimboError::DatabaseFull(e) => TursoError::DatabaseFull(e),
            LimboError::ReadOnly => TursoError::Readonly("database is readonly".to_string()),
            LimboError::Busy => TursoError::Busy("database is locked".to_string()),
            LimboError::BusySnapshot => TursoError::BusySnapshot(
                "database snapshot is stale, rollback and retry the transaction".to_string(),
            ),
            LimboError::CompletionError(turso_core::CompletionError::IOError(kind)) => {
                TursoError::IoError(kind)
            }
            _ => TursoError::Error(value.to_string()),
        }
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
            _ => return Err(TursoError::Error("unknown log level".to_string())),
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
            None => Err(TursoError::Misuse("database must be opened".to_string())),
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
            return Err(TursoError::Misuse(
                "database must be opened only once".to_string(),
            ));
        }
        let io: Arc<dyn turso_core::IO> = if let Some(io) = &self.config.io {
            io.clone()
        } else {
            match self.config.vfs.as_deref() {
                Some("memory") => Arc::new(turso_core::MemoryIO::new()),
                Some("syscall") => {
                    #[cfg(all(target_family = "unix", not(miri)))]
                    {
                        Arc::new(turso_core::UnixIO::new().map_err(|e| {
                            TursoError::Error(format!(
                                "unable to create generic syscall backend: {e}"
                            ))
                        })?)
                    }
                    #[cfg(any(not(target_family = "unix"), miri))]
                    {
                        Arc::new(turso_core::PlatformIO::new().map_err(|e| {
                            TursoError::Error(format!(
                                "unable to create generic syscall backend: {e}"
                            ))
                        })?)
                    }
                }
                #[cfg(all(target_os = "linux", not(miri)))]
                Some("io_uring") => Arc::new(turso_core::UringIO::new().map_err(|e| {
                    TursoError::Error(format!("unable to create io_uring backend: {e}"))
                })?),
                #[cfg(any(not(target_os = "linux"), miri))]
                Some("io_uring") => {
                    return Err(TursoError::Error(
                        "io_uring is only available on Linux targets".to_string(),
                    ));
                }
                Some(vfs) => {
                    return Err(TursoError::Error(format!(
                        "unsupported VFS backend: '{vfs}'"
                    )))
                }
                None => match self.config.path.as_str() {
                    ":memory:" => Arc::new(turso_core::MemoryIO::new()),
                    _ => Arc::new(turso_core::PlatformIO::new()?),
                },
            }
        };
        let open_flags = OpenFlags::default();
        let db_file = if let Some(db_file) = &self.config.db_file {
            db_file.clone()
        } else {
            let file = io.open_file(&self.config.path, open_flags, true)?;
            Arc::new(DatabaseFile::new(file))
        };
        let mut opts = DatabaseOpts::new();
        if let Some(experimental_features) = &self.config.experimental_features {
            for features in experimental_features.split(",").map(|s| s.trim()) {
                opts = match features {
                    "views" => opts.with_views(true),
                    "index_method" => opts.with_index_method(true),
                    "strict" => opts.with_strict(true),
                    "autovacuum" => opts.with_autovacuum(true),
                    "triggers" => opts.with_triggers(true),
                    "encryption" => opts.with_encryption(true),
                    _ => opts,
                };
            }
        }
        if self.config.encryption.is_some() && !opts.enable_encryption {
            return Err(TursoError::Error("encryption is experimental and must be explicitly enabled through experimental features list".to_string()));
        }
        let db = turso_core::Database::open_with_flags(
            io.clone(),
            &self.config.path,
            db_file,
            open_flags,
            opts,
            self.config.encryption.clone(),
        )?;
        *inner_db = Some(db);
        Ok(())
    }

    /// creates database connection
    /// database must be already opened with [Self::open] method
    pub fn connect(&self) -> Result<Arc<TursoConnection>, TursoError> {
        let inner_db = self.db.lock().unwrap();
        let Some(db) = inner_db.as_ref() else {
            return Err(TursoError::Misuse(
                "database must be opened first".to_string(),
            ));
        };
        let connection = db.connect()?;
        Ok(TursoConnection::new(&self.config, connection))
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
            Err(TursoError::Misuse("got null pointer".to_string()))
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

struct CachedStatement {
    program: turso_core::Program,
    pager: Arc<Pager>,
    query_mode: QueryMode,
}

#[derive(Clone)]
pub struct TursoConnection {
    async_io: bool,
    concurrent_guard: Arc<ConcurrentGuard>,
    connection: Arc<Connection>,
    cached_statements: Arc<Mutex<HashMap<String, Arc<CachedStatement>>>>,
}

impl TursoConnection {
    pub fn new(config: &TursoDatabaseConfig, connection: Arc<Connection>) -> Arc<Self> {
        Arc::new(Self {
            async_io: config.async_io,
            connection,
            concurrent_guard: Arc::new(ConcurrentGuard::new()),
            cached_statements: Arc::new(Mutex::new(HashMap::new())),
        })
    }
    /// Set busy timeout for the connection
    pub fn set_busy_timeout(&self, duration: Duration) {
        self.connection.set_busy_timeout(duration);
    }
    pub fn get_auto_commit(&self) -> bool {
        self.connection.get_auto_commit()
    }
    pub fn last_insert_rowid(&self) -> i64 {
        self.connection.last_insert_rowid()
    }

    /// prepares single SQL statement
    pub fn prepare_single(&self, sql: impl AsRef<str>) -> Result<Box<TursoStatement>, TursoError> {
        let statement = self.connection.prepare(sql)?;
        Ok(Box::new(TursoStatement {
            concurrent_guard: self.concurrent_guard.clone(),
            async_io: self.async_io,
            statement,
        }))
    }

    /// Prepare a statement from the provided SQL string and cache it for future use.
    pub fn prepare_cached(&self, sql: impl AsRef<str>) -> Result<Box<TursoStatement>, TursoError> {
        let sql_str = sql.as_ref();

        // Check if we have a cached version
        if let Some(cached) = self.cached_statements.lock().unwrap().get(sql_str) {
            // Clone the cached program and create a new Statement with fresh state
            let statement = Statement::new(
                cached.program.clone(),
                cached.pager.clone(),
                cached.query_mode,
            );
            return Ok(Box::new(TursoStatement {
                concurrent_guard: self.concurrent_guard.clone(),
                async_io: self.async_io,
                statement,
            }));
        }

        // Not cached, prepare it fresh
        let statement = self.connection.prepare(sql_str)?;

        // Cache it for future use
        let cached = Arc::new(CachedStatement {
            program: statement.get_program().clone(),
            pager: statement.get_pager().clone(),
            query_mode: statement.get_query_mode(),
        });
        self.cached_statements
            .lock()
            .unwrap()
            .insert(sql_str.to_string(), cached);

        Ok(Box::new(TursoStatement {
            concurrent_guard: self.concurrent_guard.clone(),
            async_io: self.async_io,
            statement,
        }))
    }

    /// prepares first SQL statement from the string and return prepared statement and position after the end of the parsed statement
    /// this method can be useful if SDK provides an execute(...) method which run all statements from the provided input in sequence
    pub fn prepare_first(
        &self,
        sql: impl AsRef<str>,
    ) -> Result<Option<(Box<TursoStatement>, usize)>, TursoError> {
        match self.connection.consume_stmt(sql)? {
            Some((statement, position)) => Ok(Some((
                Box::new(TursoStatement {
                    async_io: self.async_io,
                    concurrent_guard: Arc::new(ConcurrentGuard::new()),
                    statement,
                }),
                position,
            ))),
            None => Ok(None),
        }
    }

    /// close the connection preventing any further operations executed over it
    /// SAFETY: caller must guarantee that no ongoing operations are running over connection before calling close(...) method
    pub fn close(&self) -> Result<(), TursoError> {
        self.connection.close()?;
        Ok(())
    }

    /// low-level method used only by the Rust SDK
    pub fn cacheflush(&self) -> Result<(), TursoError> {
        let completions = self.connection.cacheflush()?;
        let pager = self.connection.get_pager();
        for c in completions {
            pager.io.wait_for_completion(c)?;
        }
        Ok(())
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
            Err(TursoError::Misuse("got null pointer".to_string()))
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

#[derive(Debug, Clone)]
pub struct TursoExecutionResult {
    pub status: TursoStatusCode,
    pub rows_changed: u64,
}

impl TursoStatement {
    /// return amount of row modifications (insert/delete operations) made by the most recent executed statement
    pub fn n_change(&self) -> i64 {
        self.statement.n_change()
    }
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
            return Err(TursoError::Misuse(
                "bind index must be non-zero".to_string(),
            ));
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
                return Err(TursoError::Error(format!(
                    "internal error: unexpected internal parameter name: {parameter}"
                )));
            }
            if name.as_ref() == parameter {
                return Ok(index.into());
            }
        }

        Err(TursoError::Error(format!(
            "named parameter {} not found",
            name.as_ref()
        )))
    }
    /// make one execution step of the statement
    /// method returns [TursoStatusCode::Done] if execution is finished
    /// method returns [TursoStatusCode::Row] if execution generated a row
    /// method returns [TursoStatusCode::Io] if async_io was set and execution needs IO in order to make progress
    pub fn step(&mut self, waker: Option<&Waker>) -> Result<TursoStatusCode, TursoError> {
        let guard = self.concurrent_guard.clone();
        let _guard = guard.try_use()?;
        self.step_no_guard(waker)
    }

    fn step_no_guard(&mut self, waker: Option<&Waker>) -> Result<TursoStatusCode, TursoError> {
        let async_io = self.async_io;
        loop {
            let result = if let Some(waker) = waker {
                self.statement.step_with_waker(waker)
            } else {
                self.statement.step()
            };
            return match result? {
                StepResult::Done => Ok(TursoStatusCode::Done),
                StepResult::Row => Ok(TursoStatusCode::Row),
                StepResult::Busy => Err(TursoError::Busy("database is locked".to_string())),
                StepResult::Interrupt => Err(TursoError::Interrupt("interrupted".to_string())),
                StepResult::IO => {
                    if async_io {
                        Ok(TursoStatusCode::Io)
                    } else {
                        self.run_io()?;
                        continue;
                    }
                }
            };
        }
    }
    /// execute statement to completion
    /// method returns [TursoStatusCode::Done] if execution completed
    /// method returns [TursoStatusCode::Io] if async_io was set and execution needs IO in order to make progress
    pub fn execute(&mut self, waker: Option<&Waker>) -> Result<TursoExecutionResult, TursoError> {
        let guard = self.concurrent_guard.clone();
        let _guard = guard.try_use()?;

        loop {
            let status = self.step_no_guard(waker)?;
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
            return Err(TursoError::Error(format!(
                "internal error: unexpected status code: {status:?}",
            )));
        }
    }
    /// run iteration of the IO backend
    pub fn run_io(&self) -> Result<(), TursoError> {
        self.statement._io().step()?;
        Ok(())
    }
    /// get row value reference currently pointed by the statement
    /// note, that this row will no longer be valid after execution of methods like [Self::step]/[Self::execute]/[Self::finalize]/[Self::reset]
    pub fn row_value(&self, index: usize) -> Result<turso_core::ValueRef, TursoError> {
        let Some(row) = self.statement.row() else {
            return Err(TursoError::Misuse("statement holds no row".to_string()));
        };
        if index >= row.len() {
            return Err(TursoError::Misuse(
                "attempt to access row value out of bounds".to_string(),
            ));
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
            return Err(TursoError::Misuse("column index out of bounds".to_string()));
        }
        Ok(self.statement.get_column_name(index))
    }
    /// finalize statement execution
    /// this method must be called in the end of statement execution (either successfull or not)
    pub fn finalize(&mut self, waker: Option<&Waker>) -> Result<TursoStatusCode, TursoError> {
        let guard = self.concurrent_guard.clone();
        let _guard = guard.try_use()?;

        while self.statement.execution_state().is_running() {
            let status = self.step_no_guard(waker)?;
            if status == TursoStatusCode::Io {
                return Ok(status);
            }
        }
        Ok(TursoStatusCode::Done)
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
            Err(TursoError::Misuse("got null pointer".to_string()))
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
    use crate::rsapi::{TursoDatabase, TursoDatabaseConfig, TursoError, TursoStatusCode};

    #[test]
    pub fn test_db_concurrent_use() {
        use std::sync::{Arc, Barrier};

        let mut errors = Vec::new();
        for _ in 0..16 {
            let db = TursoDatabase::new(TursoDatabaseConfig {
                path: ":memory:".to_string(),
                experimental_features: None,
                async_io: false,
                encryption: None,
                vfs: None,
                io: None,
                db_file: None,
            });
            db.open().unwrap();
            let conn = db.connect().unwrap();
            let stmt1 = conn
                .prepare_single("SELECT * FROM generate_series(1, 100000)")
                .unwrap();
            let stmt2 = conn
                .prepare_single("SELECT * FROM generate_series(1, 100000)")
                .unwrap();

            // Use a barrier to ensure both threads start executing at the same time
            let barrier = Arc::new(Barrier::new(2));
            let mut threads = Vec::new();
            for mut stmt in [stmt1, stmt2] {
                let barrier_clone = Arc::clone(&barrier);
                let thread = std::thread::spawn(move || {
                    barrier_clone.wait();
                    stmt.execute(None)
                });
                threads.push(thread);
            }
            let mut results = Vec::new();
            for thread in threads {
                results.push(thread.join().unwrap());
            }
            assert!(
                !(results[0].is_err() && results[1].is_err()),
                "results: {results:?}",
            );
            if results[0].is_err() || results[1].is_err() {
                errors.push(
                    results[0]
                        .clone()
                        .err()
                        .or(results[1].clone().err())
                        .unwrap(),
                );
            }
        }
        println!("{errors:?}");
        assert!(
            !errors.is_empty(),
            "misuse errors should be very likely with the test setup: {errors:?}"
        );
        assert!(
            errors.iter().all(|e| matches!(e, TursoError::Misuse(_))),
            "all errors must have Misuse code: {errors:?}"
        );
    }

    #[test]
    pub fn test_db_rsapi_use() {
        let db = TursoDatabase::new(TursoDatabaseConfig {
            path: ":memory:".to_string(),
            experimental_features: None,
            async_io: false,
            encryption: None,
            vfs: None,
            io: None,
            db_file: None,
        });
        db.open().unwrap();
        let conn = db.connect().unwrap();
        let mut stmt = conn
            .prepare_single("SELECT * FROM generate_series(1, 10000)")
            .unwrap();
        assert_eq!(stmt.execute(None).unwrap().status, TursoStatusCode::Done);
    }
}
