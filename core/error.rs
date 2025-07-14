use crate::storage::pager::SpillFlag;
use thiserror::Error;

#[derive(Debug, Error, miette::Diagnostic)]
pub enum LimboError {
    #[error("Corrupt database: {0}")]
    Corrupt(String),
    #[error("File is not a database")]
    NotADB,
    #[error("Internal error: {0}")]
    InternalError(String),
    #[error("Page cache is full")]
    CacheFull,
    #[error("Database is full: {0}")]
    DatabaseFull(String),
    #[error("Parse error: {0}")]
    ParseError(String),
    #[error(transparent)]
    #[diagnostic(transparent)]
    LexerError(#[from] turso_sqlite3_parser::lexer::sql::Error),
    #[error("Conversion error: {0}")]
    ConversionError(String),
    #[error("Env variable error: {0}")]
    EnvVarError(#[from] std::env::VarError),
    #[error("Transaction error: {0}")]
    TxError(String),
    #[error("I/O error: {0}")]
    IOError(#[from] std::io::Error),
    #[cfg(all(target_os = "linux", feature = "io_uring"))]
    #[error("I/O error: {0}")]
    UringIOError(String),
    #[error("Locking error: {0}")]
    LockingError(String),
    #[cfg(target_family = "unix")]
    #[error("I/O error: {0}")]
    RustixIOError(#[from] rustix::io::Errno),
    #[error("Parse error: {0}")]
    ParseIntError(#[from] std::num::ParseIntError),
    #[error("Parse error: {0}")]
    ParseFloatError(#[from] std::num::ParseFloatError),
    #[error("Parse error: {0}")]
    InvalidDate(String),
    #[error("Parse error: {0}")]
    InvalidTime(String),
    #[error("Modifier parsing error: {0}")]
    InvalidModifier(String),
    #[error("Invalid argument supplied: {0}")]
    InvalidArgument(String),
    #[error("Invalid formatter supplied: {0}")]
    InvalidFormatter(String),
    #[error("Runtime error: {0}")]
    Constraint(String),
    #[error("Extension error: {0}")]
    ExtensionError(String),
    #[error("Runtime error: integer overflow")]
    IntegerOverflow,
    #[error("Schema is locked for write")]
    SchemaLocked,
    #[error("Database Connection is read-only")]
    ReadOnly,
    #[error("Database is busy")]
    Busy,
    #[error("Spill not allowed for flag: {0}")]
    SpillNotAllowed(SpillFlag),
}

#[macro_export]
macro_rules! bail_parse_error {
    ($($arg:tt)*) => {
        return Err($crate::error::LimboError::ParseError(format!($($arg)*)))
    };
}

#[macro_export]
macro_rules! bail_corrupt_error {
    ($($arg:tt)*) => {
        return Err($crate::error::LimboError::Corrupt(format!($($arg)*)))
    };
}

#[macro_export]
macro_rules! bail_constraint_error {
    ($($arg:tt)*) => {
        return Err($crate::error::LimboError::Constraint(format!($($arg)*)))
    };
}

impl From<turso_ext::ResultCode> for LimboError {
    fn from(err: turso_ext::ResultCode) -> Self {
        LimboError::ExtensionError(err.to_string())
    }
}

pub const SQLITE_CONSTRAINT: usize = 19;
pub const SQLITE_CONSTRAINT_PRIMARYKEY: usize = SQLITE_CONSTRAINT | (6 << 8);
pub const SQLITE_CONSTRAINT_NOTNULL: usize = SQLITE_CONSTRAINT | (5 << 8);
