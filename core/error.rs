use thiserror::Error;

use crate::storage::page_cache::CacheError;

#[derive(Debug, Clone, Error, miette::Diagnostic)]
pub enum LimboError {
    #[error("Corrupt database: {0}")]
    Corrupt(String),
    #[error("File is not a database")]
    NotADB,
    #[error("Internal error: {0}")]
    InternalError(String),
    #[error(transparent)]
    CacheError(#[from] CacheError),
    #[error("Database is full: {0}")]
    DatabaseFull(String),
    #[error("Parse error: {0}")]
    ParseError(String),
    #[error(transparent)]
    #[diagnostic(transparent)]
    LexerError(#[from] turso_parser::error::Error),
    #[error("Conversion error: {0}")]
    ConversionError(String),
    #[error("Env variable error: {0}")]
    EnvVarError(#[from] std::env::VarError),
    #[error("Transaction error: {0}")]
    TxError(String),
    #[error(transparent)]
    CompletionError(#[from] CompletionError),
    #[error("Locking error: {0}")]
    LockingError(String),
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
    #[error("Runtime error: database table is locked")]
    TableLocked,
    #[error("Error: Resource is read-only")]
    ReadOnly,
    #[error("Database is busy")]
    Busy,
    #[error("Conflict: {0}")]
    Conflict(String),
    #[error("Database schema changed")]
    SchemaUpdated,
    #[error(
        "Database is empty, header does not exist - page 1 should've been allocated before this"
    )]
    Page1NotAlloc,
    #[error("Transaction terminated")]
    TxTerminated,
    #[error("Write-write conflict")]
    WriteWriteConflict,
    #[error("No such transaction ID: {0}")]
    NoSuchTransactionID(String),
    #[error("Null value")]
    NullValue,
    #[error("invalid column type")]
    InvalidColumnType,
    #[error("Invalid blob size, expected {0}")]
    InvalidBlobSize(usize),
    #[error("Planning error: {0}")]
    PlanningError(String),
}

// We only propagate the error kind so we can avoid string allocation in hot path and copying/cloning enums is cheaper
impl From<std::io::Error> for LimboError {
    fn from(value: std::io::Error) -> Self {
        Self::CompletionError(CompletionError::IOError(value.kind()))
    }
}

#[cfg(target_family = "unix")]
impl From<rustix::io::Errno> for LimboError {
    fn from(value: rustix::io::Errno) -> Self {
        CompletionError::from(value).into()
    }
}

#[cfg(all(target_os = "linux", feature = "io_uring"))]
impl From<&'static str> for LimboError {
    fn from(value: &'static str) -> Self {
        CompletionError::UringIOError(value).into()
    }
}

// We only propagate the error kind
impl From<std::io::Error> for CompletionError {
    fn from(value: std::io::Error) -> Self {
        CompletionError::IOError(value.kind())
    }
}

#[derive(Debug, Copy, Clone, Error)]
pub enum CompletionError {
    #[error("I/O error: {0}")]
    IOError(std::io::ErrorKind),
    #[cfg(target_family = "unix")]
    #[error("I/O error: {0}")]
    RustixIOError(#[from] rustix::io::Errno),
    #[cfg(all(target_os = "linux", feature = "io_uring"))]
    #[error("I/O error: {0}")]
    // TODO: if needed create an enum for IO Uring errors so that we don't have to pass strings around
    UringIOError(&'static str),
    #[error("Completion was aborted")]
    Aborted,
    #[error("Decryption failed for page={page_idx}")]
    DecryptionError { page_idx: usize },
    #[error("I/O error: partial write")]
    ShortWrite,
    #[error("Checksum mismatch on page {page_id}: expected {expected}, got {actual}")]
    ChecksumMismatch {
        page_id: usize,
        expected: u64,
        actual: u64,
    },
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
pub const SQLITE_FULL: usize = 13; // we want this in autoincrement - incase if user inserts max allowed int
pub const SQLITE_CONSTRAINT_UNIQUE: usize = 2067;
