use crate::protocol::ProtoError;

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Errors returned by the serverless driver.
///
/// Mirrors the embedded `turso::Error` so code can switch between the
/// embedded and serverless drivers. SQL errors reported by the server are
/// mapped onto the matching variant from their protocol error code
/// (section 9.2 of the protocol specification).
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("SQL conversion failure: `{0}`")]
    ToSqlConversionFailure(BoxError),
    #[error("Query returned no rows")]
    QueryReturnedNoRows,
    #[error("Conversion failure: `{0}`")]
    ConversionFailure(String),
    #[error("{0}")]
    Busy(String),
    #[error("{0}")]
    Interrupt(String),
    #[error("{0}")]
    Error(String),
    #[error("{0}")]
    Misuse(String),
    #[error("{0}")]
    Constraint(String),
    #[error("{0}")]
    Readonly(String),
    #[error("{0}")]
    DatabaseFull(String),
    #[error("{0}")]
    NotAdb(String),
    #[error("{0}")]
    Corrupt(String),
    /// The HTTP request itself failed: connection failure, timeout, or a
    /// non-200 response from the server (section 9.3).
    #[error("{0}")]
    Http(String),
}

impl From<ProtoError> for Error {
    fn from(e: ProtoError) -> Self {
        // Match on the code prefix: constraint violations may carry the
        // extended code (e.g. SQLITE_CONSTRAINT_UNIQUE) in `code`.
        let code = e.code.as_deref().unwrap_or("");
        if code.starts_with("SQLITE_CONSTRAINT") {
            Error::Constraint(e.message)
        } else if code.starts_with("SQLITE_BUSY") {
            Error::Busy(e.message)
        } else if code.starts_with("SQLITE_INTERRUPT") {
            Error::Interrupt(e.message)
        } else if code.starts_with("SQLITE_MISUSE") {
            Error::Misuse(e.message)
        } else if code.starts_with("SQLITE_READONLY") {
            Error::Readonly(e.message)
        } else if code.starts_with("SQLITE_FULL") {
            Error::DatabaseFull(e.message)
        } else if code.starts_with("SQLITE_NOTADB") {
            Error::NotAdb(e.message)
        } else if code.starts_with("SQLITE_CORRUPT") {
            Error::Corrupt(e.message)
        } else {
            Error::Error(e.message)
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
