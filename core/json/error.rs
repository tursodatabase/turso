use std::fmt::{self, Display};

/// Alias for a `Result` with error type `json5::Error`
pub type Result<T> = std::result::Result<T, Error>;

/// A bare bones error type which currently just collapses all the underlying errors in to a single
/// string... This is fine for displaying to the user, but not very useful otherwise. Work to be
/// done here.
#[derive(Clone, Debug, PartialEq)]
pub enum Error {
    /// Just shove everything in a single variant for now.
    Message {
        /// The error message.
        msg: String,
        /// The location of the error, if applicable.
        location: Option<usize>,
    },
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self::Message {
            msg: err.to_string(),
            location: None,
        }
    }
}

impl From<std::str::Utf8Error> for Error {
    fn from(err: std::str::Utf8Error) -> Self {
        Self::Message {
            msg: err.to_string(),
            location: None,
        }
    }
}

impl Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Message { ref msg, .. } => write!(formatter, "{msg}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<Error> for crate::LimboError {
    fn from(err: Error) -> Self {
        match err {
            Error::Message { msg, .. } => crate::LimboError::ParseError(msg),
        }
    }
}
