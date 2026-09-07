use crate::raw;
use std::error;
use std::fmt;
use std::io;

/// A `Result` type alias for this crate's `Error` type.
pub type Result<T> = ::std::result::Result<T, Error>;

/// An error that encapsulates all possible errors in this crate.
#[derive(Debug)]
pub enum Error {
    /// An error that occurred while reading or writing a finite state
    /// transducer.
    Fst(raw::Error),
    /// An IO error that occurred while writing a finite state transducer.
    Io(io::Error),
}

impl From<io::Error> for Error {
    #[inline]
    fn from(err: io::Error) -> Error {
        Error::Io(err)
    }
}

impl From<raw::Error> for Error {
    #[inline]
    fn from(err: raw::Error) -> Error {
        Error::Fst(err)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use self::Error::*;
        match *self {
            Fst(ref err) => err.fmt(f),
            Io(ref err) => err.fmt(f),
        }
    }
}

impl error::Error for Error {
    fn cause(&self) -> Option<&dyn error::Error> {
        use self::Error::*;
        match *self {
            Fst(ref err) => Some(err),
            Io(ref err) => Some(err),
        }
    }
}
