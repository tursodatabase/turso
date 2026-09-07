use std::error;
use std::fmt;
use std::str;
use std::string::FromUtf8Error;

use crate::raw::FstType;

/// An error that occurred while using a finite state transducer.
pub enum Error {
    /// A version mismatch occurred while reading a finite state transducer.
    ///
    /// This occurs when the API version (of the crate) does not match the
    /// version encoded in the finite state transducer.
    ///
    /// When this error is encountered, there are only two ways to fix it:
    ///
    /// 1. Change the version of the library to one that is compatible with
    ///    the given finite state transducer.
    /// 2. Rebuild the finite state transducer.
    Version {
        /// The expected version, which is hard-coded into the current version
        /// of this crate.
        expected: u64,
        /// The version read from the finite state transducer.
        got: u64,
    },
    /// An unexpected error occurred while reading a finite state transducer.
    /// Usually this occurs because the data is corrupted or is not actually
    /// a finite state transducer serialized by this library.
    Format,
    /// A duplicate key was inserted into a finite state transducer, which is
    /// not allowed.
    DuplicateKey {
        /// The duplicate key.
        got: Vec<u8>,
    },
    /// A key was inserted out of order into a finite state transducer.
    ///
    /// Keys must always be inserted in lexicographic order.
    OutOfOrder {
        /// The last key successfully inserted.
        previous: Vec<u8>,
        /// The key that caused this error to occur.
        got: Vec<u8>,
    },
    /// A finite state transducer with an unexpected type was found.
    ///
    /// This is not currently used in this crate, but callers may wish to
    /// employ its use for alternative data structures implemented on top of
    /// finite state transducers.
    WrongType {
        /// The expected finite state transducer type.
        expected: FstType,
        /// The type read from a finite state transducer.
        got: FstType,
    },
    /// An error that occurred when trying to decode a UTF-8 byte key.
    FromUtf8(FromUtf8Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use self::Error::*;
        match *self {
            FromUtf8(ref err) => err.fmt(f),
            Version { expected, got } => write!(
                f,
                "\
Error opening FST: expected API version {}, got API version {}.
It looks like the FST you're trying to open is either not an FST file or it
was generated with a different version of the 'fst' crate. You'll either need
to change the version of the 'fst' crate you're using, or re-generate the
FST.",
                expected, got
            ),
            Format => write!(
                f,
                "\
Error opening FST: An unknown error occurred. This usually means you're trying
to read data that isn't actually an encoded FST."
            ),
            DuplicateKey { ref got } => write!(
                f,
                "\
                 Error inserting duplicate key: {}.",
                format_bytes(&*got)
            ),
            OutOfOrder {
                ref previous,
                ref got,
            } => write!(
                f,
                "\
Error inserting out-of-order key: {}. (Previous key was {}.) Keys must be
inserted in lexicographic order.",
                format_bytes(&*got),
                format_bytes(&*previous)
            ),
            WrongType { expected, got } => write!(
                f,
                "\
                 Error opening FST: expected type {}, got type {}.",
                expected, got
            ),
        }
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl error::Error for Error {
    fn cause(&self) -> Option<&dyn error::Error> {
        match *self {
            Error::FromUtf8(ref err) => Some(err),
            _ => None,
        }
    }
}

impl From<FromUtf8Error> for Error {
    #[inline]
    fn from(err: FromUtf8Error) -> Self {
        Error::FromUtf8(err)
    }
}

/// Attempt to convert an arbitrary byte string to a more convenient display
/// form.
///
/// Essentially, try to decode the bytes as UTF-8 and show that. Failing that,
/// just show the sequence of bytes.
fn format_bytes(bytes: &[u8]) -> String {
    match str::from_utf8(bytes) {
        Ok(s) => s.to_owned(),
        Err(_) => format!("{:?}", bytes),
    }
}
