use std::path::PathBuf;
use std::sync::Arc;
use std::{fmt, io};

use crate::Version;

/// Error while trying to acquire a directory [lock](crate::directory::Lock).
///
/// This is returned from [`Directory::acquire_lock`](crate::Directory::acquire_lock).
#[derive(Debug, Clone, Error)]
pub enum LockError {
    /// Failed to acquired a lock as it is already held by another
    /// client.
    /// - In the context of a blocking lock, this means the lock was not released within some
    ///   `timeout` period.
    /// - In the context of a non-blocking lock, this means the lock was busy at the moment of the
    ///   call.
    #[error("Could not acquire lock as it is already held, possibly by a different process.")]
    LockBusy,
    /// Trying to acquire a lock failed with an `IoError`
    #[error("Failed to acquire the lock due to an io:Error.")]
    IoError(Arc<io::Error>),
}

impl LockError {
    /// Wraps an io error.
    pub fn wrap_io_error(io_error: io::Error) -> Self {
        Self::IoError(Arc::new(io_error))
    }
}

/// Error that may occur when opening a directory
#[derive(Debug, Clone, Error)]
pub enum OpenDirectoryError {
    /// The underlying directory does not exist.
    #[error("Directory does not exist: '{0}'.")]
    DoesNotExist(PathBuf),
    /// The path exists but is not a directory.
    #[error("Path exists but is not a directory: '{0}'.")]
    NotADirectory(PathBuf),
    /// Failed to create a temp directory.
    #[error("Failed to create a temporary directory: '{0}'.")]
    FailedToCreateTempDir(Arc<io::Error>),
    /// IoError
    #[error("IoError '{io_error:?}' while create directory in: '{directory_path:?}'.")]
    IoError {
        /// underlying io Error.
        io_error: Arc<io::Error>,
        /// directory we tried to open.
        directory_path: PathBuf,
    },
}

impl OpenDirectoryError {
    /// Wraps an io error.
    pub fn wrap_io_error(io_error: io::Error, directory_path: PathBuf) -> Self {
        Self::IoError {
            io_error: Arc::new(io_error),
            directory_path,
        }
    }
}

/// Error that may occur when starting to write in a file
#[derive(Debug, Clone, Error)]
pub enum OpenWriteError {
    /// Our directory is WORM, writing an existing file is forbidden.
    /// Checkout the `Directory` documentation.
    #[error("File already exists: '{0}'")]
    FileAlreadyExists(PathBuf),
    /// Any kind of IO error that happens when
    /// writing in the underlying IO device.
    #[error("IoError '{io_error:?}' while opening file for write: '{filepath}'.")]
    IoError {
        /// The underlying `io::Error`.
        io_error: Arc<io::Error>,
        /// File path of the file that tantivy failed to open for write.
        filepath: PathBuf,
    },
}

impl OpenWriteError {
    /// Wraps an io error.
    pub fn wrap_io_error(io_error: io::Error, filepath: PathBuf) -> Self {
        Self::IoError {
            io_error: Arc::new(io_error),
            filepath,
        }
    }
}
/// Type of index incompatibility between the library and the index found on disk
/// Used to catch and provide a hint to solve this incompatibility issue
#[derive(Clone)]
pub enum Incompatibility {
    /// This library cannot decompress the index found on disk
    CompressionMismatch {
        /// Compression algorithm used by the current version of tantivy
        library_compression_format: String,
        /// Compression algorithm that was used to serialise the index
        index_compression_format: String,
    },
    /// The index format found on disk isn't supported by this version of the library
    IndexMismatch {
        /// Version used by the library
        library_version: Version,
        /// Version the index was built with
        index_version: Version,
    },
}

impl fmt::Debug for Incompatibility {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Incompatibility::CompressionMismatch {
                library_compression_format,
                index_compression_format,
            } => {
                let err = format!(
                    "Library was compiled with {library_compression_format:?} compression, index \
                     was compressed with {index_compression_format:?}"
                );
                let advice = format!(
                    "Change the feature flag to {index_compression_format:?} and rebuild the \
                     library"
                );
                write!(f, "{err}. {advice}")?;
            }
            Incompatibility::IndexMismatch {
                library_version,
                index_version,
            } => {
                let err = format!(
                    "Library version: {}, index version: {}",
                    library_version.index_format_version, index_version.index_format_version
                );
                // TODO make a more useful error message
                // include the version range that supports this index_format_version
                let advice = format!(
                    "Change tantivy to a version compatible with index format {} (e.g. {}.{}.x) \
                     and rebuild your project.",
                    index_version.index_format_version, index_version.major, index_version.minor
                );
                write!(f, "{err}. {advice}")?;
            }
        }

        Ok(())
    }
}

/// Error that may occur when accessing a file read
#[derive(Debug, Clone, Error)]
pub enum OpenReadError {
    /// The file does not exist.
    #[error("Files does not exist: {0:?}")]
    FileDoesNotExist(PathBuf),
    /// Any kind of io::Error.
    #[error(
        "IoError: '{io_error:?}' happened while opening the following file for Read: {filepath}."
    )]
    IoError {
        /// The underlying `io::Error`.
        io_error: Arc<io::Error>,
        /// File path of the file that tantivy failed to open for read.
        filepath: PathBuf,
    },
    /// This library does not support the index version found in file footer.
    #[error("Index version unsupported: {0:?}")]
    IncompatibleIndex(Incompatibility),
}

impl OpenReadError {
    /// Wraps an io error.
    pub fn wrap_io_error(io_error: io::Error, filepath: PathBuf) -> Self {
        Self::IoError {
            io_error: Arc::new(io_error),
            filepath,
        }
    }
}
/// Error that may occur when trying to delete a file
#[derive(Debug, Clone, Error)]
pub enum DeleteError {
    /// The file does not exist.
    #[error("File does not exist: '{0}'.")]
    FileDoesNotExist(PathBuf),
    /// Any kind of IO error that happens when
    /// interacting with the underlying IO device.
    #[error("The following IO error happened while deleting file '{filepath}': '{io_error:?}'.")]
    IoError {
        /// The underlying `io::Error`.
        io_error: Arc<io::Error>,
        /// File path of the file that tantivy failed to delete.
        filepath: PathBuf,
    },
}

impl From<Incompatibility> for OpenReadError {
    fn from(incompatibility: Incompatibility) -> Self {
        OpenReadError::IncompatibleIndex(incompatibility)
    }
}
