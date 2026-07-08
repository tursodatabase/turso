//! Partition-related errors.

use std::fmt;
use std::path::PathBuf;

/// Errors that can occur during partition operations.
#[derive(Debug)]
pub enum PartitionError {
    /// Partition column not found in table
    ColumnNotFound { column: String, table: String },
    /// Partition column has invalid type (must be INTEGER)
    InvalidColumnType {
        column: String,
        expected: String,
        actual: String,
    },
    /// Invalid timestamp value for partitioning
    InvalidTimestamp { value: i64, reason: String },
    /// Partition file not found
    FileNotFound(PathBuf),
    /// Partition file already exists
    FileAlreadyExists(PathBuf),
    /// Partition is not attached
    NotAttached { table: String, partition: String },
    /// Cross-partition write in single transaction is not allowed
    CrossPartitionWrite {
        table: String,
        partition1: String,
        partition2: String,
    },
    /// Table is not configured for partitioning
    TableNotPartitioned(String),
    /// Table already registered for partitioning
    TableAlreadyRegistered(String),
    /// I/O error during partition operations
    IoError(std::io::Error),
    /// Database error during partition operations
    DatabaseError(String),
}

impl fmt::Display for PartitionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ColumnNotFound { column, table } => {
                write!(
                    f,
                    "partition column '{}' not found in table '{}'",
                    column, table
                )
            }
            Self::InvalidColumnType {
                column,
                expected,
                actual,
            } => {
                write!(
                    f,
                    "partition column '{}' has invalid type: expected {}, got {}",
                    column, expected, actual
                )
            }
            Self::InvalidTimestamp { value, reason } => {
                write!(f, "invalid timestamp {}: {}", value, reason)
            }
            Self::FileNotFound(path) => {
                write!(f, "partition file not found: {}", path.display())
            }
            Self::FileAlreadyExists(path) => {
                write!(f, "partition file already exists: {}", path.display())
            }
            Self::NotAttached { table, partition } => {
                write!(
                    f,
                    "partition '{}' is not attached for table '{}'",
                    partition, table
                )
            }
            Self::CrossPartitionWrite {
                table,
                partition1,
                partition2,
            } => {
                write!(
                    f,
                    "cross-partition write not allowed: table '{}' writes to both '{}' and '{}'",
                    table, partition1, partition2
                )
            }
            Self::TableNotPartitioned(table) => {
                write!(f, "table '{}' is not configured for partitioning", table)
            }
            Self::TableAlreadyRegistered(table) => {
                write!(
                    f,
                    "table '{}' is already registered for partitioning",
                    table
                )
            }
            Self::IoError(e) => {
                write!(f, "partition I/O error: {}", e)
            }
            Self::DatabaseError(msg) => {
                write!(f, "partition database error: {}", msg)
            }
        }
    }
}

impl std::error::Error for PartitionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::IoError(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for PartitionError {
    fn from(e: std::io::Error) -> Self {
        Self::IoError(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = PartitionError::ColumnNotFound {
            column: "ts".to_string(),
            table: "events".to_string(),
        };
        assert!(err.to_string().contains("ts"));
        assert!(err.to_string().contains("events"));
    }

    #[test]
    fn test_io_error_conversion() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let partition_err: PartitionError = io_err.into();
        assert!(matches!(partition_err, PartitionError::IoError(_)));
    }
}
