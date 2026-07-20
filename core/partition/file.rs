//! Partition file representation and operations.

use std::path::{Path, PathBuf};

use chrono::DateTime;

use super::error::PartitionError;

#[cfg(feature = "fs")]
type PartitionPathMutex = parking_lot::Mutex<()>;

#[cfg(feature = "fs")]
static PARTITION_PATH_LOCKS: std::sync::LazyLock<
    parking_lot::Mutex<std::collections::HashMap<PathBuf, std::sync::Weak<PartitionPathMutex>>>,
> = std::sync::LazyLock::new(|| parking_lot::Mutex::new(std::collections::HashMap::new()));

#[cfg(feature = "fs")]
pub(crate) fn with_partition_path_lock<T>(path: &Path, operation: impl FnOnce() -> T) -> T {
    let lock = {
        let mut locks = PARTITION_PATH_LOCKS.lock();
        if locks.len() > 1_024 {
            locks.retain(|_, lock| lock.strong_count() > 0);
        }
        match locks.get(path).and_then(std::sync::Weak::upgrade) {
            Some(lock) => lock,
            None => {
                let lock = std::sync::Arc::new(PartitionPathMutex::new(()));
                locks.insert(path.to_path_buf(), std::sync::Arc::downgrade(&lock));
                lock
            }
        }
    };
    let _guard = lock.lock();
    operation()
}

#[cfg(feature = "fs")]
fn remove_file_if_present(path: &Path) -> Result<(), PartitionError> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(PartitionError::IoError(error)),
    }
}

#[cfg(feature = "fs")]
pub(crate) fn remove_partition_sidecars(path: &Path) -> Result<(), PartitionError> {
    for suffix in ["-wal", "-tshm"] {
        let sidecar = PathBuf::from(format!("{}{suffix}", path.to_string_lossy()));
        remove_file_if_present(&sidecar)?;
    }
    Ok(())
}

/// Represents a single partition file.
///
/// A partition file contains data for a specific time range and can be
/// attached/detached from the main database connection.
#[derive(Clone, Debug)]
pub struct PartitionFile {
    /// Path to the partition file
    pub path: PathBuf,
    /// Database alias used for ATTACH (e.g., "events_20250122")
    pub db_alias: String,
    /// Start of the time range (inclusive, unix microseconds)
    pub range_start: i64,
    /// End of the time range (exclusive, unix microseconds)
    pub range_end: i64,
    /// Whether this partition is currently attached
    pub attached: bool,
    /// Database ID after ATTACH (None if not attached)
    pub database_id: Option<usize>,
}

impl PartitionFile {
    /// Create a new partition file descriptor.
    ///
    /// # Arguments
    /// * `path` - Path to the partition file
    /// * `db_alias` - Database alias for ATTACH
    /// * `range_start` - Start timestamp (inclusive, microseconds)
    /// * `range_end` - End timestamp (exclusive, microseconds)
    pub fn new(path: PathBuf, db_alias: String, range_start: i64, range_end: i64) -> Self {
        Self {
            path,
            db_alias,
            range_start,
            range_end,
            attached: false,
            database_id: None,
        }
    }

    /// Check if a timestamp falls within this partition's range.
    ///
    /// # Arguments
    /// * `timestamp_micros` - Unix timestamp in microseconds
    ///
    /// # Returns
    /// `true` if the timestamp is in the range [range_start, range_end)
    pub fn contains(&self, timestamp_micros: i64) -> bool {
        timestamp_micros >= self.range_start && timestamp_micros < self.range_end
    }

    /// Check if this partition overlaps with a time range.
    ///
    /// # Arguments
    /// * `start` - Start of range (inclusive, microseconds)
    /// * `end` - End of range (exclusive, microseconds)
    ///
    /// # Returns
    /// `true` if the ranges overlap
    pub fn overlaps(&self, start: i64, end: i64) -> bool {
        self.range_start < end && self.range_end > start
    }

    /// Check if the partition file exists on disk.
    pub fn exists(&self) -> bool {
        self.path.exists()
    }

    /// Get the file size in bytes.
    ///
    /// # Returns
    /// File size in bytes, or 0 if file doesn't exist or can't be read
    pub fn size_bytes(&self) -> u64 {
        self.path.metadata().map(|m| m.len()).unwrap_or(0)
    }

    /// Mark this partition as attached with the given database ID.
    pub fn mark_attached(&mut self, database_id: usize) {
        self.attached = true;
        self.database_id = Some(database_id);
    }

    /// Mark this partition as detached.
    pub fn mark_detached(&mut self) {
        self.attached = false;
        self.database_id = None;
    }

    /// Convert to PartitionInfo for external API.
    pub fn to_info(&self) -> PartitionInfo {
        PartitionInfo {
            file_path: self.path.to_string_lossy().into_owned(),
            db_alias: self.db_alias.clone(),
            range_start: self.range_start,
            range_end: self.range_end,
            range_start_iso: micros_to_iso(self.range_start),
            range_end_iso: micros_to_iso(self.range_end),
            attached: self.attached,
            size_bytes: self.size_bytes(),
        }
    }
}

/// Information about a partition for external API.
///
/// This struct is designed to be easily serializable and provides
/// all relevant information about a partition file.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct PartitionInfo {
    /// Path to the partition file
    pub file_path: String,
    /// Database alias for ATTACH
    pub db_alias: String,
    /// Start of range (unix microseconds)
    pub range_start: i64,
    /// End of range (unix microseconds)
    pub range_end: i64,
    /// Start of range as ISO 8601 string
    pub range_start_iso: String,
    /// End of range as ISO 8601 string
    pub range_end_iso: String,
    /// Whether partition is currently attached
    pub attached: bool,
    /// File size in bytes
    pub size_bytes: u64,
}

impl PartitionInfo {
    /// Get the duration of this partition in seconds.
    pub fn duration_seconds(&self) -> i64 {
        self.range_end.saturating_sub(self.range_start) / 1_000_000
    }
}

/// Convert microseconds to ISO 8601 string.
fn micros_to_iso(timestamp_micros: i64) -> String {
    let secs = timestamp_micros.div_euclid(1_000_000);
    let nsecs = timestamp_micros.rem_euclid(1_000_000).saturating_mul(1_000) as u32;
    match DateTime::from_timestamp(secs, nsecs) {
        Some(dt) => dt.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
        None => format!("{}", timestamp_micros),
    }
}

/// Create a partition file for a new partition.
///
/// This creates the actual database file with the table schema.
///
/// # Arguments
/// * `path` - Path where the file should be created
/// * `db_alias` - Database alias
/// * `range_start` - Start timestamp (microseconds)
/// * `range_end` - End timestamp (microseconds)
/// * `schema_sql` - CREATE TABLE SQL to initialize the partition
///
/// # Returns
/// The created PartitionFile descriptor
#[cfg(feature = "fs")]
pub fn create_partition_file(
    path: &Path,
    db_alias: String,
    range_start: i64,
    range_end: i64,
    schema_sql: &str,
) -> Result<PartitionFile, PartitionError> {
    use crate::sync::Arc;
    use crate::{Database, PlatformIO, SqliteDialect};

    if path.exists() {
        return Err(PartitionError::FileAlreadyExists(path.to_path_buf()));
    }

    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent)?;

    // Build the database under a private name and publish it only after the
    // complete schema is durable. Readers therefore never discover a
    // half-initialized daily file, and concurrent creators cannot overwrite
    // each other.
    let temporary = tempfile::NamedTempFile::new_in(parent)?;
    let temporary_path = temporary.into_temp_path();
    let temporary_wal_path = PathBuf::from(format!("{}-wal", temporary_path.to_string_lossy()));

    let path_str = temporary_path.to_string_lossy();
    let io = Arc::new(PlatformIO::new().map_err(|e| PartitionError::DatabaseError(e.to_string()))?);
    let db = Database::open_file(io, &path_str, Arc::new(SqliteDialect))
        .map_err(|e| PartitionError::DatabaseError(e.to_string()))?;
    let conn = db
        .connect()
        .map_err(|e| PartitionError::DatabaseError(e.to_string()))?;

    // Execute the complete schema so callers can include local indexes alongside
    // the physical table definition.
    if let Err(error) = conn.execute(schema_sql) {
        let _ = conn.close();
        drop(conn);
        drop(db);
        let _ = std::fs::remove_file(&temporary_wal_path);
        return Err(PartitionError::DatabaseError(format!(
            "failed to initialize partition schema: {error}"
        )));
    }
    if let Err(error) = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)") {
        let _ = conn.close();
        drop(conn);
        drop(db);
        let _ = std::fs::remove_file(&temporary_wal_path);
        return Err(PartitionError::DatabaseError(format!(
            "failed to checkpoint new partition schema: {error}"
        )));
    }
    if let Err(error) = conn.close() {
        drop(conn);
        drop(db);
        let _ = std::fs::remove_file(&temporary_wal_path);
        return Err(PartitionError::DatabaseError(format!(
            "failed to close new partition: {error}"
        )));
    }
    drop(conn);
    drop(db);
    let _ = std::fs::remove_file(&temporary_wal_path);

    with_partition_path_lock(path, || {
        if path.exists() {
            return Ok(());
        }
        remove_partition_sidecars(path)?;
        match temporary_path.persist_noclobber(path) {
            Ok(()) => Ok(()),
            Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
                // A creator in another process finished first. Its final path
                // is never overwritten.
                Ok(())
            }
            Err(error) => Err(PartitionError::IoError(error.error)),
        }
    })?;

    Ok(PartitionFile::new(
        path.to_path_buf(),
        db_alias,
        range_start,
        range_end,
    ))
}

/// Create a partition file (no-fs stub)
#[cfg(not(feature = "fs"))]
pub fn create_partition_file(
    path: &Path,
    db_alias: String,
    range_start: i64,
    range_end: i64,
    _schema_sql: &str,
) -> Result<PartitionFile, PartitionError> {
    Err(PartitionError::IoError(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "create_partition_file not available in this build (no-fs)",
    )))
}

/// Open an existing partition file and read its metadata.
///
/// # Arguments
/// * `path` - Path to the partition file
/// * `db_alias` - Database alias
/// * `range_start` - Start timestamp (microseconds)
/// * `range_end` - End timestamp (microseconds)
///
/// # Returns
/// PartitionFile descriptor if file exists
pub fn open_partition_file(
    path: &Path,
    db_alias: String,
    range_start: i64,
    range_end: i64,
) -> Result<PartitionFile, PartitionError> {
    if !path.exists() {
        return Err(PartitionError::FileNotFound(path.to_path_buf()));
    }

    Ok(PartitionFile::new(
        path.to_path_buf(),
        db_alias,
        range_start,
        range_end,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_file_contains() {
        let file = PartitionFile::new(
            PathBuf::from("/data/test.db"),
            "test_20250122".to_string(),
            1737504000_000_000, // 2025-01-22 00:00:00 UTC
            1737590400_000_000, // 2025-01-23 00:00:00 UTC
        );

        // Timestamp in range
        assert!(file.contains(1737547200_000_000)); // 2025-01-22 12:00:00

        // Timestamp at start (inclusive)
        assert!(file.contains(1737504000_000_000));

        // Timestamp at end (exclusive)
        assert!(!file.contains(1737590400_000_000));

        // Timestamp before range
        assert!(!file.contains(1737417600_000_000)); // 2025-01-21

        // Timestamp after range
        assert!(!file.contains(1737676800_000_000)); // 2025-01-24
    }

    #[test]
    fn test_partition_file_overlaps() {
        let file = PartitionFile::new(
            PathBuf::from("/data/test.db"),
            "test_20250122".to_string(),
            1737504000_000_000, // 2025-01-22 00:00:00
            1737590400_000_000, // 2025-01-23 00:00:00
        );

        // Range fully inside
        assert!(file.overlaps(1737540000_000_000, 1737560000_000_000));

        // Range overlapping start
        assert!(file.overlaps(1737400000_000_000, 1737540000_000_000));

        // Range overlapping end
        assert!(file.overlaps(1737540000_000_000, 1737700000_000_000));

        // Range fully outside (before)
        assert!(!file.overlaps(1737300000_000_000, 1737400000_000_000));

        // Range fully outside (after)
        assert!(!file.overlaps(1737700000_000_000, 1737800000_000_000));

        // Range touching at boundary (no overlap)
        assert!(!file.overlaps(1737590400_000_000, 1737700000_000_000));
    }

    #[test]
    fn test_partition_file_attach_detach() {
        let mut file = PartitionFile::new(
            PathBuf::from("/data/test.db"),
            "test_20250122".to_string(),
            1737504000_000_000,
            1737590400_000_000,
        );

        assert!(!file.attached);
        assert!(file.database_id.is_none());

        file.mark_attached(42);
        assert!(file.attached);
        assert_eq!(file.database_id, Some(42));

        file.mark_detached();
        assert!(!file.attached);
        assert!(file.database_id.is_none());
    }

    #[test]
    fn test_partition_info_duration() {
        let info = PartitionInfo {
            file_path: "/data/test.db".to_string(),
            db_alias: "test_20250122".to_string(),
            range_start: 1737504000_000_000,
            range_end: 1737590400_000_000,
            range_start_iso: "2025-01-22T00:00:00Z".to_string(),
            range_end_iso: "2025-01-23T00:00:00Z".to_string(),
            attached: false,
            size_bytes: 0,
        };

        assert_eq!(info.duration_seconds(), 86400); // 24 hours
    }

    #[test]
    fn test_micros_to_iso() {
        let timestamp = 1737504000_000_000i64; // 2025-01-22 00:00:00 UTC
        let iso = micros_to_iso(timestamp);
        assert_eq!(iso, "2025-01-22T00:00:00Z");
    }

    #[test]
    fn test_negative_micros_to_iso() {
        assert_eq!(micros_to_iso(-1), "1969-12-31T23:59:59Z");
    }
}
