use std::sync::Arc;

use crate::storage::sqlite3_ondisk::Version;
use crate::{mvcc, MvStore, OpenFlags, Result, IO};

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    strum_macros::EnumString,
    strum_macros::Display,
    strum_macros::IntoStaticStr,
)]
#[strum(ascii_case_insensitive, serialize_all = "snake_case")]
pub enum JournalMode {
    Delete,
    Truncate,
    Persist,
    Memory,
    Wal,
    ExperimentalMvcc,
    Off,
}

impl JournalMode {
    /// Modes that are supported
    #[inline]
    pub fn supported(&self) -> bool {
        matches!(self, JournalMode::Wal | JournalMode::ExperimentalMvcc)
    }

    /// As the header file version
    #[inline]
    pub fn as_version(&self) -> Option<Version> {
        match self {
            JournalMode::Wal => Some(Version::Wal),
            JournalMode::ExperimentalMvcc => Some(Version::Mvcc),
            _ => None,
        }
    }
}

impl From<Version> for JournalMode {
    fn from(value: Version) -> Self {
        match value {
            Version::Legacy => Self::Delete,
            Version::Wal => Self::Wal,
            Version::Mvcc => Self::ExperimentalMvcc,
        }
    }
}

pub fn wal_exists(wal_path: impl AsRef<std::path::Path>) -> bool {
    let wal_path = wal_path.as_ref();
    std::path::Path::exists(wal_path) && wal_path.metadata().unwrap().len() > 0
}

pub fn logical_log_exists(db_path: impl AsRef<std::path::Path>) -> bool {
    let db_path = db_path.as_ref();
    let log_path = db_path.with_extension("db-log");
    std::path::Path::exists(log_path.as_path()) && log_path.as_path().metadata().unwrap().len() > 0
}

pub fn open_mv_store<I: IO + ?Sized>(
    io: &I,
    db_path: impl AsRef<std::path::Path>,
) -> Result<Arc<MvStore>> {
    let db_path = db_path.as_ref();
    let log_path = db_path.with_extension("db-log");
    let string_path = log_path
        .as_os_str()
        .to_str()
        .expect("path should be valid string");
    let file = io.open_file(string_path, OpenFlags::default(), false)?;
    let storage = mvcc::persistent_storage::Storage::new(file);
    let mv_store = MvStore::new(mvcc::LocalClock::new(), storage);
    let mv_store = Arc::new(mv_store);
    Ok(mv_store)
}
