use crate::sync::Arc;

use crate::storage::sqlite3_ondisk::Version;
use crate::{mvcc, LimboError, MvStore, OpenFlags, Result, IO};

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
    #[strum(to_string = "mvcc", serialize = "experimental_mvcc")]
    Mvcc,
    Off,
}

impl JournalMode {
    /// Modes that are supported
    #[inline]
    pub fn supported(&self) -> bool {
        matches!(self, JournalMode::Wal | JournalMode::Mvcc)
    }

    /// As the header file version
    #[inline]
    pub fn as_version(&self) -> Option<Version> {
        match self {
            JournalMode::Wal => Some(Version::Wal),
            JournalMode::Mvcc => Some(Version::Mvcc),
            _ => None,
        }
    }
}

impl From<Version> for JournalMode {
    fn from(value: Version) -> Self {
        match value {
            Version::Legacy => Self::Delete,
            Version::Wal => Self::Wal,
            Version::Mvcc => Self::Mvcc,
        }
    }
}

pub fn logical_log_exists(db_path: impl AsRef<std::path::Path>) -> bool {
    let db_path = db_path.as_ref();
    let log_path = db_path.with_extension("db-log");
    std::path::Path::exists(log_path.as_path()) && log_path.as_path().metadata().unwrap().len() > 0
}

pub fn open_mv_store(
    io: Arc<dyn IO>,
    db_path: impl AsRef<std::path::Path>,
    flags: OpenFlags,
    durable_storage: Option<Arc<dyn mvcc::persistent_storage::DurableStorage>>,
    encryption_ctx: Option<crate::storage::encryption::EncryptionContext>,
) -> Result<Arc<MvStore>> {
    if durable_storage.is_some() && encryption_ctx.is_some() {
        return Err(LimboError::InvalidArgument(
            "encrypted MVCC is not supported with custom DurableStorage".to_string(),
        ));
    }

    let storage: Arc<dyn mvcc::persistent_storage::DurableStorage> =
        if let Some(storage) = durable_storage {
            storage
        } else {
            let db_path = db_path.as_ref();
            let log_path = db_path.with_extension("db-log");
            let string_path = log_path
                .as_os_str()
                .to_str()
                .expect("path should be valid string");
            let file = io.open_file(string_path, flags, false)?;
            Arc::new(mvcc::persistent_storage::Storage::new(
                file,
                io,
                encryption_ctx,
            ))
        };

    Ok(Arc::new(MvStore::new(mvcc::MvccClock::new(), storage)))
}
