use crate::sync::Arc;
use std::path::{Path, PathBuf};

use crate::storage::sqlite3_ondisk::Version;
use crate::{alloc::DynAllocator, mvcc, LimboError, MvStore, OpenFlags, Result, IO};

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

/// Opens the MVCC store whose logical log is at `log_path`. A custom
/// `durable_storage` replaces the log file, and then `log_path` is not used.
pub fn open_mv_store(
    io: Arc<dyn IO>,
    log_path: &Path,
    flags: OpenFlags,
    durable_storage: Option<Arc<dyn mvcc::persistent_storage::DurableStorage>>,
    encryption_ctx: Option<crate::storage::encryption::EncryptionContext>,
    allocator: DynAllocator,
    experimental_mvcc_passive_checkpoint: bool,
) -> Result<Arc<MvStore>> {
    // `encryption_ctx` encrypts database pages, but a custom DurableStorage
    // writes the MVCC log itself. If the database is encrypted, the custom
    // storage must also have an encryption context so the log is not plaintext
    if let Some(storage) = &durable_storage {
        if encryption_ctx.is_some() && storage.encryption_ctx().is_none() {
            return Err(LimboError::InvalidArgument(
                    "encrypted MVCC requires the custom DurableStorage to be configured with encryption"
                        .to_string(),
                ));
        }
    }
    let storage: Arc<dyn mvcc::persistent_storage::DurableStorage> =
        if let Some(storage) = durable_storage {
            storage
        } else {
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

    Ok(Arc::new(MvStore::new_in(
        mvcc::MvccClock::new(),
        storage,
        allocator,
        experimental_mvcc_passive_checkpoint,
    )?))
}

/// Returns the path of the MVCC logical log of the database at `db_path`: the
/// full file name followed by `-log`, as for the `-wal` file. For `app.db`,
/// the log is `app.db-log`.
pub fn logical_log_path(db_path: impl AsRef<Path>) -> PathBuf {
    sidecar_path(db_path.as_ref(), "-log")
}

/// Returns true if `path` is a file of one byte or more.
pub fn file_has_data(path: &Path) -> bool {
    std::fs::metadata(path).is_ok_and(|metadata| metadata.len() > 0)
}

fn sidecar_path(db_path: &Path, suffix: &str) -> PathBuf {
    let mut name = db_path.as_os_str().to_owned();
    name.push(suffix);
    PathBuf::from(name)
}
