use crate::sync::Arc;

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

/// Size of the MVCC logical log next to `db_path`, probed through the IO
/// backend (never the host filesystem directly), or `None` if the log file
/// does not exist.
fn logical_log_size(io: &dyn IO, db_path: impl AsRef<std::path::Path>) -> Option<u64> {
    let log_path = db_path.as_ref().with_extension("db-log");
    let log_path = log_path
        .as_os_str()
        .to_str()
        .expect("path should be valid string");
    // Read-only, no-lock, no-create probe: a missing (or unreadable) file is
    // treated as "no logical log".
    let file = io
        .open_file(log_path, OpenFlags::ReadOnly | OpenFlags::NoLock, false)
        .ok()?;
    file.size().ok()
}

pub fn logical_log_exists(io: &dyn IO, db_path: impl AsRef<std::path::Path>) -> bool {
    logical_log_size(io, db_path).is_some_and(|size| size > 0)
}

/// Whether the MVCC logical log next to `db_path` contains committed frames
/// (i.e. is larger than the fixed-size log header).
///
/// A log at or below header size carries no transactions: it is the benign
/// leftover of a `PRAGMA journal_mode=mvcc` switch that was abandoned before
/// the database header was flipped to MVCC (the log header is written and
/// synced before the page-1 flip, which is the last step of the switch).
pub fn logical_log_has_frames(io: &dyn IO, db_path: impl AsRef<std::path::Path>) -> bool {
    logical_log_size(io, db_path).is_some_and(|size| {
        size > crate::mvcc::persistent_storage::logical_log::LOG_HDR_SIZE as u64
    })
}

pub fn open_mv_store(
    io: Arc<dyn IO>,
    db_path: impl AsRef<std::path::Path>,
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

    Ok(Arc::new(MvStore::new_in(
        mvcc::MvccClock::new(),
        storage,
        allocator,
        experimental_mvcc_passive_checkpoint,
    )?))
}
