use crate::sync::Arc;
use std::io::Read;
use std::path::{Path, PathBuf};

use crate::database::{DatabaseKey, RegistryEntry, DATABASE_MANAGER};
use crate::storage::encryption::{SQLITE_HEADER, TURSO_HEADER_PREFIX};
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
    let builtin_storage = durable_storage.is_none();
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

    let mut mv_store = MvStore::new_in(
        mvcc::MvccClock::new(),
        storage,
        allocator,
        experimental_mvcc_passive_checkpoint,
    )?;
    if builtin_storage {
        mv_store = mv_store.with_logical_log_path(log_path.to_path_buf());
    }
    Ok(Arc::new(mv_store))
}

/// Returns the path of the MVCC logical log of the database at `db_path` when
/// the database enters MVCC mode: the full file name followed by `-log`, as
/// for the `-wal` file. For `app.db`, the log is `app.db-log`.
pub fn logical_log_path(db_path: impl AsRef<Path>) -> PathBuf {
    sidecar_path(db_path.as_ref(), "-log")
}

/// Returns the path of the MVCC logical log of the database at `db_path`,
/// which is in MVCC mode and whose files `io` opens.
///
/// Older versions named the log `db_path.with_extension("db-log")`. That name
/// replaces the last extension of the database file, so `app.1`, `app.2` and
/// `app.sqlite` all used `app.db-log`. For a `.db` file, the old name and the
/// new name are the same. When the old log has data and the new log does not
/// exist, the database continues to use the old log until a checkpoint empties
/// it. If another database file in the directory can also have written the old
/// log, the log can hold commits of each of them, and this returns an error.
pub fn find_logical_log_path(io: &dyn IO, db_path: impl AsRef<Path>) -> Result<PathBuf> {
    let db_path = db_path.as_ref();
    let log_path = logical_log_path(db_path);
    let old_log_path = db_path.with_extension("db-log");
    // The size checks go through `io`, because the files can be in a place
    // that only `io` can open, such as MemoryIO or the browser storage.
    if old_log_path == log_path
        || io_file_len(io, &old_log_path) == 0
        || io_file_len(io, &log_path) > 0
    {
        return Ok(log_path);
    }
    // The checks below read the files directly. If the old log is not in the
    // OS file system, they cannot run. Also, `io` can show an empty new log
    // that was not written, because the browser storage creates each file
    // before the database opens. Then the database uses the old log, as older
    // versions did.
    if std::fs::metadata(&old_log_path).is_err() {
        return Ok(old_log_path);
    }
    // The header of this database is not read: the database is in MVCC mode,
    // and closing a second descriptor of its file would release its lock.
    if log_path.exists() {
        return Ok(log_path);
    }
    let others = other_old_log_writers(io, db_path, &old_log_path)?;
    if others.is_empty() {
        return Ok(old_log_path);
    }
    Err(LimboError::InvalidArgument(format!(
        "the MVCC logical log {} was written by an older version, which used it for database {} and for {}. The log can hold commits of each of these databases. Move the other database files to a different directory, then open the database that owns the log",
        old_log_path.display(),
        db_path.display(),
        others.join(", "),
    )))
}

/// Returns the size of the file at `path`, or 0 if the file does not exist.
fn io_file_len(io: &dyn IO, path: &Path) -> u64 {
    // Read the size without opening the file when the OS file system has it.
    // Unix file locks belong to the process, so closing a second descriptor
    // of the file would release the lock that an open log holds.
    if let Ok(metadata) = std::fs::metadata(path) {
        return metadata.len();
    }
    let Some(path) = path.to_str() else {
        return 0;
    };
    io.open_file(path, OpenFlags::ReadOnly | OpenFlags::NoLock, false)
        .and_then(|file| file.size())
        .unwrap_or(0)
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

/// What the first bytes of a file tell about whether an older version could
/// have written commits of that file to an MVCC logical log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DatabaseFileKind {
    /// The file is not a database, or it has no page 1. A database writes
    /// page 1 before its first MVCC commit.
    NoPage1,
    /// A database that is not in MVCC mode, with an empty WAL. Its header is
    /// current, and a database leaves MVCC mode only after a checkpoint has
    /// emptied its log.
    NotMvcc,
    /// A database in MVCC mode, an encrypted database, or a database whose
    /// WAL can hold a newer header.
    MaybeMvcc,
    /// The first bytes are not a known database header, for example because
    /// a custom page codec encoded page 1.
    Unknown,
}

fn database_file_kind(path: &Path) -> DatabaseFileKind {
    let mut header = [0u8; 20];
    let Ok(mut file) = std::fs::File::open(path) else {
        return DatabaseFileKind::NoPage1;
    };
    if file.read_exact(&mut header).is_err() {
        return DatabaseFileKind::NoPage1;
    }
    if header.starts_with(TURSO_HEADER_PREFIX) {
        return DatabaseFileKind::MaybeMvcc;
    }
    if !header.starts_with(SQLITE_HEADER) {
        return DatabaseFileKind::Unknown;
    }
    // Bytes 18 and 19 are the write and read versions of the file format.
    let mvcc = Version::Mvcc as u8;
    if header[18] == mvcc || header[19] == mvcc || file_has_data(&sidecar_path(path, "-wal")) {
        DatabaseFileKind::MaybeMvcc
    } else {
        DatabaseFileKind::NotMvcc
    }
}

/// Returns the names of the other database files in the directory of
/// `db_path` that an older version could have opened in MVCC mode with the
/// log at `old_log_path`.
fn other_old_log_writers(io: &dyn IO, db_path: &Path, old_log_path: &Path) -> Result<Vec<String>> {
    let dir = match db_path.parent() {
        Some(dir) if !dir.as_os_str().is_empty() => dir,
        _ => Path::new("."),
    };
    let entries = std::fs::read_dir(dir).map_err(|e| {
        LimboError::InternalError(format!(
            "cannot list directory {} to find the owner of the MVCC logical log {}: {e}",
            dir.display(),
            old_log_path.display(),
        ))
    })?;
    let mut others = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|e| {
            LimboError::InternalError(format!("cannot list directory {}: {e}", dir.display()))
        })?;
        let path = entry.path();
        if Some(entry.file_name().as_os_str()) == db_path.file_name()
            || path.with_extension("db-log").file_name() != old_log_path.file_name()
            || is_sidecar_file(&entry.file_name().to_string_lossy())
        {
            continue;
        }
        // A database that has its own log in the new name does not use the
        // old log.
        let log_path = logical_log_path(&path);
        if log_path.file_name() != old_log_path.file_name() && log_path.exists() {
            continue;
        }
        // Do not read the header of a database that this process has open,
        // because closing a second descriptor of its file would release its
        // lock.
        let kind = open_database_kind(io, &path).unwrap_or_else(|| database_file_kind(&path));
        if kind == DatabaseFileKind::MaybeMvcc {
            others.push(path.display().to_string());
        }
    }
    others.sort();
    Ok(others)
}

/// Returns true if `name` is the name of a file that a database keeps next to
/// its database file. Such a file is not a database, and the database that
/// owns it can have it open.
fn is_sidecar_file(name: &str) -> bool {
    ["-wal", "-shm", "-log", "-journal"]
        .iter()
        .any(|suffix| name.ends_with(suffix))
}

/// Returns the kind of the database at `path` if this process has it open,
/// or `None` if it does not. A database that is opening can be in any mode.
fn open_database_kind(io: &dyn IO, path: &Path) -> Option<DatabaseFileKind> {
    let file_id = io.file_id(path.to_str()?).ok()?;
    let db = {
        let registry = DATABASE_MANAGER.lock();
        match registry.get(&DatabaseKey::File(file_id))? {
            RegistryEntry::Opening => return Some(DatabaseFileKind::MaybeMvcc),
            RegistryEntry::Ready(db) => db.clone(),
        }
    };
    // Upgrade the reference after the registry is unlocked, because dropping
    // the last reference to a database locks the registry.
    let db = db.upgrade()?;
    Some(if db.mvcc_enabled() {
        DatabaseFileKind::MaybeMvcc
    } else {
        DatabaseFileKind::NotMvcc
    })
}
