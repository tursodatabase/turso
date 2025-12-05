use std::sync::Arc;

use crate::util::IOExt;
use crate::{bail_corrupt_error, LimboError, MvStore};
use crate::{storage::sqlite3_ondisk::Version, Pager, Result};

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
        match self {
            JournalMode::Wal | JournalMode::ExperimentalMvcc => true,
            _ => false,
        }
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

impl TryFrom<Version> for JournalMode {
    type Error = LimboError;

    fn try_from(value: Version) -> std::result::Result<Self, Self::Error> {
        let val = match value {
            Version::Wal => Self::Wal,
            Version::Mvcc => Self::ExperimentalMvcc,
            _ => bail_corrupt_error!("invalid header version: {:?}", value),
        };
        Ok(val)
    }
}

// As this is not a performance critical function, I made it synchronous simplify to the code
// and not have to add an additional state machine
pub fn change_mode(
    db_path: impl AsRef<std::path::Path>,
    pager: &Pager,
    mv_store: Option<&Arc<MvStore>>,
    prev_mode: JournalMode,
    new_mode: JournalMode,
) -> Result<JournalMode> {
    if !new_mode.supported() {
        return Err(crate::LimboError::ParseError(format!(
            "Journal Mode `{new_mode}` is not supported"
        )));
    }

    if prev_mode == new_mode {
        return Ok(new_mode);
    }

    let db_path = db_path.as_ref();

    if matches!(new_mode, JournalMode::ExperimentalMvcc)
        && wal_exists(db_path.with_extension("db-wal"))
    {
        return Err(LimboError::InvalidArgument(format!(
                    "WAL file exists for database {}, but we want to enable MVCC mode. This is currently not supported. Open the database in non-MVCC mode and run PRAGMA wal_checkpoint(TRUNCATE) to truncate the WAL.",
                    db_path.display()
                )));
    }

    if matches!(new_mode, JournalMode::Wal) && logical_log_exists(db_path) {
        return Err(LimboError::InvalidArgument(format!(
                     "MVCC logical log file exists for database {}, but we want to enable WAL mode. This is not supported. Open the database in MVCC mode and run PRAGMA wal_checkpoint(TRUNCATE) to truncate the logical log.",
                    db_path.display()
                )));
    }

    let new_version = new_mode
        .as_version()
        .expect("Should be a supported Journal Mode");

    pager.io.block(|| {
        pager.with_header_mut(|header| {
            header.read_version = new_version;
            header.write_version = new_version;
        })
    })?;
    Ok(new_mode)
}

pub fn wal_exists(wal_path: impl AsRef<std::path::Path>) -> bool {
    let wal_path = wal_path.as_ref();
    std::path::Path::exists(&wal_path) && wal_path.metadata().unwrap().len() > 0
}

pub fn logical_log_exists(db_path: impl AsRef<std::path::Path>) -> bool {
    let db_path = db_path.as_ref();
    let log_path = db_path.with_extension("db-log");
    std::path::Path::exists(log_path.as_path()) && log_path.as_path().metadata().unwrap().len() > 0
}
