use std::sync::Arc;

use crate::storage::sqlite3_ondisk::DatabaseHeader;
use crate::storage::wal::CheckpointMode;
use crate::util::IOExt;
use crate::vdbe::Program;
use crate::{mvcc, LimboError, MvStore, OpenFlags, Result, IO};
use crate::{
    storage::sqlite3_ondisk::{begin_write_btree_page, RawVersion, Version},
    Pager,
};

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

// As this is not a performance critical function, I made it synchronous simplify to the code
// and not have to add an additional state machine
pub fn change_mode(
    db_path: impl AsRef<std::path::Path>,
    program: &Program,
    pager: &Pager,
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

    if matches!(new_mode, JournalMode::ExperimentalMvcc) && !program.connection.db.mvcc_enabled() {
        return Err(LimboError::InvalidArgument(
                "MVCC is not enabled. Enable it with `--experimental-mvcc` flag in the CLI or by setting the MVCC option in `DatabaseOpts`".to_string(),
            ));
    }

    // Checkpoint the WAL or MVCC
    program.connection.checkpoint(CheckpointMode::Truncate {
        upper_bound_inclusive: None,
    })?;

    let new_version = new_mode
        .as_version()
        .expect("Should be a supported Journal Mode");
    let raw_version = RawVersion::from(new_version);

    // After checkpoint, pager holds the most up-to-date version of the Header for both MVCC and WAL
    pager.io.block(|| {
        pager.with_header_mut(|header| {
            header.read_version = raw_version;
            header.write_version = raw_version;
        })
    })?;

    let (page, c) = pager.read_page(DatabaseHeader::PAGE_ID as i64)?;
    if let Some(c) = c {
        pager.io.wait_for_completion(c)?;
    }

    // Flush it to Disk
    let completion = begin_write_btree_page(pager, &page)?;
    pager.io.wait_for_completion(completion)?;

    pager.clear_page_cache(true);

    if matches!(new_mode, JournalMode::ExperimentalMvcc) {
        let mv_store = open_mv_store(pager.io.as_ref(), db_path)?;
        program.connection.db.mv_store.store(Some(mv_store.clone()));
        program.connection.demote_to_mvcc_connection();
        mv_store.bootstrap(program.connection.clone())?;
    }

    if matches!(new_mode, JournalMode::Wal) {
        program.connection.db.mv_store.store(None);
    }

    Ok(new_mode)
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
