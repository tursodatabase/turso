use crate::util::IOExt;
use crate::{bail_corrupt_error, LimboError};
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

// As this is not a performance critical function, I made it synchronous simplify the code
// and not have to add an additional state machine
pub fn change_mode(pager: &Pager, new_mode: JournalMode) -> Result<JournalMode> {
    if !new_mode.supported() {
        return Err(crate::LimboError::ParseError(format!(
            "Journal Mode `{new_mode}` is not supported"
        )));
    }

    let prev_mode = pager
        .io
        .block(|| pager.with_header_mut(|header| header.read_version))?;
    let prev_mode = JournalMode::try_from(prev_mode)?;

    if prev_mode == new_mode {
        return Ok(new_mode);
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
