//! Change Data Capture (CDC) configuration types.
//!
//! These types describe the `PRAGMA capture_data_changes_conn`
//! setting on a connection: which columns get captured, which table the
//! changes are written to, and which CDC schema version is in use. The
//! bytecode that writes the change records lives in `translate::emitter`.

use crate::{LimboError, Result};

pub const TURSO_CDC_DEFAULT_TABLE_NAME: &str = "turso_cdc";
pub const TURSO_CDC_VERSION_TABLE_NAME: &str = "turso_cdc_version";

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum CaptureDataChangesMode {
    Id,
    Before,
    After,
    Full,
}

/// CDC schema version with integer ordering for feature checks.
/// Higher versions are supersets of lower versions.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
#[repr(u8)]
pub enum CdcVersion {
    /// 8 columns: change_id, change_time, change_type, table_name, id, before, after, updates
    V1 = 1,
    /// 9 columns (adds change_txn_id + COMMIT records with change_type=2)
    V2 = 2,
}

pub const CDC_VERSION_CURRENT: CdcVersion = CdcVersion::V2;

impl CdcVersion {
    /// Whether this version emits COMMIT records (change_type=2)
    pub fn has_commit_record(self) -> bool {
        self >= CdcVersion::V2
    }
}

impl std::fmt::Display for CdcVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CdcVersion::V1 => write!(f, "v1"),
            CdcVersion::V2 => write!(f, "v2"),
        }
    }
}

impl std::str::FromStr for CdcVersion {
    type Err = LimboError;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "v1" => Ok(CdcVersion::V1),
            "v2" => Ok(CdcVersion::V2),
            _ => Err(LimboError::InternalError(format!(
                "unexpected CDC version: {s}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CaptureDataChangesInfo {
    pub mode: CaptureDataChangesMode,
    pub table: String,
    pub version: Option<CdcVersion>,
}

impl CaptureDataChangesInfo {
    pub fn parse(
        value: &str,
        version: Option<CdcVersion>,
    ) -> Result<Option<CaptureDataChangesInfo>> {
        let (mode, table) = value
            .split_once(",")
            .unwrap_or((value, TURSO_CDC_DEFAULT_TABLE_NAME));
        match mode {
            "off" => Ok(None),
            "id" => Ok(Some(CaptureDataChangesInfo { mode: CaptureDataChangesMode::Id, table: table.to_string(), version })),
            "before" => Ok(Some(CaptureDataChangesInfo { mode: CaptureDataChangesMode::Before, table: table.to_string(), version })),
            "after" => Ok(Some(CaptureDataChangesInfo { mode: CaptureDataChangesMode::After, table: table.to_string(), version })),
            "full" => Ok(Some(CaptureDataChangesInfo { mode: CaptureDataChangesMode::Full, table: table.to_string(), version })),
            _ => Err(LimboError::InvalidArgument(
                "unexpected pragma value: expected '<mode>' or '<mode>,<cdc-table-name>' parameter where mode is one of off|id|before|after|full".to_string(),
            ))
        }
    }
    pub fn has_updates(&self) -> bool {
        self.mode == CaptureDataChangesMode::Full
    }
    pub fn has_after(&self) -> bool {
        matches!(
            self.mode,
            CaptureDataChangesMode::After | CaptureDataChangesMode::Full
        )
    }
    pub fn has_before(&self) -> bool {
        matches!(
            self.mode,
            CaptureDataChangesMode::Before | CaptureDataChangesMode::Full
        )
    }
    pub fn mode_name(&self) -> &str {
        match self.mode {
            CaptureDataChangesMode::Id => "id",
            CaptureDataChangesMode::Before => "before",
            CaptureDataChangesMode::After => "after",
            CaptureDataChangesMode::Full => "full",
        }
    }
    pub fn cdc_version(&self) -> CdcVersion {
        self.version.unwrap_or(CDC_VERSION_CURRENT)
    }
}

/// Convenience methods for `Option<CaptureDataChangesInfo>` to keep call sites simple.
pub trait CaptureDataChangesExt {
    fn has_updates(&self) -> bool;
    fn has_after(&self) -> bool;
    fn has_before(&self) -> bool;
    fn table(&self) -> Option<&str>;
}

impl CaptureDataChangesExt for Option<CaptureDataChangesInfo> {
    fn has_updates(&self) -> bool {
        self.as_ref().is_some_and(|i| i.has_updates())
    }
    fn has_after(&self) -> bool {
        self.as_ref().is_some_and(|i| i.has_after())
    }
    fn has_before(&self) -> bool {
        self.as_ref().is_some_and(|i| i.has_before())
    }
    fn table(&self) -> Option<&str> {
        self.as_ref().map(|i| i.table.as_str())
    }
}
