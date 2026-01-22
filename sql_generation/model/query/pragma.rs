use std::fmt::Display;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Pragma {
    AutoVacuumMode(VacuumMode),
    ForeignKeyList(String),
    CaptureDataChanges(CdcMode),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum VacuumMode {
    None,
    Incremental,
    Full,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum CdcMode {
    #[default]
    Off,
    Full,
}

impl Display for Pragma {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Pragma::AutoVacuumMode(vacuum_mode) => {
                let mode = match vacuum_mode {
                    VacuumMode::None => "none",
                    VacuumMode::Incremental => "incremental",
                    VacuumMode::Full => "full",
                };

                write!(f, "PRAGMA auto_vacuum={mode}")
            }
            Pragma::ForeignKeyList(table_name) => {
                let table_name = table_name.replace('\'', "''");
                write!(f, "PRAGMA foreign_key_list('{table_name}')")
            }
            Pragma::CaptureDataChanges(cdc_mode) => {
                let mode = match cdc_mode {
                    CdcMode::Off => "off",
                    CdcMode::Full => "full",
                };

                write!(f, "PRAGMA unstable_capture_data_changes_conn('{mode}')")
            }
        }
    }
}
