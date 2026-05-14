use std::fmt::Display;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Pragma {
    AutoVacuumMode(VacuumMode),
    ForeignKeyList(String),
    WalCheckpoint(WalCheckpointMode),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum VacuumMode {
    None,
    Incremental,
    Full,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalCheckpointMode {
    Passive,
    Full,
    Restart,
    Truncate,
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

                write!(f, "PRAGMA auto_vacuum={mode}")?;
                Ok(())
            }
            Pragma::ForeignKeyList(table_name) => {
                let table_name = table_name.replace('\'', "''");
                write!(f, "PRAGMA foreign_key_list('{table_name}')")
            }
            Pragma::WalCheckpoint(mode) => {
                let mode = match mode {
                    WalCheckpointMode::Passive => "PASSIVE",
                    WalCheckpointMode::Full => "FULL",
                    WalCheckpointMode::Restart => "RESTART",
                    WalCheckpointMode::Truncate => "TRUNCATE",
                };

                write!(f, "PRAGMA wal_checkpoint({mode})")
            }
        }
    }
}
