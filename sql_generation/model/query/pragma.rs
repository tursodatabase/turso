use std::fmt::Display;

pub enum Pragma {
    AutoVacuumMode(VacuumMode),
}

pub enum VacuumMode {
    None,
    Incremental,
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

                write!(f, "PRAGMA auto_vacuum={mode} ")?;
                Ok(())
            }
        }
    }
}
