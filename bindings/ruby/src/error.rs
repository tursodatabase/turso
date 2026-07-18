use magnus::{ExceptionClass, Module, Ruby};
use turso_sdk_kit::rsapi::TursoError;

pub struct ErrorClasses {
    pub base: ExceptionClass,
    pub busy: ExceptionClass,
    pub busy_snapshot: ExceptionClass,
    pub interrupt: ExceptionClass,
    pub constraint: ExceptionClass,
    pub readonly: ExceptionClass,
    pub misuse: ExceptionClass,
    pub database_full: ExceptionClass,
    pub not_a_database: ExceptionClass,
    pub corrupt: ExceptionClass,
    pub io: ExceptionClass,
}

impl ErrorClasses {
    pub fn define(ruby: &Ruby, module: &impl Module) -> Result<Self, magnus::Error> {
        let base = module.define_error("Error", ruby.exception_standard_error())?;
        Ok(ErrorClasses {
            busy: module.define_error("BusyError", base)?,
            busy_snapshot: module.define_error("BusySnapshotError", base)?,
            interrupt: module.define_error("InterruptError", base)?,
            constraint: module.define_error("ConstraintError", base)?,
            readonly: module.define_error("ReadonlyError", base)?,
            misuse: module.define_error("MisuseError", base)?,
            database_full: module.define_error("DatabaseFullError", base)?,
            not_a_database: module.define_error("NotADatabaseError", base)?,
            corrupt: module.define_error("CorruptError", base)?,
            io: module.define_error("IoError", base)?,
        })
    }
}

pub fn from_turso_error(err: TursoError, classes: &ErrorClasses) -> magnus::Error {
    let class = match err {
        TursoError::Busy(_) => classes.busy,
        TursoError::BusySnapshot(_) => classes.busy_snapshot,
        TursoError::Interrupt(_) => classes.interrupt,
        TursoError::Constraint(_) => classes.constraint,
        TursoError::Readonly(_) => classes.readonly,
        TursoError::Misuse(_) => classes.misuse,
        TursoError::DatabaseFull(_) => classes.database_full,
        TursoError::NotAdb(_) => classes.not_a_database,
        TursoError::Corrupt(_) => classes.corrupt,
        TursoError::IoError(..) => classes.io,
        TursoError::Error(_) => classes.base,
    };
    magnus::Error::new(class, err.to_string())
}
