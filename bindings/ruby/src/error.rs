use magnus::{value::ReprValue, ExceptionClass, Module, Ruby, Value};
use std::mem::transmute;
use turso_sdk_kit::rsapi::TursoError;

pub struct ErrorClasses {
    base: usize,
    busy: usize,
    busy_snapshot: usize,
    interrupt: usize,
    constraint: usize,
    readonly: usize,
    misuse: usize,
    database_full: usize,
    not_a_database: usize,
    corrupt: usize,
    io: usize,
}

impl ErrorClasses {
    pub fn define(ruby: &Ruby, module: &impl Module) -> Result<Self, magnus::Error> {
        let base = module.define_error("Exception", ruby.exception_standard_error())?;
        let base_val = val_to_usize(base.as_value());
        let sub = |name: &str| -> Result<usize, magnus::Error> {
            let class = module.define_error(
                name,
                ExceptionClass::from_value(unsafe { transmute::<usize, Value>(base_val) }).unwrap(),
            )?;
            Ok(val_to_usize(class.as_value()))
        };
        Ok(ErrorClasses {
            base: base_val,
            busy: sub("BusyException")?,
            busy_snapshot: sub("BusySnapshotException")?,
            interrupt: sub("InterruptException")?,
            constraint: sub("ConstraintException")?,
            readonly: sub("ReadonlyException")?,
            misuse: sub("MisuseException")?,
            database_full: sub("DatabaseFullException")?,
            not_a_database: sub("NotADatabaseException")?,
            corrupt: sub("CorruptException")?,
            io: sub("IoException")?,
        })
    }

    fn exc(&self, raw: usize) -> ExceptionClass {
        ExceptionClass::from_value(unsafe { transmute::<usize, Value>(raw) }).unwrap()
    }

    pub fn base(&self) -> ExceptionClass {
        self.exc(self.base)
    }
    pub fn busy(&self) -> ExceptionClass {
        self.exc(self.busy)
    }
    pub fn busy_snapshot(&self) -> ExceptionClass {
        self.exc(self.busy_snapshot)
    }
    pub fn interrupt(&self) -> ExceptionClass {
        self.exc(self.interrupt)
    }
    pub fn constraint(&self) -> ExceptionClass {
        self.exc(self.constraint)
    }
    pub fn readonly(&self) -> ExceptionClass {
        self.exc(self.readonly)
    }
    pub fn misuse(&self) -> ExceptionClass {
        self.exc(self.misuse)
    }
    pub fn database_full(&self) -> ExceptionClass {
        self.exc(self.database_full)
    }
    pub fn not_a_database(&self) -> ExceptionClass {
        self.exc(self.not_a_database)
    }
    pub fn corrupt(&self) -> ExceptionClass {
        self.exc(self.corrupt)
    }
    pub fn io(&self) -> ExceptionClass {
        self.exc(self.io)
    }
}

fn val_to_usize(val: Value) -> usize {
    unsafe { transmute::<Value, usize>(val) }
}

pub fn from_turso_error(err: TursoError, classes: &ErrorClasses) -> magnus::Error {
    let class = match err {
        TursoError::Busy(_) => classes.busy(),
        TursoError::BusySnapshot(_) => classes.busy_snapshot(),
        TursoError::Interrupt(_) => classes.interrupt(),
        TursoError::Constraint(_) => classes.constraint(),
        TursoError::Readonly(_) => classes.readonly(),
        TursoError::Misuse(_) => classes.misuse(),
        TursoError::DatabaseFull(_) => classes.database_full(),
        TursoError::NotAdb(_) => classes.not_a_database(),
        TursoError::Corrupt(_) => classes.corrupt(),
        TursoError::IoError(..) => classes.io(),
        TursoError::Error(_) => classes.base(),
    };
    magnus::Error::new(class, err.to_string())
}
