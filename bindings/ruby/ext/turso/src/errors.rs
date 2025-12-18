
use magnus::{exception::ExceptionClass, Error, Module, Ruby};
use std::cell::RefCell;
use turso_sdk_kit::rsapi::{TursoError, TursoStatusCode};

struct ExceptionClasses {
    error: ExceptionClass,
    busy: ExceptionClass,
    constraint: ExceptionClass,
    corrupt: ExceptionClass,
    misuse: ExceptionClass,
    not_a_db: ExceptionClass,
    readonly: ExceptionClass,
    statement_closed: ExceptionClass,
}

thread_local! {
    static EXCEPTION_CLASSES: RefCell<Option<ExceptionClasses>> = const { RefCell::new(None) };
}

pub fn define_exceptions(ruby: &Ruby, module: &impl Module) -> Result<(), Error> {
    let error = module.define_error("Error", ruby.exception_standard_error())?;
    let busy = module.define_error("BusyError", error)?;
    let constraint = module.define_error("ConstraintError", error)?;
    let corrupt = module.define_error("CorruptError", error)?;
    let misuse = module.define_error("MisuseError", error)?;
    let not_a_db = module.define_error("NotADatabaseError", error)?;
    let readonly = module.define_error("ReadonlyError", error)?;
    let statement_closed = module.define_error("StatementClosedError", error)?;

    EXCEPTION_CLASSES.with(|cell| {
        *cell.borrow_mut() = Some(ExceptionClasses {
            error,
            busy,
            constraint,
            corrupt,
            misuse,
            not_a_db,
            readonly,
            statement_closed,
        });
    });

    Ok(())
}

pub fn statement_closed_error() -> Error {
    EXCEPTION_CLASSES.with(|cell| {
        let classes = cell.borrow();
        let classes = classes.as_ref().expect("Exception classes not initialized");
        Error::new(classes.statement_closed, "Statement has been finalized")
    })
}

pub fn turso_error(message: impl Into<String>) -> Error {
    let msg = message.into();
    EXCEPTION_CLASSES.with(|cell| {
        let classes = cell.borrow();
        let classes = classes.as_ref().expect("Exception classes not initialized");
        Error::new(classes.error, msg)
    })
}

pub fn map_turso_error(err: TursoError) -> Error {
    let message = err.message.unwrap_or_else(|| format!("{:?}", err.code));

    EXCEPTION_CLASSES.with(|cell| {
        let classes = cell.borrow();
        let classes = classes.as_ref().expect("Exception classes not initialized");

        let exc_class = match err.code {
            TursoStatusCode::Busy => classes.busy,
            TursoStatusCode::Constraint => classes.constraint,
            TursoStatusCode::Corrupt => classes.corrupt,
            TursoStatusCode::Misuse => classes.misuse,
            TursoStatusCode::NotAdb => classes.not_a_db,
            TursoStatusCode::Readonly => classes.readonly,
            _ => classes.error,
        };

        Error::new(exc_class, message)
    })
}
