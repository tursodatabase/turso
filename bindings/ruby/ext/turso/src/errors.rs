use magnus::{exception::ExceptionClass, Error, Module, Ruby};
use std::cell::RefCell;
use turso_sdk_kit::rsapi::{TursoError, TursoStatusCode};

pub fn define_exceptions(ruby: &Ruby, module: &impl Module) -> Result<(), Error> {
    let error = module.define_class("Error", ruby.exception_standard_error())?;
    module.define_class("BusyError", error)?;
    module.define_class("InterruptError", error)?;
    module.define_class("MisuseError", error)?;
    module.define_class("ConstraintError", error)?;
    module.define_class("ReadonlyError", error)?;
    module.define_class("DatabaseFullError", error)?;
    module.define_class("NotAdbError", error)?;
    module.define_class("CorruptError", error)?;
    module.define_class("StatementClosedError", error)?;
    Ok(())
}

pub fn statement_closed_error() -> Error {
    let ruby = Ruby::get().expect("Ruby not initialized");
    let module = ruby.define_module("Turso").expect("Turso module not found");
    let exc_class: ExceptionClass = module
        .const_get("StatementClosedError")
        .expect("StatementClosedError not found");
    Error::new(exc_class, "Statement has been finalized")
}

pub fn map_turso_error(err: TursoError) -> Error {
    let ruby = Ruby::get().expect("Ruby not initialized");
    let module = ruby.define_module("Turso").expect("Turso module not found");

    let class_name = match err.code {
        TursoStatusCode::Busy => "BusyError",
        TursoStatusCode::Interrupt => "InterruptError",
        TursoStatusCode::Error => "Error",
        TursoStatusCode::Misuse => "MisuseError",
        TursoStatusCode::Constraint => "ConstraintError",
        TursoStatusCode::Readonly => "ReadonlyError",
        TursoStatusCode::DatabaseFull => "DatabaseFullError",
        TursoStatusCode::NotAdb => "NotAdbError",
        TursoStatusCode::Corrupt => "CorruptError",
        _ => "Error",
    };

    let exc_class: ExceptionClass = module
        .const_get(class_name)
        .expect("Error class not found");

    Error::new(exc_class, err.message.unwrap_or_default())
}
