use magnus::value::Lazy;
use magnus::{exception::ExceptionClass, Class, Error, Module, Ruby};

use turso_sdk_kit::rsapi::{TursoError, TursoStatusCode};

static ERROR: Lazy<ExceptionClass> = Lazy::new(|ruby| {
    ruby.define_module("Turso")
        .unwrap()
        .const_get("Error")
        .unwrap()
});
static BUSY_ERROR: Lazy<ExceptionClass> = Lazy::new(|ruby| {
    ruby.define_module("Turso")
        .unwrap()
        .const_get("BusyError")
        .unwrap()
});
static INTERRUPT_ERROR: Lazy<ExceptionClass> = Lazy::new(|ruby| {
    ruby.define_module("Turso")
        .unwrap()
        .const_get("InterruptError")
        .unwrap()
});
static MISUSE_ERROR: Lazy<ExceptionClass> = Lazy::new(|ruby| {
    ruby.define_module("Turso")
        .unwrap()
        .const_get("MisuseError")
        .unwrap()
});
static CONSTRAINT_ERROR: Lazy<ExceptionClass> = Lazy::new(|ruby| {
    ruby.define_module("Turso")
        .unwrap()
        .const_get("ConstraintError")
        .unwrap()
});
static READONLY_ERROR: Lazy<ExceptionClass> = Lazy::new(|ruby| {
    ruby.define_module("Turso")
        .unwrap()
        .const_get("ReadonlyError")
        .unwrap()
});
static DATABASE_FULL_ERROR: Lazy<ExceptionClass> = Lazy::new(|ruby| {
    ruby.define_module("Turso")
        .unwrap()
        .const_get("DatabaseFullError")
        .unwrap()
});
static NOT_ADB_ERROR: Lazy<ExceptionClass> = Lazy::new(|ruby| {
    ruby.define_module("Turso")
        .unwrap()
        .const_get("NotAdbError")
        .unwrap()
});
static CORRUPT_ERROR: Lazy<ExceptionClass> = Lazy::new(|ruby| {
    ruby.define_module("Turso")
        .unwrap()
        .const_get("CorruptError")
        .unwrap()
});
static STATEMENT_CLOSED_ERROR: Lazy<ExceptionClass> = Lazy::new(|ruby| {
    ruby.define_module("Turso")
        .unwrap()
        .const_get("StatementClosedError")
        .unwrap()
});
static NOT_SUPPORTED_ERROR: Lazy<ExceptionClass> = Lazy::new(|ruby| {
    ruby.define_module("Turso")
        .unwrap()
        .const_get("NotSupportedError")
        .unwrap()
});

pub fn define_exceptions(ruby: &Ruby, module: &impl Module) -> Result<(), Error> {
    let error = module.define_class("Error", ruby.exception_standard_error().as_r_class())?;
    module.define_class("BusyError", error)?;
    module.define_class("InterruptError", error)?;
    module.define_class("MisuseError", error)?;
    module.define_class("ConstraintError", error)?;
    module.define_class("ReadonlyError", error)?;
    module.define_class("DatabaseFullError", error)?;
    module.define_class("NotAdbError", error)?;
    module.define_class("CorruptError", error)?;
    module.define_class("StatementClosedError", error)?;
    module.define_class("NotSupportedError", ruby.exception_runtime_error().as_r_class())?;
    Ok(())
}

pub fn statement_closed_error() -> Error {
    let ruby = Ruby::get().expect("Ruby not initialized");
    Error::new(
        ruby.get_inner(&STATEMENT_CLOSED_ERROR),
        "Statement has been finalized",
    )
}

pub fn not_supported_error(msg: &str) -> Error {
    let ruby = Ruby::get().expect("Ruby not initialized");
    Error::new(ruby.get_inner(&NOT_SUPPORTED_ERROR), msg.to_string())
}

pub fn map_turso_error(err: TursoError) -> Error {
    let ruby = Ruby::get().expect("Ruby not initialized");

    let code_str = match err.code {
        TursoStatusCode::Busy => "BUSY",
        TursoStatusCode::Interrupt => "INTERRUPT",
        TursoStatusCode::Misuse => "MISUSE",
        TursoStatusCode::Constraint => "CONSTRAINT",
        TursoStatusCode::Readonly => "READONLY",
        TursoStatusCode::DatabaseFull => "FULL",
        TursoStatusCode::NotAdb => "NOTADB",
        TursoStatusCode::Corrupt => "CORRUPT",
        _ => "ERROR",
    };

    let message = err.message.clone().unwrap_or_default();

    // Log the error before raising it to Ruby. We use WARN here because 
    // these are mapped to Ruby exceptions which might be handled.
    tracing::warn!(
        target: "turso::ruby",
        error_code = code_str,
        error_message = %message,
        "Database error"
    );

    let exc_class = match err.code {
        TursoStatusCode::Busy => ruby.get_inner(&BUSY_ERROR),
        TursoStatusCode::Interrupt => ruby.get_inner(&INTERRUPT_ERROR),
        TursoStatusCode::Error => ruby.get_inner(&ERROR),
        TursoStatusCode::Misuse => ruby.get_inner(&MISUSE_ERROR),
        TursoStatusCode::Constraint => ruby.get_inner(&CONSTRAINT_ERROR),
        TursoStatusCode::Readonly => ruby.get_inner(&READONLY_ERROR),
        TursoStatusCode::DatabaseFull => ruby.get_inner(&DATABASE_FULL_ERROR),
        TursoStatusCode::NotAdb => ruby.get_inner(&NOT_ADB_ERROR),
        TursoStatusCode::Corrupt => ruby.get_inner(&CORRUPT_ERROR),
        _ => ruby.get_inner(&ERROR),
    };

    Error::new(exc_class, message)
}
