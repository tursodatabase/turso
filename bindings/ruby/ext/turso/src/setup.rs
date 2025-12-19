use magnus::{
    function, method, prelude::*, value::Opaque, Error, Module, RModule, Ruby, Symbol, Value,
};
use once_cell::sync::Lazy;
use std::sync::Mutex;
use turso_sdk_kit::rsapi;

static LOGGER_CALLBACK: Lazy<Mutex<Option<Opaque<Value>>>> = Lazy::new(|| Mutex::new(None));

#[magnus::wrap(class = "Turso::Log")]
pub struct RbTursoLog {
    level: String,
    message: String,
    target: String,
    timestamp: u64,
    file: String,
    line: usize,
}

impl RbTursoLog {
    pub fn new(log: rsapi::TursoLog) -> Self {
        Self {
            level: log.level.to_string(),
            message: log.message.to_string(),
            target: log.target.to_string(),
            timestamp: log.timestamp,
            file: log.file.to_string(),
            line: log.line,
        }
    }

    pub fn level(ruby: &Ruby, rb_self: &Self) -> Symbol {
        ruby.to_symbol(rb_self.level.to_lowercase())
    }

    pub fn message(&self) -> String {
        self.message.clone()
    }

    pub fn target(&self) -> String {
        self.target.clone()
    }

    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }

    pub fn file(&self) -> String {
        self.file.clone()
    }

    pub fn line(&self) -> usize {
        self.line
    }
}

pub fn _native_setup(ruby: &Ruby, level: String, callback: Value) -> Result<(), Error> {
    let mut logger_guard = LOGGER_CALLBACK.lock().unwrap();
    *logger_guard = Some(Opaque::from(callback));

    rsapi::turso_setup(rsapi::TursoSetupConfig {
        logger: Some(Box::new(move |log| {
            if let Ok(ruby) = Ruby::get() {
                let callback_guard = LOGGER_CALLBACK.lock().unwrap();
                if let Some(opaque_callback) = *callback_guard {
                    let callback = ruby.get_inner(opaque_callback);
                    if !callback.is_nil() {
                        let rb_log = RbTursoLog::new(log);
                        let _: Result<Value, Error> = callback.funcall("call", (rb_log,));
                    }
                }
            }
        })),
        log_level: Some(level),
    })
    .map_err(|e| Error::new(ruby.exception_runtime_error(), e.to_string()))?;

    Ok(())
}

pub fn init(ruby: &Ruby, module: RModule) -> Result<(), Error> {
    let log_class = module.define_class("Log", ruby.class_object())?;
    log_class.define_method("level", method!(RbTursoLog::level, 0))?;
    log_class.define_method("message", method!(RbTursoLog::message, 0))?;
    log_class.define_method("target", method!(RbTursoLog::target, 0))?;
    log_class.define_method("timestamp", method!(RbTursoLog::timestamp, 0))?;
    log_class.define_method("file", method!(RbTursoLog::file, 0))?;
    log_class.define_method("line", method!(RbTursoLog::line, 0))?;

    module.define_module_function("_native_setup", function!(_native_setup, 2))?;

    Ok(())
}
