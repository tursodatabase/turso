use magnus::{
    function, gc, method, prelude::*, value::Opaque, Error, Module, RModule, Ruby, Symbol, Value,
};
use once_cell::sync::Lazy;
use std::sync::mpsc;
use std::sync::Mutex;
use turso_sdk_kit::rsapi;

static LOGGER_CALLBACK: Lazy<Mutex<Option<Opaque<Value>>>> = Lazy::new(|| Mutex::new(None));

pub struct OwnedTursoLog {
    pub level: String,
    pub message: String,
    pub target: String,
    pub timestamp: u64,
    pub file: String,
    pub line: usize,
}

static LOG_CHANNEL: Lazy<Mutex<Option<(mpsc::SyncSender<OwnedTursoLog>, Mutex<mpsc::Receiver<OwnedTursoLog>>)>>> =
    Lazy::new(|| Mutex::new(None));

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
    pub fn from_owned(log: OwnedTursoLog) -> Self {
        Self {
            level: log.level,
            message: log.message,
            target: log.target,
            timestamp: log.timestamp,
            file: log.file,
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
    gc::register_mark_object(callback);

    let mut logger_guard = LOGGER_CALLBACK.lock().unwrap();
    *logger_guard = Some(Opaque::from(callback));

    let (tx, rx) = mpsc::sync_channel(2048);
    let mut channel_guard = LOG_CHANNEL.lock().unwrap();
    *channel_guard = Some((tx, Mutex::new(rx)));

    rsapi::turso_setup(rsapi::TursoSetupConfig {
        logger: Some(Box::new(move |log| {
            if let Ok(guard) = LOG_CHANNEL.try_lock() {
                if let Some((sender, _)) = &*guard {
                    let owned = OwnedTursoLog {
                        level: log.level.to_string(),
                        message: log.message.to_string(),
                        target: log.target.to_string(),
                        timestamp: log.timestamp,
                        file: log.file.to_string(),
                        line: log.line,
                    };
                    let _ = sender.try_send(owned);
                }
            }
        })),
        log_level: Some(level),
    })
    .map_err(|e| Error::new(ruby.exception_runtime_error(), e.to_string()))?;

    Ok(())
}

pub fn _native_poll_logs(ruby: &Ruby) -> Result<(), Error> {
    let Ok(guard) = LOG_CHANNEL.try_lock() else { return Ok(()) };
    let Some((_, rx_mutex)) = &*guard else { return Ok(()) };
    let Ok(rx) = rx_mutex.try_lock() else { return Ok(()) };

    while let Ok(log) = rx.try_recv() {
        let callback_guard = LOGGER_CALLBACK.lock().unwrap();
        if let Some(opaque_callback) = *callback_guard {
            let callback = ruby.get_inner(opaque_callback);
            if !callback.is_nil() {
                let rb_log = RbTursoLog::from_owned(log);
                let rb_log_obj = ruby.obj_wrap(rb_log);

                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    if let Err(e) = callback.funcall::<_, _, Value>("call", (rb_log_obj,)) {
                        eprintln!("[Turso] Logger callback failed: {}", e);
                    }
                })).map_err(|e| {
                    eprintln!("[Turso] Logger callback panicked: {:?}", e);
                });
            }
        }
    }
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
    module.define_module_function("_native_poll_logs", function!(_native_poll_logs, 0))?;
    module.define_module_function("_clear_logger", function!(_clear_logger, 0))?;

    Ok(())
}

pub fn _clear_logger() {
    if let Ok(mut guard) = LOGGER_CALLBACK.try_lock() {
        *guard = None;
    }
}
