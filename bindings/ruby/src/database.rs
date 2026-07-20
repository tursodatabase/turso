use magnus::{
    data_type_builder, DataType, DataTypeFunctions, Error, RHash, Ruby, Symbol, TypedData, Value,
};
use std::sync::Arc;
use turso_sdk_kit::rsapi::{TursoConnection, TursoDatabase, TursoDatabaseConfig};
use turso_sdk_kit::IoBackend;

use crate::error::from_turso_error;
use crate::ERROR_CLASSES;

pub struct Database {
    inner: Arc<DatabaseInner>,
}

struct DatabaseInner {
    _db: Arc<TursoDatabase>,
    conn: Arc<TursoConnection>,
    path: String,
    closed: std::sync::atomic::AtomicBool,
}

impl DataTypeFunctions for Database {
    fn free(self: Box<Self>) {
        let _ = self.inner.conn.close();
    }
}

unsafe impl TypedData for Database {
    fn class(_ruby: &Ruby) -> magnus::RClass {
        let raw = crate::database_class();
        magnus::RClass::from_value(unsafe { std::mem::transmute::<usize, Value>(raw) }).unwrap()
    }

    fn data_type() -> &'static DataType {
        static DATA_TYPE: DataType = data_type_builder!(Database, "database")
            .free_immediately()
            .build();
        &DATA_TYPE
    }
}

impl Database {
    pub fn new(
        ruby: &Ruby,
        path: String,
        opts: RHash,
    ) -> Result<magnus::typed_data::Obj<Self>, Error> {
        let classes = ERROR_CLASSES.get().expect("ERROR_CLASSES not initialized");

        let experimental_features: Option<String> = opts.aref("experimental_features")?;
        let busy_timeout_ms: Option<u64> = opts.aref("busy_timeout")?;
        let query_timeout_ms: Option<u64> = opts.aref("query_timeout")?;
        let readonly: Option<bool> = opts.aref("readonly")?;
        let file_must_exist: Option<bool> = opts.aref("file_must_exist")?;

        let vfs: Option<Value> = opts.aref("vfs")?;
        let vfs = vfs.and_then(|v| {
            if let Some(sym) = Symbol::from_value(v) {
                sym.name().ok()
            } else {
                String::try_convert(v).ok()
            }
        });

        let encryption: Option<RHash> = opts.aref("encryption")?;
        let encryption = encryption.map(|e| -> Result<turso_sdk_kit::rsapi::EncryptionOpts, magnus::Error> {
            let cipher: String = e.aref("cipher").ok_or_else(|| {
                magnus::Error::new(ruby.exception_arg_error(), "encryption requires cipher")
            })?;
            let hexkey: String = e.aref("hexkey").ok_or_else(|| {
                magnus::Error::new(ruby.exception_arg_error(), "encryption requires hexkey")
            })?;
            Ok(turso_sdk_kit::rsapi::EncryptionOpts { cipher, hexkey })
        }).transpose()?;

        let config = TursoDatabaseConfig {
            path: path.clone(),
            experimental_features,
            async_io: false,
            encryption,
            vfs: match vfs.as_deref() {
                Some("memory") => IoBackend::Memory,
                Some("syscall") => IoBackend::Syscall,
                Some("io_uring") => IoBackend::IoUring,
                _ => IoBackend::Default,
            },
            io: None,
            db_file: None,
            readonly: readonly.unwrap_or(false),
            file_must_exist: file_must_exist.unwrap_or(false),
        };
        let db = TursoDatabase::new(config);
        let result = db.open().map_err(|e| from_turso_error(e, classes))?;
        debug_assert!(!result.is_io());
        let conn = db.connect().map_err(|e| from_turso_error(e, classes))?;

        if let Some(ms) = busy_timeout_ms {
            conn.set_busy_timeout(std::time::Duration::from_millis(ms));
        }
        if let Some(ms) = query_timeout_ms {
            conn.set_query_timeout(std::time::Duration::from_millis(ms));
        }

        let inner = DatabaseInner {
            _db: db,
            conn,
            path,
            closed: std::sync::atomic::AtomicBool::new(false),
        };
        Ok(ruby.obj_wrap(Self {
            inner: Arc::new(inner),
        }))
    }

    pub fn close(&self) -> Result<(), Error> {
        let classes = ERROR_CLASSES.get().expect("ERROR_CLASSES not initialized");
        self.inner
            .conn
            .close()
            .map_err(|e| from_turso_error(e, classes))?;
        self.inner.closed.store(true, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    pub fn path(&self) -> String {
        self.inner.path.clone()
    }

    pub fn is_open(&self) -> bool {
        !self.inner.closed.load(std::sync::atomic::Ordering::Acquire)
    }

    pub fn last_insert_rowid(&self) -> i64 {
        self.inner.conn.last_insert_rowid()
    }

    pub fn in_transaction(&self) -> bool {
        !self.inner.conn.get_auto_commit()
    }

    pub fn total_changes(&self) -> i64 {
        self.inner.conn.total_changes()
    }

    pub fn connection(&self) -> magnus::typed_data::Obj<crate::connection::Connection> {
        let ruby = unsafe { Ruby::get_unchecked() };
        ruby.obj_wrap(crate::connection::Connection::from_arc(
            self.inner.conn.clone(),
        ))
    }
}
