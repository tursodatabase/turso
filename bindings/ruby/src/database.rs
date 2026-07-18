use magnus::{
    data_type_builder, DataType, DataTypeFunctions, Error, RHash, Ruby, TypedData, Value,
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

        let config = TursoDatabaseConfig {
            path: path.clone(),
            experimental_features,
            async_io: false,
            encryption: None,
            vfs: IoBackend::Default,
            io: None,
            db_file: None,
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
        Ok(())
    }

    pub fn path(&self) -> String {
        self.inner.path.clone()
    }

    pub fn is_open(&self) -> bool {
        true
    }

    pub fn last_insert_rowid(&self) -> i64 {
        self.inner.conn.last_insert_rowid()
    }

    pub fn in_transaction(&self) -> bool {
        !self.inner.conn.get_auto_commit()
    }

    pub fn connection(&self) -> magnus::typed_data::Obj<crate::connection::Connection> {
        let ruby = unsafe { Ruby::get_unchecked() };
        ruby.obj_wrap(crate::connection::Connection::from_arc(
            self.inner.conn.clone(),
        ))
    }
}
