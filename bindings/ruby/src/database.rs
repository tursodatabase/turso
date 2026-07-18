use magnus::{typed_data::Obj, DataType, DataTypeFunctions, Error, Ruby, TypedData};
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

unsafe impl DataTypeFunctions for Database {
    fn free(&mut self) {
        let _ = self.inner.conn.close();
    }
}

unsafe impl TypedData for Database {
    fn class_name() -> &'static str {
        "Turso::Database"
    }

    fn data_type() -> DataType {
        DataType::new(Self::class_name()).free_immediately(true)
    }
}

impl Database {
    pub fn new(ruby: &Ruby, path: String) -> Result<Obj<Self>, Error> {
        let classes = ERROR_CLASSES.get().expect("ERROR_CLASSES not initialized");

        let config = TursoDatabaseConfig {
            path: path.clone(),
            experimental_features: None,
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
        let inner = DatabaseInner {
            _db: db,
            conn,
            path,
        };
        Ok(Obj::wrap(ruby, Self {
            inner: Arc::new(inner),
        }))
    }

    pub fn close(&self) -> Result<(), Error> {
        let classes = ERROR_CLASSES.get().expect("ERROR_CLASSES not initialized");
        self.inner.conn.close().map_err(|e| from_turso_error(e, classes))?;
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

    pub fn changes(&self) -> i64 {
        0
    }

    pub fn total_changes(&self) -> i64 {
        0
    }

    pub fn in_transaction(&self) -> bool {
        !self.inner.conn.get_auto_commit()
    }

    pub fn connection(&self) -> Obj<crate::connection::Connection> {
        Obj::wrap(unsafe { Ruby::get_unchecked() }, crate::connection::Connection::from_arc(self.inner.conn.clone()))
    }
}
