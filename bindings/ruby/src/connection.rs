use magnus::{typed_data::Obj, DataType, DataTypeFunctions, Error, Ruby, TypedData};
use std::sync::Arc;
use turso_sdk_kit::rsapi::TursoConnection;

use crate::error::from_turso_error;
use crate::statement::Statement;
use crate::ERROR_CLASSES;

pub struct Connection {
    inner: Arc<TursoConnection>,
}

unsafe impl DataTypeFunctions for Connection {}

unsafe impl TypedData for Connection {
    fn class_name() -> &'static str {
        "Turso::Connection"
    }

    fn data_type() -> DataType {
        DataType::new(Self::class_name())
    }
}

impl Connection {
    pub fn from_arc(inner: Arc<TursoConnection>) -> Self {
        Self { inner }
    }

    pub fn prepare_single(&self, sql: String) -> Result<Obj<Statement>, Error> {
        let classes = ERROR_CLASSES.get().expect("ERROR_CLASSES not initialized");
        let stmt = self.inner.prepare_single(&sql)
            .map_err(|e| from_turso_error(e, classes))?;
        Ok(Obj::wrap(unsafe { Ruby::get_unchecked() }, Statement::new(stmt)))
    }

    pub fn get_auto_commit(&self) -> bool {
        self.inner.get_auto_commit()
    }

    pub fn is_readonly(&self) -> bool {
        self.inner.is_readonly(0)
    }

    pub fn last_insert_rowid(&self) -> i64 {
        self.inner.last_insert_rowid()
    }

    pub fn set_busy_timeout(&self, ms: u32) {
        self.inner.set_busy_timeout(std::time::Duration::from_millis(ms as u64));
    }

    pub fn set_query_timeout(&self, ms: u32) {
        self.inner.set_query_timeout(std::time::Duration::from_millis(ms as u64));
    }

    pub fn interrupt(&self) {
        self.inner.interrupt();
    }

    pub fn close(&self) -> Result<(), Error> {
        let classes = ERROR_CLASSES.get().expect("ERROR_CLASSES not initialized");
        self.inner.close().map_err(|e| from_turso_error(e, classes))?;
        Ok(())
    }
}
