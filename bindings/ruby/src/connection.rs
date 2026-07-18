use magnus::{data_type_builder, DataType, DataTypeFunctions, Error, Ruby, TypedData, Value};
use std::sync::Arc;
use turso_sdk_kit::rsapi::TursoConnection;

use crate::error::from_turso_error;
use crate::statement::Statement;
use crate::ERROR_CLASSES;

pub struct Connection {
    inner: Arc<TursoConnection>,
}

impl DataTypeFunctions for Connection {}

unsafe impl TypedData for Connection {
    fn class(_ruby: &Ruby) -> magnus::RClass {
        let raw = crate::connection_class();
        magnus::RClass::from_value(unsafe { std::mem::transmute::<usize, Value>(raw) }).unwrap()
    }

    fn data_type() -> &'static DataType {
        static DATA_TYPE: DataType = data_type_builder!(Connection, "connection").build();
        &DATA_TYPE
    }
}

impl Connection {
    pub fn from_arc(inner: Arc<TursoConnection>) -> Self {
        Self { inner }
    }

    pub fn prepare_single(&self, sql: String) -> Result<magnus::typed_data::Obj<Statement>, Error> {
        let classes = ERROR_CLASSES.get().expect("ERROR_CLASSES not initialized");
        let stmt = self
            .inner
            .prepare_single(&sql)
            .map_err(|e| from_turso_error(e, classes))?;
        let ruby = unsafe { Ruby::get_unchecked() };
        Ok(ruby.obj_wrap(Statement::new(stmt)))
    }

    pub fn get_auto_commit(&self) -> bool {
        self.inner.get_auto_commit()
    }

    pub fn last_insert_rowid(&self) -> i64 {
        self.inner.last_insert_rowid()
    }

    pub fn set_busy_timeout(&self, ms: u32) {
        self.inner
            .set_busy_timeout(std::time::Duration::from_millis(ms as u64));
    }

    pub fn set_query_timeout(&self, ms: u32) {
        self.inner
            .set_query_timeout(std::time::Duration::from_millis(ms as u64));
    }

    pub fn interrupt(&self) {
        self.inner.interrupt();
    }

    pub fn close(&self) -> Result<(), Error> {
        let classes = ERROR_CLASSES.get().expect("ERROR_CLASSES not initialized");
        self.inner
            .close()
            .map_err(|e| from_turso_error(e, classes))?;
        Ok(())
    }
}
