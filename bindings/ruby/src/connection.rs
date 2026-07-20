use magnus::{data_type_builder, DataType, DataTypeFunctions, Error, RArray, Ruby, TypedData, Value};
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

    pub fn execute_batch(&self, sql: String) -> Result<RArray, Error> {
        let classes = ERROR_CLASSES.get().expect("ERROR_CLASSES not initialized");
        let mut results = RArray::new();
        let mut offset: usize = 0;
        loop {
            match self
                .inner
                .prepare_first(&sql[offset..])
                .map_err(|e| from_turso_error(e, classes))?
            {
                Some((mut stmt, consumed)) => {
                    let info = stmt
                        .execute(None)
                        .map_err(|e| from_turso_error(e, classes))?;
                    let ruby = unsafe { Ruby::get_unchecked() };
                    let hash = ruby.hash_new();
                    hash.aset(
                        ruby.intern("changes"),
                        ruby.integer_from_i64(info.rows_changed as i64),
                    )?;
                    hash.aset(
                        ruby.intern("last_insert_rowid"),
                        ruby.integer_from_i64(self.inner.last_insert_rowid()),
                    )?;
                    results.push(hash)?;
                    offset += consumed;
                }
                None => break,
            }
        }
        Ok(results)
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

    pub fn total_changes(&self) -> i64 {
        self.inner.total_changes()
    }

    pub fn changes(&self) -> i64 {
        self.inner.changes()
    }

    pub fn close(&self) -> Result<(), Error> {
        let classes = ERROR_CLASSES.get().expect("ERROR_CLASSES not initialized");
        self.inner
            .close()
            .map_err(|e| from_turso_error(e, classes))?;
        Ok(())
    }
}
