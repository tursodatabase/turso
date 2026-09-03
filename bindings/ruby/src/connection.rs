use magnus::{
    data_type_builder, DataType, DataTypeFunctions, Error, RArray, Ruby, TypedData, Value,
};
use std::sync::Arc;
use turso_sdk_kit::rsapi::TursoConnection;

use crate::blocking_op::run_blocking;
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
        let inner = self.inner.clone();
        let stmt = run_blocking(classes, move || inner.prepare_single(&sql))?;
        let ruby = unsafe { Ruby::get_unchecked() };
        Ok(ruby.obj_wrap(Statement::new(stmt)))
    }

    pub fn execute_batch(&self, sql: String) -> Result<RArray, Error> {
        let classes = ERROR_CLASSES.get().expect("ERROR_CLASSES not initialized");
        let inner = self.inner.clone();
        let results: Vec<(i64, i64)> = run_blocking(classes, move || {
            let mut results = Vec::new();
            let mut offset: usize = 0;
            while let Some((mut stmt, consumed)) = inner.prepare_first(&sql[offset..])? {
                let info = stmt.execute(None)?;
                results.push((info.rows_changed as i64, inner.last_insert_rowid()));
                offset += consumed;
            }
            Ok(results)
        })?;
        let ruby = unsafe { Ruby::get_unchecked() };
        let ary = ruby.ary_new();
        for (changes, rowid) in results {
            let hash = ruby.hash_new();
            hash.aset(ruby.intern("changes"), ruby.integer_from_i64(changes))?;
            hash.aset(ruby.intern("last_insert_rowid"), ruby.integer_from_i64(rowid))?;
            ary.push(hash)?;
        }
        Ok(ary)
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

    pub fn get_query_timeout(&self) -> u64 {
        self.inner.get_query_timeout().as_millis() as u64
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
        let inner = self.inner.clone();
        run_blocking(classes, move || inner.close())
    }
}
