use magnus::typed_data::Obj;
use magnus::value::ReprValue;
use magnus::{method, Error, IntoValue, Module, Ruby, Value};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use turso_sdk_kit::rsapi::TursoConnection;

use crate::errors::map_turso_error;
use crate::statement::RbStatement;


#[magnus::wrap(class = "Turso::RbConnection", free_immediately, size)]
pub struct RbConnection {
    inner: Arc<TursoConnection>,
    closed: AtomicBool,
}

impl RbConnection {
    pub fn new(conn: Arc<TursoConnection>) -> Self {
        Self {
            inner: conn,
            closed: AtomicBool::new(false),
        }
    }

    fn ensure_open(&self) -> Result<(), Error> {
        if self.closed.load(Ordering::Relaxed) {
            let ruby = Ruby::get().expect("Ruby not initialized");
            Err(Error::new(
                ruby.exception_runtime_error(),
                "Connection is closed",
            ))
        } else {
            Ok(())
        }
    }

    pub fn prepare(&self, sql: String) -> Result<RbStatement, Error> {
        self.ensure_open()?;
        let stmt = self.inner.prepare_single(&sql).map_err(map_turso_error)?;
        Ok(RbStatement::new(stmt))
    }

    pub fn prepare_first(ruby: &Ruby, rb_self: Obj<Self>, sql: String) -> Result<Value, Error> {
        let this = &*rb_self;
        this.ensure_open()?;
        match this.inner.prepare_first(&sql).map_err(map_turso_error)? {
            Some((stmt, tail_idx)) => {
                let rb_stmt = RbStatement::new(stmt);
                let rb_stmt_obj = ruby.obj_wrap(rb_stmt);
                Ok(ruby
                    .ary_new_from_values(&[rb_stmt_obj.as_value(), tail_idx.into_value_with(ruby)])
                    .as_value())
            }
            None => Ok(ruby.qnil().as_value()),
        }
    }

    /// Number of rows changed by last statement.
    pub fn changes(&self) -> u64 {
        self.inner.changes()
    }

    pub fn last_insert_row_id(&self) -> i64 {
        self.inner.last_insert_rowid()
    }

    pub fn in_transaction(&self) -> bool {
        !self.inner.get_auto_commit()
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Relaxed)
    }

    pub fn close(&self) -> Result<(), Error> {
        if self.closed.swap(true, Ordering::Relaxed) {
            return Ok(());
        }
        self.inner.close().map_err(map_turso_error)
    }
}

pub fn define_connection(ruby: &Ruby, module: &impl Module) -> Result<(), Error> {
    let class = module.define_class("RbConnection", ruby.class_object())?;
    class.define_method("prepare", method!(RbConnection::prepare, 1))?;
    class.define_method("prepare_first", method!(RbConnection::prepare_first, 1))?;
    class.define_method("changes", method!(RbConnection::changes, 0))?;
    class.define_method("last_insert_row_id", method!(RbConnection::last_insert_row_id, 0))?;
    class.define_method("in_transaction?", method!(RbConnection::in_transaction, 0))?;
    class.define_method("closed?", method!(RbConnection::is_closed, 0))?;
    class.define_method("close", method!(RbConnection::close, 0))?;
    Ok(())
}
