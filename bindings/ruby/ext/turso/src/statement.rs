use magnus::value::ReprValue;
use magnus::{
    method, r_hash::ForEach, Error, Module, RHash, Ruby, Symbol,
    TryConvert, Value,
};
use magnus::typed_data::Obj;
use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, Ordering};
use turso_sdk_kit::rsapi::{TursoStatement, TursoStatusCode};

use crate::errors::{map_turso_error, statement_closed_error};
use crate::value;

#[magnus::wrap(class = "Turso::RbStatement", free_immediately, size)]
pub struct RbStatement {
    inner: RefCell<Box<TursoStatement>>,
    finalized: AtomicBool,
}

impl RbStatement {
    pub fn new(stmt: Box<TursoStatement>) -> Self {
        Self {
            inner: RefCell::new(stmt),
            finalized: AtomicBool::new(false),
        }
    }

    fn ensure_open(&self) -> Result<(), Error> {
        if self.finalized.load(Ordering::Relaxed) {
            Err(statement_closed_error())
        } else {
            Ok(())
        }
    }

    pub fn bind(ruby: &Ruby, rb_self: Obj<Self>, args: &[Value]) -> Result<(), Error> {
        rb_self.ensure_open()?;
        let mut stmt = rb_self.inner.borrow_mut();
        
        stmt.reset().map_err(map_turso_error)?;

        if args.len() == 1 && args[0].is_kind_of(ruby.class_hash()) {
            let hash = RHash::try_convert(args[0])?;
            hash.foreach(|key: Value, val: Value| {
                let key_str = if key.is_kind_of(ruby.class_symbol()) {
                    Symbol::try_convert(key)?.name()?.to_string()
                } else {
                    String::try_convert(key)?
                };

                if let Ok(pos) = stmt.named_position(&key_str) {
                    value::bind_value(&mut stmt, pos, val)?;
                }
                Ok(ForEach::Continue)
            })?;
        } else {
            let param_count = stmt.parameters_count();
            for (i, arg) in args.iter().enumerate() {
                if i >= param_count {
                    break;
                }
                value::bind_value(&mut stmt, i + 1, *arg)?;
            }
        }
        Ok(())
    }


    pub fn step(ruby: &Ruby, rb_self: Obj<Self>) -> Result<Value, Error> {
        rb_self.ensure_open()?;
        let mut stmt = rb_self.inner.borrow_mut();
        loop {
            match stmt.step(None).map_err(map_turso_error)? {
                TursoStatusCode::Row => return Ok(ruby.sym_new("row").as_value()),
                TursoStatusCode::Done => return Ok(ruby.sym_new("done").as_value()),
                TursoStatusCode::Io => {
                    stmt.run_io().map_err(map_turso_error)?;
                }
                _ => return Ok(ruby.sym_new("done").as_value()),
            }
        }
    }

    pub fn columns(&self) -> Result<magnus::RArray, Error> {
        self.ensure_open()?;
        let ruby = Ruby::get().expect("Ruby not initialized");
        let stmt = self.inner.borrow();
        let cols = ruby.ary_new();
        let count = stmt.column_count();

        for i in 0..count {
            let name = stmt.column_name(i).map_err(map_turso_error)?;
            cols.push(name.as_ref())?;
        }

        Ok(cols)
    }

    pub fn row(&self) -> Result<magnus::RArray, Error> {
        self.ensure_open()?;
        let stmt = self.inner.borrow();
        value::extract_row_array(&stmt)
    }

    pub fn reset(&self) -> Result<(), Error> {
        self.ensure_open()?;
        self.inner.borrow_mut().reset().map_err(map_turso_error)
    }

    pub fn finalize(&self) -> Result<(), Error> {
        if self.finalized.swap(true, Ordering::Relaxed) {
            return Ok(());
        }
        self.inner.borrow_mut().finalize(None).map_err(map_turso_error)?;
        Ok(())
    }

    pub fn is_closed(&self) -> bool {
        self.finalized.load(Ordering::Relaxed)
    }

    /// Get number of rows changed by last execution of this statement.
    pub fn rows_changed(&self) -> u64 {
        self.inner.borrow().n_change()
    }
}

impl Drop for RbStatement {
    fn drop(&mut self) {
        if !self.finalized.load(Ordering::Relaxed) {
            let _ = self.inner.borrow_mut().finalize(None);
        }
    }
}

pub fn define_statement(ruby: &Ruby, module: &impl Module) -> Result<(), Error> {
    let class = module.define_class("RbStatement", ruby.class_object())?;
    class.define_method("bind", method!(RbStatement::bind, -1))?;
    class.define_method("step", method!(RbStatement::step, 0))?;
    class.define_method("columns", method!(RbStatement::columns, 0))?;
    class.define_method("row", method!(RbStatement::row, 0))?;
    class.define_method("reset!", method!(RbStatement::reset, 0))?;
    class.define_method("finalize!", method!(RbStatement::finalize, 0))?;
    class.define_method("closed?", method!(RbStatement::is_closed, 0))?;
    class.define_method("rows_changed", method!(RbStatement::rows_changed, 0))?;
    Ok(())
}
