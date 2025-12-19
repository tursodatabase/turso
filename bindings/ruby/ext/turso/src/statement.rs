use magnus::value::ReprValue;
use magnus::{
    method, r_hash::ForEach, DataTypeFunctions, Error, IntoValue, Module, RHash, Ruby, Symbol,
    TryConvert, TypedData, Value,
};
use magnus::typed_data::Obj;
use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use turso_sdk_kit::rsapi::{TursoStatement, TursoStatusCode};

use crate::errors::{map_turso_error, statement_closed_error};
use crate::value;

/// ResultSet containing query results and execution metadata.
/// Implements `Enumerable`, so you can use `.each`, `.map`, `.select`, etc.
#[derive(TypedData)]
#[magnus(class = "Turso::ResultSet", mark, size)]
pub struct RbResultSet {
    stmt: Obj<RbStatement>,
    rows_changed: AtomicU64,
    exhausted: AtomicBool,
}

unsafe impl Send for RbResultSet {}
unsafe impl Sync for RbResultSet {}

impl RbResultSet {
    pub(crate) fn new(stmt: Obj<RbStatement>) -> Self {
        Self {
            stmt,
            rows_changed: AtomicU64::new(0),
            exhausted: AtomicBool::new(false),
        }
    }

    pub fn rows_changed(&self) -> u64 {
        self.rows_changed.load(Ordering::Relaxed)
    }

    pub fn next(ruby: &Ruby, rb_self: Obj<Self>) -> Result<Value, Error> {
        if rb_self.exhausted.load(Ordering::Relaxed) {
            return Ok(ruby.qnil().as_value());
        }
        let stmt = &*rb_self.stmt;
        if stmt.step()? {
            Ok(stmt.row()?)
        } else {
            rb_self.exhausted.store(true, Ordering::Relaxed);
            let n_change = stmt.inner.borrow().n_change();
            rb_self.rows_changed.store(n_change, Ordering::Relaxed);
            stmt.reset()?;
            Ok(ruby.qnil().as_value())
        }
    }

    pub fn each(ruby: &Ruby, rb_self: Obj<Self>) -> Result<Value, Error> {
        if !ruby.block_given() {
            return Ok(rb_self.enumeratorize("each", ()).into_value_with(ruby));
        }

        loop {
            let row = Self::next(ruby, rb_self)?;
            if row.is_nil() {
                break;
            }
            let _: Value = ruby.yield_value(row)?;
        }
        Ok(ruby.qnil().as_value())
    }
}

impl DataTypeFunctions for RbResultSet {
    fn mark(&self, marker: &magnus::gc::Marker) {
        marker.mark(self.stmt);
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

#[magnus::wrap(class = "Turso::Statement", free_immediately, size)]
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

    pub fn step(&self) -> Result<bool, Error> {
        self.ensure_open()?;
        let mut stmt = self.inner.borrow_mut();
        loop {
            match stmt.step().map_err(map_turso_error)? {
                TursoStatusCode::Row => return Ok(true),
                TursoStatusCode::Done => return Ok(false),
                TursoStatusCode::Io => {
                    stmt.run_io().map_err(map_turso_error)?;
                }
                _ => return Ok(false),
            }
        }
    }

    pub fn execute(ruby: &Ruby, rb_self: Obj<Self>, args: &[Value]) -> Result<Obj<RbResultSet>, Error> {
        rb_self.ensure_open()?;
        Self::bind(ruby, rb_self, args)?;
        let rb_rs = ruby.obj_wrap(RbResultSet::new(rb_self));


        if rb_self.inner.borrow().column_count() == 0 {
            RbResultSet::next(ruby, rb_rs)?;
        }

        Ok(rb_rs)
    }

    pub fn columns(&self) -> Result<magnus::RArray, Error> {
        self.ensure_open()?;
        let ruby = Ruby::get().expect("Ruby not initialized");
        let stmt = self.inner.borrow();
        let cols = ruby.ary_new();
        let count = stmt.column_count();

        for i in 0..count {
            let name = stmt.column_name(i).map_err(map_turso_error)?;
            cols.push(ruby.sym_new(name.as_ref()))?;
        }

        Ok(cols)
    }

    pub fn row(&self) -> Result<Value, Error> {
        self.ensure_open()?;
        let stmt = self.inner.borrow();
        value::extract_row(&stmt).map(|h| h.as_value())
    }

    pub fn reset(&self) -> Result<(), Error> {
        self.ensure_open()?;
        self.inner.borrow_mut().reset().map_err(map_turso_error)
    }

    pub fn finalize(&self) -> Result<(), Error> {
        if self.finalized.swap(true, Ordering::Relaxed) {
            return Ok(());
        }
        self.inner
            .borrow_mut()
            .finalize()
            .map_err(map_turso_error)?;
        Ok(())
    }
}

impl Drop for RbStatement {
    fn drop(&mut self) {
        if !self.finalized.load(Ordering::Relaxed) {
            let _ = self.inner.borrow_mut().finalize();
        }
    }
}

pub fn define_result_set(ruby: &Ruby, module: &impl Module) -> Result<(), Error> {
    let class = module.define_class("ResultSet", ruby.class_object())?;
    class.include_module(ruby.module_enumerable())?;
    class.define_method("rows_changed", method!(RbResultSet::rows_changed, 0))?;
    class.define_method("next", method!(RbResultSet::next, 0))?;
    class.define_method("each", method!(RbResultSet::each, 0))?;
    Ok(())
}


pub fn define_statement(ruby: &Ruby, module: &impl Module) -> Result<(), Error> {
    define_result_set(ruby, module)?;

    let class = module.define_class("Statement", ruby.class_object())?;
    class.define_method("bind", method!(RbStatement::bind, -1))?;
    class.define_method("step", method!(RbStatement::step, 0))?;
    class.define_method("execute", method!(RbStatement::execute, -1))?;
    class.define_method("row", method!(RbStatement::row, 0))?;
    class.define_method("columns", method!(RbStatement::columns, 0))?;
    class.define_method("reset!", method!(RbStatement::reset, 0))?;
    class.define_method("finalize!", method!(RbStatement::finalize, 0))?;
    Ok(())
}
