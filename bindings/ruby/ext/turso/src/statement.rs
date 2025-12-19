use magnus::value::ReprValue;
use magnus::{method, DataTypeFunctions, Error, Module, Ruby, TryConvert, TypedData, Value};
use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, Ordering};
use turso_sdk_kit::rsapi::{TursoStatement, TursoStatusCode};

use crate::errors::{map_turso_error, statement_closed_error};
use crate::value;

/// ResultSet containing query results and execution metadata.
/// Implements `Enumerable`, so you can use `.each`, `.map`, `.select`, etc.
#[derive(TypedData)]
#[magnus(class = "Turso::ResultSet", mark, size)]
pub struct RbResultSet {
    rows: Vec<Value>,
    rows_changed: u64,
}


unsafe impl Send for RbResultSet {}
unsafe impl Sync for RbResultSet {}

impl RbResultSet {
    pub(crate) fn new(rows: Vec<Value>, rows_changed: u64) -> Self {
        Self { rows, rows_changed }
    }

    pub fn rows_changed(&self) -> u64 {
        self.rows_changed
    }

    pub fn each(
        ruby: &Ruby,
        rb_self: Value,
    ) -> Result<magnus::block::Yield<impl Iterator<Item = Value>>, Error> {
        let this: &Self = TryConvert::try_convert(rb_self)?;
        if ruby.block_given() {
            Ok(magnus::block::Yield::Iter(this.rows.iter().copied()))
        } else {
            Ok(magnus::block::Yield::Enumerator(
                rb_self.enumeratorize("each", ()),
            ))
        }
    }
}

impl DataTypeFunctions for RbResultSet {
    fn mark(&self, marker: &magnus::gc::Marker) {
        for row in &self.rows {
            marker.mark(*row);
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>() + self.rows.len() * std::mem::size_of::<Value>()
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

    pub fn bind(&self, args: &[Value]) -> Result<(), Error> {
        self.ensure_open()?;
        let mut stmt = self.inner.borrow_mut();
        for (i, arg) in args.iter().enumerate() {
            value::bind_value(&mut stmt, i + 1, *arg)?;
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

    pub fn execute(&self, args: &[Value]) -> Result<RbResultSet, Error> {
        self.ensure_open()?;
        self.bind(args)?;

        let mut stmt = self.inner.borrow_mut();
        let mut rows: Vec<Value> = Vec::new();
        loop {
            match stmt.step().map_err(map_turso_error)? {
                TursoStatusCode::Row => {
                    rows.push(value::extract_row(&stmt)?.as_value());
                }
                TursoStatusCode::Done => break,
                TursoStatusCode::Io => {
                    stmt.run_io().map_err(map_turso_error)?;
                }
                _ => break,
            }
        }
        let changes = stmt.n_change();
        stmt.reset().map_err(map_turso_error)?;

        Ok(RbResultSet::new(rows, changes))
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
