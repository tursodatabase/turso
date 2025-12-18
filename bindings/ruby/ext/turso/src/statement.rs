use magnus::{method, Error, Module, Object, RArray, Ruby, Value};
use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, Ordering};
use turso_sdk_kit::rsapi::{TursoStatement, TursoStatusCode};

use crate::errors::{map_turso_error, statement_closed_error};
use crate::value;

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

    pub fn execute(&self, args: &[Value]) -> Result<RArray, Error> {
        self.ensure_open()?;
        self.bind(args)?;

        let ruby = Ruby::get().expect("Ruby not initialized");
        let results = ruby.ary_new();
        while self.step()? {
            let stmt = self.inner.borrow();
            let row = value::extract_row(&stmt)?;
            results.push(row)?;
        }

        self.inner.borrow_mut().reset().map_err(map_turso_error)?;
        Ok(results)
    }

    pub fn columns(&self) -> Result<RArray, Error> {
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

    pub fn reset(&self) -> Result<(), Error> {
        self.ensure_open()?;
        self.inner.borrow_mut().reset().map_err(map_turso_error)
    }

    pub fn finalize(&self) -> Result<(), Error> {
        if self.finalized.swap(true, Ordering::Relaxed) {
            return Ok(());
        }
        self.inner.borrow_mut().finalize().map_err(map_turso_error)?;
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

pub fn define_statement(ruby: &Ruby, module: &impl Module) -> Result<(), Error> {
    let class = module.define_class("Statement", ruby.class_object())?;
    class.define_method("bind", method!(RbStatement::bind, -1))?;
    class.define_method("step", method!(RbStatement::step, 0))?;
    class.define_method("execute", method!(RbStatement::execute, -1))?;
    class.define_method("columns", method!(RbStatement::columns, 0))?;
    class.define_method("reset!", method!(RbStatement::reset, 0))?;
    class.define_method("finalize!", method!(RbStatement::finalize, 0))?;
    Ok(())
}
