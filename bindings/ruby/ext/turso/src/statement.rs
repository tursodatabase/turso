use magnus::prelude::*;
use magnus::{method, Error, Module, RArray, Ruby, Value};
use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, Ordering};
use turso_sdk_kit::rsapi::{TursoStatement, TursoStatusCode};

use crate::errors::{map_turso_error, statement_closed_error};
use crate::value;

#[magnus::wrap(class = "Turso::ExecutionResult", free_immediately, size)]
pub struct RbExecutionResult {
    rows_changed: u64,
}

impl RbExecutionResult {
    pub(crate) fn from_rsapi(rows_changed: u64) -> Self {
        Self { rows_changed }
    }

    pub fn rows_changed(&self) -> u64 {
        self.rows_changed
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

    pub fn execute(&self, args: &[Value]) -> Result<RbExecutionResult, Error> {
        self.ensure_open()?;
        self.bind(args)?;

        let result = self.inner.borrow_mut().execute().map_err(map_turso_error)?;
        Ok(RbExecutionResult {
            rows_changed: result.rows_changed,
        })
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

pub fn define_statement(ruby: &Ruby, module: &impl Module) -> Result<(), Error> {
    let result_class = module.define_class("ExecutionResult", ruby.class_object())?;
    result_class.define_method("rows_changed", method!(RbExecutionResult::rows_changed, 0))?;

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
