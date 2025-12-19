use magnus::value::ReprValue;
use magnus::{block::Proc, method, Error, Module, Ruby, TryConvert, Value};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use turso_sdk_kit::rsapi::{TursoConnection, TursoStatusCode};

use crate::errors::map_turso_error;
use crate::statement::{RbResultSet, RbStatement};
use crate::value;

#[magnus::wrap(class = "Turso::Connection", free_immediately, size)]
pub struct RbConnection {
    inner: Arc<TursoConnection>,
    last_changes: AtomicU64,
    closed: AtomicBool,
}

impl RbConnection {
    pub fn new(conn: Arc<TursoConnection>) -> Self {
        Self {
            inner: conn,
            last_changes: AtomicU64::new(0),
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

    pub fn execute(&self, args: &[Value]) -> Result<RbResultSet, Error> {
        self.ensure_open()?;

        if args.is_empty() {
            let ruby = Ruby::get().expect("Ruby not initialized");
            return Err(Error::new(
                ruby.exception_arg_error(),
                "execute requires at least a SQL string",
            ));
        }

        let sql: String = String::try_convert(args[0])?;
        let mut stmt = self.inner.prepare_single(&sql).map_err(map_turso_error)?;

        for (i, arg) in args[1..].iter().enumerate() {
            value::bind_value(&mut stmt, i + 1, *arg)?;
        }

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

        let changes = self.inner.changes();
        stmt.finalize().map_err(map_turso_error)?;
        self.last_changes.store(changes, Ordering::Relaxed);

        Ok(RbResultSet::new(rows, changes))
    }

    pub fn transaction(&self, block: Proc) -> Result<Value, Error> {
        self.execute_sql("BEGIN")?;

        match block.call::<_, Value>(()) {
            Ok(result) => {
                self.execute_sql("COMMIT")?;
                Ok(result)
            }
            Err(e) => {
                let _ = self.execute_sql("ROLLBACK");
                Err(e)
            }
        }
    }

    fn execute_sql(&self, sql: &str) -> Result<(), Error> {
        let mut stmt = self.inner.prepare_single(sql).map_err(map_turso_error)?;
        loop {
            match stmt.step().map_err(map_turso_error)? {
                TursoStatusCode::Done => break,
                TursoStatusCode::Io => stmt.run_io().map_err(map_turso_error)?,
                _ => continue,
            }
        }
        stmt.finalize().map_err(map_turso_error)?;
        Ok(())
    }

    pub fn changes(&self) -> u64 {
        self.last_changes.load(Ordering::Relaxed)
    }

    pub fn last_insert_row_id(&self) -> i64 {
        self.inner.last_insert_rowid()
    }

    pub fn in_transaction(&self) -> bool {
        !self.inner.get_auto_commit()
    }

    pub fn close(&self) -> Result<(), Error> {
        if self.closed.swap(true, Ordering::Relaxed) {
            return Ok(());
        }
        self.inner.close().map_err(map_turso_error)
    }
}

pub fn define_connection(ruby: &Ruby, module: &impl Module) -> Result<(), Error> {
    let class = module.define_class("Connection", ruby.class_object())?;
    class.define_method("prepare", method!(RbConnection::prepare, 1))?;
    class.define_method("execute", method!(RbConnection::execute, -1))?;
    class.define_method("transaction", method!(RbConnection::transaction, 1))?;
    class.define_method("changes", method!(RbConnection::changes, 0))?;
    class.define_method(
        "last_insert_row_id",
        method!(RbConnection::last_insert_row_id, 0),
    )?;
    class.define_method("in_transaction?", method!(RbConnection::in_transaction, 0))?;
    class.define_method("close", method!(RbConnection::close, 0))?;
    Ok(())
}
