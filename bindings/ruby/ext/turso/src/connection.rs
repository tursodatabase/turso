use magnus::{block::Proc, method, Error, Module, Object, RArray, Ruby, TryConvert, Value};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use turso_sdk_kit::rsapi::{TursoConnection, TursoStatusCode};

use crate::errors::map_turso_error;
use crate::statement::RbStatement;
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
            Err(Error::new(
                magnus::exception::runtime_error(),
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

    pub fn execute(&self, args: &[Value]) -> Result<RArray, Error> {
        self.ensure_open()?;

        if args.is_empty() {
            return Err(Error::new(
                magnus::exception::arg_error(),
                "execute requires at least a SQL string",
            ));
        }

        let ruby = Ruby::get().expect("Ruby not initialized");
        let sql: String = String::try_convert(args[0])?;
        let mut stmt = self.inner.prepare_single(&sql).map_err(map_turso_error)?;

        for (i, arg) in args[1..].iter().enumerate() {
            value::bind_value(&mut stmt, i + 1, *arg)?;
        }

        let results = ruby.ary_new();

        loop {
            match stmt.step().map_err(map_turso_error)? {
                TursoStatusCode::Row => {
                    let row = value::extract_row(&stmt)?;
                    results.push(row)?;
                }
                TursoStatusCode::Done => break,
                TursoStatusCode::Io => {
                    stmt.run_io().map_err(map_turso_error)?;
                }
                _ => break,
            }
        }

        stmt.finalize().map_err(map_turso_error)?;

        // Note: sdk-kit's TursoStatement doesn't expose n_change() after stepping.
        // For proper DML change count, sdk-kit would need to expose this.
        // Using 0 for now - users can check last_insert_row_id for INSERT confirmation.
        self.last_changes.store(0, Ordering::Relaxed);

        Ok(results)
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
    class.define_method("last_insert_row_id", method!(RbConnection::last_insert_row_id, 0))?;
    class.define_method("in_transaction?", method!(RbConnection::in_transaction, 0))?;
    class.define_method("close", method!(RbConnection::close, 0))?;
    Ok(())
}
