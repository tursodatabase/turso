use std::num::NonZero;
use std::task::Waker;
use std::time::Duration;

use turso_core::{
    ColumnTypeInfo, IOResult, LimboError, Row, StatementStatusCounter, StepResult, Value,
};

use crate::Result;

pub struct Statement {
    inner: turso_core::Statement,
}

impl Statement {
    pub(crate) fn new(inner: turso_core::Statement) -> Self {
        Self { inner }
    }

    pub fn step(&mut self) -> Result<StepResult> {
        self.inner.step().map_err(Into::into)
    }

    pub fn step_with_waker(&mut self, waker: &Waker) -> Result<StepResult> {
        self.inner.step_with_waker(waker).map_err(Into::into)
    }

    pub fn reset(&mut self) -> Result<()> {
        self.inner.reset().map_err(Into::into)
    }

    pub fn bind_at(&mut self, index: NonZero<usize>, value: Value) -> Result<()> {
        self.inner.bind_at(index, value).map_err(Into::into)
    }

    pub fn get_column_type_info(&self, index: usize) -> Result<Option<ColumnTypeInfo>> {
        self.inner.get_column_type_info(index).map_err(Into::into)
    }

    pub fn run_ignore_rows(&mut self) -> Result<()> {
        self.inner.run_ignore_rows().map_err(Into::into)
    }

    pub fn run_collect_rows(&mut self) -> Result<Vec<Vec<Value>>> {
        self.inner.run_collect_rows().map_err(Into::into)
    }

    pub fn run_with_row_callback(
        &mut self,
        mut callback: impl FnMut(&Row) -> Result<()>,
    ) -> Result<()> {
        loop {
            match self.step()? {
                StepResult::Done => return Ok(()),
                StepResult::Row => callback(self.row().expect("row should be present"))?,
                StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => {
                    self._io().step()?
                }
                StepResult::Interrupt => return Err(LimboError::Interrupt.into()),
                StepResult::Busy => return Err(LimboError::Busy.into()),
            }
        }
    }

    pub fn run_ignore_rows_nonblock(&mut self) -> Result<IOResult<()>> {
        self.inner
            .run_ignore_rows_nonblock()
            .map_err(|error| (*error).into())
    }

    pub fn run_with_row_callback_nonblock(
        &mut self,
        mut callback: impl FnMut(&Row) -> Result<()>,
    ) -> Result<IOResult<()>> {
        loop {
            match self.step()? {
                StepResult::Done => return Ok(IOResult::Done(())),
                StepResult::Row => callback(self.row().expect("row should be present"))?,
                StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => {
                    let completions = self.inner.take_io_completions().unwrap_or_else(|| {
                        turso_core::types::IOCompletions(turso_core::io::Completion::new_yield())
                    });
                    return Ok(IOResult::IO(completions));
                }
                StepResult::Interrupt => return Err(LimboError::Interrupt.into()),
                StepResult::Busy => return Err(LimboError::Busy.into()),
            }
        }
    }

    pub fn run_one_step_blocking(
        &mut self,
        mut pre_io: impl FnMut() -> Result<()>,
        mut post_io: impl FnMut() -> Result<()>,
    ) -> Result<Option<&Row>> {
        loop {
            match self.step()? {
                StepResult::Done => return Ok(None),
                StepResult::Row => return Ok(Some(self.row().expect("row should be present"))),
                StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => {
                    pre_io()?;
                    self._io().step()?;
                    post_io()?;
                }
                StepResult::Interrupt => return Err(LimboError::Interrupt.into()),
                StepResult::Busy => return Err(LimboError::Busy.into()),
            }
        }
    }

    pub fn take_io_completions(&mut self) -> Option<turso_core::types::IOCompletions> {
        self.inner.take_io_completions()
    }

    pub fn clear_bindings(&mut self) {
        self.inner.clear_bindings();
    }

    pub fn interrupt(&mut self) {
        self.inner.interrupt();
    }

    pub fn set_query_timeout_override(&mut self, timeout: Option<Option<Duration>>) {
        self.inner.set_query_timeout_override(timeout);
    }

    pub fn reset_metrics(&mut self) {
        self.inner.reset_metrics();
    }

    pub fn reset_stmt_status(&mut self, counter: StatementStatusCounter) {
        self.inner.reset_stmt_status(counter);
    }

    pub fn reset_best_effort(&mut self) {
        self.inner.reset_best_effort();
    }
}

impl std::ops::Deref for Statement {
    type Target = turso_core::Statement;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use turso_core::{ConnectOpts, Database, DatabaseOpts, MemoryIO, OpenFlags, SqliteDialect, IO};

    #[test]
    fn reset_reports_frontend_errors() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = Database::open_file_with_flags(
            io.clone(),
            ":memory:",
            OpenFlags::default(),
            DatabaseOpts::new(),
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap();
        let conn = db.connect(ConnectOpts::new()).unwrap();
        conn.set_foreign_keys_enabled(true);
        conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
            .unwrap();
        conn.execute(
            "CREATE TABLE child (pid INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)",
        )
        .unwrap();
        let mut stmt = Statement::new(
            conn.prepare("INSERT INTO child VALUES (99) RETURNING pid")
                .unwrap(),
        );
        loop {
            match stmt.step().unwrap() {
                StepResult::Row => break,
                StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => io.step().unwrap(),
                result => panic!("expected a row, got {result:?}"),
            }
        }
        let error = stmt.reset().unwrap_err();
        assert_eq!(error.sqlstate, "23503");
        assert!(matches!(
            error.core_error(),
            LimboError::ForeignKeyConstraint(_)
        ));
        assert!(std::error::Error::source(&error).is_some());
    }
}
