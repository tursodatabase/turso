use std::future::{poll_fn, Future};
use std::num::NonZero;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::Waker;
use std::task::{Context, Poll};
use std::time::Duration;

use turso_core::{
    ColumnTypeInfo, IOResult, LimboError, Row, StatementStatusCounter, StepResult, Value,
};

use crate::Result;

pub(crate) type DeferredIO =
    Arc<Mutex<Option<(StepResult, Option<turso_core::types::IOCompletions>)>>>;
pub(crate) type DeferredFuture =
    Pin<Box<dyn Future<Output = turso_core::Result<DeferredResult>> + Send>>;
pub(crate) type DeferredExec = Box<dyn Fn(DeferredIO) -> DeferredFuture + Send + Sync>;

pub(crate) enum DeferredResult {
    Done,
    Replace(turso_core::Statement),
    Changed(i64),
}

struct DeferredState {
    exec: DeferredExec,
    future: Option<DeferredFuture>,
    io: DeferredIO,
    completed: bool,
}

pub struct Statement {
    inner: turso_core::Statement,
    deferred: Option<DeferredState>,
}

impl Statement {
    pub(crate) fn new(inner: turso_core::Statement) -> Self {
        Self {
            inner,
            deferred: None,
        }
    }

    pub(crate) fn with_deferred(inner: turso_core::Statement, exec: DeferredExec) -> Self {
        let io = DeferredIO::default();
        let future = Some(exec(io.clone()));
        Self {
            inner,
            deferred: Some(DeferredState {
                exec,
                future,
                io,
                completed: false,
            }),
        }
    }

    fn execute_deferred(&mut self, waker: &Waker) -> Result<StepResult> {
        let Some(deferred) = self.deferred.as_mut() else {
            return Ok(StepResult::Done);
        };
        if deferred.completed {
            return Ok(StepResult::Done);
        }
        let mut cx = Context::from_waker(waker);
        match deferred
            .future
            .as_mut()
            .expect("deferred future exists")
            .as_mut()
            .poll(&mut cx)
        {
            Poll::Pending => Ok(deferred
                .io
                .lock()
                .unwrap()
                .as_mut()
                .map(|(result, _)| std::mem::replace(result, StepResult::IO))
                .unwrap_or(StepResult::IO)),
            Poll::Ready(Ok(result)) => {
                deferred.future = None;
                deferred.completed = true;
                match result {
                    DeferredResult::Done => {}
                    DeferredResult::Replace(inner) => self.inner = inner,
                    DeferredResult::Changed(count) => self.inner.set_n_change(count),
                }
                Ok(StepResult::Done)
            }
            Poll::Ready(Err(error)) => {
                deferred.restart();
                Err(error.into())
            }
        }
    }

    pub fn step(&mut self) -> Result<StepResult> {
        if self.deferred.is_some() {
            return self.execute_deferred(Waker::noop());
        }
        self.inner.step().map_err(Into::into)
    }

    pub fn step_with_waker(&mut self, waker: &Waker) -> Result<StepResult> {
        if self.deferred.is_some() {
            return self.execute_deferred(waker);
        }
        self.inner.step_with_waker(waker).map_err(Into::into)
    }

    pub fn reset(&mut self) -> Result<()> {
        if let Some(deferred) = self.deferred.as_mut() {
            deferred.restart();
        }
        self.inner.reset().map_err(Into::into)
    }

    pub fn bind_at(&mut self, index: NonZero<usize>, value: Value) -> Result<()> {
        self.inner.bind_at(index, value).map_err(Into::into)
    }

    pub fn get_column_type_info(&self, index: usize) -> Result<Option<ColumnTypeInfo>> {
        let mut info = self.inner.get_column_type_info(index)?;
        if self.inner.get_query_mode() == turso_core::QueryMode::Normal {
            let program = self.inner.get_program();
            if let (Some(info), Some(column)) = (&mut info, program.result_columns.get(index)) {
                if matches!(
                    boolean_result_type(&column.expr, program),
                    BooleanResultType::Boolean
                ) {
                    info.declared_name = "BOOLEAN".to_string();
                    info.base_type = None;
                }
            }
        }
        Ok(info)
    }

    pub fn run_ignore_rows(&mut self) -> Result<()> {
        if self.deferred.is_none() {
            return self.inner.run_ignore_rows().map_err(Into::into);
        }
        loop {
            match self.step()? {
                StepResult::Done => return Ok(()),
                StepResult::Row => {}
                StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => {
                    self._io().step()?
                }
                StepResult::Interrupt => return Err(LimboError::Interrupt.into()),
                StepResult::Busy => return Err(LimboError::Busy.into()),
            }
        }
    }

    pub fn run_collect_rows(&mut self) -> Result<Vec<Vec<Value>>> {
        if self.deferred.is_some() {
            self.run_ignore_rows()?;
            return Ok(Vec::new());
        }
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
        if self.deferred.is_none() {
            return self
                .inner
                .run_ignore_rows_nonblock()
                .map_err(|error| (*error).into());
        }
        loop {
            match self.step()? {
                StepResult::Done => return Ok(IOResult::Done(())),
                StepResult::Row => {}
                StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => {
                    return Ok(
                        IOResult::IO(
                            self.take_io_completions().unwrap_or_else(|| {
                                turso_core::types::IOCompletions(
                                    turso_core::io::Completion::new_yield(),
                                )
                            }),
                        ),
                    );
                }
                StepResult::Interrupt => return Err(LimboError::Interrupt.into()),
                StepResult::Busy => return Err(LimboError::Busy.into()),
            }
        }
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
                    let completions = self.take_io_completions().unwrap_or_else(|| {
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
        self.deferred
            .as_ref()
            .and_then(|state| {
                state
                    .io
                    .lock()
                    .unwrap()
                    .as_mut()
                    .and_then(|(_, completions)| completions.take())
            })
            .or_else(|| self.inner.take_io_completions())
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
        if let Some(deferred) = self.deferred.as_mut() {
            deferred.restart();
        }
        self.inner.reset_best_effort();
    }
}

impl DeferredState {
    fn restart(&mut self) {
        self.completed = false;
        *self.io.lock().unwrap() = None;
        self.future = Some((self.exec)(self.io.clone()));
    }
}

pub(crate) async fn run_core(
    stmt: &mut turso_core::Statement,
    io: &DeferredIO,
) -> turso_core::Result<()> {
    run_core_with_rows(stmt, io, |_| Ok(())).await
}

pub(crate) async fn collect_core(
    stmt: &mut turso_core::Statement,
    io: &DeferredIO,
) -> turso_core::Result<Vec<Vec<Value>>> {
    let mut rows = Vec::new();
    run_core_with_rows(stmt, io, |row| {
        rows.push(row.get_values().cloned().collect());
        Ok(())
    })
    .await?;
    Ok(rows)
}

async fn run_core_with_rows(
    stmt: &mut turso_core::Statement,
    io: &DeferredIO,
    mut on_row: impl FnMut(&Row) -> turso_core::Result<()>,
) -> turso_core::Result<()> {
    poll_fn(|cx| loop {
        match stmt.step_with_waker(cx.waker())? {
            StepResult::Done => return Poll::Ready(Ok(())),
            StepResult::Row => on_row(stmt.row().expect("row should be present"))?,
            result @ (StepResult::IO | StepResult::Yield | StepResult::Sleep { .. }) => {
                *io.lock().unwrap() = Some((result, stmt.take_io_completions()));
                return Poll::Pending;
            }
            StepResult::Interrupt => return Poll::Ready(Err(LimboError::Interrupt)),
            StepResult::Busy => return Poll::Ready(Err(LimboError::Busy)),
        }
    })
    .await
}

pub(crate) async fn run_pg(stmt: &mut Statement, io: &DeferredIO) -> Result<()> {
    poll_fn(|cx| loop {
        match stmt.step_with_waker(cx.waker())? {
            StepResult::Done => return Poll::Ready(Ok(())),
            StepResult::Row => {}
            result @ (StepResult::IO | StepResult::Yield | StepResult::Sleep { .. }) => {
                *io.lock().unwrap() = Some((result, stmt.take_io_completions()));
                return Poll::Pending;
            }
            StepResult::Interrupt => return Poll::Ready(Err(LimboError::Interrupt.into())),
            StepResult::Busy => return Poll::Ready(Err(LimboError::Busy.into())),
        }
    })
    .await
}

enum BooleanResultType {
    Boolean,
    Null,
    Other,
}

// TODO: With better binding and resolved expression support, read the result type
// from the resolved expression instead. Inferring it from expression shapes here
// is fragile: new expressions and rewrites can lose the boolean type.
fn boolean_result_type(
    expr: &turso_parser::ast::Expr,
    program: &turso_core::Program,
) -> BooleanResultType {
    use turso_parser::ast::{Expr, Literal};

    match expr {
        Expr::Literal(Literal::Null) => BooleanResultType::Null,
        Expr::Cast {
            type_name: Some(ty),
            ..
        } if ty.array_dimensions == 0 && is_boolean_type(&ty.name) => BooleanResultType::Boolean,
        Expr::Column { table, column, .. } => {
            let column = program
                .table_references
                .find_table_by_internal_id(*table)
                .and_then(|(_, table)| table.get_column_at(*column));
            match column {
                Some(column)
                    if column.array_dimensions() == 0 && is_boolean_type(&column.ty_str) =>
                {
                    BooleanResultType::Boolean
                }
                _ => BooleanResultType::Other,
            }
        }
        Expr::Parenthesized(exprs) if exprs.len() == 1 => boolean_result_type(&exprs[0], program),
        Expr::Collate(expr, _) => boolean_result_type(expr, program),
        Expr::Case {
            when_then_pairs,
            else_expr,
            ..
        } => {
            let mut result = BooleanResultType::Null;
            for expr in when_then_pairs
                .iter()
                .map(|(_, then)| then.as_ref())
                .chain(else_expr.as_deref())
            {
                match boolean_result_type(expr, program) {
                    BooleanResultType::Boolean => result = BooleanResultType::Boolean,
                    BooleanResultType::Null => {}
                    BooleanResultType::Other => return BooleanResultType::Other,
                }
            }
            result
        }
        _ => BooleanResultType::Other,
    }
}

fn is_boolean_type(name: &str) -> bool {
    name.eq_ignore_ascii_case("boolean") || name.eq_ignore_ascii_case("bool")
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
