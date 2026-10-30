use std::num::NonZero;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::task::Waker;
use std::time::Duration;

use turso_core::{
    ColumnTypeInfo, Connection, IOResult, LimboError, Row, StatementStatusCounter, StepResult,
    Value,
};

use crate::error::in_failed_sql_transaction_error;
use crate::Result;

pub(crate) type DeferredExec =
    Box<dyn FnMut(&mut turso_core::Statement) -> turso_core::Result<()> + Send + Sync>;

struct DeferredState {
    exec: DeferredExec,
    completed: bool,
}

/// How a statement interacts with PostgreSQL's aborted-transaction state.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum TxRole {
    /// Rejected with 25P02 while the transaction block is aborted; an
    /// execution error inside a transaction block aborts the block.
    Other,
    /// BEGIN; errors never abort the surrounding block.
    Begin,
    /// Ends an aborted block by rolling back; reports the ROLLBACK tag.
    Commit,
    /// Ends an aborted block by rolling back.
    Rollback,
    /// Allowed while aborted; success returns the block to normal.
    RollbackToSavepoint,
}

struct TxTracking {
    conn: Arc<Connection>,
    tx_aborted: Arc<AtomicBool>,
    role: TxRole,
}

pub struct Statement {
    inner: turso_core::Statement,
    deferred: Option<DeferredState>,
    tx_tracking: Option<TxTracking>,
    commit_became_rollback: bool,
}

impl Statement {
    pub(crate) fn new(inner: turso_core::Statement) -> Self {
        Self {
            inner,
            deferred: None,
            tx_tracking: None,
            commit_became_rollback: false,
        }
    }

    pub(crate) fn with_deferred(inner: turso_core::Statement, exec: DeferredExec) -> Self {
        Self {
            inner,
            deferred: Some(DeferredState {
                exec,
                completed: false,
            }),
            tx_tracking: None,
            commit_became_rollback: false,
        }
    }

    pub(crate) fn with_tx_tracking(
        mut self,
        conn: Arc<Connection>,
        tx_aborted: Arc<AtomicBool>,
        role: TxRole,
    ) -> Self {
        self.tx_tracking = Some(TxTracking {
            conn,
            tx_aborted,
            role,
        });
        self
    }

    /// The command tag the wire server should report instead of deriving it
    /// from the SQL text: a COMMIT that ended an aborted transaction block
    /// performed a rollback, and PostgreSQL reports it as ROLLBACK.
    pub fn command_tag_override(&self) -> Option<&'static str> {
        self.commit_became_rollback.then_some("ROLLBACK")
    }

    /// Enforce PostgreSQL's aborted-transaction rules before running the
    /// statement. While the block is aborted, only COMMIT, ROLLBACK, and
    /// ROLLBACK TO SAVEPOINT may run; COMMIT and ROLLBACK end the block
    /// right here by rolling back. For statements that do run, capture
    /// whether the session is inside a transaction block now — an error
    /// may make the core roll the transaction back before we see it.
    /// Returns None when COMMIT or ROLLBACK already ended the aborted block,
    /// so the inner statement must be skipped and reported as complete.
    fn tx_gate(&mut self) -> Result<Option<bool>> {
        let Some(t) = &self.tx_tracking else {
            return Ok(Some(false));
        };
        if t.tx_aborted.load(Ordering::Relaxed) {
            match t.role {
                TxRole::Other | TxRole::Begin => {
                    return Err(in_failed_sql_transaction_error().into());
                }
                TxRole::Commit | TxRole::Rollback => {
                    // A rollback failure (an unfinished statement on this
                    // connection blocks it) leaves the block aborted and
                    // fails this COMMIT/ROLLBACK: reporting success while
                    // the transaction is still open would let a later
                    // COMMIT persist the aborted block's writes.
                    rollback_aborted_block(&t.conn)?;
                    t.tx_aborted.store(false, Ordering::Relaxed);
                    if t.role == TxRole::Commit {
                        self.commit_became_rollback = true;
                    }
                    return Ok(None);
                }
                TxRole::RollbackToSavepoint => {}
            }
        }
        Ok(Some(!t.conn.get_auto_commit()))
    }

    fn note_execution_error(&self, was_in_tx: bool) {
        let Some(t) = &self.tx_tracking else {
            return;
        };
        match t.role {
            // PostgreSQL never enters the aborted state from a failed COMMIT
            // or ROLLBACK: the transaction is rolled back and the session
            // returns to idle. When the rollback itself fails, the
            // transaction is still open, and aborted is the only honest
            // state to report.
            TxRole::Commit | TxRole::Rollback => {
                if !t.conn.get_auto_commit() && rollback_aborted_block(&t.conn).is_err() {
                    t.tx_aborted.store(true, Ordering::Relaxed);
                }
            }
            TxRole::Begin => {}
            TxRole::Other | TxRole::RollbackToSavepoint => {
                if was_in_tx {
                    t.tx_aborted.store(true, Ordering::Relaxed);
                }
            }
        }
    }

    fn note_execution_done(&self) {
        let Some(t) = &self.tx_tracking else {
            return;
        };
        if t.role == TxRole::RollbackToSavepoint {
            t.tx_aborted.store(false, Ordering::Relaxed);
        }
    }

    fn execute_deferred(&mut self, was_in_tx: bool) -> Result<()> {
        let Some(deferred) = self.deferred.as_mut() else {
            return Ok(());
        };
        if !deferred.completed {
            let result: Result<()> = (deferred.exec)(&mut self.inner).map_err(Into::into);
            if result.is_err() {
                self.note_execution_error(was_in_tx);
                return result;
            }
            deferred.completed = true;
        }
        Ok(())
    }

    pub fn step(&mut self) -> Result<StepResult> {
        let Some(was_in_tx) = self.tx_gate()? else {
            return Ok(StepResult::Done);
        };
        if self.deferred.is_some() {
            self.execute_deferred(was_in_tx)?;
            return Ok(StepResult::Done);
        }
        let result = self.inner.step().map_err(Into::into);
        self.note_step_outcome(&result, was_in_tx);
        result
    }

    pub fn step_with_waker(&mut self, waker: &Waker) -> Result<StepResult> {
        let Some(was_in_tx) = self.tx_gate()? else {
            return Ok(StepResult::Done);
        };
        if self.deferred.is_some() {
            self.execute_deferred(was_in_tx)?;
            return Ok(StepResult::Done);
        }
        let result = self.inner.step_with_waker(waker).map_err(Into::into);
        self.note_step_outcome(&result, was_in_tx);
        result
    }

    fn note_step_outcome(&self, result: &Result<StepResult>, was_in_tx: bool) {
        match result {
            Ok(StepResult::Done) => self.note_execution_done(),
            // The core reports interruption and lock contention as results
            // rather than errors, but PostgreSQL treats query cancellation
            // and lock timeouts as ordinary errors that abort the block.
            Ok(StepResult::Interrupt | StepResult::Busy) => self.note_execution_error(was_in_tx),
            Err(_) => self.note_execution_error(was_in_tx),
            _ => {}
        }
    }

    pub fn reset(&mut self) -> Result<()> {
        if let Some(deferred) = self.deferred.as_mut() {
            deferred.completed = false;
        }
        self.commit_became_rollback = false;
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
        let Some(was_in_tx) = self.tx_gate()? else {
            return Ok(());
        };
        if self.deferred.is_some() {
            return self.execute_deferred(was_in_tx);
        }
        let result = self.inner.run_ignore_rows().map_err(Into::into);
        match &result {
            Ok(()) => self.note_execution_done(),
            Err(_) => self.note_execution_error(was_in_tx),
        }
        result
    }

    pub fn run_collect_rows(&mut self) -> Result<Vec<Vec<Value>>> {
        let Some(was_in_tx) = self.tx_gate()? else {
            return Ok(Vec::new());
        };
        if self.deferred.is_some() {
            self.execute_deferred(was_in_tx)?;
            return Ok(Vec::new());
        }
        let result = self.inner.run_collect_rows().map_err(Into::into);
        match &result {
            Ok(_) => self.note_execution_done(),
            Err(_) => self.note_execution_error(was_in_tx),
        }
        result
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
        let Some(was_in_tx) = self.tx_gate()? else {
            return Ok(IOResult::Done(()));
        };
        if self.deferred.is_some() {
            self.execute_deferred(was_in_tx)?;
            return Ok(IOResult::Done(()));
        }
        let result = self
            .inner
            .run_ignore_rows_nonblock()
            .map_err(|error| (*error).into());
        match &result {
            Ok(IOResult::Done(())) => self.note_execution_done(),
            Err(_) => self.note_execution_error(was_in_tx),
            _ => {}
        }
        result
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
        if let Some(deferred) = self.deferred.as_mut() {
            deferred.completed = false;
        }
        self.inner.reset_best_effort();
    }
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

/// Roll back the connection's transaction so a block can end. A transaction
/// that is already gone — some errors make the core roll it back on its own,
/// which ROLLBACK reports as a transaction error — counts as ended. Every
/// other failure means the transaction is still open and must reach the
/// caller.
fn rollback_aborted_block(conn: &Arc<Connection>) -> Result<()> {
    let run = conn
        .prepare_sqlite("ROLLBACK")
        .and_then(|mut rollback| rollback.run_ignore_rows());
    match run {
        Ok(()) | Err(LimboError::TxError(_)) => Ok(()),
        Err(err) => Err(err.into()),
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
