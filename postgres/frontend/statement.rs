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
use crate::session::DeferredExecution;
use crate::Result;

/// How a statement interacts with PostgreSQL's aborted-transaction state.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum TxRole {
    /// Rejected with 25P02 while the transaction block is aborted; an
    /// execution error inside a transaction block aborts the block.
    Other,
    /// A nested BEGIN is replaced by success in its post_run, like
    /// PostgreSQL's warning; an error that survives, like a rejected
    /// access-mode change, aborts the block like any other error.
    Begin,
    /// Ends an aborted block by rolling back; reports the ROLLBACK tag.
    Commit,
    /// Ends an aborted block by rolling back.
    Rollback,
    /// Allowed while aborted; success returns the block to normal.
    RollbackToSavepoint,
}

/// Per-session transaction state that PostgreSQL keeps and Turso's core
/// does not: the aborted-block flag, whether the current transaction has
/// run its first query, and the access mode saved by each savepoint.
#[derive(Default)]
pub struct PgTxState {
    aborted: AtomicBool,
    query_started: AtomicBool,
    /// Access mode in force when each live savepoint was created, oldest
    /// first. PostgreSQL restores the saved mode when the subtransaction
    /// ends, through RELEASE or ROLLBACK TO.
    savepoint_modes: std::sync::Mutex<Vec<(String, bool)>>,
}

impl PgTxState {
    pub(crate) fn abort(&self) {
        self.aborted.store(true, Ordering::Relaxed);
    }

    pub(crate) fn is_aborted(&self) -> bool {
        self.aborted.load(Ordering::Relaxed)
    }

    pub(crate) fn query_started(&self) -> bool {
        self.query_started.load(Ordering::Relaxed)
    }

    pub(crate) fn mark_query_started(&self) {
        self.query_started.store(true, Ordering::Relaxed);
    }

    pub(crate) fn inside_savepoint(&self) -> bool {
        !self.savepoint_modes.lock().unwrap().is_empty()
    }

    pub(crate) fn transaction_began(&self) {
        self.query_started.store(false, Ordering::Relaxed);
        self.savepoint_modes.lock().unwrap().clear();
    }

    pub(crate) fn transaction_ended(&self) {
        self.aborted.store(false, Ordering::Relaxed);
        self.transaction_began();
    }

    pub(crate) fn savepoint_created(&self, conn: &Connection, name: &str) {
        if conn.get_auto_commit() {
            return;
        }
        self.savepoint_modes
            .lock()
            .unwrap()
            .push((name.to_string(), conn.is_tx_read_only()));
    }

    /// ROLLBACK TO keeps the savepoint alive, so entries up to and
    /// including it stay.
    pub(crate) fn savepoint_rolled_back(&self, conn: &Connection, name: &str) {
        let mut modes = self.savepoint_modes.lock().unwrap();
        if let Some(i) = modes.iter().rposition(|(n, _)| n == name) {
            conn.set_tx_read_only(modes[i].1);
            modes.truncate(i + 1);
        }
    }

    pub(crate) fn savepoint_released(&self, conn: &Connection, name: &str) {
        let mut modes = self.savepoint_modes.lock().unwrap();
        if let Some(i) = modes.iter().rposition(|(n, _)| n == name) {
            conn.set_tx_read_only(modes[i].1);
            modes.truncate(i);
        }
    }
}

struct TxTracking {
    conn: Arc<Connection>,
    tx: Arc<PgTxState>,
    role: TxRole,
}

struct RollbackExecution {
    stmt: turso_core::Statement,
    outcome: Result<StepResult>,
    io_completions: Option<turso_core::types::IOCompletions>,
}

/// Called after every execution of a statement to apply connection state
/// changes that must happen at execute time, not at prepare time, and to
/// replace errors whose PostgreSQL outcome depends on execute-time
/// transaction state.
pub(crate) type PostRunFn = Arc<dyn Fn(Result<()>) -> Result<()> + Send + Sync>;

pub struct Statement {
    pub(crate) inner: turso_core::Statement,
    deferred: Option<Box<DeferredExecution>>,
    rollback: Option<Box<RollbackExecution>>,
    tx_tracking: Option<TxTracking>,
    commit_became_rollback: bool,
    /// Applied by all run/step methods after the core statement completes. Used
    /// for BEGIN/COMMIT/ROLLBACK, SAVEPOINT, and SET TRANSACTION so that
    /// connection state changes (access mode, savepoint modes) and benign
    /// error suppression ("no transaction is active", "cannot start a
    /// transaction within a transaction") happen at execute time rather
    /// than when the statement is compiled.
    post_run: Option<PostRunFn>,
    /// Whether running this statement counts as the transaction's first
    /// query, which blocks a later switch to READ WRITE. PostgreSQL takes
    /// the snapshot when the query starts, so a partially consumed or
    /// abandoned statement counts too.
    marks_query_start: bool,
}

impl Statement {
    pub(crate) fn new(inner: turso_core::Statement) -> Self {
        Self {
            inner,
            deferred: None,
            rollback: None,
            tx_tracking: None,
            commit_became_rollback: false,
            post_run: None,
            marks_query_start: false,
        }
    }

    pub(crate) fn new_with_post_run(inner: turso_core::Statement, post_run: PostRunFn) -> Self {
        Self::new(inner).with_post_run(post_run)
    }

    pub(crate) fn with_post_run(mut self, post_run: PostRunFn) -> Self {
        self.post_run = Some(post_run);
        self
    }

    pub(crate) fn with_query_start_marking(mut self, marks: bool) -> Self {
        self.marks_query_start = marks;
        self
    }

    fn mark_query_start(&self) {
        if !self.marks_query_start {
            return;
        }
        let Some(t) = &self.tx_tracking else {
            return;
        };
        if !t.conn.get_auto_commit() {
            t.tx.query_started.store(true, Ordering::Relaxed);
        }
    }

    fn apply_post_run(&self, result: Result<()>) -> Result<()> {
        match &self.post_run {
            Some(f) => f(result),
            None => result,
        }
    }

    pub(crate) fn with_deferred(inner: turso_core::Statement, exec: DeferredExecution) -> Self {
        let mut stmt = Self::new(inner);
        stmt.deferred = Some(Box::new(exec));
        stmt
    }

    pub(crate) fn without_tx_tracking(mut self) -> Self {
        self.tx_tracking = None;
        self
    }

    pub(crate) fn with_tx_tracking(
        mut self,
        conn: Arc<Connection>,
        tx: Arc<PgTxState>,
        role: TxRole,
    ) -> Self {
        self.tx_tracking = Some(TxTracking { conn, tx, role });
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
    /// through a resumable rollback. For statements that do run, capture
    /// whether the session is inside a transaction block now — an error
    /// may make the core roll the transaction back before we see it.
    /// Returns None when COMMIT or ROLLBACK must roll back the aborted block
    /// instead of executing the inner statement.
    fn tx_gate(&mut self) -> Result<Option<bool>> {
        let Some(t) = &self.tx_tracking else {
            return Ok(Some(false));
        };
        if t.tx.is_aborted() {
            match t.role {
                TxRole::Other | TxRole::Begin => {
                    return Err(in_failed_sql_transaction_error().into());
                }
                TxRole::Commit | TxRole::Rollback => {
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
                if !t.conn.get_auto_commit() {
                    t.tx.abort();
                }
            }
            // A nested BEGIN is a warning in PostgreSQL and post_run already
            // replaced it with success before this runs; a BEGIN error that
            // survives post_run, like a rejected access-mode change, aborts
            // the block like any other error.
            TxRole::Begin | TxRole::Other | TxRole::RollbackToSavepoint => {
                if was_in_tx {
                    t.tx.abort();
                }
            }
        }
    }

    fn note_execution_done(&self) {
        let Some(t) = &self.tx_tracking else {
            return;
        };
        match t.role {
            TxRole::RollbackToSavepoint => t.tx.aborted.store(false, Ordering::Relaxed),
            TxRole::Commit | TxRole::Rollback => t.tx.transaction_ended(),
            TxRole::Begin | TxRole::Other => {}
        }
    }

    pub fn step(&mut self) -> Result<StepResult> {
        self.step_inner(None)
    }

    pub fn step_with_waker(&mut self, waker: &Waker) -> Result<StepResult> {
        self.step_inner(Some(waker))
    }

    fn step_inner(&mut self, waker: Option<&Waker>) -> Result<StepResult> {
        if self.rollback.is_some() {
            return self.step_rollback(waker);
        }
        let saved_tx = self.deferred.as_ref().and_then(|exec| exec.was_in_tx);
        let tx = match saved_tx {
            Some(was_in_tx) => Some(was_in_tx),
            None => self.tx_gate()?,
        };
        let Some(was_in_tx) = tx else {
            return self.start_rollback(Ok(StepResult::Done), waker);
        };
        if saved_tx.is_none() {
            self.mark_query_start();
        }
        let result = match &mut self.deferred {
            Some(exec) => exec.step(&mut self.inner, waker),
            None => match waker {
                Some(waker) => self.inner.step_with_waker(waker),
                None => self.inner.step(),
            },
        }
        .map_err(Into::into);
        self.finish_step(result, was_in_tx, waker)
    }

    fn finish_step(
        &mut self,
        result: Result<StepResult>,
        was_in_tx: bool,
        waker: Option<&Waker>,
    ) -> Result<StepResult> {
        let result = match result {
            Ok(StepResult::Done) => self.apply_post_run(Ok(())).map(|()| StepResult::Done),
            Err(e) => self.apply_post_run(Err(e)).map(|()| StepResult::Done),
            other => other,
        };
        match &result {
            Ok(StepResult::Done) => self.note_execution_done(),
            // The core reports interruption and lock contention as results
            // rather than errors, but PostgreSQL treats query cancellation
            // and lock timeouts as ordinary errors that abort the block.
            Err(_) | Ok(StepResult::Interrupt | StepResult::Busy) => {
                if self.tx_tracking.as_ref().is_some_and(|t| {
                    matches!(t.role, TxRole::Commit | TxRole::Rollback) && !t.conn.get_auto_commit()
                }) {
                    return self.start_rollback(result, waker);
                }
                self.note_execution_error(was_in_tx);
            }
            _ => {}
        }
        result
    }

    fn start_rollback(
        &mut self,
        outcome: Result<StepResult>,
        waker: Option<&Waker>,
    ) -> Result<StepResult> {
        let t = self.tx_tracking.as_ref().unwrap();
        t.tx.abort();
        let mut stmt = match t.conn.prepare_sqlite("ROLLBACK") {
            Ok(stmt) => stmt,
            Err(error) => {
                return outcome.and(Err(error.into()));
            }
        };
        stmt.set_query_timeout_override(Some(None));
        self.rollback = Some(Box::new(RollbackExecution {
            stmt,
            outcome,
            io_completions: None,
        }));
        self.step_rollback(waker)
    }

    fn step_rollback(&mut self, waker: Option<&Waker>) -> Result<StepResult> {
        let rollback = self.rollback.as_mut().unwrap();
        let result = rollback.stmt.run_ignore_rows_nonblock();
        if let Ok(IOResult::IO(io)) = result {
            io.set_waker(waker);
            let result = if io.is_explicit_yield() {
                StepResult::Yield
            } else {
                StepResult::IO
            };
            rollback.io_completions = Some(io);
            return Ok(result);
        }
        let rollback = self.rollback.take().unwrap();
        let t = self.tx_tracking.as_ref().unwrap();
        match result {
            Ok(IOResult::Done(())) => {}
            Err(error) if matches!(*error, LimboError::NoActiveTransaction { .. }) => {}
            Err(error) => {
                t.tx.abort();
                return rollback.outcome.and(Err((*error).into()));
            }
            Ok(IOResult::IO(_)) => unreachable!(),
        }
        t.tx.transaction_ended();
        if matches!(rollback.outcome, Ok(StepResult::Done)) && t.role == TxRole::Commit {
            self.commit_became_rollback = true;
        }
        rollback.outcome
    }

    pub fn reset(&mut self) -> Result<()> {
        loop {
            match self.cleanup_nonblock()? {
                IOResult::Done(()) => break,
                IOResult::IO(io) => {
                    io.wait(self._io())?;
                }
            }
        }
        self.commit_became_rollback = false;
        self.inner.reset().map_err(Into::into)
    }

    /// Finish PostgreSQL deferred cleanup before resetting or dropping this
    /// statement. Resume after each returned completion until Done. This does
    /// not replace core statement reset, which remains synchronous.
    pub fn cleanup_nonblock(&mut self) -> Result<IOResult<()>> {
        if self.rollback.is_some() {
            match self.step_rollback(None) {
                Ok(StepResult::IO | StepResult::Yield | StepResult::Sleep { .. }) => {
                    return Ok(IOResult::IO(self.io_completions().unwrap()));
                }
                Ok(StepResult::Done) => {}
                outcome => {
                    if !self.tx_tracking.as_ref().unwrap().conn.get_auto_commit() {
                        return match outcome {
                            Err(error) => Err(error),
                            Ok(StepResult::Busy) => Err(LimboError::Busy.into()),
                            Ok(StepResult::Interrupt) => Err(LimboError::Interrupt.into()),
                            _ => unreachable!(),
                        };
                    }
                }
            }
        }
        match &mut self.deferred {
            Some(exec) => exec.cleanup_nonblock().map_err(Into::into),
            None => Ok(IOResult::Done(())),
        }
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
        self.run_with_row_callback(|_| Ok(()))
    }

    pub fn run_collect_rows(&mut self) -> Result<Vec<Vec<Value>>> {
        let mut rows = Vec::new();
        self.run_with_row_callback(|row| {
            rows.push(row.get_values().cloned().collect());
            Ok(())
        })?;
        Ok(rows)
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
        self.run_with_row_callback_nonblock(|_| Ok(()))
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
                    let completions = self.io_completions().unwrap_or_else(|| {
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

    /// Clone the completion handle while retaining it for statement cleanup.
    pub fn io_completions(&self) -> Option<turso_core::types::IOCompletions> {
        let io = if let Some(rollback) = &self.rollback {
            &rollback.io_completions
        } else if let Some(exec) = &self.deferred {
            &exec.io_completions
        } else {
            return self.inner.io_completions();
        };
        io.as_ref()
            .map(|io| turso_core::types::IOCompletions(io.0.clone()))
    }

    pub fn clear_bindings(&mut self) {
        self.inner.clear_bindings();
    }

    pub fn interrupt(&mut self) {
        if let Some(exec) = &mut self.deferred {
            exec.interrupted = true;
            return;
        }
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
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.reset())) {
            Ok(Ok(())) => {}
            Ok(Err(error)) => tracing::error!("Failed to reset PostgreSQL statement: {error}"),
            Err(_) => tracing::error!("PostgreSQL statement reset panicked"),
        }
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
        let conn = db
            .connect(ConnectOpts::new(turso_core::SessionRole::Superuser))
            .unwrap();
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
