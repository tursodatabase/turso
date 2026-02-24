use crate::turso_assert_eq;
use std::{
    borrow::Cow,
    num::NonZero,
    ops::Deref,
    sync::{atomic::Ordering, Arc},
    task::Waker,
};

use tracing::{instrument, Level};
use turso_parser::{
    ast::{fmt::ToTokens, Cmd},
    parser::Parser,
};

use crate::{
    busy::BusyHandlerState,
    parameters,
    schema::Trigger,
    stats::refresh_analyze_stats,
    translate::{self, display::PlanContext, emitter::TransactionMode},
    vdbe::{
        self,
        explain::{EXPLAIN_COLUMNS_TYPE, EXPLAIN_QUERY_PLAN_COLUMNS_TYPE},
    },
    LimboError, MvStore, Pager, QueryMode, Result, Value, EXPLAIN_COLUMNS,
    EXPLAIN_QUERY_PLAN_COLUMNS,
};

type ProgramExecutionState = vdbe::ProgramExecutionState;
type Row = vdbe::Row;
type StepResult = vdbe::StepResult;

pub struct Statement {
    pub(crate) program: vdbe::Program,
    state: vdbe::ProgramState,
    pager: Arc<Pager>,
    /// indicates if the statement is a NORMAL/EXPLAIN/EXPLAIN QUERY PLAN
    query_mode: QueryMode,
    /// Flag to show if the statement was busy
    busy: bool,
    /// Busy handler state for tracking invocations and timeouts
    busy_handler_state: Option<BusyHandlerState>,
    /// True once step() has returned Row for a write statement (INSERT/UPDATE/DELETE
    /// with RETURNING). With ephemeral-buffered RETURNING, the first Row proves all
    /// DML completed — only the scan-back remains. Used by reset_internal to decide
    /// commit vs rollback when a statement is abandoned.
    has_returned_row: bool,
}

crate::assert::assert_send_sync!(Statement);

impl std::fmt::Debug for Statement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Statement").finish()
    }
}

impl Drop for Statement {
    fn drop(&mut self) {
        self.reset_best_effort();
    }
}

impl Statement {
    pub fn new(program: vdbe::Program, pager: Arc<Pager>, query_mode: QueryMode) -> Self {
        let (max_registers, cursor_count) = match query_mode {
            QueryMode::Normal => (program.max_registers, program.cursor_ref.len()),
            QueryMode::Explain => (EXPLAIN_COLUMNS.len(), 0),
            QueryMode::ExplainQueryPlan => (EXPLAIN_QUERY_PLAN_COLUMNS.len(), 0),
        };
        let state = vdbe::ProgramState::new(max_registers, cursor_count);
        Self {
            program,
            state,
            pager,
            query_mode,
            busy: false,
            busy_handler_state: None,
            has_returned_row: false,
        }
    }

    pub fn get_trigger(&self) -> Option<Arc<Trigger>> {
        self.program.trigger.clone()
    }

    pub fn get_query_mode(&self) -> QueryMode {
        self.query_mode
    }

    pub fn get_program(&self) -> &vdbe::Program {
        &self.program
    }

    pub fn get_pager(&self) -> &Arc<Pager> {
        &self.pager
    }

    pub fn n_change(&self) -> i64 {
        self.state
            .n_change
            .load(crate::sync::atomic::Ordering::SeqCst)
    }

    pub fn set_mv_tx(&mut self, mv_tx: Option<(u64, TransactionMode)>) {
        self.program.connection.set_mv_tx(mv_tx);
    }

    pub fn interrupt(&mut self) {
        self.state.interrupt();
    }

    pub fn execution_state(&self) -> ProgramExecutionState {
        self.state.execution_state
    }

    pub fn mv_store(&self) -> impl Deref<Target = Option<Arc<MvStore>>> {
        self.program.connection.mv_store()
    }

    /// Take the pending IO completions from this statement.
    /// Returns None if no IO is pending.
    /// This is used by async state machines that need to yield the completions.
    pub fn take_io_completions(&mut self) -> Option<crate::types::IOCompletions> {
        self.state.io_completions.take()
    }

    fn _step(&mut self, waker: Option<&Waker>) -> Result<StepResult> {
        if matches!(self.state.execution_state, ProgramExecutionState::Init)
            && !self
                .program
                .prepare_context
                .matches_connection(&self.program.connection)
        {
            self.reprepare()?;
        }
        // If we're waiting for a busy handler timeout, check if we can proceed
        if let Some(busy_state) = self.busy_handler_state.as_ref() {
            if self.pager.io.current_time_monotonic() < busy_state.timeout() {
                // Yield the query as the timeout has not been reached yet
                if let Some(waker) = waker {
                    waker.wake_by_ref();
                }
                return Ok(StepResult::IO);
            }
        }

        const MAX_SCHEMA_RETRY: usize = 50;
        let mut res =
            self.program
                .step(&mut self.state, self.pager.clone(), self.query_mode, waker);
        for attempt in 0..MAX_SCHEMA_RETRY {
            // Only reprepare if we still need to update schema
            if !matches!(res, Err(LimboError::SchemaUpdated)) {
                break;
            }
            tracing::debug!("reprepare: attempt={}", attempt);
            self.reprepare()?;
            res = self
                .program
                .step(&mut self.state, self.pager.clone(), self.query_mode, waker);
        }

        // Aggregate metrics when statement completes
        if matches!(res, Ok(StepResult::Done)) {
            let mut conn_metrics = self.program.connection.metrics.write();
            conn_metrics.record_statement(self.state.metrics.clone());
            self.busy = false;
            self.busy_handler_state = None; // Reset busy state on completion
            drop(conn_metrics);

            // After ANALYZE completes, refresh in-memory stats so planners can use them.
            let sql = self.program.sql.trim_start();
            if sql.to_ascii_uppercase().starts_with("ANALYZE") {
                refresh_analyze_stats(&self.program.connection);
            }
        } else {
            self.busy = true;
        }

        // Handle busy result by invoking the busy handler
        if matches!(res, Ok(StepResult::Busy)) {
            let now = self.pager.io.current_time_monotonic();
            let handler = self.program.connection.get_busy_handler();

            // Initialize or get existing busy handler state
            let busy_state = self
                .busy_handler_state
                .get_or_insert_with(|| BusyHandlerState::new(now));

            // Invoke the busy handler to determine if we should retry
            if busy_state.invoke(&handler, now) {
                // Handler says retry, yield with IO to wait for timeout
                if let Some(waker) = waker {
                    waker.wake_by_ref();
                }
                res = Ok(StepResult::IO);
                #[cfg(shuttle)]
                crate::thread::spin_loop();
            }
            // else: Handler says stop, res stays as Busy
        }

        // Track when a write statement yields its first Row. With ephemeral-buffered
        // RETURNING, this proves all DML completed — only the scan-back remains.
        if matches!(res, Ok(StepResult::Row))
            && self.query_mode == QueryMode::Normal
            && self.program.change_cnt_on
            && !self.program.result_columns.is_empty()
        {
            self.has_returned_row = true;
        }

        res
    }

    pub fn step(&mut self) -> Result<StepResult> {
        self._step(None)
    }

    pub fn step_with_waker(&mut self, waker: &Waker) -> Result<StepResult> {
        self._step(Some(waker))
    }

    pub fn run_ignore_rows(&mut self) -> Result<()> {
        loop {
            match self.step()? {
                vdbe::StepResult::Done => return Ok(()),
                vdbe::StepResult::IO => self.pager.io.step()?,
                vdbe::StepResult::Row => continue,
                vdbe::StepResult::Interrupt | vdbe::StepResult::Busy => {
                    return Err(LimboError::Busy)
                }
            }
        }
    }

    pub fn run_collect_rows(&mut self) -> Result<Vec<Vec<Value>>> {
        let mut values = Vec::new();
        loop {
            match self.step()? {
                vdbe::StepResult::Done => return Ok(values),
                vdbe::StepResult::IO => self.pager.io.step()?,
                vdbe::StepResult::Row => {
                    values.push(self.row().unwrap().get_values().cloned().collect());
                    continue;
                }
                vdbe::StepResult::Interrupt | vdbe::StepResult::Busy => {
                    return Err(LimboError::Busy)
                }
            }
        }
    }

    /// Blocks execution, advances IO, and runs to completion of the statement
    pub fn run_with_row_callback(
        &mut self,
        mut func: impl FnMut(&Row) -> Result<()>,
    ) -> Result<()> {
        loop {
            match self.step()? {
                vdbe::StepResult::Done => break,
                vdbe::StepResult::IO => self.pager.io.step()?,
                vdbe::StepResult::Row => {
                    func(self.row().expect("row should be present"))?;
                }
                vdbe::StepResult::Interrupt => return Err(LimboError::Interrupt),
                vdbe::StepResult::Busy => return Err(LimboError::Busy),
            }
        }
        Ok(())
    }

    /// Blocks execution, advances IO, and stops at any StepResult except IO
    /// You can optionally pass a handler to run after IO is advanced
    pub fn run_one_step_blocking(
        &mut self,
        mut pre_io_func: impl FnMut() -> Result<()>,
        mut post_io_func: impl FnMut() -> Result<()>,
    ) -> Result<Option<&Row>> {
        let result = loop {
            match self.step()? {
                vdbe::StepResult::Done => break None,
                vdbe::StepResult::IO => {
                    pre_io_func()?;
                    self.pager.io.step()?;
                    post_io_func()?;
                }
                vdbe::StepResult::Row => break Some(self.row().expect("row should be present")),
                vdbe::StepResult::Interrupt => return Err(LimboError::Interrupt),
                vdbe::StepResult::Busy => return Err(LimboError::Busy),
            }
        };
        Ok(result)
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    fn reprepare(&mut self) -> Result<()> {
        tracing::trace!("repreparing statement");
        let conn = self.program.connection.clone();

        // End transactions on attached database pagers so they get a fresh view
        // of the database. Without this, the pager would still see the old page 1
        // with the stale schema cookie, causing an infinite SchemaUpdated loop.
        // SchemaUpdated can occur at different points in the Transaction opcode,
        // so the attached pager may or may not hold locks at this point.
        let attached_db_ids: Vec<usize> = self
            .program
            .prepared
            .write_databases
            .iter()
            .chain(self.program.prepared.read_databases.iter())
            .filter(|&&id| crate::is_attached_db(id))
            .copied()
            .collect();
        for db_id in attached_db_ids {
            let pager = conn.get_pager_from_database_index(&db_id);
            // Discard any connection-local schema changes for this attached DB
            // so the re-translate reads the committed schema.
            conn.database_schemas().write().remove(&db_id);
            if pager.holds_read_lock() {
                pager.rollback_attached();
            }
        }

        // Check if the DB-level schema is stale (e.g., another process created
        // a table, incrementing the schema cookie on disk). If so, reparse
        // from sqlite_schema on disk so we get the new table definitions.
        // In MVCC mode, read from MvStore (the authoritative source) to match
        // what the Transaction opcode sees and avoid infinite recursion.
        let on_disk_cookie = conn.effective_schema_cookie().unwrap_or_else(|e| {
            tracing::warn!("effective_schema_cookie failed during reprepare: {e}");
            0
        });
        let db_cookie = conn.db.schema.lock().schema_version;
        if on_disk_cookie != db_cookie {
            conn.reparse_schema()?;
            let schema = conn.schema.read().clone();
            conn.db.update_schema_if_newer(schema);
        } else {
            *conn.schema.write() = conn.db.clone_schema();
        }
        self.program = {
            let mut parser = Parser::new(self.program.sql.as_bytes());
            let cmd = parser.next_cmd()?;
            let cmd = cmd.expect("Same SQL string should be able to be parsed");

            let syms = conn.syms.read();
            let mode = self.query_mode;
            #[cfg(debug_assertions)]
            turso_assert_eq!(QueryMode::new(&cmd), mode);
            let (Cmd::Stmt(stmt) | Cmd::Explain(stmt) | Cmd::ExplainQueryPlan(stmt)) = cmd;
            let schema = conn.schema.read().clone();
            translate::translate(
                &schema,
                stmt,
                self.pager.clone(),
                conn.clone(),
                &syms,
                mode,
                &self.program.sql,
            )?
        };

        // Save parameters before they are reset
        let parameters = std::mem::take(&mut self.state.parameters);
        let (max_registers, cursor_count) = match self.query_mode {
            QueryMode::Normal => (self.program.max_registers, self.program.cursor_ref.len()),
            QueryMode::Explain => (EXPLAIN_COLUMNS.len(), 0),
            QueryMode::ExplainQueryPlan => (EXPLAIN_QUERY_PLAN_COLUMNS.len(), 0),
        };
        self.reset_internal(Some(max_registers), Some(cursor_count))?;
        // Load the parameters back into the state
        self.state.parameters = parameters;
        Ok(())
    }

    pub fn num_columns(&self) -> usize {
        match self.query_mode {
            QueryMode::Normal => self.program.result_columns.len(),
            QueryMode::Explain => EXPLAIN_COLUMNS.len(),
            QueryMode::ExplainQueryPlan => EXPLAIN_QUERY_PLAN_COLUMNS.len(),
        }
    }

    pub fn get_column_name(&self, idx: usize) -> Cow<'_, str> {
        if self.query_mode == QueryMode::Explain {
            return Cow::Owned(EXPLAIN_COLUMNS.get(idx).expect("No column").to_string());
        }
        if self.query_mode == QueryMode::ExplainQueryPlan {
            return Cow::Owned(
                EXPLAIN_QUERY_PLAN_COLUMNS
                    .get(idx)
                    .expect("No column")
                    .to_string(),
            );
        }
        match self.query_mode {
            QueryMode::Normal => {
                let column = &self.program.result_columns.get(idx).expect("No column");
                match column.name(&self.program.table_references) {
                    Some(name) => Cow::Borrowed(name),
                    None => {
                        let tables = [&self.program.table_references];
                        let ctx = PlanContext(&tables);
                        Cow::Owned(column.expr.displayer(&ctx).to_string())
                    }
                }
            }
            QueryMode::Explain => Cow::Borrowed(EXPLAIN_COLUMNS[idx]),
            QueryMode::ExplainQueryPlan => Cow::Borrowed(EXPLAIN_QUERY_PLAN_COLUMNS[idx]),
        }
    }

    pub fn get_column_table_name(&self, idx: usize) -> Option<Cow<'_, str>> {
        if self.query_mode == QueryMode::Explain || self.query_mode == QueryMode::ExplainQueryPlan {
            return None;
        }
        let column = &self.program.result_columns.get(idx).expect("No column");
        match &column.expr {
            turso_parser::ast::Expr::Column { table, .. } => self
                .program
                .table_references
                .find_table_by_internal_id(*table)
                .map(|(_, table_ref)| Cow::Borrowed(table_ref.get_name())),
            _ => None,
        }
    }

    /// Returns the declared type of a result column.
    ///
    /// This behaves similarly to SQLite's `sqlite3_column_decltype()`:
    /// If the Nth column of the returned result set of a SELECT is a table column
    /// (not an expression or subquery) then the declared type of the table column
    /// is returned. If the Nth column of the result set is an expression or subquery,
    /// then None is returned. The returned string is always UTF-8 encoded.
    ///
    /// See: <https://sqlite.org/c3ref/column_decltype.html>
    pub fn get_column_decltype(&self, idx: usize) -> Option<String> {
        if self.query_mode == QueryMode::Explain {
            return Some(
                EXPLAIN_COLUMNS_TYPE
                    .get(idx)
                    .expect("No column")
                    .to_string(),
            );
        }
        if self.query_mode == QueryMode::ExplainQueryPlan {
            return Some(
                EXPLAIN_QUERY_PLAN_COLUMNS_TYPE
                    .get(idx)
                    .expect("No column")
                    .to_string(),
            );
        }
        let column = &self.program.result_columns.get(idx).expect("No column");
        match &column.expr {
            turso_parser::ast::Expr::Column {
                table,
                column: column_idx,
                ..
            } => {
                let (_, table_ref) = self
                    .program
                    .table_references
                    .find_table_by_internal_id(*table)?;
                let table_column = table_ref.get_column_at(*column_idx)?;
                let ty_str = &table_column.ty_str;
                if ty_str.is_empty() {
                    None
                } else {
                    Some(ty_str.clone())
                }
            }
            _ => None,
        }
    }

    /// Returns the type affinity name of a result column (e.g., "INTEGER", "TEXT", "REAL", "BLOB", "NUMERIC").
    ///
    /// Unlike `get_column_decltype` which returns the original declared type string,
    /// this method returns the normalized SQLite type affinity name.
    pub fn get_column_type_name(&self, idx: usize) -> Option<String> {
        if self.query_mode == QueryMode::Explain {
            return Some(
                EXPLAIN_COLUMNS_TYPE
                    .get(idx)
                    .expect("No column")
                    .to_string(),
            );
        }
        if self.query_mode == QueryMode::ExplainQueryPlan {
            return Some(
                EXPLAIN_QUERY_PLAN_COLUMNS_TYPE
                    .get(idx)
                    .expect("No column")
                    .to_string(),
            );
        }
        let column = &self.program.result_columns.get(idx).expect("No column");
        match &column.expr {
            turso_parser::ast::Expr::Column {
                table,
                column: column_idx,
                ..
            } => {
                let (_, table_ref) = self
                    .program
                    .table_references
                    .find_table_by_internal_id(*table)?;
                let table_column = table_ref.get_column_at(*column_idx)?;
                match &table_column.ty() {
                    crate::schema::Type::Integer => Some("INTEGER".to_string()),
                    crate::schema::Type::Real => Some("REAL".to_string()),
                    crate::schema::Type::Text => Some("TEXT".to_string()),
                    crate::schema::Type::Blob => Some("BLOB".to_string()),
                    crate::schema::Type::Numeric => Some("NUMERIC".to_string()),
                    crate::schema::Type::Null => None,
                }
            }
            _ => None,
        }
    }

    pub fn parameters(&self) -> &parameters::Parameters {
        &self.program.parameters
    }

    pub fn parameters_count(&self) -> usize {
        self.program.parameters.count()
    }

    pub fn parameter_index(&self, name: &str) -> Option<NonZero<usize>> {
        self.program.parameters.index(name)
    }

    pub fn bind_at(&mut self, index: NonZero<usize>, value: Value) {
        self.state.bind_at(index, value);
    }

    pub fn clear_bindings(&mut self) {
        self.state.clear_bindings();
    }

    pub fn reset(&mut self) -> Result<()> {
        self.reset_internal(None, None)
    }

    pub fn reset_best_effort(&mut self) {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.reset())) {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                tracing::error!("Statement reset failed during best-effort cleanup: {err}");
            }
            Err(_) => {
                tracing::error!("Statement reset panicked during best-effort cleanup");
            }
        }
    }

    fn reset_internal(
        &mut self,
        max_registers: Option<usize>,
        max_cursors: Option<usize>,
    ) -> Result<()> {
        fn capture_reset_error(
            reset_error: &mut Option<LimboError>,
            err: LimboError,
            context: &str,
        ) {
            tracing::error!("{context}: {err}");
            if reset_error.is_none() {
                *reset_error = Some(err);
            }
        }

        let mut reset_error: Option<LimboError> = None;

        if let Some(io) = self.state.io_completions.take() {
            if let Err(err) = io.wait(self.pager.io.as_ref()) {
                capture_reset_error(
                    &mut reset_error,
                    err,
                    "Error while draining pending IO during statement reset",
                );
            }
        }

        if self.state.execution_state.is_running() {
            if self.query_mode == QueryMode::Normal
                && self.program.change_cnt_on
                && self.has_returned_row
            {
                // Write statement with RETURNING, user got at least one Row.
                // With ephemeral-buffered RETURNING, ALL DML completed before any
                // rows were yielded. The remaining work is just the scan-back
                // (in-memory) + Halt. Commit the transaction via halt().
                let mut halt_completed = false;
                loop {
                    match vdbe::execute::halt(
                        &self.program,
                        &mut self.state,
                        &self.pager,
                        0,
                        "",
                        None,
                    ) {
                        Ok(vdbe::execute::InsnFunctionStepResult::Done) => {
                            halt_completed = true;
                            break;
                        }
                        Ok(vdbe::execute::InsnFunctionStepResult::IO(_)) => {
                            if let Err(e) = self.pager.io.step() {
                                capture_reset_error(
                                    &mut reset_error,
                                    e,
                                    "Error committing during statement reset",
                                );
                                break;
                            }
                        }
                        Err(e) => {
                            capture_reset_error(
                                &mut reset_error,
                                e,
                                "Error halting statement during reset",
                            );
                            break;
                        }
                        Ok(vdbe::execute::InsnFunctionStepResult::Row)
                        | Ok(vdbe::execute::InsnFunctionStepResult::Step) => {
                            capture_reset_error(
                                &mut reset_error,
                                LimboError::InternalError(
                                    "Unexpected halt result during reset".to_string(),
                                ),
                                "Statement reset encountered unexpected halt result",
                            );
                            break;
                        }
                    }
                }

                if !halt_completed {
                    if let Err(abort_err) =
                        self.program
                            .abort(&self.pager, reset_error.as_ref(), &mut self.state)
                    {
                        capture_reset_error(
                            &mut reset_error,
                            abort_err,
                            "Abort failed during statement reset",
                        );
                    }
                }
            } else {
                // Either a read-only statement, a write statement that never
                // yielded a Row (DML still in progress or hit Busy/error), or a
                // write statement without RETURNING. Rollback to avoid committing
                // partial DML or silently retrying after transient errors (Busy).
                if let Err(abort_err) = self.program.abort(&self.pager, None, &mut self.state) {
                    capture_reset_error(
                        &mut reset_error,
                        abort_err,
                        "Abort failed during statement reset",
                    );
                }
            }
        } else {
            // Statement not running (Done/Failed/Init) — cleanup only.
            if let Err(abort_err) = self.program.abort(&self.pager, None, &mut self.state) {
                capture_reset_error(
                    &mut reset_error,
                    abort_err,
                    "Abort failed during statement reset",
                );
            }
        }
        self.state.reset(max_registers, max_cursors);
        self.state.n_change.store(0, Ordering::SeqCst);
        self.busy = false;
        self.busy_handler_state = None;
        self.has_returned_row = false;

        if let Some(err) = reset_error {
            return Err(err);
        }
        Ok(())
    }

    pub fn row(&self) -> Option<&Row> {
        self.state.result_row.as_ref()
    }

    pub fn get_sql(&self) -> &str {
        &self.program.sql
    }

    pub fn is_busy(&self) -> bool {
        self.busy
    }

    /// Internal method to get IO from a statement.
    /// Used by select internal crate
    ///
    /// Avoid using this method for advancing IO while iteration over `step`.
    /// Prefer to use helper methods instead such as [Self::run_with_row_callback]
    pub fn _io(&self) -> &dyn crate::IO {
        self.pager.io.as_ref()
    }
}
