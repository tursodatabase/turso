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
    /// Whether the statement accesses the database.
    /// Used to determine whether we need to check for schema changes when
    /// starting a transaction.
    accesses_db: bool,
    /// indicates if the statement is a NORMAL/EXPLAIN/EXPLAIN QUERY PLAN
    query_mode: QueryMode,
    /// Flag to show if the statement was busy
    busy: bool,
    /// Busy handler state for tracking invocations and timeouts
    busy_handler_state: Option<BusyHandlerState>,
}

impl std::fmt::Debug for Statement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Statement").finish()
    }
}

impl Drop for Statement {
    fn drop(&mut self) {
        self.reset();
    }
}

impl Statement {
    pub fn new(program: vdbe::Program, pager: Arc<Pager>, query_mode: QueryMode) -> Self {
        let accesses_db = program.accesses_db;
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
            accesses_db,
            query_mode,
            busy: false,
            busy_handler_state: None,
        }
    }

    pub fn get_trigger(&self) -> Option<Arc<Trigger>> {
        self.program.trigger.clone()
    }

    pub fn get_query_mode(&self) -> QueryMode {
        self.query_mode
    }

    pub fn n_change(&self) -> i64 {
        self.state
            .n_change
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn set_mv_tx(&mut self, mv_tx: Option<(u64, TransactionMode)>) {
        *self.program.connection.mv_tx.write() = mv_tx;
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

    fn _step(&mut self, waker: Option<&Waker>) -> Result<StepResult> {
        // If we're waiting for a busy handler timeout, check if we can proceed
        if let Some(busy_state) = self.busy_handler_state.as_ref() {
            if self.pager.io.now() < busy_state.timeout() {
                // Yield the query as the timeout has not been reached yet
                if let Some(waker) = waker {
                    waker.wake_by_ref();
                }
                return Ok(StepResult::IO);
            }
        }

        let mut res = if !self.accesses_db {
            self.program
                .step(&mut self.state, self.pager.clone(), self.query_mode, waker)
        } else {
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
                res =
                    self.program
                        .step(&mut self.state, self.pager.clone(), self.query_mode, waker);
            }
            res
        };

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
            let now = self.pager.io.now();
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
            }
            // else: Handler says stop, res stays as Busy
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

        *conn.schema.write() = conn.db.clone_schema();
        self.program = {
            let mut parser = Parser::new(self.program.sql.as_bytes());
            let cmd = parser.next_cmd()?;
            let cmd = cmd.expect("Same SQL string should be able to be parsed");

            let syms = conn.syms.read();
            let mode = self.query_mode;
            debug_assert_eq!(QueryMode::new(&cmd), mode,);
            let (Cmd::Stmt(stmt) | Cmd::Explain(stmt) | Cmd::ExplainQueryPlan(stmt)) = cmd;
            translate::translate(
                conn.schema.read().deref(),
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
        self.reset_internal(Some(max_registers), Some(cursor_count));
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

    pub fn get_column_type(&self, idx: usize) -> Option<String> {
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

    pub fn reset(&mut self) {
        self.reset_internal(None, None);
    }

    fn reset_internal(&mut self, max_registers: Option<usize>, max_cursors: Option<usize>) {
        // Run write statements (INSERT/UPDATE/DELETE) to completion if still in progress.
        // This ensures INSERT...RETURNING and similar statements complete their work
        // even if not all returned rows were consumed.
        // Skip for read-only statements (SELECT) as they have no side effects.
        // Skip if we're already panicking to avoid double-panic in drop handlers.
        while self.program.change_cnt_on
            && !std::thread::panicking()
            && self.state.execution_state.is_running()
        {
            match self
                .program
                .step(&mut self.state, self.pager.clone(), QueryMode::Normal, None)
            {
                Ok(vdbe::StepResult::Done) | Ok(vdbe::StepResult::Row) => continue,
                Ok(vdbe::StepResult::IO) => {
                    if let Err(e) = self.pager.io.step() {
                        tracing::error!("Error running statement to completion: {}", e);
                        break;
                    }
                }
                Err(e) => {
                    tracing::error!("Error running statement to completion: {}", e);
                    break;
                }
                Ok(vdbe::StepResult::Interrupt) | Ok(vdbe::StepResult::Busy) => break,
            }
        }
        // as abort uses auto_txn_cleanup value - it needs to be called before state.reset
        self.program.abort(&self.pager, None, &mut self.state);
        self.state.reset(max_registers, max_cursors);
        self.state.n_change.store(0, Ordering::SeqCst);
        self.busy = false;
        self.busy_handler_state = None;
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
