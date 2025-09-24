// Project operator for DBSP-style incremental computation
// This operator projects/transforms columns in a relational stream

use crate::incremental::dbsp::{Delta, DeltaPair, HashableRow};
use crate::incremental::expr_compiler::CompiledExpression;
use crate::incremental::operator::{
    ComputationTracker, DbspStateCursors, EvalState, IncrementalOperator,
};
use crate::types::IOResult;
use crate::{Connection, Database, Result, Value};
use std::sync::{atomic::Ordering, Arc, Mutex};

#[derive(Debug, Clone)]
pub struct ProjectColumn {
    /// Compiled expression (handles both trivial columns and complex expressions)
    pub compiled: CompiledExpression,
}

/// Project operator - selects/transforms columns
#[derive(Clone)]
pub struct ProjectOperator {
    columns: Vec<ProjectColumn>,
    input_column_names: Vec<String>,
    output_column_names: Vec<String>,
    tracker: Option<Arc<Mutex<ComputationTracker>>>,
    // Internal in-memory connection for expression evaluation
    // Programs are very dependent on having a connection, so give it one.
    //
    // We could in theory pass the current connection, but there are a host of problems with that.
    // For example: during a write transaction, where views are usually updated, we have autocommit
    // on. When the program we are executing calls Halt, it will try to commit the current
    // transaction, which is absolutely incorrect.
    //
    // There are other ways to solve this, but a read-only connection to an empty in-memory
    // database gives us the closest environment we need to execute expressions.
    internal_conn: Arc<Connection>,
}

impl std::fmt::Debug for ProjectOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProjectOperator")
            .field("columns", &self.columns)
            .field("input_column_names", &self.input_column_names)
            .field("output_column_names", &self.output_column_names)
            .finish()
    }
}

impl ProjectOperator {
    /// Create a ProjectOperator from pre-compiled expressions
    pub fn from_compiled(
        compiled_exprs: Vec<CompiledExpression>,
        aliases: Vec<Option<String>>,
        input_column_names: Vec<String>,
        output_column_names: Vec<String>,
    ) -> crate::Result<Self> {
        // Set up internal connection for expression evaluation
        let io = Arc::new(crate::MemoryIO::new());
        let db = Database::open_file(
            io, ":memory:", false, // no MVCC needed for expression evaluation
            false, // no indexes needed
        )?;
        let internal_conn = db.connect()?;
        // Set to read-only mode and disable auto-commit since we're only evaluating expressions
        internal_conn.set_query_only(true);
        internal_conn.auto_commit.store(false, Ordering::SeqCst);

        // Create ProjectColumn structs from compiled expressions
        let columns: Vec<ProjectColumn> = compiled_exprs
            .into_iter()
            .zip(aliases)
            .map(|(compiled, _alias)| ProjectColumn { compiled })
            .collect();

        Ok(Self {
            columns,
            input_column_names,
            output_column_names,
            tracker: None,
            internal_conn,
        })
    }

    fn project_values(&self, values: &[Value]) -> Vec<Value> {
        let mut output = Vec::new();

        for col in &self.columns {
            // Use the internal connection's pager for expression evaluation
            let internal_pager = self.internal_conn.pager.read().clone();

            // Execute the compiled expression (handles both columns and complex expressions)
            let result = col
                .compiled
                .execute(values, internal_pager)
                .expect("Failed to execute compiled expression for the Project operator");
            output.push(result);
        }

        output
    }
}

impl IncrementalOperator for ProjectOperator {
    fn eval(
        &mut self,
        state: &mut EvalState,
        _cursors: &mut DbspStateCursors,
    ) -> Result<IOResult<Delta>> {
        let delta = match state {
            EvalState::Init { deltas } => {
                // Project operators only use left_delta, right_delta must be empty
                assert!(
                    deltas.right.is_empty(),
                    "ProjectOperator expects right_delta to be empty"
                );
                std::mem::take(&mut deltas.left)
            }
            _ => unreachable!(
                "ProjectOperator doesn't execute the state machine. Should be in Init state"
            ),
        };

        let mut output_delta = Delta::new();

        for (row, weight) in delta.changes {
            if let Some(tracker) = &self.tracker {
                tracker.lock().unwrap().record_project();
            }

            let projected = self.project_values(&row.values);
            let projected_row = HashableRow::new(row.rowid, projected);
            output_delta.changes.push((projected_row, weight));
        }

        *state = EvalState::Done;
        Ok(IOResult::Done(output_delta))
    }

    fn commit(
        &mut self,
        deltas: DeltaPair,
        _cursors: &mut DbspStateCursors,
    ) -> Result<IOResult<Delta>> {
        // Project operator only uses left delta, right must be empty
        assert!(
            deltas.right.is_empty(),
            "ProjectOperator expects right delta to be empty in commit"
        );

        let mut output_delta = Delta::new();

        // Commit the delta to our internal state and build output
        for (row, weight) in &deltas.left.changes {
            if let Some(tracker) = &self.tracker {
                tracker.lock().unwrap().record_project();
            }
            let projected = self.project_values(&row.values);
            let projected_row = HashableRow::new(row.rowid, projected);
            output_delta.changes.push((projected_row, *weight));
        }

        Ok(crate::types::IOResult::Done(output_delta))
    }

    fn set_tracker(&mut self, tracker: Arc<Mutex<ComputationTracker>>) {
        self.tracker = Some(tracker);
    }
}
