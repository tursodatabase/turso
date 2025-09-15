use std::sync::{Arc, Mutex};

use tracing::instrument;
use turso_core::{Connection, LimboError, Result, StepResult};

use crate::generation::{
    Shadow as _,
    plan::{ConnectionState, Interaction, InteractionPlanState, InteractionType, ResultSet},
};

use super::env::{SimConnection, SimulatorEnv};

#[derive(Debug, Clone, Copy)]
pub struct Execution {
    pub connection_index: usize,
    pub interaction_index: usize,
}

impl Execution {
    pub fn new(connection_index: usize, interaction_index: usize) -> Self {
        Self {
            connection_index,
            interaction_index,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ExecutionHistory {
    pub history: Vec<Execution>,
}

impl ExecutionHistory {
    pub fn new() -> Self {
        Self {
            history: Vec::new(),
        }
    }
}

pub struct ExecutionResult {
    pub history: ExecutionHistory,
    pub error: Option<LimboError>,
}

impl ExecutionResult {
    pub fn new(history: ExecutionHistory, error: Option<LimboError>) -> Self {
        Self { history, error }
    }
}

pub(crate) fn execute_interactions(
    env: Arc<Mutex<SimulatorEnv>>,
    interactions: Vec<Interaction>,
    state: &mut InteractionPlanState,
    conn_states: &mut [ConnectionState],
    last_execution: Arc<Mutex<Execution>>,
) -> ExecutionResult {
    let mut history = ExecutionHistory::new();
    let now = std::time::Instant::now();
    env.clear_poison();
    let mut env = env.lock().unwrap();

    env.tables.clear();

    for _tick in 0..env.opts.ticks {
        tracing::trace!("Executing tick {}", _tick);

        if state.interaction_pointer >= interactions.len() {
            break;
        }

        let interaction = &interactions[state.interaction_pointer];

        let connection_index = interaction.connection_index;
        let conn_state = &mut conn_states[connection_index];

        history
            .history
            .push(Execution::new(connection_index, state.interaction_pointer));
        let mut last_execution = last_execution.lock().unwrap();
        last_execution.connection_index = connection_index;
        last_execution.interaction_index = state.interaction_pointer;
        // Execute the interaction for the selected connection
        match execute_plan(&mut env, interaction, conn_state, state) {
            Ok(_) => {}
            Err(err) => {
                return ExecutionResult::new(history, Some(err));
            }
        }
        // Check if the maximum time for the simulation has been reached
        if now.elapsed().as_secs() >= env.opts.max_time_simulation as u64 {
            return ExecutionResult::new(
                history,
                Some(LimboError::InternalError(
                    "maximum time for simulation reached".into(),
                )),
            );
        }
    }

    ExecutionResult::new(history, None)
}

fn execute_plan(
    env: &mut SimulatorEnv,
    interaction: &Interaction,
    conn_state: &mut ConnectionState,
    state: &mut InteractionPlanState,
) -> Result<()> {
    let connection_index = interaction.connection_index;
    let connection = &mut env.connections[connection_index];
    if let SimConnection::Disconnected = connection {
        tracing::debug!("connecting {}", connection_index);
        *connection = SimConnection::LimboConnection(
            env.db.as_ref().expect("db to be Some").connect().unwrap(),
        );
    } else {
        tracing::debug!("connection {} already connected", connection_index);
        match execute_interaction(env, interaction, &mut conn_state.stack) {
            Ok(next_execution) => {
                tracing::debug!("connection {} processed", connection_index);
                // Move to the next interaction or property
                match next_execution {
                    ExecutionContinuation::NextInteraction => {
                        state.interaction_pointer += 1;
                    }
                }
            }
            Err(err) => {
                tracing::error!("error {}", err);
                return Err(err);
            }
        }
    }

    Ok(())
}

/// The next point of control flow after executing an interaction.
/// `execute_interaction` uses this type in conjunction with a result, where
/// the `Err` case indicates a full-stop due to a bug, and the `Ok` case
/// indicates the next step in the plan.
#[derive(PartialEq, Debug)]
pub(crate) enum ExecutionContinuation {
    /// Default continuation, execute the next interaction.
    NextInteraction,
    //  /// Typically used in the case of preconditions failures, skip to the next property.
    // NextProperty,
}

#[instrument(skip(env, interaction, stack), fields(seed = %env.opts.seed, interaction = %interaction))]
pub(crate) fn execute_interaction(
    env: &mut SimulatorEnv,
    interaction: &Interaction,
    stack: &mut Vec<ResultSet>,
) -> Result<ExecutionContinuation> {
    // Leave this empty info! here to print the span of the execution
    let connection = &mut env.connections[interaction.connection_index];
    tracing::info!("");
    match &interaction.interaction {
        InteractionType::Query(_) => {
            let conn = match connection {
                SimConnection::LimboConnection(conn) => conn,
                SimConnection::SQLiteConnection(_) => unreachable!(),
                SimConnection::Disconnected => unreachable!(),
            };
            tracing::debug!(?interaction);
            let results = interaction.execute_query(conn);
            if results.is_err() {
                tracing::error!(?results);
            }
            stack.push(results);
            limbo_integrity_check(conn)?;
        }
        InteractionType::FsyncQuery(query) => {
            let conn = match &connection {
                SimConnection::LimboConnection(conn) => conn.clone(),
                SimConnection::SQLiteConnection(_) => unreachable!(),
                SimConnection::Disconnected => unreachable!(),
            };

            let results = interaction.execute_fsync_query(conn.clone(), env);
            if results.is_err() {
                tracing::error!(?results);
            }
            stack.push(results);

            let query_interaction = Interaction::new(
                interaction.connection_index,
                InteractionType::Query(query.clone()),
            );

            execute_interaction(env, &query_interaction, stack)?;
        }
        InteractionType::Assertion(_) => {
            interaction.execute_assertion(stack, env)?;
            stack.clear();
        }
        InteractionType::Assumption(_) => {
            let assumption_result = interaction.execute_assumption(stack, env);
            stack.clear();

            if assumption_result.is_err() {
                tracing::warn!("assumption failed: {:?}", assumption_result);
                todo!("remove assumptions");
                // return Ok(ExecutionContinuation::NextProperty);
            }
        }
        InteractionType::Fault(_) => {
            interaction.execute_fault(env, interaction.connection_index)?;
        }
        InteractionType::FaultyQuery(_) => {
            let conn = match &connection {
                SimConnection::LimboConnection(conn) => conn.clone(),
                SimConnection::SQLiteConnection(_) => unreachable!(),
                SimConnection::Disconnected => unreachable!(),
            };

            let results = interaction.execute_faulty_query(&conn, env);
            if results.is_err() {
                tracing::error!(?results);
            }
            stack.push(results);
            // Reset fault injection
            env.io.inject_fault(false);
            limbo_integrity_check(&conn)?;
        }
    }
    let _ = interaction.shadow(&mut env.tables);
    Ok(ExecutionContinuation::NextInteraction)
}

fn limbo_integrity_check(conn: &Arc<Connection>) -> Result<()> {
    let mut rows = conn.query("PRAGMA integrity_check;")?.unwrap();
    let mut result = Vec::new();

    while let Ok(row) = rows.step() {
        match row {
            StepResult::Row => {
                let row = rows.row().unwrap();

                let val = match row.get_value(0) {
                    turso_core::Value::Text(text) => text.as_str().to_string(),
                    _ => unreachable!(),
                };
                result.push(val);
            }
            StepResult::IO => {
                rows.run_once()?;
            }
            StepResult::Interrupt => {}
            StepResult::Done => {
                break;
            }
            StepResult::Busy => {
                return Err(LimboError::Busy);
            }
        }
    }

    if result.is_empty() {
        return Err(LimboError::InternalError(
            "PRAGMA integrity_check did not return a value".to_string(),
        ));
    }
    let message = result.join("\n");
    if message != "ok" {
        return Err(LimboError::InternalError(format!(
            "Integrity Check Failed: {message}"
        )));
    }
    Ok(())
}
