use std::sync::{Arc, Mutex};

use tracing::instrument;
use turso_core::{LimboError, Result, StepResult};

use crate::{
    generation::{
        plan::{Interaction, InteractionPlan, InteractionPlanState},
        Shadow,
    },
    runner::future::FuturesByConnection,
};

use super::env::{SimConnection, SimulatorEnv};

#[derive(Debug, Clone, Copy)]
pub(crate) struct Execution {
    pub(crate) connection_index: usize,
    pub(crate) interaction_index: usize,
    pub(crate) secondary_index: usize,
}

impl Execution {
    pub(crate) fn new(
        connection_index: usize,
        interaction_index: usize,
        secondary_index: usize,
    ) -> Self {
        Self {
            connection_index,
            interaction_index,
            secondary_index,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ExecutionHistory {
    pub(crate) history: Vec<Execution>,
}

impl ExecutionHistory {
    pub(crate) fn new() -> Self {
        Self {
            history: Vec::new(),
        }
    }
}

pub(crate) struct ExecutionResult {
    pub(crate) history: ExecutionHistory,
    pub(crate) error: Option<LimboError>,
}

impl ExecutionResult {
    pub(crate) fn new(history: ExecutionHistory, error: Option<LimboError>) -> Self {
        Self { history, error }
    }
}

pub(crate) fn execute_plans(
    env: Arc<Mutex<SimulatorEnv>>,
    plans: Vec<Arc<InteractionPlan>>,
    states: Vec<Arc<Mutex<InteractionPlanState>>>,
    last_execution: Arc<Mutex<Execution>>,
) -> ExecutionResult {
    let mut history = ExecutionHistory::new();
    let now = std::time::Instant::now();
    let mut futures_by_connection = FuturesByConnection::new(env.lock().unwrap().connections.len());
    let ticks = env.lock().unwrap().opts.ticks;
    let max_time_simulation = env.lock().unwrap().opts.max_time_simulation;
    let connections_len = env.lock().unwrap().connections.len();
    for _tick in 0..ticks {
        // Run every connection concurrently.
        for connection_index in 0..connections_len {
            std::thread::sleep(std::time::Duration::from_millis(
                std::env::var("TICK_SLEEP")
                    .unwrap_or("0".into())
                    .parse()
                    .unwrap_or(0),
            ));
            let state = states[connection_index].clone();
            {
                let state = state.lock().unwrap();
                history.history.push(Execution::new(
                    connection_index,
                    state.interaction_pointer,
                    state.secondary_pointer,
                ));
                let mut last_execution = last_execution.lock().unwrap();
                last_execution.connection_index = connection_index;
                last_execution.interaction_index = state.interaction_pointer;
                last_execution.secondary_index = state.secondary_pointer;
            }
            let connection = env.lock().unwrap().connections[connection_index].clone();
            // Execute the interaction for the selected connection
            if futures_by_connection.connection_without_future(connection_index) {
                futures_by_connection.set_future(
                    connection_index,
                    Box::pin(execute_plan(
                        env.clone(),
                        connection.clone(),
                        plans[connection_index].clone(),
                        state.clone(),
                    )),
                );
            }

            match futures_by_connection.poll_at(connection_index) {
                Ok(_) => {}
                Err(err) => {
                    return ExecutionResult::new(history, Some(err));
                }
            }

            // Check if the maximum time for the simulation has been reached
            if now.elapsed().as_secs() >= max_time_simulation as u64 {
                return ExecutionResult::new(
                    history,
                    Some(LimboError::InternalError(
                        "maximum time for simulation reached".into(),
                    )),
                );
            }
        }
    }

    ExecutionResult::new(history, None)
}

async fn execute_plan(
    env: Arc<Mutex<SimulatorEnv>>,
    connection: Arc<Mutex<SimConnection>>,
    plan: Arc<InteractionPlan>,
    state: Arc<Mutex<InteractionPlanState>>,
) -> Result<()> {
    let interaction_pointer = state.lock().unwrap().interaction_pointer;
    let secondary_pointer = state.lock().unwrap().secondary_pointer;
    if interaction_pointer >= plan.plan.len() {
        return Ok(());
    }

    let interaction = &plan.plan[interaction_pointer].interactions()[secondary_pointer];

    let is_connected = connection.lock().unwrap().is_connected();
    if !is_connected {
        tracing::debug!("connecting {}", 1); // todo: add index to simconnetion
        let env = env.lock().unwrap();
        *connection.lock().unwrap() = SimConnection::LimboConnection(env.db.connect().unwrap());
    } else {
        tracing::debug!("connection {} already connected", 1); // todo: add index to simconnetion
        match execute_interaction(env.clone(), connection.clone(), interaction, state.clone()).await
        {
            Ok(next_execution) => {
                tracing::debug!("connection {} processed", 1); // todo: add index to simconnetion
                                                               // Move to the next interaction or property
                match next_execution {
                    ExecutionContinuation::NextInteraction => {
                        let mut state = state.lock().unwrap();
                        if state.secondary_pointer + 1
                            >= plan.plan[state.interaction_pointer].interactions().len()
                        {
                            // If we have reached the end of the interactions for this property, move to the next property
                            state.interaction_pointer += 1;
                            state.secondary_pointer = 0;
                        } else {
                            // Otherwise, move to the next interaction
                            state.secondary_pointer += 1;
                        }
                    }
                    ExecutionContinuation::NextProperty => {
                        // Skip to the next property
                        let mut state = state.lock().unwrap();
                        state.interaction_pointer += 1;
                        state.secondary_pointer = 0;
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
    /// Typically used in the case of preconditions failures, skip to the next property.
    NextProperty,
}

#[instrument(skip(env, interaction, state, connection), fields(interaction = %interaction))]
pub(crate) async fn execute_interaction(
    env: Arc<Mutex<SimulatorEnv>>,
    connection: Arc<Mutex<SimConnection>>,
    interaction: &Interaction,
    state: Arc<Mutex<InteractionPlanState>>,
) -> Result<ExecutionContinuation> {
    // Leave this empty info! here to print the span of the execution
    tracing::info!("");
    match interaction {
        Interaction::Query(_) => {
            tracing::debug!(?interaction);
            let results = interaction.execute_query(connection.clone()).await;
            tracing::debug!(?results);
            state.lock().unwrap().stack.push(results);
            limbo_integrity_check(connection.clone())?;
        }
        Interaction::FsyncQuery(query) => {
            // SAFETY: we lock connection only inside execute plan which is async
            let results =
                interaction.execute_fsync_query(connection.clone(), &mut env.lock().unwrap());
            tracing::debug!(?results);
            state.lock().unwrap().stack.push(results);

            let query_interaction = Interaction::Query(query.clone());

            // Avoid recursive .await to prevent infinitely sized future.
            // Box::pin is used to introduce indirection.
            {
                let fut =
                    execute_interaction(env.clone(), connection.clone(), &query_interaction, state);
                Box::pin(fut).await?;
            }
        }
        Interaction::Assertion(_) => {
            interaction
                .execute_assertion(&state.lock().unwrap().stack, &mut env.lock().unwrap())?;
            state.lock().unwrap().stack.clear();
        }
        Interaction::Assumption(_) => {
            let assumption_result = interaction
                .execute_assumption(&state.lock().unwrap().stack, &mut env.lock().unwrap());
            state.lock().unwrap().stack.clear();

            if assumption_result.is_err() {
                tracing::warn!("assumption failed: {:?}", assumption_result);
                return Ok(ExecutionContinuation::NextProperty);
            }
        }
        Interaction::Fault(_) => {
            interaction.execute_fault(&mut env.lock().unwrap(), connection)?;
        }
        Interaction::FaultyQuery(_) => {
            let results = interaction
                .execute_faulty_query(connection.clone(), env.clone())
                .await;
            tracing::debug!(?results);
            state.lock().unwrap().stack.push(results);
            // Reset fault injection
            env.lock().unwrap().io.inject_fault(false);
            limbo_integrity_check(connection.clone())?;
        }
    }
    let _ = interaction.shadow(&mut env.lock().unwrap().tables);
    Ok(ExecutionContinuation::NextInteraction)
}

fn limbo_integrity_check(conn: Arc<Mutex<SimConnection>>) -> Result<()> {
    let mut conn = conn.lock().unwrap();
    let conn = match &mut *conn {
        SimConnection::LimboConnection(conn) => conn,
        SimConnection::SQLiteConnection(_) => unreachable!(),
        SimConnection::Disconnected => unreachable!(),
    };
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
