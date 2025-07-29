use std::sync::{Arc, Mutex};

use crate::{
    generation::plan::{Interaction, InteractionPlanState},
    runner::{execution::ExecutionContinuation, future::FuturesByConnection},
};

use super::{
    env::{SimConnection, SimulatorEnv},
    execution::{execute_interaction, Execution, ExecutionHistory, ExecutionResult},
};

pub(crate) fn run_simulation(
    env: Arc<Mutex<SimulatorEnv>>,
    plans: Vec<Arc<Vec<Vec<Interaction>>>>,
    last_execution: Arc<Mutex<Execution>>,
) -> ExecutionResult {
    let states = plans
        .iter()
        .map(|_| {
            Arc::new(Mutex::new(InteractionPlanState {
                stack: vec![],
                interaction_pointer: 0,
                secondary_pointer: 0,
            }))
        })
        .collect::<Vec<_>>();
    let result = execute_plans(env.clone(), plans, states, last_execution);

    let env = env.lock().unwrap();
    env.io.print_stats();

    tracing::info!("Simulation completed");

    result
}

pub(crate) fn execute_plans(
    env: Arc<Mutex<SimulatorEnv>>,
    plans: Vec<Arc<Vec<Vec<Interaction>>>>,
    states: Vec<Arc<Mutex<InteractionPlanState>>>,
    last_execution: Arc<Mutex<Execution>>,
) -> ExecutionResult {
    let mut history = ExecutionHistory::new();
    let now = std::time::Instant::now();
    let (ticks, connections_len, max_time_simulation) = {
        let env_guard = env.lock().unwrap();
        (
            env_guard.opts.ticks,
            env_guard.connections.len(),
            env_guard.opts.max_time_simulation,
        )
    };
    let mut futures_by_connection = FuturesByConnection::new(connections_len);
    for _tick in 0..ticks {
        // Run every connection concurrently.
        for connection_index in 0..connections_len {
            let state = states[connection_index].clone();
            let plan = plans[connection_index].clone();
            let connection = env.lock().unwrap().connections[connection_index].clone();
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
            // Execute the interaction for the selected connection
            if futures_by_connection.connection_without_future(connection_index) {
                futures_by_connection.set_future(
                    connection_index,
                    Box::pin(execute_plan(
                        env.clone(),
                        connection.clone(),
                        plan.clone(),
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
                    Some(turso_core::LimboError::InternalError(
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
    plan: Arc<Vec<Vec<Interaction>>>,
    state: Arc<Mutex<InteractionPlanState>>,
) -> turso_core::Result<()> {
    let interaction_pointer = state.lock().unwrap().interaction_pointer;
    let secondary_pointer = state.lock().unwrap().secondary_pointer;
    if interaction_pointer >= plan.len() {
        return Ok(());
    }

    let interaction = &plan[interaction_pointer][secondary_pointer];

    let is_connected = connection.lock().unwrap().is_connected();
    if !is_connected {
        let env = env.lock().unwrap();
        let mut conn = connection.lock().unwrap();
        tracing::debug!("connecting {}", conn);
        *conn = SimConnection::LimboConnection(env.db.connect().unwrap());
        return Ok(());
    }

    match execute_interaction(env.clone(), connection.clone(), interaction, state.clone()).await {
        Ok(next_execution) => {
            tracing::debug!("connection {} processed", 1); // todo: add index to simconnetion
                                                           // Move to the next interaction or property
            match next_execution {
                ExecutionContinuation::NextInteraction => {
                    let mut state = state.lock().unwrap();
                    if state.secondary_pointer + 1 >= plan[state.interaction_pointer].len() {
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

    Ok(())
}
