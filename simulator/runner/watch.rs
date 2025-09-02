use std::sync::{Arc, Mutex};

use sql_generation::generation::pick_index;

use crate::{
    generation::plan::{Interaction, InteractionPlanState},
    integrity_check,
    runner::execution::ExecutionContinuation,
};

use super::{
    env::{SimConnection, SimulatorEnv},
    execution::{Execution, ExecutionHistory, ExecutionResult, execute_interaction},
};

pub(crate) fn run_simulation(
    env: Arc<Mutex<SimulatorEnv>>,
    plans: &mut [Vec<Vec<Interaction>>],
    last_execution: Arc<Mutex<Execution>>,
) -> ExecutionResult {
    let mut states = plans
        .iter()
        .map(|_| InteractionPlanState {
            stack: vec![],
            interaction_pointer: 0,
            secondary_pointer: 0,
        })
        .collect::<Vec<_>>();
    let mut result = execute_plans(env.clone(), plans, &mut states, last_execution);

    let env = env.lock().unwrap();
    env.io.print_stats();

    tracing::info!("Simulation completed");

    if result.error.is_none() {
        let ic = integrity_check(&env.get_db_path());
        if let Err(err) = ic {
            tracing::error!("integrity check failed: {}", err);
            result.error = Some(turso_core::TursoError::InternalError(err.to_string()));
        } else {
            tracing::info!("integrity check passed");
        }
    }

    result
}

pub(crate) fn execute_plans(
    env: Arc<Mutex<SimulatorEnv>>,
    plans: &mut [Vec<Vec<Interaction>>],
    states: &mut [InteractionPlanState],
    last_execution: Arc<Mutex<Execution>>,
) -> ExecutionResult {
    let mut history = ExecutionHistory::new();
    let now = std::time::Instant::now();
    let mut env = env.lock().unwrap();
    for _tick in 0..env.opts.ticks {
        // Pick the connection to interact with
        let connection_index = pick_index(env.connections.len(), &mut env.rng);
        let state = &mut states[connection_index];

        history.history.push(Execution::new(
            connection_index,
            state.interaction_pointer,
            state.secondary_pointer,
        ));
        let mut last_execution = last_execution.lock().unwrap();
        last_execution.connection_index = connection_index;
        last_execution.interaction_index = state.interaction_pointer;
        last_execution.secondary_index = state.secondary_pointer;
        // Execute the interaction for the selected connection
        match execute_plan(&mut env, connection_index, plans, states) {
            Ok(_) => {}
            Err(err) => {
                return ExecutionResult::new(history, Some(err));
            }
        }
        // Check if the maximum time for the simulation has been reached
        if now.elapsed().as_secs() >= env.opts.max_time_simulation as u64 {
            return ExecutionResult::new(
                history,
                Some(turso_core::TursoError::InternalError(
                    "maximum time for simulation reached".into(),
                )),
            );
        }
    }

    ExecutionResult::new(history, None)
}

fn execute_plan(
    env: &mut SimulatorEnv,
    connection_index: usize,
    plans: &mut [Vec<Vec<Interaction>>],
    states: &mut [InteractionPlanState],
) -> turso_core::Result<()> {
    let connection = &env.connections[connection_index];
    let plan = &mut plans[connection_index];
    let state = &mut states[connection_index];

    if state.interaction_pointer >= plan.len() {
        return Ok(());
    }

    let interaction = &plan[state.interaction_pointer][state.secondary_pointer];

    if let SimConnection::Disconnected = connection {
        tracing::debug!("connecting {}", connection_index);
        env.connections[connection_index] = SimConnection::LimboConnection(
            env.db.as_ref().expect("db to be Some").connect().unwrap(),
        );
    } else {
        match execute_interaction(env, connection_index, interaction, &mut state.stack) {
            Ok(next_execution) => {
                tracing::debug!("connection {} processed", connection_index);
                // Move to the next interaction or property
                match next_execution {
                    ExecutionContinuation::NextInteraction => {
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
