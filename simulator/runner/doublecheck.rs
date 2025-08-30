use std::{
    fs,
    sync::{Arc, Mutex},
};

use sql_generation::generation::pick_index;

use crate::{
    InteractionPlan, generation::plan::InteractionPlanState,
    runner::execution::ExecutionContinuation,
};

use super::{
    env::{SimConnection, SimulatorEnv},
    execution::{Execution, ExecutionHistory, ExecutionResult, execute_interaction},
};

pub(crate) fn run_simulation(
    env: Arc<Mutex<SimulatorEnv>>,
    doublecheck_env: Arc<Mutex<SimulatorEnv>>,
    plans: &mut [InteractionPlan],
    last_execution: Arc<Mutex<Execution>>,
) -> ExecutionResult {
    tracing::info!("Executing database interaction plan...");

    let mut states = plans
        .iter()
        .map(|_| InteractionPlanState {
            stack: vec![],
            interaction_pointer: 0,
            secondary_pointer: 0,
        })
        .collect::<Vec<_>>();

    let mut doublecheck_states = plans
        .iter()
        .map(|_| InteractionPlanState {
            stack: vec![],
            interaction_pointer: 0,
            secondary_pointer: 0,
        })
        .collect::<Vec<_>>();

    let mut result = execute_plans(
        env.clone(),
        doublecheck_env.clone(),
        plans,
        &mut states,
        &mut doublecheck_states,
        last_execution,
    );

    {
        env.clear_poison();
        let env = env.lock().unwrap();

        doublecheck_env.clear_poison();
        let doublecheck_env = doublecheck_env.lock().unwrap();

        // Check if the database files are the same
        let db = fs::read(env.get_db_path()).expect("should be able to read default database file");
        let doublecheck_db = fs::read(doublecheck_env.get_db_path())
            .expect("should be able to read doublecheck database file");

        if db != doublecheck_db {
            tracing::error!("Database files are different, check binary diffs for more details.");
            tracing::debug!("Default database path: {}", env.get_db_path().display());
            tracing::debug!(
                "Doublecheck database path: {}",
                doublecheck_env.get_db_path().display()
            );
            result.error = result.error.or_else(|| {
                Some(turso_core::LimboError::InternalError(
                    "database files are different, check binary diffs for more details.".into(),
                ))
            });
        }
    }

    tracing::info!("Simulation completed");

    result
}

pub(crate) fn execute_plans(
    env: Arc<Mutex<SimulatorEnv>>,
    doublecheck_env: Arc<Mutex<SimulatorEnv>>,
    plans: &mut [InteractionPlan],
    states: &mut [InteractionPlanState],
    doublecheck_states: &mut [InteractionPlanState],
    last_execution: Arc<Mutex<Execution>>,
) -> ExecutionResult {
    let mut history = ExecutionHistory::new();
    let now = std::time::Instant::now();

    let mut env = env.lock().unwrap();
    let mut doublecheck_env = doublecheck_env.lock().unwrap();

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
        match execute_plan(
            &mut env,
            &mut doublecheck_env,
            connection_index,
            plans,
            states,
            doublecheck_states,
        ) {
            Ok(_) => {}
            Err(err) => {
                return ExecutionResult::new(history, Some(err));
            }
        }
        // Check if the maximum time for the simulation has been reached
        if now.elapsed().as_secs() >= env.opts.max_time_simulation as u64 {
            return ExecutionResult::new(
                history,
                Some(turso_core::LimboError::InternalError(
                    "maximum time for simulation reached".into(),
                )),
            );
        }
    }

    ExecutionResult::new(history, None)
}

fn execute_plan(
    env: &mut SimulatorEnv,
    doublecheck_env: &mut SimulatorEnv,
    connection_index: usize,
    plans: &mut [InteractionPlan],
    states: &mut [InteractionPlanState],
    doublecheck_states: &mut [InteractionPlanState],
) -> turso_core::Result<()> {
    let connection = &env.connections[connection_index];
    let doublecheck_connection = &doublecheck_env.connections[connection_index];
    let plan = &mut plans[connection_index];

    let state = &mut states[connection_index];
    let doublecheck_state = &mut doublecheck_states[connection_index];

    if state.interaction_pointer >= plan.plan.len() {
        return Ok(());
    }

    let interaction = &plan.plan[state.interaction_pointer].interactions()[state.secondary_pointer];

    tracing::debug!(
        "execute_plan(connection_index={}, interaction={})",
        connection_index,
        interaction
    );
    tracing::debug!(
        "connection: {}, doublecheck_connection: {}",
        connection,
        doublecheck_connection
    );
    match (connection, doublecheck_connection) {
        (SimConnection::Disconnected, SimConnection::Disconnected) => {
            tracing::debug!("connecting {}", connection_index);
            env.connect(connection_index);
            doublecheck_env.connect(connection_index);
        }
        (SimConnection::LimboConnection(_), SimConnection::LimboConnection(_)) => {
            let limbo_result =
                execute_interaction(env, connection_index, interaction, &mut state.stack);
            let doublecheck_result = execute_interaction(
                doublecheck_env,
                connection_index,
                interaction,
                &mut doublecheck_state.stack,
            );
            match (limbo_result, doublecheck_result) {
                (Ok(next_execution), Ok(next_execution_doublecheck)) => {
                    if next_execution != next_execution_doublecheck {
                        tracing::error!(
                            "expected next executions of limbo and doublecheck do not match"
                        );
                        tracing::debug!(
                            "limbo result: {:?}, doublecheck result: {:?}",
                            next_execution,
                            next_execution_doublecheck
                        );
                        return Err(turso_core::LimboError::InternalError(
                            "expected next executions of limbo and doublecheck do not match".into(),
                        ));
                    }

                    let limbo_values = state.stack.last();
                    let doublecheck_values = doublecheck_state.stack.last();
                    match (limbo_values, doublecheck_values) {
                        (Some(limbo_values), Some(doublecheck_values)) => {
                            match (limbo_values, doublecheck_values) {
                                (Ok(limbo_values), Ok(doublecheck_values)) => {
                                    if limbo_values != doublecheck_values {
                                        tracing::error!(
                                            "returned values from limbo and doublecheck results do not match"
                                        );
                                        tracing::debug!("limbo values {:?}", limbo_values);
                                        tracing::debug!(
                                            "doublecheck values {:?}",
                                            doublecheck_values
                                        );
                                        return Err(turso_core::LimboError::InternalError(
                                            "returned values from limbo and doublecheck results do not match".into(),
                                        ));
                                    }
                                }
                                (Err(limbo_err), Err(doublecheck_err)) => {
                                    if limbo_err.to_string() != doublecheck_err.to_string() {
                                        tracing::error!(
                                            "limbo and doublecheck errors do not match"
                                        );
                                        tracing::error!("limbo error {}", limbo_err);
                                        tracing::error!("doublecheck error {}", doublecheck_err);
                                        return Err(turso_core::LimboError::InternalError(
                                            "limbo and doublecheck errors do not match".into(),
                                        ));
                                    }
                                }
                                (Ok(limbo_result), Err(doublecheck_err)) => {
                                    tracing::error!(
                                        "limbo and doublecheck results do not match, limbo returned values but doublecheck failed"
                                    );
                                    tracing::error!("limbo values {:?}", limbo_result);
                                    tracing::error!("doublecheck error {}", doublecheck_err);
                                    return Err(turso_core::LimboError::InternalError(
                                        "limbo and doublecheck results do not match".into(),
                                    ));
                                }
                                (Err(limbo_err), Ok(_)) => {
                                    tracing::error!(
                                        "limbo and doublecheck results do not match, limbo failed but doublecheck returned values"
                                    );
                                    tracing::error!("limbo error {}", limbo_err);
                                    return Err(turso_core::LimboError::InternalError(
                                        "limbo and doublecheck results do not match".into(),
                                    ));
                                }
                            }
                        }
                        (None, None) => {}
                        _ => {
                            tracing::error!("limbo and doublecheck results do not match");
                            return Err(turso_core::LimboError::InternalError(
                                "limbo and doublecheck results do not match".into(),
                            ));
                        }
                    }

                    // Move to the next interaction or property
                    match next_execution {
                        ExecutionContinuation::NextInteraction => {
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
                            state.interaction_pointer += 1;
                            state.secondary_pointer = 0;
                        }
                    }
                }
                (Err(err), Ok(_)) => {
                    tracing::error!("limbo and doublecheck results do not match");
                    tracing::error!("limbo error {}", err);
                    return Err(err);
                }
                (Ok(val), Err(err)) => {
                    tracing::error!("limbo and doublecheck results do not match");
                    tracing::error!("limbo {:?}", val);
                    tracing::error!("doublecheck error {}", err);
                    return Err(err);
                }
                (Err(err), Err(err_doublecheck)) => {
                    if err.to_string() != err_doublecheck.to_string() {
                        tracing::error!("limbo and doublecheck errors do not match");
                        tracing::error!("limbo error {}", err);
                        tracing::error!("doublecheck error {}", err_doublecheck);
                        return Err(turso_core::LimboError::InternalError(
                            "limbo and doublecheck errors do not match".into(),
                        ));
                    }
                }
            }
        }
        _ => unreachable!("{} vs {}", connection, doublecheck_connection),
    }

    Ok(())
}
