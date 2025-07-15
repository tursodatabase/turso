use std::{
    fs,
    sync::{Arc, Mutex},
};

use crate::{
    generation::plan::InteractionPlanState,
    runner::{execution::ExecutionContinuation, future::FuturesByConnection},
    InteractionPlan,
};

use super::{
    env::{SimConnection, SimulatorEnv},
    execution::{execute_interaction, Execution, ExecutionHistory, ExecutionResult},
};

pub(crate) fn run_simulation(
    env: Arc<Mutex<SimulatorEnv>>,
    doublecheck_env: Arc<Mutex<SimulatorEnv>>,
    plans: Vec<Arc<InteractionPlan>>,
    last_execution: Arc<Mutex<Execution>>,
) -> ExecutionResult {
    tracing::info!("Executing database interaction plan...");

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

    let doublecheck_states = plans
        .iter()
        .map(|_| {
            Arc::new(Mutex::new(InteractionPlanState {
                stack: vec![],
                interaction_pointer: 0,
                secondary_pointer: 0,
            }))
        })
        .collect::<Vec<_>>();

    let mut result = execute_plans(
        env.clone(),
        doublecheck_env.clone(),
        plans,
        states,
        doublecheck_states,
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
    plans: Vec<Arc<InteractionPlan>>,
    states: Vec<Arc<Mutex<InteractionPlanState>>>,
    doublecheck_states: Vec<Arc<Mutex<InteractionPlanState>>>,
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
        for connection_index in 0..connections_len {
            // Pick the connection to interact with
            {
                let state = states[connection_index].lock().unwrap();

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
            if futures_by_connection.connection_without_future(connection_index) {
                futures_by_connection.set_future(
                    connection_index,
                    Box::pin(execute_plan(
                        env.clone(),
                        doublecheck_env.clone(),
                        connection_index,
                        env.lock().unwrap().connections[connection_index].clone(),
                        doublecheck_env.lock().unwrap().connections[connection_index].clone(),
                        plans[connection_index].clone(),
                        states[connection_index].clone(),
                        doublecheck_states[connection_index].clone(),
                    )),
                );
            }
            // Execute the interaction for the selected connection
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

#[allow(clippy::too_many_arguments)]
async fn execute_plan(
    env: Arc<Mutex<SimulatorEnv>>,
    doublecheck_env: Arc<Mutex<SimulatorEnv>>,
    connection_index: usize,
    connection: Arc<Mutex<SimConnection>>,
    doublecheck_connection: Arc<Mutex<SimConnection>>,
    plan: Arc<InteractionPlan>,
    state: Arc<Mutex<InteractionPlanState>>,
    doublecheck_state: Arc<Mutex<InteractionPlanState>>,
) -> turso_core::Result<()> {
    let (interaction_pointer, secondary_pointer) = {
        let state = state.lock().unwrap();
        (state.interaction_pointer, state.secondary_pointer)
    };
    if interaction_pointer >= plan.plan.len() {
        return Ok(());
    }

    let interaction = &plan.plan[interaction_pointer].interactions()[secondary_pointer];

    tracing::debug!(
        "execute_plan(connection_index={}, interaction={})",
        connection_index,
        interaction
    );
    tracing::debug!(
        "connection: {}, doublecheck_connection: {}",
        connection.lock().unwrap(),
        doublecheck_connection.lock().unwrap()
    );
    let both_connected = {
        let connection = connection.lock().unwrap();
        let doublecheck_connection = doublecheck_connection.lock().unwrap();
        if connection.is_connected() != doublecheck_connection.is_connected() {
            return Err(turso_core::LimboError::InternalError(
                format!(
                    "connection and doublecheck are not both connected or disconnected {connection} vs {doublecheck_connection}"
                )
            ));
        }
        true
    };

    if both_connected {
        let limbo_result = execute_interaction(env, connection, interaction, state.clone()).await;
        let doublecheck_result = execute_interaction(
            doublecheck_env,
            doublecheck_connection,
            interaction,
            doublecheck_state.clone(),
        )
        .await;
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

                let mut state = state.lock().unwrap();
                let doublecheck_state = doublecheck_state.lock().unwrap();
                let limbo_values = state.stack.last();
                let doublecheck_values = doublecheck_state.stack.last();
                match (limbo_values, doublecheck_values) {
                    (Some(limbo_values), Some(doublecheck_values)) => {
                        match (limbo_values, doublecheck_values) {
                            (Ok(limbo_values), Ok(doublecheck_values)) => {
                                if limbo_values != doublecheck_values {
                                    tracing::error!("returned values from limbo and doublecheck results do not match");
                                    tracing::debug!("limbo values {:?}", limbo_values);
                                    tracing::debug!("doublecheck values {:?}", doublecheck_values);
                                    return Err(turso_core::LimboError::InternalError(
                                            "returned values from limbo and doublecheck results do not match".into(),
                                        ));
                                }
                            }
                            (Err(limbo_err), Err(doublecheck_err)) => {
                                if limbo_err.to_string() != doublecheck_err.to_string() {
                                    tracing::error!("limbo and doublecheck errors do not match");
                                    tracing::error!("limbo error {}", limbo_err);
                                    tracing::error!("doublecheck error {}", doublecheck_err);
                                    return Err(turso_core::LimboError::InternalError(
                                        "limbo and doublecheck errors do not match".into(),
                                    ));
                                }
                            }
                            (Ok(limbo_result), Err(doublecheck_err)) => {
                                tracing::error!("limbo and doublecheck results do not match, limbo returned values but doublecheck failed");
                                tracing::error!("limbo values {:?}", limbo_result);
                                tracing::error!("doublecheck error {}", doublecheck_err);
                                return Err(turso_core::LimboError::InternalError(
                                    "limbo and doublecheck results do not match".into(),
                                ));
                            }
                            (Err(limbo_err), Ok(_)) => {
                                tracing::error!("limbo and doublecheck results do not match, limbo failed but doublecheck returned values");
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
    } else {
        tracing::debug!("connecting {}", connection_index);
        env.lock().unwrap().connect(connection_index);
        doublecheck_env.lock().unwrap().connect(connection_index);
    }

    Ok(())
}
