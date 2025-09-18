use std::{
    fs,
    sync::{Arc, Mutex},
};

use crate::generation::plan::{ConnectionState, Interaction, InteractionPlanState};

use super::{
    env::SimulatorEnv,
    execution::{Execution, ExecutionHistory, ExecutionResult},
};

pub fn run_simulation(
    env: Arc<Mutex<SimulatorEnv>>,
    doublecheck_env: Arc<Mutex<SimulatorEnv>>,
    plan: Vec<Interaction>,
    last_execution: Arc<Mutex<Execution>>,
) -> ExecutionResult {
    tracing::info!("Executing database interaction plan...");

    let num_conns = {
        let env = env.lock().unwrap();
        env.connections.len()
    };

    let mut conn_states = (0..num_conns)
        .map(|_| ConnectionState::default())
        .collect::<Vec<_>>();

    let mut doublecheck_states = conn_states.clone();

    let mut state = InteractionPlanState {
        interaction_pointer: 0,
    };

    let mut result = execute_plans(
        env.clone(),
        doublecheck_env.clone(),
        plan,
        &mut state,
        &mut conn_states,
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
    interactions: Vec<Interaction>,
    state: &mut InteractionPlanState,
    conn_states: &mut [ConnectionState],
    doublecheck_states: &mut [ConnectionState],
    last_execution: Arc<Mutex<Execution>>,
) -> ExecutionResult {
    let mut history = ExecutionHistory::new();

    let mut env = env.lock().unwrap();
    let mut doublecheck_env = doublecheck_env.lock().unwrap();

    env.tables.clear();
    doublecheck_env.tables.clear();

    let now = std::time::Instant::now();

    for _tick in 0..env.opts.ticks {
        if state.interaction_pointer >= interactions.len() {
            break;
        }

        let interaction = &interactions[state.interaction_pointer];

        let connection_index = interaction.connection_index;
        let turso_conn_state = &mut conn_states[connection_index];
        let doublecheck_conn_state = &mut doublecheck_states[connection_index];

        history
            .history
            .push(Execution::new(connection_index, state.interaction_pointer));
        let mut last_execution = last_execution.lock().unwrap();
        last_execution.connection_index = connection_index;
        last_execution.interaction_index = state.interaction_pointer;

        let mut turso_state = state.clone();

        // first execute turso
        let turso_res = super::execution::execute_plan(
            &mut env,
            interaction,
            turso_conn_state,
            &mut turso_state,
        );

        let mut doublecheck_state = state.clone();

        // second execute doublecheck
        let doublecheck_res = super::execution::execute_plan(
            &mut doublecheck_env,
            interaction,
            doublecheck_conn_state,
            &mut doublecheck_state,
        );

        // Compare results
        if let Err(err) = compare_results(
            turso_res,
            turso_conn_state,
            doublecheck_res,
            doublecheck_conn_state,
        ) {
            return ExecutionResult::new(history, Some(err));
        }

        state.interaction_pointer += 1;

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

fn compare_results(
    turso_res: turso_core::Result<()>,
    turso_state: &mut ConnectionState,
    doublecheck_res: turso_core::Result<()>,
    doublecheck_state: &mut ConnectionState,
) -> turso_core::Result<()> {
    match (turso_res, doublecheck_res) {
        (Ok(..), Ok(..)) => {
            let turso_values = turso_state.stack.last();
            let doublecheck_values = doublecheck_state.stack.last();
            match (turso_values, doublecheck_values) {
                (Some(limbo_values), Some(doublecheck_values)) => {
                    match (limbo_values, doublecheck_values) {
                        (Ok(limbo_values), Ok(doublecheck_values)) => {
                            if limbo_values != doublecheck_values {
                                tracing::error!(
                                    "returned values from limbo and doublecheck results do not match"
                                );
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
    Ok(())
}
