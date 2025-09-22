use std::sync::{Arc, Mutex};

use crate::generation::plan::{ConnectionState, Interaction, InteractionPlanState};

use super::{
    env::SimulatorEnv,
    execution::{Execution, ExecutionHistory, ExecutionResult},
};

pub fn run_simulation(
    env: Arc<Mutex<SimulatorEnv>>,
    rusqlite_env: Arc<Mutex<SimulatorEnv>>,
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

    let mut rusqlite_states = conn_states.clone();

    let mut state = InteractionPlanState {
        interaction_pointer: 0,
    };

    let result = execute_interactions(
        env,
        rusqlite_env,
        plan,
        &mut state,
        &mut conn_states,
        &mut rusqlite_states,
        last_execution,
    );

    tracing::info!("Simulation completed");

    result
}

pub(crate) fn execute_interactions(
    env: Arc<Mutex<SimulatorEnv>>,
    rusqlite_env: Arc<Mutex<SimulatorEnv>>,
    interactions: Vec<Interaction>,
    state: &mut InteractionPlanState,
    conn_states: &mut [ConnectionState],
    rusqlite_states: &mut [ConnectionState],
    last_execution: Arc<Mutex<Execution>>,
) -> ExecutionResult {
    let mut history = ExecutionHistory::new();

    let mut env = env.lock().unwrap();
    let mut rusqlite_env = rusqlite_env.lock().unwrap();

    env.clear_tables();
    rusqlite_env.clear_tables();

    let now = std::time::Instant::now();

    for _tick in 0..env.opts.ticks {
        if state.interaction_pointer >= interactions.len() {
            break;
        }

        let interaction = &interactions[state.interaction_pointer];

        let connection_index = interaction.connection_index;
        let turso_conn_state = &mut conn_states[connection_index];
        let rusqlite_conn_state = &mut rusqlite_states[connection_index];

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

        let mut rusqlite_state = state.clone();

        // second execute rusqlite
        let rusqlite_res = super::execution::execute_plan(
            &mut rusqlite_env,
            interaction,
            rusqlite_conn_state,
            &mut rusqlite_state,
        );

        // Compare results
        if let Err(err) = compare_results(
            turso_res,
            turso_conn_state,
            rusqlite_res,
            rusqlite_conn_state,
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
    turso_conn_state: &mut ConnectionState,
    rusqlite_res: turso_core::Result<()>,
    rusqlite_conn_state: &mut ConnectionState,
) -> turso_core::Result<()> {
    match (turso_res, rusqlite_res) {
        (Ok(..), Ok(..)) => {
            let limbo_values = turso_conn_state.stack.last();
            let rusqlite_values = rusqlite_conn_state.stack.last();
            match (limbo_values, rusqlite_values) {
                (Some(limbo_values), Some(rusqlite_values)) => {
                    match (limbo_values, rusqlite_values) {
                        (Ok(limbo_values), Ok(rusqlite_values)) => {
                            if limbo_values != rusqlite_values {
                                tracing::error!(
                                    "returned values from limbo and rusqlite results do not match"
                                );
                                let diff = limbo_values
                                    .iter()
                                    .zip(rusqlite_values.iter())
                                    .enumerate()
                                    .filter(|(_, (l, r))| l != r)
                                    .collect::<Vec<_>>();

                                let diff = diff
                                    .iter()
                                    .flat_map(|(i, (l, r))| {
                                        let mut diffs = vec![];
                                        for (j, (l, r)) in l.iter().zip(r.iter()).enumerate() {
                                            if l != r {
                                                tracing::debug!(
                                                    "difference at index {}, {}: {} != {}",
                                                    i,
                                                    j,
                                                    l.to_string(),
                                                    r.to_string()
                                                );
                                                diffs.push(((i, j), (l.clone(), r.clone())));
                                            }
                                        }
                                        diffs
                                    })
                                    .collect::<Vec<_>>();
                                tracing::debug!("limbo values {:?}", limbo_values);
                                tracing::debug!("rusqlite values {:?}", rusqlite_values);
                                tracing::debug!(
                                    "differences: {}",
                                    diff.iter()
                                        .map(|((i, j), (l, r))| format!(
                                            "\t({i}, {j}): ({l}) != ({r})"
                                        ))
                                        .collect::<Vec<_>>()
                                        .join("\n")
                                );
                                return Err(turso_core::LimboError::InternalError(
                                    "returned values from limbo and rusqlite results do not match"
                                        .into(),
                                ));
                            }
                        }
                        (Err(limbo_err), Err(rusqlite_err)) => {
                            tracing::warn!("limbo and rusqlite both fail, requires manual check");
                            tracing::warn!("limbo error {}", limbo_err);
                            tracing::warn!("rusqlite error {}", rusqlite_err);
                        }
                        (Ok(limbo_result), Err(rusqlite_err)) => {
                            tracing::error!(
                                "limbo and rusqlite results do not match, limbo returned values but rusqlite failed"
                            );
                            tracing::error!("limbo values {:?}", limbo_result);
                            tracing::error!("rusqlite error {}", rusqlite_err);
                            return Err(turso_core::LimboError::InternalError(
                                "limbo and rusqlite results do not match".into(),
                            ));
                        }
                        (Err(limbo_err), Ok(_)) => {
                            tracing::error!(
                                "limbo and rusqlite results do not match, limbo failed but rusqlite returned values"
                            );
                            tracing::error!("limbo error {}", limbo_err);
                            return Err(turso_core::LimboError::InternalError(
                                "limbo and rusqlite results do not match".into(),
                            ));
                        }
                    }
                }
                (None, None) => {}
                _ => {
                    tracing::error!("limbo and rusqlite results do not match");
                    return Err(turso_core::LimboError::InternalError(
                        "limbo and rusqlite results do not match".into(),
                    ));
                }
            }
        }
        (Err(err), Ok(_)) => {
            tracing::error!("limbo and rusqlite results do not match");
            tracing::error!("limbo error {}", err);
            return Err(err);
        }
        (Ok(val), Err(err)) => {
            tracing::error!("limbo and rusqlite results do not match");
            tracing::error!("limbo {:?}", val);
            tracing::error!("rusqlite error {}", err);
            return Err(err);
        }
        (Err(err), Err(err_rusqlite)) => {
            tracing::error!("limbo and rusqlite both fail, requires manual check");
            tracing::error!("limbo error {}", err);
            tracing::error!("rusqlite error {}", err_rusqlite);
            // todo: Previously, we returned an error here, but now we just log it.
            //       The problem is that the errors might be different, and we cannot
            //       just assume both of them being errors has the same semantics.
            // return Err(err);
        }
    }
    Ok(())
}
