use std::sync::{Arc, Mutex};

use turso_core::Value;

use crate::{
    generation::{
        pick_index,
        plan::{Interaction, InteractionPlanState, ResultSet},
    },
    model::{query::Query, table::SimValue},
    runner::execution::ExecutionContinuation,
    InteractionPlan,
};

use super::{
    env::{SimConnection, SimulatorEnv},
    execution::{execute_interaction, Execution, ExecutionHistory, ExecutionResult},
};

pub(crate) fn run_simulation(
    env: Arc<Mutex<SimulatorEnv>>,
    rusqlite_env: Arc<Mutex<SimulatorEnv>>,
    rusqlite_conn: &dyn Fn() -> rusqlite::Connection,
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

    let mut rusqlite_states = plans
        .iter()
        .map(|_| InteractionPlanState {
            stack: vec![],
            interaction_pointer: 0,
            secondary_pointer: 0,
        })
        .collect::<Vec<_>>();

    let result = execute_plans(
        env,
        rusqlite_env,
        rusqlite_conn,
        plans,
        &mut states,
        &mut rusqlite_states,
        last_execution,
    );

    tracing::info!("Simulation completed");

    result
}

fn execute_query_rusqlite(
    connection: &rusqlite::Connection,
    query: &Query,
) -> rusqlite::Result<Vec<Vec<SimValue>>> {
    match query {
        Query::Create(create) => {
            connection.execute(create.to_string().as_str(), ())?;
            Ok(vec![])
        }
        Query::Select(select) => {
            let mut stmt = connection.prepare(select.to_string().as_str())?;
            let columns = stmt.column_count();
            let rows = stmt.query_map([], |row| {
                let mut values = vec![];
                for i in 0..columns {
                    let value = row.get_unwrap(i);
                    let value = match value {
                        rusqlite::types::Value::Null => Value::Null,
                        rusqlite::types::Value::Integer(i) => Value::Integer(i),
                        rusqlite::types::Value::Real(f) => Value::Float(f),
                        rusqlite::types::Value::Text(s) => Value::build_text(s),
                        rusqlite::types::Value::Blob(b) => Value::Blob(b),
                    };
                    values.push(SimValue(value));
                }
                Ok(values)
            })?;
            let mut result = vec![];
            for row in rows {
                result.push(row?);
            }
            Ok(result)
        }
        Query::Insert(insert) => {
            connection.execute(insert.to_string().as_str(), ())?;
            Ok(vec![])
        }
        Query::Delete(delete) => {
            connection.execute(delete.to_string().as_str(), ())?;
            Ok(vec![])
        }
        Query::Drop(drop) => {
            connection.execute(drop.to_string().as_str(), ())?;
            Ok(vec![])
        }
        Query::Update(update) => {
            connection.execute(update.to_string().as_str(), ())?;
            Ok(vec![])
        }
        Query::CreateIndex(create_index) => {
            connection.execute(create_index.to_string().as_str(), ())?;
            Ok(vec![])
        }
        Query::Begin(begin) => {
            connection.execute(begin.to_string().as_str(), ())?;
            Ok(vec![])
        }
        Query::Commit(commit) => {
            connection.execute(commit.to_string().as_str(), ())?;
            Ok(vec![])
        }
        Query::Rollback(rollback) => {
            connection.execute(rollback.to_string().as_str(), ())?;
            Ok(vec![])
        }
    }
}

pub(crate) fn execute_plans(
    env: Arc<Mutex<SimulatorEnv>>,
    rusqlite_env: Arc<Mutex<SimulatorEnv>>,
    rusqlite_conn: &dyn Fn() -> rusqlite::Connection,
    plans: &mut [InteractionPlan],
    states: &mut [InteractionPlanState],
    rusqlite_states: &mut [InteractionPlanState],
    last_execution: Arc<Mutex<Execution>>,
) -> ExecutionResult {
    let mut history = ExecutionHistory::new();
    let now = std::time::Instant::now();

    let mut env = env.lock().unwrap();
    let mut rusqlite_env = rusqlite_env.lock().unwrap();

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
            &mut rusqlite_env,
            rusqlite_conn,
            connection_index,
            plans,
            states,
            rusqlite_states,
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
    rusqlite_env: &mut SimulatorEnv,
    rusqlite_conn: &dyn Fn() -> rusqlite::Connection,
    connection_index: usize,
    plans: &mut [InteractionPlan],
    states: &mut [InteractionPlanState],
    rusqlite_states: &mut [InteractionPlanState],
) -> turso_core::Result<()> {
    let connection = &env.connections[connection_index];
    let rusqlite_connection = &rusqlite_env.connections[connection_index];
    let plan = &mut plans[connection_index];
    let state = &mut states[connection_index];
    let rusqlite_state = &mut rusqlite_states[connection_index];
    if state.interaction_pointer >= plan.plan.len() {
        return Ok(());
    }

    let interaction = &plan.plan[state.interaction_pointer].interactions()[state.secondary_pointer];

    match (connection, rusqlite_connection) {
        (SimConnection::Disconnected, SimConnection::Disconnected) => {
            tracing::debug!("connecting {}", connection_index);
            env.connections[connection_index] =
                SimConnection::LimboConnection(env.db.connect().unwrap());
            rusqlite_env.connections[connection_index] =
                SimConnection::SQLiteConnection(rusqlite_conn());
        }
        (SimConnection::LimboConnection(_), SimConnection::SQLiteConnection(_)) => {
            let limbo_result =
                execute_interaction(env, connection_index, interaction, &mut state.stack);
            let ruqlite_result = execute_interaction_rusqlite(
                rusqlite_env,
                connection_index,
                interaction,
                &mut rusqlite_state.stack,
            );
            match (limbo_result, ruqlite_result) {
                (Ok(next_execution), Ok(next_execution_rusqlite)) => {
                    if next_execution != next_execution_rusqlite {
                        tracing::error!("limbo and rusqlite results do not match");
                        return Err(turso_core::LimboError::InternalError(
                            "limbo and rusqlite results do not match".into(),
                        ));
                    }

                    let limbo_values = state.stack.last();
                    let rusqlite_values = rusqlite_state.stack.last();
                    match (limbo_values, rusqlite_values) {
                        (Some(limbo_values), Some(rusqlite_values)) => {
                            match (limbo_values, rusqlite_values) {
                                (Ok(limbo_values), Ok(rusqlite_values)) => {
                                    if limbo_values != rusqlite_values {
                                        tracing::error!("limbo and rusqlite results do not match");
                                        return Err(turso_core::LimboError::InternalError(
                                            "limbo and rusqlite results do not match".into(),
                                        ));
                                    }
                                }
                                (Err(limbo_err), Err(rusqlite_err)) => {
                                    tracing::warn!(
                                        "limbo and rusqlite both fail, requires manual check"
                                    );
                                    tracing::warn!("limbo error {}", limbo_err);
                                    tracing::warn!("rusqlite error {}", rusqlite_err);
                                }
                                (Ok(limbo_result), Err(rusqlite_err)) => {
                                    tracing::error!("limbo and rusqlite results do not match");
                                    tracing::error!("limbo values {:?}", limbo_result);
                                    tracing::error!("rusqlite error {}", rusqlite_err);
                                    return Err(turso_core::LimboError::InternalError(
                                        "limbo and rusqlite results do not match".into(),
                                    ));
                                }
                                (Err(limbo_err), Ok(_)) => {
                                    tracing::error!("limbo and rusqlite results do not match");
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
                    return Err(err);
                }
            }
        }
        _ => unreachable!("{} vs {}", connection, rusqlite_connection),
    }

    Ok(())
}

fn execute_interaction_rusqlite(
    env: &mut SimulatorEnv,
    connection_index: usize,
    interaction: &Interaction,
    stack: &mut Vec<ResultSet>,
) -> turso_core::Result<ExecutionContinuation> {
    tracing::trace!(
        "execute_interaction_rusqlite(connection_index={}, interaction={})",
        connection_index,
        interaction
    );
    match interaction {
        Interaction::Query(query) => {
            let conn = match &mut env.connections[connection_index] {
                SimConnection::SQLiteConnection(conn) => conn,
                SimConnection::LimboConnection(_) => unreachable!(),
                SimConnection::Disconnected => unreachable!(),
            };

            tracing::debug!("{}", interaction);
            let results = execute_query_rusqlite(conn, query).map_err(|e| {
                turso_core::LimboError::InternalError(format!("error executing query: {e}"))
            });
            tracing::debug!("{:?}", results);
            stack.push(results);
        }
        Interaction::FsyncQuery(..) => {
            unimplemented!("cannot implement fsync query in rusqlite, as we do not control IO");
        }
        Interaction::Assertion(_) => {
            interaction.execute_assertion(stack, env)?;
            stack.clear();
        }
        Interaction::Assumption(_) => {
            let assumption_result = interaction.execute_assumption(stack, env);
            stack.clear();

            if assumption_result.is_err() {
                tracing::warn!("assumption failed: {:?}", assumption_result);
                return Ok(ExecutionContinuation::NextProperty);
            }
        }
        Interaction::Fault(_) => {
            interaction.execute_fault(env, connection_index)?;
        }
        Interaction::FaultyQuery(_) => {
            unimplemented!("cannot implement faulty query in rusqlite, as we do not control IO");
        }
    }

    Ok(ExecutionContinuation::NextInteraction)
}
