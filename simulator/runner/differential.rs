use std::sync::{Arc, Mutex};

use turso_core::Value;

use crate::{
    generation::{
        plan::{Interaction, InteractionPlanState, ResultSet},
        Shadow,
    },
    model::{query::Query, table::SimValue},
    runner::{execution::ExecutionContinuation, future::FuturesByConnection},
    InteractionPlan,
};

use super::{
    env::{SimConnection, SimulatorEnv},
    execution::{execute_interaction, Execution, ExecutionHistory, ExecutionResult},
};

pub(crate) fn run_simulation(
    env: Arc<Mutex<SimulatorEnv>>,
    rusqlite_env: Arc<Mutex<SimulatorEnv>>,
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

    let rusqlite_states = plans
        .iter()
        .map(|_| {
            Arc::new(Mutex::new(InteractionPlanState {
                stack: vec![],
                interaction_pointer: 0,
                secondary_pointer: 0,
            }))
        })
        .collect::<Vec<_>>();

    let result = execute_plans(
        env,
        rusqlite_env,
        plans,
        states,
        rusqlite_states,
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
    plans: Vec<Arc<InteractionPlan>>,
    states: Vec<Arc<Mutex<InteractionPlanState>>>,
    rusqlite_states: Vec<Arc<Mutex<InteractionPlanState>>>,
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
            let rusqlite_connection =
                rusqlite_env.lock().unwrap().connections[connection_index].clone();
            let connection = env.lock().unwrap().connections[connection_index].clone();
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
            // Execute the interaction for the selected connection
            if futures_by_connection.connection_without_future(connection_index) {
                futures_by_connection.set_future(
                    connection_index,
                    Box::pin(execute_plan(
                        env.clone(),
                        rusqlite_env.clone(),
                        rusqlite_connection,
                        connection,
                        plans[connection_index].clone(),
                        state.clone(),
                        rusqlite_states[connection_index].clone(),
                        connection_index,
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

#[allow(clippy::too_many_arguments)]
async fn execute_plan(
    env: Arc<Mutex<SimulatorEnv>>,
    rusqlite_env: Arc<Mutex<SimulatorEnv>>,
    rusqlite_conn: Arc<Mutex<SimConnection>>,
    connection: Arc<Mutex<SimConnection>>,
    plan: Arc<InteractionPlan>,
    state: Arc<Mutex<InteractionPlanState>>,
    rusqlite_state: Arc<Mutex<InteractionPlanState>>,
    connection_index: usize,
) -> turso_core::Result<()> {
    let interaction_pointer = state.lock().unwrap().interaction_pointer;
    let secondary_pointer = state.lock().unwrap().secondary_pointer;
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
        "connection: {}, rusqlite_connection: {}",
        connection.lock().unwrap(),
        rusqlite_conn.lock().unwrap()
    );
    let both_connected = {
        let conn = connection.lock().unwrap();
        let rusqlite_conn = rusqlite_conn.lock().unwrap();
        if conn.is_connected() != rusqlite_conn.is_connected() {
            return Err(turso_core::LimboError::InternalError(
                "limbo and rusqlite connections are not connected".into(),
            ));
        }
        conn.is_connected() && rusqlite_conn.is_connected()
    };
    if !both_connected {
        env.lock().unwrap().connect(connection_index);
        rusqlite_env.lock().unwrap().connect(connection_index);
        return Ok(());
    }

    let limbo_result =
        execute_interaction(env.clone(), connection.clone(), interaction, state.clone()).await;
    let mut rusqlite_env = rusqlite_env.lock().unwrap();
    let ruqlite_result = execute_interaction_rusqlite(
        &mut rusqlite_env,
        rusqlite_conn.clone(),
        interaction,
        &mut rusqlite_state.lock().unwrap().stack,
    );
    match (limbo_result, ruqlite_result) {
        (Ok(next_execution), Ok(next_execution_rusqlite)) => {
            if next_execution != next_execution_rusqlite {
                tracing::error!("expected next executions of limbo and rusqlite do not match");
                tracing::debug!(
                    "limbo result: {:?}, rusqlite result: {:?}",
                    next_execution,
                    next_execution_rusqlite
                );
                return Err(turso_core::LimboError::InternalError(
                    "expected next executions of limbo and rusqlite do not match".into(),
                ));
            }

            let mut state = state.lock().unwrap();
            let limbo_values = state.stack.last();
            let rusqlite_state = rusqlite_state.lock().unwrap();
            let rusqlite_values = rusqlite_state.stack.last();
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
                            tracing::error!("limbo and rusqlite results do not match, limbo returned values but rusqlite failed");
                            tracing::error!("limbo values {:?}", limbo_result);
                            tracing::error!("rusqlite error {}", rusqlite_err);
                            return Err(turso_core::LimboError::InternalError(
                                "limbo and rusqlite results do not match".into(),
                            ));
                        }
                        (Err(limbo_err), Ok(_)) => {
                            tracing::error!("limbo and rusqlite results do not match, limbo failed but rusqlite returned values");
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
            // todo: Previously, we returned an error here, but now we just log it.
            //       The problem is that the errors might be different, and we cannot
            //       just assume both of them being errors has the same semantics.
            // return Err(err);
        }
    }

    Ok(())
}

fn execute_interaction_rusqlite(
    env: &mut SimulatorEnv,
    connection: Arc<Mutex<SimConnection>>,
    interaction: &Interaction,
    stack: &mut Vec<ResultSet>,
) -> turso_core::Result<ExecutionContinuation> {
    tracing::trace!(
        "execute_interaction_rusqlite(connection_index={}, interaction={})",
        1, // todo: add index to simconnetion
        interaction
    );
    match interaction {
        Interaction::Query(query) => {
            let mut conn = connection.lock().unwrap();
            let conn = match &mut *conn {
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
            interaction.execute_fault(env, connection)?;
        }
        Interaction::FaultyQuery(_) => {
            unimplemented!("cannot implement faulty query in rusqlite, as we do not control IO");
        }
    }

    let _ = interaction.shadow(&mut env.tables);
    Ok(ExecutionContinuation::NextInteraction)
}
